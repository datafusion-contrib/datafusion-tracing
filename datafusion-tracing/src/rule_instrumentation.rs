// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.
//
// This product includes software developed at Datadog (https://www.datadoghq.com/) Copyright 2025 Datadog, Inc.

use crate::planner::TracingQueryPlanner;
use crate::rule_options::RuleInstrumentationOptions;
use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNodeRewriter};
use datafusion::common::{DataFusionError, Result};
use datafusion::execution::SessionState;
use datafusion::execution::session_state::SessionStateBuilder;
use datafusion::logical_expr::LogicalPlan;
use datafusion::optimizer::analyzer::AnalyzerRule;
use datafusion::optimizer::optimizer::{ApplyOrder, OptimizerConfig, OptimizerRule};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use similar::{ChangeTag, TextDiff};
use std::cell::RefCell;
use std::fmt::Debug;
use std::sync::Arc;
use tracing::Level;
use tracing::Span;

// ============================================================================
// Semantic constants for sentinel rule names and phase names
// ============================================================================

/// Internal sentinel rule names used to identify tracing infrastructure rules.
mod sentinel_names {
    pub const ANALYZER: &str = "__tracing_analyzer_phase";
    pub const OPTIMIZER: &str = "__tracing_optimizer_phase";
    pub const PHYSICAL_OPTIMIZER: &str = "__tracing_physical_optimizer_phase";
}

/// Phase names used for span identification in traces.
mod phase_names {
    pub const ANALYZE_LOGICAL_PLAN: &str = "analyze_logical_plan";
    pub const OPTIMIZE_LOGICAL_PLAN: &str = "optimize_logical_plan";
    pub const OPTIMIZE_PHYSICAL_PLAN: &str = "optimize_physical_plan";
}

// ============================================================================
// Plan formatting trait for unified plan-to-string conversion
// ============================================================================

/// Trait for formatting plans as strings for diffing and tracing.
///
/// This provides a unified interface for converting different plan types
/// (logical and physical) to their string representations.
trait FormatPlan {
    /// Formats the plan as a string suitable for diff comparison.
    fn format_for_diff(&self) -> String;
}

impl FormatPlan for LogicalPlan {
    fn format_for_diff(&self) -> String {
        self.display_indent_schema().to_string()
    }
}

impl FormatPlan for Arc<dyn ExecutionPlan> {
    fn format_for_diff(&self) -> String {
        displayable(self.as_ref()).indent(true).to_string()
    }
}

/// Type alias for a function that creates a phase span given the phase name.
/// Example phase names: "analyze_logical_plan", "optimize_logical_plan", "optimize_physical_plan".
pub(crate) type PhaseSpanCreateFn = dyn Fn(&str) -> Span + Send + Sync;

// ============================================================================
// Unified Planning Context
// ============================================================================

/// The phase of query planning currently active.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PlanningPhase {
    Analyzer,
    Optimizer,
    PhysicalOptimizer,
}

/// Context for tracking the active planning phase span.
///
/// Stores the current phase, the entered span guard, and state needed for
/// computing plan diffs and tracking which rules modified the plan.
struct PlanningContext {
    /// The current planning phase.
    phase: PlanningPhase,
    /// The entered span guard - keeps the span open until this context is dropped.
    _entered: tracing::span::EnteredSpan,
    /// The plan state at the start of the phase (for computing diff).
    /// Only captured if plan_diff option is enabled and span is active.
    plan_before: Option<String>,
    /// List of rule names that modified the plan during this phase.
    effective_rules: Vec<String>,
}

/// Tracks the optimizer pass count, resetting when a new query starts.
/// A new query is detected by a change in the parent span ID or when
/// the physical optimizer phase completes.
struct OptimizerPassTracker {
    parent_span_id: Option<tracing::span::Id>,
    pass_count: usize,
}

impl OptimizerPassTracker {
    const fn new() -> Self {
        Self {
            parent_span_id: None,
            pass_count: 0,
        }
    }

    /// Gets the current pass number, resetting if this is a new query (different parent span).
    fn get_and_increment(
        &mut self,
        current_parent_id: Option<tracing::span::Id>,
    ) -> usize {
        if self.parent_span_id != current_parent_id {
            // New query detected - reset counter
            self.parent_span_id = current_parent_id;
            self.pass_count = 0;
        }
        let pass = self.pass_count;
        self.pass_count += 1;
        pass
    }

    /// Explicitly resets the pass counter. Called when physical optimizer completes
    /// to provide a deterministic end-of-query signal.
    fn reset(&mut self) {
        self.parent_span_id = None;
        self.pass_count = 0;
    }
}

// Thread-local storage for the unified planning context.
//
// Why thread-local is correct here:
// - DataFusion's analyzer and optimizer loops run synchronously on a single thread.
//   Each phase (analyze, optimize, physical optimize) processes all its rules sequentially
//   before returning control.
// - Sentinel rules at the start/end of each rule list toggle the phase span:
//   first call opens it, second call closes it.
// - The optimizer may run multiple passes, each getting its own phase span.
//   OPTIMIZER_PASS_TRACKER tracks which pass we're on, and resets when:
//   1. The parent span changes (indicating a new query), or
//   2. The physical optimizer phase completes (deterministic end-of-query signal)
// - Concurrent sessions on different threads each get their own thread-local storage.
// - If sessions somehow share a thread (e.g., sequential queries), the sentinel
//   close ensures the previous phase span ends before any new phase begins.
thread_local! {
    /// The current planning context, if any phase is active.
    static PLANNING_CONTEXT: RefCell<Option<PlanningContext>> = const { RefCell::new(None) };
    /// Tracks optimizer pass count and the parent span ID to detect new queries.
    static OPTIMIZER_PASS_TRACKER: RefCell<OptimizerPassTracker> = const { RefCell::new(OptimizerPassTracker::new()) };
}

/// Records that a rule modified the plan in the current phase.
fn record_modified_rule_in_context(rule_name: &str) {
    PLANNING_CONTEXT.with(|cell| {
        if let Some(ref mut ctx) = *cell.borrow_mut() {
            ctx.effective_rules.push(rule_name.to_string());
        }
    });
}

/// Drops the current planning context on a fatal rule error, closing the active
/// phase span and resetting `OPTIMIZER_PASS_TRACKER` for `Optimizer` and
/// `PhysicalOptimizer` phases so the next query starts at pass 0.
fn drop_planning_context() {
    let ctx = PLANNING_CONTEXT.with(|cell| cell.borrow_mut().take());
    if let Some(ctx) = ctx {
        if matches!(
            ctx.phase,
            PlanningPhase::Optimizer | PlanningPhase::PhysicalOptimizer
        ) {
            OPTIMIZER_PASS_TRACKER.with(|cell| cell.borrow_mut().reset());
        }
        drop(ctx);
    }
}

/// Closes a phase span, recording effective rules and plan diff if enabled.
///
/// The span is closed when `ctx` is dropped at the end of this function,
/// as `ctx._entered` holds the entered span guard.
fn close_phase_span<P: FormatPlan>(ctx: PlanningContext, plan_after: &P) {
    let current = Span::current();
    // Only perform expensive operations if span is active (lazy instrumentation)
    if !current.is_disabled() {
        if !ctx.effective_rules.is_empty() {
            current.record("datafusion.effective_rules", ctx.effective_rules.join(", "));
        }
        if let Some(before) = &ctx.plan_before {
            let after = plan_after.format_for_diff();
            if before != &after {
                let diff = generate_plan_diff(before, &after);
                current.record("datafusion.plan_diff", diff);
            }
        }
    }
    // ctx._entered is dropped here when ctx goes out of scope, closing the span
}

// ============================================================================
// Sentinel rules for opening/closing phase spans (toggle pattern)
// ============================================================================

/// Sentinel analyzer rule that toggles the phase span.
/// First call opens the span, second call closes it.
struct AnalyzerPhaseSentinel {
    phase_span_create_fn: Arc<PhaseSpanCreateFn>,
    plan_diff: bool,
}

impl Debug for AnalyzerPhaseSentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("AnalyzerPhaseSentinel").finish()
    }
}

impl AnalyzerRule for AnalyzerPhaseSentinel {
    fn analyze(&self, plan: LogicalPlan, _config: &ConfigOptions) -> Result<LogicalPlan> {
        PLANNING_CONTEXT.with(|cell| {
            let mut guard = cell.borrow_mut();
            // Check if we should open or close the analyzer phase
            let should_open = guard
                .as_ref()
                .map(|ctx| ctx.phase != PlanningPhase::Analyzer)
                .unwrap_or(true);

            if should_open {
                // Open the analyzer phase span
                let span = (self.phase_span_create_fn)(phase_names::ANALYZE_LOGICAL_PLAN);
                // Only capture plan for diff if enabled AND span is active (lazy instrumentation)
                let plan_before = if self.plan_diff && !span.is_disabled() {
                    Some(plan.format_for_diff())
                } else {
                    None
                };
                *guard = Some(PlanningContext {
                    phase: PlanningPhase::Analyzer,
                    _entered: span.entered(),
                    plan_before,
                    effective_rules: Vec::new(),
                });
            } else {
                // Close the analyzer phase span
                if let Some(ctx) = guard.take() {
                    close_phase_span(ctx, &plan);
                }
            }
        });
        Ok(plan)
    }

    fn name(&self) -> &str {
        sentinel_names::ANALYZER
    }
}

/// Sentinel optimizer rule that toggles the phase span.
/// First call opens the span, second call closes it.
struct OptimizerPhaseSentinel {
    phase_span_create_fn: Arc<PhaseSpanCreateFn>,
    plan_diff: bool,
}

impl Debug for OptimizerPhaseSentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OptimizerPhaseSentinel").finish()
    }
}

impl OptimizerRule for OptimizerPhaseSentinel {
    fn name(&self) -> &str {
        sentinel_names::OPTIMIZER
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        None
    }

    #[allow(deprecated)]
    fn supports_rewrite(&self) -> bool {
        true
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        PLANNING_CONTEXT.with(|cell| {
            let mut guard = cell.borrow_mut();
            // Check if we should open or close the optimizer phase
            let should_open = guard
                .as_ref()
                .map(|ctx| ctx.phase != PlanningPhase::Optimizer)
                .unwrap_or(true);

            if should_open {
                // Get pass number, resetting if this is a new query (different parent span)
                let current_parent_id = Span::current().id();
                let pass = OPTIMIZER_PASS_TRACKER.with(|tracker| {
                    tracker.borrow_mut().get_and_increment(current_parent_id)
                });

                // Open the optimizer phase span
                let span =
                    (self.phase_span_create_fn)(phase_names::OPTIMIZE_LOGICAL_PLAN);

                // Only record pass info if span is active (lazy instrumentation)
                if !span.is_disabled() {
                    let max_passes = config.options().optimizer.max_passes;
                    span.record("datafusion.optimizer.pass", pass + 1);
                    span.record("datafusion.optimizer.max_passes", max_passes as i64);
                }

                // Only capture plan for diff if enabled AND span is active (lazy instrumentation)
                let plan_before = if self.plan_diff && !span.is_disabled() {
                    Some(plan.format_for_diff())
                } else {
                    None
                };
                *guard = Some(PlanningContext {
                    phase: PlanningPhase::Optimizer,
                    _entered: span.entered(),
                    plan_before,
                    effective_rules: Vec::new(),
                });
            } else {
                // Close the optimizer phase span
                if let Some(ctx) = guard.take() {
                    close_phase_span(ctx, &plan);
                }
            }
        });
        Ok(Transformed::no(plan))
    }
}

/// Sentinel physical optimizer rule that toggles the phase span.
/// First call opens the span, second call closes it.
struct PhysicalOptimizerPhaseSentinel {
    phase_span_create_fn: Arc<PhaseSpanCreateFn>,
    plan_diff: bool,
}

impl Debug for PhysicalOptimizerPhaseSentinel {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PhysicalOptimizerPhaseSentinel").finish()
    }
}

impl PhysicalOptimizerRule for PhysicalOptimizerPhaseSentinel {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        PLANNING_CONTEXT.with(|cell| {
            let mut guard = cell.borrow_mut();
            // Check if we should open or close the physical optimizer phase
            let should_open = guard
                .as_ref()
                .map(|ctx| ctx.phase != PlanningPhase::PhysicalOptimizer)
                .unwrap_or(true);

            if should_open {
                // Open the physical optimizer phase span
                let span =
                    (self.phase_span_create_fn)(phase_names::OPTIMIZE_PHYSICAL_PLAN);
                // Only capture plan for diff if enabled AND span is active (lazy instrumentation)
                let plan_before = if self.plan_diff && !span.is_disabled() {
                    Some(plan.format_for_diff())
                } else {
                    None
                };
                *guard = Some(PlanningContext {
                    phase: PlanningPhase::PhysicalOptimizer,
                    _entered: span.entered(),
                    plan_before,
                    effective_rules: Vec::new(),
                });
            } else {
                // Close the physical optimizer phase span
                if let Some(ctx) = guard.take() {
                    close_phase_span(ctx, &plan);
                }

                // Physical optimizer is the final planning phase - reset pass tracker
                // to ensure accurate pass counting for the next query
                OPTIMIZER_PASS_TRACKER.with(|tracker| {
                    tracker.borrow_mut().reset();
                });
            }
        });
        Ok(plan)
    }

    fn name(&self) -> &str {
        sentinel_names::PHYSICAL_OPTIMIZER
    }

    fn schema_check(&self) -> bool {
        true
    }
}

// ============================================================================
// Error-cleanup wrappers for phase_only mode
// ============================================================================
// When individual rule spans are disabled (PhaseOnly level), rules run unwrapped
// between the two sentinel rules. If a rule errors, the closing sentinel never
// runs, leaving the phase span open. These thin wrappers intercept errors and
// drop PLANNING_CONTEXT so the phase span is always closed on failure.

struct ErrorCleanupAnalyzerRule {
    inner: Arc<dyn AnalyzerRule + Send + Sync>,
}

impl Debug for ErrorCleanupAnalyzerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorCleanupAnalyzerRule").finish()
    }
}

impl AnalyzerRule for ErrorCleanupAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let result = self.inner.analyze(plan, config);
        if result.is_err() {
            drop_planning_context();
        }
        result
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

struct ErrorCleanupOptimizerRule {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
}

impl Debug for ErrorCleanupOptimizerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorCleanupOptimizerRule").finish()
    }
}

impl OptimizerRule for ErrorCleanupOptimizerRule {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        self.inner.apply_order()
    }

    #[allow(deprecated)]
    fn supports_rewrite(&self) -> bool {
        self.inner.supports_rewrite()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        let result = self.inner.rewrite(plan, config);
        if result.is_err() {
            drop_planning_context();
        }
        result
    }
}

struct ErrorCleanupPhysicalOptimizerRule {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
}

impl Debug for ErrorCleanupPhysicalOptimizerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ErrorCleanupPhysicalOptimizerRule").finish()
    }
}

impl PhysicalOptimizerRule for ErrorCleanupPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let result = self.inner.optimize(plan, config);
        if result.is_err() {
            drop_planning_context();
        }
        result
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }
}

/// Applies an optimizer rule across the tree manually, keeping all work under a single span.
///
/// When a rule specifies `apply_order()` as `Some(TopDown)` or `Some(BottomUp)`, the optimizer
/// framework would normally handle tree traversal, calling the rule's `rewrite` method on each
/// node. This would result in one span per node if we simply wrapped the inner rule.
///
/// Instead, `SingleSpanTreeTraverser` takes over tree traversal so that the entire rule
/// application (across all nodes) is consolidated under a single tracing span. This gives
/// cleaner traces that show one span per rule rather than one span per (rule × node) combination.
struct SingleSpanTreeTraverser<'a> {
    apply_order: ApplyOrder,
    rule: &'a dyn OptimizerRule,
    config: &'a dyn OptimizerConfig,
}

impl<'a> SingleSpanTreeTraverser<'a> {
    fn new(
        apply_order: ApplyOrder,
        rule: &'a dyn OptimizerRule,
        config: &'a dyn OptimizerConfig,
    ) -> Self {
        Self {
            apply_order,
            rule,
            config,
        }
    }
}

impl TreeNodeRewriter for SingleSpanTreeTraverser<'_> {
    type Node = LogicalPlan;

    fn f_down(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::TopDown {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }

    fn f_up(&mut self, node: LogicalPlan) -> Result<Transformed<LogicalPlan>> {
        if self.apply_order == ApplyOrder::BottomUp {
            self.rule.rewrite(node, self.config)
        } else {
            Ok(Transformed::no(node))
        }
    }
}

/// Generates a unified diff between two plan strings.
fn generate_plan_diff(before: &str, after: &str) -> String {
    let diff = TextDiff::from_lines(before, after);
    let mut output = String::new();

    for change in diff.iter_all_changes() {
        let sign = match change.tag() {
            ChangeTag::Delete => "-",
            ChangeTag::Insert => "+",
            ChangeTag::Equal => " ",
        };
        output.push_str(&format!("{}{}", sign, change));
    }

    output
}

// ============================================================================
// Modification detection helper
// ============================================================================

/// Detects if a plan was modified and updates span attributes accordingly.
///
/// This function:
/// 1. Compares plan string representations to detect actual changes
/// 2. Generates and records a unified diff if `record_diff` is true
/// 3. Updates the span's `otel.name` to indicate modification
/// 4. Records the rule name in the planning context for effective_rules tracking
fn detect_and_record_modification(
    before_str: &str,
    after_str: &str,
    span: &Span,
    record_diff: bool,
    rule_name: &str,
) {
    if before_str == after_str {
        return;
    }

    // Plan was modified - record diff if enabled
    if record_diff {
        let diff = generate_plan_diff(before_str, after_str);
        span.record("datafusion.plan_diff", diff);
    }

    // Update span name to indicate modification
    span.record("otel.name", format!("{} (modified)", rule_name));

    // Record in unified planning context for effective_rules tracking
    record_modified_rule_in_context(rule_name);
}

/// Function type that creates a span for a rule, given the rule name.
pub(crate) type RuleSpanCreateFn = dyn Fn(&str) -> Span + Send + Sync;

/// A wrapper for an `AnalyzerRule` that adds tracing instrumentation.
struct InstrumentedAnalyzerRule {
    inner: Arc<dyn AnalyzerRule + Send + Sync>,
    options: RuleInstrumentationOptions,
    span_create_fn: Arc<RuleSpanCreateFn>,
}

impl Debug for InstrumentedAnalyzerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedAnalyzerRule")
            .field("inner", &self.inner)
            .field("options", &self.options)
            .finish()
    }
}

impl InstrumentedAnalyzerRule {
    /// Creates a new instrumented analyzer rule.
    fn new(
        inner: Arc<dyn AnalyzerRule + Send + Sync>,
        options: RuleInstrumentationOptions,
        span_create_fn: Arc<RuleSpanCreateFn>,
    ) -> Self {
        Self {
            inner,
            options,
            span_create_fn,
        }
    }
}

impl AnalyzerRule for InstrumentedAnalyzerRule {
    fn analyze(&self, plan: LogicalPlan, config: &ConfigOptions) -> Result<LogicalPlan> {
        let span = (self.span_create_fn)(self.name());
        let enter = span.enter();

        // Only clone if we need to track modifications and span is active (lazy instrumentation)
        let plan_before = if !span.is_disabled() {
            Some(plan.clone())
        } else {
            None
        };

        let result = self.inner.analyze(plan, config);

        // Only perform expensive modification detection if span is active
        if !span.is_disabled() {
            match &result {
                Ok(plan_after) => {
                    if let Some(plan_before) = plan_before {
                        // Check if the plan was modified (quick check first)
                        if &plan_before != plan_after {
                            // Use helper for string comparison and recording
                            let before_str = plan_before.format_for_diff();
                            let after_str = plan_after.format_for_diff();
                            detect_and_record_modification(
                                &before_str,
                                &after_str,
                                &span,
                                self.options.plan_diff,
                                self.name(),
                            );
                        }
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "AnalyzerRule failed");
                }
            }
        }

        if result.is_err() {
            // Exit the rule span before closing the phase span to maintain
            // proper nesting order: inner spans must exit before outer spans.
            drop(enter);
            drop_planning_context();
        }

        result
    }

    fn name(&self) -> &str {
        self.inner.name()
    }
}

/// A wrapper for an `OptimizerRule` that adds tracing instrumentation.
struct InstrumentedOptimizerRule {
    inner: Arc<dyn OptimizerRule + Send + Sync>,
    options: RuleInstrumentationOptions,
    span_create_fn: Arc<RuleSpanCreateFn>,
}

impl Debug for InstrumentedOptimizerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedOptimizerRule")
            .field("inner", &self.inner)
            .field("options", &self.options)
            .finish()
    }
}

impl InstrumentedOptimizerRule {
    /// Creates a new instrumented optimizer rule.
    fn new(
        inner: Arc<dyn OptimizerRule + Send + Sync>,
        options: RuleInstrumentationOptions,
        span_create_fn: Arc<RuleSpanCreateFn>,
    ) -> Self {
        Self {
            inner,
            options,
            span_create_fn,
        }
    }

    /// Applies the inner rule to the plan, handling tree traversal appropriately.
    ///
    /// If the inner rule specifies an `apply_order`, we perform manual tree traversal
    /// using `SingleSpanTreeTraverser` to keep all work under a single span. Otherwise,
    /// we delegate directly to the inner rule's `rewrite` method.
    fn apply_inner(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        match self.inner.apply_order() {
            // Inner rule wants the optimizer to handle recursion.
            // We do it ourselves to keep everything under one span.
            Some(apply_order) => {
                plan.rewrite_with_subqueries(&mut SingleSpanTreeTraverser::new(
                    apply_order,
                    self.inner.as_ref(),
                    config,
                ))
            }
            // Inner rule handles its own recursion
            None => self.inner.rewrite(plan, config),
        }
    }
}

impl OptimizerRule for InstrumentedOptimizerRule {
    fn name(&self) -> &str {
        self.inner.name()
    }

    fn apply_order(&self) -> Option<ApplyOrder> {
        // Always return None - we handle the tree traversal ourselves.
        // This ensures we get a single span per rule application instead of
        // one span per node when the optimizer would normally handle recursion.
        None
    }

    #[allow(deprecated)]
    fn supports_rewrite(&self) -> bool {
        self.inner.supports_rewrite()
    }

    fn rewrite(
        &self,
        plan: LogicalPlan,
        config: &dyn OptimizerConfig,
    ) -> Result<Transformed<LogicalPlan>, DataFusionError> {
        // Create a single span for the entire rule application
        let span = (self.span_create_fn)(self.name());
        let enter = span.enter();

        // Clone plan when span is active. We defer string formatting until after we know
        // the rule claimed to modify the plan (transformed=true), avoiding expensive
        // formatting when most rules don't modify most plans.
        let plan_before = if !span.is_disabled() {
            Some(plan.clone())
        } else {
            None
        };

        // Apply the rule using our extracted traversal logic
        let result = self.apply_inner(plan, config);

        // Only perform modification detection if span is active
        if !span.is_disabled() {
            match &result {
                Ok(transformed) => {
                    // We trust transformed=false (rule says it didn't modify) as an optimization
                    // to skip string comparison. However, we verify when transformed=true because
                    // rules may report false positives (claim modified when actually unchanged).
                    if transformed.transformed
                        && let Some(before) = plan_before
                    {
                        let before_str = before.format_for_diff();
                        let after_str = transformed.data.format_for_diff();
                        detect_and_record_modification(
                            &before_str,
                            &after_str,
                            &span,
                            self.options.plan_diff,
                            self.name(),
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "OptimizerRule failed");
                }
            }
        }

        if result.is_err() {
            drop(enter);
            drop_planning_context();
        }

        result
    }
}

/// A wrapper for a `PhysicalOptimizerRule` that adds tracing instrumentation.
struct InstrumentedPhysicalOptimizerRule {
    inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
    options: RuleInstrumentationOptions,
    span_create_fn: Arc<RuleSpanCreateFn>,
}

impl Debug for InstrumentedPhysicalOptimizerRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("InstrumentedPhysicalOptimizerRule")
            .field("inner", &self.inner)
            .field("options", &self.options)
            .finish()
    }
}

impl InstrumentedPhysicalOptimizerRule {
    /// Creates a new instrumented physical optimizer rule.
    fn new(
        inner: Arc<dyn PhysicalOptimizerRule + Send + Sync>,
        options: RuleInstrumentationOptions,
        span_create_fn: Arc<RuleSpanCreateFn>,
    ) -> Self {
        Self {
            inner,
            options,
            span_create_fn,
        }
    }
}

impl PhysicalOptimizerRule for InstrumentedPhysicalOptimizerRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let span = (self.span_create_fn)(self.name());
        let enter = span.enter();

        // Only clone the Arc when span is active. We defer string formatting until
        // after ptr_eq check to avoid expensive formatting when the plan wasn't modified.
        // This is safe because ExecutionPlan is immutable through Arc - the clone still
        // points to the original plan even after the rule returns a new Arc.
        let plan_clone = if !span.is_disabled() {
            Some(Arc::clone(&plan))
        } else {
            None
        };

        let result = self.inner.optimize(plan, config);

        // Modification detection - plan_clone is Some iff span is enabled
        if let Some(old_plan) = plan_clone {
            match &result {
                Ok(new_plan) => {
                    // Check if the plan was modified (different Arc means new plan was created)
                    if !Arc::ptr_eq(&old_plan, new_plan) {
                        // Only format strings when we know the Arc changed
                        let before_str = old_plan.format_for_diff();
                        let after_str = new_plan.format_for_diff();
                        detect_and_record_modification(
                            &before_str,
                            &after_str,
                            &span,
                            self.options.plan_diff,
                            self.name(),
                        );
                    }
                }
                Err(e) => {
                    tracing::error!(error = %e, "PhysicalOptimizerRule failed");
                }
            }
        }

        if result.is_err() {
            drop(enter);
            drop_planning_context();
        }

        result
    }

    fn name(&self) -> &str {
        self.inner.name()
    }

    fn schema_check(&self) -> bool {
        self.inner.schema_check()
    }
}

/// Instruments a `SessionState` with tracing for all rule phases.
///
/// This is the internal function called by the `instrument_rules_with_*_spans!` macros.
/// It wraps analyzer, optimizer, and physical optimizer rules with tracing spans,
/// and automatically instruments the query planner when physical optimizer is enabled.
#[doc(hidden)]
pub fn instrument_session_state(
    state: SessionState,
    options: RuleInstrumentationOptions,
    span_create_fn: Arc<RuleSpanCreateFn>,
    phase_span_create_fn: Arc<PhaseSpanCreateFn>,
    span_level: Level,
) -> SessionState {
    // Instrument Analyzer rules
    let analyzers = instrument_analyzer_rules(
        state.analyzer().rules.clone(),
        &options,
        &span_create_fn,
        &phase_span_create_fn,
    );

    // Instrument Optimizer rules
    let optimizers = instrument_optimizer_rules(
        Vec::from(state.optimizers()),
        &options,
        &span_create_fn,
        &phase_span_create_fn,
    );

    // Instrument Physical Optimizer rules
    let physical_optimizers = instrument_physical_optimizer_rules(
        Vec::from(state.physical_optimizers()),
        &options,
        &span_create_fn,
        &phase_span_create_fn,
    );

    // Rebuild SessionState with instrumented rules
    let state = SessionStateBuilder::from(state)
        .with_analyzer_rules(analyzers)
        .with_optimizer_rules(optimizers)
        .with_physical_optimizer_rules(physical_optimizers)
        .build();

    // Automatically instrument the query planner when physical optimizer is enabled
    if options.physical_optimizer.phase_span_enabled() {
        TracingQueryPlanner::instrument_state_with_level(state, span_level)
    } else {
        state
    }
}

/// Instruments analyzer rules with phase sentinel and optional rule-level spans.
fn instrument_analyzer_rules(
    rules: Vec<Arc<dyn AnalyzerRule + Send + Sync>>,
    options: &RuleInstrumentationOptions,
    span_create_fn: &Arc<RuleSpanCreateFn>,
    phase_span_create_fn: &Arc<PhaseSpanCreateFn>,
) -> Vec<Arc<dyn AnalyzerRule + Send + Sync>> {
    let level = options.analyzer;
    if !level.phase_span_enabled() || rules.is_empty() {
        return rules;
    }

    let mut result = Vec::with_capacity(rules.len() + 2);

    // Sentinel at start opens the phase span
    result.push(Arc::new(AnalyzerPhaseSentinel {
        phase_span_create_fn: phase_span_create_fn.clone(),
        plan_diff: options.plan_diff,
    }) as Arc<dyn AnalyzerRule + Send + Sync>);

    // Wrap rules with instrumentation (if Full) or error-cleanup only (if PhaseOnly).
    // Even in PhaseOnly mode we wrap to ensure the phase span is closed on error,
    // since the closing sentinel never runs when a rule returns Err.
    for rule in rules {
        if level.rule_spans_enabled() {
            result.push(Arc::new(InstrumentedAnalyzerRule::new(
                rule,
                options.clone(),
                span_create_fn.clone(),
            )) as Arc<dyn AnalyzerRule + Send + Sync>);
        } else {
            result.push(Arc::new(ErrorCleanupAnalyzerRule { inner: rule })
                as Arc<dyn AnalyzerRule + Send + Sync>);
        }
    }

    // Sentinel at end closes the phase span
    result.push(Arc::new(AnalyzerPhaseSentinel {
        phase_span_create_fn: phase_span_create_fn.clone(),
        plan_diff: options.plan_diff,
    }) as Arc<dyn AnalyzerRule + Send + Sync>);

    result
}

/// Instruments optimizer rules with phase sentinel and optional rule-level spans.
fn instrument_optimizer_rules(
    rules: Vec<Arc<dyn OptimizerRule + Send + Sync>>,
    options: &RuleInstrumentationOptions,
    span_create_fn: &Arc<RuleSpanCreateFn>,
    phase_span_create_fn: &Arc<PhaseSpanCreateFn>,
) -> Vec<Arc<dyn OptimizerRule + Send + Sync>> {
    let level = options.optimizer;
    if !level.phase_span_enabled() || rules.is_empty() {
        return rules;
    }

    let mut result = Vec::with_capacity(rules.len() + 2);

    // Sentinel at start opens the phase span
    result.push(Arc::new(OptimizerPhaseSentinel {
        phase_span_create_fn: phase_span_create_fn.clone(),
        plan_diff: options.plan_diff,
    }) as Arc<dyn OptimizerRule + Send + Sync>);

    // Wrap rules with instrumentation (if Full) or error-cleanup only (if PhaseOnly).
    for rule in rules {
        if level.rule_spans_enabled() {
            result.push(Arc::new(InstrumentedOptimizerRule::new(
                rule,
                options.clone(),
                span_create_fn.clone(),
            )) as Arc<dyn OptimizerRule + Send + Sync>);
        } else {
            result.push(Arc::new(ErrorCleanupOptimizerRule { inner: rule })
                as Arc<dyn OptimizerRule + Send + Sync>);
        }
    }

    // Sentinel at end closes the phase span
    result.push(Arc::new(OptimizerPhaseSentinel {
        phase_span_create_fn: phase_span_create_fn.clone(),
        plan_diff: options.plan_diff,
    }) as Arc<dyn OptimizerRule + Send + Sync>);

    result
}

/// Instruments physical optimizer rules with phase sentinel and optional rule-level spans.
fn instrument_physical_optimizer_rules(
    rules: Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>>,
    options: &RuleInstrumentationOptions,
    span_create_fn: &Arc<RuleSpanCreateFn>,
    phase_span_create_fn: &Arc<PhaseSpanCreateFn>,
) -> Vec<Arc<dyn PhysicalOptimizerRule + Send + Sync>> {
    let level = options.physical_optimizer;
    if !level.phase_span_enabled() || rules.is_empty() {
        return rules;
    }

    let mut result = Vec::with_capacity(rules.len() + 2);

    // Sentinel at start opens the phase span
    result.push(Arc::new(PhysicalOptimizerPhaseSentinel {
        phase_span_create_fn: phase_span_create_fn.clone(),
        plan_diff: options.plan_diff,
    }) as Arc<dyn PhysicalOptimizerRule + Send + Sync>);

    // Wrap rules with instrumentation (if Full) or error-cleanup only (if PhaseOnly).
    for rule in rules {
        if level.rule_spans_enabled() {
            result.push(Arc::new(InstrumentedPhysicalOptimizerRule::new(
                rule,
                options.clone(),
                span_create_fn.clone(),
            ))
                as Arc<dyn PhysicalOptimizerRule + Send + Sync>);
        } else {
            result.push(Arc::new(ErrorCleanupPhysicalOptimizerRule { inner: rule })
                as Arc<dyn PhysicalOptimizerRule + Send + Sync>);
        }
    }

    // Sentinel at end closes the phase span
    result.push(Arc::new(PhysicalOptimizerPhaseSentinel {
        phase_span_create_fn: phase_span_create_fn.clone(),
        plan_diff: options.plan_diff,
    }) as Arc<dyn PhysicalOptimizerRule + Send + Sync>);

    result
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::DataFusionError;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::{SessionConfig, SessionContext};
    use std::fmt;
    use std::sync::atomic::{AtomicBool, Ordering};
    use std::sync::{Arc, Mutex};
    use tracing::Instrument as _;
    use tracing::field::{Field, Visit};
    use tracing::{Id, Subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    // -----------------------------------------------------------------------
    // Minimal span-event capture layer
    // -----------------------------------------------------------------------

    struct CapturedName(String);

    #[derive(Clone, Debug)]
    struct SpanEvent {
        kind: &'static str, // "open" | "close"
        name: String,
        parent: Option<String>,
    }

    #[derive(Clone, Default)]
    struct SpanCapture(Arc<Mutex<Vec<SpanEvent>>>);

    impl SpanCapture {
        fn snapshot(&self) -> Vec<SpanEvent> {
            self.0.lock().unwrap().clone()
        }
    }

    struct OtelNameVisitor(Option<String>);
    impl Visit for OtelNameVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            if field.name() == "otel.name" {
                self.0 = Some(value.to_owned());
            }
        }
        fn record_debug(&mut self, _: &Field, _: &dyn Debug) {}
    }

    impl<S: Subscriber + for<'s> LookupSpan<'s>> Layer<S> for SpanCapture {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &Id,
            ctx: Context<'_, S>,
        ) {
            let mut v = OtelNameVisitor(None);
            attrs.record(&mut v);
            let name = v.0.unwrap_or_else(|| attrs.metadata().name().to_owned());

            if let Some(span) = ctx.span(id) {
                span.extensions_mut().insert(CapturedName(name.clone()));
            }

            let parent = ctx.span(id).and_then(|span| {
                span.parent().and_then(|p| {
                    p.extensions().get::<CapturedName>().map(|n| n.0.clone())
                })
            });

            self.0.lock().unwrap().push(SpanEvent {
                kind: "open",
                name,
                parent,
            });
        }

        fn on_close(&self, id: Id, ctx: Context<'_, S>) {
            let name = ctx
                .span(&id)
                .and_then(|s| s.extensions().get::<CapturedName>().map(|n| n.0.clone()))
                .unwrap_or_default();
            self.0.lock().unwrap().push(SpanEvent {
                kind: "close",
                name,
                parent: None,
            });
        }
    }

    // -----------------------------------------------------------------------
    // Failing-once analyzer rule
    // -----------------------------------------------------------------------

    struct FailOnceAnalyzer(Arc<AtomicBool>);

    impl Debug for FailOnceAnalyzer {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.write_str("FailOnceAnalyzer")
        }
    }

    impl AnalyzerRule for FailOnceAnalyzer {
        fn analyze(&self, plan: LogicalPlan, _: &ConfigOptions) -> Result<LogicalPlan> {
            if self.0.swap(false, Ordering::SeqCst) {
                return Err(DataFusionError::Internal(
                    "intentional analyzer failure".into(),
                ));
            }
            Ok(plan)
        }

        fn name(&self) -> &str {
            "fail_once"
        }
    }

    fn make_ctx_with_fail_once(options: RuleInstrumentationOptions) -> SessionContext {
        let state = SessionStateBuilder::new()
            .with_config(SessionConfig::new())
            .with_default_features()
            .with_analyzer_rule(Arc::new(FailOnceAnalyzer(Arc::new(AtomicBool::new(
                true,
            )))))
            .build();
        let state = crate::instrument_rules_with_trace_spans!(
            options: options,
            state: state
        );
        SessionContext::new_with_state(state)
    }

    fn is_planning_context_open() -> bool {
        PLANNING_CONTEXT.with(|cell| cell.borrow().is_some())
    }

    fn count(events: &[SpanEvent], kind: &str, name: &str) -> usize {
        events
            .iter()
            .filter(|e| e.kind == kind && e.name == name)
            .count()
    }

    // -----------------------------------------------------------------------
    // Helper: run a query end-to-end (sql + collect), return the first error.
    // The analyzer runs during collect(), not sql(), so we need both steps.
    // -----------------------------------------------------------------------

    async fn run_query(ctx: &SessionContext) -> Result<()> {
        ctx.sql("SELECT 1").await?.collect().await?;
        Ok(())
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// Regression: phase_only mode must close the phase span when an analyzer
    /// rule returns an error (the closing sentinel never runs in that case).
    #[tokio::test]
    async fn phase_only_closes_phase_span_on_analyzer_error() {
        let ctx = make_ctx_with_fail_once(RuleInstrumentationOptions::phase_only());
        let result = run_query(&ctx).await;
        assert!(result.is_err(), "expected analyzer failure");
        assert!(
            !is_planning_context_open(),
            "phase span must not be left open after analyzer error (phase_only mode)"
        );
    }

    /// Regression: full instrumentation mode must also close the phase span on
    /// analyzer error.
    #[tokio::test]
    async fn full_closes_phase_span_on_analyzer_error() {
        let ctx = make_ctx_with_fail_once(RuleInstrumentationOptions::full());
        let result = run_query(&ctx).await;
        assert!(result.is_err(), "expected analyzer failure");
        assert!(
            !is_planning_context_open(),
            "phase span must not be left open after analyzer error (full mode)"
        );
    }

    /// Regression: a successful query that follows a failed one must not be
    /// parented under the stale phase span from the failure, and must leave no
    /// open spans behind.
    ///
    /// The analyzer runs during `collect()`, not `sql()`, so phase spans are
    /// emitted there. We wrap the successful query in `successful_query` and
    /// assert it is not parented under `analyze_logical_plan`.
    #[tokio::test]
    async fn stale_phase_span_not_parent_of_subsequent_query() {
        let capture = SpanCapture::default();
        let subscriber = tracing_subscriber::registry()
            .with(tracing_subscriber::filter::LevelFilter::TRACE)
            .with(capture.clone());
        let _guard = tracing::subscriber::set_default(subscriber);

        let ctx = make_ctx_with_fail_once(RuleInstrumentationOptions::phase_only());

        // First query: fails in the analyzer (during collect)
        let _ = run_query(&ctx).await;

        let after_failure = capture.snapshot();
        let opened_after_failure = count(&after_failure, "open", "analyze_logical_plan");
        let closed_after_failure = count(&after_failure, "close", "analyze_logical_plan");
        assert_eq!(
            opened_after_failure, 1,
            "one phase span opened during failed query"
        );
        assert_eq!(
            closed_after_failure, 1,
            "phase span must be closed immediately after analyzer error"
        );

        // Second query: must succeed without being nested under a stale span.
        // Without the fix, the still-entered analyze_logical_plan guard would
        // make it the current span, so successful_query would be parented under it.
        async {
            ctx.sql("SELECT 1").await.unwrap().collect().await.unwrap();
        }
        .instrument(tracing::info_span!("successful_query"))
        .await;

        let after_success = capture.snapshot();

        // Every opened phase span must have been closed
        let total_opened = count(&after_success, "open", "analyze_logical_plan");
        let total_closed = count(&after_success, "close", "analyze_logical_plan");
        assert_eq!(
            total_opened, total_closed,
            "every analyze_logical_plan span must be closed"
        );

        // The successful_query span must NOT be a child of analyze_logical_plan
        let successful_query_parent = after_success
            .iter()
            .find(|e| e.kind == "open" && e.name == "successful_query")
            .and_then(|e| e.parent.as_deref());
        assert_ne!(
            successful_query_parent,
            Some("analyze_logical_plan"),
            "successful_query must not be parented under the stale analyze_logical_plan span"
        );
    }
}
