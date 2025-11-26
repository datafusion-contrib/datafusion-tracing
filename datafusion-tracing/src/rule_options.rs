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

/// Instrumentation level for a phase (analyzer, optimizer, or physical optimizer).
///
/// Rule spans always require a parent phase span, so the levels are hierarchical:
/// - `Disabled`: no spans (default)
/// - `PhaseOnly`: only the phase span
/// - `Full`: phase span + individual rule spans
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum InstrumentationLevel {
    /// No instrumentation for this phase (default).
    #[default]
    Disabled,
    /// Only phase-level span (e.g., "optimize_logical_plan"), no individual rule spans.
    /// Useful for reducing trace verbosity while still tracking phase timing.
    PhaseOnly,
    /// Full instrumentation: phase span + individual rule spans.
    Full,
}

impl InstrumentationLevel {
    /// Returns true if the phase span should be created.
    pub fn phase_span_enabled(&self) -> bool {
        matches!(self, Self::PhaseOnly | Self::Full)
    }

    /// Returns true if individual rule spans should be created.
    pub fn rule_spans_enabled(&self) -> bool {
        matches!(self, Self::Full)
    }
}

/// Configuration options for instrumented DataFusion rules (Analyzer, Optimizer, Physical Optimizer).
#[derive(Clone, Debug)]
pub struct RuleInstrumentationOptions {
    /// Whether to include a unified diff of plan changes in the span.
    pub(crate) plan_diff: bool,

    /// Instrumentation level for analyzer rules.
    pub(crate) analyzer: InstrumentationLevel,

    /// Instrumentation level for logical optimizer rules.
    pub(crate) optimizer: InstrumentationLevel,

    /// Instrumentation level for physical optimizer rules.
    pub(crate) physical_optimizer: InstrumentationLevel,
}

impl Default for RuleInstrumentationOptions {
    fn default() -> Self {
        Self {
            plan_diff: false,
            analyzer: InstrumentationLevel::Disabled,
            optimizer: InstrumentationLevel::Disabled,
            physical_optimizer: InstrumentationLevel::Disabled,
        }
    }
}

impl RuleInstrumentationOptions {
    /// Creates a new builder for `RuleInstrumentationOptions`.
    pub fn builder() -> RuleInstrumentationOptionsBuilder {
        RuleInstrumentationOptionsBuilder::default()
    }

    /// Creates options with all phases enabled at `Full` level.
    ///
    /// This is a convenience constructor for the common case of enabling
    /// full instrumentation for all rule phases.
    ///
    /// # Example
    ///
    /// ```
    /// use datafusion_tracing::RuleInstrumentationOptions;
    ///
    /// let options = RuleInstrumentationOptions::full();
    /// ```
    pub fn full() -> Self {
        Self {
            plan_diff: false,
            analyzer: InstrumentationLevel::Full,
            optimizer: InstrumentationLevel::Full,
            physical_optimizer: InstrumentationLevel::Full,
        }
    }

    /// Creates options with all phases enabled at `PhaseOnly` level.
    ///
    /// This creates phase-level spans without individual rule spans,
    /// useful for reducing trace verbosity while still tracking phase timing.
    pub fn phase_only() -> Self {
        Self {
            plan_diff: false,
            analyzer: InstrumentationLevel::PhaseOnly,
            optimizer: InstrumentationLevel::PhaseOnly,
            physical_optimizer: InstrumentationLevel::PhaseOnly,
        }
    }

    /// Returns a new options with plan diff enabled.
    pub fn with_plan_diff(mut self) -> Self {
        self.plan_diff = true;
        self
    }
}

/// The builder for `RuleInstrumentationOptions`.
#[derive(Default)]
pub struct RuleInstrumentationOptionsBuilder {
    plan_diff: bool,
    analyzer: InstrumentationLevel,
    optimizer: InstrumentationLevel,
    physical_optimizer: InstrumentationLevel,
}

impl RuleInstrumentationOptionsBuilder {
    /// Enables plan diff in spans.
    pub fn plan_diff(mut self) -> Self {
        self.plan_diff = true;
        self
    }

    /// Enables all phases at `Full` level.
    pub fn all(mut self) -> Self {
        self.analyzer = InstrumentationLevel::Full;
        self.optimizer = InstrumentationLevel::Full;
        self.physical_optimizer = InstrumentationLevel::Full;
        self
    }

    /// Enables all phases at `PhaseOnly` level.
    pub fn all_phase_only(mut self) -> Self {
        self.analyzer = InstrumentationLevel::PhaseOnly;
        self.optimizer = InstrumentationLevel::PhaseOnly;
        self.physical_optimizer = InstrumentationLevel::PhaseOnly;
        self
    }

    /// Enables analyzer instrumentation at `Full` level.
    pub fn analyzer(mut self) -> Self {
        self.analyzer = InstrumentationLevel::Full;
        self
    }

    /// Enables analyzer instrumentation at `PhaseOnly` level.
    pub fn analyzer_phase_only(mut self) -> Self {
        self.analyzer = InstrumentationLevel::PhaseOnly;
        self
    }

    /// Enables logical optimizer instrumentation at `Full` level.
    pub fn optimizer(mut self) -> Self {
        self.optimizer = InstrumentationLevel::Full;
        self
    }

    /// Enables logical optimizer instrumentation at `PhaseOnly` level.
    pub fn optimizer_phase_only(mut self) -> Self {
        self.optimizer = InstrumentationLevel::PhaseOnly;
        self
    }

    /// Enables physical optimizer instrumentation at `Full` level.
    pub fn physical_optimizer(mut self) -> Self {
        self.physical_optimizer = InstrumentationLevel::Full;
        self
    }

    /// Enables physical optimizer instrumentation at `PhaseOnly` level.
    pub fn physical_optimizer_phase_only(mut self) -> Self {
        self.physical_optimizer = InstrumentationLevel::PhaseOnly;
        self
    }

    /// Consumes the builder and creates a `RuleInstrumentationOptions` instance.
    pub fn build(self) -> RuleInstrumentationOptions {
        RuleInstrumentationOptions {
            plan_diff: self.plan_diff,
            analyzer: self.analyzer,
            optimizer: self.optimizer,
            physical_optimizer: self.physical_optimizer,
        }
    }
}
