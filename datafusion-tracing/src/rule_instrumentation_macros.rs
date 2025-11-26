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

/// Instruments a `SessionState` with tracing spans for all rule phases.
///
/// This macro wraps analyzer, optimizer, and physical optimizer rules with tracing spans
/// at the specified level. Rule spans are grouped under phase spans
/// ("analyze_logical_plan", "optimize_logical_plan", "optimize_physical_plan").
///
/// When physical optimizer instrumentation is enabled, the query planner is also
/// automatically instrumented to trace physical plan creation.
///
/// # Syntax
///
/// ```ignore
/// instrument_rules_with_spans!(
///     $level,
///     options: $options,
///     state: $session_state,
///     $($fields)*
/// )
/// ```
///
/// # Arguments
///
/// * `$level` - The tracing level (e.g., `tracing::Level::INFO`)
/// * `options` - A `RuleInstrumentationOptions` instance
/// * `state` - A `SessionState` to instrument
/// * `$fields` - Optional additional span fields
///
/// # Example
///
/// ```rust,ignore
/// use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};
///
/// let options = RuleInstrumentationOptions::full().with_plan_diff();
/// let session_state = instrument_rules_with_info_spans!(
///     options: options,
///     state: session_state
/// );
/// ```
#[macro_export]
macro_rules! instrument_rules_with_spans {
    (target: $target:expr, $lvl:expr, options: $options:expr, state: $state:expr, $($fields:tt)*) => {{
        let options = $options;
        let span_create_fn = std::sync::Arc::new(move |rule_name: &str| {
            tracing::span!(
                target: $target,
                $lvl,
                "Rule",
                otel.name = rule_name,
                datafusion.plan_diff = tracing::field::Empty,
                $($fields)*
            )
        });
        let phase_span_create_fn = std::sync::Arc::new(move |phase_name: &str| {
            tracing::span!(
                target: $target,
                $lvl,
                "Phase",
                otel.name = phase_name,
                // Fields recorded by sentinels at phase close
                datafusion.effective_rules = tracing::field::Empty,
                datafusion.plan_diff = tracing::field::Empty,
                // Fields recorded by optimizer phase sentinel (pass tracking)
                datafusion.optimizer.pass = tracing::field::Empty,
                datafusion.optimizer.max_passes = tracing::field::Empty
            )
        });
        $crate::instrument_session_state($state, options, span_create_fn, phase_span_create_fn, $lvl)
    }};
    (target: $target:expr, $lvl:expr, options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_spans!(target: $target, $lvl, options: $options, state: $state,)
    };
    ($lvl:expr, options: $options:expr, state: $state:expr, $($fields:tt)*) => {
        $crate::instrument_rules_with_spans!(target: module_path!(), $lvl, options: $options, state: $state, $($fields)*)
    };
    ($lvl:expr, options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_spans!(target: module_path!(), $lvl, options: $options, state: $state)
    };
}

/// Instruments a `SessionState` with TRACE-level tracing spans.
///
/// This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
/// sets the tracing level to `TRACE`.
///
/// See [`instrument_rules_with_spans!`] for detailed documentation.
///
/// # Example
///
/// ```rust,ignore
/// use datafusion_tracing::{instrument_rules_with_trace_spans, RuleInstrumentationOptions};
///
/// let options = RuleInstrumentationOptions::full();
/// let session_state = instrument_rules_with_trace_spans!(
///     options: options,
///     state: session_state
/// );
/// ```
#[macro_export]
macro_rules! instrument_rules_with_trace_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_spans!(
            target: $target,
            tracing::Level::TRACE,
            options: $options,
            state: $state,
            $($field)*
        )
    };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_trace_spans!(target: module_path!(), options: $options, state: $state, $($field)*)
    };
    (target: $target:expr, options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_trace_spans!(target: $target, options: $options, state: $state,)
    };
    (options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_trace_spans!(target: module_path!(), options: $options, state: $state)
    };
}

/// Instruments a `SessionState` with DEBUG-level tracing spans.
///
/// This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
/// sets the tracing level to `DEBUG`.
///
/// See [`instrument_rules_with_spans!`] for detailed documentation.
///
/// # Example
///
/// ```rust,ignore
/// use datafusion_tracing::{instrument_rules_with_debug_spans, RuleInstrumentationOptions};
///
/// let options = RuleInstrumentationOptions::full();
/// let session_state = instrument_rules_with_debug_spans!(
///     options: options,
///     state: session_state
/// );
/// ```
#[macro_export]
macro_rules! instrument_rules_with_debug_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_spans!(
            target: $target,
            tracing::Level::DEBUG,
            options: $options,
            state: $state,
            $($field)*
        )
    };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_debug_spans!(target: module_path!(), options: $options, state: $state, $($field)*)
    };
    (target: $target:expr, options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_debug_spans!(target: $target, options: $options, state: $state,)
    };
    (options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_debug_spans!(target: module_path!(), options: $options, state: $state)
    };
}

/// Instruments a `SessionState` with INFO-level tracing spans.
///
/// This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
/// sets the tracing level to `INFO`.
///
/// See [`instrument_rules_with_spans!`] for detailed documentation.
///
/// # Example
///
/// ```rust,ignore
/// use datafusion_tracing::{instrument_rules_with_info_spans, RuleInstrumentationOptions};
///
/// let options = RuleInstrumentationOptions::full().with_plan_diff();
/// let session_state = instrument_rules_with_info_spans!(
///     options: options,
///     state: session_state
/// );
/// ```
#[macro_export]
macro_rules! instrument_rules_with_info_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_spans!(
            target: $target,
            tracing::Level::INFO,
            options: $options,
            state: $state,
            $($field)*
        )
    };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_info_spans!(target: module_path!(), options: $options, state: $state, $($field)*)
    };
    (target: $target:expr, options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_info_spans!(target: $target, options: $options, state: $state,)
    };
    (options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_info_spans!(target: module_path!(), options: $options, state: $state)
    };
}

/// Instruments a `SessionState` with WARN-level tracing spans.
///
/// This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
/// sets the tracing level to `WARN`.
///
/// See [`instrument_rules_with_spans!`] for detailed documentation.
///
/// # Example
///
/// ```rust,ignore
/// use datafusion_tracing::{instrument_rules_with_warn_spans, RuleInstrumentationOptions};
///
/// let options = RuleInstrumentationOptions::full();
/// let session_state = instrument_rules_with_warn_spans!(
///     options: options,
///     state: session_state
/// );
/// ```
#[macro_export]
macro_rules! instrument_rules_with_warn_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_spans!(
            target: $target,
            tracing::Level::WARN,
            options: $options,
            state: $state,
            $($field)*
        )
    };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_warn_spans!(target: module_path!(), options: $options, state: $state, $($field)*)
    };
    (target: $target:expr, options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_warn_spans!(target: $target, options: $options, state: $state,)
    };
    (options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_warn_spans!(target: module_path!(), options: $options, state: $state)
    };
}

/// Instruments a `SessionState` with ERROR-level tracing spans.
///
/// This is a convenience wrapper around [`instrument_rules_with_spans!`] that automatically
/// sets the tracing level to `ERROR`.
///
/// See [`instrument_rules_with_spans!`] for detailed documentation.
///
/// # Example
///
/// ```rust,ignore
/// use datafusion_tracing::{instrument_rules_with_error_spans, RuleInstrumentationOptions};
///
/// let options = RuleInstrumentationOptions::full();
/// let session_state = instrument_rules_with_error_spans!(
///     options: options,
///     state: session_state
/// );
/// ```
#[macro_export]
macro_rules! instrument_rules_with_error_spans {
    (target: $target:expr, options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_spans!(
            target: $target,
            tracing::Level::ERROR,
            options: $options,
            state: $state,
            $($field)*
        )
    };
    (options: $options:expr, state: $state:expr, $($field:tt)*) => {
        $crate::instrument_rules_with_error_spans!(target: module_path!(), options: $options, state: $state, $($field)*)
    };
    (target: $target:expr, options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_error_spans!(target: $target, options: $options, state: $state,)
    };
    (options: $options:expr, state: $state:expr) => {
        $crate::instrument_rules_with_error_spans!(target: module_path!(), options: $options, state: $state)
    };
}
