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

/// Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan`.
///
/// This macro is modeled after the [`span!`] macro from the [tracing] crate but is specifically designed
/// to instrument DataFusion execution plans. It creates a new `PhysicalOptimizerRule` that will wrap
/// each node of the plan in a custom `InstrumentedExec` node.
///
/// By default, it includes all known metrics fields relevant to DataFusion execution.
///
/// # Instrumentation Options
///
/// The instrumentation options are specified via the [`crate::InstrumentationOptions`] struct, which includes:
/// - `record_metrics`: Enable or disable recording of DataFusion execution metrics.
/// - `preview_limit`: Set the number of rows to preview per span (set to `0` to disable).
/// - `preview_fn`: Provide an optional callback for formatting previewed batches.
///   If unspecified, [`datafusion::arrow::util::pretty::pretty_format_batches`] will be used.
/// - `custom_fields`: Provide custom key-value pairs for additional span metadata.
///
/// # Span Fields
///
/// This macro supports the same field–value syntax used by tracing's span macros, allowing you to add additional
/// contextual information as needed.
///
/// Refer to the [tracing documentation][tracing_span] for more information on the syntax
/// accepted by these macros, as it closely follows `span!`.
///
/// # Examples
///
/// Creating a new `InstrumentRule` to wrap the plan with TRACE level spans:
/// ```rust
/// # use datafusion_tracing::{instrument_with_spans, InstrumentationOptions};
/// # use tracing::Level;
/// let instrument_rule = instrument_with_spans!(Level::TRACE, options: InstrumentationOptions::default());
/// ```
///
/// Adding additional fields to the instrumentation:
/// ```rust
/// # use datafusion_tracing::{instrument_with_spans, InstrumentationOptions};
/// # use tracing::{field, Level};
/// # use std::collections::HashMap;
/// let custom_fields = HashMap::from([
///     ("custom.key1".to_string(), "value1".to_string()),
///     ("custom.key2".to_string(), "value2".to_string()),
/// ]);
/// let options = InstrumentationOptions {
///    record_metrics: true,
///    preview_limit: 10,
///    custom_fields,
///    ..Default::default()
/// };
/// let instrument_rule = instrument_with_spans!(
///     Level::INFO,
///     options: options,
///     datafusion.additional_info = "some info",
///     datafusion.user_id = 42,
///     custom.key1 = field::Empty,
///     custom.key2 = field::Empty,
/// );
/// // The instrumentation now includes additional fields, and all spans will be tagged with:
/// // - "datafusion.additional_info": "some info"
/// // - "datafusion.user_id": 42
/// // - "custom.key1": "value1"
/// // - "custom.key2": "value2"
/// // as well as all DataFusion metrics fields, and a 10 line preview of the data.
/// ```
///
/// [tracing_span]: https://docs.rs/tracing/latest/tracing/macro.span.html
/// [`span!`]: https://docs.rs/tracing/latest/tracing/macro.span.html
///
/// **Note for crate Developers:**
///
/// The list of native datafusion metrics can be re-generated by running the following bash command at the root of the [datafusion](https://github.com/apache/datafusion) repository:
/// ```bash
/// (
///   find . -type f -name '*.rs' ! -path '*/metrics/mod.rs' -exec grep -A2 'MetricBuilder::new' {} \; | grep -E '(counter|gauge|subset_time)'
///   grep -E -o 'Self::.*=>.*"' datafusion/physical-plan/src/metrics/value.rs
/// ) | cut -s -f 2 -d '"' | sort -u | sed 's/\(.*\)/datafusion.metrics.\1 = tracing::field::Empty,/g'
/// ```
#[macro_export]
macro_rules! instrument_with_spans {
    (target: $target:expr, $lvl:expr, options: $options:expr, $($fields:tt)*) => {{
        let options = $options;
        let custom_fields = options.custom_fields.clone();
        $crate::new_instrument_rule(
            std::sync::Arc::new(move || {
                let span = tracing::span!(
                    target: $target,
                    $lvl,
                    "InstrumentedExec",
                    otel.name = tracing::field::Empty,
                    datafusion.node = tracing::field::Empty,
                    datafusion.partitioning = tracing::field::Empty,
                    datafusion.emission_type = tracing::field::Empty,
                    datafusion.boundedness = tracing::field::Empty,
                    datafusion.preview = tracing::field::Empty,
                    datafusion.preview_rows = tracing::field::Empty,
                    datafusion.metrics.bloom_filter_eval_time = tracing::field::Empty,
                    datafusion.metrics.build_input_batches = tracing::field::Empty,
                    datafusion.metrics.build_input_rows = tracing::field::Empty,
                    datafusion.metrics.build_mem_used = tracing::field::Empty,
                    datafusion.metrics.build_time = tracing::field::Empty,
                    datafusion.metrics.bytes_scanned = tracing::field::Empty,
                    datafusion.metrics.elapsed_compute = tracing::field::Empty,
                    datafusion.metrics.end_timestamp = tracing::field::Empty,
                    datafusion.metrics.fetch_time = tracing::field::Empty,
                    datafusion.metrics.file_open_errors = tracing::field::Empty,
                    datafusion.metrics.file_scan_errors = tracing::field::Empty,
                    datafusion.metrics.input_batches = tracing::field::Empty,
                    datafusion.metrics.input_rows = tracing::field::Empty,
                    datafusion.metrics.join_time = tracing::field::Empty,
                    datafusion.metrics.mem_used = tracing::field::Empty,
                    datafusion.metrics.metadata_load_time = tracing::field::Empty,
                    datafusion.metrics.num_bytes = tracing::field::Empty,
                    datafusion.metrics.num_predicate_creation_errors = tracing::field::Empty,
                    datafusion.metrics.output_batches = tracing::field::Empty,
                    datafusion.metrics.output_rows = tracing::field::Empty,
                    datafusion.metrics.page_index_eval_time = tracing::field::Empty,
                    datafusion.metrics.page_index_rows_matched = tracing::field::Empty,
                    datafusion.metrics.page_index_rows_pruned = tracing::field::Empty,
                    datafusion.metrics.peak_mem_used = tracing::field::Empty,
                    datafusion.metrics.predicate_evaluation_errors = tracing::field::Empty,
                    datafusion.metrics.pushdown_rows_matched = tracing::field::Empty,
                    datafusion.metrics.pushdown_rows_pruned = tracing::field::Empty,
                    datafusion.metrics.repartition_time = tracing::field::Empty,
                    datafusion.metrics.row_groups_matched_bloom_filter = tracing::field::Empty,
                    datafusion.metrics.row_groups_matched_statistics = tracing::field::Empty,
                    datafusion.metrics.row_groups_pruned_bloom_filter = tracing::field::Empty,
                    datafusion.metrics.row_groups_pruned_statistics = tracing::field::Empty,
                    datafusion.metrics.row_pushdown_eval_time = tracing::field::Empty,
                    datafusion.metrics.row_replacements = tracing::field::Empty,
                    datafusion.metrics.send_time = tracing::field::Empty,
                    datafusion.metrics.skipped_aggregation_rows = tracing::field::Empty,
                    datafusion.metrics.spill_count = tracing::field::Empty,
                    datafusion.metrics.spilled_bytes = tracing::field::Empty,
                    datafusion.metrics.spilled_rows = tracing::field::Empty,
                    datafusion.metrics.start_timestamp = tracing::field::Empty,
                    datafusion.metrics.statistics_eval_time = tracing::field::Empty,
                    datafusion.metrics.stream_memory_usage = tracing::field::Empty,
                    datafusion.metrics.time_elapsed_opening = tracing::field::Empty,
                    datafusion.metrics.time_elapsed_processing = tracing::field::Empty,
                    datafusion.metrics.time_elapsed_scanning_total = tracing::field::Empty,
                    datafusion.metrics.time_elapsed_scanning_until_data = tracing::field::Empty,
                    $($fields)*
                );
                for (key, value) in custom_fields.iter() {
                    span.record(key.as_str(), value);
                }
                span
            }),
            options
        )
    }};
    (target: $target:expr, $lvl:expr, options: $options:expr) => {
        $crate::instrument_with_spans!(target: $target, $lvl, options: $options,)
    };
    ($lvl:expr, options: $options:expr, $($fields:tt)*) => {
        $crate::instrument_with_spans!(target: module_path!(), $lvl, options: $options, $($fields)*)
    };
    ($lvl:expr, options: $options:expr) => {
        $crate::instrument_with_spans!(target: module_path!(), $lvl, options: $options)
    };
}

/// Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the trace level.
///
/// This macro automatically sets the tracing level to `TRACE` and is a convenience wrapper around
/// [`instrument_with_spans!`] with the appropriate log level preset.
///
/// See [`instrument_with_spans!`] for details on instrumentation options and span fields.
///
/// # Examples
///
/// Basic usage with default options:
/// ```rust
/// # use datafusion_tracing::{instrument_with_trace_spans, InstrumentationOptions};
/// let instrument_rule = instrument_with_trace_spans!(options: InstrumentationOptions::default());
/// ```
///
/// Adding custom fields:
/// ```rust
/// # use datafusion_tracing::{instrument_with_trace_spans, InstrumentationOptions};
/// # use tracing::field;
/// # use std::collections::HashMap;
/// let custom_fields = HashMap::from([
///     ("custom.key1".to_string(), "value1".to_string()),
/// ]);
/// let options = InstrumentationOptions {
///    custom_fields,
///    ..Default::default()
/// };
/// let instrument_rule = instrument_with_trace_spans!(
///     options: options,
///     datafusion.additional_info = "some info",
///     custom.key1 = field::Empty,
/// );
/// ```
///
/// [tracing_trace_span]: https://docs.rs/tracing/latest/tracing/macro.trace_span.html
/// [`trace_span!`]: https://docs.rs/tracing/latest/tracing/macro.trace_span.html
/// [`instrument_with_spans!`]: crate::instrument_with_spans!
#[macro_export]
macro_rules! instrument_with_trace_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_spans!(
            target: $target,
            tracing::Level::TRACE,
            options: $options,
            $($field)*
        )
    };
    (options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_trace_spans!(target: module_path!(), options: $options, $($field)*)
    };
    (target: $target:expr, options: $options:expr) => {
        $crate::instrument_with_trace_spans!(target: $target, options: $options, )
    };
    (options: $options:expr) => {
        $crate::instrument_with_trace_spans!(target: module_path!(), options: $options)
    };
}

/// Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the debug level.
///
/// This macro automatically sets the tracing level to `DEBUG` and is a convenience wrapper around
/// [`instrument_with_spans!`] with the appropriate log level preset.
///
/// See [`instrument_with_spans!`] for details on instrumentation options and span fields.
///
/// # Examples
///
/// Basic usage with default options:
/// ```rust
/// # use datafusion_tracing::{instrument_with_debug_spans, InstrumentationOptions};
/// let instrument_rule = instrument_with_debug_spans!(options: InstrumentationOptions::default());
/// ```
///
/// Adding custom fields:
/// ```rust
/// # use datafusion_tracing::{instrument_with_debug_spans, InstrumentationOptions};
/// # use tracing::field;
/// # use std::collections::HashMap;
/// let custom_fields = HashMap::from([
///     ("custom.key1".to_string(), "value1".to_string()),
/// ]);
/// let options = InstrumentationOptions {
///    custom_fields,
///    ..Default::default()
/// };
/// let instrument_rule = instrument_with_debug_spans!(
///     options: options,
///     datafusion.additional_info = "some info",
///     custom.key1 = field::Empty,
/// );
/// ```
///
/// [tracing_debug_span]: https://docs.rs/tracing/latest/tracing/macro.debug_span.html
/// [`debug_span!`]: https://docs.rs/tracing/latest/tracing/macro.debug_span.html
/// [`instrument_with_spans!`]: crate::instrument_with_spans!
#[macro_export]
macro_rules! instrument_with_debug_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_spans!(
            target: $target,
            tracing::Level::DEBUG,
            options: $options,
            $($field)*
        )
    };
    (options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_debug_spans!(target: module_path!(), options: $options, $($field)*)
    };
    (target: $target:expr, options: $options:expr) => {
        $crate::instrument_with_debug_spans!(target: $target, options: $options, )
    };
    (options: $options:expr) => {
        $crate::instrument_with_debug_spans!(target: module_path!(), options: $options)
    };
}

/// Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the info level.
///
/// This macro automatically sets the tracing level to `INFO` and is a convenience wrapper around
/// [`instrument_with_spans!`] with the appropriate log level preset.
///
/// See [`instrument_with_spans!`] for details on instrumentation options and span fields.
///
/// # Examples
///
/// Basic usage with default options:
/// ```rust
/// # use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};
/// let instrument_rule = instrument_with_info_spans!(options: InstrumentationOptions::default());
/// ```
///
/// Adding custom fields:
/// ```rust
/// # use datafusion_tracing::{instrument_with_info_spans, InstrumentationOptions};
/// # use tracing::field;
/// # use std::collections::HashMap;
/// let custom_fields = HashMap::from([
///     ("custom.key1".to_string(), "value1".to_string()),
/// ]);
/// let options = InstrumentationOptions {
///    custom_fields,
///    ..Default::default()
/// };
/// let instrument_rule = instrument_with_info_spans!(
///     options: options,
///     datafusion.additional_info = "some info",
///     custom.key1 = field::Empty,
/// );
/// ```
///
/// [tracing_info_span]: https://docs.rs/tracing/latest/tracing/macro.info_span.html
/// [`info_span!`]: https://docs.rs/tracing/latest/tracing/macro.info_span.html
/// [`instrument_with_spans!`]: crate::instrument_with_spans!
#[macro_export]
macro_rules! instrument_with_info_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_spans!(
            target: $target,
            tracing::Level::INFO,
            options: $options,
            $($field)*
        )
    };
    (options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_info_spans!(target: module_path!(), options: $options, $($field)*)
    };
    (target: $target:expr, options: $options:expr) => {
        $crate::instrument_with_info_spans!(target: $target, options: $options, )
    };
    (options: $options:expr) => {
        $crate::instrument_with_info_spans!(target: module_path!(), options: $options)
    };
}

/// Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the warn level.
///
/// This macro automatically sets the tracing level to `WARN` and is a convenience wrapper around
/// [`instrument_with_spans!`] with the appropriate log level preset.
///
/// See [`instrument_with_spans!`] for details on instrumentation options and span fields.
///
/// # Examples
///
/// Basic usage with default options:
/// ```rust
/// # use datafusion_tracing::{instrument_with_warn_spans, InstrumentationOptions};
/// let instrument_rule = instrument_with_warn_spans!(options: InstrumentationOptions::default());
/// ```
///
/// Adding custom fields:
/// ```rust
/// # use datafusion_tracing::{instrument_with_warn_spans, InstrumentationOptions};
/// # use tracing::field;
/// # use std::collections::HashMap;
/// let custom_fields = HashMap::from([
///     ("custom.key1".to_string(), "value1".to_string()),
/// ]);
/// let options = InstrumentationOptions {
///    custom_fields,
///    ..Default::default()
/// };
/// let instrument_rule = instrument_with_warn_spans!(
///     options: options,
///     datafusion.additional_info = "some info",
///     custom.key1 = field::Empty,
/// );
/// ```
///
/// [tracing_warn_span]: https://docs.rs/tracing/latest/tracing/macro.warn_span.html
/// [`warn_span!`]: https://docs.rs/tracing/latest/tracing/macro.warn_span.html
/// [`instrument_with_spans!`]: crate::instrument_with_spans!
#[macro_export]
macro_rules! instrument_with_warn_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_spans!(
            target: $target,
            tracing::Level::WARN,
            options: $options,
            $($field)*
        )
    };
    (options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_warn_spans!(target: module_path!(), options: $options, $($field)*)
    };
    (target: $target:expr, options: $options:expr) => {
        $crate::instrument_with_warn_spans!(target: $target, options: $options, )
    };
    (options: $options:expr) => {
        $crate::instrument_with_warn_spans!(target: module_path!(), options: $options)
    };
}

/// Constructs a new instrumentation `PhysicalOptimizerRule` for a DataFusion `ExecutionPlan` at the error level.
///
/// This macro automatically sets the tracing level to `ERROR` and is a convenience wrapper around
/// [`instrument_with_spans!`] with the appropriate log level preset.
///
/// See [`instrument_with_spans!`] for details on instrumentation options and span fields.
///
/// # Examples
///
/// Basic usage with default options:
/// ```rust
/// # use datafusion_tracing::{instrument_with_error_spans, InstrumentationOptions};
/// let instrument_rule = instrument_with_error_spans!(options: InstrumentationOptions::default());
/// ```
///
/// Adding custom fields:
/// ```rust
/// # use datafusion_tracing::{instrument_with_error_spans, InstrumentationOptions};
/// # use tracing::field;
/// # use std::collections::HashMap;
/// let custom_fields = HashMap::from([
///     ("custom.key1".to_string(), "value1".to_string()),
/// ]);
/// let options = InstrumentationOptions {
///    custom_fields,
///    ..Default::default()
/// };
/// let instrument_rule = instrument_with_error_spans!(
///     options: options,
///     datafusion.additional_info = "some info",
///     custom.key1 = field::Empty,
/// );
/// ```
///
/// [tracing_error_span]: https://docs.rs/tracing/latest/tracing/macro.error_span.html
/// [`error_span!`]: https://docs.rs/tracing/latest/tracing/macro.error_span.html
/// [`instrument_with_spans!`]: crate::instrument_with_spans!
#[macro_export]
macro_rules! instrument_with_error_spans {
    (target: $target:expr, options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_spans!(
            target: $target,
            tracing::Level::ERROR,
            options: $options,
            $($field)*
        )
    };
    (options: $options:expr, $($field:tt)*) => {
        $crate::instrument_with_error_spans!(target: module_path!(), options: $options, $($field)*)
    };
    (target: $target:expr, options: $options:expr) => {
        $crate::instrument_with_error_spans!(target: $target, options: $options, )
    };
    (options: $options:expr) => {
        $crate::instrument_with_error_spans!(target: module_path!(), options: $options)
    };
}
