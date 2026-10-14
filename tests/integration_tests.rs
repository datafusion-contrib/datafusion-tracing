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

mod test_utils;

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, Once, OnceLock};

use datafusion::arrow::array::{ArrayRef, Int64Array};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::Statistics;
use datafusion::common::stats::Precision;
use datafusion::common::tree_node::{TreeNode, TreeNodeRecursion};
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_plan::joins::{HashJoinExec, PartitionMode};
use datafusion::physical_plan::operator_statistics::{
    ExtendedStatistics, StatisticsProvider, StatisticsRegistry, StatisticsResult,
};
use datafusion::physical_plan::{ExecutionPlan, collect};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_tracing::{RuleInstrumentationOptions, instrument_rules_with_info_spans};
use insta::{assert_json_snapshot, assert_snapshot};
use integration_utils::{SessionBuilder, run_traced_query};
use serde_json::Value;
use tracing::{Instrument, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{Registry, fmt, prelude::*};

use crate::test_utils::in_memory_writer::InMemoryMakeWriter;
use crate::test_utils::insta_settings;
use crate::test_utils::insta_settings::preview_redacted_settings;

// A global buffer to store logs across tests, initialized only once.
static LOG_BUFFER: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();

// A global "once" to ensure the subscriber is initialized only once.
static SUBSCRIBER_INIT: Once = Once::new();

// Snapshot assertions read from a process-global tracing subscriber. Run these
// test cases one at a time so close-event ordering is not affected by other
// snapshot tests running concurrently.
static TEST_EXECUTION_LOCK: OnceLock<tokio::sync::Mutex<()>> = OnceLock::new();

/// A struct describing how a particular query test should be run.
#[derive(Debug, Default)]
struct QueryTestCase<'a> {
    /// The SQL query to run.
    sql_query: &'a str,
    /// Session configuration (metrics, preview, object store, etc.)
    session: SessionBuilder,
    /// Indices of spans for which preview assertions should be skipped.
    ignored_preview_spans: &'a [usize],
    /// Whether to ignore the full trace in assertions.
    ignore_full_trace: bool,
    /// Whether to collapse duplicate recursive execution close events.
    collapse_recursive_exec_duplicates: bool,
}

impl<'a> QueryTestCase<'a> {
    fn new(sql_query: &'a str) -> Self {
        Self {
            sql_query,
            ..Default::default()
        }
    }

    // --- Passthrough methods delegating to SessionBuilder ---

    fn with_object_store_collection(mut self) -> Self {
        self.session = self.session.record_object_store();
        self
    }

    fn with_metrics_collection(mut self) -> Self {
        self.session = self.session.record_metrics();
        self
    }

    fn with_row_limit(mut self, limit: usize) -> Self {
        self.session = self.session.preview_limit(limit);
        self
    }

    fn with_compact_preview(mut self) -> Self {
        self.session = self.session.compact_preview();
        self
    }

    // --- Test-specific methods ---

    fn ignore_preview_spans(mut self, spans: &'a [usize]) -> Self {
        self.ignored_preview_spans = spans;
        self
    }

    fn ignore_full_trace(mut self) -> Self {
        self.ignore_full_trace = true;
        self
    }

    fn collapse_recursive_exec_duplicates(mut self) -> Self {
        self.collapse_recursive_exec_duplicates = true;
        self
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic() -> Result<()> {
    execute_test_case("01_basic", &QueryTestCase::new("select_one")).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_metrics() -> Result<()> {
    execute_test_case(
        "02_basic_metrics",
        &QueryTestCase::new("select_one").with_metrics_collection(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_preview() -> Result<()> {
    execute_test_case(
        "03_basic_preview",
        &QueryTestCase::new("select_one").with_row_limit(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_compact_preview() -> Result<()> {
    execute_test_case(
        "04_basic_compact_preview",
        &QueryTestCase::new("select_one")
            .with_row_limit(5)
            .with_compact_preview(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_all_options() -> Result<()> {
    execute_test_case(
        "05_basic_all_options",
        &QueryTestCase::new("select_one")
            .with_metrics_collection()
            .with_row_limit(5)
            .with_compact_preview(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_object_store_all_options() -> Result<()> {
    execute_test_case(
        "06_object_store_all_options",
        &QueryTestCase::new("order_nations")
            .with_object_store_collection()
            .with_metrics_collection()
            .with_row_limit(5)
            .with_compact_preview(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_scrabble_all_options() -> Result<()> {
    execute_test_case(
        "07_scrabble_all_options",
        &QueryTestCase::new("tpch_scrabble")
            .with_metrics_collection()
            .with_row_limit(5)
            .with_compact_preview()
            // skip preview assertions for these spans as they depend on the partitioning
            // and are not deterministic
            .ignore_preview_spans(&[
                12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25,
            ])
            // ignore the full trace assertions as they are not deterministic: exact span ordering can vary for this complex query
            .ignore_full_trace(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_recursive() -> Result<()> {
    execute_test_case(
        "08_recursive",
        &QueryTestCase::new("recursive").collapse_recursive_exec_duplicates(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_recursive_all_options() -> Result<()> {
    execute_test_case(
        "09_recursive_all_options",
        &QueryTestCase::new("recursive")
            .with_metrics_collection()
            .with_row_limit(5)
            .with_compact_preview()
            .collapse_recursive_exec_duplicates(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_topk_lineitem() -> Result<()> {
    // Expect a filled `DynamicFilterPhysicalExpr` on the `DataSourceExec` "datafusion.node" field
    execute_test_case("10_topk_lineitem", &QueryTestCase::new("topk_lineitem")).await
}

/// Estimates both join inputs above the collection thresholds, unlike the tiny
/// tables' built-in statistics. Losing this provider changes the join mode.
#[derive(Debug)]
struct LargeJoinInputStatistics {
    calls: Arc<AtomicUsize>,
}

impl StatisticsProvider for LargeJoinInputStatistics {
    fn compute_statistics(
        &self,
        plan: &dyn ExecutionPlan,
        _: &[ExtendedStatistics],
    ) -> Result<StatisticsResult> {
        self.calls.fetch_add(1, Ordering::SeqCst);
        let mut statistics = Statistics::new_unknown(plan.schema().as_ref());
        statistics.num_rows = Precision::Inexact(1_000);
        // JoinSelection considers byte size before row count.
        statistics.total_byte_size = Precision::Inexact(8_192);
        Ok(StatisticsResult::Computed(ExtendedStatistics::new(
            statistics,
        )))
    }
}

async fn statistics_registry_join_mode(
    options: Option<RuleInstrumentationOptions>,
    calls: Option<Arc<AtomicUsize>>,
) -> Result<PartitionMode> {
    let mut config = SessionConfig::new().with_target_partitions(2);
    let optimizer = &mut config.options_mut().optimizer;
    optimizer.use_statistics_registry = true;
    optimizer.hash_join_single_partition_threshold = 1_024;
    optimizer.hash_join_single_partition_threshold_rows = 10;
    let mut builder = SessionStateBuilder::new()
        .with_config(config)
        .with_default_features();
    if let Some(calls) = calls {
        builder =
            builder.with_statistics_registry(StatisticsRegistry::with_providers(vec![
                Arc::new(LargeJoinInputStatistics { calls }),
            ]));
    }
    let state = builder.build();
    let state = if let Some(options) = options {
        instrument_rules_with_info_spans!(options: options, state: state)
    } else {
        state
    };
    let ctx = SessionContext::new_with_state(state);
    for (table, values) in [
        ("left_table", vec![1, 2, 3]),
        ("right_table", vec![2, 3, 4]),
    ] {
        ctx.register_batch(
            table,
            RecordBatch::try_from_iter([(
                "id",
                Arc::new(Int64Array::from(values)) as ArrayRef,
            )])?,
        )?;
    }
    let plan = ctx
        .sql("SELECT l.id FROM left_table l JOIN right_table r ON l.id = r.id")
        .await?
        .create_physical_plan()
        .await?;
    let mut modes = Vec::new();
    plan.apply(|node| {
        if let Some(join) = node.downcast_ref::<HashJoinExec>() {
            modes.push(*join.partition_mode());
        }
        Ok(TreeNodeRecursion::Continue)
    })?;
    assert_eq!(modes.len(), 1, "expected one native HashJoinExec");
    let batches = collect(plan, ctx.task_ctx()).await?;
    assert_eq!(
        batches.iter().map(|batch| batch.num_rows()).sum::<usize>(),
        2
    );
    Ok(modes[0])
}

async fn assert_statistics_registry_join_selection(
    test_name: &str,
    options: RuleInstrumentationOptions,
) -> Result<()> {
    let _test_guard = TEST_EXECUTION_LOCK
        .get_or_init(Default::default)
        .lock()
        .await;
    let log_buffer = init_tracing();
    log_buffer.lock().unwrap().clear();

    // Prove that falling back to built-in statistics would change the plan.
    assert_eq!(
        statistics_registry_join_mode(None, None).await?,
        PartitionMode::CollectLeft
    );
    // Keep the default optimizer pipeline, including the real JoinSelection.
    // Independent counters ensure baseline calls cannot mask lost forwarding.
    for instrumentation in [None, Some(options)] {
        let calls = Arc::new(AtomicUsize::new(0));
        let capture_trace = instrumentation.is_some();
        let query =
            statistics_registry_join_mode(instrumentation, Some(Arc::clone(&calls)));
        // Snapshot the traced query; the controls keep their focused assertions.
        let mode = if capture_trace {
            query
                .instrument(tracing::info_span!("test", test_name = %test_name))
                .await?
        } else {
            query.await?
        };
        assert_eq!(mode, PartitionMode::Partitioned);
        assert!(
            calls.load(Ordering::SeqCst) > 0,
            "JoinSelection must consult the custom provider"
        );
    }
    let _insta_guard = insta_settings::settings().bind_to_scope();
    assert_json_snapshot!(
        format!("{test_name}_trace"),
        captured_trace_lines(log_buffer, test_name)
    );
    Ok(())
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_phase_only_preserves_statistics_registry_join_selection() -> Result<()> {
    assert_statistics_registry_join_selection(
        "11_statistics_registry_phase_only",
        RuleInstrumentationOptions::phase_only(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn test_full_preserves_statistics_registry_join_selection() -> Result<()> {
    assert_statistics_registry_join_selection(
        "12_statistics_registry_full",
        RuleInstrumentationOptions::full(),
    )
    .await
}

/// Executes the provided [`QueryTestCase`], setting up tracing and verifying
/// log output according to its parameters.
async fn execute_test_case(test_name: &str, test_case: &QueryTestCase<'_>) -> Result<()> {
    let _test_guard = TEST_EXECUTION_LOCK
        .get_or_init(Default::default)
        .lock()
        .await;

    // Initialize tracing infrastructure and collect a log buffer.
    let log_buffer = init_tracing();
    log_buffer.lock().unwrap().clear();

    // Initialize the DataFusion session with the requested options.
    // plan_diff disabled for tests to avoid non-deterministic output
    let ctx = test_case.session.clone().build().await?;

    // Run the SQL query with tracing enabled.
    run_traced_query(&ctx, test_case.sql_query)
        .instrument(tracing::info_span!("test", test_name = %test_name))
        .await?;

    let json_lines = captured_trace_lines(log_buffer, test_name);

    // Bind insta settings for snapshot testing.
    let _insta_guard = insta_settings::settings().bind_to_scope();

    let full_trace_lines = normalize_full_trace_lines(
        test_name,
        &json_lines,
        test_case.collapse_recursive_exec_duplicates,
    );

    // If we have a preview row_limit, do dedicated assertions on the previews.
    if test_case.session.get_preview_limit() > 0 {
        let preview_lines = if test_case.collapse_recursive_exec_duplicates {
            normalize_full_trace_lines(test_name, &json_lines, false)
        } else {
            full_trace_lines.clone()
        };
        let mut preview_id = 0;
        for json_line in &preview_lines {
            if let Some(span_name) = extract_json_field_value(json_line, "otel.name")
                && let Some(preview) =
                    extract_json_field_value(json_line, "datafusion.preview")
            {
                // Only assert if we have not been asked to ignore this preview span.
                if !test_case.ignored_preview_spans.contains(&preview_id) {
                    assert_snapshot!(
                        format!("{test_name}_{:02}_{span_name}", preview_id),
                        preview
                    );
                }
                preview_id += 1;
            }
        }
    }

    // General assertion on the full trace.
    if !test_case.ignore_full_trace {
        // Redact `datafusion.preview` values as they are often non-deterministic.
        preview_redacted_settings().bind(|| {
            assert_json_snapshot!(format!("{test_name}_trace"), full_trace_lines);
        });
    }

    Ok(())
}

fn captured_trace_lines(log_buffer: &Arc<Mutex<Vec<u8>>>, test_name: &str) -> Vec<Value> {
    // Clone the buffer to avoid holding the lock while parsing and filtering.
    let logs = log_buffer.lock().unwrap().clone();
    String::from_utf8_lossy(&logs)
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON line"))
        .filter(|json_line| extract_test_name(json_line) == test_name)
        .collect()
}

fn normalize_full_trace_lines(
    test_name: &str,
    json_lines: &[Value],
    collapse_recursive_exec_duplicates: bool,
) -> Vec<Value> {
    if !test_name.contains("recursive") {
        return json_lines.to_vec();
    }

    // Recursive queries may close child execution streams in a different order
    // across runtimes. Keep the trace contents intact, but make the snapshot
    // independent of that scheduling detail.
    let mut normalized_lines = Vec::new();
    let mut recursive_exec_lines = Vec::new();
    let mut recursive_exec_insert_at = None;

    for json_line in json_lines {
        if is_recursive_exec_close(json_line) {
            recursive_exec_insert_at.get_or_insert(normalized_lines.len());
            recursive_exec_lines.push(json_line.clone());
        } else {
            normalized_lines.push(json_line.clone());
        }
    }

    recursive_exec_lines.sort_by_key(recursive_exec_sort_key);
    if collapse_recursive_exec_duplicates {
        // Without metrics or previews, repeated recursive iterations can emit
        // indistinguishable close events. Keep asserting the recursive plan
        // shape, but do not assert scheduler-dependent repeat counts.
        recursive_exec_lines.dedup_by(|left, right| {
            recursive_exec_sort_key(left) == recursive_exec_sort_key(right)
        });
    }

    if let Some(insert_at) = recursive_exec_insert_at {
        normalized_lines.splice(insert_at..insert_at, recursive_exec_lines);
    }

    normalized_lines
}

fn is_recursive_exec_close(json_line: &Value) -> bool {
    extract_json_field_value(json_line, "name").as_deref() == Some("InstrumentedExec")
        && json_line["spans"].as_array().is_some_and(|spans| {
            spans.iter().any(|span| {
                span.get("otel.name").and_then(Value::as_str)
                    == Some("RecursiveQueryExec")
            })
        })
}

fn recursive_exec_sort_key(json_line: &Value) -> String {
    let span_path = json_line["spans"]
        .as_array()
        .into_iter()
        .flatten()
        .map(|span| {
            [
                value_to_sort_string(span.get("name")),
                value_to_sort_string(span.get("otel.name")),
                value_to_sort_string(span.get("datafusion.node")),
                value_to_sort_string(span.get("datafusion.partitioning")),
            ]
            .join(":")
        })
        .collect::<Vec<_>>()
        .join("/");

    [
        span_path,
        span_field_sort_value(json_line, "name"),
        span_field_sort_value(json_line, "otel.name"),
        span_field_sort_value(json_line, "datafusion.node"),
        span_field_sort_value(json_line, "datafusion.partitioning"),
        span_field_sort_value(json_line, "datafusion.metrics.output_rows"),
        span_field_sort_value(json_line, "datafusion.metrics.output_batches"),
        span_field_sort_value(json_line, "datafusion.metrics.output_bytes"),
    ]
    .join("|")
}

fn span_field_sort_value(json_line: &Value, field_name: &str) -> String {
    value_to_sort_string(json_line["span"].get(field_name))
}

fn value_to_sort_string(value: Option<&Value>) -> String {
    value
        .map(|value| {
            value
                .as_str()
                .map(ToString::to_string)
                .unwrap_or_else(|| value.to_string())
        })
        .unwrap_or_default()
}

/// Extracts a field's value from the `"span"` object in a JSON line, returning it as an `Option<String>`.
fn extract_json_field_value(json_line: &Value, field_name: &str) -> Option<String> {
    let span_fields = &json_line["span"];
    if let Some(field) = span_fields.get(field_name) {
        serde_json::from_str(field.to_string().as_str()).unwrap()
    } else {
        None
    }
}

/// Extracts the test name from the root span in a JSON line, falling back to empty string.
fn extract_test_name(json_line: &Value) -> String {
    let Some(root_span) = json_line["spans"].get(0) else {
        return "".to_string();
    };

    if let Some(field) = root_span.get("test_name") {
        serde_json::from_str(field.to_string().as_str()).unwrap()
    } else {
        "".to_string()
    }
}

/// Initializes an in-memory logging/tracing pipeline exactly once,
/// returning a reference to the shared log buffer for all tests.
fn init_tracing() -> &'static Arc<Mutex<Vec<u8>>> {
    let log_buffer = LOG_BUFFER.get_or_init(Arc::default);

    // Ensure the global subscriber is initialized only once.
    SUBSCRIBER_INIT.call_once(|| {
        init_subscriber(log_buffer.clone());
    });

    log_buffer
}

/// Sets up a tracing subscriber pipeline that writes to both the shared in-memory buffer
/// and stdout (for info-level logs). All subsequent tests share this subscriber setup.
fn init_subscriber(log_buffer: Arc<Mutex<Vec<u8>>>) {
    // Create a layer that outputs only spans to the log buffer as JSON.
    let assertion_layer = fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .event_format(fmt::format().json().flatten_event(true).without_time())
        .json()
        .flatten_event(true)
        .with_ansi(false)
        .with_writer(
            InMemoryMakeWriter::new(log_buffer)
                .with_max_level(Level::INFO)
                .with_filter(|meta| meta.is_span()),
        );

    // Create a layer for INFO-level logs/spans to stdout.
    let stdout_layer = fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(std::io::stdout.with_max_level(Level::INFO));

    // Register the layers.
    Registry::default()
        .with(assertion_layer)
        .with(stdout_layer)
        .init();
}
