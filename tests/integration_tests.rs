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

use std::collections::HashMap;
use std::sync::{Arc, Mutex, Once, OnceLock};

use datafusion::error::Result;
use insta::{assert_json_snapshot, assert_snapshot};
use integration_utils::{DistributedMode, SessionBuilder, run_traced_query};
use serde_json::Value;
use tracing::{Instrument, Level};
use tracing_subscriber::fmt::format::FmtSpan;
use tracing_subscriber::{Registry, fmt, prelude::*};

use crate::test_utils::insta_settings;
use crate::test_utils::insta_settings::preview_redacted_settings;
use crate::test_utils::multi_buffer_writer::MultiBufferMakeWriter;

// A global buffer to store logs across tests, initialized only once.
static LOG_BUFFER: OnceLock<Arc<Mutex<Vec<u8>>>> = OnceLock::new();

// A global multi-buffer writer for distributed localhost mode
static MULTI_BUFFER_WRITER: OnceLock<MultiBufferMakeWriter> = OnceLock::new();

// A global "once" to ensure the subscriber is initialized only once.
static SUBSCRIBER_INIT: Once = Once::new();

/// A struct describing how a particular query test should be run.
#[derive(Debug)]
struct QueryTestCase<'a> {
    /// The SQL query to run.
    sql_query: &'a str,
    /// Session builder for configuring the DataFusion session.
    session_builder: SessionBuilder,
    /// Indices of spans for which preview assertions should be skipped.
    ignored_preview_spans: &'a [usize],
    /// Whether to ignore the full trace in assertions.
    ignore_full_trace: bool,
}

impl<'a> QueryTestCase<'a> {
    fn new(sql_query: &'a str) -> Self {
        Self {
            sql_query,
            session_builder: SessionBuilder::new(),
            ignored_preview_spans: &[],
            ignore_full_trace: false,
        }
    }

    fn with_object_store_tracing(mut self) -> Self {
        self.session_builder = self.session_builder.with_object_store_tracing();
        self
    }

    fn with_metrics(mut self) -> Self {
        self.session_builder = self.session_builder.with_metrics();
        self
    }

    fn with_preview(mut self, limit: usize) -> Self {
        self.session_builder = self.session_builder.with_preview(limit);
        self
    }

    fn with_compact_preview(mut self, limit: usize) -> Self {
        self.session_builder = self
            .session_builder
            .with_preview(limit)
            .with_compact_preview();
        self
    }

    fn ignore_preview_spans(mut self, spans: &'a [usize]) -> Self {
        self.ignored_preview_spans = spans;
        self
    }

    fn ignore_full_trace(mut self) -> Self {
        self.ignore_full_trace = true;
        self
    }

    fn distributed_memory(mut self) -> Self {
        self.session_builder = self
            .session_builder
            .with_distributed_mode(DistributedMode::Memory);
        self
    }

    fn distributed_localhost(mut self) -> Self {
        use crate::test_utils::multi_buffer_writer::WORKER_PORT;
        use integration_utils::WorkerTaskWrapper;

        // Create a worker task wrapper that sets the WORKER_PORT task-local
        let wrapper: WorkerTaskWrapper =
            Arc::new(move |port, fut| Box::pin(WORKER_PORT.scope(port, fut)));

        self.session_builder = self
            .session_builder
            .with_distributed_mode(DistributedMode::Localhost)
            .with_worker_task_wrapper(wrapper);
        self
    }

    /// Returns the session builder for this test case.
    fn session_builder(self) -> SessionBuilder {
        self.session_builder
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic() -> Result<()> {
    execute_test_case("01_basic", QueryTestCase::new("select_one")).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_metrics() -> Result<()> {
    execute_test_case(
        "02_basic_metrics",
        QueryTestCase::new("select_one").with_metrics(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_preview() -> Result<()> {
    execute_test_case(
        "03_basic_preview",
        QueryTestCase::new("select_one").with_preview(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_compact_preview() -> Result<()> {
    execute_test_case(
        "04_basic_compact_preview",
        QueryTestCase::new("select_one").with_compact_preview(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_basic_all_options() -> Result<()> {
    execute_test_case(
        "05_basic_all_options",
        QueryTestCase::new("select_one")
            .with_metrics()
            .with_compact_preview(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_object_store_all_options() -> Result<()> {
    execute_test_case(
        "06_object_store_all_options",
        QueryTestCase::new("order_nations")
            .with_object_store_tracing()
            .with_metrics()
            .with_compact_preview(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_scrabble_all_options() -> Result<()> {
    execute_test_case(
        "07_scrabble_all_options",
        QueryTestCase::new("tpch_scrabble")
            .with_metrics()
            .with_compact_preview(5)
            // skip preview assertions for these spans as they depend on the partitioning
            // and are not deterministic
            .ignore_preview_spans(&[15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25])
            // ignore the full trace assertions as they are not deterministic: exact span ordering can vary for this complex query
            .ignore_full_trace(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_recursive() -> Result<()> {
    execute_test_case("08_recursive", QueryTestCase::new("recursive")).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_recursive_all_options() -> Result<()> {
    execute_test_case(
        "09_recursive_all_options",
        QueryTestCase::new("recursive")
            .with_metrics()
            .with_compact_preview(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_topk_lineitem() -> Result<()> {
    // Expect a filled `DynamicFilterPhysicalExpr` on the `DataSourceExec` "datafusion.node" field
    execute_test_case("10_topk_lineitem", QueryTestCase::new("topk_lineitem")).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_weather() -> Result<()> {
    execute_test_case(
        "11_weather",
        QueryTestCase::new("weather")
            .with_metrics()
            .with_compact_preview(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_distributed_weather_memory() -> Result<()> {
    execute_test_case(
        "12_distributed_weather_memory",
        QueryTestCase::new("weather")
            .distributed_memory()
            .with_metrics()
            .with_compact_preview(5),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_distributed_weather_localhost() -> Result<()> {
    execute_test_case(
        "13_distributed_weather_localhost",
        QueryTestCase::new("weather")
            .distributed_localhost()
            .with_metrics()
            .with_compact_preview(5),
    )
    .await
}

/// Executes the provided [`QueryTestCase`], setting up tracing and verifying
/// log output according to its parameters.
async fn execute_test_case(test_name: &str, test_case: QueryTestCase<'_>) -> Result<()> {
    // Store test case parameters before consuming it
    let sql_query = test_case.sql_query;
    let preview_limit = test_case.session_builder.preview_limit();
    let ignored_preview_spans = test_case.ignored_preview_spans;
    let ignore_full_trace = test_case.ignore_full_trace;
    let is_localhost_distributed = matches!(
        test_case.session_builder.distributed_mode(),
        Some(DistributedMode::Localhost)
    );

    // Initialize tracing infrastructure (always uses multi-buffer mode)
    let _log_buffer = init_tracing();

    // Initialize the DataFusion session with the requested options.
    let ctx = test_case.session_builder().build().await?;

    // Run the SQL query with tracing enabled.
    run_traced_query(&ctx, sql_query)
        .instrument(tracing::info_span!("test", test_name = %test_name))
        .await?;

    // Collect logs - use multi-buffer writer for localhost distributed tests
    let logs = if is_localhost_distributed {
        if let Some(multi_buffer_writer) = MULTI_BUFFER_WRITER.get() {
            multi_buffer_writer.collect_all_buffers()
        } else {
            // Fallback to main buffer if multi-buffer wasn't initialized
            _log_buffer.lock().unwrap().clone()
        }
    } else {
        // Non-distributed or memory-distributed tests use the main buffer
        _log_buffer.lock().unwrap().clone()
    };

    let log_str = String::from_utf8_lossy(&logs);

    // Convert each log line to JSON, filtering by this test case name.
    // For localhost distributed tests, also include spans without a test name
    // (these are worker execution spans that run in separate gRPC handlers)
    let json_lines = log_str
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON line"))
        .filter(|json_line| {
            let test_name_in_span = extract_test_name(json_line);
            if is_localhost_distributed {
                // For localhost tests, include spans that match the test name OR have no test name
                // (worker spans don't have test context due to gRPC boundary)
                test_name_in_span.is_empty() || test_name_in_span == *test_name
            } else {
                // For non-distributed tests, only include spans that match the test name
                test_name_in_span == *test_name
            }
        })
        .collect::<Vec<Value>>();

    // Bind insta settings for snapshot testing.
    let _insta_guard = insta_settings::settings().bind_to_scope();

    // If we have a preview limit, do dedicated assertions on the previews.
    if preview_limit > 0 {
        // Collect all preview spans with their worker identifier and node information
        let mut preview_spans: Vec<(String, String, Option<String>, String)> = vec![];

        for json_line in &json_lines {
            if let Some(span_name) = extract_json_field_value(json_line, "otel.name") {
                if let Some(preview) =
                    extract_json_field_value(json_line, "datafusion.preview")
                {
                    // Extract a unique identifier for the span (use datafusion.node as it's unique per operator)
                    let span_id = extract_json_field_value(json_line, "datafusion.node")
                        .unwrap_or_else(|| span_name.clone());
                    // Extract worker identifier if available
                    let worker_id = extract_json_field_value(json_line, "worker.id");
                    preview_spans.push((span_name, span_id, worker_id, preview));
                }
            }
        }

        // Sort spans deterministically by node description and worker ID
        // This ensures consistent ordering across test runs
        preview_spans.sort_by(|a, b| {
            // First sort by node description, then by worker ID
            match a.1.cmp(&b.1) {
                std::cmp::Ordering::Equal => a.2.cmp(&b.2),
                other => other,
            }
        });

        for (preview_id, (span_name, _, worker_id, preview)) in
            preview_spans.iter().enumerate()
        {
            // Only assert if we have not been asked to ignore this preview span.
            if !ignored_preview_spans.contains(&preview_id) {
                // Include worker ID in snapshot name for distributed tests if available
                let snapshot_name = if let Some(worker) = worker_id {
                    format!("{test_name}_{:02}_{span_name}_{worker}", preview_id)
                } else {
                    format!("{test_name}_{:02}_{span_name}", preview_id)
                };
                assert_snapshot!(snapshot_name, preview);
            }
        }
    }

    // General assertion on the full trace.
    if !ignore_full_trace {
        // Redact `datafusion.preview` values as they are often non-deterministic.
        preview_redacted_settings().bind(|| {
            assert_json_snapshot!(format!("{test_name}_trace"), json_lines);
        });
    }

    Ok(())
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
///
/// Always uses multi-buffer routing so localhost worker tests can route logs
/// to separate buffers. Non-localhost tests will simply use the main buffer.
fn init_tracing() -> &'static Arc<Mutex<Vec<u8>>> {
    let log_buffer = LOG_BUFFER.get_or_init(Arc::default);

    // Ensure the global subscriber is initialized only once with multi-buffer support
    SUBSCRIBER_INIT.call_once(|| {
        init_subscriber_multi_buffer(log_buffer.clone());
    });

    log_buffer
}

/// Sets up a tracing subscriber with multi-buffer support for localhost workers.
/// Each worker gets its own buffer based on task-local storage.
fn init_subscriber_multi_buffer(main_buffer: Arc<Mutex<Vec<u8>>>) {
    // Set up the global text map propagator for trace context propagation.
    // Even though these tests don't use OpenTelemetry exporters, setting this up
    // ensures that if OpenTelemetry is added in the future, context will propagate correctly.
    // The propagator is a noop when no OpenTelemetry tracer is active.
    use opentelemetry_sdk::propagation::TraceContextPropagator;
    opentelemetry::global::set_text_map_propagator(TraceContextPropagator::new());

    // Create worker buffers for the default worker ports
    let worker_buffers = Arc::new(Mutex::new(HashMap::new()));

    // Pre-create buffers for all expected worker ports
    {
        let mut buffers = worker_buffers.lock().unwrap();
        for &port in &[50051u16, 50052, 50053, 50054] {
            buffers.insert(port, Arc::new(Mutex::new(Vec::new())));
        }
    }

    let multi_buffer_writer = MultiBufferMakeWriter::new(main_buffer, worker_buffers);

    // Store the multi-buffer writer globally so we can access it later for collecting logs
    let _ = MULTI_BUFFER_WRITER.set(multi_buffer_writer.clone());

    // Create a layer that outputs only spans to the appropriate buffer as JSON
    let assertion_layer = fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .event_format(fmt::format().json().flatten_event(true).without_time())
        .json()
        .flatten_event(true)
        .with_ansi(false)
        .with_writer(
            multi_buffer_writer
                .with_max_level(Level::INFO)
                .with_filter(|meta| meta.is_span()),
        );

    // Create a layer for INFO-level logs/spans to stdout
    let stdout_layer = fmt::layer()
        .with_span_events(FmtSpan::CLOSE)
        .with_writer(std::io::stdout.with_max_level(Level::INFO));

    // Register the layers
    Registry::default()
        .with(assertion_layer)
        .with(stdout_layer)
        .init();
}
