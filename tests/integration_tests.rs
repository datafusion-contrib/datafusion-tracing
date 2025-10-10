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

use std::sync::{Arc, Mutex, Once, OnceLock};

use datafusion::error::Result;
use insta::{assert_json_snapshot, assert_snapshot};
use integration_utils::{init_session, run_traced_query};
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

/// A struct describing how a particular query test should be run.
#[derive(Debug, Default)]
struct QueryTestCase<'a> {
    /// The SQL query to run.
    sql_query: &'a str,
    /// Whether to instrument the object store for this test.
    record_object_store: bool,
    /// Whether to collect (record) metrics for this test.
    should_record_metrics: bool,
    /// Maximum number of rows to preview in logs.
    row_limit: usize,
    /// Use compact formatting for the row preview.
    use_compact_preview: bool,
    /// Indices of spans for which preview assertions should be skipped.
    ignored_preview_spans: &'a [usize],
    /// Whether to ignore the full trace in assertions.
    ignore_full_trace: bool,
}

impl<'a> QueryTestCase<'a> {
    fn new(sql_query: &'a str) -> Self {
        Self {
            sql_query,
            ..Default::default()
        }
    }

    fn with_object_store_collection(mut self) -> Self {
        self.record_object_store = true;
        self
    }

    fn with_metrics_collection(mut self) -> Self {
        self.should_record_metrics = true;
        self
    }

    fn with_row_limit(mut self, limit: usize) -> Self {
        self.row_limit = limit;
        self
    }

    fn with_compact_preview(mut self) -> Self {
        self.use_compact_preview = true;
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
            .ignore_preview_spans(&[15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25])
            // ignore the full trace assertions as they are not deterministic: exact span ordering can vary for this complex query
            .ignore_full_trace(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_recursive() -> Result<()> {
    execute_test_case("08_recursive", &QueryTestCase::new("recursive")).await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_recursive_all_options() -> Result<()> {
    execute_test_case(
        "09_recursive_all_options",
        &QueryTestCase::new("recursive")
            .with_metrics_collection()
            .with_row_limit(5)
            .with_compact_preview(),
    )
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn test_topk_lineitem() -> Result<()> {
    // Expect a filled `DynamicFilterPhysicalExpr` on the `DataSourceExec` "datafusion.node" field
    execute_test_case("10_topk_lineitem", &QueryTestCase::new("topk_lineitem")).await
}

/// Executes the provided [`QueryTestCase`], setting up tracing and verifying
/// log output according to its parameters.
async fn execute_test_case(test_name: &str, test_case: &QueryTestCase<'_>) -> Result<()> {
    // Initialize tracing infrastructure and collect a log buffer.
    let log_buffer = init_tracing();

    // Initialize the DataFusion session with the requested options.
    let ctx = init_session(
        test_case.record_object_store,
        test_case.should_record_metrics,
        test_case.row_limit,
        test_case.use_compact_preview,
    )
    .await?;

    // Run the SQL query with tracing enabled.
    run_traced_query(&ctx, test_case.sql_query)
        .instrument(tracing::info_span!("test", test_name = %test_name))
        .await?;

    // Clone the log buffer to avoid holding the lock while processing.
    let logs = log_buffer.lock().unwrap().clone();
    let log_str = String::from_utf8_lossy(&logs);

    // Convert each log line to JSON, filtering by this test case name.
    let json_lines = log_str
        .lines()
        .map(|line| serde_json::from_str(line).expect("Failed to parse JSON line"))
        .filter(|json_line| extract_test_name(json_line) == *test_name)
        .collect::<Vec<Value>>();

    // Bind insta settings for snapshot testing.
    let _insta_guard = insta_settings::settings().bind_to_scope();

    // If we have a preview row_limit, do dedicated assertions on the previews.
    if test_case.row_limit > 0 {
        let mut preview_id = 0;
        for json_line in &json_lines {
            if let Some(span_name) = extract_json_field_value(json_line, "otel.name") {
                if let Some(preview) =
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
    }

    // General assertion on the full trace.
    if !test_case.ignore_full_trace {
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
