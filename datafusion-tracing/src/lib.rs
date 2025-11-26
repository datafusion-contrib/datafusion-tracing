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

//! DataFusion Tracing is an extension for [Apache DataFusion](https://datafusion.apache.org/) that helps you monitor and debug queries. It uses [`tracing`](https://docs.rs/tracing/latest/tracing/) and [OpenTelemetry](https://opentelemetry.io/) to gather DataFusion metrics, trace execution steps, and preview partial query results.
//!
//! **Note:** This is not an official Apache Software Foundation release.
//!
//! # Overview
//!
//! When you run queries with DataFusion Tracing enabled, it automatically adds tracing around execution steps, records all native DataFusion metrics such as execution time and output row count, lets you preview partial results for easier debugging, and integrates with OpenTelemetry for distributed tracing. This makes it simpler to understand and improve query performance.
//!
//! ## See it in action
//!
//! Here's what DataFusion Tracing can look like in practice:
//!
//! <details>
//! <summary>Jaeger UI</summary>
//!
//! ![Jaeger UI screenshot](https://raw.githubusercontent.com/datafusion-contrib/datafusion-tracing/main/datafusion-tracing/docs/screenshots/jaeger.png)
//! </details>
//!
//! <details>
//! <summary>DataDog UI</summary>
//!
//! ![DataDog UI screenshot](https://raw.githubusercontent.com/datafusion-contrib/datafusion-tracing/main/datafusion-tracing/docs/screenshots/datadog.png)
//! </details>
//!
//! # Getting Started
//!
//! ## Installation
//!
//! Include DataFusion Tracing in your project's `Cargo.toml`:
//!
//! ```toml
//! [dependencies]
//! datafusion = "52.0.0"
//! datafusion-tracing = "52.0.0"
//! ```
//!
//! ## Quick Start Example
//!
//! ```rust
//! use datafusion::{
//!     arrow::{array::RecordBatch, util::pretty::pretty_format_batches},
//!     error::Result,
//!     execution::SessionStateBuilder,
//!     prelude::*,
//! };
//! use datafusion_tracing::{
//!     instrument_rules_with_info_spans, instrument_with_info_spans,
//!     pretty_format_compact_batch, InstrumentationOptions, RuleInstrumentationOptions,
//! };
//! use std::sync::Arc;
//! use tracing::field;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Initialize tracing subscriber as usual
//!     // (See examples/otlp.rs for a complete example).
//!
//!     // Set up execution plan tracing options (you can customize these).
//!     let exec_options = InstrumentationOptions::builder()
//!         .record_metrics(true)
//!         .preview_limit(5)
//!         .preview_fn(Arc::new(|batch: &RecordBatch| {
//!             pretty_format_compact_batch(batch, 64, 3, 10).map(|fmt| fmt.to_string())
//!         }))
//!         .add_custom_field("env", "production")
//!         .add_custom_field("region", "us-west")
//!         .build();
//!
//!     let instrument_rule = instrument_with_info_spans!(
//!         options: exec_options,
//!         env = field::Empty,
//!         region = field::Empty,
//!     );
//!
//!     let session_state = SessionStateBuilder::new()
//!         .with_default_features()
//!         .with_physical_optimizer_rule(instrument_rule)
//!         .build();
//!
//!     // Instrument all rules (analyzer, logical optimizer, physical optimizer)
//!     // Physical plan creation tracing is automatically enabled when physical_optimizer is set
//!     let rule_options = RuleInstrumentationOptions::full().with_plan_diff();
//!     let session_state = instrument_rules_with_info_spans!(
//!         options: rule_options,
//!         state: session_state
//!     );
//!
//!     let ctx = SessionContext::new_with_state(session_state);
//!
//!     // Execute a query - the entire lifecycle is now traced:
//!     // SQL Parsing -> Logical Plan -> Analyzer Rules -> Optimizer Rules ->
//!     // Physical Plan Creation -> Physical Optimizer Rules -> Execution
//!     let results = ctx.sql("SELECT 1").await?.collect().await?;
//!     println!(
//!         "Query Results:\n{}",
//!         pretty_format_batches(results.as_slice())?
//!     );
//!
//!     Ok(())
//! }
//! ```
//!
//! A more complete example can be found in the [examples directory](https://github.com/datafusion-contrib/datafusion-tracing/tree/main/examples).
//!

// Execution plan instrumentation (wraps ExecutionPlan nodes with tracing)
mod exec_instrument_macros;
mod exec_instrument_rule;
mod instrumented_exec;

// Rule instrumentation (wraps analyzer/optimizer/physical optimizer rules with tracing)
mod rule_instrumentation;
mod rule_instrumentation_macros;

// Shared utilities
mod metrics;
mod node;
mod options;
mod planner;
mod preview;
mod preview_utils;
mod rule_options;
mod utils;

// Hide implementation details from documentation.
// These functions are only public because they need to be accessed by the macros,
// but they're not intended for direct use by consumers of this crate.
#[doc(hidden)]
pub use exec_instrument_rule::new_instrument_rule;
#[doc(hidden)]
pub use rule_instrumentation::instrument_session_state;

pub use options::InstrumentationOptions;
pub use preview_utils::pretty_format_compact_batch;
pub use rule_options::RuleInstrumentationOptions;
