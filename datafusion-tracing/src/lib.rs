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
//!     instrument_with_info_spans, pretty_format_compact_batch, InstrumentationOptions,
//! };
//! use std::sync::Arc;
//! use tracing::field;
//!
//! #[tokio::main]
//! async fn main() -> Result<()> {
//!     // Initialize tracing subscriber as usual
//!     // (See examples/otlp.rs for a complete example).
//!
//!     // Set up tracing options (you can customize these).
//!     let options = InstrumentationOptions::builder()
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
//!         options: options,
//!         env = field::Empty,
//!         region = field::Empty,
//!     );
//!
//!     let session_state = SessionStateBuilder::new()
//!         .with_default_features()
//!         .with_physical_optimizer_rule(instrument_rule)
//!         .build();
//!
//!     let ctx = SessionContext::new_with_state(session_state);
//!
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

mod instrument_rule;
mod instrumented;
mod instrumented_macros;
mod metrics;
mod node;
mod options;
mod preview;
mod preview_utils;
mod utils;

// Hide implementation details from documentation.
// This function is only public because it needs to be accessed by the macros,
// but it's not intended for direct use by consumers of this crate.
#[doc(hidden)]
pub use instrument_rule::new_instrument_rule;

pub use options::InstrumentationOptions;
pub use preview_utils::pretty_format_compact_batch;
