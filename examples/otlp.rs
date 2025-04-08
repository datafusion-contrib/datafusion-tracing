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

//! # OTLP Example
//!
//! This example demonstrates OpenTelemetry integration with DataFusion for distributed tracing.
//! It shows how to:
//!
//! - Configure OpenTelemetry with an OTLP exporter
//! - Set up service metadata and tracing layers
//! - Instrument DataFusion query execution
//!
//! ## Prerequisites
//!
//! Before running this example, you'll need an OpenTelemetry collector. For local development,
//! Jaeger is recommended:
//!
//! ```bash
//! docker run --rm --name jaeger \
//!   -p 16686:16686 \
//!   -p 4317:4317 \
//!   jaegertracing/jaeger:2.5.0
//! ```
//!
//! After starting Jaeger, you can view traces at http://localhost:16686
//!
//! ## Running the Example
//!
//! ```bash
//! cargo run --example otlp
//! ```
//!
//! This example executes a deliberately complex SQL query with multiple CTEs, window functions,
//! and joins against data stored in an object store. The complexity showcases how tracing
//! captures and visualizes the execution of different parts of the query plan.

use std::time::Duration;

use datafusion::{common::internal_datafusion_err, error::Result};
use integration_utils::{init_session, run_traced_query};
use opentelemetry::{trace::TracerProvider, KeyValue};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{trace::Sampler, Resource};
use tracing::{instrument, Level};
use tracing_subscriber::{fmt, prelude::*, Registry};

// Query to be executed for demonstration purposes.
const QUERY_NAME: &str = "tpch_scrabble";

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing infrastructure and obtain a tracer provider.
    let tracer_provider = init_tracing()?;

    // Run the example under the root span.
    run_otlp_example().await?;

    // Properly shutdown tracing to ensure all data is flushed.
    tracer_provider
        .shutdown()
        .map_err(|e| internal_datafusion_err!("Tracer shutdown error: {}", e))
}

#[instrument(level = "info")]
async fn run_otlp_example() -> Result<()> {
    // Initialize the DataFusion session context.
    let ctx = init_session(true, 5, true).await?;

    // Run the SQL query with tracing enabled.
    run_traced_query(&ctx, QUERY_NAME).await?;

    Ok(())
}

/// Initializes OpenTelemetry and tracing infrastructure to enable tracing of query execution.
fn init_tracing() -> Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    // Set service metadata for tracing.
    let resource = Resource::builder()
        .with_attribute(KeyValue::new("service.name", "datafusion-tracing"))
        .build();

    // Configure an OTLP exporter to send tracing data.
    let exporter = opentelemetry_otlp::SpanExporter::builder()
        .with_tonic()
        .with_endpoint("http://localhost:4317") // Endpoint for OTLP collector.
        .with_timeout(Duration::from_secs(10))
        .build()
        .map_err(|e| internal_datafusion_err!("OTLP exporter error: {}", e))?;

    // Create a tracer provider configured with the exporter and sampling strategy.
    let tracer_provider = opentelemetry_sdk::trace::SdkTracerProvider::builder()
        .with_batch_exporter(exporter)
        .with_resource(resource)
        .with_sampler(Sampler::AlwaysOn)
        .build();

    // Obtain a tracer instance for recording tracing information.
    let tracer = tracer_provider.tracer("datafusion-tracing-query");

    // Create a telemetry layer using the tracer to collect and filter tracing data at INFO level.
    let telemetry_layer = tracing_opentelemetry::layer()
        .with_tracer(tracer)
        .with_filter(tracing::level_filters::LevelFilter::INFO);

    // Create a formatting layer to output logs to stdout, including thread IDs and names.
    let fmt_layer = fmt::layer()
        .with_thread_ids(true)
        .with_thread_names(true)
        .with_writer(std::io::stdout.with_max_level(Level::INFO));

    // Combine the telemetry and formatting layers into a tracing subscriber and initialize it.
    Registry::default()
        .with(telemetry_layer)
        .with(fmt_layer)
        .init();

    // Return the configured tracer provider
    Ok(tracer_provider)
}
