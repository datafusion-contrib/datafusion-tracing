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

//! # Distributed OTLP Example
//!
//! This example demonstrates OpenTelemetry integration with DataFusion for distributed tracing.
//! It shows how to:
//!
//! - Configure OpenTelemetry with an OTLP exporter
//! - Set up service metadata and tracing layers
//! - Instrument DataFusion query execution for distributed plans
//! - Execute multiple queries with separate contexts
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
//!   jaegertracing/jaeger:2.7.0
//! ```
//!
//! After starting Jaeger, you can view traces at http://localhost:16686
//!
//! ## Running the Example
//!
//! ```bash
//! cargo run --example distributed_otlp
//! ```
//!
//! This example executes all 22 TPCH benchmark queries with distributed query execution enabled.

use std::time::Duration;

use datafusion::{common::internal_datafusion_err, error::Result};
use integration_utils::{DistributedMode, SessionBuilder, run_traced_query};
use opentelemetry::{KeyValue, trace::TracerProvider};
use opentelemetry_otlp::WithExportConfig;
use opentelemetry_sdk::{Resource, trace::Sampler};
use tracing::{Instrument, Level};
use tracing_subscriber::{Registry, fmt, prelude::*};

/// Internal helper macro for generating match arms using repetition.
/// This uses Rust's macro repetition syntax ($(...)*).
macro_rules! generate_query_span_match {
    ($i:expr, $mode_name:expr, $query_name:expr, $($num:tt),*) => {
        match ($i, $mode_name) {
            $(
                ($num, "Memory") => tracing::info_span!(
                    concat!("tpch_query_", stringify!($num), "_memory"),
                    query = %$query_name,
                    query_num = $i,
                    distributed_mode = $mode_name
                ),
                ($num, "Localhost") => tracing::info_span!(
                    concat!("tpch_query_", stringify!($num), "_localhost"),
                    query = %$query_name,
                    query_num = $i,
                    distributed_mode = $mode_name
                ),
            )*
            _ => unreachable!("Invalid query number or mode"),
        }
    };
}

/// Macro to generate span creation for all query/mode combinations.
/// This is necessary because tracing span names must be compile-time constants.
///
/// Usage: create_query_span!(query_num, mode_name, query_name)
macro_rules! create_query_span {
    ($i:expr, $mode_name:expr, $query_name:expr) => {
        generate_query_span_match!(
            $i,
            $mode_name,
            $query_name,
            1,
            2,
            3,
            4,
            5,
            6,
            7,
            8,
            9,
            10,
            11,
            12,
            13,
            14,
            15,
            16,
            17,
            18,
            19,
            20,
            21,
            22
        )
    };
}

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing infrastructure and obtain a tracer provider.
    let tracer_provider = init_tracing()?;

    // Run the example under the root span.
    run_distributed_otlp_example().await?;

    // Properly shutdown tracing to ensure all data is flushed.
    tracer_provider
        .shutdown()
        .map_err(|e| internal_datafusion_err!("Tracer shutdown error: {}", e))
}

async fn run_distributed_otlp_example() -> Result<()> {
    // Test both distributed execution modes
    let modes = [
        (DistributedMode::Localhost, "Localhost"),
        (DistributedMode::Memory, "Memory"),
    ];

    for (mode, mode_name) in modes {
        tracing::info!("Starting TPCH queries with {} distributed mode", mode_name);

        // Loop over all 22 TPCH queries
        for i in 1..=22 {
            let query_name = format!("tpch/q{}", i);

            // Create a new root span for each query to ensure independent traces.
            // This span will be the root of a new trace tree.
            // Note: Span names must be compile-time constants, so we use a macro to generate them.
            let span = create_query_span!(i, mode_name, query_name);

            // Execute the query within the new root span context.
            async {
                tracing::info!(
                    "Running TPCH query: {} in {} mode",
                    query_name,
                    mode_name
                );

                // Initialize a distinct DataFusion session context for each query.
                let ctx = SessionBuilder::new()
                    .with_metrics()
                    .with_preview(5)
                    .with_compact_preview()
                    .with_distributed_mode(mode)
                    .build()
                    .await?;

                // Run the SQL query with tracing enabled.
                run_traced_query(&ctx, &query_name).await
            }
            .instrument(span)
            .await?;
        }

        tracing::info!(
            "Completed all TPCH queries with {} distributed mode",
            mode_name
        );
    }

    Ok(())
}

/// Initializes OpenTelemetry and tracing infrastructure to enable tracing of query execution.
fn init_tracing() -> Result<opentelemetry_sdk::trace::SdkTracerProvider> {
    // Set up the global text map propagator for trace context propagation across gRPC boundaries.
    // This is essential for distributed tracing to work - without it, worker spans won't be
    // linked to parent traces.
    opentelemetry::global::set_text_map_propagator(
        opentelemetry_sdk::propagation::TraceContextPropagator::new(),
    );

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
