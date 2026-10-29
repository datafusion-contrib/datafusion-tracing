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

//! Run with `cargo run -p datafusion-tracing --example metrics_prometheus --features prometheus`.
//! Demonstrates a caller-owned registry without an HTTP server or tracing subscriber.

use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_tracing::metrics_export::{MetricsExporter, PrometheusMetricsCollector};
use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};
use prometheus_client::{encoding::text::encode, registry::Registry};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let exporter = MetricsExporter::new();
    let mut registry = Registry::default();
    registry
        .register_collector(Box::new(PrometheusMetricsCollector::new(exporter.clone())));

    let options = InstrumentationOptions::builder()
        .metrics_exporter(exporter)
        .build();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(instrument_with_info_spans!(options: options))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let plan = ctx
        .sql("SELECT SUM(value) FROM (VALUES (1), (2), (3)) AS numbers(value)")
        .await?
        .create_physical_plan()
        .await?;
    datafusion::physical_plan::collect(plan.clone(), ctx.task_ctx()).await?;

    // The collector refreshes native values even though the physical plan is
    // retained. In an application, return this body from the existing endpoint.
    let mut body = String::new();
    encode(&mut body, &registry)?;
    print!("{body}");
    Ok(())
}
