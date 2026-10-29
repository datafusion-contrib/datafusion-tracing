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

//! Run with `cargo run -p datafusion-tracing --example metrics_snapshot`.
//! No tracing subscriber, collector service, optional feature, or test data is needed.

use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_tracing::metrics_export::MetricsExporter;
use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};

#[tokio::main(flavor = "current_thread")]
async fn main() -> Result<()> {
    let exporter = MetricsExporter::new();
    let options = InstrumentationOptions::builder()
        .metrics_exporter(exporter.clone())
        .build();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(instrument_with_info_spans!(options: options))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.sql("SELECT SUM(value) FROM (VALUES (1), (2), (3)) AS numbers(value)")
        .await?
        .collect()
        .await?;

    for operator in exporter.snapshot() {
        for (metric, value) in operator.counters() {
            println!("{} operator={:?} {value}", metric.name(), operator.operator);
        }
    }
    Ok(())
}
