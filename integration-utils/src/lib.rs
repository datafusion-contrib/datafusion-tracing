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

//! # DataFusion Tracing Integration Utilities
//!
//! This utility crate provides helper functions and tools for integration testing
//! and examples in the DataFusion Tracing project. **Note: This crate is intended for
//! internal use, examples, and testing only - not for production use.**
//!
//! ## Overview
//!
//! The integration utilities include:
//!
//! - Functions for setting up DataFusion sessions with tracing instrumentation
//! - Helpers for executing traced queries with consistent output
//! - Tools for registering and accessing test data (TPCH tables)
//! - Utilities for reading and parsing SQL queries from files
//!
//! ## Example Usage
//!
//! ```rust
//! use datafusion::prelude::*;
//! use integration_utils::{init_session, run_traced_query};
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! // Initialize a session with all tracing options enabled
//! let ctx = init_session(true, true, 5, true).await?;
//!
//! // Run a traced query - results and execution details will be logged
//! run_traced_query(&ctx, "simple_query").await?;
//! # Ok(())
//! # }
//! ```

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::internal_datafusion_err;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::{
    error::Result, execution::SessionStateBuilder,
    physical_optimizer::PhysicalOptimizerRule, prelude::*,
};
use datafusion_distributed::{DistributedExt, DistributedPhysicalOptimizerRule};
use datafusion_tracing::{
    InstrumentationOptions, instrument_with_info_spans, pretty_format_compact_batch,
};
use instrumented_object_store::instrument_object_store;
use tracing::{field, info, instrument};
use url::Url;

mod channel_resolver;
use channel_resolver::InMemoryChannelResolver;

/// Executes the SQL query with instrumentation enabled, providing detailed tracing output.
#[instrument(level = "info", skip(ctx))]
pub async fn run_traced_query(ctx: &SessionContext, query_name: &str) -> Result<()> {
    info!("Beginning traced query execution");

    let query = read_query(query_name)?;

    // Parse the SQL query into a DataFusion dataframe.
    let df = parse_sql(ctx, query.as_str()).await?;

    // Generate a physical execution plan from the logical plan.
    let physical_plan = create_physical_plan(df).await?;

    // Execute the physical plan and collect results.
    let results = collect(physical_plan.clone(), ctx.task_ctx()).await?;

    info!("Query Results:\n{}", pretty_format_batches(&results)?);

    Ok(())
}

#[instrument(level = "info")]
pub async fn init_session(
    record_object_store: bool,
    record_metrics: bool,
    preview_limit: usize,
    compact_preview: bool,
    distributed: bool,
) -> Result<SessionContext> {
    // Configure the session state with instrumentation for query execution.
    let mut session_state_builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::default().with_target_partitions(8)); // Enforce target partitions to ensure consistent test results regardless of the number of CPU cores.
    if distributed {
        session_state_builder = session_state_builder
            .with_distributed_channel_resolver(InMemoryChannelResolver::new())
            .with_physical_optimizer_rule(Arc::new(
                DistributedPhysicalOptimizerRule::new(),
            ));
    }
    session_state_builder = session_state_builder.with_physical_optimizer_rule(
        create_instrumentation_rule(record_metrics, preview_limit, compact_preview),
    );
    let session_state = session_state_builder.build();

    let ctx = SessionContext::new_with_state(session_state);

    // Instrument the local filesystem object store for tracing file access.
    let local_store = Arc::new(object_store::local::LocalFileSystem::new());
    let object_store = if record_object_store {
        instrument_object_store(local_store, "local_fs")
    } else {
        local_store
    };

    // Register the instrumented object store for handling file:// URLs.
    ctx.register_object_store(&Url::parse("file://").unwrap(), object_store);

    // Register the tpch tables.
    register_tpch_tables(&ctx).await?;

    Ok(ctx)
}

/// Creates an instrumentation rule that captures metrics and provides previews of data during execution.
pub fn create_instrumentation_rule(
    record_metrics: bool,
    preview_limit: usize,
    compact_preview: bool,
) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    let options_builder = InstrumentationOptions::builder()
        .add_custom_field("env", "production") // Custom fields
        .add_custom_field("region", "us-west")
        .record_metrics(record_metrics)
        .preview_limit(preview_limit);
    let options_builder = if compact_preview {
        options_builder.preview_fn(Arc::new(|batch: &RecordBatch| {
            // Format data batches for compact preview in span fields.
            pretty_format_compact_batch(batch, 64, 3, 10).map(|fmt| fmt.to_string())
        }))
    } else {
        options_builder
    };

    instrument_with_info_spans!(
        options: options_builder.build(),
        env = field::Empty, // custom fields keys must be defined at compile time
        region = field::Empty,
    )
}

/// Returns the path to the directory containing the Parquet tables.
pub fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

/// Registers all TPCH Parquet tables required for executing the queries.
#[instrument(level = "info", skip(ctx))]
async fn register_tpch_tables(ctx: &SessionContext) -> Result<()> {
    // Construct the path to the directory containing Parquet data.
    let data_dir = data_dir();

    // Register the weather table.
    ctx.register_parquet(
        "weather",
        data_dir.join("weather").to_string_lossy(),
        ParquetReadOptions::default(),
    )
    .await?;

    // Generate and register each table from Parquet files.
    // This includes all standard TPCH tables so examples/tests can rely on them.
    for table in [
        "nation", "region", "part", "supplier", "partsupp", "customer", "orders",
        "lineitem",
    ] {
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()));

        let parquet_path = data_dir.join(table).with_extension("parquet");
        if !parquet_path.exists() {
            return Err(internal_datafusion_err!(
                "Missing TPCH Parquet file: {}.\nGenerate TPCH data first by running: ./dev/generate_tpch_parquet.sh\nThis script requires 'tpchgen-cli' (install with: cargo install tpchgen-cli)",
                parquet_path.display()
            ));
        }

        // Generate the file path URL for the Parquet data.
        let table_path = format!("file://{}", parquet_path.to_string_lossy());

        info!("Registering table '{}' from {}", table, table_path);

        // Register the table with DataFusion's session context.
        ctx.register_listing_table(table, &table_path, listing_options, None, None)
            .await?;
    }

    Ok(())
}

#[instrument(level = "info", fields(query))]
pub fn read_query(name: &str) -> Result<String> {
    // Construct the path to the directory containing the SQL queries.
    let query_dir = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("queries");
    let query_path = query_dir.join(name).with_extension("sql");
    let query = std::fs::read_to_string(query_path)
        .map_err(|e| internal_datafusion_err!("Failed to read query file: {}", e))?;

    // Record the query as part of the current span.
    tracing::Span::current().record("query", query.as_str());
    info!("SQL Query:\n{}", query);

    Ok(query)
}

#[instrument(level = "info", skip(ctx), fields(logical_plan))]
pub async fn parse_sql(ctx: &SessionContext, sql: &str) -> Result<DataFrame> {
    // Parse the SQL query into a DataFrame.
    let df = ctx.sql(sql).await?;

    // Record the logical plan as part of the current span.
    let logical_plan = df.logical_plan().display_indent_schema().to_string();
    tracing::Span::current().record("logical_plan", logical_plan.as_str());
    info!("Logical Plan:\n{}", logical_plan);

    Ok(df)
}

#[instrument(level = "info", skip(df), fields(physical_plan))]
pub async fn create_physical_plan(df: DataFrame) -> Result<Arc<dyn ExecutionPlan>> {
    // Create a physical plan from the DataFrame.
    let physical_plan = df.create_physical_plan().await?;

    // Record the physical plan as part of the current span.
    let physical_plan_str = displayable(physical_plan.as_ref()).indent(true).to_string();
    tracing::Span::current().record("physical_plan", physical_plan_str.as_str());
    info!("Physical Plan:\n{}", physical_plan_str);

    Ok(physical_plan)
}
