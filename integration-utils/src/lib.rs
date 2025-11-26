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
//! use integration_utils::{SessionBuilder, run_traced_query};
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! // Initialize a session with tracing options via builder
//! let ctx = SessionBuilder::new()
//!     .record_object_store()
//!     .record_metrics()
//!     .preview_limit(5)
//!     .compact_preview()
//!     .build()
//!     .await?;
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
use datafusion::physical_plan::collect;
use datafusion::{
    error::Result, execution::SessionStateBuilder,
    physical_optimizer::PhysicalOptimizerRule, prelude::*,
};
use datafusion_tracing::{
    InstrumentationOptions, RuleInstrumentationOptions, instrument_rules_with_info_spans,
    instrument_with_info_spans, pretty_format_compact_batch,
};
use instrumented_object_store::instrument_object_store;
use tracing::{field, info, instrument};
use url::Url;

/// Executes the SQL query with instrumentation enabled, providing detailed tracing output.
#[instrument(level = "info", skip(ctx))]
pub async fn run_traced_query(ctx: &SessionContext, query_name: &str) -> Result<()> {
    info!("Beginning traced query execution");

    let query = read_query(query_name)?;

    // Parse SQL and create logical plan.
    let df = parse_sql(ctx, query.as_str()).await?;

    // Generate a physical execution plan from the logical plan.
    // All phases are instrumented by instrument_rules_with_info_spans!:
    // analyzer, logical optimizer, physical plan creation, and physical optimizer.
    let physical_plan = df.create_physical_plan().await?;

    // Execute the physical plan and collect results.
    let results = collect(physical_plan.clone(), ctx.task_ctx()).await?;

    info!("Query Results:\n{}", pretty_format_batches(&results)?);

    Ok(())
}

/// Builder for creating a test [`SessionContext`] with tracing instrumentation.
#[derive(Debug, Default, Clone)]
pub struct SessionBuilder {
    record_object_store: bool,
    record_metrics: bool,
    preview_limit: usize,
    compact_preview: bool,
    plan_diff: bool,
}

impl SessionBuilder {
    /// Returns the configured preview limit.
    pub fn get_preview_limit(&self) -> usize {
        self.preview_limit
    }
}

impl SessionBuilder {
    /// Creates a new session builder with default settings.
    pub fn new() -> Self {
        Self::default()
    }

    /// Enables object store instrumentation for tracing file access.
    pub fn record_object_store(mut self) -> Self {
        self.record_object_store = true;
        self
    }

    /// Enables recording execution metrics in spans.
    pub fn record_metrics(mut self) -> Self {
        self.record_metrics = true;
        self
    }

    /// Sets the maximum number of rows to preview in span fields.
    pub fn preview_limit(mut self, value: usize) -> Self {
        self.preview_limit = value;
        self
    }

    /// Enables compact preview formatting.
    pub fn compact_preview(mut self) -> Self {
        self.compact_preview = true;
        self
    }

    /// Enables unified diffs of plan changes in rule spans.
    pub fn plan_diff(mut self) -> Self {
        self.plan_diff = true;
        self
    }

    /// Builds and returns the configured [`SessionContext`].
    #[instrument(level = "info", skip(self))]
    pub async fn build(self) -> Result<SessionContext> {
        // Configure the session state with instrumentation for query execution.
        let session_state = SessionStateBuilder::new()
            .with_default_features()
            .with_config(SessionConfig::default().with_target_partitions(8)) // Enforce target partitions to ensure consistent test results regardless of the number of CPU cores.
            .with_physical_optimizer_rule(create_instrumentation_rule(
                self.record_metrics,
                self.preview_limit,
                self.compact_preview,
            ))
            .build();

        // Instrument all rules (analyzer, logical optimizer, physical optimizer)
        // Rules are grouped under phase spans (analyze_logical_plan, optimize_logical_plan, optimize_physical_plan)
        // Physical plan creation tracing is automatically enabled when physical_optimizer is set
        let rule_options = if self.plan_diff {
            RuleInstrumentationOptions::full().with_plan_diff()
        } else {
            RuleInstrumentationOptions::full()
        };
        let session_state = instrument_rules_with_info_spans!(
            options: rule_options,
            state: session_state
        );

        let ctx = SessionContext::new_with_state(session_state);

        // Instrument the local filesystem object store for tracing file access.
        let local_store = Arc::new(object_store::local::LocalFileSystem::new());
        let object_store = if self.record_object_store {
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

/// Returns the path to the directory containing the TPCH Parquet tables.
pub fn tpch_tables_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

/// Registers all TPCH Parquet tables required for executing the queries.
#[instrument(level = "info", skip(ctx))]
async fn register_tpch_tables(ctx: &SessionContext) -> Result<()> {
    // Construct the path to the directory containing Parquet data.
    let data_dir = tpch_tables_dir();

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
