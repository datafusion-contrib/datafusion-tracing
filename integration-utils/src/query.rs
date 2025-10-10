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

//! Query execution utilities

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::common::internal_datafusion_err;
use datafusion::physical_plan::{ExecutionPlan, collect, displayable};
use datafusion::{error::Result, prelude::*};
use tracing::{info, instrument};

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

/// Reads a SQL query from a file.
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

/// Parses SQL into a DataFusion DataFrame.
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

/// Creates a physical execution plan from a DataFrame.
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
