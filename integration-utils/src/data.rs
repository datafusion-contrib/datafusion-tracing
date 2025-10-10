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

//! Data and table utilities

use std::path::PathBuf;
use std::sync::Arc;

use datafusion::common::internal_datafusion_err;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::ListingOptions;
use datafusion::{error::Result, prelude::*};
use tracing::{info, instrument};

/// Returns the path to the directory containing the Parquet tables.
pub fn data_dir() -> PathBuf {
    PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("data")
}

/// Registers all TPCH Parquet tables required for executing the queries.
#[instrument(level = "info", skip(ctx))]
pub(crate) async fn register_tpch_tables(ctx: &SessionContext) -> Result<()> {
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
