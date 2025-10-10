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
//! use integration_utils::{SessionBuilder, run_traced_query, DistributedMode};
//!
//! # async fn example() -> datafusion::error::Result<()> {
//! // Initialize a session with default options
//! let ctx = SessionBuilder::new().build().await?;
//!
//! // Initialize with all tracing options enabled
//! let ctx = SessionBuilder::new()
//!     .with_object_store_tracing()
//!     .with_metrics()
//!     .with_preview(5)
//!     .with_compact_preview()
//!     .build()
//!     .await?;
//!
//! // Or with in-memory distributed execution
//! let ctx = SessionBuilder::new()
//!     .with_distributed_mode(DistributedMode::Memory)
//!     .build()
//!     .await?;
//!
//! // Or with localhost worker distributed execution
//! let ctx = SessionBuilder::new()
//!     .with_distributed_mode(DistributedMode::Localhost)
//!     .build()
//!     .await?;
//!
//! // Run a traced query - results and execution details will be logged
//! run_traced_query(&ctx, "simple_query").await?;
//! # Ok(())
//! # }
//! ```

mod data;
mod distributed;
mod query;
mod session;

// Re-export public API
pub use data::data_dir;
pub use distributed::DistributedMode;
pub use query::{create_physical_plan, parse_sql, read_query, run_traced_query};
pub use session::{SessionBuilder, WorkerTaskWrapper, create_instrumentation_rule};
