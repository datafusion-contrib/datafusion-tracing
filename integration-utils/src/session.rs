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

//! Session configuration and initialization

use std::sync::Arc;

use datafusion::arrow::array::RecordBatch;
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

use crate::data::register_tpch_tables;
use crate::distributed::{
    DEFAULT_WORKER_PORTS, DistributedMode, InMemoryChannelResolver,
    LocalhostChannelResolver, spawn_localhost_workers,
};

/// Builder for configuring and creating a DataFusion SessionContext with tracing instrumentation.
///
/// # Example
///
/// ```rust
/// use integration_utils::{SessionBuilder, DistributedMode};
///
/// # async fn example() -> datafusion::error::Result<()> {
/// // Create a session with custom configuration
/// let ctx = SessionBuilder::new()
///     .with_object_store_tracing()
///     .with_metrics()
///     .with_preview(10)
///     .with_compact_preview()
///     .with_distributed_mode(DistributedMode::Memory)
///     .build()
///     .await?;
/// # Ok(())
/// # }
/// ```
#[derive(Debug, Clone)]
pub struct SessionBuilder {
    record_object_store: bool,
    record_metrics: bool,
    preview_limit: usize,
    compact_preview: bool,
    distributed_mode: Option<DistributedMode>,
}

impl Default for SessionBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl SessionBuilder {
    /// Creates a new SessionBuilder with sensible default values.
    ///
    /// Default configuration:
    /// - Object store tracing: disabled
    /// - Metrics recording: disabled
    /// - Preview limit: 0 (no preview)
    /// - Compact preview: false
    /// - Distributed mode: None (non-distributed)
    pub fn new() -> Self {
        Self {
            record_object_store: false,
            record_metrics: false,
            preview_limit: 0,
            compact_preview: false,
            distributed_mode: None,
        }
    }

    /// Enable object store tracing instrumentation.
    ///
    /// When enabled, file system access operations will be traced.
    pub fn with_object_store_tracing(mut self) -> Self {
        self.record_object_store = true;
        self
    }

    /// Enable metrics recording during query execution.
    ///
    /// When enabled, execution metrics like row counts and elapsed time will be recorded.
    pub fn with_metrics(mut self) -> Self {
        self.record_metrics = true;
        self
    }

    /// Set the number of rows to preview in trace spans.
    ///
    /// Set to 0 to disable previews. Non-zero values enable data previews in spans.
    pub fn with_preview(mut self, limit: usize) -> Self {
        self.preview_limit = limit;
        self
    }

    /// Enable compact preview formatting.
    ///
    /// When enabled (along with a non-zero preview limit), data previews will use
    /// a more compact formatting style suitable for narrow displays.
    pub fn with_compact_preview(mut self) -> Self {
        self.compact_preview = true;
        self
    }

    /// Set the distributed execution mode.
    ///
    /// Use `Some(DistributedMode::Memory)` for in-memory distributed execution (testing),
    /// or `Some(DistributedMode::Localhost)` for localhost TCP workers (more realistic).
    /// Use `None` for non-distributed execution (default).
    pub fn with_distributed_mode(mut self, mode: DistributedMode) -> Self {
        self.distributed_mode = Some(mode);
        self
    }

    /// Builds and returns the configured SessionContext.
    ///
    /// This method initializes the DataFusion session with all the configured options,
    /// sets up tracing instrumentation, registers the object store, and loads TPCH tables.
    pub async fn build(self) -> Result<SessionContext> {
        init_session(
            self.record_object_store,
            self.record_metrics,
            self.preview_limit,
            self.compact_preview,
            self.distributed_mode,
        )
        .await
    }
}

/// Initializes a DataFusion session with the specified configuration.
#[instrument(level = "info")]
pub async fn init_session(
    record_object_store: bool,
    record_metrics: bool,
    preview_limit: usize,
    compact_preview: bool,
    distributed_mode: Option<DistributedMode>,
) -> Result<SessionContext> {
    // Configure the session state with instrumentation for query execution.
    let mut session_state_builder = SessionStateBuilder::new()
        .with_default_features()
        .with_config(SessionConfig::default().with_target_partitions(8)); // Enforce target partitions to ensure consistent test results regardless of the number of CPU cores.

    // Configure distributed execution if requested
    if let Some(mode) = distributed_mode {
        match mode {
            DistributedMode::Memory => {
                info!("Using in-memory channel resolver for distributed execution");
                session_state_builder = session_state_builder
                    .with_distributed_channel_resolver(InMemoryChannelResolver::new())
                    .with_physical_optimizer_rule(Arc::new(
                        DistributedPhysicalOptimizerRule::new(),
                    ));
            }
            DistributedMode::Localhost => {
                info!(
                    "Spawning localhost workers on ports {:?}",
                    DEFAULT_WORKER_PORTS
                );
                // Spawn localhost workers and configure the resolver
                spawn_localhost_workers(DEFAULT_WORKER_PORTS).await?;
                let localhost_resolver =
                    LocalhostChannelResolver::new(DEFAULT_WORKER_PORTS.to_vec());

                session_state_builder = session_state_builder
                    .with_distributed_channel_resolver(localhost_resolver)
                    .with_physical_optimizer_rule(Arc::new(
                        DistributedPhysicalOptimizerRule::new()
                            .with_network_coalesce_tasks(3)
                            .with_network_shuffle_tasks(4),
                    ));
            }
        }
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
