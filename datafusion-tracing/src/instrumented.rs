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

use crate::{
    metrics::{MetricsRecorder, MetricsRecordingStream},
    node::{NodeRecorder, NodeRecordingStream},
    options::InstrumentationOptions,
    preview::{PreviewFn, PreviewRecorder, PreviewRecordingStream},
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Statistics,
    config::ConfigOptions,
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{Distribution, OrderingRequirements},
    physical_plan::{
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
        PhysicalExpr, PlanProperties,
        execution_plan::{CardinalityEffect, InvariantLevel},
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPhase,
            FilterPushdownPropagation,
        },
        metrics::MetricsSet,
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
    },
};
use delegate::delegate;
use std::{
    any::Any,
    collections::HashMap,
    fmt::{self, Debug},
    sync::{Arc, OnceLock},
};
use tracing::{Span, field};
use tracing_futures::Instrument;

/// Type alias for a function that creates a tracing span.
pub(crate) type SpanCreateFn = dyn Fn() -> Span + Send + Sync;

/// An [`ExecutionPlan`] wrapper that instruments execution with tracing spans and metrics recording.
pub struct InstrumentedExec {
    /// The inner execution plan to delegate execution to.
    inner: Arc<dyn ExecutionPlan>,

    /// Tracing span lazily initialized during execution, shared safely across concurrent partition executions.
    span: OnceLock<Span>,

    record_metrics: bool,

    /// Metrics recorder lazily initialized during execution, shared safely across concurrent partition executions.
    metrics_recorder: OnceLock<Arc<MetricsRecorder>>,

    /// Node recorder lazily initialized during execution, shared safely across concurrent partition executions.
    node_recorder: OnceLock<Arc<NodeRecorder>>,

    preview_limit: usize,
    preview_fn: Option<Arc<PreviewFn>>,

    /// Preview recorder lazily initialized during execution, shared safely across concurrent partition executions.
    preview_recorder: OnceLock<Arc<PreviewRecorder>>,

    /// Function to create and initialize tracing spans.
    span_create_fn: Arc<SpanCreateFn>,
}

impl Debug for InstrumentedExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("InstrumentedExec")
            .field("inner", &self.inner)
            .finish()
    }
}

impl InstrumentedExec {
    /// Creates a new `InstrumentedExec` that wraps an execution plan with tracing and metrics.
    pub fn new(
        inner: Arc<dyn ExecutionPlan>,
        span_create_fn: Arc<SpanCreateFn>,
        options: &InstrumentationOptions,
    ) -> InstrumentedExec {
        Self {
            inner,
            span: OnceLock::new(),
            record_metrics: options.record_metrics,
            metrics_recorder: OnceLock::new(),
            node_recorder: OnceLock::new(),
            preview_limit: options.preview_limit,
            preview_fn: options.preview_fn.clone(),
            preview_recorder: OnceLock::new(),
            span_create_fn,
        }
    }

    /// Creates a new `InstrumentedExec` with the same configuration as this instance but with a different inner execution plan.
    ///
    /// This method is used when the optimizer needs to replace the inner execution plan while preserving
    /// all the instrumentation settings (metrics recording, preview limits, span creation function, etc.).
    fn with_new_inner(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<dyn ExecutionPlan> {
        Arc::new(InstrumentedExec::new(
            inner,
            self.span_create_fn.clone(),
            &InstrumentationOptions {
                record_metrics: self.record_metrics,
                preview_limit: self.preview_limit,
                preview_fn: self.preview_fn.clone(),
                custom_fields: HashMap::new(), // custom fields are not used by `InstrumentedExec`, only by the higher-level `instrument_with_spans` macro family
            },
        ))
    }

    /// Retrieves the tracing span, initializing it if necessary.
    fn get_span(&self) -> Span {
        self.span
            .get_or_init(|| self.create_populated_span())
            .clone()
    }

    /// Wraps the given stream with metrics recording if metrics are available.
    /// The input span is shared across all partitions,
    /// and metrics will be aggregated across all partitions before being reported.
    fn metrics_recording_stream(
        &self,
        inner_stream: SendableRecordBatchStream,
        span: &Span,
    ) -> SendableRecordBatchStream {
        if !self.record_metrics {
            return inner_stream;
        }
        let recorder = self
            .metrics_recorder
            .get_or_init(|| {
                Arc::new(MetricsRecorder::new(self.inner.clone(), span.clone()))
            })
            .clone();
        Box::pin(MetricsRecordingStream::new(inner_stream, recorder))
    }

    /// Wraps the given stream with batch preview recording.
    /// The input span is shared across all partitions,
    /// and the preview limit will be applied globally on all partitions before the preview is reported.
    fn preview_recording_stream(
        &self,
        inner_stream: SendableRecordBatchStream,
        span: &Span,
        partition: usize,
    ) -> SendableRecordBatchStream {
        if self.preview_limit == 0 {
            return inner_stream;
        }
        let recorder = self
            .preview_recorder
            .get_or_init(|| {
                let partition_count = self.inner.output_partitioning().partition_count();
                Arc::new(
                    PreviewRecorder::builder(span.clone(), partition_count)
                        .limit(self.preview_limit)
                        .preview_fn(self.preview_fn.clone())
                        .build(),
                )
            })
            .clone();
        Box::pin(PreviewRecordingStream::new(
            inner_stream,
            recorder,
            partition,
        ))
    }

    /// Wraps the given stream with a completion recorder so fields that are only
    /// fully qualified after execution (such as `datafusion.node`) are recorded
    /// once all partitions have finished executing.
    fn node_recording_stream(
        &self,
        inner_stream: SendableRecordBatchStream,
        span: &Span,
    ) -> SendableRecordBatchStream {
        let recorder = self
            .node_recorder
            .get_or_init(|| Arc::new(NodeRecorder::new(self.inner.clone(), span.clone())))
            .clone();
        Box::pin(NodeRecordingStream::new(inner_stream, recorder))
    }

    /// Creates a tracing span populated with metadata about the execution plan.
    fn create_populated_span(&self) -> Span {
        let span = self.span_create_fn.as_ref()();

        span.record("otel.name", field::display(self.inner.name()));
        span.record(
            "datafusion.partitioning",
            field::display(self.inner.properties().partitioning.clone()),
        );
        span.record(
            "datafusion.emission_type",
            field::debug(self.inner.properties().emission_type),
        );
        span.record(
            "datafusion.boundedness",
            field::debug(self.inner.properties().boundedness),
        );

        span
    }
}

#[warn(clippy::missing_trait_methods)]
impl ExecutionPlan for InstrumentedExec {
    // Delegate all ExecutionPlan methods to the inner plan, except for `as_any` and `execute`.
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn schema(&self) -> SchemaRef;
            fn properties(&self) -> &PlanProperties;
            fn check_invariants(&self, check: InvariantLevel) -> Result<()>;
            fn required_input_distribution(&self) -> Vec<Distribution>;
            fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn metrics(&self) -> Option<MetricsSet>;
            // We need to delegate to the inner plan's statistics method to preserve behavior,
            // even though it's deprecated in favor of partition_statistics
            #[allow(deprecated)]
            fn statistics(&self) -> Result<Statistics>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics>;
            fn supports_limit_pushdown(&self) -> bool;
            fn fetch(&self) -> Option<usize>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn gather_filters_for_pushdown(
                &self,
                phase: FilterPushdownPhase,
                parent_filters: Vec<Arc<dyn PhysicalExpr>>,
                config: &ConfigOptions,
            ) -> Result<FilterDescription>;
        }
    }

    fn static_name() -> &'static str {
        "InstrumentedExec"
    }

    /// Delegate to the inner plan for repartitioning and rewrap with an InstrumentedExec.
    fn repartitioned(
        &self,
        target_partitions: usize,
        config: &ConfigOptions,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(new_inner) = self
            .inner
            .clone()
            .repartitioned(target_partitions, config)?
        {
            Ok(Some(self.with_new_inner(new_inner)))
        } else {
            Ok(None)
        }
    }

    /// Delegate to the inner plan for fetching and rewrap with an InstrumentedExec.
    fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>> {
        if let Some(new_inner) = self.inner.clone().with_fetch(limit) {
            Some(self.with_new_inner(new_inner))
        } else {
            None
        }
    }

    /// Delegate to the inner plan for swapping with a projection and rewrap with an InstrumentedExec.
    fn try_swapping_with_projection(
        &self,
        projection: &ProjectionExec,
    ) -> Result<Option<Arc<dyn ExecutionPlan>>> {
        if let Some(new_inner) = self
            .inner
            .clone()
            .try_swapping_with_projection(projection)?
        {
            Ok(Some(self.with_new_inner(new_inner)))
        } else {
            Ok(None)
        }
    }

    /// Delegate to the inner plan for handling child pushdown result and rewrap with an InstrumentedExec.
    fn handle_child_pushdown_result(
        &self,
        phase: FilterPushdownPhase,
        child_pushdown_result: ChildPushdownResult,
        config: &ConfigOptions,
    ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>> {
        // If the inner plan updated itself, rewrap the updated node to preserve instrumentation
        let FilterPushdownPropagation {
            filters,
            updated_node,
        } = self.inner.handle_child_pushdown_result(
            phase,
            child_pushdown_result,
            config,
        )?;
        let updated_node = updated_node.map(|n| self.with_new_inner(n));
        Ok(FilterPushdownPropagation {
            filters,
            updated_node,
        })
    }

    /// Delegate to the inner plan for creating new children and rewrap with an InstrumentedExec.
    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let new_inner = self.inner.clone().with_new_children(children)?;
        Ok(self.with_new_inner(new_inner))
    }

    /// Delegate to the inner plan for resetting state and rewrap with an InstrumentedExec.
    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let new_inner = self.inner.clone().reset_state()?;
        Ok(self.with_new_inner(new_inner))
    }

    /// Delegate to the inner plan for injecting run-time state and rewrap with an InstrumentedExec.
    fn with_new_state(
        &self,
        state: Arc<dyn Any + Send + Sync>,
    ) -> Option<Arc<dyn ExecutionPlan>> {
        let new_inner = self.inner.with_new_state(state)?;
        Some(self.with_new_inner(new_inner))
    }

    fn as_any(&self) -> &dyn Any {
        self
    }

    /// Executes the plan for a given partition and context, instrumented with tracing and metrics recording.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let span = self.get_span();

        let inner_stream = self.inner.execute(partition, context)?;

        // Wrap the stream with node recording so `datafusion.node` is recorded only after
        // completion, once it is fully qualified.
        let node_stream = self.node_recording_stream(inner_stream, &span);

        // Wrap the stream with metrics recording capability (only if inner metrics are available).
        let metrics_stream = self.metrics_recording_stream(node_stream, &span);

        // Wrap the stream with batch preview recording (only if preview limit is set).
        let preview_stream =
            self.preview_recording_stream(metrics_stream, &span, partition);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.schema(),
            preview_stream.instrument(span),
        )))
    }
}

impl DisplayAs for InstrumentedExec {
    delegate! {
        to self.inner {
            fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result;
        }
    }
}
