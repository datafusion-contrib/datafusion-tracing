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
    options::InstrumentationOptions,
    preview::{PreviewFn, PreviewRecorder, PreviewRecordingStream},
    utils::DefaultDisplay,
};
use datafusion::{
    arrow::datatypes::SchemaRef,
    common::Statistics,
    config::ConfigOptions,
    error::Result,
    execution::{SendableRecordBatchStream, TaskContext},
    physical_expr::{Distribution, LexRequirement},
    physical_plan::{
        execution_plan::{CardinalityEffect, InvariantLevel},
        filter_pushdown::{
            ChildPushdownResult, FilterDescription, FilterPushdownPropagation,
        },
        metrics::MetricsSet,
        projection::ProjectionExec,
        stream::RecordBatchStreamAdapter,
        DisplayAs, DisplayFormatType, ExecutionPlan, ExecutionPlanProperties,
        PhysicalExpr, PlanProperties,
    },
};
use delegate::delegate;
use std::{
    any::Any,
    fmt,
    fmt::Debug,
    sync::{Arc, OnceLock},
};
use tracing::{field, Span};
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
            preview_limit: options.preview_limit,
            preview_fn: options.preview_fn.clone(),
            preview_recorder: OnceLock::new(),
            span_create_fn,
        }
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

    /// Creates a tracing span populated with metadata about the execution plan.
    fn create_populated_span(&self) -> Span {
        let span = self.span_create_fn.as_ref()();

        span.record("otel.name", field::display(self.inner.name()));
        span.record(
            "datafusion.node",
            field::display(DefaultDisplay(self.inner.as_ref())),
        );
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

impl ExecutionPlan for InstrumentedExec {
    // Delegate all ExecutionPlan methods to the inner plan, except for `as_any` and `execute`.
    //
    // Note: Methods returning a new ExecutionPlan instance delegate directly to the inner plan,
    // resulting in loss of instrumentation. This is intentional since instrumentation should
    // be applied by the final PhysicalOptimizerRule pass. Therefore, any resulting plans will
    // be re-instrumented after optimizer transformations.
    delegate! {
        to self.inner {
            fn name(&self) -> &str;
            fn schema(&self) -> SchemaRef;
            fn properties(&self) -> &PlanProperties;
            fn check_invariants(&self, check: InvariantLevel) -> Result<()>;
            fn required_input_distribution(&self) -> Vec<Distribution>;
            fn required_input_ordering(&self) -> Vec<Option<LexRequirement>>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn repartitioned(
                &self,
                target_partitions: usize,
                config: &ConfigOptions,
            ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn metrics(&self) -> Option<MetricsSet>;
            // We need to delegate to the inner plan's statistics method to preserve behavior,
            // even though it's deprecated in favor of partition_statistics
            #[allow(deprecated)]
            fn statistics(&self) -> Result<Statistics>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Statistics>;
            fn supports_limit_pushdown(&self) -> bool;
            fn with_fetch(&self, limit: Option<usize>) -> Option<Arc<dyn ExecutionPlan>>;
            fn fetch(&self) -> Option<usize>;
            fn cardinality_effect(&self) -> CardinalityEffect;
            fn try_swapping_with_projection(
                &self,
                projection: &ProjectionExec,
            ) -> Result<Option<Arc<dyn ExecutionPlan>>>;
            fn gather_filters_for_pushdown(
                &self,
                parent_filters: Vec<Arc<dyn PhysicalExpr>>,
                config: &ConfigOptions,
            ) -> Result<FilterDescription>;
            fn handle_child_pushdown_result(
                &self,
                child_pushdown_result: ChildPushdownResult,
                config: &ConfigOptions,
            ) -> Result<FilterPushdownPropagation<Arc<dyn ExecutionPlan>>>;
        }
    }

    delegate! {
        to self.inner.clone() {
            fn with_new_children(
                self: Arc<Self>,
                children: Vec<Arc<dyn ExecutionPlan>>,
            ) -> Result<Arc<dyn ExecutionPlan>>;
        }
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

        let inner_stream = span.in_scope(|| self.inner.execute(partition, context))?;

        // Wrap the inner stream with metrics recording capability (only if inner metrics are available).
        let metrics_stream = self.metrics_recording_stream(inner_stream, &span);

        // Wrap the inner stream with batch preview recording (only if preview limit is set).
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
