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
    arrow::{array::RecordBatch, datatypes::SchemaRef},
    common::Statistics,
    config::ConfigOptions,
    error::Result,
    execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext},
    physical_expr::{Distribution, OrderingRequirements, PhysicalSortExpr},
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
        sort_pushdown::SortOrderPushdownResult,
        stream::RecordBatchStreamAdapter,
    },
};
use delegate::delegate;
use futures::Stream;
use pin_project::{pin_project, pinned_drop};
use std::{
    any::Any,
    collections::{HashMap, HashSet},
    fmt::{self, Debug},
    pin::Pin,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
use tracing::{Id, Span, field};
use tracing_futures::Instrument;

/// Type alias for a function that creates a tracing span.
pub(crate) type SpanCreateFn = dyn Fn() -> Span + Send + Sync;

/// An [`ExecutionPlan`] wrapper that instruments execution with tracing spans and metrics recording.
pub struct InstrumentedExec {
    /// The inner execution plan to delegate execution to.
    inner: Arc<dyn ExecutionPlan>,

    record_metrics: bool,

    preview_limit: usize,
    preview_fn: Option<Arc<PreviewFn>>,

    /// Shared recorder groups for active executions of this plan node.
    ///
    /// Groups are kept alive only until their streams have finished, so dropping
    /// an already-consumed plan does not close spans synchronously. Concurrent
    /// executions that belong to the same task context and touch distinct
    /// partitions share a group; independent or duplicate executions get a fresh
    /// group.
    recorders: Arc<Mutex<Vec<Arc<ExecutionRecorders>>>>,

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
            record_metrics: options.record_metrics,
            preview_limit: options.preview_limit,
            preview_fn: options.preview_fn.clone(),
            recorders: Arc::new(Mutex::new(Vec::new())),
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

    /// Returns and reserves the recorder group for this execution stream.
    ///
    /// The stream is reserved before calling `inner.execute` so concurrent
    /// callers cannot observe an idle recorder group and clear or reuse it
    /// incorrectly.
    fn reserve_recorders(
        &self,
        context: Arc<TaskContext>,
        partition: usize,
    ) -> Arc<ExecutionRecorders> {
        let parent_span_id = Span::current().id();

        let mut groups = self.recorders.lock().unwrap();
        for recorders in groups.iter() {
            if recorders.is_same_execution(parent_span_id.as_ref(), &context)
                && recorders.try_reserve_partition(partition)
            {
                return recorders.clone();
            }
        }

        let span = self.create_populated_span();
        let preview_recorder = (self.preview_limit > 0).then(|| {
            let partition_count = self.inner.output_partitioning().partition_count();
            PreviewRecorder::builder(span.clone(), partition_count)
                .limit(self.preview_limit)
                .preview_fn(self.preview_fn.clone())
                .build()
        });
        let recorders = Arc::new(ExecutionRecorders::new(
            Arc::downgrade(&self.recorders),
            parent_span_id,
            context,
            partition,
            NodeRecorder::new(self.inner.clone(), span.clone()),
            self.record_metrics
                .then(|| MetricsRecorder::new(self.inner.clone(), span.clone())),
            preview_recorder,
        ));
        groups.push(recorders.clone());
        recorders
    }

    /// Wraps the given stream so recorder state for this partition is released
    /// after the stream is dropped.
    fn execution_recording_stream(
        &self,
        inner_stream: SendableRecordBatchStream,
        recorders: Arc<ExecutionRecorders>,
    ) -> SendableRecordBatchStream {
        Box::pin(ExecutionRecordingStream::new(inner_stream, recorders))
    }

    /// Wraps the given stream with a completion recorder so fields that are only
    /// fully qualified after execution (such as `datafusion.node`) are recorded
    /// once all partitions have finished executing.
    fn node_recording_stream(
        &self,
        inner_stream: SendableRecordBatchStream,
        recorder: Arc<NodeRecorder>,
    ) -> SendableRecordBatchStream {
        Box::pin(NodeRecordingStream::new(inner_stream, recorder))
    }

    /// Wraps the given stream with metrics recording.
    /// Metrics are aggregated across all partitions before being reported.
    fn metrics_recording_stream(
        &self,
        inner_stream: SendableRecordBatchStream,
        recorder: Arc<MetricsRecorder>,
    ) -> SendableRecordBatchStream {
        Box::pin(MetricsRecordingStream::new(inner_stream, recorder))
    }

    /// Wraps the given stream with batch preview recording.
    /// The preview limit is applied globally across all partitions before the preview is reported.
    fn preview_recording_stream(
        &self,
        inner_stream: SendableRecordBatchStream,
        recorder: Arc<PreviewRecorder>,
        partition: usize,
    ) -> SendableRecordBatchStream {
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

    /// Returns true if the plan is an `InstrumentedExec` wrapper.
    ///
    pub(crate) fn is_instrumented(plan: &dyn ExecutionPlan) -> bool {
        plan.is::<InstrumentedExec>()
    }
}

impl ExecutionPlan for InstrumentedExec {
    // Most ExecutionPlan methods are delegated to the inner plan. Methods that must return a
    // wrapped plan or provide custom behavior are implemented manually below.
    delegate! {
        to self.inner {
            fn schema(&self) -> SchemaRef;
            fn properties(&self) -> &Arc<PlanProperties>;
            fn name(&self) -> &str;
            fn check_invariants(&self, check: InvariantLevel) -> Result<()>;
            fn required_input_distribution(&self) -> Vec<Distribution>;
            fn required_input_ordering(&self) -> Vec<Option<OrderingRequirements>>;
            fn maintains_input_order(&self) -> Vec<bool>;
            fn benefits_from_input_partitioning(&self) -> Vec<bool>;
            fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>>;
            fn metrics(&self) -> Option<MetricsSet>;
            fn partition_statistics(&self, partition: Option<usize>) -> Result<Arc<Statistics>>;
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

    /// Delegate to the inner plan for sort pushdown and rewrap with an InstrumentedExec.
    fn try_pushdown_sort(
        &self,
        order: &[PhysicalSortExpr],
    ) -> Result<SortOrderPushdownResult<Arc<dyn ExecutionPlan>>> {
        let result = self.inner.try_pushdown_sort(order)?;
        Ok(match result {
            SortOrderPushdownResult::Exact { inner } => SortOrderPushdownResult::Exact {
                inner: self.with_new_inner(inner),
            },
            SortOrderPushdownResult::Inexact { inner } => {
                SortOrderPushdownResult::Inexact {
                    inner: self.with_new_inner(inner),
                }
            }
            SortOrderPushdownResult::Unsupported => SortOrderPushdownResult::Unsupported,
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

    /// Executes the plan for a given partition and context, instrumented with tracing and metrics recording.
    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        let recorders = self.reserve_recorders(context.clone(), partition);
        let span = recorders.span();

        let inner_stream = match span.in_scope(|| self.inner.execute(partition, context))
        {
            Ok(stream) => stream,
            Err(error) => {
                recorders.cancel_stream(partition);
                return Err(error);
            }
        };

        // Wrap the stream with node recording so `datafusion.node` is recorded only after
        // completion, once it is fully qualified.
        let node_stream =
            self.node_recording_stream(inner_stream, recorders.node_recorder.clone());

        // Wrap the stream with metrics recording capability (only if inner metrics are available).
        let metrics_stream = if let Some(metrics_recorder) = &recorders.metrics_recorder {
            self.metrics_recording_stream(node_stream, metrics_recorder.clone())
        } else {
            node_stream
        };

        // Wrap the stream with batch preview recording (only if preview limit is set).
        let preview_stream = if let Some(preview_recorder) = &recorders.preview_recorder {
            self.preview_recording_stream(
                metrics_stream,
                preview_recorder.clone(),
                partition,
            )
        } else {
            metrics_stream
        };
        let recording_stream = self.execution_recording_stream(preview_stream, recorders);

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.inner.schema(),
            recording_stream.instrument(span),
        )))
    }
}

struct ExecutionRecorders {
    slot: Weak<Mutex<Vec<Arc<ExecutionRecorders>>>>,
    parent_span_id: Option<Id>,
    context: Arc<TaskContext>,
    seen_partitions: Mutex<HashSet<usize>>,
    active_streams: AtomicUsize,
    node_recorder: Arc<NodeRecorder>,
    metrics_recorder: Option<Arc<MetricsRecorder>>,
    preview_recorder: Option<Arc<PreviewRecorder>>,
}

impl ExecutionRecorders {
    fn new(
        slot: Weak<Mutex<Vec<Arc<ExecutionRecorders>>>>,
        parent_span_id: Option<Id>,
        context: Arc<TaskContext>,
        partition: usize,
        node_recorder: NodeRecorder,
        metrics_recorder: Option<MetricsRecorder>,
        preview_recorder: Option<PreviewRecorder>,
    ) -> Self {
        Self {
            slot,
            parent_span_id,
            context,
            seen_partitions: Mutex::new(HashSet::from([partition])),
            active_streams: AtomicUsize::new(1),
            node_recorder: Arc::new(node_recorder),
            metrics_recorder: metrics_recorder.map(Arc::new),
            preview_recorder: preview_recorder.map(Arc::new),
        }
    }

    fn span(&self) -> Span {
        self.node_recorder.span()
    }

    fn is_same_execution(
        &self,
        parent_span_id: Option<&Id>,
        context: &Arc<TaskContext>,
    ) -> bool {
        match (&self.parent_span_id, parent_span_id) {
            (Some(a), Some(b)) => a == b && Arc::ptr_eq(&self.context, context),
            (None, None) => Arc::ptr_eq(&self.context, context),
            _ => false,
        }
    }

    fn try_reserve_partition(&self, partition: usize) -> bool {
        let mut seen_partitions = self.seen_partitions.lock().unwrap();
        if seen_partitions.contains(&partition) {
            return false;
        }

        seen_partitions.insert(partition);
        self.active_streams.fetch_add(1, Ordering::AcqRel);
        true
    }

    fn finish_stream(self: &Arc<Self>) {
        self.release_stream(None);
    }

    fn cancel_stream(self: &Arc<Self>, partition: usize) {
        self.release_stream(Some(partition));
    }

    fn release_stream(self: &Arc<Self>, canceled_partition: Option<usize>) {
        if let Some(slot) = self.slot.upgrade() {
            let mut groups = slot.lock().unwrap();

            if let Some(partition) = canceled_partition {
                self.seen_partitions.lock().unwrap().remove(&partition);
            }

            if self.active_streams.fetch_sub(1, Ordering::AcqRel) == 1 {
                groups.retain(|recorders| !Arc::ptr_eq(recorders, self));
            }
        }
    }
}

#[pin_project(PinnedDrop)]
struct ExecutionRecordingStream {
    #[pin]
    inner: SendableRecordBatchStream,
    recorders: Arc<ExecutionRecorders>,
}

impl ExecutionRecordingStream {
    fn new(inner: SendableRecordBatchStream, recorders: Arc<ExecutionRecorders>) -> Self {
        Self { inner, recorders }
    }
}

impl Stream for ExecutionRecordingStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

#[pinned_drop]
impl PinnedDrop for ExecutionRecordingStream {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        this.recorders.finish_stream();
    }
}

impl RecordBatchStream for ExecutionRecordingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

impl DisplayAs for InstrumentedExec {
    delegate! {
        to self.inner {
            fn fmt_as(&self, format: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::common::DataFusionError;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::prelude::SessionContext;
    use futures::StreamExt;
    use std::collections::HashMap;
    use std::sync::atomic::AtomicBool;
    use tracing::field::{Field, Visit};
    use tracing::{Id, Subscriber};
    use tracing_subscriber::Layer;
    use tracing_subscriber::layer::Context;
    use tracing_subscriber::prelude::*;
    use tracing_subscriber::registry::LookupSpan;

    // -----------------------------------------------------------------------
    // Minimal span-event capture layer
    // -----------------------------------------------------------------------

    struct CapturedName(String);
    struct CapturedFields(HashMap<String, String>);

    #[derive(Clone, Default)]
    struct SpanCapture(Arc<Mutex<Vec<SpanEvent>>>);

    #[derive(Clone)]
    struct SpanEvent {
        kind: &'static str,
        name: String,
        fields: HashMap<String, String>,
    }

    #[derive(Default)]
    struct FieldVisitor(HashMap<String, String>);

    impl Visit for FieldVisitor {
        fn record_str(&mut self, field: &Field, value: &str) {
            self.0.insert(field.name().to_owned(), value.to_owned());
        }

        fn record_debug(&mut self, field: &Field, value: &dyn Debug) {
            self.0.insert(field.name().to_owned(), format!("{value:?}"));
        }
    }

    impl SpanCapture {
        fn opened(&self, name: &str) -> usize {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.kind == "open" && e.name == name)
                .count()
        }

        fn closed(&self, name: &str) -> usize {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.kind == "close" && e.name == name)
                .count()
        }

        fn closed_field_values(&self, name: &str, field: &str) -> Vec<String> {
            self.0
                .lock()
                .unwrap()
                .iter()
                .filter(|e| e.kind == "close" && e.name == name)
                .filter_map(|e| e.fields.get(field).cloned())
                .collect()
        }
    }

    impl<S: Subscriber + for<'s> LookupSpan<'s>> Layer<S> for SpanCapture {
        fn on_new_span(
            &self,
            attrs: &tracing::span::Attributes<'_>,
            id: &Id,
            ctx: Context<'_, S>,
        ) {
            let mut visitor = FieldVisitor::default();
            attrs.record(&mut visitor);
            let name = visitor
                .0
                .get("otel.name")
                .cloned()
                .unwrap_or_else(|| attrs.metadata().name().to_owned());
            if let Some(span) = ctx.span(id) {
                let mut extensions = span.extensions_mut();
                extensions.insert(CapturedName(name.clone()));
                extensions.insert(CapturedFields(visitor.0.clone()));
            }
            self.0.lock().unwrap().push(SpanEvent {
                kind: "open",
                name,
                fields: visitor.0,
            });
        }

        fn on_record(
            &self,
            id: &Id,
            values: &tracing::span::Record<'_>,
            ctx: Context<'_, S>,
        ) {
            let mut visitor = FieldVisitor::default();
            values.record(&mut visitor);
            if let Some(span) = ctx.span(id)
                && let Some(fields) = span.extensions_mut().get_mut::<CapturedFields>()
            {
                fields.0.extend(visitor.0);
            }
        }

        fn on_close(&self, id: Id, ctx: Context<'_, S>) {
            let (name, fields) = ctx
                .span(&id)
                .map(|span| {
                    let extensions = span.extensions();
                    let name = extensions
                        .get::<CapturedName>()
                        .map(|n| n.0.clone())
                        .unwrap_or_default();
                    let fields = extensions
                        .get::<CapturedFields>()
                        .map(|fields| fields.0.clone())
                        .unwrap_or_default();
                    (name, fields)
                })
                .unwrap_or_default();
            self.0.lock().unwrap().push(SpanEvent {
                kind: "close",
                name,
                fields,
            });
        }
    }

    // -----------------------------------------------------------------------
    // Context helper
    // -----------------------------------------------------------------------

    async fn make_ctx() -> SessionContext {
        make_ctx_with_options(InstrumentationOptions::default()).await
    }

    async fn make_ctx_with_options(options: InstrumentationOptions) -> SessionContext {
        let rule = crate::instrument_with_info_spans!(options: options);
        let state = SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rule(rule)
            .build();
        SessionContext::new_with_state(state)
    }

    // -----------------------------------------------------------------------
    // Tests
    // -----------------------------------------------------------------------

    /// Regression for issue #27: spans must close when the last execution stream
    /// is consumed, not when the plan is dropped.
    ///
    /// Before the fix, `InstrumentedExec` held `OnceLock<Arc<*Recorder>>` and a
    /// `Span` clone, keeping spans alive until `drop(plan)`. With
    /// `SimpleSpanProcessor` that caused `drop()` to block for
    /// `N_nodes × OTLP_latency` seconds.
    #[tokio::test]
    async fn span_closes_when_stream_finishes_not_when_plan_drops() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let ctx = make_ctx().await;
        let plan = ctx
            .sql("SELECT 1")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let plan_clone = plan.clone(); // keep plan alive after streams are consumed

        let task_ctx = ctx.task_ctx();
        for part in 0..plan.properties().partitioning.partition_count() {
            let mut stream = plan.execute(part, task_ctx.clone()).unwrap();
            while let Some(b) = stream.next().await {
                b.unwrap();
            }
        }
        drop(plan);

        // Spans are already closed — they were closed when the streams finished.
        let closed_after_collect = capture.closed("InstrumentedExec");
        assert!(
            closed_after_collect > 0,
            "InstrumentedExec spans must close when streams finish"
        );

        // Dropping the extra plan clone must not trigger any new span closures.
        drop(plan_clone);
        assert_eq!(
            capture.closed("InstrumentedExec"),
            closed_after_collect,
            "dropping the plan must not close additional spans (regression: issue #27)"
        );
    }

    /// Spans must remain open while execution streams are alive, even after the
    /// plan itself is dropped. Span lifetime tracks stream lifetime.
    #[tokio::test]
    async fn span_stays_open_while_stream_alive() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let ctx = make_ctx().await;
        let plan = ctx
            .sql("SELECT 1")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();

        let task_ctx = ctx.task_ctx();
        // Collect all streams before dropping the plan, so the streams (and their
        // Arc<NodeRecorder>) are alive while the plan Weak is dropped.
        let streams: Vec<_> = (0..plan.properties().partitioning.partition_count())
            .map(|p| plan.execute(p, task_ctx.clone()).unwrap())
            .collect();

        // Drop the plan — only the Weak<NodeRecorder> is released, not the span.
        drop(plan);
        assert_eq!(
            capture.closed("InstrumentedExec"),
            0,
            "spans must not close when the plan drops while streams are still alive"
        );

        // Consuming and dropping the streams releases the Arc<NodeRecorder>.
        for mut stream in streams {
            while let Some(b) = stream.next().await {
                b.unwrap();
            }
        }
        assert!(
            capture.closed("InstrumentedExec") > 0,
            "InstrumentedExec spans must close once all streams are consumed"
        );
    }

    fn two_partition_inner() -> Arc<dyn ExecutionPlan> {
        use datafusion::arrow::array::Int64Array;
        use datafusion::arrow::datatypes::{DataType, Field, Schema};
        use datafusion::arrow::record_batch::RecordBatch;
        use datafusion::physical_plan::test::TestMemoryExec;

        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch_1 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        let batch_2 = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![2]))],
        )
        .unwrap();
        TestMemoryExec::try_new_exec(&[vec![batch_1], vec![batch_2]], schema, None)
            .unwrap()
    }

    fn two_partition_plan() -> InstrumentedExec {
        two_partition_plan_with_options(&InstrumentationOptions::default())
    }

    fn two_partition_plan_with_options(
        options: &InstrumentationOptions,
    ) -> InstrumentedExec {
        InstrumentedExec::new(
            two_partition_inner(),
            Arc::new(|| {
                tracing::info_span!(
                    "InstrumentedExec",
                    datafusion.preview = field::Empty,
                    datafusion.metrics.output_rows = field::Empty,
                )
            }),
            options,
        )
    }

    struct FailFirstExecute {
        inner: Arc<dyn ExecutionPlan>,
        fail_next: AtomicBool,
    }

    impl FailFirstExecute {
        fn new(inner: Arc<dyn ExecutionPlan>) -> Self {
            Self {
                inner,
                fail_next: AtomicBool::new(true),
            }
        }
    }

    impl Debug for FailFirstExecute {
        fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
            f.debug_struct("FailFirstExecute")
                .field("inner", &self.inner)
                .finish()
        }
    }

    impl DisplayAs for FailFirstExecute {
        fn fmt_as(
            &self,
            format: DisplayFormatType,
            f: &mut fmt::Formatter<'_>,
        ) -> fmt::Result {
            self.inner.fmt_as(format, f)
        }
    }

    impl ExecutionPlan for FailFirstExecute {
        fn name(&self) -> &str {
            self.inner.name()
        }

        fn properties(&self) -> &Arc<PlanProperties> {
            self.inner.properties()
        }

        fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
            self.inner.children()
        }

        fn with_new_children(
            self: Arc<Self>,
            children: Vec<Arc<dyn ExecutionPlan>>,
        ) -> Result<Arc<dyn ExecutionPlan>> {
            let inner = self.inner.clone().with_new_children(children)?;
            Ok(Arc::new(FailFirstExecute::new(inner)))
        }

        fn execute(
            &self,
            partition: usize,
            context: Arc<TaskContext>,
        ) -> Result<SendableRecordBatchStream> {
            if self.fail_next.swap(false, Ordering::AcqRel) {
                return Err(DataFusionError::Internal(
                    "intentional execute failure".into(),
                ));
            }
            self.inner.execute(partition, context)
        }
    }

    /// Concurrent partition streams from one execution share a recorder group,
    /// preserving aggregation while those streams are active.
    #[tokio::test]
    async fn concurrent_partitions_share_one_span() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let plan = two_partition_plan();
        let task_ctx = Arc::new(TaskContext::default());
        let streams: Vec<_> = (0..2)
            .map(|part| plan.execute(part, task_ctx.clone()).unwrap())
            .collect();

        for mut stream in streams {
            while let Some(batch) = stream.next().await {
                batch.unwrap();
            }
        }

        assert_eq!(
            capture.opened("InstrumentedExec"),
            1,
            "concurrent partition streams should share one span"
        );
        assert_eq!(
            capture.closed("InstrumentedExec"),
            1,
            "shared span should close when the active stream group finishes"
        );
    }

    /// Concurrent executions on different task contexts represent independent
    /// executions and must not share a span.
    #[tokio::test]
    async fn different_task_contexts_get_fresh_spans() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let plan = two_partition_plan();
        let task_ctx_1 = Arc::new(TaskContext::default());
        let task_ctx_2 = Arc::new(TaskContext::default());
        let streams = vec![
            plan.execute(0, task_ctx_1).unwrap(),
            plan.execute(1, task_ctx_2).unwrap(),
        ];

        for mut stream in streams {
            while let Some(batch) = stream.next().await {
                batch.unwrap();
            }
        }

        assert_eq!(
            capture.opened("InstrumentedExec"),
            2,
            "different task contexts should create independent spans"
        );
        assert_eq!(
            capture.closed("InstrumentedExec"),
            2,
            "each independent context execution should close its own span"
        );
    }

    /// The current parent span is a stronger execution identity than the task
    /// context. Concurrent executions with the same task context but different
    /// parent spans must not share recorder state.
    #[tokio::test]
    async fn different_parent_spans_get_fresh_spans() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let plan = two_partition_plan();
        let task_ctx = Arc::new(TaskContext::default());
        let parent_1 = tracing::info_span!("parent_1");
        let parent_2 = tracing::info_span!("parent_2");
        let streams = vec![
            parent_1.in_scope(|| plan.execute(0, task_ctx.clone()).unwrap()),
            parent_2.in_scope(|| plan.execute(1, task_ctx).unwrap()),
        ];

        for mut stream in streams {
            while let Some(batch) = stream.next().await {
                batch.unwrap();
            }
        }

        assert_eq!(
            capture.opened("InstrumentedExec"),
            2,
            "different parent spans should create independent spans"
        );
        assert_eq!(
            capture.closed("InstrumentedExec"),
            2,
            "each parent-span execution should close its own span"
        );
    }

    /// `ExecutionPlan::execute` may legally be called for only a subset of
    /// partitions. Recorder lifetime must not wait for partitions that were
    /// never executed.
    #[tokio::test]
    async fn partial_partition_execution_closes_span() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let plan = two_partition_plan();
        let task_ctx = Arc::new(TaskContext::default());
        let mut stream = plan.execute(0, task_ctx).unwrap();
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        drop(stream);

        assert_eq!(
            capture.opened("InstrumentedExec"),
            1,
            "same plan node should create one span across sequential partitions"
        );
        assert_eq!(
            capture.closed("InstrumentedExec"),
            1,
            "span should close after the only executed partition stream finishes"
        );
    }

    /// Recorder acquisition reserves a stream before calling the inner plan.
    /// If the inner plan fails to execute, the reservation must be released so
    /// the failed group is not left active forever.
    #[tokio::test]
    async fn execute_error_releases_reserved_recorder_group() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let plan = InstrumentedExec::new(
            Arc::new(FailFirstExecute::new(two_partition_inner())),
            Arc::new(|| tracing::info_span!("InstrumentedExec")),
            &InstrumentationOptions::default(),
        );
        let task_ctx = Arc::new(TaskContext::default());

        assert!(plan.execute(0, task_ctx.clone()).is_err());

        let mut stream = plan.execute(0, task_ctx).unwrap();
        while let Some(batch) = stream.next().await {
            batch.unwrap();
        }
        drop(stream);

        assert_eq!(
            capture.opened("InstrumentedExec"),
            2,
            "failed execute should close its span and retry should create a fresh one"
        );
        assert_eq!(
            capture.closed("InstrumentedExec"),
            2,
            "failed execute must not leave an active recorder group behind"
        );
    }

    /// Repeated execution of the same partition is legal. Each independent
    /// stream group should close before the next one starts instead of reusing
    /// stale completion state.
    #[tokio::test]
    async fn repeated_same_partition_execution_gets_fresh_span() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let plan = two_partition_plan();
        let task_ctx = Arc::new(TaskContext::default());
        for _ in 0..2 {
            let mut stream = plan.execute(0, task_ctx.clone()).unwrap();
            while let Some(batch) = stream.next().await {
                batch.unwrap();
            }
        }

        assert_eq!(
            capture.opened("InstrumentedExec"),
            2,
            "separate stream groups should create separate spans"
        );
        assert_eq!(
            capture.closed("InstrumentedExec"),
            2,
            "each repeated execution should close its own span"
        );
    }

    /// Overlapping execution of the same partition is legal. Each active
    /// duplicate stream should get a separate span so their recorder state does
    /// not mix.
    #[tokio::test]
    async fn overlapping_same_partition_execution_gets_fresh_spans() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let plan = two_partition_plan();
        let task_ctx = Arc::new(TaskContext::default());
        let mut stream_1 = plan.execute(0, task_ctx.clone()).unwrap();
        let mut stream_2 = plan.execute(0, task_ctx).unwrap();

        while let Some(batch) = stream_1.next().await {
            batch.unwrap();
        }
        while let Some(batch) = stream_2.next().await {
            batch.unwrap();
        }
        drop(stream_1);
        drop(stream_2);

        assert_eq!(
            capture.opened("InstrumentedExec"),
            2,
            "overlapping duplicate partition streams should create separate spans"
        );
        assert_eq!(
            capture.closed("InstrumentedExec"),
            2,
            "each overlapping duplicate stream should close its own span"
        );
    }

    /// Preview recorders are owned by a recorder group. Independent groups must
    /// record independent previews instead of sharing preview state.
    #[tokio::test]
    async fn previews_do_not_mix_between_independent_recorder_groups() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let options = InstrumentationOptions::builder().preview_limit(5).build();
        let plan = two_partition_plan_with_options(&options);
        let task_ctx_1 = Arc::new(TaskContext::default());
        let task_ctx_2 = Arc::new(TaskContext::default());
        let mut stream_1 = plan.execute(0, task_ctx_1).unwrap();
        let mut stream_2 = plan.execute(1, task_ctx_2).unwrap();

        while let Some(batch) = stream_1.next().await {
            batch.unwrap();
        }
        while let Some(batch) = stream_2.next().await {
            batch.unwrap();
        }
        drop(stream_1);
        drop(stream_2);

        let previews =
            capture.closed_field_values("InstrumentedExec", "datafusion.preview");
        assert_eq!(
            previews.len(),
            2,
            "independent recorder groups should each close with a preview"
        );
        assert!(
            previews.iter().any(|preview| preview.contains("| 1 |")),
            "one recorder group should preview partition 0"
        );
        assert!(
            previews.iter().any(|preview| preview.contains("| 2 |")),
            "one recorder group should preview partition 1"
        );
        assert!(
            previews
                .iter()
                .all(|preview| !preview.contains("| 1 |\n|---|\n| 2 |")),
            "independent recorder group previews must not be concatenated"
        );
    }

    /// Metrics are still DataFusion's native plan metrics, but they should be
    /// recorded when the stream-owned recorder group closes.
    #[tokio::test]
    async fn metrics_are_recorded_when_stream_group_closes() {
        let capture = SpanCapture::default();
        let _guard = tracing::subscriber::set_default(
            tracing_subscriber::registry()
                .with(tracing::level_filters::LevelFilter::INFO)
                .with(capture.clone()),
        );

        let options = InstrumentationOptions::builder()
            .record_metrics(true)
            .build();
        let ctx = make_ctx_with_options(options).await;
        let plan = ctx
            .sql("SELECT 1")
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let task_ctx = ctx.task_ctx();

        for partition in 0..plan.properties().partitioning.partition_count() {
            let mut stream = plan.execute(partition, task_ctx.clone()).unwrap();
            while let Some(batch) = stream.next().await {
                batch.unwrap();
            }
            drop(stream);
        }

        let output_rows = capture
            .closed_field_values("InstrumentedExec", "datafusion.metrics.output_rows");
        assert!(
            output_rows.iter().any(|value| value == "1"),
            "stream group close should record native DataFusion output row metrics"
        );
    }
}
