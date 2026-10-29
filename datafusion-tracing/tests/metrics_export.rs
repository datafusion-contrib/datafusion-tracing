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

//! Public API regressions for native metrics accounting and stream lifetimes.

use datafusion::arrow::array::{ArrayRef, Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{
    DataFusionError, Result,
    tree_node::{TreeNode, TreeNodeRecursion},
};
use datafusion::config::ConfigOptions;
use datafusion::execution::{
    RecordBatchStream, SendableRecordBatchStream, SessionStateBuilder, TaskContext,
};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::metrics::{
    Count, ExecutionPlanMetricsSet, Metric, MetricBuilder, MetricValue, MetricsSet,
    RatioMetrics, Time,
};
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::recursive_query::RecursiveQueryExec;
use datafusion::physical_plan::work_table::WorkTableExec;
use datafusion::physical_plan::{
    ChildrenPropertiesMode, DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr,
    PlanProperties, ReplaceChildrenOptions, collect,
};
use datafusion::prelude::{SessionConfig, SessionContext};
use datafusion_tracing::metrics_export::{ExecutionMetric, MetricsExporter, MetricsSink};
use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};
use futures::{Stream, StreamExt};
use std::fmt;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};
use std::time::Duration;

const OPERATOR: &str = "MetricsExec";

fn value(exporter: &MetricsExporter, metric: ExecutionMetric) -> u64 {
    exporter
        .snapshot()
        .iter()
        .filter(|entry| entry.operator == OPERATOR)
        .map(|entry| entry.value(metric))
        .sum()
}

fn instrument(
    inner: Arc<dyn ExecutionPlan>,
    exporter: &MetricsExporter,
) -> Arc<dyn ExecutionPlan> {
    let options = InstrumentationOptions::builder()
        .metrics_exporter(exporter.clone())
        .build();
    instrument_with_info_spans!(options: options)
        .optimize(inner, &ConfigOptions::default())
        .unwrap()
}

async fn consume(mut stream: SendableRecordBatchStream) -> usize {
    let mut rows = 0;
    while let Some(batch) = stream.next().await {
        rows += batch.unwrap().num_rows();
    }
    rows
}

#[derive(Clone, Copy, Debug)]
enum Behavior {
    Complete,
    PendingAfterBatch,
    ErrorAfterBatch,
    ExecuteError,
}

#[derive(Clone, Debug)]
struct NativeCounters {
    rows: Count,
    batches: Count,
    bytes: Count,
    spills: Count,
    spilled_rows: Count,
    spilled_bytes: Count,
    compute: Time,
}

impl NativeCounters {
    fn new(metrics: &ExecutionPlanMetricsSet, partition: usize) -> Self {
        Self {
            rows: MetricBuilder::new(metrics).output_rows(partition),
            batches: MetricBuilder::new(metrics).output_batches(partition),
            bytes: MetricBuilder::new(metrics).output_bytes(partition),
            spills: MetricBuilder::new(metrics).spill_count(partition),
            spilled_rows: MetricBuilder::new(metrics).spilled_rows(partition),
            spilled_bytes: MetricBuilder::new(metrics).spilled_bytes(partition),
            compute: MetricBuilder::new(metrics).elapsed_compute(partition),
        }
    }

    fn record(&self, batch: &RecordBatch) {
        self.rows.add(batch.num_rows());
        self.batches.add(1);
        self.bytes.add(batch.get_array_memory_size());
        self.spills.add(1);
        self.spilled_rows.add(2);
        self.spilled_bytes.add(5);
        self.compute.add_duration(Duration::from_nanos(42));
    }
}

/// Unlike a canned memory source, this fixture deliberately reuses its native
/// registrations across execute calls. It also finalizes a metric during Drop.
#[derive(Debug)]
struct MetricsExec {
    operator: String,
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    metrics_reads: Arc<AtomicUsize>,
    rebuild_metrics: bool,
    ephemeral_ratio: bool,
    counters: Vec<NativeCounters>,
    behavior: Behavior,
    drop_spilled_bytes: usize,
}

impl MetricsExec {
    fn new(partitions: usize, behavior: Behavior) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let properties = EmptyExec::new(schema)
            .with_partitions(partitions)
            .properties()
            .clone();
        let metrics = ExecutionPlanMetricsSet::new();
        let counters = (0..partitions)
            .map(|partition| NativeCounters::new(&metrics, partition))
            .collect();
        Self {
            operator: OPERATOR.to_owned(),
            properties,
            metrics,
            metrics_reads: Arc::new(AtomicUsize::new(0)),
            rebuild_metrics: false,
            ephemeral_ratio: false,
            counters,
            behavior,
            drop_spilled_bytes: 0,
        }
    }
}

impl DisplayAs for MetricsExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(OPERATOR)
    }
}

impl ExecutionPlan for MetricsExec {
    fn name(&self) -> &str {
        &self.operator
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }

    fn apply_expressions(
        &self,
        _: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> Result<TreeNodeRecursion>,
    ) -> Result<TreeNodeRecursion> {
        Ok(TreeNodeRecursion::Continue)
    }

    fn replace_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
        _: ReplaceChildrenOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        assert!(children.is_empty());
        Ok(self)
    }

    fn reset_state(self: Arc<Self>) -> Result<Arc<dyn ExecutionPlan>> {
        let mut fresh = Self::new(self.counters.len(), self.behavior);
        fresh.operator.clone_from(&self.operator);
        fresh.rebuild_metrics = self.rebuild_metrics;
        fresh.ephemeral_ratio = self.ephemeral_ratio;
        fresh.drop_spilled_bytes = self.drop_spilled_bytes;
        Ok(Arc::new(fresh))
    }

    fn metrics(&self) -> Option<MetricsSet> {
        self.metrics_reads.fetch_add(1, Ordering::Relaxed);
        let mut metrics = self.metrics.clone_inner();
        if self.ephemeral_ratio {
            metrics.push(Arc::new(Metric::new(
                MetricValue::Ratio {
                    name: "output_rows_skew".into(),
                    ratio_metrics: RatioMetrics::new(),
                },
                None,
            )));
        }
        if self.rebuild_metrics {
            Some(
                metrics
                    .iter()
                    .map(|metric| {
                        Arc::new(Metric::new(metric.value().clone(), metric.partition()))
                    })
                    .collect(),
            )
        } else {
            Some(metrics)
        }
    }

    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if matches!(self.behavior, Behavior::ExecuteError) {
            return Err(DataFusionError::Execution("execute failed".into()));
        }
        Ok(Box::pin(MetricsStream {
            schema: self.schema(),
            counters: self.counters[partition].clone(),
            partition,
            next_batch: 0,
            behavior: self.behavior,
            drop_spilled_bytes: self.drop_spilled_bytes,
        }))
    }
}

struct MetricsStream {
    schema: SchemaRef,
    counters: NativeCounters,
    partition: usize,
    next_batch: usize,
    behavior: Behavior,
    drop_spilled_bytes: usize,
}

impl Stream for MetricsStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        if self.next_batch == 1 {
            match self.behavior {
                Behavior::PendingAfterBatch => return Poll::Pending,
                Behavior::ErrorAfterBatch => {
                    self.next_batch = 2;
                    return Poll::Ready(Some(Err(DataFusionError::Execution(
                        "stream failed".into(),
                    ))));
                }
                _ => {}
            }
        }
        if self.next_batch == 2 {
            return Poll::Ready(None);
        }
        let size = [3, 7][self.next_batch] * (self.partition + 1);
        self.next_batch += 1;
        let batch = RecordBatch::try_new(
            self.schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1; size]))],
        )
        .unwrap();
        self.counters.record(&batch);
        Poll::Ready(Some(Ok(batch)))
    }
}

impl RecordBatchStream for MetricsStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for MetricsStream {
    fn drop(&mut self) {
        self.counters.spilled_bytes.add(self.drop_spilled_bytes);
    }
}

#[tokio::test]
async fn sql_exports_native_metrics_without_a_tracing_subscriber() {
    let exporter = MetricsExporter::new();
    let options = InstrumentationOptions::builder()
        .metrics_exporter(exporter.clone())
        .build();
    let state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(instrument_with_info_spans!(options: options))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let batches = ctx
        .sql("SELECT 1 + 2 AS answer")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    assert_eq!(batches.iter().map(RecordBatch::num_rows).sum::<usize>(), 1);
    let snapshot = exporter.snapshot();
    assert!(
        snapshot
            .iter()
            .any(|entry| entry.value(ExecutionMetric::OutputRows) == 1)
    );
    assert!(
        snapshot
            .iter()
            .any(|entry| entry.value(ExecutionMetric::StreamsCompleted) == 1)
    );
    assert!(
        snapshot
            .iter()
            .all(|entry| entry.value(ExecutionMetric::StreamsFailed) == 0)
    );
}

#[tokio::test]
async fn sequential_partitions_export_deltas_instead_of_recounting_snapshots() {
    let exporter = MetricsExporter::new();
    let plan = instrument(Arc::new(MetricsExec::new(2, Behavior::Complete)), &exporter);
    let context = Arc::new(TaskContext::default());
    assert_eq!(consume(plan.execute(0, context.clone()).unwrap()).await, 10);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 10);
    assert_eq!(consume(plan.execute(1, context).unwrap()).await, 20);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 30);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsStarted), 2);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 2);
    assert_eq!(value(&exporter, ExecutionMetric::OutputBatches), 4);
    assert_eq!(value(&exporter, ExecutionMetric::SpillCount), 4);
    assert_eq!(value(&exporter, ExecutionMetric::SpilledRows), 8);
    assert_eq!(value(&exporter, ExecutionMetric::SpilledBytes), 20);
    assert_eq!(value(&exporter, ExecutionMetric::ElapsedCompute), 168);
    assert!(value(&exporter, ExecutionMetric::OutputBytes) >= 30 * 8);
}

#[tokio::test]
async fn repeated_execution_reusing_native_counters_counts_each_increment_once() {
    let exporter = MetricsExporter::new();
    let plan = instrument(Arc::new(MetricsExec::new(1, Behavior::Complete)), &exporter);
    let context = Arc::new(TaskContext::default());
    for expected in [10, 20, 30] {
        assert_eq!(consume(plan.execute(0, context.clone()).unwrap()).await, 10);
        assert_eq!(value(&exporter, ExecutionMetric::OutputRows), expected);
    }
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 3);
}

#[tokio::test]
async fn overlapping_duplicate_partitions_and_distinct_contexts_do_not_double_count() {
    let exporter = MetricsExporter::new();
    let plan = instrument(Arc::new(MetricsExec::new(2, Behavior::Complete)), &exporter);
    let context = Arc::new(TaskContext::default());
    let first = plan.execute(0, context.clone()).unwrap();
    let duplicate = plan.execute(0, context).unwrap();
    let second = plan.execute(1, Arc::new(TaskContext::default())).unwrap();
    let (first_rows, duplicate_rows, second_rows) =
        futures::join!(consume(first), consume(duplicate), consume(second));
    assert_eq!((first_rows, duplicate_rows, second_rows), (10, 10, 20));
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 40);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 3);
}

#[tokio::test]
async fn retained_stream_at_eof_has_one_completed_outcome() {
    let exporter = MetricsExporter::new();
    let plan = instrument(Arc::new(MetricsExec::new(1, Behavior::Complete)), &exporter);
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    assert!(stream.next().await.unwrap().is_ok());
    assert!(stream.next().await.is_none());
    assert!(stream.next().await.is_none());
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 1);
    drop(stream);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 1);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCancelled), 0);
}

#[tokio::test]
async fn execute_errors_count_as_failed_attempts() {
    let exporter = MetricsExporter::new();
    let plan = instrument(
        Arc::new(MetricsExec::new(1, Behavior::ExecuteError)),
        &exporter,
    );
    assert!(plan.execute(0, Arc::new(TaskContext::default())).is_err());
    assert_eq!(value(&exporter, ExecutionMetric::StreamsStarted), 1);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsFailed), 1);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 0);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCancelled), 0);
}

#[tokio::test]
async fn stream_error_outcome_is_sticky_across_eof_and_drop() {
    let exporter = MetricsExporter::new();
    let plan = instrument(
        Arc::new(MetricsExec::new(1, Behavior::ErrorAfterBatch)),
        &exporter,
    );
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert!(stream.next().await.unwrap().is_ok());
    assert!(stream.next().await.unwrap().is_err());
    assert_eq!(value(&exporter, ExecutionMetric::StreamsFailed), 1);
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsFailed), 1);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 0);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCancelled), 0);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 3);
}

#[tokio::test]
async fn dropping_unconsumed_or_partially_consumed_streams_records_cancellation() {
    let exporter = MetricsExporter::new();
    let plan = instrument(
        Arc::new(MetricsExec::new(1, Behavior::PendingAfterBatch)),
        &exporter,
    );
    drop(plan.execute(0, Arc::new(TaskContext::default())).unwrap());
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 0);
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    assert_eq!(stream.next().await.unwrap().unwrap().num_rows(), 3);
    drop(stream);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsStarted), 2);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCancelled), 2);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 0);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 3);
}

#[tokio::test]
async fn native_drop_finalization_is_observed_after_stream_drop() {
    let sink = Arc::new(RecordingSink::default());
    let exporter = MetricsExporter::with_sink(sink.clone());
    let mut inner = MetricsExec::new(1, Behavior::Complete);
    inner.drop_spilled_bytes = 17;
    let plan = instrument(Arc::new(inner), &exporter);
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    stream.next().await.unwrap().unwrap();
    assert_eq!(value(&exporter, ExecutionMetric::SpilledBytes), 5);
    // Release all plan ownership before the partially consumed stream. Verify
    // the sink before collecting again: final sampling must see inner Drop.
    // At EOF, DF55's RecordBatchStreamAdapter can release the inner eagerly.
    drop(plan);
    drop(stream);
    let reported: u64 = sink
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|(metric, _, _)| *metric == ExecutionMetric::SpilledBytes)
        .map(|(_, _, delta)| delta)
        .sum();
    assert_eq!(reported, 22);
    assert_eq!(value(&exporter, ExecutionMetric::SpilledBytes), 22);
}

#[tokio::test]
async fn snapshots_refresh_live_values_and_remain_idempotent() {
    let exporter = MetricsExporter::new();
    let plan = instrument(Arc::new(MetricsExec::new(1, Behavior::Complete)), &exporter);
    let mut stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    stream.next().await.unwrap().unwrap();
    for _ in 0..3 {
        assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 3);
    }
    stream.next().await.unwrap().unwrap();
    for _ in 0..3 {
        assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 10);
    }
    assert!(stream.next().await.is_none());
    drop(stream);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 10);
}

#[tokio::test]
async fn retained_plan_allows_collecting_native_updates_after_stream_cancellation() {
    let exporter = MetricsExporter::new();
    let inner = Arc::new(MetricsExec::new(1, Behavior::PendingAfterBatch));
    let plan = instrument(inner.clone(), &exporter);
    let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    drop(stream);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCancelled), 1);
    // Native work offloaded to another task can finish after the consumer drops
    // its stream. An explicit collect must still observe the retained source.
    inner.counters[0].rows.add(9);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 9);
    drop(plan);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 9);
}

#[tokio::test]
async fn shared_registrations_survive_wrapper_retirement_without_recounting() {
    let exporter = MetricsExporter::new();
    let inner = Arc::new(MetricsExec::new(1, Behavior::Complete));
    let first = instrument(inner.clone(), &exporter);
    assert_eq!(
        consume(first.execute(0, Arc::new(TaskContext::default())).unwrap()).await,
        10
    );
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 10);
    drop(first);
    // The raw plan still owns exactly the same native registrations.
    inner.counters[0].rows.add(7);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 17);
    let second = instrument(inner, &exporter);
    assert_eq!(
        consume(second.execute(0, Arc::new(TaskContext::default())).unwrap()).await,
        10
    );
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 27);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 2);
}

#[tokio::test]
async fn reset_state_keeps_export_configuration_and_counts_fresh_registrations() {
    let exporter = MetricsExporter::new();
    let first = instrument(Arc::new(MetricsExec::new(1, Behavior::Complete)), &exporter);
    assert_eq!(
        consume(first.execute(0, Arc::new(TaskContext::default())).unwrap()).await,
        10
    );
    let reset = first.clone().reset_state().unwrap();
    assert_eq!(
        consume(reset.execute(0, Arc::new(TaskContext::default())).unwrap()).await,
        10
    );
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 20);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 2);
}

#[tokio::test]
async fn late_registrations_with_identical_names_are_counted_independently() {
    let exporter = MetricsExporter::new();
    let inner = Arc::new(MetricsExec::new(1, Behavior::Complete));
    let plan = instrument(inner.clone(), &exporter);
    let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    let first = Count::new();
    let second = Count::new();
    first.add(11);
    second.add(13);
    for counter in [first, second] {
        inner.metrics.register(Arc::new(Metric::new(
            MetricValue::OutputRows(counter),
            Some(0),
        )));
    }
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 24);
    drop(stream);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 24);
}

#[tokio::test]
async fn native_labels_and_unselected_custom_values_do_not_become_series() {
    let exporter = MetricsExporter::new();
    let inner = Arc::new(MetricsExec::new(1, Behavior::Complete));
    MetricBuilder::new(&inner.metrics)
        .with_new_label("filename", "user-specific-path")
        .output_rows(0)
        .add(11);
    // A custom Count with the same printed name is not the native OutputRows variant.
    MetricBuilder::new(&inner.metrics)
        .global_counter("output_rows")
        .add(999);
    MetricBuilder::new(&inner.metrics).mem_used(0).set(777);
    let plan = instrument(inner, &exporter);
    assert_eq!(
        consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await,
        10
    );
    let snapshot = exporter.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].operator, OPERATOR);
    assert_eq!(snapshot[0].value(ExecutionMetric::OutputRows), 21);
}

#[tokio::test]
async fn completed_totals_survive_without_retaining_plans_or_streams() {
    let exporter = MetricsExporter::new();
    for _ in 0..20 {
        let inner = Arc::new(MetricsExec::new(1, Behavior::Complete));
        let weak = Arc::downgrade(&inner);
        let plan = instrument(inner.clone(), &exporter);
        let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
        drop(inner);
        drop(plan);
        assert!(
            weak.upgrade().is_some(),
            "live stream must retain its instrumentation"
        );
        assert_eq!(consume(stream).await, 10);
        assert!(
            weak.upgrade().is_none(),
            "exporter must not retain completed plans"
        );
    }
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 200);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 20);
    assert_eq!(exporter.snapshot().len(), 1);
}

#[tokio::test]
async fn concurrent_collectors_and_native_updates_preserve_exact_totals() {
    let exporter = MetricsExporter::new();
    let inner = Arc::new(MetricsExec::new(1, Behavior::PendingAfterBatch));
    let plan = instrument(inner.clone(), &exporter);
    let stream = plan.execute(0, Arc::new(TaskContext::default())).unwrap();
    std::thread::scope(|scope| {
        for _ in 0..4 {
            let exporter = exporter.clone();
            let counter = inner.counters[0].rows.clone();
            scope.spawn(move || {
                for _ in 0..500 {
                    counter.add(1);
                    exporter.collect();
                }
            });
        }
    });
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 2_000);
    drop(stream);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 2_000);
}

#[derive(Default)]
struct RecordingSink {
    calls: Mutex<Vec<(ExecutionMetric, String, u64)>>,
    callback: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
}

impl MetricsSink for RecordingSink {
    fn add_counter(&self, metric: ExecutionMetric, operator: &str, delta: u64) {
        assert!(delta > 0, "sinks must never receive zero increments");
        self.calls
            .lock()
            .unwrap()
            .push((metric, operator.to_owned(), delta));
        let callback = self.callback.lock().unwrap().clone();
        if let Some(callback) = callback {
            callback();
        }
    }
}

#[tokio::test]
async fn custom_sinks_receive_positive_deltas_and_collection_is_idempotent() {
    let sink = Arc::new(RecordingSink::default());
    let exporter = MetricsExporter::with_sink(sink.clone());
    let plan = instrument(Arc::new(MetricsExec::new(2, Behavior::Complete)), &exporter);
    for partition in 0..2 {
        consume(
            plan.execute(partition, Arc::new(TaskContext::default()))
                .unwrap(),
        )
        .await;
        exporter.collect();
    }
    let calls_after_collection = sink.calls.lock().unwrap().clone();
    exporter.collect();
    exporter.snapshot();
    assert_eq!(*sink.calls.lock().unwrap(), calls_after_collection);
    assert!(
        calls_after_collection
            .iter()
            .all(|(_, operator, _)| operator == OPERATOR)
    );
    let recorded_rows: u64 = calls_after_collection
        .iter()
        .filter(|(metric, _, _)| *metric == ExecutionMetric::OutputRows)
        .map(|(_, _, delta)| delta)
        .sum();
    assert_eq!(recorded_rows, 30);
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), recorded_rows);
}

#[tokio::test]
async fn sink_callbacks_can_collect_without_exporter_locks_being_held() {
    let sink = Arc::new(RecordingSink::default());
    let exporter = MetricsExporter::with_sink(sink.clone());
    let callback_exporter = exporter.clone();
    *sink.callback.lock().unwrap() = Some(Arc::new(move || {
        callback_exporter.snapshot();
    }));
    let plan = instrument(Arc::new(MetricsExec::new(1, Behavior::Complete)), &exporter);
    consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 10);
    // Break the callback's intentional strong reference back to the exporter.
    sink.callback.lock().unwrap().take();
}

#[tokio::test]
async fn rejected_snapshots_do_not_recursively_notify_a_reentrant_sink() {
    let sink = Arc::new(RecordingSink::default());
    let exporter = MetricsExporter::with_sink(sink.clone());
    let callback_exporter = exporter.clone();
    *sink.callback.lock().unwrap() = Some(Arc::new(move || {
        callback_exporter.snapshot();
    }));
    let mut inner = MetricsExec::new(1, Behavior::Complete);
    inner.rebuild_metrics = true;
    let plan = instrument(Arc::new(inner), &exporter);
    consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 0);
    assert!(value(&exporter, ExecutionMetric::NativeSnapshotsRejected) > 0);
    sink.callback.lock().unwrap().take();
}

#[tokio::test]
async fn reentrant_sink_snapshots_do_not_repeat_native_discovery() {
    let sink = Arc::new(RecordingSink::default());
    let exporter = MetricsExporter::with_sink(sink.clone());
    let inner = Arc::new(MetricsExec::new(1, Behavior::Complete));
    let plan = instrument(inner.clone(), &exporter);
    consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    let before = inner.metrics_reads.load(Ordering::Relaxed);
    let requests = Arc::new(AtomicUsize::new(0));
    let callback_requests = requests.clone();
    let callback_exporter = exporter.clone();
    *sink.callback.lock().unwrap() = Some(Arc::new(move || {
        callback_requests.fetch_add(1, Ordering::Relaxed);
        callback_exporter.snapshot();
    }));
    exporter.collect();
    assert!(requests.load(Ordering::Relaxed) >= 7);
    assert_eq!(inner.metrics_reads.load(Ordering::Relaxed), before + 1);
    sink.callback.lock().unwrap().take();
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 10);
}

#[tokio::test]
async fn dynamically_named_operators_have_bounded_series_cardinality() {
    let exporter = MetricsExporter::new();
    let operators = MetricsExporter::MAX_OPERATORS + 17;
    for i in 0..operators {
        let mut inner = MetricsExec::new(1, Behavior::Complete);
        inner.operator = format!("CustomOperator{i}");
        let plan = instrument(Arc::new(inner), &exporter);
        consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    }
    let snapshot = exporter.snapshot();
    assert_eq!(snapshot.len(), MetricsExporter::MAX_OPERATORS + 1);
    assert_eq!(
        snapshot
            .iter()
            .map(|entry| entry.value(ExecutionMetric::OutputRows))
            .sum::<u64>(),
        operators as u64 * 10,
    );
    let overflow = snapshot
        .iter()
        .find(|entry| entry.operator == "__other__")
        .unwrap();
    assert_eq!(overflow.value(ExecutionMetric::OutputRows), 170);
    assert_eq!(overflow.value(ExecutionMetric::StreamsCompleted), 17);
}

#[tokio::test]
async fn operator_names_are_truncated_at_a_valid_utf8_boundary_within_the_byte_limit() {
    let exporter = MetricsExporter::new();
    for suffix in ["first", "second"] {
        let mut inner = MetricsExec::new(1, Behavior::Complete);
        // The three-byte character straddles the 128-byte label limit.
        inner.operator = format!("{}€{suffix}", "x".repeat(127));
        let plan = instrument(Arc::new(inner), &exporter);
        consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    }
    let snapshot = exporter.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].operator, "x".repeat(127));
    assert_eq!(snapshot[0].value(ExecutionMetric::OutputRows), 20);
}

#[tokio::test]
async fn native_discovery_does_not_run_per_batch_or_sequential_execution() {
    let sink = Arc::new(RecordingSink::default());
    let exporter = MetricsExporter::with_sink(sink.clone());
    let inner = Arc::new(MetricsExec::new(1, Behavior::Complete));
    let reads = inner.metrics_reads.clone();
    let plan = instrument(inner.clone(), &exporter);
    let context = Arc::new(TaskContext::default());
    for _ in 0..100 {
        assert_eq!(consume(plan.execute(0, context.clone()).unwrap()).await, 10);
    }
    assert_eq!(
        reads.load(Ordering::Relaxed),
        0,
        "native snapshots must not be discovered on the batch/execute path"
    );
    exporter.collect();
    let reads_after_collect = reads.load(Ordering::Relaxed);
    assert!(reads_after_collect > 0);
    let recorded_rows = || {
        sink.calls
            .lock()
            .unwrap()
            .iter()
            .filter(|(metric, _, _)| *metric == ExecutionMetric::OutputRows)
            .map(|(_, _, delta)| delta)
            .sum::<u64>()
    };
    assert_eq!(recorded_rows(), 1_000);
    assert_eq!(consume(plan.execute(0, context).unwrap()).await, 10);
    assert_eq!(reads.load(Ordering::Relaxed), reads_after_collect);

    drop(inner);
    drop(plan);
    assert!(reads.load(Ordering::Relaxed) > reads_after_collect);
    assert_eq!(
        recorded_rows(),
        1_010,
        "final observer destruction must collect remaining work"
    );
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 1_010);
}

#[tokio::test]
async fn normalized_operator_labels_are_shared_by_snapshots_and_custom_sinks() {
    let sink = Arc::new(RecordingSink::default());
    let exporter = MetricsExporter::with_sink(sink.clone());
    for operator in ["Custom/Op\"\\\n😀", "Custom_Op____"] {
        let mut inner = MetricsExec::new(1, Behavior::Complete);
        inner.operator = operator.to_owned();
        let plan = instrument(Arc::new(inner), &exporter);
        consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    }
    let snapshot = exporter.snapshot();
    assert_eq!(snapshot.len(), 1);
    assert_eq!(snapshot[0].operator, "Custom_Op____");
    assert_eq!(snapshot[0].value(ExecutionMetric::OutputRows), 20);
    assert!(
        sink.calls
            .lock()
            .unwrap()
            .iter()
            .all(|(_, operator, _)| operator == "Custom_Op____")
    );
}

#[tokio::test]
async fn unstable_native_registrations_are_rejected_without_silent_recounting() {
    let exporter = MetricsExporter::new();
    let mut inner = MetricsExec::new(1, Behavior::Complete);
    inner.rebuild_metrics = true;
    let plan = instrument(Arc::new(inner), &exporter);
    assert_eq!(
        consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await,
        10
    );

    let mut previous_rejections = 0;
    for _ in 0..3 {
        exporter.collect();
        let snapshot = exporter.snapshot();
        assert_eq!(snapshot.len(), 1);
        let operator = &snapshot[0];
        assert_eq!(operator.value(ExecutionMetric::OutputRows), 0);
        assert_eq!(operator.value(ExecutionMetric::OutputBatches), 0);
        assert_eq!(operator.value(ExecutionMetric::ElapsedCompute), 0);
        let rejections = operator.value(ExecutionMetric::NativeSnapshotsRejected);
        assert!(rejections > previous_rejections);
        previous_rejections = rejections;
        // Wrapper-level outcomes do not depend on native registration identity.
        assert_eq!(operator.value(ExecutionMetric::StreamsStarted), 1);
        assert_eq!(operator.value(ExecutionMetric::StreamsCompleted), 1);
        assert_eq!(operator.value(ExecutionMetric::StreamsFailed), 0);
        assert_eq!(operator.value(ExecutionMetric::StreamsCancelled), 0);
    }
    drop(plan);
    let snapshot = exporter.snapshot();
    assert_eq!(snapshot[0].value(ExecutionMetric::OutputRows), 0);
    assert!(
        snapshot[0].value(ExecutionMetric::NativeSnapshotsRejected) > previous_rejections
    );
}

#[tokio::test]
async fn computed_ephemeral_ratios_do_not_reject_stable_native_counters() {
    let exporter = MetricsExporter::new();
    let mut inner = MetricsExec::new(1, Behavior::Complete);
    // Built-in plans such as RepartitionExec append computed ratio values to
    // otherwise stable native snapshots. These are outside the exported schema.
    inner.ephemeral_ratio = true;
    let plan = instrument(Arc::new(inner), &exporter);
    consume(plan.execute(0, Arc::new(TaskContext::default())).unwrap()).await;
    for _ in 0..3 {
        let snapshot = exporter.snapshot();
        assert_eq!(snapshot[0].value(ExecutionMetric::OutputRows), 10);
        assert_eq!(
            snapshot[0].value(ExecutionMetric::NativeSnapshotsRejected),
            0
        );
    }
}

#[tokio::test]
async fn recursive_ctes_preserve_runtime_state_and_accumulate_exact_root_totals() {
    // Issue #5: wrapping WorkTableExec used to hide it from recursive state
    // assignment. Each recursive iteration also resets and rewraps plan nodes.
    let exporter = MetricsExporter::new();
    let options = InstrumentationOptions::builder()
        .metrics_exporter(exporter.clone())
        .build();
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_default_features()
        .with_physical_optimizer_rule(instrument_with_info_spans!(options: options))
        .build();
    let ctx = SessionContext::new_with_state(state);
    let sql = "WITH RECURSIVE numbers(n) AS (
        SELECT CAST(1 AS BIGINT)
        UNION ALL
        SELECT n + 1 FROM numbers WHERE n < 10
    ) SELECT n FROM numbers";

    for execution in 1..=2 {
        // Replan: DataFusion does not support reexecuting an entire recursive
        // physical plan with reset_plan_states(). Internal iteration resets are
        // part of normal recursive execution and must retain instrumentation.
        let plan = ctx
            .sql(sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        let mut recursive_nodes = 0;
        let mut work_tables = 0;
        plan.apply(|node| {
            recursive_nodes += usize::from(node.is::<RecursiveQueryExec>());
            work_tables += usize::from(node.is::<WorkTableExec>());
            Ok(TreeNodeRecursion::Continue)
        })
        .unwrap();
        assert_eq!(recursive_nodes, 1);
        assert_eq!(work_tables, 1);
        let batches = collect(plan.clone(), ctx.task_ctx()).await.unwrap();
        let mut values = batches
            .iter()
            .flat_map(|batch| {
                batch
                    .column(0)
                    .as_any()
                    .downcast_ref::<Int64Array>()
                    .unwrap()
                    .values()
                    .iter()
                    .copied()
            })
            .collect::<Vec<_>>();
        values.sort_unstable();
        assert_eq!(values, (1..=10).collect::<Vec<_>>());

        let first_snapshot = exporter.snapshot();
        let recursive = first_snapshot
            .iter()
            .find(|operator| operator.operator == "RecursiveQueryExec")
            .unwrap();
        assert_eq!(recursive.value(ExecutionMetric::OutputRows), execution * 10);
        assert_eq!(
            recursive.value(ExecutionMetric::StreamsCompleted),
            execution
        );
        assert!(first_snapshot.iter().all(|operator| {
            operator.value(ExecutionMetric::NativeSnapshotsRejected) == 0
        }));
        assert!(first_snapshot.len() <= MetricsExporter::MAX_OPERATORS + 1);
        // Scraping again must not count iteration snapshots or the retained root twice.
        let second_snapshot = exporter.snapshot();
        assert_eq!(
            second_snapshot
                .iter()
                .find(|operator| operator.operator == "RecursiveQueryExec")
                .unwrap()
                .value(ExecutionMetric::OutputRows),
            execution * 10
        );
    }
}

#[tokio::test]
async fn public_downcasts_and_child_replacements_preserve_metrics_export() {
    // Issues #10/#22: wrappers must preserve public introspection without exposing
    // their own type, and rewritten nodes must keep the export configuration.
    let exporter = MetricsExporter::new();
    let original =
        instrument(Arc::new(MetricsExec::new(1, Behavior::Complete)), &exporter);
    assert!(original.downcast_ref::<MetricsExec>().is_some());
    consume(
        original
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap(),
    )
    .await;
    for mode in [
        ChildrenPropertiesMode::Keep,
        ChildrenPropertiesMode::Recompute,
    ] {
        let replaced = original
            .clone()
            .replace_children(vec![], ReplaceChildrenOptions::new(mode))
            .unwrap();
        assert!(replaced.downcast_ref::<MetricsExec>().is_some());
        assert!(replaced.children().is_empty());
        consume(
            replaced
                .execute(0, Arc::new(TaskContext::default()))
                .unwrap(),
        )
        .await;
    }
    #[expect(
        deprecated,
        reason = "verify the legacy forwarding path remains transparent"
    )]
    let replaced = original.with_new_children(vec![]).unwrap();
    assert!(replaced.downcast_ref::<MetricsExec>().is_some());
    consume(
        replaced
            .execute(0, Arc::new(TaskContext::default()))
            .unwrap(),
    )
    .await;
    assert_eq!(value(&exporter, ExecutionMetric::OutputRows), 40);
    assert_eq!(value(&exporter, ExecutionMetric::StreamsCompleted), 4);
}

#[tokio::test]
async fn nested_view_planning_keeps_native_nodes_visible_with_export_enabled() {
    // Issue #10 described ViewTable planning introducing wrappers before an outer
    // optimizer pass. Exercise that path with the normal optimizer rule chain.
    let exporter = MetricsExporter::new();
    let options = InstrumentationOptions::builder()
        .metrics_exporter(exporter.clone())
        .build();
    let state = SessionStateBuilder::new()
        .with_config(SessionConfig::new().with_target_partitions(1))
        .with_default_features()
        .with_physical_optimizer_rule(instrument_with_info_spans!(options: options))
        .build();
    let ctx = SessionContext::new_with_state(state);
    ctx.register_batch(
        "source",
        RecordBatch::try_from_iter([(
            "a",
            Arc::new(Int64Array::from(vec![1, 2, 3])) as ArrayRef,
        )])
        .unwrap(),
    )
    .unwrap();
    ctx.sql("CREATE VIEW derived AS SELECT a + 1 AS b FROM source")
        .await
        .unwrap()
        .collect()
        .await
        .unwrap();
    let plan = ctx
        .sql("SELECT b FROM derived WHERE b < 4 ORDER BY b")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    let mut projections = 0;
    plan.apply(|node| {
        projections += usize::from(node.downcast_ref::<ProjectionExec>().is_some());
        Ok(TreeNodeRecursion::Continue)
    })
    .unwrap();
    assert!(
        projections > 0,
        "native projection nodes must remain inspectable"
    );
    let batches = collect(plan, ctx.task_ctx()).await.unwrap();
    let values = batches
        .iter()
        .flat_map(|batch| {
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .values()
                .iter()
                .copied()
        })
        .collect::<Vec<_>>();
    assert_eq!(values, vec![2, 3]);
    assert!(exporter.snapshot().iter().any(|operator| {
        operator.operator == "ProjectionExec"
            && operator.value(ExecutionMetric::OutputRows) >= 2
    }));
}
