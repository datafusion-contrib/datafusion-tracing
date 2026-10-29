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

//! Backend-independent execution metrics, optional adapters, and typed snapshots.
//!
//! Attach one [`MetricsExporter`] to [`InstrumentationOptions`](crate::InstrumentationOptions)
//! and share it across sessions whose metrics should be aggregated. Call
//! [`collect`](MetricsExporter::collect) before a scrape, or periodically when using
//! a push backend. [`snapshot`](MetricsExporter::snapshot) requests collection automatically.
//! Completed totals survive plan destruction; exporters do not retain physical plans.
//!
//! See the [metrics export guide](https://github.com/datafusion-contrib/datafusion-tracing/blob/main/datafusion-tracing/docs/metrics-export.md)
//! for setup, metric meanings, lifecycle guarantees, and custom plan requirements.

use datafusion::physical_plan::{
    ExecutionPlan,
    metrics::{Metric, MetricValue},
};
use std::{
    collections::HashMap,
    fmt,
    sync::{
        Arc, Mutex, Weak,
        atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering},
    },
};

#[cfg(any(feature = "metrics", feature = "opentelemetry", feature = "prometheus"))]
mod backends;
#[cfg(feature = "opentelemetry")]
pub use backends::OpenTelemetryMetricsSink;
#[cfg(feature = "prometheus")]
pub use backends::PrometheusMetricsCollector;
#[cfg(feature = "metrics")]
pub use backends::RecorderMetricsSink;

/// The unit of a counter, independent of an export protocol.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[non_exhaustive]
pub enum MetricUnit {
    /// A number of rows, batches, spills, or streams.
    Count,
    /// Bytes.
    Bytes,
    /// Nanoseconds. Integer units preserve subsecond compute time in all backends.
    Nanoseconds,
}

/// The bounded set of cumulative execution counters exported by this crate.
///
/// Native values are summed across partitions and native labels, then across
/// nodes with the same operator name. Stream counters count partition executions,
/// not queries. Additional counters may be added in future releases.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ExecutionMetric {
    /// Rows emitted by operators, including intermediate rows.
    OutputRows,
    /// Record batches emitted by operators.
    OutputBatches,
    /// Native operator output bytes.
    OutputBytes,
    /// Native operator spill events.
    SpillCount,
    /// Native rows spilled to disk.
    SpilledRows,
    /// Native bytes spilled to disk.
    SpilledBytes,
    /// Native elapsed compute time, in nanoseconds (not query latency or CPU time).
    ElapsedCompute,
    /// Calls to an instrumented operator's `execute` method.
    StreamsStarted,
    /// Streams that yielded end-of-stream before any error.
    StreamsCompleted,
    /// Execute calls or streams that returned an error.
    StreamsFailed,
    /// Streams dropped before yielding either an error or end-of-stream.
    StreamsCancelled,
    /// Native snapshots rejected because selected registrations have no stable owner.
    NativeSnapshotsRejected,
}

impl ExecutionMetric {
    /// All supported counters, in a stable order.
    pub const ALL: &'static [Self] = &[
        Self::OutputRows,
        Self::OutputBatches,
        Self::OutputBytes,
        Self::SpillCount,
        Self::SpilledRows,
        Self::SpilledBytes,
        Self::ElapsedCompute,
        Self::StreamsStarted,
        Self::StreamsCompleted,
        Self::StreamsFailed,
        Self::StreamsCancelled,
        Self::NativeSnapshotsRejected,
    ];

    /// The complete metric name, including its counter suffix and unit.
    pub fn name(self) -> &'static str {
        match self {
            Self::OutputRows => "datafusion_operator_output_rows_total",
            Self::OutputBatches => "datafusion_operator_output_batches_total",
            Self::OutputBytes => "datafusion_operator_output_bytes_total",
            Self::SpillCount => "datafusion_operator_spills_total",
            Self::SpilledRows => "datafusion_operator_spilled_rows_total",
            Self::SpilledBytes => "datafusion_operator_spilled_bytes_total",
            Self::ElapsedCompute => {
                "datafusion_operator_elapsed_compute_nanoseconds_total"
            }
            Self::StreamsStarted => "datafusion_operator_streams_started_total",
            Self::StreamsCompleted => "datafusion_operator_streams_completed_total",
            Self::StreamsFailed => "datafusion_operator_streams_failed_total",
            Self::StreamsCancelled => "datafusion_operator_streams_cancelled_total",
            Self::NativeSnapshotsRejected => {
                "datafusion_operator_native_snapshots_rejected_total"
            }
        }
    }

    /// A description suitable for a backend's metric metadata.
    pub fn description(self) -> &'static str {
        match self {
            Self::OutputRows => "Rows emitted by operators, including intermediate rows",
            Self::OutputBatches => "Record batches emitted by operators",
            Self::OutputBytes => "Native operator output bytes",
            Self::SpillCount => "Native operator spill events",
            Self::SpilledRows => "Native rows spilled to disk",
            Self::SpilledBytes => "Native bytes spilled to disk",
            Self::ElapsedCompute => {
                "Native operator elapsed compute time in nanoseconds, including descheduling"
            }
            Self::StreamsStarted => "Operator partition execution attempts",
            Self::StreamsCompleted => {
                "Operator streams that reached end-of-stream before an error"
            }
            Self::StreamsFailed => {
                "Operator execution attempts or streams that returned an error"
            }
            Self::StreamsCancelled => {
                "Operator streams dropped before an error or end-of-stream"
            }
            Self::NativeSnapshotsRejected => {
                "Native snapshots rejected because counter registrations have no stable owner"
            }
        }
    }

    /// The unit of this counter.
    pub fn unit(self) -> MetricUnit {
        match self {
            Self::OutputBytes | Self::SpilledBytes => MetricUnit::Bytes,
            Self::ElapsedCompute => MetricUnit::Nanoseconds,
            _ => MetricUnit::Count,
        }
    }

    fn index(self) -> usize {
        self as usize
    }
}

/// Receives positive counter increments for an application-selected backend.
///
/// Calls can occur concurrently, during collection or stream/plan destruction.
/// Implementations must be thread-safe, fast, nonblocking, and must not panic.
/// Buffer network export in the application or backend; do not perform it here.
/// No exporter locks are held while calling a sink. A sink may receive increments
/// out of order, so it must add deltas rather than treat them as absolute values.
///
/// Use a fresh sink namespace for each exporter: sharing the same backend labels
/// between exporters intentionally sums their independent observations.
pub trait MetricsSink: Send + Sync {
    /// Add `delta` to the named operator counter. `delta` is always positive.
    fn add_counter(&self, metric: ExecutionMetric, operator: &str, delta: u64);
}

/// An owned, typed snapshot of cumulative counters for one operator label.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct OperatorMetricsSnapshot {
    /// Canonical operator type: ASCII letters, digits, and underscores.
    /// Long names are truncated; excess distinct names use `__other__`.
    pub operator: String,
    counters: [u64; ExecutionMetric::ALL.len()],
}

impl OperatorMetricsSnapshot {
    /// The cumulative value of a counter, including completed plans.
    pub fn value(&self, metric: ExecutionMetric) -> u64 {
        self.counters[metric.index()]
    }

    /// All counters and their values, in [`ExecutionMetric::ALL`] order.
    pub fn counters(&self) -> impl Iterator<Item = (ExecutionMetric, u64)> + '_ {
        ExecutionMetric::ALL
            .iter()
            .copied()
            .map(|metric| (metric, self.value(metric)))
    }

    /// Execution attempts without a recorded terminal outcome.
    ///
    /// Concurrent snapshots are eventually consistent. At quiescence this equals
    /// the number of streams still alive without EOF or an error.
    pub fn active_streams(&self) -> u64 {
        self.value(ExecutionMetric::StreamsStarted).saturating_sub(
            self.value(ExecutionMetric::StreamsCompleted)
                .saturating_add(self.value(ExecutionMetric::StreamsFailed))
                .saturating_add(self.value(ExecutionMetric::StreamsCancelled)),
        )
    }
}

/// Collects native execution counters without depending on tracing or a backend.
///
/// Clones share collection state. Keep an exporter for the lifetime of the
/// application or workload whose cumulative totals it represents. Native metrics
/// are refreshed by [`collect`](Self::collect), [`snapshot`](Self::snapshot), and
/// when the last instrumented plan/stream owner is dropped. Stream outcomes are
/// recorded immediately. Neither collection nor export walks metrics per batch.
///
/// Custom plans must return stable `Arc<Metric>` registrations from `metrics()`,
/// as `ExecutionPlanMetricsSet::clone_inner()` does. Reconstructing registrations
/// around existing counters makes source identity unknowable and is unsupported.
/// A registration exposed by multiple wrappers is counted once and attributed to
/// its first observed operator label. Values already present when a registration
/// is first discovered are included.
/// Snapshots containing selected registrations without another `Arc<Metric>`
/// owner are rejected and increment [`ExecutionMetric::NativeSnapshotsRejected`].
/// This detects common contract violations, including DataFusion 55 FFI snapshots.
/// It does not prove that a custom plan preserves registration identity.
///
/// Registrations must remain owned by the plan while their counters can change.
/// Updates from detached tasks after the last native registration owner disappears
/// are outside the observable lifetime; retain the instrumented plan until such
/// work finishes when those updates must be included.
#[derive(Clone)]
pub struct MetricsExporter {
    inner: Arc<ExporterState>,
}

impl fmt::Debug for MetricsExporter {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MetricsExporter").finish_non_exhaustive()
    }
}

impl Default for MetricsExporter {
    fn default() -> Self {
        Self::new()
    }
}

impl MetricsExporter {
    /// The maximum number of distinct operator names, plus one overflow label.
    pub const MAX_OPERATORS: usize = 128;

    /// Create a collector for typed, backend-independent snapshots.
    pub fn new() -> Self {
        Self::create(None)
    }

    /// Create a collector that also sends increments to an application-owned sink.
    pub fn with_sink(sink: Arc<dyn MetricsSink>) -> Self {
        Self::create(Some(sink))
    }

    fn create(sink: Option<Arc<dyn MetricsSink>>) -> Self {
        Self {
            inner: Arc::new(ExporterState {
                sink,
                operators: Mutex::new(HashMap::new()),
                plans: Mutex::new(HashMap::new()),
                sources: Mutex::new(HashMap::new()),
                next_plan: AtomicUsize::new(0),
                collection_active: AtomicBool::new(false),
            }),
        }
    }

    /// Discover and sample native counters of currently tracked plans.
    ///
    /// Also samples registrations whose raw plans outlive their instrumentation,
    /// preserving checkpoints across rewrapping. Retired registrations are sampled
    /// one last time and released. Call before exposing a sink's metrics, or from
    /// an application-managed reporting task. No background task is installed.
    ///
    /// Concurrent calls are supported; each source's delta is accounted once.
    /// Concurrent and reentrant requests coalesce with an active collection and
    /// may observe its last published values, without waiting for it to finish.
    /// Values are eventually consistent, not an atomic snapshot of all operators.
    pub fn collect(&self) {
        // A sink may request another snapshot while receiving a delta. Coalesce
        // that request instead of recursively walking all remaining sources.
        if self
            .inner
            .collection_active
            .compare_exchange(false, true, Ordering::Acquire, Ordering::Relaxed)
            .is_err()
        {
            return;
        }
        let _guard = CollectionGuard(&self.inner.collection_active);
        let plans: Vec<_> = self
            .inner
            .plans
            .lock()
            .unwrap()
            .values()
            .filter_map(Weak::upgrade)
            .collect();
        for plan in plans {
            plan.discover();
        }
        let sources: Vec<_> = self
            .inner
            .sources
            .lock()
            .unwrap()
            .values()
            .cloned()
            .collect();
        for source in &sources {
            source.sample();
        }
        self.retire_sources(&sources);
    }

    /// Request collection and return typed cumulative values, sorted by operator label.
    pub fn snapshot(&self) -> Vec<OperatorMetricsSnapshot> {
        self.collect();
        let mut snapshots: Vec<_> = self
            .inner
            .operators
            .lock()
            .unwrap()
            .values()
            .map(|operator| OperatorMetricsSnapshot {
                operator: operator.name.clone(),
                counters: std::array::from_fn(|i| {
                    operator.counters[i].load(Ordering::Relaxed)
                }),
            })
            .collect();
        snapshots.sort_unstable_by(|a, b| a.operator.cmp(&b.operator));
        snapshots
    }

    pub(crate) fn track(&self, inner: Arc<dyn ExecutionPlan>) -> Arc<PlanMetrics> {
        let operator = self.operator(inner.name());
        let id = self.inner.next_plan.fetch_add(1, Ordering::Relaxed);
        let plan = Arc::new(PlanMetrics {
            inner: Some(inner),
            exporter: self.clone(),
            operator,
            id,
        });
        self.inner
            .plans
            .lock()
            .unwrap()
            .insert(id, Arc::downgrade(&plan));
        plan
    }

    fn operator(&self, name: &str) -> Arc<OperatorState> {
        // Bound label count and size, even for dynamically named custom plans.
        // Canonical identifiers stay transport-independent: some backend label
        // encoders do not escape strings, and text escaping corrupts protobuf.
        let end = name.floor_char_boundary(128);
        let name: String = name[..end]
            .chars()
            .map(|c| {
                if c.is_ascii_alphanumeric() || c == '_' {
                    c
                } else {
                    '_'
                }
            })
            .collect();
        let mut operators = self.inner.operators.lock().unwrap();
        if let Some(operator) = operators.get(&name) {
            return operator.clone();
        }
        let name = if operators.len() >= Self::MAX_OPERATORS {
            "__other__"
        } else {
            &name
        };
        operators
            .entry(name.to_owned())
            .or_insert_with(|| {
                Arc::new(OperatorState {
                    name: name.to_owned(),
                    counters: std::array::from_fn(|_| AtomicU64::new(0)),
                    sink: self.inner.sink.clone(),
                })
            })
            .clone()
    }

    fn source(
        &self,
        metric: &Arc<Metric>,
        kind: ExecutionMetric,
        operator: &Arc<OperatorState>,
    ) -> Arc<MetricSource> {
        let key = Arc::as_ptr(metric) as usize;
        self.inner
            .sources
            .lock()
            .unwrap()
            .entry(key)
            .or_insert_with(|| {
                Arc::new(MetricSource {
                    metric: metric.clone(),
                    kind,
                    operator: operator.clone(),
                    previous: Mutex::new(0),
                })
            })
            .clone()
    }

    fn retire_sources(&self, candidates: &[Arc<MetricSource>]) {
        let retired = {
            let mut sources = self.inner.sources.lock().unwrap();
            let mut retired = Vec::new();
            for source in candidates {
                // The index owns the registration until nobody can return it again.
                // Keeping the Arc also prevents address reuse from aliasing a checkpoint.
                if Arc::strong_count(&source.metric) == 1
                    && let Some(source) =
                        sources.remove(&(Arc::as_ptr(&source.metric) as usize))
                {
                    retired.push(source);
                }
            }
            shrink_idle_index(&mut sources);
            retired
        };
        for source in retired {
            // A plan may have finalized its counter between the previous sample
            // and retiring the registration. Sample again, without index locks.
            source.sample();
        }
    }
}

struct ExporterState {
    sink: Option<Arc<dyn MetricsSink>>,
    operators: Mutex<HashMap<String, Arc<OperatorState>>>,
    plans: Mutex<HashMap<usize, Weak<PlanMetrics>>>,
    sources: Mutex<HashMap<usize, Arc<MetricSource>>>,
    next_plan: AtomicUsize,
    collection_active: AtomicBool,
}

/// Keep index capacity proportional to live entries after unusually large plans.
/// The slack avoids reallocating small registries or compacting on every removal.
fn shrink_idle_index<V>(index: &mut HashMap<usize, V>) {
    if index.capacity() > 1024 && index.len() < index.capacity() / 4 {
        index.shrink_to((index.len() * 2).max(256));
    }
}

pub(crate) struct OperatorState {
    name: String,
    counters: [AtomicU64; ExecutionMetric::ALL.len()],
    sink: Option<Arc<dyn MetricsSink>>,
}

impl OperatorState {
    pub(crate) fn add(&self, metric: ExecutionMetric, delta: u64) {
        if delta == 0 {
            return;
        }
        let previous = self.counters[metric.index()]
            .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |value| {
                Some(value.saturating_add(delta))
            })
            .unwrap();
        let delta = delta.min(u64::MAX - previous);
        if delta > 0
            && let Some(sink) = &self.sink
        {
            sink.add_counter(metric, &self.name, delta);
        }
    }
}

/// Lives with a wrapper and its streams, but is held weakly by the exporter.
pub(crate) struct PlanMetrics {
    inner: Option<Arc<dyn ExecutionPlan>>,
    exporter: MetricsExporter,
    operator: Arc<OperatorState>,
    id: usize,
}

struct CollectionGuard<'a>(&'a AtomicBool);

impl Drop for CollectionGuard<'_> {
    fn drop(&mut self) {
        self.0.store(false, Ordering::Release);
    }
}

impl PlanMetrics {
    pub(crate) fn exporter(&self) -> MetricsExporter {
        self.exporter.clone()
    }

    fn discover(&self) -> Vec<Arc<MetricSource>> {
        let Some(metrics) = self.inner.as_ref().and_then(|plan| plan.metrics()) else {
            return Vec::new();
        };
        // Native snapshots clone registrations retained by the plan. Rebuilt
        // wrappers (including DF55 FFI snapshots) have only the snapshot's owner.
        // Treating these as new sources would count complete values every scrape.
        if metrics.iter().any(|metric| {
            native_counter(metric.value()).is_some() && Arc::strong_count(metric) == 1
        }) {
            self.add(ExecutionMetric::NativeSnapshotsRejected);
            return Vec::new();
        }
        metrics
            .iter()
            .filter_map(|metric| {
                native_counter(metric.value())
                    .map(|(kind, _)| self.exporter.source(metric, kind, &self.operator))
            })
            .collect()
    }

    pub(crate) fn add(&self, metric: ExecutionMetric) {
        self.operator.add(metric, 1);
    }
}

impl Drop for PlanMetrics {
    fn drop(&mut self) {
        let sources = self.discover();
        // Plan destructors may finalize native metrics. Sources retain the
        // registrations, so sampling remains possible after releasing the plan.
        drop(self.inner.take());
        for source in &sources {
            source.sample();
        }
        self.exporter.retire_sources(&sources);
        let mut plans = self.exporter.inner.plans.lock().unwrap();
        plans.remove(&self.id);
        shrink_idle_index(&mut plans);
    }
}

struct MetricSource {
    metric: Arc<Metric>,
    kind: ExecutionMetric,
    operator: Arc<OperatorState>,
    previous: Mutex<usize>,
}

impl MetricSource {
    fn sample(&self) {
        let delta = {
            // Read under the checkpoint lock: reading first permits concurrent
            // collectors to account older values after newer ones.
            let mut previous = self.previous.lock().unwrap();
            let (_, current) = native_counter(self.metric.value()).unwrap();
            let delta = current.wrapping_sub(*previous) as u64;
            *previous = current;
            delta
        };
        self.operator.add(self.kind, delta);
    }
}

fn native_counter(value: &MetricValue) -> Option<(ExecutionMetric, usize)> {
    let (kind, value) = match value {
        MetricValue::OutputRows(value) => (ExecutionMetric::OutputRows, value.value()),
        MetricValue::OutputBatches(value) => {
            (ExecutionMetric::OutputBatches, value.value())
        }
        MetricValue::OutputBytes(value) => (ExecutionMetric::OutputBytes, value.value()),
        MetricValue::SpillCount(value) => (ExecutionMetric::SpillCount, value.value()),
        MetricValue::SpilledRows(value) => (ExecutionMetric::SpilledRows, value.value()),
        MetricValue::SpilledBytes(value) => {
            (ExecutionMetric::SpilledBytes, value.value())
        }
        MetricValue::ElapsedCompute(value) => {
            (ExecutionMetric::ElapsedCompute, value.value())
        }
        _ => return None,
    };
    Some((kind, value))
}

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::physical_plan::metrics::Count;

    fn output_rows(count: &Count) -> Arc<Metric> {
        Arc::new(Metric::new(MetricValue::OutputRows(count.clone()), None))
    }

    fn rows(operator: &OperatorState) -> u64 {
        operator.counters[ExecutionMetric::OutputRows.index()].load(Ordering::Relaxed)
    }

    #[test]
    fn counter_schema_has_unique_portable_names_and_nonempty_descriptions() {
        let mut names = std::collections::HashSet::new();
        for &metric in ExecutionMetric::ALL {
            let name = metric.name();
            assert!(names.insert(name), "duplicate metric family: {name}");
            assert!(name.starts_with("datafusion_operator_"));
            assert!(name.ends_with("_total"));
            assert!(name.bytes().all(|c| c.is_ascii_alphanumeric() || c == b'_'));
            assert!(!metric.description().is_empty());
        }
    }

    #[test]
    fn idle_indexes_release_peak_capacity_after_large_plans_retire() {
        use datafusion::arrow::datatypes::Schema;
        use datafusion::physical_plan::empty::EmptyExec;

        let exporter = MetricsExporter::new();
        let operator = exporter.operator("LargePlanExec");
        let registrations: Vec<_> = (0..4096)
            .map(|_| {
                let count = Count::new();
                count.add(1);
                let metric = output_rows(&count);
                exporter.source(&metric, ExecutionMetric::OutputRows, &operator);
                metric
            })
            .collect();
        let inner: Arc<dyn ExecutionPlan> =
            Arc::new(EmptyExec::new(Arc::new(Schema::empty())));
        let plans: Vec<_> = (0..4096).map(|_| exporter.track(inner.clone())).collect();
        assert!(exporter.inner.sources.lock().unwrap().capacity() > 1024);
        assert!(exporter.inner.plans.lock().unwrap().capacity() > 1024);
        drop(registrations);
        drop(plans);
        exporter.collect();
        let sources = exporter.inner.sources.lock().unwrap();
        let plans = exporter.inner.plans.lock().unwrap();
        assert!(sources.is_empty());
        assert!(plans.is_empty());
        assert!(sources.capacity() <= 1024);
        assert!(plans.capacity() <= 1024);
        assert_eq!(rows(&operator), 4096);
    }

    #[test]
    fn collection_reclaims_retired_registrations_and_preserves_totals() {
        let exporter = MetricsExporter::new();
        let operator = exporter.operator("ChurnExec");
        let mut expected = 0;

        for _ in 0..8 {
            let mut registrations = Vec::new();
            for amount in 1..=128 {
                let count = Count::new();
                count.add(amount);
                let metric = output_rows(&count);
                registrations.push(Arc::downgrade(&metric));
                exporter
                    .source(&metric, ExecutionMetric::OutputRows, &operator)
                    .sample();
                expected += amount as u64;
            }

            // Only the source index owns these registrations now. Collection
            // must release them instead of retaining every completed execution.
            assert_eq!(exporter.inner.sources.lock().unwrap().len(), 128);
            assert!(
                registrations
                    .iter()
                    .all(|metric| metric.upgrade().is_some())
            );
            exporter.collect();
            assert!(exporter.inner.sources.lock().unwrap().is_empty());
            assert!(
                registrations
                    .iter()
                    .all(|metric| metric.upgrade().is_none())
            );
            assert_eq!(rows(&operator), expected);
        }

        assert_eq!(
            exporter.snapshot()[0].value(ExecutionMetric::OutputRows),
            expected
        );
    }

    #[test]
    fn retained_registration_keeps_its_checkpoint_across_source_rediscovery() {
        let exporter = MetricsExporter::new();
        let operator = exporter.operator("ReusedExec");
        let count = Count::new();
        count.add(7);
        let metric = output_rows(&count);
        let source = exporter.source(&metric, ExecutionMetric::OutputRows, &operator);
        let checkpoint = Arc::downgrade(&source);
        source.sample();
        drop(source);

        // A raw plan may retain a registration after its first instrumented
        // wrapper disappears. It must keep the same checkpoint when rewrapped.
        exporter.collect();
        assert_eq!(rows(&operator), 7);
        assert_eq!(exporter.inner.sources.lock().unwrap().len(), 1);
        let source = exporter.source(&metric, ExecutionMetric::OutputRows, &operator);
        assert!(Arc::ptr_eq(&source, &checkpoint.upgrade().unwrap()));
        source.sample();
        assert_eq!(rows(&operator), 7);

        count.add(11);
        source.sample();
        assert_eq!(rows(&operator), 18);
        drop(source);
        drop(metric);
        exporter.collect();
        assert!(checkpoint.upgrade().is_none());
        assert!(exporter.inner.sources.lock().unwrap().is_empty());
        assert_eq!(rows(&operator), 18);
    }

    #[test]
    fn native_counter_wrap_preserves_the_increment() {
        let exporter = MetricsExporter::new();
        let operator = exporter.operator("WrappingExec");
        let count = Count::new();
        count.add(usize::MAX - 3);
        let metric = output_rows(&count);
        let source = exporter.source(&metric, ExecutionMetric::OutputRows, &operator);

        // Start from an existing checkpoint so this test isolates the native
        // usize wrap from the independent saturation of exported u64 totals.
        *source.previous.lock().unwrap() = count.value();
        count.add(9);
        assert_eq!(count.value(), 5);
        source.sample();
        assert_eq!(rows(&operator), 9);
        source.sample();
        assert_eq!(rows(&operator), 9);
    }

    #[derive(Default)]
    struct RecordingSink(Mutex<Vec<u64>>);

    impl MetricsSink for RecordingSink {
        fn add_counter(&self, _: ExecutionMetric, _: &str, delta: u64) {
            self.0.lock().unwrap().push(delta);
        }
    }

    #[test]
    fn exported_totals_saturate_and_only_forward_representable_increments() {
        let sink = Arc::new(RecordingSink::default());
        let exporter = MetricsExporter::with_sink(sink.clone());
        let operator = exporter.operator("SaturatingExec");

        operator.add(ExecutionMetric::OutputRows, u64::MAX - 2);
        operator.add(ExecutionMetric::OutputRows, 10);
        operator.add(ExecutionMetric::OutputRows, 1);
        assert_eq!(rows(&operator), u64::MAX);
        assert_eq!(*sink.0.lock().unwrap(), vec![u64::MAX - 2, 2]);

        // Saturation belongs to a particular series, not the whole operator.
        operator.add(ExecutionMetric::StreamsStarted, 3);
        assert_eq!(
            operator.counters[ExecutionMetric::StreamsStarted.index()]
                .load(Ordering::Relaxed),
            3
        );
        assert_eq!(*sink.0.lock().unwrap(), vec![u64::MAX - 2, 2, 3]);
    }

    #[test]
    fn retirement_samples_updates_after_the_previous_collection() {
        let exporter = MetricsExporter::new();
        let operator = exporter.operator("FinalizingExec");
        let count = Count::new();
        count.add(11);
        let metric = output_rows(&count);
        let registration = Arc::downgrade(&metric);
        let source = exporter.source(&metric, ExecutionMetric::OutputRows, &operator);
        source.sample();
        assert_eq!(rows(&operator), 11);

        // Model a synchronous native finalizer between the ordinary sample and
        // the decision to release a registration with no remaining raw owners.
        count.add(5);
        drop(metric);
        exporter.retire_sources(std::slice::from_ref(&source));
        assert_eq!(rows(&operator), 16);
        assert!(exporter.inner.sources.lock().unwrap().is_empty());
        drop(source);
        assert!(registration.upgrade().is_none());
        exporter.collect();
        assert_eq!(rows(&operator), 16);
    }
}
