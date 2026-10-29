# Exporting execution metrics

`datafusion-tracing` can aggregate execution metrics without a tracing subscriber
or an OpenTelemetry tracing pipeline. Collection is independent of export: use
typed Rust snapshots, an application-provided `MetricsSink`, the Rust `metrics`
facade, OpenTelemetry counters, or the optional Prometheus collector.

These are **operator metrics**, including intermediate work and individual
partition streams. They do not measure query latency, rows returned to a client,
or object-store operations during planning.

These APIs are currently unreleased. The dependency examples below use this
checkout; adjust the path to the `datafusion-tracing` package in your workspace.

## Start with a collector

Create one `MetricsExporter` for the application or workload whose totals you
want to retain. Clone it into instrumentation options for every relevant session.
Register the instrumentation rule last, after other physical optimizer rules.

```rust
use datafusion::execution::SessionStateBuilder;
use datafusion::prelude::SessionContext;
use datafusion_tracing::metrics_export::{ExecutionMetric, MetricsExporter};
use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};

let exporter = MetricsExporter::new();
let options = InstrumentationOptions::builder()
    .metrics_exporter(exporter.clone())
    .build();
let state = SessionStateBuilder::new()
    .with_default_features()
    .with_physical_optimizer_rule(instrument_with_info_spans!(options: options))
    .build();
let ctx = SessionContext::new_with_state(state);

// Run queries using ctx. No tracing subscriber is necessary.
// snapshot() refreshes native metrics before returning owned, typed values.
for operator in exporter.snapshot() {
    println!("{}: {} rows", operator.operator,
        operator.value(ExecutionMetric::OutputRows));
}
```

`record_metrics(true)` still controls the existing span fields. It does not enable
or disable this exporter. Both mechanisms can be used together.

Existing `InstrumentationOptions` struct literals need `metrics_exporter: None`
or `..Default::default()`. Existing builder-based setup needs no changes.

See [the runnable snapshot example](../examples/metrics_snapshot.rs).

## Choose an interface

### Typed snapshots

`exporter.snapshot()` returns `Vec<OperatorMetricsSnapshot>`, sorted by operator
label. Each entry exposes `value(ExecutionMetric)`, `counters()`, and the derived
`active_streams()`. Values include previously completed and dropped plans.

Snapshots work without any optional features. Use them for application APIs,
testing, diagnostics, or an observable-instrument backend that expects absolute
cumulative values. Concurrent snapshots are eventually consistent; the collection
does not stop execution to take a globally atomic snapshot.
Concurrent or reentrant collection requests coalesce with the active collection;
they can observe its last published values without blocking or recursively
rescanning plans. Once collection and execution are quiescent, a new snapshot
refreshes all retained native registrations.

### Prometheus / OpenMetrics

Enable the `prometheus` feature and register a collector in the application's
existing `prometheus-client` registry:

```toml
[dependencies]
datafusion-tracing = { path = "../datafusion-tracing/datafusion-tracing", features = ["prometheus"] }
prometheus-client = "0.25"
```

```rust
use datafusion_tracing::metrics_export::{MetricsExporter, PrometheusMetricsCollector};
use prometheus_client::{encoding::text::encode, registry::Registry};

let exporter = MetricsExporter::new();
let mut registry = Registry::default();
registry.register_collector(Box::new(PrometheusMetricsCollector::new(exporter.clone())));

// Attach exporter to instrumentation options as above.
// In the application's existing /metrics handler:
let mut body = String::new();
encode(&mut body, &registry)?;
// Return body using the content type from prometheus_client::encoding::text.
```

Each scrape requests native collection automatically. Overlapping requests
coalesce and can observe the active collection's last published values. The
application controls the HTTP server and registry. Use sub-registries for a namespace or fixed
workload labels. Register this collector once per namespace; registering duplicates
can produce duplicate metric families. The `operator` label is reserved by the
collector; do not also add it as a registry-level label. Keep query IDs, SQL, trace IDs, object paths,
and other unbounded values out of metric labels.

See [the runnable Prometheus example](../examples/metrics_prometheus.rs).

### The Rust `metrics` facade

Enable the `metrics` feature and pass an application-owned recorder. This supports
any compatible recorder, including backends other than Prometheus. Handles are
bound to that recorder and are unaffected by task-local recorder scopes or later
changes to global recorder configuration.

```toml
[dependencies]
datafusion-tracing = { path = "../datafusion-tracing/datafusion-tracing", features = ["metrics"] }
metrics = "0.24"
```

```rust
use std::sync::Arc;
use datafusion_tracing::metrics_export::{MetricsExporter, RecorderMetricsSink};

fn create_exporter(recorder: Arc<dyn metrics::Recorder + Send + Sync>) -> MetricsExporter {
    MetricsExporter::with_sink(Arc::new(RecorderMetricsSink::new(recorder)))
}
```

Attach the returned exporter to the instrumentation options. Call
`exporter.collect()` before reading the backend, or periodically from an
application-managed reporting task. No thread, timer, global recorder, or listener
is installed by the crate. Recording stream outcomes is immediate, but native
counters of retained plans are refreshed during collection.

The built-in push adapters initialize all schema counters to zero when an operator
is first observed. Error and cancellation series therefore exist before the first
failure; operators that never run may remain absent from push backends.

### OpenTelemetry metrics

Enable the `opentelemetry` feature and supply a meter from the application's meter
provider. This is separate from OpenTelemetry **tracing**; no tracing subscriber
or global meter provider is required.

```toml
[dependencies]
datafusion-tracing = { path = "../datafusion-tracing/datafusion-tracing", features = ["opentelemetry"] }
opentelemetry = { version = "0.31", default-features = false, features = ["metrics"] }
```

```rust
use std::sync::Arc;
use datafusion_tracing::metrics_export::{MetricsExporter, OpenTelemetryMetricsSink};

fn create_exporter(meter: opentelemetry::metrics::Meter) -> MetricsExporter {
    MetricsExporter::with_sink(Arc::new(OpenTelemetryMetricsSink::new(meter)))
}
```

The adapter creates monotonic `u64` counters using the common schema and the UCUM
units `1`, `By`, and `ns`. The application selects its SDK readers, resources,
views, exporters, and delivery protocol. Call `exporter.collect()` on the desired
native-metrics reporting cadence, and once before flushing or shutting down the
meter provider. A periodic SDK reader alone does not discover native counters of
retained plans: collection remains the application's responsibility for push
sinks. Provider flushing and network delivery remain the application's responsibility.

### Custom sinks

`MetricsSink` receives typed, **positive counter deltas** with a bounded operator
label. It can feed an application metrics abstraction, OpenTelemetry counters, a
test collector, or another backend. The core owns source checkpoints and performs
deduplication; sinks add increments and do not need to interpret native snapshots.

```rust
use datafusion_tracing::metrics_export::{ExecutionMetric, MetricsSink};

struct ApplicationSink;

impl MetricsSink for ApplicationSink {
    fn add_counter(&self, metric: ExecutionMetric, operator: &str, delta: u64) {
        // Add delta to an existing application counter identified by
        // metric.name(), metric.unit(), and the operator label.
    }
}
```

Sinks must be thread-safe, fast, nonblocking, and must not panic: callbacks can run
in stream or plan destructors. No exporter locks are held during callbacks.
Concurrent callbacks can arrive in a different order from native sampling;
addition preserves the correct total. Buffer network delivery in the backend.
An application can implement a sink that forwards to multiple backends, or use a
sink and the Prometheus pull collector with the same exporter.

## Counter schema

Every counter has one label, `operator`. The complete names below are stable
across snapshots and adapters.

| Name (prefix `datafusion_operator_`) | Meaning |
| --- | --- |
| `output_rows_total` | Native rows emitted by operators, including intermediate rows |
| `output_batches_total` | Native record batches emitted |
| `output_bytes_total` | Native output bytes; the operator defines its byte accounting |
| `spills_total` | Native spill events |
| `spilled_rows_total` | Native rows spilled to disk |
| `spilled_bytes_total` | Native bytes spilled to disk |
| `elapsed_compute_nanoseconds_total` | Native elapsed compute time, in integer nanoseconds |
| `streams_started_total` | Calls to an instrumented operator's `execute` method |
| `streams_completed_total` | Streams that yielded EOF before any error |
| `streams_failed_total` | Execute calls or streams that returned an error |
| `streams_cancelled_total` | Streams dropped before an error or EOF |
| `native_snapshots_rejected_total` | Collection attempts rejected because selected native registrations have no stable owner |

Missing native counters contribute zero. Generic operator-defined counts/times,
gauges, memory peaks, timestamps, ratios, pruning statistics, and custom values
are not guessed into this schema. In particular, this exporter does not reinterpret
a peak as current memory, export formatted strings, or sum percentages.
Custom operators should use the standard `MetricBuilder::output_rows`,
`elapsed_compute`, and related constructors for this schema. A generic counter
merely named `output_rows` is still generic and is not reinterpreted.

`output_bytes_total` preserves DataFusion's native batch-memory accounting. It can
overestimate memory when Arrow arrays share buffers; it is not unique allocated
memory, serialized bytes, or network traffic.

Compute time uses nanoseconds because the Rust `metrics` facade supports integer
counters. This preserves native precision and a consistent schema across backends.
For seconds in PromQL, divide `rate(datafusion_operator_elapsed_compute_nanoseconds_total[5m])`
by `1e9`. Native compute time includes descheduling; adding it across partitions
or operators does not produce wall-clock query latency or thread CPU usage.

Operator labels are limited to 128 distinct names. Names are truncated at a UTF-8
boundary within 128 bytes, then characters other than ASCII letters, digits, and
underscores become underscores. This gives every adapter the same portable label
without embedding protocol-specific escaping in the value. Further names aggregate
under `__other__`, allowing at most 129 label sets and 1,548 counter series per
exporter. Truncation and normalization can intentionally merge names.
The bound applies even to custom plans with dynamically generated names. Native
partition and file labels are aggregated, not forwarded.

## Lifetimes and correctness

- **Each native registration is accounted once.** Collection reads typed native
  counters and adds only changes since its previous observation. Stream groups,
  concurrent snapshots, or multiple wrappers exposing the same registration do
  not add complete snapshots repeatedly.
- **Reuse and reset have separate identities.** Reusing a physical plan adds its
  additional work. DataFusion's `reset_state()` and plan replacements that create
  new native registrations start new checkpoints. If a transformation retains
  registrations, their existing checkpoints remain in effect.
- **Return references to the same metric objects each time.** When an operator's
  `metrics()` method is called repeatedly, its existing counters must be represented
  by the same `Metric` objects. Their values can keep increasing: a counter read
  first as 100 and later as 120 contributes increments of 100 and 20. The exporter
  recognizes the object and remembers how much it has already counted.
  Custom plans should store an `ExecutionPlanMetricsSet` and return `clone_inner()`,
  which clones references to those objects. Creating new `Metric` objects on every
  call is unsupported, even if they wrap the same counter: DataFusion's API does
  not expose the identity of the counter's backing atomic. Separate registrations
  sharing one backing counter are likewise separate sources; custom plans must not
  register the same measurement twice. A registration shared by differently named
  operators is attributed to the first label under which it is discovered.
- **Unstable snapshots are diagnosed.** A selected registration owned only by the
  returned snapshot has no stable native owner. The exporter rejects that native
  snapshot and increments `native_snapshots_rejected_total`, while continuing to
  record stream outcomes. Monitor this counter when integrating custom sources.
  DataFusion 55's FFI `ForeignExecutionPlan` rebuilds registrations this way and
  cannot provide native counters through this interface. This check detects common
  violations but cannot prove stability when custom code keeps temporary extra
  owners. It does not replace the source contract.
- **Exporter boundaries are not query attribution.** A raw plan shared between
  independent exporters contributes its cumulative native values to each. Share
  one exporter when sharing physical plans; do not use separate exporters as a
  way to infer which query produced changes in a shared native counter.
- **Native discovery is not on the batch path.** It occurs on explicit collection,
  snapshots, and final instrumentation destruction. This avoids repeated scans
  when partitions execute sequentially. Collection costs are proportional to the
  tracked plans and registered native counters, not rows or batches.
  When operator spans are disabled, span metrics are off, and previews are not
  requested, execution omits tracing recorder groups and their stream layers.
  Metrics-only streams retain counter accounting and their native plan ownership.
- **Totals outlive plans; plans do not outlive their owners.** The exporter holds
  weak plan references. It preserves registrations and their checkpoints while a
  raw plan can still return them. Retired registrations are sampled and released;
  completed totals need only bounded operator counter storage. Idle index capacity
  is compacted after large plans retire, rather than retaining their peak allocation.
- **Drop is not success.** A stream records its first error or EOF immediately,
  even if the caller retains the exhausted stream. Dropping before either outcome
  records cancellation, including never-polled streams. A synchronous execute
  error records failure. Subsequent polls and destruction do not change the first
  terminal outcome. Parent operators can cancel children during normal early
  termination (for example, a limit); cancellation is not necessarily a query error.
- **Cancellation can leave partial native work.** Synchronous stream/plan
  destructors run before final sampling. Native updates remain collectable while
  their registrations are retained by plans. Work performed by detached tasks
  after all instrumented owners and native registrations disappear cannot be
  guaranteed: keep the instrumented plan alive until such work finishes if those
  updates must be observed. The exporter neither waits for asynchronous cleanup
  nor changes cancellation behavior.

The increase of each native counter between observations must fit in `usize`.
The collector uses native-width wrapping deltas; an entire counter range of
increments between samples cannot be recovered. Process totals saturate at `u64::MAX`.
Keep the same exporter alive across scrapes; replacing it starts a new cumulative
series, just as restarting a process does. Use separate backend namespaces if
several independent exporters should not be summed together.

## Testing and measuring overhead

Public API regressions cover concurrent and sequential partitions, reused and
reset plans, shared registrations, drop finalization, errors and cancellation,
disabled tracing, label bounds, and custom sinks. Adapter tests verify the schema,
units, exact integer counters, and application-owned registry/recorder behavior.

Compatibility regressions also preserve lessons from the project's resolved
issues: completed spans close while plans remain alive ([#27](https://github.com/datafusion-contrib/datafusion-tracing/issues/27));
span fields remain native snapshots while exporter counters use deltas
([#54](https://github.com/datafusion-contrib/datafusion-tracing/pull/54));
recursive work-table state and plan downcasts remain transparent
([#5](https://github.com/datafusion-contrib/datafusion-tracing/issues/5),
[#63](https://github.com/datafusion-contrib/datafusion-tracing/pull/63)).
Existing optimizer error-cleanup and context-forwarding tests remain part of the
suite. Metrics export adds no phase-span ownership or session-state rebuilding.

Run the tests and the opt-in performance experiment with:

```sh
cargo test -p datafusion-tracing --all-features
cargo test -p datafusion-tracing --test metrics_overhead \
  --config profile.test.package.datafusion-tracing.opt-level=3 \
  -- --ignored --nocapture
```

The experiment compares instrumentation with and without export over many tiny
batches, repeated executions, and fresh plans. It validates totals as well as
reporting timings. Compare results on the same machine and build profile; timings
are evidence, not a portable performance threshold. The command above optimizes
this package's library and experiment; dependency profiles remain unchanged.
