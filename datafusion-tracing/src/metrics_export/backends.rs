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

#[cfg(feature = "prometheus")]
pub use prometheus::PrometheusMetricsCollector;
#[cfg(feature = "metrics")]
pub use recorder::RecorderMetricsSink;
#[cfg(feature = "opentelemetry")]
pub use telemetry::OpenTelemetryMetricsSink;

#[cfg(feature = "prometheus")]
mod prometheus {
    use super::super::{ExecutionMetric, MetricUnit, MetricsExporter};
    use prometheus_client::collector::Collector;
    use prometheus_client::encoding::{DescriptorEncoder, EncodeMetric};
    use prometheus_client::metrics::MetricType;
    use prometheus_client::metrics::counter::ConstCounter;
    use prometheus_client::registry::Unit;

    /// Collects execution counters into an application-owned OpenMetrics registry.
    ///
    /// Available with the `prometheus` feature. Register this collector through
    /// `Registry::register_collector`. Each scrape requests native collection
    /// before encoding; concurrent requests coalesce as described by
    /// [`MetricsExporter::collect`]. The application controls serving and scheduling.
    ///
    /// The only generated label is `operator`. Use a sub-registry to add fixed
    /// application labels or namespace independent exporters. Compute duration is
    /// an integer nanosecond counter, matching other adapters without truncating
    /// short operations; divide its value by `1e9` when displaying seconds.
    #[derive(Clone, Debug)]
    pub struct PrometheusMetricsCollector {
        exporter: MetricsExporter,
    }

    impl PrometheusMetricsCollector {
        /// Creates a pull adapter over `exporter`.
        ///
        /// No listener, task, registry, or global state is created. Register the
        /// collector once per registry or use distinct sub-registry prefixes to
        /// avoid duplicate metric families.
        pub fn new(exporter: MetricsExporter) -> Self {
            Self { exporter }
        }
    }

    impl Collector for PrometheusMetricsCollector {
        fn encode(
            &self,
            mut encoder: DescriptorEncoder<'_>,
        ) -> Result<(), std::fmt::Error> {
            // Take one snapshot per scrape, rather than recollecting each family.
            let snapshot = self.exporter.snapshot();
            for &metric in ExecutionMetric::ALL {
                let family_name = metric.name().trim_end_matches("_total");
                let (name, unit) = match metric.unit() {
                    MetricUnit::Count => (family_name, None),
                    MetricUnit::Bytes => {
                        (family_name.trim_end_matches("_bytes"), Some(Unit::Bytes))
                    }
                    MetricUnit::Nanoseconds => (
                        family_name.trim_end_matches("_nanoseconds"),
                        Some(Unit::Other("nanoseconds".to_owned())),
                    ),
                };
                let mut family = encoder.encode_descriptor(
                    name,
                    metric.description(),
                    unit.as_ref(),
                    MetricType::Counter,
                )?;
                for operator in &snapshot {
                    ConstCounter::new(operator.value(metric)).encode(
                        family
                            .encode_family(&[("operator", operator.operator.as_str())])?,
                    )?;
                }
            }
            Ok(())
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use prometheus_client::encoding::text::encode;
        use prometheus_client::registry::Registry;

        #[test]
        fn preserves_names_types_units_and_integer_precision() {
            let exporter = MetricsExporter::new();
            let operator = exporter.operator("ProjectionExec");
            let large_value = (1_u64 << 53) + 1;
            for &metric in ExecutionMetric::ALL {
                operator.add(metric, large_value);
                operator.add(metric, 2);
            }
            let mut registry = Registry::default();
            registry
                .register_collector(Box::new(PrometheusMetricsCollector::new(exporter)));
            let mut output = String::new();
            encode(&mut output, &registry).unwrap();
            for &metric in ExecutionMetric::ALL {
                let family_name = metric.name().trim_end_matches("_total");
                assert!(output.contains(&format!("# TYPE {family_name} counter\n")));
                assert!(output.contains(&format!(
                    "{}{{operator=\"ProjectionExec\"}} {}\n",
                    metric.name(),
                    large_value + 2
                )));
                assert!(output.contains(&format!(
                    "# HELP {family_name} {}\n",
                    metric.description()
                )));
                match metric.unit() {
                    MetricUnit::Count => {}
                    MetricUnit::Bytes => {
                        assert!(output.contains(&format!("# UNIT {family_name} bytes\n")))
                    }
                    MetricUnit::Nanoseconds => assert!(
                        output.contains(&format!("# UNIT {family_name} nanoseconds\n"))
                    ),
                }
            }
            assert!(output.ends_with("# EOF\n"));
        }

        #[test]
        fn respects_registry_prefix_and_refreshes_each_scrape() {
            let exporter = MetricsExporter::new();
            use datafusion::physical_plan::metrics::{Count, Metric, MetricValue};
            use std::sync::Arc;

            let operator = exporter.operator("ProjectionExec");
            let count = Count::new();
            let registration =
                Arc::new(Metric::new(MetricValue::OutputRows(count.clone()), Some(0)));
            exporter.source(&registration, ExecutionMetric::OutputRows, &operator);
            let mut registry = Registry::with_prefix("service");
            registry
                .register_collector(Box::new(PrometheusMetricsCollector::new(exporter)));
            count.add(3);
            let mut output = String::new();
            encode(&mut output, &registry).unwrap();
            assert!(output.contains(
                "service_datafusion_operator_output_rows_total{operator=\"ProjectionExec\"} 3\n"
            ));
            count.add(4);
            output.clear();
            encode(&mut output, &registry).unwrap();
            assert!(output.contains(
                "service_datafusion_operator_output_rows_total{operator=\"ProjectionExec\"} 7\n"
            ));
        }

        #[test]
        fn encodes_portable_operator_labels() {
            let exporter = MetricsExporter::new();
            exporter
                .operator("Custom\"Exec\\\n")
                .add(ExecutionMetric::OutputRows, 3);
            let mut registry = Registry::default();
            registry
                .register_collector(Box::new(PrometheusMetricsCollector::new(exporter)));
            let mut output = String::new();
            encode(&mut output, &registry).unwrap();
            assert!(output.contains(
                "datafusion_operator_output_rows_total{operator=\"Custom_Exec__\"} 3\n"
            ));
        }
    }
}

#[cfg(feature = "metrics")]
mod recorder {
    use super::super::{ExecutionMetric, MetricUnit, MetricsSink};
    use ::metrics::{Counter, Key, Level, Metadata, Recorder, Unit};
    use std::collections::HashMap;
    use std::fmt::{self, Debug};
    use std::sync::{Arc, RwLock};

    type OperatorCounters = HashMap<ExecutionMetric, Counter>;

    /// Records execution counters through an application-provided `metrics` recorder.
    ///
    /// Available with the `metrics` feature. This adapter registers directly with
    /// the supplied recorder and caches its handles. It does not install or consult
    /// a global or thread-local recorder, so asynchronous execution uses the same
    /// backend on every thread.
    ///
    /// Counters use their native integer units, including nanoseconds for compute
    /// duration. The only generated label is `operator`. Recorder implementations
    /// remain responsible for their own filtering, aggregation, and export.
    /// On the first observation of an operator, all its counters are initialized
    /// to zero so error and cancellation series exist before the first failure.
    pub struct RecorderMetricsSink {
        recorder: Arc<dyn Recorder + Send + Sync>,
        counters: RwLock<HashMap<String, Arc<OperatorCounters>>>,
    }

    impl Debug for RecorderMetricsSink {
        fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
            formatter
                .debug_struct("RecorderMetricsSink")
                .finish_non_exhaustive()
        }
    }

    impl RecorderMetricsSink {
        /// Describes the execution counters to `recorder` and creates an adapter.
        ///
        /// Counter handles are cached per operator. Concurrent first
        /// observations may call `register_counter` more than once for the same key,
        /// as permitted by the `metrics::Recorder` contract.
        pub fn new(recorder: Arc<dyn Recorder + Send + Sync>) -> Self {
            for &metric in ExecutionMetric::ALL {
                let unit = match metric.unit() {
                    MetricUnit::Count => Unit::Count,
                    MetricUnit::Bytes => Unit::Bytes,
                    MetricUnit::Nanoseconds => Unit::Nanoseconds,
                };
                recorder.describe_counter(
                    metric.name().into(),
                    Some(unit),
                    metric.description().into(),
                );
            }
            Self {
                recorder,
                counters: RwLock::new(HashMap::new()),
            }
        }
    }

    impl MetricsSink for RecorderMetricsSink {
        fn add_counter(&self, metric: ExecutionMetric, operator: &str, delta: u64) {
            let existing = self
                .counters
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .get(operator)
                .cloned();
            let counters = existing.unwrap_or_else(|| {
                let metadata = Metadata::new(
                    "datafusion_tracing",
                    Level::INFO,
                    Some(module_path!()),
                );
                // Registration can call application code. Hold no cache lock
                // while registering the complete set of operator counters.
                let created = Arc::new(
                    ExecutionMetric::ALL
                        .iter()
                        .copied()
                        .map(|metric| {
                            let key = Key::from_parts(
                                metric.name(),
                                vec![::metrics::Label::new(
                                    "operator",
                                    operator.to_owned(),
                                )],
                            );
                            (metric, self.recorder.register_counter(&key, &metadata))
                        })
                        .collect::<HashMap<_, _>>(),
                );
                let (counters, inserted) = {
                    let mut cache =
                        self.counters.write().unwrap_or_else(|e| e.into_inner());
                    if let Some(existing) = cache.get(operator) {
                        (existing.clone(), false)
                    } else {
                        cache.insert(operator.to_owned(), created.clone());
                        (created, true)
                    }
                };
                if inserted {
                    // Publish the handles before invoking callbacks, permitting
                    // reentrant recording. Concurrent positive increments may
                    // precede these harmless zero increments.
                    for counter in counters.values() {
                        counter.increment(0);
                    }
                }
                counters
            });
            counters[&metric].increment(delta);
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use ::metrics::{CounterFn, Gauge, Histogram, KeyName, SharedString};
        use std::sync::Mutex;
        use std::sync::atomic::{AtomicU64, Ordering};

        #[derive(Default)]
        struct TestCounter {
            value: AtomicU64,
            zeros: AtomicU64,
        }

        impl CounterFn for TestCounter {
            fn increment(&self, value: u64) {
                self.value.fetch_add(value, Ordering::Relaxed);
                if value == 0 {
                    self.zeros.fetch_add(1, Ordering::Relaxed);
                }
            }

            fn absolute(&self, value: u64) {
                self.value.fetch_max(value, Ordering::Relaxed);
            }
        }

        #[derive(Default)]
        struct TestRecorder {
            descriptions: Mutex<Vec<(KeyName, Option<Unit>, SharedString)>>,
            registrations: Mutex<Vec<Key>>,
            counters: Mutex<HashMap<Key, Arc<TestCounter>>>,
            on_register: Mutex<Option<Arc<dyn Fn() + Send + Sync>>>,
        }

        impl Recorder for TestRecorder {
            fn describe_counter(
                &self,
                name: KeyName,
                unit: Option<Unit>,
                description: SharedString,
            ) {
                self.descriptions
                    .lock()
                    .unwrap()
                    .push((name, unit, description));
            }

            fn describe_gauge(&self, _: KeyName, _: Option<Unit>, _: SharedString) {
                panic!("the execution adapter only describes counters");
            }

            fn describe_histogram(&self, _: KeyName, _: Option<Unit>, _: SharedString) {
                panic!("the execution adapter only describes counters");
            }

            fn register_counter(&self, key: &Key, metadata: &Metadata<'_>) -> Counter {
                let callback = self.on_register.lock().unwrap().take();
                if let Some(callback) = callback {
                    callback();
                }
                assert_eq!(metadata.target(), "datafusion_tracing");
                assert_eq!(metadata.level(), &Level::INFO);
                self.registrations.lock().unwrap().push(key.clone());
                Counter::from_arc(
                    self.counters
                        .lock()
                        .unwrap()
                        .entry(key.clone())
                        .or_default()
                        .clone(),
                )
            }

            fn register_gauge(&self, _: &Key, _: &Metadata<'_>) -> Gauge {
                panic!("the execution adapter only registers counters");
            }

            fn register_histogram(&self, _: &Key, _: &Metadata<'_>) -> Histogram {
                panic!("the execution adapter only registers counters");
            }
        }

        #[test]
        fn uses_explicit_recorder_with_units_and_cached_labeled_handles() {
            let recorder = Arc::new(TestRecorder::default());
            let sink = RecorderMetricsSink::new(recorder.clone());
            for &metric in ExecutionMetric::ALL {
                sink.add_counter(metric, "ProjectionExec", 10);
                sink.add_counter(metric, "ProjectionExec", 3);
            }

            let descriptions = recorder.descriptions.lock().unwrap();
            assert_eq!(descriptions.len(), ExecutionMetric::ALL.len());
            for (name, unit, description) in descriptions.iter() {
                let metric = ExecutionMetric::ALL
                    .iter()
                    .copied()
                    .find(|metric| metric.name() == name.as_str())
                    .unwrap();
                let expected_unit = match metric.unit() {
                    MetricUnit::Count => Unit::Count,
                    MetricUnit::Bytes => Unit::Bytes,
                    MetricUnit::Nanoseconds => Unit::Nanoseconds,
                };
                assert_eq!(*unit, Some(expected_unit));
                assert_eq!(description.as_ref(), metric.description());
            }
            let registrations = recorder.registrations.lock().unwrap();
            assert_eq!(registrations.len(), ExecutionMetric::ALL.len());
            for key in registrations.iter() {
                let labels = key.labels().collect::<Vec<_>>();
                assert_eq!(labels.len(), 1);
                assert_eq!(labels[0].key(), "operator");
                assert_eq!(labels[0].value(), "ProjectionExec");
                assert_eq!(
                    recorder.counters.lock().unwrap()[key]
                        .value
                        .load(Ordering::Relaxed),
                    13
                );
            }
        }

        #[test]
        fn concurrent_updates_reuse_cached_handles_and_preserve_totals() {
            let recorder = Arc::new(TestRecorder::default());
            let sink = Arc::new(RecorderMetricsSink::new(recorder.clone()));
            std::thread::scope(|scope| {
                for _ in 0..8 {
                    let sink = sink.clone();
                    scope.spawn(move || {
                        for _ in 0..100 {
                            sink.add_counter(ExecutionMetric::OutputRows, "ScanExec", 1);
                        }
                    });
                }
            });
            let registrations = recorder.registrations.lock().unwrap().len();
            assert!(
                (ExecutionMetric::ALL.len()..=8 * ExecutionMetric::ALL.len())
                    .contains(&registrations)
            );
            assert_eq!(registrations % ExecutionMetric::ALL.len(), 0);
            sink.add_counter(ExecutionMetric::OutputRows, "ScanExec", 1);
            assert_eq!(recorder.registrations.lock().unwrap().len(), registrations);
            let counters = recorder.counters.lock().unwrap();
            assert_eq!(counters.len(), ExecutionMetric::ALL.len());
            assert_eq!(
                counters
                    .values()
                    .map(|counter| counter.value.load(Ordering::Relaxed))
                    .sum::<u64>(),
                801
            );
        }

        #[test]
        fn registration_callbacks_can_reenter_the_sink() {
            let recorder = Arc::new(TestRecorder::default());
            let sink = Arc::new(RecorderMetricsSink::new(recorder.clone()));
            let weak_sink = Arc::downgrade(&sink);
            *recorder.on_register.lock().unwrap() = Some(Arc::new(move || {
                weak_sink.upgrade().unwrap().add_counter(
                    ExecutionMetric::SpillCount,
                    "SortExec",
                    2,
                );
            }));
            sink.add_counter(ExecutionMetric::OutputRows, "SortExec", 5);
            let counters = recorder.counters.lock().unwrap();
            assert_eq!(counters.len(), ExecutionMetric::ALL.len());
            assert_eq!(
                counters
                    .values()
                    .map(|counter| counter.value.load(Ordering::Relaxed))
                    .sum::<u64>(),
                7
            );
        }

        #[test]
        fn first_observation_initializes_zero_error_and_cancellation_series() {
            let recorder = Arc::new(TestRecorder::default());
            let sink = RecorderMetricsSink::new(recorder.clone());
            assert!(recorder.counters.lock().unwrap().is_empty());
            sink.add_counter(ExecutionMetric::StreamsStarted, "ScanExec", 1);
            let counters = recorder.counters.lock().unwrap();
            assert_eq!(counters.len(), ExecutionMetric::ALL.len());
            for &metric in ExecutionMetric::ALL {
                let key = Key::from_parts(
                    metric.name(),
                    vec![::metrics::Label::new("operator", "ScanExec")],
                );
                let counter = &counters[&key];
                assert_eq!(counter.zeros.load(Ordering::Relaxed), 1);
                assert_eq!(
                    counter.value.load(Ordering::Relaxed),
                    u64::from(metric == ExecutionMetric::StreamsStarted)
                );
            }
        }
    }
}

#[cfg(feature = "opentelemetry")]
mod telemetry {
    use super::super::{ExecutionMetric, MetricUnit, MetricsSink};
    use opentelemetry::KeyValue;
    use opentelemetry::metrics::{Counter, Meter};
    use std::collections::HashMap;
    use std::sync::{Arc, RwLock};

    type OperatorAttributes = Arc<[KeyValue; 1]>;

    /// Records execution counters through an application-provided OpenTelemetry meter.
    ///
    /// Available with the `opentelemetry` feature. The application chooses the
    /// meter provider, resource, readers, exporters, and reporting cadence. This
    /// adapter creates synchronous integer counters and does not install a global
    /// provider or perform network export itself.
    ///
    /// Configure the [`MetricsExporter`](super::super::MetricsExporter) with this
    /// sink and call its `collect` method periodically for live native values.
    /// Destruction of the final instrumented plan/stream owner also refreshes
    /// native counters.
    /// Measurements use the `operator` attribute and retain their integer units,
    /// including nanoseconds for compute duration. SDK views can select or rename
    /// instruments and choose aggregation according to application policy.
    /// The first observation of an operator initializes all of its counters to
    /// zero, making error and cancellation series available before any failure.
    #[derive(Clone, Debug)]
    pub struct OpenTelemetryMetricsSink {
        counters: Arc<HashMap<ExecutionMetric, Counter<u64>>>,
        attributes: Arc<RwLock<HashMap<String, OperatorAttributes>>>,
    }

    impl OpenTelemetryMetricsSink {
        /// Creates the execution counter instruments in `meter`.
        ///
        /// Keep the meter provider alive and flush or shut it down through the
        /// application when export must complete. No global provider is consulted.
        pub fn new(meter: Meter) -> Self {
            let counters = ExecutionMetric::ALL
                .iter()
                .copied()
                .map(|metric| {
                    let unit = match metric.unit() {
                        MetricUnit::Count => "1",
                        MetricUnit::Bytes => "By",
                        MetricUnit::Nanoseconds => "ns",
                    };
                    (
                        metric,
                        meter
                            .u64_counter(metric.name())
                            .with_description(metric.description())
                            .with_unit(unit)
                            .build(),
                    )
                })
                .collect();
            Self {
                counters: Arc::new(counters),
                attributes: Arc::new(RwLock::new(HashMap::new())),
            }
        }
    }

    impl MetricsSink for OpenTelemetryMetricsSink {
        fn add_counter(&self, metric: ExecutionMetric, operator: &str, delta: u64) {
            let existing = self
                .attributes
                .read()
                .unwrap_or_else(|e| e.into_inner())
                .get(operator)
                .cloned();
            let attributes = existing.unwrap_or_else(|| {
                let (attributes, inserted) = {
                    let mut cache =
                        self.attributes.write().unwrap_or_else(|e| e.into_inner());
                    if let Some(existing) = cache.get(operator) {
                        (existing.clone(), false)
                    } else {
                        let attributes =
                            Arc::new([KeyValue::new("operator", operator.to_owned())]);
                        cache.insert(operator.to_owned(), attributes.clone());
                        (attributes, true)
                    }
                };
                if inserted {
                    for counter in self.counters.values() {
                        counter.add(0, attributes.as_ref());
                    }
                }
                attributes
            });
            self.counters[&metric].add(delta, attributes.as_ref());
        }
    }

    #[cfg(test)]
    mod tests {
        use super::*;
        use opentelemetry::metrics::MeterProvider;
        use opentelemetry_sdk::error::OTelSdkResult;
        use opentelemetry_sdk::metrics::data::{
            AggregatedMetrics, MetricData, ResourceMetrics,
        };
        use opentelemetry_sdk::metrics::exporter::PushMetricExporter;
        use opentelemetry_sdk::metrics::{PeriodicReader, SdkMeterProvider, Temporality};
        use std::sync::{Arc, Mutex};
        use std::time::Duration;

        #[derive(Clone, Debug)]
        struct MetricSummary {
            scope: String,
            name: String,
            description: String,
            unit: String,
            monotonic: bool,
            temporality: Temporality,
            points: Vec<(Vec<KeyValue>, u64)>,
        }

        #[derive(Clone, Default)]
        struct TestExporter {
            metrics: Arc<Mutex<Vec<MetricSummary>>>,
        }

        impl PushMetricExporter for TestExporter {
            fn export(
                &self,
                metrics: &ResourceMetrics,
            ) -> impl Future<Output = OTelSdkResult> + Send {
                let summaries = metrics
                    .scope_metrics()
                    .flat_map(|scope| {
                        scope.metrics().map(move |metric| {
                            let AggregatedMetrics::U64(MetricData::Sum(sum)) =
                                metric.data()
                            else {
                                panic!(
                                    "expected integer counter sum for {}",
                                    metric.name()
                                );
                            };
                            MetricSummary {
                                scope: scope.scope().name().to_owned(),
                                name: metric.name().to_owned(),
                                description: metric.description().to_owned(),
                                unit: metric.unit().to_owned(),
                                monotonic: sum.is_monotonic(),
                                temporality: sum.temporality(),
                                points: sum
                                    .data_points()
                                    .map(|point| {
                                        (
                                            point.attributes().cloned().collect(),
                                            point.value(),
                                        )
                                    })
                                    .collect(),
                            }
                        })
                    })
                    .collect();
                *self.metrics.lock().unwrap() = summaries;
                std::future::ready(Ok(()))
            }

            fn force_flush(&self) -> OTelSdkResult {
                Ok(())
            }

            fn shutdown_with_timeout(&self, _: Duration) -> OTelSdkResult {
                Ok(())
            }

            fn temporality(&self) -> Temporality {
                Temporality::Cumulative
            }
        }

        #[test]
        fn exports_exact_integer_counters_with_metadata_and_operator_attributes() {
            let exporter = TestExporter::default();
            let reader = PeriodicReader::builder(exporter.clone())
                .with_interval(Duration::from_secs(3600))
                .build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            let sink = OpenTelemetryMetricsSink::new(provider.meter("execution_test"));
            let large_value = (1_u64 << 53) + 1;
            for &metric in ExecutionMetric::ALL {
                sink.add_counter(metric, "ProjectionExec", large_value);
                sink.clone().add_counter(metric, "ProjectionExec", 2);
                sink.add_counter(metric, "SortExec", 5);
            }
            provider.force_flush().unwrap();

            let metrics = exporter.metrics.lock().unwrap().clone();
            assert_eq!(metrics.len(), ExecutionMetric::ALL.len());
            for metric in metrics {
                assert_eq!(metric.scope, "execution_test");
                let kind = ExecutionMetric::ALL
                    .iter()
                    .copied()
                    .find(|kind| kind.name() == metric.name)
                    .unwrap();
                assert_eq!(metric.description, kind.description());
                assert_eq!(
                    metric.unit,
                    match kind.unit() {
                        MetricUnit::Count => "1",
                        MetricUnit::Bytes => "By",
                        MetricUnit::Nanoseconds => "ns",
                    }
                );
                assert!(metric.monotonic);
                assert_eq!(metric.temporality, Temporality::Cumulative);
                assert_eq!(metric.points.len(), 2);
                for (attributes, value) in metric.points {
                    assert_eq!(attributes.len(), 1);
                    assert_eq!(attributes[0].key.as_str(), "operator");
                    assert_eq!(
                        value,
                        match attributes[0].value.as_str().as_ref() {
                            "ProjectionExec" => large_value + 2,
                            "SortExec" => 5,
                            unexpected => panic!("unexpected operator {unexpected}"),
                        }
                    );
                }
            }
            provider.shutdown().unwrap();
        }

        #[test]
        fn first_observation_initializes_all_series_and_clones_share_attributes() {
            let exporter = TestExporter::default();
            let reader = PeriodicReader::builder(exporter.clone())
                .with_interval(Duration::from_secs(3600))
                .build();
            let provider = SdkMeterProvider::builder().with_reader(reader).build();
            let sink = OpenTelemetryMetricsSink::new(provider.meter("execution_test"));
            let cloned_sink = sink.clone();
            assert!(Arc::ptr_eq(&sink.attributes, &cloned_sink.attributes));
            assert!(sink.attributes.read().unwrap().is_empty());
            cloned_sink.add_counter(ExecutionMetric::StreamsStarted, "ScanExec", 1);
            assert_eq!(sink.attributes.read().unwrap().len(), 1);
            provider.force_flush().unwrap();

            let metrics = exporter.metrics.lock().unwrap().clone();
            assert_eq!(metrics.len(), ExecutionMetric::ALL.len());
            for metric in metrics {
                assert_eq!(metric.points.len(), 1);
                let (attributes, value) = &metric.points[0];
                assert_eq!(attributes, &[KeyValue::new("operator", "ScanExec")]);
                assert_eq!(
                    *value,
                    u64::from(metric.name == ExecutionMetric::StreamsStarted.name())
                );
            }
            provider.shutdown().unwrap();
        }
    }
}
