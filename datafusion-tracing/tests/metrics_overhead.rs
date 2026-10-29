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

//! A local performance experiment, excluded from ordinary test runs.
//!
//! Optimize this package's library and experiment while leaving dependency
//! profiles unchanged:
//! `cargo test -p datafusion-tracing --test metrics_overhead
//! --config profile.test.package.datafusion-tracing.opt-level=3
//! -- --ignored --nocapture`.
//!
//! Both modes use the same tracing wrapper with a disabled subscriber. Export
//! collection is measured separately because applications choose its cadence.
//! Results include native metric registration and plan retirement; they do not
//! represent an end-to-end SQL benchmark or enforce a machine-specific threshold.

use datafusion::arrow::array::{Int64Array, RecordBatch};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::common::{Result, tree_node::TreeNodeRecursion};
use datafusion::config::ConfigOptions;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::metrics::{
    BaselineMetrics, ExecutionPlanMetricsSet, MetricsSet,
};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, ExecutionPlan, PhysicalExpr, PlanProperties,
    ReplaceChildrenOptions,
};
use datafusion_tracing::metrics_export::{ExecutionMetric, MetricsExporter};
use datafusion_tracing::{InstrumentationOptions, instrument_with_info_spans};
use futures::{Stream, StreamExt};
use std::fmt;
use std::hint::black_box;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

/// Registers fresh baseline metrics on every execute, as built-in operators do.
#[derive(Debug)]
struct BatchExec {
    properties: Arc<PlanProperties>,
    metrics: ExecutionPlanMetricsSet,
    batch: RecordBatch,
    batches_per_stream: usize,
}

impl BatchExec {
    fn new(batches_per_stream: usize) -> Self {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![Arc::new(Int64Array::from(vec![1]))],
        )
        .unwrap();
        Self {
            properties: EmptyExec::new(schema).properties().clone(),
            metrics: ExecutionPlanMetricsSet::new(),
            batch,
            batches_per_stream,
        }
    }
}

impl DisplayAs for BatchExec {
    fn fmt_as(&self, _: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str("BatchExec")
    }
}

impl ExecutionPlan for BatchExec {
    fn name(&self) -> &str {
        "BatchExec"
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

    fn metrics(&self) -> Option<MetricsSet> {
        Some(self.metrics.clone_inner())
    }

    fn execute(
        &self,
        partition: usize,
        _: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        assert_eq!(partition, 0);
        Ok(Box::pin(BatchStream {
            batch: self.batch.clone(),
            remaining: self.batches_per_stream,
            baseline: BaselineMetrics::new(&self.metrics, partition),
        }))
    }
}

struct BatchStream {
    batch: RecordBatch,
    remaining: usize,
    baseline: BaselineMetrics,
}

impl Stream for BatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        _: &mut Context<'_>,
    ) -> Poll<Option<Self::Item>> {
        let result = if self.remaining == 0 {
            Poll::Ready(None)
        } else {
            self.remaining -= 1;
            Poll::Ready(Some(Ok(self.batch.clone())))
        };
        self.baseline.record_poll(result)
    }
}

impl RecordBatchStream for BatchStream {
    fn schema(&self) -> SchemaRef {
        self.batch.schema()
    }
}

fn consume(mut stream: SendableRecordBatchStream) -> u64 {
    futures::executor::block_on(async move {
        let mut rows = 0;
        while let Some(batch) = stream.next().await {
            rows += black_box(batch.unwrap()).num_rows() as u64;
        }
        rows
    })
}

#[derive(Clone, Copy)]
enum Case {
    ManyBatches,
    ReusedPlan,
    FreshPlans,
}

impl Case {
    const ALL: [Self; 3] = [Self::ManyBatches, Self::ReusedPlan, Self::FreshPlans];

    fn name(self) -> &'static str {
        match self {
            Self::ManyBatches => "100000 tiny batches in one stream",
            Self::ReusedPlan => "1000 executions with fresh native registrations",
            Self::FreshPlans => "1000 separately instrumented plans",
        }
    }

    fn units(self) -> usize {
        match self {
            Self::ManyBatches => 100_000,
            Self::ReusedPlan | Self::FreshPlans => 1_000,
        }
    }

    fn executions(self) -> u64 {
        match self {
            Self::ManyBatches => 1,
            Self::ReusedPlan | Self::FreshPlans => self.units() as u64,
        }
    }
}

struct Sample {
    execution: Duration,
    collection_and_retirement: Duration,
}

fn sample(case: Case, export: bool) -> Sample {
    let exporter = export.then(MetricsExporter::new);
    let options = match &exporter {
        Some(exporter) => InstrumentationOptions::builder()
            .metrics_exporter(exporter.clone())
            .build(),
        None => InstrumentationOptions::default(),
    };
    let rule = instrument_with_info_spans!(options: options);
    run_sample(case, rule.as_ref(), exporter)
}

fn run_sample(
    case: Case,
    rule: &dyn PhysicalOptimizerRule,
    exporter: Option<MetricsExporter>,
) -> Sample {
    let config = ConfigOptions::default();
    let context = Arc::new(TaskContext::default());
    let batches = match case {
        Case::ManyBatches => case.units(),
        Case::ReusedPlan | Case::FreshPlans => 1,
    };
    // Setup for a retained plan is outside the execution measurement. FreshPlans
    // includes construction and teardown in its timed loop, including export work.
    let retained = if matches!(case, Case::FreshPlans) {
        None
    } else {
        Some(
            rule.optimize(Arc::new(BatchExec::new(batches)), &config)
                .unwrap(),
        )
    };
    let start = Instant::now();
    let mut rows = 0;
    for _ in 0..case.executions() {
        let plan = retained.clone().unwrap_or_else(|| {
            rule.optimize(Arc::new(BatchExec::new(batches)), &config)
                .unwrap()
        });
        rows += consume(plan.execute(0, context.clone()).unwrap());
    }
    let execution = start.elapsed();
    assert_eq!(black_box(rows), case.units() as u64);

    let start = Instant::now();
    if let Some(exporter) = &exporter {
        exporter.collect();
    }
    drop(retained);
    if let Some(exporter) = &exporter {
        exporter.collect();
    }
    let collection_and_retirement = start.elapsed();
    // Verify after timing so a fast but incorrect path cannot look successful.
    if let Some(exporter) = exporter {
        let snapshot = exporter.snapshot();
        assert_eq!(snapshot.len(), 1);
        assert_eq!(snapshot[0].value(ExecutionMetric::OutputRows), rows);
        assert_eq!(snapshot[0].value(ExecutionMetric::OutputBatches), rows);
        assert_eq!(
            snapshot[0].value(ExecutionMetric::StreamsStarted),
            case.executions()
        );
        assert_eq!(
            snapshot[0].value(ExecutionMetric::StreamsCompleted),
            case.executions()
        );
    }
    Sample {
        execution,
        collection_and_retirement,
    }
}

fn median_ns(samples: &[Sample], duration: impl Fn(&Sample) -> Duration) -> f64 {
    let mut values = samples
        .iter()
        .map(|sample| duration(sample).as_nanos())
        .collect::<Vec<_>>();
    values.sort_unstable();
    values[values.len() / 2] as f64
}

fn total_range_ns(samples: &[Sample], units: f64) -> (f64, f64) {
    samples
        .iter()
        .fold((f64::INFINITY, 0.0_f64), |(min, max), sample| {
            let value = (sample.execution + sample.collection_and_retirement).as_nanos()
                as f64
                / units;
            (min.min(value), max.max(value))
        })
}

#[test]
#[ignore = "local performance experiment; use an optimized build and --nocapture"]
fn metrics_export_overhead() {
    let _subscriber =
        tracing::subscriber::set_default(tracing::subscriber::NoSubscriber::default());
    for case in Case::ALL {
        // Warm both paths, then alternate their order to limit cache/warmup bias.
        sample(case, false);
        sample(case, true);
        let mut baseline = Vec::new();
        let mut exported = Vec::new();
        for repetition in 0..7 {
            for export in if repetition % 2 == 0 {
                [false, true]
            } else {
                [true, false]
            } {
                let result = sample(case, export);
                if export {
                    exported.push(result);
                } else {
                    baseline.push(result);
                }
            }
        }
        let units = case.units() as f64;
        let execution_baseline = median_ns(&baseline, |sample| sample.execution) / units;
        let execution_exported = median_ns(&exported, |sample| sample.execution) / units;
        let total_baseline = median_ns(&baseline, |sample| {
            sample.execution + sample.collection_and_retirement
        }) / units;
        let total_exported = median_ns(&exported, |sample| {
            sample.execution + sample.collection_and_retirement
        }) / units;
        println!("{}:", case.name());
        println!(
            "  execution: baseline={execution_baseline:.1} ns/unit, export={execution_exported:.1} ns/unit ({:+.1}%)",
            (execution_exported / execution_baseline - 1.0) * 100.0
        );
        println!(
            "  collection and retirement: baseline={:.1} ns/unit, export={:.1} ns/unit",
            median_ns(&baseline, |sample| sample.collection_and_retirement) / units,
            median_ns(&exported, |sample| sample.collection_and_retirement) / units
        );
        println!(
            "  including collection and retirement: baseline={total_baseline:.1} ns/unit, export={total_exported:.1} ns/unit ({:+.1}%)",
            (total_exported / total_baseline - 1.0) * 100.0
        );
        let (baseline_min, baseline_max) = total_range_ns(&baseline, units);
        let (export_min, export_max) = total_range_ns(&exported, units);
        println!(
            "  observed total range: baseline=[{baseline_min:.1}, {baseline_max:.1}], export=[{export_min:.1}, {export_max:.1}] ns/unit"
        );
    }
}
