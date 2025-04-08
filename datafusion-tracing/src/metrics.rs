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

use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::{field, Span};

/// Records execution metrics and automatically logs them via tracing upon completion.
///
/// This struct is designed to handle metrics safely across concurrent partition executions.
pub(crate) struct MetricsRecorder {
    /// Execution plan to retrieve metrics from once stream execution is finished.
    execution_plan: Arc<dyn ExecutionPlan>,

    /// Tracing span used to log metrics for observability.
    span: Span,
}

impl MetricsRecorder {
    pub fn new(execution_plan: Arc<dyn ExecutionPlan>, span: Span) -> Self {
        Self {
            execution_plan,
            span,
        }
    }
}

impl Drop for MetricsRecorder {
    /// Aggregates and logs the collected metrics when the recorder goes out of scope,
    /// which should only happen once all executed partition streams have completed.
    fn drop(&mut self) {
        let Some(metrics) = self.execution_plan.metrics() else {
            return;
        };
        for metric in metrics.iter() {
            self.span.record(
                format!(
                    "datafusion.metrics._{}.{}",
                    metric.partition().unwrap_or_default(),
                    metric.value().name()
                )
                .as_str(),
                field::display(metric.value()),
            );
        }
        for metric in metrics.aggregate_by_name().iter() {
            self.span.record(
                format!("datafusion.metrics.{}", metric.value().name()).as_str(),
                field::display(metric.value()),
            );
        }
    }
}

/// A wrapper around a DataFusion stream that integrates automatic metrics recording.
///
/// It uses `MetricsRecorder` to safely handle metric aggregation and recording, avoiding concurrency issues.
#[pin_project]
pub(crate) struct MetricsRecordingStream {
    /// The underlying DataFusion stream providing record batches.
    #[pin] // Ensures safe pinning behavior required by the inner stream.
    inner: SendableRecordBatchStream,

    /// Shared recorder used for metric aggregation and logging, which will record the metrics when dropped.
    metrics_recorder: Arc<MetricsRecorder>,
}

impl MetricsRecordingStream {
    /// Wraps a DataFusion stream with metrics recording capability.
    ///
    /// The provided `MetricsRecorder` will handle logging the metrics via tracing when it is dropped.
    pub fn new(
        inner: SendableRecordBatchStream,
        metrics_recorder: Arc<MetricsRecorder>,
    ) -> Self {
        Self {
            inner,
            metrics_recorder,
        }
    }
}

impl Stream for MetricsRecordingStream {
    type Item = datafusion::common::Result<RecordBatch>;

    /// Retrieves the next record batch from the underlying stream.
    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl RecordBatchStream for MetricsRecordingStream {
    /// Returns the schema for the record batches produced by the wrapped stream.
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
