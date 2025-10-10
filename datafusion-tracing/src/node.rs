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

use crate::utils::DefaultDisplay;
use datafusion::arrow::array::RecordBatch;
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use datafusion::physical_plan::ExecutionPlan;
use futures::Stream;
use pin_project::pin_project;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};
use tracing::{Span, field};

/// Records the `datafusion.node` span field once execution completes across all
/// partitions, when the value is fully qualified.
pub(crate) struct NodeRecorder {
    execution_plan: Arc<dyn ExecutionPlan>,
    span: Span,
}

impl NodeRecorder {
    pub fn new(execution_plan: Arc<dyn ExecutionPlan>, span: Span) -> Self {
        Self {
            execution_plan,
            span,
        }
    }
}

impl Drop for NodeRecorder {
    fn drop(&mut self) {
        self.span.record(
            "datafusion.node",
            field::display(DefaultDisplay(self.execution_plan.as_ref())),
        );
    }
}

/// A wrapper around a DataFusion stream that keeps a `NodeRecorder` alive
/// so the `datafusion.node` field is recorded only when the stream completes.
#[pin_project]
pub(crate) struct NodeRecordingStream {
    #[pin]
    inner: SendableRecordBatchStream,
    _recorder: Arc<NodeRecorder>,
}

impl NodeRecordingStream {
    pub fn new(inner: SendableRecordBatchStream, recorder: Arc<NodeRecorder>) -> Self {
        Self {
            inner,
            _recorder: recorder,
        }
    }
}

impl Stream for NodeRecordingStream {
    type Item = datafusion::common::Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.project().inner.poll_next(cx)
    }
}

impl RecordBatchStream for NodeRecordingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}
