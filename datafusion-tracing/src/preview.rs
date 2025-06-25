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

use datafusion::arrow::error::ArrowError;
use datafusion::arrow::util::pretty::pretty_format_batches;
use datafusion::arrow::{
    compute::concat_batches, datatypes::SchemaRef, record_batch::RecordBatch,
};
use datafusion::common::Result as DataFusionResult;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream};
use futures::Stream;
use pin_project::{pin_project, pinned_drop};
use std::{
    pin::Pin,
    sync::{Arc, OnceLock},
    task::{Context, Poll},
};
use tracing::Span;

pub type PreviewFn = dyn Fn(&RecordBatch) -> Result<String, ArrowError> + Send + Sync;

/// Stores up to the first `limit` rows for preview.
pub(crate) struct PreviewRecorder {
    span: Span,
    limit: usize,
    partition_previews: Vec<OnceLock<RecordBatch>>,
    preview_fn: Arc<PreviewFn>,
}

impl Drop for PreviewRecorder {
    fn drop(&mut self) {
        // Collect each partition's preview batch.
        let preview_batches: Vec<_> = self
            .partition_previews
            .iter()
            .filter_map(|cell| cell.get())
            .collect();

        // Concatenate all preview batches.
        if preview_batches.is_empty() {
            return;
        }
        let preview_schema = preview_batches[0].schema();
        if let Ok(concat) = concat_batches(&preview_schema, preview_batches) {
            let num_rows = concat.num_rows().min(self.limit);
            let sliced = concat.slice(0, num_rows);

            // Format and record the preview.
            match self.preview_fn.as_ref()(&sliced) {
                Ok(preview_str) => {
                    // Record the preview string in the span.
                    self.span
                        .record("datafusion.preview", preview_str.to_string());
                }
                Err(e) => {
                    // Warn that formatting failed
                    tracing::warn!("Failed to format preview: {}", e);
                }
            }
        }
    }
}

#[pin_project(PinnedDrop)]
pub(crate) struct PreviewRecordingStream {
    #[pin]
    inner: SendableRecordBatchStream,
    partition: usize,
    stored_rows: usize,
    limit: usize,
    preview_batch: Option<RecordBatch>,
    preview_recorder: Arc<PreviewRecorder>,
}

impl PreviewRecordingStream {
    pub fn new(
        inner: SendableRecordBatchStream,
        preview_recorder: Arc<PreviewRecorder>,
        partition: usize,
    ) -> Self {
        Self {
            inner,
            partition,
            stored_rows: 0,
            limit: preview_recorder.limit,
            preview_batch: None,
            preview_recorder,
        }
    }
}

impl Stream for PreviewRecordingStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let mut this = self.project();

        match this.inner.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                // If there's capacity left in the preview, store up to `limit` rows.
                if *this.stored_rows < *this.limit {
                    let needed = *this.limit - *this.stored_rows;
                    let to_store = needed.min(batch.num_rows());

                    if to_store > 0 {
                        let batch_slice = batch.slice(0, to_store);
                        if let Some(preview_batch) = &this.preview_batch {
                            this.preview_batch.replace(concat_batches(
                                &preview_batch.schema(),
                                [preview_batch, &batch_slice],
                            )?);
                        } else {
                            this.preview_batch.replace(batch_slice);
                        }
                        *this.stored_rows += to_store;
                    }
                }
                Poll::Ready(Some(Ok(batch)))
            }
            // Forward errors and EOF as-is.
            other => other,
        }
    }
}

#[pinned_drop]
impl PinnedDrop for PreviewRecordingStream {
    fn drop(self: Pin<&mut Self>) {
        let this = self.project();
        if let Some(preview_batch) = this.preview_batch.take() {
            // Store the preview batch in the recorder (set only once).
            this.preview_recorder.partition_previews[*this.partition]
                .set(preview_batch)
                .unwrap_or_else(|e| {
                    tracing::warn!(
                        "Failed to set preview batch for partition {}: {:?}",
                        this.partition,
                        e
                    )
                });
        }
    }
}

impl RecordBatchStream for PreviewRecordingStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }
}

fn default_preview_fn(batch: &RecordBatch) -> Result<String, ArrowError> {
    pretty_format_batches(&[batch.clone()]).map(|b| b.to_string())
}

impl PreviewRecorder {
    /// Returns a new builder for creating a `PreviewRecorder`.
    pub fn builder(span: Span, partition_count: usize) -> PreviewRecorderBuilder {
        PreviewRecorderBuilder {
            span,
            partition_count,
            limit: None,
            preview_fn: Arc::new(default_preview_fn),
        }
    }
}
/// Builder for [`PreviewRecorder`].
pub struct PreviewRecorderBuilder {
    span: Span,
    partition_count: usize,
    limit: Option<usize>,
    preview_fn: Arc<PreviewFn>,
}

impl PreviewRecorderBuilder {
    pub fn limit(mut self, limit: usize) -> Self {
        self.limit = Some(limit);
        self
    }

    pub fn preview_fn(mut self, preview_fn: Option<Arc<PreviewFn>>) -> Self {
        if let Some(preview_fn) = preview_fn {
            self.preview_fn = preview_fn;
        } else {
            self.preview_fn = Arc::new(default_preview_fn);
        }
        self
    }

    /// Build the `PreviewRecorder`, returning a `DataFusionResult`
    /// which is `Ok` if all required fields were set or an `Err` if any are missing.
    pub fn build(self) -> PreviewRecorder {
        PreviewRecorder {
            span: self.span,
            limit: self.limit.unwrap_or_default(),
            partition_previews: (0..self.partition_count)
                .map(|_| OnceLock::new())
                .collect(),
            preview_fn: Arc::clone(&self.preview_fn),
        }
    }
}
