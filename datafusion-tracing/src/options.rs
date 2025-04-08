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

use crate::preview::PreviewFn;
use std::collections::HashMap;
use std::sync::Arc;

/// Configuration options for instrumented execution plans.
#[derive(Clone, Default)]
pub struct InstrumentationOptions {
    /// Whether to record metrics during execution.
    pub record_metrics: bool,

    /// Maximum number of rows to preview per span.
    ///
    /// If set to `0`, batch preview recording will be disabled.
    pub preview_limit: usize,

    /// Optional callback function for formatting previewed record batches.
    ///
    /// The provided function will be invoked for each previewed batch of at most `preview_limit` rows.
    pub preview_fn: Option<Arc<PreviewFn>>,

    /// User-defined custom fields for extensible configuration.
    ///
    /// This can be used to store arbitrary key-value pairs relevant to instrumentation or metadata.
    pub custom_fields: HashMap<String, String>,
}

impl InstrumentationOptions {
    /// Creates a new builder for `InstrumentationOptions`.
    pub fn builder() -> InstrumentationOptionsBuilder {
        InstrumentationOptionsBuilder::default()
    }
}

/// The builder for `InstrumentationOptions`.
#[derive(Default)]
pub struct InstrumentationOptionsBuilder {
    record_metrics: bool,
    preview_limit: usize,
    preview_fn: Option<Arc<PreviewFn>>,
    custom_fields: HashMap<String, String>,
}

impl InstrumentationOptionsBuilder {
    /// Sets whether to record metrics during execution.
    pub fn record_metrics(mut self, record: bool) -> Self {
        self.record_metrics = record;
        self
    }

    /// Sets the maximum number of rows to preview per span.
    /// Setting this to `0` disables the batch preview.
    pub fn preview_limit(mut self, limit: usize) -> Self {
        self.preview_limit = limit;
        self
    }

    /// Sets the optional callback function for formatting previewed record batches.
    pub fn preview_fn(mut self, func: Arc<PreviewFn>) -> Self {
        self.preview_fn = Some(func);
        self
    }

    /// Adds a single custom field.
    pub fn add_custom_field<K: Into<String>, V: Into<String>>(
        mut self,
        key: K,
        value: V,
    ) -> Self {
        self.custom_fields.insert(key.into(), value.into());
        self
    }

    /// Replaces all custom fields with the provided `HashMap`.
    pub fn custom_fields(mut self, fields: HashMap<String, String>) -> Self {
        self.custom_fields = fields;
        self
    }

    /// Consumes the builder and creates an `InstrumentationOptions` instance.
    pub fn build(self) -> InstrumentationOptions {
        InstrumentationOptions {
            record_metrics: self.record_metrics,
            preview_limit: self.preview_limit,
            preview_fn: self.preview_fn,
            custom_fields: self.custom_fields,
        }
    }
}
