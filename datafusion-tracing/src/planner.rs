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

use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::Result;
use datafusion::execution::context::QueryPlanner;
use datafusion::logical_expr::LogicalPlan;
use datafusion::physical_plan::{ExecutionPlan, displayable};
use std::sync::Arc;
use tracing::{Instrument, Level};

/// Creates a span at the specified tracing level with the given name and fields.
///
/// This macro eliminates the need for repetitive match blocks when creating spans
/// at different levels. The level must be a `tracing::Level` value.
macro_rules! span_at_level {
    ($level:expr, $name:expr, $($field:tt)*) => {
        match $level {
            Level::TRACE => tracing::trace_span!($name, $($field)*),
            Level::DEBUG => tracing::debug_span!($name, $($field)*),
            Level::INFO => tracing::info_span!($name, $($field)*),
            Level::WARN => tracing::warn_span!($name, $($field)*),
            Level::ERROR => tracing::error_span!($name, $($field)*),
        }
    };
}

/// A `QueryPlanner` that instruments the creation of the physical plan.
///
/// This is automatically applied when instrumenting a `SessionState` with physical
/// optimizer instrumentation enabled (PhaseOnly or Full).
#[derive(Debug)]
pub(crate) struct TracingQueryPlanner {
    inner: Arc<dyn QueryPlanner + Send + Sync>,
    level: Level,
}

impl TracingQueryPlanner {
    /// Wrap the session's query planner with tracing at the specified level.
    pub(crate) fn new(inner: Arc<dyn QueryPlanner + Send + Sync>, level: Level) -> Self {
        Self { inner, level }
    }
}

#[async_trait]
impl QueryPlanner for TracingQueryPlanner {
    async fn create_physical_plan(
        &self,
        logical_plan: &LogicalPlan,
        session_state: &dyn Session,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        let span = span_at_level!(
            self.level,
            "create_physical_plan",
            logical_plan = tracing::field::Empty,
            physical_plan = tracing::field::Empty,
            error = tracing::field::Empty
        );

        // Only perform expensive plan formatting if the span is enabled
        if !span.is_disabled() {
            let logical_plan_str = logical_plan.display_indent_schema().to_string();
            span.record("logical_plan", logical_plan_str.as_str());
        }

        let physical_plan = self
            .inner
            .create_physical_plan(logical_plan, session_state)
            .instrument(span.clone())
            .await;

        // Only perform expensive plan formatting if the span is enabled
        if !span.is_disabled() {
            match &physical_plan {
                Ok(plan) => {
                    let physical_plan_str =
                        displayable(plan.as_ref()).indent(true).to_string();
                    span.record("physical_plan", physical_plan_str.as_str());
                }
                Err(e) => {
                    span.record("error", e.to_string().as_str());
                }
            }
        }

        physical_plan
    }
}
