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

use crate::instrumented::InstrumentedExec;
use crate::instrumented::SpanCreateFn;
use crate::options::InstrumentationOptions;
use datafusion::common::runtime::{JoinSetTracer, set_join_set_tracer};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::{
    config::ConfigOptions, physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use std::any::Any;
use std::fmt::Debug;
use std::sync::Arc;
use std::sync::Once;
use tracing::Span;
use tracing_futures::Instrument;

pub fn new_instrument_rule(
    span_create_fn: Arc<SpanCreateFn>,
    options: InstrumentationOptions,
) -> Arc<dyn PhysicalOptimizerRule + Send + Sync> {
    INIT.call_once(|| {
        // Register the span tracer in DataFusion to ensure spawned tasks inherit the current tracing context
        set_join_set_tracer(&SpanTracer).unwrap_or_else(|e| {
            tracing::warn!("set_join_set_tracer failed to set join_set_tracer: {}", e);
        })
    });
    Arc::new(InstrumentRule {
        span_create_fn,
        options,
    })
}

struct InstrumentRule {
    span_create_fn: Arc<SpanCreateFn>,
    options: InstrumentationOptions,
}

impl Debug for InstrumentRule {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct(self.name()).finish()
    }
}

impl PhysicalOptimizerRule for InstrumentRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Wrap each node in the plan tree with InstrumentedExec.
        // Since InstrumentedExec delegates as_any() to its inner node, it's transparent
        // for downcasting - other code can still identify wrapped nodes by their actual type.
        //
        // Note: This will wrap every node, even if the rule is called multiple times on
        // the same plan (unusual but possible). The resulting double-wrapping creates
        // duplicate telemetry but remains functionally correct.
        plan.transform(|plan| {
            Ok(Transformed::yes(Arc::new(InstrumentedExec::new(
                plan,
                self.span_create_fn.clone(),
                &self.options,
            ))))
        })
        .data()
    }

    fn name(&self) -> &str {
        "Instrument"
    }

    fn schema_check(&self) -> bool {
        false
    }
}

/// A simple tracer that ensures any spawned task or blocking closure
/// inherits the current span via `in_current_span`.
struct SpanTracer;

/// Implement the `JoinSetTracer` trait so we can inject instrumentation
/// for both async futures and blocking closures.
impl JoinSetTracer for SpanTracer {
    /// Instruments a boxed future to run in the current span. The future's
    /// return type is erased to `BoxedAny`, which we simply
    /// run inside the `Span::current()` context.
    fn trace_future(&self, fut: BoxedFuture) -> BoxedFuture {
        fut.in_current_span().boxed()
    }

    /// Instruments a boxed blocking closure by running it inside the
    /// `Span::current()` context.
    fn trace_block(&self, f: BoxedClosure) -> BoxedClosure {
        Box::new(move || Span::current().in_scope(f))
    }
}

type BoxedAny = Box<dyn Any + Send>;
type BoxedFuture = BoxFuture<'static, BoxedAny>;
type BoxedClosure = Box<dyn FnOnce() -> BoxedAny + Send>;

static INIT: Once = Once::new();
