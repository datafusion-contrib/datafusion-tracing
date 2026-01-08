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

use crate::instrumented_exec::{InstrumentedExec, SpanCreateFn};
use crate::options::InstrumentationOptions;
use crate::utils::InternalOptimizerGuard;
use datafusion::common::runtime::{JoinSetTracer, set_join_set_tracer};
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::{
    config::ConfigOptions, physical_optimizer::PhysicalOptimizerRule,
    physical_plan::ExecutionPlan,
};
use futures::FutureExt;
use futures::future::BoxFuture;
use std::any::Any;
use std::fmt::{self, Debug};
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
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct(self.name()).finish()
    }
}

impl PhysicalOptimizerRule for InstrumentRule {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> datafusion::error::Result<Arc<dyn ExecutionPlan>> {
        // Activate the internal optimization context for the duration of this pass.
        // This allows InstrumentedExec to reveal its type via as_any().
        //
        // This guard is safe because PhysicalOptimizerRule::optimize is synchronous
        // and won't be suspended or moved across threads during execution.
        let _guard = InternalOptimizerGuard::new();

        // Iterate over the plan using transform_down to ensure all nodes are instrumented,
        // including any new nodes added by other optimizer rules.
        plan.transform_down(|plan| {
            if InstrumentedExec::is_instrumented(plan.as_ref()) {
                // If the node is already instrumented, we don't want to wrap it again.
                // We continue to its children to ensure they are also instrumented.
                return Ok(Transformed::no(plan));
            }

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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::options::InstrumentationOptions;
    use datafusion::arrow::datatypes::{DataType, Field, Schema};
    use datafusion::physical_plan::placeholder_row::PlaceholderRowExec;
    use std::sync::Arc;
    use tracing::Span;

    #[test]
    fn test_skip_already_instrumented() -> datafusion::error::Result<()> {
        let schema = Arc::new(Schema::new(vec![Field::new("a", DataType::Int64, false)]));
        let plan = Arc::new(PlaceholderRowExec::new(schema));

        let span_create_fn: Arc<SpanCreateFn> = Arc::new(Span::none);
        let options = InstrumentationOptions::default();
        let rule = new_instrument_rule(span_create_fn, options);

        // First optimization pass
        let optimized_once = rule.optimize(plan, &ConfigOptions::default())?;

        // Second optimization pass
        let optimized_twice =
            rule.optimize(optimized_once.clone(), &ConfigOptions::default())?;

        // Verify it is still instrumented but NOT wrapped twice.
        // We check this by ensuring optimized_twice is exactly the same Arc as optimized_once
        // because transform_down returns Transformed::no(plan) when it skips.
        assert!(
            Arc::ptr_eq(&optimized_once, &optimized_twice),
            "Plan should not be wrapped twice; it should be the same Arc as the first pass"
        );

        Ok(())
    }
}
