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

use datafusion::physical_plan::{DisplayFormatType, ExecutionPlan};
use std::cell::Cell;
use std::fmt;

thread_local! {
    /// Flag to indicate that we are performing an internal check to identify
    /// instrumentation nodes. This allows `InstrumentedExec` to reveal its
    /// type during optimization passes while remaining transparent
    /// to users.
    static IS_INTERNAL_OPTIMIZER_CHECK: Cell<bool> = const { Cell::new(false) };
}

pub(crate) fn is_internal_optimizer_check() -> bool {
    IS_INTERNAL_OPTIMIZER_CHECK.with(|c| c.get())
}

/// RAII guard to safely set and reset the internal optimizer check flag.
pub(crate) struct InternalOptimizerGuard(bool);

impl InternalOptimizerGuard {
    pub fn new() -> Self {
        let prev = IS_INTERNAL_OPTIMIZER_CHECK.with(|c| {
            let prev = c.get();
            c.set(true);
            prev
        });
        Self(prev)
    }
}

impl Drop for InternalOptimizerGuard {
    fn drop(&mut self) {
        IS_INTERNAL_OPTIMIZER_CHECK.with(|c| c.set(self.0));
    }
}

/// Helper struct for default display formatting of an `ExecutionPlan`.
pub(crate) struct DefaultDisplay<'a>(pub &'a dyn ExecutionPlan);

impl fmt::Display for DefaultDisplay<'_> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        self.0.fmt_as(DisplayFormatType::Default, f)
    }
}
