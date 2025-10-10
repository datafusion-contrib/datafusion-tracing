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

//! Distributed execution utilities
//!
//! This module provides channel resolvers and utilities for distributed query execution:
//! - `in_memory`: In-memory channel resolver for fast testing
//! - `localhost`: TCP-based localhost worker resolver for more realistic testing

mod in_memory;
mod localhost;

pub(crate) use in_memory::InMemoryChannelResolver;
pub(crate) use localhost::{LocalhostChannelResolver, spawn_localhost_workers};

/// Default ports to use for localhost workers in distributed mode
pub(crate) const DEFAULT_WORKER_PORTS: &[u16] = &[50051, 50052, 50053, 50054];

/// Distributed execution mode configuration
#[derive(Debug, Clone, Copy)]
pub enum DistributedMode {
    /// Use in-memory channels (faster, for testing)
    Memory,
    /// Use localhost TCP workers (more realistic)
    Localhost,
}
