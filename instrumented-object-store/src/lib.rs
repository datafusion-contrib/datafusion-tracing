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

//! # Instrumented Object Store
//!
//! Adds tracing instrumentation to any [Object Store](https://docs.rs/object_store/) implementation.
//!
//! # Features
//!
//! - Automatically captures spans for all storage operations (get, put, list, etc.)
//! - Records metadata like file paths and content sizes
//! - Captures error details when operations fail
//! - Works with OpenTelemetry for distributed tracing
//!
//! # Getting Started
//!
//! ```rust
//! # use object_store::{path::Path, ObjectStore};
//! # use std::sync::Arc;
//! # use instrumented_object_store::instrument_object_store;
//! # use datafusion::execution::context::SessionContext;
//! # use url::Url;
//! # use object_store::Result;
//!
//! # async fn example() -> Result<()> {
//! // Create your object store
//! let store = Arc::new(object_store::local::LocalFileSystem::new());
//!
//! // Wrap it with instrumentation (prefix for span names)
//! let instrumented_store = instrument_object_store(store, "local_fs");
//!
//! // Use directly for file operations
//! let result = instrumented_store.get(&Path::from("path/to/file")).await?;
//!
//! // Or integrate with DataFusion
//! let ctx = SessionContext::new();
//! ctx.register_object_store(&Url::parse("file://").unwrap(), instrumented_store);
//! # Ok(())
//! # }
//! ```
//!
//! When combined with the [`datafusion-tracing`](https://github.com/datafusion-contrib/datafusion-tracing/tree/main/datafusion-tracing)
//! crate, this provides end-to-end visibility from query execution to storage operations.

mod instrumented_object_store;

pub use instrumented_object_store::{instrument_object_store, InstrumentedObjectStore};
