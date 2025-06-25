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

use std::{
    io::{Result as IoResult, Write},
    sync::{Arc, Mutex, MutexGuard},
};
use tracing_subscriber::fmt::MakeWriter;

/// A type implementing `MakeWriter` for an `Arc<Mutex<Vec<u8>>>`.
///
/// When asked to produce a writer, it locks the `Arc<Mutex<Vec<u8>>>` and
/// returns a guard that implements `io::Write`.
#[derive(Clone)]
pub struct InMemoryMakeWriter(Arc<Mutex<Vec<u8>>>);

impl InMemoryMakeWriter {
    pub fn new(inner: Arc<Mutex<Vec<u8>>>) -> Self {
        Self(inner)
    }
}

impl<'a> MakeWriter<'a> for InMemoryMakeWriter {
    // We'll return our own guard type that implements `Write`.
    type Writer = InMemoryWriter<'a>;

    fn make_writer(&'a self) -> Self::Writer {
        // Lock the mutex and wrap the guard in `InMemoryWriter`.
        InMemoryWriter {
            guard: self.0.lock().unwrap(),
        }
    }
}

/// Our guard type that implements `io::Write` by forwarding to the locked buffer.
pub struct InMemoryWriter<'a> {
    guard: MutexGuard<'a, Vec<u8>>,
}

impl Write for InMemoryWriter<'_> {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        self.guard.write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        self.guard.flush()
    }
}
