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
    collections::HashMap,
    io::{Result as IoResult, Write},
    sync::{Arc, Mutex},
};
use tracing_subscriber::fmt::MakeWriter;

tokio::task_local! {
    /// Task-local storage for the worker port, used to route logs to the correct buffer
    pub static WORKER_PORT: u16;
}

/// A type implementing `MakeWriter` that routes logs to different buffers based on worker port.
///
/// The main buffer receives logs from the coordinator/main thread.
/// Worker buffers receive logs from worker tasks based on task-local storage.
#[derive(Clone)]
pub struct MultiBufferMakeWriter {
    /// Main buffer for coordinator logs
    main_buffer: Arc<Mutex<Vec<u8>>>,
    /// Map of worker port -> buffer for worker logs
    worker_buffers: Arc<Mutex<HashMap<u16, Arc<Mutex<Vec<u8>>>>>>,
}

impl MultiBufferMakeWriter {
    pub fn new(
        main_buffer: Arc<Mutex<Vec<u8>>>,
        worker_buffers: Arc<Mutex<HashMap<u16, Arc<Mutex<Vec<u8>>>>>>,
    ) -> Self {
        Self {
            main_buffer,
            worker_buffers,
        }
    }

    /// Get all buffers (main + workers) for concatenation
    pub fn collect_all_buffers(&self) -> Vec<u8> {
        let mut result = Vec::new();

        // First, collect main buffer
        {
            let main = self.main_buffer.lock().unwrap();
            result.extend_from_slice(&main);
        }

        // Then, collect worker buffers in port order
        {
            let worker_buffers = self.worker_buffers.lock().unwrap();
            let mut ports: Vec<_> = worker_buffers.keys().copied().collect();
            ports.sort();

            for port in ports {
                if let Some(buffer) = worker_buffers.get(&port) {
                    let buffer = buffer.lock().unwrap();
                    result.extend_from_slice(&buffer);
                }
            }
        }

        result
    }
}

impl<'a> MakeWriter<'a> for MultiBufferMakeWriter {
    type Writer = MultiBufferWriter;

    fn make_writer(&'a self) -> Self::Writer {
        // Try to get the worker port from task-local storage
        let worker_port = WORKER_PORT.try_with(|&port| port).ok();

        // Select the appropriate buffer
        let buffer = if let Some(port) = worker_port {
            let worker_buffers = self.worker_buffers.lock().unwrap();
            if let Some(worker_buffer) = worker_buffers.get(&port) {
                Arc::clone(worker_buffer)
            } else {
                Arc::clone(&self.main_buffer)
            }
        } else {
            Arc::clone(&self.main_buffer)
        };

        MultiBufferWriter { buffer }
    }
}

/// Guard type that implements `io::Write` by forwarding to the selected buffer.
pub struct MultiBufferWriter {
    buffer: Arc<Mutex<Vec<u8>>>,
}

impl Write for MultiBufferWriter {
    fn write(&mut self, buf: &[u8]) -> IoResult<usize> {
        let mut guard = self.buffer.lock().unwrap();
        guard.write(buf)
    }

    fn flush(&mut self) -> IoResult<()> {
        let mut guard = self.buffer.lock().unwrap();
        guard.flush()
    }
}
