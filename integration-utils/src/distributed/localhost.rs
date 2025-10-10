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

use std::future::Future;
use std::net::{IpAddr, Ipv4Addr, SocketAddr};
use std::pin::Pin;

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use dashmap::{DashMap, Entry};
use datafusion::common::DataFusionError;
use datafusion::error::Result;
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedSessionBuilderContext,
};
use tokio::sync::oneshot;
use tonic::transport::{Channel, Server};
use tower::ServiceBuilder;
use url::Url;

use super::trace_middleware::{TracingClientLayer, TracingServerLayer};

/// Channel resolver for localhost workers that connects via TCP
#[derive(Clone)]
pub(crate) struct LocalhostChannelResolver {
    ports: Vec<u16>,
    cached: DashMap<Url, FlightServiceClient<BoxCloneSyncChannel>>,
}

impl LocalhostChannelResolver {
    pub(crate) fn new(ports: Vec<u16>) -> Self {
        Self {
            ports,
            cached: DashMap::new(),
        }
    }
}

#[async_trait]
impl ChannelResolver for LocalhostChannelResolver {
    fn get_urls(&self) -> Result<Vec<Url>, DataFusionError> {
        Ok(self
            .ports
            .iter()
            .map(|port| Url::parse(&format!("http://localhost:{port}")).unwrap())
            .collect())
    }

    async fn get_flight_client_for_url(
        &self,
        url: &Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        match self.cached.entry(url.clone()) {
            Entry::Occupied(v) => Ok(v.get().clone()),
            Entry::Vacant(v) => {
                let channel = Channel::from_shared(url.to_string())
                    .unwrap()
                    .connect_lazy();

                // Wrap the channel with tracing middleware to inject trace context
                let channel = ServiceBuilder::new()
                    .layer(TracingClientLayer)
                    .service(channel);

                let channel = FlightServiceClient::new(BoxCloneSyncChannel::new(channel));
                v.insert(channel.clone());
                Ok(channel)
            }
        }
    }
}

/// Configuration for worker instrumentation
#[derive(Debug, Clone, Copy)]
pub(crate) struct WorkerInstrumentationConfig {
    pub record_metrics: bool,
    pub preview_limit: usize,
    pub compact_preview: bool,
}

/// Spawns localhost workers on the specified ports
pub(crate) async fn spawn_localhost_workers(
    ports: &[u16],
    instrumentation_config: Option<WorkerInstrumentationConfig>,
) -> Result<()> {
    let mut ready_receivers = Vec::new();

    for &port in ports {
        let all_ports = ports.to_vec();
        let (ready_tx, ready_rx) = oneshot::channel();
        ready_receivers.push(ready_rx);

        // Don't create a worker span here - workers should only create spans when handling
        // requests, using the trace context extracted from the gRPC metadata. This ensures
        // worker spans are properly linked to the parent coordinator trace.
        tracing::info!("Spawning localhost worker on port {}", port);

        tokio::spawn(async move {
            let localhost_resolver = LocalhostChannelResolver::new(all_ports);

            let endpoint = ArrowFlightEndpoint::try_new(
                move |ctx: DistributedSessionBuilderContext| {
                    let local_host_resolver = localhost_resolver.clone();
                    async move {
                        let mut builder = SessionStateBuilder::new()
                            .with_runtime_env(ctx.runtime_env)
                            .with_distributed_channel_resolver(local_host_resolver)
                            .with_default_features();

                        // Build and add instrumentation rule if config is provided
                        if let Some(config) = instrumentation_config {
                            let instrumentation_rule = crate::session::create_instrumentation_rule_with_worker_id(
                                config.record_metrics,
                                config.preview_limit,
                                config.compact_preview,
                                Some(format!("localhost:{}", port)),
                            );
                            builder = builder.with_physical_optimizer_rule(instrumentation_rule);
                        }

                        Ok(builder.build())
                    }
                },
            )
            .expect("Failed to create ArrowFlightEndpoint");

            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

            // Wrap the FlightService with tracing middleware to extract trace context
            let service = ServiceBuilder::new()
                .layer(TracingServerLayer)
                .service(FlightServiceServer::new(endpoint));

            let server = Server::builder().add_service(service).serve(addr);

            // Notify that we're about to start serving (the server will bind immediately)
            let _ = ready_tx.send(());

            server
                .await
                .unwrap_or_else(|_| panic!("Failed to start worker on port {}", port));
        });
    }

    // Wait for all workers to signal they're ready
    for (i, ready_rx) in ready_receivers.into_iter().enumerate() {
        ready_rx.await.map_err(|_| {
            DataFusionError::Execution(format!(
                "Worker on port {} failed to start",
                ports[i]
            ))
        })?;
    }

    Ok(())
}

/// Spawns localhost workers with task-local buffer support for separate log buffers per worker
pub(crate) async fn spawn_localhost_workers_with_buffers<F, Fut>(
    ports: &[u16],
    instrumentation_config: Option<WorkerInstrumentationConfig>,
    task_local_wrapper: F,
) -> Result<()>
where
    F: Fn(u16, Pin<Box<dyn Future<Output = ()> + Send>>) -> Fut + Send + Sync + 'static,
    Fut: Future<Output = ()> + Send + 'static,
{
    let mut ready_receivers = Vec::new();

    for &port in ports {
        let all_ports = ports.to_vec();
        let (ready_tx, ready_rx) = oneshot::channel();
        ready_receivers.push(ready_rx);

        // Don't create a worker span here - workers should only create spans when handling
        // requests, using the trace context extracted from the gRPC metadata. This ensures
        // worker spans are properly linked to the parent coordinator trace.
        tracing::info!("Spawning localhost worker on port {}", port);

        // Clone the wrapper function for this task
        let wrapper = &task_local_wrapper;

        let worker_future = async move {
            let localhost_resolver = LocalhostChannelResolver::new(all_ports);

            let endpoint = ArrowFlightEndpoint::try_new(
                move |ctx: DistributedSessionBuilderContext| {
                    let local_host_resolver = localhost_resolver.clone();
                    async move {
                        let mut builder = SessionStateBuilder::new()
                            .with_runtime_env(ctx.runtime_env)
                            .with_distributed_channel_resolver(local_host_resolver)
                            .with_default_features();

                        // Build and add instrumentation rule if config is provided
                        if let Some(config) = instrumentation_config {
                            let instrumentation_rule = crate::session::create_instrumentation_rule_with_worker_id(
                                config.record_metrics,
                                config.preview_limit,
                                config.compact_preview,
                                Some(format!("localhost:{}", port)),
                            );
                            builder = builder.with_physical_optimizer_rule(instrumentation_rule);
                        }

                        Ok(builder.build())
                    }
                },
            )
            .expect("Failed to create ArrowFlightEndpoint");

            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);

            // Wrap the FlightService with tracing middleware to extract trace context
            let service = ServiceBuilder::new()
                .layer(TracingServerLayer)
                .service(FlightServiceServer::new(endpoint));

            let server = Server::builder().add_service(service).serve(addr);

            // Notify that we're about to start serving (the server will bind immediately)
            let _ = ready_tx.send(());

            server
                .await
                .unwrap_or_else(|_| panic!("Failed to start worker on port {}", port));
        };

        tokio::spawn(wrapper(port, Box::pin(worker_future)));
    }

    // Wait for all workers to signal they're ready
    for (i, ready_rx) in ready_receivers.into_iter().enumerate() {
        ready_rx.await.map_err(|_| {
            DataFusionError::Execution(format!(
                "Worker on port {} failed to start",
                ports[i]
            ))
        })?;
    }

    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use tokio::net::TcpStream;

    #[tokio::test]
    async fn test_spawn_and_connect_to_workers() {
        // Use high-numbered ports to avoid conflicts
        let ports = vec![50051, 50052, 50053];

        // Spawn workers (without instrumentation for this test)
        spawn_localhost_workers(&ports, None)
            .await
            .expect("Failed to spawn workers");

        // Verify each worker is accepting connections
        for &port in &ports {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
            let result = TcpStream::connect(addr).await;
            assert!(
                result.is_ok(),
                "Failed to connect to worker on port {}",
                port
            );
        }

        // Create a resolver and verify we can get URLs
        let resolver = LocalhostChannelResolver::new(ports.clone());
        let urls = resolver.get_urls().expect("Failed to get URLs");
        assert_eq!(urls.len(), ports.len());

        // Verify URLs are correctly formatted
        for (i, url) in urls.iter().enumerate() {
            assert_eq!(
                url.as_str(),
                format!("http://localhost:{}/", ports[i]),
                "URL format mismatch for port {}",
                ports[i]
            );
        }

        // Create flight clients for each URL
        for url in &urls {
            let _client = resolver
                .get_flight_client_for_url(url)
                .await
                .expect("Failed to create flight client");
            // Verify client was cached
            assert!(resolver.cached.contains_key(url));

            // Get the same client again and verify it's cached
            let _client2 = resolver
                .get_flight_client_for_url(url)
                .await
                .expect("Failed to get cached flight client");
            // Both clients should be the same (from cache)
            assert_eq!(
                resolver.cached.len(),
                urls.iter().position(|u| u == url).unwrap() + 1
            );
        }
    }

    #[tokio::test]
    async fn test_worker_readiness_signal() {
        // Use a different set of ports to avoid conflicts with other tests
        let ports = vec![50061, 50062];

        // Measure time to spawn and become ready
        let start = std::time::Instant::now();
        spawn_localhost_workers(&ports, None)
            .await
            .expect("Failed to spawn workers");
        let elapsed = start.elapsed();

        // Workers should be ready very quickly (well under 1 second)
        // This verifies we're not sleeping unnecessarily
        assert!(
            elapsed.as_millis() < 1000,
            "Workers took too long to become ready: {:?}",
            elapsed
        );

        // Verify workers are actually running
        for &port in &ports {
            let addr = SocketAddr::new(IpAddr::V4(Ipv4Addr::LOCALHOST), port);
            TcpStream::connect(addr).await.expect(&format!(
                "Worker on port {} is not accepting connections",
                port
            ));
        }
    }

    #[tokio::test]
    async fn test_empty_ports_list() {
        // Spawning with no ports should succeed (no-op)
        let result = spawn_localhost_workers(&[], None).await;
        assert!(result.is_ok());
    }
}
