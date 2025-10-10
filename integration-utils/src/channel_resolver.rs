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

use arrow_flight::flight_service_client::FlightServiceClient;
use arrow_flight::flight_service_server::FlightServiceServer;
use async_trait::async_trait;
use datafusion::common::DataFusionError;
use datafusion::execution::SessionStateBuilder;
use datafusion_distributed::{
    ArrowFlightEndpoint, BoxCloneSyncChannel, ChannelResolver, DistributedExt,
    DistributedSessionBuilderContext,
};
use hyper_util::rt::TokioIo;
use tonic::transport::{Endpoint, Server};

const DUMMY_URL: &str = "http://localhost:50051";
const MAX_MESSAGE_SIZE: usize = 2 * 1024 * 1024 * 1024; // 2GB

/// [ChannelResolver] implementation that returns gRPC clients baked by an in-memory
/// tokio duplex rather than a TCP connection.
#[derive(Clone)]
pub(crate) struct InMemoryChannelResolver {
    channel: FlightServiceClient<BoxCloneSyncChannel>,
}

impl InMemoryChannelResolver {
    pub fn new() -> Self {
        let (client, server) = tokio::io::duplex(1024 * 1024); // 1MB buffer

        let mut client = Some(client);
        let channel = Endpoint::try_from(DUMMY_URL)
            .expect(
                "Invalid dummy URL for building an endpoint. This should never happen",
            )
            .connect_with_connector_lazy(tower::service_fn(move |_| {
                let client = client
                    .take()
                    .expect("Client taken twice. This should never happen");
                async move { Ok::<_, std::io::Error>(TokioIo::new(client)) }
            }));

        let this = Self {
            channel: FlightServiceClient::new(BoxCloneSyncChannel::new(channel))
                .max_decoding_message_size(MAX_MESSAGE_SIZE)
                .max_encoding_message_size(MAX_MESSAGE_SIZE),
        };
        let this_clone = this.clone();

        let endpoint =
            ArrowFlightEndpoint::try_new(move |ctx: DistributedSessionBuilderContext| {
                let this = this.clone();
                async move {
                    let builder = SessionStateBuilder::new()
                        .with_default_features()
                        .with_distributed_channel_resolver(this)
                        .with_runtime_env(ctx.runtime_env.clone());
                    Ok(builder.build())
                }
            })
            .unwrap();

        tokio::spawn(async move {
            Server::builder()
                .add_service(
                    FlightServiceServer::new(endpoint)
                        .max_decoding_message_size(MAX_MESSAGE_SIZE)
                        .max_encoding_message_size(MAX_MESSAGE_SIZE),
                )
                .serve_with_incoming(tokio_stream::once(Ok::<_, std::io::Error>(server)))
                .await
        });

        this_clone
    }
}

#[async_trait]
impl ChannelResolver for InMemoryChannelResolver {
    fn get_urls(&self) -> Result<Vec<url::Url>, DataFusionError> {
        Ok(vec![url::Url::parse(DUMMY_URL).unwrap()])
    }

    async fn get_flight_client_for_url(
        &self,
        _: &url::Url,
    ) -> Result<FlightServiceClient<BoxCloneSyncChannel>, DataFusionError> {
        Ok(self.channel.clone())
    }
}
