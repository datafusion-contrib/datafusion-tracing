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

//! Tower middleware layers for OpenTelemetry trace context propagation.
//!
//! This module provides middleware that automatically handles trace context propagation
//! across gRPC boundaries using the W3C Trace Context format.
//!
//! - `TracingClientLayer`: Injects trace context into outgoing gRPC requests
//! - `TracingServerLayer`: Extracts trace context from incoming gRPC requests and creates spans

use opentelemetry::propagation::{Extractor, Injector};
use pin_project_lite::pin_project;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use tower::{Layer, Service};
use tracing::Instrument;
use tracing_opentelemetry::OpenTelemetrySpanExt;

// ============================================================================
// Client-side middleware (injects trace context into outgoing requests)
// ============================================================================

/// Tower layer that injects OpenTelemetry trace context into outgoing gRPC requests.
///
/// This middleware reads the current tracing span's OpenTelemetry context and injects
/// it into the HTTP headers of outgoing requests using the configured global propagator
/// (typically W3C Trace Context format).
///
/// # Example
///
/// ```ignore
/// use tower::ServiceBuilder;
/// use tonic::transport::Channel;
///
/// let channel = Channel::from_static("http://localhost:50051")
///     .connect_lazy();
///
/// let channel = ServiceBuilder::new()
///     .layer(TracingClientLayer)
///     .service(channel);
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TracingClientLayer;

impl<S> Layer<S> for TracingClientLayer {
    type Service = TracingClient<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingClient { inner }
    }
}

/// Service wrapper that injects trace context into requests.
#[derive(Debug, Clone)]
pub struct TracingClient<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for TracingClient<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = S::Future;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, mut request: http::Request<ReqBody>) -> Self::Future {
        // Get the current OpenTelemetry context from the tracing span
        let context = tracing::Span::current().context();

        // Inject the context into HTTP headers
        let headers = request.headers_mut();
        let mut injector = HttpHeaderInjector(headers);
        opentelemetry::global::get_text_map_propagator(|propagator| {
            propagator.inject_context(&context, &mut injector);
        });

        self.inner.call(request)
    }
}

/// Helper struct for injecting trace context into HTTP headers.
struct HttpHeaderInjector<'a>(&'a mut http::HeaderMap);

impl Injector for HttpHeaderInjector<'_> {
    fn set(&mut self, key: &str, value: String) {
        if let Ok(name) = http::HeaderName::try_from(key) {
            if let Ok(val) = http::HeaderValue::try_from(value) {
                self.0.insert(name, val);
            }
        }
    }
}

// ============================================================================
// Server-side middleware (extracts trace context and creates spans)
// ============================================================================

/// Tower layer that extracts OpenTelemetry trace context from incoming gRPC requests
/// and creates instrumented spans.
///
/// This middleware reads W3C Trace Context headers from incoming requests, extracts
/// the parent context, and creates a new span that is properly linked to the parent trace.
/// The span remains active for the duration of the request processing.
///
/// # Example
///
/// ```ignore
/// use tower::ServiceBuilder;
/// use arrow_flight::flight_service_server::FlightServiceServer;
///
/// let service = FlightServiceServer::new(endpoint);
///
/// let service = ServiceBuilder::new()
///     .layer(TracingServerLayer)
///     .service(service);
///
/// Server::builder()
///     .add_service(service)
///     .serve(addr)
///     .await?;
/// ```
#[derive(Debug, Clone, Copy)]
pub struct TracingServerLayer;

impl<S> Layer<S> for TracingServerLayer {
    type Service = TracingServer<S>;

    fn layer(&self, inner: S) -> Self::Service {
        TracingServer { inner }
    }
}

/// Service wrapper that extracts trace context and creates spans.
#[derive(Debug, Clone)]
pub struct TracingServer<S> {
    inner: S,
}

impl<S, ReqBody, ResBody> Service<http::Request<ReqBody>> for TracingServer<S>
where
    S: Service<http::Request<ReqBody>, Response = http::Response<ResBody>>,
    S::Future: Send + 'static,
{
    type Response = S::Response;
    type Error = S::Error;
    type Future = TracingServerFuture<S::Future>;

    fn poll_ready(&mut self, cx: &mut Context<'_>) -> Poll<Result<(), Self::Error>> {
        self.inner.poll_ready(cx)
    }

    fn call(&mut self, request: http::Request<ReqBody>) -> Self::Future {
        // Extract trace context from HTTP headers
        let extractor = HttpHeaderExtractor(request.headers());
        let parent_context =
            opentelemetry::global::get_text_map_propagator(|propagator| {
                propagator.extract(&extractor)
            });

        // Parse the gRPC path to extract useful span information
        let path = request.uri().path();
        let method_name = path.rsplit('/').next().unwrap_or("unknown");

        // Attach the parent context and create a span for this request.
        // This must be INFO level or above to be captured by the OpenTelemetry layer.
        let span = {
            let _guard = parent_context.attach();
            tracing::info_span!(
                "grpc_request",
                otel.kind = "server",
                rpc.system = "grpc",
                rpc.service = "arrow.flight.protocol.FlightService",
                rpc.method = method_name,
            )
        };

        let inner_future = self.inner.call(request);
        TracingServerFuture {
            inner: inner_future.instrument(span),
        }
    }
}

pin_project! {
    /// Future wrapper that instruments the request processing with a tracing span.
    pub struct TracingServerFuture<F> {
        #[pin]
        inner: tracing::instrument::Instrumented<F>,
    }
}

impl<F, T, E> Future for TracingServerFuture<F>
where
    F: Future<Output = Result<T, E>>,
{
    type Output = Result<T, E>;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.project();
        this.inner.poll(cx)
    }
}

/// Helper struct for extracting trace context from HTTP headers.
struct HttpHeaderExtractor<'a>(&'a http::HeaderMap);

impl Extractor for HttpHeaderExtractor<'_> {
    fn get(&self, key: &str) -> Option<&str> {
        self.0.get(key).and_then(|v| v.to_str().ok())
    }

    fn keys(&self) -> Vec<&str> {
        self.0.keys().map(|k| k.as_str()).collect()
    }
}

// Implement tonic's NamedService for TracingServer to allow it to be used with add_service
impl<S> tonic::server::NamedService for TracingServer<S>
where
    S: tonic::server::NamedService,
{
    const NAME: &'static str = S::NAME;
}

#[cfg(test)]
mod tests {
    use super::*;
    use tower::ServiceExt;

    #[tokio::test]
    async fn test_client_middleware_doesnt_panic() {
        // Create a simple mock service
        let service = tower::service_fn(|req: http::Request<String>| async move {
            Ok::<_, std::convert::Infallible>(http::Response::new(req.into_body()))
        });

        // Wrap with tracing middleware
        let mut service = TracingClientLayer.layer(service);

        // Make a request
        let request = http::Request::builder()
            .uri("http://test.com")
            .body("test".to_string())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await;
        assert!(response.is_ok());
    }

    #[tokio::test]
    async fn test_server_middleware_doesnt_panic() {
        // Create a simple mock service
        let service = tower::service_fn(|req: http::Request<String>| async move {
            Ok::<_, std::convert::Infallible>(http::Response::new(req.into_body()))
        });

        // Wrap with tracing middleware
        let mut service = TracingServerLayer.layer(service);

        // Make a request
        let request = http::Request::builder()
            .uri("http://test.com/arrow.flight.protocol.FlightService/DoGet")
            .body("test".to_string())
            .unwrap();

        let response = service.ready().await.unwrap().call(request).await;
        assert!(response.is_ok());
    }
}
