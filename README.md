# DataFusion Tracing

[![Crates.io][crates-badge]][crates-url]
[![Apache licensed][license-badge]][license-url]
[![Build Status][actions-badge]][actions-url]
[![Documentation][docs-badge]][docs-url]

[crates-badge]: https://img.shields.io/crates/v/datafusion-tracing.svg
[crates-url]: https://crates.io/crates/datafusion-tracing
[license-badge]: https://img.shields.io/badge/license-Apache%20v2-blue.svg
[license-url]: https://github.com/datafusion-contrib/datafusion-tracing/blob/main/LICENSE
[actions-badge]: https://github.com/datafusion-contrib/datafusion-tracing/actions/workflows/ci.yml/badge.svg
[actions-url]: https://github.com/datafusion-contrib/datafusion-tracing/actions?query=branch%3Amain
[docs-badge]: https://docs.rs/datafusion-tracing/badge.svg
[docs-url]: https://docs.rs/datafusion-tracing


<img src="https://raw.githubusercontent.com/datafusion-contrib/datafusion-tracing/main/docs/logo/logo.png" width="512" alt="logo"/>

<!-- 
This section below is auto-generated from the `datafusion-tracing` crate documentation.
To regenerate it, run the following command from the repository root:
```bash
cargo install cargo-rdme
cargo rdme --readme-path README.md --workspace-project datafusion-tracing
```
-->
<!-- cargo-rdme start -->

DataFusion Tracing is an extension for [Apache DataFusion](https://datafusion.apache.org/) that helps you monitor and debug queries. It uses [`tracing`](https://docs.rs/tracing/latest/tracing/) and [OpenTelemetry](https://opentelemetry.io/) to gather DataFusion metrics, trace execution steps, and preview partial query results.

**Note:** This is not an official Apache Software Foundation release.

## Overview

When you run queries with DataFusion Tracing enabled, it automatically adds tracing around execution steps, records all native DataFusion metrics such as execution time and output row count, lets you preview partial results for easier debugging, and integrates with OpenTelemetry for distributed tracing. This makes it simpler to understand and improve query performance.

### See it in action

Here's what DataFusion Tracing can look like in practice:

<details>
<summary>Jaeger UI</summary>

![Jaeger UI screenshot](https://raw.githubusercontent.com/datafusion-contrib/datafusion-tracing/main/datafusion-tracing/docs/screenshots/jaeger.png)
</details>

<details>
<summary>DataDog UI</summary>

![DataDog UI screenshot](https://raw.githubusercontent.com/datafusion-contrib/datafusion-tracing/main/datafusion-tracing/docs/screenshots/datadog.png)
</details>

## Getting Started

### Installation

Include DataFusion Tracing in your project's `Cargo.toml`:

```toml
[dependencies]
datafusion = "48.0.0"
datafusion-tracing = "48.0.0"
```

### Quick Start Example

```rust
use datafusion::{
    arrow::{array::RecordBatch, util::pretty::pretty_format_batches},
    error::Result,
    execution::SessionStateBuilder,
    prelude::*,
};
use datafusion_tracing::{
    instrument_with_info_spans, pretty_format_compact_batch, InstrumentationOptions,
};
use std::sync::Arc;
use tracing::field;

#[tokio::main]
async fn main() -> Result<()> {
    // Initialize tracing subscriber as usual
    // (See examples/otlp.rs for a complete example).

    // Set up tracing options (you can customize these).
    let options = InstrumentationOptions::builder()
        .record_metrics(true)
        .preview_limit(5)
        .preview_fn(Arc::new(|batch: &RecordBatch| {
            pretty_format_compact_batch(batch, 64, 3, 10).map(|fmt| fmt.to_string())
        }))
        .add_custom_field("env", "production")
        .add_custom_field("region", "us-west")
        .build();

    let instrument_rule = instrument_with_info_spans!(
        options: options,
        env = field::Empty,
        region = field::Empty,
    );

    let session_state = SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rule(instrument_rule)
        .build();

    let ctx = SessionContext::new_with_state(session_state);

    let results = ctx.sql("SELECT 1").await?.collect().await?;
    println!(
        "Query Results:\n{}",
        pretty_format_batches(results.as_slice())?
    );

    Ok(())
}
```

A more complete example can be found in the [examples directory](https://github.com/datafusion-contrib/datafusion-tracing/tree/main/examples).

<!-- cargo-rdme end -->

## Setting Up a Collector

Before diving into DataFusion Tracing, you'll need to set up an OpenTelemetry collector to receive and process the tracing data. There are several options available:

### Jaeger (Local Development)

For local development and testing, Jaeger is a great choice. It's an open-source distributed tracing system that's easy to set up. You can run it with Docker using:

```bash
docker run --rm --name jaeger \
  -p 16686:16686 \
  -p 4317:4317 \
  -p 4318:4318 \
  -p 5778:5778 \
  -p 9411:9411 \
  jaegertracing/jaeger:2.7.0
```

Once running, you can access the Jaeger UI at http://localhost:16686. For more details, check out their [getting started guide](https://www.jaegertracing.io/docs/latest/getting-started/).

### DataDog (Cloud-Native)

For a cloud-native approach, DataDog offers a hosted solution for OpenTelemetry data. You can send your traces directly to their platform by configuring your DataDog API key and endpoint - their [OpenTelemetry integration guide](https://docs.datadoghq.com/opentelemetry/#send-opentelemetry-data-to-datadog) has all the details.

### Other Collectors

Of course, you can use any OpenTelemetry-compatible collector. The [official OpenTelemetry Collector](https://opentelemetry.io/docs/collector/) is a good starting point if you want to build a custom setup.

## Repository Structure

The repository is organized as follows:
- `datafusion-tracing/`: Core tracing functionality for DataFusion
- `instrumented-object-store/`: Object store instrumentation
- `integration-utils/`: Integration utilities and helpers for examples and tests (not for production use)
- `examples/`: Example applications demonstrating the library usage
- `tests/`: Integration tests
- `docs/`: Documentation, including logos and screenshots

## Building and Testing

Use these commands to build and test:

```bash
cargo build --workspace
cargo test --workspace
```

## Contributing

Contributions are welcome. Make sure your code passes all tests, follow existing formatting and coding styles, and include tests and documentation. See [CONTRIBUTING.md](https://github.com/datafusion-contrib/datafusion-tracing/blob/main/CONTRIBUTING.md) for detailed guidelines.

## License

Licensed under the [Apache License, Version 2.0](http://www.apache.org/licenses/LICENSE-2.0). See [LICENSE](https://github.com/datafusion-contrib/datafusion-tracing/blob/main/LICENSE).

## Acknowledgments

This project includes software developed at Datadog (<info@datadoghq.com>).
