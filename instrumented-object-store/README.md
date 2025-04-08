# Instrumented Object Store

<!-- 
This section below is auto-generated from the `instrumented-object-store` crate documentation.
To regenerate it, run the following command from the repository root:
```bash
cargo install cargo-rdme
cargo rdme --readme-path instrumented-object-store/README.md --workspace-project instrumented-object-store
```
-->
<!-- cargo-rdme start -->

## Instrumented Object Store

Adds tracing instrumentation to any [Object Store](https://docs.rs/object_store/) implementation.

## Features

- Automatically captures spans for all storage operations (get, put, list, etc.)
- Records metadata like file paths and content sizes
- Captures error details when operations fail
- Works with OpenTelemetry for distributed tracing

## Getting Started

```rust

// Create your object store
let store = Arc::new(object_store::local::LocalFileSystem::new());

// Wrap it with instrumentation (prefix for span names)
let instrumented_store = instrument_object_store(store, "local_fs");

// Use directly for file operations
let result = instrumented_store.get(&Path::from("path/to/file")).await?;

// Or integrate with DataFusion
let ctx = SessionContext::new();
ctx.register_object_store(&Url::parse("file://").unwrap(), instrumented_store);
```

When combined with the [`datafusion-tracing`](https://github.com/datafusion-contrib/datafusion-tracing/tree/main/datafusion-tracing)
crate, this provides end-to-end visibility from query execution to storage operations.

<!-- cargo-rdme end -->
