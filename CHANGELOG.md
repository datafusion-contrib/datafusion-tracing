# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

Version numbers follow Apache DataFusion releases for compatibility alignment.

## [52.0.0] - 2026-01-13

### Added

- Add tracing instrumentation for plan creation lifecycle via `TracingQueryPlanner`
  and rule instrumentation macros (`instrument_rules_with_info_spans!`). Phase spans
  now group individual rule executions for analyzer, logical optimizer, and physical
  optimizer, with automatic plan diff capture when rules modify plans
- Add transparent downcasting through `InstrumentedExec` by delegating `as_any()`
  to the inner plan. This allows code to inspect underlying execution node types
  without directly handling the instrumentation wrapper

### Changed

- Update to Apache DataFusion 52.0.0
- Update OpenTelemetry dependencies to 0.31 and tracing-opentelemetry to 0.32
- Update Jaeger documentation to reference version 2.14.0

## [51.0.0] - 2025-11-18

### Changed

- Update to Apache DataFusion 51.0.0
- Update minimum supported Rust version

### Documentation

- Clarify `InstrumentedExec` visibility design decisions

## [50.0.2] - 2025-09-24

### Fixed

- Disable truncation indicator on plan preview output to prevent visual clutter

### Documentation

- Clarify that `instrument_rule` should be placed last in the optimizer rules chain
- Add explanation of why `InstrumentedExec` is intentionally private

## [50.0.1] - 2025-09-22

### Fixed

- Downgrade `comfy-table` dependency to version 7.1 for broader compatibility

## [50.0.0] - 2025-09-05

### Added

- Display dynamic filters in the `datafusion.node` span field, providing visibility
  into filter pushdown during query execution

### Changed

- Update to Apache DataFusion 50.0.0
- Switch to `tpchgen-rs` for generating TPC-H parquet test files

## [49.0.0] - 2025-08-02

### Fixed

- Finalize recursive query instrumentation support. `WorkTableExec` nodes can now
  be safely instrumented following upstream DataFusion changes
  ([apache/datafusion#16469](https://github.com/apache/datafusion/pull/16469)),
  enabling full tracing of all nodes in recursive query execution

### Changed

- Update to Apache DataFusion 49.0.0

## [48.0.1] - 2025-06-20

### Fixed

- Backport partial support for recursive queries instrumentation to the 48.x line

## [48.0.0] - 2025-06-10

### Changed

- Update to Apache DataFusion 48.0.0

## [47.0.2] - 2025-06-20

### Fixed

- Add partial support for recursive queries instrumentation. Recursive queries now
  spawn a new span group for each recursive call via `with_new_inner` support on
  `InstrumentedExec`. Note: `WorkTableExec` nodes are not instrumented in this
  version due to DataFusion limitations

## [47.0.1] - 2025-04-24

### Documentation

- Clarify versioning scheme
- Fix documentation image links

## [47.0.0] - 2025-04-08

Initial public release of DataFusion Tracing.

### Added

- Tracing instrumentation for Apache DataFusion query execution plans
- Automatic span creation around execution steps via physical optimizer rule
- Native DataFusion metrics recording (execution time, output row count, etc.)
- Partial query result preview for debugging
- OpenTelemetry integration for distributed tracing
- Customizable `InstrumentationOptions` for controlling tracing behavior
- Support for custom span fields via `add_custom_field`
- Preview formatting utilities (`pretty_format_compact_batch`)
- Integration with Jaeger, DataDog, and other OpenTelemetry-compatible collectors

[52.0.0]: https://github.com/datafusion-contrib/datafusion-tracing/compare/51.0.0...52.0.0
[51.0.0]: https://github.com/datafusion-contrib/datafusion-tracing/compare/50.0.2...51.0.0
[50.0.2]: https://github.com/datafusion-contrib/datafusion-tracing/compare/50.0.1...50.0.2
[50.0.1]: https://github.com/datafusion-contrib/datafusion-tracing/compare/50.0.0...50.0.1
[50.0.0]: https://github.com/datafusion-contrib/datafusion-tracing/compare/49.0.0...50.0.0
[49.0.0]: https://github.com/datafusion-contrib/datafusion-tracing/compare/48.0.1...49.0.0
[48.0.1]: https://github.com/datafusion-contrib/datafusion-tracing/compare/48.0.0...48.0.1
[48.0.0]: https://github.com/datafusion-contrib/datafusion-tracing/compare/47.0.2...48.0.0
[47.0.2]: https://github.com/datafusion-contrib/datafusion-tracing/compare/47.0.1...47.0.2
[47.0.1]: https://github.com/datafusion-contrib/datafusion-tracing/compare/47.0.0...47.0.1
[47.0.0]: https://github.com/datafusion-contrib/datafusion-tracing/releases/tag/47.0.0
