# Contributing to DataFusion Tracing

Thanks for your interest in contributing! Here's how to get started.

## Getting Started

Make sure you have Rust installed (latest stable version). Then clone the repo and get to work:

```bash
git clone https://github.com/datafusion-contrib/datafusion-tracing.git
cd datafusion-tracing
```

## Linting

We use automated tools to keep the code clean and consistent. The easiest way to run all lints is with `./dev/rust_lint.sh`. This script runs all the checks from `ci/scripts/`, including formatting, clippy, TOML formatting, and documentation. If you don't have `taplo` installed, the script will install it for you.

## Making Changes

Fork the repo, create a branch from `main`, make your changes, run the linting script, and open a PR with a clear description of what you've done.

## Code Style

Use standard Rust formatting, write clear commit messages, document public APIs, add tests for new code, and keep PRs focused.

## Versioning

This project follows semantic versioning (SemVer) with the following constraints:

- The **major** version number must align with the major version of the DataFusion dependency.
- The **minor** and **patch** versions can change independently based on features and fixes specific to this project.

This allows users to easily understand compatibility with specific DataFusion versions while giving flexibility for this project's own development cycle.

## License

Contributions are licensed under Apache License 2.0. 