# Contributing to VarveDB

Thank you for your interest in contributing to VarveDB! We welcome contributions from the community.

## Getting Started

1.  **Fork the repository** on GitHub.
2.  **Clone your fork** locally.
3.  **Create a new branch** for your feature or bug fix.

## Development Workflow

### Prerequisites

-   Rust (stable)
-   `liblmdb-dev` (for LMDB)

### Running Tests

```bash
cargo test
```

### Running Benchmarks

```bash
cargo bench
```

### Code Style

We use `rustfmt` to enforce code style. Please ensure your code is formatted before submitting a PR.

```bash
cargo fmt
```

### Linting

We use `clippy` for linting.

```bash
cargo clippy -- -D warnings
```

## Pull Request Process

1.  Ensure all tests pass.
2.  Update documentation if necessary.
3.  Add a description of your changes in the PR.
4.  Link to any relevant issues.

## Release Process

We use `release-plz` to automate the release process.
1.  Changes are pushed to `main`.
2.  `release-plz` creates a Release PR with version bumps and changelog updates.
3.  When the Release PR is merged, `release-plz` publishes the crate to crates.io and creates a GitHub release.

## License

By contributing, you agree that your contributions will be licensed under the MPL-2.0 license.
