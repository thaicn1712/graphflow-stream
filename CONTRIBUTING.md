# Contributing

```bash
git clone https://github.com/thaicn1712/graphflow-stream
cd graphflow-stream
cargo test --all-targets
cargo clippy --all-targets -- -D warnings
cargo fmt --check
```

All four must pass before a PR is merged — CI runs the same checks. Keep public API changes covered by an integration test in `tests/integration.rs`.
