# Polars Rust app

A minimal Cargo application that creates and prints a Polars DataFrame.

## Requirements

Install stable Rust using [rustup](https://rustup.rs/) and a native C linker
(for example, `build-essential` on Debian/Ubuntu, Xcode Command Line Tools on
macOS, or Visual Studio C++ Build Tools on Windows). The toolchain file selects
stable Rust with rustfmt and Clippy. GNU Make is optional.

## Build and run

Run these commands from this directory:

```sh
cargo build
cargo run
cargo build --release
```

The example prints three names and scores. Release binaries are written to
`target/release/`.

```sh
cargo check --all-targets
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

The Makefile provides equivalent `build` (default), `run`, `release`, `check`,
`test`, `fmt` (format check), and `lint` targets, plus `clean`.
Use `cargo fmt --all` to apply formatting.

## Dependency version

The [published Rust documentation](https://docs.rs/polars/latest/polars/)
lists Polars **0.55.2** as the latest release when this scaffold was created.
The requested Rust Polars 2.0 release could not be verified, so this app uses
0.55.2. Default features are disabled and only table formatting is enabled to
keep the initial build small. Add features such as `lazy`, `csv`, or `parquet`
to `Cargo.toml` as needed.

The first Cargo build resolves dependencies and generates `Cargo.lock`. Commit
that file for reproducible application builds, then use `cargo build --locked`
and `cargo test --locked` in CI. Use `cargo update` to intentionally refresh
compatible dependency versions.

## Validation status

The manifest and Makefile were checked structurally. Compilation and execution
could not be verified in the scaffold environment: Rust was not installed and
the network proxy was unreachable, preventing toolchain and dependency downloads.
For the same reason, no generated `Cargo.lock` is included yet.
