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

The first local Cargo build resolves dependencies and generates `Cargo.lock`.
Commit that file for reproducible builds across runs. Until it is committed,
GitHub Actions resolves dependencies once per workflow run. All CI lint,
test, and release builds use
`--locked`. Use `cargo update` to intentionally refresh compatible dependencies.

## GitHub Actions and releases

The [Rust workflow](../.github/workflows/rust.yml) runs on relevant pull requests
and pushes to `main`. It checks formatting, runs Clippy and tests, builds an
optimized executable, and runs that executable on Linux x64. Each run saves
the binary and resolved lockfile as artifacts. CI and releases use a single
Ubuntu job; Windows and macOS runners are not used for the Rust app.

Cargo downloads and compiled targets are cached by compiler version,
dependency configuration, and application source. Source changes can restore
the matching dependency cache and rebuild just the changed application.
CI disables debug symbols and incremental compilation to reduce cache size.
A warm cache avoids recompiling Polars, but total runtime still includes
runner startup, toolchain setup, cache transfer, and artifact uploads.

The existing [release workflow](../.github/workflows/release.yml) calls the same
checks and builds from the release tag. Rust releases
currently target Linux x64 only and attach these assets to the GitHub release:

- `polars-app-linux-amd64`
- `polars-app-Cargo.lock`

Each asset also receives a `.sigstore.json` signature bundle from the existing
Cosign signing job. The lockfile records the exact dependencies used for that
release. Linux binaries use glibc from the Ubuntu 26.04 runner; use a compatible
Linux system. After downloading the Linux binary, make it executable
with `chmod +x <filename>`.

After merging, create the next repository release by pushing a new
`v<number>.<number>.<number>` tag at the desired commit. This creates the GitHub
release and publishes both Go and Rust assets. The release version follows the
repository tag; `Cargo.toml` currently retains the scaffold's package version.
Re-running the existing Release workflow with a tag replaces its assets.
Older tags without `rust-app/Cargo.toml` skip Rust builds and retain the Go
release behavior.

## Validation status

The manifest, Makefile, and workflow configuration were checked locally.
Compilation and execution could not be verified in the scaffold environment:
Rust was not installed and the network proxy was unreachable, preventing
toolchain and dependency downloads. GitHub Actions performs those checks on
hosted runners. No generated `Cargo.lock` is committed yet.
