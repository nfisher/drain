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

`Cargo.lock` is committed from a successful Linux CI build, fixing dependency
versions across runs. All CI lint, test, and release builds use `--locked`.
Use `cargo update` to intentionally refresh compatible dependencies and commit
the updated lockfile. Older release tags without a lockfile resolve one during
the workflow run.

The lockfile pins dependency versions and belongs in Git. The cache stores
downloaded dependencies and compiled code; it may be evicted without changing
which dependencies the app builds against.

## GitHub Actions and releases

The [Rust workflow](../.github/workflows/rust.yml) runs on relevant pull requests,
pushes to `main`, and releases. It checks formatting, runs Clippy and tests,
builds an optimized executable, and runs it natively for Linux AMD64, Linux
ARM64, and macOS ARM64. The three jobs share one lockfile and save each binary
as an artifact. Windows is not a Rust build target.

Cargo downloads and compiled targets are cached by platform, compiler version,
dependency configuration, and application source. Source changes can restore
the matching dependency cache and rebuild just the changed application.
CI disables debug symbols and incremental compilation to reduce cache size.
A warm cache avoids recompiling Polars, but total runtime still includes
runner startup, toolchain setup, cache transfer, and artifact uploads.

The existing [release workflow](../.github/workflows/release.yml) calls the same
checks and builds from the release tag. Rust releases attach these assets:

- `polars-app-linux-amd64`
- `polars-app-linux-arm64`
- `polars-app-osx-arm64`
- `polars-app-Cargo.lock`

Each asset also receives a `.sigstore.json` signature bundle from the existing
Cosign signing job. The lockfile records the exact dependencies used for that
release. Linux binaries use glibc from the Ubuntu 26.04 runner; use a compatible
Linux binaries use glibc from Ubuntu 26.04; use a compatible Linux system.
After downloading a binary, make it executable with `chmod +x <filename>`.

After merging, create the next repository release by pushing a new
`v<number>.<number>.<number>` tag at the desired commit. This creates the GitHub
release and publishes both Go and Rust assets. The release version follows the
repository tag; `Cargo.toml` currently retains the scaffold's package version.
Re-running the existing Release workflow with a tag replaces its assets.
Older tags without `rust-app/Cargo.toml` skip Rust builds and retain the Go
release behavior.

## Validation status

The manifest, Makefile, and workflow configuration were checked locally.
Hosted Linux CI passed formatting, Clippy, tests, optimized compilation,
execution of the example, artifact uploads, and build-cache creation.
