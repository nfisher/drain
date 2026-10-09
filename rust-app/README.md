# Polars Rust app

A Cargo application that reads Parquet files from S3 into a Polars DataFrame
and prints it. With no arguments it creates and prints a sample DataFrame.

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

## Read Parquet from S3

Pass an S3 URI after Cargo's `--` argument separator:

```sh
export AWS_REGION=us-east-1
# Supply AWS_ACCESS_KEY_ID and AWS_SECRET_ACCESS_KEY through your environment.
# Temporary credentials also require AWS_SESSION_TOKEN.
cargo run -- 's3://logs/parsed/format=parquet/run_id=my-run/part-00000.parquet'
```

The downloaded release executable accepts the same argument:

```sh
./polars-app-linux-amd64 's3://logs/parsed/format=parquet/run_id=my-run/part-00000.parquet'
```

Use a quoted glob to read multiple files with compatible schemas. The quotes
prevent the shell from expanding the pattern:

```sh
cargo run -- 's3://logs/parsed/format=parquet/run_id=my-run/*.parquet'
```

Polars scans Parquet directly through its S3 object-store support, then collects
the result in memory. The full result must fit in memory; the printed table may
abbreviate large DataFrames. Single objects require read permission; globs also
require permission to list the bucket. Storage, authentication, and Parquet
errors are printed to stderr and cause a nonzero exit status. `--help` prints
usage without connecting to storage.

### S3-compatible storage

The reader accepts the Go CLI's `S3_ENDPOINT`, `S3_REGION`,
`S3_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY`, and `S3_SESSION_TOKEN` environment
variables. Nonempty `S3_*` values override the corresponding AWS configuration.
Otherwise Polars uses its AWS credential discovery, including standard AWS
environment variables. Set the region explicitly for AWS buckets.

For a local MinIO endpoint:

```sh
export S3_ENDPOINT=http://127.0.0.1:9000
export S3_REGION=us-east-1
# Supply S3_ACCESS_KEY_ID and S3_SECRET_ACCESS_KEY through your environment.
cargo run -- 's3://logs/parsed/format=parquet/run_id=my-run/*.parquet'
```

Path-style addressing is the default. Set
`AWS_VIRTUAL_HOSTED_STYLE_REQUEST=true` if your endpoint requires virtual-hosted
bucket addressing. `AWS_ENDPOINT` is also supported when `S3_ENDPOINT` is unset.
AWS uses HTTPS by default. For custom storage, the endpoint's `https://` or
`http://` scheme selects the transport.
The Rust reader does not use the Go CLI's `S3_USE_SSL`, `S3_PATH_STYLE`, or
`*_FILE` settings; inject mounted credentials as environment variables instead.

## Checks

```sh
cargo check --all-targets
cargo test
cargo fmt --all -- --check
cargo clippy --all-targets -- -D warnings
```

The Makefile provides equivalent `build` (default), `run`, `release`, `check`,
`test`, `fmt` (format check), and `lint` targets, plus `clean`.
Use `cargo fmt --all` to apply formatting.

Tests include a local S3-compatible HTTP fixture that checks signed requests,
single-file and glob reads, temporary session tokens, missing objects, and
invalid Parquet data. No live bucket or real credentials are required.

## Dependency version

The [published Rust documentation](https://docs.rs/polars/latest/polars/)
lists Polars **0.55.2** as the latest release when this scaffold was created.
The requested Rust Polars 2.0 release could not be verified, so this app uses
0.55.2. Default features are disabled; `fmt`, `lazy`, `parquet`, and `aws`
enable table formatting and native S3 Parquet scans.

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
