# GitHub Actions

## Continuous integration

[`workflows/go.yml`](workflows/go.yml) runs on pushes to `main` and pull requests.
Its jobs run independently so failures remain easy to identify:

| Job | Purpose |
| --- | --- |
| `race-test` | Run all tests with CGO, systemd, the race detector, and coverage. |
| `no-cgo` | Build without CGO and test the unsupported systemd backend. |
| `dependency-review` | Review dependency changes on pull requests. |
| `security` | Run govulncheck and gosec; upload reports before failing on findings or incomplete scans. |
| `benchmark` | Save main's benchmarks or compare a pull request against the latest successful main run. |

Security steps use `continue-on-error` to collect every report. The final gate
checks each step's **outcome**, which retains failures even when execution
continues. SARIF uploads are skipped for pull requests from forks.
Gosec scans once, writing SARIF to disk and rendering the same findings as text
for the job log and text artifact. Bash's `pipefail` preserves scan failures
when output is piped through `tee`.

Benchmarks run five times. A pull request fails if benchstat reports a
statistically significant increase greater than `BENCHMARK_REGRESSION_THRESHOLD`
(currently 10%) in a time or allocation metric. The `benchmarks` artifact is
retained for 90 days; the workflow requires a successful main run with that
artifact as its baseline. Keep the workflow filename and artifact names stable:
the baseline lookup uses them.

## Releases

[`workflows/release.yml`](workflows/release.yml) coordinates releases from a
`v*.*.*` tag push, a published release, or a manual run with a tag. Tag pushes
must match `v<number>.<number>.<number>` and create the GitHub release first;
published-release and manual runs upload to the existing release.

1. `prepare-release` resolves the tag and decides whether to proceed.
2. `build-linux` and `build-non-linux` call
   [`workflows/release-binaries.yml`](workflows/release-binaries.yml) with their
   target lists. The shared workflow checks out the release tag, builds each
   target, and uploads the binary as both a workflow artifact and release asset.
   Linux builds use CGO and libsystemd in the pinned Go container; macOS and
   Windows builds use the static unsupported-systemd implementation.
3. `sbom` generates and uploads the source SBOM independently of the builds.
4. `sign-release-assets` waits for both build groups and the source SBOM, then
   signs their artifacts with Cosign and uploads the signature bundles.
5. `container` waits only for Linux builds. It assembles the multi-architecture
   image from those binaries, publishes and signs it, and uploads container
   references and an SBOM with their signatures.

Add or change release targets in `release.yml`; shared build and upload logic
belongs in `release-binaries.yml`. Keep release build steps self-contained so
manual runs can build older tags that lack newer local composite actions.

## Shared setup

- [`actions/setup-go`](actions/setup-go/action.yml) pins the CI Go version,
  shares downloaded modules, and separates build caches by CGO mode and build
  instrumentation. The no-CGO job writes the module cache; race-test, no-CGO,
  and benchmark each write their build cache. Security restores the no-CGO
  cache without writing it. Set `CGO_ENABLED` at job scope when it must also
  affect cache selection.
- [`actions/setup-go-systemd`](actions/setup-go-systemd/action.yml) installs
  missing native dependencies for the race-test and benchmark jobs.

All runners are pinned to Ubuntu 26.04. Keep Go versions aligned in the CI setup
action and the release workflow's native setup and Linux container image.
