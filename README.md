# Drain

> This project is an golang port of the original [Drain3](https://github.com/IBM/Drain3) project.

Drain is an online log template miner that can extract templates (clusters) from a stream of log messages in a timely manner. It employs a parse tree with fixed depth to guide the log group search process, which effectively avoids constructing a very deep and unbalanced tree.

`Config.LogClusterDepth` controls the fixed tree depth, `Config.MaxChildren`
caps child nodes per internal node, and `Config.PreserveNumericTokens` keeps
digit-bearing tokens as exact tree keys when the default numeric
parameterization is not desired. `Match` keeps the fast tree-only inference
path, while `MatchWithOptions` can use `FullSearchFallback` or
`FullSearchAlways` to scan same-length clusters when exact inference must avoid
tree-search false negatives.

## Example

```go
package main

import (
	"fmt"
	"strings"

	"github.com/faceair/drain"
)

func main() {
	logger := drain.New(drain.DefaultConfig())

	for _, line := range []string{
		"connected to 10.0.0.1",
		"connected to 10.0.0.2",
		"connected to 10.0.0.3",
		"Hex number 0xDEADBEAF",
		"Hex number 0x10000",
		"user davidoh logged in",
		"user eranr logged in",
	} {
		logger.Train(line)
	}

	for _, cluster := range logger.ClusterSnapshots() {
		fmt.Printf("id={%d} : size={%d} : %s\n", cluster.ID, cluster.Size, strings.Join(cluster.TemplateTokens, " "))
	}

	cluster := logger.Match("user faceair logged in")
	if cluster == nil {
		println("no match")
	} else {
		fmt.Printf("cluster matched: %s", cluster.String())
	}
}
```

Output:
```
id={1} : size={3} : connected to <*>
id={2} : size={2} : Hex number <*>
id={3} : size={2} : user <*> logged in
cluster matched: id={3} : size={2} : user <*> logged in
```

## Masking

Use `Config.MaskingRules` to replace known variable patterns before Drain tokenizes
and clusters a log line. A rule with an empty `MaskWith` uses `ParamString`
(`<*>` by default). A plain `MaskWith` name renders as a Drain3-style named
mask token, so `MaskWith: "IP"` becomes `<:IP:>`. Values containing `<`, `>`,
or `$` are treated as explicit literal replacements for compatibility.

For example, this masks a bracketed timestamp prefix such as
`[Mon May 11 13:41:21 2026]`:

```go
config := drain.DefaultConfig()
config.MaskingRules = []drain.MaskingRule{
	{
		Pattern: `^\[[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-3]?[0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [0-9]{4}\]`,
	},
}

logger := drain.New(config)
```

With that rule, a log line beginning with `[Mon May 11 13:41:21 2026] Linux version ...`
is mined as `<*> Linux version ...`.

The `cluster` CLI can load masking rules from a JSON array with
`-masking-rules masks.json`. The file replaces the CLI defaults, so include any
default rules you want to keep when overriding timestamp masking. Rule objects
use the same fields as `drain.MaskingRule`; `regex_pattern` is also accepted as
a Drain3-compatible alias for `pattern`.

## Extra delimiters

Use `Config.ExtraDelimiters` to split tokens on literal delimiters in addition
to whitespace. Delimiters are applied after masking, matching Drain3's
`extra_delimiters` behavior:

```go
config := drain.DefaultConfig()
config.ExtraDelimiters = []string{"_", ":"}

logger := drain.New(config)
cluster := logger.Train("user_alice:logged_in")
// cluster.Template() == "user alice logged in"
```

Named masks can be extracted from a mined template:

```go
config := drain.DefaultConfig()
config.MaskingRules = []drain.MaskingRule{
	{Pattern: `\d+`, MaskWith: "NUM"},
}

logger := drain.New(config)
cluster := logger.Train("service id=123 status ok")
parameters, ok := logger.ExtractParameters(cluster.Template(), "service id=456 status ok")
// ok == true
// parameters == []drain.ExtractedParameter{{Value: "456", MaskName: "NUM"}}
```

## Cluster CLI build info

Use `cluster version`, `cluster --version`, or `cluster -version` to print the
CLI build version and commit. Local builds default both values to `dev`.
Release binaries use the release tag without the leading `v`, plus the short
Git commit as SemVer build metadata:

```text
version: 1.2.3+abc1234def56
commit: abc1234def56
```

## Cluster container

Build a minimal container for the `cluster` CLI from the repository root after
placing a matching Linux binary in `dist/`. On Linux with `libsystemd-dev` and
`pkg-config` installed, one way to produce that binary is:

```sh
TARGETARCH="$(go env GOARCH)"
mkdir -p dist
CGO_ENABLED=1 GOOS=linux GOARCH="$TARGETARCH" go build \
  -trimpath \
  -ldflags="-s -w -X main.buildVersion=dev -X main.buildCommit=dev" \
  -o "dist/cluster-linux-${TARGETARCH}" \
  ./cmd/cluster
docker build --build-arg TARGETARCH="$TARGETARCH" -t drain-cluster .
```

The Dockerfile copies `dist/cluster-linux-${TARGETARCH}` into a non-root Debian
runtime image with `ca-certificates` and `libsystemd0`. Populate
`cluster version` by passing the `main.buildVersion` and `main.buildCommit`
linker values when building the binary:

```sh
TARGETARCH="$(go env GOARCH)"
CGO_ENABLED=1 GOOS=linux GOARCH="$TARGETARCH" go build \
  -trimpath \
  -ldflags="-s -w -X main.buildVersion=1.2.3+abc1234def56 -X main.buildCommit=abc1234def56" \
  -o "dist/cluster-linux-${TARGETARCH}" \
  ./cmd/cluster
docker build --build-arg TARGETARCH="$TARGETARCH" -t drain-cluster:1.2.3 .
```

Release tags publish a multi-architecture container to GitHub Container
Registry as `ghcr.io/<owner>/<repo>-cluster:v1.2.3` and
`ghcr.io/<owner>/<repo>-cluster:1.2.3`, plus
`ghcr.io/<owner>/<repo>-cluster:latest`. The image is assembled from the Linux
release binaries, so the container and release assets use the same
`1.2.3+<commit>` executable. The release gets a `cluster-container.txt` asset
with the pushed tags and digest. The `latest` tag tracks the most recent
release; pin production deployments to a specific version tag or digest.

## Kubernetes DaemonSet

Use `daemonset.yaml` to run `cluster parse` on every Linux node with the dmesg
source and JSONL output on stdout. The manifest includes a service account, a
`drain-cluster-config` ConfigMap with `config.hcl`, and a DaemonSet that exposes
the Prometheus `/metrics` endpoint on port 9090 with standard scrape
annotations. Host `/dev`, `/proc`, `/sys`, `/run`, and `/var/log` are mounted
read-only under `/host`, and the dmesg source reads `/host/dev/kmsg`.

The trained model is supplied separately. The `drain-cluster-model` ConfigMap
must exist in the same namespace as the DaemonSet and must contain a
`model.json` key. The DaemonSet mounts that key at
`/etc/drain/model/model.json`; keep this mounted path unchanged because the HCL
pipeline references that exact file.

```sh
kubectl create configmap drain-cluster-model --from-file=model.json=./model.json
kubectl apply -f daemonset.yaml
```

The DaemonSet uses `ghcr.io/nfisher/drain-cluster:latest` by default. Before
deploying to production, edit `daemonset.yaml` to replace `latest` with a
specific version such as `ghcr.io/nfisher/drain-cluster:1.2.3` or pin the image
by digest. The dmesg source reads the host kernel message device through the
configured `/host/dev/kmsg` path. Tests in representative Kubernetes clusters
should first try the narrowest viable security context: run as UID 0, keep host
mounts read-only, and add explicit capabilities only if the cluster requires
them. If `/host/dev/kmsg` is still blocked without privileged mode, keep the
manifest's privileged security context and grant this DaemonSet an exception in
clusters enforcing the restricted Pod Security profile.

## Cluster model metadata

The `cluster` CLI can merge a JSON object into the generated `model.json` as a
top-level `metadata` key. This is useful for recording details about the system
used to train the model, such as values from `lsb_release`, `uname`, or the
target architecture.

For example, create `system.json`:

```json
{
  "system": {
    "os": "Ubuntu 24.04.2 LTS",
    "arch": "aarch64",
    "kernel": "6.14.0-1008-nvidia-64k"
  }
}
```

Then pass it during training:

```sh
go run ./cmd/cluster train -filename example.log -model model.json -metadata system.json
```

The command writes a generated UTC `created_at` timestamp into `metadata`. When
updating an existing model with `-update`, it preserves existing metadata,
shallow-merges the file from `-metadata` when provided, and writes a generated
UTC `updated_at` timestamp.

## Cluster parse output

By default, `cluster parse` writes JSONL to stdout:

```sh
go run ./cmd/cluster parse -filename target.log -model model.json
```

Add `-metrics-listen-address` to enable a Prometheus endpoint at `/metrics`
while `parse` is running:

```sh
go run ./cmd/cluster parse -filename target.log -model model.json -metrics-listen-address :9090
```

`parse` reads from the `file` source by default. The equivalent explicit form is:

```sh
go run ./cmd/cluster parse -source file -filename target.log -model model.json
```

Use `-checkpoint <state.json>` to persist the last acknowledged source position
after the sink write succeeds. On restart with the same checkpoint path, file
sources seek to the saved byte offset, dmesg sources skip records through the
saved kernel-message cursor, and systemd sources seek after the saved journal
cursor. The checkpoint file is written atomically as JSON and is also available
as `checkpoint = "..."` on HCL source blocks.

```sh
go run ./cmd/cluster parse -filename target.log -model model.json -checkpoint state/target.json
go run ./cmd/cluster parse -source systemd -systemd-follow -model model.json -checkpoint state/journal.json
```

To parse the current kernel ring buffer, use the `dmesg` source. Linux snapshot
reads use `/dev/kmsg` directly and fall back to the kernel syslog API when
`/dev/kmsg` is unavailable; BSD-derived systems read `kern.msgbuf` directly.
Add `-follow` on Linux to stream new `/dev/kmsg` records until the process is
interrupted. Use `-dmesg-kmsg-path` when `/dev/kmsg` is mounted somewhere else,
such as `/host/dev/kmsg` in Kubernetes; the syslog fallback is only used for the
default path.

```sh
go run ./cmd/cluster parse -source dmesg -model model.json
go run ./cmd/cluster parse -source dmesg -follow -model model.json
go run ./cmd/cluster parse -source dmesg -follow -dmesg-kmsg-path /host/dev/kmsg -model model.json
```

To parse systemd journal entries, use the `systemd` source. On Linux this source
uses the native `sd-journal` API directly and requires a cgo build with
`libsystemd`; non-Linux and no-cgo builds return an explicit unsupported-source
error. By default Drain sees the journal `MESSAGE` field; use
`-systemd-line-format short` for a journal-like line, or `json` to parse a JSON
record built from journal fields. Add `-systemd-follow` to read history and then
stream new entries:

```sh
go run ./cmd/cluster parse -source systemd -model model.json \
  -systemd-unit ssh.service \
  -systemd-since today

go run ./cmd/cluster parse -source systemd -model model.json \
  -systemd-unit ssh.service \
  -systemd-follow
```

For multiple parse pipelines, pass an HCL config. Each pipeline has its own
model, one or more sources, and one or more sinks. Pipelines and their sources
run concurrently, and each parsed record is written to every sink in that
pipeline:

```hcl
telemetry {
  metrics_listen_address = ":9090"
}

source "file" "target" {
  filename   = "target.log"
  checkpoint = "state/target.json"
}

source "dmesg" "kernel" {
  follow     = true
  kmsg_path  = "/dev/kmsg"
  checkpoint = "state/dmesg.json"
}

source "systemd" "ssh" {
  units       = ["ssh.service"]
  since       = "today"
  line_format = "message"
  checkpoint  = "state/systemd.json"
}

sink "jsonl" "local" {
  output = "out/parsed"
  exclude_source = true
}

sink "parquet" "s3" {
  output = "s3://logs/parsed"

  s3 {
    endpoint_env = "S3_ENDPOINT"
    access_key_id_file = "/var/run/secrets/drain-s3/access_key_id"
    secret_access_key_file = "/var/run/secrets/drain-s3/secret_access_key"
  }
}

pipeline "kernel" {
  model = "models/kernel.json"
  sources = ["file.target", "dmesg.kernel", "systemd.ssh"]
  sinks = ["jsonl.local", "parquet.s3"]
}
```

```sh
go run ./cmd/cluster parse -config pipelines.hcl
```

The optional top-level `telemetry` block enables a Prometheus endpoint at
`/metrics`. The endpoint exposes `drain_cluster_build_info` with `version` and
`commit` labels:

```text
drain_cluster_build_info{commit="abc1234def56",version="1.2.3+abc1234def56"} 1
```

For config runs, `-metrics-listen-address` may also be passed as an operational
override; if both are set, the flag wins.

`-config` is exclusive with the source, model, output, batching, and S3 flags,
but may be combined with `-metrics-listen-address`. Use CLI flags for a simple
source -> model -> sink pipeline. Inline `source` and `sink` blocks inside a
`pipeline` are also supported for one-off definitions.

To turn simple CLI parse flags into a one-pipeline HCL config with reusable
top-level source and sink definitions, use `-generate-config`. It prints the
config and exits without reading the source or model files:

```sh
go run ./cmd/cluster parse -generate-config -filename target.log -model model.json -output out/parsed -metrics-listen-address :9090 > pipelines.hcl
```

Use `-output` to write files under a local prefix. JSONL remains the default
format:

```sh
go run ./cmd/cluster parse -filename target.log -model model.json -output out/parsed
```

Write Parquet by setting `-format parquet`:

```sh
go run ./cmd/cluster parse -filename target.log -model model.json -format parquet -output out/parsed
```

Output files use query-friendly partition paths:

```text
out/parsed/format=jsonl/run_id=<run-id>/part-00000.jsonl
out/parsed/format=parquet/run_id=<run-id>/part-00000.parquet
```

Parts rotate after `-batch-size` rows, default `10000`, or when a non-empty part
reaches `-batch-max-age`, default `5s`. The final part is flushed when parsing
finishes.

Every parsed row includes `template_id`, `model_id`, `source_kind`,
`source_name`, and `variables`. By default, parse keeps `variables` and omits
typed parameters from output. Pass `-include-parameters` to emit the JSONL
`parameters` field and Parquet `parameters` column.

Pass `-exclude-source`, or set `exclude_source = true` on an HCL sink, to omit
`source_kind` and `source_name` from that sink's JSONL or Parquet output.

S3-compatible prefixes use `s3://bucket/prefix`. Configure storage with env
vars:

```sh
export S3_ENDPOINT=http://127.0.0.1:9000
export S3_ACCESS_KEY_ID=minioadmin
export S3_SECRET_ACCESS_KEY=minioadmin

go run ./cmd/cluster parse -filename target.log -model model.json -format parquet -output s3://logs/parsed
```

CLI flags override env vars:

```sh
go run ./cmd/cluster parse -filename target.log -model model.json \
  -format jsonl \
  -output s3://logs/parsed \
  -s3-endpoint http://127.0.0.1:9000 \
  -s3-access-key-id minioadmin \
  -s3-secret-access-key minioadmin
```

`S3_REGION` defaults to `us-east-1`. TLS for S3 requests defaults to enabled,
even when the endpoint is written with an `http://` scheme; disable it only
when required with `S3_USE_SSL=false`, `-s3-use-ssl=false`, or HCL
`use_ssl = false`. Path-style bucket lookup defaults to true for S3-compatible
storage.

For Kubernetes Secrets mounted as files, use the matching `*_FILE` env vars or
CLI file flags. Secret file contents are trimmed, so the trailing newline added
by common Secret workflows is safe:

```sh
export S3_ENDPOINT_FILE=/var/run/secrets/drain-s3/endpoint
export S3_ACCESS_KEY_ID_FILE=/var/run/secrets/drain-s3/access_key_id
export S3_SECRET_ACCESS_KEY_FILE=/var/run/secrets/drain-s3/secret_access_key

go run ./cmd/cluster parse -filename target.log -model model.json -output s3://logs/parsed
```

The equivalent CLI flags are `-s3-endpoint-file`,
`-s3-access-key-id-file`, `-s3-secret-access-key-file`, and corresponding
`-file` variants for region, session token, SSL, and path-style settings.

HCL `s3` blocks also support direct values, mounted ConfigMap/Secret files, and
explicit env var references for each field. Use one of `endpoint`,
`endpoint_file`, or `endpoint_env`; the same pattern applies to `region`,
`access_key_id`, `secret_access_key`, `session_token`, `use_ssl`, and
`path_style`. Set `use_ssl = false` to explicitly opt out of TLS from HCL.
If a field is omitted, the standard S3/AWS env var fallback still applies.

## LICENSE

[MIT](LICENSE)
