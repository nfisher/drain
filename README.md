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

	for _, cluster := range logger.Clusters() {
		println(cluster.String())
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

Build a minimal container for the `cluster` CLI from the repository root:

```sh
docker build -t drain-cluster .
```

The Dockerfile compiles `./cmd/cluster` in the standard Golang image and copies
the binary into a non-root Distroless runtime image. Pass `VERSION` and `COMMIT`
build arguments to populate `cluster version`:

```sh
docker build \
  --build-arg VERSION=1.2.3+abc1234def56 \
  --build-arg COMMIT=abc1234def56 \
  -t drain-cluster:1.2.3 .
```

Release tags publish a multi-architecture container to GitHub Container
Registry as `ghcr.io/<owner>/<repo>-cluster:v1.2.3` and
`ghcr.io/<owner>/<repo>-cluster:1.2.3`. The image is built with the same
`1.2.3+<commit>` version string used for release binaries, and the release gets
a `cluster-container.txt` asset with the pushed tags and digest.

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

`parse` reads from the `file` source by default. The equivalent explicit form is:

```sh
go run ./cmd/cluster parse -source file -filename target.log -model model.json
```

To parse the current kernel ring buffer, use the `dmesg` source. Add `-follow`
to stream `dmesg -w` until the process is interrupted:

```sh
go run ./cmd/cluster parse -source dmesg -model model.json
go run ./cmd/cluster parse -source dmesg -follow -model model.json
```

To parse systemd journal entries, use the `systemd` source. By default Drain
sees the journal `MESSAGE` field; use `-systemd-line-format short` for a
journalctl-like line, or `json` to parse the raw journal JSON record. Add
`-systemd-follow` to read history and then stream new entries:

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
source "file" "target" {
  filename = "target.log"
}

source "dmesg" "kernel" {
  follow = true
}

source "systemd" "ssh" {
  units = ["ssh.service"]
  since = "today"
  line_format = "message"
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

`-config` is exclusive with the source, model, output, batching, and S3 flags.
Use CLI flags for a simple source -> model -> sink pipeline. Inline
`source` and `sink` blocks inside a `pipeline` are also supported for one-off
definitions.

To turn simple CLI parse flags into a one-pipeline HCL config with reusable
top-level source and sink definitions, use `-generate-config`. It prints the
config and exits without reading the source or model files:

```sh
go run ./cmd/cluster parse -generate-config -filename target.log -model model.json -output out/parsed > pipelines.hcl
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

`S3_REGION` defaults to `us-east-1`, and path-style bucket lookup defaults to
true for S3-compatible storage.

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
`path_style`. If a field is omitted, the standard S3/AWS env var fallback still
applies.

## LICENSE

[MIT](LICENSE)
