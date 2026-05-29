# Cluster CLI

`cluster` trains Drain templates from log files, evaluates a saved model against
another log file, and parses matching lines into template IDs plus extracted
variables.

## Train

Train reads a log file with the existing Drain training path and writes a fresh
JSON model:

```sh
go run ./cmd/cluster train -filename example.log -model model.json
```

The model contains the command's Drain config, metadata, masking rules, and
sorted templates with IDs, sizes, template strings, and token lists. A compact
runtime `model_id` is computed as an unpadded base64url SHA-256 digest of the
complete templates list when the model is read; it is not stored in the model
JSON. A timestamp prefix masking rule and Drain3-inspired `ID`, `IP`, `SEQ`,
`HEX`, and `NUM` masks are enabled by default, the cluster depth is set to `6`,
`max_children` is set to `100`, numeric tokens are parameterized, and the
training similarity threshold defaults to `0.4`.

To use a different training similarity threshold, pass `-sim-th` with a value
from `0` through `1`:

```sh
go run ./cmd/cluster train -filename example.log -model model.json -sim-th 0.6
```

Tree-shape options can also be set during training:

```sh
go run ./cmd/cluster train -filename example.log -model model.json -depth 7 -max-children 200 -parametrize-numeric-tokens=false
```

Extra delimiters split tokens on literal separators after masking. Repeat the
flag to configure more than one delimiter:

```sh
go run ./cmd/cluster train -filename example.log -model model.json -extra-delimiter _ -extra-delimiter :
```

Masking rule files replace the default masking rules. This is useful when the
input uses a different timestamp format or when a log source has domain-specific
variables:

```sh
go run ./cmd/cluster train -filename example.log -model model.json -masking-rules masks.json
```

The file is a JSON array. Use `pattern` for a Go regular expression, or
`regex_pattern` when reusing Drain3-style rule files. `mask_with` names a
Drain3-style mask. If `mask_with` is omitted, the command uses the default
parameter token, `<*>`.

This file is equivalent to the built-in defaults:

```json
[
  {
    "pattern": "^\\[[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-3]?[0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [0-9]{4}\\]"
  },
  {
    "pattern": "\\b(?:[0-9a-f]{2,}:){3,}[0-9a-f]{2,}\\b",
    "mask_with": "ID"
  },
  {
    "pattern": "\\b\\d{1,3}(?:\\.\\d{1,3}){3}\\b",
    "mask_with": "IP"
  },
  {
    "pattern": "\\b[0-9a-f]{6,}(?:\\s+[0-9a-f]{6,}){2,}\\b",
    "mask_with": "SEQ"
  },
  {
    "pattern": "\\b[0-9A-F]{4}(?:\\s+[0-9A-F]{4}){3,}\\b",
    "mask_with": "SEQ"
  },
  {
    "pattern": "\\b0x[a-f0-9A-F]+\\b",
    "mask_with": "HEX"
  },
  {
    "pattern": "[-+]?\\b\\d+\\b",
    "mask_with": "NUM"
  }
]
```

To override timestamp masking, provide the timestamp rule you want plus any
other defaults or custom variables you still want to keep. Put specific rules
before broad ones; for example, place `URL` before `VERSION` and `PATH`, and
place `PCI`, `UUID`, and `MAC` before broad `ID`, `HEX`, or `NUM` rules.

```json
[
  {
    "pattern": "^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})",
    "mask_with": "TIMESTAMP"
  },
  {
    "pattern": "\\b(?:[0-9a-fA-F]{4}:)?[0-9a-fA-F]{2}:[0-9a-fA-F]{2}\\.[0-7]\\b",
    "mask_with": "PCI"
  },
  {
    "pattern": "\\bhttps?://[^\\s]+",
    "mask_with": "URL"
  },
  {
    "pattern": "\\bv?\\d+\\.\\d+\\.\\d+(?:-[0-9A-Za-z.-]+)?(?:\\+[0-9A-Za-z.-]+)?\\b",
    "mask_with": "VERSION"
  },
  {
    "pattern": "\\b\\d+(?:\\.\\d+)?\\s*(?:[KMGTPE]i?B/s|[KMGTPE]?B/s|[KMGTPE]?b/s|[KMGTPE]?bps)\\b",
    "mask_with": "BANDWIDTH"
  },
  {
    "pattern": "\\b[0-9a-fA-F]{2}(?::[0-9a-fA-F]{2}){5}\\b",
    "mask_with": "MAC"
  },
  {
    "pattern": "\\b[0-9a-fA-F]{8}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{4}-[0-9a-fA-F]{12}\\b",
    "mask_with": "UUID"
  },
  {
    "pattern": "\\b[0-9a-fA-F]{32,64}\\b",
    "mask_with": "HASH"
  },
  {
    "pattern": "\\b\\d+(?:\\.\\d+)?\\s*(?:B|KiB|MiB|GiB|TiB|KB|MB|GB|TB)\\b",
    "mask_with": "SIZE"
  },
  {
    "pattern": "\\b\\d+(?:\\.\\d+)?\\s*(?:ns|us|ms|s|m|h)\\b",
    "mask_with": "DURATION"
  },
  {
    "pattern": "(?:/[^\\s:/]+)+",
    "mask_with": "PATH"
  },
  {
    "pattern": "\\b[^\\s@]+@[^\\s@]+\\.[^\\s@]+\\b",
    "mask_with": "EMAIL"
  }
]
```

The default masks reduce duplicate templates for networking, kernel, driver,
and numeric-heavy logs, and make parsed parameters more descriptive. The
tradeoff is that broad masks can hide meaningful literals, change templates
compared with older models, and add regex cost. Keep source-specific masks in a
rule file when those tradeoffs are not appropriate globally.

To merge system metadata into the saved model, pass `-metadata` with a JSON
object file. The command writes the object under the top-level `metadata` key
and adds a generated UTC `created_at` timestamp:

```json
{
  "system": {
    "os": "Ubuntu 24.04.2 LTS",
    "arch": "aarch64",
    "kernel": "6.14.0-1008-nvidia-64k"
  }
}
```

```sh
go run ./cmd/cluster train -filename example.log -model model.json -metadata system.json
```

To update an existing model with additional logs, pass `-update`:

```sh
go run ./cmd/cluster train -update -filename new.log -model model.json
```

Incremental training restores the saved templates into Drain before training the
new file. Existing template IDs and sizes are preserved, matching new lines
update those templates, and newly discovered templates receive IDs after the
highest restored ID. Updates reuse saved `sim_th`, `log_cluster_depth`,
`max_children`, `parametrize_numeric_tokens`, and `extra_delimiters` unless the
corresponding flag is passed again. Existing metadata is preserved, metadata
from `-metadata` is shallow-merged over it, and the command writes a generated
UTC `updated_at` timestamp.

## Test

Test loads a model and reports how many lines in a target file match each
template. Evaluation uses Drain's perfect-match fallback path, so `sim_th` only
affects training:

```sh
go run ./cmd/cluster test -filename target.log -model model.json
```

Output is JSON:

```json
{
  "total": 3,
  "matched": 2,
  "unmatched": 1,
  "templates": [
    {
      "template_id": 1,
      "model_id": "wK5I_oSM65L6xMlu04Dsx7S-e6fJBabRsHvSUoJs4Lg",
      "template": "<*> user <*> logged in",
      "count": 2
    }
  ]
}
```

## Parse

Parse loads a model and emits one JSON object per input line. By default, it
writes JSONL to stdout:
Like `test`, it uses perfect fallback template matching rather than the
training similarity threshold.

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

To parse systemd journal entries, use the `systemd` source. Drain receives the
journal `MESSAGE` field by default, while `-systemd-line-format short` creates a
deterministic journalctl-like line from journal metadata and `json` passes the
raw journal JSON record through. Add `-systemd-follow` to read historical
entries first and then stream new entries until interrupted:

```sh
go run ./cmd/cluster parse -source systemd -model model.json \
  -systemd-unit ssh.service \
  -systemd-identifier sshd \
  -systemd-priority warning \
  -systemd-since today

go run ./cmd/cluster parse -source systemd -model model.json \
  -systemd-unit ssh.service \
  -systemd-follow
```

Systemd filters map to journalctl options: `-systemd-unit`,
`-systemd-identifier`, `-systemd-priority`, `-systemd-since`,
`-systemd-until`, `-systemd-boot`, and `-systemd-after-cursor`.

For multiple parse pipelines, pass an HCL config file. `-config` is exclusive
with the source, model, output, batching, and S3 flags; the flags continue to
represent a simple source -> model -> sink pipeline.

```hcl
pipeline "kernel" {
  model = "models/kernel.json"

  source "file" {
    filename = "target.log"
  }

  source "dmesg" {
    follow = true
  }

  source "systemd" {
    follow = true
    units = ["ssh.service"]
    identifiers = ["sshd"]
    priority = "warning"
    since = "today"
    line_format = "message"
  }

  sink "jsonl" {
    output = "out/parsed"
    include_parameters = true
    exclude_source = true
    batch_size = 10000
    batch_max_age = "5s"
  }

  sink "parquet" {
    output = "s3://logs/parsed"

    s3 {
      endpoint_env = "S3_ENDPOINT"
      region = "us-east-1"
      access_key_id_file = "/var/run/secrets/drain-s3/access_key_id"
      secret_access_key_file = "/var/run/secrets/drain-s3/secret_access_key"
      use_ssl_file = "/etc/drain-s3/use_ssl"
      path_style = true
    }
  }
}
```

```sh
go run ./cmd/cluster parse -config pipelines.hcl
```

Each pipeline loads its own model, runs its sources concurrently, and writes
each parsed record to every sink in that pipeline. Supported sources are
`file`, `dmesg`, and `systemd`. Supported sinks are `jsonl` and `parquet`;
`jsonl` may omit `output` to write to stdout, while `parquet` requires an
output prefix.

Matched lines include the template ID, model ID, source metadata, and
positional variables:

```jsonl
{"template_id":1,"model_id":"wK5I_oSM65L6xMlu04Dsx7S-e6fJBabRsHvSUoJs4Lg","source_kind":"file","source_name":"target.log","variables":["[Mon May 11 13:41:21 2026]","alice"]}
{"template_id":null,"model_id":"wK5I_oSM65L6xMlu04Dsx7S-e6fJBabRsHvSUoJs4Lg","source_kind":"file","source_name":"target.log","variables":[]}
```

To write files, pass `-output` as a local prefix. JSONL is still the default
format:

```sh
go run ./cmd/cluster parse -filename target.log -model model.json -output out/parsed
```

Parquet output is available with `-format parquet`:

```sh
go run ./cmd/cluster parse -filename target.log -model model.json -format parquet -output out/parsed
```

File output is written under partition-style paths:

```text
out/parsed/format=jsonl/run_id=<run-id>/part-00000.jsonl
out/parsed/format=parquet/run_id=<run-id>/part-00000.parquet
```

Parts rotate after `-batch-size` rows, default `10000`, or when a non-empty part
reaches `-batch-max-age`, default `5s`. Remaining rows are flushed when parsing
finishes.

Parquet columns are `template_id`, `model_id`, `source_kind`, `source_name`,
and `variables` by default. Pass `-include-parameters` to add typed parameters
to output: JSONL emits the `parameters` field, and Parquet adds a `parameters`
column. `variables` is a list of strings, and `parameters` is a list of structs
with `value` and `mask_name` fields.

Pass `-exclude-source`, or set `exclude_source = true` on an HCL sink, to omit
`source_kind` and `source_name` from that sink's JSONL or Parquet output.

Variables are extracted left to right from wildcard tokens. Masked values, such
as the bracketed timestamp prefix, are preserved as one variable even when they
contain spaces.

When a model contains Drain3-style named masks, `-include-parameters` emits
typed parameters while preserving `variables`:

```jsonl
{"template_id":1,"model_id":"wK5I_oSM65L6xMlu04Dsx7S-e6fJBabRsHvSUoJs4Lg","source_kind":"file","source_name":"target.log","variables":["123","42","retry"],"parameters":[{"value":"123","mask_name":"NUM"},{"value":"42","mask_name":"NUM"},{"value":"retry","mask_name":"*"}]}
```

S3-compatible storage uses `s3://bucket/prefix` output prefixes. Configure the
client with env vars:

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

Supported env names are `S3_ENDPOINT` or `AWS_ENDPOINT_URL`, `S3_REGION` or
`AWS_REGION` or `AWS_DEFAULT_REGION`, `S3_ACCESS_KEY_ID` or
`AWS_ACCESS_KEY_ID`, `S3_SECRET_ACCESS_KEY` or `AWS_SECRET_ACCESS_KEY`,
`S3_SESSION_TOKEN` or `AWS_SESSION_TOKEN`, `S3_USE_SSL`, and `S3_PATH_STYLE`.
The region defaults to `us-east-1`, and path-style bucket lookup defaults to
true.

For Kubernetes Secrets mounted as files, use matching `*_FILE` env vars:

```sh
export S3_ENDPOINT_FILE=/var/run/secrets/drain-s3/endpoint
export S3_ACCESS_KEY_ID_FILE=/var/run/secrets/drain-s3/access_key_id
export S3_SECRET_ACCESS_KEY_FILE=/var/run/secrets/drain-s3/secret_access_key

go run ./cmd/cluster parse -filename target.log -model model.json -output s3://logs/parsed
```

Secret file contents are trimmed. The supported file env names are
`S3_ENDPOINT_FILE` or `AWS_ENDPOINT_URL_FILE`, `S3_REGION_FILE` or
`AWS_REGION_FILE` or `AWS_DEFAULT_REGION_FILE`, `S3_ACCESS_KEY_ID_FILE` or
`AWS_ACCESS_KEY_ID_FILE`, `S3_SECRET_ACCESS_KEY_FILE` or
`AWS_SECRET_ACCESS_KEY_FILE`, `S3_SESSION_TOKEN_FILE` or
`AWS_SESSION_TOKEN_FILE`, `S3_USE_SSL_FILE`, and `S3_PATH_STYLE_FILE`.
Matching CLI flags are also available, such as `-s3-access-key-id-file` and
`-s3-secret-access-key-file`.

In HCL `s3` blocks, every S3 field supports a direct value, a mounted
ConfigMap/Secret file, or an explicit env var reference. For example, use one
of `endpoint`, `endpoint_file`, or `endpoint_env`; the same pattern is
available for `region`, `access_key_id`, `secret_access_key`, `session_token`,
`use_ssl`, and `path_style`. Mounted file contents are trimmed, and boolean
file/env values use Go boolean parsing. If an HCL S3 field is omitted, the
standard S3/AWS env vars and `*_FILE` env vars remain the fallback.

After successfully parsing the whole file, parse writes a throughput trace to
stderr so stdout remains valid JSONL:

```text
time=2026-05-25T12:00:00.000-04:00 level=INFO msg=parse_trace event=finished filename=target.log lines=2 bytes=42 duration_seconds=0.000123 lines_per_second=16260.16 bytes_per_second=341463.41
```

## Benchmarks

The cluster benchmarks cover the hot paths used by `train`, `test`, and `parse`:
restoring saved templates, matching log lines, preserving masked variables, and
the end-to-end parse loop.

Run all benchmarks with allocation counts:

```sh
go test ./... -bench=. -benchmem
```

Run only the cluster command benchmarks:

```sh
go test ./cmd/cluster -bench=Cluster -benchmem
```
