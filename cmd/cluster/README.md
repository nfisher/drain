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
sorted templates with IDs, sizes, template strings, and token lists. The
timestamp prefix masking rule is enabled by default, the cluster depth is set to
`6`, `max_children` is set to `100`, numeric tokens are parameterized, and the
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
      "template": "<*> user <*> logged in",
      "count": 2
    }
  ]
}
```

## Parse

Parse loads a model and emits one JSON object per input line:
Like `test`, it uses perfect fallback template matching rather than the
training similarity threshold.

```sh
go run ./cmd/cluster parse -filename target.log -model model.json
```

Matched lines include the template ID and positional variables:

```jsonl
{"template_id":1,"variables":["[Mon May 11 13:41:21 2026]","alice"]}
{"template_id":null,"variables":[]}
```

Variables are extracted left to right from wildcard tokens. Masked values, such
as the bracketed timestamp prefix, are preserved as one variable even when they
contain spaces.

When a model contains Drain3-style named masks, parse also emits typed
parameters while preserving `variables`:

```jsonl
{"template_id":1,"variables":["123","42","retry"],"parameters":[{"value":"123","mask_name":"NUM"},{"value":"42","mask_name":"NUM"},{"value":"retry","mask_name":"*"}]}
```

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
