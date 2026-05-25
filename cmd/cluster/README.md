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

The model contains the command's Drain config, masking rules, and sorted
templates with IDs, sizes, template strings, and token lists. The timestamp
prefix masking rule is enabled by default, and the cluster depth is set to `6`.

To update an existing model with additional logs, pass `-update`:

```sh
go run ./cmd/cluster train -update -filename new.log -model model.json
```

Incremental training restores the saved templates into Drain before training the
new file. Existing template IDs and sizes are preserved, matching new lines
update those templates, and newly discovered templates receive IDs after the
highest restored ID.

## Test

Test loads a model and reports how many lines in a target file match each
template:

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

After successfully parsing the whole file, parse writes a throughput trace to
stderr so stdout remains valid JSONL:

```text
time=2026-05-25T12:00:00.000-04:00 level=INFO msg=parse_trace event=finished filename=target.log lines=2 bytes=42 duration_seconds=0.000123 lines_per_second=16260.16 bytes_per_second=341463.41
```
