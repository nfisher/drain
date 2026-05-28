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

## LICENSE

[MIT](LICENSE)
