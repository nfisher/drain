package main

import (
	"errors"
	"fmt"
	"io"
	"os"
)

var (
	buildVersion = "dev"
	buildCommit  = "dev"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	if len(args) == 0 {
		printUsage(stderr)
		return errors.New("missing subcommand")
	}

	switch args[0] {
	case "train":
		return runTrain(args[1:], stdout)
	case "test":
		return runTest(args[1:], stdout)
	case "parse":
		return runParse(args[1:], stdout, stderr)
	case "version", "--version", "-version":
		return runVersion(stdout)
	case "-h", "--help", "help":
		printUsage(stdout)
		return nil
	default:
		printUsage(stderr)
		return fmt.Errorf("unknown subcommand %q", args[0])
	}
}

func printUsage(w io.Writer) {
	fmt.Fprintln(w, "usage:")
	fmt.Fprintln(w, "  cluster train [-update] [-metadata <metadata.json>] [-masking-rules <rules.json>] [-sim-th <0..1>] [-depth <n>] [-max-children <n>] [-parametrize-numeric-tokens=<bool>] [-extra-delimiter <value>]... -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster test  -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster parse [-source file|dmesg|systemd] [-checkpoint <state.json>] [-follow] [-dmesg-kmsg-path <path>] [-format jsonl|parquet] [-include-parameters] [-exclude-source] [-output <prefix|s3://bucket/prefix>] [-batch-size <n>] [-batch-max-age <duration>] [-metrics-listen-address <addr>] -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster parse -generate-config [-source file|dmesg|systemd] [-checkpoint <state.json>] [-follow] [-dmesg-kmsg-path <path>] [-format jsonl|parquet] [-include-parameters] [-exclude-source] [-output <prefix|s3://bucket/prefix>] [-batch-size <n>] [-batch-max-age <duration>] [-metrics-listen-address <addr>] -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster parse -config <pipelines.hcl> [-metrics-listen-address <addr>]")
	fmt.Fprintln(w, "  cluster version")
}

func runVersion(stdout io.Writer) error {
	fmt.Fprintf(stdout, "version: %s\ncommit: %s\n", buildVersion, buildCommit)
	return nil
}
