package main

import (
	"bufio"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"math"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/faceair/drain"
	"github.com/faceair/drain/internal/parseio"
)

const (
	modelVersion           = 1
	timestampPrefixPattern = `^\[[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-3]?[0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [0-9]{4}\]`
	parseErrorLineMaxBytes = 512
)

var (
	buildVersion = "dev"
	buildCommit  = "dev"
)

type modelFile struct {
	Version                  int                        `json:"version"`
	ModelID                  string                     `json:"-"`
	ParamString              string                     `json:"param_string"`
	SimTh                    *float64                   `json:"sim_th,omitempty"`
	LogClusterDepth          *int                       `json:"log_cluster_depth,omitempty"`
	MaxChildren              *int                       `json:"max_children,omitempty"`
	ParametrizeNumericTokens *bool                      `json:"parametrize_numeric_tokens,omitempty"`
	ExtraDelimiters          []string                   `json:"extra_delimiters,omitempty"`
	Metadata                 map[string]json.RawMessage `json:"metadata,omitempty"`
	MaskingRules             []modelMaskingRule         `json:"masking_rules"`
	Templates                []templateModel            `json:"templates"`
}

type modelMaskingRule struct {
	Pattern      string `json:"pattern,omitempty"`
	RegexPattern string `json:"regex_pattern,omitempty"`
	MaskWith     string `json:"mask_with,omitempty"`
	Replacement  string `json:"replacement,omitempty"`
}

type templateModel struct {
	ID       int      `json:"id"`
	Size     int      `json:"size"`
	Template string   `json:"template"`
	Tokens   []string `json:"tokens"`
}

type canonicalTemplateModel struct {
	ID       int      `json:"id"`
	Size     int      `json:"size"`
	Template string   `json:"template"`
	Tokens   []string `json:"tokens"`
}

type templateDistribution struct {
	TemplateID int    `json:"template_id"`
	ModelID    string `json:"model_id"`
	Template   string `json:"template"`
	Count      int    `json:"count"`
}

type testOutput struct {
	Total     int                    `json:"total"`
	Matched   int                    `json:"matched"`
	Unmatched int                    `json:"unmatched"`
	Templates []templateDistribution `json:"templates"`
}

type parseOutput struct {
	TemplateID *int                       `json:"template_id"`
	ModelID    string                     `json:"model_id"`
	SourceKind string                     `json:"source_kind,omitempty"`
	SourceName string                     `json:"source_name,omitempty"`
	Variables  []string                   `json:"variables"`
	Parameters []drain.ExtractedParameter `json:"parameters,omitempty"`
}

type compiledMaskingRule struct {
	regex       *regexp.Regexp
	replacement string
}

type lineToken struct {
	value     string
	rawString string
}

type parseTemplate struct {
	id         int
	template   string
	tokens     []string
	paramCount int
}

type extraDelimiterFlags []string

func (f *extraDelimiterFlags) String() string {
	return strings.Join(*f, ",")
}

func (f *extraDelimiterFlags) Set(value string) error {
	if value == "" {
		return errors.New("extra delimiter must not be empty")
	}
	*f = append(*f, value)
	return nil
}

type repeatedStringFlags []string

func (f *repeatedStringFlags) String() string {
	return strings.Join(*f, ",")
}

func (f *repeatedStringFlags) Set(value string) error {
	if value == "" {
		return errors.New("value must not be empty")
	}
	*f = append(*f, value)
	return nil
}

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
	fmt.Fprintln(w, "  cluster parse [-source file|dmesg|systemd] [-follow] [-format jsonl|parquet] [-include-parameters] [-exclude-source] [-output <prefix|s3://bucket/prefix>] [-batch-size <n>] [-batch-max-age <duration>] [-metrics-listen-address <addr>] -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster parse -generate-config [-source file|dmesg|systemd] [-follow] [-format jsonl|parquet] [-include-parameters] [-exclude-source] [-output <prefix|s3://bucket/prefix>] [-batch-size <n>] [-batch-max-age <duration>] [-metrics-listen-address <addr>] -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster parse -config <pipelines.hcl> [-metrics-listen-address <addr>]")
	fmt.Fprintln(w, "  cluster version")
}

func runVersion(stdout io.Writer) error {
	fmt.Fprintf(stdout, "version: %s\ncommit: %s\n", buildVersion, buildCommit)
	return nil
}

func runTrain(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("train", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	filename := fs.String("filename", "example.log", "training log file")
	modelPath := fs.String("model", "model.json", "model output path")
	update := fs.Bool("update", false, "load and update the existing model")
	metadataPath := fs.String("metadata", "", "metadata JSON object to merge into the model")
	maskingRulesPath := fs.String("masking-rules", "", "masking rules JSON array to use instead of defaults")
	defaultConfig := clusterConfig()
	simTh := fs.Float64("sim-th", defaultConfig.SimTh, "training similarity threshold")
	depth := fs.Int("depth", defaultConfig.LogClusterDepth, "max depth levels of log clusters")
	maxChildren := fs.Int("max-children", defaultConfig.MaxChildren, "max number of children of an internal node")
	parametrizeNumericTokens := fs.Bool("parametrize-numeric-tokens", !defaultConfig.PreserveNumericTokens, "treat tokens containing digits as template parameters")
	var extraDelimiters extraDelimiterFlags
	fs.Var(&extraDelimiters, "extra-delimiter", "literal delimiter to split on after masking; repeat for multiple delimiters")
	if err := fs.Parse(args); err != nil {
		return err
	}
	simThProvided := flagWasProvided(fs, "sim-th")
	depthProvided := flagWasProvided(fs, "depth")
	maxChildrenProvided := flagWasProvided(fs, "max-children")
	parametrizeNumericTokensProvided := flagWasProvided(fs, "parametrize-numeric-tokens")
	extraDelimitersProvided := flagWasProvided(fs, "extra-delimiter")
	maskingRulesProvided := flagWasProvided(fs, "masking-rules")
	if err := validateSimTh("sim-th", *simTh); err != nil {
		return err
	}
	if err := validateDepth("depth", *depth); err != nil {
		return err
	}
	if err := validateMaxChildren("max-children", *maxChildren); err != nil {
		return err
	}
	if err := validateExtraDelimiters("extra-delimiter", extraDelimiters); err != nil {
		return err
	}
	var maskingRulesFromFile []modelMaskingRule
	if maskingRulesProvided {
		var err error
		maskingRulesFromFile, err = readMaskingRulesFile(*maskingRulesPath)
		if err != nil {
			return err
		}
	}

	config := defaultConfig
	config.SimTh = *simTh
	config.LogClusterDepth = *depth
	config.MaxChildren = *maxChildren
	config.PreserveNumericTokens = !*parametrizeNumericTokens
	if extraDelimitersProvided {
		config.ExtraDelimiters = copyStrings(extraDelimiters)
	}
	if maskingRulesProvided {
		config.MaskingRules = drainMaskingRules(maskingRulesFromFile)
	}
	logger := drain.New(config)
	var metadata map[string]json.RawMessage
	if *update {
		existingModel, _, err := readModel(*modelPath)
		if err != nil {
			return err
		}
		metadata = copyMetadata(existingModel.Metadata)
		config = configFromModel(existingModel)
		if simThProvided {
			config.SimTh = *simTh
		}
		if depthProvided {
			config.LogClusterDepth = *depth
		}
		if maxChildrenProvided {
			config.MaxChildren = *maxChildren
		}
		if parametrizeNumericTokensProvided {
			config.PreserveNumericTokens = !*parametrizeNumericTokens
		}
		if extraDelimitersProvided {
			config.ExtraDelimiters = copyStrings(extraDelimiters)
		}
		if maskingRulesProvided {
			config.MaskingRules = drainMaskingRules(maskingRulesFromFile)
		}
		logger = drain.New(config)
		if err := logger.LoadClusters(snapshotsFromModel(existingModel)); err != nil {
			return err
		}
	}

	var metadataFromFile map[string]json.RawMessage
	if *metadataPath != "" {
		var err error
		metadataFromFile, err = readMetadataFile(*metadataPath)
		if err != nil {
			return err
		}
	}

	if err := scanLines(*filename, func(line string) error {
		logger.Train(line)
		return nil
	}); err != nil {
		return err
	}

	model := modelFromDrain(config, logger)
	model.Metadata = metadataWithTimestamps(metadata, metadataFromFile, *update, time.Now().UTC())
	if err := writeModel(*modelPath, model); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "wrote %d templates to %s\n", len(model.Templates), *modelPath)
	return nil
}

func flagWasProvided(fs *flag.FlagSet, name string) bool {
	provided := false
	fs.Visit(func(f *flag.Flag) {
		if f.Name == name {
			provided = true
		}
	})
	return provided
}

func runTest(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("test", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	filename := fs.String("filename", "example.log", "target log file")
	modelPath := fs.String("model", "model.json", "model path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	model, _, err := readModel(*modelPath)
	if err != nil {
		return err
	}
	logger, err := drainFromModel(model)
	if err != nil {
		return err
	}

	counts := make(map[int]int, len(model.Templates))
	for _, template := range model.Templates {
		counts[template.ID] = 0
	}

	output := testOutput{
		Templates: make([]templateDistribution, 0, len(model.Templates)),
	}
	if err := scanLines(*filename, func(line string) error {
		output.Total++
		cluster := logger.MatchWithOptions(line, drain.MatchOptions{
			FullSearchStrategy: drain.FullSearchFallback,
		})
		if cluster == nil {
			output.Unmatched++
			return nil
		}
		output.Matched++
		counts[cluster.ID()]++
		return nil
	}); err != nil {
		return err
	}

	for _, template := range model.Templates {
		output.Templates = append(output.Templates, templateDistribution{
			TemplateID: template.ID,
			ModelID:    model.ModelID,
			Template:   template.Template,
			Count:      counts[template.ID],
		})
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(output)
}

func runParse(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("parse", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configPath := fs.String("config", "", "HCL parse pipeline config")
	generateConfig := fs.Bool("generate-config", false, "write equivalent HCL parse pipeline config to stdout and exit")
	sourceKind := fs.String("source", "file", "input source: file, dmesg, or systemd")
	follow := fs.Bool("follow", false, "follow streaming input sources")
	filename := fs.String("filename", "example.log", "target log file")
	modelPath := fs.String("model", "model.json", "model path")
	outputFormat := fs.String("format", parseFormatJSONL, "output format: jsonl or parquet")
	includeParameters := fs.Bool("include-parameters", false, "include parsed parameters in output")
	excludeSource := fs.Bool("exclude-source", false, "exclude source_kind and source_name from output")
	outputPrefix := fs.String("output", "", "output prefix; local path or s3://bucket/prefix")
	batchSize := fs.Int("batch-size", defaultParseBatchSize, "rows per output part")
	batchMaxAge := fs.Duration("batch-max-age", defaultParseBatchMaxAge, "maximum age of a non-empty output part")
	metricsListenAddress := fs.String("metrics-listen-address", "", "Prometheus metrics listen address; disabled when empty")
	s3Endpoint := fs.String("s3-endpoint", "", "S3-compatible endpoint")
	s3EndpointFile := fs.String("s3-endpoint-file", "", "file containing S3-compatible endpoint")
	s3Region := fs.String("s3-region", "", "S3 region")
	s3RegionFile := fs.String("s3-region-file", "", "file containing S3 region")
	s3AccessKeyID := fs.String("s3-access-key-id", "", "S3 access key ID")
	s3AccessKeyIDFile := fs.String("s3-access-key-id-file", "", "file containing S3 access key ID")
	s3SecretAccessKey := fs.String("s3-secret-access-key", "", "S3 secret access key")
	s3SecretAccessKeyFile := fs.String("s3-secret-access-key-file", "", "file containing S3 secret access key")
	s3SessionToken := fs.String("s3-session-token", "", "S3 session token")
	s3SessionTokenFile := fs.String("s3-session-token-file", "", "file containing S3 session token")
	s3UseSSL := fs.Bool("s3-use-ssl", false, "use TLS for S3 requests")
	s3UseSSLFile := fs.String("s3-use-ssl-file", "", "file containing whether to use TLS for S3 requests")
	s3PathStyle := fs.Bool("s3-path-style", false, "use path-style S3 bucket lookup")
	s3PathStyleFile := fs.String("s3-path-style-file", "", "file containing whether to use path-style S3 bucket lookup")
	systemdFollow := fs.Bool("systemd-follow", false, "continue reading new systemd journal entries after historical entries")
	var systemdUnits repeatedStringFlags
	fs.Var(&systemdUnits, "systemd-unit", "systemd unit filter; repeat for multiple units")
	var systemdIdentifiers repeatedStringFlags
	fs.Var(&systemdIdentifiers, "systemd-identifier", "systemd syslog identifier filter; repeat for multiple identifiers")
	systemdPriority := fs.String("systemd-priority", "", "systemd journal priority filter")
	systemdSince := fs.String("systemd-since", "", "systemd journal start time")
	systemdUntil := fs.String("systemd-until", "", "systemd journal end time")
	systemdBoot := fs.String("systemd-boot", "", "systemd boot filter")
	systemdAfterCursor := fs.String("systemd-after-cursor", "", "systemd journal cursor to resume after")
	systemdLineFormat := fs.String("systemd-line-format", parseio.SystemdLineFormatMessage, "systemd line format: message, short, or json")
	if err := fs.Parse(args); err != nil {
		return err
	}
	telemetry := parseTelemetryOptions{
		MetricsListenAddress: *metricsListenAddress,
	}
	if flagWasProvided(fs, "config") {
		if strings.TrimSpace(*configPath) == "" {
			return errors.New("config path must not be empty")
		}
		if err := validateParseConfigFlagExclusivity(fs); err != nil {
			return err
		}
		config, err := readParseConfig(*configPath)
		if err != nil {
			return err
		}
		if flagWasProvided(fs, "metrics-listen-address") {
			config.Telemetry = telemetry
		}
		ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
		defer stop()
		metrics, err := startMetricsServer(config.Telemetry)
		if err != nil {
			return err
		}
		defer func() {
			_ = metrics.Close(context.Background())
		}()
		return runParsePipelines(ctx, stdout, stderr, config.Pipelines)
	}
	if err := validateParseOutputOptions(*outputFormat, *batchSize, *batchMaxAge); err != nil {
		return err
	}
	if err := validateParseSourceFlags(fs, *sourceKind); err != nil {
		return err
	}
	sourceOpts := parseSourceOptions{
		Kind:     *sourceKind,
		Filename: *filename,
		Follow:   *follow,
		Systemd: parseio.SystemdOptions{
			Follow:      *systemdFollow,
			Units:       copyStrings(systemdUnits),
			Identifiers: copyStrings(systemdIdentifiers),
			Priority:    *systemdPriority,
			Since:       *systemdSince,
			Until:       *systemdUntil,
			Boot:        *systemdBoot,
			AfterCursor: *systemdAfterCursor,
			LineFormat:  *systemdLineFormat,
		},
	}
	s3Opts := parseio.S3Options{
		Endpoint:            stringFlagValue(fs, "s3-endpoint", *s3Endpoint),
		EndpointFile:        stringFlagValue(fs, "s3-endpoint-file", *s3EndpointFile),
		Region:              stringFlagValue(fs, "s3-region", *s3Region),
		RegionFile:          stringFlagValue(fs, "s3-region-file", *s3RegionFile),
		AccessKeyID:         stringFlagValue(fs, "s3-access-key-id", *s3AccessKeyID),
		AccessKeyIDFile:     stringFlagValue(fs, "s3-access-key-id-file", *s3AccessKeyIDFile),
		SecretAccessKey:     stringFlagValue(fs, "s3-secret-access-key", *s3SecretAccessKey),
		SecretAccessKeyFile: stringFlagValue(fs, "s3-secret-access-key-file", *s3SecretAccessKeyFile),
		SessionToken:        stringFlagValue(fs, "s3-session-token", *s3SessionToken),
		SessionTokenFile:    stringFlagValue(fs, "s3-session-token-file", *s3SessionTokenFile),
		UseSSL:              boolFlagValue(fs, "s3-use-ssl", *s3UseSSL),
		UseSSLFile:          stringFlagValue(fs, "s3-use-ssl-file", *s3UseSSLFile),
		PathStyle:           boolFlagValue(fs, "s3-path-style", *s3PathStyle),
		PathStyleFile:       stringFlagValue(fs, "s3-path-style-file", *s3PathStyleFile),
	}
	outputOpts := parseOutputOptions{
		Format:            *outputFormat,
		Prefix:            *outputPrefix,
		IncludeParameters: *includeParameters,
		ExcludeSource:     *excludeSource,
		BatchSize:         *batchSize,
		BatchMaxAge:       *batchMaxAge,
		S3:                s3Opts,
		Now:               time.Now,
	}
	if *generateConfig {
		return writeGeneratedParseConfig(stdout, fs, *modelPath, sourceOpts, outputOpts, telemetry)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()
	metrics, err := startMetricsServer(telemetry)
	if err != nil {
		return err
	}
	defer func() {
		_ = metrics.Close(context.Background())
	}()
	source, err := newParseSource(sourceOpts)
	if err != nil {
		return err
	}
	sourceInfo := source.Info()

	model, compiledRules, err := readModel(*modelPath)
	if err != nil {
		_ = source.Close(ctx)
		return err
	}
	processor, err := newParseProcessor(model, compiledRules)
	if err != nil {
		_ = source.Close(ctx)
		return err
	}

	sink, err := newParseSink(ctx, stdout, outputOpts)
	if err != nil {
		_ = source.Close(ctx)
		return err
	}

	parsedLines := 0
	var parsedBytes int64
	var record parseio.SourceRecord
	var output parseOutput
	started := time.Now()
	processErr := parseSourceRecords(ctx, source, processor, sink, &record, &output, func(record parseio.SourceRecord) {
		parsedLines++
		parsedBytes += record.Bytes
	})
	closeCtx := context.Background()
	sinkCloseErr := sink.Close(closeCtx)
	sourceCloseErr := source.Close(closeCtx)
	if processErr != nil {
		return processErr
	}
	if sinkCloseErr != nil {
		return sinkCloseErr
	}
	if sourceCloseErr != nil {
		return sourceCloseErr
	}
	traceParseSpeed(stderr, sourceInfo, parsedLines, sourceTraceBytes(sourceInfo, parsedBytes), time.Since(started))
	return nil
}

func validateParseConfigFlagExclusivity(fs *flag.FlagSet) error {
	var conflicts []string
	fs.Visit(func(f *flag.Flag) {
		if f.Name == "config" || f.Name == "metrics-listen-address" {
			return
		}
		conflicts = append(conflicts, "-"+f.Name)
	})
	if len(conflicts) > 0 {
		sort.Strings(conflicts)
		return fmt.Errorf("-config cannot be combined with %s", strings.Join(conflicts, ", "))
	}
	return nil
}

type parseSourceOptions struct {
	Kind     string
	Filename string
	Follow   bool
	Systemd  parseio.SystemdOptions
}

var newDmesgParseSource = func(follow bool) (parseio.Source, error) {
	return parseio.NewDmesgSource(follow)
}

var newSystemdParseSource = func(options parseio.SystemdOptions) (parseio.Source, error) {
	return parseio.NewSystemdSource(options)
}

func newParseSource(opts parseSourceOptions) (parseio.Source, error) {
	switch opts.Kind {
	case "", "file":
		if opts.Follow {
			return nil, fmt.Errorf("source %q does not support -follow", "file")
		}
		return parseio.NewFileSource(opts.Filename)
	case "dmesg":
		return newDmesgParseSource(opts.Follow)
	case "systemd":
		systemdOptions := opts.Systemd
		systemdOptions.Follow = systemdOptions.Follow || opts.Follow
		return newSystemdParseSource(systemdOptions)
	default:
		return nil, fmt.Errorf("source %q is not supported yet", opts.Kind)
	}
}

func validateParseSourceFlags(fs *flag.FlagSet, sourceKind string) error {
	if sourceKind == "" {
		sourceKind = "file"
	}
	if sourceKind == "systemd" {
		if flagWasProvided(fs, "filename") {
			return errors.New("-filename is only supported with -source file")
		}
		return nil
	}
	for _, name := range systemdParseFlagNames() {
		if flagWasProvided(fs, name) {
			return errors.New("systemd flags require -source systemd")
		}
	}
	return nil
}

func systemdParseFlagNames() []string {
	return []string{
		"systemd-follow",
		"systemd-unit",
		"systemd-identifier",
		"systemd-priority",
		"systemd-since",
		"systemd-until",
		"systemd-boot",
		"systemd-after-cursor",
		"systemd-line-format",
	}
}

func parseSourceRecords(ctx context.Context, source parseio.Source, processor *parseProcessor, sink parseSink, record *parseio.SourceRecord, output *parseOutput, onRecord func(parseio.SourceRecord)) error {
	sourceInfo := source.Info()
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		ok, err := source.Next(ctx, record)
		if err != nil {
			return err
		}
		if !ok {
			return nil
		}
		if err := processor.Parse(record.Line, output); err != nil {
			return parseRecordError(sourceInfo, *record, err)
		}
		output.SourceKind = sourceInfo.Kind
		output.SourceName = sourceInfo.Name
		if err := sink.Write(ctx, *output); err != nil {
			return err
		}
		if err := source.Ack(ctx); err != nil {
			return err
		}
		if onRecord != nil {
			onRecord(*record)
		}
	}
}

func parseRecordError(sourceInfo parseio.SourceInfo, record parseio.SourceRecord, err error) error {
	var message strings.Builder
	message.WriteString("parse")
	if sourceInfo.Kind != "" {
		message.WriteString(" ")
		message.WriteString(sourceInfo.Kind)
	}
	if sourceInfo.Name != "" {
		message.WriteString(" ")
		message.WriteString(sourceInfo.Name)
	}
	locator := sourceRecordLocator(record)
	for _, key := range sortedLocatorKeys(locator) {
		message.WriteString(" ")
		message.WriteString(key)
		message.WriteString("=")
		message.WriteString(locator[key])
	}
	return fmt.Errorf("%s: %w: line=%s", message.String(), err, quotedLinePreview(record.Line))
}

func sourceRecordLocator(record parseio.SourceRecord) map[string]string {
	if len(record.Locator) == 0 && record.LineNumber == 0 {
		return nil
	}
	locator := make(map[string]string, len(record.Locator)+2)
	for key, value := range record.Locator {
		locator[key] = value
	}
	if record.LineNumber > 0 {
		if _, ok := locator["line"]; !ok {
			locator["line"] = fmt.Sprint(record.LineNumber)
		}
		if _, ok := locator["byte"]; !ok {
			locator["byte"] = fmt.Sprint(record.ByteOffset)
		}
	}
	return locator
}

func sortedLocatorKeys(locator map[string]string) []string {
	if len(locator) == 0 {
		return nil
	}
	keys := make([]string, 0, len(locator))
	for _, key := range []string{"line", "byte"} {
		if _, ok := locator[key]; ok {
			keys = append(keys, key)
		}
	}
	genericKeys := make([]string, 0, len(locator)-len(keys))
	for key := range locator {
		if key == "line" || key == "byte" {
			continue
		}
		genericKeys = append(genericKeys, key)
	}
	sort.Strings(genericKeys)
	keys = append(keys, genericKeys...)
	return keys
}

func quotedLinePreview(line string) string {
	truncated := false
	if len(line) > parseErrorLineMaxBytes {
		line = line[:parseErrorLineMaxBytes]
		truncated = true
	}
	encoded, _ := json.Marshal(line)
	if truncated {
		return string(encoded) + " (truncated)"
	}
	return string(encoded)
}

func clusterConfig() *drain.Config {
	config := drain.DefaultConfig()
	config.MaskingRules = defaultMaskingRules()
	config.LogClusterDepth = 6
	return config
}

func defaultMaskingRules() []drain.MaskingRule {
	return []drain.MaskingRule{
		{Pattern: timestampPrefixPattern},
		{Pattern: `\b(?:[0-9a-f]{2,}:){3,}[0-9a-f]{2,}\b`, MaskWith: "ID"},
		{Pattern: `\b\d{1,3}(?:\.\d{1,3}){3}\b`, MaskWith: "IP"},
		{Pattern: `\b[0-9a-f]{6,}(?:\s+[0-9a-f]{6,}){2,}\b`, MaskWith: "SEQ"},
		{Pattern: `\b[0-9A-F]{4}(?:\s+[0-9A-F]{4}){3,}\b`, MaskWith: "SEQ"},
		{Pattern: `\b0x[a-f0-9A-F]+\b`, MaskWith: "HEX"},
		{Pattern: `[-+]?\b\d+\b`, MaskWith: "NUM"},
	}
}

func modelMaskingRules(rules []drain.MaskingRule) []modelMaskingRule {
	modelRules := make([]modelMaskingRule, 0, len(rules))
	for _, rule := range rules {
		modelRules = append(modelRules, modelMaskingRule{
			Pattern:     rule.Pattern,
			MaskWith:    rule.MaskWith,
			Replacement: rule.Replacement,
		})
	}
	return modelRules
}

func splitTemplate(template string) []string {
	return strings.Fields(template)
}

func templateTokens(template templateModel) []string {
	if len(template.Tokens) > 0 {
		return template.Tokens
	}
	if template.Template != "" {
		return splitTemplate(template.Template)
	}
	return nil
}

func parseTemplatesFromModel(model modelFile) (map[int]parseTemplate, int) {
	templates := make(map[int]parseTemplate, len(model.Templates))
	maxParamCount := 0
	for _, template := range model.Templates {
		tokens := templateTokens(template)
		paramCount := countParams(model.ParamString, tokens)
		if paramCount > maxParamCount {
			maxParamCount = paramCount
		}
		templates[template.ID] = parseTemplate{
			id:         template.ID,
			template:   strings.Join(tokens, " "),
			tokens:     tokens,
			paramCount: paramCount,
		}
	}
	return templates, maxParamCount
}

func countParams(paramString string, tokens []string) int {
	count := 0
	for _, token := range tokens {
		if token == paramString {
			count++
		}
	}
	return count
}

func modelFromDrain(config *drain.Config, logger *drain.Drain) modelFile {
	snapshots := logger.ClusterSnapshots()
	model := modelFile{
		Version:                  modelVersion,
		ParamString:              config.ParamString,
		SimTh:                    float64Pointer(config.SimTh),
		LogClusterDepth:          intPointer(config.LogClusterDepth),
		MaxChildren:              intPointer(config.MaxChildren),
		ParametrizeNumericTokens: boolPointer(!config.PreserveNumericTokens),
		ExtraDelimiters:          copyStrings(config.ExtraDelimiters),
		MaskingRules:             modelMaskingRules(config.MaskingRules),
		Templates:                make([]templateModel, 0, len(snapshots)),
	}
	for _, snapshot := range snapshots {
		tokens := make([]string, len(snapshot.TemplateTokens))
		copy(tokens, snapshot.TemplateTokens)
		model.Templates = append(model.Templates, templateModel{
			ID:       snapshot.ID,
			Size:     snapshot.Size,
			Template: strings.Join(tokens, " "),
			Tokens:   tokens,
		})
	}
	return model
}

func configFromModel(model modelFile) *drain.Config {
	config := clusterConfig()
	config.ParamString = model.ParamString
	if model.SimTh != nil {
		config.SimTh = *model.SimTh
	}
	if model.LogClusterDepth != nil {
		config.LogClusterDepth = *model.LogClusterDepth
	}
	if model.MaxChildren != nil {
		config.MaxChildren = *model.MaxChildren
	}
	if model.ParametrizeNumericTokens != nil {
		config.PreserveNumericTokens = !*model.ParametrizeNumericTokens
	}
	config.ExtraDelimiters = copyStrings(model.ExtraDelimiters)
	config.MaskingRules = drainMaskingRules(model.MaskingRules)
	return config
}

func float64Pointer(value float64) *float64 {
	return &value
}

func intPointer(value int) *int {
	return &value
}

func boolPointer(value bool) *bool {
	return &value
}

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	copied := make([]string, len(values))
	copy(copied, values)
	return copied
}

func readMetadataFile(path string) (map[string]json.RawMessage, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read metadata %s: %w", path, err)
	}

	var root any
	if err := json.Unmarshal(contents, &root); err != nil {
		return nil, fmt.Errorf("decode metadata %s: %w", path, err)
	}
	if _, ok := root.(map[string]any); !ok {
		return nil, fmt.Errorf("metadata file %s must contain a JSON object", path)
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(contents, &metadata); err != nil {
		return nil, fmt.Errorf("decode metadata %s: %w", path, err)
	}
	return metadata, nil
}

func readMaskingRulesFile(path string) ([]modelMaskingRule, error) {
	contents, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read masking rules %s: %w", path, err)
	}

	var root any
	if err := json.Unmarshal(contents, &root); err != nil {
		return nil, fmt.Errorf("decode masking rules %s: %w", path, err)
	}
	if _, ok := root.([]any); !ok {
		return nil, fmt.Errorf("masking rules file %s must contain a JSON array", path)
	}

	var rules []modelMaskingRule
	if err := json.Unmarshal(contents, &rules); err != nil {
		return nil, fmt.Errorf("decode masking rules %s: %w", path, err)
	}
	if err := normalizeMaskingRules(rules, "masking_rules"); err != nil {
		return nil, err
	}
	return rules, nil
}

func normalizeMaskingRules(rules []modelMaskingRule, name string) error {
	for i := range rules {
		if err := normalizeMaskingRule(&rules[i], fmt.Sprintf("%s[%d]", name, i)); err != nil {
			return err
		}
	}
	return nil
}

func normalizeMaskingRule(rule *modelMaskingRule, name string) error {
	if rule.Pattern != "" && rule.RegexPattern != "" && rule.Pattern != rule.RegexPattern {
		return fmt.Errorf("%s has conflicting pattern and regex_pattern", name)
	}
	if rule.Pattern == "" {
		rule.Pattern = rule.RegexPattern
	}
	rule.RegexPattern = ""
	if rule.Pattern == "" {
		return fmt.Errorf("%s pattern must not be empty", name)
	}
	if _, err := regexp.Compile(rule.Pattern); err != nil {
		return fmt.Errorf("compile %s pattern %q: %w", name, rule.Pattern, err)
	}
	return nil
}

func metadataWithTimestamps(existing, metadataFromFile map[string]json.RawMessage, update bool, now time.Time) map[string]json.RawMessage {
	metadata := copyMetadata(existing)
	mergeMetadata(metadata, metadataFromFile)

	timestamp := metadataString(now.UTC().Format(time.RFC3339))
	if update {
		if !hasValidMetadataTimestamp(metadata, "created_at") {
			metadata["created_at"] = timestamp
		}
		metadata["updated_at"] = timestamp
		return metadata
	}

	metadata["created_at"] = timestamp
	delete(metadata, "updated_at")
	return metadata
}

func copyMetadata(metadata map[string]json.RawMessage) map[string]json.RawMessage {
	if len(metadata) == 0 {
		return make(map[string]json.RawMessage)
	}
	copied := make(map[string]json.RawMessage, len(metadata))
	for key, value := range metadata {
		copied[key] = cloneRawMessage(value)
	}
	return copied
}

func mergeMetadata(dst, src map[string]json.RawMessage) {
	for key, value := range src {
		if isMetadataTimestampKey(key) {
			continue
		}
		dst[key] = cloneRawMessage(value)
	}
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	copied := make(json.RawMessage, len(value))
	copy(copied, value)
	return copied
}

func isMetadataTimestampKey(key string) bool {
	return key == "created_at" || key == "updated_at"
}

func metadataString(value string) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

func hasValidMetadataTimestamp(metadata map[string]json.RawMessage, key string) bool {
	var value string
	if err := json.Unmarshal(metadata[key], &value); err != nil {
		return false
	}
	_, err := time.Parse(time.RFC3339, value)
	return err == nil
}

func drainMaskingRules(rules []modelMaskingRule) []drain.MaskingRule {
	drainRules := make([]drain.MaskingRule, 0, len(rules))
	for _, rule := range rules {
		drainRules = append(drainRules, drain.MaskingRule{
			Pattern:     rule.Pattern,
			MaskWith:    rule.MaskWith,
			Replacement: rule.Replacement,
		})
	}
	return drainRules
}

func snapshotsFromModel(model modelFile) []drain.LogClusterSnapshot {
	snapshots := make([]drain.LogClusterSnapshot, 0, len(model.Templates))
	for _, template := range model.Templates {
		tokens := templateTokens(template)
		copiedTokens := make([]string, len(tokens))
		copy(copiedTokens, tokens)
		snapshots = append(snapshots, drain.LogClusterSnapshot{
			ID:             template.ID,
			Size:           template.Size,
			TemplateTokens: copiedTokens,
		})
	}
	return snapshots
}

func drainFromModel(model modelFile) (*drain.Drain, error) {
	logger := drain.New(configFromModel(model))
	if err := logger.LoadClusters(snapshotsFromModel(model)); err != nil {
		return nil, err
	}
	return logger, nil
}

func sortTemplates(templates []templateModel) {
	sort.Slice(templates, func(i, j int) bool {
		return templates[i].ID < templates[j].ID
	})
}

func modelIDFromTemplates(templates []templateModel) string {
	sortedTemplates := append([]templateModel(nil), templates...)
	sortTemplates(sortedTemplates)

	canonicalTemplates := make([]canonicalTemplateModel, 0, len(sortedTemplates))
	for _, template := range sortedTemplates {
		tokens := templateTokens(template)
		copiedTokens := make([]string, len(tokens))
		copy(copiedTokens, tokens)
		canonicalTemplates = append(canonicalTemplates, canonicalTemplateModel{
			ID:       template.ID,
			Size:     template.Size,
			Template: template.Template,
			Tokens:   copiedTokens,
		})
	}

	var encodedTemplates bytes.Buffer
	encoder := json.NewEncoder(&encodedTemplates)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(canonicalTemplates)
	digestInput := bytes.TrimSuffix(encodedTemplates.Bytes(), []byte("\n"))
	digest := sha256.Sum256(digestInput)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func writeModel(path string, model modelFile) error {
	file, err := os.Create(path)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(model)
}

func readModel(path string) (modelFile, []compiledMaskingRule, error) {
	file, err := os.Open(path)
	if err != nil {
		return modelFile{}, nil, err
	}
	defer file.Close()

	var model modelFile
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&model); err != nil {
		return modelFile{}, nil, err
	}
	if model.Version != modelVersion {
		return modelFile{}, nil, fmt.Errorf("unsupported model version %d", model.Version)
	}
	if model.ParamString == "" {
		return modelFile{}, nil, errors.New("model param_string must not be empty")
	}
	if model.SimTh != nil {
		if err := validateSimTh("model sim_th", *model.SimTh); err != nil {
			return modelFile{}, nil, err
		}
	}
	if model.LogClusterDepth != nil {
		if err := validateDepth("model log_cluster_depth", *model.LogClusterDepth); err != nil {
			return modelFile{}, nil, err
		}
	}
	if model.MaxChildren != nil {
		if err := validateMaxChildren("model max_children", *model.MaxChildren); err != nil {
			return modelFile{}, nil, err
		}
	}
	if err := validateExtraDelimiters("model extra_delimiters", model.ExtraDelimiters); err != nil {
		return modelFile{}, nil, err
	}
	if err := normalizeMaskingRules(model.MaskingRules, "model masking_rules"); err != nil {
		return modelFile{}, nil, err
	}
	sortTemplates(model.Templates)
	preserveLegacyLiteralMaskReplacements(&model)
	model.ModelID = modelIDFromTemplates(model.Templates)

	compiledRules, err := compileMaskingRules(model.MaskingRules, model.ParamString)
	if err != nil {
		return modelFile{}, nil, err
	}
	return model, compiledRules, nil
}

func validateSimTh(name string, value float64) error {
	if math.IsNaN(value) || value < 0 || value > 1 {
		return fmt.Errorf("%s must be between 0 and 1, got %g", name, value)
	}
	return nil
}

func validateDepth(name string, value int) error {
	if value < 3 {
		return fmt.Errorf("%s must be at least 3, got %d", name, value)
	}
	return nil
}

func validateMaxChildren(name string, value int) error {
	if value < 1 {
		return fmt.Errorf("%s must be at least 1, got %d", name, value)
	}
	return nil
}

func validateExtraDelimiters(name string, extraDelimiters []string) error {
	for i, extraDelimiter := range extraDelimiters {
		if extraDelimiter == "" {
			return fmt.Errorf("%s[%d] must not be empty", name, i)
		}
	}
	return nil
}

func parameterValues(parameters []drain.ExtractedParameter) []string {
	return appendParameterValues(make([]string, 0, len(parameters)), parameters)
}

func appendParameterValues(values []string, parameters []drain.ExtractedParameter) []string {
	values = values[:0]
	for _, parameter := range parameters {
		values = append(values, parameter.Value)
	}
	return values
}

func hasNamedParameters(parameters []drain.ExtractedParameter) bool {
	for _, parameter := range parameters {
		if parameter.MaskName != "*" {
			return true
		}
	}
	return false
}

func preserveLegacyLiteralMaskReplacements(model *modelFile) {
	for i := range model.MaskingRules {
		rule := &model.MaskingRules[i]
		if rule.Replacement != "" || rule.MaskWith == "" || isExplicitMaskReplacement(rule.MaskWith) {
			continue
		}
		if modelUsesMaskToken(*model, namedMaskToken(rule.MaskWith)) {
			continue
		}
		if modelUsesMaskToken(*model, rule.MaskWith) {
			rule.Replacement = rule.MaskWith
		}
	}
}

func modelUsesMaskToken(model modelFile, token string) bool {
	for _, template := range model.Templates {
		for _, templateToken := range templateTokens(template) {
			if tokenAppearsAsMaskReplacement(templateToken, token) {
				return true
			}
		}
	}
	return false
}

func tokenAppearsAsMaskReplacement(templateToken, token string) bool {
	if isExplicitMaskReplacement(token) {
		return strings.Contains(templateToken, token)
	}
	for searchOffset := 0; searchOffset < len(templateToken); {
		index := strings.Index(templateToken[searchOffset:], token)
		if index < 0 {
			return false
		}
		start := searchOffset + index
		end := start + len(token)
		if hasReplacementBoundary(templateToken, start, end) {
			return true
		}
		searchOffset = end
	}
	return false
}

func hasReplacementBoundary(value string, start, end int) bool {
	if start > 0 {
		r, _ := utf8.DecodeLastRuneInString(value[:start])
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return false
		}
	}
	if end < len(value) {
		r, _ := utf8.DecodeRuneInString(value[end:])
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return false
		}
	}
	return true
}

func compileMaskingRules(rules []modelMaskingRule, defaultReplacement string) ([]compiledMaskingRule, error) {
	compiled := make([]compiledMaskingRule, 0, len(rules))
	for _, rule := range rules {
		if rule.Pattern == "" {
			return nil, errors.New("masking rule pattern must not be empty")
		}
		regex, err := regexp.Compile(rule.Pattern)
		if err != nil {
			return nil, fmt.Errorf("compile masking rule %q: %w", rule.Pattern, err)
		}
		replacement := maskingRuleReplacement(rule, defaultReplacement)
		compiled = append(compiled, compiledMaskingRule{
			regex:       regex,
			replacement: replacement,
		})
	}
	return compiled, nil
}

func maskingRuleReplacement(rule modelMaskingRule, defaultReplacement string) string {
	if rule.Replacement != "" {
		return rule.Replacement
	}
	if rule.MaskWith == "" {
		return defaultReplacement
	}
	if isExplicitMaskReplacement(rule.MaskWith) {
		return rule.MaskWith
	}
	return namedMaskToken(rule.MaskWith)
}

func isExplicitMaskReplacement(maskWith string) bool {
	return strings.ContainsAny(maskWith, "<>$")
}

func namedMaskToken(maskName string) string {
	return "<:" + maskName + ":>"
}

func scanLines(filename string, handle func(string) error) error {
	file, err := os.Open(filename)
	if err != nil {
		return err
	}
	defer file.Close()

	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		if err := handle(scanner.Text()); err != nil {
			return err
		}
	}
	return scanner.Err()
}

func sourceTraceBytes(info parseio.SourceInfo, fallback int64) int64 {
	if info.SizeBytes != nil {
		return *info.SizeBytes
	}
	return fallback
}

func traceParseSpeed(w io.Writer, sourceInfo parseio.SourceInfo, lines int, bytes int64, elapsed time.Duration) {
	traceParseSpeedWithPipeline(w, "", sourceInfo, lines, bytes, elapsed)
}

func traceParseSpeedWithPipeline(w io.Writer, pipelineName string, sourceInfo parseio.SourceInfo, lines int, bytes int64, elapsed time.Duration) {
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		elapsedSeconds = 1e-9
	}
	logger := slog.New(slog.NewTextHandler(w, nil))
	attrs := []slog.Attr{
		slog.String("event", "finished"),
		slog.String("filename", sourceInfo.Name),
		slog.String("source_kind", sourceInfo.Kind),
		slog.String("source_name", sourceInfo.Name),
		slog.Bool("source_finite", sourceInfo.Finite),
		slog.Int("lines", lines),
		slog.Int64("bytes", bytes),
		slog.Float64("duration_seconds", elapsedSeconds),
		slog.Float64("lines_per_second", float64(lines)/elapsedSeconds),
		slog.Float64("bytes_per_second", float64(bytes)/elapsedSeconds),
	}
	if pipelineName != "" {
		attrs = append([]slog.Attr{slog.String("pipeline", pipelineName)}, attrs...)
	}
	logger.LogAttrs(context.Background(), slog.LevelInfo, "parse_trace", attrs...)
}

func matchTemplate(paramString string, templateTokens []string, lineTokens []lineToken, variables []string) ([]string, bool) {
	if len(templateTokens) != len(lineTokens) {
		return nil, false
	}
	variables = variables[:0]
	for i, templateToken := range templateTokens {
		lineToken := lineTokens[i]
		if templateToken == paramString {
			variables = append(variables, lineToken.rawString)
			continue
		}
		if templateToken != lineToken.value {
			return nil, false
		}
	}
	return variables, true
}

func tokenizeLine(line string, maskingRules []compiledMaskingRule, extraDelimiters []string) []lineToken {
	line = strings.TrimSpace(line)
	if len(maskingRules) == 1 && len(extraDelimiters) == 0 {
		if tokens, ok := tokenizeLineSingleMask(line, maskingRules[0]); ok {
			return tokens
		}
	}
	return tokenizeLineLegacy(line, maskingRules, extraDelimiters)
}

func tokenizeLineLegacy(line string, maskingRules []compiledMaskingRule, extraDelimiters []string) []lineToken {
	line = strings.TrimSpace(line)
	masked := maskLine(line, maskingRules)
	replacedParts := make([]string, 0, len(masked))
	maskedByMarker := make(map[string]lineSegment)
	markers := make([]string, 0, len(masked))
	maskedCount := 0
	for _, segment := range masked {
		if !segment.masked {
			replacedParts = append(replacedParts, replaceExtraDelimiters(segment.rawString, extraDelimiters))
			continue
		}
		marker := fmt.Sprintf("\x00DRAIN_MASK_%d\x00", maskedCount)
		maskedCount++
		replacedParts = append(replacedParts, marker)
		maskedByMarker[marker] = segment
		markers = append(markers, marker)
	}

	replaced := strings.Join(replacedParts, "")
	replacedTokens := strings.Fields(replaced)
	tokens := make([]lineToken, 0)
	for _, token := range replacedTokens {
		value := token
		rawString := token
		for _, marker := range markers {
			segment := maskedByMarker[marker]
			if !strings.Contains(value, marker) {
				continue
			}
			value = strings.ReplaceAll(value, marker, segment.value)
			rawString = strings.ReplaceAll(rawString, marker, segment.rawString)
		}
		tokens = append(tokens, lineToken{
			value:     value,
			rawString: rawString,
		})
	}
	return tokens
}

func tokenizeLineSingleMask(line string, maskingRule compiledMaskingRule) ([]lineToken, bool) {
	matches := maskingRule.regex.FindAllStringIndex(line, -1)
	if len(matches) == 0 {
		return splitLineTokens(line, nil), true
	}
	for _, match := range matches {
		if !isStandaloneMask(line, match[0], match[1]) {
			return nil, false
		}
	}

	tokens := make([]lineToken, 0, lineTokenCapacity(line, matches))
	matchIndex := 0
	for index := 0; index < len(line); {
		if matchIndex < len(matches) && index == matches[matchIndex][0] {
			match := matches[matchIndex]
			tokens = append(tokens, lineToken{
				value:     maskingRule.replacement,
				rawString: line[match[0]:match[1]],
			})
			index = match[1]
			matchIndex++
			continue
		}
		if isSpace, size := spaceAt(line, index); isSpace {
			index += size
			for index < len(line) {
				isSpace, size := spaceAt(line, index)
				if !isSpace {
					break
				}
				index += size
			}
			continue
		}

		start := index
		nextMatchStart := len(line)
		if matchIndex < len(matches) {
			nextMatchStart = matches[matchIndex][0]
		}
		for index < len(line) && index != nextMatchStart {
			isSpace, size := spaceAt(line, index)
			if isSpace {
				break
			}
			index += size
		}
		tokens = append(tokens, lineToken{
			value:     line[start:index],
			rawString: line[start:index],
		})
	}
	return tokens, true
}

func isStandaloneMask(line string, start, end int) bool {
	if start == end {
		return false
	}
	if isSpaceAt(line, start) || isSpaceBefore(line, end) {
		return false
	}
	if start > 0 && !isSpaceBefore(line, start) {
		return false
	}
	if end < len(line) && !isSpaceAt(line, end) {
		return false
	}
	return true
}

func spaceAt(s string, index int) (bool, int) {
	r, size := utf8.DecodeRuneInString(s[index:])
	return unicode.IsSpace(r), size
}

func isSpaceAt(s string, index int) bool {
	isSpace, _ := spaceAt(s, index)
	return isSpace
}

func isSpaceBefore(s string, index int) bool {
	r, _ := utf8.DecodeLastRuneInString(s[:index])
	return unicode.IsSpace(r)
}

func lineTokenCapacity(line string, matches [][]int) int {
	capacity := strings.Count(line, " ") + 1
	for _, match := range matches {
		capacity -= strings.Count(line[match[0]:match[1]], " ")
	}
	if capacity < 1 {
		return 1
	}
	return capacity
}

func splitLineTokens(line string, extraDelimiters []string) []lineToken {
	parts := strings.Fields(replaceExtraDelimiters(line, extraDelimiters))
	tokens := make([]lineToken, 0, len(parts))
	for _, part := range parts {
		tokens = append(tokens, lineToken{
			value:     part,
			rawString: part,
		})
	}
	return tokens
}

func replaceExtraDelimiters(line string, extraDelimiters []string) string {
	for _, extraDelimiter := range extraDelimiters {
		line = strings.Replace(line, extraDelimiter, " ", -1)
	}
	return line
}

type lineSegment struct {
	masked    bool
	value     string
	rawString string
}

func maskLine(line string, maskingRules []compiledMaskingRule) []lineSegment {
	segments := []lineSegment{{rawString: line}}
	for _, rule := range maskingRules {
		next := make([]lineSegment, 0, len(segments))
		for _, segment := range segments {
			if segment.masked {
				next = append(next, segment)
				continue
			}
			next = append(next, maskSegment(segment.rawString, rule)...)
		}
		segments = next
	}
	return segments
}

func maskSegment(text string, rule compiledMaskingRule) []lineSegment {
	matches := rule.regex.FindAllStringIndex(text, -1)
	if len(matches) == 0 {
		return []lineSegment{{rawString: text}}
	}

	segments := make([]lineSegment, 0, len(matches)*2+1)
	offset := 0
	for _, match := range matches {
		if match[0] > offset {
			segments = append(segments, lineSegment{rawString: text[offset:match[0]]})
		}
		raw := text[match[0]:match[1]]
		segments = append(segments, lineSegment{
			masked:    true,
			value:     rule.replacement,
			rawString: raw,
		})
		offset = match[1]
	}
	if offset < len(text) {
		segments = append(segments, lineSegment{rawString: text[offset:]})
	}
	return segments
}
