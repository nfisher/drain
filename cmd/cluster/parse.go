package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/faceair/drain/internal/parseio"
)

const parseErrorLineMaxBytes = 512

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
func runParse(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("parse", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	configPath := fs.String("config", "", "HCL parse pipeline config")
	generateConfig := fs.Bool("generate-config", false, "write equivalent HCL parse pipeline config to stdout and exit")
	sourceKind := fs.String("source", "file", "input source: file, dmesg, or systemd")
	follow := fs.Bool("follow", false, "follow streaming input sources")
	checkpointPath := fs.String("checkpoint", "", "source checkpoint file used to resume acknowledged records")
	filename := fs.String("filename", "example.log", "target log file")
	modelPath := fs.String("model", "model.json", "model path")
	outputFormat := fs.String("format", parseFormatJSONL, "output format: jsonl or parquet")
	includeParameters := fs.Bool("include-parameters", false, "include parsed parameters in output")
	excludeSource := fs.Bool("exclude-source", false, "exclude source_kind and source_name from output")
	outputPrefix := fs.String("output", "", "output prefix; local path or s3://bucket/prefix")
	batchSize := fs.Int("batch-size", defaultParseBatchSize, "rows per output part")
	batchMaxAge := fs.Duration("batch-max-age", defaultParseBatchMaxAge, "maximum age of a non-empty output part")
	metricsListenAddress := fs.String("metrics-listen-address", "", "Prometheus metrics listen address; disabled when empty")
	dmesgKmsgPath := fs.String("dmesg-kmsg-path", parseio.DefaultDmesgKmsgPath, "dmesg kernel message device path")
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
	s3UseSSL := fs.Bool("s3-use-ssl", true, "use TLS for S3 requests")
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
		Kind:       *sourceKind,
		Filename:   *filename,
		Follow:     *follow,
		Checkpoint: *checkpointPath,
		Dmesg: parseio.DmesgOptions{
			Follow:   *follow,
			KmsgPath: *dmesgKmsgPath,
		},
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
	Kind       string
	Filename   string
	Follow     bool
	Dmesg      parseio.DmesgOptions
	Systemd    parseio.SystemdOptions
	Checkpoint string
}

var newDmesgParseSource = func(options parseio.DmesgOptions) (parseio.Source, error) {
	return parseio.NewDmesgSourceWithOptions(options)
}

var newSystemdParseSource = func(options parseio.SystemdOptions) (parseio.Source, error) {
	return parseio.NewSystemdSource(options)
}

func newParseSource(opts parseSourceOptions) (parseio.Source, error) {
	var source parseio.Source
	var err error
	switch opts.Kind {
	case "", "file":
		if opts.Follow {
			return nil, fmt.Errorf("source %q does not support -follow", "file")
		}
		source, err = parseio.NewFileSource(opts.Filename)
	case "dmesg":
		dmesgOptions := opts.Dmesg
		dmesgOptions.Follow = dmesgOptions.Follow || opts.Follow
		source, err = newDmesgParseSource(dmesgOptions)
	case "systemd":
		systemdOptions := opts.Systemd
		systemdOptions.Follow = systemdOptions.Follow || opts.Follow
		source, err = newSystemdParseSource(systemdOptions)
	default:
		return nil, fmt.Errorf("source %q is not supported yet", opts.Kind)
	}
	if err != nil {
		return nil, err
	}
	return checkpointParseSource(source, opts.Checkpoint)
}

type checkpointedParseSource struct {
	parseio.Source
	path         string
	checkpointer parseio.SourceCheckpointer
}

func checkpointParseSource(source parseio.Source, checkpointPath string) (parseio.Source, error) {
	checkpointPath = strings.TrimSpace(checkpointPath)
	if checkpointPath == "" {
		return source, nil
	}
	checkpointer, ok := source.(parseio.SourceCheckpointer)
	if !ok {
		_ = source.Close(context.Background())
		return nil, fmt.Errorf("source %q does not support checkpoints", source.Info().Kind)
	}
	checkpoint, err := parseio.LoadSourceCheckpoint(checkpointPath)
	if err != nil {
		_ = source.Close(context.Background())
		return nil, err
	}
	if err := checkpointer.Resume(checkpoint); err != nil {
		_ = source.Close(context.Background())
		return nil, err
	}
	return &checkpointedParseSource{Source: source, path: checkpointPath, checkpointer: checkpointer}, nil
}

func (s *checkpointedParseSource) Ack(ctx context.Context) error {
	if err := s.Source.Ack(ctx); err != nil {
		return err
	}
	return parseio.SaveSourceCheckpoint(ctx, s.path, s.checkpointer.Checkpoint())
}

func validateParseSourceFlags(fs *flag.FlagSet, sourceKind string) error {
	if sourceKind == "" {
		sourceKind = "file"
	}
	if sourceKind == "dmesg" {
		for _, name := range systemdParseFlagNames() {
			if flagWasProvided(fs, name) {
				return errors.New("systemd flags require -source systemd")
			}
		}
		return nil
	}
	if sourceKind == "systemd" {
		if flagWasProvided(fs, "filename") {
			return errors.New("-filename is only supported with -source file")
		}
		for _, name := range dmesgParseFlagNames() {
			if flagWasProvided(fs, name) {
				return errors.New("dmesg flags require -source dmesg")
			}
		}
		return nil
	}
	for _, name := range systemdParseFlagNames() {
		if flagWasProvided(fs, name) {
			return errors.New("systemd flags require -source systemd")
		}
	}
	for _, name := range dmesgParseFlagNames() {
		if flagWasProvided(fs, name) {
			return errors.New("dmesg flags require -source dmesg")
		}
	}
	return nil
}

func dmesgParseFlagNames() []string {
	return []string{
		"dmesg-kmsg-path",
	}
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

func scanLines(filename string, handle func(string) error) error {
	file, err := os.Open(filename) // #nosec G304 -- log filename is an explicit CLI input.
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
