package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

const (
	parseFormatJSONL   = "jsonl"
	parseFormatParquet = "parquet"

	defaultParseBatchSize   = 10_000
	defaultParseBatchMaxAge = 5 * time.Second

	parseJSONLContentType   = "application/x-ndjson"
	parseParquetContentType = "application/vnd.apache.parquet"
)

var parseParquetSchema = arrow.NewSchema([]arrow.Field{
	{Name: "template_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "model_id", Type: arrow.BinaryTypes.String},
	{Name: "variables", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	{Name: "parameters", Type: arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "mask_name", Type: arrow.BinaryTypes.String},
	))},
}, nil)

type parseOutputWriter interface {
	Write(parseOutput) error
	Close() error
}

type parseOutputOptions struct {
	Format      string
	Prefix      string
	BatchSize   int
	BatchMaxAge time.Duration
	S3          s3FlagValues
	RunID       string
	Now         func() time.Time
}

type parseOutputDestination interface {
	NewWriter(ctx context.Context, objectPath, contentType string) (io.WriteCloser, error)
}

type optionalStringFlag struct {
	Value string
	Set   bool
}

type optionalBoolFlag struct {
	Value bool
	Set   bool
}

type s3FlagValues struct {
	Endpoint            optionalStringFlag
	EndpointFile        optionalStringFlag
	Region              optionalStringFlag
	RegionFile          optionalStringFlag
	AccessKeyID         optionalStringFlag
	AccessKeyIDFile     optionalStringFlag
	SecretAccessKey     optionalStringFlag
	SecretAccessKeyFile optionalStringFlag
	SessionToken        optionalStringFlag
	SessionTokenFile    optionalStringFlag
	UseSSL              optionalBoolFlag
	UseSSLFile          optionalStringFlag
	PathStyle           optionalBoolFlag
	PathStyleFile       optionalStringFlag
}

type s3Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	UseSSL          bool
	PathStyle       bool
}

func validateParseOutputOptions(format string, batchSize int, batchMaxAge time.Duration) error {
	switch format {
	case parseFormatJSONL, parseFormatParquet:
	default:
		return fmt.Errorf("format must be %q or %q, got %q", parseFormatJSONL, parseFormatParquet, format)
	}
	if batchSize <= 0 {
		return fmt.Errorf("batch-size must be greater than 0, got %d", batchSize)
	}
	if batchMaxAge <= 0 {
		return fmt.Errorf("batch-max-age must be greater than 0, got %s", batchMaxAge)
	}
	return nil
}

func newParseOutputWriter(ctx context.Context, stdout io.Writer, opts parseOutputOptions) (parseOutputWriter, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Prefix == "" {
		if opts.Format != parseFormatJSONL {
			return nil, fmt.Errorf("%s output requires -output", opts.Format)
		}
		return newJSONLStreamWriter(stdout), nil
	}

	destination, err := newParseOutputDestination(opts.Prefix, opts.S3)
	if err != nil {
		return nil, err
	}
	runID := opts.RunID
	if runID == "" {
		runID, err = newOutputRunID(opts.Now())
		if err != nil {
			return nil, err
		}
	}
	if opts.Format == parseFormatParquet {
		return newPartParquetWriter(ctx, destination, runID, opts.BatchSize, opts.BatchMaxAge, opts.Now), nil
	}
	return newPartJSONLWriter(ctx, destination, runID, opts.BatchSize, opts.BatchMaxAge, opts.Now), nil
}

func stringFlagValue(fs *flag.FlagSet, name, value string) optionalStringFlag {
	return optionalStringFlag{Value: value, Set: flagWasProvided(fs, name)}
}

func boolFlagValue(fs *flag.FlagSet, name string, value bool) optionalBoolFlag {
	return optionalBoolFlag{Value: value, Set: flagWasProvided(fs, name)}
}

type jsonlStreamWriter struct {
	encoder *json.Encoder
}

func newJSONLStreamWriter(w io.Writer) *jsonlStreamWriter {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return &jsonlStreamWriter{encoder: encoder}
}

func (w *jsonlStreamWriter) Write(output parseOutput) error {
	return w.encoder.Encode(output)
}

func (w *jsonlStreamWriter) Close() error {
	return nil
}

type partJSONLWriter struct {
	ctx         context.Context
	destination parseOutputDestination
	runID       string
	batchSize   int
	batchMaxAge time.Duration
	now         func() time.Time

	partNumber  int
	rowsInPart  int
	partStarted time.Time
	writer      io.WriteCloser
	buffered    *bufio.Writer
	encoder     *json.Encoder
}

func newPartJSONLWriter(ctx context.Context, destination parseOutputDestination, runID string, batchSize int, batchMaxAge time.Duration, now func() time.Time) *partJSONLWriter {
	return &partJSONLWriter{
		ctx:         ctx,
		destination: destination,
		runID:       runID,
		batchSize:   batchSize,
		batchMaxAge: batchMaxAge,
		now:         now,
	}
}

func (w *partJSONLWriter) Write(output parseOutput) error {
	if w.writer != nil && w.rowsInPart > 0 && !w.now().Before(w.partStarted.Add(w.batchMaxAge)) {
		if err := w.closePart(); err != nil {
			return err
		}
	}
	if w.writer == nil {
		if err := w.openPart(); err != nil {
			return err
		}
	}
	if err := w.encoder.Encode(output); err != nil {
		return err
	}
	w.rowsInPart++
	if w.rowsInPart >= w.batchSize {
		return w.closePart()
	}
	return nil
}

func (w *partJSONLWriter) Close() error {
	if w.writer == nil {
		return nil
	}
	return w.closePart()
}

func (w *partJSONLWriter) openPart() error {
	objectPath := batchOutputPartPath(parseFormatJSONL, w.runID, w.partNumber)
	writer, err := w.destination.NewWriter(w.ctx, objectPath, parseJSONLContentType)
	if err != nil {
		return err
	}
	w.writer = writer
	w.buffered = bufio.NewWriter(writer)
	w.encoder = json.NewEncoder(w.buffered)
	w.encoder.SetEscapeHTML(false)
	w.rowsInPart = 0
	w.partStarted = w.now()
	return nil
}

func (w *partJSONLWriter) closePart() error {
	flushErr := w.buffered.Flush()
	closeErr := w.writer.Close()
	w.writer = nil
	w.buffered = nil
	w.encoder = nil
	w.rowsInPart = 0
	w.partNumber++
	if flushErr != nil {
		return flushErr
	}
	return closeErr
}

type partParquetWriter struct {
	ctx           context.Context
	destination   parseOutputDestination
	runID         string
	batchSize     int
	batchMaxAge   time.Duration
	now           func() time.Time
	allocator     memory.Allocator
	partNumber    int
	rowsInPart    int
	partStarted   time.Time
	writer        io.WriteCloser
	parquetWriter *pqarrow.FileWriter
	builder       *array.RecordBuilder
}

func newPartParquetWriter(ctx context.Context, destination parseOutputDestination, runID string, batchSize int, batchMaxAge time.Duration, now func() time.Time) *partParquetWriter {
	return &partParquetWriter{
		ctx:         ctx,
		destination: destination,
		runID:       runID,
		batchSize:   batchSize,
		batchMaxAge: batchMaxAge,
		now:         now,
		allocator:   memory.NewGoAllocator(),
	}
}

func (w *partParquetWriter) Write(output parseOutput) error {
	if w.writer != nil && w.rowsInPart > 0 && !w.now().Before(w.partStarted.Add(w.batchMaxAge)) {
		if err := w.closePart(); err != nil {
			return err
		}
	}
	if w.writer == nil {
		if err := w.openPart(); err != nil {
			return err
		}
	}
	appendParseParquetRow(w.builder, output)
	w.rowsInPart++
	if w.rowsInPart >= w.batchSize {
		return w.closePart()
	}
	return nil
}

func (w *partParquetWriter) Close() error {
	if w.writer == nil {
		return nil
	}
	return w.closePart()
}

func (w *partParquetWriter) openPart() error {
	objectPath := batchOutputPartPath(parseFormatParquet, w.runID, w.partNumber)
	writer, err := w.destination.NewWriter(w.ctx, objectPath, parseParquetContentType)
	if err != nil {
		return err
	}
	parquetWriter, err := pqarrow.NewFileWriter(
		parseParquetSchema,
		writeOnly{Writer: writer},
		parquet.NewWriterProperties(),
		pqarrow.DefaultWriterProps(),
	)
	if err != nil {
		_ = writer.Close()
		return err
	}
	w.writer = writer
	w.parquetWriter = parquetWriter
	w.builder = array.NewRecordBuilder(w.allocator, parseParquetSchema)
	w.rowsInPart = 0
	w.partStarted = w.now()
	return nil
}

func (w *partParquetWriter) closePart() error {
	var firstErr error
	if w.rowsInPart > 0 {
		record := w.builder.NewRecord()
		if err := w.parquetWriter.Write(record); err != nil && firstErr == nil {
			firstErr = err
		}
		record.Release()
	}
	if err := w.parquetWriter.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := w.writer.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	w.builder.Release()
	w.writer = nil
	w.parquetWriter = nil
	w.builder = nil
	w.rowsInPart = 0
	w.partNumber++
	return firstErr
}

type writeOnly struct {
	io.Writer
}

func appendParseParquetRow(builder *array.RecordBuilder, output parseOutput) {
	templateIDBuilder := builder.Field(0).(*array.Int64Builder)
	if output.TemplateID == nil {
		templateIDBuilder.AppendNull()
	} else {
		templateIDBuilder.Append(int64(*output.TemplateID))
	}

	builder.Field(1).(*array.StringBuilder).Append(output.ModelID)

	variablesBuilder := builder.Field(2).(*array.ListBuilder)
	variableValues := variablesBuilder.ValueBuilder().(*array.StringBuilder)
	variablesBuilder.Append(true)
	for _, variable := range output.Variables {
		variableValues.Append(variable)
	}

	parametersBuilder := builder.Field(3).(*array.ListBuilder)
	parameterStructs := parametersBuilder.ValueBuilder().(*array.StructBuilder)
	parameterValues := parameterStructs.FieldBuilder(0).(*array.StringBuilder)
	parameterMaskNames := parameterStructs.FieldBuilder(1).(*array.StringBuilder)
	parametersBuilder.Append(true)
	for _, parameter := range output.Parameters {
		parameterStructs.Append(true)
		parameterValues.Append(parameter.Value)
		parameterMaskNames.Append(parameter.MaskName)
	}
}

func batchOutputPartPath(format, runID string, partNumber int) string {
	return path.Join(
		"format="+format,
		"run_id="+runID,
		fmt.Sprintf("part-%05d.%s", partNumber, format),
	)
}

func newOutputRunID(now time.Time) (string, error) {
	var randomBytes [8]byte
	if _, err := rand.Read(randomBytes[:]); err != nil {
		return "", err
	}
	return now.UTC().Format("20060102T150405Z") + "-" + hex.EncodeToString(randomBytes[:]), nil
}

func newParseOutputDestination(prefix string, s3Flags s3FlagValues) (parseOutputDestination, error) {
	if strings.HasPrefix(prefix, "s3://") {
		return newS3OutputDestination(prefix, s3Flags)
	}
	if strings.Contains(prefix, "://") {
		return nil, fmt.Errorf("unsupported output prefix scheme in %q", prefix)
	}
	return localOutputDestination{prefix: prefix}, nil
}

type localOutputDestination struct {
	prefix string
}

func (d localOutputDestination) NewWriter(_ context.Context, objectPath, _ string) (io.WriteCloser, error) {
	localPath := filepath.Join(d.prefix, filepath.FromSlash(objectPath))
	if err := os.MkdirAll(filepath.Dir(localPath), 0o755); err != nil {
		return nil, err
	}
	return os.Create(localPath)
}

type s3OutputDestination struct {
	bucket string
	prefix string
	config s3Config
}

func newS3OutputDestination(prefix string, flags s3FlagValues) (parseOutputDestination, error) {
	bucket, objectPrefix, err := parseS3Prefix(prefix)
	if err != nil {
		return nil, err
	}
	config, err := resolveS3Config(flags)
	if err != nil {
		return nil, err
	}
	return s3OutputDestination{bucket: bucket, prefix: objectPrefix, config: config}, nil
}

func parseS3Prefix(value string) (bucket, objectPrefix string, err error) {
	withoutScheme := strings.TrimPrefix(value, "s3://")
	if withoutScheme == "" {
		return "", "", errors.New("s3 output prefix must include a bucket")
	}
	bucket, objectPrefix, _ = strings.Cut(withoutScheme, "/")
	if bucket == "" {
		return "", "", errors.New("s3 output prefix must include a bucket")
	}
	return bucket, strings.Trim(objectPrefix, "/"), nil
}

func (d s3OutputDestination) NewWriter(ctx context.Context, objectPath, contentType string) (io.WriteCloser, error) {
	key := path.Join(d.prefix, objectPath)
	file, err := os.CreateTemp("", "drain-cluster-output-*")
	if err != nil {
		return nil, err
	}
	return &s3TempWriter{
		ctx:         ctx,
		file:        file,
		config:      d.config,
		bucket:      d.bucket,
		key:         key,
		contentType: contentType,
	}, nil
}

type s3TempWriter struct {
	ctx         context.Context
	file        *os.File
	config      s3Config
	bucket      string
	key         string
	contentType string
}

func (w *s3TempWriter) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

func (w *s3TempWriter) Close() error {
	var firstErr error
	info, err := w.file.Stat()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr == nil {
		if err := putS3Object(w.ctx, w.config, w.bucket, w.key, w.file, info.Size(), w.contentType); err != nil {
			firstErr = err
		}
	}
	name := w.file.Name()
	if err := w.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := os.Remove(name); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

type putS3ObjectFunc func(ctx context.Context, config s3Config, bucket, key string, reader io.Reader, size int64, contentType string) error

var putS3Object putS3ObjectFunc = putS3ObjectMinIO

func putS3ObjectMinIO(ctx context.Context, config s3Config, bucket, key string, reader io.Reader, size int64, contentType string) error {
	bucketLookup := minio.BucketLookupPath
	if !config.PathStyle {
		bucketLookup = minio.BucketLookupDNS
	}
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, config.SessionToken),
		Secure:       config.UseSSL,
		Region:       config.Region,
		BucketLookup: bucketLookup,
	})
	if err != nil {
		return err
	}
	_, err = client.PutObject(ctx, bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	return err
}

func resolveS3Config(flags s3FlagValues) (s3Config, error) {
	endpointRaw, err := stringFromFlagEnvOrFile(
		flags.Endpoint,
		flags.EndpointFile,
		[]string{"S3_ENDPOINT", "AWS_ENDPOINT_URL"},
		[]string{"S3_ENDPOINT_FILE", "AWS_ENDPOINT_URL_FILE"},
	)
	if err != nil {
		return s3Config{}, err
	}
	endpoint, schemeUseSSL, err := normalizeS3Endpoint(endpointRaw)
	if err != nil {
		return s3Config{}, err
	}
	if endpoint == "" {
		return s3Config{}, errors.New("s3 endpoint is required; set -s3-endpoint or S3_ENDPOINT")
	}

	useSSLDefault := true
	if schemeUseSSL != nil {
		useSSLDefault = *schemeUseSSL
	}
	useSSL, err := boolFromFlagEnvOrFile(flags.UseSSL, flags.UseSSLFile, "s3-use-ssl-file", useSSLDefault, []string{"S3_USE_SSL"}, []string{"S3_USE_SSL_FILE"})
	if err != nil {
		return s3Config{}, err
	}
	pathStyle, err := boolFromFlagEnvOrFile(flags.PathStyle, flags.PathStyleFile, "s3-path-style-file", true, []string{"S3_PATH_STYLE"}, []string{"S3_PATH_STYLE_FILE"})
	if err != nil {
		return s3Config{}, err
	}
	region, err := stringFromFlagEnvOrFile(flags.Region, flags.RegionFile, []string{"S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"}, []string{"S3_REGION_FILE", "AWS_REGION_FILE", "AWS_DEFAULT_REGION_FILE"})
	if err != nil {
		return s3Config{}, err
	}
	accessKeyID, err := stringFromFlagEnvOrFile(flags.AccessKeyID, flags.AccessKeyIDFile, []string{"S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"}, []string{"S3_ACCESS_KEY_ID_FILE", "AWS_ACCESS_KEY_ID_FILE"})
	if err != nil {
		return s3Config{}, err
	}
	secretAccessKey, err := stringFromFlagEnvOrFile(flags.SecretAccessKey, flags.SecretAccessKeyFile, []string{"S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"}, []string{"S3_SECRET_ACCESS_KEY_FILE", "AWS_SECRET_ACCESS_KEY_FILE"})
	if err != nil {
		return s3Config{}, err
	}
	sessionToken, err := stringFromFlagEnvOrFile(flags.SessionToken, flags.SessionTokenFile, []string{"S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"}, []string{"S3_SESSION_TOKEN_FILE", "AWS_SESSION_TOKEN_FILE"})
	if err != nil {
		return s3Config{}, err
	}

	config := s3Config{
		Endpoint:        endpoint,
		Region:          stringDefault(region, "us-east-1"),
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
		UseSSL:          useSSL,
		PathStyle:       pathStyle,
	}
	if (config.AccessKeyID == "") != (config.SecretAccessKey == "") {
		return s3Config{}, errors.New("s3 credentials require both access key ID and secret access key")
	}
	if config.AccessKeyID == "" {
		return s3Config{}, errors.New("s3 access key ID and secret access key are required")
	}
	return config, nil
}

func normalizeS3Endpoint(value string) (endpoint string, schemeUseSSL *bool, err error) {
	if value == "" {
		return "", nil, nil
	}
	if strings.Contains(value, "://") {
		scheme, rest, _ := strings.Cut(value, "://")
		switch scheme {
		case "http":
			useSSL := false
			schemeUseSSL = &useSSL
		case "https":
			useSSL := true
			schemeUseSSL = &useSSL
		default:
			return "", nil, fmt.Errorf("s3 endpoint scheme must be http or https, got %q", scheme)
		}
		if rest == "" || strings.Contains(rest, "/") {
			return "", nil, fmt.Errorf("s3 endpoint must be a host without a path, got %q", value)
		}
		return rest, schemeUseSSL, nil
	}
	return value, nil, nil
}

func stringFromFlagEnvOrFile(valueFlag, fileFlag optionalStringFlag, envNames, envFileNames []string) (string, error) {
	if valueFlag.Set {
		return valueFlag.Value, nil
	}
	if fileFlag.Set {
		return readSecretFile(fileFlag.Value)
	}
	for _, name := range envNames {
		if value, ok := os.LookupEnv(name); ok {
			return value, nil
		}
	}
	for _, name := range envFileNames {
		if value, ok := os.LookupEnv(name); ok {
			return readSecretFile(value)
		}
	}
	return "", nil
}

func boolFromFlagEnvOrFile(valueFlag optionalBoolFlag, fileFlag optionalStringFlag, fileFlagName string, defaultValue bool, envNames, envFileNames []string) (bool, error) {
	if valueFlag.Set {
		return valueFlag.Value, nil
	}
	if fileFlag.Set {
		return boolFromFile(fileFlag.Value, fileFlagName)
	}
	for _, name := range envNames {
		if value, ok := os.LookupEnv(name); ok {
			parsed, err := strconv.ParseBool(value)
			if err != nil {
				return false, fmt.Errorf("%s must be a boolean, got %q", name, value)
			}
			return parsed, nil
		}
	}
	for _, name := range envFileNames {
		if value, ok := os.LookupEnv(name); ok {
			return boolFromFile(value, name)
		}
	}
	return defaultValue, nil
}

func boolFromFile(path, source string) (bool, error) {
	value, err := readSecretFile(path)
	if err != nil {
		return false, err
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must point to a file containing a boolean, got %q", source, value)
	}
	return parsed, nil
}

func readSecretFile(path string) (string, error) {
	if path == "" {
		return "", errors.New("secret file path must not be empty")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret file %s: %w", path, err)
	}
	return strings.TrimSpace(string(contents)), nil
}

func stringDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}
