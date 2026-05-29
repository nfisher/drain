package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"path"
	"strings"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/faceair/drain/internal/parseio"
)

const (
	parseFormatJSONL   = "jsonl"
	parseFormatParquet = "parquet"

	defaultParseBatchSize   = 10_000
	defaultParseBatchMaxAge = 5 * time.Second

	parseJSONLContentType   = "application/x-ndjson"
	parseParquetContentType = "application/vnd.apache.parquet"
)

var (
	parseParquetBaseFields = []arrow.Field{
		{Name: "template_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
		{Name: "model_id", Type: arrow.BinaryTypes.String},
	}
	parseParquetSourceFields = []arrow.Field{
		{Name: "source_kind", Type: arrow.BinaryTypes.String},
		{Name: "source_name", Type: arrow.BinaryTypes.String},
	}
	parseParquetVariablesField  = arrow.Field{Name: "variables", Type: arrow.ListOf(arrow.BinaryTypes.String)}
	parseParquetParametersField = arrow.Field{Name: "parameters", Type: arrow.ListOf(arrow.StructOf(
		arrow.Field{Name: "value", Type: arrow.BinaryTypes.String},
		arrow.Field{Name: "mask_name", Type: arrow.BinaryTypes.String},
	))}
)

var parseParquetFields = []arrow.Field{
	{Name: "template_id", Type: arrow.PrimitiveTypes.Int64, Nullable: true},
	{Name: "model_id", Type: arrow.BinaryTypes.String},
	{Name: "source_kind", Type: arrow.BinaryTypes.String},
	{Name: "source_name", Type: arrow.BinaryTypes.String},
	{Name: "variables", Type: arrow.ListOf(arrow.BinaryTypes.String)},
	parseParquetParametersField,
}

type parseSink interface {
	Write(context.Context, parseOutput) error
	Close(context.Context) error
}

type parseOutputOptions struct {
	Format            string
	Prefix            string
	IncludeParameters bool
	ExcludeSource     bool
	BatchSize         int
	BatchMaxAge       time.Duration
	S3                parseio.S3Options
	RunID             string
	Now               func() time.Time
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

func newParseSink(_ context.Context, stdout io.Writer, opts parseOutputOptions) (parseSink, error) {
	if opts.Now == nil {
		opts.Now = time.Now
	}
	if opts.Prefix == "" {
		if opts.Format != parseFormatJSONL {
			return nil, fmt.Errorf("%s output requires -output", opts.Format)
		}
		return newJSONLStreamWriter(stdout, opts.IncludeParameters, opts.ExcludeSource), nil
	}

	store, err := newParseObjectStore(opts.Prefix, opts.S3)
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
		return newPartParquetWriter(store, runID, opts.BatchSize, opts.BatchMaxAge, opts.IncludeParameters, opts.ExcludeSource, opts.Now), nil
	}
	return newPartJSONLWriter(store, runID, opts.BatchSize, opts.BatchMaxAge, opts.IncludeParameters, opts.ExcludeSource, opts.Now), nil
}

func parseParquetOutputSchema(includeParameters, excludeSource bool) *arrow.Schema {
	fields := make([]arrow.Field, 0, len(parseParquetFields))
	fields = append(fields, parseParquetBaseFields...)
	if !excludeSource {
		fields = append(fields, parseParquetSourceFields...)
	}
	fields = append(fields, parseParquetVariablesField)
	if includeParameters {
		fields = append(fields, parseParquetParametersField)
	}
	return arrow.NewSchema(fields, nil)
}

func stringFlagValue(fs *flag.FlagSet, name, value string) parseio.OptionalString {
	return parseio.OptionalString{Value: value, Set: flagWasProvided(fs, name)}
}

func boolFlagValue(fs *flag.FlagSet, name string, value bool) parseio.OptionalBool {
	return parseio.OptionalBool{Value: value, Set: flagWasProvided(fs, name)}
}

type jsonlStreamWriter struct {
	encoder           *json.Encoder
	includeParameters bool
	excludeSource     bool
}

func newJSONLStreamWriter(w io.Writer, includeParameters, excludeSource bool) *jsonlStreamWriter {
	encoder := json.NewEncoder(w)
	encoder.SetEscapeHTML(false)
	return &jsonlStreamWriter{
		encoder:           encoder,
		includeParameters: includeParameters,
		excludeSource:     excludeSource,
	}
}

func (w *jsonlStreamWriter) Write(_ context.Context, output parseOutput) error {
	if w.excludeSource {
		output.SourceKind = ""
		output.SourceName = ""
	}
	if !w.includeParameters {
		output.Parameters = nil
	}
	return w.encoder.Encode(output)
}

func (w *jsonlStreamWriter) Close(context.Context) error {
	return nil
}

type partJSONLWriter struct {
	store             parseio.ObjectStore
	runID             string
	batchSize         int
	batchMaxAge       time.Duration
	includeParameters bool
	excludeSource     bool
	now               func() time.Time

	partNumber  int
	rowsInPart  int
	partStarted time.Time
	writer      io.WriteCloser
	buffered    *bufio.Writer
	encoder     *json.Encoder
}

func newPartJSONLWriter(store parseio.ObjectStore, runID string, batchSize int, batchMaxAge time.Duration, includeParameters, excludeSource bool, now func() time.Time) *partJSONLWriter {
	return &partJSONLWriter{
		store:             store,
		runID:             runID,
		batchSize:         batchSize,
		batchMaxAge:       batchMaxAge,
		includeParameters: includeParameters,
		excludeSource:     excludeSource,
		now:               now,
	}
}

func (w *partJSONLWriter) Write(ctx context.Context, output parseOutput) error {
	if w.writer != nil && w.rowsInPart > 0 && !w.now().Before(w.partStarted.Add(w.batchMaxAge)) {
		if err := w.closePart(); err != nil {
			return err
		}
	}
	if w.writer == nil {
		if err := w.openPart(ctx); err != nil {
			return err
		}
	}
	if w.excludeSource {
		output.SourceKind = ""
		output.SourceName = ""
	}
	if !w.includeParameters {
		output.Parameters = nil
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

func (w *partJSONLWriter) Close(context.Context) error {
	if w.writer == nil {
		return nil
	}
	return w.closePart()
}

func (w *partJSONLWriter) openPart(ctx context.Context) error {
	objectPath := batchOutputPartPath(parseFormatJSONL, w.runID, w.partNumber)
	writer, err := w.store.Create(ctx, objectPath, parseJSONLContentType)
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
	store             parseio.ObjectStore
	runID             string
	batchSize         int
	batchMaxAge       time.Duration
	includeParameters bool
	excludeSource     bool
	now               func() time.Time
	allocator         memory.Allocator
	schema            *arrow.Schema
	partNumber        int
	rowsInPart        int
	partStarted       time.Time
	writer            io.WriteCloser
	parquetWriter     *pqarrow.FileWriter
	builder           *array.RecordBuilder
}

func newPartParquetWriter(store parseio.ObjectStore, runID string, batchSize int, batchMaxAge time.Duration, includeParameters, excludeSource bool, now func() time.Time) *partParquetWriter {
	return &partParquetWriter{
		store:             store,
		runID:             runID,
		batchSize:         batchSize,
		batchMaxAge:       batchMaxAge,
		includeParameters: includeParameters,
		excludeSource:     excludeSource,
		now:               now,
		allocator:         memory.NewGoAllocator(),
		schema:            parseParquetOutputSchema(includeParameters, excludeSource),
	}
}

func (w *partParquetWriter) Write(ctx context.Context, output parseOutput) error {
	if w.writer != nil && w.rowsInPart > 0 && !w.now().Before(w.partStarted.Add(w.batchMaxAge)) {
		if err := w.closePart(); err != nil {
			return err
		}
	}
	if w.writer == nil {
		if err := w.openPart(ctx); err != nil {
			return err
		}
	}
	appendParseParquetRow(w.builder, output, w.includeParameters, w.excludeSource)
	w.rowsInPart++
	if w.rowsInPart >= w.batchSize {
		return w.closePart()
	}
	return nil
}

func (w *partParquetWriter) Close(context.Context) error {
	if w.writer == nil {
		return nil
	}
	return w.closePart()
}

func (w *partParquetWriter) openPart(ctx context.Context) error {
	objectPath := batchOutputPartPath(parseFormatParquet, w.runID, w.partNumber)
	writer, err := w.store.Create(ctx, objectPath, parseParquetContentType)
	if err != nil {
		return err
	}
	parquetWriter, err := pqarrow.NewFileWriter(
		w.schema,
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
	w.builder = array.NewRecordBuilder(w.allocator, w.schema)
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

func appendParseParquetRow(builder *array.RecordBuilder, output parseOutput, includeParameters, excludeSource bool) {
	templateIDBuilder := builder.Field(0).(*array.Int64Builder)
	if output.TemplateID == nil {
		templateIDBuilder.AppendNull()
	} else {
		templateIDBuilder.Append(int64(*output.TemplateID))
	}

	builder.Field(1).(*array.StringBuilder).Append(output.ModelID)

	fieldIndex := 2
	if !excludeSource {
		builder.Field(fieldIndex).(*array.StringBuilder).Append(output.SourceKind)
		fieldIndex++
		builder.Field(fieldIndex).(*array.StringBuilder).Append(output.SourceName)
		fieldIndex++
	}

	variablesBuilder := builder.Field(fieldIndex).(*array.ListBuilder)
	fieldIndex++
	variableValues := variablesBuilder.ValueBuilder().(*array.StringBuilder)
	variablesBuilder.Append(true)
	for _, variable := range output.Variables {
		variableValues.Append(variable)
	}

	if !includeParameters {
		return
	}

	parametersBuilder := builder.Field(fieldIndex).(*array.ListBuilder)
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

func newParseObjectStore(prefix string, s3Flags parseio.S3Options) (parseio.ObjectStore, error) {
	if strings.HasPrefix(prefix, "s3://") {
		return parseio.NewS3Store(prefix, s3Flags)
	}
	if strings.Contains(prefix, "://") {
		return nil, fmt.Errorf("unsupported output prefix scheme in %q", prefix)
	}
	return parseio.LocalStore{Prefix: prefix}, nil
}
