package main

import (
	"context"
	"flag"
	"io"
	"time"

	"github.com/faceair/drain/internal/parseio"
)

const (
	parseFormatJSONL   = parseio.FormatJSONL
	parseFormatParquet = parseio.FormatParquet

	defaultParseBatchSize   = parseio.DefaultBatchSize
	defaultParseBatchMaxAge = parseio.DefaultBatchMaxAge

	parseJSONLContentType   = parseio.JSONLContentType
	parseParquetContentType = parseio.ParquetContentType
)

type parseOutput = parseio.Output
type parseSink = parseio.Sink
type parseOutputOptions = parseio.SinkOptions

func validateParseOutputOptions(format string, batchSize int, batchMaxAge time.Duration) error {
	return parseio.ValidateSinkOptions(format, batchSize, batchMaxAge)
}

func newParseSink(ctx context.Context, stdout io.Writer, opts parseOutputOptions) (parseSink, error) {
	return parseio.NewSink(ctx, stdout, opts)
}

func stringFlagValue(fs *flag.FlagSet, name, value string) parseio.OptionalString {
	return parseio.OptionalString{Value: value, Set: flagWasProvided(fs, name)}
}

func boolFlagValue(fs *flag.FlagSet, name string, value bool) parseio.OptionalBool {
	return parseio.OptionalBool{Value: value, Set: flagWasProvided(fs, name)}
}
