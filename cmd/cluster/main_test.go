package main

import (
	"bytes"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/parquet/pqarrow"
	"github.com/faceair/drain"
	"github.com/faceair/drain/internal/parseio"
	a "github.com/gogunit/gunit/hammy"
	"github.com/gogunit/gunit/hammy/jsonassert"
	"github.com/google/go-cmp/cmp"
)

func TestRunParseTracesWholeFileSpeedToStderr(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "user <*> logged in",
				Tokens:   []string{"user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)

	logPath := filepath.Join(dir, "target.log")
	logContent := "user alice logged in\nuser bob logged in\nother line\n"
	if err := os.WriteFile(logPath, []byte(logContent), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"alice"}},
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"bob"}},
		parseOutput{TemplateID: nil, ModelID: modelID, Variables: []string{}},
	)

	trace := stderr.String()
	for _, want := range []string{
		"msg=parse_trace",
		"event=finished",
		"filename=" + logPath,
		"lines=3",
		"bytes=" + strconv.Itoa(len(logContent)),
		"duration_seconds=",
		"lines_per_second=",
		"bytes_per_second=",
	} {
		assert.Requires(a.String(trace).Contains(want))
	}
	assert.Requires(a.String(stdout.String()).NotContains("parse_trace"))
}

func TestRunParseSourceFileMatchesFilenameBehavior(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user <*> logged in",
				Tokens:   []string{"user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "user alice logged in\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-source", "file", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"alice"}},
	)
}

func TestRunParseRejectsUnsupportedSources(t *testing.T) {
	for _, source := range []string{"kafka", "systemd", "syslog"} {
		t.Run(source, func(t *testing.T) {
			assert := a.New(t)
			var stdout bytes.Buffer
			err := run([]string{"parse", "-source", source}, &stdout, ioDiscard{})
			assert.Requires(a.Error(err))

			want := "source " + strconv.Quote(source) + " is not supported yet"
			assert.Requires(a.String(err.Error()).EqualTo(want))
		})
	}
}

func TestParseProcessorParseHandlesMatchedUnmatchedAndNamedParameters(t *testing.T) {
	assert := a.New(t)
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\d+`, MaskWith: "NUM"}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "service id=<:NUM:> status <*>",
				Tokens:   []string{"service", "id=<:NUM:>", "status", "<*>"},
			},
		},
	}
	compiledRules, err := compileMaskingRules(model.MaskingRules, model.ParamString)
	assert.Requires(a.NilError(err))

	processor, err := newParseProcessor(model, compiledRules)
	assert.Requires(a.NilError(err))

	var output parseOutput

	assert.Requires(a.NilError(processor.Parse("service id=123 status retry", &output)))

	assert.Requires(ParseOutput(output).HasTemplateID(1))
	assert.Requires(ParseOutput(output).HasParameters(
		drain.ExtractedParameter{Value: "123", MaskName: "NUM"},
		drain.ExtractedParameter{Value: "retry", MaskName: "*"},
	))
	assert.Requires(ParseOutput(output).HasVariables("123", "retry"))

	assert.Requires(a.NilError(processor.Parse("other line", &output)))

	assert.Requires(ParseOutput(output).IsUnmatched())
}

func TestParseSourceRecordsAcksOnlyAfterSuccessfulSinkWrite(t *testing.T) {
	assert := a.New(t)
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user <*>",
				Tokens:   []string{"user", "<*>"},
			},
		},
	}
	processor, err := newParseProcessor(model, nil)
	assert.Requires(a.NilError(err))

	ctx := context.Background()
	source := &fakeParseSource{lines: []string{"user alice"}}
	sink := &capturingParseSink{}
	var record parseio.SourceRecord
	var output parseOutput

	assert.Requires(a.NilError(parseSourceRecords(ctx, source, processor, sink, &record, &output, func(parseio.SourceRecord) {})))

	assert.Requires(a.Number(source.acks).EqualTo(1))

	writeErr := errors.New("write failed")
	source = &fakeParseSource{lines: []string{"user bob"}}
	sink = &capturingParseSink{writeErr: writeErr}

	assert.Requires(a.True(errors.Is(parseSourceRecords(ctx, source, processor, sink, &record, &output, func(parseio.SourceRecord) {}),
		writeErr)))

	assert.Requires(a.Number(source.acks).EqualTo(0))
}

func TestParseSourceRecordsWrapsProcessorErrorsWithFileContext(t *testing.T) {
	assert := a.New(t)
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       79,
				Size:     1,
				Template: "user <*>",
				Tokens:   []string{"user", "<*>"},
			},
		},
	}
	processor, err := newParseProcessor(model, nil)
	assert.Requires(a.NilError(err))

	processor.parseTemplates[79] = parseTemplate{
		id:       79,
		template: "user fixed",
		tokens:   []string{"user", "fixed"},
	}

	source := &fakeParseSource{
		kind:     "file",
		name:     "target.log",
		lines:    []string{"user alice"},
		locators: []map[string]string{{"line": "7", "byte": "128"}},
	}
	var record parseio.SourceRecord
	var output parseOutput
	err = parseSourceRecords(context.Background(), source, processor, &capturingParseSink{}, &record, &output, nil)
	assert.Requires(a.Error(err))

	for _, want := range []string{
		"parse file target.log line=7 byte=128:",
		"matched cluster 79 did not match during variable extraction",
		`template="user fixed"`,
		`line="user alice"`,
	} {
		assert.Requires(a.String(err.Error()).Contains(want))
	}
	assert.Requires(a.Number(source.acks).EqualTo(0))
}

func TestParseSourceRecordsWrapsProcessorErrorsWithGenericLocator(t *testing.T) {
	assert := a.New(t)
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       79,
				Size:     1,
				Template: "user <*>",
				Tokens:   []string{"user", "<*>"},
			},
		},
	}
	processor, err := newParseProcessor(model, nil)
	assert.Requires(a.NilError(err))

	processor.parseTemplates[79] = parseTemplate{
		id:       79,
		template: "user fixed",
		tokens:   []string{"user", "fixed"},
	}

	source := &fakeParseSource{
		kind:  "kafka",
		name:  "logs",
		lines: []string{"user alice"},
		locators: []map[string]string{{
			"topic":     "logs",
			"partition": "3",
			"offset":    "991284",
		}},
	}
	var record parseio.SourceRecord
	var output parseOutput
	err = parseSourceRecords(context.Background(), source, processor, &capturingParseSink{}, &record, &output, nil)
	assert.Requires(a.Error(err))

	for _, want := range []string{
		"parse kafka logs",
		"offset=991284",
		"partition=3",
		"topic=logs",
		`template="user fixed"`,
		`line="user alice"`,
	} {
		assert.Requires(a.String(err.Error()).Contains(want))
	}
}

func TestParseSourceRecordsTruncatesLongErrorLinePreview(t *testing.T) {
	assert := a.New(t)
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       79,
				Size:     1,
				Template: "user <*>",
				Tokens:   []string{"user", "<*>"},
			},
		},
	}
	processor, err := newParseProcessor(model, nil)
	assert.Requires(a.NilError(err))

	processor.parseTemplates[79] = parseTemplate{
		id:       79,
		template: "user fixed",
		tokens:   []string{"user", "fixed"},
	}

	longLine := "user " + strings.Repeat("a", parseErrorLineMaxBytes+20)
	source := &fakeParseSource{
		kind:     "file",
		name:     "target.log",
		lines:    []string{longLine},
		locators: []map[string]string{{"line": "1", "byte": "0"}},
	}
	var record parseio.SourceRecord
	var output parseOutput
	err = parseSourceRecords(context.Background(), source, processor, &capturingParseSink{}, &record, &output, nil)
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains(" (truncated)"))
	assert.Requires(a.String(err.Error()).NotContains(strings.Repeat("a", parseErrorLineMaxBytes+1)))
}

func TestRunParseWritesJSONLToLocalPrefix(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "user <*> logged in",
				Tokens:   []string{"user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "user alice logged in\nother line\n")
	outputPrefix := filepath.Join(dir, "parse-output")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath, "-output", outputPrefix}, &stdout, &stderr)))

	assert.Requires(a.Number(stdout.Len()).EqualTo(0))

	parts := localOutputParts(t, outputPrefix, "jsonl")
	assert.Requires(a.Number(len(parts)).EqualTo(1))

	assertBaseName(t, parts[0], "part-00000.jsonl")
	assertJSONLFileContent(t, parts[0],
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"alice"}},
		parseOutput{TemplateID: nil, ModelID: modelID, Variables: []string{}},
	)
}

func TestRunParseOmitsParametersFromJSONLPrefix(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\d+`, MaskWith: "NUM"}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "service id=<:NUM:> status <*>",
				Tokens:   []string{"service", "id=<:NUM:>", "status", "<*>"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "service id=123 status retry\n")
	outputPrefix := filepath.Join(dir, "parse-output")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath, "-output", outputPrefix}, &stdout, &stderr)))
	assert.Requires(a.Number(stdout.Len()).EqualTo(0))

	parts := localOutputParts(t, outputPrefix, "jsonl")
	assert.Requires(a.Number(len(parts)).EqualTo(1))
	assertJSONLFileContent(t, parts[0],
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"123", "retry"}},
	)
	contents, err := os.ReadFile(parts[0])
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(string(contents)).NotContains(`"parameters"`))
}

func TestRunParseRotatesJSONLByBatchSize(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     3,
				Template: "user <*> logged in",
				Tokens:   []string{"user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "user alice logged in\nuser bob logged in\nuser carol logged in\n")
	outputPrefix := filepath.Join(dir, "parse-output")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath, "-output", outputPrefix, "-batch-size", "2"}, &stdout, &stderr)))

	parts := localOutputParts(t, outputPrefix, "jsonl")
	assert.Requires(a.Number(len(parts)).EqualTo(2))

	assertBaseName(t, parts[0], "part-00000.jsonl")
	assertBaseName(t, parts[1], "part-00001.jsonl")
	assertSameRunDir(t, parts[0], parts[1])
	assertJSONLFileContent(t, parts[0],
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"alice"}},
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"bob"}},
	)
	assertJSONLFileContent(t, parts[1],
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"carol"}},
	)
}

func TestPartJSONLWriterRotatesByBatchMaxAge(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	outputPrefix := filepath.Join(dir, "parse-output")
	now := time.Date(2026, 5, 28, 15, 0, 0, 0, time.UTC)
	ctx := context.Background()
	writer, err := newParseSink(ctx, io.Discard, parseOutputOptions{
		Format:      parseFormatJSONL,
		Prefix:      outputPrefix,
		BatchSize:   100,
		BatchMaxAge: 5 * time.Second,
		RunID:       "test-run",
		Now: func() time.Time {
			return now
		},
	})
	assert.Requires(a.NilError(err))

	assert.Requires(a.NilError(writer.Write(ctx, parseOutput{ModelID: "model", Variables: []string{"first"}})))

	now = now.Add(5 * time.Second)

	assert.Requires(a.NilError(writer.Write(ctx, parseOutput{ModelID: "model", Variables: []string{"second"}})))

	assert.Requires(a.NilError(writer.Close(ctx)))

	parts := localOutputParts(t, outputPrefix, "jsonl")
	assert.Requires(a.Number(len(parts)).EqualTo(2))

	assertBaseName(t, parts[0], "part-00000.jsonl")
	assertBaseName(t, parts[1], "part-00001.jsonl")
	assertJSONLFileContent(t, parts[0],
		parseOutput{TemplateID: nil, ModelID: "model", Variables: []string{"first"}},
	)
	assertJSONLFileContent(t, parts[1],
		parseOutput{TemplateID: nil, ModelID: "model", Variables: []string{"second"}},
	)
}

func TestRunParseRejectsInvalidOutputOptions(t *testing.T) {
	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "format",
			args: []string{"parse", "-format", "xml"},
			want: "format must be",
		},
		{
			name: "batch size",
			args: []string{"parse", "-batch-size", "0"},
			want: "batch-size must be greater than 0",
		},
		{
			name: "batch max age",
			args: []string{"parse", "-batch-max-age", "0s"},
			want: "batch-max-age must be greater than 0",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert := a.New(t)
			var stdout bytes.Buffer
			err := run(test.args, &stdout, ioDiscard{})
			assert.Requires(a.Error(err))
			assert.Requires(a.String(err.Error()).Contains(test.want))
		})
	}
}

func TestRunParseRejectsInvalidS3OutputOptions(t *testing.T) {
	clearS3Env(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{ID: 1, Size: 1, Template: "user <*>", Tokens: []string{"user", "<*>"}},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	logPath := writeTestLog(t, dir, "user alice\n")

	for _, test := range []struct {
		name string
		args []string
		want string
	}{
		{
			name: "missing bucket",
			args: []string{"parse", "-filename", logPath, "-model", modelPath, "-output", "s3://"},
			want: "s3 output prefix must include a bucket",
		},
		{
			name: "missing endpoint",
			args: []string{"parse", "-filename", logPath, "-model", modelPath, "-output", "s3://bucket/prefix"},
			want: "s3 endpoint is required",
		},
		{
			name: "partial credentials",
			args: []string{"parse", "-filename", logPath, "-model", modelPath, "-output", "s3://bucket/prefix", "-s3-endpoint", "localhost:9000", "-s3-access-key-id", "access"},
			want: "s3 credentials require both",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert := a.New(t)
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			err := run(test.args, &stdout, &stderr)
			assert.Requires(a.Error(err))
			assert.Requires(a.String(err.Error()).Contains(test.want))
		})
	}
}

func TestResolveS3ConfigUsesFlagEnvCascade(t *testing.T) {
	assert := a.New(t)
	clearS3Env(t)
	t.Setenv("S3_ENDPOINT", "http://env:9000")
	t.Setenv("S3_REGION", "env-region")
	t.Setenv("S3_ACCESS_KEY_ID", "env-access")
	t.Setenv("S3_SECRET_ACCESS_KEY", "env-secret")
	t.Setenv("S3_USE_SSL", "false")
	t.Setenv("S3_PATH_STYLE", "true")

	config, err := parseio.ResolveS3Config(parseio.S3Options{
		Endpoint:        parseio.OptionalString{Value: "https://flag:9443", Set: true},
		Region:          parseio.OptionalString{Value: "flag-region", Set: true},
		AccessKeyID:     parseio.OptionalString{Value: "flag-access", Set: true},
		SecretAccessKey: parseio.OptionalString{Value: "flag-secret", Set: true},
		UseSSL:          parseio.OptionalBool{Value: true, Set: true},
		PathStyle:       parseio.OptionalBool{Value: false, Set: true},
	})
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(config.Endpoint).EqualTo("flag:9443"))
	assert.Requires(a.String(config.Region).EqualTo("flag-region"))
	assert.Requires(a.String(config.AccessKeyID).EqualTo("flag-access"))
	assert.Requires(a.String(config.SecretAccessKey).EqualTo("flag-secret"))

	assert.Requires(a.True(config.UseSSL))
	assert.Requires(a.Assert(!config.PathStyle, "flag should override env and disable path-style lookup"))
}

func TestResolveS3ConfigReadsKubernetesSecretFiles(t *testing.T) {
	assert := a.New(t)
	clearS3Env(t)
	dir := t.TempDir()
	endpointFile := writeSecretFile(t, dir, "endpoint", "http://secrets:9000\n")
	regionFile := writeSecretFile(t, dir, "region", "secret-region\n")
	accessKeyFile := writeSecretFile(t, dir, "access_key_id", "secret-access\n")
	secretKeyFile := writeSecretFile(t, dir, "secret_access_key", "secret-key\n")
	sessionTokenFile := writeSecretFile(t, dir, "session_token", "secret-session\n")
	useSSLFile := writeSecretFile(t, dir, "use_ssl", "false\n")
	pathStyleFile := writeSecretFile(t, dir, "path_style", "true\n")

	t.Setenv("S3_ENDPOINT_FILE", endpointFile)
	t.Setenv("S3_REGION_FILE", regionFile)
	t.Setenv("S3_ACCESS_KEY_ID_FILE", accessKeyFile)
	t.Setenv("S3_SECRET_ACCESS_KEY_FILE", secretKeyFile)
	t.Setenv("S3_SESSION_TOKEN_FILE", sessionTokenFile)
	t.Setenv("S3_USE_SSL_FILE", useSSLFile)
	t.Setenv("S3_PATH_STYLE_FILE", pathStyleFile)

	config, err := parseio.ResolveS3Config(parseio.S3Options{})
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(config.Endpoint).EqualTo("secrets:9000"))
	assert.Requires(a.String(config.Region).EqualTo("secret-region"))
	assert.Requires(a.String(config.AccessKeyID).EqualTo("secret-access"))
	assert.Requires(a.String(config.SecretAccessKey).EqualTo("secret-key"))
	assert.Requires(a.String(config.SessionToken).EqualTo("secret-session"))

	assert.Requires(a.Assert(!config.UseSSL, "secret file should disable SSL"))
	assert.Requires(a.True(config.PathStyle))
}

func TestResolveS3ConfigDirectValuesOverrideSecretFiles(t *testing.T) {
	assert := a.New(t)
	clearS3Env(t)
	dir := t.TempDir()
	endpointFile := writeSecretFile(t, dir, "endpoint", "http://secret:9000\n")
	accessKeyFile := writeSecretFile(t, dir, "access_key_id", "secret-access\n")
	secretKeyFile := writeSecretFile(t, dir, "secret_access_key", "secret-key\n")

	config, err := parseio.ResolveS3Config(parseio.S3Options{
		Endpoint:            parseio.OptionalString{Value: "https://flag:9443", Set: true},
		EndpointFile:        parseio.OptionalString{Value: endpointFile, Set: true},
		AccessKeyID:         parseio.OptionalString{Value: "flag-access", Set: true},
		AccessKeyIDFile:     parseio.OptionalString{Value: accessKeyFile, Set: true},
		SecretAccessKey:     parseio.OptionalString{Value: "flag-secret", Set: true},
		SecretAccessKeyFile: parseio.OptionalString{Value: secretKeyFile, Set: true},
	})
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(config.Endpoint).EqualTo("flag:9443"))
	assert.Requires(a.String(config.AccessKeyID).EqualTo("flag-access"))
	assert.Requires(a.String(config.SecretAccessKey).EqualTo("flag-secret"))

	assert.Requires(a.True(config.UseSSL))
}

func TestRunParseWritesJSONLToS3Prefix(t *testing.T) {
	assert := a.New(t)
	clearS3Env(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{ID: 1, Size: 1, Template: "user <*>", Tokens: []string{"user", "<*>"}},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "user alice\n")

	var captured struct {
		config      parseio.S3Config
		bucket      string
		key         string
		contentType string
		size        int64
		body        string
	}
	originalPutS3Object := parseio.PutS3Object
	parseio.PutS3Object = func(_ context.Context, config parseio.S3Config, bucket, key string, reader io.Reader, size int64, contentType string) error {
		body, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		captured.config = config
		captured.bucket = bucket
		captured.key = key
		captured.contentType = contentType
		captured.size = size
		captured.body = string(body)
		return nil
	}
	defer func() {
		parseio.PutS3Object = originalPutS3Object
	}()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"parse",
		"-filename", logPath,
		"-model", modelPath,
		"-output", "s3://bucket/prefix",
		"-s3-endpoint", "http://localhost:9000",
		"-s3-access-key-id", "access",
		"-s3-secret-access-key", "secret",
	}, &stdout, &stderr)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(captured.bucket).EqualTo("bucket"))
	assert.Requires(a.String(captured.key).HasPrefix("prefix/format=jsonl/run_id="))
	assert.Requires(a.String(captured.key).HasSuffix("/part-00000.jsonl"))
	assert.Requires(a.String(captured.contentType).EqualTo(parseJSONLContentType))
	assert.Requires(a.String(captured.config.Endpoint).EqualTo("localhost:9000"))

	assert.Requires(a.Assert(!captured.config.UseSSL, "http endpoint should default to non-SSL"))

	assertJSONLines(t, captured.body,
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"alice"}},
	)
	assert.Requires(a.Number(captured.size).EqualTo(int64(len(captured.body))))
}

func TestRunParseWritesParquetToLocalPrefix(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\d+`, MaskWith: "NUM"}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "service id=<:NUM:> path=/users/<:NUM:> status <*>",
				Tokens:   []string{"service", "id=<:NUM:>", "path=/users/<:NUM:>", "status", "<*>"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "service id=123 path=/users/42 status retry\nother line\n")
	outputPrefix := filepath.Join(dir, "parse-output")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath, "-format", "parquet", "-output", outputPrefix, "-include-parameters"}, &stdout, &stderr)))

	assert.Requires(a.Number(stdout.Len()).EqualTo(0))

	parts := localOutputParts(t, outputPrefix, "parquet")
	assert.Requires(a.Number(len(parts)).EqualTo(1))

	assertBaseName(t, parts[0], "part-00000.parquet")
	rows := readParquetParseRows(t, parts[0])
	assert.Requires(a.Slice(rows).EqualTo(
		parquetParseRow{
			TemplateID: int64Pointer(1),
			ModelID:    modelID,
			Variables:  []string{"123", "42", "retry"},
			Parameters: []parquetParameter{
				{Value: "123", MaskName: "NUM"},
				{Value: "42", MaskName: "NUM"},
				{Value: "retry", MaskName: "*"},
			},
		},
		parquetParseRow{
			TemplateID: nil,
			ModelID:    modelID,
			Variables:  []string{},
			Parameters: []parquetParameter{},
		},
	))
}

func TestRunParseOmitsParametersFromParquet(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\d+`, MaskWith: "NUM"}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "service id=<:NUM:> status <*>",
				Tokens:   []string{"service", "id=<:NUM:>", "status", "<*>"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "service id=123 status retry\n")
	outputPrefix := filepath.Join(dir, "parse-output")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath, "-format", "parquet", "-output", outputPrefix}, &stdout, &stderr)))
	assert.Requires(a.Number(stdout.Len()).EqualTo(0))

	parts := localOutputParts(t, outputPrefix, "parquet")
	assert.Requires(a.Number(len(parts)).EqualTo(1))
	rows := readParquetParseRowsWithoutParameters(t, parts[0])
	assert.Requires(a.Slice(rows).EqualTo(
		parquetParseRow{
			TemplateID: int64Pointer(1),
			ModelID:    modelID,
			Variables:  []string{"123", "retry"},
		},
	))
}

func TestRunParseWritesParquetToS3Prefix(t *testing.T) {
	assert := a.New(t)
	clearS3Env(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{ID: 1, Size: 1, Template: "user <*>", Tokens: []string{"user", "<*>"}},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "user alice\n")

	var captured struct {
		bucket      string
		key         string
		contentType string
		size        int64
		body        []byte
	}
	originalPutS3Object := parseio.PutS3Object
	parseio.PutS3Object = func(_ context.Context, _ parseio.S3Config, bucket, key string, reader io.Reader, size int64, contentType string) error {
		body, err := io.ReadAll(reader)
		if err != nil {
			return err
		}
		captured.bucket = bucket
		captured.key = key
		captured.contentType = contentType
		captured.size = size
		captured.body = body
		return nil
	}
	defer func() {
		parseio.PutS3Object = originalPutS3Object
	}()

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	err := run([]string{
		"parse",
		"-filename", logPath,
		"-model", modelPath,
		"-format", "parquet",
		"-output", "s3://bucket/prefix",
		"-s3-endpoint", "localhost:9000",
		"-s3-access-key-id", "access",
		"-s3-secret-access-key", "secret",
	}, &stdout, &stderr)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(captured.bucket).EqualTo("bucket"))
	assert.Requires(a.String(captured.key).HasPrefix("prefix/format=parquet/run_id="))
	assert.Requires(a.String(captured.key).HasSuffix("/part-00000.parquet"))
	assert.Requires(a.String(captured.contentType).EqualTo(parseParquetContentType))

	assert.Requires(a.Number(captured.size).EqualTo(int64(len(captured.body))))

	parquetPath := filepath.Join(dir, "captured.parquet")
	if err := os.WriteFile(parquetPath, captured.body, 0o644); err != nil {
		t.Fatalf("write captured parquet: %v", err)
	}
	rows := readParquetParseRowsWithoutParameters(t, parquetPath)
	assert.Requires(a.Slice(rows).EqualTo(
		parquetParseRow{
			TemplateID: int64Pointer(1),
			ModelID:    modelID,
			Variables:  []string{"alice"},
		},
	))
}

func TestRunTestReportsTemplateDistribution(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "user <*> logged in",
				Tokens:   []string{"user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)

	logPath := filepath.Join(dir, "target.log")
	logContent := "user alice logged in\nuser bob logged in\nother line\n"
	if err := os.WriteFile(logPath, []byte(logContent), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"test", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assertJSONValue(t, stdout.String(), testOutput{
		Total:     3,
		Matched:   2,
		Unmatched: 1,
		Templates: []templateDistribution{
			{TemplateID: 1, ModelID: modelID, Template: "user <*> logged in", Count: 2},
		},
	})
}

func TestRunTestUsesFallbackFullSearch(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := writeFallbackModel(t, dir)
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "alpha target ready\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"test", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assertJSONValue(t, stdout.String(), testOutput{
		Total:     1,
		Matched:   1,
		Unmatched: 0,
		Templates: []templateDistribution{
			{TemplateID: 1, ModelID: modelID, Template: "alpha fixed ready", Count: 0},
			{TemplateID: 2, ModelID: modelID, Template: "<*> target ready", Count: 1},
		},
	})
}

func TestRunTestRestoresExtraDelimiters(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:         modelVersion,
		ParamString:     "<*>",
		ExtraDelimiters: []string{"_"},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "user <*> logged in",
				Tokens:   []string{"user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	logPath := writeTestLog(t, dir, "user_alice_logged_in\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"test", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assert.Requires(a.String(stdout.String()).Contains("\"matched\": 1"))
	assert.Requires(a.String(stdout.String()).Contains("\"count\": 1"))
}

func TestRunTrainWritesSimilarityThreshold(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "user alice logged in\nuser bob logged in\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-filename", logPath, "-model", modelPath, "-sim-th", "0.73"}, &stdout, ioDiscard{})))

	assertModelSimTh(t, modelPath, 0.73)
}

func TestRunTrainWritesTreeConfigFlags(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "host web-001 ready\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{
		"train",
		"-filename", logPath,
		"-model", modelPath,
		"-depth", "7",
		"-max-children", "13",
		"-parametrize-numeric-tokens=false",
	}, &stdout, ioDiscard{}),
	))

	assertModelTreeConfig(t, modelPath, 7, 13, false)
}

func TestRunTrainWritesExtraDelimiters(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "user:logged_in:alice\nuser:logged_in:bob\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{
		"train",
		"-filename", logPath,
		"-model", modelPath,
		"-extra-delimiter", "_",
		"-extra-delimiter", ":",
	}, &stdout, ioDiscard{}),
	))

	assertModelExtraDelimiters(t, modelPath, []string{"_", ":"})
	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(model.Templates[0].Template).EqualTo("user logged in <*>"))
}

func TestRunTrainWritesDefaultMaskingRules(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "user alice logged in\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assertModelMaskingRules(t, modelPath, modelMaskingRules(defaultMaskingRules()))
}

func TestRunTrainDefaultMaskingRulesAffectTemplates(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "device ab:cd:ef:01 addr 10.1.2.3 seq abcdef 123456 fedcba hex 0xdeadbeef num -42\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Number(len(model.Templates)).EqualTo(1))

	want := "device <:ID:> addr <:IP:> seq <:SEQ:> hex <:HEX:> num <:NUM:>"

	assert.Requires(a.String(model.Templates[0].Template).EqualTo(want))
}

func TestRunTrainMaskingRulesFileReplacesDefaultsAndSupportsRegexPattern(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "2026-05-28T12:34:56Z device GPU-7 addr 10.1.2.3\n")
	rulesPath := filepath.Join(dir, "masks.json")
	rulesContent := `[
  {"regex_pattern":"^\\d{4}-\\d{2}-\\d{2}T\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?(?:Z|[+-]\\d{2}:\\d{2})","mask_with":"TIMESTAMP"},
  {"pattern":"\\bGPU-\\d+\\b","mask_with":"GPU"}
]
`
	if err := os.WriteFile(rulesPath, []byte(rulesContent), 0o644); err != nil {
		t.Fatalf("write masking rules: %v", err)
	}

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-filename", logPath, "-model", modelPath, "-masking-rules", rulesPath}, &stdout, ioDiscard{})))

	wantRules := []modelMaskingRule{
		{Pattern: `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})`, MaskWith: "TIMESTAMP"},
		{Pattern: `\bGPU-\d+\b`, MaskWith: "GPU"},
	}
	assertModelMaskingRules(t, modelPath, wantRules)

	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))

	for _, rule := range model.MaskingRules {
		assert.Requires(a.Match(rule.Pattern, a.Not(a.EqualTo(timestampPrefixPattern))))
	}
	assert.Requires(a.String(model.Templates[0].Template).EqualTo("<:TIMESTAMP:> device <:GPU:> addr 10.1.2.3"))
}

func TestRunTrainWritesMetadataFileAndCreatedAt(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "user alice logged in\n")
	metadataPath := filepath.Join(dir, "system.json")
	metadataContent := `{
  "source": "lsb_release",
  "system": {
    "os": "Ubuntu 24.04.2 LTS",
    "arch": "aarch64",
    "kernel": "6.14.0-1008-nvidia-64k"
  },
  "created_at": "1999-01-01T00:00:00Z",
  "updated_at": "1999-01-01T00:00:00Z"
}
`
	if err := os.WriteFile(metadataPath, []byte(metadataContent), 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-filename", logPath, "-model", modelPath, "-metadata", metadataPath}, &stdout, ioDiscard{})))

	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))

	createdAt := assertMetadataUTCTimestamp(t, model.Metadata, "created_at")
	assert.Requires(a.Match(createdAt, a.Not(a.EqualTo("1999-01-01T00:00:00Z"))))
	_, ok := model.Metadata["updated_at"]
	assert.Requires(a.False(ok))

	assertMetadataString(t, model.Metadata, "source", "lsb_release")
	var system map[string]string
	decodeMetadataValue(t, model.Metadata, "system", &system)
	assert.Requires(a.String(system["os"]).EqualTo("Ubuntu 24.04.2 LTS"))
	assert.Requires(a.String(system["arch"]).EqualTo("aarch64"))
}

func TestRunTrainUpdateMergesMetadataAndWritesUpdatedAt(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	createdAt := "2026-05-01T12:00:00Z"
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Metadata: map[string]json.RawMessage{
			"created_at": metadataString(createdAt),
			"owner":      metadataString("kernel-team"),
			"system":     json.RawMessage(`{"os":"Ubuntu 22.04","arch":"x86_64"}`),
		},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user alice",
				Tokens:   []string{"user", "alice"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	logPath := writeTestLog(t, dir, "user bob\n")
	metadataPath := filepath.Join(dir, "system.json")
	metadataContent := `{
  "run_id": "second",
  "system": {
    "arch": "aarch64"
  },
  "created_at": "1999-01-01T00:00:00Z",
  "updated_at": "1999-01-01T00:00:00Z"
}
`
	if err := os.WriteFile(metadataPath, []byte(metadataContent), 0o644); err != nil {
		t.Fatalf("write metadata: %v", err)
	}

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath, "-metadata", metadataPath}, &stdout, ioDiscard{})))

	updatedModel, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))

	assertMetadataString(t, updatedModel.Metadata, "created_at", createdAt)
	updatedAt := assertMetadataUTCTimestamp(t, updatedModel.Metadata, "updated_at")
	assert.Requires(a.Match(updatedAt, a.Not(a.EqualTo("1999-01-01T00:00:00Z"))))

	assertMetadataString(t, updatedModel.Metadata, "owner", "kernel-team")
	assertMetadataString(t, updatedModel.Metadata, "run_id", "second")
	var system map[string]string
	decodeMetadataValue(t, updatedModel.Metadata, "system", &system)
	assert.Requires(a.String(system["arch"]).EqualTo("aarch64"))

	_, ok := system["os"]
	assert.Requires(a.False(ok))
}

func TestRunTrainUpdateReplacesInvalidCreatedAtMetadata(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Metadata: map[string]json.RawMessage{
			"created_at": json.RawMessage(`"not a timestamp"`),
		},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user alice",
				Tokens:   []string{"user", "alice"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	logPath := writeTestLog(t, dir, "user bob\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	updatedModel, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))

	createdAt := assertMetadataUTCTimestamp(t, updatedModel.Metadata, "created_at")
	assert.Requires(a.Match(createdAt, a.Not(a.EqualTo("not a timestamp"))))

	assertMetadataUTCTimestamp(t, updatedModel.Metadata, "updated_at")
}

func TestRunTrainRejectsInvalidMetadataFile(t *testing.T) {
	for _, test := range []struct {
		name    string
		content *string
		want    string
	}{
		{
			name: "missing file",
			want: "read metadata",
		},
		{
			name:    "invalid json",
			content: stringPointer(`{"system":`),
			want:    "decode metadata",
		},
		{
			name:    "array",
			content: stringPointer(`[]`),
			want:    "must contain a JSON object",
		},
		{
			name:    "null",
			content: stringPointer(`null`),
			want:    "must contain a JSON object",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert := a.New(t)
			dir := t.TempDir()
			modelPath := filepath.Join(dir, "model.json")
			logPath := writeTestLog(t, dir, "user alice logged in\n")
			metadataPath := filepath.Join(dir, "metadata.json")
			if test.content != nil {
				if err := os.WriteFile(metadataPath, []byte(*test.content), 0o644); err != nil {
					t.Fatalf("write metadata: %v", err)
				}
			}

			var stdout bytes.Buffer
			err := run([]string{"train", "-filename", logPath, "-model", modelPath, "-metadata", metadataPath}, &stdout, ioDiscard{})
			assert.Requires(a.Error(err))
			assert.Requires(a.String(err.Error()).Contains(test.want))
		})
	}
}

func TestRunTrainUpdatePreservesSavedSimilarityThreshold(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeThresholdModel(t, modelPath, 0.82)
	logPath := writeTestLog(t, dir, "user alice\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assertModelSimTh(t, modelPath, 0.82)
}

func TestRunTrainUpdateOverridesSavedSimilarityThreshold(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeThresholdModel(t, modelPath, 0.82)
	logPath := writeTestLog(t, dir, "user alice\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath, "-sim-th", "0.55"}, &stdout, ioDiscard{})))

	assertModelSimTh(t, modelPath, 0.55)
}

func TestRunTrainUpdatePreservesSavedTreeConfig(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeTreeConfigModel(t, modelPath, 7, 13, false)
	logPath := writeTestLog(t, dir, "user bob\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assertModelTreeConfig(t, modelPath, 7, 13, false)
}

func TestRunTrainUpdateOverridesSavedTreeConfig(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeTreeConfigModel(t, modelPath, 7, 13, false)
	logPath := writeTestLog(t, dir, "user bob\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{
		"train",
		"-update",
		"-filename", logPath,
		"-model", modelPath,
		"-depth", "5",
		"-max-children", "9",
		"-parametrize-numeric-tokens=true",
	}, &stdout, ioDiscard{}),
	))

	assertModelTreeConfig(t, modelPath, 5, 9, true)
}

func TestRunTrainUpdatePreservesSavedExtraDelimiters(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeExtraDelimiterModel(t, modelPath, []string{"_"})
	logPath := writeTestLog(t, dir, "user_bob\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assertModelExtraDelimiters(t, modelPath, []string{"_"})
}

func TestRunTrainUpdateOverridesSavedExtraDelimiters(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeExtraDelimiterModel(t, modelPath, []string{"_"})
	logPath := writeTestLog(t, dir, "service:bob\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{
		"train",
		"-update",
		"-filename", logPath,
		"-model", modelPath,
		"-extra-delimiter", ":",
	}, &stdout, ioDiscard{}),
	))

	assertModelExtraDelimiters(t, modelPath, []string{":"})
}

func TestRunTrainUpdatePreservesSavedMaskingRules(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	savedRules := []modelMaskingRule{{Pattern: `\bnode-\d+\b`, MaskWith: "NODE"}}
	writeMaskingRulesModel(t, modelPath, savedRules)
	logPath := writeTestLog(t, dir, "node-2 ready\n")

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{})))

	assertModelMaskingRules(t, modelPath, savedRules)
}

func TestRunTrainUpdateOverridesSavedMaskingRules(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeMaskingRulesModel(t, modelPath, []modelMaskingRule{{Pattern: `\bnode-\d+\b`, MaskWith: "NODE"}})
	logPath := writeTestLog(t, dir, "node-2 ready\n")
	rulesPath := filepath.Join(dir, "masks.json")
	rulesContent := `[{"pattern":"\\bGPU-\\d+\\b","mask_with":"GPU"}]`
	if err := os.WriteFile(rulesPath, []byte(rulesContent), 0o644); err != nil {
		t.Fatalf("write masking rules: %v", err)
	}

	var stdout bytes.Buffer

	assert.Requires(a.NilError(run([]string{"train", "-update", "-filename", logPath, "-model", modelPath, "-masking-rules", rulesPath}, &stdout, ioDiscard{})))

	assertModelMaskingRules(t, modelPath, []modelMaskingRule{{Pattern: `\bGPU-\d+\b`, MaskWith: "GPU"}})
}

func TestRunTrainRejectsInvalidSimilarityThreshold(t *testing.T) {
	assert := a.New(t)
	var stdout bytes.Buffer
	err := run([]string{"train", "-filename", "missing.log", "-model", "model.json", "-sim-th", "1.1"}, &stdout, ioDiscard{})
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("sim-th must be between 0 and 1"))
}

func TestRunTrainRejectsInvalidMaxChildren(t *testing.T) {
	assert := a.New(t)
	var stdout bytes.Buffer
	err := run([]string{"train", "-filename", "missing.log", "-model", "model.json", "-max-children", "0"}, &stdout, ioDiscard{})
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("max-children must be at least 1"))
}

func TestRunTrainRejectsEmptyExtraDelimiter(t *testing.T) {
	assert := a.New(t)
	var stdout bytes.Buffer
	err := run([]string{"train", "-filename", "missing.log", "-model", "model.json", "-extra-delimiter", ""}, &stdout, ioDiscard{})
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("extra delimiter must not be empty"))
}

func TestRunTrainRejectsInvalidMaskingRulesFile(t *testing.T) {
	for _, test := range []struct {
		name    string
		content *string
		want    string
	}{
		{
			name: "missing file",
			want: "read masking rules",
		},
		{
			name:    "invalid json",
			content: stringPointer(`[{"pattern":`),
			want:    "decode masking rules",
		},
		{
			name:    "object",
			content: stringPointer(`{"pattern":"\\d+"}`),
			want:    "must contain a JSON array",
		},
		{
			name:    "null",
			content: stringPointer(`null`),
			want:    "must contain a JSON array",
		},
		{
			name:    "empty pattern",
			content: stringPointer(`[{"mask_with":"NUM"}]`),
			want:    "pattern must not be empty",
		},
		{
			name:    "invalid regex",
			content: stringPointer(`[{"pattern":"["}]`),
			want:    "compile masking_rules[0] pattern",
		},
		{
			name:    "conflicting aliases",
			content: stringPointer(`[{"pattern":"a","regex_pattern":"b"}]`),
			want:    "conflicting pattern and regex_pattern",
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			assert := a.New(t)
			dir := t.TempDir()
			modelPath := filepath.Join(dir, "model.json")
			logPath := writeTestLog(t, dir, "user alice logged in\n")
			rulesPath := filepath.Join(dir, "masks.json")
			if test.content != nil {
				if err := os.WriteFile(rulesPath, []byte(*test.content), 0o644); err != nil {
					t.Fatalf("write masking rules: %v", err)
				}
			}

			var stdout bytes.Buffer
			err := run([]string{"train", "-filename", logPath, "-model", modelPath, "-masking-rules", rulesPath}, &stdout, ioDiscard{})
			assert.Requires(a.Error(err))
			assert.Requires(a.String(err.Error()).Contains(test.want))
		})
	}
}

func TestReadOldModelWithoutSimilarityThresholdUsesDefault(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	oldModel := `{
  "version": 1,
  "param_string": "<*>",
  "masking_rules": [],
  "templates": [
    {
      "id": 1,
      "size": 1,
      "template": "user <*>",
      "tokens": ["user", "<*>"]
    }
  ]
}
`
	if err := os.WriteFile(modelPath, []byte(oldModel), 0o644); err != nil {
		t.Fatalf("write old model: %v", err)
	}

	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Nil(model.SimTh))
	assert.Requires(a.Number(configFromModel(model).SimTh).EqualTo(clusterConfig().SimTh))

	config := configFromModel(model)
	assert.Requires(a.Number(config.LogClusterDepth).EqualTo(clusterConfig().LogClusterDepth))
	assert.Requires(a.Number(config.MaxChildren).EqualTo(clusterConfig().MaxChildren))

	assert.Requires(a.Assert(!config.PreserveNumericTokens, "old model should default to parameterizing numeric tokens"))
	assert.Requires(a.Number(len(config.ExtraDelimiters)).EqualTo(0))
}

func TestReadModelRejectsInvalidSimilarityThreshold(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := `{
  "version": 1,
  "param_string": "<*>",
  "sim_th": -0.1,
  "masking_rules": [],
  "templates": []
}
`
	if err := os.WriteFile(modelPath, []byte(model), 0o644); err != nil {
		t.Fatalf("write model: %v", err)
	}

	_, _, err := readModel(modelPath)
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("model sim_th must be between 0 and 1"))
}

func TestReadModelRejectsInvalidMaxChildren(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := `{
  "version": 1,
  "param_string": "<*>",
  "max_children": 0,
  "masking_rules": [],
  "templates": []
}
`
	if err := os.WriteFile(modelPath, []byte(model), 0o644); err != nil {
		t.Fatalf("write model: %v", err)
	}

	_, _, err := readModel(modelPath)
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("model max_children must be at least 1"))
}

func TestReadModelRejectsEmptyExtraDelimiter(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := `{
  "version": 1,
  "param_string": "<*>",
  "extra_delimiters": ["_",""],
  "masking_rules": [],
  "templates": []
}
`
	if err := os.WriteFile(modelPath, []byte(model), 0o644); err != nil {
		t.Fatalf("write model: %v", err)
	}

	_, _, err := readModel(modelPath)
	assert.Requires(a.Error(err))
	assert.Requires(a.String(err.Error()).Contains("model extra_delimiters[1] must not be empty"))
}

func TestReadModelComputesStableBase64URLModelID(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	firstPath := filepath.Join(dir, "first.json")
	secondPath := filepath.Join(dir, "second.json")
	firstModel := `{
  "version": 1,
  "param_string": "<*>",
  "metadata": {"owner": "first"},
  "masking_rules": [],
  "templates": [
    {
      "tokens": ["<*>", "target", "ready"],
      "template": "<*> target ready",
      "size": 4,
      "id": 2
    },
    {
      "size": 1,
      "id": 1,
      "tokens": ["alpha", "fixed", "ready"],
      "template": "alpha fixed ready"
    }
  ]
}
`
	secondModel := `{
  "version": 1,
  "param_string": "<*>",
  "metadata": {"owner": "second"},
  "masking_rules": [],
  "templates": [
    {
      "id": 1,
      "size": 1,
      "template": "alpha fixed ready",
      "tokens": ["alpha", "fixed", "ready"]
    },
    {
      "id": 2,
      "size": 4,
      "template": "<*> target ready",
      "tokens": ["<*>", "target", "ready"]
    }
  ]
}
`
	if err := os.WriteFile(firstPath, []byte(firstModel), 0o644); err != nil {
		t.Fatalf("write first model: %v", err)
	}
	if err := os.WriteFile(secondPath, []byte(secondModel), 0o644); err != nil {
		t.Fatalf("write second model: %v", err)
	}

	first, _, err := readModel(firstPath)
	assert.Requires(a.NilError(err))

	second, _, err := readModel(secondPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(first.ModelID).EqualTo(second.ModelID))

	decoded, err := base64.RawURLEncoding.DecodeString(first.ModelID)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Number(len(decoded)).EqualTo(32))
	assert.Requires(a.String(first.ModelID).NotContains("="))
}

func TestModelIDChangesWhenTemplateContentChanges(t *testing.T) {
	baseTemplates := []templateModel{
		{
			ID:       1,
			Size:     2,
			Template: "user <*> logged in",
			Tokens:   []string{"user", "<*>", "logged", "in"},
		},
	}
	baseID := modelIDFromTemplates(baseTemplates)

	tests := []struct {
		name      string
		templates []templateModel
	}{
		{
			name: "id",
			templates: []templateModel{
				{ID: 2, Size: 2, Template: "user <*> logged in", Tokens: []string{"user", "<*>", "logged", "in"}},
			},
		},
		{
			name: "size",
			templates: []templateModel{
				{ID: 1, Size: 3, Template: "user <*> logged in", Tokens: []string{"user", "<*>", "logged", "in"}},
			},
		},
		{
			name: "template",
			templates: []templateModel{
				{ID: 1, Size: 2, Template: "user <*> logged out", Tokens: []string{"user", "<*>", "logged", "in"}},
			},
		},
		{
			name: "tokens",
			templates: []templateModel{
				{ID: 1, Size: 2, Template: "user <*> logged in", Tokens: []string{"user", "<*>", "logged", "out"}},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := a.New(t)

			assert.Requires(a.Match(modelIDFromTemplates(tt.templates),
				a.Not(a.EqualTo(baseID))))

		})
	}
}

func TestWriteModelDoesNotPersistModelID(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ModelID:     "cached-model-id",
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user alice",
				Tokens:   []string{"user", "alice"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	contents, err := os.ReadFile(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(string(contents)).NotContains("model_id"))
	assert.Requires(a.String(string(contents)).NotContains("cached-model-id"))
}

func TestRunParseExtractsMaskedRawValuesWithSpaces(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: timestampPrefixPattern}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "<*> user <*> logged in",
				Tokens:   []string{"<*>", "user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)

	logPath := filepath.Join(dir, "target.log")
	logContent := "[Mon May 11 13:41:21 2026]\t  user   alice\tlogged  in\n"
	if err := os.WriteFile(logPath, []byte(logContent), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"[Mon May 11 13:41:21 2026]", "alice"}},
	)
}

func TestRunParseExtractsExtraDelimiterVariables(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:         modelVersion,
		ParamString:     "<*>",
		ExtraDelimiters: []string{"_"},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "user <*> logged in",
				Tokens:   []string{"user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "user_alice_logged_in\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"alice"}},
	)
}

func TestRunParsePreservesMaskedValuesWithExtraDelimiters(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:         modelVersion,
		ParamString:     "<*>",
		ExtraDelimiters: []string{":"},
		MaskingRules:    []modelMaskingRule{{Pattern: timestampPrefixPattern}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "<*> user <*> logged in",
				Tokens:   []string{"<*>", "user", "<*>", "logged", "in"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "[Mon May 11 13:41:21 2026]:user:alice:logged:in\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"[Mon May 11 13:41:21 2026]", "alice"}},
	)
}

func TestRunParseUsesFallbackFullSearch(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := writeFallbackModel(t, dir)
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "alpha target ready\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(2), ModelID: modelID, Variables: []string{"alpha"}},
	)
}

func TestRunParseIncludesNamedParametersForEmbeddedMasks(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\d+`, MaskWith: "NUM"}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     2,
				Template: "service id=<:NUM:> path=/users/<:NUM:> status <*>",
				Tokens:   []string{"service", "id=<:NUM:>", "path=/users/<:NUM:>", "status", "<*>"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "service id=123 path=/users/42 status retry\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath, "-include-parameters"}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{
			TemplateID: intPointer(1),
			ModelID:    modelID,
			Variables:  []string{"123", "42", "retry"},
			Parameters: []drain.ExtractedParameter{
				{Value: "123", MaskName: "NUM"},
				{Value: "42", MaskName: "NUM"},
				{Value: "retry", MaskName: "*"},
			},
		},
	)
}

func TestRunParseOmitsParametersFromStdoutJSONL(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\d+`, MaskWith: "NUM"}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "service id=<:NUM:> status <*>",
				Tokens:   []string{"service", "id=<:NUM:>", "status", "<*>"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "service id=123 status retry\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{"123", "retry"}},
	)
	assert.Requires(a.String(stdout.String()).NotContains(`"parameters"`))
}

func TestRunParseKeepsLegacyPlainMaskWithLiteralModels(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\b\d{1,3}(?:\.\d{1,3}){3}\b`, MaskWith: "IP"}},
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "connected to IP",
				Tokens:   []string{"connected", "to", "IP"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "connected to 10.0.0.1\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(1), ModelID: modelID, Variables: []string{}},
	)
}

func TestRunParseFallbackMatchesEmbeddedLegacyLiteralMasks(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: []modelMaskingRule{{Pattern: `\b\d{1,3}(?:\.\d{1,3}){3}\b`, MaskWith: "IP"}},
		Templates: []templateModel{
			{
				ID:       79,
				Size:     1,
				Template: "target=IP status ok",
				Tokens:   []string{"target=IP", "status", "ok"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "target=10.0.0.1 status ok\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer

	assert.Requires(a.NilError(run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr)))

	assertJSONLines(t, stdout.String(),
		parseOutput{TemplateID: intPointer(79), ModelID: modelID, Variables: []string{}},
	)
}

func TestTokenizeLineFastPathMatchesLegacy(t *testing.T) {
	assert := a.New(t)
	compiledRules, err := compileMaskingRules([]modelMaskingRule{
		{Pattern: `\[[^\]]+\]`},
	}, "<*>")
	assert.Requires(a.NilError(err))

	for _, line := range []string{
		"[Mon May 11 13:41:21 2026] user alice",
		"user [Mon May 11 13:41:21 2026] logged in",
		"user alice logged in",
		"alpha  [Mon May 11 13:41:21 2026]  beta",
		"alpha\t[Mon May 11 13:41:21 2026]\t beta",
		"   ",
		"\t\t",
		"prefix[Mon May 11 13:41:21 2026]suffix",
	} {
		t.Run(line, func(t *testing.T) {
			assert := a.New(t)
			got := tokenizeLine(line, compiledRules, nil)
			want := tokenizeLineLegacy(line, compiledRules, nil)
			assert.Requires(a.Assert(cmp.Equal(got, want, cmp.Exporter(func(reflect.Type) bool {
				return true
			})), "got <%#v>, wanted <%#v>", got, want))
		})
	}
	_, ok := tokenizeLineSingleMask("prefix[Mon May 11 13:41:21 2026]suffix", compiledRules[0])
	assert.Requires(a.False(ok))
}

func TestTokenizeLineRestoresEmbeddedMasks(t *testing.T) {
	assert := a.New(t)
	compiledRules, err := compileMaskingRules([]modelMaskingRule{
		{Pattern: `\b\d{1,3}(?:\.\d{1,3}){3}\b`, Replacement: "IP"},
	}, "<*>")
	assert.Requires(a.NilError(err))

	got := tokenizeLine("target=10.0.0.1 status ok", compiledRules, nil)
	want := []lineToken{
		{value: "target=IP", rawString: "target=10.0.0.1"},
		{value: "status", rawString: "status"},
		{value: "ok", rawString: "ok"},
	}
	assert.Requires(a.Assert(cmp.Equal(got, want, cmp.Exporter(func(reflect.Type) bool {
		return true
	})), "got <%#v>, wanted <%#v>", got, want))
}

func TestTokenizeLineUsesExtraDelimitersOutsideMasks(t *testing.T) {
	assert := a.New(t)
	compiledRules, err := compileMaskingRules([]modelMaskingRule{
		{Pattern: timestampPrefixPattern},
	}, "<*>")
	assert.Requires(a.NilError(err))

	got := tokenizeLine("[Mon May 11 13:41:21 2026]:user:alice", compiledRules, []string{":"})
	want := []lineToken{
		{value: "<*>", rawString: "[Mon May 11 13:41:21 2026]"},
		{value: "user", rawString: "user"},
		{value: "alice", rawString: "alice"},
	}
	assert.Requires(a.Assert(cmp.Equal(got, want, cmp.Exporter(func(reflect.Type) bool {
		return true
	})), "got <%#v>, wanted <%#v>", got, want))
}

func writeTestLog(t *testing.T, dir, content string) string {
	t.Helper()
	logPath := filepath.Join(dir, "train.log")
	if err := os.WriteFile(logPath, []byte(content), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}
	return logPath
}

func localOutputParts(t *testing.T, prefix, format string) []string {
	t.Helper()
	assert := a.New(t)
	matches, err := filepath.Glob(filepath.Join(prefix, "format="+format, "run_id=*", "part-*."+format))
	assert.Requires(a.NilError(err))

	return matches
}

func assertBaseName(t *testing.T, path, want string) {
	t.Helper()
	assert := a.New(t)

	assert.Requires(a.String(filepath.Base(path)).EqualTo(want))
}

func assertSameRunDir(t *testing.T, first, second string) {
	t.Helper()
	assert := a.New(t)
	assert.Requires(a.String(filepath.Dir(first)).EqualTo(filepath.Dir(second)))
}

func assertJSONValue(t *testing.T, actual string, expected any) {
	t.Helper()
	assert := a.New(t)
	assert.Requires(a.String(actual).HasSuffix("\n"))
	assert.Requires(jsonassert.Equal(actual, mustMarshalJSON(t, expected)))
}

func assertJSONLines(t *testing.T, actual string, expected ...any) {
	t.Helper()
	assert := a.New(t)
	assert.Requires(a.String(actual).HasSuffix("\n"))

	lines := strings.Split(strings.TrimSuffix(actual, "\n"), "\n")
	assert.Requires(a.Number(len(lines)).EqualTo(len(expected)))
	for i, expectedValue := range expected {
		assert.Requires(jsonassert.Equal(lines[i], mustMarshalJSON(t, expectedValue)))
	}
}

func assertJSONLFileContent(t *testing.T, path string, expected ...any) {
	t.Helper()
	assert := a.New(t)
	contents, err := os.ReadFile(path)
	assert.Requires(a.NilError(err))
	assertJSONLines(t, string(contents), expected...)
}

func mustMarshalJSON(t *testing.T, value any) string {
	t.Helper()
	assert := a.New(t)
	encoded, err := json.Marshal(value)
	assert.Requires(a.NilError(err))
	return string(encoded)
}

func writeSecretFile(t *testing.T, dir, name, content string) string {
	t.Helper()
	path := filepath.Join(dir, name)
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write secret file: %v", err)
	}
	return path
}

type parquetParameter struct {
	Value    string
	MaskName string
}

type parquetParseRow struct {
	TemplateID *int64
	ModelID    string
	Variables  []string
	Parameters []parquetParameter
}

func readParquetParseRows(t *testing.T, parquetPath string) []parquetParseRow {
	return readParquetParseRowsWithParameters(t, parquetPath, true)
}

func readParquetParseRowsWithoutParameters(t *testing.T, parquetPath string) []parquetParseRow {
	return readParquetParseRowsWithParameters(t, parquetPath, false)
}

func readParquetParseRowsWithParameters(t *testing.T, parquetPath string, includeParameters bool) []parquetParseRow {
	t.Helper()
	assert := a.New(t)
	reader, err := os.Open(parquetPath)
	assert.Requires(a.NilError(err))

	defer reader.Close()

	table, err := pqarrow.ReadTable(context.Background(), reader, nil, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	assert.Requires(a.NilError(err))

	defer table.Release()
	expectedColumns := int64(3)
	if includeParameters {
		expectedColumns = 4
	}
	assert.Requires(a.Number(table.NumCols()).EqualTo(expectedColumns))
	assert.Requires(a.String(table.Schema().Field(0).Name).EqualTo("template_id"))
	assert.Requires(a.String(table.Schema().Field(1).Name).EqualTo("model_id"))
	assert.Requires(a.String(table.Schema().Field(2).Name).EqualTo("variables"))
	if includeParameters {
		assert.Requires(a.String(table.Schema().Field(3).Name).EqualTo("parameters"))
	}

	templateIDs := singleChunk[*array.Int64](t, table, 0)
	modelIDs := singleChunk[*array.String](t, table, 1)
	variables := singleChunk[*array.List](t, table, 2)
	variableValues := variables.ListValues().(*array.String)
	var parameters *array.List
	var parameterValues *array.String
	var parameterMaskNames *array.String
	if includeParameters {
		parameters = singleChunk[*array.List](t, table, 3)
		parameterStructs := parameters.ListValues().(*array.Struct)
		parameterValues = parameterStructs.Field(0).(*array.String)
		parameterMaskNames = parameterStructs.Field(1).(*array.String)
	}

	rows := make([]parquetParseRow, 0, table.NumRows())
	for i := 0; i < int(table.NumRows()); i++ {
		var templateID *int64
		if !templateIDs.IsNull(i) {
			templateID = int64Pointer(templateIDs.Value(i))
		}
		row := parquetParseRow{
			TemplateID: templateID,
			ModelID:    modelIDs.Value(i),
			Variables:  stringListValues(variables, variableValues, i),
		}
		if includeParameters {
			row.Parameters = parameterListValues(parameters, parameterValues, parameterMaskNames, i)
		}
		rows = append(rows, row)
	}
	return rows
}

func singleChunk[T arrow.Array](t *testing.T, table arrow.Table, column int) T {
	t.Helper()
	assert := a.New(t)
	chunks := table.Column(column).Data().Chunks()
	assert.Requires(a.Number(len(chunks)).EqualTo(1))

	chunk, ok := chunks[0].(T)
	assert.Requires(a.True(ok))

	return chunk
}

func stringListValues(list *array.List, values *array.String, row int) []string {
	start, end := list.ValueOffsets(row)
	result := make([]string, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, values.Value(int(i)))
	}
	return result
}

func parameterListValues(list *array.List, values *array.String, maskNames *array.String, row int) []parquetParameter {
	start, end := list.ValueOffsets(row)
	result := make([]parquetParameter, 0, end-start)
	for i := start; i < end; i++ {
		result = append(result, parquetParameter{
			Value:    values.Value(int(i)),
			MaskName: maskNames.Value(int(i)),
		})
	}
	return result
}

func int64Pointer(value int64) *int64 {
	return &value
}

func clearS3Env(t *testing.T) {
	t.Helper()
	for _, name := range []string{
		"S3_ENDPOINT",
		"S3_ENDPOINT_FILE",
		"AWS_ENDPOINT_URL",
		"AWS_ENDPOINT_URL_FILE",
		"S3_REGION",
		"S3_REGION_FILE",
		"AWS_REGION",
		"AWS_REGION_FILE",
		"AWS_DEFAULT_REGION",
		"AWS_DEFAULT_REGION_FILE",
		"S3_ACCESS_KEY_ID",
		"S3_ACCESS_KEY_ID_FILE",
		"AWS_ACCESS_KEY_ID",
		"AWS_ACCESS_KEY_ID_FILE",
		"S3_SECRET_ACCESS_KEY",
		"S3_SECRET_ACCESS_KEY_FILE",
		"AWS_SECRET_ACCESS_KEY",
		"AWS_SECRET_ACCESS_KEY_FILE",
		"S3_SESSION_TOKEN",
		"S3_SESSION_TOKEN_FILE",
		"AWS_SESSION_TOKEN",
		"AWS_SESSION_TOKEN_FILE",
		"S3_USE_SSL",
		"S3_USE_SSL_FILE",
		"S3_PATH_STYLE",
		"S3_PATH_STYLE_FILE",
	} {
		oldValue, hadValue := os.LookupEnv(name)
		if err := os.Unsetenv(name); err != nil {
			t.Fatalf("unset %s: %v", name, err)
		}
		t.Cleanup(func() {
			if hadValue {
				if err := os.Setenv(name, oldValue); err != nil {
					t.Fatalf("restore %s: %v", name, err)
				}
				return
			}
			if err := os.Unsetenv(name); err != nil {
				t.Fatalf("restore unset %s: %v", name, err)
			}
		})
	}
}

func writeThresholdModel(t *testing.T, modelPath string, simTh float64) {
	t.Helper()
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		SimTh:       &simTh,
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user alice",
				Tokens:   []string{"user", "alice"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
}

func writeTreeConfigModel(t *testing.T, modelPath string, depth, maxChildren int, parametrizeNumericTokens bool) {
	t.Helper()
	model := modelFile{
		Version:                  modelVersion,
		ParamString:              "<*>",
		LogClusterDepth:          intPointer(depth),
		MaxChildren:              intPointer(maxChildren),
		ParametrizeNumericTokens: boolPointer(parametrizeNumericTokens),
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user alice",
				Tokens:   []string{"user", "alice"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
}

func writeExtraDelimiterModel(t *testing.T, modelPath string, extraDelimiters []string) {
	t.Helper()
	model := modelFile{
		Version:         modelVersion,
		ParamString:     "<*>",
		ExtraDelimiters: copyStrings(extraDelimiters),
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "user <*>",
				Tokens:   []string{"user", "<*>"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
}

func writeMaskingRulesModel(t *testing.T, modelPath string, maskingRules []modelMaskingRule) {
	t.Helper()
	model := modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: append([]modelMaskingRule(nil), maskingRules...),
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "node <*> ready",
				Tokens:   []string{"node", "<*>", "ready"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
}

func writeFallbackModel(t *testing.T, dir string) string {
	t.Helper()
	modelPath := filepath.Join(dir, "model.json")
	model := modelFile{
		Version:     modelVersion,
		ParamString: "<*>",
		Templates: []templateModel{
			{
				ID:       1,
				Size:     1,
				Template: "alpha fixed ready",
				Tokens:   []string{"alpha", "fixed", "ready"},
			},
			{
				ID:       2,
				Size:     1,
				Template: "<*> target ready",
				Tokens:   []string{"<*>", "target", "ready"},
			},
		},
	}
	if err := writeModel(modelPath, model); err != nil {
		t.Fatalf("write model: %v", err)
	}
	return modelPath
}

func readModelID(t *testing.T, modelPath string) string {
	t.Helper()
	assert := a.New(t)
	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Match(model.ModelID, a.Not(a.EqualTo(""))))

	return model.ModelID
}

func assertModelSimTh(t *testing.T, modelPath string, want float64) {
	t.Helper()
	assert := a.New(t)
	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.NotNil(model.SimTh))

	assert.Requires(a.Number(*model.SimTh).EqualTo(want))
}

func assertModelTreeConfig(t *testing.T, modelPath string, wantDepth, wantMaxChildren int, wantParametrizeNumericTokens bool) {
	t.Helper()
	assert := a.New(t)
	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.NotNil(model.LogClusterDepth))

	assert.Requires(a.Number(*model.LogClusterDepth).EqualTo(wantDepth))

	assert.Requires(a.NotNil(model.MaxChildren))

	assert.Requires(a.Number(*model.MaxChildren).EqualTo(wantMaxChildren))

	assert.Requires(a.NotNil(model.ParametrizeNumericTokens))

	assert.Requires(a.True(*model.ParametrizeNumericTokens ==
		wantParametrizeNumericTokens))

}

func assertModelExtraDelimiters(t *testing.T, modelPath string, want []string) {
	t.Helper()
	assert := a.New(t)
	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Slice(model.ExtraDelimiters).EqualTo(want...))
}

func assertModelMaskingRules(t *testing.T, modelPath string, want []modelMaskingRule) {
	t.Helper()
	assert := a.New(t)
	model, _, err := readModel(modelPath)
	assert.Requires(a.NilError(err))
	assert.Requires(a.Slice(model.MaskingRules).EqualTo(want...))
}

func assertMetadataString(t *testing.T, metadata map[string]json.RawMessage, key, want string) {
	t.Helper()
	assert := a.New(t)

	assert.Requires(a.String(metadataStringValue(t, metadata, key)).EqualTo(want))
}

func metadataStringValue(t *testing.T, metadata map[string]json.RawMessage, key string) string {
	t.Helper()
	var value string
	decodeMetadataValue(t, metadata, key, &value)
	return value
}

func assertMetadataUTCTimestamp(t *testing.T, metadata map[string]json.RawMessage, key string) string {
	t.Helper()
	assert := a.New(t)
	value := metadataStringValue(t, metadata, key)
	parsed, err := time.Parse(time.RFC3339, value)
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(parsed.UTC().Format(time.RFC3339)).EqualTo(value))

	return value
}

func decodeMetadataValue(t *testing.T, metadata map[string]json.RawMessage, key string, value any) {
	t.Helper()
	assert := a.New(t)
	raw, ok := metadata[key]
	assert.Requires(a.True(ok))

	assert.Requires(a.NilError(json.Unmarshal(raw, value)))
}

func stringPointer(value string) *string {
	return &value
}

func TestMatchTemplateUsesScratchVariables(t *testing.T) {
	assert := a.New(t)
	lineTokens := []lineToken{
		{value: "user", rawString: "user"},
		{value: "<*>", rawString: "alice"},
		{value: "logged", rawString: "logged"},
		{value: "in", rawString: "in"},
	}
	scratch := make([]string, 0, 4)

	variables, ok := matchTemplate("<*>", []string{"user", "<*>", "logged", "in"}, lineTokens, scratch)
	assert.Requires(a.True(ok))
	assert.Requires(a.Slice(variables).EqualTo("alice"))
	assert.Requires(a.Number(cap(variables)).EqualTo(cap(scratch)))
	_, ok = matchTemplate("<*>", []string{"user", "<*>", "failed"}, lineTokens, variables[:0])
	assert.Requires(a.False(ok))
}

type ioDiscard struct{}

func (ioDiscard) Write(p []byte) (int, error) {
	return len(p), nil
}

type fakeParseSource struct {
	kind     string
	name     string
	lines    []string
	locators []map[string]string
	index    int
	acks     int
}

func (s *fakeParseSource) Info() parseio.SourceInfo {
	kind := s.kind
	if kind == "" {
		kind = "fake"
	}
	name := s.name
	if name == "" {
		name = "fake"
	}
	return parseio.SourceInfo{Kind: kind, Name: name, Finite: true}
}

func (s *fakeParseSource) Next(_ context.Context, record *parseio.SourceRecord) (bool, error) {
	if s.index >= len(s.lines) {
		return false, nil
	}
	line := s.lines[s.index]
	locator := map[string]string(nil)
	if s.index < len(s.locators) {
		locator = cloneStringMap(s.locators[s.index])
	}
	s.index++
	record.Line = line
	record.Bytes = int64(len(line))
	record.Locator = locator
	return true, nil
}

func (s *fakeParseSource) Ack(context.Context) error {
	s.acks++
	return nil
}

func (s *fakeParseSource) Close(context.Context) error {
	return nil
}

func cloneStringMap(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	cloned := make(map[string]string, len(values))
	for key, value := range values {
		cloned[key] = value
	}
	return cloned
}

type capturingParseSink struct {
	rows     []parseOutput
	writeErr error
}

func (s *capturingParseSink) Write(_ context.Context, row parseOutput) error {
	if s.writeErr != nil {
		return s.writeErr
	}
	s.rows = append(s.rows, row)
	return nil
}

func (s *capturingParseSink) Close(context.Context) error {
	return nil
}
