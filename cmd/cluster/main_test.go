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
)

func TestRunParseTracesWholeFileSpeedToStderr(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	wantStdout := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"alice\"]}\n" +
		"{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"bob\"]}\n" +
		"{\"template_id\":null,\"model_id\":\"" + modelID + "\",\"variables\":[]}\n"
	if stdout.String() != wantStdout {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", wantStdout, stdout.String())
	}

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
		if !strings.Contains(trace, want) {
			t.Fatalf("trace %q does not contain %q", trace, want)
		}
	}
	if strings.Contains(stdout.String(), "parse_trace") {
		t.Fatalf("parse trace should not be written to stdout: %q", stdout.String())
	}
}

func TestRunParseSourceFileMatchesFilenameBehavior(t *testing.T) {
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
	if err := run([]string{"parse", "-source", "file", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"alice\"]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunParseRejectsUnsupportedSources(t *testing.T) {
	for _, source := range []string{"kafka", "systemd", "syslog"} {
		t.Run(source, func(t *testing.T) {
			var stdout bytes.Buffer
			err := run([]string{"parse", "-source", source}, &stdout, ioDiscard{})
			if err == nil {
				t.Fatal("expected unsupported source to fail")
			}
			want := "source " + strconv.Quote(source) + " is not supported yet"
			if err.Error() != want {
				t.Fatalf("error mismatch:\nwant %q\ngot  %q", want, err.Error())
			}
		})
	}
}

func TestParseProcessorParseHandlesMatchedUnmatchedAndNamedParameters(t *testing.T) {
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
	if err != nil {
		t.Fatalf("compile masking rules: %v", err)
	}
	processor, err := newParseProcessor(model, compiledRules)
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	var output parseOutput
	if err := processor.Parse("service id=123 status retry", &output); err != nil {
		t.Fatalf("parse matched line: %v", err)
	}
	if output.TemplateID == nil || *output.TemplateID != 1 {
		t.Fatalf("template ID mismatch: %#v", output.TemplateID)
	}
	wantParameters := []drain.ExtractedParameter{
		{Value: "123", MaskName: "NUM"},
		{Value: "retry", MaskName: "*"},
	}
	if !reflect.DeepEqual(output.Parameters, wantParameters) {
		t.Fatalf("parameters mismatch:\nwant %#v\ngot  %#v", wantParameters, output.Parameters)
	}
	if wantVariables := []string{"123", "retry"}; !reflect.DeepEqual(output.Variables, wantVariables) {
		t.Fatalf("variables mismatch:\nwant %#v\ngot  %#v", wantVariables, output.Variables)
	}

	if err := processor.Parse("other line", &output); err != nil {
		t.Fatalf("parse unmatched line: %v", err)
	}
	if output.TemplateID != nil {
		t.Fatalf("unmatched line should not have template ID, got %#v", output.TemplateID)
	}
	if output.Parameters != nil {
		t.Fatalf("unmatched line should not retain parameters, got %#v", output.Parameters)
	}
	if output.Variables == nil || len(output.Variables) != 0 {
		t.Fatalf("unmatched line should have empty non-nil variables, got %#v", output.Variables)
	}
}

func TestParseSourceRecordsAcksOnlyAfterSuccessfulSinkWrite(t *testing.T) {
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
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}

	ctx := context.Background()
	source := &fakeParseSource{lines: []string{"user alice"}}
	sink := &capturingParseSink{}
	var record parseio.SourceRecord
	var output parseOutput
	if err := parseSourceRecords(ctx, source, processor, sink, &record, &output, func(parseio.SourceRecord) {}); err != nil {
		t.Fatalf("parse source records: %v", err)
	}
	if source.acks != 1 {
		t.Fatalf("expected one ack after successful write, got %d", source.acks)
	}

	writeErr := errors.New("write failed")
	source = &fakeParseSource{lines: []string{"user bob"}}
	sink = &capturingParseSink{writeErr: writeErr}
	if err := parseSourceRecords(ctx, source, processor, sink, &record, &output, func(parseio.SourceRecord) {}); !errors.Is(err, writeErr) {
		t.Fatalf("expected write error, got %v", err)
	}
	if source.acks != 0 {
		t.Fatalf("failed write should not ack source record, got %d", source.acks)
	}
}

func TestParseSourceRecordsWrapsProcessorErrorsWithFileContext(t *testing.T) {
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
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
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
	if err == nil {
		t.Fatal("expected processor error")
	}
	for _, want := range []string{
		"parse file target.log line=7 byte=128:",
		"matched cluster 79 did not match during variable extraction",
		`template="user fixed"`,
		`line="user alice"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
	if source.acks != 0 {
		t.Fatalf("failed parse should not ack source record, got %d", source.acks)
	}
}

func TestParseSourceRecordsWrapsProcessorErrorsWithGenericLocator(t *testing.T) {
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
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
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
	if err == nil {
		t.Fatal("expected processor error")
	}
	for _, want := range []string{
		"parse kafka logs",
		"offset=991284",
		"partition=3",
		"topic=logs",
		`template="user fixed"`,
		`line="user alice"`,
	} {
		if !strings.Contains(err.Error(), want) {
			t.Fatalf("error %q does not contain %q", err.Error(), want)
		}
	}
}

func TestParseSourceRecordsTruncatesLongErrorLinePreview(t *testing.T) {
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
	if err != nil {
		t.Fatalf("new processor: %v", err)
	}
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
	if err == nil {
		t.Fatal("expected processor error")
	}
	if !strings.Contains(err.Error(), " (truncated)") {
		t.Fatalf("expected truncated marker in error: %q", err.Error())
	}
	if strings.Contains(err.Error(), strings.Repeat("a", parseErrorLineMaxBytes+1)) {
		t.Fatalf("error line preview was not capped: %q", err.Error())
	}
}

func TestRunParseWritesJSONLToLocalPrefix(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath, "-output", outputPrefix}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout should be empty when -output is set, got %q", stdout.String())
	}

	parts := localOutputParts(t, outputPrefix, "jsonl")
	if len(parts) != 1 {
		t.Fatalf("expected one JSONL part, got %#v", parts)
	}
	assertBaseName(t, parts[0], "part-00000.jsonl")
	want := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"alice\"]}\n" +
		"{\"template_id\":null,\"model_id\":\"" + modelID + "\",\"variables\":[]}\n"
	assertFileContent(t, parts[0], want)
}

func TestRunParseRotatesJSONLByBatchSize(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath, "-output", outputPrefix, "-batch-size", "2"}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	parts := localOutputParts(t, outputPrefix, "jsonl")
	if len(parts) != 2 {
		t.Fatalf("expected two JSONL parts, got %#v", parts)
	}
	assertBaseName(t, parts[0], "part-00000.jsonl")
	assertBaseName(t, parts[1], "part-00001.jsonl")
	assertSameRunDir(t, parts[0], parts[1])
	assertFileContent(t, parts[0], "{\"template_id\":1,\"model_id\":\""+modelID+"\",\"variables\":[\"alice\"]}\n"+
		"{\"template_id\":1,\"model_id\":\""+modelID+"\",\"variables\":[\"bob\"]}\n")
	assertFileContent(t, parts[1], "{\"template_id\":1,\"model_id\":\""+modelID+"\",\"variables\":[\"carol\"]}\n")
}

func TestPartJSONLWriterRotatesByBatchMaxAge(t *testing.T) {
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
	if err != nil {
		t.Fatalf("new writer: %v", err)
	}
	if err := writer.Write(ctx, parseOutput{ModelID: "model", Variables: []string{"first"}}); err != nil {
		t.Fatalf("write first row: %v", err)
	}
	now = now.Add(5 * time.Second)
	if err := writer.Write(ctx, parseOutput{ModelID: "model", Variables: []string{"second"}}); err != nil {
		t.Fatalf("write second row: %v", err)
	}
	if err := writer.Close(ctx); err != nil {
		t.Fatalf("close writer: %v", err)
	}

	parts := localOutputParts(t, outputPrefix, "jsonl")
	if len(parts) != 2 {
		t.Fatalf("expected two JSONL parts, got %#v", parts)
	}
	assertBaseName(t, parts[0], "part-00000.jsonl")
	assertBaseName(t, parts[1], "part-00001.jsonl")
	assertFileContent(t, parts[0], "{\"template_id\":null,\"model_id\":\"model\",\"variables\":[\"first\"]}\n")
	assertFileContent(t, parts[1], "{\"template_id\":null,\"model_id\":\"model\",\"variables\":[\"second\"]}\n")
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
			var stdout bytes.Buffer
			err := run(test.args, &stdout, ioDiscard{})
			if err == nil {
				t.Fatal("expected invalid output option to fail")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unexpected error:\nwant substring %q\ngot  %v", test.want, err)
			}
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
			var stdout bytes.Buffer
			var stderr bytes.Buffer
			err := run(test.args, &stdout, &stderr)
			if err == nil {
				t.Fatal("expected invalid S3 output option to fail")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unexpected error:\nwant substring %q\ngot  %v", test.want, err)
			}
		})
	}
}

func TestResolveS3ConfigUsesFlagEnvCascade(t *testing.T) {
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
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	if got, want := config.Endpoint, "flag:9443"; got != want {
		t.Fatalf("endpoint mismatch: want %q got %q", want, got)
	}
	if got, want := config.Region, "flag-region"; got != want {
		t.Fatalf("region mismatch: want %q got %q", want, got)
	}
	if got, want := config.AccessKeyID, "flag-access"; got != want {
		t.Fatalf("access key mismatch: want %q got %q", want, got)
	}
	if got, want := config.SecretAccessKey, "flag-secret"; got != want {
		t.Fatalf("secret key mismatch: want %q got %q", want, got)
	}
	if !config.UseSSL {
		t.Fatal("flag should override env and enable SSL")
	}
	if config.PathStyle {
		t.Fatal("flag should override env and disable path-style lookup")
	}
}

func TestResolveS3ConfigReadsKubernetesSecretFiles(t *testing.T) {
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
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	if got, want := config.Endpoint, "secrets:9000"; got != want {
		t.Fatalf("endpoint mismatch: want %q got %q", want, got)
	}
	if got, want := config.Region, "secret-region"; got != want {
		t.Fatalf("region mismatch: want %q got %q", want, got)
	}
	if got, want := config.AccessKeyID, "secret-access"; got != want {
		t.Fatalf("access key mismatch: want %q got %q", want, got)
	}
	if got, want := config.SecretAccessKey, "secret-key"; got != want {
		t.Fatalf("secret key mismatch: want %q got %q", want, got)
	}
	if got, want := config.SessionToken, "secret-session"; got != want {
		t.Fatalf("session token mismatch: want %q got %q", want, got)
	}
	if config.UseSSL {
		t.Fatal("secret file should disable SSL")
	}
	if !config.PathStyle {
		t.Fatal("secret file should enable path-style lookup")
	}
}

func TestResolveS3ConfigDirectValuesOverrideSecretFiles(t *testing.T) {
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
	if err != nil {
		t.Fatalf("resolve config: %v", err)
	}
	if got, want := config.Endpoint, "flag:9443"; got != want {
		t.Fatalf("endpoint mismatch: want %q got %q", want, got)
	}
	if got, want := config.AccessKeyID, "flag-access"; got != want {
		t.Fatalf("access key mismatch: want %q got %q", want, got)
	}
	if got, want := config.SecretAccessKey, "flag-secret"; got != want {
		t.Fatalf("secret key mismatch: want %q got %q", want, got)
	}
	if !config.UseSSL {
		t.Fatal("https endpoint should default to SSL")
	}
}

func TestRunParseWritesJSONLToS3Prefix(t *testing.T) {
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
	if err != nil {
		t.Fatalf("run parse: %v", err)
	}
	if captured.bucket != "bucket" {
		t.Fatalf("bucket mismatch: %q", captured.bucket)
	}
	if !strings.HasPrefix(captured.key, "prefix/format=jsonl/run_id=") || !strings.HasSuffix(captured.key, "/part-00000.jsonl") {
		t.Fatalf("unexpected object key: %q", captured.key)
	}
	if got, want := captured.contentType, parseJSONLContentType; got != want {
		t.Fatalf("content type mismatch: want %q got %q", want, got)
	}
	if got, want := captured.config.Endpoint, "localhost:9000"; got != want {
		t.Fatalf("endpoint mismatch: want %q got %q", want, got)
	}
	if captured.config.UseSSL {
		t.Fatal("http endpoint should default to non-SSL")
	}
	wantBody := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"alice\"]}\n"
	if captured.body != wantBody {
		t.Fatalf("body mismatch:\nwant %q\ngot  %q", wantBody, captured.body)
	}
	if captured.size != int64(len(wantBody)) {
		t.Fatalf("size mismatch: want %d got %d", len(wantBody), captured.size)
	}
}

func TestRunParseWritesParquetToLocalPrefix(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath, "-format", "parquet", "-output", outputPrefix}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}
	if stdout.Len() != 0 {
		t.Fatalf("stdout should be empty when -output is set, got %q", stdout.String())
	}

	parts := localOutputParts(t, outputPrefix, "parquet")
	if len(parts) != 1 {
		t.Fatalf("expected one Parquet part, got %#v", parts)
	}
	assertBaseName(t, parts[0], "part-00000.parquet")
	rows := readParquetParseRows(t, parts[0])
	want := []parquetParseRow{
		{
			TemplateID: int64Pointer(1),
			ModelID:    modelID,
			Variables:  []string{"123", "42", "retry"},
			Parameters: []parquetParameter{
				{Value: "123", MaskName: "NUM"},
				{Value: "42", MaskName: "NUM"},
				{Value: "retry", MaskName: "*"},
			},
		},
		{
			TemplateID: nil,
			ModelID:    modelID,
			Variables:  []string{},
			Parameters: []parquetParameter{},
		},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("parquet rows mismatch:\nwant %#v\ngot  %#v", want, rows)
	}
}

func TestRunParseWritesParquetToS3Prefix(t *testing.T) {
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
	if err != nil {
		t.Fatalf("run parse: %v", err)
	}
	if captured.bucket != "bucket" {
		t.Fatalf("bucket mismatch: %q", captured.bucket)
	}
	if !strings.HasPrefix(captured.key, "prefix/format=parquet/run_id=") || !strings.HasSuffix(captured.key, "/part-00000.parquet") {
		t.Fatalf("unexpected object key: %q", captured.key)
	}
	if got, want := captured.contentType, parseParquetContentType; got != want {
		t.Fatalf("content type mismatch: want %q got %q", want, got)
	}
	if captured.size != int64(len(captured.body)) {
		t.Fatalf("size mismatch: want %d got %d", len(captured.body), captured.size)
	}

	parquetPath := filepath.Join(dir, "captured.parquet")
	if err := os.WriteFile(parquetPath, captured.body, 0o644); err != nil {
		t.Fatalf("write captured parquet: %v", err)
	}
	rows := readParquetParseRows(t, parquetPath)
	want := []parquetParseRow{
		{
			TemplateID: int64Pointer(1),
			ModelID:    modelID,
			Variables:  []string{"alice"},
			Parameters: []parquetParameter{},
		},
	}
	if !reflect.DeepEqual(rows, want) {
		t.Fatalf("parquet rows mismatch:\nwant %#v\ngot  %#v", want, rows)
	}
}

func TestRunTestReportsTemplateDistribution(t *testing.T) {
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
	if err := run([]string{"test", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run test: %v", err)
	}

	want := "{\n" +
		"  \"total\": 3,\n" +
		"  \"matched\": 2,\n" +
		"  \"unmatched\": 1,\n" +
		"  \"templates\": [\n" +
		"    {\n" +
		"      \"template_id\": 1,\n" +
		"      \"model_id\": \"" + modelID + "\",\n" +
		"      \"template\": \"user <*> logged in\",\n" +
		"      \"count\": 2\n" +
		"    }\n" +
		"  ]\n" +
		"}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunTestUsesFallbackFullSearch(t *testing.T) {
	dir := t.TempDir()
	modelPath := writeFallbackModel(t, dir)
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "alpha target ready\n")

	var stdout bytes.Buffer
	if err := run([]string{"test", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run test: %v", err)
	}

	want := "{\n" +
		"  \"total\": 1,\n" +
		"  \"matched\": 1,\n" +
		"  \"unmatched\": 0,\n" +
		"  \"templates\": [\n" +
		"    {\n" +
		"      \"template_id\": 1,\n" +
		"      \"model_id\": \"" + modelID + "\",\n" +
		"      \"template\": \"alpha fixed ready\",\n" +
		"      \"count\": 0\n" +
		"    },\n" +
		"    {\n" +
		"      \"template_id\": 2,\n" +
		"      \"model_id\": \"" + modelID + "\",\n" +
		"      \"template\": \"<*> target ready\",\n" +
		"      \"count\": 1\n" +
		"    }\n" +
		"  ]\n" +
		"}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunTestRestoresExtraDelimiters(t *testing.T) {
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
	if err := run([]string{"test", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run test: %v", err)
	}

	if !strings.Contains(stdout.String(), "\"matched\": 1") || !strings.Contains(stdout.String(), "\"count\": 1") {
		t.Fatalf("expected delimiter-normalized line to match, got:\n%s", stdout.String())
	}
}

func TestRunTrainWritesSimilarityThreshold(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "user alice logged in\nuser bob logged in\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-filename", logPath, "-model", modelPath, "-sim-th", "0.73"}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train: %v", err)
	}

	assertModelSimTh(t, modelPath, 0.73)
}

func TestRunTrainWritesTreeConfigFlags(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "host web-001 ready\n")

	var stdout bytes.Buffer
	if err := run([]string{
		"train",
		"-filename", logPath,
		"-model", modelPath,
		"-depth", "7",
		"-max-children", "13",
		"-parametrize-numeric-tokens=false",
	}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train: %v", err)
	}

	assertModelTreeConfig(t, modelPath, 7, 13, false)
}

func TestRunTrainWritesExtraDelimiters(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "user:logged_in:alice\nuser:logged_in:bob\n")

	var stdout bytes.Buffer
	if err := run([]string{
		"train",
		"-filename", logPath,
		"-model", modelPath,
		"-extra-delimiter", "_",
		"-extra-delimiter", ":",
	}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train: %v", err)
	}

	assertModelExtraDelimiters(t, modelPath, []string{"_", ":"})
	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if got, want := model.Templates[0].Template, "user logged in <*>"; got != want {
		t.Fatalf("template mismatch: want %q got %q", want, got)
	}
}

func TestRunTrainWritesDefaultMaskingRules(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "user alice logged in\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train: %v", err)
	}

	assertModelMaskingRules(t, modelPath, modelMaskingRules(defaultMaskingRules()))
}

func TestRunTrainDefaultMaskingRulesAffectTemplates(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := writeTestLog(t, dir, "device ab:cd:ef:01 addr 10.1.2.3 seq abcdef 123456 fedcba hex 0xdeadbeef num -42\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train: %v", err)
	}

	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if len(model.Templates) != 1 {
		t.Fatalf("expected one template, got %#v", model.Templates)
	}
	want := "device <:ID:> addr <:IP:> seq <:SEQ:> hex <:HEX:> num <:NUM:>"
	if got := model.Templates[0].Template; got != want {
		t.Fatalf("template mismatch: want %q got %q", want, got)
	}
}

func TestRunTrainMaskingRulesFileReplacesDefaultsAndSupportsRegexPattern(t *testing.T) {
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
	if err := run([]string{"train", "-filename", logPath, "-model", modelPath, "-masking-rules", rulesPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train: %v", err)
	}

	wantRules := []modelMaskingRule{
		{Pattern: `^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})`, MaskWith: "TIMESTAMP"},
		{Pattern: `\bGPU-\d+\b`, MaskWith: "GPU"},
	}
	assertModelMaskingRules(t, modelPath, wantRules)

	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	for _, rule := range model.MaskingRules {
		if rule.Pattern == timestampPrefixPattern {
			t.Fatal("custom masking file should replace the built-in timestamp rule")
		}
	}
	if got, want := model.Templates[0].Template, "<:TIMESTAMP:> device <:GPU:> addr 10.1.2.3"; got != want {
		t.Fatalf("template mismatch: want %q got %q", want, got)
	}
}

func TestRunTrainWritesMetadataFileAndCreatedAt(t *testing.T) {
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
	if err := run([]string{"train", "-filename", logPath, "-model", modelPath, "-metadata", metadataPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train: %v", err)
	}

	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	createdAt := assertMetadataUTCTimestamp(t, model.Metadata, "created_at")
	if createdAt == "1999-01-01T00:00:00Z" {
		t.Fatal("metadata file should not override generated created_at")
	}
	if _, ok := model.Metadata["updated_at"]; ok {
		t.Fatal("fresh train should not write updated_at")
	}
	assertMetadataString(t, model.Metadata, "source", "lsb_release")
	var system map[string]string
	decodeMetadataValue(t, model.Metadata, "system", &system)
	if got, want := system["os"], "Ubuntu 24.04.2 LTS"; got != want {
		t.Fatalf("metadata system os mismatch: want %q got %q", want, got)
	}
	if got, want := system["arch"], "aarch64"; got != want {
		t.Fatalf("metadata system arch mismatch: want %q got %q", want, got)
	}
}

func TestRunTrainUpdateMergesMetadataAndWritesUpdatedAt(t *testing.T) {
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
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath, "-metadata", metadataPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	updatedModel, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read updated model: %v", err)
	}
	assertMetadataString(t, updatedModel.Metadata, "created_at", createdAt)
	updatedAt := assertMetadataUTCTimestamp(t, updatedModel.Metadata, "updated_at")
	if updatedAt == "1999-01-01T00:00:00Z" {
		t.Fatal("metadata file should not override generated updated_at")
	}
	assertMetadataString(t, updatedModel.Metadata, "owner", "kernel-team")
	assertMetadataString(t, updatedModel.Metadata, "run_id", "second")
	var system map[string]string
	decodeMetadataValue(t, updatedModel.Metadata, "system", &system)
	if got, want := system["arch"], "aarch64"; got != want {
		t.Fatalf("metadata system arch mismatch: want %q got %q", want, got)
	}
	if _, ok := system["os"]; ok {
		t.Fatalf("metadata merge should be shallow; system os should have been replaced, got %#v", system)
	}
}

func TestRunTrainUpdateReplacesInvalidCreatedAtMetadata(t *testing.T) {
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
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	updatedModel, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read updated model: %v", err)
	}
	createdAt := assertMetadataUTCTimestamp(t, updatedModel.Metadata, "created_at")
	if createdAt == "not a timestamp" {
		t.Fatal("invalid existing created_at should be replaced")
	}
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
			if err == nil {
				t.Fatal("expected invalid metadata to fail")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unexpected error:\nwant substring %q\ngot  %v", test.want, err)
			}
		})
	}
}

func TestRunTrainUpdatePreservesSavedSimilarityThreshold(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeThresholdModel(t, modelPath, 0.82)
	logPath := writeTestLog(t, dir, "user alice\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelSimTh(t, modelPath, 0.82)
}

func TestRunTrainUpdateOverridesSavedSimilarityThreshold(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeThresholdModel(t, modelPath, 0.82)
	logPath := writeTestLog(t, dir, "user alice\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath, "-sim-th", "0.55"}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelSimTh(t, modelPath, 0.55)
}

func TestRunTrainUpdatePreservesSavedTreeConfig(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeTreeConfigModel(t, modelPath, 7, 13, false)
	logPath := writeTestLog(t, dir, "user bob\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelTreeConfig(t, modelPath, 7, 13, false)
}

func TestRunTrainUpdateOverridesSavedTreeConfig(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeTreeConfigModel(t, modelPath, 7, 13, false)
	logPath := writeTestLog(t, dir, "user bob\n")

	var stdout bytes.Buffer
	if err := run([]string{
		"train",
		"-update",
		"-filename", logPath,
		"-model", modelPath,
		"-depth", "5",
		"-max-children", "9",
		"-parametrize-numeric-tokens=true",
	}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelTreeConfig(t, modelPath, 5, 9, true)
}

func TestRunTrainUpdatePreservesSavedExtraDelimiters(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeExtraDelimiterModel(t, modelPath, []string{"_"})
	logPath := writeTestLog(t, dir, "user_bob\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelExtraDelimiters(t, modelPath, []string{"_"})
}

func TestRunTrainUpdateOverridesSavedExtraDelimiters(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	writeExtraDelimiterModel(t, modelPath, []string{"_"})
	logPath := writeTestLog(t, dir, "service:bob\n")

	var stdout bytes.Buffer
	if err := run([]string{
		"train",
		"-update",
		"-filename", logPath,
		"-model", modelPath,
		"-extra-delimiter", ":",
	}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelExtraDelimiters(t, modelPath, []string{":"})
}

func TestRunTrainUpdatePreservesSavedMaskingRules(t *testing.T) {
	dir := t.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	savedRules := []modelMaskingRule{{Pattern: `\bnode-\d+\b`, MaskWith: "NODE"}}
	writeMaskingRulesModel(t, modelPath, savedRules)
	logPath := writeTestLog(t, dir, "node-2 ready\n")

	var stdout bytes.Buffer
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelMaskingRules(t, modelPath, savedRules)
}

func TestRunTrainUpdateOverridesSavedMaskingRules(t *testing.T) {
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
	if err := run([]string{"train", "-update", "-filename", logPath, "-model", modelPath, "-masking-rules", rulesPath}, &stdout, ioDiscard{}); err != nil {
		t.Fatalf("run train update: %v", err)
	}

	assertModelMaskingRules(t, modelPath, []modelMaskingRule{{Pattern: `\bGPU-\d+\b`, MaskWith: "GPU"}})
}

func TestRunTrainRejectsInvalidSimilarityThreshold(t *testing.T) {
	var stdout bytes.Buffer
	err := run([]string{"train", "-filename", "missing.log", "-model", "model.json", "-sim-th", "1.1"}, &stdout, ioDiscard{})
	if err == nil {
		t.Fatal("expected invalid sim-th to fail")
	}
	if !strings.Contains(err.Error(), "sim-th must be between 0 and 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunTrainRejectsInvalidMaxChildren(t *testing.T) {
	var stdout bytes.Buffer
	err := run([]string{"train", "-filename", "missing.log", "-model", "model.json", "-max-children", "0"}, &stdout, ioDiscard{})
	if err == nil {
		t.Fatal("expected invalid max-children to fail")
	}
	if !strings.Contains(err.Error(), "max-children must be at least 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRunTrainRejectsEmptyExtraDelimiter(t *testing.T) {
	var stdout bytes.Buffer
	err := run([]string{"train", "-filename", "missing.log", "-model", "model.json", "-extra-delimiter", ""}, &stdout, ioDiscard{})
	if err == nil {
		t.Fatal("expected empty extra delimiter to fail")
	}
	if !strings.Contains(err.Error(), "extra delimiter must not be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
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
			if err == nil {
				t.Fatal("expected invalid masking rules to fail")
			}
			if !strings.Contains(err.Error(), test.want) {
				t.Fatalf("unexpected error:\nwant substring %q\ngot  %v", test.want, err)
			}
		})
	}
}

func TestReadOldModelWithoutSimilarityThresholdUsesDefault(t *testing.T) {
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
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if model.SimTh != nil {
		t.Fatalf("old model should not set sim_th, got %v", *model.SimTh)
	}
	if got, want := configFromModel(model).SimTh, clusterConfig().SimTh; got != want {
		t.Fatalf("default sim_th mismatch: want %v got %v", want, got)
	}
	config := configFromModel(model)
	if got, want := config.LogClusterDepth, clusterConfig().LogClusterDepth; got != want {
		t.Fatalf("default depth mismatch: want %v got %v", want, got)
	}
	if got, want := config.MaxChildren, clusterConfig().MaxChildren; got != want {
		t.Fatalf("default max children mismatch: want %v got %v", want, got)
	}
	if config.PreserveNumericTokens {
		t.Fatal("old model should default to parameterizing numeric tokens")
	}
	if len(config.ExtraDelimiters) != 0 {
		t.Fatalf("old model should default to no extra delimiters, got %#v", config.ExtraDelimiters)
	}
}

func TestReadModelRejectsInvalidSimilarityThreshold(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected invalid model sim_th to fail")
	}
	if !strings.Contains(err.Error(), "model sim_th must be between 0 and 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadModelRejectsInvalidMaxChildren(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected invalid model max_children to fail")
	}
	if !strings.Contains(err.Error(), "model max_children must be at least 1") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadModelRejectsEmptyExtraDelimiter(t *testing.T) {
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
	if err == nil {
		t.Fatal("expected invalid model extra delimiter to fail")
	}
	if !strings.Contains(err.Error(), "model extra_delimiters[1] must not be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReadModelComputesStableBase64URLModelID(t *testing.T) {
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
	if err != nil {
		t.Fatalf("read first model: %v", err)
	}
	second, _, err := readModel(secondPath)
	if err != nil {
		t.Fatalf("read second model: %v", err)
	}
	if first.ModelID != second.ModelID {
		t.Fatalf("model IDs should match for reordered templates:\nfirst  %q\nsecond %q", first.ModelID, second.ModelID)
	}
	decoded, err := base64.RawURLEncoding.DecodeString(first.ModelID)
	if err != nil {
		t.Fatalf("model ID is not raw base64url: %q: %v", first.ModelID, err)
	}
	if len(decoded) != 32 {
		t.Fatalf("model ID should decode to 32 SHA-256 bytes, got %d", len(decoded))
	}
	if strings.Contains(first.ModelID, "=") {
		t.Fatalf("model ID should be unpadded, got %q", first.ModelID)
	}
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
			if got := modelIDFromTemplates(tt.templates); got == baseID {
				t.Fatalf("model ID did not change after changing %s", tt.name)
			}
		})
	}
}

func TestWriteModelDoesNotPersistModelID(t *testing.T) {
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
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if strings.Contains(string(contents), "model_id") || strings.Contains(string(contents), "cached-model-id") {
		t.Fatalf("writeModel persisted model ID:\n%s", contents)
	}
}

func TestRunParseExtractsMaskedRawValuesWithSpaces(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"[Mon May 11 13:41:21 2026]\",\"alice\"]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunParseExtractsExtraDelimiterVariables(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"alice\"]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunParsePreservesMaskedValuesWithExtraDelimiters(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"[Mon May 11 13:41:21 2026]\",\"alice\"]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunParseUsesFallbackFullSearch(t *testing.T) {
	dir := t.TempDir()
	modelPath := writeFallbackModel(t, dir)
	modelID := readModelID(t, modelPath)
	logPath := writeTestLog(t, dir, "alpha target ready\n")

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":2,\"model_id\":\"" + modelID + "\",\"variables\":[\"alpha\"]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunParseOutputsNamedParametersForEmbeddedMasks(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[\"123\",\"42\",\"retry\"],\"parameters\":[{\"value\":\"123\",\"mask_name\":\"NUM\"},{\"value\":\"42\",\"mask_name\":\"NUM\"},{\"value\":\"retry\",\"mask_name\":\"*\"}]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunParseKeepsLegacyPlainMaskWithLiteralModels(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":1,\"model_id\":\"" + modelID + "\",\"variables\":[]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestRunParseFallbackMatchesEmbeddedLegacyLiteralMasks(t *testing.T) {
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
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":79,\"model_id\":\"" + modelID + "\",\"variables\":[]}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
	}
}

func TestTokenizeLineFastPathMatchesLegacy(t *testing.T) {
	compiledRules, err := compileMaskingRules([]modelMaskingRule{
		{Pattern: `\[[^\]]+\]`},
	}, "<*>")
	if err != nil {
		t.Fatalf("compile masking rules: %v", err)
	}

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
			got := tokenizeLine(line, compiledRules, nil)
			want := tokenizeLineLegacy(line, compiledRules, nil)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("tokens mismatch:\nwant %#v\ngot  %#v", want, got)
			}
		})
	}

	if _, ok := tokenizeLineSingleMask("prefix[Mon May 11 13:41:21 2026]suffix", compiledRules[0]); ok {
		t.Fatal("embedded mask should use legacy fallback")
	}
}

func TestTokenizeLineRestoresEmbeddedMasks(t *testing.T) {
	compiledRules, err := compileMaskingRules([]modelMaskingRule{
		{Pattern: `\b\d{1,3}(?:\.\d{1,3}){3}\b`, Replacement: "IP"},
	}, "<*>")
	if err != nil {
		t.Fatalf("compile masking rules: %v", err)
	}

	got := tokenizeLine("target=10.0.0.1 status ok", compiledRules, nil)
	want := []lineToken{
		{value: "target=IP", rawString: "target=10.0.0.1"},
		{value: "status", rawString: "status"},
		{value: "ok", rawString: "ok"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens mismatch:\nwant %#v\ngot  %#v", want, got)
	}
}

func TestTokenizeLineUsesExtraDelimitersOutsideMasks(t *testing.T) {
	compiledRules, err := compileMaskingRules([]modelMaskingRule{
		{Pattern: timestampPrefixPattern},
	}, "<*>")
	if err != nil {
		t.Fatalf("compile masking rules: %v", err)
	}

	got := tokenizeLine("[Mon May 11 13:41:21 2026]:user:alice", compiledRules, []string{":"})
	want := []lineToken{
		{value: "<*>", rawString: "[Mon May 11 13:41:21 2026]"},
		{value: "user", rawString: "user"},
		{value: "alice", rawString: "alice"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("tokens mismatch:\nwant %#v\ngot  %#v", want, got)
	}
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
	matches, err := filepath.Glob(filepath.Join(prefix, "format="+format, "run_id=*", "part-*."+format))
	if err != nil {
		t.Fatalf("glob output parts: %v", err)
	}
	return matches
}

func assertBaseName(t *testing.T, path, want string) {
	t.Helper()
	if got := filepath.Base(path); got != want {
		t.Fatalf("base name mismatch: want %q got %q", want, got)
	}
}

func assertSameRunDir(t *testing.T, first, second string) {
	t.Helper()
	if filepath.Dir(first) != filepath.Dir(second) {
		t.Fatalf("parts should share one run directory:\nfirst  %s\nsecond %s", first, second)
	}
}

func assertFileContent(t *testing.T, path, want string) {
	t.Helper()
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if got := string(contents); got != want {
		t.Fatalf("content mismatch for %s:\nwant %q\ngot  %q", path, want, got)
	}
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
	t.Helper()
	reader, err := os.Open(parquetPath)
	if err != nil {
		t.Fatalf("open parquet: %v", err)
	}
	defer reader.Close()

	table, err := pqarrow.ReadTable(context.Background(), reader, nil, pqarrow.ArrowReadProperties{}, memory.NewGoAllocator())
	if err != nil {
		t.Fatalf("read parquet table: %v", err)
	}
	defer table.Release()

	if got, want := table.NumCols(), int64(4); got != want {
		t.Fatalf("parquet column count mismatch: want %d got %d", want, got)
	}
	if got, want := table.Schema().Field(0).Name, "template_id"; got != want {
		t.Fatalf("field 0 mismatch: want %q got %q", want, got)
	}
	templateIDs := singleChunk[*array.Int64](t, table, 0)
	modelIDs := singleChunk[*array.String](t, table, 1)
	variables := singleChunk[*array.List](t, table, 2)
	variableValues := variables.ListValues().(*array.String)
	parameters := singleChunk[*array.List](t, table, 3)
	parameterStructs := parameters.ListValues().(*array.Struct)
	parameterValues := parameterStructs.Field(0).(*array.String)
	parameterMaskNames := parameterStructs.Field(1).(*array.String)

	rows := make([]parquetParseRow, 0, table.NumRows())
	for i := 0; i < int(table.NumRows()); i++ {
		var templateID *int64
		if !templateIDs.IsNull(i) {
			templateID = int64Pointer(templateIDs.Value(i))
		}
		rows = append(rows, parquetParseRow{
			TemplateID: templateID,
			ModelID:    modelIDs.Value(i),
			Variables:  stringListValues(variables, variableValues, i),
			Parameters: parameterListValues(parameters, parameterValues, parameterMaskNames, i),
		})
	}
	return rows
}

func singleChunk[T arrow.Array](t *testing.T, table arrow.Table, column int) T {
	t.Helper()
	chunks := table.Column(column).Data().Chunks()
	if len(chunks) != 1 {
		t.Fatalf("expected column %d to have one chunk, got %d", column, len(chunks))
	}
	chunk, ok := chunks[0].(T)
	if !ok {
		t.Fatalf("unexpected chunk type for column %d: %T", column, chunks[0])
	}
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
	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if model.ModelID == "" {
		t.Fatal("model ID is empty")
	}
	return model.ModelID
}

func assertModelSimTh(t *testing.T, modelPath string, want float64) {
	t.Helper()
	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if model.SimTh == nil {
		t.Fatalf("model missing sim_th, want %v", want)
	}
	if got := *model.SimTh; got != want {
		t.Fatalf("sim_th mismatch: want %v got %v", want, got)
	}
}

func assertModelTreeConfig(t *testing.T, modelPath string, wantDepth, wantMaxChildren int, wantParametrizeNumericTokens bool) {
	t.Helper()
	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if model.LogClusterDepth == nil {
		t.Fatalf("model missing log_cluster_depth, want %d", wantDepth)
	}
	if got := *model.LogClusterDepth; got != wantDepth {
		t.Fatalf("log_cluster_depth mismatch: want %d got %d", wantDepth, got)
	}
	if model.MaxChildren == nil {
		t.Fatalf("model missing max_children, want %d", wantMaxChildren)
	}
	if got := *model.MaxChildren; got != wantMaxChildren {
		t.Fatalf("max_children mismatch: want %d got %d", wantMaxChildren, got)
	}
	if model.ParametrizeNumericTokens == nil {
		t.Fatalf("model missing parametrize_numeric_tokens, want %v", wantParametrizeNumericTokens)
	}
	if got := *model.ParametrizeNumericTokens; got != wantParametrizeNumericTokens {
		t.Fatalf("parametrize_numeric_tokens mismatch: want %v got %v", wantParametrizeNumericTokens, got)
	}
}

func assertModelExtraDelimiters(t *testing.T, modelPath string, want []string) {
	t.Helper()
	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if !reflect.DeepEqual(model.ExtraDelimiters, want) {
		t.Fatalf("extra_delimiters mismatch: want %#v got %#v", want, model.ExtraDelimiters)
	}
}

func assertModelMaskingRules(t *testing.T, modelPath string, want []modelMaskingRule) {
	t.Helper()
	model, _, err := readModel(modelPath)
	if err != nil {
		t.Fatalf("read model: %v", err)
	}
	if !reflect.DeepEqual(model.MaskingRules, want) {
		t.Fatalf("masking_rules mismatch:\nwant %#v\ngot  %#v", want, model.MaskingRules)
	}
}

func assertMetadataString(t *testing.T, metadata map[string]json.RawMessage, key, want string) {
	t.Helper()
	if got := metadataStringValue(t, metadata, key); got != want {
		t.Fatalf("metadata %s mismatch: want %q got %q", key, want, got)
	}
}

func metadataStringValue(t *testing.T, metadata map[string]json.RawMessage, key string) string {
	t.Helper()
	var value string
	decodeMetadataValue(t, metadata, key, &value)
	return value
}

func assertMetadataUTCTimestamp(t *testing.T, metadata map[string]json.RawMessage, key string) string {
	t.Helper()
	value := metadataStringValue(t, metadata, key)
	parsed, err := time.Parse(time.RFC3339, value)
	if err != nil {
		t.Fatalf("metadata %s is not RFC3339: %q: %v", key, value, err)
	}
	if got, want := parsed.UTC().Format(time.RFC3339), value; got != want {
		t.Fatalf("metadata %s should be a canonical UTC timestamp: want %q got %q", key, got, want)
	}
	return value
}

func decodeMetadataValue(t *testing.T, metadata map[string]json.RawMessage, key string, value any) {
	t.Helper()
	raw, ok := metadata[key]
	if !ok {
		t.Fatalf("metadata missing %s", key)
	}
	if err := json.Unmarshal(raw, value); err != nil {
		t.Fatalf("decode metadata %s: %v", key, err)
	}
}

func stringPointer(value string) *string {
	return &value
}

func TestMatchTemplateUsesScratchVariables(t *testing.T) {
	lineTokens := []lineToken{
		{value: "user", rawString: "user"},
		{value: "<*>", rawString: "alice"},
		{value: "logged", rawString: "logged"},
		{value: "in", rawString: "in"},
	}
	scratch := make([]string, 0, 4)

	variables, ok := matchTemplate("<*>", []string{"user", "<*>", "logged", "in"}, lineTokens, scratch)
	if !ok {
		t.Fatal("expected template to match")
	}
	if !reflect.DeepEqual(variables, []string{"alice"}) {
		t.Fatalf("variables mismatch: %#v", variables)
	}
	if cap(variables) != cap(scratch) {
		t.Fatalf("expected variables to reuse scratch capacity %d, got %d", cap(scratch), cap(variables))
	}

	if _, ok := matchTemplate("<*>", []string{"user", "<*>", "failed"}, lineTokens, variables[:0]); ok {
		t.Fatal("expected mismatched template to fail")
	}
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
