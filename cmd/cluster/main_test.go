package main

import (
	"bytes"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
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

	logPath := filepath.Join(dir, "target.log")
	logContent := "user alice logged in\nuser bob logged in\n"
	if err := os.WriteFile(logPath, []byte(logContent), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	wantStdout := "{\"template_id\":1,\"variables\":[\"alice\"]}\n" +
		"{\"template_id\":1,\"variables\":[\"bob\"]}\n"
	if stdout.String() != wantStdout {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", wantStdout, stdout.String())
	}

	trace := stderr.String()
	for _, want := range []string{
		"msg=parse_trace",
		"event=finished",
		"filename=" + logPath,
		"lines=2",
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
		"      \"template\": \"user <*> logged in\",\n" +
		"      \"count\": 2\n" +
		"    }\n" +
		"  ]\n" +
		"}\n"
	if stdout.String() != want {
		t.Fatalf("stdout mismatch:\nwant %q\ngot  %q", want, stdout.String())
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

	logPath := filepath.Join(dir, "target.log")
	logContent := "[Mon May 11 13:41:21 2026] user alice logged in\n"
	if err := os.WriteFile(logPath, []byte(logContent), 0o644); err != nil {
		t.Fatalf("write log: %v", err)
	}

	var stdout bytes.Buffer
	var stderr bytes.Buffer
	if err := run([]string{"parse", "-filename", logPath, "-model", modelPath}, &stdout, &stderr); err != nil {
		t.Fatalf("run parse: %v", err)
	}

	want := "{\"template_id\":1,\"variables\":[\"[Mon May 11 13:41:21 2026]\",\"alice\"]}\n"
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
		"   ",
		"prefix[Mon May 11 13:41:21 2026]suffix",
	} {
		t.Run(line, func(t *testing.T) {
			got := tokenizeLine(line, compiledRules)
			want := tokenizeLineLegacy(line, compiledRules)
			if !reflect.DeepEqual(got, want) {
				t.Fatalf("tokens mismatch:\nwant %#v\ngot  %#v", want, got)
			}
		})
	}

	if _, ok := tokenizeLineSingleMask("prefix[Mon May 11 13:41:21 2026]suffix", compiledRules[0]); ok {
		t.Fatal("embedded mask should use legacy fallback")
	}
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
