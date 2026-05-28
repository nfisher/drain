package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
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
