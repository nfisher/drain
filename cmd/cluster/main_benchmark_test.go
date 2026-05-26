package main

import (
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/faceair/drain"
)

var (
	benchmarkLineTokensSink  []lineToken
	benchmarkVariablesSink   []string
	benchmarkTemplateIDSink  int
	benchmarkTemplateOKSink  bool
	benchmarkTemplateLenSink int
)

func benchmarkTemplate(id, size int, template string) templateModel {
	return templateModel{
		ID:       id,
		Size:     size,
		Template: template,
		Tokens:   splitTemplate(template),
	}
}

func benchmarkModel() modelFile {
	return modelFile{
		Version:      modelVersion,
		ParamString:  "<*>",
		MaskingRules: modelMaskingRules(clusterConfig().MaskingRules),
		Templates: []templateModel{
			benchmarkTemplate(1, 10_000, "<*> service api host <*> request <*> method GET path <*> status <*> latency_ms <*>"),
			benchmarkTemplate(2, 9_000, "<*> service api host <*> request <*> method POST path <*> status <*> latency_ms <*>"),
			benchmarkTemplate(3, 8_000, "<*> service worker host <*> job <*> queue email status done duration_ms <*>"),
			benchmarkTemplate(4, 7_500, "<*> service worker host <*> job <*> queue billing status retry duration_ms <*>"),
			benchmarkTemplate(5, 6_500, "<*> service db host <*> query <*> table users operation select rows <*> duration_ms <*>"),
			benchmarkTemplate(6, 6_000, "<*> service auth host <*> user <*> action login result success duration_ms <*>"),
		},
	}
}

func benchmarkClusterLines(count int) []string {
	lines := make([]string, count)
	for i := range lines {
		timestamp := fmt.Sprintf("[Mon May %02d 13:%02d:%02d 2026]", i%28+1, i%60, (i*7)%60)
		switch i % 6 {
		case 0:
			lines[i] = fmt.Sprintf("%s service api host web-%03d request r-%06d method GET path /v1/users/%d status 200 latency_ms %d", timestamp, i%64, i, 1000+i, 5+i%90)
		case 1:
			lines[i] = fmt.Sprintf("%s service api host web-%03d request r-%06d method POST path /v1/orders/%d status 201 latency_ms %d", timestamp, i%64, i, 2000+i, 12+i%120)
		case 2:
			lines[i] = fmt.Sprintf("%s service worker host worker-%03d job j-%06d queue email status done duration_ms %d", timestamp, i%32, i, 20+i%80)
		case 3:
			lines[i] = fmt.Sprintf("%s service worker host worker-%03d job j-%06d queue billing status retry duration_ms %d", timestamp, i%32, i, 30+i%100)
		case 4:
			lines[i] = fmt.Sprintf("%s service db host db-%02d query q-%06d table users operation select rows %d duration_ms %d", timestamp, i%12, i, 10+i%500, 3+i%50)
		default:
			lines[i] = fmt.Sprintf("%s service auth host auth-%02d user user-%06d action login result success duration_ms %d", timestamp, i%8, i, 1+i%30)
		}
	}
	return lines
}

func benchmarkCompiledRules(b *testing.B, model modelFile) []compiledMaskingRule {
	b.Helper()
	compiledRules, err := compileMaskingRules(model.MaskingRules, model.ParamString)
	if err != nil {
		b.Fatalf("compile masking rules: %v", err)
	}
	return compiledRules
}

func benchmarkDrainFromModel(b *testing.B, model modelFile) *drain.Drain {
	b.Helper()
	logger, err := drainFromModel(model)
	if err != nil {
		b.Fatalf("restore model: %v", err)
	}
	return logger
}

func BenchmarkClusterMatchRestoredModel(b *testing.B) {
	model := benchmarkModel()
	logger := benchmarkDrainFromModel(b, model)
	lines := benchmarkClusterLines(2048)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		cluster := logger.Match(lines[i%len(lines)])
		if cluster == nil {
			b.Fatalf("line did not match: %q", lines[i%len(lines)])
		}
		benchmarkTemplateIDSink = cluster.ID()
	}
}

func BenchmarkClusterTokenizeLineWithMasks(b *testing.B) {
	model := benchmarkModel()
	compiledRules := benchmarkCompiledRules(b, model)
	line := benchmarkClusterLines(1)[0]

	b.ReportAllocs()
	b.SetBytes(int64(len(line)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		benchmarkLineTokensSink = tokenizeLine(line, compiledRules)
	}
}

func BenchmarkClusterMatchTemplateVariables(b *testing.B) {
	model := benchmarkModel()
	compiledRules := benchmarkCompiledRules(b, model)
	templateTokens := model.Templates[0].Tokens
	line := benchmarkClusterLines(1)[0]
	lineTokens := tokenizeLine(line, compiledRules)
	variablesScratch := make([]string, 0, countParams(model.ParamString, templateTokens))

	b.ReportAllocs()
	b.SetBytes(int64(len(line)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		variables, ok := matchTemplate(model.ParamString, templateTokens, lineTokens, variablesScratch[:0])
		if !ok {
			b.Fatalf("template did not match line: %q", line)
		}
		benchmarkVariablesSink = variables
		benchmarkTemplateLenSink = len(variables)
		benchmarkTemplateOKSink = ok
	}
}

func BenchmarkClusterParseLineHotPath(b *testing.B) {
	model := benchmarkModel()
	compiledRules := benchmarkCompiledRules(b, model)
	logger := benchmarkDrainFromModel(b, model)
	parseTemplates, maxTemplateParamCount := parseTemplatesFromModel(model)
	variablesScratch := make([]string, 0, maxTemplateParamCount)
	lines := benchmarkClusterLines(2048)

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		line := lines[i%len(lines)]
		cluster := logger.Match(line)
		if cluster == nil {
			b.Fatalf("line did not match: %q", line)
		}
		parseTemplate, ok := parseTemplates[cluster.ID()]
		if !ok {
			b.Fatalf("template %d was not found in model", cluster.ID())
		}
		variables, ok := matchTemplate(model.ParamString, parseTemplate.tokens, tokenizeLine(line, compiledRules), variablesScratch[:0])
		if !ok {
			b.Fatalf("template %d did not match line: %q", parseTemplate.id, line)
		}
		benchmarkVariablesSink = variables
	}
}

func BenchmarkClusterRunParseEndToEnd(b *testing.B) {
	dir := b.TempDir()
	modelPath := filepath.Join(dir, "model.json")
	logPath := filepath.Join(dir, "target.log")
	model := benchmarkModel()
	if err := writeModel(modelPath, model); err != nil {
		b.Fatalf("write model: %v", err)
	}

	lines := benchmarkClusterLines(4096)
	logContent := strings.Join(lines, "\n") + "\n"
	if err := os.WriteFile(logPath, []byte(logContent), 0o644); err != nil {
		b.Fatalf("write target log: %v", err)
	}

	args := []string{"parse", "-filename", logPath, "-model", modelPath}
	b.ReportAllocs()
	b.SetBytes(int64(len(logContent)))
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		if err := run(args, io.Discard, io.Discard); err != nil {
			b.Fatalf("run parse: %v", err)
		}
	}
}
