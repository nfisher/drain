package main

import (
	"bufio"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log/slog"
	"os"
	"regexp"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/faceair/drain"
)

const (
	modelVersion           = 1
	timestampPrefixPattern = `^\[[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-3]?[0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [0-9]{4}\]`
)

type modelFile struct {
	Version      int                `json:"version"`
	ParamString  string             `json:"param_string"`
	MaskingRules []modelMaskingRule `json:"masking_rules"`
	Templates    []templateModel    `json:"templates"`
}

type modelMaskingRule struct {
	Pattern  string `json:"pattern"`
	MaskWith string `json:"mask_with,omitempty"`
}

type templateModel struct {
	ID       int      `json:"id"`
	Size     int      `json:"size"`
	Template string   `json:"template"`
	Tokens   []string `json:"tokens"`
}

type templateDistribution struct {
	TemplateID int    `json:"template_id"`
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
	TemplateID *int     `json:"template_id"`
	Variables  []string `json:"variables"`
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
	tokens     []string
	paramCount int
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
	fmt.Fprintln(w, "  cluster train [-update] -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster test  -filename <log> -model <model.json>")
	fmt.Fprintln(w, "  cluster parse -filename <log> -model <model.json>")
}

func runTrain(args []string, stdout io.Writer) error {
	fs := flag.NewFlagSet("train", flag.ContinueOnError)
	fs.SetOutput(io.Discard)
	filename := fs.String("filename", "example.log", "training log file")
	modelPath := fs.String("model", "model.json", "model output path")
	update := fs.Bool("update", false, "load and update the existing model")
	if err := fs.Parse(args); err != nil {
		return err
	}

	config := clusterConfig()
	logger := drain.New(config)
	if *update {
		existingModel, _, err := readModel(*modelPath)
		if err != nil {
			return err
		}
		config = configFromModel(existingModel)
		logger = drain.New(config)
		if err := logger.LoadClusters(snapshotsFromModel(existingModel)); err != nil {
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
	if err := writeModel(*modelPath, model); err != nil {
		return err
	}
	fmt.Fprintf(stdout, "wrote %d templates to %s\n", len(model.Templates), *modelPath)
	return nil
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
		cluster := logger.Match(line)
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
	filename := fs.String("filename", "example.log", "target log file")
	modelPath := fs.String("model", "model.json", "model path")
	if err := fs.Parse(args); err != nil {
		return err
	}

	model, compiledRules, err := readModel(*modelPath)
	if err != nil {
		return err
	}
	logger, err := drainFromModel(model)
	if err != nil {
		return err
	}
	parseTemplates, maxTemplateParamCount := parseTemplatesFromModel(model)

	fileInfo, err := os.Stat(*filename)
	if err != nil {
		return err
	}

	encoder := json.NewEncoder(stdout)
	encoder.SetEscapeHTML(false)
	parsedLines := 0
	variablesScratch := make([]string, 0, maxTemplateParamCount)
	started := time.Now()
	if err := scanLines(*filename, func(line string) error {
		cluster := logger.Match(line)
		output := parseOutput{
			Variables: []string{},
		}
		if cluster != nil {
			clusterID := cluster.ID()
			parseTemplate, ok := parseTemplates[clusterID]
			if !ok {
				return fmt.Errorf("matched cluster %d was not found in model", clusterID)
			}
			variables, ok := matchTemplate(model.ParamString, parseTemplate.tokens, tokenizeLine(line, compiledRules), variablesScratch[:0])
			if !ok {
				return fmt.Errorf("matched cluster %d did not match during variable extraction", clusterID)
			}
			templateID := parseTemplate.id
			output.TemplateID = &templateID
			output.Variables = variables
		}
		if err := encoder.Encode(output); err != nil {
			return err
		}
		parsedLines++
		return nil
	}); err != nil {
		return err
	}
	traceParseSpeed(stderr, *filename, parsedLines, fileInfo.Size(), time.Since(started))
	return nil
}

func clusterConfig() *drain.Config {
	config := drain.DefaultConfig()
	config.MaskingRules = []drain.MaskingRule{
		{
			Pattern: timestampPrefixPattern,
		},
	}
	config.LogClusterDepth = 6
	return config
}

func modelMaskingRules(rules []drain.MaskingRule) []modelMaskingRule {
	modelRules := make([]modelMaskingRule, 0, len(rules))
	for _, rule := range rules {
		modelRules = append(modelRules, modelMaskingRule{
			Pattern:  rule.Pattern,
			MaskWith: rule.MaskWith,
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
		Version:      modelVersion,
		ParamString:  config.ParamString,
		MaskingRules: modelMaskingRules(config.MaskingRules),
		Templates:    make([]templateModel, 0, len(snapshots)),
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
	config.MaskingRules = drainMaskingRules(model.MaskingRules)
	return config
}

func drainMaskingRules(rules []modelMaskingRule) []drain.MaskingRule {
	drainRules := make([]drain.MaskingRule, 0, len(rules))
	for _, rule := range rules {
		drainRules = append(drainRules, drain.MaskingRule{
			Pattern:  rule.Pattern,
			MaskWith: rule.MaskWith,
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
	sortTemplates(model.Templates)

	compiledRules, err := compileMaskingRules(model.MaskingRules, model.ParamString)
	if err != nil {
		return modelFile{}, nil, err
	}
	return model, compiledRules, nil
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
		replacement := rule.MaskWith
		if replacement == "" {
			replacement = defaultReplacement
		}
		compiled = append(compiled, compiledMaskingRule{
			regex:       regex,
			replacement: replacement,
		})
	}
	return compiled, nil
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

func traceParseSpeed(w io.Writer, filename string, lines int, bytes int64, elapsed time.Duration) {
	elapsedSeconds := elapsed.Seconds()
	if elapsedSeconds <= 0 {
		elapsedSeconds = 1e-9
	}
	logger := slog.New(slog.NewTextHandler(w, nil))
	logger.Info(
		"parse_trace",
		slog.String("event", "finished"),
		slog.String("filename", filename),
		slog.Int("lines", lines),
		slog.Int64("bytes", bytes),
		slog.Float64("duration_seconds", elapsedSeconds),
		slog.Float64("lines_per_second", float64(lines)/elapsedSeconds),
		slog.Float64("bytes_per_second", float64(bytes)/elapsedSeconds),
	)
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

func tokenizeLine(line string, maskingRules []compiledMaskingRule) []lineToken {
	line = strings.TrimSpace(line)
	if len(maskingRules) == 1 {
		if tokens, ok := tokenizeLineSingleMask(line, maskingRules[0]); ok {
			return tokens
		}
	}
	return tokenizeLineLegacy(line, maskingRules)
}

func tokenizeLineLegacy(line string, maskingRules []compiledMaskingRule) []lineToken {
	line = strings.TrimSpace(line)
	masked := maskLine(line, maskingRules)
	replacedParts := make([]string, 0, len(masked))
	maskedByMarker := make(map[string]lineSegment)
	maskedCount := 0
	for _, segment := range masked {
		if !segment.masked {
			replacedParts = append(replacedParts, segment.rawString)
			continue
		}
		marker := fmt.Sprintf("\x00DRAIN_MASK_%d\x00", maskedCount)
		maskedCount++
		replacedParts = append(replacedParts, marker)
		maskedByMarker[marker] = segment
	}

	replaced := strings.Join(replacedParts, "")
	replacedTokens := strings.Fields(replaced)
	tokens := make([]lineToken, 0)
	for _, token := range replacedTokens {
		if segment, ok := maskedByMarker[token]; ok {
			tokens = append(tokens, lineToken{
				value:     segment.value,
				rawString: segment.rawString,
			})
			continue
		}
		tokens = append(tokens, lineToken{
			value:     token,
			rawString: token,
		})
	}
	return tokens
}

func tokenizeLineSingleMask(line string, maskingRule compiledMaskingRule) ([]lineToken, bool) {
	matches := maskingRule.regex.FindAllStringIndex(line, -1)
	if len(matches) == 0 {
		return splitLineTokens(line), true
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

func splitLineTokens(line string) []lineToken {
	parts := strings.Fields(line)
	tokens := make([]lineToken, 0, len(parts))
	for _, part := range parts {
		tokens = append(tokens, lineToken{
			value:     part,
			rawString: part,
		})
	}
	return tokens
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
