package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"math"
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
	modelVersion           = 2
	minimumModelVersion    = 1
	modelFingerprintSchema = "drain-model-fingerprint-v1"
	timestampPrefixPattern = `^\[[A-Z][a-z]{2} [A-Z][a-z]{2} [ 0-3]?[0-9] [0-2][0-9]:[0-5][0-9]:[0-5][0-9] [0-9]{4}\]`
)

type modelFile struct {
	Version                  int                        `json:"version"`
	ModelID                  string                     `json:"-"`
	Fingerprint              string                     `json:"fingerprint,omitempty"`
	ParamString              string                     `json:"param_string"`
	SimTh                    *float64                   `json:"sim_th,omitempty"`
	LogClusterDepth          *int                       `json:"log_cluster_depth,omitempty"`
	MaxChildren              *int                       `json:"max_children,omitempty"`
	ParametrizeNumericTokens *bool                      `json:"parametrize_numeric_tokens,omitempty"`
	ExtraDelimiters          []string                   `json:"extra_delimiters,omitempty"`
	Metadata                 map[string]json.RawMessage `json:"metadata,omitempty"`
	MaskingRules             []modelMaskingRule         `json:"masking_rules"`
	Templates                []templateModel            `json:"templates"`
}

type modelMaskingRule struct {
	Pattern      string `json:"pattern,omitempty"`
	RegexPattern string `json:"regex_pattern,omitempty"`
	MaskWith     string `json:"mask_with,omitempty"`
	Replacement  string `json:"replacement,omitempty"`
}

type templateModel struct {
	ID       int      `json:"id"`
	Size     int      `json:"size"`
	Template string   `json:"template"`
	Tokens   []string `json:"tokens"`
}

type canonicalModelFingerprint struct {
	Schema                   string             `json:"schema"`
	Version                  int                `json:"version"`
	ParamString              string             `json:"param_string"`
	SimTh                    float64            `json:"sim_th"`
	LogClusterDepth          int                `json:"log_cluster_depth"`
	MaxChildren              int                `json:"max_children"`
	ParametrizeNumericTokens bool               `json:"parametrize_numeric_tokens"`
	ExtraDelimiters          []string           `json:"extra_delimiters,omitempty"`
	MaskingRules             []modelMaskingRule `json:"masking_rules,omitempty"`
	Templates                []templateModel    `json:"templates"`
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
	template   string
	tokens     []string
	paramCount int
}

type extraDelimiterFlags []string

func (f *extraDelimiterFlags) String() string {
	return strings.Join(*f, ",")
}

func (f *extraDelimiterFlags) Set(value string) error {
	if value == "" {
		return errors.New("extra delimiter must not be empty")
	}
	*f = append(*f, value)
	return nil
}
func clusterConfig() *drain.Config {
	config := drain.DefaultConfig()
	config.MaskingRules = defaultMaskingRules()
	config.LogClusterDepth = 6
	return config
}

func defaultMaskingRules() []drain.MaskingRule {
	return []drain.MaskingRule{
		{Pattern: timestampPrefixPattern},
		{Pattern: `\b(?:[0-9a-f]{2,}:){3,}[0-9a-f]{2,}\b`, MaskWith: "ID"},
		{Pattern: `\b\d{1,3}(?:\.\d{1,3}){3}\b`, MaskWith: "IP"},
		{Pattern: `\b[0-9a-f]{6,}(?:\s+[0-9a-f]{6,}){2,}\b`, MaskWith: "SEQ"},
		{Pattern: `\b[0-9A-F]{4}(?:\s+[0-9A-F]{4}){3,}\b`, MaskWith: "SEQ"},
		{Pattern: `\b0x[a-f0-9A-F]+\b`, MaskWith: "HEX"},
		{Pattern: `[-+]?\b\d+\b`, MaskWith: "NUM"},
	}
}

func modelMaskingRules(rules []drain.MaskingRule) []modelMaskingRule {
	modelRules := make([]modelMaskingRule, 0, len(rules))
	for _, rule := range rules {
		modelRules = append(modelRules, modelMaskingRule{
			Pattern:     rule.Pattern,
			MaskWith:    rule.MaskWith,
			Replacement: rule.Replacement,
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
			template:   strings.Join(tokens, " "),
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
		Version:                  modelVersion,
		ParamString:              config.ParamString,
		SimTh:                    float64Pointer(config.SimTh),
		LogClusterDepth:          intPointer(config.LogClusterDepth),
		MaxChildren:              intPointer(config.MaxChildren),
		ParametrizeNumericTokens: boolPointer(!config.PreserveNumericTokens),
		ExtraDelimiters:          copyStrings(config.ExtraDelimiters),
		MaskingRules:             modelMaskingRules(config.MaskingRules),
		Templates:                make([]templateModel, 0, len(snapshots)),
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
	if model.SimTh != nil {
		config.SimTh = *model.SimTh
	}
	if model.LogClusterDepth != nil {
		config.LogClusterDepth = *model.LogClusterDepth
	}
	if model.MaxChildren != nil {
		config.MaxChildren = *model.MaxChildren
	}
	if model.ParametrizeNumericTokens != nil {
		config.PreserveNumericTokens = !*model.ParametrizeNumericTokens
	}
	config.ExtraDelimiters = copyStrings(model.ExtraDelimiters)
	config.MaskingRules = drainMaskingRules(model.MaskingRules)
	return config
}

func float64Pointer(value float64) *float64 {
	return &value
}

func intPointer(value int) *int {
	return &value
}

func boolPointer(value bool) *bool {
	return &value
}

func copyStrings(values []string) []string {
	if len(values) == 0 {
		return nil
	}
	copied := make([]string, len(values))
	copy(copied, values)
	return copied
}

func readMetadataFile(path string) (map[string]json.RawMessage, error) {
	contents, err := os.ReadFile(path) // #nosec G304 -- metadata path is an explicit CLI input.
	if err != nil {
		return nil, fmt.Errorf("read metadata %s: %w", path, err)
	}

	var root any
	if err := json.Unmarshal(contents, &root); err != nil {
		return nil, fmt.Errorf("decode metadata %s: %w", path, err)
	}
	if _, ok := root.(map[string]any); !ok {
		return nil, fmt.Errorf("metadata file %s must contain a JSON object", path)
	}

	var metadata map[string]json.RawMessage
	if err := json.Unmarshal(contents, &metadata); err != nil {
		return nil, fmt.Errorf("decode metadata %s: %w", path, err)
	}
	return metadata, nil
}

func readMaskingRulesFile(path string) ([]modelMaskingRule, error) {
	contents, err := os.ReadFile(path) // #nosec G304 -- masking-rules path is an explicit CLI input.
	if err != nil {
		return nil, fmt.Errorf("read masking rules %s: %w", path, err)
	}

	var root any
	if err := json.Unmarshal(contents, &root); err != nil {
		return nil, fmt.Errorf("decode masking rules %s: %w", path, err)
	}
	if _, ok := root.([]any); !ok {
		return nil, fmt.Errorf("masking rules file %s must contain a JSON array", path)
	}

	var rules []modelMaskingRule
	if err := json.Unmarshal(contents, &rules); err != nil {
		return nil, fmt.Errorf("decode masking rules %s: %w", path, err)
	}
	if err := normalizeMaskingRules(rules, "masking_rules"); err != nil {
		return nil, err
	}
	return rules, nil
}

func normalizeMaskingRules(rules []modelMaskingRule, name string) error {
	for i := range rules {
		if err := normalizeMaskingRule(&rules[i], fmt.Sprintf("%s[%d]", name, i)); err != nil {
			return err
		}
	}
	return nil
}

func normalizeMaskingRule(rule *modelMaskingRule, name string) error {
	if rule.Pattern != "" && rule.RegexPattern != "" && rule.Pattern != rule.RegexPattern {
		return fmt.Errorf("%s has conflicting pattern and regex_pattern", name)
	}
	if rule.Pattern == "" {
		rule.Pattern = rule.RegexPattern
	}
	rule.RegexPattern = ""
	if rule.Pattern == "" {
		return fmt.Errorf("%s pattern must not be empty", name)
	}
	if _, err := regexp.Compile(rule.Pattern); err != nil {
		return fmt.Errorf("compile %s pattern %q: %w", name, rule.Pattern, err)
	}
	return nil
}

func metadataWithTimestamps(existing, metadataFromFile map[string]json.RawMessage, update bool, now time.Time) map[string]json.RawMessage {
	metadata := copyMetadata(existing)
	mergeMetadata(metadata, metadataFromFile)

	timestamp := metadataString(now.UTC().Format(time.RFC3339))
	if update {
		if !hasValidMetadataTimestamp(metadata, "created_at") {
			metadata["created_at"] = timestamp
		}
		metadata["updated_at"] = timestamp
		return metadata
	}

	metadata["created_at"] = timestamp
	delete(metadata, "updated_at")
	return metadata
}

func copyMetadata(metadata map[string]json.RawMessage) map[string]json.RawMessage {
	if len(metadata) == 0 {
		return make(map[string]json.RawMessage)
	}
	copied := make(map[string]json.RawMessage, len(metadata))
	for key, value := range metadata {
		copied[key] = cloneRawMessage(value)
	}
	return copied
}

func mergeMetadata(dst, src map[string]json.RawMessage) {
	for key, value := range src {
		if isMetadataTimestampKey(key) {
			continue
		}
		dst[key] = cloneRawMessage(value)
	}
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	if value == nil {
		return nil
	}
	copied := make(json.RawMessage, len(value))
	copy(copied, value)
	return copied
}

func isMetadataTimestampKey(key string) bool {
	return key == "created_at" || key == "updated_at"
}

func metadataString(value string) json.RawMessage {
	encoded, _ := json.Marshal(value)
	return encoded
}

func hasValidMetadataTimestamp(metadata map[string]json.RawMessage, key string) bool {
	var value string
	if err := json.Unmarshal(metadata[key], &value); err != nil {
		return false
	}
	_, err := time.Parse(time.RFC3339, value)
	return err == nil
}

func drainMaskingRules(rules []modelMaskingRule) []drain.MaskingRule {
	drainRules := make([]drain.MaskingRule, 0, len(rules))
	for _, rule := range rules {
		drainRules = append(drainRules, drain.MaskingRule{
			Pattern:     rule.Pattern,
			MaskWith:    rule.MaskWith,
			Replacement: rule.Replacement,
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

func modelIDFromTemplates(templates []templateModel) string {
	return fingerprintJSON(canonicalModelFingerprint{
		Schema:    "drain-template-fingerprint-v1",
		Templates: canonicalTemplates(templates),
	})
}

func modelFingerprint(model modelFile) string {
	config := configFromModel(model)
	return fingerprintJSON(canonicalModelFingerprint{
		Schema:                   modelFingerprintSchema,
		Version:                  modelVersion,
		ParamString:              config.ParamString,
		SimTh:                    config.SimTh,
		LogClusterDepth:          config.LogClusterDepth,
		MaxChildren:              config.MaxChildren,
		ParametrizeNumericTokens: !config.PreserveNumericTokens,
		ExtraDelimiters:          copyStrings(config.ExtraDelimiters),
		MaskingRules:             append([]modelMaskingRule(nil), model.MaskingRules...),
		Templates:                canonicalTemplates(model.Templates),
	})
}

func canonicalTemplates(templates []templateModel) []templateModel {
	sortedTemplates := append([]templateModel(nil), templates...)
	sortTemplates(sortedTemplates)

	canonicalTemplates := make([]templateModel, 0, len(sortedTemplates))
	for _, template := range sortedTemplates {
		tokens := templateTokens(template)
		copiedTokens := make([]string, len(tokens))
		copy(copiedTokens, tokens)
		canonicalTemplate := template.Template
		if canonicalTemplate == "" {
			canonicalTemplate = strings.Join(copiedTokens, " ")
		}
		canonicalTemplates = append(canonicalTemplates, templateModel{
			ID:       template.ID,
			Size:     template.Size,
			Template: canonicalTemplate,
			Tokens:   copiedTokens,
		})
	}
	return canonicalTemplates
}

func fingerprintJSON(value any) string {
	var encoded bytes.Buffer
	encoder := json.NewEncoder(&encoded)
	encoder.SetEscapeHTML(false)
	_ = encoder.Encode(value)
	digestInput := bytes.TrimSuffix(encoded.Bytes(), []byte("\n"))
	digest := sha256.Sum256(digestInput)
	return base64.RawURLEncoding.EncodeToString(digest[:])
}

func writeModel(path string, model modelFile) error {
	model, err := modelForWrite(model)
	if err != nil {
		return err
	}

	file, err := os.Create(path) // #nosec G304 -- model path is an explicit CLI output.
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	encoder.SetEscapeHTML(false)
	encoder.SetIndent("", "  ")
	return encoder.Encode(model)
}

func modelForWrite(model modelFile) (modelFile, error) {
	model.Version = modelVersion
	if err := normalizeMaskingRules(model.MaskingRules, "model masking_rules"); err != nil {
		return modelFile{}, err
	}
	sortTemplates(model.Templates)
	preserveLegacyLiteralMaskReplacements(&model)
	if err := validateModel(model); err != nil {
		return modelFile{}, err
	}
	model.Fingerprint = modelFingerprint(model)
	return model, nil
}

func readModel(path string) (modelFile, []compiledMaskingRule, error) {
	file, err := os.Open(path) // #nosec G304 -- model path is an explicit CLI/config input.
	if err != nil {
		return modelFile{}, nil, err
	}
	defer file.Close()

	var model modelFile
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&model); err != nil {
		return modelFile{}, nil, err
	}
	model, err = migrateModel(model)
	if err != nil {
		return modelFile{}, nil, err
	}
	if err := normalizeMaskingRules(model.MaskingRules, "model masking_rules"); err != nil {
		return modelFile{}, nil, err
	}
	sortTemplates(model.Templates)
	preserveLegacyLiteralMaskReplacements(&model)
	if err := validateModel(model); err != nil {
		return modelFile{}, nil, err
	}
	computedFingerprint := modelFingerprint(model)
	if model.Fingerprint != "" && model.Fingerprint != computedFingerprint {
		return modelFile{}, nil, fmt.Errorf("model fingerprint mismatch: expected %q, computed %q", model.Fingerprint, computedFingerprint)
	}
	model.Fingerprint = computedFingerprint
	model.ModelID = computedFingerprint

	compiledRules, err := compileMaskingRules(model.MaskingRules, model.ParamString)
	if err != nil {
		return modelFile{}, nil, err
	}
	return model, compiledRules, nil
}

func migrateModel(model modelFile) (modelFile, error) {
	switch {
	case model.Version == modelVersion:
		return model, nil
	case model.Version >= minimumModelVersion && model.Version < modelVersion:
		model.Version = modelVersion
		model.Fingerprint = ""
		return model, nil
	default:
		return modelFile{}, fmt.Errorf("unsupported model version %d", model.Version)
	}
}

func validateModel(model modelFile) error {
	if model.Version != modelVersion {
		return fmt.Errorf("unsupported model version %d", model.Version)
	}
	if model.ParamString == "" {
		return errors.New("model param_string must not be empty")
	}
	if model.SimTh != nil {
		if err := validateSimTh("model sim_th", *model.SimTh); err != nil {
			return err
		}
	}
	if model.LogClusterDepth != nil {
		if err := validateDepth("model log_cluster_depth", *model.LogClusterDepth); err != nil {
			return err
		}
	}
	if model.MaxChildren != nil {
		if err := validateMaxChildren("model max_children", *model.MaxChildren); err != nil {
			return err
		}
	}
	if err := validateExtraDelimiters("model extra_delimiters", model.ExtraDelimiters); err != nil {
		return err
	}
	seenTemplateIDs := make(map[int]struct{}, len(model.Templates))
	for i, template := range model.Templates {
		if template.ID <= 0 {
			return fmt.Errorf("model templates[%d] id must be positive, got %d", i, template.ID)
		}
		if template.Size <= 0 {
			return fmt.Errorf("model templates[%d] size must be positive, got %d", i, template.Size)
		}
		if _, ok := seenTemplateIDs[template.ID]; ok {
			return fmt.Errorf("model templates[%d] duplicates id %d", i, template.ID)
		}
		seenTemplateIDs[template.ID] = struct{}{}

		tokens := templateTokens(template)
		if len(tokens) == 0 {
			return fmt.Errorf("model templates[%d] tokens must not be empty", i)
		}
		if template.Template != "" && template.Template != strings.Join(tokens, " ") {
			return fmt.Errorf("model templates[%d] template must match tokens", i)
		}
	}
	return nil
}

func validateSimTh(name string, value float64) error {
	if math.IsNaN(value) || value < 0 || value > 1 {
		return fmt.Errorf("%s must be between 0 and 1, got %g", name, value)
	}
	return nil
}

func validateDepth(name string, value int) error {
	if value < 3 {
		return fmt.Errorf("%s must be at least 3, got %d", name, value)
	}
	return nil
}

func validateMaxChildren(name string, value int) error {
	if value < 1 {
		return fmt.Errorf("%s must be at least 1, got %d", name, value)
	}
	return nil
}

func validateExtraDelimiters(name string, extraDelimiters []string) error {
	for i, extraDelimiter := range extraDelimiters {
		if extraDelimiter == "" {
			return fmt.Errorf("%s[%d] must not be empty", name, i)
		}
	}
	return nil
}

func parameterValues(parameters []drain.ExtractedParameter) []string {
	return appendParameterValues(make([]string, 0, len(parameters)), parameters)
}

func appendParameterValues(values []string, parameters []drain.ExtractedParameter) []string {
	values = values[:0]
	for _, parameter := range parameters {
		values = append(values, parameter.Value)
	}
	return values
}

func hasNamedParameters(parameters []drain.ExtractedParameter) bool {
	for _, parameter := range parameters {
		if parameter.MaskName != "*" {
			return true
		}
	}
	return false
}

func preserveLegacyLiteralMaskReplacements(model *modelFile) {
	for i := range model.MaskingRules {
		rule := &model.MaskingRules[i]
		if rule.Replacement != "" || rule.MaskWith == "" || isExplicitMaskReplacement(rule.MaskWith) {
			continue
		}
		if modelUsesMaskToken(*model, namedMaskToken(rule.MaskWith)) {
			continue
		}
		if modelUsesMaskToken(*model, rule.MaskWith) {
			rule.Replacement = rule.MaskWith
		}
	}
}

func modelUsesMaskToken(model modelFile, token string) bool {
	for _, template := range model.Templates {
		for _, templateToken := range templateTokens(template) {
			if tokenAppearsAsMaskReplacement(templateToken, token) {
				return true
			}
		}
	}
	return false
}

func tokenAppearsAsMaskReplacement(templateToken, token string) bool {
	if isExplicitMaskReplacement(token) {
		return strings.Contains(templateToken, token)
	}
	for searchOffset := 0; searchOffset < len(templateToken); {
		index := strings.Index(templateToken[searchOffset:], token)
		if index < 0 {
			return false
		}
		start := searchOffset + index
		end := start + len(token)
		if hasReplacementBoundary(templateToken, start, end) {
			return true
		}
		searchOffset = end
	}
	return false
}

func hasReplacementBoundary(value string, start, end int) bool {
	if start > 0 {
		r, _ := utf8.DecodeLastRuneInString(value[:start])
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return false
		}
	}
	if end < len(value) {
		r, _ := utf8.DecodeRuneInString(value[end:])
		if unicode.IsLetter(r) || unicode.IsNumber(r) {
			return false
		}
	}
	return true
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
		replacement := maskingRuleReplacement(rule, defaultReplacement)
		compiled = append(compiled, compiledMaskingRule{
			regex:       regex,
			replacement: replacement,
		})
	}
	return compiled, nil
}

func maskingRuleReplacement(rule modelMaskingRule, defaultReplacement string) string {
	if rule.Replacement != "" {
		return rule.Replacement
	}
	if rule.MaskWith == "" {
		return defaultReplacement
	}
	if isExplicitMaskReplacement(rule.MaskWith) {
		return rule.MaskWith
	}
	return namedMaskToken(rule.MaskWith)
}

func isExplicitMaskReplacement(maskWith string) bool {
	return strings.ContainsAny(maskWith, "<>$")
}

func namedMaskToken(maskName string) string {
	return "<:" + maskName + ":>"
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

func tokenizeLine(line string, maskingRules []compiledMaskingRule, extraDelimiters []string) []lineToken {
	line = strings.TrimSpace(line)
	if len(maskingRules) == 1 && len(extraDelimiters) == 0 {
		if tokens, ok := tokenizeLineSingleMask(line, maskingRules[0]); ok {
			return tokens
		}
	}
	return tokenizeLineLegacy(line, maskingRules, extraDelimiters)
}

func tokenizeLineLegacy(line string, maskingRules []compiledMaskingRule, extraDelimiters []string) []lineToken {
	line = strings.TrimSpace(line)
	masked := maskLine(line, maskingRules)
	replacedParts := make([]string, 0, len(masked))
	maskedByMarker := make(map[string]lineSegment)
	markers := make([]string, 0, len(masked))
	maskedCount := 0
	for _, segment := range masked {
		if !segment.masked {
			replacedParts = append(replacedParts, replaceExtraDelimiters(segment.rawString, extraDelimiters))
			continue
		}
		marker := fmt.Sprintf("\x00DRAIN_MASK_%d\x00", maskedCount)
		maskedCount++
		replacedParts = append(replacedParts, marker)
		maskedByMarker[marker] = segment
		markers = append(markers, marker)
	}

	replaced := strings.Join(replacedParts, "")
	replacedTokens := strings.Fields(replaced)
	tokens := make([]lineToken, 0)
	for _, token := range replacedTokens {
		value := token
		rawString := token
		for _, marker := range markers {
			segment := maskedByMarker[marker]
			if !strings.Contains(value, marker) {
				continue
			}
			value = strings.ReplaceAll(value, marker, segment.value)
			rawString = strings.ReplaceAll(rawString, marker, segment.rawString)
		}
		tokens = append(tokens, lineToken{
			value:     value,
			rawString: rawString,
		})
	}
	return tokens
}

func tokenizeLineSingleMask(line string, maskingRule compiledMaskingRule) ([]lineToken, bool) {
	matches := maskingRule.regex.FindAllStringIndex(line, -1)
	if len(matches) == 0 {
		return splitLineTokens(line, nil), true
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

func splitLineTokens(line string, extraDelimiters []string) []lineToken {
	parts := strings.Fields(replaceExtraDelimiters(line, extraDelimiters))
	tokens := make([]lineToken, 0, len(parts))
	for _, part := range parts {
		tokens = append(tokens, lineToken{
			value:     part,
			rawString: part,
		})
	}
	return tokens
}

func replaceExtraDelimiters(line string, extraDelimiters []string) string {
	for _, extraDelimiter := range extraDelimiters {
		line = strings.Replace(line, extraDelimiter, " ", -1)
	}
	return line
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
