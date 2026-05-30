package main

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/faceair/drain/internal/parseio"
	"github.com/hashicorp/hcl/v2/hclsimple"
)

type parsePipelinesConfigFile struct {
	Sources   []parseNamedSourceConfig `hcl:"source,block"`
	Sinks     []parseNamedSinkConfig   `hcl:"sink,block"`
	Pipelines []parsePipelineConfig    `hcl:"pipeline,block"`
}

type parsePipelineConfig struct {
	Name       string              `hcl:"name,label"`
	Model      string              `hcl:"model"`
	SourceRefs []string            `hcl:"sources,optional"`
	SinkRefs   []string            `hcl:"sinks,optional"`
	Sources    []parseSourceConfig `hcl:"source,block"`
	Sinks      []parseSinkConfig   `hcl:"sink,block"`
}

type parseSourceConfig struct {
	Kind        string   `hcl:"kind,label"`
	Filename    string   `hcl:"filename,optional"`
	Follow      bool     `hcl:"follow,optional"`
	Units       []string `hcl:"units,optional"`
	Identifiers []string `hcl:"identifiers,optional"`
	Priority    string   `hcl:"priority,optional"`
	Since       string   `hcl:"since,optional"`
	Until       string   `hcl:"until,optional"`
	Boot        string   `hcl:"boot,optional"`
	AfterCursor string   `hcl:"after_cursor,optional"`
	LineFormat  string   `hcl:"line_format,optional"`
}

type parseNamedSourceConfig struct {
	Kind        string   `hcl:"kind,label"`
	Name        string   `hcl:"name,label"`
	Filename    string   `hcl:"filename,optional"`
	Follow      bool     `hcl:"follow,optional"`
	Units       []string `hcl:"units,optional"`
	Identifiers []string `hcl:"identifiers,optional"`
	Priority    string   `hcl:"priority,optional"`
	Since       string   `hcl:"since,optional"`
	Until       string   `hcl:"until,optional"`
	Boot        string   `hcl:"boot,optional"`
	AfterCursor string   `hcl:"after_cursor,optional"`
	LineFormat  string   `hcl:"line_format,optional"`
}

type parseSinkConfig struct {
	Format            string          `hcl:"format,label"`
	Output            *string         `hcl:"output,optional"`
	IncludeParameters bool            `hcl:"include_parameters,optional"`
	ExcludeSource     bool            `hcl:"exclude_source,optional"`
	BatchSize         *int            `hcl:"batch_size,optional"`
	BatchMaxAge       *string         `hcl:"batch_max_age,optional"`
	S3                []parseS3Config `hcl:"s3,block"`
}

type parseNamedSinkConfig struct {
	Format            string          `hcl:"format,label"`
	Name              string          `hcl:"name,label"`
	Output            *string         `hcl:"output,optional"`
	IncludeParameters bool            `hcl:"include_parameters,optional"`
	ExcludeSource     bool            `hcl:"exclude_source,optional"`
	BatchSize         *int            `hcl:"batch_size,optional"`
	BatchMaxAge       *string         `hcl:"batch_max_age,optional"`
	S3                []parseS3Config `hcl:"s3,block"`
}

type parseS3Config struct {
	Endpoint            *string `hcl:"endpoint,optional"`
	EndpointFile        *string `hcl:"endpoint_file,optional"`
	EndpointEnv         *string `hcl:"endpoint_env,optional"`
	Region              *string `hcl:"region,optional"`
	RegionFile          *string `hcl:"region_file,optional"`
	RegionEnv           *string `hcl:"region_env,optional"`
	AccessKeyID         *string `hcl:"access_key_id,optional"`
	AccessKeyIDFile     *string `hcl:"access_key_id_file,optional"`
	AccessKeyIDEnv      *string `hcl:"access_key_id_env,optional"`
	SecretAccessKey     *string `hcl:"secret_access_key,optional"`
	SecretAccessKeyFile *string `hcl:"secret_access_key_file,optional"`
	SecretAccessKeyEnv  *string `hcl:"secret_access_key_env,optional"`
	SessionToken        *string `hcl:"session_token,optional"`
	SessionTokenFile    *string `hcl:"session_token_file,optional"`
	SessionTokenEnv     *string `hcl:"session_token_env,optional"`
	UseSSL              *bool   `hcl:"use_ssl,optional"`
	UseSSLFile          *string `hcl:"use_ssl_file,optional"`
	UseSSLEnv           *string `hcl:"use_ssl_env,optional"`
	PathStyle           *bool   `hcl:"path_style,optional"`
	PathStyleFile       *string `hcl:"path_style_file,optional"`
	PathStyleEnv        *string `hcl:"path_style_env,optional"`
}

type parsePipelineOptions struct {
	Name      string
	ModelPath string
	Sources   []parseSourceOptions
	Sinks     []parseOutputOptions
}

func readParsePipelinesConfig(path string) ([]parsePipelineOptions, error) {
	var file parsePipelinesConfigFile
	if err := hclsimple.DecodeFile(path, nil, &file); err != nil {
		return nil, err
	}
	return parsePipelineConfigOptions(file)
}

func parsePipelineConfigOptions(file parsePipelinesConfigFile) ([]parsePipelineOptions, error) {
	if len(file.Pipelines) == 0 {
		return nil, errors.New("config must contain at least one pipeline block")
	}
	sources, err := parseNamedSourceOptions(file.Sources)
	if err != nil {
		return nil, err
	}
	sinks, err := parseNamedSinkOptions(file.Sinks)
	if err != nil {
		return nil, err
	}

	pipelines := make([]parsePipelineOptions, 0, len(file.Pipelines))
	for i, pipeline := range file.Pipelines {
		opts, err := parsePipelineOptionsFromConfig(pipeline, sources, sinks)
		if err != nil {
			return nil, fmt.Errorf("pipeline %q: %w", pipelineConfigName(pipeline, i), err)
		}
		pipelines = append(pipelines, opts)
	}
	return pipelines, nil
}

func parseNamedSourceOptions(configs []parseNamedSourceConfig) (map[string]parseSourceOptions, error) {
	sources := make(map[string]parseSourceOptions, len(configs))
	for i, config := range configs {
		key, err := namedConfigReference(config.Kind, config.Name)
		if err != nil {
			return nil, fmt.Errorf("source[%d]: %w", i, err)
		}
		opts, err := parseSourceOptionsFromConfig(namedSourceConfigBody(config))
		if err != nil {
			return nil, fmt.Errorf("source %q: %w", key, err)
		}
		if _, exists := sources[key]; exists {
			return nil, fmt.Errorf("source %q is defined more than once", key)
		}
		sources[key] = opts
	}
	return sources, nil
}

func parseNamedSinkOptions(configs []parseNamedSinkConfig) (map[string]parseOutputOptions, error) {
	sinks := make(map[string]parseOutputOptions, len(configs))
	for i, config := range configs {
		key, err := namedConfigReference(config.Format, config.Name)
		if err != nil {
			return nil, fmt.Errorf("sink[%d]: %w", i, err)
		}
		opts, err := parseSinkOptionsFromConfig(namedSinkConfigBody(config))
		if err != nil {
			return nil, fmt.Errorf("sink %q: %w", key, err)
		}
		if _, exists := sinks[key]; exists {
			return nil, fmt.Errorf("sink %q is defined more than once", key)
		}
		sinks[key] = opts
	}
	return sinks, nil
}

func parsePipelineOptionsFromConfig(config parsePipelineConfig, namedSources map[string]parseSourceOptions, namedSinks map[string]parseOutputOptions) (parsePipelineOptions, error) {
	if strings.TrimSpace(config.Model) == "" {
		return parsePipelineOptions{}, errors.New("model must not be empty")
	}
	if len(config.Sources) == 0 && len(config.SourceRefs) == 0 {
		return parsePipelineOptions{}, errors.New("must contain at least one source block or source reference")
	}
	if len(config.Sinks) == 0 && len(config.SinkRefs) == 0 {
		return parsePipelineOptions{}, errors.New("must contain at least one sink block or sink reference")
	}

	pipeline := parsePipelineOptions{
		Name:      config.Name,
		ModelPath: config.Model,
		Sources:   make([]parseSourceOptions, 0, len(config.SourceRefs)+len(config.Sources)),
		Sinks:     make([]parseOutputOptions, 0, len(config.SinkRefs)+len(config.Sinks)),
	}
	for i, ref := range config.SourceRefs {
		opts, err := parseReferencedSourceOptions(ref, namedSources)
		if err != nil {
			return parsePipelineOptions{}, fmt.Errorf("sources[%d]: %w", i, err)
		}
		pipeline.Sources = append(pipeline.Sources, opts)
	}
	for i, source := range config.Sources {
		opts, err := parseSourceOptionsFromConfig(source)
		if err != nil {
			return parsePipelineOptions{}, fmt.Errorf("source[%d] %q: %w", i, source.Kind, err)
		}
		pipeline.Sources = append(pipeline.Sources, opts)
	}
	for i, ref := range config.SinkRefs {
		opts, err := parseReferencedSinkOptions(ref, namedSinks)
		if err != nil {
			return parsePipelineOptions{}, fmt.Errorf("sinks[%d]: %w", i, err)
		}
		pipeline.Sinks = append(pipeline.Sinks, opts)
	}
	for i, sink := range config.Sinks {
		opts, err := parseSinkOptionsFromConfig(sink)
		if err != nil {
			return parsePipelineOptions{}, fmt.Errorf("sink[%d] %q: %w", i, sink.Format, err)
		}
		pipeline.Sinks = append(pipeline.Sinks, opts)
	}
	return pipeline, nil
}

func parseReferencedSourceOptions(ref string, sources map[string]parseSourceOptions) (parseSourceOptions, error) {
	key, err := referencedConfigKey(ref, "source")
	if err != nil {
		return parseSourceOptions{}, err
	}
	opts, ok := sources[key]
	if !ok {
		return parseSourceOptions{}, fmt.Errorf("source reference %q is not defined", key)
	}
	return copyParseSourceOptions(opts), nil
}

func parseReferencedSinkOptions(ref string, sinks map[string]parseOutputOptions) (parseOutputOptions, error) {
	key, err := referencedConfigKey(ref, "sink")
	if err != nil {
		return parseOutputOptions{}, err
	}
	opts, ok := sinks[key]
	if !ok {
		return parseOutputOptions{}, fmt.Errorf("sink reference %q is not defined", key)
	}
	return opts, nil
}

func namedConfigReference(kind, name string) (string, error) {
	kind = strings.TrimSpace(kind)
	name = strings.TrimSpace(name)
	if kind == "" {
		return "", errors.New("kind label must not be empty")
	}
	if name == "" {
		return "", errors.New("name label must not be empty")
	}
	if strings.Contains(kind, ".") || strings.Contains(name, ".") {
		return "", errors.New("kind and name labels must not contain .")
	}
	return kind + "." + name, nil
}

func referencedConfigKey(ref, blockType string) (string, error) {
	ref = strings.TrimSpace(ref)
	kind, name, ok := strings.Cut(ref, ".")
	if ref == "" || !ok || strings.TrimSpace(kind) == "" || strings.TrimSpace(name) == "" || strings.Contains(name, ".") {
		return "", fmt.Errorf("%s reference must use <kind>.<name>, got %q", blockType, ref)
	}
	return ref, nil
}

func namedSourceConfigBody(config parseNamedSourceConfig) parseSourceConfig {
	return parseSourceConfig{
		Kind:        config.Kind,
		Filename:    config.Filename,
		Follow:      config.Follow,
		Units:       copyStrings(config.Units),
		Identifiers: copyStrings(config.Identifiers),
		Priority:    config.Priority,
		Since:       config.Since,
		Until:       config.Until,
		Boot:        config.Boot,
		AfterCursor: config.AfterCursor,
		LineFormat:  config.LineFormat,
	}
}

func namedSinkConfigBody(config parseNamedSinkConfig) parseSinkConfig {
	return parseSinkConfig{
		Format:            config.Format,
		Output:            config.Output,
		IncludeParameters: config.IncludeParameters,
		ExcludeSource:     config.ExcludeSource,
		BatchSize:         config.BatchSize,
		BatchMaxAge:       config.BatchMaxAge,
		S3:                config.S3,
	}
}

func copyParseSourceOptions(opts parseSourceOptions) parseSourceOptions {
	opts.Systemd.Units = copyStrings(opts.Systemd.Units)
	opts.Systemd.Identifiers = copyStrings(opts.Systemd.Identifiers)
	return opts
}

func parseSourceOptionsFromConfig(config parseSourceConfig) (parseSourceOptions, error) {
	switch config.Kind {
	case "file":
		if strings.TrimSpace(config.Filename) == "" {
			return parseSourceOptions{}, errors.New("filename must not be empty")
		}
		if config.Follow {
			return parseSourceOptions{}, fmt.Errorf("source %q does not support follow", config.Kind)
		}
		if sourceConfigHasSystemdOptions(config) {
			return parseSourceOptions{}, errors.New("systemd options are only supported for systemd sources")
		}
		return parseSourceOptions{Kind: config.Kind, Filename: config.Filename}, nil
	case "dmesg":
		if strings.TrimSpace(config.Filename) != "" {
			return parseSourceOptions{}, errors.New("filename is only supported for file sources")
		}
		if sourceConfigHasSystemdOptions(config) {
			return parseSourceOptions{}, errors.New("systemd options are only supported for systemd sources")
		}
		return parseSourceOptions{Kind: config.Kind, Follow: config.Follow}, nil
	case "systemd":
		if strings.TrimSpace(config.Filename) != "" {
			return parseSourceOptions{}, errors.New("filename is only supported for file sources")
		}
		systemdOptions := parseio.SystemdOptions{
			Follow:      config.Follow,
			Units:       copyStrings(config.Units),
			Identifiers: copyStrings(config.Identifiers),
			Priority:    config.Priority,
			Since:       config.Since,
			Until:       config.Until,
			Boot:        config.Boot,
			AfterCursor: config.AfterCursor,
			LineFormat:  config.LineFormat,
		}
		if err := parseio.ValidateSystemdOptions(systemdOptions); err != nil {
			return parseSourceOptions{}, err
		}
		return parseSourceOptions{Kind: config.Kind, Systemd: systemdOptions}, nil
	default:
		return parseSourceOptions{}, fmt.Errorf("source %q is not supported yet", config.Kind)
	}
}

func sourceConfigHasSystemdOptions(config parseSourceConfig) bool {
	return len(config.Units) > 0 ||
		len(config.Identifiers) > 0 ||
		config.Priority != "" ||
		config.Since != "" ||
		config.Until != "" ||
		config.Boot != "" ||
		config.AfterCursor != "" ||
		config.LineFormat != ""
}

func parseSinkOptionsFromConfig(config parseSinkConfig) (parseOutputOptions, error) {
	opts := parseOutputOptions{
		Format:            config.Format,
		IncludeParameters: config.IncludeParameters,
		ExcludeSource:     config.ExcludeSource,
		BatchSize:         defaultParseBatchSize,
		BatchMaxAge:       defaultParseBatchMaxAge,
	}
	if config.Output != nil {
		opts.Prefix = *config.Output
	}
	if config.BatchSize != nil {
		opts.BatchSize = *config.BatchSize
	}
	if config.BatchMaxAge != nil {
		duration, err := time.ParseDuration(*config.BatchMaxAge)
		if err != nil {
			return parseOutputOptions{}, fmt.Errorf("batch_max_age must be a duration: %w", err)
		}
		opts.BatchMaxAge = duration
	}
	if len(config.S3) > 1 {
		return parseOutputOptions{}, errors.New("must contain at most one s3 block")
	}
	if len(config.S3) == 1 {
		if opts.Prefix == "" || !strings.HasPrefix(opts.Prefix, "s3://") {
			return parseOutputOptions{}, errors.New("s3 block requires output to start with s3://")
		}
		s3, err := parseS3OptionsFromConfig(config.S3[0])
		if err != nil {
			return parseOutputOptions{}, err
		}
		opts.S3 = s3
	}
	if opts.Format == parseFormatParquet && strings.TrimSpace(opts.Prefix) == "" {
		return parseOutputOptions{}, fmt.Errorf("%s output requires output", opts.Format)
	}
	if err := validateParseOutputOptions(opts.Format, opts.BatchSize, opts.BatchMaxAge); err != nil {
		return parseOutputOptions{}, err
	}
	return opts, nil
}

func parseS3OptionsFromConfig(config parseS3Config) (parseio.S3Options, error) {
	var opts parseio.S3Options
	var err error
	if opts.Endpoint, opts.EndpointFile, opts.EndpointEnv, err = s3StringOptions("endpoint", config.Endpoint, config.EndpointFile, config.EndpointEnv); err != nil {
		return parseio.S3Options{}, err
	}
	if opts.Region, opts.RegionFile, opts.RegionEnv, err = s3StringOptions("region", config.Region, config.RegionFile, config.RegionEnv); err != nil {
		return parseio.S3Options{}, err
	}
	if opts.AccessKeyID, opts.AccessKeyIDFile, opts.AccessKeyIDEnv, err = s3StringOptions("access_key_id", config.AccessKeyID, config.AccessKeyIDFile, config.AccessKeyIDEnv); err != nil {
		return parseio.S3Options{}, err
	}
	if opts.SecretAccessKey, opts.SecretAccessKeyFile, opts.SecretAccessKeyEnv, err = s3StringOptions("secret_access_key", config.SecretAccessKey, config.SecretAccessKeyFile, config.SecretAccessKeyEnv); err != nil {
		return parseio.S3Options{}, err
	}
	if opts.SessionToken, opts.SessionTokenFile, opts.SessionTokenEnv, err = s3StringOptions("session_token", config.SessionToken, config.SessionTokenFile, config.SessionTokenEnv); err != nil {
		return parseio.S3Options{}, err
	}
	if opts.UseSSL, opts.UseSSLFile, opts.UseSSLEnv, err = s3BoolOptions("use_ssl", config.UseSSL, config.UseSSLFile, config.UseSSLEnv); err != nil {
		return parseio.S3Options{}, err
	}
	if opts.PathStyle, opts.PathStyleFile, opts.PathStyleEnv, err = s3BoolOptions("path_style", config.PathStyle, config.PathStyleFile, config.PathStyleEnv); err != nil {
		return parseio.S3Options{}, err
	}
	return opts, nil
}

func s3StringOptions(name string, value, file, env *string) (parseio.OptionalString, parseio.OptionalString, parseio.OptionalString, error) {
	if countSetPointers(value, file, env) > 1 {
		return parseio.OptionalString{}, parseio.OptionalString{}, parseio.OptionalString{}, fmt.Errorf("s3 %s can set only one of %s, %s_file, or %s_env", name, name, name, name)
	}
	return optionalString(value), optionalString(file), optionalString(env), nil
}

func s3BoolOptions(name string, value *bool, file, env *string) (parseio.OptionalBool, parseio.OptionalString, parseio.OptionalString, error) {
	if countSetBoolPointers(value, file, env) > 1 {
		return parseio.OptionalBool{}, parseio.OptionalString{}, parseio.OptionalString{}, fmt.Errorf("s3 %s can set only one of %s, %s_file, or %s_env", name, name, name, name)
	}
	return optionalBool(value), optionalString(file), optionalString(env), nil
}

func optionalString(value *string) parseio.OptionalString {
	if value == nil {
		return parseio.OptionalString{}
	}
	return parseio.OptionalString{Value: *value, Set: true}
}

func optionalBool(value *bool) parseio.OptionalBool {
	if value == nil {
		return parseio.OptionalBool{}
	}
	return parseio.OptionalBool{Value: *value, Set: true}
}

func countSetPointers(values ...*string) int {
	count := 0
	for _, value := range values {
		if value != nil {
			count++
		}
	}
	return count
}

func countSetBoolPointers(value *bool, strings ...*string) int {
	count := 0
	if value != nil {
		count++
	}
	return count + countSetPointers(strings...)
}

func pipelineConfigName(config parsePipelineConfig, index int) string {
	if config.Name != "" {
		return config.Name
	}
	return fmt.Sprintf("%d", index)
}
