package main

import (
	"errors"
	"flag"
	"fmt"
	"io"
	"strings"

	"github.com/faceair/drain/internal/parseio"
	"github.com/hashicorp/hcl/v2/hclwrite"
	"github.com/zclconf/go-cty/cty"
)

func writeGeneratedParseConfig(stdout io.Writer, fs *flag.FlagSet, modelPath string, source parseSourceOptions, sink parseOutputOptions) error {
	generated, err := generateParseConfigHCL(fs, modelPath, source, sink)
	if err != nil {
		return err
	}
	_, err = stdout.Write(generated)
	return err
}

func generateParseConfigHCL(fs *flag.FlagSet, modelPath string, source parseSourceOptions, sink parseOutputOptions) ([]byte, error) {
	if strings.TrimSpace(modelPath) == "" {
		return nil, errors.New("model path must not be empty")
	}
	source.Kind = normalizedParseSourceKind(source.Kind)
	if err := validateParseGenerateSourceOptions(source); err != nil {
		return nil, err
	}
	if err := validateParseGenerateSinkOptions(sink); err != nil {
		return nil, err
	}

	file := hclwrite.NewEmptyFile()
	pipeline := file.Body().AppendNewBlock("pipeline", []string{"default"})
	pipelineBody := pipeline.Body()
	pipelineBody.SetAttributeValue("model", cty.StringVal(modelPath))
	pipelineBody.AppendNewline()

	sourceBlock := pipelineBody.AppendNewBlock("source", []string{source.Kind})
	writeGeneratedSourceConfig(sourceBlock.Body(), source, fs)
	pipelineBody.AppendNewline()

	sinkBlock := pipelineBody.AppendNewBlock("sink", []string{sink.Format})
	writeGeneratedSinkConfig(sinkBlock.Body(), sink, fs)

	out := hclwrite.Format(file.Bytes())
	if len(out) == 0 || out[len(out)-1] != '\n' {
		out = append(out, '\n')
	}
	return out, nil
}

func validateParseGenerateSourceOptions(source parseSourceOptions) error {
	switch source.Kind {
	case "file":
		if strings.TrimSpace(source.Filename) == "" {
			return errors.New("filename must not be empty")
		}
		if source.Follow {
			return fmt.Errorf("source %q does not support -follow", source.Kind)
		}
	case "dmesg":
	case "systemd":
		systemdOptions := source.Systemd
		systemdOptions.Follow = systemdOptions.Follow || source.Follow
		if err := parseio.ValidateSystemdOptions(systemdOptions); err != nil {
			return err
		}
	default:
		return fmt.Errorf("source %q is not supported yet", source.Kind)
	}
	return nil
}

func validateParseGenerateSinkOptions(sink parseOutputOptions) error {
	if err := validateParseOutputOptions(sink.Format, sink.BatchSize, sink.BatchMaxAge); err != nil {
		return err
	}
	if sink.Format == parseFormatParquet && strings.TrimSpace(sink.Prefix) == "" {
		return fmt.Errorf("%s output requires -output", sink.Format)
	}
	if parseS3OptionsProvided(sink.S3) && !strings.HasPrefix(sink.Prefix, "s3://") {
		return errors.New("S3 flags require -output to start with s3:// when generating config")
	}
	return nil
}

func writeGeneratedSourceConfig(body *hclwrite.Body, source parseSourceOptions, fs *flag.FlagSet) {
	switch source.Kind {
	case "file":
		body.SetAttributeValue("filename", cty.StringVal(source.Filename))
	case "dmesg":
		if source.Follow {
			body.SetAttributeValue("follow", cty.BoolVal(true))
		}
	case "systemd":
		systemdOptions := source.Systemd
		if source.Follow || systemdOptions.Follow {
			body.SetAttributeValue("follow", cty.BoolVal(true))
		}
		setStringListAttribute(body, "units", systemdOptions.Units)
		setStringListAttribute(body, "identifiers", systemdOptions.Identifiers)
		setNonEmptyStringAttribute(body, "priority", systemdOptions.Priority)
		setNonEmptyStringAttribute(body, "since", systemdOptions.Since)
		setNonEmptyStringAttribute(body, "until", systemdOptions.Until)
		setNonEmptyStringAttribute(body, "boot", systemdOptions.Boot)
		setNonEmptyStringAttribute(body, "after_cursor", systemdOptions.AfterCursor)
		if flagWasProvided(fs, "systemd-line-format") || systemdOptions.LineFormat != parseio.SystemdLineFormatMessage {
			setNonEmptyStringAttribute(body, "line_format", systemdOptions.LineFormat)
		}
	}
}

func writeGeneratedSinkConfig(body *hclwrite.Body, sink parseOutputOptions, fs *flag.FlagSet) {
	setNonEmptyStringAttribute(body, "output", sink.Prefix)
	if sink.IncludeParameters {
		body.SetAttributeValue("include_parameters", cty.BoolVal(true))
	}
	if sink.ExcludeSource {
		body.SetAttributeValue("exclude_source", cty.BoolVal(true))
	}
	if flagWasProvided(fs, "batch-size") || sink.BatchSize != defaultParseBatchSize {
		body.SetAttributeValue("batch_size", cty.NumberIntVal(int64(sink.BatchSize)))
	}
	if flagWasProvided(fs, "batch-max-age") || sink.BatchMaxAge != defaultParseBatchMaxAge {
		body.SetAttributeValue("batch_max_age", cty.StringVal(sink.BatchMaxAge.String()))
	}
	if strings.HasPrefix(sink.Prefix, "s3://") && parseS3OptionsProvided(sink.S3) {
		body.AppendNewline()
		s3Block := body.AppendNewBlock("s3", nil)
		writeGeneratedS3Config(s3Block.Body(), sink.S3)
	}
}

func writeGeneratedS3Config(body *hclwrite.Body, opts parseio.S3Options) {
	setGeneratedS3StringAttribute(body, "endpoint", opts.Endpoint, opts.EndpointFile, opts.EndpointEnv)
	setGeneratedS3StringAttribute(body, "region", opts.Region, opts.RegionFile, opts.RegionEnv)
	setGeneratedS3StringAttribute(body, "access_key_id", opts.AccessKeyID, opts.AccessKeyIDFile, opts.AccessKeyIDEnv)
	setGeneratedS3StringAttribute(body, "secret_access_key", opts.SecretAccessKey, opts.SecretAccessKeyFile, opts.SecretAccessKeyEnv)
	setGeneratedS3StringAttribute(body, "session_token", opts.SessionToken, opts.SessionTokenFile, opts.SessionTokenEnv)
	setGeneratedS3BoolAttribute(body, "use_ssl", opts.UseSSL, opts.UseSSLFile, opts.UseSSLEnv)
	setGeneratedS3BoolAttribute(body, "path_style", opts.PathStyle, opts.PathStyleFile, opts.PathStyleEnv)
}

func setNonEmptyStringAttribute(body *hclwrite.Body, name, value string) {
	if value != "" {
		body.SetAttributeValue(name, cty.StringVal(value))
	}
}

func setStringListAttribute(body *hclwrite.Body, name string, values []string) {
	if len(values) == 0 {
		return
	}
	ctyValues := make([]cty.Value, 0, len(values))
	for _, value := range values {
		ctyValues = append(ctyValues, cty.StringVal(value))
	}
	body.SetAttributeValue(name, cty.ListVal(ctyValues))
}

func setOptionalStringAttribute(body *hclwrite.Body, name string, value parseio.OptionalString) {
	if value.Set {
		body.SetAttributeValue(name, cty.StringVal(value.Value))
	}
}

func setOptionalBoolAttribute(body *hclwrite.Body, name string, value parseio.OptionalBool) {
	if value.Set {
		body.SetAttributeValue(name, cty.BoolVal(value.Value))
	}
}

func setGeneratedS3StringAttribute(body *hclwrite.Body, name string, direct, file, env parseio.OptionalString) {
	switch {
	case direct.Set:
		setOptionalStringAttribute(body, name, direct)
	case file.Set:
		setOptionalStringAttribute(body, name+"_file", file)
	case env.Set:
		setOptionalStringAttribute(body, name+"_env", env)
	}
}

func setGeneratedS3BoolAttribute(body *hclwrite.Body, name string, direct parseio.OptionalBool, file, env parseio.OptionalString) {
	switch {
	case direct.Set:
		setOptionalBoolAttribute(body, name, direct)
	case file.Set:
		setOptionalStringAttribute(body, name+"_file", file)
	case env.Set:
		setOptionalStringAttribute(body, name+"_env", env)
	}
}

func normalizedParseSourceKind(sourceKind string) string {
	if sourceKind == "" {
		return "file"
	}
	return sourceKind
}

func parseS3OptionsProvided(opts parseio.S3Options) bool {
	return opts.Endpoint.Set ||
		opts.EndpointFile.Set ||
		opts.EndpointEnv.Set ||
		opts.Region.Set ||
		opts.RegionFile.Set ||
		opts.RegionEnv.Set ||
		opts.AccessKeyID.Set ||
		opts.AccessKeyIDFile.Set ||
		opts.AccessKeyIDEnv.Set ||
		opts.SecretAccessKey.Set ||
		opts.SecretAccessKeyFile.Set ||
		opts.SecretAccessKeyEnv.Set ||
		opts.SessionToken.Set ||
		opts.SessionTokenFile.Set ||
		opts.SessionTokenEnv.Set ||
		opts.UseSSL.Set ||
		opts.UseSSLFile.Set ||
		opts.UseSSLEnv.Set ||
		opts.PathStyle.Set ||
		opts.PathStyleFile.Set ||
		opts.PathStyleEnv.Set
}
