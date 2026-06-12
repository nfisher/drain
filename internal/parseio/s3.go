package parseio

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strconv"
	"strings"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
)

type OptionalString struct {
	Value string
	Set   bool
}

type OptionalBool struct {
	Value bool
	Set   bool
}

type S3Options struct {
	Endpoint            OptionalString
	EndpointFile        OptionalString
	EndpointEnv         OptionalString
	Region              OptionalString
	RegionFile          OptionalString
	RegionEnv           OptionalString
	AccessKeyID         OptionalString
	AccessKeyIDFile     OptionalString
	AccessKeyIDEnv      OptionalString
	SecretAccessKey     OptionalString
	SecretAccessKeyFile OptionalString
	SecretAccessKeyEnv  OptionalString
	SessionToken        OptionalString
	SessionTokenFile    OptionalString
	SessionTokenEnv     OptionalString
	UseSSL              OptionalBool
	UseSSLFile          OptionalString
	UseSSLEnv           OptionalString
	PathStyle           OptionalBool
	PathStyleFile       OptionalString
	PathStyleEnv        OptionalString
}

type S3Config struct {
	Endpoint        string
	Region          string
	AccessKeyID     string
	SecretAccessKey string
	SessionToken    string
	UseSSL          bool
	PathStyle       bool
}

type S3Store struct {
	bucket string
	prefix string
	config S3Config
}

func NewS3Store(prefix string, options S3Options) (*S3Store, error) {
	bucket, objectPrefix, err := parseS3Prefix(prefix)
	if err != nil {
		return nil, err
	}
	config, err := ResolveS3Config(options)
	if err != nil {
		return nil, err
	}
	return &S3Store{bucket: bucket, prefix: objectPrefix, config: config}, nil
}

func parseS3Prefix(value string) (bucket, objectPrefix string, err error) {
	withoutScheme := strings.TrimPrefix(value, "s3://")
	if withoutScheme == "" {
		return "", "", errors.New("s3 output prefix must include a bucket")
	}
	bucket, objectPrefix, _ = strings.Cut(withoutScheme, "/")
	if bucket == "" {
		return "", "", errors.New("s3 output prefix must include a bucket")
	}
	return bucket, strings.Trim(objectPrefix, "/"), nil
}

func (s *S3Store) Create(ctx context.Context, objectPath, contentType string) (io.WriteCloser, error) {
	key := path.Join(s.prefix, objectPath)
	file, err := os.CreateTemp("", "drain-cluster-output-*")
	if err != nil {
		return nil, err
	}
	return &s3TempWriter{
		ctx:         ctx,
		file:        file,
		config:      s.config,
		bucket:      s.bucket,
		key:         key,
		contentType: contentType,
	}, nil
}

type s3TempWriter struct {
	ctx         context.Context
	file        *os.File
	config      S3Config
	bucket      string
	key         string
	contentType string
}

func (w *s3TempWriter) Write(p []byte) (int, error) {
	return w.file.Write(p)
}

func (w *s3TempWriter) Close() error {
	var firstErr error
	info, err := w.file.Stat()
	if err != nil && firstErr == nil {
		firstErr = err
	}
	if _, err := w.file.Seek(0, io.SeekStart); err != nil && firstErr == nil {
		firstErr = err
	}
	if firstErr == nil {
		if err := PutS3Object(w.ctx, w.config, w.bucket, w.key, w.file, info.Size(), w.contentType); err != nil {
			firstErr = err
		}
	}
	name := w.file.Name()
	if err := w.file.Close(); err != nil && firstErr == nil {
		firstErr = err
	}
	if err := os.Remove(name); err != nil && firstErr == nil {
		firstErr = err
	}
	return firstErr
}

type PutS3ObjectFunc func(ctx context.Context, config S3Config, bucket, key string, reader io.Reader, size int64, contentType string) error

var PutS3Object PutS3ObjectFunc = putS3ObjectMinIO

func putS3ObjectMinIO(ctx context.Context, config S3Config, bucket, key string, reader io.Reader, size int64, contentType string) error {
	bucketLookup := minio.BucketLookupPath
	if !config.PathStyle {
		bucketLookup = minio.BucketLookupDNS
	}
	client, err := minio.New(config.Endpoint, &minio.Options{
		Creds:        credentials.NewStaticV4(config.AccessKeyID, config.SecretAccessKey, config.SessionToken),
		Secure:       config.UseSSL,
		Region:       config.Region,
		BucketLookup: bucketLookup,
	})
	if err != nil {
		return err
	}
	_, err = client.PutObject(ctx, bucket, key, reader, size, minio.PutObjectOptions{
		ContentType: contentType,
	})
	return err
}

func ResolveS3Config(options S3Options) (S3Config, error) {
	endpointRaw, err := stringFromFlagEnvOrFile(
		options.Endpoint,
		options.EndpointFile,
		options.EndpointEnv,
		"s3 endpoint",
		[]string{"S3_ENDPOINT", "AWS_ENDPOINT_URL"},
		[]string{"S3_ENDPOINT_FILE", "AWS_ENDPOINT_URL_FILE"},
	)
	if err != nil {
		return S3Config{}, err
	}
	endpoint, err := normalizeS3Endpoint(endpointRaw)
	if err != nil {
		return S3Config{}, err
	}
	if endpoint == "" {
		return S3Config{}, errors.New("s3 endpoint is required; set -s3-endpoint or S3_ENDPOINT")
	}

	useSSLDefault := true
	useSSL, err := boolFromFlagEnvOrFile(options.UseSSL, options.UseSSLFile, options.UseSSLEnv, "s3-use-ssl-file", "s3 use_ssl", useSSLDefault, []string{"S3_USE_SSL"}, []string{"S3_USE_SSL_FILE"})
	if err != nil {
		return S3Config{}, err
	}
	pathStyle, err := boolFromFlagEnvOrFile(options.PathStyle, options.PathStyleFile, options.PathStyleEnv, "s3-path-style-file", "s3 path_style", true, []string{"S3_PATH_STYLE"}, []string{"S3_PATH_STYLE_FILE"})
	if err != nil {
		return S3Config{}, err
	}
	region, err := stringFromFlagEnvOrFile(options.Region, options.RegionFile, options.RegionEnv, "s3 region", []string{"S3_REGION", "AWS_REGION", "AWS_DEFAULT_REGION"}, []string{"S3_REGION_FILE", "AWS_REGION_FILE", "AWS_DEFAULT_REGION_FILE"})
	if err != nil {
		return S3Config{}, err
	}
	accessKeyID, err := stringFromFlagEnvOrFile(options.AccessKeyID, options.AccessKeyIDFile, options.AccessKeyIDEnv, "s3 access_key_id", []string{"S3_ACCESS_KEY_ID", "AWS_ACCESS_KEY_ID"}, []string{"S3_ACCESS_KEY_ID_FILE", "AWS_ACCESS_KEY_ID_FILE"})
	if err != nil {
		return S3Config{}, err
	}
	secretAccessKey, err := stringFromFlagEnvOrFile(options.SecretAccessKey, options.SecretAccessKeyFile, options.SecretAccessKeyEnv, "s3 secret_access_key", []string{"S3_SECRET_ACCESS_KEY", "AWS_SECRET_ACCESS_KEY"}, []string{"S3_SECRET_ACCESS_KEY_FILE", "AWS_SECRET_ACCESS_KEY_FILE"})
	if err != nil {
		return S3Config{}, err
	}
	sessionToken, err := stringFromFlagEnvOrFile(options.SessionToken, options.SessionTokenFile, options.SessionTokenEnv, "s3 session_token", []string{"S3_SESSION_TOKEN", "AWS_SESSION_TOKEN"}, []string{"S3_SESSION_TOKEN_FILE", "AWS_SESSION_TOKEN_FILE"})
	if err != nil {
		return S3Config{}, err
	}

	config := S3Config{
		Endpoint:        endpoint,
		Region:          stringDefault(region, "us-east-1"),
		AccessKeyID:     accessKeyID,
		SecretAccessKey: secretAccessKey,
		SessionToken:    sessionToken,
		UseSSL:          useSSL,
		PathStyle:       pathStyle,
	}
	if (config.AccessKeyID == "") != (config.SecretAccessKey == "") {
		return S3Config{}, errors.New("s3 credentials require both access key ID and secret access key")
	}
	if config.AccessKeyID == "" {
		return S3Config{}, errors.New("s3 access key ID and secret access key are required")
	}
	return config, nil
}

func normalizeS3Endpoint(value string) (string, error) {
	if value == "" {
		return "", nil
	}
	if strings.Contains(value, "://") {
		scheme, rest, _ := strings.Cut(value, "://")
		if scheme != "http" && scheme != "https" {
			return "", fmt.Errorf("s3 endpoint scheme must be http or https, got %q", scheme)
		}
		if rest == "" || strings.Contains(rest, "/") {
			return "", fmt.Errorf("s3 endpoint must be a host without a path, got %q", value)
		}
		return rest, nil
	}
	return value, nil
}

func stringFromFlagEnvOrFile(valueFlag, fileFlag, envFlag OptionalString, name string, envNames, envFileNames []string) (string, error) {
	if valueFlag.Set {
		return valueFlag.Value, nil
	}
	if fileFlag.Set {
		return readSecretFile(fileFlag.Value)
	}
	if envFlag.Set {
		return requiredEnvValue(envFlag.Value, name)
	}
	for _, name := range envNames {
		if value, ok := os.LookupEnv(name); ok {
			return value, nil
		}
	}
	for _, name := range envFileNames {
		if value, ok := os.LookupEnv(name); ok {
			return readSecretFile(value)
		}
	}
	return "", nil
}

func boolFromFlagEnvOrFile(valueFlag OptionalBool, fileFlag, envFlag OptionalString, fileFlagName, name string, defaultValue bool, envNames, envFileNames []string) (bool, error) {
	if valueFlag.Set {
		return valueFlag.Value, nil
	}
	if fileFlag.Set {
		return boolFromFile(fileFlag.Value, fileFlagName)
	}
	if envFlag.Set {
		value, err := requiredEnvValue(envFlag.Value, name)
		if err != nil {
			return false, err
		}
		return parseBoolValue(name+" env "+envFlag.Value, value)
	}
	for _, name := range envNames {
		if value, ok := os.LookupEnv(name); ok {
			return parseBoolValue(name, value)
		}
	}
	for _, name := range envFileNames {
		if value, ok := os.LookupEnv(name); ok {
			return boolFromFile(value, name)
		}
	}
	return defaultValue, nil
}

func requiredEnvValue(envName, name string) (string, error) {
	if envName == "" {
		return "", fmt.Errorf("%s env var name must not be empty", name)
	}
	value, ok := os.LookupEnv(envName)
	if !ok {
		return "", fmt.Errorf("%s env var %s is not set", name, envName)
	}
	return value, nil
}

func parseBoolValue(name, value string) (bool, error) {
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must be a boolean, got %q", name, value)
	}
	return parsed, nil
}

func boolFromFile(path, source string) (bool, error) {
	value, err := readSecretFile(path)
	if err != nil {
		return false, err
	}
	parsed, err := strconv.ParseBool(value)
	if err != nil {
		return false, fmt.Errorf("%s must point to a file containing a boolean, got %q", source, value)
	}
	return parsed, nil
}

func readSecretFile(path string) (string, error) {
	if path == "" {
		return "", errors.New("secret file path must not be empty")
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		return "", fmt.Errorf("read secret file %s: %w", path, err)
	}
	return strings.TrimSpace(string(contents)), nil
}

func stringDefault(value, defaultValue string) string {
	if value == "" {
		return defaultValue
	}
	return value
}
