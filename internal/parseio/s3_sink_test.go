package parseio

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/faceair/drain"
	a "github.com/gogunit/gunit/hammy"
)

func TestResolveS3ConfigPrefersExplicitValuesAndNormalizesEndpoint(t *testing.T) {
	assert := a.New(t)
	t.Setenv("S3_ENDPOINT", "ignored.example")
	t.Setenv("S3_USE_SSL", "false")

	config, err := ResolveS3Config(S3Options{
		Endpoint:        OptionalString{Value: "https://s3.example.test", Set: true},
		AccessKeyID:     OptionalString{Value: "access", Set: true},
		SecretAccessKey: OptionalString{Value: "secret", Set: true},
		SessionToken:    OptionalString{Value: "token", Set: true},
		UseSSL:          OptionalBool{Value: true, Set: true},
		PathStyle:       OptionalBool{Value: false, Set: true},
	})

	assert.Requires(a.NilError(err))
	assert.Requires(a.String(config.Endpoint).EqualTo("s3.example.test"))
	assert.Requires(a.String(config.Region).EqualTo("us-east-1"))
	assert.Requires(a.String(config.AccessKeyID).EqualTo("access"))
	assert.Requires(a.String(config.SecretAccessKey).EqualTo("secret"))
	assert.Requires(a.String(config.SessionToken).EqualTo("token"))
	assert.Requires(a.True(config.UseSSL))
	assert.Requires(a.False(config.PathStyle))
}

func TestResolveS3ConfigReadsNamedEnvironmentAndSecretFiles(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	secretPath := filepath.Join(dir, "secret")
	useSSLPath := filepath.Join(dir, "use-ssl")
	assert.Requires(a.NilError(os.WriteFile(secretPath, []byte("secret-from-file\n"), 0o600)))
	assert.Requires(a.NilError(os.WriteFile(useSSLPath, []byte("false\n"), 0o600)))
	t.Setenv("CUSTOM_ENDPOINT", "http://minio.local:9000")
	t.Setenv("CUSTOM_ACCESS_KEY", "access-from-env")
	t.Setenv("CUSTOM_USE_SSL_FILE", useSSLPath)
	t.Setenv("CUSTOM_PATH_STYLE", "true")

	config, err := ResolveS3Config(S3Options{
		EndpointEnv:         OptionalString{Value: "CUSTOM_ENDPOINT", Set: true},
		AccessKeyIDEnv:      OptionalString{Value: "CUSTOM_ACCESS_KEY", Set: true},
		SecretAccessKeyFile: OptionalString{Value: secretPath, Set: true},
		UseSSLFile:          OptionalString{Value: useSSLPath, Set: true},
		PathStyleEnv:        OptionalString{Value: "CUSTOM_PATH_STYLE", Set: true},
	})

	assert.Requires(a.NilError(err))
	assert.Requires(a.String(config.Endpoint).EqualTo("minio.local:9000"))
	assert.Requires(a.String(config.AccessKeyID).EqualTo("access-from-env"))
	assert.Requires(a.String(config.SecretAccessKey).EqualTo("secret-from-file"))
	assert.Requires(a.False(config.UseSSL))
	assert.Requires(a.True(config.PathStyle))
}

func TestResolveS3ConfigReportsActionableErrors(t *testing.T) {
	tests := []struct {
		name    string
		options S3Options
		want    string
	}{
		{name: "missing endpoint", options: S3Options{}, want: "s3 endpoint is required"},
		{name: "unsupported scheme", options: S3Options{Endpoint: OptionalString{Value: "ftp://host", Set: true}}, want: `s3 endpoint scheme must be http or https`},
		{name: "endpoint path", options: S3Options{Endpoint: OptionalString{Value: "https://host/path", Set: true}}, want: "s3 endpoint must be a host without a path"},
		{name: "missing secret", options: S3Options{Endpoint: OptionalString{Value: "host", Set: true}, AccessKeyID: OptionalString{Value: "access", Set: true}}, want: "s3 credentials require both"},
		{name: "bad bool", options: S3Options{Endpoint: OptionalString{Value: "host", Set: true}, AccessKeyID: OptionalString{Value: "access", Set: true}, SecretAccessKey: OptionalString{Value: "secret", Set: true}, UseSSLEnv: OptionalString{Value: "CUSTOM_BAD_BOOL", Set: true}}, want: "s3 use_ssl env var CUSTOM_BAD_BOOL is not set"},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			assert := a.New(t)
			_, err := ResolveS3Config(tt.options)
			assert.Requires(a.Error(err))
			assert.Requires(a.String(err.Error()).Contains(tt.want))
		})
	}
}

func TestNewS3StoreParsesBucketPrefixAndUploadsViaTempFile(t *testing.T) {
	assert := a.New(t)
	var uploaded struct {
		bucket, key, contentType, body string
		size                           int64
	}
	originalPut := PutS3Object
	PutS3Object = func(_ context.Context, _ S3Config, bucket, key string, reader io.Reader, size int64, contentType string) error {
		body, err := io.ReadAll(reader)
		assert.Requires(a.NilError(err))
		uploaded.bucket = bucket
		uploaded.key = key
		uploaded.contentType = contentType
		uploaded.body = string(body)
		uploaded.size = size
		return nil
	}
	defer func() { PutS3Object = originalPut }()

	store, err := NewS3Store("s3://logs/output/", S3Options{Endpoint: OptionalString{Value: "host", Set: true}, AccessKeyID: OptionalString{Value: "access", Set: true}, SecretAccessKey: OptionalString{Value: "secret", Set: true}})
	assert.Requires(a.NilError(err))
	writer, err := store.Create(context.Background(), "part.jsonl", "application/x-ndjson")
	assert.Requires(a.NilError(err))
	_, err = writer.Write([]byte("payload"))
	assert.Requires(a.NilError(err))
	assert.Requires(a.NilError(writer.Close()))

	assert.Requires(a.String(uploaded.bucket).EqualTo("logs"))
	assert.Requires(a.String(uploaded.key).EqualTo("output/part.jsonl"))
	assert.Requires(a.String(uploaded.contentType).EqualTo("application/x-ndjson"))
	assert.Requires(a.String(uploaded.body).EqualTo("payload"))
	assert.Requires(a.Number(uploaded.size).EqualTo(int64(len("payload"))))
}

func TestJSONLStreamWriterHonorsProjectionOptions(t *testing.T) {
	assert := a.New(t)
	var output bytes.Buffer
	sink, err := NewSink(context.Background(), &output, SinkOptions{Format: FormatJSONL, IncludeParameters: false, ExcludeSource: true})
	assert.Requires(a.NilError(err))
	templateID := 7

	err = sink.Write(context.Background(), Output{
		TemplateID: &templateID,
		ModelID:    "model",
		SourceKind: "file",
		SourceName: "app.log",
		Variables:  []string{"one"},
		Parameters: []drain.ExtractedParameter{{Value: "one", MaskName: "word"}},
	})

	assert.Requires(a.NilError(err))
	assert.Requires(a.NilError(sink.Close(context.Background())))
	var got map[string]any
	assert.Requires(a.NilError(json.Unmarshal(output.Bytes(), &got)))
	assert.Requires(a.Number(got["template_id"].(float64)).EqualTo(float64(templateID)))
	assert.Requires(a.String(got["model_id"].(string)).EqualTo("model"))
	_, hasSourceKind := got["source_kind"]
	_, hasSourceName := got["source_name"]
	_, hasParameters := got["parameters"]
	assert.Requires(a.False(hasSourceKind))
	assert.Requires(a.False(hasSourceName))
	assert.Requires(a.False(hasParameters))
}

func TestPartJSONLWriterRotatesByBatchSizeAndAge(t *testing.T) {
	assert := a.New(t)
	dir := t.TempDir()
	now := time.Date(2026, 6, 13, 12, 0, 0, 0, time.UTC)
	sink, err := NewSink(context.Background(), nil, SinkOptions{Format: FormatJSONL, Prefix: dir, BatchSize: 2, BatchMaxAge: time.Minute, RunID: "run", Now: func() time.Time { return now }})
	assert.Requires(a.NilError(err))

	assert.Requires(a.NilError(sink.Write(context.Background(), Output{ModelID: "model", Variables: []string{"one"}})))
	assert.Requires(a.NilError(sink.Write(context.Background(), Output{ModelID: "model", Variables: []string{"two"}})))
	now = now.Add(2 * time.Minute)
	assert.Requires(a.NilError(sink.Write(context.Background(), Output{ModelID: "model", Variables: []string{"three"}})))
	assert.Requires(a.NilError(sink.Close(context.Background())))

	first, err := os.ReadFile(filepath.Join(dir, "format=jsonl", "run_id=run", "part-00000.jsonl"))
	assert.Requires(a.NilError(err))
	second, err := os.ReadFile(filepath.Join(dir, "format=jsonl", "run_id=run", "part-00001.jsonl"))
	assert.Requires(a.NilError(err))
	assert.Requires(a.String(string(first)).Contains("one"))
	assert.Requires(a.String(string(first)).Contains("two"))
	assert.Requires(a.String(string(second)).Contains("three"))
}
