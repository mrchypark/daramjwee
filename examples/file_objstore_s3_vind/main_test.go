package main

import (
	"strings"
	"testing"
)

func TestLoadExampleConfigDefaults(t *testing.T) {
	cfg, err := loadExampleConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("loadExampleConfig returned error: %v", err)
	}

	if cfg.bucket != "daramjwee-s3-local" {
		t.Fatalf("expected default bucket, got %q", cfg.bucket)
	}
	if cfg.endpoint != "127.0.0.1:9000" {
		t.Fatalf("expected default endpoint, got %q", cfg.endpoint)
	}
	if cfg.accessKey != "rustfsadmin" {
		t.Fatalf("expected default access key, got %q", cfg.accessKey)
	}
	if cfg.secretKey != "ChangeMe123!" {
		t.Fatalf("expected default secret key, got %q", cfg.secretKey)
	}
}

func TestBuildBucketConfig(t *testing.T) {
	cfg := exampleConfig{
		bucket:    "bucket-a",
		endpoint:  "127.0.0.1:9999",
		region:    "ap-northeast-2",
		accessKey: "key-a",
		secretKey: "secret-a",
	}

	conf := string(buildBucketConfig(cfg))
	for _, want := range []string{
		"bucket: bucket-a",
		"endpoint: 127.0.0.1:9999",
		"access_key: key-a",
		"secret_key: secret-a",
		"bucket_lookup_type: path",
		"insecure: true",
	} {
		if !strings.Contains(conf, want) {
			t.Fatalf("expected config to contain %q, got %q", want, conf)
		}
	}
}
