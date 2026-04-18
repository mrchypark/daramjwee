package main

import (
	"testing"
)

func TestLoadExampleConfigDefaults(t *testing.T) {
	cfg, err := loadExampleConfig(func(string) string { return "" })
	if err != nil {
		t.Fatalf("loadExampleConfig returned error: %v", err)
	}

	if cfg.bucket != "daramjwee-gcs-local" {
		t.Fatalf("expected default bucket, got %q", cfg.bucket)
	}
	if cfg.emulatorHost != "127.0.0.1:4443" {
		t.Fatalf("expected default emulator host, got %q", cfg.emulatorHost)
	}
}

func TestBuildBucketConfig(t *testing.T) {
	cfg := exampleConfig{
		bucket:       "bucket-a",
		emulatorHost: "127.0.0.1:5555",
	}

	conf, err := buildBucketConfig(cfg)
	if err != nil {
		t.Fatalf("buildBucketConfig returned error: %v", err)
	}
	if conf.Bucket != "bucket-a" {
		t.Fatalf("expected bucket in config, got %q", conf.Bucket)
	}
	if conf.UseGRPC {
		t.Fatalf("expected HTTP client mode for fake-gcs-server")
	}
}

type privateBoolTarget struct {
	hidden bool
	text   string
}

func TestSetPrivateBoolSetsNamedBoolField(t *testing.T) {
	target := privateBoolTarget{}

	if err := setPrivateBool(&target, "hidden", true); err != nil {
		t.Fatalf("setPrivateBool returned error: %v", err)
	}
	if !target.hidden {
		t.Fatalf("expected hidden field to be updated")
	}
}

func TestSetPrivateBoolReturnsErrorForMissingField(t *testing.T) {
	target := privateBoolTarget{}

	if err := setPrivateBool(&target, "missing", true); err == nil {
		t.Fatalf("expected missing field to return an error")
	}
}

func TestSetPrivateBoolReturnsErrorForNonBoolField(t *testing.T) {
	target := privateBoolTarget{}

	if err := setPrivateBool(&target, "text", true); err == nil {
		t.Fatalf("expected non-bool field to return an error")
	}
}
