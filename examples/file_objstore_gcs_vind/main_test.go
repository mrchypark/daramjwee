package main

import (
	"reflect"
	"testing"
	"unsafe"

	"github.com/thanos-io/objstore/providers/gcs"
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
	if !readNoAuthField(conf) {
		t.Fatalf("expected private noAuth field to be enabled for emulator mode")
	}
}

func readNoAuthField(conf gcs.Config) bool {
	value := reflect.ValueOf(&conf).Elem()
	field := value.FieldByName("noAuth")
	return reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().Bool()
}

func TestBuildBucketConfigLeavesNoAuthReadable(t *testing.T) {
	conf, err := buildBucketConfig(exampleConfig{bucket: "bucket-b", emulatorHost: "127.0.0.1:4443"})
	if err != nil {
		t.Fatalf("buildBucketConfig returned error: %v", err)
	}
	if !readNoAuthField(conf) {
		t.Fatalf("expected noAuth to remain enabled")
	}
}
