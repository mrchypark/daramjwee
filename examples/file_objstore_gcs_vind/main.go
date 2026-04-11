package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"reflect"
	"time"
	"unsafe"

	"cloud.google.com/go/storage"
	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee/examples/internal/objectstoredemo"
	"github.com/thanos-io/objstore/providers/gcs"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
)

type exampleConfig struct {
	bucket       string
	emulatorHost string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller, "example", "file_objstore_gcs_vind")

	cfg, err := loadExampleConfig(os.Getenv)
	if err != nil {
		fatal(logger, "load example config", err)
	}

	restoreEnv := setEmulatorEnv(cfg.emulatorHost)
	defer restoreEnv()

	if err := ensureBucketReady(ctx, cfg); err != nil {
		fatal(logger, "prepare gcsemulator bucket", err)
	}

	bucketCfg, err := buildBucketConfig(cfg)
	if err != nil {
		fatal(logger, "build GCS objectstore config", err)
	}
	bucket, err := gcs.NewBucketWithConfig(ctx, logger, bucketCfg, "daramjwee", func(rt http.RoundTripper) http.RoundTripper {
		return &loggingRoundTripper{next: rt}
	})
	if err != nil {
		fatal(logger, "create GCS objectstore bucket", err)
	}
	defer bucket.Close()

	prefix := fmt.Sprintf("examples/%s/%d", objectstoredemo.SanitizePrefix("file-objstore-gcs-vind"), time.Now().UnixNano())
	summary, err := objectstoredemo.RunOrderedTierScenario(ctx, logger, bucket, prefix, "gcsemulator")
	if err != nil {
		fatal(logger, "run ordered-tier scenario", err)
	}
	_ = logger.Log("level", "info", "msg", "verification summary", "tier0_dir", summary.Tier0Dir, "tier1_workspace", summary.Tier1Workspace)
}

func loadExampleConfig(getenv func(string) string) (exampleConfig, error) {
	cfg := exampleConfig{
		bucket:       getenvDefault(getenv, "DARAMJWEE_GCS_BUCKET", "daramjwee-gcs-local"),
		emulatorHost: getenvDefault(getenv, "DARAMJWEE_GCS_EMULATOR_HOST", "127.0.0.1:4443"),
	}
	if cfg.bucket == "" {
		return exampleConfig{}, errors.New("DARAMJWEE_GCS_BUCKET must not be empty")
	}
	if cfg.emulatorHost == "" {
		return exampleConfig{}, errors.New("DARAMJWEE_GCS_EMULATOR_HOST must not be empty")
	}
	return cfg, nil
}

func buildBucketConfig(cfg exampleConfig) (gcs.Config, error) {
	conf := gcs.Config{
		Bucket:  cfg.bucket,
		UseGRPC: false,
	}
	// The Thanos GCS provider keeps emulator anonymous mode behind a private field.
	// This example opts into that mode explicitly so fake-gcs-server can run without ADC.
	if err := setPrivateBool(&conf, "noAuth", true); err != nil {
		return gcs.Config{}, err
	}
	return conf, nil
}

func ensureBucketReady(ctx context.Context, cfg exampleConfig) error {
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := ensureBucket(ctx, cfg.bucket); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for fake-gcs-server bucket readiness: %w", lastErr)
		case <-time.After(time.Second):
		}
	}
}

func ensureBucket(ctx context.Context, bucketName string) error {
	client, err := storage.NewClient(ctx, option.WithoutAuthentication())
	if err != nil {
		return err
	}
	defer client.Close()

	handle := client.Bucket(bucketName)
	if _, err := handle.Attrs(ctx); err == nil {
		return nil
	} else {
		var gErr *googleapi.Error
		if !errors.As(err, &gErr) || gErr.Code != http.StatusNotFound {
			return err
		}
	}

	if err := handle.Create(ctx, "daramjwee-local", nil); err != nil {
		var gErr *googleapi.Error
		if errors.As(err, &gErr) && gErr.Code == http.StatusConflict {
			return nil
		}
		return err
	}

	return nil
}

func setEmulatorEnv(host string) func() {
	prevStorage, hadStorage := os.LookupEnv("STORAGE_EMULATOR_HOST")
	prevGCS, hadGCS := os.LookupEnv("GCS_EMULATOR_HOST")
	_ = os.Setenv("STORAGE_EMULATOR_HOST", host)
	_ = os.Setenv("GCS_EMULATOR_HOST", host)

	return func() {
		restoreEnv("STORAGE_EMULATOR_HOST", prevStorage, hadStorage)
		restoreEnv("GCS_EMULATOR_HOST", prevGCS, hadGCS)
	}
}

func restoreEnv(key string, value string, hadValue bool) {
	if hadValue {
		_ = os.Setenv(key, value)
		return
	}
	_ = os.Unsetenv(key)
}

func setPrivateBool(target interface{}, fieldName string, value bool) error {
	field := reflect.ValueOf(target).Elem().FieldByName(fieldName)
	if !field.IsValid() {
		return fmt.Errorf("field %q not found on %T", fieldName, target)
	}
	if field.Kind() != reflect.Bool {
		return fmt.Errorf("field %q on %T is %s, want bool", fieldName, target, field.Kind())
	}
	reflect.NewAt(field.Type(), unsafe.Pointer(field.UnsafeAddr())).Elem().SetBool(value)
	return nil
}

func getenvDefault(getenv func(string) string, key string, fallback string) string {
	if value := getenv(key); value != "" {
		return value
	}
	return fallback
}

func fatal(logger log.Logger, msg string, err error) {
	_ = logger.Log("level", "error", "msg", msg, "err", err)
	os.Exit(1)
}

type loggingRoundTripper struct {
	next http.RoundTripper
}

func (l *loggingRoundTripper) RoundTrip(req *http.Request) (*http.Response, error) {
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller, "transport", "gcs")
	_ = logger.Log("level", "debug", "msg", "request start", "method", req.Method, "url", req.URL.String())
	start := time.Now()
	resp, err := l.next.RoundTrip(req)
	if err != nil {
		_ = logger.Log("level", "error", "msg", "request failed", "err", err, "duration", time.Since(start))
		return nil, err
	}
	_ = logger.Log("level", "debug", "msg", "request complete", "status", resp.Status, "duration", time.Since(start))
	return resp, nil
}
