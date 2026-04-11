package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"os"
	"time"

	"github.com/go-kit/log"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/mrchypark/daramjwee/examples/internal/objectstoredemo"
	"github.com/thanos-io/objstore/providers/s3"
)

type exampleConfig struct {
	bucket    string
	endpoint  string
	region    string
	accessKey string
	secretKey string
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()

	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller, "example", "file_objstore_s3_vind")

	cfg, err := loadExampleConfig(os.Getenv)
	if err != nil {
		fatal(logger, "load example config", err)
	}

	if err := ensureBucketReady(ctx, cfg); err != nil {
		fatal(logger, "prepare RustFS bucket", err)
	}

	bucket, err := s3.NewBucket(logger, buildBucketConfig(cfg), "daramjwee", func(rt http.RoundTripper) http.RoundTripper {
		return &loggingRoundTripper{next: rt}
	})
	if err != nil {
		fatal(logger, "create S3 objectstore bucket", err)
	}
	defer bucket.Close()

	prefix := fmt.Sprintf("examples/%s/%d", objectstoredemo.SanitizePrefix("file-objstore-s3-vind"), time.Now().UnixNano())
	summary, err := objectstoredemo.RunOrderedTierScenario(ctx, logger, bucket, prefix, "rustfs")
	if err != nil {
		fatal(logger, "run ordered-tier scenario", err)
	}
	_ = logger.Log("level", "info", "msg", "verification summary", "tier0_dir", summary.Tier0Dir, "tier1_workspace", summary.Tier1Workspace)
}

func loadExampleConfig(getenv func(string) string) (exampleConfig, error) {
	cfg := exampleConfig{
		bucket:    getenvDefault(getenv, "DARAMJWEE_S3_BUCKET", "daramjwee-s3-local"),
		endpoint:  getenvDefault(getenv, "DARAMJWEE_S3_ENDPOINT", "127.0.0.1:9000"),
		region:    getenvDefault(getenv, "DARAMJWEE_S3_REGION", "us-east-1"),
		accessKey: getenvDefault(getenv, "DARAMJWEE_S3_ACCESS_KEY", "rustfsadmin"),
		secretKey: getenvDefault(getenv, "DARAMJWEE_S3_SECRET_KEY", "ChangeMe123!"),
	}

	if cfg.bucket == "" || cfg.endpoint == "" || cfg.region == "" || cfg.accessKey == "" || cfg.secretKey == "" {
		return exampleConfig{}, errors.New("S3 example config values must not be empty")
	}

	return cfg, nil
}

func buildBucketConfig(cfg exampleConfig) []byte {
	return []byte(fmt.Sprintf(
		"bucket: %s\nendpoint: %s\nregion: %s\naccess_key: %s\nsecret_key: %s\ninsecure: true\nbucket_lookup_type: path\nsend_content_md5: false\n",
		cfg.bucket,
		cfg.endpoint,
		cfg.region,
		cfg.accessKey,
		cfg.secretKey,
	))
}

func ensureBucketReady(ctx context.Context, cfg exampleConfig) error {
	var lastErr error
	for attempt := 1; ; attempt++ {
		if err := ensureBucket(ctx, cfg); err == nil {
			return nil
		} else {
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("timed out waiting for RustFS bucket readiness: %w", lastErr)
		case <-time.After(time.Second):
		}
	}
}

func ensureBucket(ctx context.Context, cfg exampleConfig) error {
	client, err := minio.New(cfg.endpoint, &minio.Options{
		Creds:  credentials.NewStaticV4(cfg.accessKey, cfg.secretKey, ""),
		Secure: false,
		Region: cfg.region,
	})
	if err != nil {
		return err
	}

	exists, err := client.BucketExists(ctx, cfg.bucket)
	if err != nil {
		return err
	}
	if exists {
		return nil
	}

	if err := client.MakeBucket(ctx, cfg.bucket, minio.MakeBucketOptions{Region: cfg.region}); err != nil {
		exists, existsErr := client.BucketExists(ctx, cfg.bucket)
		if existsErr == nil && exists {
			return nil
		}
		return err
	}

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
	logger = log.With(logger, "ts", log.DefaultTimestampUTC, "caller", log.DefaultCaller, "transport", "s3")
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
