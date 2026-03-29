package adapter

import (
	"context"
	"fmt"
	"io"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/goccy/go-json"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/objectstore"
	"github.com/thanos-io/objstore"
)

// NewObjstoreAdapter is a deprecated compatibility shim for pkg/store/objectstore.New.
func NewObjstoreAdapter(bucket objstore.Bucket, logger log.Logger, opts ...objectstore.Option) daramjwee.Store {
	if logger == nil {
		logger = log.NewNopLogger()
	}
	return &objstoreAdapter{
		modern: objectstore.New(bucket, logger, opts...),
		bucket: bucket,
		logger: logger,
	}
}

type objstoreAdapter struct {
	modern *objectstore.Store
	bucket objstore.Bucket
	logger log.Logger
}

func (a *objstoreAdapter) GetStreamUsesContext() bool { return true }

func (a *objstoreAdapter) GetStream(ctx context.Context, key string) (io.ReadCloser, *daramjwee.Metadata, error) {
	stream, meta, err := a.modern.GetStream(ctx, key)
	if err == nil || !isNotFound(err) {
		return stream, meta, err
	}

	meta, err = a.legacyStat(ctx, key)
	if err != nil {
		return nil, nil, err
	}

	reader, err := a.bucket.Get(ctx, key)
	if err != nil {
		if a.bucket.IsObjNotFoundErr(err) {
			return nil, nil, daramjwee.ErrNotFound
		}
		level.Warn(a.logger).Log("msg", "failed to get legacy objstore data", "key", key, "err", err)
		return nil, nil, fmt.Errorf("failed to get legacy object for key %q: %w", key, err)
	}
	return reader, meta, nil
}

func (a *objstoreAdapter) BeginSet(ctx context.Context, key string, metadata *daramjwee.Metadata) (daramjwee.WriteSink, error) {
	return a.modern.BeginSet(ctx, key, metadata)
}

func (a *objstoreAdapter) Delete(ctx context.Context, key string) error {
	legacyExists, err := a.hasLegacyMetadataObject(ctx, key)
	if err != nil {
		return err
	}
	if err := a.modern.Delete(ctx, key); err != nil {
		return err
	}
	if !legacyExists {
		if a.modern.OwnsObjectPath(key) {
			return nil
		}
		err := a.bucket.Delete(ctx, key)
		if err != nil && !a.bucket.IsObjNotFoundErr(err) {
			return err
		}
		return nil
	}
	for _, name := range []string{key, legacyMetaPath(key)} {
		err := a.bucket.Delete(ctx, name)
		if err != nil && !a.bucket.IsObjNotFoundErr(err) {
			return err
		}
	}
	return nil
}

func (a *objstoreAdapter) Stat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	meta, err := a.modern.Stat(ctx, key)
	if err == nil || !isNotFound(err) {
		return meta, err
	}
	return a.legacyStat(ctx, key)
}

func (a *objstoreAdapter) legacyStat(ctx context.Context, key string) (*daramjwee.Metadata, error) {
	reader, err := a.bucket.Get(ctx, legacyMetaPath(key))
	if err != nil {
		if a.bucket.IsObjNotFoundErr(err) {
			return nil, daramjwee.ErrNotFound
		}
		return nil, fmt.Errorf("failed to get legacy metadata for key %q: %w", key, err)
	}
	defer func() {
		if closeErr := reader.Close(); closeErr != nil {
			level.Warn(a.logger).Log("msg", "failed to close legacy metadata reader", "key", key, "err", closeErr)
		}
	}()

	data, err := io.ReadAll(reader)
	if err != nil {
		return nil, fmt.Errorf("failed to read legacy metadata for key %q: %w", key, err)
	}

	var meta daramjwee.Metadata
	if err := json.Unmarshal(data, &meta); err != nil {
		return nil, fmt.Errorf("failed to decode legacy metadata for key %q: %w", key, err)
	}
	return &meta, nil
}

func (a *objstoreAdapter) hasLegacyMetadataObject(ctx context.Context, key string) (bool, error) {
	exists, err := a.bucket.Exists(ctx, legacyMetaPath(key))
	if err != nil {
		return false, fmt.Errorf("failed to check legacy metadata for key %q: %w", key, err)
	}
	return exists, nil
}

func legacyMetaPath(key string) string {
	return key + ".meta.json"
}

func isNotFound(err error) bool {
	return err == daramjwee.ErrNotFound
}
