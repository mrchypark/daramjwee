package objectstoredemo

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	remotestore "github.com/mrchypark/daramjwee/pkg/store/objectstore"
	objstore "github.com/thanos-io/objstore"
)

type Observation struct {
	LocalFiles    int
	RemoteObjects int
}

type ScenarioSummary struct {
	Tier0Dir       string
	Tier1Workspace string
	RemoteLabel    string
	Prefix         string
	AfterFirstGet  Observation
	AfterTier0Wipe Observation
	AfterPromotion Observation
}

func (s ScenarioSummary) Verify() error {
	switch {
	case s.AfterFirstGet.LocalFiles <= 0:
		return errors.New("tier 0 was not populated after the first get")
	case s.AfterFirstGet.RemoteObjects <= 0:
		return errors.New("tier 1 did not persist any remote objects after the first get")
	case s.AfterTier0Wipe.LocalFiles != 0:
		return errors.New("tier 0 still had local files after the wipe step")
	case s.AfterTier0Wipe.RemoteObjects <= 0:
		return errors.New("tier 1 remote objects disappeared after the tier-0 wipe")
	case s.AfterPromotion.LocalFiles <= 0:
		return errors.New("tier 0 was not repopulated after the lower-tier promotion")
	case s.AfterPromotion.RemoteObjects <= 0:
		return errors.New("tier 1 remote objects were missing after promotion")
	default:
		return nil
	}
}

type simpleFetcher struct {
	data []byte
}

func (f simpleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{CacheTag: "v1"},
	}, nil
}

// RunOrderedTierScenario runs the same ordered FileStore -> objectstore flow used
// by the existing examples, but lets callers supply the backing object bucket.
func RunOrderedTierScenario(ctx context.Context, logger log.Logger, bucket objstore.Bucket, prefix string, remoteLabel string) (ScenarioSummary, error) {
	summary := ScenarioSummary{
		RemoteLabel: remoteLabel,
		Prefix:      prefix,
	}

	tier1DataDir, err := os.MkdirTemp("", "daramjwee-objectstore-*")
	if err != nil {
		return summary, fmt.Errorf("create tier-1 workspace: %w", err)
	}
	defer os.RemoveAll(tier1DataDir)
	summary.Tier1Workspace = filepath.Join(tier1DataDir, "workspace")

	tier1Store := remotestore.New(
		bucket,
		log.With(logger, "tier", "1"),
		remotestore.WithDir(summary.Tier1Workspace),
		remotestore.WithPrefix(prefix),
		remotestore.WithPackThreshold(1<<20),
		remotestore.WithPageSize(256<<10),
		remotestore.WithBlockCache(64<<20),
		remotestore.WithCheckpointCache(16<<20),
		remotestore.WithCheckpointTTL(2*time.Second),
	)

	hotStoreDir, err := os.MkdirTemp("", "daramjwee-hot-cache-*")
	if err != nil {
		return summary, fmt.Errorf("create tier-0 workspace: %w", err)
	}
	defer os.RemoveAll(hotStoreDir)
	summary.Tier0Dir = hotStoreDir

	hotStore, err := filestore.New(hotStoreDir, log.With(logger, "tier", "0"))
	if err != nil {
		return summary, fmt.Errorf("create tier-0 filestore: %w", err)
	}

	cache, err := daramjwee.New(
		logger,
		daramjwee.WithTiers(hotStore, tier1Store),
		daramjwee.WithFreshness(10*time.Minute, time.Minute),
	)
	if err != nil {
		return summary, fmt.Errorf("create cache: %w", err)
	}
	defer func() {
		if cache != nil {
			cache.Close()
		}
	}()

	const cacheKey = "my-object"
	originData := []byte("Hello from Origin!")
	fetcher := simpleFetcher{data: originData}

	_ = logger.Log("level", "info", "msg", "Tier 1 objectstore initialized", "remote", remoteLabel, "prefix", prefix, "data_dir", summary.Tier1Workspace)
	_ = logger.Log("level", "info", "msg", "Tier 0 filestore initialized", "path", hotStoreDir)

	_ = logger.Log("level", "info", "msg", "Scenario 1: miss in both tiers, expect origin fetch")
	if err := getAndCompare(ctx, cache, cacheKey, fetcher, originData); err != nil {
		return summary, err
	}
	if summary.AfterFirstGet, err = observeState(ctx, bucket, prefix, hotStoreDir, 2); err != nil {
		return summary, err
	}

	_ = logger.Log("level", "info", "msg", "Scenario 2: tier-0 hit")
	if err := getAndCompare(ctx, cache, cacheKey, fetcher, originData); err != nil {
		return summary, err
	}

	_ = logger.Log("level", "info", "msg", "Scenario 3: wipe tier 0 and expect lower-tier promotion", "path", hotStoreDir)
	cache.Close()
	cache = nil

	if err := os.RemoveAll(hotStoreDir); err != nil {
		return summary, fmt.Errorf("remove tier-0 workspace: %w", err)
	}
	if summary.AfterTier0Wipe, err = observeState(ctx, bucket, prefix, hotStoreDir, 2); err != nil {
		return summary, err
	}

	hotStore, err = filestore.New(hotStoreDir, log.With(logger, "tier", "0"))
	if err != nil {
		return summary, fmt.Errorf("recreate tier-0 filestore: %w", err)
	}

	cache, err = daramjwee.New(
		logger,
		daramjwee.WithTiers(hotStore, tier1Store),
		daramjwee.WithFreshness(10*time.Minute, time.Minute),
	)
	if err != nil {
		return summary, fmt.Errorf("recreate cache: %w", err)
	}

	if err := getAndCompare(ctx, cache, cacheKey, fetcher, originData); err != nil {
		return summary, err
	}
	if summary.AfterPromotion, err = observeState(ctx, bucket, prefix, hotStoreDir, 2); err != nil {
		return summary, err
	}
	if err := summary.Verify(); err != nil {
		return summary, err
	}

	_ = logger.Log(
		"level", "info",
		"msg", "All ordered-tier scenarios completed successfully",
		"remote", remoteLabel,
		"prefix", prefix,
		"after_first_get_local_files", summary.AfterFirstGet.LocalFiles,
		"after_first_get_remote_objects", summary.AfterFirstGet.RemoteObjects,
		"after_wipe_local_files", summary.AfterTier0Wipe.LocalFiles,
		"after_wipe_remote_objects", summary.AfterTier0Wipe.RemoteObjects,
		"after_promotion_local_files", summary.AfterPromotion.LocalFiles,
		"after_promotion_remote_objects", summary.AfterPromotion.RemoteObjects,
	)
	return summary, nil
}

func getAndCompare(ctx context.Context, cache daramjwee.Cache, key string, fetcher daramjwee.Fetcher, expectedData []byte) error {
	rc, err := cache.Get(ctx, key, daramjwee.GetRequest{}, fetcher)
	if err != nil {
		return fmt.Errorf("cache get %q: %w", key, err)
	}
	defer rc.Close()

	readData, err := io.ReadAll(rc)
	if err != nil {
		return fmt.Errorf("read cache stream %q: %w", key, err)
	}

	if !bytes.Equal(readData, expectedData) {
		return fmt.Errorf("data mismatch for %q: expected %q got %q", key, string(expectedData), string(readData))
	}

	return nil
}

func SanitizePrefix(name string) string {
	return strings.ReplaceAll(name, " ", "-")
}

func observeState(ctx context.Context, bucket objstore.Bucket, prefix string, localDir string, minRemoteObjects int) (Observation, error) {
	remoteCount, err := waitForRemoteObjects(ctx, bucket, prefix, minRemoteObjects)
	if err != nil {
		return Observation{}, err
	}
	localCount, err := countLocalFiles(localDir)
	if err != nil {
		return Observation{}, err
	}
	return Observation{
		LocalFiles:    localCount,
		RemoteObjects: remoteCount,
	}, nil
}

func waitForRemoteObjects(ctx context.Context, bucket objstore.Bucket, prefix string, minRemoteObjects int) (int, error) {
	observeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()

	var (
		lastCount int
		lastErr   error
	)
	for {
		lastCount, lastErr = countRemoteObjects(observeCtx, bucket, prefix)
		if lastErr == nil && lastCount >= minRemoteObjects {
			return lastCount, nil
		}

		select {
		case <-observeCtx.Done():
			if lastErr != nil {
				return 0, fmt.Errorf("observe remote objects: %w", lastErr)
			}
			return lastCount, fmt.Errorf("observe remote objects: timed out waiting for at least %d objects under %q; last count=%d", minRemoteObjects, prefix, lastCount)
		case <-time.After(100 * time.Millisecond):
		}
	}
}

func countRemoteObjects(ctx context.Context, bucket objstore.Bucket, prefix string) (int, error) {
	count := 0
	err := bucket.Iter(ctx, prefix, func(name string) error {
		count++
		return nil
	}, objstore.WithRecursiveIter())
	if err != nil {
		return 0, err
	}
	return count, nil
}

func countLocalFiles(root string) (int, error) {
	if _, err := os.Stat(root); err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, nil
		}
		return 0, err
	}

	count := 0
	err := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		count++
		return nil
	})
	if err != nil {
		return 0, err
	}
	return count, nil
}
