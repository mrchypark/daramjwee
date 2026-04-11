package main

import (
	"context"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

type simpleFetcher struct {
	label string
	data  string
	tag   string
}

func (f simpleFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	oldTag := "none"
	if oldMetadata != nil && oldMetadata.CacheTag != "" {
		oldTag = oldMetadata.CacheTag
	}
	fmt.Printf("[%s origin] fetch (old=%s)\n", f.label, oldTag)
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(f.data)),
		Metadata: &daramjwee.Metadata{CacheTag: f.tag},
	}, nil
}

func main() {
	ctx := context.Background()
	logger := log.NewLogfmtLogger(log.NewSyncWriter(os.Stderr))

	baseDir, err := os.MkdirTemp("", "daramjwee-cache-group-example-")
	if err != nil {
		fmt.Printf("failed to create temp dir: %v\n", err)
		os.Exit(1)
	}
	defer os.RemoveAll(baseDir)

	userTier0 := memstore.New(512*1024, policy.NewLRU())
	userTier1, err := filestore.New(baseDir+"/users", logger)
	if err != nil {
		fmt.Printf("failed to create user file tier: %v\n", err)
		os.Exit(1)
	}

	reportTier0, err := filestore.New(baseDir+"/reports", logger)
	if err != nil {
		fmt.Printf("failed to create report file tier: %v\n", err)
		os.Exit(1)
	}

	group, err := daramjwee.NewGroup(
		logger,
		daramjwee.WithGroupWorkers(2),
		daramjwee.WithGroupWorkerQueueDefault(8),
		daramjwee.WithGroupWorkerTimeout(5*time.Second),
		daramjwee.WithGroupCloseTimeout(5*time.Second),
	)
	if err != nil {
		fmt.Printf("failed to create cache group: %v\n", err)
		os.Exit(1)
	}
	defer group.Close()

	userCache, err := group.NewCache(
		"users",
		daramjwee.WithTiers(userTier0, userTier1),
		daramjwee.WithFreshness(time.Minute, 10*time.Second),
		daramjwee.WithWeight(4),
		daramjwee.WithQueueLimit(8),
	)
	if err != nil {
		fmt.Printf("failed to create user cache: %v\n", err)
		os.Exit(1)
	}
	defer userCache.Close()

	reportCache, err := group.NewCache(
		"reports",
		daramjwee.WithTiers(reportTier0),
		daramjwee.WithFreshness(2*time.Minute, 30*time.Second),
		daramjwee.WithWeight(1),
	)
	if err != nil {
		fmt.Printf("failed to create report cache: %v\n", err)
		os.Exit(1)
	}
	defer reportCache.Close()

	userFetcher := simpleFetcher{
		label: "users",
		data:  `{"id":1,"name":"Ada Lovelace"}`,
		tag:   "user-v1",
	}
	reportFetcher := simpleFetcher{
		label: "reports",
		data:  "daily-report-v1",
		tag:   "report-v1",
	}

	fmt.Println("=== CacheGroup Demo ===")
	fmt.Println("shared runtime: workers=2 queue_default=8")
	fmt.Println("user cache: mem -> file, weight=4, queue=8")
	fmt.Println("report cache: file only, weight=1, queue=default")

	userResp, err := userCache.Get(ctx, "user:1", daramjwee.GetRequest{}, userFetcher)
	if err != nil {
		fmt.Printf("user cache get failed: %v\n", err)
		os.Exit(1)
	}
	userBody, err := io.ReadAll(userResp)
	_ = userResp.Close()
	if err != nil {
		fmt.Printf("user cache read failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("user payload: %s\n", string(userBody))

	reportResp, err := reportCache.Get(ctx, "report:daily", daramjwee.GetRequest{}, reportFetcher)
	if err != nil {
		fmt.Printf("report cache get failed: %v\n", err)
		os.Exit(1)
	}
	reportBody, err := io.ReadAll(reportResp)
	_ = reportResp.Close()
	if err != nil {
		fmt.Printf("report cache read failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("report payload: %s\n", string(reportBody))

	if err := userCache.ScheduleRefresh(ctx, "user:1", userFetcher); err != nil {
		fmt.Printf("user refresh schedule failed: %v\n", err)
		os.Exit(1)
	}
	if err := reportCache.ScheduleRefresh(ctx, "report:daily", reportFetcher); err != nil {
		fmt.Printf("report refresh schedule failed: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("scheduled background refresh on both caches")

	time.Sleep(200 * time.Millisecond)
	fmt.Println("cache group example complete")
}
