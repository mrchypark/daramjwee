// Package main demonstrates HTTP server integration examples with daramjwee cache
// This file contains practical examples of integrating HTTP servers with daramjwee cache
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/lock"
	"github.com/mrchypark/daramjwee/pkg/policy"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
	"github.com/mrchypark/daramjwee/pkg/store/memstore"
)

// HTTPFetcher is a Fetcher implementation that retrieves data through HTTP requests
type HTTPFetcher struct {
	url    string
	client *http.Client
}

func NewHTTPFetcher(url string) *HTTPFetcher {
	return &HTTPFetcher{
		url:    url,
		client: &http.Client{Timeout: 10 * time.Second},
	}
}

func (f *HTTPFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", f.url, nil)
	if err != nil {
		return nil, err
	}

	// ETag-based conditional request
	if oldMetadata != nil && oldMetadata.ETag != "" {
		req.Header.Set("If-None-Match", oldMetadata.ETag)
	}

	resp, err := f.client.Do(req)
	if err != nil {
		return nil, err
	}

	// Handle 304 Not Modified
	if resp.StatusCode == http.StatusNotModified {
		resp.Body.Close()
		return nil, daramjwee.ErrNotModified
	}

	if resp.StatusCode == http.StatusNotFound {
		resp.Body.Close()
		return nil, daramjwee.ErrCacheableNotFound
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("HTTP error: %d", resp.StatusCode)
	}

	metadata := &daramjwee.Metadata{
		ETag:     resp.Header.Get("ETag"),
		CachedAt: time.Now(),
	}

	return &daramjwee.FetchResult{
		Body:     resp.Body,
		Metadata: metadata,
	}, nil
}

// DatabaseFetcher is a Fetcher that simulates database queries
type DatabaseFetcher struct {
	query string
	delay time.Duration // Database delay simulation
}

func NewDatabaseFetcher(query string, delay time.Duration) *DatabaseFetcher {
	return &DatabaseFetcher{query: query, delay: delay}
}

func (f *DatabaseFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	// Simulate database query delay
	time.Sleep(f.delay)

	// Generate simple JSON response
	data := map[string]interface{}{
		"query":     f.query,
		"timestamp": time.Now().Unix(),
		"results":   []string{"item1", "item2", "item3"},
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return nil, err
	}

	metadata := &daramjwee.Metadata{
		ETag:     fmt.Sprintf("db-%d", time.Now().Unix()),
		CachedAt: time.Now(),
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(string(jsonData))),
		Metadata: metadata,
	}, nil
}

// StaticFileFetcher is a Fetcher that simulates static files
type StaticFileFetcher struct {
	filename string
}

func (f *StaticFileFetcher) Fetch(ctx context.Context, oldMetadata *daramjwee.Metadata) (*daramjwee.FetchResult, error) {
	// Simulate static file content
	var content string
	switch {
	case strings.HasSuffix(f.filename, ".txt"):
		content = fmt.Sprintf("This is the content of %s file.\nGenerated at: %s", f.filename, time.Now().Format(time.RFC3339))
	case strings.HasSuffix(f.filename, ".json"):
		data := map[string]interface{}{
			"filename":  f.filename,
			"timestamp": time.Now().Unix(),
			"content":   "JSON file content",
		}
		jsonData, _ := json.Marshal(data)
		content = string(jsonData)
	default:
		content = fmt.Sprintf("File: %s\nContent: Binary data simulation", f.filename)
	}

	metadata := &daramjwee.Metadata{
		ETag:     fmt.Sprintf("static-%s-%d", f.filename, time.Now().Unix()),
		CachedAt: time.Now(),
	}

	return &daramjwee.FetchResult{
		Body:     io.NopCloser(strings.NewReader(content)),
		Metadata: metadata,
	}, nil
}

func main() {
	logger := log.NewLogfmtLogger(os.Stdout)
	logger = level.NewFilter(logger, level.AllowInfo())

	fmt.Println("=== HTTP Server Integration Examples ===")

	// 1. Web proxy server
	go webProxyServer(logger)
	time.Sleep(100 * time.Millisecond) // Wait for server to start

	// 2. API caching server
	go apiCachingServer(logger)
	time.Sleep(100 * time.Millisecond)

	// 3. Static file server
	go staticFileServer(logger)
	time.Sleep(100 * time.Millisecond)

	// 4. Database caching server
	go databaseCachingServer(logger)
	time.Sleep(100 * time.Millisecond)

	fmt.Println("\nAll servers have been started!")
	fmt.Println("- Web Proxy: http://localhost:8081")
	fmt.Println("- API Caching: http://localhost:8082")
	fmt.Println("- Static Files: http://localhost:8083")
	fmt.Println("- DB Caching: http://localhost:8084")
	fmt.Println("\nTest examples:")
	fmt.Println("curl http://localhost:8081/proxy?url=https://httpbin.org/json")
	fmt.Println("curl http://localhost:8082/api/users")
	fmt.Println("curl http://localhost:8083/static/example.txt")
	fmt.Println("curl http://localhost:8084/db/users")

	// Keep servers running
	select {}
}

// 1. Web proxy server - Proxy that caches external URLs
func webProxyServer(logger log.Logger) {
	fmt.Println("\n1. Starting web proxy server (port 8081)")

	// Proxy cache configuration
	hotStore := memstore.New(
		100*1024*1024, // 100MB
		policy.NewLRUPolicy(),
		memstore.WithLocker(lock.NewStripeLock(512)),
	)

	tmpDir, _ := os.MkdirTemp("", "proxy-cache-*")
	coldStore, _ := filestore.New(
		tmpDir,
		logger,
		1024*1024*1024, // 1GB
		policy.NewS3FIFOPolicy(1024*1024*1024, 0.1),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
		daramjwee.WithWorker("pool", 10, 1000, 30*time.Second),
		daramjwee.WithCache(5*time.Minute),
		daramjwee.WithNegativeCache(1*time.Minute),
	)
	if err != nil {
		fmt.Printf("프록시 캐시 생성 실패: %v\n", err)
		return
	}

	http.HandleFunc("/proxy", func(w http.ResponseWriter, r *http.Request) {
		targetURL := r.URL.Query().Get("url")
		if targetURL == "" {
			http.Error(w, "url 파라미터가 필요합니다", http.StatusBadRequest)
			return
		}

		fetcher := NewHTTPFetcher(targetURL)
		reader, err := cache.Get(r.Context(), targetURL, fetcher)
		if err != nil {
			http.Error(w, fmt.Sprintf("프록시 요청 실패: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "daramjwee-proxy")
		io.Copy(w, reader)
	})

	if err := http.ListenAndServe(":8081", nil); err != nil {
		fmt.Printf("웹 프록시 서버 시작 실패: %v\n", err)
	}
}

// 2. API 캐싱 서버 - API 응답을 캐싱
func apiCachingServer(logger log.Logger) {
	fmt.Println("2. API 캐싱 서버 시작 (포트 8082)")

	// API 캐시 설정
	hotStore := memstore.New(
		50*1024*1024, // 50MB
		policy.NewSievePolicy(),
		memstore.WithLocker(lock.NewStripeLock(256)),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithWorker("pool", 5, 500, 15*time.Second),
		daramjwee.WithCache(2*time.Minute),
		daramjwee.WithNegativeCache(30*time.Second),
	)
	if err != nil {
		fmt.Printf("API 캐시 생성 실패: %v\n", err)
		return
	}

	// 사용자 API 엔드포인트
	http.HandleFunc("/api/users", func(w http.ResponseWriter, r *http.Request) {
		fetcher := NewDatabaseFetcher("SELECT * FROM users", 500*time.Millisecond)
		reader, err := cache.Get(r.Context(), "api:users", fetcher)
		if err != nil {
			http.Error(w, fmt.Sprintf("API 요청 실패: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "daramjwee-api")
		io.Copy(w, reader)
	})

	// 특정 사용자 API 엔드포인트
	http.HandleFunc("/api/users/", func(w http.ResponseWriter, r *http.Request) {
		userID := strings.TrimPrefix(r.URL.Path, "/api/users/")
		if userID == "" {
			http.Error(w, "사용자 ID가 필요합니다", http.StatusBadRequest)
			return
		}

		fetcher := NewDatabaseFetcher(fmt.Sprintf("SELECT * FROM users WHERE id=%s", userID), 200*time.Millisecond)
		cacheKey := fmt.Sprintf("api:users:%s", userID)

		reader, err := cache.Get(r.Context(), cacheKey, fetcher)
		if err != nil {
			http.Error(w, fmt.Sprintf("사용자 조회 실패: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "daramjwee-api")
		io.Copy(w, reader)
	})

	if err := http.ListenAndServe(":8082", nil); err != nil {
		fmt.Printf("API 캐싱 서버 시작 실패: %v\n", err)
	}
}

// 3. 정적 파일 서버 - 정적 파일을 캐싱
func staticFileServer(logger log.Logger) {
	fmt.Println("3. 정적 파일 서버 시작 (포트 8083)")

	// 정적 파일용 캐시 설정
	tmpDir, _ := os.MkdirTemp("", "static-cache-*")
	fileStore, _ := filestore.New(
		tmpDir,
		logger,
		500*1024*1024, // 500MB
		policy.NewLRUPolicy(),
		filestore.WithHashedKeys(2, 2),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(fileStore),
		daramjwee.WithWorker("pool", 3, 200, 60*time.Second),
		daramjwee.WithCache(1*time.Hour), // 1시간 캐시
		daramjwee.WithNegativeCache(5*time.Minute),
	)
	if err != nil {
		fmt.Printf("정적 파일 캐시 생성 실패: %v\n", err)
		return
	}

	http.HandleFunc("/static/", func(w http.ResponseWriter, r *http.Request) {
		filename := strings.TrimPrefix(r.URL.Path, "/static/")
		if filename == "" {
			http.Error(w, "파일명이 필요합니다", http.StatusBadRequest)
			return
		}

		// 간단한 정적 파일 시뮬레이션
		fetcher := &StaticFileFetcher{filename: filename}
		reader, err := cache.Get(r.Context(), fmt.Sprintf("static:%s", filename), fetcher)
		if err != nil {
			http.Error(w, fmt.Sprintf("파일 조회 실패: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		// 파일 확장자에 따른 Content-Type 설정
		if strings.HasSuffix(filename, ".txt") {
			w.Header().Set("Content-Type", "text/plain")
		} else if strings.HasSuffix(filename, ".json") {
			w.Header().Set("Content-Type", "application/json")
		} else {
			w.Header().Set("Content-Type", "application/octet-stream")
		}

		w.Header().Set("X-Cache", "daramjwee-static")
		io.Copy(w, reader)
	})

	if err := http.ListenAndServe(":8083", nil); err != nil {
		fmt.Printf("정적 파일 서버 시작 실패: %v\n", err)
	}
}

// 4. 데이터베이스 캐싱 서버 - 복잡한 DB 쿼리 결과를 캐싱
func databaseCachingServer(logger log.Logger) {
	fmt.Println("4. 데이터베이스 캐싱 서버 시작 (포트 8084)")

	// DB 캐시용 하이브리드 설정
	hotStore := memstore.New(
		200*1024*1024, // 200MB 메모리
		policy.NewS3FIFOPolicy(200*1024*1024, 0.15),
		memstore.WithLocker(lock.NewStripeLock(1024)),
	)

	tmpDir, _ := os.MkdirTemp("", "db-cache-*")
	coldStore, _ := filestore.New(
		tmpDir,
		logger,
		2*1024*1024*1024, // 2GB 디스크
		policy.NewLRUPolicy(),
	)

	cache, err := daramjwee.New(logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithColdStore(coldStore),
		daramjwee.WithWorker("pool", 8, 1000, 45*time.Second),
		daramjwee.WithCache(10*time.Minute),
		daramjwee.WithNegativeCache(2*time.Minute),
	)
	if err != nil {
		fmt.Printf("DB 캐시 생성 실패: %v\n", err)
		return
	}

	// 사용자 목록 조회
	http.HandleFunc("/db/users", func(w http.ResponseWriter, r *http.Request) {
		page := r.URL.Query().Get("page")
		if page == "" {
			page = "1"
		}

		fetcher := NewDatabaseFetcher(fmt.Sprintf("SELECT * FROM users LIMIT 10 OFFSET %s", page), 1*time.Second)
		cacheKey := fmt.Sprintf("db:users:page:%s", page)

		reader, err := cache.Get(r.Context(), cacheKey, fetcher)
		if err != nil {
			http.Error(w, fmt.Sprintf("DB 조회 실패: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "daramjwee-db")
		io.Copy(w, reader)
	})

	// 복잡한 통계 쿼리
	http.HandleFunc("/db/stats", func(w http.ResponseWriter, r *http.Request) {
		statsType := r.URL.Query().Get("type")
		if statsType == "" {
			statsType = "daily"
		}

		// 복잡한 쿼리 시뮬레이션 (더 긴 지연)
		fetcher := NewDatabaseFetcher(
			fmt.Sprintf("SELECT COUNT(*), AVG(value) FROM analytics WHERE type='%s' GROUP BY date", statsType),
			3*time.Second, // 복잡한 쿼리는 더 오래 걸림
		)
		cacheKey := fmt.Sprintf("db:stats:%s", statsType)

		reader, err := cache.Get(r.Context(), cacheKey, fetcher)
		if err != nil {
			http.Error(w, fmt.Sprintf("통계 조회 실패: %v", err), http.StatusInternalServerError)
			return
		}
		defer reader.Close()

		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Cache", "daramjwee-db-stats")
		io.Copy(w, reader)
	})

	// 캐시 상태 조회 엔드포인트
	http.HandleFunc("/cache/status", func(w http.ResponseWriter, r *http.Request) {
		status := map[string]interface{}{
			"cache_type": "daramjwee-hybrid",
			"hot_tier":   "memory-200MB",
			"cold_tier":  "disk-2GB",
			"timestamp":  time.Now().Unix(),
		}

		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(status)
	})

	if err := http.ListenAndServe(":8084", nil); err != nil {
		fmt.Printf("DB 캐싱 서버 시작 실패: %v\n", err)
	}
}
