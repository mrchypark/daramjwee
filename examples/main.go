package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/mrchypark/daramjwee"
	"github.com/mrchypark/daramjwee/pkg/store/filestore"
)

// --- 1. 가상 원본(Origin) 서버 시뮬레이션 ---
var fakeOrigin = map[string]struct {
	data string
	etag string
}{
	"hello": {"Hello, Daramjwee! This is the first object.", "v1"},
	"world": {"World is beautiful. This is the second object.", "v2"},
}

// --- 2. 커스텀 Fetcher 구현 ---

// originFetcher는 fakeOrigin과 통신하는 Fetcher입니다.
type originFetcher struct {
	key string
}

func (f *originFetcher) Fetch(ctx context.Context, oldETag string) (*daramjwee.FetchResult, error) {
	fmt.Printf("[Origin] Fetching key: %s, (old ETag: '%s')\n", f.key, oldETag)
	time.Sleep(500 * time.Millisecond) // 원본과의 통신 지연 시뮬레이션

	obj, ok := fakeOrigin[f.key]
	if !ok {
		return nil, daramjwee.ErrNotFound
	}

	// ETag가 동일하면, 데이터 변경이 없음을 알립니다.
	if oldETag != "" && oldETag == obj.etag {
		return nil, daramjwee.ErrNotModified
	}

	// 새로운 데이터와 메타데이터를 반환합니다.
	return &daramjwee.FetchResult{
		Body:     io.NopCloser(bytes.NewReader([]byte(obj.data))),
		Metadata: &daramjwee.Metadata{ETag: obj.etag},
	}, nil
}

func main() {
	// --- 로거 설정 ---
	logger := log.NewLogfmtLogger(os.Stderr)
	logger = level.NewFilter(logger, level.AllowDebug())

	// --- 3. daramjwee 캐시 설정 ---

	// Hot Tier: 현재 디렉토리에 filestore 생성
	hotStoreDir := "./daramjwee-hot-store"

	// Create the directory if it doesn't exist
	if err := os.MkdirAll(hotStoreDir, 0755); err != nil {
		panic(err)
	}

	// 변경: filestore.New 호출 시그니처 변경에 맞게 수정
	// 기본값(rename 사용)으로 생성하려면 추가 옵션 없이 호출하면 됩니다.
	// copy 방식을 사용하려면 filestore.WithCopyAndTruncate()를 추가합니다.
	hotStore, err := filestore.New(hotStoreDir, log.With(logger, "tier", "hot"))
	if err != nil {
		panic(err)
	}

	// daramjwee 인스턴스 생성
	cache, err := daramjwee.New(
		logger,
		daramjwee.WithHotStore(hotStore),
		daramjwee.WithDefaultTimeout(5*time.Second),
	)
	if err != nil {
		panic(err)
	}
	defer cache.Close()

	// --- 4. 웹 서버 실행 ---

	http.HandleFunc("/objects/", func(w http.ResponseWriter, r *http.Request) {
		key := strings.TrimPrefix(r.URL.Path, "/objects/")
		if key == "" {
			http.Error(w, "Key is required", http.StatusBadRequest)
			return
		}

		fmt.Printf("\n--- Handling request for key: %s ---\n", key)

		// Get 메서드를 호출하여 캐시/원본으로부터 데이터 스트림을 받습니다.
		stream, err := cache.Get(r.Context(), key, &originFetcher{key: key})
		if err != nil {
			if err == daramjwee.ErrNotFound {
				fmt.Println("[Handler] Object not found.")
				http.Error(w, "Object Not Found", http.StatusNotFound)
			} else {
				fmt.Printf("[Handler] Error: %v\n", err)
				http.Error(w, err.Error(), http.StatusInternalServerError)
			}
			return
		}
		defer stream.Close()

		// 받은 스트림을 사용자에게 그대로 전달합니다.
		fmt.Println("[Handler] Streaming response to client...")
		w.Header().Set("Content-Type", "text/plain")
		io.Copy(w, stream)
		fmt.Println("[Handler] Done.")
	})

	fmt.Println("daramjwee example server is running on :8080")
	fmt.Println("Try visiting:")
	fmt.Println("  http://localhost:8080/objects/hello")
	fmt.Println("  http://localhost:8080/objects/world")
	fmt.Println("  http://localhost:8080/objects/not-exist")
	http.ListenAndServe(":8080", nil)
}
