// Filename: pkg/store/memstore/memory_test.go
package memstore

import (
	"context"
	"io"
	"sync"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

// --- Mock Eviction Policy for precise testing ---

type mockPolicy struct {
	mu          sync.Mutex
	touched     []string
	added       map[string]int64
	removed     []string
	keysToEvict []string
}

func newMockPolicy() *mockPolicy {
	return &mockPolicy{
		added: make(map[string]int64),
	}
}

func (m *mockPolicy) Touch(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.touched = append(m.touched, key)
}

func (m *mockPolicy) Add(key string, size int64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.added[key] = size
}

func (m *mockPolicy) Remove(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.removed = append(m.removed, key)
}

func (m *mockPolicy) Evict() []string {
	m.mu.Lock()
	defer m.mu.Unlock()
	if len(m.keysToEvict) == 0 {
		return nil
	}
	key := m.keysToEvict[0]
	m.keysToEvict = m.keysToEvict[1:]
	return []string{key}
}

func (m *mockPolicy) setKeysToEvict(keys ...string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.keysToEvict = keys
}

func (m *mockPolicy) clear() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.touched = nil
	m.added = make(map[string]int64)
	m.removed = nil
	m.keysToEvict = nil
}

// --- Test Cases ---

// TestMemStore_SetAndGetStream tests the basic happy path for setting and getting data.
func TestMemStore_SetAndGetStream(t *testing.T) {
	ctx := context.Background()
	store := New(0, nil) // No capacity limit
	key := "test-key"
	etag := "v1"
	content := "hello world"

	// 1. Set data
	writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)
	_, err = writer.Write([]byte(content))
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// 2. Get data
	reader, meta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, reader)
	require.NotNil(t, meta)
	defer reader.Close()

	// 3. Verify content and metadata
	readBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, content, string(readBytes))
	assert.Equal(t, etag, meta.ETag)
}

// TestMemStore_Get_NotFound tests getting a non-existent key.
func TestMemStore_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	store := New(0, nil)

	_, _, err := store.GetStream(ctx, "non-existent-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "Expected ErrNotFound for a non-existent key")
}

// TestMemStore_Stat tests retrieving metadata for an object.
func TestMemStore_Stat(t *testing.T) {
	ctx := context.Background()
	policy := newMockPolicy()
	store := New(0, policy)
	key := "stat-key"
	etag := "etag-for-stat"

	// 1. Stat non-existent key
	_, err := store.Stat(ctx, "non-existent-key")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)

	// 2. Set data
	writer, _ := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	writer.Write([]byte("data"))
	writer.Close()

	// 3. Stat existing key
	meta, err := store.Stat(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, meta)
	assert.Equal(t, etag, meta.ETag)

	// 4. Verify policy was touched
	assert.Contains(t, policy.touched, key, "Stat should touch the policy")
}

// TestMemStore_Delete tests object deletion.
func TestMemStore_Delete(t *testing.T) {
	ctx := context.Background()
	policy := newMockPolicy()
	store := New(100, policy) // Capacity to check size updates
	key := "delete-key"
	content := "some data"

	// 1. Set data
	writer, _ := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
	writer.Write([]byte(content))
	writer.Close()

	initialSize := store.currentSize
	assert.Equal(t, int64(len(content)), initialSize)
	assert.Contains(t, policy.added, key)

	// 2. Delete the object
	err := store.Delete(ctx, key)
	require.NoError(t, err)

	// 3. Verify it's gone
	_, _, err = store.GetStream(ctx, key)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound)
	assert.Equal(t, int64(0), store.currentSize, "currentSize should be updated after delete")
	assert.Contains(t, policy.removed, key, "Delete should notify the policy")

	// 4. Delete non-existent key (should not error)
	err = store.Delete(ctx, key)
	assert.NoError(t, err, "Deleting a non-existent key should not return an error")
}

// TestMemStore_Overwrite tests overwriting an existing key.
func TestMemStore_Overwrite(t *testing.T) {
	ctx := context.Background()
	store := New(100, nil)
	key := "overwrite-key"

	// 1. Write initial version
	writer1, _ := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
	writer1.Write([]byte("version 1"))
	writer1.Close()
	assert.Equal(t, int64(len("version 1")), store.currentSize)

	// 2. Write new version
	newContent := "this is version 2"
	writer2, _ := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v2"})
	writer2.Write([]byte(newContent))
	writer2.Close()

	// 3. Verify new version is stored correctly
	assert.Equal(t, int64(len(newContent)), store.currentSize, "currentSize should be updated after overwrite")

	reader, meta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()
	readBytes, _ := io.ReadAll(reader)

	assert.Equal(t, "v2", meta.ETag)
	assert.Equal(t, newContent, string(readBytes))
}

// TestMemStore_Eviction tests that eviction is triggered when capacity is exceeded.
func TestMemStore_Eviction(t *testing.T) {
	ctx := context.Background()
	policy := newMockPolicy()
	store := New(20, policy) // Capacity of 20 bytes

	// 1. Set up eviction target
	policy.setKeysToEvict("key1")

	// 2. Add first item (10 bytes)
	writer1, _ := store.SetWithWriter(ctx, "key1", &daramjwee.Metadata{ETag: "v1"})
	writer1.Write([]byte("0123456789"))
	writer1.Close()
	assert.Equal(t, int64(10), store.currentSize)
	_, _, err := store.GetStream(ctx, "key1")
	require.NoError(t, err, "key1 should exist before eviction")

	// 3. Add second item (15 bytes), which exceeds capacity (10 + 15 > 20)
	writer2, _ := store.SetWithWriter(ctx, "key2", &daramjwee.Metadata{ETag: "v2"})
	writer2.Write([]byte("0123456789ABCDE"))
	err = writer2.Close() // Eviction happens here
	require.NoError(t, err)

	// 4. Verify state after eviction
	assert.Equal(t, int64(15), store.currentSize, "currentSize should be size of key2 after eviction")
	_, _, err = store.GetStream(ctx, "key1")
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "key1 should be evicted")
	_, _, err = store.GetStream(ctx, "key2")
	assert.NoError(t, err, "key2 should remain after eviction")
}

// TestMemStore_PolicyIntegration tests that all policy methods are called correctly.
func TestMemStore_PolicyIntegration(t *testing.T) {
	ctx := context.Background()
	policy := newMockPolicy()
	store := New(10, policy)
	key1, key2 := "key1", "key2"

	// 1. Add key1
	writer1, _ := store.SetWithWriter(ctx, key1, &daramjwee.Metadata{ETag: "v1"})
	writer1.Write([]byte("data1"))
	writer1.Close()
	assert.Contains(t, policy.added, key1, "Add should be called for key1")
	assert.Equal(t, int64(5), policy.added[key1])

	// 2. Get key1
	reader, _, _ := store.GetStream(ctx, key1)
	reader.Close()
	assert.Contains(t, policy.touched, key1, "GetStream should call Touch")
	policy.clear() // Reset for next check

	// 3. Stat key1
	store.Stat(ctx, key1)
	assert.Contains(t, policy.touched, key1, "Stat should call Touch")
	policy.clear()

	// 4. Delete key1
	store.Delete(ctx, key1)
	assert.Contains(t, policy.removed, key1, "Delete should call Remove")

	// 5. Eviction (via Add)
	policy.setKeysToEvict(key2)
	writer2, _ := store.SetWithWriter(ctx, key2, &daramjwee.Metadata{ETag: "v2"})
	writer2.Write([]byte("data2"))
	writer2.Close() // Add key2

	writer3, _ := store.SetWithWriter(ctx, "key3", &daramjwee.Metadata{ETag: "v3"})
	writer3.Write([]byte("data3-long")) // Exceeds capacity
	writer3.Close()

	// In this scenario, Evict() will be called, but since the mock returns "key2",
	// the test should verify that key2 is gone.
	_, _, err := store.GetStream(ctx, key2)
	assert.ErrorIs(t, err, daramjwee.ErrNotFound, "key2 should have been evicted")
}

// TestMemStore_Concurrency tests thread-safety of the store.
func TestMemStore_Concurrency(t *testing.T) {
	ctx := context.Background()
	store := New(0, nil)
	wg := sync.WaitGroup{}
	numGoroutines := 100

	// Run concurrent Set and Get operations
	for i := 0; i < numGoroutines; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			key := "key"
			content := "content"

			// Perform a write
			writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v1"})
			if assert.NoError(t, err) {
				_, err = writer.Write([]byte(content))
				if assert.NoError(t, err) {
					err = writer.Close()
					assert.NoError(t, err)
				}
			}

			// Perform a read
			reader, meta, err := store.GetStream(ctx, key)
			if assert.NoError(t, err) {
				assert.Equal(t, "v1", meta.ETag)
				_, err := io.ReadAll(reader)
				assert.NoError(t, err)
				reader.Close()
			}
		}(i)
	}

	wg.Wait()
}

// TestMemStore_Parallel tests different methods in parallel to catch race conditions.
func TestMemStore_Parallel(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	store := New(1024, newMockPolicy())

	// Pre-populate with some data
	keys := []string{"keyA", "keyB", "keyC", "keyD"}
	for _, key := range keys {
		writer, _ := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: "v_init"})
		writer.Write([]byte("initial"))
		writer.Close()
	}

	t.Run("group", func(t *testing.T) {
		t.Run("Set-Get", func(t *testing.T) {
			t.Parallel()
			writer, _ := store.SetWithWriter(ctx, "keyA", &daramjwee.Metadata{ETag: "v_setget"})
			writer.Write([]byte("from set-get"))
			writer.Close()
			r, m, err := store.GetStream(ctx, "keyA")
			assert.NoError(t, err)
			if err == nil {
				assert.Equal(t, "v_setget", m.ETag)
				r.Close()
			}
		})
		t.Run("Stat", func(t *testing.T) {
			t.Parallel()
			_, err := store.Stat(ctx, "keyB")
			assert.NoError(t, err)
		})
		t.Run("Delete", func(t *testing.T) {
			t.Parallel()
			err := store.Delete(ctx, "keyC")
			assert.NoError(t, err)
		})
		t.Run("Set-New", func(t *testing.T) {
			t.Parallel()
			writer, _ := store.SetWithWriter(ctx, "keyE", &daramjwee.Metadata{ETag: "v_new"})
			writer.Write([]byte("new key"))
			writer.Close()
		})
	})

	// Final state verification
	store.mu.RLock()
	defer store.mu.RUnlock()

	_, keyA_ok := store.data["keyA"]
	_, keyB_ok := store.data["keyB"]
	_, keyC_ok := store.data["keyC"]
	_, keyD_ok := store.data["keyD"]
	_, keyE_ok := store.data["keyE"]

	assert.True(t, keyA_ok)
	assert.True(t, keyB_ok)
	assert.False(t, keyC_ok, "keyC should have been deleted")
	assert.True(t, keyD_ok)
	assert.True(t, keyE_ok)
}

// TestMemStore_SetEmptyValue tests setting an empty value.
// TestMemStore_NegativeCache_NoBody tests that setting an item with IsNegative=true
// and writing no data results in a zero-byte entry.
func TestMemStore_NegativeCache_NoBody(t *testing.T) {
	ctx := context.Background()
	store := New(100, nil)
	key := "negative-cache-key"

	// 1. Set an item with IsNegative=true and an empty body.
	meta := &daramjwee.Metadata{ETag: "v-neg", IsNegative: true}
	writer, err := store.SetWithWriter(ctx, key, meta)
	require.NoError(t, err)
	// Write *no* data to the writer.
	err = writer.Close()
	require.NoError(t, err)

	// 2. Verify internal state.
	store.mu.RLock()
	entry, ok := store.data[key]
	require.True(t, ok, "Entry should exist in the map")
	assert.Len(t, entry.value, 0, "Internal value should be a zero-length byte slice")
	store.mu.RUnlock()

	// 3. Verify via GetStream.
	reader, retrievedMeta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	// Check metadata
	assert.True(t, retrievedMeta.IsNegative)
	assert.Equal(t, "v-neg", retrievedMeta.ETag)

	// Check body
	readBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Len(t, readBytes, 0, "Retrieved body should be empty for a negative cache entry")
}

func TestMemStore_SetEmptyValue(t *testing.T) {
	ctx := context.Background()
	store := New(100, nil)
	key := "empty-value-key"
	etag := "v_empty"

	// Set empty data
	writer, err := store.SetWithWriter(ctx, key, &daramjwee.Metadata{ETag: etag})
	require.NoError(t, err)
	// Write nothing
	err = writer.Close()
	require.NoError(t, err)

	// Verify size and existence
	assert.Equal(t, int64(0), store.currentSize)
	reader, meta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	defer reader.Close()

	readBytes, err := io.ReadAll(reader)
	require.NoError(t, err)
	assert.Equal(t, "", string(readBytes))
	assert.Equal(t, etag, meta.ETag)
}

// TestMemStore_MetadataFields ensures all metadata fields are stored and retrieved correctly.
func TestMemStore_MetadataFields(t *testing.T) {
	ctx := context.Background()
	store := New(0, nil)
	key := "metadata-test-key"
	now := time.Now().Truncate(time.Millisecond) // Truncate for reliable comparison

	// 1. Set data with complex metadata
	originalMeta := &daramjwee.Metadata{
		ETag:       "v-complex",
		CachedAt:   now,
		IsNegative: true,
	}
	writer, err := store.SetWithWriter(ctx, key, originalMeta)
	require.NoError(t, err)
	err = writer.Close()
	require.NoError(t, err)

	// 2. Get data and verify metadata
	_, retrievedMeta, err := store.GetStream(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, retrievedMeta)

	assert.Equal(t, originalMeta.ETag, retrievedMeta.ETag)
	assert.True(t, originalMeta.CachedAt.Equal(retrievedMeta.CachedAt), "GraceUntil should be equal")
	assert.Equal(t, originalMeta.IsNegative, retrievedMeta.IsNegative)

	// 3. Stat data and verify metadata
	retrievedMetaFromStat, err := store.Stat(ctx, key)
	require.NoError(t, err)
	require.NotNil(t, retrievedMetaFromStat)

	assert.Equal(t, originalMeta.ETag, retrievedMetaFromStat.ETag)
	assert.True(t, originalMeta.CachedAt.Equal(retrievedMetaFromStat.CachedAt), "GraceUntil from Stat should be equal")
	assert.Equal(t, originalMeta.IsNegative, retrievedMetaFromStat.IsNegative)
}

// --- 비정상 정책 테스트를 위한 Mock Policy ---
type badPolicy struct {
	// Evict는 항상 존재하지 않는 키를 반환합니다.
	EvictFunc func() []string
}

func (p *badPolicy) Touch(key string)           {}
func (p *badPolicy) Add(key string, size int64) {}
func (p *badPolicy) Remove(key string)          {}
func (p *badPolicy) Evict() []string {
	if p.EvictFunc != nil {
		return p.EvictFunc()
	}
	return []string{"non-existent-key-1", "non-existent-key-2"}
}

// TestMemStore_EvictionLoop_WithBadPolicy는 EvictionPolicy가
// 유효하지 않은 (존재하지 않는) 키를 계속 반환할 때, MemStore가
// 무한 루프나 교착 상태에 빠지지 않는지 검증합니다.
func TestMemStore_EvictionLoop_WithBadPolicy(t *testing.T) {
	ctx := context.Background()
	// 1. 비정상적으로 동작하는 정책을 생성합니다.
	policy := &badPolicy{}
	// 용량이 100바이트인 저장소 생성
	store := New(100, policy)

	// 2. 용량을 초과하는 첫 번째 아이템을 추가합니다.
	// 이 아이템 자체는 정상적으로 추가되어야 합니다.
	writer1, err := store.SetWithWriter(ctx, "key1", &daramjwee.Metadata{})
	require.NoError(t, err)
	_, err = writer1.Write(make([]byte, 80)) // 80 bytes
	require.NoError(t, err)
	err = writer1.Close()
	require.NoError(t, err)

	// 3. 용량을 초과하게 만드는 두 번째 아이템을 추가합니다.
	// 이로 인해 Close() 내부에서 축출 루프가 시작됩니다.
	writer2, err := store.SetWithWriter(ctx, "key2", &daramjwee.Metadata{})
	require.NoError(t, err)
	_, err = writer2.Write(make([]byte, 80)) // 80 bytes. Total=160, Capacity=100
	require.NoError(t, err)

	// 4. Close() 호출이 무한 루프에 빠지지 않고 일정 시간 내에 완료되는지 검증합니다.
	// 이 채널은 Close()가 성공적으로 리턴되면 닫힙니다.
	closeDone := make(chan struct{})
	go func() {
		// badPolicy가 존재하지 않는 키만 반환하므로,
		// 방어 코드가 없다면 이 호출은 영원히 끝나지 않을 것입니다.
		err = writer2.Close()
		require.NoError(t, err) // 에러 없이 정상 종료되어야 함
		close(closeDone)
	}()

	select {
	case <-closeDone:
		// 테스트 성공. Close()가 정상적으로 리턴되었습니다.
	case <-time.After(500 * time.Millisecond):
		// 500ms 동안 Close()가 리턴되지 않으면 테스트 실패.
		t.Fatal("writer.Close() did not complete in time, potential infinite loop detected.")
	}

	// 5. 최종 상태 검증
	// 축출이 일어나지 않았으므로, 현재 크기는 160이어야 합니다.
	store.mu.RLock()
	defer store.mu.RUnlock()
	assert.Equal(t, int64(160), store.currentSize, "Eviction should have failed, so size should be the sum of both items.")
	_, key1Exists := store.data["key1"]
	_, key2Exists := store.data["key2"]
	assert.True(t, key1Exists, "key1 should still exist")
	assert.True(t, key2Exists, "key2 should still exist")
}
