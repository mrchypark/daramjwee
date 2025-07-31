package lock

import (
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/mrchypark/daramjwee"
)

// benchmarkLocker is a helper function to run a standardized benchmark for any Locker implementation.
func benchmarkLocker(b *testing.B, locker daramjwee.Locker, readRatio float64) {
	keys := make([]string, 1000)
	for i := 0; i < len(keys); i++ {
		keys[i] = fmt.Sprintf("key-%d", i)
	}

	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		for pb.Next() {
			key := keys[r.Intn(len(keys))]

			if r.Float64() < readRatio {
				// Read operation
				locker.RLock(key)
				// Simulate some read work (no actual work needed for lock benchmark)
				locker.RUnlock(key)
			} else {
				// Write operation
				locker.Lock(key)
				// Simulate some write work
				locker.Unlock(key)
			}
		}
	})
}

// --- MutexLock Benchmarks ---

func BenchmarkMutexLock_Read90(b *testing.B) {
	benchmarkLocker(b, NewMutexLock(), 0.90)
}

func BenchmarkMutexLock_Read50(b *testing.B) {
	benchmarkLocker(b, NewMutexLock(), 0.50)
}

func BenchmarkMutexLock_Read10(b *testing.B) {
	benchmarkLocker(b, NewMutexLock(), 0.10)
}

// --- StripeLock Benchmarks ---

func BenchmarkStripeLock_Read90(b *testing.B) {
	benchmarkLocker(b, NewStripeLock(256), 0.90)
}

func BenchmarkStripeLock_Read50(b *testing.B) {
	benchmarkLocker(b, NewStripeLock(256), 0.50)
}

func BenchmarkStripeLock_Read10(b *testing.B) {
	benchmarkLocker(b, NewStripeLock(256), 0.10)
}
