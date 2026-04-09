package daramjwee

import "testing"

func BenchmarkTopWriteSinkWrite_4KiB(b *testing.B) {
	payload := make([]byte, 4*1024)

	b.Run("DirectSink", func(b *testing.B) {
		sink := &benchmarkCountSink{}
		b.SetBytes(int64(len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := sink.Write(payload); err != nil {
				b.Fatalf("write: %v", err)
			}
		}
	})

	b.Run("TopWriteSink", func(b *testing.B) {
		state := &topWriteState{}
		sink := &topWriteSink{
			WriteSink:  &benchmarkCountSink{},
			state:      state,
			generation: 1,
		}
		b.SetBytes(int64(len(payload)))
		b.ReportAllocs()
		b.ResetTimer()
		for i := 0; i < b.N; i++ {
			if _, err := sink.Write(payload); err != nil {
				b.Fatalf("write: %v", err)
			}
		}
		b.StopTimer()
		if state.lastTouched.Load() == 0 {
			b.Fatal("expected lastTouched to be updated")
		}
	})
}

type benchmarkCountSink struct{}

func (s *benchmarkCountSink) Write(p []byte) (int, error) { return len(p), nil }
func (s *benchmarkCountSink) Close() error                { return nil }
func (s *benchmarkCountSink) Abort() error                { return nil }
