package daramjwee

import (
	"bytes"
	"io"
	"testing"
)

func BenchmarkStreamThrough_1MiB(b *testing.B) {
	payload := bytes.Repeat([]byte("s"), 1*1024*1024)
	b.SetBytes(int64(len(payload)))

	b.Run("WriteTo", func(b *testing.B) {
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			stream := streamThrough(io.NopCloser(bytes.NewReader(payload)), &benchmarkDiscardSink{}, nil, nil)
			if _, err := io.Copy(io.Discard, stream); err != nil {
				b.Fatalf("copy: %v", err)
			}
			if err := stream.Close(); err != nil {
				b.Fatalf("close: %v", err)
			}
		}
	})

	b.Run("ReadLoop", func(b *testing.B) {
		buf := make([]byte, 4*1024)
		b.ReportAllocs()
		for i := 0; i < b.N; i++ {
			stream := streamThrough(io.NopCloser(bytes.NewReader(payload)), &benchmarkDiscardSink{}, nil, nil)
			for {
				_, err := stream.Read(buf)
				if err == io.EOF {
					break
				}
				if err != nil {
					b.Fatalf("read: %v", err)
				}
			}
			if err := stream.Close(); err != nil {
				b.Fatalf("close: %v", err)
			}
		}
	})
}

type benchmarkDiscardSink struct{}

func (s *benchmarkDiscardSink) Write(p []byte) (int, error) {
	return len(p), nil
}

func (s *benchmarkDiscardSink) Close() error {
	return nil
}

func (s *benchmarkDiscardSink) Abort() error {
	return nil
}
