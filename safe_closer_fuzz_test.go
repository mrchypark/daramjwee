package daramjwee

import (
	"bytes"
	"io"
	"testing"
)

func FuzzSafeCloserReadAll(f *testing.F) {
	f.Add([]byte("hello world"), uint8(3))
	f.Add([]byte{}, uint8(1))
	f.Add(bytes.Repeat([]byte("ab"), 32), uint8(7))

	f.Fuzz(func(t *testing.T, payload []byte, chunk uint8) {
		reader := &chunkedReadCloser{
			data:      bytes.Clone(payload),
			chunkSize: int(chunk%17) + 1,
		}
		callbacks := 0
		sc := newSafeCloser(reader, func() {
			callbacks++
		})

		got, err := sc.ReadAll()
		if err != nil {
			t.Fatalf("safe closer read all: %v", err)
		}
		if !bytes.Equal(got, payload) {
			t.Fatalf("safe closer read mismatch: got %q want %q", got, payload)
		}
		if callbacks != 1 || reader.closeCount != 1 {
			t.Fatalf("safe closer close counts: callback=%d readerClose=%d", callbacks, reader.closeCount)
		}
		if err := sc.Close(); err != nil {
			t.Fatalf("safe closer second close: %v", err)
		}
		if callbacks != 1 || reader.closeCount != 1 {
			t.Fatalf("safe closer duplicate close changed counts: callback=%d readerClose=%d", callbacks, reader.closeCount)
		}

		plain := io.NopCloser(bytes.NewReader(payload))
		plainRead, err := ReadAll(plain)
		if err != nil {
			t.Fatalf("ReadAll helper: %v", err)
		}
		if !bytes.Equal(plainRead, payload) {
			t.Fatalf("ReadAll helper mismatch: got %q want %q", plainRead, payload)
		}
	})
}

type chunkedReadCloser struct {
	data       []byte
	offset     int
	chunkSize  int
	closeCount int
}

func (r *chunkedReadCloser) Read(p []byte) (int, error) {
	if r.offset >= len(r.data) {
		return 0, io.EOF
	}
	limit := r.chunkSize
	if limit > len(p) {
		limit = len(p)
	}
	if remaining := len(r.data) - r.offset; limit > remaining {
		limit = remaining
	}
	n := copy(p, r.data[r.offset:r.offset+limit])
	r.offset += n
	return n, nil
}

func (r *chunkedReadCloser) Close() error {
	r.closeCount++
	return nil
}
