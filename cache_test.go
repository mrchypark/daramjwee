package daramjwee

import (
	"io"
	"strings"
	"testing"
)

func TestSafeCloserReadAll(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		callback bool
	}{
		{
			name:     "normal read all",
			input:    "hello world",
			expected: "hello world",
			callback: true,
		},
		{
			name:     "empty string",
			input:    "",
			expected: "",
			callback: true,
		},
		{
			name:     "large text",
			input:    strings.Repeat("test data ", 100),
			expected: strings.Repeat("test data ", 100),
			callback: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			// 콜백 실행 확인용
			callbackExecuted := false

			// strings.NewReader로 ReadCloser 생성
			reader := io.NopCloser(strings.NewReader(tt.input))

			// safeCloser 생성
			sc := newSafeCloser(reader, func() {
				callbackExecuted = true
			})

			// ReadAll 테스트
			result, err := sc.ReadAll()

			// 결과 검증
			if err != nil {
				t.Errorf("ReadAll() error = %v", err)
				return
			}

			if string(result) != tt.expected {
				t.Errorf("ReadAll() = %q, want %q", string(result), tt.expected)
			}

			// 콜백 실행 확인
			if callbackExecuted != tt.callback {
				t.Errorf("callback executed = %v, want %v", callbackExecuted, tt.callback)
			}
		})
	}
}

func TestSafeCloserReadAllAutoClose(t *testing.T) {
	// 콜백 실행 확인
	callbackExecuted := false
	closeCount := 0

	// 커스텀 ReadCloser로 Close 호출 횟수 확인
	reader := &testReadCloser{
		Reader: strings.NewReader("test data"),
		onClose: func() {
			closeCount++
		},
	}

	sc := newSafeCloser(reader, func() {
		callbackExecuted = true
	})

	// ReadAll 실행
	data, err := sc.ReadAll()

	if err != nil {
		t.Fatalf("ReadAll() error = %v", err)
	}

	if string(data) != "test data" {
		t.Errorf("ReadAll() = %q, want %q", string(data), "test data")
	}

	// EOF 도달 시 자동으로 한 번만 닫혀야 함
	if closeCount != 1 {
		t.Errorf("close count = %d, want 1", closeCount)
	}

	// 콜백이 실행되어야 함
	if !callbackExecuted {
		t.Error("callback should be executed")
	}

	// 다시 Close 호출해도 중복 실행되지 않아야 함
	sc.Close()
	if closeCount != 1 {
		t.Errorf("close count after second Close() = %d, want 1", closeCount)
	}
}

// 테스트용 ReadCloser
type testReadCloser struct {
	io.Reader
	onClose func()
}

func (t *testReadCloser) Close() error {
	if t.onClose != nil {
		t.onClose()
	}
	return nil
}
func TestReadAllSmart(t *testing.T) {
	tests := []struct {
		name     string
		input    string
		expected string
		useSafe  bool
	}{
		{
			name:     "with safeCloser",
			input:    "test data with safeCloser",
			expected: "test data with safeCloser",
			useSafe:  true,
		},
		{
			name:     "with regular ReadCloser",
			input:    "test data with regular ReadCloser",
			expected: "test data with regular ReadCloser",
			useSafe:  false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var rc io.ReadCloser
			callbackExecuted := false

			if tt.useSafe {
				// safeCloser 사용
				reader := io.NopCloser(strings.NewReader(tt.input))
				rc = newSafeCloser(reader, func() {
					callbackExecuted = true
				})
			} else {
				// 일반 ReadCloser 사용
				rc = io.NopCloser(strings.NewReader(tt.input))
			}

			// ReadAllSmart 테스트
			result, err := ReadAll(rc)

			if err != nil {
				t.Errorf("ReadAllSmart() error = %v", err)
				return
			}

			if string(result) != tt.expected {
				t.Errorf("ReadAllSmart() = %q, want %q", string(result), tt.expected)
			}

			// safeCloser인 경우 콜백이 실행되어야 함
			if tt.useSafe && !callbackExecuted {
				t.Error("callback should be executed for safeCloser")
			}

			// 일반 ReadCloser인 경우 콜백이 실행되지 않아야 함
			if !tt.useSafe && callbackExecuted {
				t.Error("callback should not be executed for regular ReadCloser")
			}
		})
	}
}
