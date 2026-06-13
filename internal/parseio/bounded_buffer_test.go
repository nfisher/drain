package parseio

import "testing"

func TestBoundedBufferRetainsBytesUpToLimitAndReportsTruncation(t *testing.T) {
	var buffer boundedBuffer
	buffer.limit = 5

	if n, err := buffer.Write([]byte("abc")); err != nil || n != 3 {
		t.Fatalf("first Write() = %d, %v; want 3, nil", n, err)
	}
	if n, err := buffer.Write([]byte("def")); err != nil || n != 3 {
		t.Fatalf("second Write() = %d, %v; want 3, nil", n, err)
	}
	if got, want := buffer.String(), "abcde...(truncated)"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}

func TestBoundedBufferWithNonPositiveLimitDiscardsAndMarksNonEmptyWrites(t *testing.T) {
	var buffer boundedBuffer

	if n, err := buffer.Write([]byte("ignored")); err != nil || n != len("ignored") {
		t.Fatalf("Write() = %d, %v; want full length, nil", n, err)
	}
	if got, want := buffer.String(), "...(truncated)"; got != want {
		t.Fatalf("String() = %q, want %q", got, want)
	}
}
