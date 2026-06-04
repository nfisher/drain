package parseio

import "bytes"

type boundedBuffer struct {
	buffer    bytes.Buffer
	limit     int
	truncated bool
}

func (b *boundedBuffer) Write(p []byte) (int, error) {
	written := len(p)
	if b.limit <= 0 {
		b.truncated = b.truncated || len(p) > 0
		return written, nil
	}
	remaining := b.limit - b.buffer.Len()
	if remaining <= 0 {
		b.truncated = b.truncated || len(p) > 0
		return written, nil
	}
	if len(p) > remaining {
		_, _ = b.buffer.Write(p[:remaining])
		b.truncated = true
		return written, nil
	}
	_, _ = b.buffer.Write(p)
	return written, nil
}

func (b *boundedBuffer) String() string {
	value := b.buffer.String()
	if b.truncated {
		return value + "...(truncated)"
	}
	return value
}
