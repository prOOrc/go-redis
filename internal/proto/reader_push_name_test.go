package proto

import (
	"io"
	"testing"
	"time"
)

// slowReader delivers bytes in chunks with delays, emulating a push
// notification split across TCP segments.
type chunkedReader struct {
	chunks [][]byte
	delay  time.Duration
	i      int
}

func (r *chunkedReader) Read(p []byte) (int, error) {
	if r.i >= len(r.chunks) {
		return 0, io.EOF
	}
	time.Sleep(r.delay)
	n := copy(p, r.chunks[r.i])
	r.i++
	return n, nil
}

// TestPeekPushNotificationNamePartialArrival verifies that the push
// notification name is parsed even when the notification is only partially
// buffered: PeekPushNotificationName must wait for the remaining bytes
// instead of failing and leaving the notification in the stream, where it
// would later be parsed as a command reply.
func TestPeekPushNotificationNamePartialArrival(t *testing.T) {
	// A full invalidate notification: >2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nkey01\r\n
	full := []byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nkey01\r\n")
	for _, split := range []int{1, 2, 4, 7, 15, 20, len(full) - 1} {
		chunks := [][]byte{full[:split], full[split:]}
		rd := NewReader(&chunkedReader{chunks: chunks, delay: 2 * time.Millisecond})
		name, err := rd.PeekPushNotificationName()
		if err != nil {
			t.Fatalf("split=%d: PeekPushNotificationName returned error: %v", split, err)
		}
		if name != "invalidate" {
			t.Fatalf("split=%d: name=%q, want %q", split, name, "invalidate")
		}
		// the notification must still be intact for ReadReply
		reply, err := rd.ReadReply()
		if err != nil {
			t.Fatalf("split=%d: ReadReply returned error: %v", split, err)
		}
		arr, ok := reply.([]interface{})
		if !ok || len(arr) != 2 || arr[0] != "invalidate" {
			t.Fatalf("split=%d: unexpected reply %#v", split, reply)
		}
	}
}

func TestPeekPushNotificationNameComplete(t *testing.T) {
	rd := NewReader(&chunkedReader{chunks: [][]byte{
		[]byte(">2\r\n$10\r\ninvalidate\r\n*1\r\n$5\r\nkey01\r\n"),
	}})
	name, err := rd.PeekPushNotificationName()
	if err != nil {
		t.Fatalf("PeekPushNotificationName returned error: %v", err)
	}
	if name != "invalidate" {
		t.Fatalf("name=%q, want %q", name, "invalidate")
	}
}

// A non-push reply type must be rejected without consuming it.
func TestPeekPushNotificationNameNotPush(t *testing.T) {
	rd := NewReader(&chunkedReader{chunks: [][]byte{[]byte("$3\r\nabc\r\n")}})
	if _, err := rd.PeekPushNotificationName(); err == nil {
		t.Fatal("expected an error for a non-push reply type")
	}
}
