package redis_test

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"net"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

// startFakeResp3Server runs a minimal RESP3 server that answers HELLO, GET and
// CLIENT commands. On the second GET it injects an invalidate push notification
// split across two writes with a delay, emulating a TCP segmentation race, and
// only then writes the GET reply. Regression test for reply desync: before the
// fix the partially received notification was parsed as the command reply.
func startFakeResp3Server(t *testing.T, pushDelay time.Duration) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			go serveFakeResp3Conn(conn, pushDelay)
		}
	}()
	return ln.Addr().String()
}

func serveFakeResp3Conn(conn net.Conn, pushDelay time.Duration) {
	defer conn.Close()
	rd := bufio.NewReader(conn)
	gets := 0
	for {
		args, err := readRespCommand(rd)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}
		switch strings.ToUpper(args[0]) {
		case "HELLO":
			fmt.Fprintf(conn, "%%7\r\n"+
				"$6\r\nserver\r\n$5\r\nredis\r\n"+
				"$7\r\nversion\r\n$5\r\n7.4.0\r\n"+
				"$5\r\nproto\r\n:3\r\n"+
				"$2\r\nid\r\n:1\r\n"+
				"$4\r\nmode\r\n$10\r\nstandalone\r\n"+
				"$4\r\nrole\r\n$6\r\nmaster\r\n"+
				"$7\r\nmodules\r\n*0\r\n")
		case "GET":
			gets++
			if gets == 2 {
				// inject the invalidate push in two chunks: the header line
				// ">2\r\n" arrives alone (an incomplete parse state for the old
				// code), the remainder after pushDelay, then the GET reply
				if _, err := conn.Write([]byte(">2\r\n")); err != nil {
					return
				}
				time.Sleep(pushDelay)
				if _, err := conn.Write([]byte("$10\r\ninvalidate\r\n*1\r\n$5\r\npfx:k\r\n")); err != nil {
					return
				}
			}
			if _, err := conn.Write([]byte("$1\r\nv\r\n")); err != nil {
				return
			}
		default:
			if _, err := conn.Write([]byte("+OK\r\n")); err != nil {
				return
			}
		}
	}
}

func readRespCommand(rd *bufio.Reader) ([]string, error) {
	line, err := readRespLine(rd)
	if err != nil {
		return nil, err
	}
	if len(line) == 0 {
		return nil, nil
	}
	if line[0] != '*' {
		return strings.Fields(line), nil
	}
	n, err := strconv.Atoi(line[1:])
	if err != nil {
		return nil, err
	}
	args := make([]string, 0, n)
	for i := 0; i < n; i++ {
		argLine, err := readRespLine(rd)
		if err != nil {
			return nil, err
		}
		if len(argLine) == 0 || argLine[0] != '$' {
			return nil, fmt.Errorf("expected bulk string, got %q", argLine)
		}
		size, err := strconv.Atoi(argLine[1:])
		if err != nil {
			return nil, err
		}
		data := make([]byte, size+2)
		if _, err := io.ReadFull(rd, data); err != nil {
			return nil, err
		}
		args = append(args, string(data[:size]))
	}
	return args, nil
}

func readRespLine(rd *bufio.Reader) (string, error) {
	line, err := rd.ReadString('\n')
	if err != nil {
		return "", err
	}
	return strings.TrimSuffix(line, "\r\n"), nil
}

// TestConcurrentPushNotificationSplit verifies that a push notification split
// across TCP writes does not desynchronize the reply stream: the pending
// notification must be consumed before the command reply is parsed, and the
// command must return the correct value.
func TestConcurrentPushNotificationSplit(t *testing.T) {
	addr := startFakeResp3Server(t, 100*time.Millisecond)
	rdb := redis.NewClient(&redis.Options{
		Addr:     addr,
		Protocol: 3,
		PoolSize: 1,
		// no retries: the regression must surface on the first attempt
		MaxRetries: 0,
	})
	defer rdb.Close()
	ctx := context.Background()

	if err := rdb.Do(ctx, "CLIENT", "TRACKING", "ON", "BCAST", "PREFIX", "pfx:").Err(); err != nil {
		t.Fatalf("CLIENT TRACKING: %v", err)
	}
	// first GET initializes the flow
	if v, err := rdb.Get(ctx, "pfx:k").Result(); err != nil || v != "v" {
		t.Fatalf("warm-up GET: v=%q err=%v", v, err)
	}
	// second GET races with the split push notification
	done := make(chan error, 1)
	var val string
	go func() {
		v, err := rdb.Get(ctx, "pfx:k").Result()
		val = v
		done <- err
	}()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("GET failed with a split push notification: %v", err)
		}
		if val != "v" {
			t.Fatalf("GET returned %q, want %q", val, "v")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("GET timed out: reply stream desynchronized by a split push notification")
	}
}
