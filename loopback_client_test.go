package gossip

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoopbackClientInitRequiresOnMessage(t *testing.T) {
	c := &LoopbackClient{LogFile: filepath.Join(t.TempDir(), "loopback.bin")}
	if err := c.Init(); err == nil {
		t.Fatal("Init() unexpectedly succeeded")
	}
}

func TestLoopbackClientInitCreatesFileWithExpectedMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loopback.bin")
	c := &LoopbackClient{
		LogFile: path,
		OnMessage: func(id string, ts int64, data []byte) error {
			return nil
		},
	}
	if err := c.Init(); err != nil {
		t.Fatalf("Init(): %v", err)
	}

	info, err := os.Stat(path)
	if err != nil {
		t.Fatalf("Stat(): %v", err)
	}
	if got := info.Mode().Perm(); got != 0o644 {
		t.Fatalf("mode = %o, want %o", got, 0o644)
	}
}

func TestLoopbackClientInitReplaysExistingData(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loopback.bin")
	writer := &LoopbackClient{
		LogFile: path,
		OnMessage: func(id string, ts int64, data []byte) error {
			return nil
		},
	}
	if err := writer.Init(); err != nil {
		t.Fatalf("writer.Init(): %v", err)
	}
	if err := writer.Publish("id-1", time.Now().UnixNano(), []byte("hello")); err != nil {
		t.Fatalf("writer.Publish(): %v", err)
	}

	var replayed int
	reader := &LoopbackClient{
		LogFile: path,
		OnMessage: func(id string, ts int64, data []byte) error {
			replayed++
			if id != "id-1" || string(data) != "hello" {
				t.Fatalf("replayed (%q, %q), want (%q, %q)", id, string(data), "id-1", "hello")
			}
			return nil
		},
	}
	if err := reader.Init(); err != nil {
		t.Fatalf("reader.Init(): %v", err)
	}
	if replayed != 1 {
		t.Fatalf("replayed = %d, want 1", replayed)
	}
}

func TestLoopbackClientEmitDoesNotPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loopback.bin")
	var emitted int
	writer := &LoopbackClient{
		LogFile: path,
		OnMessage: func(id string, ts int64, data []byte) error {
			emitted++
			if id != "sig-1" || string(data) != "hello" {
				t.Fatalf("emitted (%q, %q), want (%q, %q)", id, string(data), "sig-1", "hello")
			}
			return nil
		},
	}
	if err := writer.Init(); err != nil {
		t.Fatalf("writer.Init(): %v", err)
	}
	if err := writer.Emit("sig-1", time.Now().UnixNano(), []byte("hello")); err != nil {
		t.Fatalf("writer.Emit(): %v", err)
	}
	if emitted != 1 {
		t.Fatalf("emitted = %d, want 1", emitted)
	}

	var replayed int
	reader := &LoopbackClient{
		LogFile: path,
		OnMessage: func(id string, ts int64, data []byte) error {
			replayed++
			return nil
		},
	}
	if err := reader.Init(); err != nil {
		t.Fatalf("reader.Init(): %v", err)
	}
	if replayed != 0 {
		t.Fatalf("replayed = %d, want 0", replayed)
	}
}

func TestLoopbackClientClose(t *testing.T) {
	c := &LoopbackClient{
		LogFile: filepath.Join(t.TempDir(), "loopback.bin"),
		OnMessage: func(id string, ts int64, data []byte) error {
			return nil
		},
	}
	if err := c.Init(); err != nil {
		t.Fatalf("Init(): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := c.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("second Close() error = %v, want %v", err, os.ErrClosed)
	}
	if err := c.Publish("id-1", time.Now().UnixNano(), []byte("hello")); err == nil {
		t.Fatal("Publish() after Close() unexpectedly succeeded")
	}
	if err := c.Emit("id-2", time.Now().UnixNano(), []byte("hello")); err == nil {
		t.Fatal("Emit() after Close() unexpectedly succeeded")
	}
}
