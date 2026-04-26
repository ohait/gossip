package gossip

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestLoopbackClientInitRequiresCallback(t *testing.T) {
	c := &LoopbackClient{LogFile: filepath.Join(t.TempDir(), "loopback.bin")}
	if err := c.Init(nil); err == nil {
		t.Fatal("Init() unexpectedly succeeded")
	}
}

func TestLoopbackClientInitCreatesFileWithExpectedMode(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loopback.bin")
	c := &LoopbackClient{LogFile: path}
	if err := c.Init(func(topic, id string, ts int64, data []byte) error {
		return nil
	}); err != nil {
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
	writer := &LoopbackClient{LogFile: path}
	if err := writer.Init(func(topic, id string, ts int64, data []byte) error {
		return nil
	}); err != nil {
		t.Fatalf("writer.Init(): %v", err)
	}
	if err := writer.PublishLWW("id-1", time.Now().UnixNano(), []byte("hello")); err != nil {
		t.Fatalf("writer.Publish(): %v", err)
	}

	var replayed int
	reader := &LoopbackClient{LogFile: path}
	if err := reader.Init(func(topic, id string, ts int64, data []byte) error {
		replayed++
		if id != "id-1" || string(data) != "hello" {
			t.Fatalf("replayed (%q, %q), want (%q, %q)", id, string(data), "id-1", "hello")
		}
		return nil
	}); err != nil {
		t.Fatalf("reader.Init(): %v", err)
	}
	if replayed != 1 {
		t.Fatalf("replayed = %d, want 1", replayed)
	}
}

func TestLoopbackClientSignalDoesNotPersist(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loopback.bin")
	var emitted int
	writer := &LoopbackClient{LogFile: path}
	if err := writer.Init(func(topic, id string, ts int64, data []byte) error {
		emitted++
		if id != "sig-1" || string(data) != "hello" {
			t.Fatalf("emitted (%q, %q), want (%q, %q)", id, string(data), "sig-1", "hello")
		}
		return nil
	}); err != nil {
		t.Fatalf("writer.Init(): %v", err)
	}
	if err := writer.Signal("sig-1", time.Now().UnixNano(), []byte("hello")); err != nil {
		t.Fatalf("writer.Signal(): %v", err)
	}
	if emitted != 1 {
		t.Fatalf("emitted = %d, want 1", emitted)
	}

	var replayed int
	reader := &LoopbackClient{LogFile: path}
	if err := reader.Init(func(topic, id string, ts int64, data []byte) error {
		replayed++
		return nil
	}); err != nil {
		t.Fatalf("reader.Init(): %v", err)
	}
	if replayed != 0 {
		t.Fatalf("replayed = %d, want 0", replayed)
	}
}

func TestLoopbackClientClose(t *testing.T) {
	c := &LoopbackClient{LogFile: filepath.Join(t.TempDir(), "loopback.bin")}
	if err := c.Init(func(topic, id string, ts int64, data []byte) error {
		return nil
	}); err != nil {
		t.Fatalf("Init(): %v", err)
	}
	if err := c.Close(); err != nil {
		t.Fatalf("Close(): %v", err)
	}
	if err := c.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("second Close() error = %v, want %v", err, os.ErrClosed)
	}
	if err := c.PublishLWW("id-1", time.Now().UnixNano(), []byte("hello")); err == nil {
		t.Fatal("Publish() after Close() unexpectedly succeeded")
	}
	if err := c.Signal("id-2", time.Now().UnixNano(), []byte("hello")); err == nil {
		t.Fatal("Signal() after Close() unexpectedly succeeded")
	}
}

func TestLoopbackClientPublishCASSucceedsAndAssignsNewTS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loopback.bin")
	var seenTS []int64
	c := &LoopbackClient{LogFile: path}
	if err := c.Init(func(topic, id string, ts int64, data []byte) error {
		seenTS = append(seenTS, ts)
		return nil
	}); err != nil {
		t.Fatalf("Init(): %v", err)
	}

	initialTS := time.Now().Add(-time.Second).UnixNano()
	if err := c.PublishLWW("id-1", initialTS, []byte("first")); err != nil {
		t.Fatalf("PublishLWW(): %v", err)
	}
	if err := c.PublishCAS("id-1", seenTS[0], []byte("second")); err != nil {
		t.Fatalf("PublishCAS(): %v", err)
	}

	if len(seenTS) != 2 {
		t.Fatalf("seenTS len = %d, want 2", len(seenTS))
	}
	if seenTS[1] == initialTS {
		t.Fatal("CAS should assign a new timestamp")
	}
	if seenTS[1] <= seenTS[0] {
		t.Fatalf("CAS timestamp = %d, want > %d", seenTS[1], seenTS[0])
	}
}

func TestLoopbackClientPublishCASFailsOnMismatchedTS(t *testing.T) {
	path := filepath.Join(t.TempDir(), "loopback.bin")
	c := &LoopbackClient{LogFile: path}
	if err := c.Init(func(topic, id string, ts int64, data []byte) error {
		return nil
	}); err != nil {
		t.Fatalf("Init(): %v", err)
	}

	initialTS := time.Now().Add(-time.Second).UnixNano()
	if err := c.PublishLWW("id-1", initialTS, []byte("first")); err != nil {
		t.Fatalf("PublishLWW(): %v", err)
	}

	if err := c.PublishCAS("id-1", initialTS+1, []byte("second")); err == nil {
		t.Fatal("PublishCAS() unexpectedly succeeded")
	}
}
