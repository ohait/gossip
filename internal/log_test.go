package gossip

import (
	"os"
	"testing"
)

func newTestLog(t *testing.T) (*Log, func()) {
	t.Helper()
	f, err := os.CreateTemp(t.TempDir(), "log-*.bin")
	if err != nil {
		t.Fatal(err)
	}
	l := &Log{path: f.Name(), f: f}
	return l, func() { f.Close() }
}

func TestAppendAndRead(t *testing.T) {
	l, cleanup := newTestLog(t)
	defer cleanup()

	msg := Msg{ID: "msg-1", TS: 12345, Data: []byte("hello world")}
	entry, err := l.Append(msg)
	if err != nil {
		t.Fatalf("Append: %v", err)
	}
	if entry.Offset != 0 {
		t.Errorf("first entry offset = %d, want 0", entry.Offset)
	}
	if entry.TS != msg.TS {
		t.Errorf("entry.TS = %d, want %d", entry.TS, msg.TS)
	}
	if entry.File != l.path {
		t.Errorf("entry.File = %q, want %q", entry.File, l.path)
	}

	got, err := l.Read(entry.Offset)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.ID != msg.ID {
		t.Errorf("ID = %q, want %q", got.ID, msg.ID)
	}
	if got.TS != msg.TS {
		t.Errorf("TS = %d, want %d", got.TS, msg.TS)
	}
	if string(got.Data) != string(msg.Data) {
		t.Errorf("Data = %q, want %q", got.Data, msg.Data)
	}
}

func TestAppendMultipleAndRange(t *testing.T) {
	l, cleanup := newTestLog(t)
	defer cleanup()

	msgs := []Msg{
		{ID: "a", TS: 1, Data: []byte("first")},
		{ID: "b", TS: 2, Data: []byte("second")},
		{ID: "c", TS: 3, Data: []byte("third")},
	}
	entries := make([]IndexEntry, len(msgs))
	for i, m := range msgs {
		e, err := l.Append(m)
		if err != nil {
			t.Fatalf("Append[%d]: %v", i, err)
		}
		entries[i] = e
	}

	// Verify each can be read back by offset.
	for i, e := range entries {
		got, err := l.Read(e.Offset)
		if err != nil {
			t.Fatalf("Read[%d]: %v", i, err)
		}
		if got.ID != msgs[i].ID {
			t.Errorf("Read[%d] ID = %q, want %q", i, got.ID, msgs[i].ID)
		}
	}

	// Range should visit all three in order.
	var visited []string
	err := l.Range(func(id string, entry IndexEntry) error {
		visited = append(visited, id)
		return nil
	})
	if err != nil {
		t.Fatalf("Range: %v", err)
	}
	if len(visited) != len(msgs) {
		t.Fatalf("Range visited %d entries, want %d", len(visited), len(msgs))
	}
	for i, id := range visited {
		if id != msgs[i].ID {
			t.Errorf("Range[%d] ID = %q, want %q", i, id, msgs[i].ID)
		}
	}
}

func TestRangeReportsCorrectOffsets(t *testing.T) {
	l, cleanup := newTestLog(t)
	defer cleanup()

	msgs := []Msg{
		{ID: "x", TS: 10, Data: []byte("foo")},
		{ID: "y", TS: 20, Data: []byte("bar")},
	}
	appended := make([]IndexEntry, len(msgs))
	for i, m := range msgs {
		e, err := l.Append(m)
		if err != nil {
			t.Fatal(err)
		}
		appended[i] = e
	}

	i := 0
	l.Range(func(id string, entry IndexEntry) error {
		if entry.Offset != appended[i].Offset {
			t.Errorf("Range[%d] offset = %d, want %d", i, entry.Offset, appended[i].Offset)
		}
		i++
		return nil
	})
}

func TestAppendRejectsLongID(t *testing.T) {
	l, cleanup := newTestLog(t)
	defer cleanup()

	msg := Msg{ID: string(make([]byte, 257)), TS: 1, Data: []byte("data")}
	_, err := l.Append(msg)
	if err == nil {
		t.Fatal("expected error for ID > 256 bytes, got nil")
	}
}

func TestAppendEmptyData(t *testing.T) {
	l, cleanup := newTestLog(t)
	defer cleanup()

	msg := Msg{ID: "empty", TS: 99, Data: []byte{}}
	entry, err := l.Append(msg)
	if err != nil {
		t.Fatalf("Append empty data: %v", err)
	}
	got, err := l.Read(entry.Offset)
	if err != nil {
		t.Fatalf("Read empty data: %v", err)
	}
	if len(got.Data) != 0 {
		t.Errorf("expected empty data, got %d bytes", len(got.Data))
	}
}

func TestReadCorruptedHash(t *testing.T) {
	l, cleanup := newTestLog(t)
	defer cleanup()

	msg := Msg{ID: "corrupt", TS: 1, Data: []byte("original data")}
	entry, err := l.Append(msg)
	if err != nil {
		t.Fatal(err)
	}

	// Flip a byte in the data portion (last byte of file).
	info, err := l.f.Stat()
	if err != nil {
		t.Fatal(err)
	}
	corruptOffset := info.Size() - 1
	buf := make([]byte, 1)
	l.f.ReadAt(buf, corruptOffset)
	buf[0] ^= 0xFF
	l.f.WriteAt(buf, corruptOffset)

	_, err = l.Read(entry.Offset)
	if err == nil {
		t.Fatal("expected hash mismatch error after corruption, got nil")
	}
}

func TestRangeEmptyLog(t *testing.T) {
	l, cleanup := newTestLog(t)
	defer cleanup()

	count := 0
	err := l.Range(func(id string, entry IndexEntry) error {
		count++
		return nil
	})
	if err != nil {
		t.Fatalf("Range on empty log: %v", err)
	}
	if count != 0 {
		t.Errorf("Range on empty log visited %d entries", count)
	}
}
