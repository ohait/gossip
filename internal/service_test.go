package gossip

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func newTestService(t *testing.T) *Service {
	t.Helper()
	s := &Service{LogsFolder: t.TempDir()}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	return s
}

func TestInitEmptyFolder(t *testing.T) {
	s := newTestService(t)
	if len(s.index) != 0 {
		t.Errorf("expected empty index, got %d entries", len(s.index))
	}
}

func TestAddStoresInIndex(t *testing.T) {
	s := newTestService(t)
	msg := Msg{ID: "msg-1", TS: 100, Data: []byte("hello")}
	if err := s.Add(msg, false); err != nil {
		t.Fatal(err)
	}
	entry, ok := s.index["msg-1"]
	if !ok {
		t.Fatal("no index entry for msg-1")
	}
	if entry.TS != 100 {
		t.Errorf("entry.TS = %d, want 100", entry.TS)
	}
	if entry.File == "" {
		t.Error("entry.File is empty")
	}
}

func TestAddHigherTSWins(t *testing.T) {
	s := newTestService(t)
	s.Add(Msg{ID: "x", TS: 1, Data: []byte("first")}, false)
	s.Add(Msg{ID: "x", TS: 2, Data: []byte("second")}, false)
	if entry := s.index["x"]; entry.TS != 2 {
		t.Errorf("expected TS=2, got %d", entry.TS)
	}
}

func TestAddLowerTSIgnored(t *testing.T) {
	s := newTestService(t)
	s.Add(Msg{ID: "x", TS: 10, Data: []byte("newer")}, false)
	s.Add(Msg{ID: "x", TS: 5, Data: []byte("older")}, false)
	if entry := s.index["x"]; entry.TS != 10 {
		t.Errorf("expected index to keep TS=10, got %d", entry.TS)
	}
}

func TestAddEqualTSIgnored(t *testing.T) {
	s := newTestService(t)
	s.Add(Msg{ID: "x", TS: 10, Data: []byte("first")}, false)
	s.Add(Msg{ID: "x", TS: 10, Data: []byte("duplicate")}, false)
	if entry := s.index["x"]; entry.TS != 10 {
		t.Errorf("expected index to keep TS=10, got %d", entry.TS)
	}
	// only one entry should have been written to the log
	count := 0
	s.log.Range(func(id string, _ IndexEntry) error {
		if id == "x" {
			count++
		}
		return nil
	})
	if count != 1 {
		t.Errorf("expected 1 log entry for id=x, got %d", count)
	}
}

func TestAddCASSucceedsAndAssignsNewTS(t *testing.T) {
	s := newTestService(t)

	initial := Msg{ID: "cas-1", TS: time.Now().Add(-time.Second).UnixNano(), Data: []byte("first")}
	if err := s.Add(initial, false); err != nil {
		t.Fatalf("Add(initial): %v", err)
	}

	prev := s.index[initial.ID]
	if err := s.Add(Msg{ID: initial.ID, TS: prev.TS, Data: []byte("second")}, true); err != nil {
		t.Fatalf("Add(CAS): %v", err)
	}

	entry := s.index[initial.ID]
	if entry.TS == prev.TS {
		t.Fatal("CAS should assign a new timestamp")
	}
	if entry.TS <= prev.TS {
		t.Fatalf("entry.TS = %d, want > %d", entry.TS, prev.TS)
	}
}

func TestAddCASFailsOnMismatchedTS(t *testing.T) {
	s := newTestService(t)

	initial := Msg{ID: "cas-1", TS: time.Now().Add(-time.Second).UnixNano(), Data: []byte("first")}
	if err := s.Add(initial, false); err != nil {
		t.Fatalf("Add(initial): %v", err)
	}

	prev := s.index[initial.ID]
	err := s.Add(Msg{ID: initial.ID, TS: prev.TS + 1, Data: []byte("second")}, true)
	if err == nil {
		t.Fatal("Add(CAS) unexpectedly succeeded")
	}
	if !strings.Contains(err.Error(), "CAS failed") {
		t.Fatalf("error = %v, want CAS failure", err)
	}
	if got := s.index[initial.ID].TS; got != prev.TS {
		t.Fatalf("index TS = %d, want %d", got, prev.TS)
	}
}

func TestSignalBroadcastsWithoutPersisting(t *testing.T) {
	s := newTestService(t)
	inbox := make(chan Outbound, 1)
	s.m.Lock()
	s.Clients["test"] = inbox
	s.m.Unlock()

	msg := Msg{ID: "sig-1", TS: 100, Data: []byte("hello")}
	if err := s.Signal(msg); err != nil {
		t.Fatal(err)
	}

	select {
	case got := <-inbox:
		if got.Cmd != CmdSignal {
			t.Fatalf("cmd = %q, want %q", got.Cmd, CmdSignal)
		}
		if got.Msg.ID != msg.ID || got.Msg.TS != msg.TS || string(got.Msg.Data) != string(msg.Data) {
			t.Fatalf("got %+v, want %+v", got.Msg, msg)
		}
	default:
		t.Fatal("signal was not broadcast")
	}

	if len(s.index) != 0 {
		t.Fatalf("signal should not touch index, got %d entries", len(s.index))
	}
	if s.log != nil {
		t.Fatal("signal should not create or append to a log")
	}
}

func TestInitRebuildsIndex(t *testing.T) {
	dir := t.TempDir()

	s1 := &Service{LogsFolder: dir}
	if err := s1.Init(); err != nil {
		t.Fatal(err)
	}
	msgs := []Msg{
		{ID: "a", TS: 10, Data: []byte("alpha")},
		{ID: "b", TS: 20, Data: []byte("beta")},
		{ID: "c", TS: 30, Data: []byte("gamma")},
	}
	for _, m := range msgs {
		if err := s1.Add(m, false); err != nil {
			t.Fatal(err)
		}
	}
	s1.log.Close()

	s2 := &Service{LogsFolder: dir}
	if err := s2.Init(); err != nil {
		t.Fatal(err)
	}
	if len(s2.index) != len(msgs) {
		t.Fatalf("index has %d entries, want %d", len(s2.index), len(msgs))
	}
	for _, m := range msgs {
		entry, ok := s2.index[m.ID]
		if !ok {
			t.Errorf("missing index entry for %q after replay", m.ID)
			continue
		}
		if entry.TS != m.TS {
			t.Errorf("index[%q].TS = %d, want %d", m.ID, entry.TS, int(m.TS))
		}
	}
}

func TestInitKeepsHighestTSAcrossLogs(t *testing.T) {
	dir := t.TempDir()

	olderPath := dir + "/a-older.bin"
	olderLog, err := CreateLog(olderPath)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := olderLog.Append(Msg{ID: "same", TS: 10, Data: []byte("older")}); err != nil {
		t.Fatal(err)
	}
	if err := olderLog.Close(); err != nil {
		t.Fatal(err)
	}

	newerPath := dir + "/z-newer.bin"
	newerLog, err := CreateLog(newerPath)
	if err != nil {
		t.Fatal(err)
	}
	newerMsg := Msg{ID: "same", TS: 20, Data: []byte("newer")}
	newerEntry, err := newerLog.Append(newerMsg)
	if err != nil {
		t.Fatal(err)
	}
	if err := newerLog.Close(); err != nil {
		t.Fatal(err)
	}

	s := &Service{LogsFolder: dir}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}

	entry, ok := s.index["same"]
	if !ok {
		t.Fatal("missing index entry after replay")
	}
	if entry.TS != newerMsg.TS {
		t.Fatalf("entry.TS = %d, want %d", entry.TS, newerMsg.TS)
	}
	if entry.File != newerEntry.File || entry.Offset != newerEntry.Offset {
		t.Fatalf("entry = %+v, want file=%q offset=%d", entry, newerEntry.File, newerEntry.Offset)
	}
}

func TestInitCompactsOldEntries(t *testing.T) {
	dir := t.TempDir()

	oldPath := filepath.Join(dir, "a-old.bin")
	oldLog, err := CreateLog(oldPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		if _, err := oldLog.Append(Msg{ID: fmt.Sprintf("stale-%02d", i), TS: int64(i + 1), Data: []byte("old")}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 3; i++ {
		if _, err := oldLog.Append(Msg{ID: fmt.Sprintf("keep-%02d", i), TS: int64(i + 1), Data: []byte("keep")}); err != nil {
			t.Fatal(err)
		}
	}
	if err := oldLog.Close(); err != nil {
		t.Fatal(err)
	}

	newPath := filepath.Join(dir, "z-new.bin")
	newLog, err := CreateLog(newPath)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 10; i++ {
		msg := Msg{ID: fmt.Sprintf("stale-%02d", i), TS: int64(100 + i), Data: []byte("new")}
		if _, err := newLog.Append(msg); err != nil {
			t.Fatal(err)
		}
	}
	if err := newLog.Close(); err != nil {
		t.Fatal(err)
	}

	s := &Service{LogsFolder: dir}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}

	files, err := filepath.Glob(filepath.Join(dir, "*.bin"))
	if err != nil {
		t.Fatal(err)
	}
	if len(files) != 1 {
		t.Fatalf("got %d log files after compaction, want 1: %v", len(files), files)
	}
	if files[0] == oldPath || files[0] == newPath {
		t.Fatalf("compaction kept original log file %q", files[0])
	}

	compacted, err := OpenLog(files[0])
	if err != nil {
		t.Fatal(err)
	}
	defer compacted.Close()

	seen := map[string]Msg{}
	if err := compacted.Range(func(id string, entry IndexEntry) error {
		msg, err := compacted.Read(entry.Offset)
		if err != nil {
			return err
		}
		seen[id] = msg
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	if len(seen) != 13 {
		t.Fatalf("compacted log has %d entries, want 13", len(seen))
	}
	for i := 0; i < 10; i++ {
		id := fmt.Sprintf("stale-%02d", i)
		msg, ok := seen[id]
		if !ok {
			t.Fatalf("missing compacted entry %q", id)
		}
		if msg.TS != int64(100+i) || string(msg.Data) != "new" {
			t.Fatalf("%s = %+v, want TS=%d data=new", id, msg, 100+i)
		}
	}
	for i := 0; i < 3; i++ {
		id := fmt.Sprintf("keep-%02d", i)
		msg, ok := seen[id]
		if !ok {
			t.Fatalf("missing compacted entry %q", id)
		}
		if msg.TS != int64(i+1) || string(msg.Data) != "keep" {
			t.Fatalf("%s = %+v, want TS=%d data=keep", id, msg, i+1)
		}
	}
	for id, entry := range s.index {
		if entry.File != files[0] {
			t.Fatalf("index[%q].File = %q, want compacted file %q", id, entry.File, files[0])
		}
	}
}

func TestInitIndexEntriesPointToReadableData(t *testing.T) {
	dir := t.TempDir()

	s1 := &Service{LogsFolder: dir}
	if err := s1.Init(); err != nil {
		t.Fatal(err)
	}
	want := Msg{ID: "readable", TS: 42, Data: []byte("some data")}
	if err := s1.Add(want, false); err != nil {
		t.Fatal(err)
	}
	s1.log.f.Close()

	s2 := &Service{LogsFolder: dir}
	if err := s2.Init(); err != nil {
		t.Fatal(err)
	}
	entry, ok := s2.index[want.ID]
	if !ok {
		t.Fatal("missing index entry after replay")
	}

	f, err := OpenLog(entry.File)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	got, err := f.Read(entry.Offset)
	if err != nil {
		t.Fatalf("Read: %v", err)
	}
	if got.ID != want.ID || got.TS != want.TS || string(got.Data) != string(want.Data) {
		t.Errorf("got %+v, want %+v", got, want)
	}
}

func TestInitFailsOnCorruptedLogData(t *testing.T) {
	dir := t.TempDir()

	s1 := &Service{LogsFolder: dir}
	if err := s1.Init(); err != nil {
		t.Fatal(err)
	}
	if err := s1.Add(Msg{ID: "corrupt", TS: 1, Data: []byte("original data")}, false); err != nil {
		t.Fatal(err)
	}
	if err := s1.log.f.Close(); err != nil {
		t.Fatal(err)
	}

	info, err := os.Stat(s1.log.path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := os.OpenFile(s1.log.path, os.O_RDWR, 0)
	if err != nil {
		t.Fatal(err)
	}
	buf := make([]byte, 1)
	corruptOffset := info.Size() - 1
	if _, err := f.ReadAt(buf, corruptOffset); err != nil {
		t.Fatal(err)
	}
	buf[0] ^= 0xFF
	if _, err := f.WriteAt(buf, corruptOffset); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}

	s2 := &Service{LogsFolder: dir}
	if err := s2.Init(); err == nil {
		t.Fatal("expected Init to fail on corrupted log data")
	}
}
