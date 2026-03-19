package gossip

import (
	"os"
	"testing"
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
	if err := s.Add(msg); err != nil {
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
	s.Add(Msg{ID: "x", TS: 1, Data: []byte("first")})
	s.Add(Msg{ID: "x", TS: 2, Data: []byte("second")})
	if entry := s.index["x"]; entry.TS != 2 {
		t.Errorf("expected TS=2, got %d", entry.TS)
	}
}

func TestAddLowerTSIgnored(t *testing.T) {
	s := newTestService(t)
	s.Add(Msg{ID: "x", TS: 10, Data: []byte("newer")})
	s.Add(Msg{ID: "x", TS: 5, Data: []byte("older")})
	if entry := s.index["x"]; entry.TS != 10 {
		t.Errorf("expected index to keep TS=10, got %d", entry.TS)
	}
}

func TestAddEqualTSIgnored(t *testing.T) {
	s := newTestService(t)
	s.Add(Msg{ID: "x", TS: 10, Data: []byte("first")})
	s.Add(Msg{ID: "x", TS: 10, Data: []byte("duplicate")})
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
		if err := s1.Add(m); err != nil {
			t.Fatal(err)
		}
	}
	s1.log.f.Close()

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
	olderFile, err := os.Create(olderPath)
	if err != nil {
		t.Fatal(err)
	}
	olderLog := &Log{path: olderPath, f: olderFile}
	if _, err := olderLog.Append(Msg{ID: "same", TS: 10, Data: []byte("older")}); err != nil {
		t.Fatal(err)
	}
	if err := olderFile.Close(); err != nil {
		t.Fatal(err)
	}

	newerPath := dir + "/z-newer.bin"
	newerFile, err := os.Create(newerPath)
	if err != nil {
		t.Fatal(err)
	}
	newerLog := &Log{path: newerPath, f: newerFile}
	newerMsg := Msg{ID: "same", TS: 20, Data: []byte("newer")}
	newerEntry, err := newerLog.Append(newerMsg)
	if err != nil {
		t.Fatal(err)
	}
	if err := newerFile.Close(); err != nil {
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

func TestInitIndexEntriesPointToReadableData(t *testing.T) {
	dir := t.TempDir()

	s1 := &Service{LogsFolder: dir}
	if err := s1.Init(); err != nil {
		t.Fatal(err)
	}
	want := Msg{ID: "readable", TS: 42, Data: []byte("some data")}
	if err := s1.Add(want); err != nil {
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

	osf, err := os.Open(entry.File)
	if err != nil {
		t.Fatal(err)
	}
	defer osf.Close()
	f := &Log{path: entry.File, f: osf}

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
	if err := s1.Add(Msg{ID: "corrupt", TS: 1, Data: []byte("original data")}); err != nil {
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
