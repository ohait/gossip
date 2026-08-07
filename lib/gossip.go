package lib

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	maxLogSize = 200 * 1024 * 1024
	maxIDLen   = 1024
	maxDataLen = 1024 * 1024 * 1024
)

var (
	ErrNotInitialized = errors.New("gossip: not initialized")
	ErrCASConflict    = errors.New("gossip: CAS conflict")
)

type Gossip struct {
	// LogsFolder holds the .bin log files; defaults to "logs".
	LogsFolder string

	m     sync.Mutex
	index map[string]IndexEntry
	log   *Log // current append target; nil until the first append
	cb    func(topic, id string, ts int64, data []byte) error
}

var _ Client = (*Gossip)(nil)

type IndexEntry struct {
	TS     int64
	File   string
	Offset int64
}

func (g *Gossip) Init(cb func(topic, id string, ts int64, data []byte) error) error {
	if err := g.buildIndex(); err != nil {
		return err
	}

	if err := g.compact(); err != nil {
		return err
	}

	if cb == nil {
		return nil
	}

	g.cb = cb

	return nil
}

func (g *Gossip) PublishCAS(topic, id string, ts int64, data []byte) error {
	if g.index == nil {
		return ErrNotInitialized
	}

	msg := Msg{Topic: topic, ID: id, TS: ts, Data: data}
	if err := validateFields(msg); err != nil {
		return err
	}
	committed, err := g.commitCAS(msg)
	if err != nil {
		return err
	}
	return g.notify(committed)
}

func (g *Gossip) Signal(topic, id string, ts int64, data []byte) error {
	if g.index == nil {
		return ErrNotInitialized
	}

	msg := Msg{Topic: topic, ID: id, TS: ts, Data: data}
	if err := validateFields(msg); err != nil {
		return err
	}
	return g.notify(msg)
}

func (g *Gossip) Replay(since int64, f func(Msg) error) error {
	files := map[string]struct{}{}
	current := map[string]int64{}
	g.m.Lock()
	for id, entry := range g.index {
		if entry.TS >= since {
			current[id] = entry.TS
			files[entry.File] = struct{}{}
		}
	}
	g.m.Unlock()

	for file := range files {
		l, err := OpenLog(file)
		if err != nil {
			return err
		}
		err = l.RangeSince(since, func(msg Msg) error {
			if msg.TS != current[msg.ID] {
				return nil // a superseded version of this ID
			}
			return f(msg)
		})
		l.Close()
		if err != nil {
			return err
		}
	}
	return nil
}

func (g *Gossip) buildIndex() error {
	if g.index != nil {
		return fmt.Errorf("gossip: already initialized")
	}
	if g.LogsFolder == "" {
		g.LogsFolder = "logs"
	}
	if err := os.MkdirAll(g.LogsFolder, 0o755); err != nil {
		return err
	}
	files, err := os.ReadDir(g.LogsFolder)
	if err != nil {
		return err
	}

	g.index = make(map[string]IndexEntry)

	for _, file := range files {
		if filepath.Ext(file.Name()) != ".bin" {
			continue
		}
		path := filepath.Join(g.LogsFolder, file.Name())
		if err := g.indexFile(path); err != nil {
			// Leave the store uninitialized rather than half-indexed: a partial
			// index would silently report the wrong previous TS to CAS.
			g.index = nil
			return fmt.Errorf("indexing %s: %w", path, err)
		}
	}

	return nil
}

func (g *Gossip) indexFile(path string) error {
	lg, err := OpenLog(path)
	if err != nil {
		return err
	}
	defer lg.Close()

	return lg.Range(func(id string, entry IndexEntry) error {
		if prev, ok := g.index[id]; ok && prev.TS > entry.TS {
			return nil // an older version of an ID we have already indexed
		}
		g.index[id] = entry
		return nil
	})
}

func (g *Gossip) commitCAS(msg Msg) (Msg, error) {
	g.m.Lock()
	defer g.m.Unlock()

	var prevTS int64
	if prev, ok := g.index[msg.ID]; ok {
		prevTS = prev.TS
	}
	if prevTS != msg.TS {
		return Msg{}, fmt.Errorf("%w: %q holds TS %d, caller expected %d",
			ErrCASConflict, msg.ID, prevTS, msg.TS)
	}

	msg.TS = time.Now().UnixNano()
	if msg.TS <= prevTS {
		msg.TS = prevTS + 1
	}
	if err := g.commit(msg); err != nil {
		return Msg{}, err
	}
	return msg, nil
}

func (g *Gossip) commit(msg Msg) error {
	if g.log == nil {
		path := filepath.Join(g.LogsFolder, fmt.Sprintf("log-%x.bin", time.Now().UnixNano()))
		lg, err := CreateLog(path)
		if err != nil {
			return err
		}
		g.log = lg
	}
	entry, err := g.log.Append(msg)
	if err != nil {
		return err
	}

	if err := g.log.Flush(); err != nil {
		return err
	}

	g.index[msg.ID] = entry

	if entry.Offset > maxLogSize {
		err := g.log.Close()
		g.log = nil
		return err
	}
	return nil
}

func (g *Gossip) notify(msg Msg) error {
	if g.cb == nil {
		return nil
	}

	return g.cb(msg.Topic, msg.ID, msg.TS, msg.Data)
}

func (g *Gossip) compact() error {
	if g.log != nil {
		if err := g.log.Close(); err != nil {
			return err
		}
		g.log = nil
	}

	replaced, err := g.readLogFiles()
	if err != nil {
		return err
	}
	if len(replaced) == 0 {
		return nil
	}

	current := make(map[string]int64, len(g.index))
	sources := make(map[string]struct{})

	for id, entry := range g.index {
		current[id] = entry.TS
		sources[entry.File] = struct{}{}
	}

	for file := range sources {
		l, err := OpenLog(file)
		if err != nil {
			return err
		}
		err = l.RangeSince(0, func(msg Msg) error {
			if msg.TS != current[msg.ID] {
				return nil // a superseded version
			}
			return g.commit(msg)
		})
		l.Close()
		if err != nil {
			return err
		}
	}
	if g.log != nil {
		if err := g.log.Close(); err != nil {
			return err
		}
		g.log = nil
	}

	for _, file := range replaced {
		if err := os.Remove(file); err != nil {
			return err
		}
	}
	return nil
}

func (g *Gossip) readLogFiles() ([]string, error) {
	entries, err := os.ReadDir(g.LogsFolder)
	if err != nil {
		return nil, err
	}
	var out []string
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".bin" {
			out = append(out, filepath.Join(g.LogsFolder, e.Name()))
		}
	}
	return out, nil
}

func validateFields(msg Msg) error {
	if len(msg.ID) > maxIDLen {
		return fmt.Errorf("id length %d exceeds maximum allowed length %d", len(msg.ID), maxIDLen)
	}
	if len(msg.Data) > maxDataLen {
		return fmt.Errorf("data length %d exceeds maximum allowed length 1GB", len(msg.Data))
	}
	return nil
}
