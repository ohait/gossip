package gossip

import (
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

type Service struct {
	LogsFolder string
	MaxData    int
	m          sync.Mutex
	index      map[string]IndexEntry
	log        *Log

	clients map[string]chan<- *Msg
}

func (s *Service) Init() error {
	s.index = make(map[string]IndexEntry)
	s.clients = make(map[string]chan<- *Msg)
	if s.LogsFolder == "" {
		s.LogsFolder = "logs"
	}
	if s.MaxData == 0 {
		s.MaxData = 10 * 1024 * 1024
	}
	if s.MaxData > 1024*1024*1024 {
		return fmt.Errorf("MaxData %d exceeds maximum allowed value 1GB", s.MaxData)
	}
	err := os.MkdirAll(s.LogsFolder, 0o755)
	if err != nil {
		return err
	}

	// scan all the .bin files in the logs folder
	files, err := os.ReadDir(s.LogsFolder)
	if err != nil {
		return err
	}
	for _, file := range files {
		if filepath.Ext(file.Name()) == ".bin" {
			path := filepath.Join(s.LogsFolder, file.Name())
			f, err := os.Open(path)
			if err != nil {
				return err
			}
			log := &Log{path: path, f: f}
			err = log.Range(func(id string, entry IndexEntry) error {
				s.index[id] = entry
				return nil
			})
			f.Close()
			if err != nil {
				return fmt.Errorf("replaying %s: %w", path, err)
			}
		}
	}
	return nil
}

type IndexEntry struct {
	TS     int
	File   string
	Offset int64
}

func (s *Service) Add(msg Msg) error {
	s.m.Lock()
	defer s.m.Unlock()
	prev := s.index[msg.ID]
	if prev.File != "" {
		if prev.TS >= int(msg.TS) {
			log.Printf("Duplicate message ID %s with older timestamp %d (existing TS: %d)\n", msg.ID, msg.TS, prev.TS)
			return nil
		}
	}
	if s.log == nil {
		path := filepath.Join(s.LogsFolder, fmt.Sprintf("log-%x.bin", time.Now().UnixNano()))
		f, err := os.Create(path)
		if err != nil {
			return err
		}
		s.log = &Log{path: path, f: f}
	}
	entry, err := s.log.Append(msg)
	if err != nil {
		return err
	}
	if entry.Offset > 200*1024*1024 {
		s.log.f.Close()
		s.log = nil
	} else {
		s.log.f.Sync() // Ensure data is flushed to disk
	}
	//prev := s.index[id]
	//if prev.File != "" {
	//}
	s.index[msg.ID] = entry
	s.broadcast(msg)
	return nil
}

func (s *Service) broadcast(msg Msg) {
	for _, inbox := range s.clients {
		select {
		case inbox <- &msg:
		default:
		}
	}
}
