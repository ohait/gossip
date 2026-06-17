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
	// TODO: this single mutex serializes replay-visible state, appends, and broadcasts.
	// It is simple for now, but it will become a throughput bottleneck under write load.
	m     sync.Mutex
	index map[string]IndexEntry
	log   *Log

	Clients map[string]chan<- Outbound
}

type Outbound struct {
	Cmd byte
	Msg Msg
}

func (s *Service) Init() error {
	s.index = make(map[string]IndexEntry)
	s.Clients = make(map[string]chan<- Outbound)
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
	old := 0
	tot := 0
	t0 := time.Now()
	replayNow := t0.UnixNano()

	oldFiles := []string{}
	for _, file := range files {
		size := 0
		if info, _ := file.Info(); info != nil {
			size = int(info.Size())
		}
		log.Printf("scanning %s (%d bytes)\n", file.Name(), size)
		if filepath.Ext(file.Name()) == ".bin" {
			oldFiles = append(oldFiles, file.Name())
			path := filepath.Join(s.LogsFolder, file.Name())
			lg, err := OpenLog(path)
			if err != nil {
				return err
			}
			err = lg.Range(func(id string, entry IndexEntry) error {
				if entry.TS > replayNow {
					log.Printf("message ID %s has future timestamp %d during replay (now: %d)\n", id, entry.TS, replayNow)
				}
				prev, ok := s.index[id]
				if ok {
					old++
					if prev.TS <= entry.TS {
						s.index[id] = entry
					}
				} else {
					tot++
					s.index[id] = entry
				}
				return nil
			})
			lg.Close()
			if err != nil {
				return fmt.Errorf("replaying %s: %w", path, err)
			}
		}
	}
	log.Printf("replayed %d messages (%d old) in %s\n", tot, old, time.Since(t0))
	if old < 10 {
		return nil
	}

	err = s.replay(0, func(msg Msg) error {
		if s.log == nil {
			path := filepath.Join(s.LogsFolder, fmt.Sprintf("log-%x.bin", time.Now().UnixNano()))
			var err error
			s.log, err = CreateLog(path)
			if err != nil {
				return err
			}
		}
		entry, err := s.log.Append(msg)
		if err != nil {
			return err
		}
		s.index[msg.ID] = entry
		if err := s.log.Flush(); err != nil {
			return err
		}
		if entry.Offset > 200*1024*1024 {
			if err := s.log.Close(); err != nil {
				return err
			}
			s.log = nil
		}
		return nil
	})
	if err != nil {
		log.Printf("Error compacting messages: %v", err)
		return err
	}
	if s.log != nil {
		if err := s.log.Close(); err != nil {
			return err
		}
		s.log = nil
	}
	log.Printf("compacted messages")

	for _, file := range oldFiles {
		err := os.Remove(filepath.Join(s.LogsFolder, file))
		if err != nil {
			return err
		}
	}

	log.Printf("compacted %d messages in %s\n", tot, time.Since(t0))
	return nil
}

type IndexEntry struct {
	TS     int64
	File   string
	Offset int64
}

func (s *Service) Add(msg Msg, cas bool) error {
	s.m.Lock()
	defer s.m.Unlock()
	if err := validateMsg(msg); err != nil {
		return err
	}
	prev := s.index[msg.ID]
	if cas {
		prevTS := int64(0)
		if prev.File != "" {
			prevTS = prev.TS
		}
		if prevTS != msg.TS {
			return fmt.Errorf("CAS failed: existing TS %d does not match expected TS %d", prevTS, msg.TS)
		}
		msg.TS = time.Now().UnixNano()
	} else {
		if prev.File != "" {
			if prev.TS >= msg.TS {
				log.Printf("Duplicate message ID %s with older timestamp %d (existing TS: %d)\n", msg.ID, msg.TS, prev.TS)
				return nil
			}
		}
	}
	if s.log == nil {
		path := filepath.Join(s.LogsFolder, fmt.Sprintf("log-%x.bin", time.Now().UnixNano()))
		var err error
		s.log, err = CreateLog(path)
		if err != nil {
			return err
		}
	}
	entry, err := s.log.Append(msg)
	if err != nil {
		return err
	}
	s.log.Flush()
	if entry.Offset > 200*1024*1024 {
		s.log.Close()
		s.log = nil
	}
	//prev := s.index[id]
	//if prev.File != "" {
	//}
	s.index[msg.ID] = entry
	s.broadcast(CmdLWW, msg)
	return nil
}

func (s *Service) Signal(msg Msg) error {
	if err := validateMsg(msg); err != nil {
		return err
	}
	s.m.Lock()
	defer s.m.Unlock()
	s.broadcast(CmdSignal, msg)
	return nil
}

func validateMsg(msg Msg) error {
	if msg.TS >= time.Now().Add(time.Second).UnixNano() {
		return fmt.Errorf("message timestamp %d is too far in the future", msg.TS)
	}
	if len(msg.ID) > 256 {
		return fmt.Errorf("id length %d exceeds maximum allowed length 256", len(msg.ID))
	}
	if len(msg.Data) > 1024*1024*1024 {
		return fmt.Errorf("data length %d exceeds maximum allowed length 1GB", len(msg.Data))
	}
	return nil
}

func (s *Service) broadcast(cmd byte, msg Msg) {
	for _, inbox := range s.Clients {
		select {
		case inbox <- Outbound{Cmd: cmd, Msg: msg}:
		default:
		}
	}
}
