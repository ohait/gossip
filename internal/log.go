package gossip

import (
	"errors"
	"fmt"
	"io"
	"os"

	xxhash "github.com/cespare/xxhash/v2"
)

type Log struct {
	path string
	f    *os.File
	v    int
}

func CreateLog(path string) (*Log, error) {
	f, err := os.Create(path)
	if err != nil {
		return nil, err
	}
	_, err = f.Write([]byte("GSP1"))
	if err != nil {
		f.Close()
		return nil, err
	}
	return &Log{path: path, f: f, v: 1}, nil
}

// open a pre-existing log (cannot be empty)
func OpenLog(path string) (*Log, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	var magic [4]byte
	ct, err := f.Read(magic[:])
	if err != nil {
		return nil, err // can be EOF
	}
	if ct != 4 {
		return nil, fmt.Errorf("invalid log file: %s", path)
	}
	if string(magic[:]) == "GSP1" {
		return &Log{path: path, f: f, v: 1}, nil
	}
	if magic[0] == 0 && magic[1] == 0 && magic[2] == 0 && magic[3] == 0 {
		// old log format without magic header, assume v0
		_, err := f.Seek(0, 0) // reset to start
		return &Log{path: path, f: f}, err
	}
	return nil, fmt.Errorf("unsupported log file format: %s", path)
}

// AppendLog opens an existing log file for both reading and appending,
// creating it if it does not exist. Use this when you need to replay
// existing entries and then continue writing new ones.
func AppendLog(path string) (*Log, error) {
	l, err := OpenLog(path)
	if errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) {
		return CreateLog(path)
	}
	if err != nil {
		return nil, err
	}
	switch l.v {
	case 0:
		defer l.Close()
		// do migration
		l2, err := CreateLog(path + ".new")
		if err != nil {
			return nil, fmt.Errorf("creating new log for migration: %w", err)
		}
		err = l.RangeSince(0, func(msg Msg) error {
			_, err := l2.Append(msg)
			return err
		})
		if err != nil {
			l2.Close()
			return nil, err
		}
		err = os.Rename(path+".new", path)
		if err != nil {
			l2.Close()
			os.Remove(path + ".new")
			return nil, err
		}
		l2.path = path
		return l2, nil
	default:
		// reopen readwrite
		l.f.Close()
		l.f, err = os.OpenFile(path, os.O_RDWR, 0)
		return l, err
	}
}

func (l *Log) readNext() (offset int64, msg Msg, err error) {
	offset, err = l.f.Seek(0, io.SeekCurrent)
	if err != nil {
		return
	}
	var hash, length uint64
	switch l.v {
	case 0:
		msg.ID, err = ReadString(l.f, 256)
		if err != nil {
			return
		}
		msg.TS, err = ReadInt64(l.f)
		if err != nil {
			return
		}
		hash, err = ReadUint64(l.f)
		if err != nil {
			return
		}
		length, err = ReadUint64(l.f)
		if err != nil {
			return
		}
		if length > 1024*1024*1024 {
			err = fmt.Errorf("data length %d exceeds maximum allowed length 1GB", length)
			return
		}
		msg.Data = make([]byte, length)
		_, err = io.ReadFull(l.f, msg.Data)
		if err != nil {
			return
		}
		if verifyHash := xxhash.Sum64(msg.Data); verifyHash != hash {
			err = fmt.Errorf("data hash mismatch for %s: expected %016x, got %016x", msg.ID, hash, verifyHash)
		}
	case 1:
		msg.Topic, err = ReadID(l.f)
		if err != nil {
			return
		}
		msg.ID, err = ReadID(l.f)
		if err != nil {
			return
		}
		msg.TS, err = ReadInt64(l.f)
		if err != nil {
			return
		}
		hash, err = ReadUint64(l.f)
		if err != nil {
			return
		}
		length, err = ReadUint64(l.f)
		if err != nil {
			return
		}
		if length > 1024*1024*1024 {
			err = fmt.Errorf("data length %d exceeds maximum allowed length 1GB", length)
			return
		}
		msg.Data = make([]byte, length)
		_, err = io.ReadFull(l.f, msg.Data)
		if err != nil {
			return
		}
		if verifyHash := xxhash.Sum64(msg.Data); verifyHash != hash {
			err = fmt.Errorf("data hash mismatch for %s: expected %016x, got %016x", msg.ID, hash, verifyHash)
		}
	default:
		err = fmt.Errorf("unsupported log version %d", l.v)
	}
	return
}

func (l *Log) Close() error {
	return l.f.Close()
}

func (l *Log) Flush() error {
	return l.f.Sync()
}

func (l *Log) LastTS(id string) int64 {
	var lastTS int64
	l.Range(func(eid string, entry IndexEntry) error {
		if eid == id && entry.TS > lastTS {
			lastTS = entry.TS
		}
		return nil
	})
	return lastTS
}

func (l *Log) Append(msg Msg) (entry IndexEntry, err error) {
	// only v1 should get here
	entry.TS = msg.TS
	entry.File = l.path
	if len(msg.ID) > 1024 {
		return entry, fmt.Errorf("id length %d exceeds maximum allowed length 1024", len(msg.ID))
	}
	for i := 0; i < len(msg.ID); i++ {
		c := msg.ID[i]
		if c < 0x21 || c > 0x7e {
			return entry, fmt.Errorf("id contains invalid character %q at position %d: only printable ASCII (no spaces) is allowed", c, i)
		}
	}

	if len(msg.Data) > 1024*1024*1024 {
		return entry, fmt.Errorf("data length %d exceeds maximum allowed length 1GB", len(msg.Data))
	}
	hash := xxhash.Sum64(msg.Data)
	entry.Offset, err = l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return entry, err
	}
	if err = WriteID(l.f, msg.Topic); err != nil {
		return
	}
	if err = WriteID(l.f, msg.ID); err != nil {
		return
	}
	if err = WriteInt64(l.f, msg.TS); err != nil {
		return
	}
	if err = WriteUint64(l.f, hash); err != nil {
		return
	}
	if err = WriteInt64(l.f, int64(len(msg.Data))); err != nil {
		return
	}
	_, err = l.f.Write(msg.Data)
	return
}

func (l *Log) Read(offset int64) (msg Msg, err error) {
	_, err = l.f.Seek(offset, io.SeekStart)
	if err != nil {
		return
	}
	switch l.v {
	case 0:
		msg.Topic = ""
		msg.ID, err = ReadString(l.f, 256)
		if err != nil {
			return
		}
	case 1:
		msg.Topic, err = ReadID(l.f)
		if err != nil {
			return
		}
		msg.ID, err = ReadID(l.f)
		if err != nil {
			return
		}
	default:
		err = fmt.Errorf("unsupported log version %d", l.v)
		return
	}
	msg.TS, err = ReadInt64(l.f)
	if err != nil {
		return
	}
	hash, err := ReadUint64(l.f)
	if err != nil {
		return
	}
	length, err := ReadUint64(l.f)
	if err != nil {
		return
	}
	if length > 1024*1024*1024 {
		err = fmt.Errorf("data length %d exceeds maximum allowed length 1GB", length)
		return
	}
	msg.Data = make([]byte, length)
	_, err = io.ReadFull(l.f, msg.Data)
	if err != nil {
		return
	}
	verifyHash := xxhash.Sum64(msg.Data)
	if verifyHash != hash {
		err = fmt.Errorf("data hash mismatch: expected %016x, got %016x", hash, verifyHash)
	}
	return
}

func (l *Log) RangeSince(since int64, f func(msg Msg) error) error {
	switch l.v {
	case 0:
		_, err := l.f.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
	case 1:
		_, err := l.f.Seek(4, io.SeekStart)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported log version %d", l.v)
	}
	for {
		_, msg, err := l.readNext()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		if msg.TS >= since {
			err = f(msg)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

func (l *Log) Range(f func(id string, entry IndexEntry) error) error {
	switch l.v {
	case 0:
		_, err := l.f.Seek(0, io.SeekStart)
		if err != nil {
			return err
		}
	case 1:
		_, err := l.f.Seek(4, io.SeekStart)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("unsupported log version %d", l.v)
	}
	for {
		offset, msg, err := l.readNext()
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		err = f(msg.ID, IndexEntry{
			TS:     msg.TS,
			File:   l.path,
			Offset: offset,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
