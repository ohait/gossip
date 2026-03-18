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
}

func (l *Log) Append(msg Msg) (entry IndexEntry, err error) {
	entry.TS = int(msg.TS)
	entry.File = l.path
	if len(msg.ID) > 256 {
		return entry, fmt.Errorf("id length %d exceeds maximum allowed length 256", len(msg.ID))
	}
	// TODO check if msg.ID is valid (printable, no spaces, no symbols, ascii only)

	if len(msg.Data) > 1024*1024*1024 {
		return entry, fmt.Errorf("data length %d exceeds maximum allowed length 1GB", len(msg.Data))
	}
	hash := xxhash.Sum64(msg.Data)
	entry.Offset, err = l.f.Seek(0, io.SeekEnd)
	if err != nil {
		return entry, err
	}
	if err = WriteString(l.f, msg.ID); err != nil {
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
	msg.ID, err = ReadString(l.f, 256)
	if err != nil {
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

func (l *Log) Range(f func(id string, entry IndexEntry) error) error {
	l.f.Seek(0, io.SeekStart)
	for {
		offset, err := l.f.Seek(0, io.SeekCurrent)
		if err != nil {
			return err
		}
		id, err := ReadString(l.f, 256)
		if err != nil {
			if errors.Is(err, io.EOF) {
				break
			}
			return err
		}
		ts, err := ReadInt64(l.f)
		if err != nil {
			return err
		}
		_, err = ReadUint64(l.f) // skip hash
		if err != nil {
			return err
		}
		length, err := ReadUint64(l.f)
		if err != nil {
			return err
		}
		if length > 1024*1024*1024 {
			return fmt.Errorf("data length %d exceeds maximum allowed length 1GB", length)
		}
		_, err = l.f.Seek(int64(length), io.SeekCurrent)
		if err != nil {
			return err
		}
		err = f(id, IndexEntry{
			TS:     int(ts),
			File:   l.path,
			Offset: offset,
		})
		if err != nil {
			return err
		}
	}
	return nil
}
