package gossip

import (
	"encoding/binary"
	"fmt"
	"io"
)

func WriteBytes(f io.Writer, b []byte) error {
	var lenBuf [8]byte
	n := uint64(len(b))
	binary.BigEndian.PutUint64(lenBuf[:], n)
	if nw, err := f.Write(lenBuf[:]); err != nil {
		return err
	} else if nw < len(lenBuf) {
		return io.ErrShortWrite
	}
	if nw, err := f.Write(b); err != nil {
		return err
	} else if nw < len(b) {
		return io.ErrShortWrite
	}
	return nil
}

func ReadBytes(f io.Reader, maxLength int) ([]byte, error) {
	lenBuf := make([]byte, 8)
	if _, err := io.ReadFull(f, lenBuf); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint64(lenBuf)
	if n > uint64(maxLength) {
		return nil, fmt.Errorf("data length %d exceeds maximum allowed length %d", n, maxLength)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, err
	}
	return b, nil
}

func WriteString(f io.Writer, s string) error {
	return WriteBytes(f, []byte(s))
}

func ReadString(f io.Reader, maxLength int) (string, error) {
	b, err := ReadBytes(f, maxLength)
	if err != nil {
		return "", err
	}
	return string(b), nil
}

func WriteUint64(f io.Writer, n uint64) error {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], n)
	if nw, err := f.Write(buf[:]); err != nil {
		return err
	} else if nw < len(buf) {
		return io.ErrShortWrite
	}
	return nil
}

func WriteInt64(f io.Writer, n int64) error {
	return WriteUint64(f, uint64(n))
}

func ReadUint64(f io.Reader) (uint64, error) {
	var buf [8]byte
	if _, err := io.ReadFull(f, buf[:]); err != nil {
		return 0, err
	}
	return binary.BigEndian.Uint64(buf[:]), nil
}

func ReadInt64(f io.Reader) (int64, error) {
	n, err := ReadUint64(f)
	return int64(n), err
}
