package gossip

import (
	"bytes"
	"compress/zlib"
	"encoding/binary"
	"fmt"
	"io"
)

const (
	HandshakePrefix = "GOSSIP"   // 6-byte prefix sent by the client
	Handshake       = "GOSSIP\n" // 7-byte ack sent by the server

	CmdLWW   = byte('M') // last write wins message command byte
	CmdCAS       = byte('C') // compare-and-swap command byte
	CmdSignal    = byte('S') // signal (like a message, but not persisted or replayed)
	CmdReplyDone = byte('D')

	PayloadEncodingRaw  = byte('=') // payload is stored as-is
	PayloadEncodingZlib = byte('z') // payload is zlib-compressed
)

// EncodePayload compresses data with zlib if that reduces its size, prefixing
// the result with a one-byte encoding tag (PayloadEncodingZlib or PayloadEncodingRaw).
func EncodePayload(data []byte) ([]byte, error) {
	var compressed bytes.Buffer
	zw, err := zlib.NewWriterLevel(&compressed, zlib.BestSpeed)
	if err != nil {
		return nil, err
	}
	if _, err := zw.Write(data); err != nil {
		zw.Close()
		return nil, err
	}
	if err := zw.Close(); err != nil {
		return nil, err
	}
	if compressed.Len() < len(data) {
		out := make([]byte, 1+compressed.Len())
		out[0] = PayloadEncodingZlib
		copy(out[1:], compressed.Bytes())
		return out, nil
	}
	out := make([]byte, 1+len(data))
	out[0] = PayloadEncodingRaw
	copy(out[1:], data)
	return out, nil
}

// DecodePayload reverses EncodePayload, stripping the encoding tag and
// decompressing if necessary.
func DecodePayload(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("missing payload encoding")
	}
	switch data[0] {
	case PayloadEncodingRaw:
		out := make([]byte, len(data)-1)
		copy(out, data[1:])
		return out, nil
	case PayloadEncodingZlib:
		zr, err := zlib.NewReader(bytes.NewReader(data[1:]))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		out, err := io.ReadAll(zr)
		if err != nil {
			return nil, err
		}
		return out, nil
	default:
		return nil, fmt.Errorf("unknown payload encoding %q", data[0])
	}
}

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

func ReadID(f io.Reader) (string, error) {
	lenBuf := make([]byte, 2) // 2 bytes => max length 65535, which is more than enough for IDs
	if _, err := io.ReadFull(f, lenBuf); err != nil {
		return "", err
	}
	n := binary.BigEndian.Uint16(lenBuf)
	if n > 1024 {
		return "", fmt.Errorf("id length %d exceeds maximum allowed length 1024", n)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(f, b); err != nil {
		return "", err
	}
	return string(b), nil
}

func ReadBytes(f io.Reader, maxLength int) ([]byte, error) {
	lenBuf := make([]byte, 8)
	if _, err := io.ReadFull(f, lenBuf); err != nil {
		return nil, err
	}
	n := binary.BigEndian.Uint64(lenBuf)
	if maxLength > 0 && n > uint64(maxLength) {
		return nil, fmt.Errorf("data length %d exceeds maximum allowed length %d", n, maxLength)
	}
	b := make([]byte, n)
	if _, err := io.ReadFull(f, b); err != nil {
		return nil, err
	}
	return b, nil
}

func WriteID(f io.Writer, s string) error {
	b := []byte(s)
	if len(b) > 1024 {
		return fmt.Errorf("id length %d exceeds maximum allowed length 1024", len(s))
	}
	var lenBuf [2]byte
	binary.BigEndian.PutUint16(lenBuf[:], uint16(len(b)))
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
