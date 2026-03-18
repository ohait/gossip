package gossip

import (
	"bytes"
	"compress/zlib"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	int "oha.it/gossip/internal"
)

const (
	payloadEncodingRaw  = byte('=')
	payloadEncodingZlib = byte('z')
)

type Client interface {
	// Setup the client and reply the history, blocks until the replay is completed
	Init() error

	// Send writes a message to the log and broadcast to all the replicas, might block retrying if the log is not available.
	Send(id string, ts int64, data []byte) error

	// Close the client
	Close() error
}

type TCPClient struct {
	m     sync.Mutex
	conn  net.Conn
	close atomic.Bool
	done  chan struct{}
	Log   func(format string, args ...any)

	Addr         string
	ReplayMargin time.Duration // replay messages starting from LastTS-ReplayMargin; default 5s
	LastTS       int64         // nanoseconds epoch: server will replay all messages with TS > Since - ReplayMargin
	OnMessage    func(id string, ts int64, data []byte) error
	replayErr    chan error
}

var _ Client = (*TCPClient)(nil)

func (c *TCPClient) Init() error {
	if c.done != nil {
		return fmt.Errorf("client already initialized")
	}
	if c.Addr == "" {
		return fmt.Errorf("missing Addr")
	}
	if c.OnMessage == nil {
		return fmt.Errorf("missing OnMessage callback")
	}
	c.done = make(chan struct{})
	if c.ReplayMargin == 0 {
		c.ReplayMargin = 5 * time.Second
	}
	if c.Log == nil {
		c.Log = func(format string, args ...any) {
			log.Printf("gossip: "+format, args...)
		}
	}
	c.replayErr = make(chan error)
	go c.loop()
	err := <-c.replayErr
	c.replayErr = nil // disable for future reconnects
	return err
}

func (c *TCPClient) Close() error {
	if c.close.Swap(true) {
		return os.ErrClosed
	}
	close(c.done)
	c.m.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.m.Unlock()
	return nil
}

func (c *TCPClient) loop() {
	for !c.close.Load() {
		t0 := time.Now()
		err := c.connectAndReceive()
		c.Log("connection error: %v", err)
		select {
		case c.replayErr <- err:
		default:
		}
		elapsed := time.Since(t0)
		if elapsed < 5*time.Second {
			select {
			case <-time.After(5*time.Second - elapsed):
			case <-c.done:
				return
			}
		}
	}
}

func (c *TCPClient) connectAndReceive() error {
	c.Log("Connecting to server at %s...", c.Addr)
	conn, err := c.connect()
	if err != nil {
		return err
	}
	c.m.Lock()
	c.conn = conn
	c.m.Unlock()

	defer conn.Close()
	var cmd [1]byte
	for {
		_, err := io.ReadFull(conn, cmd[:])
		if err != nil {
			return err
		}
		switch cmd[0] {
		case int.CmdReplyDone:
			c.replayErr <- nil // signal success
		case int.CmdMessage:
			var msg int.Msg
			if _, err := msg.Decode(conn, true, 0); err != nil {
				return err
			}
			data, err := decodePayload(msg.Data)
			if err != nil {
				return err
			}
			id, ts := msg.ID, msg.TS
			if c.LastTS < ts {
				c.LastTS = ts // move Since forward
			}
			onMessage := c.OnMessage
			if onMessage != nil {
				err := onMessage(id, ts, data)
				if err != nil {
					return err
				}
			}
		}
	}
}

func (c *TCPClient) connect() (net.Conn, error) {
	// TODO setup keepalive and no delay options to detect broken connections faster
	conn, err := net.Dial("tcp", c.Addr)
	if err != nil {
		return nil, err
	}
	// send GOSSIP<since:int64>
	if _, err = conn.Write([]byte(int.HandshakePrefix)); err != nil {
		conn.Close()
		return nil, err
	}
	// replay 5 seconds before Since, to tollerate races
	if err = int.WriteInt64(conn, c.LastTS-int64(c.ReplayMargin)); err != nil {
		conn.Close()
		return nil, err
	}
	var buf [len(int.Handshake)]byte
	_, err = io.ReadFull(conn, buf[:])
	if err != nil {
		conn.Close()
		return nil, err
	}
	if string(buf[:]) != int.Handshake {
		conn.Close()
		return nil, fmt.Errorf("unexpected handshake response: %q", string(buf[:]))
	}
	return conn, nil
}

// Send writes a message to the server with automatic retry on failure.
// TODO: accept a context.Context to allow cancellation during retries.
func (c *TCPClient) Send(id string, ts int64, data []byte) error {
	var firstError error
	for i := 1; i <= 5; i++ {
		err := c.send(id, ts, data)
		if err == nil {
			return nil
		}
		if firstError == nil {
			firstError = err
		} else {
			c.m.Lock()
			if c.conn != nil {
				c.conn.Close() // force reconnect
			}
			c.m.Unlock()
		}
		time.Sleep(time.Second * time.Duration(i*i/2)) // 500ms, 2s, 4.5s, 8s, 12.5s (should be enough for a full restart of gossip server)
	}
	return fmt.Errorf("after 5 retries: %w", firstError)
}

func (c *TCPClient) send(id string, ts int64, data []byte) error {
	if c.close.Load() {
		return os.ErrClosed
	}
	data, err := encodePayload(data)
	if err != nil {
		return err
	}
	c.m.Lock()
	defer c.m.Unlock()
	conn := c.conn
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	_, err = int.Msg{ID: id, TS: ts, Data: data}.WriteTo(conn)
	return err
}

func encodePayload(data []byte) ([]byte, error) {
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
		out[0] = payloadEncodingZlib
		copy(out[1:], compressed.Bytes())
		return out, nil
	}
	out := make([]byte, 1+len(data))
	out[0] = payloadEncodingRaw
	copy(out[1:], data)
	return out, nil
}

func decodePayload(data []byte) ([]byte, error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("missing payload encoding")
	}
	switch data[0] {
	case payloadEncodingRaw:
		out := make([]byte, len(data)-1)
		copy(out, data[1:])
		return out, nil
	case payloadEncodingZlib:
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
