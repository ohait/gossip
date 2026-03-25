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

	gi "github.com/ohait/gossip/internal"
)

const (
	payloadEncodingRaw  = byte('=') // TODO move to enc
	payloadEncodingZlib = byte('z') // TODO move to enc
	tcpKeepAlivePeriod  = 30 * time.Second
	defaultTimeout      = 10 * time.Second
)

type Client interface {
	// Setup the client and reply the history, blocks until the replay is completed
	Init() error

	// Publish broadcasts data and persists it.
	Publish(id string, ts int64, data []byte) error

	// Emit broadcasts transient data without persisting it.
	Emit(id string, ts int64, data []byte) error

	// Close the client
	Close() error
}

type TCPClient struct {
	m     sync.Mutex
	conn  net.Conn
	close atomic.Bool
	done  chan struct{}

	Log          func(format string, args ...any)
	Addr         string
	Timeout      time.Duration // per network operation; default 10s
	ReplayMargin time.Duration // replay messages starting from LastTS-ReplayMargin; default 5s
	LastTS       int64         // nanoseconds epoch: server will replay all messages with TS > Since - ReplayMargin
	OnMessage    func(id string, ts int64, data []byte) error

	replayErr chan error
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
	if err != nil {
		c.Close()
		return err
	}
	c.Log("initial replay completed")
	return nil
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
		select {
		case c.replayErr <- err:
		default:
			c.Log("connection error: %v", err)
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

	defer func() {
		c.m.Lock()
		if c.conn == conn {
			c.conn = nil
		}
		c.m.Unlock()
		conn.Close()
	}()
	var cmd [1]byte
	for {
		conn.SetReadDeadline(time.Time{}) // no timeout between messages
		_, err := io.ReadFull(conn, cmd[:])
		if err != nil {
			return err
		}
		conn.SetReadDeadline(time.Now().Add(c.timeout())) // timeout for the rest of the message after reading the command byte
		switch cmd[0] {
		case gi.CmdReplyDone:
			select {
			case c.replayErr <- nil:
			default:
			}
		case gi.CmdMessage:
			if err := c.handleIncoming(conn, true); err != nil {
				return err
			}
		case gi.CmdSignal:
			if err := c.handleIncoming(conn, false); err != nil {
				return err
			}
		}
	}
}

func (c *TCPClient) handleIncoming(conn net.Conn, persist bool) error {
	var msg gi.Msg
	if _, err := msg.Decode(conn, 0); err != nil {
		return err
	}
	data, err := decodePayload(msg.Data)
	if err != nil {
		return err
	}
	if persist && c.LastTS < msg.TS {
		c.LastTS = msg.TS // move Since forward only for replayable data
	}
	if c.OnMessage != nil {
		return c.OnMessage(msg.ID, msg.TS, data)
	}
	return nil
}

func (c *TCPClient) connect() (net.Conn, error) {
	dialer := &net.Dialer{
		KeepAlive: tcpKeepAlivePeriod,
		Timeout:   c.timeout(),
	}
	conn, err := dialer.Dial("tcp", c.Addr)
	if err != nil {
		return nil, err
	}
	tcpConn, ok := conn.(*net.TCPConn)
	if !ok {
		conn.Close()
		return nil, fmt.Errorf("unexpected connection type %T", conn)
	}
	if err := tcpConn.SetKeepAlive(true); err != nil {
		conn.Close()
		return nil, err
	}
	if err := tcpConn.SetKeepAlivePeriod(tcpKeepAlivePeriod); err != nil {
		conn.Close()
		return nil, err
	}
	if err := conn.SetDeadline(time.Now().Add(c.timeout())); err != nil {
		conn.Close()
		return nil, err
	}
	// send GOSSIP<since:int64>
	if _, err = conn.Write([]byte(gi.HandshakePrefix)); err != nil {
		conn.Close()
		return nil, err
	}
	// replay 5 seconds before Since, to tollerate races
	if err = gi.WriteInt64(conn, c.LastTS-int64(c.ReplayMargin)); err != nil {
		conn.Close()
		return nil, err
	}
	var buf [len(gi.Handshake)]byte
	_, err = io.ReadFull(conn, buf[:])
	if err != nil {
		conn.Close()
		return nil, err
	}
	if string(buf[:]) != gi.Handshake {
		conn.Close()
		return nil, fmt.Errorf("unexpected handshake response: %q", string(buf[:]))
	}
	if err := conn.SetDeadline(time.Time{}); err != nil {
		conn.Close()
		return nil, err
	}
	c.Log("Connected to server at %s, replaying messages since %d", c.Addr, c.LastTS)
	return conn, nil
}

// Publish writes durable data to the server with automatic retry on failure.
// TODO: accept a context.Context to allow cancellation during retries.
func (c *TCPClient) Publish(id string, ts int64, data []byte) error {
	return c.sendWithCmd(gi.CmdMessage, id, ts, data)
}

// Emit writes transient data to the server with automatic retry on failure.
func (c *TCPClient) Emit(id string, ts int64, data []byte) error {
	return c.sendWithCmd(gi.CmdSignal, id, ts, data)
}

func (c *TCPClient) sendWithCmd(cmd byte, id string, ts int64, data []byte) error {
	var firstError error
	for i := 1; i <= 5; i++ {
		err := c.send(cmd, id, ts, data)
		if err == nil {
			return nil
		}
		if firstError == nil {
			firstError = err
		}
		c.m.Lock()
		if c.conn != nil {
			c.conn.Close() // force reconnect after any write error
		}
		c.m.Unlock()
		time.Sleep(time.Second * time.Duration(i*i/2)) // 500ms, 2s, 4.5s, 8s, 12.5s (should be enough for a full restart of gossip server)
	}
	return fmt.Errorf("after 5 retries: %w", firstError)
}

func (c *TCPClient) send(cmd byte, id string, ts int64, data []byte) error {
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
	if err := conn.SetWriteDeadline(time.Now().Add(c.timeout())); err != nil {
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	msg := gi.Msg{ID: id, TS: ts, Data: data}
	switch cmd {
	case gi.CmdSignal:
		_, err = msg.WriteSignalTo(conn)
	default:
		_, err = msg.WriteTo(conn)
	}
	return err
}

func (c *TCPClient) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultTimeout
}

// TODO move to enc
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

// TODO move to enc
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
