package gossip

import (
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
	tcpKeepAlivePeriod = 30 * time.Second
	defaultTimeout     = 10 * time.Second
)

type Client interface {
	// Setup the client and reply the history, blocks until the replay is completed
	Init(func(topic, id string, ts int64, data []byte) error) error

	// PublishLWW broadcasts data and persists it using last write wins
	PublishLWW(id string, ts_epoch_ns int64, data []byte) error

	// PublishCAS broadcasts data and persists it using compare-and-swap
	// the ts_epoch_ns is expected to match the old one (or zero for new entries)
	// the data will then get a new ts_epoch_ns assigned if succeed
	PublishCAS(id string, ts_epoch_ns int64, data []byte) error

	// Signal broadcasts transient data without persisting it.
	Signal(id string, ts_epoch_ns int64, data []byte) error

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
	cb    func(topic, id string, ts int64, data []byte) error

	replayErr chan error
}

var _ Client = (*TCPClient)(nil)

func (c *TCPClient) Init(cb func(topic, id string, ts int64, data []byte) error) error {
	if c.done != nil {
		return fmt.Errorf("client already initialized")
	}
	if c.Addr == "" {
		return fmt.Errorf("missing Addr")
	}
	c.cb = cb
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
		case gi.CmdCAS:
			return fmt.Errorf("server should never send CAS messages")
		case gi.CmdLWW:
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
	data, err := gi.DecodePayload(msg.Data)
	if err != nil {
		return err
	}
	if persist && c.LastTS < msg.TS {
		c.LastTS = msg.TS // move Since forward only for replayable data
	}
	if c.cb != nil {
		return c.cb(msg.Topic, msg.ID, msg.TS, data)
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

// PublishLWW writes durable data to the server with automatic retry on failure.
// TODO: accept a context.Context to allow cancellation during retries.
func (c *TCPClient) PublishLWW(id string, ts int64, data []byte) error {
	return c.sendWithCmd(gi.CmdLWW, id, ts, data)
}

func (c *TCPClient) PublishCAS(id string, ts int64, data []byte) error {
	return c.sendWithCmd(gi.CmdCAS, id, ts, data)
}

// Signal writes transient data to the server with automatic retry on failure.
func (c *TCPClient) Signal(id string, ts int64, data []byte) error {
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
	data, err := gi.EncodePayload(data)
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
	_, err = msg.WriteTo(conn, cmd)
	return err
}

func (c *TCPClient) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultTimeout
}
