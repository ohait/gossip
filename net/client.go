package net

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"github.com/ohait/gossip/lib"
)

const (
	tcpKeepAlivePeriod = 30 * time.Second
	defaultTimeout     = 10 * time.Second
)

type TCPClient struct {
	m     sync.Mutex
	conn  net.Conn
	close atomic.Bool
	done  chan struct{}

	Log          func(format string, args ...any)
	Addr         string
	Timeout      time.Duration // per network operation and idle read timeout; default 10s
	ReplayMargin time.Duration // replay messages starting from LastTS-ReplayMargin; default 5s
	LastTS       int64         // nanoseconds epoch: server will replay all messages with TS > Since - ReplayMargin
	cb           func(topic, id string, ts int64, data []byte, persist bool) error

	replayErr chan error
}

func (c *TCPClient) Replay(since int64, f func(lib.Msg) error) error {
	return errors.New("tcp: reply not supported. Replay is called in init.")
}

var _ lib.Client = (*TCPClient)(nil)

func (c *TCPClient) Init(cb func(topic, id string, ts int64, data []byte, persist bool) error) error {
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

func (c *TCPClient) PublishCAS(topic, id string, ts int64, data []byte) error {
	return c.send(CmdCAS, topic, id, ts, data)
}

// Signal writes transient data to the server with automatic retry on failure.
func (c *TCPClient) Signal(topic, id string, ts int64, data []byte) error {
	return c.send(CmdSignal, topic, id, ts, data)
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

	defer c.closeConn(conn)
	var cmd [1]byte
	for {
		conn.SetReadDeadline(time.Time{}) // no timeout between messages; dead peers are caught by TCP keep-alive
		_, err := io.ReadFull(conn, cmd[:])
		if err != nil {
			return err
		}
		conn.SetReadDeadline(time.Now().Add(c.timeout())) // timeout for the rest of the message after reading the command byte
		switch cmd[0] {
		case CmdReplyDone:
			select {
			case c.replayErr <- nil:
			default:
			}
		case CmdCAS:
			return fmt.Errorf("server should never send CAS messages")
		case CmdCommit:
			if err := c.handleIncoming(conn, true); err != nil {
				return err
			}
		case CmdSignal:
			if err := c.handleIncoming(conn, false); err != nil {
				return err
			}
		}
	}
}

func (c *TCPClient) handleIncoming(conn net.Conn, persist bool) error {
	msg, err := readMsg(conn, 0)
	if err != nil {
		return err
	}
	if persist && c.LastTS < msg.TS {
		c.LastTS = msg.TS // move Since forward only for replayable data
	}
	if c.cb != nil {
		return c.cb(msg.Topic, msg.ID, msg.TS, msg.Data, persist)
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
	if _, err = conn.Write([]byte(HandshakePrefix)); err != nil {
		conn.Close()
		return nil, err
	}
	// replay 5 seconds before Since, to tollerate races
	if err = lib.WriteInt64(conn, c.LastTS-int64(c.ReplayMargin)); err != nil {
		conn.Close()
		return nil, err
	}
	var buf [len(Handshake)]byte
	_, err = io.ReadFull(conn, buf[:])
	if err != nil {
		conn.Close()
		return nil, err
	}
	if string(buf[:]) != Handshake {
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

func (c *TCPClient) closeConn(conn net.Conn) {
	c.m.Lock()
	if c.conn == conn {
		c.conn = nil
	}
	c.m.Unlock()
	conn.Close()
}

func (c *TCPClient) send(cmd byte, topic, id string, ts int64, data []byte) error {
	if c.close.Load() {
		return os.ErrClosed
	}
	c.m.Lock()
	defer c.m.Unlock()
	conn := c.conn
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	if err := conn.SetWriteDeadline(time.Now().Add(c.timeout())); err != nil {
		if c.conn == conn {
			c.conn = nil
			conn.Close()
		}
		return err
	}
	defer conn.SetWriteDeadline(time.Time{})
	msg := lib.Msg{Topic: topic, ID: id, TS: ts, Data: data}
	err := writeMsg(conn, cmd, msg)
	if err != nil && c.conn == conn {
		c.conn = nil
		conn.Close()
	}
	return err
}

func (c *TCPClient) timeout() time.Duration {
	if c.Timeout > 0 {
		return c.Timeout
	}
	return defaultTimeout
}
