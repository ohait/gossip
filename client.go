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

	int "oha.it/gossip/internal"
)

type Client struct {
	m     sync.Mutex
	conn  net.Conn
	close atomic.Bool
	done  chan struct{}
	Log   func(format string, args ...any)

	Addr         string
	ReplayMargin time.Duration // replay messages starting from LastTS-ReplayMargin; default 5s
	LastTS       int64         // nanoseconds epoch: server will replay all messages with TS > Since - ReplayMargin
	OnMessage    func(id string, ts int64, data []byte) error
}

func (c *Client) Init() error {
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
	go c.loop()
	return nil
}

func (c *Client) Close() error {
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

func (c *Client) loop() {
	for !c.close.Load() {
		t0 := time.Now()
		err := c.connectAndReceive()
		c.Log("connection error: %v", err)
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

func (c *Client) connectAndReceive() error {
	c.Log("Connecting to server at %s...", c.Addr)
	conn, err := c.connect()
	if err != nil {
		return err
	}
	c.m.Lock()
	c.conn = conn
	c.m.Unlock()

	defer conn.Close()
	for {
		id, err := int.ReadString(conn, 256)
		if err != nil {
			return err
		}
		ts, err := int.ReadInt64(conn)
		if err != nil {
			return err
		}
		data, err := int.ReadBytes(conn, 1024*1024*1024) // max 1GB (server might have a smaller limit)
		if err != nil {
			return err
		}
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

func (c *Client) connect() (net.Conn, error) {
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
func (c *Client) Send(id string, ts int64, data []byte) error {
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
		time.Sleep(time.Second * time.Duration(i*i)) // 1, 4, 9, 16, 25 seconds
	}
	return fmt.Errorf("after 5 retries: %w", firstError)
}

func (c *Client) send(id string, ts int64, data []byte) error {
	if c.close.Load() {
		return os.ErrClosed
	}
	c.m.Lock()
	defer c.m.Unlock()
	conn := c.conn
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	if err := int.WriteString(conn, id); err != nil {
		return err
	}
	if err := int.WriteInt64(conn, ts); err != nil {
		return err
	}
	if err := int.WriteBytes(conn, data); err != nil {
		return err
	}
	return nil
}
