package gossip

import (
	"fmt"
	"io"
	"sync"
	"time"
)

type MockClient struct {
	m    sync.Mutex
	last map[string]int64
	cb   func(topic, id string, ts int64, data []byte) error
}

var _ Client = (*MockClient)(nil)

func (c *MockClient) Init(cb func(topic, id string, ts int64, data []byte) error) error {
	c.last = make(map[string]int64)
	c.cb = cb
	return nil
}

func (c *MockClient) PublishCAS(id string, ts int64, data []byte) error {
	c.m.Lock()
	last := c.last[id]
	if last != ts {
		c.m.Unlock()
		return fmt.Errorf("CAS failed: expected %v got %v", last, ts)
	}
	ts = time.Now().UnixNano()
	c.last[id] = ts
	c.m.Unlock()
	return c.cb("", id, ts, data)
}

func (c *MockClient) PublishLWW(id string, ts int64, data []byte) error {
	c.m.Lock()
	if ts < c.last[id] {
		c.m.Unlock()
		return nil // noop
	}
	if ts > time.Now().UnixNano() {
		c.m.Unlock()
		return fmt.Errorf("LWW failed: timestamp in the future: %v", ts)
	}
	c.last[id] = ts
	c.m.Unlock()
	return c.cb("", id, ts, data)
}

func (c *MockClient) Signal(id string, ts int64, data []byte) error {
	return c.cb("", id, ts, data)
}

func (c *MockClient) Close() error {
	c.cb = func(topic, id string, ts int64, data []byte) error {
		return io.EOF
	}
	return nil
}
