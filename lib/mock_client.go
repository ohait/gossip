package lib

import (
	"fmt"
	"io"
	"sync"
	"time"
)

type MockClient struct {
	m    sync.Mutex
	last map[string]int64
	cb   func(topic, id string, ts int64, data []byte, persist bool) error
}

var _ Client = (*MockClient)(nil)

// cb's persist argument tells the caller whether this message came from a
// durable PublishCAS commit (true) or a transient Signal (false).
func (c *MockClient) Init(cb func(topic, id string, ts int64, data []byte, persist bool) error) error {
	c.last = make(map[string]int64)
	c.cb = cb
	return nil
}

func (c *MockClient) PublishCAS(topic, id string, ts int64, data []byte) error {
	c.m.Lock()
	last := c.last[id]
	if last != ts {
		c.m.Unlock()
		return fmt.Errorf("CAS failed: expected %v got %v", last, ts)
	}
	ts = time.Now().UnixNano()
	c.last[id] = ts
	c.m.Unlock()
	return c.cb(topic, id, ts, data, true)
}

func (c *MockClient) Signal(topic, id string, ts int64, data []byte) error {
	return c.cb(topic, id, ts, data, false)
}

func (c *MockClient) Replay(since int64, f func(Msg) error) error {
	return nil
}

func (c *MockClient) Close() error {
	c.cb = func(topic, id string, ts int64, data []byte, persist bool) error {
		return io.EOF
	}
	return nil
}
