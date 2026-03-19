package gossip

import (
	"fmt"
	"os"

	gi "oha.it/gossip/internal"
)

type LoopbackClient struct {
	log       *gi.Log
	LogFile   string
	OnMessage func(id string, ts int64, data []byte) error
	Log       func(format string, args ...any)
}

var _ Client = (*LoopbackClient)(nil)

// Init prepare the log and replay any previous data
func (c *LoopbackClient) Init() error {
	if c.OnMessage == nil {
		return fmt.Errorf("missing OnMessage callback")
	}
	f, err := os.OpenFile(c.LogFile, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0o644)
	if err != nil {
		return err
	}
	log := gi.NewLog(f)
	err = log.RangeSince(0, func(m gi.Msg) error {
		return c.OnMessage(m.ID, m.TS, m.Data)
	})
	if err != nil {
		log.Close()
		return err
	}
	c.log = log
	return nil
}

func (c *LoopbackClient) Close() error {
	if c.log == nil {
		return os.ErrClosed
	}
	err := c.log.Close()
	c.log = nil
	return err
}

func (c *LoopbackClient) Emit(id string, ts int64, data []byte) error {
	if c.log == nil {
		return fmt.Errorf("client not initialized")
	}
	return c.OnMessage(id, ts, data)
}

func (c *LoopbackClient) Publish(id string, ts int64, data []byte) error {
	if c.log == nil {
		return fmt.Errorf("client not initialized")
	}
	_, err := c.log.Append(gi.Msg{
		ID:   id,
		TS:   ts,
		Data: data,
	})
	if err != nil {
		return err
	}
	err = c.log.Flush()
	if err != nil {
		return err
	}
	if c.OnMessage != nil {
		return c.OnMessage(id, ts, data)
	}
	return nil
}
