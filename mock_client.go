package gossip

import "io"

type MockClient struct {
	OnMessage func(id string, ts int64, data []byte) error
}

var _ Client = (*MockClient)(nil)

func (c *MockClient) Init() error {
	return nil
}

func (c *MockClient) Send(id string, ts int64, data []byte) error {
	return c.OnMessage(id, ts, data)
}

func (c *MockClient) Close() error {
	c.OnMessage = func(id string, ts int64, data []byte) error {
		return io.EOF
	}
	return nil
}
