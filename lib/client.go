package lib

type Client interface {
	// Init registers cb to receive every message
	Init(cb func(topic, id string, ts_epoch_ns int64, data []byte, persist bool) error) error

	PublishCAS(topic, id string, ts_epoch_ns int64, data []byte) error

	// Signal broadcasts transient data without persisting it.
	Signal(topic, id string, ts_epoch_ns int64, data []byte) error

	Replay(since int64, f func(Msg) error) error
}
