package lib

type Client interface {
	PublishCAS(topic, id string, ts_epoch_ns int64, data []byte) error

	// Signal broadcasts transient data without persisting it.
	Signal(topic, id string, ts_epoch_ns int64, data []byte) error

	Replay(since int64, f func(Msg) error) error
}
