package gossip

import (
	"testing"
	"time"

	int "oha.it/gossip/internal"
)

func TestE2E(t *testing.T) {
	svc := &int.Service{LogsFolder: t.TempDir()}
	if err := svc.Init(); err != nil {
		t.Fatal(err)
	}
	addr, err := svc.Bind("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan int.Msg, 1)
	cli := &Client{
		Addr:  addr,
		OnMessage: func(id string, ts int64, data []byte) error {
			received <- int.Msg{ID: id, TS: ts, Data: data}
			return nil
		},
	}
	err = cli.Init()
	if err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	// poll until the client has connected and the send succeeds
	var sendErr error
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if sendErr = cli.Send("msg-1", 42, []byte("hello")); sendErr == nil {
			break
		}
	}
	if sendErr != nil {
		t.Fatalf("Send: %v", sendErr)
	}

	select {
	case got := <-received:
		if got.ID != "msg-1" || got.TS != 42 || string(got.Data) != "hello" {
			t.Errorf("got {ID:%q TS:%d Data:%q}, want {ID:\"msg-1\" TS:42 Data:\"hello\"}", got.ID, got.TS, got.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message to come back")
	}
}
