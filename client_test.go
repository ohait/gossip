package gossip

import (
	"encoding/json"
	"strings"
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
	cli := &TCPClient{
		Addr: addr,
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

	largeJSON, err := json.Marshal(map[string]any{
		"type":    "event",
		"message": strings.Repeat("hello", 200),
		"values":  []int64{1, 1, 1, 1, 1, 1, 1, 1},
	})
	if err != nil {
		t.Fatal(err)
	}

	tests := []int.Msg{
		{ID: "msg-1", TS: 42, Data: []byte("hello")},
		{ID: "msg-2", TS: 43, Data: largeJSON},
	}
	for _, tt := range tests {
		tt := tt
		// poll until the client has connected and the send succeeds
		var sendErr error
		for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if sendErr = cli.Send(tt.ID, tt.TS, tt.Data); sendErr == nil {
				break
			}
		}
		if sendErr != nil {
			t.Fatalf("Send(%s): %v", tt.ID, sendErr)
		}

		select {
		case got := <-received:
			if got.ID != tt.ID || got.TS != tt.TS || string(got.Data) != string(tt.Data) {
				t.Fatalf("got {ID:%q TS:%d Data:%q}, want {ID:%q TS:%d Data:%q}", got.ID, got.TS, got.Data, tt.ID, tt.TS, tt.Data)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s to come back", tt.ID)
		}
	}
}
