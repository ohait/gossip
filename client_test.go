package gossip

import (
	"encoding/json"
	"errors"
	"net"
	"runtime"
	"strings"
	"testing"
	"time"

	int "github.com/ohait/gossip/internal"
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
			if sendErr = cli.Publish(tt.ID, tt.TS, tt.Data); sendErr == nil {
				break
			}
		}
		if sendErr != nil {
			t.Fatalf("Publish(%s): %v", tt.ID, sendErr)
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

func TestClientInitTimeout(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan struct{})
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		close(accepted)
		select {}
	}()

	cli := &TCPClient{
		Addr:    ln.Addr().String(),
		Timeout: 50 * time.Millisecond,
		OnMessage: func(id string, ts int64, data []byte) error {
			return nil
		},
	}

	err = cli.Init()
	if err == nil {
		t.Fatal("Init() unexpectedly succeeded")
	}
	<-accepted
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("Init() error = %v, want timeout", err)
	}
}

func TestClientSendTimeout(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	cli := &TCPClient{
		Timeout: 50 * time.Millisecond,
		conn:    clientConn,
	}

	data := make([]byte, 1<<20)
	err := cli.send(int.CmdMessage, "msg-1", 1, data)
	if err == nil {
		t.Fatal("send() unexpectedly succeeded")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("send() error = %v, want timeout", err)
	}
}

func TestSignalE2EAndNoReplay(t *testing.T) {
	svc := &int.Service{LogsFolder: t.TempDir()}
	if err := svc.Init(); err != nil {
		t.Fatal(err)
	}
	addr, err := svc.Bind("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan int.Msg, 1)
	receiver := &TCPClient{
		Addr: addr,
		OnMessage: func(id string, ts int64, data []byte) error {
			received <- int.Msg{ID: id, TS: ts, Data: data}
			return nil
		},
	}
	if err := receiver.Init(); err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()

	sender := &TCPClient{
		Addr: addr,
		OnMessage: func(id string, ts int64, data []byte) error {
			return nil
		},
	}
	if err := sender.Init(); err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	want := int.Msg{ID: "sig-1", TS: 42, Data: []byte("flash")}
	var sendErr error
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if sendErr = sender.Emit(want.ID, want.TS, want.Data); sendErr == nil {
			break
		}
	}
	if sendErr != nil {
		t.Fatalf("Emit(%s): %v", want.ID, sendErr)
	}

	select {
	case got := <-received:
		if got.ID != want.ID || got.TS != want.TS || string(got.Data) != string(want.Data) {
			t.Fatalf("got {ID:%q TS:%d Data:%q}, want {ID:%q TS:%d Data:%q}", got.ID, got.TS, got.Data, want.ID, want.TS, want.Data)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for transient send")
	}

	replayed := make(chan struct{}, 1)
	reconnect := &TCPClient{
		Addr: addr,
		OnMessage: func(id string, ts int64, data []byte) error {
			replayed <- struct{}{}
			return nil
		},
	}
	if err := reconnect.Init(); err != nil {
		t.Fatal(err)
	}
	defer reconnect.Close()

	select {
	case <-replayed:
		t.Fatal("transient send was replayed to a new client")
	case <-time.After(200 * time.Millisecond):
	}
}

// TestInitErrorStopsLoop verifies that when Init() returns an error the
// internal reconnect loop does not keep running in the background.
func TestInitErrorStopsLoop(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				return
			}
			conn.Close()
		}
	}()

	before := runtime.NumGoroutine()
	cli := &TCPClient{
		Addr:      ln.Addr().String(),
		Timeout:   50 * time.Millisecond,
		OnMessage: func(id string, ts int64, data []byte) error { return nil },
	}
	if err := cli.Init(); err == nil {
		t.Fatal("Init() should have failed")
	}

	time.Sleep(100 * time.Millisecond) // let goroutines settle
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("goroutine leak: %d goroutines before Init(), %d after", before, got)
	}
}
