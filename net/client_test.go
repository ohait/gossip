package net

import (
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"runtime"
	"strings"
	"testing"
	"time"

	"github.com/ohait/gossip/lib"
)

// TestE2E publishes messages through a real client/server connection and
// checks they come back over the broadcast callback with an assigned TS.
func TestE2E(t *testing.T) {
	resetShutdownForTest(t)

	svc := &Server{G: &lib.Gossip{LogsFolder: t.TempDir()}}
	if err := svc.Init(); err != nil {
		t.Fatal(err)
	}
	addr, err := svc.Bind("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan lib.Msg, 1)
	cli := &TCPClient{
		Addr: addr,
	}
	err = cli.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
		received <- lib.Msg{ID: id, TS: ts, Data: data}
		return nil
	})
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

	// PublishCAS's ts is the *expected previous* TS, not the desired final
	// one: 0 means "this ID doesn't exist yet". The server assigns the real
	// TS itself on commit, so we can't assert an exact value back.
	tests := []struct {
		ID   string
		Data []byte
	}{
		{ID: "msg-1", Data: []byte("hello")},
		{ID: "msg-2", Data: largeJSON},
	}
	for _, tt := range tests {
		// poll until the client has connected and the send succeeds
		var sendErr error
		for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
			if sendErr = cli.PublishCAS("", tt.ID, 0, tt.Data); sendErr == nil {
				break
			}
		}
		if sendErr != nil {
			t.Fatalf("Publish(%s): %v", tt.ID, sendErr)
		}

		select {
		case got := <-received:
			if got.ID != tt.ID || got.TS <= 0 || string(got.Data) != string(tt.Data) {
				t.Fatalf("got {ID:%q TS:%d Data:%q}, want {ID:%q TS:>0 Data:%q}", got.ID, got.TS, got.Data, tt.ID, tt.Data)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for %s to come back", tt.ID)
		}
	}
}

// TestClientInitTimeout verifies Init() fails with a timeout error when the
// server accepts the connection but never completes the handshake.
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
	}

	err = cli.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
		return nil
	})
	if err == nil {
		t.Fatal("Init() unexpectedly succeeded")
	}
	<-accepted
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("Init() error = %v, want timeout", err)
	}
}

// TestClientSendTimeout verifies send() fails with a timeout error when the
// peer never reads off the pipe.
func TestClientSendTimeout(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	defer clientConn.Close()

	cli := &TCPClient{
		Timeout: 50 * time.Millisecond,
		conn:    clientConn,
	}

	data := make([]byte, 1<<20)
	err := cli.send(CmdCAS, "", "msg-1", 1, data)
	if err == nil {
		t.Fatal("send() unexpectedly succeeded")
	}
	var netErr net.Error
	if !errors.As(err, &netErr) || !netErr.Timeout() {
		t.Fatalf("send() error = %v, want timeout", err)
	}
}

// TestClientSendErrorClearsConnection verifies send() clears c.conn after
// writing to an already-closed connection fails.
func TestClientSendErrorClearsConnection(t *testing.T) {
	serverConn, clientConn := net.Pipe()
	defer serverConn.Close()
	clientConn.Close()

	cli := &TCPClient{
		Timeout: 50 * time.Millisecond,
		conn:    clientConn,
	}

	err := cli.send(CmdCAS, "", "msg-1", 1, []byte("hello"))
	if err == nil {
		t.Fatal("send() unexpectedly succeeded")
	}
	if cli.conn != nil {
		t.Fatal("send() left a failed connection installed")
	}
}

// TestClientIdleReadTimeoutReturns drives the handshake by hand and verifies
// connectAndReceive() returns a timeout error, and clears c.conn, once the
// server goes idle without sending anything more.
func TestClientIdleReadTimeoutReturns(t *testing.T) {
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()

	accepted := make(chan net.Conn, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		accepted <- conn
	}()

	cli := &TCPClient{
		Addr:    ln.Addr().String(),
		Timeout: 50 * time.Millisecond,
		Log:     func(string, ...any) {},
	}

	done := make(chan error, 1)
	go func() {
		done <- cli.connectAndReceive()
	}()

	conn := <-accepted
	defer conn.Close()
	var prefix [len(HandshakePrefix)]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		t.Fatal(err)
	}
	if string(prefix[:]) != HandshakePrefix {
		t.Fatalf("handshake prefix = %q, want %q", string(prefix[:]), HandshakePrefix)
	}
	if _, err := lib.ReadInt64(conn); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte(Handshake)); err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write([]byte{CmdReplyDone}); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-done:
		var netErr net.Error
		if !errors.As(err, &netErr) || !netErr.Timeout() {
			t.Fatalf("connectAndReceive() error = %v, want timeout", err)
		}
	case <-time.After(time.Second):
		t.Fatal("connectAndReceive() did not return after idle read timeout")
	}
	if cli.conn != nil {
		t.Fatal("connectAndReceive() left timed out connection installed")
	}
}

// TestSignalE2EAndNoReplay verifies Signal() delivers to currently connected
// clients with its exact TS preserved, but is transient: a client that
// connects afterward does not receive it on replay.
func TestSignalE2EAndNoReplay(t *testing.T) {
	resetShutdownForTest(t)

	svc := &Server{G: &lib.Gossip{LogsFolder: t.TempDir()}}
	if err := svc.Init(); err != nil {
		t.Fatal(err)
	}
	addr, err := svc.Bind("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	received := make(chan lib.Msg, 1)
	receiver := &TCPClient{
		Addr: addr,
	}
	if err := receiver.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
		received <- lib.Msg{ID: id, TS: ts, Data: data}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer receiver.Close()

	sender := &TCPClient{
		Addr: addr,
	}
	if err := sender.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer sender.Close()

	want := lib.Msg{ID: "sig-1", TS: 42, Data: []byte("flash")}
	var sendErr error
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if sendErr = sender.Signal("", want.ID, want.TS, want.Data); sendErr == nil {
			break
		}
	}
	if sendErr != nil {
		t.Fatalf("Signal(%s): %v", want.ID, sendErr)
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
	}
	if err := reconnect.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
		replayed <- struct{}{}
		return nil
	}); err != nil {
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
		Addr:    ln.Addr().String(),
		Timeout: 50 * time.Millisecond,
	}
	if err := cli.Init(func(topic, id string, ts int64, data []byte, persist bool) error { return nil }); err == nil {
		t.Fatal("Init() should have failed")
	}

	time.Sleep(100 * time.Millisecond) // let goroutines settle
	if got := runtime.NumGoroutine(); got > before {
		t.Errorf("goroutine leak: %d goroutines before Init(), %d after", before, got)
	}
}

// newTestServer starts a Gossip server backed by a fresh temp dir and
// returns the address it's listening on.
func newTestServer(t *testing.T) string {
	t.Helper()
	resetShutdownForTest(t)

	svc := &Server{G: &lib.Gossip{LogsFolder: t.TempDir()}}
	if err := svc.Init(); err != nil {
		t.Fatal(err)
	}
	addr, err := svc.Bind("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	return addr
}

// TestClientReconnectsAfterConnectionDrop verifies loop() notices a dropped
// connection and re-establishes it on its own, without the caller having to
// call Init() again. loop() retries on a hardcoded 5s backoff, so this test
// is inherently slow.
func TestClientReconnectsAfterConnectionDrop(t *testing.T) {
	addr := newTestServer(t)

	received := make(chan lib.Msg, 1)
	cli := &TCPClient{Addr: addr}
	if err := cli.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
		received <- lib.Msg{ID: id, TS: ts, Data: data}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	cli.m.Lock()
	conn := cli.conn
	cli.m.Unlock()
	if conn == nil {
		t.Fatal("client has no live connection to drop")
	}
	conn.Close() // simulate a dropped connection; loop() should reconnect on its own

	var sendErr error
	for deadline := time.Now().Add(8 * time.Second); time.Now().Before(deadline); time.Sleep(50 * time.Millisecond) {
		if sendErr = cli.PublishCAS("", "after-reconnect", 0, []byte("back")); sendErr == nil {
			break
		}
	}
	if sendErr != nil {
		t.Fatalf("PublishCAS after reconnect: %v", sendErr)
	}

	select {
	case got := <-received:
		if got.ID != "after-reconnect" || string(got.Data) != "back" {
			t.Fatalf("got {ID:%q Data:%q}, want {ID:%q Data:%q}", got.ID, got.Data, "after-reconnect", "back")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for message after reconnect")
	}
}

// TestClientPublishCASConflictClosesConnection documents an asymmetry in the
// wire protocol: a CAS conflict is never reported back to send() as an
// error, since the write to the socket itself succeeds. The server instead
// signals it by closing the connection, which the client only discovers on
// its next read.
func TestClientPublishCASConflictClosesConnection(t *testing.T) {
	addr := newTestServer(t)

	cli := &TCPClient{Addr: addr}
	if err := cli.Init(func(topic, id string, ts int64, data []byte, persist bool) error { return nil }); err != nil {
		t.Fatal(err)
	}
	defer cli.Close()

	var sendErr error
	for deadline := time.Now().Add(time.Second); time.Now().Before(deadline); time.Sleep(10 * time.Millisecond) {
		if sendErr = cli.PublishCAS("", "dup", 0, []byte("first")); sendErr == nil {
			break
		}
	}
	if sendErr != nil {
		t.Fatalf("first PublishCAS: %v", sendErr)
	}

	// ts=0 now conflicts with whatever TS the server just assigned "dup".
	if err := cli.PublishCAS("", "dup", 0, []byte("second")); err != nil {
		t.Fatalf("PublishCAS on conflict unexpectedly failed locally: %v", err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		cli.m.Lock()
		conn := cli.conn
		cli.m.Unlock()
		if conn == nil {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("server did not close the connection after a CAS conflict")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestClientInitTwiceFails verifies a second Init() call is rejected instead
// of spawning a competing reconnect loop.
func TestClientInitTwiceFails(t *testing.T) {
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

	cli := &TCPClient{
		Addr:    ln.Addr().String(),
		Timeout: 50 * time.Millisecond,
	}
	cb := func(topic, id string, ts int64, data []byte, persist bool) error { return nil }
	if err := cli.Init(cb); err == nil {
		t.Fatal("first Init() unexpectedly succeeded")
	}

	err = cli.Init(cb)
	if err == nil {
		t.Fatal("second Init() unexpectedly succeeded")
	}
	if err.Error() != "client already initialized" {
		t.Fatalf("second Init() error = %q, want %q", err.Error(), "client already initialized")
	}
}

// TestClientCloseTwiceReturnsErrClosed verifies Close() is idempotent. A
// failed Init() already calls Close() internally, so the explicit Close()
// below is itself the second call — no server needed to observe this.
func TestClientCloseTwiceReturnsErrClosed(t *testing.T) {
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

	cli := &TCPClient{
		Addr:    ln.Addr().String(),
		Timeout: 50 * time.Millisecond,
	}
	if err := cli.Init(func(topic, id string, ts int64, data []byte, persist bool) error { return nil }); err == nil {
		t.Fatal("Init() unexpectedly succeeded")
	}

	if err := cli.Close(); !errors.Is(err, os.ErrClosed) {
		t.Fatalf("Close() = %v, want %v", err, os.ErrClosed)
	}
}
