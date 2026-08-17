package net

import (
	"fmt"
	stdnet "net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ohait/gossip/lib"
)

func TestReplayingClientReceivesMessagePublishedByAnotherClient(t *testing.T) {
	resetShutdownForTest(t)

	dir := t.TempDir()
	// We populate existing log data
	if err := writeReplayLog(filepath.Join(dir, "log-seed.bin")); err != nil {
		t.Fatal(err)
	}

	g := &lib.Gossip{LogsFolder: dir}
	s := &Server{G: g}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}
	addr, err := s.Bind("127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	replayStarted := make(chan struct{})
	continueReplay := make(chan struct{})
	liveReceived := make(chan lib.Msg, 1)
	initDone := make(chan error, 1)
	var once sync.Once

	publishingClient := &TCPClient{
		Addr:         addr,
		ReplayMargin: time.Nanosecond,
		Timeout:      time.Second,
		Log:          func(string, ...any) {},
	}
	defer publishingClient.Close()
	if err := publishingClient.Init(func(string, string, int64, []byte, bool) error {
		return nil
	}); err != nil {
		t.Fatal(err)
	}

	replayingClient := &TCPClient{
		Addr:         addr,
		ReplayMargin: time.Nanosecond,
		Timeout:      time.Second,
		Log:          func(string, ...any) {},
	}
	defer replayingClient.Close()

	go func() {
		initDone <- replayingClient.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
			switch id {
			case "replay-1":
				once.Do(func() {
					close(replayStarted)
					<-continueReplay
				})
			case "live":
				liveReceived <- lib.Msg{Topic: topic, ID: id, TS: ts, Data: data}
			}
			return nil
		})
	}()

	select {
	case <-replayStarted:
	case <-time.After(time.Second):
		t.Fatal("replaying client did not start replay")
	}

	if err := publishingClient.PublishCAS("folder", "live", 0, []byte("X")); err != nil {
		t.Fatal(err)
	}
	close(continueReplay)

	select {
	case err := <-initDone:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("receiver init did not finish")
	}

	select {
	case msg := <-liveReceived:
		if string(msg.Data) != "X" {
			t.Fatalf("live data = %q, want X", msg.Data)
		}
	case <-time.After(time.Second):
		t.Fatal("replaying client did not get message published by another client")
	}
}

func TestClientInboxBufferFillsWhileReplayWriterBlocked(t *testing.T) {
	resetShutdownForTest(t)

	dir := t.TempDir()
	if err := writeReplayLog(filepath.Join(dir, "log-seed.bin")); err != nil {
		t.Fatal(err)
	}

	g := &lib.Gossip{LogsFolder: dir}
	s := &Server{G: g}
	if err := s.Init(); err != nil {
		t.Fatal(err)
	}

	serverConn, clientConn := stdnet.Pipe()
	defer clientConn.Close()
	defer serverConn.Close()
	go s.handleConnection(serverConn)

	if _, err := clientConn.Write([]byte(HandshakePrefix)); err != nil {
		t.Fatal(err)
	}
	if err := lib.WriteInt64(clientConn, 0); err != nil {
		t.Fatal(err)
	}
	var handshake [len(Handshake)]byte
	if _, err := clientConn.Read(handshake[:]); err != nil {
		t.Fatal(err)
	}
	if string(handshake[:]) != Handshake {
		t.Fatalf("handshake = %q, want %q", string(handshake[:]), Handshake)
	}

	waitForClientInbox(t, s)

	for i := 0; i < 101; i++ {
		id := fmt.Sprintf("live-%03d", i)
		if err := s.G.PublishCAS("folder", id, 0, []byte("live")); err != nil {
			t.Fatal(err)
		}
	}

	if got := clientInboxLen(t, s); got != 100 {
		t.Fatalf("client inbox len = %d, want full buffer length 100", got)
	}
}

func resetShutdownForTest(t *testing.T) {
	t.Helper()

	Shutdown = sync.WaitGroup{}
	ShuttingDown = make(chan struct{})
	t.Cleanup(func() {
		close(ShuttingDown)
		Shutdown.Wait()
	})
}

func waitForClientInbox(t *testing.T, s *Server) {
	t.Helper()

	deadline := time.After(time.Second)
	tick := time.NewTicker(time.Millisecond)
	defer tick.Stop()
	for {
		if clientInboxLen(t, s) >= 0 {
			return
		}
		select {
		case <-deadline:
			t.Fatal("server did not register client inbox")
		case <-tick.C:
		}
	}
}

func clientInboxLen(t *testing.T, s *Server) int {
	t.Helper()

	s.m.Lock()
	defer s.m.Unlock()
	for _, inbox := range s.clients {
		return len(inbox)
	}
	return -1
}

func writeReplayLog(path string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	l, err := lib.CreateLog(path)
	if err != nil {
		return err
	}
	defer l.Close()
	if _, err := l.Append(lib.Msg{Topic: "folder", ID: "replay-1", TS: 1, Data: []byte("A")}); err != nil {
		return err
	}
	if _, err := l.Append(lib.Msg{Topic: "folder", ID: "replay-2", TS: 2, Data: []byte("B")}); err != nil {
		return err
	}
	return l.Flush()
}
