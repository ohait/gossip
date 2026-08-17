package net

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/ohait/gossip/lib"
)

type Server struct {
	G       *lib.Gossip
	MaxData int
	m       sync.Mutex
	clients map[string]chan<- Outbound
}

type Outbound struct {
	Cmd byte
	Msg lib.Msg
}

func (s *Server) Init() error {
	if s.G == nil {
		return fmt.Errorf("tcp: Gossip is nil")
	}
	if s.MaxData == 0 {
		s.MaxData = 10 * 1024 * 1024
	}

	s.m.Lock()
	s.clients = make(map[string]chan<- Outbound)
	s.m.Unlock()

	return s.G.Init(func(topic, id string, ts int64, data []byte, persist bool) error {
		cmd := byte(CmdSignal)
		if persist {
			cmd = CmdCommit
		}
		s.broadcast(cmd, lib.Msg{Topic: topic, ID: id, TS: ts, Data: data})
		return nil
	})
}

func (s *Server) broadcast(cmd byte, msg lib.Msg) {
	s.m.Lock()
	defer s.m.Unlock()
	for _, inbox := range s.clients {
		select {
		case inbox <- Outbound{Cmd: cmd, Msg: msg}:
		default:
		}
	}
}

// Bind starts a TCP server on the specified address and listens for incoming connections.
// Returns the address actually bound (useful when addr is "host:0" for a random port).
func (s *Server) Bind(addr string) (string, error) {
	s.m.Lock()
	initialized := s.clients != nil
	s.m.Unlock()
	if !initialized {
		// Without this the first connection would panic assigning into a nil
		// map, long after the mistake was made.
		return "", fmt.Errorf("tcp: Bind called before Init")
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return "", err
	}
	go func() {
		<-ShuttingDown
		ln.Close()
	}()
	Shutdown.Go(func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				select {
				case <-ShuttingDown:
					return
				default:
				}
				// TODO: consider sleep+retry on EMFILE/ENFILE (fd exhaustion)
				log.Printf("Accept error: %v", err)
				return
			}
			go s.handleConnection(conn)
		}
	})
	return ln.Addr().String(), nil
}

func (s *Server) handleConnection(conn net.Conn) {
	defer conn.Close()

	// expect GOSSIP<since:int64>
	var prefix [len(HandshakePrefix)]byte
	conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
		if err == io.EOF {
			return
		}
		log.Printf("Error reading handshake prefix: %v", err)
		return
	}
	if string(prefix[:]) != HandshakePrefix {
		conn.Write([]byte("Invalid handshake\n"))
		log.Printf("Invalid handshake prefix: %q", string(prefix[:]))
		return
	}
	since, err := lib.ReadInt64(conn)
	if err != nil {
		log.Printf("Error reading handshake ts: %v", err)
		return
	}
	_, err = conn.Write([]byte(Handshake))
	if err != nil {
		log.Printf("Error writing handshake response: %v", err)
		return
	}
	inbox := make(chan Outbound, 100)
	s.m.Lock()
	s.clients[conn.RemoteAddr().String()] = inbox
	s.m.Unlock()

	// spool messages to the client in a separate goroutine
	Shutdown.Go(func() {
		defer func() {
			log.Printf("Closing connection to %s", conn.RemoteAddr().String())
			conn.Close()
		}()

		err := s.G.Replay(since, func(msg lib.Msg) error {
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			return writeMsg(conn, CmdCommit, msg)
		})
		if err != nil {
			log.Printf("Error replaying messages: %v", err)
			return
		}
		conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
		_, err = conn.Write([]byte{CmdReplyDone})
		if err != nil {
			log.Printf("Error replaying messages: %v", err)
			return
		}
		for {
			select {
			case item, ok := <-inbox:
				if !ok {
					log.Printf("Inbox channel closed for %s", conn.RemoteAddr().String())
					return
				}
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				err := writeMsg(conn, item.Cmd, item.Msg)
				if err != nil {
					log.Printf("Error writing message: %v", err)
					return
				}
			case <-ShuttingDown:
				log.Printf("Shutting down connection to %s", conn.RemoteAddr().String())
				return
			}
		}
	})

	defer func() {
		s.m.Lock()
		log.Printf("Removing client %s", conn.RemoteAddr().String())
		delete(s.clients, conn.RemoteAddr().String())
		s.m.Unlock()
		close(inbox) // close the inbox channel to signal the spooler goroutine to exit
	}()
	for {
		conn.SetReadDeadline(time.Time{}) // not timeout between messages
		var cmd [1]byte
		_, err = io.ReadFull(conn, cmd[:])
		if err != nil {
			return
		}
		conn.SetReadDeadline(time.Now().Add(10 * time.Second)) // timeout for the rest of the message after reading the command byte
		switch cmd[0] {
		case CmdCAS:
			msg, err := readMsg(conn, s.MaxData)
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("Client %s disconnected", conn.RemoteAddr().String())
				} else {
					conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
					conn.Write([]byte("Error decoding message\n"))
					log.Printf("Error decoding message: %v", err)
				}
				return
			}
			// The store notifies the subscription registered in Init, which is
			// what puts the accepted message — carrying the timestamp CAS
			// assigned — in front of every connected peer.
			err = s.G.PublishCAS(msg.Topic, msg.ID, msg.TS, msg.Data)

			if err != nil {
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				conn.Write([]byte("Error adding message\n"))
				log.Printf("Error adding message: %v", err)
				return
			}
			log.Printf("Added message: ID=%s, TS=%d, DataSize=%d", msg.ID, msg.TS, len(msg.Data))
		case CmdSignal:
			msg, err := readMsg(conn, s.MaxData)
			if err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("Client %s disconnected", conn.RemoteAddr().String())
				} else {
					conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
					conn.Write([]byte("Error decoding signal\n"))
					log.Printf("Error decoding signal: %v", err)
				}
				return
			}
			// A signal is not stored, so it never reaches the store at all: the
			// server forwards it straight to the connected peers.
			err = s.G.Signal(msg.Topic, msg.ID, msg.TS, msg.Data)

			log.Printf("Forwarded signal: ID=%s, TS=%d, DataSize=%d", msg.ID, msg.TS, len(msg.Data))
		default:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			conn.Write([]byte("Unknown command\n"))
			log.Printf("Unknown command byte: %q", cmd[0])
			return
		}
	}
}
