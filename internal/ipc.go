package gossip

import (
	"errors"
	"io"
	"log"
	"net"
	"os"
	"time"
)

type Msg struct {
	ID   string
	TS   int64
	Data []byte
}

func (m Msg) WriteTo(w io.Writer) (n int64, err error) {
	return m.writeTo(w, CmdMessage)
}

func (m Msg) WriteSignalTo(w io.Writer) (n int64, err error) {
	return m.writeTo(w, CmdSignal)
}

func (m Msg) writeTo(w io.Writer, cmd byte) (n int64, err error) {
	_, err = w.Write([]byte{cmd})
	if err != nil {
		return
	}
	err = WriteString(w, m.ID)
	if err != nil {
		return
	}
	err = WriteInt64(w, m.TS)
	if err != nil {
		return
	}
	err = WriteBytes(w, m.Data)
	if err != nil {
		return
	}
	return 1 + int64(len(m.ID)) + 8 + 8 + int64(len(m.Data)), nil
}

func (m *Msg) Decode(r io.Reader, maxSize int) (n int64, err error) {
	m.ID, err = ReadString(r, 256)
	if err != nil {
		return
	}
	m.TS, err = ReadInt64(r)
	if err != nil {
		return
	}
	m.Data, err = ReadBytes(r, maxSize)
	if err != nil {
		return
	}
	n = int64(len(m.ID)) + 8 + 8 + int64(len(m.Data))
	return
}

// Bind starts a TCP server on the specified address and listens for incoming connections.
// Returns the address actually bound (useful when addr is "host:0" for a random port).
func (s *Service) Bind(addr string) (string, error) {
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

func (s *Service) replay(since int64, conn net.Conn) error {
	files := map[string]struct{}{}
	ts := map[string]int64{}
	s.m.Lock()
	for id, entry := range s.index {
		if entry.TS >= since {
			ts[id] = entry.TS
			files[entry.File] = struct{}{}
		}
	}
	s.m.Unlock()
	for file := range files {
		f, err := os.Open(file)
		if err != nil {
			return err
		}
		l := &Log{path: file, f: f}
		err = l.RangeSince(since, func(msg Msg) error {
			if msg.TS != ts[msg.ID] {
				return nil // skip older versions of the same ID
			}
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			_, err := msg.WriteTo(conn)
			return err
		})
		f.Close()
		if err != nil {
			return err
		}
	}
	conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err := conn.Write([]byte{CmdReplyDone})
	return err
}

func (s *Service) handleConnection(conn net.Conn) {
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
	since, err := ReadInt64(conn)
	if err != nil {
		log.Printf("Error reading handshake ts: %v", err)
		return
	}
	_, err = conn.Write([]byte(Handshake))
	if err != nil {
		log.Printf("Error writing handshake response: %v", err)
		return
	}
	inbox := make(chan outboundMsg, 100)
	s.m.Lock()
	s.clients[conn.RemoteAddr().String()] = inbox
	s.m.Unlock()

	// spool messages to the client in a separate goroutine
	Shutdown.Go(func() {
		defer func() {
			log.Printf("Closing connection to %s", conn.RemoteAddr().String())
			conn.Close()
		}()
		if err := s.replay(since, conn); err != nil {
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
				var err error
				switch item.cmd {
				case CmdSignal:
					_, err = item.msg.WriteSignalTo(conn)
				default:
					_, err = item.msg.WriteTo(conn)
				}
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
		case CmdMessage:
			var msg Msg
			if _, err := msg.Decode(conn, s.MaxData); err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("Client %s disconnected", conn.RemoteAddr().String())
				} else {
					conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
					conn.Write([]byte("Error decoding message\n"))
					log.Printf("Error decoding message: %v", err)
				}
				return
			}
			err = s.Add(msg)
			if err != nil {
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				conn.Write([]byte("Error adding message\n"))
				log.Printf("Error adding message: %v", err)
				return
			}
			log.Printf("Added message: ID=%s, TS=%d, DataSize=%d", msg.ID, msg.TS, len(msg.Data))
		case CmdSignal:
			var msg Msg
			if _, err := msg.Decode(conn, s.MaxData); err != nil {
				if errors.Is(err, io.EOF) {
					log.Printf("Client %s disconnected", conn.RemoteAddr().String())
				} else {
					conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
					conn.Write([]byte("Error decoding signal\n"))
					log.Printf("Error decoding signal: %v", err)
				}
				return
			}
			if err := s.Signal(msg); err != nil {
				conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
				conn.Write([]byte("Error forwarding signal\n"))
				log.Printf("Error forwarding signal: %v", err)
				return
			}
			log.Printf("Forwarded signal: ID=%s, TS=%d, DataSize=%d", msg.ID, msg.TS, len(msg.Data))
		default:
			conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
			conn.Write([]byte("Unknown command\n"))
			log.Printf("Unknown command byte: %q", cmd[0])
			return
		}
	}
}
