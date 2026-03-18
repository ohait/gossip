package gossip

import (
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"os"
)

type Msg struct {
	ID   string
	TS   int64
	Data []byte
}

func (m Msg) WriteTo(w io.Writer) (n int64, err error) {
	_, err = w.Write([]byte{CmdMessage})
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

func (m *Msg) Decode(r io.Reader, skipCmd bool, maxSize int) (n int64, err error) {
	if !skipCmd {
		var cmd [1]byte
		if _, err = io.ReadFull(r, cmd[:]); err != nil {
			return
		}
		if cmd[0] != CmdMessage {
			err = fmt.Errorf("unexpected command byte: %q", cmd[0])
			return
		}
		n++
	}
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

func (s *Service) replay(since int64, w io.Writer) error {
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
			_, err := msg.WriteTo(w)
			return err
		})
		f.Close()
		if err != nil {
			return err
		}
	}
	_, err := w.Write([]byte{CmdReplyDone})
	return err
}

func (s *Service) handleConnection(conn net.Conn) {
	defer conn.Close()
	// expect GOSSIP<since:int64>
	var prefix [len(HandshakePrefix)]byte
	if _, err := io.ReadFull(conn, prefix[:]); err != nil {
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
	inbox := make(chan *Msg, 100)
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
			case msg, ok := <-inbox:
				if !ok {
					log.Printf("Inbox channel closed for %s", conn.RemoteAddr().String())
					return
				}
				if _, err := msg.WriteTo(conn); err != nil {
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
		msg, err := s.readRequest(conn)
		if err != nil {
			if errors.Is(err, io.EOF) {
				log.Printf("Client %s disconnected", conn.RemoteAddr().String())
				return
			}
			conn.Write([]byte("Error reading request\n"))
			log.Printf("Error reading request: %v", err)
			return
		}
		err = s.Add(msg)
		if err != nil {
			conn.Write([]byte("Error adding message\n"))
			log.Printf("Error adding message: %v", err)
			return
		}
		log.Printf("Added message: ID=%s, TS=%d, DataSize=%d", msg.ID, msg.TS, len(msg.Data))
	}
}

func (s *Service) readRequest(conn io.Reader) (msg Msg, err error) {
	_, err = msg.Decode(conn, false, s.MaxData)
	return
}
