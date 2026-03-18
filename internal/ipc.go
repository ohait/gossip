package gossip

import (
	"errors"
	"io"
	"log"
	"net"
)

type Msg struct {
	ID   string
	TS   int64
	Data []byte
}

// Bind starts a TCP server on the specified address and listens for incoming connections.
func (s *Service) Bind(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
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
	return nil
}

func (s *Service) handleConnection(conn net.Conn) {
	defer conn.Close()
	// expect GOSSIP\n as the first bytes
	buf := make([]byte, 7)
	_, err := io.ReadFull(conn, buf)
	if err != nil {
		log.Printf("Error reading handshake: %v", err)
		return
	}
	if string(buf) != "GOSSIP\n" {
		conn.Write([]byte("Invalid handshake\n"))
		log.Printf("Invalid handshake: %s", string(buf))
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
		for {
			select {
			case msg, ok := <-inbox:
				if !ok {
					log.Printf("Inbox channel closed for %s", conn.RemoteAddr().String())
					return
				}
				err := WriteString(conn, msg.ID)
				if err != nil {
					log.Printf("Error writing message ID: %v", err)
					return
				}
				err = WriteInt64(conn, msg.TS)
				if err != nil {
					log.Printf("Error writing message TS: %v", err)
					return
				}
				err = WriteBytes(conn, msg.Data)
				if err != nil {
					log.Printf("Error writing message data: %v", err)
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
	msg.ID, err = ReadString(conn, 256)
	if err != nil {
		return
	}
	msg.TS, err = ReadInt64(conn)
	if err != nil {
		return
	}
	msg.Data, err = ReadBytes(conn, s.MaxData)
	return
}
