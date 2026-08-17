package net

import (
	"io"

	"github.com/ohait/gossip/lib"
)

const (
	HandshakePrefix = "GOSSIP"   // 6-byte prefix sent by the client
	Handshake       = "GOSSIP\n" // 7-byte ack sent by the server

	CmdCommit    = byte('M') // server -> client: a durable message, from replay or a live broadcast
	CmdCAS       = byte('C') // client -> server: write with compare-and-swap semantics
	CmdSignal    = byte('S') // either direction: transient, never persisted or replayed
	CmdReplyDone = byte('D') // server -> client: marks the end of the initial replay
)

func writeMsg(w io.Writer, cmd byte, m lib.Msg) error {
	data, err := lib.EncodePayload(m.Data)
	if err != nil {
		return err
	}
	m.Data = data
	if _, err := w.Write([]byte{cmd}); err != nil {
		return err
	}
	if err := lib.WriteID(w, m.Topic); err != nil {
		return err
	}
	if err := lib.WriteID(w, m.ID); err != nil {
		return err
	}
	if err := lib.WriteInt64(w, m.TS); err != nil {
		return err
	}
	return lib.WriteBytes(w, m.Data)
}

func readMsg(r io.Reader, maxData int) (m lib.Msg, err error) {
	if m.Topic, err = lib.ReadID(r); err != nil {
		return
	}
	if m.ID, err = lib.ReadID(r); err != nil {
		return
	}
	if m.TS, err = lib.ReadInt64(r); err != nil {
		return
	}
	if m.Data, err = lib.ReadBytes(r, maxData); err != nil {
		return
	}
	m.Data, err = lib.DecodePayload(m.Data)
	return
}
