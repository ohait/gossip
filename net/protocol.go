package net

import (
	"io"

	"github.com/ohait/gossip/lib"
)

const (
	HandshakePrefix = "GOSSIP"   // 6-byte prefix sent by the client
	Handshake       = "GOSSIP\n" // 7-byte ack sent by the server

	CmdLWW       = byte('M') // last write wins message command byte
	CmdCAS       = byte('C') // compare-and-swap command byte
	CmdSignal    = byte('S') // signal (like a message, but not persisted or replayed)
	CmdReplyDone = byte('D')
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
