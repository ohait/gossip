# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

```bash
go build ./...    # build
go test ./...     # run all tests
go mod tidy       # sync dependencies
```

## What it is

A persistent pub/sub log service over TCP. Clients connect, send messages (`ID`, `timestamp`, `data`), which are appended to binary log files on disk and broadcast live to all other connected clients. On startup, the service replays `.bin` log files to rebuild the in-memory index.

## Architecture

**Module:** `oha.it/gossip` (Go 1.25), entry point at `cmd/main.go`.

- **`internal/service.go`** — `Service` struct with `index map[string]IndexEntry` (keyed by message ID) and a rolling `*Log`. `Init` replays log files; `Add` appends and broadcasts; `MaxData` caps incoming message size (default 10MB, max 1GB). Log files roll over at 200MB.
- **`internal/ipc.go`** — TCP server (`Bind`). Handshake: `GOSSIP\n`. Each connection reads incoming messages and spools broadcast messages back to the client. `Msg` is `{ID string, TS int64, Data []byte}`.
- **`internal/log.go`** — Append-only binary log file. `Append` writes an entry and returns its `IndexEntry` (offset + timestamp). `Range` iterates all entries for index rebuild. `Read(offset)` seeks and reads a single entry with hash verification.
- **`internal/enc.go`** — Big-endian, length-prefixed binary encoding helpers used throughout.
- **`internal/shutdown.go`** — `Shutdown chan struct{}` (closed on SIGINT/QUIT) and `Hold()`/`Wait()` for goroutine drain.

**Binary format per log entry:**
1. ID (length-prefixed string)
2. Timestamp (int64)
3. XXHash64 checksum (uint64)
4. Data length (int64) + raw bytes

**Constraints:** ID ≤ 256 bytes, data ≤ 1GB in storage; IPC cap is `Service.MaxData`.

**Dependency:** `github.com/cespare/xxhash/v2` for checksums.
