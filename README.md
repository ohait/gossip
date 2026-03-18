# gossip

`gossip` is a small persistent pub/sub log over TCP.

Clients send messages with:

- `id`
- `ts`
- `data`

The server appends each message to a binary log on disk and broadcasts it to connected clients. On startup, it replays the log files to rebuild its in-memory index.

## Message model

Each message is:

- `ID string`
- `TS int64`
- `Data []byte`

The message ID is the key. For a given ID, the newest timestamp wins.

## Timestamp

`ts` is expected to be a nanosecond timestamp, typically produced by:

```go
ts := time.Now().UnixNano()
```

The code compares timestamps as plain `int64` values. If a message arrives for an existing ID and its timestamp is older or equal, it is ignored.

For a given `id`, a newer event replaces the older one logically. Older versions may still exist on disk for now, but they are superseded and should not be treated as current state.

## Delete

Delete is represented as writing the same ID with empty data:

```go
Msg{
	ID:   "some-key",
	TS:   time.Now().UnixNano(),
	Data: nil, // or []byte{}
}
```

That tombstone is stored in the log and broadcast like any other message. Consumers should treat `len(data) == 0` as deleted.

## How it works

1. A client connects to the TCP server.
2. The client sends the `GOSSIP` handshake prefix and a `since` timestamp.
3. The server replies with `GOSSIP\n`.
4. The server replays messages with `ts >= since`.
5. New incoming messages are appended to disk and broadcast to connected clients.

## Future

Compaction will be added soon.

Compaction does not need to be all-or-nothing. It can be incremental.

For example, the system can:

- compact a single log by removing duplicates inside it
- join multiple logs into a new compacted log
- remove old logs after the compacted replacement is safely written

When compaction runs, it will keep only the newest event for each `id` in the compacted output. Older log files will then be replaced by the compacted ones.

A crash in the middle of compaction is acceptable. It may leave multiple copies of the same logical event across log files, but duplicate data is not a correctness problem for `gossip`. The next compaction pass can prune those extra copies.

That means older superseded versions are not part of the long-term retention model. They may exist temporarily before compaction, or temporarily after an interrupted compaction, but they should be treated as obsolete.

## Notes

- IDs are limited to 256 bytes.
- Message data is capped by server configuration (`MaxData`), default 10 MiB.
- Empty data is valid and is the delete marker.
