# gossip

`gossip` is a small persistent pub/sub log over TCP.

Clients `Publish` messages with:

- `id`
- `ts`
- `data`

The server appends each message to a binary log on disk and broadcasts it to connected clients. On startup, it replays the log files to rebuild its in-memory index.

Clients can also `Emit` the same payload as transient data. It is forwarded live to connected clients, but it is not written to disk and never appears in replay.

It's not meant to run in a distributed system, but just as an helper for applications that need a simple persistent pub/sub log.

`gossip` does not index the data, or provide any querying capability. Just a simple log with replay and pub/sub. The clients are expected to maintain their own state based on the messages they receive.

## Message model

Each message is:

- `ID string`
- `TS int64`
- `Data []byte`

The message ID is the key. For a given ID, the newest timestamp wins logically.

`Publish` updates the durable index and participates in replay. `Emit` is forwarded to currently connected clients only, does not update the durable index, and is not replayed after reconnect.

For a given `id`, `Publish` and `Emit` should not be mixed unless you explicitly want that behavior. `Emit` does not supersede the persisted state for the same `id`. If the last persisted value is `v1` and a later transient send carries `v2`, live clients may observe `v2`, but replay will still return `v1`.

If you need to remove durable state for an `id`, send a persisted tombstone. A transient send does not clear or replace the replayable value.

## Timestamp

`ts` is expected to be a nanosecond timestamp, typically produced by:

```go
ts := time.Now().UnixNano()
```

The code compares timestamps as plain `int64` values. If a message arrives for an existing ID and its timestamp is older or equal, it is ignored.

For a given `id`, a newer event replaces the older one logically. Older versions may still exist, and replay may include older or duplicate entries, but the latest timestamp is the one that defines current state.

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

## Replay

On connection, the client sends the last known TS minus a small delta (e.g. 5 seconds).

The server will then replay messages with `ts >= (lastKnownTS - delta)`. This allows the client to catch up on recent changes while avoiding a large replay of old data.

Replay is not a full-state snapshot. The server replays the latest known message for each `id` whose latest timestamp is within that replay window, and updates that happen during replay are then delivered on the live stream. Duplicates or older messages near the replay boundary are still possible.

## Future

### Compaction

Compaction will be added soon.

Compaction does not need to be all-or-nothing. It can be incremental.

For example, the system can:

- compact a single log by removing duplicates inside it
- join multiple logs into a new compacted log
- remove old logs after the compacted replacement is safely written

When compaction runs, it will keep the newest event for each `id` in the compacted output. Older log files will then be replaced by the compacted ones.

A crash in the middle of compaction is acceptable. It may leave multiple copies of the same logical event across log files, but duplicate data is not a correctness problem for `gossip`. The next compaction pass can prune those extra copies.

That means older superseded versions are not part of the long-term retention model. They may exist temporarily before compaction, or temporarily after an interrupted compaction, but they should be treated as obsolete.

Compaction might also remove old tombstone entries.

### Querying

There is no plan to add querying capabilities to the server, but we might allow late fetches of messages by ID.

## Notes

- IDs are limited to 256 bytes.
- Message data is capped by server configuration (`MaxData`), default 10 MiB.
- Empty data is valid and is the delete marker.
