# TODO

## COMPACT

periodically or at start, compact multiple logs into one and remove the old entries

heuristic will help to decide which logs to compact, and when.

since the upsert operations, we can compact logs and remove the old entries after, and a crash in the middle will just cause some extra work later.
