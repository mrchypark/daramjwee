# Fill Coordination

Concurrent cold misses are coordinated per cache key. The first caller becomes
the miss leader and fetches from the origin while streaming the response into
the top tier. A waiter blocks for at most 200 ms (or until its context ends),
then either re-reads the published top-tier value or performs its own origin
fetch. Waiters never share the leader's response stream.

Coalescing is skipped when an earlier tier read failed because a safe top-tier
fill cannot be guaranteed. A slow or abandoned leader therefore adds only a
bounded delay, and a waiter can still make progress independently.

The miss coordinator owns only leader registration and notification. The
write coordinator separately owns the staged top-tier fill and its generation
fence. A fill becomes visible only after the source reaches EOF and the body is
closed; early close aborts it. `Set` or `Delete` advances the generation, so an
older fill or promotion cannot resurrect stale data.

These are deliberately separate guarantees:

- miss coordination reduces duplicate origin work for fast fills;
- staged publication prevents partial objects from becoming visible;
- generation validation prevents stale publication;
- failure to acquire the cache writer before streaming falls back to the
  successful origin response;
- a write or finalization failure after streaming starts is returned to the
  caller and the partial fill is never published.

See `tests/miss_coalescing_test.go` and the generation/write-coordinator tests
for the executable contract.
