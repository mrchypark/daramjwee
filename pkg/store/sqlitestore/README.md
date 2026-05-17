# SQLiteStore

`sqlitestore` implements `daramjwee.Store` on top of a local SQLite database.

Writes are staged into temporary chunk rows and become visible only when the
returned `WriteSink` is closed successfully. `Abort` removes staged chunks, and
`Delete` advances the key generation so an older open writer cannot publish a
stale value after the delete.

```go
store, err := sqlitestore.New(
	"/var/lib/daramjwee/cache.db",
	log.NewNopLogger(),
	sqlitestore.WithChunkSize(512<<10),
)
if err != nil {
	return err
}
defer store.Close()
```

The store opens SQLite in WAL mode. Keep the database file together with its
`-wal` and `-shm` sidecars when copying or backing up the cache. Close streams
returned by `GetStream` promptly; they hold a read transaction for snapshot
consistency and long-lived readers can delay WAL checkpointing. Staged chunks are
cleaned on normal close/abort paths. If a process exits while a writer is open,
orphaned staged chunks from that process remain until a future store open
reclaims staged chunks older than 24 hours; committed reads ignore those rows. If
that cleanup removes chunks from an unusually long-lived active writer, its later
`Close` fails instead of publishing a partial object.
