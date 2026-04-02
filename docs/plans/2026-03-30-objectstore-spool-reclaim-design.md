# Objectstore Spool Reclaim Design

## Goal

Clarify `objectstore` local disk semantics so local files act only as a write spool, not as a second durable/read cache tier. Once a record is safely committed to remote storage, its local sealed segment should become reclaimable.

## Problem

Today `pkg/store/objectstore` keeps local sealed segment files after remote flush succeeds. That creates two issues:

1. The local disk role is ambiguous. It behaves partly like a write spool and partly like a local file cache.
2. Local disk usage can grow without an explicit budget, even when a higher local tier such as `filestore` already exists and owns the read-cache role.

This is especially confusing in ordered-tier setups such as:

- `tier 0 = filestore`
- `tier 1 = objectstore`

In that layout, `filestore` already provides read-through local file caching. Keeping `objectstore` local sealed segments indefinitely duplicates responsibility.

## Design Principles

- `filestore` and other regular tiers are the user-visible local cache tiers.
- `objectstore` local disk is an implementation detail for pure-streaming writes.
- Remote checkpoint publication is the durability boundary for reclaiming local spool files.
- Open readers must remain valid even if a segment becomes reclaimable.
- Recovery after pod restart must continue serving remote-flushed data even when local disk is empty.

## Proposed Model

### Local Disk Role

`objectstore` local disk is reduced to:

- active write segments
- sealed but not-yet-remotely-committed segments
- temporarily retained sealed segments that still have open readers

It is not a persistent read cache.

### Remote Role

Remote storage remains the source of truth after flush:

- packed shard segments for small records
- direct blobs for oversized records
- shard checkpoints for remote lookup
- legacy manifests only for compatibility paths

### Read Role

Read acceleration is split as follows:

- upper ordered tiers (`filestore`, `memstore`, `redisstore`) provide explicit cache-tier behavior
- `objectstore` keeps only in-memory block/page caches for remote read efficiency
- `objectstore` does not try to rebuild or retain a durable local file cache

## Segment State Machine

Each local segment moves through these states:

1. `active`
   - created by `BeginSet`
   - written under `ingest/active/...`
   - never reclaimable
2. `sealed_local`
   - writer `Close()` succeeds
   - catalog entry points at local `SegmentPath`
   - caller-visible
   - not yet reclaimable
3. `flushing_remote`
   - flusher is reading the sealed local segment and uploading remote data
   - not yet reclaimable
4. `remote_committed`
   - remote object upload succeeded
   - shard checkpoint publish succeeded
   - local catalog entry has been updated with `RemotePath`
   - local file may become reclaimable
5. `reclaimable`
   - all live records in the local segment are `remote_committed`
   - no open local readers reference the segment
6. `deleted`
   - local sealed segment removed from disk

## Reclaim Rules

A local sealed segment may be deleted only when all of the following hold:

- every live catalog entry that still points at that segment also has a committed `RemotePath`
- the corresponding remote checkpoint write already succeeded
- no active local reader still references the segment

This means reclaim is segment-based, not page-based. The existing write path already treats the sealed local file as the physical unit of publication and flush input, so reclaim must follow that unit.

## Reader Safety

Local reader safety should use process-local segment refcounts:

- key: `SegmentPath`
- `openLocalEntry(...)` increments the count before returning a `ReadCloser`
- the returned closer decrements the count on `Close()`
- reclaim may delete a segment only when its refcount reaches zero

This state must not be persisted in the catalog. Reader counts are runtime-only ownership data.

## Recovery Semantics

When a pod restarts with an empty `dataDir`:

- remote-flushed entries must still be readable from remote checkpoints or manifests
- missing local segment paths should be repaired to remote-backed entries, not treated as fatal loss when `RemotePath` exists
- no attempt is made to rebuild a local file cache from remote data during startup

This preserves the current good property: a cold pod can still serve cached data from remote storage without going back to origin.

## Interaction with Ordered Tiers

Example: `WithTiers(filestore, objectstore)`

1. New pod starts with empty local directories.
2. Request hits `filestore`: miss.
3. Request hits `objectstore`: remote hit.
4. Response streams to caller.
5. `tier 0` (`filestore`) is repopulated through normal ordered-tier fill.
6. `objectstore` local spool is not retained as a second disk cache after remote commit.

This keeps responsibility clean:

- `filestore` is the local file cache tier
- `objectstore` local disk is the write spool

## Scope

In scope:

- segment refcount tracking
- reclaim on remote commit
- recovery-safe repair of missing local segments
- tests covering overwrite, delete, remote flush, and open-reader protection

Out of scope:

- a disk-backed read cache inside `objectstore`
- startup prewarming from remote into local disk
- byte-budgeted local spool eviction before remote commit

## Recommendation

Implement reclaim immediately after successful remote checkpoint publication, guarded by per-segment reader refcounts. Keep remote data and upper tiers as the only read-cache layers, and treat `objectstore` local disk as spool-only going forward.
