# History-informed architecture review

This review records why the current architecture exists and which parts should
remain. It is based on every reachable commit at `4842553` (306 commits, 268 on
`main`, 186 first-parent commits, 28 merges, and 30 release tags from v0.1.0 to
v0.15.0).

## Goal then and now

The first README described a pragmatic, inexpensive, "good enough" hybrid
cache: a fast local tier in front of object storage, streaming reads, negative
caching, freshness, and background refresh without a large framework.

The product is now an ordered N-tier, whole-object cache. Memory, file, and
object stores are peers in one tier list. Reads are stream-through; a cache fill
is published only after EOF and close, while an early close aborts it. The cache
also owns validators and 200/304/404 results, stale-while-revalidate, promotion,
fanout, bounded background work, miss coalescing, promotion probation, and
single-process generation fences.

That is a wider product, but not a different one. Ordered tiers generalize the
original local-plus-object model. A separate `DurableTier` does not add a
capability that the tier list currently lacks.

## Evolution and its consequences

| Phase | Material change | Current conclusion |
| --- | --- | --- |
| Bootstrap | Two-tier streaming API, metadata/TTL, negative entries, eviction and refresh | The small streaming cache goal remains valid. |
| Backend growth | File, generic, Redis, and object-backed stores; freshness fixes | Store diversity is intentional; cache policy must stay store-agnostic. |
| March 2026 redesign | Pure streaming, ordered tiers, and the objectstore segment engine landed together in an 83-file, +11,720/-1,282 commit | Ordered tiers survived implementation. The separate durable-tier design was abandoned in the same change and is historical, not a pending contract. |
| Concurrency hardening | Writer-lifetime locks caused races/orphans; generation-coordinated staged writes replaced them | The write coordinator and generation fences are essential correctness machinery. |
| Shared runtime | CacheGroup gained weighted queues while standalone retained the older worker strategies | The two schedulers have different public behavior. Their common surface may stay small; merging engines is not presently justified. |
| Performance passes | Pools and allocation reductions were added; unsafe reuse caused use-after-free fixes and rollback | Prefer ownership clarity over speculative pooling. |
| Coalescing | Miss coalescing was added, removed for simplicity, then restored with a 200 ms waiter bound | The bounded leader/waiter algorithm is a measured feature, not a general shared-response abstraction. |
| Phase 1 API | Planner and ReadSession were exported, but the Planner only shadowed the old decision tree and ReadSession exposed unused lifecycle concepts | One Planner must be authoritative. ReadSession compatibility should not drive new internal design. |

Production Go grew from 1,382 lines initially to 2,315 at v0.1, 8,059 at
v0.4, 11,321 at v0.8, 12,994 at v0.9.2, and 14,812 at v0.15. Growth alone is
not the problem: the costly parts are duplicated authority, speculative public
surface, and documentation that describes code that never existed.

## Target boundaries

The read path has one decision flow:

```text
tier I/O -> Observation -> Planner.Plan -> canonical ReadPlan -> executor
                                                        |
                                                        +-> response lifecycle
                                                        +-> staged publish/fanout
                                                        +-> refresh scheduling
```

`Planner` owns only pure policy. The executor owns I/O, revalidation, body
lifetime, and side effects. It accepts only canonical plan shapes. Runtime
planning always supplies private valid/invalid generation state; the public
`Plan(Observation)` method retains its v0.15 behavior and the public
`Observation` layout remains source compatible. Unknown internal states fail
closed.

The remaining responsibilities stay separate:

- ordered-tier lookup determines source, freshness, and upper-tier health;
- the write coordinator owns staged top-tier publication and generation
  invalidation;
- the miss coordinator owns only bounded leader registration and notification;
- the runtime owns bounded job admission and exact-once `Run` or `Discard`;
- stores own persistence details, including objectstore block/checkpoint caches.

## Refactor decisions

### P0: do now

- Make the Planner authoritative for lower-tier hits and remove the older
  action enum/decision function.
- Require runtime generation validity in lower-tier observations and revalidate
  before returning 304 or starting a side effect.
- Consolidate equivalent objectstore TTL caches and delete the duplicate page
  cache implementation.
- Remove duplicated wrappers, rejection branches, and test scaffolding where a
  single implementation expresses the same contract.
- Replace aspirational fill, stampede, and metrics documentation with current
  executable behavior.

### P1: compatibility cleanup

- Keep the v0.15 `ReadSession` and `Outcome` names for source compatibility, but
  do not route miss coordination or publication through them. Internally they
  remain an exact-once cleanup guard until a versioned API removal is chosen.
- Keep the Planner's public compatibility behavior, while runtime authorization
  supplies explicit private valid/invalid generation state to the same policy
  implementation.
- Review configuration by observable mode and delete only proven cross-mode
  validation/default duplication. A lower cognitive-complexity score alone is
  not a reason to add helpers.

### P2: defer until evidence exists

- Do not merge the standalone worker engine and CacheGroup scheduler until the
  public `all`/`pool` and weighted-group semantics can share one implementation
  without compatibility adapters.
- Do not redesign the write coordinator or split its state machine merely to
  reduce function complexity.
- Do not revive `DurableTier`, add a metrics framework, add a shared response
  stream, or add another planner/executor abstraction.

## Non-negotiable invariants

1. A partial read never publishes a complete cache entry.
2. Failure to acquire a fill writer leaves a successful origin body usable;
   write or finalization failure after streaming starts is reported and aborts
   publication.
3. Delete, set, refresh, and promotion cannot publish through an obsolete
   top-write generation.
4. Conditional 304 is returned only after the relevant generation is valid at
   execution time.
5. Every accepted background job runs or is discarded exactly once, including
   queue rejection and shutdown.
6. Shutdown remains bounded and caller cancellation does not silently transfer
   ownership of a response stream.
7. Miss waiters have bounded latency and re-read published state rather than
   sharing the leader's body.

## Acceptance evidence

- table tests cover every canonical Planner output plus invalid and unknown
  generation states;
- executor tests reject malformed plans without starting effects;
- conditional lower-tier tests mutate the generation between planning and
  execution and must not return 304;
- partial-read, cache-fill-failure, delete/set-versus-fill, promotion probation,
  refresh deduplication, slow-leader fallback, and runtime shutdown tests pass;
- the full suite, race detector, vet, duplicate-code check, and diff whitespace
  check pass;
- before/after traces agree on status, source tier, body bytes, publication,
  refresh, and fanout for fresh/stale, positive/negative, conditional, dirty
  upper-tier, and promotion-admission cases.

Historical plan documents remain useful provenance, but they are not current
contracts. Current behavior is defined by the public README/API and executable
tests.
