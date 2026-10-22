# First Replicate experiment

Registered before implementation/timing, 2026-10-22. Experimental, no winner.

Replicate is a new generic Rust project. Rill State remains unfinished; switching
work does not accept its performance gate. Its public draft PR is
https://github.com/siliconjungle/rill-state/pull/1. Existing State controllers are
allowed to finish without intervention. Their mutable evidence stays in State.

## Contract

One authoritative replaceable dataset per replication scope, arbitrary immutable
application state, pluggable full/delta codec. Each session is a unique connection
and scope incarnation. A delta names an exactly acknowledged retained baseline.
Receiver keeps bounded revision history, rejects wrong-session/stale data, and
requests a full refresh if its baseline is missing. Server history eviction also
forces full fallback. Application ACKs name exact emitted attempts, never an
arbitrary revision. No delivery implies an ACK. Slow sessions cannot pin history.

Snapshots represent complete scope membership, including removals. Events are a
separate Rill Delivery concern: a replacement delta does not preserve every
intermediate event. First kernel does not yet provide arbitrary overlapping
subscription scopes or a wire format, transport, authentication or durable state.

## Fixed first alternatives

Full replacement per recipient; per-recipient acknowledged delta; shared encoded
delta keyed by exact baseline; shared delta with full-size fallback. All four use
identical codecs, complete receiver reconstruction and ACK/loss semantics. Shared
full messages are also reused in shared modes. Compare at two bounded history
windows (4, 16) and two cache capacities (0, 64), 16 configurations. No per-case
oracle selection or hiding unfavorable results. A cache miss remains correct.

First development fixtures: 20,000 simulated recipients, 256 u64 records, at least
32 revisions, sparse (1 record) and dense changes, synchronized and staggered ACK
baselines, deterministic packet loss, ACK loss, duplication, reverse delivery,
stalled recipients, reconnect and history eviction. Include a record-map fixture
with explicit removal/reinsertion. Full contents checked after every applied
update and at final convergence. Model tests and memory-release tests precede
comparative timing. Report encoder calls, encoded body bytes, logical transmitted
body bytes (including dropped/duplicate attempts), cache hits, fallbacks, retained
revisions/cache bytes/attempt metadata and sender time separately from receiver
verification. Header/transport bytes must be reported separately when implemented.

Parallel process screens discover tradeoffs. Final performance claims require
frozen source/compiler/lock/flags/binary bindings, fresh serial randomized paired
processes, sustained workloads and confidence intervals. Other active workers
mean results are exploratory. 20,000 simulated recipients are not 20,000 sockets.
No comparison to an upstream engine is accepted until an actual adapter proves
equivalent applicable semantics. A source catalog is not a list of beaten engines.

## Initial primary sources, retrieved 2026-10-22

- https://new.gafferongames.com/post/snapshot_compression/ — acknowledges the
  baseline before delta construction; initial full-state path.
- https://docs.rs/bevy_replicon/latest/bevy_replicon/ — mutation ACKs, visibility,
  removals, per-entity freshness and priorities. Candidate, not yet adapted.
- https://github.com/cBournhonesque/lightyear — upstream currently integrates
  Replicon for replication; do not count the same replication path twice as
  independent algorithms. Candidate, not yet adapted or pinned.
- https://github.com/naia-lib/naia — entity/field replication and scope membership.
  Candidate, not yet adapted or pinned.

Source inspection and exact version pins remain open. No dependency or source
from private Nocturne is needed for the public library.
