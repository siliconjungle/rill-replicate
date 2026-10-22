# Rill Replicate

Experimental generic Rust replication of replaceable state against acknowledged
baselines. No normal dependencies; library source forbids unsafe code.
MIT OR Apache-2.0. **No performance winner or network-capacity claim.**

A sender owns one complete immutable dataset per replication scope. Applications
provide any state type and a `Codec<T>` for full encoding, delta encoding and exact
reconstruction. A receiver applies only newer revisions, keeps a bounded history,
and rejects packets from the wrong connection/scope incarnation.

Five fixed strategies are available: full snapshots, shared full snapshots,
per-recipient deltas, shared baseline deltas, and shared deltas with a smaller-full
fallback. `Envelope` bodies are `Arc<[u8]>`; recipients with the same baseline can
share encoded bytes. Each emitted attempt has an exact `Receipt`. Call
`acknowledge` only after application on the receiver, including a confirmed duplicate
of an already applied revision. Network send completion is not an application ACK.

`Sender::publish` publishes a complete dataset. `add_peer` establishes a fresh
`Session`; `prepare` constructs an envelope. `Receiver::apply` returns `Updated`,
`Stale`, a missing-baseline error, or a codec/session error. On missing baseline,
request `Sender::resynchronize`, then send a full replacement. Sender-side history
expiry automatically selects full replacement. `remove_peer` invalidates local
attempts. Every reconnect, scope change and sender restart requires a unique Session.

The first [comparison protocol](research/protocol.md) was committed before the
implementation. It covers 40 fixed configurations across five policies, two codec
layouts, two history windows and two cache limits. The demonstration
[fanout experiment](examples/fanout.rs) verifies complete receiver contents while
separately recording sender time, encoded/transmitted body bytes, lag, cache reuse,
missing baselines and fallback. It uses simulated recipients, not sockets.
Upstream Replicon, Lightyear and Naia are research candidates, not defeated engines.
The optional `comparisons` feature admits actual binary-delta codecs separately;
`bytes::ByteRuns` is an opt-in generic byte codec with a configurable output limit.
The second study compares 52 codec combinations. None is an accepted winner.

```sh
RUSTUP_TOOLCHAIN=stable cargo test --release --all-targets --locked
RUSTUP_TOOLCHAIN=stable cargo clippy --all-targets --locked -- -D warnings
python3 tools/study.py freeze local-fanout
python3 tools/study.py run --binary local-fanout --label local-smoke --peers 128
python3 tools/study.py run --binary local-fanout --label local-20k --peers 20000
```

Freeze requires committed source and never overwrites an existing executable.
Parallel screens are exploratory; performance acceptance requires isolated paired
processes and equivalent upstream adapters. [Initial evidence](research/results.md)
records completed work and remaining gates.

Limits: logical revision/attempt/peer counts and encoded cache bytes are bounded.
Application payload allocation, external envelope handles and decoder allocation
are caller-owned; there is no total physical memory bound. A cache charges each
entry's body conservatively, even if entries share an Arc. Oldest attempts expire
at the per-peer cap, so late ACKs cannot update progress. Sessions are provided by
the application and must never be reused. The codec must be consistent across a
session, reconstruct exact state, and leave retained immutable inputs unchanged.
Errors before successful decoding preserve receiver state. Panicking application
code and destructors are not a transactional recovery boundary.

One scope is the first kernel. Arbitrary overlapping membership, framing,
authentication, transport, congestion control, durable recovery, and events are
not yet implemented. Complete map snapshots can represent membership/removals;
model tests check deletion and reinsertion. Rill Delivery remains a separate
scheduler; a chain of deltas cannot be treated as independently replaceable state.
