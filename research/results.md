# Initial status

Experimental. No accepted performance winner. Nine contract tests cover exact ACKs,
loss/reordering, deletion/reinsertion, missing baselines, reconnect rejection,
encoder/decoder failure, logical bounds and value/cache reclamation. The model
runs 30 policy/history/cache combinations. A tenth test checks both fixture codecs
across empty and boundary lengths. All-target Clippy passes with warnings denied.
Selected logs: evidence/contracts-v4.log and evidence/clippy-v3.log.

## First frozen screen

The 40 fixed configurations (five policies × two codecs × two history windows ×
two cache capacities) passed all four workloads at 128 recipients: **160 fresh
processes**, two parallel workers, complete receiver-content checks, final
convergence, and no failures. All observations and matched contrasts, including
losses, are in smoke-v1-review.json. Binary/source/compiler/lock/flags bindings are
in evidence/fanout-v1-build.json. The same 160-process coverage also passed at **20,000 simulated recipients**,
32 publication frames plus two final recovery rounds. The complete audited
20,000-recipient results are in recipients-20k-v1-review.json. Both screens used
two workers and had zero failed processes.

This is a development screen with one process per cell, forced final refresh and
other workers active. Sender timings exclude setup and receiver verification but
include timer overhead; wall time includes decoding and verification. Byte counts
are encoded bodies and omit transport framing. There is no final acceptance or
sustained network latency claim. Upstream adapters, overlapping scopes and serial
paired comparisons remain open. Rill State has not passed its outstanding
performance gate. No Nocturne code has been changed by this project.

## Example fixed profile, with tradeoffs

The following is a fixed reference profile (index codec, history 16, cache 64),
not a winning configuration selected per workload. Sender time is the sum across
32 publication frames and two recovery rounds, including prepare/ACK calls and
timer overhead. These are individual exploratory observations, not intervals.
Body MB is decimal and includes attempted loss/duplicate bodies.

| Workload | Strategy | Sender ms | Attempted body MB |
|---|---|---:|---:|
| Synchronized | Shared full | 229.849 | 1398.080 |
| Synchronized | Shared delta | 249.280 | 97.280 |
| Synchronized | Individual delta | 593.626 | 97.280 |
| Dense changes | Shared full | 199.544 | 1398.080 |
| Dense changes | Shared delta | 250.173 | 2626.880 |
| Dense changes | Shared adaptive | 228.088 | 1357.120 |

Shared delta's synchronized body traffic is about 93% lower than shared full in
this fixture, while measured sender elapsed time is higher. Dense deltas are larger than full
snapshots; the adaptive policy avoids that expansion by also encoding/comparing
full state. That extra work is charged. Shared full remains a strong sender-time control.
All other profiles and losses remain in the machine-readable review. No fixed
policy dominates all metrics, and no upstream replication engine is yet admitted.

## Byte-codec admission, second frozen baseline

The normal library still has zero dependencies. An opt-in `bytes::ByteRuns` codec
now supports arbitrary byte payloads and size changes with a checked decode-output
limit. Twenty fixed block/gap configurations are explored. The comparison-only
feature adds actual pinned bsdiff 0.2.1, qbsdiff 1.4.4, bidiff/bipatch 1.0.0 and an
LZ4 wrapper. Default and tuned native parameters remain separate configurations.
These are **three delta algorithm families**, not 52 replication engines.

All 52 combinations (40 run configurations including LZ4, ten upstream codec
configurations, two full controls) passed 520 component smoke processes and the
**6,240-process full component grid**. Complete reconstructed bytes and matching
trace checksums are verified, including empty, sparse, dense, random, shifted and
resized payloads. The actual adapter path through Rill's Sender/Receiver passed
36 small-recipient processes. Its corresponding **36-process 20k study also passed**, including actual receiver
reconstruction, loss and final convergence. These use the same Rill kernel with
codec adapters; they are not separate upstream replication-engine runs.

Fourteen checks now pass across contract/model/codec tests. All-target/all-feature
Clippy passes. Pinned bidiff panics on an empty source; the retained native probe
shows this and its explicit compatibility adapter sends a tagged full replacement.
All adapters pay the same delta/full tag. Nothing treats the native panic as a
speed win. Default qbsdiff and its explicitly tuned small-buffer version remain
separate, so a poor untuned parameter is not the only external comparison.

`byte-codec-v2-review.json` retains every cell median and fixed candidate/external
comparison, including encoding, decoding and size losses. **No fixed run candidate
has lower observed encoding medians in every external cell.** These parallel
component medians are not confidence intervals or a full-engine comparison.

An actual Replicon 0.45.2/Bevy 0.20.0 fixture passed **four native scenarios**
using a separate Rust 1.97.1 toolchain: full/native-diff replication × loss/no-loss,
each with four real client Apps, 32 complete dataset revisions, despawn/reinsert
and fresh-receiver reconnect. Native serialized messages go through actual client
decoders. The first fixture reused a client App after disconnect and found retained
entities; native reset intentionally preserves entity data. Its negative probe is
retained, and v2 uses a fresh receiver incarnation, matching Rill Sessions.
See evidence/native-replicon-proof-v2-receipt.json. These are component-container
capability proofs, not the separate resource path or a performance comparison.
Timing fairness, native ACK-delay/reordering/history-expiry coverage, alternate
engine adapters and 20k native-engine scaling remain open. The source-discovery catalog explicitly separates algorithms,
replication engines and support packages; metadata discovery is not admission.

## Native bulk and exhaustive 20k decoder replay

The native bulk router passed 48 direct client-App processes at 1/8/64/512 clients,
including full/native-diff, loss/duplicates/reordering and 0/4/24-frame ordered ACK
delays. The fixed-clock replay harness then passed 48 corresponding staged
processes and all **12 processes at 20,000 simultaneous native server peers**.
Every process decodes every recipient using an actual fresh native client App;
responses must match the predicted native ACK/handshake bytes exactly. Aggregate:
240,000 native decoder replays and 12,080,000 complete-value/ACK-frame checks.
The ACKed target must be applied by logical server ACK delivery, not merely
accepted into a native buffer. See native-replay-method.md for the proof argument,
resource costs and actual limits. This is not a live-socket or end-to-end latency
measurement, and it does not hold 20,000 native client Apps in memory at once.

The new generic wire boundary passes 17 all-feature checks and all-target Clippy;
14 default-core checks also pass on the declared Rust 1.85 minimum. It adds stack
headers, contiguous/vectored output, bounded borrowed decoding, borrowed Receiver
application and exact attempt ACKs. No normal dependency is added. Source-bound
validation lives in evidence/wire-v5-validation.json.

Matched engine framing/input costs, a Rill authored-change input equivalent to
native Diffable, broad payload/scope cases, other engine adapters, library-only
memory, sustained latency and final serial paired statistics remain outstanding.
The full performance objective remains unmet.

## Matched snapshots and contiguous-history sender candidate

The v6 frozen binary (source `870849755e3e841c741be283c605efa49bd1cf77`)
uses one compiler/binary for native Replicon full snapshots and generic Rill
framed snapshots. All 1,044 configuration-admission processes and all 660
64-recipient payload/ACK/fault processes passed. The complete 330-process target
cohort is still running. The staged review retains every fixed contrast,
including time or byte losses. See matched-snapshot-method.md for costs,
equivalent inputs, remaining capability differences and reproduction paths.

The v7 candidate (source `09d8720ea42e557508449534b835f4ca0b56e603`) removes
linear sender-history searches and a duplicate cache lookup. All 1,044 actual
framed configuration-admission processes pass, plus 17 all-feature checks,
Clippy and 14 Rust 1.85 default-core checks. Its 108-process counterbalanced
20k v6/v7 comparison is queued behind the active v6 target cohort; no speed
improvement has been accepted. The old frozen executable remains the control.

Exact published Naia 0.25.0 archives are checksum-verified and selected as the
next independent native-engine admission baseline. Its local transport and
test clock are present in actual source. This is source inspection, not an
implemented adapter or defeated library; the separate admission plan names
the remaining proof gates.
