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
