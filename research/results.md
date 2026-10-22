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
in evidence/fanout-v1-build.json. Twenty-thousand-recipient coverage is running.

This is a development screen with one process per cell, forced final refresh and
other workers active. Sender timings exclude setup and receiver verification but
include timer overhead; wall time includes decoding and verification. Byte counts
are encoded bodies and omit transport framing. There is no final acceptance or
sustained network latency claim. Upstream adapters, overlapping scopes and serial
paired comparisons remain open. Rill State has not passed its outstanding
performance gate. No Nocturne code has been changed by this project.
