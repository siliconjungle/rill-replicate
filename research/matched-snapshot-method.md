# Matched snapshot boundary, v6/v7

The child workspace now compiles actual Replicon and Rill in the same Rust 1.97.1
release binary. Both replicate a complete `Dataset { revision, bytes }` with the
same model generation, fixed logical clock, message-fault schedule and reliable
FIFO ACK delays. Application revisions are serialized, not inferred from the
replication header to save bytes. Rill serializes postcard once on publication;
that work is charged. Replicon receives a complete component replacement and
uses its native serialization and scheduling. Native authored diffs remain a
separate input contract requiring a matching Rill authored-change path.

Rill uses its actual generic Sender, codec adapters, contiguous wire headers and
Receiver. Preparing a body is insufficient: timing includes constructing the
complete outgoing application message. Its ordinary host adapter visits peers,
retries their unacknowledged current state, and suppresses a send once current
state is acknowledged. Replicon uses its native dirty tracking and retry policy.
Initial full Rill data is reliable; later data uses the same unreliable fault
schedule as native mutations. Reliable native updates are not deliberately
damaged. Both sides pay the actual messages their policy generates.

The common transport/oracle records complete messages and predicts responses.
All server peers coexist. Each independent real receiver is then replayed through
all recorded ticks, checking exact emitted ACK messages, complete reconstructed
Dataset values, final convergence, and application before server ACK delivery.
The native replay proof argument still applies. No recipient sampling, receiver
state injection, successful-only rows or guessed unverified ACKs are allowed.

The three main measurements are:

- Sender work: publication, server scheduling/prepare/complete-message export,
  ACK inbox insertion, and ACK decoding/processing on the next step.
- Receiver work: incoming message insertion, native update or Rill wire/payload/
  application decoding, and actual ACK encoding/export. Full model comparison
  and receiver construction are outside this timer and recorded separately.
- Bytes: every complete outgoing data and ACK application message, including
  attempted drops and fault-injected duplicates. Channel IDs and transport-owned
  session identities are out of band for both. Native protocol setup bytes and
  time are recorded separately. Socket, encryption and transport framing are
  absent from both fixtures.

The preregistered finite v6 cohorts have 1,044 small-scale processes covering all
261 policy/codec/compression configurations, 660 representative payload/ACK/fault
processes at 64 peers, and 330 target processes at 20k. The parameter-space plan
is not a claim that every possible Cartesian combination has run. The 52 codecs
contain three upstream delta algorithm families and tuned/compressed variants;
they are not 52 independent replication engines.

The first two v6 cohorts passed, as did the 1,044-process v7 admission cohort.
The 20k v6 cohort is still running at this commit. The v7 candidate removes
repeated contiguous sender-history scans and a duplicate cache lookup. Seventeen
all-feature tests, Clippy, and fourteen default-core tests on Rust 1.85 pass.
Its 108-process, counterbalanced v6/v7 comparison is a dependent pipeline after
the current memory-heavy target cohort, avoiding four concurrent large retained
trace fixtures. It will fail if model, wire-byte or fault counts differ.

Every fixed exploratory engine contrast is retained by
`tools/review_matched_snapshot.py`; the ratio is Rill/native, so a value above one
is a loss. A median across different workload cells is descriptive, not repeated
statistical evidence. Paired v7 observations also remain exploratory while other
workers use the host. There is no final speed acceptance or universal winner.

Important limits: Replicon provides additional ECS, entity, visibility and
scheduling capabilities. This fixture exercises one complete dataset per scope;
it does not establish equivalence of all engine features. Only one receiver is
resident at a time. Entire-process peak RSS includes all retained traces and
oracle bookkeeping and cannot be called library-only server memory. This is
neither 20k sockets nor sustained game load or p99 latency. Other independent
engine adapters, authored-input comparisons, larger multi-scope cases, isolated
paired statistics and holdouts remain required by the full objective.
