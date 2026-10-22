# Actual Replicon admission, before implementation

Pinned bevy_replicon 0.45.2 and Bevy 0.20.0, default graphics features disabled.
Bevy 0.20.0 declares Rust 1.97.1 in the official sparse index. Install that named
minimal toolchain without updating stable or touching existing worker processes.
Source discovery reference: 926dcae1106c7a87a293fd72a92d4f233e8ff183 upstream;
Cargo.lock must bind the actual published package and dependencies before execution.

First proof: actual server/client Apps and upstream ServerTestAppExt handshakes,
initial complete byte resource, 32 replacement revisions, skipped exchanges with
later recovery, exact receiver values, explicit removal/reinsertion and reconnect.
Use full resource replication first, then native Diffable/ACK/history behavior.
The server's real serialized messages must go through real client decoders. Do
not clone authoritative state into receivers or substitute our sender in place
of the native implementation. Staged small recipient proofs precede 20k scaling.
Upstream ECS/app/setup costs and wire framing are distinct from Rill's current
kernel/body-only fixture. No cross-harness speed conclusion until this mismatch
is addressed. A newer-toolchain build failure is not a performance defeat.

Implementation refinement: the first fixture uses one replicated component
containing the complete dataset, to exercise native entity despawn/reinsertion.
It does not yet admit the separate resource-replication path described above.

The first executed fixture demonstrated that disconnecting and reusing the same
client App retains its previous remote entities, giving two datasets after a
fresh handshake. This is a fixture cleanup assumption, not an engine speed loss.
The native reset clears its entity map and buffers but preserves entity data;
Remote's docs describe preserving entities to start a local server. Reconnect
therefore creates a fresh client App, matching Rill's fresh Receiver/session
contract. Keep the initial negative probe; retain the one-dataset assertion.
