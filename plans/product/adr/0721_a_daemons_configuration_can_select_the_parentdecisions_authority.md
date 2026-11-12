# ADR 0721: a daemon's configuration can select the ParentDecisions authority

Status: proposed, 2026-11-12 (Claude, Codex). Census and design reviewed by Codex 01a1d118, 01a1d11d, 01a1d172 (`reviews/pd-daemon-20261112/`). Implementation lands with this plan. ADR 0720 (membership fixed at the first admitted write) was the boundary required first.

## Why
- The three-node workflow that keeps every acknowledged row through tiering, restart and failover runs under ParentDecisions. **A daemon started from its configuration could only run Today** (ADR 0718 named this gap): the mode had no configuration key, and bootstrap refused it without development keys.
- Its decision-signing keys could only be the development keys, which are derivable from a node id by anyone.
- Reading the code for real keys found three hazards that development keys had hidden:
  - a leader bound a node's key from **its own** trust map, immutably. With real keys, a disagreement between two configuration files would bind the wrong key for good;
  - a node restarted with a different key still reported ready, and everything it signed was refused;
  - `[identity]` and `[cluster]` ignored unknown keys, so a mistyped key name meant "unset".

## Decision
1. **Selection.** `[cluster] decision_authority = "today" | "parent-decisions"`, default `"today"`. It applies to a fresh store; a store keeps the authority it was created under and the other is refused, as before. No conversion.
2. **Decision keys come from the file.** With `parent-decisions`, `[identity] decision_signing_secret` and `[identity.trusted_decision_signers]` are required; nothing is defaulted from a file. With `today` they are refused. Format and checks are those of the lease role: the local key is in the map and matches, no key is reused from another role, no key names two nodes.
3. **Development keys.** A recognised development decision key is accepted only under `identity.allow_dev_keys`, as for the other roles. This replaces the rule that the mode itself required `allow_dev_keys`. Programmatic construction keeps the development default.
4. **Unknown keys.** `[identity]` and `[cluster]` refuse unknown keys. Parse failures keep ADR 0718's contract: position only.
5. **Secrets.** The decision secret is absent from `Debug` output of the raw and built configurations and from every error.
6. **A forwarded binding carries the sender's key.** `BindSigner` forwarded to the leader names the verifying key of the secret the sender signs with. The leader still looks the sender up in its own trust map, and binds that entry **only if the two are equal**; otherwise it refuses `SignerKeyDisagrees` and binds nothing, so correcting a file can still bind the right key. The sender's key never replaces the lookup. This is key equality for an authenticated sender, not proof of possession.
7. **A forwarded decision proposal is versioned.** `DECISION_PROPOSE_REQ` and its answer carry the ParentDecisions envelope version, now 10, and nothing else:
   - the frame reader, which does not know a node's mode, accepts that version on those two types (and on Raft frames) and version 1 everywhere, as before;
   - **the mode-aware handler judges the version before decoding the body**: a ParentDecisions node takes only its envelope; a Today node takes only version 1, and then answers `ModeToday` as before;
   - the client judges the answer's type and version before decoding it;
   - the other decision requests (lookup, cut) keep version 1. The classifier is an envelope family; it dispatches nothing as Raft.
   No rolling upgrade between envelope versions is claimed.
8. **A key that is not the bound one.** One comparison, `local_signer_binding`, of the key a store signs with against the replicated binding of its incarnation: `NotCurrent`, `Unbound`, `Bound`, `Mismatch`.
   - Start: if replayed state already shows `Mismatch`, the node does not start. `Unbound` starts: control and catch-up traffic must run to establish it.
   - Readiness: `Mismatch` is reported as `DecisionSignerMismatch`, also when it is learned after start.
   - No rebind and no rotation.
   **Readiness is a diagnosis, not the fence.** The fence is unchanged: evidence signed with another key fails verification against the replicated binding wherever it is counted.
9. **Clearer refusals and text.** The refusal of a node MVCC retention window names `time.mvcc_retention_window_secs = 0`. The module text that said nothing could select the mode is corrected.

## What selecting it does not give
**ParentDecisions selected by a daemon is a fresh, disposable checkpoint, not a durable deployment.** Real keys do not change that. Stated in the example configuration and here:
- no backup, no member replacement, no resizing; membership is fixed at the first admitted write (ADR 0720), so the cluster is formed and its membership checked before writing;
- reclamation can stall while a member is absent;
- a limited surface: table writes, single-row deletes, streamed LINEARIZABLE table queries, single-key KV;
- key custody beyond a configured secret, and key rotation, do not exist.
Member replacement and tested backup and restore are recorded release requirements (operator, 2026-11-12); resizing is later.

## Algebra
- Identity of a binding: (node, store incarnation). Key compared: exactly that pair's replicated entry against the local secret's verifying key. A stale incarnation is its own result, never judged as a mismatch.
- A binding is created only when `leader_map[sender] = sender_key`. Both sides' configurations must agree for a key to become immutable.
- Version gate on a forwarded proposal: accepted iff `frame.version = envelope(mode of the receiving node)`, evaluated before decode, in the handler for requests and in the client for answers.

## Compatibility
- Raft and forwarded-proposal envelope 9 → 10. **No on-disk format change**: `DecisionCommand::BindSigner` as logged is unchanged, as are the decision state and the store marker; confirmed by the format-17 corpus passing unchanged.
- Existing Today configurations load unchanged unless they contain an unknown key under `[identity]` or `[cluster]`, which is now refused.

## Acceptance
- Loader: default Today; ParentDecisions with its keys loads and prints without the secret; missing secret, missing map, keys without the authority, a development key without `allow_dev_keys`, a non-zero MVCC window and a malformed secret are refused as specified; a mistyped identity key, cluster key or authority value fails by position only.
- Identity: the decision role's own tests (none existed): keys of their own pass without development keys; development keys need `allow_dev_keys`; absent, differing, role-reused and twice-named keys are refused without echoing a key.
- Binding comparison: the four results at the reducer's state.
- Forwarded binding: a disagreeing key binds nothing; an unknown sender is refused whatever key it sends; the refusal is appended to the wire enum.
- Version gate: the frame reader accepts envelope 10 on the two proposal types and refuses it on lookup and cut; retired envelopes are refused on a proposal; the answer check refuses an ADR 0720 answer, a version-1 answer and another type; lookups keep version 1.
- Three nodes, in process: the existing workflow passes with each node signing with a secret of its own and every verifying key named explicitly.
- **Real processes:** one loopback smoke with three `nilket wendelon` daemons from configuration files. Its four executions are the next section.
- One checkpoint gate, shared with ADR 0722. **Its outcome is recorded below: it failed.**

## The real-process smoke: four executions
All sealed under `reviews/pd-daemon-20261112/`; the test is `crates/wendelon/crates/wendelon-client/tests/parent_decisions_processes.rs`, run under an external owner.
1. **FAILED.** The fixture gave the data client an operator certificate; data requests are accepted only from a `data_node` identity, in every mode. A wrong premise in the test, not a product failure. It exposed DC-225 (the refusal reaches the client as a closed stream) and DC-226 (a background sweep that ParentDecisions peers refuse).
2. **FAILED**, corrected fixture (operator-created tenant 1; a storage certificate for it under a non-member node name): every functional step passed; the last node, by then leader with no reachable peer, did not stop within the daemon's 35 s watchdog (DC-227).
3. **FAILED**, the same on an instrumented tree, diagnostic only: it named the wait, the QUIC endpoint's idle wait after close. ADR 0722 bounds it.
4. **PASSED**, uninstrumented, on ADR 0722 and this ADR composed: three daemons from configuration files with non-development keys, a local-sealed key store and mutual TLS; tenant and schema by the operator; one write and the exact row read back through a storage client built from each node, before and after a follower restart; every daemon, the last included, exits 0 on SIGTERM. One pass, on one host; not a rate.
What the passing execution does not show: which node answered a query (the public client chooses), Raft voters as distinct from the epoch's peers, a committed acknowledgement as distinct from a visible row, an object store, a leader failure, or more than one host.

## Checkpoint: FAILED, and what followed
- **The one shared checkpoint for ADR 0722 and ADR 0721 FAILED, 4554 of 4556**, on the composed candidate before a last correction (`reviews/pd-daemon-20261112/adr0722-0721-gate1.log`, sha256 d156256a…):
  - `adr0093_quic_config_construction_is_confined_to_the_tls_choke_point`: a real rule violation in ADR 0722's unit test, which built a Quinn client configuration outside `network/tls.rs`. Corrected: the existing client constructor in `network/tls.rs` was extracted, unchanged and in the same order, into `quic_client_config_from_rustls`, which `client_config` now calls and the test uses. On the delivered code the rule's own test and the unit test pass (architecture_test 206 of 206 and the handle-result test).
  - `authority_encoded_live_heap_and_process_resident_assay`: DC-167, an open parallel-only candidate (900 bytes of a process-wide live-heap counter not returned; the same two numbers as on 2026-11-05, when it measured 0 of 500 quiet and 8 of 1,500 under load on that day's tree). **Unresolved and unattributed.** Neither ADR touches that test or the code it measures; that does not exonerate them.
- **The uninstrumented three-daemon smoke passed once**, on the candidate before that behaviour-preserving extraction (`reviews/pd-daemon-20261112/smoke-run4-accept/`, seal 33035333…). **It was not rerun on the delivered tip. No passing full checkpoint exists for the delivered tip.**
- **This exact candidate is delivered under a recorded, one-off known-failure exception** (Codex 01a1d1f6). It is not a general policy, not an exoneration, not a "flake" label and not a green gate.

## Limits
- The handler's refusal of a wrong-version proposal on a live node, a mismatching binding learned after start on a live node, and a start refused by a replayed mismatch are covered by the comparison and gate functions' tests and by code review of their single call sites; they are not each driven through a live cluster in this ADR unless the smoke or a focused test says so in the evidence.
- The other ParentDecisions-only data frames keep frame version 1; this ADR versions the forwarded proposal only.
- Not addressed: member replacement, backup and restore (both recorded release requirements), resizing, key rotation, recovery from a contained membership divergence, RustFS with real processes, three hosts, DC-167, DC-223, DC-224, DC-225, DC-226, section 8 (FAILED, unwaived). DC-227 is addressed by ADR 0722 only for the wait it names.
