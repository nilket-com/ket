# ADR 0718: the daemon's configuration can name an S3-compatible object store

Status: proposed, 2026-11-12 (Claude, Codex). Census and RustFS steps cleared by Codex 01a1ce49, 01a1ce4e, 01a1ce5a; this plan accepted with corrections by Codex 01a1ce63 (`reviews/rustfs-path-20261112/CODEX-ADR-0718-PLAN.txt`). Implementation lands with this plan. Records: `reviews/rustfs-path-20261112/` (`CENSUS.md`, `RESULT-STEPS-1-2.md`).

## Why
- The operator's day-one install requires remote object tiering (RustFS in development; B2, S3, GCS later).
- `load_node_bootstrap_config` sets `l2: None` unconditionally (`config.rs:1036`) and `tiering` to `SweeperConfig::default()`. The configuration file has no object-store section. **A daemon started from its configuration cannot tier to anything.**
- The adapter and the node already accept it: `ObjectStoreConfig::Remote` → `RemoteObjectStore` (OpenDAL), built at startup (`node.rs:1878`). Against RustFS 1.0.4 the adapter tests pass and the three-node workflow passed once (one process, simulated network, ParentDecisions; one pass after two failing executions, not a rate).

## Decision
**An optional `[storage.object_store]` section maps to `ObjectStoreConfig::Remote` with the S3-compatible provider. Absent: no object store and default tiering settings, as before. Present: every field is validated locally at load, and a section that fails that validation fails startup; nothing falls back to no store or to a local directory. Local validation cannot know whether the endpoint answers, the bucket exists or the keys are accepted: those failures appear at run time through the existing verification, read and tiering paths.**

```toml
[storage.object_store]
provider = "s3-compatible"            # the only value accepted by this ADR
endpoint = "http://10.0.0.6:9000"     # absolute http or https URL: host, optional port, nothing else
bucket = "ket-prod"
region = "us-east-1"
prefix = "cluster-a"                  # optional, default ""; key prefix inside the bucket
addressing = "path"                   # "path" or "virtual-host"; required, no default
access_key_id_file = "/etc/ket/object-store.access-key"       # relative paths resolve from the
secret_access_key_file = "/etc/ket/object-store.secret-key"   # configuration file's directory

[storage.tiering]                     # optional; only read when object_store is present
scan_interval_millis = 5000           # default 5000, must be > 0
min_slab_bytes = 0                    # default 0
```

1. **Parsing.** `RawObjectStoreConfig` and `RawTieringConfig` with `deny_unknown_fields`. **`RawStorageConfig` gains `deny_unknown_fields` as well: a deliberate tightening.** A misspelled `[storage.object_stor]`, or any other unknown key under `[storage]` that was silently ignored until now, fails the load. Every currently supported `[storage]` key stays declared and the existing configuration tests run unchanged. **A parse failure is reported as file, line and column and nothing the file holds.** The parser's message and its rendering of the source line are both withheld, for every section: either can repeat a wrongly typed value, an unknown key's name or a whole line, any of which may be a secret written in the wrong place. In their place the error names any required key that is absent, from a fixed list in the loader (`node`, `node.node_id`, `node.advertise_address`, `storage`, `storage.data_dir`, `identity`, and the seven required object-store keys once that section exists). This changes the wording of every configuration parse error; one existing test asserted the old wording and now asserts the position.
2. **Validation at load, before any store is opened.** Unknown provider. Endpoint: must parse as an absolute URL with scheme http or https and a non-empty host; user information, a query, a fragment and any path other than `/` are refused (a location inside the bucket is `prefix`; the adapter receives scheme, host and port only). Bucket or region empty or holding whitespace. `addressing` other than the two values. `prefix` not plain segments (`.`, `..`, empty segments, a leading slash, whitespace, backslash). A key file unreadable, empty after trimming, or holding more than one token. `[storage.tiering]` without `[storage.object_store]`; `scan_interval_millis = 0`. Each names its field and never repeats a configured value. A missing required field, an unknown field (an inline `secret_access_key` among them), a wrongly typed value and a misspelled section fail in the parser, with the position-only report of point 1. No anonymous mode, no inline secrets, no session token in this ADR.
3. **Secrets.** Read once at load from the two files. Errors name the path and the field, never the content. `S3CompatibleProviderConfig`, `BackblazeB2ProviderConfig` and `GoogleCloudStorageProviderConfig` get hand-written `Debug` that prints `<redacted>` or `<unset>` for every credential-bearing field: S3 access key id, secret key and session token; B2 application key id and key; GCS credential, credential path, service account and token. They derived `Debug` until now, and the node configuration is debug-printed by tools such as the measurement harness's effective-config record.
4. **Tiering defaults an operator gets with the section present and no `[storage.tiering]`:** scan every 5 s, tier every sealed slab (`min_slab_bytes` 0), no upload rate limit, `RehydrationPolicy::default()`, slab size from the existing `storage.slab_max_bytes`. Stated in the configuration reference with the sentence that a slab tiers once sealed and that MVCC retention (`time.mvcc_retention_window_secs`) governs rewrites.
5. **No startup reachability probe.** `NodeBootstrapConfig::validate` builds the store, which surfaces configuration errors and contacts nothing. An unreachable store, a missing bucket or refused keys are met wherever the node first uses the store: a tiering pass on a fresh store, or the verification of tiered slabs and reads on reopening a store that already tiered. Named here so it is a decision, not an accident; a startup probe is a separate question because it changes availability when the store is down at boot.
6. **Unchanged:** the adapter, the sweeper, every wire and file format, LocalFs (still test- and harness-only, not selectable from the file), B2 and GCS (adapter variants stay; no daemon configuration for them yet).

## The decision-mode gap (named, not solved here)
- The daemon's configuration always yields `DecisionMode::Today` (`config.rs:1040`); `ParentDecisions` is `pub(crate)`, selectable only by `with_parent_decisions_for_disposable_store`, refused without development keys (`node.rs:1787`), and its module says it is development-only.
- **The three-node workflow that passed on RustFS runs in ParentDecisions.** So after this ADR a real daemon can tier to RustFS in Today mode, and the existing three-node evidence is for the other mode.
- What exists for Today mode with tiering: the single-node measurement harness (LocalFs) and the unit and integration tests of the sweeper. **No three-node Today-mode tiering workflow exists, local or remote.**
- This ADR does not add a mode setting. **ParentDecisions is the intended three-node workflow and the only one demonstrated; a daemon cannot select it.** Exposing it to a daemon needs its own contract and review. Today mode with its known three-node admission findings is not an approved alternative release path. **So this ADR does not complete the three-host release path; it removes one of its two configuration blockers.**

## Acceptance (finite)
Configuration tests in `config.rs` (no network):
1. No section → `l2` is `None` and the tiering fields equal the defaults.
2. The example above with two temp key files → `ObjectStoreConfig::Remote` with exactly those fields, and the store builds; `addressing = "virtual-host"` sets `virtual_host_style`; no `prefix` and an https endpoint with a trailing slash load.
3. Every refusal in Decision 2, each asserting the error kind and the field name and that neither key, an inline secret nor endpoint credentials appear in the message; malformed URL, bad host and a credential-bearing endpoint among them.
4. `[storage.object_stor]` (misspelled) fails. A string holding a sentinel in a numeric field and an unknown key named with a sentinel both fail without the sentinel in the message, at the offending key's line and column; an absent required object-store key is named.
5. Debug of the built node configuration and of each of the three provider structs, alone and nested, holds no credential field's value.
6. `[storage.tiering]` values reach `SweeperConfig`.

The tests are in `tests/config_object_store.rs`. External, four tests in three files, each `#[ignore]`d, run by explicit selection, failing rather than skipping without a server: `tests/l2_rustfs.rs` (two) and the RustFS variant of `tests/three_node_workflow.rs` (one), bundled as reviewed; and `a_loaded_file_builds_a_store_rustfs_accepts` (one), which loads a TOML file through `load_node_bootstrap_config` and round-trips one object through the built store. **That last test proves the mapping, not that a daemon tiers. The others' limits stay as recorded:** in-process nodes, ParentDecisions, small objects, no pagination, no faults, one provider.

## Delivery and check
One plan commit and one impl commit, signed, landing together. Gate on bigfoot: fmt, workspace check, full `nextest -p wendelon` checkpoint (the same recipe as ADR 0717).

**External evidence is carried beside the gate, not run again for the final candidate and not part of its pass count.** The four external tests were run once, by name, on the candidate as it stood before the parse-diagnostic correction (log `adr0718-focused-1.log`, 60b58011…): the two adapter tests and the configuration-to-store test passed; **the RustFS three-node workflow failed with `QUERY_STALLED` after the follower restart (DC-223, open).** The correction that followed changed `config.rs`'s parse-error report, `tests/config_loader.rs` and `tests/config_object_store.rs` only; the adapter, `storage/l2.rs`, the workflow and its support file are byte-identical between the two. The final candidate itself has no fresh external run.

RustFS workflow executions to date, as a list and not a rate: setup variables missing (refused); `Fenced` at an unwaited reopen; `QUERY_STALLED` after tiering; pass (with the bounded fence wait); `QUERY_STALLED` after the follower restart. All in one topology: three nodes in one process, one loopback-bound RustFS reached through two ssh forwards.

## First checkpoint: FAILED, and what followed
- **Checkpoint 1 on the first signed pair (plan e4a093c16, impl 549cdeeca; kept at `refs/archive/adr0718-pair1`): FAILED 4524/4525** (`adr0718-gate1.log`, b8dd4f54…). The failing test was the local variant of `tests/three_node_workflow.rs`, which this ADR changes: after "leader 3 stopped, 1 took over" a write entering at node 1 returned the client's own dial error naming stopped node 3. It is recorded as a failure, not a flake; a later pass does not erase it.
- **Read-only trace** (`reviews/rustfs-path-20261112/TRACE-0718-GATE.md`): the log proves the stage order and the error text only. From source: the error is the public client's own connect; a failed connect ends `table_write` at once, with no other attempt or replica; the simulated transport reports a dial to an unpublished node as `NotFound` where QUIC reports `ConnectionRefused`, which the test already retried. **What named node 3 at runtime is not established.** The new tiering-node restart stage moves leadership, so the failover stage can now stop a leader other than node 1; whether the old flow could reach the same dial is not established either.
- **Test-only correction, folded into this implementation** (source cleared, Codex 01a1ce93): the workflow's `write_one` retries, with the same row and write id and inside the existing bound, only on the exact simulated refusal (kind and whole message) for a node the fixture itself stopped; nine negative controls stay final; `read_failure`, reads and every stage and assertion are unchanged. A deterministic test, `a_hint_to_a_stopped_node_fails_the_clients_call_on_its_own_connect`, builds one possible route (a running node hinting a stopped one) and pins what the client returns. It is not the checkpoint's history.
- **DC-224 (open candidate):** after a leader stop the public client can be sent to the stopped node and fail its call on its own connect. Not shown to be a defect; nothing in the product is changed for it here. The restart stage stays in the workflow.
- Checkpoint 2 is run once on the final pair and reported with checkpoint 1.

## Limits
- No readiness claim. Not shown by this ADR: a daemon process tiering to RustFS (that is the first act of step 4), real network between nodes, interrupted uploads, outages, backup/restore, failover under remote faults, B2/S3/GCS.
- DC-223 (a remote-backed query fails `QUERY_STALLED`; default stall bound 250 ms; the expired wait unidentified) is open and untouched. **The remote three-node workflow's last execution failed on it. No remote-read readiness is claimed.**
- Section 8 stays FAILED, unwaived; memory ownership is unattributed.
