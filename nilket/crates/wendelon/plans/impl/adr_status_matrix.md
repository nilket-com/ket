# ADR Status Matrix

Date: 2026-04-23
Owner: `WS0 / A`
Roadmap: `plans/impl/roadmap.md`

## Purpose

This is the living status matrix for ADR control.

It has one job: make the status, owner, milestone gate, and next action for every ADR visible enough that CI, a dashboard, or a gate review can consume it without re-reading every ADR.

Status vocabulary is taken from the ADR files. A `Proposed` ADR is allowed only when it has a named owner and a blocking milestone or deliberate deferral.

## Matrix

| ADR | Source | Title | Current status | Owning workstream(s) | Blocking milestone | Last review date | Next action |
|---|---|---|---|---|---|---|---|
| `001` | `plans/adr/001_autonomous_healing_boundary.md` | Boundary of Autonomous Self-Healing vs. Infrastructure Provisioning | `Accepted` | `H` | `M3` | 2026-04-23 | Track drift against recovery/operator claims |
| `002` | `plans/adr/002_unified_hash_ring.md` | Unified Hash Ring Topology | `Accepted` | `B` + `F` | `M1` | 2026-04-23 | Track drift against membership and identity work |
| `003` | `plans/adr/003_stellarator_bifurcated_writes.md` | Stellarator Bifurcated Write Path | `Accepted (amended 2026-04-22)` | `A` + `B` | `M1` | 2026-04-23 | Keep B transition/fault evidence aligned with the outcome states |
| `004` | `plans/adr/004_zero_copy_transport.md` | Zero-Copy Custom Transport | `Accepted` | `C` | `M2` | 2026-04-23 | Keep scoped behind ADR `011` QUIC-first support language |
| `005` | `plans/adr/005_compute_pushdown_wpbc.md` | Compute Pushdown Bytecode | `Accepted` | `I` | `M4` | 2026-04-23 | Track Toron pushdown limits against ADR `034` |
| `006` | `plans/adr/006_tiered_storage_lazy_migration.md` | Tiered Storage and Lazy Migration | `Accepted (extended by ADRs 022, 023, 024)` | `B` + `C` | `M2` | 2026-04-23 | Keep extension ADRs explicit while they remain proposed |
| `007` | `plans/adr/007_raft_implementation_strategy.md` | Raft Implementation Strategy | `Accepted` | `B` | `M1` | 2026-04-23 | Track against Raft storage and control-ring changes |
| `008` | `plans/adr/008_dependency_selection.md` | Dependency Selection and Async Runtime Model | `Accepted` | `C` + `J` | `M0` | 2026-04-23 | Track dependency and crypto-boundary changes |
| `009` | `plans/adr/009_io_uring_threading_model.md` | `io_uring` Execution Topology | `Accepted (amended 2026-04-22)` | `A` + `C` + `D` | `M2` | 2026-04-23 | Keep topology notes, metrics, and benchmark evidence aligned |
| `010` | `plans/adr/010_serialization_tiers.md` | Tiered Serialization Strategy | `Accepted` | `B` + `I` | `M2` | 2026-04-23 | Track schema/wire compatibility changes |
| `011` | `plans/adr/011_auto_adaptive_transport.md` | QUIC First, Multi-Transport Later | `Accepted (amended 2026-04-22)` | `A` + `C` + `D` | `M2` | 2026-04-23 | Enforce transport claim discipline and overload evidence |
| `012` | `plans/adr/012_bounded_clock_drift.md` | Bounded Clock Drift and HLC Safety | `Accepted` | `B` + `D` | `M1` | 2026-04-23 | Track chaos/simulator clock-drift coverage |
| `013` | `plans/adr/013_time_travel_queries.md` | Time Travel Queries and MVCC Retention | `Accepted` | `B` + `G` | `M3` | 2026-04-23 | Track retention and audit retention claims together |
| `014` | `plans/adr/014_rustfs_test_backend.md` | RustFS as the OpenDAL Compatibility Test Backend | `Accepted` | `D` | `M2` | 2026-04-23 | Keep object-store compatibility evidence current |
| `015` | `plans/adr/015_segmented_wal_and_snapshot_lifecycle.md` | Segmented WAL and Snapshot Lifecycle | `Proposed` | `B` | `M1` | 2026-04-23 | Owner must close WAL/snapshot lifecycle questions before M1 exit |
| `016` | `plans/adr/016_durability_model_fsync_group_commit.md` | Explicit fsync Group Commit | `Proposed` | `B` | `M1` | 2026-04-23 | Owner must align fsync batching semantics with B evidence |
| `017` | `plans/adr/017_persistent_local_index_redb.md` | Persistent Local Index on redb | `Proposed` | `B` | `M2` | 2026-04-23 | Owner must decide acceptance or defer before M2 storage claims |
| `018` | `plans/adr/018_dual_ca_mtls_identity_authorization.md` | Dual-CA mTLS Identity and Authorization | `Proposed (amended 2026-04-22)` | `A` + `F` + `I` | `M3` | 2026-04-23 | Owner must freeze final OID after PEN assignment and accept before pilot credentials |
| `019` | `plans/adr/019_cipher_selection_and_fips_posture.md` | Cipher Selection and FIPS Posture | `Proposed` | `J` | `M5` | 2026-04-23 | Owner must align with CMVP/lab posture before regulated claims |
| `020` | `plans/adr/020_l2_envelope_encryption_and_dek_lifecycle.md` | L2 Envelope Encryption and DEK Lifecycle | `Proposed` | `E` + `J` | `M3` | 2026-04-23 | Owner must accept custody/rotation semantics before production custody gate |
| `021` | `plans/adr/021_tenant_aware_architecture_day_one.md` | Tenant-Aware Architecture from Day One | `Accepted` | `E` + `F` + `G` + `I` | `M3` | 2026-04-23 | Track tenant ID use across custody, authz, audit, and Toron |
| `022` | `plans/adr/022_l2_slab_format_footer_indexed_range_reads.md` | L2 Slab Format and Footer-Indexed Range Reads | `Proposed` | `B` + `C` | `M2` | 2026-04-23 | Owner must accept or defer before range-read support claims |
| `023` | `plans/adr/023_l2_upload_lifecycle.md` | L2 Upload Lifecycle | `Proposed` | `B` + `G` | `M2` | 2026-04-23 | Owner must align upload ledger semantics with audit/evidence before M2 |
| `024` | `plans/adr/024_l2_orphan_cleanup_remote_reclaim.md` | L2 Orphan Cleanup and Remote Reclaim | `Proposed` | `C` + `G` | `M2` | 2026-04-23 | Owner must fix reclaim TTL/accounting language before M2 docs truth-up |
| `025` | `plans/adr/025_audit_event_model_and_tamper_evident_hash_chain.md` | Audit Event Model and Tamper-Evident Hash Chain | `Proposed (amended 2026-04-22)` | `A` + `G` + `H` | `M3` | 2026-04-23 | Owner must close encoding/signing/retention specifics before pilot audit claims |
| `026` | `plans/adr/026_fips_140_3_certification_target.md` | FIPS 140-3 Certification Target and Module Boundary | `Proposed` | `J` | `M5` | 2026-04-23 | Owner must keep CMVP language synchronized with lab tracker |
| `027` | `plans/adr/027_observability_posture.md` | Observability Posture | `Accepted` | `C` + `D` + `G` + `I` | `M2` | 2026-04-23 | Track metric/trace catalog and alert inventory |
| `028` | `plans/adr/028_deterministic_simulator_and_asyncfileops_abstraction.md` | Deterministic Simulator and AsyncFileOps Abstraction | `Proposed` | `D` | `M2` | 2026-04-23 | Owner must accept or defer before simulator evidence is used as gate proof |
| `029` | `plans/adr/029_identity_vs_locality.md` | Identity vs. Locality | `Accepted` | `F` + `I` | `M3` | 2026-04-23 | Track naming/lookup contracts against cert and Toron surfaces |
| `030` | `plans/adr/030_deployment_profiles_and_support_envelope.md` | Deployment Profiles and Support Envelope | `Proposed` | `A` + `C` + `H` + `J` | `M0` | 2026-04-23 | Owner must accept before external support or hardware claims expand |
| `031` | `plans/adr/031_production_key_custody_provider_stack.md` | Production Key Custody Provider Stack | `Proposed` | `A` + `E` + `F` + `J` | `M3` | 2026-04-23 | Owner must accept before customer pilot custody claims |
| `032` | `plans/adr/032_schema_evolution_and_control_ring_contract.md` | Schema Evolution and Control-Ring Contract | `Proposed` | `A` + `I` | `M4` | 2026-04-23 | Owner must accept before Toron schema integration claims |
| `033` | `plans/adr/033_backup_restore_and_node_replacement_architecture.md` | Backup, Restore, and Node Replacement | `Proposed` | `A` + `H` | `M3` | 2026-04-23 | Owner must accept before recovery/operator claims become release-facing |
| `034` | `plans/adr/034_toron_wendelon_integration_contract.md` | Toron-Wendelon Integration Contract | `Proposed` | `A` + `F` + `I` | `M4` | 2026-04-23 | Owner must accept before regulated Toron pilot authorization |

## Control Rules

- Proposed ADRs are not free to drift. The owning workstream listed above owns closure, deferral, or acceptance before the blocking milestone.
- Any change that contradicts an `Accepted` ADR must reopen the ADR or update the design-lock register in the same PR series.
- Any release-facing claim that depends on a `Proposed` ADR must cite the proposal status and may not present the posture as generally supported.
- `A` owns this matrix; implementation workstreams own the ADR rows mapped to them.
