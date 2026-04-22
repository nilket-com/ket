# Wendelon Consolidated Execution Plan

Date: 2026-04-22
Inputs:
- `plans/gap/claude_execution_20260422.md`
- `plans/gap/codex_execution_20260422.md`
- `plans/gap/gemini_execution_20260422.md`

## Objective

Aggregate the three execution plans into one program that closes:

- Codex's code-level hot-path, correctness, observability, and pilot-readiness gaps
- Gemini's day-2 operations, closed-loop healing, and ecosystem-integration gaps
- Claude's security, governance, audit, DR, FIPS, and regulated-procurement gaps

This consolidated plan separates three delivery bars that were implicit across the source plans:

1. **Customer-facing design-partner pilot**
2. **Regulated Fortune 500 pilot**
3. **Multi-customer regulated GA**

## Planning Horizons

The source plans use different time horizons. The merged plan keeps all three without forcing them into one unrealistic date:

- **Horizon A: Customer-facing pilot hardening**
  - target window: `10-24 weeks`
  - source emphasis: Codex + Gemini
- **Horizon B: Regulated pilot enablement**
  - target window: `6-12 months`
  - source emphasis: Claude + Codex + Gemini
- **Horizon C: Regulated GA / compliance completion**
  - target window: `12-18 months`
  - source emphasis: Claude

The key reconciliation is:

- Codex and Gemini are the right frame for short-term pilot hardening.
- Claude is the right frame for calendar-dominant compliance, governance, and regulated-GA work.

## Non-Negotiable Program Rules

1. No design claim survives a code mismatch. Code, ADRs, comments, and operator docs must agree in the same PR series.
2. Correctness and crash recovery land before throughput tuning.
3. No dependent implementation lands while load-bearing semantics remain unresolved. ADR/open-question closure or explicit design-lock notes come first.
4. Every hot-path change ships with metrics, tests, and benchmark deltas in the same change series.
5. Validation is binary. A gate is either complete or not complete.
6. No workstream claims completion without green scorecard evidence and durable gate artifacts.
7. External customer traffic does not start before the minimum gate for that customer class is complete.

## Unified Phases And Gates

| Phase | Gate Name | Window | Primary Theme | Minimum Exit |
|---|---|---:|---|---|
| 0 | `Foundation` | Weeks 1-6 | ADR closure, baselines, calendar-dominant dependencies | scorecard live, critical ADRs accepted, CMVP/KMS/IANA kickoffs started |
| 1 | `Correctness Locked` | Weeks 7-12 | Stellarator correctness, admission-path cleanup, panic inventory | no stranded quorum-durable writes in 1,000 injected failures |
| 2 | `Scale & Survive` | Weeks 13-20 | `io_uring` split, QUIC admission control, bounded overload behavior | WAL/slab decoupled, overload bounded, docs truth-matched |
| 3 | `Procurement Ready` | Months 4-6 | KMS, RBAC split, audit completeness, DR/operator path, security review | minimum bar for customer-facing pilot |
| 4 | `Pilot Authorization` | Months 6-12 | regulated pilot, runbook-only restore, Toron integration, red-team closure | minimum bar for regulated Fortune 500 pilot |
| 5 | `Regulated GA` | Months 12-18+ | CMVP closeout, SBOM/SLSA, PITR, full compliance pack | multi-customer regulated GA |

### Gate Meaning

- **Customer-facing design-partner pilot:** allowed after `Gate 3`
- **Regulated Fortune 500 pilot:** allowed after `Gate 4`
- **Regulated GA:** allowed after `Gate 5`

## Workstream Structure

The merged plan uses ten workstreams. This keeps Codex's concrete runtime work, Gemini's operator/ecosystem priorities, and Claude's procurement/compliance program intact.

### WS0: Governance, ADR Closure, And Calendar Dependencies

Scope:

- close critical ADR open questions before dependent implementation
- promote load-bearing ADRs to `Accepted`
- file IANA PEN
- start CMVP lab engagement
- start KMS vendor/legal work
- create the CI program scorecard and design-lock decision log

Initial locked decisions required by Gate 0:

- Stellarator failure semantics and fencing policy
- transport truth-up path: `implement` vs `rename/retract`
- pilot audit evidence source of truth
- supported node replacement contract
- first production root-KEK provider target

### WS1: Storage And Stellarator Correctness

Scope:

- mandatory pending-intent persistence
- true concurrent local+remote durability legs
- authoritative write-outcome state model:
  - `DataBranchPending`
  - `DurableQuorumReached`
  - `FinalizationCommitted`
  - `FinalizationRecoverablePending`
  - `FinalizationTerminalFailure`
- restart replay and idempotency hardening
- leader fencing/degraded mode on local persistence distrust

Primary code areas:

- `src/network/data_server.rs`
- `src/network/pending_finalizations.rs`
- `src/storage/manager.rs`
- `tests/`

### WS2: Transport, Admission Control, And Runtime Stability

Scope:

- replace full `SmState` clone in request admission with narrow watch/cache state
- split WAL and slab I/O execution topology
- move remaining steady-state blocking file operations off `spawn_blocking`
- add QUIC admission controls:
  - max total connections
  - max per peer/IP
  - max streams per connection
  - bounded task fan-out
  - per-tenant backpressure after auth
- reconcile transport claims with implementation
- inventory and remove recoverable live-path `expect`/poison aborts
- reduce shared-pool lock contention where benchmarks justify it

Primary code areas:

- `src/node.rs`
- `src/storage/io_uring.rs`
- `src/storage/file_ops.rs`
- `src/network/server.rs`
- `src/network/quic_transport.rs`
- `src/network/transport.rs`
- `src/network/core.rs`
- `src/network/pool.rs`
- `src/consensus/raft_storage.rs`

### WS3: Observability, Validation, And SLO Evidence

Scope:

- add missing metrics for:
  - pending finalization backlog/replay
  - replica fan-out latency
  - quorum acquisition latency
  - WAL append/sync
  - `io_uring` queue depth and failures
  - active connections and streams
  - request rejection/backpressure
  - lock contention or saturation indicators
- add trace spans for write branch split, finalization, migration-read proxying, transport rejections
- expand chaos matrix and simulator ladder
- add deterministic lazy-migration pressure scenarios
- add linearizability coverage
- promote SLOs from `TARGET` to `COMMITTED` only after soak evidence

### WS4: Crypto Custody And Key Lifecycle

Scope:

- replace `dev-insecure` with real production custody
- ship cloud KMS providers first
- add PKCS#11/HSM-compatible path for enterprise/on-prem use
- M-of-N root-KEK escrow ceremony design
- tenant KEK rotation RPC
- DEK re-wrap sweeper
- plaintext buffer zeroization in sensitive paths
- fail-fast production startup when insecure custody is configured

### WS5: Identity, RBAC, Authorization, And Governance

Scope:

- freeze enterprise OID once PEN is assigned
- design and ship cert-rotation service
- define CRL distribution story
- split `operator` into smaller roles:
  - `tenant_admin`
  - `cluster_admin`
  - `backup_admin`
  - `compliance_auditor`
- dual-authorization for destructive operations
- break-glass workflow hardening
- IdP mapping for enterprise operator onboarding

### WS6: Audit, Evidence, And Retention

Scope:

- add `data.read`, `data.write`, and related audit events
- clarify the authoritative audit archive
- promote Kafka from optional distribution-only posture to a viable pilot compliance path if chosen
- canonicalize off-box payloads with chain fields if Kafka is the pilot evidence path
- add WORM/object-lock archive support
- move retention defaults to enterprise-safe values
- ship `audit verify` tooling for replay and attestation

### WS7: Node Replacement, DR, And Operator Runbooks

Scope:

- close the Level-2 healing gap with:
  - preferred: first-party Kubernetes Operator
  - fallback: tightly specified external automation contract with timing SLA
- cluster-level consistent-cut backup
- remote-tier validation during restore
- recovery drill procedure
- cluster restore choreography
- backup encryption
- eventual PITR path
- runbooks for:
  - node replacement
  - pending-finalization backlog
  - audit sink/archive failure
  - cluster restore

### WS8: Toron And Ecosystem Integration

Scope:

- `ops/toron_integration.md`
- end-to-end compute/storage operational contract
- JDBC/ODBC/SSO/SAML mapping to Wendelon identity model
- ELT/ingest reference patterns
- trace/observability stitching between Toron and Wendelon
- quota/audit visibility from the compute tier

### WS9: FIPS, Supply Chain, And Release Compliance

Scope:

- `rustls-aws-lc-rs` transport path for FIPS builds
- Security Policy authoring
- symbol sweep plus KAT/self-test evidence
- SBOM generation
- artifact signing
- SLSA maturity target
- customer-facing compliance pack
- final CMVP certificate tracking

## Phase-By-Phase Execution Summary

## Phase 0: Foundation

Must complete:

- CI scorecard wiring
- benchmark baselines
- fault-injection harness skeleton
- ADR/open-question closure for critical-path decisions
- CMVP lab kickoff
- IANA PEN filing
- KMS vendor/legal kickoff

Exit artifact set:

- baseline benchmark JSON or equivalent CI artifact
- design-lock decision log
- ADR status updates
- scorecard dashboard

## Phase 1: Correctness Locked

Must complete:

- WS1 core Stellarator fixes
- admission-path watch/cache replacement
- live-path panic/poison inventory

Hard gate criteria:

- `1,000` injected crash/failure iterations with zero stranded quorum-durable writes
- pending-intent persistence no longer has a best-effort continuation path
- request admission no longer clones full `SmState`

## Phase 2: Scale & Survive

Must complete:

- WAL/slab `io_uring` decoupling
- QUIC admission control and bounded fan-out
- transport truth-up
- major missing metrics from WS3

Hard gate criteria:

- Raft commit p99 is materially decoupled from concurrent slab load
- overload from one peer or tenant produces bounded rejection/backpressure instead of scheduler collapse
- transport docs and code agree

## Phase 3: Procurement Ready

Must complete:

- production KMS path in code
- role split and dual-auth
- security review or red-team style audit of auth/custody path
- audit completeness and explicit evidence story
- node replacement path for the pilot
- cluster-scoped DR plan and drill

Hard gate criteria:

- `dev-insecure` cannot be loaded in default production profiles
- security review findings are closed or explicitly accepted with owner and due date
- customer-facing pilot operators have documented recovery and replacement paths

## Phase 4: Pilot Authorization

Must complete:

- Toron integration runbooks and validation
- expanded chaos matrix and simulator ladder
- runbook-only restore/rebuild validation
- migration-read QoS controls
- DR and node replacement exercised by non-authors

Hard gate criteria:

- full teardown-and-restore of a test cluster succeeds using only documented runbooks
- no tribal knowledge is required
- regulated-pilot review package is complete

## Phase 5: Regulated GA

Must complete:

- CMVP certificate or final compliance posture committed
- SBOM + signed artifacts + SLSA target
- PITR
- SLOs promoted to `COMMITTED`
- multi-customer compliance pack

## CI Program Scorecard

The scorecard is the single source of truth for phase gating.

| Track | Measurement | Gate 1 | Gate 2 | Gate 3+ |
|---|---|---:|---:|---:|
| Hot path | Raft commit p99 under slab load | decoupling visible | held | held |
| Hot path | request-admission allocator bytes/req | major drop from baseline | held | held |
| Hot path | Stellarator write p99 | concurrent fan-out verified | improved | held |
| Fault injection | crash iterations without stranded durable writes | `>= 1,000` | `>= 10,000` | soak-scale |
| Chaos | named scenarios passing | expanded beyond smoke | broad matrix | nightly matrix |
| Simulator | named ladder coverage | > current single scenario | core matrix | full matrix |
| Metrics | required metrics present | critical set | extended set | complete |
| Runbooks | paging/recovery runbooks | initial set | broad set | complete |
| Security | auth/custody review | planned | passed/accepted | held |
| Compliance | FIPS evidence | sweep + KAT direction | stronger evidence | CMVP or committed posture |

## Resourcing

The source plans imply two staffing levels:

- **Horizon A pilot hardening:** approximately `6-8` engineers plus `2-3` shared security/SRE/PM roles
- **Horizon B/C regulated pilot and GA:** approximately `18` engineering FTE plus `~7` non-engineering/security/compliance/PM/SRE FTE, plus external lab/vendor support

Minimum role coverage:

- architecture owner
- storage/runtime lead
- consensus/data-path lead
- network/transport lead
- observability/SRE lead
- security/platform lead
- compliance/program lead
- Toron/integration lead

## Top Execution Risks

| Risk | Why It Matters | Mitigation |
|---|---|---|
| CMVP lab delay | slips regulated GA regardless of engineering progress | start in Phase 0; run in parallel |
| upstream `rustls-aws-lc-rs` or FIPS-path instability | blocks transport compliance path | contribution budget; explicit fallback posture |
| hot-path refactors regress correctness | can erase pilot gains | feature flags where possible, chaos soak before default |
| customer InfoSec review finds structural issue late | rework during pilot window | pre-review with friendly customer security team |
| KMS vendor/legal delay | blocks production custody kickoff | parallel negotiations from Week 1 |
| dual-auth creates incident deadlock | ops friction in emergencies | break-glass path remains explicit and auditable |
| Toron roadmap drift | ecosystem milestone slips | joint ownership and weekly cross-project review |
| runbook-only restore fails | reveals tribal knowledge dependency | dogfood runbooks in drills before Gate 4 |

## Acceptance Checklist

The consolidated plan is complete when all statements below are true:

- no full `SmState` clone remains on request admission
- no best-effort pending-finalization branch remains
- WAL and slab durability are no longer coupled through one `io_uring` worker
- transport overload is bounded by explicit admission control
- transport naming/docs match reality
- live-path panic inventory is resolved or explicitly justified
- production root-KEK provider exists
- security review has been completed for the pilot boundary
- audit evidence path is explicit and operationally supportable
- node replacement path is defined and exercised
- DR procedure is cluster-scoped rather than node-image-only
- full teardown-and-restore passes using only documented runbooks
- chaos and simulator coverage materially exceed smoke-level
- Toron integration boundary is documented and tested before regulated pilot
- supply-chain and compliance artifacts exist for the intended release class

## Go / No-Go

- **No-go for any external pilot** before `Gate 3`
- **No-go for regulated Fortune 500 pilot** before `Gate 4`
- **No-go for multi-customer regulated GA** before `Gate 5`

Short version:

- Codex and Gemini define the shortest honest path to a customer-facing pilot.
- Claude defines the longer path to a procurement-safe regulated deployment.
- The merged program uses Codex for runtime truth, Gemini for operator reality, and Claude for compliance and enterprise closure.
