# Brainstorm 021: The Boron Semiring — A Five-Generator Basis for Coordination and LTAP

## Context

Distributed transaction and hybrid transactional/analytical (LTAP) engines
routinely degenerate into an $O(N^2)$ matrix of pairwise special cases: lock
tables interacting with DDL fences, vacuum/tiering racing in-flight readers,
backup holds blocking schema changes, and every new SQL verb demanding its own
state-machine protocol.

Across the `ParentDecisions` cutover (ADRs 0100, 0112–0127), Wendelon and Toron
have already built roughly three-quarters of a general LTAP substrate on the
Unified Ring. Stepping back from individual SQL verbs shows that every
distributed coordination, ingestion, query, readiness, and background
maintenance operation factors through **five atomic generator classes** over
hierarchical ring cells `(tenant, table, key_arc)`.

We call this five-generator structure the **Boron semiring** ($Z = 5$,
$1s^2\,2s^2\,2p^1$):

- It is a **trace-enriched idempotent semiring with tests**, not a group
  (projections, `AppendOnly` logs, and `max`-watermarks have no inverses) and
  not a field (sequential composition is non-commutative and guards are zero
  divisors).
- Its five generators split into **$2 + 2 + 1$**: a monoid-pullback pair of
  abelian subalgebras (`read`, `merge`), their non-abelian universal closure
  (`update`), and two boundary couplings (`await`, `send`).
- Five is **minimal** up to footprint-preserving, progress-preserving, and
  consensus-cost-preserving observational equivalence.

This note records the carrier semiring, the pullback and minimality laws, the
commutation matrix, and how future Wendelon × Toron work harvests them without
bespoke per-verb protocols.

---

## 1. Carrier Semiring: $\mathcal{B} \subseteq \mathcal{P}(\Omega \times \mathcal{R} \times \Omega)$

Let the logical state and outbox configuration space be

$$\Omega = \left(\prod_{c \in \mathcal{C}} X_c\right) \times \mathcal{M}_{\text{outbox}}$$

where $\mathcal{C}$ is the poset of hierarchical ring cells
`(tenant, table, key_arc)` ordered by containment (a table cell
`(tenant, T, [-\infty, +\infty))` dominates its key sub-arcs and point keys),
and $\mathcal{M}_{\text{outbox}} \cong \mathbb{N}^{\text{Msg}}$ is the free
commutative monoid indexed by unique durable intent identities $m \in \text{Msg}$.

### Why Plain Relations and Ordered Word Traces Both Fail

- Plain state relations $\mathcal{P}(\Omega \times \Omega)$ drop caller outputs,
  collapsing every total `read` $D_g$ onto the identity $\mathbf{1}$.
- Appending caller outputs to an *ordered* word $u \cdot v \in \mathcal{O}^*$
  over-orders concurrent observations: $[g(\omega), h(\omega)] \neq
  [h(\omega), g(\omega)]$ whenever $g(\omega) \neq h(\omega)$, which would
  falsely break self-commutation between two concurrent reads.

Instead, tag each value-returning invocation by its unique call handle
$\iota \in \text{CallId}$ and take the response monoid to be finite partial maps
$\mathcal{R} = \text{FinMap}(\text{CallId}, \text{Val})$ under disjoint-domain
union $\uplus$. Operations live in $\mathcal{P}(\Omega \times \mathcal{R} \times \Omega)$
with **left-to-right** sequential composition ($A \cdot B$ means "$A$ then $B$"):

- **Choice / union ($\oplus$):** set union ($A \oplus A = A$).
- **Sequential composition ($\cdot$):**
  $$A \cdot B = \{(\omega_0, r_1 \uplus r_2, \omega_2) \mid \exists \omega_1:\;
  (\omega_0, r_1, \omega_1) \in A \;\land\; (\omega_1, r_2, \omega_2) \in B,\;
  \text{dom}(r_1) \cap \text{dom}(r_2) = \emptyset\}.$$
  Associative, distributing over $\oplus$, and **non-commutative** in general.
- **Blocked / empty ($0 = \emptyset$):** annihilator ($A \cdot 0 = 0 \cdot A = 0$, $A \oplus 0 = A$).
- **Identity ($\mathbf{1} = \{(\omega, \emptyset, \omega) \mid \omega \in \Omega\}$):**
  state-preserving, silent no-op.
- **Boolean test subalgebra ($\mathcal{T} \subseteq [0, \mathbf{1}]$):**
  silent predicates $P = \{(\omega, \emptyset, \omega) \mid \text{pred}(\omega)\}$
  with complement $\bar{P}$, satisfying $P \cdot P = P$, $P \oplus \bar{P} = \mathbf{1}$,
  $P \cdot \bar{P} = 0$, and $P \cdot Q = Q \cdot P$.

Working in this semiring requires no subtraction or artificial "negative
executions":

- A guarded transition (`if P then C_f else skip`) is the disjoint sum
  $P \cdot C_f \oplus \bar{P}$.
- Commutation is the exact semiring equality **$A \cdot B = B \cdot A$**
  (identical ending state and call-indexed response map under both orders).

---

## 2. Derivation and Minimality of the Five Generators ($1s^2\,2s^2\,2p^1$)

Every generator acts nontrivially only on a finite support (footprint) of
factors, $O = O_F \otimes \mathbf{1}_{\Omega \setminus F}$. Operators with
disjoint supports commute unconditionally ($F_A \cap F_B = \emptyset \implies
A \cdot B = B \cdot A$).

### A. Total Cell Operations: $2s^2 + 2p^1$ (Size 3)

On a single cell $X_c$, total (non-blocking) operations split into two
conjugate abelian families and their closure:

1. **`read(R, K)` — Diagonal observation subalgebra $\mathcal{D}$ (Consensus #1):**
   $$D_g^{(\iota)} = \{(\omega, \{\iota \mapsto g(\omega_R)\}, \omega) \mid \omega \in \Omega\}.$$
   State-preserving and total, yet distinct from $\mathbf{1}$ because it records
   $\iota \mapsto g(\omega_R)$. For distinct calls $\iota_1 \neq \iota_2$,
   disjoint-map union commutes, so
   $D_g^{(\iota_1)} \cdot D_h^{(\iota_2)} = D_h^{(\iota_2)} \cdot D_g^{(\iota_1)}$.
   Evaluated at a **closed snapshot cut $K$**, `read(R, K)` is a constant
   function of committed history $\le K$, commuting with every transition
   ordered after $K$.
2. **`merge(M, id, δ)` — Abelian shift subalgebra $\mathcal{C}$ (Consensus #1):**
   $$C_\delta = \{(\omega, \emptyset, t_\delta(\omega)) \mid \omega \in \Omega\}, \qquad t_\delta(x) = x \oplus_M \delta$$
   in a commutative monoid $(X_M, \oplus_M)$ (deduplicated by `id`), emitting
   the empty response map $\emptyset$ (unit return `()`).
   - *Pullback duality (no group inverses required):* Unlike the group-theoretic
     Weyl phase relation, which needs Pontryagin characters and inverses that
     `AppendOnly` logs and `max`-watermarks lack, `read` and `merge` satisfy the
     universal **pullback intertwining identity** on any monoid:
     $$C_\delta \cdot D_g^{(\iota)} = D_{g \circ t_\delta}^{(\iota)} \cdot C_\delta.$$
     Shifting by $\delta$ then reading $g$ equals reading the pulled-back
     observable $g \circ t_\delta$ then shifting. In one line:
     $$C_\delta \cdot D_g^{(\iota)} = D_g^{(\iota)} \cdot C_\delta \iff g \circ t_\delta = g.$$
   - *Why `merge` must emit $\emptyset$ (`()`):* Any step that both shifts $X_c$
     and records pre-state in $\mathcal{R}$ (such as `fetch_add`) produces
     $\{\iota_1 \mapsto x, \iota_2 \mapsto x + 1\} \neq \{\iota_2 \mapsto x,
     \iota_1 \mapsto x + 1\}$, breaking self-commutation and raising Herlihy's
     consensus number to $\ge 2$.
   - *Why raw blind `set(v)` is excluded:* An unversioned overwrite register has
     consensus number $1$, yet $\text{set}(u) \cdot \text{set}(v) \neq
     \text{set}(v) \cdot \text{set}(u)$ for $u \neq v$. Lifting overwrites into
     timestamped LWW `merge((hlc, node, v))` under $\max$ restores
     $C_\delta \cdot C_{\delta'} = C_{\delta'} \cdot C_\delta$.
3. **`update(R, W, id, f)` — Full endomorphism closure ($P_R \cdot C_{f,W}^{(\iota)} \oplus \bar{P}_R$, Consensus $\infty$):**
   By finite-dimensional operator completeness and Herlihy's universality
   theorem (1988), combining a diagonal guard/observation on $R$ with a state
   transition and response on $W$ spans the entire non-commutative
   transformation semiring. One universal CAS-equivalent primitive handles all
   non-commuting coordination.

### B. Boundary Couplings: $1s^2$ (Size $3 \to 5$)

4. **`await(R, pred)` — Partial test projection ($P_{\text{pred}} \in \mathcal{T}$, Consensus #1):**
   Shares the diagonal commutation profile of `read`, separated by
   **partiality** (and silent response $\emptyset$): states in
   $\ker P_{\text{pred}}$ map to $0$, suspending the caller until another
   transition moves $X_R$ into $\text{im} P_{\text{pred}}$.
5. **`send(m)` — Outbox token emission ($\mathbf{1}_{\text{cells}} \otimes a^\dagger_m$, Consensus #1):**
   Increments coordinate $m$ in $\mathcal{M}_{\text{outbox}} \cong \mathbb{N}^{\text{Msg}}$
   while emitting $\emptyset$ in $\mathcal{R}$. Separating $\mathcal{M}_{\text{outbox}}$
   from ordinary cells $c \in \mathcal{C}$ achieves **two** non-interference
   properties simultaneously:
   - *Producer–producer commutation:* $(U_1 \otimes a^\dagger_{m_1}) \cdot
     (U_2 \otimes a^\dagger_{m_2}) = (U_1 \cdot U_2) \otimes a^\dagger_{\{m_1, m_2\}}$,
     so disjoint `update`s emitting intents never conflict on the outbox.
   - *Producer–consumer commutation:* For distinct message tokens $m_1 \neq m_2$,
     emitting $m_2$ acts on coordinate $m_2$ while background consumption
     $V \otimes a_{m_1}$ acts on coordinate $m_1$ plus $V$'s cells:
     $$(\mathbf{1} \otimes a^\dagger_{m_2}) \cdot (V \otimes a_{m_1}) = (V \otimes a_{m_1}) \cdot (\mathbf{1} \otimes a^\dagger_{m_2}) \qquad (m_1 \neq m_2).$$
     If `send` were simulated by `merge` onto a shared queue cell
     $c_{\text{outbox}} \in \mathcal{C}$, a sweeper acknowledging $m_1$ via
     `update` on $c_{\text{outbox}}$ would conflict with every concurrent
     foreground writer emitting $m_2$. In Wendelon, durable intents must
     therefore be keyed as independent per-message tokens (such as
     `(parent_key, ordinal)`), never as a monolithic per-slab or per-table queue
     cell.

### C. Five-Generator Minimality

In raw relational semantics without cost or footprint constraints, `update`
alone could simulate `read` and `merge`, `await` would be a partial guard, and
`send` would be a cell mutation. All five generators become **strictly
irreducible** under **footprint-preserving, progress-preserving, and
consensus-cost-preserving observational equivalence ($\equiv$)**:

| Collapse Blocked | Invariant Preserved by $\equiv$ | Why Dropping the Generator Fails |
|---|---|---|
| `{read, merge, await, send}` $\to$ `update` | Consensus / coordination cost ($1$ vs. $\infty$) | Simulating consensus-$1$ ops via `update` destroys self-commutation and serializes scalable paths through Raft. |
| `read` $\leftrightarrow$ `merge` | State purity ($\tau = \text{id}$) vs. silent shift ($\mathcal{R} = \emptyset$) | Coupling observation and shift breaks $C_{\delta_1} \cdot C_{\delta_2} = C_{\delta_2} \cdot C_{\delta_1}$ on $\mathcal{R}$ and jumps to consensus $\ge 2$. |
| `read` $\leftrightarrow$ `await` | Totality (wait-free return vs. partial $0$ on $\ker P$) | Total ops cannot suspend on $\ker P$ without busy-spinning; `await` returns $\emptyset$ and blocks until `pred` holds. |
| `merge` $\leftrightarrow$ `send` | Producer–consumer footprint non-interference | Merging into a cell $c \in \mathcal{C}$ makes consumer ack/pop (`update` on $c$) collide with concurrent producers emitting $m_2 \neq m_1$. |

---

## 3. Generator and Commutation Tables

### Table 1: The Five Boron Generators

| Subshell | Generator | Semiring Form ($A \cdot B$ = "$A$ then $B$") | Cell Footprint | Consensus # | Current & Target Nilket Realization |
|---|---|---|---|---|---|
| **$2s^2$** *(Pullback pair)* | **`read(R, K)`** | $D_g^{(\iota)} \otimes \mathbf{1}$ *(total diagonal)* | Guard $R$ *(empty vs. $>K$ once pinned to closed cut $K$)* | **1** | Snapshot scan/lookup over `certified_base@B` + `Accepted` in $(B, K]$; COCD pushdown; lockless export. |
| **$2s^2$** *(Pullback pair)* | **`merge(M, id, δ)`** | $C_\delta \otimes \mathbf{1}$ *(monoid shift, $\mathcal{R}=\emptyset$)* | Merge $M_\oplus$ *(plus guard `table.live`)* | **1** | `AppendOnly` row batches (ADR 0119, commuting under `(op, pos)`), monotone `max` cut watermarks. |
| **$2p^1$** *(Closure)* | **`update(R, W, id, f)`** | $(P_R \cdot C_{f,W}^{(\iota)} \oplus \bar{P}_R) \otimes \mathbf{1}$ | Guard $R \setminus W$, Write $W$ | **$\infty$** | `ParentDecisions` `Reserve` $\to$ `Finalize`, PK writes, `DELETE` (0112), `DROP` (0118), `CTAS` (0120), transactions. |
| **$1s^2$** *(Boundary)* | **`await(R, pred)`** | $P_{\text{pred}} \otimes \mathbf{1}$ *(partial test, $\mathcal{R}=\emptyset$)* | Guard $R$ | **1** | Local cut-activation wait (`DC-281`), lease-gate wait (`ADR 0713`), reader-drain barriers. |
| **$1s^2$** *(Boundary)* | **`send(m)`** | $\mathbf{1} \otimes a^\dagger_m$ *(per-token outbox, $\mathcal{R}=\emptyset$)* | Token $m \in \mathcal{M}_{\text{outbox}}$ *(zero cell footprint)* | **1** | Durable cleanup/tier/compaction intents, retention debt, CDC emission. |

### Table 2: Same-Cell Commutation ($A \cdot B \stackrel{?}{=} B \cdot A$ on Shared Cell $c$)

Disjoint cells always commute (`=`). When two operations with distinct call
handles ($\iota_1 \neq \iota_2$) touch a shared cell or overlapping
`(tenant, table, key_arc)`, split `update(R, W, ...)` into its **guard factors**
($c \in R \setminus W$, acting diagonally) and its **write factors** ($c \in W$):

| Shared cell $c$ | `read` ($D_g^{(\iota_2)}$) | `await` ($P$) | `merge` ($C_\delta$) | `update` guard ($c \in R \setminus W$) | `update` write ($c \in W$) | `send` ($a^\dagger_m$) |
|---|---|---|---|---|---|---|
| **`read` ($D_g^{(\iota_1)}$)** | **`=`** *(pinned $@K$: `=` w/ all $>K$)* | **`=`** | `≠` *(unless $g \circ t_\delta = g$)* | **`=`** | `≠` *(unless $g \circ f = g$)* | **`=`** |
| **`await` ($P$)** | **`=`** | **`=`** | `≠` *(wakes/blocks if $\delta$ flips $P$)* | **`=`** | `≠` *(if $f$ flips $P$)* | **`=`** |
| **`merge` ($C_\delta$)** | `≠` | `≠` | **`=`** *(same abelian $\oplus$, distinct `id`)* | `≠` *(invalidates scanned range)* | `≠` | **`=`** |
| **`update` guard ($R \setminus W$)** | **`=`** | **`=`** | `≠` | **`=`** *(shared guards commute)* | `≠` | **`=`** |
| **`update` write ($W$)** | `≠` | `≠` | `≠` | `≠` | `≠` *(requires consensus order)* | **`=`** |
| **`send` ($a^\dagger_m$)** | **`=`** | **`=`** | **`=`** | **`=`** | **`=`** | **`=`** *(and `=` w/ $a_{m'} \otimes V$ for $m \neq m'$)* |

---

## 4. Core Laws and Architectural Reductions

### Law 1: Hierarchical Guard-vs-Write Commutation (`DROP TABLE` vs. Row Writes)
Because `[update guard, update guard] =` on shared factors, thousands of
concurrent row writes `update(R = {table}, W = {row_k})` and `AppendOnly`
merges all reading `table.state == live` **commute with one another** on the
`table` cell. `DROP TABLE` executes `update(R = {names}, W = {names, table})`
(checking `names` still binds to `table_id`), putting `table` in $W$ and
therefore conflicting cleanly with every concurrent write to that table without
visiting a single row (ADR 0114, ADR 0118).

### Law 2: Pullback Invalidation of Scanned Guards by `merge`
By $C_\delta \cdot D_g^{(\iota)} = D_{g \circ t_\delta}^{(\iota)} \cdot C_\delta$
(row 3, column 4 of Table 2), `merge` commutes with `merge` (`=`), **not** with
an `update` guard (`≠`) unless the guard predicate is invariant under $t_\delta$.
If a certifying `update` relies on a range or predicate scan over `arc`, an
intervening `AppendOnly` `merge` or row insert into `arc` alters that predicate
and must fail certification. Also, logical commutation of distinct `AppendOnly`
contributions under `(op, position)` requires retries of the same `id` to carry
identical content and does not by itself make the current replication transport
consensus-free.

### Law 3: Spacetime-Arc Certification (General Transactions Without Lock Tables)
A transaction that reads `R_arcs` at closed cut $K$ (resolved from certified
base $B \le K$ plus `Accepted` transitions in $(B, K]$), stages immutable child
payloads on Data Ring replicas, and proposes commit at log index $L > K$ with
write arcs $W_{\text{arcs}}$ is serializable at $L$ iff it commutes past every
`Accepted` transition $T_i$ in the half-open log interval $(K, L)$:

$$\forall i \in (K, L):\quad (W_{T_i} \cup M_{T_i}) \cap R_{\text{arcs}} = \emptyset \;\land\; W_{T_i} \cap W_{\text{arcs}} = \emptyset.$$

That single spacetime-volume emptiness check over
$\text{arcs} \times (K, L)$ subsumes point upserts, `DELETE ... WHERE`,
`UPDATE ... WHERE`, `RecordNoop` ($W_{\text{arcs}} = \emptyset$ with durable
retry outcome), `CTAS` ($W = \{\text{names}, \text{new\_table}\}$ with atomic
manifest publication), and future multi-statement transactions — with zero
distributed lock holding across client round-trips and zero deadlocks.

### Law 4: Heap Quotienting ("Physical Presence Is Never Authority")
Let $\sigma_K$ be the logical state determined by `(certified_base@B)` plus
`Accepted` decisions in $(B, K]$, and let $H$ be the physical slab/L2 object
heap. Two heaps $H_1, H_2$ are **equivalent at $K$** ($H_1 \equiv_K H_2$) iff
every root in $\sigma_K$ resolves through verified digests to identical payload
bytes in $H_1$ and $H_2$:

1. **Unreferenced bytes carry zero authority:** Orphaned staged appends, refused
   intents, or reopened physical entries lacking an `Accepted` decision or
   `certified_base` root are invisible to reads, logical exports, and repairs.
2. **Positive reachability, never negative proofs of absence:** Garbage
   collection verifies positively that enumerated live roots resolve to valid
   copies, then reclaims unreferenced storage asynchronously with no correctness
   deadline (the ADR 0118 cutover lesson).
3. **Outbox maintenance preserves $\equiv_K$ without fencing live readers:**
   Tiering, compaction, and retention (`send` consumers) publish new immutable
   representations and advance directory pointers atomically while keeping prior
   physical locations readable until in-flight readers and retained cuts drain.
   Foreground reads and holders should not be forced into physical-location
   fences when logical state has not changed.

### Law 5: Semiring Fusion
- **Merge batching:** $C_{\delta_1} \cdot C_{\delta_2} = C_{\delta_1 \oplus_M \delta_2}$
  coalesces commuting `AppendOnly` batches and `max`-cut watermarks.
- **Cut sharing (lockless multi-table backup):**
  $D_g^{(\iota_1, K)} \cdot D_h^{(\iota_2, K)}$ evaluates against one pinned cut
  $K$. Sharing **one** common retained cut $K$ across catalog/settings,
  secondary indexes, and all table scans turns logical backup into a pure
  snapshot read, retiring `BeginBackupHold` / `ReleaseBackupHold` while
  upgrading exports from per-table cuts $K_i \ge R$ to a single cross-table
  snapshot.

---

## 5. Consensus Number vs. Crash Survival (Why Toron Has No Raft Group)

A recurring trap when mapping algebra to systems is conflating **Herlihy
consensus number ($\infty$)** with **network consensus (Raft)**. They live on
orthogonal axes:

- **Algebraic axis (Commutation):** Does $A \cdot B = B \cdot A$ hold (consensus
  #1), or can order change state and caller observations (consensus $\infty$,
  requiring linearizable compare-and-swap)?
- **Fault-model axis (Crash survival):** Is state **ephemeral** (scoped to one
  owning process's lifetime, where process death legally discards the state), or
  **durable** (required to survive machine crashes, restarts, and disk loss)?

Inside a single process's shared memory, hardware instructions
(`AtomicU64::compare_exchange` or a local `Mutex`) **already provide consensus
number $\infty$** in nanoseconds. Distributed Raft exists for one reason only:
a single CPU's `CAS` register vanishes when that host crashes. Raft is how a
cluster constructs **a crash-surviving `CAS` register** across $2f + 1$ fault
domains.

Crossing the two axes across the Weak Ring (`Toron`, ephemeral compute) and the
Strong Ring (`Wendelon`, durable storage) shows why **only one of the four
quadrants uses Raft**:

| | **Commuting ($A \cdot B = B \cdot A$, Consensus #1)** | **Non-Commuting ($A \cdot B \neq B \cdot A$, Consensus $\infty$)** |
|---|---|---|
| **Ephemeral (`Weak` / `Toron`)** *(dies cleanly with its owning process)* | Lock-free atomics, bitsets, partial aggregates, descriptor/stats caches (`read_weak`, `merge_weak`, `await_weak`, `send_weak`) — **No Raft** | **Single-owner CPU `CAS` / `Mutex` (`update_weak`)** (session tx buffers, per-node slot semaphores) — **No Raft** |
| **Durable (`Strong` / `Wendelon`)** *(survives host crash & power loss)* | Replicated / L2 immutable chunk writes and `max` watermarks (`stage_payloads`, `merge_strong`) — **No Raft per chunk** | **Raft (`update_strong`)** — **the sole quadrant in Nilket that runs Raft** |

This separation yields the **upper-triangular cross-ring product**
$\mathcal{B}_{\text{weak}} \ltimes_K \mathcal{B}_{\text{strong}}$:

1. **Strong independence:** $\Omega_{\text{strong}}$ is closed — no Strong guard
   ever reads $\Omega_{\text{weak}}$, and Wendelon never `await`s Toron. If an
   entire Toron fleet reboots, Wendelon loses zero invariants and holds zero
   orphaned locks.
2. **No "RAM-only Raft" on Toron and no cross-ring 2PC:** If state must survive
   a coordinator crash (table schemas, tenant quota definitions, committed
   outcomes), it belongs on $\Omega_{\text{strong}}$. If state is allowed to die
   with its coordinator (an active query DAG or an uncommitted `BEGIN ...
   COMMIT` session buffer), one Toron coordinator owns it in RAM via
   `update_weak` (local CPU `CAS`), streams unreferenced payloads via
   `stage_payloads`, and crosses into Wendelon with **one** atomic
   `update_strong` at `COMMIT` (resolving any mid-commit disconnect via
   `resolve_strong(tx_id)`).

---

## 6. Convergence Harvest Path (No Big-Bang Rewrite)

1. **Review razor (immediate):** New protocols or `WendelonRequest` /
   `DecisionCommand` variants must state their Boron generator, `(R, W)` cell
   footprint, identity, and cut $K$, and show why existing generators cannot
   express the invariant.
2. **Subtract `Today` dual-mode baggage (post-cutover):** Once `ParentDecisions`
   is the proven default across fresh formation, SQL, backup, restart, and
   replacement, retire legacy `Today` serving routes (`Finalize(TenantFinalization)`,
   single `AcquireLease`, bare `SlabOp`/`CancelOp`/`AckOutcomes`, legacy drop
   stages) followed by a clean pre-release enum-tag compaction cut.
3. **Opportunistic consolidation (as contracts are touched):**
   - Upgrade `logical_backup` to a single retained cut $K$ across catalog,
     indexes, and tables, then drop `BeginBackupHold` / `ReleaseBackupHold`.
   - Converge future conflicting SQL mutations (`UPDATE ... WHERE`, atomic
     table/rollup swaps, multi-statement LTAP transactions) onto uniform
     `(read_arcs, write_arcs)` certification over $(B \le K, L]$ inside
     `Reserve` / `Finalize`, preserving retry-outcome and atomic-publication
     contracts where required.

[Wendelon × Toron identity and ownership algebra](../../../toron/plans/conventions/identity-ownership-algebra.md) ·
[Wendelon philosophy](../philosophy.md) ·
[ADR 0100: Parent finalization and read visibility](../adr/0100_parent_finalization_and_read_visibility.md) ·
[ADR 0118: ParentDecisions SQL DROP](../adr/0118_a_parentdecisions_sql_drop_succeeds_at_its_logical_finalize.md)
