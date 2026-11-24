# Brainstorm 021: The Boron Semiring — A Five-Generator Basis for Coordination and LTAP

## Context

Distributed transaction and hybrid transactional/analytical (LTAP) engines
routinely degenerate into an $O(N^2)$ matrix of pairwise special cases: lock
tables interacting with DDL fences, vacuum/tiering racing in-flight readers,
backup holds blocking schema changes, and every new SQL verb demanding its own
state-machine protocol.

Across the `ParentDecisions` cutover (ADRs 0100, 0112–0125), Wendelon and Toron
have already built roughly three-quarters of a general LTAP substrate on the
Unified Ring. Stepping back from individual SQL verbs shows that every
distributed coordination, ingestion, query, readiness, and background
maintenance operation factors through **five atomic generator classes** over
hierarchical ring cells `(tenant, table, key_arc)`.

We call this five-generator structure the **Boron semiring** ($Z = 5$,
$1s^2\,2s^2\,2p^1$):

- It is an **idempotent semiring with tests**, not a group (projections and
  `max`-watermarks have no inverses) and not a field (sequential composition is
  non-commutative and guards are zero divisors).
- Its five generators split by first-principles operator structure into
  **$2 + 2 + 1$**: two conjugate abelian subalgebras (`read`, `merge`), one
  non-abelian universal closure (`update`), and two boundary couplings
  (`await`, `send`).

This note records the algebraic model, its commutation matrix, its reduction
laws, and how future Wendelon × Toron work harvests it without bespoke
per-verb protocols.

---

## 1. Semiring Structure: $\mathcal{B} = (S, \oplus, \cdot, 0, \mathbf{1})$

Let the logical state and outbox configuration space be

$$\Omega = \left(\prod_{c \in \mathcal{C}} X_c\right) \times \mathcal{M}_{\text{outbox}}$$

where $\mathcal{C}$ is the poset of hierarchical ring cells
`(tenant, table, key_arc)` ordered by containment (a table cell
`(tenant, T, [-\infty, +\infty))` dominates its key sub-arcs and point keys),
and $\mathcal{M}_{\text{outbox}}$ is the free commutative monoid (multiset) of
durable background intents.

Operations live in an **idempotent semiring with tests** (Kozen's *Kleene
Algebra with Tests*, 1997):

- **Choice / union ($\oplus$):** associative, commutative, and idempotent
  ($A \oplus A = A$).
- **Sequential composition ($\cdot$):** associative
  ($A \cdot (B \cdot C) = (A \cdot B) \cdot C$), distributing over $\oplus$,
  and **non-commutative** in general ($A \cdot B \neq B \cdot A$).
- **Blocked / empty ($0$):** annihilator for composition
  ($A \cdot 0 = 0 \cdot A = 0$, $A \oplus 0 = A$).
- **Identity ($\mathbf{1}$):** state-preserving no-op ($A \cdot \mathbf{1} = \mathbf{1} \cdot A = A$).
- **Boolean test subalgebra ($\mathcal{T} \subseteq [0, \mathbf{1}]$):**
  diagonal predicates $P \in \mathcal{T}$ with complement $\bar{P} \in \mathcal{T}$
  satisfying $P \cdot P = P$, $P \oplus \bar{P} = \mathbf{1}$, $P \cdot \bar{P} = 0$,
  and $P \cdot Q = Q \cdot P$.

Working in a semiring avoids inventing artificial "negative executions" just to
define a Lie bracket:

- A guarded transition (`if P then C_f else skip`) is written directly as the
  disjoint sum $P \cdot C_f \oplus \bar{P}$.
- Commutation is the plain equation **$A \cdot B = B \cdot A$** (identical
  resulting state and caller return values under both orders).

---

## 2. Derivation of the Five Generators ($1s^2\,2s^2\,2p^1$)

Every generator acts nontrivially only on a finite support (footprint) of
factors, $O = O_F \otimes \mathbf{1}_{\Omega \setminus F}$. Operators with
disjoint supports commute unconditionally ($F_A \cap F_B = \emptyset \implies
A \cdot B = B \cdot A$). To classify the primitives, examine how operators act
on a **single shared cell** $X_c$ and how they couple to blocking and async
work.

### A. Total Cell Operations: $2s^2 + 2p^1$ (Size 3)

On a single cell $X_c$, total (non-blocking) operations have a state transition
$\tau: X_c \to X_c$ and a caller return map $\omega: X_c \to Y$:

1. **`read(R, K)` — Diagonal subalgebra $\mathcal{D}$ (Consensus #1):**
   State-preserving ($\tau = \text{id}$), returning $\omega = g(x_R)$. Every
   diagonal operator commutes with every other diagonal operator
   ($D_g \cdot D_h = D_h \cdot D_g$). Evaluated at a **closed snapshot cut
   $K$**, `read(R, K)` is a constant function of committed history $\le K$, so
   it commutes with every transition ordered after $K$.
2. **`merge(M, id, δ)` — Abelian shift subalgebra $\mathcal{C}$ (Consensus #1):**
   Applies an associative, commutative transition $x \mapsto x \oplus_M \delta$
   (deduplicated by `id`) and returns **`()`**.
   - *Why `merge` must return `()` (discrete Weyl relation):* Diagonal
     observation $\mathcal{D}$ and shift $\mathcal{C}$ are conjugate
     ($D_g \cdot C_\delta \neq C_\delta \cdot D_g$ whenever
     $g(x \oplus_M \delta) \neq g(x)$). Returning pre-state information couples
     $\mathcal{D}$ and $\mathcal{C}$ in one step (e.g. `fetch_add`), breaking
     self-commutation on outputs and raising Herlihy's consensus number to $\ge 2$.
   - *Why raw blind `set(v)` is excluded:* An unversioned overwrite register has
     consensus number $1$ (overwriting erases prior state), yet fails
     commutation ($\text{set}(u) \cdot \text{set}(v) \neq \text{set}(v) \cdot
     \text{set}(u)$ for $u \neq v$). Lifting overwrites into timestamped LWW
     `merge((hlc, node, v))` under $\max$ restores $C_\delta \cdot C_{\delta'} =
     C_{\delta'} \cdot C_\delta$.
3. **`update(R, W, id, f)` — Full endomorphism closure ($P_R \cdot C_{f,W} \oplus \bar{P}_R$, Consensus $\infty$):**
   By Schwinger completeness (finite-dimensional Stone–von Neumann) and
   Herlihy's universality theorem (1988), combining a diagonal guard/observation
   on $R$ with a state transition on $W$ generates the entire non-commutative
   transformation semiring. One universal CAS-equivalent primitive spans all
   non-commuting coordination.

### B. Boundary Couplings: $1s^2$ (Size $3 \to 5$)

Pure single-cell commutation stops at `{read, merge, update}`. Two operational
distinctions split off the remaining pair:

4. **`await(R, pred)` — Partial test projection ($P_{\text{pred}} \in \mathcal{T}$, Consensus #1):**
   Has the exact same commutation profile as `read`, separated by
   **partiality**: states in $\ker P_{\text{pred}}$ evaluate to $0$ (blocking
   the caller) until another transition moves $X_R$ into $\text{im} P_{\text{pred}}$.
5. **`send(m)` — Outbox monoid emission ($\mathbf{1}_{\text{cells}} \otimes a^\dagger_m$, Consensus #1):**
   Appends durable intent $m$ to $\mathcal{M}_{\text{outbox}}$
   ($S \mapsto S \uplus \{m\}$). Algebraically a `merge` on the outbox factor,
   separated so `update` can emit async work atomically
   ($U_{\text{cells}} \otimes a^\dagger_m$) without causing two cell-disjoint
   `update`s to conflict on the shared outbox:
   $$(U_1 \otimes a^\dagger_{m_1}) \cdot (U_2 \otimes a^\dagger_{m_2}) = (U_1 \cdot U_2) \otimes a^\dagger_{\{m_1, m_2\}}.$$
   Subsequent consumption by a background worker ($a_m \otimes V$) inherits the
   footprint and consensus number of $V$.

---

## 3. Generator and Commutation Tables

### Table 1: The Five Boron Generators

| Subshell | Generator | Semiring / Tensor Form | Cell Footprint | Consensus # | Current & Target Nilket Realization |
|---|---|---|---|---|---|
| **$2s^2$** *(Weyl pair)* | **`read(R, K)`** | $D_g \otimes \mathbf{1}$ *(total diagonal)* | Guard $R$ *(empty vs. $>K$ once pinned to closed cut $K$)* | **1** | Snapshot scan/lookup over `certified_base@B` + `Accepted` in $(B, K]$; COCD pushdown; lockless export. |
| **$2s^2$** *(Weyl pair)* | **`merge(M, id, δ)`** | $C_\delta \otimes \mathbf{1}$ *(abelian shift, unit return)* | Merge $M_\oplus$ *(plus guard `table.live`)* | **1** | `AppendOnly` row batches (ADR 0119, commuting under `(op, pos)`), monotone `max` cut watermarks. |
| **$2p^1$** *(Closure)* | **`update(R, W, id, f)`** | $(P_R \cdot C_{f,W} \oplus \bar{P}_R) \otimes \mathbf{1}$ | Guard $R \setminus W$, Write $W$ | **$\infty$** | `ParentDecisions` `Reserve` $\to$ `Finalize`, PK writes, `DELETE` (0112), `DROP` (0118), `CTAS` (0120), transactions. |
| **$1s^2$** *(Boundary)* | **`await(R, pred)`** | $P_{\text{pred}} \otimes \mathbf{1}$ *(partial test $P^2=P$)* | Guard $R$ | **1** | Local cut-activation wait (`DC-281`), lease-gate wait (`ADR 0713`), reader-drain barriers. |
| **$1s^2$** *(Boundary)* | **`send(m)`** | $\mathbf{1} \otimes a^\dagger_m$ *(outbox union)* | Outbox $\mathcal{M}_{\text{outbox}}$ *(zero cell footprint)* | **1** | Durable cleanup/tier/compaction intents, retention debt, CDC emission. |

### Table 2: Same-Cell Commutation ($A \cdot B \stackrel{?}{=} B \cdot A$ on Shared Cell $c$)

Disjoint cells always commute (`=`). When two operations touch a shared cell or
overlapping `(tenant, table, key_arc)`, split `update(R, W, ...)` into its
**guard factors** ($c \in R \setminus W$, where it acts diagonally in
$\mathcal{T}$) and its **write factors** ($c \in W$):

| Shared cell $c$ | `read` ($D_g$) | `await` ($P$) | `merge` ($C_\delta$) | `update` guard ($c \in R \setminus W$) | `update` write ($c \in W$) | `send` ($a^\dagger_m$) |
|---|---|---|---|---|---|---|
| **`read` ($D_g$)** | **`=`** *(pinned $@K$: `=` w/ all $>K$)* | **`=`** | `≠` *(unless $g(x\oplus\delta)=g(x)$)* | **`=`** | `≠` *(unless $g \circ f = g$)* | **`=`** |
| **`await` ($P$)** | **`=`** | **`=`** | `≠` *(wakes/blocks if $\delta$ flips $P$)* | **`=`** | `≠` *(if $f$ flips $P$)* | **`=`** |
| **`merge` ($C_\delta$)** | `≠` | `≠` | **`=`** *(same abelian $\oplus$, distinct `id`)* | `≠` *(invalidates scanned range)* | `≠` | **`=`** |
| **`update` guard ($R \setminus W$)** | **`=`** | **`=`** | `≠` | **`=`** *(shared guards commute)* | `≠` | **`=`** |
| **`update` write ($W$)** | `≠` | `≠` | `≠` | `≠` | `≠` *(requires consensus order)* | **`=`** |
| **`send` ($a^\dagger_m$)** | **`=`** | **`=`** | **`=`** | **`=`** | **`=`** | **`=`** *(until $a_m \otimes V$)* |

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

### Law 2: Merge Invalidates Scanned Guards
In row 3, column 4 of Table 2, `merge` commutes with `merge` (`=`), **not** with
an `update` guard (`≠`). If a certifying `update` relies on a range or
predicate scan over `arc`, an intervening `AppendOnly` `merge` or row insert
into `arc` alters that predicate and must fail certification. Also, logical
commutation of distinct `AppendOnly` contributions under `(op, position)`
requires retries of the same `id` to carry identical content and does not by
itself make the current replication transport consensus-free.

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
- **Merge batching:** $C_{\delta_1} \cdot C_{\delta_2} = C_{\delta_1 \oplus \delta_2}$
  coalesces commuting `AppendOnly` batches and `max`-cut watermarks.
- **Cut sharing (lockless multi-table backup):** $D_g^{(K)} \cdot D_h^{(K)} = D_{(g,h)}^{(K)}$.
  Pinning **one** common retained cut $K$ across catalog/settings, secondary
  indexes, and all table scans turns logical backup into a pure `read(R_all, K)`,
  retiring `BeginBackupHold` / `ReleaseBackupHold` while upgrading exports from
  per-table cuts $K_i \ge R$ to a single cross-table snapshot.

---

## 5. Convergence Harvest Path (No Big-Bang Rewrite)

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
