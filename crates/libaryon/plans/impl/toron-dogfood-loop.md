# The Toron dogfood loop

Scope: make the data lifecycle boring enough to live in — create, shape,
query, and (later) plot a dataset in Polariton against Toron, daily.
**Cross-repo**: this document owns the ket side and fixes the shared
contract; nilket-side work items are named here but executed under
nilket's own plan conventions. Plotting (ADR 0006) resumes *after* this
loop closes — strong opinions about plot configuration will be formed by
someone who shaped the data being plotted, in-product, that morning.

## The contract: one acceptance script, two honest outcomes per statement

```sql
create table dogfood (
  id bigint not null primary key,
  category text,
  value double
);

insert into dogfood values
  (1, 'alpha', 10.0),
  (2, 'beta', 20.0),
  (3, 'alpha', 30.0);

select category, sum(value)
from dogfood
group by category
order by category;

update dogfood set value = 25.0 where id = 2;
delete from dogfood where id = 1;
drop table dogfood;
```

Every statement either **executes** or yields a **typed, fully readable
refusal that becomes an enumerated nilket work item**. Toron's ~15% SQL
surface makes partial coverage the expected starting state. One subset is
non-negotiable: `CREATE` + `INSERT` + restart + `SELECT` must execute before
the loop is closed, because without it there is no durable product loop to
dogfood. Aggregate, `UPDATE`, `DELETE`, and `DROP` may close as enumerated
gaps. The script is the driver for nilket's SQL-surface work, run again after
each nilket advance.

**The durability sequence is part of acceptance**: `create` + `insert` →
**restart the deployment** → `select` returns the rows. "Durable
launcher" means durable *storage*, exercised — not a launcher that
restarts politely over amnesia.

## Nilket-side prerequisites (named here, owned there)

From the first-contact findings, now promoted:

1. **One durable command** launches local Wendelon + Toron into a
   *queryable* state. It obeys nilket brainstorm 005's two-process invariant,
   uses real loopback mTLS between the roles, and targets Wendelon's real
   bootstrapped default tenant (`0`) rather than relying on the currently
   leaderless/empty-provider non-TLS Toron path or the example config's
   mismatched tenant `1`.
2. **A public Toron client crate** — connect, execute, cancel-frame,
   health probe — extracted from `toron::cli`'s private internals, not
   redesigned. Polariton's thin client and its duplicated protocol enums
   die the day this ships (second death warrant honored).
3. SQL surface advances driven by the contract script's refusals.

## Ket-side work (this repo, deliberately thin)

- **Harness-free connect**: a Toron connection profile targeting the
  durable deployment; no test scaffolding in the path.
- **Readable errors**: Toron's typed codes and full message text reach a
  surface that can show all of it — the existing output-panel machinery
  suffices; no truncated status-bar-only errors. Full error-UX design
  waits for use to inform it.
- **Reconnect behavior explicit**: connection loss and re-establishment
  are visible states with predictable recovery, not silent staleness.
- **Provenance retained**: results identify their connection/source
  honestly end to end.
- **Catalog browsing (last)**: query Toron's existing tenant-scoped
  `information_schema.schemata`, `.tables`, and `.columns` through the same
  general SQL client — minimal, read-only, no catalog-specific wire method.
- Adopt the public client crate when it ships; delete
  `adapters/toron/client.rs`.

## The dogfood diary (the slice's living deliverable)

`crates/libaryon/plans/impl/toron-dogfood-diary.md` — appended per
session, dated entries, same honesty register as the first-contact
findings: what worked, what refused, what chafed, what the seam must
express. This diary — not the script — is what informs ADR 0006 and the
Arrow seam. Strong opinions require receipts.

## Explicitly out of scope

- **UI cancellation** — the first-contact findings established it needs a
  query handle the closed adapter trait cannot express; it is the Arrow
  seam's first deliverable, not this slice's last step. Mistaken queries
  ride Toron's statement timeout until then.
- **CREATE USER / auth** — loopback tenant identity suffices; authn/authz
  get their own design after catalog ownership is *felt* through use.
- The Arrow seam itself, ToronScratch, promotion-gate progress claims,
  performance work, remote/mTLS connections.

## Review checklist (ket side)

- [ ] Contract script checked in and runnable as an ignored live fixture;
      per-statement outcome (executed / typed refusal) recorded.
- [ ] Durability sequence demonstrated across a real restart.
- [ ] Errors fully readable; typed codes preserved to the UI boundary.
- [ ] Reconnect states explicit; no silent staleness.
- [ ] Thin client deleted if the client crate landed; otherwise its death
      warrant updated with the crate's name.
- [ ] Diary exists with at least the first real session's entry —
      authored from use, not from tests.
