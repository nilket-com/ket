# ADR 0167 implementation evidence

Status: implemented 2026-09-12. The record remains proposed.

Nilket side only. `WendelonClient::tenant_admin` sends `TENANT_ADMIN_REQ`
to the leader with the same one-redirect-per-attempt retry the schema
update call uses, and `tenant_admin_outcome` turns the handler's reason
code into an error kind (authorization failed → permission denied, not
found → not found, invalid target → already exists, invalid config →
invalid input) keeping the handler's message verbatim. `nilket wendelon
tenant create|get|list` is a new CLI module over that call; `create` takes
`--tenant`, with `--quota-bytes` defaulting to the bootstrap tenant's
unlimited quota (`DEFAULT_TENANT_QUOTA_BYTES`, now public) and
`--replication-factor` defaulting to the cluster's replication factor as
the client learned it; `--json` prints the records. The connection flags
and the `connect` function are shared with `wendelon index` rather than
copied. `tls_pki::issue_operator` and `nilket toron pki issue-operator`
issue an operator leaf (role `operator`, no tenant claim, no DNS name, a
random key id) signed by the operator CA, the key written owner-only and
never overwritten. Nothing in Toron's startup and nothing in Polariton
changed.

Tests: `tenant_admin_outcome` mapping; the tenant CLI's parser (defaults,
explicit values, `list` refusing `--tenant`, the identity being required,
unknown verbs); the rendered line naming an unlimited quota; the operator
leaf's claims and name rule; the pki verb writing a `0600` key once; and
an architecture guard asserting the dispatch arm, the three documented
verbs, the client sending the opcode, and the tenant CLI never touching an
in-process node.

## Scope discovered during implementation

The `run_cell` receipt reports that a SQL cell applied or failed but
carries no result rows, so an agent cannot see a query's answer through
the receipt alone. This cut evidences rows through the product's other
surface for data, `publish`, whose manifest names the dataset, its
producer, and its row count. Carrying a result summary in the `run_cell`
receipt is a gap in ADR 0166's remaining distance, recorded there.

The Windows Polariton binary must be built with `--features toron` for the
`connect` command to know the adapter; an integration-test build of the
package had rebuilt it without, which the first attempt's "unknown
adapter" refusal exposed. Not a code change; noted for the runbook.

## Verification

Nilket: Bigfoot, Ubuntu, Rust 1.98.1, the plan commit `3e69b4e0` with this
implementation in place (snapshot `332311c0`). `tls-pki` issue tests 7
passed; `toron` pki CLI tests 3 passed; `wendelon` client and tenant CLI
tests 4 passed; `wendelon` architecture tests 18 passed including
`adr0167_wendelon_tenant_command_reaches_the_tenant_admin_rpc`. The Nilket
lane on the same snapshot: exit 0, 181 targets, zero failures. Log: `~/ci-logs/adr-0167-nilket.log`.

Acceptance, on the Bigfoot instance as it stood: Wendelon node 1 at
`127.0.0.1:7001` holding tenant 0 only; the ADR 0165 Toron worker
(`tenant_id = 1`, storage identity claiming tenant 1, SQL listener
`192.168.0.2:7742`); the Windows Polariton with the tenant-1 client
identity, driven only through `polariton act` on a window opened with
`--agents` on a three-cell markdown notebook (`SELECT` from
`trend_showcase`, `INSERT` into a probe table, `SELECT` from it). Both
daemons ran throughout; `pgrep` showed the same two processes before and
after.

1. Before any tenant existed, `connect` applied (generation 1), and the
   cells answered: the read `Toron Internal: task failure: Internal {
   reason: "wendelon query failed: tenant_unavailable(8)" }`, the write
   `Toron StorageError: storage unavailable` (Toron folds the write-path
   tenant refusal into storage unavailability, `toron/src/error.rs`).
2. `nilket toron pki issue-operator --ca-cert pki/operator-ca.crt --ca-key
   pki/operator-ca.key --out-dir pki-0165/ops --name bigfoot-ops` wrote
   `operator.crt` and a `0600` `operator.key`. With that identity,
   `wendelon tenant list` showed tenant 0 alone; `wendelon tenant create
   --tenant 1` printed `created tenant 1 Active quota unlimited used 0
   bytes replication 1` (14:21:04 UTC); `list` then showed 0 and 1
   `Active`; `create --tenant 1` again exited 1 with `tenant 1 already
   exists`, and `get --tenant 1 --json` showed the record with its active
   tenant KEK. The same `create` with the tenant-scoped storage identity
   (node CA, tenant claim 1) exited 1 with the handler's `tenant-scoped
   principals may not use TenantAdmin`; with the tenant-1 client identity
   (deployment CA) it never passed TLS, `connection lost`, because Wendelon
   trusts the node and operator CAs only. Neither refusal changed the list.
3. `nilket toron sql --endpoint 192.168.0.2:7742 --server-name
   toron-…42.cluster.local --cert pki-0165/bigfoot-t1.crt --key
   pki-0165/bigfoot-t1.key --ca pki-0165/ca.crt --file
   uat/plot_trends_showcase.sql` ran its three statements (`OK`, `OK`,
   `OK rows=24`); `SELECT count(*) FROM trend_showcase` answered 24.
4. In the same Windows window and connection (generation 1, no restart of
   Toron, Wendelon, or Polariton), the probe `INSERT` and `SELECT` that
   had failed in step 1 both applied. A second notebook, connected the
   same way, ran `SELECT period, SUM(value) AS value FROM trend_showcase
   GROUP BY period ORDER BY period` and a Rune cell plotting it, and
   `publish` wrote a package whose manifest records the dataset
   `portfolio`, producer `Toron`, 8 rows, fields `period` and `value`,
   with the rows in `data/<sha256>.arrow` and the figure in
   `figures/figure-1/figure.svg`. The Arrow stream read back (`arrow` 58
   `StreamReader`) holds one batch of 8 rows: periods 1 through 8 with
   values 113, 121, 121, 136, 148, 147, 166, 179, which are exactly the
   per-period sums of the seed's Northstar, Horizon, and Pulse values.
   The seeded rows reached Windows with their values; the rendering is
   not this ADR's claim. The receipts, manifest, Arrow data, its decoded
   form, and the figure are kept under
   `plans/product/impl/exhibits/0167_tenant_provisioning/`.

Recipe, for the record: issue the operator identity once per deployment;
`wendelon tenant create --tenant N` with it; `toron sql --file <seed.sql>`
with a client identity for N; then any client for N sees the relation.
