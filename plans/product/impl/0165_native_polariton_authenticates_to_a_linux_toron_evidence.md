# ADR 0165 implementation evidence

Status: implemented 2026-09-12 across four records; the human form check
of acceptance 1 is an explicit step for the operator (below). The record
remains proposed.

ADR 0165 was implemented in pieces because its first acceptance run
exposed two missing capabilities, each of which became its own record:

- the Nilket half (`impl: adr 0165 nilket issues and guards the client
  identity`, `impl: adr 0165 nilket issues storage identities and
  portable claims`): `key-guard`, `tls-pki` issuance (`init-ca`,
  `issue-listener`, `sign-client` with the tenant SAN forced server-side,
  `issue-storage` claiming the tenant), the portable `actor_claims`
  encoder, and the `nilket toron pki` verbs;
- the Ket half rode in ADR 0166's implementation (`impl: adr 0166 the
  agent surface is first class`) because the connection operation was
  built on it: the `toron` / `toron-local` feature split, the four TLS
  fields beside `endpoint`, `target_from_fields` (all four or none;
  partial refused naming the missing fields; a remote endpoint with no
  TLS refused before any transport), `describe_connect_error`, and the
  key-guard check on the client key on both platforms;
- ADR 0167 provisioned the tenant the Windows identity names, without
  which every query answered `tenant_unavailable`;
- ADR 0168 made a query's answer readable on the receipt, and ADR 0169
  let a Rune source be executed after the connection is established.

The Bigfoot instance stood in for the separate Linux Toron throughout
(`~/nilket-0165`: Wendelon node 1, a Toron worker for tenant 1 with a
storage identity claiming tenant 1, the SQL listener on `192.168.0.2:7742`
with a listener certificate whose SAN is
`toron-00000000-0000-0000-0000-000000000042.cluster.local`, the Windows
client identity `client-1.cluster.local` issued from the deployment CA
against a CSR whose key never left this desktop).

## Acceptance

1. Native Windows Polariton, through its own adapter flow, executed
   authenticated queries against the Bigfoot Toron and received rows
   belonging to tenant 1: ADR 0168's evidence records `run_cell` on
   `SELECT period, SUM(value) AS value FROM trend_showcase GROUP BY
   period ORDER BY period` returning the eight weekly sums in the receipt
   (values checked against the seed), and ADR 0169's evidence records
   `drive uat/plot_trends_showcase.rn` publishing four figures over the
   24-row relation with producer `Toron`. Both went through the same
   connection operation the form uses (ADR 0166 §2, tested by
   `the_form_and_the_command_enter_the_same_connection_operation`).
   The human form check, kept separate on purpose: the operator opens
   Polariton on this desktop, connects through the form with the five
   Toron fields, runs the same query in the SQL pane, and sees the eight
   rows in the grid. Performed by: PENDING (operator).
2. ADR 0166's evidence: an untrusted client certificate ends the
   handshake and is reported as ending during authentication with the
   listener's rejection and its unavailability both named (Toron's log
   establishes `UnknownIssuer`); a server name the listener's
   certificate does not cover is reported as a server name mismatch. The
   two reports are distinct, and each is the form's text.
3. ADR 0166's evidence: one to three of the four fields are refused
   naming the missing fields; the refusal happens before any transport
   and never falls back to loopback (`a_partial_toron_tls_configuration_is_refused_before_any_transport`).
4. ADR 0166's evidence: a client key that fails the access-control check
   is refused before any connection attempt ("client key refused before
   connecting: … is not private: access entry 0 admits a principal other
   than the owner, SYSTEM, or Administrators"), on Windows live and on
   both platforms in `key-guard`'s tests.
5. ADR 0163 and ADR 0167's evidence: SQLite scratch and plotting work in
   the Windows build with no Toron configured (the render-evidence
   harness captured both native showcase plots through the GPU layer).
6. Every Ket change and every Nilket change landed in separate
   implementation commits, named above.

## Limits

The separate instance is a lab deployment on Bigfoot, not slim's
performance deployment, which stayed untouched as agreed. The Windows
identity is a client certificate for tenant 1 only; tenant 0 remains
unissuable by design. ADR 0166's cross-account pipe refusal remains
pending, unrelated to this record's claims.
