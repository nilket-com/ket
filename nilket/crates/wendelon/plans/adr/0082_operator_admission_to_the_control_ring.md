# Wendelon ADR 082: Operator Admission to the Control Ring over the Shared HRFT Listener

**Status:** Accepted 2026-08-09. Implementation is code-complete; live acceptance remains
pending the separately authorized ADR 0539/0541 confirmation rerun.
**Category:** Trust-boundary correction discovered when ADR 081's operator CLI could not reach
the control ring it was designed to drive.

## Evidence that forced the decision

ADR 081 shipped `wendelon index create|drop|status` and routed index mutations through the
control ring. On a clean profile the surface is blocked before any mutation: the CLI presents
an operator certificate signed by `operator_ca_path`, and the listener trusts only
`node_ca_path`. OpenSSL reports `unknown ca` for the operator certificate and a successful
handshake for a node-signed control connection.

The contradiction is that **the control server is already written for operator actors**:

```rust
ControlOperation::SchemaDrop  => ActorPermission::TenantAdminMutation,
ControlOperation::IndexStatus => ActorPermission::TenantAdminRead,
```

`authorize_actor` is called per operation, `ActorTrustDomain` is a real concept, and
`metrics/observability.rs` already loads `ActorTrustRoots::load(node_ca, operator_ca)` and
admits operator identities. The two-domain model exists and works — it was simply never wired
to the control listener, which is built single-root:

```rust
let material = TlsMaterial::load(&tls.cert_path, &tls.key_path, &tls.node_ca_path)?;
let acceptor = TlsAcceptor::from(Arc::new(rustls_server_config(&material)?));
```

So every operator-role branch on this listener is unreachable code. `rustls_server_config_with_peer_roots`
already exists; the control path does not call it.

**The listener is shared, and that is the whole difficulty.** It is not a control-only
listener: the same HRFT ingress routes Data Ring and Raft traffic. Raft ignores
`RequestContext` entirely and relies on the transport's node-only admission for its safety —
an assumption that evaporates the moment any non-node certificate is admitted.

Worse, the Data Ring already contains reachable-on-admission operator branches:

```rust
DataOperation::Write | Read | Query | CdcSubscribe | StreamFinalization
    => matches!(actor.role, ActorRole::DataNode | ActorRole::Operator | ActorRole::BreakGlassAdmin),
DataOperation::Replicate | Migrate | PendingWitnessPull
    => matches!(actor.role, ActorRole::DataNode),
```

`ActorRole::Operator` is *already* permitted tenant reads, writes, queries, CDC subscription
and stream finalization. That code is dead only because no operator certificate can complete
TLS today. Admitting operator certificates without further restriction would therefore grant
operators full tenant data access as an invisible side effect of making a CLI connect — a
privilege expansion nobody asked for, arriving through a change whose stated purpose is
routing.

**This is not a server-config swap.** TLS acceptance is only the first of three gates:

1. **TLS trust** — single-root today; rejects the operator chain outright.
2. **Peer verification** — both inbound paths call `verified_transport_peer*`, which requires
   `InterNodeTransport` permission and derives a canonical `NodeId` from the chain. An operator
   certificate legitimately has neither.
3. **Request context** — `RequestContext { pub remote: NodeId, pub actor: ActorIdentity }`
   makes node identity **mandatory**. An operator connection has no node identity to supply.

Widening only gate 1 moves the rejection to gate 2 and makes the failure harder to read, not
easier. All three must change together, and gate 3 is where the tempting shortcut lives.

## Decisions

1. **The shared listener accepts both trust domains, and non-node actors reach only
   authorized Control Ring messages.** Admission is built from the trust-root-aware path rather
   than a single-root `TlsMaterial`, as the observability listener already is — but admission to
   the *listener* is not admission to the *node*. This record grants non-node actors nothing
   beyond Control Ring operations they are separately authorized for. Data Ring operator
   admission is explicitly **not** granted here, despite the branches that would permit it;
   widening tenant data access is its own decision with its own evidence, and it may travel in
   its own record if wanted. Least privilege is the rule precisely because the permissive
   branches already exist and would otherwise activate silently.

2. **The actual signing root must match the claimed actor role.** Admission is not "either root
   is fine"; a node-signed certificate may not claim an operator role, and an operator-signed
   certificate may not claim a node role. The trust-root-aware verifier decides which domain a
   chain actually came from, and the claimed role is checked against it. Accepting two roots
   without binding role to root would erase the very boundary this record exists to keep.

3. **Dial-side inter-node transport stays node-only.** Nothing here widens what Wendelon will
   *connect out* as, or what it accepts for replication and consensus traffic. Node-to-node
   remains node-signed in both directions.

4. **Optional node identity travels the whole inbound stack, not just `RequestContext`.**
   `TransportConnection::remote_id()`, `QuicTransportConnection`, and `InboundTcpTlsConnection`
   all require a `NodeId` today; each must represent its absence rather than demand a value.
   Making only `RequestContext` optional would push the impossible obligation one layer down,
   where the shortcut in Decision 5 becomes irresistible.

5. **Ingress is classified by a message census, enforced before dispatch.** Every inbound
   message type is declared either *node-required* or *actor-authorized*, and the requirement is
   enforced centrally rather than by patching handlers. At minimum Raft, replication, migration,
   witness, lease and release, and node-origin lifecycle messages are node-required and refuse
   dispatch without an authenticated node identity. A census with a guard is safer than
   per-handler checks because the failure mode is a *missing* check, which no per-handler review
   reliably catches — and because Raft's current safety rests on an assumption this record
   removes.

6. **No synthetic node identity for operators.** There is no sentinel, no zero node, no
   placeholder. A fabricated `NodeId` would satisfy the type and silently corrupt every consumer
   of `remote` — audit attribution, metrics labelling, and fencing decisions — producing records
   that name a node which did not act. An unrepresentable absence is exactly how this defect
   class hides.

7. **QUIC and TCP+TLS obey the same rule.** Both inbound paths verify peers and both build
   request contexts; a rule honoured on one and not the other is not a boundary.

## Guards and controls

- A control proves an operator-signed certificate completes the handshake and is admitted with
  operator role, on both QUIC and TCP+TLS.
- A control proves a node-signed certificate claiming an operator role is rejected, and an
  operator-signed certificate claiming a node role is rejected — cross-domain claims fail
  regardless of which root signed them.
- A control proves node-required messages reject a request whose node identity is absent,
  naming that absence rather than defaulting.
- A control proves an admitted operator actor is refused on the Data Ring — reads, writes and
  queries alike — so the branches that would permit it stay unreachable under this record.
- A control proves an admitted operator actor is refused on Raft ingress.
- A guard proves the ingress census is complete: every message type is classified, so a new
  message cannot default into actor-authorized by omission.
- A control proves ordinary node traffic — replication, consensus, lease and transfer paths — is
  unchanged, with its node identity still present and attributed.
- No test constructs a placeholder `NodeId` to satisfy the context type.

## Acceptance

1. An operator-signed CLI completes an index mutation through the control ring on a clean
   profile, with no test-only construction in the path.
2. Cross-domain role claims are rejected in both directions, proved per transport.
3. Node-only operations reject absent node identity explicitly.
4. Node traffic is unchanged and still attributed, proved by controls that would fail if
   attribution regressed to a placeholder.
5. An admitted operator actor is refused every Data Ring operation and every Raft ingress,
   proved per transport — admission to the listener grants nothing beyond authorized Control
   Ring messages.
6. The ingress census is complete and guarded, so an unclassified message cannot default into
   actor-authorized.
7. ADR 081's Acceptance 1 becomes satisfiable, and with it the ADR 0539 / 0541 confirmation
   sequence.
8. Standard Wendelon, Toron, architecture, format and full `--no-fail-fast` gates pass.

## Rejected and deferred

- **Having the CLI present a node certificate.** It collapses the operator/node distinction and
  grants the CLI node identity — an escalation in the wrong direction, and the opposite of what
  the two-root model is for.
- **A separate operator-only admin listener.** Clean isolation, but it adds a port to secure,
  configure, and reason about while the shared listener already authorizes per operation.
- **A sentinel node identity for operator requests.** See Decision 5; this is the failure mode,
  not a shortcut around it.
- **Widening dial-side transport.** Out of scope and undesirable.
- **Data Ring admission for operator actors.** The permitting branches already exist, so this
  record must refuse them explicitly rather than inherit them; granting tenant data access is a
  separate decision needing its own evidence.

## Consequences

ADR 081 is code-complete but not live-accepted: its Acceptance 1 requires a mutation through
the supported interface, which is unreachable until this lands. The ADR 0539 / 0541
confirmation sequence stays blocked behind it. Closing this makes the operator trust domain
real on the control ring rather than declared and unreachable — and every future
operator-driven control operation inherits the fix rather than rediscovering the wall.
