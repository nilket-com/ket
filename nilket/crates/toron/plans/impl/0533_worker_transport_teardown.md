# impl 533 - Worker transport teardown hygiene

Scope: make successful Toron worker RPC completion end as a clean transport
exchange instead of a warning-shaped connection failure. This is an
implementation correction, not a new architecture decision: the coordinator
already owns request completion, `ToronTransport` already exposes a
bidirectional stream, and the worker already treats a clean EOF between
requests as normal. Deliverable is one `impl:` commit reviewed against the
gates below.

## Dogfood evidence

The first overnight local-deployment soak (2026-08-01) remained healthy:
Polariton reconnected, queries returned data, and plots rendered. Successful
queries nevertheless emitted paired warnings from `toron::daemon`:

```text
QUIC worker connection error error=query IO error: connection lost
```

This is not the public SQL connection. `run_quic_worker_accept_loop` accepts an
internal coordinator-to-worker QUIC stream and reports any non-`Ok` result from
`ToronWorker::handle_connection` as a warning.

The code path explains the contradiction:

1. `QuicTransport` creates a QUIC connection and one bidirectional stream for a
   worker RPC.
2. `ToronCoordinator` reads the terminal `TaskComplete`, `TaskFailed`,
   `PartitionData`, `PartitionMissing`, or `TaskCancelled` response.
3. The coordinator returns and drops the boxed stream without closing its send
   half.
4. The worker has completed the request and loops for a possible next envelope.
   QUIC reports the dropped connection as `connection lost`, not the
   `UnexpectedEof` currently classified as normal teardown.

The paired warnings are consistent with a query dispatching two worker RPCs.
The warning therefore describes transport lifecycle noise after successful
work, not lost work. Tests based only on `tokio::io::DuplexStream` did not catch
the distinction because dropping that fixture produces an ordinary EOF.

## Existing soak infrastructure

Wendelon already carries the repository's soak vocabulary in
`crates/wendelon/examples/soak_driver.rs` and
`crates/wendelon/scripts/soak/run_commodity_soak.sh`: bounded in-memory samples,
an explicit output directory, machine-readable configuration/samples/summary,
and stable `SOAK_START`/`SOAK_DONE` publication lines. The later idle-control-
plane pair must extend those evidence conventions rather than create an
unrelated overnight harness.

The historical scheduled workflows also survived the monorepo graft under
`crates/wendelon/.github/workflows/`. GitHub only discovers workflows from the
repository-root `.github/workflows`, so those nested schedules are currently
dormant. Reactivating them with monorepo-correct paths is a CI follow-up and is
not bundled into this transport correction. The production-QUIC regression
below remains a fast deterministic test suitable for ordinary validation.

## Required behavior

### 1. The request owner closes its write half

Every coordinator RPC must call `AsyncWriteExt::shutdown` after it receives and
validates a terminal response and before it returns:

- task execution, including success and remote `TaskFailed`;
- partition pull, including present and missing responses;
- cancellation acknowledgement.

The close belongs to the request owner rather than `Drop`: it is ordered after
the terminal frame, works through the transport-neutral `ToronStream` trait,
and can be tested. TCP receives a FIN; QUIC finishes the bidirectional stream's
send half. No QUIC-specific downcast or close call enters the coordinator.

If a terminal response reports a query/task failure and the subsequent close
also fails, preserve the terminal application error as the primary result. A
close failure after a successful response is a transport error because the
exchange did not complete cleanly. A small helper may encode this precedence,
but it must not hide a pre-terminal I/O failure.

### 2. Worker classification follows protocol phase

`ToronWorker::handle_connection` distinguishes three states:

- EOF before any envelope: an unused connection closed cleanly;
- peer closure after a terminal response was successfully sent: idle teardown
  between completed exchanges, not failed work;
- closure while reading or writing an active exchange: a connection failure.

The cause fix above should make current QUIC success use the first clean-FIN
path after one completed exchange. The phase-aware classification is retained
as defense in depth for peers and transports whose connection closure races the
stream FIN. It must be narrow: a peer disappearing after `StartTask` but before
`TaskComplete`/`TaskFailed` remains an error. Do not globally demote
`ConnectionReset`, `ConnectionAborted`, `BrokenPipe`, or string-matched
"connection lost" errors.

The implementation should represent “a terminal response was sent” explicitly
at the response boundary. Inferring it from task registration, audit state, or
whether any bytes were exchanged would misclassify partial work.

### 3. Logging reports work truthfully

The daemon continues to warn when `handle_connection` returns an actual error.
Normal completion returns `Ok(())`, so it produces no warning. Do not solve the
dogfood symptom by lowering every worker-connection error to `debug` or by
string-filtering the daemon log.

## Tests

1. **Real QUIC success.** Execute a worker task through `QuicTransport` and the
   production accept/handler shape. Assert the task completes and the worker
   handler returns `Ok(())` after the coordinator exits. A duplex-only fixture
   is insufficient for this regression.
2. **Terminal failure closes cleanly.** A worker-produced `TaskFailed` remains
   the coordinator's typed task error while the worker observes clean teardown.
3. **Partition and cancellation closure.** Present/missing partition responses
   and `TaskCancelled` all end cleanly; this prevents the same warning from
   returning through a less common RPC.
4. **Abrupt pre-terminal loss remains an error.** Drop or reset a real QUIC
   connection after a valid request begins but before its terminal response;
   the handler must return an error classified for warning.
5. **Connection reuse remains valid.** An in-memory stream may carry two
   complete envelopes before the client closes. The phase tracking must not
   force the worker to exit after its first request; only the coordinator's
   one-request stream owner chooses to shut down.
6. **Positive control for classification.** Exercise both the accepted
   between-exchange close and rejected active-exchange close in the same test
   module, so the negative assertion cannot pass because the classifier stopped
   observing disconnects altogether.

No elapsed-time assertion is required. Completion and handler results are the
contract; captured tracing output may be an additional assertion, not the only
one.

## Non-goals

- connection pooling or persistent worker sessions;
- retries, cancellation redesign, or query scheduling changes;
- SQL-client transport changes;
- Wendelon CDC cadence, audit retention, or OpenRaft snapshot policy;
- suppressing genuine multi-node network failures.

## Review gates

- [ ] Coordinator task, partition, and cancellation RPCs explicitly close the
      send half after terminal responses.
- [ ] Application-error precedence over a secondary close error is tested.
- [ ] Worker clean-close classification is phase-aware and does not contain a
      broad I/O-kind or error-string suppression.
- [ ] A production-QUIC regression reproduces the transport behavior that the
      duplex fixtures missed.
- [ ] The transport pair does not fork or duplicate Wendelon's soak-driver
      evidence format; the idle-soak extension remains assigned to the
      Wendelon pair.
- [ ] A pre-terminal QUIC loss still returns an error.
- [ ] Multi-envelope worker handling remains supported.
- [ ] Toron unit, integration, architecture, formatting, and focused Clippy
      validation pass.
- [ ] The running dogfood deployment is not restarted or mutated during
      implementation validation; post-install confirmation is a separate user
      soak.
