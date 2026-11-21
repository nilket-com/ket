//! Wendelon ADR 0120, live local-profile controls run from Toron's tests (they need the
//! public SQL client and the public Wendelon client; the nilket manifest names neither).
//! Each starts real `nilket local` processes from the binary under test,
//! `WENDELON_SMOKE_NILKET_BIN`, and the cross-build control also the previous build's,
//! `WENDELON_SMOKE_NILKET_OLD_BIN`. They bind the profile's fixed public endpoints, so
//! they are ignored by default and run one at a time in nextest's
//! `live-local-fixed-ports` group.

use std::io::{BufRead, BufReader};
use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use arrow::array::{Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use toron_client::{
	ClientError, CtasPublicationCause, MutationCommitKnowledge, MutationResolution,
	PdCtasAbandonCause, PdCtasOutcomeResponse, PdCtasState, QueryId, ResolvedOperationOutcome,
	ToronClient,
};
use wendelon::schema::{
	LocalityPolicy, MultiRowLayoutDescriptor, RecordLayoutPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicy, TableObjectId, TableSchemaDescriptor, TenantId,
};

/// The profile's tenant: its storage credential's.
const TENANT: TenantId = TenantId::ZERO;
const SECRET: [u8; 32] = [0x5e; 32];

fn binary(variable: &str) -> PathBuf {
	PathBuf::from(std::env::var(variable).unwrap_or_else(|_| panic!("{variable} is required")))
}

fn fresh_dir(tag: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.unwrap()
		.as_nanos();
	let dir = std::env::temp_dir().join(format!(
		"toron-pd-profile-{tag}-{}-{nanos}",
		std::process::id()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

fn init(binary: &Path, profile: &Path, authority: &str) {
	let init = Command::new(binary)
		.args(["local", "init", "--data-dir"])
		.arg(profile)
		.args(["--decision-authority", authority])
		.output()
		.expect("run nilket local init");
	assert!(init.status.success(), "{init:?}");
}

fn signal(pid: u32, signal: &str) {
	let sent = Command::new("kill")
		.args([signal, &pid.to_string()])
		.status()
		.expect("run kill");
	assert!(sent.success(), "kill {signal} {pid}");
}

fn children(pid: u32) -> Vec<u32> {
	std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children"))
		.unwrap_or_default()
		.split_whitespace()
		.filter_map(|pid| pid.parse().ok())
		.collect()
}

/// A running `nilket local` supervisor and its two role processes.
struct Live {
	child: Child,
	sql: SocketAddr,
	roles: Vec<u32>,
}

impl Live {
	fn spawn(binary: &Path, profile: &Path, log: &Path) -> Self {
		let stderr = std::fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(log)
			.unwrap();
		let mut child = Command::new(binary)
			.args(["local", "--data-dir"])
			.arg(profile)
			.stdout(Stdio::piped())
			.stderr(stderr)
			.spawn()
			.expect("spawn nilket local");
		let stdout = child.stdout.take().unwrap();
		let (sender, receiver) = std::sync::mpsc::sync_channel(1);
		std::thread::spawn(move || {
			let mut line = String::new();
			let _ = BufReader::new(stdout).read_line(&mut line);
			let _ = sender.send(line);
		});
		let line = receiver
			.recv_timeout(Duration::from_secs(60))
			.unwrap_or_else(|error| {
				let _ = child.kill();
				panic!("no readiness line: {error}")
			});
		let sql = line
			.split_whitespace()
			.find_map(|token| token.strip_prefix("sql="))
			.unwrap_or_else(|| panic!("readiness line {line:?}"))
			.parse()
			.expect("the SQL endpoint");
		let by = Instant::now() + Duration::from_secs(10);
		let roles = loop {
			let roles = children(child.id());
			if roles.len() == 2 || Instant::now() > by {
				break roles;
			}
			std::thread::sleep(Duration::from_millis(50));
		};
		assert_eq!(roles.len(), 2, "two roles");
		Self { child, sql, roles }
	}

	/// The Wendelon role's process.
	fn wendelon_role(&self) -> u32 {
		self.roles
			.iter()
			.copied()
			.find(|pid| {
				std::fs::read(format!("/proc/{pid}/cmdline"))
					.map(|cmdline| String::from_utf8_lossy(&cmdline).contains("wendelon"))
					.unwrap_or(false)
			})
			.expect("the Wendelon role")
	}

	fn stop(mut self) {
		signal(self.child.id(), "-INT");
		let by = Instant::now() + Duration::from_secs(90);
		let status = loop {
			if let Some(status) = self.child.try_wait().unwrap() {
				break status;
			}
			assert!(Instant::now() < by, "the supervisor stops in time");
			std::thread::sleep(Duration::from_millis(100));
		};
		assert!(status.success(), "supervisor exit: {status}");
	}
}

impl Drop for Live {
	fn drop(&mut self) {
		if self.child.try_wait().ok().flatten().is_none() {
			let _ = self.child.kill();
			let _ = self.child.wait();
		}
	}
}

async fn sql(client: &ToronClient, statement: &str) -> toron_client::QueryResult {
	client
		.execute(statement)
		.await
		.unwrap_or_else(|error| panic!("{statement:?}: {error:?}"))
}

/// `(id, v)` rows of a two-column query, in order.
async fn rows(client: &ToronClient, statement: &str) -> Vec<(i64, String)> {
	let result = sql(client, statement).await;
	let mut rows = Vec::new();
	for bytes in &result.batches {
		let reader = arrow::ipc::reader::StreamReader::try_new(std::io::Cursor::new(bytes), None)
			.expect("an IPC stream");
		for batch in reader {
			let batch = batch.expect("a batch");
			let ids = batch
				.column(0)
				.as_any()
				.downcast_ref::<Int64Array>()
				.unwrap();
			let values = batch
				.column(1)
				.as_any()
				.downcast_ref::<StringArray>()
				.unwrap();
			for row in 0..batch.num_rows() {
				rows.push((ids.value(row), values.value(row).to_owned()));
			}
		}
	}
	rows
}

fn owned(rows: &[(i64, &str)]) -> Vec<(i64, String)> {
	rows.iter().map(|(id, v)| (*id, (*v).to_owned())).collect()
}

/// The profile's Wendelon through its storage credential.
async fn profile_wendelon(pki: &Path) -> wendelon_client::Client {
	use wendelon_client::{BootstrapPeer, Client, ClientConfig, TlsPaths};
	let config = ClientConfig::new(
		"127.0.0.1:0".parse().unwrap(),
		vec![BootstrapPeer {
			node_id: 1,
			address: "127.0.0.1:7001".to_owned(),
		}],
		TlsPaths {
			cert_path: pki.join("toron-storage.crt"),
			key_path: pki.join("toron-storage.key"),
			ca_path: pki.join("node-ca.crt"),
		},
	);
	tokio::time::timeout(Duration::from_secs(30), Client::connect_quic(config))
		.await
		.expect("the client connects in time")
		.expect("the client connects")
}

fn pd_schema() -> Schema {
	Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("v", DataType::Utf8, true),
	])
}

/// A ParentDecisions CTAS target in the profile's default namespace (AppendOnly).
fn pd_target(object: u8, name: &str) -> TableSchemaDescriptor {
	TableSchemaDescriptor::new_with_layout(
		TableObjectId::new([object; 16]),
		1,
		"default",
		name,
		&pd_schema(),
		RowIdentityPolicy::AppendOnly,
		LocalityPolicy::default_table_spread(),
		RecordLayoutPolicy::MultiRow(MultiRowLayoutDescriptor::default()),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.expect("a CTAS target")
}

fn pd_rows(rows: &[(i64, &str)]) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(pd_schema()),
		vec![
			Arc::new(Int64Array::from(
				rows.iter().map(|(id, _)| *id).collect::<Vec<_>>(),
			)),
			Arc::new(StringArray::from(
				rows.iter().map(|(_, v)| *v).collect::<Vec<_>>(),
			)),
		],
	)
	.expect("CTAS rows")
}

fn decided(response: wendelon_client::ProposalResponse) -> wendelon_client::DecisionResult {
	match response {
		wendelon_client::ProposalResponse::Applied {
			outcome: wendelon_client::ProposalOutcome::Decision(result),
			..
		} => result,
		other => panic!("a ParentDecisions CTAS command: {other:?}"),
	}
}

/// An owner driving a chain through the public Wendelon client, as Toron does: Begin
/// declaring every chunk (sequences 1, 2, ...), then the chunks it sends.
struct Owner {
	op: [u8; 16],
	descriptor: TableSchemaDescriptor,
	chunks: Vec<RecordBatch>,
}

impl Owner {
	async fn begin(
		wendelon: &wendelon_client::Client,
		id: QueryId,
		descriptor: TableSchemaDescriptor,
		chunks: Vec<RecordBatch>,
	) -> Self {
		let op = *id.get().as_bytes();
		let declared: Vec<wendelon_client::ChunkDecl> = chunks
			.iter()
			.enumerate()
			.map(|(at, chunk)| wendelon_client::ChunkDecl {
				sequence_no: at as u64 + 1,
				rows: chunk.num_rows() as u32,
				request_digest: wendelon_client::ctas_chunk_request_digest(
					TENANT,
					&descriptor,
					op,
					at as u64 + 1,
					chunk,
				)
				.expect("a chunk digest"),
			})
			.collect();
		let begun = decided(
			wendelon
				.ctas_decision(&wendelon_client::CtasProposal::Begin(
					wendelon_client::CtasBeginRequest {
						tenant: TENANT,
						op,
						fingerprint: [0xf1; 32],
						descriptor: descriptor.clone(),
						declaration: wendelon_client::encode_declaration(&declared)
							.expect("a declaration"),
						dispatch_hash: wendelon_client::ctas_owner_commitment(&SECRET),
					},
				))
				.await
				.expect("Begin"),
		);
		assert!(
			matches!(begun, wendelon_client::DecisionResult::CtasBegun { .. }),
			"{begun:?}"
		);
		Self {
			op,
			descriptor,
			chunks,
		}
	}

	async fn send(&self, wendelon: &wendelon_client::Client, at: usize) {
		let options = wendelon_client::WriteOptions {
			await_finalization: true,
			operation_id: Some(wendelon::schema::ClientWriteId {
				client_uuid: self.op,
				sequence_no: at as u64 + 1,
				idempotency_ttl_ms: 300_000,
				_pad: 0,
			}),
			..wendelon_client::WriteOptions::default()
		};
		wendelon
			.ctas_chunk_write(
				TENANT,
				&self.descriptor,
				self.chunks[at].clone(),
				options,
				wendelon_client::CtasChunkCredential {
					op: self.op,
					secret: SECRET,
				},
			)
			.await
			.expect("the chunk commits");
	}

	async fn command(
		&self,
		wendelon: &wendelon_client::Client,
		proposal: wendelon_client::CtasProposal,
	) -> wendelon_client::DecisionResult {
		decided(
			wendelon
				.ctas_decision(&proposal)
				.await
				.expect("a CTAS command"),
		)
	}
}

/// The public resolution of `id`: a ParentDecisions CTAS outcome naming `id`.
async fn pd_outcome(client: &ToronClient, id: QueryId) -> PdCtasOutcomeResponse {
	match client.resolve_mutation(id).await.expect("resolve") {
		ResolvedOperationOutcome::PdCtas(outcome) => {
			assert_eq!(outcome.operation_query_id, id);
			outcome
		}
		other => panic!("a ParentDecisions CTAS outcome: {other:?}"),
	}
}

/// Wendelon ADR 0120 (Codex 01a1fc44, 01a1fc48): on a live ParentDecisions profile the
/// public client resolves operation ids from their decision chains, through the real
/// Toron SQL listener and handler. Published (non-empty and empty, through SQL); an
/// ordinary INSERT (the ordinary outcome, under its own identity); a chain pending,
/// then partly accepted with unequal chunks (3 and 2 rows), then abandoned; a
/// conflict. With the Wendelon role frozen, a resolution answers a typed SQL error,
/// never a dropped stream; the role then resumes and the profile stops cleanly.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts the live local profile on its fixed ports; needs WENDELON_SMOKE_NILKET_BIN"]
async fn a_parent_decisions_profile_resolves_ctas_operations_from_their_chains() {
	let nilket = binary("WENDELON_SMOKE_NILKET_BIN");
	let harness = fresh_dir("resolve");
	let profile = harness.join("profile");
	init(&nilket, &profile, "parent-decisions");
	let live = Live::spawn(&nilket, &profile, &harness.join("local.stderr.log"));
	let client = ToronClient::loopback(live.sql.to_string()).expect("public client");
	sql(
		&client,
		"CREATE TABLE src (id BIGINT NOT NULL PRIMARY KEY, label TEXT)",
	)
	.await;
	let insert = QueryId::new();
	client
		.execute_with_query_id(
			insert,
			"INSERT INTO src (id, label) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
			"UTC",
		)
		.await
		.expect("the source rows");

	// Published through SQL, and from an empty declaration.
	let published = QueryId::new();
	client
		.execute_with_query_id(
			published,
			"CREATE TABLE pd_published AS SELECT id, label FROM src",
			"UTC",
		)
		.await
		.expect("a published CTAS");
	let outcome = pd_outcome(&client, published).await;
	assert_eq!(outcome.state, PdCtasState::Published);
	assert_eq!(
		outcome.publication_commit_knowledge,
		MutationCommitKnowledge::Committed
	);
	assert_eq!((outcome.declared_rows, outcome.accepted_rows), (3, 3));
	assert_eq!(outcome.accepted_chunks, outcome.declared_chunks);
	assert!(outcome.declared_chunks >= 1 && outcome.refused_chunks == 0);
	assert_eq!(outcome.epoch, 1);
	assert_ne!(outcome.table_object_id, [0; 16]);
	let empty = QueryId::new();
	client
		.execute_with_query_id(
			empty,
			"CREATE TABLE pd_empty AS SELECT id, label FROM src WHERE id < 0",
			"UTC",
		)
		.await
		.expect("an empty CTAS");
	let outcome = pd_outcome(&client, empty).await;
	assert_eq!(outcome.state, PdCtasState::Published);
	assert_eq!(
		(
			outcome.declared_chunks,
			outcome.declared_rows,
			outcome.accepted_rows
		),
		(0, 0, 0),
		"an empty declaration"
	);

	// An ordinary operation resolves as before, under its own identity.
	match client.resolve_mutation(insert).await.expect("resolve") {
		ResolvedOperationOutcome::Mutation(outcome) => {
			assert_eq!(outcome.operation_query_id, insert);
			assert_eq!(outcome.resolution, MutationResolution::Present);
		}
		other => panic!("an ordinary INSERT: {other:?}"),
	}

	// Pending, then partly accepted (chunks of 3 and 2 rows), then abandoned.
	let wendelon = profile_wendelon(&profile.join("pki")).await;
	let abandoned = QueryId::new();
	let owner = Owner::begin(
		&wendelon,
		abandoned,
		pd_target(0xE5, "pd_abandoned"),
		vec![
			pd_rows(&[(1, "a"), (1, "a"), (2, "b")]),
			pd_rows(&[(3, "c"), (4, "d")]),
		],
	)
	.await;
	let outcome = pd_outcome(&client, abandoned).await;
	assert_eq!(outcome.state, PdCtasState::Pending);
	assert_eq!(
		(
			outcome.declared_chunks,
			outcome.declared_rows,
			outcome.accepted_chunks
		),
		(2, 5, 0)
	);
	assert_eq!(
		outcome.publication_commit_knowledge,
		MutationCommitKnowledge::Unspecified
	);
	assert_eq!(outcome.table_object_id, [0xE5; 16]);
	owner.send(&wendelon, 0).await;
	let outcome = pd_outcome(&client, abandoned).await;
	assert_eq!(outcome.state, PdCtasState::Pending);
	assert_eq!((outcome.accepted_chunks, outcome.accepted_rows), (1, 3));
	let abandon = owner
		.command(
			&wendelon,
			wendelon_client::CtasProposal::Abandon {
				tenant: TENANT,
				op: owner.op,
				epoch: 1,
				secret: SECRET,
				cause: wendelon_client::CtasAbandonCause::Deadline,
			},
		)
		.await;
	assert!(
		matches!(
			abandon,
			wendelon_client::DecisionResult::CtasAbandoned { .. }
		),
		"{abandon:?}"
	);
	let outcome = pd_outcome(&client, abandoned).await;
	assert_eq!(
		outcome.state,
		PdCtasState::Abandoned(PdCtasAbandonCause::Deadline)
	);
	assert_eq!(
		(
			outcome.declared_chunks,
			outcome.declared_rows,
			outcome.accepted_chunks,
			outcome.accepted_rows,
			outcome.refused_chunks
		),
		(2, 5, 1, 3, 0),
		"exactly the evidence: one chunk of 3 rows accepted, one never sent"
	);
	assert_eq!(
		outcome.publication_commit_knowledge,
		MutationCommitKnowledge::KnownNotCommitted
	);

	// Conflict: every chunk accepted, the name bound before Publish.
	let conflicted = QueryId::new();
	let owner = Owner::begin(
		&wendelon,
		conflicted,
		pd_target(0xE6, "pd_conflict"),
		vec![pd_rows(&[(1, "a"), (2, "b")])],
	)
	.await;
	owner.send(&wendelon, 0).await;
	sql(
		&client,
		"CREATE TABLE pd_conflict (id BIGINT NOT NULL, v TEXT)",
	)
	.await;
	let publish = owner
		.command(
			&wendelon,
			wendelon_client::CtasProposal::Publish {
				tenant: TENANT,
				op: owner.op,
				epoch: 1,
				secret: SECRET,
			},
		)
		.await;
	assert!(
		matches!(
			publish,
			wendelon_client::DecisionResult::CtasConflict { .. }
		),
		"{publish:?}"
	);
	let outcome = pd_outcome(&client, conflicted).await;
	assert_eq!(outcome.state, PdCtasState::Conflict);
	assert_eq!((outcome.accepted_chunks, outcome.accepted_rows), (1, 2));
	assert_eq!(
		(
			outcome.publication_commit_knowledge,
			outcome.publication_cause
		),
		(
			MutationCommitKnowledge::KnownNotCommitted,
			Some(CtasPublicationCause::BindingConflict)
		)
	);
	drop(wendelon);

	// The Wendelon role frozen: the lookup ends at the server's bound with a typed SQL
	// error, never a dropped stream. The role then resumes.
	let role = live.wendelon_role();
	signal(role, "-STOP");
	let frozen = client.resolve_mutation(published).await;
	signal(role, "-CONT");
	match frozen {
		Err(ClientError::Remote { code, message }) => {
			eprintln!("ADR0120: a resolution with the store frozen: {code:?} {message}");
		}
		other => panic!("a typed SQL error: {other:?}"),
	}
	drop(client);
	live.stop();
	std::fs::remove_dir_all(harness).expect("remove the fixture");
}

/// Stale ParentDecisions commands of an unknown operation (Resolve, Publish, Abandon)
/// sent to the profile's Wendelon.
async fn stale_parent_decisions_requests(pki: &Path) -> Vec<wendelon_client::ProposalResponse> {
	use wendelon_client::CtasProposal;
	let wendelon = profile_wendelon(pki).await;
	let op = *QueryId::new().get().as_bytes();
	let mut answers = Vec::new();
	for proposal in [
		CtasProposal::Resolve { tenant: TENANT, op },
		CtasProposal::Publish {
			tenant: TENANT,
			op,
			epoch: 1,
			secret: SECRET,
		},
		CtasProposal::Abandon {
			tenant: TENANT,
			op,
			epoch: 1,
			secret: SECRET,
			cause: wendelon_client::CtasAbandonCause::Deadline,
		},
	] {
		answers.push(
			tokio::time::timeout(Duration::from_secs(30), wendelon.ctas_decision(&proposal))
				.await
				.expect("an answer in time")
				.expect("an answer"),
		);
	}
	answers
}

/// Wendelon ADR 0120's fence: Today's encodings are unchanged (Codex 01a1fad7, 01a1fc24,
/// 01a1fc44), in both directions. A Today local profile written by the PREVIOUS build
/// (a keyed table and a CTAS through Today's saga, so its log holds saga entries)
/// reopens on this build: both tables read exactly, a new CTAS succeeds on Today's path,
/// the source takes a new row, and this build's receiver refuses stale ParentDecisions
/// requests before any proposal (`Refused(ModeToday)`). Then the PREVIOUS build reopens
/// the same store and reads everything this build wrote.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "starts live local profiles on their fixed ports; needs WENDELON_SMOKE_NILKET_BIN and WENDELON_SMOKE_NILKET_OLD_BIN"]
async fn a_previous_builds_today_profile_reopens_and_takes_a_new_ctas() {
	let (old, new) = (
		binary("WENDELON_SMOKE_NILKET_OLD_BIN"),
		binary("WENDELON_SMOKE_NILKET_BIN"),
	);
	let harness = fresh_dir("today-reopen");
	let profile = harness.join("profile");
	let log = harness.join("local.stderr.log");
	init(&old, &profile, "today");
	let config = std::fs::read_to_string(profile.join("config/wendelon.toml")).unwrap();
	assert!(
		!config.lines().any(|line| {
			line.trim_start().starts_with("decision_authority") && line.contains("parent-decisions")
		}),
		"premise: a Today profile (no ParentDecisions setting)"
	);
	let expected = owned(&[(1, "one"), (2, "two"), (3, "three")]);

	let first = Live::spawn(&old, &profile, &log);
	let client = ToronClient::loopback(first.sql.to_string()).expect("public client");
	sql(
		&client,
		"CREATE TABLE src (id BIGINT NOT NULL PRIMARY KEY, label TEXT)",
	)
	.await;
	sql(
		&client,
		"INSERT INTO src (id, label) VALUES (1, 'one'), (2, 'two'), (3, 'three')",
	)
	.await;
	sql(&client, "CREATE TABLE copied AS SELECT id, label FROM src").await;
	assert_eq!(
		rows(&client, "SELECT id, label FROM copied ORDER BY id").await,
		expected,
		"the previous build's Today CTAS"
	);
	drop(client);
	first.stop();

	let second = Live::spawn(&new, &profile, &log);
	let client = ToronClient::loopback(second.sql.to_string()).expect("public client");
	for table in ["src", "copied"] {
		assert_eq!(
			rows(
				&client,
				&format!("SELECT id, label FROM {table} ORDER BY id")
			)
			.await,
			expected,
			"{table}, reopened by this build"
		);
	}
	sql(
		&client,
		"CREATE TABLE recopied AS SELECT id, label FROM src",
	)
	.await;
	assert_eq!(
		rows(&client, "SELECT id, label FROM recopied ORDER BY id").await,
		expected,
		"a new CTAS on the reopened Today profile"
	);
	sql(&client, "INSERT INTO src (id, label) VALUES (4, 'four')").await;
	let with_four = owned(&[(1, "one"), (2, "two"), (3, "three"), (4, "four")]);
	assert_eq!(
		rows(&client, "SELECT id, label FROM src ORDER BY id").await,
		with_four,
		"the source takes a new row"
	);
	let refused = stale_parent_decisions_requests(&profile.join("pki")).await;
	assert!(
		refused.iter().all(|answer| matches!(
			answer,
			wendelon_client::ProposalResponse::Refused(wendelon_client::ProposalRefusal::ModeToday)
		)),
		"a Today authority refuses a stale ParentDecisions request: {refused:?}"
	);
	drop(client);
	second.stop();

	let third = Live::spawn(&old, &profile, &log);
	let client = ToronClient::loopback(third.sql.to_string()).expect("public client");
	for table in ["copied", "recopied"] {
		assert_eq!(
			rows(
				&client,
				&format!("SELECT id, label FROM {table} ORDER BY id")
			)
			.await,
			expected,
			"{table}, reopened by the previous build"
		);
	}
	assert_eq!(
		rows(&client, "SELECT id, label FROM src ORDER BY id").await,
		with_four,
		"the source, reopened by the previous build"
	);
	drop(client);
	third.stop();
	std::fs::remove_dir_all(harness).expect("remove the fixture");
}
