//! Wendelon ADR 0120 S3: Toron's ParentDecisions CTAS path, against a catalog
//! that answers as a ParentDecisions authority does. The catalog records every
//! ParentDecisions call, so a control sees whether the statement resolved its
//! operation before the SELECT, ran a chain, or adopted one.

use std::sync::{Arc, Mutex};

use arrow::datatypes::SchemaRef;
use async_trait::async_trait;
use toron::binder::PlannerConfig;
use toron::catalog::{
	Catalog, CreateTableOutcome, DescriptorRefreshMetrics, InvalidatedDescriptor, MockCatalog,
	PdCtasOutcome, PdCtasRequest, TableBinding, TableRef,
};
use toron::coordinator::QueryCoordinator;
use toron::distributed::identity::QueryId;
use toron::error::CatalogError;
use toron::exec::Executor;
use toron::ir::CreateTableRowIdentity;
use toron::sql_planner::SqlPlanner;
use toron::storage::{InMemoryTableStorage, TenantId};

/// How the fake authority answers.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Authority {
	/// The definitive pre-proposal wrong-mode answer to the Today saga.
	ParentDecisions,
	/// A Today saga failure that is not that answer.
	Unavailable,
}

/// What the fake decision chain does when run.
#[derive(Clone, Copy, PartialEq, Eq)]
enum Run {
	Publish,
	/// A concurrent statement binds the name first (Begin's NameBound or a
	/// conflict at Publish), with the given schema.
	WinnerBinds {
		same_schema: bool,
	},
}

struct PdCatalog {
	inner: Arc<MockCatalog>,
	authority: Authority,
	run: Run,
	/// Published operations: op → the statement fingerprint.
	published: Mutex<Vec<([u8; 16], [u8; 32])>>,
	/// Every run: op and its chunks' row counts.
	runs: Mutex<Vec<([u8; 16], Vec<usize>)>>,
	resolves: Mutex<usize>,
	/// A same-operation publication that races the bound-name check: when the
	/// named table is first resolved, another run of the operation last resolved
	/// publishes it, with this fingerprint.
	race: Mutex<Option<(TableRef, [u8; 32])>>,
	last_op: Mutex<Option<[u8; 16]>>,
}

impl PdCatalog {
	fn new(inner: Arc<MockCatalog>, authority: Authority, run: Run) -> Self {
		Self {
			inner,
			authority,
			run,
			published: Mutex::new(Vec::new()),
			runs: Mutex::new(Vec::new()),
			resolves: Mutex::new(0),
			race: Mutex::new(None),
			last_op: Mutex::new(None),
		}
	}

	fn runs(&self) -> Vec<([u8; 16], Vec<usize>)> {
		self.runs.lock().unwrap().clone()
	}

	fn resolves(&self) -> usize {
		*self.resolves.lock().unwrap()
	}

	fn published_fingerprint(&self) -> [u8; 32] {
		self.published.lock().unwrap()[0].1
	}
}

fn published_chain(fingerprint: [u8; 32]) -> wendelon_client::CtasChain {
	wendelon_client::CtasChain {
		object: wendelon_client::TableObjectId::new([7; 16]),
		fingerprint,
		chunks: Vec::new(),
		row_count: 0,
		dispatch_hash: [0; 32],
		epoch: 1,
		lease_until_ms: 0,
		terminal_at_ms: 0,
		phase: wendelon_client::CtasPhase::Published { at: 1 },
		accepted: 0,
		refused: 0,
		retained_bytes: 0,
	}
}

#[async_trait]
impl Catalog for PdCatalog {
	fn scoped(self: Arc<Self>, _tenant_id: TenantId) -> Arc<dyn Catalog> {
		self
	}

	fn descriptor_refresh_metrics(&self) -> Arc<DescriptorRefreshMetrics> {
		self.inner.descriptor_refresh_metrics()
	}

	async fn resolve_table(&self, table: &TableRef) -> Result<TableBinding, CatalogError> {
		let race = {
			let mut race = self.race.lock().unwrap();
			match race.as_ref() {
				Some((name, _)) if name == table => race.take(),
				_ => None,
			}
		};
		if let Some((name, fingerprint)) = race {
			let op = self.last_op.lock().unwrap().expect("a resolved operation");
			self.inner
				.create_table_with_outcome(
					&name,
					Arc::new(arrow::datatypes::Schema::new(vec![
						arrow::datatypes::Field::new(
							"id",
							arrow::datatypes::DataType::Int32,
							false,
						),
					])),
					CreateTableRowIdentity::AppendOnly,
					false,
				)
				.await?;
			self.published.lock().unwrap().push((op, fingerprint));
		}
		self.inner.resolve_table(table).await
	}

	async fn resolve_table_id(
		&self,
		table: toron::ir::TableId,
	) -> Result<TableBinding, CatalogError> {
		self.inner.resolve_table_id(table).await
	}

	async fn resolve_durable_logical_type_by_name(
		&self,
		name: &str,
	) -> Result<toron::ir::LogicalTypeSpec, CatalogError> {
		self.inner.resolve_durable_logical_type_by_name(name).await
	}

	async fn invalidate_table_descriptor(
		&self,
		table: toron::ir::TableId,
	) -> Result<Option<InvalidatedDescriptor>, CatalogError> {
		self.inner.invalidate_table_descriptor(table).await
	}

	async fn create_table_with_outcome(
		&self,
		name: &TableRef,
		schema: SchemaRef,
		row_identity: CreateTableRowIdentity,
		if_not_exists: bool,
	) -> Result<CreateTableOutcome, CatalogError> {
		self.inner
			.create_table_with_outcome(name, schema, row_identity, if_not_exists)
			.await
	}

	async fn resolve_ctas_saga(
		&self,
		_operation_id: wendelon_client::CtasOperationId,
	) -> Result<Option<wendelon_client::CtasSagaEntry>, CatalogError> {
		match self.authority {
			Authority::ParentDecisions => Err(CatalogError::CtasUnderParentDecisions),
			Authority::Unavailable => Err(CatalogError::AdminFailed {
				message: "resolve CTAS saga: connection reset".to_string(),
			}),
		}
	}

	async fn resolve_pd_ctas(
		&self,
		op: [u8; 16],
	) -> Result<Option<wendelon_client::CtasChain>, CatalogError> {
		*self.resolves.lock().unwrap() += 1;
		*self.last_op.lock().unwrap() = Some(op);
		Ok(self
			.published
			.lock()
			.unwrap()
			.iter()
			.find(|(published, _)| *published == op)
			.map(|(_, fingerprint)| published_chain(*fingerprint)))
	}

	async fn run_pd_ctas(&self, request: PdCtasRequest) -> Result<PdCtasOutcome, CatalogError> {
		self.runs.lock().unwrap().push((
			request.op,
			request
				.chunks
				.iter()
				.map(|chunk| chunk.num_rows())
				.collect(),
		));
		match self.run {
			Run::Publish => {
				self.inner
					.create_table_with_outcome(
						&request.name,
						request.schema.clone(),
						CreateTableRowIdentity::AppendOnly,
						false,
					)
					.await?;
				self.published
					.lock()
					.unwrap()
					.push((request.op, request.fingerprint));
				Ok(PdCtasOutcome::Published)
			}
			Run::WinnerBinds { same_schema } => {
				let schema = if same_schema {
					request.schema.clone()
				} else {
					Arc::new(arrow::datatypes::Schema::new(vec![
						arrow::datatypes::Field::new(
							"other",
							arrow::datatypes::DataType::Utf8,
							true,
						),
					]))
				};
				self.inner
					.create_table_with_outcome(
						&request.name,
						schema,
						CreateTableRowIdentity::AppendOnly,
						false,
					)
					.await?;
				Ok(PdCtasOutcome::NameBound)
			}
		}
	}

	async fn drop_table(
		&self,
		name: &TableRef,
		if_exists: bool,
	) -> Result<Option<TableBinding>, CatalogError> {
		self.inner.drop_table(name, if_exists).await
	}

	async fn complete_table_drop(&self, binding: &TableBinding) -> Result<(), CatalogError> {
		self.inner.complete_table_drop(binding).await
	}
}

/// A coordinator over a `PdCatalog`, with `people(id INT)` holding `ids`.
async fn fixture(
	authority: Authority,
	run: Run,
	ids: &[i32],
) -> (Arc<PdCatalog>, QueryCoordinator, SqlPlanner) {
	fixture_at(authority, run, ids, None).await
}

/// [`fixture`], its statements under the caller deadline `deadline`.
async fn fixture_at(
	authority: Authority,
	run: Run,
	ids: &[i32],
	deadline: Option<std::time::Instant>,
) -> (Arc<PdCatalog>, QueryCoordinator, SqlPlanner) {
	let inner = Arc::new(MockCatalog::new());
	let storage = Arc::new(InMemoryTableStorage::new());
	let planner = SqlPlanner::new();
	let setup = QueryCoordinator::with_writer(
		inner.clone(),
		Executor::new(storage.clone()),
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(storage.clone());
	let values = ids
		.iter()
		.map(|id| format!("({id})"))
		.collect::<Vec<_>>()
		.join(", ");
	for sql in [
		"CREATE TABLE people (id INT NOT NULL)".to_string(),
		format!("INSERT INTO people (id) VALUES {values}"),
	] {
		setup
			.execute_query(&planner.plan(&sql).expect("plan setup"), None)
			.await
			.expect("setup");
	}
	let catalog = Arc::new(PdCatalog::new(inner, authority, run));
	let coordinator = QueryCoordinator::with_writer(
		catalog.clone(),
		match deadline {
			None => Executor::new(storage.clone()),
			Some(deadline) => {
				let mut context = toron::exec::ExecutionContext::local_test();
				context.deadline = Some(deadline);
				Executor::with_context(storage.clone(), context)
			}
		},
		PlannerConfig::default(),
		storage.clone(),
	)
	.with_storage_admin(storage);
	(catalog, coordinator, planner)
}

/// The operation is resolved before the SELECT. A retry of the same statement
/// under the same operation id, after its source changed, adopts the published
/// outcome and never runs again (a changed source cannot redeclare it); another
/// statement under that id is refused before anything runs.
#[tokio::test]
async fn a_declared_operation_is_adopted_and_never_rerun_after_its_source_changes() {
	let (catalog, coordinator, planner) =
		fixture(Authority::ParentDecisions, Run::Publish, &[1, 1, 2]).await;
	let statement = planner
		.plan("CREATE TABLE copy AS SELECT id FROM people")
		.expect("plan CTAS");
	let query_id = QueryId::new();
	coordinator
		.execute_mutation_with_query_id(&statement, query_id)
		.await
		.expect("the first run publishes");
	assert_eq!(catalog.runs().len(), 1);
	assert_eq!(
		catalog.runs()[0].1,
		vec![3],
		"one chunk of the source's three rows"
	);

	// The source changes; the same statement retries under the same id.
	coordinator
		.execute_query(
			&planner
				.plan("INSERT INTO people (id) VALUES (9), (9)")
				.expect("plan insert"),
			None,
		)
		.await
		.expect("change the source");
	coordinator
		.execute_mutation_with_query_id(&statement, query_id)
		.await
		.expect("the retry adopts the publication");
	assert_eq!(catalog.runs().len(), 1, "the retry ran no chain");

	// Another statement under the same id: refused before anything runs.
	let other = planner
		.plan("CREATE TABLE copy_two AS SELECT id FROM people")
		.expect("plan another CTAS");
	let error = coordinator
		.execute_mutation_with_query_id(&other, query_id)
		.await
		.expect_err("a different statement under a declared id");
	assert!(
		error
			.to_string()
			.contains("reused for a different CTAS intent"),
		"{error}"
	);
	assert_eq!(catalog.runs().len(), 1);
	assert!(
		catalog
			.resolve_table(&TableRef::new("copy_two"))
			.await
			.is_err(),
		"no table for the refused statement"
	);
}

/// Only the definitive pre-proposal wrong-mode answer selects the decision
/// chain: any other failure of the Today resolve fails the statement, with no
/// ParentDecisions resolve and no chain.
#[tokio::test]
async fn only_the_definitive_wrong_mode_answer_selects_the_decision_chain() {
	let (catalog, coordinator, planner) = fixture(Authority::Unavailable, Run::Publish, &[1]).await;
	let error = coordinator
		.execute_mutation_with_query_id(
			&planner
				.plan("CREATE TABLE copy AS SELECT id FROM people")
				.expect("plan CTAS"),
			QueryId::new(),
		)
		.await
		.expect_err("an unavailable authority");
	assert!(error.to_string().contains("connection reset"), "{error}");
	assert_eq!((catalog.resolves(), catalog.runs().len()), (0, 0));
}

/// OR REPLACE is refused under ParentDecisions before any chain or SELECT.
#[tokio::test]
async fn or_replace_is_refused_before_any_effect() {
	let (catalog, coordinator, planner) =
		fixture(Authority::ParentDecisions, Run::Publish, &[1]).await;
	let error = coordinator
		.execute_mutation_with_query_id(
			&planner
				.plan("CREATE OR REPLACE TABLE copy AS SELECT id FROM people")
				.expect("plan CTAS"),
			QueryId::new(),
		)
		.await
		.expect_err("OR REPLACE");
	assert!(
		error.to_string().contains("refused under ParentDecisions"),
		"{error}"
	);
	assert_eq!((catalog.resolves(), catalog.runs().len()), (0, 0));
}

/// A concurrent statement binds the target name first. IF NOT EXISTS adopts a
/// same-schema winner; a plain CTAS is refused; a different-schema winner is a
/// schema mismatch even under IF NOT EXISTS.
#[tokio::test]
async fn a_concurrent_binding_winner_is_judged_as_if_not_exists_requires() {
	for (sql, same_schema, expect) in [
		(
			"CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people",
			true,
			Ok(()),
		),
		(
			"CREATE TABLE copy AS SELECT id FROM people",
			true,
			Err("table already exists"),
		),
		(
			"CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people",
			false,
			Err("does not match"),
		),
	] {
		let (catalog, coordinator, planner) = fixture(
			Authority::ParentDecisions,
			Run::WinnerBinds { same_schema },
			&[1, 2],
		)
		.await;
		let result = coordinator
			.execute_mutation_with_query_id(&planner.plan(sql).expect("plan CTAS"), QueryId::new())
			.await;
		match (result, expect) {
			(Ok(_), Ok(())) => {}
			(Err(error), Err(expected)) => {
				assert!(error.to_string().contains(expected), "{sql}: {error}")
			}
			(result, expected) => panic!("{sql}: {result:?}, expected {expected:?}"),
		}
		assert_eq!(catalog.runs().len(), 1, "{sql}: one chain");
	}
}

/// A fresh, bound same-schema IF NOT EXISTS succeeds before any chain runs; an
/// empty result runs a chain of no chunks.
#[tokio::test]
async fn if_not_exists_over_a_bound_name_runs_nothing_and_an_empty_result_declares_no_chunk() {
	let (catalog, coordinator, planner) =
		fixture(Authority::ParentDecisions, Run::Publish, &[1]).await;
	coordinator
		.execute_mutation_with_query_id(
			&planner
				.plan("CREATE TABLE IF NOT EXISTS people AS SELECT id FROM people")
				.expect("plan CTAS"),
			QueryId::new(),
		)
		.await
		.expect("a bound same-schema name");
	assert_eq!(catalog.runs().len(), 0);
	coordinator
		.execute_mutation_with_query_id(
			&planner
				.plan("CREATE TABLE empty_copy AS SELECT id FROM people WHERE id < 0")
				.expect("plan CTAS"),
			QueryId::new(),
		)
		.await
		.expect("an empty CTAS");
	assert_eq!(catalog.runs()[0].1, Vec::<usize>::new(), "no chunk");
}

/// The fingerprint `sql` declares, learned from one publication.
async fn fingerprint_of(sql: &str) -> [u8; 32] {
	let (catalog, coordinator, planner) =
		fixture(Authority::ParentDecisions, Run::Publish, &[1, 2]).await;
	coordinator
		.execute_mutation_with_query_id(&planner.plan(sql).expect("plan CTAS"), QueryId::new())
		.await
		.expect("learn the fingerprint");
	catalog.published_fingerprint()
}

/// Codex 01a1fb6a §1: a run of the same operation publishes the target after
/// this run resolved the operation absent and before its bound-name check. The
/// binding is then resolved as this operation's own: the same statement adopts
/// the publication (no SELECT, no chain); a different statement under the id is
/// refused even under IF NOT EXISTS with a matching schema.
#[tokio::test]
async fn a_same_operation_publication_racing_the_bound_name_check_is_resolved_as_the_operation() {
	let plain = "CREATE TABLE copy AS SELECT id FROM people";
	let guarded = "CREATE TABLE IF NOT EXISTS copy AS SELECT id FROM people";
	for (sql, race_fingerprint, expect) in [
		(plain, fingerprint_of(plain).await, Ok(())),
		(
			guarded,
			[0xAB; 32],
			Err("reused for a different CTAS intent"),
		),
	] {
		let (catalog, coordinator, planner) =
			fixture(Authority::ParentDecisions, Run::Publish, &[1, 2]).await;
		*catalog.race.lock().unwrap() = Some((TableRef::new("copy"), race_fingerprint));
		let result = coordinator
			.execute_mutation_with_query_id(&planner.plan(sql).expect("plan CTAS"), QueryId::new())
			.await;
		match (result, expect) {
			(Ok(_), Ok(())) => {}
			(Err(error), Err(expected)) => {
				assert!(error.to_string().contains(expected), "{sql}: {error}")
			}
			(result, expected) => panic!("{sql}: {result:?}, expected {expected:?}"),
		}
		assert!(
			catalog.race.lock().unwrap().is_none(),
			"{sql}: the race ran"
		);
		assert_eq!(catalog.runs().len(), 0, "{sql}: no SELECT-and-chain run");
		assert_eq!(
			catalog.resolves(),
			2,
			"{sql}: resolved again after the binding"
		);
	}
}

/// Codex 01a1fb6a §2: a caller deadline that has passed when the SELECT ends
/// fails the statement before any Begin.
#[tokio::test]
async fn a_deadline_passed_after_the_select_begins_nothing() {
	let (catalog, late, planner) = fixture_at(
		Authority::ParentDecisions,
		Run::Publish,
		&[1, 2],
		Some(std::time::Instant::now() + std::time::Duration::from_millis(300)),
	)
	.await;
	tokio::time::sleep(std::time::Duration::from_millis(400)).await;
	let error = late
		.execute_mutation_with_query_id(
			&planner
				.plan("CREATE TABLE copy AS SELECT id FROM people")
				.expect("plan CTAS"),
			QueryId::new(),
		)
		.await
		.expect_err("a passed deadline");
	assert!(
		error.to_string().contains("deadline elapsed before Begin"),
		"{error}"
	);
	assert_eq!(catalog.runs().len(), 0, "no Begin");
}
