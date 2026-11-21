//! Wendelon ADR 0120 slice 1: the decision-state core's controls, through the
//! applied Raft requests (`decide`), on the decision fixture's three-voter state.

use super::*;
use crate::consensus::decisions::tests as fixture;
use crate::consensus::decisions::{DecisionCommand, DecisionResult};
use crate::consensus::{WendelonRequest, WendelonResponse};
use fixture::{T0, TENANT};

const SECRET: [u8; 32] = [5; 32];
const OTHER_SECRET: [u8; 32] = [6; 32];
const D0: [u8; 32] = [0xd0; 32];
const D1: [u8; 32] = [0xd1; 32];
/// The fixture's two-child claims hold 2 rows per child.
const CHUNK_ROWS: u32 = 4;
const BASE_SEQ: u64 = 0x4354_4153_0000_0001;

fn op(n: u8) -> [u8; 16] {
	fixture::uuid_v7(T0, n)
}

fn object(n: u8) -> TableObjectId {
	TableObjectId::new([0xc0 | n; 16])
}

fn descriptor_of(
	object: TableObjectId,
	name: &str,
	v: arrow::datatypes::DataType,
) -> TableSchemaDescriptor {
	use arrow::datatypes::{DataType, Field, Schema};
	TableSchemaDescriptor::new_with_layout(
		object,
		1,
		"analytics",
		name,
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("v", v, true),
		]),
		crate::schema::RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::default_table_spread(),
		crate::schema::RecordLayoutPolicy::MultiRow(
			crate::schema::MultiRowLayoutDescriptor::default(),
		),
		crate::schema::SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn descriptor(object: TableObjectId) -> TableSchemaDescriptor {
	descriptor_of(object, "copy", arrow::datatypes::DataType::Utf8)
}

fn decl(digests: &[[u8; 32]]) -> CtasDeclarationBytes {
	encode_declaration(
		&digests
			.iter()
			.enumerate()
			.map(|(at, digest)| ChunkDecl {
				sequence_no: BASE_SEQ + at as u64,
				rows: CHUNK_ROWS,
				request_digest: *digest,
			})
			.collect::<Vec<_>>(),
	)
	.unwrap()
}

/// The winning owner's commitment for `secret`, as owner commands name it.
fn owner(secret: [u8; 32]) -> [u8; 32] {
	*blake3::hash(&secret).as_bytes()
}

fn begin_with(
	op: [u8; 16],
	descriptor: TableSchemaDescriptor,
	fingerprint: u8,
	declaration: CtasDeclarationBytes,
	secret: [u8; 32],
	stamp_ms: u64,
) -> DecisionCommand {
	DecisionCommand::CtasBegin {
		begin: CtasBegin {
			tenant: TENANT,
			op,
			fingerprint: [fingerprint; 32],
			descriptor,
			declaration,
			dispatch_hash: *blake3::hash(&secret).as_bytes(),
			stamp_ms,
		},
	}
}

fn begin(op: [u8; 16], object: TableObjectId, digests: &[[u8; 32]]) -> DecisionCommand {
	begin_with(op, descriptor(object), 1, decl(digests), SECRET, T0)
}

fn granted() -> SmState {
	let mut state = fixture::base(3);
	fixture::grant_all(&mut state);
	state
}

/// A chain over `object` begun under `op`.
fn begun(digests: &[[u8; 32]]) -> SmState {
	let mut state = granted();
	assert!(matches!(
		fixture::decide(&mut state, begin(op(1), object(1), digests)),
		DecisionResult::CtasBegun {
			replayed: false,
			..
		}
	));
	state
}

fn chunk(state: &SmState, op: [u8; 16], object: TableObjectId, at: u64, digest: [u8; 32]) -> Claim {
	let mut claim = fixture::claim(
		ParentKey {
			tenant: TENANT,
			namespace: ParentNamespace::Table(object),
			client_uuid: op,
			sequence_no: BASE_SEQ + at,
		},
		fixture::children(state, 2),
		0,
	);
	claim.request_digest = digest;
	claim
}

fn reserve(state: &mut SmState, claim: &Claim) -> DecisionResult {
	fixture::reserve(state, claim, T0)
}

fn reserved(state: &mut SmState, claim: &Claim) -> u64 {
	match reserve(state, claim) {
		DecisionResult::Reserved { reservation } => reservation,
		other => panic!("reserve: {other:?}"),
	}
}

fn accept(state: &mut SmState, claim: &Claim) {
	let reservation = reserved(state, claim);
	let acks = fixture::quorum_acks(claim, reservation);
	assert!(matches!(
		fixture::finalize(state, claim, acks),
		DecisionResult::Terminal { .. }
	));
}

fn chain(state: &SmState) -> CtasChain {
	state
		.decisions
		.as_ref()
		.unwrap()
		.ctas()
		.chain(TENANT, op(1))
		.unwrap()
		.clone()
}

fn publish(state: &mut SmState, epoch: u64, secret: [u8; 32]) -> DecisionResult {
	fixture::decide(
		state,
		DecisionCommand::CtasPublish {
			tenant: TENANT,
			op: op(1),
			epoch,
			owner: owner(secret),
			stamp_ms: T0,
		},
	)
}

fn recover(state: &mut SmState, stamp_ms: u64) -> DecisionResult {
	fixture::decide(
		state,
		DecisionCommand::CtasRecover {
			tenant: TENANT,
			op: op(1),
			new_epoch: 2,
			stamp_ms,
		},
	)
}

fn conclude(state: &mut SmState) -> DecisionResult {
	fixture::decide(
		state,
		DecisionCommand::CtasConclude {
			tenant: TENANT,
			op: op(1),
			epoch: 2,
			stamp_ms: T0,
		},
	)
}

fn abandon(state: &mut SmState, secret: [u8; 32]) -> DecisionResult {
	fixture::decide(
		state,
		DecisionCommand::CtasAbandon {
			tenant: TENANT,
			op: op(1),
			epoch: 1,
			owner: owner(secret),
			cause: CtasAbandonCause::Deadline,
			stamp_ms: T0,
		},
	)
}

fn refused(refusal: CtasRefusal) -> DecisionResult {
	DecisionResult::Refused(DecisionRefusal::Ctas(refusal))
}

const LAPSED: u64 = T0 + CTAS_OWNER_LEASE_MS;

/// ADR 0120 D2: the advertised count is bounded and the length checked against
/// it before any vector is built.
#[test]
fn a_declaration_decodes_only_within_its_bounds() {
	let entry = |at: u64| ChunkDecl {
		sequence_no: BASE_SEQ + at,
		rows: 1,
		request_digest: [at as u8; 32],
	};
	let encoded = |chunks: &[ChunkDecl]| encode_declaration(chunks).unwrap().as_slice().to_vec();
	assert_eq!(decode_declaration(&encoded(&[])), Ok(Vec::new()));
	let full: Vec<ChunkDecl> = (0..128).map(entry).collect();
	assert_eq!(decode_declaration(&encoded(&full)), Ok(full.clone()));
	// 129 chunks: the encoder refuses (never truncates the count), and 129
	// advertised with exactly its length is refused on the count.
	let over: Vec<ChunkDecl> = (0..129).map(entry).collect();
	assert_eq!(
		encode_declaration(&over),
		Err(DeclarationError::TooManyChunks)
	);
	let mut exact = 129u16.to_be_bytes().to_vec();
	exact.extend(std::iter::repeat_n(0u8, 129 * CTAS_CHUNK_ENTRY_BYTES));
	assert_eq!(
		decode_declaration(&exact),
		Err(DeclarationError::TooManyChunks)
	);
	// 129, or the largest count, advertised over a few bytes: refused on the count.
	assert_eq!(
		decode_declaration(&[0, 129, 0, 0]),
		Err(DeclarationError::TooManyChunks)
	);
	assert_eq!(
		decode_declaration(&[0xff, 0xff]),
		Err(DeclarationError::TooManyChunks)
	);
	assert_eq!(decode_declaration(&[1]), Err(DeclarationError::Short));
	let mut two = encoded(&full[..2]);
	two.truncate(two.len() - 1);
	assert_eq!(
		decode_declaration(&two),
		Err(DeclarationError::LengthMismatch)
	);
	let mut long = encoded(&full[..2]);
	long.push(0);
	assert_eq!(
		decode_declaration(&long),
		Err(DeclarationError::LengthMismatch)
	);
	assert_eq!(encoded(&full[..1]).len(), 2 + CTAS_CHUNK_ENTRY_BYTES);
}

/// ADR 0120 D3: the first Begin chooses the declaration, object and owner; a
/// same-fingerprint retry answers that chain unchanged, a different one refuses.
#[test]
fn the_first_begin_owns_the_operation_and_a_retry_replays_it() {
	let mut state = begun(&[D0]);
	let first = chain(&state);
	assert!(state.table_schemas.is_provisional(TENANT, object(1)));
	assert_eq!(first.phase, CtasPhase::Dispatching);
	assert_eq!(first.epoch, 1);
	// The same statement retried: another object, other rows, another secret.
	let retry = begin_with(
		op(1),
		descriptor(object(2)),
		1,
		decl(&[D1]),
		OTHER_SECRET,
		T0 + 30_000,
	);
	assert_eq!(
		fixture::decide(&mut state, retry),
		DecisionResult::CtasBegun {
			tenant: TENANT,
			object: object(1),
			replayed: true
		}
	);
	assert_eq!(
		chain(&state),
		first,
		"the winner is neither renewed nor replaced"
	);
	assert!(!state.table_schemas.is_provisional(TENANT, object(2)));
	let other = begin_with(op(1), descriptor(object(3)), 2, decl(&[D0]), SECRET, T0);
	assert_eq!(
		fixture::decide(&mut state, other),
		refused(CtasRefusal::FingerprintMismatch)
	);
	assert_eq!(chain(&state), first);
}

/// ADR 0120 D3: Begin proves exclusive ownership of its object: not a live
/// table, not another chain's (even with the identical descriptor), not an
/// abandoned id; and ordinary Create refuses a chain's ids too.
#[test]
fn begin_proves_its_object_exclusive_and_the_owner_survives() {
	let mut state = begun(&[D0]);
	let first = chain(&state);
	// A live table's id.
	let live = begin_with(
		op(2),
		descriptor(fixture::TABLE),
		1,
		decl(&[D0]),
		SECRET,
		T0,
	);
	assert_eq!(
		fixture::decide(&mut state, live),
		refused(CtasRefusal::ObjectNotExclusive)
	);
	// Another chain presenting the owned object's identical descriptor.
	let twin = begin_with(
		op(2),
		descriptor(object(1)),
		1,
		decl(&[D0]),
		OTHER_SECRET,
		T0,
	);
	assert_eq!(
		fixture::decide(&mut state, twin),
		refused(CtasRefusal::ObjectNotExclusive)
	);
	assert_eq!(chain(&state), first, "the existing owner survives");
	assert!(state.table_schemas.is_provisional(TENANT, object(1)));
	assert!(
		state
			.table_schemas
			.latest_by_name(TENANT, "analytics", "copy")
			.is_none(),
		"and stays invisible"
	);
	// Ordinary Create of the owned id.
	assert!(
		create(
			&mut state,
			descriptor_of(object(1), "plain", arrow::datatypes::DataType::Utf8)
		)
		.is_err()
	);
	// Abandoned: the id is retired for good.
	assert!(matches!(
		abandon(&mut state, SECRET),
		DecisionResult::CtasAbandoned {
			replayed: false,
			..
		}
	));
	let again = begin_with(op(3), descriptor(object(1)), 1, decl(&[D0]), SECRET, T0);
	assert_eq!(
		fixture::decide(&mut state, again),
		refused(CtasRefusal::ObjectNotExclusive)
	);
	assert!(
		create(
			&mut state,
			descriptor_of(object(1), "plain", arrow::datatypes::DataType::Utf8)
		)
		.is_err()
	);
	// The retired record alone refuses: with the retained descriptor gone (as a
	// later retirement unit would remove it), Create and Begin still refuse the id.
	state
		.table_schemas
		.retire_unbound_object(TENANT, object(1))
		.unwrap();
	assert!(
		state
			.table_schemas
			.latest_by_id_including_provisional(TENANT, object(1))
			.is_none()
	);
	let refusal = create(
		&mut state,
		descriptor_of(object(1), "plain", arrow::datatypes::DataType::Utf8),
	)
	.unwrap_err();
	assert!(
		refusal.contains(crate::consensus::raft_storage::PD_CTAS_TABLE_ID_REUSE_REFUSED),
		"{refusal}"
	);
	let again = begin_with(op(4), descriptor(object(1)), 1, decl(&[D0]), SECRET, T0);
	assert_eq!(
		fixture::decide(&mut state, again),
		refused(CtasRefusal::ObjectNotExclusive)
	);
}

/// An ordinary Create of `descriptor` under the fixture tenant.
fn create(state: &mut SmState, descriptor: TableSchemaDescriptor) -> Result<(), String> {
	use crate::integration::compute::*;
	let envelope = encode_schema_update_envelope_v1(&SchemaUpdateEnvelope {
		contract_version: COMPUTE_STORAGE_CONTRACT_VERSION,
		origin: SchemaIntentOrigin::ComputeClient,
		tenant_id: TENANT,
		object: SchemaObjectRef {
			kind: SchemaObjectKind::Table,
			namespace: descriptor.namespace.clone(),
			name: descriptor.table_name.clone(),
		},
		expected_generation: SchemaGeneration::ZERO,
		requested_generation: SchemaGeneration::new(descriptor.schema_generation),
		intent_id: SchemaIntentId::new(*descriptor.table_object_id.as_bytes()),
		mutation: SchemaMutationKind::Create,
		payload: crate::schema::table::encode_table_schema_descriptor_v1(&descriptor).unwrap(),
	})
	.unwrap();
	match fixture::apply_next(state, WendelonRequest::SchemaUpdate(envelope)) {
		WendelonResponse::Success => Ok(()),
		other => Err(format!("{other:?}")),
	}
}

/// ADR 0120 D3: Begin's other guards, each refusing with nothing recorded.
#[test]
fn begin_refuses_outside_its_window_type_bounds_hold_and_capacity() {
	let mut state = granted();
	let expired = begin_with(
		op(1),
		descriptor(object(1)),
		1,
		decl(&[D0]),
		SECRET,
		T0 + CTAS_OPERATION_WINDOW_MS + 1,
	);
	assert_eq!(
		fixture::decide(&mut state, expired),
		refused(CtasRefusal::OperationExpired)
	);
	let future = begin_with(
		op(1),
		descriptor(object(1)),
		1,
		decl(&[D0]),
		SECRET,
		T0 - 3_600_000,
	);
	assert_eq!(
		fixture::decide(&mut state, future),
		refused(CtasRefusal::OperationExpired)
	);
	let float16 = begin_with(
		op(1),
		descriptor_of(object(1), "copy", arrow::datatypes::DataType::Float16),
		1,
		decl(&[D0]),
		SECRET,
		T0,
	);
	assert_eq!(
		fixture::decide(&mut state, float16),
		refused(CtasRefusal::Descriptor)
	);
	let zero = encode_declaration(&[ChunkDecl {
		sequence_no: BASE_SEQ,
		rows: 0,
		request_digest: D0,
	}])
	.unwrap();
	let zero = begin_with(op(1), descriptor(object(1)), 1, zero, SECRET, T0);
	assert_eq!(
		fixture::decide(&mut state, zero),
		refused(CtasRefusal::Declaration(DeclarationError::RowsOutOfRange))
	);
	let gap = encode_declaration(&[
		ChunkDecl {
			sequence_no: BASE_SEQ,
			rows: 1,
			request_digest: D0,
		},
		ChunkDecl {
			sequence_no: BASE_SEQ + 2,
			rows: 1,
			request_digest: D1,
		},
	])
	.unwrap();
	let gap = begin_with(op(1), descriptor(object(1)), 1, gap, SECRET, T0);
	assert_eq!(
		fixture::decide(&mut state, gap),
		refused(CtasRefusal::Declaration(DeclarationError::NotConsecutive))
	);
	assert!(matches!(
		fixture::decide(&mut state, DecisionCommand::BeginBackupHold { id: 9 }),
		DecisionResult::HoldEntered { .. }
	));
	assert_eq!(
		fixture::decide(&mut state, begin(op(1), object(1), &[D0])),
		DecisionResult::Refused(DecisionRefusal::BackupHold)
	);
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.ctas()
			.chain(TENANT, op(1))
			.is_none()
	);
	assert!(!state.table_schemas.is_provisional(TENANT, object(1)));
}

#[test]
fn begin_refuses_beyond_the_active_chain_capacity() {
	let mut state = granted();
	for n in 0..CTAS_MAX_ACTIVE_CHAINS as u8 {
		let command = begin_with(
			fixture::uuid_v7(T0, 100 + n),
			descriptor(TableObjectId::new([0x40 + n; 16])),
			1,
			decl(&[D0]),
			SECRET,
			T0,
		);
		assert!(matches!(
			fixture::decide(&mut state, command),
			DecisionResult::CtasBegun {
				replayed: false,
				..
			}
		));
	}
	assert_eq!(
		fixture::decide(&mut state, begin(op(1), object(1), &[D0])),
		refused(CtasRefusal::Capacity)
	);
}

/// ADR 0120 D4, D5: only the declared chunk, with its digest and rows, is
/// reserved while the chain dispatches; a refusal before Reserve sets no
/// evidence; the deciding transition sets exactly one bit.
#[test]
fn a_chunk_is_admitted_only_as_declared_and_its_decision_is_its_evidence() {
	let mut state = begun(&[D0, D1]);
	let wrong_digest = chunk(&state, op(1), object(1), 0, [0xee; 32]);
	assert_eq!(
		reserve(&mut state, &wrong_digest),
		refused(CtasRefusal::ChunkUndeclared)
	);
	let mut wrong_rows = chunk(&state, op(1), object(1), 0, D0);
	wrong_rows.children.truncate(1);
	wrong_rows.coordinator = 0;
	assert_eq!(
		reserve(&mut state, &wrong_rows),
		refused(CtasRefusal::ChunkUndeclared)
	);
	let other_op = chunk(&state, op(9), object(1), 0, D0);
	assert_eq!(
		reserve(&mut state, &other_op),
		refused(CtasRefusal::ChunkUndeclared)
	);
	let undeclared = chunk(&state, op(1), object(1), 2, D0);
	assert_eq!(
		reserve(&mut state, &undeclared),
		refused(CtasRefusal::ChunkUndeclared)
	);
	assert_eq!(
		(chain(&state).accepted, chain(&state).refused),
		(0, 0),
		"no poisoning"
	);
	// The right chunk 0 is accepted: bit 0.
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert_eq!((chain(&state).accepted, chain(&state).refused), (0b01, 0));
	// Chunk 1 aborted: bit 1 refused; the same answer replays without a second bit.
	let c1 = chunk(&state, op(1), object(1), 1, D1);
	let rsv = reserved(&mut state, &c1);
	assert!(matches!(
		fixture::decide(&mut state, fixture::abort_command(&c1, rsv)),
		DecisionResult::Terminal { .. }
	));
	assert!(matches!(
		fixture::decide(&mut state, fixture::abort_command(&c1, rsv)),
		DecisionResult::Terminal { .. }
	));
	assert_eq!(
		(chain(&state).accepted, chain(&state).refused),
		(0b01, 0b10)
	);
	assert_eq!(
		publish(&mut state, 1, SECRET),
		refused(CtasRefusal::ChunksIncomplete)
	);
}

/// ADR 0120 D7: a first Reserve that applies before takeover is recorded and
/// waited for; recovery publishes once it is accepted.
#[test]
fn a_reserve_before_takeover_is_waited_for_then_published() {
	let mut state = begun(&[D0, D1]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	let c1 = chunk(&state, op(1), object(1), 1, D1);
	let rsv = reserved(&mut state, &c1);
	assert_eq!(
		recover(&mut state, LAPSED - 1),
		refused(CtasRefusal::LeaseLive)
	);
	assert_eq!(
		recover(&mut state, LAPSED),
		DecisionResult::CtasRecovering { epoch: 2 }
	);
	assert_eq!(conclude(&mut state), refused(CtasRefusal::StillReserved));
	// The recorded parent still finishes in Recovering, and replays.
	assert_eq!(
		reserve(&mut state, &c1),
		DecisionResult::Reserved { reservation: rsv }
	);
	let acks = fixture::quorum_acks(&c1, rsv);
	assert!(matches!(
		fixture::finalize(&mut state, &c1, acks),
		DecisionResult::Terminal { .. }
	));
	let DecisionResult::CtasPublished {
		object: published,
		visible_from,
		replayed: false,
		..
	} = conclude(&mut state)
	else {
		panic!("published");
	};
	assert_eq!(published, object(1));
	assert!(
		state
			.table_schemas
			.latest_by_name(TENANT, "analytics", "copy")
			.is_some()
	);
	assert_eq!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.ctas()
			.visible_from(TENANT, object(1)),
		Some(visible_from)
	);
}

/// ADR 0120 D7: after takeover no first Reserve is admitted, even with the
/// exact declared bytes; recovery ends the chain unpublished.
#[test]
fn a_reserve_after_takeover_is_refused_and_recovery_abandons() {
	let mut state = begun(&[D0, D1]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert_eq!(
		recover(&mut state, LAPSED),
		DecisionResult::CtasRecovering { epoch: 2 }
	);
	let c1 = chunk(&state, op(1), object(1), 1, D1);
	assert_eq!(reserve(&mut state, &c1), refused(CtasRefusal::WrongPhase));
	assert!(matches!(
		conclude(&mut state),
		DecisionResult::CtasAbandoned {
			cause: CtasAbandonCause::SourceBytesLost,
			replayed: false,
			..
		}
	));
	assert!(
		state
			.table_schemas
			.latest_by_name(TENANT, "analytics", "copy")
			.is_none()
	);
	assert!(
		state.table_schemas.is_provisional(TENANT, object(1)),
		"retained privately"
	);
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.ctas()
			.retired(TENANT, object(1))
			.is_some()
	);
	assert!(matches!(
		conclude(&mut state),
		DecisionResult::CtasAbandoned { replayed: true, .. }
	));
}

/// ADR 0120 D7: Published is the terminal success. A lost acknowledgement
/// replays it; an Abandon after it refuses; ownership and phase are checked
/// before a stale Publish could record anything.
#[test]
fn publication_is_terminal_replayable_and_owner_only() {
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert_eq!(
		publish(&mut state, 1, OTHER_SECRET),
		refused(CtasRefusal::NotOwner)
	);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished { replayed: true, .. }
	));
	assert!(matches!(
		recover(&mut state, LAPSED),
		DecisionResult::CtasPublished { replayed: true, .. }
	));
	assert_eq!(
		abandon(&mut state, SECRET),
		refused(CtasRefusal::WrongPhase)
	);
	assert!(
		state
			.table_schemas
			.latest_by_name(TENANT, "analytics", "copy")
			.is_some()
	);
	assert_eq!(
		state.decisions.as_ref().unwrap().ctas().retained(),
		(0, 0),
		"publication releases the retained slot"
	);
}

/// ADR 0120 D7: a stale owner (after takeover) cannot publish; a non-owner
/// cannot record a conflict; a bound name ends the owner's Publish in Conflict.
#[test]
fn a_stale_or_foreign_publish_records_nothing_and_a_bound_name_conflicts() {
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	// The name is bound by an ordinary table meanwhile.
	create(
		&mut state,
		descriptor_of(object(7), "copy", arrow::datatypes::DataType::Utf8),
	)
	.unwrap();
	assert_eq!(
		publish(&mut state, 1, OTHER_SECRET),
		refused(CtasRefusal::NotOwner)
	);
	assert_eq!(
		chain(&state).phase,
		CtasPhase::Dispatching,
		"nothing recorded"
	);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasConflict {
			replayed: false,
			..
		}
	));
	assert_eq!(
		state
			.table_schemas
			.latest_by_name(TENANT, "analytics", "copy")
			.map(|d| d.table_object_id),
		Some(object(7))
	);
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.ctas()
			.retired(TENANT, object(1))
			.is_some()
	);

	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert_eq!(
		recover(&mut state, LAPSED),
		DecisionResult::CtasRecovering { epoch: 2 }
	);
	assert_eq!(
		publish(&mut state, 1, SECRET),
		refused(CtasRefusal::StaleEpoch)
	);
	assert!(matches!(
		conclude(&mut state),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
}

/// ADR 0120 D2, D7: an explicit empty declaration publishes an empty table.
#[test]
fn an_empty_declaration_publishes_an_empty_table() {
	let mut state = begun(&[]);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	assert!(
		state
			.table_schemas
			.latest_by_name(TENANT, "analytics", "copy")
			.is_some()
	);
}

/// ADR 0120 D7: interrupted publication: every chunk accepted, the owner gone
/// before Publish; recovery publishes the original rows.
#[test]
fn an_interrupted_publication_is_completed_by_recovery() {
	let mut state = begun(&[D0, D1]);
	for (at, digest) in [(0, D0), (1, D1)] {
		let claim = chunk(&state, op(1), object(1), at, digest);
		accept(&mut state, &claim);
	}
	assert_eq!(
		recover(&mut state, LAPSED),
		DecisionResult::CtasRecovering { epoch: 2 }
	);
	assert!(matches!(
		conclude(&mut state),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
}

/// ADR 0120 D7: the owner's Abandon concludes a still-Reserved chunk as
/// `Refused { CtasAbandoned }` in the same apply, retains the descriptor under
/// budget and retires the id.
#[test]
fn abandonment_closes_reserved_chunks_and_keeps_the_descriptor_charged() {
	let mut state = begun(&[D0, D1]);
	let charged = state.decisions.as_ref().unwrap().ctas().retained();
	assert_eq!(charged.0, 1);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	let rsv = reserved(&mut state, &c0);
	assert_eq!(
		abandon(&mut state, OTHER_SECRET),
		refused(CtasRefusal::NotOwner)
	);
	assert!(matches!(
		abandon(&mut state, SECRET),
		DecisionResult::CtasAbandoned {
			replayed: false,
			..
		}
	));
	let decisions = state.decisions.as_ref().unwrap();
	assert!(matches!(
		decisions.bodies[&rsv].status,
		ParentStatus::Refused {
			cause: RefusalCause::CtasAbandoned,
			..
		}
	));
	assert_eq!(chain(&state).refused, 0b01);
	assert_eq!(
		decisions.ctas().retained(),
		charged,
		"kept through abandonment"
	);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::Refused(_)
	));
}

#[test]
fn ctas_commands_refuse_on_a_today_store() {
	let mut state = granted();
	state.decisions = None;
	assert_eq!(
		fixture::decide(&mut state, begin(op(1), object(1), &[D0])),
		DecisionResult::Refused(DecisionRefusal::ModeToday)
	);
	assert!(!state.table_schemas.is_provisional(TENANT, object(1)));
}

/// A fresh ordinary write (another operation id) on `object`.
/// [`fresh_write`] with its operation issued at `ms`.
fn fresh_write_at(state: &SmState, object: TableObjectId, n: u8, ms: u64) -> Claim {
	fixture::claim(
		ParentKey {
			tenant: TENANT,
			namespace: ParentNamespace::Table(object),
			client_uuid: fixture::uuid_v7(ms, n),
			sequence_no: 1,
		},
		fixture::children(state, 2),
		0,
	)
}

fn fresh_write(state: &SmState, object: TableObjectId, n: u8) -> Claim {
	fixture::claim(
		ParentKey {
			tenant: TENANT,
			namespace: ParentNamespace::Table(object),
			client_uuid: fixture::uuid_v7(T0, n),
			sequence_no: 1,
		},
		fixture::children(state, 2),
		0,
	)
}

/// Codex 01a1fa67 §1: a successful Publish (by the owner, and by recovery's
/// Conclude) ends provisional routing in the same apply, so an ordinary signed
/// Reserve with a fresh id on the published table is admitted at once; a fresh
/// write on an abandoned or conflicted object still refuses.
#[test]
fn a_published_table_admits_ordinary_writes_at_once() {
	// The owner publishes.
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	let fresh = fresh_write(&state, object(1), 40);
	assert!(
		matches!(reserve(&mut state, &fresh), DecisionResult::Reserved { .. }),
		"an ordinary write on the owner-published table"
	);
	// Recovery publishes.
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert_eq!(
		recover(&mut state, LAPSED),
		DecisionResult::CtasRecovering { epoch: 2 }
	);
	assert!(matches!(
		conclude(&mut state),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	let fresh = fresh_write(&state, object(1), 41);
	assert!(
		matches!(reserve(&mut state, &fresh), DecisionResult::Reserved { .. }),
		"an ordinary write on the recovery-published table"
	);
	// Abandoned: a fresh write still refuses.
	let mut state = begun(&[D0]);
	assert!(matches!(
		abandon(&mut state, SECRET),
		DecisionResult::CtasAbandoned { .. }
	));
	let fresh = fresh_write(&state, object(1), 42);
	assert!(matches!(
		reserve(&mut state, &fresh),
		DecisionResult::Refused(_)
	));
	// Conflicted: a fresh write still refuses.
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	create(
		&mut state,
		descriptor_of(object(7), "copy", arrow::datatypes::DataType::Utf8),
	)
	.unwrap();
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasConflict { .. }
	));
	let fresh = fresh_write(&state, object(1), 43);
	assert!(matches!(
		reserve(&mut state, &fresh),
		DecisionResult::Refused(_)
	));
}

/// Codex 01a1fa67 §2: Begin refuses the reserved KV object id on a tenant with
/// no descriptor under it, and a fresh operation whose target name is bound;
/// neither records a chain, charges the budget or changes the catalog.
#[test]
fn begin_refuses_the_kv_id_and_a_bound_name_with_nothing_recorded() {
	let mut state = granted();
	assert!(
		state
			.table_schemas
			.latest_by_id_including_provisional(TENANT, TableObjectId::DEFAULT_KV)
			.is_none()
	);
	let kv = begin_with(
		op(1),
		descriptor(TableObjectId::DEFAULT_KV),
		1,
		decl(&[D0]),
		SECRET,
		T0,
	);
	assert_eq!(
		fixture::decide(&mut state, kv),
		refused(CtasRefusal::ObjectNotExclusive)
	);
	create(
		&mut state,
		descriptor_of(object(7), "copy", arrow::datatypes::DataType::Utf8),
	)
	.unwrap();
	let bound = begin(op(2), object(1), &[D0]);
	assert_eq!(
		fixture::decide(&mut state, bound),
		refused(CtasRefusal::NameBound)
	);
	let decisions = state.decisions.as_ref().unwrap();
	assert!(decisions.ctas().chain(TENANT, op(1)).is_none());
	assert!(decisions.ctas().chain(TENANT, op(2)).is_none());
	assert_eq!(decisions.ctas().retained(), (0, 0));
	assert!(
		!state
			.table_schemas
			.is_provisional(TENANT, TableObjectId::DEFAULT_KV)
	);
	assert!(!state.table_schemas.is_provisional(TENANT, object(1)));
}

/// Codex 01a1fa67 §3: a backup hold entered after Begin stops the binding
/// transition: neither the owner's Publish nor recovery's Conclude binds the
/// name, and phase, visibility and budget stay; after the exact-token release
/// publication succeeds. A Publish committed before the hold still replays.
#[test]
fn a_backup_hold_stops_publication_until_its_release() {
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	let DecisionResult::HoldEntered { id, entered_at } =
		fixture::decide(&mut state, DecisionCommand::BeginBackupHold { id: 9 })
	else {
		panic!("hold");
	};
	let before = chain(&state);
	let retained = state.decisions.as_ref().unwrap().ctas().retained();
	assert_eq!(
		publish(&mut state, 1, SECRET),
		DecisionResult::Refused(DecisionRefusal::BackupHold)
	);
	assert_eq!(
		recover(&mut state, LAPSED),
		DecisionResult::CtasRecovering { epoch: 2 }
	);
	assert_eq!(
		conclude(&mut state),
		DecisionResult::Refused(DecisionRefusal::BackupHold)
	);
	assert!(
		state
			.table_schemas
			.latest_by_name(TENANT, "analytics", "copy")
			.is_none()
	);
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.ctas()
			.visible_from(TENANT, object(1))
			.is_none()
	);
	assert_eq!(chain(&state).accepted, before.accepted);
	assert_eq!(chain(&state).phase, CtasPhase::Recovering);
	assert_eq!(
		state.decisions.as_ref().unwrap().ctas().retained(),
		retained
	);
	assert!(matches!(
		fixture::decide(
			&mut state,
			DecisionCommand::ReleaseBackupHold { id, entered_at }
		),
		DecisionResult::HoldReleased
	));
	assert!(matches!(
		conclude(&mut state),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	// A Publish committed before a hold replays under it.
	assert!(matches!(
		fixture::decide(&mut state, DecisionCommand::BeginBackupHold { id: 10 }),
		DecisionResult::HoldEntered { .. }
	));
	assert!(matches!(
		conclude(&mut state),
		DecisionResult::CtasPublished { replayed: true, .. }
	));
	// The owner's Publish of the committed chain replays under the hold too.
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished { replayed: true, .. }
	));
}

/// Codex 01a1fa67 §5: the command's declaration field is bounded on the real
/// command codec: the admitted maximum (128 chunks, 5,634 bytes) round-trips
/// through a Raft request; one byte more is refused by the deserializer, before
/// the inner decoder sees it.
#[test]
fn the_declaration_field_is_bounded_on_the_command_codec() {
	let full: Vec<ChunkDecl> = (0..128)
		.map(|at| ChunkDecl {
			sequence_no: BASE_SEQ + at,
			rows: 1,
			request_digest: [at as u8; 32],
		})
		.collect();
	let declaration = encode_declaration(&full).unwrap();
	assert_eq!(declaration.as_slice().len(), CTAS_DECLARATION_MAX_BYTES);
	assert_eq!(CTAS_DECLARATION_MAX_BYTES, 5_634);
	let request = |declaration: CtasDeclarationBytes| {
		WendelonRequest::Decision(begin_with(
			op(1),
			descriptor(object(1)),
			1,
			declaration,
			SECRET,
			T0,
		))
	};
	let bytes = postcard::to_stdvec(&request(declaration.clone())).unwrap();
	let decoded: WendelonRequest = postcard::from_bytes(&bytes).unwrap();
	assert_eq!(decoded, request(declaration));
	let over = CtasDeclarationBytes::unchecked_for_test(vec![0; CTAS_DECLARATION_MAX_BYTES + 1]);
	let bytes = postcard::to_stdvec(&request(over)).unwrap();
	assert!(postcard::from_bytes::<WendelonRequest>(&bytes).is_err());
}

/// Saturation of the chain records (256, terminal summaries included): each
/// chain abandoned, so the 64-active bound never binds; the 257th Begin refuses.
#[test]
fn begin_refuses_beyond_the_chain_record_capacity() {
	let mut state = granted();
	for n in 0..CTAS_CHAIN_CAPACITY as u16 {
		let id = fixture::uuid_v7(T0, 0);
		let mut op_bytes = id;
		op_bytes[14..].copy_from_slice(&(n + 1).to_be_bytes());
		let mut object_bytes = [0x90; 16];
		object_bytes[14..].copy_from_slice(&n.to_be_bytes());
		let command = begin_with(
			op_bytes,
			descriptor(TableObjectId::new(object_bytes)),
			1,
			decl(&[D0]),
			SECRET,
			T0,
		);
		assert!(
			matches!(
				fixture::decide(&mut state, command),
				DecisionResult::CtasBegun {
					replayed: false,
					..
				}
			),
			"begin {n}"
		);
		assert!(matches!(
			fixture::decide(
				&mut state,
				DecisionCommand::CtasAbandon {
					tenant: TENANT,
					op: op_bytes,
					epoch: 1,
					owner: owner(SECRET),
					cause: CtasAbandonCause::Deadline,
					stamp_ms: T0,
				}
			),
			DecisionResult::CtasAbandoned { .. }
		));
	}
	assert_eq!(
		fixture::decide(&mut state, begin(op(1), object(1), &[D0])),
		refused(CtasRefusal::Capacity)
	);
	assert_eq!(
		state.decisions.as_ref().unwrap().ctas().retained().0,
		CTAS_CHAIN_CAPACITY as u32,
		"every abandoned chain keeps its descriptor charged"
	);
}

/// A valid AppendOnly descriptor of `columns` columns with long names: a large
/// real encoding, for the retained-bytes budget.
fn wide_descriptor(object: TableObjectId, name: &str, columns: usize) -> TableSchemaDescriptor {
	use arrow::datatypes::{DataType, Field, Schema};
	let mut fields = vec![Field::new("id", DataType::Int64, false)];
	for at in 0..columns {
		fields.push(Field::new(
			format!("a_rather_long_column_name_for_the_retained_budget_{at:05}"),
			DataType::Int64,
			true,
		));
	}
	TableSchemaDescriptor::new_with_layout(
		object,
		1,
		"analytics",
		name,
		&Schema::new(fields),
		crate::schema::RowIdentityPolicy::AppendOnly,
		crate::schema::LocalityPolicy::default_table_spread(),
		crate::schema::RecordLayoutPolicy::MultiRow(
			crate::schema::MultiRowLayoutDescriptor::default(),
		),
		crate::schema::SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

/// Codex 01a1fa76: the retained-descriptor byte budget (16 MiB), with both
/// count limits slack. Real descriptors are charged their v1 encoding at Begin.
/// A fresh Begin crossing the budget refuses `Capacity` with the catalog and the
/// accounting unchanged; an admitted Publish releases its charge and the
/// refused Begin then fits; Abandon keeps its charge.
#[test]
fn the_retained_descriptor_bytes_are_bounded_and_released_only_by_publication() {
	const COLUMNS: usize = 1_000;
	let mut state = granted();
	let op_of = |n: u16| {
		let mut op = fixture::uuid_v7(T0, 0);
		op[14..].copy_from_slice(&(n + 1).to_be_bytes());
		op
	};
	let object_of = |n: u16| {
		let mut object = [0xa0; 16];
		object[14..].copy_from_slice(&n.to_be_bytes());
		TableObjectId::new(object)
	};
	let charge = crate::schema::table::encode_table_schema_descriptor_v1(&wide_descriptor(
		object_of(0),
		"wide_00000",
		COLUMNS,
	))
	.unwrap()
	.len() as u64;
	let fits = CTAS_RETAINED_DESCRIPTOR_BYTES_MAX / charge;
	assert!(
		(2..CTAS_CHAIN_CAPACITY as u64 - 2).contains(&fits),
		"premise: the budget binds before the record capacity ({fits} of {charge} B)"
	);
	let begin_wide = |state: &mut SmState, n: u16| {
		fixture::decide(
			state,
			begin_with(
				op_of(n),
				wide_descriptor(object_of(n), &format!("wide_{n:05}"), COLUMNS),
				1,
				decl(&[]),
				SECRET,
				T0,
			),
		)
	};
	// Chain 0 stays open and publishable (an empty declaration); the others are
	// abandoned, so at most two are active at once.
	assert!(matches!(
		begin_wide(&mut state, 0),
		DecisionResult::CtasBegun {
			replayed: false,
			..
		}
	));
	for n in 1..fits as u16 {
		assert!(
			matches!(
				begin_wide(&mut state, n),
				DecisionResult::CtasBegun {
					replayed: false,
					..
				}
			),
			"begin {n}"
		);
		assert!(matches!(
			fixture::decide(
				&mut state,
				DecisionCommand::CtasAbandon {
					tenant: TENANT,
					op: op_of(n),
					epoch: 1,
					owner: owner(SECRET),
					cause: CtasAbandonCause::Deadline,
					stamp_ms: T0,
				}
			),
			DecisionResult::CtasAbandoned { .. }
		));
	}
	let ctas = |state: &SmState| state.decisions.as_ref().unwrap().ctas().retained();
	assert_eq!(
		ctas(&state),
		(fits as u32, fits * charge),
		"abandoned chains keep their charge"
	);
	// The next one crosses the budget.
	let crossing = fits as u16;
	assert_eq!(
		begin_wide(&mut state, crossing),
		refused(CtasRefusal::Capacity)
	);
	assert_eq!(
		ctas(&state),
		(fits as u32, fits * charge),
		"accounting unchanged"
	);
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.ctas()
			.chain(TENANT, op_of(crossing))
			.is_none()
	);
	assert!(
		state
			.table_schemas
			.latest_by_id_including_provisional(TENANT, object_of(crossing))
			.is_none()
	);
	// Publishing chain 0 releases its charge; the refused Begin now fits.
	assert!(matches!(
		fixture::decide(
			&mut state,
			DecisionCommand::CtasPublish {
				tenant: TENANT,
				op: op_of(0),
				epoch: 1,
				owner: owner(SECRET),
				stamp_ms: T0,
			}
		),
		DecisionResult::CtasPublished {
			replayed: false,
			..
		}
	));
	assert_eq!(ctas(&state), (fits as u32 - 1, (fits - 1) * charge));
	assert!(matches!(
		begin_wide(&mut state, crossing),
		DecisionResult::CtasBegun {
			replayed: false,
			..
		}
	));
	assert_eq!(ctas(&state), (fits as u32, fits * charge));
}

/// ADR 0120 D4 (Codex 01a1fad7 §1): a key named by a CTAS operation is that
/// operation's chunk in every phase, never an ordinary write. After Publish ends
/// the object's provisional routing, a NEW first Reserve under the operation
/// (an undeclared sequence, or a declared one) is refused with nothing recorded;
/// the accepted chunk's key still replays its decision; a fresh ordinary write
/// on the published table is admitted.
#[test]
fn a_published_operations_keys_never_become_ordinary_writes() {
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished { .. }
	));
	let undeclared = chunk(&state, op(1), object(1), 1, D1);
	assert_eq!(
		reserve(&mut state, &undeclared),
		refused(CtasRefusal::WrongPhase),
		"an undeclared chunk after publication"
	);
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.bodies()
			.values()
			.all(|body| body.claim.key != undeclared.key),
		"nothing recorded for the refused key"
	);
	// Another object, the same operation: still the operation's key.
	let elsewhere = chunk(&state, op(1), object(2), 1, D1);
	assert!(matches!(
		reserve(&mut state, &elsewhere),
		DecisionResult::Refused(_)
	));
	assert!(
		!matches!(reserve(&mut state, &c0), DecisionResult::Refused(_)),
		"the accepted chunk's key replays its decision"
	);
	let fresh = fresh_write(&state, object(1), 43);
	assert!(
		matches!(reserve(&mut state, &fresh), DecisionResult::Reserved { .. }),
		"a fresh ordinary write on the published table"
	);
	assert_eq!(chain(&state).accepted, 1, "no new evidence");
}

/// ADR 0120 D3 (found building Codex 01a1fad7 §2's control): Begin judges its
/// operation id on the committed decision clock, as a Reserve judges its key:
/// the applied clock or the stamp, whichever is later. With the committed clock
/// ahead of a lagging stamp, an operation issued at the clock is admitted, and
/// one issued before the window behind it is refused though the stamp alone
/// would admit it.
#[test]
fn begin_judges_its_operation_on_the_committed_clock() {
	let mut state = granted();
	let floor = T0 + 3_600_000;
	fixture::decide(
		&mut state,
		DecisionCommand::AdvanceFloor {
			floor_ms: floor,
			stamp_ms: floor + crate::consensus::decisions::DECISION_EXPIRY_MS,
		},
	);
	let clock = state.decisions.as_ref().unwrap().clock_ms();
	assert!(
		clock > T0 + CTAS_OPERATION_WINDOW_MS,
		"the clock moved: {clock}"
	);
	let at_clock = fixture::uuid_v7(clock, 1);
	assert!(matches!(
		fixture::decide(
			&mut state,
			begin_with(at_clock, descriptor(object(1)), 1, decl(&[D0]), SECRET, T0)
		),
		DecisionResult::CtasBegun { .. }
	));
	assert_eq!(
		fixture::decide(
			&mut state,
			begin_with(op(2), descriptor(object(2)), 1, decl(&[D0]), SECRET, T0)
		),
		refused(CtasRefusal::OperationExpired)
	);
}

/// Advances the decision floor to `floor_ms`, the committed clock to `stamp_ms`.
fn advance(state: &mut SmState, floor_ms: u64, stamp_ms: u64) {
	let answer = fixture::decide(state, DecisionCommand::AdvanceFloor { floor_ms, stamp_ms });
	assert!(!matches!(answer, DecisionResult::Refused(_)), "{answer:?}");
}

fn prune(state: &mut SmState, op: [u8; 16]) -> DecisionResult {
	fixture::decide(state, DecisionCommand::CtasPrune { tenant: TENANT, op })
}

/// ADR 0120 D9 (S4): a chain's record is pruned only when it is terminal, its
/// retention lapsed on the committed clock, and its operation was issued at or
/// below the floor. Pruning keeps the object's visibility index (published), or
/// its retirement, routing and retained descriptor (abandoned): an ordinary
/// write on the published table is still admitted, a fresh write on the
/// abandoned object still refused, a repeated prune answers Unknown.
#[test]
fn a_terminal_chain_is_pruned_only_after_its_retention_and_the_floor() {
	let e = crate::consensus::decisions::DECISION_EXPIRY_MS;
	// Published.
	let mut state = begun(&[D0]);
	assert_eq!(
		prune(&mut state, op(1)),
		refused(CtasRefusal::WrongPhase),
		"dispatching"
	);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished { .. }
	));
	let terminal_at = chain(&state).terminal_at_ms;
	assert_eq!(
		prune(&mut state, op(1)),
		refused(CtasRefusal::PruneEarly),
		"retention"
	);
	// Past the retention, the floor still below the operation's issuance.
	let late = terminal_at + CTAS_TERMINAL_RETENTION_MS + 1;
	advance(&mut state, T0 - 1, late.max(T0 - 1 + e));
	assert_eq!(
		prune(&mut state, op(1)),
		refused(CtasRefusal::PruneEarly),
		"floor"
	);
	advance(&mut state, T0 + 1, (T0 + 1 + e).max(late));
	assert_eq!(
		prune(&mut state, op(1)),
		DecisionResult::CtasPruned {
			tenant: TENANT,
			op: op(1)
		}
	);
	let ctas = state.decisions.as_ref().unwrap().ctas();
	assert!(ctas.chain(TENANT, op(1)).is_none(), "the record is gone");
	assert!(
		ctas.visible_from(TENANT, object(1)).is_some(),
		"visibility stays"
	);
	assert_eq!(prune(&mut state, op(1)), refused(CtasRefusal::Unknown));

	// Terminal late (the owner renewed for an expiry): the floor covers the
	// operation while its retention, measured from the terminal time, has not
	// lapsed.
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	let renewed = fixture::decide(
		&mut state,
		DecisionCommand::CtasRenew {
			tenant: TENANT,
			op: op(1),
			epoch: 1,
			owner: owner(SECRET),
			stamp_ms: T0 + e,
		},
	);
	assert!(
		matches!(renewed, DecisionResult::CtasRenewed { .. }),
		"{renewed:?}"
	);
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasPublished { .. }
	));
	assert_eq!(
		chain(&state).terminal_at_ms,
		T0 + e,
		"the committed clock at Publish"
	);
	advance(&mut state, T0 + 1, T0 + 1 + e);
	assert_eq!(
		prune(&mut state, op(1)),
		refused(CtasRefusal::PruneEarly),
		"retention, the floor already covering the operation"
	);
	advance(&mut state, T0 + 2, T0 + e + CTAS_TERMINAL_RETENTION_MS);
	assert!(matches!(
		prune(&mut state, op(1)),
		DecisionResult::CtasPruned { .. }
	));

	// Abandoned, terminal late: retention from its terminal time, then pruned.
	let mut state = begun(&[D0]);
	renew_at(&mut state, T0 + e);
	assert!(matches!(
		abandon(&mut state, SECRET),
		DecisionResult::CtasAbandoned { .. }
	));
	assert_eq!(chain(&state).terminal_at_ms, T0 + e);
	advance(&mut state, T0 + 1, T0 + 1 + e);
	assert_eq!(prune(&mut state, op(1)), refused(CtasRefusal::PruneEarly));
	advance(&mut state, T0 + 2, T0 + e + CTAS_TERMINAL_RETENTION_MS);
	let retained = state.decisions.as_ref().unwrap().ctas().retained();
	assert!(matches!(
		prune(&mut state, op(1)),
		DecisionResult::CtasPruned { .. }
	));
	let ctas = state.decisions.as_ref().unwrap().ctas();
	assert!(
		ctas.retired(TENANT, object(1)).is_some(),
		"the retirement stays"
	);
	assert!(ctas.claims_object(TENANT, object(1)), "still private");
	assert_eq!(ctas.retained(), retained, "the retained descriptor stays");
	// Codex 01a1fbf1 R3: an old key is refused by the floor; a current,
	// eligible key on the abandoned object by its table; the same shape of key
	// on a live table is admitted.
	let now = state.decisions.as_ref().unwrap().clock_ms();
	let old = fresh_write(&state, object(1), 44);
	assert_eq!(
		reserve(&mut state, &old),
		DecisionResult::Refused(DecisionRefusal::OutcomeUnavailable),
		"an old key, below the floor"
	);
	let current = fresh_write_at(&state, object(1), 45, now);
	assert_eq!(
		fixture::reserve(&mut state, &current, now),
		DecisionResult::Refused(DecisionRefusal::TableUnavailable),
		"a current key on the abandoned object"
	);
	create(
		&mut state,
		descriptor_of(object(9), "live", arrow::datatypes::DataType::Utf8),
	)
	.unwrap();
	let live = fresh_write_at(&state, object(9), 46, now);
	assert!(
		matches!(
			fixture::reserve(&mut state, &live, now),
			DecisionResult::Reserved { .. }
		),
		"a current key on a live table"
	);

	// Conflict, terminal late: the same.
	let mut state = begun(&[D0]);
	let c0 = chunk(&state, op(1), object(1), 0, D0);
	accept(&mut state, &c0);
	renew_at(&mut state, T0 + e);
	create(
		&mut state,
		descriptor_of(object(7), "copy", arrow::datatypes::DataType::Utf8),
	)
	.unwrap();
	assert!(matches!(
		publish(&mut state, 1, SECRET),
		DecisionResult::CtasConflict { .. }
	));
	assert_eq!(chain(&state).terminal_at_ms, T0 + e);
	advance(&mut state, T0 + 1, T0 + 1 + e);
	assert_eq!(prune(&mut state, op(1)), refused(CtasRefusal::PruneEarly));
	advance(&mut state, T0 + 2, T0 + e + CTAS_TERMINAL_RETENTION_MS);
	assert!(matches!(
		prune(&mut state, op(1)),
		DecisionResult::CtasPruned { .. }
	));
}

/// The owner renews `op(1)` at `stamp_ms`, raising the committed clock.
fn renew_at(state: &mut SmState, stamp_ms: u64) {
	let renewed = fixture::decide(
		state,
		DecisionCommand::CtasRenew {
			tenant: TENANT,
			op: op(1),
			epoch: 1,
			owner: owner(SECRET),
			stamp_ms,
		},
	);
	assert!(
		matches!(renewed, DecisionResult::CtasRenewed { .. }),
		"{renewed:?}"
	);
}

/// ADR 0120 D3 (S4): the retained-descriptor count bound (1,024) is reached once
/// prune frees chain records: rounds of 256 abandoned chains, each round pruned
/// after the floor passes it, retain 1,024 descriptors; the next Begin refuses
/// `Capacity` with nothing recorded.
#[test]
fn the_retained_descriptor_count_is_bounded_once_prune_frees_records() {
	let e = crate::consensus::decisions::DECISION_EXPIRY_MS;
	let mut state = granted();
	let mut now = T0;
	let mut n: u32 = 0;
	for _round in 0..(CTAS_RETAINED_DESCRIPTORS_MAX as usize / CTAS_CHAIN_CAPACITY) {
		let mut ops = Vec::new();
		for _ in 0..CTAS_CHAIN_CAPACITY {
			n += 1;
			let op = fixture::uuid_v7(now, (n % 251) as u8);
			let mut id = [0u8; 16];
			id[..4].copy_from_slice(&n.to_be_bytes());
			id[15] = 0xC7;
			let object = TableObjectId::new(id);
			let begun = fixture::decide(
				&mut state,
				begin_with(
					op,
					descriptor_of(
						object,
						&format!("retained_{n:05}"),
						arrow::datatypes::DataType::Utf8,
					),
					1,
					decl(&[D0]),
					SECRET,
					now,
				),
			);
			assert!(
				matches!(begun, DecisionResult::CtasBegun { .. }),
				"{n}: {begun:?}"
			);
			let abandoned = fixture::decide(
				&mut state,
				DecisionCommand::CtasAbandon {
					tenant: TENANT,
					op,
					epoch: 1,
					owner: owner(SECRET),
					cause: CtasAbandonCause::Deadline,
					stamp_ms: T0,
				},
			);
			assert!(matches!(abandoned, DecisionResult::CtasAbandoned { .. }));
			ops.push(op);
			now += 1;
		}
		let clock = state.decisions.as_ref().unwrap().clock_ms();
		let floor = now + 1;
		advance(
			&mut state,
			floor,
			(floor + e).max(clock + CTAS_TERMINAL_RETENTION_MS + 1),
		);
		for op in ops {
			assert!(matches!(
				prune(&mut state, op),
				DecisionResult::CtasPruned { .. }
			));
		}
		now = state.decisions.as_ref().unwrap().clock_ms();
	}
	assert_eq!(
		state.decisions.as_ref().unwrap().ctas().retained().0 as usize,
		CTAS_RETAINED_DESCRIPTORS_MAX as usize
	);
	let op = fixture::uuid_v7(now, 1);
	let mut id = [0xD7u8; 16];
	id[0] = 0;
	let refusal = fixture::decide(
		&mut state,
		begin_with(
			op,
			descriptor_of(
				TableObjectId::new(id),
				"one_more",
				arrow::datatypes::DataType::Utf8,
			),
			1,
			decl(&[D0]),
			SECRET,
			now,
		),
	);
	assert_eq!(refusal, refused(CtasRefusal::Capacity));
	assert!(
		state
			.decisions
			.as_ref()
			.unwrap()
			.ctas()
			.chain(TENANT, op)
			.is_none(),
		"nothing recorded"
	);
}

/// Codex 01a1fbf1 R1: the terminal time is the terminal command's own gated
/// stamp (the committed clock with it), never a stale clock left by the last
/// stamped command. A chain begun and accepted at T0 sits through a quiet period;
/// it becomes terminal with a stamp well past an expiry, with no stamped command
/// between. The floor step that then covers its operation does not admit an
/// immediate Prune; the full retention from the terminal time does. For the
/// owner's Publish, a Publish ending in Conflict, the owner's Abandon and
/// recovery's Conclude, and through a serialization round trip of the
/// decision state.
#[test]
fn the_terminal_time_is_the_terminal_commands_gated_stamp() {
	let e = crate::consensus::decisions::DECISION_EXPIRY_MS;
	let late = T0 + e + 20 * 60 * 1_000;
	for path in ["publish", "conflict", "abandon", "conclude"] {
		let mut state = begun(&[D0]);
		let c0 = chunk(&state, op(1), object(1), 0, D0);
		accept(&mut state, &c0);
		if path == "conflict" {
			// The name is bound meanwhile: the late Publish ends in Conflict.
			create(
				&mut state,
				descriptor_of(object(7), "copy", arrow::datatypes::DataType::Utf8),
			)
			.unwrap();
		}
		let terminal = match path {
			"publish" | "conflict" => fixture::decide(
				&mut state,
				DecisionCommand::CtasPublish {
					tenant: TENANT,
					op: op(1),
					epoch: 1,
					owner: owner(SECRET),
					stamp_ms: late,
				},
			),
			"abandon" => fixture::decide(
				&mut state,
				DecisionCommand::CtasAbandon {
					tenant: TENANT,
					op: op(1),
					epoch: 1,
					owner: owner(SECRET),
					cause: CtasAbandonCause::Deadline,
					stamp_ms: late,
				},
			),
			_ => {
				assert!(matches!(
					recover(&mut state, LAPSED),
					DecisionResult::CtasRecovering { epoch: 2 }
				));
				fixture::decide(
					&mut state,
					DecisionCommand::CtasConclude {
						tenant: TENANT,
						op: op(1),
						epoch: 2,
						stamp_ms: late,
					},
				)
			}
		};
		assert!(
			matches!(
				terminal,
				DecisionResult::CtasPublished { .. }
					| DecisionResult::CtasAbandoned { .. }
					| DecisionResult::CtasConflict { .. }
			),
			"{path}: {terminal:?}"
		);
		assert_eq!(chain(&state).terminal_at_ms, late, "{path}");
		let decisions = state.decisions.as_ref().unwrap();
		let bytes = postcard::to_stdvec(decisions).unwrap();
		let back: DecisionState = postcard::from_bytes(&bytes).unwrap();
		assert_eq!(
			back.ctas()
				.chain(TENANT, op(1))
				.map(|chain| chain.terminal_at_ms),
			Some(late),
			"{path}: across a round trip"
		);
		advance(&mut state, T0 + 1, T0 + 1 + e);
		assert_eq!(
			prune(&mut state, op(1)),
			refused(CtasRefusal::PruneEarly),
			"{path}: the floor covers the op; the retention from the terminal time does not"
		);
		advance(&mut state, T0 + 2, late + CTAS_TERMINAL_RETENTION_MS);
		assert!(
			matches!(prune(&mut state, op(1)), DecisionResult::CtasPruned { .. }),
			"{path}"
		);
	}
}

/// Codex 01a1fbf1 R2: maintenance proposes exactly what Conclude can decide now,
/// through the reducer's own predicate (`conclude_outcome`). Under a backup hold,
/// 17 fully accepted recovering chains earlier in key order (more than one batch
/// of 16) are not proposed: their publication is held. A later recovering chain
/// with refused evidence and another chunk still Reserved is proposed, and its
/// Conclude abandons it, concluding the Reserved chunk. Nothing recurs for the
/// held chains while the hold lasts; after the release they are proposed.
#[test]
fn maintenance_proposes_only_what_conclude_can_decide_under_a_hold() {
	let mut state = granted();
	let mut held = Vec::new();
	let recover_op = |state: &mut SmState, op: [u8; 16], lapsed: u64| {
		let answer = fixture::decide(
			state,
			DecisionCommand::CtasRecover {
				tenant: TENANT,
				op,
				new_epoch: 2,
				stamp_ms: lapsed,
			},
		);
		assert_eq!(answer, DecisionResult::CtasRecovering { epoch: 2 });
	};
	for n in 0..17u8 {
		let op = fixture::uuid_v7(T0, 1 + n);
		let mut id = [0xA1u8; 16];
		id[0] = n;
		let object = TableObjectId::new(id);
		let begun = fixture::decide(
			&mut state,
			begin_with(
				op,
				descriptor_of(
					object,
					&format!("held_{n:02}"),
					arrow::datatypes::DataType::Utf8,
				),
				1,
				decl(&[D0]),
				SECRET,
				T0,
			),
		);
		assert!(
			matches!(begun, DecisionResult::CtasBegun { .. }),
			"{begun:?}"
		);
		let c0 = chunk(&state, op, object, 0, D0);
		accept(&mut state, &c0);
		held.push(op);
	}
	// The later chain: chunk 0 refused, chunk 1 still Reserved.
	let late = fixture::uuid_v7(T0 + 1, 1);
	let object = TableObjectId::new([0xB7; 16]);
	assert!(matches!(
		fixture::decide(
			&mut state,
			begin_with(
				late,
				descriptor_of(object, "actionable", arrow::datatypes::DataType::Utf8),
				1,
				decl(&[D0, D1]),
				SECRET,
				T0 + 1,
			)
		),
		DecisionResult::CtasBegun { .. }
	));
	let c0 = chunk(&state, late, object, 0, D0);
	let rsv = reserved(&mut state, &c0);
	assert!(matches!(
		fixture::decide(&mut state, fixture::abort_command(&c0, rsv)),
		DecisionResult::Terminal { .. }
	));
	let c1 = chunk(&state, late, object, 1, D1);
	let reserved_rsv = reserved(&mut state, &c1);
	// Past every chain's lease (each runs from the committed clock at its Begin).
	let lapsed =
		held.iter()
			.chain([&late])
			.map(|op| {
				state
					.decisions
					.as_ref()
					.unwrap()
					.ctas()
					.chain(TENANT, *op)
					.unwrap()
					.lease_until_ms
			})
			.max()
			.unwrap() + 1;
	for op in held.iter().copied().chain([late]) {
		recover_op(&mut state, op, lapsed);
	}

	let entered = match fixture::decide(&mut state, DecisionCommand::BeginBackupHold { id: 9 }) {
		DecisionResult::HoldEntered { id, entered_at } => (id, entered_at),
		other => panic!("{other:?}"),
	};
	let due = maintenance_due(state.decisions.as_ref().unwrap(), lapsed, 16);
	let expected = DecisionCommand::CtasConclude {
		tenant: TENANT,
		op: late,
		epoch: 2,
		stamp_ms: lapsed,
	};
	assert_eq!(
		due,
		vec![expected.clone()],
		"only the actionable chain, under the hold"
	);
	assert!(matches!(
		fixture::decide(&mut state, expected),
		DecisionResult::CtasAbandoned {
			cause: CtasAbandonCause::ChunkRefused,
			..
		}
	));
	assert!(matches!(
		state.decisions.as_ref().unwrap().bodies()[&reserved_rsv].status,
		ParentStatus::Refused { .. }
	));
	assert!(
		maintenance_due(state.decisions.as_ref().unwrap(), lapsed, 16).is_empty(),
		"nothing recurs for the held chains"
	);
	assert!(!matches!(
		fixture::decide(
			&mut state,
			DecisionCommand::ReleaseBackupHold {
				id: entered.0,
				entered_at: entered.1,
			}
		),
		DecisionResult::Refused(_)
	));
	assert_eq!(
		maintenance_due(state.decisions.as_ref().unwrap(), lapsed, 16).len(),
		16,
		"released: the held chains are proposed, one batch at a time"
	);
}
