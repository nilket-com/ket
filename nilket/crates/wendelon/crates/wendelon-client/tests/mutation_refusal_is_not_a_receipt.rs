//! A mutation answer is a success receipt only when it says the operation COMMITTED
//! (ADR 0732, DC-238). The transport is scripted: each answer is the response struct a
//! node sends, with a chosen status and commit knowledge. Among them is the pair nodes
//! sent for a parent their decision log recorded as refused before the `WRITE_REFUSED`
//! status existed, `OK` with `KNOWN_NOT_COMMITTED`, which was returned as a receipt.

mod mutation_transport;

use std::sync::Arc;

use arrow::array::{Int64Array, LargeStringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use mutation_transport::{MutationWire, inner_client};
use wendelon::schema::{
	ClientWriteId, Identity, LocalityPolicy, MsgType, MutationCommitKnowledge, MutationStopReason,
	PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableDeleteRowsRequestMeta,
	TableDeleteRowsResponseMeta, TableObjectId, TableSchemaDescriptor, TableWriteRequestMeta,
	TableWriteResponseMeta, TenantId, WriteRequestMeta, WriteResponseMeta, WriteStatus,
};
use wendelon_client::{Client, ClientError, WriteOptions};

fn descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([0x21; 16]),
		1,
		"smoke",
		"rows",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

fn one_row() -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
		])),
		vec![
			Arc::new(Int64Array::from(vec![32_i64])),
			Arc::new(LargeStringArray::from(vec!["thirty-two"])),
		],
	)
	.unwrap()
}

/// The inner write answer for `write_id`: the given status and knowledge, nothing else.
fn write_answer(
	write_id: ClientWriteId,
	status: WriteStatus,
	knowledge: MutationCommitKnowledge,
) -> WriteResponseMeta {
	let mut write: WriteResponseMeta = bytemuck::Zeroable::zeroed();
	write.status = status;
	write.echoed_write_id = write_id;
	write.with_mutation_outcome(knowledge, MutationStopReason::NONE)
}

fn request_identity(msg_type: u8, meta: &[u8]) -> ClientWriteId {
	if msg_type == MsgType::TABLE_WRITE_REQ.0 {
		bytemuck::from_bytes::<TableWriteRequestMeta>(
			&meta[..std::mem::size_of::<TableWriteRequestMeta>()],
		)
		.parent_write_id
	} else if msg_type == MsgType::TABLE_DELETE_REQ.0 {
		bytemuck::from_bytes::<TableDeleteRowsRequestMeta>(
			&meta[..std::mem::size_of::<TableDeleteRowsRequestMeta>()],
		)
		.parent_write_id
	} else if msg_type == MsgType::WRITE_REQ.0 {
		bytemuck::from_bytes::<WriteRequestMeta>(&meta[..std::mem::size_of::<WriteRequestMeta>()])
			.write_id
	} else {
		panic!("an unexpected request, message type {msg_type:#x}");
	}
}

/// A client whose every table write, table delete and key-value write is answered with
/// `status` and `knowledge`, echoing the request's own identity.
fn client_answered(
	status: WriteStatus,
	knowledge: MutationCommitKnowledge,
) -> (Client, MutationWire) {
	let wire = MutationWire::answering(Arc::new(move |msg_type, meta| {
		let write = write_answer(request_identity(msg_type, meta), status, knowledge);
		Some(if msg_type == MsgType::TABLE_WRITE_REQ.0 {
			let response = TableWriteResponseMeta {
				write,
				reject_code: 0,
				_pad: [0; 6],
			};
			(
				MsgType::TABLE_WRITE_RESP.0,
				bytemuck::bytes_of(&response).to_vec(),
			)
		} else if msg_type == MsgType::TABLE_DELETE_REQ.0 {
			let mut response: TableDeleteRowsResponseMeta = bytemuck::Zeroable::zeroed();
			response.write = write;
			(
				MsgType::TABLE_DELETE_RESP.0,
				bytemuck::bytes_of(&response).to_vec(),
			)
		} else {
			(MsgType::WRITE_RESP.0, bytemuck::bytes_of(&write).to_vec())
		})
	}));
	(Client::from_inner(inner_client(&wire)), wire)
}

/// The identity the one request on the wire carried.
fn sent_identity(wire: &MutationWire) -> ClientWriteId {
	let requests = wire.requests();
	assert_eq!(requests.len(), 1, "one request, no retry");
	request_identity(requests[0].0, &requests[0].1)
}

#[derive(Clone, Copy, Debug)]
enum Mutation {
	TableWrite,
	TableDelete,
	KeyValueWrite,
}

const MUTATIONS: [Mutation; 3] = [
	Mutation::TableWrite,
	Mutation::TableDelete,
	Mutation::KeyValueWrite,
];

/// One public mutation call answered with `status` and `knowledge`: whether its receipt
/// says duplicate, or its error; and the wire.
async fn answered(
	mutation: Mutation,
	status: WriteStatus,
	knowledge: MutationCommitKnowledge,
) -> (Result<bool, ClientError>, MutationWire) {
	let (client, wire) = client_answered(status, knowledge);
	let tenant = TenantId::new(1);
	let options = WriteOptions::default();
	let result = match mutation {
		Mutation::TableWrite => client
			.table_write(tenant, &descriptor(), one_row(), options)
			.await
			.map(|receipt| receipt.duplicate),
		Mutation::TableDelete => client
			.table_delete(tenant, &descriptor(), vec![Identity::new([7; 32])], options)
			.await
			.map(|receipt| receipt.duplicate),
		Mutation::KeyValueWrite => client
			.write(tenant, b"key", b"value", options)
			.await
			.map(|receipt| receipt.duplicate),
	};
	(result, wire)
}

/// What a refusal must look like to the caller: an error that names this operation and
/// says, as the node did, that it is known not to have committed.
fn assert_reported_not_committed(
	result: Result<bool, ClientError>,
	wire: &MutationWire,
	what: &str,
) {
	match result {
		Ok(duplicate) => panic!(
			"{what}: a mutation the node answered KNOWN_NOT_COMMITTED was returned as a success receipt (duplicate {duplicate})"
		),
		Err(ClientError::MutationStopped {
			operation_id,
			status,
			knowledge,
			..
		}) => {
			assert_eq!(status, WriteStatus::WRITE_REFUSED, "{what}");
			assert_eq!(
				knowledge,
				MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
				"{what}"
			);
			assert_eq!(operation_id, sent_identity(wire), "{what}");
		}
		Err(other) => panic!("{what}: the refusal lost its identity or its knowledge: {other:?}"),
	}
}

#[tokio::test]
async fn a_table_write_answered_ok_but_known_not_committed_is_not_a_receipt() {
	let (result, wire) = answered(
		Mutation::TableWrite,
		WriteStatus::OK,
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
	)
	.await;
	assert_reported_not_committed(result, &wire, "table write");
}

#[tokio::test]
async fn a_table_delete_answered_ok_but_known_not_committed_is_not_a_receipt() {
	let (result, wire) = answered(
		Mutation::TableDelete,
		WriteStatus::OK,
		MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
	)
	.await;
	assert_reported_not_committed(result, &wire, "table delete");
}

/// Positive control: the same transport, answered OK and COMMITTED, is a receipt.
#[tokio::test]
async fn a_table_write_answered_ok_and_committed_is_a_receipt() {
	let (result, wire) = answered(
		Mutation::TableWrite,
		WriteStatus::OK,
		MutationCommitKnowledge::COMMITTED,
	)
	.await;
	assert!(!result.expect("a committed write is a receipt"));
	assert_eq!(wire.requests().len(), 1);
}

/// The whole reading, for every public mutation that returns a receipt here: success is
/// OK or DUPLICATE WITH COMMITTED and nothing else.
#[tokio::test]
async fn a_receipt_needs_a_committed_answer_and_a_refusal_needs_known_non_commit() {
	let unknown = MutationCommitKnowledge(9);
	for mutation in MUTATIONS {
		for (status, duplicate) in [(WriteStatus::OK, false), (WriteStatus::DUPLICATE, true)] {
			let (result, wire) =
				answered(mutation, status, MutationCommitKnowledge::COMMITTED).await;
			assert_eq!(
				result.unwrap_or_else(|error| panic!("{mutation:?} {status:?}: {error:?}")),
				duplicate
			);
			assert_eq!(wire.requests().len(), 1);
		}
		// A recorded refusal: the status, and the pair nodes sent before it existed.
		for status in [
			WriteStatus::WRITE_REFUSED,
			WriteStatus::OK,
			WriteStatus::DUPLICATE,
		] {
			let (result, wire) = answered(
				mutation,
				status,
				MutationCommitKnowledge::KNOWN_NOT_COMMITTED,
			)
			.await;
			assert_reported_not_committed(result, &wire, &format!("{mutation:?} {status:?}"));
		}
		// Contradictory or silent pairs: the outcome is unknown, the raw pair is kept,
		// and nothing is retried.
		for (status, knowledge) in [
			(WriteStatus::OK, MutationCommitKnowledge::UNSPECIFIED),
			(WriteStatus::OK, MutationCommitKnowledge::INDETERMINATE),
			(WriteStatus::OK, unknown),
			(WriteStatus::DUPLICATE, MutationCommitKnowledge::UNSPECIFIED),
			(
				WriteStatus::DUPLICATE,
				MutationCommitKnowledge::INDETERMINATE,
			),
			(WriteStatus::DUPLICATE, unknown),
			(
				WriteStatus::WRITE_REFUSED,
				MutationCommitKnowledge::COMMITTED,
			),
			(
				WriteStatus::WRITE_REFUSED,
				MutationCommitKnowledge::INDETERMINATE,
			),
			(
				WriteStatus::WRITE_REFUSED,
				MutationCommitKnowledge::UNSPECIFIED,
			),
			(WriteStatus::WRITE_REFUSED, unknown),
		] {
			let (result, wire) = answered(mutation, status, knowledge).await;
			let what = format!("{mutation:?} {status:?} with {knowledge:?}");
			match result {
				Err(ClientError::MutationAnswerInconsistent {
					operation_id,
					status: got_status,
					knowledge: got_knowledge,
				}) => {
					assert_eq!((got_status, got_knowledge), (status, knowledge), "{what}");
					assert_eq!(operation_id, sent_identity(&wire), "{what}");
				}
				other => panic!("{what}: {other:?}"),
			}
		}
	}
}
