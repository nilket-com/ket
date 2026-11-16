//! ADR 0105 (wendelon): the data request answers of a member that serves no data.
//!
//! A control-only replacement (ADR 0104) holds no data, signs no client work and
//! takes no lease. A request that reaches it anyway (a client routed to its
//! slot, a survivor fanning out to the slot's replica) gets a typed refusal in
//! its own family's response schema, never a dropped stream and never a
//! fabricated success. This surface constructs no data server, slab manager or
//! background duty; it reads and writes nothing.
//!
//! | Request | Answer |
//! |---|---|
//! | `TABLE_WRITE_REQ` | `TABLE_WRITE_RESP`: `NOT_LEADER`, outcome `INDETERMINATE`, no hint, the request's write id echoed, no log position, commit clock or finalization |
//! | `TABLE_DELETE_REQ` | `TABLE_DELETE_RESP`: the same, with zero counts |
//! | `TABLE_QUERY_REQ` | `TABLE_QUERY_RESP` (stream end): `NOT_LEADER`, no hint, no rows |
//! | `TABLE_CHILD_WRITE_REQ`, `TABLE_CHILD_REPLICATE_REQ` | their response: `ChildWriteResponse::Refused`, no acknowledgement |
//! | `TABLE_CHILD_FETCH_REQ` | `TABLE_CHILD_FETCH_RESP`: `ChildFetchResponse::Refused`, no rows |
//! | `TABLE_BASE_FETCH_REQ` | `TABLE_BASE_FETCH_RESP`: `Refused(NotReady)`, no entries or rows (ADR 0106) |
//! | `PENDING_WITNESS_REQ` | `PENDING_WITNESS_RESP`: `INTERNAL_ERROR`, no witnesses (never `OK` with none: it holds no inventory to establish absence) |
//! | anything else, or a malformed request | an error: the stream ends unanswered, as before |
//!
//! Node-to-node families require an authenticated data node, as the serving
//! handlers do. A client family is answered without authorizing its tenant:
//! the answer says only that this member does not serve.

use super::child_writes::ChildWriteResponse;
use super::repair::ChildFetchResponse;
use super::*;

/// Why every refusal here is given.
const NOT_SERVING: &str = "this member is control-only and serves no data";

pub(crate) struct NotServing {
	pool: Arc<BufPool>,
}

impl NotServing {
	pub(crate) fn new(pool: Arc<BufPool>) -> Self {
		Self { pool }
	}

	/// The answer to one request, or the error that ends its stream unanswered.
	pub(crate) fn answer(
		&self,
		context: &RequestContext,
		frame: &HrftFrame,
	) -> std::io::Result<HrftFrame> {
		let stream_id = frame.header.stream_id;
		match MsgType(frame.header.msg_type) {
			MsgType::TABLE_WRITE_REQ => {
				let meta: TableWriteRequestMeta = fixed(frame, "TABLE_WRITE_REQ")?;
				let response = TableWriteResponseMeta {
					write: not_leader(meta.expected_epoch, meta.parent_write_id),
					reject_code: PbcRejectCode::NONE.0,
					_pad: [0; 6],
				};
				Ok(self.frame(
					MsgType::TABLE_WRITE_RESP,
					stream_id,
					bytemuck::bytes_of(&response),
					0,
				))
			}
			MsgType::TABLE_DELETE_REQ => {
				let meta: TableDeleteRowsRequestMeta = fixed(frame, "TABLE_DELETE_REQ")?;
				let response = TableDeleteRowsResponseMeta {
					write: not_leader(meta.expected_epoch, meta.parent_write_id),
					deleted_count: 0,
					already_deleted_count: 0,
					not_found_count: 0,
					reject_code: PbcRejectCode::NONE.0,
					_pad: [0; 10],
				};
				Ok(self.frame(
					MsgType::TABLE_DELETE_RESP,
					stream_id,
					bytemuck::bytes_of(&response),
					0,
				))
			}
			MsgType::TABLE_QUERY_REQ => {
				let meta: TableQueryRequestMeta = fixed(frame, "TABLE_QUERY_REQ")?;
				let response = query_error_meta(
					&table_query_as_range_query(&meta),
					ReadStatus::NOT_LEADER,
					ZERO_NODE,
				);
				Ok(self.frame(
					MsgType::TABLE_QUERY_RESP,
					stream_id,
					bytemuck::bytes_of(&response),
					FRAME_FLAG_STREAM_END,
				))
			}
			MsgType::TABLE_CHILD_WRITE_REQ | MsgType::TABLE_CHILD_REPLICATE_REQ => {
				let name = if frame.header.msg_type == MsgType::TABLE_CHILD_WRITE_REQ.0 {
					"TABLE_CHILD_WRITE_REQ"
				} else {
					"TABLE_CHILD_REPLICATE_REQ"
				};
				context.require_authenticated_node(name)?;
				let response_type = if frame.header.msg_type == MsgType::TABLE_CHILD_WRITE_REQ.0 {
					MsgType::TABLE_CHILD_WRITE_RESP
				} else {
					MsgType::TABLE_CHILD_REPLICATE_RESP
				};
				let body = postcard::to_stdvec(&ChildWriteResponse::Refused(NOT_SERVING.into()))
					.map_err(Error::other)?;
				Ok(self.frame(response_type, stream_id, &body, 0))
			}
			MsgType::TABLE_CHILD_FETCH_REQ => {
				context.require_authenticated_node("TABLE_CHILD_FETCH_REQ")?;
				let body = postcard::to_stdvec(&ChildFetchResponse::Refused(NOT_SERVING.into()))
					.map_err(Error::other)?;
				Ok(self.frame(MsgType::TABLE_CHILD_FETCH_RESP, stream_id, &body, 0))
			}
			MsgType::TABLE_BASE_FETCH_REQ => {
				context.require_authenticated_node("TABLE_BASE_FETCH_REQ")?;
				let body = postcard::to_stdvec(&super::base_fetch::BaseFetchAnswer::Refused(
					super::base_fetch::BaseFetchRefusal::NotReady,
				))
				.map_err(Error::other)?;
				Ok(self.frame(MsgType::TABLE_BASE_FETCH_RESP, stream_id, &body, 0))
			}
			MsgType::PENDING_WITNESS_REQ => {
				let _: PendingWitnessPullRequestMeta = fixed(frame, "PENDING_WITNESS_REQ")?;
				context.require_authenticated_node("PENDING_WITNESS_REQ")?;
				let response = PendingWitnessPullResponseMeta {
					status: PendingWitnessPullStatus::INTERNAL_ERROR,
					_pad1: [0; 7],
					witness_count: 0,
				};
				Ok(self.frame(
					MsgType::PENDING_WITNESS_RESP,
					stream_id,
					bytemuck::bytes_of(&response),
					0,
				))
			}
			other => Err(Error::new(
				ErrorKind::Unsupported,
				format!(
					"{NOT_SERVING}: msg_type=0x{:02x} is not answered here",
					other.0
				),
			)),
		}
	}

	fn frame(&self, msg_type: MsgType, stream_id: u32, body: &[u8], flags: u16) -> HrftFrame {
		let mut meta = self.pool.acquire();
		meta.as_mut_vec().extend_from_slice(body);
		HrftFrame {
			header: FrameHeader {
				magic: HRFT_MAGIC,
				version: 1,
				msg_type: msg_type.0,
				flags,
				stream_id,
				meta_len: meta.len() as u32,
				arrow_len: 0,
				trailing_len: 0,
			},
			meta,
			arrow_payload: None,
			trailing: None,
		}
	}
}

#[async_trait::async_trait]
impl crate::network::router::DataRingServer for NotServing {
	async fn handle_data_frame(
		&self,
		context: RequestContext,
		frame: HrftFrame,
	) -> std::io::Result<Option<HrftFrame>> {
		self.answer(&context, &frame).map(Some)
	}
}

/// A request's fixed meta, read whole or refused as malformed.
fn fixed<T: bytemuck::Pod>(frame: &HrftFrame, name: &str) -> std::io::Result<T> {
	let width = std::mem::size_of::<T>();
	if frame.meta.len() < width {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{name} meta too small"),
		));
	}
	Ok(*bytemuck::from_bytes(&frame.meta[..width]))
}

/// A mutation this member did not take: not a leader here, no hint (it cannot
/// name a coordinator), the outcome unknown (it did no work, which says nothing
/// of the operation elsewhere), and no position, commit clock or finalization.
fn not_leader(epoch: u64, write_id: crate::schema::ClientWriteId) -> WriteResponseMeta {
	WriteResponseMeta {
		status: WriteStatus::NOT_LEADER,
		_pad1: [0; 7],
		log_index: 0,
		term: 0,
		epoch,
		commit_hlc: HybridTimestamp {
			physical_ms: 0,
			logical: 0,
			node_id: 0,
		},
		leader_hint: ZERO_NODE,
		echoed_write_id: write_id,
		has_finalization: 0,
		_pad2: [0; 7],
	}
	.with_mutation_outcome(
		MutationCommitKnowledge::INDETERMINATE,
		MutationStopReason::NONE,
	)
}
