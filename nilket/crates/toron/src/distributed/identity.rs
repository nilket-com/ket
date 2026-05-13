use serde::{Deserialize, Serialize};
use uuid::Uuid;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct QueryId(Uuid);

impl QueryId {
	pub fn new() -> Self {
		Self(Uuid::now_v7())
	}

	pub const fn from_uuid(value: Uuid) -> Self {
		Self(value)
	}

	pub const fn from_u128(value: u128) -> Self {
		Self(Uuid::from_u128(value))
	}

	pub const fn get(self) -> Uuid {
		self.0
	}
}

impl Default for QueryId {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StageId(pub u32);

impl StageId {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct TaskId(pub u32);

impl TaskId {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct AttemptId(pub u32);

impl AttemptId {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn first() -> Self {
		Self(0)
	}

	pub const fn next(self) -> Self {
		Self(self.0 + 1)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct WorkerId(Uuid);

impl WorkerId {
	pub fn new() -> Self {
		Self(Uuid::now_v7())
	}

	pub const fn from_uuid(value: Uuid) -> Self {
		Self(value)
	}

	pub const fn from_u128(value: u128) -> Self {
		Self(Uuid::from_u128(value))
	}

	pub const fn get(self) -> Uuid {
		self.0
	}
}

impl Default for WorkerId {
	fn default() -> Self {
		Self::new()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct PartitionId(pub u32);

impl PartitionId {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct ExchangeId(pub u32);

impl ExchangeId {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct LeaseEpoch(pub u32);

impl LeaseEpoch {
	pub const fn new(value: u32) -> Self {
		Self(value)
	}

	pub const fn next(self) -> Self {
		Self(self.0 + 1)
	}

	pub const fn get(self) -> u32 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct FrameByteLen(pub u32);

impl FrameByteLen {
	pub fn new(value: usize) -> Option<Self> {
		u32::try_from(value).ok().map(Self)
	}

	pub const fn from_u32(value: u32) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u32 {
		self.0
	}

	pub fn as_usize(self) -> usize {
		usize::try_from(self.0).expect("u32 frame byte length fits usize")
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize)]
pub struct StreamSeq(pub u64);

impl StreamSeq {
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Serialize, Deserialize)]
pub struct TaskIdentity {
	pub query_id: QueryId,
	pub stage_id: StageId,
	pub task_id: TaskId,
	pub attempt_id: AttemptId,
}

impl TaskIdentity {
	pub const fn new(
		query_id: QueryId,
		stage_id: StageId,
		task_id: TaskId,
		attempt_id: AttemptId,
	) -> Self {
		Self {
			query_id,
			stage_id,
			task_id,
			attempt_id,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn task_identity_serializes_with_uuid_query_id() {
		let identity = TaskIdentity::new(
			QueryId::from_u128(42),
			StageId::new(1),
			TaskId::new(2),
			AttemptId::first(),
		);

		let encoded = postcard::to_allocvec(&identity).expect("encode identity");
		let decoded: TaskIdentity = postcard::from_bytes(&encoded).expect("decode identity");

		assert_eq!(decoded, identity);
		assert_eq!(decoded.query_id.get(), uuid::Uuid::from_u128(42));
	}

	#[test]
	fn attempt_id_bumps_explicitly() {
		assert_eq!(AttemptId::first().get(), 0);
		assert_eq!(AttemptId::first().next().get(), 1);
	}
}
