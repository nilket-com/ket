use super::*;
#[derive(Debug)]
pub(super) enum StoreError {
	Codec(RootError2),
	Io(std::io::Error),
	Unavailable,
	Limit,
	Fenced,
	Conflict,
}
pub(super) type StoreResult<T> = Result<T, StoreError>;
impl From<RootError2> for StoreError {
	fn from(e: RootError2) -> Self {
		Self::Codec(e)
	}
}
impl From<std::io::Error> for StoreError {
	fn from(e: std::io::Error) -> Self {
		Self::Io(e)
	}
}
pub(super) fn require(ok: bool) -> StoreResult<()> {
	if ok {
		Ok(())
	} else {
		Err(StoreError::Unavailable)
	}
}
pub(super) const DIRS: [&str; 6] = [
	"allocations",
	"data",
	"index",
	"journals",
	"grants",
	"quarantine",
];
pub(super) const PARENTS: usize = 32768;
pub(super) const GRANTS: usize = 4096;
pub(super) const SEGMENT_RECORDS: u64 = 4096;
pub(super) const SEGMENTS: usize = 9;
pub(super) const CHECKPOINT_CAP: usize = 16 * 1024 * 1024;
pub(super) const JOURNAL_CAP: usize = 32 * 1024 * 1024;
pub(super) const HEADROOM: u64 = 33555864;
pub(super) const QUOTA: u64 = 1 << 40;
pub(super) const FREE_FLOOR: u64 = 64 * 1024 * 1024;
pub(super) const READ_WORK: u64 = 4 << 40;
pub(super) const TEMP_FILES: usize = 64;
pub(super) const NAMESPACE: usize = 102489;
pub(super) const BUFFER_BYTES: u32 = 256 * 1024 * 1024;
pub(super) const IO_CHUNK: usize = 64 * 1024;
