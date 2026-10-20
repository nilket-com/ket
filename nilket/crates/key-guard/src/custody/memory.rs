//! Secret memory (K3, G4, G5): best-effort `mlock`, and a fixed-capacity heap buffer that is
//! zeroed before it is freed.
//!
//! mlock is best effort: a failure is not fatal, and [`SecretBuf::is_locked`] reports it so the
//! caller can warn. Locking is per page and not counted, so unlocking one buffer can unlock a
//! neighbour that shares its page; that is within best effort.
use crate::custody::sys;
use zeroize::Zeroize;

/// Locks the pages under `bytes` into memory.
pub fn lock_memory(bytes: &[u8]) -> std::io::Result<()> {
	sys::mlock(bytes)
}

/// Unlocks the pages under `bytes`. Best effort; never fails.
pub fn unlock_memory(bytes: &[u8]) {
	sys::munlock(bytes)
}

/// A secret of at most `capacity` bytes. The allocation is made once and never grows, so no copy
/// of the secret is left behind by a reallocation. Dropping it zeroes the whole allocation, then
/// unlocks it. `Debug` shows only the length, capacity and lock state.
pub struct SecretBuf {
	bytes: Box<[u8]>,
	len: usize,
	locked: bool,
}

/// The buffer is full; the byte was not added.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct CapacityExceeded;

impl SecretBuf {
	/// An empty buffer that can hold `capacity` bytes.
	pub fn with_capacity(capacity: usize) -> Self {
		let bytes = vec![0_u8; capacity].into_boxed_slice();
		let locked = lock_memory(&bytes).is_ok();
		Self {
			bytes,
			len: 0,
			locked,
		}
	}

	/// A buffer of exactly `len` zero bytes, to be filled through [`Self::as_mut_slice`] (for
	/// example with a generated seed).
	pub fn zeroed(len: usize) -> Self {
		let mut buf = Self::with_capacity(len);
		buf.len = len;
		buf
	}

	pub fn as_slice(&self) -> &[u8] {
		&self.bytes[..self.len]
	}

	pub fn as_mut_slice(&mut self) -> &mut [u8] {
		&mut self.bytes[..self.len]
	}

	pub fn len(&self) -> usize {
		self.len
	}

	pub fn is_empty(&self) -> bool {
		self.len == 0
	}

	pub fn capacity(&self) -> usize {
		self.bytes.len()
	}

	/// Whether `mlock` succeeded for this buffer.
	pub fn is_locked(&self) -> bool {
		self.locked
	}

	pub fn push(&mut self, byte: u8) -> Result<(), CapacityExceeded> {
		let slot = self.bytes.get_mut(self.len).ok_or(CapacityExceeded)?;
		*slot = byte;
		self.len += 1;
		Ok(())
	}

	/// Removes the last UTF-8 character (or the last byte, if the tail is not UTF-8), zeroing it.
	pub fn pop_char(&mut self) {
		while self.len > 0 {
			self.len -= 1;
			let byte = std::mem::take(&mut self.bytes[self.len]);
			if byte & 0b1100_0000 != 0b1000_0000 {
				break;
			}
		}
	}

	/// Zeroes the contents and empties the buffer.
	pub fn clear(&mut self) {
		self.bytes.zeroize();
		self.len = 0;
	}
}

impl std::fmt::Debug for SecretBuf {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.debug_struct("SecretBuf")
			.field("len", &self.len)
			.field("capacity", &self.bytes.len())
			.field("locked", &self.locked)
			.finish()
	}
}

impl Drop for SecretBuf {
	fn drop(&mut self) {
		#[cfg(test)]
		probe::observe(probe::Stage::BeforeWipe, &self.bytes);
		self.bytes.zeroize();
		self.len = 0;
		#[cfg(test)]
		probe::observe(probe::Stage::AfterWipe, &self.bytes);
		if self.locked {
			unlock_memory(&self.bytes);
		}
	}
}

/// T-K3z: a per-thread record of what the allocation held just before and just after the wipe,
/// taken inside `drop` while the allocation is still live.
#[cfg(test)]
pub(crate) mod probe {
	use std::cell::RefCell;

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	pub(crate) enum Stage {
		BeforeWipe,
		AfterWipe,
	}

	thread_local! {
		static SEEN: RefCell<Option<Vec<(Stage, Vec<u8>)>>> = const { RefCell::new(None) };
	}

	pub(crate) fn observe(stage: Stage, bytes: &[u8]) {
		SEEN.with(|seen| {
			if let Some(seen) = seen.borrow_mut().as_mut() {
				seen.push((stage, bytes.to_vec()));
			}
		});
	}

	/// Runs `f` with the probe on, returning everything it saw.
	pub(crate) fn record(f: impl FnOnce()) -> Vec<(Stage, Vec<u8>)> {
		SEEN.with(|seen| *seen.borrow_mut() = Some(Vec::new()));
		f();
		SEEN.with(|seen| seen.borrow_mut().take().unwrap_or_default())
	}
}
