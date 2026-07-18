use super::message::MessageLog;
use super::registration::SourceStaging;
use crate::core::RegistryOwnerId;
use crate::ecs::UastRegistry;
use crate::engine::event_subscribers::SubscriberContext;
use crate::engine::jobs::JobReservation;
use crate::engine::language::LanguageRegistry;
use crate::engine::session::EditorSession;
use crate::svp::SvpResolver;
use rune::Any;
use rune::runtime::Function;
use std::ops::{Deref, DerefMut};
use std::path::PathBuf;
use std::sync::{Arc, Mutex, MutexGuard};

pub(crate) const DEFAULT_HOST_CALL_QUOTA: usize = 100_000;
pub(crate) const DEFAULT_HOST_READ_BYTE_QUOTA: usize = 32 * 1024 * 1024;
pub(crate) const DEFAULT_HOST_EDIT_BYTE_QUOTA: usize = 16 * 1024 * 1024;
const MAX_DEFERRED_LIFECYCLE_ACTIONS: usize = 64;
const MAX_DEFERRED_PATH_BYTES: usize = 32 * 1024;
const MAX_DEFERRED_PATH_TOTAL_BYTES: usize = 64 * 1024;
pub(crate) const MAX_DEFERRED_RUNE_JOBS: usize = 16;
pub(crate) const MAX_DEFERRED_RUNE_JOB_METADATA_BYTES: usize = 128 * 1024;

pub(crate) struct DeferredRuneJob {
	pub(crate) reservation: JobReservation,
	pub(crate) completion: Function,
}

pub(crate) enum DeferredRuneLifecycle {
	Source(PathBuf),
	Unload(PathBuf),
}

#[derive(Debug)]
pub(crate) struct HostBudget {
	remaining_calls: usize,
	remaining_read_bytes: usize,
	remaining_edit_bytes: usize,
}

impl Default for HostBudget {
	fn default() -> Self {
		Self {
			remaining_calls: DEFAULT_HOST_CALL_QUOTA,
			remaining_read_bytes: DEFAULT_HOST_READ_BYTE_QUOTA,
			remaining_edit_bytes: DEFAULT_HOST_EDIT_BYTE_QUOTA,
		}
	}
}

impl HostBudget {
	pub(crate) fn reset(&mut self, quota: usize) {
		self.remaining_calls = quota;
		self.remaining_read_bytes = DEFAULT_HOST_READ_BYTE_QUOTA;
		self.remaining_edit_bytes = DEFAULT_HOST_EDIT_BYTE_QUOTA;
	}

	pub(crate) fn charge(&mut self) -> Result<(), &'static str> {
		if self.remaining_calls == 0 {
			return Err("Rune host-call quota exhausted");
		}
		self.remaining_calls -= 1;
		Ok(())
	}

	fn reserve_bytes(&mut self, kind: HostByteKind, bytes: usize) -> Result<(), &'static str> {
		let (remaining, message) = match kind {
			HostByteKind::Read => (
				&mut self.remaining_read_bytes,
				"Rune host read-byte quota exhausted",
			),
			HostByteKind::Edit => (
				&mut self.remaining_edit_bytes,
				"Rune host edit-byte quota exhausted",
			),
		};
		if bytes > *remaining {
			return Err(message);
		}
		*remaining -= bytes;
		Ok(())
	}

	fn release_bytes(&mut self, kind: HostByteKind, bytes: usize) {
		let (remaining, ceiling) = match kind {
			HostByteKind::Read => (&mut self.remaining_read_bytes, DEFAULT_HOST_READ_BYTE_QUOTA),
			HostByteKind::Edit => (&mut self.remaining_edit_bytes, DEFAULT_HOST_EDIT_BYTE_QUOTA),
		};
		*remaining = remaining.saturating_add(bytes).min(ceiling);
	}
}

#[derive(Debug, Clone, Copy)]
enum HostByteKind {
	Read,
	Edit,
}

pub(crate) struct HostByteReservation {
	services: SharedHostServices,
	kind: HostByteKind,
	reserved: usize,
	active: bool,
}

impl HostByteReservation {
	pub(crate) fn read(services: &SharedHostServices, bytes: usize) -> Result<Self, &'static str> {
		Self::new(services, HostByteKind::Read, bytes)
	}

	pub(crate) fn edit(services: &SharedHostServices, bytes: usize) -> Result<Self, &'static str> {
		Self::new(services, HostByteKind::Edit, bytes)
	}

	fn new(
		services: &SharedHostServices,
		kind: HostByteKind,
		bytes: usize,
	) -> Result<Self, &'static str> {
		lock_services(services).budget.reserve_bytes(kind, bytes)?;
		Ok(Self {
			services: services.clone(),
			kind,
			reserved: bytes,
			active: true,
		})
	}

	pub(crate) fn complete(mut self, actual: usize) -> Result<(), &'static str> {
		if actual > self.reserved {
			let extra = actual - self.reserved;
			lock_services(&self.services)
				.budget
				.reserve_bytes(self.kind, extra)?;
			self.reserved = actual;
		}
		if actual < self.reserved {
			lock_services(&self.services)
				.budget
				.release_bytes(self.kind, self.reserved - actual);
		}
		self.active = false;
		Ok(())
	}
}

impl Drop for HostByteReservation {
	fn drop(&mut self) {
		if self.active {
			lock_services(&self.services)
				.budget
				.release_bytes(self.kind, self.reserved);
		}
	}
}

#[derive(Debug, Default, Clone, Copy, PartialEq, Eq)]
pub(crate) struct HostEffects {
	pub(crate) needs_render: bool,
}

#[derive(Debug, Default)]
pub(crate) struct HostServices {
	pub(crate) messages: MessageLog,
	pub(crate) budget: HostBudget,
}

pub(crate) type SharedHostServices = Arc<Mutex<HostServices>>;

pub(crate) fn lock_services(services: &SharedHostServices) -> MutexGuard<'_, HostServices> {
	match services.lock() {
		Ok(guard) => guard,
		Err(poisoned) => poisoned.into_inner(),
	}
}

#[derive(Any)]
#[rune(item = ::baryon, name = Editor)]
pub(crate) struct EditorHost {
	pub(crate) session: EditorSession,
	pub(crate) services: SharedHostServices,
	pub(crate) language_registry: Option<Arc<LanguageRegistry>>,
	pub(super) registry: Arc<UastRegistry>,
	pub(super) resolver: Arc<SvpResolver>,
	pub(crate) staging: Option<SourceStaging>,
	effects: HostEffects,
	active_callback_owner: Option<RegistryOwnerId>,
	deferred_jobs: Vec<DeferredRuneJob>,
	deferred_job_metadata_bytes: usize,
	deferred_lifecycle: Vec<DeferredRuneLifecycle>,
	deferred_path_bytes: usize,
}

impl EditorHost {
	pub(crate) fn registry(&self) -> &Arc<UastRegistry> {
		&self.registry
	}

	pub(crate) fn new(
		session: EditorSession,
		services: SharedHostServices,
		registry: Arc<UastRegistry>,
		resolver: Arc<SvpResolver>,
	) -> Self {
		Self {
			session,
			services,
			language_registry: None,
			registry,
			resolver,
			staging: None,
			effects: HostEffects::default(),
			active_callback_owner: None,
			deferred_jobs: Vec::new(),
			deferred_job_metadata_bytes: 0,
			deferred_lifecycle: Vec::new(),
			deferred_path_bytes: 0,
		}
	}

	pub(crate) fn set_language_registry(&mut self, registry: Arc<LanguageRegistry>) {
		self.language_registry = Some(registry);
	}

	pub(crate) fn reset_effects(&mut self) {
		self.effects = HostEffects::default();
	}

	pub(crate) fn take_effects(&mut self) -> HostEffects {
		std::mem::take(&mut self.effects)
	}

	pub(crate) fn mark_needs_render(&mut self) {
		self.effects.needs_render = true;
	}

	pub(crate) fn begin_callback(&mut self, owner: RegistryOwnerId) {
		debug_assert!(
			self.active_callback_owner.is_none(),
			"Rune callbacks are non-reentrant"
		);
		self.active_callback_owner = Some(owner);
	}

	pub(crate) fn finish_callback(&mut self) -> (Vec<DeferredRuneLifecycle>, Vec<DeferredRuneJob>) {
		self.active_callback_owner = None;
		self.deferred_path_bytes = 0;
		self.deferred_job_metadata_bytes = 0;
		(
			std::mem::take(&mut self.deferred_lifecycle),
			std::mem::take(&mut self.deferred_jobs),
		)
	}

	pub(crate) fn active_callback_owner(&self) -> Option<RegistryOwnerId> {
		self.active_callback_owner
	}

	pub(crate) fn defer_job(
		&mut self,
		job: DeferredRuneJob,
		metadata_bytes: usize,
	) -> Result<(), &'static str> {
		if self.active_callback_owner.is_none() {
			return Err("Rune jobs are only valid during a callback");
		}
		if self.deferred_jobs.len() >= MAX_DEFERRED_RUNE_JOBS {
			return Err("Rune deferred-job limit is reached");
		}
		let total = self
			.deferred_job_metadata_bytes
			.checked_add(metadata_bytes)
			.ok_or("Rune deferred-job accounting overflowed")?;
		if total > MAX_DEFERRED_RUNE_JOB_METADATA_BYTES {
			return Err("Rune deferred-job metadata limit is reached");
		}
		self.deferred_job_metadata_bytes = total;
		self.deferred_jobs.push(job);
		Ok(())
	}

	pub(crate) fn defer_source_path(
		&mut self,
		path: &str,
		unload: bool,
	) -> Result<(), &'static str> {
		if self.active_callback_owner.is_none() {
			return Err("Rune source lifecycle changes are only valid during a callback");
		}
		if self.deferred_lifecycle.len() >= MAX_DEFERRED_LIFECYCLE_ACTIONS {
			return Err("Rune deferred source-action limit is reached");
		}
		let path_bytes = path.len();
		if path_bytes > MAX_DEFERRED_PATH_BYTES {
			return Err("Rune deferred source path is too long");
		}
		let total = self
			.deferred_path_bytes
			.checked_add(path_bytes)
			.ok_or("Rune deferred source-path accounting overflowed")?;
		if total > MAX_DEFERRED_PATH_TOTAL_BYTES {
			return Err("Rune deferred source-path byte limit is reached");
		}
		self.deferred_path_bytes = total;
		let path = PathBuf::from(path);
		let action = if unload {
			DeferredRuneLifecycle::Unload(path)
		} else {
			DeferredRuneLifecycle::Source(path)
		};
		self.deferred_lifecycle.push(action);
		Ok(())
	}
}

impl Deref for EditorHost {
	type Target = EditorSession;

	fn deref(&self) -> &Self::Target {
		&self.session
	}
}

impl DerefMut for EditorHost {
	fn deref_mut(&mut self) -> &mut Self::Target {
		&mut self.session
	}
}

impl SubscriberContext for EditorHost {
	fn subscriber_session(&self) -> &EditorSession {
		&self.session
	}

	fn subscriber_session_mut(&mut self) -> &mut EditorSession {
		&mut self.session
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn byte_reservations_release_errors_and_charge_only_actual_output() {
		let services = Arc::new(Mutex::new(HostServices::default()));
		{
			let mut service = lock_services(&services);
			service.budget.remaining_read_bytes = 8;
		}

		let reservation = HostByteReservation::read(&services, 6).expect("reserve read bytes");
		reservation.complete(4).expect("finish read bytes");
		assert_eq!(lock_services(&services).budget.remaining_read_bytes, 4);

		{
			let _released = HostByteReservation::read(&services, 4).expect("reserve remaining");
			assert_eq!(lock_services(&services).budget.remaining_read_bytes, 0);
		}
		assert_eq!(lock_services(&services).budget.remaining_read_bytes, 4);
		assert!(HostByteReservation::read(&services, 5).is_err());
		assert_eq!(lock_services(&services).budget.remaining_read_bytes, 4);
	}

	#[test]
	fn resetting_an_entry_resets_all_three_host_quotas() {
		let mut budget = HostBudget::default();
		budget.reset(1);
		assert!(budget.charge().is_ok());
		assert!(budget.charge().is_err());
		assert_eq!(budget.remaining_read_bytes, DEFAULT_HOST_READ_BYTE_QUOTA);
		assert_eq!(budget.remaining_edit_bytes, DEFAULT_HOST_EDIT_BYTE_QUOTA);
	}
}
