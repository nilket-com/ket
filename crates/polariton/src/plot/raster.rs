//! PRODUCT-0149's off-thread scatter-raster coordinator.
//!
//! A coordinator belongs to one interactive plot surface. Pixel ownership is
//! split into addressed producer slots, while execution is serialized per
//! surface and admitted through one process-wide bounded semaphore.

use crate::plot::common::PlotLayout;
use crate::plot::kernels::scatter::{
	ScatterPreparedData, ScatterRasterKey, ScatterRasterSettings, build_raster_rgba_owned,
};
use crate::result_artifact::ArtifactToken;
use iced::advanced::image;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex, OnceLock};
use tokio::sync::Semaphore;

const NO_REQUEST: u64 = 0;

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RasterFieldBinding {
	pub x: String,
	pub y: String,
	pub color: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct RasterSubject {
	pub publication: ArtifactToken,
	pub binding: RasterFieldBinding,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct RasterSlotId(pub Vec<u32>);

impl RasterSlotId {
	pub fn root() -> Self {
		Self(vec![0])
	}

	pub fn child(&self, ordinal: usize) -> Self {
		let mut path = self.0.clone();
		path.push(u32::try_from(ordinal).unwrap_or(u32::MAX));
		Self(path)
	}
}

#[derive(Clone, Debug, PartialEq)]
pub struct RasterJobIdentity {
	pub slot_id: RasterSlotId,
	pub subject: RasterSubject,
	pub key: ScatterRasterKey,
	pub request_id: u64,
}

#[derive(Clone)]
pub struct ScatterRasterWant {
	pub slot_id: RasterSlotId,
	pub subject: RasterSubject,
	pub key: ScatterRasterKey,
	pub prepared_data: Arc<ScatterPreparedData>,
	pub layout: PlotLayout,
	pub settings: ScatterRasterSettings,
}

impl std::fmt::Debug for ScatterRasterWant {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("ScatterRasterWant")
			.field("slot_id", &self.slot_id)
			.field("subject", &self.subject)
			.field("key", &self.key)
			.finish_non_exhaustive()
	}
}

impl ScatterRasterWant {
	fn same_pixels(&self, other: &Self) -> bool {
		self.slot_id == other.slot_id && self.subject == other.subject && self.key == other.key
	}
}

#[derive(Clone)]
pub struct RasterJob {
	pub identity: RasterJobIdentity,
	pub prepared_data: Arc<ScatterPreparedData>,
	pub layout: PlotLayout,
	pub settings: ScatterRasterSettings,
	pub current_slot_request: Arc<AtomicU64>,
}

impl std::fmt::Debug for RasterJob {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		formatter
			.debug_struct("RasterJob")
			.field("identity", &self.identity)
			.finish_non_exhaustive()
	}
}

#[derive(Clone, Debug)]
pub struct RasterCompletion {
	pub identity: RasterJobIdentity,
	pub rgba: Option<Arc<Vec<u8>>>,
	pub image_handle: Option<image::Handle>,
	#[allow(dead_code)] // Production does not expose progress; acceptance gates inspect it.
	pub scanned_points: usize,
}

#[derive(Clone)]
pub struct RasterBitmap {
	pub subject: RasterSubject,
	pub key: ScatterRasterKey,
	pub width: u32,
	pub height: u32,
	pub rgba: Arc<Vec<u8>>,
	pub image_handle: image::Handle,
}

#[derive(Clone)]
struct RasterSlot {
	current_request: Arc<AtomicU64>,
	want: Option<ScatterRasterWant>,
	pending: Option<RasterJob>,
	in_flight: Option<RasterJobIdentity>,
	last_good: Option<RasterBitmap>,
}

impl Default for RasterSlot {
	fn default() -> Self {
		Self {
			current_request: Arc::new(AtomicU64::new(NO_REQUEST)),
			want: None,
			pending: None,
			in_flight: None,
			last_good: None,
		}
	}
}

#[derive(Default)]
struct RasterCoordinatorState {
	next_request_id: u64,
	slots: HashMap<RasterSlotId, RasterSlot>,
	ready: VecDeque<RasterSlotId>,
	executing: Option<RasterSlotId>,
}

#[derive(Default)]
pub struct RasterCoordinator {
	state: Mutex<RasterCoordinatorState>,
}

pub struct RasterCoordinatorUpdate {
	pub published: bool,
	pub next_job: Option<RasterJob>,
}

impl RasterCoordinator {
	pub fn new() -> Self {
		Self::default()
	}

	/// Whether every live want already has the same pixel authority recorded
	/// for its slot. This is the UI-loop half of deduplication: callers consult
	/// it before publishing a message, while `request` repeats the check before
	/// minting for correctness at the mutable boundary.
	pub fn is_current(&self, wants: &[ScatterRasterWant]) -> bool {
		let state = self.state.lock().expect("raster coordinator poisoned");
		wants.iter().all(|want| {
			state.slots.get(&want.slot_id).is_some_and(|slot| {
				slot.want
					.as_ref()
					.is_some_and(|current| current.same_pixels(want))
			})
		})
	}

	/// Deduplicate before minting. Repeating an identical redraw therefore
	/// leaves both the generation and the running job untouched.
	pub fn request(&self, wants: Vec<ScatterRasterWant>) -> Option<RasterJob> {
		let mut state = self.state.lock().expect("raster coordinator poisoned");
		let active = wants
			.iter()
			.map(|want| want.slot_id.clone())
			.collect::<HashSet<_>>();
		state.slots.retain(|slot_id, slot| {
			if active.contains(slot_id) {
				true
			} else {
				slot.current_request.store(NO_REQUEST, Ordering::Release);
				false
			}
		});
		state.ready.retain(|slot_id| active.contains(slot_id));

		for want in wants {
			let slot_id = want.slot_id.clone();
			let duplicate = state.slots.get(&slot_id).is_some_and(|slot| {
				slot.want
					.as_ref()
					.is_some_and(|current| current.same_pixels(&want))
			});
			if duplicate {
				continue;
			}

			state.next_request_id = state.next_request_id.wrapping_add(1).max(1);
			let request_id = state.next_request_id;
			let slot = state.slots.entry(slot_id.clone()).or_default();
			if slot
				.last_good
				.as_ref()
				.is_some_and(|bitmap| bitmap.subject != want.subject)
			{
				slot.last_good = None;
			}
			slot.current_request.store(request_id, Ordering::Release);
			let identity = RasterJobIdentity {
				slot_id: slot_id.clone(),
				subject: want.subject.clone(),
				key: want.key.clone(),
				request_id,
			};
			slot.pending = Some(RasterJob {
				identity,
				prepared_data: Arc::clone(&want.prepared_data),
				layout: want.layout.clone(),
				settings: want.settings.clone(),
				current_slot_request: Arc::clone(&slot.current_request),
			});
			slot.want = Some(want);
			if state.executing.as_ref() != Some(&slot_id) && !state.ready.contains(&slot_id) {
				state.ready.push_back(slot_id);
			}
		}
		Self::take_next_locked(&mut state)
	}

	pub fn complete(&self, completion: RasterCompletion) -> RasterCoordinatorUpdate {
		let mut state = self.state.lock().expect("raster coordinator poisoned");
		if state.executing.as_ref() == Some(&completion.identity.slot_id) {
			state.executing = None;
		}
		let mut published = false;
		if let Some(slot) = state.slots.get_mut(&completion.identity.slot_id) {
			let matches = slot.current_request.load(Ordering::Acquire)
				== completion.identity.request_id
				&& slot.in_flight.as_ref() == Some(&completion.identity)
				&& slot.want.as_ref().is_some_and(|want| {
					want.subject == completion.identity.subject
						&& want.key == completion.identity.key
				});
			slot.in_flight = None;
			if matches
				&& let (Some(rgba), Some(image_handle)) = (completion.rgba, completion.image_handle)
			{
				let width = completion.identity.key.width;
				let height = completion.identity.key.height;
				slot.last_good = Some(RasterBitmap {
					subject: completion.identity.subject,
					key: completion.identity.key,
					width,
					height,
					image_handle,
					rgba,
				});
				published = true;
			}
			if slot.pending.is_some() && !state.ready.contains(&completion.identity.slot_id) {
				state.ready.push_back(completion.identity.slot_id.clone());
			}
		}
		RasterCoordinatorUpdate {
			published,
			next_job: Self::take_next_locked(&mut state),
		}
	}

	pub fn bitmap(&self, slot_id: &RasterSlotId) -> Option<RasterBitmap> {
		self.state
			.lock()
			.expect("raster coordinator poisoned")
			.slots
			.get(slot_id)
			.and_then(|slot| slot.last_good.clone())
	}

	pub fn invalidate(&self) {
		let mut state = self.state.lock().expect("raster coordinator poisoned");
		for slot in state.slots.values() {
			slot.current_request.store(NO_REQUEST, Ordering::Release);
		}
		state.slots.clear();
		state.ready.clear();
		// Keep the execution seat occupied until the abandoned blocking job
		// returns through `complete`. Its slot-local atomic makes it stop at the
		// next checkpoint; releasing the seat here would overlap old and new
		// kernel lifetimes inside one surface.
	}

	fn take_next_locked(state: &mut RasterCoordinatorState) -> Option<RasterJob> {
		if state.executing.is_some() {
			return None;
		}
		while let Some(slot_id) = state.ready.pop_front() {
			let Some(slot) = state.slots.get_mut(&slot_id) else {
				continue;
			};
			let Some(job) = slot.pending.take() else {
				continue;
			};
			slot.in_flight = Some(job.identity.clone());
			state.executing = Some(slot_id);
			return Some(job);
		}
		None
	}
}

fn production_admission_limit() -> usize {
	std::thread::available_parallelism()
		.map(|parallelism| (parallelism.get() / 2).max(1))
		.unwrap_or(1)
}

fn production_admission() -> Arc<Semaphore> {
	static ADMISSION: OnceLock<Arc<Semaphore>> = OnceLock::new();
	Arc::clone(ADMISSION.get_or_init(|| Arc::new(Semaphore::new(production_admission_limit()))))
}

pub async fn run_raster_job(job: RasterJob) -> RasterCompletion {
	run_raster_job_with_admission(job, production_admission()).await
}

#[cfg(test)]
pub(crate) fn run_raster_job_sync(job: RasterJob) -> RasterCompletion {
	let (rgba, scanned_points) = build_raster_rgba_owned(
		&job.prepared_data,
		&job.layout,
		&job.identity.key,
		&job.settings,
		&job.current_slot_request,
		job.identity.request_id,
	);
	completion_from_pixels(job.identity, rgba, scanned_points)
}

async fn run_raster_job_with_admission(
	job: RasterJob,
	admission: Arc<Semaphore>,
) -> RasterCompletion {
	let permit = admission
		.acquire_owned()
		.await
		.expect("raster admission semaphore closed");
	let identity = job.identity.clone();
	let fallback_identity = identity.clone();
	let result = tokio::task::spawn_blocking(move || {
		let _permit = permit;
		let (rgba, scanned_points) = build_raster_rgba_owned(
			&job.prepared_data,
			&job.layout,
			&job.identity.key,
			&job.settings,
			&job.current_slot_request,
			job.identity.request_id,
		);
		completion_from_pixels(job.identity, rgba, scanned_points)
	})
	.await;
	result.unwrap_or(RasterCompletion {
		identity: fallback_identity,
		rgba: None,
		image_handle: None,
		scanned_points: 0,
	})
}

fn completion_from_pixels(
	identity: RasterJobIdentity,
	rgba: Option<Vec<u8>>,
	scanned_points: usize,
) -> RasterCompletion {
	let rgba = rgba.map(Arc::new);
	let image_handle = rgba.as_ref().map(|pixels| {
		image::Handle::from_rgba(
			identity.key.width,
			identity.key.height,
			pixels.as_slice().to_vec(),
		)
	});
	RasterCompletion {
		identity,
		rgba,
		image_handle,
		scanned_points,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{AxisType, PlotLayout};
	use crate::plot::kernels::scatter::prepare_scatter_data;
	use iced::Color;
	use polars::df;

	fn want(slot: RasterSlotId, x_range: (f64, f64), rows: usize) -> ScatterRasterWant {
		let xs = (0..rows).map(|row| row as f64).collect::<Vec<_>>();
		let ys = (0..rows).map(|row| (row % 11) as f64).collect::<Vec<_>>();
		let frame = df!("x" => xs, "y" => ys).expect("raster fixture");
		let prepared_data = Arc::new(prepare_scatter_data(&frame, "", "x", "y"));
		let subject = prepared_data.raster_subject.clone();
		ScatterRasterWant {
			slot_id: slot,
			subject,
			key: ScatterRasterKey {
				width: 32,
				height: 24,
				point_size: 1.0_f32.to_bits(),
				x_range: (x_range.0.to_bits(), x_range.1.to_bits()),
				y_range: (0.0_f64.to_bits(), 10.0_f64.to_bits()),
				background: [0, 0, 0, 255],
				series_colors: vec![[255, 255, 255, 255]],
			},
			prepared_data,
			layout: PlotLayout::Cartesian {
				x_range,
				y_range: (0.0, 10.0),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_temporal_domain: None,
				y_temporal_domain: None,
			},
			settings: ScatterRasterSettings {
				point_radius: 1.0,
				background: Color::BLACK,
				series_colors: vec![Color::WHITE],
			},
		}
	}

	fn completion(job: &RasterJob) -> RasterCompletion {
		completion_with_byte(job, 0)
	}

	fn completion_with_byte(job: &RasterJob, byte: u8) -> RasterCompletion {
		completion_from_pixels(
			job.identity.clone(),
			Some(vec![
				byte;
				(job.identity.key.width * job.identity.key.height * 4)
					as usize
			]),
			0,
		)
	}

	#[test]
	fn product_0149_identical_redraws_deduplicate_before_request_id_minting() {
		let coordinator = RasterCoordinator::new();
		let requested = want(RasterSlotId::root(), (0.0, 10.0), 20);
		let first = coordinator
			.request(vec![requested.clone()])
			.expect("first redraw launches");
		for _ in 0..20 {
			assert!(coordinator.request(vec![requested.clone()]).is_none());
		}
		assert_eq!(first.identity.request_id, 1);
		let update = coordinator.complete(completion(&first));
		assert!(update.published && update.next_job.is_none());
		assert!(coordinator.request(vec![requested]).is_none());
	}

	#[test]
	fn product_0149_publication_matches_slot_subject_key_and_lifetime_id() {
		for mutation in 0..4 {
			let coordinator = RasterCoordinator::new();
			let requested = want(RasterSlotId::root(), (0.0, 10.0), 20);
			let job = coordinator.request(vec![requested]).expect("job");
			let mut done = completion(&job);
			match mutation {
				0 => done.identity.subject.publication = ArtifactToken::mint(),
				1 => done.identity.subject.binding.y = "other_y".to_owned(),
				2 => done.identity.key.width += 1,
				3 => done.identity.request_id += 100,
				_ => unreachable!(),
			}
			assert!(!coordinator.complete(done).published);
			assert!(coordinator.bitmap(&RasterSlotId::root()).is_none());
		}

		let coordinator = RasterCoordinator::new();
		let requested = want(RasterSlotId::root(), (0.0, 10.0), 20);
		let job = coordinator.request(vec![requested]).expect("job");
		assert!(coordinator.complete(completion(&job)).published);
		assert!(coordinator.bitmap(&RasterSlotId::root()).is_some());
	}

	#[test]
	fn product_0149_kernel_replacement_rejects_an_apparently_identical_old_completion() {
		let coordinator = RasterCoordinator::new();
		let requested = want(RasterSlotId::root(), (0.0, 10.0), 20);
		let old = coordinator
			.request(vec![requested.clone()])
			.expect("old job");
		coordinator.invalidate();
		assert!(
			coordinator.request(vec![requested.clone()]).is_none(),
			"the abandoned job keeps the surface execution seat until it returns"
		);
		let update = coordinator.complete(completion(&old));
		assert!(!update.published);
		let fresh = update
			.next_job
			.expect("replacement job follows old completion");
		assert_ne!(old.identity.request_id, fresh.identity.request_id);
		assert_eq!(old.identity.subject, fresh.identity.subject);
		assert_eq!(old.identity.key, fresh.identity.key);
		assert!(coordinator.complete(completion(&fresh)).published);
	}

	#[test]
	fn product_0149_newest_pending_want_is_the_only_one_that_starts() {
		let coordinator = RasterCoordinator::new();
		let first = coordinator
			.request(vec![want(RasterSlotId::root(), (0.0, 10.0), 20)])
			.expect("first");
		assert!(
			coordinator
				.request(vec![want(RasterSlotId::root(), (1.0, 11.0), 20)])
				.is_none()
		);
		assert!(
			coordinator
				.request(vec![want(RasterSlotId::root(), (2.0, 12.0), 20)])
				.is_none()
		);
		let update = coordinator.complete(completion(&first));
		assert!(!update.published);
		let newest = update.next_job.expect("newest pending");
		assert_eq!(newest.identity.request_id, 3);
		assert_eq!(newest.identity.key.x_range.0, 2.0_f64.to_bits());
	}

	#[test]
	fn product_0149_reversed_completion_order_cannot_replace_the_newest_pixels() {
		let coordinator = RasterCoordinator::new();
		let old = coordinator
			.request(vec![want(RasterSlotId::root(), (0.0, 10.0), 20)])
			.expect("old request");
		assert!(
			coordinator
				.request(vec![want(RasterSlotId::root(), (1.0, 11.0), 20)])
				.is_none()
		);
		let update = coordinator.complete(completion_with_byte(&old, 1));
		assert!(!update.published, "superseded completion cannot publish");
		let newest = update.next_job.expect("newest request starts");
		assert!(
			coordinator
				.complete(completion_with_byte(&newest, 2))
				.published
		);

		assert!(
			!coordinator
				.complete(completion_with_byte(&old, 3))
				.published,
			"an older completion arriving after the newest is a total drop"
		);
		let bitmap = coordinator
			.bitmap(&RasterSlotId::root())
			.expect("newest pixels survive");
		assert_eq!(bitmap.rgba[0], 2);
		assert_eq!(bitmap.key, newest.identity.key);
	}

	#[test]
	fn product_0149_many_supersessions_publish_one_final_settled_identity() {
		let coordinator = RasterCoordinator::new();
		let first = coordinator
			.request(vec![want(RasterSlotId::root(), (0.0, 20_000.0), 20_000)])
			.expect("first request");
		let mut final_want = None;
		for start in 1..=8 {
			let requested = want(
				RasterSlotId::root(),
				(start as f64, 20_000.0 + start as f64),
				20_000,
			);
			final_want = Some(requested.clone());
			assert!(coordinator.request(vec![requested]).is_none());
		}
		let mut publications = 0;
		let abandoned = run_raster_job_sync(first);
		assert!(abandoned.rgba.is_none());
		let update = coordinator.complete(abandoned);
		publications += usize::from(update.published);
		let final_job = update.next_job.expect("only final identity starts");
		assert_eq!(final_job.identity.request_id, 9);
		let update = coordinator.complete(run_raster_job_sync(final_job));
		publications += usize::from(update.published);
		assert!(update.next_job.is_none());
		assert_eq!(publications, 1);
		assert!(
			coordinator
				.request(vec![final_want.expect("final want retained exactly")])
				.is_none(),
			"the satisfied final identity stays quiet"
		);
	}

	#[test]
	fn product_0149_cancellation_is_slot_scoped_and_checkpointed() {
		let coordinator = RasterCoordinator::new();
		let slot_a = RasterSlotId(vec![0, 0]);
		let slot_b = RasterSlotId(vec![0, 1]);
		let stable = want(slot_a.clone(), (0.0, 20_000.0), 20_000);
		let first = coordinator
			.request(vec![stable.clone()])
			.expect("stable A starts");
		assert!(
			coordinator
				.request(vec![stable, want(slot_b, (0.0, 10.0), 20)])
				.is_none()
		);
		let completed = run_raster_job_sync(first.clone());
		assert!(completed.rgba.is_some(), "slot B must not cancel slot A");
		assert_eq!(completed.scanned_points, 20_000);

		let coordinator = RasterCoordinator::new();
		let old = coordinator
			.request(vec![want(slot_a.clone(), (0.0, 20_000.0), 20_000)])
			.expect("old A");
		assert!(
			coordinator
				.request(vec![want(slot_a, (1.0, 20_001.0), 20_000)])
				.is_none()
		);
		let cancelled = run_raster_job_sync(old);
		assert!(cancelled.rgba.is_none());
		assert!(cancelled.scanned_points <= 4_096);
	}

	#[test]
	fn product_0149_identical_twin_layers_keep_independent_slots() {
		let coordinator = RasterCoordinator::new();
		let mut second = want(RasterSlotId(vec![0, 1]), (0.0, 10.0), 20);
		let first_want = want(RasterSlotId(vec![0, 0]), (0.0, 10.0), 20);
		second.subject = first_want.subject.clone();
		second.prepared_data = Arc::clone(&first_want.prepared_data);
		let first = coordinator
			.request(vec![first_want, second])
			.expect("first twin");
		let update = coordinator.complete(completion(&first));
		assert!(update.published);
		let second = update.next_job.expect("second twin");
		assert_ne!(first.identity.slot_id, second.identity.slot_id);
		assert_eq!(first.identity.subject, second.identity.subject);
		assert_eq!(first.identity.key, second.identity.key);
		assert!(coordinator.complete(completion(&second)).published);
		assert!(coordinator.bitmap(&first.identity.slot_id).is_some());
		assert!(coordinator.bitmap(&second.identity.slot_id).is_some());
	}

	#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
	async fn product_0149_forced_single_seat_makes_progress_for_two_surfaces() {
		let admission = Arc::new(Semaphore::new(1));
		let first_surface = RasterCoordinator::new();
		let second_surface = RasterCoordinator::new();
		let stable_slot = RasterSlotId(vec![0, 0]);
		let churning_slot = RasterSlotId(vec![0, 1]);
		let stable = want(stable_slot.clone(), (0.0, 5_000.0), 5_000);
		let first = first_surface
			.request(vec![stable.clone()])
			.expect("first surface job");
		for shift in 0..8 {
			assert!(
				first_surface
					.request(vec![
						stable.clone(),
						want(
							churning_slot.clone(),
							(shift as f64, 5_000.0 + shift as f64),
							5_000,
						),
					])
					.is_none()
			);
		}
		let second = second_surface
			.request(vec![want(RasterSlotId::root(), (0.0, 5_000.0), 5_000)])
			.expect("second surface job");
		let (first_done, second_done) = tokio::join!(
			run_raster_job_with_admission(first, Arc::clone(&admission)),
			run_raster_job_with_admission(second, Arc::clone(&admission)),
		);
		assert!(first_done.rgba.is_some() && second_done.rgba.is_some());
		assert_eq!(
			first_done.scanned_points, 5_000,
			"sibling churn never cancels the stable executing slot"
		);
		let first_update = first_surface.complete(first_done);
		assert!(first_update.published);
		assert!(second_surface.complete(second_done).published);
		let newest_sibling = first_update
			.next_job
			.expect("newest sibling follows fairly");
		assert_eq!(newest_sibling.identity.request_id, 9);
		let sibling_done = run_raster_job_with_admission(newest_sibling, admission).await;
		assert!(first_surface.complete(sibling_done).published);
		assert!(first_surface.bitmap(&stable_slot).is_some());
		assert!(first_surface.bitmap(&churning_slot).is_some());
		assert!(second_surface.bitmap(&RasterSlotId::root()).is_some());
	}

	#[test]
	fn product_0149_production_admission_is_half_parallelism_with_one_fallback() {
		let expected = std::thread::available_parallelism()
			.map(|parallelism| (parallelism.get() / 2).max(1))
			.unwrap_or(1);
		assert_eq!(production_admission_limit(), expected);
		assert!(production_admission_limit() >= 1);
	}
}
