//! Session-scoped durable plot artifacts from PRODUCT-0007.
//!
//! Persistence deliberately does not live here. A live artifact re-resolves
//! against every stamped result; a pin retains the completed resolved spec and
//! exact revision, recomputing only viewport layout.

#![allow(dead_code)] // Public session API awaits an accepted durable-plot surface.

use crate::plot::spec::{
	AuthoredPlotSpec, ColorIdentity, ResolvedLayout, ResolvedPlotSpec, StampedResult,
	resolve_channels, resolve_domains,
};
use polars::frame::DataFrame;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct DurablePlotId(u64);

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DurabilityMode {
	Live,
	Pinned { revision: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum DurablePlotStatus {
	Current,
	Stale { revision: u64, reason: String },
	Unverifiable { revision: u64, reason: String },
}

#[derive(Clone, Debug)]
pub enum RevisionObservation {
	Available(StampedResult),
	Unavailable { revision: u64, reason: String },
	Unverifiable { revision: u64, reason: String },
}

#[derive(Clone, Debug)]
pub struct DurablePlotSnapshot {
	pub mode: DurabilityMode,
	pub resolved_spec: ResolvedPlotSpec,
	pub data: Arc<DataFrame>,
	pub status: DurablePlotStatus,
	pub layout: ResolvedLayout,
}

#[derive(Clone, Debug)]
pub struct DurablePlot {
	authored_spec: AuthoredPlotSpec,
	mode: DurabilityMode,
	resolved_spec: ResolvedPlotSpec,
	last_result: StampedResult,
	color_identity: ColorIdentity,
	status: DurablePlotStatus,
}

impl DurablePlot {
	pub fn live(authored_spec: AuthoredPlotSpec, result: StampedResult) -> Result<Self, String> {
		let (resolved_spec, color_identity) =
			resolve(&authored_spec, &result, &ColorIdentity::default())?;
		Ok(Self {
			authored_spec,
			mode: DurabilityMode::Live,
			resolved_spec,
			last_result: result,
			color_identity,
			status: DurablePlotStatus::Current,
		})
	}

	pub fn pinned(
		authored_spec: AuthoredPlotSpec,
		resolved_spec: ResolvedPlotSpec,
		result: StampedResult,
	) -> Result<Self, String> {
		let revision = resolved_spec
			.provenance
			.revision
			.ok_or_else(|| "a pinned plot requires an exact resolved revision".to_owned())?;
		if result.provenance.revision != Some(revision) {
			return Err(
				"the pinned result and resolved specification name different revisions".to_owned(),
			);
		}
		Ok(Self {
			authored_spec,
			mode: DurabilityMode::Pinned { revision },
			resolved_spec,
			last_result: result,
			color_identity: ColorIdentity::default(),
			status: DurablePlotStatus::Current,
		})
	}

	pub fn mode(&self) -> DurabilityMode {
		self.mode
	}

	pub fn observe(&mut self, observation: RevisionObservation) -> Result<(), String> {
		self.observe_with(observation, resolve)
	}

	pub fn snapshot(&self, viewport_width: u32, viewport_height: u32) -> DurablePlotSnapshot {
		DurablePlotSnapshot {
			mode: self.mode,
			resolved_spec: self.resolved_spec.clone(),
			data: Arc::clone(&self.last_result.data),
			status: self.status.clone(),
			layout: ResolvedLayout::from_spec(&self.resolved_spec, viewport_width, viewport_height),
		}
	}

	fn observe_with(
		&mut self,
		observation: RevisionObservation,
		resolver: impl Fn(
			&AuthoredPlotSpec,
			&StampedResult,
			&ColorIdentity,
		) -> Result<(ResolvedPlotSpec, ColorIdentity), String>,
	) -> Result<(), String> {
		match self.mode {
			DurabilityMode::Live => {
				let RevisionObservation::Available(result) = observation else {
					return Err("a live plot requires a returned stamped result".to_owned());
				};
				let (resolved_spec, color_identity) =
					resolver(&self.authored_spec, &result, &self.color_identity)?;
				self.resolved_spec = resolved_spec;
				self.last_result = result;
				self.color_identity = color_identity;
				self.status = DurablePlotStatus::Current;
			}
			DurabilityMode::Pinned { revision } => match observation {
				RevisionObservation::Available(result)
					if result.provenance.revision == Some(revision) =>
				{
					// The exact revision may be fetched again, but its already-resolved
					// presentation is retained across resolver upgrades.
					self.last_result = result;
					self.status = DurablePlotStatus::Current;
				}
				RevisionObservation::Available(_) => {
					// A later current revision is irrelevant to an exact pin.
				}
				RevisionObservation::Unavailable {
					revision: observed,
					reason,
				} if observed == revision => {
					self.status = DurablePlotStatus::Stale { revision, reason };
				}
				RevisionObservation::Unverifiable {
					revision: observed,
					reason,
				} if observed == revision => {
					self.status = DurablePlotStatus::Unverifiable { revision, reason };
				}
				RevisionObservation::Unavailable { .. }
				| RevisionObservation::Unverifiable { .. } => {}
			},
		}
		Ok(())
	}
}

#[derive(Default)]
pub struct SessionPlotArtifacts {
	next_id: u64,
	plots: HashMap<DurablePlotId, DurablePlot>,
}

impl SessionPlotArtifacts {
	pub fn insert(&mut self, plot: DurablePlot) -> DurablePlotId {
		self.next_id = self.next_id.wrapping_add(1).max(1);
		let id = DurablePlotId(self.next_id);
		self.plots.insert(id, plot);
		id
	}

	pub fn get(&self, id: DurablePlotId) -> Option<&DurablePlot> {
		self.plots.get(&id)
	}

	pub fn get_mut(&mut self, id: DurablePlotId) -> Option<&mut DurablePlot> {
		self.plots.get_mut(&id)
	}

	pub fn remove(&mut self, id: DurablePlotId) -> Option<DurablePlot> {
		self.plots.remove(&id)
	}
}

fn resolve(
	authored_spec: &AuthoredPlotSpec,
	result: &StampedResult,
	identity: &ColorIdentity,
) -> Result<(ResolvedPlotSpec, ColorIdentity), String> {
	let binding = resolve_channels(authored_spec, result.data.schema().as_ref())
		.map_err(|error| error.to_string())?;
	resolve_domains(binding, result, identity).map_err(|error| error.to_string())
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::spec::{Channel, PlotProvenance, ScaleFamily};
	use polars::df;
	use std::cell::Cell;

	fn result(revision: u64, x: polars::prelude::Column, y: Vec<i64>) -> StampedResult {
		let height = y.len();
		let frame = DataFrame::new(height, vec![x, polars::prelude::Column::new("y".into(), y)])
			.expect("frame");
		StampedResult {
			data: Arc::new(frame),
			provenance: PlotProvenance {
				source_name: "durable".to_owned(),
				revision: Some(revision),
			},
		}
	}

	fn authored() -> AuthoredPlotSpec {
		AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y")
	}

	#[test]
	fn live_plot_re_resolves_an_integer_x_to_a_string_point_scale() {
		let first = result(
			1,
			polars::prelude::Column::new("x".into(), [1_i64, 2]),
			vec![3, 4],
		);
		let mut plot = DurablePlot::live(authored(), first).expect("live plot");
		assert_eq!(
			plot.snapshot(800, 480)
				.resolved_spec
				.channel(Channel::X)
				.unwrap()
				.binding
				.scale,
			ScaleFamily::Linear
		);
		plot.observe(RevisionObservation::Available(result(
			2,
			polars::prelude::Column::new("x".into(), ["a", "b"]),
			vec![3, 4],
		)))
		.expect("live refresh");
		assert_eq!(
			plot.snapshot(800, 480)
				.resolved_spec
				.channel(Channel::X)
				.unwrap()
				.binding
				.scale,
			ScaleFamily::Point
		);
	}

	#[test]
	fn pinned_plot_keeps_spec_across_viewports_current_revision_and_resolver_drift() {
		let initial = result(
			7,
			polars::prelude::Column::new("x".into(), [1_i64, 2]),
			vec![3, 4],
		);
		let (spec, _) = resolve(&authored(), &initial, &ColorIdentity::default()).unwrap();
		let mut plot = DurablePlot::pinned(authored(), spec.clone(), initial).expect("pin");
		let small = plot.snapshot(320, 200);
		let large = plot.snapshot(1600, 900);
		assert_eq!(small.resolved_spec, large.resolved_spec);
		assert_ne!(small.layout.viewport_width, large.layout.viewport_width);

		plot.observe(RevisionObservation::Available(result(
			8,
			polars::prelude::Column::new("x".into(), ["new"]),
			vec![9],
		)))
		.expect("later current revision is irrelevant");
		assert_eq!(plot.snapshot(800, 480).resolved_spec, spec);

		let calls = Cell::new(0);
		plot.observe_with(
			RevisionObservation::Available(result(
				7,
				polars::prelude::Column::new("x".into(), [1_i64, 2]),
				vec![3, 4],
			)),
			|_, _, _| {
				calls.set(calls.get() + 1);
				Err("changed inference defaults must not run for a pin".to_owned())
			},
		)
		.expect("pin retains resolved spec");
		assert_eq!(calls.get(), 0);
		assert_eq!(plot.snapshot(800, 480).resolved_spec, spec);
	}

	#[test]
	fn pinned_revision_failure_keeps_last_result_and_is_explicitly_attributed() {
		let initial = result(
			41,
			polars::prelude::Column::new("x".into(), [1_i64, 2]),
			vec![3, 4],
		);
		let last_data = Arc::clone(&initial.data);
		let (spec, _) = resolve(&authored(), &initial, &ColorIdentity::default()).unwrap();
		let mut plot = DurablePlot::pinned(authored(), spec, initial).expect("pin");
		plot.observe(RevisionObservation::Unavailable {
			revision: 41,
			reason: "revision was compacted".to_owned(),
		})
		.expect("stale observation");
		let stale = plot.snapshot(800, 480);
		assert_eq!(stale.mode, DurabilityMode::Pinned { revision: 41 });
		assert!(Arc::ptr_eq(&stale.data, &last_data));
		assert_eq!(
			stale.status,
			DurablePlotStatus::Stale {
				revision: 41,
				reason: "revision was compacted".to_owned()
			}
		);
		plot.observe(RevisionObservation::Unverifiable {
			revision: 41,
			reason: "revision proof could not be read".to_owned(),
		})
		.expect("unverifiable observation");
		assert!(matches!(
			plot.snapshot(800, 480).status,
			DurablePlotStatus::Unverifiable { revision: 41, .. }
		));
	}

	#[test]
	fn store_is_session_scoped_and_owns_no_persistence_surface() {
		let initial = result(
			1,
			polars::prelude::Column::new("x".into(), [1_i64]),
			vec![2],
		);
		let mut store = SessionPlotArtifacts::default();
		let id = store.insert(DurablePlot::live(authored(), initial).unwrap());
		assert!(store.get(id).is_some());
		assert_eq!(store.get_mut(id).unwrap().mode(), DurabilityMode::Live);
		assert!(store.remove(id).is_some());
		assert!(store.get(id).is_none());
		let _ = df!("control" => [1_i64]).expect("positive frame control");
	}
}
