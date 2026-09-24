//! PRODUCT-0160: state shared by the batch driver and a live connection.
//!
//! Baselines belong to figure incarnations, not to clients. Capture only
//! once both the installed kernel and its projected plot area are present.

use super::AppState;
use crate::driver::{FigureDescription, LiveState, describe_export, live_state_of};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct Surface {
	baselines: HashMap<u64, LiveState>,
	pub(super) known: HashMap<u64, u64>,
}

impl AppState {
	pub(super) fn capture_surface_baselines(&mut self) {
		let current = self.code_editor.host_plot_incarnations();
		self.surface
			.baselines
			.retain(|token, _| current.contains(token));
		for (panel, incarnation, export) in self.code_editor.host_plot_exports_with_identity() {
			// A baseline is captured once, on the incarnation's first appearance,
			// which the attribution map records. An incarnation seen before but
			// holding no baseline lost it, and a later pass must not mint one
			// from what is live now: that would present an edit as load-time state.
			let first_seen = self
				.surface
				.known
				.insert(incarnation.token(), panel.get())
				.is_none();
			if first_seen {
				self.surface
					.baselines
					.insert(incarnation.token(), live_state_of(&export));
			}
		}
	}

	pub(super) fn describe_surface(&self) -> Vec<FigureDescription> {
		self.code_editor
			.host_plot_exports_with_identity()
			.iter()
			.map(|(panel, incarnation, export)| {
				let mut description = describe_export(*panel, *incarnation, export);
				// A projected figure has a baseline after every update; if one is
				// ever missing the figure says so, and no client mints one.
				description.loaded = self.surface.baselines.get(&incarnation.token()).cloned();
				description
			})
			.collect()
	}
}

#[cfg(all(test, feature = "toron"))]
impl AppState {
	/// Break the baseline invariant on purpose, to prove a describe survives it.
	pub(super) fn forget_baseline_for_test(&mut self, token: u64) -> bool {
		self.surface.baselines.remove(&token).is_some()
	}
}
