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
			self.surface.known.insert(incarnation.token(), panel.get());
			self.surface
				.baselines
				.entry(incarnation.token())
				.or_insert_with(|| live_state_of(&export));
		}
	}

	pub(super) fn describe_surface(&self) -> Vec<FigureDescription> {
		self.code_editor
			.host_plot_exports_with_identity()
			.iter()
			.map(|(panel, incarnation, export)| {
				let mut description = describe_export(*panel, *incarnation, export);
				description.loaded = self
					.surface
					.baselines
					.get(&incarnation.token())
					.cloned()
					.expect("projected figures have a load baseline after update");
				description
			})
			.collect()
	}
}
