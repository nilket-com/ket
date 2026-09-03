use crate::plot::common::{PlotKernel, ResolvedPlotSettings, ViewportMotion, ViewportStamp};
use crate::plot::core::PlotType;
use crate::plot::spec::Override;
use crate::plot::theme::{PlotSettings, PlotSettingsLayer, PlotThemeSpec};
use crate::presentation_registry::PlotSourceAttachment;
use crate::presentation_registry::SettingKey;
use std::collections::HashMap;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum PlotPaneMsg {
	UpdateHover(Option<String>),
	RasterRequested(Vec<crate::plot::raster::ScatterRasterWant>),
	RasterCompleted(crate::plot::raster::RasterCompletion),
	ToggleSettingsInspector,
	SettingChanged(SettingKey, String),
	SettingSubmitted(SettingKey, Option<String>),
	SettingReset(SettingKey),
	CancelSettingPreview,
	ViewportStarted(ViewportStamp),
	ViewportChanged {
		stamp: ViewportStamp,
		domains: crate::plot::theme::PlotDomainOverrides,
		quiet_settle: bool,
	},
	ViewportReset(ViewportStamp),
	ViewportSettled(ViewportStamp),
}

impl PlotPaneMsg {
	pub(crate) fn quiet_settle_stamp(&self) -> Option<ViewportStamp> {
		match self {
			Self::ViewportChanged {
				stamp,
				quiet_settle: true,
				..
			} => Some(*stamp),
			_ => None,
		}
	}
}

fn default_application_plot_settings() -> PlotSettingsLayer {
	let mut settings = PlotSettingsLayer::default();
	settings.theme = Override::Set(PlotThemeSpec::default());
	settings
}

fn host_plot_settings_model(
	application_theme: PlotSettingsLayer,
	authored_spec: PlotSettingsLayer,
) -> PlotSettings {
	PlotSettings {
		application_theme,
		authored_spec,
		..PlotSettings::default()
	}
}

/// Plot state owned by an ordinary host panel. Rune supplies an already-built
/// kernel and faithful display label; libaryon owns its placement and chrome.
pub struct HostPlotState {
	pub display_label: String,
	pub kernel: Arc<dyn PlotKernel + Send + Sync>,
	pub plot_settings: ResolvedPlotSettings,
	pub plot_settings_model: PlotSettings,
	pub hovered_info: Option<String>,
	pub render_revision: u64,
	pub resize_render_suspended: bool,
	pub raster_coordinator: crate::plot::raster::RasterCoordinator,
	pub viewport_motion: ViewportMotion,
	pub viewport_stamp: ViewportStamp,
	pub(crate) settings_inspector_open: bool,
	pub(crate) source_attachment: Option<PlotSourceAttachment>,
	pub(crate) plot_type: Option<PlotType>,
	pub(crate) setting_drafts: HashMap<SettingKey, String>,
	pub(crate) writeback_status: Option<String>,
	authoritative_before_preview: Option<PlotStateSnapshot>,
}

#[derive(Clone)]
struct PlotStateSnapshot {
	display_label: String,
	kernel: Arc<dyn PlotKernel + Send + Sync>,
	plot_settings: ResolvedPlotSettings,
	plot_settings_model: PlotSettings,
	source_attachment: Option<PlotSourceAttachment>,
	plot_type: Option<PlotType>,
}

impl HostPlotState {
	pub(crate) fn writeback_attachment(&self) -> Option<&PlotSourceAttachment> {
		self.authoritative_before_preview
			.as_ref()
			.and_then(|snapshot| snapshot.source_attachment.as_ref())
			.or(self.source_attachment.as_ref())
	}

	pub(crate) fn writeback_settings_model(&self) -> &PlotSettings {
		self.authoritative_before_preview
			.as_ref()
			.map(|snapshot| &snapshot.plot_settings_model)
			.unwrap_or(&self.plot_settings_model)
	}

	pub fn new(
		display_label: String,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		authored_settings: PlotSettingsLayer,
	) -> Self {
		let plot_settings_model =
			host_plot_settings_model(default_application_plot_settings(), authored_settings);
		let plot_settings = plot_settings_model.resolve();
		Self {
			display_label,
			kernel,
			plot_settings,
			plot_settings_model,
			hovered_info: None,
			render_revision: 0,
			resize_render_suspended: false,
			raster_coordinator: crate::plot::raster::RasterCoordinator::new(),
			viewport_motion: ViewportMotion::Settled,
			viewport_stamp: ViewportStamp::mint(),
			settings_inspector_open: false,
			source_attachment: None,
			plot_type: None,
			setting_drafts: HashMap::new(),
			writeback_status: None,
			authoritative_before_preview: None,
		}
	}

	pub(crate) fn new_with_source(
		display_label: String,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		authored_settings: PlotSettingsLayer,
		source_attachment: Option<PlotSourceAttachment>,
		plot_type: Option<PlotType>,
	) -> Self {
		let mut state = Self::new(display_label, kernel, authored_settings);
		state.source_attachment = source_attachment;
		state.plot_type = plot_type;
		state
	}

	pub fn replace(
		&mut self,
		display_label: String,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		authored_settings: PlotSettingsLayer,
	) {
		self.authoritative_before_preview = None;
		self.setting_drafts.clear();
		self.display_label = display_label;
		self.kernel = kernel;
		self.raster_coordinator.invalidate();
		self.viewport_stamp = self.viewport_stamp.next();
		self.viewport_motion = ViewportMotion::Settled;
		self.plot_settings_model =
			host_plot_settings_model(default_application_plot_settings(), authored_settings);
		self.plot_settings = self.plot_settings_model.resolve();
		self.hovered_info = None;
		self.render_revision = self.render_revision.wrapping_add(1);
		self.source_attachment = None;
		self.plot_type = None;
		self.writeback_status = None;
	}

	pub(crate) fn show_source_preview(
		&mut self,
		display_label: String,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		authored_settings: PlotSettingsLayer,
		source_attachment: PlotSourceAttachment,
		plot_type: Option<PlotType>,
	) {
		if self.authoritative_before_preview.is_none() {
			self.authoritative_before_preview = Some(PlotStateSnapshot {
				display_label: self.display_label.clone(),
				kernel: Arc::clone(&self.kernel),
				plot_settings: self.plot_settings.clone(),
				plot_settings_model: self.plot_settings_model.clone(),
				source_attachment: self.source_attachment.clone(),
				plot_type: self.plot_type,
			});
		}
		self.display_label = display_label;
		self.kernel = kernel;
		self.raster_coordinator.invalidate();
		self.viewport_stamp = self.viewport_stamp.next();
		self.viewport_motion = ViewportMotion::Settled;
		self.plot_settings_model =
			host_plot_settings_model(default_application_plot_settings(), authored_settings);
		self.plot_settings = self.plot_settings_model.resolve();
		self.source_attachment = Some(source_attachment);
		self.plot_type = plot_type;
		self.render_revision = self.render_revision.wrapping_add(1);
		self.writeback_status = Some("preview".to_owned());
	}

	pub(crate) fn discard_source_preview(&mut self) {
		let Some(authoritative) = self.authoritative_before_preview.take() else {
			self.setting_drafts.clear();
			self.writeback_status = None;
			return;
		};
		self.display_label = authoritative.display_label;
		self.kernel = authoritative.kernel;
		self.raster_coordinator.invalidate();
		self.viewport_stamp = self.viewport_stamp.next();
		self.viewport_motion = ViewportMotion::Settled;
		self.plot_settings = authoritative.plot_settings;
		self.plot_settings_model = authoritative.plot_settings_model;
		self.source_attachment = authoritative.source_attachment;
		self.plot_type = authoritative.plot_type;
		self.render_revision = self.render_revision.wrapping_add(1);
		self.setting_drafts.clear();
		self.writeback_status = None;
	}

	pub(crate) fn promote_source_preview(
		&mut self,
		new_generation: libaryon::core::StateId,
	) -> bool {
		if self.authoritative_before_preview.take().is_none() {
			self.setting_drafts.clear();
			self.writeback_status = None;
			return false;
		}
		if let Some(attachment) = &mut self.source_attachment {
			attachment.source.document_generation = new_generation;
		} else {
			self.setting_drafts.clear();
			self.writeback_status = None;
			return false;
		}
		// PRODUCT-0150 §10: the preview's viewport is ephemeral too. Promotion
		// acknowledges the candidate source, not any pane-only navigation that
		// happened while inspecting it.
		self.viewport_stamp = self.viewport_stamp.next();
		self.viewport_motion = ViewportMotion::Settled;
		self.plot_settings_model.pane_override.domains = Override::Inherit;
		self.plot_settings = self.plot_settings_model.resolve();
		self.hovered_info = None;
		self.render_revision = self.render_revision.wrapping_add(1);
		self.setting_drafts.clear();
		self.writeback_status = None;
		true
	}

	pub(crate) fn replace_with_source(
		&mut self,
		display_label: String,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		authored_settings: PlotSettingsLayer,
		source_attachment: Option<PlotSourceAttachment>,
		plot_type: Option<PlotType>,
	) {
		self.replace(display_label, kernel, authored_settings);
		self.source_attachment = source_attachment;
		self.plot_type = plot_type;
	}

	pub fn update(&mut self, message: PlotPaneMsg) -> Option<crate::plot::raster::RasterJob> {
		match message {
			PlotPaneMsg::UpdateHover(hover) => self.hovered_info = hover,
			PlotPaneMsg::RasterRequested(wants) => {
				return self.raster_coordinator.request(wants);
			}
			PlotPaneMsg::RasterCompleted(completion) => {
				let update = self.raster_coordinator.complete(completion);
				if update.published {
					self.render_revision = self.render_revision.wrapping_add(1);
				}
				return update.next_job;
			}
			PlotPaneMsg::ToggleSettingsInspector => {
				self.settings_inspector_open = !self.settings_inspector_open;
			}
			PlotPaneMsg::SettingChanged(key, value) => {
				self.setting_drafts.insert(key, value);
				self.writeback_status = Some("previewing…".to_owned());
			}
			PlotPaneMsg::SettingSubmitted(..) => {
				self.writeback_status = Some("committing…".to_owned());
			}
			PlotPaneMsg::SettingReset(key) => {
				self.setting_drafts.remove(&key);
				self.writeback_status = Some("resetting…".to_owned());
			}
			PlotPaneMsg::CancelSettingPreview => {
				self.writeback_status = None;
			}
			PlotPaneMsg::ViewportStarted(stamp) => {
				if stamp == self.viewport_stamp.next() {
					self.viewport_stamp = stamp;
					self.viewport_motion = ViewportMotion::Moving;
					self.hovered_info = None;
					self.render_revision = self.render_revision.wrapping_add(1);
				}
			}
			PlotPaneMsg::ViewportChanged { stamp, domains, .. } => {
				if stamp == self.viewport_stamp.next() {
					self.viewport_stamp = stamp;
					self.plot_settings_model.pane_override.domains = Override::Set(domains);
					self.plot_settings = self.plot_settings_model.resolve();
					self.viewport_motion = ViewportMotion::Moving;
					self.hovered_info = None;
					self.render_revision = self.render_revision.wrapping_add(1);
				}
			}
			PlotPaneMsg::ViewportReset(stamp) => {
				if stamp == self.viewport_stamp.next() {
					self.plot_settings_model.pane_override.domains = Override::Inherit;
					self.plot_settings = self.plot_settings_model.resolve();
					self.viewport_stamp = stamp.next();
					self.viewport_motion = ViewportMotion::Settled;
					self.hovered_info = None;
					self.render_revision = self.render_revision.wrapping_add(1);
				}
			}
			PlotPaneMsg::ViewportSettled(stamp) => {
				if stamp == self.viewport_stamp {
					self.viewport_stamp = stamp.next();
					self.viewport_motion = ViewportMotion::Settled;
					self.render_revision = self.render_revision.wrapping_add(1);
				}
			}
		}
		None
	}
}

#[cfg(test)]
mod product_0150_state_tests {
	use super::*;
	use crate::plot::common::{AxisType, CoordinateTransformer, PlotBackend, PlotHit, PlotLayout};
	use iced::Rectangle;
	use iced::advanced::mouse::Cursor;

	struct Kernel;

	impl PlotKernel for Kernel {
		fn layout(&self, settings: ResolvedPlotSettings) -> PlotLayout {
			PlotLayout::Cartesian {
				x_range: (
					settings.x_min.unwrap_or(0.0),
					settings.x_max.unwrap_or(10.0),
				),
				y_range: (
					settings.y_min.unwrap_or(0.0),
					settings.y_max.unwrap_or(20.0),
				),
				x_axis_type: AxisType::Linear,
				y_axis_type: AxisType::Linear,
				x_temporal_domain: None,
				y_temporal_domain: None,
			}
		}

		fn plot(
			&self,
			_backend: &mut dyn PlotBackend,
			_bounds: Rectangle,
			_transform: &CoordinateTransformer,
			_cursor: Cursor,
			_settings: ResolvedPlotSettings,
		) {
		}

		fn hit(&self, _transform: &CoordinateTransformer, _cursor: Cursor) -> Option<PlotHit> {
			None
		}
	}

	fn state() -> HostPlotState {
		HostPlotState::new(
			"probe".to_owned(),
			Arc::new(Kernel),
			PlotSettingsLayer::default(),
		)
	}

	#[test]
	fn product_0150_matching_settle_consumes_generation_while_stale_settle_is_inert() {
		let mut state = state();
		let initial = state.viewport_stamp;
		let active = initial.next();
		state.update(PlotPaneMsg::ViewportStarted(active));
		assert_eq!(state.viewport_stamp, active);
		assert_eq!(state.viewport_motion, ViewportMotion::Moving);

		state.update(PlotPaneMsg::ViewportSettled(initial));
		assert_eq!(
			state.viewport_stamp, active,
			"stale settle advances nothing"
		);
		assert_eq!(state.viewport_motion, ViewportMotion::Moving);

		state.update(PlotPaneMsg::ViewportSettled(active));
		assert_eq!(state.viewport_stamp, active.next());
		assert_eq!(state.viewport_motion, ViewportMotion::Settled);
	}

	#[test]
	fn product_0150_wheel_quiet_settle_is_newest_wins_and_press_invalidates_it() {
		let mut state = state();
		let first = state.viewport_stamp.next();
		state.update(PlotPaneMsg::ViewportChanged {
			stamp: first,
			domains: crate::plot::theme::PlotDomainOverrides {
				x_min: Some(1.0),
				x_max: Some(9.0),
				y_min: Some(2.0),
				y_max: Some(18.0),
			},
			quiet_settle: true,
		});
		let second = first.next();
		state.update(PlotPaneMsg::ViewportChanged {
			stamp: second,
			domains: crate::plot::theme::PlotDomainOverrides {
				x_min: Some(2.0),
				x_max: Some(8.0),
				y_min: Some(4.0),
				y_max: Some(16.0),
			},
			quiet_settle: true,
		});
		state.update(PlotPaneMsg::ViewportSettled(first));
		assert_eq!(state.viewport_stamp, second);
		assert_eq!(state.viewport_motion, ViewportMotion::Moving);

		let pressed = second.next();
		state.update(PlotPaneMsg::ViewportStarted(pressed));
		state.update(PlotPaneMsg::ViewportSettled(second));
		assert_eq!(state.viewport_stamp, pressed);
		assert_eq!(state.viewport_motion, ViewportMotion::Moving);

		state.update(PlotPaneMsg::ViewportSettled(pressed));
		assert_eq!(state.viewport_stamp, pressed.next());
		assert_eq!(state.viewport_motion, ViewportMotion::Settled);
	}

	#[test]
	fn product_0150_only_the_newest_of_many_wheel_settles_is_consumed() {
		let mut state = state();
		let mut timers = Vec::new();
		for index in 0..8 {
			let stamp = state.viewport_stamp.next();
			state.update(PlotPaneMsg::ViewportChanged {
				stamp,
				domains: crate::plot::theme::PlotDomainOverrides {
					x_min: Some(index as f64),
					x_max: Some(index as f64 + 10.0),
					y_min: Some(0.0),
					y_max: Some(20.0),
				},
				quiet_settle: true,
			});
			timers.push(stamp);
		}
		let newest = *timers.last().expect("wheel timer");
		let revision_before_settles = state.render_revision;
		for stale in &timers[..timers.len() - 1] {
			state.update(PlotPaneMsg::ViewportSettled(*stale));
		}
		assert_eq!(state.viewport_stamp, newest);
		assert_eq!(state.render_revision, revision_before_settles);
		state.update(PlotPaneMsg::ViewportSettled(newest));
		assert_eq!(state.viewport_stamp, newest.next());
		assert_eq!(state.viewport_motion, ViewportMotion::Settled);
		assert_eq!(state.render_revision, revision_before_settles + 1);
		state.update(PlotPaneMsg::ViewportSettled(newest));
		assert_eq!(
			state.render_revision,
			revision_before_settles + 1,
			"even the newest quiet timer is consumed only once"
		);
	}

	#[test]
	fn product_0150_viewport_mutates_only_the_pane_tier_and_replacement_resets_it() {
		let mut state = state();
		let before = state.plot_settings_model.clone();
		let stamp = state.viewport_stamp.next();
		state.update(PlotPaneMsg::ViewportChanged {
			stamp,
			domains: crate::plot::theme::PlotDomainOverrides {
				x_min: Some(2.0),
				x_max: Some(8.0),
				y_min: Some(4.0),
				y_max: Some(16.0),
			},
			quiet_settle: false,
		});
		assert_eq!(
			state.plot_settings_model.library_defaults.x_min,
			before.library_defaults.x_min
		);
		assert_eq!(
			state.plot_settings_model.library_defaults.y_max,
			before.library_defaults.y_max
		);
		assert_eq!(
			state.plot_settings_model.application_theme,
			before.application_theme
		);
		assert_eq!(
			state.plot_settings_model.authored_spec,
			before.authored_spec
		);
		assert_eq!(state.plot_settings_model.layer, before.layer);
		assert!(matches!(
			state.plot_settings_model.pane_override.domains,
			Override::Set(_)
		));

		let stale = state.viewport_stamp;
		state.replace(
			"replacement".to_owned(),
			Arc::new(Kernel),
			PlotSettingsLayer::default(),
		);
		assert_eq!(state.viewport_motion, ViewportMotion::Settled);
		assert!(matches!(
			state.plot_settings_model.pane_override.domains,
			Override::Inherit
		));
		let after_replace = state.viewport_stamp;
		state.update(PlotPaneMsg::ViewportSettled(stale));
		assert_eq!(state.viewport_stamp, after_replace);
	}
}
