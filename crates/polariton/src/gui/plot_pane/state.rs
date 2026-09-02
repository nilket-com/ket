use crate::plot::common::{PlotKernel, ResolvedPlotSettings};
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
	SettingSubmitted(SettingKey),
	SettingReset(SettingKey),
	CancelSettingPreview,
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
			PlotPaneMsg::SettingSubmitted(_) => {
				self.writeback_status = Some("committing…".to_owned());
			}
			PlotPaneMsg::SettingReset(key) => {
				self.setting_drafts.remove(&key);
				self.writeback_status = Some("resetting…".to_owned());
			}
			PlotPaneMsg::CancelSettingPreview => {
				self.writeback_status = None;
			}
		}
		None
	}
}
