use crate::plot::common::{PlotKernel, ResolvedPlotSettings};
use crate::plot::core::PlotType;
use crate::plot::spec::Override;
use crate::plot::theme::{PlotSettings, PlotSettingsLayer, PlotThemeSpec};
use crate::presentation_registry::PlotSourceAttachment;
use std::sync::Arc;

#[derive(Clone, Debug)]
pub enum PlotPaneMsg {
	UpdateHover(Option<String>),
	ToggleSettingsInspector,
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
	pub(crate) settings_inspector_open: bool,
	pub(crate) source_attachment: Option<PlotSourceAttachment>,
	pub(crate) plot_type: Option<PlotType>,
}

impl HostPlotState {
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
			settings_inspector_open: false,
			source_attachment: None,
			plot_type: None,
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
		self.display_label = display_label;
		self.kernel = kernel;
		self.plot_settings_model =
			host_plot_settings_model(default_application_plot_settings(), authored_settings);
		self.plot_settings = self.plot_settings_model.resolve();
		self.hovered_info = None;
		self.render_revision = self.render_revision.wrapping_add(1);
		self.source_attachment = None;
		self.plot_type = None;
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

	pub fn update(&mut self, message: PlotPaneMsg) {
		match message {
			PlotPaneMsg::UpdateHover(hover) => self.hovered_info = hover,
			PlotPaneMsg::ToggleSettingsInspector => {
				self.settings_inspector_open = !self.settings_inspector_open;
			}
		}
	}
}
