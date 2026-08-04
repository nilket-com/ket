use super::message::PlotPaneMsg;
use crate::plot::colors;
use crate::plot::common::{PlotKernel, ResolvedPlotSettings};
use crate::plot::composition::PlotComposition;
use crate::plot::core::PlotType;
use crate::plot::spec::Override;
use crate::plot::theme::{PlotSettings, PlotThemeSpec};
use iced::{Rectangle, Size};
use std::sync::Arc;

fn default_resolved_plot_settings() -> ResolvedPlotSettings {
	let mut settings = PlotSettings::default();
	settings.application_theme.theme = Override::Set(PlotThemeSpec::default());
	settings.resolve()
}

/// Plot state owned by an ordinary host panel. Unlike `PlotPaneState`, this
/// type has no authored composition or layer-editing model: Rune supplies an
/// already-built kernel and a faithful display label.
#[cfg(feature = "toron")]
pub struct HostPlotState {
	pub display_label: String,
	pub kernel: Arc<dyn PlotKernel + Send + Sync>,
	pub plot_settings: ResolvedPlotSettings,
	pub hovered_info: Option<String>,
	pub render_revision: u64,
	pub resize_render_suspended: bool,
}

#[cfg(feature = "toron")]
impl HostPlotState {
	pub fn new(display_label: String, kernel: Arc<dyn PlotKernel + Send + Sync>) -> Self {
		Self {
			display_label,
			kernel,
			plot_settings: default_resolved_plot_settings(),
			hovered_info: None,
			render_revision: 0,
			resize_render_suspended: false,
		}
	}

	pub fn replace(&mut self, display_label: String, kernel: Arc<dyn PlotKernel + Send + Sync>) {
		self.display_label = display_label;
		self.kernel = kernel;
		self.hovered_info = None;
		self.render_revision = self.render_revision.wrapping_add(1);
	}

	pub fn update(&mut self, message: PlotPaneMsg) {
		if let PlotPaneMsg::UpdateHover(hover) = message {
			self.hovered_info = hover;
		}
	}
}

pub struct PlotPaneState {
	pub kernel: Arc<dyn PlotKernel + Send + Sync>,
	pub hovered_info: Option<String>,
	pub composition: PlotComposition,
	pub kernel_composition: PlotComposition,
	pub plot_settings: ResolvedPlotSettings,
	pub live_updates_enabled: bool,
	pub resize_render_suspended: bool,
	pub render_revision: u64,
	pub last_bounds: Rectangle,
	pub max_legend_rows_input: String,
	pub legend_x_input: String,
	pub legend_y_input: String,
	pub x_rotation_input: String,
	pub x_offset_input: String,
	pub bg_color_input: String,
	pub decoration_color_input: String,
	pub x_min_input: String,
	pub x_max_input: String,
	pub y_min_input: String,
	pub y_max_input: String,
	pub title_input: String,
	pub subtitle_input: String,
	pub x_label_input: String,
	pub y_label_input: String,
	pub title_offset_input: String,
	pub subtitle_offset_input: String,
	pub x_label_padding_input: String,
	pub y_label_padding_input: String,
	pub plot_padding_top_input: String,
	pub plot_padding_bottom_input: String,
	pub plot_padding_left_input: String,
	pub plot_padding_right_input: String,
	pub title_size_input: String,
	pub subtitle_size_input: String,
	pub x_label_size_input: String,
	pub y_label_size_input: String,
	pub x_tick_size_input: String,
	pub y_tick_size_input: String,
	pub legend_size_input: String,
	pub x_ticks_input: String,
	pub y_ticks_input: String,
	pub x_minor_ticks_input: String,
	pub y_minor_ticks_input: String,
	pub x_major_grid_width_input: String,
	pub y_major_grid_width_input: String,
	pub x_minor_grid_width_input: String,
	pub y_minor_grid_width_input: String,
	pub scatter_max_vector_points_input: String,
	pub scatter_downsample_target_input: String,
	pub scatter_raster_threshold_input: String,
	pub settings_open: bool,
}

impl PlotPaneState {
	pub fn with_kernel(
		composition: PlotComposition,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
		width: u32,
		height: u32,
	) -> Self {
		let plot_settings = default_resolved_plot_settings();
		Self {
			kernel,
			hovered_info: None,
			composition: composition.clone(),
			kernel_composition: composition,
			live_updates_enabled: true,
			resize_render_suspended: false,
			last_bounds: Rectangle::with_size(Size::new(width as f32, height as f32)),
			render_revision: 0,
			bg_color_input: colors::color_to_hex(plot_settings.background_color),
			decoration_color_input: colors::color_to_hex(plot_settings.decoration_color),
			x_min_input: String::new(),
			x_max_input: String::new(),
			y_min_input: String::new(),
			y_max_input: String::new(),
			title_input: String::new(),
			subtitle_input: String::new(),
			x_label_input: String::new(),
			y_label_input: String::new(),
			title_offset_input: plot_settings.title_offset.to_string(),
			subtitle_offset_input: plot_settings.subtitle_offset.to_string(),
			x_label_padding_input: plot_settings.x_label_padding.to_string(),
			y_label_padding_input: plot_settings.y_label_padding.to_string(),
			plot_padding_top_input: plot_settings.plot_padding_top.to_string(),
			plot_padding_bottom_input: plot_settings.plot_padding_bottom.to_string(),
			plot_padding_left_input: plot_settings.plot_padding_left.to_string(),
			plot_padding_right_input: plot_settings.plot_padding_right.to_string(),
			title_size_input: plot_settings.title_size.to_string(),
			subtitle_size_input: plot_settings.subtitle_size.to_string(),
			x_label_size_input: plot_settings.x_label_size.to_string(),
			y_label_size_input: plot_settings.y_label_size.to_string(),
			x_tick_size_input: plot_settings.x_tick_size.to_string(),
			y_tick_size_input: plot_settings.y_tick_size.to_string(),
			legend_size_input: plot_settings.legend_size.to_string(),
			x_ticks_input: plot_settings.x_ticks.to_string(),
			y_ticks_input: plot_settings.y_ticks.to_string(),
			x_minor_ticks_input: plot_settings.x_minor_ticks.to_string(),
			y_minor_ticks_input: plot_settings.y_minor_ticks.to_string(),
			x_major_grid_width_input: plot_settings.x_major_grid_width.to_string(),
			y_major_grid_width_input: plot_settings.y_major_grid_width.to_string(),
			x_minor_grid_width_input: plot_settings.x_minor_grid_width.to_string(),
			y_minor_grid_width_input: plot_settings.y_minor_grid_width.to_string(),
			scatter_max_vector_points_input: plot_settings.scatter_max_vector_points.to_string(),
			scatter_downsample_target_input: plot_settings.scatter_downsample_target.to_string(),
			scatter_raster_threshold_input: plot_settings.scatter_raster_threshold.to_string(),
			plot_settings: plot_settings.clone(),
			max_legend_rows_input: plot_settings.max_legend_rows.to_string(),
			legend_x_input: plot_settings.legend_x.to_string(),
			legend_y_input: plot_settings.legend_y.to_string(),
			x_rotation_input: plot_settings.x_label_rotation.to_string(),
			x_offset_input: plot_settings.x_label_offset.to_string(),
			settings_open: false,
		}
	}

	pub fn set_kernel(
		&mut self,
		composition: PlotComposition,
		kernel: Arc<dyn PlotKernel + Send + Sync>,
	) {
		self.kernel = kernel;
		self.composition = composition.clone();
		self.kernel_composition = composition;
		self.hovered_info = None;
		self.render_revision = self.render_revision.wrapping_add(1);
	}

	pub fn selected_plot_type(&self) -> PlotType {
		self.composition.primary_plot_type()
	}

	pub fn update(&mut self, message: PlotPaneMsg) {
		let was_live_updates_enabled = self.live_updates_enabled;
		let manual_apply = matches!(&message, PlotPaneMsg::ApplySettings);
		let requires_redraw = !matches!(
			&message,
			PlotPaneMsg::UpdateHover(_)
				| PlotPaneMsg::UpdateBounds(_)
				| PlotPaneMsg::ToggleSettings
				| PlotPaneMsg::CloseSettings
				| PlotPaneMsg::RefreshData
				| PlotPaneMsg::ApplySettings
		);
		match message {
			PlotPaneMsg::AddLayer(plot_type) => {
				let _ = self.composition.add_layer(plot_type);
			}
			PlotPaneMsg::ApplySettings => {}
			PlotPaneMsg::RefreshData => {}
			PlotPaneMsg::UpdateHover(hover) => self.hovered_info = hover,
			PlotPaneMsg::UpdateBounds(bounds) => self.last_bounds = bounds,
			PlotPaneMsg::ChangePlotType(new_type) => {
				if new_type != self.selected_plot_type() {
					self.composition.set_single(new_type);
				}
			}
			PlotPaneMsg::RemoveLayer(index) => {
				let _ = self.composition.remove_layer(index);
			}
			PlotPaneMsg::SetMaxLegendRows(rows) => {
				self.plot_settings.max_legend_rows = rows;
				self.max_legend_rows_input = rows.to_string();
			}
			PlotPaneMsg::SetScatterRenderMode(mode) => {
				self.plot_settings.scatter_render_mode = mode
			}
			PlotPaneMsg::SetScatterMaxVectorPoints(points) => {
				self.plot_settings.scatter_max_vector_points = points.max(1);
				self.scatter_max_vector_points_input = points.to_string();
			}
			PlotPaneMsg::SetScatterDownsampleTarget(points) => {
				self.plot_settings.scatter_downsample_target = points.max(1);
				self.scatter_downsample_target_input = points.to_string();
			}
			PlotPaneMsg::SetScatterRasterThreshold(points) => {
				self.plot_settings.scatter_raster_threshold = points.max(1);
				self.scatter_raster_threshold_input = points.to_string();
			}
			PlotPaneMsg::SetLegendX(x) => {
				self.plot_settings.legend_x = x.clamp(0.0, 1.0);
				self.legend_x_input = x.to_string();
			}
			PlotPaneMsg::SetLegendY(y) => {
				self.plot_settings.legend_y = y.clamp(0.0, 1.0);
				self.legend_y_input = y.to_string();
			}
			PlotPaneMsg::SetXRotation(deg) => {
				self.plot_settings.x_label_rotation = deg;
				self.x_rotation_input = deg.to_string();
			}
			PlotPaneMsg::SetXOffset(offset) => {
				self.plot_settings.x_label_offset = offset;
				self.x_offset_input = offset.to_string();
			}
			PlotPaneMsg::ChangeColorTheme(theme) => self.plot_settings.color_theme = theme,
			PlotPaneMsg::ChangeBackgroundColor(color) => {
				self.plot_settings.background_color = color;
				self.bg_color_input = colors::color_to_hex(color);
				self.plot_settings.decoration_color = colors::contrast_color(color);
				self.decoration_color_input =
					colors::color_to_hex(self.plot_settings.decoration_color);
			}
			PlotPaneMsg::ChangeBackgroundHex(hex) => {
				self.bg_color_input = hex.clone();
				if let Some(color) = colors::hex_to_color(&hex) {
					self.plot_settings.background_color = color;
					self.plot_settings.decoration_color = colors::contrast_color(color);
					self.decoration_color_input =
						colors::color_to_hex(self.plot_settings.decoration_color);
				}
			}
			PlotPaneMsg::ChangeDecorationColor(color) => {
				self.plot_settings.decoration_color = color;
				self.decoration_color_input = colors::color_to_hex(color);
			}
			PlotPaneMsg::ChangeDecorationHex(hex) => {
				self.decoration_color_input = hex.clone();
				if let Some(color) = colors::hex_to_color(&hex) {
					self.plot_settings.decoration_color = color;
				}
			}
			PlotPaneMsg::SetXMin(val) => {
				self.plot_settings.x_min = val;
				self.x_min_input = val.map(|v| v.to_string()).unwrap_or_default();
			}
			PlotPaneMsg::SetXMax(val) => {
				self.plot_settings.x_max = val;
				self.x_max_input = val.map(|v| v.to_string()).unwrap_or_default();
			}
			PlotPaneMsg::SetYMin(val) => {
				self.plot_settings.y_min = val;
				self.y_min_input = val.map(|v| v.to_string()).unwrap_or_default();
			}
			PlotPaneMsg::SetYMax(val) => {
				self.plot_settings.y_max = val;
				self.y_max_input = val.map(|v| v.to_string()).unwrap_or_default();
			}
			PlotPaneMsg::SetTitle(val) => {
				self.plot_settings.title = val.as_ref().map(|s| Arc::new(s.clone()));
				self.title_input = val.unwrap_or_default();
			}
			PlotPaneMsg::SetSubtitle(val) => {
				self.plot_settings.subtitle = val.as_ref().map(|s| Arc::new(s.clone()));
				self.subtitle_input = val.unwrap_or_default();
			}
			PlotPaneMsg::SetXLabel(val) => {
				self.plot_settings.x_label = val.as_ref().map(|s| Arc::new(s.clone()));
				self.x_label_input = val.unwrap_or_default();
			}
			PlotPaneMsg::SetYLabel(val) => {
				self.plot_settings.y_label = val.as_ref().map(|s| Arc::new(s.clone()));
				self.y_label_input = val.unwrap_or_default();
			}
			PlotPaneMsg::SetTitleOffset(val) => {
				self.plot_settings.title_offset = val;
				self.title_offset_input = val.to_string();
			}
			PlotPaneMsg::SetSubtitleOffset(val) => {
				self.plot_settings.subtitle_offset = val;
				self.subtitle_offset_input = val.to_string();
			}
			PlotPaneMsg::SetXLabelPadding(val) => {
				self.plot_settings.x_label_padding = val;
				self.x_label_padding_input = val.to_string();
			}
			PlotPaneMsg::SetYLabelPadding(val) => {
				self.plot_settings.y_label_padding = val;
				self.y_label_padding_input = val.to_string();
			}
			PlotPaneMsg::SetPlotPaddingTop(val) => {
				self.plot_settings.plot_padding_top = val;
				self.plot_padding_top_input = val.to_string();
			}
			PlotPaneMsg::SetPlotPaddingBottom(val) => {
				self.plot_settings.plot_padding_bottom = val;
				self.plot_padding_bottom_input = val.to_string();
			}
			PlotPaneMsg::SetPlotPaddingLeft(val) => {
				self.plot_settings.plot_padding_left = val;
				self.plot_padding_left_input = val.to_string();
			}
			PlotPaneMsg::SetPlotPaddingRight(val) => {
				self.plot_settings.plot_padding_right = val;
				self.plot_padding_right_input = val.to_string();
			}
			PlotPaneMsg::SetTitleSize(val) => {
				self.plot_settings.title_size = val;
				self.title_size_input = val.to_string();
			}
			PlotPaneMsg::SetSubtitleSize(val) => {
				self.plot_settings.subtitle_size = val;
				self.subtitle_size_input = val.to_string();
			}
			PlotPaneMsg::SetXLabelSize(val) => {
				self.plot_settings.x_label_size = val;
				self.x_label_size_input = val.to_string();
			}
			PlotPaneMsg::SetYLabelSize(val) => {
				self.plot_settings.y_label_size = val;
				self.y_label_size_input = val.to_string();
			}
			PlotPaneMsg::SetXTickSize(val) => {
				self.plot_settings.x_tick_size = val;
				self.x_tick_size_input = val.to_string();
			}
			PlotPaneMsg::SetYTickSize(val) => {
				self.plot_settings.y_tick_size = val;
				self.y_tick_size_input = val.to_string();
			}
			PlotPaneMsg::SetLegendSize(val) => {
				self.plot_settings.legend_size = val;
				self.legend_size_input = val.to_string();
			}
			PlotPaneMsg::SetXTicks(val) => {
				self.plot_settings.x_ticks = val;
				self.x_ticks_input = val.to_string();
			}
			PlotPaneMsg::SetYTicks(val) => {
				self.plot_settings.y_ticks = val;
				self.y_ticks_input = val.to_string();
			}
			PlotPaneMsg::ToggleLiveUpdates(val) => self.live_updates_enabled = val,
			PlotPaneMsg::SetXMinorTicks(val) => {
				self.plot_settings.x_minor_ticks = val;
				self.x_minor_ticks_input = val.to_string();
			}
			PlotPaneMsg::SetYMinorTicks(val) => {
				self.plot_settings.y_minor_ticks = val;
				self.y_minor_ticks_input = val.to_string();
			}
			PlotPaneMsg::ToggleXMinorTicks(val) => self.plot_settings.show_x_minor_ticks = val,
			PlotPaneMsg::ToggleYMinorTicks(val) => self.plot_settings.show_y_minor_ticks = val,
			PlotPaneMsg::ToggleXMajorGrid(val) => self.plot_settings.show_x_major_grid = val,
			PlotPaneMsg::ToggleYMajorGrid(val) => self.plot_settings.show_y_major_grid = val,
			PlotPaneMsg::ToggleXMinorGrid(val) => self.plot_settings.show_x_minor_grid = val,
			PlotPaneMsg::ToggleYMinorGrid(val) => self.plot_settings.show_y_minor_grid = val,
			PlotPaneMsg::SetXMajorGridWidth(val) => {
				self.plot_settings.x_major_grid_width = val;
				self.x_major_grid_width_input = val.to_string();
			}
			PlotPaneMsg::SetYMajorGridWidth(val) => {
				self.plot_settings.y_major_grid_width = val;
				self.y_major_grid_width_input = val.to_string();
			}
			PlotPaneMsg::SetXMinorGridWidth(val) => {
				self.plot_settings.x_minor_grid_width = val;
				self.x_minor_grid_width_input = val.to_string();
			}
			PlotPaneMsg::SetYMinorGridWidth(val) => {
				self.plot_settings.y_minor_grid_width = val;
				self.y_minor_grid_width_input = val.to_string();
			}
			PlotPaneMsg::SetXMajorGridStyle(val) => self.plot_settings.x_major_grid_style = val,
			PlotPaneMsg::SetYMajorGridStyle(val) => self.plot_settings.y_major_grid_style = val,
			PlotPaneMsg::SetXMinorGridStyle(val) => self.plot_settings.x_minor_grid_style = val,
			PlotPaneMsg::SetYMinorGridStyle(val) => self.plot_settings.y_minor_grid_style = val,
			PlotPaneMsg::ToggleSettings => self.settings_open = !self.settings_open,
			PlotPaneMsg::CloseSettings => self.settings_open = false,
		}
		if manual_apply || (requires_redraw && self.live_updates_enabled) {
			self.render_revision = self.render_revision.wrapping_add(1);
		}
		if !was_live_updates_enabled && self.live_updates_enabled {
			self.render_revision = self.render_revision.wrapping_add(1);
		}
	}
}

#[cfg(all(test, feature = "toron"))]
mod tests {
	use super::HostPlotState;
	use crate::plot::factory::create_layered_plot;
	use crate::plot::spec::{LayerMark, LayerSpec, LayeredPlotSpec};

	#[test]
	fn product_0014_host_plot_state_cannot_carry_a_plot_composition() {
		let frame =
			polars::df!("x" => [1.0_f64, 2.0], "y" => [2.0_f64, 4.0]).expect("fixture frame");
		let kernel = create_layered_plot(
			&LayeredPlotSpec::new(LayerSpec::xy(LayerMark::Line, "x", "y")),
			&frame,
			1200,
			720,
		)
		.expect("one-layer kernel");
		let state = HostPlotState::new("Line".to_owned(), kernel);
		let HostPlotState {
			display_label,
			kernel: _,
			plot_settings: _,
			hovered_info,
			render_revision,
			resize_render_suspended,
		} = state;
		assert_eq!(display_label, "Line");
		assert_eq!(hovered_info, None);
		assert_eq!(render_revision, 0);
		assert!(!resize_render_suspended);
	}
}
