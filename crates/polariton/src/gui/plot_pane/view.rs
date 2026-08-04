#[cfg(feature = "toron")]
use super::HostPlotState;
use super::PlotPaneState;
use crate::gui::colors;
use crate::gui::dashboard::DashboardMsg;
use crate::gui::messages::PlotMessage;
use crate::gui::pane_grid::PaneId;
use crate::plot::colors::ColorTheme;
use crate::plot::common::{GridLineStyle, PlotRenderLayer, PlotWidget, ScatterRenderMode};
use crate::plot::core::PlotType;
use iced::widget::{
	button, canvas, checkbox, column, container, opaque, pick_list, row, scrollable, space, stack,
	text, text_input,
};
use iced::{
	Alignment, Background, Center, Color, Element, Fill, Font, Length, Theme, border, font,
};

pub fn view<'a>(id: PaneId, state: &'a PlotPaneState) -> Element<'a, DashboardMsg> {
	let title = state.composition.title();
	let data_canvas: Element<PlotMessage> = canvas(PlotWidget {
		kernel: state.kernel.as_ref(),
		title: title.clone(),
		padding: 20.0,
		settings: state.plot_settings.clone(),
		render_revision: state.render_revision,
		resize_render_suspended: state.resize_render_suspended,
		layer: PlotRenderLayer::Data,
	})
	.width(Fill)
	.height(Fill)
	.into();
	let overlay_canvas: Element<PlotMessage> = canvas(PlotWidget {
		kernel: state.kernel.as_ref(),
		title,
		padding: 20.0,
		settings: state.plot_settings.clone(),
		render_revision: state.render_revision,
		resize_render_suspended: state.resize_render_suspended,
		layer: PlotRenderLayer::OverlayInteractive,
	})
	.width(Fill)
	.height(Fill)
	.into();
	let plot_content: Element<PlotMessage> = stack![data_canvas, overlay_canvas].into();
	let plot_content = plot_content.map(move |pm| DashboardMsg::Plot(id, pm));
	let close_button = container(
		button(
			text("\u{2715}")
				.font(Font {
					weight: font::Weight::Bold,
					..Default::default()
				})
				.size(10)
				.align_y(Center)
				.align_x(Center),
		)
		.width(22)
		.height(18)
		.style(|_theme: &Theme, status: button::Status| match status {
			button::Status::Hovered => button::Style {
				background: Some(Background::Color(colors::BRAND_PURPLE)),
				text_color: colors::TEXT_TITLE_BUTTON_HOVER,
				..button::Style::default()
			},
			_ => button::Style {
				background: Some(Background::Color(Color {
					a: 0.5,
					..Color::BLACK
				})),
				text_color: colors::TEXT_TITLE_BUTTON,
				..button::Style::default()
			},
		})
		.on_press(DashboardMsg::ClosePlot(id)),
	)
	.width(Fill)
	.align_x(iced::alignment::Horizontal::Right);
	let mut main_stack = stack![plot_content, close_button];
	if let Some(info) = &state.hovered_info {
		main_stack =
			main_stack.push(
				container(text(info))
					.padding(6)
					.style(|_| container::Style {
						background: Some(iced::Background::Color(iced::Color {
							a: 0.85,
							..state.plot_settings.background_color
						})),
						border: iced::Border {
							color: iced::Color {
								a: 0.2,
								..state.plot_settings.decoration_color
							},
							width: 1.0,
							radius: 2.0.into(),
						},
						text_color: Some(state.plot_settings.decoration_color),
						..Default::default()
					}),
			);
	}
	if state.settings_open {
		let settings_panel = plot_settings_panel(id, state);
		let modal_overlay = container(opaque(
			row![space::horizontal(), settings_panel].width(Fill),
		))
		.width(Fill)
		.height(Fill)
		.style(|_| container::Style {
			background: Some(Background::Color(Color {
				a: 0.2,
				..Color::BLACK
			})),
			..Default::default()
		});
		main_stack = main_stack.push(modal_overlay);
	}
	container(main_stack.width(Fill).height(Fill))
		.style(|_| container::Style {
			background: Some(Background::Color(state.plot_settings.background_color)),
			text_color: Some(state.plot_settings.decoration_color),
			..Default::default()
		})
		.into()
}

/// Plot content hosted directly by a libaryon panel. Host-panel chrome owns
/// closing and layout, so this surface emits only plot interaction and never a
/// [`DashboardMsg`].
#[cfg(feature = "toron")]
pub fn host_view(state: &HostPlotState) -> Element<'_, PlotMessage> {
	let title = state.display_label.clone();
	let data = canvas(PlotWidget {
		kernel: state.kernel.as_ref(),
		title: title.clone(),
		padding: 20.0,
		settings: state.plot_settings.clone(),
		render_revision: state.render_revision,
		resize_render_suspended: state.resize_render_suspended,
		layer: PlotRenderLayer::Data,
	})
	.width(Fill)
	.height(Fill);
	let interaction = canvas(PlotWidget {
		kernel: state.kernel.as_ref(),
		title,
		padding: 20.0,
		settings: state.plot_settings.clone(),
		render_revision: state.render_revision,
		resize_render_suspended: state.resize_render_suspended,
		layer: PlotRenderLayer::OverlayInteractive,
	})
	.width(Fill)
	.height(Fill);
	let mut layers = stack![data, interaction];
	if let Some(info) = &state.hovered_info {
		layers = layers.push(
			container(text(info))
				.padding(6)
				.style(|_| container::Style {
					background: Some(iced::Background::Color(iced::Color {
						a: 0.85,
						..state.plot_settings.background_color
					})),
					text_color: Some(state.plot_settings.decoration_color),
					..Default::default()
				}),
		);
	}
	container(layers.width(Fill).height(Fill))
		.style(|_| container::Style {
			background: Some(Background::Color(state.plot_settings.background_color)),
			text_color: Some(state.plot_settings.decoration_color),
			..Default::default()
		})
		.into()
}

fn plot_settings_panel<'a>(id: PaneId, state: &'a PlotPaneState) -> Element<'a, DashboardMsg> {
	let plot_event = move |pm| DashboardMsg::Plot(id, pm);
	container(
		column![
			row![
				text("Plot Settings").size(24),
				space::horizontal(),
				button("Refresh Data").on_press(plot_event(PlotMessage::RefreshData)),
				space::horizontal(),
				button("Close").on_press(plot_event(PlotMessage::CloseSettings))
			]
			.align_y(Alignment::Center),
			scrollable(
				column![
					section(
						"General",
						column![
							row![
								checkbox(state.live_updates_enabled)
									.label("Live Updates")
									.on_toggle(move |v| {
										plot_event(PlotMessage::ToggleLiveUpdates(v))
									}),
								space::horizontal(),
								button("Apply").on_press(plot_event(PlotMessage::ApplySettings)),
							]
							.align_y(Alignment::Center),
							horizontal_rule(),
							field(
								"Base Layer",
								pick_list(
									&PlotType::ALL[..],
									Some(state.selected_plot_type()),
									move |pt| plot_event(PlotMessage::ChangePlotType(pt))
								)
							),
							field(
								"Theme",
								pick_list(
									&ColorTheme::ALL[..],
									Some(state.plot_settings.color_theme),
									move |ct| plot_event(PlotMessage::ChangeColorTheme(ct))
								)
							),
							horizontal_rule(),
							field(
								"Scatter Mode",
								pick_list(
									&ScatterRenderMode::ALL[..],
									Some(state.plot_settings.scatter_render_mode),
									move |mode| plot_event(PlotMessage::SetScatterRenderMode(mode))
								)
							),
							field(
								"Vector Limit",
								text_input("", &state.scatter_max_vector_points_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<u32>() {
											plot_event(PlotMessage::SetScatterMaxVectorPoints(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
							field(
								"Sample Target",
								text_input("", &state.scatter_downsample_target_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<u32>() {
											plot_event(PlotMessage::SetScatterDownsampleTarget(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
							field(
								"Raster Threshold",
								text_input("", &state.scatter_raster_threshold_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<u32>() {
											plot_event(PlotMessage::SetScatterRasterThreshold(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
						]
					),
					section("Layers", layer_section(id, state)),
					section(
						"Titles",
						column![
							field(
								"Title",
								text_input("auto", &state.title_input).on_input(move |s| {
									if s.is_empty() {
										plot_event(PlotMessage::SetTitle(None))
									} else {
										plot_event(PlotMessage::SetTitle(Some(s)))
									}
								})
							),
							field(
								"Title Size",
								text_input("", &state.title_size_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetTitleSize(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Title Offset",
								text_input("", &state.title_offset_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetTitleOffset(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Subtitle",
								text_input("none", &state.subtitle_input).on_input(move |s| {
									if s.is_empty() {
										plot_event(PlotMessage::SetSubtitle(None))
									} else {
										plot_event(PlotMessage::SetSubtitle(Some(s)))
									}
								})
							),
							field(
								"Subtitle Size",
								text_input("", &state.subtitle_size_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetSubtitleSize(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Subtitle Offset",
								text_input("", &state.subtitle_offset_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetSubtitleOffset(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
						]
					),
					section(
						"X Axis",
						column![
							field(
								"Label",
								text_input("auto", &state.x_label_input).on_input(move |s| {
									if s.is_empty() {
										plot_event(PlotMessage::SetXLabel(None))
									} else {
										plot_event(PlotMessage::SetXLabel(Some(s)))
									}
								})
							),
							field(
								"Label Size",
								text_input("", &state.x_label_size_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetXLabelSize(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Label Padding",
								text_input("", &state.x_label_padding_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetXLabelPadding(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							horizontal_rule(),
							field(
								"Major Ticks",
								text_input("", &state.x_ticks_input).on_input(move |s| {
									if let Ok(val) = s.parse::<u32>() {
										plot_event(PlotMessage::SetXTicks(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Tick Size",
								text_input("", &state.x_tick_size_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetXTickSize(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							checkbox(state.plot_settings.show_x_minor_ticks)
								.label("Show Minor Ticks")
								.on_toggle(move |v| plot_event(PlotMessage::ToggleXMinorTicks(v))),
							field(
								"Minor Ticks",
								text_input("", &state.x_minor_ticks_input).on_input(move |s| {
									if let Ok(val) = s.parse::<u32>() {
										plot_event(PlotMessage::SetXMinorTicks(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							horizontal_rule(),
							checkbox(state.plot_settings.show_x_major_grid)
								.label("Show Major Grid")
								.on_toggle(move |v| plot_event(PlotMessage::ToggleXMajorGrid(v))),
							field(
								"Grid Width",
								text_input("", &state.x_major_grid_width_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<f32>() {
											plot_event(PlotMessage::SetXMajorGridWidth(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
							field(
								"Grid Style",
								pick_list(
									&GridLineStyle::ALL[..],
									Some(state.plot_settings.x_major_grid_style),
									move |s| plot_event(PlotMessage::SetXMajorGridStyle(s))
								)
							),
							horizontal_rule(),
							checkbox(state.plot_settings.show_x_minor_grid)
								.label("Show Minor Grid")
								.on_toggle(move |v| plot_event(PlotMessage::ToggleXMinorGrid(v))),
							field(
								"Minor Grid Width",
								text_input("", &state.x_minor_grid_width_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<f32>() {
											plot_event(PlotMessage::SetXMinorGridWidth(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
							field(
								"Minor Grid Style",
								pick_list(
									&GridLineStyle::ALL[..],
									Some(state.plot_settings.x_minor_grid_style),
									move |s| plot_event(PlotMessage::SetXMinorGridStyle(s))
								)
							),
						]
						.spacing(10)
					),
					section(
						"Y Axis",
						column![
							field(
								"Label",
								text_input("auto", &state.y_label_input).on_input(move |s| {
									if s.is_empty() {
										plot_event(PlotMessage::SetYLabel(None))
									} else {
										plot_event(PlotMessage::SetYLabel(Some(s)))
									}
								})
							),
							field(
								"Label Size",
								text_input("", &state.y_label_size_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetYLabelSize(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Label Padding",
								text_input("", &state.y_label_padding_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetYLabelPadding(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							horizontal_rule(),
							field(
								"Major Ticks",
								text_input("", &state.y_ticks_input).on_input(move |s| {
									if let Ok(val) = s.parse::<u32>() {
										plot_event(PlotMessage::SetYTicks(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Tick Size",
								text_input("", &state.y_tick_size_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetYTickSize(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							checkbox(state.plot_settings.show_y_minor_ticks)
								.label("Show Minor Ticks")
								.on_toggle(move |v| plot_event(PlotMessage::ToggleYMinorTicks(v))),
							field(
								"Minor Ticks",
								text_input("", &state.y_minor_ticks_input).on_input(move |s| {
									if let Ok(val) = s.parse::<u32>() {
										plot_event(PlotMessage::SetYMinorTicks(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							horizontal_rule(),
							checkbox(state.plot_settings.show_y_major_grid)
								.label("Show Major Grid")
								.on_toggle(move |v| plot_event(PlotMessage::ToggleYMajorGrid(v))),
							field(
								"Grid Width",
								text_input("", &state.y_major_grid_width_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<f32>() {
											plot_event(PlotMessage::SetYMajorGridWidth(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
							field(
								"Grid Style",
								pick_list(
									&GridLineStyle::ALL[..],
									Some(state.plot_settings.y_major_grid_style),
									move |s| plot_event(PlotMessage::SetYMajorGridStyle(s))
								)
							),
							horizontal_rule(),
							checkbox(state.plot_settings.show_y_minor_grid)
								.label("Show Minor Grid")
								.on_toggle(move |v| plot_event(PlotMessage::ToggleYMinorGrid(v))),
							field(
								"Minor Grid Width",
								text_input("", &state.y_minor_grid_width_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<f32>() {
											plot_event(PlotMessage::SetYMinorGridWidth(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
							field(
								"Minor Grid Style",
								pick_list(
									&GridLineStyle::ALL[..],
									Some(state.plot_settings.y_minor_grid_style),
									move |s| plot_event(PlotMessage::SetYMinorGridStyle(s))
								)
							),
						]
						.spacing(10)
					),
					section(
						"Plot Padding",
						column![
							field(
								"Top",
								text_input("", &state.plot_padding_top_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetPlotPaddingTop(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Bottom",
								text_input("", &state.plot_padding_bottom_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<f32>() {
											plot_event(PlotMessage::SetPlotPaddingBottom(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
							field(
								"Left",
								text_input("", &state.plot_padding_left_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetPlotPaddingLeft(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Right",
								text_input("", &state.plot_padding_right_input).on_input(
									move |s| {
										if let Ok(val) = s.parse::<f32>() {
											plot_event(PlotMessage::SetPlotPaddingRight(val))
										} else {
											plot_event(PlotMessage::UpdateHover(
												state.hovered_info.clone(),
											))
										}
									}
								)
							),
						]
					),
					section(
						"Colors",
						column![
							field(
								"Background",
								text_input("", &state.bg_color_input).on_input(move |s| {
									plot_event(PlotMessage::ChangeBackgroundHex(s))
								})
							),
							field(
								"Decoration",
								text_input("", &state.decoration_color_input).on_input(move |s| {
									plot_event(PlotMessage::ChangeDecorationHex(s))
								})
							),
						]
					),
					section(
						"Legend",
						column![
							field(
								"Max Rows",
								text_input("", &state.max_legend_rows_input).on_input(move |s| {
									if let Ok(rows) = s.parse::<u32>() {
										plot_event(PlotMessage::SetMaxLegendRows(rows))
									} else if s.is_empty() {
										plot_event(PlotMessage::SetMaxLegendRows(0))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Legend Size",
								text_input("", &state.legend_size_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f32>() {
										plot_event(PlotMessage::SetLegendSize(val))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"X (0-1)",
								text_input("", &state.legend_x_input).on_input(move |s| {
									if let Ok(x) = s.parse::<f32>() {
										plot_event(PlotMessage::SetLegendX(x))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Y (0-1)",
								text_input("", &state.legend_y_input).on_input(move |s| {
									if let Ok(y) = s.parse::<f32>() {
										plot_event(PlotMessage::SetLegendY(y))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
						]
					),
					section(
						"X Axis Labels",
						column![
							field(
								"Rotation",
								text_input("", &state.x_rotation_input).on_input(move |s| {
									if let Ok(deg) = s.parse::<f32>() {
										plot_event(PlotMessage::SetXRotation(deg))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Offset",
								text_input("", &state.x_offset_input).on_input(move |s| {
									if let Ok(offset) = s.parse::<f32>() {
										plot_event(PlotMessage::SetXOffset(offset))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
						]
					),
					section(
						"X Axis Range",
						column![
							field(
								"Min",
								text_input("auto", &state.x_min_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f64>() {
										plot_event(PlotMessage::SetXMin(Some(val)))
									} else if s.is_empty() {
										plot_event(PlotMessage::SetXMin(None))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Max",
								text_input("auto", &state.x_max_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f64>() {
										plot_event(PlotMessage::SetXMax(Some(val)))
									} else if s.is_empty() {
										plot_event(PlotMessage::SetXMax(None))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
						]
					),
					section(
						"Y Axis Range",
						column![
							field(
								"Min",
								text_input("auto", &state.y_min_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f64>() {
										plot_event(PlotMessage::SetYMin(Some(val)))
									} else if s.is_empty() {
										plot_event(PlotMessage::SetYMin(None))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
							field(
								"Max",
								text_input("auto", &state.y_max_input).on_input(move |s| {
									if let Ok(val) = s.parse::<f64>() {
										plot_event(PlotMessage::SetYMax(Some(val)))
									} else if s.is_empty() {
										plot_event(PlotMessage::SetYMax(None))
									} else {
										plot_event(PlotMessage::UpdateHover(
											state.hovered_info.clone(),
										))
									}
								})
							),
						]
					),
				]
				.spacing(20)
			)
			.direction(scrollable::Direction::Vertical(
				scrollable::Scrollbar::new()
					.width(4)
					.margin(2)
					.scroller_width(4)
			))
			.style(|_theme, _status| scrollable::Style {
				container: container::Style::default(),
				vertical_rail: scrollable::Rail {
					background: Some(Background::Color(Color::TRANSPARENT)),
					border: border::Border::default(),
					scroller: scrollable::Scroller {
						background: Background::Color(colors::SCROLLBAR_THUMB),
						border: border::Border {
							radius: 2.0.into(),
							..Default::default()
						},
					},
				},
				horizontal_rail: scrollable::Rail {
					background: Some(Background::Color(Color::TRANSPARENT)),
					border: border::Border::default(),
					scroller: scrollable::Scroller {
						background: Background::Color(colors::SCROLLBAR_THUMB),
						border: border::Border {
							radius: 2.0.into(),
							..Default::default()
						},
					},
				},
				gap: None,
				auto_scroll: scrollable::AutoScroll {
					background: Background::Color(Color::TRANSPARENT),
					border: border::Border::default(),
					shadow: iced::Shadow::default(),
					icon: Color::TRANSPARENT,
				},
			})
		]
		.spacing(20)
		.padding(20),
	)
	.width(400)
	.height(Fill)
	.style(move |_| container::Style {
		background: Some(Background::Color(Color {
			a: 0.95,
			..state.plot_settings.background_color
		})),
		border: border::Border {
			color: state.plot_settings.decoration_color,
			width: 1.0,
			radius: 0.0.into(),
		},
		text_color: Some(state.plot_settings.decoration_color),
		..Default::default()
	})
	.into()
}

fn layer_section<'a>(id: PaneId, state: &'a PlotPaneState) -> Element<'a, DashboardMsg> {
	let plot_event = move |pm| DashboardMsg::Plot(id, pm);
	let mut rows = column![
		text(format!("Rendered as {}", state.composition.title())).color(Color {
			a: 0.75,
			..state.plot_settings.decoration_color
		})
	]
	.spacing(8);

	for (index, layer) in state.composition.layers.iter().enumerate() {
		let role = if index == 0 { "Base" } else { "Overlay" };
		let action = if index == 0 {
			None
		} else {
			Some(
				button("Remove")
					.on_press(plot_event(PlotMessage::RemoveLayer(index)))
					.into(),
			)
		};
		rows = rows.push(layer_row(index + 1, role, layer.plot_type, action));
	}

	if state.composition.can_add_layer_type(PlotType::Line) {
		rows = rows.push(
			row![
				space::horizontal().width(Length::Fixed(76.0)),
				button("Add Line Overlay")
					.on_press(plot_event(PlotMessage::AddLayer(PlotType::Line))),
			]
			.align_y(Alignment::Center),
		);
	}

	if state.composition.can_add_layer_type(PlotType::Scatter) {
		rows = rows.push(
			row![
				space::horizontal().width(Length::Fixed(76.0)),
				button("Add Scatter Overlay")
					.on_press(plot_event(PlotMessage::AddLayer(PlotType::Scatter))),
			]
			.align_y(Alignment::Center),
		);
	}

	if state.composition.can_add_layer_type(PlotType::FillBetween) {
		rows = rows.push(
			row![
				space::horizontal().width(Length::Fixed(76.0)),
				button("Add Fill Overlay")
					.on_press(plot_event(PlotMessage::AddLayer(PlotType::FillBetween))),
			]
			.align_y(Alignment::Center),
		);
	}

	rows.into()
}

fn layer_row<'a>(
	index: usize,
	role: &'a str,
	plot_type: PlotType,
	action: Option<Element<'a, DashboardMsg>>,
) -> Element<'a, DashboardMsg> {
	let meta = text(format!("{index}. {role}")).size(14).color(Color {
		a: 0.7,
		..colors::TEXT_SECONDARY
	});
	let label = text(plot_type.to_string());
	let controls = action.unwrap_or_else(|| space::horizontal().width(Length::Fixed(1.0)).into());

	row![
		container(meta).width(Length::Fixed(76.0)),
		label.width(Fill),
		controls,
	]
	.align_y(Alignment::Center)
	.spacing(10)
	.into()
}

fn section<'a>(
	title: &'a str,
	content: impl Into<Element<'a, DashboardMsg>>,
) -> Element<'a, DashboardMsg> {
	column![
		text(title).size(18),
		container(content).padding(10).style(|_| container::Style {
			border: border::Border {
				color: Color {
					a: 0.2,
					r: 0.5,
					g: 0.5,
					b: 0.5
				},
				width: 1.0,
				radius: 4.0.into(),
			},
			..Default::default()
		})
	]
	.spacing(8)
	.into()
}

fn field<'a>(
	label: &'a str,
	widget: impl Into<Element<'a, DashboardMsg>>,
) -> Element<'a, DashboardMsg> {
	row![text(label).width(Length::Fixed(160.0)), widget.into()]
		.spacing(10)
		.align_y(Alignment::Center)
		.into()
}

fn horizontal_rule<'a>() -> Element<'a, DashboardMsg> {
	container(row![].width(Fill).height(1))
		.style(|_| container::Style {
			background: Some(Background::Color(Color {
				a: 0.1,
				..Color::WHITE
			})),
			..Default::default()
		})
		.into()
}
