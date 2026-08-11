use super::HostPlotState;
use crate::gui::messages::PlotMessage;
use crate::plot::common::{PlotRenderLayer, PlotWidget};
use iced::widget::{canvas, container, stack, text};
use iced::{Background, Element, Fill};

/// Plot content hosted directly by a libaryon panel. Host-panel chrome owns
/// closing and layout, so this surface emits plot interaction only.
pub fn host_view(state: &HostPlotState) -> Element<'_, PlotMessage> {
	let title = state.display_label.clone();
	let data = canvas(PlotWidget {
		kernel: state.kernel.as_ref(),
		title: title.clone(),
		padding: 20.0,
		settings: state.plot_settings.clone(),
		settings_model: state.plot_settings_model.clone(),
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
		settings_model: state.plot_settings_model.clone(),
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
					background: Some(Background::Color(iced::Color {
						a: 0.85,
						..state.plot_settings.surrounding_background_color
					})),
					text_color: Some(state.plot_settings.surrounding_decoration_color),
					..Default::default()
				}),
		);
	}
	container(layers.width(Fill).height(Fill))
		.style(|_| container::Style {
			background: Some(Background::Color(
				state.plot_settings.surrounding_background_color,
			)),
			text_color: Some(state.plot_settings.surrounding_decoration_color),
			..Default::default()
		})
		.into()
}
