use super::HostPlotState;
use crate::gui::messages::PlotMessage;
use crate::plot::common::{PlotRenderLayer, PlotWidget, ResolvedPlotSettings};
use iced::widget::{canvas, container, stack, text};
use iced::{Background, Element, Fill, Pixels};

/// PRODUCT-0112's sole conversion from resolved hover typography to iced.
/// The canvas never consumes this value.
fn hover_text_pixels(settings: &ResolvedPlotSettings) -> Pixels {
	Pixels(settings.hover_size.points())
}

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
			container(text(info).size(hover_text_pixels(&state.plot_settings)))
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

#[cfg(test)]
mod tests {
	use super::hover_text_pixels;
	use crate::plot::common::ResolvedPlotSettings;
	use crate::plot::spec::HoverTextSize;

	#[test]
	fn product_0112_hover_overlay_uses_the_resolved_size_once() {
		let mut settings = ResolvedPlotSettings::default();
		assert_eq!(
			hover_text_pixels(&settings),
			iced::Settings::default().default_text_size,
			"the library default inherits iced's existing default rather than restating it"
		);
		settings.hover_size = HoverTextSize::new(20.0).unwrap();
		assert_eq!(hover_text_pixels(&settings), iced::Pixels(20.0));
	}
}
