use super::HostPlotState;
use crate::gui::messages::PlotMessage;
use crate::plot::common::{PlotRenderLayer, PlotWidget, ResolvedPlotSettings};
use crate::presentation_registry::{InspectorSection, SourceAttachmentStatus, inspector_rows};
use iced::widget::{button, canvas, column, container, row, rule, scrollable, stack, text};
use iced::{Background, Element, Fill, Length, Pixels};

/// PRODUCT-0112's sole conversion from resolved hover typography to iced.
/// The canvas never consumes this value.
fn hover_text_pixels(settings: &ResolvedPlotSettings) -> Pixels {
	Pixels(settings.hover_size.points())
}

/// Plot content hosted directly by a libaryon panel. Host-panel chrome owns
/// closing and layout, so this surface emits plot interaction only.
pub fn host_view(
	state: &HostPlotState,
	attachment_status: SourceAttachmentStatus,
) -> Element<'_, PlotMessage> {
	let title = state.display_label.clone();
	let data = canvas(PlotWidget::new(
		state.kernel.as_ref(),
		title.clone(),
		20.0,
		state.plot_settings_model.clone(),
		state.render_revision,
		state.resize_render_suspended,
		PlotRenderLayer::Data,
	))
	.width(Fill)
	.height(Fill);
	let interaction = canvas(PlotWidget::new(
		state.kernel.as_ref(),
		title,
		20.0,
		state.plot_settings_model.clone(),
		state.render_revision,
		state.resize_render_suspended,
		PlotRenderLayer::OverlayInteractive,
	))
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
	let plot = container(layers.width(Fill).height(Fill)).style(|_| container::Style {
		background: Some(Background::Color(
			state.plot_settings.surrounding_background_color,
		)),
		text_color: Some(state.plot_settings.surrounding_decoration_color),
		..Default::default()
	});
	let body: Element<'_, PlotMessage> = if state.settings_inspector_open {
		row![
			plot.width(Fill),
			settings_inspector(state, attachment_status)
		]
		.height(Fill)
		.into()
	} else {
		plot.width(Fill).height(Fill).into()
	};
	let toggle_label = if state.settings_inspector_open {
		"Hide settings"
	} else {
		"Plot settings"
	};
	column![
		container(
			button(text(toggle_label).size(11)).on_press(PlotMessage::ToggleSettingsInspector)
		)
		.width(Fill)
		.padding([3, 6])
		.align_x(iced::alignment::Horizontal::Right),
		body,
	]
	.height(Fill)
	.into()
}

fn section_label(section: InspectorSection) -> &'static str {
	match section {
		InspectorSection::Figure => "Figure",
		InspectorSection::Axes => "Axes",
		InspectorSection::Legend => "Legend",
		InspectorSection::Mark => "Mark",
		InspectorSection::Facet => "Facet",
		InspectorSection::Padding => "Padding",
	}
}

fn settings_inspector(
	state: &HostPlotState,
	attachment_status: SourceAttachmentStatus,
) -> Element<'_, PlotMessage> {
	let attachment = match attachment_status {
		SourceAttachmentStatus::Detached => "detached",
		SourceAttachmentStatus::OutOfDate => "out of date",
		SourceAttachmentStatus::Attached => "attached",
	};
	let rows = inspector_rows(
		&state.plot_settings_model,
		&state.plot_settings,
		state.plot_type,
		state.source_attachment.as_ref(),
	);
	let mut content = column![
		text("Plot settings").size(16),
		text(attachment).size(11),
		rule::horizontal(1),
	]
	.spacing(5);
	let mut previous_section = None;
	for setting in rows {
		if previous_section != Some(setting.section) {
			content = content.push(text(section_label(setting.section)).size(13));
			previous_section = Some(setting.section);
		}
		let mut badges = setting.provenance.to_string();
		if setting.expression_authored {
			badges.push_str(" · expression");
		}
		if setting.shared_source {
			badges.push_str(" · shared source");
		}
		content = content.push(
			column![
				row![
					text(setting.label).size(12).width(Fill),
					text(setting.value.to_string()).size(12),
				]
				.spacing(8),
				text(badges).size(10),
			]
			.spacing(1),
		);
	}
	container(scrollable(content).height(Fill))
		.width(Length::Fixed(270.0))
		.height(Fill)
		.padding(10)
		.style(|_| container::Style {
			background: Some(Background::Color(iced::Color {
				a: 0.96,
				..state.plot_settings.surrounding_background_color
			})),
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
