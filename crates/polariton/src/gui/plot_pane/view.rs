use super::HostPlotState;
use crate::gui::messages::PlotMessage;
use crate::plot::common::{PlotRenderLayer, PlotWidget, ResolvedPlotSettings};
use crate::presentation_registry::{InspectorSection, SourceAttachmentStatus, inspector_rows};
#[cfg(feature = "toron")]
use crate::settings_writeback::{WritebackCapability, writeback_spec};
use iced::widget::{
	button, canvas, column, container, row, rule, scrollable, stack, text, text_input,
};
use iced::{Background, Element, Fill, Length, Pixels};

/// PRODUCT-0112's sole conversion from resolved hover typography to iced.
/// The canvas never consumes this value.
fn hover_text_pixels(settings: &ResolvedPlotSettings) -> Pixels {
	Pixels(settings.hover_size.points())
}

fn displayed_pane_submit_value(
	key: crate::presentation_registry::SettingKey,
	provenance: crate::presentation_registry::ProvenanceTier,
	value: &str,
	has_draft: bool,
) -> Option<String> {
	(provenance == crate::presentation_registry::ProvenanceTier::PaneOverride
		&& matches!(
			key,
			crate::presentation_registry::SettingKey::XAxisBounds
				| crate::presentation_registry::SettingKey::YAxisBounds
		) && !has_draft)
		.then(|| value.to_owned())
}

/// Plot content hosted directly by a libaryon panel. Host-panel chrome owns
/// closing and layout, so this surface emits plot interaction only.
pub fn host_view(
	state: &HostPlotState,
	attachment_status: SourceAttachmentStatus,
) -> Element<'_, PlotMessage> {
	let title = state.display_label.clone();
	let data = canvas(
		PlotWidget::new(
			state.kernel.as_ref(),
			title.clone(),
			20.0,
			state.plot_settings_model.clone(),
			state.render_revision,
			state.resize_render_suspended,
			PlotRenderLayer::Data,
		)
		.with_raster_coordinator(&state.raster_coordinator)
		.with_viewport_state(state.viewport_stamp, state.viewport_motion),
	)
	.width(Fill)
	.height(Fill);
	let interaction = canvas(
		PlotWidget::new(
			state.kernel.as_ref(),
			title,
			20.0,
			state.plot_settings_model.clone(),
			state.render_revision,
			state.resize_render_suspended,
			PlotRenderLayer::OverlayInteractive,
		)
		.with_viewport_state(state.viewport_stamp, state.viewport_motion)
		.with_settings_inspector_gesture(),
	)
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
	body
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
	if let Some(status) = &state.writeback_status {
		content = content.push(text(status).size(10));
	}
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
		if setting.provenance == crate::presentation_registry::ProvenanceTier::AuthoredSpec
			&& !setting.source_occurrence
		{
			badges.push_str(" · unattributed source");
		}
		#[cfg(feature = "toron")]
		let editable = attachment_status == SourceAttachmentStatus::Attached
			&& writeback_spec(setting.key).capability == WritebackCapability::Editable
			&& !setting.expression_authored
			&& !setting.shared_source
			&& !(setting.provenance == crate::presentation_registry::ProvenanceTier::AuthoredSpec
				&& !setting.source_occurrence);
		#[cfg(not(feature = "toron"))]
		let editable = false;
		let value = state
			.setting_drafts
			.get(&setting.key)
			.cloned()
			.unwrap_or_else(|| setting.value.to_string());
		let value_row: Element<'_, PlotMessage> = if editable {
			let key = setting.key;
			let displayed_pane_value = displayed_pane_submit_value(
				key,
				setting.provenance,
				&value,
				state.setting_drafts.contains_key(&key),
			);
			let mut controls = row![
				text_input(setting.label, &value)
					.on_input(move |value| PlotMessage::SettingChanged(key, value))
					.on_submit(PlotMessage::SettingSubmitted(key, displayed_pane_value))
					.size(12),
			]
			.spacing(4);
			if setting.provenance == crate::presentation_registry::ProvenanceTier::AuthoredSpec {
				controls = controls
					.push(button(text("Reset").size(10)).on_press(PlotMessage::SettingReset(key)));
			}
			controls.into()
		} else {
			row![
				text(setting.label).size(12).width(Fill),
				text(value).size(12),
			]
			.spacing(8)
			.into()
		};
		content = content.push(column![value_row, text(badges).size(10)].spacing(1));
	}
	if state.writeback_status.is_some() {
		content = content.push(
			button(text("Cancel preview").size(10)).on_press(PlotMessage::CancelSettingPreview),
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
mod product_0150_promote_tests {
	use super::displayed_pane_submit_value;
	use crate::presentation_registry::{ProvenanceTier, SettingKey};

	#[test]
	fn product_0150_only_untouched_pane_bounds_seed_the_displayed_submit_value() {
		for key in [SettingKey::XAxisBounds, SettingKey::YAxisBounds] {
			assert_eq!(
				displayed_pane_submit_value(key, ProvenanceTier::PaneOverride, "1, 9", false),
				Some("1, 9".to_owned())
			);
			assert_eq!(
				displayed_pane_submit_value(key, ProvenanceTier::PaneOverride, "1, 9", true),
				None,
				"a typed draft remains the authority"
			);
			for provenance in [
				ProvenanceTier::LibraryDefault,
				ProvenanceTier::ApplicationTheme,
				ProvenanceTier::AuthoredSpec,
				ProvenanceTier::Layer,
			] {
				assert_eq!(
					displayed_pane_submit_value(key, provenance, "1, 9", false),
					None,
					"untouched non-pane rows preserve PRODUCT-0142's refusal"
				);
			}
		}
		assert_eq!(
			displayed_pane_submit_value(
				SettingKey::PointSize,
				ProvenanceTier::PaneOverride,
				"4",
				false,
			),
			None,
			"pane fallback is scoped to domain bounds"
		);
	}
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
