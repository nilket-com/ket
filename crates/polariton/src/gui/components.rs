use crate::adapters::{
	common::{AdapterFieldType, AdapterStage},
	driver::{AdapterSelection, AdapterState, fields_for},
};
use crate::editor::CodeEditor;
use crate::gui::messages::Message;
use crate::gui::statusbar::{Segment, StatusBar, Tone};
use crate::gui::{
	colors,
	menu::{MenuBar, MenuFontPolicy, MenuItem, MenuRoot, MenuState},
};
use iced::widget::{
	TextInput, button, center, checkbox, column, container, mouse_area, opaque, row, space, stack,
	text, text_editor, text_input, tooltip,
};
use iced::{
	Alignment, Background, Center, Color, Element, Fill, FillPortion, Font, Length, border, font,
	mouse,
	theme::{Palette, Theme},
	window::Direction,
};
use std::time::Duration;

pub const BUTTON_SIZE_DEFAULT: (u32, u32) = (120, 40);

pub fn theme() -> Theme {
	Theme::custom(
		"BlackHole".to_string(),
		Palette {
			background: colors::BG_PRIMARY,
			danger: colors::DANGER,
			primary: colors::PRIMARY,
			success: colors::SUCCESS,
			text: colors::TEXT_PRIMARY,
			warning: colors::WARNING,
		},
	)
}

pub fn title_bar<'a>() -> Element<'a, Message> {
	let width = 34;
	let height = 30;
	let font_size = 16;
	container(
		row![
			mouse_area(container(row![
				space::horizontal(),
				space::horizontal().width(width),
				text("Polariton")
					.size(font_size)
					.font(Font {
						weight: font::Weight::Bold,
						..Default::default()
					})
					.color(colors::WHITE),
				space::horizontal()
			]))
			.on_press(Message::DragWindow),
			button(
				text("—")
					.font(Font {
						weight: font::Weight::Bold,
						..Default::default()
					})
					.size(font_size)
					.align_y(Center)
					.align_x(Center)
			)
			.width(width)
			.height(height)
			.style(|_theme: &Theme, status: button::Status| match status {
				button::Status::Hovered => button::Style {
					background: Some(Background::Color(colors::BRAND_PURPLE)),
					text_color: colors::TEXT_TITLE_BUTTON_HOVER,
					..button::Style::default()
				},
				_ => button::Style {
					background: Some(Background::Color(Color::TRANSPARENT)),
					text_color: colors::TEXT_TITLE_BUTTON,
					..button::Style::default()
				},
			})
			.on_press(Message::MinimizeWindow),
			button(
				text("□")
					.font(Font {
						weight: font::Weight::Bold,
						..Default::default()
					})
					.size(font_size)
					.align_y(Center)
					.align_x(Center)
			)
			.width(width)
			.height(height)
			.style(|_theme: &Theme, status: button::Status| match status {
				button::Status::Hovered => button::Style {
					background: Some(Background::Color(colors::BRAND_PURPLE)),
					text_color: colors::TEXT_TITLE_BUTTON_HOVER,
					..button::Style::default()
				},
				_ => button::Style {
					background: Some(Background::Color(Color::TRANSPARENT)),
					text_color: colors::TEXT_TITLE_BUTTON,
					..button::Style::default()
				},
			})
			.on_press(Message::MaximizeWindow),
			button(
				text("✕")
					.font(Font {
						weight: font::Weight::Bold,
						..Default::default()
					})
					.size(font_size)
					.align_y(Center)
					.align_x(Center)
			)
			.width(width)
			.height(height)
			.style(|_theme: &Theme, status: button::Status| match status {
				button::Status::Hovered => button::Style {
					background: Some(Background::Color(colors::BRAND_PURPLE)),
					text_color: colors::TEXT_TITLE_BUTTON_HOVER,
					..button::Style::default()
				},
				_ => button::Style {
					background: Some(Background::Color(Color::TRANSPARENT)),
					text_color: colors::TEXT_TITLE_BUTTON,
					..button::Style::default()
				},
			})
			.on_press(Message::CloseWindow),
		]
		.padding(0)
		.align_y(iced::Center),
	)
	.width(Fill)
	.height(height)
	.style(|_theme| container::Style {
		background: Some(colors::BG_SECONDARY.into()),
		border: border::Border {
			color: colors::WHITE,
			width: 0.2,
			radius: 0.0.into(),
		},
		..Default::default()
	})
	.into()
}

pub fn main_screen<'a>(
	menu_state: &'a MenuState,
	code_editor: &'a CodeEditor,
	status_msg: &'a str,
	status_error: &'a str,
	status_df_size: Option<(usize, usize)>,
	status_time_elapsed: Option<Duration>,
	editor_spinner_phase: Option<usize>,
	adapter_state: &'a AdapterState,
	showing_settings: bool,
	show_column_types: bool,
	status_detail: Option<(&'a str, &'a text_editor::Content)>,
	status_detail_open: bool,
	local_workspace_offer: Option<&'a std::path::Path>,
	saved_session: bool,
	close_without_checkpoint: bool,
) -> Element<'a, Message> {
	// Baryon's workspace is the sole top-level layout owner.
	let main_content = container(code_editor.view().map(Message::CodeEditEvent))
		.padding(4)
		.width(Fill)
		.height(Fill)
		.style(|_| container::Style {
			border: border::Border {
				color: colors::BORDER_PRIMARY,
				width: 1.0,
				radius: 5.0.into(),
			},
			..Default::default()
		});
	let status_bar: Element<'a, Message> = app_status_bar(
		status_msg,
		status_error,
		status_df_size,
		status_time_elapsed,
		editor_spinner_phase,
		adapter_state,
		saved_session,
	)
	.view();
	let status_bar: Element<'a, Message> = if let Some((detail, _)) = status_detail {
		tooltip(
			mouse_area(status_bar).on_press(Message::ToggleStatusDetail),
			container(
				text(detail)
					.size(13)
					.font(Font::MONOSPACE)
					.wrapping(text::Wrapping::Word),
			)
			.width(Length::Fixed(720.0))
			.padding(10)
			.style(status_detail_card_style),
			tooltip::Position::Top,
		)
		.gap(6)
		.delay(std::time::Duration::from_millis(350))
		.into()
	} else {
		status_bar
	};
	let main_window = window_decorations(
		column![main_content, status_bar],
		menu_state,
		saved_session,
		close_without_checkpoint,
	);
	let adapter_modal = adapter_view(adapter_state);
	let settings_modal: Element<Message> = if showing_settings {
		settings_dialog_view(show_column_types)
	} else {
		container(text("")).into()
	};
	let detail_overlay = if status_detail_open {
		status_detail
			.map(|(_, content)| status_detail_overlay(content))
			.unwrap_or_else(|| container(text("")).into())
	} else {
		container(text("")).into()
	};
	#[cfg(feature = "toron")]
	let local_workspace_overlay: Element<Message> = local_workspace_offer.map_or_else(
		|| container(text("")).into(),
		|profile_root| {
			container(opaque(
				container(
					column![
						text("Start local Toron workspace?").size(18),
						text(format!(
							"Create a session-bound development workspace at {}. It will stop when this editor closes.",
							profile_root.display()
						))
						.wrapping(text::Wrapping::Word),
						row![
							space::horizontal(),
							button("Start workspace").on_press(Message::StartLocalWorkspace)
						]
					]
					.spacing(14),
				)
				.width(Length::Fixed(620.0))
				.padding(18)
				.style(status_detail_card_style),
			))
			.width(Fill)
			.height(Fill)
			.align_x(Alignment::Center)
			.align_y(Alignment::Center)
			.into()
		},
	);
	#[cfg(not(feature = "toron"))]
	let local_workspace_overlay: Element<Message> = {
		let _ = local_workspace_offer;
		container(text("")).into()
	};
	stack![
		main_window,
		adapter_modal,
		settings_modal,
		detail_overlay,
		local_workspace_overlay
	]
	.into()
}

fn status_detail_overlay(content: &text_editor::Content) -> Element<'_, Message> {
	let header = row![
		text("SQL diagnostic")
			.size(14)
			.font(Font {
				weight: font::Weight::Bold,
				..Font::MONOSPACE
			})
			.color(colors::TEXT_PRIMARY),
		space::horizontal(),
		button(text("Copy").size(12))
			.padding([3, 9])
			.on_press(Message::CopyStatusDetail),
		button(text("×").size(14))
			.padding([2, 8])
			.on_press(Message::CloseStatusDetail),
	]
	.spacing(6)
	.align_y(Alignment::Center);
	let diagnostic = text_editor(content)
		.on_action(Message::StatusDetailAction)
		.font(Font::MONOSPACE)
		.size(13)
		.wrapping(text::Wrapping::Word)
		.height(Length::Fill);
	let card = container(column![header, diagnostic].spacing(8))
		.width(Length::Fixed(720.0))
		.height(Length::Fixed(190.0))
		.padding(12)
		.style(status_detail_card_style);
	container(opaque(card))
		.width(Fill)
		.height(Fill)
		.padding(iced::Padding {
			top: 0.0,
			right: 12.0,
			bottom: 38.0,
			left: 0.0,
		})
		.align_x(Alignment::End)
		.align_y(Alignment::End)
		.into()
}

fn status_detail_card_style(_theme: &Theme) -> container::Style {
	container::Style {
		background: Some(Background::Color(colors::BG_SECONDARY)),
		border: border::Border {
			color: colors::WHITE,
			width: 1.0,
			radius: 3.0.into(),
		},
		shadow: iced::Shadow {
			color: Color::BLACK.scale_alpha(0.65),
			offset: iced::Vector::new(0.0, 4.0),
			blur_radius: 12.0,
		},
		..Default::default()
	}
}

fn app_status_bar(
	status_msg: &str,
	status_error: &str,
	status_df_size: Option<(usize, usize)>,
	status_time_elapsed: Option<Duration>,
	editor_spinner_phase: Option<usize>,
	adapter_state: &AdapterState,
	saved_session: bool,
) -> StatusBar {
	let (connection_label, connection_tone) = match adapter_state.stage {
		AdapterStage::Connected => ("connected", Tone::Success),
		AdapterStage::Configured => ("configured", Tone::Accent),
		AdapterStage::Unconfigured => ("configure adapter", Tone::Warning),
		AdapterStage::Unselected => ("select adapter", Tone::Warning),
		AdapterStage::None => ("disconnected", Tone::Danger),
	};
	// The session scratch database answers SQL whenever no external adapter
	// is connected: report that target instead of a scary "disconnected".
	let (connection_label, connection_tone, target_label) = if adapter_state.scratch_is_active() {
		("scratch", Tone::Accent, "SQLite :memory:")
	} else {
		(
			connection_label,
			connection_tone,
			adapter_state.selection.adapter_type_str(),
		)
	};
	let mut bar = StatusBar::new().lane_split(2, 5);
	if saved_session {
		bar = bar.left(Segment::text("durable session"));
	}
	bar = bar
		.left(Segment::toned_text(connection_label, connection_tone))
		.left(Segment::text(target_label));
	if let Some(phase) = editor_spinner_phase {
		bar = bar.left(Segment::spinner("editor", phase, Tone::Accent));
	}
	if !status_error.is_empty() {
		bar = bar.right(
			Segment::toned_text(status_error, Tone::Danger)
				.max_chars(112)
				.fill_portion(if status_msg.is_empty() { 1 } else { 2 }),
		);
	}
	if !status_msg.is_empty() {
		bar = bar.right(
			Segment::toned_text(status_msg, Tone::Accent)
				.max_chars(112)
				.fill_portion(1),
		);
	}
	if let Some((rows, cols)) = status_df_size {
		bar = bar.right(
			Segment::label_value("table", format!("{rows} x {cols}"), Tone::Normal)
				.reserve_chars(18),
		);
	}
	if let Some(time_elapsed) = status_time_elapsed {
		bar = bar.right(Segment::timer("time", time_elapsed, Tone::Normal).reserve_chars(12));
	}
	bar
}

pub fn menu_bar<'a>(
	menu_state: &'a MenuState,
	saved_session: bool,
	close_without_checkpoint: bool,
) -> Element<'a, Message> {
	Element::from(
		MenuBar::new(
			build_menu_roots(saved_session, close_without_checkpoint),
			menu_state,
		)
		.font_policy(MenuFontPolicy::SystemWithFallback),
	)
	.map(Message::Menu)
}

fn build_menu_roots(saved_session: bool, close_without_checkpoint: bool) -> Vec<MenuRoot> {
	let mut roots = vec![
		MenuRoot {
			id: "connect".into(),
			label: "Connect".into(),
			items: vec![MenuItem::Action {
				id: "connect:new".into(),
				label: "New".into(),
			}],
		},
		MenuRoot {
			id: "code".into(),
			label: "Code".into(),
			items: vec![
				MenuItem::Action {
					id: "code:run".into(),
					label: "Run all  (Ctrl+Enter)".into(),
				},
				MenuItem::Action {
					id: "code:run-snippet".into(),
					label: "Run selection / line  (Shift+Enter)".into(),
				},
			],
		},
		MenuRoot {
			id: "plot".into(),
			label: "Plot".into(),
			items: vec![MenuItem::Submenu {
				id: "plot:export".into(),
				label: "Export".into(),
				items: [
					crate::gui::messages::ExportFormat::SVG,
					crate::gui::messages::ExportFormat::PNG,
					crate::gui::messages::ExportFormat::AVIF,
				]
				.into_iter()
				.map(|format| MenuItem::Action {
					id: format!("plot:export:{format}"),
					label: format.to_string(),
				})
				.collect(),
			}],
		},
		MenuRoot {
			id: "settings".into(),
			label: "Settings".into(),
			items: vec![MenuItem::Action {
				id: "settings:preferences".into(),
				label: "Preferences".into(),
			}],
		},
	];
	if saved_session {
		let mut items = vec![
			MenuItem::Action {
				id: "session:checkpoint".into(),
				label: "Checkpoint".into(),
			},
			MenuItem::Action {
				id: "session:copy-id".into(),
				label: "Copy session ID".into(),
			},
		];
		if close_without_checkpoint {
			items.push(MenuItem::Action {
				id: "session:close-without-checkpoint".into(),
				label: "Close without checkpoint".into(),
			});
		}
		roots.push(MenuRoot {
			id: "session".into(),
			label: "Session".into(),
			items,
		});
	}
	roots
}

fn section<'a>(title: &'a str, content: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
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

fn field<'a>(label: &'a str, widget: impl Into<Element<'a, Message>>) -> Element<'a, Message> {
	row![text(label).width(Length::Fixed(160.0)), widget.into()]
		.spacing(10)
		.align_y(Alignment::Center)
		.into()
}

fn window_decorations<'a>(
	underlay: impl Into<Element<'a, Message>>,
	menu_state: &'a MenuState,
	saved_session: bool,
	close_without_checkpoint: bool,
) -> Element<'a, Message> {
	let resize_thin = 6;
	let resize_thick = 60;
	let resize_area_northeast_top =
		styled_resize_area(resize_thick, resize_thin / 2, Direction::NorthEast);
	let resize_area_north = styled_resize_area(Fill, resize_thin / 2, Direction::North);
	let resize_area_northwest_top =
		styled_resize_area(resize_thick, resize_thin / 2, Direction::NorthWest);
	let resize_area_northwest_side =
		styled_resize_area(resize_thin, resize_thick, Direction::NorthWest);
	let resize_area_west = styled_resize_area(resize_thin, Fill, Direction::West);
	let resize_area_southwest_side =
		styled_resize_area(resize_thin, resize_thick, Direction::SouthWest);
	let resize_area_southwest_bottom =
		styled_resize_area(resize_thick, resize_thin, Direction::SouthWest);
	let resize_area_south = styled_resize_area(Fill, resize_thin, Direction::South);
	let resize_area_southeast_bottom =
		styled_resize_area(resize_thick, resize_thin, Direction::SouthEast);
	let resize_area_northeast_side =
		styled_resize_area(resize_thin, resize_thick, Direction::NorthEast);
	let resize_area_east = styled_resize_area(resize_thin, Fill, Direction::East);
	let resize_area_southeast_side =
		styled_resize_area(resize_thin, resize_thick, Direction::SouthEast);
	column![
		row![
			resize_area_northwest_top,
			resize_area_north,
			resize_area_northeast_top,
		],
		row![
			column![
				resize_area_northwest_side,
				resize_area_west,
				resize_area_southwest_side
			],
			column![
				row![title_bar()],
				stack![
					column![space::vertical().height(32), underlay.into(),],
					row![menu_bar(
						menu_state,
						saved_session,
						close_without_checkpoint
					)],
				],
				row![
					resize_area_southwest_bottom,
					resize_area_south,
					resize_area_southeast_bottom,
				],
			],
			column![
				resize_area_northeast_side,
				resize_area_east,
				resize_area_southeast_side
			],
		]
	]
	.into()
}

pub fn styled_text_input<'a, Message: Clone + 'a>(
	default_str: &str,
	input_str: &str,
) -> TextInput<'a, Message> {
	text_input(default_str, input_str)
		.padding(10)
		.size(18)
		.style(|_theme: &Theme, status: text_input::Status| match status {
			text_input::Status::Focused { .. } => text_input::Style {
				background: Background::Color(colors::BG_INPUT_FOCUS),
				border: border::Border {
					color: colors::BORDER_ACCENT,
					width: 2.0,
					radius: 5.0.into(),
				},
				icon: colors::TEXT_SECONDARY,
				placeholder: colors::TEXT_PLACEHOLDER_HOVER,
				value: colors::TEXT_SECONDARY,
				selection: colors::SELECTION,
			},
			text_input::Status::Hovered => text_input::Style {
				background: Background::Color(colors::BG_INPUT_HOVER),
				border: border::Border {
					color: colors::BORDER_HOVER,
					width: 1.5,
					radius: 5.0.into(),
				},
				icon: colors::TEXT_SECONDARY,
				placeholder: colors::TEXT_PLACEHOLDER,
				value: colors::TEXT_SECONDARY,
				selection: colors::SELECTION,
			},
			_ => text_input::Style {
				background: Background::Color(colors::BG_INPUT),
				border: border::Border {
					color: colors::BORDER_PRIMARY,
					width: 1.0,
					radius: 5.0.into(),
				},
				icon: colors::TEXT_SECONDARY,
				placeholder: colors::TEXT_PLACEHOLDER,
				value: colors::TEXT_SECONDARY,
				selection: colors::SELECTION,
			},
		})
}

#[cfg(test)]
mod tests {
	use super::{app_status_bar, build_menu_roots};
	use crate::adapters::driver::AdapterState;
	use crate::gui::menu::MenuItem;

	fn action_ids(saved_session: bool, close_without_checkpoint: bool) -> Vec<String> {
		build_menu_roots(saved_session, close_without_checkpoint)
			.into_iter()
			.flat_map(|root| root.items)
			.filter_map(|item| match item {
				MenuItem::Action { id, .. } => Some(id),
				MenuItem::Submenu { .. } | MenuItem::Separator => None,
			})
			.collect()
	}

	#[test]
	fn product_0063_session_menu_exists_only_for_saved_authority() {
		let ephemeral = action_ids(false, false);
		assert!(!ephemeral.iter().any(|id| id.starts_with("session:")));

		let saved = action_ids(true, false);
		assert!(saved.contains(&"session:checkpoint".to_owned()));
		assert!(saved.contains(&"session:copy-id".to_owned()));
		assert!(!saved.contains(&"session:close-without-checkpoint".to_owned()));

		let failed_close = action_ids(true, true);
		assert!(failed_close.contains(&"session:close-without-checkpoint".to_owned()));
	}

	#[test]
	fn product_0075_plot_menu_retains_only_the_three_export_actions() {
		fn collect(items: &[MenuItem], ids: &mut Vec<String>) {
			for item in items {
				match item {
					MenuItem::Action { id, .. } => ids.push(id.clone()),
					MenuItem::Submenu { items, .. } => collect(items, ids),
					MenuItem::Separator => {}
				}
			}
		}

		let roots = build_menu_roots(false, false);
		let plot = roots
			.iter()
			.find(|root| root.id == "plot")
			.expect("Plot menu remains as the export surface");
		let mut ids = Vec::new();
		collect(&plot.items, &mut ids);
		assert_eq!(
			ids,
			["plot:export:SVG", "plot:export:PNG", "plot:export:AVIF"]
		);
		assert!(ids.iter().all(|id| !id.starts_with("plot:new:")));
	}

	#[test]
	fn product_0071_durable_session_segment_names_attachment_not_freshness() {
		let adapter = AdapterState::default();
		let ephemeral = app_status_bar("", "", None, None, None, &adapter, false);
		let durable = app_status_bar("", "", None, None, None, &adapter, true);
		let checkpointed =
			app_status_bar("Session checkpointed", "", None, None, None, &adapter, true);
		let failed = app_status_bar("", "checkpoint failed", None, None, None, &adapter, true);

		let durable_segment = ("left", "durable session");
		assert!(
			!ephemeral
				.product_0071_text_segments()
				.contains(&durable_segment),
			"ephemeral authority must not render the durable attachment segment"
		);
		for (label, bar) in [
			("ordinary", durable),
			("checkpointed", checkpointed),
			("failed", failed),
		] {
			let segments = bar.product_0071_text_segments();
			assert_eq!(
				segments
					.iter()
					.filter(|segment| **segment == durable_segment)
					.count(),
				1,
				"{label} durable authority must expose exactly one attachment segment"
			);
			assert!(
				segments
					.iter()
					.filter(|(lane, _)| *lane == "left")
					.map(|(_, text)| text.to_ascii_lowercase())
					.all(|text| ["saved", "clean", "dirty"]
						.iter()
						.all(|claim| !text.contains(claim))),
				"persistent attachment chrome must not claim publication freshness"
			);
		}
	}

	#[test]
	fn product_0071_session_menu_keeps_identity_behind_the_copy_action() {
		let roots = build_menu_roots(true, false);
		let session = roots
			.iter()
			.find(|root| root.id == "session")
			.expect("saved authority has one Session menu");
		let labels = session
			.items
			.iter()
			.map(|item| match item {
				MenuItem::Action { label, .. } | MenuItem::Submenu { label, .. } => label.as_str(),
				MenuItem::Separator => "<separator>",
			})
			.collect::<Vec<_>>();
		assert_eq!(labels, ["Checkpoint", "Copy session ID"]);
		assert!(
			labels.iter().all(|label| !label.contains("12345678")),
			"the menu names the exact-copy action without displaying identity bytes"
		);
	}
}

fn styled_resize_area<'a, WT: Into<Length>, HT: Into<Length>>(
	width: WT,
	height: HT,
	direction: Direction,
) -> Element<'a, Message> {
	mouse_area(
		container(space::horizontal().width(width).height(height)).style(|_| container::Style {
			background: Some(colors::BG_SECONDARY.into()),
			border: border::Border {
				color: colors::BORDER_DIM,
				width: 1.0,
				radius: 0.0.into(),
			},
			..Default::default()
		}),
	)
	.interaction(match direction {
		Direction::West | Direction::East => mouse::Interaction::ResizingHorizontally,
		Direction::North | Direction::South => mouse::Interaction::ResizingVertically,
		Direction::NorthEast | Direction::SouthWest => mouse::Interaction::ResizingDiagonallyUp,
		Direction::NorthWest | Direction::SouthEast => mouse::Interaction::ResizingDiagonallyDown,
	})
	.on_press(Message::ResizeWindow(direction))
	.into()
}

pub fn styled_button<'a, Message: Clone + 'a>(
	label: &str,
	msg: Message,
	size: (u32, u32),
) -> Element<'a, Message> {
	button(
		text(label.to_string())
			.size(18)
			.width(Fill)
			.align_x(Alignment::Center)
			.align_y(Alignment::Center)
			.font(Font {
				weight: font::Weight::Semibold,
				..Default::default()
			}),
	)
	.width(size.0)
	.height(size.1)
	.style(|theme: &Theme, status: button::Status| {
		let base = button::primary(theme, status);
		match status {
			button::Status::Hovered => button::Style {
				background: Some(Background::Color(colors::BG_BUTTON_HOVER)),
				border: border::Border {
					color: colors::BORDER_ACCENT,
					width: 2.0,
					radius: 5.0.into(),
				},
				text_color: colors::TEXT_SECONDARY,
				..base
			},
			_ => button::Style {
				background: Some(Background::Color(colors::BG_BUTTON)),
				border: border::Border {
					color: colors::BORDER_PRIMARY,
					width: 1.0,
					radius: 5.0.into(),
				},
				text_color: colors::TEXT_SECONDARY,
				..base
			},
		}
	})
	.on_press(msg)
	.into()
}

pub fn adapter_view(adapter_state: &AdapterState) -> Element<'static, Message> {
	match adapter_state.stage {
		AdapterStage::None => container(text("")).into(),
		AdapterStage::Unselected => adapter_gallery_view(),
		AdapterStage::Unconfigured => adapter_configuration_view(adapter_state),
		AdapterStage::Configured => container(text("")).into(),
		AdapterStage::Connected => container(text("")).into(),
	}
}

const MODAL_FILL_PORTION_V: u16 = 30;
const MODAL_FILL_PORTION_H: u16 = 40;

fn adapter_gallery_view() -> Element<'static, Message> {
	let adapter_buttons = vec![
		styled_button(
			"BigQuery",
			Message::AdapterSelected(AdapterSelection::BigQuery),
			BUTTON_SIZE_DEFAULT,
		),
		styled_button(
			"MySQL",
			Message::AdapterSelected(AdapterSelection::MySQL),
			BUTTON_SIZE_DEFAULT,
		),
		styled_button(
			"Parquet",
			Message::AdapterSelected(AdapterSelection::Parquet),
			BUTTON_SIZE_DEFAULT,
		),
		styled_button(
			"Postgres",
			Message::AdapterSelected(AdapterSelection::Postgres),
			BUTTON_SIZE_DEFAULT,
		),
		styled_button(
			"SQLite",
			Message::AdapterSelected(AdapterSelection::SQLite),
			BUTTON_SIZE_DEFAULT,
		),
	];
	#[cfg(feature = "toron")]
	let adapter_buttons = {
		let mut buttons = adapter_buttons;
		buttons.push(styled_button(
			"Toron",
			Message::AdapterSelected(AdapterSelection::Toron),
			BUTTON_SIZE_DEFAULT,
		));
		buttons
	};
	let dialog: Element<Message> = container(column![
		center(text("Select Adapter").size(24)),
		center(row(adapter_buttons).spacing(20))
	])
	.width(FillPortion(MODAL_FILL_PORTION_H))
	.height(FillPortion(MODAL_FILL_PORTION_V))
	.style(|_| container::Style {
		background: Some(colors::BG_MODAL.into()),
		border: border::Border {
			color: colors::BORDER_PRIMARY,
			width: 1.0,
			radius: 5.0.into(),
		},
		..Default::default()
	})
	.into();
	column![
		space::vertical().height(FillPortion((100 - MODAL_FILL_PORTION_V) / 2)),
		row![
			space::horizontal().width(FillPortion((100 - MODAL_FILL_PORTION_H) / 2)),
			dialog,
			space::horizontal().width(FillPortion((100 - MODAL_FILL_PORTION_H) / 2)),
		],
		space::vertical().height(FillPortion((100 - MODAL_FILL_PORTION_V) / 2)),
	]
	.into()
}

fn settings_dialog_view<'a>(show_column_types: bool) -> Element<'a, Message> {
	let dialog: Element<Message> = container(
		column![
			row![
				text("Settings").size(24),
				space::horizontal(),
				styled_button("✕", Message::CloseSettings, (40, 32)),
			]
			.align_y(Alignment::Center),
			section(
				"Table Display",
				checkbox(show_column_types)
					.label("Show column types in header")
					.on_toggle(Message::ToggleShowColumnTypes),
			),
		]
		.spacing(20)
		.padding(20),
	)
	.width(Length::Fixed(520.0))
	.style(|_| container::Style {
		background: Some(colors::BG_MODAL.into()),
		border: border::Border {
			color: colors::BORDER_PRIMARY,
			width: 1.0,
			radius: 5.0.into(),
		},
		..Default::default()
	})
	.into();
	column![
		space::vertical().height(FillPortion((100 - MODAL_FILL_PORTION_V) / 2)),
		row![space::horizontal(), dialog, space::horizontal()],
		space::vertical().height(FillPortion((100 - MODAL_FILL_PORTION_V) / 2)),
	]
	.into()
}

pub fn adapter_configuration_view(adapter_state: &AdapterState) -> Element<'static, Message> {
	let name = adapter_state.name.clone();
	let adapter_label = match &adapter_state.selection {
		AdapterSelection::None => "None",
		AdapterSelection::BigQuery => "BigQuery",
		AdapterSelection::MySQL => "MySQL",
		AdapterSelection::Parquet => "Parquet",
		AdapterSelection::Postgres => "Postgres",
		AdapterSelection::SQLite => "SQLite",
		#[cfg(feature = "toron")]
		AdapterSelection::Toron => "Toron",
	};
	let fields_section: Element<Message> = match &adapter_state.selection {
		AdapterSelection::None => text("Select an adapter to configure.").into(),
		selection => {
			let descriptors = fields_for(selection);
			let inputs = descriptors
				.iter()
				.fold(column![].spacing(8), |col, descriptor| {
					let current_value = adapter_state
						.fields
						.get(descriptor.key)
						.cloned()
						.unwrap_or_default();
					let key = descriptor.key;
					match descriptor.field_type {
						AdapterFieldType::Text => {
							let input = if descriptor.is_secure {
								styled_text_input(descriptor.placeholder, &current_value)
									.secure(true)
							} else {
								styled_text_input(descriptor.placeholder, &current_value)
							};
							let input = input
								.on_input(move |val| {
									Message::AdapterConfigurationChanged(key.into(), val)
								})
								.on_submit(Message::AdapterConfigurationSubmitted);
							col.push(field(descriptor.key, input))
						}
					}
				});
			section(adapter_label, inputs)
		}
	};
	let dialog: Element<Message> = container(
		column![
			row![text("Configure Adapter").size(24), space::horizontal(),]
				.align_y(Alignment::Center),
			section(
				"Connection",
				field(
					"Name",
					styled_text_input("Required", &name)
						.on_input(Message::ConnectionNameChanged)
						.on_submit(Message::AdapterConfigurationSubmitted),
				),
			),
			fields_section,
			row![
				space::horizontal(),
				styled_button(
					"Connect",
					Message::AdapterConfigurationSubmitted,
					BUTTON_SIZE_DEFAULT,
				),
			]
			.spacing(10)
			.align_y(Alignment::Center),
		]
		.spacing(20)
		.padding(20),
	)
	.width(Length::Fixed(800.0))
	.style(|_| container::Style {
		background: Some(colors::BG_MODAL.into()),
		border: border::Border {
			color: colors::BORDER_PRIMARY,
			width: 1.0,
			radius: 5.0.into(),
		},
		..Default::default()
	})
	.into();
	column![
		space::vertical().height(FillPortion((100 - MODAL_FILL_PORTION_V) / 2)),
		row![space::horizontal(), dialog, space::horizontal(),],
		space::vertical().height(FillPortion((100 - MODAL_FILL_PORTION_V) / 2)),
	]
	.into()
}
