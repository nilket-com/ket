use crate::adapters::{
	common::{AdapterFieldType, AdapterStage},
	driver::{fields_for, AdapterSelection, AdapterState},
};
use crate::editor::CodeEditor;
use crate::gui::dashboard::DashboardState;
use crate::gui::messages::Message;
use crate::gui::pane_grid::{PaneGrid, PaneId, State as PaneState};
use crate::gui::{
	colors,
	menu::{MenuBar, MenuFontPolicy, MenuItem, MenuRoot, MenuState},
};
use crate::persistence::{SavedConnection, SavedStatement};
use crate::gui::statusbar::{Segment, StatusBar, Tone};
use iced::widget::{
	button, center, checkbox, column, container, mouse_area, row, space, stack, text,
	text_input, TextInput,
};
use iced::{
	border, font, mouse,
	theme::{Palette, Theme},
	window::Direction,
	Alignment, Background, Center, Color, Element, Fill, FillPortion, Font, Length,
};
use std::collections::HashMap;

pub const BUTTON_SIZE_DEFAULT: (u32, u32) = (120, 40);

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PaneType {
	CodeEditor,
}

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
	panes: &'a PaneState,
	pane_content: &'a HashMap<PaneId, PaneType>,
	dashboard: &'a DashboardState,
	menu_state: &'a MenuState,
	code_editor: &'a CodeEditor,
	status_msg: &'a str,
	status_error: &'a str,
	status_df_size: Option<(usize, usize)>,
	status_time_elapsed: Option<f64>,
	adapter_state: &'a AdapterState,
	saved_connections: &'a [SavedConnection],
	saved_statements: &'a [SavedStatement],
	showing_password_prompt: bool,
	password_entry: &'a str,
	password_entry_error: &'a str,
	showing_settings: bool,
	settings_new_password: &'a str,
	settings_confirm_password: &'a str,
	settings_error: &'a str,
	is_password_protected: bool,
	show_column_types: bool,
	showing_save_statement_dialog: bool,
	save_statement_name: &'a str,
	editing_statement_id: Option<i64>,
) -> Element<'a, Message> {
	let content: Vec<(PaneId, Element<'a, Message>)> = panes
		.panes()
		.iter()
		.filter_map(|&id| {
			let pane_type = pane_content.get(&id)?;
			let element: Element<'a, Message> = match pane_type {
				PaneType::CodeEditor => center(
					container(code_editor.view(dashboard).map(Message::CodeEditEvent))
						.padding(1)
						.style(|_| container::Style {
							border: border::Border {
								color: colors::BORDER_PRIMARY,
								width: 1.0,
								radius: 5.0.into(),
							},
							..Default::default()
						}),
				)
				.into(),
			};
			Some((id, element))
		})
		.collect();
	let main_pane: Element<'a, Message> =
		PaneGrid::new(panes, content, Message::PaneAction).into();
	let main_content = container(main_pane).padding(4).width(Fill);
	let status_bar = app_status_bar(
		status_msg,
		status_error,
		status_df_size,
		status_time_elapsed,
		adapter_state,
	)
	.view();
	let main_window = window_decorations(
		column![main_content, status_bar],
		menu_state,
		saved_connections,
		saved_statements,
	);
	let adapter_modal = adapter_view(adapter_state);
	let password_modal: Element<Message> = if showing_password_prompt {
		password_prompt_view(password_entry, password_entry_error)
	} else {
		container(text("")).into()
	};
	let settings_modal: Element<Message> = if showing_settings {
		settings_dialog_view(
			settings_new_password,
			settings_confirm_password,
			settings_error,
			is_password_protected,
			show_column_types,
		)
	} else {
		container(text("")).into()
	};
	let save_statement_modal: Element<Message> = if showing_save_statement_dialog {
		save_statement_dialog_view(save_statement_name, editing_statement_id.is_some())
	} else {
		container(text("")).into()
	};
	stack![main_window, adapter_modal, password_modal, settings_modal, save_statement_modal].into()
}

fn app_status_bar(
	status_msg: &str,
	status_error: &str,
	status_df_size: Option<(usize, usize)>,
	status_time_elapsed: Option<f64>,
	adapter_state: &AdapterState,
) -> StatusBar {
	let (connection_label, connection_tone) = match adapter_state.stage {
		AdapterStage::Connected => ("connected", Tone::Success),
		AdapterStage::Configured => ("configured", Tone::Accent),
		AdapterStage::Unconfigured => ("configure adapter", Tone::Warning),
		AdapterStage::Unselected => ("select adapter", Tone::Warning),
		AdapterStage::None => ("disconnected", Tone::Danger),
	};
	let mut bar = StatusBar::new()
		.lane_split(2, 5)
		.left(Segment::toned_text(connection_label, connection_tone))
		.left(Segment::text(adapter_state.selection.adapter_type_str()))
		.right(
			Segment::toned_text(status_error, Tone::Danger)
				.max_chars(42)
				.fill_portion(1),
		)
		.right(
			Segment::toned_text(status_msg, Tone::Accent)
				.max_chars(56)
				.fill_portion(2),
		);
	if let Some((rows, cols)) = status_df_size {
		bar = bar.right(
			Segment::label_value("table", format!("{rows} x {cols}"), Tone::Normal)
				.reserve_chars(18),
		);
	}
	if let Some(time_elapsed) = status_time_elapsed {
		bar = bar.right(
			Segment::label_value("time", format!("{time_elapsed:.3}s"), Tone::Normal)
				.reserve_chars(12),
		);
	}
	bar
}

pub fn menu_bar<'a>(
	menu_state: &'a MenuState,
	saved_connections: &'a [SavedConnection],
	saved_statements: &'a [SavedStatement],
) -> Element<'a, Message> {
	Element::from(MenuBar::new(
		build_menu_roots(saved_connections, saved_statements),
		menu_state,
	)
	.font_policy(MenuFontPolicy::SystemWithFallback))
	.map(Message::Menu)
}

fn build_menu_roots(
	saved_connections: &[SavedConnection],
	saved_statements: &[SavedStatement],
) -> Vec<MenuRoot> {
	vec![
		MenuRoot {
			id: "connect".into(),
			label: "Connect".into(),
			items: vec![
				MenuItem::Action {
					id: "connect:new".into(),
					label: "New".into(),
				},
				MenuItem::Separator,
				MenuItem::Submenu {
					id: "connect:saved".into(),
					label: "Saved".into(),
					items: build_saved_connection_items(saved_connections),
				},
			],
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
				MenuItem::Action {
					id: "code:save".into(),
					label: "Save...".into(),
				},
				MenuItem::Separator,
				MenuItem::Submenu {
					id: "code:saved".into(),
					label: "Saved".into(),
					items: build_saved_statement_items(saved_statements),
				},
			],
		},
		MenuRoot {
			id: "plot".into(),
			label: "Plot".into(),
			items: vec![
				MenuItem::Submenu {
					id: "plot:new".into(),
					label: "New".into(),
					items: crate::plot::core::PlotType::ALL
						.iter()
						.map(|plot_type| MenuItem::Action {
							id: format!("plot:new:{plot_type:?}"),
							label: plot_type.to_string(),
						})
						.collect(),
				},
				MenuItem::Submenu {
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
				},
			],
		},
		MenuRoot {
			id: "settings".into(),
			label: "Settings".into(),
			items: vec![MenuItem::Action {
				id: "settings:preferences".into(),
				label: "Preferences".into(),
			}],
		},
	]
}

fn build_saved_connection_items(saved_connections: &[SavedConnection]) -> Vec<MenuItem> {
	if saved_connections.is_empty() {
		return vec![MenuItem::Action {
			id: "noop".into(),
			label: "None saved".into(),
		}];
	}

	saved_connections
		.iter()
		.map(|conn| MenuItem::Submenu {
			id: format!("connect:saved:{}", conn.id),
			label: conn.name.clone(),
			items: vec![
				MenuItem::Action {
					id: format!("connect:load:{}", conn.id),
					label: "Load".into(),
				},
				MenuItem::Action {
					id: format!("connect:edit:{}", conn.id),
					label: "Edit".into(),
				},
				MenuItem::Action {
					id: format!("connect:delete:{}", conn.id),
					label: "Delete".into(),
				},
			],
		})
		.collect()
}

fn build_saved_statement_items(saved_statements: &[SavedStatement]) -> Vec<MenuItem> {
	if saved_statements.is_empty() {
		return vec![MenuItem::Action {
			id: "noop".into(),
			label: "None saved".into(),
		}];
	}

	saved_statements
		.iter()
		.map(|stmt| MenuItem::Submenu {
			id: format!("code:saved:{}", stmt.id),
			label: stmt.name.clone(),
			items: vec![
				MenuItem::Action {
					id: format!("code:load:{}", stmt.id),
					label: "Load".into(),
				},
				MenuItem::Action {
					id: format!("code:edit:{}", stmt.id),
					label: "Edit".into(),
				},
				MenuItem::Action {
					id: format!("code:delete:{}", stmt.id),
					label: "Delete".into(),
				},
			],
		})
		.collect()
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
	saved_connections: &'a [SavedConnection],
	saved_statements: &'a [SavedStatement],
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
					column![
						space::vertical().height(32),
						underlay.into(),
					],
					row![menu_bar(menu_state, saved_connections, saved_statements)],
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
	let dialog: Element<Message> = container(column![
		center(text("Select Adapter").size(24)),
		center(
			row![
				styled_button(
					"BigQuery",
					Message::AdapterSelected(AdapterSelection::BigQuery),
					BUTTON_SIZE_DEFAULT
				),
				styled_button(
					"MySQL",
					Message::AdapterSelected(AdapterSelection::MySQL),
					BUTTON_SIZE_DEFAULT
				),
				styled_button(
					"Parquet",
					Message::AdapterSelected(AdapterSelection::Parquet),
					BUTTON_SIZE_DEFAULT
				),
				styled_button(
					"Postgres",
					Message::AdapterSelected(AdapterSelection::Postgres),
					BUTTON_SIZE_DEFAULT
				),
				styled_button(
					"SQLite",
					Message::AdapterSelected(AdapterSelection::SQLite),
					BUTTON_SIZE_DEFAULT
				)
			]
			.spacing(20)
		)
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

fn password_prompt_view<'a>(password_entry: &'a str, error: &'a str) -> Element<'a, Message> {
	let error_el: Element<Message> = if error.is_empty() {
		space::vertical().height(24).into()
	} else {
		container(text(error).color(colors::DANGER).size(14))
			.height(24)
			.into()
	};
	let dialog: Element<Message> = container(
		column![
			row![text("Enter Password").size(24), space::horizontal()].align_y(Alignment::Center),
			section(
				"Password",
				column![
					styled_text_input("Enter password", password_entry)
						.secure(true)
						.on_input(Message::PasswordEntryChanged)
						.on_submit(Message::PasswordEntrySubmit),
					error_el,
				]
				.spacing(4),
			),
			row![
				space::horizontal(),
				styled_button("Unlock", Message::PasswordEntrySubmit, BUTTON_SIZE_DEFAULT),
			]
			.align_y(Alignment::Center),
		]
		.spacing(20)
		.padding(20),
	)
	.width(Length::Fixed(480.0))
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

fn save_statement_dialog_view<'a>(name: &'a str, is_editing: bool) -> Element<'a, Message> {
	let title = if is_editing { "Update Statement" } else { "Save Statement" };
	let btn_label = if is_editing { "Update" } else { "Save" };
	let dialog: Element<Message> = container(
		column![
			row![text(title).size(24), space::horizontal()].align_y(Alignment::Center),
			section(
				"Name",
				styled_text_input("Statement name", name)
					.on_input(Message::SaveStatementNameChanged)
					.on_submit(Message::SaveStatement),
			),
			row![
				space::horizontal(),
				styled_button("Cancel", Message::CloseSaveStatementDialog, (100, 40)),
				styled_button(btn_label, Message::SaveStatement, BUTTON_SIZE_DEFAULT),
			]
			.spacing(8)
			.align_y(Alignment::Center),
		]
		.spacing(20)
		.padding(20),
	)
	.width(Length::Fixed(480.0))
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

fn settings_dialog_view<'a>(
	new_password: &'a str,
	confirm_password: &'a str,
	error: &'a str,
	is_password_protected: bool,
	show_column_types: bool,
) -> Element<'a, Message> {
	let error_el: Element<Message> = if error.is_empty() {
		space::vertical().height(24).into()
	} else {
		container(text(error).color(colors::DANGER).size(14))
			.height(24)
			.into()
	};
	let apply_label = if is_password_protected {
		"Change"
	} else {
		"Set"
	};
	let mut action_row = row![
		space::horizontal(),
		styled_button(
			apply_label,
			Message::SettingsApplyPassword,
			BUTTON_SIZE_DEFAULT
		),
	]
	.spacing(10)
	.align_y(Alignment::Center);
	if is_password_protected {
		action_row = action_row.push(styled_button(
			"Remove",
			Message::SettingsRemovePassword,
			BUTTON_SIZE_DEFAULT,
		));
	}
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
			section(
				"Security",
				column![
					styled_text_input("New password", new_password)
						.secure(true)
						.on_input(Message::SettingsNewPasswordChanged)
						.on_submit(Message::SettingsApplyPassword),
					styled_text_input("Confirm password", confirm_password)
						.secure(true)
						.on_input(Message::SettingsConfirmPasswordChanged)
						.on_submit(Message::SettingsApplyPassword),
					error_el,
				]
				.spacing(8),
			),
			action_row,
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
								styled_text_input(descriptor.value, &current_value).secure(true)
							} else {
								styled_text_input(descriptor.value, &current_value)
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
				styled_button("Save", Message::SaveConnection, BUTTON_SIZE_DEFAULT),
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
