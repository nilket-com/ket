use super::machine_test_frontend::TestFrontend;
use crate::core::{
	CursorPosition, DisplayRow, DocByte, DocDisplayCol, DocLine, PaneCol, PaneId, PaneLocalCol,
	PaneLocalRow, PaneRect, PaneRow, ScreenCol, ScreenRow,
};
use crate::engine::{
	ConfirmAction, EditorCommand, EditorMode, GutterNumbering, KeyCode, Modifiers, MoveDirection,
	PaneFocusDirection, PaneResizeDirection, SplitDirection, VisualKind, WordMotion,
};
use crate::uast::{PaneViewport, Viewport};
use std::sync::mpsc;

fn build_frontend() -> (TestFrontend, mpsc::Receiver<EditorCommand>) {
	TestFrontend::new()
}

fn test_viewport() -> Viewport {
	let pane_id = PaneId::new(1);
	let view_id = crate::core::ViewId::new(1);
	Viewport {
		viewport_id: crate::core::ViewportId::new(1),
		panes: vec![PaneViewport {
			pane_id,
			buffer_id: crate::core::BufferId::new(1),
			view_id,
			rect: PaneRect::new(
				ScreenCol::ZERO,
				ScreenRow::ZERO,
				PaneCol::new(80),
				PaneRow::new(41),
			),
			tokens: Vec::new(),
			scroll_y: 0,
			viewport_start_line: DocLine::new(10),
			viewport_row_offset: DisplayRow::ZERO,
			viewport_line_count: 40,
			wrap_enabled: false,
			visible_whitespace: true,
			gutter_numbering: Some(GutterNumbering::Absolute),
			gutter_line_numbers: Vec::new(),
			cursor_lines: vec![DocLine::new(20)],
			cursor_pane_row: PaneLocalRow::new(10),
			cursor_abs_pos: CursorPosition::new(DocLine::new(20), DocDisplayCol::new(5)),
			cursor_pane_col: PaneLocalCol::new(5),
			cursor_abs_byte: DocByte::new(100),
			cursor_line_start_byte: DocByte::new(95),
			secondary_carets: Vec::new(),
			cursor_count: 1,
			max_doc_line: DocLine::new(200),
			file_name: Some("test.rs".to_string()),
			file_size: 5000,
			line_count: Some(201),
			is_dirty: false,
			search_match_info: None,
			global_start_byte: DocByte::ZERO,
			highlights: Vec::new(),
			diagnostics: Vec::new(),
			inline_annotations: Vec::new(),
			selection_ranges: Vec::new(),
			yank_flash: None,
			minimap: None,
			markdown_cells: Vec::new(),
			markdown_preview: None,
			uast_graph: None,
			host_panel: None,
			theme_colors: Default::default(),
			render_rows: Vec::new(),
			visible_rows: Vec::new(),
			visible_line_starts: Vec::new(),
			dma_pending: false,
			language_id: Some(crate::engine::language::LanguageId::from_registered("rust")),
			language_label: "Rust",
			is_file_browser: false,
			is_output_panel: false,
			is_git_status_panel: false,
			background_override: None,
			tabs: vec![crate::uast::projection::DocTabSummary {
				buffer_id: crate::core::BufferId::new(1),
				view_id,
				title: "test.rs".to_string(),
				is_dirty: false,
				snapshot: None,
			}],
			active_doctab_index: 0,
			diff_pair: None,
		}],
		focused_pane_id: pane_id,
		git_branch: None,
		git_status_repo_root: None,
		git_refresh_mode: crate::engine::GitRefreshMode::Interval(7),
		status_message: None,
		document_text: None,
		document_text_buffer_id: None,
		dividers: Vec::new(),
		busy: false,
		should_quit: false,
		search_pattern: None,
		search_case_insensitive: false,
		ui_palette: crate::engine::UiPalette::default(),
		confirm_prompt: None,
		input: crate::engine::InputProjection::default(),
		language_completion_candidates: Vec::new(),
	}
}

fn with_viewport(frontend: &mut TestFrontend) {
	frontend.current_viewport = Some(test_viewport());
}

fn press(frontend: &mut TestFrontend, code: KeyCode) -> bool {
	press_with(frontend, code, Modifiers::NONE)
}

fn press_with(frontend: &mut TestFrontend, code: KeyCode, modifiers: Modifiers) -> bool {
	let mut should_quit = false;
	let stopped = frontend.dispatch_key(code, modifiers, &mut should_quit);
	assert_eq!(stopped, should_quit);
	stopped
}

fn chars(frontend: &mut TestFrontend, sequence: &str) {
	for ch in sequence.chars() {
		assert!(!press(frontend, KeyCode::Char(ch)));
	}
}

fn drain(rx: &mpsc::Receiver<EditorCommand>) {
	while rx.try_recv().is_ok() {}
}

#[test]
fn normal_register_and_prefix_sequences_are_characterized() {
	for (sequence, expected_register) in
		[("yy", '"'), ("\"ayy", 'a'), ("y\"ay", 'a'), ("\"add", 'a')]
	{
		let (mut frontend, rx) = build_frontend();
		chars(&mut frontend, sequence);
		match rx.try_recv().expect("sequence should emit a command") {
			EditorCommand::YankLine { register } | EditorCommand::DeleteLine { register } => {
				assert_eq!(register, expected_register, "sequence {sequence}");
			}
			other => panic!(
				"unexpected command for {sequence}: {:?}",
				std::mem::discriminant(&other)
			),
		}
	}

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "\"acw");
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::ChangeWordMotion {
			register: 'a',
			motion: WordMotion::NextStart,
		})
	));

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "g\"ap");
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::Put { register: 'a' })
	));

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "\"agp");
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::Put { register: '"' })
	));

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "g\"at");
	assert!(matches!(rx.try_recv(), Ok(EditorCommand::DocTabNext)));
}

#[test]
fn operator_objects_and_cancellation_are_characterized() {
	for (sequence, expected) in [
		("diw", "delete-inner"),
		("ciw", "change-inner"),
		("daf", "delete-around"),
		("caf", "change-around"),
	] {
		let (mut frontend, rx) = build_frontend();
		chars(&mut frontend, sequence);
		let command = rx.try_recv().expect("operator object command");
		assert!(
			matches!(
				(expected, command),
				("delete-inner", EditorCommand::DeleteInnerWord)
					| ("change-inner", EditorCommand::ChangeInnerWord)
					| (
						"delete-around",
						EditorCommand::DeleteAroundFunction { register: '"' }
					) | ("change-around", EditorCommand::ChangeAroundFunction)
			),
			"sequence {sequence}"
		);
	}

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "yj");
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::MoveCursor(MoveDirection::Down))
	));

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "dl");
	assert!(rx.try_recv().is_err());

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "daw");
	assert!(rx.try_recv().is_err());

	let (mut frontend, rx) = build_frontend();
	press(&mut frontend, KeyCode::Char('d'));
	press(&mut frontend, KeyCode::Esc);
	assert!(rx.try_recv().is_err());
	press(&mut frontend, KeyCode::Char('l'));
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::MoveCursor(MoveDirection::Right))
	));
}

#[test]
fn modified_keys_do_not_fall_through_to_plain_or_wildcard_bindings() {
	let (mut frontend, rx) = build_frontend();
	press_with(&mut frontend, KeyCode::Char('h'), Modifiers::CONTROL);
	assert!(rx.try_recv().is_err());

	let (mut frontend, rx) = build_frontend();
	press(&mut frontend, KeyCode::Char('d'));
	press_with(&mut frontend, KeyCode::Char('w'), Modifiers::CONTROL);
	assert!(rx.try_recv().is_err());

	let (mut frontend, rx) = build_frontend();
	press(&mut frontend, KeyCode::Char('"'));
	press_with(&mut frontend, KeyCode::Char('z'), Modifiers::CONTROL);
	press(&mut frontend, KeyCode::Char('p'));
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::Put { register: '"' })
	));

	let (mut frontend, rx) = build_frontend();
	press(&mut frontend, KeyCode::Char('r'));
	press_with(&mut frontend, KeyCode::Char('c'), Modifiers::CONTROL);
	assert!(rx.try_recv().is_err());
	press(&mut frontend, KeyCode::Char('x'));
	assert!(matches!(rx.try_recv(), Ok(EditorCommand::Delete)));
}

#[test]
fn alt_j_and_k_add_cursors_in_normal_and_insert_modes() {
	for (key, direction) in [
		('j', crate::engine::VerticalDirection::Down),
		('k', crate::engine::VerticalDirection::Up),
	] {
		let (mut frontend, rx) = build_frontend();
		press_with(&mut frontend, KeyCode::Char(key), Modifiers::ALT);
		assert!(matches!(
			rx.try_recv(),
			Ok(EditorCommand::AddCursorAdjacent(actual)) if actual == direction
		));

		let (mut frontend, rx) = build_frontend();
		press(&mut frontend, KeyCode::Char('i'));
		press_with(&mut frontend, KeyCode::Char(key), Modifiers::ALT);
		assert!(matches!(
			rx.try_recv(),
			Ok(EditorCommand::AddCursorAdjacent(actual)) if actual == direction
		));
	}
}

#[test]
fn normal_escape_collapses_an_active_cursor_set() {
	let (mut frontend, rx) = build_frontend();
	frontend.current_viewport = Some(test_viewport());
	frontend
		.current_viewport
		.as_mut()
		.expect("viewport")
		.focused_pane_mut()
		.expect("focused pane")
		.cursor_count = 2;
	press(&mut frontend, KeyCode::Esc);
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::ClearVisualSelection)
	));
	assert!(matches!(rx.try_recv(), Ok(EditorCommand::CollapseCursors)));
}

#[test]
fn folds_brackets_and_panes_are_characterized() {
	for (sequence, expected) in [
		("za", "toggle"),
		("zc", "close"),
		("zo", "open"),
		("zM", "close-all"),
		("zR", "open-all"),
		("]m", "next-function"),
	] {
		let (mut frontend, rx) = build_frontend();
		chars(&mut frontend, sequence);
		let command = rx.try_recv().expect("prefix command");
		assert!(matches!(
			(expected, command),
			("toggle", EditorCommand::ToggleFold)
				| ("close", EditorCommand::CloseFold)
				| ("open", EditorCommand::OpenFold)
				| ("close-all", EditorCommand::CloseAllFolds)
				| ("open-all", EditorCommand::OpenAllFolds)
				| ("next-function", EditorCommand::NextFunction)
		));
	}

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "]x");
	assert!(rx.try_recv().is_err());

	for (sequence, expected) in [
		("ss", "split-h"),
		("sv", "split-v"),
		("sh", "focus-l"),
		("sj", "focus-d"),
		("sk", "focus-u"),
		("sl", "focus-r"),
		("sc", "close"),
	] {
		let (mut frontend, rx) = build_frontend();
		chars(&mut frontend, sequence);
		let command = rx.try_recv().expect("pane command");
		assert!(matches!(
			(expected, command),
			(
				"split-h",
				EditorCommand::SplitPane(SplitDirection::Horizontal)
			) | (
				"split-v",
				EditorCommand::SplitPane(SplitDirection::Vertical)
			) | (
				"focus-l",
				EditorCommand::FocusPaneDirection(PaneFocusDirection::Left)
			) | (
				"focus-d",
				EditorCommand::FocusPaneDirection(PaneFocusDirection::Down)
			) | (
				"focus-u",
				EditorCommand::FocusPaneDirection(PaneFocusDirection::Up)
			) | (
				"focus-r",
				EditorCommand::FocusPaneDirection(PaneFocusDirection::Right)
			) | ("close", EditorCommand::ClosePane)
		));
	}

	let (mut frontend, rx) = build_frontend();
	chars(&mut frontend, "srhhl");
	for expected in [
		(SplitDirection::Vertical, PaneResizeDirection::Decrease),
		(SplitDirection::Vertical, PaneResizeDirection::Decrease),
		(SplitDirection::Vertical, PaneResizeDirection::Increase),
	] {
		assert!(matches!(
			rx.try_recv(),
			Ok(EditorCommand::ResizePane(split, resize)) if (split, resize) == expected
		));
	}
	press(&mut frontend, KeyCode::Esc);
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::ClearVisualSelection)
	));
	press(&mut frontend, KeyCode::Char('h'));
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::MoveCursor(MoveDirection::Left))
	));
}

#[test]
fn file_browser_dispatch_and_register_preservation_are_characterized() {
	let (mut frontend, rx) = build_frontend();
	with_viewport(&mut frontend);
	frontend.current_viewport.as_mut().unwrap().panes[0].is_file_browser = true;

	for (key, expected) in [
		(KeyCode::Char('h'), "collapse"),
		(KeyCode::Char('j'), "down"),
		(KeyCode::Char('k'), "up"),
		(KeyCode::Char('l'), "activate"),
		(KeyCode::Enter, "activate"),
		(KeyCode::Char('r'), "refresh"),
		(KeyCode::Char('H'), "top-line"),
		(KeyCode::Char('M'), "middle-line"),
		(KeyCode::Char('L'), "bottom-line"),
	] {
		press(&mut frontend, key);
		let command = rx.try_recv().expect("browser command");
		let correct = match (expected, command) {
			("collapse", EditorCommand::CollapseBrowserEntryOrParent)
			| ("down", EditorCommand::MoveCursor(MoveDirection::Down))
			| ("up", EditorCommand::MoveCursor(MoveDirection::Up))
			| ("activate", EditorCommand::ActivateBrowserEntry)
			| ("refresh", EditorCommand::RefreshFileBrowser) => true,
			("top-line", EditorCommand::GotoLine(line)) => line == DocLine::new(10),
			("middle-line", EditorCommand::GotoLine(line)) => line == DocLine::new(30),
			("bottom-line", EditorCommand::GotoLine(line)) => line == DocLine::new(49),
			_ => false,
		};
		assert!(correct, "wrong browser command for {expected}");
		assert!(
			rx.try_recv().is_err(),
			"browser H/M/L must not emit FirstNonWhitespace"
		);
	}

	press(&mut frontend, KeyCode::Char('i'));
	assert_eq!(
		frontend.status_message.as_deref(),
		Some("File browser is read-only")
	);
	assert!(rx.try_recv().is_err());

	frontend.current_viewport.as_mut().unwrap().panes[0].is_file_browser = false;
	chars(&mut frontend, "\"a");
	frontend.current_viewport.as_mut().unwrap().panes[0].is_file_browser = true;
	press(&mut frontend, KeyCode::Char('g'));
	frontend.current_viewport.as_mut().unwrap().panes[0].is_file_browser = false;
	press(&mut frontend, KeyCode::Char('p'));
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::Put { register: 'a' })
	));
}

#[test]
fn gd_dispatches_go_to_definition_and_clears_the_prefix() {
	let (mut frontend, rx) = build_frontend();
	with_viewport(&mut frontend);

	chars(&mut frontend, "gd");
	assert!(matches!(rx.try_recv(), Ok(EditorCommand::GotoDefinition)));
	assert!(rx.try_recv().is_err());

	press(&mut frontend, KeyCode::Char('j'));
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::MoveCursor(MoveDirection::Down))
	));
}

#[test]
fn visual_mode_sweep_is_characterized() {
	for (key, expected_kind) in [
		(KeyCode::Char('v'), VisualKind::Char),
		(KeyCode::Char('V'), VisualKind::Line),
	] {
		let (mut frontend, rx) = build_frontend();
		with_viewport(&mut frontend);
		press(&mut frontend, key);
		assert!(matches!(
			frontend.editor_mode(),
			EditorMode::Visual { kind, .. } if kind == expected_kind
		));
		assert!(matches!(
			rx.try_recv(),
			Ok(EditorCommand::SetVisualSelection { kind, .. }) if kind == expected_kind
		));
	}

	let (mut frontend, rx) = build_frontend();
	with_viewport(&mut frontend);
	press_with(&mut frontend, KeyCode::Char('v'), Modifiers::CONTROL);
	assert!(matches!(
		frontend.editor_mode(),
		EditorMode::Visual {
			kind: VisualKind::Block,
			..
		}
	));
	drain(&rx);

	for key in ['h', 'j', 'k', 'l', 'w', 'b', 'e'] {
		press(&mut frontend, KeyCode::Char(key));
		assert!(matches!(frontend.editor_mode(), EditorMode::Visual { .. }));
	}
	drain(&rx);
	chars(&mut frontend, "gg");
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::MoveCursor(MoveDirection::Top))
	));
	chars(&mut frontend, "af");
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::SelectAroundFunction)
	));
	press_with(&mut frontend, KeyCode::Char('u'), Modifiers::CONTROL);
	assert!(matches!(rx.try_recv(), Ok(EditorCommand::Scroll(-20))));

	for (operator, expected) in [('y', "yank"), ('d', "delete"), ('c', "change")] {
		let (mut frontend, rx) = build_frontend();
		with_viewport(&mut frontend);
		press(&mut frontend, KeyCode::Char('v'));
		drain(&rx);
		press(&mut frontend, KeyCode::Char(operator));
		let command = rx.try_recv().expect("visual operator");
		assert!(matches!(
			(expected, command),
			("yank", EditorCommand::VisualYank { .. })
				| ("delete", EditorCommand::VisualDelete { .. })
				| ("change", EditorCommand::VisualChange { .. })
		));
		assert_eq!(frontend.editor_mode(), EditorMode::Normal);
	}
}

#[test]
fn insert_confirm_and_prompt_keys_are_characterized() {
	let (mut frontend, rx) = build_frontend();
	press(&mut frontend, KeyCode::Char('i'));
	for (code, expected) in [
		(KeyCode::Enter, "newline"),
		(KeyCode::Tab, "tab"),
		(KeyCode::Backspace, "backspace"),
		(KeyCode::Delete, "delete"),
		(KeyCode::Left, "left"),
		(KeyCode::Home, "home"),
	] {
		press(&mut frontend, code);
		let command = rx.try_recv().expect("insert command");
		assert!(matches!(
			(expected, command),
			("newline", EditorCommand::InsertChar('\n'))
				| ("tab", EditorCommand::InsertChar('\t'))
				| ("backspace", EditorCommand::Backspace)
				| ("delete", EditorCommand::Delete)
				| ("left", EditorCommand::MoveCursor(MoveDirection::Left))
				| ("home", EditorCommand::SmartHome)
		));
	}
	press_with(&mut frontend, KeyCode::Char('x'), Modifiers::CONTROL);
	assert!(rx.try_recv().is_err());
	press(&mut frontend, KeyCode::Esc);
	assert_eq!(frontend.editor_mode(), EditorMode::Normal);

	for (key, modifiers, expected) in [
		('y', Modifiers::NONE, Some(ConfirmAction::Yes)),
		('y', Modifiers::CONTROL, None),
		('n', Modifiers::NONE, Some(ConfirmAction::No)),
		('a', Modifiers::NONE, Some(ConfirmAction::All)),
		('q', Modifiers::NONE, Some(ConfirmAction::Quit)),
	] {
		let (mut frontend, rx) = build_frontend();
		frontend.set_editor_mode_for_test(EditorMode::Confirm);
		press_with(&mut frontend, KeyCode::Char(key), modifiers);
		if let Some(expected) = expected {
			assert!(matches!(
				rx.try_recv(),
				Ok(EditorCommand::ConfirmResponse(action)) if action == expected
			));
		} else {
			assert!(rx.try_recv().is_err());
		}
	}

	for mode in [EditorMode::Command, EditorMode::Search] {
		let (mut frontend, _rx) = build_frontend();
		frontend.set_editor_mode_for_test(mode);
		press(&mut frontend, KeyCode::Backspace);
		assert_eq!(frontend.editor_mode(), EditorMode::Normal);
	}
}

#[test]
fn actual_mode_override_clears_pending_input() {
	let (mut frontend, rx) = build_frontend();
	press(&mut frontend, KeyCode::Char('d'));
	frontend.apply_mode_override(EditorMode::Insert);
	frontend.apply_mode_override(EditorMode::Normal);
	press(&mut frontend, KeyCode::Char('w'));
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::MoveCursor(MoveDirection::NextWord))
	));
}

#[test]
fn same_mode_override_preserves_pending_input() {
	let (mut frontend, rx) = build_frontend();
	press(&mut frontend, KeyCode::Char('d'));
	frontend.apply_mode_override(EditorMode::Normal);
	press(&mut frontend, KeyCode::Char('w'));
	assert!(matches!(
		rx.try_recv(),
		Ok(EditorCommand::DeleteWordMotion {
			register: '"',
			motion: WordMotion::NextStart,
		})
	));
}
