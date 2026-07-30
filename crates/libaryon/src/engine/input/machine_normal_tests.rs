#[cfg(test)]
mod tests {
	use super::super::keymap::{InputState, NormalPending};
	use super::super::machine_test_frontend::TestFrontend;
	use crate::engine::{EditorCommand, EditorMode, KeyCode, Modifiers, WordMotion};
	use std::sync::mpsc;

	fn build_frontend() -> (TestFrontend, mpsc::Receiver<EditorCommand>) {
		TestFrontend::new()
	}

	#[test]
	fn double_d_dispatches_delete_line() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('d'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('d'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("dd should send a command"),
			EditorCommand::DeleteLine { register: '"' }
		));
	}

	#[test]
	fn dw_dispatches_delete_word_motion() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('d'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('w'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("dw should send a command"),
			EditorCommand::DeleteWordMotion {
				register: '"',
				motion: WordMotion::NextStart,
			}
		));
	}

	#[test]
	fn ye_dispatches_yank_word_motion() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('y'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('e'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("ye should send a command"),
			EditorCommand::YankWordMotion {
				register: '"',
				motion: WordMotion::NextEnd,
			}
		));
	}

	#[test]
	fn cb_dispatches_change_word_motion() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('c'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('b'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("cb should send a command"),
			EditorCommand::ChangeWordMotion {
				register: '"',
				motion: WordMotion::PrevStart,
			}
		));
	}

	#[test]
	fn capital_j_dispatches_join_line() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('J'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("J should send a command"),
			EditorCommand::JoinLine
		));
	}

	#[test]
	fn percent_dispatches_match_delimiter() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('%'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("% should send a command"),
			EditorCommand::MatchDelimiter
		));
	}

	#[test]
	fn uast_inspector_keys_dispatch_jump_fold_and_follow_actions() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Normal);
		frontend.set_uast_inspector_for_test(true);

		for (key, expected) in [
			(KeyCode::Enter, "jump"),
			(KeyCode::Char(' '), "fold"),
			(KeyCode::Char('f'), "follow"),
		] {
			assert!(!frontend.dispatch_key(key, Modifiers::NONE, &mut should_quit));
			let command = rx_cmd.try_recv().expect("UAST key should dispatch");
			assert!(
				matches!(
					(expected, command),
					("jump", EditorCommand::ActivateUastInspectorRow)
						| ("fold", EditorCommand::ToggleUastInspectorFold)
						| ("follow", EditorCommand::ToggleUastInspectorFollowCursor)
				),
				"wrong UAST command for {expected}"
			);
		}
	}

	#[test]
	fn x_dispatches_delete() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("x should send a command"),
			EditorCommand::Delete
		));
	}

	#[test]
	fn browser_l_dispatches_activate_entry() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;
		frontend.current_viewport.as_mut().unwrap().panes[0].is_file_browser = true;

		assert!(!frontend.dispatch_key(KeyCode::Char('l'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("browser l should send a command"),
			EditorCommand::ActivateBrowserEntry
		));
	}

	#[test]
	fn browser_i_does_not_enter_insert_mode() {
		let (mut frontend, _rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;
		frontend.current_viewport.as_mut().unwrap().panes[0].is_file_browser = true;

		assert!(!frontend.dispatch_key(KeyCode::Char('i'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(frontend.editor_mode(), EditorMode::Normal));
		assert_eq!(
			frontend.status_message.as_deref(),
			Some("File browser is read-only")
		);
	}

	#[test]
	fn capital_x_dispatches_backspace() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('X'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("X should send a command"),
			EditorCommand::Backspace
		));
	}

	#[test]
	fn tilde_dispatches_toggle_case() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('~'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("~ should send a command"),
			EditorCommand::ToggleCaseAtCursor
		));
	}

	#[test]
	fn r_waits_for_char_then_replaces() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('r'), Modifiers::NONE, &mut should_quit));
		// r alone should not dispatch yet.
		assert!(rx_cmd.try_recv().is_err());

		assert!(!frontend.dispatch_key(KeyCode::Char('z'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect("rz should dispatch replace"),
			EditorCommand::ReplaceChar('z')
		));
	}

	#[test]
	fn append_enters_insert_mode_after_stepping_right() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('a'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("a should step right"),
			EditorCommand::MoveCursor(crate::engine::MoveDirection::Right)
		));
		assert_eq!(frontend.editor_mode(), EditorMode::Insert);
	}

	#[test]
	fn append_end_dispatches_atomic_command() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('A'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("A should dispatch AppendAtLineEnd"),
			EditorCommand::AppendAtLineEnd
		));
	}

	#[test]
	fn capital_i_moves_to_first_non_whitespace_then_inserts() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('I'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("I should move to first-non-ws"),
			EditorCommand::FirstNonWhitespace
		));
		assert_eq!(frontend.editor_mode(), EditorMode::Insert);
	}

	#[test]
	fn open_below_dispatches_atomic_command() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('o'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("o should dispatch OpenLineBelow"),
			EditorCommand::OpenLineBelow
		));
		// No extra commands — the engine handles everything atomically.
		assert!(rx_cmd.try_recv().is_err());
	}

	#[test]
	fn open_above_dispatches_atomic_command() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('O'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("O should dispatch OpenLineAbove"),
			EditorCommand::OpenLineAbove
		));
		// No extra commands — the engine handles everything atomically.
		assert!(rx_cmd.try_recv().is_err());
	}

	#[test]
	fn f_waits_for_char_then_dispatches_find_char_and_records_search() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('f'), Modifiers::NONE, &mut should_quit));
		assert!(rx_cmd.try_recv().is_err());

		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));

		let cmd = rx_cmd.try_recv().expect("fx should dispatch find char");
		match cmd {
			EditorCommand::MoveCursor(crate::engine::MoveDirection::FindChar {
				ch,
				forward,
				inclusive,
			}) => {
				assert_eq!(ch, 'x');
				assert!(forward);
				assert!(inclusive);
			}
			other => panic!("unexpected command: {:?}", std::mem::discriminant(&other)),
		}

		let last = frontend
			.last_char_search
			.expect("last_char_search recorded");
		assert_eq!(last.ch, 'x');
		assert!(last.forward);
		assert!(last.inclusive);
	}

	#[test]
	fn semicolon_repeats_last_char_search_same_direction() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		// T'x' — backward, exclusive.
		assert!(!frontend.dispatch_key(KeyCode::Char('T'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));
		let _ = rx_cmd.try_recv().expect("initial Tx");

		// `;` should repeat the same Tx motion.
		assert!(!frontend.dispatch_key(KeyCode::Char(';'), Modifiers::NONE, &mut should_quit));
		match rx_cmd.try_recv().expect("; should repeat") {
			EditorCommand::MoveCursor(crate::engine::MoveDirection::FindChar {
				ch,
				forward,
				inclusive,
			}) => {
				assert_eq!(ch, 'x');
				assert!(!forward);
				assert!(!inclusive);
			}
			_ => panic!("unexpected command"),
		}
	}

	#[test]
	fn comma_repeats_last_char_search_opposite_direction() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		// f'x' — forward, inclusive.
		assert!(!frontend.dispatch_key(KeyCode::Char('f'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));
		let _ = rx_cmd.try_recv().expect("initial fx");

		// `,` should dispatch the same char in the opposite direction, but
		// it must NOT mutate the recorded search state.
		assert!(!frontend.dispatch_key(KeyCode::Char(','), Modifiers::NONE, &mut should_quit));
		match rx_cmd.try_recv().expect(", should reverse") {
			EditorCommand::MoveCursor(crate::engine::MoveDirection::FindChar {
				ch,
				forward,
				inclusive,
			}) => {
				assert_eq!(ch, 'x');
				assert!(!forward, "comma flips direction");
				assert!(inclusive, "comma preserves inclusivity");
			}
			_ => panic!("unexpected command"),
		}
		let last = frontend.last_char_search.expect("still recorded");
		assert!(last.forward, "comma must not mutate the sticky search");
	}

	fn build_frontend_with_viewport() -> (TestFrontend, mpsc::Receiver<EditorCommand>) {
		use crate::core::{
			CursorPosition, DisplayRow, DocByte, DocDisplayCol, DocLine, PaneCol, PaneId,
			PaneLocalCol, PaneLocalRow, PaneRect, PaneRow, ScreenCol, ScreenRow,
		};
		use crate::engine::GutterNumbering;
		use crate::uast::{PaneViewport, Viewport};

		let (mut frontend, rx_cmd) = build_frontend();
		let pane_id = PaneId::new(1);
		let view_id = crate::core::ViewId::new(1);
		frontend.current_viewport = Some(Viewport {
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
			picker: None,
			input: crate::engine::InputProjection::default(),
			language_completion_candidates: Vec::new(),
		});
		(frontend, rx_cmd)
	}

	#[test]
	fn ctrl_u_dispatches_half_page_scroll_up() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('u'), Modifiers::CONTROL, &mut should_quit));

		match rx_cmd.try_recv().expect("Ctrl-u should dispatch Scroll") {
			EditorCommand::Scroll(delta) => {
				// viewport_line_count = 40, half = 20
				assert_eq!(delta, -20);
			}
			other => panic!("expected Scroll, got {:?}", std::mem::discriminant(&other)),
		}
	}

	#[test]
	fn ctrl_d_dispatches_half_page_scroll_down() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('d'), Modifiers::CONTROL, &mut should_quit));

		match rx_cmd.try_recv().expect("Ctrl-d should dispatch Scroll") {
			EditorCommand::Scroll(delta) => {
				assert_eq!(delta, 20);
			}
			other => panic!("expected Scroll, got {:?}", std::mem::discriminant(&other)),
		}
	}

	#[test]
	fn ctrl_b_dispatches_full_page_scroll_up() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('b'), Modifiers::CONTROL, &mut should_quit));

		match rx_cmd.try_recv().expect("Ctrl-b should dispatch Scroll") {
			EditorCommand::Scroll(delta) => {
				// viewport_line_count = 40, saturating_sub(2) = 38
				assert_eq!(delta, -38);
			}
			other => panic!("expected Scroll, got {:?}", std::mem::discriminant(&other)),
		}
	}

	#[test]
	fn ctrl_f_dispatches_full_page_scroll_down() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('f'), Modifiers::CONTROL, &mut should_quit));

		match rx_cmd.try_recv().expect("Ctrl-f should dispatch Scroll") {
			EditorCommand::Scroll(delta) => {
				assert_eq!(delta, 38);
			}
			other => panic!("expected Scroll, got {:?}", std::mem::discriminant(&other)),
		}
	}

	#[test]
	fn capital_h_goes_to_viewport_top() {
		use crate::core::DocLine;

		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('H'), Modifiers::NONE, &mut should_quit));

		// viewport_start_line = 10
		match rx_cmd.try_recv().expect("H should dispatch GotoLine") {
			EditorCommand::GotoLine(line) => {
				assert_eq!(line, DocLine::new(10));
			}
			other => panic!(
				"expected GotoLine, got {:?}",
				std::mem::discriminant(&other)
			),
		}
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("H should also dispatch FirstNonWhitespace"),
			EditorCommand::FirstNonWhitespace
		));
	}

	#[test]
	fn capital_m_goes_to_viewport_middle() {
		use crate::core::DocLine;

		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('M'), Modifiers::NONE, &mut should_quit));

		// viewport_start_line=10, viewport_line_count=40, middle = 10 + 20 = 30
		match rx_cmd.try_recv().expect("M should dispatch GotoLine") {
			EditorCommand::GotoLine(line) => {
				assert_eq!(line, DocLine::new(30));
			}
			other => panic!(
				"expected GotoLine, got {:?}",
				std::mem::discriminant(&other)
			),
		}
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("M should also dispatch FirstNonWhitespace"),
			EditorCommand::FirstNonWhitespace
		));
	}

	#[test]
	fn capital_l_goes_to_viewport_bottom() {
		use crate::core::DocLine;

		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('L'), Modifiers::NONE, &mut should_quit));

		// viewport_start_line=10, viewport_line_count=40, bottom = 10 + 39 = 49
		match rx_cmd.try_recv().expect("L should dispatch GotoLine") {
			EditorCommand::GotoLine(line) => {
				assert_eq!(line, DocLine::new(49));
			}
			other => panic!(
				"expected GotoLine, got {:?}",
				std::mem::discriminant(&other)
			),
		}
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("L should also dispatch FirstNonWhitespace"),
			EditorCommand::FirstNonWhitespace
		));
	}

	#[test]
	fn star_dispatches_search_word_under_cursor_forward() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('*'), Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect("* should dispatch SearchWordUnderCursor")
		{
			EditorCommand::SearchWordUnderCursor { forward } => {
				assert!(forward, "* searches forward");
			}
			other => panic!(
				"expected SearchWordUnderCursor, got {:?}",
				std::mem::discriminant(&other)
			),
		}
	}

	#[test]
	fn hash_dispatches_search_word_under_cursor_backward() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(KeyCode::Char('#'), Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect("# should dispatch SearchWordUnderCursor")
		{
			EditorCommand::SearchWordUnderCursor { forward } => {
				assert!(!forward, "# searches backward");
			}
			other => panic!(
				"expected SearchWordUnderCursor, got {:?}",
				std::mem::discriminant(&other)
			),
		}
	}

	#[test]
	fn question_mark_is_reserved_for_backward_search() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		assert!(!frontend.dispatch_key(
			KeyCode::Char('?'),
			Modifiers::NONE,
			&mut should_quit
		));
		assert!(
			rx_cmd.try_recv().is_err(),
			"normal-mode ? must remain unbound until backward search lands"
		);
	}

	#[test]
	fn escape_dispatches_clear_search_when_search_is_active() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.set_editor_mode_for_test(EditorMode::Normal);
		if let Some(view) = frontend.current_viewport.as_mut() {
			view.search_pattern = Some(r"\bpattern\b".to_string());
			view.search_match_info = Some("[2/6]".to_string());
		}

		assert!(!frontend.dispatch_key(KeyCode::Esc, Modifiers::NONE, &mut should_quit));

		// Esc now always clears any lingering visual selection first, then
		// clears the active search when one is present.
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("Esc should dispatch ClearVisualSelection"),
			EditorCommand::ClearVisualSelection
		));
		assert!(matches!(
			rx_cmd.try_recv().expect("Esc should clear active search"),
			EditorCommand::ClearSearch
		));
	}

	#[test]
	fn gt_dispatches_next_doctab_and_clears_g_prefix() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		assert!(!frontend.dispatch_key(KeyCode::Char('g'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			frontend.state,
			InputState::Normal(NormalPending::Goto { .. })
		));

		assert!(!frontend.dispatch_key(KeyCode::Char('t'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("gt should dispatch a command"),
			EditorCommand::DocTabNext
		));
		assert_eq!(frontend.state, InputState::normal());
	}

	#[test]
	fn g_capital_t_dispatches_previous_doctab_and_clears_g_prefix() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		assert!(!frontend.dispatch_key(KeyCode::Char('g'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			frontend.state,
			InputState::Normal(NormalPending::Goto { .. })
		));

		assert!(!frontend.dispatch_key(KeyCode::Char('T'), Modifiers::SHIFT, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("gT should dispatch a command"),
			EditorCommand::DocTabPrev
		));
		assert_eq!(frontend.state, InputState::normal());
	}

	#[test]
	fn s_v_dispatches_vertical_split_and_clears_prefix() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		assert!(!frontend.dispatch_key(KeyCode::Char('s'), Modifiers::NONE, &mut should_quit));
		assert_eq!(frontend.state, InputState::Normal(NormalPending::Pane));

		assert!(!frontend.dispatch_key(KeyCode::Char('v'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("sv should dispatch a command"),
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical)
		));
		assert_eq!(frontend.state, InputState::normal());
	}

	#[test]
	fn s_h_dispatches_focus_left_and_clears_prefix() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		assert!(!frontend.dispatch_key(KeyCode::Char('s'), Modifiers::NONE, &mut should_quit));
		assert_eq!(frontend.state, InputState::Normal(NormalPending::Pane));

		assert!(!frontend.dispatch_key(KeyCode::Char('h'), Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("sh should dispatch a command"),
			EditorCommand::FocusPaneDirection(crate::engine::PaneFocusDirection::Left)
		));
		assert_eq!(frontend.state, InputState::normal());
	}

	#[test]
	fn s_r_enters_sticky_pane_resize_mode() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		assert!(!frontend.dispatch_key(KeyCode::Char('s'), Modifiers::NONE, &mut should_quit));
		assert_eq!(frontend.state, InputState::Normal(NormalPending::Pane));

		assert!(!frontend.dispatch_key(KeyCode::Char('r'), Modifiers::NONE, &mut should_quit));

		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);
		assert!(
			rx_cmd.try_recv().is_err(),
			"sr should only arm resize mode, not dispatch a command"
		);
	}

	#[test]
	fn sticky_resize_mode_dispatches_spatial_resizes_and_stays_active() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		assert!(!frontend.dispatch_key(KeyCode::Char('s'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('r'), Modifiers::NONE, &mut should_quit));
		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);

		assert!(!frontend.dispatch_key(KeyCode::Char('h'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("resize h should dispatch a command"),
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Vertical,
				crate::engine::PaneResizeDirection::Decrease
			)
		));
		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);

		assert!(!frontend.dispatch_key(KeyCode::Char('j'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("resize j should dispatch a command"),
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Horizontal,
				crate::engine::PaneResizeDirection::Increase
			)
		));
		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);

		assert!(!frontend.dispatch_key(KeyCode::Char('k'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("resize k should dispatch a command"),
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Horizontal,
				crate::engine::PaneResizeDirection::Decrease
			)
		));
		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);

		assert!(!frontend.dispatch_key(KeyCode::Char('l'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("resize l should dispatch a command"),
			EditorCommand::ResizePane(
				crate::engine::SplitDirection::Vertical,
				crate::engine::PaneResizeDirection::Increase
			)
		));
		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);
	}

	#[test]
	fn sticky_resize_mode_ignores_non_resize_keys_until_escape() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;

		assert!(!frontend.dispatch_key(KeyCode::Char('s'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('r'), Modifiers::NONE, &mut should_quit));
		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);

		assert!(!frontend.dispatch_key(KeyCode::Char('i'), Modifiers::NONE, &mut should_quit));

		assert_eq!(
			frontend.editor_mode(),
			EditorMode::Normal,
			"resize mode should not fall through into normal commands"
		);
		assert_eq!(
			frontend.state,
			InputState::Normal(NormalPending::PaneResize)
		);
		assert!(
			rx_cmd.try_recv().is_err(),
			"non-resize keys should be ignored while resize mode is active"
		);
	}

	#[test]
	fn escape_without_active_search_only_clears_prefixes() {
		let (mut frontend, rx_cmd) = build_frontend_with_viewport();
		let mut should_quit = false;

		frontend.state = InputState::Normal(NormalPending::PaneResize);

		assert!(!frontend.dispatch_key(KeyCode::Esc, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.state, InputState::normal());
		// Esc unconditionally clears any lingering visual selection so
		// highlighting from a previous `v`/`V` session is dismissed.
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("Esc should dispatch ClearVisualSelection"),
			EditorCommand::ClearVisualSelection
		));
		assert!(
			rx_cmd.try_recv().is_err(),
			"Esc should not dispatch further commands when no search is active"
		);
	}
}
