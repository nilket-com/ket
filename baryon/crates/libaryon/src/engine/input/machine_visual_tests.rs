#[cfg(test)]
mod tests {
	use super::super::keymap::{InputState, VisualPending};
	use super::super::machine_test_frontend::TestFrontend;
	use crate::core::DocByte;
	use crate::engine::{EditorCommand, EditorMode, KeyCode, Modifiers, MoveDirection, VisualKind};
	use std::sync::mpsc;

	/// Build a Frontend wired up with an `mpsc` test harness and park it in
	/// character-wise Visual mode anchored at byte 0. Every char-search test
	/// starts from this baseline so they can assert on the exact command
	/// stream produced by a key sequence.
	fn build_visual_frontend() -> (TestFrontend, mpsc::Receiver<EditorCommand>) {
		let (mut frontend, rx_cmd) = TestFrontend::new();
		frontend.set_editor_mode_for_test(EditorMode::Visual {
			anchor: DocByte::ZERO,
			kind: VisualKind::Char,
		});
		(frontend, rx_cmd)
	}

	/// Drain any `SetVisualSelection` / auxiliary commands the Frontend
	/// emits before the key press we actually care about so individual
	/// assertions are not polluted by entry-side noise.
	fn drain(rx: &mpsc::Receiver<EditorCommand>) {
		while rx.try_recv().is_ok() {}
	}

	#[test]
	fn visual_f_waits_for_char_then_dispatches_find_char_forward_inclusive() {
		let (mut frontend, rx_cmd) = build_visual_frontend();
		let mut should_quit = false;
		drain(&rx_cmd);

		assert!(!frontend.dispatch_key(KeyCode::Char('f'), Modifiers::NONE, &mut should_quit));
		// The first key only arms the pending search — no command yet.
		assert!(rx_cmd.try_recv().is_err());

		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect("fx should dispatch find char") {
			EditorCommand::MoveCursor(MoveDirection::FindChar {
				ch,
				forward,
				inclusive,
			}) => {
				assert_eq!(ch, 'x');
				assert!(forward);
				assert!(inclusive);
			}
			_ => panic!("unexpected command"),
		}

		let last = frontend
			.last_char_search
			.expect("last_char_search recorded from visual mode");
		assert_eq!(last.ch, 'x');
		assert!(last.forward);
		assert!(last.inclusive);

		// And the mode must remain Visual — char search extends the
		// selection, it does not leave Visual mode.
		assert!(matches!(frontend.editor_mode(), EditorMode::Visual { .. }));
	}

	#[test]
	fn visual_capital_t_is_backward_exclusive() {
		let (mut frontend, rx_cmd) = build_visual_frontend();
		let mut should_quit = false;
		drain(&rx_cmd);

		assert!(!frontend.dispatch_key(KeyCode::Char('T'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('q'), Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect("Tq should dispatch") {
			EditorCommand::MoveCursor(MoveDirection::FindChar {
				ch,
				forward,
				inclusive,
			}) => {
				assert_eq!(ch, 'q');
				assert!(!forward);
				assert!(!inclusive);
			}
			_ => panic!("unexpected command"),
		}
	}

	#[test]
	fn visual_semicolon_repeats_last_char_search() {
		let (mut frontend, rx_cmd) = build_visual_frontend();
		let mut should_quit = false;
		drain(&rx_cmd);

		// Prime with fx.
		assert!(!frontend.dispatch_key(KeyCode::Char('f'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));
		let _ = rx_cmd.try_recv().expect("initial fx");

		// `;` repeats the same fx.
		assert!(!frontend.dispatch_key(KeyCode::Char(';'), Modifiers::NONE, &mut should_quit));
		match rx_cmd.try_recv().expect("; should repeat fx") {
			EditorCommand::MoveCursor(MoveDirection::FindChar {
				ch,
				forward,
				inclusive,
			}) => {
				assert_eq!(ch, 'x');
				assert!(forward);
				assert!(inclusive);
			}
			_ => panic!("unexpected command"),
		}
	}

	#[test]
	fn visual_comma_reverses_last_char_search_without_mutating_it() {
		let (mut frontend, rx_cmd) = build_visual_frontend();
		let mut should_quit = false;
		drain(&rx_cmd);

		// Prime with fx (forward, inclusive).
		assert!(!frontend.dispatch_key(KeyCode::Char('f'), Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));
		let _ = rx_cmd.try_recv().expect("initial fx");

		assert!(!frontend.dispatch_key(KeyCode::Char(','), Modifiers::NONE, &mut should_quit));
		match rx_cmd.try_recv().expect(", should reverse") {
			EditorCommand::MoveCursor(MoveDirection::FindChar {
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

		let last = frontend
			.last_char_search
			.expect("search must still be recorded");
		assert!(
			last.forward,
			"visual-mode `,` must not mutate the sticky search state"
		);
	}

	#[test]
	fn visual_pending_char_search_is_cleared_by_non_char_key() {
		let (mut frontend, rx_cmd) = build_visual_frontend();
		let mut should_quit = false;
		drain(&rx_cmd);

		// Arm a pending char search with `t`...
		assert!(!frontend.dispatch_key(KeyCode::Char('t'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			frontend.state,
			InputState::Visual {
				pending: VisualPending::FindChar(_),
				..
			}
		));

		// ...then deliver an Esc. The visual-mode handler's pending-search
		// branch routes non-char follow-ups through `clear_prefixes` and
		// returns early, so no FindChar command should be emitted and the
		// pending state must be cleared.
		assert!(!frontend.dispatch_key(KeyCode::Esc, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			frontend.state,
			InputState::Visual {
				pending: VisualPending::Ready,
				..
			}
		));
		assert!(rx_cmd.try_recv().is_err());
	}

	#[test]
	fn visual_iw_dispatches_select_inner_word() {
		let (mut frontend, rx_cmd) = build_visual_frontend();
		let mut should_quit = false;
		drain(&rx_cmd);

		assert!(!frontend.dispatch_key(KeyCode::Char('i'), Modifiers::NONE, &mut should_quit));
		assert!(
			rx_cmd.try_recv().is_err(),
			"i should only arm the text object"
		);

		assert!(!frontend.dispatch_key(KeyCode::Char('w'), Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("iw should dispatch SelectInnerWord"),
			EditorCommand::SelectInnerWord
		));
		assert!(matches!(frontend.editor_mode(), EditorMode::Visual { .. }));
	}

	#[test]
	fn visual_colon_enters_command_mode_with_visual_range_marker() {
		let (mut frontend, rx_cmd) = build_visual_frontend();
		let mut should_quit = false;
		// Anchor was 0; pretend cursor has advanced to byte 42 via the viewport.
		frontend.set_editor_mode_for_test(EditorMode::Visual {
			anchor: DocByte::new(10),
			kind: VisualKind::Char,
		});
		drain(&rx_cmd);

		assert!(!frontend.dispatch_key(KeyCode::Char(':'), Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.editor_mode(), EditorMode::Command);
		assert_eq!(frontend.command_buffer, "'<,'>");
		let (start, end) = frontend
			.visual_command_range
			.expect("colon must snapshot the visual byte range");
		// Without a viewport the snapshot falls back to anchor for both ends,
		// but the important thing is that the field is populated so the
		// command parser can resolve `'<,'>`.
		assert_eq!(start, DocByte::new(10));
		assert_eq!(end, DocByte::new(10));
	}
}
