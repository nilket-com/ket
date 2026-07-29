use super::history::{HistoryStore, LoadedHistory};
use super::keymap::{
	BindingSpec, BrowserOp, ConfirmKeyAction, Context, EditOp, InputState, InsertAction,
	InsertEntry, MotionOp, NormalAction, NormalContext, NormalMotion, NormalPending, Operator,
	OperatorTarget, PaneOp, PromptAction, PromptOutcome, RegisterContinuation, ScrollOp, SearchOp,
	TabOp, Unmatched, ViewportPos, VisualAction, VisualContext, VisualMotion, VisualOp,
	VisualPending, YankTarget, arm_target, binding_is_applicable, normal_context, on_unmatched,
	prompt_kind, resolve_confirm, resolve_insert, resolve_normal, resolve_prompt, resolve_visual,
	visual_context,
};
use super::prompt::{PathCompletionController, PromptCursorMovement};
use super::runtime_keymap::{
	EffectiveKeymap, MappingLookup, format_sequence, is_builtin_prefix, mapping_mode_for,
	normalize_stroke,
};
use crate::core::{DocByte, DocLine, DocLineCount};
use crate::engine::request::InternalSender;
use crate::engine::runtime_registry::{
	InvocationSnapshot, KeyStroke, MappingMode, ResolvedMappingTarget, RuntimeRegistries,
};
use crate::engine::{EditorCommand, EditorMode, KeyCode, Modifiers, MoveDirection, VisualKind};
use crate::engine::{InputCompletionStatus, InputProjection};
use zeroize::{Zeroize, Zeroizing};

#[derive(Debug, Clone, Copy, Default)]
pub(crate) struct InputContext {
	pub(crate) is_file_browser: bool,
	pub(crate) is_uast_inspector: bool,
	/// The focused pane is host-rendered, so document-shaped input is declined
	/// rather than acted on.
	pub(crate) is_host_panel: bool,
	pub(crate) cursor_available: bool,
	pub(crate) viewport_line_count: u32,
	pub(crate) viewport_start_line: DocLine,
	pub(crate) cursor_abs_byte: DocByte,
	pub(crate) cursor_line_start_byte: DocByte,
	pub(crate) cursor_line: DocLine,
	pub(crate) cursor_count: usize,
	pub(crate) active_search: bool,
}

#[derive(Debug, Default)]
pub(crate) struct InputDispatch {
	pub(crate) commands: Vec<EditorCommand>,
	pub(crate) dynamic_invocations: Vec<InvocationSnapshot>,
	pub(crate) status_message: Option<String>,
	pub(crate) clear_status: bool,
	pub(crate) needs_render: bool,
	/// A key the engine declined while a host panel held focus. The runtime
	/// forwards it to the frontend as an ordered host event.
	pub(crate) declined_input: Option<(KeyCode, Modifiers)>,
}

#[derive(Clone, Copy)]
pub(crate) struct LastCharSearch {
	pub(crate) ch: char,
	pub(crate) forward: bool,
	pub(crate) inclusive: bool,
}

pub(crate) struct InputRuntime {
	pub(super) state: InputState,
	pub(super) command_buffer: String,
	pub(super) command_cursor: usize,
	pub(super) command_generation: u64,
	pub(super) path_completion: PathCompletionController,
	pub(super) language_completion_candidates: Vec<String>,
	pub(super) last_char_search: Option<LastCharSearch>,
	pub(super) visual_command_range: Option<(DocByte, DocByte)>,
	pub(super) command_history: Vec<String>,
	pub(super) command_history_index: Option<usize>,
	pub(super) history_store: HistoryStore,
	pub(super) search_buffer: String,
	pub(super) search_cursor: usize,
	pub(super) search_history: Vec<String>,
	pub(super) search_history_index: Option<usize>,
	/// Kept separate from command/search buffers so it can never be added to
	/// history or surfaced in the viewport projection.
	pub(super) password_buffer: Zeroizing<String>,
	pub(super) password_label: Option<String>,
	pub(super) context: InputContext,
	pub(super) commands: Vec<EditorCommand>,
	pub(super) dynamic_invocations: Vec<InvocationSnapshot>,
	pub(super) status_message: Option<String>,
	pub(super) needs_render: bool,
	pub(super) completion_pending: bool,
	pub(super) effective_keymap: EffectiveKeymap,
	pub(super) runtime_mapping_mode: Option<MappingMode>,
	pub(super) runtime_mapping_sequence: Vec<KeyStroke>,
}

impl InputRuntime {
	pub(crate) fn new() -> Self {
		Self::with_history(LoadedHistory::empty())
	}

	#[cfg(not(test))]
	pub(crate) fn load_persistent_history(&mut self) {
		let history = LoadedHistory::load_default();
		self.command_history = history.command_history;
		self.search_history = history.search_history;
		self.history_store = history.store;
	}

	pub(crate) fn attach_internal_sender(&mut self, sender: InternalSender) {
		self.path_completion.attach_internal_sender(sender);
	}

	pub(crate) fn set_language_completion_candidates(&mut self, candidates: Vec<String>) {
		self.language_completion_candidates = candidates;
	}

	pub(super) fn with_history(history: LoadedHistory) -> Self {
		Self {
			state: InputState::normal(),
			command_buffer: String::new(),
			command_cursor: 0,
			command_generation: 0,
			path_completion: PathCompletionController::default(),
			language_completion_candidates: Vec::new(),
			last_char_search: None,
			visual_command_range: None,
			command_history: history.command_history,
			command_history_index: None,
			history_store: history.store,
			search_buffer: String::new(),
			search_cursor: 0,
			search_history: history.search_history,
			search_history_index: None,
			password_buffer: Zeroizing::new(String::new()),
			password_label: None,
			context: InputContext::default(),
			commands: Vec::new(),
			dynamic_invocations: Vec::new(),
			status_message: None,
			needs_render: false,
			completion_pending: false,
			effective_keymap: EffectiveKeymap::default(),
			runtime_mapping_mode: None,
			runtime_mapping_sequence: Vec::new(),
		}
	}

	pub(crate) fn editor_mode(&self) -> EditorMode {
		self.state.editor_mode()
	}

	pub(crate) fn apply_mode_override(&mut self, mode: EditorMode) -> bool {
		if self.editor_mode() == mode {
			return false;
		}
		self.invalidate_command_completion();
		self.state = InputState::from_editor_mode(mode);
		self.clear_runtime_mapping_pending();
		true
	}

	pub(crate) fn projection(&self) -> InputProjection {
		let (prompt_buffer, prompt_cursor, prompt_label, prompt_masked) = match self.state {
			InputState::Command => (
				self.command_buffer.clone(),
				self.command_cursor,
				None,
				false,
			),
			InputState::Search => (self.search_buffer.clone(), self.search_cursor, None, false),
			InputState::Password => (
				"•".repeat(self.password_buffer.chars().count()),
				self.password_buffer.chars().count(),
				self.password_label.clone(),
				true,
			),
			_ => (String::new(), 0, None, false),
		};
		InputProjection {
			mode: self.editor_mode(),
			prompt_label,
			prompt_masked,
			prompt_buffer,
			prompt_cursor,
			pending_sequence: self
				.runtime_mapping_mode
				.map(|_| format_sequence(&self.runtime_mapping_sequence))
				.or_else(|| pending_sequence(self.state).map(str::to_string)),
			completion: if self.completion_pending {
				InputCompletionStatus::Pending
			} else {
				InputCompletionStatus::Idle
			},
		}
	}

	#[cfg(test)]
	pub(crate) fn dispatch_key(
		&mut self,
		context: InputContext,
		code: KeyCode,
		modifiers: Modifiers,
	) -> InputDispatch {
		self.dispatch_key_with_registries(context, code, modifiers, &RuntimeRegistries::new())
	}

	pub(crate) fn dispatch_key_with_registries(
		&mut self,
		context: InputContext,
		code: KeyCode,
		modifiers: Modifiers,
		registries: &RuntimeRegistries,
	) -> InputDispatch {
		self.context = context;
		self.commands.clear();
		self.dynamic_invocations.clear();
		self.status_message = None;
		self.needs_render = true;
		let mode_before = self.editor_mode();
		let mut should_quit = false;
		// Password input is never eligible for user mappings: a mapping can
		// persist, log, or reroute keystrokes outside libaryon's secret boundary.
		let runtime_handled = !matches!(self.editor_mode(), EditorMode::Password)
			&& self.dispatch_runtime_mapping_key(code, modifiers, registries, &mut should_quit);
		if !runtime_handled {
			match self.editor_mode() {
				EditorMode::Normal => self.dispatch_normal_key(code, modifiers, &mut should_quit),
				EditorMode::Insert => self.dispatch_insert_key(code, modifiers, &mut should_quit),
				EditorMode::Command | EditorMode::Search => {
					self.dispatch_prompt_key(code, modifiers, &mut should_quit, registries)
				}
				EditorMode::Password => {
					self.dispatch_password_key(code, modifiers);
					true
				}
				EditorMode::Confirm => self.dispatch_confirm_key(code, modifiers, &mut should_quit),
				EditorMode::Visual { .. } => {
					self.dispatch_visual_key(code, modifiers, &mut should_quit)
				}
			};
		}
		if should_quit {
			self.send_command(EditorCommand::Quit);
		}
		// Engine-declines: with a host panel focused, Baryon keeps whatever its
		// own resolver treated as workspace control — including any mode change,
		// so `:` and `/` still open their prompts — and declines the rest. The
		// frontend never sees a key list; it just receives what was declined.
		let declined_input = if self.context.is_host_panel
			&& matches!(mode_before, EditorMode::Normal)
			&& matches!(self.editor_mode(), EditorMode::Normal)
			&& !self.commands.iter().any(EditorCommand::is_workspace_scoped)
		{
			self.commands.clear();
			Some((code, modifiers))
		} else {
			None
		};
		InputDispatch {
			commands: std::mem::take(&mut self.commands),
			dynamic_invocations: std::mem::take(&mut self.dynamic_invocations),
			status_message: self.status_message.take(),
			clear_status: true,
			needs_render: self.needs_render,
			declined_input,
		}
	}

	pub(crate) fn begin_password_prompt(&mut self, label: String) {
		self.password_buffer.zeroize();
		self.password_buffer = Zeroizing::new(String::new());
		self.password_label = Some(label);
		self.state = InputState::Password;
		self.clear_runtime_mapping_pending();
		self.needs_render = true;
	}

	fn dispatch_password_key(&mut self, code: KeyCode, modifiers: Modifiers) {
		match code {
			KeyCode::Esc => {
				self.clear_password_prompt();
			}
			KeyCode::Backspace => {
				self.password_buffer.pop();
			}
			KeyCode::Enter => {
				let buffer =
					std::mem::replace(&mut self.password_buffer, Zeroizing::new(String::new()));
				self.password_label = None;
				self.state = InputState::normal();
				let secret = crate::engine::SecretInput::new(buffer.as_bytes().to_vec());
				// `buffer` zeroizes its allocation on drop after the deliberately
				// isolated copy above moves into the redacted command payload.
				drop(buffer);
				self.send_command(EditorCommand::SubmitPassword(secret));
			}
			KeyCode::Char(ch)
				if !modifiers
					.intersects(Modifiers::CONTROL | Modifiers::ALT | Modifiers::SUPER) =>
			{
				self.password_buffer.push(ch);
			}
			_ => {}
		}
	}

	fn clear_password_prompt(&mut self) {
		self.password_buffer.zeroize();
		self.password_buffer = Zeroizing::new(String::new());
		self.password_label = None;
		self.state = InputState::normal();
	}

	pub(crate) fn dispatch_paste(&mut self, context: InputContext, data: String) -> InputDispatch {
		self.context = context;
		self.commands.clear();
		self.dynamic_invocations.clear();
		self.status_message = None;
		self.needs_render = false;
		self.handle_paste(data);
		InputDispatch {
			commands: std::mem::take(&mut self.commands),
			dynamic_invocations: std::mem::take(&mut self.dynamic_invocations),
			status_message: self.status_message.take(),
			clear_status: false,
			needs_render: self.needs_render,
			declined_input: None,
		}
	}

	pub(crate) fn send_command(&mut self, command: EditorCommand) {
		self.commands.push(command);
	}

	fn send_move(&mut self, direction: MoveDirection) {
		self.send_command(EditorCommand::MoveCursor(direction));
	}

	fn is_file_browser_view(&self) -> bool {
		self.context.is_file_browser
	}

	fn dispatch_runtime_mapping_key(
		&mut self,
		code: KeyCode,
		modifiers: Modifiers,
		registries: &RuntimeRegistries,
		should_quit: &mut bool,
	) -> bool {
		self.effective_keymap.refresh(registries);
		let continuing = self.runtime_mapping_mode.is_some();
		if !continuing && !self.runtime_mapping_can_start(code, modifiers) {
			return false;
		}
		let mode = self
			.runtime_mapping_mode
			.unwrap_or_else(|| mapping_mode_for(self.editor_mode(), self.is_file_browser_view()));
		let mut sequence = self.runtime_mapping_sequence.clone();
		sequence.push(normalize_stroke(code, modifiers));
		match self.effective_keymap.lookup(mode, &sequence) {
			MappingLookup::None if continuing => {
				// A builtin-prefix runtime sequence advanced both machines. Preserve the
				// builtin pending state and let it consume this sibling terminal.
				if is_builtin_prefix(mode, &self.runtime_mapping_sequence) {
					self.clear_runtime_mapping_pending();
					false
				} else {
					self.clear_prefixes();
					true
				}
			}
			MappingLookup::None => false,
			MappingLookup::Prefix => {
				self.runtime_mapping_mode = Some(mode);
				self.runtime_mapping_sequence = sequence.clone();
				!is_builtin_prefix(mode, &sequence)
			}
			MappingLookup::Terminal => {
				self.clear_prefixes();
				match registries
					.mapping_target(mode, &sequence)
					.expect("effective mapping terminal must retain a target")
				{
					ResolvedMappingTarget::Action(invocation) => {
						self.dynamic_invocations.push(invocation);
					}
					ResolvedMappingTarget::Command(command) => {
						if self.dispatch_command_text(registries, &command) {
							self.request_quit(should_quit);
						}
					}
				}
				true
			}
		}
	}

	fn runtime_mapping_can_start(&self, code: KeyCode, modifiers: Modifiers) -> bool {
		match self.state {
			InputState::Normal(NormalPending::Ready { .. })
			| InputState::Visual {
				pending: VisualPending::Ready,
				..
			}
			| InputState::Insert
			| InputState::Command
			| InputState::Search
			| InputState::Password
			| InputState::Confirm => true,
			InputState::Normal(_) => {
				let Some(context) = normal_context(self.state, self.is_file_browser_view()) else {
					return false;
				};
				on_unmatched(Context::Normal(context)) == Unmatched::FallThrough
					&& resolve_normal(context, code, modifiers).is_none()
			}
			InputState::Visual { .. } => false,
		}
	}

	fn clear_runtime_mapping_pending(&mut self) {
		self.runtime_mapping_mode = None;
		self.runtime_mapping_sequence.clear();
	}

	fn dispatch_find_char(&mut self, ch: char, forward: bool, inclusive: bool) {
		self.last_char_search = Some(LastCharSearch {
			ch,
			forward,
			inclusive,
		});
		self.send_move(MoveDirection::FindChar {
			ch,
			forward,
			inclusive,
		});
	}

	fn request_quit(&mut self, should_quit: &mut bool) -> bool {
		*should_quit = true;
		true
	}

	fn enter_visual_mode(&mut self, anchor: DocByte, kind: VisualKind) {
		self.state = InputState::Visual {
			anchor,
			kind,
			pending: VisualPending::Ready,
		};
		self.send_command(EditorCommand::SetVisualSelection { anchor, kind });
		self.clear_prefixes();
	}

	fn clear_prefixes(&mut self) {
		self.clear_runtime_mapping_pending();
		self.state = match self.state {
			InputState::Normal(_) => InputState::normal(),
			InputState::Visual { anchor, kind, .. } => InputState::Visual {
				anchor,
				kind,
				pending: VisualPending::Ready,
			},
			state => state,
		};
	}

	fn apply_cursor_style(&mut self) {
		self.needs_render = true;
	}
}

fn pending_sequence(state: InputState) -> Option<&'static str> {
	match state {
		InputState::Normal(NormalPending::Ready { register: Some(_) }) => Some("\""),
		InputState::Normal(NormalPending::RegisterCapture { continuation }) => {
			Some(match continuation {
				RegisterContinuation::Ready => "\"",
				RegisterContinuation::Yank => "y\"",
				RegisterContinuation::Goto => "g\"",
			})
		}
		InputState::Normal(NormalPending::FindChar(kind)) => Some(match kind {
			crate::engine::input::keymap::CharSearchKind::ForwardInclusive => "f",
			crate::engine::input::keymap::CharSearchKind::BackwardInclusive => "F",
			crate::engine::input::keymap::CharSearchKind::ForwardTill => "t",
			crate::engine::input::keymap::CharSearchKind::BackwardTill => "T",
		}),
		InputState::Normal(NormalPending::Operator { op, .. }) => Some(match op {
			Operator::Delete => "d",
			Operator::Change => "c",
		}),
		InputState::Normal(NormalPending::OperatorInner { op, .. }) => Some(match op {
			Operator::Delete => "di",
			Operator::Change => "ci",
		}),
		InputState::Normal(NormalPending::OperatorAround { op, .. }) => Some(match op {
			Operator::Delete => "da",
			Operator::Change => "ca",
		}),
		InputState::Normal(NormalPending::Replace) => Some("r"),
		InputState::Normal(NormalPending::Yank { .. }) => Some("y"),
		InputState::Normal(NormalPending::Goto { .. }) => Some("g"),
		InputState::Normal(NormalPending::Fold) => Some("z"),
		InputState::Normal(NormalPending::Bracket) => Some("]"),
		InputState::Normal(NormalPending::Pane) => Some("s"),
		InputState::Normal(NormalPending::PaneResize) => Some("sr"),
		InputState::Visual {
			pending: VisualPending::FindChar(kind),
			..
		} => Some(match kind {
			crate::engine::input::keymap::CharSearchKind::ForwardInclusive => "f",
			crate::engine::input::keymap::CharSearchKind::BackwardInclusive => "F",
			crate::engine::input::keymap::CharSearchKind::ForwardTill => "t",
			crate::engine::input::keymap::CharSearchKind::BackwardTill => "T",
		}),
		InputState::Visual {
			pending: VisualPending::Inner,
			..
		} => Some("i"),
		InputState::Visual {
			pending: VisualPending::Around,
			..
		} => Some("a"),
		InputState::Visual {
			pending: VisualPending::Goto,
			..
		} => Some("g"),
		_ => None,
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum DispatchControl {
	Continue,
	Quit,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct ExecutionOutcome {
	next: InputState,
	control: DispatchControl,
}

impl InputRuntime {
	fn execute(
		&mut self,
		old: InputState,
		code: KeyCode,
		binding: &BindingSpec,
	) -> ExecutionOutcome {
		self.execute_with_registries(old, code, binding, None)
	}

	fn execute_with_registries(
		&mut self,
		old: InputState,
		code: KeyCode,
		binding: &BindingSpec,
		registries: Option<&RuntimeRegistries>,
	) -> ExecutionOutcome {
		debug_assert!(binding_is_applicable(binding));
		self.state = old;
		let control = if self.execute_arm_transition(old, binding) {
			DispatchControl::Continue
		} else {
			match binding {
				BindingSpec::Normal { action, .. } => self.execute_normal(old, code, *action),
				BindingSpec::Visual {
					context, action, ..
				} => self.execute_visual(old, *context, code, *action),
				BindingSpec::Insert { action, .. } => self.execute_insert(code, *action),
				BindingSpec::Confirm { action, .. } => self.execute_confirm(*action),
				BindingSpec::Prompt { kind, action, .. } => {
					self.execute_prompt(*kind, code, *action, registries)
				}
			}
		};
		ExecutionOutcome {
			next: self.state,
			control,
		}
	}

	fn finish_execution(&mut self, outcome: ExecutionOutcome, should_quit: &mut bool) -> bool {
		self.state = outcome.next;
		match outcome.control {
			DispatchControl::Continue => false,
			DispatchControl::Quit => self.request_quit(should_quit),
		}
	}

	fn execute_arm_transition(&mut self, old: InputState, binding: &BindingSpec) -> bool {
		let Some(target) = arm_target(binding) else {
			return false;
		};
		self.state = match (old, target) {
			(InputState::Normal(pending), Context::Normal(target)) => {
				InputState::Normal(match target {
					NormalContext::Operator(op) => NormalPending::Operator {
						op,
						register: pending.captured_register().unwrap_or('"'),
					},
					NormalContext::OperatorInner(op) => {
						let NormalPending::Operator { register, .. } = pending else {
							return false;
						};
						NormalPending::OperatorInner { op, register }
					}
					NormalContext::OperatorAround(op) => {
						let NormalPending::Operator { register, .. } = pending else {
							return false;
						};
						NormalPending::OperatorAround { op, register }
					}
					NormalContext::Yank => NormalPending::Yank {
						register: pending.captured_register(),
					},
					NormalContext::Goto => NormalPending::Goto {
						register: if matches!(
							binding.context(),
							Context::Normal(NormalContext::FileBrowser)
						) {
							pending.captured_register()
						} else {
							None
						},
					},
					NormalContext::Fold => NormalPending::Fold,
					NormalContext::Bracket => NormalPending::Bracket,
					NormalContext::Pane => NormalPending::Pane,
					NormalContext::PaneResize => NormalPending::PaneResize,
					NormalContext::Replace => NormalPending::Replace,
					NormalContext::RegisterCapture => {
						let continuation = match pending {
							NormalPending::Yank { .. } => RegisterContinuation::Yank,
							NormalPending::Goto { .. } => RegisterContinuation::Goto,
							_ => RegisterContinuation::Ready,
						};
						NormalPending::RegisterCapture { continuation }
					}
					NormalContext::FindChar(search) => NormalPending::FindChar(search),
					NormalContext::Ready | NormalContext::FileBrowser => return false,
				})
			}
			(InputState::Visual { anchor, kind, .. }, Context::Visual(target)) => {
				InputState::Visual {
					anchor,
					kind,
					pending: match target {
						VisualContext::Ready => VisualPending::Ready,
						VisualContext::FindChar(search) => VisualPending::FindChar(search),
						VisualContext::Inner => VisualPending::Inner,
						VisualContext::Around => VisualPending::Around,
						VisualContext::Goto => VisualPending::Goto,
					},
				}
			}
			_ => return false,
		};
		true
	}

	pub(super) fn dispatch_prompt_key(
		&mut self,
		code: KeyCode,
		modifiers: Modifiers,
		should_quit: &mut bool,
		registries: &RuntimeRegistries,
	) -> bool {
		let Some(kind) = prompt_kind(self.state) else {
			return false;
		};
		let Some(binding) = resolve_prompt(kind, code, modifiers) else {
			return false;
		};
		let outcome = self.execute_with_registries(self.state, code, binding, Some(registries));
		self.finish_execution(outcome, should_quit)
	}

	fn execute_prompt(
		&mut self,
		kind: super::keymap::PromptKind,
		code: KeyCode,
		action: PromptAction,
		registries: Option<&RuntimeRegistries>,
	) -> DispatchControl {
		if action != PromptAction::Complete {
			self.invalidate_command_completion();
		}
		let outcome = match action {
			PromptAction::Quit => PromptOutcome::Quit,
			PromptAction::InsertChar => match code {
				KeyCode::Char(ch) => self.prompt_insert_char(kind, ch),
				_ => PromptOutcome::Stay,
			},
			PromptAction::Enter => self.prompt_enter(
				kind,
				registries.expect("prompt dispatch must carry the effective registries"),
			),
			PromptAction::Cancel => self.prompt_cancel(kind),
			PromptAction::HistoryUp => self.prompt_history(kind, true),
			PromptAction::HistoryDown => self.prompt_history(kind, false),
			PromptAction::Backspace => self.prompt_backspace(kind),
			PromptAction::Delete => self.prompt_delete(kind),
			PromptAction::CursorLeft => self.prompt_move_cursor(kind, PromptCursorMovement::Left),
			PromptAction::CursorRight => self.prompt_move_cursor(kind, PromptCursorMovement::Right),
			PromptAction::CursorHome => self.prompt_move_cursor(kind, PromptCursorMovement::Home),
			PromptAction::CursorEnd => self.prompt_move_cursor(kind, PromptCursorMovement::End),
			PromptAction::Complete => self.prompt_complete(
				registries.expect("prompt dispatch must carry the effective registries"),
			),
		};
		match outcome {
			PromptOutcome::Stay => DispatchControl::Continue,
			PromptOutcome::ToNormal => {
				self.state = InputState::normal();
				DispatchControl::Continue
			}
			PromptOutcome::Quit => DispatchControl::Quit,
		}
	}

	pub(super) fn dispatch_normal_key(
		&mut self,
		code: KeyCode,
		modifiers: Modifiers,
		should_quit: &mut bool,
	) -> bool {
		if self.context.is_uast_inspector && modifiers == Modifiers::NONE {
			let command = match code {
				KeyCode::Enter => Some(EditorCommand::ActivateUastInspectorRow),
				KeyCode::Char(' ') => Some(EditorCommand::ToggleUastInspectorFold),
				KeyCode::Char('f') => Some(EditorCommand::ToggleUastInspectorFollowCursor),
				_ => None,
			};
			if let Some(command) = command {
				self.send_command(command);
				self.state = InputState::normal();
				return true;
			}
		}
		let browser = self.is_file_browser_view();
		let Some(context) = normal_context(self.state, browser) else {
			return false;
		};
		let mut binding = resolve_normal(context, code, modifiers);
		if binding.is_none() {
			match on_unmatched(Context::Normal(context)) {
				Unmatched::FallThrough => {
					let base = if browser {
						NormalContext::FileBrowser
					} else {
						NormalContext::Ready
					};
					binding = resolve_normal(base, code, modifiers);
					if binding.is_none() {
						self.state = InputState::normal();
						return false;
					}
				}
				Unmatched::SwallowClear => {
					self.state = InputState::normal();
					return false;
				}
				Unmatched::SwallowKeep => return false,
			}
		}
		let Some(binding) = binding else {
			return false;
		};
		let outcome = self.execute(self.state, code, binding);
		self.finish_execution(outcome, should_quit)
	}

	fn execute_normal(
		&mut self,
		old: InputState,
		code: KeyCode,
		action: NormalAction,
	) -> DispatchControl {
		let InputState::Normal(pending) = old else {
			return DispatchControl::Continue;
		};
		match action {
			NormalAction::Quit => return DispatchControl::Quit,
			NormalAction::Motion(motion) => {
				self.send_move(match motion {
					NormalMotion::Up => MoveDirection::Up,
					NormalMotion::Down => MoveDirection::Down,
					NormalMotion::Left => MoveDirection::Left,
					NormalMotion::Right => MoveDirection::Right,
					NormalMotion::NextWord => MoveDirection::NextWord,
					NormalMotion::PrevWord => MoveDirection::PrevWord,
					NormalMotion::NextWordEnd => MoveDirection::NextWordEnd,
					NormalMotion::Top => MoveDirection::Top,
					NormalMotion::Bottom => MoveDirection::Bottom,
				});
				self.state = InputState::normal();
			}
			NormalAction::Scroll(scroll) => {
				let amount = match scroll {
					ScrollOp::HalfUp | ScrollOp::HalfDown => {
						(self.context.viewport_line_count / 2) as i32
					}
					ScrollOp::PageUp | ScrollOp::PageDown => {
						self.context.viewport_line_count.saturating_sub(2) as i32
					}
				};
				let delta = match scroll {
					ScrollOp::HalfUp | ScrollOp::PageUp => -amount,
					ScrollOp::HalfDown | ScrollOp::PageDown => amount,
				};
				self.send_command(EditorCommand::Scroll(delta));
				self.state = InputState::normal();
			}
			NormalAction::ViewportLine {
				pos,
				first_non_blank,
			} => {
				{
					let line = match pos {
						ViewportPos::Top => self.context.viewport_start_line,
						ViewportPos::Middle => {
							self.context.viewport_start_line
								+ DocLineCount::new(u64::from(self.context.viewport_line_count / 2))
						}
						ViewportPos::Bottom => {
							self.context.viewport_start_line
								+ DocLineCount::new(u64::from(
									self.context.viewport_line_count.saturating_sub(1),
								))
						}
					};
					self.send_command(EditorCommand::GotoLine(line));
					if first_non_blank {
						self.send_command(EditorCommand::FirstNonWhitespace);
					}
				}
				self.state = InputState::normal();
			}
			NormalAction::Edit(edit) => {
				self.send_command(match edit {
					EditOp::LineStart => EditorCommand::LineStart,
					EditOp::FirstNonBlank => EditorCommand::FirstNonWhitespace,
					EditOp::LineEnd => EditorCommand::LineEnd,
					EditOp::SmartHome => EditorCommand::SmartHome,
					EditOp::PageUp => EditorCommand::PageUp,
					EditOp::PageDown => EditorCommand::PageDown,
					EditOp::MatchDelimiter => EditorCommand::MatchDelimiter,
					EditOp::DeleteToLineEnd => EditorCommand::DeleteToLineEnd,
					EditOp::JoinLine => EditorCommand::JoinLine,
					EditOp::Undo => EditorCommand::Undo,
					EditOp::Redo => EditorCommand::Redo,
					EditOp::DeleteChar => EditorCommand::Delete,
					EditOp::BackspaceChar => EditorCommand::Backspace,
					EditOp::ToggleCase => EditorCommand::ToggleCaseAtCursor,
					EditOp::OpenBelow => EditorCommand::OpenLineBelow,
					EditOp::OpenAbove => EditorCommand::OpenLineAbove,
					EditOp::AppendAtLineEnd => EditorCommand::AppendAtLineEnd,
				});
				self.state = InputState::normal();
			}
			NormalAction::Search(search) => {
				self.send_command(match search {
					SearchOp::Next => EditorCommand::SearchNext,
					SearchOp::Previous => EditorCommand::SearchPrev,
					SearchOp::WordForward => EditorCommand::SearchWordUnderCursor { forward: true },
					SearchOp::WordBackward => {
						EditorCommand::SearchWordUnderCursor { forward: false }
					}
				});
				self.state = InputState::normal();
			}
			NormalAction::Fold(fold) => {
				self.send_command(match fold {
					super::keymap::FoldOp::Toggle => EditorCommand::ToggleFold,
					super::keymap::FoldOp::Close => EditorCommand::CloseFold,
					super::keymap::FoldOp::Open => EditorCommand::OpenFold,
					super::keymap::FoldOp::CloseAll => EditorCommand::CloseAllFolds,
					super::keymap::FoldOp::OpenAll => EditorCommand::OpenAllFolds,
				});
				self.state = InputState::normal();
			}
			NormalAction::Pane(pane) => {
				self.send_command(match pane {
					PaneOp::Focus(direction) => EditorCommand::FocusPaneDirection(direction),
					PaneOp::Split(direction) => EditorCommand::SplitPane(direction),
					PaneOp::Close => EditorCommand::ClosePane,
					PaneOp::Resize(split, resize) => EditorCommand::ResizePane(split, resize),
				});
				self.state = if matches!(pane, PaneOp::Resize(..)) {
					InputState::Normal(NormalPending::PaneResize)
				} else {
					InputState::normal()
				};
			}
			NormalAction::Tab(tab) => {
				self.send_command(match tab {
					TabOp::Next => EditorCommand::DocTabNext,
					TabOp::Previous => EditorCommand::DocTabPrev,
				});
				self.state = InputState::normal();
			}
			NormalAction::Browser(browser) => {
				self.send_command(match browser {
					BrowserOp::CollapseOrParent => EditorCommand::CollapseBrowserEntryOrParent,
					BrowserOp::Activate => EditorCommand::ActivateBrowserEntry,
					BrowserOp::Refresh => EditorCommand::RefreshFileBrowser,
				});
				self.state = InputState::normal();
			}
			NormalAction::OperatorApply(target) => {
				let (op, register) = match pending {
					NormalPending::Operator { op, register }
					| NormalPending::OperatorInner { op, register }
					| NormalPending::OperatorAround { op, register } => (op, register),
					_ => return DispatchControl::Continue,
				};
				self.send_command(match (op, target) {
					(Operator::Delete, OperatorTarget::Line) => {
						EditorCommand::DeleteLine { register }
					}
					(Operator::Delete, OperatorTarget::Word(motion)) => {
						EditorCommand::DeleteWordMotion { register, motion }
					}
					(Operator::Change, OperatorTarget::Word(motion)) => {
						EditorCommand::ChangeWordMotion { register, motion }
					}
					(Operator::Delete, OperatorTarget::InnerWord) => EditorCommand::DeleteInnerWord,
					(Operator::Change, OperatorTarget::InnerWord) => EditorCommand::ChangeInnerWord,
					(Operator::Delete, OperatorTarget::AroundFunction) => {
						EditorCommand::DeleteAroundFunction { register }
					}
					(Operator::Change, OperatorTarget::AroundFunction) => {
						EditorCommand::ChangeAroundFunction
					}
					(Operator::Change, OperatorTarget::Line) => {
						return DispatchControl::Continue;
					}
				});
				self.state = InputState::normal();
			}
			NormalAction::YankApply(target) => {
				let register = pending.captured_register().unwrap_or('"');
				self.send_command(match target {
					YankTarget::Line => EditorCommand::YankLine { register },
					YankTarget::Word(motion) => EditorCommand::YankWordMotion { register, motion },
				});
				self.state = InputState::normal();
			}
			NormalAction::Put => {
				self.send_command(EditorCommand::Put {
					register: pending.captured_register().unwrap_or('"'),
				});
				self.state = InputState::normal();
			}
			NormalAction::ArmOperator(_)
			| NormalAction::ArmYank
			| NormalAction::ArmGoto
			| NormalAction::ArmFold
			| NormalAction::ArmBracket
			| NormalAction::ArmPane
			| NormalAction::ArmPaneResize
			| NormalAction::ArmReplace
			| NormalAction::ArmRegisterCapture
			| NormalAction::ArmFind(_)
			| NormalAction::ArmOperatorObject(_) => {
				unreachable!("arm transitions are executed through arm_target")
			}
			NormalAction::CaptureRegister => {
				let NormalPending::RegisterCapture { continuation } = pending else {
					return DispatchControl::Continue;
				};
				let KeyCode::Char(register) = code else {
					return DispatchControl::Continue;
				};
				self.state = InputState::Normal(match continuation {
					RegisterContinuation::Ready => NormalPending::Ready {
						register: Some(register),
					},
					RegisterContinuation::Yank => NormalPending::Yank {
						register: Some(register),
					},
					RegisterContinuation::Goto => NormalPending::Goto {
						register: Some(register),
					},
				});
			}
			NormalAction::CaptureFindChar => {
				let NormalPending::FindChar(search) = pending else {
					return DispatchControl::Continue;
				};
				let KeyCode::Char(ch) = code else {
					return DispatchControl::Continue;
				};
				let (forward, inclusive) = search.parts();
				self.state = InputState::normal();
				self.dispatch_find_char(ch, forward, inclusive);
			}
			NormalAction::CaptureReplaceChar => {
				if let KeyCode::Char(ch) = code {
					self.send_command(EditorCommand::ReplaceChar(ch));
				}
				self.state = InputState::normal();
			}
			NormalAction::RepeatFind => {
				if let Some(last) = self.last_char_search {
					self.dispatch_find_char(last.ch, last.forward, last.inclusive);
				}
				self.state = InputState::normal();
			}
			NormalAction::RepeatFindReversed => {
				if let Some(last) = self.last_char_search {
					self.send_move(MoveDirection::FindChar {
						ch: last.ch,
						forward: !last.forward,
						inclusive: last.inclusive,
					});
				}
				self.state = InputState::normal();
			}
			NormalAction::EnterInsert(entry) => {
				match entry {
					InsertEntry::AtCursor => {}
					InsertEntry::FirstNonBlank => {
						self.send_command(EditorCommand::FirstNonWhitespace)
					}
					InsertEntry::AfterCursor => self.send_move(MoveDirection::Right),
				}
				self.state = InputState::Insert;
				self.apply_cursor_style();
			}
			NormalAction::EnterVisual(kind) => {
				if !self.context.cursor_available {
					self.state = old;
					return DispatchControl::Continue;
				}
				let anchor = if matches!(kind, VisualKind::Line) {
					self.context.cursor_line_start_byte
				} else {
					self.context.cursor_abs_byte
				};
				self.enter_visual_mode(anchor, kind);
			}
			NormalAction::EnterCommandPrompt => {
				self.command_buffer.clear();
				self.command_cursor = 0;
				self.state = InputState::Command;
			}
			NormalAction::EnterSearchPrompt => {
				self.search_buffer.clear();
				self.search_cursor = 0;
				self.state = InputState::Search;
			}
			NormalAction::Esc => {
				let active_search = self.context.active_search;
				let has_multiple_cursors = self.context.cursor_count > 1;
				self.state = InputState::normal();
				self.send_command(EditorCommand::ClearVisualSelection);
				if active_search {
					self.send_command(EditorCommand::ClearSearch);
				}
				if has_multiple_cursors {
					self.send_command(EditorCommand::CollapseCursors);
				}
			}
			NormalAction::ReadOnlyStatus => {
				self.status_message = Some("File browser is read-only".to_string());
				self.state = InputState::normal();
			}
			NormalAction::ClearPending => self.state = InputState::normal(),
			NormalAction::GotoDefinition => {
				self.send_command(EditorCommand::GotoDefinition);
				self.state = InputState::normal();
			}
			NormalAction::JumpBack => {
				self.send_command(EditorCommand::JumpBack);
				self.state = InputState::normal();
			}
			NormalAction::JumpForward => {
				self.send_command(EditorCommand::JumpForward);
				self.state = InputState::normal();
			}
			NormalAction::NextFunction => {
				self.send_command(EditorCommand::NextFunction);
				self.state = InputState::normal();
			}
			NormalAction::AddCursor(direction) => {
				self.send_command(EditorCommand::AddCursorAdjacent(direction));
				self.state = InputState::normal();
			}
		}
		DispatchControl::Continue
	}

	pub(super) fn dispatch_visual_key(
		&mut self,
		code: KeyCode,
		modifiers: Modifiers,
		should_quit: &mut bool,
	) -> bool {
		let Some(context) = visual_context(self.state) else {
			return false;
		};
		let Some(binding) = resolve_visual(context, code, modifiers) else {
			match on_unmatched(Context::Visual(context)) {
				Unmatched::SwallowClear => self.set_visual_pending(VisualPending::Ready),
				Unmatched::SwallowKeep => {}
				Unmatched::FallThrough => unreachable!(),
			}
			return false;
		};
		let outcome = self.execute(self.state, code, binding);
		self.finish_execution(outcome, should_quit)
	}

	fn execute_visual(
		&mut self,
		old: InputState,
		context: VisualContext,
		code: KeyCode,
		action: VisualAction,
	) -> DispatchControl {
		let InputState::Visual { anchor, kind, .. } = old else {
			return DispatchControl::Continue;
		};
		match action {
			VisualAction::Quit => return DispatchControl::Quit,
			VisualAction::Motion(motion) => match motion {
				VisualMotion::Up => self.send_move(MoveDirection::Up),
				VisualMotion::Down => self.send_move(MoveDirection::Down),
				VisualMotion::Left => self.send_move(MoveDirection::Left),
				VisualMotion::Right => self.send_move(MoveDirection::Right),
				VisualMotion::NextWord => self.send_move(MoveDirection::NextWord),
				VisualMotion::PrevWord => self.send_move(MoveDirection::PrevWord),
				VisualMotion::NextWordEnd => self.send_move(MoveDirection::NextWordEnd),
				VisualMotion::LineStart => self.send_command(EditorCommand::LineStart),
				VisualMotion::FirstNonBlank => self.send_command(EditorCommand::FirstNonWhitespace),
				VisualMotion::LineEnd => self.send_command(EditorCommand::LineEnd),
				VisualMotion::SmartHome => self.send_command(EditorCommand::SmartHome),
				VisualMotion::PageUp => self.send_command(EditorCommand::PageUp),
				VisualMotion::PageDown => self.send_command(EditorCommand::PageDown),
				VisualMotion::Bottom => self.send_move(MoveDirection::Bottom),
			},
			VisualAction::Scroll(scroll) => {
				let amount = match scroll {
					ScrollOp::HalfUp | ScrollOp::HalfDown => {
						(self.context.viewport_line_count / 2) as i32
					}
					ScrollOp::PageUp | ScrollOp::PageDown => {
						self.context.viewport_line_count.saturating_sub(2) as i32
					}
				};
				let delta = match scroll {
					ScrollOp::HalfUp | ScrollOp::PageUp => -amount,
					ScrollOp::HalfDown | ScrollOp::PageDown => amount,
				};
				self.send_command(EditorCommand::Scroll(delta));
			}
			VisualAction::Apply(operation) => {
				self.clear_prefixes();
				self.send_command(match operation {
					VisualOp::Yank => EditorCommand::VisualYank { anchor, kind },
					VisualOp::Delete => EditorCommand::VisualDelete { anchor, kind },
					VisualOp::Change => EditorCommand::VisualChange { anchor, kind },
				});
				self.state = InputState::normal();
				self.apply_cursor_style();
			}
			VisualAction::Esc => {
				self.state = InputState::normal();
				self.send_command(EditorCommand::ClearVisualSelection);
				self.apply_cursor_style();
			}
			VisualAction::CommandPrompt => {
				let cursor = if self.context.cursor_available {
					self.context.cursor_abs_byte
				} else {
					anchor
				};
				self.visual_command_range = Some((anchor, cursor));
				self.command_buffer = "'<,'>".to_string();
				self.command_cursor = self.command_buffer.len();
				self.state = InputState::Command;
				self.apply_cursor_style();
			}
			VisualAction::ArmInner
			| VisualAction::ArmAround
			| VisualAction::ArmGoto
			| VisualAction::ArmFind(_) => {
				unreachable!("arm transitions are executed through arm_target")
			}
			VisualAction::SelectInnerWord => {
				self.send_command(EditorCommand::SelectInnerWord);
				self.set_visual_pending(VisualPending::Ready);
			}
			VisualAction::SelectAroundFunction => {
				self.send_command(EditorCommand::SelectAroundFunction);
				self.set_visual_pending(VisualPending::Ready);
			}
			VisualAction::GotoTop => {
				self.send_move(MoveDirection::Top);
				self.set_visual_pending(VisualPending::Ready);
			}
			VisualAction::CaptureFindChar => {
				let VisualContext::FindChar(search) = context else {
					return DispatchControl::Continue;
				};
				if let KeyCode::Char(ch) = code {
					let (forward, inclusive) = search.parts();
					self.set_visual_pending(VisualPending::Ready);
					self.dispatch_find_char(ch, forward, inclusive);
				}
			}
			VisualAction::RepeatFind => {
				if let Some(last) = self.last_char_search {
					self.dispatch_find_char(last.ch, last.forward, last.inclusive);
				}
			}
			VisualAction::RepeatFindReversed => {
				if let Some(last) = self.last_char_search {
					self.send_move(MoveDirection::FindChar {
						ch: last.ch,
						forward: !last.forward,
						inclusive: last.inclusive,
					});
				}
			}
		}
		DispatchControl::Continue
	}

	fn set_visual_pending(&mut self, pending: VisualPending) {
		if let InputState::Visual { anchor, kind, .. } = self.state {
			self.state = InputState::Visual {
				anchor,
				kind,
				pending,
			};
		}
	}

	pub(super) fn dispatch_insert_key(
		&mut self,
		code: KeyCode,
		modifiers: Modifiers,
		should_quit: &mut bool,
	) -> bool {
		let Some(binding) = resolve_insert(code, modifiers) else {
			return false;
		};
		let outcome = self.execute(self.state, code, binding);
		self.finish_execution(outcome, should_quit)
	}

	fn execute_insert(&mut self, code: KeyCode, action: InsertAction) -> DispatchControl {
		match action {
			InsertAction::Quit => return DispatchControl::Quit,
			InsertAction::Leave => {
				self.state = InputState::normal();
				self.apply_cursor_style();
			}
			InsertAction::InsertChar => {
				if let KeyCode::Char(ch) = code {
					self.send_command(EditorCommand::InsertChar(ch));
				}
			}
			InsertAction::Newline => self.send_command(EditorCommand::InsertChar('\n')),
			InsertAction::Tab => self.send_command(EditorCommand::InsertChar('\t')),
			InsertAction::Backspace => self.send_command(EditorCommand::Backspace),
			InsertAction::Delete => self.send_command(EditorCommand::Delete),
			InsertAction::Motion(motion) => self.send_move(match motion {
				MotionOp::Up => MoveDirection::Up,
				MotionOp::Down => MoveDirection::Down,
				MotionOp::Left => MoveDirection::Left,
				MotionOp::Right => MoveDirection::Right,
			}),
			InsertAction::SmartHome => self.send_command(EditorCommand::SmartHome),
			InsertAction::LineEnd => self.send_command(EditorCommand::LineEnd),
			InsertAction::PageUp => self.send_command(EditorCommand::PageUp),
			InsertAction::PageDown => self.send_command(EditorCommand::PageDown),
			InsertAction::AddCursor(direction) => {
				self.send_command(EditorCommand::AddCursorAdjacent(direction));
			}
		}
		DispatchControl::Continue
	}

	pub(super) fn dispatch_confirm_key(
		&mut self,
		code: KeyCode,
		modifiers: Modifiers,
		should_quit: &mut bool,
	) -> bool {
		let Some(binding) = resolve_confirm(code, modifiers) else {
			return false;
		};
		let outcome = self.execute(self.state, code, binding);
		self.finish_execution(outcome, should_quit)
	}

	fn execute_confirm(&mut self, action: ConfirmKeyAction) -> DispatchControl {
		match action {
			ConfirmKeyAction::Quit => DispatchControl::Quit,
			ConfirmKeyAction::Answer(answer) => {
				self.send_command(EditorCommand::ConfirmResponse(answer));
				DispatchControl::Continue
			}
		}
	}
}
