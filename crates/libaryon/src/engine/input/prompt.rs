use super::command_registry::{ArgCompletion, CommandCtx, DispatchResult};
use super::history::append_history_entry;
use super::keymap::{PromptKind, PromptOutcome};
use super::machine::InputRuntime;
use super::option_registry::{VALUE_OPTIONS, ValueCompletion};
use super::runtime_catalog;
use crate::core::RegistryRev;
use crate::engine::canonical_theme_names;
use crate::engine::request::{InternalEvent, InternalSender};
use crate::engine::runtime_registry::{RuntimeCompletion, RuntimeRegistries};
use crate::engine::{EditorCommand, EditorMode};
use std::sync::{Arc, Condvar, Mutex};
use std::thread;
use std::time::Duration;

impl InputRuntime {
	pub(super) fn handle_paste(&mut self, data: String) {
		if data.is_empty() {
			return;
		}
		if self.context.is_picker {
			let pasted = sanitized_prompt_paste(&data);
			if !pasted.is_empty() {
				self.send_command(EditorCommand::Picker(crate::engine::PickerAction::Insert(
					pasted,
				)));
				self.needs_render = true;
			}
			return;
		}
		match self.editor_mode() {
			EditorMode::Insert => {
				self.send_command(EditorCommand::InsertText(data));
				self.needs_render = true;
			}
			EditorMode::Command => {
				self.invalidate_command_completion();
				let pasted = sanitized_prompt_paste(&data);
				insert_at_prompt_cursor(
					&mut self.command_buffer,
					&mut self.command_cursor,
					&pasted,
				);
				self.needs_render = true;
			}
			EditorMode::Search => {
				self.invalidate_command_completion();
				let pasted = sanitized_prompt_paste(&data);
				insert_at_prompt_cursor(&mut self.search_buffer, &mut self.search_cursor, &pasted);
				self.needs_render = true;
			}
			_ => {}
		}
	}

	pub(crate) fn record_startup_file(&mut self, path: &str) {
		let entry = format!("e {path}");
		if push_history_entry(
			&entry,
			&mut self.command_history,
			&mut self.command_history_index,
		) {
			let _ = self.history_store.append_command_entry(&entry);
		}
	}

	pub(super) fn prompt_enter(
		&mut self,
		kind: PromptKind,
		registries: &RuntimeRegistries,
	) -> PromptOutcome {
		match kind {
			PromptKind::Command => {
				let command = self.command_buffer.clone();
				if self.dispatch_command_text(registries, &command) {
					return PromptOutcome::Quit;
				}
				if push_history_entry(
					&self.command_buffer,
					&mut self.command_history,
					&mut self.command_history_index,
				) {
					let _ = self
						.history_store
						.append_command_entry(&self.command_buffer);
				}
				self.visual_command_range = None;
				self.send_command(EditorCommand::ClearVisualSelection);
			}
			PromptKind::Search => {
				if !self.search_buffer.is_empty() {
					self.send_command(EditorCommand::SearchStart(self.search_buffer.clone()));
				}
				if push_history_entry(
					&self.search_buffer,
					&mut self.search_history,
					&mut self.search_history_index,
				) {
					let _ = self.history_store.append_search_entry(&self.search_buffer);
				}
			}
		}
		PromptOutcome::ToNormal
	}

	pub(super) fn dispatch_command_text(
		&mut self,
		registries: &RuntimeRegistries,
		command: &str,
	) -> bool {
		let command = command.strip_prefix(':').unwrap_or(command);
		let visual_range = self.visual_command_range.or(match self.state {
			crate::engine::input::keymap::InputState::Visual { anchor, .. } => {
				Some((anchor, self.context.cursor_abs_byte))
			}
			_ => None,
		});
		let mut commands = Vec::new();
		let mut dynamic_invocations = Vec::new();
		let mut ctx = CommandCtx {
			commands: &mut commands,
			dynamic_invocations: &mut dynamic_invocations,
			runtime_registries: Some(registries),
			status: &mut self.status_message,
			cursor_line: self.context.cursor_line,
			visual_range,
			quit: false,
		};
		let result = runtime_catalog::dispatch_ex_command(&mut ctx, registries, command);
		let quit = ctx.quit;
		self.dynamic_invocations.extend(dynamic_invocations);
		for command in commands {
			self.send_command(command);
		}
		if result == DispatchResult::Unknown {
			self.status_message = Some(match runtime_catalog::suggest(registries, command) {
				Some(suggestion) => {
					format!("Unknown command: {command} (did you mean :{suggestion}?)")
				}
				None => format!("Unknown command: {command}"),
			});
		}
		quit
	}

	pub(super) fn prompt_cancel(&mut self, kind: PromptKind) -> PromptOutcome {
		match kind {
			PromptKind::Command => {
				self.command_history_index = None;
				self.command_cursor = 0;
				self.visual_command_range = None;
				self.send_command(EditorCommand::ClearVisualSelection);
			}
			PromptKind::Search => {
				self.search_history_index = None;
				self.search_cursor = 0;
			}
		}
		PromptOutcome::ToNormal
	}

	pub(super) fn prompt_history(&mut self, kind: PromptKind, up: bool) -> PromptOutcome {
		match (kind, up) {
			(PromptKind::Command, true) => {
				recall_history_up(
					&mut self.command_buffer,
					&self.command_history,
					&mut self.command_history_index,
				);
				self.command_cursor = self.command_buffer.len();
			}
			(PromptKind::Command, false) => {
				recall_history_down(
					&mut self.command_buffer,
					&self.command_history,
					&mut self.command_history_index,
				);
				self.command_cursor = self.command_buffer.len();
			}
			(PromptKind::Search, true) => {
				recall_history_up(
					&mut self.search_buffer,
					&self.search_history,
					&mut self.search_history_index,
				);
				self.search_cursor = self.search_buffer.len();
			}
			(PromptKind::Search, false) => {
				recall_history_down(
					&mut self.search_buffer,
					&self.search_history,
					&mut self.search_history_index,
				);
				self.search_cursor = self.search_buffer.len();
			}
		}
		PromptOutcome::Stay
	}

	pub(super) fn prompt_backspace(&mut self, kind: PromptKind) -> PromptOutcome {
		let empty = match kind {
			PromptKind::Command => self.command_buffer.is_empty(),
			PromptKind::Search => self.search_buffer.is_empty(),
		};
		if empty {
			match kind {
				PromptKind::Command => {
					self.command_history_index = None;
					self.command_cursor = 0;
					self.visual_command_range = None;
				}
				PromptKind::Search => {
					self.search_history_index = None;
					self.search_cursor = 0;
				}
			}
			PromptOutcome::ToNormal
		} else {
			match kind {
				PromptKind::Command => {
					backspace_at_prompt_cursor(&mut self.command_buffer, &mut self.command_cursor)
				}
				PromptKind::Search => {
					backspace_at_prompt_cursor(&mut self.search_buffer, &mut self.search_cursor)
				}
			}
			PromptOutcome::Stay
		}
	}

	pub(super) fn prompt_insert_char(&mut self, kind: PromptKind, ch: char) -> PromptOutcome {
		match kind {
			PromptKind::Command => {
				let mut encoded = [0; 4];
				insert_at_prompt_cursor(
					&mut self.command_buffer,
					&mut self.command_cursor,
					ch.encode_utf8(&mut encoded),
				);
			}
			PromptKind::Search => {
				let mut encoded = [0; 4];
				insert_at_prompt_cursor(
					&mut self.search_buffer,
					&mut self.search_cursor,
					ch.encode_utf8(&mut encoded),
				);
			}
		}
		PromptOutcome::Stay
	}

	pub(super) fn prompt_delete(&mut self, kind: PromptKind) -> PromptOutcome {
		match kind {
			PromptKind::Command => {
				delete_at_prompt_cursor(&mut self.command_buffer, &mut self.command_cursor)
			}
			PromptKind::Search => {
				delete_at_prompt_cursor(&mut self.search_buffer, &mut self.search_cursor)
			}
		}
		PromptOutcome::Stay
	}

	pub(super) fn prompt_move_cursor(
		&mut self,
		kind: PromptKind,
		movement: PromptCursorMovement,
	) -> PromptOutcome {
		match kind {
			PromptKind::Command => {
				move_prompt_cursor(&self.command_buffer, &mut self.command_cursor, movement)
			}
			PromptKind::Search => {
				move_prompt_cursor(&self.search_buffer, &mut self.search_cursor, movement)
			}
		}
		PromptOutcome::Stay
	}

	pub(super) fn prompt_complete(&mut self, registries: &RuntimeRegistries) -> PromptOutcome {
		self.invalidate_command_completion();
		let before = self.command_buffer.clone();
		// Each completion kind claims the buffer only when it recognises the command
		// it is looking at, so they are tried in turn and the path completion, which
		// accepts anything, goes last.
		let attempts: [CompletionAttempt; 5] = [
			(CompletionKind::Setting, |state| {
				complete_setting(state.buffer, state.registries)
			}),
			(CompletionKind::Theme, |state| {
				complete_theme_command(state.buffer, state.registries)
			}),
			(CompletionKind::SaveDurability, |state| {
				complete_save_durability_command(state.buffer)
			}),
			(CompletionKind::Language, |state| {
				complete_language_command(state.buffer, state.language_candidates, state.registries)
			}),
			(CompletionKind::Command, |state| {
				complete_runtime_static_command(state.buffer, state.registries)
			}),
		];
		let mut state = CommandCompletionState {
			buffer: &mut self.command_buffer,
			language_candidates: &self.language_completion_candidates,
			registries,
		};
		let completion = attempts
			.into_iter()
			.map(|(kind, complete)| (kind, complete(&mut state)))
			.find(|(_, result)| !matches!(result, PathCompletionResult::NotApplicable));
		let Some((kind, result)) = completion else {
			self.path_completion.request(
				self.command_generation,
				registries.revision(),
				self.command_buffer.clone(),
				self.command_cursor,
				runtime_catalog::completion_command_names(registries, ArgCompletion::Path),
			);
			self.status_message = Some("Completing path…".to_string());
			self.completion_pending = true;
			self.needs_render = true;
			return PromptOutcome::Stay;
		};

		self.apply_completion_result(kind, result, before);
		PromptOutcome::Stay
	}

	pub(super) fn invalidate_command_completion(&mut self) {
		self.command_generation = self.command_generation.wrapping_add(1);
		self.completion_pending = false;
		self.path_completion.invalidate();
	}

	pub(crate) fn apply_path_completion(
		&mut self,
		registries: &RuntimeRegistries,
		response: PathCompletionResponse,
	) -> Option<super::machine::InputDispatch> {
		if !response.matches(
			self.path_completion.pending_request_id,
			self.command_generation,
			&self.command_buffer,
			self.command_cursor,
			registries.revision(),
		) {
			return None;
		}
		self.path_completion.pending_request_id = None;
		let before = std::mem::replace(&mut self.command_buffer, response.completed_buffer);
		self.completion_pending = false;
		self.apply_completion_result(CompletionKind::Path, response.result, before);
		Some(super::machine::InputDispatch {
			commands: Vec::new(),
			dynamic_invocations: Vec::new(),
			status_message: self.status_message.take(),
			clear_status: true,
			needs_render: true,
			declined_input: None,
		})
	}

	fn apply_completion_result(
		&mut self,
		kind: CompletionKind,
		result: PathCompletionResult,
		before: String,
	) {
		match result {
			PathCompletionResult::NotApplicable => {
				self.status_message = None;
				self.needs_render = true;
			}
			PathCompletionResult::Completed => {
				self.status_message = None;
				self.needs_render = true;
			}
			PathCompletionResult::NoMatches => {
				self.status_message = Some(format!("No {} matches", kind.label()));
				self.needs_render = true;
			}
			PathCompletionResult::Ambiguous(matches) => {
				self.status_message = Some(format_completion_status(kind, &matches));
				self.needs_render = true;
			}
			PathCompletionResult::Limited(matches) => {
				self.status_message = Some(format_limited_completion_status(kind, &matches));
				self.needs_render = true;
			}
		}
		if self.command_buffer != before {
			self.command_cursor = self.command_buffer.len();
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum PromptCursorMovement {
	Left,
	Right,
	Home,
	End,
}

fn sanitized_prompt_paste(data: &str) -> String {
	data.chars()
		.filter(|ch| !matches!(ch, '\r' | '\n'))
		.collect()
}

fn clamped_prompt_cursor(buffer: &str, cursor: usize) -> usize {
	let mut cursor = cursor.min(buffer.len());
	while !buffer.is_char_boundary(cursor) {
		cursor -= 1;
	}
	cursor
}

fn previous_char_boundary(buffer: &str, cursor: usize) -> Option<usize> {
	buffer[..cursor]
		.char_indices()
		.next_back()
		.map(|(index, _)| index)
}

fn next_char_boundary(buffer: &str, cursor: usize) -> Option<usize> {
	buffer[cursor..]
		.chars()
		.next()
		.map(|ch| cursor + ch.len_utf8())
}

pub(crate) fn insert_at_prompt_cursor(buffer: &mut String, cursor: &mut usize, text: &str) {
	let insertion = clamped_prompt_cursor(buffer, *cursor);
	buffer.insert_str(insertion, text);
	*cursor = insertion + text.len();
}

pub(crate) fn backspace_at_prompt_cursor(buffer: &mut String, cursor: &mut usize) {
	let insertion = clamped_prompt_cursor(buffer, *cursor);
	if let Some(previous) = previous_char_boundary(buffer, insertion) {
		buffer.drain(previous..insertion);
		*cursor = previous;
	} else {
		*cursor = insertion;
	}
}

pub(crate) fn delete_at_prompt_cursor(buffer: &mut String, cursor: &mut usize) {
	let deletion = clamped_prompt_cursor(buffer, *cursor);
	if let Some(next) = next_char_boundary(buffer, deletion) {
		buffer.drain(deletion..next);
	}
	*cursor = deletion;
}

pub(crate) fn move_prompt_cursor(buffer: &str, cursor: &mut usize, movement: PromptCursorMovement) {
	let current = clamped_prompt_cursor(buffer, *cursor);
	*cursor = match movement {
		PromptCursorMovement::Left => previous_char_boundary(buffer, current).unwrap_or(current),
		PromptCursorMovement::Right => next_char_boundary(buffer, current).unwrap_or(current),
		PromptCursorMovement::Home => 0,
		PromptCursorMovement::End => buffer.len(),
	};
}

#[derive(Debug, PartialEq, Eq)]
enum PathCompletionResult {
	NotApplicable,
	Completed,
	NoMatches,
	Ambiguous(Vec<String>),
	Limited(Vec<String>),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum CompletionKind {
	Path,
	Language,
	Setting,
	Theme,
	SaveDurability,
	Command,
}

impl CompletionKind {
	const fn label(self) -> &'static str {
		match self {
			Self::Path => "path",
			Self::Language => "language",
			Self::Setting => "setting",
			Self::Theme => "theme",
			Self::SaveDurability => "save durability",
			Self::Command => "command argument",
		}
	}
}

#[derive(Debug, PartialEq, Eq)]
struct PathCompletionCandidate {
	completion: String,
	display: String,
	is_dir: bool,
}

#[derive(Debug, PartialEq, Eq)]
struct PathCompletionScan {
	candidates: Vec<PathCompletionCandidate>,
	truncated: bool,
}

#[derive(Debug)]
struct PathCompletionRequest {
	id: u64,
	generation: u64,
	registry_revision: RegistryRev,
	buffer: String,
	cursor: usize,
	path_commands: Vec<String>,
}

#[derive(Debug)]
pub(crate) struct PathCompletionResponse {
	id: u64,
	generation: u64,
	registry_revision: RegistryRev,
	original_buffer: String,
	cursor: usize,
	completed_buffer: String,
	result: PathCompletionResult,
}

impl PathCompletionResponse {
	fn matches(
		&self,
		pending_id: Option<u64>,
		generation: u64,
		buffer: &str,
		cursor: usize,
		registry_revision: RegistryRev,
	) -> bool {
		pending_id == Some(self.id)
			&& self.generation == generation
			&& self.original_buffer == buffer
			&& self.cursor == cursor
			&& self.registry_revision == registry_revision
	}
}

#[derive(Debug, Default)]
struct PathCompletionQueueState {
	pending: Option<PathCompletionRequest>,
	stopped: bool,
}

#[derive(Debug, Default)]
struct PathCompletionQueue {
	state: Mutex<PathCompletionQueueState>,
	ready: Condvar,
}

#[derive(Debug)]
struct PathCompletionRuntime {
	queue: Arc<PathCompletionQueue>,
	worker: Option<thread::JoinHandle<()>>,
}

#[derive(Debug, Default)]
pub(super) struct PathCompletionController {
	runtime: Option<PathCompletionRuntime>,
	internal: Option<InternalSender>,
	next_request_id: u64,
	pending_request_id: Option<u64>,
}

const PATH_COMPLETION_ENTRY_LIMIT: usize = 4_096;
const PATH_COMPLETION_BYTE_LIMIT: usize = 1024 * 1024;

impl PathCompletionController {
	pub(super) fn attach_internal_sender(&mut self, sender: InternalSender) {
		self.internal = Some(sender);
	}

	fn request(
		&mut self,
		generation: u64,
		registry_revision: RegistryRev,
		buffer: String,
		cursor: usize,
		path_commands: Vec<String>,
	) {
		let Some(internal) = self.internal.clone() else {
			return;
		};
		let runtime = self
			.runtime
			.get_or_insert_with(|| PathCompletionRuntime::spawn(internal));
		let id = self.next_request_id;
		self.next_request_id = self.next_request_id.wrapping_add(1);
		self.pending_request_id = Some(id);
		let mut state = runtime.queue.state.lock().expect("path completion queue");
		state.pending = Some(PathCompletionRequest {
			id,
			generation,
			registry_revision,
			buffer,
			cursor,
			path_commands,
		});
		runtime.queue.ready.notify_one();
	}

	fn invalidate(&mut self) {
		self.pending_request_id = None;
		if let Some(runtime) = self.runtime.as_ref() {
			let mut state = runtime.queue.state.lock().expect("path completion queue");
			state.pending = None;
		}
	}
}

impl PathCompletionRuntime {
	fn spawn(internal: InternalSender) -> Self {
		let queue = Arc::new(PathCompletionQueue::default());
		let worker_queue = Arc::clone(&queue);
		let worker = thread::Builder::new()
			.name("path-completion".to_string())
			.spawn(move || Self::run(worker_queue, internal))
			.expect("failed to spawn path completion worker");
		Self {
			queue,
			worker: Some(worker),
		}
	}

	fn run(queue: Arc<PathCompletionQueue>, internal: InternalSender) {
		loop {
			let request = {
				let mut state = queue.state.lock().expect("path completion queue");
				while state.pending.is_none() && !state.stopped {
					state = queue.ready.wait(state).expect("path completion queue");
				}
				if state.stopped {
					return;
				}
				state.pending.take().expect("pending request checked")
			};
			let PathCompletionRequest {
				id,
				generation,
				registry_revision,
				buffer: original_buffer,
				cursor,
				path_commands,
			} = request;
			let mut completed_buffer = original_buffer.clone();
			let result = complete_path_command(&mut completed_buffer, &path_commands);
			let mut event = InternalEvent::PathCompletion(PathCompletionResponse {
				id,
				generation,
				registry_revision,
				original_buffer,
				cursor,
				completed_buffer,
				result,
			});
			loop {
				match internal.try_send(event) {
					Ok(()) => break,
					Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return,
					Err(std::sync::mpsc::TrySendError::Full(returned)) => {
						event = returned;
						let state = queue.state.lock().expect("path completion queue");
						if state.stopped {
							return;
						}
						let (state, _) = queue
							.ready
							.wait_timeout(state, Duration::from_millis(10))
							.expect("path completion queue");
						if state.stopped {
							return;
						}
					}
				}
			}
		}
	}
}

impl Drop for PathCompletionRuntime {
	fn drop(&mut self) {
		{
			let mut state = self.queue.state.lock().expect("path completion queue");
			state.stopped = true;
			state.pending = None;
			self.queue.ready.notify_one();
		}
		if let Some(worker) = self.worker.take() {
			let _ = worker.join();
		}
	}
}

const PLAINTEXT_LANGUAGE_COMPLETIONS: &[&str] = &["plain", "plaintext", "txt"];

/// A completion kind and the function that attempts it. Each attempt inspects
/// the buffer and declines with `NotApplicable` when it does not recognise the
/// command, so they can be tried in order.
type CompletionAttempt = (
	CompletionKind,
	fn(&mut CommandCompletionState<'_>) -> PathCompletionResult,
);

/// The buffer plus whatever context a completion kind needs to produce
/// candidates, so they can all be tried through one signature.
struct CommandCompletionState<'a> {
	buffer: &'a mut String,
	language_candidates: &'a [String],
	registries: &'a RuntimeRegistries,
}

fn complete_save_durability_command(buffer: &mut String) -> PathCompletionResult {
	let Some(arg_start) = value_option_arg_start(buffer, ValueCompletion::SaveDurability) else {
		return PathCompletionResult::NotApplicable;
	};
	let prefix = &buffer[arg_start..];
	let matches = ["atomic", "bounded"]
		.into_iter()
		.filter(|candidate| candidate.starts_with(prefix))
		.map(str::to_string)
		.collect::<Vec<_>>();
	if matches.is_empty() {
		return PathCompletionResult::NoMatches;
	}
	complete_simple_word(buffer, arg_start, prefix.len(), matches)
}

fn complete_setting(buffer: &mut String, registries: &RuntimeRegistries) -> PathCompletionResult {
	match complete_setting_key(buffer, registries) {
		PathCompletionResult::NotApplicable => complete_runtime_option_value(buffer, registries),
		result => result,
	}
}

fn complete_setting_key(
	buffer: &mut String,
	registries: &RuntimeRegistries,
) -> PathCompletionResult {
	let Some(rest) = buffer.strip_prefix("set") else {
		return PathCompletionResult::NotApplicable;
	};
	if !rest.chars().next().is_some_and(char::is_whitespace) {
		return PathCompletionResult::NotApplicable;
	}
	let Some(relative_start) = rest
		.char_indices()
		.find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index))
	else {
		return PathCompletionResult::NotApplicable;
	};
	let key_start = "set".len() + relative_start;
	let prefix = &buffer[key_start..];
	if prefix.chars().any(char::is_whitespace) {
		return PathCompletionResult::NotApplicable;
	}

	let mut matches = super::runtime_options::setting_key_candidates(registries)
		.into_iter()
		.filter(|candidate| candidate.starts_with(prefix))
		.collect::<Vec<_>>();
	matches.sort();
	matches.dedup();
	if matches.is_empty() {
		return PathCompletionResult::NoMatches;
	}
	complete_simple_word(buffer, key_start, prefix.len(), matches)
}

fn complete_runtime_option_value(
	buffer: &mut String,
	registries: &RuntimeRegistries,
) -> PathCompletionResult {
	let Some(rest) = buffer.strip_prefix("set") else {
		return PathCompletionResult::NotApplicable;
	};
	if !rest.chars().next().is_some_and(char::is_whitespace) {
		return PathCompletionResult::NotApplicable;
	}
	let Some(key_offset) = rest
		.char_indices()
		.find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index))
	else {
		return PathCompletionResult::NotApplicable;
	};
	let key_start = "set".len() + key_offset;
	let Some(relative_end) = buffer[key_start..].find(char::is_whitespace) else {
		return PathCompletionResult::NotApplicable;
	};
	let key_end = key_start + relative_end;
	let key = &buffer[key_start..key_end];
	let Some(candidates) = super::runtime_options::static_value_completion(registries, key) else {
		return PathCompletionResult::NotApplicable;
	};
	let value_rest = &buffer[key_end..];
	let value_start = key_end
		+ value_rest
			.char_indices()
			.find_map(|(index, ch)| (!ch.is_whitespace()).then_some(index))
			.unwrap_or(value_rest.len());
	let prefix = &buffer[value_start..];
	if prefix.chars().any(char::is_whitespace) {
		return PathCompletionResult::NoMatches;
	}
	let matches = candidates
		.iter()
		.filter(|candidate| candidate.starts_with(prefix))
		.cloned()
		.collect::<Vec<_>>();
	if matches.is_empty() {
		return PathCompletionResult::NoMatches;
	}
	complete_simple_word(buffer, value_start, prefix.len(), matches)
}

fn complete_theme_command(
	buffer: &mut String,
	registries: &RuntimeRegistries,
) -> PathCompletionResult {
	let Some(arg_start) = theme_command_arg_start(buffer, registries) else {
		return PathCompletionResult::NotApplicable;
	};
	let prefix = &buffer[arg_start..];
	let normalized = prefix.to_ascii_lowercase();
	let matches = canonical_theme_names()
		.filter(|candidate| candidate.starts_with(&normalized))
		.map(str::to_string)
		.collect::<Vec<_>>();
	if matches.is_empty() {
		return PathCompletionResult::NoMatches;
	}
	complete_simple_word(buffer, arg_start, prefix.len(), matches)
}

fn theme_command_arg_start(buffer: &str, registries: &RuntimeRegistries) -> Option<usize> {
	command_arg_start(
		buffer,
		&runtime_catalog::completion_command_names(registries, ArgCompletion::Theme),
	)
	.or_else(|| value_option_arg_start(buffer, ValueCompletion::Theme))
}

fn value_option_arg_start(buffer: &str, completion: ValueCompletion) -> Option<usize> {
	let command = "set";
	let rest = buffer.strip_prefix(command)?;
	if !rest.chars().next().is_some_and(char::is_whitespace) {
		return None;
	}
	let key_start = command.len()
		+ rest
			.char_indices()
			.find(|(_, ch)| !ch.is_whitespace())
			.map(|(idx, _)| idx)?;
	let key_and_rest = &buffer[key_start..];
	let key_end = key_and_rest.find(char::is_whitespace)?;
	let key = &key_and_rest[..key_end];
	if !VALUE_OPTIONS.iter().any(|option| {
		(option.key == key || option.aliases.contains(&key)) && option.completion == completion
	}) {
		return None;
	}
	let value_rest = &key_and_rest[key_end..];
	let value_start = key_start + key_end;
	Some(
		value_start
			+ value_rest
				.char_indices()
				.find(|(_, ch)| !ch.is_whitespace())
				.map(|(idx, _)| idx)
				.unwrap_or(value_rest.len()),
	)
}

fn complete_language_command(
	buffer: &mut String,
	candidates: &[String],
	registries: &RuntimeRegistries,
) -> PathCompletionResult {
	let Some(arg_start) = language_command_arg_start(buffer, registries) else {
		return PathCompletionResult::NotApplicable;
	};
	let prefix = &buffer[arg_start..];
	let matches = language_completion_candidates(prefix, candidates);
	if matches.is_empty() {
		return PathCompletionResult::NoMatches;
	}
	complete_simple_word(buffer, arg_start, prefix.len(), matches)
}

fn language_command_arg_start(buffer: &str, registries: &RuntimeRegistries) -> Option<usize> {
	command_arg_start(
		buffer,
		&runtime_catalog::completion_command_names(registries, ArgCompletion::Language),
	)
}

fn complete_runtime_static_command(
	buffer: &mut String,
	registries: &RuntimeRegistries,
) -> PathCompletionResult {
	let Some(first_space) = buffer.find(char::is_whitespace) else {
		return PathCompletionResult::NotApplicable;
	};
	let token = buffer[..first_space]
		.strip_suffix('!')
		.unwrap_or(&buffer[..first_space]);
	let Some(command) = registries.command(token) else {
		return PathCompletionResult::NotApplicable;
	};
	let RuntimeCompletion::Static(candidates) = &command.completion else {
		return PathCompletionResult::NotApplicable;
	};
	let rest = &buffer[first_space..];
	let arg_start = first_space
		+ rest
			.char_indices()
			.find(|(_, ch)| !ch.is_whitespace())
			.map(|(index, _)| index)
			.unwrap_or(rest.len());
	let prefix = &buffer[arg_start..];
	if prefix.chars().any(char::is_whitespace) {
		return PathCompletionResult::NoMatches;
	}
	let matches = candidates
		.iter()
		.filter(|candidate| candidate.starts_with(prefix))
		.cloned()
		.collect::<Vec<_>>();
	if matches.is_empty() {
		return PathCompletionResult::NoMatches;
	}
	complete_simple_word(buffer, arg_start, prefix.len(), matches)
}

fn language_completion_candidates(prefix: &str, candidates: &[String]) -> Vec<String> {
	let mut matches = candidates
		.iter()
		.filter(|candidate| candidate.starts_with(prefix))
		.cloned()
		.collect::<Vec<_>>();
	matches.extend(
		PLAINTEXT_LANGUAGE_COMPLETIONS
			.iter()
			.filter(|candidate| candidate.starts_with(prefix))
			.map(|candidate| candidate.to_string()),
	);
	matches.sort();
	matches.dedup();
	matches
}

fn complete_path_command(buffer: &mut String, path_commands: &[String]) -> PathCompletionResult {
	let Some(path_start) = path_command_arg_start(buffer, path_commands) else {
		return PathCompletionResult::NotApplicable;
	};
	let path_arg = &buffer[path_start..];
	let scan = path_completion_candidates(path_arg);
	let mut candidates = scan.candidates;
	if scan.truncated {
		candidates.sort_by(|a, b| {
			b.is_dir.cmp(&a.is_dir).then_with(|| {
				compare_path_completion(
					&a.completion,
					&b.completion,
					path_completion_case_insensitive(),
				)
			})
		});
		return PathCompletionResult::Limited(
			candidates
				.into_iter()
				.map(|candidate| candidate.display)
				.collect(),
		);
	}
	if candidates.is_empty() {
		return PathCompletionResult::NoMatches;
	}
	let case_insensitive = path_completion_case_insensitive();
	candidates.sort_by(|a, b| {
		b.is_dir
			.cmp(&a.is_dir)
			.then_with(|| compare_path_completion(&a.completion, &b.completion, case_insensitive))
	});

	let replacement = if candidates.len() == 1 {
		candidates[0].completion.clone()
	} else {
		common_path_completion_prefix(candidates.iter().map(|candidate| &candidate.completion))
	};

	if replacement.len() > path_arg.len() {
		buffer.truncate(path_start);
		buffer.push_str(&replacement);
		PathCompletionResult::Completed
	} else if candidates.len() == 1 {
		PathCompletionResult::Completed
	} else {
		PathCompletionResult::Ambiguous(
			candidates
				.into_iter()
				.map(|candidate| candidate.display)
				.collect(),
		)
	}
}

fn complete_simple_word(
	buffer: &mut String,
	start: usize,
	prefix_len: usize,
	candidates: Vec<String>,
) -> PathCompletionResult {
	let replacement = if candidates.len() == 1 {
		candidates[0].clone()
	} else {
		common_language_completion_prefix(candidates.iter())
	};
	if replacement.len() > prefix_len {
		buffer.truncate(start);
		buffer.push_str(&replacement);
		PathCompletionResult::Completed
	} else if candidates.len() == 1 {
		PathCompletionResult::Completed
	} else {
		PathCompletionResult::Ambiguous(candidates)
	}
}

fn path_command_arg_start(buffer: &str, path_commands: &[String]) -> Option<usize> {
	command_arg_start(buffer, path_commands)
}

fn command_arg_start(buffer: &str, commands: &[String]) -> Option<usize> {
	let first_space = buffer.find(char::is_whitespace)?;
	let command = &buffer[..first_space];
	if !commands.iter().any(|candidate| candidate == command) {
		return None;
	}
	let rest = &buffer[first_space..];
	Some(
		first_space
			+ rest
				.char_indices()
				.find(|(_, ch)| !ch.is_whitespace())
				.map(|(idx, _)| idx)
				.unwrap_or(rest.len()),
	)
}

fn path_completion_candidates(path_arg: &str) -> PathCompletionScan {
	path_completion_candidates_with_limits(
		path_arg,
		PATH_COMPLETION_ENTRY_LIMIT,
		PATH_COMPLETION_BYTE_LIMIT,
	)
}

fn path_completion_candidates_with_limits(
	path_arg: &str,
	entry_limit: usize,
	byte_limit: usize,
) -> PathCompletionScan {
	let parts = split_path_completion_arg(path_arg);
	let read_dir_path = crate::core::path::expand_path(&parts.dir_arg);
	let Ok(entries) = std::fs::read_dir(read_dir_path) else {
		return PathCompletionScan {
			candidates: Vec::new(),
			truncated: false,
		};
	};

	let include_hidden = parts.file_prefix.starts_with('.');
	let case_insensitive = path_completion_case_insensitive();
	let mut candidates = Vec::new();
	let mut candidate_bytes = 0usize;
	let mut truncated = false;
	for (visited, entry) in entries.enumerate() {
		if visited == entry_limit {
			truncated = true;
			break;
		}
		let Ok(entry) = entry else {
			continue;
		};
		let name = entry.file_name().to_string_lossy().into_owned();
		if !include_hidden && name.starts_with('.') {
			continue;
		}
		if !path_name_starts_with(&name, &parts.file_prefix, case_insensitive) {
			continue;
		}
		let is_dir = entry.file_type().is_ok_and(|file_type| file_type.is_dir());
		let suffix = if is_dir {
			parts.suffix_separator.to_string()
		} else {
			String::new()
		};
		let completion = format!("{}{name}{suffix}", parts.display_prefix);
		let display = format!("{name}{suffix}");
		let bytes = completion.len().saturating_add(display.len());
		if candidate_bytes.saturating_add(bytes) > byte_limit {
			truncated = true;
			break;
		}
		candidate_bytes += bytes;
		candidates.push(PathCompletionCandidate {
			completion,
			display,
			is_dir,
		});
	}
	PathCompletionScan {
		candidates,
		truncated,
	}
}

#[derive(Debug, PartialEq, Eq)]
struct PathCompletionParts {
	dir_arg: String,
	display_prefix: String,
	file_prefix: String,
	suffix_separator: char,
}

fn split_path_completion_arg(path_arg: &str) -> PathCompletionParts {
	split_path_completion_arg_with(path_arg, cfg!(windows), std::path::MAIN_SEPARATOR)
}

fn split_path_completion_arg_with(
	path_arg: &str,
	allow_backslash_separator: bool,
	default_separator: char,
) -> PathCompletionParts {
	let suffix_separator = path_arg
		.char_indices()
		.rev()
		.find(|(_, ch)| is_path_separator(*ch, allow_backslash_separator))
		.map(|(_, ch)| ch)
		.unwrap_or(default_separator);
	let Some((separator_idx, _)) = path_arg
		.char_indices()
		.rev()
		.find(|(_, ch)| is_path_separator(*ch, allow_backslash_separator))
	else {
		return PathCompletionParts {
			dir_arg: ".".to_string(),
			display_prefix: String::new(),
			file_prefix: path_arg.to_string(),
			suffix_separator,
		};
	};
	let display_prefix = path_arg[..=separator_idx].to_string();
	let dir_arg = if separator_idx == 0
		|| (allow_backslash_separator && is_windows_drive_root_separator(path_arg, separator_idx))
	{
		path_arg[..=separator_idx].to_string()
	} else {
		path_arg[..separator_idx].to_string()
	};
	let file_prefix = path_arg[separator_idx + 1..].to_string();
	PathCompletionParts {
		dir_arg,
		display_prefix,
		file_prefix,
		suffix_separator,
	}
}

fn is_path_separator(ch: char, allow_backslash_separator: bool) -> bool {
	ch == '/' || (allow_backslash_separator && ch == '\\')
}

fn is_windows_drive_root_separator(path_arg: &str, separator_idx: usize) -> bool {
	separator_idx == 2
		&& path_arg.as_bytes().get(1) == Some(&b':')
		&& path_arg
			.as_bytes()
			.first()
			.is_some_and(|byte| byte.is_ascii_alphabetic())
}

fn path_completion_case_insensitive() -> bool {
	cfg!(windows) || cfg!(target_os = "macos")
}

fn path_name_starts_with(name: &str, prefix: &str, case_insensitive: bool) -> bool {
	if case_insensitive {
		name.to_lowercase().starts_with(&prefix.to_lowercase())
	} else {
		name.starts_with(prefix)
	}
}

fn compare_path_completion(a: &str, b: &str, case_insensitive: bool) -> std::cmp::Ordering {
	if case_insensitive {
		a.to_lowercase()
			.cmp(&b.to_lowercase())
			.then_with(|| a.cmp(b))
	} else {
		a.cmp(b)
	}
}

fn common_path_completion_prefix<'a>(mut completions: impl Iterator<Item = &'a String>) -> String {
	let Some(first) = completions.next() else {
		return String::new();
	};
	let case_insensitive = path_completion_case_insensitive();
	let mut prefix = first.clone();
	for completion in completions {
		while !path_name_starts_with(completion, &prefix, case_insensitive) {
			if prefix.pop().is_none() {
				return String::new();
			}
		}
	}
	prefix
}

fn common_language_completion_prefix<'a>(
	mut completions: impl Iterator<Item = &'a String>,
) -> String {
	let Some(first) = completions.next() else {
		return String::new();
	};
	let mut prefix = first.clone();
	for completion in completions {
		while !completion.starts_with(&prefix) {
			if prefix.pop().is_none() {
				return String::new();
			}
		}
	}
	prefix
}

fn format_completion_status(kind: CompletionKind, matches: &[String]) -> String {
	let shown = matches.iter().take(6).cloned().collect::<Vec<_>>();
	let suffix = if matches.len() > shown.len() {
		format!(" ... +{}", matches.len() - shown.len())
	} else {
		String::new()
	};
	format!(
		"{} {} matches: {}{}",
		matches.len(),
		kind.label(),
		shown.join(", "),
		suffix
	)
}

fn format_limited_completion_status(kind: CompletionKind, matches: &[String]) -> String {
	if matches.is_empty() {
		return format!(
			"Limited scan; no {} matches in scanned entries",
			kind.label()
		);
	}
	let details = format_completion_status(kind, matches);
	format!("Limited scan; at least {details}")
}

fn push_history_entry(
	buffer: &str,
	history: &mut Vec<String>,
	history_index: &mut Option<usize>,
) -> bool {
	let appended = append_history_entry(buffer, history);
	*history_index = None;
	appended
}

fn recall_history_up(buffer: &mut String, history: &[String], history_index: &mut Option<usize>) {
	if history.is_empty() {
		return;
	}

	let new_index = match *history_index {
		Some(idx) => idx.saturating_sub(1),
		None => history.len() - 1,
	};
	*history_index = Some(new_index);
	*buffer = history[new_index].clone();
}

fn recall_history_down(buffer: &mut String, history: &[String], history_index: &mut Option<usize>) {
	if let Some(idx) = *history_index {
		if idx + 1 < history.len() {
			let new_index = idx + 1;
			*history_index = Some(new_index);
			*buffer = history[new_index].clone();
		} else {
			*history_index = None;
			buffer.clear();
		}
	}
}

#[cfg(test)]
mod tests {
	use super::super::history::LoadedHistory;
	use super::super::keymap::InputState;
	use super::super::machine::{InputContext, InputRuntime};
	use crate::engine::request::{EngineRequest, InternalEvent};
	use crate::engine::runtime_registry::RuntimeRegistries;
	use crate::engine::{CommandRange, EditorCommand, EditorMode, KeyCode, Modifiers};
	use std::ops::{Deref, DerefMut};
	use std::sync::mpsc;

	struct TestFrontend {
		runtime: InputRuntime,
		registries: RuntimeRegistries,
		context: InputContext,
		tx_cmd: mpsc::Sender<EditorCommand>,
		rx_internal: mpsc::Receiver<EngineRequest>,
	}

	impl Deref for TestFrontend {
		type Target = InputRuntime;

		fn deref(&self) -> &Self::Target {
			&self.runtime
		}
	}

	impl DerefMut for TestFrontend {
		fn deref_mut(&mut self) -> &mut Self::Target {
			&mut self.runtime
		}
	}

	impl TestFrontend {
		fn with_history(history: LoadedHistory) -> (Self, mpsc::Receiver<EditorCommand>) {
			let (tx_cmd, rx_cmd) = mpsc::channel();
			let (_commands, _input, internal, rx_internal) = crate::engine::request::channel();
			let mut runtime = InputRuntime::with_history(history);
			runtime.attach_internal_sender(internal);
			(
				Self {
					runtime,
					registries: RuntimeRegistries::new(),
					context: InputContext::default(),
					tx_cmd,
					rx_internal,
				},
				rx_cmd,
			)
		}

		fn set_editor_mode_for_test(&mut self, mode: EditorMode) {
			self.runtime.state = InputState::from_editor_mode(mode);
		}

		fn dispatch_key(
			&mut self,
			code: KeyCode,
			modifiers: Modifiers,
			should_quit: &mut bool,
		) -> bool {
			let dispatch = self.runtime.dispatch_key_with_registries(
				self.context,
				code,
				modifiers,
				&self.registries,
			);
			if dispatch.clear_status {
				self.runtime.status_message = None;
			}
			if dispatch.status_message.is_some() {
				self.runtime.status_message = dispatch.status_message;
			}
			let mut quit = false;
			for command in dispatch.commands {
				quit |= matches!(command, EditorCommand::Quit);
				self.tx_cmd.send(command).expect("test command receiver");
			}
			*should_quit |= quit;
			quit
		}

		fn handle_paste(&mut self, data: String) {
			let dispatch = self.runtime.dispatch_paste(self.context, data);
			for command in dispatch.commands {
				self.tx_cmd.send(command).expect("test command receiver");
			}
		}

		fn poll_path_completion(&mut self) -> bool {
			while let Ok(request) = self.rx_internal.try_recv() {
				let EngineRequest::Internal(InternalEvent::PathCompletion(response)) = request
				else {
					continue;
				};
				let Some(dispatch) = self
					.runtime
					.apply_path_completion(&self.registries, response)
				else {
					continue;
				};
				if dispatch.clear_status {
					self.runtime.status_message = None;
				}
				if dispatch.status_message.is_some() {
					self.runtime.status_message = dispatch.status_message;
				}
				return true;
			}
			false
		}
	}

	fn build_frontend() -> (TestFrontend, mpsc::Receiver<EditorCommand>) {
		TestFrontend::with_history(LoadedHistory::empty())
	}

	fn build_frontend_with_history_dir(
		dir: std::path::PathBuf,
	) -> (TestFrontend, mpsc::Receiver<EditorCommand>) {
		TestFrontend::with_history(LoadedHistory::load_from_state_dir(Some(dir)))
	}

	fn temp_history_dir(name: &str) -> std::path::PathBuf {
		let unique = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("time")
			.as_nanos();
		let path = std::env::temp_dir().join(format!(
			"baryon-command-history-{}-{name}-{unique}",
			std::process::id()
		));
		std::fs::create_dir_all(&path).expect("create temp history dir");
		path
	}

	fn temp_path_completion_dir(name: &str) -> std::path::PathBuf {
		let unique = std::time::SystemTime::now()
			.duration_since(std::time::UNIX_EPOCH)
			.expect("time")
			.as_nanos();
		let path = std::env::temp_dir().join(format!(
			"baryon-path-completion-{}-{name}-{unique}",
			std::process::id()
		));
		std::fs::create_dir_all(&path).expect("create temp completion dir");
		path
	}

	fn path_prefix(path: &std::path::Path, prefix: &str) -> String {
		path.with_file_name(prefix).display().to_string()
	}

	fn path_with_trailing_separator(path: &std::path::Path) -> String {
		format!("{}{}", path.display(), std::path::MAIN_SEPARATOR)
	}

	fn complete_path(frontend: &mut TestFrontend, should_quit: &mut bool) {
		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, should_quit));
		assert_eq!(frontend.status_message.as_deref(), Some("Completing path…"));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
		while !frontend.poll_path_completion() {
			assert!(
				std::time::Instant::now() < deadline,
				"timed out waiting for path completion"
			);
			std::thread::yield_now();
		}
	}

	fn assert_unknown_command(command: &str) {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = command.to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		while let Ok(cmd) = rx_cmd.try_recv() {
			assert!(
				matches!(cmd, EditorCommand::ClearVisualSelection),
				"unexpected command dispatched: {:?}",
				std::mem::discriminant(&cmd)
			);
		}
		assert!(
			frontend
				.status_message
				.as_deref()
				.is_some_and(|s| s.contains("Unknown command"))
		);
	}

	#[test]
	fn command_mode_paste_appends_to_command_buffer() {
		let (mut frontend, _rx_cmd) = build_frontend();
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "e ".to_string();
		frontend.command_cursor = frontend.command_buffer.len();

		frontend.handle_paste(
			"crates/toron/plans/impl/413_one_pass_partition_reservoir_results.md".to_string(),
		);

		assert_eq!(
			frontend.command_buffer,
			"e crates/toron/plans/impl/413_one_pass_partition_reservoir_results.md"
		);
		assert!(frontend.needs_render);
	}

	#[test]
	fn startup_file_is_persisted_as_edit_command_history() {
		let dir = temp_history_dir("startup-file");
		let (mut frontend, _rx_cmd) = build_frontend_with_history_dir(dir.clone());

		frontend.record_startup_file("crates/baryon/src/app.rs");

		assert_eq!(frontend.command_history, vec!["e crates/baryon/src/app.rs"]);
		frontend.history_store.flush().expect("flush history");
		let written =
			std::fs::read_to_string(dir.join("command_history")).expect("command history file");
		assert_eq!(written, "e crates/baryon/src/app.rs\n");

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_cursor_edits_the_start_of_a_recalled_long_path() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_history = vec![
			"w".to_string(),
			"e crates/baryon/src/engine/very/long/path.rs".to_string(),
		];

		assert!(!frontend.dispatch_key(KeyCode::Up, Modifiers::NONE, &mut should_quit,));
		assert_eq!(frontend.command_cursor, frontend.command_buffer.len());
		assert!(!frontend.dispatch_key(KeyCode::Home, Modifiers::NONE, &mut should_quit,));
		for _ in 0..2 {
			assert!(!frontend.dispatch_key(KeyCode::Right, Modifiers::NONE, &mut should_quit,));
		}
		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit,));

		assert_eq!(
			frontend.command_buffer,
			"e xcrates/baryon/src/engine/very/long/path.rs"
		);
		assert_eq!(frontend.command_cursor, 3);
	}

	#[test]
	fn command_cursor_supports_delete_and_utf8_boundaries() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "e café.rs".to_string();
		frontend.command_cursor = frontend.command_buffer.len();

		for _ in 0..3 {
			assert!(!frontend.dispatch_key(KeyCode::Left, Modifiers::NONE, &mut should_quit,));
		}
		assert!(!frontend.dispatch_key(KeyCode::Backspace, Modifiers::NONE, &mut should_quit,));
		assert!(!frontend.dispatch_key(KeyCode::Delete, Modifiers::NONE, &mut should_quit,));

		assert_eq!(frontend.command_buffer, "e cafrs");
		assert_eq!(frontend.command_cursor, "e caf".len());
		assert!(
			frontend
				.command_buffer
				.is_char_boundary(frontend.command_cursor)
		);
	}

	#[test]
	fn search_cursor_supports_history_and_utf8_editing() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Search);
		frontend.search_history = vec!["café.rs".to_string()];

		assert!(!frontend.dispatch_key(KeyCode::Up, Modifiers::NONE, &mut should_quit));
		assert_eq!(frontend.search_cursor, frontend.search_buffer.len());
		for _ in 0..3 {
			assert!(!frontend.dispatch_key(KeyCode::Left, Modifiers::NONE, &mut should_quit));
		}
		assert!(!frontend.dispatch_key(KeyCode::Backspace, Modifiers::NONE, &mut should_quit));
		assert!(!frontend.dispatch_key(KeyCode::Delete, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.search_buffer, "cafrs");
		assert_eq!(frontend.search_cursor, "caf".len());
		assert!(
			frontend
				.search_buffer
				.is_char_boundary(frontend.search_cursor)
		);

		assert!(!frontend.dispatch_key(KeyCode::Char('a'), Modifiers::CONTROL, &mut should_quit,));
		assert!(!frontend.dispatch_key(KeyCode::Char('x'), Modifiers::NONE, &mut should_quit));
		assert_eq!(frontend.search_buffer, "xcafrs");
		assert_eq!(frontend.search_cursor, 1);
		assert!(!frontend.dispatch_key(KeyCode::Char('e'), Modifiers::CONTROL, &mut should_quit,));
		assert_eq!(frontend.search_cursor, frontend.search_buffer.len());
	}

	#[test]
	fn command_enter_persists_command_history() {
		let dir = temp_history_dir("command");
		let (mut frontend, rx_cmd) = build_frontend_with_history_dir(dir.clone());
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "tab".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("tab command"),
			EditorCommand::DocTabNew
		));
		frontend.history_store.flush().expect("flush history");
		let written =
			std::fs::read_to_string(dir.join("command_history")).expect("command history file");
		assert_eq!(written, "tab\n");

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn search_enter_persists_search_history() {
		let dir = temp_history_dir("search");
		let (mut frontend, rx_cmd) = build_frontend_with_history_dir(dir.clone());
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Search);
		frontend.search_buffer = "needle".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("search command"),
			EditorCommand::SearchStart(pattern) if pattern == "needle"
		));
		frontend.history_store.flush().expect("flush history");
		let written =
			std::fs::read_to_string(dir.join("search_history")).expect("search history file");
		assert_eq!(written, "needle\n");

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_mode_paste_drops_line_breaks() {
		let (mut frontend, _rx_cmd) = build_frontend();
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "e ".to_string();
		frontend.command_cursor = frontend.command_buffer.len();

		frontend.handle_paste("plans/reload.md\n".to_string());

		assert_eq!(frontend.command_buffer, "e plans/reload.md");
	}

	#[test]
	fn search_mode_paste_inserts_at_search_cursor() {
		let (mut frontend, _rx_cmd) = build_frontend();
		frontend.set_editor_mode_for_test(EditorMode::Search);
		frontend.search_buffer = "one pass".to_string();
		frontend.search_cursor = "one".len();

		frontend.handle_paste(" bright\n".to_string());

		assert_eq!(frontend.search_buffer, "one bright pass");
		assert_eq!(frontend.search_cursor, "one bright".len());
	}

	#[test]
	fn insert_mode_paste_sends_insert_text_command() {
		let (mut frontend, rx_cmd) = build_frontend();
		frontend.set_editor_mode_for_test(EditorMode::Insert);

		frontend.handle_paste("hello\nworld".to_string());

		assert!(frontend.needs_render);
		assert!(matches!(
			rx_cmd.try_recv(),
			Ok(EditorCommand::InsertText(text)) if text == "hello\nworld"
		));
	}

	#[test]
	fn path_completion_split_treats_backslash_as_literal_on_unix() {
		let parts = super::split_path_completion_arg_with("foo\\bar", false, '/');

		assert_eq!(
			parts,
			super::PathCompletionParts {
				dir_arg: ".".to_string(),
				display_prefix: String::new(),
				file_prefix: "foo\\bar".to_string(),
				suffix_separator: '/',
			}
		);
	}

	#[test]
	fn path_completion_split_handles_windows_drive_root() {
		let parts = super::split_path_completion_arg_with("C:\\Win", true, '\\');

		assert_eq!(
			parts,
			super::PathCompletionParts {
				dir_arg: "C:\\".to_string(),
				display_prefix: "C:\\".to_string(),
				file_prefix: "Win".to_string(),
				suffix_separator: '\\',
			}
		);
	}

	#[test]
	fn path_completion_split_preserves_windows_nested_separator() {
		let parts = super::split_path_completion_arg_with("C:\\Users\\me", true, '\\');

		assert_eq!(
			parts,
			super::PathCompletionParts {
				dir_arg: "C:\\Users".to_string(),
				display_prefix: "C:\\Users\\".to_string(),
				file_prefix: "me".to_string(),
				suffix_separator: '\\',
			}
		);
	}

	#[test]
	fn path_completion_split_preserves_typed_forward_slash_on_windows() {
		let parts = super::split_path_completion_arg_with("C:/Use", true, '\\');

		assert_eq!(
			parts,
			super::PathCompletionParts {
				dir_arg: "C:/".to_string(),
				display_prefix: "C:/".to_string(),
				file_prefix: "Use".to_string(),
				suffix_separator: '/',
			}
		);
	}

	#[test]
	fn path_completion_prefix_match_can_be_case_insensitive() {
		assert!(super::path_name_starts_with("Alpha.md", "al", true));
		assert!(!super::path_name_starts_with("Alpha.md", "al", false));
	}

	#[test]
	fn path_completion_scan_enforces_entry_and_byte_caps() {
		let dir = temp_path_completion_dir("caps");
		std::fs::write(dir.join("alpha.md"), b"").expect("write alpha");
		std::fs::write(dir.join("beta.md"), b"").expect("write beta");
		let path_arg = format!("{}{}", dir.display(), std::path::MAIN_SEPARATOR);

		let entry_limited = super::path_completion_candidates_with_limits(&path_arg, 1, usize::MAX);
		assert!(entry_limited.truncated);
		assert!(entry_limited.candidates.len() <= 1);

		let byte_limited = super::path_completion_candidates_with_limits(&path_arg, usize::MAX, 0);
		assert!(byte_limited.truncated);
		assert!(byte_limited.candidates.is_empty());

		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn path_completion_response_requires_id_generation_buffer_and_cursor_match() {
		let response = super::PathCompletionResponse {
			id: 7,
			generation: 11,
			registry_revision: crate::core::RegistryRev::new(3),
			original_buffer: "e crates/bar".to_string(),
			cursor: 12,
			completed_buffer: "e crates/baryon".to_string(),
			result: super::PathCompletionResult::Completed,
		};
		let revision = crate::core::RegistryRev::new(3);
		assert!(response.matches(Some(7), 11, "e crates/bar", 12, revision));
		assert!(!response.matches(Some(8), 11, "e crates/bar", 12, revision));
		assert!(!response.matches(Some(7), 12, "e crates/bar", 12, revision));
		assert!(!response.matches(Some(7), 11, "e crates/baz", 12, revision));
		assert!(!response.matches(Some(7), 11, "e crates/bar", 11, revision));
		assert!(!response.matches(
			Some(7),
			11,
			"e crates/bar",
			12,
			crate::core::RegistryRev::new(4)
		));
	}

	#[test]
	fn command_tab_completes_edit_path() {
		let dir = temp_path_completion_dir("edit");
		let file = dir.join("413_one_pass_partition_reservoir_results.md");
		std::fs::write(&file, b"").expect("write completion file");
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = format!("e {}", path_prefix(&file, "413_one"));

		complete_path(&mut frontend, &mut should_quit);

		assert_eq!(frontend.command_buffer, format!("e {}", file.display()));
		assert!(frontend.status_message.is_none());
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_tab_completes_directory_with_trailing_slash() {
		let dir = temp_path_completion_dir("dir");
		let child = dir.join("crates");
		std::fs::create_dir_all(&child).expect("create child dir");
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = format!("edit {}", path_prefix(&child, "cra"));

		complete_path(&mut frontend, &mut should_quit);

		assert_eq!(
			frontend.command_buffer,
			format!("edit {}", path_with_trailing_separator(&child))
		);
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_tab_extends_ambiguous_path_to_common_prefix() {
		let dir = temp_path_completion_dir("common");
		std::fs::write(dir.join("alpha.md"), b"").expect("write alpha");
		std::fs::write(dir.join("alphabet.md"), b"").expect("write alphabet");
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = format!("e {}", dir.join("alp").display());

		complete_path(&mut frontend, &mut should_quit);

		assert_eq!(
			frontend.command_buffer,
			format!("e {}", dir.join("alpha").display())
		);
		assert!(frontend.status_message.is_none());
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_tab_reports_ambiguous_path_when_no_common_extension() {
		let dir = temp_path_completion_dir("ambiguous");
		std::fs::write(dir.join("alpha.md"), b"").expect("write alpha");
		std::fs::write(dir.join("alpine.md"), b"").expect("write alpine");
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = format!("e {}", dir.join("al").display());

		complete_path(&mut frontend, &mut should_quit);

		assert_eq!(
			frontend.command_buffer,
			format!("e {}", dir.join("alp").display())
		);
		assert!(frontend.status_message.is_none());
		complete_path(&mut frontend, &mut should_quit);
		assert_eq!(
			frontend.command_buffer,
			format!("e {}", dir.join("alp").display())
		);
		assert!(
			frontend
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("2 path matches"))
		);
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_tab_applies_to_other_path_commands() {
		let dir = temp_path_completion_dir("commands");
		let file = dir.join("target.md");
		std::fs::write(&file, b"").expect("write target");

		for command in ["w", "panediff", "paned", "pd", "tabedit", "tabe", "te"] {
			let (mut frontend, _rx_cmd) = build_frontend();
			let mut should_quit = false;
			frontend.set_editor_mode_for_test(EditorMode::Command);
			frontend.command_buffer = format!("{command} {}", path_prefix(&file, "tar"));

			complete_path(&mut frontend, &mut should_quit);

			assert_eq!(
				frontend.command_buffer,
				format!("{command} {}", file.display())
			);
		}
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_tab_reports_no_path_matches() {
		let dir = temp_path_completion_dir("none");
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = format!("e {}", dir.join("missing").display());

		complete_path(&mut frontend, &mut should_quit);

		assert_eq!(
			frontend.command_buffer,
			format!("e {}", dir.join("missing").display())
		);
		assert_eq!(frontend.status_message.as_deref(), Some("No path matches"));
		let _ = std::fs::remove_dir_all(dir);
	}

	#[test]
	fn command_tab_completes_language_from_registry_candidates() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.language_completion_candidates = vec!["ipynb".to_string(), "json".to_string()];
		frontend.command_buffer = "lang ip".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "lang ipynb");
		assert!(frontend.status_message.is_none());
	}

	#[test]
	fn command_tab_reports_ambiguous_language_matches() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.language_completion_candidates = vec![
			"rn".to_string(),
			"rs".to_string(),
			"rune".to_string(),
			"rust".to_string(),
		];
		frontend.command_buffer = "language r".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "language r");
		assert!(
			frontend
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("4 language matches"))
		);
	}

	#[test]
	fn command_tab_completes_plaintext_language_alias() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "lang pla".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "lang plain");
		assert!(frontend.status_message.is_none());
	}

	#[test]
	fn command_tab_reports_no_language_matches() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.language_completion_candidates = vec!["rust".to_string()];
		frontend.command_buffer = "lang zzz".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "lang zzz");
		assert_eq!(
			frontend.status_message.as_deref(),
			Some("No language matches")
		);
	}

	#[test]
	fn command_tab_completes_theme_from_registry() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "theme gruv".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "theme gruvbox");
		assert!(frontend.status_message.is_none());
	}

	#[test]
	fn command_tab_completes_persistent_theme() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set theme one".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "set theme onedark");
		assert!(frontend.status_message.is_none());
	}

	#[test]
	fn command_tab_completes_registered_ui_color_keys() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set colors.minimap.a".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "set colors.minimap.accent");
		assert!(frontend.status_message.is_none());
	}

	#[test]
	fn command_tab_completes_save_durability() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set savedurability b".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.command_buffer, "set savedurability bounded");
		assert!(frontend.status_message.is_none());
	}

	#[test]
	fn command_tab_reports_ambiguous_and_missing_themes() {
		let (mut frontend, _rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "theme green".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));
		assert_eq!(frontend.command_buffer, "theme green");
		assert!(
			frontend
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("theme matches"))
		);

		frontend.command_buffer = "theme zzz".to_string();
		assert!(!frontend.dispatch_key(KeyCode::Tab, Modifiers::NONE, &mut should_quit));
		assert_eq!(frontend.status_message.as_deref(), Some("No theme matches"));
	}

	#[test]
	fn wrap_command_toggles_wrap() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "wrap".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":wrap should send a command"),
			EditorCommand::ToggleWrap
		));
	}

	#[test]
	fn nowrap_command_forces_wrap_off() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "nowrap".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":nowrap should send a command"),
			EditorCommand::SetWrap(false)
		));
	}

	#[test]
	fn minimap_command_forces_minimap_on() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "minimap".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":minimap should send a command"),
			EditorCommand::SetMinimap(true)
		));
	}

	#[test]
	fn vis_command_forces_whitespace_visible() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "vis".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":vis should send a command"),
			EditorCommand::SetVisibleWhitespace(true)
		));
	}

	#[test]
	fn num_command_forces_line_numbers_visible() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "num".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":num should send a command"),
			EditorCommand::SetLineNumbers(true)
		));
	}

	#[test]
	fn nonum_command_forces_line_numbers_hidden() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "nonum".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":nonum should send a command"),
			EditorCommand::SetLineNumbers(false)
		));
	}

	#[test]
	fn relnum_command_enables_relative_line_numbers() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "relnum".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":relnum should send a command"),
			EditorCommand::SetRelativeLineNumbers(true)
		));
	}

	#[test]
	fn novis_command_forces_whitespace_hidden() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "novis".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":novis should send a command"),
			EditorCommand::SetVisibleWhitespace(false)
		));
	}

	#[test]
	fn noviz_command_alias_hides_whitespace() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "noviz".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":noviz should send a command"),
			EditorCommand::SetVisibleWhitespace(false)
		));
	}

	#[test]
	fn lang_command_sets_language() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "lang py".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":lang py should send a command") {
			EditorCommand::SetLanguage(ref lang) => {
				assert_eq!(lang.as_deref(), Some("py"));
			}
			_ => panic!("Expected SetLanguage"),
		}
	}

	#[test]
	fn lang_command_sets_rune() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "lang rune".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":lang rune should send a command") {
			EditorCommand::SetLanguage(ref lang) => {
				assert_eq!(lang.as_deref(), Some("rune"));
			}
			_ => panic!("Expected SetLanguage"),
		}
	}

	#[test]
	fn lang_command_sets_markdown() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "lang md".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":lang md should send a command") {
			EditorCommand::SetLanguage(ref lang) => {
				assert_eq!(lang.as_deref(), Some("md"));
			}
			_ => panic!("Expected SetLanguage"),
		}
	}

	#[test]
	fn lang_command_sets_json() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "lang json".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":lang json should send a command") {
			EditorCommand::SetLanguage(ref lang) => {
				assert_eq!(lang.as_deref(), Some("json"));
			}
			_ => panic!("Expected SetLanguage"),
		}
	}

	#[test]
	fn lang_command_sets_json_for_ipynb_extension_alias() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "lang ipynb".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":lang ipynb should send a command")
		{
			EditorCommand::SetLanguage(ref lang) => {
				assert_eq!(lang.as_deref(), Some("ipynb"));
			}
			_ => panic!("Expected SetLanguage"),
		}
	}

	#[test]
	fn language_command_sets_language() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "language rs".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":language rs should send a command")
		{
			EditorCommand::SetLanguage(ref lang) => {
				assert_eq!(lang.as_deref(), Some("rs"));
			}
			_ => panic!("Expected SetLanguage"),
		}
	}

	#[test]
	fn lang_command_plaintext_alias_sends_clear_language() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "lang plain".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":lang plain should send a command")
		{
			EditorCommand::SetLanguage(ref lang) => {
				assert_eq!(lang.as_deref(), None);
			}
			_ => panic!("Expected SetLanguage"),
		}
	}

	#[test]
	fn nominimap_command_forces_minimap_off() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "nominimap".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":nominimap should send a command"),
			EditorCommand::SetMinimap(false)
		));
	}

	#[test]
	fn background_command_sends_set_background() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "background #1a2b3c".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":background should send a command")
		{
			EditorCommand::SetBackground(hex) => {
				assert_eq!(hex, "#1a2b3c");
			}
			_ => panic!("Expected SetBackground"),
		}
	}

	#[test]
	fn background_command_accepts_short_hex() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "background #F80".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":background #F80 should send a command")
		{
			EditorCommand::SetBackground(hex) => {
				assert_eq!(hex, "#F80");
			}
			_ => panic!("Expected SetBackground"),
		}
	}

	#[test]
	fn edit_command_reloads_current_file() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "e".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":e should send a command"),
			EditorCommand::ReloadFile { force: false }
		));
	}

	#[test]
	fn edit_bang_command_force_reloads_current_file() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "e!".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":e! should send a command"),
			EditorCommand::ReloadFile { force: true }
		));
	}

	#[test]
	fn edit_long_command_opens_file_path() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "edit src/main.rs".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":edit path should send a command") {
			EditorCommand::LoadFile(path) => {
				assert!(path.ends_with("src/main.rs"), "unexpected path: {}", path);
			}
			_ => panic!("Expected LoadFile"),
		}
	}

	#[test]
	fn tab_command_creates_a_new_doctab() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "tab".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":tab should send a command"),
			EditorCommand::DocTabNew
		));
	}

	#[test]
	fn panehsplit_command_creates_a_horizontal_pane() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "panehsplit".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect(":panehsplit should send a command"),
			EditorCommand::SplitPane(crate::engine::SplitDirection::Horizontal)
		));
	}

	#[test]
	fn panevsplit_command_creates_a_vertical_pane() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "panevsplit".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect(":panevsplit should send a command"),
			EditorCommand::SplitPane(crate::engine::SplitDirection::Vertical)
		));
	}

	#[test]
	fn pane_new_commands_create_independent_empty_panes() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "new".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect(":new should send a command"),
			EditorCommand::NewPane {
				direction: crate::engine::SplitDirection::Horizontal,
				language: None
			}
		));

		let (mut frontend, rx_cmd) = build_frontend();
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "vnew".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect(":vnew should send a command"),
			EditorCommand::NewPane {
				direction: crate::engine::SplitDirection::Vertical,
				language: None
			}
		));
	}

	#[test]
	fn paneclose_command_closes_the_focused_pane() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "paneclose".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":paneclose should send a command"),
			EditorCommand::ClosePane
		));
	}

	#[test]
	fn tree_command_opens_the_file_browser() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "tree".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":tree should send a command"),
			EditorCommand::OpenFileBrowser
		));
	}

	#[test]
	fn mdpreview_command_opens_markdown_preview() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "mdpreview".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":mdpreview should send a command"),
			EditorCommand::MarkdownPreview { serif: false }
		));
	}

	#[test]
	fn mp_command_opens_markdown_preview() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "mp".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":mp should send a command"),
			EditorCommand::MarkdownPreview { serif: false }
		));
	}

	#[test]
	fn mps_command_opens_serif_markdown_preview() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "mps".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":mps should send a command"),
			EditorCommand::MarkdownPreview { serif: true }
		));
	}

	#[test]
	fn pane_command_short_aliases_are_supported() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "ph rune".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect(":ph should send a command"),
			EditorCommand::NewPane {
				direction: crate::engine::SplitDirection::Horizontal,
				language: Some(ref language)
			} if language == "rune"
		));

		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "pv".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect(":pv should send a command"),
			EditorCommand::NewPane {
				direction: crate::engine::SplitDirection::Vertical,
				language: None
			}
		));

		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "pc".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect(":pc should send a command"),
			EditorCommand::ClosePane
		));
	}

	#[test]
	fn panediff_defaults_to_an_empty_target_and_passes_revisions_through() {
		for (buffer, expected) in [("pd", ""), ("pd HEAD~3", "HEAD~3"), ("pd main", "main")] {
			let (mut frontend, rx_cmd) = build_frontend();
			let mut should_quit = false;
			frontend.set_editor_mode_for_test(EditorMode::Command);
			frontend.command_buffer = buffer.to_string();

			assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
			match rx_cmd.try_recv().expect(":pd should send a command") {
				EditorCommand::DiffSplit(target) => assert_eq!(target, expected, "{buffer}"),
				other => panic!("Expected DiffSplit, got {other:?}"),
			}
		}
	}

	#[test]
	fn diffstep_parses_directions_and_signed_counts() {
		for (buffer, expected) in [
			("diffstep older", -1i64),
			("dstep newer", 1),
			("diffstep -3", -3),
			("diffstep +2", 2),
		] {
			let (mut frontend, rx_cmd) = build_frontend();
			let mut should_quit = false;
			frontend.set_editor_mode_for_test(EditorMode::Command);
			frontend.command_buffer = buffer.to_string();

			assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
			match rx_cmd.try_recv().expect(":diffstep should send a command") {
				EditorCommand::DiffStep(delta) => assert_eq!(delta, expected, "{buffer}"),
				other => panic!("Expected DiffStep, got {other:?}"),
			}
		}
		for buffer in ["diffstep 0", "diffstep sideways"] {
			let (mut frontend, rx_cmd) = build_frontend();
			let mut should_quit = false;
			frontend.set_editor_mode_for_test(EditorMode::Command);
			frontend.command_buffer = buffer.to_string();

			assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
			// Command-mode Enter emits housekeeping commands regardless;
			// only a DiffStep dispatch would be wrong here.
			while let Ok(command) = rx_cmd.try_recv() {
				assert!(
					!matches!(command, EditorCommand::DiffStep(_)),
					"{buffer} must not dispatch a step"
				);
			}
			assert!(
				frontend
					.status_message
					.as_deref()
					.is_some_and(|message| message.contains("Usage: :diffstep")),
				"{buffer}"
			);
		}
	}

	#[test]
	fn ambiguous_split_aliases_are_rejected() {
		for command in ["split", "sp"] {
			assert_unknown_command(command);
		}
	}

	#[test]
	fn old_pane_command_names_are_rejected() {
		for command in [
			"hsplit",
			"hs",
			"vsplit",
			"vs",
			"csplit",
			"cs",
			"diffsplit",
			"ds",
		] {
			assert_unknown_command(command);
		}
	}

	#[test]
	fn legacy_close_alias_is_rejected() {
		assert_unknown_command("close");
	}

	#[test]
	fn tabedit_command_opens_a_file_in_a_new_doctab() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "tabedit src/main.rs".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":tabedit should send a command") {
			EditorCommand::DocTabEdit(path) => {
				assert!(path.ends_with("src/main.rs"), "unexpected path: {}", path);
			}
			_ => panic!("Expected DocTabEdit"),
		}
	}

	#[test]
	fn set_wrap_command_persists_wrap_off() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set wrap off".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect(":set wrap off should send a command"),
			EditorCommand::PersistWrap(false)
		));
	}

	#[test]
	fn set_vis_command_persists_whitespace_visible() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set vis".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":set vis should send a command"),
			EditorCommand::PersistVisibleWhitespace(true)
		));
	}

	#[test]
	fn set_num_command_persists_line_numbers_visible() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set num".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":set num should send a command"),
			EditorCommand::PersistLineNumbers(true)
		));
	}

	#[test]
	fn set_nonum_command_persists_line_numbers_hidden() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set nonum".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":set nonum should send a command"),
			EditorCommand::PersistLineNumbers(false)
		));
	}

	#[test]
	fn set_relative_number_alias_persists_relative_line_numbers() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set relnum".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect(":set relnum should send a command"),
			EditorCommand::PersistRelativeLineNumbers(true)
		));
	}

	#[test]
	fn set_novis_command_persists_whitespace_hidden() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set novis".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect(":set novis should send a command"),
			EditorCommand::PersistVisibleWhitespace(false)
		));
	}

	#[test]
	fn set_reload_command_persists_auto_reload_enabled() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set reload".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect(":set reload should send a command"),
			EditorCommand::PersistReload(true)
		));
	}

	#[test]
	fn set_noreload_command_persists_auto_reload_disabled() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set noreload".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect(":set noreload should send a command"),
			EditorCommand::PersistReload(false)
		));
	}

	#[test]
	fn set_theme_command_persists_theme_name() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set theme onedark".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":set theme onedark should send a command")
		{
			EditorCommand::PersistTheme(name) => assert_eq!(name, "onedark"),
			_ => panic!("Expected PersistTheme"),
		}
	}

	#[test]
	fn set_langtheme_command_persists_language_theme() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set langtheme rust glowmax".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":set langtheme rust glowmax should send a command")
		{
			EditorCommand::PersistLangTheme { language, theme } => {
				assert_eq!(language, "rust");
				assert_eq!(theme, "glowmax");
			}
			_ => panic!("Expected PersistLangTheme"),
		}
	}

	#[test]
	fn set_nolangtheme_command_removes_language_theme() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set nolangtheme rust".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":set nolangtheme rust should send a command")
		{
			EditorCommand::PersistNoLangTheme(language) => assert_eq!(language, "rust"),
			_ => panic!("Expected PersistNoLangTheme"),
		}
	}

	#[test]
	fn canonical_language_theme_default_removes_language_theme() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set language.theme rust default".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		assert!(matches!(
			rx_cmd.try_recv().expect("canonical language theme command"),
			EditorCommand::PersistNoLangTheme(language) if language == "rust"
		));
	}

	#[test]
	fn canonical_ui_color_set_and_query_dispatch_typed_commands() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set colors.accent 0f0".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect("set color command"),
			EditorCommand::PersistUiColor {
				key: crate::engine::UiColorKey::Accent,
				value: Some(color),
			} if color == crate::core::Rgb::new(0, 255, 0)
		));
		let _ = rx_cmd.try_recv(); // ClearVisualSelection.

		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "set colors.accent?".to_string();
		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));
		assert!(matches!(
			rx_cmd.try_recv().expect("query color command"),
			EditorCommand::QueryUiColor(crate::engine::UiColorKey::Accent)
		));
	}

	#[test]
	fn sort_command_defaults_to_whole_file() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "sort".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":sort should send a command") {
			EditorCommand::Sort {
				range,
				reverse,
				unique,
				numeric,
				ignore_case,
			} => {
				assert!(matches!(range, crate::engine::CommandRange::WholeFile));
				assert!(!reverse);
				assert!(!unique);
				assert!(!numeric);
				assert!(!ignore_case);
			}
			_ => panic!("Expected Sort"),
		}
	}

	#[test]
	fn sort_command_parses_bang_and_flags() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "1,5sort!uni".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":1,5sort! should send a command") {
			EditorCommand::Sort {
				range,
				reverse,
				unique,
				numeric,
				ignore_case,
			} => {
				assert!(matches!(range, crate::engine::CommandRange::LineRange(..)));
				assert!(reverse);
				assert!(unique);
				assert!(numeric);
				assert!(ignore_case);
			}
			_ => panic!("Expected Sort"),
		}
	}

	#[test]
	fn uniq_command_defaults_to_whole_file() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "uniq".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":uniq should send a command") {
			EditorCommand::Uniq { range, ignore_case } => {
				assert!(matches!(range, crate::engine::CommandRange::WholeFile));
				assert!(!ignore_case);
			}
			_ => panic!("Expected Uniq"),
		}
	}

	#[test]
	fn command_enter_dispatches_clear_visual_selection() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "wrap".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		let mut saw_clear = false;
		while let Ok(cmd) = rx_cmd.try_recv() {
			if matches!(cmd, EditorCommand::ClearVisualSelection) {
				saw_clear = true;
			}
		}
		assert!(
			saw_clear,
			"Enter in Command mode must dispatch ClearVisualSelection"
		);
	}

	#[test]
	fn command_escape_dispatches_clear_visual_selection() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "sor".to_string();
		frontend.visual_command_range =
			Some((crate::core::DocByte::new(1), crate::core::DocByte::new(5)));

		assert!(!frontend.dispatch_key(KeyCode::Esc, Modifiers::NONE, &mut should_quit));

		assert_eq!(frontend.editor_mode(), EditorMode::Normal);
		assert!(frontend.visual_command_range.is_none());
		assert!(matches!(
			rx_cmd
				.try_recv()
				.expect("Esc should dispatch ClearVisualSelection"),
			EditorCommand::ClearVisualSelection
		));
	}

	#[test]
	fn sort_command_with_visual_range_marker_uses_visual_bytes() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "'<,'>sort".to_string();
		frontend.visual_command_range =
			Some((crate::core::DocByte::new(5), crate::core::DocByte::new(42)));

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":'<,'>sort should send a Sort command")
		{
			EditorCommand::Sort { range, .. } => match range {
				crate::engine::CommandRange::Visual {
					start_byte,
					end_byte,
				} => {
					assert_eq!(start_byte, crate::core::DocByte::new(5));
					assert_eq!(end_byte, crate::core::DocByte::new(42));
				}
				_ => panic!("Expected CommandRange::Visual, got {:?}", range),
			},
			_ => panic!("Expected Sort"),
		}
		assert!(
			frontend.visual_command_range.is_none(),
			"visual snapshot must be cleared after command dispatches"
		);
	}

	#[test]
	fn visual_sort_without_snapshot_is_rejected() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "'<,'>sort".to_string();
		frontend.visual_command_range = None;

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		// With no snapshot, the range parser returns None and the command is
		// reported as unknown rather than silently falling back. Enter always
		// emits a ClearVisualSelection afterwards — no Sort command should
		// leak onto the channel.
		while let Ok(cmd) = rx_cmd.try_recv() {
			assert!(
				matches!(cmd, EditorCommand::ClearVisualSelection),
				"unexpected command dispatched: {:?}",
				std::mem::discriminant(&cmd)
			);
		}
		assert!(
			frontend
				.status_message
				.as_deref()
				.is_some_and(|s| s.contains("Unknown command"))
		);
	}

	#[test]
	fn uniq_command_parses_ignore_case_flag() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "%uniqi".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd.try_recv().expect(":%uniqi should send a command") {
			EditorCommand::Uniq { range, ignore_case } => {
				assert!(matches!(range, crate::engine::CommandRange::WholeFile));
				assert!(ignore_case);
			}
			_ => panic!("Expected Uniq"),
		}
	}

	#[test]
	fn substitute_command_unescapes_tab_replacement() {
		let (mut frontend, rx_cmd) = build_frontend();
		let mut should_quit = false;
		frontend.set_editor_mode_for_test(EditorMode::Command);
		frontend.command_buffer = "%s/    /\\t/g".to_string();

		assert!(!frontend.dispatch_key(KeyCode::Enter, Modifiers::NONE, &mut should_quit));

		match rx_cmd
			.try_recv()
			.expect(":%s/    /\\t/g should send a substitute command")
		{
			EditorCommand::SubstituteAll {
				pattern,
				replacement,
				range,
				flags,
			} => {
				assert_eq!(pattern, "    ");
				assert_eq!(replacement, "\t");
				assert!(matches!(range, CommandRange::WholeFile));
				assert!(flags.global);
				assert!(!flags.confirm);
			}
			_ => panic!("Expected SubstituteAll"),
		}
	}
}
