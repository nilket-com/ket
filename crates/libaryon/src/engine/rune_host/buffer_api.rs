use super::facade::{EditorHost, HostByteReservation, lock_services};
use crate::core::{BufferId, DocByte, DocLine, StateId, ViewId};
use crate::ecs::NodeId;
use crate::engine::document::access::{
	self, BufferEdit, ChunkCursor, EditError, LineCursor, ReadError, Readiness, WaitingOn,
};
use crate::engine::session::{Affinity, MAX_CURSORS, ViewContent};
use rune::alloc::String as RuneString;
use rune::alloc::Vec as RuneVec;
use rune::alloc::fmt::TryWrite;
use rune::runtime::{Bytes, Formatter, Protocol, VmResult};
use rune::{Any, Module};

pub(crate) const MAX_HANDLE_RESULTS: usize = 4_096;

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = Buffer)]
pub(crate) struct RuneBuffer {
	id: BufferId,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = View)]
pub(crate) struct RuneView {
	id: ViewId,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = Version)]
pub(crate) struct RuneVersion {
	state: StateId,
}

impl RuneBuffer {
	pub(super) const fn from_id(id: BufferId) -> Self {
		Self { id }
	}

	pub(super) const fn id(self) -> BufferId {
		self.id
	}
}

impl RuneView {
	pub(super) const fn from_id(id: ViewId) -> Self {
		Self { id }
	}
}

impl RuneVersion {
	pub(super) const fn from_state(state: StateId) -> Self {
		Self { state }
	}
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon)]
pub(crate) enum BufferErrorKind {
	#[rune(constructor)]
	NotReady(#[rune(get, copy)] NotReadyKind),
	#[rune(constructor)]
	StaleBuffer,
	#[rune(constructor)]
	StaleView,
	#[rune(constructor)]
	NotTextBuffer,
	#[rune(constructor)]
	NotTextView,
	#[rune(constructor)]
	StaleVersion,
	#[rune(constructor)]
	ResultLimitExceeded,
	#[rune(constructor)]
	Read(#[rune(get, copy)] ReadErrorKind),
	#[rune(constructor)]
	Edit(#[rune(get, copy)] EditErrorKind),
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon)]
pub(crate) enum NotReadyKind {
	#[rune(constructor)]
	LineIndexBuilding,
	#[rune(constructor)]
	ColdExtent,
	#[rune(constructor)]
	LineNewlineHydration,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon)]
pub(crate) enum ReadErrorKind {
	#[rune(constructor)]
	TooLarge,
	#[rune(constructor)]
	InvalidUtf8,
	#[rune(constructor)]
	QueueFull,
	#[rune(constructor)]
	ScanLimitExceeded,
	#[rune(constructor)]
	ColdReadFailed,
	#[rune(constructor)]
	NotResident,
	#[rune(constructor)]
	InvalidChunkSize,
	#[rune(constructor)]
	LineOutOfRange,
	#[rune(constructor)]
	WorkLimitExceeded,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon)]
pub(crate) enum EditErrorKind {
	#[rune(constructor)]
	NotUtf8Boundary,
	#[rune(constructor)]
	RangeInvalid,
	#[rune(constructor)]
	TooLarge,
	#[rune(constructor)]
	BatchTooLarge,
	#[rune(constructor)]
	NotUtf8Text,
	#[rune(constructor)]
	WorkLimitExceeded,
	#[rune(constructor)]
	QueueFull,
	#[rune(constructor)]
	ColdReadFailed,
	#[rune(constructor)]
	ViewUnavailable,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon)]
pub(crate) enum OnInvalid {
	#[rune(constructor)]
	Strict,
	#[rune(constructor)]
	Lossy,
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = Affinity)]
pub(crate) enum RuneAffinity {
	#[rune(constructor)]
	Before,
	#[rune(constructor)]
	After,
}

impl From<Affinity> for RuneAffinity {
	fn from(value: Affinity) -> Self {
		match value {
			Affinity::Before => Self::Before,
			Affinity::After => Self::After,
		}
	}
}

#[derive(Any, Debug, Clone, Copy, PartialEq, Eq)]
#[rune(item = ::baryon, name = Caret)]
pub(crate) struct RuneCaret {
	#[rune(get)]
	byte: u64,
	#[rune(get, copy)]
	affinity: RuneAffinity,
}

#[derive(Any, Debug, PartialEq, Eq)]
#[rune(item = ::baryon, name = Edit)]
pub(crate) enum RuneEdit {
	Bytes {
		start: u64,
		end: u64,
		replacement: Bytes,
	},
	Text {
		start: u64,
		end: u64,
		replacement: RuneString,
	},
}

impl RuneEdit {
	#[rune::function(path = Self::bytes)]
	fn bytes(start: u64, end: u64, replacement: Bytes) -> Self {
		Self::Bytes {
			start,
			end,
			replacement,
		}
	}

	#[rune::function(path = Self::text)]
	fn text(start: u64, end: u64, replacement: RuneString) -> Self {
		Self::Text {
			start,
			end,
			replacement,
		}
	}

	fn range(&self) -> (u64, u64) {
		match self {
			Self::Bytes { start, end, .. } | Self::Text { start, end, .. } => (*start, *end),
		}
	}

	fn replacement_len(&self) -> u64 {
		match self {
			Self::Bytes { replacement, .. } => replacement.len() as u64,
			Self::Text { replacement, .. } => replacement.len() as u64,
		}
	}

	fn to_engine_edit(&self) -> BufferEdit {
		match self {
			Self::Bytes {
				start,
				end,
				replacement,
			} => BufferEdit::Bytes {
				start: DocByte::new(*start),
				end: DocByte::new(*end),
				replacement: replacement.as_slice().to_vec(),
			},
			Self::Text {
				start,
				end,
				replacement,
			} => BufferEdit::Text {
				start: DocByte::new(*start),
				end: DocByte::new(*end),
				replacement: replacement.as_bytes().to_vec(),
			},
		}
	}
}

#[derive(Any, Debug, Clone, PartialEq, Eq)]
#[rune(item = ::baryon, name = ChunkCursor)]
pub(crate) struct RuneChunkCursor {
	buffer: BufferId,
	cursor: ChunkCursor,
}

#[derive(Any, Debug, Clone, PartialEq, Eq)]
#[rune(item = ::baryon, name = LineCursor)]
pub(crate) struct RuneLineCursor {
	buffer: BufferId,
	cursor: LineCursor,
}

#[derive(Any, Debug, PartialEq, Eq)]
#[rune(item = ::baryon, name = LinePage)]
pub(crate) struct RuneLinePage {
	#[rune(get)]
	line: u64,
	#[rune(get)]
	start: u64,
	#[rune(get)]
	end: u64,
	#[rune(get)]
	bytes: Bytes,
	#[rune(get)]
	truncated: bool,
}

#[derive(Any, Debug, Clone, PartialEq, Eq)]
#[rune(item = ::baryon)]
pub(crate) struct BufferError {
	kind: BufferErrorKind,
	detail: Option<u64>,
	maximum: Option<u64>,
}

impl BufferError {
	fn new(kind: BufferErrorKind) -> Self {
		Self {
			kind,
			detail: None,
			maximum: None,
		}
	}

	fn kind(&self) -> BufferErrorKind {
		self.kind
	}

	fn detail(&self) -> Option<u64> {
		self.detail
	}

	fn maximum(&self) -> Option<u64> {
		self.maximum
	}
}

fn with_detail(kind: BufferErrorKind, detail: u64, maximum: Option<u64>) -> BufferError {
	BufferError {
		kind,
		detail: Some(detail),
		maximum,
	}
}

fn waiting_error(waiting_on: WaitingOn) -> BufferError {
	match waiting_on {
		WaitingOn::LineIndexBuilding => {
			BufferError::new(BufferErrorKind::NotReady(NotReadyKind::LineIndexBuilding))
		}
		WaitingOn::ColdExtent { start, end } => BufferError {
			kind: BufferErrorKind::NotReady(NotReadyKind::ColdExtent),
			detail: Some(start.get()),
			maximum: Some(end.get()),
		},
		WaitingOn::LineNewlineHydration { line } => BufferError {
			kind: BufferErrorKind::NotReady(NotReadyKind::LineNewlineHydration),
			detail: Some(line.get()),
			maximum: None,
		},
	}
}

fn read_error(error: ReadError) -> BufferError {
	match error {
		ReadError::TooLarge { requested, maximum } => with_detail(
			BufferErrorKind::Read(ReadErrorKind::TooLarge),
			requested,
			Some(maximum),
		),
		ReadError::InvalidUtf8 { absolute_offset } => with_detail(
			BufferErrorKind::Read(ReadErrorKind::InvalidUtf8),
			absolute_offset.get(),
			None,
		),
		ReadError::QueueFull => BufferError::new(BufferErrorKind::Read(ReadErrorKind::QueueFull)),
		ReadError::ScanLimitExceeded { scanned, maximum } => with_detail(
			BufferErrorKind::Read(ReadErrorKind::ScanLimitExceeded),
			scanned,
			Some(maximum),
		),
		ReadError::ColdReadFailed => {
			BufferError::new(BufferErrorKind::Read(ReadErrorKind::ColdReadFailed))
		}
		ReadError::StaleVersion => BufferError::new(BufferErrorKind::StaleVersion),
		ReadError::NotResident => {
			BufferError::new(BufferErrorKind::Read(ReadErrorKind::NotResident))
		}
		ReadError::InvalidChunkSize => {
			BufferError::new(BufferErrorKind::Read(ReadErrorKind::InvalidChunkSize))
		}
		ReadError::LineOutOfRange { line, line_count } => with_detail(
			BufferErrorKind::Read(ReadErrorKind::LineOutOfRange),
			line.get(),
			Some(line_count),
		),
		ReadError::WorkLimitExceeded { maximum } => with_detail(
			BufferErrorKind::Read(ReadErrorKind::WorkLimitExceeded),
			maximum as u64,
			Some(maximum as u64),
		),
	}
}

fn edit_error(error: EditError) -> BufferError {
	match error {
		EditError::NotUtf8Boundary { endpoint } => with_detail(
			BufferErrorKind::Edit(EditErrorKind::NotUtf8Boundary),
			endpoint.get(),
			None,
		),
		EditError::RangeInvalid { start, end, total } => {
			let (detail, maximum) = if end.get() > total {
				(end.get(), total)
			} else {
				(start.get(), end.get())
			};
			BufferError {
				kind: BufferErrorKind::Edit(EditErrorKind::RangeInvalid),
				detail: Some(detail),
				maximum: Some(maximum),
			}
		}
		EditError::TooLarge { requested, maximum } => with_detail(
			BufferErrorKind::Edit(EditErrorKind::TooLarge),
			requested,
			Some(maximum),
		),
		EditError::BatchTooLarge { requested, maximum } => with_detail(
			BufferErrorKind::Edit(EditErrorKind::BatchTooLarge),
			requested,
			Some(maximum),
		),
		EditError::NotUtf8Text { relative_offset } => with_detail(
			BufferErrorKind::Edit(EditErrorKind::NotUtf8Text),
			relative_offset,
			None,
		),
		EditError::WorkLimitExceeded { maximum } => with_detail(
			BufferErrorKind::Edit(EditErrorKind::WorkLimitExceeded),
			maximum as u64,
			Some(maximum as u64),
		),
		EditError::QueueFull => BufferError::new(BufferErrorKind::Edit(EditErrorKind::QueueFull)),
		EditError::ColdReadFailed => {
			BufferError::new(BufferErrorKind::Edit(EditErrorKind::ColdReadFailed))
		}
		EditError::StaleVersion => BufferError::new(BufferErrorKind::StaleVersion),
		EditError::ViewUnavailable => {
			BufferError::new(BufferErrorKind::Edit(EditErrorKind::ViewUnavailable))
		}
	}
}

fn text_buffer_state(
	editor: &EditorHost,
	buffer_id: BufferId,
) -> Result<(NodeId, StateId), BufferError> {
	editor
		.session
		.buffers
		.get(buffer_id)
		.ok_or_else(|| BufferError::new(BufferErrorKind::StaleBuffer))
		.and_then(|buffer| {
			buffer
				.text()
				.ok_or_else(|| BufferError::new(BufferErrorKind::NotTextBuffer))
		})
		.map(|document| (document.root_id, document.ledger.current_state_id))
}

fn to_rune_bytes(bytes: Vec<u8>) -> Result<Bytes, rune::alloc::Error> {
	Bytes::try_from(bytes)
}

fn to_rune_string(string: String) -> Result<RuneString, rune::alloc::Error> {
	RuneString::try_from(string)
}

fn charge_call(editor: &EditorHost) -> Result<(), &'static str> {
	lock_services(&editor.services).budget.charge()
}

fn focused_view_id(editor: &EditorHost) -> Result<ViewId, BufferError> {
	let pane = editor
		.session
		.layout
		.find_pane(editor.session.focused_pane_id)
		.ok_or_else(|| BufferError::new(BufferErrorKind::StaleView))?;
	let view = pane
		.tabs
		.get(pane.active_tab_index)
		.ok_or_else(|| BufferError::new(BufferErrorKind::StaleView))?;
	if !matches!(view.content, ViewContent::Text(_)) {
		return Err(BufferError::new(BufferErrorKind::NotTextView));
	}
	if editor
		.session
		.buffers
		.get(view.buffer_id)
		.and_then(|buffer| buffer.text())
		.is_none()
	{
		return Err(BufferError::new(BufferErrorKind::NotTextBuffer));
	}
	Ok(view.id)
}

fn focused_buffer(editor: &mut EditorHost) -> VmResult<Result<RuneBuffer, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let view_id = match focused_view_id(editor) {
		Ok(view_id) => view_id,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let Some(view) = editor.session.layout.find_view(view_id) else {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::StaleView)));
	};
	VmResult::Ok(Ok(RuneBuffer { id: view.buffer_id }))
}

fn focused_view(editor: &mut EditorHost) -> VmResult<Result<RuneView, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	VmResult::Ok(focused_view_id(editor).map(|id| RuneView { id }))
}

fn buffer_version(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
) -> VmResult<Result<RuneVersion, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let result = editor
		.session
		.buffers
		.get(buffer.id)
		.ok_or_else(|| BufferError::new(BufferErrorKind::StaleBuffer))
		.and_then(|buffer| {
			buffer
				.text()
				.ok_or_else(|| BufferError::new(BufferErrorKind::NotTextBuffer))
		})
		.map(|document| RuneVersion {
			state: document.ledger.current_state_id,
		});
	VmResult::Ok(result)
}

fn buffer_total_bytes(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
) -> VmResult<Result<u64, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let result = editor
		.session
		.buffers
		.get(buffer.id)
		.ok_or_else(|| BufferError::new(BufferErrorKind::StaleBuffer))
		.and_then(|buffer| {
			buffer
				.text()
				.ok_or_else(|| BufferError::new(BufferErrorKind::NotTextBuffer))
		})
		.map(|document| editor.registry.get_total_bytes(document.root_id));
	VmResult::Ok(result)
}

fn view_buffer(
	view: &RuneView,
	editor: &mut EditorHost,
) -> VmResult<Result<RuneBuffer, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let result = editor
		.session
		.layout
		.find_view(view.id)
		.ok_or_else(|| BufferError::new(BufferErrorKind::StaleView))
		.and_then(|view| {
			if !matches!(view.content, ViewContent::Text(_)) {
				return Err(BufferError::new(BufferErrorKind::NotTextView));
			}
			if editor
				.session
				.buffers
				.get(view.buffer_id)
				.and_then(|buffer| buffer.text())
				.is_none()
			{
				return Err(BufferError::new(BufferErrorKind::NotTextBuffer));
			}
			Ok(RuneBuffer { id: view.buffer_id })
		});
	VmResult::Ok(result)
}

fn buffer_views(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
) -> VmResult<Result<RuneVec<RuneView>, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let buffer_exists = editor.session.buffers.get(buffer.id);
	let Some(_document) = buffer_exists.and_then(|buffer| buffer.text()) else {
		let kind = if buffer_exists.is_some() {
			BufferErrorKind::NotTextBuffer
		} else {
			BufferErrorKind::StaleBuffer
		};
		return VmResult::Ok(Err(BufferError::new(kind)));
	};
	let mut ids = Vec::new();
	let count =
		editor
			.session
			.layout
			.collect_view_ids_for_buffer(buffer.id, &mut ids, MAX_HANDLE_RESULTS);
	if count > MAX_HANDLE_RESULTS {
		return VmResult::Ok(Err(BufferError {
			kind: BufferErrorKind::ResultLimitExceeded,
			detail: Some(count as u64),
			maximum: Some(MAX_HANDLE_RESULTS as u64),
		}));
	}
	let mut views = match RuneVec::try_with_capacity(ids.len()) {
		Ok(views) => views,
		Err(error) => return VmResult::err(error),
	};
	for id in ids {
		if let Err(error) = views.try_push(RuneView { id }) {
			return VmResult::err(error);
		}
	}
	VmResult::Ok(Ok(views))
}

fn buffer_read_bytes(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
	start: u64,
	end: u64,
) -> VmResult<Result<Bytes, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, _) = match text_buffer_state(editor, buffer.id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let total = editor.registry.get_total_bytes(root);
	let clamped_start = start.min(total);
	let clamped_end = end.min(total);
	let requested = clamped_end.saturating_sub(clamped_start);
	if requested > access::MAX_READ_BYTES {
		return VmResult::Ok(Err(read_error(ReadError::TooLarge {
			requested,
			maximum: access::MAX_READ_BYTES,
		})));
	}
	let reservation = match HostByteReservation::read(&editor.services, requested as usize) {
		Ok(reservation) => reservation,
		Err(error) => return VmResult::panic(error),
	};
	match access::read_bytes_for_buffer(
		&editor.registry,
		&editor.resolver,
		buffer.id,
		root,
		DocByte::new(start),
		DocByte::new(end),
	) {
		Ok(Readiness::Ready(bytes)) => {
			let actual = bytes.len();
			match to_rune_bytes(bytes) {
				Ok(bytes) => {
					if let Err(error) = reservation.complete(actual) {
						return VmResult::panic(error);
					}
					VmResult::Ok(Ok(bytes))
				}
				Err(error) => VmResult::err(error),
			}
		}
		Ok(Readiness::NotReady { waiting_on }) => VmResult::Ok(Err(waiting_error(waiting_on))),
		Err(error) => VmResult::Ok(Err(read_error(error))),
	}
}

fn buffer_read_text(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
	start: u64,
	end: u64,
	on_invalid: OnInvalid,
) -> VmResult<Result<RuneString, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, _) = match text_buffer_state(editor, buffer.id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let total = editor.registry.get_total_bytes(root);
	let clamped_start = start.min(total);
	let clamped_end = end.min(total);
	let requested = clamped_end.saturating_sub(clamped_start);
	if requested > access::MAX_READ_BYTES {
		return VmResult::Ok(Err(read_error(ReadError::TooLarge {
			requested,
			maximum: access::MAX_READ_BYTES,
		})));
	}
	let output_bound = match on_invalid {
		OnInvalid::Strict => requested,
		OnInvalid::Lossy => requested.saturating_mul(3),
	};
	let reservation = match HostByteReservation::read(&editor.services, output_bound as usize) {
		Ok(reservation) => reservation,
		Err(error) => return VmResult::panic(error),
	};
	let bytes = match access::read_bytes_for_buffer(
		&editor.registry,
		&editor.resolver,
		buffer.id,
		root,
		DocByte::new(start),
		DocByte::new(end),
	) {
		Ok(Readiness::Ready(bytes)) => bytes,
		Ok(Readiness::NotReady { waiting_on }) => {
			return VmResult::Ok(Err(waiting_error(waiting_on)));
		}
		Err(error) => return VmResult::Ok(Err(read_error(error))),
	};
	let absolute_start = DocByte::new(clamped_start);
	let text = match on_invalid {
		OnInvalid::Strict => match access::decode_strict(&bytes, absolute_start) {
			Ok(text) => text,
			Err(error) => return VmResult::Ok(Err(read_error(error))),
		},
		OnInvalid::Lossy => access::decode_lossy(&bytes),
	};
	let actual = text.len();
	match to_rune_string(text) {
		Ok(text) => {
			if let Err(error) = reservation.complete(actual) {
				return VmResult::panic(error);
			}
			VmResult::Ok(Ok(text))
		}
		Err(error) => VmResult::err(error),
	}
}

fn buffer_line_count(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
) -> VmResult<Result<u64, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, _) = match text_buffer_state(editor, buffer.id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	match access::line_count(&editor.registry, root) {
		Ok(Readiness::Ready(count)) => VmResult::Ok(Ok(count)),
		Ok(Readiness::NotReady { waiting_on }) => VmResult::Ok(Err(waiting_error(waiting_on))),
		Err(error) => VmResult::Ok(Err(read_error(error))),
	}
}

fn buffer_line_range(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
	line: u64,
) -> VmResult<Result<(u64, u64), BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, _) = match text_buffer_state(editor, buffer.id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let line = DocLine::new(line);
	let start = match access::line_start_for_buffer(
		&editor.registry,
		&editor.resolver,
		buffer.id,
		root,
		line,
	) {
		Ok(Readiness::Ready(start)) => start,
		Ok(Readiness::NotReady { waiting_on }) => {
			return VmResult::Ok(Err(waiting_error(waiting_on)));
		}
		Err(error) => return VmResult::Ok(Err(read_error(error))),
	};
	let count = match access::line_count(&editor.registry, root) {
		Ok(Readiness::Ready(count)) => count,
		Ok(Readiness::NotReady { waiting_on }) => {
			return VmResult::Ok(Err(waiting_error(waiting_on)));
		}
		Err(error) => return VmResult::Ok(Err(read_error(error))),
	};
	let end = if line.get().saturating_add(1) < count {
		match access::line_start_for_buffer(
			&editor.registry,
			&editor.resolver,
			buffer.id,
			root,
			line.saturating_add(1),
		) {
			Ok(Readiness::Ready(end)) => end,
			Ok(Readiness::NotReady { waiting_on }) => {
				return VmResult::Ok(Err(waiting_error(waiting_on)));
			}
			Err(error) => return VmResult::Ok(Err(read_error(error))),
		}
	} else {
		DocByte::new(editor.registry.get_total_bytes(root))
	};
	VmResult::Ok(Ok((start.get(), end.get())))
}

fn buffer_read_chunks(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
	start: u64,
	end: u64,
	max_chunk_len: u64,
) -> VmResult<Result<RuneChunkCursor, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (_, state) = match text_buffer_state(editor, buffer.id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	VmResult::Ok(Ok(RuneChunkCursor {
		buffer: buffer.id,
		cursor: ChunkCursor {
			start: DocByte::new(start),
			end: DocByte::new(end),
			position: DocByte::new(start),
			state,
			max_chunk_len,
		},
	}))
}

fn chunk_cursor_next(
	cursor: &mut RuneChunkCursor,
	editor: &mut EditorHost,
) -> VmResult<Result<Option<Bytes>, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, state) = match text_buffer_state(editor, cursor.buffer) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	if state != cursor.cursor.state {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::StaleVersion)));
	}
	let total = editor.registry.get_total_bytes(root);
	let requested_start = cursor.cursor.start.get().min(total);
	let requested_end = cursor.cursor.end.get().min(total);
	let (start, end) = if requested_start > requested_end {
		(requested_end, requested_end)
	} else {
		(requested_start, requested_end)
	};
	let position = cursor.cursor.position.get().clamp(start, end);
	let remaining = end.saturating_sub(position);
	if remaining == 0 {
		cursor.cursor.position = DocByte::new(end);
		return VmResult::Ok(Ok(None));
	}
	let reserve = remaining
		.min(cursor.cursor.max_chunk_len)
		.min(access::MAX_READ_BYTES);
	let reservation = match HostByteReservation::read(&editor.services, reserve as usize) {
		Ok(reservation) => reservation,
		Err(error) => return VmResult::panic(error),
	};
	match access::read_chunk_for_buffer(
		&editor.registry,
		&editor.resolver,
		cursor.buffer,
		root,
		state,
		&cursor.cursor,
	) {
		Ok(Readiness::Ready(page)) if page.done && page.bytes.is_empty() => {
			cursor.cursor.position = page.next;
			if let Err(error) = reservation.complete(0) {
				return VmResult::panic(error);
			}
			VmResult::Ok(Ok(None))
		}
		Ok(Readiness::Ready(page)) => {
			let actual = page.bytes.len();
			match to_rune_bytes(page.bytes) {
				Ok(bytes) => {
					if let Err(error) = reservation.complete(actual) {
						return VmResult::panic(error);
					}
					cursor.cursor.position = page.next;
					VmResult::Ok(Ok(Some(bytes)))
				}
				Err(error) => VmResult::err(error),
			}
		}
		Ok(Readiness::NotReady { waiting_on }) => VmResult::Ok(Err(waiting_error(waiting_on))),
		Err(error) => VmResult::Ok(Err(read_error(error))),
	}
}

fn buffer_lines(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
	per_line_cap: u64,
) -> VmResult<Result<RuneLineCursor, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, state) = match text_buffer_state(editor, buffer.id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let count = match access::line_count(&editor.registry, root) {
		Ok(Readiness::Ready(count)) => count,
		Ok(Readiness::NotReady { waiting_on }) => {
			return VmResult::Ok(Err(waiting_error(waiting_on)));
		}
		Err(error) => return VmResult::Ok(Err(read_error(error))),
	};
	VmResult::Ok(Ok(RuneLineCursor {
		buffer: buffer.id,
		cursor: LineCursor {
			next_line: DocLine::ZERO,
			end_line: DocLine::new(count),
			state,
			max_line_bytes: per_line_cap,
		},
	}))
}

fn line_cursor_next(
	cursor: &mut RuneLineCursor,
	editor: &mut EditorHost,
) -> VmResult<Result<Option<RuneLinePage>, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, state) = match text_buffer_state(editor, cursor.buffer) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	if state != cursor.cursor.state {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::StaleVersion)));
	}
	let exhausted = cursor.cursor.next_line >= cursor.cursor.end_line;
	if exhausted {
		return VmResult::Ok(Ok(None));
	}
	let reserve = cursor.cursor.max_line_bytes.min(access::MAX_READ_BYTES);
	let reservation = match HostByteReservation::read(&editor.services, reserve as usize) {
		Ok(reservation) => reservation,
		Err(error) => return VmResult::panic(error),
	};
	match access::read_line_page_for_buffer(
		&editor.registry,
		&editor.resolver,
		cursor.buffer,
		root,
		state,
		&cursor.cursor,
	) {
		Ok(Readiness::Ready(page)) => {
			let actual = page.bytes.len();
			match to_rune_bytes(page.bytes) {
				Ok(bytes) => {
					if let Err(error) = reservation.complete(actual) {
						return VmResult::panic(error);
					}
					cursor.cursor.next_line = page.next_line;
					VmResult::Ok(Ok(Some(RuneLinePage {
						line: page.line.get(),
						start: page.start.get(),
						end: page.end.get(),
						bytes,
						truncated: page.truncated,
					})))
				}
				Err(error) => VmResult::err(error),
			}
		}
		Ok(Readiness::NotReady { waiting_on }) => VmResult::Ok(Err(waiting_error(waiting_on))),
		Err(error) => VmResult::Ok(Err(read_error(error))),
	}
}

fn buffer_snapshot(
	buffer: &RuneBuffer,
	editor: &mut EditorHost,
	max_bytes: u64,
) -> VmResult<Result<Bytes, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let (root, _) = match text_buffer_state(editor, buffer.id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let total = editor.registry.get_total_bytes(root);
	let maximum = max_bytes.min(access::MAX_SNAPSHOT_BYTES);
	if total > maximum || max_bytes > access::MAX_SNAPSHOT_BYTES {
		return VmResult::Ok(Err(read_error(ReadError::TooLarge {
			requested: total.max(max_bytes),
			maximum,
		})));
	}
	let reservation = match HostByteReservation::read(&editor.services, total as usize) {
		Ok(reservation) => reservation,
		Err(error) => return VmResult::panic(error),
	};
	match access::snapshot(&editor.registry, root, max_bytes) {
		Ok(bytes) => {
			let actual = bytes.len();
			match to_rune_bytes(bytes) {
				Ok(bytes) => {
					if let Err(error) = reservation.complete(actual) {
						return VmResult::panic(error);
					}
					VmResult::Ok(Ok(bytes))
				}
				Err(error) => VmResult::err(error),
			}
		}
		Err(error) => VmResult::Ok(Err(read_error(error))),
	}
}

fn view_text_buffer_id(editor: &EditorHost, view_id: ViewId) -> Result<BufferId, BufferError> {
	let view = editor
		.session
		.layout
		.find_view(view_id)
		.ok_or_else(|| BufferError::new(BufferErrorKind::StaleView))?;
	if !matches!(view.content, ViewContent::Text(_)) {
		return Err(BufferError::new(BufferErrorKind::NotTextView));
	}
	if editor
		.session
		.buffers
		.get(view.buffer_id)
		.and_then(|buffer| buffer.text())
		.is_none()
	{
		return Err(BufferError::new(BufferErrorKind::NotTextBuffer));
	}
	Ok(view.buffer_id)
}

fn caret_value(caret: &crate::engine::session::Caret) -> RuneCaret {
	RuneCaret {
		byte: caret.byte().get(),
		affinity: caret.affinity().into(),
	}
}

fn view_primary_caret(
	view: &RuneView,
	editor: &mut EditorHost,
) -> VmResult<Result<RuneCaret, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	if let Err(error) = view_text_buffer_id(editor, view.id) {
		return VmResult::Ok(Err(error));
	}
	let Some(view) = editor.session.layout.find_view(view.id) else {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::StaleView)));
	};
	let ViewContent::Text(text) = &view.content else {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::NotTextView)));
	};
	VmResult::Ok(Ok(caret_value(text.cursor.primary())))
}

fn view_carets(
	view: &RuneView,
	editor: &mut EditorHost,
) -> VmResult<Result<RuneVec<RuneCaret>, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	if let Err(error) = view_text_buffer_id(editor, view.id) {
		return VmResult::Ok(Err(error));
	}
	let Some(view) = editor.session.layout.find_view(view.id) else {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::StaleView)));
	};
	let ViewContent::Text(text) = &view.content else {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::NotTextView)));
	};
	let count = text.cursor.points().map_or(1, |points| points.len());
	if count > MAX_CURSORS {
		return VmResult::Ok(Err(with_detail(
			BufferErrorKind::ResultLimitExceeded,
			count as u64,
			Some(MAX_CURSORS as u64),
		)));
	}
	let mut carets = match RuneVec::try_with_capacity(count) {
		Ok(carets) => carets,
		Err(error) => return VmResult::err(error),
	};
	if let Some(points) = text.cursor.points() {
		for caret in points.all() {
			if let Err(error) = carets.try_push(caret_value(caret)) {
				return VmResult::err(error);
			}
		}
	} else if let Err(error) = carets.try_push(caret_value(text.cursor.primary())) {
		return VmResult::err(error);
	}
	VmResult::Ok(Ok(carets))
}

fn apply_view_edits(
	view: &RuneView,
	editor: &mut EditorHost,
	edits: &[RuneEdit],
	expected: Option<StateId>,
) -> VmResult<Result<RuneVersion, BufferError>> {
	let buffer_id = match view_text_buffer_id(editor, view.id) {
		Ok(buffer_id) => buffer_id,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let (root, current_version) = match text_buffer_state(editor, buffer_id) {
		Ok(state) => state,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	if expected.is_some_and(|expected| expected != current_version) {
		return VmResult::Ok(Err(BufferError::new(BufferErrorKind::StaleVersion)));
	}
	let total = editor.registry.get_total_bytes(root);
	if edits.len() > access::MAX_BATCH_DELTAS {
		return VmResult::Ok(Err(with_detail(
			BufferErrorKind::Edit(EditErrorKind::BatchTooLarge),
			edits.len() as u64,
			Some(access::MAX_BATCH_DELTAS as u64),
		)));
	}
	let mut accounted = 0u64;
	for edit in edits {
		let (start, end) = edit.range();
		if start > end || end > total {
			return VmResult::Ok(Err(edit_error(EditError::RangeInvalid {
				start: DocByte::new(start),
				end: DocByte::new(end),
				total,
			})));
		}
		let Some(bytes) = end
			.checked_sub(start)
			.and_then(|deleted| deleted.checked_add(edit.replacement_len()))
		else {
			return VmResult::Ok(Err(with_detail(
				BufferErrorKind::Edit(EditErrorKind::TooLarge),
				u64::MAX,
				Some(access::MAX_EDIT_BYTES),
			)));
		};
		if bytes > access::MAX_EDIT_BYTES {
			return VmResult::Ok(Err(with_detail(
				BufferErrorKind::Edit(EditErrorKind::TooLarge),
				bytes,
				Some(access::MAX_EDIT_BYTES),
			)));
		}
		accounted = match accounted.checked_add(bytes) {
			Some(accounted) => accounted,
			None => {
				return VmResult::Ok(Err(with_detail(
					BufferErrorKind::Edit(EditErrorKind::BatchTooLarge),
					u64::MAX,
					Some(access::MAX_BATCH_BYTES),
				)));
			}
		};
	}
	if accounted > access::MAX_BATCH_BYTES {
		return VmResult::Ok(Err(with_detail(
			BufferErrorKind::Edit(EditErrorKind::BatchTooLarge),
			accounted.max(edits.len() as u64),
			Some(access::MAX_BATCH_BYTES),
		)));
	}
	let reservation = match HostByteReservation::edit(&editor.services, accounted as usize) {
		Ok(reservation) => reservation,
		Err(error) => return VmResult::panic(error),
	};
	let engine_edits: Vec<_> = edits.iter().map(RuneEdit::to_engine_edit).collect();
	let registry = editor.registry.clone();
	let resolver = editor.resolver.clone();
	match access::edit_text_view(
		&registry,
		&resolver,
		&mut editor.session,
		view.id,
		&engine_edits,
		expected,
	) {
		Ok(Readiness::Ready(outcome)) => {
			let actual = outcome.deleted_bytes.saturating_add(outcome.inserted_bytes) as usize;
			if let Err(error) = reservation.complete(actual) {
				return VmResult::panic(error);
			}
			editor.mark_needs_render();
			VmResult::Ok(Ok(RuneVersion {
				state: outcome.version,
			}))
		}
		Ok(Readiness::NotReady { waiting_on }) => VmResult::Ok(Err(waiting_error(waiting_on))),
		Err(error) => VmResult::Ok(Err(edit_error(error))),
	}
}

fn view_replace_bytes(
	view: &RuneView,
	editor: &mut EditorHost,
	start: u64,
	end: u64,
	replacement: Bytes,
) -> VmResult<Result<RuneVersion, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let edit = RuneEdit::Bytes {
		start,
		end,
		replacement,
	};
	apply_view_edits(view, editor, std::slice::from_ref(&edit), None)
}

fn view_replace_bytes_if(
	view: &RuneView,
	editor: &mut EditorHost,
	expected: RuneVersion,
	range: (u64, u64),
	replacement: Bytes,
) -> VmResult<Result<RuneVersion, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let edit = RuneEdit::Bytes {
		start: range.0,
		end: range.1,
		replacement,
	};
	apply_view_edits(
		view,
		editor,
		std::slice::from_ref(&edit),
		Some(expected.state),
	)
}

fn view_replace_text(
	view: &RuneView,
	editor: &mut EditorHost,
	start: u64,
	end: u64,
	replacement: RuneString,
) -> VmResult<Result<RuneVersion, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let edit = RuneEdit::Text {
		start,
		end,
		replacement,
	};
	apply_view_edits(view, editor, std::slice::from_ref(&edit), None)
}

fn view_replace_text_if(
	view: &RuneView,
	editor: &mut EditorHost,
	expected: RuneVersion,
	range: (u64, u64),
	replacement: RuneString,
) -> VmResult<Result<RuneVersion, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	let edit = RuneEdit::Text {
		start: range.0,
		end: range.1,
		replacement,
	};
	apply_view_edits(
		view,
		editor,
		std::slice::from_ref(&edit),
		Some(expected.state),
	)
}

fn view_edit(
	view: &RuneView,
	editor: &mut EditorHost,
	edits: RuneVec<RuneEdit>,
) -> VmResult<Result<RuneVersion, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	apply_view_edits(view, editor, &edits, None)
}

fn view_edit_if(
	view: &RuneView,
	editor: &mut EditorHost,
	expected: RuneVersion,
	edits: RuneVec<RuneEdit>,
) -> VmResult<Result<RuneVersion, BufferError>> {
	if let Err(error) = charge_call(editor) {
		return VmResult::panic(error);
	}
	apply_view_edits(view, editor, &edits, Some(expected.state))
}

fn clone_buffer(this: &RuneBuffer) -> RuneBuffer {
	*this
}

fn clone_view(this: &RuneView) -> RuneView {
	*this
}

fn clone_version(this: &RuneVersion) -> RuneVersion {
	*this
}

fn eq_buffer(this: &RuneBuffer, other: &RuneBuffer) -> bool {
	this == other
}

fn eq_view(this: &RuneView, other: &RuneView) -> bool {
	this == other
}

fn eq_version(this: &RuneVersion, other: &RuneVersion) -> bool {
	this == other
}

fn debug_buffer(this: &RuneBuffer, formatter: &mut Formatter) -> VmResult<()> {
	rune::vm_write!(formatter, "Buffer({})", this.id.get())
}

fn debug_view(this: &RuneView, formatter: &mut Formatter) -> VmResult<()> {
	rune::vm_write!(formatter, "View({})", this.id.get())
}

fn debug_version(this: &RuneVersion, formatter: &mut Formatter) -> VmResult<()> {
	rune::vm_write!(formatter, "Version({})", this.state.get())
}

pub(crate) fn install(module: &mut Module) -> Result<(), rune::ContextError> {
	module.ty::<RuneBuffer>()?;
	module.ty::<RuneView>()?;
	module.ty::<RuneVersion>()?;
	module.ty::<RuneChunkCursor>()?;
	module.ty::<RuneLineCursor>()?;
	module.ty::<RuneLinePage>()?;
	module.ty::<RuneAffinity>()?;
	module.ty::<RuneCaret>()?;
	module.ty::<RuneEdit>()?;
	module.ty::<BufferError>()?;
	module.ty::<BufferErrorKind>()?;
	module.ty::<NotReadyKind>()?;
	module.ty::<ReadErrorKind>()?;
	module.ty::<EditErrorKind>()?;
	module.ty::<OnInvalid>()?;

	module.function("focused_buffer", focused_buffer).build()?;
	module.function("focused_view", focused_view).build()?;
	module.associated_function("version", buffer_version)?;
	module.associated_function("total_bytes", buffer_total_bytes)?;
	module.associated_function("views", buffer_views)?;
	module.associated_function("read_bytes", buffer_read_bytes)?;
	module.associated_function("read_text", buffer_read_text)?;
	module.associated_function("line_count", buffer_line_count)?;
	module.associated_function("line_range", buffer_line_range)?;
	module.associated_function("read_chunks", buffer_read_chunks)?;
	module.associated_function("lines", buffer_lines)?;
	module.associated_function("snapshot", buffer_snapshot)?;
	module.associated_function("next", chunk_cursor_next)?;
	module.associated_function("next", line_cursor_next)?;
	module.associated_function("buffer", view_buffer)?;
	module.associated_function("primary_caret", view_primary_caret)?;
	module.associated_function("carets", view_carets)?;
	module.associated_function("replace_bytes", view_replace_bytes)?;
	module.associated_function("replace_bytes_if", view_replace_bytes_if)?;
	module.associated_function("replace_text", view_replace_text)?;
	module.associated_function("replace_text_if", view_replace_text_if)?;
	module.associated_function("edit", view_edit)?;
	module.associated_function("edit_if", view_edit_if)?;
	module.function_meta(RuneEdit::bytes)?;
	module.function_meta(RuneEdit::text)?;
	module.associated_function("kind", BufferError::kind)?;
	module.associated_function("detail", BufferError::detail)?;
	module.associated_function("maximum", BufferError::maximum)?;

	module.associated_function(&Protocol::CLONE, clone_buffer)?;
	module.associated_function(&Protocol::CLONE, clone_view)?;
	module.associated_function(&Protocol::CLONE, clone_version)?;
	module.associated_function(&Protocol::PARTIAL_EQ, eq_buffer)?;
	module.associated_function(&Protocol::PARTIAL_EQ, eq_view)?;
	module.associated_function(&Protocol::PARTIAL_EQ, eq_version)?;
	module.associated_function(&Protocol::DEBUG_FMT, debug_buffer)?;
	module.associated_function(&Protocol::DEBUG_FMT, debug_view)?;
	module.associated_function(&Protocol::DEBUG_FMT, debug_version)?;
	module.implement_trait::<RuneBuffer>(rune::item!(::std::clone::Clone))?;
	module.implement_trait::<RuneView>(rune::item!(::std::clone::Clone))?;
	module.implement_trait::<RuneVersion>(rune::item!(::std::clone::Clone))?;
	module.implement_trait::<RuneBuffer>(rune::item!(::std::cmp::PartialEq))?;
	module.implement_trait::<RuneView>(rune::item!(::std::cmp::PartialEq))?;
	module.implement_trait::<RuneVersion>(rune::item!(::std::cmp::PartialEq))?;
	Ok(())
}
