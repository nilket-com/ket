use super::{EditorCommand, InputEvent};
use crate::core::{BufferId, JobId};
use std::sync::mpsc;

const ENGINE_REQUEST_QUEUE_LIMIT: usize = 2_048;

#[derive(Debug)]
pub(crate) enum EngineRequest {
	Command(EditorCommand),
	Input(InputEvent),
	Internal(InternalEvent),
}

#[derive(Debug)]
pub(crate) enum InternalEvent {
	Refresh,
	DrainEvents,
	HydrationCompleted(BufferId),
	ClearYankFlash,
	PathCompletion(super::input::PathCompletionResponse),
	RuneCompile(Box<super::rune_host::SourceCompileResult>),
	JobCompleted(JobId),
	GitObservationChanged,
	RefreshGitStatus,
}

/// Typed command ingress for a running editor engine.
///
/// This adapter preserves the public command-oriented embedding API while
/// keeping the engine's ordered request stream and internal events private.
#[derive(Clone)]
pub struct CommandSender {
	tx: mpsc::SyncSender<EngineRequest>,
}

impl CommandSender {
	pub fn send(&self, command: EditorCommand) -> Result<(), mpsc::SendError<EditorCommand>> {
		self.tx
			.send(EngineRequest::Command(command))
			.map_err(|mpsc::SendError(request)| match request {
				EngineRequest::Command(command) => mpsc::SendError(command),
				EngineRequest::Input(_) | EngineRequest::Internal(_) => {
					unreachable!("sent a command request")
				}
			})
	}

	pub fn try_send(
		&self,
		command: EditorCommand,
	) -> Result<(), mpsc::TrySendError<EditorCommand>> {
		self.tx
			.try_send(EngineRequest::Command(command))
			.map_err(|error| match error {
				mpsc::TrySendError::Full(EngineRequest::Command(command)) => {
					mpsc::TrySendError::Full(command)
				}
				mpsc::TrySendError::Disconnected(EngineRequest::Command(command)) => {
					mpsc::TrySendError::Disconnected(command)
				}
				mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_) => {
					unreachable!("sent a command request")
				}
			})
	}

	#[cfg(test)]
	pub(crate) fn refresh_for_test(&self) -> Result<(), mpsc::SendError<InternalEvent>> {
		InternalSender {
			tx: self.tx.clone(),
		}
		.send(InternalEvent::Refresh)
	}
}

/// Typed input ingress sharing the engine's ordered request stream.
#[derive(Clone)]
pub struct InputSender {
	tx: mpsc::SyncSender<EngineRequest>,
}

impl InputSender {
	pub fn send(&self, event: InputEvent) -> Result<(), mpsc::SendError<InputEvent>> {
		self.tx.send(EngineRequest::Input(event)).map_err(
			|mpsc::SendError(request)| match request {
				EngineRequest::Input(event) => mpsc::SendError(event),
				EngineRequest::Command(_) | EngineRequest::Internal(_) => {
					unreachable!("sent an input request")
				}
			},
		)
	}

	pub fn try_send(&self, event: InputEvent) -> Result<(), mpsc::TrySendError<InputEvent>> {
		self.tx
			.try_send(EngineRequest::Input(event))
			.map_err(|error| match error {
				mpsc::TrySendError::Full(EngineRequest::Input(event)) => {
					mpsc::TrySendError::Full(event)
				}
				mpsc::TrySendError::Disconnected(EngineRequest::Input(event)) => {
					mpsc::TrySendError::Disconnected(event)
				}
				mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_) => {
					unreachable!("sent an input request")
				}
			})
	}
}

#[derive(Debug, Clone)]
pub(crate) struct InternalSender {
	tx: mpsc::SyncSender<EngineRequest>,
}

impl InternalSender {
	pub(crate) fn send(&self, event: InternalEvent) -> Result<(), mpsc::SendError<InternalEvent>> {
		self.tx
			.send(EngineRequest::Internal(event))
			.map_err(|mpsc::SendError(request)| match request {
				EngineRequest::Internal(event) => mpsc::SendError(event),
				EngineRequest::Command(_) | EngineRequest::Input(_) => {
					unreachable!("sent an internal request")
				}
			})
	}

	pub(crate) fn try_send(
		&self,
		event: InternalEvent,
	) -> Result<(), mpsc::TrySendError<InternalEvent>> {
		self.tx
			.try_send(EngineRequest::Internal(event))
			.map_err(|error| match error {
				mpsc::TrySendError::Full(EngineRequest::Internal(event)) => {
					mpsc::TrySendError::Full(event)
				}
				mpsc::TrySendError::Disconnected(EngineRequest::Internal(event)) => {
					mpsc::TrySendError::Disconnected(event)
				}
				mpsc::TrySendError::Full(_) | mpsc::TrySendError::Disconnected(_) => {
					unreachable!("sent an internal request")
				}
			})
	}
}

pub(crate) fn channel() -> (
	CommandSender,
	InputSender,
	InternalSender,
	mpsc::Receiver<EngineRequest>,
) {
	let (tx, rx) = mpsc::sync_channel(ENGINE_REQUEST_QUEUE_LIMIT);
	(
		CommandSender { tx: tx.clone() },
		InputSender { tx: tx.clone() },
		InternalSender { tx },
		rx,
	)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn typed_adapters_share_one_ordered_stream() {
		let (commands, input, internal, requests) = channel();
		commands.send(EditorCommand::Undo).expect("command");
		input
			.send(InputEvent::Key {
				code: super::super::KeyCode::Char('x'),
				modifiers: super::super::Modifiers::NONE,
			})
			.expect("input");
		internal.send(InternalEvent::Refresh).expect("internal");
		commands.send(EditorCommand::Redo).expect("command");

		assert!(matches!(
			requests.recv(),
			Ok(EngineRequest::Command(EditorCommand::Undo))
		));
		assert!(matches!(
			requests.recv(),
			Ok(EngineRequest::Input(InputEvent::Key {
				code: super::super::KeyCode::Char('x'),
				modifiers: super::super::Modifiers::NONE,
			}))
		));
		assert!(matches!(
			requests.recv(),
			Ok(EngineRequest::Internal(InternalEvent::Refresh))
		));
		assert!(matches!(
			requests.recv(),
			Ok(EngineRequest::Command(EditorCommand::Redo))
		));
	}
}
