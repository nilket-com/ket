use super::{EditorCommand, InputEvent};
use crate::core::{BufferId, JobId};
use std::sync::mpsc;

pub(crate) const ENGINE_REQUEST_QUEUE_LIMIT: usize = 2_048;

#[derive(Debug)]
pub(crate) enum EngineRequest {
	Command(EditorCommand),
	Input(InputEvent),
	Internal(InternalEvent),
	Checkpoint(mpsc::Sender<Result<(), super::session_checkpoint::SessionCheckpointError>>),
	CheckpointWithApplicationOwner(
		super::session_checkpoint::ApplicationCheckpoint,
		mpsc::Sender<Result<(), super::session_checkpoint::SessionCheckpointError>>,
	),
	#[cfg(test)]
	#[cfg_attr(
		not(unix),
		allow(
			dead_code,
			reason = "fault injection drives the Unix-only saved-session tests"
		)
	)]
	CheckpointWithFault(
		super::session_checkpoint::CheckpointFault,
		Option<super::session_checkpoint::ApplicationCheckpoint>,
		mpsc::Sender<Result<(), super::session_checkpoint::SessionCheckpointError>>,
	),
}

#[derive(Debug)]
pub(crate) enum InternalEvent {
	Refresh,
	Reclaim,
	DrainEvents,
	HydrationCompleted(BufferId),
	ClearYankFlash,
	PathCompletion(super::input::PathCompletionResponse),
	RepoSearchReady(u64),
	RepoSearchBatch(Box<super::fuzzy_search::RepoSearchBatch>),
	NavigationResolved(Box<super::navigation::NavigationResolved>),
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
				EngineRequest::Input(_)
				| EngineRequest::Internal(_)
				| EngineRequest::Checkpoint(_)
				| EngineRequest::CheckpointWithApplicationOwner(_, _) => {
					unreachable!("sent a command request")
				}
				#[cfg(test)]
				EngineRequest::CheckpointWithFault(_, _, _) => unreachable!("sent a command request"),
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
				EngineRequest::Command(_)
				| EngineRequest::Internal(_)
				| EngineRequest::Checkpoint(_)
				| EngineRequest::CheckpointWithApplicationOwner(_, _) => {
					unreachable!("sent an input request")
				}
				#[cfg(test)]
				EngineRequest::CheckpointWithFault(_, _, _) => unreachable!("sent an input request"),
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
				EngineRequest::Command(_)
				| EngineRequest::Input(_)
				| EngineRequest::Checkpoint(_)
				| EngineRequest::CheckpointWithApplicationOwner(_, _) => {
					unreachable!("sent an internal request")
				}
				#[cfg(test)]
				EngineRequest::CheckpointWithFault(_, _, _) => unreachable!("sent an internal request"),
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

pub(crate) fn checkpoint(
	commands: &CommandSender,
) -> Result<(), super::session_checkpoint::SessionCheckpointError> {
	let (reply, response) = mpsc::channel();
	commands
		.tx
		.send(EngineRequest::Checkpoint(reply))
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?;
	response
		.recv()
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?
}

pub(crate) fn checkpoint_with_application_owner(
	commands: &CommandSender,
	checkpoint: super::session_checkpoint::ApplicationCheckpoint,
) -> Result<(), super::session_checkpoint::SessionCheckpointError> {
	let (reply, response) = mpsc::channel();
	commands
		.tx
		.send(EngineRequest::CheckpointWithApplicationOwner(
			checkpoint, reply,
		))
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?;
	response
		.recv()
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?
}

#[cfg(all(test, unix))]
pub(crate) fn checkpoint_with_fault(
	commands: &CommandSender,
	fault: super::session_checkpoint::CheckpointFault,
) -> Result<(), super::session_checkpoint::SessionCheckpointError> {
	let (reply, response) = mpsc::channel();
	commands
		.tx
		.send(EngineRequest::CheckpointWithFault(fault, None, reply))
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?;
	response
		.recv()
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?
}

#[cfg(all(test, unix))]
pub(crate) fn checkpoint_with_application_owner_and_fault(
	commands: &CommandSender,
	fault: super::session_checkpoint::CheckpointFault,
	checkpoint: super::session_checkpoint::ApplicationCheckpoint,
) -> Result<(), super::session_checkpoint::SessionCheckpointError> {
	let (reply, response) = mpsc::channel();
	commands
		.tx
		.send(EngineRequest::CheckpointWithFault(
			fault,
			Some(checkpoint),
			reply,
		))
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?;
	response
		.recv()
		.map_err(|_| super::session_checkpoint::SessionCheckpointError::EngineStopped)?
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
