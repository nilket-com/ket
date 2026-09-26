//! Each invocation owns its files and child; cancellation waits for reaping.
use super::{BridgeError, BridgeErrorKind, Dispatch, DriveArgs};
use crate::driver;
use serde_json::Value;
use std::{path::Path, process::Stdio};
use tokio::io::AsyncReadExt;
use tokio_util::sync::CancellationToken;

pub(super) async fn run(
	executable: &Path,
	args: DriveArgs,
	cancel: &CancellationToken,
	stop: &CancellationToken,
	tasks: &tokio_util::task::TaskTracker,
) -> Result<Value, BridgeError> {
	let prepare = tokio::task::spawn_blocking(move || -> Result<_, String> {
		let directory = tempfile::tempdir().map_err(|e| e.to_string())?;
		let commands = directory.path().join("commands.json");
		let receipt = directory.path().join("receipt.json");
		let input = serde_json::to_vec(&driver::Commands {
			commands: args.commands,
		})
		.map_err(|e| e.to_string())?;
		std::fs::write(&commands, input).map_err(|e| e.to_string())?;
		Ok((directory, commands, receipt))
	});
	let (directory, commands, receipt) = tokio::select! { biased;
		_ = cancel.cancelled() => return Err(BridgeError::not_sent("drive cancelled before starting")),
		_ = stop.cancelled() => return Err(BridgeError::not_sent("server stopped before starting driver")),
		result = tokio::time::timeout(super::PRE_SEND, prepare) => result
			.map_err(|_| BridgeError::not_sent("driver file preparation timed out"))?
			.map_err(|e| BridgeError::not_sent(e.to_string()))?
			.map_err(BridgeError::not_sent)?,
	};
	if cancel.is_cancelled() || stop.is_cancelled() {
		return Err(BridgeError::not_sent("drive cancelled before starting"));
	}
	let mut command = tokio::process::Command::new(executable);
	command
		.arg("drive")
		.arg(args.source)
		.arg("--commands")
		.arg(commands)
		.arg("--receipt")
		.arg(&receipt)
		.arg("--execute")
		.arg(match args.execution {
			driver::ExecutionMode::OnOpen => "on-open",
			driver::ExecutionMode::ByCommand => "by-command",
		})
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::piped())
		.kill_on_drop(true);
	let (started_tx, started_rx) = tokio::sync::oneshot::channel();
	let (done_tx, done_rx) = tokio::sync::oneshot::channel();
	let abandoned = CancellationToken::new();
	let launch_cancel = abandoned.clone();
	let guard = abandoned.drop_guard();
	let cancel = cancel.clone();
	let stop = stop.clone();
	tasks.spawn(async move {
		// The supervisor always owns the child, including a spawn which returns
		// after startup timed out. Only acknowledgements cross the channels.
		let spawn_cancel = launch_cancel.clone();
		let request_cancel = cancel.clone();
		let server_stop = stop.clone();
		let child = tokio::task::spawn_blocking(move || {
			if spawn_cancel.is_cancelled()
				|| request_cancel.is_cancelled()
				|| server_stop.is_cancelled()
			{
				return Err(std::io::Error::other("driver start abandoned"));
			}
			command.spawn()
		})
		.await
		.unwrap_or_else(|error| Err(std::io::Error::other(error)));
		let _ = started_tx.send(());
		let result = match child {
			Ok(child) => supervise(child, &receipt, &cancel, &stop, &launch_cancel).await,
			Err(error) => Err(BridgeError::not_sent(format!(
				"cannot start driver: {error}"
			))),
		};
		let _ = done_tx.send(result);
		drop(directory);
	});
	if tokio::time::timeout(super::PRE_SEND, started_rx)
		.await
		.is_err()
	{
		return Err(BridgeError {
			kind: BridgeErrorKind::BridgeError,
			dispatch: Dispatch::Unknown,
			reason:
				"driver startup timed out; it may have started and will be reaped, never retried"
					.into(),
			exit_status: None,
			stderr_tail: None,
		});
	}
	let result = done_rx.await.unwrap_or_else(|_| {
		Err(BridgeError {
			kind: BridgeErrorKind::BridgeError,
			dispatch: Dispatch::Unknown,
			reason: "driver supervisor failed; effects may have occurred".into(),
			exit_status: None,
			stderr_tail: None,
		})
	});
	drop(guard);
	result
}

async fn supervise(
	mut child: tokio::process::Child,
	receipt: &Path,
	cancel: &CancellationToken,
	stop: &CancellationToken,
	abandoned: &CancellationToken,
) -> Result<Value, BridgeError> {
	let mut stderr = child.stderr.take().expect("piped stderr");
	let tail = std::sync::Arc::new(std::sync::Mutex::new(Vec::new()));
	let captured = tail.clone();
	let drain = tokio::spawn(async move {
		let mut bytes = [0; 4096];
		while let Ok(count) = stderr.read(&mut bytes).await {
			if count == 0 {
				break;
			}
			let mut tail = captured.lock().unwrap();
			tail.extend_from_slice(&bytes[..count]);
			let excess = tail.len().saturating_sub(8192);
			tail.drain(..excess);
		}
	});
	let status = tokio::select! { biased;
		_ = cancel.cancelled() => { let _ = child.start_kill(); child.wait().await },
		_ = abandoned.cancelled() => { let _ = child.start_kill(); child.wait().await },
		_ = stop.cancelled() => { let _ = child.start_kill(); child.wait().await },
		status = child.wait() => status,
	};
	// A descendant may inherit stderr. Never let it hold the MCP call open.
	let mut drain = drain;
	if tokio::time::timeout(std::time::Duration::from_millis(100), &mut drain)
		.await
		.is_err()
	{
		drain.abort();
		let _ = drain.await;
	}
	let document = tokio::fs::read(&receipt).await;
	let result = document.map_err(|e| e.to_string()).and_then(|bytes| {
		serde_json::from_slice::<driver::Receipt>(&bytes).map_err(|e| e.to_string())?;
		serde_json::from_slice::<Value>(&bytes).map_err(|e| e.to_string())
	});
	result.map_err(|reason| BridgeError {
		kind: BridgeErrorKind::BridgeError,
		dispatch: Dispatch::Unknown,
		reason: format!("driver left no valid receipt: {reason}; effects may have occurred"),
		exit_status: Some(match status {
			Ok(status) => status.to_string(),
			Err(error) => error.to_string(),
		}),
		stderr_tail: Some(String::from_utf8_lossy(&tail.lock().unwrap()).into_owned()),
	})
}
