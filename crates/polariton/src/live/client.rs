//! CLI clients own no application. They connect to a selected live window.

use super::{
	Event,
	discovery::{Selector, discover, runtime_directory, select},
};
use std::io::{self, Write};

pub(crate) fn selector(args: impl Iterator<Item = String>) -> Result<Selector, String> {
	let mut args = args.peekable();
	let mut selected = None;
	while let Some(arg) = args.next() {
		if selected.is_some() {
			return Err("name exactly one window, --session id, or --source path".into());
		}
		selected = Some(match arg.as_str() {
			"--session" => Selector::Session(args.next().ok_or("--session requires an id")?),
			"--source" => Selector::Source(args.next().ok_or("--source requires a path")?.into()),
			_ if !arg.starts_with('-') => Selector::Window(arg),
			_ => return Err(format!("unknown window option {arg:?}")),
		});
	}
	selected.ok_or_else(|| "name a window, --session id, or --source path".into())
}

pub(crate) fn windows() -> Result<(), String> {
	let windows =
		discover(&runtime_directory().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
	let mut output = io::stdout().lock();
	writeln!(output, "WINDOW\tSESSION\tAGENTS\tSOURCES").map_err(|e| e.to_string())?;
	for window in windows {
		writeln!(
			output,
			"{}\t{}\t{}\t{}",
			window.window,
			window.session.as_deref().unwrap_or("—"),
			if window.accepting { "on" } else { "off" },
			window
				.sources
				.iter()
				.map(|p| p.display().to_string())
				.collect::<Vec<_>>()
				.join(", ")
		)
		.map_err(|e| e.to_string())?;
	}
	Ok(())
}

/// The client end of the platform transport.
#[cfg(unix)]
type ClientStream = tokio::net::UnixStream;
#[cfg(windows)]
type ClientStream = tokio::net::windows::named_pipe::NamedPipeClient;

/// ADR 0166 §4: reach the advertised process, not merely a same-user peer.
/// Unix checks the peer's uid; Windows checks the pipe server's pid and
/// start token against the advertisement before anything is sent.
async fn open_stream(window: &super::discovery::Advertisement) -> Result<ClientStream, String> {
	#[cfg(unix)]
	{
		let socket = tokio::net::UnixStream::connect(&window.socket)
			.await
			.map_err(|e| format!("cannot connect to window {}: {e}", window.window))?;
		// SAFETY: geteuid has no preconditions and cannot fail.
		let uid = unsafe { libc::geteuid() };
		if !socket.peer_cred().is_ok_and(|peer| peer.uid() == uid) {
			return Err("window peer belongs to another user".into());
		}
		Ok(socket)
	}
	#[cfg(windows)]
	{
		use std::os::windows::io::AsRawHandle;
		let pipe = windows_open(window.socket.as_os_str())
			.await
			.map_err(|e| format!("cannot connect to window {}: {e}", window.window))?;
		let mut pid = 0_u32;
		// SAFETY: the handle is a connected client end owned by `pipe`.
		if unsafe {
			windows_sys::Win32::System::Pipes::GetNamedPipeServerProcessId(
				pipe.as_raw_handle() as _,
				&mut pid,
			)
		} == 0
		{
			return Err("cannot identify the window's pipe server process".into());
		}
		if pid != window.pid
			|| super::discovery::process_start(pid).as_deref() != Some(&window.process_start)
		{
			return Err("pipe server is not the advertised window process".into());
		}
		Ok(pipe)
	}
}

async fn connect(
	selector: &Selector,
) -> Result<(tokio::io::BufReader<ClientStream>, Event), String> {
	let windows =
		discover(&runtime_directory().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
	let window = select(&windows, selector)?;
	let socket = open_stream(&window).await?;
	let mut reader = tokio::io::BufReader::new(socket);
	let first = tokio::time::timeout(
		std::time::Duration::from_secs(5),
		super::transport::read_line(&mut reader, usize::MAX),
	)
	.await
	.map_err(|_| "window handshake timed out")?
	.map_err(|e| e.to_string())?
	.ok_or("window disconnected before its identity arrived")?;
	let welcome: Event = super::decode(&first)?;
	match &welcome {
		Event::Welcome { window: actual, .. }
			if actual.window == window.window
				&& actual.pid == window.pid
				&& actual.process_start == window.process_start => {}
		_ => return Err("window identity changed during connection; discover again".into()),
	}
	Ok((reader, welcome))
}

fn runtime() -> Result<tokio::runtime::Runtime, String> {
	tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|e| e.to_string())
}

fn emit(output: &mut impl Write, event: &Event) -> Result<(), String> {
	output
		.write_all(&super::encode(event)?)
		.and_then(|()| output.flush())
		.map_err(|e| e.to_string())
}

pub(crate) fn observe(selector: &Selector) -> Result<(), String> {
	runtime()?.block_on(async {
		let (mut reader, welcome) = connect(selector).await?;
		let mut output = io::stdout().lock();
		emit(&mut output, &welcome)?;
		while let Some(line) = super::transport::read_line(&mut reader, usize::MAX)
			.await
			.map_err(|e| e.to_string())?
		{
			let event: Event = super::decode(&line)?;
			emit(&mut output, &event)?;
			if matches!(event, Event::Closed { .. }) {
				return Ok(());
			}
		}
		Err("window disconnected without a closed event".into())
	})
}

pub(crate) fn act_arguments(
	args: impl Iterator<Item = String>,
) -> Result<(Selector, std::path::PathBuf), String> {
	let mut remaining = Vec::new();
	let mut file = None;
	let mut args = args.peekable();
	while let Some(arg) = args.next() {
		if arg == "--commands" {
			if file.is_some() {
				return Err("--commands may occur only once".into());
			}
			file = Some(std::path::PathBuf::from(
				args.next().ok_or("--commands requires a file")?,
			));
		} else if arg == "--source" || arg == "--session" {
			remaining.push(arg);
			remaining.push(args.next().ok_or("window selector requires a value")?);
		} else {
			remaining.push(arg);
		}
	}
	Ok((
		selector(remaining.into_iter())?,
		file.ok_or("act requires --commands <file>")?,
	))
}

pub(crate) fn act(selector: &Selector, commands: &std::path::Path) -> Result<(), String> {
	// Parse and enforce wire sizes before the first connection is opened.
	let script = crate::driver::read_commands(commands)?;
	let lines = script
		.commands
		.iter()
		.enumerate()
		.map(|(i, command)| {
			let bytes = super::encode(&super::Request {
				id: i as u64 + 1,
				command: command.clone(),
			})?;
			if bytes.len() > super::MAX_LINE {
				Err(format!(
					"command {} exceeds the 64 KiB request limit",
					i + 1
				))
			} else {
				Ok(bytes)
			}
		})
		.collect::<Result<Vec<_>, String>>()?;
	runtime()?.block_on(async {
		let (mut reader, welcome) = connect(selector).await?;
		let mut output = io::stdout().lock();
		emit(&mut output, &welcome)?;
		drive_connection(&mut reader, &lines, |event| emit(&mut output, event)).await
	})
}

async fn drive_connection<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin>(
	reader: &mut tokio::io::BufReader<S>,
	lines: &[Vec<u8>],
	mut output: impl FnMut(&Event) -> Result<(), String>,
) -> Result<(), String> {
	use tokio::io::AsyncWriteExt;
	let mut failed = false;
	for (i, bytes) in lines.iter().enumerate() {
		let id = i as u64 + 1;
		let mut applied = false;
		let result: Result<(), String> = async {
			reader
				.get_mut()
				.write_all(bytes)
				.await
				.map_err(|e| e.to_string())?;
			loop {
				let line = super::transport::read_line(reader, usize::MAX)
					.await
					.map_err(|e| e.to_string())?
					.ok_or("window disconnected")?;
				let event: Event = super::decode(&line)?;
				output(&event)?;
				if matches!(event, Event::Closed { .. }) {
					return Err("window closed".into());
				}
				if let Some(answer_id) = event.id() {
					if answer_id != id {
						return Err("answer named a different request".into());
					}
					if matches!(event, Event::Applied { .. }) {
						applied = true;
					}
					if event.is_final() {
						failed |= matches!(
							event,
							Event::Refused { .. } | Event::Failed { .. } | Event::Superseded { .. }
						);
						return Ok(());
					}
				}
			}
		}
		.await;
		if let Err(reason) = result {
			output(&Event::Unknown {
				id,
				applied,
				reason,
			})?;
			return Err(if applied {
				"request applied; completion unknown; not replayed"
			} else {
				"request outcome unknown; not replayed"
			}
			.into());
		}
	}
	if failed {
		Err("one or more commands were refused, failed, or superseded; see receipts".into())
	} else {
		Ok(())
	}
}

/// Open a named pipe, waiting briefly while the server is between instances
/// (`ERROR_PIPE_BUSY`) or has not created the next one yet (`NotFound`),
/// which are the two transient states a live server passes through.
#[cfg(windows)]
pub(crate) async fn windows_open(
	name: &std::ffi::OsStr,
) -> std::io::Result<tokio::net::windows::named_pipe::NamedPipeClient> {
	const ERROR_PIPE_BUSY: i32 = 231;
	let deadline = std::time::Instant::now() + std::time::Duration::from_secs(2);
	loop {
		match tokio::net::windows::named_pipe::ClientOptions::new().open(name) {
			Ok(pipe) => return Ok(pipe),
			Err(error)
				if (error.raw_os_error() == Some(ERROR_PIPE_BUSY)
					|| error.kind() == std::io::ErrorKind::NotFound)
					&& std::time::Instant::now() < deadline =>
			{
				tokio::time::sleep(std::time::Duration::from_millis(20)).await;
			}
			Err(error) => return Err(error),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::driver::{Command, CommandReceipt, FigureRef};
	use tokio::io::{AsyncWriteExt, BufReader};

	/// ADR 0166 §4, acceptance 4: a client refuses a pipe whose server is
	/// not the advertised process, before sending a byte; the advertised
	/// process is accepted.
	#[cfg(windows)]
	#[tokio::test]
	async fn adr_0166_client_refuses_a_pipe_whose_server_is_not_the_advertised_process() {
		use super::super::discovery::{Advertisement, endpoint_for, process_start};
		use tokio::net::windows::named_pipe::ServerOptions;
		let window = uuid::Uuid::new_v4().simple().to_string();
		let socket = endpoint_for(std::path::Path::new(""), &window);
		let pid = std::process::id();
		let mut advertised = Advertisement {
			window: window.clone(),
			socket: socket.clone(),
			pid,
			process_start: process_start(pid).unwrap(),
			session: None,
			sources: Vec::new(),
			started_at: 0,
			accepting: true,
		};
		let mut server = ServerOptions::new()
			.first_pipe_instance(true)
			.create(&socket)
			.unwrap();
		const REFUSAL: &str = "pipe server is not the advertised window process";
		// Another incarnation of this pid.
		advertised.process_start = "another-incarnation".into();
		let (attempt, _) = tokio::join!(open_stream(&advertised), server.connect());
		assert_eq!(attempt.err().as_deref(), Some(REFUSAL));
		server = ServerOptions::new().create(&socket).unwrap();
		// Another pid entirely.
		advertised.process_start = process_start(pid).unwrap();
		advertised.pid = pid.wrapping_add(1);
		let (attempt, _) = tokio::join!(open_stream(&advertised), server.connect());
		assert_eq!(attempt.err().as_deref(), Some(REFUSAL));
		server = ServerOptions::new().create(&socket).unwrap();
		// The advertised process itself.
		advertised.pid = pid;
		let (attempt, connected) = tokio::join!(open_stream(&advertised), server.connect());
		assert!(attempt.is_ok() && connected.is_ok());
	}

	#[tokio::test]
	async fn product_0160_script_waits_for_final_answers_and_does_not_replay_unknowns() {
		for applied_before_disconnect in [false, true] {
			let (client, server) = tokio::io::duplex(65536);
			let commands = [
				Command::Resize {
					figure: FigureRef::View("plot".into()),
					width: 360.0,
					height: 360.0,
				},
				Command::Describe {},
			];
			let lines = commands
				.iter()
				.enumerate()
				.map(|(i, command)| {
					super::super::encode(&super::super::Request {
						id: i as u64 + 1,
						command: command.clone(),
					})
					.unwrap()
				})
				.collect::<Vec<_>>();
			let first_command = commands[0].clone();
			let server = tokio::spawn(async move {
				let mut server = BufReader::new(server);
				let first = super::super::transport::read_line(&mut server, super::super::MAX_LINE)
					.await
					.unwrap()
					.unwrap();
				assert_eq!(
					super::super::decode::<super::super::Request>(&first)
						.unwrap()
						.id,
					1
				);
				if applied_before_disconnect {
					server
						.get_mut()
						.write_all(
							&super::super::encode(&Event::Applied {
								id: 1,
								terminal: false,
								receipt: CommandReceipt::new(&first_command),
							})
							.unwrap(),
						)
						.await
						.unwrap();
				}
				// No second command may arrive after a nonterminal applied.
				assert!(
					tokio::time::timeout(
						std::time::Duration::from_millis(30),
						super::super::transport::read_line(&mut server, super::super::MAX_LINE)
					)
					.await
					.is_err()
				);
			});
			let mut events = Vec::new();
			let result = drive_connection(&mut BufReader::new(client), &lines, |event| {
				events.push(event.clone());
				Ok(())
			})
			.await;
			server.await.unwrap();
			assert!(result.is_err());
			assert!(
				matches!(events.last().unwrap(), Event::Unknown { id: 1, applied, .. } if *applied == applied_before_disconnect)
			);
			assert!(!events.iter().any(|event| event.id() == Some(2)));
		}
	}

	#[tokio::test]
	async fn product_0160_script_advances_after_completion_or_satisfied_resize() {
		for immediate in [false, true] {
			let (client, server) = tokio::io::duplex(65536);
			let commands = [
				Command::Resize {
					figure: FigureRef::View("plot".into()),
					width: 360.0,
					height: 360.0,
				},
				Command::Describe {},
			];
			let lines = commands
				.iter()
				.enumerate()
				.map(|(i, command)| {
					super::super::encode(&super::super::Request {
						id: i as u64 + 1,
						command: command.clone(),
					})
					.unwrap()
				})
				.collect::<Vec<_>>();
			let server = tokio::spawn(async move {
				let mut server = BufReader::new(server);
				for (i, command) in commands.iter().enumerate() {
					let line =
						super::super::transport::read_line(&mut server, super::super::MAX_LINE)
							.await
							.unwrap()
							.unwrap();
					let request: super::super::Request = super::super::decode(&line).unwrap();
					assert_eq!(request.id, i as u64 + 1);
					let receipt = CommandReceipt::new(command);
					server
						.get_mut()
						.write_all(
							&super::super::encode(&Event::Applied {
								id: request.id,
								terminal: immediate || i > 0,
								receipt: receipt.clone(),
							})
							.unwrap(),
						)
						.await
						.unwrap();
					if i == 0 && !immediate {
						assert!(
							tokio::time::timeout(
								std::time::Duration::from_millis(30),
								super::super::transport::read_line(
									&mut server,
									super::super::MAX_LINE
								)
							)
							.await
							.is_err()
						);
						server
							.get_mut()
							.write_all(
								&super::super::encode(&Event::Completed {
									id: request.id,
									receipt,
								})
								.unwrap(),
							)
							.await
							.unwrap();
					}
				}
			});
			drive_connection(&mut BufReader::new(client), &lines, |_| Ok(()))
				.await
				.unwrap();
			server.await.unwrap();
		}
	}

	#[test]
	fn product_0160_act_accepts_one_selector_and_one_script() {
		let parse = |args: &[&str]| act_arguments(args.iter().map(|s| s.to_string()));
		assert_eq!(
			parse(&["--commands", "commands.json", "--source", "showcase.rn"]).unwrap(),
			(
				Selector::Source("showcase.rn".into()),
				"commands.json".into()
			)
		);
		assert!(parse(&["window"]).is_err());
		assert!(parse(&["window", "--commands", "a", "--commands", "b"]).is_err());
		assert!(parse(&["window", "--commands", "a", "second-window"]).is_err());
	}
	#[test]
	fn product_0160_window_selection_is_explicit() {
		let parse = |args: &[&str]| selector(args.iter().map(|s| s.to_string()));
		assert_eq!(parse(&["abc"]).unwrap(), Selector::Window("abc".into()));
		assert_eq!(
			parse(&["--session", "s"]).unwrap(),
			Selector::Session("s".into())
		);
		assert_eq!(
			parse(&["--source", "file.rn"]).unwrap(),
			Selector::Source("file.rn".into())
		);
		for args in [
			vec![],
			vec!["--source"],
			vec!["a", "b"],
			vec!["--session", "s", "--source", "f"],
		] {
			assert!(parse(&args).is_err());
		}
	}
}
