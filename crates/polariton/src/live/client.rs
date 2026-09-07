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

pub(crate) fn observe(selector: &Selector) -> Result<(), String> {
	let windows =
		discover(&runtime_directory().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
	let window = select(&windows, selector)?;
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|e| e.to_string())?;
	runtime.block_on(async {
		let socket = tokio::net::UnixStream::connect(&window.socket)
			.await
			.map_err(|e| format!("cannot connect to window {}: {e}", window.window))?;
		if !socket
			.peer_cred()
			.is_ok_and(|peer| peer.uid() == super::discovery::uid())
		{
			return Err("window peer belongs to another user".into());
		}
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
		let mut output = io::stdout().lock();
		output
			.write_all(&first)
			.and_then(|()| output.flush())
			.map_err(|e| e.to_string())?;
		while let Some(line) = super::transport::read_line(&mut reader, usize::MAX)
			.await
			.map_err(|e| e.to_string())?
		{
			let event: Event = super::decode(&line)?;
			output
				.write_all(&line)
				.and_then(|()| output.flush())
				.map_err(|e| e.to_string())?;
			if matches!(event, Event::Closed { .. }) {
				return Ok(());
			}
		}
		Err("window disconnected without a closed event".into())
	})
}

#[cfg(test)]
mod tests {
	use super::*;
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
