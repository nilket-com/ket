use serde_json::{Value, json};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};
use std::process::{Child, ChildStdin, ChildStdout, Command, Stdio};
use url::Url;

#[derive(Debug, Clone)]
pub struct DefinitionPoint {
	pub path: PathBuf,
	pub line: u32,
	pub utf16_character: u32,
}

#[derive(Debug, Clone)]
pub struct OutgoingCall {
	pub point: DefinitionPoint,
	pub occurrences: usize,
}

#[derive(Debug, Clone)]
struct ServerStatus {
	health: String,
	quiescent: bool,
	message: Option<String>,
}

pub struct RustAnalyzer {
	child: Child,
	input: BufWriter<ChildStdin>,
	output: BufReader<ChildStdout>,
	next_id: u64,
	version: String,
	server_status: Option<ServerStatus>,
	finished: bool,
}

impl RustAnalyzer {
	pub fn start(executable: &Path, root: &Path) -> Result<Self, String> {
		let version_output = Command::new(executable)
			.arg("--version")
			.output()
			.map_err(|error| format!("unable to execute {}: {error}", executable.display()))?;
		if !version_output.status.success() {
			return Err(format!("{} --version failed", executable.display()));
		}
		let version = String::from_utf8_lossy(&version_output.stdout)
			.trim()
			.to_string();
		let mut child = Command::new(executable)
			.current_dir(root)
			.stdin(Stdio::piped())
			.stdout(Stdio::piped())
			.stderr(Stdio::null())
			.spawn()
			.map_err(|error| format!("unable to start {}: {error}", executable.display()))?;
		let input = BufWriter::new(
			child
				.stdin
				.take()
				.ok_or_else(|| "rust-analyzer stdin unavailable".to_string())?,
		);
		let output = BufReader::new(
			child
				.stdout
				.take()
				.ok_or_else(|| "rust-analyzer stdout unavailable".to_string())?,
		);
		let mut analyzer = Self {
			child,
			input,
			output,
			next_id: 1,
			version,
			server_status: None,
			finished: false,
		};
		let root_uri = file_uri(root)?;
		analyzer.request(
			"initialize",
			json!({
				"processId": null,
				"rootUri": root_uri,
				"workspaceFolders": [{"uri": root_uri, "name": "ket"}],
				"capabilities": {
					"window": {"workDoneProgress": true},
					"textDocument": {
						"definition": {"linkSupport": true},
						"callHierarchy": {"dynamicRegistration": false}
					},
					"experimental": {"serverStatusNotification": true}
				}
			}),
		)?;
		analyzer.notify("initialized", json!({}))?;
		analyzer.wait_until_quiescent()?;
		Ok(analyzer)
	}

	pub fn version(&self) -> &str {
		&self.version
	}

	pub fn outgoing_calls(
		&mut self,
		path: &Path,
		source: &str,
		byte_offset: usize,
	) -> Result<Vec<OutgoingCall>, String> {
		let canonical = path
			.canonicalize()
			.map_err(|error| format!("unable to canonicalize {}: {error}", path.display()))?;
		let (line, utf16_character) = byte_to_position(source, byte_offset)?;
		let prepare = self.request_with_content_retry(
			"textDocument/prepareCallHierarchy",
			json!({
			"textDocument": {"uri": file_uri(&canonical)?},
			"position": {"line": line, "character": utf16_character}
			}),
		)?;
		let items = match prepare {
			Value::Null => return Ok(Vec::new()),
			Value::Array(items) => items,
			item @ Value::Object(_) => vec![item],
			other => return Err(format!("unexpected call-hierarchy preparation: {other}")),
		};
		let mut calls = Vec::new();
		for item in items {
			let outgoing = self
				.request_with_content_retry("callHierarchy/outgoingCalls", json!({"item": item}))?;
			let Value::Array(outgoing) = outgoing else {
				if outgoing.is_null() {
					continue;
				}
				return Err(format!("unexpected outgoing-call result: {outgoing}"));
			};
			for call in outgoing {
				let target = call
					.get("to")
					.ok_or_else(|| "outgoing call omitted target".to_string())?;
				let occurrences = call
					.get("fromRanges")
					.and_then(Value::as_array)
					.map_or(1, |ranges| ranges.len().max(1));
				calls.push(OutgoingCall {
					point: parse_definition_point(target)?,
					occurrences,
				});
			}
		}
		Ok(calls)
	}

	pub fn shutdown(mut self) {
		let _ = self.request("shutdown", Value::Null);
		let _ = self.notify("exit", Value::Null);
		let _ = self.child.wait();
		self.finished = true;
	}

	fn request(&mut self, method: &str, params: Value) -> Result<Value, String> {
		let id = self.next_id;
		self.next_id += 1;
		self.send(&json!({
			"jsonrpc": "2.0",
			"id": id,
			"method": method,
			"params": params
		}))?;
		loop {
			let message = self.read_message()?;
			if message.get("id").and_then(Value::as_u64) == Some(id) {
				if let Some(error) = message.get("error") {
					return Err(format!("rust-analyzer {method} failed: {error}"));
				}
				return Ok(message.get("result").cloned().unwrap_or(Value::Null));
			}
			if message.get("method").is_some() && message.get("id").is_some() {
				self.answer_server_request(&message)?;
			} else {
				self.observe_notification(&message);
			}
		}
	}

	fn notify(&mut self, method: &str, params: Value) -> Result<(), String> {
		self.send(&json!({
			"jsonrpc": "2.0",
			"method": method,
			"params": params
		}))
	}

	fn wait_until_quiescent(&mut self) -> Result<(), String> {
		loop {
			if let Some(status) = &self.server_status
				&& status.quiescent
			{
				if status.health == "ok" {
					return Ok(());
				}
				return Err(format!(
					"rust-analyzer became quiescent with health={}{}",
					status.health,
					status
						.message
						.as_deref()
						.map_or_else(String::new, |message| format!(": {message}"))
				));
			}
			let message = self.read_message()?;
			if message.get("method").is_some() && message.get("id").is_some() {
				self.answer_server_request(&message)?;
			} else {
				self.observe_notification(&message);
			}
		}
	}

	fn observe_notification(&mut self, message: &Value) {
		if let Some(status) = parse_server_status(message) {
			self.server_status = Some(status);
		}
	}

	fn request_with_content_retry(&mut self, method: &str, params: Value) -> Result<Value, String> {
		for attempt in 0..8 {
			match self.request(method, params.clone()) {
				Ok(result) => return Ok(result),
				Err(error) if error.contains("content modified") && attempt < 7 => {
					std::thread::yield_now();
				}
				Err(error) => return Err(error),
			}
		}
		unreachable!("content-modified retry loop returns on its final attempt")
	}

	fn answer_server_request(&mut self, message: &Value) -> Result<(), String> {
		let id = message
			.get("id")
			.cloned()
			.ok_or_else(|| "rust-analyzer request omitted id".to_string())?;
		let result = match message.get("method").and_then(Value::as_str) {
			Some("workspace/configuration") => {
				let count = message
					.pointer("/params/items")
					.and_then(Value::as_array)
					.map_or(0, Vec::len);
				Value::Array(vec![Value::Null; count])
			}
			_ => Value::Null,
		};
		self.send(&json!({"jsonrpc": "2.0", "id": id, "result": result}))
	}

	fn send(&mut self, message: &Value) -> Result<(), String> {
		let body = serde_json::to_vec(message)
			.map_err(|error| format!("unable to encode LSP request: {error}"))?;
		write!(self.input, "Content-Length: {}\r\n\r\n", body.len())
			.and_then(|_| self.input.write_all(&body))
			.and_then(|_| self.input.flush())
			.map_err(|error| format!("unable to write rust-analyzer request: {error}"))
	}

	fn read_message(&mut self) -> Result<Value, String> {
		let mut content_length = None;
		loop {
			let mut header = String::new();
			let read = self
				.output
				.read_line(&mut header)
				.map_err(|error| format!("unable to read rust-analyzer header: {error}"))?;
			if read == 0 {
				return Err("rust-analyzer closed its output".to_string());
			}
			let header = header.trim_end_matches(['\r', '\n']);
			if header.is_empty() {
				break;
			}
			if let Some(value) = header.strip_prefix("Content-Length:") {
				content_length = value.trim().parse::<usize>().ok();
			}
		}
		let length =
			content_length.ok_or_else(|| "LSP message omitted Content-Length".to_string())?;
		let mut body = vec![0; length];
		self.output
			.read_exact(&mut body)
			.map_err(|error| format!("unable to read rust-analyzer message: {error}"))?;
		serde_json::from_slice(&body)
			.map_err(|error| format!("invalid rust-analyzer JSON response: {error}"))
	}
}

impl Drop for RustAnalyzer {
	fn drop(&mut self) {
		if !self.finished {
			let _ = self.child.kill();
			let _ = self.child.wait();
		}
	}
}

fn file_uri(path: &Path) -> Result<String, String> {
	Url::from_file_path(path)
		.map(|url| url.to_string())
		.map_err(|_| format!("cannot express {} as a file URI", path.display()))
}

fn parse_server_status(message: &Value) -> Option<ServerStatus> {
	if message.get("method").and_then(Value::as_str) != Some("experimental/serverStatus") {
		return None;
	}
	Some(ServerStatus {
		health: message.pointer("/params/health")?.as_str()?.to_string(),
		quiescent: message.pointer("/params/quiescent")?.as_bool()?,
		message: message
			.pointer("/params/message")
			.and_then(Value::as_str)
			.map(str::to_string),
	})
}

fn parse_definition_point(value: &Value) -> Result<DefinitionPoint, String> {
	let uri = value
		.get("targetUri")
		.or_else(|| value.get("uri"))
		.and_then(Value::as_str)
		.ok_or_else(|| "definition target omitted URI".to_string())?;
	let range = value
		.get("targetSelectionRange")
		.or_else(|| value.get("selectionRange"))
		.or_else(|| value.get("range"))
		.ok_or_else(|| "definition target omitted range".to_string())?;
	let line = range
		.pointer("/start/line")
		.and_then(Value::as_u64)
		.and_then(|line| u32::try_from(line).ok())
		.ok_or_else(|| "definition target omitted start line".to_string())?;
	let utf16_character = range
		.pointer("/start/character")
		.and_then(Value::as_u64)
		.and_then(|character| u32::try_from(character).ok())
		.ok_or_else(|| "definition target omitted start character".to_string())?;
	let path = Url::parse(uri)
		.map_err(|error| format!("invalid definition URI: {error}"))?
		.to_file_path()
		.map_err(|_| "definition target is not a file URI".to_string())?;
	Ok(DefinitionPoint {
		path,
		line,
		utf16_character,
	})
}

pub fn byte_to_position(source: &str, byte_offset: usize) -> Result<(u32, u32), String> {
	if byte_offset > source.len() || !source.is_char_boundary(byte_offset) {
		return Err("call-site byte offset is not a UTF-8 boundary".to_string());
	}
	let prefix = &source[..byte_offset];
	let line = u32::try_from(prefix.bytes().filter(|byte| *byte == b'\n').count())
		.map_err(|_| "line number exceeds LSP range".to_string())?;
	let line_start = prefix.rfind('\n').map_or(0, |index| index + 1);
	let utf16_character = u32::try_from(source[line_start..byte_offset].encode_utf16().count())
		.map_err(|_| "column exceeds LSP range".to_string())?;
	Ok((line, utf16_character))
}

pub fn position_to_byte(source: &str, line: u32, utf16_character: u32) -> Option<usize> {
	let mut line_start = 0usize;
	for _ in 0..line {
		let next = source[line_start..].find('\n')?;
		line_start += next + 1;
	}
	let line_end = source[line_start..]
		.find('\n')
		.map_or(source.len(), |offset| line_start + offset);
	let mut utf16 = 0u32;
	for (offset, character) in source[line_start..line_end].char_indices() {
		if utf16 == utf16_character {
			return Some(line_start + offset);
		}
		utf16 = utf16.checked_add(character.len_utf16() as u32)?;
		if utf16 > utf16_character {
			return None;
		}
	}
	(utf16 == utf16_character).then_some(line_end)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn byte_and_lsp_positions_round_trip_unicode() {
		let source = "a😀b\n中x";
		for offset in source
			.char_indices()
			.map(|(offset, _)| offset)
			.chain(std::iter::once(source.len()))
		{
			let (line, column) = byte_to_position(source, offset).unwrap();
			assert_eq!(position_to_byte(source, line, column), Some(offset));
		}
	}

	#[test]
	fn definition_parser_accepts_locations_links_and_call_items() {
		let location = json!({"uri": "file:///tmp/a.rs", "range": {"start": {"line": 1, "character": 2}, "end": {"line": 1, "character": 3}}});
		let link = json!({"targetUri": "file:///tmp/b.rs", "targetSelectionRange": {"start": {"line": 4, "character": 5}, "end": {"line": 4, "character": 6}}});
		let item = json!({"uri": "file:///tmp/c.rs", "selectionRange": {"start": {"line": 7, "character": 8}, "end": {"line": 7, "character": 9}}});
		assert_eq!(parse_definition_point(&location).unwrap().line, 1);
		assert_eq!(parse_definition_point(&link).unwrap().utf16_character, 5);
		assert_eq!(parse_definition_point(&item).unwrap().line, 7);
	}

	#[test]
	fn server_status_parser_requires_the_advertised_health_and_quiescence_shape() {
		let status = parse_server_status(&json!({
			"method": "experimental/serverStatus",
			"params": {"health": "ok", "quiescent": true, "message": "ready"}
		}))
		.expect("valid server status");
		assert_eq!(status.health, "ok");
		assert!(status.quiescent);
		assert_eq!(status.message.as_deref(), Some("ready"));
		assert!(
			parse_server_status(&json!({
				"method": "experimental/serverStatus",
				"params": {"health": "ok"}
			}))
			.is_none()
		);
		assert!(
			parse_server_status(&json!({
				"method": "window/logMessage",
				"params": {"health": "ok", "quiescent": true}
			}))
			.is_none()
		);
	}
}
