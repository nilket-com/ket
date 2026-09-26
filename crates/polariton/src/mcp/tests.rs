use super::*;

#[test]
fn every_command_has_a_valid_schema() {
	let tools = schema::tools();
	assert_eq!(tools.len(), schema::command_schemas().len() + 2);
	assert_eq!(schema::validators(&tools).len(), tools.len());
	for tool in tools {
		jsonschema::validator_for(&tool["outputSchema"]).unwrap();
	}
}

#[test]
fn live_output_schema_accepts_only_settling_events_and_bridge_errors() {
	let tools = schema::tools();
	let tool = tools
		.iter()
		.find(|tool| tool["name"] == "describe")
		.unwrap();
	let validator = jsonschema::validator_for(&tool["outputSchema"]).unwrap();
	let receipt = driver::CommandReceipt::new(&driver::Command::Describe {});
	for terminal in [false, true] {
		let event = live::Event::Applied {
			id: 1,
			terminal,
			receipt: receipt.clone(),
		};
		assert_eq!(
			validator.is_valid(&serde_json::to_value(event).unwrap()),
			terminal
		);
	}
	for event in [
		live::Event::Changed {
			figures: Vec::new(),
		},
		live::Event::Closed {
			reason: "closed".into(),
		},
	] {
		assert!(!validator.is_valid(&serde_json::to_value(event).unwrap()));
	}
	assert!(validator.is_valid(&json!({"event":"unknown","id":1,"applied":true,"reason":"EOF"})));
	assert!(
		validator.is_valid(&json!({"event":"refused","id":1,"reason":"refused","receipt":null}))
	);
	for tool in tools {
		for branch in tool["outputSchema"]["anyOf"].as_array().unwrap() {
			assert!(branch.get("$schema").is_none());
			assert!(branch.get("title").is_none());
		}
	}
}

struct Harness {
	bridge: Arc<Bridge>,
	writer: tokio::io::WriteHalf<tokio::io::DuplexStream>,
	reader: tokio::io::BufReader<tokio::io::ReadHalf<tokio::io::DuplexStream>>,
	server: tokio::task::JoinHandle<()>,
}
impl Harness {
	fn new() -> Self {
		Self::with_executable(std::env::current_exe().unwrap())
	}
	fn with_executable(executable: PathBuf) -> Self {
		let bridge = Bridge::new(live::discovery::test_runtime_root("mcp"), executable);
		let (client, server) = tokio::io::duplex(1024 * 1024);
		let (read, write) = tokio::io::split(server);
		let service = rmcp::service::serve_directly(
			McpService(bridge.clone()),
			transport::Stdio::new(read, write, bridge.stop.clone()),
			None,
		);
		let cleanup = bridge.clone();
		let server = tokio::spawn(async move {
			let _ = service.waiting().await;
			cleanup.stop.cancel();
			cleanup.tasks.close();
			cleanup.tasks.wait().await;
		});
		let (reader, writer) = tokio::io::split(client);
		Self {
			bridge,
			writer,
			reader: tokio::io::BufReader::new(reader),
			server,
		}
	}
	async fn send(&mut self, value: Value) {
		use tokio::io::AsyncWriteExt;
		let mut bytes = serde_json::to_vec(&value).unwrap();
		bytes.push(b'\n');
		self.writer.write_all(&bytes).await.unwrap();
	}
	async fn receive(&mut self) -> Value {
		use tokio::io::AsyncBufReadExt;
		let mut line = String::new();
		assert!(
			tokio::time::timeout(Duration::from_secs(10), self.reader.read_line(&mut line))
				.await
				.unwrap()
				.unwrap() > 0
		);
		serde_json::from_str(&line).unwrap()
	}
	async fn request(&mut self, id: u64, method: &str, params: Value) -> Value {
		self.send(json!({"jsonrpc":"2.0","id":id,"method":method,"params":params}))
			.await;
		loop {
			let reply = self.receive().await;
			if reply.get("id") == Some(&json!(id)) {
				return reply;
			}
		}
	}
	async fn initialize(&mut self, version: &str) {
		let reply = self
			.request(
				1,
				"initialize",
				json!({"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"test","version":"1"}}),
			)
			.await;
		assert_eq!(reply["result"]["protocolVersion"], "2025-06-18", "{reply}");
		self.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
			.await;
		assert_eq!(
			self.request(2, "ping", json!({})).await["result"],
			json!({})
		);
	}
	async fn close(self) {
		let Self {
			bridge,
			reader,
			writer,
			server,
		} = self;
		drop(reader);
		drop(writer);
		tokio::time::timeout(Duration::from_secs(5), server)
			.await
			.unwrap()
			.unwrap();
		let _ = std::fs::remove_dir_all(&bridge.root);
	}
}

#[tokio::test]
async fn lifecycle_negotiates_and_gates_without_replying_to_notifications() {
	for version in ["2025-06-18", "2026-07-28"] {
		let mut h = Harness::new();
		assert_eq!(h.request(1, "ping", json!({})).await["result"], json!({}));
		h.send(json!({"jsonrpc":"2.0","method":"notifications/initialized"}))
			.await;
		assert!(
			h.request(2, "tools/list", json!({}))
				.await
				.get("error")
				.is_some()
		);
		assert!(
			h.request(3, "resources/list", json!({}))
				.await
				.get("error")
				.is_some()
		);
		assert!(
			h.request(4, "initialize", json!({}))
				.await
				.get("error")
				.is_some()
		);
		h.initialize(version).await;
		assert_eq!(
			h.request(5, "tools/list", json!({})).await["result"]["tools"]
				.as_array()
				.unwrap()
				.len(),
			17
		);
		assert!(
			h.request(
				6,
				"initialize",
				json!({"protocolVersion":version,"capabilities":{},"clientInfo":{"name":"test","version":"1"}})
			)
			.await
			.get("error")
			.is_some()
		);
		h.close().await;
	}
}

#[test]
fn schemas_and_wire_decode_agree_including_defaults() {
	let samples = json!({
	 "describe":{},"describe_notebook":{},"open_data":{"path":"rates.csv"},
	 "replace_cell":{"target":{"buffer":"1","generation":"2","cell":0},"source":"let x = 1;"},
	 "run_cell":{"target":{"buffer":"1","generation":"2","cell":0}},
	 "hide":{"figure":{"view":"rates"},"series":0},"show":{"figure":{"incarnation":"42"},"series":1},
	 "isolate":{"figure":{"view":"rates"},"series":2},"restore":{"figure":{"view":"rates"}},
	 "resize":{"figure":{"view":"rates"},"width":900,"height":600},"publish":{"directory":"out"},
	 "connect":{"adapter":"sqlite"},"disconnect":{"generation":3},"connection_status":{},"run_source":{}
	});
	let schemas = schema::command_schemas();
	assert_eq!(schemas.len(), samples.as_object().unwrap().len());
	for (name, sample) in samples.as_object().unwrap() {
		let validator = jsonschema::validator_for(&schemas[name]).unwrap();
		let mut cases = vec![sample.clone(), json!(null), json!([]), json!({})];
		let mut extra = sample.clone();
		extra["unexpected"] = json!(true);
		cases.push(extra);
		for key in sample.as_object().unwrap().keys() {
			let mut missing = sample.clone();
			missing.as_object_mut().unwrap().remove(key);
			cases.push(missing);
			let mut wrong = sample.clone();
			wrong[key] = json!([]);
			cases.push(wrong);
		}
		for args in cases {
			let direct = serde_json::from_value::<driver::Command>(json!({name:args}));
			assert_eq!(validator.is_valid(&args), direct.is_ok(), "{name}: {args}");
			if let Ok(direct) = direct {
				assert_eq!(schema::decode_command(name, args).unwrap().1, direct);
			}
		}
	}
}

#[tokio::test]
async fn absent_window_is_a_structured_bridge_error_and_bad_arguments_are_protocol_errors() {
	let mut h = Harness::new();
	h.initialize("2025-06-18").await;
	let reply = h
		.request(3, "tools/call", json!({"name":"run_source","arguments":{}}))
		.await;
	assert_eq!(reply["result"]["structuredContent"]["kind"], "bridge_error");
	assert_eq!(reply["result"]["structuredContent"]["dispatch"], "not_sent");
	assert_eq!(reply["result"]["isError"], true);
	assert_eq!(
		serde_json::from_str::<Value>(reply["result"]["content"][1]["text"].as_str().unwrap())
			.unwrap(),
		reply["result"]["structuredContent"]
	);
	for args in [
		json!({"name":"no_such_tool"}),
		json!({"name":"resize","arguments":{}}),
		json!({"name":"run_source","arguments":null}),
	] {
		assert!(
			h.request(4, "tools/call", args)
				.await
				.get("error")
				.is_some()
		);
	}
	h.close().await;
}

#[cfg(unix)]
mod unix {
	use super::*;
	use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
	pub(super) fn window(h: &Harness) -> (live::discovery::Window, tokio::net::UnixListener) {
		let mut window =
			live::discovery::Window::create(h.bridge.root.clone(), None, vec![]).unwrap();
		let listener = tokio::net::UnixListener::bind(&window.advertisement.socket).unwrap();
		window.advertisement.accepting = true;
		window.write().unwrap();
		(window, listener)
	}
	pub(super) async fn welcome(
		listener: &tokio::net::UnixListener,
		window: &live::discovery::Advertisement,
	) -> tokio::io::BufReader<tokio::net::UnixStream> {
		let (mut stream, _) = listener.accept().await.unwrap();
		stream
			.write_all(
				&live::encode(&live::Event::Welcome {
					window: window.clone(),
					generation: 0,
					figures: vec![],
				})
				.unwrap(),
			)
			.await
			.unwrap();
		tokio::io::BufReader::new(stream)
	}
	pub(super) async fn command(
		stream: &mut tokio::io::BufReader<tokio::net::UnixStream>,
	) -> live::Request {
		let mut line = String::new();
		assert!(stream.read_line(&mut line).await.unwrap() > 0);
		serde_json::from_str(&line).unwrap()
	}
	pub(super) async fn emit(
		stream: &mut tokio::io::BufReader<tokio::net::UnixStream>,
		event: &live::Event,
	) {
		stream
			.get_mut()
			.write_all(&live::encode(event).unwrap())
			.await
			.unwrap();
	}
	#[tokio::test]
	async fn exact_same_dispatch_events_and_unknown_are_returned_without_replay() {
		let mut h = Harness::new();
		let (window, listener) = window(&h);
		h.initialize("2025-06-18").await;
		let advertised = window.advertisement.clone();
		let server = tokio::spawn(async move {
			let mut sent = Vec::new();
			for case in 0..7 {
				let mut stream = welcome(&listener, &advertised).await;
				let request = command(&mut stream).await;
				let receipt = driver::CommandReceipt::new(&request.command);
				emit(&mut stream, &live::Event::Changed { figures: vec![] }).await;
				let event = match case {
					0 => live::Event::Applied {
						id: request.id,
						terminal: true,
						receipt,
					},
					1 => {
						emit(
							&mut stream,
							&live::Event::Applied {
								id: request.id,
								terminal: false,
								receipt: receipt.clone(),
							},
						)
						.await;
						live::Event::Completed {
							id: request.id,
							receipt,
						}
					}
					2 => live::Event::Refused {
						id: request.id,
						reason: "notebook cannot run_source".into(),
						receipt: None,
					},
					3 => live::Event::Failed {
						id: request.id,
						reason: "publication failed".into(),
						error: None,
						root: None,
						receipt,
					},
					4 => live::Event::Superseded {
						id: request.id,
						figure: "1".into(),
						current: Some("2".into()),
					},
					5 => {
						emit(
							&mut stream,
							&live::Event::Applied {
								id: request.id,
								terminal: false,
								receipt,
							},
						)
						.await;
						sent.push(None);
						continue;
					}
					_ => {
						emit(
							&mut stream,
							&live::Event::Closed {
								reason: "closed".into(),
							},
						)
						.await;
						sent.push(None);
						continue;
					}
				};
				emit(&mut stream, &event).await;
				sent.push(Some(serde_json::to_value(event).unwrap()));
				let mut extra = String::new();
				assert_eq!(
					stream.read_line(&mut extra).await.unwrap(),
					0,
					"command replayed"
				);
			}
			sent
		});
		let mut results = Vec::new();
		for (i, (name, args)) in [
			("describe", json!({})),
			("run_source", json!({})),
			("run_source", json!({})),
			("publish", json!({"directory":"out"})),
			(
				"resize",
				json!({"figure":{"view":"rates"},"width":900,"height":600}),
			),
			("run_source", json!({})),
			("describe", json!({})),
		]
		.into_iter()
		.enumerate()
		{
			let response = h
				.request(
					i as u64 + 10,
					"tools/call",
					json!({"name":name,"arguments":args}),
				)
				.await;
			let result = &response["result"];
			assert_eq!(result["isError"], json!(i >= 2), "{response}");
			let value = result["structuredContent"].clone();
			assert_eq!(
				serde_json::from_str::<Value>(result["content"][1]["text"].as_str().unwrap())
					.unwrap(),
				value
			);
			let tool = h.bridge.tools.iter().find(|t| t["name"] == name).unwrap();
			assert!(
				jsonschema::validator_for(&tool["outputSchema"])
					.unwrap()
					.is_valid(&value)
			);
			results.push(value);
		}
		for (actual, expected) in results.iter().zip(server.await.unwrap()) {
			if let Some(expected) = expected {
				assert_eq!(*actual, expected);
			}
		}
		assert_eq!(results[5]["event"], "unknown");
		assert_eq!(results[5]["applied"], true);
		assert_eq!(results[6]["event"], "unknown");
		assert_eq!(results[6]["applied"], false);
		drop(window);
		h.close().await;
	}

	#[tokio::test]
	async fn concurrent_ping_discovery_and_call_complete_while_run_waits_and_cancel_releases_it() {
		let mut h = Harness::new();
		let (window, listener) = window(&h);
		h.initialize("2025-06-18").await;
		h.send(
			json!({"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"run_source"}}),
		)
		.await;
		let mut slow = welcome(&listener, &window.advertisement).await;
		assert_eq!(
			command(&mut slow).await.command,
			driver::Command::RunSource {}
		);
		assert_eq!(h.request(11, "ping", json!({})).await["result"], json!({}));
		assert!(h.request(12, "tools/list", json!({})).await["result"]["tools"].is_array());
		assert!(h.request(13,"tools/call",json!({"name":"windows"})).await["result"]["structuredContent"]["windows"].is_array());
		h.send(json!({"jsonrpc":"2.0","id":14,"method":"tools/call","params":{"name":"describe"}}))
			.await;
		let mut quick = welcome(&listener, &window.advertisement).await;
		let request = command(&mut quick).await;
		emit(
			&mut quick,
			&live::Event::Applied {
				id: request.id,
				terminal: true,
				receipt: driver::CommandReceipt::new(&request.command),
			},
		)
		.await;
		loop {
			let reply = h.receive().await;
			assert_ne!(reply["id"], 10);
			if reply["id"] == 14 {
				break;
			}
		}
		h.send(
			json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":10}}),
		)
		.await;
		let mut bytes = String::new();
		assert_eq!(
			tokio::time::timeout(Duration::from_secs(2), slow.read_line(&mut bytes))
				.await
				.unwrap()
				.unwrap(),
			0
		);
		// The ping acts as a protocol barrier; no cancelled response may precede it.
		h.send(json!({"jsonrpc":"2.0","id":15,"method":"ping"}))
			.await;
		loop {
			let reply = h.receive().await;
			assert_ne!(reply["id"], 10);
			if reply["id"] == 15 {
				break;
			}
		}
		drop(window);
		h.close().await;
	}
}

#[tokio::test]
async fn malformed_json_reports_protocol_error_and_notifications_get_no_answer() {
	use tokio::io::AsyncWriteExt;
	let mut h = Harness::new();
	h.writer.write_all(b"{broken\n").await.unwrap();
	assert_eq!(h.receive().await["error"]["code"], -32700);
	h.send(json!({"jsonrpc":"2.0","method":"notifications/initialized","params":[]}))
		.await;
	h.send(json!({"jsonrpc":"2.0","id":9,"method":"ping"}))
		.await;
	assert_eq!(h.receive().await["id"], 9);
	h.close().await;
}

#[cfg(unix)]
mod batch_tests {
	use super::*;
	use std::os::unix::fs::PermissionsExt;
	pub(super) fn fixture(mode: &str) -> (tempfile::TempDir, PathBuf, DriveArgs) {
		let dir = tempfile::tempdir().unwrap();
		let executable = dir.path().join("driver");
		std::fs::write(&executable,r#"#!/usr/bin/python3
import os, sys, json, time
from pathlib import Path
source=Path(sys.argv[2]); mode=source.read_text()
receipt=Path(sys.argv[sys.argv.index('--receipt')+1])
commands=json.loads(Path(sys.argv[sys.argv.index('--commands')+1]).read_text())['commands']
print('this output must not reach the protocol',flush=True)
print('E'*20000,file=sys.stderr,flush=True)
if mode in ['success','refused','receipt_then_wait']:
 result={'schema_version':1,'source':str(source),'execution':'by-command','loaded':[], 'commands':[{'command':c,'outcome':'refused' if mode=='refused' else 'applied'} for c in commands]}
 receipt.write_text(json.dumps(result))
if mode=='invalid': receipt.write_text('{truncated')
source.with_suffix('.pid').write_text(str(os.getpid()))
if mode in ['wait','receipt_then_wait']:
 while True: time.sleep(1)
"#).unwrap();
		std::fs::set_permissions(&executable, std::fs::Permissions::from_mode(0o700)).unwrap();
		let source = dir.path().join("source.rn");
		std::fs::write(&source, mode).unwrap();
		let args = DriveArgs {
			source,
			commands: vec![driver::Command::Describe {}],
			execution: driver::ExecutionMode::ByCommand,
		};
		(dir, executable, args)
	}
	pub(super) async fn pid(path: &std::path::Path) -> i32 {
		tokio::time::timeout(Duration::from_secs(5), async {
			loop {
				if let Ok(text) = tokio::fs::read_to_string(path.with_extension("pid")).await {
					if let Ok(pid) = text.parse() {
						return pid;
					}
				}
				tokio::time::sleep(Duration::from_millis(10)).await;
			}
		})
		.await
		.unwrap()
	}
	#[tokio::test]
	async fn drive_receipts_determine_outcome_and_invalid_receipts_report_unknown() {
		for mode in ["success", "refused", "invalid"] {
			let (_dir, executable, args) = fixture(mode);
			let tasks = TaskTracker::new();
			let result = batch::run(
				&executable,
				args,
				&CancellationToken::new(),
				&CancellationToken::new(),
				&tasks,
			)
			.await;
			if mode == "invalid" {
				let error = serde_json::to_value(result.unwrap_err()).unwrap();
				assert_eq!(error["dispatch"], "unknown");
				assert!(error["stderr_tail"].as_str().unwrap().len() <= 8192);
			} else {
				let result = tool_result(result.unwrap());
				assert_eq!(result["isError"], mode == "refused");
				let tool = schema::tools()
					.into_iter()
					.find(|t| t["name"] == "drive")
					.unwrap();
				assert!(
					jsonschema::validator_for(&tool["outputSchema"])
						.unwrap()
						.is_valid(&result["structuredContent"])
				);
			}
			tasks.close();
			tasks.wait().await;
		}
	}
	#[tokio::test]
	async fn external_kill_before_and_after_receipt_and_cancellation_reap_the_child() {
		for (mode, cancel_it) in [
			("wait", false),
			("receipt_then_wait", false),
			("wait", true),
		] {
			let (_dir, executable, args) = fixture(mode);
			let source = args.source.clone();
			let cancel = CancellationToken::new();
			let request_cancel = cancel.clone();
			let tasks = TaskTracker::new();
			let running_tasks = tasks.clone();
			let run = tokio::spawn(async move {
				batch::run(
					&executable,
					args,
					&request_cancel,
					&CancellationToken::new(),
					&running_tasks,
				)
				.await
			});
			let pid = pid(&source).await;
			if cancel_it {
				cancel.cancel();
			} else {
				assert_eq!(unsafe { libc::kill(pid, libc::SIGKILL) }, 0);
			}
			let result = tokio::time::timeout(Duration::from_secs(5), run)
				.await
				.unwrap()
				.unwrap();
			assert_eq!(result.is_ok(), mode == "receipt_then_wait");
			tasks.close();
			tasks.wait().await;
			assert_eq!(unsafe { libc::kill(pid, 0) }, -1, "child still exists");
			let mut status = 0;
			assert_eq!(
				unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
				-1,
				"child not reaped"
			);
			assert_eq!(
				std::io::Error::last_os_error().raw_os_error(),
				Some(libc::ECHILD)
			);
		}
	}
}

#[test]
fn tool_input_schemas_match_the_reviewed_snapshot() {
	let tools = schema::tools();
	let inputs: BTreeMap<_, _> = tools
		.iter()
		.map(|tool| (tool["name"].as_str().unwrap(), tool["inputSchema"].clone()))
		.collect();
	let snapshot = serde_json::to_string_pretty(&inputs).unwrap() + "\n";
	let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
		.join("tests/fixtures/mcp-input-schemas.json");
	if std::env::var_os("POLARITON_UPDATE_MCP_SCHEMA_SNAPSHOT").is_some() {
		std::fs::create_dir_all(path.parent().unwrap()).unwrap();
		std::fs::write(&path, &snapshot).unwrap();
	}
	assert_eq!(
		std::fs::read_to_string(path).expect("review/update MCP input schema snapshot"),
		snapshot
	);
}

#[cfg(unix)]
mod resources_tests {
	use super::unix::{command, emit, welcome, window};
	use super::*;
	use tokio::io::AsyncBufReadExt;
	fn figure(id: &str) -> driver::FigureDescription {
		driver::FigureDescription {
			figure: id.into(),
			pane: 1,
			title: "Rates".into(),
			status: "fresh".into(),
			view: Some("rates".into()),
			family: None,
			size: driver::Size::default(),
			series: driver::SeriesReport::Absent {
				reason: "fixture".into(),
			},
			loaded: None,
			live: driver::LiveState::default(),
			render_mode: None,
			settings: BTreeMap::new(),
		}
	}
	#[tokio::test]
	async fn resource_subscribe_read_unsubscribe_and_window_list_changes() {
		let mut h = Harness::new();
		let (owner, listener) = window(&h);
		h.initialize("2025-06-18").await;
		let uri = format!("{RESOURCE_PREFIX}{}", owner.advertisement.window);
		h.send(
			json!({"jsonrpc":"2.0","id":10,"method":"resources/subscribe","params":{"uri":uri}}),
		)
		.await;
		let mut subscription = welcome(&listener, &owner.advertisement).await;
		loop {
			let event = h.receive().await;
			if event["id"] == 10 {
				assert_eq!(event["result"], json!({}));
				break;
			}
		}
		// Run completes with a new incarnation; its Changed notification reaches the subscriber.
		h.send(
			json!({"jsonrpc":"2.0","id":11,"method":"tools/call","params":{"name":"run_source"}}),
		)
		.await;
		let mut run = welcome(&listener, &owner.advertisement).await;
		let request = command(&mut run).await;
		let figures = vec![figure("new-incarnation")];
		let mut receipt = driver::CommandReceipt::new(&request.command);
		receipt.figures = Some(figures.clone());
		emit(
			&mut run,
			&live::Event::Completed {
				id: request.id,
				receipt,
			},
		)
		.await;
		emit(
			&mut subscription,
			&live::Event::Changed {
				figures: figures.clone(),
			},
		)
		.await;
		let mut completed = false;
		let mut updated = false;
		while !(completed && updated) {
			let message = h.receive().await;
			completed |= message["id"] == 11;
			updated |= message["method"] == "notifications/resources/updated"
				&& message["params"]["uri"] == uri;
		}
		h.send(json!({"jsonrpc":"2.0","id":12,"method":"resources/read","params":{"uri":uri}}))
			.await;
		let mut read = welcome(&listener, &owner.advertisement).await;
		let request = command(&mut read).await;
		assert_eq!(request.command, driver::Command::Describe {});
		let mut receipt = driver::CommandReceipt::new(&request.command);
		receipt.figures = Some(figures);
		emit(
			&mut read,
			&live::Event::Applied {
				id: request.id,
				terminal: true,
				receipt,
			},
		)
		.await;
		loop {
			let response = h.receive().await;
			if response["id"] == 12 {
				let contents: Value = serde_json::from_str(
					response["result"]["contents"][0]["text"].as_str().unwrap(),
				)
				.unwrap();
				assert_eq!(contents["figures"][0]["figure"], "new-incarnation");
				break;
			}
		}
		assert_eq!(
			h.request(13, "resources/unsubscribe", json!({"uri":uri}))
				.await["result"],
			json!({})
		);
		let mut line = String::new();
		assert_eq!(
			tokio::time::timeout(Duration::from_secs(2), subscription.read_line(&mut line))
				.await
				.unwrap()
				.unwrap(),
			0
		);
		let (second, _listener) = window(&h);
		loop {
			let event = h.receive().await;
			assert_ne!(event["method"], "notifications/resources/updated");
			if event["method"] == "notifications/resources/list_changed" {
				break;
			}
		}
		assert_eq!(
			h.request(14, "resources/list", json!({})).await["result"]["resources"]
				.as_array()
				.unwrap()
				.len(),
			2
		);
		drop(owner);
		assert!(
			h.request(15, "resources/read", json!({"uri":uri}))
				.await
				.get("error")
				.is_some()
		);
		drop(second);
		h.close().await;
	}
	#[tokio::test]
	async fn ambiguous_gone_and_no_welcome_windows_are_not_sent_and_not_refused() {
		let mut h = Harness::new();
		let (first, listener) = window(&h);
		let (second, _) = window(&h);
		h.initialize("2025-06-18").await;
		let reply = h
			.request(10, "tools/call", json!({"name":"describe"}))
			.await;
		assert_eq!(reply["result"]["structuredContent"]["dispatch"], "not_sent");
		let reason = reply["result"]["structuredContent"]["reason"]
			.as_str()
			.unwrap();
		assert!(reason.contains(&first.advertisement.window));
		assert!(reason.contains(&second.advertisement.window));
		let id = second.advertisement.window.clone();
		drop(second);
		assert_eq!(
			h.request(
				11,
				"tools/call",
				json!({"name":"describe","arguments":{"window":id}})
			)
			.await["result"]["structuredContent"]["dispatch"],
			"not_sent"
		);
		h.send(json!({"jsonrpc":"2.0","id":12,"method":"tools/call","params":{"name":"describe"}}))
			.await;
		let (stream, _) = listener.accept().await.unwrap();
		let start = std::time::Instant::now();
		loop {
			let reply = h.receive().await;
			if reply["id"] == 12 {
				assert_eq!(reply["result"]["structuredContent"]["kind"], "bridge_error");
				assert_eq!(reply["result"]["structuredContent"]["dispatch"], "not_sent");
				break;
			}
		}
		assert!(start.elapsed() < Duration::from_secs(7));
		drop(stream);
		drop(first);
		h.close().await;
	}
	#[tokio::test]
	async fn closing_and_reopening_a_window_does_not_retarget_a_dispatched_call() {
		let mut h = Harness::new();
		let (first, listener) = window(&h);
		h.initialize("2025-06-18").await;
		h.send(
			json!({"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"run_source"}}),
		)
		.await;
		let mut stream = welcome(&listener, &first.advertisement).await;
		let _ = command(&mut stream).await;
		drop(first);
		let (second, reopened) = window(&h);
		drop(stream);
		loop {
			let reply = h.receive().await;
			if reply["id"] == 10 {
				assert_eq!(reply["result"]["structuredContent"]["event"], "unknown");
				break;
			}
		}
		assert!(
			tokio::time::timeout(Duration::from_millis(100), reopened.accept())
				.await
				.is_err()
		);
		drop(second);
		h.close().await;
	}
}

#[cfg(unix)]
#[tokio::test]
async fn mcp_drive_cancel_and_eof_send_no_answer_and_reap_the_child() {
	use tokio::io::{AsyncBufReadExt, AsyncWriteExt};
	for eof in [false, true] {
		let (_dir, executable, args) = batch_tests::fixture("wait");
		let source = args.source.clone();
		let mut h = Harness::with_executable(executable);
		h.initialize("2025-06-18").await;
		h.send(json!({"jsonrpc":"2.0","id":10,"method":"tools/call","params":{"name":"drive","arguments":args}})).await;
		let pid = batch_tests::pid(&source).await;
		if eof {
			h.writer.shutdown().await.unwrap();
			let mut line = String::new();
			assert_eq!(
				tokio::time::timeout(Duration::from_secs(5), h.reader.read_line(&mut line))
					.await
					.unwrap()
					.unwrap(),
				0,
				"reply after EOF: {line}"
			);
		} else {
			h.send(
				json!({"jsonrpc":"2.0","method":"notifications/cancelled","params":{"requestId":10}}),
			)
			.await;
			tokio::time::timeout(Duration::from_secs(5), async {
				while unsafe { libc::kill(pid, 0) } == 0 {
					tokio::time::sleep(Duration::from_millis(10)).await;
				}
			})
			.await
			.unwrap();
			h.send(json!({"jsonrpc":"2.0","id":11,"method":"ping"}))
				.await;
			loop {
				let reply = h.receive().await;
				assert_ne!(reply["id"], 10);
				if reply["id"] == 11 {
					break;
				}
			}
		}
		h.close().await;
		assert_eq!(unsafe { libc::kill(pid, 0) }, -1);
		let mut status = 0;
		assert_eq!(
			unsafe { libc::waitpid(pid, &mut status, libc::WNOHANG) },
			-1
		);
		assert_eq!(
			std::io::Error::last_os_error().raw_os_error(),
			Some(libc::ECHILD)
		);
	}
}

#[test]
fn unsigned_wire_bounds_are_schema_assertions_not_only_format_annotations() {
	let schemas = schema::command_schemas();
	for (name, cases) in [
		(
			"hide",
			vec![
				json!({"figure":{"view":"v"},"series":u32::MAX}),
				json!({"figure":{"view":"v"},"series":u32::MAX as u64+1}),
				json!({"figure":{"view":"v"},"series":-1}),
			],
		),
		(
			"disconnect",
			vec![
				json!({"generation":u64::MAX}),
				serde_json::from_str("{\"generation\":18446744073709551616}").unwrap(),
				json!({"generation":-1}),
			],
		),
	] {
		let validator = jsonschema::validator_for(&schemas[name]).unwrap();
		for args in cases {
			assert_eq!(
				validator.is_valid(&args),
				serde_json::from_value::<driver::Command>(json!({name:args})).is_ok(),
				"{name}: {args}"
			);
		}
	}
	// Standard JSON Schema has no lexical integer-spelling constraint. Keep
	// this edge explicit: schema acceptance never bypasses the wire decoder.
	let args = json!({"generation":1.0});
	assert!(
		jsonschema::validator_for(&schemas["disconnect"])
			.unwrap()
			.is_valid(&args)
	);
	assert!(schema::decode_command("disconnect", args).is_err());
}

#[tokio::test]
async fn integral_float_spelling_is_refused_by_wire_decoder_as_invalid_params() {
	let mut h = Harness::new();
	h.initialize("2025-06-18").await;
	for (name, arguments) in [
		("disconnect", json!({"generation": 1.0})),
		("hide", json!({"figure": {"view": "test"}, "series": 1.0})),
	] {
		let response = h
			.request(
				3,
				"tools/call",
				json!({"name": name, "arguments": arguments}),
			)
			.await;
		assert_eq!(response["error"]["code"], -32602, "{response}");
		assert!(response.get("result").is_none());
	}
	h.close().await;
}
