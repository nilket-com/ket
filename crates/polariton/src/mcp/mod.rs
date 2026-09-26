//! ADR 0173: stdio binding of the existing live client and batch driver.
//! rmcp owns typed messages, concurrent request IDs, cancellation and response delivery.
//! We use its direct service entry so the ADR's strict initialization gate stays
//! explicit (the SDK's normal server entry permits requests before initialized).
mod batch;
mod schema;
mod transport;

use crate::{driver, live};
use rmcp::{
	ErrorData, RoleServer, Service,
	model::*,
	service::{NotificationContext, RequestContext},
};
use schemars::JsonSchema;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
	collections::{BTreeMap, HashMap},
	path::PathBuf,
	sync::{
		Arc, Mutex,
		atomic::{AtomicU8, Ordering},
	},
	time::Duration,
};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

const PRE_SEND: Duration = Duration::from_secs(5);
const RESOURCE_PREFIX: &str = "polariton://window/";

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub(super) struct BridgeError {
	kind: BridgeErrorKind,
	dispatch: Dispatch,
	reason: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	exit_status: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	stderr_tail: Option<String>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum BridgeErrorKind {
	BridgeError,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
enum Dispatch {
	NotSent,
	Unknown,
}

impl BridgeError {
	fn not_sent(reason: impl Into<String>) -> Self {
		Self {
			kind: BridgeErrorKind::BridgeError,
			dispatch: Dispatch::NotSent,
			reason: reason.into(),
			exit_status: None,
			stderr_tail: None,
		}
	}
}

#[derive(Serialize, JsonSchema)]
struct WindowsResult {
	windows: Vec<live::discovery::Advertisement>,
}

#[derive(Debug, Serialize, Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
struct DriveArgs {
	/// Rune source file. Relative paths resolve against the MCP server cwd.
	source: PathBuf,
	/// Existing wire commands, e.g. [{"run_source":{}},{"describe":{}}]. Paths inside commands also resolve against the server cwd.
	commands: Vec<driver::Command>,
	/// Explicit execution mode: by-command runs only at run_source; on-open runs the source before commands.
	execution: driver::ExecutionMode,
}

struct Bridge {
	phase: AtomicU8, // 0: before initialize, 1: waiting initialized, 2: ready
	tools: Vec<Value>,
	validators: BTreeMap<String, jsonschema::Validator>,
	root: PathBuf,
	executable: PathBuf,
	stop: CancellationToken,
	tasks: TaskTracker,
	subscriptions: Mutex<HashMap<String, Arc<CancellationToken>>>,
}

impl Bridge {
	fn new(root: PathBuf, executable: PathBuf) -> Arc<Self> {
		let tools = schema::tools();
		Arc::new(Self {
			phase: AtomicU8::new(0),
			validators: schema::validators(&tools),
			tools,
			root,
			executable,
			stop: CancellationToken::new(),
			tasks: TaskTracker::new(),
			subscriptions: Mutex::new(HashMap::new()),
		})
	}

	async fn discover(&self) -> Result<Vec<live::discovery::Advertisement>, BridgeError> {
		let root = self.root.clone();
		tokio::time::timeout(
			PRE_SEND,
			tokio::task::spawn_blocking(move || live::discovery::discover(&root)),
		)
		.await
		.map_err(|_| BridgeError::not_sent("window discovery timed out"))?
		.map_err(|e| BridgeError::not_sent(format!("window discovery failed: {e}")))?
		.map_err(|e| BridgeError::not_sent(e.to_string()))
	}

	async fn select(
		&self,
		selected: Option<&str>,
	) -> Result<live::discovery::Advertisement, BridgeError> {
		let windows = self.discover().await?;
		if let Some(id) = selected {
			return windows
				.into_iter()
				.find(|window| window.window == id)
				.ok_or_else(|| {
					BridgeError::not_sent(format!("window {id} is not advertised; discover again"))
				});
		}
		match windows.as_slice() {
			[window] => Ok(window.clone()),
			[] => Err(BridgeError::not_sent(
				"no advertised window; open polariton --agents <source.rn> or use the drive tool",
			)),
			_ => Err(BridgeError::not_sent(format!(
				"name one window: {}",
				windows
					.iter()
					.map(|w| w.window.as_str())
					.collect::<Vec<_>>()
					.join(", ")
			))),
		}
	}

	async fn live_call(
		&self,
		selected: Option<&str>,
		command: driver::Command,
	) -> Result<Value, BridgeError> {
		let bytes =
			live::encode(&live::Request { id: 1, command }).map_err(BridgeError::not_sent)?;
		if bytes.len() > live::MAX_LINE {
			return Err(BridgeError::not_sent(
				"command exceeds the 64 KiB wire request limit",
			));
		}
		let window = self.select(selected).await?;
		let mut connection = live::client::Connection::open(&window)
			.await
			.map_err(BridgeError::not_sent)?;
		Ok(serde_json::to_value(connection.command(bytes).await).expect("event serializes"))
	}

	async fn call(
		&self,
		params: Value,
		context: &RequestContext<RoleServer>,
	) -> Result<Value, ErrorData> {
		let name = params
			.get("name")
			.and_then(Value::as_str)
			.ok_or_else(|| invalid("tool name is required"))?;
		let args = params
			.get("arguments")
			.cloned()
			.unwrap_or_else(|| json!({}));
		let validator = self
			.validators
			.get(name)
			.ok_or_else(|| invalid(format!("unknown tool {name}")))?;
		if let Err(error) = validator.validate(&args) {
			return Err(invalid(error.to_string()));
		}
		let result = match name {
			"windows" => self
				.discover()
				.await
				.map(|windows| serde_json::to_value(WindowsResult { windows }).unwrap()),
			"drive" => {
				let args: DriveArgs =
					serde_json::from_value(args).map_err(|e| invalid(e.to_string()))?;
				batch::run(&self.executable, args, &context.ct, &self.stop, &self.tasks).await
			}
			_ => {
				let (window, command) = schema::decode_command(name, args).map_err(invalid)?;
				self.live_call(window.as_deref(), command).await
			}
		};
		let value = result.unwrap_or_else(|error| serde_json::to_value(error).unwrap());
		Ok(tool_result(value))
	}

	async fn resources(&self) -> Result<Value, ErrorData> {
		let windows = self.discover().await.map_err(|e| internal(e.reason))?;
		Ok(json!({"resources":windows.iter().map(|window| json!({
			"uri":format!("{RESOURCE_PREFIX}{}",window.window), "name":window.window,
			"description":"Current Polariton window advertisement and figures (not an event log)","mimeType":"application/json"
		})).collect::<Vec<_>>()}))
	}

	async fn read_resource(&self, uri: &str) -> Result<Value, ErrorData> {
		let id = resource_id(uri)?;
		let window = self
			.select(Some(id))
			.await
			.map_err(|e| not_found(e.reason))?;
		let mut connection = live::client::Connection::open(&window)
			.await
			.map_err(not_found)?;
		let bytes = live::encode(&live::Request {
			id: 1,
			command: driver::Command::Describe {},
		})
		.map_err(internal)?;
		let event = connection.command(bytes).await;
		let figures = match event {
			live::Event::Applied { receipt, .. } | live::Event::Completed { receipt, .. } => {
				receipt
					.figures
					.ok_or_else(|| internal("describe omitted its figures"))?
			}
			_ => return Err(internal("could not read the window's current figures")),
		};
		Ok(
			json!({"contents":[{"uri":uri,"mimeType":"application/json","text":serde_json::to_string(&json!({"window":window,"figures":figures})).unwrap()}]}),
		)
	}

	async fn subscribe(
		self: &Arc<Self>,
		uri: &str,
		peer: rmcp::Peer<RoleServer>,
	) -> Result<Value, ErrorData> {
		let id = resource_id(uri)?;
		let window = self
			.select(Some(id))
			.await
			.map_err(|e| not_found(e.reason))?;
		let connection = live::client::Connection::open(&window)
			.await
			.map_err(not_found)?;
		let token = Arc::new(self.stop.child_token());
		if let Some(old) = self
			.subscriptions
			.lock()
			.unwrap()
			.insert(uri.into(), token.clone())
		{
			old.cancel();
		}
		let uri = uri.to_owned();
		let owner = Arc::downgrade(self);
		self.tasks.spawn(async move {
			let mut connection = connection;
			loop {
				let next = tokio::select! { biased; _ = token.cancelled() => break, result = connection.next() => result };
				match next {
					Ok(Some(live::Event::Changed {..})) => {
						let notification = ResourceUpdatedNotificationParam::new(uri.clone());
						tokio::select! { biased; _ = token.cancelled() => break, _ = peer.notify_resource_updated(notification) => {} }
					}
					Ok(Some(live::Event::Closed {..})) | Ok(None) | Err(_) => break,
					_ => {}
				}
			}
			if let Some(owner) = owner.upgrade() {
				let mut subscriptions = owner.subscriptions.lock().unwrap();
				if subscriptions.get(&uri).is_some_and(|current| Arc::ptr_eq(current, &token)) {
					subscriptions.remove(&uri);
				}
			}
		});
		Ok(json!({}))
	}

	fn observe_discovery(self: &Arc<Self>, peer: rmcp::Peer<RoleServer>) {
		let bridge = self.clone();
		self.tasks.spawn(async move {
			let mut previous = bridge.discover().await.ok();
			let mut timer = tokio::time::interval(Duration::from_millis(500));
			loop {
				tokio::select! { biased; _ = bridge.stop.cancelled() => break, _ = timer.tick() => {} }
				let windows = tokio::select! { biased; _ = bridge.stop.cancelled() => break, result = bridge.discover() => result.ok() };
				if let Some(windows) = windows {
					if previous.as_ref() != Some(&windows) {
						tokio::select! { biased; _ = bridge.stop.cancelled() => break, _ = peer.notify_resource_list_changed() => {} }
					}
					previous = Some(windows);
				}
			}
		});
	}

	async fn request(
		self: &Arc<Self>,
		request: ClientRequest,
		context: &RequestContext<RoleServer>,
	) -> Result<ServerResult, ErrorData> {
		let method = request.method().to_owned();
		if matches!(&request, ClientRequest::CustomRequest(_))
			&& matches!(
				method.as_str(),
				"initialize"
					| "ping" | "tools/list"
					| "tools/call" | "resources/list"
					| "resources/templates/list"
					| "resources/read"
					| "resources/subscribe"
					| "resources/unsubscribe"
			) {
			return Err(invalid(format!("malformed {method} parameters")));
		}
		let request = serde_json::to_value(request).map_err(|e| internal(e.to_string()))?;
		let params = request.get("params").cloned().unwrap_or_else(|| json!({}));
		if method == "ping" {
			return Ok(ServerResult::CustomResult(CustomResult(json!({}))));
		}
		if method == "initialize" {
			serde_json::from_value::<InitializeRequestParams>(params)
				.map_err(|e| invalid(e.to_string()))?;
			if self
				.phase
				.compare_exchange(0, 1, Ordering::SeqCst, Ordering::SeqCst)
				.is_err()
			{
				return Err(invalid("initialize may occur only once"));
			}
			return Ok(ServerResult::InitializeResult(server_info()));
		}
		if self.phase.load(Ordering::SeqCst) != 2 {
			return Err(invalid(
				"initialize and notifications/initialized must precede tools and resources",
			));
		}
		let value = match method.as_str() {
			"tools/list" => json!({"tools":self.tools}),
			"tools/call" => self.call(params, context).await?,
			"resources/list" => self.resources().await?,
			"resources/templates/list" => json!({"resourceTemplates":[]}),
			"resources/read" => self.read_resource(uri(&params)?).await?,
			"resources/subscribe" => self.subscribe(uri(&params)?, context.peer.clone()).await?,
			"resources/unsubscribe" => {
				let uri = uri(&params)?;
				resource_id(uri)?;
				if let Some(token) = self.subscriptions.lock().unwrap().remove(uri) {
					token.cancel();
				}
				json!({})
			}
			_ => {
				return Err(ErrorData::new(
					ErrorCode::METHOD_NOT_FOUND,
					format!("unknown method {method}"),
					None,
				));
			}
		};
		Ok(ServerResult::CustomResult(CustomResult(value)))
	}
}

// A wrapper avoids the SDK's blanket Service implementation for Arc<T>.
struct McpService(Arc<Bridge>);
impl Service<RoleServer> for McpService {
	async fn handle_request(
		&self,
		request: ClientRequest,
		context: RequestContext<RoleServer>,
	) -> Result<ServerResult, ErrorData> {
		let _task = self.0.tasks.token();
		// rmcp removes cancelled request IDs before our handler returns and drops
		// their eventual responses. These internal cancellation errors are not sent.
		// drive must observe cancellation itself to terminate AND reap its child.
		if request.method() == "tools/call"
			&& serde_json::to_value(&request)
				.ok()
				.is_some_and(|v| v["params"]["name"] == "drive")
		{
			return self.0.request(request, &context).await;
		}
		tokio::select! { biased;
			_ = self.0.stop.cancelled() => Err(internal("server shutting down")),
			_ = context.ct.cancelled() => Err(internal("request cancelled; dispatched effects are not undone")),
			result = self.0.request(request, &context) => result,
		}
	}
	async fn handle_notification(
		&self,
		notification: ClientNotification,
		context: NotificationContext<RoleServer>,
	) -> Result<(), ErrorData> {
		if matches!(notification, ClientNotification::InitializedNotification(_)) {
			if self
				.0
				.phase
				.compare_exchange(1, 2, Ordering::SeqCst, Ordering::SeqCst)
				.is_ok()
			{
				self.0.observe_discovery(context.peer);
			} else {
				eprintln!("polariton mcp: ignored out-of-order initialized notification");
			}
		} else if self.0.phase.load(Ordering::SeqCst) != 2 {
			eprintln!("polariton mcp: ignored notification before initialization");
		}
		Ok(())
	}
	fn get_info(&self) -> ServerConfig {
		server_info()
	}
	fn supported_protocol_versions(&self) -> std::borrow::Cow<'static, [ProtocolVersion]> {
		std::borrow::Cow::Owned(vec![ProtocolVersion::V_2025_06_18])
	}
}

fn server_info() -> ServerConfig {
	serde_json::from_value(json!({"protocolVersion":"2025-06-18","capabilities":{"tools":{},"resources":{"subscribe":true,"listChanged":true}},
		"serverInfo":{"name":"polariton","version":env!("CARGO_PKG_VERSION")},"instructions":"Discover windows first, then pin the chosen window ID. Never replay an unknown outcome automatically."})).unwrap()
}
fn invalid(message: impl Into<String>) -> ErrorData {
	ErrorData::invalid_params(message.into(), None)
}
fn internal(message: impl Into<String>) -> ErrorData {
	ErrorData::internal_error(message.into(), None)
}
fn not_found(message: impl Into<String>) -> ErrorData {
	ErrorData::new(ErrorCode(-32002), message.into(), None)
}
fn uri(params: &Value) -> Result<&str, ErrorData> {
	params["uri"]
		.as_str()
		.ok_or_else(|| invalid("resource URI is required"))
}
fn resource_id(uri: &str) -> Result<&str, ErrorData> {
	let id = uri
		.strip_prefix(RESOURCE_PREFIX)
		.filter(|s| s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit()));
	id.ok_or_else(|| not_found("unknown Polariton resource URI"))
}
fn tool_result(value: Value) -> Value {
	let failed = value["kind"] == "bridge_error"
		|| matches!(
			value["event"].as_str(),
			Some("refused" | "failed" | "superseded" | "unknown")
		) || value.get("refusal").is_some_and(|v| !v.is_null())
		|| value["commands"]
			.as_array()
			.is_some_and(|commands| commands.iter().any(|c| c["outcome"] != "applied"));
	let summary = if failed {
		"Polariton did not report success; inspect the outcome before taking another action."
	} else {
		"Polariton completed the request."
	};
	json!({"content":[{"type":"text","text":summary},{"type":"text","text":serde_json::to_string(&value).unwrap()}],"structuredContent":value,"isError":failed})
}

pub(crate) fn run() -> Result<(), String> {
	let bridge = Bridge::new(
		live::discovery::runtime_directory().map_err(|e| e.to_string())?,
		std::env::current_exe().map_err(|e| e.to_string())?,
	);
	let runtime = tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
		.map_err(|e| e.to_string())?;
	let result = runtime.block_on(async {
		let service = rmcp::service::serve_directly(
			McpService(bridge.clone()),
			transport::Stdio::new(tokio::io::stdin(), tokio::io::stdout(), bridge.stop.clone()),
			None,
		);
		let waiting = service.waiting();
		tokio::pin!(waiting);
		let result = tokio::select! {
			result = &mut waiting => result,
			_ = shutdown_signal() => { bridge.stop.cancel(); waiting.await }
		};
		bridge.stop.cancel();
		bridge.tasks.close();
		bridge.tasks.wait().await;
		result.map(|_| ()).map_err(|e| e.to_string())
	});
	// Tokio's stdin uses an uninterruptible blocking read. All owned children
	// and subscriptions have settled above; do not wait forever for another
	// input byte when SIGTERM/interrupt caused shutdown instead of EOF.
	runtime.shutdown_timeout(Duration::from_secs(1));
	result
}

async fn shutdown_signal() {
	#[cfg(unix)]
	{
		if let Ok(mut terminate) =
			tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
		{
			tokio::select! { _ = terminate.recv() => {}, _ = tokio::signal::ctrl_c() => {} }
			return;
		}
	}
	let _ = tokio::signal::ctrl_c().await;
}

#[cfg(test)]
mod tests;
