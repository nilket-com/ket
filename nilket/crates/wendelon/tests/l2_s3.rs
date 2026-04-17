use async_trait::async_trait;
use openraft::storage::RaftStateMachine;
use openraft::{CommittedLeaderId, Entry, EntryPayload, LogId};
use std::collections::HashMap;
use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU64, Ordering};
use std::time::Duration;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::TcpListener;
use tokio::sync::Mutex as AsyncMutex;
use wendelon::consensus::{
	TierObserver, TieredSlab, WendelonRequest, WendelonStateMachine, WendelonTypeConfig,
};
use wendelon::schema::HybridTimestamp;
use wendelon::storage::{
	ManagerTierObserver, ObjectStore, RemoteObjectStore, RemoteObjectStoreConfig, SlabManager,
	SlabManagerConfig, SlabSweeper, SweepReport, SweeperConfig, TierCommitter, spawn_io_thread,
};

fn fresh_dir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-l2-remote-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
}

fn hlc() -> HybridTimestamp {
	HybridTimestamp {
		physical_ms: 1,
		logical: 0,
		node_id: 1,
	}
}

fn compat_config(endpoint: String, root: &str) -> RemoteObjectStoreConfig {
	RemoteObjectStoreConfig {
		bucket: "wendelon-tier".into(),
		endpoint,
		region: "us-east-1".into(),
		root: root.into(),
		access_key_id: Some("test-access-key".into()),
		secret_access_key: Some("test-secret-key".into()),
		session_token: None,
		allow_anonymous: false,
		virtual_host_style: false,
	}
}

struct AppliedStateMachineCommitter {
	sm: Arc<AsyncMutex<WendelonStateMachine>>,
	next_index: AtomicU64,
}

impl AppliedStateMachineCommitter {
	fn new(sm: WendelonStateMachine) -> Self {
		Self {
			sm: Arc::new(AsyncMutex::new(sm)),
			next_index: AtomicU64::new(1),
		}
	}
}

#[async_trait]
impl TierCommitter for AppliedStateMachineCommitter {
	async fn commit_tier_slab(&self, tier: TieredSlab) -> std::io::Result<()> {
		let index = self.next_index.fetch_add(1, Ordering::Relaxed);
		let entry = Entry::<WendelonTypeConfig> {
			log_id: LogId::new(CommittedLeaderId::new(1, 0), index),
			payload: EntryPayload::Normal(WendelonRequest::TierSlab(tier)),
		};
		let mut sm = self.sm.lock().await;
		<WendelonStateMachine as RaftStateMachine<WendelonTypeConfig>>::apply(
			&mut *sm,
			vec![entry],
		)
		.await
		.map_err(|err| Error::other(format!("test tier commit failed: {err}")))?;
		Ok(())
	}
}

#[derive(Clone, Default)]
struct FakeCompatStoreState {
	objects: Arc<AsyncMutex<HashMap<String, Vec<u8>>>>,
	fail_puts_remaining: Arc<AsyncMutex<HashMap<String, usize>>>,
	put_attempts: Arc<AsyncMutex<HashMap<String, usize>>>,
}

impl FakeCompatStoreState {
	async fn fail_next_puts(&self, path: &str, count: usize) {
		self.fail_puts_remaining
			.lock()
			.await
			.insert(path.to_string(), count);
	}

	async fn put_attempts(&self, path: &str) -> usize {
		*self.put_attempts.lock().await.get(path).unwrap_or(&0)
	}
}

struct FakeCompatStoreServer {
	state: FakeCompatStoreState,
	endpoint: String,
	_task: tokio::task::JoinHandle<()>,
}

impl FakeCompatStoreServer {
	async fn start() -> Self {
		let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
		let endpoint = format!("http://{}", listener.local_addr().unwrap());
		let state = FakeCompatStoreState::default();
		let task_state = state.clone();
		let task = tokio::spawn(async move {
			loop {
				let Ok((mut stream, _)) = listener.accept().await else {
					break;
				};
				let state = task_state.clone();
				tokio::spawn(async move {
					let Ok(Some(req)) = read_request(&mut stream).await else {
						return;
					};
					let _ = handle_request(&mut stream, state, req).await;
				});
			}
		});
		Self {
			state,
			endpoint,
			_task: task,
		}
	}
}

struct ParsedRequest {
	method: String,
	path: String,
	headers: HashMap<String, String>,
	body: Vec<u8>,
}

async fn read_request(
	stream: &mut tokio::net::TcpStream,
) -> std::io::Result<Option<ParsedRequest>> {
	let mut buf = Vec::new();
	let header_end = loop {
		if let Some(idx) = find_bytes(&buf, b"\r\n\r\n") {
			break idx;
		}
		let mut chunk = [0_u8; 4096];
		let read = stream.read(&mut chunk).await?;
		if read == 0 {
			if buf.is_empty() {
				return Ok(None);
			}
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"connection closed before request headers completed",
			));
		}
		buf.extend_from_slice(&chunk[..read]);
	};

	let header_bytes = &buf[..header_end];
	let header_text = std::str::from_utf8(header_bytes).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("request headers are not valid UTF-8: {err}"),
		)
	})?;
	let mut lines = header_text.split("\r\n");
	let request_line = lines
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing request line"))?;
	let mut parts = request_line.split_whitespace();
	let method = parts
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing request method"))?
		.to_string();
	let path = parts
		.next()
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "missing request path"))?
		.to_string();
	let mut headers = HashMap::new();
	for line in lines {
		if line.is_empty() {
			continue;
		}
		let (name, value) = line.split_once(':').ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidData,
				format!("malformed request header: {line}"),
			)
		})?;
		headers.insert(name.trim().to_ascii_lowercase(), value.trim().to_string());
	}

	if matches!(headers.get("expect"), Some(expect) if expect.eq_ignore_ascii_case("100-continue"))
	{
		stream.write_all(b"HTTP/1.1 100 Continue\r\n\r\n").await?;
	}

	let content_length = headers
		.get("content-length")
		.and_then(|value| value.parse::<usize>().ok())
		.unwrap_or(0);
	let mut body = buf[(header_end + 4)..].to_vec();
	while body.len() < content_length {
		let mut chunk = vec![0_u8; content_length - body.len()];
		let read = stream.read(&mut chunk).await?;
		if read == 0 {
			return Err(Error::new(
				ErrorKind::UnexpectedEof,
				"connection closed before request body completed",
			));
		}
		body.extend_from_slice(&chunk[..read]);
	}
	body.truncate(content_length);

	Ok(Some(ParsedRequest {
		method,
		path,
		headers,
		body,
	}))
}

fn find_bytes(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

async fn write_response(
	stream: &mut tokio::net::TcpStream,
	status: &str,
	headers: &[(&str, String)],
	body: &[u8],
) -> std::io::Result<()> {
	let mut response = format!("HTTP/1.1 {status}\r\n");
	response.push_str(&format!("Content-Length: {}\r\n", body.len()));
	response.push_str("Connection: close\r\n");
	for (name, value) in headers {
		response.push_str(name);
		response.push_str(": ");
		response.push_str(value);
		response.push_str("\r\n");
	}
	response.push_str("\r\n");
	stream.write_all(response.as_bytes()).await?;
	stream.write_all(body).await
}

async fn handle_request(
	stream: &mut tokio::net::TcpStream,
	state: FakeCompatStoreState,
	req: ParsedRequest,
) -> std::io::Result<()> {
	let path = req
		.path
		.split('?')
		.next()
		.unwrap_or_default()
		.trim_start_matches('/')
		.to_string();

	match req.method.as_str() {
		"PUT" => {
			let _auth_present = req.headers.contains_key("authorization");
			let mut attempts = state.put_attempts.lock().await;
			let entry = attempts.entry(path.clone()).or_insert(0);
			*entry += 1;
			drop(attempts);

			let mut fail_map = state.fail_puts_remaining.lock().await;
			if let Some(remaining) = fail_map.get_mut(&path) {
				if *remaining > 0 {
					*remaining -= 1;
					return write_response(
						stream,
						"503 Service Unavailable",
						&[("Content-Type", "application/xml".into())],
						b"<Error><Code>SlowDown</Code></Error>",
					)
					.await;
				}
			}
			drop(fail_map);

			state.objects.lock().await.insert(path, req.body);
			write_response(stream, "200 OK", &[], &[]).await
		}
		"GET" => {
			let objects = state.objects.lock().await;
			if let Some(bytes) = objects.get(&path) {
				write_response(
					stream,
					"200 OK",
					&[("Content-Type", "application/octet-stream".into())],
					bytes,
				)
				.await
			} else {
				write_response(stream, "404 Not Found", &[], &[]).await
			}
		}
		"HEAD" => {
			if path == "wendelon-tier" || state.objects.lock().await.contains_key(&path) {
				write_response(stream, "200 OK", &[], &[]).await
			} else {
				write_response(stream, "404 Not Found", &[], &[]).await
			}
		}
		"DELETE" => {
			state.objects.lock().await.remove(&path);
			write_response(stream, "204 No Content", &[], &[]).await
		}
		_ => write_response(stream, "405 Method Not Allowed", &[], &[]).await,
	}
}

async fn build_manager_with_sealed_slabs(
	l2: Option<Arc<dyn ObjectStore>>,
) -> (Arc<SlabManager>, PathBuf) {
	use wendelon::storage::StorageEngine;

	let dir = fresh_dir("l1");
	let io = spawn_io_thread(64).unwrap();
	let manager = Arc::new(
		SlabManager::open(SlabManagerConfig {
			dir: dir.clone(),
			max_slab_bytes: 1,
			io,
			l2,
			rehydration: wendelon::storage::RehydrationPolicy::default(),
		})
		.unwrap(),
	);
	manager.append(1, hlc(), b"hello-1").await.unwrap();
	manager.append(2, hlc(), b"hello-2").await.unwrap();
	manager.append(3, hlc(), b"hello-3").await.unwrap();
	(manager, dir)
}

#[tokio::test]
async fn remote_object_store_round_trips_put_get_delete() {
	let server = FakeCompatStoreServer::start().await;
	let store =
		RemoteObjectStore::new(compat_config(server.endpoint.clone(), "cluster-a")).unwrap();

	let uri = store
		.put("sealed/slab_0001.wnd", b"hello remote tier")
		.await
		.unwrap();
	assert_eq!(uri, "s3://wendelon-tier/cluster-a/sealed/slab_0001.wnd");

	let bytes = store.get(&uri).await.unwrap();
	assert_eq!(bytes, b"hello remote tier");

	store.delete(&uri).await.unwrap();
	assert!(store.get(&uri).await.is_err());
}

#[tokio::test]
async fn remote_object_store_retries_transient_put_failures() {
	let server = FakeCompatStoreServer::start().await;
	let object_path = "wendelon-tier/retry/slab_retry.wnd";
	server.state.fail_next_puts(object_path, 1).await;

	let store = RemoteObjectStore::new(compat_config(server.endpoint.clone(), "retry")).unwrap();
	let uri = store.put("slab_retry.wnd", b"retry me").await.unwrap();

	assert_eq!(uri, "s3://wendelon-tier/retry/slab_retry.wnd");
	assert_eq!(server.state.put_attempts(object_path).await, 2);
}

#[tokio::test]
async fn sweeper_persists_remote_uri_shape_in_tier_metadata() {
	let server = FakeCompatStoreServer::start().await;
	let remote: Arc<dyn ObjectStore> =
		Arc::new(RemoteObjectStore::new(compat_config(server.endpoint.clone(), "tiered")).unwrap());
	let (manager, _dir) = build_manager_with_sealed_slabs(Some(remote.clone())).await;

	let observer: Arc<dyn TierObserver> = Arc::new(ManagerTierObserver::new(manager.clone()));
	let state_machine = WendelonStateMachine::new().with_tier_observer(observer);
	let committer: Arc<dyn TierCommitter> =
		Arc::new(AppliedStateMachineCommitter::new(state_machine.clone()));

	let sweeper = SlabSweeper::new(
		manager.clone(),
		remote,
		committer,
		state_machine.clone(),
		SweeperConfig {
			scan_interval: Duration::from_millis(10),
			min_slab_bytes: 0,
			max_upload_bytes_per_sec: None,
			key_prefix: "sealed/".into(),
		},
	);

	let report: SweepReport = sweeper.sweep_once().await.unwrap();
	assert!(report.tiered > 0);

	let snapshot = state_machine.state_snapshot().await;
	assert!(
		!snapshot.tiered_slabs.is_empty(),
		"expected tier metadata to be committed into the state machine"
	);
	for tiered in snapshot.tiered_slabs.values() {
		assert!(
			tiered
				.l2_uri
				.starts_with("s3://wendelon-tier/tiered/sealed/slab_"),
			"unexpected tier URI: {}",
			tiered.l2_uri
		);
	}
}
