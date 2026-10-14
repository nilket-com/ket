//! Held execution for the SQL listener timeout regression; no production timing hook.
use std::sync::{Arc, Mutex};
use std::time::Duration;
use tokio::sync::oneshot;
use tokio::task::{JoinHandle, JoinSet};
use toron::config::StaticWorkerConfig;
use toron::distributed::identity::{QueryId, WorkerId};
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::worker_registry::WorkerEndpoint;
use toron::error::StorageError;
use toron::quic::QuicEndpoint;
use toron::server::ToronWorker;
use toron::server::memory::MemoryManager;
use toron::sql_wire::{SqlError, SqlFrame, SqlMessage, read_frame, write_frame};
use toron::storage::{ScanRequest, ScanStream, StorageScan, TenantId};

const BOUND: Duration = Duration::from_secs(10);

struct HeldScan {
	entered: Mutex<Option<oneshot::Sender<(QueryId, TenantId)>>>,
}
#[async_trait::async_trait]
impl StorageScan for HeldScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		if let Some(entered) = self.entered.lock().unwrap().take() {
			let _ = entered.send((request.query_id, request.tenant_id));
		}
		// Only cancellation/owned task teardown may drop this future; it cannot succeed or
		// manufacture a lower-layer error at the SQL deadline.
		std::future::pending().await
	}
}

pub(super) struct HeldWorker {
	pub config: StaticWorkerConfig,
	endpoint: QuicEndpoint,
	stop: Option<oneshot::Sender<()>>,
	task: Option<JoinHandle<()>>,
	entered: Option<oneshot::Receiver<(QueryId, TenantId)>>,
}
impl HeldWorker {
	pub fn start() -> Self {
		let endpoint = QuicEndpoint::bind_loopback("127.0.0.1:0".parse().unwrap()).unwrap();
		let address = endpoint.local_addr().unwrap();
		let worker_id = WorkerId::from_u128(535536);
		let (entered_tx, entered_rx) = oneshot::channel();
		let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
			Arc::new(HeldScan {
				entered: Mutex::new(Some(entered_tx)),
			}),
			Arc::new(MemoryManager::new(32, 64 * 1024 * 1024)),
			Arc::new(InMemoryPartitionStore::new()),
			worker_id,
			WorkerEndpoint::new(address.to_string()),
		));
		let listener = endpoint.clone();
		let (stop_tx, mut stop_rx) = oneshot::channel();
		let task = tokio::spawn(async move {
			let mut connections = JoinSet::new();
			loop {
				tokio::select! {
					_ = &mut stop_rx => break,
					accepted = listener.accept_sql_incoming() => {
						let incoming = accepted.expect("held worker incoming");
						let worker = worker.clone();
						connections.spawn(async move {
							let connection = QuicEndpoint::accept_incoming_connection(incoming).await.expect("held worker handshake");
							let stream = QuicEndpoint::accept_stream_on(&connection).await.expect("held worker stream");
							let _ = worker.handle_connection(stream).await;
						});
					}
					joined = connections.join_next(), if !connections.is_empty() => {
						joined.unwrap().expect("held worker connection task");
					}
				}
			}
			listener.close();
			connections.abort_all();
			while let Some(result) = connections.join_next().await {
				if let Err(error) = result {
					assert!(error.is_cancelled(), "worker task panicked: {error}");
				}
			}
		});
		Self {
			config: StaticWorkerConfig {
				worker_id,
				endpoint: address,
			},
			endpoint,
			stop: Some(stop_tx),
			task: Some(task),
			entered: Some(entered_rx),
		}
	}
	pub async fn entered(&mut self) -> (QueryId, TenantId) {
		tokio::time::timeout(BOUND, self.entered.take().unwrap())
			.await
			.expect("SQL query did not enter held scan before safety bound")
			.expect("held worker stopped before scan entry")
	}
	pub async fn shutdown(mut self) {
		let _ = self.stop.take().unwrap().send(());
		tokio::time::timeout(BOUND, self.task.as_mut().unwrap())
			.await
			.expect("held worker drain exceeded safety bound")
			.expect("held worker accept task");
		self.task.take();
		self.endpoint.close();
	}
}
impl Drop for HeldWorker {
	fn drop(&mut self) {
		self.endpoint.close();
		if let Some(stop) = self.stop.take() {
			let _ = stop.send(());
		}
		if let Some(task) = self.task.take() {
			task.abort();
		}
	}
}

pub(super) struct WireOutcome {
	pub error: Option<SqlError>,
	pub result_batches: usize,
	pub completes: usize,
}
/// Owns the request task even on assertion failure or cancellation.
pub(super) struct SqlRequest(Option<JoinHandle<WireOutcome>>);
impl SqlRequest {
	pub fn start(endpoint: std::net::SocketAddr, query_id: QueryId) -> Self {
		Self(Some(tokio::spawn(async move {
			let client = QuicEndpoint::client().expect("SQL client endpoint");
			let mut stream = client
				.connect_stream(endpoint)
				.await
				.expect("SQL connection");
			write_frame(
				&mut stream,
				&SqlFrame::execute(query_id, "SELECT cat, val FROM test2".into(), None),
			)
			.await
			.expect("SQL request");
			stream.finish().await.expect("SQL request finish");
			let mut result_batches = 0;
			let outcome = loop {
				let frame = read_frame(&mut stream)
					.await
					.expect("SQL response transport");
				assert_eq!(frame.query_id, query_id);
				match frame.message {
					SqlMessage::ResultBatch(_) => result_batches += 1,
					SqlMessage::Complete(_) => {
						break WireOutcome {
							error: None,
							result_batches,
							completes: 1,
						};
					}
					SqlMessage::Error(error) => {
						break WireOutcome {
							error: Some(error),
							result_batches,
							completes: 0,
						};
					}
					other => panic!("unexpected SQL frame: {other:?}"),
				}
			};
			drop(stream);
			client.close();
			outcome
		})))
	}
	pub async fn outcome(mut self) -> WireOutcome {
		let outcome = tokio::time::timeout(BOUND, self.0.as_mut().unwrap())
			.await
			.expect("SQL response exceeded safety bound")
			.expect("SQL request task failed");
		self.0.take();
		outcome
	}
}
impl Drop for SqlRequest {
	fn drop(&mut self) {
		if let Some(task) = self.0.take() {
			task.abort();
		}
	}
}

pub(super) fn assert_query_timeout_audit(path: &std::path::Path, query_id: QueryId) {
	// Observed-log check after producer shutdown; the detached sink queue is not proven drained.
	// Held execution and the selected listener timer path support the no-Completed assertion.
	let body = std::fs::read_to_string(path).expect("timeout audit file");
	let query = query_id.get().to_string();
	let records: Vec<serde_json::Value> = body
		.lines()
		.map(|line| serde_json::from_str(line).expect("audit JSON"))
		.filter(|event: &serde_json::Value| {
			event["parameters"]["query_id"].as_str() == Some(query.as_str())
		})
		.collect();
	let timeouts: Vec<_> = records
		.iter()
		.filter(|event| event["operation"] == "toron.sql.query.timed_out")
		.collect();
	assert_eq!(
		timeouts.len(),
		1,
		"exactly one timeout audit for {query}: {records:?}"
	);
	assert_eq!(
		timeouts[0]["parameters"]["sql_error_code_class"],
		"statement_timeout"
	);
	for event in records {
		assert_ne!(event["operation"], "toron.sql.query.planned_error");
		assert_ne!(event["operation"], "toron.sql.query.completed");
		assert_ne!(
			event["parameters"]["sql_error_code_class"],
			"planning_error"
		);
	}
}
