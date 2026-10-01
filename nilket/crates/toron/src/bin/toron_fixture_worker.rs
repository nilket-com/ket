use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use toron::distributed::identity::WorkerId;
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::worker_registry::WorkerEndpoint;
use toron::error::StorageError;
use toron::ir::TableId;
use toron::quic::QuicEndpoint;
use toron::server::ToronWorker;
use toron::server::memory::MemoryManager;
use toron::server::transport::{QuicTransport, TcpTransport, ToronTransport};
use toron::storage::{ScanRequest, ScanStream, StorageScan};

#[derive(Clone, Copy, PartialEq, Eq)]
enum FixtureTransport {
	Tcp,
	/// Serves tasks and partition pulls on a loopback QUIC endpoint only — no TCP listener — and
	/// pulls its own shuffle inputs over QUIC.
	Quic,
}

/// Host mode: authenticated QUIC (mTLS) on a host address, for multinode runs. The worker's
/// listener certificate is both its server identity and its peer-client identity.
struct HostMode {
	bind: std::net::SocketAddr,
	advertise: std::net::SocketAddr,
	listener_cert: std::path::PathBuf,
	listener_key: std::path::PathBuf,
	peer_ca: std::path::PathBuf,
	cluster_domain: String,
}

/// One timestamped line on stderr: receiver-side evidence for a multinode attempt.
fn host_event(event: &str, detail: impl std::fmt::Display) {
	let unix_ms = std::time::SystemTime::now()
		.duration_since(std::time::UNIX_EPOCH)
		.map(|elapsed| elapsed.as_millis())
		.unwrap_or(0);
	eprintln!("{event} unix_ms={unix_ms} {detail}");
}

/// Test-only evidence: appends one `<this worker> <producer worker> <producer endpoint>` line per
/// sealed-pull dial, then dials through the real transport unchanged.
struct DialRecordingTransport {
	inner: Arc<dyn ToronTransport>,
	this_worker: WorkerId,
	path: std::path::PathBuf,
}

#[async_trait::async_trait]
impl ToronTransport for DialRecordingTransport {
	async fn connect(
		&self,
		address: &str,
	) -> Result<Box<dyn toron::server::transport::ToronStream>, toron::error::QueryError> {
		self.inner.connect(address).await
	}

	async fn connect_peer(
		&self,
		worker_id: &WorkerId,
		endpoint: &WorkerEndpoint,
	) -> Result<Box<dyn toron::server::transport::ToronStream>, toron::error::QueryError> {
		use std::io::Write as _;
		let line = format!(
			"{} {} {}\n",
			self.this_worker.get().as_u128(),
			worker_id.get().as_u128(),
			endpoint.as_str()
		);
		std::fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&self.path)
			.and_then(|mut file| file.write_all(line.as_bytes()))
			.map_err(|error| toron::error::QueryError::Internal {
				message: format!("dial record failed: {error}"),
			})?;
		self.inner.connect_peer(worker_id, endpoint).await
	}
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let (worker_id, transport, record_peer_dials, host_mode) = args()?;
	let peer = |inner: Arc<dyn ToronTransport>| -> Arc<dyn ToronTransport> {
		match &record_peer_dials {
			Some(path) => Arc::new(DialRecordingTransport {
				inner,
				this_worker: worker_id,
				path: path.clone(),
			}),
			None => inner,
		}
	};
	let worker = |endpoint: WorkerEndpoint, peer: Arc<dyn ToronTransport>| {
		Ok::<_, Box<dyn Error>>(Arc::new(
			ToronWorker::with_partition_store_and_endpoint(
				Arc::new(FixtureStorageScan::new()?),
				Arc::new(MemoryManager::new(32, 1024 * 1024)),
				Arc::new(InMemoryPartitionStore::new()),
				worker_id,
				endpoint,
			)
			.with_peer_transport(peer),
		))
	};
	if let Some(host) = host_mode {
		let policy = match toron::network::tls::ListenerPolicy::load_from_paths(
			&host.listener_cert,
			&host.listener_key,
			&host.peer_ca,
			&host.cluster_domain,
			&worker_id,
		) {
			Ok(policy) => policy,
			Err(error) => {
				host_event("STARTUP_REJECTED", format!("error={error}"));
				return Err(error.into());
			}
		};
		let quic = QuicEndpoint::bind_authenticated(
			host.bind,
			policy.server_config_for(toron::network::tls::ListenerRole::WorkerTask),
		)?;
		let endpoint = WorkerEndpoint::new(host.advertise.to_string());
		let worker = worker(
			endpoint.clone(),
			peer(Arc::new(QuicTransport::authenticated(
				policy.client_config(),
				policy.cluster_domain(),
			)?)),
		)?;
		host_event(
			"LISTENING",
			format!("bind={} advertise={}", host.bind, host.advertise),
		);
		announce(&endpoint).await?;
		loop {
			let connection = match quic.accept_connection().await {
				Ok(connection) => connection,
				Err(error) => {
					host_event("HANDSHAKE_REJECTED", format!("error={error}"));
					continue;
				}
			};
			let remote = connection.remote_address();
			let worker = worker.clone();
			tokio::spawn(async move {
				match QuicEndpoint::accept_stream_on(&connection).await {
					Ok(stream) => {
						host_event("STREAM_ACCEPTED", format!("remote={remote}"));
						if let Err(error) = worker.handle_connection(stream).await {
							host_event(
								"CONNECTION_FAILED",
								format!("remote={remote} error={error}"),
							);
						}
					}
					Err(error) => {
						host_event("STREAM_FAILED", format!("remote={remote} error={error}"))
					}
				}
			});
		}
	}
	match transport {
		FixtureTransport::Tcp => {
			let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
			let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
			let worker = worker(endpoint.clone(), peer(Arc::new(TcpTransport::new())))?;
			announce(&endpoint).await?;
			loop {
				let (stream, _) = listener.accept().await?;
				let worker = worker.clone();
				tokio::spawn(async move {
					if let Err(error) = worker.handle_connection(stream).await {
						eprintln!("fixture worker connection failed: {error}");
					}
				});
			}
		}
		FixtureTransport::Quic => {
			let quic = QuicEndpoint::bind_loopback("127.0.0.1:0".parse()?)?;
			let endpoint = WorkerEndpoint::new(quic.local_addr()?.to_string());
			let worker = worker(endpoint.clone(), peer(Arc::new(QuicTransport::new()?)))?;
			announce(&endpoint).await?;
			loop {
				let connection = match quic.accept_connection().await {
					Ok(connection) => connection,
					Err(error) => {
						eprintln!("fixture worker QUIC handshake failed: {error}");
						continue;
					}
				};
				let worker = worker.clone();
				tokio::spawn(async move {
					match QuicEndpoint::accept_stream_on(&connection).await {
						Ok(stream) => {
							if let Err(error) = worker.handle_connection(stream).await {
								eprintln!("fixture worker connection failed: {error}");
							}
						}
						Err(error) => eprintln!("fixture worker QUIC stream failed: {error}"),
					}
				});
			}
		}
	}
}

async fn announce(endpoint: &WorkerEndpoint) -> Result<(), Box<dyn Error>> {
	let mut stdout = tokio::io::stdout();
	stdout
		.write_all(format!("READY {}\n", endpoint.as_str()).as_bytes())
		.await?;
	stdout.flush().await?;
	Ok(())
}

struct FixtureStorageScan {
	tables: HashMap<TableId, RecordBatch>,
}

impl FixtureStorageScan {
	fn new() -> Result<Self, arrow::error::ArrowError> {
		let vals_schema = Arc::new(Schema::new(vec![
			Field::new("val", DataType::Int32, false),
			Field::new("float_val", DataType::Float64, true),
			Field::new("category", DataType::Utf8, true),
			Field::new("sample", DataType::Float64, true),
		]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]));
		let set_left_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("set_key", DataType::Int64, true),
		]));
		let set_right_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("set_key", DataType::Int64, true),
		]));
		let vals = RecordBatch::try_new(
			vals_schema,
			vec![
				Arc::new(Int32Array::from((0..16).collect::<Vec<i32>>())),
				Arc::new(Float64Array::from(
					(0..16)
						.map(|row| Some(row as f64 + 0.25))
						.collect::<Vec<_>>(),
				)),
				Arc::new(StringArray::from(
					(0..16)
						.map(|row| Some(if row < 9 { "fence" } else { "zero" }))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Float64Array::from(vec![
					Some(-6.0),
					Some(-1.0),
					Some(0.0),
					Some(1.0),
					Some(2.0),
					Some(3.0),
					Some(4.0),
					Some(5.0),
					Some(10.0),
					Some(0.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(10.0),
				])),
			],
		)?;
		let orders = RecordBatch::try_new(
			orders_schema,
			vec![
				Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
				Arc::new(Int64Array::from(vec![1, 2, 2, 3])),
			],
		)?;
		// ADR 233 cardinal: both matched customers sit in "west", but their ids hash to different
		// join partitions (cust 2 -> p0, cust 3 -> p3 under XxHash3_64 % 4), so "west"'s rows are
		// produced by two join workers; its SUM/COUNT must merge partials across producers at the shuffle.
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from(vec![2, 3])),
				Arc::new(StringArray::from(vec![Some("west"), Some("west")])),
			],
		)?;
		let set_left = RecordBatch::try_new(
			set_left_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
				Arc::new(Int64Array::from(vec![Some(7), None, Some(9), None])),
			],
		)?;
		let set_right = RecordBatch::try_new(
			set_right_schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 11, 12])),
				Arc::new(Int64Array::from(vec![None, Some(9), Some(10)])),
			],
		)?;

		Ok(Self {
			tables: HashMap::from([
				(TableId(1), vals),
				(TableId(2), orders),
				(TableId(3), customers),
				(TableId(4), set_left),
				(TableId(5), set_right),
			]),
		})
	}
}

#[async_trait::async_trait]
impl StorageScan for FixtureStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.tables
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;
		// ADR 238: honor a pushed predicate in the source-column domain (before projection),
		// reusing the production PBC evaluator — the same `wendelon::schema::pbc::execute` the real
		// in-memory scan uses (storage.rs). Fail loud on an unevaluable program; never return
		// silently-unfiltered rows.
		if let Some(pbc) = &request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}
		if let Some((morsel_id, total_morsels)) = request.morsel {
			let total_rows = batch.num_rows();
			let rows_per_morsel =
				(total_rows + total_morsels as usize - 1) / total_morsels as usize;
			let start = morsel_id.0 as usize * rows_per_morsel;
			let end = std::cmp::min(start + rows_per_morsel, total_rows);
			batch = if start < total_rows {
				batch.slice(start, end - start)
			} else {
				batch.slice(0, 0)
			};
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
}

type FixtureArgs = (
	WorkerId,
	FixtureTransport,
	Option<std::path::PathBuf>,
	Option<HostMode>,
);

fn invalid(message: String) -> Box<dyn Error> {
	Box::new(std::io::Error::new(
		std::io::ErrorKind::InvalidInput,
		message,
	))
}

fn args() -> Result<FixtureArgs, Box<dyn Error>> {
	let mut args = std::env::args().skip(1);
	let mut worker_u128 = None;
	let mut transport = FixtureTransport::Tcp;
	let mut record_peer_dials = None;
	let mut bind: Option<std::net::SocketAddr> = None;
	let mut advertise: Option<std::net::SocketAddr> = None;
	let mut tls_cert = None;
	let mut tls_key = None;
	let mut tls_ca = None;
	let mut cluster_domain = None;
	while let Some(arg) = args.next() {
		if arg == "--worker-u128" {
			worker_u128 = args.next().map(|value| value.parse::<u128>()).transpose()?;
		} else if arg == "--bind" {
			bind = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--advertise" {
			advertise = args.next().map(|value| value.parse()).transpose()?;
		} else if arg == "--tls-cert" {
			tls_cert = args.next().map(std::path::PathBuf::from);
		} else if arg == "--tls-key" {
			tls_key = args.next().map(std::path::PathBuf::from);
		} else if arg == "--tls-ca" {
			tls_ca = args.next().map(std::path::PathBuf::from);
		} else if arg == "--cluster-domain" {
			cluster_domain = args.next();
		} else if arg == "--record-peer-dials" {
			record_peer_dials = args.next().map(std::path::PathBuf::from);
		} else if arg == "--transport" {
			transport = match args.next().as_deref() {
				Some("tcp") => FixtureTransport::Tcp,
				Some("quic") => FixtureTransport::Quic,
				other => {
					return Err(Box::new(std::io::Error::new(
						std::io::ErrorKind::InvalidInput,
						format!("--transport must be tcp or quic, got {other:?}"),
					)));
				}
			};
		}
	}
	let worker_u128 = worker_u128.ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"missing required --worker-u128 argument",
		)
	})?;
	let host_mode = host_mode(bind, advertise, tls_cert, tls_key, tls_ca, cluster_domain)?;
	Ok((
		WorkerId::from_u128(worker_u128),
		transport,
		record_peer_dials,
		host_mode,
	))
}

/// Host mode needs every host flag or none. It refuses a loopback or unspecified advertised
/// address: a loopback endpoint would take the unauthenticated loopback QUIC path silently.
fn host_mode(
	bind: Option<std::net::SocketAddr>,
	advertise: Option<std::net::SocketAddr>,
	tls_cert: Option<std::path::PathBuf>,
	tls_key: Option<std::path::PathBuf>,
	tls_ca: Option<std::path::PathBuf>,
	cluster_domain: Option<String>,
) -> Result<Option<HostMode>, Box<dyn Error>> {
	match (bind, advertise, tls_cert, tls_key, tls_ca, cluster_domain) {
		(None, None, None, None, None, None) => Ok(None),
		(Some(bind), Some(advertise), Some(listener_cert), Some(listener_key), Some(peer_ca), Some(cluster_domain)) => {
			if advertise.ip().is_loopback() || advertise.ip().is_unspecified() || advertise.port() == 0 {
				return Err(invalid(format!(
					"--advertise must be a routable host address and port, got {advertise}"
				)));
			}
			Ok(Some(HostMode {
				bind,
				advertise,
				listener_cert,
				listener_key,
				peer_ca,
				cluster_domain,
			}))
		}
		_ => Err(invalid(
			"host mode needs --bind, --advertise, --tls-cert, --tls-key, --tls-ca and --cluster-domain together"
				.to_string(),
		)),
	}
}
