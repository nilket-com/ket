use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::time::Duration;
use tracing::Dispatch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;

pub const TRACE_SAMPLING_RATE_DENOMINATOR: u32 = 1_000_000;
const DEFAULT_LOG_FILTER: &str = "info";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TraceExportProtocol {
	HttpProtobuf,
	Grpc,
}

impl TraceExportProtocol {
	pub fn as_config_value(self) -> &'static str {
		match self {
			Self::HttpProtobuf => "http/protobuf",
			Self::Grpc => "grpc",
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TraceExportConfig {
	pub endpoint: String,
	pub protocol: TraceExportProtocol,
	pub sampling_rate_per_million: u32,
	pub client_cert_path: PathBuf,
	pub client_key_path: PathBuf,
	pub ca_bundle_path: PathBuf,
	pub max_queue_size: usize,
	pub export_timeout: Duration,
}

impl TraceExportConfig {
	pub fn validates_sampling_rate(&self) -> bool {
		self.sampling_rate_per_million <= TRACE_SAMPLING_RATE_DENOMINATOR
	}
}

pub struct TelemetryHandle {
	#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
	shutdown: Option<otlp::OtlpShutdownHandle>,
}

impl TelemetryHandle {
	pub async fn shutdown(self) -> std::io::Result<()> {
		#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
		if let Some(handle) = self.shutdown {
			return handle.shutdown().await;
		}
		Ok(())
	}
}

pub fn build_dispatch(
	trace_cfg: Option<&TraceExportConfig>,
	local_node_id: u32,
) -> std::io::Result<(Dispatch, TelemetryHandle)> {
	let filter = build_env_filter()?;
	let fmt_layer = tracing_subscriber::fmt::layer().with_target(true);
	match trace_cfg {
		Some(cfg) => match cfg.protocol {
			TraceExportProtocol::Grpc => {
				#[cfg(feature = "otlp-grpc")]
				{
					let (layer, shutdown) = otlp::OtlpTraceLayer::new(cfg.clone(), local_node_id)?;
					let dispatch = Dispatch::new(
						tracing_subscriber::registry()
							.with(filter)
							.with(fmt_layer)
							.with(layer),
					);
					Ok((
						dispatch,
						TelemetryHandle {
							shutdown: Some(shutdown),
						},
					))
				}
				#[cfg(not(feature = "otlp-grpc"))]
				{
					let _ = local_node_id;
					Err(Error::new(
						ErrorKind::Unsupported,
						"observability.traces.protocol=grpc requires the otlp-grpc cargo feature",
					))
				}
			}
			TraceExportProtocol::HttpProtobuf => {
				#[cfg(feature = "otlp-http")]
				{
					let (layer, shutdown) = otlp::OtlpTraceLayer::new(cfg.clone(), local_node_id)?;
					let dispatch = Dispatch::new(
						tracing_subscriber::registry()
							.with(filter)
							.with(fmt_layer)
							.with(layer),
					);
					Ok((
						dispatch,
						TelemetryHandle {
							shutdown: Some(shutdown),
						},
					))
				}
				#[cfg(not(feature = "otlp-http"))]
				{
					let _ = local_node_id;
					Err(Error::new(
						ErrorKind::Unsupported,
						"observability.traces.protocol=http/protobuf requires the otlp-http cargo feature",
					))
				}
			}
		},
		None => Ok((
			Dispatch::new(tracing_subscriber::registry().with(filter).with(fmt_layer)),
			TelemetryHandle {
				#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
				shutdown: None,
			},
		)),
	}
}

pub fn install_global(
	trace_cfg: Option<&TraceExportConfig>,
	local_node_id: u32,
) -> std::io::Result<TelemetryHandle> {
	let (dispatch, handle) = build_dispatch(trace_cfg, local_node_id)?;
	tracing::dispatcher::set_global_default(dispatch).map_err(|err| {
		Error::new(
			ErrorKind::AlreadyExists,
			format!("global tracing subscriber already installed: {err}"),
		)
	})?;
	Ok(handle)
}

fn build_env_filter() -> std::io::Result<EnvFilter> {
	EnvFilter::try_from_env("WENDELON_LOG")
		.or_else(|_| EnvFilter::try_new(DEFAULT_LOG_FILTER))
		.map_err(|err| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("invalid WENDELON_LOG filter: {err}"),
			)
		})
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
mod otlp {
	use super::{TRACE_SAMPLING_RATE_DENOMINATOR, TraceExportConfig, TraceExportProtocol};
	use crate::metrics::global as metrics;
	#[cfg(feature = "otlp-grpc")]
	use crate::network::tls::{TlsMaterial, rustls_client_config_with_alpn};
	#[cfg(feature = "otlp-grpc")]
	use bytes::Bytes;
	#[cfg(feature = "otlp-grpc")]
	use http::{Request, Uri};
	#[cfg(feature = "otlp-http")]
	use reqwest::{Certificate, Client as HttpClient, Identity, Url};
	#[cfg(feature = "otlp-grpc")]
	use rustls_pki_types::ServerName;
	use std::collections::HashMap;
	use std::io::{Error, ErrorKind};
	#[cfg(feature = "otlp-grpc")]
	use std::sync::Arc;
	use std::sync::atomic::{AtomicU64, Ordering};
	use std::time::{Duration, SystemTime, UNIX_EPOCH};
	#[cfg(feature = "otlp-grpc")]
	use tokio::net::TcpStream;
	#[cfg(feature = "otlp-grpc")]
	use tokio::sync::Mutex;
	use tokio::sync::{mpsc, oneshot};
	use tokio::task::JoinHandle;
	#[cfg(feature = "otlp-grpc")]
	use tokio_rustls::TlsConnector;
	use tracing::field::{Field, Visit};
	use tracing::span::{Attributes, Id, Record};
	use tracing::{Event, Subscriber};
	use tracing_subscriber::layer::{Context, Layer};
	use tracing_subscriber::registry::LookupSpan;

	#[cfg(feature = "otlp-http")]
	const OTLP_HTTP_TRACES_PATH: &str = "/v1/traces";
	#[cfg(feature = "otlp-grpc")]
	const OTLP_GRPC_TRACES_PATH: &str =
		"/opentelemetry.proto.collector.trace.v1.TraceService/Export";
	const OTLP_SCOPE_NAME: &str = "wendelon.traces";
	const OTLP_DROP_WARN_INTERVAL: Duration = Duration::from_secs(60);

	#[derive(Clone, Copy)]
	enum OtlpTransport {
		#[cfg(feature = "otlp-http")]
		HttpProtobuf,
		#[cfg(feature = "otlp-grpc")]
		Grpc,
	}

	enum OtlpClient {
		#[cfg(feature = "otlp-http")]
		Http { client: HttpClient, endpoint: Url },
		#[cfg(feature = "otlp-grpc")]
		Grpc(GrpcClient),
	}

	#[cfg(feature = "otlp-grpc")]
	struct GrpcClient {
		endpoint: GrpcEndpoint,
		connector: TlsConnector,
		connection: Mutex<Option<GrpcConnection>>,
	}

	#[cfg(feature = "otlp-grpc")]
	struct GrpcEndpoint {
		uri: Uri,
		connect_addr: String,
		server_name: String,
	}

	#[cfg(feature = "otlp-grpc")]
	struct GrpcConnection {
		sender: Option<h2::client::SendRequest<Bytes>>,
		driver: JoinHandle<()>,
	}

	pub struct OtlpShutdownHandle {
		tx: mpsc::Sender<ExportCommand>,
		task: JoinHandle<()>,
	}

	impl OtlpShutdownHandle {
		pub async fn shutdown(self) -> std::io::Result<()> {
			let (ack_tx, ack_rx) = oneshot::channel();
			let _ = self.tx.send(ExportCommand::Shutdown(ack_tx)).await;
			let _ = ack_rx.await;
			match self.task.await {
				Ok(()) => Ok(()),
				Err(err) if err.is_cancelled() => Ok(()),
				Err(err) => Err(Error::other(format!(
					"otlp export task failed during shutdown: {err}",
				))),
			}
		}
	}

	pub struct OtlpTraceLayer {
		tx: mpsc::Sender<ExportCommand>,
		sampling_rate_per_million: u32,
		queue_capacity: usize,
		local_node_id: u32,
		next_trace_seq: AtomicU64,
		next_span_seq: AtomicU64,
		last_drop_warn_unix_nanos: AtomicU64,
	}

	impl OtlpTraceLayer {
		pub fn new(
			cfg: TraceExportConfig,
			local_node_id: u32,
		) -> std::io::Result<(Self, OtlpShutdownHandle)> {
			let transport = OtlpTransport::from_config(cfg.protocol)?;
			let client = build_client(&cfg, transport)?;
			let (tx, rx) = mpsc::channel(cfg.max_queue_size);
			let task = tokio::spawn(run_export_loop(
				rx,
				client,
				local_node_id,
				cfg.export_timeout,
			));
			Ok((
				Self {
					tx: tx.clone(),
					sampling_rate_per_million: cfg.sampling_rate_per_million,
					queue_capacity: cfg.max_queue_size,
					local_node_id,
					next_trace_seq: AtomicU64::new(1),
					next_span_seq: AtomicU64::new(1),
					last_drop_warn_unix_nanos: AtomicU64::new(0),
				},
				OtlpShutdownHandle { tx, task },
			))
		}

		fn should_sample_root_span(&self) -> bool {
			match self.sampling_rate_per_million {
				0 => false,
				TRACE_SAMPLING_RATE_DENOMINATOR => true,
				threshold => rand::random::<u32>() % TRACE_SAMPLING_RATE_DENOMINATOR < threshold,
			}
		}

		fn next_trace_id(&self) -> [u8; 16] {
			debug_assert_ne!(
				self.local_node_id, 0,
				"OTLP trace IDs rely on validated node_id > 0",
			);
			let seq = self
				.next_trace_seq
				.fetch_add(1, Ordering::Relaxed)
				.to_be_bytes();
			let mut out = [0u8; 16];
			out[..4].copy_from_slice(&self.local_node_id.to_be_bytes());
			out[4..8].copy_from_slice(&std::process::id().to_be_bytes());
			out[8..].copy_from_slice(&seq);
			out
		}

		fn next_span_id(&self) -> [u8; 8] {
			self.next_span_seq
				.fetch_add(1, Ordering::Relaxed)
				.to_be_bytes()
		}

		fn note_dropped_span(&self, saturated: bool) {
			metrics().otlp_spans_dropped_total.inc();
			if saturated
				&& should_emit_rate_limited_warning(
					&self.last_drop_warn_unix_nanos,
					unix_time_nanos(),
					OTLP_DROP_WARN_INTERVAL,
				) {
				tracing::warn!(
					queue_capacity = self.queue_capacity,
					dropped_total = metrics().otlp_spans_dropped_total.get(),
					"otlp span export queue saturated; dropping sampled spans",
				);
			}
		}
	}

	fn should_emit_rate_limited_warning(
		last_warn_unix_nanos: &AtomicU64,
		now_unix_nanos: u64,
		interval: Duration,
	) -> bool {
		let interval_nanos = interval.as_nanos().min(u128::from(u64::MAX)) as u64;
		let mut observed = last_warn_unix_nanos.load(Ordering::Relaxed);
		loop {
			if observed != 0 && now_unix_nanos.saturating_sub(observed) < interval_nanos {
				return false;
			}
			match last_warn_unix_nanos.compare_exchange(
				observed,
				now_unix_nanos,
				Ordering::Relaxed,
				Ordering::Relaxed,
			) {
				Ok(_) => return true,
				Err(actual) => observed = actual,
			}
		}
	}

	impl OtlpTransport {
		fn from_config(protocol: TraceExportProtocol) -> std::io::Result<Self> {
			match protocol {
				TraceExportProtocol::HttpProtobuf => {
					#[cfg(feature = "otlp-http")]
					{
						Ok(Self::HttpProtobuf)
					}
					#[cfg(not(feature = "otlp-http"))]
					{
						Err(Error::new(
							ErrorKind::Unsupported,
							"observability.traces.protocol=http/protobuf requires the otlp-http cargo feature",
						))
					}
				}
				TraceExportProtocol::Grpc => {
					#[cfg(feature = "otlp-grpc")]
					{
						Ok(Self::Grpc)
					}
					#[cfg(not(feature = "otlp-grpc"))]
					{
						Err(Error::new(
							ErrorKind::Unsupported,
							"observability.traces.protocol=grpc requires the otlp-grpc cargo feature",
						))
					}
				}
			}
		}
	}

	impl<S> Layer<S> for OtlpTraceLayer
	where
		S: Subscriber + for<'lookup> LookupSpan<'lookup>,
	{
		fn on_new_span(&self, attrs: &Attributes<'_>, id: &Id, ctx: Context<'_, S>) {
			let metadata = attrs.metadata();
			let parent = attrs
				.parent()
				.and_then(|parent| trace_context_for_span(ctx.span(parent)))
				.or_else(|| {
					if attrs.is_contextual() {
						trace_context_for_span(ctx.lookup_current())
					} else {
						None
					}
				});
			let sampled = parent
				.as_ref()
				.map(|parent| parent.sampled)
				.unwrap_or_else(|| self.should_sample_root_span());
			let trace_id = parent
				.as_ref()
				.map(|parent| parent.trace_id)
				.unwrap_or_else(|| self.next_trace_id());
			let parent_span_id = parent.as_ref().map(|parent| parent.span_id);
			let span_id = self.next_span_id();
			let mut visitor = AttributeVisitor::default();
			attrs.record(&mut visitor);
			let mut attributes = visitor.finish_attributes();
			attributes.push(KeyValue::string("tracing.level", metadata.level().as_str()));
			attributes.push(KeyValue::string("tracing.target", metadata.target()));
			let state = SpanState {
				trace_id,
				span_id,
				parent_span_id,
				name: metadata.name().to_string(),
				start_time_unix_nanos: unix_time_nanos(),
				attributes,
				events: Vec::new(),
				sampled,
				status_code: SpanStatusCode::Unset,
			};
			if let Some(span) = ctx.span(id) {
				span.extensions_mut().insert(state);
			}
		}

		fn on_record(&self, id: &Id, values: &Record<'_>, ctx: Context<'_, S>) {
			let Some(span) = ctx.span(id) else {
				return;
			};
			let mut extensions = span.extensions_mut();
			let Some(state) = extensions.get_mut::<SpanState>() else {
				return;
			};
			if !state.sampled {
				return;
			}
			let mut visitor = AttributeVisitor::default();
			values.record(&mut visitor);
			state.attributes.extend(visitor.finish_attributes());
		}

		fn on_event(&self, event: &Event<'_>, ctx: Context<'_, S>) {
			let Some(span) = ctx.lookup_current() else {
				return;
			};
			let mut extensions = span.extensions_mut();
			let Some(state) = extensions.get_mut::<SpanState>() else {
				return;
			};
			if !state.sampled {
				return;
			}
			let metadata = event.metadata();
			let mut visitor = AttributeVisitor::default();
			event.record(&mut visitor);
			let event_name = visitor
				.message
				.clone()
				.unwrap_or_else(|| metadata.name().to_string());
			let mut attributes = visitor.finish_attributes();
			attributes.push(KeyValue::string("tracing.level", metadata.level().as_str()));
			attributes.push(KeyValue::string("tracing.target", metadata.target()));
			if metadata.level() == &tracing::Level::ERROR {
				state.status_code = SpanStatusCode::Error;
			}
			state.events.push(SpanEvent {
				time_unix_nanos: unix_time_nanos(),
				name: event_name,
				attributes,
			});
		}

		fn on_close(&self, id: Id, ctx: Context<'_, S>) {
			let Some(span) = ctx.span(&id) else {
				return;
			};
			let Some(state) = span.extensions_mut().remove::<SpanState>() else {
				return;
			};
			if !state.sampled {
				return;
			}
			let completed = CompletedSpan {
				trace_id: state.trace_id,
				span_id: state.span_id,
				parent_span_id: state.parent_span_id,
				name: state.name,
				start_time_unix_nanos: state.start_time_unix_nanos,
				end_time_unix_nanos: unix_time_nanos(),
				attributes: state.attributes,
				events: state.events,
				status_code: state.status_code,
			};
			match self.tx.try_send(ExportCommand::Span(completed)) {
				Ok(()) => {}
				Err(mpsc::error::TrySendError::Full(_)) => self.note_dropped_span(true),
				Err(mpsc::error::TrySendError::Closed(_)) => self.note_dropped_span(false),
			}
		}
	}

	fn trace_context_for_span<S>(
		span: Option<tracing_subscriber::registry::SpanRef<'_, S>>,
	) -> Option<TraceContext>
	where
		S: Subscriber + for<'lookup> LookupSpan<'lookup>,
	{
		let span = span?;
		let extensions = span.extensions();
		let state = extensions.get::<SpanState>()?;
		Some(TraceContext {
			trace_id: state.trace_id,
			span_id: state.span_id,
			sampled: state.sampled,
		})
	}

	async fn run_export_loop(
		mut rx: mpsc::Receiver<ExportCommand>,
		client: OtlpClient,
		local_node_id: u32,
		export_timeout: Duration,
	) {
		while let Some(command) = rx.recv().await {
			match command {
				ExportCommand::Span(first) => {
					let mut batch = vec![first];
					while batch.len() < 128 {
						match tokio::time::timeout(Duration::from_millis(25), rx.recv()).await {
							Ok(Some(ExportCommand::Span(span))) => batch.push(span),
							Ok(Some(ExportCommand::Shutdown(ack))) => {
								let _ = flush_batch(&client, local_node_id, &batch).await;
								shutdown_client(&client).await;
								let _ = ack.send(());
								return;
							}
							Ok(None) => {
								let _ = flush_batch(&client, local_node_id, &batch).await;
								shutdown_client(&client).await;
								return;
							}
							Err(_) => break,
						}
					}
					match tokio::time::timeout(
						export_timeout,
						flush_batch(&client, local_node_id, &batch),
					)
					.await
					{
						Err(err) => {
							eprintln!("wendelon: otlp export timed out: {err}");
							shutdown_client(&client).await;
						}
						Ok(Err(err)) => eprintln!("wendelon: otlp export failed: {err}"),
						Ok(Ok(())) => {}
					}
				}
				ExportCommand::Shutdown(ack) => {
					shutdown_client(&client).await;
					let _ = ack.send(());
					return;
				}
			}
		}
		shutdown_client(&client).await;
	}

	async fn flush_batch(
		client: &OtlpClient,
		local_node_id: u32,
		spans: &[CompletedSpan],
	) -> std::io::Result<()> {
		match client {
			#[cfg(feature = "otlp-http")]
			OtlpClient::Http { client, endpoint } => {
				flush_http_batch(client, endpoint, local_node_id, spans).await?
			}
			#[cfg(feature = "otlp-grpc")]
			OtlpClient::Grpc(client) => flush_grpc_batch(client, local_node_id, spans).await?,
		}
		Ok(())
	}

	async fn shutdown_client(client: &OtlpClient) {
		match client {
			#[cfg(feature = "otlp-http")]
			OtlpClient::Http { .. } => {}
			#[cfg(feature = "otlp-grpc")]
			OtlpClient::Grpc(client) => shutdown_grpc_connection(client).await,
		}
	}

	fn build_client(
		cfg: &TraceExportConfig,
		transport: OtlpTransport,
	) -> std::io::Result<OtlpClient> {
		match transport {
			#[cfg(feature = "otlp-http")]
			OtlpTransport::HttpProtobuf => {
				#[cfg(feature = "otlp-http")]
				{
					Ok(OtlpClient::Http {
						client: build_http_client(cfg)?,
						endpoint: normalize_http_endpoint(&cfg.endpoint)?,
					})
				}
				#[cfg(not(feature = "otlp-http"))]
				{
					let _ = cfg;
					Err(Error::new(
						ErrorKind::Unsupported,
						"observability.traces.protocol=http/protobuf requires the otlp-http cargo feature",
					))
				}
			}
			#[cfg(feature = "otlp-grpc")]
			OtlpTransport::Grpc => {
				#[cfg(feature = "otlp-grpc")]
				{
					Ok(OtlpClient::Grpc(build_grpc_client(cfg)?))
				}
				#[cfg(not(feature = "otlp-grpc"))]
				{
					let _ = cfg;
					Err(Error::new(
						ErrorKind::Unsupported,
						"observability.traces.protocol=grpc requires the otlp-grpc cargo feature",
					))
				}
			}
		}
	}

	#[cfg(feature = "otlp-http")]
	fn build_http_client(cfg: &TraceExportConfig) -> std::io::Result<HttpClient> {
		let cert_pem = std::fs::read(&cfg.client_cert_path)?;
		let key_pem = std::fs::read(&cfg.client_key_path)?;
		let mut identity_pem = Vec::with_capacity(cert_pem.len() + key_pem.len() + 1);
		identity_pem.extend_from_slice(&cert_pem);
		if !identity_pem.ends_with(b"\n") {
			identity_pem.push(b'\n');
		}
		identity_pem.extend_from_slice(&key_pem);
		let identity = Identity::from_pem(&identity_pem).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("failed to load OTLP client identity PEM: {err}"),
			)
		})?;
		let ca_pem = std::fs::read(&cfg.ca_bundle_path)?;
		let ca = Certificate::from_pem(&ca_pem).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("failed to load OTLP CA bundle PEM: {err}"),
			)
		})?;
		HttpClient::builder()
			.use_rustls_tls()
			.http1_only()
			.identity(identity)
			.add_root_certificate(ca)
			.timeout(cfg.export_timeout)
			.build()
			.map_err(|err| Error::other(format!("failed to build OTLP HTTP client: {err}")))
	}

	#[cfg(feature = "otlp-http")]
	fn normalize_http_endpoint(endpoint: &str) -> std::io::Result<Url> {
		let mut url = Url::parse(endpoint).map_err(|err| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("invalid OTLP endpoint URL: {err}"),
			)
		})?;
		if url.path().is_empty() || url.path() == "/" {
			url.set_path(OTLP_HTTP_TRACES_PATH);
		}
		Ok(url)
	}

	#[cfg(feature = "otlp-http")]
	async fn flush_http_batch(
		client: &HttpClient,
		endpoint: &Url,
		local_node_id: u32,
		spans: &[CompletedSpan],
	) -> std::io::Result<()> {
		let payload = encode_export_trace_service_request(local_node_id, spans);
		let response = client
			.post(endpoint.clone())
			.header("content-type", "application/x-protobuf")
			.body(payload)
			.send()
			.await
			.map_err(|err| Error::other(format!("otlp export request failed: {err}")))?;
		response
			.error_for_status()
			.map_err(|err| Error::other(format!("otlp export failed: {err}")))?;
		Ok(())
	}

	#[cfg(feature = "otlp-grpc")]
	fn build_grpc_client(cfg: &TraceExportConfig) -> std::io::Result<GrpcClient> {
		let material = TlsMaterial::load(
			&cfg.client_cert_path,
			&cfg.client_key_path,
			&cfg.ca_bundle_path,
		)?;
		let client_config = rustls_client_config_with_alpn(&material, vec![b"h2".to_vec()])?;
		Ok(GrpcClient {
			endpoint: normalize_grpc_endpoint(&cfg.endpoint)?,
			connector: TlsConnector::from(Arc::new(client_config)),
			connection: Mutex::new(None),
		})
	}

	#[cfg(feature = "otlp-grpc")]
	async fn shutdown_grpc_connection(client: &GrpcClient) {
		let connection = client.connection.lock().await.take();
		close_grpc_connection(connection).await;
	}

	#[cfg(feature = "otlp-grpc")]
	async fn close_grpc_connection(connection: Option<GrpcConnection>) {
		if let Some(connection) = connection {
			connection.driver.abort();
			let _ = connection.driver.await;
		}
	}

	#[cfg(feature = "otlp-grpc")]
	async fn connect_grpc_connection(client: &GrpcClient) -> std::io::Result<GrpcConnection> {
		let stream = TcpStream::connect(&client.endpoint.connect_addr)
			.await
			.map_err(|err| Error::other(format!("otlp gRPC connect failed: {err}")))?;
		let server_name =
			ServerName::try_from(client.endpoint.server_name.clone()).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("invalid OTLP gRPC server name: {err}"),
				)
			})?;
		let tls = client
			.connector
			.connect(server_name, stream)
			.await
			.map_err(|err| Error::other(format!("otlp gRPC TLS handshake failed: {err}")))?;
		let (sender, connection) = h2::client::handshake(tls)
			.await
			.map_err(|err| Error::other(format!("otlp gRPC h2 handshake failed: {err}")))?;
		let driver = tokio::spawn(async move {
			let _ = connection.await;
		});
		Ok(GrpcConnection {
			sender: Some(sender),
			driver,
		})
	}

	#[cfg(feature = "otlp-grpc")]
	fn normalize_grpc_endpoint(endpoint: &str) -> std::io::Result<GrpcEndpoint> {
		let mut uri: Uri = endpoint.parse().map_err(|err| {
			Error::new(
				ErrorKind::InvalidInput,
				format!("invalid OTLP endpoint URI: {err}"),
			)
		})?;
		if uri.scheme_str() != Some("https") {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"OTLP gRPC endpoint must use https",
			));
		}
		if uri.path().is_empty() || uri.path() == "/" {
			let authority = uri.authority().ok_or_else(|| {
				Error::new(
					ErrorKind::InvalidInput,
					"OTLP gRPC endpoint is missing authority",
				)
			})?;
			uri = format!("https://{authority}{OTLP_GRPC_TRACES_PATH}")
				.parse()
				.map_err(|err| {
					Error::new(
						ErrorKind::InvalidInput,
						format!("invalid normalized OTLP gRPC endpoint: {err}"),
					)
				})?;
		}
		let host = uri.host().ok_or_else(|| {
			Error::new(
				ErrorKind::InvalidInput,
				"OTLP gRPC endpoint is missing host",
			)
		})?;
		let port = uri.port_u16().unwrap_or(443);
		Ok(GrpcEndpoint {
			connect_addr: format!("{host}:{port}"),
			server_name: host.to_string(),
			uri,
		})
	}

	#[cfg(feature = "otlp-grpc")]
	async fn flush_grpc_batch(
		client: &GrpcClient,
		local_node_id: u32,
		spans: &[CompletedSpan],
	) -> std::io::Result<()> {
		let payload = encode_export_trace_service_request(local_node_id, spans);
		let mut connection = client.connection.lock().await;
		if connection.is_none() {
			*connection = Some(connect_grpc_connection(client).await?);
		}
		let result = send_grpc_batch(
			connection
				.as_mut()
				.expect("gRPC connection must exist after initialization"),
			&client.endpoint.uri,
			&payload,
		)
		.await;
		if result.is_err() {
			let failed_connection = connection.take();
			drop(connection);
			close_grpc_connection(failed_connection).await;
		}
		result
	}

	#[cfg(feature = "otlp-grpc")]
	async fn send_grpc_batch(
		connection: &mut GrpcConnection,
		endpoint_uri: &Uri,
		payload: &[u8],
	) -> std::io::Result<()> {
		let sender = connection
			.sender
			.take()
			.expect("gRPC sender must be present while the connection is live");
		let mut sender = sender
			.ready()
			.await
			.map_err(|err| Error::other(format!("otlp gRPC send readiness failed: {err}")))?;
		let request = Request::builder()
			.method("POST")
			.uri(endpoint_uri.clone())
			.header("content-type", "application/grpc")
			.header("te", "trailers")
			.body(())
			.map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("gRPC request build failed: {err}"),
				)
			})?;
		let (response_future, mut send_stream) = sender
			.send_request(request, false)
			.map_err(|err| Error::other(format!("otlp gRPC send_request failed: {err}")))?;
		connection.sender = Some(sender);
		send_stream
			.send_data(Bytes::from(encode_grpc_message(&payload)), true)
			.map_err(|err| Error::other(format!("otlp gRPC send body failed: {err}")))?;
		let response = response_future
			.await
			.map_err(|err| Error::other(format!("otlp gRPC response failed: {err}")))?;
		if response.status() != http::StatusCode::OK {
			return Err(Error::other(format!(
				"otlp gRPC export failed with HTTP status {}",
				response.status()
			)));
		}
		let (parts, mut body) = response.into_parts();
		while let Some(chunk) = body.data().await {
			let chunk =
				chunk.map_err(|err| Error::other(format!("otlp gRPC body read failed: {err}")))?;
			body.flow_control()
				.release_capacity(chunk.len())
				.map_err(|err| {
					Error::other(format!("otlp gRPC flow-control release failed: {err}"))
				})?;
		}
		let trailers = body
			.trailers()
			.await
			.map_err(|err| Error::other(format!("otlp gRPC trailers read failed: {err}")))?;
		let status = grpc_status_from_headers(trailers.as_ref().unwrap_or(&parts.headers))?;
		if status == 0 {
			return Ok(());
		}
		Err(Error::other(format!(
			"otlp gRPC export failed with grpc-status={status}",
		)))
	}

	#[cfg(feature = "otlp-grpc")]
	fn grpc_status_from_headers(headers: &http::HeaderMap) -> std::io::Result<u32> {
		let raw = headers.get("grpc-status").ok_or_else(|| {
			Error::new(ErrorKind::InvalidData, "gRPC response missing grpc-status")
		})?;
		let raw = raw.to_str().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("invalid grpc-status header: {err}"),
			)
		})?;
		raw.parse::<u32>().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("invalid grpc-status value {raw:?}: {err}"),
			)
		})
	}

	#[cfg(feature = "otlp-grpc")]
	fn encode_grpc_message(message: &[u8]) -> Vec<u8> {
		let mut out = Vec::with_capacity(message.len() + 5);
		out.push(0);
		out.extend_from_slice(&(message.len() as u32).to_be_bytes());
		out.extend_from_slice(message);
		out
	}

	enum ExportCommand {
		Span(CompletedSpan),
		Shutdown(oneshot::Sender<()>),
	}

	#[derive(Clone, Copy)]
	struct TraceContext {
		trace_id: [u8; 16],
		span_id: [u8; 8],
		sampled: bool,
	}

	struct SpanState {
		trace_id: [u8; 16],
		span_id: [u8; 8],
		parent_span_id: Option<[u8; 8]>,
		name: String,
		start_time_unix_nanos: u64,
		attributes: Vec<KeyValue>,
		events: Vec<SpanEvent>,
		sampled: bool,
		status_code: SpanStatusCode,
	}

	struct CompletedSpan {
		trace_id: [u8; 16],
		span_id: [u8; 8],
		parent_span_id: Option<[u8; 8]>,
		name: String,
		start_time_unix_nanos: u64,
		end_time_unix_nanos: u64,
		attributes: Vec<KeyValue>,
		events: Vec<SpanEvent>,
		status_code: SpanStatusCode,
	}

	struct SpanEvent {
		time_unix_nanos: u64,
		name: String,
		attributes: Vec<KeyValue>,
	}

	#[derive(Clone)]
	struct KeyValue {
		key: String,
		value: Value,
	}

	impl KeyValue {
		fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
			Self {
				key: key.into(),
				value: Value::String(value.into()),
			}
		}

		fn integer(key: impl Into<String>, value: i64) -> Self {
			Self {
				key: key.into(),
				value: Value::Integer(value),
			}
		}
	}

	#[derive(Clone)]
	enum Value {
		String(String),
		Bool(bool),
		Integer(i64),
		Double(f64),
	}

	#[derive(Clone, Copy)]
	enum SpanStatusCode {
		Unset,
		Error,
	}

	#[derive(Default)]
	struct AttributeVisitor {
		attributes: HashMap<String, Value>,
		message: Option<String>,
	}

	impl AttributeVisitor {
		fn record_value(&mut self, field: &Field, value: Value) {
			if field.name() == "message" {
				if let Value::String(message) = &value {
					self.message = Some(message.clone());
				}
			}
			self.attributes.insert(field.name().to_string(), value);
		}

		fn finish_attributes(self) -> Vec<KeyValue> {
			self.attributes
				.into_iter()
				.map(|(key, value)| KeyValue { key, value })
				.collect()
		}
	}

	impl Visit for AttributeVisitor {
		fn record_i64(&mut self, field: &Field, value: i64) {
			self.record_value(field, Value::Integer(value));
		}

		fn record_u64(&mut self, field: &Field, value: u64) {
			match i64::try_from(value) {
				Ok(value) => self.record_value(field, Value::Integer(value)),
				Err(_) => self.record_value(field, Value::String(value.to_string())),
			}
		}

		fn record_bool(&mut self, field: &Field, value: bool) {
			self.record_value(field, Value::Bool(value));
		}

		fn record_str(&mut self, field: &Field, value: &str) {
			self.record_value(field, Value::String(value.to_string()));
		}

		fn record_f64(&mut self, field: &Field, value: f64) {
			if value.is_finite() {
				self.record_value(field, Value::Double(value));
			} else {
				self.record_value(field, Value::String(value.to_string()));
			}
		}

		fn record_debug(&mut self, field: &Field, value: &dyn std::fmt::Debug) {
			self.record_value(field, Value::String(format!("{value:?}")));
		}
	}

	fn unix_time_nanos() -> u64 {
		SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap_or_default()
			.as_nanos()
			.min(u64::MAX as u128) as u64
	}

	fn encode_export_trace_service_request(local_node_id: u32, spans: &[CompletedSpan]) -> Vec<u8> {
		let mut out = Vec::new();
		encode_message(&mut out, 1, |resource_spans| {
			encode_message(resource_spans, 1, |resource| {
				for attr in [
					KeyValue::string("service.name", "wendelon"),
					KeyValue::string("service.version", env!("CARGO_PKG_VERSION")),
					KeyValue::integer("wendelon.node_id", local_node_id as i64),
				] {
					encode_key_value(resource, 1, &attr);
				}
			});
			encode_message(resource_spans, 2, |scope_spans| {
				encode_message(scope_spans, 1, |scope| {
					encode_string(scope, 1, OTLP_SCOPE_NAME);
					encode_string(scope, 2, env!("CARGO_PKG_VERSION"));
				});
				for span in spans {
					encode_message(scope_spans, 2, |encoded| encode_span(encoded, span));
				}
			});
		});
		out
	}

	fn encode_span(out: &mut Vec<u8>, span: &CompletedSpan) {
		encode_bytes(out, 1, &span.trace_id);
		encode_bytes(out, 2, &span.span_id);
		if let Some(parent_span_id) = span.parent_span_id {
			encode_bytes(out, 4, &parent_span_id);
		}
		encode_string(out, 5, &span.name);
		encode_varint_field(out, 6, 1);
		encode_fixed64(out, 7, span.start_time_unix_nanos);
		encode_fixed64(out, 8, span.end_time_unix_nanos);
		for attr in &span.attributes {
			encode_key_value(out, 9, attr);
		}
		for event in &span.events {
			encode_message(out, 11, |encoded| encode_span_event(encoded, event));
		}
		if matches!(span.status_code, SpanStatusCode::Error) {
			encode_message(out, 15, |status| {
				encode_varint_field(status, 3, 2);
			});
		}
	}

	fn encode_span_event(out: &mut Vec<u8>, event: &SpanEvent) {
		encode_fixed64(out, 1, event.time_unix_nanos);
		encode_string(out, 2, &event.name);
		for attr in &event.attributes {
			encode_key_value(out, 3, attr);
		}
	}

	fn encode_key_value(out: &mut Vec<u8>, field_number: u32, key_value: &KeyValue) {
		encode_message(out, field_number, |entry| {
			encode_string(entry, 1, &key_value.key);
			encode_message(entry, 2, |value| encode_any_value(value, &key_value.value));
		});
	}

	fn encode_any_value(out: &mut Vec<u8>, value: &Value) {
		match value {
			Value::String(value) => encode_string(out, 1, value),
			Value::Bool(value) => encode_bool(out, 2, *value),
			Value::Integer(value) => encode_varint_field(out, 3, *value as u64),
			Value::Double(value) => encode_fixed64(out, 4, value.to_bits()),
		}
	}

	fn encode_message(out: &mut Vec<u8>, field_number: u32, f: impl FnOnce(&mut Vec<u8>)) {
		let mut inner = Vec::new();
		f(&mut inner);
		encode_bytes(out, field_number, &inner);
	}

	fn encode_string(out: &mut Vec<u8>, field_number: u32, value: &str) {
		encode_bytes(out, field_number, value.as_bytes());
	}

	fn encode_bytes(out: &mut Vec<u8>, field_number: u32, value: &[u8]) {
		encode_key(out, field_number, 2);
		encode_varint(out, value.len() as u64);
		out.extend_from_slice(value);
	}

	fn encode_bool(out: &mut Vec<u8>, field_number: u32, value: bool) {
		encode_varint_field(out, field_number, u64::from(value));
	}

	fn encode_varint_field(out: &mut Vec<u8>, field_number: u32, value: u64) {
		encode_key(out, field_number, 0);
		encode_varint(out, value);
	}

	fn encode_fixed64(out: &mut Vec<u8>, field_number: u32, value: u64) {
		encode_key(out, field_number, 1);
		out.extend_from_slice(&value.to_le_bytes());
	}

	fn encode_key(out: &mut Vec<u8>, field_number: u32, wire_type: u8) {
		encode_varint(out, ((field_number << 3) | wire_type as u32) as u64);
	}

	fn encode_varint(out: &mut Vec<u8>, mut value: u64) {
		while value >= 0x80 {
			out.push((value as u8 & 0x7F) | 0x80);
			value >>= 7;
		}
		out.push(value as u8);
	}

	#[cfg(test)]
	mod tests {
		use super::*;

		#[test]
		fn otlp_drop_warning_is_rate_limited() {
			let last_warn = AtomicU64::new(0);

			assert!(should_emit_rate_limited_warning(
				&last_warn,
				1_000,
				Duration::from_secs(60),
			));
			assert!(!should_emit_rate_limited_warning(
				&last_warn,
				2_000,
				Duration::from_secs(60),
			));
			assert!(should_emit_rate_limited_warning(
				&last_warn,
				60_000_000_000 + 1_000,
				Duration::from_secs(60),
			));
		}
	}
}
