//! Shared trace telemetry primitives and OTLP export mechanics.

use std::io::{Error, ErrorKind};
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

pub const TRACE_SAMPLING_RATE_DENOMINATOR: u32 = 1_000_000;
pub const TRACEPARENT_LEN: usize = 55;
pub const OTEL_TRACE_ID_FIELD: &str = "otel.trace_id";
pub const OTEL_SPAN_ID_FIELD: &str = "otel.span_id";
pub const OTEL_PARENT_SPAN_ID_FIELD: &str = "otel.parent_span_id";
pub const OTEL_SAMPLED_FIELD: &str = "otel.sampled";

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
pub use otlp::{OtlpShutdownHandle, OtlpTraceLayer};

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

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct TraceContext {
	pub traceparent: String,
	pub tracestate: Option<String>,
}

impl TraceContext {
	pub fn validate(&self) -> std::io::Result<()> {
		validate_traceparent(self.traceparent.as_str())?;
		validate_tracestate(self.tracestate.as_deref())?;
		Ok(())
	}
}

pub fn validate_traceparent(traceparent: &str) -> std::io::Result<()> {
	let bytes = traceparent.as_bytes();
	if bytes.len() != TRACEPARENT_LEN {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"traceparent must be exactly 55 bytes",
		));
	}
	if bytes[2] != b'-' || bytes[35] != b'-' || bytes[52] != b'-' {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"traceparent separators are invalid",
		));
	}
	let version = &traceparent[0..2];
	let trace_id = &traceparent[3..35];
	let span_id = &traceparent[36..52];
	let flags = &traceparent[53..55];
	if version != "00" {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"only W3C traceparent version 00 is supported",
		));
	}
	if !is_lower_hex(trace_id) || trace_id.bytes().all(|byte| byte == b'0') {
		return Err(Error::new(ErrorKind::InvalidData, "trace id is invalid"));
	}
	if !is_lower_hex(span_id) || span_id.bytes().all(|byte| byte == b'0') {
		return Err(Error::new(ErrorKind::InvalidData, "span id is invalid"));
	}
	if !is_lower_hex(flags) {
		return Err(Error::new(
			ErrorKind::InvalidData,
			"trace flags are invalid",
		));
	}
	Ok(())
}

pub fn validate_tracestate(tracestate: Option<&str>) -> std::io::Result<()> {
	if let Some(tracestate) = tracestate
		&& (tracestate.len() > 512 || tracestate.chars().any(char::is_control))
	{
		return Err(Error::new(
			ErrorKind::InvalidData,
			"tracestate is out of bounds",
		));
	}
	Ok(())
}

fn is_lower_hex(value: &str) -> bool {
	value
		.bytes()
		.all(|byte| byte.is_ascii_digit() || matches!(byte, b'a'..=b'f'))
}

#[derive(Clone)]
pub struct TelemetryProfile {
	pub local_node_id: u32,
	pub resource_attributes: Vec<TelemetryAttribute>,
	pub scope_name: String,
	pub scope_version: String,
	pub dropped_span_observer: Arc<dyn DroppedSpanObserver>,
	pub warning_hook: Arc<dyn TelemetryWarningHook>,
}

impl TelemetryProfile {
	pub fn new(
		local_node_id: u32,
		resource_attributes: Vec<TelemetryAttribute>,
		scope_name: impl Into<String>,
		scope_version: impl Into<String>,
		dropped_span_observer: Arc<dyn DroppedSpanObserver>,
		warning_hook: Arc<dyn TelemetryWarningHook>,
	) -> Self {
		Self {
			local_node_id,
			resource_attributes,
			scope_name: scope_name.into(),
			scope_version: scope_version.into(),
			dropped_span_observer,
			warning_hook,
		}
	}
}

pub trait DroppedSpanObserver: Send + Sync + 'static {
	fn record_dropped_span(&self) -> u64;
}

pub trait TelemetryWarningHook: Send + Sync + 'static {
	fn warn(&self, warning: TelemetryWarning<'_>);
}

pub enum TelemetryWarning<'a> {
	QueueSaturated {
		queue_capacity: usize,
		dropped_total: u64,
	},
	ExportTimedOut {
		error: &'a str,
	},
	ExportFailed {
		error: &'a str,
	},
}

#[derive(Clone, Debug, PartialEq)]
pub struct TelemetryAttribute {
	pub key: String,
	pub value: TelemetryValue,
}

impl TelemetryAttribute {
	pub fn string(key: impl Into<String>, value: impl Into<String>) -> Self {
		Self {
			key: key.into(),
			value: TelemetryValue::String(value.into()),
		}
	}

	pub fn integer(key: impl Into<String>, value: i64) -> Self {
		Self {
			key: key.into(),
			value: TelemetryValue::Integer(value),
		}
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum TelemetryValue {
	String(String),
	Bool(bool),
	Integer(i64),
	Double(f64),
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn traceparent_accepts_w3c_v00_and_rejects_uppercase_or_zero_ids() {
		assert!(
			validate_traceparent("00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01").is_ok()
		);
		assert!(
			validate_traceparent("00-4BF92F3577B34DA6A3CE929D0E0E4736-00f067aa0ba902b7-01")
				.is_err()
		);
		assert!(
			validate_traceparent("00-00000000000000000000000000000000-00f067aa0ba902b7-01")
				.is_err()
		);
	}

	#[test]
	fn tracestate_rejects_control_chars_and_overlong_values() {
		assert!(validate_tracestate(Some("vendor=value")).is_ok());
		assert!(validate_tracestate(Some("vendor=\u{7f}")).is_err());
		assert!(validate_tracestate(Some(&"a".repeat(513))).is_err());
	}
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
mod otlp {
	use super::{
		OTEL_PARENT_SPAN_ID_FIELD, OTEL_SAMPLED_FIELD, OTEL_SPAN_ID_FIELD, OTEL_TRACE_ID_FIELD,
		TRACE_SAMPLING_RATE_DENOMINATOR, TelemetryAttribute as KeyValue, TelemetryProfile,
		TelemetryValue as Value, TelemetryWarning, TraceExportConfig, TraceExportProtocol,
	};
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
		profile: Arc<TelemetryProfile>,
		next_trace_seq: AtomicU64,
		next_span_seq: AtomicU64,
		last_drop_warn_unix_nanos: AtomicU64,
	}

	impl OtlpTraceLayer {
		pub fn new(
			cfg: TraceExportConfig,
			profile: TelemetryProfile,
		) -> std::io::Result<(Self, OtlpShutdownHandle)> {
			let transport = OtlpTransport::from_config(cfg.protocol)?;
			let client = build_client(&cfg, transport)?;
			let (tx, rx) = mpsc::channel(cfg.max_queue_size);
			let profile = Arc::new(profile);
			let task = tokio::spawn(run_export_loop(
				rx,
				client,
				Arc::clone(&profile),
				cfg.export_timeout,
			));
			Ok((
				Self {
					tx: tx.clone(),
					sampling_rate_per_million: cfg.sampling_rate_per_million,
					queue_capacity: cfg.max_queue_size,
					profile,
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
				self.profile.local_node_id, 0,
				"OTLP trace IDs rely on validated local_node_id > 0",
			);
			let seq = self
				.next_trace_seq
				.fetch_add(1, Ordering::Relaxed)
				.to_be_bytes();
			let mut out = [0u8; 16];
			out[..4].copy_from_slice(&self.profile.local_node_id.to_be_bytes());
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
			let dropped_total = self.profile.dropped_span_observer.record_dropped_span();
			if saturated
				&& should_emit_rate_limited_warning(
					&self.last_drop_warn_unix_nanos,
					unix_time_nanos(),
					OTLP_DROP_WARN_INTERVAL,
				) {
				self.profile
					.warning_hook
					.warn(TelemetryWarning::QueueSaturated {
						queue_capacity: self.queue_capacity,
						dropped_total,
					});
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
			let mut visitor = AttributeVisitor::default();
			attrs.record(&mut visitor);
			let (mut attributes, explicit_context) = visitor.finish();
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
			let default_trace_id = || {
				parent
					.as_ref()
					.map(|parent| parent.trace_id)
					.unwrap_or_else(|| self.next_trace_id())
			};
			let default_parent_span_id = || parent.as_ref().map(|parent| parent.span_id);
			let default_span_id = || self.next_span_id();
			let (sampled, trace_id, parent_span_id, span_id) =
				match explicit_context.to_authoritative_context() {
					ExplicitSamplingContext::Absent => {
						let sampled = parent
							.as_ref()
							.map(|parent| parent.sampled)
							.unwrap_or_else(|| self.should_sample_root_span());
						(
							sampled,
							default_trace_id(),
							default_parent_span_id(),
							default_span_id(),
						)
					}
					ExplicitSamplingContext::Unsampled => (
						false,
						default_trace_id(),
						default_parent_span_id(),
						default_span_id(),
					),
					ExplicitSamplingContext::Sampled(context) => (
						true,
						context.trace_id,
						context.parent_span_id,
						context.span_id,
					),
					ExplicitSamplingContext::Malformed => (
						false,
						default_trace_id(),
						default_parent_span_id(),
						default_span_id(),
					),
				};
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
			let (attributes, _) = visitor.finish();
			state.attributes.extend(attributes);
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
			let (mut attributes, _) = visitor.finish();
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
		profile: Arc<TelemetryProfile>,
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
								let _ = flush_batch(&client, &profile, &batch).await;
								shutdown_client(&client).await;
								let _ = ack.send(());
								return;
							}
							Ok(None) => {
								let _ = flush_batch(&client, &profile, &batch).await;
								shutdown_client(&client).await;
								return;
							}
							Err(_) => break,
						}
					}
					match tokio::time::timeout(
						export_timeout,
						flush_batch(&client, &profile, &batch),
					)
					.await
					{
						Err(err) => {
							profile.warning_hook.warn(TelemetryWarning::ExportTimedOut {
								error: &err.to_string(),
							});
							shutdown_client(&client).await;
						}
						Ok(Err(err)) => {
							profile.warning_hook.warn(TelemetryWarning::ExportFailed {
								error: &err.to_string(),
							});
						}
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
		profile: &TelemetryProfile,
		spans: &[CompletedSpan],
	) -> std::io::Result<()> {
		match client {
			#[cfg(feature = "otlp-http")]
			OtlpClient::Http { client, endpoint } => {
				flush_http_batch(client, endpoint, profile, spans).await?
			}
			#[cfg(feature = "otlp-grpc")]
			OtlpClient::Grpc(client) => flush_grpc_batch(client, profile, spans).await?,
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
		profile: &TelemetryProfile,
		spans: &[CompletedSpan],
	) -> std::io::Result<()> {
		let payload = encode_export_trace_service_request(profile, spans);
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
		let material = tls_pki::TlsMaterial::load(
			cfg.client_cert_path.as_path(),
			cfg.client_key_path.as_path(),
			cfg.ca_bundle_path.as_path(),
		)
		.map_err(pki_error_to_io)?;
		let verifier = tls_pki::build_server_cert_chain_verifier(material.ca_roots.clone())
			.map_err(pki_error_to_io)?;
		let client_config = tls_pki::build_rustls_client_config_with_alpns(
			&material,
			verifier,
			vec![b"h2".to_vec()],
		)
		.map_err(pki_error_to_io)?;
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
	fn pki_error_to_io(error: tls_pki::PkiError) -> std::io::Error {
		Error::new(ErrorKind::InvalidData, error)
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
		profile: &TelemetryProfile,
		spans: &[CompletedSpan],
	) -> std::io::Result<()> {
		let payload = encode_export_trace_service_request(profile, spans);
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

	#[derive(Clone, Copy)]
	enum SpanStatusCode {
		Unset,
		Error,
	}

	#[derive(Default)]
	struct AttributeVisitor {
		attributes: HashMap<String, Value>,
		message: Option<String>,
		explicit_context: ExplicitSpanContextFields,
	}

	impl AttributeVisitor {
		fn record_value(&mut self, field: &Field, value: Value) {
			if self.explicit_context.record(field.name(), &value) {
				return;
			}
			if field.name() == "message" {
				if let Value::String(message) = &value {
					self.message = Some(message.clone());
				}
			}
			self.attributes.insert(field.name().to_string(), value);
		}

		fn finish(self) -> (Vec<KeyValue>, ExplicitSpanContextFields) {
			let attributes = self
				.attributes
				.into_iter()
				.map(|(key, value)| KeyValue { key, value })
				.collect();
			(attributes, self.explicit_context)
		}
	}

	#[derive(Clone, Copy, Default)]
	struct ExplicitSpanContextFields {
		trace_id: Option<[u8; 16]>,
		span_id: Option<[u8; 8]>,
		parent_span_id: Option<[u8; 8]>,
		sampled: Option<bool>,
		malformed: bool,
	}

	impl ExplicitSpanContextFields {
		fn record(&mut self, field_name: &str, value: &Value) -> bool {
			match field_name {
				OTEL_TRACE_ID_FIELD => {
					self.record_trace_id(value);
					true
				}
				OTEL_SPAN_ID_FIELD => {
					self.record_span_id(value);
					true
				}
				OTEL_PARENT_SPAN_ID_FIELD => {
					self.record_parent_span_id(value);
					true
				}
				OTEL_SAMPLED_FIELD => {
					self.record_sampled(value);
					true
				}
				_ => false,
			}
		}

		fn record_trace_id(&mut self, value: &Value) {
			match parse_trace_id(value) {
				Ok(trace_id) => self.trace_id = Some(trace_id),
				Err(()) => self.malformed = true,
			}
		}

		fn record_span_id(&mut self, value: &Value) {
			match parse_span_id(value) {
				Ok(span_id) => self.span_id = Some(span_id),
				Err(()) => self.malformed = true,
			}
		}

		fn record_parent_span_id(&mut self, value: &Value) {
			match parse_span_id(value) {
				Ok(span_id) => self.parent_span_id = Some(span_id),
				Err(()) => self.malformed = true,
			}
		}

		fn record_sampled(&mut self, value: &Value) {
			match value {
				Value::Bool(sampled) => self.sampled = Some(*sampled),
				_ => self.malformed = true,
			}
		}

		fn to_authoritative_context(self) -> ExplicitSamplingContext {
			let Some(sampled) = self.sampled else {
				return if self.malformed {
					ExplicitSamplingContext::Malformed
				} else {
					ExplicitSamplingContext::Absent
				};
			};
			if !sampled {
				return ExplicitSamplingContext::Unsampled;
			}
			if self.malformed {
				return ExplicitSamplingContext::Malformed;
			}
			let (Some(trace_id), Some(span_id)) = (self.trace_id, self.span_id) else {
				return ExplicitSamplingContext::Malformed;
			};
			ExplicitSamplingContext::Sampled(ExplicitSpanContext {
				trace_id,
				span_id,
				parent_span_id: self.parent_span_id,
			})
		}
	}

	enum ExplicitSamplingContext {
		Absent,
		Unsampled,
		Sampled(ExplicitSpanContext),
		Malformed,
	}

	struct ExplicitSpanContext {
		trace_id: [u8; 16],
		span_id: [u8; 8],
		parent_span_id: Option<[u8; 8]>,
	}

	fn parse_trace_id(value: &Value) -> Result<[u8; 16], ()> {
		let Value::String(value) = value else {
			return Err(());
		};
		parse_lower_hex_array::<16>(value)
	}

	fn parse_span_id(value: &Value) -> Result<[u8; 8], ()> {
		let Value::String(value) = value else {
			return Err(());
		};
		parse_lower_hex_array::<8>(value)
	}

	fn parse_lower_hex_array<const N: usize>(value: &str) -> Result<[u8; N], ()> {
		let bytes = value.as_bytes();
		if bytes.len() != N * 2 {
			return Err(());
		}
		let mut out = [0u8; N];
		for i in 0..N {
			let high = lower_hex_nibble(bytes[i * 2])?;
			let low = lower_hex_nibble(bytes[i * 2 + 1])?;
			out[i] = (high << 4) | low;
		}
		if out.iter().all(|byte| *byte == 0) {
			return Err(());
		}
		Ok(out)
	}

	fn lower_hex_nibble(byte: u8) -> Result<u8, ()> {
		match byte {
			b'0'..=b'9' => Ok(byte - b'0'),
			b'a'..=b'f' => Ok(byte - b'a' + 10),
			_ => Err(()),
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

	fn encode_export_trace_service_request(
		profile: &TelemetryProfile,
		spans: &[CompletedSpan],
	) -> Vec<u8> {
		let mut out = Vec::new();
		encode_message(&mut out, 1, |resource_spans| {
			encode_message(resource_spans, 1, |resource| {
				for attr in &profile.resource_attributes {
					encode_key_value(resource, 1, attr);
				}
			});
			encode_message(resource_spans, 2, |scope_spans| {
				encode_message(scope_spans, 1, |scope| {
					encode_string(scope, 1, &profile.scope_name);
					encode_string(scope, 2, &profile.scope_version);
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

		#[test]
		fn explicit_sampled_context_requires_lower_hex_nonzero_ids() {
			let mut explicit = ExplicitSpanContextFields::default();
			assert!(explicit.record(
				OTEL_TRACE_ID_FIELD,
				&Value::String("4bf92f3577b34da6a3ce929d0e0e4736".to_string())
			));
			assert!(explicit.record(
				OTEL_SPAN_ID_FIELD,
				&Value::String("00f067aa0ba902b7".to_string())
			));
			assert!(explicit.record(OTEL_SAMPLED_FIELD, &Value::Bool(true)));

			let ExplicitSamplingContext::Sampled(context) = explicit.to_authoritative_context()
			else {
				panic!("valid explicit context must sample");
			};
			assert_eq!(
				context.trace_id,
				[
					0x4b, 0xf9, 0x2f, 0x35, 0x77, 0xb3, 0x4d, 0xa6, 0xa3, 0xce, 0x92, 0x9d, 0x0e,
					0x0e, 0x47, 0x36
				]
			);
			assert_eq!(
				context.span_id,
				[0x00, 0xf0, 0x67, 0xaa, 0x0b, 0xa9, 0x02, 0xb7]
			);
		}

		#[test]
		fn malformed_explicit_context_is_unsampled() {
			let mut explicit = ExplicitSpanContextFields::default();
			assert!(explicit.record(
				OTEL_TRACE_ID_FIELD,
				&Value::String("4BF92F3577B34DA6A3CE929D0E0E4736".to_string())
			));
			assert!(explicit.record(OTEL_SAMPLED_FIELD, &Value::Bool(true)));

			assert!(matches!(
				explicit.to_authoritative_context(),
				ExplicitSamplingContext::Malformed
			));

			let mut unsampled = ExplicitSpanContextFields::default();
			assert!(unsampled.record(OTEL_SAMPLED_FIELD, &Value::Bool(false)));
			assert!(matches!(
				unsampled.to_authoritative_context(),
				ExplicitSamplingContext::Unsampled
			));
		}
	}
}
