use std::io::{Error, ErrorKind};
#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
use std::sync::Arc;

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
use observability::telemetry as shared_telemetry;
pub use observability::telemetry::{
	TRACE_SAMPLING_RATE_DENOMINATOR, TraceExportConfig, TraceExportProtocol,
};
use tracing::Dispatch;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::layer::SubscriberExt as _;

const DEFAULT_LOG_FILTER: &str = "info";
#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
const OTLP_SCOPE_NAME: &str = "wendelon.traces";

pub struct TelemetryHandle {
	#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
	shutdown: Option<shared_telemetry::OtlpShutdownHandle>,
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
					let profile = wendelon_profile(local_node_id);
					let (layer, shutdown) =
						shared_telemetry::OtlpTraceLayer::new(cfg.clone(), profile)?;
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
					let profile = wendelon_profile(local_node_id);
					let (layer, shutdown) =
						shared_telemetry::OtlpTraceLayer::new(cfg.clone(), profile)?;
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
fn wendelon_profile(local_node_id: u32) -> shared_telemetry::TelemetryProfile {
	shared_telemetry::TelemetryProfile::new(
		local_node_id,
		vec![
			shared_telemetry::TelemetryAttribute::string("service.name", "wendelon"),
			shared_telemetry::TelemetryAttribute::string(
				"service.version",
				env!("CARGO_PKG_VERSION"),
			),
			shared_telemetry::TelemetryAttribute::integer("wendelon.node_id", local_node_id as i64),
		],
		OTLP_SCOPE_NAME,
		env!("CARGO_PKG_VERSION"),
		Arc::new(WendelonDroppedSpanObserver),
		Arc::new(WendelonTelemetryWarningHook),
	)
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
struct WendelonDroppedSpanObserver;

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
impl shared_telemetry::DroppedSpanObserver for WendelonDroppedSpanObserver {
	fn record_dropped_span(&self) -> u64 {
		let metrics = crate::metrics::global();
		metrics.otlp_spans_dropped_total.inc();
		metrics.otlp_spans_dropped_total.get()
	}
}

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
struct WendelonTelemetryWarningHook;

#[cfg(any(feature = "otlp-http", feature = "otlp-grpc"))]
impl shared_telemetry::TelemetryWarningHook for WendelonTelemetryWarningHook {
	fn warn(&self, warning: shared_telemetry::TelemetryWarning<'_>) {
		match warning {
			shared_telemetry::TelemetryWarning::QueueSaturated {
				queue_capacity,
				dropped_total,
			} => {
				tracing::warn!(
					queue_capacity,
					dropped_total,
					"otlp span export queue saturated; dropping sampled spans",
				);
			}
			shared_telemetry::TelemetryWarning::ExportTimedOut { error } => {
				eprintln!("wendelon: otlp export timed out: {error}");
			}
			shared_telemetry::TelemetryWarning::ExportFailed { error } => {
				eprintln!("wendelon: otlp export failed: {error}");
			}
		}
	}
}
