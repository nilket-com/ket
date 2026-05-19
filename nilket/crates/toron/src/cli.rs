use std::net::SocketAddr;
use std::path::{Path, PathBuf};
use std::process::ExitCode;

use arrow::ipc::reader::StreamReader;
use arrow::record_batch::RecordBatch;

const RESERVED_MESSAGE_SUFFIX: &str = "is reserved; production execution is not wired in ADR 040";
const DEFAULT_SQL_ENDPOINT: &str = "127.0.0.1:7742";

pub fn run<I>(program: &str, args: I) -> ExitCode
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<String> = args.into_iter().collect();
	match args.as_slice() {
		[] => {
			print_usage(program);
			ExitCode::from(2)
		}
		[flag] if flag == "--help" || flag == "-h" => {
			print_usage(program);
			ExitCode::SUCCESS
		}
		[flag] if flag == "--version" || flag == "-V" => {
			println!("toron {}", env!("CARGO_PKG_VERSION"));
			ExitCode::SUCCESS
		}
		[flag] if flag == "--print-example-config" => {
			print!("{}", crate::config::render_example_config());
			ExitCode::SUCCESS
		}
		[flag, path] if flag == "--write-example-config" => {
			match crate::config::write_example_config(Path::new(path)) {
				Ok(()) => {
					eprintln!("toron: wrote starter config to {path}");
					ExitCode::SUCCESS
				}
				Err(error) => {
					eprintln!("failed to write starter config to {path}: {error}");
					ExitCode::from(2)
				}
			}
		}
		[verb, config_path] if verb == "start" => {
			crate::daemon::run_start(PathBuf::from(config_path))
		}
		[verb] if verb == "start" => {
			eprintln!("toron start requires <path/to/toron.toml>");
			ExitCode::from(2)
		}
		[verb, rest @ ..] if verb == "sql" && !rest.is_empty() => run_sql(rest),
		[verb] if verb == "sql" => {
			eprintln!("toron sql requires <statement...>");
			ExitCode::from(2)
		}
		[verb] if verb == "inspect" => reserved("inspect"),
		[verb, _rest @ ..] if verb == "inspect" => reserved("inspect"),
		[unknown, ..] => {
			eprintln!("unknown toron command: {unknown}");
			print_usage(program);
			ExitCode::from(2)
		}
	}
}

fn reserved(verb: &str) -> ExitCode {
	eprintln!("toron {verb} {RESERVED_MESSAGE_SUFFIX}");
	ExitCode::from(2)
}

fn run_sql(args: &[String]) -> ExitCode {
	let (endpoint, statement_args) = match parse_sql_args(args) {
		Ok(parsed) => parsed,
		Err(message) => {
			eprintln!("{message}");
			return ExitCode::from(2);
		}
	};
	if statement_args.is_empty() {
		eprintln!("toron sql requires <statement...>");
		return ExitCode::from(2);
	}
	let statement = statement_args.join(" ");
	let runtime = match tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("failed to initialize Toron runtime: {error}");
			return ExitCode::from(2);
		}
	};
	runtime.block_on(run_sql_async(endpoint, statement))
}

fn parse_sql_args(args: &[String]) -> Result<(SocketAddr, &[String]), String> {
	if args.first().map(String::as_str) == Some("--endpoint") {
		let endpoint = args
			.get(1)
			.ok_or_else(|| "toron sql --endpoint requires <socket-addr>".to_string())?
			.parse::<SocketAddr>()
			.map_err(|error| format!("invalid toron sql endpoint: {error}"))?;
		return Ok((endpoint, &args[2..]));
	}

	let endpoint = match std::env::var("TORON_SQL_ENDPOINT") {
		Ok(value) => value
			.parse::<SocketAddr>()
			.map_err(|error| format!("invalid TORON_SQL_ENDPOINT: {error}"))?,
		Err(_) => DEFAULT_SQL_ENDPOINT
			.parse::<SocketAddr>()
			.expect("compiled default SQL endpoint is valid"),
	};
	Ok((endpoint, args))
}

async fn run_sql_async(endpoint: SocketAddr, statement: String) -> ExitCode {
	let client = match crate::quic::QuicEndpoint::client() {
		Ok(client) => client,
		Err(error) => {
			eprintln!("failed to initialize Toron SQL QUIC client: {error}");
			return ExitCode::from(2);
		}
	};
	let mut stream = match tokio::time::timeout(
		std::time::Duration::from_secs(2),
		client.connect_stream(endpoint),
	)
	.await
	{
		Ok(Ok(stream)) => stream,
		Ok(Err(error)) => {
			eprintln!("failed to connect to toron sql endpoint {endpoint}: {error}");
			return ExitCode::from(2);
		}
		Err(_) => {
			eprintln!("failed to connect to toron sql endpoint {endpoint}: connection timed out");
			return ExitCode::from(2);
		}
	};
	let query_id = crate::distributed::identity::QueryId::new();
	let request = crate::sql_wire::SqlFrame::execute(query_id, statement, None);
	if let Err(error) = crate::sql_wire::write_frame(&mut stream, &request).await {
		eprintln!("failed to send toron sql request: {error}");
		return ExitCode::from(2);
	}
	if let Err(error) = stream.finish().await {
		eprintln!("failed to finish toron sql request stream: {error}");
		return ExitCode::from(2);
	}

	let mut batches = Vec::new();
	loop {
		let frame = match crate::sql_wire::read_frame(&mut stream).await {
			Ok(frame) => frame,
			Err(error) => {
				eprintln!("failed to read toron sql response: {error}");
				return ExitCode::from(1);
			}
		};
		match frame.message {
			crate::sql_wire::SqlMessage::ResultBatch(batch) => {
				if let Err(error) = decode_sql_batch(&batch.arrow_ipc_bytes, &mut batches) {
					eprintln!("failed to decode toron sql result batch: {error}");
					return ExitCode::from(1);
				}
			}
			crate::sql_wire::SqlMessage::Complete(complete) => {
				if batches.is_empty() {
					if complete.row_count == 0 {
						println!("OK");
					} else {
						println!("OK rows={}", complete.row_count);
					}
				} else if let Err(error) = render_batches(&batches) {
					eprintln!("failed to render toron sql results: {error}");
					return ExitCode::from(1);
				}
				return ExitCode::SUCCESS;
			}
			crate::sql_wire::SqlMessage::Error(error) => {
				eprintln!("toron sql error [{:?}]: {}", error.code, error.message);
				return ExitCode::from(1);
			}
			crate::sql_wire::SqlMessage::Execute(_) | crate::sql_wire::SqlMessage::Cancel(_) => {}
		}
	}
}

fn decode_sql_batch(
	bytes: &[u8],
	batches: &mut Vec<RecordBatch>,
) -> Result<(), arrow::error::ArrowError> {
	let reader = StreamReader::try_new(bytes, None)?;
	for batch in reader {
		batches.push(batch?);
	}
	Ok(())
}

fn render_batches(batches: &[RecordBatch]) -> Result<(), arrow::error::ArrowError> {
	let total_rows = batches.iter().map(RecordBatch::num_rows).sum::<usize>();
	let rendered = arrow_cast::pretty::pretty_format_batches(batches)?;
	println!("{rendered}");
	println!("({total_rows} rows)");
	Ok(())
}

fn print_usage(program: &str) {
	eprintln!(
		"usage:\n  {program} start <path/to/toron.toml>\n  {program} --print-example-config\n  {program} --write-example-config <path>\n  {program} sql [--endpoint <socket-addr>] <statement...>\n  {program} inspect [target]\n\nToron inspect is reserved in ADR 040."
	);
}
