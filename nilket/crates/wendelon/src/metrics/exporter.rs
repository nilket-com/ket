use crate::metrics::global;
use std::io::{Error, ErrorKind};
use std::net::SocketAddr;
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::JoinHandle;

pub const METRICS_PATH: &str = "/metrics";

pub struct MetricsExporterHandle {
	pub local_addr: SocketAddr,
	pub task: JoinHandle<()>,
}

pub async fn spawn(bind_address: &str) -> std::io::Result<MetricsExporterHandle> {
	let listener = TcpListener::bind(bind_address).await?;
	spawn_on(listener).await
}

pub async fn spawn_on(listener: TcpListener) -> std::io::Result<MetricsExporterHandle> {
	let local_addr = listener.local_addr()?;
	let task = tokio::spawn(async move {
		loop {
			let Ok((stream, _peer)) = listener.accept().await else {
				return;
			};
			tokio::spawn(async move {
				let _ = handle_connection(stream).await;
			});
		}
	});
	Ok(MetricsExporterHandle { local_addr, task })
}

async fn handle_connection(mut stream: TcpStream) -> std::io::Result<()> {
	let mut buf = [0u8; 4096];
	let n = stream.read(&mut buf).await?;
	if n == 0 {
		return Ok(());
	}
	let req = std::str::from_utf8(&buf[..n]).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("invalid HTTP request: {err}"),
		)
	})?;
	let first_line = req.lines().next().unwrap_or_default();
	let mut parts = first_line.split_whitespace();
	let method = parts.next().unwrap_or_default();
	let path = parts.next().unwrap_or_default();

	if method == "GET" && path == METRICS_PATH {
		global().metrics_scrapes_total.inc();
		let body = global().render_prometheus();
		let resp = format!(
			"HTTP/1.1 200 OK\r\ncontent-type: text/plain; version=0.0.4; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
			body.len(),
			body
		);
		stream.write_all(resp.as_bytes()).await?;
		stream.flush().await?;
		return Ok(());
	}

	let body = "not found\n";
	let resp = format!(
		"HTTP/1.1 404 Not Found\r\ncontent-type: text/plain; charset=utf-8\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{}",
		body.len(),
		body
	);
	stream.write_all(resp.as_bytes()).await?;
	stream.flush().await?;
	Ok(())
}
