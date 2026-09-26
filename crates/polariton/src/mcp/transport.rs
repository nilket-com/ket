//! Stdio framing with ADR 0173's EOF contract. The SDK owns JSON-RPC
//! messages and dispatch; this adapter reports malformed JSON and stops
//! output immediately on EOF instead of draining replies after disconnect.
use super::*;
use rmcp::{
	service::{RxJsonRpcMessage, TxJsonRpcMessage},
	transport::Transport,
};
use tokio::io::{AsyncBufReadExt, AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt, BufReader};

// Includes the newline. An oversized input closes the session without dispatch.
const MAX_FRAME: usize = 8 * 1024 * 1024;

pub(super) struct Stdio<R, W> {
	reader: BufReader<R>,
	line: Vec<u8>,
	writer: Arc<tokio::sync::Mutex<W>>,
	stop: CancellationToken,
}
impl<R: AsyncRead + Unpin, W> Stdio<R, W> {
	pub(super) fn new(reader: R, writer: W, stop: CancellationToken) -> Self {
		Self {
			reader: BufReader::new(reader),
			line: Vec::new(),
			writer: Arc::new(tokio::sync::Mutex::new(writer)),
			stop,
		}
	}
}
impl<R, W> Transport<RoleServer> for Stdio<R, W>
where
	R: AsyncRead + Send + Unpin,
	W: AsyncWrite + Send + Unpin + 'static,
{
	type Error = std::io::Error;
	fn send(
		&mut self,
		message: TxJsonRpcMessage<RoleServer>,
	) -> impl Future<Output = Result<(), Self::Error>> + Send + 'static {
		let writer = self.writer.clone();
		let stop = self.stop.clone();
		async move {
			let mut bytes = serde_json::to_vec(&message).map_err(std::io::Error::other)?;
			bytes.push(b'\n');
			tokio::select! { biased;
			 _ = stop.cancelled() => Ok(()),
			 result = async { let mut writer = writer.lock().await; writer.write_all(&bytes).await?; writer.flush().await } => result,
			}
		}
	}
	async fn receive(&mut self) -> Option<RxJsonRpcMessage<RoleServer>> {
		loop {
			// take bounds read_until's allocation even when no newline arrives.
			// A cancelled receive retains its prefix; the next read gets only the
			// remaining allowance, never another full frame's worth.
			let mut bounded = (&mut self.reader).take((MAX_FRAME - self.line.len()) as u64);
			let count = tokio::select! { biased; _ = self.stop.cancelled() => return None, result = bounded.read_until(b'\n', &mut self.line) => result };
			match count {
				Ok(0) | Err(_) => {
					self.stop.cancel();
					return None;
				}
				Ok(_) => {}
			}
			if !self.line.ends_with(b"\n") {
				if self.line.len() == MAX_FRAME {
					eprintln!("polariton mcp: input frame exceeded the 8 MiB limit; closing");
				}
				// Oversized or unterminated frames are never dispatched.
				self.stop.cancel();
				return None;
			}
			// read_until retains partial bytes across cancellation of receive().
			let parsed = serde_json::from_slice::<Value>(&self.line);
			self.line.clear();
			let (error, id) = match parsed {
				Ok(value)
					if value.get("id").is_some()
						&& value["method"] == "tools/call"
						&& value["params"]
							.get("arguments")
							.is_some_and(|args| !args.is_object()) =>
				{
					// Preserve schema rejection before the SDK's optional argument
					// map could normalize an explicit null into an omitted map.
					(
						ErrorData::invalid_params("tool arguments must be an object", None),
						value
							.get("id")
							.cloned()
							.and_then(|id| serde_json::from_value(id).ok()),
					)
				}
				Ok(value) => {
					match serde_json::from_value::<RxJsonRpcMessage<RoleServer>>(value.clone()) {
						Ok(message) => return Some(message),
						Err(_) if value.get("method").is_some() && value.get("id").is_none() => {
							eprintln!("polariton mcp: ignored malformed notification");
							continue;
						}
						Err(_) => (
							ErrorData::invalid_request("Invalid JSON-RPC request", None),
							value
								.get("id")
								.cloned()
								.and_then(|id| serde_json::from_value(id).ok()),
						),
					}
				}
				Err(_) => (
					ErrorData::new(ErrorCode(-32700), "Invalid JSON", None),
					None,
				),
			};
			if self
				.send(TxJsonRpcMessage::<RoleServer>::error(error, id))
				.await
				.is_err()
			{
				self.stop.cancel();
				return None;
			}
		}
	}
	async fn close(&mut self) -> Result<(), Self::Error> {
		self.stop.cancel();
		self.writer.lock().await.shutdown().await
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn framing_accepts_the_limit_and_closes_before_oversized_input_is_dispatched() {
		let request = br#"{"jsonrpc":"2.0","id":1,"method":"ping"}"#;
		for extra in [0, 1] {
			let mut input = request.to_vec();
			input.resize(MAX_FRAME - 1 + extra, b' ');
			input.push(b'\n');
			let stop = CancellationToken::new();
			let mut transport = Stdio::new(input.as_slice(), tokio::io::sink(), stop.clone());
			let message = transport.receive().await;
			assert_eq!(message.is_some(), extra == 0);
			assert_eq!(stop.is_cancelled(), extra != 0);
			assert!(transport.line.len() <= MAX_FRAME);
			if extra != 0 {
				assert_eq!(transport.line.len(), MAX_FRAME);
				assert!(transport.receive().await.is_none());
			}
		}
	}
}
