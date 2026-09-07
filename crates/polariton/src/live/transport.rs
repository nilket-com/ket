//! Bounded sockets deliver messages, never references to application state.

use super::{Event, MAX_ANSWERS, MAX_LINE, MAX_OUTSTANDING, Request};
use iced::futures::{SinkExt, StreamExt};
use std::hash::{Hash, Hasher};
use std::io;
use std::os::unix::fs::PermissionsExt;
use std::path::Path;
use std::sync::{
	Arc, Mutex,
	atomic::{AtomicBool, AtomicUsize, Ordering},
};
use tokio::io::{AsyncBufRead, AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::sync::{mpsc, watch};

#[derive(Clone)]
pub(crate) enum Inbound {
	Connected {
		connection: u64,
		generation: u64,
		sink: Sink,
	},
	Request {
		connection: u64,
		generation: u64,
		request: Request,
		reply: Reply,
	},
	Disconnected {
		connection: u64,
		generation: u64,
	},
}

#[derive(Clone)]
pub(crate) struct Sink {
	answers: mpsc::Sender<Event>,
	changed: watch::Sender<Option<Event>>,
	closed: watch::Sender<Option<String>>,
	outstanding: Arc<AtomicUsize>,
}

impl Sink {
	pub(crate) fn answer(&self, event: Event) {
		if self.answers.try_send(event).is_err() {
			self.close("answer_queue_full");
		}
	}
	pub(crate) fn changed(&self, event: Event) {
		self.changed.send_replace(Some(event));
	}
	pub(crate) fn close(&self, reason: &str) {
		self.closed.send_replace(Some(reason.to_owned()));
	}
	pub(crate) fn is_closed(&self) -> bool {
		self.closed.borrow().is_some()
	}
	fn admit(&self) -> Option<Reply> {
		self.outstanding
			.fetch_update(Ordering::AcqRel, Ordering::Acquire, |n| {
				(n < MAX_OUTSTANDING).then_some(n + 1)
			})
			.ok()?;
		Some(Reply {
			sink: self.clone(),
			finished: Arc::new(AtomicBool::new(false)),
		})
	}
}

/// A request's outstanding slot is released exactly once on a final answer.
#[derive(Clone)]
pub(crate) struct Reply {
	sink: Sink,
	finished: Arc<AtomicBool>,
}

impl Reply {
	pub(crate) fn answer(&self, event: Event) {
		if self.finished.load(Ordering::Acquire) {
			return;
		}
		if event.is_final() && !self.finished.swap(true, Ordering::AcqRel) {
			self.sink.outstanding.fetch_sub(1, Ordering::AcqRel);
		}
		self.sink.answer(event);
	}
}

struct Receivers {
	answers: mpsc::Receiver<Event>,
	changed: watch::Receiver<Option<Event>>,
	closed: watch::Receiver<Option<String>>,
}

fn queues() -> (Sink, Receivers) {
	let (answers, answers_rx) = mpsc::channel(MAX_ANSWERS);
	let (changed, changed_rx) = watch::channel(None);
	let (closed, closed_rx) = watch::channel(None);
	(
		Sink {
			answers,
			changed,
			closed,
			outstanding: Arc::new(AtomicUsize::new(0)),
		},
		Receivers {
			answers: answers_rx,
			changed: changed_rx,
			closed: closed_rx,
		},
	)
}

#[cfg(test)]
pub(crate) fn test_request() -> (Sink, Reply, mpsc::Receiver<Event>) {
	let (sink, receivers) = queues();
	let reply = sink.admit().unwrap();
	(sink, reply, receivers.answers)
}

/// Read without ever accumulating more than the wire limit. EOF in a partial
/// line is a disconnect, never a request that could unexpectedly run.
pub(crate) async fn read_line<R: AsyncBufRead + Unpin>(
	reader: &mut R,
	limit: usize,
) -> io::Result<Option<Vec<u8>>> {
	let mut line = Vec::new();
	loop {
		let bytes = reader.fill_buf().await?;
		if bytes.is_empty() {
			return Ok(None);
		}
		let end = bytes.iter().position(|b| *b == b'\n');
		let count = end.map_or(bytes.len(), |i| i + 1);
		if line.len() + count > limit {
			return Err(io::Error::other("request_line_too_long"));
		}
		line.extend_from_slice(&bytes[..count]);
		reader.consume(count);
		if end.is_some() {
			return Ok(Some(line));
		}
	}
}

#[derive(Clone)]
struct Worker {
	identity: (String, u64),
	listener: Arc<Mutex<Option<std::os::unix::net::UnixListener>>>,
	stop: watch::Receiver<bool>,
}

impl Hash for Worker {
	fn hash<H: Hasher>(&self, hasher: &mut H) {
		self.identity.hash(hasher);
	}
}

pub(crate) struct Server {
	worker: Worker,
	stop: watch::Sender<bool>,
}

impl Server {
	pub(crate) fn bind(path: &Path, window: &str, generation: u64) -> io::Result<Self> {
		let listener = std::os::unix::net::UnixListener::bind(path)?;
		std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
		listener.set_nonblocking(true)?;
		let (stop, receiver) = watch::channel(false);
		Ok(Self {
			worker: Worker {
				identity: (window.to_owned(), generation),
				listener: Arc::new(Mutex::new(Some(listener))),
				stop: receiver,
			},
			stop,
		})
	}
	pub(crate) fn subscription(&self) -> iced::Subscription<Inbound> {
		iced::Subscription::run_with(self.worker.clone(), incoming)
	}
}

impl Drop for Server {
	fn drop(&mut self) {
		self.stop.send_replace(true);
	}
}

fn incoming(worker: &Worker) -> iced::futures::stream::BoxStream<'static, Inbound> {
	let worker = worker.clone();
	iced::stream::channel(16, move |mut output: iced::futures::channel::mpsc::Sender<Inbound>| async move {
		let listener = worker.listener.lock().expect("listener lock").take();
		let Some(listener) = listener else { return; };
		let Ok(listener) = tokio::net::UnixListener::from_std(listener) else { return; };
		let mut stop = worker.stop.clone();
		let mut connections = tokio::task::JoinSet::new();
		let mut next = 0_u64;
		loop {
			tokio::select! {
				biased;
				_ = stop.changed() => break,
				_ = connections.join_next(), if !connections.is_empty() => {},
				accepted = listener.accept() => {
					let Ok((socket, _)) = accepted else { break; };
					if !socket.peer_cred().is_ok_and(|peer| peer.uid() == super::discovery::uid()) { continue; }
					let Some(id) = next.checked_add(1) else { break; };
					next = id;
					let (sink, receivers) = queues();
					let connected = Inbound::Connected { connection: id, generation: worker.identity.1, sink: sink.clone() };
					if output.send(connected).await.is_err() { break; }
					connections.spawn(connection(socket, id, worker.identity.1, sink, receivers, output.clone(), stop.clone()));
				}
			}
		}
		// A best-effort close gets a short opportunity to reach writable peers.
		// A blocked peer cannot hold up the application or subscription teardown.
		let _ = tokio::time::timeout(std::time::Duration::from_millis(50), async {
			while connections.join_next().await.is_some() {}
		}).await;
	}).boxed()
}

async fn connection(
	socket: tokio::net::UnixStream,
	id: u64,
	generation: u64,
	sink: Sink,
	mut receivers: Receivers,
	mut output: iced::futures::channel::mpsc::Sender<Inbound>,
	mut stop: watch::Receiver<bool>,
) {
	let (reader, mut writer) = socket.into_split();
	let mut reader = BufReader::new(reader);
	let reading_sink = sink.clone();
	let mut requests = output.clone();
	let read = async move {
		let mut highest: Option<u64> = None;
		loop {
			let bytes = match read_line(&mut reader, MAX_LINE).await {
				Ok(Some(bytes)) => bytes,
				Ok(None) => break,
				Err(error) => {
					reading_sink.close(&error.to_string());
					break;
				}
			};
			let request: Request = match super::decode(&bytes) {
				Ok(request) => request,
				Err(_) => {
					reading_sink.close("invalid_request");
					break;
				}
			};
			let reason = if highest.is_some_and(|n| request.id <= n) {
				Some("duplicate_request")
			} else {
				None
			};
			if let Some(reason) = reason {
				reading_sink.answer(Event::Refused {
					id: request.id,
					reason: reason.into(),
					receipt: None,
				});
				continue;
			}
			let Some(reply) = reading_sink.admit() else {
				reading_sink.answer(Event::Refused {
					id: request.id,
					reason: "busy".into(),
					receipt: None,
				});
				continue;
			};
			highest = Some(request.id);
			if requests
				.send(Inbound::Request {
					connection: id,
					generation,
					request,
					reply,
				})
				.await
				.is_err()
			{
				break;
			}
		}
	};
	let write = async {
		loop {
			let event = tokio::select! {
				biased;
				event = receivers.answers.recv() => match event { Some(event) => event, None => break },
				changed = receivers.changed.changed() => {
					if changed.is_err() { break; }
					let Some(event) = receivers.changed.borrow_and_update().clone() else { continue; };
					event
				}
			};
			let Ok(bytes) = super::encode(&event) else {
				break;
			};
			if writer.write_all(&bytes).await.is_err() {
				break;
			}
		}
	};

	tokio::select! {
		_ = read => {},
		_ = write => {},
		_ = stop.changed() => { sink.close("gate_closed"); },
		_ = receivers.closed.changed() => {},
	}
	let reason = receivers
		.closed
		.borrow()
		.clone()
		.unwrap_or_else(|| "disconnected".into());
	sink.close(&reason);
	if let Ok(bytes) = super::encode(&Event::Closed { reason }) {
		let _ = tokio::time::timeout(
			std::time::Duration::from_millis(20),
			writer.write_all(&bytes),
		)
		.await;
	}
	let _ = output
		.send(Inbound::Disconnected {
			connection: id,
			generation,
		})
		.await;
}

#[cfg(test)]
mod tests {
	use super::*;

	#[tokio::test]
	async fn product_0160_socket_ids_are_connection_scoped_and_busy_does_not_queue() {
		let root = std::env::temp_dir().join(format!(
			"p16-wire-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		let window = super::super::discovery::Window::create(root.clone(), None, vec![]).unwrap();
		let server = Server::bind(
			&window.advertisement.socket,
			&window.advertisement.window,
			1,
		)
		.unwrap();
		assert_eq!(
			std::fs::metadata(&window.advertisement.socket)
				.unwrap()
				.permissions()
				.mode() & 0o777,
			0o600
		);
		let mut events = incoming(&server.worker);
		let (send, mut receive) = mpsc::channel(32);
		let pump = tokio::spawn(async move {
			while let Some(event) = events.next().await {
				if send.send(event).await.is_err() {
					break;
				}
			}
		});
		let socket = tokio::net::UnixStream::connect(&window.advertisement.socket)
			.await
			.unwrap();
		let (read, mut write) = socket.into_split();
		let mut read = BufReader::new(read);
		let next = |receive: &mut mpsc::Receiver<Inbound>| {
			// Kept inline below for borrow lifetimes.
			receive.try_recv()
		};
		let connected = tokio::time::timeout(std::time::Duration::from_secs(2), receive.recv())
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(
			connected,
			Inbound::Connected { generation: 1, .. }
		));
		for id in 1..=17 {
			write
				.write_all(
					&super::super::encode(&Request {
						id,
						command: crate::driver::Command::Describe {},
					})
					.unwrap(),
				)
				.await
				.unwrap();
		}
		let mut replies = Vec::new();
		for id in 1..=16 {
			let event = tokio::time::timeout(std::time::Duration::from_secs(2), receive.recv())
				.await
				.unwrap()
				.unwrap();
			let Inbound::Request { request, reply, .. } = event else {
				panic!("expected request")
			};
			assert_eq!(request.id, id);
			replies.push(reply);
		}
		let bytes = tokio::time::timeout(
			std::time::Duration::from_secs(2),
			read_line(&mut read, MAX_LINE),
		)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
		assert_eq!(
			super::super::decode::<Event>(&bytes).unwrap(),
			Event::Refused {
				id: 17,
				reason: "busy".into(),
				receipt: None
			}
		);
		assert!(next(&mut receive).is_err());
		write
			.write_all(
				&super::super::encode(&Request {
					id: 1,
					command: crate::driver::Command::Describe {},
				})
				.unwrap(),
			)
			.await
			.unwrap();
		let bytes = tokio::time::timeout(
			std::time::Duration::from_secs(2),
			read_line(&mut read, MAX_LINE),
		)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
		assert_eq!(
			super::super::decode::<Event>(&bytes).unwrap(),
			Event::Refused {
				id: 1,
				reason: "duplicate_request".into(),
				receipt: None
			}
		);
		assert!(next(&mut receive).is_err());

		// A new connection has no memory of the first one's high-water mark.
		let mut second = tokio::net::UnixStream::connect(&window.advertisement.socket)
			.await
			.unwrap();
		assert!(matches!(
			receive.recv().await.unwrap(),
			Inbound::Connected { .. }
		));
		second
			.write_all(
				&super::super::encode(&Request {
					id: 1,
					command: crate::driver::Command::Describe {},
				})
				.unwrap(),
			)
			.await
			.unwrap();
		let fresh = tokio::time::timeout(std::time::Duration::from_secs(2), receive.recv())
			.await
			.unwrap()
			.unwrap();
		assert!(matches!(
			fresh,
			Inbound::Request {
				request: Request { id: 1, .. },
				..
			}
		));
		drop(server);
		let bytes = tokio::time::timeout(
			std::time::Duration::from_secs(2),
			read_line(&mut read, MAX_LINE),
		)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
		assert!(matches!(
			super::super::decode::<Event>(&bytes).unwrap(),
			Event::Closed { .. }
		));
		pump.abort();
		drop((write, read, second, window));
		std::fs::remove_dir_all(root).unwrap();
	}
	#[test]
	fn product_0160_reply_bounds_and_latest_snapshot_never_wait() {
		let (sink, mut receivers) = queues();
		let replies: Vec<_> = (0..MAX_OUTSTANDING)
			.map(|_| sink.admit().unwrap())
			.collect();
		assert!(sink.admit().is_none());
		replies[0].answer(Event::Refused {
			id: 0,
			reason: "gate_closed".into(),
			receipt: None,
		});
		assert!(sink.admit().is_some());
		assert!(sink.admit().is_none());
		for n in 0..1000 {
			sink.changed(Event::Closed {
				reason: n.to_string(),
			});
		}
		assert_eq!(
			receivers.changed.borrow_and_update().clone(),
			Some(Event::Closed {
				reason: "999".into()
			})
		);
		for n in 1..MAX_ANSWERS {
			sink.answer(Event::Refused {
				id: n as u64,
				reason: "busy".into(),
				receipt: None,
			});
		}
		assert!(!sink.is_closed());
		sink.answer(Event::Refused {
			id: 999,
			reason: "busy".into(),
			receipt: None,
		});
		assert!(sink.is_closed());
		assert_eq!(receivers.answers.len(), MAX_ANSWERS);
	}

	#[tokio::test]
	async fn product_0160_oversized_and_partial_lines_cannot_be_requests() {
		let bytes = vec![b'x'; MAX_LINE + 1];
		assert!(read_line(&mut bytes.as_slice(), MAX_LINE).await.is_err());
		assert!(
			read_line(&mut &b"partial"[..], MAX_LINE)
				.await
				.unwrap()
				.is_none()
		);
		assert_eq!(
			read_line(&mut &b"{}\nnext"[..], MAX_LINE).await.unwrap(),
			Some(b"{}\n".to_vec())
		);
	}
}
