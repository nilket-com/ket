//! Bounded sockets deliver messages, never references to application state.

use super::{Event, MAX_ANSWERS, MAX_LINE, MAX_OUTSTANDING, Request};
use iced::futures::{SinkExt, StreamExt};
use std::hash::{Hash, Hasher};
use std::io;
#[cfg(unix)]
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
	pub(crate) fn is_closed(&self) -> bool {
		self.sink.is_closed()
	}
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
	listener: Arc<Mutex<Option<PlatformListener>>>,
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
		let listener = PlatformListener::bind(path)?;
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
		let Ok(mut listener) = listener.into_async() else { return; };
		let mut stop = worker.stop.clone();
		let mut connections = tokio::task::JoinSet::new();
		let mut next = 0_u64;
		loop {
			tokio::select! {
				biased;
				_ = stop.changed() => break,
				_ = connections.join_next(), if !connections.is_empty() => {},
				accepted = listener.accept() => {
					let socket = match accepted {
						Accepted::Peer(socket) => socket,
						Accepted::Foreign => continue,
						Accepted::Closed => break,
					};
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

async fn connection<S: tokio::io::AsyncRead + tokio::io::AsyncWrite + Unpin + Send + 'static>(
	socket: S,
	id: u64,
	generation: u64,
	sink: Sink,
	mut receivers: Receivers,
	mut output: iced::futures::channel::mpsc::Sender<Inbound>,
	mut stop: watch::Receiver<bool>,
) {
	let (reader, mut writer) = tokio::io::split(socket);
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

	/// ADR 0166 §4, acceptance 4: the pipe's DACL admits the running user
	/// alone, the same policy a private key must satisfy; a same-user client
	/// is a peer, and the name cannot be taken over by a second server.
	/// (A client from another account never reaches `accept`: the kernel
	/// refuses it on the DACL, so that case is the descriptor check here.)
	#[cfg(windows)]
	#[tokio::test]
	async fn adr_0166_windows_pipe_admits_the_running_user_alone() {
		use std::os::windows::io::AsRawHandle;
		let window = uuid::Uuid::new_v4().simple().to_string();
		let name = super::super::discovery::endpoint_for(Path::new(""), &window);
		let mut listener = PlatformListener::bind(&name).unwrap().into_async().unwrap();
		assert!(
			super::windows_pipe::create(&name.to_string_lossy(), true).is_err(),
			"a second first-instance creator is refused"
		);
		let (accepted, client) = tokio::join!(listener.accept(), test_connect(&name));
		let Accepted::Peer(server) = accepted else {
			panic!("a same-user client is a peer");
		};
		// SAFETY: `server` keeps the handle open for the call.
		unsafe { key_guard::windows::pipe_admits_current_user_only(server.as_raw_handle() as _) }
			.expect("the pipe instance the client reached admits the running user alone");
		assert!(super::windows_pipe::client_is_same_user(&server));
		drop(client);
		// The name outlives the first session: a second client, after the
		// first has gone, reaches a fresh instance under the same DACL.
		drop(server);
		let (accepted, client) = tokio::join!(listener.accept(), test_connect(&name));
		assert!(
			matches!(accepted, Accepted::Peer(_)),
			"second client is a peer"
		);
		drop(client);
		// An accept abandoned mid-wait, as the subscription select does when
		// a session ends, must not lose the waiting instance.
		drop(accepted);
		tokio::select! {
			_ = listener.accept() => panic!("nobody connected"),
			_ = tokio::time::sleep(std::time::Duration::from_millis(20)) => {}
		}
		let (accepted, client) = tokio::join!(listener.accept(), test_connect(&name));
		assert!(
			matches!(accepted, Accepted::Peer(_)),
			"third client, after an abandoned accept, is a peer"
		);
		drop(client);
	}

	/// The client end of the platform transport, for tests on either platform.
	#[cfg(unix)]
	async fn test_connect(path: &Path) -> tokio::net::UnixStream {
		tokio::net::UnixStream::connect(path).await.unwrap()
	}
	#[cfg(windows)]
	async fn test_connect(path: &Path) -> tokio::net::windows::named_pipe::NamedPipeClient {
		super::super::client::windows_open(path.as_os_str())
			.await
			.unwrap()
	}

	fn picture(title: &str) -> Event {
		Event::Changed {
			figures: vec![crate::driver::FigureDescription {
				figure: "1".into(),
				pane: 1,
				title: title.into(),
				view: None,
				family: None,
				size: crate::driver::Size::default(),
				series: crate::driver::SeriesReport::Absent {
					reason: "test".into(),
				},
				loaded: None,
				live: crate::driver::LiveState::default(),
				render_mode: None,
				settings: Default::default(),
			}],
		}
	}

	#[tokio::test]
	async fn product_0160_slow_socket_gets_latest_picture_and_full_answers_disconnect() {
		use tokio::io::AsyncReadExt;
		for overflow in [false, true] {
			let (server, client) = tokio::io::duplex(64 * 1024);
			let (sink, receivers) = queues();
			let (output, _events) = iced::futures::channel::mpsc::channel(8);
			let (_stop, stopping) = watch::channel(false);
			let job = tokio::spawn(connection(
				server,
				1,
				1,
				sink.clone(),
				receivers,
				output,
				stopping,
			));
			// This answer exceeds the socket send buffer. Reading one byte proves
			// the writer started; it cannot finish while this client stops reading.
			sink.answer(Event::Refused {
				id: 0,
				reason: "x".repeat(2 * 1024 * 1024),
				receipt: None,
			});
			let mut client = BufReader::new(client);
			let first = client.read_u8().await.unwrap();
			assert_eq!(first, b'{');
			if overflow {
				let started = std::time::Instant::now();
				for id in 1..=MAX_ANSWERS + 1 {
					sink.answer(Event::Refused {
						id: id as u64,
						reason: "busy".into(),
						receipt: None,
					});
				}
				assert!(started.elapsed() < std::time::Duration::from_secs(1));
				assert!(sink.is_closed());
				tokio::time::timeout(std::time::Duration::from_secs(2), job)
					.await
					.unwrap()
					.unwrap();
			} else {
				for id in 0..1000 {
					sink.changed(picture(&id.to_string()));
				}
				// Finish the large answer, then receive just the newest snapshot.
				read_line(&mut client, usize::MAX).await.unwrap().unwrap();
				let newest = read_line(&mut client, usize::MAX).await.unwrap().unwrap();
				assert_eq!(
					super::super::decode::<Event>(&newest).unwrap(),
					picture("999")
				);
				assert!(
					tokio::time::timeout(
						std::time::Duration::from_millis(30),
						read_line(&mut client, MAX_LINE)
					)
					.await
					.is_err()
				);
				sink.close("done");
				job.await.unwrap();
			}
		}
	}

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
		#[cfg(unix)]
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
		let socket = test_connect(&window.advertisement.socket).await;
		let (read, mut write) = tokio::io::split(socket);
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
		let mut second = test_connect(&window.advertisement.socket).await;
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
		second.write_all(&vec![b'x'; MAX_LINE + 1]).await.unwrap();
		let mut second = BufReader::new(second);
		let bytes = tokio::time::timeout(
			std::time::Duration::from_secs(2),
			read_line(&mut second, MAX_LINE),
		)
		.await
		.unwrap()
		.unwrap()
		.unwrap();
		assert!(
			matches!(super::super::decode::<Event>(&bytes).unwrap(), Event::Closed { reason } if reason == "request_line_too_long")
		);
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

// ============================================================
// ADR 0166 §4: one framed protocol over the platform's same-user transport.
// ============================================================

/// What an accept produced: a same-user peer, a foreign peer to ignore, or
/// the end of the listener.
enum Accepted {
	Peer(PlatformStream),
	Foreign,
	Closed,
}

#[cfg(unix)]
pub(crate) type PlatformStream = tokio::net::UnixStream;

#[cfg(unix)]
pub(crate) struct PlatformListener(std::os::unix::net::UnixListener);

#[cfg(unix)]
struct AsyncListener(tokio::net::UnixListener);

#[cfg(unix)]
impl PlatformListener {
	pub(crate) fn bind(path: &Path) -> io::Result<Self> {
		let listener = std::os::unix::net::UnixListener::bind(path)?;
		std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o600))?;
		listener.set_nonblocking(true)?;
		Ok(Self(listener))
	}
	fn into_async(self) -> io::Result<AsyncListener> {
		tokio::net::UnixListener::from_std(self.0).map(AsyncListener)
	}
}

#[cfg(unix)]
impl AsyncListener {
	async fn accept(&mut self) -> Accepted {
		let Ok((socket, _)) = self.0.accept().await else {
			return Accepted::Closed;
		};
		// SAFETY: geteuid has no preconditions and cannot fail.
		let uid = unsafe { libc::geteuid() };
		if socket.peer_cred().is_ok_and(|peer| peer.uid() == uid) {
			Accepted::Peer(socket)
		} else {
			Accepted::Foreign
		}
	}
}

#[cfg(windows)]
pub(crate) type PlatformStream = tokio::net::windows::named_pipe::NamedPipeServer;

/// A named pipe whose DACL admits the running user alone; the DACL is the
/// same-user check, applied by the kernel before `connect` completes.
#[cfg(windows)]
pub(crate) struct PlatformListener {
	name: String,
}

#[cfg(windows)]
struct AsyncListener {
	name: String,
	waiting: Option<tokio::net::windows::named_pipe::NamedPipeServer>,
}

#[cfg(windows)]
impl PlatformListener {
	/// Binding records the name; the first pipe instance is created inside the
	/// subscription's runtime, which tokio's named pipes require.
	pub(crate) fn bind(path: &Path) -> io::Result<Self> {
		Ok(Self {
			name: path.to_string_lossy().into_owned(),
		})
	}
	fn into_async(self) -> io::Result<AsyncListener> {
		let first = windows_pipe::create(&self.name, true)?;
		Ok(AsyncListener {
			name: self.name,
			waiting: Some(first),
		})
	}
}

#[cfg(windows)]
impl AsyncListener {
	/// Cancel-safe: the waiting instance is taken only once a client has
	/// connected, so a select that abandons a pending accept loses no
	/// instance and the name stays served.
	async fn accept(&mut self) -> Accepted {
		let Some(waiting) = self.waiting.as_ref() else {
			return Accepted::Closed;
		};
		if waiting.connect().await.is_err() {
			return Accepted::Closed;
		}
		let Some(server) = self.waiting.take() else {
			return Accepted::Closed;
		};
		// The next instance must exist before this one is handed out, or a
		// client arriving in between finds no pipe and reports "not found".
		match windows_pipe::create(&self.name, false) {
			Ok(next) => self.waiting = Some(next),
			Err(_) => return Accepted::Closed,
		}
		if windows_pipe::client_is_same_user(&server) {
			Accepted::Peer(server)
		} else {
			// Never reached with the owner-only DACL in place; belt and braces.
			let _ = server.disconnect();
			Accepted::Foreign
		}
	}
}

#[cfg(windows)]
mod windows_pipe {
	use std::io;
	use std::os::windows::io::AsRawHandle;
	use tokio::net::windows::named_pipe::{NamedPipeServer, ServerOptions};

	/// Create one pipe instance with an owner-only DACL. `first` reserves the
	/// name: a second creator of the same name fails instead of hijacking it.
	pub(super) fn create(name: &str, first: bool) -> io::Result<NamedPipeServer> {
		let descriptor = key_guard::windows::current_user_only_security_descriptor()
			.map_err(io::Error::other)?;
		let mut attributes = SecurityAttributes {
			length: std::mem::size_of::<SecurityAttributes>() as u32,
			descriptor: descriptor.as_ptr() as *mut _,
			inherit: 0,
		};
		// SAFETY: `attributes` outlives the call and points at a valid
		// self-relative security descriptor held by `descriptor`.
		unsafe {
			ServerOptions::new()
				.first_pipe_instance(first)
				.reject_remote_clients(true)
				.create_with_security_attributes_raw(name, &mut attributes as *mut _ as *mut _)
		}
	}

	#[repr(C)]
	struct SecurityAttributes {
		length: u32,
		descriptor: *mut core::ffi::c_void,
		inherit: i32,
	}

	/// The kernel already enforced the DACL; this confirms the client's
	/// process token belongs to the same user, so a misconfigured pipe cannot
	/// silently admit anyone.
	pub(super) fn client_is_same_user(server: &NamedPipeServer) -> bool {
		// SAFETY: `server` is a connected instance and outlives the call.
		unsafe { key_guard::windows::pipe_client_is_current_user(server.as_raw_handle() as _) }
			.unwrap_or(false)
	}
}
