//! Latest-wins engine-to-frontend viewport stream
//! (`crates/libaryon/plans/adr/0008-latest-viewport-delivery.md`).
//!
//! A [`Viewport`] is mostly state, so a frame the frontend has not read yet is
//! replaced by the next one and a stalled frontend holds one frame, not every
//! frame since it stopped reading. Two fields are occurrences the engine emits
//! in exactly one frame, and they survive replacement:
//!
//! - `status_message`: an unread frame's message passes to the newer frame when
//!   that frame has none, matching frontends that keep the latest message.
//! - `document_text` with its buffer and state ids: a frame carrying this reply
//!   to `QueryDocumentText` is never replaced. Reply frames queue in order, and
//!   a frontend that pairs one pending query with each reply still sees every
//!   reply.
//!
//! `should_quit` is OR-ed into the newer frame. A pending frame stays readable
//! after the sender is dropped, so the engine's final frame is not lost.
//!
//! The sender blocks only when [`REPLY_LIMIT`] replies are unread: a frontend
//! that keeps querying without reading would otherwise grow the reply queue
//! without bound. The receiver keeps `std::sync::mpsc` method names and error
//! types.

use crate::uast::Viewport;
use std::collections::VecDeque;
use std::sync::mpsc::{RecvError, RecvTimeoutError, SendError, TryRecvError};
use std::sync::{Arc, Condvar, Mutex};
use std::time::{Duration, Instant};

/// Unread document-text replies held before [`ViewportSender::send`] waits.
/// The same as the engine's request queue: a frontend cannot have more
/// queries outstanding than that queue admits at once.
pub(crate) const REPLY_LIMIT: usize = crate::engine::request::ENGINE_REQUEST_QUEUE_LIMIT;

/// How a frame type coalesces. Only [`Viewport`] implements it outside tests.
pub(crate) trait Frame {
	/// A reply that must be delivered, never replaced.
	fn is_reply(&self) -> bool;
	/// Take what an unread, replaced frame must hand on.
	fn inherit(&mut self, older: Self);
}

impl Frame for Viewport {
	fn is_reply(&self) -> bool {
		self.document_text.is_some()
	}

	fn inherit(&mut self, older: Self) {
		self.should_quit |= older.should_quit;
		if self.status_message.is_none() {
			self.status_message = older.status_message;
		}
	}
}

struct Slot<T> {
	/// Reply frames, oldest first. Every one is older than `latest`.
	replies: VecDeque<T>,
	latest: Option<T>,
	senders: usize,
	receiver_alive: bool,
}

impl<T> Slot<T> {
	fn take(&mut self) -> Option<T> {
		self.replies.pop_front().or_else(|| self.latest.take())
	}

	fn is_disconnected(&self) -> bool {
		self.senders == 0 && self.replies.is_empty() && self.latest.is_none()
	}
}

struct Shared<T> {
	slot: Mutex<Slot<T>>,
	/// Signalled when a frame arrives or the last sender drops.
	ready: Condvar,
	/// Signalled when the reader takes a frame or drops.
	space: Condvar,
}

pub fn channel() -> (ViewportSender, ViewportReceiver) {
	let (sender, receiver) = frame_channel();
	(ViewportSender(sender), ViewportReceiver(receiver))
}

/// The engine's end of the viewport stream.
#[derive(Clone)]
pub struct ViewportSender(Sender<Viewport>);

impl ViewportSender {
	/// Publish `viewport`, replacing an unread snapshot. Fails, returning the
	/// frame, once the receiver is gone.
	pub fn send(&self, viewport: Viewport) -> Result<(), SendError<Viewport>> {
		self.0.send(viewport)
	}
}

/// A frontend's end of the viewport stream; see the module docs.
pub struct ViewportReceiver(Receiver<Viewport>);

impl ViewportReceiver {
	pub fn try_recv(&self) -> Result<Viewport, TryRecvError> {
		self.0.try_recv()
	}

	pub fn recv(&self) -> Result<Viewport, RecvError> {
		self.0.recv()
	}

	pub fn recv_timeout(&self, timeout: Duration) -> Result<Viewport, RecvTimeoutError> {
		self.0.recv_timeout(timeout)
	}

	/// Frames available now, without waiting.
	pub fn try_iter(&self) -> impl Iterator<Item = Viewport> + '_ {
		self.0.try_iter()
	}
}

pub(crate) fn frame_channel<T: Frame>() -> (Sender<T>, Receiver<T>) {
	let shared = Arc::new(Shared {
		slot: Mutex::new(Slot {
			replies: VecDeque::new(),
			latest: None,
			senders: 1,
			receiver_alive: true,
		}),
		ready: Condvar::new(),
		space: Condvar::new(),
	});
	(
		Sender {
			shared: Arc::clone(&shared),
		},
		Receiver { shared },
	)
}

pub(crate) struct Sender<T> {
	shared: Arc<Shared<T>>,
}

impl<T: Frame> Sender<T> {
	/// Publish `viewport`, replacing an unread snapshot. Fails, returning the
	/// frame, once the receiver is gone.
	pub(crate) fn send(&self, mut viewport: T) -> Result<(), SendError<T>> {
		let mut slot = self.shared.slot.lock().unwrap();
		if let Some(older) = slot.latest.take() {
			viewport.inherit(older);
		}
		let is_reply = viewport.is_reply();
		if is_reply {
			while slot.receiver_alive && slot.replies.len() >= REPLY_LIMIT {
				slot = self.shared.space.wait(slot).unwrap();
			}
		}
		if !slot.receiver_alive {
			return Err(SendError(viewport));
		}
		if is_reply {
			slot.replies.push_back(viewport);
		} else {
			slot.latest = Some(viewport);
		}
		drop(slot);
		self.shared.ready.notify_one();
		Ok(())
	}
}

impl<T> Clone for Sender<T> {
	fn clone(&self) -> Self {
		self.shared.slot.lock().unwrap().senders += 1;
		Self {
			shared: Arc::clone(&self.shared),
		}
	}
}

impl<T> Drop for Sender<T> {
	fn drop(&mut self) {
		let mut slot = self.shared.slot.lock().unwrap();
		slot.senders -= 1;
		if slot.senders == 0 {
			drop(slot);
			self.shared.ready.notify_all();
		}
	}
}

pub(crate) struct Receiver<T> {
	shared: Arc<Shared<T>>,
}

impl<T> Receiver<T> {
	pub(crate) fn try_recv(&self) -> Result<T, TryRecvError> {
		let mut slot = self.shared.slot.lock().unwrap();
		match slot.take() {
			Some(viewport) => {
				drop(slot);
				self.shared.space.notify_one();
				Ok(viewport)
			}
			None if slot.is_disconnected() => Err(TryRecvError::Disconnected),
			None => Err(TryRecvError::Empty),
		}
	}

	pub(crate) fn recv(&self) -> Result<T, RecvError> {
		let mut slot = self.shared.slot.lock().unwrap();
		loop {
			if let Some(viewport) = slot.take() {
				drop(slot);
				self.shared.space.notify_one();
				return Ok(viewport);
			}
			if slot.is_disconnected() {
				return Err(RecvError);
			}
			slot = self.shared.ready.wait(slot).unwrap();
		}
	}

	pub(crate) fn recv_timeout(&self, timeout: Duration) -> Result<T, RecvTimeoutError> {
		let deadline = Instant::now() + timeout;
		let mut slot = self.shared.slot.lock().unwrap();
		loop {
			if let Some(viewport) = slot.take() {
				drop(slot);
				self.shared.space.notify_one();
				return Ok(viewport);
			}
			if slot.is_disconnected() {
				return Err(RecvTimeoutError::Disconnected);
			}
			let Some(left) = deadline.checked_duration_since(Instant::now()) else {
				return Err(RecvTimeoutError::Timeout);
			};
			slot = self.shared.ready.wait_timeout(slot, left).unwrap().0;
		}
	}

	pub(crate) fn try_iter(&self) -> impl Iterator<Item = T> + '_ {
		std::iter::from_fn(|| self.try_recv().ok())
	}
}

impl<T> Drop for Receiver<T> {
	fn drop(&mut self) {
		self.shared.slot.lock().unwrap().receiver_alive = false;
		self.shared.space.notify_all();
	}
}

#[cfg(test)]
mod tests {
	use super::{Frame, REPLY_LIMIT, frame_channel};
	use std::sync::mpsc::{RecvTimeoutError, TryRecvError};
	use std::time::Duration;

	/// The viewport fields the channel acts on, plus an identity.
	#[derive(Debug, Default)]
	struct Shot {
		id: u32,
		status: Option<&'static str>,
		reply: Option<&'static str>,
		quit: bool,
	}

	impl Frame for Shot {
		fn is_reply(&self) -> bool {
			self.reply.is_some()
		}

		fn inherit(&mut self, older: Self) {
			self.quit |= older.quit;
			if self.status.is_none() {
				self.status = older.status;
			}
		}
	}

	fn status(status: &'static str) -> Shot {
		Shot {
			status: Some(status),
			..Shot::default()
		}
	}

	fn reply(text: &'static str) -> Shot {
		Shot {
			reply: Some(text),
			..Shot::default()
		}
	}

	#[test]
	fn a_stalled_reader_holds_only_the_latest_snapshot() {
		let (tx, rx) = frame_channel();
		for id in 0..1_000 {
			tx.send(Shot {
				id,
				..Shot::default()
			})
			.unwrap();
		}
		assert_eq!(rx.try_recv().unwrap().id, 999);
		assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
	}

	#[test]
	fn replaced_frames_hand_on_status_and_quit() {
		let (tx, rx) = frame_channel();
		tx.send(Shot {
			status: Some("saved"),
			quit: true,
			..Shot::default()
		})
		.unwrap();
		tx.send(Shot::default()).unwrap();
		let latest = rx.try_recv().unwrap();
		assert_eq!(latest.status, Some("saved"));
		assert!(latest.quit);

		tx.send(status("old")).unwrap();
		tx.send(status("new")).unwrap();
		assert_eq!(rx.try_recv().unwrap().status, Some("new"));
	}

	#[test]
	fn every_reply_arrives_in_order_before_newer_snapshots() {
		let (tx, rx) = frame_channel();
		tx.send(status("before")).unwrap();
		tx.send(reply("first")).unwrap();
		tx.send(Shot::default()).unwrap();
		tx.send(reply("second")).unwrap();
		tx.send(status("after")).unwrap();

		let first = rx.try_recv().unwrap();
		assert_eq!(first.reply, Some("first"));
		assert_eq!(first.status, Some("before"));
		assert_eq!(rx.try_recv().unwrap().reply, Some("second"));
		let latest = rx.try_recv().unwrap();
		assert_eq!(latest.reply, None);
		assert_eq!(latest.status, Some("after"));
		assert!(matches!(rx.try_recv(), Err(TryRecvError::Empty)));
	}

	#[test]
	fn the_reply_bound_makes_the_sender_wait_for_the_reader() {
		let (tx, rx) = frame_channel();
		for _ in 0..REPLY_LIMIT {
			tx.send(reply("queued")).unwrap();
		}
		let sender = std::thread::spawn(move || tx.send(reply("waiting")).is_ok());
		std::thread::sleep(Duration::from_millis(50));
		assert!(!sender.is_finished(), "the sender waits at the reply bound");
		rx.try_recv().unwrap();
		assert!(sender.join().unwrap());
		assert_eq!(rx.try_iter().count(), REPLY_LIMIT);
	}

	#[test]
	fn the_final_frame_outlives_the_sender_then_the_stream_disconnects() {
		let (tx, rx) = frame_channel();
		tx.send(Shot {
			quit: true,
			..Shot::default()
		})
		.unwrap();
		drop(tx);
		assert!(rx.recv().unwrap().quit);
		assert!(rx.recv().is_err());
		assert!(matches!(
			rx.recv_timeout(Duration::from_millis(1)),
			Err(RecvTimeoutError::Disconnected)
		));
	}

	#[test]
	fn sending_after_the_reader_is_gone_fails_and_recv_times_out() {
		let (tx, rx) = frame_channel::<Shot>();
		assert!(matches!(
			rx.recv_timeout(Duration::from_millis(5)),
			Err(RecvTimeoutError::Timeout)
		));
		drop(rx);
		assert!(tx.send(Shot::default()).is_err());
	}
}
