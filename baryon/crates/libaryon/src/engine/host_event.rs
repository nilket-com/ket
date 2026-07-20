//! Ordered engine-to-frontend event stream.
//!
//! [`Viewport`](crate::uast::Viewport) projections are *state*: a frontend
//! coalesces them and keeps only the latest, which is correct for geometry,
//! focus, and content but silently drops anything that must be observed exactly
//! once. Host events are that second category — unhandled input, panel
//! lifecycle — so they travel on their own channel where ordering and delivery
//! are the contract.
//!
//! The channel is deliberately **unbounded**: a lossy bounded queue would
//! defeat the purpose. Frontends are expected to drain frequently. Send failure
//! means the frontend is gone, which is a normal shutdown condition rather than
//! an error.

use super::InputEvent;
use std::sync::mpsc;

/// Identity of a host-rendered panel. Stable across the panel's lifetime and
/// meaningful only to the frontend that created it; the engine treats it as an
/// opaque token for geometry, focus, and tab membership.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HostPanelId(u64);

impl HostPanelId {
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

/// Presentation metadata the engine persists for a host panel. Deliberately
/// free of any host data: no DataFrame, texture, or connection is representable
/// here.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct HostPanelDescriptor {
	/// Frontend-defined panel class, e.g. `"result-table"` or `"dashboard"`.
	pub kind: String,
	pub title: String,
}

/// Monotonic revision of the host-owned content behind a panel. The engine only
/// relays it, so the frontend can detect stale outputs after a re-run.
#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PanelRevision(u64);

impl PanelRevision {
	pub const fn new(value: u64) -> Self {
		Self(value)
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

/// An engine-to-frontend occurrence that must not be coalesced away.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum HostEvent {
	/// A key the engine declined to interpret while a host panel held workspace
	/// focus. The engine cannot know that `j` scrolls a table, so it forwards.
	UnhandledInput {
		panel_id: HostPanelId,
		input: InputEvent,
	},
	/// The **last** workspace reference to this panel is gone; the frontend may
	/// release its backing object.
	///
	/// Splits and tabs can hold several references to one `HostPanelId`, so
	/// this is deliberately not "a pane closed": closing one reference while
	/// others remain must not discard a DataFrame or texture still on screen
	/// elsewhere. The engine already refcounts views per buffer
	/// (`view_count_for_buffer(..) == 0` gating `release_buffer`); panel
	/// disposal follows the same discipline.
	///
	/// If per-pane lifecycle is ever needed, it belongs in a separate event
	/// carrying both `panel_id` and `pane_id` — not in this one.
	PanelDisposed { panel_id: HostPanelId },
	/// A persisted layout was restored and references a panel the frontend has
	/// no content for; it should render an unresolved placeholder.
	///
	/// Emitted once per panel identity, not once per visible reference, so a
	/// panel restored into two splits announces itself a single time.
	PanelRestored {
		panel_id: HostPanelId,
		descriptor: HostPanelDescriptor,
	},
	/// The panel was rebound to a new revision of its source.
	PanelRebound {
		panel_id: HostPanelId,
		revision: PanelRevision,
	},
}

/// Engine-side sender. Cloneable so any engine subsystem can emit.
#[derive(Clone)]
pub(crate) struct HostEventSender {
	tx: mpsc::Sender<HostEvent>,
}

impl HostEventSender {
	/// Emit an event. Returns `false` once the frontend has dropped its
	/// receiver, which is an ordinary shutdown condition: callers ignore it
	/// rather than treating a departed frontend as an error.
	pub(crate) fn send(&self, event: HostEvent) -> bool {
		self.tx.send(event).is_ok()
	}
}

/// Unbounded by contract — see the module docs.
pub(crate) fn channel() -> (HostEventSender, mpsc::Receiver<HostEvent>) {
	let (tx, rx) = mpsc::channel();
	(HostEventSender { tx }, rx)
}

#[cfg(test)]
mod tests {
	use super::{HostEvent, HostPanelDescriptor, HostPanelId, PanelRevision, channel};
	use crate::engine::{InputEvent, KeyCode, Modifiers};

	fn key(character: char) -> InputEvent {
		InputEvent::Key {
			code: KeyCode::Char(character),
			modifiers: Modifiers::NONE,
		}
	}

	#[test]
	fn a_burst_of_events_arrives_complete_and_in_order() {
		let (tx, rx) = channel();
		let panel_id = HostPanelId::new(7);
		// Unboundedness comes from the channel type, not from this test. What
		// this guards is the contract that depends on it: a burst larger than
		// any bounded queue in this crate survives complete and in order, so a
		// future switch to a lossy `try_send` fails here.
		for index in 0..10_000u32 {
			assert!(tx.send(HostEvent::UnhandledInput {
				panel_id,
				input: key(char::from(b'a' + (index % 26) as u8)),
			}));
		}
		let drained = rx.try_iter().collect::<Vec<_>>();
		assert_eq!(drained.len(), 10_000, "no host event may be dropped");
		for (index, event) in drained.iter().enumerate() {
			let expected = key(char::from(b'a' + (index % 26) as u8));
			assert_eq!(
				*event,
				HostEvent::UnhandledInput {
					panel_id,
					input: expected
				},
				"host events must arrive in emission order"
			);
		}
	}

	#[test]
	fn lifecycle_events_keep_their_relative_order() {
		let (tx, rx) = channel();
		let panel_id = HostPanelId::new(1);
		let descriptor = HostPanelDescriptor {
			kind: "result-table".to_string(),
			title: "Trip distribution".to_string(),
		};
		tx.send(HostEvent::PanelRestored {
			panel_id,
			descriptor: descriptor.clone(),
		});
		tx.send(HostEvent::PanelRebound {
			panel_id,
			revision: PanelRevision::new(2),
		});
		tx.send(HostEvent::PanelDisposed { panel_id });

		assert_eq!(
			rx.try_iter().collect::<Vec<_>>(),
			vec![
				HostEvent::PanelRestored {
					panel_id,
					descriptor
				},
				HostEvent::PanelRebound {
					panel_id,
					revision: PanelRevision::new(2)
				},
				HostEvent::PanelDisposed { panel_id },
			]
		);
	}

	#[test]
	fn a_departed_frontend_is_a_shutdown_condition_not_an_error() {
		let (tx, rx) = channel();
		drop(rx);
		assert!(
			!tx.send(HostEvent::PanelDisposed {
				panel_id: HostPanelId::new(1)
			}),
			"send reports the frontend is gone"
		);
	}
}
