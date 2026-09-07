//! PRODUCT-0160 observe cut: the socket is a subscription to the update thread.

use super::{AppState, Message};
use crate::driver::{Command, CommandReceipt, Outcome};
use crate::live::{
	Event,
	discovery::{Window, runtime_directory},
	transport::{Inbound, Server, Sink},
};
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct Agents {
	window: Option<Window>,
	server: Option<Server>,
	generation: u64,
	connections: HashMap<u64, Sink>,
	last: Vec<(u64, u64, u64, u32, u32)>,
}

impl Agents {
	pub(super) fn subscription(&self) -> iced::Subscription<Message> {
		self.server
			.as_ref()
			.map_or_else(iced::Subscription::none, |server| {
				server.subscription().map(Message::AgentInbound)
			})
	}
	pub(super) fn status(&self) -> (bool, usize) {
		(
			self.server.is_some(),
			self.connections
				.values()
				.filter(|sink| !sink.is_closed())
				.count(),
		)
	}
	fn disable(&mut self) {
		for sink in self.connections.values() {
			sink.close("gate_closed");
		}
		self.connections.clear();
		self.server = None;
		if let Some(window) = &mut self.window {
			let _ = std::fs::remove_file(&window.advertisement.socket);
			window.advertisement.accepting = false;
			let _ = window.write();
		}
	}
}

impl Drop for Agents {
	fn drop(&mut self) {
		self.disable();
	}
}

impl AppState {
	pub(super) fn advertise_window(&mut self, accepting: bool) -> Result<(), String> {
		self.agents.window = Some(
			Window::create(
				runtime_directory().map_err(|e| e.to_string())?,
				self.session_id.map(|id| id.to_string()),
				self.code_editor.open_source_paths(),
			)
			.map_err(|e| e.to_string())?,
		);
		if accepting {
			self.toggle_agents()?;
		}
		Ok(())
	}

	pub(super) fn toggle_agents(&mut self) -> Result<(), String> {
		if self.agents.server.is_some() {
			self.agents.disable();
			return Ok(());
		}
		let window = self
			.agents
			.window
			.as_mut()
			.ok_or("this application has no window advertisement")?;
		self.agents.generation = self
			.agents
			.generation
			.checked_add(1)
			.ok_or("connection generation exhausted")?;
		self.agents.server = Some(
			Server::bind(
				&window.advertisement.socket,
				&window.advertisement.window,
				self.agents.generation,
			)
			.map_err(|e| e.to_string())?,
		);
		window.advertisement.accepting = true;
		window.write().map_err(|e| e.to_string())
	}

	pub(super) fn agent_inbound(&mut self, inbound: Inbound) {
		match inbound {
			Inbound::Connected {
				connection,
				generation,
				sink,
			} => {
				if self.agents.server.is_none() || generation != self.agents.generation {
					sink.close("gate_closed");
					return;
				}
				let window = self
					.agents
					.window
					.as_ref()
					.expect("server has an advertisement")
					.advertisement
					.clone();
				sink.answer(Event::Welcome {
					window,
					generation,
					figures: self.describe_surface(),
				});
				self.agents.connections.insert(connection, sink);
			}
			Inbound::Disconnected {
				connection,
				generation,
			} => {
				if generation == self.agents.generation {
					self.agents.connections.remove(&connection);
				}
			}
			Inbound::Request {
				connection: _,
				generation,
				request,
				reply,
			} => {
				if self.agents.server.is_none() || generation != self.agents.generation {
					reply.answer(Event::Refused {
						id: request.id,
						reason: "gate_closed".into(),
						receipt: None,
					});
					return;
				}
				match request.command {
					Command::Describe {} => reply.answer(Event::Applied {
						id: request.id,
						terminal: true,
						receipt: CommandReceipt {
							command: request.command,
							outcome: Outcome::Applied,
							figure: None,
							refusal: None,
							state: None,
							figures: Some(self.describe_surface()),
							publish: None,
						},
					}),
					_ => reply.answer(Event::Refused {
						id: request.id,
						reason: "observe_only".into(),
						receipt: None,
					}),
				}
			}
		}
	}

	pub(super) fn refresh_agents(&mut self) {
		if let Some(window) = &mut self.agents.window {
			let sources = self.code_editor.open_source_paths();
			if window.advertisement.sources != sources {
				window.advertisement.sources = sources;
				if let Err(error) = window.write() {
					self.status_error = format!("Agent advertisement: {error}");
				}
			}
		}
		if self.agents.connections.is_empty() {
			return;
		}
		let stamp = self
			.code_editor
			.host_plot_exports_with_identity()
			.iter()
			.map(|(panel, incarnation, export)| {
				(
					panel.get(),
					incarnation.token(),
					export.render_revision,
					export.width.to_bits(),
					export.height.to_bits(),
				)
			})
			.collect::<Vec<_>>();
		if stamp != self.agents.last {
			self.agents.last = stamp;
			let event = Event::Changed {
				figures: self.describe_surface(),
			};
			for sink in self.agents.connections.values() {
				sink.changed(event.clone());
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::live::{Request, transport::test_request};

	#[test]
	fn product_0160_update_rejects_queued_requests_across_gate_generations() {
		let (mut state, task) =
			super::super::new(crate::configuration::StartupData::default(), None);
		drop(task);
		let root = std::env::temp_dir().join(format!(
			"p16-gate-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		state.agents.window = Some(Window::create(root.clone(), None, vec![]).unwrap());
		let socket = state
			.agents
			.window
			.as_ref()
			.unwrap()
			.advertisement
			.socket
			.clone();
		assert!(!socket.exists());
		state.toggle_agents().unwrap();
		assert!(socket.exists());
		let first_generation = state.agents.generation;
		let (sink, _, mut welcome) = test_request();
		drop(super::super::update(
			&mut state,
			Message::AgentInbound(Inbound::Connected {
				connection: 1,
				generation: first_generation,
				sink: sink.clone(),
			}),
		));
		assert!(matches!(welcome.try_recv().unwrap(), Event::Welcome { .. }));
		assert_eq!(state.agents.status(), (true, 1));
		state.toggle_agents().unwrap();
		assert!(!socket.exists());
		assert!(sink.is_closed());
		assert_eq!(state.agents.status(), (false, 0));
		for reopened in [false, true] {
			if reopened {
				state.toggle_agents().unwrap();
			}
			let (_, reply, mut answers) = test_request();
			drop(super::super::update(
				&mut state,
				Message::AgentInbound(Inbound::Request {
					connection: 1,
					generation: first_generation,
					request: Request {
						id: 7,
						command: Command::Describe {},
					},
					reply,
				}),
			));
			assert_eq!(
				answers.try_recv().unwrap(),
				Event::Refused {
					id: 7,
					reason: "gate_closed".into(),
					receipt: None
				}
			);
		}
		assert!(state.agents.generation > first_generation);
		drop(state);
		assert!(!socket.exists());
		std::fs::remove_dir_all(root).unwrap();
	}
}
