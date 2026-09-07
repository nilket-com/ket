//! PRODUCT-0160: the socket is a subscription to the update thread.

use super::actions::{Pending, Resize, ResizeResult};
use super::{AppState, Message};
#[cfg(test)]
use crate::driver::Command;
use crate::driver::{CommandReceipt, Failure, Outcome, PublishRefusal, published_receipt};
use crate::live::{
	Event,
	discovery::{Window, runtime_directory},
	transport::{Inbound, Reply, Server, Sink},
};
use iced::Task;
use std::collections::HashMap;

#[derive(Default)]
pub(super) struct Agents {
	window: Option<Window>,
	server: Option<Server>,
	generation: u64,
	pub(super) closing: bool,
	connections: HashMap<u64, Sink>,
	last: Vec<(u64, u64, u64, u32, u32)>,
	resizes: Vec<PendingResize>,
}

struct PendingResize {
	id: u64,
	reply: Reply,
	receipt: CommandReceipt,
	resize: Resize,
}

impl Agents {
	pub(super) fn subscription(&self) -> iced::Subscription<Message> {
		let socket = self
			.server
			.as_ref()
			.map_or_else(iced::Subscription::none, |server| {
				server.subscription().map(Message::AgentInbound)
			});
		let timer = if self.resizes.is_empty() {
			iced::Subscription::none()
		} else {
			iced::time::every(std::time::Duration::from_millis(50)).map(Message::AgentTick)
		};
		iced::Subscription::batch([socket, timer])
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
	pub(super) fn disable(&mut self) {
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
	#[cfg(test)]
	pub(super) fn enable_agents_for_test(&mut self) -> std::path::PathBuf {
		let root = std::env::temp_dir().join(format!(
			"p16-actions-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		self.agents.window = Some(Window::create(root.clone(), None, vec![]).unwrap());
		self.toggle_agents().unwrap();
		root
	}

	#[cfg(test)]
	pub(super) fn agent_request_for_test(
		&self,
		command: crate::driver::Command,
		id: u64,
	) -> (Message, Sink, tokio::sync::mpsc::Receiver<Event>) {
		let (sink, reply, answers) = crate::live::transport::test_request();
		(
			Message::AgentInbound(Inbound::Request {
				connection: 1,
				generation: self.agents.generation,
				request: crate::live::Request { id, command },
				reply,
			}),
			sink,
			answers,
		)
	}

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
		if self.agents.closing {
			return Err("the window is closing".into());
		}
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

	pub(super) fn agent_inbound(&mut self, inbound: Inbound) -> Task<Message> {
		match inbound {
			Inbound::Connected {
				connection,
				generation,
				sink,
			} => {
				if self.agents.server.is_none() || generation != self.agents.generation {
					sink.close("gate_closed");
					return Task::none();
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
				connection,
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
					return Task::none();
				}
				let _connection = connection; // Reply carries this connection's bounded answer queue.
				let started = self.start_command(&request.command);
				let receipt = started.receipt;
				if receipt.outcome == Outcome::Refused {
					reply.answer(Event::Refused {
						id: request.id,
						reason: "command_refused".into(),
						receipt: Some(receipt),
					});
					return started.task;
				}
				reply.answer(Event::Applied {
					id: request.id,
					terminal: started.pending.is_none(),
					receipt: receipt.clone(),
				});
				match started.pending {
					None => return started.task,
					Some(Pending::Resize(resize)) => {
						self.agents.resizes.push(PendingResize {
							id: request.id,
							reply,
							receipt,
							resize,
						});
						return started.task;
					}
					Some(Pending::Publish(job)) => {
						let root = job.root.display().to_string();
						let work_root = job.root.clone();
						let task = Task::perform(
							async move {
								tokio::task::spawn_blocking(move || {
									job.run()
										.map(|package| published_receipt(&work_root, &package))
										.map_err(|error| PublishRefusal::from(&error))
								})
								.await
								.unwrap_or_else(|error| {
									Err(PublishRefusal::Write {
										error: format!("publish task failed: {error}"),
									})
								})
							},
							move |result| Message::AgentPublishFinished {
								id: request.id,
								reply,
								receipt,
								root,
								result,
							},
						);
						return Task::batch([started.task, task]);
					}
				}
			}
		}
		Task::none()
	}

	pub(super) fn refresh_agents(&mut self) {
		self.finish_agent_resizes(std::time::Instant::now());
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

impl AppState {
	pub(super) fn finish_agent_resizes(&mut self, now: std::time::Instant) {
		let pending = std::mem::take(&mut self.agents.resizes);
		for mut waiting in pending {
			if waiting.reply.is_closed() {
				continue;
			}
			let event = match self.resize_result(&waiting.resize, now) {
				ResizeResult::Waiting => {
					self.agents.resizes.push(waiting);
					continue;
				}
				ResizeResult::Completed(state) => {
					waiting.receipt.state = Some(state);
					Event::Completed {
						id: waiting.id,
						receipt: waiting.receipt,
					}
				}
				ResizeResult::Superseded(current) => Event::Superseded {
					id: waiting.id,
					figure: waiting.resize.incarnation.token().to_string(),
					current,
				},
				ResizeResult::Failed(state) => {
					waiting.receipt.state = Some(state);
					waiting.receipt.fail(Failure::ResizeDeadline {
						figure: waiting.resize.incarnation.token().to_string(),
					});
					Event::Failed {
						id: waiting.id,
						error: None,
						reason: "resize_deadline".into(),
						root: None,
						receipt: waiting.receipt,
					}
				}
			};
			waiting.reply.answer(event);
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
		let (message, _sink, mut answers) = state.agent_request_for_test(
			Command::Publish {
				directory: root.join("empty-package").display().to_string(),
			},
			8,
		);
		drop(super::super::update(&mut state, message));
		assert!(
			matches!(answers.try_recv().unwrap(), Event::Refused { receipt: Some(receipt), .. }
			if matches!(receipt.refusal, Some(crate::driver::Refusal::PublishRefused { reason: PublishRefusal::NoFigures })))
		);
		assert!(!root.join("empty-package").exists());
		drop(super::super::begin_window_shutdown(&mut state));
		assert!(!socket.exists());
		assert!(state.toggle_agents().is_err());
		drop(state);
		assert!(!socket.exists());
		std::fs::remove_dir_all(root).unwrap();
	}
}
