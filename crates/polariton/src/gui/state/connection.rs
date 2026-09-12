//! ADR 0166 §2–§3: the one external-connection operation the form, the
//! profile menu, and the agent commands all enter. It owns the database
//! connection generation: every connect that starts, every result that
//! installs, and every disconnect mints the next one, and a result or a
//! disconnect naming any other generation is refused or ignored, never
//! applied to whatever stands now.

use super::{AppState, Message};
use crate::adapters::common::{AdapterStage, CancelCapability, DatabaseAdapter};
use crate::adapters::driver::{AdapterConfiguration, AdapterSelection, AdapterState};
use crate::driver::ConnectionDescription;
use iced::Task;
use std::collections::BTreeMap;
use std::sync::Arc;
use tokio::sync::RwLock;

impl AppState {
	/// The form's submission rules, shared with the `connect` command: no
	/// mixed input kinds, and every declared field that is neither secure
	/// nor optional present. Optional groups are judged by the adapter.
	/// The form's fields as they stand, judged as the form judges them on
	/// submit.
	pub(super) fn validate_connection_submission(&self) -> Result<(), String> {
		if let Some(error) = self.adapter_state.input_kind_error() {
			return Err(error);
		}
		let ad_hoc_active = !self
			.adapter_state
			.ad_hoc_connection_string
			.trim()
			.is_empty();
		Self::validate_candidate(
			&self.adapter_state.selection,
			&self.adapter_state.fields,
			ad_hoc_active,
		)
	}

	/// A candidate selection and field set, judged before anything live is
	/// touched: a refusal here leaves the installed connection, the reported
	/// adapter, and the generation exactly as they were.
	pub(super) fn validate_candidate(
		selection: &AdapterSelection,
		fields: &BTreeMap<String, String>,
		ad_hoc_active: bool,
	) -> Result<(), String> {
		let missing = !ad_hoc_active
			&& crate::adapters::driver::fields_for(selection)
				.iter()
				.filter(|f| !f.is_secure && !f.is_optional)
				.any(|f| fields.get(f.key).is_none_or(|v| v.trim().is_empty()));
		if missing {
			return Err("All connection fields are required.".to_owned());
		}
		// ADR 0165 §4: a partial TLS group is refused here, before any transport.
		#[cfg(feature = "toron")]
		if matches!(selection, AdapterSelection::Toron) && !ad_hoc_active {
			let field = |key: &str| fields.get(key).cloned().unwrap_or_default();
			crate::adapters::toron::target_from_fields(
				&field("endpoint"),
				&field("server_name"),
				&field("client_cert"),
				&field("client_key"),
				&field("ca_cert"),
			)?;
		}
		Ok(())
	}

	/// Mint the next database connection generation.
	/// Any attempt still in flight is retired: its result, whenever it
	/// arrives, belongs to an older generation and must not install.
	pub(super) fn next_connection_generation(&mut self) -> u64 {
		self.connection_generation = self.connection_generation.saturating_add(1);
		self.connect_in_flight = None;
		self.welcome.own_kind = None;
		self.welcome.own_data_ready = false;
		self.connection_generation
	}

	/// Configure from the current fields and start connecting. The attempt
	/// is tagged with a fresh generation; only a result carrying that
	/// generation may install.
	pub(super) fn begin_connect(&mut self) -> Task<Message> {
		self.adapter_state.configure();
		let configuration = self.adapter_state.configuration.clone();
		self.begin_connect_with(configuration)
	}

	pub(super) fn begin_connect_with(
		&mut self,
		configuration: AdapterConfiguration,
	) -> Task<Message> {
		self.adapter_state.configuration = configuration.clone();
		self.adapter_state.stage = AdapterStage::Configured;
		let generation = self.next_connection_generation();
		self.connect_in_flight = Some(generation);
		self.status_error.clear();
		self.status_msg = "Connecting…".into();
		Task::perform(
			async move { AdapterState::connect(configuration).await },
			move |result| Message::AdapterConnected { generation, result },
		)
	}

	/// A connect result arrived. Installs only when it is the in-flight
	/// attempt; anything else is a stale result and changes nothing.
	pub(super) fn connect_settled(
		&mut self,
		generation: u64,
		result: Result<(Arc<RwLock<dyn DatabaseAdapter>>, CancelCapability), String>,
	) -> Task<Message> {
		if self.connect_in_flight != Some(generation) {
			// A newer human or agent choice superseded this attempt. The
			// result is dropped; the newer choice stands.
			let stale = self.connection_description();
			self.complete_connect_command(
				generation,
				Err(format!(
					"connection attempt {generation} was superseded by generation {}",
					stale.generation
				)),
			);
			return Task::none();
		}
		self.connect_in_flight = None;
		let outcome = match result {
			Ok((connection, cancellation)) => {
				self.install_connection(connection, cancellation);
				Ok(())
			}
			Err(error) => {
				self.adapter_state.connection = None;
				// Scratch remains explicitly available for SQL, while the
				// attempted adapter remains visibly unattached.
				self.adapter_state.stage = AdapterStage::Configured;
				self.status_error = error.clone();
				self.status_msg = "Adapter unavailable; SQL scratch remains active.".into();
				Err(error)
			}
		};
		self.complete_connect_command(generation, outcome);
		Task::none()
	}

	/// The installed connection is generation `connection_generation`; the
	/// attempt's generation was minted when it began and is kept.
	pub(super) fn install_connection(
		&mut self,
		connection: Arc<RwLock<dyn DatabaseAdapter>>,
		cancellation: CancelCapability,
	) {
		self.adapter_state.connection = Some(connection);
		self.adapter_state.cancellation = cancellation;
		self.adapter_state.stage = AdapterStage::Connected;
		self.status_error.clear();
		self.status_msg = "Adapter connected.".into();
	}

	/// Tear down the external connection that `generation` names. A stale
	/// generation is refused so a delayed disconnect never removes the
	/// connection that replaced its target.
	pub(super) fn disconnect_external(
		&mut self,
		generation: u64,
	) -> Result<ConnectionDescription, String> {
		if generation != self.connection_generation {
			return Err(format!(
				"disconnect names generation {generation}; the current connection is generation {}",
				self.connection_generation
			));
		}
		if self.adapter_state.connection.is_none() && self.connect_in_flight.is_none() {
			return Err("no external connection to disconnect".to_owned());
		}
		self.adapter_state.connection = None;
		self.adapter_state.cancellation = CancelCapability::Refuse;
		self.adapter_state.stage = if matches!(
			self.adapter_state.stage,
			AdapterStage::None | AdapterStage::Unselected
		) {
			AdapterStage::None
		} else {
			AdapterStage::Configured
		};
		self.connect_in_flight = None;
		self.next_connection_generation();
		self.status_error.clear();
		self.status_msg = "Adapter disconnected; SQL scratch remains active.".into();
		Ok(self.connection_description())
	}

	pub(super) fn connection_description(&self) -> ConnectionDescription {
		let stage = match self.adapter_state.stage {
			AdapterStage::None => "none",
			AdapterStage::Unselected => "unselected",
			AdapterStage::Unconfigured => "unconfigured",
			AdapterStage::Configured => {
				if self.connect_in_flight.is_some() {
					"connecting"
				} else {
					"configured"
				}
			}
			AdapterStage::Connected => "connected",
		};
		ConnectionDescription {
			adapter: self.adapter_state.selection.adapter_type_str().to_owned(),
			name: self.adapter_state.name.clone(),
			stage: stage.to_owned(),
			generation: self.connection_generation,
			error: (!self.status_error.is_empty()).then(|| self.status_error.clone()),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::super::Message;
	use super::super::headless::HeadlessApplication;
	use crate::adapters::common::AdapterStage;
	use crate::adapters::driver::{AdapterConfiguration, AdapterSelection};
	use crate::driver::{Command, ConnectionDescription, Outcome, Refusal};
	use std::collections::BTreeMap;
	use std::time::Duration;

	fn app() -> HeadlessApplication {
		let root = std::env::temp_dir().join(format!(
			"polariton-0166-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		std::fs::create_dir_all(&root).unwrap();
		let source = root.join("notebook.md");
		std::fs::write(&source, "# ADR 0166\n").unwrap();
		let mut app = HeadlessApplication::open_target(
			crate::gui::StartupTarget::Ephemeral {
				initial_file: Some(source.display().to_string()),
			},
			crate::configuration::StartupData::default(),
		)
		.unwrap();
		app.settle_until(Duration::from_secs(10), |s| {
			s.adapter_state.sql_connection().is_some()
		})
		.unwrap();
		app
	}

	fn sqlite_fields() -> BTreeMap<String, String> {
		BTreeMap::from([("connection_string".to_owned(), "memory".to_owned())])
	}

	#[test]
	fn the_form_and_the_command_enter_the_same_connection_operation() {
		// The form path: select, fill, submit, settle.
		let mut form = app();
		form.dispatch(Message::AdapterSelected(AdapterSelection::SQLite));
		form.dispatch(Message::AdapterConfigurationChanged(
			"connection_string".into(),
			"memory".into(),
		));
		form.dispatch(Message::AdapterConfigurationSubmitted);
		form.settle_until(Duration::from_secs(10), |s| {
			matches!(s.adapter_state.stage, AdapterStage::Connected)
		})
		.unwrap();
		let by_form = form.state().connection_description();

		// The command path, from the same starting state.
		let mut command = app();
		let receipt = command.apply_command(&Command::Connect {
			adapter: "SQLite".into(),
			name: String::new(),
			fields: sqlite_fields(),
		});
		assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
		let by_command = receipt.connection.clone().expect("connection in receipt");

		// The form path minted one generation on selection and one on submit;
		// the command path minted one. Everything else must be identical.
		assert_eq!(
			ConnectionDescription {
				generation: 0,
				..by_form.clone()
			},
			ConnectionDescription {
				generation: 0,
				..by_command.clone()
			}
		);
		assert_eq!(by_form.generation, 2);
		assert_eq!(by_command.generation, 1);
		assert_eq!(by_command.stage, "connected");
		assert!(matches!(
			command.state().adapter_state.stage,
			AdapterStage::Connected
		));
		assert_eq!(
			command.state().connection_description(),
			by_command,
			"the receipt reports the owner's state, not a copy of the request"
		);
	}

	#[test]
	fn connection_status_and_a_stale_disconnect_are_generation_gated() {
		let mut app = app();
		let connected = app
			.apply_command(&Command::Connect {
				adapter: "SQLite".into(),
				name: "scratch-twin".into(),
				fields: sqlite_fields(),
			})
			.connection
			.unwrap();
		let status = app.apply_command(&Command::ConnectionStatus {});
		assert_eq!(status.connection.as_ref(), Some(&connected));

		let stale = app.apply_command(&Command::Disconnect {
			generation: connected.generation - 1,
		});
		assert_eq!(stale.outcome, Outcome::Refused);
		assert!(matches!(
			stale.refusal,
			Some(Refusal::ConnectionRefused { .. })
		));
		assert!(
			matches!(app.state().adapter_state.stage, AdapterStage::Connected),
			"a stale disconnect never tears down the connection that stands"
		);

		let current = app.apply_command(&Command::Disconnect {
			generation: connected.generation,
		});
		assert_eq!(current.outcome, Outcome::Applied, "{current:?}");
		let after = current.connection.unwrap();
		assert_ne!(after.generation, connected.generation);
		assert_ne!(after.stage, "connected");
		assert!(app.state().adapter_state.connection.is_none());
		// Now nothing stands to disconnect; the generation gate still answers.
		let again = app.apply_command(&Command::Disconnect {
			generation: after.generation,
		});
		assert_eq!(again.outcome, Outcome::Refused);
	}

	#[test]
	fn a_connect_result_delayed_past_a_newer_choice_does_not_install() {
		let mut app = app();
		// An attempt starts but its transport result has not yet arrived.
		let task = app
			.state_mut()
			.begin_connect_with(AdapterConfiguration::SQLite {
				connection_string: "memory".into(),
			});
		drop(task);
		let first = app.state().connection_description();
		assert_eq!(first.stage, "connecting");
		// A newer human choice: the person selects another adapter, which
		// mints a generation and retires anything in flight.
		app.dispatch(Message::AdapterSelected(AdapterSelection::SQLite));
		let newer = app.state().connection_description();
		assert!(newer.generation > first.generation);
		assert!(app.state().adapter_state.connection.is_none());

		// The old attempt's result arrives late.
		let scratch = app.state().adapter_state.scratch.clone().unwrap();
		let task = app.state_mut().connect_settled(
			first.generation,
			Ok((scratch, crate::adapters::common::CancelCapability::Refuse)),
		);
		drop(task);
		assert!(
			app.state().adapter_state.connection.is_none(),
			"a superseded result must not install"
		);
		assert_eq!(
			app.state().connection_description().generation,
			newer.generation
		);
	}

	/// A refused connect changes nothing: starting connected, an incomplete
	/// Toron candidate is refused and SQL still runs against the installed
	/// connection, which status still describes.
	#[cfg(feature = "toron")]
	#[test]
	fn a_refused_connect_leaves_the_established_connection_and_its_report() {
		let mut app = app();
		let before = app
			.apply_command(&Command::Connect {
				adapter: "SQLite".into(),
				name: "established".into(),
				fields: sqlite_fields(),
			})
			.connection
			.unwrap();
		assert_eq!(before.stage, "connected");
		let receipt = app.apply_command(&Command::Connect {
			adapter: "Toron".into(),
			name: "lab".into(),
			fields: BTreeMap::from([
				("endpoint".to_owned(), "192.0.2.1:7742".to_owned()),
				("server_name".to_owned(), "toron-x.cluster.local".to_owned()),
			]),
		});
		assert_eq!(receipt.outcome, Outcome::Refused, "{receipt:?}");
		assert!(app.state().adapter_state.connection.is_some());
		assert!(matches!(
			app.state().adapter_state.selection,
			AdapterSelection::SQLite
		));
		assert_eq!(app.state().adapter_state.name, "established");
		assert_eq!(
			app.apply_command(&Command::ConnectionStatus {}).connection,
			Some(before)
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn a_partial_toron_tls_configuration_is_refused_before_any_transport() {
		let mut app = app();
		let receipt = app.apply_command(&Command::Connect {
			adapter: "Toron".into(),
			name: String::new(),
			fields: BTreeMap::from([
				("endpoint".to_owned(), "192.0.2.1:7742".to_owned()),
				("server_name".to_owned(), "toron-x.cluster.local".to_owned()),
			]),
		});
		assert_eq!(receipt.outcome, Outcome::Refused, "{receipt:?}");
		let Some(Refusal::ConnectionRefused { message }) = receipt.refusal else {
			panic!("expected a connection refusal: {receipt:?}");
		};
		assert!(
			message.contains("missing: client_cert, client_key, ca_cert"),
			"{message}"
		);
		assert!(
			app.state().adapter_state.connection.is_none()
				&& !matches!(app.state().adapter_state.stage, AdapterStage::Connected),
			"never a loopback fallback"
		);
	}
}
