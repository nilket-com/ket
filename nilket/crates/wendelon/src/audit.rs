use crate::consensus::monitor::{ClockDriftEvent, ClockDriftSubscriber};
use crate::network::tls::{ActorIdentity, ActorRole, ActorTrustDomain};
use crate::schema::NodeId;
use std::fs::OpenOptions;
use std::io::Write;
use std::path::Path;
use std::sync::{Arc, Mutex};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AuditAuthority {
	NodeLocal,
	RaftLog,
}

impl AuditAuthority {
	fn as_str(self) -> &'static str {
		match self {
			Self::NodeLocal => "node_local",
			Self::RaftLog => "raft_log",
		}
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AuditEvent {
	pub authority: AuditAuthority,
	pub event_type: String,
	pub node_id: NodeId,
	pub actor_role: Option<ActorRole>,
	pub actor_trust_domain: Option<ActorTrustDomain>,
	pub actor_key_id: Option<Vec<u8>>,
	pub break_glass: bool,
	pub message: String,
}

impl AuditEvent {
	pub fn new(
		authority: AuditAuthority,
		event_type: impl Into<String>,
		node_id: NodeId,
		message: impl Into<String>,
	) -> Self {
		Self {
			authority,
			event_type: event_type.into(),
			node_id,
			actor_role: None,
			actor_trust_domain: None,
			actor_key_id: None,
			break_glass: false,
			message: message.into(),
		}
	}

	pub fn with_actor(mut self, actor: &ActorIdentity) -> Self {
		self.actor_role = Some(actor.role);
		self.actor_trust_domain = Some(actor.trust_domain);
		self.actor_key_id = Some(actor.key_id.clone());
		self.break_glass = actor.break_glass;
		self
	}

	pub fn to_json_line(&self) -> String {
		let actor_role = self
			.actor_role
			.map(|role| format!("\"{}\"", role.as_str()))
			.unwrap_or_else(|| "null".to_string());
		let actor_trust_domain = self
			.actor_trust_domain
			.map(|domain| format!("\"{domain}\""))
			.unwrap_or_else(|| "null".to_string());
		let actor_key_id = self
			.actor_key_id
			.as_ref()
			.map(|key_id| format!("\"{}\"", hex_string(key_id)))
			.unwrap_or_else(|| "null".to_string());
		format!(
			"{{\"authority\":\"{}\",\"event_type\":\"{}\",\"node_id\":\"n-{:02}\",\"actor_role\":{},\"actor_trust_domain\":{},\"actor_key_id\":{},\"break_glass\":{},\"message\":\"{}\"}}\n",
			self.authority.as_str(),
			escape_json(&self.event_type),
			self.node_id.id,
			actor_role,
			actor_trust_domain,
			actor_key_id,
			if self.break_glass { "true" } else { "false" },
			escape_json(&self.message),
		)
	}
}

pub trait AuditSink: Send + Sync {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()>;
}

pub struct NoopAuditSink;

impl AuditSink for NoopAuditSink {
	fn emit(&self, _event: AuditEvent) -> std::io::Result<()> {
		Ok(())
	}
}

pub struct LocalAuditSink {
	file: Mutex<std::fs::File>,
}

impl LocalAuditSink {
	pub fn open(path: impl AsRef<Path>) -> std::io::Result<Self> {
		let file = OpenOptions::new().create(true).append(true).open(path)?;
		Ok(Self {
			file: Mutex::new(file),
		})
	}
}

impl AuditSink for LocalAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		let line = event.to_json_line();
		let mut guard = self.file.lock().expect("audit file mutex poisoned");
		guard.write_all(line.as_bytes())?;
		guard.flush()
	}
}

pub struct AuditClockDriftSubscriber {
	sink: Arc<dyn AuditSink>,
}

impl AuditClockDriftSubscriber {
	pub fn new(sink: Arc<dyn AuditSink>) -> Self {
		Self { sink }
	}
}

impl ClockDriftSubscriber for AuditClockDriftSubscriber {
	fn on_drift_exceeded(&self, event: &ClockDriftEvent) {
		let audit = AuditEvent::new(
			AuditAuthority::NodeLocal,
			"clock_drift_exceeded",
			event.local_node,
			format!(
				"local_physical_ms={} cluster_median_ms={} offset_ms={} max_offset_ms={}",
				event.local_physical_ms,
				event.cluster_median_ms,
				event.offset_ms,
				event.max_offset_ms,
			),
		);
		if let Err(err) = self.sink.emit(audit) {
			eprintln!("wendelon: audit emission failed for clock drift event: {err}");
		}
	}
}

fn escape_json(raw: &str) -> String {
	let mut out = String::with_capacity(raw.len());
	for ch in raw.chars() {
		match ch {
			'"' => out.push_str("\\\""),
			'\\' => out.push_str("\\\\"),
			'\n' => out.push_str("\\n"),
			'\r' => out.push_str("\\r"),
			'\t' => out.push_str("\\t"),
			ch if ch.is_control() => out.push_str(&format!("\\u{:04x}", ch as u32)),
			ch => out.push(ch),
		}
	}
	out
}

fn hex_string(bytes: &[u8]) -> String {
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02x}").expect("writing to string cannot fail");
	}
	out
}

#[cfg(test)]
pub struct InMemoryAuditSink {
	events: Mutex<Vec<AuditEvent>>,
}

#[cfg(test)]
impl InMemoryAuditSink {
	pub fn new() -> Self {
		Self {
			events: Mutex::new(Vec::new()),
		}
	}

	pub fn events(&self) -> Vec<AuditEvent> {
		self.events.lock().unwrap().clone()
	}
}

#[cfg(test)]
impl Default for InMemoryAuditSink {
	fn default() -> Self {
		Self::new()
	}
}

#[cfg(test)]
impl AuditSink for InMemoryAuditSink {
	fn emit(&self, event: AuditEvent) -> std::io::Result<()> {
		self.events.lock().unwrap().push(event);
		Ok(())
	}
}
