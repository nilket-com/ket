//! PRODUCT-0160: the live wire format. Application authority stays in update.

pub(crate) mod client;
pub(crate) mod discovery;
pub(crate) mod transport;

use crate::driver::{Command, CommandReceipt, FigureDescription};
use serde::{Deserialize, Serialize};

pub(crate) const MAX_LINE: usize = 64 * 1024;
pub(crate) const MAX_OUTSTANDING: usize = 16;
pub(crate) const MAX_ANSWERS: usize = 256;

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Request {
	pub(crate) id: u64,
	pub(crate) command: Command,
}

#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(tag = "event", rename_all = "snake_case")]
pub(crate) enum Event {
	Welcome {
		window: discovery::Advertisement,
		generation: u64,
		figures: Vec<FigureDescription>,
	},
	Applied {
		id: u64,
		terminal: bool,
		receipt: CommandReceipt,
	},
	Refused {
		id: u64,
		reason: String,
		receipt: Option<CommandReceipt>,
	},
	Completed {
		id: u64,
		receipt: CommandReceipt,
	},
	Superseded {
		id: u64,
		figure: String,
		current: Option<String>,
	},
	Failed {
		id: u64,
		reason: String,
		receipt: CommandReceipt,
	},
	Changed {
		figures: Vec<FigureDescription>,
	},
	Closed {
		reason: String,
	},
}

impl Event {
	pub(crate) fn is_final(&self) -> bool {
		matches!(
			self,
			Self::Applied { terminal: true, .. }
				| Self::Refused { .. }
				| Self::Completed { .. }
				| Self::Superseded { .. }
				| Self::Failed { .. }
		)
	}
	pub(crate) fn id(&self) -> Option<u64> {
		match self {
			Self::Applied { id, .. }
			| Self::Refused { id, .. }
			| Self::Completed { id, .. }
			| Self::Superseded { id, .. }
			| Self::Failed { id, .. } => Some(*id),
			_ => None,
		}
	}
}

pub(crate) fn encode<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
	let mut line = serde_json::to_vec(value).map_err(|error| error.to_string())?;
	line.push(b'\n');
	Ok(line)
}

pub(crate) fn decode<T: for<'de> Deserialize<'de>>(bytes: &[u8]) -> Result<T, String> {
	serde_json::from_slice(bytes).map_err(|error| error.to_string())
}
