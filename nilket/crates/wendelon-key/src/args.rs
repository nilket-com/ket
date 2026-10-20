//! The argument parser: one subcommand, then `--flag value` pairs and bare switches, each from the
//! subcommand's own list. Anything else is refused, so there is no passphrase flag to find.
use crate::error::ToolError;
use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;

/// What one subcommand accepts. Value flags may repeat only when listed in `twice` (at most two).
pub(crate) struct Spec {
	pub(crate) values: &'static [&'static str],
	pub(crate) twice: &'static [&'static str],
	pub(crate) switches: &'static [&'static str],
}

#[derive(Debug)]
pub(crate) struct Args {
	values: BTreeMap<&'static str, Vec<String>>,
	switches: BTreeSet<&'static str>,
}

pub(crate) fn parse(rest: &[String], spec: &Spec) -> Result<Args, ToolError> {
	let mut values: BTreeMap<&'static str, Vec<String>> = BTreeMap::new();
	let mut switches = BTreeSet::new();
	let mut i = 0;
	while i < rest.len() {
		let flag = rest[i].as_str();
		if let Some(s) = spec.switches.iter().find(|s| **s == flag) {
			if !switches.insert(*s) {
				return Err(ToolError::Usage(format!("{flag} is given twice")));
			}
			i += 1;
			continue;
		}
		let Some(v) = spec.values.iter().find(|v| **v == flag) else {
			return Err(ToolError::Usage(format!("unknown argument {flag:?}")));
		};
		let Some(value) = rest.get(i + 1) else {
			return Err(ToolError::Usage(format!("{flag} needs a value")));
		};
		let entry = values.entry(*v).or_default();
		let limit = if spec.twice.contains(v) { 2 } else { 1 };
		if entry.len() == limit {
			return Err(ToolError::Usage(format!("{flag} is given too many times")));
		}
		entry.push(value.clone());
		i += 2;
	}
	Ok(Args { values, switches })
}

impl Args {
	pub(crate) fn opt(&self, flag: &str) -> Option<&str> {
		self.values
			.get(flag)
			.and_then(|v| v.first())
			.map(String::as_str)
	}
	pub(crate) fn value(&self, flag: &str) -> Result<&str, ToolError> {
		self.opt(flag)
			.ok_or_else(|| ToolError::Usage(format!("{flag} is required")))
	}
	pub(crate) fn path(&self, flag: &str) -> Result<&Path, ToolError> {
		self.value(flag).map(Path::new)
	}
	pub(crate) fn paths(&self, flag: &str) -> Vec<&Path> {
		self.values
			.get(flag)
			.map(|v| v.iter().map(Path::new).collect())
			.unwrap_or_default()
	}
	pub(crate) fn number(&self, flag: &str) -> Result<u64, ToolError> {
		self.value(flag)?
			.parse()
			.map_err(|_| ToolError::Usage(format!("{flag} needs a non-negative integer")))
	}
	pub(crate) fn switch(&self, flag: &str) -> bool {
		self.switches.contains(flag)
	}
}
