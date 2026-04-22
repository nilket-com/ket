//! Focused correctness checkers for simulator-produced histories.
//!
//! These stay intentionally small: the monotonic counter checker is the
//! always-on breadth pass, and the register checker gives a bounded
//! in-Rust linearizability verdict for short single-register histories.
//! External Elle analysis consumes the exported history artifacts in
//! [`crate::simulator::history`].

use std::collections::HashMap;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CounterHistoryEntry {
	pub process: u64,
	pub key: String,
	pub started_at: u64,
	pub completed_at: u64,
	pub op: CounterOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum CounterOperation {
	Add { by: u64 },
	Read { observed: u64 },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RegisterHistoryEntry {
	pub process: u64,
	pub key: String,
	pub started_at: u64,
	pub completed_at: u64,
	pub op: RegisterOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum RegisterOperation {
	Write { value: i64 },
	Read { observed: Option<i64> },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct CheckerFailure {
	pub key: String,
	pub message: String,
}

pub struct MonotonicCounterChecker;

impl MonotonicCounterChecker {
	/// Validate a counter history under half-open intervals:
	/// operations definitely visible to a read satisfy
	/// `completed_at <= read.started_at`, and operations definitely
	/// not yet visible satisfy `started_at >= read.completed_at`.
	pub fn check(history: &[CounterHistoryEntry]) -> Result<(), CheckerFailure> {
		let mut reads_by_process: HashMap<(&str, u64), Vec<&CounterHistoryEntry>> = HashMap::new();
		let mut writes_by_key: HashMap<&str, Vec<&CounterHistoryEntry>> = HashMap::new();

		for entry in history {
			if entry.completed_at < entry.started_at {
				return Err(CheckerFailure {
					key: entry.key.clone(),
					message: format!(
						"operation completed before it started (start={}, complete={})",
						entry.started_at, entry.completed_at
					),
				});
			}
			match entry.op {
				CounterOperation::Add { .. } => {
					writes_by_key
						.entry(entry.key.as_str())
						.or_default()
						.push(entry);
				}
				CounterOperation::Read { .. } => {
					reads_by_process
						.entry((entry.key.as_str(), entry.process))
						.or_default()
						.push(entry);
				}
			}
		}

		for entry in history {
			let CounterOperation::Read { observed } = entry.op else {
				continue;
			};
			let writes = writes_by_key
				.get(entry.key.as_str())
				.cloned()
				.unwrap_or_default();
			let lower = writes
				.iter()
				.filter_map(|candidate| match candidate.op {
					CounterOperation::Add { by } if candidate.completed_at <= entry.started_at => {
						Some(by)
					}
					_ => None,
				})
				.sum::<u64>();
			let upper = writes
				.iter()
				.filter_map(|candidate| match candidate.op {
					CounterOperation::Add { by } if candidate.started_at < entry.completed_at => {
						Some(by)
					}
					_ => None,
				})
				.sum::<u64>();
			if observed < lower {
				return Err(CheckerFailure {
					key: entry.key.clone(),
					message: format!(
						"read observed {observed}, below the committed lower bound {lower}"
					),
				});
			}
			if observed > upper {
				return Err(CheckerFailure {
					key: entry.key.clone(),
					message: format!(
						"read observed {observed}, above the possible upper bound {upper}"
					),
				});
			}
		}

		for ((key, _process), reads) in &mut reads_by_process {
			reads.sort_by_key(|entry| (entry.started_at, entry.completed_at));
			let mut previous: Option<&CounterHistoryEntry> = None;
			for read in reads.iter().copied() {
				if let Some(earlier) = previous {
					if earlier.completed_at <= read.started_at {
						let CounterOperation::Read {
							observed: earlier_observed,
						} = earlier.op
						else {
							unreachable!("read history contains non-read entry");
						};
						let CounterOperation::Read { observed } = read.op else {
							unreachable!("read history contains non-read entry");
						};
						if observed < earlier_observed {
							return Err(CheckerFailure {
								key: (*key).to_string(),
								message: format!(
									"process {} observed counter regression from {earlier_observed} to {observed}",
									_process
								),
							});
						}
					}
				}
				previous = Some(read);
			}
		}

		Ok(())
	}
}

pub struct RegisterLinearizabilityChecker;

impl RegisterLinearizabilityChecker {
	/// Check each key independently against a single-register model.
	/// This is intentionally scoped to short histories; the simulator's
	/// broader coverage comes from pairing this with the cheaper
	/// monotonic-counter pass. Callers should batch longer traces into
	/// small prefixes before invoking this checker; keeping a single call
	/// at roughly <= 16 operations per key avoids stepping on the
	/// backtracking worst case.
	pub fn check(history: &[RegisterHistoryEntry]) -> Result<(), CheckerFailure> {
		let mut per_key: HashMap<&str, Vec<RegisterHistoryEntry>> = HashMap::new();
		for entry in history {
			if entry.completed_at < entry.started_at {
				return Err(CheckerFailure {
					key: entry.key.clone(),
					message: format!(
						"operation completed before it started (start={}, complete={})",
						entry.started_at, entry.completed_at
					),
				});
			}
			per_key
				.entry(entry.key.as_str())
				.or_default()
				.push(entry.clone());
		}

		for (key, ops) in per_key {
			let mut used = vec![false; ops.len()];
			if !linearizable_register_prefix(&ops, &mut used, None) {
				return Err(CheckerFailure {
					key: key.to_string(),
					message: "history is not linearizable for a single-register model".to_string(),
				});
			}
		}

		Ok(())
	}
}

fn linearizable_register_prefix(
	ops: &[RegisterHistoryEntry],
	used: &mut [bool],
	state: Option<i64>,
) -> bool {
	if used.iter().all(|entry| *entry) {
		return true;
	}

	for index in 0..ops.len() {
		if used[index] || has_unmet_predecessor(index, ops, used) {
			continue;
		}
		let Some(next_state) = apply_register_operation(state, &ops[index].op) else {
			continue;
		};
		used[index] = true;
		if linearizable_register_prefix(ops, used, next_state) {
			return true;
		}
		used[index] = false;
	}

	false
}

fn has_unmet_predecessor(index: usize, ops: &[RegisterHistoryEntry], used: &[bool]) -> bool {
	let candidate = &ops[index];
	ops.iter().enumerate().any(|(other_index, other)| {
		!used[other_index] && other.completed_at <= candidate.started_at && other_index != index
	})
}

fn apply_register_operation(state: Option<i64>, op: &RegisterOperation) -> Option<Option<i64>> {
	match op {
		RegisterOperation::Write { value } => Some(Some(*value)),
		RegisterOperation::Read { observed } if *observed == state => Some(state),
		RegisterOperation::Read { .. } => None,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn monotonic_counter_checker_accepts_valid_overlaps() {
		let history = vec![
			CounterHistoryEntry {
				process: 1,
				key: "tenant/a".to_string(),
				started_at: 1,
				completed_at: 5,
				op: CounterOperation::Add { by: 1 },
			},
			CounterHistoryEntry {
				process: 7,
				key: "tenant/a".to_string(),
				started_at: 2,
				completed_at: 3,
				op: CounterOperation::Read { observed: 0 },
			},
			CounterHistoryEntry {
				process: 7,
				key: "tenant/a".to_string(),
				started_at: 6,
				completed_at: 7,
				op: CounterOperation::Read { observed: 1 },
			},
		];
		MonotonicCounterChecker::check(&history).unwrap();
	}

	#[test]
	fn monotonic_counter_checker_rejects_lost_write() {
		let history = vec![
			CounterHistoryEntry {
				process: 1,
				key: "tenant/a".to_string(),
				started_at: 1,
				completed_at: 2,
				op: CounterOperation::Add { by: 1 },
			},
			CounterHistoryEntry {
				process: 2,
				key: "tenant/a".to_string(),
				started_at: 3,
				completed_at: 4,
				op: CounterOperation::Read { observed: 0 },
			},
		];
		let err = MonotonicCounterChecker::check(&history).unwrap_err();
		assert!(err.message.contains("lower bound"));
	}

	#[test]
	fn register_checker_accepts_linearizable_history() {
		let history = vec![
			RegisterHistoryEntry {
				process: 1,
				key: "tenant/a".to_string(),
				started_at: 1,
				completed_at: 2,
				op: RegisterOperation::Write { value: 1 },
			},
			RegisterHistoryEntry {
				process: 2,
				key: "tenant/a".to_string(),
				started_at: 3,
				completed_at: 4,
				op: RegisterOperation::Read { observed: Some(1) },
			},
			RegisterHistoryEntry {
				process: 3,
				key: "tenant/a".to_string(),
				started_at: 5,
				completed_at: 6,
				op: RegisterOperation::Write { value: 2 },
			},
			RegisterHistoryEntry {
				process: 2,
				key: "tenant/a".to_string(),
				started_at: 7,
				completed_at: 8,
				op: RegisterOperation::Read { observed: Some(2) },
			},
		];
		RegisterLinearizabilityChecker::check(&history).unwrap();
	}

	#[test]
	fn register_checker_rejects_stale_read_after_committed_write() {
		let history = vec![
			RegisterHistoryEntry {
				process: 1,
				key: "tenant/a".to_string(),
				started_at: 1,
				completed_at: 2,
				op: RegisterOperation::Write { value: 1 },
			},
			RegisterHistoryEntry {
				process: 1,
				key: "tenant/a".to_string(),
				started_at: 3,
				completed_at: 4,
				op: RegisterOperation::Write { value: 2 },
			},
			RegisterHistoryEntry {
				process: 2,
				key: "tenant/a".to_string(),
				started_at: 5,
				completed_at: 6,
				op: RegisterOperation::Read { observed: Some(1) },
			},
		];
		let err = RegisterLinearizabilityChecker::check(&history).unwrap_err();
		assert!(err.message.contains("not linearizable"));
	}
}
