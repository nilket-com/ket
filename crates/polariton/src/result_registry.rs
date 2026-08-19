//! Session registry of results published by plain SQL buffers.
//!
//! `-- @result test` names exactly the tabular result displayed by that buffer
//! execution. Bindings follow latest-dispatch-wins: a revision is assigned when
//! execution starts, so an older slow query cannot overwrite a newer
//! publication merely because it finishes later. Failure, or completion
//! without a tabular final result, leaves the name unbound rather than
//! restoring stale data (`plans/wip/rune-data-products.md`).

use crate::result_artifact::{ArtifactToken, ResultArtifact, ResultArtifactError};
use std::collections::HashMap;
use std::sync::Arc;
use std::time::SystemTime;

/// Whether a published product is already resident or still requires host
/// planning. Only materialized SQL results exist until deferred relations
/// land, but provenance records the distinction from the start.
#[allow(dead_code)] // `Planned` is activated by plan steps 7–8.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ResultDataKind {
	Materialized,
	Planned,
}

/// A dispatch-time claim on a name, carried through the execution's message
/// round trip and redeemed against the registry on completion.
#[derive(Clone, Debug)]
pub struct PendingPublication {
	pub name: String,
	pub revision: u64,
	pub source_buffer: libaryon::core::BufferId,
	pub connection: String,
}

/// Where a published binding came from, shown wherever the data is consumed.
#[allow(dead_code)] // Provenance display belongs to plan step 8.
#[derive(Clone, Debug)]
pub struct ResultProvenance {
	pub source_buffer: libaryon::core::BufferId,
	pub revision: u64,
	pub artifact_token: ArtifactToken,
	pub connection: String,
	pub completed_at: SystemTime,
	pub rows: usize,
	pub cols: usize,
	pub data_kind: ResultDataKind,
}

struct PublishedResult {
	artifact: Arc<ResultArtifact>,
	provenance: ResultProvenance,
}

#[derive(Default)]
pub struct ResultRegistry {
	next_revision: u64,
	/// The revision of the latest dispatch per name; only that revision may
	/// bind or conclude the name.
	pending: HashMap<String, u64>,
	bound: HashMap<String, PublishedResult>,
}

impl ResultRegistry {
	/// Claim `name` for a starting execution: the previous binding is retired
	/// immediately so no consumer can observe stale data while the query runs.
	pub fn begin(
		&mut self,
		name: &str,
		source_buffer: libaryon::core::BufferId,
		connection: String,
	) -> PendingPublication {
		self.next_revision += 1;
		let revision = self.next_revision;
		self.bound.remove(name);
		self.pending.insert(name.to_owned(), revision);
		PendingPublication {
			name: name.to_owned(),
			revision,
			source_buffer,
			connection,
		}
	}

	/// Bind the completed frame, unless a newer dispatch has claimed the name.
	/// Returns whether the publication took effect.
	pub fn publish(&mut self, claim: &PendingPublication, artifact: Arc<ResultArtifact>) -> bool {
		if !self.concludes_latest_dispatch(claim) {
			return false;
		}
		self.pending.remove(&claim.name);
		let provenance = ResultProvenance {
			source_buffer: claim.source_buffer,
			revision: claim.revision,
			artifact_token: artifact.artifact_token(),
			connection: claim.connection.clone(),
			completed_at: SystemTime::now(),
			rows: artifact.height(),
			cols: artifact.width(),
			data_kind: ResultDataKind::Materialized,
		};
		self.bound.insert(
			claim.name.clone(),
			PublishedResult {
				artifact,
				provenance,
			},
		);
		true
	}

	/// Conclude an execution that produced no tabular result (error, DDL,
	/// batch): the name stays unbound. Returns whether this claim was still
	/// the latest dispatch.
	pub fn conclude_unbound(&mut self, claim: &PendingPublication) -> bool {
		if !self.concludes_latest_dispatch(claim) {
			return false;
		}
		self.pending.remove(&claim.name);
		true
	}

	fn concludes_latest_dispatch(&self, claim: &PendingPublication) -> bool {
		self.pending.get(&claim.name) == Some(&claim.revision)
	}

	/// The immutable workspace a Rune execution receives, cloned at dispatch.
	pub fn snapshot(&self) -> Result<HashMap<String, Arc<ResultArtifact>>, ResultArtifactError> {
		self.bound
			.iter()
			.map(|(name, published)| Ok((name.clone(), Arc::clone(&published.artifact))))
			.collect()
	}

	#[cfg(test)]
	pub fn artifact(&self, name: &str) -> Option<&Arc<ResultArtifact>> {
		self.bound.get(name).map(|published| &published.artifact)
	}

	#[allow(dead_code)] // The plot-surface reader lands with plan step 8.
	pub fn provenance(&self, name: &str) -> Option<&ResultProvenance> {
		self.bound.get(name).map(|published| &published.provenance)
	}

	/// The token of the currently published artifact, when the name is bound.
	#[allow(dead_code)] // Selection-context consumption lands in the next stage.
	pub fn artifact_token(&self, name: &str) -> Option<ArtifactToken> {
		self.provenance(name)
			.map(|provenance| provenance.artifact_token)
	}
}

/// Find the buffer's `-- @result name` annotation.
///
/// One buffer publishes one name; multiple annotations agreeing on the name
/// are tolerated, disagreeing ones are an error, and a nameless annotation is
/// an error rather than a silent no-op.
pub fn parse_result_annotation(sql: &str) -> Result<Option<String>, String> {
	let mut found: Option<String> = None;
	for line in sql.lines() {
		let Some(comment) = line.trim_start().strip_prefix("--") else {
			continue;
		};
		let Some(rest) = comment.trim_start().strip_prefix("@result") else {
			continue;
		};
		// `@results` or similar is a different word, not a nameless annotation.
		if !rest.is_empty() && !rest.starts_with(char::is_whitespace) {
			continue;
		}
		let mut tokens = rest.split_whitespace();
		let Some(name) = tokens.next() else {
			return Err("`-- @result` needs a name, e.g. `-- @result test`".to_owned());
		};
		if tokens.next().is_some() {
			return Err(format!(
				"`-- @result {name}` must be the only token on its line"
			));
		}
		let valid_start = name
			.chars()
			.next()
			.is_some_and(|c| c.is_ascii_alphabetic() || c == '_');
		let valid_rest = name
			.chars()
			.all(|c| c.is_ascii_alphanumeric() || c == '_' || c == '-');
		if !valid_start || !valid_rest {
			return Err(format!(
				"`-- @result` names use letters, digits, `_`, and `-`, starting with a letter or `_`: {name}"
			));
		}
		match &found {
			Some(previous) if previous != name => {
				return Err(format!(
					"One buffer publishes one result; found both `@result {previous}` and `@result {name}`"
				));
			}
			_ => found = Some(name.to_owned()),
		}
	}
	Ok(found)
}

#[cfg(test)]
mod tests {
	use super::*;
	use libaryon::core::BufferId;
	use polars::df;

	fn frame(value: i32) -> Arc<ResultArtifact> {
		ResultArtifact::test_frame(df!("x" => [value]).expect("frame"))
	}

	fn begin(registry: &mut ResultRegistry, name: &str) -> PendingPublication {
		registry.begin(name, BufferId::new(7), "scratch".to_owned())
	}

	#[test]
	fn publish_binds_data_and_provenance() {
		let mut registry = ResultRegistry::default();
		let claim = begin(&mut registry, "test");
		assert!(registry.publish(&claim, frame(1)));
		assert_eq!(registry.snapshot().expect("snapshot").len(), 1);
		let provenance = registry.provenance("test").expect("provenance recorded");
		assert_eq!(
			registry
				.snapshot()
				.expect("snapshot recorded")
				.get("test")
				.expect("snapshot publication")
				.artifact_token(),
			provenance.artifact_token
		);
		assert_eq!(provenance.source_buffer, BufferId::new(7));
		assert_eq!(
			provenance.artifact_token,
			registry.artifact_token("test").unwrap()
		);
		assert_eq!(provenance.rows, 1);
		assert_eq!(provenance.cols, 1);
		assert_eq!(provenance.connection, "scratch");
		assert_eq!(provenance.data_kind, ResultDataKind::Materialized);
	}

	#[test]
	fn publication_artifact_token_is_exposed_by_provenance() {
		let mut registry = ResultRegistry::default();
		let claim = begin(&mut registry, "test");
		let artifact = frame(1);
		let token = artifact.artifact_token();

		assert!(registry.publish(&claim, artifact));
		assert_eq!(registry.artifact_token("test"), Some(token));
	}

	#[test]
	fn dispatch_retires_the_previous_binding_immediately() {
		let mut registry = ResultRegistry::default();
		let claim = begin(&mut registry, "test");
		assert!(registry.publish(&claim, frame(1)));
		let _running = begin(&mut registry, "test");
		assert!(
			registry.snapshot().expect("snapshot").is_empty(),
			"stale data must not be observable while a newer query runs"
		);
	}

	#[test]
	fn an_older_dispatch_cannot_overwrite_a_newer_one() {
		let mut registry = ResultRegistry::default();
		let old = begin(&mut registry, "test");
		let new = begin(&mut registry, "test");
		assert!(registry.publish(&new, frame(2)));
		assert!(!registry.publish(&old, frame(1)));
		let snapshot = registry.snapshot().expect("snapshot");
		let bound = snapshot.get("test").expect("newer publication bound");
		assert_eq!(
			bound
				.projection()
				.unwrap()
				.column("x")
				.unwrap()
				.i32()
				.unwrap()
				.get(0),
			Some(2),
			"the newer dispatch's data must win regardless of completion order"
		);
	}

	#[test]
	fn failure_leaves_the_name_unbound() {
		let mut registry = ResultRegistry::default();
		let first = begin(&mut registry, "test");
		assert!(registry.publish(&first, frame(1)));
		let failed = begin(&mut registry, "test");
		assert!(registry.conclude_unbound(&failed));
		assert!(registry.snapshot().expect("snapshot").is_empty());
	}

	#[test]
	fn annotation_parses_a_single_name() {
		assert_eq!(
			parse_result_annotation("-- @result test\nselect 1 as x"),
			Ok(Some("test".to_owned()))
		);
		assert_eq!(parse_result_annotation("select 1 as x"), Ok(None));
	}

	#[test]
	fn annotation_ignores_lookalike_words() {
		assert_eq!(parse_result_annotation("-- @results test"), Ok(None));
	}

	#[test]
	fn annotation_rejects_missing_and_conflicting_names() {
		assert!(parse_result_annotation("-- @result").is_err());
		assert!(parse_result_annotation("-- @result test extra").is_err());
		assert!(parse_result_annotation("-- @result a\n-- @result b").is_err());
		assert!(parse_result_annotation("-- @result 9lives").is_err());
		assert_eq!(
			parse_result_annotation("-- @result test\n-- @result test"),
			Ok(Some("test".to_owned()))
		);
	}
}
