//! Context exports (C-S6b) as the tool reads them: plain files the operator carried over, read
//! with a bound, then re-verified by the facade's reader against the operator's trusted manifest.
//! Nothing here judges a context; the facade does.
use crate::error::ToolError;
use std::io::Read;
use std::path::Path;
use wendelon::storage::mutation_visibility::root2::operator_proof::{
	VerifiedContext, verify_context_export,
};

/// An export holds at most 192 successors of bounded size; anything larger is refused unread.
const MAX_EXPORT_BYTES: u64 = 16 << 20;
const MANIFEST_BYTES: u64 = 241;

pub(crate) fn read_bounded(path: &Path, max: u64) -> Result<Vec<u8>, ToolError> {
	let what = path.display().to_string();
	let file = std::fs::File::open(path).map_err(ToolError::read(what.clone()))?;
	let mut bytes = Vec::new();
	file.take(max + 1)
		.read_to_end(&mut bytes)
		.map_err(ToolError::read(what.clone()))?;
	if bytes.len() as u64 > max {
		return Err(ToolError::TooLarge { what, max });
	}
	Ok(bytes)
}

/// Every `--context` export, re-verified against the `--trusted-manifest`.
pub(crate) fn load(contexts: &[&Path], manifest: &Path) -> Result<Vec<VerifiedContext>, ToolError> {
	let trusted = read_bounded(manifest, MANIFEST_BYTES)?;
	contexts
		.iter()
		.map(|path| {
			let bytes = read_bounded(path, MAX_EXPORT_BYTES)?;
			verify_context_export(&bytes, &trusted).map_err(ToolError::Context)
		})
		.collect()
}

/// Two exports are two sources only from different asserted nodes with byte-identical contexts.
/// This is procedural evidence, not authentication: the runbook carries them on separate
/// channels.
pub(crate) fn second_source(
	first: &VerifiedContext,
	second: &VerifiedContext,
) -> Result<(), ToolError> {
	if first.context() != second.context() {
		return Err(ToolError::refused(
			"the two context exports carry different contexts",
		));
	}
	if first.node_id().same_node_as(second.node_id()) {
		return Err(ToolError::refused(
			"the two context exports come from the same node; a second source needs another node",
		));
	}
	Ok(())
}
