//! `merge`: two hosts' partial `VISROL03` proofs of one joint command become one. No key file is
//! opened. The facade refuses any difference outside the two signature areas, a slot filled twice
//! or not at all, a layout other than the one rebuilt from (command, context), and a slot that
//! does not verify under its claimed (role, epoch); it self-checks the result before returning it.
use super::describe::{self, MAX_COMMAND, MAX_PROOF};
use super::sign::write;
use crate::args::Args;
use crate::context::{self, read_bounded};
use crate::envelope::hex;
use crate::error::ToolError;
use crate::io::Io;
use crate::session::{self, key_name, must_be_free};
use wendelon::storage::mutation_visibility::root2::operator_proof as facade;

pub(super) fn run(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let out = format!("{}.sidecar", key_name(a.value("--out")?)?);
	let partials = a.paths("--partial");
	if partials.len() != 2 {
		return Err(ToolError::Usage(
			"merge takes exactly two --partial files".into(),
		));
	}
	session::run(io, a.path("--dir")?, "merge", |io, s| {
		let ctx = context::load(&[a.path("--context")?], a.path("--trusted-manifest")?)?.remove(0);
		let command_path = a.path("--command")?;
		let command = read_bounded(command_path, MAX_COMMAND)?;
		let d = describe::decode(&command_path.display().to_string(), &command, &ctx)?;
		s.line.command_digest = Some(hex(&d.digest));
		let first = read_bounded(partials[0], MAX_PROOF)?;
		let second = read_bounded(partials[1], MAX_PROOF)?;
		must_be_free(s.dir, &out)?;
		let merged = facade::merge(&ctx, &command, &first, &second).map_err(ToolError::Proof)?;
		s.line.form_slot = Some(describe::form_slot(&merged, 0).replace(" slot ?", " merged"));
		write(io, s, &[(&out, &merged)])
	})
}
