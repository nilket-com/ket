//! The C-S6 audit output, pinned byte for byte over a history that exercises every role-proof
//! form (a statement, Freeze, a recovery rotation of the administrator, a joint Unfreeze, a
//! governance rotation and RetireRoot). Recorded before the containment form existed, so a form
//! added to the role-proof domain table cannot change what the audit reports (activation
//! Addendum A5). The C-S6b export is pinned by `export_chain::t_fmt_golden_layout_and_refusals`.
use super::support::*;
use crate::storage::mutation_visibility::root2::codec::hash;
use crate::storage::mutation_visibility::root2::command::Command2;

const GOLDEN_AUDIT_SHA256: &str =
	"61645b57360e0695d7c48cc7ab4f81062a8f2eb4e9035a2e3df0e91988b29f07";

/// The audit stdout digest of the pinned history, retired at the end or not.
fn audit_digest(retire: bool) -> (String, String) {
	let mut r = Root::new();
	r.enrol_authority();
	r.statement_use(11);
	r.freeze();
	r.rotate(ADMIN, 40);
	let c = Command2::Unfreeze(super::wire::log(5));
	let p = r.proof(&c, [r.current(GOVERNANCE), r.current(RECOVERY)]);
	r.apply(c, Some(p));
	r.rotate(GOVERNANCE, 41);
	if retire {
		r.retire();
	}
	let dirs = r.write(None);
	let out = audit(
		&dirs,
		&suspects(&[(ADMIN, 0), (GOVERNANCE, 0), (RECOVERY, 0)]),
	);
	assert_eq!(out.code, 0, "{}", out.stderr);
	let stdout = out
		.stdout
		.replace(&dirs.data.display().to_string(), "<data>");
	let digest = hash(b"", stdout.as_bytes())
		.iter()
		.map(|b| format!("{b:02x}"))
		.collect();
	(digest, stdout)
}

#[test]
fn the_audit_output_of_every_role_proof_form_is_unchanged() {
	let (digest, stdout) = audit_digest(true);
	assert_eq!(digest, GOLDEN_AUDIT_SHA256, "{stdout}");
	// Control: one event fewer (no RetireRoot) changes the digest.
	assert_ne!(audit_digest(false).0, GOLDEN_AUDIT_SHA256);
}
