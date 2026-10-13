//! Frozen current VISAUT04 encoder, compiled only by the state cfg(test) child.
use super::*;
pub(super) fn encode_state_reference(s: &State2) -> Result2<Vec<u8>> {
	validate_state(s)?;
	let u = usage(s)?;
	let mut b = Vec::new();
	b.extend(s.generation);
	b.extend(s.cluster);
	log(&mut b, s.bootstrap)?;
	log(&mut b, s.covered)?;
	b.extend(s.fingerprint);
	b.extend(s.command_digest);
	b.extend(s.initial_epoch.to_le_bytes());
	b.extend(s.control_digest);
	b.extend(s.control_len.to_le_bytes());
	b.extend((u.parents as u32).to_le_bytes());
	b.extend((u.held as u64).to_le_bytes());
	b.extend((u.history as u64).to_le_bytes());
	b.extend((u.registry as u64).to_le_bytes());
	write_vec(&mut b, s.selectors.values().cloned(), s.selectors.len())?;
	write_vec(&mut b, s.authorities.values().cloned(), s.authorities.len())?;
	write_vec(&mut b, s.issuers.values().cloned(), s.issuers.len())?;
	b.extend(
		crate::storage::mutation_visibility::root2::admission_registry::encode_permission_section2(
			&s.permissions,
			permission_limits2(),
		)
		.map_err(section_error)?,
	);
	write_vec(&mut b, s.lineages.values().cloned(), s.lineages.len())?;
	write_vec(&mut b, s.cuts.iter().cloned(), s.cuts.len())?;
	write_vec(&mut b, s.parents.values().cloned(), s.parents.len())?;
	envelope_versioned(b"VISAUT04", 4, &b, ROOT_BYTES2)
}
