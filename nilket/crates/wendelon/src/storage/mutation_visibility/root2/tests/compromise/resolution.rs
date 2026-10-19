//! A4 (ambiguity half): suspect-key resolution over a key history. A valid root's history never
//! holds a key twice (every key is unique across roles and epochs), so the ambiguous case is
//! exercised here on a synthetic history; the absent-key case also runs end to end.
use super::resolve_key2;
use super::trace::History2;
use crate::storage::mutation_visibility::root2::types::Log2;

fn at(index: u64) -> Log2 {
	Log2 {
		term: 1,
		node: 1,
		index,
	}
}

#[test]
fn a4_key_resolves_to_exactly_one_identity_or_is_unresolved() {
	let history = History2::from_epochs([
		vec![([1; 32], at(2)), ([4; 32], at(5))],
		vec![([2; 32], at(2))],
		vec![([3; 32], at(2))],
	]);
	// Control: each held key resolves to its own role and epoch.
	assert_eq!(resolve_key2(&history, &[4; 32]), Ok((1, 1)));
	assert_eq!(resolve_key2(&history, &[3; 32]), Ok((3, 0)));
	assert!(resolve_key2(&history, &[9; 32]).is_err(), "absent key");
	let duplicated = History2::from_epochs([
		vec![([1; 32], at(2)), ([2; 32], at(5))],
		vec![([2; 32], at(2))],
		vec![([3; 32], at(2))],
	]);
	assert_eq!(
		resolve_key2(&duplicated, &[2; 32]),
		Err("--key resolves to more than one identity")
	);
	assert_eq!(resolve_key2(&duplicated, &[1; 32]), Ok((1, 0)));
}
