//! Published Ed25519 seeds for the profile-3 S1 and KAT fixtures. TEST-ONLY: these keys are public
//! and must never authorize anything outside tests. The administrator seed signs VISADS02 over the
//! `M3_S1`, `M3_KAT` and `M3_KAT_FOREIGN` manifests only; statements over `M3_PRIMARY` keep the
//! seed `[11; 32]`. The governance and recovery seeds give those manifests their other two roles.
use super::*;
use crate::storage::mutation_visibility::root2::admin::key::check_enrollable_admin_key2;
use ed25519_dalek::SigningKey;

pub(super) const ADMIN_SEED: [u8; 32] = [0xe1; 32];
pub(super) const GOVERNANCE_SEED: [u8; 32] = [0xe2; 32];
pub(super) const RECOVERY_SEED: [u8; 32] = [0xe3; 32];

fn public(seed: &[u8; 32]) -> Digest2 {
	SigningKey::from_bytes(seed).verifying_key().to_bytes()
}

#[test]
fn published_seeds_give_distinct_enrollable_keys() {
	let keys = [ADMIN_SEED, GOVERNANCE_SEED, RECOVERY_SEED].map(|s| public(&s));
	let hex: Vec<String> = keys
		.iter()
		.map(|k| k.iter().map(|x| format!("{x:02x}")).collect())
		.collect();
	assert_eq!(
		hex,
		[
			"25fc32c478a5a4f84e4557a0342e08278c883641b6149bdb0b4a986ab6c2cb78",
			"c91cb3ce2b84e4ba85f562ece41edfe4e27afc52d88d507f66a18638df823e9f",
			"4dd9b6496a27571acb089a5e3482dfc86acdeddc4d1f28e15a9b04f026d7f226",
		]
	);
	// Distinct from each other and from the `M3_PRIMARY` keys (seeds 11, 12 and 13).
	let primary = [11u8, 12, 13].map(|s| public(&[s; 32]));
	for (i, k) in keys.iter().enumerate() {
		check_enrollable_admin_key2(k).unwrap();
		assert!(!primary.contains(k));
		assert!(keys[i + 1..].iter().all(|other| other != k));
	}
}
