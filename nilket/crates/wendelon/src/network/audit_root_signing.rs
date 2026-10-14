//! Separate dev-only audit root signing domain. Custody and rotation are operator concerns.
use ed25519_dalek::SigningKey;

pub fn dev_audit_root_signing_secret(node_id: u32) -> [u8; 32] {
	crate::hash::sha256::digest(format!("wendelon-dev-audit-root-signing-key-{node_id}").as_bytes())
}
pub fn audit_root_verifying_key_bytes(secret: [u8; 32]) -> [u8; 32] {
	SigningKey::from_bytes(&secret).verifying_key().to_bytes()
}
