use crate::schema::{NodeId, TransferToken};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

#[derive(Clone)]
pub struct TransferAuthority {
	local_issuer: NodeId,
	signing_key: Arc<SigningKey>,
	trusted_signers: Arc<BTreeMap<u32, VerifyingKey>>,
}

impl TransferAuthority {
	pub fn dev_single_node(local_issuer: NodeId) -> Self {
		let secret = dev_transfer_signing_secret(local_issuer.id);
		let mut trusted = BTreeMap::new();
		trusted.insert(local_issuer.id, transfer_verifying_key_bytes(secret));
		Self::from_secret_bytes(local_issuer, secret, &trusted)
			.expect("dev transfer signing key material must be valid")
	}

	pub fn from_secret_bytes(
		local_issuer: NodeId,
		secret: [u8; 32],
		trusted: &BTreeMap<u32, [u8; 32]>,
	) -> std::io::Result<Self> {
		let signing_key = SigningKey::from_bytes(&secret);
		let mut trusted_signers = BTreeMap::new();
		for (&node_id, key_bytes) in trusted {
			let verifying_key = VerifyingKey::from_bytes(key_bytes).map_err(|err| {
				Error::new(
					ErrorKind::InvalidInput,
					format!("invalid transfer verifying key for node {node_id}: {err}"),
				)
			})?;
			trusted_signers.insert(node_id, verifying_key);
		}
		Ok(Self {
			local_issuer,
			signing_key: Arc::new(signing_key),
			trusted_signers: Arc::new(trusted_signers),
		})
	}

	pub fn sign_token(&self, mut token: TransferToken) -> TransferToken {
		token.from_leader = self.local_issuer;
		token.signature = [0; 64];
		let signature = self.signing_key.sign(transfer_signed_payload(&token));
		token.signature = signature.to_bytes();
		token
	}

	pub fn verify_token(&self, token: &TransferToken) -> std::io::Result<()> {
		let Some(verifying_key) = self.trusted_signers.get(&token.from_leader.id) else {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"missing trusted transfer verifying key for issuer {}",
					token.from_leader.id
				),
			));
		};
		let signature = Signature::from_bytes(&token.signature);
		verifying_key
			.verify(transfer_signed_payload(token), &signature)
			.map_err(|err| {
				Error::new(
					ErrorKind::PermissionDenied,
					format!(
						"transfer token verification failed for issuer {}: {err}",
						token.from_leader.id
					),
				)
			})
	}
}

/// # Security
/// These deterministic keys are publicly derivable. Use only in tests and
/// local development; never provision a reachable node with them.
pub fn dev_transfer_signing_secret(node_id: u32) -> [u8; 32] {
	crate::hash::sha256::digest(format!("wendelon-dev-transfer-signing-key-{node_id}").as_bytes())
}

pub fn transfer_verifying_key_bytes(secret: [u8; 32]) -> [u8; 32] {
	SigningKey::from_bytes(&secret).verifying_key().to_bytes()
}

pub fn transfer_signed_payload(token: &TransferToken) -> &[u8] {
	let bytes = bytemuck::bytes_of(token);
	&bytes[..TRANSFER_SIGNED_LEN]
}

pub const TRANSFER_SIGNED_LEN: usize = std::mem::size_of::<TransferToken>()
	- std::mem::size_of::<[u8; 64]>()
	- std::mem::size_of::<[u8; 8]>();
