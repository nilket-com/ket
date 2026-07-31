use crate::schema::{LeaseDescriptor, NodeId};
use ed25519_dalek::{Signature, Signer, SigningKey, Verifier, VerifyingKey};
use std::collections::BTreeMap;
use std::io::{Error, ErrorKind};
use std::sync::Arc;

#[derive(Clone)]
pub struct LeaseAuthority {
	local_issuer: NodeId,
	signing_key: Arc<SigningKey>,
	trusted_signers: Arc<BTreeMap<u32, VerifyingKey>>,
}

impl LeaseAuthority {
	pub fn dev_single_node(local_issuer: NodeId) -> Self {
		let secret = dev_lease_signing_secret(local_issuer.id);
		let mut trusted = BTreeMap::new();
		trusted.insert(local_issuer.id, lease_verifying_key_bytes(secret));
		Self::from_secret_bytes(local_issuer, secret, &trusted)
			.expect("dev lease signing key material must be valid")
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
					format!("invalid lease verifying key for node {node_id}: {err}"),
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

	pub fn sign_lease(&self, mut lease: LeaseDescriptor) -> LeaseDescriptor {
		lease.issuer = self.local_issuer;
		lease.signature = [0; 64];
		let signature = self.signing_key.sign(lease_signed_payload(&lease));
		lease.signature = signature.to_bytes();
		lease
	}

	pub fn verify_lease(&self, lease: &LeaseDescriptor) -> std::io::Result<()> {
		let Some(verifying_key) = self.trusted_signers.get(&lease.issuer.id) else {
			return Err(Error::new(
				ErrorKind::PermissionDenied,
				format!(
					"missing trusted lease verifying key for issuer {}",
					lease.issuer.id
				),
			));
		};
		let signature = Signature::from_bytes(&lease.signature);
		verifying_key
			.verify(lease_signed_payload(lease), &signature)
			.map_err(|err| {
				Error::new(
					ErrorKind::PermissionDenied,
					format!(
						"lease signature verification failed for issuer {}: {err}",
						lease.issuer.id
					),
				)
			})
	}
}

pub fn dev_lease_signing_secret(node_id: u32) -> [u8; 32] {
	crate::hash::sha256::digest(format!("wendelon-dev-lease-signing-key-{node_id}").as_bytes())
}

pub fn lease_verifying_key_bytes(secret: [u8; 32]) -> [u8; 32] {
	SigningKey::from_bytes(&secret).verifying_key().to_bytes()
}

pub fn lease_signed_payload(lease: &LeaseDescriptor) -> &[u8] {
	let bytes = bytemuck::bytes_of(lease);
	&bytes[..LEASE_SIGNED_LEN]
}

pub const LEASE_SIGNED_LEN: usize =
	std::mem::size_of::<LeaseDescriptor>() - std::mem::size_of::<[u8; 64]>();
