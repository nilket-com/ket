//! Dormant R2 structural codecs. Checksums, receipts and parsed terminal records grant no trust.
//! No live dispatch, root-format change or read/admission integration.
mod bundle;
mod fixed;
mod journal;
pub(crate) mod loader;
#[allow(dead_code)]
mod local_store;
mod wire;
mod witness;
use super::{
	codec::{Read2, Wire2, blob, log},
	*,
};
use crate::storage::mutation_visibility::{
	MutationIdentity, provenance::UnverifiedFrameProvenance,
};
pub use bundle::*;
pub use fixed::*;
pub use journal::*;
use wire::*;
pub use witness::*;

#[cfg(test)]
mod tests;
