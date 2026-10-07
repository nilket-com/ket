//! Dormant R2 structural codecs. Checksums, receipts and parsed terminal records grant no trust.
//! No live IO, root-format change or read/admission integration.
mod bundle;
mod fixed;
mod journal;
pub(crate) mod loader;
mod wire;
mod witness;
use super::super::{MutationIdentity, provenance::UnverifiedFrameProvenance};
use super::{
	codec::{Read2, Wire2, blob, log},
	*,
};
pub use bundle::*;
pub use fixed::*;
pub use journal::*;
use wire::*;
pub use witness::*;

#[cfg(test)]
mod tests;
