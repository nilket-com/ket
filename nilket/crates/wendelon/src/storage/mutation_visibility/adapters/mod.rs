//! Bounded logical adapters. Dormant: no admission, persistence or reader wiring.
mod error;
mod parent;
mod scalar;
mod table;
pub use error::AdapterError;
use error::{KEY_MAGIC, ROW_MAGIC};
pub use parent::*;
pub use scalar::ProofScalarType;
pub use table::ProofSchema;
#[cfg(test)]
mod tests;
