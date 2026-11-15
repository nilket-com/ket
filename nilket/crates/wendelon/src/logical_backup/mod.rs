//! ADR 0736: a quiescent logical backup of rows, schemas and tenant settings, and its restore
//! into a fresh cluster.
//!
//! This module holds the pieces that need no cluster: the sealed artifact ([`artifact`]) and
//! the bounded, framed per-table digest ([`digest`]), and the roster a
//! settled cut gives under the backup hold ([`roster`]). The export and restore drivers use them.

pub mod artifact;
pub mod digest;
pub mod export;
pub mod format;
pub mod hold;
pub mod restore;
pub mod roster;
