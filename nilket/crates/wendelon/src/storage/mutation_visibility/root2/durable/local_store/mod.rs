//! Dormant profile-1 physical persistence. No caller in live dispatch and no authority token.
//! Retained parents stop at 32768 (~4.55 days at 300/hour); reclamation and L1 stay open.
use super::*;
use crate::storage::file_ops::{AsyncFileOps, FileHandle};
use std::{
	collections::{BTreeMap, BTreeSet},
	path::{Path, PathBuf},
	sync::Arc,
};
use tokio::sync::{Mutex, OwnedSemaphorePermit, Semaphore};
mod adapter;
mod capacity;
mod format;
mod inspection;
mod prospective;
mod recovery;
mod repair;
mod scan;
mod store;
mod types;
use adapter::*;
use capacity::*;
use format::*;
use types::*;

#[cfg(test)]
mod tests;
