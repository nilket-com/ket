//! Role epoch tests use actual carrier adaptation and the sole production verification producer.
use super::governance_verification::support as wire;
use crate::storage::mutation_visibility::root2::{
	admin::manifest::*, admin_install::*, admin_statement::*, codec::*, command::*, origin::*,
	role_command, types::*, verification::*,
};
use ed25519_dalek::{Signer, SigningKey};
mod codec;
mod compatibility;
mod rotation;
mod support;
mod verification;
mod view;
use support::*;
