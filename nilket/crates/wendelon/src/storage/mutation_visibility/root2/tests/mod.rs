use super::*;
use super::{codec::*, validation::*};
mod acquisition;
mod admin_command;
mod admin_install;
mod admin_statement;
mod admission_command;
mod admission_gate;
mod admission_registry;
mod admission_reserve;
pub(crate) mod committed;
mod control_class;
mod cover;
mod delivery_shapes;
mod entry_identity;
mod g1c_roundtrip;
mod goldens;
mod governance_command;
mod invariants;
mod issuer_registry;
mod lease_payload;
mod mark_store;
mod origin;
mod owner_tuple_delivery;
mod prefix;
mod profile_three_reserve;
mod profile_two_binding;
mod profile_two_chain;
mod profile_two_refusal;
mod profile_two_reserve;
mod recovery;
mod refusals;
mod removed_forms;
mod restoration;
mod review_regressions;
mod root_profile;
mod statement_seeds;
mod transitions;
mod verification;

mod support;
use support::*;

mod r0_assay;

mod s1c;
mod s1c_oracle;

mod s1c_review;

pub(crate) mod governance_verification;

mod role_epochs;

mod role_freeze;

mod retirement;

mod compromise;
