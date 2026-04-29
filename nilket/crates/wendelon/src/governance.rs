//! Governance boundary for autonomous actions (ADR 001 / W9.d).
//!
//! The allowed autonomous surface is an explicit code artifact so a
//! new background self-healing action is a source change, not a
//! silent config flip.
//!
//! Administrative authorization intentionally has two layers:
//!
//! - the dispatcher applies the coarse ring-level permission needed
//!   to reach a handler at all
//! - this module applies the action-specific overlay that the coarse
//!   permission cannot express, such as tenant-scope rejection,
//!   human-only destructive paths, and break-glass-only operations
//!
//! That split lets inter-node coordination traffic continue to use
//! the same control messages while still forcing human-initiated
//! destructive actions through an explicit governance seam.

use crate::network::tls::{
	ActorIdentity, ActorPermission, ActorRole, ActorTrustDomain, BreakGlassScope, authorize_actor,
	authorize_break_glass_scope,
};
use std::io::{Error, ErrorKind};

macro_rules! autonomous_action_surfaces {
	($($surface:ident),+ $(,)?) => {
		#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
		pub enum AutonomousActionSurface {
			$($surface),+
		}

		pub const AUTONOMOUS_ACTION_INVENTORY: &[AutonomousActionSurface] = &[
			$(AutonomousActionSurface::$surface),+
		];
	};
}

autonomous_action_surfaces! {
	ClockDriftAbort,
	ControlRingPeerEviction,
	L2TieringUpload,
	L2RetentionRewrite,
	L1MvccRetentionPrune,
	L1RehydratedSlabReclaim,
	MigrationTombstoneReclaim,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AdministrativeAction {
	TenantCreate,
	TenantUpdateQuota,
	TenantSuspend,
	TenantResume,
	TenantDelete,
	TableRetentionPolicyUpdate,
	TenantKekRotate,
	TenantKekRetire,
	RevocationList,
	RevokeActorKey,
	TransferLeadership,
	AdvanceEpoch,
	JoinCluster,
	BreakGlassAccess,
	CrossCaPromotion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
#[must_use]
pub struct AutonomousActionPermit {
	surface: AutonomousActionSurface,
}

impl AutonomousActionPermit {
	pub const fn surface(self) -> AutonomousActionSurface {
		self.surface
	}
}

pub fn autonomous_action_inventory() -> &'static [AutonomousActionSurface] {
	AUTONOMOUS_ACTION_INVENTORY
}

pub fn authorize_autonomous_action(surface: AutonomousActionSurface) -> AutonomousActionPermit {
	AutonomousActionPermit { surface }
}

fn break_glass_only_error(action: AdministrativeAction) -> std::io::Error {
	Error::new(
		ErrorKind::PermissionDenied,
		format!("{action:?} requires a break-glass OPS certificate"),
	)
}

pub fn require_administrative_action(
	actor: &ActorIdentity,
	action: AdministrativeAction,
) -> std::io::Result<()> {
	if actor.claimed_tenant_id()?.is_some() {
		return Err(Error::new(
			ErrorKind::PermissionDenied,
			format!("tenant-scoped principals may not perform {action:?}"),
		));
	}

	match action {
		AdministrativeAction::TenantCreate
		| AdministrativeAction::TenantUpdateQuota
		| AdministrativeAction::TenantSuspend
		| AdministrativeAction::TenantResume
		| AdministrativeAction::TableRetentionPolicyUpdate => {
			authorize_actor(actor, ActorPermission::TenantAdminMutation)
		}
		AdministrativeAction::TenantKekRotate | AdministrativeAction::TenantKekRetire => {
			authorize_actor(actor, ActorPermission::TenantKekAdmin)
		}
		AdministrativeAction::RevocationList | AdministrativeAction::RevokeActorKey => {
			authorize_break_glass_scope(
				actor,
				&[BreakGlassScope::ClusterOps],
				&format!("{action:?}"),
			)?;
			match actor.role {
				ActorRole::Operator | ActorRole::ClusterAdmin | ActorRole::BreakGlassAdmin => {
					Ok(())
				}
				_ => Err(Error::new(
					ErrorKind::PermissionDenied,
					format!("actor role {} is not allowed for {action:?}", actor.role),
				)),
			}
		}
		AdministrativeAction::TransferLeadership
		| AdministrativeAction::AdvanceEpoch
		| AdministrativeAction::JoinCluster
		| AdministrativeAction::CrossCaPromotion => {
			authorize_actor(actor, ActorPermission::ClusterControl)
		}
		AdministrativeAction::TenantDelete | AdministrativeAction::BreakGlassAccess
			if actor.break_glass
				&& actor.role == ActorRole::BreakGlassAdmin
				&& actor.trust_domain == ActorTrustDomain::Ops =>
		{
			authorize_break_glass_scope(actor, &[BreakGlassScope::Full], &format!("{action:?}"))?;
			Ok(())
		}
		AdministrativeAction::TenantDelete | AdministrativeAction::BreakGlassAccess => {
			Err(break_glass_only_error(action))
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::tls::{ActorRole, ActorTrustDomain, BreakGlassScope};
	use crate::schema::NodeId;

	#[test]
	fn autonomous_inventory_is_explicit_and_non_empty() {
		assert!(!autonomous_action_inventory().is_empty());
		assert!(
			autonomous_action_inventory()
				.contains(&AutonomousActionSurface::ControlRingPeerEviction)
		);
		assert!(autonomous_action_inventory().contains(&AutonomousActionSurface::ClockDriftAbort));
	}

	#[test]
	fn administrative_action_policy_distinguishes_allowed_and_restricted_actions() {
		let data_node = ActorIdentity::data_node(NodeId {
			id: 7,
			_reserved: 0,
		});
		let operator = ActorIdentity::new(
			ActorRole::Operator,
			None,
			None,
			vec!["cluster".to_string()],
			vec![1, 2, 3, 4],
		)
		.unwrap();
		let tenant_admin = ActorIdentity::new(
			ActorRole::TenantAdmin,
			None,
			None,
			vec!["cluster".to_string()],
			vec![1, 1, 1, 1],
		)
		.unwrap();
		let backup_admin = ActorIdentity::new(
			ActorRole::BackupAdmin,
			None,
			None,
			vec!["cluster".to_string()],
			vec![2, 2, 2, 2],
		)
		.unwrap();
		let cluster_admin = ActorIdentity::new(
			ActorRole::ClusterAdmin,
			None,
			None,
			vec!["cluster".to_string()],
			vec![3, 3, 3, 3],
		)
		.unwrap();
		let break_glass = ActorIdentity {
			role: ActorRole::BreakGlassAdmin,
			trust_domain: ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: None,
			scopes: vec![BreakGlassScope::Full.as_str().to_string()],
			key_id: vec![4, 3, 2, 1],
			break_glass: true,
		};
		let tenant_scoped_operator = ActorIdentity::new(
			ActorRole::Operator,
			None,
			Some("7".to_string()),
			vec!["cluster".to_string()],
			vec![5, 5, 5, 5],
		)
		.unwrap();

		assert!(
			require_administrative_action(&tenant_admin, AdministrativeAction::TenantCreate)
				.is_ok()
		);
		assert!(
			require_administrative_action(&tenant_admin, AdministrativeAction::TenantUpdateQuota)
				.is_ok()
		);
		assert!(
			require_administrative_action(
				&tenant_admin,
				AdministrativeAction::TableRetentionPolicyUpdate,
			)
			.is_ok()
		);
		assert!(
			require_administrative_action(&backup_admin, AdministrativeAction::TenantKekRotate)
				.is_ok()
		);
		assert!(
			require_administrative_action(&backup_admin, AdministrativeAction::TenantKekRetire)
				.is_ok()
		);
		assert!(
			require_administrative_action(&cluster_admin, AdministrativeAction::RevokeActorKey)
				.is_ok()
		);
		assert!(
			require_administrative_action(&cluster_admin, AdministrativeAction::TransferLeadership)
				.is_ok()
		);
		assert!(
			require_administrative_action(&cluster_admin, AdministrativeAction::CrossCaPromotion)
				.is_ok()
		);
		assert!(
			require_administrative_action(&break_glass, AdministrativeAction::TenantDelete).is_ok()
		);
		assert!(
			require_administrative_action(&data_node, AdministrativeAction::TenantCreate).is_err()
		);
		assert!(
			require_administrative_action(&data_node, AdministrativeAction::TenantKekRotate)
				.is_err()
		);
		assert!(
			require_administrative_action(&data_node, AdministrativeAction::RevokeActorKey)
				.is_err()
		);
		assert!(
			require_administrative_action(&tenant_admin, AdministrativeAction::TenantKekRotate)
				.is_err()
		);
		assert!(
			require_administrative_action(&tenant_admin, AdministrativeAction::RevokeActorKey)
				.is_err()
		);
		assert!(
			require_administrative_action(&backup_admin, AdministrativeAction::TenantDelete)
				.is_err()
		);
		assert!(
			require_administrative_action(&backup_admin, AdministrativeAction::CrossCaPromotion)
				.is_err()
		);
		assert!(
			require_administrative_action(&operator, AdministrativeAction::BreakGlassAccess)
				.is_err()
		);
		assert!(
			require_administrative_action(&break_glass, AdministrativeAction::BreakGlassAccess)
				.is_ok()
		);
		assert!(
			require_administrative_action(
				&tenant_scoped_operator,
				AdministrativeAction::TransferLeadership
			)
			.is_err()
		);

		let forged_break_glass = ActorIdentity {
			role: ActorRole::Operator,
			trust_domain: ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: None,
			scopes: vec![BreakGlassScope::Full.as_str().to_string()],
			key_id: vec![9, 9, 9, 9],
			break_glass: true,
		};
		assert!(
			require_administrative_action(
				&forged_break_glass,
				AdministrativeAction::BreakGlassAccess
			)
			.is_err()
		);
	}
}
