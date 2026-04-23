//! Governance boundary for autonomous actions (ADR 001 / W9.d).
//!
//! The allowed autonomous surface is an explicit code artifact so a
//! new background self-healing action is a source change, not a
//! silent config flip.

use crate::network::tls::{
	ActorIdentity, ActorPermission, ActorRole, ActorTrustDomain, authorize_actor,
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
pub enum OperatorGatedAction {
	TenantDeleteConfirm,
	TenantKekRotateConfirm,
	TenantKekRetireConfirm,
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

pub fn require_operator_signal(
	actor: &ActorIdentity,
	action: OperatorGatedAction,
) -> std::io::Result<()> {
	match action {
		OperatorGatedAction::TenantDeleteConfirm
		| OperatorGatedAction::TenantKekRotateConfirm
		| OperatorGatedAction::TenantKekRetireConfirm
		| OperatorGatedAction::CrossCaPromotion => authorize_actor(actor, ActorPermission::OpsControl),
		OperatorGatedAction::BreakGlassAccess
			if actor.break_glass
				&& actor.role == ActorRole::BreakGlassAdmin
				&& actor.trust_domain == ActorTrustDomain::Ops =>
		{
			Ok(())
		}
		OperatorGatedAction::BreakGlassAccess => Err(Error::new(
			ErrorKind::PermissionDenied,
			"break-glass access requires a break-glass OPS certificate",
		)),
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::network::tls::{ActorRole, ActorTrustDomain};
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
	fn operator_signal_gate_distinguishes_ops_and_break_glass_actions() {
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
		let break_glass = ActorIdentity {
			role: ActorRole::BreakGlassAdmin,
			trust_domain: ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: None,
			scopes: vec!["cluster".to_string()],
			key_id: vec![4, 3, 2, 1],
			break_glass: true,
		};

		assert!(
			require_operator_signal(&operator, OperatorGatedAction::TenantDeleteConfirm).is_ok()
		);
		assert!(
			require_operator_signal(&operator, OperatorGatedAction::TenantKekRotateConfirm).is_ok()
		);
		assert!(
			require_operator_signal(&operator, OperatorGatedAction::TenantKekRetireConfirm).is_ok()
		);
		assert!(
			require_operator_signal(&data_node, OperatorGatedAction::TenantDeleteConfirm).is_err()
		);
		assert!(
			require_operator_signal(&data_node, OperatorGatedAction::TenantKekRotateConfirm)
				.is_err()
		);
		assert!(
			require_operator_signal(&data_node, OperatorGatedAction::TenantKekRetireConfirm)
				.is_err()
		);
		assert!(require_operator_signal(&operator, OperatorGatedAction::BreakGlassAccess).is_err());
		assert!(
			require_operator_signal(&break_glass, OperatorGatedAction::BreakGlassAccess).is_ok()
		);

		let forged_break_glass = ActorIdentity {
			role: ActorRole::Operator,
			trust_domain: ActorTrustDomain::Ops,
			cert_subject: None,
			node_id: None,
			tenant_id: None,
			scopes: vec!["cluster".to_string()],
			key_id: vec![9, 9, 9, 9],
			break_glass: true,
		};
		assert!(
			require_operator_signal(&forged_break_glass, OperatorGatedAction::BreakGlassAccess)
				.is_err()
		);
	}
}
