//! Governance boundary for autonomous actions (ADR 001 / W9.d).
//!
//! The allowed autonomous surface is an explicit code artifact so a
//! new background self-healing action is a source change, not a
//! silent config flip.

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum AutonomousActionSurface {
	ClockDriftAbort,
	ControlRingPeerEviction,
	L2TieringUpload,
	L2RetentionRewrite,
	L1MvccRetentionPrune,
	L1RehydratedSlabReclaim,
	MigrationTombstoneReclaim,
}

pub const AUTONOMOUS_ACTION_INVENTORY: &[AutonomousActionSurface] = &[
	AutonomousActionSurface::ClockDriftAbort,
	AutonomousActionSurface::ControlRingPeerEviction,
	AutonomousActionSurface::L2TieringUpload,
	AutonomousActionSurface::L2RetentionRewrite,
	AutonomousActionSurface::L1MvccRetentionPrune,
	AutonomousActionSurface::L1RehydratedSlabReclaim,
	AutonomousActionSurface::MigrationTombstoneReclaim,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum OperatorGatedAction {
	TenantDeleteConfirm,
	BreakGlassAccess,
	CrossCaPromotion,
}

pub fn autonomous_action_inventory() -> &'static [AutonomousActionSurface] {
	AUTONOMOUS_ACTION_INVENTORY
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn autonomous_inventory_is_explicit_and_non_empty() {
		assert!(!autonomous_action_inventory().is_empty());
		assert!(
			autonomous_action_inventory()
				.contains(&AutonomousActionSurface::ControlRingPeerEviction)
		);
		assert!(autonomous_action_inventory().contains(&AutonomousActionSurface::ClockDriftAbort));
	}
}
