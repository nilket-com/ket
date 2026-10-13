use super::*;
#[test]
fn permissions_are_exact_bits() {
	for action in GovernanceAction2::ALL {
		assert!(!permits(0, action));
		assert!(!permits(0x1ff ^ action.bit(), action));
		assert!(permits(0x1ff, action));
		for candidate in GovernanceAction2::ALL {
			assert_eq!(permits(candidate.bit(), action), candidate == action);
		}
	}
}
