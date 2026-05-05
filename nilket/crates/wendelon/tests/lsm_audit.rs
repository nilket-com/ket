use wendelon::audit::AuditOperation;

#[test]
fn lsm_audit_operations_are_stable_strings() {
	let cases = [
		(
			AuditOperation::LsmCompactionCommitted,
			"\"lsm.compaction_committed\"",
		),
		(
			AuditOperation::LsmInvariantViolation,
			"\"lsm.invariant_violation\"",
		),
		(
			AuditOperation::LsmCompactionPaused,
			"\"lsm.compaction_paused\"",
		),
	];

	for (operation, encoded) in cases {
		assert_eq!(serde_json::to_string(&operation).unwrap(), encoded);
		assert_eq!(
			serde_json::from_str::<AuditOperation>(encoded).unwrap(),
			operation
		);
	}
}
