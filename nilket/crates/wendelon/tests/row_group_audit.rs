use wendelon::audit::AuditOperation;

#[test]
fn row_group_audit_operations_are_stable_strings() {
	let cases = [
		(
			AuditOperation::RowGroupFetchSampled,
			r#""row_group.fetch_sampled""#,
		),
		(
			AuditOperation::RowGroupInvariantViolation,
			r#""row_group.invariant_violation""#,
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
