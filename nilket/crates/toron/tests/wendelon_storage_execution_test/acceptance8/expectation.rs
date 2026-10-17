pub(super) fn persistent(
	outcome: &Option<Result<wendelon_client::TableWriteReceipt, wendelon_client::ClientError>>,
	pending: bool,
) {
	assert!(!pending, "I must return before35s");
	match outcome {
		Some(Err(wendelon_client::ClientError::MutationStopped {
			status,
			reason,
			knowledge,
			..
		})) => {
			assert_eq!(*status, wendelon::schema::WriteStatus::WRITE_STALLED);
			assert_eq!(*reason, wendelon::schema::MutationStopReason::NO_PROGRESS);
			assert_eq!(
				*knowledge,
				wendelon::schema::MutationCommitKnowledge::INDETERMINATE
			);
		}
		other => panic!("wrong frozen persistent outcome: {other:?}"),
	}
}
