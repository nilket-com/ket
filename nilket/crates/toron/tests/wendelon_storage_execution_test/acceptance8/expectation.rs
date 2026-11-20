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
			// The server stops the held attempt at its stall bound (WRITE_STALLED,
			// NO_PROGRESS, INDETERMINATE) and records that outcome (ADR 542); the
			// shared client retries a stall under the same identity within its
			// budget (ADR 0700), each retry replays the recorded stall with no new
			// append, and the exhausted call stays INDETERMINATE.
			assert_eq!(
				*status,
				wendelon::schema::WriteStatus::WRITE_ATTEMPTS_EXHAUSTED
			);
			assert_eq!(
				*reason,
				wendelon::schema::MutationStopReason::ATTEMPT_EXHAUSTION
			);
			assert_eq!(
				*knowledge,
				wendelon::schema::MutationCommitKnowledge::INDETERMINATE
			);
		}
		other => panic!("wrong frozen persistent outcome: {other:?}"),
	}
}

/// The server's persisted record for the held write's own identity: the stall
/// itself (ADR 0542's post-dispatch hold), whatever the SDK then reported.
pub(super) fn persisted_stall(
	resolved: &wendelon_client::ResolvedMutationOutcome,
	expected_id: wendelon::schema::ClientWriteId,
) {
	match resolved {
		wendelon_client::ResolvedMutationOutcome::Present {
			operation_id,
			status,
			reason,
			knowledge,
			..
		} => {
			assert_eq!(*operation_id, expected_id, "the same identity");
			assert_eq!(*status, wendelon::schema::WriteStatus::WRITE_STALLED);
			assert_eq!(*reason, wendelon::schema::MutationStopReason::NO_PROGRESS);
			assert_eq!(
				*knowledge,
				wendelon::schema::MutationCommitKnowledge::INDETERMINATE
			);
		}
		other => panic!("the held write's record is not its stall: {other:?}"),
	}
}
