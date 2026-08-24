//! ADR 0092: versioned WAL-body replay.
//!
//! The WAL body is `postcard(Entry<WendelonTypeConfig>)` — positional and not
//! self-describing — so a superseded body version must be decoded by its own
//! frozen shape and upconverted, never decoded by the current shape. Each
//! version module here freezes the **changed container spine**: derived Serde
//! cannot substitute a leaf inside a derived container, so the
//! `WendelonRequest` enum spine (every discriminant, in order) is frozen per
//! version, while unchanged payload types are referenced from the live schema.
//!
//! These modules are replay machinery, not an API: nothing outside the WAL
//! decode dispatch and the format-fixture controls may name them.

/// The version-1 body shape: ADR 0085's, before ADR 0087 appended the
/// materialization certificate to the CTAS `Provision` mutation.
///
/// Frozen spine: `WendelonRequest` and `CtasSagaMutation`. Everything either
/// enum carries besides the certificate is referenced from the live types,
/// and the byte fixtures pinning this shape fail if any referenced type's
/// encoding ever moves — which is the moment this module must fork its own
/// copy.
pub(crate) mod v1 {
	use crate::consensus::core::{
		AdvanceEpochRecord, CreateTenant, MigrationComplete, SeedMigration,
		SetTableRetentionPolicy, TableDropFinalizeRequest, TableDropPrepareRequest,
		TableReclaimCompletionRequest, TenantFinalization, TieredSlab, UpdateTenantQuota,
		WendelonResponse,
	};
	use crate::consensus::{RetireTieredSlab, WendelonRequest as CurrentRequest};
	use crate::crypto::{InstallTenantKekVersion, RetireTenantKekVersion};
	use crate::schema::{
		ClientWriteId, CtasExpectedBinding, CtasIntentFingerprint, CtasOperationId,
		CtasSagaMutation as CurrentMutation, CtasTerminalCause, LeaseDescriptor,
		ReleaseLeaseRequestMeta, TableSchemaDescriptor, TenantId,
	};
	use crate::storage::CompactionCommit as CompactionCommitBody;
	use serde::{Deserialize, Serialize};

	/// The v1 `WendelonRequest` discriminant spine. Order is the contract:
	/// postcard stores only the variant index, so this list must never be
	/// reordered, and it is complete for version 1 by the grounded fact that
	/// no consensus body type changed between ADR 0085 and ADR 0087.
	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub(crate) enum WendelonRequest {
		Finalize(TenantFinalization),
		AcquireLease(LeaseDescriptor),
		ReleaseLease(ReleaseLeaseRequestMeta),
		AdvanceEpoch(AdvanceEpochRecord),
		#[serde(with = "serde_bytes")]
		SchemaUpdate(Vec<u8>),
		TierSlab(TieredSlab),
		RetireTieredSlab(RetireTieredSlab),
		CompactionCommit(CompactionCommitBody),
		SeedMigration(SeedMigration),
		MigrationComplete(MigrationComplete),
		CreateTenant(CreateTenant),
		UpdateTenantQuota(UpdateTenantQuota),
		SuspendTenant(TenantId),
		ResumeTenant(TenantId),
		DeleteTenant(TenantId),
		InstallTenantKekVersion(InstallTenantKekVersion),
		RetireTenantKekVersion(RetireTenantKekVersion),
		RevokeActorKey(crate::consensus::core::RevokeActorKey),
		SetTableRetentionPolicy(SetTableRetentionPolicy),
		TableDropPrepare(TableDropPrepareRequest),
		TableReclaimCompletion(TableReclaimCompletionRequest),
		TableDropFinalize(TableDropFinalizeRequest),
		CtasSaga(CtasSagaMutation),
	}

	/// The v1 `CtasSagaMutation` spine: `Provision` without the certificate
	/// field, every other variant shape-identical to the current one.
	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub(crate) enum CtasSagaMutation {
		Provision {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			intent_fingerprint: CtasIntentFingerprint,
			descriptor: TableSchemaDescriptor,
			expected_binding: CtasExpectedBinding,
			prior_descriptor: Option<TableSchemaDescriptor>,
			row_write_id: ClientWriteId,
			owner_epoch: u64,
			observed_now_ms: u64,
			owner_lease_expires_at_ms: u64,
		},
		AttestRowsCommitted {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			row_count: u64,
		},
		MarkRowDispatched {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			row_count: u64,
		},
		Publish {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
		},
		MarkRetiringOld {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
		},
		Complete {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			completed_at_ms: u64,
		},
		Reclaim {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			cause: CtasTerminalCause,
			completed_at_ms: u64,
		},
		RecordPublicationConflict {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			completed_at_ms: u64,
		},
		RecordRetryDebt {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
		},
		Claim {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			expected_owner_epoch: u64,
			new_owner_epoch: u64,
			observed_now_ms: u64,
			lease_expires_at_ms: u64,
		},
	}

	openraft::declare_raft_types!(
		/// A type config whose only difference from `WendelonTypeConfig` is
		/// the v1 request shape, so openraft's own generics guarantee the
		/// surrounding `Entry`/`EntryPayload`/`Membership` layout is decoded
		/// exactly as the current one is.
		pub(crate) WendelonWalV1TypeConfig:
			D = WendelonRequest,
			R = WendelonResponse,
			NodeId = u32,
			Node = openraft::BasicNode,
			Entry = openraft::Entry<WendelonWalV1TypeConfig>,
			SnapshotData = crate::consensus::raft_storage::SnapshotDataFile,
			AsyncRuntime = openraft::TokioRuntime,
	);

	/// The total v1 → current conversion: `Provision` gains
	/// `certificate: None` — exactly what every v1 writer meant — and every
	/// other variant moves unchanged.
	impl From<CtasSagaMutation> for CurrentMutation {
		fn from(mutation: CtasSagaMutation) -> Self {
			match mutation {
				CtasSagaMutation::Provision {
					tenant_id,
					operation_id,
					intent_fingerprint,
					descriptor,
					expected_binding,
					prior_descriptor,
					row_write_id,
					owner_epoch,
					observed_now_ms,
					owner_lease_expires_at_ms,
				} => Self::Provision {
					tenant_id,
					operation_id,
					intent_fingerprint,
					descriptor,
					expected_binding,
					prior_descriptor,
					row_write_id,
					owner_epoch,
					observed_now_ms,
					owner_lease_expires_at_ms,
					certificate: None,
				},
				CtasSagaMutation::AttestRowsCommitted {
					tenant_id,
					operation_id,
					owner_epoch,
					row_count,
				} => Self::AttestRowsCommitted {
					tenant_id,
					operation_id,
					owner_epoch,
					row_count,
				},
				CtasSagaMutation::MarkRowDispatched {
					tenant_id,
					operation_id,
					owner_epoch,
					row_count,
				} => Self::MarkRowDispatched {
					tenant_id,
					operation_id,
					owner_epoch,
					row_count,
				},
				CtasSagaMutation::Publish {
					tenant_id,
					operation_id,
					owner_epoch,
				} => Self::Publish {
					tenant_id,
					operation_id,
					owner_epoch,
				},
				CtasSagaMutation::MarkRetiringOld {
					tenant_id,
					operation_id,
					owner_epoch,
				} => Self::MarkRetiringOld {
					tenant_id,
					operation_id,
					owner_epoch,
				},
				CtasSagaMutation::Complete {
					tenant_id,
					operation_id,
					owner_epoch,
					completed_at_ms,
				} => Self::Complete {
					tenant_id,
					operation_id,
					owner_epoch,
					completed_at_ms,
				},
				CtasSagaMutation::Reclaim {
					tenant_id,
					operation_id,
					owner_epoch,
					cause,
					completed_at_ms,
				} => Self::Reclaim {
					tenant_id,
					operation_id,
					owner_epoch,
					cause,
					completed_at_ms,
				},
				CtasSagaMutation::RecordPublicationConflict {
					tenant_id,
					operation_id,
					owner_epoch,
					completed_at_ms,
				} => Self::RecordPublicationConflict {
					tenant_id,
					operation_id,
					owner_epoch,
					completed_at_ms,
				},
				CtasSagaMutation::RecordRetryDebt {
					tenant_id,
					operation_id,
					owner_epoch,
				} => Self::RecordRetryDebt {
					tenant_id,
					operation_id,
					owner_epoch,
				},
				CtasSagaMutation::Claim {
					tenant_id,
					operation_id,
					expected_owner_epoch,
					new_owner_epoch,
					observed_now_ms,
					lease_expires_at_ms,
				} => Self::Claim {
					tenant_id,
					operation_id,
					expected_owner_epoch,
					new_owner_epoch,
					observed_now_ms,
					lease_expires_at_ms,
				},
			}
		}
	}

	impl From<WendelonRequest> for CurrentRequest {
		fn from(request: WendelonRequest) -> Self {
			match request {
				WendelonRequest::Finalize(body) => Self::Finalize(body),
				WendelonRequest::AcquireLease(body) => Self::AcquireLease(body),
				WendelonRequest::ReleaseLease(body) => Self::ReleaseLease(body),
				WendelonRequest::AdvanceEpoch(body) => Self::AdvanceEpoch(body),
				WendelonRequest::SchemaUpdate(body) => Self::SchemaUpdate(body),
				WendelonRequest::TierSlab(body) => Self::TierSlab(body),
				WendelonRequest::RetireTieredSlab(body) => Self::RetireTieredSlab(body),
				WendelonRequest::CompactionCommit(body) => Self::CompactionCommit(body),
				WendelonRequest::SeedMigration(body) => Self::SeedMigration(body),
				WendelonRequest::MigrationComplete(body) => Self::MigrationComplete(body),
				WendelonRequest::CreateTenant(body) => Self::CreateTenant(body),
				WendelonRequest::UpdateTenantQuota(body) => Self::UpdateTenantQuota(body),
				WendelonRequest::SuspendTenant(body) => Self::SuspendTenant(body),
				WendelonRequest::ResumeTenant(body) => Self::ResumeTenant(body),
				WendelonRequest::DeleteTenant(body) => Self::DeleteTenant(body),
				WendelonRequest::InstallTenantKekVersion(body) => {
					Self::InstallTenantKekVersion(body)
				}
				WendelonRequest::RetireTenantKekVersion(body) => Self::RetireTenantKekVersion(body),
				WendelonRequest::RevokeActorKey(body) => Self::RevokeActorKey(body),
				WendelonRequest::SetTableRetentionPolicy(body) => {
					Self::SetTableRetentionPolicy(body)
				}
				WendelonRequest::TableDropPrepare(body) => Self::TableDropPrepare(body),
				WendelonRequest::TableReclaimCompletion(body) => Self::TableReclaimCompletion(body),
				WendelonRequest::TableDropFinalize(body) => Self::TableDropFinalize(body),
				WendelonRequest::CtasSaga(mutation) => Self::CtasSaga(mutation.into()),
			}
		}
	}

	/// Decode a v1 WAL body and upconvert it. The payload must be consumed
	/// exactly: trailing bytes mean the record does not actually hold the
	/// shape its header declares, and guessing would defeat the dispatch.
	pub(crate) fn decode_entry(
		payload: &[u8],
	) -> std::io::Result<openraft::Entry<crate::consensus::WendelonTypeConfig>> {
		let (entry, rest) =
			postcard::take_from_bytes::<openraft::Entry<WendelonWalV1TypeConfig>>(payload)
				.map_err(|err| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						format!("log entry postcard decode failed: {err}"),
					)
				})?;
		if !rest.is_empty() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"log entry decoded with {} unconsumed payload bytes",
					rest.len()
				),
			));
		}
		let payload = match entry.payload {
			openraft::EntryPayload::Blank => openraft::EntryPayload::Blank,
			openraft::EntryPayload::Normal(request) => {
				openraft::EntryPayload::Normal(request.into())
			}
			openraft::EntryPayload::Membership(membership) => {
				openraft::EntryPayload::Membership(membership)
			}
		};
		Ok(openraft::Entry {
			log_id: entry.log_id,
			payload,
		})
	}
}

/// WAL v2: ADR 0087's certificate-bearing CTAS mutation spine, before ADR
/// 0094 split chain declaration from row dispatch.
pub(crate) mod v2 {
	use crate::consensus::core::{
		AdvanceEpochRecord, CreateTenant, MigrationComplete, SeedMigration,
		SetTableRetentionPolicy, TableDropFinalizeRequest, TableDropPrepareRequest,
		TableReclaimCompletionRequest, TenantFinalization, TieredSlab, UpdateTenantQuota,
		WendelonResponse,
	};
	use crate::consensus::{RetireTieredSlab, WendelonRequest as CurrentRequest};
	use crate::crypto::{InstallTenantKekVersion, RetireTenantKekVersion};
	use crate::schema::{
		ClientWriteId, CtasExpectedBinding, CtasIntentFingerprint, CtasOperationId,
		CtasSagaMutation as CurrentMutation, CtasTerminalCause, LeaseDescriptor,
		ReleaseLeaseRequestMeta, ReplayLegacyCtasRowDispatched, TableSchemaDescriptor, TenantId,
	};
	use crate::storage::CompactionCommit as CompactionCommitBody;
	use serde::{Deserialize, Serialize};

	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub(crate) enum WendelonRequest {
		Finalize(TenantFinalization),
		AcquireLease(LeaseDescriptor),
		ReleaseLease(ReleaseLeaseRequestMeta),
		AdvanceEpoch(AdvanceEpochRecord),
		#[serde(with = "serde_bytes")]
		SchemaUpdate(Vec<u8>),
		TierSlab(TieredSlab),
		RetireTieredSlab(RetireTieredSlab),
		CompactionCommit(CompactionCommitBody),
		SeedMigration(SeedMigration),
		MigrationComplete(MigrationComplete),
		CreateTenant(CreateTenant),
		UpdateTenantQuota(UpdateTenantQuota),
		SuspendTenant(TenantId),
		ResumeTenant(TenantId),
		DeleteTenant(TenantId),
		InstallTenantKekVersion(InstallTenantKekVersion),
		RetireTenantKekVersion(RetireTenantKekVersion),
		RevokeActorKey(crate::consensus::core::RevokeActorKey),
		SetTableRetentionPolicy(SetTableRetentionPolicy),
		TableDropPrepare(TableDropPrepareRequest),
		TableReclaimCompletion(TableReclaimCompletionRequest),
		TableDropFinalize(TableDropFinalizeRequest),
		CtasSaga(CtasSagaMutation),
	}

	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub(crate) enum CtasSagaMutation {
		Provision {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			intent_fingerprint: CtasIntentFingerprint,
			descriptor: TableSchemaDescriptor,
			expected_binding: CtasExpectedBinding,
			prior_descriptor: Option<TableSchemaDescriptor>,
			row_write_id: ClientWriteId,
			owner_epoch: u64,
			observed_now_ms: u64,
			owner_lease_expires_at_ms: u64,
			certificate: Option<Vec<u8>>,
		},
		AttestRowsCommitted {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			row_count: u64,
		},
		MarkRowDispatched {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			row_count: u64,
		},
		Publish {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
		},
		MarkRetiringOld {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
		},
		Complete {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			completed_at_ms: u64,
		},
		Reclaim {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			cause: CtasTerminalCause,
			completed_at_ms: u64,
		},
		RecordPublicationConflict {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
			completed_at_ms: u64,
		},
		RecordRetryDebt {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			owner_epoch: u64,
		},
		Claim {
			tenant_id: TenantId,
			operation_id: CtasOperationId,
			expected_owner_epoch: u64,
			new_owner_epoch: u64,
			observed_now_ms: u64,
			lease_expires_at_ms: u64,
		},
	}

	openraft::declare_raft_types!(
		pub(crate) WendelonWalV2TypeConfig:
			D = WendelonRequest,
			R = WendelonResponse,
			NodeId = u32,
			Node = openraft::BasicNode,
			Entry = openraft::Entry<WendelonWalV2TypeConfig>,
			SnapshotData = crate::consensus::raft_storage::SnapshotDataFile,
			AsyncRuntime = openraft::TokioRuntime,
	);

	fn convert_mutation(mutation: CtasSagaMutation) -> CurrentRequest {
		match mutation {
			CtasSagaMutation::MarkRowDispatched {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			} => CurrentRequest::ReplayLegacyCtasRowDispatched(ReplayLegacyCtasRowDispatched {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			}),
			CtasSagaMutation::Provision {
				tenant_id,
				operation_id,
				intent_fingerprint,
				descriptor,
				expected_binding,
				prior_descriptor,
				row_write_id,
				owner_epoch,
				observed_now_ms,
				owner_lease_expires_at_ms,
				certificate,
			} => CurrentRequest::CtasSaga(CurrentMutation::Provision {
				tenant_id,
				operation_id,
				intent_fingerprint,
				descriptor,
				expected_binding,
				prior_descriptor,
				row_write_id,
				owner_epoch,
				observed_now_ms,
				owner_lease_expires_at_ms,
				certificate,
			}),
			CtasSagaMutation::AttestRowsCommitted {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			} => CurrentRequest::CtasSaga(CurrentMutation::AttestRowsCommitted {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			}),
			CtasSagaMutation::Publish {
				tenant_id,
				operation_id,
				owner_epoch,
			} => CurrentRequest::CtasSaga(CurrentMutation::Publish {
				tenant_id,
				operation_id,
				owner_epoch,
			}),
			CtasSagaMutation::MarkRetiringOld {
				tenant_id,
				operation_id,
				owner_epoch,
			} => CurrentRequest::CtasSaga(CurrentMutation::MarkRetiringOld {
				tenant_id,
				operation_id,
				owner_epoch,
			}),
			CtasSagaMutation::Complete {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			} => CurrentRequest::CtasSaga(CurrentMutation::Complete {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			}),
			CtasSagaMutation::Reclaim {
				tenant_id,
				operation_id,
				owner_epoch,
				cause,
				completed_at_ms,
			} => CurrentRequest::CtasSaga(CurrentMutation::Reclaim {
				tenant_id,
				operation_id,
				owner_epoch,
				cause,
				completed_at_ms,
			}),
			CtasSagaMutation::RecordPublicationConflict {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			} => CurrentRequest::CtasSaga(CurrentMutation::RecordPublicationConflict {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			}),
			CtasSagaMutation::RecordRetryDebt {
				tenant_id,
				operation_id,
				owner_epoch,
			} => CurrentRequest::CtasSaga(CurrentMutation::RecordRetryDebt {
				tenant_id,
				operation_id,
				owner_epoch,
			}),
			CtasSagaMutation::Claim {
				tenant_id,
				operation_id,
				expected_owner_epoch,
				new_owner_epoch,
				observed_now_ms,
				lease_expires_at_ms,
			} => CurrentRequest::CtasSaga(CurrentMutation::Claim {
				tenant_id,
				operation_id,
				expected_owner_epoch,
				new_owner_epoch,
				observed_now_ms,
				lease_expires_at_ms,
			}),
		}
	}

	#[cfg(test)]
	pub(crate) fn project_mutation(mutation: CurrentMutation) -> Option<CtasSagaMutation> {
		Some(match mutation {
			CurrentMutation::Provision {
				tenant_id,
				operation_id,
				intent_fingerprint,
				descriptor,
				expected_binding,
				prior_descriptor,
				row_write_id,
				owner_epoch,
				observed_now_ms,
				owner_lease_expires_at_ms,
				certificate,
			} => CtasSagaMutation::Provision {
				tenant_id,
				operation_id,
				intent_fingerprint,
				descriptor,
				expected_binding,
				prior_descriptor,
				row_write_id,
				owner_epoch,
				observed_now_ms,
				owner_lease_expires_at_ms,
				certificate,
			},
			CurrentMutation::AttestRowsCommitted {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			} => CtasSagaMutation::AttestRowsCommitted {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			},
			CurrentMutation::MarkRowDispatched {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			} => CtasSagaMutation::MarkRowDispatched {
				tenant_id,
				operation_id,
				owner_epoch,
				row_count,
			},
			CurrentMutation::Publish {
				tenant_id,
				operation_id,
				owner_epoch,
			} => CtasSagaMutation::Publish {
				tenant_id,
				operation_id,
				owner_epoch,
			},
			CurrentMutation::MarkRetiringOld {
				tenant_id,
				operation_id,
				owner_epoch,
			} => CtasSagaMutation::MarkRetiringOld {
				tenant_id,
				operation_id,
				owner_epoch,
			},
			CurrentMutation::Complete {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			} => CtasSagaMutation::Complete {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			},
			CurrentMutation::Reclaim {
				tenant_id,
				operation_id,
				owner_epoch,
				cause,
				completed_at_ms,
			} => CtasSagaMutation::Reclaim {
				tenant_id,
				operation_id,
				owner_epoch,
				cause,
				completed_at_ms,
			},
			CurrentMutation::RecordPublicationConflict {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			} => CtasSagaMutation::RecordPublicationConflict {
				tenant_id,
				operation_id,
				owner_epoch,
				completed_at_ms,
			},
			CurrentMutation::RecordRetryDebt {
				tenant_id,
				operation_id,
				owner_epoch,
			} => CtasSagaMutation::RecordRetryDebt {
				tenant_id,
				operation_id,
				owner_epoch,
			},
			CurrentMutation::Claim {
				tenant_id,
				operation_id,
				expected_owner_epoch,
				new_owner_epoch,
				observed_now_ms,
				lease_expires_at_ms,
			} => CtasSagaMutation::Claim {
				tenant_id,
				operation_id,
				expected_owner_epoch,
				new_owner_epoch,
				observed_now_ms,
				lease_expires_at_ms,
			},
			CurrentMutation::DeclareChain { .. }
			| CurrentMutation::MarkChainPinsReleased { .. } => return None,
		})
	}

	impl From<WendelonRequest> for CurrentRequest {
		fn from(request: WendelonRequest) -> Self {
			match request {
				WendelonRequest::Finalize(v) => Self::Finalize(v),
				WendelonRequest::AcquireLease(v) => Self::AcquireLease(v),
				WendelonRequest::ReleaseLease(v) => Self::ReleaseLease(v),
				WendelonRequest::AdvanceEpoch(v) => Self::AdvanceEpoch(v),
				WendelonRequest::SchemaUpdate(v) => Self::SchemaUpdate(v),
				WendelonRequest::TierSlab(v) => Self::TierSlab(v),
				WendelonRequest::RetireTieredSlab(v) => Self::RetireTieredSlab(v),
				WendelonRequest::CompactionCommit(v) => Self::CompactionCommit(v),
				WendelonRequest::SeedMigration(v) => Self::SeedMigration(v),
				WendelonRequest::MigrationComplete(v) => Self::MigrationComplete(v),
				WendelonRequest::CreateTenant(v) => Self::CreateTenant(v),
				WendelonRequest::UpdateTenantQuota(v) => Self::UpdateTenantQuota(v),
				WendelonRequest::SuspendTenant(v) => Self::SuspendTenant(v),
				WendelonRequest::ResumeTenant(v) => Self::ResumeTenant(v),
				WendelonRequest::DeleteTenant(v) => Self::DeleteTenant(v),
				WendelonRequest::InstallTenantKekVersion(v) => Self::InstallTenantKekVersion(v),
				WendelonRequest::RetireTenantKekVersion(v) => Self::RetireTenantKekVersion(v),
				WendelonRequest::RevokeActorKey(v) => Self::RevokeActorKey(v),
				WendelonRequest::SetTableRetentionPolicy(v) => Self::SetTableRetentionPolicy(v),
				WendelonRequest::TableDropPrepare(v) => Self::TableDropPrepare(v),
				WendelonRequest::TableReclaimCompletion(v) => Self::TableReclaimCompletion(v),
				WendelonRequest::TableDropFinalize(v) => Self::TableDropFinalize(v),
				WendelonRequest::CtasSaga(v) => convert_mutation(v),
			}
		}
	}

	pub(crate) fn decode_entry(
		payload: &[u8],
	) -> std::io::Result<openraft::Entry<crate::consensus::WendelonTypeConfig>> {
		let (entry, rest) =
			postcard::take_from_bytes::<openraft::Entry<WendelonWalV2TypeConfig>>(payload)
				.map_err(|err| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						format!("log entry postcard decode failed: {err}"),
					)
				})?;
		if !rest.is_empty() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"log entry decoded with {} unconsumed payload bytes",
					rest.len()
				),
			));
		}
		let payload = match entry.payload {
			openraft::EntryPayload::Blank => openraft::EntryPayload::Blank,
			openraft::EntryPayload::Normal(request) => {
				openraft::EntryPayload::Normal(request.into())
			}
			openraft::EntryPayload::Membership(membership) => {
				openraft::EntryPayload::Membership(membership)
			}
		};
		Ok(openraft::Entry {
			log_id: entry.log_id,
			payload,
		})
	}
}

/// WAL v3: ADR 0094's chain-aware request spine before ADR 0095 appended
/// logical DROP finalize and settlement mutations. None of the payload shapes
/// referenced here changed in ADR 0095; the frozen enum spine is the boundary.
pub(crate) mod v3 {
	use crate::consensus::core::{
		AdvanceEpochRecord, CreateTenant, MigrationComplete, SeedMigration,
		SetTableRetentionPolicy, TableDropFinalizeRequest, TableDropPrepareRequest,
		TableReclaimCompletionRequest, TenantFinalization, TieredSlab, UpdateTenantQuota,
		WendelonResponse,
	};
	use crate::consensus::{RetireTieredSlab, WendelonRequest as CurrentRequest};
	use crate::crypto::{InstallTenantKekVersion, RetireTenantKekVersion};
	use crate::schema::{
		CtasSagaMutation, LeaseDescriptor, ReleaseLeaseRequestMeta, ReplayLegacyCtasRowDispatched,
		TenantId,
	};
	use crate::storage::CompactionCommit as CompactionCommitBody;
	use serde::{Deserialize, Serialize};

	#[derive(Clone, Debug, Serialize, Deserialize)]
	pub(crate) enum WendelonRequest {
		Finalize(TenantFinalization),
		AcquireLease(LeaseDescriptor),
		ReleaseLease(ReleaseLeaseRequestMeta),
		AdvanceEpoch(AdvanceEpochRecord),
		#[serde(with = "serde_bytes")]
		SchemaUpdate(Vec<u8>),
		TierSlab(TieredSlab),
		RetireTieredSlab(RetireTieredSlab),
		CompactionCommit(CompactionCommitBody),
		SeedMigration(SeedMigration),
		MigrationComplete(MigrationComplete),
		CreateTenant(CreateTenant),
		UpdateTenantQuota(UpdateTenantQuota),
		SuspendTenant(TenantId),
		ResumeTenant(TenantId),
		DeleteTenant(TenantId),
		InstallTenantKekVersion(InstallTenantKekVersion),
		RetireTenantKekVersion(RetireTenantKekVersion),
		RevokeActorKey(crate::consensus::core::RevokeActorKey),
		SetTableRetentionPolicy(SetTableRetentionPolicy),
		TableDropPrepare(TableDropPrepareRequest),
		TableReclaimCompletion(TableReclaimCompletionRequest),
		TableDropFinalize(TableDropFinalizeRequest),
		CtasSaga(CtasSagaMutation),
		ReplayLegacyCtasRowDispatched(ReplayLegacyCtasRowDispatched),
	}

	openraft::declare_raft_types!(
		pub(crate) WendelonWalV3TypeConfig:
			D = WendelonRequest,
			R = WendelonResponse,
			NodeId = u32,
			Node = openraft::BasicNode,
			Entry = openraft::Entry<WendelonWalV3TypeConfig>,
			SnapshotData = crate::consensus::raft_storage::SnapshotDataFile,
			AsyncRuntime = openraft::TokioRuntime,
	);

	impl From<WendelonRequest> for CurrentRequest {
		fn from(request: WendelonRequest) -> Self {
			match request {
				WendelonRequest::Finalize(v) => Self::Finalize(v),
				WendelonRequest::AcquireLease(v) => Self::AcquireLease(v),
				WendelonRequest::ReleaseLease(v) => Self::ReleaseLease(v),
				WendelonRequest::AdvanceEpoch(v) => Self::AdvanceEpoch(v),
				WendelonRequest::SchemaUpdate(v) => Self::SchemaUpdate(v),
				WendelonRequest::TierSlab(v) => Self::TierSlab(v),
				WendelonRequest::RetireTieredSlab(v) => Self::RetireTieredSlab(v),
				WendelonRequest::CompactionCommit(v) => Self::CompactionCommit(v),
				WendelonRequest::SeedMigration(v) => Self::SeedMigration(v),
				WendelonRequest::MigrationComplete(v) => Self::MigrationComplete(v),
				WendelonRequest::CreateTenant(v) => Self::CreateTenant(v),
				WendelonRequest::UpdateTenantQuota(v) => Self::UpdateTenantQuota(v),
				WendelonRequest::SuspendTenant(v) => Self::SuspendTenant(v),
				WendelonRequest::ResumeTenant(v) => Self::ResumeTenant(v),
				WendelonRequest::DeleteTenant(v) => Self::DeleteTenant(v),
				WendelonRequest::InstallTenantKekVersion(v) => Self::InstallTenantKekVersion(v),
				WendelonRequest::RetireTenantKekVersion(v) => Self::RetireTenantKekVersion(v),
				WendelonRequest::RevokeActorKey(v) => Self::RevokeActorKey(v),
				WendelonRequest::SetTableRetentionPolicy(v) => Self::SetTableRetentionPolicy(v),
				WendelonRequest::TableDropPrepare(v) => Self::TableDropPrepare(v),
				WendelonRequest::TableReclaimCompletion(v) => Self::TableReclaimCompletion(v),
				WendelonRequest::TableDropFinalize(v) => Self::TableDropFinalize(v),
				WendelonRequest::CtasSaga(v) => Self::CtasSaga(v),
				WendelonRequest::ReplayLegacyCtasRowDispatched(v) => {
					Self::ReplayLegacyCtasRowDispatched(v)
				}
			}
		}
	}

	pub(crate) fn decode_entry(
		payload: &[u8],
	) -> std::io::Result<openraft::Entry<crate::consensus::WendelonTypeConfig>> {
		let (entry, rest) =
			postcard::take_from_bytes::<openraft::Entry<WendelonWalV3TypeConfig>>(payload)
				.map_err(|err| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						format!("log entry postcard decode failed: {err}"),
					)
				})?;
		if !rest.is_empty() {
			return Err(std::io::Error::new(
				std::io::ErrorKind::InvalidData,
				format!(
					"log entry decoded with {} unconsumed payload bytes",
					rest.len()
				),
			));
		}
		let payload = match entry.payload {
			openraft::EntryPayload::Blank => openraft::EntryPayload::Blank,
			openraft::EntryPayload::Normal(request) => {
				openraft::EntryPayload::Normal(request.into())
			}
			openraft::EntryPayload::Membership(membership) => {
				openraft::EntryPayload::Membership(membership)
			}
		};
		Ok(openraft::Entry {
			log_id: entry.log_id,
			payload,
		})
	}
}
