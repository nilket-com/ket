//! Maintenance entry points must refuse before rewriting counter record classes.
use super::*;
use crate::storage::counter::{CounterFormatError, CounterKey, counter_identity};

fn refusal(error: std::io::Error) {
	assert_eq!(error.kind(), std::io::ErrorKind::InvalidData);
	assert_eq!(
		error
			.get_ref()
			.unwrap()
			.downcast_ref::<CounterFormatError>(),
		Some(&CounterFormatError::MaintenanceUnsupported)
	);
}
#[tokio::test]
async fn ctr4_unsupported_maintenance_refuses_before_record_rewrite() {
	for legacy in [false, true] {
		for indexed in [false, true] {
			let (_dir, manager) = fresh_manager().await;
			let key = counter_identity(&CounterKey(b"maintenance".to_vec()));
			if legacy {
				let slab = manager.state.lock().await.active.clone();
				slab.write_framed(
					tenant(),
					key,
					hlc(1),
					crate::schema::record_class::COUNTER_TENTATIVE,
					&11u64.to_le_bytes(),
				)
				.await
				.unwrap();
			} else {
				manager
					.counter_append_tentative(
						tenant(),
						key,
						hlc(1),
						11,
						NodeId {
							id: 1,
							_reserved: 0,
						},
					)
					.await
					.unwrap();
			}
			if indexed {
				manager
					.append_durable(
						tenant(),
						counter_identity(&CounterKey(b"ordinary-indexed".to_vec())),
						hlc(2),
						b"blob",
					)
					.await
					.unwrap();
			}
			let sealed = manager.force_seal_active_for_test().await;
			let bytes = manager.read_sealed_raw(sealed).await.unwrap();
			let entries = std::fs::read_dir(manager.data_dir())
				.unwrap()
				.map(|e| e.unwrap().file_name())
				.collect::<std::collections::HashSet<_>>();
			refusal(manager.refuse_counter_migration(sealed).await.unwrap_err());
			let expected_rows = usize::from(!legacy) + usize::from(indexed);
			let indexed_rows: usize = manager
				.key_index
				.iter()
				.map(|entry| {
					entry
						.value()
						.iter()
						.filter(|location| location.slab_id == sealed)
						.count()
				})
				.sum();
			assert_eq!(indexed_rows, expected_rows);
			let manifest = manager.compaction_manifest_snapshot();
			if expected_rows == 0 {
				assert!(manifest.run(sealed).is_none());
			} else {
				assert!(manifest.run(sealed).is_some_and(|run| run.state.is_live()));
			}
			let outcome = manager
				.compact_one_sealed_slab(
					sealed,
					&[],
					SecondaryIndexCompactionContext {
						table_schemas: &TableSchemaCatalog::default(),
						secondary_indexes: &SecondaryIndexCatalog::default(),
					},
				)
				.await;
			if !legacy || indexed {
				match outcome {
					Err(error) => refusal(error),
					Ok(_) => panic!("mixed counter slab rewrite accepted"),
				}
			} else {
				let outcome = outcome.unwrap();
				assert!(matches!(outcome.scan, SealedSlabScan::Unclaimed));
				assert!(outcome.compaction.is_none());
			}

			assert_eq!(manager.read_sealed_raw(sealed).await.unwrap(), bytes);
			assert_eq!(
				std::fs::read_dir(manager.data_dir())
					.unwrap()
					.map(|e| e.unwrap().file_name())
					.collect::<std::collections::HashSet<_>>(),
				entries,
				"no replacement file written"
			);
			refusal(
				manager
					.encrypt_tiered_object_blob(sealed, &bytes)
					.unwrap_err(),
			);
		}
	}
}
