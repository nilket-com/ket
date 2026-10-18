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
async fn ctr4_v2_maintenance_preserves_pair_and_legacy_refuses_before_rewrite() {
	for legacy in [false, true] {
		for indexed in [false, true] {
			let (_dir, manager) = fresh_manager().await;
			let key = counter_identity(&CounterKey(b"maintenance".to_vec()));
			let origin = NodeId {
				id: 1,
				_reserved: 0,
			};
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
				let service = crate::storage::counter::CounterReplicaService::new(manager.clone());
				service
					.replicate_tentative(
						origin,
						1,
						tenant(),
						key,
						11,
						HybridTimestamp {
							physical_ms: 1,
							logical: 0,
							node_id: 0,
						},
					)
					.await
					.unwrap();
				service.commit(origin, 1, tenant(), key).await.unwrap();
			}
			let ordinary = if indexed {
				manager
					.append_durable(
						tenant(),
						counter_identity(&CounterKey(b"ordinary-indexed".to_vec())),
						hlc(2),
						b"blob",
					)
					.await
					.unwrap();
				true
			} else {
				false
			};
			let sealed = manager.force_seal_active_for_test().await;
			let bytes = manager.read_sealed_raw(sealed).await.unwrap();
			let entries = std::fs::read_dir(manager.data_dir())
				.unwrap()
				.map(|e| e.unwrap().file_name())
				.collect::<std::collections::HashSet<_>>();
			refusal(manager.refuse_counter_migration(sealed).await.unwrap_err());
			let expected_rows = usize::from(!legacy) + usize::from(ordinary);
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
			if !legacy {
				let source_frames = manager
					.state
					.lock()
					.await
					.sealed
					.get(&sealed)
					.unwrap()
					.value()
					.scan_counter_frames()
					.await
					.unwrap();
				assert_eq!(source_frames.len(), 2);
				assert_eq!(
					source_frames
						.iter()
						.map(|frame| frame.record_class)
						.collect::<Vec<_>>(),
					vec![
						crate::schema::record_class::COUNTER_TENTATIVE_V2,
						crate::schema::record_class::COUNTER_COMMIT_V2,
					]
				);
				let eligible_before = manager
					.counter_eligible_cells()
					.read(&(tenant(), key))
					.unwrap();
				let outcome = manager
					.compact_one_sealed_slab(
						sealed,
						&[],
						SecondaryIndexCompactionContext {
							table_schemas: &TableSchemaCatalog::default(),
							secondary_indexes: &SecondaryIndexCatalog::default(),
						},
					)
					.await
					.unwrap();
				assert!(outcome.compaction.is_some());
				let moved = manager
					.lookup(tenant(), key)
					.expect("V2 tentative remains indexed");
				let replacement = manager
					.state
					.lock()
					.await
					.sealed
					.get(&moved.slab_id)
					.unwrap()
					.value()
					.clone();
				let rewritten_frames = replacement.scan_counter_frames().await.unwrap();
				assert_eq!(rewritten_frames.len(), 2);
				let tentative_offset = rewritten_frames
					.iter()
					.find(|frame| {
						frame.record_class == crate::schema::record_class::COUNTER_TENTATIVE_V2
					})
					.unwrap()
					.location
					.offset;
				for (source, rewritten) in source_frames.iter().zip(&rewritten_frames) {
					assert_eq!(rewritten.record_class, source.record_class);
					assert_eq!(rewritten.location.hlc, source.location.hlc);
					let (value_or_offset, rewritten_origin) =
						crate::storage::counter::decode_origin_frame(&rewritten.payload).unwrap();
					assert_eq!(rewritten_origin, origin);
					match rewritten.record_class {
						crate::schema::record_class::COUNTER_TENTATIVE_V2 => {
							assert_eq!(value_or_offset, 11);
							assert_eq!(rewritten.payload, source.payload);
						}
						crate::schema::record_class::COUNTER_COMMIT_V2 => {
							assert_eq!(value_or_offset, tentative_offset);
							assert_eq!(&rewritten.payload[8..], &source.payload[8..]);
						}
						other => panic!("unexpected V2 counter class {other}"),
					}
				}
				assert_eq!(
					manager
						.counter_eligible_cells()
						.read(&(tenant(), key))
						.unwrap(),
					eligible_before,
					"eligible counter dump is unchanged by V2 rewrite"
				);
				assert_eq!(manager.read_sealed_raw(sealed).await.unwrap(), bytes);
			} else {
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
				match outcome {
					Err(error) => refusal(error),
					Ok(_) => panic!("legacy counter slab rewrite accepted"),
				}
			}

			if legacy {
				assert_eq!(manager.read_sealed_raw(sealed).await.unwrap(), bytes);
				assert_eq!(
					std::fs::read_dir(manager.data_dir())
						.unwrap()
						.map(|e| e.unwrap().file_name())
						.collect::<std::collections::HashSet<_>>(),
					entries,
					"legacy maintenance refusal must not write a replacement file"
				);
			}
			refusal(
				manager
					.encrypt_tiered_object_blob(sealed, &bytes)
					.unwrap_err(),
			);
		}
	}
}
