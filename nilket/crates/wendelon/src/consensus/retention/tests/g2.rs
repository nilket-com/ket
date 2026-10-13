//! A1 real file-backed Full store, synthetic producer; no Raft or state-machine apply.
use super::*;
use crate::test_support::g2::{Phase, case, emit};
use openraft::storage::RaftLogStorageExt;
use openraft::{CommittedLeaderId, Entry, EntryPayload};
use std::os::unix::fs::MetadataExt;
fn files(dir: &Path) -> serde_json::Value {
	let mut rows = Vec::new();
	for e in std::fs::read_dir(dir).unwrap() {
		let e = e.unwrap();
		let m = e.path().symlink_metadata().unwrap();
		assert!(m.is_file(), "unexpected scratch entry");
		rows.push(serde_json::json!({"name":e.file_name().to_str().unwrap(),
			"logical_bytes":m.len(),"allocated_bytes":m.blocks()*512}));
	}
	rows.sort_by_key(|r| r["name"].as_str().unwrap().to_owned());
	serde_json::Value::Array(rows)
}
#[tokio::test(flavor = "current_thread")]
#[ignore = "G2 A1: reviewed synthetic runner only"]
async fn g2_source_growth() {
	let c = case();
	assert_eq!(c["assay"], "A1");
	let n = c["n"].as_u64().unwrap();
	assert!([256, 4096].contains(&n));
	let mix = c["mix"].as_str().unwrap();
	assert!(["blank", "membership", "schema_1024", "schema_16384", "mix"].contains(&mix));
	let dir = crate::test_support::TestDir::new("wendelon-g2-source");
	let guard = RetentionGuard::new(ControlRetention::Full);
	let mut store = WendelonLogStore::open_for_retention_test(&dir, 262144, Some(guard.clone()))
		.await
		.unwrap();
	assert_eq!(guard.status().history, HistoryCheck::Fresh);
	let p = Phase::begin("empty");
	emit(p.end());
	let start = std::time::Instant::now();
	let mut payload_bytes = 0u64;
	for from in (0..n).step_by(256) {
		let entries = (from..(from + 256).min(n))
			.map(|i| {
				let kind = if i == 0 {
					"membership"
				} else if mix == "mix" {
					["blank", "membership", "schema_1024", "schema_16384"][i as usize % 4]
				} else {
					mix
				};
				let payload = match kind {
					"blank" => EntryPayload::Blank,
					"membership" => {
						EntryPayload::Membership(openraft::Membership::from(BTreeMap::from([(
							1,
							BasicNode {
								addr: "synthetic".into(),
							},
						)])))
					}
					"schema_1024" => {
						EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![0xa5; 1024]))
					}
					"schema_16384" => {
						EntryPayload::Normal(WendelonRequest::SchemaUpdate(vec![0xa5; 16384]))
					}
					_ => unreachable!(),
				};
				let e = Entry::<crate::consensus::WendelonTypeConfig> {
					log_id: LogId::new(CommittedLeaderId::new(1, 1), i),
					payload,
				};
				payload_bytes += postcard::experimental::serialized_size(&e).unwrap() as u64;
				e
			})
			.collect::<Vec<_>>();
		store.blocking_append(entries).await.unwrap();
	}
	let append_ns = start.elapsed().as_nanos();
	let p = Phase::begin("final");
	emit(p.end());
	emit(
		serde_json::json!({"event":"source","case":c,"entries":n,"payload_bytes":payload_bytes,
		"append_ns":append_ns,"files":files(&dir),"backend":"BlockingFileOps; no Raft/io_uring/apply",
		"checkpoint_schedule":"none beyond durable append persistence"}),
	);
	assert_eq!(guard.status().violation, None);
	for i in 0..n {
		let row = store.try_get_log_entries(i..=i).await.unwrap();
		assert_eq!(row.len(), 1);
		assert_eq!(row[0].log_id.index, i);
	}
	drop(store);
	let reopened_guard = RetentionGuard::new(ControlRetention::Full);
	let reopened =
		WendelonLogStore::open_for_retention_test(&dir, 262144, Some(reopened_guard.clone()))
			.await
			.unwrap();
	assert_eq!(
		reopened_guard.status().history,
		HistoryCheck::Complete { stored_last: n - 1 }
	);
	drop(reopened);
}
