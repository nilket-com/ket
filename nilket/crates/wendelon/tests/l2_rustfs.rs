//! The remote object-store adapter against a real RustFS server over HTTP. External: see
//! `support/rustfs_env.rs`. Run with
//! `cargo test -p wendelon --test l2_rustfs -- --ignored`.
//!
//! What this covers: exact bytes through put, get and ranged get, head size, list, a missing key,
//! wrong credentials, and deletion of this test's own objects. It does not cover multipart upload
//! (the adapter has none), other providers, or the database's tiering path.

#[path = "support/rustfs_env.rs"]
mod rustfs_env;

use std::io::ErrorKind;
use wendelon::storage::{ObjectStore, RemoteObjectStore};

/// Deterministic bytes that do not repeat on any short period.
fn payload(len: usize) -> Vec<u8> {
	let mut state = 0x9e37_79b9_7f4a_7c15_u64;
	(0..len)
		.map(|_| {
			state ^= state << 13;
			state ^= state >> 7;
			state ^= state << 17;
			(state >> 32) as u8
		})
		.collect()
}

#[tokio::test]
#[ignore = "external: needs a running RustFS (support/rustfs_env.rs); run with --ignored"]
async fn rustfs_round_trips_exact_bytes_and_lists_and_deletes_its_own_objects() {
	let store = RemoteObjectStore::new(rustfs_env::remote_config("adapter")).unwrap();
	let small = b"wendelon adapter check".to_vec();
	let large = payload(4 * 1024 * 1024 + 123);

	let small_uri = store.put("check/small.bin", &small).await.unwrap();
	let large_uri = store.put("check/nested/large.bin", &large).await.unwrap();
	assert!(small_uri.starts_with("l2://"), "{small_uri}");

	assert_eq!(store.get(&small_uri).await.unwrap(), small);
	assert_eq!(store.get(&large_uri).await.unwrap(), large);
	assert_eq!(
		store.head(&large_uri).await.unwrap().size_bytes,
		large.len() as u64
	);
	for range in [
		0..1_u64,
		5..4096,
		1_048_570..1_048_600,
		(large.len() as u64 - 7)..large.len() as u64,
	] {
		let got = store.get_range(&large_uri, range.clone()).await.unwrap();
		assert_eq!(
			&got[..],
			&large[range.start as usize..range.end as usize],
			"{range:?}"
		);
	}

	let listed = store.list("check/").await.unwrap();
	let mut seen = listed
		.iter()
		.map(|object| (object.uri.clone(), object.size_bytes))
		.collect::<Vec<_>>();
	seen.sort();
	let mut wanted = vec![
		(small_uri.clone(), small.len() as u64),
		(large_uri.clone(), large.len() as u64),
	];
	wanted.sort();
	assert_eq!(seen, wanted);

	let missing = small_uri.replace("small.bin", "absent.bin");
	assert_eq!(
		store.head(&missing).await.unwrap_err().kind(),
		ErrorKind::NotFound
	);
	assert_eq!(
		store.get(&missing).await.unwrap_err().kind(),
		ErrorKind::NotFound
	);

	// Only what this test wrote, under its own random prefix.
	store.delete(&small_uri).await.unwrap();
	store.delete(&large_uri).await.unwrap();
	assert_eq!(
		store.head(&large_uri).await.unwrap_err().kind(),
		ErrorKind::NotFound
	);
	assert!(store.list("check/").await.unwrap().is_empty());
}

#[tokio::test]
#[ignore = "external: needs a running RustFS (support/rustfs_env.rs); run with --ignored"]
async fn rustfs_refuses_a_wrong_secret_and_nothing_is_written() {
	let good = rustfs_env::remote_config("adapter-auth");
	let mut bad = rustfs_env::remote_config_with_secret("unused", "not-the-secret".to_owned());
	bad.root = good.root.clone();
	let bad = RemoteObjectStore::new(bad).unwrap();
	let error = bad.put("denied.bin", b"x").await.unwrap_err();
	assert_eq!(error.kind(), ErrorKind::PermissionDenied, "{error}");
	let good = RemoteObjectStore::new(good).unwrap();
	assert!(
		good.list("").await.unwrap().is_empty(),
		"a refused put left an object"
	);
}
