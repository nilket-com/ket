//! Where the external RustFS tests find their server. They are `#[ignore]`d, so a default run
//! does not count as remote coverage; run explicitly with `--ignored`, they fail (never skip)
//! when the configuration is missing or the endpoint does not answer.
//!
//! `WENDELON_RUSTFS_ENDPOINT` (for example `http://127.0.0.1:9000`), `WENDELON_RUSTFS_BUCKET`,
//! `WENDELON_RUSTFS_ACCESS_KEY_FILE` and `WENDELON_RUSTFS_SECRET_KEY_FILE` (paths, so no secret is in the
//! environment or a command line). Optional `WENDELON_RUSTFS_REGION`, default `us-east-1`.

use wendelon::storage::{
	RemoteObjectStoreConfig, RemoteObjectStoreProvider, S3CompatibleProviderConfig,
};

fn required(name: &str) -> String {
	match std::env::var(name) {
		Ok(value) if !value.trim().is_empty() => value,
		_ => panic!("{name} is not set: this test needs a running RustFS and does not skip"),
	}
}

fn secret_file(name: &str) -> String {
	let path = required(name);
	let value = std::fs::read_to_string(&path)
		.unwrap_or_else(|error| panic!("{name}: cannot read {path}: {error}"));
	let value = value.trim().to_owned();
	assert!(!value.is_empty(), "{name}: {path} is empty");
	value
}

/// A remote config for this run's own prefix `<area>/<random>` in the configured bucket.
pub fn remote_config(area: &str) -> RemoteObjectStoreConfig {
	remote_config_with_secret(area, secret_file("WENDELON_RUSTFS_SECRET_KEY_FILE"))
}

pub fn remote_config_with_secret(area: &str, secret_access_key: String) -> RemoteObjectStoreConfig {
	RemoteObjectStoreConfig {
		bucket: required("WENDELON_RUSTFS_BUCKET"),
		root: format!("{area}/{}", uuid::Uuid::new_v4().simple()),
		provider: RemoteObjectStoreProvider::S3Compatible(S3CompatibleProviderConfig {
			endpoint: required("WENDELON_RUSTFS_ENDPOINT"),
			region: std::env::var("WENDELON_RUSTFS_REGION")
				.unwrap_or_else(|_| "us-east-1".to_owned()),
			access_key_id: Some(secret_file("WENDELON_RUSTFS_ACCESS_KEY_FILE")),
			secret_access_key: Some(secret_access_key),
			session_token: None,
			allow_anonymous: false,
			virtual_host_style: false,
		}),
	}
}
