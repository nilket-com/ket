//! `[storage.object_store]` and `[storage.tiering]` in the daemon's configuration file: what
//! loads, what is refused at load, and that no message or debug rendering holds a key. Nothing
//! here contacts a store, except the one `#[ignore]`d test at the end (see
//! `support/rustfs_env.rs`), which shows that a loaded file builds a store RustFS accepts. That
//! is the mapping, not a daemon tiering.

#[path = "support/rustfs_env.rs"]
mod rustfs_env;
#[path = "../src/test_support/test_dir.rs"]
mod test_dir;

use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use std::time::Duration;
use wendelon::config::{load_node_bootstrap_config, render_example_config};
use wendelon::crypto::DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV;
use wendelon::storage::{
	BackblazeB2ProviderConfig, GoogleCloudStorageProviderConfig, ObjectStoreConfig,
	RemoteObjectStoreConfig, RemoteObjectStoreProvider, S3CompatibleProviderConfig, SweeperConfig,
};

const ACCESS: &str = "AKIDEXAMPLEONLY";
const SECRET: &str = "s3cr3t-never-in-a-message";

/// What every parse failure says after its position; nothing from the file.
const PARSE_REFUSAL: &str = "a key there is unknown, missing or of the wrong type";

const SECTION: &str = r#"
[storage.object_store]
provider = "s3-compatible"
endpoint = "http://10.0.0.6:9000"
bucket = "ket-prod"
region = "us-east-1"
prefix = "cluster-a"
addressing = "path"
access_key_id_file = "object-store.access-key"
secret_access_key_file = "object-store.secret-key"
"#;

/// A directory with a data dir, both key files, and the example configuration plus `extra`.
fn config_with(tag: &str, extra: &str) -> (test_dir::TestDir, PathBuf) {
	let root = test_dir::TestDir::new(&format!("wendelon-config-object-store-{tag}"));
	std::fs::create_dir_all(root.join("data")).unwrap();
	std::fs::write(root.join("object-store.access-key"), format!("{ACCESS}\n")).unwrap();
	std::fs::write(root.join("object-store.secret-key"), format!("{SECRET}\n")).unwrap();
	unsafe {
		std::env::set_var(
			DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV,
			"config-object-store-test-local-sealed-secret",
		);
	}
	let path = root.join("wendelon.toml");
	std::fs::write(&path, format!("{}\n{extra}", render_example_config())).unwrap();
	(root, path)
}

fn refused(tag: &str, extra: &str, kind: ErrorKind, names: &str) {
	let (_root, path) = config_with(tag, extra);
	let error = load_node_bootstrap_config(&path).unwrap_err();
	let message = error.to_string();
	assert_eq!(error.kind(), kind, "{tag}: {message}");
	assert!(message.contains(names), "{tag}: {message}");
	for held in [ACCESS, SECRET, "inline-secret", "user:pw", "sentinel"] {
		assert!(!message.contains(held), "{tag}: the message holds {held}");
	}
}

fn remote(cfg: &ObjectStoreConfig) -> (&RemoteObjectStoreConfig, &S3CompatibleProviderConfig) {
	let ObjectStoreConfig::Remote(remote) = cfg else {
		panic!("not a remote store: {cfg:?}");
	};
	let RemoteObjectStoreProvider::S3Compatible(s3) = &remote.provider else {
		panic!("not S3-compatible: {remote:?}");
	};
	(remote, s3)
}

#[test]
fn without_the_section_there_is_no_object_store_and_tiering_is_the_default() {
	let (_root, path) = config_with("absent", "");
	let cfg = load_node_bootstrap_config(&path).unwrap();
	assert_eq!(cfg.l2, None);
	let default = SweeperConfig::default();
	assert_eq!(cfg.tiering.scan_interval, default.scan_interval);
	assert_eq!(cfg.tiering.min_slab_bytes, default.min_slab_bytes);
}

#[test]
fn the_section_maps_to_the_remote_s3_compatible_store() {
	let (_root, path) = config_with("present", SECTION);
	let cfg = load_node_bootstrap_config(&path).unwrap();
	let (store, s3) = remote(cfg.l2.as_ref().unwrap());
	assert_eq!(store.bucket, "ket-prod");
	assert_eq!(store.root, "cluster-a");
	assert_eq!(s3.endpoint, "http://10.0.0.6:9000");
	assert_eq!(s3.region, "us-east-1");
	assert_eq!(s3.access_key_id.as_deref(), Some(ACCESS));
	assert_eq!(s3.secret_access_key.as_deref(), Some(SECRET));
	assert_eq!(s3.session_token, None);
	assert!(!s3.allow_anonymous);
	assert!(!s3.virtual_host_style);
	assert_eq!(cfg.tiering.scan_interval, Duration::from_secs(5));
	assert_eq!(cfg.tiering.min_slab_bytes, 0);
	// The node's own validation builds the store from it, still without contacting anything.
	cfg.l2.as_ref().unwrap().build().unwrap();

	let virtual_host = SECTION.replace("addressing = \"path\"", "addressing = \"virtual-host\"");
	let (_root, path) = config_with("virtual-host", &virtual_host);
	let cfg = load_node_bootstrap_config(&path).unwrap();
	assert!(remote(cfg.l2.as_ref().unwrap()).1.virtual_host_style);

	let bare = SECTION
		.replace("prefix = \"cluster-a\"\n", "")
		.replace("http://10.0.0.6:9000", "https://objects.example.net/");
	let (_root, path) = config_with("no-prefix-https", &bare);
	let cfg = load_node_bootstrap_config(&path).unwrap();
	let (store, s3) = remote(cfg.l2.as_ref().unwrap());
	assert_eq!(store.root, "");
	assert_eq!(s3.endpoint, "https://objects.example.net");
}

#[test]
fn tiering_values_reach_the_sweeper_and_need_an_object_store() {
	let tiering = "\n[storage.tiering]\nscan_interval_millis = 250\nmin_slab_bytes = 4096\n";
	let (_root, path) = config_with("tiering", &format!("{SECTION}{tiering}"));
	let cfg = load_node_bootstrap_config(&path).unwrap();
	assert_eq!(cfg.tiering.scan_interval, Duration::from_millis(250));
	assert_eq!(cfg.tiering.min_slab_bytes, 4096);

	refused(
		"tiering-alone",
		tiering,
		ErrorKind::InvalidInput,
		"storage.tiering",
	);
	refused(
		"tiering-zero",
		&format!("{SECTION}\n[storage.tiering]\nscan_interval_millis = 0\n"),
		ErrorKind::InvalidInput,
		"scan_interval_millis",
	);
	refused(
		"tiering-unknown",
		&format!("{SECTION}\n[storage.tiering]\nscan_interval = 5\n"),
		ErrorKind::InvalidData,
		PARSE_REFUSAL,
	);
}

#[test]
fn a_wrong_section_is_refused_at_load_by_field() {
	let with = |from: &str, to: &str| {
		assert!(SECTION.contains(from), "{from}");
		SECTION.replace(from, to)
	};
	let invalid = ErrorKind::InvalidInput;
	let cases = [
		("provider", with("\"s3-compatible\"", "\"b2\""), "provider"),
		(
			"endpoint-relative",
			with("http://10.0.0.6:9000", "10.0.0.6:9000"),
			"endpoint",
		),
		(
			"endpoint-not-a-url",
			with("http://10.0.0.6:9000", "http://"),
			"endpoint",
		),
		(
			"endpoint-bad-host",
			with("http://10.0.0.6:9000", "http://bad host:9000"),
			"endpoint",
		),
		(
			"endpoint-scheme",
			with("http://10.0.0.6:9000", "s3://10.0.0.6:9000"),
			"endpoint",
		),
		(
			"endpoint-userinfo",
			with("http://10.0.0.6:9000", "http://user:pw@10.0.0.6:9000"),
			"endpoint",
		),
		(
			"endpoint-query",
			with(
				"http://10.0.0.6:9000",
				"http://10.0.0.6:9000/?x=inline-secret",
			),
			"endpoint",
		),
		(
			"endpoint-fragment",
			with(
				"http://10.0.0.6:9000",
				"http://10.0.0.6:9000/#inline-secret",
			),
			"endpoint",
		),
		(
			"endpoint-path",
			with("http://10.0.0.6:9000", "http://10.0.0.6:9000/tenant"),
			"endpoint",
		),
		("bucket-empty", with("\"ket-prod\"", "\"\""), "bucket"),
		("region-empty", with("\"us-east-1\"", "\"\""), "region"),
		(
			"addressing",
			with("addressing = \"path\"", "addressing = \"auto\""),
			"addressing",
		),
		(
			"prefix-parent",
			with("\"cluster-a\"", "\"a/../b\""),
			"prefix",
		),
		("prefix-absolute", with("\"cluster-a\"", "\"/a\""), "prefix"),
	];
	for (tag, section, field) in &cases {
		refused(
			tag,
			section,
			invalid,
			&format!("storage.object_store.{field}"),
		);
	}
	// A required field left out, a misspelled field, a misspelled section, an inline secret.
	let parse = ErrorKind::InvalidData;
	refused(
		"no-addressing",
		&with("addressing = \"path\"\n", ""),
		parse,
		"absent: storage.object_store.addressing",
	);
	refused(
		"no-secret-file",
		&with("secret_access_key_file = \"object-store.secret-key\"\n", ""),
		parse,
		"absent: storage.object_store.secret_access_key_file",
	);
	refused(
		"unknown-field",
		&format!("{SECTION}anonymous = true\n"),
		parse,
		PARSE_REFUSAL,
	);
	refused(
		"inline-key",
		&format!("{SECTION}secret_access_key = \"inline-secret\"\n"),
		parse,
		PARSE_REFUSAL,
	);
	refused(
		"misspelled-section",
		&SECTION.replace("[storage.object_store]", "[storage.object_stor]"),
		parse,
		PARSE_REFUSAL,
	);
}

#[test]
fn a_parse_failure_gives_its_position_and_nothing_the_file_holds() {
	let parse = ErrorKind::InvalidData;
	// A string where a number belongs: the parser's own message would quote it.
	refused(
		"wrong-type",
		&format!("{SECTION}\n[storage.tiering]\nscan_interval_millis = \"sentinel-value\"\n"),
		parse,
		PARSE_REFUSAL,
	);
	// An unknown key: the parser's own message would name it.
	refused(
		"unknown-key",
		&format!("{SECTION}sentinel_key = 1\n"),
		parse,
		PARSE_REFUSAL,
	);
	// The position is the offending key's.
	let extra = format!("{SECTION}sentinel_key = 1\n");
	let (_root, path) = config_with("position", &extra);
	let text = std::fs::read_to_string(&path).unwrap();
	let line =
		text.lines()
			.position(|line| line.starts_with("sentinel_key"))
			.unwrap() + 1;
	let message = load_node_bootstrap_config(&path).unwrap_err().to_string();
	assert!(
		message.contains(&format!("at line {line}, column 1:")),
		"{message}"
	);
}

#[test]
fn key_files_are_read_by_path_and_refused_without_showing_what_they_hold() {
	let missing = SECTION.replace("object-store.secret-key", "absent.secret-key");
	refused(
		"key-missing",
		&missing,
		ErrorKind::NotFound,
		"secret_access_key_file",
	);

	for (tag, content) in [
		("key-empty", "\n"),
		("key-two-words", "s3cr3t-never-in-a-message extra\n"),
	] {
		let (root, path) = config_with(tag, SECTION);
		std::fs::write(root.join("object-store.secret-key"), content).unwrap();
		let error = load_node_bootstrap_config(&path).unwrap_err();
		let message = error.to_string();
		assert_eq!(error.kind(), ErrorKind::InvalidInput, "{message}");
		assert!(message.contains("secret_access_key_file"), "{message}");
		assert!(
			!message.contains(SECRET) && !message.contains("extra"),
			"{message}"
		);
	}

	// An absolute path is used as written.
	let (root, _) = config_with("key-absolute-holder", "");
	let absolute: &Path = &root.join("object-store.secret-key");
	let section = SECTION.replace("object-store.secret-key", absolute.to_str().unwrap());
	let (_other, path) = config_with("key-absolute", &section);
	let cfg = load_node_bootstrap_config(&path).unwrap();
	assert_eq!(
		remote(cfg.l2.as_ref().unwrap())
			.1
			.secret_access_key
			.as_deref(),
		Some(SECRET)
	);
}

#[test]
fn no_debug_rendering_holds_a_credential() {
	let (_root, path) = config_with("debug", SECTION);
	let cfg = load_node_bootstrap_config(&path).unwrap();
	for rendered in [
		format!("{cfg:?}"),
		format!("{:?}", cfg.l2),
		format!("{:#?}", cfg.l2),
	] {
		assert!(
			!rendered.contains(ACCESS) && !rendered.contains(SECRET),
			"{rendered}"
		);
		assert!(rendered.contains("<redacted>") && rendered.contains("10.0.0.6"));
	}
	let s3 = S3CompatibleProviderConfig {
		endpoint: "http://h".into(),
		region: "r".into(),
		access_key_id: Some("hidden-1".into()),
		secret_access_key: Some("hidden-2".into()),
		session_token: Some("hidden-3".into()),
		allow_anonymous: false,
		virtual_host_style: false,
	};
	let b2 = BackblazeB2ProviderConfig {
		bucket_id: "bucket-id".into(),
		application_key_id: "hidden-4".into(),
		application_key: "hidden-5".into(),
	};
	let gcs = GoogleCloudStorageProviderConfig {
		endpoint: None,
		credential: Some("hidden-6".into()),
		credential_path: Some("hidden-7".into()),
		service_account: Some("hidden-8".into()),
		token: Some("hidden-9".into()),
		allow_anonymous: false,
	};
	let nested = [
		RemoteObjectStoreProvider::S3Compatible(s3),
		RemoteObjectStoreProvider::BackblazeB2(b2),
		RemoteObjectStoreProvider::GoogleCloudStorage(gcs),
	]
	.map(|provider| {
		ObjectStoreConfig::Remote(RemoteObjectStoreConfig {
			bucket: "b".into(),
			root: String::new(),
			provider,
		})
	});
	let rendered = format!("{nested:?} {nested:#?}");
	assert!(!rendered.contains("hidden-"), "{rendered}");
	assert!(rendered.contains("bucket-id") && rendered.contains("<redacted>"));
}

#[tokio::test]
#[ignore = "external: needs a running RustFS (support/rustfs_env.rs); run with --ignored"]
async fn a_loaded_file_builds_a_store_rustfs_accepts() {
	let wanted = rustfs_env::remote_config("config-object-store");
	let RemoteObjectStoreProvider::S3Compatible(s3) = &wanted.provider else {
		unreachable!()
	};
	let section = format!(
		"\n[storage.object_store]\nprovider = \"s3-compatible\"\nendpoint = \"{}\"\nbucket = \"{}\"\nregion = \"{}\"\nprefix = \"{}\"\naddressing = \"path\"\naccess_key_id_file = \"{}\"\nsecret_access_key_file = \"{}\"\n",
		s3.endpoint,
		wanted.bucket,
		s3.region,
		wanted.root,
		std::env::var("WENDELON_RUSTFS_ACCESS_KEY_FILE").unwrap(),
		std::env::var("WENDELON_RUSTFS_SECRET_KEY_FILE").unwrap(),
	);
	let (_root, path) = config_with("rustfs", &section);
	let cfg = load_node_bootstrap_config(&path).unwrap();
	cfg.validate().unwrap();
	let store = cfg.l2.as_ref().unwrap().build().unwrap();
	let uri = store
		.put("from-config.bin", b"loaded from wendelon.toml")
		.await
		.unwrap();
	assert_eq!(store.get(&uri).await.unwrap(), b"loaded from wendelon.toml");
	store.delete(&uri).await.unwrap();
}
