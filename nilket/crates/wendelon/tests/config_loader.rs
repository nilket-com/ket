#[path = "../src/test_support/test_dir.rs"]
mod test_dir;
use std::io::ErrorKind;
use std::path::{Path, PathBuf};
use wendelon::config::{load_node_bootstrap_config, render_example_config, write_example_config};
use wendelon::crypto::{
	DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV, DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES,
	DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES, SLAB_MAX_BYTES_HARD_CEILING,
};
use wendelon::network::{
	audit_root_verifying_key_bytes, dev_audit_root_signing_secret, dev_lease_signing_secret,
	dev_transfer_signing_secret, lease_verifying_key_bytes, transfer_verifying_key_bytes,
};
use wendelon::node::{ClusterBootstrapConfig, WendelonNode};
use wendelon::storage::{
	ROW_GROUP_SCAN_MAX_ROWS, ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES,
	ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS, ROW_GROUP_SCAN_TARGET_ROWS, RowGroupSizingProfile,
};

fn fresh_dir(tag: &str) -> test_dir::TestDir {
	test_dir::TestDir::new(&format!("wendelon-config-loader-{tag}"))
}

fn hex(bytes: [u8; 32]) -> String {
	let mut out = String::with_capacity(64);
	for byte in bytes {
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02x}").unwrap();
	}
	out
}

fn write_config(dir: &Path, body: &str) -> PathBuf {
	unsafe {
		std::env::set_var(
			DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV,
			"config-loader-test-local-sealed-secret",
		);
	}
	let path = dir.join("wendelon.toml");
	std::fs::write(&path, body).unwrap();
	path
}

fn write_config_with_row_group_writer(
	root: &Path,
	node_id: u32,
	row_group_writer: &str,
) -> PathBuf {
	std::fs::create_dir_all(root.join("data")).unwrap();
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	write_config(
		root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4715"

[storage]
data_dir = "data"

[storage.row_group_writer]
{row_group_writer}

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			row_group_writer = row_group_writer,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	)
}

#[test]
fn loads_valid_wendelon_toml() {
	let root = fresh_dir("valid");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let node_id = 7u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4701"

[storage]
data_dir = "data"
slab_max_bytes = 123456
lsm_compaction_paused = true
mvcc_retention_paused = true

[tuning]
replication_factor = 3
wal_io_depth = 128
slab_io_depth = 128
cdc_locality_heartbeat_interval_millis = 2500
data_ring_lease_ttl_millis = 9000

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.node_id, node_id);
	assert_eq!(cfg.data_dir, data_dir);
	assert_eq!(cfg.slab_max_bytes, 123456);
	assert!(cfg.lsm_compaction_paused);
	assert!(cfg.mvcc_retention_paused);
	assert_eq!(cfg.rf, 3);
	assert_eq!(cfg.wal_io_depth, 128);
	assert_eq!(cfg.slab_io_depth, 128);
	assert_eq!(
		cfg.cdc_locality_heartbeat_interval,
		std::time::Duration::from_millis(2500)
	);
	assert_eq!(
		cfg.data_ring_lease_ttl,
		std::time::Duration::from_millis(9000)
	);
	assert_eq!(cfg.advertise_address, "127.0.0.1:4701");
	assert_eq!(cfg.lease_signing_secret, lease_secret);
	assert_eq!(
		cfg.trusted_lease_signers.get(&node_id).copied(),
		Some(lease_verifying_key_bytes(lease_secret))
	);
	assert_eq!(cfg.transfer_signing_secret, transfer_secret);
	assert_eq!(
		cfg.trusted_transfer_signers.get(&node_id).copied(),
		Some(transfer_verifying_key_bytes(transfer_secret))
	);
	assert!(cfg.l2.is_none());
	assert_eq!(cfg.drift_max_offset_ms, wendelon::consensus::MAX_OFFSET_MS);
	assert_eq!(
		cfg.mvcc_retention_policy,
		wendelon::storage::MvccRetentionPolicy::FixedWindow(
			wendelon::node::DEFAULT_MVCC_RETENTION_WINDOW
		)
	);
	assert_eq!(
		cfg.crypto_cache.tenant_kek_cache_max_entries,
		DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES
	);
	assert_eq!(
		cfg.crypto_cache.slab_dek_cache_max_entries,
		DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES
	);
	match &cfg.root_kek_provider {
		wendelon::crypto::RootKekProviderConfig::LocalSealed(local) => {
			assert_eq!(
				local.unlock_secret_env.as_deref(),
				Some(DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV)
			);
			assert!(local.unlock_secret_file.is_none());
		}
		other => panic!("expected local-sealed root_kek_provider default, got {other:?}"),
	}
}

#[test]
fn obsolete_rf_tuning_key_is_rejected_instead_of_silently_defaulting() {
	let root = fresh_dir("obsolete-rf-key");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let body = render_example_config().replace("replication_factor = 1", "rf = 3");
	let config_path = write_config(&root, &body);
	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	// A parse failure names the place, not the key or its value (ADR 0718).
	let line =
		body.lines()
			.position(|line| line.starts_with("rf = 3"))
			.unwrap() + 1;
	let message = err.to_string();
	assert_eq!(err.kind(), ErrorKind::InvalidData);
	assert!(
		message.contains(&format!("at line {line}, column 1:")),
		"unexpected error: {err}"
	);
}

#[test]
fn loads_split_io_depth_tuning() {
	let root = fresh_dir("split-io-depth");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 43u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4713"

[storage]
data_dir = "data"

[tuning]
wal_io_depth = 32
slab_io_depth = 256

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.wal_io_depth, 32);
	assert_eq!(cfg.slab_io_depth, 256);
	assert_eq!(
		cfg.cdc_locality_heartbeat_interval,
		std::time::Duration::from_millis(
			wendelon::storage::DEFAULT_CDC_LOCALITY_HEARTBEAT_INTERVAL_MILLIS
		)
	);
	assert_eq!(
		cfg.data_ring_lease_ttl,
		std::time::Duration::from_millis(u64::from(
			wendelon::node::DEFAULT_DATA_RING_LEASE_TTL_MILLIS
		))
	);
}

#[test]
fn zero_cdc_locality_heartbeat_interval_is_rejected() {
	let root = fresh_dir("zero-cdc-heartbeat");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let body = render_example_config().replace(
		"cdc_locality_heartbeat_interval_millis = 1000",
		"cdc_locality_heartbeat_interval_millis = 0",
	);
	let config_path = write_config(&root, &body);
	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	assert!(
		err.to_string()
			.contains("tuning.cdc_locality_heartbeat_interval_millis must be greater than zero"),
		"unexpected error: {err}"
	);
}

#[test]
fn serious_example_keeps_the_one_second_cdc_heartbeat_default() {
	let rendered = render_example_config();
	assert!(rendered.contains("cdc_locality_heartbeat_interval_millis = 1000"));
	assert!(rendered.contains("data_ring_lease_ttl_millis = 3000"));
}

#[test]
fn invalid_data_ring_lease_ttls_are_rejected() {
	for invalid in [0u64, 2, u64::from(u32::MAX) + 1] {
		let root = fresh_dir(&format!("invalid-lease-ttl-{invalid}"));
		std::fs::create_dir_all(root.join("data")).unwrap();
		let body = render_example_config().replace(
			"data_ring_lease_ttl_millis = 3000",
			&format!("data_ring_lease_ttl_millis = {invalid}"),
		);
		let config_path = write_config(&root, &body);
		let err = load_node_bootstrap_config(&config_path).unwrap_err();
		assert!(
			err.to_string()
				.contains("tuning.data_ring_lease_ttl_millis must be between 3 and 4294967295"),
			"unexpected error for {invalid}: {err}"
		);
	}
}

#[test]
fn loads_row_group_tuning() {
	let root = fresh_dir("row-group-tuning");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 45u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4715"

[storage]
data_dir = "data"

[storage.row_group_writer]
enabled = true
target_row_group_rows = 2048
max_row_group_rows = 4096
min_paged_record_rows = 4096
min_paged_record_bytes = 131072

[storage.l2_row_group_fetch]
enabled = true
max_selected_row_group_fraction_micros = 200000
max_selected_byte_fraction_micros = 400000
max_ranges_per_record = 12
max_parallel_range_gets = 3

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert!(cfg.row_group_writer.enabled);
	assert_eq!(
		cfg.row_group_writer.profile,
		RowGroupSizingProfile::PointRead
	);
	assert_eq!(cfg.row_group_writer.target_row_group_rows().get(), 2048);
	assert_eq!(cfg.row_group_writer.max_row_group_rows().get(), 4096);
	assert_eq!(cfg.row_group_writer.min_paged_record_rows().get(), 4096);
	assert_eq!(cfg.row_group_writer.min_paged_record_bytes().get(), 131072);
	assert!(cfg.l2_row_group_fetch.enabled);
	assert_eq!(
		cfg.l2_row_group_fetch
			.max_selected_row_group_fraction_micros,
		200000
	);
	assert_eq!(
		cfg.l2_row_group_fetch.max_selected_byte_fraction_micros,
		400000
	);
	assert_eq!(cfg.l2_row_group_fetch.max_ranges_per_record.get(), 12);
	assert_eq!(cfg.l2_row_group_fetch.max_parallel_range_gets.get(), 3);
}

#[test]
fn loads_scan_row_group_profile() {
	let root = fresh_dir("scan-row-group-profile");
	let config_path = write_config_with_row_group_writer(
		&root,
		145,
		r#"enabled = true
profile = "scan""#,
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert!(cfg.row_group_writer.enabled);
	assert_eq!(cfg.row_group_writer.profile, RowGroupSizingProfile::Scan);
	assert_eq!(
		cfg.row_group_writer.target_row_group_rows().get(),
		ROW_GROUP_SCAN_TARGET_ROWS
	);
	assert_eq!(
		cfg.row_group_writer.max_row_group_rows().get(),
		ROW_GROUP_SCAN_MAX_ROWS
	);
	assert_eq!(
		cfg.row_group_writer.min_paged_record_rows().get(),
		ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS
	);
	assert_eq!(
		cfg.row_group_writer.min_paged_record_bytes().get(),
		ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES
	);
}

#[test]
fn scan_row_group_profile_ignores_numeric_overrides() {
	let root = fresh_dir("scan-row-group-profile-numeric-overrides");
	let config_path = write_config_with_row_group_writer(
		&root,
		146,
		r#"enabled = true
profile = "scan"
target_row_group_rows = 2048
max_row_group_rows = 4096
min_paged_record_rows = 4096
min_paged_record_bytes = 131072"#,
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.row_group_writer.profile, RowGroupSizingProfile::Scan);
	assert_eq!(
		cfg.row_group_writer.target_row_group_rows().get(),
		ROW_GROUP_SCAN_TARGET_ROWS
	);
	assert_eq!(
		cfg.row_group_writer.max_row_group_rows().get(),
		ROW_GROUP_SCAN_MAX_ROWS
	);
	assert_eq!(
		cfg.row_group_writer.min_paged_record_rows().get(),
		ROW_GROUP_SCAN_MIN_PAGED_RECORD_ROWS
	);
	assert_eq!(
		cfg.row_group_writer.min_paged_record_bytes().get(),
		ROW_GROUP_SCAN_MIN_PAGED_RECORD_BYTES
	);
}

#[test]
fn rejects_unknown_row_group_profile() {
	let root = fresh_dir("unknown-row-group-profile");
	let config_path = write_config_with_row_group_writer(
		&root,
		147,
		r#"enabled = true
profile = "bulk""#,
	);

	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	assert!(
		err.to_string()
			.contains("unknown storage.row_group_writer profile")
	);
}

#[test]
fn loads_encoding_tuning() {
	let root = fresh_dir("encoding-tuning");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 46u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4716"

[storage]
data_dir = "data"

[storage.encoding_writer]
enabled = true
target_simd_page_values = 512
cascade_sample_rows = 128
min_buffer_bytes_for_encoding = 2048
enabled_cascades = ["plain", "bit_packed", "rle"]

[storage.l2_encoding_fetch]
enabled = true
max_simd_pages_per_record = 17
max_parallel_materialize_tasks = 5

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert!(cfg.encoding_writer.enabled);
	assert_eq!(cfg.encoding_writer.target_simd_page_values.get(), 512);
	assert_eq!(cfg.encoding_writer.cascade_sample_rows.get(), 128);
	assert_eq!(
		cfg.encoding_writer.min_buffer_bytes_for_encoding.get(),
		2048
	);
	assert_eq!(cfg.encoding_writer.enabled_cascades.len(), 3);
	assert!(cfg.l2_encoding_fetch.enabled);
	assert_eq!(cfg.l2_encoding_fetch.max_simd_pages_per_record.get(), 17);
	assert_eq!(
		cfg.l2_encoding_fetch.max_parallel_materialize_tasks.get(),
		5
	);
}

#[test]
fn loads_transport_admission_tuning() {
	let root = fresh_dir("transport-admission");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 44u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4714"

[storage]
data_dir = "data"

[tuning]
max_inbound_connections = 12
max_inbound_connections_per_ip = 3
max_inbound_streams_per_connection = 7
max_concurrent_stream_tasks = 33

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.transport_admission.max_inbound_connections, 12);
	assert_eq!(cfg.transport_admission.max_inbound_connections_per_ip, 3);
	assert_eq!(
		cfg.transport_admission.max_inbound_streams_per_connection,
		7
	);
	assert_eq!(cfg.transport_admission.max_concurrent_stream_tasks, 33);
}

#[test]
fn loads_crypto_cache_tuning() {
	let root = fresh_dir("crypto-cache-tuning");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 41u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4711"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[crypto]
tenant_kek_cache_max_entries = 17
slab_dek_cache_max_entries = 91
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.crypto_cache.tenant_kek_cache_max_entries, 17);
	assert_eq!(cfg.crypto_cache.slab_dek_cache_max_entries, 91);
}

#[test]
fn loads_local_sealed_root_kek_provider_from_secret_file() {
	let root = fresh_dir("local-sealed-provider");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let secret_path = root.join("root-kek.secret");
	std::fs::write(&secret_path, b"correct horse battery staple").unwrap();

	let node_id = 45u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4715"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[crypto]
root_kek_provider = {{ kind = "local-sealed", unlock_secret_file = "root-kek.secret" }}
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	match &cfg.root_kek_provider {
		wendelon::crypto::RootKekProviderConfig::LocalSealed(local) => {
			assert!(local.unlock_secret_env.is_none());
			assert_eq!(
				local.unlock_secret_file.as_deref(),
				Some(secret_path.as_path())
			);
		}
		other => panic!("expected local-sealed root_kek_provider, got {other:?}"),
	}
}

#[test]
fn loads_aws_kms_root_kek_provider_from_config() {
	let root = fresh_dir("aws-kms-provider");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 46u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4716"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[crypto]
root_kek_provider = {{ kind = "aws-kms", region = "us-east-1", key_id = "arn:aws:kms:us-east-1:111122223333:key/test", endpoint = "http://127.0.0.1:4566", access_key_id_env = "WENDELON_TEST_AWS_ACCESS_KEY_ID", secret_access_key_env = "WENDELON_TEST_AWS_SECRET_ACCESS_KEY", session_token_env = "WENDELON_TEST_AWS_SESSION_TOKEN" }}
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	match &cfg.root_kek_provider {
		wendelon::crypto::RootKekProviderConfig::AwsKms(aws) => {
			assert_eq!(aws.region, "us-east-1");
			assert_eq!(aws.key_id, "arn:aws:kms:us-east-1:111122223333:key/test");
			assert_eq!(aws.endpoint.as_deref(), Some("http://127.0.0.1:4566"));
			assert_eq!(aws.access_key_id_env, "WENDELON_TEST_AWS_ACCESS_KEY_ID");
			assert_eq!(
				aws.secret_access_key_env,
				"WENDELON_TEST_AWS_SECRET_ACCESS_KEY"
			);
			assert_eq!(aws.session_token_env, "WENDELON_TEST_AWS_SESSION_TOKEN");
		}
		other => panic!("expected aws-kms root_kek_provider, got {other:?}"),
	}
}

#[test]
fn loads_pkcs11_root_kek_provider_from_config() {
	let root = fresh_dir("pkcs11-provider");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let module_path = root.join("libpkcs11-test.so");
	std::fs::write(&module_path, b"not-a-real-pkcs11-module").unwrap();
	let pin_env = format!("WENDELON_TEST_PKCS11_PIN_{}", uuid::Uuid::new_v4());
	unsafe {
		std::env::set_var(&pin_env, "123456");
	}

	let node_id = 47u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4717"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[crypto]
root_kek_provider = {{ kind = "pkcs11", library_path = "libpkcs11-test.so", token_label = "WendelonToken", key_label = "root-kek", pin_env = "{pin_env}", slot = 7 }}
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
			pin_env = pin_env,
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	match &cfg.root_kek_provider {
		wendelon::crypto::RootKekProviderConfig::Pkcs11(pkcs11) => {
			assert_eq!(pkcs11.library_path, module_path);
			assert_eq!(pkcs11.token_label, "WendelonToken");
			assert_eq!(pkcs11.key_label, "root-kek");
			assert_eq!(pkcs11.pin_env, pin_env);
			assert_eq!(pkcs11.slot, Some(7));
		}
		other => panic!("expected pkcs11 root_kek_provider, got {other:?}"),
	}

	unsafe {
		std::env::remove_var(&pin_env);
	}
}

#[test]
fn rejects_zero_crypto_cache_tuning() {
	let root = fresh_dir("crypto-cache-zero");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 42u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4712"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[crypto]
tenant_kek_cache_max_entries = 0
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	assert!(
		err.to_string()
			.contains("crypto.tenant_kek_cache_max_entries must be greater than zero"),
		"unexpected error: {err}"
	);
}

#[test]
fn rejects_missing_required_configuration() {
	let root = fresh_dir("missing");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let config_path = write_config(
		&root,
		r#"
[node]
node_id = 9
advertise_address = "127.0.0.1:4702"

[storage]
data_dir = "data"
"#,
	);

	let err = load_node_bootstrap_config(&config_path).err().unwrap();
	assert!(
		err.to_string().contains("identity"),
		"unexpected error: {err}"
	);
}

#[test]
fn rejects_invalid_key_material() {
	let root = fresh_dir("invalid");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let config_path = write_config(
		&root,
		r#"
[node]
node_id = 11
advertise_address = "127.0.0.1:4703"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "not-hex"
transfer_signing_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
audit_root_signing_secret = "9d61b19deffd5a60ba844af492ec2cc44449c5697b326919703bac031cae7f60"

[identity.trusted_lease_signers]
"11" = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[identity.trusted_transfer_signers]
"11" = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[identity.trusted_audit_root_keys]
"11" = "d75a980182b10ab7d54bfed3c964073a0ee172f3daa62325af021a68f707511a"
"#,
	);

	let err = load_node_bootstrap_config(&config_path).err().unwrap();
	assert!(
		err.to_string().contains("lease_signing_secret"),
		"unexpected error: {err}"
	);
}

#[test]
fn rejects_slab_max_bytes_above_w4_ceiling() {
	let root = fresh_dir("slab-ceiling");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let node_id = 12u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4704"

[storage]
data_dir = "data"
slab_max_bytes = {too_large}

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			too_large = SLAB_MAX_BYTES_HARD_CEILING + 1,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	assert!(
		err.to_string().contains("hard ceiling"),
		"unexpected error: {err}"
	);
}

#[test]
fn loads_join_cluster_configuration() {
	let root = fresh_dir("join-config");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let node_id = 21u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "loopback://node-{node_id}"

[cluster]
mode = "join"

[[cluster.seed_nodes]]
node_id = 1
address = "loopback://node-1"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	match cfg.cluster {
		ClusterBootstrapConfig::Join { seeds } => {
			assert_eq!(seeds.len(), 1);
			assert_eq!(seeds[0].node_id, 1);
			assert_eq!(seeds[0].address, "loopback://node-1");
		}
		other => panic!("expected join cluster config, got {other:?}"),
	}
}

#[test]
fn loads_transport_tls_and_observability_configuration() {
	let root = fresh_dir("transport-observability");
	let data_dir = root.join("data");
	let pki_dir = root.join("pki");
	let syslog_dir = root.join("run");
	std::fs::create_dir_all(&data_dir).unwrap();
	std::fs::create_dir_all(&pki_dir).unwrap();
	std::fs::create_dir_all(&syslog_dir).unwrap();
	for name in ["node.crt", "node.key", "ca.crt", "ops_ca.crt"] {
		std::fs::write(pki_dir.join(name), "test").unwrap();
	}
	let syslog_socket = syslog_dir.join("syslog.sock");
	std::fs::write(&syslog_socket, "").unwrap();

	let node_id = 31u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4704"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[time]
drift_max_offset_ms = 750
mvcc_retention_window_secs = 30

[transport.tls]
cert_path = "pki/node.crt"
key_path = "pki/node.key"
node_ca_path = "pki/ca.crt"
operator_ca_path = "pki/ops_ca.crt"

[observability.listeners]
health = "127.0.0.1:7431"
ready = "127.0.0.1:7432"
metrics = "127.0.0.1:7433"

[audit.syslog]
socket_path = "run/syslog.sock"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let tls = cfg.transport_tls.as_ref().expect("transport TLS config");
	assert_eq!(tls.cert_path, pki_dir.join("node.crt"));
	assert_eq!(tls.key_path, pki_dir.join("node.key"));
	assert_eq!(tls.node_ca_path, pki_dir.join("ca.crt"));
	assert_eq!(tls.operator_ca_path, pki_dir.join("ops_ca.crt"));
	assert_eq!(tls.cluster_domain, "cluster.local");
	assert_eq!(cfg.drift_max_offset_ms, 750);
	assert_eq!(
		cfg.mvcc_retention_policy,
		wendelon::storage::MvccRetentionPolicy::FixedWindow(std::time::Duration::from_secs(30))
	);
	let observability = cfg.observability.as_ref().expect("observability config");
	let listeners = observability
		.listeners
		.as_ref()
		.expect("observability listeners");
	assert_eq!(listeners.health, "127.0.0.1:7431");
	assert_eq!(listeners.ready, "127.0.0.1:7432");
	assert_eq!(listeners.metrics, "127.0.0.1:7433");
	assert!(observability.traces.is_none());
	assert_eq!(
		cfg.audit.syslog.as_ref().unwrap().socket_path,
		syslog_socket
	);
	assert!(cfg.audit.archive.is_none());
	assert!(cfg.audit.kafka.is_none());
}

#[test]
fn loads_archive_audit_sink_config() {
	let root = fresh_dir("audit-archive");
	std::fs::create_dir_all(root.join("data")).unwrap();
	std::fs::create_dir_all(root.join("remote-archive")).unwrap();

	let node_id = 37u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4709"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[audit.archive]
directory = "remote-archive"
max_segment_bytes = 1048576
lock_mode = "governance"
retain_days = 2555
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let archive = cfg.audit.archive.expect("archive config should be loaded");
	assert_eq!(archive.directory, root.join("remote-archive"));
	assert_eq!(archive.max_segment_bytes, 1_048_576);
	assert_eq!(
		archive.lock_mode,
		wendelon::audit::AuditArchiveLockMode::Governance
	);
	assert_eq!(archive.retain_days, Some(2555));
}

#[cfg(not(feature = "otlp-http"))]
#[test]
fn rejects_http_protobuf_trace_export_without_feature() {
	let root = fresh_dir("otlp-http-disabled");
	let data_dir = root.join("data");
	let pki_dir = root.join("pki");
	std::fs::create_dir_all(&data_dir).unwrap();
	std::fs::create_dir_all(&pki_dir).unwrap();
	for name in ["client.crt", "client.key", "ca.crt"] {
		std::fs::write(pki_dir.join(name), "test").unwrap();
	}

	let node_id = 35u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4707"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[observability.traces]
endpoint = "https://otel-collector.internal:4318"
protocol = "http/protobuf"
client_cert_path = "pki/client.crt"
client_key_path = "pki/client.key"
ca_bundle_path = "pki/ca.crt"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	assert!(
		err.to_string()
			.contains("requires the otlp-http cargo feature"),
		"unexpected error: {err}"
	);
}

#[cfg(feature = "otlp-http")]
#[test]
fn loads_http_protobuf_trace_export_with_feature() {
	let root = fresh_dir("otlp-http-enabled");
	let data_dir = root.join("data");
	let pki_dir = root.join("pki");
	std::fs::create_dir_all(&data_dir).unwrap();
	std::fs::create_dir_all(&pki_dir).unwrap();
	for name in ["client.crt", "client.key", "ca.crt"] {
		std::fs::write(pki_dir.join(name), "test").unwrap();
	}

	let node_id = 35u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4707"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[observability.traces]
endpoint = "https://otel-collector.internal:4318"
protocol = "http/protobuf"
sampling_rate = 0.25
client_cert_path = "pki/client.crt"
client_key_path = "pki/client.key"
ca_bundle_path = "pki/ca.crt"
max_queue_size = 1024
export_timeout_s = 11
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let traces = cfg
		.observability
		.as_ref()
		.and_then(|observability| observability.traces.as_ref())
		.expect("trace export config");
	assert_eq!(traces.endpoint, "https://otel-collector.internal:4318");
	assert_eq!(
		traces.protocol,
		wendelon::metrics::telemetry::TraceExportProtocol::HttpProtobuf
	);
	assert_eq!(traces.sampling_rate_per_million, 250_000);
	assert_eq!(traces.client_cert_path, pki_dir.join("client.crt"));
	assert_eq!(traces.client_key_path, pki_dir.join("client.key"));
	assert_eq!(traces.ca_bundle_path, pki_dir.join("ca.crt"));
	assert_eq!(traces.max_queue_size, 1024);
	assert_eq!(traces.export_timeout.as_secs(), 11);
}

#[cfg(not(feature = "otlp-grpc"))]
#[test]
fn rejects_grpc_trace_export_without_feature() {
	let root = fresh_dir("otlp-grpc");
	let data_dir = root.join("data");
	let pki_dir = root.join("pki");
	std::fs::create_dir_all(&data_dir).unwrap();
	std::fs::create_dir_all(&pki_dir).unwrap();
	for name in ["client.crt", "client.key", "ca.crt"] {
		std::fs::write(pki_dir.join(name), "test").unwrap();
	}

	let node_id = 36u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4708"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[observability.traces]
endpoint = "https://otel-collector.internal:4317"
protocol = "grpc"
client_cert_path = "pki/client.crt"
client_key_path = "pki/client.key"
ca_bundle_path = "pki/ca.crt"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	assert!(
		err.to_string()
			.contains("requires the otlp-grpc cargo feature"),
		"unexpected error: {err}"
	);
}

#[cfg(feature = "otlp-grpc")]
#[test]
fn loads_grpc_trace_export_with_feature() {
	let root = fresh_dir("otlp-grpc-enabled");
	let data_dir = root.join("data");
	let pki_dir = root.join("pki");
	std::fs::create_dir_all(&data_dir).unwrap();
	std::fs::create_dir_all(&pki_dir).unwrap();
	for name in ["client.crt", "client.key", "ca.crt"] {
		std::fs::write(pki_dir.join(name), "test").unwrap();
	}

	let node_id = 36u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4708"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[observability.traces]
endpoint = "https://otel-collector.internal:4317"
protocol = "grpc"
sampling_rate = 0.5
client_cert_path = "pki/client.crt"
client_key_path = "pki/client.key"
ca_bundle_path = "pki/ca.crt"
max_queue_size = 256
export_timeout_s = 9
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let traces = cfg
		.observability
		.as_ref()
		.and_then(|observability| observability.traces.as_ref())
		.expect("trace export config");
	assert_eq!(traces.endpoint, "https://otel-collector.internal:4317");
	assert_eq!(
		traces.protocol,
		wendelon::metrics::telemetry::TraceExportProtocol::Grpc
	);
	assert_eq!(traces.sampling_rate_per_million, 500_000);
	assert_eq!(traces.client_cert_path, pki_dir.join("client.crt"));
	assert_eq!(traces.client_key_path, pki_dir.join("client.key"));
	assert_eq!(traces.ca_bundle_path, pki_dir.join("ca.crt"));
	assert_eq!(traces.max_queue_size, 256);
	assert_eq!(traces.export_timeout.as_secs(), 9);
}

#[test]
fn zero_mvcc_retention_window_disables_pruning() {
	let root = fresh_dir("mvcc-retention-disabled");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 33u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4705"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[time]
mvcc_retention_window_secs = 0
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(
		cfg.mvcc_retention_policy,
		wendelon::storage::MvccRetentionPolicy::Disabled
	);
}

#[test]
fn rejects_observability_without_transport_tls() {
	let root = fresh_dir("obs-without-transport");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let node_id = 32u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4705"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[observability.listeners]
health = "127.0.0.1:7431"
ready = "127.0.0.1:7432"
metrics = "127.0.0.1:7433"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let err = load_node_bootstrap_config(&config_path).err().unwrap();
	assert!(
		err.to_string()
			.contains("observability requires transport_tls"),
		"unexpected error: {err}"
	);
}

#[cfg(not(feature = "audit-kafka"))]
#[test]
fn rejects_kafka_audit_sink_without_feature() {
	let root = fresh_dir("audit-kafka-disabled");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 34u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4706"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[audit.kafka]
brokers = "kafka-1.internal:9092"
topic = "wendelon.audit"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let err = load_node_bootstrap_config(&config_path).unwrap_err();
	assert!(
		err.to_string()
			.contains("audit.kafka requires the audit-kafka cargo feature"),
		"unexpected error: {err}"
	);
}

#[cfg(feature = "audit-kafka")]
#[test]
fn loads_kafka_audit_sink_with_feature() {
	let root = fresh_dir("audit-kafka-enabled");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 34u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4706"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[audit.kafka]
brokers = "kafka-1.internal:9092"
topic = "wendelon.audit"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let kafka = cfg.audit.kafka.expect("kafka config should be loaded");
	assert_eq!(kafka.brokers, "kafka-1.internal:9092");
	assert_eq!(kafka.topic, "wendelon.audit");
	assert_eq!(kafka.client_id, None);
	assert_eq!(
		kafka.payload_mode,
		wendelon::audit::KafkaAuditPayloadMode::Distribution
	);
}

#[cfg(feature = "audit-kafka")]
#[test]
fn loads_kafka_canonical_audit_sink_with_feature() {
	let root = fresh_dir("audit-kafka-canonical-enabled");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 38u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:4710"

[storage]
data_dir = "data"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"

[audit.kafka]
brokers = "kafka-1.internal:9092"
topic = "wendelon.audit"
payload_mode = "canonical"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let kafka = cfg.audit.kafka.expect("kafka config should be loaded");
	assert_eq!(kafka.brokers, "kafka-1.internal:9092");
	assert_eq!(kafka.topic, "wendelon.audit");
	assert_eq!(
		kafka.payload_mode,
		wendelon::audit::KafkaAuditPayloadMode::Canonical
	);
}

#[tokio::test]
async fn loaded_config_bootstraps_single_node() {
	let root = fresh_dir("bootstrap");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let node_id = 13u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
	let audit_secret = dev_audit_root_signing_secret(node_id);
	let config_path = write_config(
		&root,
		&format!(
			r#"
[node]
node_id = {node_id}
advertise_address = "127.0.0.1:0"

[storage]
data_dir = "{data_dir}"

[identity]
allow_dev_keys = true
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"
audit_root_signing_secret = "{audit_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[identity.trusted_audit_root_keys]
"{node_id}" = "{audit_vk}"
"#,
			node_id = node_id,
			data_dir = data_dir.display(),
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			audit_secret = hex(audit_secret),
			audit_vk = hex(audit_root_verifying_key_bytes(audit_secret)),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let node = WendelonNode::bootstrap_single_node(cfg).await.unwrap();
	assert_eq!(node.local_node.id, node_id);
	node.handles.ring_updater.abort();
	node.handles.drift_monitor.abort();
	node.handles.eviction_watchdog.abort();
	node.handles.lease_manager.abort();
	node.handles.epoch_reconciler.abort();
	node.handles.migration_sweeper.abort();
}

#[test]
fn rendered_example_config_loads_after_writing() {
	let root = fresh_dir("rendered-example");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();
	let config_path = root.join("wendelon.toml");
	std::fs::write(&config_path, render_example_config()).unwrap();

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.node_id, 1);
	assert!(!cfg.allow_dev_keys);
	assert_eq!(cfg.data_dir, data_dir);
	assert_eq!(cfg.advertise_address, "127.0.0.1:7001");
	assert!(!cfg.lsm_compaction_paused);
	assert!(
		!cfg.mvcc_retention_paused,
		"physical MVCC reclaim runs unless a node opts out"
	);
	assert!(matches!(
		cfg.root_kek_provider,
		wendelon::crypto::RootKekProviderConfig::DevInsecure
	));
}

#[test]
fn write_example_config_refuses_to_overwrite() {
	let root = fresh_dir("write-example");
	let config_path = root.join("wendelon.toml");

	write_example_config(&config_path).unwrap();
	#[cfg(unix)]
	{
		use std::os::unix::fs::PermissionsExt;
		assert_eq!(
			std::fs::metadata(&config_path)
				.unwrap()
				.permissions()
				.mode() & 0o077,
			0,
			"fresh starter signing secrets must remain owner-private"
		);
	}
	let err = write_example_config(&config_path).err().unwrap();
	assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
}

// Deliberately provision the unsafe keys only inside the refusal controls.
// Production starter rendering must no longer provide these fixtures.
fn deterministic_example_for_dev_key_controls() -> String {
	let mut document: toml::Value = toml::from_str(&render_example_config()).unwrap();
	let identity = document.get_mut("identity").unwrap();
	identity["allow_dev_keys"] = toml::Value::Boolean(true);
	for (secret_name, trust_name, secret, verifying) in [
		(
			"lease_signing_secret",
			"trusted_lease_signers",
			dev_lease_signing_secret(1),
			lease_verifying_key_bytes(dev_lease_signing_secret(1)),
		),
		(
			"transfer_signing_secret",
			"trusted_transfer_signers",
			dev_transfer_signing_secret(1),
			transfer_verifying_key_bytes(dev_transfer_signing_secret(1)),
		),
		(
			"audit_root_signing_secret",
			"trusted_audit_root_keys",
			dev_audit_root_signing_secret(1),
			audit_root_verifying_key_bytes(dev_audit_root_signing_secret(1)),
		),
	] {
		identity[secret_name] = toml::Value::String(hex(secret));
		identity[trust_name]["1"] = toml::Value::String(hex(verifying));
	}
	toml::to_string(&document).unwrap()
}

#[test]
fn identity_toml_dev_keys_require_explicit_authorization() {
	use wendelon::node::{IdentityKeyError, IdentityKeyRole};
	let root = fresh_dir("identity-dev-authorization");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let rendered = deterministic_example_for_dev_key_controls();
	for flag in ["", "allow_dev_keys = false\n"] {
		let body = rendered.replace("allow_dev_keys = true\n", flag);
		let path = write_config(&root, &body);
		let error = load_node_bootstrap_config(path).unwrap_err();
		assert_eq!(error.kind(), std::io::ErrorKind::InvalidInput);
		assert_eq!(
			error.get_ref().unwrap().downcast_ref::<IdentityKeyError>(),
			Some(&IdentityKeyError::DevKey {
				role: IdentityKeyRole::Lease,
				node: 1
			})
		);
	}
	let path = write_config(&root, &rendered);
	let cfg = load_node_bootstrap_config(path).unwrap();
	assert!(cfg.allow_dev_keys);
	cfg.validate().unwrap();
}

#[test]
fn identity_toml_peer_dev_key_is_refused_with_authorization_off() {
	use wendelon::node::{IdentityKeyError, IdentityKeyRole};
	let root = fresh_dir("identity-peer-dev");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let lease_secret = [41; 32];
	let transfer_secret = [42; 32];
	let audit_secret = [43; 32];
	let body = deterministic_example_for_dev_key_controls()
		.replace("allow_dev_keys = true", "allow_dev_keys = false")
		.replace(&hex(dev_lease_signing_secret(1)), &hex(lease_secret))
		.replace(&hex(dev_transfer_signing_secret(1)), &hex(transfer_secret))
		.replace(&hex(dev_audit_root_signing_secret(1)), &hex(audit_secret))
		.replace(
			&hex(audit_root_verifying_key_bytes(
				dev_audit_root_signing_secret(1),
			)),
			&hex(audit_root_verifying_key_bytes(audit_secret)),
		)
		.replace(
			&hex(lease_verifying_key_bytes(dev_lease_signing_secret(1))),
			&hex(lease_verifying_key_bytes(lease_secret)),
		)
		.replace(
			&hex(transfer_verifying_key_bytes(dev_transfer_signing_secret(1))),
			&hex(transfer_verifying_key_bytes(transfer_secret)),
		)
		.replace(
			"[identity.trusted_lease_signers]",
			&format!(
				"[identity.trusted_lease_signers]\n\"2\" = \"{}\"",
				hex(lease_verifying_key_bytes(dev_lease_signing_secret(2)))
			),
		);
	let error = load_node_bootstrap_config(write_config(&root, &body)).unwrap_err();
	assert_eq!(
		error.get_ref().unwrap().downcast_ref::<IdentityKeyError>(),
		Some(&IdentityKeyError::DevKey {
			role: IdentityKeyRole::Lease,
			node: 2
		})
	);
}

#[test]
fn identity_audit_toml_normalizes_ids_and_refuses_alias_duplicates() {
	use wendelon::audit::AuditTrustError;
	let root = fresh_dir("audit-alias");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let key = hex(audit_root_verifying_key_bytes(
		dev_audit_root_signing_secret(1),
	));
	let alias = render_example_config().replace(
		"[identity.trusted_audit_root_keys]",
		&format!("[identity.trusted_audit_root_keys]\n\"01\" = \"{key}\""),
	);
	let error = load_node_bootstrap_config(write_config(&root, &alias)).unwrap_err();
	assert_eq!(
		error.get_ref().unwrap().downcast_ref::<AuditTrustError>(),
		Some(&AuditTrustError::DuplicateNode { node: 1 })
	);
	let normalized = render_example_config().replace(
		&format!("[identity.trusted_audit_root_keys]\n\"1\" = \"{key}\""),
		&format!("[identity.trusted_audit_root_keys]\n\"01\" = \"{key}\""),
	);
	let cfg = load_node_bootstrap_config(write_config(&root, &normalized)).unwrap();
	assert_eq!(
		cfg.trusted_audit_root_keys.get(&1),
		Some(&audit_root_verifying_key_bytes(
			cfg.audit_root_signing_secret
		))
	);
}
#[test]
fn identity_audit_toml_requires_secret_and_trust_map() {
	let root = fresh_dir("audit-required");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let rendered = render_example_config();
	let document: toml::Value = toml::from_str(&rendered).unwrap();
	let secret = document["identity"]["audit_root_signing_secret"]
		.as_str()
		.unwrap();
	let without_secret = rendered.replace(&format!("audit_root_signing_secret = \"{secret}\""), "");
	assert!(load_node_bootstrap_config(write_config(&root, &without_secret)).is_err());
	let without_map = rendered.replace(
		"[identity.trusted_audit_root_keys]",
		"[identity.absent_audit_keys]",
	);
	assert!(load_node_bootstrap_config(write_config(&root, &without_map)).is_err());
}

#[test]
fn counter_retention_defaults_and_overrides_load() {
	let root = fresh_dir("counter-retention");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let defaults = write_config(&root, &render_example_config());
	let loaded = load_node_bootstrap_config(&defaults).unwrap();
	assert_eq!(
		loaded.counter_retention,
		wendelon::storage::counter::CounterRetentionConfig::default()
	);
	assert_eq!(loaded.counter_retention.expiry_ms, 3_960_000);

	let root = fresh_dir("counter-retention-override");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let body = format!(
		"{}\n[counter]\nattempt_horizon_secs = 600\nclock_skew_secs = 30\n",
		render_example_config()
	);
	let loaded = load_node_bootstrap_config(write_config(&root, &body)).unwrap();
	let retention = loaded.counter_retention;
	assert_eq!(
		(
			retention.attempt_horizon_ms,
			retention.clock_skew_ms,
			retention.expiry_ms
		),
		(600_000, 30_000, 3_600_000 + 600_000 + 30_000),
		"the expiry defaults to the incarnation lifetime plus H plus S"
	);
}

// T-CFGVAL: an out-of-range counter dedup horizon is refused at startup: a zero
// value, a skew allowance above the attempt horizon, an attempt horizon below
// the SDK's, an expiry shorter than the incarnation lifetime plus both, and an
// unknown key.
#[test]
fn out_of_range_counter_retention_is_refused_at_startup() {
	let cases = [
		("attempt_horizon_secs = 0", ErrorKind::InvalidInput),
		("clock_skew_secs = 0", ErrorKind::InvalidInput),
		("expiry_secs = 0", ErrorKind::InvalidInput),
		(
			"attempt_horizon_secs = 600\nclock_skew_secs = 601",
			ErrorKind::InvalidInput,
		),
		// Addendum A7: below the SDK's 300 s attempt horizon.
		("attempt_horizon_secs = 299", ErrorKind::InvalidInput),
		("expiry_secs = 3959", ErrorKind::InvalidInput),
		("expiry_secs = 9223372036854775807", ErrorKind::InvalidInput),
		("expiry_ms = 3960000", ErrorKind::InvalidData),
	];
	for (index, (section, kind)) in cases.into_iter().enumerate() {
		let root = fresh_dir(&format!("counter-retention-invalid-{index}"));
		std::fs::create_dir_all(root.join("data")).unwrap();
		let body = format!("{}\n[counter]\n{section}\n", render_example_config());
		let err = load_node_bootstrap_config(write_config(&root, &body)).unwrap_err();
		assert_eq!(err.kind(), kind, "{section}: {err}");
	}
	// The boundary itself is accepted.
	let root = fresh_dir("counter-retention-boundary");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let body = format!(
		"{}\n[counter]\nexpiry_secs = 3960\n",
		render_example_config()
	);
	load_node_bootstrap_config(write_config(&root, &body)).unwrap();
}

/// `tuning.consensus_timing_base_millis`: absent is the default profile, an accepted value
/// is carried, and a value outside 10..=1000 refuses the configuration with the range named.
#[test]
fn the_consensus_timing_base_loads_defaults_and_is_refused_outside_its_range() {
	let with_base = |tag: &str, line: &str| {
		let root = fresh_dir(tag);
		std::fs::create_dir_all(root.join("data")).unwrap();
		let example = render_example_config();
		let marker = "consensus_timing_base_millis = 50";
		assert_eq!(example.matches(marker).count(), 1);
		let body = example.replace(marker, line);
		let loaded = load_node_bootstrap_config(write_config(&root, &body));
		(root, loaded)
	};
	let (_root, absent) = with_base("consensus-timing-absent", "");
	assert_eq!(absent.unwrap().consensus_timing.base_millis(), 50);
	let (_root, example) = with_base(
		"consensus-timing-example",
		"consensus_timing_base_millis = 50",
	);
	assert_eq!(
		example.unwrap().consensus_timing,
		wendelon::consensus::ConsensusTiming::default()
	);
	for accepted in [10_u64, 250, 1000] {
		let (_root, loaded) = with_base(
			&format!("consensus-timing-{accepted}"),
			&format!("consensus_timing_base_millis = {accepted}"),
		);
		assert_eq!(loaded.unwrap().consensus_timing.base_millis(), accepted);
	}
	for refused in ["0", "9", "1001", "9223372036854775807"] {
		let (_root, loaded) = with_base(
			&format!("consensus-timing-refused-{refused}"),
			&format!("consensus_timing_base_millis = {refused}"),
		);
		let err = loaded.unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput, "{refused}: {err}");
		assert!(
			err.to_string()
				.contains("tuning.consensus_timing_base_millis must be between 10 and 1000"),
			"{refused}: {err}"
		);
	}
}

#[test]
fn example_signing_keys_are_fresh_independent_and_dev_keys_stay_disabled() {
	let scratch = fresh_dir("fresh-example-signing-keys");
	std::fs::create_dir_all(scratch.join("data")).unwrap();
	let mut previous = Vec::new();
	for iteration in 0..2 {
		let path = scratch.join(format!("example-{iteration}.toml"));
		std::fs::write(&path, render_example_config()).unwrap();
		let cfg = load_node_bootstrap_config(&path).unwrap();
		assert!(!cfg.allow_dev_keys);
		let keys = [
			cfg.lease_signing_secret,
			cfg.transfer_signing_secret,
			cfg.audit_root_signing_secret,
		];
		for key in keys {
			assert_ne!(key, [0; 32]);
			assert!(
				!previous.contains(&key),
				"a role or render reused a signing key"
			);
			assert_ne!(key, dev_lease_signing_secret(cfg.node_id));
			assert_ne!(key, dev_transfer_signing_secret(cfg.node_id));
			assert_ne!(key, dev_audit_root_signing_secret(cfg.node_id));
			previous.push(key);
		}
		assert_eq!(
			cfg.trusted_lease_signers[&cfg.node_id],
			lease_verifying_key_bytes(cfg.lease_signing_secret)
		);
		assert_eq!(
			cfg.trusted_transfer_signers[&cfg.node_id],
			transfer_verifying_key_bytes(cfg.transfer_signing_secret)
		);
		assert_eq!(
			cfg.trusted_audit_root_keys[&cfg.node_id],
			audit_root_verifying_key_bytes(cfg.audit_root_signing_secret)
		);
	}
}

fn stall_bound_config(tag: &str, line: &str) -> (test_dir::TestDir, PathBuf) {
	let root = fresh_dir(tag);
	std::fs::create_dir_all(root.join("data")).unwrap();
	let rendered = render_example_config();
	assert!(rendered.contains("query_stall_bound_millis = 250\n"));
	let body = rendered.replace("query_stall_bound_millis = 250\n", line);
	let config_path = write_config(&root, &body);
	(root, config_path)
}

#[test]
fn the_query_stall_bound_defaults_to_250_ms_and_loads_within_its_range() {
	for (tag, line, expected) in [
		(
			"stall-bound-example",
			"query_stall_bound_millis = 250\n",
			250,
		),
		("stall-bound-absent", "", 250),
		("stall-bound-least", "query_stall_bound_millis = 1\n", 1),
		(
			"stall-bound-most",
			"query_stall_bound_millis = 600000\n",
			600_000,
		),
		(
			"stall-bound-chosen",
			"query_stall_bound_millis = 1500\n",
			1_500,
		),
	] {
		let (_root, config_path) = stall_bound_config(tag, line);
		let cfg = load_node_bootstrap_config(&config_path).unwrap();
		assert_eq!(
			cfg.query_stall_bound.get(),
			std::time::Duration::from_millis(expected),
			"{tag}"
		);
	}
}

#[test]
fn a_query_stall_bound_outside_its_range_is_refused_by_name() {
	for (tag, value) in [("stall-bound-zero", 0u64), ("stall-bound-over", 600_001)] {
		let (_root, config_path) =
			stall_bound_config(tag, &format!("query_stall_bound_millis = {value}\n"));
		let err = load_node_bootstrap_config(&config_path).unwrap_err();
		assert_eq!(err.kind(), ErrorKind::InvalidInput, "{tag}");
		assert!(
			err.to_string()
				.contains("tuning.query_stall_bound_millis must be between 1 and 600000"),
			"unexpected error for {tag}: {err}"
		);
	}
}

#[test]
fn a_mistyped_or_non_integer_query_stall_bound_fails_by_position_only() {
	for (tag, line) in [
		("stall-bound-typo", "query_stall_bound_milis = 400\n"),
		(
			"stall-bound-text",
			"query_stall_bound_millis = \"sentinel-9c1f\"\n",
		),
		(
			"stall-bound-negative",
			"query_stall_bound_millis = -48151623\n",
		),
	] {
		let (_root, config_path) = stall_bound_config(tag, line);
		let body = std::fs::read_to_string(&config_path).unwrap();
		let at = body
			.lines()
			.position(|candidate| candidate == line.trim_end())
			.unwrap() + 1;
		let err = load_node_bootstrap_config(&config_path).unwrap_err();
		let message = err.to_string();
		assert_eq!(err.kind(), ErrorKind::InvalidData, "{tag}");
		assert!(
			message.contains(&format!("at line {at}, column ")),
			"unexpected error for {tag}: {err}"
		);
		for withheld in ["milis", "sentinel-9c1f", "48151623"] {
			assert!(
				!message.contains(withheld),
				"{tag} echoed {withheld}: {err}"
			);
		}
	}
}

// ADR 0721: a daemon's configuration selects the ParentDecisions authority.

fn hex32(bytes: [u8; 32]) -> String {
	bytes.iter().map(|byte| format!("{byte:02x}")).collect()
}

/// The rendered example (fresh non-development keys, `allow_dev_keys = false`)
/// selecting ParentDecisions, with `cluster`, `identity` and trailing text added.
fn parent_decisions_config(
	tag: &str,
	cluster: &str,
	identity: &str,
	tail: &str,
) -> (test_dir::TestDir, PathBuf) {
	let root = fresh_dir(tag);
	std::fs::create_dir_all(root.join("data")).unwrap();
	let rendered = render_example_config();
	assert!(rendered.contains("mode = \"bootstrap\"\n"));
	assert!(rendered.contains("allow_dev_keys = false\n"));
	let body = rendered
		.replace(
			"mode = \"bootstrap\"\n",
			&format!("mode = \"bootstrap\"\n{cluster}"),
		)
		.replace(
			"allow_dev_keys = false\n",
			&format!("allow_dev_keys = false\n{identity}"),
		) + tail;
	let config_path = write_config(&root, &body);
	(root, config_path)
}

const DECISION_SECRET: [u8; 32] = [0x6B; 32];

fn decision_identity() -> (String, String) {
	let verifying = wendelon::consensus::decisions::decision_verifying_key_bytes(DECISION_SECRET);
	(
		format!("decision_signing_secret = \"{}\"\n", hex32(DECISION_SECRET)),
		format!(
			"\n[time]\nmvcc_retention_window_secs = 0\n\n[identity.trusted_decision_signers]\n\"1\" = \"{}\"\n",
			hex32(verifying)
		),
	)
}

#[test]
fn the_decision_authority_defaults_to_today_and_is_selected_with_its_own_keys() {
	let root = fresh_dir("authority-default");
	std::fs::create_dir_all(root.join("data")).unwrap();
	let default =
		load_node_bootstrap_config(&write_config(&root, &render_example_config())).unwrap();
	assert!(!default.selects_parent_decisions());

	let (secret, tail) = decision_identity();
	let (_root, path) = parent_decisions_config(
		"authority-selected",
		"decision_authority = \"parent-decisions\"\n",
		&secret,
		&tail,
	);
	let cfg = load_node_bootstrap_config(&path).unwrap();
	assert!(cfg.selects_parent_decisions());
	assert!(!cfg.allow_dev_keys);
	// The configuration prints without its decision secret.
	let printed = format!("{cfg:?}");
	assert!(printed.contains("decision_signing_secret: Some(<redacted>)"));
	assert!(!printed.contains(&format!("{DECISION_SECRET:?}")));
	assert!(!printed.contains(&hex32(DECISION_SECRET)));

	let (_root, explicit) = parent_decisions_config(
		"authority-today",
		"decision_authority = \"today\"\n",
		"",
		"",
	);
	assert!(
		!load_node_bootstrap_config(&explicit)
			.unwrap()
			.selects_parent_decisions()
	);
}

#[test]
fn a_selected_authority_without_its_keys_or_keys_without_it_are_refused() {
	let (secret, tail) = decision_identity();
	for (tag, cluster, identity, tail, expected) in [
		(
			"authority-no-keys",
			"decision_authority = \"parent-decisions\"\n",
			String::new(),
			"\n[time]\nmvcc_retention_window_secs = 0\n".to_owned(),
			"requires identity.decision_signing_secret and identity.trusted_decision_signers",
		),
		(
			"authority-no-secret",
			"decision_authority = \"parent-decisions\"\n",
			String::new(),
			tail.clone(),
			"requires identity.decision_signing_secret and identity.trusted_decision_signers",
		),
		(
			"authority-no-map",
			"decision_authority = \"parent-decisions\"\n",
			secret.clone(),
			"\n[time]\nmvcc_retention_window_secs = 0\n".to_owned(),
			"requires identity.decision_signing_secret and identity.trusted_decision_signers",
		),
		(
			"keys-without-authority",
			"",
			secret.clone(),
			tail.clone(),
			"are only for cluster.decision_authority = \"parent-decisions\"",
		),
	] {
		let (_root, path) = parent_decisions_config(tag, cluster, &identity, &tail);
		let error = load_node_bootstrap_config(&path).unwrap_err();
		let message = error.to_string();
		assert_eq!(error.kind(), ErrorKind::InvalidInput, "{tag}: {message}");
		assert!(message.contains(expected), "{tag}: {message}");
		assert!(!message.contains(&hex32(DECISION_SECRET)), "{tag}");
	}
}

#[test]
fn a_parent_decisions_store_refuses_development_keys_a_retention_window_and_a_bad_key() {
	let (secret, tail) = decision_identity();
	// A recognised development key in the decision map.
	let dev = wendelon::consensus::decisions::decision_verifying_key_bytes(
		wendelon::consensus::decisions::dev_decision_signing_secret(2),
	);
	let (_root, path) = parent_decisions_config(
		"authority-dev-key",
		"decision_authority = \"parent-decisions\"\n",
		&secret,
		&format!("{tail}\"2\" = \"{}\"\n", hex32(dev)),
	);
	let error = load_node_bootstrap_config(&path).unwrap_err();
	assert!(
		error.to_string().contains("needs identity.allow_dev_keys"),
		"{error}"
	);

	// The default seven-day MVCC window.
	let no_window = tail.replace("[time]\nmvcc_retention_window_secs = 0\n", "");
	let (_root, path) = parent_decisions_config(
		"authority-window",
		"decision_authority = \"parent-decisions\"\n",
		&secret,
		&no_window,
	);
	let error = load_node_bootstrap_config(&path).unwrap_err();
	assert!(
		error
			.to_string()
			.contains("set time.mvcc_retention_window_secs = 0"),
		"{error}"
	);

	// A secret that is not 64 hex characters: named by field, never echoed.
	let (_root, path) = parent_decisions_config(
		"authority-short-secret",
		"decision_authority = \"parent-decisions\"\n",
		"decision_signing_secret = \"c0ffee-sentinel-7d2a\"\n",
		&tail,
	);
	let error = load_node_bootstrap_config(&path).unwrap_err();
	let message = error.to_string();
	assert!(
		message.contains("identity.decision_signing_secret must be 64 hex characters"),
		"{message}"
	);
	assert!(!message.contains("c0ffee-sentinel-7d2a"), "{message}");
}

#[test]
fn an_unknown_identity_or_cluster_key_fails_by_position_only() {
	let (secret, tail) = decision_identity();
	for (tag, cluster, identity, withheld) in [
		(
			"identity-typo",
			"decision_authority = \"parent-decisions\"\n".to_owned(),
			secret.replace("decision_signing_secret", "decision_signing_secrte"),
			"secrte",
		),
		(
			"cluster-typo",
			"decision_authorty = \"parent-decisions\"\n".to_owned(),
			secret.clone(),
			"authorty",
		),
		(
			"cluster-unknown-authority",
			"decision_authority = \"parent-decisons\"\n".to_owned(),
			secret.clone(),
			"decisons",
		),
	] {
		let (_root, path) = parent_decisions_config(tag, &cluster, &identity, &tail);
		let error = load_node_bootstrap_config(&path).unwrap_err();
		let message = error.to_string();
		assert_eq!(error.kind(), ErrorKind::InvalidData, "{tag}: {message}");
		assert!(message.contains(" at line "), "{tag}: {message}");
		assert!(!message.contains(withheld), "{tag}: {message}");
		assert!(
			!message.contains(&hex32(DECISION_SECRET)),
			"{tag}: {message}"
		);
	}
}
