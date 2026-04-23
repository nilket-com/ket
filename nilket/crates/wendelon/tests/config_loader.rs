use std::path::{Path, PathBuf};
use wendelon::config::{load_node_bootstrap_config, render_example_config, write_example_config};
use wendelon::crypto::{
	DEFAULT_LOCAL_SEALED_ROOT_KEK_SECRET_ENV, DEFAULT_SLAB_DEK_CACHE_MAX_ENTRIES,
	DEFAULT_TENANT_KEK_CACHE_MAX_ENTRIES, SLAB_MAX_BYTES_HARD_CEILING,
};
use wendelon::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use wendelon::node::{ClusterBootstrapConfig, WendelonNode};

fn fresh_dir(tag: &str) -> PathBuf {
	let dir = std::env::temp_dir().join(format!(
		"wendelon-config-loader-{tag}-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4()
	));
	std::fs::create_dir_all(&dir).unwrap();
	dir
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

#[test]
fn loads_valid_wendelon_toml() {
	let root = fresh_dir("valid");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let node_id = 7u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
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

[tuning]
rf = 3
wal_io_depth = 128
slab_io_depth = 128

[identity]
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.node_id, node_id);
	assert_eq!(cfg.data_dir, data_dir);
	assert_eq!(cfg.slab_max_bytes, 123456);
	assert_eq!(cfg.rf, 3);
	assert_eq!(cfg.wal_io_depth, 128);
	assert_eq!(cfg.slab_io_depth, 128);
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
fn loads_split_io_depth_tuning() {
	let root = fresh_dir("split-io-depth");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 43u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	assert_eq!(cfg.wal_io_depth, 32);
	assert_eq!(cfg.slab_io_depth, 256);
}

#[test]
fn loads_transport_admission_tuning() {
	let root = fresh_dir("transport-admission");
	std::fs::create_dir_all(root.join("data")).unwrap();

	let node_id = 44u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[crypto]
tenant_kek_cache_max_entries = 17
slab_dek_cache_max_entries = 91
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[crypto]
root_kek_provider = {{ kind = "local-sealed", unlock_secret_file = "root-kek.secret" }}
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[crypto]
root_kek_provider = {{ kind = "aws-kms", region = "us-east-1", key_id = "arn:aws:kms:us-east-1:111122223333:key/test", endpoint = "http://127.0.0.1:4566", access_key_id_env = "WENDELON_TEST_AWS_ACCESS_KEY_ID", secret_access_key_env = "WENDELON_TEST_AWS_SECRET_ACCESS_KEY", session_token_env = "WENDELON_TEST_AWS_SESSION_TOKEN" }}
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[crypto]
root_kek_provider = {{ kind = "pkcs11", library_path = "libpkcs11-test.so", token_label = "WendelonToken", key_label = "root-kek", pin_env = "{pin_env}", slot = 7 }}
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[crypto]
tenant_kek_cache_max_entries = 0
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "not-hex"
transfer_signing_secret = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[identity.trusted_lease_signers]
"11" = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"

[identity.trusted_transfer_signers]
"11" = "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef"
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"
"#,
			node_id = node_id,
			too_large = SLAB_MAX_BYTES_HARD_CEILING + 1,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

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
	assert!(cfg.audit.kafka.is_none());
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[time]
mvcc_retention_window_secs = 0
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[observability.listeners]
health = "127.0.0.1:7431"
ready = "127.0.0.1:7432"
metrics = "127.0.0.1:7433"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[audit.kafka]
brokers = "kafka-1.internal:9092"
topic = "wendelon.audit"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"

[audit.kafka]
brokers = "kafka-1.internal:9092"
topic = "wendelon.audit"
"#,
			node_id = node_id,
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
			lease_vk = hex(lease_verifying_key_bytes(lease_secret)),
			transfer_vk = hex(transfer_verifying_key_bytes(transfer_secret)),
		),
	);

	let cfg = load_node_bootstrap_config(&config_path).unwrap();
	let kafka = cfg.audit.kafka.expect("kafka config should be loaded");
	assert_eq!(kafka.brokers, "kafka-1.internal:9092");
	assert_eq!(kafka.topic, "wendelon.audit");
	assert_eq!(kafka.client_id, None);
}

#[tokio::test]
async fn loaded_config_bootstraps_single_node() {
	let root = fresh_dir("bootstrap");
	let data_dir = root.join("data");
	std::fs::create_dir_all(&data_dir).unwrap();

	let node_id = 13u32;
	let lease_secret = dev_lease_signing_secret(node_id);
	let transfer_secret = dev_transfer_signing_secret(node_id);
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
lease_signing_secret = "{lease_secret}"
transfer_signing_secret = "{transfer_secret}"

[identity.trusted_lease_signers]
"{node_id}" = "{lease_vk}"

[identity.trusted_transfer_signers]
"{node_id}" = "{transfer_vk}"
"#,
			node_id = node_id,
			data_dir = data_dir.display(),
			lease_secret = hex(lease_secret),
			transfer_secret = hex(transfer_secret),
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
	assert_eq!(cfg.data_dir, data_dir);
	assert_eq!(cfg.advertise_address, "127.0.0.1:7001");
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
	let err = write_example_config(&config_path).err().unwrap();
	assert_eq!(err.kind(), std::io::ErrorKind::AlreadyExists);
}
