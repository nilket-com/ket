use crate::network::{
	dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
	transfer_verifying_key_bytes,
};
use crate::node::{ClusterBootstrapConfig, NodeBootstrapConfig, SeedNodeConfig};
use crate::schema::ProtocolVersionWindow;
use serde::Deserialize;
use std::collections::BTreeMap;
use std::fs::OpenOptions;
use std::io::{Error, ErrorKind, Write};
use std::path::{Path, PathBuf};

const EXAMPLE_CONFIG_TEMPLATE: &[u8] = include_bytes!("../examples/wendelon.toml.example");
const EXAMPLE_NODE_ID: u32 = 1;
const EXAMPLE_ADVERTISE_ADDRESS: &str = "127.0.0.1:7001";
const EXAMPLE_DATA_DIR: &str = "data";

/// Load a `wendelon.toml` file and translate it into the
/// bootstrap shape consumed by [`crate::node::WendelonNode`].
pub fn load_node_bootstrap_config(path: impl AsRef<Path>) -> std::io::Result<NodeBootstrapConfig> {
	let path = path.as_ref();
	let text = std::fs::read_to_string(path)?;
	let raw: RawConfig = toml::from_str(&text).map_err(|err| {
		Error::new(
			ErrorKind::InvalidData,
			format!("failed to parse {}: {err}", path.display()),
		)
	})?;
	raw.into_bootstrap_config(path)
}

/// Render a concrete starter `wendelon.toml` from the checked-in
/// template. The generated file is valid for a single-node dev
/// bootstrap and can be edited in place by operators afterward.
pub fn render_example_config() -> String {
	let template = std::str::from_utf8(EXAMPLE_CONFIG_TEMPLATE)
		.expect("example config template must be valid UTF-8");
	let lease_secret = dev_lease_signing_secret(EXAMPLE_NODE_ID);
	let transfer_secret = dev_transfer_signing_secret(EXAMPLE_NODE_ID);

	template
		.replace("{{NODE_ID}}", &EXAMPLE_NODE_ID.to_string())
		.replace("{{ADVERTISE_ADDRESS}}", EXAMPLE_ADVERTISE_ADDRESS)
		.replace("{{DATA_DIR}}", EXAMPLE_DATA_DIR)
		.replace("{{SLAB_MAX_BYTES}}", &default_slab_max_bytes().to_string())
		.replace("{{RF}}", &default_rf().to_string())
		.replace("{{IO_DEPTH}}", &default_io_depth().to_string())
		.replace("{{LEASE_SIGNING_SECRET}}", &hex_string(lease_secret))
		.replace(
			"{{LEASE_VERIFYING_KEY}}",
			&hex_string(lease_verifying_key_bytes(lease_secret)),
		)
		.replace("{{TRANSFER_SIGNING_SECRET}}", &hex_string(transfer_secret))
		.replace(
			"{{TRANSFER_VERIFYING_KEY}}",
			&hex_string(transfer_verifying_key_bytes(transfer_secret)),
		)
}

/// Write a concrete starter config to `path`. Refuses to clobber an
/// existing file.
pub fn write_example_config(path: impl AsRef<Path>) -> std::io::Result<()> {
	let path = path.as_ref();
	let mut file = OpenOptions::new().write(true).create_new(true).open(path)?;
	file.write_all(render_example_config().as_bytes())?;
	file.flush()?;
	Ok(())
}

#[derive(Debug, Deserialize)]
struct RawConfig {
	node: RawNodeConfig,
	storage: RawStorageConfig,
	identity: RawIdentityConfig,
	#[serde(default)]
	tuning: RawTuningConfig,
	#[serde(default)]
	cluster: RawClusterConfig,
}

#[derive(Debug, Deserialize)]
struct RawNodeConfig {
	node_id: u32,
	advertise_address: String,
}

#[derive(Debug, Deserialize)]
struct RawStorageConfig {
	data_dir: PathBuf,
	#[serde(default = "default_slab_max_bytes")]
	slab_max_bytes: u64,
}

#[derive(Debug, Deserialize)]
struct RawTuningConfig {
	#[serde(default = "default_rf")]
	rf: usize,
	#[serde(default = "default_io_depth")]
	io_depth: u32,
}

impl Default for RawTuningConfig {
	fn default() -> Self {
		Self {
			rf: default_rf(),
			io_depth: default_io_depth(),
		}
	}
}

#[derive(Debug, Deserialize)]
struct RawIdentityConfig {
	lease_signing_secret: String,
	trusted_lease_signers: BTreeMap<String, String>,
	transfer_signing_secret: String,
	trusted_transfer_signers: BTreeMap<String, String>,
}

#[derive(Debug, Deserialize)]
struct RawClusterConfig {
	#[serde(default)]
	mode: RawClusterMode,
	#[serde(default)]
	seed_nodes: Vec<RawSeedNode>,
}

impl Default for RawClusterConfig {
	fn default() -> Self {
		Self {
			mode: RawClusterMode::Bootstrap,
			seed_nodes: Vec::new(),
		}
	}
}

#[derive(Clone, Copy, Debug, Default, Deserialize)]
#[serde(rename_all = "kebab-case")]
enum RawClusterMode {
	#[default]
	Bootstrap,
	Join,
}

#[derive(Debug, Deserialize)]
struct RawSeedNode {
	node_id: u32,
	address: String,
}

impl RawConfig {
	fn into_bootstrap_config(self, source_path: &Path) -> std::io::Result<NodeBootstrapConfig> {
		let data_dir = absolutize_from_config_dir(source_path, &self.storage.data_dir);
		if !data_dir.is_dir() {
			return Err(Error::new(
				ErrorKind::NotFound,
				format!(
					"configured data_dir is not an existing directory: {}",
					data_dir.display()
				),
			));
		}
		let advertise_address = self.node.advertise_address.trim().to_string();
		if advertise_address.is_empty() {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"node.advertise_address must not be empty",
			));
		}
		if self.storage.slab_max_bytes == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"storage.slab_max_bytes must be greater than zero",
			));
		}
		if self.tuning.rf == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tuning.rf must be greater than zero",
			));
		}
		if self.tuning.io_depth == 0 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"tuning.io_depth must be greater than zero",
			));
		}

		let lease_signing_secret = parse_key_material(
			"identity.lease_signing_secret",
			&self.identity.lease_signing_secret,
		)?;
		let transfer_signing_secret = parse_key_material(
			"identity.transfer_signing_secret",
			&self.identity.transfer_signing_secret,
		)?;
		let trusted_lease_signers = parse_trusted_signers(
			"identity.trusted_lease_signers",
			&self.identity.trusted_lease_signers,
		)?;
		let trusted_transfer_signers = parse_trusted_signers(
			"identity.trusted_transfer_signers",
			&self.identity.trusted_transfer_signers,
		)?;
		let cluster = match self.cluster.mode {
			RawClusterMode::Bootstrap => ClusterBootstrapConfig::Bootstrap,
			RawClusterMode::Join => ClusterBootstrapConfig::Join {
				seeds: self
					.cluster
					.seed_nodes
					.into_iter()
					.map(|seed| SeedNodeConfig {
						node_id: seed.node_id,
						address: seed.address,
					})
					.collect(),
			},
		};

		let cfg = NodeBootstrapConfig {
			node_id: self.node.node_id,
			data_dir,
			slab_max_bytes: self.storage.slab_max_bytes,
			group_commit: crate::storage::GroupCommitConfig::default(),
			rf: self.tuning.rf,
			io_depth: self.tuning.io_depth,
			advertise_address,
			lease_signing_secret,
			trusted_lease_signers,
			transfer_signing_secret,
			trusted_transfer_signers,
			cluster,
			l2: None,
			protocol: ProtocolVersionWindow::default(),
		};
		cfg.validate()?;
		Ok(cfg)
	}
}

fn absolutize_from_config_dir(config_path: &Path, value: &Path) -> PathBuf {
	if value.is_absolute() {
		value.to_path_buf()
	} else {
		config_path
			.parent()
			.unwrap_or_else(|| Path::new("."))
			.join(value)
	}
}

fn parse_trusted_signers(
	field: &str,
	raw: &BTreeMap<String, String>,
) -> std::io::Result<BTreeMap<u32, [u8; 32]>> {
	if raw.is_empty() {
		return Err(Error::new(
			ErrorKind::InvalidInput,
			format!("{field} must not be empty"),
		));
	}
	let mut parsed = BTreeMap::new();
	for (node_id, key) in raw {
		let node_id: u32 = node_id.parse().map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("{field} contains non-u32 node id {node_id:?}: {err}"),
			)
		})?;
		let key_bytes = parse_key_material(&format!("{field}.{node_id}"), key)?;
		parsed.insert(node_id, key_bytes);
	}
	Ok(parsed)
}

fn parse_key_material(field: &str, raw: &str) -> std::io::Result<[u8; 32]> {
	let compact = raw.trim();
	let compact = compact.strip_prefix("0x").unwrap_or(compact);
	if compact.len() != 64 {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("{field} must be 64 hex characters, got {}", compact.len()),
		));
	}
	let mut out = [0u8; 32];
	for (idx, chunk) in compact.as_bytes().chunks_exact(2).enumerate() {
		let hex = std::str::from_utf8(chunk).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("{field} contains invalid UTF-8 hex: {err}"),
			)
		})?;
		out[idx] = u8::from_str_radix(hex, 16).map_err(|err| {
			Error::new(
				ErrorKind::InvalidData,
				format!("{field} contains invalid hex: {err}"),
			)
		})?;
	}
	Ok(out)
}

fn hex_string(bytes: [u8; 32]) -> String {
	let mut out = String::with_capacity(64);
	for byte in bytes {
		use std::fmt::Write as _;
		write!(&mut out, "{byte:02x}").expect("writing to string cannot fail");
	}
	out
}

const fn default_slab_max_bytes() -> u64 {
	64 * 1024 * 1024
}

const fn default_rf() -> usize {
	1
}

const fn default_io_depth() -> u32 {
	64
}
