//! ADR 0721: three real `nilket wendelon` daemons, started from configuration files, form a
//! ParentDecisions cluster over mutual TLS on loopback and serve the public network client.
//!
//! External and `#[ignore]`d: it spawns processes and binds ports, so it never runs in a default
//! pass. It lives beside the public client it drives; the `nilket` binary is another package's,
//! so its path comes from `WENDELON_SMOKE_NILKET_BIN` and the test fails, never skips, without it. Run it alone, under an external owner that can end the whole run (a transient systemd
//! unit with a runtime limit): the guard below kills this run's process groups on drop, but a
//! drop does not happen when the test process itself is killed.
//!
//! What its observations prove, and no more:
//! - `nilket wendelon inspect` exiting 0 at a node: that node's `/ready` answered 200, which
//!   under ParentDecisions includes its decision signer bound to the key it signs with and its
//!   primary ranges held. Its `cluster_members` are the **epoch's peers**, not the Raft voters.
//! - Who may do what. Data requests (table write, table query) are accepted only from a
//!   `data_node` identity, in every mode; an application's is a **storage** certificate: role
//!   `data_node` under the node CA, carrying a tenant claim that confines it to that tenant, with
//!   a node name that is not a cluster member's. An operator certificate may inspect, create a
//!   tenant and submit a schema update, and may not send a data request. The default tenant 0
//!   cannot have a storage certificate, so the test creates tenant 1 through the public
//!   `nilket wendelon tenant create` as the operator, and all its data is in tenant 1. The two
//!   credentials are never mixed: the operator's client never sends a data request and the
//!   storage client never administers.
//! - A row read back through `wendelon_client::Client`: the public client discovered the
//!   cluster from one bootstrap peer and was answered by whichever node it routed to. The
//!   client chooses the node; a client is built from each node in turn so discovery is
//!   exercised through all three, which is not the same as choosing the answering node.
//!
//! - The write is ONE call to the public `table_write`; whatever retries happen are the
//!   client's own, inside that call, under one operation identity. Its `Ok` is not treated as
//!   proof of a committed acknowledgement: the public receipt does not expose the server's
//!   commit knowledge. What this test relies on is the exact read-back of the row afterwards.
//! - Every client call is made once. Any error, at any step, fails the test: nothing is asked
//!   again from here, so no transport, authorisation or decode failure can be retried into a pass.
//!
//! Not covered: an object store, tiering, a leader failure, more than one host.

use std::fmt::Write as _;
use std::io::Read as _;
use std::os::unix::process::CommandExt as _;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::Arc;
use std::time::{Duration, Instant};

use arrow::array::{Int64Array, LargeStringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use wendelon::integration::compute::SchemaIntentId;
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptor, RowIdentityPolicy, SchemaEvolutionPolicy, TableObjectId,
	TableSchemaDescriptor, TenantId,
};
use wendelon_client::{
	BootstrapPeer, Client, ClientConfig, TableQueryOptions, TableSchemaUpdateRequest, TlsPaths,
	WriteOptions,
};

const NODES: [u32; 3] = [1, 2, 3];
/// The application's tenant, created by the operator. Not the default tenant 0.
const TENANT: u64 = 1;
/// The storage client's node name index: deliberately no member of the cluster.
const STORAGE_NODE_INDEX: u32 = 9;
const DOMAIN: &str = "cluster.local";
/// The whole test, cleanup included. The external owner's limit must be longer.
const TEST_BOUND: Duration = Duration::from_secs(180);
const READY_BOUND: Duration = Duration::from_secs(60);
const CLIENT_BOUND: Duration = Duration::from_secs(30);
/// One `nilket wendelon inspect` child.
const INSPECT_BOUND: Duration = Duration::from_secs(10);
/// `storage_shutdown_timeout_seconds = 5` puts the daemon's own shutdown watchdog at 35 s.
const STOP_BOUND: Duration = Duration::from_secs(40);

fn nilket_bin() -> PathBuf {
	match std::env::var_os("WENDELON_SMOKE_NILKET_BIN") {
		Some(path) if !path.is_empty() => PathBuf::from(path),
		_ => panic!("WENDELON_SMOKE_NILKET_BIN is not set: this test needs the nilket binary"),
	}
}

/// Signals one process; `false` if it could not be signalled.
fn signal(pid: u32, signal: libc::c_int) -> bool {
	// SAFETY: `kill` takes plain integers and touches no memory of this process.
	unsafe { libc::kill(pid as libc::pid_t, signal) == 0 }
}

/// Kills a process group this test created (each daemon leads its own).
fn kill_group(pid: u32) {
	// SAFETY: as for `signal`.
	unsafe {
		libc::killpg(pid as libc::pid_t, libc::SIGKILL);
	}
}

fn random32() -> [u8; 32] {
	let mut bytes = [0u8; 32];
	std::fs::File::open("/dev/urandom")
		.and_then(|mut file| file.read_exact(&mut bytes))
		.expect("OS randomness");
	bytes
}

fn hex(bytes: [u8; 32]) -> String {
	bytes.iter().fold(String::new(), |mut out, byte| {
		write!(out, "{byte:02x}").unwrap();
		out
	})
}

fn write_private(path: &Path, contents: &[u8]) {
	use std::io::Write as _;
	use std::os::unix::fs::OpenOptionsExt as _;
	std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o600)
		.open(path)
		.and_then(|mut file| file.write_all(contents))
		.unwrap_or_else(|error| panic!("write {}: {error}", path.display()));
}

/// One UDP and one TCP socket on the same loopback port, as a node's QUIC and TCP-TLS listeners
/// share its advertised port. Released before the daemon starts; a port taken in between fails
/// the test, it is not retried.
fn reserve_shared_port() -> u16 {
	let udp = std::net::UdpSocket::bind("127.0.0.1:0").expect("reserve a UDP port");
	let port = udp.local_addr().unwrap().port();
	std::net::TcpListener::bind(("127.0.0.1", port))
		.unwrap_or_else(|error| panic!("TCP port {port} is taken beside its UDP port: {error}"));
	port
}

fn reserve_tcp_port() -> u16 {
	std::net::TcpListener::bind("127.0.0.1:0")
		.expect("reserve a TCP port")
		.local_addr()
		.unwrap()
		.port()
}

struct Pki {
	node_ca: PathBuf,
	operator_ca: PathBuf,
	operator_cert: PathBuf,
	operator_key: PathBuf,
	storage_cert: PathBuf,
	storage_key: PathBuf,
}

/// Which credential a client presents.
#[derive(Clone, Copy)]
enum Credential {
	/// Inspection and administration. Never a data request.
	Operator,
	/// The application's data access, confined to its tenant.
	Storage,
}

/// A node CA, an operator CA, a `data_node` certificate for each node (SAN
/// `node-<id>.<domain>`, which is where a peer reads the node id) and one operator certificate.
fn write_pki(dir: &Path) -> Pki {
	std::fs::create_dir_all(dir).unwrap();
	let node_ca_pem = tls_pki::generate_ca("smoke-node-ca").unwrap();
	let operator_ca_pem = tls_pki::generate_ca("smoke-operator-ca").unwrap();
	let node_ca = tls_pki::load_ca(&node_ca_pem.cert, node_ca_pem.key.as_deref().unwrap()).unwrap();
	let operator_ca = tls_pki::load_ca(
		&operator_ca_pem.cert,
		operator_ca_pem.key.as_deref().unwrap(),
	)
	.unwrap();
	std::fs::write(dir.join("node-ca.crt"), &node_ca_pem.cert).unwrap();
	std::fs::write(dir.join("operator-ca.crt"), &operator_ca_pem.cert).unwrap();
	for id in NODES {
		let leaf = tls_pki::claimed_leaf(
			&format!("smoke-node-{id}"),
			Some(format!("node-{id}.{DOMAIN}")),
			"data_node",
			None,
			id.to_le_bytes().to_vec(),
		)
		.unwrap();
		std::fs::write(
			dir.join(format!("node-{id}.crt")),
			leaf.serialize_pem_with_signer(&node_ca).unwrap(),
		)
		.unwrap();
		write_private(
			&dir.join(format!("node-{id}.key")),
			leaf.serialize_private_key_pem().as_bytes(),
		);
	}
	let operator =
		tls_pki::issue_operator(&operator_ca, "smoke-operator", b"smoke-operator").unwrap();
	std::fs::write(dir.join("operator.crt"), &operator.cert).unwrap();
	write_private(&dir.join("operator.key"), operator.key.unwrap().as_bytes());
	// The issuer validates the tenant (zero is refused) and writes the tenant claim.
	assert!(!NODES.contains(&STORAGE_NODE_INDEX));
	let storage =
		tls_pki::issue_storage(&node_ca, &TENANT.to_string(), DOMAIN, STORAGE_NODE_INDEX).unwrap();
	std::fs::write(dir.join("storage.crt"), &storage.cert).unwrap();
	write_private(&dir.join("storage.key"), storage.key.unwrap().as_bytes());
	Pki {
		node_ca: dir.join("node-ca.crt"),
		operator_ca: dir.join("operator-ca.crt"),
		operator_cert: dir.join("operator.crt"),
		operator_key: dir.join("operator.key"),
		storage_cert: dir.join("storage.crt"),
		storage_key: dir.join("storage.key"),
	}
}

struct Secrets {
	lease: [u8; 32],
	transfer: [u8; 32],
	audit: [u8; 32],
	decision: [u8; 32],
}

struct Member {
	id: u32,
	config: PathBuf,
	address: String,
	ready: String,
	log: PathBuf,
}

/// This run's daemons. Dropping it kills every process group it started that is still alive,
/// so a panic leaves nothing behind. It cannot act if this process is killed: the external
/// owner covers that.
struct Daemons {
	children: Vec<(u32, Child)>,
}

impl Daemons {
	fn start(&mut self, member: &Member) {
		let log = std::fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(&member.log)
			.unwrap();
		let child = Command::new(nilket_bin())
			.arg("wendelon")
			.arg(&member.config)
			.stdin(Stdio::null())
			.stdout(log.try_clone().unwrap())
			.stderr(log)
			.process_group(0)
			.spawn()
			.unwrap_or_else(|error| panic!("spawn node {}: {error}", member.id));
		self.children.push((member.id, child));
	}

	/// An orderly stop of one node: SIGTERM, then its exit within the bound with status 0. A
	/// node that has to be killed, or exits non-zero, fails the step.
	fn stop(&mut self, id: u32, deadline: Instant) {
		let at = self
			.children
			.iter()
			.position(|(node, _)| *node == id)
			.unwrap_or_else(|| panic!("node {id} is not running"));
		let (_, mut child) = self.children.remove(at);
		assert!(signal(child.id(), libc::SIGTERM), "signal node {id}");
		let bound =
			Instant::now() + STOP_BOUND.min(deadline.saturating_duration_since(Instant::now()));
		loop {
			match child.try_wait().expect("wait for the daemon") {
				Some(status) => {
					assert!(status.success(), "node {id} exited {status} on SIGTERM");
					return;
				}
				None if Instant::now() >= bound => {
					kill_group(child.id());
					let _ = child.wait();
					panic!("node {id} did not stop on SIGTERM within its bound and was killed");
				}
				None => std::thread::sleep(Duration::from_millis(50)),
			}
		}
	}
}

impl Drop for Daemons {
	fn drop(&mut self) {
		for (_, child) in &mut self.children {
			if child.try_wait().ok().flatten().is_none() {
				kill_group(child.id());
				let _ = child.wait();
			}
		}
	}
}

/// What one node's `/ready` said, as `nilket wendelon inspect --json` printed it.
struct Inspection {
	/// The command exited 0: the listener answered 200.
	ready: bool,
	/// The parsed body, when stdout was one JSON object. Stderr is never parsed.
	body: Option<serde_json::Value>,
	/// Exit, stdout and stderr, for a failure message only.
	raw: String,
}

impl Inspection {
	fn node_ids(&self, field: &str) -> Option<Vec<u32>> {
		self.body
			.as_ref()?
			.get(field)?
			.as_array()?
			.iter()
			.map(|member| node_id_of(member.as_str()?))
			.collect()
	}

	/// The leader this node names, if it names one of this cluster's nodes.
	fn leader(&self) -> Option<u32> {
		node_id_of(self.body.as_ref()?.get("leader_id")?.as_str()?).filter(|id| NODES.contains(id))
	}
}

/// `n-01` is node 1.
fn node_id_of(text: &str) -> Option<u32> {
	text.strip_prefix("n-")?.parse().ok()
}

/// `nilket wendelon inspect --json` against one node's ready listener, as an operator. The
/// child is killed if it has not exited within its bound, and then counts as not ready.
fn inspect(member: &Member, pki: &Pki) -> Inspection {
	let mut child = Command::new(nilket_bin())
		.args([
			"wendelon",
			"inspect",
			"--endpoint",
			&member.ready,
			"--server-name",
		])
		.arg(format!("node-{}.{DOMAIN}", member.id))
		.arg("--cert")
		.arg(&pki.operator_cert)
		.arg("--key")
		.arg(&pki.operator_key)
		.arg("--ca")
		.arg(&pki.node_ca)
		.arg("--json")
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("run nilket wendelon inspect");
	let bound = Instant::now() + INSPECT_BOUND;
	let status = loop {
		match child.try_wait().expect("wait for inspect") {
			Some(status) => break Some(status),
			None if Instant::now() >= bound => {
				let _ = child.kill();
				let _ = child.wait();
				break None;
			}
			None => std::thread::sleep(Duration::from_millis(20)),
		}
	};
	let (mut stdout, mut stderr) = (String::new(), String::new());
	if let Some(mut pipe) = child.stdout.take() {
		let _ = pipe.read_to_string(&mut stdout);
	}
	if let Some(mut pipe) = child.stderr.take() {
		let _ = pipe.read_to_string(&mut stderr);
	}
	Inspection {
		ready: status.is_some_and(|status| status.success()),
		body: serde_json::from_str(stdout.trim()).ok(),
		raw: format!(
			"exit {status:?}; stdout: {}; stderr: {}",
			stdout.trim(),
			stderr.trim()
		),
	}
}

/// Waits until `inspect` reports the node ready with exactly the three epoch peers.
fn wait_ready(member: &Member, pki: &Pki, daemons: &mut Daemons, deadline: Instant, what: &str) {
	let bound =
		Instant::now() + READY_BOUND.min(deadline.saturating_duration_since(Instant::now()));
	loop {
		for (id, child) in &mut daemons.children {
			if let Some(status) = child.try_wait().expect("poll a daemon") {
				panic!(
					"{what}: node {id} exited {status} while waiting for node {}",
					member.id
				);
			}
		}
		let inspection = inspect(member, pki);
		if inspection.ready && inspection.node_ids("cluster_members") == Some(NODES.to_vec()) {
			return;
		}
		assert!(
			Instant::now() < bound,
			"{what}: node {} is not ready with three members; last answer: {}",
			member.id,
			inspection.raw
		);
		std::thread::sleep(Duration::from_millis(500));
	}
}

/// One `nilket wendelon tenant …` command as the operator, through `member`. The child is
/// killed past its bound. Returns whether it exited 0, with its output for a failure message.
fn tenant_command(member: &Member, pki: &Pki, arguments: &[&str]) -> (bool, String) {
	let mut child = Command::new(nilket_bin())
		.args(["wendelon", "tenant"])
		.args(arguments)
		.arg("--bootstrap")
		.arg(format!("{}@{}", member.id, member.address))
		.arg("--cert")
		.arg(&pki.operator_cert)
		.arg("--key")
		.arg(&pki.operator_key)
		.arg("--ca")
		.arg(&pki.node_ca)
		.args(["--cluster-domain", DOMAIN])
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("run nilket wendelon tenant");
	let bound = Instant::now() + CLIENT_BOUND;
	let status = loop {
		match child.try_wait().expect("wait for the tenant command") {
			Some(status) => break Some(status),
			None if Instant::now() >= bound => {
				let _ = child.kill();
				let _ = child.wait();
				break None;
			}
			None => std::thread::sleep(Duration::from_millis(20)),
		}
	};
	let (mut stdout, mut stderr) = (String::new(), String::new());
	if let Some(mut pipe) = child.stdout.take() {
		let _ = pipe.read_to_string(&mut stdout);
	}
	if let Some(mut pipe) = child.stderr.take() {
		let _ = pipe.read_to_string(&mut stderr);
	}
	(
		status.is_some_and(|status| status.success()),
		format!(
			"exit {status:?}; stdout: {}; stderr: {}",
			stdout.trim(),
			stderr.trim()
		),
	)
}

/// The time one client step may take: its own bound, or what is left of the test's.
fn step(deadline: Instant) -> Duration {
	let left = deadline.saturating_duration_since(Instant::now());
	assert!(!left.is_zero(), "the test's bound was reached");
	CLIENT_BOUND.min(left)
}

fn descriptor() -> TableSchemaDescriptor {
	TableSchemaDescriptor::new(
		TableObjectId::new([0x21; 16]),
		1,
		"smoke",
		"rows",
		&Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
		]),
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptor::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicy::AdditiveOnly,
	)
	.unwrap()
}

async fn client_through(
	member: &Member,
	pki: &Pki,
	credential: Credential,
	deadline: Instant,
) -> Client {
	let (cert_path, key_path) = match credential {
		Credential::Operator => (pki.operator_cert.clone(), pki.operator_key.clone()),
		Credential::Storage => (pki.storage_cert.clone(), pki.storage_key.clone()),
	};
	let mut config = ClientConfig::new(
		"127.0.0.1:0".parse().unwrap(),
		vec![BootstrapPeer {
			node_id: member.id,
			address: member.address.clone(),
		}],
		TlsPaths {
			cert_path,
			key_path,
			ca_path: pki.node_ca.clone(),
		},
	);
	config.cluster_domain = DOMAIN.into();
	tokio::time::timeout(step(deadline), Client::connect_quic(config))
		.await
		.unwrap_or_else(|_| panic!("the client did not connect through node {}", member.id))
		.unwrap_or_else(|error| panic!("the client through node {}: {error:?}", member.id))
}

/// The one row, read with ONE LINEARIZABLE streamed table query, bounded by its own deadline
/// and by the wait around it. Any error fails the test; nothing is asked again.
async fn expect_the_row(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	deadline: Instant,
	what: &str,
) {
	let bound = step(deadline);
	let options = TableQueryOptions {
		deadline: Some(Instant::now() + bound),
		..TableQueryOptions::default()
	};
	let result = tokio::time::timeout(
		bound,
		client.table_query(TenantId::new(TENANT), descriptor, Vec::new(), options),
	)
	.await
	.unwrap_or_else(|_| panic!("{what}: the query had no answer within its bound"))
	.unwrap_or_else(|error| panic!("{what}: the query: {error:?}"));
	let mut rows = Vec::new();
	for batch in &result.batches {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.expect("an id column");
		let names = batch
			.column_by_name("name")
			.and_then(|column| column.as_any().downcast_ref::<LargeStringArray>())
			.expect("a name column");
		for row in 0..batch.num_rows() {
			rows.push((ids.value(row), names.value(row).to_owned()));
		}
	}
	assert_eq!(rows, vec![(7, "seven".to_owned())], "{what}");
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons; run alone under an external owner (see the module text)"]
async fn three_daemons_from_configuration_files_form_a_parent_decisions_cluster_and_serve_a_client()
{
	let deadline = Instant::now() + TEST_BOUND;
	// Kept on failure for its logs; removed only at the end of a pass. Holds secrets: it is
	// created private and is never printed.
	let root = std::env::temp_dir().join(format!("nilket-pd-smoke-{}", std::process::id()));
	std::fs::create_dir_all(&root).unwrap();
	std::fs::set_permissions(&root, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
	eprintln!("smoke: run directory {}", root.display());
	let pki = write_pki(&root.join("pki"));

	let secrets: Vec<Secrets> = NODES
		.iter()
		.map(|_| Secrets {
			lease: random32(),
			transfer: random32(),
			audit: random32(),
			decision: random32(),
		})
		.collect();
	let mut trusted = [String::new(), String::new(), String::new(), String::new()];
	for (id, secret) in NODES.iter().zip(&secrets) {
		let keys = [
			wendelon::network::lease_verifying_key_bytes(secret.lease),
			wendelon::network::transfer_verifying_key_bytes(secret.transfer),
			wendelon::network::audit_root_verifying_key_bytes(secret.audit),
			wendelon::consensus::decisions::decision_verifying_key_bytes(secret.decision),
		];
		for (map, key) in trusted.iter_mut().zip(keys) {
			writeln!(map, "\"{id}\" = \"{}\"", hex(key)).unwrap();
		}
	}
	let ports: Vec<u16> = NODES.iter().map(|_| reserve_shared_port()).collect();
	let seed = format!("127.0.0.1:{}", ports[0]);
	let mut members = Vec::new();
	// ADR 0726: one root KEK per cluster. Every member is given the same unlock
	// secret; the sealed root itself is copied from the forming member below.
	let unlock_secret = random32();
	for ((id, secret), port) in NODES.iter().zip(&secrets).zip(&ports) {
		let dir = root.join(format!("node-{id}"));
		std::fs::create_dir_all(dir.join("data")).unwrap();
		write_private(&dir.join("kek-unlock"), &unlock_secret);
		let (health, ready, metrics) = (reserve_tcp_port(), reserve_tcp_port(), reserve_tcp_port());
		let cluster = if *id == NODES[0] {
			"mode = \"bootstrap\"".to_owned()
		} else {
			format!(
				"mode = \"join\"\nseed_nodes = [{{ node_id = {}, address = \"{seed}\" }}]",
				NODES[0]
			)
		};
		let pki_dir = root.join("pki");
		let body = format!(
			r#"[node]
node_id = {id}
advertise_address = "127.0.0.1:{port}"

[cluster]
{cluster}
decision_authority = "parent-decisions"

[storage]
data_dir = "data"

[tuning]
replication_factor = 3
storage_shutdown_timeout_seconds = 5

[time]
mvcc_retention_window_secs = 0

[identity]
allow_dev_keys = false
lease_signing_secret = "{lease}"
transfer_signing_secret = "{transfer}"
audit_root_signing_secret = "{audit}"
decision_signing_secret = "{decision}"

[identity.trusted_lease_signers]
{trusted_lease}
[identity.trusted_transfer_signers]
{trusted_transfer}
[identity.trusted_audit_root_keys]
{trusted_audit}
[identity.trusted_decision_signers]
{trusted_decision}
[crypto]
root_kek_provider = {{ kind = "local-sealed", unlock_secret_file = "kek-unlock" }}

[transport.tls]
cert_path = "{pki}/node-{id}.crt"
key_path = "{pki}/node-{id}.key"
node_ca_path = "{node_ca}"
operator_ca_path = "{operator_ca}"
cluster_domain = "{DOMAIN}"

[observability.listeners]
health = "127.0.0.1:{health}"
ready = "127.0.0.1:{ready}"
metrics = "127.0.0.1:{metrics}"
"#,
			lease = hex(secret.lease),
			transfer = hex(secret.transfer),
			audit = hex(secret.audit),
			decision = hex(secret.decision),
			trusted_lease = trusted[0],
			trusted_transfer = trusted[1],
			trusted_audit = trusted[2],
			trusted_decision = trusted[3],
			pki = pki_dir.display(),
			node_ca = pki.node_ca.display(),
			operator_ca = pki.operator_ca.display(),
		);
		write_private(&dir.join("wendelon.toml"), body.as_bytes());
		members.push(Member {
			id: *id,
			config: dir.join("wendelon.toml"),
			address: format!("127.0.0.1:{port}"),
			ready: format!("127.0.0.1:{ready}"),
			log: dir.join("daemon.log"),
		});
	}

	let mut daemons = Daemons {
		children: Vec::new(),
	};
	// Form the cluster before any write: ADR 0720 fixes the membership at the first one.
	// The enrollment procedure of ADR 0726, as an operator performs it: start the
	// forming member, copy its sealed root KEK file, owner-only, to the same path in
	// each other member's empty data directory, then start those members in join mode.
	daemons.start(&members[0]);
	let sealed_root = Path::new(".wendelon").join("local-sealed-root-kek.bin");
	let forming_root = root
		.join(format!("node-{}", NODES[0]))
		.join("data")
		.join(&sealed_root);
	let sealed = loop {
		// The file is created and written in one step; a complete one is 104 bytes.
		match std::fs::read(&forming_root) {
			Ok(bytes) if bytes.len() == 104 => break bytes,
			_ => {}
		}
		assert!(
			step(deadline) > Duration::ZERO,
			"the forming member never wrote its sealed root KEK"
		);
		std::thread::sleep(Duration::from_millis(50));
	};
	for id in &NODES[1..] {
		let target = root
			.join(format!("node-{id}"))
			.join("data")
			.join(&sealed_root);
		std::fs::create_dir_all(target.parent().unwrap()).unwrap();
		write_private(&target, &sealed);
	}
	daemons.start(&members[1]);
	daemons.start(&members[2]);
	for member in &members {
		wait_ready(member, &pki, &mut daemons, deadline, "formation");
	}

	// The operator creates the application's tenant with the cluster's replication factor,
	// through the public command, once. A failure is this step's result.
	assert!(step(deadline) > Duration::ZERO);
	let tenant = TENANT.to_string();
	let (created, output) = tenant_command(
		&members[0],
		&pki,
		&["create", "--tenant", &tenant, "--replication-factor", "3"],
	);
	assert!(created, "tenant create: {output}");
	// Every node answers for the tenant before any data request is made. This is one
	// read-only command per node, made once each: it shows the control answer, not that a
	// node's data path has refreshed its view of the tenant.
	for member in &members {
		let (found, output) = tenant_command(member, &pki, &["get", "--tenant", &tenant]);
		assert!(found, "tenant get through node {}: {output}", member.id);
	}

	let descriptor = descriptor();
	// The operator's client administers: it submits the schema update and nothing else.
	let operator = client_through(&members[0], &pki, Credential::Operator, deadline).await;
	tokio::time::timeout(
		step(deadline),
		operator.submit_table_schema_update(TableSchemaUpdateRequest::create(
			TenantId::new(TENANT),
			descriptor.clone(),
			SchemaIntentId::new([0x21; 16]),
		)),
	)
	.await
	.expect("the schema update had no answer within its bound")
	.unwrap_or_else(|error| panic!("the schema update: {error:?}"));

	let row = RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
		])),
		vec![
			Arc::new(Int64Array::from(vec![7])),
			Arc::new(LargeStringArray::from(vec!["seven"])),
		],
	)
	.unwrap();
	// The application's client: the storage credential, confined to its tenant.
	let client = client_through(&members[0], &pki, Credential::Storage, deadline).await;
	// ONE call. Its retries, if any, are the client's own under one operation identity; an
	// error is the result of this step, not a reason to call again.
	let bound = step(deadline);
	let options = WriteOptions {
		deadline: Some(Instant::now() + bound),
		..WriteOptions::default()
	};
	tokio::time::timeout(
		bound,
		client.table_write(TenantId::new(TENANT), &descriptor, row, options),
	)
	.await
	.expect("the write had no answer within its bound")
	.unwrap_or_else(|error| panic!("the write: {error:?}"));

	for member in &members {
		let through = client_through(member, &pki, Credential::Storage, deadline).await;
		expect_the_row(
			&through,
			&descriptor,
			deadline,
			&format!("after the write, discovered through node {}", member.id),
		)
		.await;
	}

	// Stop a follower in an orderly way, start it again from the same file, and read again.
	// The follower is chosen positively: node 1 must name a leader that is one of the three,
	// and the node restarted is one that is not it.
	let inspection = inspect(&members[0], &pki);
	assert!(
		inspection.ready,
		"node 1 stopped being ready: {}",
		inspection.raw
	);
	let leader = inspection
		.leader()
		.unwrap_or_else(|| panic!("node 1 names no leader among the three: {}", inspection.raw));
	let follower = members
		.iter()
		.rev()
		.find(|member| member.id != leader)
		.expect("a node that is not the leader");
	eprintln!(
		"smoke: leader {leader}; restarting follower {}",
		follower.id
	);
	daemons.stop(follower.id, deadline);
	daemons.start(follower);
	for member in &members {
		wait_ready(
			member,
			&pki,
			&mut daemons,
			deadline,
			"after the follower restart",
		);
	}
	for member in &members {
		let through = client_through(member, &pki, Credential::Storage, deadline).await;
		expect_the_row(
			&through,
			&descriptor,
			deadline,
			&format!("after the restart, discovered through node {}", member.id),
		)
		.await;
	}

	// Orderly stop of all three. A forced kill fails the test.
	for id in NODES {
		daemons.stop(id, deadline);
	}
	assert!(Instant::now() < deadline, "the test overran its bound");
	std::fs::remove_dir_all(&root).unwrap();
}
