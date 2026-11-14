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
//! - Every client call is made once, with one exception, the takeover stage's one write (below).
//!   Everywhere else any error, at any step, fails the test: nothing is asked again, so no
//!   transport, authorisation or decode failure can be retried into a pass.
//!
//! - ADR 0729, the last stage: a range's coordinating node is stopped and a write to that range
//!   is answered by another. Which node coordinates a range is read from an actual committed
//!   write: the receipt's commit HLC carries the node id of the coordinator that stamped it,
//!   which is the holder of the row's range at that commit. Rows are written until one is
//!   coordinated by a node that is not the control leader; that process is stopped in an
//!   orderly way; the SAME row id (so the same range) is written again through the two live
//!   processes, and its receipt must name a live node. This stage alone repeats a call: the
//!   public client returns a stall as an error, so the one write is offered again, with the
//!   SAME operation identity, until its one explicit bound. It is one logical operation. Only
//!   the answers `offers_the_same_operation_again` names are offered again (a failed
//!   connection, a redirect, this operation's own stall or exhausted attempts); any other
//!   answer, and the bound itself, ends the test. What it shows: with a non-leader
//!   range holder stopped, real processes move that range's authority and commit a write to it.
//!   What it does not: a leader failure, a partition, or how long the takeover takes in general.
//!
//! Not covered by that test: an object store, tiering, a leader failure, more than one host.
//!
//! The object-store test (`three_daemons_offload_to_the_object_store_…`) needs a remote store:
//! `WENDELON_RUSTFS_ENDPOINT`, `WENDELON_RUSTFS_BUCKET`, `WENDELON_RUSTFS_ACCESS_KEY_FILE`,
//! `WENDELON_RUSTFS_SECRET_KEY_FILE` (key FILES, so no secret is in the environment) and
//! optionally `WENDELON_RUSTFS_REGION`. It writes under a new prefix of its own in that bucket
//! and deletes nothing there. With `WENDELON_SMOKE_RECORD_DIR` set, a pass copies each daemon's
//! log there before the run directory goes. What it shows and does not is in its own text.

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
use wendelon::storage::{
	ObjectStore, ObjectStoreConfig, RemoteObjectStoreConfig, RemoteObjectStoreProvider,
	S3CompatibleProviderConfig,
};
use wendelon_client::{
	BootstrapPeer, Client, ClientConfig, ClientError, TableQueryOptions, TableSchemaUpdateRequest,
	TlsPaths, WriteOptions,
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

/// The object-store test's whole bound, its offload wait, and each judged query: the caller's
/// deadline, and a separate grace the test waits past it only to collect the answer.
const TIERING_TEST_BOUND: Duration = Duration::from_secs(600);
const TIER_BOUND: Duration = Duration::from_secs(150);
const QUERY_DEADLINE: Duration = Duration::from_secs(30);
const QUERY_WATCHDOG_GRACE: Duration = Duration::from_secs(10);
/// Every node's `[tuning] query_stall_bound_millis` in the object-store test: the DECLARED
/// profile of this test for the development RustFS store, 2000 ms. It is not the shipped
/// default, which stays 250 ms, and it is not a recommendation for any deployment. At the
/// default this test's judged queries ended QUERY_STALLED in three of five (runs
/// pd-offload-20261113T235743Z, 20261114T002030Z, 20261114T005124Z; DC-223); those results
/// stand. A pass at this value says the scenario passed once at this declared setting.
const QUERY_STALL_BOUND_MILLIS: u64 = 2000;
/// A diagnostic stress shape, not deployment sizing: one row of about 4 KiB that does not
/// compress fills an 8 KiB slab, so every row is a slab and an object of its own.
const ROWS: i64 = 64;
const NAME_HEX_BLOCKS: usize = 64;
const SLAB_MAX_BYTES: u64 = 8 * 1024;
/// How many distinct slabs each node must have in the store and off its own disk before a
/// judged query. `ROWS` rows seal 62 or 63 slabs (one is still being written; how the rows pack
/// has varied by one between runs), so the bound is `ROWS - 2`.
const OFFLOADED_PER_OWNER: usize = ROWS as usize - 2;

fn required(name: &str) -> String {
	std::env::var(name).unwrap_or_else(|_| panic!("{name} is not set"))
}

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

/// The remote object store the daemons tier to, from the environment (key FILES, so no secret
/// is in the environment), with this run's own new prefix in the bucket. The daemons reach it
/// directly; nothing relays or observes their requests here.
struct StoreEnv {
	endpoint: String,
	bucket: String,
	region: String,
	access_key_file: PathBuf,
	secret_key_file: PathBuf,
	prefix: String,
}

impl StoreEnv {
	fn from_environment() -> Self {
		Self {
			endpoint: required("WENDELON_RUSTFS_ENDPOINT"),
			bucket: required("WENDELON_RUSTFS_BUCKET"),
			region: std::env::var("WENDELON_RUSTFS_REGION")
				.unwrap_or_else(|_| "us-east-1".to_owned()),
			access_key_file: required("WENDELON_RUSTFS_ACCESS_KEY_FILE").into(),
			secret_key_file: required("WENDELON_RUSTFS_SECRET_KEY_FILE").into(),
			prefix: format!("pd-smoke/{}", &hex(random32())[..16]),
		}
	}

	fn key(path: &Path) -> String {
		let text = std::fs::read_to_string(path)
			.unwrap_or_else(|error| panic!("cannot read {}: {}", path.display(), error.kind()));
		let key = text.trim().to_owned();
		assert!(!key.is_empty(), "{} is empty", path.display());
		key
	}

	/// This test's own handle on the run's prefix: it lists, and never writes or deletes.
	fn handle(&self) -> Arc<dyn ObjectStore> {
		ObjectStoreConfig::Remote(RemoteObjectStoreConfig {
			bucket: self.bucket.clone(),
			root: self.prefix.clone(),
			provider: RemoteObjectStoreProvider::S3Compatible(S3CompatibleProviderConfig {
				endpoint: self.endpoint.clone(),
				region: self.region.clone(),
				access_key_id: Some(Self::key(&self.access_key_file)),
				secret_access_key: Some(Self::key(&self.secret_key_file)),
				session_token: None,
				allow_anonymous: false,
				virtual_host_style: false,
			}),
		})
		.build()
		.expect("the object store handle")
	}
}

struct Member {
	id: u32,
	config: PathBuf,
	address: String,
	ready: String,
	log: PathBuf,
}

/// Other hosts: with `WENDELON_SMOKE_NODE_CONTROL=<executable>` this test starts no daemon
/// itself. Each node's directory (its configuration, certificates, keys and empty data
/// directory, with nothing in its configuration pointing outside it) is handed to the
/// executable once, and every later per-node action goes through it, each call bounded here
/// and killed past its bound: `prepare <id> <dir>`, `start <id>`, `stop <id> <seconds>` (exit 0 only for
/// an orderly stop with status 0 within that many seconds), `state <id>` (stdout `running`, or anything else),
/// `inspect <id>` (`nilket wendelon inspect --json` on that host), `inventory <id>` (one
/// `<bytes><TAB><path>` line per file of the node's data directory, then `END<TAB><count>`;
/// it must fail on a symbolic link or on anything it cannot read), `sealed-root <id>` (the
/// forming node's sealed root KEK file, raw), `cleanup <id>` (ends the node's owner if it is
/// still there; removes no file). `WENDELON_SMOKE_NODE_ADDRESSES`
/// (`1=host:port,2=host:port,3=host:port`) gives the advertised addresses; each node's
/// health, ready and metrics listeners are on its own loopback at that port plus 10, 11, 12.
fn node_control() -> Option<PathBuf> {
	std::env::var_os("WENDELON_SMOKE_NODE_CONTROL")
		.filter(|path| !path.is_empty())
		.map(PathBuf::from)
}

/// A node-control call's bound, beyond whatever the action itself is given.
const CONTROL_BOUND: Duration = Duration::from_secs(30);

/// One node-control call: whether it succeeded, its stdout, and exit, stdout and stderr as
/// text for a failure message. ONE bound covers the command, everything it started, and the
/// reading of both its streams: the command runs in a process group of its own; at the bound
/// the whole group is killed; a stream that is still open after that (or that could not be
/// read) fails the call, and nothing waits for it. Success needs exit 0 AND both streams read
/// to their end within the bound, so a cut-off output is never taken for a whole one.
fn node_control_call(
	control: &Path,
	verb: &str,
	id: u32,
	argument: Option<&std::ffi::OsStr>,
	bound: Duration,
) -> (bool, Vec<u8>, String) {
	let mut command = Command::new(control);
	command.arg(verb).arg(id.to_string());
	if let Some(argument) = argument {
		command.arg(argument);
	}
	let mut child = command
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.process_group(0)
		.spawn()
		.unwrap_or_else(|error| panic!("run node-control {verb} {id}: {error}"));
	let group = child.id();
	// Each stream is read to its end on a thread of its own, which reports once.
	let reader = |pipe: Option<Box<dyn std::io::Read + Send>>| {
		let (sender, receiver) = std::sync::mpsc::channel();
		std::thread::spawn(move || {
			let mut bytes = Vec::new();
			let read = match pipe {
				Some(mut pipe) => pipe.read_to_end(&mut bytes).map(|_| bytes),
				None => Ok(bytes),
			};
			let _ = sender.send(read);
		});
		receiver
	};
	let stdout = reader(child.stdout.take().map(|pipe| Box::new(pipe) as _));
	let stderr = reader(child.stderr.take().map(|pipe| Box::new(pipe) as _));
	let end = Instant::now() + bound;
	let status = loop {
		match child.try_wait().expect("wait for node-control") {
			Some(status) => break Some(status),
			None if Instant::now() >= end => break None,
			None => std::thread::sleep(Duration::from_millis(20)),
		}
	};
	// The streams, within what is left of the same bound. A descendant that keeps one open
	// past it is killed with the group; a stream still open after that is abandoned, failed.
	let left = |end: Instant| end.saturating_duration_since(Instant::now());
	let mut out = stdout.recv_timeout(left(end));
	let mut err = stderr.recv_timeout(left(end));
	let cut = status.is_none() || out.is_err() || err.is_err();
	if cut {
		kill_group(group);
		let _ = child.wait();
		let grace = Instant::now() + Duration::from_secs(2);
		if out.is_err() {
			out = stdout.recv_timeout(left(grace));
		}
		if err.is_err() {
			err = stderr.recv_timeout(left(grace));
		}
	}
	let text =
		|read: &Result<std::io::Result<Vec<u8>>, std::sync::mpsc::RecvTimeoutError>| match read {
			Ok(Ok(bytes)) => String::from_utf8_lossy(bytes).trim().to_owned(),
			Ok(Err(error)) => format!("<read failed: {error}>"),
			Err(_) => "<still open after the bound and the kill>".to_owned(),
		};
	let raw = format!(
		"node-control {verb} {id}: exit {status:?}{}; stdout: {}; stderr: {}",
		if cut { " (CUT at its bound)" } else { "" },
		text(&out),
		text(&err)
	);
	let whole = !cut && matches!(err, Ok(Ok(_)));
	match out {
		Ok(Ok(bytes)) if whole && status.is_some_and(|status| status.success()) => {
			(true, bytes, raw)
		}
		Ok(Ok(bytes)) => (false, bytes, raw),
		_ => (false, Vec::new(), raw),
	}
}

#[test]
fn a_node_control_call_is_bounded_with_everything_it_started() {
	use std::os::unix::fs::PermissionsExt as _;
	let dir = std::env::temp_dir().join(format!("nilket-pd-control-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&dir);
	std::fs::create_dir_all(&dir).unwrap();
	let script = |name: &str, body: &str| {
		let path = dir.join(name);
		std::fs::write(&path, format!("#!/bin/bash\n{body}\n")).unwrap();
		std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o700)).unwrap();
		path
	};
	// Whole: exit 0 and its output.
	let (ok, stdout, raw) = node_control_call(
		&script("whole", "echo \"$1 $2 $3\""),
		"verb",
		7,
		Some(std::ffi::OsStr::new("extra")),
		Duration::from_secs(5),
	);
	assert!(ok, "{raw}");
	assert_eq!(stdout, b"verb 7 extra\n");
	// A non-zero exit is a failure with its output kept for the message.
	let (ok, _, raw) = node_control_call(
		&script("fails", "echo said; exit 3"),
		"verb",
		7,
		None,
		Duration::from_secs(5),
	);
	assert!(!ok && raw.contains("said"), "{raw}");
	// The command exits at once and leaves a descendant holding its output open for far
	// longer than the bound: the call ends at the bound, failed, and does not wait for it.
	let started = Instant::now();
	let (ok, _, raw) = node_control_call(
		&script("lingers", "echo early; sleep 30 &\nexit 0"),
		"verb",
		7,
		None,
		Duration::from_millis(500),
	);
	let took = started.elapsed();
	assert!(!ok, "a cut-off stream was taken for a whole one: {raw}");
	assert!(raw.contains("CUT at its bound"), "{raw}");
	assert!(
		took < Duration::from_secs(5),
		"the call outlived its bound: {took:?}"
	);
	// The command itself never ends: killed at the bound.
	let started = Instant::now();
	let (ok, _, raw) = node_control_call(
		&script("hangs", "sleep 30"),
		"verb",
		7,
		None,
		Duration::from_millis(300),
	);
	assert!(!ok && raw.contains("exit None"), "{raw}");
	assert!(started.elapsed() < Duration::from_secs(5));
	std::fs::remove_dir_all(&dir).unwrap();
}

/// This run's daemons: children of this test, or daemons on other hosts behind the node-control
/// executable. Dropping it kills every local process group still alive and asks each host to
/// end its node's owner, so a panic leaves no daemon behind. It cannot act if this process is
/// killed: the external owners (here, and on each host) cover that.
struct Daemons {
	children: Vec<(u32, Child)>,
	/// Other hosts only: the nodes handed to their host, and those started and not stopped.
	prepared: Vec<u32>,
	hosted: Vec<u32>,
}

impl Daemons {
	/// Other hosts only: the host receives the node's directory, once.
	fn prepare(&mut self, member: &Member) {
		let Some(control) = node_control() else {
			return;
		};
		self.prepared.push(member.id);
		let dir = member.config.parent().unwrap();
		let (ok, _, raw) = node_control_call(
			&control,
			"prepare",
			member.id,
			Some(dir.as_os_str()),
			CONTROL_BOUND,
		);
		assert!(ok, "{raw}");
	}

	/// A daemon that should be running and is not, with what is known of why.
	fn exited(&mut self) -> Option<String> {
		if let Some(control) = node_control() {
			for id in &self.hosted {
				let (ok, stdout, raw) =
					node_control_call(&control, "state", *id, None, CONTROL_BOUND);
				if !ok || String::from_utf8_lossy(&stdout).trim() != "running" {
					return Some(format!("node {id} is not running: {raw}"));
				}
			}
			return None;
		}
		for (id, child) in &mut self.children {
			if let Some(status) = child.try_wait().expect("poll a daemon") {
				return Some(format!("node {id} exited {status}"));
			}
		}
		None
	}

	fn start(&mut self, member: &Member) {
		if let Some(control) = node_control() {
			let (ok, _, raw) = node_control_call(&control, "start", member.id, None, CONTROL_BOUND);
			assert!(ok, "{raw}");
			self.hosted.push(member.id);
			return;
		}
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
		if let Some(control) = node_control() {
			let at = self
				.hosted
				.iter()
				.position(|node| *node == id)
				.unwrap_or_else(|| panic!("node {id} is not running"));
			self.hosted.remove(at);
			// The stop's own budget is what is left of STOP_BOUND, in whole seconds, and the
			// host is told it: a node that ends later than that has not stopped in order.
			let left = STOP_BOUND.min(deadline.saturating_duration_since(Instant::now()));
			let budget = left.as_secs().max(1).to_string();
			let (ok, _, raw) = node_control_call(
				&control,
				"stop",
				id,
				Some(std::ffi::OsStr::new(&budget)),
				left + CONTROL_BOUND,
			);
			eprintln!("smoke: {raw}");
			assert!(ok, "node {id} did not stop in order on SIGTERM: {raw}");
			return;
		}
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
		if let Some(control) = node_control() {
			// Every node handed to a host, started or not: its owner is ended there.
			for id in &self.prepared {
				let (ok, _, raw) = node_control_call(&control, "cleanup", *id, None, CONTROL_BOUND);
				if !ok {
					eprintln!("smoke: CLEANUP NOT CONFIRMED: {raw}");
				}
			}
		}
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
	if let Some(control) = node_control() {
		let (ok, stdout, raw) = node_control_call(
			&control,
			"inspect",
			member.id,
			None,
			INSPECT_BOUND + CONTROL_BOUND,
		);
		return Inspection {
			ready: ok,
			body: serde_json::from_slice(&stdout).ok(),
			raw,
		};
	}
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
		if let Some(exited) = daemons.exited() {
			panic!("{what}: {exited} while waiting for node {}", member.id);
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
	// Loopback beside local daemons; any address when the daemons are on other hosts.
	let bind = match node_control() {
		Some(_) => "0.0.0.0:0",
		None => "127.0.0.1:0",
	};
	let mut config = ClientConfig::new(
		bind.parse().unwrap(),
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

/// The one row of the first stages.
async fn expect_the_row(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	deadline: Instant,
	what: &str,
) {
	expect_rows(
		client,
		descriptor,
		deadline,
		what,
		&[(7, "seven".to_owned())],
	)
	.await;
}

fn one_row(id: i64, name: &str) -> RecordBatch {
	RecordBatch::try_new(
		Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("name", DataType::LargeUtf8, true),
		])),
		vec![
			Arc::new(Int64Array::from(vec![id])),
			Arc::new(LargeStringArray::from(vec![name])),
		],
	)
	.unwrap()
}

/// A fresh caller-minted operation identity, as the client mints its own.
fn operation_id() -> wendelon::schema::ClientWriteId {
	wendelon::schema::ClientWriteId {
		client_uuid: *uuid::Uuid::now_v7().as_bytes(),
		sequence_no: 0,
		idempotency_ttl_ms: 60_000,
		_pad: 0,
	}
}

/// ADR 0729, the takeover stage only: whether an answer to its one write means the range's
/// authority is unavailable for now, so the SAME operation may be offered again within its
/// one bound. Exactly: a failed connection to a node (the stopped one), a redirect, the
/// server's own stall of this operation, or the client's attempts for this operation running
/// out. Everything else ends the test: an expired or conflicting operation, an internal
/// error, a deadline, bad data or arguments, authorization, an answer about another operation.
fn offers_the_same_operation_again(
	error: &ClientError,
	identity: wendelon::schema::ClientWriteId,
) -> bool {
	use std::io::ErrorKind;
	use wendelon::schema::{MutationStopReason, WriteStatus};
	match error {
		ClientError::Io(io) => matches!(
			io.kind(),
			ErrorKind::ConnectionRefused
				| ErrorKind::ConnectionReset
				| ErrorKind::ConnectionAborted
				| ErrorKind::NotConnected
		),
		ClientError::WriteFailed { status } => *status == WriteStatus::NOT_LEADER,
		ClientError::MutationStopped {
			operation_id,
			status,
			reason,
			..
		} => {
			operation_id.client_uuid == identity.client_uuid
				&& operation_id.sequence_no == identity.sequence_no
				&& ((*status == WriteStatus::WRITE_STALLED && *reason == MutationStopReason::NONE)
					|| (*status == WriteStatus::WRITE_ATTEMPTS_EXHAUSTED
						&& *reason == MutationStopReason::ATTEMPT_EXHAUSTION))
		}
		_ => false,
	}
}

/// The retry rule's cases, by name. Runs in a default pass: it spawns nothing.
#[test]
fn the_takeover_stage_offers_its_operation_again_only_for_an_unavailable_range() {
	use std::io::{Error, ErrorKind};
	use wendelon::schema::{MutationCommitKnowledge, MutationStopReason, WriteStatus};
	let identity = operation_id();
	let other = operation_id();
	assert_ne!(identity.client_uuid, other.client_uuid);
	let stopped = |operation_id, status, reason, knowledge| ClientError::MutationStopped {
		operation_id,
		status,
		reason,
		knowledge,
	};
	let io = |kind: ErrorKind| ClientError::Io(Error::new(kind, "test"));
	let unknown = MutationCommitKnowledge::INDETERMINATE;
	let not_committed = MutationCommitKnowledge::KNOWN_NOT_COMMITTED;
	let cases: Vec<(&str, ClientError, bool)> = vec![
		("connection refused", io(ErrorKind::ConnectionRefused), true),
		("connection reset", io(ErrorKind::ConnectionReset), true),
		("connection aborted", io(ErrorKind::ConnectionAborted), true),
		("not connected", io(ErrorKind::NotConnected), true),
		("invalid data", io(ErrorKind::InvalidData), false),
		("invalid input", io(ErrorKind::InvalidInput), false),
		("timed out", io(ErrorKind::TimedOut), false),
		("unexpected eof", io(ErrorKind::UnexpectedEof), false),
		("permission denied", io(ErrorKind::PermissionDenied), false),
		("other", io(ErrorKind::Other), false),
		(
			"a redirect",
			ClientError::WriteFailed {
				status: WriteStatus::NOT_LEADER,
			},
			true,
		),
		(
			"an internal error",
			ClientError::WriteFailed {
				status: WriteStatus::INTERNAL_ERROR,
			},
			false,
		),
		(
			"a key conflict",
			ClientError::WriteFailed {
				status: WriteStatus::WRITE_KEY_CONFLICT,
			},
			false,
		),
		(
			"a stall surfaced as a bare failure",
			ClientError::WriteFailed {
				status: WriteStatus::WRITE_STALLED,
			},
			false,
		),
		(
			"this operation stalled, outcome indeterminate",
			stopped(
				identity,
				WriteStatus::WRITE_STALLED,
				MutationStopReason::NONE,
				unknown,
			),
			true,
		),
		(
			"this operation stalled, known not committed",
			stopped(
				identity,
				WriteStatus::WRITE_STALLED,
				MutationStopReason::NONE,
				not_committed,
			),
			true,
		),
		(
			"this operation's attempts ran out",
			stopped(
				identity,
				WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
				MutationStopReason::ATTEMPT_EXHAUSTION,
				unknown,
			),
			true,
		),
		(
			"a stall of another operation",
			stopped(
				other,
				WriteStatus::WRITE_STALLED,
				MutationStopReason::NONE,
				unknown,
			),
			false,
		),
		(
			"a stall that was the caller's deadline",
			stopped(
				identity,
				WriteStatus::WRITE_STALLED,
				MutationStopReason::DEADLINE,
				unknown,
			),
			false,
		),
		(
			"attempts exhausted with another reason",
			stopped(
				identity,
				WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
				MutationStopReason::NONE,
				unknown,
			),
			false,
		),
		(
			"this operation expired",
			stopped(
				identity,
				WriteStatus::WRITE_OPERATION_EXPIRED,
				MutationStopReason::NONE,
				unknown,
			),
			false,
		),
		(
			"this operation's deadline",
			stopped(
				identity,
				WriteStatus::WRITE_DEADLINE_EXCEEDED,
				MutationStopReason::DEADLINE,
				unknown,
			),
			false,
		),
		("authorization", ClientError::AuthorizationFailed, false),
		(
			"quota",
			ClientError::QuotaExceeded {
				tenant_id: TenantId::new(TENANT),
			},
			false,
		),
	];
	for (name, error, expected) in cases {
		assert_eq!(
			offers_the_same_operation_again(&error, identity),
			expected,
			"{name}: {error:?}"
		);
	}
}

/// Exactly `expected`, read with ONE LINEARIZABLE streamed table query, bounded by its own
/// deadline and by the wait around it. Any error fails the test; nothing is asked again.
async fn expect_rows(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	deadline: Instant,
	what: &str,
	expected: &[(i64, String)],
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
	rows.sort();
	let mut expected = expected.to_vec();
	expected.sort();
	assert_eq!(rows, expected, "{what}");
}

/// A formed cluster with the application's tenant and table: everything before the first write.
/// Dropping it ends the daemons. `root` holds secrets and is removed only at the end of a pass.
struct Formed {
	root: PathBuf,
	pki: Pki,
	members: Vec<Member>,
	daemons: Daemons,
	descriptor: TableSchemaDescriptor,
}

/// A node's `[storage]` body and what follows `[tuning]`'s first lines. With no store both are
/// what the configuration always held. With one: small slabs, the store, an eager tiering
/// scan, and the stall bound written out at the test's declared value.
fn storage_section(dir: &Path, store: Option<&StoreEnv>) -> (String, String) {
	let Some(store) = store else {
		return ("data_dir = \"data\"\n".to_owned(), String::new());
	};
	let keys = dir.join("s3");
	std::fs::create_dir_all(&keys).unwrap();
	std::fs::set_permissions(&keys, std::os::unix::fs::PermissionsExt::from_mode(0o700)).unwrap();
	for (from, to) in [
		(&store.access_key_file, "access-key"),
		(&store.secret_key_file, "secret-key"),
	] {
		write_private(&keys.join(to), StoreEnv::key(from).as_bytes());
	}
	(
		format!(
			r#"data_dir = "data"
slab_max_bytes = {SLAB_MAX_BYTES}

[storage.object_store]
provider = "s3-compatible"
endpoint = "{endpoint}"
bucket = "{bucket}"
region = "{region}"
prefix = "{prefix}"
addressing = "path"
access_key_id_file = "s3/access-key"
secret_access_key_file = "s3/secret-key"

[storage.tiering]
scan_interval_millis = 100
min_slab_bytes = 1
"#,
			endpoint = store.endpoint,
			bucket = store.bucket,
			region = store.region,
			prefix = store.prefix,
		),
		format!("query_stall_bound_millis = {QUERY_STALL_BOUND_MILLIS}\n"),
	)
}

async fn form(deadline: Instant, store: Option<&StoreEnv>) -> Formed {
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
	// Each node's advertised address and its health, ready and metrics listeners: reserved on
	// loopback here, or given for other hosts (the three listeners then on the node's own
	// loopback, beside its port).
	let placed: Vec<(String, [String; 3])> = match node_control() {
		None => NODES
			.iter()
			.map(|_| {
				(
					format!("127.0.0.1:{}", reserve_shared_port()),
					[(); 3].map(|()| format!("127.0.0.1:{}", reserve_tcp_port())),
				)
			})
			.collect(),
		Some(_) => {
			let given = required("WENDELON_SMOKE_NODE_ADDRESSES");
			NODES
				.iter()
				.map(|id| {
					let address = given
						.split(',')
						.filter_map(|pair| pair.split_once('='))
						.find(|(node, _)| node.trim().parse() == Ok(*id))
						.map(|(_, address)| address.trim().to_owned())
						.unwrap_or_else(|| {
							panic!("WENDELON_SMOKE_NODE_ADDRESSES names no node {id}")
						});
					let socket: std::net::SocketAddr = address.parse().unwrap_or_else(|_| {
						panic!("node {id}: {address} is not an address and a port")
					});
					let side =
						[10, 11, 12].map(|offset| format!("127.0.0.1:{}", socket.port() + offset));
					(address, side)
				})
				.collect()
		}
	};
	let seed = placed[0].0.clone();
	let mut members = Vec::new();
	// ADR 0726: one root KEK per cluster. Every member is given the same unlock
	// secret; the sealed root itself is copied from the forming member below.
	let unlock_secret = random32();
	for ((id, secret), (address, [health, ready, metrics])) in
		NODES.iter().zip(&secrets).zip(&placed)
	{
		let dir = root.join(format!("node-{id}"));
		std::fs::create_dir_all(dir.join("data")).unwrap();
		write_private(&dir.join("kek-unlock"), &unlock_secret);
		let cluster = if *id == NODES[0] {
			"mode = \"bootstrap\"".to_owned()
		} else {
			format!(
				"mode = \"join\"\nseed_nodes = [{{ node_id = {}, address = \"{seed}\" }}]",
				NODES[0]
			)
		};
		// Beside local daemons the certificates stay in the run's one directory. For another
		// host the node's directory carries its own copies, named by relative paths: its
		// certificate and key, both CAs, and the operator's certificate and key, with which
		// that host runs `inspect` against the node's loopback ready listener.
		let (pki_dir, node_ca, operator_ca) = match node_control() {
			None => (
				root.join("pki").display().to_string(),
				pki.node_ca.display().to_string(),
				pki.operator_ca.display().to_string(),
			),
			Some(_) => {
				let tls = dir.join("tls");
				std::fs::create_dir_all(&tls).unwrap();
				std::fs::set_permissions(&tls, std::os::unix::fs::PermissionsExt::from_mode(0o700))
					.unwrap();
				let from = root.join("pki");
				for (name, private) in [
					(format!("node-{id}.crt"), false),
					(format!("node-{id}.key"), true),
					("node-ca.crt".to_owned(), false),
					("operator-ca.crt".to_owned(), false),
					("operator.crt".to_owned(), false),
					("operator.key".to_owned(), true),
				] {
					let bytes = std::fs::read(from.join(&name)).unwrap();
					if private {
						write_private(&tls.join(&name), &bytes);
					} else {
						std::fs::write(tls.join(&name), &bytes).unwrap();
					}
				}
				(
					"tls".to_owned(),
					"tls/node-ca.crt".to_owned(),
					"tls/operator-ca.crt".to_owned(),
				)
			}
		};
		let (storage, stall) = storage_section(&dir, store);
		let body = format!(
			r#"[node]
node_id = {id}
advertise_address = "{address}"

[cluster]
{cluster}
decision_authority = "parent-decisions"

[storage]
{storage}
[tuning]
replication_factor = 3
storage_shutdown_timeout_seconds = 5
{stall}
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
health = "{health}"
ready = "{ready}"
metrics = "{metrics}"
"#,
			lease = hex(secret.lease),
			transfer = hex(secret.transfer),
			audit = hex(secret.audit),
			decision = hex(secret.decision),
			trusted_lease = trusted[0],
			trusted_transfer = trusted[1],
			trusted_audit = trusted[2],
			trusted_decision = trusted[3],
			pki = pki_dir,
		);
		write_private(&dir.join("wendelon.toml"), body.as_bytes());
		members.push(Member {
			id: *id,
			config: dir.join("wendelon.toml"),
			address: address.clone(),
			ready: ready.clone(),
			log: dir.join("daemon.log"),
		});
	}

	let mut daemons = Daemons {
		children: Vec::new(),
		prepared: Vec::new(),
		hosted: Vec::new(),
	};
	// Form the cluster before any write: ADR 0720 fixes the membership at the first one.
	// The enrollment procedure of ADR 0726, as an operator performs it: start the
	// forming member, copy its sealed root KEK file, owner-only, to the same path in
	// each other member's empty data directory, then start those members in join mode.
	// On other hosts the same procedure: the forming member's directory goes to its host and
	// is started; its sealed root is read back from that host; the joiners' directories
	// receive it here and only then go to their hosts.
	daemons.prepare(&members[0]);
	daemons.start(&members[0]);
	let sealed_root = Path::new(".wendelon").join("local-sealed-root-kek.bin");
	let forming_root = root
		.join(format!("node-{}", NODES[0]))
		.join("data")
		.join(&sealed_root);
	let sealed = loop {
		// The file is created and written in one step; a complete one is 104 bytes.
		let read = match node_control() {
			None => std::fs::read(&forming_root).ok(),
			Some(control) => {
				let (ok, bytes, _) =
					node_control_call(&control, "sealed-root", NODES[0], None, CONTROL_BOUND);
				ok.then_some(bytes)
			}
		};
		match read {
			Some(bytes) if bytes.len() == 104 => break bytes,
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
	daemons.prepare(&members[1]);
	daemons.prepare(&members[2]);
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

	Formed {
		root,
		pki,
		members,
		daemons,
		descriptor,
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons; run alone under an external owner (see the module text)"]
async fn three_daemons_from_configuration_files_form_a_parent_decisions_cluster_and_serve_a_client()
{
	let deadline = Instant::now() + TEST_BOUND;
	let Formed {
		root,
		pki,
		members,
		mut daemons,
		descriptor,
	} = form(deadline, None).await;
	let row = one_row(7, "seven");
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

	// ADR 0729: a range's coordinating node is stopped, and a write to that range commits.
	// The control leader as the running nodes name it now.
	let inspection = inspect(&members[0], &pki);
	assert!(inspection.ready, "node 1 is not ready: {}", inspection.raw);
	let leader = inspection
		.leader()
		.unwrap_or_else(|| panic!("node 1 names no leader among the three: {}", inspection.raw));
	// Rows written, each ONE call, until one is coordinated by a node that is not the leader:
	// the coordinator stamps the commit HLC, and it is the holder of the row's range.
	let mut expected = vec![(7_i64, "seven".to_owned())];
	let mut target = None;
	for id in 100_i64..116 {
		let bound = step(deadline);
		let receipt = tokio::time::timeout(
			bound,
			client.table_write(
				TenantId::new(TENANT),
				&descriptor,
				one_row(id, "before"),
				WriteOptions {
					deadline: Some(Instant::now() + bound),
					..WriteOptions::default()
				},
			),
		)
		.await
		.unwrap_or_else(|_| panic!("the probe write of row {id} had no answer within its bound"))
		.unwrap_or_else(|error| panic!("the probe write of row {id}: {error:?}"));
		expected.push((id, "before".to_owned()));
		let holder = receipt.commit_hlc.node_id;
		assert!(NODES.contains(&holder), "row {id} was stamped by {holder}");
		if holder != leader {
			target = Some((id, holder));
			break;
		}
	}
	let (row_id, holder) =
		target.expect("none of sixteen rows is coordinated by a node other than the leader");
	eprintln!("smoke: leader {leader}; row {row_id} is coordinated by node {holder}; stopping it");
	let stopped = members
		.iter()
		.find(|member| member.id == holder)
		.expect("the holder is a member");
	daemons.stop(holder, deadline);
	let live: Vec<&Member> = members
		.iter()
		.filter(|member| member.id != holder)
		.collect();
	// The same row id, so the same range, through a live process: one operation identity,
	// offered again only while the answer is a stall or a failed connection, until its bound.
	let through = client_through(live[0], &pki, Credential::Storage, deadline).await;
	let identity = operation_id();
	let write_bound = Instant::now() + step(deadline);
	let started = Instant::now();
	let (receipt, attempts) = {
		let mut attempts = 0_u32;
		loop {
			attempts += 1;
			let left = write_bound.saturating_duration_since(Instant::now());
			assert!(
				!left.is_zero(),
				"row {row_id} did not commit within its bound with node {holder} stopped \
				 ({attempts} attempts)"
			);
			let answer = tokio::time::timeout(
				left,
				through.table_write(
					TenantId::new(TENANT),
					&descriptor,
					one_row(row_id, "after"),
					WriteOptions {
						deadline: Some(Instant::now() + left),
						operation_id: Some(identity),
						..WriteOptions::default()
					},
				),
			)
			.await
			.unwrap_or_else(|_| {
				panic!("row {row_id} had no answer within its bound with node {holder} stopped")
			});
			match answer {
				Ok(receipt) => break (receipt, attempts),
				// The range's authority is not available yet, or the client was sent to the
				// stopped node: the same operation is offered again. Nothing else is.
				Err(error) if offers_the_same_operation_again(&error, identity) => {
					eprintln!("smoke: attempt {attempts} of row {row_id}: {error}");
					tokio::time::sleep(Duration::from_millis(250)).await;
				}
				Err(error) => panic!("the write of row {row_id}: {error:?}"),
			}
		}
	};
	let successor = receipt.commit_hlc.node_id;
	eprintln!(
		"smoke: row {row_id} committed under node {successor} after {attempts} attempts in {:?}",
		started.elapsed()
	);
	assert!(
		successor != holder && NODES.contains(&successor),
		"row {row_id} was stamped by {successor} with node {holder} stopped"
	);
	expected.retain(|(id, _)| *id != row_id);
	expected.push((row_id, "after".to_owned()));
	for member in &live {
		let through = client_through(member, &pki, Credential::Storage, deadline).await;
		expect_rows(
			&through,
			&descriptor,
			deadline,
			&format!(
				"with node {holder} stopped, discovered through node {}",
				member.id
			),
			&expected,
		)
		.await;
	}
	// The stopped node returns, from the same file; every node is ready and reads the same.
	daemons.start(stopped);
	for member in &members {
		wait_ready(
			member,
			&pki,
			&mut daemons,
			deadline,
			"after the range holder's return",
		);
	}
	for member in &members {
		let through = client_through(member, &pki, Credential::Storage, deadline).await;
		expect_rows(
			&through,
			&descriptor,
			deadline,
			&format!(
				"after the holder's return, discovered through node {}",
				member.id
			),
			&expected,
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

// ============================================================
//  THE WHOLE-PROCESS PAUSE AND RECOVERY WORKFLOW
// ============================================================

/// Rows written and acknowledged before the pause, one operation each.
const PAUSE_ROWS_BEFORE: i64 = 8;
/// How long all three daemons stay stopped, from the last confirmed stop: longer than the
/// range lease (3 s) plus the takeover grace (1 s) plus the default election horizon, so
/// every lease lapses and every election timer passes while nothing runs.
const PAUSE_LENGTH: Duration = Duration::from_secs(8);
/// Each stop and each resume is confirmed from the kernel's process state within this.
const PAUSE_CONFIRM_BOUND: Duration = Duration::from_secs(2);
/// The caller deadline of the one offer made while the daemons are stopped.
const PAUSED_OFFER_DEADLINE: Duration = Duration::from_secs(3);
/// From immediately before the first SIGCONT: every node Ready, every outstanding operation
/// settled and the fresh operation committed. One shared budget; nothing is called after it.
const RECOVERY_BOUND: Duration = Duration::from_secs(60);
/// The caller deadline of one call inside the recovery budget, clipped to what is left of it.
const RECOVERY_CALL_BOUND: Duration = Duration::from_secs(5);
/// One operation is offered at most this many times in all.
const MAX_OFFERS_PER_OPERATION: u32 = 12;
/// New operations after a definite refusal: at most this many in the run, one per refusal.
const MAX_NEW_OPERATIONS: usize = 2;
/// A new operation's row key is its logical row plus this, so no upsert can make one
/// operation's commit look like another's.
const NEW_OPERATION_KEY_OFFSET: i64 = 100;

/// A daemon this test spawned, by pid AND kernel start time: a pid alone can be reused.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct OwnedProcess {
	node: u32,
	pid: u32,
	started: u64,
}

/// A process's kernel state letter and start time (`/proc/<pid>/stat` fields 3 and 22), or
/// why they could not be read. The command name may hold spaces and parentheses: fields are
/// counted after the LAST `)`.
fn process_state(pid: u32) -> Result<(char, u64), String> {
	let stat = std::fs::read_to_string(format!("/proc/{pid}/stat"))
		.map_err(|error| format!("pid {pid}: stat not readable: {error}"))?;
	let rest = stat
		.rsplit_once(')')
		.map(|(_, rest)| rest)
		.ok_or_else(|| format!("pid {pid}: stat has no command name"))?;
	let fields: Vec<&str> = rest.split_whitespace().collect();
	let state = fields
		.first()
		.and_then(|state| state.chars().next())
		.ok_or_else(|| format!("pid {pid}: stat has no state"))?;
	let started = fields
		.get(19)
		.and_then(|started| started.parse().ok())
		.ok_or_else(|| format!("pid {pid}: stat has no start time"))?;
	Ok((state, started))
}

impl OwnedProcess {
	/// The state of THIS process: an error if it is gone, unreadable, or another process now
	/// has its pid.
	fn state(&self) -> Result<char, String> {
		let (state, started) = process_state(self.pid)?;
		if started != self.started {
			return Err(format!(
				"node {}: pid {} now belongs to another process (start time {started}, was {})",
				self.node, self.pid, self.started
			));
		}
		Ok(state)
	}

	/// Signals this process only while it is still the one recorded.
	fn signal(&self, signal_number: libc::c_int) -> Result<(), String> {
		self.state()?;
		if signal(self.pid, signal_number) {
			Ok(())
		} else {
			Err(format!(
				"node {}: pid {} could not be signalled",
				self.node, self.pid
			))
		}
	}
}

impl Daemons {
	/// The local daemons as owned processes. Local processes only.
	fn owned(&self) -> Vec<OwnedProcess> {
		assert!(
			node_control().is_none(),
			"the pause workflow runs local processes only"
		);
		self.children
			.iter()
			.map(|(node, child)| {
				let pid = child.id();
				let (_, started) = process_state(pid).unwrap_or_else(|why| panic!("{why}"));
				OwnedProcess {
					node: *node,
					pid,
					started,
				}
			})
			.collect()
	}
}

/// Created BEFORE the first SIGSTOP and alive to the end of the test: on every way out,
/// a panic included, each recorded daemon that is still the same process is sent SIGCONT, so
/// none is left stopped for the teardown that follows (`Daemons`' own drop, declared earlier
/// and so dropped later, then ends them). Resuming a running process does nothing.
struct ResumeOnExit(Vec<OwnedProcess>);

impl Drop for ResumeOnExit {
	fn drop(&mut self) {
		for process in &self.0 {
			if let Err(why) = process.signal(libc::SIGCONT) {
				eprintln!("smoke: pause: not resumed at exit: {why}");
			}
		}
	}
}

/// Stops every process and confirms each is stopped (state `T`). Returns when the first
/// signal was sent and when the last stop was confirmed.
fn pause_all(processes: &[OwnedProcess]) -> Result<(Instant, Instant), String> {
	let first_signal = Instant::now();
	for process in processes {
		process.signal(libc::SIGSTOP)?;
	}
	let bound = Instant::now() + PAUSE_CONFIRM_BOUND;
	for process in processes {
		loop {
			let state = process.state()?;
			// The bound first: a stop seen after it is not a confirmed stop.
			if Instant::now() > bound {
				return Err(format!(
					"node {}: pid {} not confirmed stopped within {PAUSE_CONFIRM_BOUND:?} (state {state})",
					process.node, process.pid
				));
			}
			if state == 'T' {
				break;
			}
			std::thread::sleep(Duration::from_millis(10));
		}
	}
	Ok((first_signal, Instant::now()))
}

/// Resumes every process and confirms each is the same process, alive and no longer
/// stopped. A zombie, a dead or missing process, a reused pid or an unreadable state is a
/// failure, never "resumed". This says a process can be scheduled; it says nothing about
/// service. Returns when the last one was confirmed.
fn resume_all(processes: &[OwnedProcess]) -> Result<Instant, String> {
	for process in processes {
		process.signal(libc::SIGCONT)?;
	}
	let bound = Instant::now() + PAUSE_CONFIRM_BOUND;
	for process in processes {
		loop {
			let state = process.state()?;
			// The bound first: a running state seen after it is not a confirmed resume.
			if Instant::now() > bound {
				return Err(format!(
					"node {}: pid {} not confirmed running within {PAUSE_CONFIRM_BOUND:?} (state {state})",
					process.node, process.pid
				));
			}
			match state {
				'R' | 'S' | 'D' | 'I' => break,
				'T' | 't' => std::thread::sleep(Duration::from_millis(10)),
				state => {
					return Err(format!(
						"node {}: pid {} is not running after SIGCONT (state {state})",
						process.node, process.pid
					));
				}
			}
		}
	}
	Ok(Instant::now())
}

/// What one answer means for its operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Disposition {
	/// A receipt: committed.
	Committed,
	/// The recorded refusal: `WRITE_REFUSED` with `KNOWN_NOT_COMMITTED`, naming this operation.
	Refused,
	/// Nothing is known: the SAME operation may be offered again, within its bounds.
	Unknown,
}

/// One logical write: one identity and one row for its whole life. `key` is unique to the
/// operation and the row's name says which operation it is.
struct PauseOperation {
	logical_row: i64,
	key: i64,
	name: String,
	identity: wendelon::schema::ClientWriteId,
	offers: u32,
	disposition: Disposition,
}

impl PauseOperation {
	fn new(logical_row: i64, ordinal: u32) -> Self {
		let key = if ordinal == 1 {
			logical_row
		} else {
			logical_row + NEW_OPERATION_KEY_OFFSET
		};
		Self {
			logical_row,
			key,
			name: format!("row{key}-op{ordinal}"),
			identity: operation_id(),
			offers: 0,
			disposition: Disposition::Unknown,
		}
	}
}

/// The reading of one answer, by its status AND its commit knowledge. It does not use the
/// older retry rule above, which reads no knowledge.
///
/// - Committed: a receipt of exactly one row.
/// - Refused: `MutationStopped` naming THIS operation, `WRITE_REFUSED` with
///   `KNOWN_NOT_COMMITTED`: the recorded refusal.
/// - Unknown (the SAME operation may be offered again): no answer inside the call's bound;
///   a failed connection; a redirect (`NOT_LEADER`, which carries no knowledge to the
///   caller); and `MutationStopped` naming THIS operation as: `WRITE_STALLED` (reason NONE)
///   with `INDETERMINATE`; `WRITE_DEADLINE_EXCEEDED` (reason DEADLINE) with `INDETERMINATE`; or the client's
///   own summary `WRITE_ATTEMPTS_EXHAUSTED` (reason ATTEMPT_EXHAUSTION) with `INDETERMINATE`
///   or with `KNOWN_NOT_COMMITTED`. That last pair says no attempt of this CALL was left
///   unresolved; it is not the recorded refusal and is never read as one.
/// - Everything else is an error that ends the test and is never retried into a pass: a
///   stop that claims `COMMITTED`, an unspecified or unknown knowledge on a stop, an answer
///   about another operation, an inconsistent answer, a conflict, an expiry, an internal
///   error, a receipt for another row count.
fn pause_disposition(
	answer: &Option<Result<wendelon_client::TableWriteReceipt, ClientError>>,
	identity: wendelon::schema::ClientWriteId,
) -> Result<Disposition, String> {
	use std::io::ErrorKind;
	use wendelon::schema::{MutationCommitKnowledge as Knowledge, MutationStopReason, WriteStatus};
	match answer {
		None => Ok(Disposition::Unknown),
		Some(Ok(receipt)) if receipt.rows == 1 => Ok(Disposition::Committed),
		Some(Ok(receipt)) => Err(format!("a receipt for {} rows", receipt.rows)),
		Some(Err(ClientError::Io(io)))
			if matches!(
				io.kind(),
				ErrorKind::ConnectionRefused
					| ErrorKind::ConnectionReset
					| ErrorKind::ConnectionAborted
					| ErrorKind::NotConnected
			) =>
		{
			Ok(Disposition::Unknown)
		}
		Some(Err(ClientError::WriteFailed { status })) if *status == WriteStatus::NOT_LEADER => {
			Ok(Disposition::Unknown)
		}
		Some(Err(ClientError::MutationStopped {
			operation_id,
			status,
			reason,
			knowledge,
		})) if operation_id.client_uuid == identity.client_uuid
			&& operation_id.sequence_no == identity.sequence_no =>
		{
			let (status, reason, knowledge) = (*status, *reason, *knowledge);
			if status == WriteStatus::WRITE_REFUSED && knowledge == Knowledge::KNOWN_NOT_COMMITTED {
				Ok(Disposition::Refused)
			} else if (status == WriteStatus::WRITE_STALLED
				&& reason == MutationStopReason::NONE
				&& knowledge == Knowledge::INDETERMINATE)
				|| (status == WriteStatus::WRITE_DEADLINE_EXCEEDED
					&& reason == MutationStopReason::DEADLINE
					&& knowledge == Knowledge::INDETERMINATE)
				|| (status == WriteStatus::WRITE_ATTEMPTS_EXHAUSTED
					&& reason == MutationStopReason::ATTEMPT_EXHAUSTION
					&& (knowledge == Knowledge::INDETERMINATE
						|| knowledge == Knowledge::KNOWN_NOT_COMMITTED))
			{
				Ok(Disposition::Unknown)
			} else {
				Err(format!(
					"a stop this workflow does not allow: status {status:?}, reason {reason:?}, knowledge {knowledge:?}"
				))
			}
		}
		Some(Err(error)) => Err(format!("{error:?}")),
	}
}

/// How long after a call's caller deadline this test's controller may first SEE that the
/// deadline passed. It is an allowance for observing a notification, declared once: it
/// changes no caller deadline, admits no late outcome and allows no call after a shared mark.
const DEADLINE_NOTICE_ALLOWANCE: Duration = Duration::from_millis(250);

/// Whether `answer` is nothing more than word that the caller deadline passed with the
/// operation unresolved: no client answer at all (the wrapper's timeout), or THIS
/// operation's `WRITE_DEADLINE_EXCEEDED` with reason DEADLINE and `INDETERMINATE`.
fn is_deadline_notice(
	answer: &Option<Result<wendelon_client::TableWriteReceipt, ClientError>>,
	identity: wendelon::schema::ClientWriteId,
) -> bool {
	use wendelon::schema::{MutationCommitKnowledge, MutationStopReason, WriteStatus};
	match answer {
		None => true,
		Some(Err(ClientError::MutationStopped {
			operation_id,
			status,
			reason,
			knowledge,
		})) => {
			operation_id.client_uuid == identity.client_uuid
				&& operation_id.sequence_no == identity.sequence_no
				&& *status == WriteStatus::WRITE_DEADLINE_EXCEEDED
				&& *reason == MutationStopReason::DEADLINE
				&& *knowledge == MutationCommitKnowledge::INDETERMINATE
		}
		_ => false,
	}
}

/// THE acceptance rule of one offer, used on the offer path and by the control below. The
/// answer is read first (identity, status, reason, knowledge: `pause_disposition`); then
/// time:
/// - any answer that says something about the operation (a receipt, the refusal, a stall, a
///   redirect, an exhaustion, a connection error) is accepted only if it was observed at or
///   before the call's caller deadline. A timeout wrapper is cooperative, so the clock is
///   read after the answer; a late one fails, whatever it says;
/// - word that the deadline passed (`is_deadline_notice`) is accepted only as Unknown,
///   including an early notification, when observed no later than the deadline plus
///   `DEADLINE_NOTICE_ALLOWANCE`; later than that fails too.
fn accepted_disposition(
	answer: &Option<Result<wendelon_client::TableWriteReceipt, ClientError>>,
	identity: wendelon::schema::ClientWriteId,
	answered: Instant,
	call_deadline: Instant,
) -> Result<Disposition, String> {
	let disposition = pause_disposition(answer, identity)?;
	let notice = is_deadline_notice(answer, identity);
	let limit = if notice {
		call_deadline + DEADLINE_NOTICE_ALLOWANCE
	} else {
		call_deadline
	};
	if answered <= limit {
		Ok(disposition)
	} else if notice {
		Err(format!(
			"word of the deadline came {:?} after it, beyond the {DEADLINE_NOTICE_ALLOWANCE:?} this controller allows for observing it",
			answered - call_deadline
		))
	} else {
		Err(format!(
			"a completed answer {:?} after its caller deadline",
			answered - call_deadline
		))
	}
}

/// The two rules above, by case. Runs in a default pass: it spawns nothing.
#[test]
fn the_pause_workflow_reads_an_answer_by_status_knowledge_and_time() {
	use wendelon::schema::{MutationCommitKnowledge as Knowledge, MutationStopReason, WriteStatus};
	let identity = operation_id();
	let stopped = |status, reason, knowledge| {
		Some(Err(ClientError::MutationStopped {
			operation_id: identity,
			status,
			reason,
			knowledge,
		}))
	};
	let read = |answer| pause_disposition(&answer, identity);
	let (none, exhausted) = (
		MutationStopReason::NONE,
		MutationStopReason::ATTEMPT_EXHAUSTION,
	);
	assert_eq!(read(None), Ok(Disposition::Unknown));
	assert_eq!(
		read(stopped(
			WriteStatus::WRITE_REFUSED,
			none,
			Knowledge::KNOWN_NOT_COMMITTED
		)),
		Ok(Disposition::Refused)
	);
	for allowed in [
		stopped(WriteStatus::WRITE_STALLED, none, Knowledge::INDETERMINATE),
		stopped(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			Knowledge::INDETERMINATE,
		),
		stopped(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			exhausted,
			Knowledge::INDETERMINATE,
		),
		stopped(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			exhausted,
			Knowledge::KNOWN_NOT_COMMITTED,
		),
		Some(Err(ClientError::WriteFailed {
			status: WriteStatus::NOT_LEADER,
		})),
		Some(Err(ClientError::Io(std::io::Error::from(
			std::io::ErrorKind::ConnectionRefused,
		)))),
	] {
		assert_eq!(read(allowed), Ok(Disposition::Unknown));
	}
	// Contradictory or silent knowledge on a stop is never "unknown, offer again".
	for refused in [
		stopped(WriteStatus::WRITE_STALLED, none, Knowledge::COMMITTED),
		stopped(WriteStatus::WRITE_STALLED, none, Knowledge::UNSPECIFIED),
		stopped(
			WriteStatus::WRITE_STALLED,
			none,
			Knowledge::KNOWN_NOT_COMMITTED,
		),
		stopped(WriteStatus::WRITE_STALLED, none, Knowledge(9)),
		stopped(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			exhausted,
			Knowledge::COMMITTED,
		),
		stopped(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			exhausted,
			Knowledge::UNSPECIFIED,
		),
		stopped(
			WriteStatus::WRITE_ATTEMPTS_EXHAUSTED,
			none,
			Knowledge::INDETERMINATE,
		),
		stopped(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			Knowledge::COMMITTED,
		),
		stopped(WriteStatus::WRITE_REFUSED, none, Knowledge::INDETERMINATE),
		stopped(WriteStatus::WRITE_REFUSED, none, Knowledge::COMMITTED),
		stopped(
			WriteStatus::WRITE_OPERATION_EXPIRED,
			none,
			Knowledge::INDETERMINATE,
		),
		Some(Err(ClientError::MutationAnswerInconsistent {
			operation_id: identity,
			status: WriteStatus::OK,
			knowledge: Knowledge::UNSPECIFIED,
		})),
		Some(Err(ClientError::WriteFailed {
			status: WriteStatus::INTERNAL_ERROR,
		})),
		Some(Err(ClientError::Io(std::io::Error::from(
			std::io::ErrorKind::InvalidData,
		)))),
	] {
		assert!(read(refused).is_err());
	}
	// An answer about another operation is never this operation's answer.
	let mut other = identity;
	other.sequence_no += 1;
	assert!(
		read(Some(Err(ClientError::MutationStopped {
			operation_id: other,
			status: WriteStatus::WRITE_REFUSED,
			reason: none,
			knowledge: Knowledge::KNOWN_NOT_COMMITTED,
		})))
		.is_err()
	);
	// A deadline report with another reason is not the deadline report.
	assert!(
		read(stopped(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			none,
			Knowledge::INDETERMINATE
		))
		.is_err()
	);

	// Time. An outcome is accepted only at or before the caller deadline; word that the
	// deadline passed may be observed up to the allowance after it, and is only ever Unknown.
	let deadline = Instant::now() + Duration::from_secs(1);
	let ms = Duration::from_millis;
	let at = |answer, answered| accepted_disposition(&answer, identity, answered, deadline);
	let receipt = || {
		Some(Ok(wendelon_client::TableWriteReceipt {
			table_object_id: descriptor().table_object_id,
			schema_generation: 1,
			rows: 1,
			duplicate: false,
			requested_durability: WriteOptions::default().durability,
			log_index: 0,
			term: 0,
			epoch: 0,
			commit_hlc: wendelon::schema::HybridTimestamp {
				physical_ms: 0,
				logical: 0,
				node_id: 0,
			},
			finalization: None,
		}))
	};
	let refusal = || {
		stopped(
			WriteStatus::WRITE_REFUSED,
			none,
			Knowledge::KNOWN_NOT_COMMITTED,
		)
	};
	let notice = || {
		stopped(
			WriteStatus::WRITE_DEADLINE_EXCEEDED,
			MutationStopReason::DEADLINE,
			Knowledge::INDETERMINATE,
		)
	};
	assert_eq!(at(receipt(), deadline), Ok(Disposition::Committed));
	assert_eq!(at(receipt(), deadline - ms(1)), Ok(Disposition::Committed));
	assert!(at(receipt(), deadline + ms(1)).is_err());
	assert_eq!(at(refusal(), deadline), Ok(Disposition::Refused));
	assert!(at(refusal(), deadline + ms(1)).is_err());
	assert!(
		at(
			stopped(WriteStatus::WRITE_STALLED, none, Knowledge::INDETERMINATE),
			deadline + ms(1)
		)
		.is_err()
	);
	for (late, accepted) in [(0, true), (1, true), (250, true), (251, false)] {
		assert_eq!(
			at(notice(), deadline + ms(late)).is_ok(),
			accepted,
			"the client's deadline report {late} ms after the deadline"
		);
		assert_eq!(
			at(None, deadline + ms(late)).is_ok(),
			accepted,
			"no client answer, seen {late} ms after the deadline"
		);
	}
	assert_eq!(at(notice(), deadline - ms(5)), Ok(Disposition::Unknown));
	assert_eq!(at(notice(), deadline + ms(250)), Ok(Disposition::Unknown));
	// The allowance is not for another operation's report, nor for a contradictory one.
	assert!(
		at(
			Some(Err(ClientError::MutationStopped {
				operation_id: other,
				status: WriteStatus::WRITE_DEADLINE_EXCEEDED,
				reason: MutationStopReason::DEADLINE,
				knowledge: Knowledge::INDETERMINATE,
			})),
			deadline + ms(1)
		)
		.is_err()
	);
	assert!(
		at(
			stopped(
				WriteStatus::WRITE_DEADLINE_EXCEEDED,
				MutationStopReason::DEADLINE,
				Knowledge::COMMITTED
			),
			deadline + ms(1)
		)
		.is_err()
	);
}

/// ONE offer of an operation, journaled. The call has ONE absolute deadline: now plus
/// `call_bound`, clipped to `limit` (the shared recovery mark, or the test's own bound). With
/// nothing left before `limit`, or the operation's offers used up, no call is made and `false`
/// is returned. The same instant is the client and wrapper deadline. The notice allowance
/// bounds when the controller observes a timeout; the wrapper does not consume it by waiting
/// longer. Whether the answer is accepted is `accepted_disposition`'s rule alone. The
/// operation's disposition is updated only from an accepted answer; an operation is never
/// offered once it is Committed or Refused.
async fn offer(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	operation: &mut PauseOperation,
	call_bound: Duration,
	limit: Instant,
	clock: Instant,
	stage: &str,
) -> bool {
	assert_eq!(
		operation.disposition,
		Disposition::Unknown,
		"a settled operation was offered again"
	);
	let asked = Instant::now();
	let call_deadline = (asked + call_bound).min(limit);
	if call_deadline <= asked || operation.offers >= MAX_OFFERS_PER_OPERATION {
		return false;
	}
	operation.offers += 1;
	let answer = tokio::time::timeout_at(
		tokio::time::Instant::from_std(call_deadline),
		client.table_write(
			TenantId::new(TENANT),
			descriptor,
			one_row(operation.key, &operation.name),
			WriteOptions {
				deadline: Some(call_deadline),
				operation_id: Some(operation.identity),
				..WriteOptions::default()
			},
		),
	)
	.await
	.ok();
	let answered = Instant::now();
	let read = accepted_disposition(&answer, operation.identity, answered, call_deadline);
	eprintln!(
		"smoke: JOURNAL {stage}: logical row {} key {} name {} identity {:02x?}/{} offer {} asked at {:+.3} s answered after {:?} (caller deadline {:?} after it was asked; observed {:+.3} ms relative to that deadline): {} => {}",
		operation.logical_row,
		operation.key,
		operation.name,
		&operation.identity.client_uuid,
		operation.identity.sequence_no,
		operation.offers,
		asked.saturating_duration_since(clock).as_secs_f64()
			- clock.saturating_duration_since(asked).as_secs_f64(),
		answered - asked,
		call_deadline - asked,
		(answered
			.saturating_duration_since(call_deadline)
			.as_secs_f64()
			- call_deadline
				.saturating_duration_since(answered)
				.as_secs_f64())
			* 1000.0,
		match &answer {
			None => "NO ANSWER from the client within the caller deadline (unresolved)".to_owned(),
			Some(Ok(receipt)) => format!("{receipt:?}"),
			Some(Err(error)) => format!("{error:?}"),
		},
		match &read {
			Ok(disposition) => format!("{disposition:?}"),
			Err(why) => format!("NOT ACCEPTED: {why}"),
		}
	);
	operation.disposition = read.unwrap_or_else(|why| {
		panic!(
			"{stage}: the offer of {} had an answer this workflow does not accept: {why}",
			operation.name
		)
	});
	true
}

/// Offers one operation until it is Committed or Refused. `false`: it is still unknown and
/// no further call fits before `mark`, or its offers are used up.
async fn settle(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	operation: &mut PauseOperation,
	mark: Instant,
	clock: Instant,
	stage: &str,
) -> bool {
	while operation.disposition == Disposition::Unknown {
		if !offer(
			client,
			descriptor,
			operation,
			RECOVERY_CALL_BOUND,
			mark,
			clock,
			stage,
		)
		.await
		{
			return false;
		}
		if operation.disposition == Disposition::Unknown {
			tokio::time::sleep(
				Duration::from_millis(250).min(mark.saturating_duration_since(Instant::now())),
			)
			.await;
		}
	}
	true
}

/// Three real daemons at every default, with NO object store, are all stopped at once for
/// `PAUSE_LENGTH` and released; they are never restarted and nothing is cleared.
///
/// What it is: a whole-process scheduling pause (SIGSTOP, SIGCONT) of every daemon. It is not
/// an injected disk stall and not a network partition. It exercises no tiering, no object
/// store and no remote read: it cannot stand for the object-store workflow above.
///
/// What a pass claims, in three parts kept apart in the log:
/// 1. DURING the pause, the one offer made got an allowed answer inside its own deadline.
/// 2. AFTER the release, inside ONE budget of `RECOVERY_BOUND` that starts immediately before
///    the first SIGCONT: every node reported Ready; every operation left unknown was settled
///    by offering the SAME operation again (a receipt, or the recorded refusal); a refused
///    operation was followed by at most one new operation with a new identity and a new row
///    key; and one fresh operation, begun after the release, got a receipt.
/// 3. THEN one exact read, under its own bound: the table holds exactly the rows whose
///    operation got a receipt, each with its own text; no refused-only row and no row of
///    another origin. And each daemon is the same process as before the pause.
///
/// What it does not claim: that writes are available during a pause; any internal
/// transition (the oracle is what the caller was told and what a complete read returned);
/// anything about a slower network, a stalled disk or a longer pause. A failed write BEFORE
/// the pause is a failure of the product at rest, reported as such.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons and stops them with signals; run alone under an external owner (see the module text)"]
async fn three_daemons_resume_service_without_restart_after_all_are_paused() {
	let deadline = Instant::now() + TEST_BOUND;
	let Formed {
		root,
		pki,
		members,
		mut daemons,
		descriptor,
	} = form(deadline, None).await;
	let client = client_through(&members[0], &pki, Credential::Storage, deadline).await;
	let processes = daemons.owned();
	eprintln!("smoke: pause: owned daemons {processes:?}");
	let mut operations: Vec<PauseOperation> = Vec::new();
	let before_everything = Instant::now();

	// Before the pause: each row once, each must be acknowledged.
	for row in 1..=PAUSE_ROWS_BEFORE {
		let mut operation = PauseOperation::new(row, 1);
		offer(
			&client,
			&descriptor,
			&mut operation,
			RECOVERY_CALL_BOUND,
			deadline,
			before_everything,
			"before the pause",
		)
		.await;
		assert_eq!(
			operation.disposition,
			Disposition::Committed,
			"BASELINE, before any pause: the write of {} was not acknowledged by its one call",
			operation.name
		);
		operations.push(operation);
	}

	// The pause. The guard exists before the first stop and outlives every step below.
	let resume_on_exit = ResumeOnExit(processes.clone());
	let (stop_sent, stopped) = pause_all(&processes).unwrap_or_else(|why| panic!("pause: {why}"));
	eprintln!(
		"smoke: PAUSE: all three stopped; first signal to last confirmation {:?}",
		stopped - stop_sent
	);

	// DURING: one offer, its own deadline, journaled and read like any other.
	let mut during = PauseOperation::new(PAUSE_ROWS_BEFORE + 1, 1);
	offer(
		&client,
		&descriptor,
		&mut during,
		PAUSED_OFFER_DEADLINE,
		deadline,
		before_everything,
		"during the pause",
	)
	.await;
	eprintln!(
		"smoke: RESULT 1 (during the pause): the offer of {} ended {:?}",
		during.name, during.disposition
	);
	operations.push(during);
	tokio::time::sleep((stopped + PAUSE_LENGTH).saturating_duration_since(Instant::now())).await;
	for process in &processes {
		let state = process.state().unwrap_or_else(|why| panic!("pause: {why}"));
		assert_eq!(state, 'T', "node {} did not stay stopped", process.node);
	}

	// The release. The recovery budget starts BEFORE the first SIGCONT.
	let release = Instant::now();
	let mark = release + RECOVERY_BOUND;
	let running = resume_all(&processes).unwrap_or_else(|why| panic!("release: {why}"));
	eprintln!(
		"smoke: RELEASE: every daemon stopped for at least {:?} (last stop confirmed to first SIGCONT); all three confirmed runnable {:?} after the release began. Recovery bound {RECOVERY_BOUND:?} from the release",
		release - stopped,
		running - release
	);

	// (a) Every node Ready, inside the budget.
	for member in &members {
		wait_ready(member, &pki, &mut daemons, mark, "after the release");
	}
	let all_ready = Instant::now();
	assert!(all_ready <= mark, "readiness came after the recovery bound");
	eprintln!(
		"smoke: RECOVERY: every node Ready {:?} after the release",
		all_ready - release
	);

	// (b), (c) Settle what is outstanding; a refusal may be followed by ONE new operation.
	let mut new_operations = 0_usize;
	let mut index = 0;
	while index < operations.len() {
		if !settle(
			&client,
			&descriptor,
			&mut operations[index],
			mark,
			release,
			"settling after the release",
		)
		.await
		{
			panic!(
				"UNRESOLVED at the recovery bound: {} after {} offers",
				operations[index].name, operations[index].offers
			);
		}
		if operations[index].disposition == Disposition::Refused
			&& operations[index].key == operations[index].logical_row
			&& new_operations < MAX_NEW_OPERATIONS
		{
			new_operations += 1;
			let replacement = PauseOperation::new(operations[index].logical_row, 2);
			eprintln!(
				"smoke: NEW OPERATION {} of {MAX_NEW_OPERATIONS} at most: {} follows the refused {}",
				new_operations, replacement.name, operations[index].name
			);
			operations.push(replacement);
		}
		index += 1;
	}

	// (d) Fresh progress: an operation begun after the release must get a receipt. A fresh
	// operation that is refused may, like any other, be followed by one new operation while
	// the run's allowance lasts; only a receipt counts as progress.
	let mut ordinal = 1;
	let fresh_committed = loop {
		let mut fresh = PauseOperation::new(PAUSE_ROWS_BEFORE + 2, ordinal);
		let settled = settle(
			&client,
			&descriptor,
			&mut fresh,
			mark,
			release,
			"the fresh operation",
		)
		.await;
		let (name, disposition) = (fresh.name.clone(), fresh.disposition);
		operations.push(fresh);
		assert!(
			settled,
			"NO FRESH PROGRESS inside the recovery bound: {name} is unresolved"
		);
		if disposition == Disposition::Committed {
			break name;
		}
		assert!(
			ordinal == 1 && new_operations < MAX_NEW_OPERATIONS,
			"NO FRESH PROGRESS: {name} was refused and no new operation is left"
		);
		new_operations += 1;
		ordinal = 2;
		eprintln!(
			"smoke: NEW OPERATION {new_operations} of {MAX_NEW_OPERATIONS} at most: after the refused {name}"
		);
	};
	eprintln!("smoke: RECOVERY: fresh progress: {fresh_committed} committed");
	let progressed = Instant::now();
	assert!(progressed <= mark, "progress came after the recovery bound");
	let count = |disposition| {
		operations
			.iter()
			.filter(|operation| operation.disposition == disposition)
			.count()
	};
	assert_eq!(count(Disposition::Unknown), 0, "an operation is unresolved");
	eprintln!(
		"smoke: RESULT 2 (after the release): Ready after {:?}, everything settled and a fresh write committed after {:?}; operations {}: committed {}, refused {}, new operations after a refusal {new_operations}",
		all_ready - release,
		progressed - release,
		operations.len(),
		count(Disposition::Committed),
		count(Disposition::Refused),
	);

	// THEN one exact read: exactly the committed operations' rows, with their own text.
	let expected: Vec<(i64, String)> = operations
		.iter()
		.filter(|operation| operation.disposition == Disposition::Committed)
		.map(|operation| (operation.key, operation.name.clone()))
		.collect();
	let read_asked = Instant::now();
	expect_rows(
		&client,
		&descriptor,
		deadline,
		"the one exact read after everything was settled",
		&expected,
	)
	.await;
	eprintln!(
		"smoke: FINAL READ: exactly the {} committed rows, {:?} after it was asked, {:?} after the release",
		expected.len(),
		read_asked.elapsed(),
		release.elapsed()
	);

	// No restart: the same processes, alive, and none has exited.
	for process in &processes {
		let state = process
			.state()
			.unwrap_or_else(|why| panic!("no-restart check: {why}"));
		assert!(
			matches!(state, 'R' | 'S' | 'D' | 'I'),
			"node {} is in state {state} at the end",
			process.node
		);
	}
	if let Some(exited) = daemons.exited() {
		panic!("no-restart check: {exited}");
	}
	assert_eq!(
		daemons.owned(),
		processes,
		"a daemon is not the process it was"
	);
	eprintln!(
		"smoke: RESULT 3 (overall): PASS: acknowledged rows read exactly, refusals true to the read, fresh progress inside the bound, the same three processes throughout"
	);

	drop(resume_on_exit);
	for id in NODES {
		daemons.stop(id, deadline);
	}
	assert!(Instant::now() < deadline, "the test overran its bound");
	// A pass keeps its daemon logs with the record, as the object-store workflow does.
	if let Ok(record) = std::env::var("WENDELON_SMOKE_RECORD_DIR") {
		for member in &members {
			std::fs::copy(
				&member.log,
				Path::new(&record).join(format!("node-{}.daemon.log", member.id)),
			)
			.unwrap();
		}
	}
	std::fs::remove_dir_all(&root).unwrap();
}

/// The node and the slab an object's key names:
/// `…/<node id, 8 hex digits>-<incarnation>/slab_<slab id, 16 hex digits>_<unique>.wnd`.
fn owner_and_slab(key: &str) -> Option<(u32, &str)> {
	let mut parts = key.rsplit('/');
	let (file, owner) = (parts.next()?, parts.next()?);
	let node = u32::from_str_radix(owner.get(..8)?, 16).ok()?;
	let slab = file.strip_prefix("slab_")?.get(..16)?;
	(owner.as_bytes().get(8) == Some(&b'-')
		&& file.ends_with(".wnd")
		&& slab.bytes().all(|byte| byte.is_ascii_hexdigit()))
	.then_some((node, slab))
}

/// Every file name under a directory, read only, or why the listing is not whole. Absence is
/// never inferred from an error: any failure to read a directory, an entry or an entry's type
/// fails the listing, except one case taken narrowly: an entry that the directory named and that
/// is NotFound when its type is asked, which is a file removed in between and is not listed.
/// Symbolic links are neither followed nor listed as files of the tree: one is an error here,
/// because a node's data directory is expected to hold none.
fn file_names(dir: &Path, names: &mut Vec<String>) -> Result<(), String> {
	let entries =
		std::fs::read_dir(dir).map_err(|error| format!("reading {}: {error}", dir.display()))?;
	for entry in entries {
		let entry = entry.map_err(|error| format!("an entry of {}: {error}", dir.display()))?;
		let path = entry.path();
		let kind = match std::fs::symlink_metadata(&path) {
			Ok(metadata) => metadata.file_type(),
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => continue,
			Err(error) => return Err(format!("the type of {}: {error}", path.display())),
		};
		if kind.is_symlink() {
			return Err(format!("{} is a symbolic link", path.display()));
		} else if kind.is_dir() {
			file_names(&path, names)?;
		} else {
			names.push(entry.file_name().to_string_lossy().into_owned());
		}
	}
	Ok(())
}

#[test]
fn a_listing_that_cannot_be_read_whole_is_an_error_not_an_absence() {
	use std::os::unix::fs::PermissionsExt as _;
	let root = std::env::temp_dir().join(format!("nilket-pd-listing-{}", std::process::id()));
	let _ = std::fs::remove_dir_all(&root);
	std::fs::create_dir_all(root.join("sub")).unwrap();
	std::fs::write(root.join("slab_0000000000000001.wnd"), b"x").unwrap();
	std::fs::write(root.join("sub").join("inner.bin"), b"x").unwrap();
	// Whole: every file, at any depth.
	let mut names = Vec::new();
	file_names(&root, &mut names).unwrap();
	names.sort();
	assert_eq!(names, ["inner.bin", "slab_0000000000000001.wnd"]);
	// A directory that cannot be read: an error, and its files are not taken as gone.
	std::fs::set_permissions(root.join("sub"), std::fs::Permissions::from_mode(0o000)).unwrap();
	let unreadable = file_names(&root, &mut Vec::new());
	std::fs::set_permissions(root.join("sub"), std::fs::Permissions::from_mode(0o700)).unwrap();
	assert!(
		unreadable.as_ref().is_err_and(|why| why.contains("sub")),
		"{unreadable:?}"
	);
	// A symbolic link is not followed and not skipped: an error.
	std::os::unix::fs::symlink(root.join("sub"), root.join("link")).unwrap();
	let linked = file_names(&root, &mut Vec::new());
	assert!(
		linked
			.as_ref()
			.is_err_and(|why| why.contains("symbolic link")),
		"{linked:?}"
	);
	// A directory that is not there at all: an error.
	assert!(file_names(&root.join("absent"), &mut Vec::new()).is_err());
	std::fs::remove_dir_all(&root).unwrap();
}

/// Whether a local file's name is a slab file of this slab id (16 hex digits): the name carries
/// the token `slab_<id>`. That is the payload `slab_<id>.wnd`, any temporary suffix of it, and
/// the product's prefixed copies (`compaction-temp-<job>-slab_<id>.wnd`,
/// `lsm-quarantine-<job>-slab_<id>.wnd`). The bare id is NOT matched: a control WAL segment is
/// named by a 20-digit number that contains slab ids as substrings, which an earlier form of
/// this test counted as slab files (run pd-offload-20261114T000557Z failed its precondition on
/// that, with every sealed slab in fact offloaded; in the runs before it one slab per node,
/// slab 0, was miscounted as local for the same reason, hidden by the threshold's slack).
fn names_the_slab(name: &str, slab: &str) -> bool {
	name.contains(&format!("slab_{slab}"))
}

#[test]
fn only_a_slab_named_file_keeps_a_slab_on_disk() {
	let (zero, three, fifty) = ("0000000000000000", "0000000000000003", "0000000000000032");
	// The control WAL segments observed in that run: none of them is a slab file.
	for wal in [
		"control.wal.00000000000000000000.seg",
		"control.wal.00000000000000000323.seg",
	] {
		for slab in [zero, three, fifty] {
			assert!(!names_the_slab(wal, slab), "{wal} is not slab {slab}");
		}
	}
	// The payload, a temporary suffix, and the prefixed copies are.
	for name in [
		"slab_0000000000000003.wnd",
		"slab_0000000000000003.wnd.tmp",
		"slab_0000000000000003_500210871f3241c5b37007cb0e3f21bf.wnd",
		"compaction-temp-7-slab_0000000000000003.wnd",
		"lsm-quarantine-7-slab_0000000000000003.wnd",
	] {
		assert!(names_the_slab(name, three), "{name}");
		// Another slab's id is not matched by it.
		assert!(!names_the_slab(name, zero), "{name}");
		assert!(!names_the_slab(name, fifty), "{name}");
	}
	// Through the snapshot: three objects of node 1 (two of them for one slab), with that
	// node holding the WAL segments and slab 0x32's payload. Slabs 0 and 3 are off its disk,
	// counted once each; slab 0x32 is not. Nodes 2 and 3 own nothing.
	let owner = "00000001-00000000000000000000000000000000";
	let object = |slab: &str, unique: &str| (format!("{owner}/slab_{slab}_{unique}.wnd"), 1_u64);
	let snapshot = Snapshot {
		objects: vec![
			object(zero, "a"),
			object(three, "a"),
			object(three, "b"),
			object(fifty, "a"),
		],
		files: vec![
			(
				1,
				vec![
					"control.wal.00000000000000000000.seg".to_owned(),
					"control.wal.00000000000000000323.seg".to_owned(),
					format!("slab_{fifty}.wnd"),
				],
			),
			(2, Vec::new()),
			(3, Vec::new()),
		],
	};
	let offloaded = snapshot.offloaded();
	assert_eq!(offloaded[0], (vec![zero, three], 4));
	assert_eq!(offloaded[1], (Vec::new(), 0));
	assert_eq!(offloaded[2], (Vec::new(), 0));
}

/// The file names of a host's inventory: one `<bytes><TAB><path>` line per file, then a last
/// line `END<TAB><count>` with the number of file lines. A line of any other shape, a missing
/// or wrong END line, or anything after it is an error: an output that was cut short, or a
/// listing its producer could not finish, proves nothing absent.
fn inventory_names(text: &str) -> Result<Vec<String>, String> {
	let mut lines: Vec<&str> = text.lines().collect();
	let end = lines.pop().ok_or("no END line")?;
	let count: usize = end
		.strip_prefix("END\t")
		.and_then(|count| count.parse().ok())
		.ok_or_else(|| format!("the last line is not END with a count: {end:?}"))?;
	if count != lines.len() {
		return Err(format!("END names {count} files, {} listed", lines.len()));
	}
	lines
		.into_iter()
		.map(|line| {
			let (bytes, path) = line
				.split_once('\t')
				.ok_or_else(|| format!("a line without a tab: {line:?}"))?;
			bytes
				.parse::<u64>()
				.map_err(|_| format!("a line without a size: {line:?}"))?;
			let name = path.rsplit('/').next().unwrap_or(path);
			if name.is_empty() {
				return Err(format!("a line without a file name: {line:?}"));
			}
			Ok(name.to_owned())
		})
		.collect()
}

#[test]
fn a_hosts_inventory_is_read_whole_or_not_at_all() {
	assert_eq!(
		inventory_names(
			"8192\tslab_000000000000003e.wnd\n104\t.wendelon/local-sealed-root-kek.bin\nEND\t2\n"
		),
		Ok(vec![
			"slab_000000000000003e.wnd".to_owned(),
			"local-sealed-root-kek.bin".to_owned()
		])
	);
	assert_eq!(inventory_names("END\t0\n"), Ok(Vec::new()));
	// Cut short: no END, or fewer lines than END names, or more.
	assert!(inventory_names("").is_err());
	assert!(inventory_names("8192\tslab_0000000000000001.wnd\n").is_err());
	assert!(inventory_names("8192\tslab_0000000000000001.wnd\nEND\t2\n").is_err());
	assert!(inventory_names("8192\ta\n8192\tb\nEND\t1\n").is_err());
	// Other shapes.
	assert!(inventory_names("8192 slab_0000000000000001.wnd\nEND\t1\n").is_err());
	assert!(inventory_names("many\tslab_0000000000000001.wnd\nEND\t1\n").is_err());
	assert!(inventory_names("12\tdir/\nEND\t1\n").is_err());
	assert!(inventory_names("L\tslab_0000000000000001.wnd\nEND\t1\n").is_err());
}

/// The store's listing of the run's prefix and every node's data directory, at one moment.
/// The test's own listing, straight to the store: it is not an observation of any daemon's
/// request.
struct Snapshot {
	objects: Vec<(String, u64)>,
	files: Vec<(u32, Vec<String>)>,
}

impl Snapshot {
	/// The listing, or why it is not whole. Never partial: an object listing that failed or a
	/// data directory that could not be read whole yields no snapshot at all.
	async fn try_take(
		formed: &Formed,
		objects: &Arc<dyn ObjectStore>,
		bound: Duration,
	) -> Result<Self, String> {
		let mut listed: Vec<(String, u64)> = tokio::time::timeout(bound, objects.list(""))
			.await
			.map_err(|_| "the object store did not list within its bound".to_owned())?
			.map_err(|error| format!("listing the run's prefix: {error}"))?
			.into_iter()
			.map(|object| (object.key, object.size_bytes))
			.collect();
		listed.sort();
		let mut files = Vec::new();
		for member in &formed.members {
			let mut names = Vec::new();
			match node_control() {
				None => file_names(&member.config.parent().unwrap().join("data"), &mut names)
					.map_err(|why| format!("node {}'s data directory: {why}", member.id))?,
				// The host's own whole listing of the node's data directory, or no snapshot:
				// a call that fails, or a line that is not `<bytes><TAB><path>`, is an error.
				Some(control) => {
					let (ok, stdout, raw) =
						node_control_call(&control, "inventory", member.id, None, CONTROL_BOUND);
					if !ok {
						return Err(format!("node {}'s data directory: {raw}", member.id));
					}
					let text = String::from_utf8(stdout)
						.map_err(|_| format!("node {}'s inventory is not text", member.id))?;
					names = inventory_names(&text)
						.map_err(|why| format!("node {}'s inventory: {why}", member.id))?;
				}
			}
			files.push((member.id, names));
		}
		Ok(Self {
			objects: listed,
			files,
		})
	}

	async fn take(formed: &Formed, objects: &Arc<dyn ObjectStore>, deadline: Instant) -> Self {
		Self::try_take(formed, objects, step(deadline))
			.await
			.unwrap_or_else(|why| panic!("the snapshot is not whole: {why}"))
	}

	/// Per node, in `NODES` order: the DISTINCT slab ids of objects whose key names that node as
	/// owner and of which that node's data directory has no file that `names_the_slab`; and how
	/// many objects name it as owner at all.
	fn offloaded(&self) -> Vec<(Vec<&str>, usize)> {
		NODES
			.iter()
			.map(|id| {
				let (_, files) = self.files.iter().find(|(node, _)| node == id).unwrap();
				let owned: Vec<&str> = self
					.objects
					.iter()
					.filter_map(|(key, _)| owner_and_slab(key))
					.filter(|(node, _)| node == id)
					.map(|(_, slab)| slab)
					.collect();
				let mut off: Vec<&str> = owned
					.iter()
					.copied()
					.filter(|slab| files.iter().all(|name| !names_the_slab(name, slab)))
					.collect();
				off.sort_unstable();
				off.dedup();
				(off, owned.len())
			})
			.collect()
	}

	/// Per node: local files whose name says slab payload.
	fn local_slab_files(&self) -> Vec<usize> {
		NODES
			.iter()
			.map(|id| {
				let (_, files) = self.files.iter().find(|(node, _)| node == id).unwrap();
				files
					.iter()
					.filter(|name| name.starts_with("slab_") && name.ends_with(".wnd"))
					.count()
			})
			.collect()
	}

	fn report(&self, what: &str) {
		let offloaded = self.offloaded();
		eprintln!(
			"smoke: {what}: objects {}; per node 1,2,3: objects owned {:?}, of them off the owner's disk {:?}, local slab payload files {:?}",
			self.objects.len(),
			offloaded
				.iter()
				.map(|(_, owned)| *owned)
				.collect::<Vec<_>>(),
			offloaded
				.iter()
				.map(|(off, _)| off.len())
				.collect::<Vec<_>>(),
			self.local_slab_files(),
		);
	}
}

/// Three real daemons tier to the configured remote object store, reached directly, every one
/// of them offloads its own slabs, and the rows are read back exactly before and after each
/// daemon has been stopped and started again on its own store.
///
/// - Workload: `ROWS` single-row writes, each ONE logical operation under one identity, offered
///   again only as `ingest_the_row` says and within one bound per row; each row a slab of its
///   own (a stress shape for this path, not deployment sizing). A pass does not say a single
///   write call is always answered: see `ingest_the_row` for the failed run that showed it is
///   not. Every node's stall bound is written
///   out at this test's declared profile value (`QUERY_STALL_BOUND_MILLIS`, not the shipped
///   default); rehydration is the default policy.
/// - The offload precondition, required before EACH judged query, for EACH of the three nodes:
///   at least `OFFLOADED_PER_OWNER` objects in the run's prefix whose key names that node as
///   owner, each for a distinct slab of which that node's data directory, listed whole, has no
///   slab file (`names_the_slab`). Other files (the active slab, control state) are not judged. It does not show that
///   every acknowledged row has a current cold copy on each node, nor that an object is the
///   committed one.
/// - A judged query is ONE call with caller deadline `QUERY_DEADLINE`, made right after the
///   precondition was observed, with nothing asked of any node in between. The test waits
///   `QUERY_WATCHDOG_GRACE` longer only to see how a late query ends. It must return every
///   row exactly, complete within `QUERY_DEADLINE` of being asked: a later answer fails. Its outcome is printed first; then the node or nodes the client sent it to (the
///   client's own attempts, the last being the one that answered), and a listing that is
///   reported, not judged: under the default promotion a local copy may be back by then, so
///   the test does not claim that every row of a judged query was read from an object.
/// - Two judged queries: after the first offload, and after the rolling restarts. A restart is
///   an orderly stop (exit 0 on SIGTERM) and a start from the same files; after each, every
///   node's readiness is polled within `READY_BOUND`, as at formation. Readiness is the only
///   thing waited for; a judged query is never asked twice.
/// - Not covered: three hosts, a node that does not return, a leader failure, a partition,
///   another store, any timing claim.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons and needs a remote object store; run alone under an external owner (see the module text)"]
async fn three_daemons_offload_to_the_object_store_and_serve_their_rows_across_restarts() {
	let deadline = Instant::now() + TIERING_TEST_BOUND;
	let store = StoreEnv::from_environment();
	let objects = store.handle();
	// Fail here, by name, when the store does not answer or refuses the keys.
	let before = tokio::time::timeout(CLIENT_BOUND, objects.list(""))
		.await
		.expect("the object store did not answer a list within its bound")
		.expect("the object store refused a list: endpoint, bucket or keys");
	assert!(before.is_empty(), "the run's prefix is not new");
	eprintln!(
		"smoke: object prefix {} in bucket {} at {} (direct, no relay); DECLARED stall bound {QUERY_STALL_BOUND_MILLIS} ms per node (shipped default 250); {ROWS} rows, slabs of {SLAB_MAX_BYTES} bytes",
		store.prefix, store.bucket, store.endpoint
	);
	let started = Instant::now();
	let at = |what: &str| {
		eprintln!(
			"smoke: PHASE {what} at +{} ms",
			started.elapsed().as_millis()
		)
	};

	at("formation");
	let mut formed = form(deadline, Some(&store)).await;
	let rows: Vec<(i64, String)> = (1..=ROWS)
		.map(|id| {
			let name: String = (0..NAME_HEX_BLOCKS).map(|_| hex(random32())).collect();
			(id, name)
		})
		.collect();
	let client = client_through(
		&formed.members[0],
		&formed.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	at("writes");
	let mut offers = 0_u32;
	for (id, name) in &rows {
		offers += ingest_the_row(&client, &formed.descriptor, *id, name, deadline).await;
	}
	eprintln!(
		"smoke: all {ROWS} rows acknowledged in {offers} offers ({} beyond one per row)",
		offers - ROWS as u32
	);

	at("offload wait");
	offloaded_by_every_owner(&mut formed, &objects, deadline, "offload wait").await;
	// No inspection or wait between the offload observation and the judged query.
	at("the judged query before any restart");
	judged_query(&client, &formed, &objects, &rows, "before any restart").await;

	// Each daemon in turn: an orderly stop, a start from the same files, then every node ready.
	for at_member in 0..formed.members.len() {
		let id = formed.members[at_member].id;
		at(&format!("restart of node {id}"));
		formed.daemons.stop(id, deadline);
		formed.daemons.start(&formed.members[at_member]);
		for member in &formed.members {
			wait_ready(
				member,
				&formed.pki,
				&mut formed.daemons,
				deadline,
				&format!("after restarting node {id}"),
			);
		}
	}
	// A client built after the restarts, so no connection of the earlier one is reused.
	let through = client_through(
		&formed.members[0],
		&formed.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	at("offload observation after the restarts");
	offloaded_by_every_owner(&mut formed, &objects, deadline, "after the restarts").await;
	at("the judged query after the restarts");
	judged_query(&through, &formed, &objects, &rows, "after the restarts").await;

	for id in NODES {
		formed.daemons.stop(id, deadline);
	}
	assert!(Instant::now() < deadline, "the test overran its bound");
	// Each daemon's log is part of the record: kept when a record directory is given.
	// On other hosts the logs are on those hosts; whoever launched the run collects them.
	if let (Ok(record), None) = (std::env::var("WENDELON_SMOKE_RECORD_DIR"), node_control()) {
		for member in &formed.members {
			std::fs::copy(
				&member.log,
				Path::new(&record).join(format!("node-{}.daemon.log", member.id)),
			)
			.unwrap();
		}
	}
	std::fs::remove_dir_all(&formed.root).unwrap();
}

/// One row's ingestion: ONE logical operation under ONE operation identity, minted before the
/// first offer and kept, with the same row, schema and options, on every offer. One absolute
/// bound for the row (`CLIENT_BOUND`, or what is left of the test's) covers every call and every
/// pause; an offer never resets it, and an answer after it is not accepted. The same operation
/// is offered again only for the answers `offers_the_same_operation_again` names (a failed
/// connection, a redirect, this operation's own stall or exhausted attempts), after a pause of
/// 250 ms or what is left of the bound. Any other answer, or the bound, fails the test. Only a
/// receipt counts the row as acknowledged. Returns how many offers were made; each failed offer
/// is printed.
///
/// Why offers are repeated here (recorded, not hidden): run pd-offload-20261113T234639Z of this
/// test with one call per row FAILED in this phase, when the control leadership changed with
/// all three daemons alive and one call ended with its attempts exhausted, its outcome
/// indeterminate (DC-236, cause not established). That run stays failed. With this loop a pass
/// claims bounded progress of each logical write with re-offers, and never that a single write
/// call is available without interruption.
async fn ingest_the_row(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	id: i64,
	name: &str,
	deadline: Instant,
) -> u32 {
	let identity = operation_id();
	let started = Instant::now();
	let row_bound = started + CLIENT_BOUND.min(step(deadline));
	let mut offers = 0_u32;
	loop {
		offers += 1;
		let left = row_bound.saturating_duration_since(Instant::now());
		assert!(
			!left.is_zero(),
			"row {id} was not acknowledged within its bound ({} offers before this one)",
			offers - 1
		);
		let answer = tokio::time::timeout(
			left,
			client.table_write(
				TenantId::new(TENANT),
				descriptor,
				one_row(id, name),
				WriteOptions {
					deadline: Some(Instant::now() + left),
					operation_id: Some(identity),
					..WriteOptions::default()
				},
			),
		)
		.await
		.unwrap_or_else(|_| panic!("row {id} had no answer within its bound (offer {offers})"));
		match answer {
			Ok(_) => {
				assert!(
					Instant::now() <= row_bound,
					"row {id} was acknowledged after its bound"
				);
				if offers > 1 {
					eprintln!(
						"smoke: row {id} acknowledged on offer {offers}, {:?} after its first",
						started.elapsed()
					);
				}
				return offers;
			}
			Err(error) if offers_the_same_operation_again(&error, identity) => {
				eprintln!(
					"smoke: offer {offers} of row {id}, {:?} after its first: {error}",
					started.elapsed()
				);
				let pause = Duration::from_millis(250)
					.min(row_bound.saturating_duration_since(Instant::now()));
				tokio::time::sleep(pause).await;
			}
			Err(error) => panic!("the write of row {id} (offer {offers}): {error:?}"),
		}
	}
}

/// Polls, within `TIER_BOUND`, until each of the three nodes has `OFFLOADED_PER_OWNER` of its
/// own slabs in the store and off its own wholly listed disk; prints the counts at every poll
/// and the slab ids when it holds. A daemon that has exited, a listing that is not whole, or the
/// bound fails the step with what was observed. It asks nothing of any node.
async fn offloaded_by_every_owner(
	formed: &mut Formed,
	objects: &Arc<dyn ObjectStore>,
	deadline: Instant,
	what: &str,
) {
	let by = Instant::now() + TIER_BOUND.min(deadline.saturating_duration_since(Instant::now()));
	loop {
		if let Some(exited) = formed.daemons.exited() {
			panic!("{what}: {exited}");
		}
		let snapshot = Snapshot::take(formed, objects, deadline).await;
		snapshot.report(what);
		if snapshot
			.offloaded()
			.iter()
			.all(|(off, _)| off.len() >= OFFLOADED_PER_OWNER)
		{
			for (id, (off, owned)) in NODES.iter().zip(snapshot.offloaded()) {
				eprintln!(
					"smoke: OFFLOADED ({what}) node {id}: {} of its {owned} objects are for slabs with no file on its disk: {off:?}",
					off.len()
				);
			}
			return;
		}
		if Instant::now() >= by {
			panic!(
				"PARTIAL ({what}): within {TIER_BOUND:?} not every node had {OFFLOADED_PER_OWNER} of its slabs in the store and off its own disk"
			);
		}
		tokio::time::sleep(Duration::from_secs(2)).await;
	}
}

/// ONE table query, judged: every row exactly, within the caller's deadline. See the test's
/// text for what is printed and in which order.
async fn judged_query(
	client: &Client,
	formed: &Formed,
	objects: &Arc<dyn ObjectStore>,
	rows: &[(i64, String)],
	what: &str,
) {
	let targets = Arc::new(std::sync::Mutex::new(Vec::new()));
	let noted = targets.clone();
	let asked = Instant::now();
	let call = client.table_query_with_runtime_filter_and_attempt_hook(
		TenantId::new(TENANT),
		&formed.descriptor,
		Vec::new(),
		None,
		TableQueryOptions {
			deadline: Some(asked + QUERY_DEADLINE),
			..TableQueryOptions::default()
		},
		move |attempt: wendelon::client::TableQueryAttempt| {
			noted.lock().unwrap().push(attempt.target.id);
			Ok(Box::new(()) as wendelon::client::TableQueryAttemptGuard)
		},
	);
	let outcome = tokio::time::timeout(QUERY_DEADLINE + QUERY_WATCHDOG_GRACE, call).await;
	let took = asked.elapsed();
	// The judged outcome is in the log before anything else is asked of anyone.
	eprintln!(
		"smoke: QUERY OUTCOME ({what}) after {took:?}: {}; the client's attempts went to nodes {:?}",
		match &outcome {
			Err(_) => "the test's watchdog ended it; no answer of any kind".to_owned(),
			Ok(Err(error)) => format!("an error: {error:?}"),
			Ok(Ok(result)) => format!("a complete answer, {} batches", result.batches.len()),
		},
		targets.lock().unwrap()
	);
	// Reported only. If it cannot be taken, that is said, and the outcome above stands.
	match Snapshot::try_take(formed, objects, CLIENT_BOUND).await {
		Ok(snapshot) => snapshot.report(&format!(
			"after the judged query {what} (reported, not judged)"
		)),
		Err(why) => eprintln!(
			"smoke: the listing after the judged query {what} is UNAVAILABLE ({why}); the query's outcome is unaffected"
		),
	}
	let result = match outcome {
		Err(_) => panic!(
			"QUERY ENDED BY ({what}): the test's watchdog, {took:?} after it was asked (caller deadline {QUERY_DEADLINE:?} plus {QUERY_WATCHDOG_GRACE:?})"
		),
		Ok(Err(error)) => panic!("QUERY ENDED BY ({what}): an error after {took:?}: {error:?}"),
		Ok(Ok(result)) => result,
	};
	// The grace is for seeing how a late query ends, never for accepting it: an answer that
	// completed after the caller's deadline fails, whatever it holds. `took` was captured when
	// the call returned, before the listing above.
	assert!(
		took <= QUERY_DEADLINE,
		"QUERY ENDED BY ({what}): an answer, but {took:?} after it was asked, past the caller's deadline of {QUERY_DEADLINE:?}"
	);
	let mut got = Vec::new();
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
			got.push((ids.value(row), names.value(row).to_owned()));
		}
	}
	got.sort();
	assert_eq!(
		got.iter().map(|row| row.0).collect::<Vec<_>>(),
		rows.iter().map(|row| row.0).collect::<Vec<_>>(),
		"{what}: the ids"
	);
	for (got, want) in got.iter().zip(rows) {
		assert!(got.1 == want.1, "{what}: row {} has another name", want.0);
	}
	let answered_by = targets.lock().unwrap().last().copied();
	eprintln!(
		"smoke: QUERY ENDED BY ({what}): an exact answer of {} rows after {took:?}, from node {answered_by:?}",
		got.len()
	);
}
