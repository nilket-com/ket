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

/// Wendelon ADR 0118 D2: the binary spawns use while set (the cross-binary control
/// forms and exports with the previous build, then restores with this one).
static BINARY_OVERRIDE: std::sync::Mutex<Option<PathBuf>> = std::sync::Mutex::new(None);

fn nilket_bin() -> PathBuf {
	if let Some(path) = BINARY_OVERRIDE
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner)
		.clone()
	{
		return path;
	}
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

#[derive(Clone)]
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

#[derive(Clone)]
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
/// forming node's sealed root KEK file, raw), `crash <id>` (SIGKILL of the node's daemon;
/// exit 0 only when it was running and ended from that signal, its status on stdout),
/// `identity <id>` (`pid <main pid> started <when>` of the node's running daemon; fails when
/// it is not running),
/// `cleanup <id>` (ends the node's owner if it is
/// still there; removes no file). `WENDELON_SMOKE_NODE_ADDRESSES`
/// (`1=host:port,2=host:port,3=host:port`) gives the advertised addresses; each node's
/// health, ready and metrics listeners are on its own loopback at that port plus 10, 11, 12.
fn node_control() -> Option<PathBuf> {
	std::env::var_os("WENDELON_SMOKE_NODE_CONTROL")
		.filter(|path| !path.is_empty())
		.map(PathBuf::from)
}

/// Which cluster's nodes node-control acts on: empty for the first (`node-<id>`), or a name
/// for a second cluster formed in the same run after the first is gone (`PD_CLUSTER`).
static NODE_CONTROL_CLUSTER: std::sync::Mutex<&'static str> = std::sync::Mutex::new("");

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
	let arguments: Vec<std::ffi::OsString> = argument.into_iter().map(Into::into).collect();
	node_control_call_with(control, verb, id, &arguments, bound)
}

/// `node_control_call` with any number of arguments after the node id.
fn node_control_call_with(
	control: &Path,
	verb: &str,
	id: u32,
	arguments: &[std::ffi::OsString],
	bound: Duration,
) -> (bool, Vec<u8>, String) {
	let mut command = Command::new(control);
	command.arg(verb).arg(id.to_string());
	let cluster = *NODE_CONTROL_CLUSTER
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner);
	if !cluster.is_empty() {
		command.env("PD_CLUSTER", cluster);
	}
	command.args(arguments);
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

/// DC-253: the node-control stand-ins this test runs are files of the repository
/// (`tests/node_control_fixtures`, executable in git), never written while tests run. A script
/// written at run time can be held open for writing by another test thread's child between its
/// fork and its exec, and Linux refuses to execute a file open for writing (`ETXTBSY`); see
/// `a_script_held_open_for_writing_cannot_be_executed`.
fn node_control_fixture(name: &str) -> PathBuf {
	PathBuf::from(env!("CARGO_MANIFEST_DIR"))
		.join("tests/node_control_fixtures")
		.join(name)
}

/// DC-253, the mechanism only: executing a file while a descriptor holds it open for writing is
/// refused with `ETXTBSY` on this kernel. It shows the refusal from a writable descriptor; it is
/// not proof of which fork held one in the failures recorded for the test below.
#[test]
fn a_script_held_open_for_writing_cannot_be_executed() {
	use std::io::Write as _;
	use std::os::unix::fs::OpenOptionsExt as _;
	let dir = std::env::temp_dir().join(format!(
		"nilket-pd-etxtbsy-{}-{}",
		std::process::id(),
		uuid::Uuid::new_v4().simple()
	));
	std::fs::create_dir(&dir).unwrap();
	let path = dir.join("held");
	let mut held = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.mode(0o700)
		.open(&path)
		.unwrap();
	held.write_all(b"#!/bin/bash\nexit 0\n").unwrap();
	held.flush().unwrap();
	let refused = Command::new(&path)
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.spawn()
		.map(|mut child| {
			let _ = child.kill();
			let _ = child.wait();
		})
		.expect_err("a file open for writing was executed");
	drop(held);
	assert_eq!(refused.raw_os_error(), Some(26), "not ETXTBSY: {refused}");
	// No second execution of this file: another thread's child may still hold the descriptor
	// just dropped, which is the hazard itself.
	std::fs::remove_dir_all(&dir).unwrap();
}

#[test]
fn a_node_control_call_is_bounded_with_everything_it_started() {
	let script = |name: &str, body: &str| {
		let path = node_control_fixture(name);
		// The fixture is the stand-in this assertion names, as committed.
		assert_eq!(
			std::fs::read_to_string(&path).unwrap(),
			format!("#!/bin/bash\n{body}\n"),
			"{}",
			path.display()
		);
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

	/// An ABRUPT stop of one node: SIGKILL, with no SIGTERM and no drain, and the proof that it
	/// ended from that signal, as text. Anything else fails the step: a node that was not
	/// running, a signal that could not be sent, a node that did not end, or one that ended any
	/// other way (a normal exit is not a crash). Its files are untouched. Only after the
	/// confirmed stop is the node taken out of those expected to run; it stays among the nodes
	/// handed to a host, so its owner is still ended at cleanup.
	fn crash(&mut self, id: u32) -> String {
		if let Some(control) = node_control() {
			let at = self
				.hosted
				.iter()
				.position(|node| *node == id)
				.unwrap_or_else(|| panic!("node {id} is not running"));
			let (ok, stdout, raw) = node_control_call(&control, "crash", id, None, CONTROL_BOUND);
			assert!(ok, "node {id} was not confirmed killed by SIGKILL: {raw}");
			self.hosted.remove(at);
			return String::from_utf8_lossy(&stdout).trim().to_owned();
		}
		let at = self
			.children
			.iter()
			.position(|(node, _)| *node == id)
			.unwrap_or_else(|| panic!("node {id} is not running"));
		let running = self.children[at].1.try_wait().expect("poll the daemon");
		assert!(
			running.is_none(),
			"node {id} had already exited ({running:?}): nothing to crash"
		);
		let pid = self.children[at].1.id();
		// The 10 s run from before the signal is sent.
		let bound = Instant::now() + Duration::from_secs(10);
		assert!(signal(pid, libc::SIGKILL), "SIGKILL to node {id}");
		let status = loop {
			// The observation and the time it was made: an end seen after the bound is late,
			// whatever its status.
			let seen = self.children[at].1.try_wait().expect("wait for the daemon");
			let late = Instant::now() >= bound;
			match seen {
				Some(status) if !late => break status,
				Some(status) => {
					panic!("node {id} was seen ended only after 10 s of SIGKILL ({status})")
				}
				None if late => panic!("node {id} did not end within 10 s of SIGKILL"),
				None => std::thread::sleep(Duration::from_millis(20)),
			}
		};
		assert_eq!(
			std::os::unix::process::ExitStatusExt::signal(&status),
			Some(libc::SIGKILL),
			"node {id} ended, but not from SIGKILL: {status}"
		);
		let (_, mut child) = self.children.remove(at);
		// Whatever else was in its process group.
		kill_group(child.id());
		let _ = child.wait();
		format!("node {id} ended by signal 9 ({status})")
	}

	/// A crashed node is still not running. Other hosts: its unit is asked; anything but a
	/// confirmed not-running answer fails. Here the child was reaped by `crash`.
	fn still_down(&self, id: u32) {
		assert!(
			!self.hosted.contains(&id) && self.children.iter().all(|(node, _)| *node != id),
			"node {id} is among the running"
		);
		if let Some(control) = node_control() {
			let (ok, stdout, raw) = node_control_call(&control, "state", id, None, CONTROL_BOUND);
			let state = String::from_utf8_lossy(&stdout);
			assert!(
				ok && (state.starts_with("absent") || state.starts_with("ended")),
				"node {id} is not confirmed down: {raw}"
			);
		}
	}

	/// Which process a running node's daemon is, as text that changes if it is ever another
	/// one: here its pid and kernel start time; on another host what `identity <id>` prints
	/// (the unit's main pid and the time it was started). A node that is not running, or whose
	/// identity cannot be read, fails the step.
	fn identity(&self, id: u32) -> String {
		if let Some(control) = node_control() {
			assert!(self.hosted.contains(&id), "node {id} is not running");
			let (ok, stdout, raw) =
				node_control_call(&control, "identity", id, None, CONTROL_BOUND);
			let text = String::from_utf8_lossy(&stdout).trim().to_owned();
			assert!(
				ok && text.starts_with("pid "),
				"node {id}: its process identity could not be read: {raw}"
			);
			return text;
		}
		let child = self
			.children
			.iter()
			.find(|(node, _)| *node == id)
			.unwrap_or_else(|| panic!("node {id} is not running"));
		let pid = child.1.id();
		let (_, started) = process_state(pid).unwrap_or_else(|why| panic!("node {id}: {why}"));
		format!("pid {pid} started {started}")
	}

	/// Wendelon ADR 0108: the exit status of node `id`'s own process if it has ended (reaped,
	/// and no longer counted as running), or `None` while it runs. On other hosts the unit's
	/// last execution is read once (`exit-status`) and must be `owned`, the execution this test
	/// started: another one, or a read that is not whole, fails the step.
	fn ended(&mut self, id: u32, owned: &str) -> Option<std::process::ExitStatus> {
		if let Some(control) = node_control() {
			assert!(self.hosted.contains(&id), "node {id} is not running");
			let (ok, stdout, raw) =
				node_control_call(&control, "exit-status", id, None, CONTROL_BOUND);
			assert!(ok, "node {id}: its execution could not be read: {raw}");
			let status = owned_end(&String::from_utf8_lossy(&stdout), owned)
				.unwrap_or_else(|why| panic!("node {id}: {why}"))?;
			self.hosted.retain(|node| *node != id);
			return Some(status);
		}
		let at = self
			.children
			.iter()
			.position(|(node, _)| *node == id)
			.unwrap_or_else(|| panic!("node {id} is not running"));
		let status = self.children[at].1.try_wait().expect("poll a daemon")?;
		self.children.remove(at);
		Some(status)
	}

	/// A daemon other than `id` that should be running and is not.
	fn exited_except(&mut self, id: u32) -> Option<String> {
		if let Some(control) = node_control() {
			for node in self.hosted.iter().filter(|node| **node != id) {
				let (ok, stdout, raw) =
					node_control_call(&control, "state", *node, None, CONTROL_BOUND);
				if !ok || String::from_utf8_lossy(&stdout).trim() != "running" {
					return Some(format!("node {node} is not running: {raw}"));
				}
			}
			return None;
		}
		for (node, child) in &mut self.children {
			if *node == id {
				continue;
			}
			if let Some(status) = child.try_wait().expect("poll a daemon") {
				return Some(format!("node {node} exited {status}"));
			}
		}
		None
	}

	/// Starts node `member` and returns the identity of the execution started, read once right
	/// after the start whether or not it still runs (on other hosts `start` prints it).
	fn start_identified(&mut self, member: &Member) -> String {
		if let Some(control) = node_control() {
			let (ok, stdout, raw) =
				node_control_call(&control, "start", member.id, None, CONTROL_BOUND);
			assert!(ok, "{raw}");
			self.hosted.push(member.id);
			return parse_identity(String::from_utf8_lossy(&stdout).trim_end_matches('\n'))
				.unwrap_or_else(|why| panic!("node {}: {why}", member.id));
		}
		self.start(member);
		self.identity(member.id)
	}

	/// Other hosts only: one call of `verb` for node `id` within the step's absolute deadline
	/// `by` (Codex 01a1e49d R3): the wrapper gets only the time left before `by`, and an answer
	/// observed after it is refused, whatever it says.
	fn hosted_call(
		&self,
		verb: &str,
		id: u32,
		arguments: &[std::ffi::OsString],
		by: Instant,
	) -> (Vec<u8>, String) {
		let control = node_control().expect("other hosts only");
		let left = by.saturating_duration_since(Instant::now());
		assert!(!left.is_zero(), "node {id}: no time left for {verb}");
		let (ok, stdout, raw) = node_control_call_with(&control, verb, id, arguments, left);
		answered_within(Instant::now(), by)
			.unwrap_or_else(|why| panic!("node {id}: {verb} {why}: {raw}"));
		assert!(ok, "node {id}: {verb}: {raw}");
		(stdout, raw)
	}

	/// Other hosts: node `id`'s data directory set aside as `old-data` (kept whole), a new one
	/// holding only the cluster's sealed root copied on that host; within `CONTROL_BOUND`.
	fn lose(&self, id: u32, deadline: Instant) {
		let by = deadline.min(Instant::now() + CONTROL_BOUND);
		self.hosted_call("lose", id, &[], by);
	}

	/// Other hosts: the blank start's directory set aside, and a new one as `lose` makes.
	fn blank_aside(&self, id: u32, deadline: Instant) {
		let by = deadline.min(Instant::now() + CONTROL_BOUND);
		self.hosted_call("blank-aside", id, &[], by);
	}

	/// Other hosts: one start of node `id`'s daemon from its configuration within the step's
	/// deadline `by`: its unit gets the whole seconds left (at most 60) and the call the time
	/// left; its output before the result, the result, and the raw call text.
	fn run_once(&self, id: u32, by: Instant) -> (String, OneOff, String) {
		let seconds = phase_seconds(by, Instant::now(), 60)
			.unwrap_or_else(|why| panic!("node {id}: run-once: {why}"));
		let (stdout, raw) = self.hosted_call("run-once", id, &[seconds.to_string().into()], by);
		let (before, result) = parse_one_off(&String::from_utf8_lossy(&stdout))
			.unwrap_or_else(|why| panic!("node {id}: run-once: {why}; the whole call: {raw}"));
		(before, result, raw)
	}

	/// Other hosts: `replace provision` into node `id`'s data directory, on its host, within
	/// the step's deadline `by` (its unit gets the whole seconds left, at most 45).
	fn provision(&self, id: u32, token: u128, by: Instant) -> String {
		let seconds = phase_seconds(by, Instant::now(), REPLACE_COMMAND_BOUND.as_secs())
			.unwrap_or_else(|why| panic!("node {id}: provision: {why}"));
		let (stdout, raw) = self.hosted_call(
			"provision",
			id,
			&[format!("{token:032x}").into(), seconds.to_string().into()],
			by,
		);
		let (_, result) = parse_one_off(&String::from_utf8_lossy(&stdout))
			.unwrap_or_else(|why| panic!("node {id}: provision: {why}; the whole call: {raw}"));
		assert_eq!(result, OneOff::Exit(0), "node {id}: provision: {raw}");
		raw
	}

	/// Other hosts: node `id`'s STOPPED store, read in place on its host by this run's pinned
	/// test binary (`hosted_store_probe`), whole or refused, within `by`.
	fn probe(&self, id: u32, descriptor: Option<&TableSchemaDescriptor>, by: Instant) -> Probed {
		let argument = descriptor.map(|descriptor| {
			base64_of(&serde_json::to_vec(descriptor).expect("the descriptor as JSON"))
		});
		let kind = if descriptor.is_some() {
			"recovered"
		} else {
			"role"
		};
		let control = node_control().expect("other hosts only");
		let mut command_arguments = vec![std::ffi::OsString::from(kind)];
		command_arguments.extend(argument.map(std::ffi::OsString::from));
		let left = by.saturating_duration_since(Instant::now());
		assert!(
			!left.is_zero(),
			"node {id}: no time left for the store probe"
		);
		let (ok, stdout, raw) = node_control_call_with(
			&control,
			"store-probe",
			id,
			&command_arguments,
			left.min(PROBE_UNIT_BOUND + CONTROL_BOUND),
		);
		answered_within(Instant::now(), by)
			.unwrap_or_else(|why| panic!("node {id}: the store probe {why}: {raw}"));
		assert!(ok, "node {id}: store-probe {kind}: {raw}");
		probe_answer(&stdout, &raw)
			.unwrap_or_else(|why| panic!("node {id}: store-probe {kind}: {why}"))
	}

	/// Other hosts: the old instance of node `id` started on its old disk (`old-data`) under
	/// the node's unit; the identity of that execution.
	fn start_old(&mut self, id: u32) -> String {
		let control = node_control().expect("other hosts only");
		let (ok, stdout, raw) = node_control_call(&control, "start-old", id, None, CONTROL_BOUND);
		assert!(ok, "node {id}: start-old: {raw}");
		self.hosted.push(id);
		parse_identity(String::from_utf8_lossy(&stdout).trim_end_matches('\n'))
			.unwrap_or_else(|why| panic!("node {id}: {why}"))
	}

	/// Other hosts: the old instance's log, whole.
	fn old_log(&self, id: u32) -> String {
		let control = node_control().expect("other hosts only");
		let (ok, stdout, raw) = node_control_call(
			&control,
			"log",
			id,
			Some(std::ffi::OsStr::new("old")),
			CONTROL_BOUND,
		);
		assert!(ok, "node {id}: the old instance's log: {raw}");
		String::from_utf8_lossy(&stdout).into_owned()
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
			report_consensus_timing(member, &inspection, what);
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
	/// Each member's configured decision verifying key, in `NODES` order.
	decision_keys: Vec<[u8; 32]>,
}

/// A node's `[storage]` body and what follows `[tuning]`'s first lines. With no store both are
/// what the configuration always held. With one: small slabs, the store, an eager tiering
/// scan, and the stall bound written out at the test's declared value.
fn storage_section(dir: &Path, store: Option<&StoreEnv>, declared: bool) -> (String, String) {
	// The declared profile (stall bound, and the consensus base when one is declared) is
	// written with a store, or without one when the caller asks (`declared`); with neither,
	// the configuration states no profile and the product defaults apply.
	let tuning = format!(
		"query_stall_bound_millis = {QUERY_STALL_BOUND_MILLIS}\n{}",
		declared_consensus_base()
			.map(|base| format!("consensus_timing_base_millis = {base}\n"))
			.unwrap_or_default()
	);
	let Some(store) = store else {
		let tuning = if declared { tuning } else { String::new() };
		return ("data_dir = \"data\"\n".to_owned(), tuning);
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
		tuning,
	)
}

/// A consensus timing base declared for every node of an object-store test by
/// `WENDELON_SMOKE_CONSENSUS_TIMING_BASE_MILLIS`. Unset: the key is not written, every node
/// runs the default profile and nothing here prints or checks anything. Set to anything that
/// is not a base the product accepts: the test stops, so a mistyped value is never taken for
/// the default.
fn declared_consensus_base() -> Option<u64> {
	consensus_base_of(std::env::var_os("WENDELON_SMOKE_CONSENSUS_TIMING_BASE_MILLIS").as_deref())
		.unwrap_or_else(|why| panic!("WENDELON_SMOKE_CONSENSUS_TIMING_BASE_MILLIS: {why}"))
}

/// The declared base in `value`: `None` when nothing is declared; an error for text that is
/// not Unicode, not a whole number, or outside the range the product accepts.
fn consensus_base_of(value: Option<&std::ffi::OsStr>) -> Result<Option<u64>, String> {
	let Some(value) = value else {
		return Ok(None);
	};
	let text = value
		.to_str()
		.ok_or_else(|| format!("not Unicode: {value:?}"))?;
	let base: u64 = text
		.parse()
		.map_err(|_| format!("not a whole number of milliseconds: {text:?}"))?;
	wendelon::consensus::timing::ConsensusTiming::from_base_millis(base)
		.map_err(|_| format!("{base} is outside the range the product accepts"))?;
	Ok(Some(base))
}

#[test]
fn a_declared_consensus_base_is_taken_whole_or_refused() {
	use std::ffi::OsStr;
	use std::os::unix::ffi::OsStrExt;
	assert_eq!(consensus_base_of(None), Ok(None));
	assert_eq!(consensus_base_of(Some(OsStr::new("250"))), Ok(Some(250)));
	assert_eq!(consensus_base_of(Some(OsStr::new("10"))), Ok(Some(10)));
	assert_eq!(consensus_base_of(Some(OsStr::new("1000"))), Ok(Some(1000)));
	for refused in [
		"",
		" 250",
		"250 ",
		"250ms",
		"-1",
		"2.5",
		"0",
		"9",
		"1001",
		"99999999999999999999",
	] {
		assert!(
			consensus_base_of(Some(OsStr::new(refused))).is_err(),
			"{refused:?} was taken"
		);
	}
	assert!(consensus_base_of(Some(OsStr::from_bytes(b"25\xff"))).is_err());
}

/// With a profile declared: the profile this node reports is printed and must be the declared
/// base and the values derived from it (ADR 0731). With none declared this does nothing.
fn report_consensus_timing(member: &Member, inspection: &Inspection, what: &str) {
	let Some(base) = declared_consensus_base() else {
		return;
	};
	let timing = inspection
		.body
		.as_ref()
		.and_then(|body| body.get("consensus_timing"))
		.cloned()
		.unwrap_or(serde_json::Value::Null);
	eprintln!(
		"smoke: node {} consensus_timing ({what}): {timing}",
		member.id
	);
	assert_eq!(
		timing,
		serde_json::json!({
			"base_millis": base,
			"heartbeat_millis": base,
			"election_min_millis": 3 * base,
			"election_max_millis": 6 * base,
			"snapshot_chunk_millis": (4 * base).max(200),
		}),
		"{what}: node {} does not report the declared consensus profile",
		member.id
	);
}

async fn form(deadline: Instant, store: Option<&StoreEnv>) -> Formed {
	form_cluster(deadline, store, "", true, false).await
}

/// `form`, with the run directory's name suffixed by `label` (a second cluster in the same
/// process), with the tenant and its table created only when `establish` is set (a cluster
/// formed without them holds only what formation creates), and with the run's declared
/// profile written even without a store when `declared_without_store` is set.
async fn form_cluster(
	deadline: Instant,
	store: Option<&StoreEnv>,
	label: &str,
	establish: bool,
	declared_without_store: bool,
) -> Formed {
	// Kept on failure for its logs; removed only at the end of a pass. Holds secrets: it is
	// created private and is never printed.
	let root = std::env::temp_dir().join(format!("nilket-pd-smoke-{}{label}", std::process::id()));
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
	let decision_keys: Vec<[u8; 32]> = secrets
		.iter()
		.map(|secret| wendelon::consensus::decisions::decision_verifying_key_bytes(secret.decision))
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
			// ADR 0104 (wendelon): the one permission to form, provisioned beside
			// the forming member's fresh store and consumed at its first start.
			std::fs::write(dir.join("data").join("FORMATION_INTENT"), b"").unwrap();
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
		let (storage, stall) = storage_section(&dir, store, declared_without_store);
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

	let descriptor = descriptor();
	if !establish {
		return Formed {
			root,
			pki,
			members,
			daemons,
			descriptor,
			decision_keys,
		};
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
		decision_keys,
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
		..
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

	// Wendelon ADR 0112: ONE delete statement of three written rows and one never written,
	// served on the daemons' own worker stacks: one receipt, exact counts, and the row of
	// the first stage stays.
	let mut doomed = Vec::new();
	for id in [201_i64, 202, 203] {
		let bound = step(deadline);
		let written = one_row(id, "doomed");
		doomed.extend(
			wendelon::schema::table_row_identities(TenantId::new(TENANT), &descriptor, &written)
				.expect("the row's identity"),
		);
		tokio::time::timeout(
			bound,
			client.table_write(
				TenantId::new(TENANT),
				&descriptor,
				written,
				WriteOptions {
					deadline: Some(Instant::now() + bound),
					..WriteOptions::default()
				},
			),
		)
		.await
		.unwrap_or_else(|_| panic!("the write of row {id} had no answer within its bound"))
		.unwrap_or_else(|error| panic!("the write of row {id}: {error:?}"));
	}
	doomed.extend(
		wendelon::schema::table_row_identities(
			TenantId::new(TENANT),
			&descriptor,
			&one_row(299, "never"),
		)
		.expect("the absent row's identity"),
	);
	let delete_id = operation_id();
	let bound = step(deadline);
	let receipt = tokio::time::timeout(
		bound,
		client.table_delete(
			TenantId::new(TENANT),
			&descriptor,
			doomed,
			WriteOptions {
				deadline: Some(Instant::now() + bound),
				operation_id: Some(delete_id),
				..WriteOptions::default()
			},
		),
	)
	.await
	.expect("the delete statement had no answer within its bound")
	.unwrap_or_else(|error| panic!("the delete statement: {error:?}"));
	assert_eq!(
		(
			receipt.deleted_count,
			receipt.already_deleted_count,
			receipt.not_found_count
		),
		(3, 0, 1),
		"one statement's counts: {receipt:?}"
	);
	for member in &members {
		let through = client_through(member, &pki, Credential::Storage, deadline).await;
		expect_the_row(
			&through,
			&descriptor,
			deadline,
			&format!("after the delete statement, through node {}", member.id),
		)
		.await;
		// Wendelon ADR 0113: the statement resolves, through every node, to its
		// committed receipt; a never-used id to UNKNOWN.
		let resolved = tokio::time::timeout(
			step(deadline),
			through.resolve_table_mutation(TenantId::new(TENANT), delete_id),
		)
		.await
		.expect("the resolve had no answer within its bound")
		.unwrap_or_else(|error| panic!("resolve through node {}: {error:?}", member.id));
		assert!(
			matches!(
				resolved,
				wendelon_client::ResolvedMutationOutcome::Present {
					status: wendelon::schema::WriteStatus::OK,
					knowledge: wendelon::schema::MutationCommitKnowledge::COMMITTED,
					delete_counts: Some([3, 0, 1]),
					..
				}
			),
			"through node {}: {resolved:?}",
			member.id
		);
		let never = tokio::time::timeout(
			step(deadline),
			through.resolve_table_mutation(TenantId::new(TENANT), operation_id()),
		)
		.await
		.expect("the resolve had no answer within its bound")
		.unwrap_or_else(|error| panic!("resolve through node {}: {error:?}", member.id));
		assert!(
			matches!(
				never,
				wendelon_client::ResolvedMutationOutcome::Unknown { .. }
			),
			"through node {}: {never:?}",
			member.id
		);
		// A public table-query cancel reaches each real node's handler and is answered,
		// typed, never a dropped stream: an id not yet running is ACCEPTED (the existing
		// early-cancel contract).
		let cancelled = tokio::time::timeout(
			step(deadline),
			through.cancel_table_query_attempt(
				wendelon::schema::NodeId {
					id: member.id,
					_reserved: 0,
				},
				TenantId::new(TENANT),
				wendelon::schema::TableQueryRequestId::new(*uuid::Uuid::now_v7().as_bytes()),
			),
		)
		.await
		.expect("the cancel had no answer within its bound")
		.unwrap_or_else(|error| panic!("cancel at node {}: {error:?}", member.id));
		assert_eq!(
			cancelled.status,
			wendelon::schema::TableQueryCancelStatus::ACCEPTED,
			"at node {}",
			member.id
		);
	}
	// Over the bound: the public client's 513-row statement is refused typed (batch too
	// large) from its declared count, and the first stage's row, among the 513, stays.
	let oversized: Vec<_> = (7_i64..520)
		.flat_map(|id| {
			wendelon::schema::table_row_identities(
				TenantId::new(TENANT),
				&descriptor,
				&one_row(id, "x"),
			)
			.expect("an identity")
		})
		.collect();
	assert_eq!(oversized.len(), 513);
	let bound = step(deadline);
	let refused = tokio::time::timeout(
		bound,
		client.table_delete(
			TenantId::new(TENANT),
			&descriptor,
			oversized,
			WriteOptions {
				deadline: Some(Instant::now() + bound),
				..WriteOptions::default()
			},
		),
	)
	.await
	.expect("the oversized statement had no answer within its bound");
	assert!(
		matches!(
			refused,
			Err(ClientError::TableDeleteRejected { reject_code })
				if reject_code == wendelon::schema::PbcRejectCode::PBC_REJECT_BATCH_TOO_LARGE
		),
		"a typed refusal: {refused:?}"
	);
	expect_the_row(
		&client,
		&descriptor,
		deadline,
		"after the oversized statement",
	)
	.await;

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
	pause_recovery_workflow(Paused::All).await;
}

/// The same workflow, with one difference: only the daemons that are NOT the leader observed
/// at selection are stopped; that one process keeps running for the whole interval.
///
/// The leader is read from all three nodes' readiness answers, which must name the same
/// node; nothing assumes which node that is. Reading three answers is not an atomic fact:
/// leadership may change between the reads and the first SIGSTOP, and after it. "The leader"
/// below always means the leader observed at selection.
///
/// With two of three voters stopped there is no quorum. The running node can receive a
/// request but cannot be assumed to decide anything, and it may stop being leader; a change
/// of leadership across the pause is reported, not judged. This is a scheduling pause that
/// removes the quorum. It is not a stalled disk and not a partition, and it guarantees
/// neither a takeover nor a refusal: when no refusal occurs, those branches of the oracle
/// were not exercised, and the log says so. The running process is checked to be the same
/// live, unstopped process at both ends of the interval, which does not show that it had
/// the processor throughout.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons and stops two with signals; run alone under an external owner (see the module text)"]
async fn three_daemons_resume_service_without_restart_after_the_followers_are_paused() {
	pause_recovery_workflow(Paused::AllButTheObservedLeader).await;
}

/// Which daemons the workflow stops.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Paused {
	All,
	AllButTheObservedLeader,
}

/// What each node's readiness answer names as leader, read once each, now.
fn leader_views(members: &[Member], pki: &Pki, limit: Instant) -> Vec<(u32, Option<u32>)> {
	members
		.iter()
		.map(|member| {
			assert!(
				Instant::now() < limit,
				"the leader-view bound passed before inspection"
			);
			let view = inspect(member, pki).leader();
			assert!(
				Instant::now() <= limit,
				"a leader view came after its bound"
			);
			(member.id, view)
		})
		.collect()
}

/// The node all three readiness answers name as leader, read again until they agree or
/// `limit` passes. No agreement inside the limit ends the test before any signal.
fn observed_leader(members: &[Member], pki: &Pki, limit: Instant) -> u32 {
	loop {
		let views = leader_views(members, pki, limit);
		let named: Vec<u32> = views.iter().filter_map(|(_, leader)| *leader).collect();
		assert!(
			Instant::now() < limit,
			"CONTROL, before any signal: leader selection exceeded its bound: {views:?}"
		);
		if named.len() == members.len() && named.iter().all(|leader| *leader == named[0]) {
			return named[0];
		}
		std::thread::sleep(Duration::from_millis(100));
	}
}

async fn pause_recovery_workflow(which: Paused) {
	let deadline = Instant::now() + TEST_BOUND;
	let Formed {
		root,
		pki,
		members,
		mut daemons,
		descriptor,
		..
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

	// Which daemons are stopped. For the followers' case the leader is the one all three
	// nodes name now; it is an observation at selection, not a promise about the interval.
	let (paused, running): (Vec<OwnedProcess>, Vec<OwnedProcess>) = match which {
		Paused::All => (processes.clone(), Vec::new()),
		Paused::AllButTheObservedLeader => {
			let selected_at = Instant::now();
			let leader = observed_leader(
				&members,
				&pki,
				selected_at + READY_BOUND.min(step(deadline)),
			);
			let (running, paused): (Vec<OwnedProcess>, Vec<OwnedProcess>) = processes
				.iter()
				.copied()
				.partition(|process| process.node == leader);
			assert_eq!((running.len(), paused.len()), (1, NODES.len() - 1));
			eprintln!(
				"smoke: SELECTION: all three nodes named node {leader} as leader, {:?} after the baseline writes; left running: {running:?}; to be stopped: {paused:?}",
				selected_at.duration_since(before_everything)
			);
			(paused, running)
		}
	};
	// A process left running must be the same process, alive and not stopped.
	let check_running = |when: &str| {
		for process in &running {
			let state = process
				.state()
				.unwrap_or_else(|why| panic!("{when}: {why}"));
			assert!(
				matches!(state, 'R' | 'S' | 'D' | 'I'),
				"{when}: node {}, left running, is in state {state}",
				process.node
			);
		}
	};

	// The pause. The guard exists before the first stop, covers every daemon, and outlives
	// every step below.
	let resume_on_exit = ResumeOnExit(processes.clone());
	let (stop_sent, stopped) = pause_all(&paused).unwrap_or_else(|why| panic!("pause: {why}"));
	check_running("at the start of the pause");
	eprintln!(
		"smoke: PAUSE: {} of three stopped (nodes {:?}); first signal to last confirmation {:?}",
		paused.len(),
		paused
			.iter()
			.map(|process| process.node)
			.collect::<Vec<_>>(),
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
	for process in &paused {
		let state = process.state().unwrap_or_else(|why| panic!("pause: {why}"));
		assert_eq!(state, 'T', "node {} did not stay stopped", process.node);
	}
	check_running("at the end of the pause");

	// The release. The recovery budget starts BEFORE the first SIGCONT.
	let release = Instant::now();
	let mark = release + RECOVERY_BOUND;
	let runnable = resume_all(&paused).unwrap_or_else(|why| panic!("release: {why}"));
	eprintln!(
		"smoke: RELEASE: each stopped daemon was stopped for at least {:?} (last stop confirmed to first SIGCONT); all of them confirmed runnable {:?} after the release began. Recovery bound {RECOVERY_BOUND:?} from the release",
		release - stopped,
		runnable - release
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
	// Extra role observations belong only to the new subset case. The landed all-paused
	// case keeps its original call sequence and recovery budget.
	if which == Paused::AllButTheObservedLeader {
		let views = leader_views(&members, &pki, mark);
		eprintln!("smoke: RECOVERY: leader named by each node once all were Ready: {views:?}");
		assert!(
			Instant::now() <= mark,
			"the leader views came after the recovery bound"
		);
	}

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
		"smoke: RESULT 2 (after the release): Ready after {:?}, everything settled and a fresh write committed after {:?}; operations {}: committed {}, refused {}, new operations after a refusal {new_operations}{}",
		all_ready - release,
		progressed - release,
		operations.len(),
		count(Disposition::Committed),
		count(Disposition::Refused),
		if count(Disposition::Refused) == 0 {
			"; NO refusal occurred: the refusal and new-operation branches were NOT exercised"
		} else {
			""
		}
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
/// text for what is printed and in which order. Returns the node the client's last attempt
/// went to, which is the node whose answer was returned.
async fn judged_query(
	client: &Client,
	formed: &Formed,
	objects: &Arc<dyn ObjectStore>,
	rows: &[(i64, String)],
	what: &str,
) -> u32 {
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
	answered_by.expect("the answered call made an attempt")
}

/// A stand-in child for the crash bookkeeping checks: a process in a group of its own.
#[cfg(test)]
fn stand_in_child(command: &str) -> Child {
	Command::new("/bin/sh")
		.args(["-c", command])
		.stdin(Stdio::null())
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.process_group(0)
		.spawn()
		.unwrap()
}

#[test]
fn a_crash_is_confirmed_as_an_end_from_sigkill_and_only_then_is_the_node_down() {
	let mut daemons = Daemons {
		children: vec![
			(2, stand_in_child("sleep 30")),
			(3, stand_in_child("sleep 30")),
		],
		prepared: Vec::new(),
		hosted: Vec::new(),
	};
	let proof = daemons.crash(2);
	assert!(proof.contains("signal 9"), "{proof}");
	daemons.still_down(2);
	// The other node is still expected to run, and does.
	assert_eq!(daemons.children.len(), 1);
	assert_eq!(daemons.exited(), None);
}

#[test]
#[should_panic(expected = "had already exited")]
fn a_node_that_had_already_exited_cannot_be_counted_as_crashed() {
	let mut child = stand_in_child("exit 0");
	// It has ended by itself before any signal is sent.
	let bound = Instant::now() + Duration::from_secs(5);
	while child.try_wait().unwrap().is_none() {
		assert!(Instant::now() < bound);
		std::thread::sleep(Duration::from_millis(10));
	}
	let mut daemons = Daemons {
		children: vec![(2, child)],
		prepared: Vec::new(),
		hosted: Vec::new(),
	};
	daemons.crash(2);
}

#[test]
#[should_panic(expected = "is among the running")]
fn a_running_node_is_not_reported_down() {
	let daemons = Daemons {
		children: vec![(2, stand_in_child("sleep 30"))],
		prepared: Vec::new(),
		hosted: Vec::new(),
	};
	daemons.still_down(2);
}

/// ONE logical row to an acknowledgement, by the recovery-aware rules of the workflow below
/// (`settle_one`, `confirm_refusal`, the journal and its caps, which count over the whole
/// run): the original under key `row`; after each recorded refusal, confirmed, one replacement
/// under a new key. Every call ends no later than `limit`; an operation's own bound is
/// `CLIENT_BOUND` or what is left before `limit`. No clock starts at a refusal here. Returns
/// when the acknowledgement was observed.
async fn ingest_logical_row(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	journal: &mut Journal,
	row: i64,
	limit: Instant,
	stage: &str,
) -> Instant {
	let mut ordinal = 0_u32;
	loop {
		let index = journal.begin(row, ordinal);
		let what = if ordinal == 0 {
			format!("{stage}: original offer")
		} else {
			format!("{stage}: replacement offer")
		};
		let bound = (Instant::now() + CLIENT_BOUND).min(limit);
		let observed = settle_one(client, descriptor, journal, index, &what, bound).await;
		if journal.operations[index].disposition == Disposition::Committed {
			return observed;
		}
		journal.refusals += 1;
		journal.first_refusal.get_or_insert(observed);
		assert!(
			ordinal < MAX_REPLACEMENTS_PER_ROW && journal.replacements < MAX_REPLACEMENTS,
			"{stage}: logical row {row}: refused, and no replacement is left (this row {ordinal} of {MAX_REPLACEMENTS_PER_ROW}, the run {} of {MAX_REPLACEMENTS})",
			journal.replacements
		);
		ordinal += 1;
		journal.replacements += 1;
		let confirm_by = (Instant::now() + CLIENT_BOUND).min(limit);
		confirm_refusal(client, descriptor, journal, index, confirm_by).await;
	}
}

/// ONE table query that must return exactly `rows`, ended no later than `limit` (and no later
/// than `QUERY_DEADLINE` from when it is asked). Nothing else is asked of anyone inside it.
/// Returns the node whose answer was returned and the instant the call completed.
async fn exact_query_within(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	rows: &[(i64, String)],
	limit: Instant,
	what: &str,
) -> (u32, Instant) {
	let asked = Instant::now();
	let call_deadline = (asked + QUERY_DEADLINE).min(limit);
	assert!(
		asked < call_deadline,
		"{what}: no time is left for the query"
	);
	let targets = Arc::new(std::sync::Mutex::new(Vec::new()));
	let noted = targets.clone();
	let outcome = tokio::time::timeout_at(
		tokio::time::Instant::from_std(call_deadline),
		client.table_query_with_runtime_filter_and_attempt_hook(
			TenantId::new(TENANT),
			descriptor,
			Vec::new(),
			None,
			TableQueryOptions {
				deadline: Some(call_deadline),
				..TableQueryOptions::default()
			},
			move |attempt: wendelon::client::TableQueryAttempt| {
				noted.lock().unwrap().push(attempt.target.id);
				Ok(Box::new(()) as wendelon::client::TableQueryAttemptGuard)
			},
		),
	)
	.await;
	let completed = Instant::now();
	eprintln!(
		"smoke: QUERY OUTCOME ({what}) after {:?} (caller deadline {:?} from when it was asked): {}; the client's attempts went to nodes {:?}",
		completed - asked,
		call_deadline - asked,
		match &outcome {
			Err(_) => "no answer of any kind by its deadline".to_owned(),
			Ok(Err(error)) => format!("an error: {error:?}"),
			Ok(Ok(result)) => format!("a complete answer, {} batches", result.batches.len()),
		},
		targets.lock().unwrap()
	);
	let result = match outcome {
		Err(_) => panic!("{what}: the query had no answer by its deadline"),
		Ok(Err(error)) => panic!("{what}: the query: {error:?}"),
		Ok(Ok(result)) => result,
	};
	assert!(
		completed <= call_deadline,
		"{what}: the answer completed after its deadline"
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
	let mut want = rows.to_vec();
	want.sort();
	assert_eq!(
		got.iter().map(|row| row.0).collect::<Vec<_>>(),
		want.iter().map(|row| row.0).collect::<Vec<_>>(),
		"{what}: the keys"
	);
	for (got, want) in got.iter().zip(&want) {
		assert!(got.1 == want.1, "{what}: key {} has another name", want.0);
	}
	let answered_by = targets
		.lock()
		.unwrap()
		.last()
		.copied()
		.expect("the answered call made an attempt");
	(answered_by, completed)
}

/// Three real daemons with a remote object store; the node that served a read is KILLED; the
/// two that remain acknowledge a new logical row and answer an exact read; the killed node
/// returns on its own files and the cluster answers a read of everything acknowledged.
///
/// Profile. Whatever `WENDELON_SMOKE_CONSENSUS_TIMING_BASE_MILLIS` declares is written to
/// every node and each node's reported profile must equal it whenever the node is Ready, the
/// returned node included. Leases and every other timer are the product's; the query stall
/// bound is this file's declared one. A declared base is a profile for a fixture: it promises
/// no latency and no lease survives a leader's death because of it.
///
/// Writes, before the kill and after it, follow the recovery-aware workflow's rules with ONE
/// journal and its caps for the whole run: an unknown outcome keeps its identity and row; only
/// the recorded refusal, confirmed, permits a replacement under a new key; a refusal is
/// terminal. A truthful refusal is therefore allowed at any point, the outage included.
///
/// - Formation, the `ROWS` logical rows, the all-owner offload precondition and the first
///   judged query are the object-store workflow's. A failure there is a failure of that
///   workflow at this profile, not an observation of survival.
/// - S is the node that first query's answer came from. What each node names as control leader
///   is printed just before the kill; S is NOT required to be it, and when it is not, a pass
///   says nothing about losing the control leader.
/// - The kill is SIGKILL of S's daemon, confirmed as an end from that signal. No file of S is
///   touched, nothing is reconfigured and the members stay the original three.
/// - THE OUTAGE MARK: `RECOVERY_BOUND` from the instant the kill was CONFIRMED (the instant it
///   was requested is printed too), clipped to the test's bound. It never moves: not for a
///   refusal, a replacement, a redirect, a readiness answer or a change of leader. Inside it,
///   with S down and the two survivors the same processes as before the kill: a client through
///   a survivor; logical row `ROWS + 1` acknowledged (a new commit); both survivors Ready; and
///   ONE exact query of every acknowledged row, answered by a survivor. Every call ends no
///   later than the mark and nothing that completes after it counts. The object listing that
///   follows is reported outside the mark.
/// - S is then started from its own files. All three Ready, the survivors still the same
///   processes, then ONE judged query through a client bootstrapped at S: every acknowledged
///   row, exactly. The node that answered is printed; it need not be S.
/// - Not covered: a write known to be in the dead node's range, a node that does not return,
///   two nodes down, a partition, a loss of power (a killed process keeps what the kernel
///   had), replacement, backup and restore, any rate, any validated network envelope.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons and needs a remote object store; run alone under an external owner (see the module text)"]
async fn three_daemons_keep_serving_when_the_serving_node_is_killed_and_it_returns() {
	let deadline = Instant::now() + TIERING_TEST_BOUND;
	let store = StoreEnv::from_environment();
	let objects = store.handle();
	let before = tokio::time::timeout(CLIENT_BOUND, objects.list(""))
		.await
		.expect("the object store did not answer a list within its bound")
		.expect("the object store refused a list: endpoint, bucket or keys");
	assert!(before.is_empty(), "the run's prefix is not new");
	eprintln!(
		"smoke: SERVING-NODE LOSS workflow: object prefix {} in bucket {} at {} (direct, no relay); DECLARED consensus timing base {} on every node; DECLARED stall bound {QUERY_STALL_BOUND_MILLIS} ms per node (shipped default 250); {ROWS} logical rows before the kill and one after, slabs of {SLAB_MAX_BYTES} bytes; outage mark {RECOVERY_BOUND:?} from the confirmed kill; replacements at most {MAX_REPLACEMENTS_PER_ROW} a row, {MAX_REPLACEMENTS} in all",
		store.prefix,
		store.bucket,
		store.endpoint,
		match declared_consensus_base() {
			Some(base) => format!("{base} ms"),
			None => "NOT declared (the product's default)".to_owned(),
		}
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
	let client = client_through(
		&formed.members[0],
		&formed.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	let mut journal = Journal {
		clock: started,
		operations: Vec::new(),
		probes: Vec::new(),
		first_refusal: None,
		refusals: 0,
		replacements: 0,
	};
	at("writes");
	for row in 1..=ROWS {
		ingest_logical_row(
			&client,
			&formed.descriptor,
			&mut journal,
			row,
			deadline,
			"before the kill",
		)
		.await;
	}
	eprintln!(
		"smoke: all {ROWS} logical rows acknowledged before the kill; operations {}, definitive refusals {}, replacements {}",
		journal.operations.len(),
		journal.refusals,
		journal.replacements
	);
	at("offload wait");
	offloaded_by_every_owner(&mut formed, &objects, deadline, "offload wait").await;
	at("the judged query before the kill");
	let rows = journal.acknowledged();
	let serving = judged_query(&client, &formed, &objects, &rows, "before the kill").await;
	assert!(
		NODES.contains(&serving),
		"the query was answered by {serving}, which is no node of this cluster"
	);

	// What each node names as control leader, now. An observation, not a condition.
	let mut named_leader = Vec::new();
	for member in &formed.members {
		let inspection = inspect(member, &formed.pki);
		eprintln!(
			"smoke: before the kill node {} names control leader {:?} (ready {})",
			member.id,
			inspection.leader(),
			inspection.ready
		);
		named_leader.push(inspection.leader());
	}
	let was_leader = named_leader.iter().all(|leader| *leader == Some(serving));
	eprintln!(
		"smoke: the node to be killed, {serving}, {} named control leader by all three just before the kill{}",
		if was_leader { "WAS" } else { "was NOT" },
		if was_leader {
			""
		} else {
			": this run does not exercise the loss of the control leader"
		}
	);
	let survivors: Vec<usize> = (0..formed.members.len())
		.filter(|at| formed.members[*at].id != serving)
		.collect();
	let returned = (0..formed.members.len())
		.find(|at| formed.members[*at].id == serving)
		.expect("the serving node is a member");
	// The survivors as processes, before the kill: they must be these to the end.
	let surviving: Vec<(u32, String)> = survivors
		.iter()
		.map(|at| {
			let id = formed.members[*at].id;
			(id, formed.daemons.identity(id))
		})
		.collect();
	eprintln!("smoke: the survivors before the kill: {surviving:?}");
	let same_survivors = |daemons: &mut Daemons, what: &str| {
		if let Some(exited) = daemons.exited() {
			panic!("{what}: {exited}");
		}
		for (id, identity) in &surviving {
			assert_eq!(
				&daemons.identity(*id),
				identity,
				"{what}: node {id} is not the process it was before the kill"
			);
		}
	};

	at(&format!("kill of node {serving}"));
	let kill_requested = Instant::now();
	let proof = formed.daemons.crash(serving);
	let kill_confirmed = Instant::now();
	let mark = (kill_confirmed + RECOVERY_BOUND).min(deadline);
	eprintln!(
		"smoke: KILL of serving node {serving}: {proof}; requested at +{} ms, confirmed {:?} later; ONE outage mark {RECOVERY_BOUND:?} after the confirmation",
		(kill_requested - started).as_millis(),
		kill_confirmed - kill_requested
	);

	// Everything from here to OUTAGE RESULT ends no later than the mark.
	at("the new logical row with the node down");
	let through = client_through(
		&formed.members[survivors[0]],
		&formed.pki,
		Credential::Storage,
		mark,
	)
	.await;
	assert!(
		Instant::now() <= mark,
		"OUTAGE: the client through a survivor came after the mark"
	);
	let committed = ingest_logical_row(
		&through,
		&formed.descriptor,
		&mut journal,
		ROWS + 1,
		mark,
		"with the node down",
	)
	.await;
	assert!(
		committed <= mark,
		"OUTAGE: the new commit came after the mark"
	);
	formed.daemons.still_down(serving);
	at("the survivors' readiness");
	for at in &survivors {
		wait_ready(
			&formed.members[*at],
			&formed.pki,
			&mut formed.daemons,
			mark,
			"OUTAGE readiness",
		);
	}
	let ready = Instant::now();
	assert!(ready <= mark, "OUTAGE: readiness came after the mark");
	assert_eq!(
		journal.count(Disposition::Unknown),
		0,
		"OUTAGE: an operation is unresolved"
	);
	// Named a judged query so that the record's completeness rule counts it with its outcome.
	at("the judged query with the node down");
	let rows = journal.acknowledged();
	let (answered, read) = exact_query_within(
		&through,
		&formed.descriptor,
		&rows,
		mark,
		"OUTAGE: every acknowledged row, with the node down",
	)
	.await;
	assert!(
		read <= mark,
		"OUTAGE: the exact query completed after the mark"
	);
	assert!(
		answered != serving && NODES.contains(&answered),
		"OUTAGE: the query was answered by node {answered}"
	);
	formed.daemons.still_down(serving);
	same_survivors(&mut formed.daemons, "at the end of the outage checks");
	eprintln!(
		"smoke: OUTAGE RESULT: PASS inside {RECOVERY_BOUND:?} of the confirmed kill of node {serving}: logical row {} acknowledged after {:?}, both survivors Ready after {:?}, exactly the {} acknowledged rows read from node {answered} after {:?}; node {serving} down throughout, the survivors the same processes; definitive refusals so far {}, replacements {}",
		ROWS + 1,
		committed - kill_confirmed,
		ready - kill_confirmed,
		rows.len(),
		read - kill_confirmed,
		journal.refusals,
		journal.replacements
	);
	// Reported only, and outside the mark.
	for at in &survivors {
		let inspection = inspect(&formed.members[*at], &formed.pki);
		eprintln!(
			"smoke: with node {serving} down node {} names control leader {:?} (ready {})",
			formed.members[*at].id,
			inspection.leader(),
			inspection.ready
		);
	}
	match Snapshot::try_take(&formed, &objects, CLIENT_BOUND).await {
		Ok(snapshot) => snapshot.report("with the node down (reported, not judged)"),
		Err(why) => eprintln!(
			"smoke: the listing with the node down is UNAVAILABLE ({why}); the outage result is unaffected"
		),
	}

	// The node returns on its own files.
	at(&format!("return of node {serving}"));
	formed.daemons.start(&formed.members[returned]);
	for member in &formed.members {
		wait_ready(
			member,
			&formed.pki,
			&mut formed.daemons,
			deadline,
			"after the killed node's return",
		);
	}
	same_survivors(&mut formed.daemons, "after the killed node's return");
	let back = client_through(
		&formed.members[returned],
		&formed.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	at("the judged query after the return");
	let answered = judged_query(&back, &formed, &objects, &rows, "after the return").await;
	eprintln!(
		"smoke: after node {serving} returned, a client bootstrapped at it was answered by node {answered}{}",
		if answered == serving {
			""
		} else {
			": a cluster read after the return, not a read served by the returned node"
		}
	);
	eprintln!(
		"smoke: OVERALL: PASS: the serving node {serving} ({}the control leader named before the kill) was killed and returned; {} operations, {} acknowledged, {} refused, {} replacements",
		if was_leader { "" } else { "NOT " },
		journal.operations.len(),
		journal.count(Disposition::Committed),
		journal.count(Disposition::Refused),
		journal.replacements
	);

	for id in NODES {
		formed.daemons.stop(id, deadline);
	}
	assert!(Instant::now() < deadline, "the test overran its bound");
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

// ============================================================
//  THE OBJECT-STORE WORKFLOW THAT GOES ON AFTER A REFUSED WRITE
// ============================================================

/// A replacement operation's row key is its logical row plus this times its ordinal, so no
/// upsert can make one operation's commit look like another's.
const REPLACEMENT_KEY_STEP: i64 = 1000;
/// New operations after definite refusals: at most this many for one logical row.
const MAX_REPLACEMENTS_PER_ROW: u32 = 2;
/// And at most this many in the whole run.
const MAX_REPLACEMENTS: u32 = 8;
/// A recorded refusal is confirmed by asking the same operation again, at most this many times.
const REFUSAL_PROBES: u32 = 3;

/// The row key of logical row `row`'s operation number `ordinal` (0: the original).
fn replacement_key(row: i64, ordinal: u32) -> i64 {
	row + REPLACEMENT_KEY_STEP * i64::from(ordinal)
}

#[test]
fn a_replacement_never_shares_a_key_with_any_other_operation() {
	let mut keys = std::collections::BTreeSet::new();
	for row in 1..=ROWS {
		for ordinal in 0..=MAX_REPLACEMENTS_PER_ROW {
			assert!(keys.insert(replacement_key(row, ordinal)));
		}
	}
	assert_eq!(replacement_key(7, 0), 7);
	assert!(ROWS < REPLACEMENT_KEY_STEP);
}

/// Every operation of the run, in the order it was begun. Its summary is printed when it is
/// dropped, on a pass and on a panic alike; each offer is printed as it happens.
struct Journal {
	clock: Instant,
	operations: Vec<PauseOperation>,
	/// Asks of an already refused operation, kept apart from offers.
	probes: Vec<u32>,
	first_refusal: Option<Instant>,
	refusals: u32,
	replacements: u32,
}

impl Journal {
	fn begin(&mut self, logical_row: i64, ordinal: u32) -> usize {
		self.operations.push(PauseOperation {
			logical_row,
			key: replacement_key(logical_row, ordinal),
			name: (0..NAME_HEX_BLOCKS).map(|_| hex(random32())).collect(),
			identity: operation_id(),
			offers: 0,
			disposition: Disposition::Unknown,
		});
		self.probes.push(0);
		self.operations.len() - 1
	}

	/// The rows a complete read must return: exactly the acknowledged operations', by key.
	fn acknowledged(&self) -> Vec<(i64, String)> {
		let mut rows: Vec<(i64, String)> = self
			.operations
			.iter()
			.filter(|operation| operation.disposition == Disposition::Committed)
			.map(|operation| (operation.key, operation.name.clone()))
			.collect();
		rows.sort();
		rows
	}

	fn count(&self, disposition: Disposition) -> usize {
		self.operations
			.iter()
			.filter(|operation| operation.disposition == disposition)
			.count()
	}
}

impl Drop for Journal {
	fn drop(&mut self) {
		eprintln!(
			"smoke: JOURNAL SUMMARY: operations {}: acknowledged {}, refused {}, UNRESOLVED {}; originals {}, replacements {}; first definitive refusal {}",
			self.operations.len(),
			self.count(Disposition::Committed),
			self.count(Disposition::Refused),
			self.count(Disposition::Unknown),
			self.operations.len() - self.replacements as usize,
			self.replacements,
			match self.first_refusal {
				Some(at) => format!("at +{:.3} s", (at - self.clock).as_secs_f64()),
				None => "NONE".to_owned(),
			}
		);
		for (operation, probes) in self.operations.iter().zip(&self.probes) {
			if operation.disposition != Disposition::Committed || operation.offers > 1 {
				eprintln!(
					"smoke: JOURNAL SUMMARY: logical row {} key {} name {}… identity {:02x?}: {:?} after {} offers and {probes} asks after its refusal",
					operation.logical_row,
					operation.key,
					&operation.name[..16],
					&operation.identity.client_uuid,
					operation.disposition,
					operation.offers
				);
			}
		}
	}
}

/// ONE call for one operation, the same identity and the same row every time, printed before
/// it is made and again when it has ended. `call_deadline` is the client's deadline and the
/// wrapper's. Returns the reading of the answer by status, knowledge and time
/// (`accepted_disposition`): a late answer, another operation's answer, a contradictory
/// knowledge or any status this workflow does not allow is an `Err`, never "unknown".
/// Also returns the answer-observation instant captured before formatting or logging it.
async fn journalled_call(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	operation: &PauseOperation,
	what: &str,
	number: u32,
	call_deadline: Instant,
	clock: Instant,
) -> (Result<Disposition, String>, Instant) {
	let asked = Instant::now();
	eprintln!(
		"smoke: JOURNAL SUBMIT {what} {number}: logical row {} key {} name {}… identity {:02x?}/{} at +{:.3} s, caller deadline {:?} from now",
		operation.logical_row,
		operation.key,
		&operation.name[..16],
		&operation.identity.client_uuid,
		operation.identity.sequence_no,
		(asked - clock).as_secs_f64(),
		call_deadline.saturating_duration_since(asked)
	);
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
		"smoke: JOURNAL ANSWER {what} {number}: key {} after {:?} ({:+.3} ms relative to its caller deadline): {} => {}",
		operation.key,
		answered - asked,
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
	(read, answered)
}

/// Offers operation `at` until it is acknowledged or refused, under ONE absolute bound for the
/// operation (every call's deadline is that bound, as in the owning workflow's ingestion; an
/// offer never resets it). While its outcome is unknown nothing else is issued for its row.
/// Unknown at the bound, or an answer that is not accepted, fails the test. Returns when the
/// definitive answer was observed.
async fn settle_one(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	journal: &mut Journal,
	at: usize,
	what: &str,
	bound: Instant,
) -> Instant {
	loop {
		let operation = &journal.operations[at];
		assert!(
			Instant::now() < bound,
			"{what}: logical row {} key {} is UNRESOLVED at its bound after {} offers",
			operation.logical_row,
			operation.key,
			operation.offers
		);
		let number = operation.offers + 1;
		let (read, observed) = journalled_call(
			client,
			descriptor,
			operation,
			what,
			number,
			bound,
			journal.clock,
		)
		.await;
		let operation = &mut journal.operations[at];
		operation.offers = number;
		operation.disposition = match read {
			Ok(disposition) => disposition,
			Err(why) => panic!(
				"{what}: the offer of logical row {} key {} had an answer this workflow does not accept: {why}",
				operation.logical_row, operation.key
			),
		};
		if operation.disposition != Disposition::Unknown {
			return observed;
		}
		tokio::time::sleep(
			Duration::from_millis(250).min(bound.saturating_duration_since(Instant::now())),
		)
		.await;
	}
}

/// Asks a REFUSED operation again, same identity and row, and requires the same recorded
/// refusal. The operation stays Refused whatever happens here: an unknown answer (a failed
/// connection, a redirect, a stall, a deadline) proves nothing about the record and is asked
/// again, at most `REFUSAL_PROBES` times and never past `limit`; a receipt, or any answer not
/// accepted, contradicts the record and fails. Not confirmed within those asks: a failure of
/// this check, reported as that. It grants no new operation and no new budget.
async fn confirm_refusal(
	client: &Client,
	descriptor: &TableSchemaDescriptor,
	journal: &mut Journal,
	at: usize,
	limit: Instant,
) {
	assert_eq!(journal.operations[at].disposition, Disposition::Refused);
	for probe in 1..=REFUSAL_PROBES {
		let call_deadline = (Instant::now() + RECOVERY_CALL_BOUND).min(limit);
		assert!(
			Instant::now() < call_deadline,
			"the refusal of key {} was not confirmed before the limit ({} asks)",
			journal.operations[at].key,
			probe - 1
		);
		journal.probes[at] = probe;
		let (read, _) = journalled_call(
			client,
			descriptor,
			&journal.operations[at],
			"ask of a refused operation",
			probe,
			call_deadline,
			journal.clock,
		)
		.await;
		match read {
			Ok(Disposition::Refused) => return,
			Ok(Disposition::Unknown) => {
				tokio::time::sleep(
					Duration::from_millis(250).min(limit.saturating_duration_since(Instant::now())),
				)
				.await;
			}
			Ok(Disposition::Committed) => panic!(
				"a REFUSED operation (key {}) was answered with a receipt when asked again",
				journal.operations[at].key
			),
			Err(why) => panic!(
				"a REFUSED operation (key {}) asked again: {why}",
				journal.operations[at].key
			),
		}
	}
	panic!(
		"the refusal of key {} was not confirmed in {REFUSAL_PROBES} asks",
		journal.operations[at].key
	);
}

/// The daemons are the processes recorded before ingestion, alive, and none has exited.
fn same_processes(formed: &mut Formed, processes: &[OwnedProcess], what: &str) {
	for process in processes {
		let state = process
			.state()
			.unwrap_or_else(|why| panic!("{what}: no-restart check: {why}"));
		assert!(
			matches!(state, 'R' | 'S' | 'D' | 'I'),
			"{what}: node {} is in state {state}",
			process.node
		);
	}
	if let Some(exited) = formed.daemons.exited() {
		panic!("{what}: no-restart check: {exited}");
	}
	assert_eq!(
		formed.daemons.owned(),
		processes,
		"{what}: a daemon is not the process it was"
	);
}

/// The owning object-store workflow above, with ONE difference: a write that is truthfully
/// refused does not end it. It is a separate scenario with a separate question (does the
/// cluster serve again, by itself, after a recorded refusal) and never stands for the owning
/// test, whose rule (the first refusal fails) and whose records are unchanged.
///
/// Fixture and settings are the owning test's: three real daemons, the remote object store,
/// default consensus and leases, the declared stall bound, the same rows, slabs and bounds. No
/// fault is injected and rows are offered one after another.
///
/// Operations. Each logical row's ORIGINAL operation keeps one identity and one row and is
/// offered again only while its outcome is unknown, inside its one `CLIENT_BOUND`; answers are
/// read by status, commit knowledge and time (`accepted_disposition`). Only the recorded
/// refusal (`WRITE_REFUSED` with `KNOWN_NOT_COMMITTED`) permits a REPLACEMENT: a new identity
/// under a new row key (`replacement_key`) and a new name, at most `MAX_REPLACEMENTS_PER_ROW`
/// for a row and `MAX_REPLACEMENTS` in the run. A refused operation stays refused in the
/// journal; it is asked again only to confirm that (`confirm_refusal`).
///
/// The checkpoint. ONE budget of `RECOVERY_BOUND` starts when the caller first observes a
/// definitive refusal and is never restarted. It is the caller's clock, not a claim about when
/// any stall ended. Inside it, with the same three processes, no restart, no signal and nothing
/// cleared: the refusal is confirmed; a replacement for that row is acknowledged (a new commit
/// after the refusal); every node reports Ready; and one exact read returns exactly the rows
/// acknowledged so far. Every call in it ends no later than that mark. Later refusals are
/// handled under the caps and each operation's ordinary bound, with no second budget.
///
/// Then the owning test's stages, reported apart from the checkpoint: the offload wait, the
/// judged query, three rolling restarts, the offload observation and the judged query again,
/// each against exactly the acknowledged rows. The object counts are aggregate observations:
/// an object written for a refused operation can be among them, so they do not say which
/// acknowledged row is cold.
///
/// If no refusal occurs the run reports its other results and says the checkpoint was NOT
/// exercised. It claims no rate, no cause of any stall and nothing for the owning test.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons and needs a remote object store; run alone under an external owner (see the module text)"]
async fn three_daemons_offload_to_the_object_store_and_recover_after_a_refused_write() {
	let deadline = Instant::now() + TIERING_TEST_BOUND;
	let store = StoreEnv::from_environment();
	let objects = store.handle();
	let before = tokio::time::timeout(CLIENT_BOUND, objects.list(""))
		.await
		.expect("the object store did not answer a list within its bound")
		.expect("the object store refused a list: endpoint, bucket or keys");
	assert!(before.is_empty(), "the run's prefix is not new");
	eprintln!(
		"smoke: RECOVERY-AWARE workflow (not the owning acceptance): object prefix {} in bucket {} at {} (direct, no relay); DECLARED stall bound {QUERY_STALL_BOUND_MILLIS} ms per node (shipped default 250); {ROWS} logical rows, slabs of {SLAB_MAX_BYTES} bytes; checkpoint {RECOVERY_BOUND:?} from the first definitive refusal; replacements at most {MAX_REPLACEMENTS_PER_ROW} a row, {MAX_REPLACEMENTS} in all",
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
	let client = client_through(
		&formed.members[0],
		&formed.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	// Recorded before any write: the checkpoint and the read before the restarts require
	// these same processes.
	let processes = formed.daemons.owned();
	eprintln!("smoke: the daemons before ingestion: {processes:?}");
	let mut journal = Journal {
		clock: started,
		operations: Vec::new(),
		probes: Vec::new(),
		first_refusal: None,
		refusals: 0,
		replacements: 0,
	};

	at("writes");
	for row in 1..=ROWS {
		let mut ordinal = 0_u32;
		loop {
			let index = journal.begin(row, ordinal);
			let what = if ordinal == 0 {
				"original offer"
			} else {
				"replacement offer"
			};
			let bound = Instant::now() + CLIENT_BOUND.min(step(deadline));
			let observed = settle_one(
				&client,
				&formed.descriptor,
				&mut journal,
				index,
				what,
				bound,
			)
			.await;
			if journal.operations[index].disposition == Disposition::Committed {
				break;
			}
			// The recorded refusal of this operation.
			journal.refusals += 1;
			assert!(
				ordinal < MAX_REPLACEMENTS_PER_ROW && journal.replacements < MAX_REPLACEMENTS,
				"logical row {row}: refused, and no replacement is left (this row {ordinal} of {MAX_REPLACEMENTS_PER_ROW}, the run {} of {MAX_REPLACEMENTS})",
				journal.replacements
			);
			ordinal += 1;
			journal.replacements += 1;
			if journal.first_refusal.is_some() {
				// A later refusal: confirmed under the ordinary bound; no second checkpoint.
				let limit = Instant::now() + CLIENT_BOUND.min(step(deadline));
				confirm_refusal(&client, &formed.descriptor, &mut journal, index, limit).await;
				continue;
			}

			// THE CHECKPOINT: one mark, from the first definitive refusal the caller saw.
			journal.first_refusal = Some(observed);
			let mark = (observed + RECOVERY_BOUND).min(deadline);
			at(&format!(
				"CHECKPOINT after the first definitive refusal (logical row {row}); one budget of {RECOVERY_BOUND:?} from when it was observed"
			));
			same_processes(&mut formed, &processes, "at the first refusal");
			confirm_refusal(&client, &formed.descriptor, &mut journal, index, mark).await;
			let confirmed = Instant::now();
			// A new operation for this row, begun after the refusal: it, or its own
			// replacement under the same caps, must be acknowledged before the mark.
			let committed = loop {
				let next = journal.begin(row, ordinal);
				let bound = (Instant::now() + CLIENT_BOUND).min(mark);
				settle_one(
					&client,
					&formed.descriptor,
					&mut journal,
					next,
					"replacement offer (checkpoint)",
					bound,
				)
				.await;
				if journal.operations[next].disposition == Disposition::Committed {
					break Instant::now();
				}
				journal.refusals += 1;
				assert!(
					ordinal < MAX_REPLACEMENTS_PER_ROW && journal.replacements < MAX_REPLACEMENTS,
					"CHECKPOINT: the replacement for logical row {row} was refused and none is left"
				);
				ordinal += 1;
				journal.replacements += 1;
				confirm_refusal(&client, &formed.descriptor, &mut journal, next, mark).await;
			};
			assert!(
				committed <= mark,
				"CHECKPOINT: the new commit came after the mark"
			);
			for member in &formed.members {
				wait_ready(
					member,
					&formed.pki,
					&mut formed.daemons,
					mark,
					"CHECKPOINT readiness",
				);
			}
			let ready = Instant::now();
			assert!(ready <= mark, "CHECKPOINT: readiness came after the mark");
			assert_eq!(
				journal.count(Disposition::Unknown),
				0,
				"CHECKPOINT: an operation is unresolved"
			);
			let expected = journal.acknowledged();
			assert!(
				Instant::now() < mark,
				"CHECKPOINT: no time left for the exact read"
			);
			expect_rows(
				&client,
				&formed.descriptor,
				mark,
				"CHECKPOINT: the exact read of every acknowledged row",
				&expected,
			)
			.await;
			let read = Instant::now();
			assert!(
				read <= mark,
				"CHECKPOINT: the exact read completed after the mark"
			);
			same_processes(&mut formed, &processes, "at the end of the checkpoint");
			eprintln!(
				"smoke: CHECKPOINT RESULT: PASS inside {RECOVERY_BOUND:?} of the first definitive refusal: refusal confirmed after {:?}, a new operation acknowledged after {:?}, every node Ready after {:?}, exactly the {} acknowledged rows read after {:?}; the same three processes, no restart",
				confirmed - observed,
				committed - observed,
				ready - observed,
				expected.len(),
				read - observed
			);
			break;
		}
	}
	let rows = journal.acknowledged();
	assert_eq!(
		rows.len(),
		ROWS as usize,
		"one acknowledged operation per logical row"
	);
	eprintln!(
		"smoke: all {ROWS} logical rows acknowledged; operations {}, definitive refusals {}, replacements {}{}",
		journal.operations.len(),
		journal.refusals,
		journal.replacements,
		if journal.refusals == 0 {
			"; NO refusal occurred: the checkpoint and replacement branches were NOT exercised"
		} else {
			""
		}
	);

	at("offload wait");
	offloaded_by_every_owner(&mut formed, &objects, deadline, "offload wait").await;
	at("the judged query before any restart");
	judged_query(&client, &formed, &objects, &rows, "before any restart").await;
	// Everything above was served by the processes recorded before ingestion.
	same_processes(&mut formed, &processes, "before the first restart");
	eprintln!("smoke: the same three processes served everything up to here; restarts begin now");

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

// ============================================================
//  ADR 0736: BACK UP COLD ROWS, LOSE THE INSTALLATION, RESTORE INTO A FRESH CLUSTER
// ============================================================

/// The whole workflow's bound: two formations, the offload, the backup and the restore.
const BACKUP_TEST_BOUND: Duration = Duration::from_secs(1200);
/// One `nilket wendelon backup` command's bound (its scrypt costs 128 MiB and seconds).
const BACKUP_COMMAND_BOUND: Duration = Duration::from_secs(300);

/// One command's run under ONE absolute deadline that covers its execution and its complete
/// output: both pipes are drained on threads of their own while it runs, and the result counts
/// only when the exit status AND both streams' ends were observed before the deadline. Past it,
/// the command's process group is killed and the run is a failure, whatever arrived.
struct Ran {
	/// Exit status and both streams whole, all before the deadline.
	within: bool,
	/// The exit code, when the process exited (before or after the deadline).
	code: Option<i32>,
	/// The deadline ended the run.
	timed_out: bool,
	stdout: String,
	stderr: String,
}

impl Ran {
	fn succeeded(&self) -> bool {
		self.within && self.code == Some(0)
	}

	fn text(&self) -> String {
		format!(
			"exit {:?}{}; stdout: {}; stderr: {}",
			self.code,
			if self.timed_out {
				" (CUT at its deadline)"
			} else if !self.within {
				" (output not whole)"
			} else {
				""
			},
			self.stdout.trim(),
			self.stderr.trim()
		)
	}
}

fn bounded_run(program: &Path, arguments: &[std::ffi::OsString], end: Instant) -> Ran {
	let mut child = Command::new(program)
		.args(arguments)
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.process_group(0)
		.spawn()
		.unwrap_or_else(|error| panic!("run {}: {error}", program.display()));
	let group = child.id();
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
	let status = loop {
		match child.try_wait().expect("wait for the command") {
			Some(status) => break Some(status),
			None if Instant::now() >= end => break None,
			None => std::thread::sleep(Duration::from_millis(20)),
		}
	};
	let left = |end: Instant| end.saturating_duration_since(Instant::now());
	let mut out = stdout.recv_timeout(left(end));
	let mut err = stderr.recv_timeout(left(end));
	// Observed in time only if everything arrived and the deadline had not passed by then.
	let whole_in_time = status.is_some()
		&& matches!(out, Ok(Ok(_)))
		&& matches!(err, Ok(Ok(_)))
		&& Instant::now() <= end;
	if !whole_in_time {
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
			Ok(Ok(bytes)) => String::from_utf8_lossy(bytes).into_owned(),
			Ok(Err(error)) => format!("<read failed: {error}>"),
			Err(_) => "<still open after the deadline and the kill>".to_owned(),
		};
	Ran {
		within: whole_in_time,
		code: status.and_then(|status| status.code()),
		timed_out: status.is_none() || Instant::now() > end && !whole_in_time,
		stdout: text(&out),
		stderr: text(&err),
	}
}

/// One `nilket wendelon backup <arguments>` run as an operator would, as the step `step`
/// (unique within the test), under min(`deadline`, its start + `BACKUP_COMMAND_BOUND`). Its
/// begin and its outcome are printed as `smoke: COMMAND BEGIN <step>` and `smoke: COMMAND
/// OUTCOME <step> <ok|failed|timed-out> …`, so a record whose owner died mid-command shows a
/// begun step without an outcome.
fn backup_command(step: &str, arguments: &[std::ffi::OsString], deadline: Instant) -> Ran {
	operator_command("backup", step, arguments, BACKUP_COMMAND_BOUND, deadline)
}

/// One `nilket wendelon <command> <arguments>` run, printed and bounded as `backup_command`
/// describes, under min(`deadline`, its start + `bound`).
fn operator_command(
	command: &str,
	step: &str,
	arguments: &[std::ffi::OsString],
	bound: Duration,
	deadline: Instant,
) -> Ran {
	let end = deadline.min(Instant::now() + bound);
	eprintln!("smoke: COMMAND BEGIN {step}");
	let mut full = vec![std::ffi::OsString::from("wendelon"), command.into()];
	full.extend_from_slice(arguments);
	let ran = bounded_run(&nilket_bin(), &full, end);
	let outcome = if ran.succeeded() {
		"ok"
	} else if ran.timed_out {
		"timed-out"
	} else {
		"failed"
	};
	eprintln!("smoke: COMMAND OUTCOME {step} {outcome} {}", ran.text());
	ran
}

/// The operator's connection options through `member`, presenting `pki`'s operator certificate.
fn operator_connection(member: &Member, pki: &Pki) -> Vec<std::ffi::OsString> {
	operator_connection_as(member, &pki.node_ca, &pki.operator_cert, &pki.operator_key)
}

/// Connection options through `member`, verifying it with `server_ca` and presenting `cert`.
fn operator_connection_as(
	member: &Member,
	server_ca: &Path,
	cert: &Path,
	key: &Path,
) -> Vec<std::ffi::OsString> {
	vec![
		"--bootstrap".into(),
		format!("{}@{}", member.id, member.address).into(),
		"--cert".into(),
		cert.into(),
		"--key".into(),
		key.into(),
		"--ca".into(),
		server_ca.into(),
		"--cluster-domain".into(),
		DOMAIN.into(),
	]
}

/// `--storage 1=<cert>,<key>`: `pki`'s tenant-1 storage certificate.
fn storage_option(pki: &Pki) -> Vec<std::ffi::OsString> {
	let mut value = std::ffi::OsString::from(format!("{TENANT}="));
	value.push(&pki.storage_cert);
	value.push(",");
	value.push(&pki.storage_key);
	vec!["--storage".into(), value]
}

/// ADR 0736's owning workflow. A three-node cluster with a remote object store takes `ROWS`
/// rows from the application's tenant-1 storage client, and every node offloads its cold slabs
/// to the store, so the backup reads rows that live in the store. The operator exports with
/// `nilket wendelon backup export` (the operator's certificate for the hold and the roster,
/// the tenant's issued storage certificate for its rows), verifies the artifact, and finds no
/// hold left. The installation is then lost: every node is stopped and left as it is (its
/// directories and its object prefix are kept, never deleted). A second cluster is formed on
/// the same hosts and port with NEW certificate authorities (an isolated trust domain: the
/// first cluster's operator is refused there), new node secrets and a new root key, and NO
/// object store: daemon configuration supports an S3-compatible store or none, the source's
/// store and keys are never given to it, and its own data stays on its nodes' disks. That is
/// the narrower claim this run makes about the target: a restore into a fresh cluster with
/// local storage, not into a tiered one. `nilket wendelon backup restore` verifies the
/// artifact before it contacts the target, refuses a target that is not fresh, establishes
/// tenant 1 and the table through the public routes, imports the rows through the target's
/// own issued tenant-1 storage certificate, and verifies every table's count and digest under
/// a hold. Then, through the target's storage client: every row exactly, ONE new write under a
/// fresh operation identity and its exact read-back, and no hold left.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns two three-daemon clusters in turn and needs a remote object store; run alone under an external owner (see the module text)"]
async fn three_daemons_back_up_cold_rows_and_restore_into_a_fresh_cluster_that_takes_new_writes() {
	let deadline = Instant::now() + BACKUP_TEST_BOUND;
	let store = StoreEnv::from_environment();
	let objects = store.handle();
	let before = tokio::time::timeout(CLIENT_BOUND, objects.list(""))
		.await
		.expect("the object store did not answer a list within its bound")
		.expect("the object store refused a list: endpoint, bucket or keys");
	assert!(before.is_empty(), "the run's prefix is not new");
	let started = Instant::now();
	let at = |what: &str| {
		eprintln!(
			"smoke: PHASE {what} at +{} ms",
			started.elapsed().as_millis()
		)
	};

	at("source formation");
	let mut source = form(deadline, Some(&store)).await;
	let mut rows: Vec<(i64, String)> = (1..=ROWS)
		.map(|id| {
			let name: String = (0..NAME_HEX_BLOCKS).map(|_| hex(random32())).collect();
			(id, name)
		})
		.collect();
	let client = client_through(
		&source.members[0],
		&source.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	at("source writes");
	for (id, name) in &rows {
		ingest_the_row(&client, &source.descriptor, *id, name, deadline).await;
	}
	at("offload wait");
	offloaded_by_every_owner(&mut source, &objects, deadline, "before the backup").await;

	at("export");
	let artifact = source.root.join("backup");
	let passphrase = source.root.join("backup-passphrase");
	write_private(&passphrase, hex(random32()).as_bytes());
	let mut export: Vec<std::ffi::OsString> = vec![
		"export".into(),
		"--to".into(),
		artifact.clone().into(),
		"--passphrase-file".into(),
		passphrase.clone().into(),
	];
	export.extend(operator_connection(&source.members[0], &source.pki));
	export.extend(storage_option(&source.pki));
	let ran = backup_command("export", &export, deadline);
	assert!(ran.succeeded(), "the export: {}", ran.text());
	let ran = backup_command(
		"verify",
		&[
			"verify".into(),
			artifact.clone().into(),
			"--passphrase-file".into(),
			passphrase.clone().into(),
		],
		deadline,
	);
	assert!(ran.succeeded(), "the verification: {}", ran.text());
	let mut status: Vec<std::ffi::OsString> = vec!["hold-status".into()];
	status.extend(operator_connection(&source.members[0], &source.pki));
	let ran = backup_command("source-hold-after-export", &status, deadline);
	assert!(
		ran.succeeded() && ran.stdout.contains("no backup hold is held"),
		"the source's hold after the export: {}",
		ran.text()
	);

	at("the installation is lost");
	drop(client);
	for id in NODES {
		source.daemons.stop(id, deadline);
	}
	let Formed {
		root: source_root,
		pki: source_pki,
		members: source_members,
		daemons: source_daemons,
		descriptor,
		..
	} = source;
	// Ends the source's owners through node-control; nothing of it is deleted.
	drop(source_daemons);

	at("target formation");
	*NODE_CONTROL_CLUSTER
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner) = "target";
	// The target states the run's declared profile, as the source did with its store.
	let mut target = form_cluster(deadline, None, "-target", false, true).await;
	assert_ne!(
		std::fs::read(&target.pki.node_ca).unwrap(),
		std::fs::read(&source_pki.node_ca).unwrap(),
		"the target has its own node CA"
	);
	at("restore");
	let mut restore: Vec<std::ffi::OsString> = vec![
		"restore".into(),
		artifact.clone().into(),
		"--passphrase-file".into(),
		passphrase.clone().into(),
	];
	restore.extend(operator_connection(&target.members[0], &target.pki));
	restore.extend(storage_option(&target.pki));
	let ran = backup_command("restore", &restore, deadline);
	assert!(ran.succeeded(), "the restore: {}", ran.text());

	at("the restored rows and a new write");
	let client = client_through(
		&target.members[0],
		&target.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	expect_rows(&client, &descriptor, deadline, "after the restore", &rows).await;
	let fresh = (ROWS + 1, hex(random32()));
	ingest_the_row(&client, &descriptor, fresh.0, &fresh.1, deadline).await;
	rows.push(fresh);
	expect_rows(&client, &descriptor, deadline, "after a new write", &rows).await;
	let mut status: Vec<std::ffi::OsString> = vec!["hold-status".into()];
	status.extend(operator_connection(&target.members[0], &target.pki));
	let ran = backup_command("target-hold-after-restore", &status, deadline);
	assert!(
		ran.succeeded() && ran.stdout.contains("no backup hold is held"),
		"the target's hold after the restore: {}",
		ran.text()
	);

	// An isolated trust domain: controlled NEGATIVE SMOKE coverage, checked after the restore
	// so it cannot hide the restore's evidence. The SOURCE operator's certificate and key are
	// the deliberately changed input: presented at the target's endpoint, verifying the target
	// server with the TARGET CA, between two calls of the target's own operator at the same
	// endpoint and CA, both required to succeed. The foreign attempt must fail within its
	// command budget, with complete output and runtime exit 1 (not a usage error, not cut).
	// This does not show which check refused it: the TLS or authorization cause is unobserved,
	// and a transient transport failure affecting only that connection is not excluded. Its
	// full outcome text is recorded and asserted on nothing.
	let mut own: Vec<std::ffi::OsString> = vec!["hold-status".into()];
	own.extend(operator_connection(&target.members[0], &target.pki));
	let ran = backup_command("target-operator-admitted-before", &own, deadline);
	assert!(
		ran.succeeded() && ran.stdout.contains("no backup hold is held"),
		"the target's own operator before the foreign attempt: {}",
		ran.text()
	);
	let mut foreign: Vec<std::ffi::OsString> = vec!["hold-status".into()];
	foreign.extend(operator_connection_as(
		&target.members[0],
		&target.pki.node_ca,
		&source_pki.operator_cert,
		&source_pki.operator_key,
	));
	let refused = backup_command("source-operator-refused-at-target", &foreign, deadline);
	let ran = backup_command("target-operator-admitted-after", &own, deadline);
	assert!(
		ran.succeeded() && ran.stdout.contains("no backup hold is held"),
		"the target's own operator after the foreign attempt: {}",
		ran.text()
	);
	assert!(
		!refused.succeeded(),
		"the source's operator was admitted by the target: {}",
		refused.text()
	);
	assert!(
		refused.within && !refused.timed_out && refused.code == Some(1),
		"the source operator's attempt did not end as a refused command: {}",
		refused.text()
	);

	drop(client);
	for id in NODES {
		target.daemons.stop(id, deadline);
	}
	assert!(Instant::now() < deadline, "the test overran its bound");
	if let (Ok(record), None) = (std::env::var("WENDELON_SMOKE_RECORD_DIR"), node_control()) {
		for (label, members) in [("source", &source_members), ("target", &target.members)] {
			for member in members {
				std::fs::copy(
					&member.log,
					Path::new(&record).join(format!("{label}-node-{}.daemon.log", member.id)),
				)
				.unwrap();
			}
		}
	}
	std::fs::remove_dir_all(&source_root).unwrap();
	std::fs::remove_dir_all(&target.root).unwrap();
}

/// Wendelon ADR 0118 D2, the upgrade route: a cluster formed and written by the
/// PREVIOUS build's binary (`WENDELON_SMOKE_NILKET_OLD_BIN`, a pre-0118 ParentDecisions
/// store, which the new binary refuses to open) is exported and verified by that
/// binary; a fresh cluster formed by the NEW binary restores the artifact, verified
/// by the new binary first, and serves every row exactly and a new write. The source
/// is stopped and kept, never converted. Local storage only: no object store is given
/// to either cluster.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns two three-daemon clusters in turn and needs the previous build's binary; run alone under an external owner (see the module text)"]
async fn an_old_binarys_export_restores_verified_on_the_new_binary() {
	let deadline = Instant::now() + BACKUP_TEST_BOUND;
	let old = PathBuf::from(required("WENDELON_SMOKE_NILKET_OLD_BIN"));
	*BINARY_OVERRIDE
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner) = Some(old);
	let mut source = form_cluster(deadline, None, "-old", true, false).await;
	let mut rows: Vec<(i64, String)> = (1..=ROWS).map(|id| (id, hex(random32()))).collect();
	let client = client_through(
		&source.members[0],
		&source.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	for (id, name) in &rows {
		ingest_the_row(&client, &source.descriptor, *id, name, deadline).await;
	}
	drop(client);
	let artifact = source.root.join("backup");
	let passphrase = source.root.join("backup-passphrase");
	write_private(&passphrase, hex(random32()).as_bytes());
	let mut export: Vec<std::ffi::OsString> = vec![
		"export".into(),
		"--to".into(),
		artifact.clone().into(),
		"--passphrase-file".into(),
		passphrase.clone().into(),
	];
	export.extend(operator_connection(&source.members[0], &source.pki));
	export.extend(storage_option(&source.pki));
	let ran = backup_command("old-binary-export", &export, deadline);
	assert!(ran.succeeded(), "the old binary's export: {}", ran.text());
	for id in NODES {
		source.daemons.stop(id, deadline);
	}
	let descriptor = source.descriptor.clone();

	*BINARY_OVERRIDE
		.lock()
		.unwrap_or_else(std::sync::PoisonError::into_inner) = None;
	// D2: the new binary's ordinary startup on a disposable copy of a genuine old
	// node directory (its configuration names its data directory relatively, so
	// the copy is a separate store) refuses, and leaves every durable byte as it was.
	let original = source.members[0]
		.config
		.parent()
		.expect("a node directory")
		.to_path_buf();
	let copy = source.root.join("old-store-copy");
	copy_tree(&original, &copy);
	let config = copy.join(source.members[0].config.file_name().unwrap());
	let before = durable_bytes(&copy.join("data"));
	assert!(!before.is_empty(), "premise: the old store holds files");
	let started = bounded_run(
		&nilket_bin(),
		&["wendelon".into(), config.into()],
		Instant::now() + Duration::from_secs(120),
	);
	eprintln!(
		"ADR 0118 D2: the new binary on an old store: {}",
		started.text()
	);
	assert!(
		started.within && !started.timed_out && !started.succeeded(),
		"the new binary refuses the old store: {}",
		started.text()
	);
	assert_eq!(
		durable_bytes(&copy.join("data")),
		before,
		"no durable byte of the old store changed"
	);

	let ran = backup_command(
		"new-binary-verify",
		&[
			"verify".into(),
			artifact.clone().into(),
			"--passphrase-file".into(),
			passphrase.clone().into(),
		],
		deadline,
	);
	assert!(
		ran.succeeded(),
		"the new binary verifies it: {}",
		ran.text()
	);
	let mut target = form_cluster(deadline, None, "-new-target", false, true).await;
	let mut restore: Vec<std::ffi::OsString> = vec![
		"restore".into(),
		artifact.clone().into(),
		"--passphrase-file".into(),
		passphrase.clone().into(),
	];
	restore.extend(operator_connection(&target.members[0], &target.pki));
	restore.extend(storage_option(&target.pki));
	let ran = backup_command("new-binary-restore", &restore, deadline);
	assert!(ran.succeeded(), "the new binary's restore: {}", ran.text());
	let client = client_through(
		&target.members[0],
		&target.pki,
		Credential::Storage,
		deadline,
	)
	.await;
	expect_rows(&client, &descriptor, deadline, "after the restore", &rows).await;
	let fresh = (ROWS + 1, hex(random32()));
	ingest_the_row(&client, &descriptor, fresh.0, &fresh.1, deadline).await;
	rows.push(fresh);
	expect_rows(&client, &descriptor, deadline, "after a new write", &rows).await;
	drop(client);
	for id in NODES {
		target.daemons.stop(id, deadline);
	}
	assert!(Instant::now() < deadline, "the test overran its bound");
	std::fs::remove_dir_all(&source.root).unwrap();
	std::fs::remove_dir_all(&target.root).unwrap();
}

/// The bounded command runner counts a run only when its exit and both whole streams arrive
/// before the deadline: a quick success counts; a late finisher, a command whose descendant
/// holds its output open, and a failing exit do not.
#[test]
fn a_backup_command_counts_only_when_its_exit_and_whole_output_arrive_in_time() {
	let sh = Path::new("/bin/sh");
	let run = |script: &str, within: Duration| {
		bounded_run(sh, &["-c".into(), script.into()], Instant::now() + within)
	};
	let quick = run("echo ok; echo note >&2", Duration::from_secs(5));
	assert!(quick.succeeded(), "{}", quick.text());
	assert_eq!(
		(quick.stdout.as_str(), quick.stderr.as_str()),
		("ok\n", "note\n")
	);

	let started = Instant::now();
	let late = run("sleep 3; echo late", Duration::from_millis(500));
	assert!(!late.succeeded() && late.timed_out, "{}", late.text());
	assert!(
		started.elapsed() < Duration::from_secs(3),
		"the late command was ended at its deadline"
	);

	// The shell exits at once; a descendant keeps stdout open past the deadline.
	let held = run(
		"(sleep 3; echo held) & echo started",
		Duration::from_millis(500),
	);
	assert!(!held.succeeded() && !held.within, "{}", held.text());

	let failed = run("echo no >&2; exit 1", Duration::from_secs(5));
	assert!(!failed.succeeded() && failed.within && failed.code == Some(1));
	assert!(!failed.timed_out);
}

// ============================================================
//  ADR 0104 (wendelon): A LOST FOLLOWER IS REPLACED BY A CONTROL-ONLY INSTANCE
// ============================================================

/// The whole workflow's bound, within the launcher's unit cap (480 s).
const REPLACEMENT_TEST_BOUND: Duration = Duration::from_secs(420);
/// One `nilket wendelon replace` command's bound; its own exchange is bounded at 30 s.
const REPLACE_COMMAND_BOUND: Duration = Duration::from_secs(45);
/// From the replacement's start until the operator's status read reports it done.
const DONE_BOUND: Duration = Duration::from_secs(120);
/// How long a full start on a blank store may take to exit refused.
const REFUSED_START_BOUND: Duration = Duration::from_secs(60);
/// One logical write's bound, its takeover wait included (ADR 0729: lease 3 s plus grace).
const OFFERED_WRITE_BOUND: Duration = Duration::from_secs(30);
/// How long the old instance runs before the facts taken while it runs.
const OLD_INSTANCE_RUN: Duration = Duration::from_secs(5);
/// Wendelon ADR 0105: from done until the status shows the replacement's store promoted and
/// its key bound. The promotion waits for the lost slot's lineages to move by takeover (lease
/// expiry, then a staggered takeover), so this bound is larger than one lease.
const PREPARED_BOUND: Duration = Duration::from_secs(120);
/// Wendelon ADR 0106: from the preparation's end until the status shows its recovery
/// recorded (the base and owed children copied from the survivors and validated).
const RECOVERED_BOUND: Duration = Duration::from_secs(120);
/// Wendelon ADR 0106: one stop and one stopped-store read of the replacement.
const INSPECTION_BOUND: Duration = Duration::from_secs(60);
/// Wendelon ADR 0106: the declared wait, with the restarted replacement running, before its
/// delta is read.
const DELTA_WAIT: Duration = Duration::from_secs(30);
/// Wendelon ADR 0108: from the recorded recovery until the status reports the activation (the
/// replacement's own view of its cut, then the proposal and its apply).
const ACTIVATED_BOUND: Duration = Duration::from_secs(120);
/// Wendelon ADR 0108: from a (re)start of the Full replacement until it answers ready as itself
/// (its first full start validates its whole store).
const SERVING_BOUND: Duration = Duration::from_secs(120);

/// Wendelon ADR 0108: how long after its activation is seen the ControlOnly process may take
/// to complete its handoff and exit with the restart-request status.
const HANDOFF_EXIT_BOUND: Duration = Duration::from_secs(60);

/// Three hosts: the stopped-store probe's own unit bound on the node's host (the controller's
/// `store-probe`, RuntimeMaxSec 60).
const PROBE_UNIT_BOUND: Duration = Duration::from_secs(60);

/// Wendelon ADR 0108: the status a ControlOnly process exits with once its handoff completed
/// (`wendelon::cli::ACTIVATION_RESTART_EXIT`, `EX_TEMPFAIL`), asking its supervisor to start
/// it again.
const ACTIVATION_RESTART_EXIT: i32 = wendelon::cli::ACTIVATION_RESTART_EXIT as i32;

/// Wendelon ADR 0108: waits, within `SERVING_BOUND` (and the workflow's `deadline`), until
/// `member` answers ready (exit 0) as the first allocated instance serving `store`, from the
/// owned process `owned` (its identity read before and after). Its readiness answer names both;
/// a leader hint is never taken as proof of which store serves. An answer observed after the
/// bound is refused, not accepted late.
async fn serving_as(
	member: &Member,
	pki: &Pki,
	store: u128,
	deadline: Instant,
	(daemons, owned): (&Daemons, &str),
	what: &str,
) {
	serving_as_with(
		member,
		pki,
		store,
		deadline,
		(&|| daemons.identity(member.id), owned),
		what,
	)
	.await;
}

/// `serving_as` with the running process's identity read by `current` (a child, a host's unit
/// or a service), compared with `owned` after the ready answer.
async fn serving_as_with(
	member: &Member,
	pki: &Pki,
	store: u128,
	deadline: Instant,
	(current, owned): (&dyn Fn() -> String, &str),
	what: &str,
) {
	let started = Instant::now();
	let by = deadline.min(started + SERVING_BOUND);
	let wanted = format!("{store:032x}");
	let mut reads = 0_u32;
	loop {
		reads += 1;
		let inspection = inspect(member, pki);
		let named = inspection.body.as_ref().map(|body| {
			(
				body.get("raft_instance").and_then(|value| value.as_u64()),
				body.get("store_incarnation")
					.and_then(|value| value.as_str())
					.map(str::to_owned),
			)
		});
		let answered_at = Instant::now();
		let serving = inspection.ready
			&& named == Some((Some(u64::from(FIRST_ALLOCATED)), Some(wanted.clone())));
		match accepted_in_time(serving, answered_at, by) {
			Ok(true) => {
				assert_eq!(
					current(),
					owned,
					"{what}: the ready answer must come from the owned process"
				);
				eprintln!(
					"smoke: {what}: node {} ({owned}) ready as instance {FIRST_ALLOCATED} serving \
					 store {wanted}, {:?} after asked ({reads} readiness reads)",
					member.id,
					started.elapsed()
				);
				return;
			}
			Ok(false) => {}
			Err(late) => panic!(
				"{what}: node {} {late} (bound {SERVING_BOUND:?}): {}",
				member.id, inspection.raw
			),
		}
		tokio::time::sleep(Duration::from_secs(1)).await;
	}
}

/// Wendelon ADR 0108: whether an observation made at `answered_at` is accepted: a match within
/// `by` is; a match or a miss after `by` ends the wait, never accepted late.
fn accepted_in_time(matched: bool, answered_at: Instant, by: Instant) -> Result<bool, String> {
	if answered_at > by {
		return Err(if matched {
			"answered as wanted only after its bound".to_owned()
		} else {
			"never answered as wanted within its bound".to_owned()
		});
	}
	Ok(matched)
}

/// Wendelon ADR 0108: whether an exit status observed at `observed_at` is the ControlOnly
/// process's restart request within its bound `by`, under `accepted_in_time`'s policy: a
/// status first observed after the bound is refused, and only exactly
/// `ACTIVATION_RESTART_EXIT` is the request (another code or a signal is not).
fn restart_request(
	status: std::process::ExitStatus,
	observed_at: Instant,
	by: Instant,
) -> Result<(), String> {
	match accepted_in_time(
		status.code() == Some(ACTIVATION_RESTART_EXIT),
		observed_at,
		by,
	) {
		Ok(true) => Ok(()),
		Ok(false) => Err(format!("exited {status}, not with the restart request")),
		Err(late) => Err(format!("exited {status}: {late}")),
	}
}

/// Wendelon ADR 0108: the replacement's ControlOnly process (the one this test started), from
/// that start through its handoff. It may end on its own at any time after it, but only with
/// the restart request: it is reaped once, and its status is kept with when it was observed.
/// Its end never replaces a fact the workflow waits for, and the one supervised start is
/// allowed only through `handed_off`, which consumes it.
struct ReplacementExit {
	id: u32,
	identity: String,
	seen: Option<(std::process::ExitStatus, Instant)>,
}

impl ReplacementExit {
	fn new(id: u32, identity: String) -> Self {
		Self {
			id,
			identity,
			seen: None,
		}
	}

	/// Records the process's end as `ended` reports it, observed at `now()` read AFTER `ended`
	/// returned it (Codex 01a1e47a: never the poll's start). `ended` is asked only while no end
	/// is kept. Any status but the restart request is refused at once.
	fn record(
		&mut self,
		ended: impl FnOnce() -> Option<std::process::ExitStatus>,
		now: impl FnOnce() -> Instant,
	) -> Result<(), String> {
		if self.seen.is_some() {
			return Ok(());
		}
		if let Some(status) = ended() {
			let at = now();
			restart_request(status, at, at)
				.map_err(|why| format!("the ControlOnly process ({}) {why}", self.identity))?;
			self.seen = Some((status, at));
		}
		Ok(())
	}

	/// Every other daemon strictly, and this one's end; panics naming `before`.
	fn poll(&mut self, daemons: &mut Daemons, before: &str) {
		if let Some(why) = daemons.exited_except(self.id) {
			panic!("{before}: {why}");
		}
		let (id, owned) = (self.id, self.identity.clone());
		if let Err(why) = self.record(|| daemons.ended(id, &owned), Instant::now) {
			panic!("{before}: {why}");
		}
	}

	/// The kept end, if it was the restart request observed within `by`; the process identity is
	/// returned for the comparison with the supervised start's. Consumed: one start follows.
	fn handed_off(self, by: Instant) -> Result<(std::process::ExitStatus, String), String> {
		let Some((status, at)) = self.seen else {
			return Err(format!(
				"the ControlOnly process ({}) has not exited",
				self.identity
			));
		};
		restart_request(status, at, by)
			.map_err(|why| format!("the ControlOnly process ({}) {why}", self.identity))?;
		Ok((status, self.identity))
	}
}

/// Three-host replacement (Codex 01a1e48d): one hosted execution as `exit-status` reports it.
#[derive(Clone, Debug, PartialEq, Eq)]
enum HostedExecution {
	Running {
		identity: String,
	},
	/// `code` is the unit's ExecMainCode: 1 an exit (status its code), 2 a signal (status its
	/// number).
	Ended {
		identity: String,
		code: u8,
		status: i32,
		result: String,
	},
}

impl HostedExecution {
	fn identity(&self) -> &str {
		match self {
			Self::Running { identity } | Self::Ended { identity, .. } => identity,
		}
	}

	/// An ended execution as a process status, only from a coherent unit record (Codex 01a1e49d
	/// R2): an exit (code 1) with status 0 to 255 and result `exit-code` (or `success` for 0),
	/// or a signal (code 2) with a signal number 1 to 64 and result `signal` or `core-dump`.
	/// Anything else (a timeout, an unknown result, a status out of range) is an error, never
	/// normalized into an exit.
	fn status(&self) -> Result<Option<std::process::ExitStatus>, String> {
		use std::os::unix::process::ExitStatusExt;
		match self {
			Self::Running { .. } => Ok(None),
			Self::Ended {
				code: 1,
				status,
				result,
				..
			} if (0..=255).contains(status)
				&& (result == "exit-code" || (*status == 0 && result == "success")) =>
			{
				Ok(Some(std::process::ExitStatus::from_raw(status << 8)))
			}
			Self::Ended {
				code: 2,
				status,
				result,
				..
			} if (1..=64).contains(status) && (result == "signal" || result == "core-dump") => {
				Ok(Some(std::process::ExitStatus::from_raw(*status)))
			}
			Self::Ended {
				code,
				status,
				result,
				..
			} => Err(format!(
				"an incoherent end: code {code} status {status} result {result}"
			)),
		}
	}
}

/// Standard base64 with padding (the controller decodes it with `base64 -d`).
fn base64_of(bytes: &[u8]) -> String {
	const ALPHABET: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
	let mut text = String::with_capacity(bytes.len().div_ceil(3) * 4);
	for chunk in bytes.chunks(3) {
		let word = (u32::from(chunk[0]) << 16)
			| (u32::from(*chunk.get(1).unwrap_or(&0)) << 8)
			| u32::from(*chunk.get(2).unwrap_or(&0));
		for at in 0..4 {
			if at <= chunk.len() {
				text.push(ALPHABET[((word >> (18 - 6 * at)) & 63) as usize] as char);
			} else {
				text.push('=');
			}
		}
	}
	text
}

/// `pid <n> started <t>`, both nonzero whole numbers, and nothing else.
fn parse_identity(text: &str) -> Result<String, String> {
	let fields: Vec<&str> = text.split(' ').collect();
	match fields.as_slice() {
		["pid", pid, "started", at]
			if [pid, at]
				.iter()
				.all(|v| !v.is_empty() && v.bytes().all(|b| b.is_ascii_digit()) && **v != "0") =>
		{
			Ok(text.to_owned())
		}
		_ => Err(format!("not an execution identity: {text:?}")),
	}
}

/// The ONE line `exit-status` prints, whole or refused: `running <identity>` or `ended
/// <identity> code <1|2> status <n> result <word>`. Anything else (another line, a missing or
/// extra field, a code other than 1 or 2) is refused: a missing status is never an end.
fn parse_exit_status(stdout: &str) -> Result<HostedExecution, String> {
	let lines: Vec<&str> = stdout.lines().collect();
	let [line] = lines.as_slice() else {
		return Err(format!("not one line: {stdout:?}"));
	};
	let fields: Vec<&str> = line.split(' ').collect();
	match fields.as_slice() {
		["running", rest @ ..] if rest.len() == 4 => Ok(HostedExecution::Running {
			identity: parse_identity(&rest.join(" "))?,
		}),
		[
			"ended",
			p,
			pid,
			s,
			at,
			"code",
			code,
			"status",
			status,
			"result",
			result,
		] if !result.is_empty() => {
			let identity = parse_identity(&[*p, pid, s, at].join(" "))?;
			let code = match *code {
				"1" => 1,
				"2" => 2,
				other => return Err(format!("an unknown end code {other}: {line:?}")),
			};
			// Canonical decimal only: digits, no sign, no leading zero, at most three digits.
			let canonical = !status.is_empty()
				&& status.len() <= 3
				&& status.bytes().all(|b| b.is_ascii_digit())
				&& (*status == "0" || !status.starts_with('0'));
			let status = canonical
				.then(|| status.parse::<i32>().ok())
				.flatten()
				.ok_or_else(|| format!("not a status: {line:?}"))?;
			Ok(HostedExecution::Ended {
				identity,
				code,
				status,
				result: (*result).to_owned(),
			})
		}
		_ => Err(format!("not an execution: {line:?}")),
	}
}

/// The end of the execution `owned` from one `exit-status` answer: `None` while it runs, its
/// status once it ended. Another execution (running or ended) or an answer that is not whole
/// is an error: it is never taken for the owned one's end.
fn owned_end(stdout: &str, owned: &str) -> Result<Option<std::process::ExitStatus>, String> {
	let execution = parse_exit_status(stdout)?;
	if execution.identity() != owned {
		return Err(format!(
			"the unit's execution ({}) is not the one this test started ({owned})",
			execution.identity()
		));
	}
	execution.status()
}

/// Three hosts (Codex 01a1e49d R3): the whole seconds a one-off unit may run before the step's
/// deadline `by`, at most `most`. Less than one whole second left is an error: an expired
/// budget is never rounded up to start work.
fn phase_seconds(by: Instant, now: Instant, most: u64) -> Result<u64, String> {
	let left = by.saturating_duration_since(now).as_secs().min(most);
	(left >= 1)
		.then_some(left)
		.ok_or_else(|| "less than one second is left of its step".to_owned())
}

/// An answer observed at `observed` counts only when that is not after the step's deadline.
fn answered_within(observed: Instant, by: Instant) -> Result<(), String> {
	(observed <= by)
		.then_some(())
		.ok_or_else(|| "answered only after its step's deadline".to_owned())
}

/// How a one-off unit (`run-once`, `provision`, `store-probe`) ended, read from the unit.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum OneOff {
	Exit(i32),
	Signal(i32),
	Timeout,
}

/// A one-off's stdout: everything before its ONE `RESULT` line, which must be the last line,
/// and that result. Refused when there is no such line, more than one, or one not last.
fn parse_one_off(stdout: &str) -> Result<(String, OneOff), String> {
	let lines: Vec<&str> = stdout.lines().collect();
	let results = lines
		.iter()
		.filter(|line| line.starts_with("RESULT "))
		.count();
	let Some((last, before)) = lines.split_last() else {
		return Err("no output".to_owned());
	};
	if results != 1 || !last.starts_with("RESULT ") {
		return Err(format!("not exactly one RESULT line, last: {stdout:?}"));
	}
	let number = |text: &str| {
		text.parse::<i32>()
			.ok()
			.filter(|n| *n >= 0)
			.ok_or_else(|| format!("not a number in {last:?}"))
	};
	let result = match last.split(' ').collect::<Vec<_>>().as_slice() {
		["RESULT", "exit", n] => OneOff::Exit(number(n)?),
		["RESULT", "signal", n] => OneOff::Signal(number(n)?),
		["RESULT", "timeout"] => OneOff::Timeout,
		_ => return Err(format!("not a result: {last:?}")),
	};
	Ok((before.join("\n"), result))
}

/// Wendelon ADR 0106/0108, three hosts: what the stopped-store probe read ON the node's host.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct Probed {
	/// `role`: the vote page's (instance, control-only), or none.
	role: Option<(u32, bool)>,
	/// `recovered`: what the store holds of its recovery.
	recovered: Option<RecoveredSummary>,
}

/// `wendelon::node::RecoveredRowsForTest` as the oracle reads it: the saved statement (token
/// and store as 32 hex digits), the Complete children, each verification failure and the rows.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
struct RecoveredSummary {
	saved: Option<(String, u32, String, u64, u64)>,
	complete_children: usize,
	unverified: Vec<String>,
	rows: Vec<(i64, String)>,
	active_gid: u64,
}

impl RecoveredSummary {
	fn of(found: &wendelon::node::RecoveredRowsForTest) -> Self {
		Self {
			saved: found.saved.map(|(token, instance, store, gid, k)| {
				(
					format!("{token:032x}"),
					instance,
					format!("{store:032x}"),
					gid,
					k,
				)
			}),
			complete_children: found.complete_children,
			unverified: found.unverified.clone(),
			rows: held_rows(&found.rows),
			active_gid: found.active_gid,
		}
	}
}

/// The probe's stdout, whole or refused: exactly one `PROBE <json>` line whose JSON is a
/// `Probed` with no unknown or missing field, and a `RESULT exit 0` last.
/// The probe's answer from one call's stdout. Any refusal carries the WHOLE call text `raw`
/// (the call's status, stdout and stderr): the probe's own panic goes to stderr under
/// `--nocapture`, and a failure reported from stdout alone lost its cause once (RESULT-03).
fn probe_answer(stdout: &[u8], raw: &str) -> Result<Probed, String> {
	parse_probe(&String::from_utf8_lossy(stdout))
		.map_err(|why| format!("{why}; the whole call: {raw}"))
}

fn parse_probe(stdout: &str) -> Result<Probed, String> {
	let (before, result) = parse_one_off(stdout)?;
	if result != OneOff::Exit(0) {
		return Err(format!("the probe ended {result:?}: {stdout:?}"));
	}
	let probes: Vec<&str> = before
		.lines()
		.filter_map(|line| line.strip_prefix("PROBE "))
		.collect();
	let [json] = probes.as_slice() else {
		return Err(format!("not exactly one PROBE line: {stdout:?}"));
	};
	// Every field present, null or not: serde would read a missing `Option` as `None`, and a
	// missing answer is never an absent one.
	let value: serde_json::Value =
		serde_json::from_str(json).map_err(|error| format!("the PROBE line: {error}: {json:?}"))?;
	let exactly = |value: &serde_json::Value, fields: &[&str], what: &str| {
		let keys: Vec<&str> = value
			.as_object()
			.map(|object| object.keys().map(String::as_str).collect())
			.unwrap_or_default();
		let mut wanted = fields.to_vec();
		wanted.sort_unstable();
		(keys == wanted)
			.then_some(())
			.ok_or_else(|| format!("{what} has fields {keys:?}, not {wanted:?}: {json:?}"))
	};
	exactly(&value, &["recovered", "role"], "the probe")?;
	if !value["recovered"].is_null() {
		exactly(
			&value["recovered"],
			&[
				"active_gid",
				"complete_children",
				"rows",
				"saved",
				"unverified",
			],
			"the recovered probe",
		)?;
	}
	serde_json::from_value(value).map_err(|error| format!("the PROBE line: {error}: {json:?}"))
}

/// Wendelon ADR 0108, three hosts (Codex 01a1e48d point 6): the old instance's baseline
/// objects, listed before it returns, are all still listed after it stopped. Only those two
/// endpoints are observed: a deletion and re-creation in between, or changed bytes, are not.
/// A missing baseline object fails; objects added in between are the caller's record.
fn baseline_still_listed(
	baseline: &std::collections::BTreeSet<String>,
	after: &[(String, u64)],
) -> Result<(), String> {
	let listed: std::collections::BTreeSet<&str> =
		after.iter().map(|(key, _)| key.as_str()).collect();
	let missing: Vec<&String> = baseline
		.iter()
		.filter(|key| !listed.contains(key.as_str()))
		.collect();
	if !missing.is_empty() {
		return Err(format!("baseline objects no longer listed: {missing:?}"));
	}
	Ok(())
}

/// The run's object keys owned by (`node`, `incarnation`), the incarnation as 32 hex digits.
fn keys_of_owner(objects: &[(String, u64)], node: u32, incarnation: &str) -> Vec<String> {
	let owner = format!("{node:08x}-{incarnation}");
	objects
		.iter()
		.filter(|(key, _)| key.rsplit('/').nth(1) == Some(owner.as_str()))
		.map(|(key, _)| key.clone())
		.collect()
}

/// The incarnations (32 hex digits) of `node`'s objects in a listing.
fn incarnations_of(objects: &[(String, u64)], node: u32) -> std::collections::BTreeSet<String> {
	let prefix = format!("{node:08x}-");
	objects
		.iter()
		.filter_map(|(key, _)| key.rsplit('/').nth(1))
		.filter_map(|owner| owner.strip_prefix(prefix.as_str()))
		.filter(|incarnation| {
			incarnation.len() == 32 && incarnation.bytes().all(|b| b.is_ascii_hexdigit())
		})
		.map(str::to_owned)
		.collect()
}

/// Three hosts (Codex 01a1e48d): the stopped-store probe, run ON a node's host by the run's
/// pinned test binary through the controller's `store-probe` (never by a person, never on a
/// running node). `PD_PROBE` is `role` or `recovered`; `PD_PROBE_CONFIG` is the node's own
/// configuration (its data directory and object store, as the node reads them);
/// `PD_PROBE_DESCRIPTOR` the table's descriptor as JSON for `recovered`. The store is opened in
/// place as the one-host oracle opens it (`stored_raft_role_for_test`, `recovered_rows_for_test`,
/// under its own directory owner, closed again) and ONE `PROBE <json>` line is printed. It holds
/// no key material: statement fields, counts, verification failures and the rows. It is not a
/// product interface.
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "run only by the three-host controller's store-probe, on a stopped node's host"]
async fn hosted_store_probe() {
	let kind = required("PD_PROBE");
	let config = PathBuf::from(std::env::var_os("PD_PROBE_CONFIG").unwrap_or_default());
	let probed = match kind.as_str() {
		"role" => {
			let data_dir = wendelon::config::load_node_bootstrap_config(&config)
				.unwrap_or_else(|error| panic!("the configuration: {error}"))
				.data_dir;
			Probed {
				role: wendelon::consensus::stored_raft_role_for_test(&data_dir)
					.unwrap_or_else(|error| panic!("the vote page: {error}")),
				recovered: None,
			}
		}
		"recovered" => {
			let descriptor: TableSchemaDescriptor = serde_json::from_slice(
				&std::fs::read(required("PD_PROBE_DESCRIPTOR"))
					.unwrap_or_else(|error| panic!("the descriptor: {error}")),
			)
			.unwrap_or_else(|error| panic!("the descriptor: {error}"));
			let found = wendelon::node::recovered_rows_for_test(&config, &descriptor)
				.await
				.unwrap_or_else(|error| panic!("the stopped store could not be read: {error}"));
			Probed {
				role: None,
				recovered: Some(RecoveredSummary::of(&found)),
			}
		}
		// The envelope alone, for the control that reads this binary's real libtest output.
		"envelope" => Probed {
			role: None,
			recovered: None,
		},
		other => panic!("PD_PROBE is {other:?}, not role or recovered"),
	};
	// libtest has already printed `test hosted_store_probe ... ` with no line end (Codex
	// 01a1e49d R1): the envelope starts a line of its own.
	println!(
		"\nPROBE {}",
		serde_json::to_string(&probed).expect("the probe as JSON")
	);
}

/// Wendelon ADR 0108: the (id, name) rows of `batches`, sorted (duplicates kept), refusing a
/// missing column or a null id or name: the fixture's values are never null, and Arrow's
/// `value` alone ignores validity.
fn strict_rows(batches: &[RecordBatch]) -> Result<Vec<(i64, String)>, String> {
	use arrow::array::Array;
	let mut held = Vec::new();
	for batch in batches {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.ok_or("no id column")?;
		let names = batch
			.column_by_name("name")
			.and_then(|column| column.as_any().downcast_ref::<LargeStringArray>())
			.ok_or("no name column")?;
		for row in 0..batch.num_rows() {
			if ids.is_null(row) || names.is_null(row) {
				return Err(format!("row {row} holds a null id or name"));
			}
			held.push((ids.value(row), names.value(row).to_owned()));
		}
	}
	held.sort();
	Ok(held)
}

/// Wendelon ADR 0108: the verdict on ONE direct answer observed at `answered_at`, whose call had
/// the one absolute end `by`. `answer` is `None` when the call was not answered in time;
/// otherwise the call's outcome, whose `Ok(None)` is a redirect. Only an OK answer from the
/// target holding exactly `expected` (sorted), observed by `by`, passes: an answer observed
/// after `by` fails even if it arrived complete.
fn direct_answer_verdict(
	answer: Option<Result<Option<Vec<(i64, String)>>, String>>,
	expected: &[(i64, String)],
	answered_at: Instant,
	by: Instant,
) -> Result<(), String> {
	if answered_at > by {
		return Err("observed only after the call's bound".to_owned());
	}
	match answer {
		None => Err("no answer within its bound".to_owned()),
		Some(Err(error)) => Err(format!("refused or failed: {error}")),
		Some(Ok(None)) => Err("redirected, not served there".to_owned()),
		Some(Ok(Some(rows))) => {
			let mut wanted = expected.to_vec();
			wanted.sort();
			if rows == wanted {
				Ok(())
			} else {
				Err(format!("answered {rows:?}, not exactly {wanted:?}"))
			}
		}
	}
}

/// Three hosts (Codex 01a1e48d point 3): an `exit-status` answer counts only whole and only
/// for the execution this test started; a 75 seen early, a signal, a missing or partial result
/// and another execution are told apart. The kept end's timing is `ReplacementExit`'s (above).
#[test]
fn a_hosted_end_is_read_whole_and_only_for_the_owned_execution() {
	let owned = "pid 41 started 900";
	assert_eq!(owned_end("running pid 41 started 900\n", owned), Ok(None));
	let early = owned_end(
		"ended pid 41 started 900 code 1 status 75 result exit-code\n",
		owned,
	)
	.unwrap()
	.unwrap();
	assert_eq!(early.code(), Some(ACTIVATION_RESTART_EXIT));
	assert!(
		restart_request(
			early,
			Instant::now(),
			Instant::now() + Duration::from_secs(1)
		)
		.is_ok()
	);
	let killed = owned_end(
		"ended pid 41 started 900 code 2 status 9 result signal\n",
		owned,
	)
	.unwrap()
	.unwrap();
	assert_eq!(
		(
			killed.code(),
			std::os::unix::process::ExitStatusExt::signal(&killed)
		),
		(None, Some(9))
	);
	assert!(
		restart_request(
			killed,
			Instant::now(),
			Instant::now() + Duration::from_secs(1)
		)
		.is_err()
	);
	let other = owned_end(
		"ended pid 41 started 901 code 1 status 75 result exit-code\n",
		owned,
	);
	assert!(other.unwrap_err().contains("not the one this test started"));
	assert!(owned_end("running pid 42 started 900\n", owned).is_err());
	for refused in [
		"",
		"ended pid 41 started 900 code 1 status 75\n",
		"ended pid 41 started 900 code 3 status 75 result exit-code\n",
		"ended pid 41 started 900 code 1 status x result exit-code\n",
		"ended pid 41 started 900 code 1 status 75 result exit-code\nrunning pid 41 started 900\n",
		"ended pid 0 started 900 code 1 status 75 result exit-code\n",
		"running pid 41 started\n",
		"unit fields incomplete\n",
	] {
		assert!(owned_end(refused, owned).is_err(), "{refused:?}");
	}
	assert_eq!(
		parse_identity("pid 7 started 8"),
		Ok("pid 7 started 8".to_owned())
	);
	for refused in [
		"pid 7 started",
		"pid 0 started 8",
		"pid 7 started 8 x",
		"pid x started 8",
	] {
		assert!(parse_identity(refused).is_err(), "{refused:?}");
	}
}

/// Three hosts (Codex 01a1e49d R1): the probe's envelope as THIS binary's real libtest prints
/// it under the controller's arguments, with the controller's RESULT line after it, is read
/// whole. No store is opened (`PD_PROBE=envelope`).
#[test]
fn the_real_probe_output_is_read_as_one_envelope() {
	let ran = Command::new(std::env::current_exe().expect("this test binary"))
		.args([
			"hosted_store_probe",
			"--exact",
			"--ignored",
			"--nocapture",
			"--test-threads",
			"1",
		])
		.env("PD_PROBE", "envelope")
		.env_remove("PD_PROBE_CONFIG")
		.stdin(Stdio::null())
		.output()
		.expect("run this binary's probe");
	assert!(ran.status.success(), "{ran:?}");
	let stdout = String::from_utf8(ran.stdout).unwrap();
	assert!(
		stdout.contains("test hosted_store_probe ... \nPROBE "),
		"the layout changed: {stdout:?}"
	);
	assert_eq!(
		parse_probe(&format!("{stdout}RESULT exit 0\n")),
		Ok(Probed {
			role: None,
			recovered: None
		}),
		"{stdout:?}"
	);
}

/// Three hosts (Codex 01a1e49d R2): only a coherent `exit-code` record with status 75 is a
/// restart request; an out-of-range or non-canonical status, a timeout or another result with
/// status 75, and a signal are not.
#[test]
fn only_a_coherent_exit_75_is_a_restart_request() {
	let owned = "pid 41 started 900";
	let request = |line: &str| -> Result<(), String> {
		let status = owned_end(line, owned)?.ok_or("still running")?;
		restart_request(
			status,
			Instant::now(),
			Instant::now() + Duration::from_secs(1),
		)
	};
	assert_eq!(
		request("ended pid 41 started 900 code 1 status 75 result exit-code\n"),
		Ok(())
	);
	for refused in [
		"ended pid 41 started 900 code 1 status 331 result exit-code\n",
		"ended pid 41 started 900 code 1 status 75 result timeout\n",
		"ended pid 41 started 900 code 1 status 75 result success\n",
		"ended pid 41 started 900 code 1 status 75 result oom-kill\n",
		"ended pid 41 started 900 code 1 status 075 result exit-code\n",
		"ended pid 41 started 900 code 1 status +75 result exit-code\n",
		"ended pid 41 started 900 code 2 status 75 result signal\n",
		"ended pid 41 started 900 code 2 status 9 result signal\n",
		"ended pid 41 started 900 code 2 status 9 result exit-code\n",
		"ended pid 41 started 900 code 1 status 1 result exit-code\n",
	] {
		assert!(request(refused).is_err(), "{refused:?}");
	}
	// The ordinary ends stay what they are (not requests): exit 1, exit 0, SIGKILL.
	let exit1 = owned_end(
		"ended pid 41 started 900 code 1 status 1 result exit-code\n",
		owned,
	)
	.unwrap()
	.unwrap();
	assert_eq!(exit1.code(), Some(1));
	let exit0 = owned_end(
		"ended pid 41 started 900 code 1 status 0 result success\n",
		owned,
	)
	.unwrap()
	.unwrap();
	assert_eq!(exit0.code(), Some(0));
	let killed = owned_end(
		"ended pid 41 started 900 code 2 status 9 result signal\n",
		owned,
	)
	.unwrap()
	.unwrap();
	assert_eq!(
		std::os::unix::process::ExitStatusExt::signal(&killed),
		Some(9)
	);
}

/// Three hosts (Codex 01a1e49d R3): a step's unit gets only the whole seconds left of the
/// step (never rounded up from less than one), and an answer observed after the step's
/// deadline is refused.
#[test]
fn a_hosted_step_gets_only_its_own_time_and_refuses_a_late_answer() {
	let now = Instant::now();
	assert_eq!(
		phase_seconds(now + Duration::from_millis(10_700), now, 45),
		Ok(10)
	);
	assert_eq!(
		phase_seconds(now + Duration::from_secs(100), now, 45),
		Ok(45)
	);
	assert!(phase_seconds(now + Duration::from_millis(900), now, 45).is_err());
	assert!(phase_seconds(now, now, 45).is_err());
	assert!(phase_seconds(now, now + Duration::from_secs(1), 45).is_err());
	let by = now + Duration::from_secs(60);
	assert_eq!(answered_within(by, by), Ok(()));
	assert!(answered_within(by + Duration::from_millis(1), by).is_err());
}

/// Three hosts: a one-off's RESULT is its one last line; a probe is one strict PROBE line with
/// a RESULT exit 0; a timeout is never an exit.
/// RESULT-03: a failed probe keeps its whole call text, so the panic its process wrote to
/// stderr is in the refusal. A nonzero RESULT, a missing envelope, a malformed one and a
/// timeout are each refused with the stderr sentinel, the stdout and the RESULT line.
#[test]
fn a_failed_probe_keeps_its_whole_call_diagnostic() {
	let sentinel = "SENTINEL-the-probe-panicked-here";
	let cases = [
		"\nrunning 1 test\ntest hosted_store_probe ... FAILED\n\nRESULT exit 101\n",
		"\nrunning 1 test\ntest hosted_store_probe ... ok\nRESULT exit 0\n",
		"\nrunning 1 test\ntest hosted_store_probe ... \nPROBE {\"role\":\nok\nRESULT exit 0\n",
		"\nrunning 1 test\nRESULT timeout\n",
	];
	for stdout in cases {
		// The call text as node_control_call_with writes it.
		let raw = format!(
			"node-control store-probe 3: exit Some(0); stdout: {}; stderr: thread 'hosted_store_probe' panicked: {sentinel}",
			stdout.trim()
		);
		let why = probe_answer(stdout.as_bytes(), &raw).expect_err(stdout);
		assert!(why.contains(sentinel), "{why}");
		let result = stdout.trim().lines().last().unwrap();
		assert!(why.contains(result), "{why}");
		assert!(why.contains("running 1 test"), "{why}");
	}
}

#[test]
fn a_hosted_one_off_and_probe_are_read_whole_or_refused() {
	assert_eq!(
		parse_one_off("refused: x\nRESULT exit 1\n"),
		Ok(("refused: x".to_owned(), OneOff::Exit(1)))
	);
	assert_eq!(
		parse_one_off("RESULT timeout\n").unwrap().1,
		OneOff::Timeout
	);
	assert_eq!(
		parse_one_off("RESULT signal 9\n").unwrap().1,
		OneOff::Signal(9)
	);
	for refused in [
		"",
		"out\n",
		"RESULT exit 1\nmore\n",
		"RESULT exit 1\nRESULT exit 0\n",
		"RESULT exit\n",
		"RESULT exit -1\n",
		"RESULT done\n",
	] {
		assert!(parse_one_off(refused).is_err(), "{refused:?}");
	}
	let probed = Probed {
		role: Some((FIRST_ALLOCATED, false)),
		recovered: None,
	};
	let line = format!(
		"running 1 test\nPROBE {}\ntest hosted_store_probe ... ok\nRESULT exit 0\n",
		serde_json::to_string(&probed).unwrap()
	);
	assert_eq!(parse_probe(&line), Ok(probed.clone()));
	let json = serde_json::to_string(&probed).unwrap();
	for refused in [
		format!("PROBE {json}\nRESULT exit 101\n"),
		format!("PROBE {json}\nPROBE {json}\nRESULT exit 0\n"),
		"RESULT exit 0\n".to_owned(),
		"PROBE {\"role\":null}\nRESULT exit 0\n".to_owned(),
		"PROBE {\"role\":null,\"recovered\":null,\"extra\":1}\nRESULT exit 0\n".to_owned(),
		"PROBE {\"role\":null,\"recovered\":{\"complete_children\":0,\"unverified\":[],\"rows\":[],\"active_gid\":0}}\nRESULT exit 0\n".to_owned(),
		format!("PROBE {json}\nRESULT timeout\n"),
	] {
		assert!(parse_probe(&refused).is_err(), "{refused:?}");
	}
	assert_eq!(base64_of(b""), "");
	assert_eq!(base64_of(b"f"), "Zg==");
	assert_eq!(base64_of(b"fo"), "Zm8=");
	assert_eq!(base64_of(b"foo"), "Zm9v");
	assert_eq!(
		base64_of(b"{\"table_object_id\":7}"),
		"eyJ0YWJsZV9vYmplY3RfaWQiOjd9"
	);
}

/// Three hosts (Codex 01a1e48d point 6): the old instance's baseline objects at two endpoints,
/// and the owner keys they are found by.
#[test]
fn an_old_instances_baseline_objects_are_checked_at_both_endpoints() {
	let i = "0123456789abcdef0123456789abcdef";
	let j = "fedcba9876543210fedcba9876543210";
	let key = |node: u32, incarnation: &str, slab: u32| {
		(
			format!("pd-smoke/x/{node:08x}-{incarnation}/slab_{slab:016x}_00.wnd"),
			1_u64,
		)
	};
	let listing = vec![key(3, i, 1), key(3, i, 2), key(3, j, 3), key(2, i, 1)];
	assert_eq!(
		incarnations_of(&listing, 3),
		[i.to_owned(), j.to_owned()].into_iter().collect()
	);
	let baseline: std::collections::BTreeSet<String> =
		keys_of_owner(&listing, 3, i).into_iter().collect();
	assert_eq!(baseline.len(), 2);
	assert_eq!(baseline_still_listed(&baseline, &listing), Ok(()));
	let mut more = listing.clone();
	more.push(key(3, i, 9));
	assert_eq!(baseline_still_listed(&baseline, &more), Ok(()));
	let fewer = vec![key(3, i, 1), key(3, j, 3)];
	assert!(
		baseline_still_listed(&baseline, &fewer)
			.unwrap_err()
			.contains("no longer listed")
	);
	assert_eq!(keys_of_owner(&listing, 3, j).len(), 1);
	assert_eq!(
		owner_and_slab(&key(3, j, 3).0),
		Some((3, "0000000000000003"))
	);
}

/// Wendelon ADR 0108 (Codex 01a1e471 R1, R2): the replacement's expected end. A restart request
/// observed early is kept, and the process is never polled again; another code or a signal is
/// refused; a request first observed after its bound is refused; there is no supervised start
/// before an end is kept, and only one after (`handed_off` consumes it).
#[test]
fn a_replacement_end_counts_only_as_a_restart_request_in_time() {
	use std::os::unix::process::ExitStatusExt;
	let request = std::process::ExitStatus::from_raw(ACTIVATION_RESTART_EXIT << 8);
	let start = Instant::now();
	let by = start + Duration::from_secs(60);

	let mut early = ReplacementExit::new(3, "pid 1 started 1".to_owned());
	early
		.record(|| None, || panic!("no end, no observation time"))
		.unwrap();
	assert!(early.seen.is_none());
	early.record(|| Some(request), || start).unwrap();
	early
		.record(
			|| panic!("a kept end is never polled again"),
			|| panic!("nor timed"),
		)
		.unwrap();
	let (status, identity) = early.handed_off(by).unwrap();
	assert_eq!(status.code(), Some(ACTIVATION_RESTART_EXIT));
	assert_eq!(identity, "pid 1 started 1");

	for refused in [
		std::process::ExitStatus::from_raw(1 << 8),
		std::process::ExitStatus::from_raw(0),
		std::process::ExitStatus::from_raw(libc::SIGKILL),
	] {
		let mut exit = ReplacementExit::new(3, "pid 2 started 2".to_owned());
		assert!(
			exit.record(|| Some(refused), || start).is_err(),
			"{refused}"
		);
		assert!(exit.seen.is_none());
		assert!(
			exit.handed_off(by).is_err(),
			"no supervised start after {refused}"
		);
	}

	let mut late = ReplacementExit::new(3, "pid 3 started 3".to_owned());
	late.record(|| Some(request), || by + Duration::from_millis(1))
		.unwrap();
	assert!(late.handed_off(by).unwrap_err().contains("after its bound"));

	// The poll began before the bound, and the end was observed only after it (a scheduling
	// pause inside `ended`): its time is the observation's, read after `ended` returned.
	let clock = std::cell::Cell::new(by - Duration::from_millis(1));
	let mut paused = ReplacementExit::new(3, "pid 5 started 5".to_owned());
	paused
		.record(
			|| {
				clock.set(by + Duration::from_millis(1));
				Some(request)
			},
			|| clock.get(),
		)
		.unwrap();
	assert!(
		paused
			.handed_off(by)
			.unwrap_err()
			.contains("after its bound")
	);

	let none = ReplacementExit::new(3, "pid 4 started 4".to_owned());
	assert!(
		none.handed_off(by).is_err(),
		"no supervised start before an end is kept"
	);
}

#[test]
fn a_direct_answer_passes_only_exact_ok_and_in_time() {
	let expected = vec![(2, "b".to_owned()), (1, "a".to_owned())];
	let rows = |rows: &[(i64, &str)]| {
		Some(Ok(Some(
			rows.iter()
				.map(|(id, name)| (*id, (*name).to_owned()))
				.collect::<Vec<_>>(),
		)))
	};
	let now = Instant::now();
	let by = now + Duration::from_secs(1);
	assert_eq!(
		direct_answer_verdict(rows(&[(1, "a"), (2, "b")]), &expected, now, by),
		Ok(())
	);
	// An exact answer observed after the call's own bound fails, whatever later bound remains.
	assert!(
		direct_answer_verdict(
			rows(&[(1, "a"), (2, "b")]),
			&expected,
			by + Duration::from_millis(1),
			by
		)
		.is_err(),
		"an exact answer observed after the probe's end"
	);
	for (answer, why) in [
		(rows(&[(1, "a")]), "OK with a missing row"),
		(rows(&[]), "OK and empty"),
		(rows(&[(1, "a"), (2, "x")]), "OK with another value"),
		(Some(Ok(None)), "a redirect"),
		(Some(Err("refused".to_owned())), "a refusal"),
		(None, "no answer"),
	] {
		assert!(
			direct_answer_verdict(answer, &expected, now, by).is_err(),
			"{why}"
		);
	}
	// A null id or name is refused, never read as a value.
	let schema = std::sync::Arc::new(arrow::datatypes::Schema::new(vec![
		arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int64, true),
		arrow::datatypes::Field::new("name", arrow::datatypes::DataType::LargeUtf8, true),
	]));
	let batch = |ids: Vec<Option<i64>>, names: Vec<Option<&str>>| {
		RecordBatch::try_new(
			schema.clone(),
			vec![
				std::sync::Arc::new(Int64Array::from(ids)),
				std::sync::Arc::new(LargeStringArray::from(names)),
			],
		)
		.unwrap()
	};
	assert_eq!(
		strict_rows(&[batch(vec![Some(2), Some(1)], vec![Some("b"), Some("a")])]),
		Ok(vec![(1, "a".to_owned()), (2, "b".to_owned())])
	);
	assert!(strict_rows(&[batch(vec![Some(1)], vec![None])]).is_err());
	assert!(strict_rows(&[batch(vec![None], vec![Some("a")])]).is_err());
	assert_eq!(accepted_in_time(true, now, by), Ok(true));
	assert_eq!(accepted_in_time(false, now, by), Ok(false));
	assert!(
		accepted_in_time(true, by + Duration::from_millis(1), by).is_err(),
		"a match after its bound is never accepted"
	);
	assert!(accepted_in_time(false, by + Duration::from_millis(1), by).is_err());
}

/// Wendelon ADR 0108: one LINEARIZABLE table query sent straight to `target`, with no redirect
/// followed and nothing asked again, bounded by `PROBE_BOUND` and `deadline`: the rows of that
/// ONE answer must be exactly `expected`, observed within the bound.
async fn direct_query_exact(
	reader: &Client,
	descriptor: &TableSchemaDescriptor,
	target: u32,
	expected: &[(i64, String)],
	deadline: Instant,
	what: &str,
) {
	let inner = reader.inner().clone();
	let mut meta: wendelon::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
	meta.expected_epoch = inner.epoch();
	meta.tenant_id = TenantId::new(TENANT);
	meta.table_object_id = descriptor.table_object_id;
	meta.schema_generation = descriptor.schema_generation;
	meta.physical_descriptor_fingerprint = *descriptor.physical_descriptor_fingerprint.as_bytes();
	meta.consistency = wendelon::schema::Consistency::LINEARIZABLE;
	meta.row_identity_projection = wendelon::client::RowIdentityProjection::None.as_u8();
	// ONE absolute end for the call: its wire budget, its wrapper and the observed-success check.
	let asked = Instant::now();
	let by = deadline.min(asked + PROBE_BOUND);
	let bound = by.saturating_duration_since(asked);
	meta.deadline_remaining_millis = bound.as_millis() as u64;
	let request = wendelon::client::TableQueryRequest {
		meta,
		descriptor: descriptor.clone(),
		pbc: Vec::new(),
		runtime_filter: None,
		row_identity: wendelon::client::RowIdentityProjection::None,
	};
	let answer = tokio::time::timeout(
		bound,
		inner.table_query_rows_at_for_test(
			wendelon::schema::NodeId {
				id: target,
				_reserved: 0,
			},
			request,
		),
	)
	.await
	.ok()
	.map(|answer| match answer {
		Ok(Some(batches)) => strict_rows(&batches).map(Some),
		Ok(None) => Ok(None),
		Err(error) => Err(error.to_string()),
	});
	let answered_at = Instant::now();
	if let Err(why) = direct_answer_verdict(answer, expected, answered_at, by) {
		panic!("{what}: a query sent straight to node {target}: {why}");
	}
	eprintln!(
		"smoke: {what}: a query sent straight to node {target} answered exactly {} rows there in {:?}",
		expected.len(),
		asked.elapsed()
	);
}

/// Wendelon ADR 0106: reads the stopped replacement's store once, within `by` (checked before
/// the result is accepted). On a timeout the read is left to finish and close on its own; it is
/// never cancelled inside an open.
async fn inspect_recovered(
	config: &Path,
	descriptor: &TableSchemaDescriptor,
	by: Instant,
) -> RecoveredSummary {
	let config = config.to_path_buf();
	let descriptor = descriptor.clone();
	let mut read =
		tokio::spawn(
			async move { wendelon::node::recovered_rows_for_test(&config, &descriptor).await },
		);
	match tokio::time::timeout(by.saturating_duration_since(Instant::now()), &mut read).await {
		Ok(joined) => {
			let found = joined
				.expect("the stopped-store read")
				.unwrap_or_else(|error| panic!("the stopped store could not be read: {error}"));
			assert!(
				Instant::now() <= by,
				"the stopped store was read after its bound"
			);
			RecoveredSummary::of(&found)
		}
		Err(_) => panic!("the stopped store was not read within its bound"),
	}
}

/// Wendelon ADR 0106: the stopped replacement's saved statement is exactly the recorded one,
/// every Complete child verifies from its stored rows, and the rows read from the slab are
/// exactly `expected`, one copy each.
fn expect_recovered(
	found: &RecoveredSummary,
	(token, store, recovered): (u128, u128, (u64, u64)),
	expected: &[(i64, String)],
	what: &str,
) {
	assert_eq!(
		found.saved,
		Some((
			format!("{token:032x}"),
			FIRST_ALLOCATED,
			format!("{store:032x}"),
			recovered.0,
			recovered.1
		)),
		"{what}: the statement saved on its store is the recorded one"
	);
	assert!(
		found.unverified.is_empty(),
		"{what}: children fail the stored-row verification: {:?}",
		found.unverified
	);
	let held = found.rows.clone();
	let mut wanted = expected.to_vec();
	wanted.sort();
	assert_eq!(
		held, wanted,
		"{what}: the rows read from the replacement's slab"
	);
	eprintln!(
		"smoke: {what}: {} Complete children verified; {} rows read from the slab exactly",
		found.complete_children,
		held.len()
	);
}
/// The (id, name) rows of `batches`, sorted.
fn held_rows(batches: &[RecordBatch]) -> Vec<(i64, String)> {
	let mut held = Vec::new();
	for batch in batches {
		let ids = batch
			.column_by_name("id")
			.and_then(|column| column.as_any().downcast_ref::<Int64Array>())
			.expect("an id column");
		let names = batch
			.column_by_name("name")
			.and_then(|column| column.as_any().downcast_ref::<LargeStringArray>())
			.expect("a name column");
		for row in 0..batch.num_rows() {
			held.push((ids.value(row), names.value(row).to_owned()));
		}
	}
	held.sort();
	held
}

/// One routed Raft probe's bound: dial, request and answer.
const PROBE_BOUND: Duration = Duration::from_secs(10);
/// The first allocated Raft instance: the one a cluster's first replacement is granted.
const FIRST_ALLOCATED: u32 = 1 << 31;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum ReportedPhase {
	Begun,
	Reserved(u32),
	Done(u32),
}

/// One report line of `nilket wendelon replace begin|status`, read whole:
/// `replacement <32 hex digits>: slot <n> replacing instance <n>: <phase>`, the phase `begun`,
/// `reserved <n>` or `done <n>`. Anything else is not a report.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct Reported {
	token: u128,
	slot: u32,
	r_old: u32,
	phase: ReportedPhase,
	/// Wendelon ADR 0105: the promoted store, printed only after `done`.
	promoted: Option<u128>,
	/// Wendelon ADR 0105: the decision key bound to that store, printed only after it.
	bound_key: Option<[u8; 32]>,
	/// Wendelon ADR 0106: the recorded recovery's (gid, cut), printed only after the key.
	recovered: Option<(u64, u64)>,
	/// Wendelon ADR 0107: the cluster's current certificate (gid, cut), printed only after the
	/// recovery.
	certified: Option<(u64, u64)>,
	/// Wendelon ADR 0108: the recorded activation's (cut, certificate gid), printed only after
	/// the recovery.
	activated: Option<(u64, u64)>,
}

/// A decimal number of digits only.
fn number(text: &str) -> Option<u32> {
	(!text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
		.then(|| text.parse().ok())
		.flatten()
}

/// A token of exactly 32 hexadecimal digits.
fn token_of(text: &str) -> Option<u128> {
	(text.len() == 32 && text.bytes().all(|byte| byte.is_ascii_hexdigit()))
		.then(|| u128::from_str_radix(text, 16).ok())
		.flatten()
}

fn parse_report(line: &str) -> Option<Reported> {
	let rest = line.strip_prefix("replacement ")?;
	let (token, rest) = rest.split_once(": slot ")?;
	let (slot, rest) = rest.split_once(" replacing instance ")?;
	let (r_old, phase) = rest.split_once(": ")?;
	let (phase, recovered, certified, activated) = match phase.split_once("; recovered gid ") {
		Some((phase, rest)) => {
			let (rest, certified) = match rest.split_once("; certified gid ") {
				Some((rest, certified)) => {
					let (gid, t) = certified.split_once(" through ")?;
					(rest, Some((count(gid)?, count(t)?)))
				}
				None => (rest, None),
			};
			let (rest, activated) = match rest.split_once("; activated through ") {
				Some((rest, activated)) => {
					let (through, gid) = activated.split_once(" at gid ")?;
					(rest, Some((count(through)?, count(gid)?)))
				}
				None => (rest, None),
			};
			let (gid, k) = rest.split_once(" through ")?;
			(phase, Some((count(gid)?, count(k)?)), certified, activated)
		}
		None => (phase, None, None, None),
	};
	let (phase, promoted, bound_key) = match phase.split_once("; promoted store ") {
		Some((phase, rest)) => match rest.split_once("; bound key ") {
			Some((store, key)) => (phase, Some(token_of(store)?), Some(key_of(key)?)),
			None => (phase, Some(token_of(rest)?), None),
		},
		None => (phase, None, None),
	};
	if recovered.is_some() && bound_key.is_none() {
		return None;
	}
	let phase = match phase.split_once(' ') {
		None if phase == "begun" && promoted.is_none() => ReportedPhase::Begun,
		Some(("reserved", instance)) if promoted.is_none() => {
			ReportedPhase::Reserved(number(instance)?)
		}
		Some(("done", instance)) => ReportedPhase::Done(number(instance)?),
		_ => return None,
	};
	Some(Reported {
		token: token_of(token)?,
		slot: number(slot)?,
		r_old: number(r_old)?,
		phase,
		promoted,
		bound_key,
		recovered,
		certified,
		activated,
	})
}

/// A decimal count of digits only (u64).
fn count(text: &str) -> Option<u64> {
	(!text.is_empty() && text.bytes().all(|byte| byte.is_ascii_digit()))
		.then(|| text.parse().ok())
		.flatten()
}

/// Exactly 64 lowercase hex digits: a 32-byte key.
fn key_of(text: &str) -> Option<[u8; 32]> {
	if text.len() != 64
		|| !text
			.bytes()
			.all(|byte| matches!(byte, b'0'..=b'9' | b'a'..=b'f'))
	{
		return None;
	}
	let mut key = [0_u8; 32];
	for (index, byte) in key.iter_mut().enumerate() {
		*byte = u8::from_str_radix(&text[2 * index..2 * index + 2], 16).ok()?;
	}
	Some(key)
}

/// The token `begin` prints before it sends anything:
/// `replacement token <32 hex digits> for slot <n>; …`.
fn announced_token(line: &str, slot: u32) -> Option<u128> {
	let rest = line.strip_prefix("replacement token ")?;
	let (token, rest) = rest.split_once(" for slot ")?;
	let (named, _) = rest.split_once(';')?;
	(number(named)? == slot).then_some(())?;
	token_of(token)
}

#[test]
fn a_replacement_report_is_read_whole_or_not_at_all() {
	let token = "0104000000000000000000000000aa00";
	let report = |phase: &str| format!("replacement {token}: slot 3 replacing instance 3: {phase}");
	let reported = |phase| {
		Some(Reported {
			token: 0x0104000000000000000000000000aa00,
			slot: 3,
			r_old: 3,
			phase,
			promoted: None,
			bound_key: None,
			recovered: None,
			certified: None,
			activated: None,
		})
	};
	assert_eq!(
		parse_report(&report("begun")),
		reported(ReportedPhase::Begun)
	);
	assert_eq!(
		parse_report(&report("reserved 2147483648")),
		reported(ReportedPhase::Reserved(FIRST_ALLOCATED))
	);
	assert_eq!(
		parse_report(&report("done 2147483648")),
		reported(ReportedPhase::Done(FIRST_ALLOCATED))
	);
	// Wendelon ADR 0105: a promoted store follows `done`, and only `done`.
	let store = "00000000000000000000000000000105";
	assert_eq!(
		parse_report(&report(&format!("done 2147483648; promoted store {store}"))),
		Some(Reported {
			promoted: Some(0x0105),
			..reported(ReportedPhase::Done(FIRST_ALLOCATED)).unwrap()
		})
	);
	let key = "ab".repeat(32);
	assert_eq!(
		parse_report(&report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}"
		))),
		Some(Reported {
			promoted: Some(0x0105),
			bound_key: Some([0xab; 32]),
			..reported(ReportedPhase::Done(FIRST_ALLOCATED)).unwrap()
		})
	);
	assert_eq!(
		parse_report(&report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; recovered gid 2 through 41"
		))),
		Some(Reported {
			promoted: Some(0x0105),
			bound_key: Some([0xab; 32]),
			recovered: Some((2, 41)),
			..reported(ReportedPhase::Done(FIRST_ALLOCATED)).unwrap()
		})
	);
	// Wendelon ADR 0107: the current certificate follows the recovery, never alone.
	assert_eq!(
		parse_report(&report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; recovered gid 2 through 41; \
			 certified gid 3 through 90"
		))),
		Some(Reported {
			promoted: Some(0x0105),
			bound_key: Some([0xab; 32]),
			recovered: Some((2, 41)),
			certified: Some((3, 90)),
			..reported(ReportedPhase::Done(FIRST_ALLOCATED)).unwrap()
		})
	);
	// Wendelon ADR 0108: the activation follows the recovery, before the certificate.
	assert_eq!(
		parse_report(&report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; recovered gid 2 through 41; \
			 activated through 60 at gid 3; certified gid 3 through 90"
		))),
		Some(Reported {
			promoted: Some(0x0105),
			bound_key: Some([0xab; 32]),
			recovered: Some((2, 41)),
			activated: Some((60, 3)),
			certified: Some((3, 90)),
			..reported(ReportedPhase::Done(FIRST_ALLOCATED)).unwrap()
		})
	);
	for refused in [
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; recovered gid 2 through 41; \
			 activated through 60"
		)),
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; activated through 60 at gid 3"
		)),
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; certified gid 3 through 90"
		)),
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; recovered gid 2 through 41; \
			 certified gid 3 through"
		)),
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; recovered gid 2 through 41; \
			 certified gid 3"
		)),
		report(&format!(
			"done 2147483648; promoted store {store}; recovered gid 2 through 41"
		)),
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {key}; recovered gid 2 through"
		)),
		report(&format!("done 2147483648; bound key {key}")),
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {}",
			&key[1..]
		)),
		report(&format!(
			"done 2147483648; promoted store {store}; bound key {}",
			key.to_uppercase()
		)),
		report(&format!("begun; promoted store {store}")),
		report(&format!("reserved 2147483648; promoted store {store}")),
		report("done 2147483648; promoted store 105"),
		report(&format!("done 2147483648; promoted store {store} ")),
		report("done"),
		report("done +2147483648"),
		report("done 2147483648 "),
		report("Done { r_new: 2147483648 }"),
		report("finished 1"),
		format!("replacement {token}0: slot 3 replacing instance 3: begun"),
		format!(
			"replacement {}: slot 3 replacing instance 3: begun",
			&token[1..]
		),
		format!("replacement {token}: slot 3 replacing instance: begun"),
		String::new(),
	] {
		assert_eq!(parse_report(&refused), None, "{refused:?}");
	}
	let announced = format!("replacement token {token} for slot 3; retry with …");
	assert_eq!(
		announced_token(&announced, 3),
		Some(0x0104000000000000000000000000aa00)
	);
	assert_eq!(announced_token(&announced, 2), None);
	assert_eq!(
		announced_token(&format!("replacement token {token}"), 3),
		None
	);
}

/// One `nilket wendelon replace <arguments>` run.
fn replace_command(step: &str, arguments: &[std::ffi::OsString], deadline: Instant) -> Ran {
	operator_command("replace", step, arguments, REPLACE_COMMAND_BOUND, deadline)
}

/// The operator's status read of `token`, parsed: `None` when the command did not succeed in
/// its bound or printed anything but one report line for this token and `slot`.
fn replacement_status(
	step: &str,
	token: u128,
	slot: u32,
	connection: &[std::ffi::OsString],
	deadline: Instant,
) -> Option<ReportedPhase> {
	replacement_report(step, token, slot, connection, deadline).map(|reported| reported.phase)
}

/// The whole status report of `token`, as `replacement_status` reads it.
fn replacement_report(
	step: &str,
	token: u128,
	slot: u32,
	connection: &[std::ffi::OsString],
	deadline: Instant,
) -> Option<Reported> {
	let mut arguments: Vec<std::ffi::OsString> = vec![
		"status".into(),
		"--token".into(),
		format!("{token:032x}").into(),
	];
	arguments.extend_from_slice(connection);
	let ran = replace_command(step, &arguments, deadline);
	if !ran.succeeded() {
		return None;
	}
	let lines: Vec<&str> = ran.stdout.lines().collect();
	let [line] = lines.as_slice() else {
		panic!("{step}: not one report line: {}", ran.text());
	};
	let reported =
		parse_report(line).unwrap_or_else(|| panic!("{step}: not a report: {}", ran.text()));
	assert_eq!(
		(reported.token, reported.slot, reported.r_old),
		(token, slot, slot),
		"{step}: another replacement: {}",
		ran.text()
	);
	Some(reported)
}

/// ONE logical write of row `row_id` under one operation identity through `through`, offered
/// again only for the answers `offers_the_same_operation_again` names, within
/// `OFFERED_WRITE_BOUND`: a range whose holder is gone is taken over first (ADR 0729). Its
/// receipt's coordinator, a member that is not `gone`, and its receipt's decision index.
async fn commit_offering_again(
	through: &Client,
	descriptor: &TableSchemaDescriptor,
	row_id: i64,
	name: &str,
	gone: u32,
	what: &str,
) -> (u32, u64) {
	let identity = operation_id();
	let end = Instant::now() + OFFERED_WRITE_BOUND;
	let started = Instant::now();
	let mut attempts = 0_u32;
	loop {
		attempts += 1;
		let left = end.saturating_duration_since(Instant::now());
		assert!(
			!left.is_zero(),
			"{what}: row {row_id} did not commit within {OFFERED_WRITE_BOUND:?} ({attempts} attempts)"
		);
		let answer = tokio::time::timeout(
			left,
			through.table_write(
				TenantId::new(TENANT),
				descriptor,
				one_row(row_id, name),
				WriteOptions {
					deadline: Some(Instant::now() + left),
					operation_id: Some(identity),
					..WriteOptions::default()
				},
			),
		)
		.await
		.unwrap_or_else(|_| panic!("{what}: row {row_id} had no answer within its bound"));
		match answer {
			Ok(receipt) => {
				let coordinator = receipt.commit_hlc.node_id;
				eprintln!(
					"smoke: {what}: row {row_id} committed under node {coordinator} after \
					 {attempts} attempts in {:?}",
					started.elapsed()
				);
				assert!(
					NODES.contains(&coordinator) && coordinator != gone,
					"{what}: row {row_id} was stamped by {coordinator}"
				);
				return (coordinator, receipt.log_index);
			}
			Err(error) if offers_the_same_operation_again(&error, identity) => {
				eprintln!("smoke: {what}: attempt {attempts} of row {row_id}: {error}");
				tokio::time::sleep(Duration::from_millis(250)).await;
			}
			Err(error) => panic!("{what}: the write of row {row_id}: {error:?}"),
		}
	}
}

/// Every survivor's view: the expected rows exactly, through a client discovered from each.
async fn expect_at_survivors(
	survivors: &[&Member],
	pki: &Pki,
	descriptor: &TableSchemaDescriptor,
	deadline: Instant,
	expected: &[(i64, String)],
	what: &str,
) {
	for member in survivors {
		let through = client_through(member, pki, Credential::Storage, deadline).await;
		expect_rows(
			&through,
			descriptor,
			deadline,
			&format!("{what}, discovered through node {}", member.id),
			expected,
		)
		.await;
	}
}

/// ADR 0104 (wendelon), member replacement gate 1, with real processes on ONE host.
///
/// A three-daemon cluster takes rows; a follower is killed (SIGKILL, confirmed) and its disk is
/// lost: its data directory is moved aside whole, never deleted, and an empty one takes its
/// place. Then, each judged from typed or parsed facts only:
/// - a blank store given the cluster's sealed root, as a joiner is, cannot start as the lost
///   voter: the full start exits non-zero within its bound, refused `SlotGranted`;
/// - the operator's `replace begin` through a survivor prints the token before it sends
///   anything and reports the replacement `begun`; `replace provision` writes the token;
/// - the node's unchanged configuration starts it control-only, and the operator's status
///   reads report `done <first allocated instance>` within `DONE_BOUND`;
/// - the survivors take new writes (ranges the lost slot held move by ADR 0729's takeover) and
///   read every acknowledged row exactly;
/// - wendelon ADR 0105: within `PREPARED_BOUND` of done, the operator's status reports the
///   replacement's own store promoted and the slot's configured decision key bound to it (the
///   key compared with the one this test configured; the test binds nothing itself); stopped,
///   its store's vote page names the first allocated instance, control-only; restarted, the
///   status names the same store and key;
/// - wendelon ADR 0106: enrolled under the cluster's root as a joiner is, the replacement
///   recovers the slot's data from the survivors and the status reports its recovery
///   recorded within `RECOVERED_BOUND`; after one declared `DELTA_WAIT` with the process
///   running (writes accepted after the recorded cut are repaired as delta), stopped, its store
///   (opened as its recovery opens it, under its own directory owner, read once within one
///   absolute bound of `DELTA_WAIT + INSPECTION_BOUND`) saves exactly the
///   recorded statement for this token, instance and store, every Complete child verifies from
///   its stored rows, and the rows read from its slab are exactly the acknowledged ones;
///   restarted, the status still reports it, and after one declared `DELTA_WAIT` with that
///   process running, one stop and one read show the row written after the restart held the
///   same way (the delta is repaired only after that start's validation);
/// - wendelon ADR 0108: the recovered replacement activates on its own, and the status reports
///   its activation (through a cut no lower than the recorded one) within `ACTIVATED_BOUND`;
///   the owned ControlOnly process (the one this test started) completes its handoff and exits
///   with exactly the restart-request status within `HANDOFF_EXIT_BOUND` (any other status, a
///   signal or a timeout fails); reaped, its vote page names the first allocated instance, Full.
///   This test then acts as the declared supervisor and starts it exactly once, from the same
///   directory, executable and configuration (an expected supervised handoff, not an operator
///   repair, and not a test of a service manager's wiring): a new process, it answers ready
///   (within `SERVING_BOUND`) as that instance serving the promoted store, and one query sent
///   straight to it, with no redirect followed, is answered OK there. Row 11, written then, is held by it: after one
///   declared `DELTA_WAIT`, stopped in order, its vote page names that instance, Full, and its
///   store (read once within its bound) saves the recorded statement, verifies every Complete
///   child and holds exactly the acknowledged rows. Restarted Full, it answers ready as itself
///   again, a direct query is answered there, and the status reports the same recovery and
///   activation. Its certification as a Full member is an in-process control; ADR 0107's
///   real-time expiry run (pd-replace-20261116T063455Z) is carried, not repeated;
/// - the old instance comes back on its old disk at the slot's address while the replacement's
///   process is stopped in order, and must keep running (its process identity is read). A
///   survivor sends it authenticated, correctly formed Raft votes as itself, each bounded and
///   on a fresh connection. The one addressed to the old instance is answered, the one addressed
///   to the replacement is refused with no answer, and the old instance answers again after it.
///   Wendelon ADR 0105: one table query sent straight to the slot (the old process), with no
///   redirect followed and nothing asked again, is not answered OK, between two to a survivor
///   that are.
///   Then a survivor still names a survivor as leader, a new write commits, and reads stay exact.
///   The old process's ready answer and the refusals counted in its log are printed as a record;
///   neither is an oracle;
/// - the replacement's process returns: still done, writes commit, reads exact; every process
///   stops in order with status 0.
///
/// Not claimed: more than one host, a leader's loss, the replacement leading, its index rebuild,
/// its certification after the handover (in-process controls), a lease request by the old
/// process (counted only if one is observed), or any time as a measurement.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons, kills one and replaces it through the operator's commands; run alone under an external owner (see the module text)"]
async fn three_daemons_replace_a_lost_follower_with_a_control_only_instance() {
	assert!(
		node_control().is_none(),
		"this workflow runs its three daemons on one host"
	);
	let deadline = Instant::now() + REPLACEMENT_TEST_BOUND;
	let started = Instant::now();
	let Formed {
		root,
		pki,
		members,
		mut daemons,
		descriptor,
		decision_keys,
	} = form(deadline, None).await;
	let storage = client_through(&members[0], &pki, Credential::Storage, deadline).await;
	let mut expected = Vec::new();
	for id in 1_i64..=4 {
		// ONE call each: an error is this step's result.
		let bound = step(deadline);
		tokio::time::timeout(
			bound,
			storage.table_write(
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
		.unwrap_or_else(|_| panic!("the write of row {id} had no answer within its bound"))
		.unwrap_or_else(|error| panic!("the write of row {id}: {error:?}"));
		expected.push((id, "before".to_owned()));
	}

	// The lost member is a follower, chosen positively from node 1's view of the leader.
	let inspection = inspect(&members[0], &pki);
	assert!(inspection.ready, "node 1 is not ready: {}", inspection.raw);
	let leader = inspection
		.leader()
		.unwrap_or_else(|| panic!("node 1 names no leader among the three: {}", inspection.raw));
	let lost = members
		.iter()
		.rev()
		.find(|member| member.id != leader)
		.expect("a node that is not the leader");
	let survivors: Vec<&Member> = members
		.iter()
		.filter(|member| member.id != lost.id)
		.collect();
	eprintln!("smoke: leader {leader}; node {} loses its disk", lost.id);
	eprintln!("smoke: {}", daemons.crash(lost.id));
	daemons.still_down(lost.id);
	let node_dir = lost.config.parent().unwrap().to_path_buf();
	let data = node_dir.join("data");
	let old_data = node_dir.join("old-data");
	std::fs::rename(&data, &old_data).unwrap();
	std::fs::create_dir(&data).unwrap();

	// A blank store is not the lost voter, even with the cluster's sealed root as a joiner
	// has it. Its directory is then moved aside too, kept.
	let sealed_root = Path::new(".wendelon").join("local-sealed-root-kek.bin");
	let sealed = std::fs::read(old_data.join(&sealed_root)).unwrap();
	std::fs::create_dir_all(data.join(".wendelon")).unwrap();
	write_private(&data.join(&sealed_root), &sealed);
	let blank = bounded_run(
		&nilket_bin(),
		&["wendelon".into(), lost.config.clone().into_os_string()],
		deadline.min(Instant::now() + REFUSED_START_BOUND),
	);
	eprintln!("smoke: blank full start: {}", blank.text());
	assert!(
		blank.within && blank.code.is_some_and(|code| code != 0),
		"a blank store was not refused as the lost voter: {}",
		blank.text()
	);
	assert!(
		blank.stderr.contains("InstanceGrantRefused: SlotGranted"),
		"the blank start was refused for another reason: {}",
		blank.text()
	);
	std::fs::rename(&data, node_dir.join("blank-start-data")).unwrap();
	std::fs::create_dir(&data).unwrap();
	// Wendelon ADR 0106: the replacement is enrolled under the cluster's root as a joiner
	// is (ADR 0726), so it can recover the slot's data.
	std::fs::create_dir_all(data.join(".wendelon")).unwrap();
	write_private(&data.join(&sealed_root), &sealed);

	// The operator opens the replacement through a survivor and provisions its token.
	let connection = operator_connection(survivors[0], &pki);
	let mut arguments: Vec<std::ffi::OsString> =
		vec!["begin".into(), "--slot".into(), lost.id.to_string().into()];
	arguments.extend(connection.iter().cloned());
	let began = replace_command("replace-begin", &arguments, deadline);
	assert!(began.succeeded(), "replace begin: {}", began.text());
	let lines: Vec<&str> = began.stdout.lines().collect();
	let [announced, report] = lines.as_slice() else {
		panic!(
			"replace begin printed {} lines: {}",
			lines.len(),
			began.text()
		);
	};
	let token = announced_token(announced, lost.id)
		.unwrap_or_else(|| panic!("replace begin announced no token first: {}", began.text()));
	assert_eq!(
		parse_report(report),
		Some(Reported {
			token,
			slot: lost.id,
			r_old: lost.id,
			phase: ReportedPhase::Begun,
			promoted: None,
			bound_key: None,
			recovered: None,
			certified: None,
			activated: None,
		}),
		"replace begin: {}",
		began.text()
	);
	let provisioned = replace_command(
		"replace-provision",
		&[
			"provision".into(),
			"--data-dir".into(),
			data.clone().into_os_string(),
			"--token".into(),
			format!("{token:032x}").into(),
		],
		deadline,
	);
	assert!(
		provisioned.succeeded(),
		"replace provision: {}",
		provisioned.text()
	);

	// The node's unchanged configuration starts it, control-only, and the leader's controller
	// adds, promotes and records it.
	let replacement_started = Instant::now();
	daemons.start(lost);
	// Wendelon ADR 0108: from now through its handoff, this process may end on its own, but only
	// with the restart request; every other daemon is held to running.
	let mut replacement = ReplacementExit::new(lost.id, daemons.identity(lost.id));
	let done_by = deadline.min(Instant::now() + DONE_BOUND);
	let mut reads = 0_u32;
	let done = loop {
		reads += 1;
		let phase = replacement_status(
			&format!("replace-status-{reads}"),
			token,
			lost.id,
			&connection,
			done_by,
		);
		if let Some(ReportedPhase::Done(instance)) = phase {
			break instance;
		}
		replacement.poll(&mut daemons, "before done");
		assert!(
			Instant::now() < done_by,
			"the replacement was not done within {DONE_BOUND:?} ({reads} status reads, last \
			 {phase:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	eprintln!(
		"smoke: replacement done as instance {done} {:?} after its start ({reads} status reads)",
		replacement_started.elapsed()
	);
	assert_eq!(done, FIRST_ALLOCATED);

	// The survivors keep every acknowledged row and take new writes.
	let through = client_through(survivors[0], &pki, Credential::Storage, deadline).await;
	for id in 5_i64..=8 {
		commit_offering_again(&through, &descriptor, id, "replaced", lost.id, "after done").await;
		expected.push((id, "replaced".to_owned()));
	}
	expect_at_survivors(
		&survivors,
		&pki,
		&descriptor,
		deadline,
		&expected,
		"after done",
	)
	.await;

	// Wendelon ADR 0105: the replacement admits its own store and binds its slot's configured
	// key, both read from the operator's status (neither is written by this test); and it is
	// still control-only on disk after a restart.
	let configured = decision_keys[NODES.iter().position(|id| *id == lost.id).unwrap()];
	let prepared_from = Instant::now();
	let prepared_by = deadline.min(prepared_from + PREPARED_BOUND);
	let mut reads = 0_u32;
	let store = loop {
		reads += 1;
		let reported = replacement_report(
			&format!("replace-prepared-{reads}"),
			token,
			lost.id,
			&connection,
			prepared_by,
		);
		if let Some(Reported {
			phase: ReportedPhase::Done(FIRST_ALLOCATED),
			promoted: Some(store),
			bound_key: Some(key),
			..
		}) = reported
		{
			assert_eq!(
				key, configured,
				"the bound key is the slot's configured one"
			);
			break store;
		}
		replacement.poll(&mut daemons, "before the replacement was prepared");
		assert!(
			Instant::now() < prepared_by,
			"the replacement was not prepared within {PREPARED_BOUND:?} ({reads} status reads, \
			 last {reported:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	eprintln!(
		"smoke: replacement promoted store {store:032x} and bound its configured key {:?} after \
		 done ({reads} status reads)",
		prepared_from.elapsed()
	);
	// Wendelon ADR 0106: the replacement recovers the slot's data and its recovery is
	// recorded, read from the operator's status.
	let recovered_from = Instant::now();
	let recovered_by = deadline.min(recovered_from + RECOVERED_BOUND);
	let mut reads = 0_u32;
	let recovered = loop {
		reads += 1;
		let reported = replacement_report(
			&format!("replace-recovered-{reads}"),
			token,
			lost.id,
			&connection,
			recovered_by,
		);
		if let Some(Reported {
			recovered: Some(recovered),
			..
		}) = reported
		{
			break recovered;
		}
		replacement.poll(&mut daemons, "before the recovery was recorded");
		assert!(
			Instant::now() < recovered_by,
			"the recovery was not recorded within {RECOVERED_BOUND:?} ({reads} status reads, \
			 last {reported:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	eprintln!(
		"smoke: replacement recorded its recovery {recovered:?}, polled {:?} ({reads} status reads)",
		recovered_from.elapsed()
	);
	// Wendelon ADR 0108: the recovered replacement activates on its own, from one view of its
	// cut, and hands over: it closes every ControlOnly part, rewrites its role to Full, closes
	// its owner and exits with the restart-request status; its supervisor (this test) starts it
	// again, Full. The status reports the activation (through a cut no lower than the recorded
	// one) within `ACTIVATED_BOUND`. The ControlOnly process may have ended by then: only
	// another status ends the wait early.
	let activated_from = Instant::now();
	let activated_by = deadline.min(activated_from + ACTIVATED_BOUND);
	let mut reads = 0_u32;
	let (activated_through, activated_gid) = loop {
		reads += 1;
		let reported = replacement_report(
			&format!("replace-activated-{reads}"),
			token,
			lost.id,
			&connection,
			activated_by,
		);
		if let Some(Reported {
			recovered: Some(seen),
			activated: Some(activated),
			..
		}) = reported
		{
			assert_eq!(seen, recovered, "the recorded recovery never changes");
			break activated;
		}
		replacement.poll(&mut daemons, "before the replacement activated");
		assert!(
			Instant::now() < activated_by,
			"the replacement did not activate within {ACTIVATED_BOUND:?} ({reads} status reads, \
			 last {reported:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	assert!(
		activated_through >= recovered.1,
		"the activation's cut {activated_through} is below the recorded one {}",
		recovered.1
	);
	eprintln!(
		"smoke: replacement activated through {activated_through} at gid {activated_gid}, \
		 polled {:?} after the record ({reads} status reads)",
		activated_from.elapsed()
	);
	// The ControlOnly process this test started exits, within its bound, with exactly the
	// restart-request status; any other status, a signal or the bound's end fails.
	// An end kept from before the activation was seen counts; one first observed after the
	// bound does not.
	let exit_by = deadline.min(Instant::now() + HANDOFF_EXIT_BOUND);
	loop {
		replacement.poll(&mut daemons, "before the replacement's handoff exit");
		if replacement.seen.is_some() {
			break;
		}
		assert!(
			Instant::now() < exit_by,
			"the ControlOnly process ({}) did not exit within {HANDOFF_EXIT_BOUND:?} of its \
			 activation being seen",
			replacement.identity
		);
		tokio::time::sleep(Duration::from_millis(50)).await;
	}
	let (status, control_only) = replacement
		.handed_off(exit_by)
		.unwrap_or_else(|why| panic!("{why} (bound {HANDOFF_EXIT_BOUND:?})"));
	assert_eq!(
		wendelon::consensus::stored_raft_role_for_test(&data).unwrap(),
		Some((FIRST_ALLOCATED, false)),
		"the replacement's store is Full on disk once its ControlOnly process exited"
	);
	eprintln!(
		"smoke: the ControlOnly process ({control_only}) exited {status} {:?} after the \
		 activation was seen; started again once, as its supervisor would",
		activated_from.elapsed()
	);
	// This test, as the declared supervisor, starts it once from the same directory, executable
	// and configuration. A new process answers ready as the replacement's own instance and
	// store; a leader hint is never taken as that proof.
	daemons.start(lost);
	let owned = daemons.identity(lost.id);
	assert_ne!(owned, control_only, "the Full member is a new process");
	serving_as(
		lost,
		&pki,
		store,
		deadline,
		(&daemons, &owned),
		"after the handover",
	)
	.await;
	// One LINEARIZABLE table query sent straight to the replacement's address, with no redirect
	// followed and nothing asked again, answers exactly the acknowledged rows (1-8) there.
	let direct_reader = client_through(survivors[0], &pki, Credential::Storage, deadline).await;
	direct_query_exact(
		&direct_reader,
		&descriptor,
		lost.id,
		&expected,
		deadline,
		"after the handover",
	)
	.await;
	// Row 11, written now, is replicated to the Full replacement. One declared wait with the
	// process running, then one stop in order and one stopped-store read within one absolute
	// bound: its vote page names the first allocated instance, Full; the statement saved is the
	// recorded one; every Complete child verifies; the rows read are exactly the acknowledged
	// ones (rows 1-8 and 11), one copy each.
	commit_offering_again(
		&through,
		&descriptor,
		11,
		"full",
		lost.id,
		"after the handover",
	)
	.await;
	// Id 11: the steps below write 9 ("old-back") and 10, so this row never collides.
	expected.push((11, "full".to_owned()));
	let observe_by = deadline.min(Instant::now() + DELTA_WAIT + INSPECTION_BOUND);
	tokio::time::sleep(DELTA_WAIT).await;
	if let Some(why) = daemons.exited() {
		panic!("before the full replacement's store was read: {why}");
	}
	daemons.stop(lost.id, observe_by);
	assert_eq!(
		wendelon::consensus::stored_raft_role_for_test(&data).unwrap(),
		Some((FIRST_ALLOCATED, false)),
		"the replacement's store is Full on disk after its handover"
	);
	let found = inspect_recovered(&lost.config, &descriptor, observe_by).await;
	expect_recovered(
		&found,
		(token, store, recovered),
		&expected,
		"the row written after the handover, held by the full replacement",
	);
	// A Full restart: its start authority is judged before its Raft starts; it answers ready
	// as itself again, a direct query is answered there, and the status still reports the
	// same recovery and activation.
	daemons.start(lost);
	let owned = daemons.identity(lost.id);
	serving_as(
		lost,
		&pki,
		store,
		deadline,
		(&daemons, &owned),
		"after a full restart",
	)
	.await;
	direct_query_exact(
		&direct_reader,
		&descriptor,
		lost.id,
		&expected,
		deadline,
		"after a full restart",
	)
	.await;
	let restarted = replacement_report(
		"replace-status-full-restart",
		token,
		lost.id,
		&connection,
		deadline,
	)
	.expect("a status after the full restart");
	assert_eq!(
		(restarted.recovered, restarted.activated),
		(Some(recovered), Some((activated_through, activated_gid))),
		"the same recovery and activation after the full restart"
	);
	daemons.stop(lost.id, deadline);

	// The old instance returns on its old disk at the slot's address: the replacement's
	// process is stopped in order, and the same configuration but for its data directory starts.
	let text = std::fs::read_to_string(&lost.config).unwrap();
	assert_eq!(text.matches("data_dir = \"data\"\n").count(), 1);
	let old = Member {
		id: lost.id,
		config: node_dir.join("wendelon-old.toml"),
		address: lost.address.clone(),
		ready: lost.ready.clone(),
		log: node_dir.join("old-daemon.log"),
	};
	write_private(
		&old.config,
		text.replace("data_dir = \"data\"\n", "data_dir = \"old-data\"\n")
			.as_bytes(),
	);
	daemons.start(&old);
	tokio::time::sleep(OLD_INSTANCE_RUN).await;
	if let Some(why) = daemons.exited() {
		panic!("with the old instance back: {why}");
	}
	eprintln!(
		"smoke: the old instance runs as {}",
		daemons.identity(old.id)
	);
	// The exclusion itself, at that live process: an authenticated, correctly formed Raft
	// request from a survivor (its own node certificate, as itself) addressed to the old
	// instance is answered; the same request addressed to the replacement is refused, with
	// no answer; and the old instance answers again after it, so the refusal is the target's
	// and not a lost process or route. Each on a fresh connection, each bounded.
	let sender = survivors[0].id;
	let probe = |target_instance: u32| {
		let pki_dir = root.join("pki");
		let address = old.address.clone();
		let node_ca = pki.node_ca.clone();
		let slot = old.id;
		async move {
			tokio::time::timeout(
				PROBE_BOUND,
				wendelon::consensus::raft_network::routed_vote_for_test(
					&pki_dir.join(format!("node-{sender}.crt")),
					&pki_dir.join(format!("node-{sender}.key")),
					&node_ca,
					DOMAIN,
					slot,
					&address,
					sender,
					target_instance,
				),
			)
			.await
		}
	};
	let answered = |what: &str, outcome| match outcome {
		Ok(Ok(Ok(response))) => response,
		other => {
			panic!("{what}: the old instance did not answer a request addressed to it: {other:?}")
		}
	};
	let before = answered("before", probe(lost.id).await);
	assert!(
		!before.vote_granted,
		"a term-0 vote was granted: {before:?}"
	);
	eprintln!(
		"smoke: the old instance answered as itself with its vote {:?}",
		before.vote
	);
	match probe(FIRST_ALLOCATED).await {
		Ok(Err(refused)) => eprintln!(
			"smoke: a request addressed to the replacement was refused at the old instance: {refused}"
		),
		other => panic!(
			"a request addressed to the replacement was not refused at the old instance: {other:?}"
		),
	}
	let after = answered("after", probe(lost.id).await);
	assert!(!after.vote_granted, "a term-0 vote was granted: {after:?}");
	if let Some(why) = daemons.exited() {
		panic!("after the probes: {why}");
	}
	// Wendelon ADR 0105: the old process serves no read. One LINEARIZABLE table query is sent
	// straight to the slot's address, where only the old process runs now (its identity is read
	// above), with no redirect followed and nothing asked again; the same query to a survivor
	// answers OK before and after it, so a refusal is the old process's and not the query's.
	let reader = client_through(survivors[0], &pki, Credential::Storage, deadline).await;
	let direct = |target: u32| {
		let inner = reader.inner().clone();
		let mut meta: wendelon::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
		meta.expected_epoch = inner.epoch();
		meta.tenant_id = TenantId::new(TENANT);
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.physical_descriptor_fingerprint =
			*descriptor.physical_descriptor_fingerprint.as_bytes();
		meta.consistency = wendelon::schema::Consistency::LINEARIZABLE;
		meta.row_identity_projection = wendelon::client::RowIdentityProjection::None.as_u8();
		meta.deadline_remaining_millis = PROBE_BOUND.as_millis() as u64;
		let request = wendelon::client::TableQueryRequest {
			meta,
			descriptor: descriptor.clone(),
			pbc: Vec::new(),
			runtime_filter: None,
			row_identity: wendelon::client::RowIdentityProjection::None,
		};
		async move {
			tokio::time::timeout(
				PROBE_BOUND,
				inner.table_query_at_for_test(
					wendelon::schema::NodeId {
						id: target,
						_reserved: 0,
					},
					request,
				),
			)
			.await
		}
	};
	let survivor_before = direct(survivors[0].id).await;
	assert!(
		matches!(survivor_before, Ok(Ok(true))),
		"a survivor did not answer the direct read: {survivor_before:?}"
	);
	let at_old = direct(lost.id).await;
	assert!(
		!matches!(at_old, Ok(Ok(true))),
		"the old process answered a read OK"
	);
	eprintln!("smoke: a read sent straight to the old process: {at_old:?}");
	let survivor_after = direct(survivors[0].id).await;
	assert!(
		matches!(survivor_after, Ok(Ok(true))),
		"a survivor did not answer the direct read after: {survivor_after:?}"
	);
	if let Some(why) = daemons.exited() {
		panic!("after the read probes: {why}");
	}
	let view = inspect(survivors[0], &pki);
	assert!(
		view.ready,
		"a survivor is not ready with the old instance back: {}",
		view.raw
	);
	let named = view
		.leader()
		.unwrap_or_else(|| panic!("a survivor names no leader: {}", view.raw));
	assert!(
		survivors.iter().any(|member| member.id == named),
		"with the old instance back, the leader is {named}"
	);
	commit_offering_again(
		&through,
		&descriptor,
		9,
		"old-back",
		lost.id,
		"with the old instance",
	)
	.await;
	expected.push((9, "old-back".to_owned()));
	expect_at_survivors(
		&survivors,
		&pki,
		&descriptor,
		deadline,
		&expected,
		"with the old instance back",
	)
	.await;
	let old_view = inspect(&old, &pki);
	eprintln!(
		"smoke: RECORD: the old instance's ready answer: {} ({})",
		old_view.ready, old_view.raw
	);
	daemons.stop(old.id, deadline);
	let refused = std::fs::read_to_string(&old.log)
		.map(|log| {
			log.matches("it is addressed to another instance of this slot")
				.count()
		})
		.unwrap_or(0);
	eprintln!(
		"smoke: RECORD: requests addressed to another instance refused by the old process: \
		 {refused}"
	);

	// The replacement's process returns from its unchanged file: still done; writes commit.
	daemons.start(lost);
	tokio::time::sleep(Duration::from_secs(2)).await;
	assert_eq!(
		replacement_status(
			"replace-status-returned",
			token,
			lost.id,
			&connection,
			deadline
		),
		Some(ReportedPhase::Done(FIRST_ALLOCATED))
	);
	commit_offering_again(
		&through,
		&descriptor,
		10,
		"returned",
		lost.id,
		"after the return",
	)
	.await;
	expected.push((10, "returned".to_owned()));
	expect_at_survivors(
		&survivors,
		&pki,
		&descriptor,
		deadline,
		&expected,
		"after the replacement's return",
	)
	.await;

	// Orderly stop of every process. A forced kill fails the test.
	daemons.stop(lost.id, deadline);
	for member in &survivors {
		daemons.stop(member.id, deadline);
	}
	eprintln!(
		"smoke: replacement workflow passed in {:?}",
		started.elapsed()
	);
	assert!(Instant::now() < deadline, "the test overran its bound");
	std::fs::remove_dir_all(&root).unwrap();
}

/// Three hosts: the hosted replacement workflow's whole bound (Codex 01a1e48d: 900 s, in a
/// 1080 s test unit, with node units of 1200 s).
const REPLACEMENT_HOSTED_TEST_BOUND: Duration = Duration::from_secs(900);
/// Three hosts: the consensus timing base this workflow requires to be declared and written on
/// every node (heartbeat 250, election 750-1500, snapshot chunk 1000): the profile of the earlier
/// three-host runs. It is not a product default.
const REPLACEMENT_HOSTED_BASE_MILLIS: u64 = 250;

/// The run's whole object listing within `by`, or the step fails: never partial.
async fn listed(objects: &Arc<dyn ObjectStore>, by: Instant, what: &str) -> Vec<(String, u64)> {
	let bound = by.saturating_duration_since(Instant::now());
	let mut listed: Vec<(String, u64)> = tokio::time::timeout(bound, objects.list(""))
		.await
		.unwrap_or_else(|_| panic!("{what}: the object store did not list within its bound"))
		.unwrap_or_else(|error| panic!("{what}: listing the run's prefix: {error}"))
		.into_iter()
		.map(|object| (object.key, object.size_bytes))
		.collect();
	assert!(
		Instant::now() <= by,
		"{what}: the listing came after its bound"
	);
	listed.sort();
	listed
}

/// The service-restart workflow: one end of the service's main process, as the static
/// `ExecStopPost` recorder (`tests/service_fixtures/exit_record`) wrote it from the variables
/// systemd sets: `result=<SERVICE_RESULT> code=<EXIT_CODE> status=<EXIT_STATUS>`.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ServiceEnd {
	result: String,
	code: String,
	status: String,
}

impl ServiceEnd {
	/// The handoff's restart request exactly: the main process exited (not killed, not dumped,
	/// not timed out) with `ACTIVATION_RESTART_EXIT`, and systemd judged it an exit-code failure.
	fn is_restart_request(&self) -> bool {
		self.result == "exit-code"
			&& self.code == "exited"
			&& self.status == ACTIVATION_RESTART_EXIT.to_string()
	}

	/// An orderly end: exited 0, judged a success.
	fn is_orderly(&self) -> bool {
		self.result == "success" && self.code == "exited" && self.status == "0"
	}
}

/// Every line of the recorder's file, whole or refused: each exactly the three fields in their
/// order, each value nonempty and without spaces; a missing file is no end yet.
fn parse_service_ends(text: &str) -> Result<Vec<ServiceEnd>, String> {
	if !text.is_empty() && !text.ends_with('\n') {
		return Err(format!("the record does not end a line: {text:?}"));
	}
	text.lines()
		.map(|line| {
			let fields: Vec<&str> = line.split(' ').collect();
			let [result, code, status] = fields.as_slice() else {
				return Err(format!("not three fields: {line:?}"));
			};
			let value = |field: &str, key: &str| {
				field
					.strip_prefix(key)
					.filter(|value| !value.is_empty())
					.map(str::to_owned)
					.ok_or_else(|| format!("no {key}<value>: {line:?}"))
			};
			Ok(ServiceEnd {
				result: value(result, "result=")?,
				code: value(code, "code=")?,
				status: value(status, "status=")?,
			})
		})
		.collect()
}

/// One `systemctl --user show` of the service, whole or refused.
#[derive(Clone, Debug, PartialEq, Eq)]
struct ServiceState {
	load: String,
	active: String,
	restarts: u32,
	/// `pid <ExecMainPID> started <ExecMainStartTimestampMonotonic>`, or `None` with no main
	/// process (pid 0).
	identity: Option<String>,
}

fn parse_service_state(text: &str) -> Result<ServiceState, String> {
	let mut fields = std::collections::BTreeMap::new();
	for line in text.lines() {
		let (key, value) = line
			.split_once('=')
			.ok_or_else(|| format!("not key=value: {line:?}"))?;
		if fields.insert(key, value).is_some() {
			return Err(format!("{key} twice: {text:?}"));
		}
	}
	let wanted = [
		"ActiveState",
		"ExecMainPID",
		"ExecMainStartTimestampMonotonic",
		"LoadState",
		"NRestarts",
	];
	if fields.keys().copied().collect::<Vec<_>>() != wanted {
		return Err(format!("not exactly the fields {wanted:?}: {text:?}"));
	}
	let number = |key: &str| {
		let value = fields[key];
		(!value.is_empty() && value.bytes().all(|byte| byte.is_ascii_digit()))
			.then(|| value.parse::<u64>().ok())
			.flatten()
			.ok_or_else(|| format!("{key} is not a number: {text:?}"))
	};
	let pid = number("ExecMainPID")?;
	let started = number("ExecMainStartTimestampMonotonic")?;
	let restarts = u32::try_from(number("NRestarts")?).map_err(|error| error.to_string())?;
	let identity = match (pid, started) {
		(0, _) => None,
		(_, 0) => return Err(format!("a main process with no start time: {text:?}")),
		(pid, started) => Some(format!("pid {pid} started {started}")),
	};
	Ok(ServiceState {
		load: fields["LoadState"].to_owned(),
		active: fields["ActiveState"].to_owned(),
		restarts,
		identity,
	})
}

/// One node run by the user's service manager as the operator's guide documents it ("Running
/// under systemd": `KillMode=control-group`, `TimeoutStopSec=100`, `Restart=on-failure`, the
/// restart delay and start limit at their defaults). Beyond that unit, and changing none of its
/// restart behavior: `BindsTo=` and `After=` the run's owner unit (Codex 01a1e552 R1), so the
/// owner's end, expected or not, stops it (a stop, never a restart); the static recorder as
/// `ExecStopPost`, which writes one line per end of the main process; the node's output; the
/// test's `TMPDIR`. The record and the output are files in the run's record directory
/// (`PD_SERVICE_RECORD_DIR`, no keys), so the launcher seals them whatever the result. Dropped,
/// a unit still loaded is stopped and reset.
struct ServiceNode {
	unit: String,
	record: PathBuf,
}

/// How long one `systemctl` or `systemd-run` call may take.
const SERVICE_CALL_BOUND: Duration = Duration::from_secs(15);
/// After the explicit stop, how long the service is watched for a restart: 20 times the
/// default restart delay of 100 ms.
const SERVICE_HOLD: Duration = Duration::from_secs(2);

impl ServiceNode {
	fn start(member: &Member, owner: &str, records: &Path) -> Self {
		let unit = format!("{owner}-n{}", member.id);
		let dir = member.config.parent().unwrap();
		let record = records.join(format!("n{}.ends", member.id));
		let output = records.join(format!("n{}.log", member.id));
		let recorder =
			PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/service_fixtures/exit_record");
		let property = |text: String| std::ffi::OsString::from(text);
		let mut arguments: Vec<std::ffi::OsString> = vec![
			"--user".into(),
			"--quiet".into(),
			"--unit".into(),
			unit.clone().into(),
			"-p".into(),
			"KillMode=control-group".into(),
			"-p".into(),
			"TimeoutStopSec=100".into(),
			"-p".into(),
			"Restart=on-failure".into(),
			"-p".into(),
			property(format!("BindsTo={owner}.service")),
			"-p".into(),
			property(format!("After={owner}.service")),
			"-p".into(),
			property(format!("StandardOutput=append:{}", output.display())),
			"-p".into(),
			property(format!("StandardError=append:{}", output.display())),
			"-p".into(),
			property(format!(
				"ExecStopPost={} {}",
				recorder.display(),
				record.display()
			)),
			property(format!("--working-directory={}", dir.display())),
			property(format!(
				"--setenv=TMPDIR={}",
				std::env::temp_dir().display()
			)),
			"--setenv=RUST_BACKTRACE=1".into(),
			"--".into(),
		];
		arguments.push(nilket_bin().into_os_string());
		arguments.push("wendelon".into());
		arguments.push(member.config.clone().into_os_string());
		let ran = bounded_run(
			Path::new("systemd-run"),
			&arguments,
			Instant::now() + SERVICE_CALL_BOUND,
		);
		assert!(ran.succeeded(), "start the service {unit}: {}", ran.text());
		Self { unit, record }
	}

	fn state(&self) -> ServiceState {
		self.try_state()
			.unwrap_or_else(|why| panic!("show {}: {why}", self.unit))
	}

	fn try_state(&self) -> Result<ServiceState, String> {
		let ran = bounded_run(
			Path::new("systemctl"),
			&[
				"--user".into(),
				"show".into(),
				self.unit.clone().into(),
				"-p".into(),
				"LoadState".into(),
				"-p".into(),
				"ActiveState".into(),
				"-p".into(),
				"NRestarts".into(),
				"-p".into(),
				"ExecMainPID".into(),
				"-p".into(),
				"ExecMainStartTimestampMonotonic".into(),
			],
			Instant::now() + SERVICE_CALL_BOUND,
		);
		if !ran.succeeded() {
			return Err(ran.text());
		}
		parse_service_state(&ran.stdout)
	}

	fn ends(&self) -> Vec<ServiceEnd> {
		let text = match std::fs::read_to_string(&self.record) {
			Ok(text) => text,
			Err(error) if error.kind() == std::io::ErrorKind::NotFound => String::new(),
			Err(error) => panic!("the service's end record: {error}"),
		};
		parse_service_ends(&text).unwrap_or_else(|why| panic!("the service's end record: {why}"))
	}

	/// The running execution's identity, from a state with a main process.
	fn running(&self, what: &str) -> (ServiceState, String) {
		let state = self.state();
		match (&state.active[..], &state.identity) {
			("active", Some(identity)) => {
				let identity = identity.clone();
				(state, identity)
			}
			_ => panic!(
				"{what}: the service {} is not running: {state:?}",
				self.unit
			),
		}
	}

	/// Until its handoff: the first execution is still the one running and has ended no way,
	/// or it has ended exactly once with the restart request. Anything else fails.
	/// The state is read BEFORE the record: the recorder writes an end before systemd restarts
	/// the unit, so a restart seen in the state is always matched by a recorded end.
	fn hold_first(&self, first: &str, what: &str) {
		let state = self.state();
		let ends = self.ends();
		match ends.as_slice() {
			[] => {
				assert!(
					state.restarts == 0 && state.identity.as_deref() == Some(first),
					"{what}: the first execution ({first}) is not the one running and nothing \
					 ended: {state:?}"
				);
			}
			[end] => assert!(
				end.is_restart_request(),
				"{what}: the first execution ({first}) ended {end:?}, not with the restart \
				 request"
			),
			more => panic!("{what}: the service ended more than once: {more:?}"),
		}
	}

	/// The explicit stop, within `by`: `systemctl --user stop` returns success.
	fn stop(&self, by: Instant) {
		let ran = bounded_run(
			Path::new("systemctl"),
			&["--user".into(), "stop".into(), self.unit.clone().into()],
			by,
		);
		assert!(ran.succeeded(), "stop {}: {}", self.unit, ran.text());
	}
}

impl Drop for ServiceNode {
	fn drop(&mut self) {
		// A transient unit that ended in order is already gone: nothing to stop.
		if let Ok(state) = self.try_state()
			&& state.load == "not-found"
		{
			return;
		}
		for verb in ["stop", "reset-failed"] {
			let ran = bounded_run(
				Path::new("systemctl"),
				&["--user".into(), verb.into(), self.unit.clone().into()],
				Instant::now() + SERVICE_CALL_BOUND + STOP_BOUND,
			);
			if verb == "stop" && !ran.succeeded() {
				eprintln!(
					"smoke: CLEANUP of {} NOT CONFIRMED: {}",
					self.unit,
					ran.text()
				);
			}
		}
	}
}

/// Codex 01a1e552 R2: one observation of the service after the first execution's restart
/// request, made at `observed_at`, judged against the ONE absolute restart-to-ready end `by`.
/// `Ok(Some(identity))` is the restarted execution: active, with a main process that is not
/// `first`, exactly one restart, observed within `by`. `Ok(None)` is not yet (not active),
/// within `by`. Anything observed after `by`, or active as the first execution or after more
/// than one restart, is refused.
fn restarted_execution(
	state: &ServiceState,
	first: &str,
	observed_at: Instant,
	by: Instant,
) -> Result<Option<String>, String> {
	let active = match (&state.active[..], &state.identity) {
		("active", Some(identity)) => Some(identity),
		_ => None,
	};
	match accepted_in_time(active.is_some(), observed_at, by) {
		Err(late) => Err(format!("the restarted execution {late}: {state:?}")),
		Ok(false) => Ok(None),
		Ok(true) => {
			let identity = active.unwrap();
			if identity == first {
				return Err(format!(
					"active as the first execution ({first}): {state:?}"
				));
			}
			if state.restarts != 1 {
				return Err(format!("not exactly one automatic restart: {state:?}"));
			}
			Ok(Some(identity.clone()))
		}
	}
}

#[test]
fn a_restarted_execution_is_accepted_only_new_once_restarted_and_in_time() {
	let by = Instant::now() + Duration::from_secs(60);
	let state = |active: &str, restarts: u32, identity: Option<&str>| ServiceState {
		load: "loaded".to_owned(),
		active: active.to_owned(),
		restarts,
		identity: identity.map(str::to_owned),
	};
	let first = "pid 1 started 10";
	let second = "pid 2 started 20";
	let early = by - Duration::from_secs(1);
	let late = by + Duration::from_millis(1);
	assert_eq!(
		restarted_execution(&state("active", 1, Some(second)), first, early, by),
		Ok(Some(second.to_owned()))
	);
	assert_eq!(
		restarted_execution(&state("activating", 0, None), first, early, by),
		Ok(None)
	);
	assert_eq!(
		restarted_execution(&state("deactivating", 0, Some(first)), first, early, by),
		Ok(None)
	);
	for (refused, at) in [
		(state("active", 1, Some(second)), late),
		(state("activating", 0, None), late),
		(state("active", 0, Some(first)), early),
		(state("active", 1, Some(first)), early),
		(state("active", 2, Some(second)), early),
		(state("active", 0, Some(second)), early),
	] {
		assert!(
			restarted_execution(&refused, first, at, by).is_err(),
			"{refused:?}"
		);
	}
}

/// The service-restart records are read whole or refused, and only an exit with exactly the
/// restart-request status, judged an exit-code failure, is the handoff's restart request.
#[test]
fn a_service_end_is_read_whole_and_only_an_exit_75_is_the_restart_request() {
	let ends = parse_service_ends(
		"result=exit-code code=exited status=75\nresult=success code=exited status=0\n",
	)
	.unwrap();
	assert!(ends[0].is_restart_request() && !ends[0].is_orderly());
	assert!(ends[1].is_orderly() && !ends[1].is_restart_request());
	assert_eq!(parse_service_ends(""), Ok(Vec::new()));
	for refused in [
		"result=exit-code code=exited status=75",
		"result=exit-code code=exited\n",
		"result=exit-code code=exited status=75 extra=1\n",
		"result= code=exited status=75\n",
		"code=exited result=exit-code status=75\n",
		"result=exit-code  code=exited status=75\n",
	] {
		assert!(parse_service_ends(refused).is_err(), "{refused:?}");
	}
	for (line, request) in [
		("result=exit-code code=exited status=75", true),
		("result=exit-code code=exited status=1", false),
		("result=exit-code code=exited status=750", false),
		("result=timeout code=exited status=75", false),
		("result=signal code=killed status=TERM", false),
		("result=core-dump code=dumped status=ABRT", false),
		("result=exit-code code=killed status=75", false),
	] {
		let end = parse_service_ends(&format!("{line}\n")).unwrap();
		assert_eq!(end[0].is_restart_request(), request, "{line}");
	}
	assert!(
		!parse_service_ends("result=success code=killed status=TERM\n").unwrap()[0].is_orderly()
	);
	let shown = "LoadState=loaded\nActiveState=active\nNRestarts=1\nExecMainPID=42\n\
		ExecMainStartTimestampMonotonic=1000\n";
	assert_eq!(
		parse_service_state(shown),
		Ok(ServiceState {
			load: "loaded".to_owned(),
			active: "active".to_owned(),
			restarts: 1,
			identity: Some("pid 42 started 1000".to_owned()),
		})
	);
	let gone = "LoadState=not-found\nActiveState=inactive\nNRestarts=0\nExecMainPID=0\n\
		ExecMainStartTimestampMonotonic=0\n";
	assert_eq!(parse_service_state(gone).unwrap().identity, None);
	for refused in [
		"LoadState=loaded\nActiveState=active\nNRestarts=1\nExecMainPID=42\n",
		"LoadState=loaded\nActiveState=active\nNRestarts=-1\nExecMainPID=42\n\
		 ExecMainStartTimestampMonotonic=1000\n",
		"LoadState=loaded\nActiveState=active\nNRestarts=1\nExecMainPID=42\n\
		 ExecMainStartTimestampMonotonic=0\n",
		"LoadState=loaded\nLoadState=loaded\nActiveState=active\nNRestarts=1\nExecMainPID=42\n\
		 ExecMainStartTimestampMonotonic=1000\n",
	] {
		assert!(parse_service_state(refused).is_err(), "{refused:?}");
	}
}

/// The service manager restarts an activated replacement as a full member, on one host (Codex
/// 01a1e541, 01a1e547). The cluster forms and a follower loses its disk as in
/// `three_daemons_replace_a_lost_follower_with_a_control_only_instance` (its blank-store refusal
/// is not repeated); the replacement is begun and provisioned through the operator's commands,
/// and then started not by this test but as a user service with the documented unit
/// (`ServiceNode`). This test starts it once and never again:
///
/// - Precondition: the first execution's identity (main pid and start time) is read at its
///   start, with no restart and no end recorded (a restart request before that read is not
///   covered). Until its handoff it stays that execution, or has ended exactly once with the
///   restart request.
/// - The replacement is done, its store promoted with the configured key bound, its recovery
///   recorded and its activation reported (each through `replace status`, replicated status).
/// - Its first execution ends with exactly the restart request: one end recorded by the
///   service's own `ExecStopPost` (`result=exit-code code=exited status=75`), within
///   `HANDOFF_EXIT_BOUND` of the activation being seen.
/// - The service manager starts it again by itself, all within ONE `SERVING_BOUND` from the
///   recorded end (each observation judged when it completed): the service is active with
///   exactly one restart and a new execution (main pid and start time differ from the first's),
///   and that execution, read again after the answer, answers ready as the first allocated
///   instance serving the promoted store. One LINEARIZABLE query sent straight to it, with no
///   redirect followed and nothing asked again, answers exactly every acknowledged row (logical
///   rows, not physical uniqueness).
/// - Stopped explicitly (`systemctl --user stop`) within `STOP_BOUND`: its second end is
///   recorded as orderly (`result=success code=exited status=0`). Sampled every 100 ms for
///   `SERVICE_HOLD` after the stop returned, and once more at its end, the service is neither
///   active nor activating, and no third end is recorded: no start was seen after the stop
///   (sampled checks over a fixed hold, not a continuous observation of every possible start).
/// - The survivors stop in order with status 0, and the whole workflow ends within
///   `REPLACEMENT_TEST_BOUND`.
///
/// Not claimed: a stop that ends with a nonzero status (this stop exits 0, which
/// `Restart=on-failure` would not restart anyway, so the documented "an explicit stop is never
/// restarted, whatever the status" is not exercised); a stop during the handoff; the start rate
/// limit; a system (not user) service manager; more than one host; an object store; any time as
/// a measurement; release readiness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "spawns three daemons, one as a user service; run alone under the service launcher's owner unit (PD_OWNER_UNIT)"]
async fn a_service_manager_restarts_an_activated_replacement_as_full() {
	assert!(
		node_control().is_none(),
		"this workflow runs its three daemons on one host"
	);
	let owner = std::env::var("PD_OWNER_UNIT").expect("PD_OWNER_UNIT names the run's owner unit");
	assert!(
		owner.starts_with("pd-")
			&& owner
				.bytes()
				.all(|byte| byte.is_ascii_alphanumeric() || byte == b'-'),
		"the owner unit {owner:?} is not a run-owned name"
	);
	let records = PathBuf::from(
		std::env::var_os("PD_SERVICE_RECORD_DIR")
			.expect("PD_SERVICE_RECORD_DIR names the run's record directory"),
	);
	assert!(records.is_dir(), "{} is not a directory", records.display());
	let deadline = Instant::now() + REPLACEMENT_TEST_BOUND;
	let Formed {
		root,
		pki,
		members,
		mut daemons,
		descriptor,
		decision_keys,
	} = form(deadline, None).await;
	let storage = client_through(&members[0], &pki, Credential::Storage, deadline).await;
	let mut expected = Vec::new();
	for id in 1_i64..=4 {
		let bound = step(deadline);
		tokio::time::timeout(
			bound,
			storage.table_write(
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
		.unwrap_or_else(|_| panic!("the write of row {id} had no answer within its bound"))
		.unwrap_or_else(|error| panic!("the write of row {id}: {error:?}"));
		expected.push((id, "before".to_owned()));
	}

	// A follower, chosen positively from node 1's view of the leader, loses its disk.
	let inspection = inspect(&members[0], &pki);
	assert!(inspection.ready, "node 1 is not ready: {}", inspection.raw);
	let leader = inspection
		.leader()
		.unwrap_or_else(|| panic!("node 1 names no leader among the three: {}", inspection.raw));
	let lost = members
		.iter()
		.rev()
		.find(|member| member.id != leader)
		.expect("a node that is not the leader");
	let survivors: Vec<&Member> = members
		.iter()
		.filter(|member| member.id != lost.id)
		.collect();
	eprintln!("smoke: leader {leader}; node {} loses its disk", lost.id);
	eprintln!("smoke: {}", daemons.crash(lost.id));
	daemons.still_down(lost.id);
	let node_dir = lost.config.parent().unwrap().to_path_buf();
	let data = node_dir.join("data");
	let old_data = node_dir.join("old-data");
	std::fs::rename(&data, &old_data).unwrap();
	std::fs::create_dir(&data).unwrap();
	// Enrolled under the cluster's root as a joiner is (ADR 0726), so it can recover.
	let sealed_root = Path::new(".wendelon").join("local-sealed-root-kek.bin");
	let sealed = std::fs::read(old_data.join(&sealed_root)).unwrap();
	std::fs::create_dir_all(data.join(".wendelon")).unwrap();
	write_private(&data.join(&sealed_root), &sealed);

	// The operator opens the replacement through a survivor and provisions its token.
	let connection = operator_connection(survivors[0], &pki);
	let mut arguments: Vec<std::ffi::OsString> =
		vec!["begin".into(), "--slot".into(), lost.id.to_string().into()];
	arguments.extend(connection.iter().cloned());
	let began = replace_command("replace-begin", &arguments, deadline);
	assert!(began.succeeded(), "replace begin: {}", began.text());
	let first_line = began.stdout.lines().next().unwrap_or_default();
	let token = announced_token(first_line, lost.id)
		.unwrap_or_else(|| panic!("replace begin announced no token first: {}", began.text()));
	let provisioned = replace_command(
		"replace-provision",
		&[
			"provision".into(),
			"--data-dir".into(),
			data.clone().into_os_string(),
			"--token".into(),
			format!("{token:032x}").into(),
		],
		deadline,
	);
	assert!(
		provisioned.succeeded(),
		"replace provision: {}",
		provisioned.text()
	);

	// The service manager runs it from here on.
	let service = ServiceNode::start(lost, &owner, &records);
	let (state, first) = service.running("at its start");
	assert_eq!(
		(state.restarts, service.ends()),
		(0, Vec::new()),
		"the first execution was read before any restart or end"
	);
	eprintln!(
		"smoke: service {} runs the replacement's first execution ({first})",
		service.unit
	);
	let hold = |daemons: &mut Daemons, what: &str| {
		if let Some(why) = daemons.exited() {
			panic!("{what}: {why}");
		}
		service.hold_first(&first, what);
	};
	let done_by = deadline.min(Instant::now() + DONE_BOUND);
	let mut reads = 0_u32;
	loop {
		reads += 1;
		let phase = replacement_status(
			&format!("replace-status-{reads}"),
			token,
			lost.id,
			&connection,
			done_by,
		);
		if phase == Some(ReportedPhase::Done(FIRST_ALLOCATED)) {
			break;
		}
		hold(&mut daemons, "before done");
		assert!(
			Instant::now() < done_by,
			"the replacement was not done within {DONE_BOUND:?} ({reads} status reads, last \
			 {phase:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	}
	eprintln!("smoke: replacement done as instance {FIRST_ALLOCATED} ({reads} status reads)");
	let configured = decision_keys[NODES.iter().position(|id| *id == lost.id).unwrap()];
	let mut reads = 0_u32;
	let reported_by =
		deadline.min(Instant::now() + PREPARED_BOUND + RECOVERED_BOUND + ACTIVATED_BOUND);
	let (store, recovered, (activated_through, activated_gid)) = loop {
		reads += 1;
		let reported = replacement_report(
			&format!("replace-activated-{reads}"),
			token,
			lost.id,
			&connection,
			reported_by,
		);
		if let Some(Reported {
			phase: ReportedPhase::Done(FIRST_ALLOCATED),
			promoted: Some(store),
			bound_key: Some(key),
			recovered: Some(recovered),
			activated: Some(activated),
			..
		}) = reported
		{
			assert_eq!(
				key, configured,
				"the bound key is the slot's configured one"
			);
			break (store, recovered, activated);
		}
		hold(&mut daemons, "before the replacement activated");
		assert!(
			Instant::now() < reported_by,
			"the replacement was not promoted, recovered and activated within its bounds \
			 ({reads} status reads, last {reported:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	assert!(
		activated_through >= recovered.1,
		"the activation's cut {activated_through} is below the recorded one {}",
		recovered.1
	);
	eprintln!(
		"smoke: replacement promoted store {store:032x}, recovered {recovered:?}, activated \
		 through {activated_through} at gid {activated_gid} ({reads} status reads)"
	);

	// The first execution ends with exactly the restart request, recorded by the service.
	let exit_by = deadline.min(Instant::now() + HANDOFF_EXIT_BOUND);
	loop {
		hold(&mut daemons, "before the handoff exit");
		let ends = service.ends();
		let observed_at = Instant::now();
		if let [end] = ends.as_slice() {
			assert!(
				end.is_restart_request(),
				"the first execution ended {end:?}"
			);
			assert!(
				accepted_in_time(true, observed_at, exit_by).is_ok(),
				"the restart request was observed only after its bound {HANDOFF_EXIT_BOUND:?}"
			);
			eprintln!("smoke: the first execution ({first}) ended {end:?}");
			break;
		}
		assert!(
			Instant::now() < exit_by,
			"the first execution ({first}) did not end within {HANDOFF_EXIT_BOUND:?} of the \
			 activation being seen"
		);
		tokio::time::sleep(Duration::from_millis(200)).await;
	}

	// The service manager started it again by itself: one restart, a new execution, ready as
	// the replacement serving its store, and exact at it.
	// ONE absolute restart-to-ready end (Codex 01a1e552 R2), the workflow's end its cap.
	let restarted_by = deadline.min(Instant::now() + SERVING_BOUND);
	let second = loop {
		let state = service.state();
		let observed_at = Instant::now();
		match restarted_execution(&state, &first, observed_at, restarted_by) {
			Ok(Some(identity)) => break identity,
			Ok(None) => {}
			Err(why) => panic!("the service's restart within {SERVING_BOUND:?}: {why}"),
		}
		tokio::time::sleep(Duration::from_millis(200)).await;
	};
	assert_eq!(
		service.ends().len(),
		1,
		"one end so far, the first execution's"
	);
	eprintln!("smoke: the service manager started it again by itself as {second} (restarts 1)");
	serving_as_with(
		lost,
		&pki,
		store,
		restarted_by,
		(&|| service.running("after the restart").1, &second),
		"after the service's restart",
	)
	.await;
	let direct_reader = client_through(survivors[0], &pki, Credential::Storage, deadline).await;
	direct_query_exact(
		&direct_reader,
		&descriptor,
		lost.id,
		&expected,
		deadline,
		"after the service's restart",
	)
	.await;
	let (state, still) = service.running("before the explicit stop");
	assert_eq!(
		(state.restarts, still.as_str()),
		(1, second.as_str()),
		"the same restarted execution answered"
	);

	// The explicit stop: an orderly end, and no start after it.
	let stop_by = deadline.min(Instant::now() + STOP_BOUND);
	service.stop(stop_by);
	let stopped_at = Instant::now();
	let ends = service.ends();
	assert!(
		ends.len() == 2 && ends[1].is_orderly(),
		"the explicit stop ended the restarted execution in order: {ends:?}"
	);
	while stopped_at.elapsed() < SERVICE_HOLD {
		let state = service.state();
		assert!(
			state.active != "active" && state.active != "activating",
			"the service started again after an explicit stop: {state:?}"
		);
		tokio::time::sleep(Duration::from_millis(100)).await;
	}
	let last = service.state();
	assert!(
		last.active != "active" && last.active != "activating",
		"the service started again after an explicit stop: {last:?}"
	);
	let ends = service.ends();
	assert_eq!(ends.len(), 2, "no end after the explicit stop's: {ends:?}");
	eprintln!(
		"smoke: stopped explicitly: ended {:?}; no start seen in {SERVICE_HOLD:?} of samples, \
		 last {last:?}",
		ends[1]
	);
	drop(service);
	let stops_by = deadline.min(Instant::now() + STOP_BOUND * 2);
	for survivor in &survivors {
		daemons.stop(survivor.id, stops_by);
	}
	assert!(
		Instant::now() <= deadline,
		"the workflow outlived its bound {REPLACEMENT_TEST_BOUND:?}"
	);
	eprintln!("smoke: the service-restart workflow passed");
	// Only after a pass, as the one-host workflow does: a failure keeps the run's directory
	// for its logs (its service records are in the record directory either way).
	std::fs::remove_dir_all(&root).unwrap();
}

/// Three hosts (Codex 01a1e487, 01a1e48d): a replica that has offloaded its cold slabs to the
/// remote object store is lost with its disk, and replaced on its own host. One daemon on each
/// of three hosts (node-control), each tiering directly to the run's own prefix of the
/// development RustFS; the declared consensus base `REPLACEMENT_HOSTED_BASE_MILLIS` and the
/// declared stall bound `QUERY_STALL_BOUND_MILLIS` are written on every node and verified.
///
/// - S1. `ROWS` rows (each a slab of its own) are acknowledged and every node has
///   `OFFLOADED_PER_OWNER` of its own slabs in the store and off its own disk.
/// - S2. A follower (not the leader named by node 1) is killed (SIGKILL, confirmed); its data
///   directory is set aside whole as `old-data` on its host. Its objects (one incarnation, I) are
///   listed.
/// - S3. A blank Full start with the sealed root is refused `InstanceGrantRefused:
///   SlotGranted` (an exit, not a timeout or signal) within `REFUSED_START_BOUND`.
/// - S4. begin through a survivor, provision ON the node's host, start; done as the first
///   allocated instance. The execution started is read at its start (`start` prints it).
/// - Offload observed before the replacement (Codex 01a1e49d): one sequential, sampled listing
///   right before that start (the store, then each node's disk in turn; not an atomic snapshot
///   and not a trace of reads) shows each survivor with at least `OFFLOADED_PER_OWNER` distinct
///   slab ids of its own in the store and off its own disk; the counts are printed as measured.
///   Slab ids are not row identities and how rows pack has varied, so no row or count is claimed
///   to have been read remotely during recovery (a copy may also be promoted after the listing).
///   What is claimed: that offload was observed, and that the replacement then answers exactly
///   the acknowledged rows. With no certificate yet (gid 0), those rows are recovered as
///   accepted children, not as a certified base.
/// - S5. A tail of 4 rows written after done.
/// - S6. Promoted store J and bound key; recovery recorded at (G, K); activation through A >= K.
/// - S7. The ControlOnly execution exits exactly 75 (observed through its unit, compared with the
///   identity read at its start, its observation timed after the read), within
///   `HANDOFF_EXIT_BOUND`; its vote page is Full (read on its host); this test, as the declared
///   supervisor, starts it once; the new execution differs.
/// - S8. Ready as R/J; a direct query to it, no redirect, answers exactly every acknowledged row.
/// - S10. A row written now; after `DELTA_WAIT`, a direct query to the RUNNING replacement, no
///   redirect, answers exactly every acknowledged row, that one included; then it is stopped in
///   order and, on its host, its vote page is Full. The stopped store's rows are NOT read here
///   (Codex 01a1e4f2, RESULT-03/ATTRIBUTION-04): once the Full member has tiered, a stopped
///   store opened without its cluster has no published tier generation and refuses every row
///   read (`Fenced`), by design. So this run has no independent check of the saved statement,
///   of Complete-child verification or of one copy per row on disk; those keep only their
///   one-host scope. The live answer is a logical-row check, not proof of physical uniqueness;
///   the recorded recovery and activation are checked through `replace status` (S6, S11),
///   claims of replicated status, not a read of J's disk.
/// - S11. A Full restart: ready as itself, the direct query exact, the same recovery and activation.
/// - S9. J tiers as a Full member: within `TIER_BOUND`, an object of (node, J) in the prefix whose
///   slab J's data directory holds no file of; the direct query is exact again after it.
/// - S12. The old instance returns on its old disk under the node's unit: a survivor's vote
///   addressed to it is answered, one addressed to the replacement refused, a direct read is not
///   OK; writes commit and reads stay exact. I's objects listed after the loss are all listed
///   right before it starts and right after it stopped (those two endpoints only).
/// - S13. The replacement returns: still done; a write commits; reads exact; every daemon stops
///   in order with status 0.
///
/// Not claimed: a service manager's restart (this test supervises), a leader's loss, a second
/// replacement, reclaim of I's objects, any time as a measurement, release readiness.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "three daemons on three hosts with a remote object store; run alone under the three-host launcher"]
async fn three_hosts_replace_a_lost_offloaded_replica() {
	assert!(
		node_control().is_some(),
		"this workflow runs each daemon on a host of its own (node-control)"
	);
	assert_eq!(
		declared_consensus_base(),
		Some(REPLACEMENT_HOSTED_BASE_MILLIS),
		"this workflow runs at its declared consensus base only"
	);
	let deadline = Instant::now() + REPLACEMENT_HOSTED_TEST_BOUND;
	let started = Instant::now();
	let at = |what: &str| {
		eprintln!(
			"smoke: PHASE {what} at +{} ms",
			started.elapsed().as_millis()
		)
	};
	let store_env = StoreEnv::from_environment();
	let objects = store_env.handle();
	let before = listed(&objects, Instant::now() + CLIENT_BOUND, "the new prefix").await;
	assert!(before.is_empty(), "the run's prefix is not new");
	eprintln!(
		"smoke: HOSTED REPLACEMENT workflow: object prefix {} in bucket {} at {} (direct, no relay); DECLARED consensus base {REPLACEMENT_HOSTED_BASE_MILLIS} ms and stall bound {QUERY_STALL_BOUND_MILLIS} ms on every node; {ROWS} rows, slabs of {SLAB_MAX_BYTES} bytes",
		store_env.prefix, store_env.bucket, store_env.endpoint
	);

	// S1.
	at("formation");
	let mut formed = form(deadline, Some(&store_env)).await;
	let pki = formed.pki.clone();
	let descriptor = formed.descriptor.clone();
	let members: Vec<Member> = formed.members.clone();
	let decision_keys = formed.decision_keys.clone();
	let mut expected: Vec<(i64, String)> = (1..=ROWS)
		.map(|id| {
			let name: String = (0..NAME_HEX_BLOCKS).map(|_| hex(random32())).collect();
			(id, name)
		})
		.collect();
	let client = client_through(&members[0], &pki, Credential::Storage, deadline).await;
	at("writes");
	let mut offers = 0_u32;
	for (id, name) in &expected {
		offers += ingest_the_row(&client, &descriptor, *id, name, deadline).await;
	}
	eprintln!(
		"smoke: all {ROWS} rows acknowledged in {offers} offers ({} beyond one per row)",
		offers - ROWS as u32
	);
	at("offload wait");
	offloaded_by_every_owner(&mut formed, &objects, deadline, "before the loss").await;

	// S2.
	at("loss");
	let inspection = inspect(&members[0], &pki);
	assert!(inspection.ready, "node 1 is not ready: {}", inspection.raw);
	let leader = inspection
		.leader()
		.unwrap_or_else(|| panic!("node 1 names no leader among the three: {}", inspection.raw));
	let lost = members
		.iter()
		.rev()
		.find(|member| member.id != leader)
		.expect("a node that is not the leader");
	let survivors: Vec<&Member> = members
		.iter()
		.filter(|member| member.id != lost.id)
		.collect();
	eprintln!("smoke: leader {leader}; node {} loses its disk", lost.id);
	eprintln!("smoke: {}", formed.daemons.crash(lost.id));
	formed.daemons.still_down(lost.id);
	let after_loss = listed(&objects, deadline, "after the loss").await;
	let lost_incarnations = incarnations_of(&after_loss, lost.id);
	let [old_incarnation] = lost_incarnations.iter().collect::<Vec<_>>()[..] else {
		panic!("the lost node's objects name {lost_incarnations:?}, not one incarnation");
	};
	let old_incarnation = old_incarnation.clone();
	let old_objects: std::collections::BTreeSet<String> =
		keys_of_owner(&after_loss, lost.id, &old_incarnation)
			.into_iter()
			.collect();
	eprintln!(
		"smoke: the lost node's objects: {} of incarnation {old_incarnation}",
		old_objects.len()
	);
	formed.daemons.lose(lost.id, deadline);

	// S3.
	at("blank start");
	let blank_by = deadline.min(Instant::now() + REFUSED_START_BOUND);
	let (output, result, raw) = formed.daemons.run_once(lost.id, blank_by);
	eprintln!("smoke: blank full start: {result:?}; {raw}");
	assert!(
		matches!(result, OneOff::Exit(code) if code != 0),
		"a blank store was not refused as the lost voter (an exit, not a timeout or signal): {result:?}: {output}"
	);
	assert!(
		raw.contains("InstanceGrantRefused: SlotGranted"),
		"the blank start was refused for another reason: {raw}"
	);
	formed.daemons.blank_aside(lost.id, deadline);

	// S4.
	at("replacement");
	let connection = operator_connection(survivors[0], &pki);
	let mut arguments: Vec<std::ffi::OsString> =
		vec!["begin".into(), "--slot".into(), lost.id.to_string().into()];
	arguments.extend(connection.iter().cloned());
	let began = replace_command("replace-begin", &arguments, deadline);
	assert!(began.succeeded(), "replace begin: {}", began.text());
	let lines: Vec<&str> = began.stdout.lines().collect();
	let [announced, report] = lines.as_slice() else {
		panic!(
			"replace begin printed {} lines: {}",
			lines.len(),
			began.text()
		);
	};
	let token = announced_token(announced, lost.id)
		.unwrap_or_else(|| panic!("replace begin announced no token first: {}", began.text()));
	assert_eq!(
		parse_report(report).map(|reported| (reported.token, reported.phase)),
		Some((token, ReportedPhase::Begun)),
		"replace begin: {}",
		began.text()
	);
	eprintln!("smoke: COMMAND BEGIN replace-provision");
	let provision_by = deadline.min(Instant::now() + REPLACE_COMMAND_BOUND);
	let provisioned = formed.daemons.provision(lost.id, token, provision_by);
	eprintln!("smoke: COMMAND OUTCOME replace-provision ok {provisioned}");

	// The cold accounting, observed once right before the replacement starts.
	let cold = Snapshot::take(&formed, &objects, deadline).await;
	cold.report("before the replacement starts");
	let offloaded = cold.offloaded();
	for survivor in &survivors {
		let at_node = NODES.iter().position(|id| *id == survivor.id).unwrap();
		let (off, _) = &offloaded[at_node];
		assert!(
			off.len() >= OFFLOADED_PER_OWNER,
			"survivor {} has only {} of its slabs off its disk before the replacement starts",
			survivor.id,
			off.len()
		);
		eprintln!(
			"smoke: COLD survivor {}: {} of its own slabs in the store and off its disk: {off:?}",
			survivor.id,
			off.len()
		);
	}
	eprintln!(
		"smoke: COLD: offload observed before the replacement in one sequential, sampled listing (distinct slab ids off each survivor's disk, as printed); no row is claimed read remotely"
	);
	let replacement_started = Instant::now();
	let control_identity = formed.daemons.start_identified(lost);
	let mut replacement = ReplacementExit::new(lost.id, control_identity);
	let done_by = deadline.min(Instant::now() + DONE_BOUND);
	let mut reads = 0_u32;
	let done = loop {
		reads += 1;
		let phase = replacement_status(
			&format!("replace-status-{reads}"),
			token,
			lost.id,
			&connection,
			done_by,
		);
		if let Some(ReportedPhase::Done(instance)) = phase {
			break instance;
		}
		replacement.poll(&mut formed.daemons, "before done");
		assert!(
			Instant::now() < done_by,
			"the replacement was not done within {DONE_BOUND:?} ({reads} status reads, last {phase:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	eprintln!(
		"smoke: replacement done as instance {done} {:?} after its start ({reads} status reads)",
		replacement_started.elapsed()
	);
	assert_eq!(done, FIRST_ALLOCATED);

	// S5.
	at("tail");
	let through = client_through(survivors[0], &pki, Credential::Storage, deadline).await;
	for id in ROWS + 1..=ROWS + 4 {
		commit_offering_again(&through, &descriptor, id, "tail", lost.id, "after done").await;
		expected.push((id, "tail".to_owned()));
	}

	// S6.
	at("recovery");
	let configured = decision_keys[NODES.iter().position(|id| *id == lost.id).unwrap()];
	let prepared_by = deadline.min(Instant::now() + PREPARED_BOUND);
	let mut reads = 0_u32;
	let store = loop {
		reads += 1;
		let reported = replacement_report(
			&format!("replace-prepared-{reads}"),
			token,
			lost.id,
			&connection,
			prepared_by,
		);
		if let Some(Reported {
			phase: ReportedPhase::Done(FIRST_ALLOCATED),
			promoted: Some(store),
			bound_key: Some(key),
			..
		}) = reported
		{
			assert_eq!(
				key, configured,
				"the bound key is the slot's configured one"
			);
			break store;
		}
		replacement.poll(&mut formed.daemons, "before the replacement was prepared");
		assert!(
			Instant::now() < prepared_by,
			"the replacement was not prepared within {PREPARED_BOUND:?} ({reads} status reads, last {reported:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	eprintln!("smoke: replacement promoted store {store:032x} and bound its configured key");
	let recovered_by = deadline.min(Instant::now() + RECOVERED_BOUND);
	let mut reads = 0_u32;
	let recovered = loop {
		reads += 1;
		let reported = replacement_report(
			&format!("replace-recovered-{reads}"),
			token,
			lost.id,
			&connection,
			recovered_by,
		);
		if let Some(Reported {
			recovered: Some(recovered),
			..
		}) = reported
		{
			break recovered;
		}
		replacement.poll(&mut formed.daemons, "before the recovery was recorded");
		assert!(
			Instant::now() < recovered_by,
			"the recovery was not recorded within {RECOVERED_BOUND:?} ({reads} status reads, last {reported:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	eprintln!(
		"smoke: replacement recorded its recovery {recovered:?} ({reads} status reads){}",
		if recovered.0 == 0 {
			"; gid 0: the rows were recovered as accepted children, not a certified base"
		} else {
			""
		}
	);
	let activated_from = Instant::now();
	let activated_by = deadline.min(activated_from + ACTIVATED_BOUND);
	let mut reads = 0_u32;
	let (activated_through, activated_gid) = loop {
		reads += 1;
		let reported = replacement_report(
			&format!("replace-activated-{reads}"),
			token,
			lost.id,
			&connection,
			activated_by,
		);
		if let Some(Reported {
			recovered: Some(seen),
			activated: Some(activated),
			..
		}) = reported
		{
			assert_eq!(seen, recovered, "the recorded recovery never changes");
			break activated;
		}
		replacement.poll(&mut formed.daemons, "before the replacement activated");
		assert!(
			Instant::now() < activated_by,
			"the replacement did not activate within {ACTIVATED_BOUND:?} ({reads} status reads, last {reported:?})"
		);
		tokio::time::sleep(Duration::from_secs(1)).await;
	};
	assert!(
		activated_through >= recovered.1,
		"the activation's cut {activated_through} is below the recorded one {}",
		recovered.1
	);
	eprintln!(
		"smoke: replacement activated through {activated_through} at gid {activated_gid} ({reads} status reads)"
	);

	// S7.
	at("handoff");
	let exit_by = deadline.min(Instant::now() + HANDOFF_EXIT_BOUND);
	loop {
		replacement.poll(&mut formed.daemons, "before the replacement's handoff exit");
		if replacement.seen.is_some() {
			break;
		}
		assert!(
			Instant::now() < exit_by,
			"the ControlOnly execution ({}) did not exit within {HANDOFF_EXIT_BOUND:?} of its activation being seen",
			replacement.identity
		);
		tokio::time::sleep(Duration::from_millis(500)).await;
	}
	let (status, control_only) = replacement
		.handed_off(exit_by)
		.unwrap_or_else(|why| panic!("{why} (bound {HANDOFF_EXIT_BOUND:?})"));
	let probe_by = deadline.min(Instant::now() + INSPECTION_BOUND);
	assert_eq!(
		formed.daemons.probe(lost.id, None, probe_by).role,
		Some((FIRST_ALLOCATED, false)),
		"the replacement's store is Full on disk once its ControlOnly execution exited"
	);
	eprintln!(
		"smoke: the ControlOnly execution ({control_only}) exited {status}; its page is Full; started again once, as its supervisor would"
	);
	let owned = formed.daemons.start_identified(lost);
	assert_ne!(owned, control_only, "the Full member is a new execution");

	// S8.
	at("serving");
	serving_as(
		lost,
		&pki,
		store,
		deadline,
		(&formed.daemons, &owned),
		"after the handover",
	)
	.await;
	let direct_reader = client_through(survivors[0], &pki, Credential::Storage, deadline).await;
	direct_query_exact(
		&direct_reader,
		&descriptor,
		lost.id,
		&expected,
		deadline,
		"after the handover",
	)
	.await;

	// S10.
	at("full store");
	commit_offering_again(
		&through,
		&descriptor,
		ROWS + 5,
		"full",
		lost.id,
		"after the handover",
	)
	.await;
	expected.push((ROWS + 5, "full".to_owned()));
	let observe_by = deadline.min(Instant::now() + DELTA_WAIT + INSPECTION_BOUND);
	tokio::time::sleep(DELTA_WAIT).await;
	if let Some(why) = formed.daemons.exited() {
		panic!("before the full replacement's store was read: {why}");
	}
	direct_query_exact(
		&direct_reader,
		&descriptor,
		lost.id,
		&expected,
		observe_by,
		"the running full replacement, with the row written after the handover",
	)
	.await;
	formed.daemons.stop(lost.id, observe_by);
	assert_eq!(
		formed.daemons.probe(lost.id, None, observe_by).role,
		Some((FIRST_ALLOCATED, false)),
		"the replacement's store is Full on disk after its handover"
	);

	// S11.
	at("full restart");
	let owned = formed.daemons.start_identified(lost);
	serving_as(
		lost,
		&pki,
		store,
		deadline,
		(&formed.daemons, &owned),
		"after a full restart",
	)
	.await;
	direct_query_exact(
		&direct_reader,
		&descriptor,
		lost.id,
		&expected,
		deadline,
		"after a full restart",
	)
	.await;
	let restarted = replacement_report(
		"replace-status-full-restart",
		token,
		lost.id,
		&connection,
		deadline,
	)
	.expect("a status after the full restart");
	assert_eq!(
		(restarted.recovered, restarted.activated),
		(Some(recovered), Some((activated_through, activated_gid))),
		"the same recovery and activation after the full restart"
	);

	// S9.
	at("replacement tiers");
	let new_incarnation = format!("{store:032x}");
	let tier_by = deadline.min(Instant::now() + TIER_BOUND);
	let (tiered_key, tiered_slab) = loop {
		if let Some(why) = formed.daemons.exited() {
			panic!("while the replacement tiers: {why}");
		}
		let snapshot = Snapshot::take(&formed, &objects, deadline).await;
		let (_, files) = snapshot
			.files
			.iter()
			.find(|(node, _)| *node == lost.id)
			.expect("the replacement's listing");
		let found = keys_of_owner(&snapshot.objects, lost.id, &new_incarnation)
			.into_iter()
			.filter_map(|key| {
				let slab = owner_and_slab(&key)?.1.to_owned();
				files
					.iter()
					.all(|name| !names_the_slab(name, &slab))
					.then_some((key, slab))
			})
			.next();
		assert!(
			Instant::now() <= tier_by || found.is_none(),
			"the replacement's object was observed only after {TIER_BOUND:?}"
		);
		if let Some(found) = found {
			break found;
		}
		assert!(
			Instant::now() < tier_by,
			"within {TIER_BOUND:?} the replacement (node {}, store {new_incarnation}) had no object of its own whose slab is off its disk",
			lost.id
		);
		tokio::time::sleep(Duration::from_secs(5)).await;
	};
	eprintln!(
		"smoke: TIERED the replacement's own object {tiered_key} (slab {tiered_slab}, no file of it on its disk)"
	);
	direct_query_exact(
		&direct_reader,
		&descriptor,
		lost.id,
		&expected,
		deadline,
		"after the replacement tiered",
	)
	.await;
	formed.daemons.stop(lost.id, deadline);

	// S12.
	at("old instance");
	let before_old = listed(&objects, deadline, "before the old instance returns").await;
	baseline_still_listed(&old_objects, &before_old)
		.unwrap_or_else(|why| panic!("before the old instance returns: {why}"));
	let old_identity = formed.daemons.start_old(lost.id);
	tokio::time::sleep(OLD_INSTANCE_RUN).await;
	if let Some(why) = formed.daemons.exited() {
		panic!("with the old instance back: {why}");
	}
	eprintln!("smoke: the old instance runs as {old_identity}");
	let sender = survivors[0].id;
	let pki_dir = formed.root.join("pki");
	let probe = |target_instance: u32| {
		let (pki_dir, address, node_ca, slot) = (
			pki_dir.clone(),
			lost.address.clone(),
			pki.node_ca.clone(),
			lost.id,
		);
		async move {
			tokio::time::timeout(
				PROBE_BOUND,
				wendelon::consensus::raft_network::routed_vote_for_test(
					&pki_dir.join(format!("node-{sender}.crt")),
					&pki_dir.join(format!("node-{sender}.key")),
					&node_ca,
					DOMAIN,
					slot,
					&address,
					sender,
					target_instance,
				),
			)
			.await
		}
	};
	let answered = |what: &str, outcome| match outcome {
		Ok(Ok(Ok(response))) => response,
		other => {
			panic!("{what}: the old instance did not answer a request addressed to it: {other:?}")
		}
	};
	let before_vote = answered("before", probe(lost.id).await);
	assert!(
		!before_vote.vote_granted,
		"a term-0 vote was granted: {before_vote:?}"
	);
	eprintln!(
		"smoke: the old instance answered as itself with its vote {:?}",
		before_vote.vote
	);
	match probe(FIRST_ALLOCATED).await {
		Ok(Err(refused)) => eprintln!(
			"smoke: a request addressed to the replacement was refused at the old instance: {refused}"
		),
		other => panic!(
			"a request addressed to the replacement was not refused at the old instance: {other:?}"
		),
	}
	let after_vote = answered("after", probe(lost.id).await);
	assert!(
		!after_vote.vote_granted,
		"a term-0 vote was granted: {after_vote:?}"
	);
	let reader = client_through(survivors[0], &pki, Credential::Storage, deadline).await;
	let direct = |target: u32| {
		let inner = reader.inner().clone();
		let mut meta: wendelon::schema::TableQueryRequestMeta = bytemuck::Zeroable::zeroed();
		meta.expected_epoch = inner.epoch();
		meta.tenant_id = TenantId::new(TENANT);
		meta.table_object_id = descriptor.table_object_id;
		meta.schema_generation = descriptor.schema_generation;
		meta.physical_descriptor_fingerprint =
			*descriptor.physical_descriptor_fingerprint.as_bytes();
		meta.consistency = wendelon::schema::Consistency::LINEARIZABLE;
		meta.row_identity_projection = wendelon::client::RowIdentityProjection::None.as_u8();
		meta.deadline_remaining_millis = PROBE_BOUND.as_millis() as u64;
		let request = wendelon::client::TableQueryRequest {
			meta,
			descriptor: descriptor.clone(),
			pbc: Vec::new(),
			runtime_filter: None,
			row_identity: wendelon::client::RowIdentityProjection::None,
		};
		async move {
			tokio::time::timeout(
				PROBE_BOUND,
				inner.table_query_at_for_test(
					wendelon::schema::NodeId {
						id: target,
						_reserved: 0,
					},
					request,
				),
			)
			.await
		}
	};
	let survivor_before = direct(survivors[0].id).await;
	assert!(
		matches!(survivor_before, Ok(Ok(true))),
		"a survivor did not answer the direct read: {survivor_before:?}"
	);
	let at_old = direct(lost.id).await;
	assert!(
		!matches!(at_old, Ok(Ok(true))),
		"the old process answered a read OK"
	);
	eprintln!("smoke: a read sent straight to the old process: {at_old:?}");
	let survivor_after = direct(survivors[0].id).await;
	assert!(
		matches!(survivor_after, Ok(Ok(true))),
		"a survivor did not answer the direct read after: {survivor_after:?}"
	);
	commit_offering_again(
		&through,
		&descriptor,
		ROWS + 6,
		"old-back",
		lost.id,
		"with the old instance",
	)
	.await;
	expected.push((ROWS + 6, "old-back".to_owned()));
	expect_at_survivors(
		&survivors,
		&pki,
		&descriptor,
		deadline,
		&expected,
		"with the old instance back",
	)
	.await;
	if let Some(why) = formed.daemons.exited() {
		panic!("with the old instance back, after the probes: {why}");
	}
	formed.daemons.stop(lost.id, deadline);
	let after_old = listed(&objects, deadline, "after the old instance stopped").await;
	baseline_still_listed(&old_objects, &after_old)
		.unwrap_or_else(|why| panic!("after the old instance stopped: {why}"));
	let added: Vec<String> = keys_of_owner(&after_old, lost.id, &old_incarnation)
		.into_iter()
		.filter(|key| !old_objects.contains(key))
		.collect();
	eprintln!(
		"smoke: I's {} objects listed after the loss are all listed before the old instance returned and after it stopped; RECORD: objects of I added since the loss: {added:?}",
		old_objects.len()
	);
	let refused = formed
		.daemons
		.old_log(lost.id)
		.matches("it is addressed to another instance of this slot")
		.count();
	eprintln!(
		"smoke: RECORD: requests addressed to another instance refused by the old process: {refused}"
	);

	// S13.
	at("return");
	formed.daemons.start(lost);
	tokio::time::sleep(Duration::from_secs(2)).await;
	assert_eq!(
		replacement_status(
			"replace-status-returned",
			token,
			lost.id,
			&connection,
			deadline
		),
		Some(ReportedPhase::Done(FIRST_ALLOCATED))
	);
	commit_offering_again(
		&through,
		&descriptor,
		ROWS + 7,
		"returned",
		lost.id,
		"after the return",
	)
	.await;
	expected.push((ROWS + 7, "returned".to_owned()));
	expect_at_survivors(
		&survivors,
		&pki,
		&descriptor,
		deadline,
		&expected,
		"after the replacement's return",
	)
	.await;
	at("stops");
	formed.daemons.stop(lost.id, deadline);
	for member in &survivors {
		formed.daemons.stop(member.id, deadline);
	}
	eprintln!(
		"smoke: hosted replacement workflow passed in {:?}",
		started.elapsed()
	);
	assert!(Instant::now() < deadline, "the test overran its bound");
	std::fs::remove_dir_all(&formed.root).unwrap();
}

/// Copies a directory tree, files and directories only.
fn copy_tree(from: &Path, to: &Path) {
	std::fs::create_dir_all(to).unwrap();
	for entry in std::fs::read_dir(from).unwrap() {
		let entry = entry.unwrap();
		let target = to.join(entry.file_name());
		let kind = entry.file_type().unwrap();
		if kind.is_dir() {
			copy_tree(&entry.path(), &target);
		} else if kind.is_file() {
			std::fs::copy(entry.path(), &target).unwrap();
		}
	}
}

/// Every regular file under `dir`, by relative path, with its bytes.
fn durable_bytes(dir: &Path) -> std::collections::BTreeMap<PathBuf, Vec<u8>> {
	let mut files = std::collections::BTreeMap::new();
	let mut pending = vec![dir.to_path_buf()];
	while let Some(at) = pending.pop() {
		for entry in std::fs::read_dir(&at).unwrap() {
			let path = entry.unwrap().path();
			if path.is_dir() {
				pending.push(path);
			} else if path.is_file() {
				files.insert(
					path.strip_prefix(dir).unwrap().to_path_buf(),
					std::fs::read(&path).unwrap(),
				);
			}
		}
	}
	files
}
