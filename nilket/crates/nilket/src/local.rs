//! Foreground supervisor for the generated local Nilket deployment profile.
//!
//! It owns process lifecycle and configuration artifacts only. Role runtimes
//! remain in their explicit child processes (TORON-532).

use std::fs::{self, File, OpenOptions};
use std::io::{self, Read, Write};
use std::net::{SocketAddr, TcpListener, UdpSocket};
use std::os::fd::AsRawFd;
use std::os::unix::fs::{DirBuilderExt, OpenOptionsExt, PermissionsExt};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, ExitCode, ExitStatus, Stdio};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use fs2::FileExt;
use nilket_local::{
	DevelopmentPki, FORMAT_VERSION, LocalWorkspace, ProfileMarker, ProfilePaths, RuntimeManifest,
	SQL_ENDPOINT, WENDELON_ENDPOINT, blocking_discover, process_start_token,
	publish_runtime_manifest, resolve_profile_root, write_create_new,
	write_profile_development_pki,
};
use nix::sys::signal::{Signal, killpg};
use nix::unistd::{Pid, getpid, getppid};
use signal_hook::consts::SIGINT;
use toron_client::blocking_health_loopback;
use uuid::Uuid;
use wendelon::cli::ReadinessProbe;
use wendelon::storage::ownership::{DataDirectoryOwner, UNCLEAN_SHUTDOWN_EXIT_CODE};

const READY_ENDPOINT: &str = "127.0.0.1:7432";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const TORON_DRAIN_TIMEOUT: Duration = Duration::from_secs(35);
/// Supervisor margin in Wendelon's stop grace. Wendelon's own shutdown watchdog
/// fires at 2 x `tuning.storage_shutdown_timeout_seconds` + 25 s (the quiescence
/// wait and the storage close are each bounded by the timeout, plus 25 s for the
/// Raft/handle/listener steps and the runtime teardown) and then self-terminates
/// with [`UNCLEAN_SHUTDOWN_EXIT_CODE`]. The supervisor grace is 2 x timeout + 40 s,
/// i.e. that watchdog deadline + 15 s, so Wendelon always reports its own
/// outcome before the supervisor resorts to SIGKILL.
const WENDELON_STOP_MARGIN: Duration = Duration::from_secs(40);
/// Grace used when the retained Wendelon config cannot be read at stop time:
/// 2 x the default 30 s storage shutdown timeout + [`WENDELON_STOP_MARGIN`]. It
/// must exceed Wendelon's own watchdog deadline (2 x the timeout + 25 s).
const DEFAULT_WENDELON_STOP_GRACE: Duration = Duration::from_secs(100);
/// The signal that asks a role to drain. Wendelon drains on SIGINT or SIGTERM;
/// Toron drains on Ctrl-C (`tokio::signal::ctrl_c`) only, where SIGTERM would end
/// it without a drain. SIGINT works for both; switch to SIGTERM once Toron
/// handles it.
const GRACEFUL_STOP_SIGNAL: Signal = Signal::SIGINT;
/// Bound on reaping the group leader after SIGKILL.
const KILL_REAP_DEADLINE: Duration = Duration::from_secs(2);
/// Bound on joining the stdout/stderr forwarders once the child is gone. A
/// descendant that escaped the group can hold a pipe open; its reader is detached.
const READER_DRAIN_DEADLINE: Duration = Duration::from_secs(2);
const STOP_POLL_INTERVAL: Duration = Duration::from_millis(20);
const MAX_CHILD_LINE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalOptions {
	pub data_dir: Option<PathBuf>,
	pub reset: bool,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalInitOptions {
	pub data_dir: Option<PathBuf>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalReady {
	pub sql_endpoint: SocketAddr,
	pub session_nonce: Uuid,
}

impl LocalReady {
	pub fn format_line(&self) -> String {
		format!(
			"LOCAL_READY v=1 profile=local sql={} session={}\n",
			self.sql_endpoint, self.session_nonce
		)
	}

	pub fn parse(line: &str) -> Result<Self, String> {
		if !line.ends_with('\n') {
			return Err("LOCAL_READY line must end with a newline".to_owned());
		}
		let fields: Vec<_> = line
			.trim_end_matches('\n')
			.split_ascii_whitespace()
			.collect();
		if fields.len() != 5 || fields[0] != "LOCAL_READY" {
			return Err("LOCAL_READY v1 requires exactly four ordered fields".to_owned());
		}
		if fields[1] != "v=1" {
			return Err("unsupported LOCAL_READY version".to_owned());
		}
		if fields[2] != "profile=local" {
			return Err("LOCAL_READY v1 profile must be local".to_owned());
		}
		let sql_endpoint = fields[3]
			.strip_prefix("sql=")
			.ok_or_else(|| "LOCAL_READY v1 is missing sql".to_owned())?
			.parse::<SocketAddr>()
			.map_err(|error| format!("invalid LOCAL_READY sql endpoint: {error}"))?;
		let session_nonce = Uuid::parse_str(
			fields[4]
				.strip_prefix("session=")
				.ok_or_else(|| "LOCAL_READY v1 is missing session".to_owned())?,
		)
		.map_err(|error| format!("invalid LOCAL_READY session nonce: {error}"))?;
		Ok(Self {
			sql_endpoint,
			session_nonce,
		})
	}
}

pub fn run(options: LocalOptions) -> ExitCode {
	match run_owned(options) {
		Ok(code) => code,
		Err(message) => {
			eprintln!("nilket local: {message}");
			ExitCode::from(1)
		}
	}
}

fn run_owned(options: LocalOptions) -> Result<ExitCode, String> {
	// Production owns the well-known local endpoints; only tests substitute their own,
	// and they substitute *ephemeral* ones so a running deployment can never decide
	// whether the suite passes.
	run_owned_with_requirements(options, &ENDPOINT_REQUIREMENTS)
}

fn run_owned_with_requirements(
	options: LocalOptions,
	requirements: &[EndpointRequirement],
) -> Result<ExitCode, String> {
	if !cfg!(target_os = "linux") {
		return Err("the local profile currently supports Linux only".to_owned());
	}
	let paths = ProfilePaths::new(resolve_profile_root(options.data_dir.as_deref())?);
	validate_profile_root_candidate(&paths)?;
	prepare_root_for_lock(&paths.root)?;
	let lock_file = open_lock(&paths.lock)?;
	match lock_file.try_lock_exclusive() {
		Ok(()) => {}
		Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
			return report_existing(&paths);
		}
		Err(error) => return Err(format!("failed to lock {}: {error}", paths.lock.display())),
	}

	if options.reset {
		reset_locked_profile(&paths)?;
	}
	preflight_requirements(requirements).map_err(|conflict| conflict.to_string())?;
	let (marker, pki) = prepare_profile(&paths)?;
	refuse_non_regular_artifact_if_present(&paths.runtime, "runtime manifest")?;
	if paths.runtime.exists() {
		fs::remove_file(&paths.runtime).map_err(|error| {
			format!(
				"failed to remove stale {}: {error}",
				paths.runtime.display()
			)
		})?;
	}

	eprintln!("nilket local: profile {}", paths.root.display());
	eprintln!("nilket local: development-only PKI; not production custody");
	let interrupted = Arc::new(AtomicBool::new(false));
	signal_hook::flag::register(SIGINT, Arc::clone(&interrupted))
		.map_err(|error| format!("failed to install Ctrl-C handler: {error}"))?;

	let executable = std::env::current_exe()
		.map_err(|error| format!("failed to resolve current nilket executable: {error}"))?;
	let wendelon_grace = wendelon_stop_grace(&paths.wendelon_config);
	let mut wendelon = OwnedChild::spawn(
		"wendelon",
		&executable,
		["wendelon", path_text(&paths.wendelon_config)?],
	)?;
	let readiness = ReadinessProbe::new(
		READY_ENDPOINT
			.parse::<SocketAddr>()
			.expect("fixed ready endpoint"),
		"node-1.cluster.local",
		&pki.operator_cert,
		&pki.operator_key,
		&pki.node_ca_cert,
	);
	if let Err(error) = wait_for_wendelon(&mut wendelon.child, &readiness, &interrupted) {
		let report = wendelon.stop(GRACEFUL_STOP_SIGNAL, wendelon_grace);
		if report.outcome.needs_recovery() {
			eprintln!("nilket local: {error}");
			return Ok(unclean_exit_code());
		}
		return Err(error);
	}

	let mut toron = match OwnedChild::spawn(
		"toron",
		&executable,
		["toron", "start", path_text(&paths.toron_config)?],
	) {
		Ok(child) => child,
		Err(error) => {
			let report = wendelon.stop(GRACEFUL_STOP_SIGNAL, wendelon_grace);
			if report.outcome.needs_recovery() {
				eprintln!("nilket local: {error}");
				return Ok(unclean_exit_code());
			}
			return Err(error);
		}
	};
	if let Err(error) = wait_for_toron(&mut toron.child, SQL_ENDPOINT, &interrupted) {
		toron.stop(GRACEFUL_STOP_SIGNAL, TORON_DRAIN_TIMEOUT);
		let report = wendelon.stop(GRACEFUL_STOP_SIGNAL, wendelon_grace);
		if report.outcome.needs_recovery() {
			eprintln!("nilket local: {error}");
			return Ok(unclean_exit_code());
		}
		return Err(error);
	}

	let runtime = RuntimeManifest {
		version: FORMAT_VERSION,
		profile_uuid: marker.profile_uuid,
		session_nonce: Uuid::now_v7(),
		pid: std::process::id(),
		process_start_token: process_start_token(std::process::id())?,
		started_unix_ms: SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_err(|error| format!("system clock precedes Unix epoch: {error}"))?
			.as_millis(),
		wendelon_endpoint: WENDELON_ENDPOINT.to_owned(),
		sql_endpoint: SQL_ENDPOINT.to_owned(),
	};
	publish_runtime_manifest(&paths.runtime, &runtime)?;
	let ready = LocalReady {
		sql_endpoint: SQL_ENDPOINT.parse().expect("fixed SQL endpoint"),
		session_nonce: runtime.session_nonce,
	};
	print!("{}", ready.format_line());
	io::stdout()
		.flush()
		.map_err(|error| format!("publish LOCAL_READY: {error}"))?;

	let exit_code = supervise(
		&mut toron,
		&mut wendelon,
		wendelon_grace,
		&interrupted,
		&paths,
	);
	let _ = fs::remove_file(&paths.runtime);
	drop(lock_file);
	Ok(exit_code)
}

/// Write the profile's configuration and development PKI exactly as [`run`] does
/// before it starts anything, then return without starting a role (ADR 0172).
pub fn init(options: LocalInitOptions) -> ExitCode {
	match init_owned(options) {
		Ok(root) => {
			eprintln!("nilket local init: profile {}", root.display());
			eprintln!("nilket local init: development-only PKI; not production custody");
			ExitCode::SUCCESS
		}
		Err(message) => {
			eprintln!("nilket local init: {message}");
			ExitCode::from(1)
		}
	}
}

fn init_owned(options: LocalInitOptions) -> Result<PathBuf, String> {
	if !cfg!(target_os = "linux") {
		return Err("the local profile currently supports Linux only".to_owned());
	}
	let paths = ProfilePaths::new(resolve_profile_root(options.data_dir.as_deref())?);
	validate_profile_root_candidate(&paths)?;
	prepare_root_for_lock(&paths.root)?;
	let lock_file = open_lock(&paths.lock)?;
	match lock_file.try_lock_exclusive() {
		Ok(()) => {}
		Err(error) if error.kind() == io::ErrorKind::WouldBlock => {
			return Err(format!(
				"profile {} is in use by a running deployment",
				paths.root.display()
			));
		}
		Err(error) => return Err(format!("failed to lock {}: {error}", paths.lock.display())),
	}
	prepare_profile(&paths)?;
	drop(lock_file);
	Ok(paths.root)
}

/// The profile artifacts both [`run`] and [`init`] require: marker, development
/// PKI, and role configs, generated when absent and validated when retained.
fn prepare_profile(paths: &ProfilePaths) -> Result<(ProfileMarker, DevelopmentPki), String> {
	let marker = ensure_profile(paths)?;
	let pki = ensure_artifacts(paths)?;
	validate_artifacts(paths)?;
	Ok((marker, pki))
}

fn prepare_root_for_lock(root: &Path) -> Result<(), String> {
	if root.exists() {
		let metadata = fs::symlink_metadata(root)
			.map_err(|error| format!("inspect {}: {error}", root.display()))?;
		if metadata.file_type().is_symlink() || !metadata.is_dir() {
			return Err(format!(
				"profile root {} must be a real directory",
				root.display()
			));
		}
	} else {
		fs::DirBuilder::new()
			.recursive(true)
			.mode(0o700)
			.create(root)
			.map_err(|error| format!("create profile root {}: {error}", root.display()))?;
	}
	fs::set_permissions(root, fs::Permissions::from_mode(0o700))
		.map_err(|error| format!("secure profile root {}: {error}", root.display()))
}

fn validate_profile_root_candidate(paths: &ProfilePaths) -> Result<(), String> {
	if !paths.root.exists() {
		return Ok(());
	}
	let metadata = fs::symlink_metadata(&paths.root)
		.map_err(|error| format!("inspect {}: {error}", paths.root.display()))?;
	if metadata.file_type().is_symlink() || !metadata.is_dir() {
		return Err(format!(
			"profile root {} must be a real directory",
			paths.root.display()
		));
	}
	let entries: Vec<_> = fs::read_dir(&paths.root)
		.map_err(|error| format!("read profile root {}: {error}", paths.root.display()))?
		.collect::<Result<_, _>>()
		.map_err(|error| format!("read profile entry: {error}"))?;
	if entries.is_empty()
		|| paths.marker.exists()
		|| (entries.len() == 1 && entries[0].file_name() == "local.lock")
	{
		return Ok(());
	}
	Err(format!(
		"{} is not an empty directory or a marked Nilket profile",
		paths.root.display()
	))
}

fn open_lock(path: &Path) -> Result<File, String> {
	refuse_non_regular_artifact_if_present(path, "profile lock")?;
	let file = OpenOptions::new()
		.read(true)
		.write(true)
		.create(true)
		.mode(0o600)
		.custom_flags(libc::O_CLOEXEC)
		.open(path)
		.map_err(|error| format!("open profile lock {}: {error}", path.display()))?;
	let flags = unsafe { libc::fcntl(file.as_raw_fd(), libc::F_GETFD) };
	if flags < 0 || flags & libc::FD_CLOEXEC == 0 {
		return Err(format!(
			"profile lock {} is not close-on-exec",
			path.display()
		));
	}
	Ok(file)
}

fn ensure_profile(paths: &ProfilePaths) -> Result<ProfileMarker, String> {
	refuse_non_regular_artifact_if_present(&paths.marker, "profile marker")?;
	if paths.marker.exists() {
		let marker: ProfileMarker = toml::from_str(
			&fs::read_to_string(&paths.marker)
				.map_err(|error| format!("read {}: {error}", paths.marker.display()))?,
		)
		.map_err(|error| format!("malformed {}: {error}; use --reset", paths.marker.display()))?;
		if marker.format_version != FORMAT_VERSION {
			return Err(format!(
				"unsupported profile format {} in {}; expected {}; use --reset",
				marker.format_version,
				paths.marker.display(),
				FORMAT_VERSION
			));
		}
		return Ok(marker);
	}
	let foreign = fs::read_dir(&paths.root)
		.map_err(|error| format!("read profile root {}: {error}", paths.root.display()))?
		.filter_map(Result::ok)
		.any(|entry| entry.file_name() != "local.lock");
	if foreign {
		return Err(format!(
			"{} is not an empty directory or a marked Nilket profile",
			paths.root.display()
		));
	}
	let marker = ProfileMarker {
		format_version: FORMAT_VERSION,
		profile_uuid: Uuid::new_v4(),
	};
	write_create_new(
		&paths.marker,
		toml::to_string_pretty(&marker)
			.map_err(|error| format!("serialize profile marker: {error}"))?
			.as_bytes(),
		0o600,
	)?;
	Ok(marker)
}

fn refuse_non_regular_artifact_if_present(path: &Path, label: &str) -> Result<(), String> {
	match fs::symlink_metadata(path) {
		Ok(metadata) if metadata.file_type().is_symlink() || !metadata.is_file() => {
			Err(format!("{label} {} must be a regular file", path.display()))
		}
		Ok(_) => Ok(()),
		Err(error) if error.kind() == io::ErrorKind::NotFound => Ok(()),
		Err(error) => Err(format!("inspect {label} {}: {error}", path.display())),
	}
}

fn ensure_artifacts(paths: &ProfilePaths) -> Result<DevelopmentPki, String> {
	for directory in [&paths.config_dir, &paths.wendelon_data, &paths.logs_dir] {
		fs::DirBuilder::new()
			.recursive(true)
			.mode(0o700)
			.create(directory)
			.map_err(|error| format!("create {}: {error}", directory.display()))?;
		fs::set_permissions(directory, fs::Permissions::from_mode(0o700))
			.map_err(|error| format!("secure {}: {error}", directory.display()))?;
	}
	let pki = write_profile_development_pki(&paths.pki_dir)?;
	if !paths.wendelon_config.exists() {
		write_create_new(
			&paths.wendelon_config,
			wendelon::config::render_local_profile_config(&paths.wendelon_data, &pki).as_bytes(),
			0o600,
		)?;
	}
	if !paths.toron_config.exists() {
		write_create_new(
			&paths.toron_config,
			toron::config::render_local_profile_config(&pki).as_bytes(),
			0o600,
		)?;
	}
	Ok(pki)
}

fn validate_artifacts(paths: &ProfilePaths) -> Result<(), String> {
	wendelon::config::load_node_bootstrap_config(&paths.wendelon_config).map_err(|error| {
		format!(
			"invalid retained Wendelon config {}: {error}; repair it or use --reset",
			paths.wendelon_config.display()
		)
	})?;
	toron::config::load_toron_bootstrap_config(&paths.toron_config).map_err(|error| {
		format!(
			"invalid retained Toron config {}: {error}; repair it or use --reset",
			paths.toron_config.display()
		)
	})?;
	Ok(())
}

fn reset_locked_profile(paths: &ProfilePaths) -> Result<(), String> {
	let canonical = fs::canonicalize(&paths.root)
		.map_err(|error| format!("resolve reset target {}: {error}", paths.root.display()))?;
	if canonical == Path::new("/") {
		return Err("refusing to reset filesystem root".to_owned());
	}
	if let Some(home) = std::env::var_os("HOME") {
		if fs::canonicalize(home).ok().as_deref() == Some(canonical.as_path()) {
			return Err("refusing to reset the user's home directory".to_owned());
		}
	}
	let marker: ProfileMarker = toml::from_str(
		&fs::read_to_string(&paths.marker)
			.map_err(|_| format!("refusing unmarked reset target {}", canonical.display()))?,
	)
	.map_err(|error| format!("refusing malformed profile marker: {error}"))?;
	if marker.format_version != FORMAT_VERSION {
		return Err("refusing reset of unsupported profile format".to_owned());
	}
	let data_root = paths.root.join("data");
	if data_root.exists() {
		refuse_symlink_tree(&data_root)?;
	}
	let _wendelon_owner =
		DataDirectoryOwner::acquire_or_create(&paths.wendelon_data).map_err(|error| {
			format!("refusing reset without Wendelon data-directory ownership: {error}")
		})?;
	eprintln!("nilket local: resetting {}", canonical.display());
	for generated in ["config", "pki", "logs"] {
		let target = paths.root.join(generated);
		if target.exists() {
			refuse_symlink_tree(&target)?;
			fs::remove_dir_all(&target)
				.map_err(|error| format!("reset {}: {error}", target.display()))?;
		}
	}
	clear_data_preserving_wendelon_lock(&data_root, &paths.wendelon_data)?;
	if paths.runtime.exists() {
		fs::remove_file(&paths.runtime)
			.map_err(|error| format!("reset {}: {error}", paths.runtime.display()))?;
	}
	Ok(())
}

fn clear_data_preserving_wendelon_lock(
	data_root: &Path,
	wendelon_data: &Path,
) -> Result<(), String> {
	if !data_root.exists() {
		return Ok(());
	}
	for entry in fs::read_dir(data_root)
		.map_err(|error| format!("inspect reset data {}: {error}", data_root.display()))?
	{
		let path = entry
			.map_err(|error| format!("inspect reset data entry: {error}"))?
			.path();
		if path == wendelon_data {
			for child in fs::read_dir(&path).map_err(|error| {
				format!("inspect Wendelon reset data {}: {error}", path.display())
			})? {
				let child = child
					.map_err(|error| format!("inspect Wendelon reset entry: {error}"))?
					.path();
				if child.file_name().is_some_and(|name| name == "LOCK") {
					continue;
				}
				refuse_symlink_tree(&child)?;
				if child.is_dir() {
					fs::remove_dir_all(&child)
				} else {
					fs::remove_file(&child)
				}
				.map_err(|error| format!("reset {}: {error}", child.display()))?;
			}
		} else {
			refuse_symlink_tree(&path)?;
			if path.is_dir() {
				fs::remove_dir_all(&path)
			} else {
				fs::remove_file(&path)
			}
			.map_err(|error| format!("reset {}: {error}", path.display()))?;
		}
	}
	Ok(())
}

fn refuse_symlink_tree(path: &Path) -> Result<(), String> {
	let metadata = fs::symlink_metadata(path)
		.map_err(|error| format!("inspect reset target {}: {error}", path.display()))?;
	if metadata.file_type().is_symlink() {
		return Err(format!(
			"refusing symlink in marked profile: {}",
			path.display()
		));
	}
	if metadata.is_dir() {
		for entry in fs::read_dir(path)
			.map_err(|error| format!("inspect reset directory {}: {error}", path.display()))?
		{
			refuse_symlink_tree(
				&entry
					.map_err(|error| format!("inspect reset entry: {error}"))?
					.path(),
			)?;
		}
	}
	Ok(())
}

fn report_existing(paths: &ProfilePaths) -> Result<ExitCode, String> {
	match blocking_discover(Some(&paths.root)) {
		LocalWorkspace::Ready(workspace) => {
			let ready = LocalReady {
				sql_endpoint: workspace.sql_endpoint,
				session_nonce: workspace.identity.session_nonce,
			};
			print!("{}", ready.format_line());
			Ok(ExitCode::SUCCESS)
		}
		state => Err(state.to_string()),
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LocalRole {
	Wendelon,
	Toron,
}

impl std::fmt::Display for LocalRole {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Wendelon => formatter.write_str("Wendelon"),
			Self::Toron => formatter.write_str("Toron"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum EndpointTransport {
	Udp,
	Tcp,
}

impl std::fmt::Display for EndpointTransport {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Udp => formatter.write_str("UDP"),
			Self::Tcp => formatter.write_str("TCP"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct EndpointRequirement {
	role: LocalRole,
	transport: EndpointTransport,
	endpoint: &'static str,
}

const ENDPOINT_REQUIREMENTS: [EndpointRequirement; 6] = [
	EndpointRequirement {
		role: LocalRole::Wendelon,
		transport: EndpointTransport::Udp,
		endpoint: "127.0.0.1:7001",
	},
	EndpointRequirement {
		role: LocalRole::Toron,
		transport: EndpointTransport::Udp,
		endpoint: SQL_ENDPOINT,
	},
	EndpointRequirement {
		role: LocalRole::Wendelon,
		transport: EndpointTransport::Tcp,
		endpoint: "127.0.0.1:7001",
	},
	EndpointRequirement {
		role: LocalRole::Wendelon,
		transport: EndpointTransport::Tcp,
		endpoint: "127.0.0.1:7431",
	},
	EndpointRequirement {
		role: LocalRole::Wendelon,
		transport: EndpointTransport::Tcp,
		endpoint: READY_ENDPOINT,
	},
	EndpointRequirement {
		role: LocalRole::Wendelon,
		transport: EndpointTransport::Tcp,
		endpoint: "127.0.0.1:7433",
	},
];

#[derive(Debug)]
struct EndpointConflict {
	requirement: EndpointRequirement,
	source: io::Error,
}

impl std::fmt::Display for EndpointConflict {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		write!(
			formatter,
			"local creation conflict: {} {} endpoint {} is unavailable: {}",
			self.requirement.role,
			self.requirement.transport,
			self.requirement.endpoint,
			self.source
		)
	}
}

fn preflight_requirements(requirements: &[EndpointRequirement]) -> Result<(), EndpointConflict> {
	let mut held: Vec<Box<dyn std::any::Any>> = Vec::new();
	for &requirement in requirements {
		let socket: Box<dyn std::any::Any> = match requirement.transport {
			EndpointTransport::Udp => {
				Box::new(UdpSocket::bind(requirement.endpoint).map_err(|source| {
					EndpointConflict {
						requirement,
						source,
					}
				})?)
			}
			EndpointTransport::Tcp => {
				Box::new(TcpListener::bind(requirement.endpoint).map_err(|source| {
					EndpointConflict {
						requirement,
						source,
					}
				})?)
			}
		};
		held.push(socket);
	}
	drop(held);
	Ok(())
}

fn wait_for_wendelon(
	child: &mut Child,
	probe: &ReadinessProbe,
	interrupted: &AtomicBool,
) -> Result<(), String> {
	let deadline = Instant::now() + STARTUP_TIMEOUT;
	loop {
		if interrupted.load(Ordering::Relaxed) {
			return Err("startup interrupted".to_owned());
		}
		if let Some(status) = child
			.try_wait()
			.map_err(|error| format!("poll Wendelon: {error}"))?
		{
			return Err(format!("Wendelon exited before readiness: {status}"));
		}
		if probe.check_blocking().unwrap_or(false) {
			return Ok(());
		}
		if Instant::now() >= deadline {
			return Err("Wendelon authenticated readiness timed out".to_owned());
		}
		thread::sleep(POLL_INTERVAL);
	}
}

fn wait_for_toron(
	child: &mut Child,
	endpoint: &str,
	interrupted: &AtomicBool,
) -> Result<(), String> {
	let deadline = Instant::now() + STARTUP_TIMEOUT;
	loop {
		if interrupted.load(Ordering::Relaxed) {
			return Err("startup interrupted".to_owned());
		}
		if let Some(status) = child
			.try_wait()
			.map_err(|error| format!("poll Toron: {error}"))?
		{
			return Err(format!("Toron exited before health: {status}"));
		}
		if blocking_health_loopback(endpoint).is_ok() {
			return Ok(());
		}
		if Instant::now() >= deadline {
			return Err("Toron SQL health timed out".to_owned());
		}
		thread::sleep(POLL_INTERVAL);
	}
}

fn supervise(
	toron: &mut OwnedChild,
	wendelon: &mut OwnedChild,
	wendelon_grace: Duration,
	interrupted: &AtomicBool,
	paths: &ProfilePaths,
) -> ExitCode {
	loop {
		if interrupted.load(Ordering::Relaxed) {
			eprintln!("nilket local: draining Toron before Wendelon");
			toron.stop(GRACEFUL_STOP_SIGNAL, TORON_DRAIN_TIMEOUT);
			let report = wendelon.stop(GRACEFUL_STOP_SIGNAL, wendelon_grace);
			return if report.outcome.needs_recovery() {
				unclean_exit_code()
			} else if matches!(report.outcome, StopOutcome::Failed(_)) {
				ExitCode::from(1)
			} else {
				ExitCode::SUCCESS
			};
		}
		match toron.child.try_wait() {
			Ok(Some(status)) => {
				eprintln!(
					"nilket local: Toron exited unexpectedly ({status}); logs/profile: {}",
					paths.root.display()
				);
				let report = wendelon.stop(GRACEFUL_STOP_SIGNAL, wendelon_grace);
				return if report.outcome.needs_recovery() {
					unclean_exit_code()
				} else {
					ExitCode::from(1)
				};
			}
			Err(error) => {
				eprintln!("nilket local: failed to poll Toron: {error}");
				let report = wendelon.stop(GRACEFUL_STOP_SIGNAL, wendelon_grace);
				return if report.outcome.needs_recovery() {
					unclean_exit_code()
				} else {
					ExitCode::from(1)
				};
			}
			Ok(None) => {}
		}
		match wendelon.child.try_wait() {
			Ok(Some(status)) => {
				let outcome = StopOutcome::classify(Some(status), false);
				if outcome == StopOutcome::UncleanRestartIntoRecovery {
					eprintln!(
						"nilket local: Wendelon {outcome}; logs/profile: {}",
						paths.root.display()
					);
				} else {
					eprintln!(
						"nilket local: Wendelon exited unexpectedly ({status}); logs/profile: {}",
						paths.root.display()
					);
				}
				toron.stop(GRACEFUL_STOP_SIGNAL, TORON_DRAIN_TIMEOUT);
				wendelon.drain_readers(READER_DRAIN_DEADLINE);
				return if outcome.needs_recovery() {
					unclean_exit_code()
				} else {
					ExitCode::from(1)
				};
			}
			Err(error) => {
				eprintln!("nilket local: failed to poll Wendelon: {error}");
				toron.stop(GRACEFUL_STOP_SIGNAL, TORON_DRAIN_TIMEOUT);
				return ExitCode::from(1);
			}
			Ok(None) => {}
		}
		thread::sleep(POLL_INTERVAL);
	}
}

/// The supervisor's stop grace for Wendelon: 2 x its configured storage shutdown
/// timeout plus [`WENDELON_STOP_MARGIN`], so Wendelon's own bounded shutdown
/// (and its watchdog) always gets to finish before the supervisor kills the group.
fn wendelon_stop_grace(config: &Path) -> Duration {
	match wendelon::config::load_node_bootstrap_config(config) {
		Ok(config) => grace_for_storage_shutdown_timeout(config.storage_shutdown_timeout),
		Err(error) => {
			eprintln!(
				"nilket local: cannot read Wendelon storage shutdown timeout from {} ({error}); \
				 using a {DEFAULT_WENDELON_STOP_GRACE:?} stop grace",
				config.display()
			);
			DEFAULT_WENDELON_STOP_GRACE
		}
	}
}

fn grace_for_storage_shutdown_timeout(timeout: Duration) -> Duration {
	timeout
		.saturating_mul(2)
		.saturating_add(WENDELON_STOP_MARGIN)
}

fn unclean_exit_code() -> ExitCode {
	ExitCode::from(u8::try_from(UNCLEAN_SHUTDOWN_EXIT_CODE).expect("unclean exit code fits u8"))
}

/// How a supervised role ended.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum StopOutcome {
	/// Exited with status 0.
	Clean,
	/// Exited with [`UNCLEAN_SHUTDOWN_EXIT_CODE`]: the role could not prove every
	/// writer stopped. Not a configuration error; the next start runs recovery.
	UncleanRestartIntoRecovery,
	/// Did not exit within its grace and was SIGKILLed with its process group.
	KilledBySupervisor,
	/// Any other exit status, or a death by a signal the supervisor did not send.
	Failed(ExitStatus),
	/// Could not be reaped within the bounded waits.
	Unreaped,
}

impl StopOutcome {
	fn classify(status: Option<ExitStatus>, sigkill_sent: bool) -> Self {
		match status.and_then(|status| status.code()) {
			Some(0) => Self::Clean,
			Some(UNCLEAN_SHUTDOWN_EXIT_CODE) => Self::UncleanRestartIntoRecovery,
			_ if sigkill_sent => Self::KilledBySupervisor,
			_ => status.map_or(Self::Unreaped, Self::Failed),
		}
	}

	/// Whether the role ended without a clean shutdown, so its next start recovers.
	/// A death by any signal (OOM kill, an early signal before the drain handler)
	/// leaves no clean-shutdown record either.
	fn needs_recovery(self) -> bool {
		use std::os::unix::process::ExitStatusExt;
		match self {
			Self::UncleanRestartIntoRecovery | Self::KilledBySupervisor | Self::Unreaped => true,
			Self::Failed(status) => status.signal().is_some(),
			Self::Clean => false,
		}
	}
}

impl std::fmt::Display for StopOutcome {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Clean => formatter.write_str("clean stop"),
			Self::UncleanRestartIntoRecovery => write!(
				formatter,
				"unclean shutdown (exit {UNCLEAN_SHUTDOWN_EXIT_CODE}): restart into recovery"
			),
			Self::KilledBySupervisor => formatter.write_str("unclean stop (killed by supervisor)"),
			Self::Failed(status) => write!(formatter, "failed stop ({status})"),
			Self::Unreaped => formatter.write_str("unclean stop (not reaped after SIGKILL)"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct StopReport {
	outcome: StopOutcome,
	sigkill_sent: bool,
}

struct OwnedChild {
	label: &'static str,
	child: Child,
	readers: Vec<JoinHandle<()>>,
}

impl OwnedChild {
	fn spawn<I, S>(label: &'static str, executable: &Path, args: I) -> Result<Self, String>
	where
		I: IntoIterator<Item = S>,
		S: AsRef<std::ffi::OsStr>,
	{
		let parent = getpid();
		let mut command = Command::new(executable);
		command
			.args(args)
			.stdout(Stdio::piped())
			.stderr(Stdio::piped())
			.process_group(0);
		// SAFETY: the hook calls only async-signal-safe Linux syscalls through
		// nix before exec. It installs the parent-death contract and then closes
		// the fork/configure race by rechecking the captured parent PID.
		unsafe {
			command.pre_exec(move || {
				nix::sys::prctl::set_pdeathsig(Some(Signal::SIGTERM)).map_err(io::Error::other)?;
				if getppid() != parent {
					return Err(io::Error::other(
						"nilket supervisor exited before child exec",
					));
				}
				Ok(())
			});
		}
		let mut child = command
			.spawn()
			.map_err(|error| format!("spawn {label} role: {error}"))?;
		let mut readers = Vec::new();
		if let Some(stdout) = child.stdout.take() {
			readers.push(spawn_attributed_reader(label, stdout, true));
		}
		if let Some(stderr) = child.stderr.take() {
			readers.push(spawn_attributed_reader(label, stderr, false));
		}
		Ok(Self {
			label,
			child,
			readers,
		})
	}

	/// Ask the child's process group to drain with `signal`, wait up to `grace`,
	/// then SIGKILL the group. Every wait is bounded; the child's leader is the
	/// group id (`process_group(0)`), and it is only signalled while unreaped so
	/// the id cannot have been recycled.
	fn stop(&mut self, signal: Signal, grace: Duration) -> StopReport {
		let group = Pid::from_raw(self.child.id() as i32);
		let mut sigkill_sent = false;
		let mut status = self.child.try_wait().ok().flatten();
		if status.is_none() {
			let _ = killpg(group, signal);
			status = self.wait_bounded(grace);
		}
		if status.is_none() {
			eprintln!(
				"nilket local: escalating {} to SIGKILL after {grace:?}",
				self.label
			);
			let _ = killpg(group, Signal::SIGKILL);
			sigkill_sent = true;
			status = self.wait_bounded(KILL_REAP_DEADLINE);
		}
		self.drain_readers(READER_DRAIN_DEADLINE);
		let outcome = StopOutcome::classify(status, sigkill_sent);
		if outcome != StopOutcome::Clean {
			eprintln!("nilket local: {} {outcome}", self.label);
		}
		StopReport {
			outcome,
			sigkill_sent,
		}
	}

	/// Poll for the child's exit until `timeout`; `None` if it is still running
	/// (or cannot be polled) at the deadline.
	fn wait_bounded(&mut self, timeout: Duration) -> Option<ExitStatus> {
		let deadline = Instant::now() + timeout;
		loop {
			match self.child.try_wait() {
				Ok(Some(status)) => return Some(status),
				Ok(None) => {}
				Err(error) => {
					eprintln!("nilket local: failed to reap {}: {error}", self.label);
					return None;
				}
			}
			let now = Instant::now();
			if now >= deadline {
				return None;
			}
			thread::sleep(STOP_POLL_INTERVAL.min(deadline - now));
		}
	}

	/// Join the output forwarders that finish within `timeout`; detach the rest.
	fn drain_readers(&mut self, timeout: Duration) {
		let deadline = Instant::now() + timeout;
		while self.readers.iter().any(|reader| !reader.is_finished()) && Instant::now() < deadline {
			thread::sleep(STOP_POLL_INTERVAL);
		}
		for reader in self.readers.drain(..) {
			if reader.is_finished() {
				let _ = reader.join();
			} else {
				eprintln!(
					"nilket local: {} output still open after {timeout:?}; detaching its reader",
					self.label
				);
			}
		}
	}
}

fn spawn_attributed_reader<R>(
	label: &'static str,
	mut reader: R,
	consume_readiness: bool,
) -> JoinHandle<()>
where
	R: Read + Send + 'static,
{
	thread::spawn(move || {
		let mut buffer = [0u8; 4096];
		let mut line = Vec::new();
		loop {
			match reader.read(&mut buffer) {
				Ok(0) => break,
				Ok(count) => {
					for byte in &buffer[..count] {
						line.push(*byte);
						if *byte == b'\n' || line.len() >= MAX_CHILD_LINE_BYTES {
							forward_child_line(label, &line, consume_readiness);
							line.clear();
						}
					}
				}
				Err(error) => {
					eprintln!("{label}: output read failed: {error}");
					break;
				}
			}
		}
		if !line.is_empty() {
			forward_child_line(label, &line, consume_readiness);
		}
	})
}

fn forward_child_line(label: &str, bytes: &[u8], consume_readiness: bool) {
	let text = String::from_utf8_lossy(bytes);
	let trimmed = text.trim_end_matches(['\r', '\n']);
	if consume_readiness && (trimmed.starts_with("READY ") || trimmed.starts_with("SQL_READY ")) {
		return;
	}
	let unprefixed = trimmed
		.strip_prefix(&format!("{label}: "))
		.unwrap_or(trimmed);
	eprintln!("{label}: {unprefixed}");
}

fn path_text(path: &Path) -> Result<&str, String> {
	path.to_str()
		.ok_or_else(|| format!("local profile path is not UTF-8: {}", path.display()))
}

#[cfg(test)]
mod tests {
	use super::*;
	use std::os::unix::fs::MetadataExt;
	use std::sync::{Barrier, Mutex};

	static PREFLIGHT_TEST_LOCK: Mutex<()> = Mutex::new(());
	// A concurrent fork briefly inherits open descriptors before CLOEXEC runs.
	// Keep process-spawn fixtures from crossing the proof that a dropped profile
	// lock is immediately acquirable.
	static PROCESS_AND_LOCK_TEST_LOCK: Mutex<()> = Mutex::new(());

	/// Serialise without letting one failure become everyone's failure. A panicking
	/// test poisons the mutex, and `unwrap()` then converts every sibling into an
	/// unrelated `PoisonError` — which buries the one real diagnosis under a pile of
	/// second-order ones. These locks order tests; they guard no invariant that a
	/// panic could have corrupted.
	fn lock_serial(lock: &'static Mutex<()>) -> std::sync::MutexGuard<'static, ()> {
		lock.lock()
			.unwrap_or_else(std::sync::PoisonError::into_inner)
	}

	/// Endpoints this test owns, discovered by binding ephemeral ports and releasing
	/// them. Never the production ports: whether a local deployment happens to be
	/// running must not decide the outcome of a unit test.
	fn ephemeral_requirements() -> Vec<EndpointRequirement> {
		ENDPOINT_REQUIREMENTS
			.iter()
			.map(|requirement| {
				let endpoint = match requirement.transport {
					EndpointTransport::Udp => UdpSocket::bind("127.0.0.1:0")
						.and_then(|socket| socket.local_addr())
						.expect("bind ephemeral udp endpoint"),
					EndpointTransport::Tcp => TcpListener::bind("127.0.0.1:0")
						.and_then(|listener| listener.local_addr())
						.expect("bind ephemeral tcp endpoint"),
				};
				EndpointRequirement {
					endpoint: Box::leak(endpoint.to_string().into_boxed_str()),
					..*requirement
				}
			})
			.collect()
	}

	fn fresh_root(tag: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"nilket-local-{tag}-{}-{}",
			std::process::id(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("clock")
				.as_nanos()
		))
	}

	#[test]
	fn local_ready_contract_round_trips_and_malformed_control_fires() {
		let ready = LocalReady {
			sql_endpoint: SQL_ENDPOINT.parse().unwrap(),
			session_nonce: Uuid::now_v7(),
		};
		assert_eq!(LocalReady::parse(&ready.format_line()).unwrap(), ready);
		for malformed in [
			"LOCAL_READY v=2 profile=local sql=127.0.0.1:7742 session=bad\n",
			"LOCAL_READY v=1 profile=local sql=127.0.0.1:7742\n",
			"LOCAL_READY v=1 profile=local sql=127.0.0.1:7742 session=00000000-0000-0000-0000-000000000000 extra=x\n",
			"LOCAL_READY v=1 profile=local sql=127.0.0.1:7742 session=00000000-0000-0000-0000-000000000000",
		] {
			assert!(
				LocalReady::parse(malformed).is_err(),
				"positive malformed control: {malformed:?}"
			);
		}
	}

	#[test]
	fn generated_profile_is_retained_parseable_tenant_zero_and_permissioned() {
		let root = fresh_root("profile");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let lock = open_lock(&paths.lock).unwrap();
		lock.lock_exclusive().unwrap();
		let marker = ensure_profile(&paths).unwrap();
		let pki = ensure_artifacts(&paths).unwrap();
		validate_artifacts(&paths).unwrap();
		let wendelon =
			wendelon::config::load_node_bootstrap_config(&paths.wendelon_config).unwrap();
		let toron = toron::config::load_toron_bootstrap_config(&paths.toron_config).unwrap();
		assert_eq!(wendelon.node_id, 1);
		assert_eq!(
			wendelon.cdc_locality_heartbeat_interval,
			Duration::from_secs(30)
		);
		assert_eq!(wendelon.data_ring_lease_ttl, Duration::from_secs(90));
		assert!(
			fs::read_to_string(&paths.wendelon_config)
				.unwrap()
				.contains("cdc_locality_heartbeat_interval_millis = 30000")
		);
		assert!(
			fs::read_to_string(&paths.wendelon_config)
				.unwrap()
				.contains("data_ring_lease_ttl_millis = 90000")
		);
		assert_eq!(toron.node.tenant_id.0, 0);
		assert_eq!(toron.sql.unwrap().listener_bind.to_string(), SQL_ENDPOINT);
		assert_eq!(
			toron.storage.wendelon.bootstrap_peers[0].address,
			WENDELON_ENDPOINT
		);
		assert_eq!(
			fs::metadata(&pki.operator_key)
				.unwrap()
				.permissions()
				.mode() & 0o777,
			0o600
		);
		let retained = fs::read(&paths.toron_config).unwrap();
		let second = ensure_artifacts(&paths).unwrap();
		assert_eq!(pki, second);
		assert_eq!(fs::read(&paths.toron_config).unwrap(), retained);
		assert_eq!(marker.format_version, FORMAT_VERSION);
		drop(lock);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn init_writes_the_profile_starts_nothing_and_retains_adjusted_configs() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let root = fresh_root("init");
		let paths = ProfilePaths::new(root.clone());
		let options = LocalInitOptions {
			data_dir: Some(root.clone()),
		};
		assert_eq!(init_owned(options.clone()).unwrap(), root);
		validate_artifacts(&paths).unwrap();
		assert!(paths.marker.exists());
		assert!(
			!paths.runtime.exists(),
			"init must not publish a runtime manifest"
		);
		let lock = open_lock(&paths.lock).unwrap();
		lock.try_lock_exclusive()
			.expect("init releases the profile lock before returning");
		drop(lock);

		// A deployment derived from the profile (ADR 0172) adjusts the retained
		// Toron config; re-running init validates it and changes nothing.
		let generated = fs::read_to_string(&paths.toron_config).unwrap();
		assert!(generated.contains("tenant_id = 0"));
		let adjusted = generated.replacen("tenant_id = 0", "tenant_id = 1", 1);
		fs::write(&paths.toron_config, &adjusted).unwrap();
		let marker = fs::read(&paths.marker).unwrap();
		let pki = fs::read(paths.pki_dir.join("node-ca.crt")).unwrap();
		init_owned(options).unwrap();
		assert_eq!(fs::read_to_string(&paths.toron_config).unwrap(), adjusted);
		assert_eq!(fs::read(&paths.marker).unwrap(), marker);
		assert_eq!(fs::read(paths.pki_dir.join("node-ca.crt")).unwrap(), pki);
		let toron = toron::config::load_toron_bootstrap_config(&paths.toron_config).unwrap();
		assert_eq!(toron.node.tenant_id.0, 1);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn init_refuses_a_profile_held_by_a_running_deployment() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let root = fresh_root("init-held");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let holder = open_lock(&paths.lock).unwrap();
		holder.lock_exclusive().unwrap();
		let error = init_owned(LocalInitOptions {
			data_dir: Some(root.clone()),
		})
		.unwrap_err();
		assert!(error.contains("in use by a running deployment"), "{error}");
		assert!(
			!paths.marker.exists(),
			"a refused init must not write the profile"
		);
		drop(holder);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn preflight_conflict_precedes_identity_and_leaves_an_absent_profile() {
		let _serial = lock_serial(&PREFLIGHT_TEST_LOCK);
		// Occupy an ephemeral endpoint this test owns and inject it as the requirement.
		// Binding the production port instead made the assertion depend on whether a
		// local deployment was running — it holds 127.0.0.1:7742, so the bind failed
		// and this test reported a conflict it never created.
		let occupied = UdpSocket::bind("127.0.0.1:0").expect("bind ephemeral endpoint");
		let endpoint: &'static str = Box::leak(
			occupied
				.local_addr()
				.expect("ephemeral local addr")
				.to_string()
				.into_boxed_str(),
		);
		let requirements = [EndpointRequirement {
			role: LocalRole::Toron,
			transport: EndpointTransport::Udp,
			endpoint,
		}];
		let root = fresh_root("preflight-residue");
		let error = run_owned_with_requirements(
			LocalOptions {
				data_dir: Some(root.clone()),
				reset: false,
			},
			&requirements,
		)
		.unwrap_err();
		let paths = ProfilePaths::new(root.clone());
		assert!(
			error.contains(&format!("Toron UDP endpoint {endpoint}")),
			"conflict must name the injected endpoint: {error}"
		);
		assert!(
			!paths.marker.exists(),
			"failed creation must publish no identity"
		);
		assert!(
			!paths.pki_dir.exists(),
			"failed creation must generate no PKI"
		);
		assert!(paths.lock.exists(), "the ownership shape may remain");
		assert!(matches!(
			blocking_discover(Some(&root)),
			LocalWorkspace::Absent { .. }
		));
		drop(occupied);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn every_preflight_conflict_is_typed_without_guessing_an_occupant() {
		let _serial = lock_serial(&PREFLIGHT_TEST_LOCK);
		for requirement in ENDPOINT_REQUIREMENTS {
			let (occupied, endpoint): (Box<dyn std::any::Any>, SocketAddr) =
				match requirement.transport {
					EndpointTransport::Udp => {
						let socket = UdpSocket::bind("127.0.0.1:0").unwrap();
						let endpoint = socket.local_addr().unwrap();
						(Box::new(socket), endpoint)
					}
					EndpointTransport::Tcp => {
						let socket = TcpListener::bind("127.0.0.1:0").unwrap();
						let endpoint = socket.local_addr().unwrap();
						(Box::new(socket), endpoint)
					}
				};
			let endpoint: &'static str = Box::leak(endpoint.to_string().into_boxed_str());
			let synthetic = EndpointRequirement {
				endpoint,
				..requirement
			};
			let conflict = preflight_requirements(&[synthetic]).unwrap_err();
			assert_eq!(conflict.requirement, synthetic);
			let rendered = conflict.to_string();
			assert!(rendered.contains(&requirement.role.to_string()));
			assert!(rendered.contains(&requirement.transport.to_string()));
			assert!(rendered.contains(endpoint));
			for speculation in ["foreign", "another process", "another Nilket", "occupant"] {
				assert!(
					!rendered.contains(speculation),
					"bind evidence must not speculate about {speculation}: {rendered}"
				);
			}
			drop(occupied);
		}
		assert_eq!(ENDPOINT_REQUIREMENTS.len(), 6);
		assert!(ENDPOINT_REQUIREMENTS.iter().all(|entry| {
			!entry.endpoint.is_empty()
				&& matches!(entry.role, LocalRole::Wendelon | LocalRole::Toron)
				&& matches!(
					entry.transport,
					EndpointTransport::Udp | EndpointTransport::Tcp
				)
		}));
	}

	#[test]
	fn successful_preflight_allows_identity_and_pki_generation() {
		let _serial = lock_serial(&PREFLIGHT_TEST_LOCK);
		// Free endpoints this test owns, not the production ports: the subject here is
		// that a *satisfied* preflight allows identity and PKI, not which ports the
		// machine happens to have spare.
		preflight_requirements(&ephemeral_requirements())
			.expect("fixture endpoints are unoccupied");
		let root = fresh_root("preflight-positive");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let lock = open_lock(&paths.lock).unwrap();
		lock.lock_exclusive().unwrap();
		ensure_profile(&paths).unwrap();
		ensure_artifacts(&paths).unwrap();
		assert!(paths.marker.is_file());
		assert!(paths.pki_dir.is_dir());
		drop(lock);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn reset_preserves_the_locked_inode_and_excludes_a_competitor() {
		let root = fresh_root("reset-lock");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let lock = open_lock(&paths.lock).unwrap();
		lock.lock_exclusive().unwrap();
		ensure_profile(&paths).unwrap();
		ensure_artifacts(&paths).unwrap();
		let inode_before = fs::metadata(&paths.lock).unwrap().ino();
		let barrier = Arc::new(Barrier::new(2));
		let contender_barrier = Arc::clone(&barrier);
		let lock_path = paths.lock.clone();
		let contender = thread::spawn(move || {
			contender_barrier.wait();
			let other = open_lock(&lock_path).unwrap();
			other.try_lock_exclusive().is_err()
		});
		barrier.wait();
		reset_locked_profile(&paths).unwrap();
		assert!(
			contender.join().unwrap(),
			"positive control: a second inode would incorrectly lock"
		);
		assert_eq!(fs::metadata(&paths.lock).unwrap().ino(), inode_before);
		drop(lock);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn a_locking_observer_turns_real_supervisor_startup_into_report_existing_failure() {
		let root = fresh_root("observer-lock-control");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let observer_lock = open_lock(&paths.lock).unwrap();
		observer_lock.lock_exclusive().unwrap();

		let error = run_owned(LocalOptions {
			data_dir: Some(root.clone()),
			reset: false,
		})
		.unwrap_err();
		assert!(
			error.contains("local workspace is starting under pid"),
			"the real try_lock_exclusive path must enter report_existing: {error}"
		);
		assert!(
			error.contains(&std::process::id().to_string()),
			"report_existing must retain the locking observer's identity: {error}"
		);

		drop(observer_lock);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn supervisor_source_guard_rejects_role_runtime_and_harness_construction() {
		fn forbidden(source: &str) -> bool {
			[
				"WendelonNode",
				"ToronDaemon",
				"start_with_wendelon_client_for_harness",
				"SimulatedTransport",
			]
			.iter()
			.any(|needle| source.contains(needle))
		}
		let source = include_str!("local.rs")
			.split("#[cfg(test)]")
			.next()
			.expect("production section");
		assert!(
			!forbidden(source),
			"TORON-532 forbids role runtimes in the local supervisor"
		);
		assert!(
			forbidden("let _ = ToronDaemon::start();"),
			"positive control: the TORON-532 assay must fire"
		);
	}

	#[test]
	fn shared_health_and_readiness_seams_are_used_across_products() {
		let supervisor = include_str!("local.rs")
			.split("#[cfg(test)]")
			.next()
			.expect("production supervisor");
		let polariton = include_str!("../../../../crates/polariton/src/adapters/toron/mod.rs");
		let listener = include_str!("../../toron/tests/sql_listener_test.rs");
		assert!(supervisor.contains("ReadinessProbe::new("));
		assert!(supervisor.contains("blocking_health_loopback("));
		assert!(polariton.contains("client.health().await"));
		assert!(listener.contains(".health()"));
		fn bypasses_public_health(source: &str) -> bool {
			source.contains("SqlFrame::health") || source.contains("HealthRequest {")
		}
		assert!(!bypasses_public_health(supervisor));
		assert!(!bypasses_public_health(polariton));
		assert!(
			bypasses_public_health("write_frame(SqlFrame::health(id))"),
			"positive control: the TORON-532 shared-health assay must fire"
		);
	}

	#[test]
	fn profile_lock_is_close_on_exec() {
		let root = fresh_root("cloexec");
		prepare_root_for_lock(&root).unwrap();
		let lock = open_lock(&root.join("local.lock")).unwrap();
		let flags = unsafe { libc::fcntl(lock.as_raw_fd(), libc::F_GETFD) };
		assert_ne!(flags & libc::FD_CLOEXEC, 0);
		drop(lock);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn child_is_in_its_own_process_group_and_drains_on_owned_signal() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let root = fresh_root("own-group");
		fs::create_dir_all(&root).unwrap();
		let ready = root.join("trap.ready");
		let script = format!(
			"trap 'exit 0' INT; touch {}; while :; do sleep 1; done",
			ready.display()
		);
		let mut child =
			OwnedChild::spawn("fixture", Path::new("/bin/sh"), ["-c", &script]).unwrap();
		let child_pid = Pid::from_raw(child.child.id() as i32);
		let child_group = nix::unistd::getpgid(Some(child_pid)).unwrap();
		let parent_group = nix::unistd::getpgid(None).unwrap();
		assert_eq!(child_group, child_pid);
		assert_ne!(child_group, parent_group);
		wait_for_file(&ready);
		let report = child.stop(Signal::SIGINT, Duration::from_secs(2));
		assert_eq!(report.outcome, StopOutcome::Clean);
		assert!(child.child.try_wait().unwrap().is_some());
		fs::remove_dir_all(root).unwrap();
	}

	fn wait_for_file(path: &Path) {
		let deadline = Instant::now() + Duration::from_secs(5);
		while !path.exists() && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(5));
		}
		assert!(path.exists(), "fixture never reported {}", path.display());
	}

	#[test]
	fn a_child_ignoring_sigterm_is_killed_with_its_group_after_the_grace() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let root = fresh_root("stop-escalation");
		fs::create_dir_all(&root).unwrap();
		let ready = root.join("trap.ready");
		let script = format!(
			"trap '' TERM; touch {}; while :; do sleep 1; done",
			ready.display()
		);
		let mut child =
			OwnedChild::spawn("stubborn-fixture", Path::new("/bin/sh"), ["-c", &script]).unwrap();
		wait_for_file(&ready);
		let grace = Duration::from_millis(300);
		let started = Instant::now();
		let report = child.stop(Signal::SIGTERM, grace);
		let elapsed = started.elapsed();
		assert_eq!(report.outcome, StopOutcome::KilledBySupervisor);
		assert!(report.sigkill_sent);
		assert!(report.outcome.needs_recovery());
		assert_eq!(
			report.outcome.to_string(),
			"unclean stop (killed by supervisor)"
		);
		assert!(elapsed >= grace, "escalated before the grace: {elapsed:?}");
		assert!(
			elapsed < grace + Duration::from_secs(3),
			"escalation exceeded grace + slack: {elapsed:?}"
		);
		assert!(child.readers.is_empty(), "readers drained or detached");
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn a_child_exiting_86_is_unclean_restart_into_recovery_not_a_failure() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let mut child =
			OwnedChild::spawn("unclean-fixture", Path::new("/bin/sh"), ["-c", "exit 86"]).unwrap();
		let status = child
			.wait_bounded(Duration::from_secs(5))
			.expect("fixture exits");
		assert_eq!(status.code(), Some(UNCLEAN_SHUTDOWN_EXIT_CODE));
		assert_eq!(
			StopOutcome::classify(Some(status), false),
			StopOutcome::UncleanRestartIntoRecovery
		);
		// Stopping an already-exited child signals nothing and keeps the class.
		let report = child.stop(Signal::SIGTERM, Duration::from_secs(5));
		assert_eq!(report.outcome, StopOutcome::UncleanRestartIntoRecovery);
		assert!(!report.sigkill_sent);
		assert!(report.outcome.needs_recovery());
		assert!(report.outcome.to_string().contains("restart into recovery"));

		// Positive control: an ordinary failure status is not the recovery class.
		let mut failing =
			OwnedChild::spawn("failing-fixture", Path::new("/bin/sh"), ["-c", "exit 1"]).unwrap();
		let status = failing
			.wait_bounded(Duration::from_secs(5))
			.expect("fixture exits");
		failing.drain_readers(READER_DRAIN_DEADLINE);
		assert_eq!(
			StopOutcome::classify(Some(status), false),
			StopOutcome::Failed(status)
		);
		assert!(!StopOutcome::Failed(status).needs_recovery());
	}

	#[test]
	fn a_child_exiting_promptly_on_sigterm_is_a_clean_stop_without_sigkill() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let root = fresh_root("stop-clean");
		fs::create_dir_all(&root).unwrap();
		let ready = root.join("trap.ready");
		let script = format!(
			"trap 'exit 0' TERM; touch {}; while :; do sleep 1; done",
			ready.display()
		);
		let mut child =
			OwnedChild::spawn("polite-fixture", Path::new("/bin/sh"), ["-c", &script]).unwrap();
		wait_for_file(&ready);
		let grace = Duration::from_secs(10);
		let started = Instant::now();
		let report = child.stop(Signal::SIGTERM, grace);
		assert_eq!(report.outcome, StopOutcome::Clean);
		assert!(!report.sigkill_sent);
		assert!(!report.outcome.needs_recovery());
		assert!(started.elapsed() < grace, "clean stop waited out the grace");
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn wendelon_grace_covers_twice_the_storage_timeout_plus_margin() {
		assert_eq!(
			grace_for_storage_shutdown_timeout(Duration::from_secs(30)),
			DEFAULT_WENDELON_STOP_GRACE
		);
		assert_eq!(
			wendelon_stop_grace(Path::new("/nonexistent/wendelon.toml")),
			DEFAULT_WENDELON_STOP_GRACE
		);
		assert_eq!(
			grace_for_storage_shutdown_timeout(Duration::from_secs(5)),
			Duration::from_secs(50)
		);
	}

	#[test]
	fn owned_shutdown_finishes_toron_before_signaling_wendelon() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let root = fresh_root("shutdown-order");
		fs::create_dir_all(&root).unwrap();
		let log = root.join("signals.log");
		let toron_ready = root.join("toron.ready");
		let wendelon_ready = root.join("wendelon.ready");
		let toron_script = format!(
			"trap 'echo toron-begin >> {0}; sleep 0.1; echo toron-end >> {0}; exit 0' INT; touch {1}; while :; do sleep 1; done",
			log.display(),
			toron_ready.display()
		);
		let wendelon_script = format!(
			"trap 'echo wendelon-signal >> {0}; exit 0' INT; touch {1}; while :; do sleep 1; done",
			log.display(),
			wendelon_ready.display()
		);
		let mut toron =
			OwnedChild::spawn("toron-fixture", Path::new("/bin/sh"), ["-c", &toron_script])
				.unwrap();
		let mut wendelon = OwnedChild::spawn(
			"wendelon-fixture",
			Path::new("/bin/sh"),
			["-c", &wendelon_script],
		)
		.unwrap();
		let deadline = Instant::now() + Duration::from_secs(2);
		while (!toron_ready.exists() || !wendelon_ready.exists()) && Instant::now() < deadline {
			thread::sleep(Duration::from_millis(5));
		}
		assert!(toron_ready.exists() && wendelon_ready.exists());
		toron.stop(Signal::SIGINT, Duration::from_secs(2));
		wendelon.stop(Signal::SIGINT, Duration::from_secs(2));
		assert_eq!(
			fs::read_to_string(&log)
				.unwrap()
				.lines()
				.collect::<Vec<_>>(),
			["toron-begin", "toron-end", "wendelon-signal"]
		);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn stale_manifest_with_live_recycled_pid_has_no_lock_authority() {
		let _serial = lock_serial(&PROCESS_AND_LOCK_TEST_LOCK);
		let root = fresh_root("stale-pid");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let lock = open_lock(&paths.lock).unwrap();
		lock.lock_exclusive().unwrap();
		let marker = ensure_profile(&paths).unwrap();
		let runtime = RuntimeManifest {
			version: FORMAT_VERSION,
			profile_uuid: marker.profile_uuid,
			session_nonce: Uuid::now_v7(),
			pid: std::process::id(),
			process_start_token: process_start_token(std::process::id()).unwrap(),
			started_unix_ms: 0,
			wendelon_endpoint: WENDELON_ENDPOINT.to_owned(),
			sql_endpoint: SQL_ENDPOINT.to_owned(),
		};
		publish_runtime_manifest(&paths.runtime, &runtime).unwrap();
		drop(lock);
		let successor = open_lock(&paths.lock).unwrap();
		assert!(
			successor.try_lock_exclusive().is_ok(),
			"the held lock, never the live-looking PID, is authority"
		);
		drop(successor);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn reset_refuses_root_home_foreign_and_symlinked_generated_content() {
		let root_paths = ProfilePaths::new(PathBuf::from("/"));
		assert!(
			reset_locked_profile(&root_paths)
				.unwrap_err()
				.contains("filesystem root")
		);

		if let Some(home) = std::env::var_os("HOME") {
			let home_paths = ProfilePaths::new(PathBuf::from(home));
			assert!(
				reset_locked_profile(&home_paths)
					.unwrap_err()
					.contains("home directory")
			);
		}

		let foreign = fresh_root("foreign");
		fs::create_dir_all(&foreign).unwrap();
		fs::write(foreign.join("mine.txt"), b"not nilket").unwrap();
		let foreign_paths = ProfilePaths::new(foreign.clone());
		assert!(validate_profile_root_candidate(&foreign_paths).is_err());
		assert!(!foreign.join("local.lock").exists());
		fs::remove_dir_all(foreign).unwrap();

		let root = fresh_root("symlink-reset");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let lock = open_lock(&paths.lock).unwrap();
		lock.lock_exclusive().unwrap();
		ensure_profile(&paths).unwrap();
		ensure_artifacts(&paths).unwrap();
		let escape = fresh_root("escape");
		fs::create_dir_all(&escape).unwrap();
		std::os::unix::fs::symlink(&escape, paths.wendelon_data.join("escape")).unwrap();
		assert!(
			reset_locked_profile(&paths)
				.unwrap_err()
				.contains("symlink")
		);
		drop(lock);
		fs::remove_dir_all(root).unwrap();
		fs::remove_dir_all(escape).unwrap();
	}

	#[test]
	fn reset_refuses_a_live_wendelon_owner_without_changing_generated_bytes() {
		let root = fresh_root("reset-live-wendelon-owner");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let profile_lock = open_lock(&paths.lock).unwrap();
		profile_lock.lock_exclusive().unwrap();
		ensure_profile(&paths).unwrap();
		ensure_artifacts(&paths).unwrap();
		fs::write(&paths.wendelon_config, b"config-before-reset").unwrap();
		fs::write(paths.wendelon_data.join("state.bin"), b"state-before-reset").unwrap();
		let data_owner = DataDirectoryOwner::acquire_existing(&paths.wendelon_data).unwrap();

		let error = reset_locked_profile(&paths).unwrap_err();
		assert!(
			error.contains("already owned"),
			"unexpected reset error: {error}"
		);
		assert_eq!(
			fs::read(&paths.wendelon_config).unwrap(),
			b"config-before-reset"
		);
		assert_eq!(
			fs::read(paths.wendelon_data.join("state.bin")).unwrap(),
			b"state-before-reset"
		);

		drop(data_owner);
		drop(profile_lock);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn successful_reset_preserves_the_wendelon_lock_inode() {
		let root = fresh_root("reset-preserves-wendelon-lock");
		let paths = ProfilePaths::new(root.clone());
		prepare_root_for_lock(&root).unwrap();
		let profile_lock = open_lock(&paths.lock).unwrap();
		profile_lock.lock_exclusive().unwrap();
		ensure_profile(&paths).unwrap();
		ensure_artifacts(&paths).unwrap();
		fs::write(paths.wendelon_data.join("state.bin"), b"old-state").unwrap();
		let owner = DataDirectoryOwner::acquire_existing(&paths.wendelon_data).unwrap();
		let lock_inode = fs::metadata(paths.wendelon_data.join("LOCK"))
			.unwrap()
			.ino();
		drop(owner);

		reset_locked_profile(&paths).unwrap();

		assert_eq!(
			fs::metadata(paths.wendelon_data.join("LOCK"))
				.unwrap()
				.ino(),
			lock_inode
		);
		assert!(!paths.wendelon_data.join("state.bin").exists());
		assert!(!paths.wendelon_config.exists());
		drop(profile_lock);
		fs::remove_dir_all(root).unwrap();
	}
}
