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
use std::process::{Child, Command, ExitCode, Stdio};
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

const READY_ENDPOINT: &str = "127.0.0.1:7432";
const STARTUP_TIMEOUT: Duration = Duration::from_secs(300);
const POLL_INTERVAL: Duration = Duration::from_millis(100);
const TORON_DRAIN_TIMEOUT: Duration = Duration::from_secs(35);
const WENDELON_DRAIN_TIMEOUT: Duration = Duration::from_secs(35);
const MAX_CHILD_LINE_BYTES: usize = 16 * 1024;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LocalOptions {
	pub data_dir: Option<PathBuf>,
	pub reset: bool,
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
	let marker = ensure_profile(&paths)?;
	let pki = ensure_artifacts(&paths)?;
	validate_artifacts(&paths)?;
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
		wendelon.stop(Signal::SIGINT, WENDELON_DRAIN_TIMEOUT);
		return Err(error);
	}

	let mut toron = match OwnedChild::spawn(
		"toron",
		&executable,
		["toron", "start", path_text(&paths.toron_config)?],
	) {
		Ok(child) => child,
		Err(error) => {
			wendelon.stop(Signal::SIGINT, WENDELON_DRAIN_TIMEOUT);
			return Err(error);
		}
	};
	if let Err(error) = wait_for_toron(&mut toron.child, SQL_ENDPOINT, &interrupted) {
		toron.stop(Signal::SIGINT, TORON_DRAIN_TIMEOUT);
		wendelon.stop(Signal::SIGINT, WENDELON_DRAIN_TIMEOUT);
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

	let exit_code = supervise(&mut toron, &mut wendelon, &interrupted, &paths);
	let _ = fs::remove_file(&paths.runtime);
	drop(lock_file);
	Ok(exit_code)
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
	eprintln!("nilket local: resetting {}", canonical.display());
	for generated in ["config", "pki", "data", "logs"] {
		let target = paths.root.join(generated);
		if target.exists() {
			refuse_symlink_tree(&target)?;
			fs::remove_dir_all(&target)
				.map_err(|error| format!("reset {}: {error}", target.display()))?;
		}
	}
	if paths.runtime.exists() {
		fs::remove_file(&paths.runtime)
			.map_err(|error| format!("reset {}: {error}", paths.runtime.display()))?;
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
	interrupted: &AtomicBool,
	paths: &ProfilePaths,
) -> ExitCode {
	loop {
		if interrupted.load(Ordering::Relaxed) {
			eprintln!("nilket local: draining Toron before Wendelon");
			toron.stop(Signal::SIGINT, TORON_DRAIN_TIMEOUT);
			wendelon.stop(Signal::SIGINT, WENDELON_DRAIN_TIMEOUT);
			return ExitCode::SUCCESS;
		}
		match toron.child.try_wait() {
			Ok(Some(status)) => {
				eprintln!(
					"nilket local: Toron exited unexpectedly ({status}); logs/profile: {}",
					paths.root.display()
				);
				wendelon.stop(Signal::SIGINT, WENDELON_DRAIN_TIMEOUT);
				return ExitCode::from(1);
			}
			Err(error) => {
				eprintln!("nilket local: failed to poll Toron: {error}");
				wendelon.stop(Signal::SIGINT, WENDELON_DRAIN_TIMEOUT);
				return ExitCode::from(1);
			}
			Ok(None) => {}
		}
		match wendelon.child.try_wait() {
			Ok(Some(status)) => {
				eprintln!(
					"nilket local: Wendelon exited unexpectedly ({status}); logs/profile: {}",
					paths.root.display()
				);
				toron.stop(Signal::SIGINT, TORON_DRAIN_TIMEOUT);
				return ExitCode::from(1);
			}
			Err(error) => {
				eprintln!("nilket local: failed to poll Wendelon: {error}");
				toron.stop(Signal::SIGINT, TORON_DRAIN_TIMEOUT);
				return ExitCode::from(1);
			}
			Ok(None) => {}
		}
		thread::sleep(POLL_INTERVAL);
	}
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

	fn stop(&mut self, signal: Signal, timeout: Duration) {
		if self.child.try_wait().ok().flatten().is_none() {
			let _ = killpg(Pid::from_raw(self.child.id() as i32), signal);
		}
		let deadline = Instant::now() + timeout;
		while Instant::now() < deadline {
			match self.child.try_wait() {
				Ok(Some(_)) => {
					self.join_readers();
					return;
				}
				Ok(None) => thread::sleep(Duration::from_millis(50)),
				Err(error) => {
					eprintln!("nilket local: failed to reap {}: {error}", self.label);
					break;
				}
			}
		}
		eprintln!("nilket local: escalating {} after {timeout:?}", self.label);
		let _ = killpg(Pid::from_raw(self.child.id() as i32), Signal::SIGKILL);
		let _ = self.child.wait();
		self.join_readers();
	}

	fn join_readers(&mut self) {
		for reader in self.readers.drain(..) {
			let _ = reader.join();
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
		let mut child = OwnedChild::spawn(
			"fixture",
			Path::new("/bin/sh"),
			["-c", "trap 'exit 0' INT; while :; do sleep 1; done"],
		)
		.unwrap();
		let child_pid = Pid::from_raw(child.child.id() as i32);
		let child_group = nix::unistd::getpgid(Some(child_pid)).unwrap();
		let parent_group = nix::unistd::getpgid(None).unwrap();
		assert_eq!(child_group, child_pid);
		assert_ne!(child_group, parent_group);
		child.stop(Signal::SIGINT, Duration::from_secs(2));
		assert!(child.child.try_wait().unwrap().is_some());
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
}
