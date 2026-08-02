//! Session-bound creation of a local Nilket workspace (PRODUCT-0006).

use nilket_local::{LocalWorkspace, ReadyWorkspace};
use nix::sys::signal::{Signal, kill};
use nix::unistd::{Pid, getpid, getppid};
use std::collections::VecDeque;
use std::io::{self, BufRead, BufReader, Read};
use std::os::unix::fs::PermissionsExt;
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::{Arc, Mutex, mpsc};
use std::thread::{self, JoinHandle};
use std::time::{Duration, Instant};

pub(crate) const STARTUP_DEADLINE: Duration = Duration::from_secs(50);
pub(crate) const STOP_DEADLINE: Duration = Duration::from_secs(75);
const DISCOVERY_POLL: Duration = Duration::from_millis(50);
const CHILD_POLL: Duration = Duration::from_millis(20);
const MAX_DIAGNOSTIC_LINES: usize = 256;
const MAX_DIAGNOSTIC_LINE_BYTES: usize = 16 * 1024;

#[derive(Debug, Default)]
pub(crate) struct CreationGrant {
	offer: Option<PathBuf>,
	spent: bool,
}

impl CreationGrant {
	pub(crate) fn observe(&mut self, workspace: &LocalWorkspace) {
		self.offer = if !self.spent {
			match workspace {
				LocalWorkspace::Absent { profile_root } => Some(profile_root.clone()),
				_ => None,
			}
		} else {
			None
		};
	}

	pub(crate) fn offer(&self) -> Option<&Path> {
		self.offer.as_deref()
	}

	pub(crate) fn accept(&mut self) -> Option<PathBuf> {
		let root = self.offer.take()?;
		self.spent = true;
		Some(root)
	}

	#[cfg(test)]
	fn spent(&self) -> bool {
		self.spent
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ProcessPhase {
	Launching,
	Running { pid: u32 },
	Exited { status: String },
	StopDeadline,
	HandleUnavailable,
	SpawnFailed { cause: String },
}

#[derive(Debug)]
struct ProcessRecord {
	phase: ProcessPhase,
	diagnostics: VecDeque<String>,
}

impl Default for ProcessRecord {
	fn default() -> Self {
		Self {
			phase: ProcessPhase::Launching,
			diagnostics: VecDeque::new(),
		}
	}
}

#[derive(Clone, Debug)]
pub(crate) struct ProcessMonitor(Arc<Mutex<ProcessRecord>>);

impl ProcessMonitor {
	fn new() -> Self {
		Self(Arc::new(Mutex::new(ProcessRecord::default())))
	}

	fn set_phase(&self, phase: ProcessPhase) {
		self.0.lock().expect("process monitor poisoned").phase = phase;
	}

	fn push_diagnostic(&self, role: &str, line: &str) {
		let mut record = self.0.lock().expect("process monitor poisoned");
		if record.diagnostics.len() == MAX_DIAGNOSTIC_LINES {
			record.diagnostics.pop_front();
		}
		let mut line = line.trim_end().to_owned();
		if line.len() > MAX_DIAGNOSTIC_LINE_BYTES {
			let mut boundary = MAX_DIAGNOSTIC_LINE_BYTES;
			while !line.is_char_boundary(boundary) {
				boundary -= 1;
			}
			line.truncate(boundary);
			line.push('…');
		}
		record.diagnostics.push_back(format!("{role}: {line}"));
	}

	fn terminal_detail(&self) -> Option<String> {
		let record = self.0.lock().expect("process monitor poisoned");
		let phase = match &record.phase {
			ProcessPhase::Exited { status } => format!("Nilket supervisor exited: {status}"),
			ProcessPhase::StopDeadline => {
				"Nilket supervisor did not exit before the shutdown deadline".to_owned()
			}
			ProcessPhase::HandleUnavailable => {
				"Nilket supervisor handle became unavailable during shutdown".to_owned()
			}
			ProcessPhase::SpawnFailed { cause } => cause.clone(),
			ProcessPhase::Launching | ProcessPhase::Running { .. } => return None,
		};
		if record.diagnostics.is_empty() {
			Some(phase)
		} else {
			Some(format!(
				"{phase}\n{}",
				record
					.diagnostics
					.iter()
					.cloned()
					.collect::<Vec<_>>()
					.join("\n")
			))
		}
	}

	#[cfg(test)]
	fn phase(&self) -> ProcessPhase {
		self.0.lock().unwrap().phase.clone()
	}
}

enum OwnerCommand {
	Stop {
		deadline: Duration,
		finished: mpsc::SyncSender<StopOutcome>,
	},
	#[cfg(test)]
	Abandon { finished: mpsc::SyncSender<()> },
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StopOutcome {
	Exited(String),
	Deadline,
	Unavailable,
}

pub(crate) struct LocalWorkspaceOwner {
	commands: mpsc::Sender<OwnerCommand>,
	monitor: ProcessMonitor,
}

impl LocalWorkspaceOwner {
	pub(crate) fn launch(profile_root: PathBuf) -> Self {
		let monitor = ProcessMonitor::new();
		let (commands, receiver) = mpsc::channel();
		let thread_monitor = monitor.clone();
		thread::Builder::new()
			.name("polariton-local-workspace-owner".to_owned())
			.spawn(move || {
				let executable = match resolve_nilket_executable() {
					Ok(executable) => executable,
					Err(cause) => {
						thread_monitor.set_phase(ProcessPhase::SpawnFailed { cause });
						return;
					}
				};
				let mut command = Command::new(executable);
				command.args(["local", "--data-dir"]);
				command.arg(profile_root);
				owning_thread_main(command, receiver, thread_monitor, Signal::SIGINT);
			})
			.expect("spawn dedicated local workspace owner thread");
		Self { commands, monitor }
	}

	pub(crate) fn monitor(&self) -> ProcessMonitor {
		self.monitor.clone()
	}

	pub(crate) fn stop(self, deadline: Duration) -> StopOutcome {
		let (finished, result) = mpsc::sync_channel(1);
		if self
			.commands
			.send(OwnerCommand::Stop { deadline, finished })
			.is_err()
		{
			return StopOutcome::Unavailable;
		}
		result
			.recv_timeout(deadline + Duration::from_secs(1))
			.unwrap_or(StopOutcome::Deadline)
	}

	#[cfg(test)]
	pub(crate) fn launch_command(command: Command, signal: Signal) -> Self {
		let monitor = ProcessMonitor::new();
		let (commands, receiver) = mpsc::channel();
		let thread_monitor = monitor.clone();
		thread::spawn(move || owning_thread_main(command, receiver, thread_monitor, signal));
		Self { commands, monitor }
	}

	#[cfg(test)]
	fn abandon(self) {
		let (finished, result) = mpsc::sync_channel(1);
		self.commands
			.send(OwnerCommand::Abandon { finished })
			.expect("owner thread accepts abandon");
		result
			.recv_timeout(Duration::from_secs(2))
			.expect("owner thread exits");
	}
}

fn owning_thread_main(
	mut command: Command,
	receiver: mpsc::Receiver<OwnerCommand>,
	monitor: ProcessMonitor,
	parent_death_signal: Signal,
) {
	let parent = getpid();
	command
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.process_group(0);
	arm_parent_death(&mut command, parent, parent_death_signal);
	let mut child = match command.spawn() {
		Ok(child) => child,
		Err(error) => {
			monitor.set_phase(ProcessPhase::SpawnFailed {
				cause: format!("spawn Nilket local workspace: {error}"),
			});
			return;
		}
	};
	monitor.set_phase(ProcessPhase::Running { pid: child.id() });
	let readers = spawn_readers(&mut child, &monitor);
	loop {
		match receiver.recv_timeout(CHILD_POLL) {
			Ok(OwnerCommand::Stop { deadline, finished }) => {
				let outcome = stop_child(&mut child, deadline);
				monitor.set_phase(outcome_phase(&outcome));
				finish_readers(&outcome, readers);
				let _ = finished.send(outcome);
				return;
			}
			#[cfg(test)]
			Ok(OwnerCommand::Abandon { finished }) => {
				let _ = finished.send(());
				return;
			}
			Err(mpsc::RecvTimeoutError::Disconnected) => {
				let outcome = stop_child(&mut child, STOP_DEADLINE);
				monitor.set_phase(outcome_phase(&outcome));
				finish_readers(&outcome, readers);
				return;
			}
			Err(mpsc::RecvTimeoutError::Timeout) => {}
		}
		match child.try_wait() {
			Ok(Some(status)) => {
				monitor.set_phase(ProcessPhase::Exited {
					status: status.to_string(),
				});
				join_readers(readers);
				return;
			}
			Ok(None) => {}
			Err(error) => {
				monitor.set_phase(ProcessPhase::Exited {
					status: format!("poll failed: {error}"),
				});
				return;
			}
		}
	}
}

fn arm_parent_death(command: &mut Command, parent: Pid, parent_death_signal: Signal) {
	// SAFETY: only async-signal-safe Linux syscalls run between fork and exec.
	// The parent recheck closes the fork/configure race.
	unsafe {
		command.pre_exec(move || {
			nix::sys::prctl::set_pdeathsig(Some(parent_death_signal)).map_err(io::Error::other)?;
			if getppid() != parent {
				return Err(io::Error::other(
					"Polariton initiator exited before Nilket exec",
				));
			}
			Ok(())
		});
	}
}

fn stop_child(child: &mut Child, deadline: Duration) -> StopOutcome {
	if let Ok(Some(status)) = child.try_wait() {
		return StopOutcome::Exited(status.to_string());
	}
	if kill(Pid::from_raw(child.id() as i32), Signal::SIGINT).is_err() {
		return child
			.try_wait()
			.ok()
			.flatten()
			.map(|status| StopOutcome::Exited(status.to_string()))
			.unwrap_or(StopOutcome::Unavailable);
	}
	let limit = Instant::now() + deadline;
	loop {
		match child.try_wait() {
			Ok(Some(status)) => return StopOutcome::Exited(status.to_string()),
			Ok(None) if Instant::now() < limit => thread::sleep(CHILD_POLL),
			Ok(None) | Err(_) => return StopOutcome::Deadline,
		}
	}
}

fn outcome_phase(outcome: &StopOutcome) -> ProcessPhase {
	match outcome {
		StopOutcome::Exited(status) => ProcessPhase::Exited {
			status: status.clone(),
		},
		StopOutcome::Deadline => ProcessPhase::StopDeadline,
		StopOutcome::Unavailable => ProcessPhase::HandleUnavailable,
	}
}

fn spawn_readers(child: &mut Child, monitor: &ProcessMonitor) -> Vec<JoinHandle<()>> {
	let mut readers = Vec::new();
	if let Some(stdout) = child.stdout.take() {
		readers.push(spawn_reader("nilket", stdout, monitor.clone()));
	}
	if let Some(stderr) = child.stderr.take() {
		readers.push(spawn_reader("nilket", stderr, monitor.clone()));
	}
	readers
}

fn spawn_reader<R>(role: &'static str, file: R, monitor: ProcessMonitor) -> JoinHandle<()>
where
	R: Read + Send + 'static,
{
	thread::spawn(move || {
		for line in BufReader::new(file).lines() {
			match line {
				Ok(line) => monitor.push_diagnostic(role, &line),
				Err(error) => {
					monitor.push_diagnostic(role, &format!("read attributed output: {error}"));
					break;
				}
			}
		}
	})
}

fn join_readers(readers: Vec<JoinHandle<()>>) {
	for reader in readers {
		let _ = reader.join();
	}
}

fn finish_readers(outcome: &StopOutcome, readers: Vec<JoinHandle<()>>) {
	if matches!(outcome, StopOutcome::Exited(_)) {
		join_readers(readers);
	}
}

fn resolve_nilket_executable() -> Result<PathBuf, String> {
	let current = std::env::current_exe()
		.map_err(|error| format!("resolve current Polariton executable: {error}"))?;
	if let Some(sibling) = current.parent().map(|parent| parent.join("nilket"))
		&& is_executable(&sibling)
	{
		return Ok(sibling);
	}
	if let Some(path) = std::env::var_os("PATH") {
		for directory in std::env::split_paths(&path) {
			let candidate = directory.join("nilket");
			if is_executable(&candidate) {
				return Ok(candidate);
			}
		}
	}
	Err(
		"resolve nilket: no sibling or PATH executable; this sibling-then-PATH rule is shared with PRODUCT-0004 and must be extracted before a third consumer"
			.to_owned(),
	)
}

fn is_executable(path: &Path) -> bool {
	path.metadata()
		.map(|metadata| metadata.is_file() && metadata.permissions().mode() & 0o111 != 0)
		.unwrap_or(false)
}

#[derive(Debug, Clone)]
pub(crate) enum StartupOutcome {
	Ready(ReadyWorkspace),
	Failed {
		observed: LocalWorkspace,
		cause: String,
	},
}

pub(crate) async fn watch_until_ready(
	profile_root: PathBuf,
	monitor: ProcessMonitor,
	deadline: Duration,
) -> StartupOutcome {
	let limit = Instant::now() + deadline;
	loop {
		let observed = nilket_local::discover(Some(&profile_root)).await;
		match observed {
			LocalWorkspace::Ready(ready) => return StartupOutcome::Ready(ready),
			LocalWorkspace::Absent { .. } | LocalWorkspace::Starting { .. } => {}
			other => {
				return StartupOutcome::Failed {
					cause: other.to_string(),
					observed: other,
				};
			}
		}
		if let Some(cause) = monitor.terminal_detail() {
			let observed = nilket_local::discover(Some(&profile_root)).await;
			return StartupOutcome::Failed { observed, cause };
		}
		if Instant::now() >= limit {
			let observed = nilket_local::discover(Some(&profile_root)).await;
			return StartupOutcome::Failed {
				cause: format!(
					"local workspace did not become Ready before the creation deadline; last observation: {observed}"
				),
				observed,
			};
		}
		tokio::time::sleep(DISCOVERY_POLL).await;
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use nix::sys::wait::{WaitPidFlag, waitpid};
	use std::fs;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::time::{SystemTime, UNIX_EPOCH};

	const INITIATOR_HELPER_ROOT: &str = "POLARITON_PRODUCT_0006_INITIATOR_ROOT";
	const SUPERVISOR_HELPER_ROOT: &str = "POLARITON_PRODUCT_0006_SUPERVISOR_ROOT";
	static SUPERVISOR_INTERRUPTED: AtomicBool = AtomicBool::new(false);

	fn fresh_root(label: &str) -> PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.unwrap()
			.as_nanos();
		let root = std::env::temp_dir().join(format!(
			"polariton-product-0006-{label}-{}-{nonce}",
			std::process::id()
		));
		fs::create_dir_all(&root).unwrap();
		root
	}

	fn signal_fixture(root: &Path) -> (Command, PathBuf, PathBuf, PathBuf) {
		let signal = root.join("signal");
		let drain = root.join("drain");
		let ready = root.join("ready");
		let script = r#"
trap 'printf INT > "$1"; printf "toron-end\nwendelon-signal\n" > "$2"; exit 0' INT
trap 'printf TERM > "$1"; exit 0' TERM
: > "$3"
while :; do sleep 0.02; done
"#;
		let mut command = Command::new("/bin/sh");
		command
			.arg("-c")
			.arg(script)
			.arg("product-0006-fixture")
			.arg(&signal)
			.arg(&drain)
			.arg(&ready);
		(command, signal, drain, ready)
	}

	fn wait_for(path: &Path) {
		let limit = Instant::now() + Duration::from_secs(3);
		while !path.exists() && Instant::now() < limit {
			thread::sleep(Duration::from_millis(5));
		}
		assert!(path.exists(), "fixture did not publish {}", path.display());
	}

	fn wait_running(monitor: &ProcessMonitor) -> u32 {
		let limit = Instant::now() + Duration::from_secs(3);
		loop {
			match monitor.phase() {
				ProcessPhase::Running { pid } => return pid,
				ProcessPhase::SpawnFailed { cause } => panic!("fixture spawn failed: {cause}"),
				phase if Instant::now() >= limit => panic!("fixture did not run: {phase:?}"),
				_ => thread::sleep(Duration::from_millis(5)),
			}
		}
	}

	fn wait_gone(pid: u32, start_token: &str) {
		let path = PathBuf::from(format!("/proc/{pid}"));
		let limit = Instant::now() + Duration::from_secs(3);
		while Instant::now() < limit {
			match nilket_local::process_start_token(pid) {
				Err(_) => return,
				Ok(token) if token != start_token => return,
				Ok(_) => {}
			}
			if fs::read_to_string(format!("/proc/{pid}/stat"))
				.ok()
				.and_then(|stat| stat.rsplit_once(')').map(|(_, fields)| fields.to_owned()))
				.and_then(|fields| fields.split_ascii_whitespace().next().map(str::to_owned))
				.as_deref() == Some("Z")
			{
				let _ = waitpid(Pid::from_raw(pid as i32), Some(WaitPidFlag::WNOHANG));
				return;
			}
			thread::sleep(Duration::from_millis(5));
		}
		assert!(
			!path.exists(),
			"fixture pid {pid} survived with its original identity"
		);
	}

	#[test]
	fn supervisor_process_helper() {
		let Some(root) = std::env::var_os(SUPERVISOR_HELPER_ROOT) else {
			return;
		};
		let root = PathBuf::from(root);
		let pids = root.join("pids");
		extern "C" fn interrupt(_: nix::libc::c_int) {
			SUPERVISOR_INTERRUPTED.store(true, Ordering::SeqCst);
		}
		let action = nix::sys::signal::SigAction::new(
			nix::sys::signal::SigHandler::Handler(interrupt),
			nix::sys::signal::SaFlags::empty(),
			nix::sys::signal::SigSet::empty(),
		);
		// SAFETY: the test helper installs one async-signal-safe atomic handler
		// before it creates any additional Rust thread.
		unsafe { nix::sys::signal::sigaction(Signal::SIGINT, &action) }.unwrap();
		let mut role = Command::new("/bin/sh")
			.args(["-c", "while :; do sleep 1; done"])
			.stdin(Stdio::null())
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.unwrap();
		fs::write(&pids, format!("{} {}", std::process::id(), role.id())).unwrap();
		while !SUPERVISOR_INTERRUPTED.load(Ordering::SeqCst) {
			thread::sleep(Duration::from_millis(5));
		}
		let _ = kill(Pid::from_raw(role.id() as i32), Signal::SIGTERM);
		let _ = role.wait();
	}

	#[test]
	fn initiator_process_helper() {
		let Some(root) = std::env::var_os(INITIATOR_HELPER_ROOT) else {
			return;
		};
		let mut command = Command::new(std::env::current_exe().unwrap());
		command
			.args([
				"--exact",
				"local_workspace::tests::supervisor_process_helper",
				"--nocapture",
			])
			.env(SUPERVISOR_HELPER_ROOT, root)
			.stdin(Stdio::null());
		let _owner = LocalWorkspaceOwner::launch_command(command, Signal::SIGINT);
		loop {
			thread::sleep(Duration::from_secs(1));
		}
	}

	#[test]
	fn killing_the_initiator_leaves_no_supervisor_or_role() {
		let root = fresh_root("initiator-death");
		let pids = root.join("pids");
		let mut initiator = Command::new(std::env::current_exe().unwrap())
			.args([
				"--exact",
				"local_workspace::tests::initiator_process_helper",
				"--nocapture",
			])
			.env(INITIATOR_HELPER_ROOT, &root)
			.stdin(Stdio::null())
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.spawn()
			.unwrap();
		wait_for(&pids);
		let values = fs::read_to_string(&pids).unwrap();
		let mut values = values.split_ascii_whitespace();
		let supervisor_pid = values.next().unwrap().parse::<u32>().unwrap();
		let role_pid = values.next().unwrap().parse::<u32>().unwrap();
		assert!(values.next().is_none());
		let supervisor_token = nilket_local::process_start_token(supervisor_pid).unwrap();
		let role_token = nilket_local::process_start_token(role_pid).unwrap();
		kill(Pid::from_raw(initiator.id() as i32), Signal::SIGKILL).unwrap();
		initiator.wait().unwrap();
		wait_gone(supervisor_pid, &supervisor_token);
		wait_gone(role_pid, &role_token);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn absent_offers_exactly_one_creation_grant() {
		let root = fresh_root("grant");
		let mut grant = CreationGrant::default();
		let absent = LocalWorkspace::Absent {
			profile_root: root.clone(),
		};
		grant.observe(&absent);
		assert_eq!(grant.offer(), Some(root.as_path()));
		let spawns = AtomicUsize::new(0);
		if grant.accept().is_some() {
			spawns.fetch_add(1, Ordering::SeqCst);
		}
		if grant.accept().is_some() {
			spawns.fetch_add(1, Ordering::SeqCst);
		}
		grant.observe(&absent);
		assert!(grant.offer().is_none());
		assert!(grant.spent());
		assert_eq!(spawns.load(Ordering::SeqCst), 1);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn diagnostics_are_attributed_and_truncated_on_a_utf8_boundary() {
		let monitor = ProcessMonitor::new();
		let line = "é".repeat(MAX_DIAGNOSTIC_LINE_BYTES);
		monitor.push_diagnostic("nilket", &line);
		monitor.set_phase(ProcessPhase::Exited {
			status: "fixture exit".to_owned(),
		});
		let detail = monitor.terminal_detail().unwrap();
		assert!(detail.starts_with("Nilket supervisor exited: fixture exit\nnilket: "));
		assert!(detail.ends_with('…'));
		assert!(detail.len() <= MAX_DIAGNOSTIC_LINE_BYTES + 128);
	}

	#[test]
	fn executable_candidates_must_be_regular_and_executable() {
		let root = fresh_root("executable-candidate");
		let candidate = root.join("nilket");
		fs::write(&candidate, "fixture").unwrap();
		let mut permissions = fs::metadata(&candidate).unwrap().permissions();
		permissions.set_mode(0o600);
		fs::set_permissions(&candidate, permissions).unwrap();
		assert!(!is_executable(&candidate));
		let mut permissions = fs::metadata(&candidate).unwrap().permissions();
		permissions.set_mode(0o700);
		fs::set_permissions(&candidate, permissions).unwrap();
		assert!(is_executable(&candidate));
		assert!(!is_executable(&root));
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	#[allow(clippy::zombie_processes)] // The parent test reaps the control by PID after PDEATHSIG.
	fn dedicated_owner_survives_worker_reaping_while_the_control_dies() {
		let root = fresh_root("thread-owner");
		let (command, signal, _drain, ready) = signal_fixture(&root);
		let owner = LocalWorkspaceOwner::launch_command(command, Signal::SIGINT);
		wait_for(&ready);
		let owner_pid = wait_running(&owner.monitor());
		let owner_token = nilket_local::process_start_token(owner_pid).unwrap();
		assert_eq!(
			nix::unistd::getpgid(Some(Pid::from_raw(owner_pid as i32))).unwrap(),
			Pid::from_raw(owner_pid as i32),
			"workspace supervisor must own an isolated process group"
		);

		let control_root = fresh_root("worker-control");
		let (mut command, control_signal, _control_drain, control_ready) =
			signal_fixture(&control_root);
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(1)
			.max_blocking_threads(1)
			.thread_keep_alive(Duration::from_millis(20))
			.enable_all()
			.build()
			.unwrap();
		let (control_pid, control_token) = runtime
			.block_on(async move {
				tokio::task::spawn_blocking(move || {
					command
						.stdout(Stdio::null())
						.stderr(Stdio::null())
						.process_group(0);
					arm_parent_death(&mut command, getpid(), Signal::SIGINT);
					let child = command.spawn().unwrap();
					wait_for(&control_ready);
					let pid = child.id();
					let token = nilket_local::process_start_token(pid).unwrap();
					(pid, token)
				})
				.await
			})
			.unwrap();
		thread::sleep(Duration::from_millis(150));
		wait_for(&control_signal);
		wait_gone(control_pid, &control_token);
		assert!(
			Path::new(&format!("/proc/{owner_pid}")).exists(),
			"dedicated owner must survive unrelated worker retirement"
		);
		assert!(!signal.exists());
		let outcome = owner.stop(Duration::from_secs(2));
		assert!(matches!(outcome, StopOutcome::Exited(_)));
		wait_gone(owner_pid, &owner_token);
		drop(runtime);
		fs::remove_dir_all(root).unwrap();
		fs::remove_dir_all(control_root).unwrap();
	}

	#[test]
	fn parent_death_uses_sigint_and_preserves_drain_order() {
		let root = fresh_root("sigint");
		let (command, signal, drain, ready) = signal_fixture(&root);
		let owner = LocalWorkspaceOwner::launch_command(command, Signal::SIGINT);
		let monitor = owner.monitor();
		wait_for(&ready);
		let pid = wait_running(&monitor);
		let token = nilket_local::process_start_token(pid).unwrap();
		owner.abandon();
		wait_for(&signal);
		wait_gone(pid, &token);
		assert_eq!(fs::read_to_string(&signal).unwrap(), "INT");
		assert_eq!(
			fs::read_to_string(&drain)
				.unwrap()
				.lines()
				.collect::<Vec<_>>(),
			["toron-end", "wendelon-signal"]
		);

		let control_root = fresh_root("sigterm-control");
		let (command, signal, drain, ready) = signal_fixture(&control_root);
		let owner = LocalWorkspaceOwner::launch_command(command, Signal::SIGTERM);
		let monitor = owner.monitor();
		wait_for(&ready);
		let pid = wait_running(&monitor);
		let token = nilket_local::process_start_token(pid).unwrap();
		owner.abandon();
		wait_for(&signal);
		wait_gone(pid, &token);
		assert_eq!(fs::read_to_string(signal).unwrap(), "TERM");
		assert!(!drain.exists(), "SIGTERM control must bypass ordered drain");
		fs::remove_dir_all(root).unwrap();
		fs::remove_dir_all(control_root).unwrap();
	}

	#[test]
	fn graceful_stop_waits_and_the_deadline_is_bounded() {
		let root = fresh_root("graceful");
		let (command, signal, drain, ready) = signal_fixture(&root);
		let owner = LocalWorkspaceOwner::launch_command(command, Signal::SIGINT);
		wait_for(&ready);
		let pid = wait_running(&owner.monitor());
		let token = nilket_local::process_start_token(pid).unwrap();
		let outcome = owner.stop(Duration::from_secs(2));
		assert!(matches!(outcome, StopOutcome::Exited(_)));
		wait_gone(pid, &token);
		assert_eq!(fs::read_to_string(signal).unwrap(), "INT");
		assert_eq!(
			fs::read_to_string(drain)
				.unwrap()
				.lines()
				.collect::<Vec<_>>(),
			["toron-end", "wendelon-signal"]
		);

		let stubborn_root = fresh_root("deadline");
		let ready = stubborn_root.join("ready");
		let mut command = Command::new("/bin/sh");
		command
			.arg("-c")
			.arg("trap '' INT; : > \"$1\"; while :; do sleep 1; done")
			.arg("deadline-fixture")
			.arg(&ready);
		let owner = LocalWorkspaceOwner::launch_command(command, Signal::SIGINT);
		wait_for(&ready);
		let pid = wait_running(&owner.monitor());
		let token = nilket_local::process_start_token(pid).unwrap();
		let started = Instant::now();
		assert_eq!(owner.stop(Duration::from_millis(50)), StopOutcome::Deadline);
		assert!(started.elapsed() < Duration::from_secs(1));
		let _ = kill(Pid::from_raw(pid as i32), Signal::SIGKILL);
		wait_gone(pid, &token);
		fs::remove_dir_all(root).unwrap();
		fs::remove_dir_all(stubborn_root).unwrap();
	}

	#[test]
	fn readiness_requires_discovery_and_times_out_while_absent() {
		let root = fresh_root("readiness-timeout");
		let monitor = ProcessMonitor::new();
		monitor.set_phase(ProcessPhase::Running {
			pid: std::process::id(),
		});
		let runtime = tokio::runtime::Builder::new_current_thread()
			.enable_all()
			.build()
			.unwrap();
		let outcome = runtime.block_on(watch_until_ready(
			root.clone(),
			monitor,
			Duration::from_millis(10),
		));
		assert!(matches!(
			outcome,
			StartupOutcome::Failed {
				observed: LocalWorkspace::Absent { .. },
				..
			}
		));
		fs::remove_dir_all(root).unwrap();
	}
}
