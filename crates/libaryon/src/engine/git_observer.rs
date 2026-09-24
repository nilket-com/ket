use super::protocol::GitRefreshMode;
use super::request::{InternalEvent, InternalSender};
use std::collections::{BTreeSet, VecDeque};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, RwLock};
use std::thread::JoinHandle;
use std::time::{Duration, Instant};

const GIT_WATCH_DEBOUNCE: Duration = Duration::from_millis(300);
const GIT_WATCH_MAX_DELAY: Duration = Duration::from_secs(1);
const GIT_WATCH_RETRY_DELAY: Duration = Duration::from_millis(50);
const GIT_WATCH_PENDING_PATH_LIMIT: usize = 256;
const GIT_WATCH_DIRECTORY_LIMIT: usize = 8_192;
/// FSEvents restarts its stream for every `watch` call, so registering each
/// directory costs milliseconds apiece and drops events between restarts.
/// macOS registers one recursive watch per outermost directory and filters
/// events down to the directory set other platforms watch individually.
const GIT_WATCH_RECURSIVE: bool = cfg!(target_os = "macos");

enum GitWatchMessage {
	SetRoot(Option<PathBuf>),
	Event,
	Shutdown,
}

#[derive(Default)]
struct PendingGitWatchEvents {
	paths: Vec<PathBuf>,
	overflowed: bool,
	topology_changed: bool,
}

struct GitWatchState {
	root: PathBuf,
	/// The symlink-resolved root when it differs from `root`. FSEvents reports
	/// resolved paths, so macOS temp roots (`/var` → `/private/var`) and roots
	/// opened through a symlink need their events mapped back onto `root`.
	resolved_root: Option<PathBuf>,
	git_dir: Option<PathBuf>,
	watched_directories: BTreeSet<PathBuf>,
	/// The paths actually handed to the watcher.
	registered_watches: BTreeSet<PathBuf>,
}

impl GitWatchState {
	fn new(root: PathBuf) -> Self {
		let resolved_root = std::fs::canonicalize(&root)
			.ok()
			.filter(|resolved| resolved != &root);
		Self {
			root,
			resolved_root,
			git_dir: None,
			watched_directories: BTreeSet::new(),
			registered_watches: BTreeSet::new(),
		}
	}

	/// `directory` as the watcher reports it: resolved under the root.
	fn event_spelling(&self, directory: &Path) -> PathBuf {
		match (&self.resolved_root, directory.strip_prefix(&self.root)) {
			(Some(resolved), Ok(relative)) => resolved.join(relative),
			_ => directory.to_path_buf(),
		}
	}

	fn localize_event_paths(&self, batch: &mut PendingGitWatchEvents) {
		let Some(resolved) = &self.resolved_root else {
			return;
		};
		for path in &mut batch.paths {
			if let Ok(relative) = path.strip_prefix(resolved) {
				*path = self.root.join(relative);
			}
		}
	}
}

struct GitObserver {
	requested_root: Option<PathBuf>,
	messages: std::sync::mpsc::Sender<GitWatchMessage>,
	observation_available: Arc<AtomicBool>,
	#[cfg(test)]
	active_root: Arc<Mutex<Option<PathBuf>>>,
	thread: Option<JoinHandle<()>>,
}

impl GitObserver {
	fn spawn(internal: InternalSender) -> Self {
		let (messages, message_rx) = std::sync::mpsc::channel();
		let active_root = Arc::new(Mutex::new(None));
		let worker_active_root = active_root.clone();
		let observation_available = Arc::new(AtomicBool::new(false));
		let worker_observation_available = observation_available.clone();
		let worker_messages = messages.clone();
		let thread = std::thread::Builder::new()
			.name("baryon-git-watch".to_string())
			.spawn(move || {
				run_git_observer(
					internal,
					worker_messages,
					message_rx,
					worker_active_root,
					worker_observation_available,
				);
			})
			.ok();
		Self {
			requested_root: None,
			messages,
			observation_available,
			#[cfg(test)]
			active_root,
			thread,
		}
	}

	fn set_root(&mut self, root: Option<PathBuf>) {
		if self.requested_root == root {
			return;
		}
		self.requested_root = root.clone();
		let _ = self.messages.send(GitWatchMessage::SetRoot(root));
	}

	fn observation_available(&self) -> bool {
		self.observation_available.load(Ordering::Acquire)
	}

	#[cfg(test)]
	fn is_watching(&self, root: &Path) -> bool {
		self.active_root
			.lock()
			.is_ok_and(|active| active.as_deref() == Some(root))
	}

	#[cfg(test)]
	fn thread_id(&self) -> Option<std::thread::ThreadId> {
		self.thread.as_ref().map(|thread| thread.thread().id())
	}
}

impl Drop for GitObserver {
	fn drop(&mut self) {
		let _ = self.messages.send(GitWatchMessage::Shutdown);
		// The worker can be inside Git or a filesystem call on a slow mount.
		// Dropping the handle detaches it so engine shutdown never waits on it.
		self.thread.take();
	}
}

pub(super) struct GitRefreshController {
	internal: InternalSender,
	observer: Option<GitObserver>,
	poll_root: Option<PathBuf>,
	poll_period: Option<Duration>,
	poll_deadline: Option<Instant>,
}

impl GitRefreshController {
	pub(super) fn new(internal: InternalSender) -> Self {
		Self {
			internal,
			observer: None,
			poll_root: None,
			poll_period: None,
			poll_deadline: None,
		}
	}

	pub(super) fn reconcile(&mut self, root: Option<PathBuf>, mode: GitRefreshMode) {
		let watch_root = mode.watch_enabled().then(|| root.clone()).flatten();
		if self.observer.is_none() && watch_root.is_some() {
			self.observer = Some(GitObserver::spawn(self.internal.clone()));
		}
		if let Some(observer) = self.observer.as_mut() {
			observer.set_root(watch_root);
		}

		let observation_available = self
			.observer
			.as_ref()
			.is_some_and(GitObserver::observation_available);
		self.reconcile_poll(root, mode, observation_available, Instant::now());
	}

	fn reconcile_poll(
		&mut self,
		root: Option<PathBuf>,
		mode: GitRefreshMode,
		observation_available: bool,
		now: Instant,
	) {
		let poll_period = root
			.as_ref()
			.and_then(|_| mode.poll_seconds(observation_available))
			.map(Duration::from_secs);
		if self.poll_root != root || self.poll_period != poll_period {
			self.poll_root = root;
			self.poll_period = poll_period;
			self.poll_deadline = poll_period.map(|period| now + period);
		}
	}

	pub(super) fn queue_poll_if_due(&mut self, now: Instant) -> Result<(), ()> {
		if !self.poll_deadline.is_some_and(|deadline| now >= deadline) {
			return Ok(());
		}
		match self.internal.try_send(InternalEvent::RefreshGitStatus) {
			Ok(()) => {
				self.poll_deadline = self.poll_period.map(|period| now + period);
				Ok(())
			}
			Err(std::sync::mpsc::TrySendError::Full(_)) => {
				self.poll_deadline = Some(now + GIT_WATCH_RETRY_DELAY);
				Ok(())
			}
			Err(std::sync::mpsc::TrySendError::Disconnected(_)) => Err(()),
		}
	}

	pub(super) fn poll_deadline(&self) -> Option<Instant> {
		self.poll_deadline
	}
}

fn git_path_from_bytes(bytes: &[u8]) -> PathBuf {
	#[cfg(unix)]
	{
		use std::os::unix::ffi::OsStringExt;
		PathBuf::from(std::ffi::OsString::from_vec(bytes.to_vec()))
	}
	#[cfg(not(unix))]
	{
		PathBuf::from(String::from_utf8_lossy(bytes).into_owned())
	}
}

fn git_path_bytes(path: &Path) -> Vec<u8> {
	#[cfg(unix)]
	{
		use std::os::unix::ffi::OsStrExt;
		path.as_os_str().as_bytes().to_vec()
	}
	#[cfg(not(unix))]
	{
		path.to_string_lossy().as_bytes().to_vec()
	}
}

fn git_ignored_paths(root: &Path, paths: &[PathBuf]) -> Option<BTreeSet<PathBuf>> {
	use std::io::Write;
	use std::process::Stdio;

	if paths.is_empty() {
		return Some(BTreeSet::new());
	}
	let mut child = std::process::Command::new("git")
		.args(["check-ignore", "--stdin", "-z"])
		.current_dir(root)
		.stdin(Stdio::piped())
		.stdout(Stdio::piped())
		.stderr(Stdio::null())
		.spawn()
		.ok()?;
	let Some(mut stdin) = child.stdin.take() else {
		let _ = child.kill();
		let _ = child.wait();
		return None;
	};
	let mut input = Vec::new();
	for path in paths {
		input.extend_from_slice(&git_path_bytes(path));
		input.push(0);
	}
	let writer = std::thread::spawn(move || stdin.write_all(&input));
	let output = child.wait_with_output().ok()?;
	if writer.join().ok()?.is_err() || (!output.status.success() && output.status.code() != Some(1))
	{
		return None;
	}
	Some(
		output
			.stdout
			.split(|byte| *byte == 0)
			.filter(|path| !path.is_empty())
			.map(git_path_from_bytes)
			.collect(),
	)
}

fn git_directory(root: &Path) -> Option<PathBuf> {
	let output = std::process::Command::new("git")
		.args(["rev-parse", "--git-dir"])
		.current_dir(root)
		.output()
		.ok()?;
	if !output.status.success() {
		return None;
	}
	let mut path = output.stdout.as_slice();
	while path
		.last()
		.is_some_and(|byte| matches!(byte, b'\r' | b'\n'))
	{
		path = &path[..path.len().saturating_sub(1)];
	}
	if path.is_empty() {
		return None;
	}
	let path = git_path_from_bytes(path);
	Some(if path.is_absolute() {
		path
	} else {
		root.join(path)
	})
}

fn insert_git_ref_directories(path: &Path, directories: &mut BTreeSet<PathBuf>) {
	let mut pending = vec![path.to_path_buf()];
	while let Some(directory) = pending.pop() {
		if directories.len() >= GIT_WATCH_DIRECTORY_LIMIT || !directories.insert(directory.clone())
		{
			continue;
		}
		let Ok(entries) = std::fs::read_dir(directory) else {
			continue;
		};
		for entry in entries.flatten() {
			if entry.file_type().is_ok_and(|kind| kind.is_dir()) {
				pending.push(entry.path());
			}
		}
	}
}

fn desired_git_watch_directories(root: &Path) -> (Option<PathBuf>, BTreeSet<PathBuf>) {
	let mut directories = BTreeSet::from([root.to_path_buf()]);
	let git_dir = git_directory(root);
	if let Some(git_dir) = git_dir.as_deref() {
		directories.insert(git_dir.to_path_buf());
		insert_git_ref_directories(&git_dir.join("refs"), &mut directories);
		directories.insert(git_dir.join("info"));
	}

	let mut pending = VecDeque::from([root.to_path_buf()]);
	while !pending.is_empty() {
		if directories.len() >= GIT_WATCH_DIRECTORY_LIMIT {
			break;
		}
		let mut children = Vec::new();
		while children.len() < GIT_WATCH_PENDING_PATH_LIMIT {
			let Some(directory) = pending.pop_front() else {
				break;
			};
			let Ok(entries) = std::fs::read_dir(directory) else {
				continue;
			};
			children.extend(
				entries
					.flatten()
					.filter(|entry| entry.file_type().is_ok_and(|kind| kind.is_dir()))
					.map(|entry| entry.path())
					.filter(|path| {
						path.file_name().is_none_or(|name| name != ".git")
							&& git_dir.as_ref().is_none_or(|git_dir| path != git_dir)
					}),
			);
		}
		for batch in children.chunks(GIT_WATCH_PENDING_PATH_LIMIT) {
			let Some(ignored) = git_ignored_paths(root, batch) else {
				continue;
			};
			for child in batch {
				if directories.len() >= GIT_WATCH_DIRECTORY_LIMIT {
					break;
				}
				if !ignored.contains(child) && directories.insert(child.clone()) {
					pending.push_back(child.clone());
				}
			}
		}
	}
	(git_dir, directories)
}

fn reconcile_git_watch_directories(
	watcher: &mut notify::RecommendedWatcher,
	state: &mut GitWatchState,
	event_filter: &RwLock<Option<BTreeSet<PathBuf>>>,
) {
	use notify::Watcher;

	let (git_dir, desired) = desired_git_watch_directories(&state.root);
	let (registrations, mode) = if GIT_WATCH_RECURSIVE {
		(
			outermost_directories(&desired),
			notify::RecursiveMode::Recursive,
		)
	} else {
		(desired.clone(), notify::RecursiveMode::NonRecursive)
	};
	for directory in state
		.registered_watches
		.difference(&registrations)
		.cloned()
		.collect::<Vec<_>>()
	{
		let _ = watcher.unwatch(&directory);
		state.registered_watches.remove(&directory);
	}
	for directory in registrations
		.difference(&state.registered_watches)
		.cloned()
		.collect::<Vec<_>>()
	{
		if watcher.watch(&directory, mode).is_ok() {
			state.registered_watches.insert(directory);
		}
	}
	state.watched_directories = desired
		.into_iter()
		.filter(|directory| {
			state
				.registered_watches
				.iter()
				.any(|registered| directory.starts_with(registered))
		})
		.collect();
	if GIT_WATCH_RECURSIVE && let Ok(mut filter) = event_filter.write() {
		*filter = Some(
			state
				.watched_directories
				.iter()
				.map(|directory| state.event_spelling(directory))
				.collect(),
		);
	}
	state.git_dir = git_dir;
}

/// Whether a non-recursive watch of `directories` would report `path`: the
/// directory itself or one of its immediate entries.
fn is_directly_watched(directories: &BTreeSet<PathBuf>, path: &Path) -> bool {
	directories.contains(path)
		|| path
			.parent()
			.is_some_and(|parent| directories.contains(parent))
}

/// The members of `directories` not nested under another member. `Path`
/// ordering places every descendant directly after its ancestor.
fn outermost_directories(directories: &BTreeSet<PathBuf>) -> BTreeSet<PathBuf> {
	let mut outermost = BTreeSet::new();
	let mut ancestor: Option<&PathBuf> = None;
	for directory in directories {
		if ancestor.is_some_and(|ancestor| directory.starts_with(ancestor)) {
			continue;
		}
		outermost.insert(directory.clone());
		ancestor = Some(directory);
	}
	outermost
}

fn git_watch_batch_is_relevant(state: &GitWatchState, batch: &PendingGitWatchEvents) -> bool {
	if batch.overflowed || batch.paths.is_empty() {
		return true;
	}
	let mut worktree_paths = Vec::new();
	for path in &batch.paths {
		if state
			.git_dir
			.as_ref()
			.is_some_and(|git_dir| path.starts_with(git_dir))
		{
			return true;
		}
		if path.starts_with(&state.root) {
			worktree_paths.push(path.clone());
		}
	}
	if worktree_paths.is_empty() {
		return false;
	}
	git_ignored_paths(&state.root, &worktree_paths)
		.is_none_or(|ignored| worktree_paths.iter().any(|path| !ignored.contains(path)))
}

fn git_watch_batch_changes_topology(state: &GitWatchState, batch: &PendingGitWatchEvents) -> bool {
	batch.overflowed
		|| batch.topology_changed
		|| batch.paths.iter().any(|path| {
			path.file_name().is_some_and(|name| name == ".gitignore")
				|| state
					.git_dir
					.as_ref()
					.is_some_and(|git_dir| path == &git_dir.join("info/exclude"))
		})
}

fn publish_active_git_watch_root(
	state: Option<&GitWatchState>,
	active_root: &Mutex<Option<PathBuf>>,
) {
	if let Ok(mut active) = active_root.lock() {
		*active = state.and_then(|state| {
			state
				.watched_directories
				.contains(&state.root)
				.then(|| state.root.clone())
		});
	}
}

fn git_watch_deadline(
	first_event_at: Option<Instant>,
	last_event_at: Option<Instant>,
	retry_at: Option<Instant>,
) -> Option<Instant> {
	retry_at.or_else(|| {
		Some((last_event_at? + GIT_WATCH_DEBOUNCE).min(first_event_at? + GIT_WATCH_MAX_DELAY))
	})
}

fn run_git_observer(
	internal: InternalSender,
	messages: std::sync::mpsc::Sender<GitWatchMessage>,
	message_rx: std::sync::mpsc::Receiver<GitWatchMessage>,
	active_root: Arc<Mutex<Option<PathBuf>>>,
	observation_available: Arc<AtomicBool>,
) {
	use notify::EventKind;

	let event_queued = Arc::new(AtomicBool::new(false));
	let pending_events = Arc::new(Mutex::new(PendingGitWatchEvents::default()));
	let callback_queued = event_queued.clone();
	let callback_pending = pending_events.clone();
	let event_filter = Arc::new(RwLock::new(None::<BTreeSet<PathBuf>>));
	let callback_filter = event_filter.clone();
	let mut watcher = notify::recommended_watcher(move |event: notify::Result<notify::Event>| {
		let Ok(mut event) = event else {
			return;
		};
		// A recursive watch reports every descendant; keep what a
		// non-recursive watch of the same directories would have seen.
		if let Ok(filter) = callback_filter.read()
			&& let Some(directories) = filter.as_ref()
			&& !event.paths.is_empty()
		{
			event
				.paths
				.retain(|path| is_directly_watched(directories, path));
			if event.paths.is_empty() {
				return;
			}
		}
		let topology_changed = matches!(
			&event.kind,
			EventKind::Any
				| EventKind::Create(
					notify::event::CreateKind::Any
						| notify::event::CreateKind::Folder
						| notify::event::CreateKind::Other
				) | EventKind::Remove(
				notify::event::RemoveKind::Any
					| notify::event::RemoveKind::Folder
					| notify::event::RemoveKind::Other
			) | EventKind::Modify(notify::event::ModifyKind::Name(_))
		);
		if !matches!(
			&event.kind,
			EventKind::Any | EventKind::Create(_) | EventKind::Modify(_) | EventKind::Remove(_)
		) {
			return;
		}
		if let Ok(mut pending) = callback_pending.lock() {
			pending.topology_changed |= topology_changed;
			for path in event.paths {
				if pending.paths.len() >= GIT_WATCH_PENDING_PATH_LIMIT {
					pending.overflowed = true;
					break;
				}
				if !pending.paths.contains(&path) {
					pending.paths.push(path);
				}
			}
		}
		if !callback_queued.swap(true, Ordering::AcqRel)
			&& messages.send(GitWatchMessage::Event).is_err()
		{
			callback_queued.store(false, Ordering::Release);
		}
	})
	.ok();
	observation_available.store(watcher.is_some(), Ordering::Release);
	if internal.send(InternalEvent::GitObservationChanged).is_err() {
		return;
	}
	let mut state: Option<GitWatchState> = None;
	let mut first_event_at = None;
	let mut last_event_at = None;
	let mut retry_at = None;
	let mut watch_paths_dirty = false;
	let mut topology_followup_pending = false;

	loop {
		let deadline = git_watch_deadline(first_event_at, last_event_at, retry_at);
		if deadline.is_some_and(|deadline| Instant::now() >= deadline) {
			if watch_paths_dirty
				&& let (Some(watcher), Some(state)) = (watcher.as_mut(), state.as_mut())
			{
				reconcile_git_watch_directories(watcher, state, &event_filter);
				publish_active_git_watch_root(Some(state), &active_root);
				topology_followup_pending = true;
			}
			watch_paths_dirty = false;
			match internal.try_send(InternalEvent::RefreshGitStatus) {
				Ok(()) => {
					retry_at = None;
					if std::mem::take(&mut topology_followup_pending) {
						let now = Instant::now();
						first_event_at = Some(now);
						last_event_at = Some(now);
					} else {
						first_event_at = None;
						last_event_at = None;
					}
				}
				Err(std::sync::mpsc::TrySendError::Full(_)) => {
					retry_at = Some(Instant::now() + GIT_WATCH_RETRY_DELAY);
				}
				Err(std::sync::mpsc::TrySendError::Disconnected(_)) => return,
			}
			continue;
		}
		let message = match deadline {
			Some(deadline) => {
				message_rx.recv_timeout(deadline.saturating_duration_since(Instant::now()))
			}
			None => match message_rx.recv() {
				Ok(message) => Ok(message),
				Err(_) => return,
			},
		};
		match message {
			Ok(GitWatchMessage::SetRoot(root)) => {
				if let (Some(watcher), Some(previous)) = (watcher.as_mut(), state.take()) {
					use notify::Watcher;
					for directory in previous.registered_watches {
						let _ = watcher.unwatch(&directory);
					}
				}
				state = root.map(|root| {
					let mut state = GitWatchState::new(root);
					if let Some(watcher) = watcher.as_mut() {
						reconcile_git_watch_directories(watcher, &mut state, &event_filter);
					}
					state
				});
				// A change landing while the watches register goes unseen, so
				// one refresh follows setup.
				let catch_up = state
					.as_ref()
					.is_some_and(|state| !state.registered_watches.is_empty())
					.then(Instant::now);
				first_event_at = catch_up;
				last_event_at = catch_up;
				retry_at = None;
				watch_paths_dirty = false;
				topology_followup_pending = false;
				event_queued.store(false, Ordering::Release);
				if let Ok(mut pending) = pending_events.lock() {
					*pending = PendingGitWatchEvents::default();
				}
				publish_active_git_watch_root(state.as_ref(), &active_root);
			}
			Ok(GitWatchMessage::Event) => {
				event_queued.store(false, Ordering::Release);
				let mut batch = pending_events
					.lock()
					.map(|mut pending| std::mem::take(&mut *pending))
					.unwrap_or_default();
				if let Some(state) = state.as_ref() {
					state.localize_event_paths(&mut batch);
				}
				if let Some(state) = state
					.as_ref()
					.filter(|state| git_watch_batch_is_relevant(state, &batch))
				{
					watch_paths_dirty |= git_watch_batch_changes_topology(state, &batch);
					let now = Instant::now();
					first_event_at.get_or_insert(now);
					last_event_at = Some(now);
				}
			}
			Ok(GitWatchMessage::Shutdown) => return,
			Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
			Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return,
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::engine::request::{EngineRequest, channel};
	use std::time::{SystemTime, UNIX_EPOCH};

	fn git_fixture(label: &str) -> PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("clock after epoch")
			.as_nanos();
		let path =
			std::env::temp_dir().join(format!("libaryon-{label}-{}-{nonce}", std::process::id()));
		std::fs::create_dir_all(&path).expect("create Git fixture");
		let initialized = std::process::Command::new("git")
			.args(["init", "--quiet"])
			.current_dir(&path)
			.status()
			.expect("run git init");
		assert!(initialized.success(), "git init failed");
		path
	}

	#[test]
	fn recursive_registration_reduces_to_outermost_and_filters_to_direct_entries() {
		let directories = BTreeSet::from([
			PathBuf::from("/repo"),
			PathBuf::from("/repo/.git"),
			PathBuf::from("/repo/src"),
			PathBuf::from("/repo-sibling"),
			PathBuf::from("/worktrees/repo.git"),
			PathBuf::from("/worktrees/repo.git/refs"),
		]);
		assert_eq!(
			outermost_directories(&directories),
			BTreeSet::from([
				PathBuf::from("/repo"),
				PathBuf::from("/repo-sibling"),
				PathBuf::from("/worktrees/repo.git"),
			])
		);
		assert!(is_directly_watched(&directories, Path::new("/repo/src")));
		assert!(is_directly_watched(
			&directories,
			Path::new("/repo/src/main.rs")
		));
		assert!(is_directly_watched(&directories, Path::new("/repo/target")));
		assert!(!is_directly_watched(
			&directories,
			Path::new("/repo/target/debug/build.o")
		));
		assert!(!is_directly_watched(
			&directories,
			Path::new("/repo/src/nested/lib.rs")
		));
	}

	#[test]
	fn ignored_build_tree_churn_requests_no_refresh() {
		let repo = git_fixture("ignored-churn");
		std::fs::create_dir_all(repo.join("target/a/b")).expect("create ignored tree");
		std::fs::create_dir_all(repo.join("src")).expect("create source tree");
		std::fs::write(repo.join(".gitignore"), "target/\n").expect("write gitignore");
		let (_commands, _input, internal, requests) = channel();
		let mut observer = GitObserver::spawn(internal);
		observer.set_root(Some(repo.clone()));
		let deadline = Instant::now() + Duration::from_secs(10);
		while !observer.is_watching(&repo) && Instant::now() < deadline {
			std::thread::sleep(Duration::from_millis(5));
		}
		assert!(observer.is_watching(&repo), "observer must watch its root");
		let await_refresh = |within: Duration| {
			let deadline = Instant::now() + within;
			while Instant::now() < deadline {
				match requests.recv_timeout(Duration::from_millis(50)) {
					Ok(EngineRequest::Internal(InternalEvent::RefreshGitStatus)) => return true,
					Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
					Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return false,
				}
			}
			false
		};
		assert!(
			await_refresh(Duration::from_secs(10)),
			"watch setup must be followed by a catch-up refresh"
		);

		for index in 0..32 {
			std::fs::write(repo.join(format!("target/a/b/artifact-{index}")), "build\n")
				.expect("write ignored artifact");
		}
		assert!(
			!await_refresh(Duration::from_millis(1_500)),
			"writes inside an unwatched ignored tree must not refresh Git status"
		);
		std::fs::write(repo.join("src/main.rs"), "fn main() {}\n").expect("write source");
		assert!(
			await_refresh(Duration::from_secs(10)),
			"a relevant write must still refresh Git status"
		);

		drop(observer);
		let _ = std::fs::remove_dir_all(repo);
	}

	#[test]
	fn watch_directories_exclude_ignored_build_trees() {
		let repo = git_fixture("watch-directories");
		std::fs::create_dir_all(repo.join("src/nested")).expect("create source tree");
		std::fs::create_dir_all(repo.join("target/a/b/c/d/e")).expect("create ignored tree");
		std::fs::create_dir_all(repo.join("logs")).expect("create all-ignored-content directory");
		std::fs::create_dir_all(repo.join("empty")).expect("create empty directory");
		std::fs::write(repo.join(".gitignore"), "target/\n*.log\n").expect("write gitignore");
		std::fs::write(repo.join("src/nested/main.rs"), "fn main() {}\n").expect("write source");
		std::fs::write(repo.join("target/a/b/c/d/e/artifact"), "build\n")
			.expect("write ignored artifact");
		std::fs::write(repo.join("logs/current.log"), "ignored\n").expect("write ignored log");

		let (git_dir, directories) = desired_git_watch_directories(&repo);
		assert!(directories.contains(&repo));
		assert!(directories.contains(&repo.join("src")));
		assert!(directories.contains(&repo.join("src/nested")));
		assert!(
			directories.contains(&repo.join("logs")),
			"a directory whose contents are ignored can later gain a relevant file"
		);
		assert!(
			directories.contains(&repo.join("empty")),
			"the first file created in an empty directory must be observable"
		);
		assert!(
			directories
				.iter()
				.all(|directory| !directory.starts_with(repo.join("target"))),
			"ignored descendants must consume no watch descriptors: {directories:?}"
		);
		assert!(
			git_dir
				.as_ref()
				.is_some_and(|git_dir| directories.contains(git_dir)),
			"Git metadata remains observed"
		);

		let _ = std::fs::remove_dir_all(repo);
	}

	#[test]
	fn watch_debounce_has_a_hard_one_second_cap() {
		let first = Instant::now();
		let last = first + Duration::from_secs(30);
		assert_eq!(
			git_watch_deadline(Some(first), Some(last), None),
			Some(first + Duration::from_secs(1))
		);
	}

	#[test]
	fn poll_deadline_is_mode_aware_and_not_starved_by_reconciliation() {
		let (commands, _input, internal, requests) = channel();
		let mut controller = GitRefreshController::new(internal);
		let root = Some(PathBuf::from("/virtual/repository"));
		let start = Instant::now();

		controller.reconcile_poll(root.clone(), GitRefreshMode::Interval(7), true, start);
		assert_eq!(
			controller.poll_deadline(),
			Some(start + Duration::from_secs(7))
		);
		for offset in 1..=6 {
			controller.reconcile_poll(
				root.clone(),
				GitRefreshMode::Interval(7),
				true,
				start + Duration::from_secs(offset),
			);
		}
		assert_eq!(
			controller.poll_deadline(),
			Some(start + Duration::from_secs(7)),
			"sustained engine traffic must not postpone the poll"
		);
		commands
			.send(crate::engine::EditorCommand::Undo)
			.expect("queue foreground command");
		assert_eq!(
			controller.queue_poll_if_due(start + Duration::from_secs(7)),
			Ok(())
		);
		assert!(matches!(
			requests.try_recv(),
			Ok(EngineRequest::Command(crate::engine::EditorCommand::Undo))
		));
		assert!(matches!(
			requests.try_recv(),
			Ok(EngineRequest::Internal(InternalEvent::RefreshGitStatus))
		));
		assert_eq!(
			controller.poll_deadline(),
			Some(start + Duration::from_secs(14))
		);

		controller.reconcile_poll(root.clone(), GitRefreshMode::Watch, true, start);
		assert_eq!(controller.poll_deadline(), None);
		controller.reconcile_poll(root.clone(), GitRefreshMode::Watch, false, start);
		assert_eq!(
			controller.poll_deadline(),
			Some(start + Duration::from_secs(super::super::protocol::DEFAULT_GIT_REFRESH_SECONDS))
		);
		controller.reconcile_poll(root, GitRefreshMode::Disabled, false, start);
		assert_eq!(controller.poll_deadline(), None);
	}

	#[test]
	fn observer_refreshes_and_reuses_its_parked_thread() {
		let repo = git_fixture("observer-lifecycle");
		std::fs::create_dir_all(repo.join("empty")).expect("create empty directory");
		let (_commands, _input, internal, requests) = channel();
		let mut observer = GitObserver::spawn(internal);
		observer.set_root(Some(repo.clone()));

		let deadline = Instant::now() + Duration::from_secs(10);
		while !observer.is_watching(&repo) && Instant::now() < deadline {
			std::thread::sleep(Duration::from_millis(5));
		}
		assert!(observer.is_watching(&repo), "observer must watch its root");
		let thread = observer.thread_id().expect("observer thread");
		let await_refresh = || {
			let deadline = Instant::now() + Duration::from_secs(10);
			while Instant::now() < deadline {
				match requests.recv_timeout(Duration::from_millis(50)) {
					Ok(EngineRequest::Internal(InternalEvent::RefreshGitStatus)) => return true,
					Ok(_) | Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
					Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => return false,
				}
			}
			false
		};
		assert!(
			await_refresh(),
			"watch setup must be followed by a catch-up refresh"
		);

		std::fs::write(repo.join("empty/first.txt"), "dirty\n").expect("write observed file");
		assert!(
			await_refresh(),
			"an observed file creation must request refresh"
		);

		observer.set_root(None);
		let deadline = Instant::now() + Duration::from_secs(10);
		while observer.is_watching(&repo) && Instant::now() < deadline {
			std::thread::sleep(Duration::from_millis(5));
		}
		assert!(
			!observer.is_watching(&repo),
			"parking must release the root watch"
		);

		observer.set_root(Some(repo.clone()));
		let deadline = Instant::now() + Duration::from_secs(10);
		while !observer.is_watching(&repo) && Instant::now() < deadline {
			std::thread::sleep(Duration::from_millis(5));
		}
		assert_eq!(
			observer.thread_id(),
			Some(thread),
			"reopening observation must reuse the parked worker"
		);

		observer.set_root(None);
		let deadline = Instant::now() + Duration::from_secs(10);
		while observer.is_watching(&repo) && Instant::now() < deadline {
			std::thread::sleep(Duration::from_millis(5));
		}
		drop(observer);
		let _ = std::fs::remove_dir_all(repo);
	}
}
