use crate::core::{DocByte, RegistryOwnerId};
use crate::engine::runtime_registry::ActionLease;
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory};
use std::path::{Path, PathBuf};

/// Reserved owner outside the registry allocator for engine-owned Git jobs.
/// Runtime owner allocation deliberately stops before `u64::MAX`.
pub(crate) const GIT_JOB_OWNER: RegistryOwnerId = RegistryOwnerId::new(u64::MAX);
const MAX_STATUS_BYTES: usize = 64 * 1024;
const MAX_STATUS_ENTRIES: usize = 1_000;

#[derive(Debug, Clone)]
pub(crate) struct GitStatusAction {
	pub(crate) buffer_id: crate::core::BufferId,
}

/// Completion payload for the pin phase of `:panediff <REV>`:
/// `git rev-parse <REV>^{commit}` performs the acquisition's only symbolic
/// resolution — a single atomic ref read — before any content moves.
#[derive(Debug, Clone)]
pub(crate) struct GitResolveAction {
	pub(crate) left_pane_id: crate::core::PaneId,
	pub(crate) left_buffer_id: crate::core::BufferId,
	pub(crate) repo_root: PathBuf,
	pub(crate) revision: String,
	pub(crate) rel_path: String,
}

/// Completion payload for the fetch phase of `:panediff <REV>`:
/// `git cat-file --batch` fed `<commit_oid>:<relpath>`. Both the blob OID and
/// the content derive from the immutable pinned commit in one output, so a
/// ref moving mid-acquisition has nothing left to tear. (Resolving the
/// commit and blob specs as two arguments of one `rev-parse` is NOT atomic —
/// git reads the ref independently per spec, and a concurrent `branch -f`
/// demonstrably interleaves.)
#[derive(Debug, Clone)]
pub(crate) struct GitDiffAction {
	pub(crate) left_pane_id: crate::core::PaneId,
	pub(crate) left_buffer_id: crate::core::BufferId,
	pub(crate) repo_root: PathBuf,
	pub(crate) revision: String,
	pub(crate) rel_path: String,
	pub(crate) commit_oid: String,
}

fn is_hex_oid(oid: &str) -> bool {
	(oid.len() == 40 || oid.len() == 64) && oid.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Abbreviated object id for display labels (`main.rs@a1b2c3d4`). Identity
/// stays full-width in provenance; this is presentation only.
pub(crate) fn short_oid(oid: &str) -> &str {
	oid.get(..8).unwrap_or(oid)
}

/// One file-touching commit in a `git log --follow --name-status` walk,
/// carrying the path the file had *at that commit* (renames change it).
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct TimelineEntry {
	pub(crate) commit_oid: String,
	pub(crate) path: String,
}

/// Parse `git log -z --format=%H --follow --name-status` output into
/// timeline entries, newest first.
///
/// NUL framing is load-bearing: the line protocol C-quotes paths containing
/// tabs, quotes, backslashes, or newlines even under `core.quotepath=false`,
/// which would hand escape sequences to the fetch phase (and let a
/// historical LF path slip past the LF guard in escaped form). With `-z` the
/// token stream is `hash NUL "\n"+status NUL path NUL [path2 NUL]` and paths
/// arrive raw.
///
/// Deletion records are skipped — there is no blob at a `D` commit, and
/// stepping onto one would only manufacture a "missing" fetch. Hash tokens
/// without a status (path-limited merge listings) and non-UTF-8 paths are
/// likewise skipped rather than guessed at.
pub(crate) fn parse_file_timeline(stdout: &[u8]) -> Vec<TimelineEntry> {
	fn strip_newlines(token: &[u8]) -> &[u8] {
		let start = token.iter().take_while(|&&b| b == b'\n').count();
		&token[start..]
	}

	// Only NUL-terminated tokens are complete: a capture truncated midway
	// through a path would otherwise surface a partial path as a valid
	// timeline entry. Everything after the last NUL is an unterminated
	// fragment and is discarded.
	let Some(last_nul) = stdout.iter().rposition(|&b| b == 0) else {
		return Vec::new();
	};
	let stdout = &stdout[..=last_nul];
	let tokens: Vec<&[u8]> = stdout.split(|&b| b == 0).collect();
	let mut entries = Vec::new();
	let mut index = 0;
	while index < tokens.len() {
		let Ok(head) = std::str::from_utf8(strip_newlines(tokens[index])) else {
			index += 1;
			continue;
		};
		if !is_hex_oid(head) {
			index += 1;
			continue;
		}
		let commit_oid = head.to_string();
		index += 1;
		let Some(&status_token) = tokens.get(index) else {
			break;
		};
		let Ok(status) = std::str::from_utf8(strip_newlines(status_token)) else {
			index += 1;
			continue;
		};
		if is_hex_oid(status) {
			// A hash directly follows: a merge listing without a status
			// record. Leave the token for the outer loop to classify.
			continue;
		}
		let path_count = match status.chars().next() {
			// Renames and copies carry source and destination; the
			// destination is the path at that commit.
			Some('R' | 'C') => 2,
			Some('A' | 'M' | 'D' | 'T') => 1,
			_ => {
				index += 1;
				continue;
			}
		};
		let Some(path_token) = tokens.get(index + path_count).copied() else {
			break;
		};
		let deleted = status.starts_with('D');
		index += 1 + path_count;
		if deleted {
			continue;
		}
		let Ok(path) = std::str::from_utf8(path_token) else {
			continue;
		};
		if path.is_empty() {
			continue;
		}
		entries.push(TimelineEntry {
			commit_oid,
			path: path.to_string(),
		});
	}
	entries
}

/// Completion payload for the older walk of `:diffstep`: the timeline rooted
/// at the snapshot's pinned commit, using the snapshot's own path (valid at
/// that commit by construction).
#[derive(Debug, Clone)]
pub(crate) struct GitOlderWalkAction {
	pub(crate) step: StepContext,
	/// How many file-touching commits to move back.
	pub(crate) steps: u64,
}

/// Completion payload for the newer normalize job: maps the pinned commit to
/// its file-touching position before the HEAD-rooted walk can search for it.
#[derive(Debug, Clone)]
pub(crate) struct GitNewerNormalizeAction {
	pub(crate) step: StepContext,
	pub(crate) steps: u64,
	/// The worktree file's repo-relative path — `--follow` walks renames
	/// backward only from its starting point, so the HEAD-rooted timeline
	/// must use the path as it exists now.
	pub(crate) head_rel_path: String,
}

/// Completion payload for the newer walk: the HEAD-rooted timeline searched
/// for the normalized position.
#[derive(Debug, Clone)]
pub(crate) struct GitNewerWalkAction {
	pub(crate) step: StepContext,
	pub(crate) steps: u64,
	/// The pinned commit's file-touching position from the normalize job.
	pub(crate) current_position: String,
}

/// Completion payload for a step's fetch: identical `cat-file --batch`
/// mechanics to acquisition, but re-materializing into the existing snapshot
/// pane instead of splitting a new one.
#[derive(Debug, Clone)]
pub(crate) struct GitStepFetchAction {
	pub(crate) step: StepContext,
	/// Target commit and the file's path at that commit.
	pub(crate) commit_oid: String,
	pub(crate) rel_path: String,
}

/// The panes, repository, and snapshot identity a `:diffstep` operates on,
/// re-validated at every phase completion because each phase races user
/// actions: pane retargeting, snapshot edits, `:w PATH` conversion, and
/// re-pairing must all cancel the step rather than be overwritten.
#[derive(Debug, Clone)]
pub(crate) struct StepContext {
	pub(crate) snapshot_pane_id: crate::core::PaneId,
	pub(crate) snapshot_buffer_id: crate::core::BufferId,
	pub(crate) left_pane_id: crate::core::PaneId,
	pub(crate) left_buffer_id: crate::core::BufferId,
	pub(crate) repo_root: PathBuf,
	/// The snapshot's commit OID when the step was issued. A provenance
	/// entry that disappeared (write-as) or changed (concurrent
	/// re-acquisition) invalidates the step.
	pub(crate) pinned_commit_oid: String,
	/// The worktree document's file path when the step was issued. A
	/// concurrent `:w PATH` keeps the buffer and the pair but changes what
	/// the snapshot would be paired against — and stales the repository
	/// paths captured at dispatch — so any change invalidates the step.
	pub(crate) left_file_path: Option<std::path::PathBuf>,
}

/// Parse the pin phase's `git rev-parse` output: exactly one hex object id
/// (40 bytes in SHA-1 repositories, 64 in SHA-256), with nothing but blank
/// lines after it.
pub(crate) fn parse_single_oid(stdout: &[u8]) -> Option<String> {
	let text = std::str::from_utf8(stdout).ok()?;
	let mut lines = text.lines().map(str::trim);
	let oid = lines.next()?;
	if lines.any(|line| !line.is_empty()) {
		return None;
	}
	is_hex_oid(oid).then(|| oid.to_string())
}

/// Parse one `git cat-file --batch` record: `<oid> SP <type> SP <size> LF
/// <content>`. Returns the blob OID and its exact content. `expected_oid_len`
/// is the pinned commit OID's width — one repository uses one object format,
/// so a mismatched width is corruption, not a variant.
pub(crate) fn parse_batch_blob(
	stdout: &[u8],
	expected_oid_len: usize,
) -> Result<(String, Vec<u8>), String> {
	let header_end = stdout
		.iter()
		.position(|&b| b == b'\n')
		.ok_or_else(|| "object header is missing".to_string())?;
	let header = std::str::from_utf8(&stdout[..header_end])
		.map_err(|_| "object header is unreadable".to_string())?;
	// The missing form echoes the ORIGINAL spec — which may itself contain
	// spaces (Git permits them in tracked paths) — so it must be recognized
	// before any field splitting: `<spec with spaces> missing`.
	if header.strip_suffix(" missing").is_some() {
		return Err("the file does not exist at that revision".to_string());
	}
	let mut fields = header.split(' ');
	let oid = fields.next().unwrap_or_default();
	match fields.next() {
		Some("blob") => {}
		Some(other) => {
			return Err(format!(
				"the path is a {other} at that revision, not a file"
			));
		}
		None => return Err("object header is malformed".to_string()),
	}
	if !is_hex_oid(oid) || oid.len() != expected_oid_len {
		return Err("object id is malformed".to_string());
	}
	let size: usize = fields
		.next()
		.and_then(|size| size.parse().ok())
		.ok_or_else(|| "object size is malformed".to_string())?;
	if fields.next().is_some() {
		return Err("object header is malformed".to_string());
	}
	let content_start = header_end + 1;
	let content_end = content_start
		.checked_add(size)
		.filter(|&end| end <= stdout.len())
		.ok_or_else(|| "object content is incomplete".to_string())?;
	// The record ends with exactly one LF after the content — a missing
	// terminator means the stream was cut short, and anything more means a
	// second record leaked into the request.
	let tail = &stdout[content_end..];
	if tail != b"\n" {
		return Err("object record has a malformed terminator".to_string());
	}
	Ok((oid.to_string(), stdout[content_start..content_end].to_vec()))
}

/// Reject revisions that cannot be spliced into a Git object spec.
/// Whitespace never appears in a revision, a `:` would shift the path
/// boundary of a `<REV>:<relpath>` spec, and a leading `-` reads as a Git
/// option — this validation is the argv fence for the pin phase, whose
/// `rev-parse` cannot use `--end-of-options` (it would be echoed to stdout);
/// the fetch phase passes its spec on stdin and has no argv surface at all.
pub(crate) fn validate_revision(revision: &str) -> Result<(), String> {
	if revision.is_empty() {
		return Err("the Git revision is empty".to_string());
	}
	if revision.starts_with('-') || revision.chars().any(|c| c.is_whitespace() || c == ':') {
		return Err(format!("not a valid Git revision: {revision}"));
	}
	Ok(())
}

/// The focused file's path relative to the repository root, slash-separated
/// as Git expects in a `<REV>:<relpath>` spec.
pub(crate) fn repo_relative_path(repo_root: &Path, file: &Path) -> Option<String> {
	let rel = file.strip_prefix(repo_root).ok()?;
	let text = rel.to_str()?;
	(!text.is_empty()).then(|| text.replace(std::path::MAIN_SEPARATOR, "/"))
}

/// Synthetic identity handed to language services for a snapshot document:
/// `<repo_root>/.baryon-snapshots/<commit_oid>/<relpath>`.
///
/// Keyed by the immutable commit OID, never the symbolic revision — two
/// snapshots of the same moved ref hold different content and must not alias
/// each other in provider caches keyed by path. The revision label exists
/// only for titles. The location never exists on disk, so canonicalization
/// fails and providers fall back to their stable single-file mode instead of
/// mapping the snapshot onto the live file's workspace identity.
pub(crate) fn snapshot_virtual_path(repo_root: &Path, commit_oid: &str, rel_path: &str) -> String {
	repo_root
		.join(".baryon-snapshots")
		.join(commit_oid)
		.join(rel_path)
		.to_string_lossy()
		.into_owned()
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct GitStatusEntry {
	pub(crate) index: char,
	pub(crate) worktree: char,
	pub(crate) path: String,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct GitStatusSnapshot {
	pub(crate) branch: Option<String>,
	pub(crate) ahead: u32,
	pub(crate) behind: u32,
	pub(crate) entries: Vec<GitStatusEntry>,
}

#[derive(Debug, PartialEq, Eq)]
enum GitContextKey {
	CurrentDir,
	Path(PathBuf),
}

/// Repository discovery cached for the focused document context.
///
/// Resolving a path and climbing its ancestors can block on slow filesystems,
/// so both successful and unsuccessful discovery results are retained until the
/// context changes. `HEAD` itself is still read on every query so a checkout is
/// reflected without changing documents.
#[derive(Debug, Default)]
pub(crate) struct GitBranchCache {
	context: Option<GitContextKey>,
	git_dir: Option<PathBuf>,
	repo_root: Option<PathBuf>,
	branch: Option<String>,
}

impl GitBranchCache {
	fn read_branch(&self) -> Option<String> {
		let git_dir = self.git_dir.as_deref()?;
		let head = std::fs::read_to_string(git_dir.join("HEAD")).ok()?;
		parse_branch(&head)
	}

	fn set_path_context(&mut self, path: &Path) {
		let context_matches = matches!(
			self.context.as_ref(),
			Some(GitContextKey::Path(cached)) if cached == path
		);
		if !context_matches {
			let current_dir = std::env::current_dir().ok();
			(self.repo_root, self.git_dir) = resolve_context_path(path, current_dir.as_deref())
				.and_then(|path| find_git_context(&path))
				.map_or((None, None), |(root, git_dir)| (Some(root), Some(git_dir)));
			self.context = Some(GitContextKey::Path(path.to_path_buf()));
			self.branch = None;
		}
	}

	fn set_current_dir_context(&mut self) {
		if self.context != Some(GitContextKey::CurrentDir) {
			(self.repo_root, self.git_dir) = std::env::current_dir()
				.ok()
				.and_then(|path| find_git_context(&path))
				.map_or((None, None), |(root, git_dir)| (Some(root), Some(git_dir)));
			self.context = Some(GitContextKey::CurrentDir);
			self.branch = None;
		}
	}

	pub(crate) fn cached_branch_for_path(&mut self, path: &Path) -> Option<String> {
		self.set_path_context(path);
		self.branch
			.clone()
			.or_else(|| self.refresh_branch_for_path(path))
	}

	pub(crate) fn cached_branch_for_current_dir(&mut self) -> Option<String> {
		self.set_current_dir_context();
		self.branch
			.clone()
			.or_else(|| self.refresh_branch_for_current_dir())
	}

	pub(crate) fn refresh_branch_for_path(&mut self, path: &Path) -> Option<String> {
		self.set_path_context(path);
		self.branch = self.read_branch();
		self.branch.clone()
	}

	pub(crate) fn refresh_branch_for_current_dir(&mut self) -> Option<String> {
		self.set_current_dir_context();
		self.branch = self.read_branch();
		self.branch.clone()
	}

	pub(crate) fn repo_root_for_path(&mut self, path: &Path) -> Option<PathBuf> {
		self.set_path_context(path);
		self.repo_root.clone()
	}

	pub(crate) fn repo_root_for_current_dir(&mut self) -> Option<PathBuf> {
		self.set_current_dir_context();
		self.repo_root.clone()
	}
}

pub(crate) fn parse_status_porcelain(input: &[u8]) -> Result<GitStatusSnapshot, String> {
	let text = std::str::from_utf8(input)
		.map_err(|_| "git status returned non-UTF-8 output".to_string())?;
	let mut snapshot = GitStatusSnapshot::default();
	for line in text.lines() {
		if let Some(branch) = line.strip_prefix("# branch.head ") {
			snapshot.branch = (branch != "(detached)").then(|| branch.to_string());
		} else if let Some(ab) = line.strip_prefix("# branch.ab ") {
			let mut fields = ab.split_whitespace();
			snapshot.ahead = fields
				.next()
				.and_then(|value| value.strip_prefix('+'))
				.and_then(|value| value.parse().ok())
				.unwrap_or(0);
			snapshot.behind = fields
				.next()
				.and_then(|value| value.strip_prefix('-'))
				.and_then(|value| value.parse().ok())
				.unwrap_or(0);
		} else if let Some(path) = line.strip_prefix("? ") {
			snapshot.entries.push(GitStatusEntry {
				index: '?',
				worktree: '?',
				path: path.to_string(),
			});
		} else if let Some(rest) = line.strip_prefix("1 ") {
			let fields: Vec<_> = rest.splitn(8, ' ').collect();
			if let Some(code) = fields.first().and_then(|field| field.get(..2))
				&& let Some(path) = fields.get(7)
			{
				snapshot.entries.push(GitStatusEntry {
					index: code.as_bytes()[0] as char,
					worktree: code.as_bytes()[1] as char,
					path: (*path).to_string(),
				});
			}
		} else if let Some(rest) = line.strip_prefix("2 ") {
			let fields: Vec<_> = rest.splitn(9, ' ').collect();
			if let Some(code) = fields.first().and_then(|field| field.get(..2))
				&& let Some(path) = fields.get(8)
			{
				let path = path.split_once('\t').map_or_else(
					|| (*path).to_string(),
					|(path, original)| format!("{path} ← {original}"),
				);
				snapshot.entries.push(GitStatusEntry {
					index: code.as_bytes()[0] as char,
					worktree: code.as_bytes()[1] as char,
					path,
				});
			}
		}
	}
	Ok(snapshot)
}

pub(crate) fn render_status_snapshot(
	snapshot: &GitStatusSnapshot,
) -> (Vec<u8>, Vec<HighlightSpan>) {
	let branch = snapshot.branch.as_deref().unwrap_or("(detached)");
	let tracking = match (snapshot.ahead, snapshot.behind) {
		(0, 0) => String::new(),
		(ahead, behind) => format!(" ↑{ahead} ↓{behind}"),
	};
	let mut text = format!("On branch {branch}{tracking}\n");
	let mut highlights = vec![HighlightSpan::new(
		DocByte::ZERO,
		DocByte::new(text.trim_end_matches('\n').len() as u64),
		TokenCategory::Function,
	)];
	if snapshot.entries.is_empty() {
		let start = text.len();
		text.push_str("clean\n");
		highlights.push(HighlightSpan::new(
			DocByte::new(start as u64),
			DocByte::new(text.trim_end_matches('\n').len() as u64),
			TokenCategory::String,
		));
	}
	for (index, entry) in snapshot.entries.iter().enumerate() {
		if index >= MAX_STATUS_ENTRIES {
			text.push_str("… (status truncated)\n");
			break;
		}
		let start = text.len();
		let line = format!("{}{} {}\n", entry.index, entry.worktree, entry.path);
		if text.len() + line.len() > MAX_STATUS_BYTES {
			text.push_str("… (status truncated)\n");
			break;
		}
		text.push_str(&line);
		let category = if entry.index == '?' {
			TokenCategory::GitUntracked
		} else if entry.path.contains(" ← ") {
			TokenCategory::GitRename
		} else if entry.worktree != '.' {
			TokenCategory::GitUnstaged
		} else if entry.index != '.' {
			TokenCategory::GitStaged
		} else {
			TokenCategory::GitUnstaged
		};
		highlights.push(HighlightSpan::new(
			DocByte::new(start as u64),
			DocByte::new(text.trim_end_matches('\n').len() as u64),
			category,
		));
	}
	(text.into_bytes(), highlights)
}

pub(crate) fn git_status_action_lease(buffer_id: crate::core::BufferId) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, GitStatusAction { buffer_id })
}

pub(crate) fn git_resolve_action_lease(action: GitResolveAction) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, action)
}

pub(crate) fn git_diff_action_lease(action: GitDiffAction) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, action)
}

pub(crate) fn git_older_walk_action_lease(action: GitOlderWalkAction) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, action)
}

pub(crate) fn git_newer_normalize_action_lease(action: GitNewerNormalizeAction) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, action)
}

pub(crate) fn git_newer_walk_action_lease(action: GitNewerWalkAction) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, action)
}

pub(crate) fn git_step_fetch_action_lease(action: GitStepFetchAction) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, action)
}

/// Resolves the context once before repository discovery.
///
/// An unsaved relative path cannot be canonicalized yet. Anchoring it to
/// `current_dir` before climbing lets `src/new.rs` reach a repository above
/// `src`; without a current directory such a path cannot be placed at all.
fn resolve_context_path(path: &Path, current_dir: Option<&Path>) -> Option<PathBuf> {
	if let Ok(canonical) = crate::core::canonicalize(path) {
		return Some(canonical);
	}
	if path.is_absolute() {
		return Some(path.to_path_buf());
	}
	Some(current_dir?.join(path))
}

fn find_git_context(path: &Path) -> Option<(PathBuf, PathBuf)> {
	let start_dir = if path.is_dir() { path } else { path.parent()? };
	for ancestor in start_dir.ancestors() {
		let marker = ancestor.join(".git");
		if marker.is_dir() {
			return Some((ancestor.to_path_buf(), marker));
		}
		if marker.is_file() {
			if let Some(git_dir) = git_dir_from_file(&marker) {
				return Some((ancestor.to_path_buf(), git_dir));
			}
			// A malformed .git marker is a repository boundary, matching Git's
			// existing discovery behavior in find_git_dir_in.
			return None;
		}
	}
	None
}

fn git_dir_from_file(marker: &Path) -> Option<PathBuf> {
	let contents = std::fs::read_to_string(marker).ok()?;
	let git_dir = contents.trim().strip_prefix("gitdir:")?.trim();
	if git_dir.is_empty() {
		return None;
	}

	let git_dir = PathBuf::from(git_dir);
	if git_dir.is_absolute() {
		Some(git_dir)
	} else {
		Some(marker.parent()?.join(git_dir))
	}
}

fn parse_branch(head: &str) -> Option<String> {
	let reference = head.trim().strip_prefix("ref:")?.trim();
	let branch = reference.strip_prefix("refs/heads/")?;
	(!branch.is_empty()).then(|| branch.to_string())
}

#[cfg(test)]
mod tests {
	use super::{
		GitBranchCache, GitStatusEntry, GitStatusSnapshot, TimelineEntry, find_git_context,
		parse_batch_blob, parse_file_timeline, parse_single_oid, parse_status_porcelain,
		render_status_snapshot, repo_relative_path, resolve_context_path, short_oid,
		snapshot_virtual_path, validate_revision,
	};
	use crate::svp::rust_highlight::TokenCategory;
	use std::path::Path;

	fn temp_dir(label: &str) -> std::path::PathBuf {
		let path = std::env::temp_dir().join(format!(
			"baryon-git-{label}-{}-{:?}",
			std::process::id(),
			std::thread::current().id()
		));
		let _ = std::fs::remove_dir_all(&path);
		std::fs::create_dir_all(&path).expect("create git test directory");
		path
	}

	fn write_head(git_dir: &Path, contents: &str) {
		std::fs::create_dir_all(git_dir).expect("create git directory");
		std::fs::write(git_dir.join("HEAD"), contents).expect("write HEAD");
	}

	fn branch_for_path(path: &Path) -> Option<String> {
		GitBranchCache::default().refresh_branch_for_path(path)
	}

	#[test]
	fn finds_branch_by_climbing_from_a_file() {
		let root = temp_dir("ancestor");
		write_head(&root.join(".git"), "ref: refs/heads/feature/status-bar\n");
		let source = root.join("crates/lib/src/main.rs");
		std::fs::create_dir_all(source.parent().unwrap()).expect("create source directory");
		std::fs::write(&source, "fn main() {}\n").expect("write source");

		assert_eq!(
			branch_for_path(&source).as_deref(),
			Some("feature/status-bar")
		);

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn follows_relative_git_dir_pointer_files() {
		let root = temp_dir("worktree");
		let worktree = root.join("checkout");
		let git_dir = root.join("metadata/worktrees/checkout");
		std::fs::create_dir_all(&worktree).expect("create worktree");
		write_head(&git_dir, "ref: refs/heads/worktree-branch\n");
		std::fs::write(
			worktree.join(".git"),
			"gitdir: ../metadata/worktrees/checkout\n",
		)
		.expect("write gitdir pointer");

		assert_eq!(
			branch_for_path(&worktree).as_deref(),
			Some("worktree-branch")
		);

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn unsaved_relative_paths_climb_from_the_current_directory() {
		let root = temp_dir("unsaved");
		write_head(&root.join(".git"), "ref: refs/heads/new-file\n");
		let path = resolve_context_path(Path::new("src/new.rs"), Some(&root))
			.expect("resolve relative unsaved path");

		assert_eq!(path, root.join("src/new.rs"));
		assert_eq!(
			find_git_context(&path),
			Some((root.clone(), root.join(".git")))
		);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn relative_paths_are_unresolvable_without_a_current_directory() {
		assert_eq!(resolve_context_path(Path::new("src/new.rs"), None), None);
	}

	#[test]
	fn cache_rereads_head_without_repeating_repository_discovery() {
		let root = temp_dir("cache");
		let source = root.join("src/main.rs");
		std::fs::create_dir_all(source.parent().unwrap()).expect("create source directory");
		std::fs::write(&source, "fn main() {}\n").expect("write source");
		write_head(&root.join(".git"), "ref: refs/heads/main\n");
		let mut cache = GitBranchCache::default();

		assert_eq!(
			cache.refresh_branch_for_path(&source).as_deref(),
			Some("main")
		);
		std::fs::write(root.join(".git/HEAD"), "ref: refs/heads/next\n").expect("update HEAD");
		assert_eq!(
			cache.refresh_branch_for_path(&source).as_deref(),
			Some("next")
		);

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn cached_branch_populates_immediately_after_a_context_switch() {
		let first = temp_dir("context-first");
		let second = temp_dir("context-second");
		let first_file = first.join("first.rs");
		let second_file = second.join("second.rs");
		std::fs::write(&first_file, "").expect("first file");
		std::fs::write(&second_file, "").expect("second file");
		write_head(&first.join(".git"), "ref: refs/heads/first\n");
		write_head(&second.join(".git"), "ref: refs/heads/second\n");
		let mut cache = GitBranchCache::default();

		assert_eq!(
			cache.cached_branch_for_path(&first_file).as_deref(),
			Some("first")
		);
		assert_eq!(
			cache.cached_branch_for_path(&second_file).as_deref(),
			Some("second")
		);

		let _ = std::fs::remove_dir_all(first);
		let _ = std::fs::remove_dir_all(second);
	}

	#[test]
	fn cache_retains_a_negative_discovery_until_the_context_changes() {
		let root = temp_dir("negative-cache");
		let source = root.join("src/main.rs");
		std::fs::create_dir_all(source.parent().unwrap()).expect("create source directory");
		std::fs::write(&source, "fn main() {}\n").expect("write source");
		// Prevent a repository containing the test temp directory from leaking in.
		std::fs::write(root.join(".git"), "not a gitdir pointer\n")
			.expect("write repository boundary");
		let mut cache = GitBranchCache::default();

		assert_eq!(cache.refresh_branch_for_path(&source), None);
		std::fs::remove_file(root.join(".git")).expect("remove boundary");
		write_head(&root.join(".git"), "ref: refs/heads/created-later\n");
		assert_eq!(cache.refresh_branch_for_path(&source), None);

		let other = root.join("other.rs");
		assert_eq!(
			cache.refresh_branch_for_path(&other).as_deref(),
			Some("created-later")
		);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn detached_and_malformed_heads_are_omitted() {
		let detached = temp_dir("detached");
		write_head(
			&detached.join(".git"),
			"0123456789abcdef0123456789abcdef01234567\n",
		);
		assert_eq!(branch_for_path(&detached), None);

		let malformed = temp_dir("malformed");
		write_head(&malformed.join(".git"), "ref: refs/tags/v1.0\n");
		assert_eq!(branch_for_path(&malformed), None);

		let _ = std::fs::remove_dir_all(detached);
		let _ = std::fs::remove_dir_all(malformed);
	}

	#[test]
	fn a_malformed_repository_marker_stops_the_climb() {
		let root = temp_dir("boundary");
		let child = root.join("child");
		std::fs::create_dir_all(&child).expect("create child directory");
		std::fs::write(root.join(".git"), "not a gitdir pointer\n")
			.expect("write repository boundary");
		assert_eq!(branch_for_path(&child), None);
		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn exhausting_every_ancestor_yields_no_repository() {
		let root = temp_dir("plain");
		let child = root.join("src");
		std::fs::create_dir_all(&child).expect("create child directory");
		// Scanning an explicit ancestor chain keeps the exhausted-climb case from
		// depending on whether the configured temporary directory sits in a repository.
		let ancestors = [child.as_path(), root.as_path()];

		assert_eq!(ancestors.into_iter().find_map(find_git_context), None);

		let _ = std::fs::remove_dir_all(root);
	}

	#[test]
	fn revisions_reject_spec_breaking_characters() {
		for revision in ["HEAD", "HEAD~3", "main", "v1.0", "abc123", "@{-1}"] {
			assert_eq!(validate_revision(revision), Ok(()), "{revision}");
		}
		// A leading dash would read as a Git option: `:pd --output=...` must
		// never reach the spawned process as anything but an object spec.
		for revision in [
			"",
			"HEAD:file",
			"two words",
			"tab\there",
			"-",
			"--output=/tmp/pwned",
			"-p",
		] {
			assert!(validate_revision(revision).is_err(), "{revision:?}");
		}
	}

	#[test]
	fn single_oids_require_one_hex_id_and_nothing_else() {
		let sha1 = "a".repeat(40);
		let sha256 = "f".repeat(64);
		assert_eq!(
			parse_single_oid(format!("{sha1}\n").as_bytes()).as_deref(),
			Some(sha1.as_str())
		);
		assert_eq!(
			parse_single_oid(format!("{sha256}\n").as_bytes()).as_deref(),
			Some(sha256.as_str())
		);
		// Trailing blank lines are fine; junk hidden behind a blank line, a
		// second value, short ids, and non-hex are not.
		assert!(parse_single_oid(format!("{sha1}\n\n").as_bytes()).is_some());
		assert!(parse_single_oid(format!("{sha1}\n\nextra\n").as_bytes()).is_none());
		assert!(parse_single_oid(format!("{sha1}\n{sha1}\n").as_bytes()).is_none());
		assert!(parse_single_oid(b"abc123\n").is_none());
		assert!(parse_single_oid(format!("{}\n", "z".repeat(40)).as_bytes()).is_none());
		assert!(parse_single_oid(b"").is_none());
	}

	#[test]
	fn batch_blob_records_parse_strictly() {
		let oid = "b".repeat(40);
		let record = format!("{oid} blob 5\nhello\n");
		assert_eq!(
			parse_batch_blob(record.as_bytes(), 40),
			Ok((oid.clone(), b"hello".to_vec()))
		);
		// Content bytes are counted, not line-parsed: embedded newlines and
		// the exact size boundary must round-trip.
		let record = format!("{oid} blob 4\na\nb\n\n");
		assert_eq!(
			parse_batch_blob(record.as_bytes(), 40),
			Ok((oid.clone(), b"a\nb\n".to_vec()))
		);

		// One repository uses one object format: a 64-hex blob id against a
		// 40-hex commit id is corruption, not a variant.
		let sha256 = "c".repeat(64);
		assert!(parse_batch_blob(format!("{sha256} blob 5\nhello\n").as_bytes(), 40).is_err());

		assert!(
			parse_batch_blob(format!("{oid}:nope.txt missing\n").as_bytes(), 40)
				.unwrap_err()
				.contains("does not exist")
		);
		// The missing form echoes the original spec, which may contain
		// spaces; it must not be shredded by field parsing.
		assert!(
			parse_batch_blob(format!("{oid}:no such file.txt missing\n").as_bytes(), 40)
				.unwrap_err()
				.contains("does not exist")
		);
		assert!(
			parse_batch_blob(format!("{oid} tree 33\n").as_bytes(), 40)
				.unwrap_err()
				.contains("not a file")
		);
		// Truncated content, trailing data, a missing protocol terminator,
		// and malformed headers are refused.
		assert!(parse_batch_blob(format!("{oid} blob 10\nhello\n").as_bytes(), 40).is_err());
		assert!(parse_batch_blob(format!("{oid} blob 5\nhello\nextra").as_bytes(), 40).is_err());
		assert!(parse_batch_blob(format!("{oid} blob 5\nhello").as_bytes(), 40).is_err());
		assert!(parse_batch_blob(format!("{oid} blob\nhello\n").as_bytes(), 40).is_err());
		assert!(parse_batch_blob(b"no newline", 40).is_err());
	}

	fn timeline_record(commit: &str, status: &str, paths: &[&str]) -> Vec<u8> {
		// The empirically pinned `-z` token stream: hash NUL, "\n"+status
		// NUL, then each path NUL — raw, unquoted.
		let mut record = Vec::new();
		record.extend_from_slice(commit.as_bytes());
		record.push(0);
		record.push(b'\n');
		record.extend_from_slice(status.as_bytes());
		record.push(0);
		for path in paths {
			record.extend_from_slice(path.as_bytes());
			record.push(0);
		}
		record
	}

	#[test]
	fn file_timelines_carry_the_raw_path_at_each_commit() {
		let c = |n: u8| format!("{:0>40}", n);
		let mut stdout = Vec::new();
		stdout.extend(timeline_record(&c(4), "M", &["new name.txt"]));
		stdout.extend(timeline_record(
			&c(3),
			"R097",
			&["tab\tname.txt", "new name.txt"],
		));
		stdout.extend(timeline_record(&c(2), "M", &["tab\tname.txt"]));
		stdout.extend(timeline_record(&c(1), "A", &["tab\tname.txt"]));
		let timeline = parse_file_timeline(&stdout);
		// Renames carry source and destination; the destination is the path
		// at that commit. Tabs (which the line protocol would C-quote even
		// under core.quotepath=false) arrive raw under `-z`.
		assert_eq!(
			timeline,
			vec![
				TimelineEntry {
					commit_oid: c(4),
					path: "new name.txt".to_string()
				},
				TimelineEntry {
					commit_oid: c(3),
					path: "new name.txt".to_string()
				},
				TimelineEntry {
					commit_oid: c(2),
					path: "tab\tname.txt".to_string()
				},
				TimelineEntry {
					commit_oid: c(1),
					path: "tab\tname.txt".to_string()
				},
			]
		);
	}

	#[test]
	fn file_timelines_skip_deletions_and_merge_listings() {
		let c = |n: u8| format!("{:0>40}", n);
		// A → D → A history: the deletion commit has no blob and stepping
		// onto it would only manufacture a "missing" fetch.
		let mut stdout = Vec::new();
		stdout.extend(timeline_record(&c(3), "A", &["f.txt"]));
		stdout.extend(timeline_record(&c(2), "D", &["f.txt"]));
		stdout.extend(timeline_record(&c(1), "A", &["f.txt"]));
		let timeline = parse_file_timeline(&stdout);
		assert_eq!(
			timeline,
			vec![
				TimelineEntry {
					commit_oid: c(3),
					path: "f.txt".to_string()
				},
				TimelineEntry {
					commit_oid: c(1),
					path: "f.txt".to_string()
				},
			]
		);

		// A hash directly followed by another hash (path-limited merge
		// listing without a status record) is dropped, not guessed at.
		let mut stdout = Vec::new();
		stdout.extend_from_slice(c(9).as_bytes());
		stdout.push(0);
		stdout.push(b'\n');
		stdout.extend(timeline_record(&c(8), "M", &["file.rs"]));
		let timeline = parse_file_timeline(&stdout);
		assert_eq!(
			timeline,
			vec![TimelineEntry {
				commit_oid: c(8),
				path: "file.rs".to_string()
			}]
		);
		assert!(parse_file_timeline(b"not a timeline").is_empty());
		assert!(parse_file_timeline(&[0xff, 0xfe]).is_empty());
	}

	#[test]
	fn file_timelines_discard_unterminated_trailing_fragments() {
		let c = |n: u8| format!("{:0>40}", n);
		// A capture truncated midway through a path token must not surface
		// the partial path as a valid entry — only NUL-terminated tokens
		// are complete.
		let mut stdout = timeline_record(&c(2), "M", &["good.txt"]);
		stdout.extend_from_slice(c(1).as_bytes());
		stdout.push(0);
		stdout.extend_from_slice(b"\nM");
		stdout.push(0);
		stdout.extend_from_slice(b"parti"); // no terminating NUL
		assert_eq!(
			parse_file_timeline(&stdout),
			vec![TimelineEntry {
				commit_oid: c(2),
				path: "good.txt".to_string()
			}]
		);

		// Same for a rename cut before its destination path completes.
		let mut stdout = timeline_record(&c(2), "M", &["good.txt"]);
		stdout.extend_from_slice(c(1).as_bytes());
		stdout.push(0);
		stdout.extend_from_slice(b"\nR097");
		stdout.push(0);
		stdout.extend_from_slice(b"old.txt");
		stdout.push(0);
		stdout.extend_from_slice(b"new-parti"); // no terminating NUL
		assert_eq!(
			parse_file_timeline(&stdout),
			vec![TimelineEntry {
				commit_oid: c(2),
				path: "good.txt".to_string()
			}]
		);

		// No NUL at all: nothing is complete.
		assert!(parse_file_timeline(b"deadbeef fragment without any nul").is_empty());
	}

	#[test]
	fn file_timelines_preserve_historical_newline_paths_raw() {
		let c = |n: u8| format!("{:0>40}", n);
		// A historical LF-containing filename arrives raw under `-z` (the
		// line protocol would escape it, sneaking it past the LF guard).
		// The parser must hand it through verbatim so the fetch boundary
		// can refuse it precisely.
		let stdout = timeline_record(&c(1), "A", &["we\nird.txt"]);
		let timeline = parse_file_timeline(&stdout);
		assert_eq!(
			timeline,
			vec![TimelineEntry {
				commit_oid: c(1),
				path: "we\nird.txt".to_string()
			}]
		);
	}

	#[test]
	fn short_oids_abbreviate_for_display_only() {
		let oid = "0123456789abcdef0123456789abcdef01234567";
		assert_eq!(short_oid(oid), "01234567");
		assert_eq!(short_oid("abc"), "abc");
	}

	#[test]
	fn snapshot_virtual_paths_are_keyed_by_commit_oid_not_revision() {
		let root = Path::new("/repo");
		let commit_a = "a".repeat(40);
		let commit_b = "b".repeat(40);
		let path_a = snapshot_virtual_path(root, &commit_a, "src/main.rs");
		let path_b = snapshot_virtual_path(root, &commit_b, "src/main.rs");
		// The same symbolic revision label resolving to two commits (a moved
		// ref) must yield distinct provider identities: the label is not a
		// parameter here by design.
		assert_ne!(path_a, path_b);
		assert_eq!(
			path_a,
			Path::new("/repo")
				.join(".baryon-snapshots")
				.join(&commit_a)
				.join("src/main.rs")
				.to_string_lossy()
		);
	}

	#[test]
	fn repo_relative_paths_are_slash_separated_and_non_empty() {
		let root = Path::new("/repo");
		assert_eq!(
			repo_relative_path(root, Path::new("/repo/src/main.rs")).as_deref(),
			Some("src/main.rs")
		);
		assert_eq!(
			repo_relative_path(root, Path::new("/elsewhere/main.rs")),
			None
		);
		assert_eq!(repo_relative_path(root, Path::new("/repo")), None);
	}

	#[test]
	fn parses_porcelain_v2_branch_tracking_and_entries() {
		let input = b"# branch.oid abc\n# branch.head main\n# branch.ab +2 -1\n1 M. N... 100644 100644 100644 abc def src/main.rs\n2 R. N... 100644 100644 100644 abc def R100 new.rs\toriginal.rs\n? README.md\n";
		let snapshot = parse_status_porcelain(input).expect("valid porcelain status");
		assert_eq!(snapshot.branch.as_deref(), Some("main"));
		assert_eq!((snapshot.ahead, snapshot.behind), (2, 1));
		assert_eq!(snapshot.entries.len(), 3);
		assert_eq!(snapshot.entries[0].path, "src/main.rs");
		assert_eq!(snapshot.entries[1].path, "new.rs ← original.rs");
		assert_eq!(snapshot.entries[2].index, '?');
	}

	#[test]
	fn renders_clean_and_modified_status_with_highlight_spans() {
		let snapshot = parse_status_porcelain(
			b"# branch.head feature/status\n# branch.ab +1 -0\n1 M. N... 100644 100644 100644 abc def src/main.rs\n",
		)
		.expect("valid porcelain status");
		let (text, highlights) = render_status_snapshot(&snapshot);
		assert_eq!(
			String::from_utf8(text).unwrap(),
			"On branch feature/status ↑1 ↓0\nM. src/main.rs\n"
		);
		assert_eq!(highlights.len(), 2);
	}

	#[test]
	fn worktree_changes_override_staged_status_color() {
		let snapshot = GitStatusSnapshot {
			branch: Some("main".to_string()),
			entries: vec![
				GitStatusEntry {
					index: 'M',
					worktree: '.',
					path: "staged.rs".to_string(),
				},
				GitStatusEntry {
					index: '.',
					worktree: 'M',
					path: "unstaged.rs".to_string(),
				},
				GitStatusEntry {
					index: 'M',
					worktree: 'M',
					path: "both.rs".to_string(),
				},
			],
			..GitStatusSnapshot::default()
		};
		let (_, highlights) = render_status_snapshot(&snapshot);
		assert_eq!(
			highlights
				.iter()
				.map(|span| span.category)
				.collect::<Vec<_>>(),
			vec![
				TokenCategory::Function,
				TokenCategory::GitStaged,
				TokenCategory::GitUnstaged,
				TokenCategory::GitUnstaged,
			]
		);
	}
}
