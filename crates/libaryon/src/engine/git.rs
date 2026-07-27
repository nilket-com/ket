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

/// Completion payload for a `git cat-file blob <REV>:<relpath>` job started by
/// `:panediff <REV>`. Carries enough context to verify the diff source is
/// unchanged before the snapshot pane is created.
#[derive(Debug, Clone)]
pub(crate) struct GitDiffAction {
	pub(crate) left_pane_id: crate::core::PaneId,
	pub(crate) left_buffer_id: crate::core::BufferId,
	pub(crate) repo_root: PathBuf,
	pub(crate) revision: String,
	pub(crate) rel_path: String,
}

/// Reject revisions that cannot be spliced into a `<REV>:<relpath>` spec.
/// Whitespace never appears in a revision, a `:` would shift the path
/// boundary of the spec, and a leading `-` reads as a Git option (the spec is
/// additionally fenced behind `--end-of-options` when the job is spawned).
pub(crate) fn validate_revision(revision: &str) -> Result<(), String> {
	if revision.is_empty() {
		return Err("the Git revision is empty".to_string());
	}
	if revision.starts_with('-')
		|| revision.chars().any(|c| c.is_whitespace() || c == ':')
	{
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

pub(crate) fn git_diff_action_lease(action: GitDiffAction) -> ActionLease {
	ActionLease::new(GIT_JOB_OWNER, action)
}

/// Resolves the context once before repository discovery.
///
/// An unsaved relative path cannot be canonicalized yet. Anchoring it to
/// `current_dir` before climbing lets `src/new.rs` reach a repository above
/// `src`; without a current directory such a path cannot be placed at all.
fn resolve_context_path(path: &Path, current_dir: Option<&Path>) -> Option<PathBuf> {
	if let Ok(canonical) = path.canonicalize() {
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
		GitBranchCache, GitStatusEntry, GitStatusSnapshot, find_git_context,
		parse_status_porcelain, render_status_snapshot, repo_relative_path,
		resolve_context_path, validate_revision,
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
	fn repo_relative_paths_are_slash_separated_and_non_empty() {
		let root = Path::new("/repo");
		assert_eq!(
			repo_relative_path(root, Path::new("/repo/src/main.rs")).as_deref(),
			Some("src/main.rs")
		);
		assert_eq!(repo_relative_path(root, Path::new("/elsewhere/main.rs")), None);
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
