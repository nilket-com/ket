use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use triomphe::Arc as TriArc;

use ra_ap_base_db::{CrateGraphBuilder, CrateOrigin, CrateWorkspaceData, Env, SourceRoot};
use ra_ap_cfg::CfgOptions;
use ra_ap_ide::{
	Analysis, AnalysisHost, AssistResolveStrategy, Cancellable, DiagnosticsConfig, Highlight,
	HighlightConfig, HlMod, HlTag, RaFixtureConfig, SymbolKind,
};
use ra_ap_ide_db::{ChangeWithProcMacros, Severity};
use ra_ap_paths::{AbsPath, AbsPathBuf, Utf8PathBuf};
use ra_ap_project_model::{
	CargoConfig, PackageRoot, ProjectManifest, ProjectWorkspace, ProjectWorkspaceKind,
	RustLibSource,
};
use ra_ap_syntax::{Edition, SyntaxKind};
use ra_ap_vfs::{
	FileId, Vfs, VfsPath,
	file_set::{FileSet, FileSetConfig},
};
use rustc_hash::FxHashMap;

use crate::core::DocByte;
use crate::engine::language::{GreenNode, LanguageProvider, SemanticOutput};
use crate::svp::diagnostic::{DiagnosticSeverity, DiagnosticSpan};
use crate::svp::rust_highlight::{HighlightSpan, TokenCategory, highlight_viewport};
use crate::svp::rust_parse::parse_fragment;
use crate::svp::sync::{ViewportChunk, find_safe_parse_boundaries};
use crate::uast::kind::SemanticKind;

// ============================================================
// Provider
// ============================================================

pub struct RustProvider {
	state: Mutex<ProviderState>,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
struct WorkspaceKey(AbsPathBuf);

struct ProviderState {
	workspaces: HashMap<WorkspaceKey, WorkspaceState>,
	member_roots: HashMap<AbsPathBuf, WorkspaceKey>,
	manifest_aliases: HashMap<AbsPathBuf, WorkspaceKey>,
	single_files: HashMap<String, WorkspaceState>,
}

struct WorkspaceState {
	host: AnalysisHost,
	file_ids: HashMap<VfsPath, FileId>,
	diagnostic_mode: DiagnosticCollectionMode,
	fingerprint: WorkspaceFingerprint,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct WorkspaceFingerprint(Vec<(AbsPathBuf, Option<u64>)>);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DiagnosticCollectionMode {
	Full,
	SyntaxOnly,
}

impl Default for RustProvider {
	fn default() -> Self {
		Self::new()
	}
}

impl RustProvider {
	pub fn new() -> Self {
		Self {
			state: Mutex::new(ProviderState {
				workspaces: HashMap::new(),
				member_roots: HashMap::new(),
				manifest_aliases: HashMap::new(),
				single_files: HashMap::new(),
			}),
		}
	}
}

struct InitializedWorkspace {
	key: WorkspaceKey,
	state: WorkspaceState,
	member_roots: Vec<AbsPathBuf>,
	manifests: Vec<AbsPathBuf>,
}

impl ProviderState {
	fn workspace_key_for_path(&self, path: &AbsPathBuf) -> Option<WorkspaceKey> {
		if let Some((_, key)) = self
			.member_roots
			.iter()
			.filter(|(root, _)| path.starts_with(root.as_ref()))
			.max_by_key(|(root, _)| root.as_str().len())
		{
			return Some(key.clone());
		}
		discover_manifest(path).and_then(|manifest| self.manifest_aliases.get(&manifest).cloned())
	}

	fn workspace_fingerprint_changed(&self, key: &WorkspaceKey) -> bool {
		self.workspaces.get(key).is_some_and(|workspace| {
			fingerprint_for(&workspace.fingerprint.0) != workspace.fingerprint
		})
	}

	fn remove_workspace(&mut self, key: &WorkspaceKey) {
		self.workspaces.remove(key);
		self.member_roots.retain(|_, owner| owner != key);
		self.manifest_aliases.retain(|_, owner| owner != key);
	}

	fn install_workspace(&mut self, initialized: InitializedWorkspace) {
		let InitializedWorkspace {
			key,
			state,
			member_roots,
			manifests,
		} = initialized;
		self.remove_workspace(&key);
		for root in member_roots {
			self.member_roots.insert(root, key.clone());
		}
		for manifest in manifests {
			self.manifest_aliases.insert(manifest, key.clone());
		}
		self.workspaces.insert(key, state);
	}

	fn cached_single_file(&mut self, file_path: &str) -> Option<(&mut WorkspaceState, FileId)> {
		let fallback = self.single_files.get_mut(file_path)?;
		let file_id = *fallback
			.file_ids
			.values()
			.next()
			.expect("single-file state has one FileId");
		Some((fallback, file_id))
	}
}

fn canonical_path(file_path: &str) -> Option<AbsPathBuf> {
	let canonical = PathBuf::from(file_path).canonicalize().ok()?;
	let utf8 = Utf8PathBuf::try_from(canonical).ok()?;
	Some(AbsPathBuf::assert(utf8))
}

fn discover_manifest(path: &AbsPathBuf) -> Option<AbsPathBuf> {
	let manifest = ProjectManifest::discover_single(path.as_ref()).ok()?;
	Some(AsRef::<AbsPath>::as_ref(manifest.manifest_path()).to_path_buf())
}

fn fingerprint_for(paths: &[(AbsPathBuf, Option<u64>)]) -> WorkspaceFingerprint {
	WorkspaceFingerprint(
		paths
			.iter()
			.map(|(path, _)| {
				let hash = std::fs::read(path).ok().map(|contents| {
					let mut hasher = std::collections::hash_map::DefaultHasher::new();
					contents.hash(&mut hasher);
					hasher.finish()
				});
				(path.clone(), hash)
			})
			.collect(),
	)
}

fn analyze_workspace_file(
	state: &mut WorkspaceState,
	file_id: FileId,
	content: &str,
	global_offset: DocByte,
) -> SemanticOutput {
	let mut change = ChangeWithProcMacros::default();
	change.change_file(file_id, Some(content.to_owned()));
	state.host.apply_change(change);

	let analysis = state.host.analysis();
	let diag_config = diagnostics_config();
	match (
		analysis.highlight(highlight_config(), file_id),
		collect_error_diagnostics(
			&analysis,
			file_id,
			content,
			global_offset,
			&diag_config,
			state.diagnostic_mode,
		),
	) {
		(Ok(highlights), Ok(diagnostics)) => SemanticOutput {
			highlights: highlights
				.into_iter()
				.filter_map(|hl| {
					let category = map_hl_tag(hl.highlight);
					(category != TokenCategory::Unclassified).then(|| {
						HighlightSpan::new(
							global_offset.saturating_add(u64::from(u32::from(hl.range.start()))),
							global_offset.saturating_add(u64::from(u32::from(hl.range.end()))),
							category,
						)
					})
				})
				.collect(),
			diagnostics,
			inline_annotations: Vec::new(),
		},
		_ => SemanticOutput::default(),
	}
}

impl LanguageProvider for RustProvider {
	fn language_id(&self) -> &'static str {
		"rust"
	}

	fn label(&self) -> &'static str {
		"Rust"
	}

	fn extensions(&self) -> &'static [&'static str] {
		&["rs"]
	}

	fn lexical_baseline(
		&self,
		viewport_bytes: &[u8],
		offset_map: &[(usize, DocByte)],
	) -> Vec<HighlightSpan> {
		match find_safe_parse_boundaries(viewport_bytes) {
			Some(bounds) => {
				let chunk = ViewportChunk {
					global_offset: DocByte::ZERO,
					buffer: viewport_bytes.to_vec(),
				};
				let tree = parse_fragment(&chunk, bounds, offset_map.to_vec());
				highlight_viewport(&tree)
			}
			None => Vec::new(),
		}
	}

	fn parse_structural_topology(&self, source: &[u8]) -> Option<GreenNode> {
		let text = std::str::from_utf8(source).ok()?;
		Some(build_rust_green_tree(text))
	}

	fn semantic_reactor(
		&self,
		full_source: Arc<String>,
		global_offset: DocByte,
		file_path: &str,
	) -> SemanticOutput {
		let content: &str = &full_source;
		let mut state = self.state.lock().unwrap();
		let canonical_path = canonical_path(file_path);

		if let Some(path) = canonical_path.as_ref()
			&& let Some(key) = state.workspace_key_for_path(path)
		{
			if state.workspace_fingerprint_changed(&key) {
				state.remove_workspace(&key);
			} else if let Some(workspace) = state.workspaces.get_mut(&key)
				&& let Some(file_id) = workspace
					.file_ids
					.get(&VfsPath::from(path.clone()))
					.copied()
			{
				return analyze_workspace_file(workspace, file_id, content, global_offset);
			}
		}

		// A canonical path that has already failed workspace resolution must remain
		// syntax-only until the provider is evicted. In particular, do not repeat
		// cargo metadata and the workspace scan for files under excluded roots.
		if let Some((fallback, file_id)) = state.cached_single_file(file_path) {
			return analyze_workspace_file(fallback, file_id, content, global_offset);
		}

		if let Some(path) = canonical_path {
			if let Some(initialized) = initialize_workspace(path.clone()) {
				let key = initialized.key.clone();
				state.install_workspace(initialized);
				if let Some(workspace) = state.workspaces.get_mut(&key)
					&& let Some(file_id) = workspace.file_ids.get(&VfsPath::from(path)).copied()
				{
					return analyze_workspace_file(workspace, file_id, content, global_offset);
				}
			}
		}

		let fallback = state
			.single_files
			.entry(file_path.to_string())
			.or_insert_with(|| {
				let (host, file_id) = init_single_file(content);
				let mut file_ids = HashMap::new();
				file_ids.insert(VfsPath::new_virtual_path(file_path.into()), file_id);
				WorkspaceState {
					host,
					file_ids,
					diagnostic_mode: DiagnosticCollectionMode::SyntaxOnly,
					fingerprint: WorkspaceFingerprint(Vec::new()),
				}
			});
		let file_id = *fallback
			.file_ids
			.values()
			.next()
			.expect("single-file state has one FileId");
		analyze_workspace_file(fallback, file_id, content, global_offset)
	}
}

// ============================================================
// Structural topology: ra_ap_syntax → GreenNode
// ============================================================

pub(crate) fn build_rust_green_tree(source: &str) -> GreenNode {
	use ra_ap_syntax::{AstNode, SourceFile};

	let parse = SourceFile::parse(source, Edition::Edition2024);
	let root = parse.tree().syntax().clone();
	build_green_recursive(&root)
}

fn rust_kind_to_semantic(kind: SyntaxKind) -> SemanticKind {
	match kind {
		// Scope/block boundaries → RelationalTable
		SyntaxKind::FN
		| SyntaxKind::IMPL
		| SyntaxKind::TRAIT
		| SyntaxKind::MODULE
		| SyntaxKind::STRUCT
		| SyntaxKind::ENUM
		| SyntaxKind::SOURCE_FILE => SemanticKind::RelationalTable,

		// Statement-level items → RelationalRow
		SyntaxKind::LET_STMT
		| SyntaxKind::EXPR_STMT
		| SyntaxKind::USE
		| SyntaxKind::CONST
		| SyntaxKind::STATIC
		| SyntaxKind::TYPE_ALIAS
		| SyntaxKind::MACRO_DEF
		| SyntaxKind::MACRO_RULES
		| SyntaxKind::MACRO_CALL => SemanticKind::RelationalRow,

		// Everything else: if it has children it gets classified by
		// the recursive walk; leaf tokens become Token.
		_ => SemanticKind::Token,
	}
}

fn build_green_recursive(node: &ra_ap_syntax::SyntaxNode) -> GreenNode {
	use ra_ap_syntax::NodeOrToken;

	let kind = rust_kind_to_semantic(node.kind());
	let byte_len = u32::from(node.text_range().len());
	let mut children = Vec::new();

	for child in node.children_with_tokens() {
		match child {
			NodeOrToken::Node(child_node) => {
				children.push(build_green_recursive(&child_node));
			}
			NodeOrToken::Token(token) => {
				let tok_len = u32::from(token.text_range().len());
				if tok_len > 0 {
					children.push(GreenNode {
						kind: SemanticKind::Token,
						byte_len: tok_len,
						children: Vec::new(),
					});
				}
			}
		}
	}

	GreenNode {
		kind,
		byte_len,
		children,
	}
}

// ============================================================
// Workspace initialization
// ============================================================

fn path_is_excluded(path: &AbsPath, excluded: &[AbsPathBuf]) -> bool {
	excluded.iter().any(|excluded| path.starts_with(excluded))
}

fn collect_rust_files(path: AbsPathBuf, excluded: &[AbsPathBuf], files: &mut HashSet<AbsPathBuf>) {
	if path_is_excluded(path.as_ref(), excluded) {
		return;
	}

	let Ok(metadata) = std::fs::metadata(&path) else {
		return;
	};
	if metadata.is_file() {
		if path.extension().is_some_and(|extension| extension == "rs") {
			files.insert(path);
		}
		return;
	}
	if !metadata.is_dir() {
		return;
	}

	let Ok(entries) = std::fs::read_dir(&path) else {
		return;
	};
	for entry in entries.flatten() {
		let Ok(path) = Utf8PathBuf::try_from(entry.path()) else {
			continue;
		};
		collect_rust_files(AbsPathBuf::assert(path), excluded, files);
	}
}

fn load_workspace_sources(vfs: &mut Vfs, roots: &[PackageRoot]) {
	let mut files = HashSet::new();
	for root in roots {
		for include in &root.include {
			collect_rust_files(include.clone(), &root.exclude, &mut files);
		}
	}

	for path in files {
		let contents = std::fs::read(&path).ok();
		vfs.set_file_contents(VfsPath::from(path), contents);
	}
}

fn package_root_specificity(root: &PackageRoot, path: &AbsPath) -> Option<usize> {
	if path_is_excluded(path, &root.exclude) {
		return None;
	}
	root.include
		.iter()
		.filter(|include| path.starts_with(include))
		.map(|include| include.as_str().len())
		.max()
}

fn package_root_index(package_roots: &[PackageRoot], path: &AbsPath) -> Option<usize> {
	package_roots
		.iter()
		.enumerate()
		.filter_map(|(index, root)| {
			package_root_specificity(root, path).map(|specificity| (index, specificity))
		})
		.max_by_key(|(_, specificity)| *specificity)
		.map(|(index, _)| index)
}

fn roots_for_workspace(
	workspace: &ProjectWorkspace,
	package_roots: &[PackageRoot],
	vfs: &Vfs,
	crate_graph: &CrateGraphBuilder,
) -> Vec<PackageRoot> {
	let mut selected = package_roots
		.iter()
		.enumerate()
		.filter_map(|(index, root)| root.is_local.then_some(index))
		.collect::<HashSet<_>>();

	if let ProjectWorkspaceKind::Cargo { cargo, .. } = &workspace.kind {
		for package in cargo.packages().filter(|package| cargo[*package].is_local) {
			for dependency in &cargo[package].dependencies {
				// ra_ap_project_model does not publicly re-export DepKind. Its Debug
				// spelling is stable in the pinned rust-analyzer version; keep dev and
				// build edges out of the eager source-root policy.
				if format!("{:?}", dependency.kind) != "Normal" {
					continue;
				}
				let dependency_root = cargo[dependency.pkg].manifest.parent();
				if let Some(index) = package_root_index(package_roots, dependency_root) {
					selected.insert(index);
				}
			}
		}
	}

	for crate_id in crate_graph.iter() {
		let crate_root = vfs.file_path(crate_graph[crate_id].basic.root_file_id);
		let is_lang_crate = crate_graph[crate_id]
			.extra
			.display_name
			.as_ref()
			.is_some_and(|name| matches!(name.to_string().as_str(), "std" | "core" | "alloc"));
		if is_lang_crate
			&& let Some(index) = crate_root
				.as_path()
				.and_then(|path| package_root_index(package_roots, path))
		{
			selected.insert(index);
		}
	}
	let mut selected: Vec<_> = selected.into_iter().collect();
	selected.sort_unstable();
	selected
		.into_iter()
		.map(|index| package_roots[index].clone())
		.collect()
}

fn workspace_source_roots(vfs: &Vfs, roots: &[PackageRoot]) -> Vec<SourceRoot> {
	let mut config = FileSetConfig::builder();
	for root in roots {
		config.add_file_set(root.include.iter().cloned().map(VfsPath::from).collect());
	}

	config
		.build()
		.partition(vfs)
		.into_iter()
		.enumerate()
		.filter_map(|(index, files)| {
			if files.len() == 0 {
				return None;
			}
			Some(
				if roots.get(index).is_some_and(|root| root.is_local) || index >= roots.len() {
					SourceRoot::new_local(files)
				} else {
					SourceRoot::new_library(files)
				},
			)
		})
		.collect()
}

#[cfg(test)]
fn try_init_workspace(file_path: &str) -> Result<(AnalysisHost, FileId), String> {
	let active_source = std::fs::read_to_string(file_path)
		.map_err(|error| format!("active Rust source could not be read: {error}"))?;
	let path = canonical_path(file_path).ok_or_else(|| "canonicalize failed".to_string())?;
	let mut workspace =
		initialize_workspace(path).ok_or_else(|| "workspace load failed".to_string())?;
	let file_id = *workspace
		.state
		.file_ids
		.get(&VfsPath::from(
			canonical_path(file_path).expect("canonical path"),
		))
		.ok_or_else(|| "active file missing from workspace vfs".to_string())?;
	let mut change = ChangeWithProcMacros::default();
	change.change_file(file_id, Some(active_source));
	workspace.state.host.apply_change(change);
	Ok((workspace.state.host, file_id))
}

fn initialize_workspace(abs_file: AbsPathBuf) -> Option<InitializedWorkspace> {
	let manifest = match ProjectManifest::discover_single(abs_file.as_ref()) {
		Ok(manifest) => manifest,
		Err(_) => ProjectManifest::discover_single(abs_file.parent()?).ok()?,
	};

	let load_workspace = |cargo_config: &CargoConfig| {
		ProjectWorkspace::load(manifest.clone(), cargo_config, &|_| {}).ok()
	};
	let discover_sysroot = CargoConfig {
		sysroot: Some(RustLibSource::Discover),
		..CargoConfig::default()
	};
	let workspace =
		load_workspace(&discover_sysroot).or_else(|| load_workspace(&CargoConfig::default()))?;

	let mut vfs = Vfs::default();
	let extra_env: FxHashMap<String, Option<String>> = FxHashMap::default();
	let (crate_graph, _proc_macros) = workspace.to_crate_graph(
		&mut |path: &AbsPath| {
			let vfs_path = VfsPath::from(path.to_path_buf());
			if vfs.file_id(&vfs_path).is_none() {
				vfs.set_file_contents(vfs_path.clone(), std::fs::read(path).ok());
			}
			vfs.file_id(&vfs_path)
				.and_then(|(fid, exc)| (exc == ra_ap_vfs::FileExcluded::No).then_some(fid))
		},
		&extra_env,
	);
	let package_roots = workspace.to_roots();
	let loaded_roots = roots_for_workspace(&workspace, &package_roots, &vfs, &crate_graph);
	load_workspace_sources(&mut vfs, &loaded_roots);

	let active_vfs_path = VfsPath::from(abs_file.clone());
	if vfs.file_id(&active_vfs_path).is_none() {
		vfs.set_file_contents(active_vfs_path.clone(), std::fs::read(&abs_file).ok());
	}

	let mut change = ChangeWithProcMacros::default();
	for (_, changed) in vfs.take_changes() {
		match changed.change {
			ra_ap_vfs::Change::Create(contents, _) | ra_ap_vfs::Change::Modify(contents, _) => {
				if let Ok(text) = String::from_utf8(contents) {
					change.change_file(changed.file_id, Some(text));
				}
			}
			ra_ap_vfs::Change::Delete => {}
		}
	}

	// `to_crate_graph` inserts crate-root files for transitive dependencies too.
	// Partition against every package root so those files retain library
	// durability instead of falling into FileSetConfig's local catch-all.
	change.set_roots(workspace_source_roots(&vfs, &package_roots));
	change.set_crate_graph(crate_graph);

	let mut host = AnalysisHost::new(None);
	host.apply_change(change);

	let file_ids = vfs
		.iter()
		.map(|(file_id, path)| (path.clone(), file_id))
		.collect();
	let key = WorkspaceKey(workspace.workspace_root().to_path_buf());
	let (member_roots, manifests) = workspace_aliases(&workspace);
	let fingerprint_paths = manifests
		.iter()
		.cloned()
		.chain(std::iter::once(key.0.join("Cargo.lock")))
		.map(|path| (path, None))
		.collect::<Vec<_>>();

	Some(InitializedWorkspace {
		key,
		state: WorkspaceState {
			host,
			file_ids,
			diagnostic_mode: DiagnosticCollectionMode::Full,
			fingerprint: fingerprint_for(&fingerprint_paths),
		},
		member_roots,
		manifests,
	})
}

fn workspace_aliases(workspace: &ProjectWorkspace) -> (Vec<AbsPathBuf>, Vec<AbsPathBuf>) {
	let mut roots = vec![workspace.workspace_root().to_path_buf()];
	let mut manifests = vec![workspace.workspace_root().join("Cargo.toml")];
	if let ProjectWorkspaceKind::Cargo { cargo, .. } = &workspace.kind {
		for package in cargo.packages().filter(|package| cargo[*package].is_member) {
			let manifest = AsRef::<AbsPath>::as_ref(&cargo[package].manifest).to_path_buf();
			if let Some(root) = manifest.parent() {
				roots.push(root.to_path_buf());
			}
			manifests.push(manifest);
		}
	}
	roots.sort();
	roots.dedup();
	manifests.sort();
	manifests.dedup();
	(roots, manifests)
}

/// Fallback: single-file universe with no dependency resolution.
pub(crate) fn init_single_file(initial_text: &str) -> (AnalysisHost, FileId) {
	let file_id = FileId::from_raw(0);
	let mut host = AnalysisHost::new(None);

	let mut file_set = FileSet::default();
	file_set.insert(
		file_id,
		VfsPath::new_virtual_path("/baryon/active.rs".into()),
	);
	let source_root = SourceRoot::new_local(file_set);

	let mut crate_graph = CrateGraphBuilder::default();
	let proc_macro_cwd = TriArc::new(AbsPathBuf::assert("/tmp".into()));
	let ws_data = TriArc::new(CrateWorkspaceData {
		target: Err("not loaded".into()),
		toolchain: None,
	});

	crate_graph.add_crate_root(
		file_id,
		Edition::Edition2024,
		None,
		None,
		CfgOptions::default(),
		None,
		Env::default(),
		CrateOrigin::Local {
			repo: None,
			name: None,
		},
		Vec::new(),
		false,
		proc_macro_cwd,
		ws_data,
	);

	let mut change = ChangeWithProcMacros::default();
	change.set_roots(vec![source_root]);
	change.set_crate_graph(crate_graph);
	change.change_file(file_id, Some(initial_text.to_owned()));
	host.apply_change(change);

	(host, file_id)
}

// ============================================================
// Analysis helpers
// ============================================================

fn diagnostics_config() -> DiagnosticsConfig {
	let mut config = DiagnosticsConfig::test_sample();
	config.proc_macros_enabled = true;
	config.proc_attr_macros_enabled = true;
	config
}

pub(crate) fn highlight_config() -> HighlightConfig<'static> {
	HighlightConfig {
		strings: true,
		comments: true,
		punctuation: true,
		specialize_punctuation: false,
		operator: true,
		specialize_operator: false,
		inject_doc_comment: false,
		macro_bang: true,
		syntactic_name_ref_highlighting: true,
		ra_fixture: RaFixtureConfig::default(),
	}
}

fn collect_error_diagnostics(
	analysis: &Analysis,
	active_file_id: FileId,
	content: &str,
	global_offset: DocByte,
	config: &DiagnosticsConfig,
	mode: DiagnosticCollectionMode,
) -> Cancellable<Vec<DiagnosticSpan>> {
	let diagnostics = match mode {
		DiagnosticCollectionMode::Full => {
			analysis.full_diagnostics(config, AssistResolveStrategy::None, active_file_id)?
		}
		DiagnosticCollectionMode::SyntaxOnly => {
			analysis.syntax_diagnostics(config, active_file_id)?
		}
	};
	Ok(diagnostics
		.into_iter()
		.filter_map(|diagnostic| {
			if diagnostic.range.file_id != active_file_id || diagnostic.severity != Severity::Error
			{
				return None;
			}

			let start = u32::from(diagnostic.range.range.start()) as usize;
			let end = u32::from(diagnostic.range.range.end()) as usize;
			let (start, end) = normalize_diagnostic_range(content, start, end)?;
			Some(DiagnosticSpan::new(
				global_offset.saturating_add(start as u64),
				global_offset.saturating_add(end as u64),
				DiagnosticSeverity::Error,
			))
		})
		.collect())
}

fn normalize_diagnostic_range(content: &str, start: usize, end: usize) -> Option<(usize, usize)> {
	let len = content.len();
	let start = start.min(len);
	let end = end.min(len);
	if end > start {
		return Some((start, end));
	}

	if start < len {
		let next = start + content[start..].chars().next()?.len_utf8();
		return Some((start, next));
	}

	if start > 0 {
		let prev = content[..start].char_indices().next_back()?.0;
		return Some((prev, start));
	}

	None
}

pub(crate) fn map_hl_tag(highlight: Highlight) -> TokenCategory {
	let mods = highlight.mods;
	let tag = highlight.tag;

	if (mods.contains(HlMod::CrateRoot) || mods.contains(HlMod::Library))
		&& matches!(
			tag,
			HlTag::Symbol(SymbolKind::Module | SymbolKind::CrateRoot)
		) {
		return TokenCategory::Crate;
	}

	if mods.contains(HlMod::Mutable)
		&& matches!(
			tag,
			HlTag::Symbol(SymbolKind::Local | SymbolKind::ValueParam | SymbolKind::Field)
		) {
		return TokenCategory::MutableVariable;
	}

	match tag {
		HlTag::Symbol(SymbolKind::Method) => TokenCategory::Method,
		HlTag::Symbol(SymbolKind::Function) => TokenCategory::Function,
		HlTag::Symbol(
			SymbolKind::Struct
			| SymbolKind::Enum
			| SymbolKind::Union
			| SymbolKind::TypeAlias
			| SymbolKind::TypeParam
			| SymbolKind::Trait,
		) => TokenCategory::Type,
		HlTag::Symbol(SymbolKind::SelfParam | SymbolKind::SelfType) => TokenCategory::SelfKeyword,
		HlTag::Symbol(SymbolKind::Local | SymbolKind::ValueParam | SymbolKind::Field) => {
			TokenCategory::Variable
		}
		HlTag::Symbol(
			SymbolKind::Const | SymbolKind::Static | SymbolKind::ConstParam | SymbolKind::Variant,
		) => TokenCategory::Constant,
		HlTag::Symbol(SymbolKind::Macro | SymbolKind::ProcMacro) => TokenCategory::Macro,
		HlTag::Symbol(SymbolKind::Module | SymbolKind::CrateRoot | SymbolKind::ToolModule) => {
			TokenCategory::Module
		}
		HlTag::Symbol(SymbolKind::LifetimeParam) => TokenCategory::Lifetime,
		HlTag::Symbol(
			SymbolKind::Attribute
			| SymbolKind::BuiltinAttr
			| SymbolKind::Derive
			| SymbolKind::DeriveHelper,
		) => TokenCategory::Attribute,
		HlTag::Symbol(_) => TokenCategory::Unclassified,
		HlTag::BuiltinType => TokenCategory::BuiltinType,
		HlTag::Keyword | HlTag::BoolLiteral => TokenCategory::Keyword,
		HlTag::Comment => TokenCategory::Comment,
		HlTag::StringLiteral
		| HlTag::ByteLiteral
		| HlTag::CharLiteral
		| HlTag::EscapeSequence
		| HlTag::InvalidEscapeSequence
		| HlTag::FormatSpecifier => TokenCategory::String,
		HlTag::NumericLiteral => TokenCategory::Number,
		HlTag::Operator(_) => TokenCategory::Operator,
		HlTag::Punctuation(_) => TokenCategory::Punctuation,
		HlTag::AttributeBracket => TokenCategory::Attribute,
		HlTag::UnresolvedReference | HlTag::None => TokenCategory::Unclassified,
	}
}

// ============================================================
// Tests
// ============================================================

#[cfg(test)]
mod tests {
	use super::*;
	use std::collections::HashMap;

	fn empty_provider_state() -> ProviderState {
		ProviderState {
			workspaces: HashMap::new(),
			member_roots: HashMap::new(),
			manifest_aliases: HashMap::new(),
			single_files: HashMap::new(),
		}
	}

	fn syntax_only_workspace_state(fingerprint: WorkspaceFingerprint) -> WorkspaceState {
		let (host, file_id) = init_single_file("fn example() {}\n");
		let mut file_ids = HashMap::new();
		file_ids.insert(VfsPath::new_virtual_path("/baryon/test.rs".into()), file_id);
		WorkspaceState {
			host,
			file_ids,
			diagnostic_mode: DiagnosticCollectionMode::SyntaxOnly,
			fingerprint,
		}
	}

	#[test]
	fn workspace_alias_lookup_prefers_the_most_specific_member_root() {
		let parent = WorkspaceKey(AbsPathBuf::assert("/workspace".into()));
		let child = WorkspaceKey(AbsPathBuf::assert("/workspace/tools/nested".into()));
		let mut state = empty_provider_state();
		state
			.member_roots
			.insert(AbsPathBuf::assert("/workspace".into()), parent);
		state.member_roots.insert(
			AbsPathBuf::assert("/workspace/tools/nested".into()),
			child.clone(),
		);

		assert_eq!(
			state.workspace_key_for_path(&AbsPathBuf::assert(
				"/workspace/tools/nested/src/lib.rs".into(),
			)),
			Some(child)
		);
	}

	#[test]
	fn workspace_member_roots_share_one_cached_workspace_entry() {
		let key = WorkspaceKey(AbsPathBuf::assert("/workspace".into()));
		let mut state = empty_provider_state();
		state.workspaces.insert(
			key.clone(),
			syntax_only_workspace_state(WorkspaceFingerprint(Vec::new())),
		);
		for root in ["/workspace/member-a", "/workspace/member-b"] {
			state
				.member_roots
				.insert(AbsPathBuf::assert(root.into()), key.clone());
		}

		for path in [
			"/workspace/member-a/src/lib.rs",
			"/workspace/member-b/src/lib.rs",
		] {
			assert_eq!(
				state.workspace_key_for_path(&AbsPathBuf::assert(path.into())),
				Some(key.clone())
			);
		}
		assert_eq!(state.workspaces.len(), 1);
	}

	#[test]
	fn fingerprint_distinguishes_a_missing_file_from_an_empty_file() {
		let path = std::env::temp_dir().join(format!(
			"baryon-workspace-fingerprint-{}-{}",
			std::process::id(),
			std::thread::current().name().unwrap_or("test")
		));
		let path = AbsPathBuf::assert(Utf8PathBuf::try_from(path).expect("utf8 temp path"));
		let paths = vec![(path.clone(), None)];
		let missing = fingerprint_for(&paths);
		std::fs::write(&path, []).expect("write empty fingerprint file");
		let present = fingerprint_for(&paths);
		let _ = std::fs::remove_file(&path);

		assert_ne!(missing, present);
	}

	#[test]
	fn manifest_fingerprint_change_invalidates_its_workspace_entry() {
		let path = std::env::temp_dir().join(format!(
			"baryon-workspace-fingerprint-change-{}-{}",
			std::process::id(),
			std::thread::current().name().unwrap_or("test")
		));
		let path = AbsPathBuf::assert(Utf8PathBuf::try_from(path).expect("utf8 temp path"));
		std::fs::write(&path, "[package]\nname = \"before\"\n").expect("write initial manifest");
		let paths = vec![(path.clone(), None)];
		let key = WorkspaceKey(AbsPathBuf::assert("/workspace".into()));
		let mut state = empty_provider_state();
		state.workspaces.insert(
			key.clone(),
			syntax_only_workspace_state(fingerprint_for(&paths)),
		);

		assert!(!state.workspace_fingerprint_changed(&key));
		std::fs::write(&path, "[package]\nname = \"after\"\n").expect("rewrite manifest");
		assert!(state.workspace_fingerprint_changed(&key));
		let _ = std::fs::remove_file(&path);
	}

	#[test]
	fn cached_single_file_is_reused_without_workspace_reinitialization() {
		let mut state = empty_provider_state();
		state.single_files.insert(
			"/workspace/target/generated.rs".into(),
			syntax_only_workspace_state(WorkspaceFingerprint(Vec::new())),
		);

		let (fallback, _) = state
			.cached_single_file("/workspace/target/generated.rs")
			.expect("cached fallback");
		assert_eq!(
			fallback.diagnostic_mode,
			DiagnosticCollectionMode::SyntaxOnly
		);
		assert!(state.workspaces.is_empty());
	}

	#[test]
	fn switching_workspace_files_preserves_unsaved_edits_in_the_shared_host() {
		let (mut host, first_file) = init_single_file("fn before() {}\n");
		let second_file = FileId::from_raw(1);
		let first_path = VfsPath::new_virtual_path("/workspace/src/first.rs".into());
		let second_path = VfsPath::new_virtual_path("/workspace/src/second.rs".into());
		let mut file_set = FileSet::default();
		file_set.insert(first_file, first_path.clone());
		file_set.insert(second_file, second_path.clone());
		let mut roots_change = ChangeWithProcMacros::default();
		roots_change.set_roots(vec![SourceRoot::new_local(file_set)]);
		host.apply_change(roots_change);
		let mut file_ids = HashMap::new();
		file_ids.insert(first_path, first_file);
		file_ids.insert(second_path, second_file);
		let mut workspace = WorkspaceState {
			host,
			file_ids,
			diagnostic_mode: DiagnosticCollectionMode::SyntaxOnly,
			fingerprint: WorkspaceFingerprint(Vec::new()),
		};

		analyze_workspace_file(
			&mut workspace,
			first_file,
			"fn after_unsaved_edit() {}\n",
			DocByte::ZERO,
		);
		analyze_workspace_file(
			&mut workspace,
			second_file,
			"fn second_file() {}\n",
			DocByte::ZERO,
		);

		assert_eq!(
			workspace
				.host
				.analysis()
				.file_text(first_file)
				.expect("first file text")
				.as_ref(),
			"fn after_unsaved_edit() {}\n"
		);
	}

	#[test]
	fn external_crate_roots_remain_library_source_roots() {
		let local = AbsPathBuf::assert("/workspace/src/lib.rs".into());
		let external = AbsPathBuf::assert("/cargo/registry/dep/src/lib.rs".into());
		let mut vfs = Vfs::default();
		let local_path = VfsPath::from(local.clone());
		let external_path = VfsPath::from(external.clone());
		vfs.set_file_contents(local_path.clone(), Some(b"pub fn local() {}".to_vec()));
		vfs.set_file_contents(
			external_path.clone(),
			Some(b"pub fn dependency() {}".to_vec()),
		);
		let external_file = vfs.file_id(&external_path).expect("external file").0;
		let package_roots = vec![
			PackageRoot {
				is_local: true,
				include: vec![AbsPathBuf::assert("/workspace".into())],
				exclude: Vec::new(),
			},
			PackageRoot {
				is_local: false,
				include: vec![AbsPathBuf::assert("/cargo/registry/dep".into())],
				exclude: Vec::new(),
			},
		];

		let roots = workspace_source_roots(&vfs, &package_roots);
		let external_root = roots
			.iter()
			.find(|root| root.path_for_file(&external_file).is_some())
			.expect("external file is partitioned into a source root");
		assert!(external_root.is_library);
	}

	#[cfg(unix)]
	#[test]
	fn canonical_paths_unify_symlinked_file_identity() {
		use std::os::unix::fs::symlink;

		let directory = std::env::temp_dir().join(format!(
			"baryon-canonical-path-{}-{}",
			std::process::id(),
			std::thread::current().name().unwrap_or("test")
		));
		std::fs::create_dir(&directory).expect("create temporary directory");
		let real = directory.join("real.rs");
		let alias = directory.join("alias.rs");
		std::fs::write(&real, "fn main() {}\n").expect("write source file");
		symlink(&real, &alias).expect("create source symlink");

		assert_eq!(
			canonical_path(real.to_str().expect("utf8 real path")),
			canonical_path(alias.to_str().expect("utf8 alias path"))
		);
		std::fs::remove_dir_all(&directory).expect("remove temporary directory");
	}

	#[test]
	fn invalid_rust_produces_visible_error_diagnostic_spans() {
		let source = "fn main() {\n";
		let (host, file_id) = init_single_file(source);
		let diagnostics = collect_error_diagnostics(
			&host.analysis(),
			file_id,
			source,
			DocByte::ZERO,
			&diagnostics_config(),
			DiagnosticCollectionMode::SyntaxOnly,
		)
		.expect("diagnostics should resolve");

		assert!(!diagnostics.is_empty());
		assert!(diagnostics.iter().all(|diag| diag.start < diag.end));
	}

	#[test]
	fn valid_builtin_derives_do_not_produce_error_diagnostics() {
		let source = "#[derive(Debug, Clone, Copy, PartialEq, Eq)]\nstruct Sample;\n";
		let (host, file_id) = init_single_file(source);
		let diagnostics = collect_error_diagnostics(
			&host.analysis(),
			file_id,
			source,
			DocByte::ZERO,
			&diagnostics_config(),
			DiagnosticCollectionMode::SyntaxOnly,
		)
		.expect("diagnostics should resolve");

		assert!(
			diagnostics.is_empty(),
			"valid derive should not produce error diagnostics: {diagnostics:?}"
		);
	}

	#[test]
	fn empty_diagnostic_ranges_expand_to_a_visible_span() {
		assert_eq!(normalize_diagnostic_range("abc", 3, 3), Some((2, 3)));
		assert_eq!(normalize_diagnostic_range("abc", 1, 1), Some((1, 2)));
		assert_eq!(normalize_diagnostic_range("", 0, 0), None);
	}

	#[test]
	fn workspace_diagnostics_resolve_frontend_module_tree() {
		let relative_path = "../baryon/src/app.rs";
		let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative_path);
		let source = std::fs::read_to_string(&path)
			.unwrap_or_else(|_| panic!("{relative_path} should be readable"));
		let path_str = path.to_string_lossy().into_owned();
		let (mut host, file_id) =
			try_init_workspace(&path_str).expect("workspace mode should load baryon");
		let mut change = ChangeWithProcMacros::default();
		change.change_file(file_id, Some(source.clone()));
		host.apply_change(change);

		let diagnostics = collect_error_diagnostics(
			&host.analysis(),
			file_id,
			&source,
			DocByte::ZERO,
			&diagnostics_config(),
			DiagnosticCollectionMode::Full,
		)
		.expect("workspace diagnostics should resolve");
		assert!(
			diagnostics.is_empty(),
			"workspace diagnostics should resolve {relative_path}: {diagnostics:?}"
		);
	}

	#[test]
	fn workspace_semantics_resolve_engine_helper_imports() {
		let assert_import_highlighted =
			|relative_path: &str, import_line: &str, import_name: &str| {
				let path = PathBuf::from(env!("CARGO_MANIFEST_DIR")).join(relative_path);
				let source = std::fs::read_to_string(&path)
					.unwrap_or_else(|_| panic!("{relative_path} should be readable"));
				let path_str = path.to_string_lossy().into_owned();
				let (mut host, file_id) =
					try_init_workspace(&path_str).expect("workspace mode should load baryon");
				let mut change = ChangeWithProcMacros::default();
				change.change_file(file_id, Some(source.clone()));
				host.apply_change(change);
				let analysis = host.analysis();

				let diagnostics = collect_error_diagnostics(
					&analysis,
					file_id,
					&source,
					DocByte::ZERO,
					&diagnostics_config(),
					DiagnosticCollectionMode::Full,
				)
				.expect("workspace diagnostics should resolve");
				assert!(
					diagnostics.is_empty(),
					"workspace diagnostics should not flag {relative_path} imports: {diagnostics:?}"
				);

				let import_start = source.find(import_line).unwrap_or_else(|| {
					panic!("{relative_path} should contain import line {import_line}")
				});
				let import_name_offset = import_start
					+ source[import_start..]
						.find(import_name)
						.unwrap_or_else(|| panic!("{relative_path} should import {import_name}"));

				let highlights = analysis
					.highlight(highlight_config(), file_id)
					.expect("workspace highlights should resolve");
				let category_for = |offset: usize| {
					highlights
						.iter()
						.find(|hl| {
							let start = u32::from(hl.range.start()) as usize;
							let end = u32::from(hl.range.end()) as usize;
							start <= offset && offset < end
						})
						.map(|hl| map_hl_tag(hl.highlight))
				};

				assert!(
					matches!(
						category_for(import_name_offset),
						Some(TokenCategory::Function | TokenCategory::Module)
					),
					"{relative_path} should semantically resolve import {import_name}"
				);
			};

		assert_import_highlighted(
			"src/engine/runtime/system.rs",
			"use super::super::folding::{",
			"resolve_fold_boundary_at_cursor",
		);
		assert_import_highlighted(
			"src/engine/runtime/navigation.rs",
			"use super::super::layout::{",
			"viewport_geometry_for_viewport",
		);
	}

	#[test]
	fn structural_topology_maps_fn_to_relational_table() {
		let source = "fn foo() {}\nfn bar() {}\n";
		let green = build_rust_green_tree(source);
		// Root is SOURCE_FILE → RelationalTable
		assert_eq!(green.kind, SemanticKind::RelationalTable);
		assert_eq!(green.byte_len, source.len() as u32);

		// Should contain two FN children mapped to RelationalTable
		let fn_children: Vec<_> = green
			.children
			.iter()
			.filter(|c| c.kind == SemanticKind::RelationalTable)
			.collect();
		assert_eq!(fn_children.len(), 2, "expected 2 function nodes");
		assert_eq!(fn_children[0].byte_len, 11); // "fn foo() {}"
		assert_eq!(fn_children[1].byte_len, 11); // "fn bar() {}"
	}

	#[test]
	fn structural_topology_graft_enables_function_query() {
		use crate::ecs::registry::UastRegistry;
		use crate::engine::structural_graft::apply_graft;
		use crate::engine::uast_query::{UastQuery, is_function_scope};
		use crate::uast::metrics::SpanMetrics;

		let source = b"fn foo() {}\nfn bar() {}\n";
		let green = build_rust_green_tree(std::str::from_utf8(source).unwrap());

		let registry = UastRegistry::new(128);
		let root = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: 2,
			};
		}

		apply_graft(&registry, root, &green, source).expect("test graft is byte-identical");

		let q = UastQuery::new(&registry, root);

		// Cursor inside foo (byte 5) → find enclosing function
		// FN node is "fn foo() {}" = 11 bytes, trailing \n is a separate whitespace token
		let leaf = q.node_at_offset(DocByte::new(5)).unwrap();
		let func = q.find_ancestor(leaf, is_function_scope).unwrap();
		let (start, end) = q.node_bounds(func);
		assert_eq!(start, DocByte::new(0));
		assert_eq!(end, DocByte::new(11));

		// ]m from byte 5 → next function is bar at byte 12 (after 11-byte foo + 1-byte newline)
		let next = q.next_of_kind(DocByte::new(5), is_function_scope).unwrap();
		let (start2, _) = q.node_bounds(next);
		assert_eq!(start2, DocByte::new(12));
	}

	#[test]
	fn structural_graft_preserves_all_bytes_and_newlines() {
		use crate::ecs::registry::UastRegistry;
		use crate::engine::structural_graft::apply_graft;
		use crate::uast::metrics::SpanMetrics;

		let source = std::fs::read("src/engine/structural_graft.rs")
			.expect("should read structural_graft.rs");
		let text = std::str::from_utf8(&source).unwrap();
		let green = build_rust_green_tree(text);

		// Verify green node covers the full source
		assert_eq!(
			green.byte_len as usize,
			source.len(),
			"GreenNode byte_len must equal source length"
		);

		let expected_newlines = source.iter().filter(|&&b| b == b'\n').count() as u64;

		let registry = UastRegistry::new(
			crate::engine::structural_graft::graft_allocation_count(&green)
				.saturating_add(1)
				.try_into()
				.expect("test graft fits u32 registry capacity"),
		);
		let root = registry.alloc_node_internal();
		unsafe {
			*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
			*registry.metrics[root.index()].get() = SpanMetrics {
				byte_length: source.len() as u64,
				newlines: expected_newlines,
			};
		}

		let module =
			apply_graft(&registry, root, &green, &source).expect("test graft is byte-identical");

		// Grafted module must have the same byte_length and newlines as root
		let module_metrics = unsafe { *registry.metrics[module.index()].get() };
		assert_eq!(
			module_metrics.byte_length as usize,
			source.len(),
			"grafted module byte_length must equal source length"
		);
		assert_eq!(
			module_metrics.newlines, expected_newlines,
			"grafted module newlines must equal source newlines"
		);

		// Verify all bytes are accessible by reading the full document
		let mut reconstructed = Vec::with_capacity(source.len());
		fn collect_leaves(reg: &UastRegistry, node: crate::ecs::id::NodeId, out: &mut Vec<u8>) {
			let first_child = reg.get_first_child(node);
			if first_child.is_none() {
				// Leaf node: collect virtual_data
				let data = reg.resolve_physical_bytes(node);
				out.extend_from_slice(&data);
			} else {
				let mut child = first_child;
				while let Some(c) = child {
					collect_leaves(reg, c, out);
					child = reg.get_next_sibling(c);
				}
			}
		}
		collect_leaves(&registry, module, &mut reconstructed);
		assert_eq!(
			reconstructed.len(),
			source.len(),
			"reconstructed content length must equal source length"
		);
		assert_eq!(
			reconstructed, source,
			"reconstructed content must match source exactly"
		);
	}

	#[test]
	fn lexical_baseline_keeps_leading_import_block_colored() {
		use crate::engine::language::LanguageProvider;
		let provider = RustProvider::new();
		let source = b"use std::io;\nuse std::fs;\n\nfn main() {}\n";
		let highlights = provider.lexical_baseline(source, &[(0, DocByte::ZERO)]);

		assert!(highlights.iter().any(|span| {
			span.category == TokenCategory::Keyword
				&& span.start == DocByte::ZERO
				&& span.end == DocByte::new(3)
		}));
		assert!(
			highlights
				.iter()
				.any(|span| { span.category == TokenCategory::Keyword && span.start.get() > 20 })
		);
	}
}
