mod edit;
mod navigation;
mod search;
mod system;

use self::edit::{
	handle_clipboard_doctab_command, handle_edit_doctab_command, handle_history_doctab_command,
	handle_sort_doctab_command, handle_uniq_doctab_command,
};
use self::navigation::{handle_file_browser_navigation_command, handle_navigation_doctab_command};
use self::search::handle_search_doctab_command;
use self::system::{
	apply_resolved_theme_to_tab, handle_auto_reload_doctab_command, handle_file_doctab_command,
	handle_fold_doctab_command, handle_language_doctab_command, handle_persist_view_doctab_command,
	handle_view_doctab_command,
};
pub(crate) use self::system::{
	re_resolve_auto_themes, set_line_numbers_local, set_minimap_local,
	set_relative_line_numbers_local, set_visible_whitespace_local, set_wrap_local,
};
use super::core::Engine;
use super::document::io::read_document_snapshot;
use super::event_bus::{EngineEvent, EngineEventQueue};
use super::event_subscribers::{
	BaselineContributor, BoundedStatus, SubscriberBaseline, SubscriberDispatcher,
	SubscriberOutcome, drain_engine_subscribers_combined,
};
use super::file_browser::{
	BrowserEntryAction, activate_selected_browser_entry, click_browser_entry,
	collapse_selected_browser_entry_or_parent, create_file_browser_buffer, initialize_file_browser,
	refresh_file_browser,
};
use super::git_observer::GitRefreshController;
use super::input::{InputContext, InputDispatch};
use super::jobs::{JobCompletion, JobDispatcher, JobInvocation, JobSpec, MAX_JOB_DELIVERY_STEPS};
use super::language::{DefinitionLocation, DefinitionResult};
use super::markdown_image_preview::{
	open_markdown_image_preview, open_static_markdown_preview, scroll_pinned_preview,
	scroll_target_view as markdown_preview_source_target, sync_markdown_image_preview_scroll,
};
use super::navigation::{GotoDefinitionRequest, JumpEntry, NavigationResolved, NavigationStamp};
use super::protocol::{
	ConfirmAction, EditorCommand, EditorMode, MouseIntent, MoveDirection, SplitDirection,
};
use super::render::{RenderParams, render_frame};
use super::request::{EngineRequest, InternalEvent};
use super::rune_host::{
	EditorHost, HostServices, RuneAutocmdRef, RuneCallbackRef, RuneHost, StartupScriptReport,
	lock_services,
};
use super::runtime_registry::{ActionError, InvocationGate};
use super::session::{
	Affinity, BufferContent, BufferStore, CachedSumTree, CanonicalPath, EditorPane, EditorSession,
	LayoutNode, PaneResizeResult, PendingReload, SemanticState, TextEditorMut, View, ViewContent,
	text_editor, with_file_browser_buffer_mut, with_text_buffer_mut,
};
use super::settings::SaveDurability;
use super::shadow::discard_shadow_session;
use super::ui_colors::UiColorKey;
use crate::core::{BufferId, DocByte, DocLine, PaneId, RequestId, StateId};
use crate::ecs::UastRegistry;
use crate::svp::SvpResolver;
use crate::svp::diagnostic::DiagnosticSpan;
use crate::svp::rust_highlight::HighlightSpan;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex, mpsc};
use std::time::{Duration, Instant};

struct RunLoopSubscriberDispatcher<'a> {
	fallback: &'a mut (dyn SubscriberDispatcher<EditorSession> + Send),
	rune_host: Option<&'a mut RuneHost>,
}

impl BaselineContributor for RunLoopSubscriberDispatcher<'_> {
	fn contribute(
		&self,
		builder: &mut super::event_subscribers::BaselineBuilder,
	) -> Result<(), super::event_subscribers::BaselineError> {
		if let Some(host) = self.rune_host.as_deref() {
			host.contribute(builder)?;
		}
		Ok(())
	}
}

impl SubscriberDispatcher<EditorHost> for RunLoopSubscriberDispatcher<'_> {
	fn owner_is_live(&self, _owner: crate::core::RegistryOwnerId) -> bool {
		// Exact registry liveness is checked before the invocation is built. The
		// route-specific callback store check happens below after inspecting the
		// typed lease payload.
		true
	}

	fn initialize(
		&mut self,
		context: &mut EditorHost,
		invocation: super::runtime_registry::SubscriberInvocation,
		baseline: &Arc<SubscriberBaseline>,
	) -> SubscriberOutcome {
		if invocation.action::<RuneAutocmdRef>().is_some() {
			return match self.rune_host.as_deref_mut() {
				Some(host) => host.initialize(context, invocation, baseline),
				None => SubscriberOutcome::failed(BoundedStatus::fixed(
					"Rune autocmd host is unavailable",
				)),
			};
		}
		if !self.fallback.owner_is_live(invocation.owner()) {
			return SubscriberOutcome::failed(BoundedStatus::fixed(
				"Runtime event subscriber owner is retired",
			));
		}
		self.fallback
			.initialize(&mut context.session, invocation, baseline)
	}

	fn deliver(
		&mut self,
		context: &mut EditorHost,
		invocation: super::runtime_registry::SubscriberInvocation,
		event: &EngineEvent,
	) -> SubscriberOutcome {
		if invocation.action::<RuneAutocmdRef>().is_some() {
			return match self.rune_host.as_deref_mut() {
				Some(host) => host.deliver(context, invocation, event),
				None => SubscriberOutcome::failed(BoundedStatus::fixed(
					"Rune autocmd host is unavailable",
				)),
			};
		}
		if !self.fallback.owner_is_live(invocation.owner()) {
			return SubscriberOutcome::failed(BoundedStatus::fixed(
				"Runtime event subscriber owner is retired",
			));
		}
		self.fallback
			.deliver(&mut context.session, invocation, event)
	}
}

struct RunLoopJobDispatcher<'a> {
	fallback: &'a mut (dyn JobDispatcher<EditorSession> + Send),
	rune_host: Option<&'a mut RuneHost>,
}

impl JobDispatcher<EditorHost> for RunLoopJobDispatcher<'_> {
	fn owner_is_live(&self, _owner: crate::core::RegistryOwnerId) -> bool {
		true
	}

	fn complete(
		&mut self,
		context: &mut EditorHost,
		invocation: JobInvocation,
		completion: &JobCompletion,
	) -> SubscriberOutcome {
		if let Some(action) = invocation.action::<super::git::GitStatusAction>() {
			context.session.git_status_job = None;
			let refresh_pending = std::mem::take(&mut context.session.git_status_refresh_pending);
			let mut output = Vec::new();
			for chunk in completion.stdout.chunks() {
				output.extend_from_slice(chunk);
			}
			let mut branch = None;
			let (text, highlights) = match &completion.outcome {
				super::jobs::JobOutcome::Exited { status: 0 } => {
					match super::git::parse_status_porcelain(&output) {
						Ok(snapshot) => {
							branch = snapshot.branch.clone();
							super::git::render_status_snapshot(&snapshot)
						}
						Err(error) => (
							format!("Git status error: {error}\n").into_bytes(),
							Vec::new(),
						),
					}
				}
				_ => (b"Git status failed\n".to_vec(), Vec::new()),
			};
			context.session.git_status_branch = branch;
			let registry = context.registry().clone();
			if context
				.session
				.replace_output_panel(&registry, action.buffer_id, &text, highlights)
				.is_err()
			{
				return SubscriberOutcome::failed(BoundedStatus::fixed(
					"Git status panel is no longer available",
				));
			}
			if refresh_pending
				&& let Some(repo_root) = context.session.git_status_repo_root.clone()
				&& let Err(error) =
					start_git_status_job(&mut context.session, action.buffer_id, repo_root)
			{
				context.session.ui.status_message = Some(format!("Git status: {error}"));
			}
			return SubscriberOutcome {
				needs_render: true,
				foreground_status: None,
				result: super::event_subscribers::SubscriberResult::Delivered,
			};
		}
		if let Some(action) = invocation.action::<super::git::GitResolveAction>() {
			let action = action.clone();
			context.session.snapshot_fetch_job = None;
			let needs_render = complete_git_resolve_job(context, action, completion);
			return SubscriberOutcome {
				needs_render,
				foreground_status: None,
				result: super::event_subscribers::SubscriberResult::Delivered,
			};
		}
		if let Some(action) = invocation.action::<super::git::GitDiffAction>() {
			let action = action.clone();
			context.session.snapshot_fetch_job = None;
			let needs_render = complete_git_diff_job(context, action, completion);
			return SubscriberOutcome {
				needs_render,
				foreground_status: None,
				result: super::event_subscribers::SubscriberResult::Delivered,
			};
		}
		if let Some(action) = invocation.action::<super::git::GitOlderWalkAction>() {
			let action = action.clone();
			context.session.snapshot_fetch_job = None;
			let needs_render = complete_git_older_walk(context, action, completion);
			return SubscriberOutcome {
				needs_render,
				foreground_status: None,
				result: super::event_subscribers::SubscriberResult::Delivered,
			};
		}
		if let Some(action) = invocation.action::<super::git::GitNewerNormalizeAction>() {
			let action = action.clone();
			context.session.snapshot_fetch_job = None;
			let needs_render = complete_git_newer_normalize(context, action, completion);
			return SubscriberOutcome {
				needs_render,
				foreground_status: None,
				result: super::event_subscribers::SubscriberResult::Delivered,
			};
		}
		if let Some(action) = invocation.action::<super::git::GitNewerWalkAction>() {
			let action = action.clone();
			context.session.snapshot_fetch_job = None;
			let needs_render = complete_git_newer_walk(context, action, completion);
			return SubscriberOutcome {
				needs_render,
				foreground_status: None,
				result: super::event_subscribers::SubscriberResult::Delivered,
			};
		}
		if let Some(action) = invocation.action::<super::git::GitStepFetchAction>() {
			let action = action.clone();
			context.session.snapshot_fetch_job = None;
			let needs_render = complete_git_step_fetch(context, action, completion);
			return SubscriberOutcome {
				needs_render,
				foreground_status: None,
				result: super::event_subscribers::SubscriberResult::Delivered,
			};
		}
		if invocation.action::<RuneCallbackRef>().is_some() {
			return match self.rune_host.as_deref_mut() {
				Some(host) => host.complete_job(context, invocation, completion),
				None => SubscriberOutcome::failed(BoundedStatus::fixed(
					"Rune job binding is unavailable",
				)),
			};
		}
		if !self.fallback.owner_is_live(invocation.owner()) {
			return SubscriberOutcome::failed(BoundedStatus::fixed("Runtime job owner is retired"));
		}
		self.fallback
			.complete(&mut context.session, invocation, completion)
	}
}

/// Complete the pin phase of a snapshot acquisition: parse the pinned commit
/// OID, then start the fetch phase, which derives the blob and its content
/// from that immutable commit. Failures here are the "unknown revision"
/// surface.
fn complete_git_resolve_job(
	context: &mut EditorHost,
	action: super::git::GitResolveAction,
	completion: &JobCompletion,
) -> bool {
	let session = &mut context.session;
	match &completion.outcome {
		super::jobs::JobOutcome::Exited { status: 0 } => {}
		super::jobs::JobOutcome::Exited { .. } => {
			let mut stderr = Vec::new();
			for chunk in completion.stderr.chunks() {
				stderr.extend_from_slice(chunk);
			}
			let stderr = String::from_utf8_lossy(&stderr);
			let detail = stderr.lines().next().unwrap_or("git rev-parse failed").trim();
			session.ui.status_message = Some(format!("Not a file or Git revision: {detail}"));
			return true;
		}
		_ => {
			session.ui.status_message = Some("Git diff failed".to_string());
			return true;
		}
	}
	let mut stdout = Vec::new();
	for chunk in completion.stdout.chunks() {
		stdout.extend_from_slice(chunk);
	}
	let Some(commit_oid) = super::git::parse_single_oid(&stdout) else {
		session.ui.status_message = Some("Git revision resolution was unreadable".to_string());
		return true;
	};

	// Skip the fetch when the requesting document already changed.
	let left_intact = session
		.layout
		.find_pane(action.left_pane_id)
		.is_some_and(|pane| pane.active_tab().buffer_id == action.left_buffer_id);
	if !left_intact {
		session.ui.status_message = Some("Diff source changed; Git diff cancelled".to_string());
		return true;
	}

	// `--batch` takes its spec on stdin, so nothing here is parsed as an
	// option; the spec is `<hex-commit>:<relpath>` — fully immutable input.
	let spec = JobSpec {
		program: PathBuf::from("git"),
		args: vec![
			"--no-optional-locks".into(),
			"cat-file".into(),
			"--batch".into(),
		],
		cwd: Some(action.repo_root.clone()),
		stdin: Some(format!("{commit_oid}:{}\n", action.rel_path).into_bytes()),
		timeout: Duration::from_secs(10),
		completion: Some(super::git::git_diff_action_lease(
			super::git::GitDiffAction {
				left_pane_id: action.left_pane_id,
				left_buffer_id: action.left_buffer_id,
				repo_root: action.repo_root,
				revision: action.revision,
				rel_path: action.rel_path,
				commit_oid,
			},
		)),
	};
	match session.jobs.spawn(super::git::GIT_JOB_OWNER, true, spec) {
		Ok(job) => {
			session.snapshot_fetch_job = Some(job);
		}
		Err(error) => {
			session.ui.status_message = Some(format!("Git diff: {error}"));
		}
	}
	true
}

/// Materialize a completed `git cat-file --batch` fetch (blob OID + content,
/// both derived from the pinned commit) as the right-hand pane of a diff
/// pair: a vertical split holding an in-memory virtual document that carries
/// the left buffer's language and the repo-relative path as its virtual
/// identity, so the async semantic graft upgrades the diff from the
/// line-based fallback to the hybrid AST model exactly as file diffs do.
fn complete_git_diff_job(
	context: &mut EditorHost,
	action: super::git::GitDiffAction,
	completion: &JobCompletion,
) -> bool {
	match &completion.outcome {
		super::jobs::JobOutcome::Exited { status: 0 } => {}
		super::jobs::JobOutcome::Exited { .. } => {
			let mut stderr = Vec::new();
			for chunk in completion.stderr.chunks() {
				stderr.extend_from_slice(chunk);
			}
			let stderr = String::from_utf8_lossy(&stderr);
			let detail = stderr.lines().next().unwrap_or("git cat-file failed").trim();
			context.session.ui.status_message =
				Some(format!("Not a file or Git revision: {detail}"));
			return true;
		}
		_ => {
			context.session.ui.status_message = Some("Git diff failed".to_string());
			return true;
		}
	}
	if completion.stdout.truncated {
		context.session.ui.status_message =
			Some("Git snapshot is too large to diff".to_string());
		return true;
	}
	let mut stdout = Vec::new();
	for chunk in completion.stdout.chunks() {
		stdout.extend_from_slice(chunk);
	}
	let (blob_oid, blob_content) =
		match super::git::parse_batch_blob(&stdout, action.commit_oid.len()) {
			Ok(parsed) => parsed,
			Err(detail) => {
				context.session.ui.status_message =
					Some(format!("Not a file or Git revision: {detail}"));
				return true;
			}
		};

	// The fetch raced user actions: only pair against the document the diff
	// was requested for.
	let session = &mut context.session;
	let left_intact = session
		.layout
		.find_pane(action.left_pane_id)
		.is_some_and(|pane| pane.active_tab().buffer_id == action.left_buffer_id);
	if !left_intact {
		session.ui.status_message = Some("Diff source changed; Git diff cancelled".to_string());
		return true;
	}

	let registry = context.registry().clone();
	let session = &mut context.session;
	let Ok(view) = session.create_unnamed_text_view(
		&registry,
		crate::core::DEFAULT_VIEWPORT_WIDTH,
		crate::core::DEFAULT_VIEWPORT_LINES,
	) else {
		session.ui.status_message = Some("Unable to open Git diff pane".to_string());
		return true;
	};
	let right_buffer_id = view.buffer_id;
	let new_pane = EditorPane::from_view(view);
	let Some(right_pane_id) = session.split_pane_with_new_pane_order(
		action.left_pane_id,
		SplitDirection::Vertical,
		new_pane,
		false,
	) else {
		session.buffers.remove(right_buffer_id);
		session.ui.status_message = Some("Unable to split for Git diff".to_string());
		return true;
	};

	let materialized = materialize_snapshot(
		context,
		SnapshotMaterialization {
			left_pane_id: action.left_pane_id,
			left_buffer_id: action.left_buffer_id,
			snapshot_pane_id: right_pane_id,
			snapshot_buffer_id: right_buffer_id,
			repo_root: action.repo_root,
			rel_path: action.rel_path,
			revision_label: action.revision,
			commit_oid: action.commit_oid,
			blob_oid,
			blob_content,
		},
	);
	if !materialized {
		// Materialization failed (the failure status message is already
		// set): roll the empty snapshot pane back out rather than pairing a
		// diff against a blank document.
		let registry = context.registry().clone();
		let resolver = context.resolver().clone();
		if let Some(lang_registry) = context.language_registry.clone() {
			let runtime = RuntimeContext {
				registry: &registry,
				resolver: &resolver,
				lang_registry: &lang_registry,
				init_script: None,
				init_entrypoint: None,
			};
			close_focused_pane(&mut context.session, &runtime);
		}
		context.session.focus_pane(action.left_pane_id);
	}
	true
}

/// Everything a fetched blob needs to become (or refresh) the snapshot side
/// of a diff pair.
struct SnapshotMaterialization {
	left_pane_id: PaneId,
	left_buffer_id: BufferId,
	snapshot_pane_id: PaneId,
	snapshot_buffer_id: BufferId,
	repo_root: PathBuf,
	rel_path: String,
	revision_label: String,
	commit_oid: String,
	blob_oid: String,
	blob_content: Vec<u8>,
}

/// Install blob content into the snapshot pane's document and wire the pair:
/// `ReplaceDocument` mechanics with the left buffer's language and an
/// OID-keyed virtual identity, provenance, symmetric pairing, and the
/// `name@revision` status title. Returns `false` when the document could not
/// be constructed — the failure status message is already set and the
/// snapshot pane's previous document (if any) is untouched.
fn materialize_snapshot(context: &mut EditorHost, m: SnapshotMaterialization) -> bool {
	let Some(lang_registry) = context.language_registry.clone() else {
		context.session.ui.status_message = Some("Language registry is unavailable".to_string());
		return false;
	};
	let registry = context.registry().clone();
	let resolver = context.resolver().clone();
	let runtime = RuntimeContext {
		registry: &registry,
		resolver: &resolver,
		lang_registry: &lang_registry,
		init_script: None,
		init_entrypoint: None,
	};
	let session = &mut context.session;
	let language = session
		.buffers
		.get(m.left_buffer_id)
		.and_then(|buffer| buffer.text())
		.and_then(|document| document.active_language_id)
		.map(|id| id.as_str().to_string());

	// The snapshot's identity for language services: absolute so it does not
	// depend on the process working directory, keyed by the immutable commit
	// OID so re-acquiring a moved ref never aliases an older snapshot, and
	// deliberately NOT the real file's path — a shared path would map both
	// documents onto one workspace `FileId` and let the snapshot's old
	// content poison analysis of the live buffer.
	let virtual_path =
		super::git::snapshot_virtual_path(&m.repo_root, &m.commit_oid, &m.rel_path);
	let replace_cmd = EditorCommand::ReplaceDocument {
		text: String::from_utf8_lossy(&m.blob_content).into_owned(),
		language,
		virtual_path: Some(virtual_path.clone()),
	};
	let view_id = focused_doctab(&session.layout, m.snapshot_pane_id).id;
	let materialized = {
		let EditorSession {
			buffers,
			layout,
			events,
			ui,
			next_dynamic_device_id,
			persistent_settings,
			..
		} = &mut *session;
		with_text_buffer_mut(&registry, buffers, layout, events, view_id, |tab| {
			handle_file_doctab_command(
				&runtime,
				tab,
				ui,
				next_dynamic_device_id,
				persistent_settings.save_durability,
				replace_cmd,
			);
			// ReplaceDocument installs the requested virtual identity only
			// when document construction succeeded; the previous identity
			// (None for a fresh buffer, the prior OID path for a step)
			// survives every failure path.
			(tab.document.virtual_path.as_deref() == Some(virtual_path.as_str()))
				.then_some(tab.document.active_language_id)
		})
		.flatten()
	};
	let Some(language_id) = materialized else {
		return false;
	};
	{
		let EditorSession {
			layout,
			persistent_settings,
			ui,
			..
		} = &mut *session;
		if let Some(view) = layout.find_view_mut(view_id)
			&& let Err(err) = apply_resolved_theme_to_tab(view, language_id, persistent_settings)
		{
			ui.status_message = Some(err);
		}
	}

	let provenance = crate::engine::session::SnapshotProvenance::GitRevision {
		repo_root: m.repo_root,
		rel_path: m.rel_path,
		revision: m.revision_label,
		commit_oid: m.commit_oid,
		blob_oid: m.blob_oid,
	};
	let title = provenance.title();
	session
		.snapshot_partners
		.insert(m.snapshot_buffer_id, provenance);
	super::diff::compute_pane_diff(&registry, session, m.left_pane_id, m.snapshot_pane_id);
	// Announce the snapshot only when nothing more important (e.g. a theme
	// error) is already on the status line.
	session.ui.status_message.get_or_insert(title);
	true
}

/// The stepping context must be exactly as it was when the step was issued —
/// every phase completion races user actions. Beyond pane/buffer identity
/// this demands: the reciprocal pair still links the two panes (no
/// re-pairing), the snapshot's provenance still names the pinned commit (no
/// `:w PATH` conversion or concurrent re-acquisition), and the snapshot has
/// no unsaved edits that an in-place replacement would silently discard.
fn step_context_intact(session: &EditorSession, step: &super::git::StepContext) -> bool {
	let pane_shows = |pane_id, buffer_id| {
		session
			.layout
			.find_pane(pane_id)
			.is_some_and(|pane| pane.active_tab().buffer_id == buffer_id)
	};
	let paired = |pane_id, partner_id| {
		session
			.diff_pairs
			.get(&pane_id)
			.is_some_and(|pair| pair.partner_pane_id == partner_id)
	};
	let provenance_pinned = matches!(
		session.snapshot_partners.get(&step.snapshot_buffer_id),
		Some(crate::engine::session::SnapshotProvenance::GitRevision { commit_oid, .. })
			if *commit_oid == step.pinned_commit_oid
	);
	let snapshot_clean = session
		.buffers
		.get(step.snapshot_buffer_id)
		.is_some_and(|buffer| !buffer.is_dirty());
	// A concurrent `:w PATH` on the worktree document keeps its buffer and
	// pair but changes what the snapshot would be paired against; the file
	// identity captured at dispatch must still hold.
	let left_identity_pinned = session
		.buffers
		.get(step.left_buffer_id)
		.and_then(|buffer| buffer.text())
		.is_some_and(|document| document.file_path == step.left_file_path);
	pane_shows(step.left_pane_id, step.left_buffer_id)
		&& pane_shows(step.snapshot_pane_id, step.snapshot_buffer_id)
		&& paired(step.left_pane_id, step.snapshot_pane_id)
		&& paired(step.snapshot_pane_id, step.left_pane_id)
		&& provenance_pinned
		&& snapshot_clean
		&& left_identity_pinned
}

/// Collect stdout and the truncation flag from a step-phase job, or surface
/// its failure and return `None`. Stepping failures share one status prefix
/// so the user always knows which operation went wrong.
fn step_job_raw_stdout(
	session: &mut EditorSession,
	completion: &JobCompletion,
) -> Option<(Vec<u8>, bool)> {
	match &completion.outcome {
		super::jobs::JobOutcome::Exited { status: 0 } => {}
		super::jobs::JobOutcome::Exited { .. } => {
			let mut stderr = Vec::new();
			for chunk in completion.stderr.chunks() {
				stderr.extend_from_slice(chunk);
			}
			let stderr = String::from_utf8_lossy(&stderr);
			let detail = stderr.lines().next().unwrap_or("git failed").trim();
			session.ui.status_message = Some(format!("Git step failed: {detail}"));
			return None;
		}
		_ => {
			session.ui.status_message = Some("Git step failed".to_string());
			return None;
		}
	}
	let mut stdout = Vec::new();
	for chunk in completion.stdout.chunks() {
		stdout.extend_from_slice(chunk);
	}
	Some((stdout, completion.stdout.truncated))
}

/// As [`step_job_raw_stdout`], refusing truncated output outright — for
/// phases whose answer could silently change if the stream was cut short.
fn step_job_stdout(session: &mut EditorSession, completion: &JobCompletion) -> Option<Vec<u8>> {
	let (stdout, truncated) = step_job_raw_stdout(session, completion)?;
	if truncated {
		session.ui.status_message = Some("File history is too long to step".to_string());
		return None;
	}
	Some(stdout)
}

/// Launch the shared fetch for a stepped target: identical `cat-file --batch`
/// mechanics to acquisition, re-materializing in place on completion.
fn spawn_step_fetch(
	session: &mut EditorSession,
	step: super::git::StepContext,
	target: &super::git::TimelineEntry,
) {
	if target.path.contains('\n') {
		session.ui.status_message =
			Some("Cannot step into a path containing a newline".to_string());
		return;
	}
	let spec = JobSpec {
		program: PathBuf::from("git"),
		args: vec![
			"--no-optional-locks".into(),
			"cat-file".into(),
			"--batch".into(),
		],
		cwd: Some(step.repo_root.clone()),
		stdin: Some(format!("{}:{}\n", target.commit_oid, target.path).into_bytes()),
		timeout: Duration::from_secs(10),
		completion: Some(super::git::git_step_fetch_action_lease(
			super::git::GitStepFetchAction {
				step,
				commit_oid: target.commit_oid.clone(),
				rel_path: target.path.clone(),
			},
		)),
	};
	match session.jobs.spawn(super::git::GIT_JOB_OWNER, true, spec) {
		Ok(job) => {
			session.snapshot_fetch_job = Some(job);
		}
		Err(error) => {
			session.ui.status_message = Some(format!("Git step: {error}"));
		}
	}
}

/// Complete the older walk: the timeline rooted at the pinned commit, whose
/// first record is the current content's origin. The target is record `steps`.
fn complete_git_older_walk(
	context: &mut EditorHost,
	action: super::git::GitOlderWalkAction,
	completion: &JobCompletion,
) -> bool {
	let session = &mut context.session;
	let Some((stdout, truncated)) = step_job_raw_stdout(session, completion) else {
		return true;
	};
	let timeline = super::git::parse_file_timeline(&stdout);
	let steps = usize::try_from(action.steps).unwrap_or(usize::MAX);
	// Entries are newest-first, so truncation only cuts the older tail: a
	// target already in hand is valid regardless. Only a miss must
	// distinguish "history really ends here" from "the stream was cut".
	let Some(target) = timeline.get(steps) else {
		session.ui.status_message = Some(if truncated {
			"File history is too long to step".to_string()
		} else {
			"Already at the oldest revision for this file".to_string()
		});
		return true;
	};
	if !step_context_intact(session, &action.step) {
		session.ui.status_message = Some("Diff context changed; step cancelled".to_string());
		return true;
	}
	spawn_step_fetch(session, action.step, target);
	true
}

/// Complete the newer normalize job: map the pinned commit to its
/// file-touching position, then walk the HEAD-rooted timeline to find it.
fn complete_git_newer_normalize(
	context: &mut EditorHost,
	action: super::git::GitNewerNormalizeAction,
	completion: &JobCompletion,
) -> bool {
	let session = &mut context.session;
	let Some(stdout) = step_job_stdout(session, completion) else {
		return true;
	};
	let Some(current_position) = super::git::parse_single_oid(&stdout) else {
		session.ui.status_message =
			Some("Git step failed: history position unreadable".to_string());
		return true;
	};
	if !step_context_intact(session, &action.step) {
		session.ui.status_message = Some("Diff context changed; step cancelled".to_string());
		return true;
	}
	// `--follow` walks renames backward only from its starting point, so the
	// HEAD-rooted timeline must use the worktree file's current path.
	let spec = JobSpec {
		program: PathBuf::from("git"),
		args: vec![
			"--no-optional-locks".into(),
			"log".into(),
			"-z".into(),
			"--first-parent".into(),
			"--format=%H".into(),
			"--follow".into(),
			"--name-status".into(),
			"HEAD".into(),
			"--".into(),
			action.head_rel_path.clone().into(),
		],
		cwd: Some(action.step.repo_root.clone()),
		stdin: None,
		timeout: Duration::from_secs(10),
		completion: Some(super::git::git_newer_walk_action_lease(
			super::git::GitNewerWalkAction {
				step: action.step,
				steps: action.steps,
				current_position,
			},
		)),
	};
	match session.jobs.spawn(super::git::GIT_JOB_OWNER, true, spec) {
		Ok(job) => {
			session.snapshot_fetch_job = Some(job);
		}
		Err(error) => {
			session.ui.status_message = Some(format!("Git step: {error}"));
		}
	}
	true
}

/// Complete the newer walk: locate the normalized position in the
/// HEAD-rooted timeline and step toward its start.
fn complete_git_newer_walk(
	context: &mut EditorHost,
	action: super::git::GitNewerWalkAction,
	completion: &JobCompletion,
) -> bool {
	let session = &mut context.session;
	let Some((stdout, truncated)) = step_job_raw_stdout(session, completion) else {
		return true;
	};
	let timeline = super::git::parse_file_timeline(&stdout);
	// This timeline is also newest-first, so a truncated capture is a valid
	// PREFIX: if the current position is inside it, every newer target is
	// too — truncation only removed older entries. Only a missing position
	// must distinguish "not in this history" from "the stream was cut
	// before reaching it".
	let Some(index) = timeline
		.iter()
		.position(|entry| entry.commit_oid == action.current_position)
	else {
		session.ui.status_message = Some(if truncated {
			"File history is too long to step".to_string()
		} else {
			"Snapshot is not in the current file's history".to_string()
		});
		return true;
	};
	let steps = usize::try_from(action.steps).unwrap_or(usize::MAX);
	let Some(target_index) = index.checked_sub(steps) else {
		session.ui.status_message =
			Some("Already at the newest revision for this file".to_string());
		return true;
	};
	let target = timeline[target_index].clone();
	if !step_context_intact(session, &action.step) {
		session.ui.status_message = Some("Diff context changed; step cancelled".to_string());
		return true;
	}
	spawn_step_fetch(session, action.step, &target);
	true
}

/// Complete a step's fetch: re-materialize the snapshot pane in place with
/// the target commit's blob, short-OID label, and refreshed pairing. A
/// failure leaves the previous snapshot intact — there is no pane to roll
/// back.
fn complete_git_step_fetch(
	context: &mut EditorHost,
	action: super::git::GitStepFetchAction,
	completion: &JobCompletion,
) -> bool {
	{
		let session = &mut context.session;
		match &completion.outcome {
			super::jobs::JobOutcome::Exited { status: 0 } => {}
			super::jobs::JobOutcome::Exited { .. } => {
				let mut stderr = Vec::new();
				for chunk in completion.stderr.chunks() {
					stderr.extend_from_slice(chunk);
				}
				let stderr = String::from_utf8_lossy(&stderr);
				let detail = stderr.lines().next().unwrap_or("git cat-file failed").trim();
				session.ui.status_message = Some(format!("Git step failed: {detail}"));
				return true;
			}
			_ => {
				session.ui.status_message = Some("Git step failed".to_string());
				return true;
			}
		}
		if completion.stdout.truncated {
			session.ui.status_message = Some("Git snapshot is too large to diff".to_string());
			return true;
		}
	}
	let mut stdout = Vec::new();
	for chunk in completion.stdout.chunks() {
		stdout.extend_from_slice(chunk);
	}
	let (blob_oid, blob_content) =
		match super::git::parse_batch_blob(&stdout, action.commit_oid.len()) {
			Ok(parsed) => parsed,
			Err(detail) => {
				context.session.ui.status_message = Some(format!("Git step failed: {detail}"));
				return true;
			}
		};
	if !step_context_intact(&context.session, &action.step) {
		context.session.ui.status_message =
			Some("Diff context changed; step cancelled".to_string());
		return true;
	}
	let revision_label = super::git::short_oid(&action.commit_oid).to_string();
	materialize_snapshot(
		context,
		SnapshotMaterialization {
			left_pane_id: action.step.left_pane_id,
			left_buffer_id: action.step.left_buffer_id,
			snapshot_pane_id: action.step.snapshot_pane_id,
			snapshot_buffer_id: action.step.snapshot_buffer_id,
			repo_root: action.step.repo_root,
			rel_path: action.rel_path,
			revision_label,
			commit_oid: action.commit_oid,
			blob_oid,
			blob_content,
		},
	);
	true
}

fn drain_run_loop_deliveries(
	session: &mut EditorHost,
	mut rune_host: Option<&mut RuneHost>,
	subscriber_dispatcher: &mut (dyn SubscriberDispatcher<EditorSession> + Send),
	job_dispatcher: &mut (dyn JobDispatcher<EditorSession> + Send),
	internal: &super::request::InternalSender,
	gate: &InvocationGate,
	event_tap: Option<&mpsc::Sender<EngineEvent>>,
) -> bool {
	let jobs_were_pending = session.session.jobs.has_pending_delivery();
	let mut subscriber_outcome = super::event_subscribers::SubscriberDrainOutcome::default();
	if !jobs_were_pending {
		let mut subscriber_runtime = std::mem::take(&mut session.session.subscribers);
		let mut router = RunLoopSubscriberDispatcher {
			fallback: subscriber_dispatcher,
			rune_host: rune_host.as_deref_mut(),
		};
		subscriber_outcome = drain_engine_subscribers_combined(
			&mut subscriber_runtime,
			session,
			internal,
			gate,
			&mut router,
			|event| {
				if let Some(tap) = event_tap {
					let _ = tap.send(event.clone());
				}
			},
		);
		session.session.subscribers = subscriber_runtime;
	}

	let mut job_outcome = SubscriberOutcome::delivered();
	if !session.session.subscribers.has_pending() {
		session.session.jobs.capture_delivery_batch();
		let mut steps = subscriber_outcome.steps_used;
		let mut router = RunLoopJobDispatcher {
			fallback: job_dispatcher,
			rune_host,
		};
		while steps < MAX_JOB_DELIVERY_STEPS {
			let Some((invocation, completion)) = session.session.jobs.take_next_delivery() else {
				break;
			};
			steps += 1;
			let owner = invocation.owner();
			let owner_is_live = router.owner_is_live(owner);
			let outcome = match gate.invoke_guarded(owner, owner_is_live, || {
				router.complete(session, invocation, &completion)
			}) {
				Ok(outcome) => outcome,
				Err(error) => {
					SubscriberOutcome::failed(BoundedStatus::new(error.to_string()).unwrap_or_else(
						|_| BoundedStatus::fixed("Runtime job completion invocation failed"),
					))
				}
			};
			job_outcome.needs_render |= outcome.needs_render;
			if outcome.foreground_status.is_some() {
				job_outcome.foreground_status = outcome.foreground_status;
			}
		}
		if session.session.jobs.has_pending_delivery() {
			let _ = internal.try_send(InternalEvent::DrainEvents);
		} else if should_rearm_event_drain(
			subscriber_outcome.allocation_failed,
			!session.session.events.is_empty(),
		) {
			// Events raised by a completion callback belong to the next event pass.
			let _ = internal.try_send(InternalEvent::DrainEvents);
		}
		debug_assert!(steps <= MAX_JOB_DELIVERY_STEPS);
	}

	let services = session.services.clone();
	merge_delivery_statuses(
		&services,
		&mut session.ui.status_message,
		subscriber_outcome.foreground_status,
		job_outcome.foreground_status,
	);
	subscriber_outcome.needs_render || job_outcome.needs_render
}

fn merge_delivery_statuses(
	services: &Arc<Mutex<HostServices>>,
	ui_status: &mut Option<String>,
	subscriber_status: Option<BoundedStatus>,
	job_status: Option<BoundedStatus>,
) {
	if let Some(status) = subscriber_status {
		if let Some(job_status) = job_status {
			let job_status = job_status.into_string();
			lock_services(services).messages.push_line(&job_status);
		}
		*ui_status = Some(status.into_string());
	} else if let Some(status) = job_status {
		*ui_status = Some(status.into_string());
	}
}

fn should_rearm_event_drain(allocation_failed: bool, events_pending: bool) -> bool {
	events_pending && !allocation_failed
}

/// Panel identity of the focused pane, when it is host-rendered.
fn focused_host_panel_id(session: &EditorSession) -> Option<crate::engine::HostPanelId> {
	let view = focused_doctab(&session.layout, session.focused_pane_id);
	session
		.buffers
		.get(view.buffer_id)
		.and_then(|buffer| buffer.host_panel())
		.map(|panel| panel.panel_id)
}

fn current_input_context(registry: &UastRegistry, session: &EditorSession) -> InputContext {
	let view = focused_doctab(&session.layout, session.focused_pane_id);
	let common = view.common();
	let (cursor_abs_byte, cursor_line, cursor_count) = match &view.content {
		ViewContent::Text(text) => (
			text.cursor.primary().byte(),
			text.cursor.primary().abs_line(),
			text.cursor.len(),
		),
		ViewContent::FileBrowser(browser) => (
			browser.cursor.primary().byte(),
			browser.cursor.primary().abs_line(),
			browser.cursor.len(),
		),
		// Neither has a caret: nothing text-shaped is focused.
		ViewContent::MarkdownPreview(_) | ViewContent::HostPanel(_) => {
			(DocByte::ZERO, DocLine::ZERO, 0)
		}
	};
	let cursor_line_start_byte = session
		.buffers
		.get(view.buffer_id)
		.and_then(|buffer| buffer.root_id())
		.and_then(|root| super::support::line_start_byte_sparse(registry, root, cursor_line).ok())
		.unwrap_or(cursor_abs_byte);
	InputContext {
		is_file_browser: view.is_file_browser(),
		is_uast_inspector: session.uast_inspectors.contains_key(&view.buffer_id),
		is_host_panel: view.is_host_panel(),
		cursor_available: !matches!(view.content, ViewContent::MarkdownPreview(_)),
		viewport_line_count: common.viewport_lines,
		viewport_start_line: DocLine::new(common.scroll_y),
		cursor_abs_byte,
		cursor_line_start_byte,
		cursor_line,
		cursor_count,
		active_search: session.search_pattern.is_some(),
	}
}

fn apply_input_mode_override(session: &mut EditorSession) -> bool {
	session
		.ui
		.mode_override
		.take()
		.is_some_and(|mode| session.input.apply_mode_override(mode))
}

fn handle_hydration_completed(session: &mut EditorSession, buffer: crate::core::BufferId) -> bool {
	if session.buffers.get(buffer).is_none() {
		return false;
	}
	session
		.events
		.push(EngineEvent::HydrationCompleted { buffer });
	true
}

#[derive(Default, Clone, Copy)]
struct CommandEffect {
	needs_render: bool,
}

impl CommandEffect {
	const fn render() -> Self {
		Self { needs_render: true }
	}
}

#[derive(Clone, Copy)]
struct DispatchOutcome {
	needs_render: bool,
	should_stop: bool,
}

impl DispatchOutcome {
	const fn continue_with(needs_render: bool) -> Self {
		Self {
			needs_render,
			should_stop: false,
		}
	}

	const fn stop() -> Self {
		Self {
			needs_render: false,
			should_stop: true,
		}
	}
}

struct DispatchContext<'a, 'runtime> {
	runtime: &'a RuntimeContext<'runtime>,
	tx_internal: &'a super::request::InternalSender,
	tx_view: &'a std::sync::mpsc::Sender<crate::uast::Viewport>,
	reactor: &'a crate::svp::semantic::SemanticReactor,
	resolver: &'a std::sync::Arc<SvpResolver>,
	lang_registry: &'a std::sync::Arc<super::language::LanguageRegistry>,
}

struct RuntimeContext<'a> {
	registry: &'a std::sync::Arc<UastRegistry>,
	resolver: &'a SvpResolver,
	lang_registry: &'a super::language::LanguageRegistry,
	init_script: Option<&'a Path>,
	init_entrypoint: Option<&'a str>,
}

struct SemanticRuntime<'a> {
	highlights: &'a mut Vec<HighlightSpan>,
	diagnostics: &'a mut Vec<DiagnosticSpan>,
	last_state: &'a mut Option<StateId>,
	applied_structural_state_id: &'a mut Option<StateId>,
	structural_issue: &'a mut Option<super::session::StructuralIssue>,
	last_len: &'a mut usize,
	last_path: &'a mut Option<String>,
	pending_request_id: &'a mut Option<RequestId>,
	cached_sum_tree: &'a mut Option<CachedSumTree>,
	view_revision: &'a mut u64,
	minimap_invalidated: bool,
}

impl SemanticRuntime<'_> {
	fn from_semantic(semantic: &mut SemanticState) -> SemanticRuntime<'_> {
		SemanticRuntime {
			highlights: &mut semantic.highlights,
			diagnostics: &mut semantic.diagnostics,
			last_state: &mut semantic.last_semantic_state,
			applied_structural_state_id: &mut semantic.applied_structural_state_id,
			structural_issue: &mut semantic.structural_issue,
			last_len: &mut semantic.last_semantic_len,
			last_path: &mut semantic.last_semantic_path,
			pending_request_id: &mut semantic.pending_semantic_request_id,
			cached_sum_tree: &mut semantic.cached_sum_tree,
			view_revision: &mut semantic.view_revision,
			minimap_invalidated: false,
		}
	}

	fn minimap_invalidated(&self) -> bool {
		self.minimap_invalidated
	}

	fn reset_semantic_state(&mut self) {
		self.highlights.clear();
		self.diagnostics.clear();
		*self.last_state = None;
		*self.applied_structural_state_id = None;
		*self.structural_issue = None;
		*self.last_len = usize::MAX;
		*self.last_path = None;
		*self.pending_request_id = None;
		*self.cached_sum_tree = None;
		*self.view_revision = self.view_revision.wrapping_add(1);
		self.minimap_invalidated = true;
	}
}

fn reset_global_transients(session: &mut EditorSession) {
	session.search_confirm = None;
	session.pending_reload = None;
	session.ui.mode_override = None;
	session.ui.yank_flash = None;
}

fn focused_pane(layout: &LayoutNode, focused_pane_id: PaneId) -> &EditorPane {
	layout
		.find_pane(focused_pane_id)
		.expect("focused pane must always exist in the layout tree")
}

fn focused_pane_mut(layout: &mut LayoutNode, focused_pane_id: PaneId) -> &mut EditorPane {
	layout
		.find_pane_mut(focused_pane_id)
		.expect("focused pane must always exist in the layout tree")
}

fn focused_doctab(layout: &LayoutNode, focused_pane_id: PaneId) -> &View {
	focused_pane(layout, focused_pane_id).active_tab()
}

fn focused_git_path(session: &EditorSession) -> Option<PathBuf> {
	let view = focused_doctab(&session.layout, session.focused_pane_id);
	session
		.buffers
		.get(view.buffer_id)
		.and_then(|buffer| match &buffer.content {
			BufferContent::Text(document) => document
				.backing_path
				.as_deref()
				.or(document.file_path.as_deref())
				.map(PathBuf::from),
			BufferContent::FileBrowser(document) => Some(document.state.root_path.clone()),
			BufferContent::MarkdownPreview(document) => {
				document.state.source_path.clone().map(PathBuf::from)
			}
			BufferContent::HostPanel(_) => None,
		})
		.or_else(|| std::env::current_dir().ok())
}

fn focused_git_repo_root(session: &mut EditorSession) -> Option<PathBuf> {
	match focused_git_path(session) {
		Some(path) => session.git_branch_cache.repo_root_for_path(&path),
		None => session.git_branch_cache.repo_root_for_current_dir(),
	}
}

fn with_focused_text_buffer_mut<R>(
	registry: &UastRegistry,
	buffers: &mut BufferStore,
	layout: &mut LayoutNode,
	events: &mut EngineEventQueue,
	focused_pane_id: PaneId,
	mutate: impl FnOnce(&mut TextEditorMut<'_>) -> R,
) -> Option<R> {
	let view_id = focused_doctab(layout, focused_pane_id).id;
	with_text_buffer_mut(registry, buffers, layout, events, view_id, mutate)
}

fn new_doctab_viewport(session: &EditorSession) -> (u16, u32) {
	let active_doctab = focused_pane(&session.layout, session.focused_pane_id).active_tab();
	(
		active_doctab.common().viewport_width,
		active_doctab.common().viewport_lines,
	)
}

fn spawn_empty_doctab(registry: &UastRegistry, session: &mut EditorSession) -> Result<(), String> {
	let (viewport_width, viewport_lines) = new_doctab_viewport(session);
	let tab = session.create_unnamed_text_view(registry, viewport_width, viewport_lines)?;
	let pane = focused_pane_mut(&mut session.layout, session.focused_pane_id);
	pane.tabs.push(tab);
	pane.active_tab_index = pane.tabs.len().saturating_sub(1);
	reset_global_transients(session);
	Ok(())
}

/// Resolve a path to one live text buffer without deciding where it appears.
/// Loading uses a detached temporary view because the existing file command
/// surface still initializes cursor/view state; that view is discarded before
/// this function returns.
fn resolve_or_load_buffer(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	path: String,
) -> Result<(crate::core::BufferId, CommandEffect), String> {
	let canonical = CanonicalPath::resolve(&path)?;
	if let Some(buffer_id) = session.buffers.buffer_for_path(&canonical) {
		return Ok((buffer_id, CommandEffect::render()));
	}

	let mut temporary_view = session.create_unnamed_text_view(
		registry_ref(runtime),
		crate::core::DEFAULT_VIEWPORT_WIDTH,
		crate::core::DEFAULT_VIEWPORT_LINES,
	)?;
	let buffer_id = temporary_view.buffer_id;
	let reservation = match session.buffers.reserve_path(buffer_id, canonical) {
		Ok(reservation) => reservation,
		Err(err) => {
			release_buffer(runtime, session, buffer_id);
			return Err(err);
		}
	};
	let (effect, loaded) = {
		let EditorSession {
			buffers,
			ui,
			next_dynamic_device_id,
			persistent_settings,
			..
		} = session;
		buffers
			.with_detached_text_mut(buffer_id, &mut temporary_view, |editor| {
				let effect = handle_file_doctab_command(
					runtime,
					editor,
					ui,
					next_dynamic_device_id,
					persistent_settings.save_durability,
					EditorCommand::LoadFile(path),
				);
				(effect, editor.document.file_path.is_some())
			})
			.expect("new text buffer must match its detached view")
	};
	if !loaded {
		let error = session
			.ui
			.status_message
			.clone()
			.unwrap_or_else(|| "Unable to load file".to_string());
		release_buffer(runtime, session, buffer_id);
		return Err(error);
	}
	session.buffers.commit_path(reservation);
	Ok((buffer_id, effect))
}

fn spawn_file_doctab(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	path: String,
) -> CommandEffect {
	let (buffer_id, effect) = match resolve_or_load_buffer(runtime, session, path) {
		Ok(resolved) => resolved,
		Err(err) => {
			session.ui.status_message = Some(err);
			return CommandEffect::render();
		}
	};
	let (viewport_width, viewport_lines) = new_doctab_viewport(session);
	let mut tab = match session.create_text_view_for_buffer(
		registry_ref(runtime),
		buffer_id,
		viewport_width,
		viewport_lines,
	) {
		Ok(tab) => tab,
		Err(err) => {
			session.ui.status_message = Some(err);
			if session.layout.view_count_for_buffer(buffer_id) == 0 {
				release_buffer(runtime, session, buffer_id);
			}
			return CommandEffect::render();
		}
	};
	let language_id = session
		.buffers
		.get(buffer_id)
		.and_then(|buffer| buffer.text())
		.and_then(|document| document.active_language_id);
	if let Err(err) =
		apply_resolved_theme_to_tab(&mut tab, language_id, &session.persistent_settings)
	{
		session.ui.status_message = Some(err);
	}
	let pane = focused_pane_mut(&mut session.layout, session.focused_pane_id);
	pane.tabs.push(tab);
	pane.active_tab_index = pane.tabs.len().saturating_sub(1);
	reset_global_transients(session);
	effect
}

fn registry_ref<'a>(runtime: &'a RuntimeContext<'_>) -> &'a UastRegistry {
	runtime.registry
}

fn cycle_active_doctab(session: &mut EditorSession, forward: bool) {
	let pane = focused_pane_mut(&mut session.layout, session.focused_pane_id);
	let tab_count = pane.tabs.len();
	if tab_count <= 1 {
		reset_global_transients(session);
		return;
	}

	let step = if forward { 1 } else { tab_count - 1 };
	for offset in 1..=tab_count {
		let index = (pane.active_tab_index + step * offset) % tab_count;
		if session
			.buffers
			.get(pane.tabs[index].buffer_id)
			.is_none_or(|buffer| !buffer.is_output_panel())
		{
			pane.active_tab_index = index;
			break;
		}
	}
	reset_global_transients(session);
}

fn close_active_doctab(session: &mut EditorSession, runtime: &RuntimeContext<'_>) {
	let removed_buffer = {
		let pane = focused_pane_mut(&mut session.layout, session.focused_pane_id);
		if pane.tabs.len() <= 1 {
			None
		} else {
			let removed = pane.tabs.remove(pane.active_tab_index);
			if pane.active_tab_index >= pane.tabs.len() {
				pane.active_tab_index = pane.tabs.len().saturating_sub(1);
			}
			Some(removed.buffer_id)
		}
	};
	let Some(buffer_id) = removed_buffer else {
		session.ui.pending_quit = true;
		return;
	};
	let is_output_panel = session
		.buffers
		.get(buffer_id)
		.is_some_and(|buffer| buffer.is_output_panel());
	if !is_output_panel && session.layout.view_count_for_buffer(buffer_id) == 0 {
		release_buffer(runtime, session, buffer_id);
	}
	reset_global_transients(session);
}

fn close_focused_pane(session: &mut EditorSession, runtime: &RuntimeContext<'_>) {
	if let Some(removed_pane) = session.close_focused_pane() {
		let removed_buffers = removed_pane
			.tabs
			.into_iter()
			.map(|tab| tab.buffer_id)
			.collect::<std::collections::HashSet<_>>();
		for buffer_id in removed_buffers {
			if session
				.buffers
				.get(buffer_id)
				.is_some_and(|buffer| buffer.is_output_panel())
			{
				continue;
			}
			if session.layout.view_count_for_buffer(buffer_id) == 0 {
				release_buffer(runtime, session, buffer_id);
			}
		}
	} else {
		session.ui.status_message = Some("Cannot close last pane".to_string());
	}
}

fn release_buffer(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	buffer_id: crate::core::BufferId,
) {
	if session.jump_history.references(buffer_id) {
		return;
	}
	if session
		.buffers
		.get(buffer_id)
		.is_some_and(|buffer| buffer.is_output_panel())
	{
		return;
	}
	let Some(mut buffer) = session.buffers.remove(buffer_id) else {
		return;
	};
	session.snapshot_partners.remove(&buffer_id);
	// Callers reach here only once no view references the buffer, so this is
	// the panel's last workspace reference disappearing.
	if let Some(panel) = buffer.host_panel() {
		let panel_id = panel.panel_id;
		session.queue_host_event(crate::engine::HostEvent::PanelDisposed { panel_id });
	}
	if let BufferContent::Text(document) = &mut buffer.content {
		if let Some(shadow) = document.shadow.take() {
			let _ = discard_shadow_session(&shadow);
		}
		if let Some(device_id) = document.backing_device_id {
			runtime.resolver.unregister_device(device_id);
		}
	}
	if let Some(root) = buffer.root_id() {
		runtime.registry.cancel_metric_scan(root);
	}
}

fn open_file_browser_at_path(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	root_path: PathBuf,
) -> CommandEffect {
	if let Some(existing_pane_id) = session.browser_pane_id() {
		session.focus_pane(existing_pane_id);
		let view_id = session
			.layout
			.find_pane(existing_pane_id)
			.expect("existing browser pane")
			.active_tab()
			.id;
		let buffer_id = session
			.layout
			.find_view(view_id)
			.expect("browser view")
			.buffer_id;
		let reinit = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.file_browser())
			.map(|buffer| buffer.state.root_path != root_path)
			.unwrap_or(true);
		if reinit {
			let EditorSession {
				buffers,
				layout,
				ui,
				..
			} = session;
			let result = with_file_browser_buffer_mut(buffers, layout, view_id, |tab| {
				initialize_file_browser(runtime.registry, tab, root_path)
			})
			.expect("browser pair");
			if let Err(err) = result {
				ui.status_message = Some(err);
			}
		}
		reset_global_transients(session);
		return CommandEffect::render();
	}

	let (browser_buffer, leaf) =
		match create_file_browser_buffer(runtime.registry, root_path.clone()) {
			Ok(created) => created,
			Err(err) => {
				session.ui.status_message = Some(err);
				return CommandEffect::render();
			}
		};
	let buffer_id = session
		.buffers
		.insert(BufferContent::FileBrowser(browser_buffer));
	let view_id = session.allocate_view_id();
	let mut browser_view = View::new_file_browser(
		view_id,
		buffer_id,
		leaf,
		&session.persistent_settings,
		crate::core::DEFAULT_VIEWPORT_WIDTH,
		crate::core::DEFAULT_VIEWPORT_LINES,
	);
	let initialized = session
		.buffers
		.with_detached_file_browser_mut(buffer_id, &mut browser_view, |editor| {
			initialize_file_browser(runtime.registry, editor, root_path)
		})
		.expect("browser pair");
	if let Err(err) = initialized {
		session.ui.status_message = Some(err);
		session.buffers.remove(buffer_id);
		return CommandEffect::render();
	}
	let browser_pane = EditorPane::from_view(browser_view);

	if session.open_file_browser_sidebar(browser_pane).is_none() {
		session.buffers.remove(buffer_id);
		session.ui.status_message = Some("Unable to open file browser".to_string());
	}
	reset_global_transients(session);
	CommandEffect::render()
}

fn pane_midpoint_row(rect: crate::core::PaneRect) -> u32 {
	rect.screen_row.get().saturating_add(rect.height.get() / 2)
}

fn select_browser_target_pane(session: &EditorSession, browser_pane_id: PaneId) -> Option<PaneId> {
	if let Some(target_pane_id) = session.browser_target_pane_id
		&& target_pane_id != browser_pane_id
		&& session
			.layout
			.find_pane(target_pane_id)
			.is_some_and(|pane| !pane.active_tab().is_file_browser())
	{
		return Some(target_pane_id);
	}

	let layouts = session.pane_layouts();
	let browser_rect = layouts
		.iter()
		.find(|layout| layout.pane_id == browser_pane_id)
		.map(|layout| layout.rect)?;

	let mut best: Option<(u32, u32, u32, PaneId)> = None;
	for layout in layouts {
		if layout.pane_id == browser_pane_id {
			continue;
		}
		let Some(pane) = session.layout.find_pane(layout.pane_id) else {
			continue;
		};
		if pane.active_tab().is_file_browser() {
			continue;
		}

		let rect = layout.rect;
		let candidate = if rect.screen_col.get() >= browser_rect.right().get() {
			(
				0,
				rect.screen_col
					.get()
					.saturating_sub(browser_rect.right().get()),
				pane_midpoint_row(rect).abs_diff(pane_midpoint_row(browser_rect)),
				layout.pane_id,
			)
		} else {
			(1, 0, 0, layout.pane_id)
		};
		if best.map(|current| candidate < current).unwrap_or(true) {
			best = Some(candidate);
		}
	}

	best.map(|(_, _, _, pane_id)| pane_id)
}

fn ensure_browser_target_pane(
	registry: &UastRegistry,
	session: &mut EditorSession,
	browser_pane_id: PaneId,
) -> Option<PaneId> {
	if let Some(existing) = select_browser_target_pane(session, browser_pane_id) {
		return Some(existing);
	}

	let view = session
		.create_unnamed_text_view(
			registry,
			crate::core::DEFAULT_VIEWPORT_WIDTH,
			crate::core::DEFAULT_VIEWPORT_LINES,
		)
		.ok()?;
	let buffer_id = view.buffer_id;
	let new_pane = EditorPane::from_view(view);
	let result = session.split_pane_with_new_pane_order(
		browser_pane_id,
		super::protocol::SplitDirection::Vertical,
		new_pane,
		false,
	);
	if result.is_none() {
		session.buffers.remove(buffer_id);
	}
	result
}

fn split_target_pane(
	registry: &UastRegistry,
	session: &mut EditorSession,
	direction: SplitDirection,
) -> bool {
	let target_pane_id = if session
		.layout
		.find_pane(session.focused_pane_id)
		.is_some_and(|pane| pane.active_tab().is_file_browser())
	{
		let browser_pane_id = session.focused_pane_id;
		ensure_browser_target_pane(registry, session, browser_pane_id).unwrap_or(browser_pane_id)
	} else {
		session.focused_pane_id
	};

	let split = if target_pane_id == session.focused_pane_id {
		session.split_focused_pane(registry, direction)
	} else {
		let Ok(view) = session.create_unnamed_text_view(
			registry,
			crate::core::DEFAULT_VIEWPORT_WIDTH,
			crate::core::DEFAULT_VIEWPORT_LINES,
		) else {
			return false;
		};
		let buffer_id = view.buffer_id;
		let new_pane = EditorPane::from_view(view);
		let split = session
			.split_pane_with_new_pane_order(target_pane_id, direction, new_pane, false)
			.is_some();
		if !split {
			session.buffers.remove(buffer_id);
		}
		split
	};
	if split {
		let view_id = focused_doctab(&session.layout, session.focused_pane_id).id;
		let buffer_id = focused_doctab(&session.layout, session.focused_pane_id).buffer_id;
		let language_id = session
			.buffers
			.get(buffer_id)
			.and_then(|buffer| buffer.text())
			.and_then(|document| document.active_language_id);
		let EditorSession {
			layout,
			persistent_settings,
			ui,
			..
		} = session;
		let view = layout
			.find_view_mut(view_id)
			.expect("newly split view must remain attached");
		if let Err(err) = apply_resolved_theme_to_tab(view, language_id, persistent_settings) {
			ui.status_message = Some(err);
		}
	}
	split
}

struct LoadFileOutcome {
	effect: CommandEffect,
	succeeded: bool,
}

fn load_file_into_pane(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	pane_id: PaneId,
	path: String,
) -> LoadFileOutcome {
	let (buffer_id, effect) = match resolve_or_load_buffer(runtime, session, path) {
		Ok(resolved) => resolved,
		Err(err) => {
			session.ui.status_message = Some(err);
			return LoadFileOutcome {
				effect: CommandEffect::render(),
				succeeded: false,
			};
		}
	};
	let view_id = session
		.layout
		.find_pane(pane_id)
		.expect("target pane must exist")
		.active_tab()
		.id;
	if let Err(err) = retarget_text_view(runtime, session, view_id, buffer_id) {
		session.ui.status_message = Some(err);
		if session.layout.view_count_for_buffer(buffer_id) == 0 {
			release_buffer(runtime, session, buffer_id);
		}
		return LoadFileOutcome {
			effect: CommandEffect::render(),
			succeeded: false,
		};
	}
	session.focused_pane_id = pane_id;
	reset_global_transients(session);
	LoadFileOutcome {
		effect,
		succeeded: true,
	}
}

fn retarget_text_view(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	view_id: crate::core::ViewId,
	buffer_id: crate::core::BufferId,
) -> Result<(), String> {
	let old_buffer_id = session
		.layout
		.find_view(view_id)
		.ok_or_else(|| "Target view is unavailable".to_string())?
		.buffer_id;
	if old_buffer_id == buffer_id {
		return Ok(());
	}
	let document = session
		.buffers
		.get(buffer_id)
		.and_then(|buffer| buffer.text())
		.ok_or_else(|| "Target buffer is not editable text".to_string())?;
	let root = document.root_id;
	let topology_rev = document.topology_rev;
	let state_id = document.ledger.current_state_id;
	let language_id = document.active_language_id;
	let initial_leaf = runtime
		.registry
		.find_node_at_doc_byte_raw(root, crate::core::DocByte::ZERO)
		.node_id;
	let view = session
		.layout
		.find_view_mut(view_id)
		.expect("retargeted view must remain live");
	view.retarget_text_buffer(buffer_id, initial_leaf, topology_rev, state_id);
	if let Err(err) = apply_resolved_theme_to_tab(view, language_id, &session.persistent_settings) {
		session.ui.status_message = Some(err);
	}
	if session.layout.view_count_for_buffer(old_buffer_id) == 0 {
		release_buffer(runtime, session, old_buffer_id);
	}
	Ok(())
}

fn request_goto_definition(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	reactor: &crate::svp::semantic::SemanticReactor,
) -> bool {
	let view = focused_doctab(&session.layout, session.focused_pane_id);
	let view_id = view.id;
	let buffer_id = view.buffer_id;
	let Some(buffer) = session.buffers.get(buffer_id) else {
		return false;
	};
	let Some(document) = buffer.text() else {
		session.ui.status_message = Some("Go to definition requires a text document".to_string());
		return true;
	};
	let Some(language_id) = document.active_language_id else {
		session.ui.status_message =
			Some("Go to definition is unsupported for this document".to_string());
		return true;
	};
	let Some(file_path) = document
		.backing_path
		.as_deref()
		.or(document.file_path.as_deref())
		.or(document.virtual_path.as_deref())
		.map(ToOwned::to_owned)
	else {
		session.ui.status_message = Some("Go to definition requires a document path".to_string());
		return true;
	};
	let total_bytes = runtime.registry.get_total_bytes(document.root_id);
	if total_bytes >= super::MAX_SEMANTIC_BYTES {
		session.ui.status_message =
			Some("Go to definition is unavailable for documents at or above 8 MiB".to_string());
		return true;
	}
	let cursor = match &view.content {
		ViewContent::Text(text) => text.cursor.primary().byte(),
		_ => {
			session.ui.status_message =
				Some("Go to definition requires an editable text view".to_string());
			return true;
		}
	};
	let content = match read_document_snapshot(
		runtime.registry,
		document.root_id,
		document.backing_path.as_deref().map(Path::new),
	) {
		Ok(content) => content,
		Err(error) => {
			session.ui.status_message = Some(format!(
				"Unable to read document for definition lookup: {error}"
			));
			return true;
		}
	};
	let request_id = session.next_semantic_request_id;
	session.next_semantic_request_id = request_id.next();
	let stamp = NavigationStamp {
		request_id,
		buffer_id,
		root: document.root_id,
		state_id: document.ledger.current_state_id,
		view_id,
		cursor,
	};
	session.pending_navigation = Some(stamp);
	session.ui.status_message = Some("Finding definition…".to_string());
	reactor.send_goto_definition(GotoDefinitionRequest {
		stamp,
		content,
		file_path,
		language_id,
	});
	true
}

fn set_view_cursor(session: &mut EditorSession, view_id: crate::core::ViewId, byte: DocByte) {
	if let Some(view) = session.layout.find_view_mut(view_id)
		&& let ViewContent::Text(text) = &mut view.content
	{
		text.cursor.collapse();
		text.cursor.primary_mut().set_byte(byte, Affinity::After);
	}
}

fn discard_jump_entries(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	entries: impl IntoIterator<Item = JumpEntry>,
	protected_buffer: Option<BufferId>,
) {
	let entries = entries.into_iter().collect::<Vec<_>>();
	let mut buffers = std::collections::HashSet::new();
	for entry in entries {
		buffers.insert(entry.buffer_id);
		if let Some(document) = session
			.buffers
			.get_mut(entry.buffer_id)
			.and_then(|buffer| buffer.text_mut())
		{
			document.navigation_anchors.remove(entry.anchor_id);
		}
	}
	for buffer_id in buffers {
		if Some(buffer_id) != protected_buffer
			&& session.layout.view_count_for_buffer(buffer_id) == 0
			&& !session.jump_history.references(buffer_id)
		{
			release_buffer(runtime, session, buffer_id);
		}
	}
}

fn record_definition_origin(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	stamp: NavigationStamp,
	protected_buffer: Option<BufferId>,
) {
	let duplicate = session
		.jump_history
		.newest_back()
		.filter(|entry| entry.buffer_id == stamp.buffer_id)
		.and_then(|entry| {
			session
				.buffers
				.get(entry.buffer_id)
				.and_then(|buffer| buffer.text())
				.and_then(|document| document.navigation_anchors.get(entry.anchor_id))
		})
		.is_some_and(|saved| {
			saved.root == stamp.root
				&& saved.state_id == stamp.state_id
				&& saved.anchor.byte == stamp.cursor
		});
	if duplicate {
		let removed = session.jump_history.clear_forward();
		discard_jump_entries(runtime, session, removed, protected_buffer);
		return;
	}
	let Some(document) = session
		.buffers
		.get_mut(stamp.buffer_id)
		.and_then(|buffer| buffer.text_mut())
	else {
		return;
	};
	let anchor_id = document
		.navigation_anchors
		.insert(stamp.root, stamp.state_id, stamp.cursor);
	let removed = session.jump_history.record_back(JumpEntry {
		buffer_id: stamp.buffer_id,
		preferred_view_id: stamp.view_id,
		anchor_id,
	});
	discard_jump_entries(runtime, session, removed, protected_buffer);
}

fn jump_back(runtime: &RuntimeContext<'_>, session: &mut EditorSession) -> bool {
	let current_view_id = focused_doctab(&session.layout, session.focused_pane_id).id;
	let Some(current_view) = session.layout.find_view(current_view_id) else {
		session.ui.status_message = Some("Jump back requires a text view".to_string());
		return true;
	};
	let current_buffer_id = current_view.buffer_id;
	let Some((current_root, current_state, current_byte)) = session
		.buffers
		.get(current_buffer_id)
		.and_then(|buffer| buffer.text())
		.and_then(|document| {
			let ViewContent::Text(text) = &current_view.content else {
				return None;
			};
			Some((
				document.root_id,
				document.ledger.current_state_id,
				text.cursor.primary().byte(),
			))
		})
	else {
		session.ui.status_message = Some("Jump back requires a text view".to_string());
		return true;
	};

	let mut stale_count = 0usize;
	let (target_entry, target_byte) = loop {
		let Some(entry) = session.jump_history.newest_back() else {
			session.ui.status_message = Some(if stale_count == 0 {
				"No older jump".to_string()
			} else {
				format!("No older jump ({stale_count} stale entries removed)")
			});
			return true;
		};
		let resolved = session
			.buffers
			.get(entry.buffer_id)
			.and_then(|buffer| buffer.text())
			.and_then(|document| {
				let saved = document.navigation_anchors.get(entry.anchor_id)?;
				(saved.root == document.root_id
					&& saved.state_id == document.ledger.current_state_id
					&& saved.anchor.byte.get()
						<= runtime.registry.get_total_bytes(document.root_id))
				.then_some(saved.anchor.byte)
			});
		if let Some(byte) = resolved {
			break (entry, byte);
		}
		let stale = session
			.jump_history
			.pop_back()
			.expect("newest jump must remain present");
		discard_jump_entries(runtime, session, [stale], None);
		stale_count = stale_count.saturating_add(1);
	};

	let preferred_is_live = session
		.layout
		.find_view(target_entry.preferred_view_id)
		.is_some_and(|view| view.buffer_id == target_entry.buffer_id);
	let visible_target = preferred_is_live
		.then_some(target_entry.preferred_view_id)
		.or_else(|| session.layout.view_id_for_buffer(target_entry.buffer_id));
	let target_view_id = visible_target.unwrap_or(current_view_id);
	if visible_target.is_none()
		&& target_entry.buffer_id != current_buffer_id
		&& session.layout.view_count_for_buffer(current_buffer_id) == 1
		&& session
			.buffers
			.get(current_buffer_id)
			.is_some_and(|buffer| buffer.is_dirty())
	{
		session.ui.status_message =
			Some("Jump back would hide unsaved changes; write or discard first".to_string());
		return true;
	}

	let departure_anchor = {
		let document = session
			.buffers
			.get_mut(current_buffer_id)
			.and_then(|buffer| buffer.text_mut())
			.expect("validated departure buffer must remain live");
		document
			.navigation_anchors
			.insert(current_root, current_state, current_byte)
	};
	let departure = JumpEntry {
		buffer_id: current_buffer_id,
		preferred_view_id: current_view_id,
		anchor_id: departure_anchor,
	};

	if visible_target.is_none()
		&& let Err(error) =
			retarget_text_view(runtime, session, current_view_id, target_entry.buffer_id)
	{
		if let Some(document) = session
			.buffers
			.get_mut(current_buffer_id)
			.and_then(|buffer| buffer.text_mut())
		{
			document.navigation_anchors.remove(departure_anchor);
		}
		session.ui.status_message = Some(error);
		return true;
	}

	set_view_cursor(session, target_view_id, target_byte);
	if let Some(pane_id) = session.layout.pane_id_for_view(target_view_id) {
		if let Some(pane) = session.layout.find_pane_mut(pane_id) {
			pane.activate_view(target_view_id);
		}
		session.focus_pane(pane_id);
	}
	let consumed = session
		.jump_history
		.pop_back()
		.expect("applied jump must remain newest");
	if let Some(document) = session
		.buffers
		.get_mut(consumed.buffer_id)
		.and_then(|buffer| buffer.text_mut())
	{
		document.navigation_anchors.remove(consumed.anchor_id);
	}
	let removed = session.jump_history.push_forward(departure);
	discard_jump_entries(runtime, session, removed, None);
	session.ui.status_message = Some(if stale_count == 0 {
		"Jumped back".to_string()
	} else {
		format!("Jumped back ({stale_count} stale entries skipped)")
	});
	reset_global_transients(session);
	true
}

fn apply_navigation_resolved(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	resolved: NavigationResolved,
) -> bool {
	if session.pending_navigation != Some(resolved.stamp) {
		return false;
	}
	session.pending_navigation = None;
	let stamp = resolved.stamp;
	let Some(view) = session.layout.find_view(stamp.view_id) else {
		session.ui.status_message =
			Some("Definition result discarded: the requesting view was closed".to_string());
		return true;
	};
	if view.buffer_id != stamp.buffer_id {
		session.ui.status_message =
			Some("Definition result discarded: the requesting view changed documents".to_string());
		return true;
	}
	let Some(source) = session
		.buffers
		.get(stamp.buffer_id)
		.and_then(|buffer| buffer.text())
	else {
		session.ui.status_message =
			Some("Definition result discarded: the source document is unavailable".to_string());
		return true;
	};
	if source.root_id != stamp.root || source.ledger.current_state_id != stamp.state_id {
		session.ui.status_message =
			Some("Definition result discarded: the source document changed".to_string());
		return true;
	}
	let targets = match resolved.result {
		DefinitionResult::Unsupported => {
			session.ui.status_message =
				Some("Go to definition is unsupported for this language".to_string());
			return true;
		}
		DefinitionResult::NoDefinition => {
			session.ui.status_message = Some("No definition found".to_string());
			return true;
		}
		DefinitionResult::Failed(error) => {
			session.ui.status_message = Some(format!("Go to definition failed: {error}"));
			return true;
		}
		DefinitionResult::Targets(targets) if targets.is_empty() => {
			session.ui.status_message = Some("No definition found".to_string());
			return true;
		}
		DefinitionResult::Targets(targets) => targets,
	};
	let target_count = targets.len();
	let target = &targets[0];
	let range = target.focus_range.unwrap_or(target.full_range);
	if range.0 > range.1 {
		session.ui.status_message =
			Some("Definition target has an invalid source range".to_string());
		return true;
	}
	let target_byte = DocByte::new(range.0);
	match &target.location {
		DefinitionLocation::SameDocument => {
			if range.1 > runtime.registry.get_total_bytes(source.root_id) {
				session.ui.status_message =
					Some("Definition target is outside the source document".to_string());
				return true;
			}
			record_definition_origin(runtime, session, stamp, None);
			set_view_cursor(session, stamp.view_id, target_byte);
			if let Some(pane_id) = session.layout.pane_id_for_view(stamp.view_id) {
				session.focus_pane(pane_id);
			}
		}
		DefinitionLocation::File(path) => {
			let canonical = match CanonicalPath::resolve(path) {
				Ok(path) => path,
				Err(error) => {
					session.ui.status_message = Some(error);
					return true;
				}
			};
			if let Some(target_buffer) = session.buffers.buffer_for_path(&canonical)
				&& let Some(target_view) = session.layout.view_id_for_buffer(target_buffer)
			{
				let Some(target_document) = session
					.buffers
					.get(target_buffer)
					.and_then(|buffer| buffer.text())
				else {
					session.ui.status_message =
						Some("Definition target is not editable text".to_string());
					return true;
				};
				if range.1 > runtime.registry.get_total_bytes(target_document.root_id) {
					session.ui.status_message =
						Some("Definition target is outside the target document".to_string());
					return true;
				}
				record_definition_origin(runtime, session, stamp, None);
				set_view_cursor(session, target_view, target_byte);
				if let Some(pane_id) = session.layout.pane_id_for_view(target_view) {
					if let Some(pane) = session.layout.find_pane_mut(pane_id) {
						pane.activate_view(target_view);
					}
					session.focus_pane(pane_id);
				}
			} else {
				if session
					.buffers
					.get(stamp.buffer_id)
					.is_some_and(|buffer| buffer.is_dirty())
				{
					session.ui.status_message = Some(
						"Definition is in another file; write or discard source changes first"
							.to_string(),
					);
					return true;
				}
				let (target_buffer, _) =
					match resolve_or_load_buffer(runtime, session, path.display().to_string()) {
						Ok(target) => target,
						Err(error) => {
							session.ui.status_message =
								Some(format!("Unable to open definition target: {error}"));
							return true;
						}
					};
				let Some(target_document) = session
					.buffers
					.get(target_buffer)
					.and_then(|buffer| buffer.text())
				else {
					session.ui.status_message =
						Some("Definition target is not editable text".to_string());
					return true;
				};
				if range.1 > runtime.registry.get_total_bytes(target_document.root_id) {
					if session.layout.view_count_for_buffer(target_buffer) == 0 {
						release_buffer(runtime, session, target_buffer);
					}
					session.ui.status_message =
						Some("Definition target is outside the target document".to_string());
					return true;
				}
				record_definition_origin(runtime, session, stamp, Some(target_buffer));
				if let Err(error) =
					retarget_text_view(runtime, session, stamp.view_id, target_buffer)
				{
					session.ui.status_message = Some(error);
					return true;
				}
				set_view_cursor(session, stamp.view_id, target_byte);
				if let Some(pane_id) = session.layout.pane_id_for_view(stamp.view_id) {
					session.focus_pane(pane_id);
				}
			}
		}
	}
	session.ui.status_message = Some(if target_count == 1 {
		"Jumped to definition".to_string()
	} else {
		format!("Jumped to definition ({target_count} definitions)")
	});
	reset_global_transients(session);
	true
}

fn handle_file_command_for_view(
	runtime: &RuntimeContext<'_>,
	buffers: &mut BufferStore,
	layout: &mut LayoutNode,
	events: &mut EngineEventQueue,
	view_id: crate::core::ViewId,
	ui: &mut super::session::UiState,
	next_dynamic_device_id: &mut u16,
	save_durability: SaveDurability,
	command: EditorCommand,
) -> Option<(CommandEffect, Option<super::language::LanguageId>)> {
	with_text_buffer_mut(runtime.registry, buffers, layout, events, view_id, |tab| {
		let effect = handle_file_doctab_command(
			runtime,
			tab,
			ui,
			next_dynamic_device_id,
			save_durability,
			command,
		);
		(effect, tab.document.active_language_id)
	})
}

fn handle_diff_split(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	path: String,
) -> bool {
	// An existing file keeps the original file-vs-file behavior; anything
	// else — including the empty default, which means HEAD — is a Git
	// revision of the focused document.
	let arg = path.trim();
	if arg.is_empty() || !std::path::Path::new(arg).is_file() {
		return handle_revision_diff_split(session, arg);
	}
	handle_file_diff_split(runtime, session, path)
}

fn handle_file_diff_split(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	path: String,
) -> bool {
	let left_pane_id = session.focused_pane_id;
	let left_buffer_id = focused_doctab(&session.layout, left_pane_id).buffer_id;
	let canonical = match CanonicalPath::resolve(&path) {
		Ok(path) => path,
		Err(err) => {
			session.ui.status_message = Some(err);
			return true;
		}
	};
	if session.buffers.buffer_for_path(&canonical) == Some(left_buffer_id) {
		session.ui.status_message = Some("Cannot diff a buffer against itself".to_string());
		return true;
	}
	if !split_target_pane(runtime.registry, session, SplitDirection::Vertical) {
		return false;
	}
	let right_pane_id = session.focused_pane_id;

	let _ = load_file_into_pane(runtime, session, right_pane_id, path);
	let right_buffer_id = focused_doctab(&session.layout, right_pane_id).buffer_id;
	if right_buffer_id == left_buffer_id {
		let status = session.ui.status_message.take();
		let _ = session.close_focused_pane();
		session.focused_pane_id = left_pane_id;
		session.ui.status_message =
			status.or_else(|| Some("Cannot diff a buffer against itself".to_string()));
		return true;
	}

	// Pair them
	super::diff::compute_pane_diff(runtime.registry, session, left_pane_id, right_pane_id);
	true
}

/// Start the async fetch for `:panediff <REV>`: `git cat-file blob
/// <REV>:<relpath>` of
/// the focused document. The snapshot pane is created when the job completes.
fn handle_revision_diff_split(session: &mut EditorSession, arg: &str) -> bool {
	let revision = if arg.is_empty() { "HEAD" } else { arg };
	if let Err(err) = super::git::validate_revision(revision) {
		session.ui.status_message = Some(format!("Not a file, and {err}"));
		return true;
	}
	let left_pane_id = session.focused_pane_id;
	let left_buffer_id = focused_doctab(&session.layout, left_pane_id).buffer_id;
	let Some(file_path) = session
		.buffers
		.get(left_buffer_id)
		.and_then(|buffer| buffer.text())
		.and_then(|document| document.file_path.clone())
	else {
		session.ui.status_message =
			Some("Cannot diff an unnamed buffer against Git history".to_string());
		return true;
	};
	let file = std::path::Path::new(&file_path);
	let canonical = file.canonicalize().unwrap_or_else(|_| file.to_path_buf());
	let Some(repo_root) = session.git_branch_cache.repo_root_for_path(&canonical) else {
		session.ui.status_message = Some(format!("{file_path} is not in a Git repository"));
		return true;
	};
	let Some(rel_path) = super::git::repo_relative_path(&repo_root, &canonical) else {
		session.ui.status_message = Some(format!("{file_path} is outside its Git repository"));
		return true;
	};
	// Git permits LF in tracked filenames, but the fetch phase's `--batch`
	// stdin is LF-framed: an embedded newline would smuggle in a second
	// object spec. Refuse precisely instead of mangling the request.
	if rel_path.contains('\n') {
		session.ui.status_message =
			Some("Cannot diff a path containing a newline against Git history".to_string());
		return true;
	}
	if session.snapshot_fetch_job.is_some() {
		session.ui.status_message = Some("A snapshot diff is already in progress".to_string());
		return true;
	}
	// Phase 1 of the two-phase acquisition: pin the symbolic revision to an
	// immutable commit OID. This is the acquisition's ONLY symbolic
	// resolution — one spec, one atomic ref read. (Resolving the commit and
	// blob as two specs of one rev-parse is not atomic: git reads the ref
	// per spec and a concurrent `branch -f` interleaves.) Phase 2 derives
	// blob identity and content from the pinned commit. No
	// `--end-of-options` here: rev-parse echoes it to stdout (it is a
	// pass-through plumbing command), and the spec is option-safe by
	// construction — it starts with the validated revision, which cannot
	// begin with `-`.
	let spec = JobSpec {
		program: PathBuf::from("git"),
		args: vec![
			"--no-optional-locks".into(),
			"rev-parse".into(),
			format!("{revision}^{{commit}}").into(),
		],
		cwd: Some(repo_root.clone()),
		stdin: None,
		timeout: Duration::from_secs(10),
		completion: Some(super::git::git_resolve_action_lease(
			super::git::GitResolveAction {
				left_pane_id,
				left_buffer_id,
				repo_root,
				revision: revision.to_string(),
				rel_path,
			},
		)),
	};
	match session.jobs.spawn(super::git::GIT_JOB_OWNER, true, spec) {
		Ok(job) => {
			session.snapshot_fetch_job = Some(job);
			session.ui.status_message = Some(format!("Diffing against {revision}…"));
		}
		Err(error) => {
			session.ui.status_message = Some(format!("Git diff: {error}"));
		}
	}
	true
}

/// Start a `:diffstep` from either side of the focused snapshot diff pair.
/// Negative deltas walk older from the pinned commit; positive deltas walk
/// newer along the HEAD-rooted timeline of the worktree file's current path.
fn handle_diff_step(session: &mut EditorSession, delta: i64) -> bool {
	if delta == 0 {
		session.ui.status_message = Some("Nothing to step: use older, newer, or ±N".to_string());
		return true;
	}
	let focused = session.focused_pane_id;
	let Some(pair) = session.diff_pairs.get(&focused) else {
		session.ui.status_message = Some("No diff pair in the focused pane".to_string());
		return true;
	};
	let partner = pair.partner_pane_id;
	let focused_buffer = focused_doctab(&session.layout, focused).buffer_id;
	let Some(partner_buffer) = session
		.layout
		.find_pane(partner)
		.map(|pane| pane.active_tab().buffer_id)
	else {
		session.ui.status_message = Some("Diff partner pane is gone".to_string());
		return true;
	};
	let (snapshot_pane_id, snapshot_buffer_id, left_pane_id, left_buffer_id) =
		if session.snapshot_partners.contains_key(&focused_buffer) {
			(focused, focused_buffer, partner, partner_buffer)
		} else if session.snapshot_partners.contains_key(&partner_buffer) {
			(partner, partner_buffer, focused, focused_buffer)
		} else {
			session.ui.status_message =
				Some("The focused diff has no Git snapshot to step".to_string());
			return true;
		};
	let crate::engine::session::SnapshotProvenance::GitRevision {
		repo_root,
		rel_path,
		commit_oid,
		..
	} = session
		.snapshot_partners
		.get(&snapshot_buffer_id)
		.expect("snapshot membership checked above")
		.clone();
	if session.snapshot_fetch_job.is_some() {
		session.ui.status_message = Some("A snapshot diff is already in progress".to_string());
		return true;
	}
	// An edited snapshot must not be silently replaced: stepping is a
	// document swap, and unsaved edits have nowhere to go.
	if session
		.buffers
		.get(snapshot_buffer_id)
		.is_some_and(|buffer| buffer.is_dirty())
	{
		session.ui.status_message =
			Some("Snapshot has unsaved edits; :diffstep would discard them".to_string());
		return true;
	}
	let step = super::git::StepContext {
		snapshot_pane_id,
		snapshot_buffer_id,
		left_pane_id,
		left_buffer_id,
		repo_root: repo_root.clone(),
		pinned_commit_oid: commit_oid.clone(),
		left_file_path: session
			.buffers
			.get(left_buffer_id)
			.and_then(|buffer| buffer.text())
			.and_then(|document| document.file_path.clone()),
	};

	let spec = if delta < 0 {
		let steps = delta.unsigned_abs();
		// Older walk: rooted at the pinned commit with the snapshot's own
		// path (valid at that commit by construction). Record 0 is the
		// current content's origin. Uncapped: deletion and merge records
		// are skipped by the parser in unbounded numbers, so any fixed `-n`
		// slack manufactures false oldest boundaries; the job capture
		// budget is the real limit, handled at completion. `-z` framing
		// keeps exotic paths raw.
		JobSpec {
			program: PathBuf::from("git"),
			args: vec![
				"--no-optional-locks".into(),
				"log".into(),
				"-z".into(),
				"--first-parent".into(),
				"--format=%H".into(),
				"--follow".into(),
				"--name-status".into(),
				commit_oid.clone().into(),
				"--".into(),
				rel_path.clone().into(),
			],
			cwd: Some(repo_root),
			stdin: None,
			timeout: Duration::from_secs(10),
			completion: Some(super::git::git_older_walk_action_lease(
				super::git::GitOlderWalkAction { step, steps },
			)),
		}
	} else {
		// Newer walk needs the worktree file's current repo-relative path:
		// `--follow` walks renames backward only from its starting point.
		let Some(head_rel_path) = session
			.buffers
			.get(left_buffer_id)
			.and_then(|buffer| buffer.text())
			.and_then(|document| document.file_path.clone())
			.and_then(|file_path| {
				let file = std::path::Path::new(&file_path);
				let canonical = file.canonicalize().unwrap_or_else(|_| file.to_path_buf());
				super::git::repo_relative_path(&repo_root, &canonical)
			})
		else {
			session.ui.status_message =
				Some("The worktree document has no repository path".to_string());
			return true;
		};
		JobSpec {
			program: PathBuf::from("git"),
			args: vec![
				"--no-optional-locks".into(),
				"log".into(),
				"--first-parent".into(),
				"-n".into(),
				"1".into(),
				"--format=%H".into(),
				"--follow".into(),
				commit_oid.clone().into(),
				"--".into(),
				rel_path.clone().into(),
			],
			cwd: Some(repo_root),
			stdin: None,
			timeout: Duration::from_secs(10),
			completion: Some(super::git::git_newer_normalize_action_lease(
				super::git::GitNewerNormalizeAction {
					step,
					steps: delta as u64,
					head_rel_path,
				},
			)),
		}
	};
	match session.jobs.spawn(super::git::GIT_JOB_OWNER, true, spec) {
		Ok(job) => {
			session.snapshot_fetch_job = Some(job);
			session.ui.status_message = Some("Stepping through history…".to_string());
		}
		Err(error) => {
			session.ui.status_message = Some(format!("Git step: {error}"));
		}
	}
	true
}

fn read_only_virtual_blocks_command(command: &EditorCommand) -> bool {
	matches!(
		command,
		EditorCommand::InsertChar(_)
			| EditorCommand::InsertText(_)
			| EditorCommand::ReplaceChar(_)
			| EditorCommand::ToggleCaseAtCursor
			| EditorCommand::OpenLineBelow
			| EditorCommand::OpenLineAbove
			| EditorCommand::AppendAtLineEnd
			| EditorCommand::Backspace
			| EditorCommand::Delete
			| EditorCommand::Put { .. }
			| EditorCommand::DeleteLine { .. }
			| EditorCommand::DeleteInnerWord
			| EditorCommand::ChangeInnerWord
			| EditorCommand::DeleteWordMotion { .. }
			| EditorCommand::ChangeWordMotion { .. }
			| EditorCommand::DeleteToLineEnd
			| EditorCommand::JoinLine
			| EditorCommand::SetVisualSelection { .. }
			| EditorCommand::VisualYank { .. }
			| EditorCommand::VisualDelete { .. }
			| EditorCommand::VisualChange { .. }
			| EditorCommand::DeleteAroundFunction { .. }
			| EditorCommand::ChangeAroundFunction
			| EditorCommand::ReplaceDocument { .. }
			| EditorCommand::WriteFile
			| EditorCommand::WriteFileAs(_)
			| EditorCommand::WriteAndQuit
			| EditorCommand::ReloadFile { .. }
			| EditorCommand::SubstituteAll { .. }
			| EditorCommand::SubstituteConfirm { .. }
			| EditorCommand::ConfirmResponse(_)
			| EditorCommand::Sort { .. }
			| EditorCommand::Uniq { .. }
			| EditorCommand::Undo
			| EditorCommand::Redo
			| EditorCommand::SetLanguage(_)
			| EditorCommand::AddCursorAdjacent(_)
			| EditorCommand::AddCursorAt(_)
	)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum MultiCursorPolicy {
	FanOut,
	Preserve,
	Collapse,
	RejectWhileMultiple,
}

fn multi_cursor_policy(command: &EditorCommand) -> MultiCursorPolicy {
	use MultiCursorPolicy::{Collapse, FanOut, Preserve, RejectWhileMultiple};
	match command {
		EditorCommand::InsertChar(_)
		| EditorCommand::InsertText(_)
		| EditorCommand::ReplaceChar(_)
		| EditorCommand::ToggleCaseAtCursor
		| EditorCommand::Backspace
		| EditorCommand::Delete
		| EditorCommand::AppendAtLineEnd
		| EditorCommand::LineStart
		| EditorCommand::FirstNonWhitespace
		| EditorCommand::LineEnd
		| EditorCommand::SmartHome
		| EditorCommand::AddCursorAdjacent(_)
		| EditorCommand::AddCursorAt(_)
		| EditorCommand::CollapseCursors
		| EditorCommand::Undo
		| EditorCommand::Redo
		| EditorCommand::PageUp
		| EditorCommand::PageDown => FanOut,
		EditorCommand::MoveCursor(
			MoveDirection::Up
			| MoveDirection::Down
			| MoveDirection::Left
			| MoveDirection::Right
			| MoveDirection::NextWord
			| MoveDirection::PrevWord
			| MoveDirection::NextWordEnd
			| MoveDirection::FindChar { .. },
		) => FanOut,
		EditorCommand::OpenLineBelow
		| EditorCommand::OpenLineAbove
		| EditorCommand::DeleteLine { .. }
		| EditorCommand::DeleteInnerWord
		| EditorCommand::ChangeInnerWord
		| EditorCommand::DeleteWordMotion { .. }
		| EditorCommand::ChangeWordMotion { .. }
		| EditorCommand::DeleteToLineEnd
		| EditorCommand::JoinLine
		| EditorCommand::YankLine { .. }
		| EditorCommand::YankWordMotion { .. }
		| EditorCommand::Put { .. }
		| EditorCommand::DeleteAroundFunction { .. }
		| EditorCommand::ChangeAroundFunction => RejectWhileMultiple,
		EditorCommand::ToggleFold
		| EditorCommand::CloseFold
		| EditorCommand::OpenFold
		| EditorCommand::CloseAllFolds
		| EditorCommand::OpenAllFolds
		| EditorCommand::MatchDelimiter
		| EditorCommand::GotoDefinition
		| EditorCommand::JumpBack
		| EditorCommand::ClickCursor(_)
		| EditorCommand::GotoLine(_)
		| EditorCommand::MoveCursor(MoveDirection::Top | MoveDirection::Bottom)
		| EditorCommand::SetVisualSelection { .. }
		| EditorCommand::VisualYank { .. }
		| EditorCommand::VisualDelete { .. }
		| EditorCommand::VisualChange { .. }
		| EditorCommand::LoadFile(_)
		| EditorCommand::ReplaceDocument { .. }
		| EditorCommand::ReloadFile { .. }
		| EditorCommand::SearchStart(_)
		| EditorCommand::SearchNext
		| EditorCommand::SearchPrev
		| EditorCommand::SubstituteAll { .. }
		| EditorCommand::SubstituteConfirm { .. }
		| EditorCommand::Sort { .. }
		| EditorCommand::Uniq { .. }
		| EditorCommand::ConfirmResponse(_)
		| EditorCommand::SelectInnerWord
		| EditorCommand::SelectAroundFunction
		| EditorCommand::NextFunction
		| EditorCommand::SearchWordUnderCursor { .. } => Collapse,
		EditorCommand::Resize(_, _)
		| EditorCommand::FocusPane(_)
		| EditorCommand::FocusPaneDirection(_)
		| EditorCommand::ActivateDocTab { .. }
		| EditorCommand::CloseDocTab { .. }
		| EditorCommand::SplitPane(_)
		| EditorCommand::NewPane { .. }
		| EditorCommand::ClosePane
		| EditorCommand::ResizePane(_, _)
		| EditorCommand::ResizePaneBoundary { .. }
		| EditorCommand::MovePane { .. }
		| EditorCommand::Scroll(_)
		| EditorCommand::ScrollViewport(_)
		| EditorCommand::ScrollViewportToLine(_)
		| EditorCommand::ToggleWrap
		| EditorCommand::SetWrap(_)
		| EditorCommand::SetMinimap(_)
		| EditorCommand::SetLineNumbers(_)
		| EditorCommand::SetRelativeLineNumbers(_)
		| EditorCommand::SetVisibleWhitespace(_)
		| EditorCommand::PersistWrap(_)
		| EditorCommand::PersistMinimap(_)
		| EditorCommand::PersistLineNumbers(_)
		| EditorCommand::PersistRelativeLineNumbers(_)
		| EditorCommand::PersistVisibleWhitespace(_)
		| EditorCommand::PersistReload(_)
		| EditorCommand::PersistSaveDurability(_)
		| EditorCommand::SetGitRefresh(_)
		| EditorCommand::WriteInit { .. }
		| EditorCommand::WriteDefaultInit { .. }
		| EditorCommand::OpenHostPanel { .. }
		| EditorCommand::RefreshHostPanel { .. }
		| EditorCommand::ClearVisualSelection
		| EditorCommand::CheckFileReload
		| EditorCommand::DiffSplit(_)
		| EditorCommand::DiffStep(_)
		| EditorCommand::MarkdownPreview { .. }
		| EditorCommand::OpenUastInspector { .. }
		| EditorCommand::ActivateUastInspectorRow
		| EditorCommand::ToggleUastInspectorFold
		| EditorCommand::ToggleUastInspectorFollowCursor
		| EditorCommand::ActivateUastGraphNode { .. }
		| EditorCommand::ExportUastDot { .. }
		| EditorCommand::ShowMarkdownDocument { .. }
		| EditorCommand::WriteFile
		| EditorCommand::WriteFileAs(_)
		| EditorCommand::WriteAndQuit
		| EditorCommand::OpenFileBrowser
		| EditorCommand::OpenFileBrowserAt(_)
		| EditorCommand::RefreshFileBrowser
		| EditorCommand::ActivateBrowserEntry
		| EditorCommand::ClickBrowserEntry { .. }
		| EditorCommand::CollapseBrowserEntryOrParent
		| EditorCommand::DocTabNew
		| EditorCommand::DocTabEdit(_)
		| EditorCommand::DocTabNext
		| EditorCommand::DocTabPrev
		| EditorCommand::DocTabClose
		| EditorCommand::SetTheme(_)
		| EditorCommand::SetBackground(_)
		| EditorCommand::PersistTheme(_)
		| EditorCommand::PersistLangTheme { .. }
		| EditorCommand::PersistNoLangTheme(_)
		| EditorCommand::PersistUiColor { .. }
		| EditorCommand::QueryUiColor(_)
		| EditorCommand::SourceRune(_)
		| EditorCommand::UnloadRune(_)
		| EditorCommand::ClearSearch
		| EditorCommand::SetLanguage(_)
		| EditorCommand::GitStatus
		| EditorCommand::RefreshGitStatus
		| EditorCommand::OpenGitStatusEntry(_)
		| EditorCommand::MouseIntent { .. }
		| EditorCommand::QueryDocumentText
		| EditorCommand::BeginPasswordPrompt { .. }
		| EditorCommand::SubmitPassword(_)
		| EditorCommand::LockSecrets
		| EditorCommand::LoadEncryptedFile(_)
		| EditorCommand::WriteEncryptedFile
		| EditorCommand::WriteEncryptedFileAs(_)
		| EditorCommand::Quit => Preserve,
	}
}

fn handle_browser_command(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	command: EditorCommand,
) -> Option<CommandEffect> {
	match command {
		EditorCommand::OpenFileBrowser => {
			let root = match std::env::current_dir() {
				Ok(path) => path,
				Err(err) => {
					session.ui.status_message = Some(format!("File browser error: {}", err));
					return Some(CommandEffect::render());
				}
			};
			return Some(open_file_browser_at_path(runtime, session, root));
		}
		EditorCommand::OpenFileBrowserAt(path) => {
			return Some(open_file_browser_at_path(
				runtime,
				session,
				PathBuf::from(path),
			));
		}
		_ => {}
	}

	let focused_view = focused_doctab(&session.layout, session.focused_pane_id);
	let (focused_is_browser, focused_is_markdown_preview) = (
		focused_view.is_file_browser(),
		focused_view.is_markdown_preview(),
	);

	if focused_is_markdown_preview {
		return match command {
			other if read_only_virtual_blocks_command(&other) => {
				session.ui.status_message = Some("Markdown image preview is read-only".to_string());
				reset_global_transients(session);
				Some(CommandEffect::render())
			}
			_ => None,
		};
	}

	if !focused_is_browser {
		return None;
	}

	match command {
		EditorCommand::RefreshFileBrowser => {
			let focused_pane_id = session.focused_pane_id;
			let EditorSession {
				buffers,
				layout,
				ui,
				..
			} = session;
			let view_id = focused_doctab(layout, focused_pane_id).id;
			let result = with_file_browser_buffer_mut(buffers, layout, view_id, |editor| {
				refresh_file_browser(runtime.registry, editor)
			})
			.expect("focused file browser pair");
			if let Err(err) = result {
				ui.status_message = Some(err);
			}
			reset_global_transients(session);
			Some(CommandEffect::render())
		}
		EditorCommand::ActivateBrowserEntry => {
			let focused_pane_id = session.focused_pane_id;
			let action = {
				let EditorSession {
					buffers,
					layout,
					ui,
					..
				} = session;
				let view_id = focused_doctab(layout, focused_pane_id).id;
				let result = with_file_browser_buffer_mut(buffers, layout, view_id, |editor| {
					activate_selected_browser_entry(runtime.registry, editor)
				})
				.expect("focused file browser pair");
				match result {
					Ok(action) => action,
					Err(err) => {
						ui.status_message = Some(err);
						reset_global_transients(session);
						return Some(CommandEffect::render());
					}
				}
			};
			Some(apply_browser_entry_action(runtime, session, action))
		}
		EditorCommand::ClickBrowserEntry { line, double_click } => {
			let focused_pane_id = session.focused_pane_id;
			let action = {
				let EditorSession {
					buffers,
					layout,
					ui,
					..
				} = session;
				let view_id = focused_doctab(layout, focused_pane_id).id;
				let result = with_file_browser_buffer_mut(buffers, layout, view_id, |editor| {
					click_browser_entry(runtime.registry, editor, line, double_click)
				})
				.expect("focused file browser pair");
				match result {
					Ok(action) => action,
					Err(err) => {
						ui.status_message = Some(err);
						reset_global_transients(session);
						return Some(CommandEffect::render());
					}
				}
			};
			Some(apply_browser_entry_action(runtime, session, action))
		}
		EditorCommand::CollapseBrowserEntryOrParent => {
			let focused_pane_id = session.focused_pane_id;
			let changed = {
				let EditorSession {
					buffers,
					layout,
					ui,
					..
				} = session;
				let view_id = focused_doctab(layout, focused_pane_id).id;
				let result = with_file_browser_buffer_mut(buffers, layout, view_id, |editor| {
					collapse_selected_browser_entry_or_parent(runtime.registry, editor)
				})
				.expect("focused file browser pair");
				match result {
					Ok(changed) => changed,
					Err(err) => {
						ui.status_message = Some(err);
						reset_global_transients(session);
						return Some(CommandEffect::render());
					}
				}
			};
			if !changed {
				session.ui.status_message = Some("Nothing to collapse".to_string());
			}
			reset_global_transients(session);
			Some(CommandEffect::render())
		}
		other if read_only_virtual_blocks_command(&other) => {
			session.ui.status_message = Some("File browser is read-only".to_string());
			reset_global_transients(session);
			Some(CommandEffect::render())
		}
		_ => None,
	}
}

fn apply_browser_entry_action(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	action: BrowserEntryAction,
) -> CommandEffect {
	match action {
		BrowserEntryAction::None => CommandEffect::render(),
		BrowserEntryAction::OpenFile(path) => {
			let browser_pane_id = session.focused_pane_id;
			let Some(target_pane_id) =
				ensure_browser_target_pane(runtime.registry, session, browser_pane_id)
			else {
				session.ui.status_message =
					Some("Unable to find a target pane for file open".to_string());
				reset_global_transients(session);
				return CommandEffect::render();
			};
			load_file_into_pane(
				runtime,
				session,
				target_pane_id,
				path.to_string_lossy().into_owned(),
			)
			.effect
		}
	}
}

fn git_status_entry_path(
	registry: &UastRegistry,
	session: &EditorSession,
	line: DocLine,
) -> Result<PathBuf, String> {
	if line == DocLine::ZERO {
		return Err("The Git branch header is not a file".to_string());
	}
	let git_buffer_id = session
		.output_panels
		.get("git-status")
		.copied()
		.ok_or_else(|| "Git status panel is not available".to_string())?;
	let focused = focused_doctab(&session.layout, session.focused_pane_id);
	if focused.buffer_id != git_buffer_id {
		return Err("Git status entry is no longer focused".to_string());
	}
	let root = session
		.buffers
		.get(git_buffer_id)
		.and_then(|buffer| buffer.root_id())
		.ok_or_else(|| "Git status panel has no document".to_string())?;
	let line_bytes = super::support::read_line_bytes_sparse(registry, root, line, false)
		.map_err(|_| "Git status entry is no longer available".to_string())?;
	let line = std::str::from_utf8(&line_bytes)
		.map_err(|_| "Git status entry has an invalid path".to_string())?
		.trim_end_matches(['\r', '\n']);
	let (_, path) = line
		.split_once(' ')
		.ok_or_else(|| "This Git status row does not name a file".to_string())?;
	let path = path.split_once(" ← ").map_or(path, |(current, _)| current);
	let repo_root = session
		.git_status_repo_root
		.as_ref()
		.ok_or_else(|| "Git repository is no longer available".to_string())?;
	let path = repo_root.join(path);
	if !path.is_file() {
		return Err("Git status file no longer exists".to_string());
	}
	Ok(path)
}

fn start_git_status_job(
	session: &mut EditorSession,
	buffer_id: BufferId,
	repo_root: PathBuf,
) -> Result<(), String> {
	let spec = JobSpec {
		program: PathBuf::from("git"),
		args: vec![
			"--no-optional-locks".into(),
			"status".into(),
			"--porcelain=v2".into(),
			"--branch".into(),
		],
		cwd: Some(repo_root),
		stdin: None,
		timeout: Duration::from_secs(10),
		completion: Some(super::git::git_status_action_lease(buffer_id)),
	};
	let job = session
		.jobs
		.spawn(super::git::GIT_JOB_OWNER, true, spec)
		.map_err(|error| error.to_string())?;
	session.git_status_job = Some(job);
	Ok(())
}

fn open_git_status_repo_root(session: &EditorSession) -> Option<PathBuf> {
	let buffer_id = session.output_panels.get("git-status").copied()?;
	session.layout.view_id_for_buffer(buffer_id)?;
	session.git_status_repo_root.clone()
}

fn select_git_status_target_pane(
	session: &EditorSession,
	source_pane_id: PaneId,
) -> Option<PaneId> {
	if let Some(pane_id) = session.navigation_target_pane_id()
		&& pane_id != source_pane_id
	{
		return Some(pane_id);
	}
	let source_rect = session
		.pane_layouts()
		.into_iter()
		.find(|layout| layout.pane_id == source_pane_id)?
		.rect;
	let mut fallback = None;
	for layout in session.pane_layouts() {
		if layout.pane_id == source_pane_id {
			continue;
		}
		let pane = session.layout.find_pane(layout.pane_id)?;
		let Some(buffer) = session.buffers.get(pane.active_tab().buffer_id) else {
			continue;
		};
		if buffer.is_output_panel() || buffer.is_file_browser() || buffer.is_markdown_preview() {
			continue;
		}
		if layout.rect.screen_col.get() >= source_rect.right().get() {
			return Some(layout.pane_id);
		}
		fallback.get_or_insert(layout.pane_id);
	}
	fallback
}

fn sync_diff_scroll(
	registry: &UastRegistry,
	session: &mut EditorSession,
	command: &EditorCommand,
) -> bool {
	let focused_id = session.focused_pane_id;
	let Some(pair) = session.diff_pairs.get(&focused_id) else {
		return false;
	};
	let partner_id = pair.partner_pane_id;
	let blocks = pair.blocks.clone();

	let focused_tab = session
		.layout
		.find_pane(focused_id)
		.map(|p| p.active_tab())
		.unwrap();
	let focused_line = focused_tab.common().scroll_y;

	// Find the corresponding line in the partner
	let mut partner_line = focused_line;
	for block in &blocks {
		if focused_line >= block.range.0.get() && focused_line < block.range.1.get() {
			// Inside a block, interpolate
			let offset = focused_line - block.range.0.get();
			let partner_start = block.partner_range.0.get();
			let partner_end = block.partner_range.1.get();
			let focused_len = block.range.1.get() - block.range.0.get();
			let partner_len = partner_end - partner_start;

			partner_line =
				partner_start + (offset * partner_len).checked_div(focused_len).unwrap_or(0);
			break;
		} else if focused_line < block.range.0.get() {
			// Before this block, we assume 1:1 mapping from last block or start
			break;
		} else {
			// After this block, adjust base
			let focused_end = block.range.1.get();
			let partner_end = block.partner_range.1.get();
			partner_line = focused_line
				.saturating_sub(focused_end)
				.saturating_add(partner_end);
		}
	}

	let EditorSession {
		buffers,
		layout,
		events,
		ui,
		..
	} = session;
	let partner_view_id = layout.find_pane(partner_id).unwrap().active_tab().id;
	let Some(()) = with_text_buffer_mut(
		registry,
		buffers,
		layout,
		events,
		partner_view_id,
		|partner_tab| {
			// In linked scrolling, we want the partner to follow. Eventually this
			// should use the calculated visual alignment in `partner_line`.
			handle_navigation_doctab_command(registry, partner_tab, ui, command.clone());
		},
	) else {
		return false;
	};
	let _ = partner_line; // Keep for future alignment logic
	true
}

fn append_init_persistence_status(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	edit: &system::InitSettingEdit,
) {
	let Some(path) = runtime.init_script else {
		return;
	};
	let current = session.ui.status_message.take().unwrap_or_default();
	let suffix = match persist_setting_to_init(runtime, session, path, edit) {
		Ok(true) => "init.rn updated".to_string(),
		Ok(false) => "init.rn already current".to_string(),
		Err(error) => format!("session only: {error}"),
	};
	session.ui.status_message = Some(if current.is_empty() {
		suffix
	} else {
		format!("{current} · {suffix}")
	});
}

fn persist_setting_to_init(
	runtime: &RuntimeContext<'_>,
	session: &mut EditorSession,
	path: &Path,
	edit: &system::InitSettingEdit,
) -> Result<bool, String> {
	let canonical = CanonicalPath::resolve(path)?;
	let open_buffer = session.buffers.buffer_for_path(&canonical);
	let expected_source = if let Some(buffer_id) = open_buffer {
		let buffer = session
			.buffers
			.get(buffer_id)
			.ok_or_else(|| "the open init.rn buffer disappeared".to_string())?;
		if buffer.is_dirty() {
			return Err("init.rn has unsaved edits".to_string());
		}
		let document = buffer
			.text()
			.ok_or_else(|| "init.rn is not open as a text buffer".to_string())?;
		let source_path = document
			.backing_path
			.as_deref()
			.or(document.file_path.as_deref())
			.map(Path::new);
		let bytes = read_document_snapshot(runtime.registry, document.root_id, source_path)?;
		Some(
			String::from_utf8(bytes)
				.map_err(|_| "the open init.rn buffer is not valid UTF-8".to_string())?,
		)
	} else {
		None
	};
	let changed = system::persist_init_setting_at(
		path,
		runtime.init_entrypoint.unwrap_or("main"),
		expected_source.as_deref(),
		edit,
		runtime.lang_registry,
	)?;
	if !changed {
		return Ok(false);
	}

	if let Some(buffer_id) = open_buffer {
		let view_id = session
			.layout
			.view_id_for_buffer(buffer_id)
			.ok_or_else(|| "init.rn is open without a live view".to_string())?;
		let EditorSession {
			buffers,
			layout,
			events,
			ui,
			next_dynamic_device_id,
			..
		} = session;
		with_text_buffer_mut(runtime.registry, buffers, layout, events, view_id, |tab| {
			// Force the ordinary external-change path even when a filesystem
			// has coarse timestamps and the replacement is the same length.
			tab.document.file_stamp = None;
			handle_auto_reload_doctab_command(runtime, tab, ui, next_dynamic_device_id)
		})
		.ok_or_else(|| "init.rn was written, but its open buffer could not reload".to_string())?;
		if buffers
			.get(buffer_id)
			.is_some_and(|buffer| buffer.is_dirty())
		{
			return Err("init.rn was written, but its open buffer did not reload".to_string());
		}
	}
	Ok(true)
}

fn dispatch_command(
	context: DispatchContext<'_, '_>,
	session: &mut EditorSession,
	cmd: EditorCommand,
) -> DispatchOutcome {
	let DispatchContext {
		runtime,
		tx_internal,
		tx_view,
		reactor,
		resolver,
		lang_registry,
	} = context;
	let registry = runtime.registry;
	let mut needs_render = false;
	if let EditorCommand::WriteInit { force } = cmd {
		session.ui.status_message = Some(
			match runtime
				.init_script
				.ok_or_else(|| "No init.rn is configured for this editor host".to_string())
				.and_then(|path| {
					system::write_init_file(
						path,
						runtime.init_entrypoint.unwrap_or("main"),
						&session.persistent_settings,
						session.git_refresh_mode,
						lang_registry,
						force,
					)
				}) {
				Ok(path) => format!("Wrote Rune configuration to {}", path.display()),
				Err(error) => format!("Could not write init.rn: {error}"),
			},
		);
		return DispatchOutcome::continue_with(true);
	}
	if let EditorCommand::WriteDefaultInit { force } = cmd {
		session.ui.status_message = Some(
			match runtime
				.init_script
				.ok_or_else(|| "No init.rn is configured for this editor host".to_string())
				.and_then(|path| {
					system::write_default_init_file(
						path,
						runtime.init_entrypoint.unwrap_or("main"),
						lang_registry,
						force,
					)
				}) {
				Ok(path) => format!("Reset Rune configuration to defaults at {}", path.display()),
				Err(error) => format!("Could not reset init.rn: {error}"),
			},
		);
		return DispatchOutcome::continue_with(true);
	}
	let cmd = match cmd {
		EditorCommand::MouseIntent {
			viewport_id,
			intent,
		} => {
			if viewport_id != session.viewport_id {
				return DispatchOutcome::continue_with(false);
			}
			match intent {
				MouseIntent::FocusPane(pane_id) => EditorCommand::FocusPane(pane_id),
				MouseIntent::ActivateDocTab { pane_id, view_id } => {
					EditorCommand::ActivateDocTab { pane_id, view_id }
				}
				MouseIntent::CloseDocTab { pane_id, view_id } => {
					EditorCommand::CloseDocTab { pane_id, view_id }
				}
				MouseIntent::ScrollViewport(delta) => EditorCommand::ScrollViewport(delta),
				MouseIntent::ScrollViewportToLine(line) => {
					EditorCommand::ScrollViewportToLine(line)
				}
				MouseIntent::ClickBrowserEntry { line, double_click } => {
					EditorCommand::ClickBrowserEntry { line, double_click }
				}
				MouseIntent::OpenGitStatusEntry { line } => EditorCommand::OpenGitStatusEntry(line),
				MouseIntent::ClickCursor { target, alt } => {
					let view = focused_doctab(&session.layout, session.focused_pane_id);
					let is_text_document = session
						.buffers
						.get(view.buffer_id)
						.is_some_and(|buffer| buffer.text().is_some());
					if is_text_document
						&& alt && matches!(
						session.input.editor_mode(),
						EditorMode::Normal | EditorMode::Insert
					) {
						EditorCommand::AddCursorAt(target)
					} else {
						EditorCommand::ClickCursor(target)
					}
				}
			}
		}
		command => command,
	};

	// Every plaintext write path must pass through this guard before the main
	// file-command dispatch. Add future bulk-write commands here as well.
	if matches!(cmd, EditorCommand::WriteFile | EditorCommand::WriteAndQuit)
		&& encrypted_backing_path(session).is_some()
	{
		return write_encrypted_focused(
			registry,
			session,
			None,
			matches!(cmd, EditorCommand::WriteAndQuit),
		);
	}
	if matches!(cmd, EditorCommand::WriteFileAs(_)) && encrypted_backing_path(session).is_some() {
		session.ui.status_message =
			Some("Encrypted document: use :wenc PATH to choose an encrypted target".to_owned());
		return DispatchOutcome::continue_with(true);
	}
	match cmd {
		EditorCommand::LoadEncryptedFile(path) => {
			return load_encrypted_focused(
				runtime,
				tx_internal,
				tx_view,
				reactor,
				resolver,
				lang_registry,
				session,
				path,
			);
		}
		EditorCommand::WriteEncryptedFile => {
			return write_encrypted_focused(registry, session, None, false);
		}
		EditorCommand::WriteEncryptedFileAs(path) => {
			return write_encrypted_focused(registry, session, Some(path), false);
		}
		_ => {}
	}

	if let Some(effect) = handle_browser_command(runtime, session, cmd.clone()) {
		return DispatchOutcome::continue_with(effect.needs_render);
	}
	if session
		.buffers
		.get(focused_doctab(&session.layout, session.focused_pane_id).buffer_id)
		.is_some_and(|buffer| buffer.is_output_panel())
		&& read_only_virtual_blocks_command(&cmd)
	{
		session.ui.status_message = Some("Panel buffers are read-only".to_string());
		reset_global_transients(session);
		return DispatchOutcome::continue_with(true);
	}

	let focused_cursor_count = {
		let view = focused_doctab(&session.layout, session.focused_pane_id);
		session
			.buffers
			.get(view.buffer_id)
			.and_then(|buffer| text_editor(buffer, view))
			.map_or(0, |editor| editor.cursor.len())
	};
	if focused_cursor_count > 1 {
		match multi_cursor_policy(&cmd) {
			MultiCursorPolicy::FanOut | MultiCursorPolicy::Preserve => {}
			MultiCursorPolicy::Collapse => {
				let pane_id = session.focused_pane_id;
				let _ = with_focused_text_buffer_mut(
					registry,
					&mut session.buffers,
					&mut session.layout,
					&mut session.events,
					pane_id,
					|editor| {
						editor.cursor.collapse();
					},
				);
			}
			MultiCursorPolicy::RejectWhileMultiple => {
				session.ui.status_message =
					Some("Command is not supported while multiple cursors are active".to_string());
				return DispatchOutcome::continue_with(true);
			}
		}
	}

	match cmd {
		EditorCommand::GotoDefinition => {
			needs_render |= request_goto_definition(runtime, session, reactor);
		}
		EditorCommand::JumpBack => {
			needs_render |= jump_back(runtime, session);
		}
		EditorCommand::BeginPasswordPrompt { label } => {
			session.input.begin_password_prompt(label);
			needs_render = true;
		}
		EditorCommand::SubmitPassword(secret) => {
			match session.secrets.unlock(secret.into_bytes()) {
				Ok(()) => {
					session.ui.status_message = Some("Encryption session unlocked".to_owned())
				}
				Err(error) => session.ui.status_message = Some(error),
			}
			needs_render = true;
		}
		EditorCommand::LockSecrets => {
			session.secrets.lock();
			session.ui.status_message = Some("Encryption session locked".to_owned());
			needs_render = true;
		}
		EditorCommand::Resize(width, height) => {
			session.update_terminal_size(width, height);
			needs_render = true;
		}
		EditorCommand::OpenHostPanel {
			panel_id,
			kind,
			title,
			split_direction,
			focus,
		} => {
			// Reopening an existing identity never duplicates its host object.
			if let Some(buffer_id) = session.host_panel_buffer_id(panel_id) {
				if focus
					&& let Some(pane_id) = session
						.layout
						.view_id_for_buffer(buffer_id)
						.and_then(|view_id| session.layout.pane_id_for_view(view_id))
				{
					session.focus_pane(pane_id);
				}
			} else {
				let focused_pane_id = session.focused_pane_id;
				let (viewport_width, viewport_lines) = new_doctab_viewport(session);
				let view = session.create_host_panel_view(
					panel_id,
					crate::engine::HostPanelDescriptor { kind, title },
					viewport_width,
					viewport_lines,
				);
				let buffer_id = view.buffer_id;
				let pane = EditorPane::from_view(view);
				if session
					.split_pane_with_new_pane_order(focused_pane_id, split_direction, pane, false)
					.is_none()
				{
					release_buffer(runtime, session, buffer_id);
					session.ui.status_message = Some("No room to open a host panel".to_string());
				} else if !focus {
					session.focus_pane(focused_pane_id);
				}
			}
			needs_render = true;
		}
		EditorCommand::RefreshHostPanel { panel_id, revision } => {
			if let Some(buffer_id) = session.host_panel_buffer_id(panel_id)
				&& let Some(panel) = session
					.buffers
					.get_mut(buffer_id)
					.and_then(|buffer| buffer.host_panel_mut())
			{
				panel.revision = revision;
				session.queue_host_event(crate::engine::HostEvent::PanelRebound {
					panel_id,
					revision,
				});
				needs_render = true;
			}
		}
		EditorCommand::FocusPane(pane_id) => {
			if session.focus_pane(pane_id) {
				reset_global_transients(session);
				needs_render = true;
			}
		}
		EditorCommand::FocusPaneDirection(direction) => {
			if session.focus_pane_in_direction(direction) {
				reset_global_transients(session);
				needs_render = true;
			}
		}
		EditorCommand::ActivateDocTab { pane_id, view_id } => {
			let activated = session
				.layout
				.find_pane_mut(pane_id)
				.is_some_and(|pane| pane.activate_view(view_id));
			if activated {
				session.focus_pane(pane_id);
				reset_global_transients(session);
				needs_render = true;
			}
		}
		EditorCommand::CloseDocTab { pane_id, view_id } => {
			let targeted = session
				.layout
				.find_pane_mut(pane_id)
				.is_some_and(|pane| pane.activate_view(view_id));
			if targeted {
				session.focus_pane(pane_id);
				reset_global_transients(session);
				let active_buffer_id =
					focused_doctab(&session.layout, session.focused_pane_id).buffer_id;
				let closes_last_tab_in_split_pane = session
					.layout
					.find_pane(session.focused_pane_id)
					.is_some_and(|pane| pane.tabs.len() == 1)
					&& session.layout.pane_count() > 1;
				let closes_own_pane = session.buffers.get(active_buffer_id).is_some_and(|buffer| {
					buffer.is_file_browser()
						|| buffer.is_output_panel()
						|| buffer.is_markdown_preview()
				});
				if closes_own_pane || closes_last_tab_in_split_pane {
					close_focused_pane(session, runtime);
				} else {
					close_active_doctab(session, runtime);
				}
				needs_render = true;
			}
		}
		EditorCommand::SplitPane(direction) => {
			if split_target_pane(runtime.registry, session, direction) {
				reset_global_transients(session);
				needs_render = true;
			}
		}
		EditorCommand::NewPane {
			direction,
			language,
		} => {
			let target_pane_id = if session
				.layout
				.find_pane(session.focused_pane_id)
				.is_some_and(|pane| pane.active_tab().is_file_browser())
			{
				select_browser_target_pane(session, session.focused_pane_id)
					.unwrap_or(session.focused_pane_id)
			} else {
				session.focused_pane_id
			};
			let view = session.create_unnamed_text_view(
				runtime.registry,
				crate::core::DEFAULT_VIEWPORT_WIDTH,
				crate::core::DEFAULT_VIEWPORT_LINES,
			);
			if let Ok(view) = view {
				let buffer_id = view.buffer_id;
				let new_pane = EditorPane::from_view(view);
				if session
					.split_pane_with_new_pane_order(target_pane_id, direction, new_pane, false)
					.is_some()
				{
					reset_global_transients(session);
					needs_render = true;
					if let Some(language) = language {
						let EditorSession {
							buffers,
							layout,
							events,
							focused_pane_id,
							ui,
							persistent_settings,
							..
						} = session;
						let view_id = focused_doctab(layout, *focused_pane_id).id;
						if let Some(effect) = with_text_buffer_mut(
							runtime.registry,
							buffers,
							layout,
							events,
							view_id,
							|tab| {
								handle_language_doctab_command(
									runtime.lang_registry,
									tab,
									&mut ui.status_message,
									Some(language),
								)
							},
						) {
							if let Err(err) =
								re_resolve_auto_themes(buffers, layout, persistent_settings)
							{
								ui.status_message = Some(err);
							}
							needs_render |= effect.needs_render;
						}
					}
				} else {
					session.buffers.remove(buffer_id);
				}
			}
		}
		EditorCommand::ClosePane => {
			reset_global_transients(session);
			close_focused_pane(session, runtime);
			needs_render = true;
		}
		EditorCommand::ResizePane(axis, direction) => {
			reset_global_transients(session);
			match session.resize_focused_pane(axis, direction) {
				PaneResizeResult::Resized => {}
				PaneResizeResult::NoMatchingSplit => {
					let axis_name = match axis {
						super::protocol::SplitDirection::Horizontal => "horizontal",
						super::protocol::SplitDirection::Vertical => "vertical",
					};
					session.ui.status_message = Some(format!("No {} split to resize", axis_name));
				}
				PaneResizeResult::AtLimit => {
					session.ui.status_message = Some("Pane cannot be resized further".to_string());
				}
			}
			needs_render = true;
		}
		EditorCommand::ResizePaneBoundary {
			before,
			after,
			axis,
			target,
		} => {
			if session.resize_pane_boundary(before, after, axis, target) {
				needs_render = true;
			}
		}
		EditorCommand::MovePane {
			source,
			target,
			edge,
		} => {
			reset_global_transients(session);
			if session.move_pane(source, target, edge) {
				needs_render = true;
			}
		}
		navigation_cmd @ (EditorCommand::MoveCursor(_)
		| EditorCommand::GotoLine(_)
		| EditorCommand::LineStart
		| EditorCommand::FirstNonWhitespace
		| EditorCommand::LineEnd
		| EditorCommand::SmartHome
		| EditorCommand::AppendAtLineEnd
		| EditorCommand::PageUp
		| EditorCommand::PageDown
		| EditorCommand::SelectInnerWord
		| EditorCommand::SelectAroundFunction
		| EditorCommand::NextFunction
		| EditorCommand::MatchDelimiter
		| EditorCommand::Scroll(_)
		| EditorCommand::ScrollViewport(_)
		| EditorCommand::ScrollViewportToLine(_)
		| EditorCommand::ClickCursor(_)
		| EditorCommand::AddCursorAdjacent(_)
		| EditorCommand::AddCursorAt(_)
		| EditorCommand::CollapseCursors) => {
			if scroll_pinned_preview(session, &navigation_cmd) {
				needs_render = true;
			} else {
				let redirect_target = if matches!(
					navigation_cmd,
					EditorCommand::Scroll(_)
						| EditorCommand::ScrollViewport(_)
						| EditorCommand::ScrollViewportToLine(_)
						| EditorCommand::PageUp
						| EditorCommand::PageDown
				) {
					markdown_preview_source_target(session)
				} else {
					None
				};
				{
					let EditorSession {
						buffers,
						layout,
						events,
						focused_pane_id,
						ui,
						..
					} = session;
					let view_id = redirect_target
						.unwrap_or_else(|| focused_doctab(layout, *focused_pane_id).id);
					let effect = with_file_browser_buffer_mut(buffers, layout, view_id, |tab| {
						handle_file_browser_navigation_command(
							registry,
							tab,
							ui,
							navigation_cmd.clone(),
						)
					})
					.or_else(|| {
						with_text_buffer_mut(registry, buffers, layout, events, view_id, |tab| {
							handle_navigation_doctab_command(
								registry,
								tab,
								ui,
								navigation_cmd.clone(),
							)
						})
					});
					if let Some(effect) = effect {
						needs_render |= effect.needs_render;
					}
				}
				sync_diff_scroll(registry, session, &navigation_cmd);
				sync_markdown_image_preview_scroll(registry, session);
			}
		}
		edit_cmd @ (EditorCommand::SetVisualSelection { .. }
		| EditorCommand::ClearVisualSelection
		| EditorCommand::VisualYank { .. }
		| EditorCommand::VisualDelete { .. }
		| EditorCommand::VisualChange { .. }
		| EditorCommand::DeleteInnerWord
		| EditorCommand::ChangeInnerWord
		| EditorCommand::DeleteWordMotion { .. }
		| EditorCommand::ChangeWordMotion { .. }
		| EditorCommand::YankWordMotion { .. }
		| EditorCommand::JoinLine) => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				clipboard,
				ui,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| handle_edit_doctab_command(registry, tab, clipboard, ui, edit_cmd),
			) {
				needs_render |= effect.needs_render;
			}
		}
		edit_cmd @ (EditorCommand::DeleteAroundFunction { .. }
		| EditorCommand::ChangeAroundFunction
		| EditorCommand::DeleteToLineEnd
		| EditorCommand::DeleteLine { .. }
		| EditorCommand::InsertChar(_)
		| EditorCommand::InsertText(_)
		| EditorCommand::ReplaceChar(_)
		| EditorCommand::ToggleCaseAtCursor
		| EditorCommand::OpenLineBelow
		| EditorCommand::OpenLineAbove
		| EditorCommand::Backspace
		| EditorCommand::Delete
		| EditorCommand::Put { .. }) => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				clipboard,
				ui,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| handle_edit_doctab_command(registry, tab, clipboard, ui, edit_cmd),
			) {
				needs_render |= effect.needs_render;
			}
		}
		fold_cmd @ (EditorCommand::ToggleFold
		| EditorCommand::CloseFold
		| EditorCommand::OpenFold
		| EditorCommand::CloseAllFolds
		| EditorCommand::OpenAllFolds) => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| handle_fold_doctab_command(runtime.registry, tab, fold_cmd),
			) {
				needs_render |= effect.needs_render;
			}
		}
		view_cmd @ (EditorCommand::ToggleWrap
		| EditorCommand::SetWrap(_)
		| EditorCommand::SetMinimap(_)
		| EditorCommand::SetLineNumbers(_)
		| EditorCommand::SetRelativeLineNumbers(_)
		| EditorCommand::SetVisibleWhitespace(_)
		| EditorCommand::SetTheme(_)
		| EditorCommand::SetBackground(_)) => {
			let target_view_id = if matches!(
				&view_cmd,
				EditorCommand::SetTheme(_) | EditorCommand::SetBackground(_)
			) {
				markdown_preview_source_target(session)
			} else {
				None
			};
			let EditorSession {
				layout,
				focused_pane_id,
				ui,
				..
			} = session;
			let tab = if let Some(view_id) = target_view_id {
				layout
					.find_view_mut(view_id)
					.expect("Markdown preview source view must exist")
			} else {
				focused_pane_mut(layout, *focused_pane_id).active_tab_mut()
			};
			let effect = handle_view_doctab_command(tab, ui, view_cmd);
			needs_render |= effect.needs_render;
		}
		view_cmd @ (EditorCommand::PersistWrap(_)
		| EditorCommand::PersistMinimap(_)
		| EditorCommand::PersistLineNumbers(_)
		| EditorCommand::PersistRelativeLineNumbers(_)
		| EditorCommand::PersistVisibleWhitespace(_)
		| EditorCommand::PersistReload(_)
		| EditorCommand::PersistSaveDurability(_)
		| EditorCommand::PersistTheme(_)
		| EditorCommand::PersistLangTheme { .. }
		| EditorCommand::PersistNoLangTheme(_)
		| EditorCommand::PersistUiColor { .. }
		| EditorCommand::QueryUiColor(_)) => {
			let init_edit = system::init_setting_edit(&view_cmd, lang_registry);
			let should_re_resolve_themes = matches!(
				&view_cmd,
				EditorCommand::PersistTheme(_)
					| EditorCommand::PersistLangTheme { .. }
					| EditorCommand::PersistNoLangTheme(_)
			);
			let changes_editor_background = matches!(
				&view_cmd,
				EditorCommand::PersistUiColor {
					key: UiColorKey::EditorBackground,
					..
				}
			);
			let target_view_id = if matches!(&view_cmd, EditorCommand::PersistTheme(_))
				|| changes_editor_background
			{
				markdown_preview_source_target(session)
			} else {
				None
			};
			let EditorSession {
				buffers,
				layout,
				focused_pane_id,
				ui,
				persistent_settings,
				..
			} = session;
			let tab = if let Some(view_id) = target_view_id {
				layout
					.find_view_mut(view_id)
					.expect("Markdown preview source view must exist")
			} else {
				focused_pane_mut(layout, *focused_pane_id).active_tab_mut()
			};
			let effect =
				handle_persist_view_doctab_command(runtime, tab, ui, persistent_settings, view_cmd);
			if should_re_resolve_themes
				&& let Err(err) = re_resolve_auto_themes(buffers, layout, persistent_settings)
			{
				ui.status_message = Some(err);
			}
			if let Some(edit) = init_edit {
				append_init_persistence_status(runtime, session, &edit);
			}
			needs_render |= effect.needs_render;
		}
		EditorCommand::LoadFile(path) => {
			let pane_id = session.focused_pane_id;
			needs_render |= load_file_into_pane(runtime, session, pane_id, path)
				.effect
				.needs_render;
			let view = focused_doctab(&session.layout, session.focused_pane_id);
			if let Some(document) = session
				.buffers
				.get_mut(view.buffer_id)
				.and_then(|buffer| buffer.text_mut())
			{
				document.encrypted_backing = None;
			}
		}
		replace_cmd @ EditorCommand::ReplaceDocument { .. } => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				ui,
				next_dynamic_device_id,
				persistent_settings,
				..
			} = session;
			let doctab = focused_doctab(layout, *focused_pane_id);
			let view_id = doctab.id;
			let buffer_id = doctab.buffer_id;
			if let Some((effect, language_id, replaced)) =
				with_text_buffer_mut(registry, buffers, layout, events, view_id, |tab| {
					// Any ordinary replacement discards encrypted-document identity.
					// `:eenc` reinstates it only after its decrypted replacement has
					// completed successfully.
					tab.document.encrypted_backing = None;
					let effect = handle_file_doctab_command(
						runtime,
						tab,
						ui,
						next_dynamic_device_id,
						persistent_settings.save_durability,
						replace_cmd,
					);
					(
						effect,
						tab.document.active_language_id,
						tab.document.file_path.is_none(),
					)
				}) {
				if replaced {
					// The buffer no longer represents its former file, so its
					// canonical-path claim must not block reopening that file.
					buffers.detach_path(buffer_id);
				}
				let view = layout.find_view_mut(view_id).expect("replaced view");
				if let Err(err) =
					apply_resolved_theme_to_tab(view, language_id, persistent_settings)
				{
					ui.status_message = Some(err);
				}
				needs_render |= effect.needs_render;
			}
		}
		query_cmd @ EditorCommand::QueryDocumentText => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				ui,
				next_dynamic_device_id,
				persistent_settings,
				..
			} = session;
			let doctab = focused_doctab(layout, *focused_pane_id);
			let view_id = doctab.id;
			let buffer_id = doctab.buffer_id;
			if let Some((effect, _)) = handle_file_command_for_view(
				runtime,
				buffers,
				layout,
				events,
				view_id,
				ui,
				next_dynamic_device_id,
				persistent_settings.save_durability,
				query_cmd,
			) {
				if ui.document_text_response.is_some() {
					ui.document_text_response_buffer_id = Some(buffer_id);
				}
				needs_render |= effect.needs_render;
			}
		}
		EditorCommand::ReloadFile { force } => {
			let focused_buffer_id =
				focused_doctab(&session.layout, session.focused_pane_id).buffer_id;
			if !force
				&& session
					.buffers
					.get(focused_buffer_id)
					.is_some_and(|buffer| buffer.is_dirty())
			{
				session.pending_reload = Some(PendingReload {
					pane_id: session.focused_pane_id,
				});
				session.ui.mode_override = Some(EditorMode::Confirm);
				session.ui.status_message = Some("Reload and discard changes? [y/n]".to_string());
				needs_render = true;
			} else {
				let EditorSession {
					buffers,
					layout,
					events,
					focused_pane_id,
					ui,
					next_dynamic_device_id,
					persistent_settings,
					..
				} = session;
				let view_id = focused_doctab(layout, *focused_pane_id).id;
				if let Some((effect, language_id)) = handle_file_command_for_view(
					runtime,
					buffers,
					layout,
					events,
					view_id,
					ui,
					next_dynamic_device_id,
					persistent_settings.save_durability,
					EditorCommand::ReloadFile { force },
				) {
					let view = layout.find_view_mut(view_id).expect("reloaded view");
					if let Err(err) =
						apply_resolved_theme_to_tab(view, language_id, persistent_settings)
					{
						ui.status_message = Some(err);
					}
					needs_render |= effect.needs_render;
				}
			}
		}
		EditorCommand::DiffSplit(path) => {
			needs_render = handle_diff_split(runtime, session, path);
		}
		EditorCommand::DiffStep(delta) => {
			needs_render = handle_diff_step(session, delta);
		}
		EditorCommand::MarkdownPreview { serif } => {
			match open_markdown_image_preview(registry, session, serif) {
				Ok(()) => {}
				Err(err) => {
					session.ui.status_message = Some(err);
				}
			}
			needs_render = true;
		}
		EditorCommand::OpenUastInspector { mode } => {
			if let Err(error) =
				super::uast_inspector::open_uast_inspector(registry, session, mode)
			{
				session.ui.status_message = Some(error);
			}
			needs_render = true;
		}
		EditorCommand::ActivateUastInspectorRow => {
			if let Err(error) =
				super::uast_inspector::activate_selected_row(registry, session)
			{
				session.ui.status_message = Some(error);
			}
			needs_render = true;
		}
		EditorCommand::ToggleUastInspectorFold => {
			if let Err(error) = super::uast_inspector::toggle_selected_fold(session) {
				session.ui.status_message = Some(error);
			}
			needs_render = true;
		}
		EditorCommand::ToggleUastInspectorFollowCursor => {
			if let Err(error) = super::uast_inspector::toggle_follow_cursor(session) {
				session.ui.status_message = Some(error);
			}
			needs_render = true;
		}
		EditorCommand::ActivateUastGraphNode {
			inspector_view_id,
			selection,
		} => {
			if let Err(error) = super::uast_inspector::activate_graph_node(
				registry,
				session,
				inspector_view_id,
				selection,
			) {
				session.ui.status_message = Some(error);
			}
			needs_render = true;
		}
		EditorCommand::ExportUastDot { path } => {
			if let Err(error) = super::uast_inspector::export_dot(
				registry,
				lang_registry,
				session,
				path.as_deref(),
			) {
				session.ui.status_message = Some(error);
			}
			needs_render = true;
		}
		EditorCommand::ShowMarkdownDocument { title, markdown } => {
			match open_static_markdown_preview(session, title, markdown) {
				Ok(()) => {}
				Err(err) => {
					session.ui.status_message = Some(err);
				}
			}
			needs_render = true;
		}
		file_cmd @ (EditorCommand::WriteFile
		| EditorCommand::WriteFileAs(_)
		| EditorCommand::WriteAndQuit) => {
			let view_id = focused_doctab(&session.layout, session.focused_pane_id).id;
			let buffer_id = focused_doctab(&session.layout, session.focused_pane_id).buffer_id;
			let write_as_path = match &file_cmd {
				EditorCommand::WriteFileAs(path) => Some(path.clone()),
				EditorCommand::WriteFile | EditorCommand::WriteAndQuit => None,
				_ => unreachable!(),
			};
			let reservation = write_as_path
				.as_deref()
				.map(CanonicalPath::resolve)
				.transpose()
				.and_then(|path| {
					path.map_or(Ok(None), |path| {
						session.buffers.reserve_path(buffer_id, path).map(Some)
					})
				});
			match reservation {
				Err(err) => {
					session.ui.status_message = Some(err);
					needs_render = true;
				}
				Ok(path_reservation) => {
					session.ui.busy = true;
					if render_frame(RenderParams {
						registry,
						resolver,
						tx_view,
						reactor,
						lang_registry,
						session,
					}) {
						return DispatchOutcome::stop();
					}
					let EditorSession {
						buffers,
						layout,
						events,
						ui,
						next_dynamic_device_id,
						persistent_settings,
						snapshot_partners,
						..
					} = session;
					if let Some((effect, _language_id)) = handle_file_command_for_view(
						runtime,
						buffers,
						layout,
						events,
						view_id,
						ui,
						next_dynamic_device_id,
						persistent_settings.save_durability,
						file_cmd,
					) {
						if let (Some(reservation), Some(path)) =
							(path_reservation, write_as_path.as_deref())
							&& buffers
								.get(buffer_id)
								.and_then(|buffer| buffer.text())
								.and_then(|document| document.file_path.as_deref())
								== Some(path)
						{
							buffers.commit_path(reservation);
							// A snapshot written to a real path is an
							// ordinary file document now; its provenance
							// identity must not outlive the conversion.
							snapshot_partners.remove(&buffer_id);
						}
						if write_as_path.is_some()
							&& let Err(err) =
								re_resolve_auto_themes(buffers, layout, persistent_settings)
						{
							ui.status_message = Some(err);
						}
						needs_render |= effect.needs_render;
					}
					ui.busy = false;
				}
			}
		}
		EditorCommand::ConfirmResponse(action) if session.pending_reload.is_some() => {
			let pending = session
				.pending_reload
				.take()
				.expect("pending reload checked above");
			session.ui.mode_override = Some(EditorMode::Normal);
			match action {
				ConfirmAction::Yes | ConfirmAction::All => {
					let EditorSession {
						buffers,
						layout,
						events,
						ui,
						next_dynamic_device_id,
						persistent_settings,
						..
					} = session;
					if let Some(view_id) = layout
						.find_pane(pending.pane_id)
						.map(|pane| pane.active_tab().id)
					{
						let (effect, language_id) = handle_file_command_for_view(
							runtime,
							buffers,
							layout,
							events,
							view_id,
							ui,
							next_dynamic_device_id,
							persistent_settings.save_durability,
							EditorCommand::ReloadFile { force: true },
						)
						.expect("reload target must be text");
						let view = layout.find_view_mut(view_id).expect("reload target view");
						if let Err(err) =
							apply_resolved_theme_to_tab(view, language_id, persistent_settings)
						{
							ui.status_message = Some(err);
						}
						needs_render |= effect.needs_render;
					} else {
						ui.status_message = Some("Reload target pane no longer exists".to_string());
						needs_render = true;
					}
				}
				ConfirmAction::No | ConfirmAction::Quit => {
					session.ui.status_message = Some("Reload canceled".to_string());
					needs_render = true;
				}
			}
		}
		search_cmd @ (EditorCommand::SearchStart(_)
		| EditorCommand::SearchNext
		| EditorCommand::SearchPrev
		| EditorCommand::ClearSearch
		| EditorCommand::SearchWordUnderCursor { .. }
		| EditorCommand::SubstituteAll { .. }
		| EditorCommand::SubstituteConfirm { .. }
		| EditorCommand::ConfirmResponse(_)) => {
			let starts_new_search = matches!(
				&search_cmd,
				EditorCommand::SearchStart(_)
					| EditorCommand::ClearSearch
					| EditorCommand::SearchWordUnderCursor { .. }
					| EditorCommand::SubstituteAll { .. }
					| EditorCommand::SubstituteConfirm { .. }
			);
			if starts_new_search {
				session.search_pattern_revision = session.search_pattern_revision.wrapping_add(1);
				session
					.layout
					.for_each_doctab_mut(&mut |view| view.reset_minimap_cache());
			}
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				search_pattern,
				search_case_insensitive,
				search_pattern_revision,
				search_confirm,
				ui,
				next_dynamic_device_id,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| {
					tab.document
						.sync_search_pattern_revision(*search_pattern_revision);
					let modifies_results = !matches!(
						&search_cmd,
						EditorCommand::SearchNext | EditorCommand::SearchPrev
					);
					let effect = handle_search_doctab_command(
						runtime,
						tab,
						search_pattern,
						search_case_insensitive,
						search_confirm,
						ui,
						next_dynamic_device_id,
						search_cmd,
					);
					if modifies_results {
						tab.document.bump_search_revision();
					}
					tab.view.common.reset_minimap_cache();
					effect
				},
			) {
				needs_render |= effect.needs_render;
			}
		}
		clipboard_cmd @ EditorCommand::YankLine { .. } => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				clipboard,
				ui,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| {
					handle_clipboard_doctab_command(
						registry,
						tx_internal,
						tab,
						clipboard,
						ui,
						clipboard_cmd,
					)
				},
			) {
				needs_render |= effect.needs_render;
			}
		}
		history_cmd @ (EditorCommand::Undo | EditorCommand::Redo) => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				ui,
				next_dynamic_device_id,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| {
					handle_history_doctab_command(
						runtime,
						tab,
						ui,
						next_dynamic_device_id,
						history_cmd,
					)
				},
			) {
				needs_render |= effect.needs_render;
			}
		}
		EditorCommand::SetLanguage(lang_id) => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				ui,
				persistent_settings,
				..
			} = session;
			let view_id = focused_doctab(layout, *focused_pane_id).id;
			if let Some(effect) =
				with_text_buffer_mut(registry, buffers, layout, events, view_id, |tab| {
					handle_language_doctab_command(
						runtime.lang_registry,
						tab,
						&mut ui.status_message,
						lang_id,
					)
				}) {
				if let Err(err) = re_resolve_auto_themes(buffers, layout, persistent_settings) {
					ui.status_message = Some(err);
				}
				needs_render |= effect.needs_render;
			}
		}
		EditorCommand::Sort {
			range,
			reverse,
			unique,
			numeric,
			ignore_case,
		} => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				ui,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| {
					handle_sort_doctab_command(
						registry,
						tab,
						ui,
						range,
						reverse,
						unique,
						numeric,
						ignore_case,
					)
				},
			) {
				needs_render |= effect.needs_render;
			}
		}
		EditorCommand::Uniq { range, ignore_case } => {
			let EditorSession {
				buffers,
				layout,
				events,
				focused_pane_id,
				ui,
				..
			} = session;
			if let Some(effect) = with_focused_text_buffer_mut(
				registry,
				buffers,
				layout,
				events,
				*focused_pane_id,
				|tab| handle_uniq_doctab_command(registry, tab, ui, range, ignore_case),
			) {
				needs_render |= effect.needs_render;
			}
		}
		EditorCommand::DocTabNew => {
			if let Err(err) = spawn_empty_doctab(registry, session) {
				session.ui.status_message = Some(err);
			}
			needs_render = true;
		}
		EditorCommand::DocTabEdit(path) => {
			let effect = spawn_file_doctab(runtime, session, path);
			needs_render |= effect.needs_render;
		}
		EditorCommand::DocTabNext => {
			cycle_active_doctab(session, true);
			needs_render = true;
		}
		EditorCommand::DocTabPrev => {
			cycle_active_doctab(session, false);
			needs_render = true;
		}
		EditorCommand::DocTabClose => {
			close_active_doctab(session, runtime);
			needs_render = true;
		}
		EditorCommand::CheckFileReload => {
			if let Some(path) = focused_git_path(session) {
				let _ = session.git_branch_cache.refresh_branch_for_path(&path);
			} else {
				let _ = session.git_branch_cache.refresh_branch_for_current_dir();
			}
			if session.persistent_settings.reload_enabled {
				let EditorSession {
					buffers,
					layout,
					events,
					ui,
					next_dynamic_device_id,
					persistent_settings,
					..
				} = session;
				let mut view_ids = Vec::new();
				layout.collect_view_ids(&mut view_ids);
				for view_id in view_ids {
					let Some((effect, language_id)) =
						with_text_buffer_mut(registry, buffers, layout, events, view_id, |tab| {
							let effect = handle_auto_reload_doctab_command(
								runtime,
								tab,
								ui,
								next_dynamic_device_id,
							);
							(effect, tab.document.active_language_id)
						})
					else {
						continue;
					};
					if effect.needs_render {
						let view = layout.find_view_mut(view_id).expect("auto-reload view");
						if let Err(err) =
							apply_resolved_theme_to_tab(view, language_id, persistent_settings)
						{
							ui.status_message = Some(err);
						}
					}
					needs_render |= effect.needs_render;
				}
			}
		}
		EditorCommand::SetGitRefresh(mode) => {
			session.git_refresh_mode = mode;
			session.ui.status_message = Some(match mode {
				super::protocol::GitRefreshMode::Disabled => "Git refresh disabled".to_string(),
				super::protocol::GitRefreshMode::Watch => {
					"Git refresh set to watch-preferred mode".to_string()
				}
				super::protocol::GitRefreshMode::Interval(seconds) => {
					format!("Git refresh set to observation + {seconds}s polling")
				}
			});
			if let Some(edit) =
				system::init_setting_edit(&EditorCommand::SetGitRefresh(mode), lang_registry)
			{
				append_init_persistence_status(runtime, session, &edit);
			}
			needs_render = true;
		}
		command @ (EditorCommand::GitStatus | EditorCommand::RefreshGitStatus) => {
			let automatic_refresh = matches!(command, EditorCommand::RefreshGitStatus);
			let buffer_id = if automatic_refresh {
				let Some(buffer_id) = session.output_panels.get("git-status").copied() else {
					return DispatchOutcome::continue_with(false);
				};
				if session.layout.view_id_for_buffer(buffer_id).is_none() {
					return DispatchOutcome::continue_with(false);
				}
				buffer_id
			} else {
				match session.ensure_output_panel(registry, "git-status") {
					Ok((buffer_id, _)) => buffer_id,
					Err(error) => {
						session.ui.status_message = Some(error);
						return DispatchOutcome::continue_with(true);
					}
				}
			};
			if session.git_status_job.is_some() {
				// Observer bursts and polling ticks may arrive while Git is
				// still running. Preserve one trailing refresh rather than
				// dropping the newest repository state.
				session.git_status_refresh_pending = true;
			} else {
				let focused_buffer_id =
					focused_doctab(&session.layout, session.focused_pane_id).buffer_id;
				let repo_root = if automatic_refresh || focused_buffer_id == buffer_id {
					session.git_status_repo_root.clone()
				} else {
					focused_git_repo_root(session)
				};
				if let Some(repo_root) = repo_root {
					session.git_status_repo_root = Some(repo_root.clone());
					session.git_status_refresh_pending = false;
					if let Err(error) = start_git_status_job(session, buffer_id, repo_root) {
						session.ui.status_message = Some(format!("Git status: {error}"));
					}
				} else {
					session.git_status_refresh_pending = false;
					session.git_status_branch = None;
					session.git_status_repo_root = None;
					if let Err(error) = session.replace_output_panel(
						registry,
						buffer_id,
						b"Not inside a Git repository\n",
						Vec::new(),
					) {
						session.ui.status_message = Some(error);
					}
				}
			}
			needs_render = true;
		}
		EditorCommand::OpenGitStatusEntry(line) => {
			match git_status_entry_path(registry, session, line) {
				Ok(path) => {
					let source_pane_id = session.focused_pane_id;
					let Some(target_pane_id) =
						select_git_status_target_pane(session, source_pane_id)
					else {
						session.ui.status_message =
							Some("Unable to find an editor pane for Git status entry".to_string());
						needs_render = true;
						return DispatchOutcome::continue_with(needs_render);
					};
					needs_render |= load_file_into_pane(
						runtime,
						session,
						target_pane_id,
						path.to_string_lossy().into_owned(),
					)
					.effect
					.needs_render;
				}
				Err(error) => {
					session.ui.status_message = Some(error);
					needs_render = true;
				}
			}
		}
		EditorCommand::OpenFileBrowser
		| EditorCommand::OpenFileBrowserAt(_)
		| EditorCommand::RefreshFileBrowser
		| EditorCommand::ActivateBrowserEntry
		| EditorCommand::ClickBrowserEntry { .. }
		| EditorCommand::CollapseBrowserEntryOrParent => {
			unreachable!("browser commands are handled before the main command match");
		}
		EditorCommand::MouseIntent { .. } => {
			unreachable!("mouse intents are resolved before command dispatch");
		}
		EditorCommand::SourceRune(_) | EditorCommand::UnloadRune(_) => {
			unreachable!("Rune lifecycle commands are handled by the engine loop");
		}
		EditorCommand::WriteInit { .. } => {
			unreachable!("init export is handled before the main command match");
		}
		EditorCommand::WriteDefaultInit { .. } => {
			unreachable!("init reset is handled before the main command match");
		}
		EditorCommand::LoadEncryptedFile(_)
		| EditorCommand::WriteEncryptedFile
		| EditorCommand::WriteEncryptedFileAs(_) => {
			unreachable!("encrypted file commands are handled before the main command match");
		}
		EditorCommand::Quit => {
			// Frontends need one final projection to hand the quit request to
			// their native window lifecycle before this engine thread exits.
			session.ui.pending_quit = true;
			return DispatchOutcome::stop();
		}
	}

	DispatchOutcome::continue_with(needs_render)
}

fn encrypted_backing_path(session: &EditorSession) -> Option<String> {
	let view = focused_doctab(&session.layout, session.focused_pane_id);
	session
		.buffers
		.get(view.buffer_id)
		.and_then(|buffer| buffer.text())
		.and_then(|document| document.encrypted_backing.clone())
}

fn write_encrypted_focused(
	registry: &UastRegistry,
	session: &mut EditorSession,
	requested_path: Option<String>,
	quit_after_write: bool,
) -> DispatchOutcome {
	let view = focused_doctab(&session.layout, session.focused_pane_id);
	let Some(document) = session
		.buffers
		.get(view.buffer_id)
		.and_then(|buffer| buffer.text())
	else {
		session.ui.status_message = Some("No document to encrypt".to_owned());
		return DispatchOutcome::continue_with(true);
	};
	let path = requested_path.or_else(|| document.encrypted_backing.clone());
	let Some(path) = path else {
		session.ui.status_message = Some("No encrypted file name; use :wenc PATH".to_owned());
		return DispatchOutcome::continue_with(true);
	};
	let snapshot = match read_document_snapshot(registry, document.root_id, None) {
		Ok(snapshot) => snapshot,
		Err(error) => {
			session.ui.status_message = Some(error);
			return DispatchOutcome::continue_with(true);
		}
	};
	match session.secrets.write(Path::new(&path), &snapshot) {
		Ok(()) => {
			if let Some(text) = session
				.buffers
				.get_mut(view.buffer_id)
				.and_then(|buffer| buffer.text_mut())
			{
				text.encrypted_backing = Some(path.clone());
				text.ledger.mark_saved();
			}
			session.ui.status_message = Some(format!("\"{}\" encrypted", path));
			if quit_after_write {
				session.ui.pending_quit = true;
			}
		}
		Err(error) => session.ui.status_message = Some(error),
	}
	DispatchOutcome::continue_with(true)
}

#[allow(clippy::too_many_arguments)]
fn load_encrypted_focused(
	runtime: &RuntimeContext<'_>,
	tx_internal: &super::request::InternalSender,
	tx_view: &std::sync::mpsc::Sender<crate::uast::Viewport>,
	reactor: &crate::svp::semantic::SemanticReactor,
	resolver: &std::sync::Arc<SvpResolver>,
	lang_registry: &std::sync::Arc<super::language::LanguageRegistry>,
	session: &mut EditorSession,
	path: String,
) -> DispatchOutcome {
	let encrypted = match std::fs::read(&path) {
		Ok(contents) => contents,
		Err(error) => {
			session.ui.status_message = Some(format!("Could not read encrypted file: {error}"));
			return DispatchOutcome::continue_with(true);
		}
	};
	let mut plaintext = match session.secrets.open(&encrypted) {
		Ok(contents) => contents,
		Err(error) => {
			session.ui.status_message = Some(error);
			return DispatchOutcome::continue_with(true);
		}
	};
	let text = match String::from_utf8(std::mem::take(&mut *plaintext)) {
		Ok(text) => text,
		Err(_) => {
			session.ui.status_message = Some("Encrypted document is not valid UTF-8".to_owned());
			return DispatchOutcome::continue_with(true);
		}
	};
	let virtual_path = path.strip_suffix(".enc").unwrap_or(&path).to_owned();
	let language = Path::new(&virtual_path)
		.extension()
		.and_then(|extension| extension.to_str())
		.map(str::to_owned);
	let outcome = dispatch_command(
		DispatchContext {
			runtime,
			tx_internal,
			tx_view,
			reactor,
			resolver,
			lang_registry,
		},
		session,
		EditorCommand::ReplaceDocument {
			text,
			language,
			virtual_path: Some(virtual_path),
		},
	);
	if !outcome.should_stop {
		let view = focused_doctab(&session.layout, session.focused_pane_id);
		if let Some(document) = session
			.buffers
			.get_mut(view.buffer_id)
			.and_then(|buffer| buffer.text_mut())
		{
			document.encrypted_backing = Some(path.clone());
			document.ledger.mark_saved();
		}
		session.ui.status_message = Some(format!("\"{}\" decrypted", path));
	}
	outcome
}

pub(crate) fn run_engine(engine: Engine) {
	let registry = engine.registry;
	let resolver = engine.resolver;
	let rx_request = engine.rx_request;
	let tx_internal = engine.tx_internal;
	let tx_view = engine.tx_view;
	let host_events = engine.host_events;
	let reactor = engine.reactor;
	let lang_registry = engine.lang_registry;
	let initial_theme_name = engine.initial_theme_name;
	let initial_minimap_enabled = engine.initial_minimap_enabled;
	let initial_wrap_enabled = engine.initial_wrap_enabled;
	let initial_line_numbers_enabled = engine.initial_line_numbers_enabled;
	let initial_relative_line_numbers_enabled = engine.initial_relative_line_numbers_enabled;
	let initial_visible_whitespace = engine.initial_visible_whitespace;
	let initial_reload_enabled = engine.initial_reload_enabled;
	let initial_save_durability = engine.initial_save_durability;
	let initial_ui_colors = engine.initial_ui_colors;
	let initial_language_themes = engine.initial_language_themes;
	let initial_file = engine.initial_file;
	let initial_viewport = engine.initial_viewport;
	let rune_host_config = engine.rune_host;
	let configured_init_path = rune_host_config
		.as_ref()
		.map(|config| config.init_script().to_path_buf());
	let configured_init_entrypoint = rune_host_config
		.as_ref()
		.map(|config| config.init_entrypoint().to_string());
	let terminate = engine.terminate;
	let mut dynamic_action_executor = engine.dynamic_action_executor;
	let mut subscriber_dispatcher = engine.subscriber_dispatcher;
	let mut job_dispatcher = engine.job_dispatcher;
	#[cfg(test)]
	let event_tap = engine.event_tap;
	#[cfg(test)]
	let subscriber_setup = engine.subscriber_setup;
	#[cfg(not(test))]
	let event_tap: Option<mpsc::Sender<EngineEvent>> = None;
	let dynamic_invocation_gate = InvocationGate::default();
	let runtime = RuntimeContext {
		registry: &registry,
		resolver: &resolver,
		lang_registry: &lang_registry,
		init_script: configured_init_path.as_deref(),
		init_entrypoint: configured_init_entrypoint.as_deref(),
	};
	let services = Arc::new(Mutex::new(HostServices::default()));
	let mut session = EditorSession::new(
		&registry,
		initial_theme_name,
		initial_minimap_enabled,
		initial_wrap_enabled,
		initial_line_numbers_enabled,
		initial_relative_line_numbers_enabled,
		initial_visible_whitespace,
		initial_reload_enabled,
		initial_save_durability,
		initial_ui_colors,
		initial_language_themes,
		None,
	);
	if let Some((width, height)) = initial_viewport {
		session.update_terminal_size(width.max(1), height.max(1));
	}
	#[cfg(test)]
	session.jobs.attach_internal_sender(tx_internal.clone());
	#[cfg(test)]
	if let Some(setup) = subscriber_setup {
		setup(&mut session);
	}
	let mut session = EditorHost::new(
		session,
		services.clone(),
		registry.clone(),
		resolver.clone(),
	);
	session.set_language_registry(lang_registry.clone());
	#[cfg(not(test))]
	session.input.load_persistent_history();
	session.input.attach_internal_sender(tx_internal.clone());
	#[cfg(not(test))]
	session.jobs.attach_internal_sender(tx_internal.clone());
	session
		.input
		.set_language_completion_candidates(lang_registry.completion_candidates());
	let mut init_report = StartupScriptReport::default();
	let mut rune_host = match rune_host_config {
		Some(config) => match RuneHost::new(config, services, tx_internal.clone()) {
			Ok(mut host) => {
				init_report = host.run_init(&mut session);
				Some(host)
			}
			Err(error) => {
				init_report.error = Some(error);
				None
			}
		},
		None => None,
	};
	let pane_id = session.focused_pane_id;
	let mut initial_file_loaded = true;
	if let Some(path) = initial_file {
		let lifecycle_before = session.capture_text_lifecycle();
		session.input.record_startup_file(&path);
		initial_file_loaded = load_file_into_pane(&runtime, &mut session, pane_id, path).succeeded;
		session.emit_text_lifecycle_since(lifecycle_before);
	}
	if initial_file_loaded && let Some(status) = init_report.foreground_status() {
		session.ui.status_message = Some(status);
	}
	let startup_subscriber_render = drain_run_loop_deliveries(
		&mut session,
		rune_host.as_mut(),
		subscriber_dispatcher.as_mut(),
		job_dispatcher.as_mut(),
		&tx_internal,
		&dynamic_invocation_gate,
		event_tap.as_ref(),
	);
	if initial_file_loaded
		&& (init_report.needs_render || startup_subscriber_render)
		&& render_frame(RenderParams {
			registry: &registry,
			resolver: &resolver,
			tx_view: &tx_view,
			reactor: &reactor,
			lang_registry: &lang_registry,
			session: &mut session,
		}) {
		return;
	}
	let mut git_refresh = GitRefreshController::new(tx_internal.clone());
	loop {
		if terminate.load(std::sync::atomic::Ordering::Acquire) {
			break;
		}
		git_refresh.reconcile(
			open_git_status_repo_root(&session),
			session.git_refresh_mode,
		);
		if git_refresh.queue_poll_if_due(Instant::now()).is_err() {
			break;
		}
		let request = match git_refresh.poll_deadline() {
			Some(deadline) => {
				match rx_request.recv_timeout(deadline.saturating_duration_since(Instant::now())) {
					Ok(request) => request,
					Err(mpsc::RecvTimeoutError::Timeout) => continue,
					Err(mpsc::RecvTimeoutError::Disconnected) => break,
				}
			}
			None => match rx_request.recv() {
				Ok(request) => request,
				Err(_) => break,
			},
		};
		let input_dispatch = match request {
			EngineRequest::Command(command) => InputDispatch {
				commands: vec![command],
				..InputDispatch::default()
			},
			EngineRequest::Input(event) => {
				let input_context = current_input_context(&registry, &session);
				match event {
					super::InputEvent::Key { code, modifiers } => {
						let EditorSession {
							input,
							runtime_registries,
							..
						} = &mut session.session;
						input.dispatch_key_with_registries(
							input_context,
							code,
							modifiers,
							runtime_registries,
						)
					}
					super::InputEvent::Paste(data) => {
						session.input.dispatch_paste(input_context, data)
					}
				}
			}
			EngineRequest::Internal(InternalEvent::RefreshGitStatus) => InputDispatch {
				// Observation and timer events may already be queued when the
				// pane closes or refresh is disabled. Recheck authoritative
				// session state before turning the event into a command.
				commands: (session.git_refresh_mode.watch_enabled()
					&& open_git_status_repo_root(&session).is_some())
				.then_some(EditorCommand::RefreshGitStatus)
				.into_iter()
				.collect(),
				..InputDispatch::default()
			},
			EngineRequest::Internal(event) => {
				let mut needs_render = match event {
					InternalEvent::Refresh => true,
					InternalEvent::DrainEvents => false,
					InternalEvent::GitObservationChanged => false,
					InternalEvent::RefreshGitStatus => {
						unreachable!("Git refresh events enter the command path")
					}
					InternalEvent::HydrationCompleted(buffer) => {
						handle_hydration_completed(&mut session, buffer)
					}
					InternalEvent::ClearYankFlash => {
						let view = focused_doctab(&session.layout, session.focused_pane_id);
						if session
							.buffers
							.get(view.buffer_id)
							.is_some_and(|buffer| buffer.text().is_some())
						{
							session.ui.yank_flash = None;
							true
						} else {
							false
						}
					}
					InternalEvent::PathCompletion(response) => {
						let EditorSession {
							input,
							runtime_registries,
							..
						} = &mut session.session;
						match input.apply_path_completion(runtime_registries, response) {
							Some(dispatch) => {
								if dispatch.clear_status {
									session.ui.status_message = None;
								}
								if dispatch.status_message.is_some() {
									session.ui.status_message = dispatch.status_message;
								}
								dispatch.needs_render
							}
							None => false,
						}
					}
					InternalEvent::NavigationResolved(resolved) => {
						let lifecycle_before = session.capture_text_lifecycle();
						let rendered = apply_navigation_resolved(&runtime, &mut session, *resolved);
						session.emit_text_lifecycle_since(lifecycle_before);
						rendered
					}
					InternalEvent::RuneCompile(result) => {
						if let Some(host) = rune_host.as_mut() {
							let report = host.apply_compile_result(&mut session, *result);
							if let Some(status) = report.status {
								session.ui.status_message = Some(status);
							}
							report.needs_render
						} else {
							false
						}
					}
					InternalEvent::JobCompleted(job) => {
						let live = session.jobs.owner(job).is_some_and(|owner| {
							owner == super::git::GIT_JOB_OWNER
								|| session.runtime_registries.owner_is_live(owner)
						});
						session.jobs.handle_supervisor_report(job, live);
						false
					}
				};
				needs_render |= drain_run_loop_deliveries(
					&mut session,
					rune_host.as_mut(),
					subscriber_dispatcher.as_mut(),
					job_dispatcher.as_mut(),
					&tx_internal,
					&dynamic_invocation_gate,
					event_tap.as_ref(),
				);
				if needs_render
					&& render_frame(RenderParams {
						registry: &registry,
						resolver: &resolver,
						tx_view: &tx_view,
						reactor: &reactor,
						lang_registry: &lang_registry,
						session: &mut session,
					}) {
					break;
				}
				continue;
			}
		};
		let InputDispatch {
			commands,
			dynamic_invocations,
			status_message,
			clear_status,
			mut needs_render,
			declined_input,
		} = input_dispatch;
		// Declined while a host panel had focus: hand the key to the frontend
		// in order, so a table can scroll without the engine knowing how.
		if let Some((code, modifiers)) = declined_input
			&& let Some(panel_id) = focused_host_panel_id(&session)
		{
			session.queue_host_event(crate::engine::HostEvent::UnhandledInput {
				panel_id,
				input: crate::engine::InputEvent::Key { code, modifiers },
			});
		}
		if clear_status {
			session.ui.status_message = None;
		}
		if status_message.is_some() {
			session.ui.status_message = status_message;
		}
		for invocation in dynamic_invocations {
			let is_rune = invocation.action::<RuneCallbackRef>().is_some();
			let result = if is_rune {
				match rune_host.as_mut() {
					Some(host) => {
						dynamic_invocation_gate.invoke_dynamic(host, &mut session, invocation)
					}
					None => Err(ActionError::Failed(
						"Rune action host is unavailable".to_string(),
					)),
				}
			} else {
				dynamic_invocation_gate.invoke_dynamic(
					dynamic_action_executor.as_mut(),
					&mut session.session,
					invocation,
				)
			};
			match result {
				Ok(outcome) => {
					if outcome.status_message.is_some() {
						session.ui.status_message = outcome.status_message;
					}
					needs_render |= outcome.needs_render;
				}
				Err(error) => {
					session.ui.status_message = Some(error.to_string());
					needs_render = true;
				}
			}
		}
		let mut should_stop = false;
		for command in commands {
			match command {
				EditorCommand::SourceRune(path) => {
					session.ui.status_message = Some(match rune_host.as_mut() {
						Some(host) => host
							.request_source(PathBuf::from(path))
							.unwrap_or_else(|error| error),
						None => "Rune host is not enabled".to_string(),
					});
					needs_render = true;
					continue;
				}
				EditorCommand::UnloadRune(path) => {
					session.ui.status_message = Some(match rune_host.as_mut() {
						Some(host) => host
							.unload_source(PathBuf::from(path), &mut session)
							.unwrap_or_else(|error| error),
						None => "Rune host is not enabled".to_string(),
					});
					needs_render = true;
					continue;
				}
				_ => {}
			}
			let lifecycle_before = session.capture_text_lifecycle();
			let dispatch = dispatch_command(
				DispatchContext {
					runtime: &runtime,
					tx_internal: &tx_internal,
					tx_view: &tx_view,
					reactor: &reactor,
					resolver: &resolver,
					lang_registry: &lang_registry,
				},
				&mut session,
				command,
			);
			session.emit_text_lifecycle_since(lifecycle_before);
			needs_render |= dispatch.needs_render;
			needs_render |= apply_input_mode_override(&mut session);
			if dispatch.should_stop {
				should_stop = true;
				break;
			}
		}
		// Ordered, non-coalescing frontend events. Drained once per request
		// rather than per command: a declined key yields no commands, so a
		// per-command drain would never deliver its UnhandledInput.
		if let Some(sender) = host_events.as_ref() {
			for event in session.take_host_events() {
				let _ = sender.send(event);
			}
		}
		needs_render |= drain_run_loop_deliveries(
			&mut session,
			rune_host.as_mut(),
			subscriber_dispatcher.as_mut(),
			job_dispatcher.as_mut(),
			&tx_internal,
			&dynamic_invocation_gate,
			event_tap.as_ref(),
		);
		if should_stop {
			let _ = render_frame(RenderParams {
				registry: &registry,
				resolver: &resolver,
				tx_view: &tx_view,
				reactor: &reactor,
				lang_registry: &lang_registry,
				session: &mut session,
			});
			break;
		}
		if needs_render
			&& render_frame(RenderParams {
				registry: &registry,
				resolver: &resolver,
				tx_view: &tx_view,
				reactor: &reactor,
				lang_registry: &lang_registry,
				session: &mut session,
			}) {
			break;
		}
	}
	// Drop Functions and their Units before the guarded editor value they accept.
	drop(rune_host.take());
}

#[cfg(test)]
mod event_bus_integration_tests {
	use super::*;
	use crate::core::{BufferId, ViewId};
	use crate::engine::event_bus::{EngineEvent, EventKind, EventKindSet};
	use crate::engine::event_subscribers::{
		SubscriberBaseline, SubscriberDispatcher, SubscriberOutcome,
	};
	use crate::engine::language::{
		DefinitionLocation, DefinitionResult, DefinitionTarget, LanguageProvider, SemanticOutput,
	};
	use crate::engine::request::CommandSender;
	use crate::engine::runtime_registry::{
		ActionLease, RegistrationBatch, RuntimeSubscriptionRegistration, SubscriberInvocation,
	};
	use crate::engine::settings::SaveDurability;
	use crate::engine::ui_colors::UiColorOverrides;
	use crate::engine::{InputEvent, KeyCode, Modifiers};
	use std::collections::BTreeMap;
	use std::fs;
	use std::sync::atomic::{AtomicUsize, Ordering};
	use std::sync::mpsc;
	use std::thread;
	use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

	struct RunningTestEngine {
		commands: CommandSender,
		events: mpsc::Receiver<EngineEvent>,
		_views: mpsc::Receiver<crate::uast::projection::Viewport>,
		thread: thread::JoinHandle<()>,
	}

	struct NavigationTestProvider;
	static NAVIGATION_SEQUENCE: AtomicUsize = AtomicUsize::new(0);

	impl LanguageProvider for NavigationTestProvider {
		fn language_id(&self) -> &'static str {
			"navigation-test"
		}

		fn label(&self) -> &'static str {
			"Navigation Test"
		}

		fn extensions(&self) -> &'static [&'static str] {
			&["nav"]
		}

		fn lexical_baseline(
			&self,
			_viewport_bytes: &[u8],
			_offset_map: &[(usize, DocByte)],
		) -> Vec<HighlightSpan> {
			Vec::new()
		}

		fn semantic_reactor(
			&self,
			_full_source: Arc<String>,
			_global_offset: DocByte,
			_file_path: &str,
		) -> SemanticOutput {
			SemanticOutput::default()
		}

		fn goto_definition(
			&self,
			source: Arc<String>,
			_file_path: &str,
			_cursor: DocByte,
		) -> DefinitionResult {
			if source.contains("slow") {
				thread::sleep(Duration::from_millis(75));
			}
			if source.contains("unsupported") {
				return DefinitionResult::Unsupported;
			}
			if source.contains("sequence") {
				let key = if NAVIGATION_SEQUENCE.fetch_add(1, Ordering::SeqCst) == 0 {
					thread::sleep(Duration::from_millis(75));
					"first="
				} else {
					"second="
				};
				let path = source
					.find(key)
					.and_then(|marker| source[marker + key.len()..].lines().next())
					.unwrap_or_default();
				return DefinitionResult::Targets(vec![DefinitionTarget {
					location: DefinitionLocation::File(PathBuf::from(path)),
					focus_range: Some((0, 4)),
					full_range: (0, 4),
					name: key.to_string(),
				}]);
			}
			let Some(marker) = source.find("target=") else {
				return DefinitionResult::NoDefinition;
			};
			let path = source[marker + "target=".len()..]
				.lines()
				.next()
				.unwrap_or_default();
			let first = DefinitionTarget {
				location: DefinitionLocation::File(PathBuf::from(path)),
				focus_range: Some((0, 4)),
				full_range: (0, 4),
				name: "test_target".to_string(),
			};
			if let Some(marker) = source.find("second=") {
				let second_path = source[marker + "second=".len()..]
					.lines()
					.next()
					.unwrap_or_default();
				DefinitionResult::Targets(vec![
					DefinitionTarget {
						location: DefinitionLocation::File(PathBuf::from(second_path)),
						focus_range: Some((0, 4)),
						full_range: (0, 4),
						name: "second_target".to_string(),
					},
					first,
				])
			} else {
				DefinitionResult::Targets(vec![first])
			}
		}
	}

	fn spawn_test_engine(initial_file: Option<String>) -> RunningTestEngine {
		let (commands, _input, internal, requests) = crate::engine::request::channel();
		let (tx_view, views) = mpsc::channel();
		let (tx_event, events) = mpsc::channel();
		let registry = Arc::new(UastRegistry::new(16_384));
		let (tx_notify, _notifications) = mpsc::channel();
		let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
			registry.clone(),
			tx_notify,
		));
		let mut languages = super::super::language::LanguageRegistry::new();
		languages.register(crate::svp::rust_provider::RustProvider::new());
		languages.register(NavigationTestProvider);
		let languages = Arc::new(languages);
		let engine = Engine::new(
			registry,
			resolver,
			requests,
			internal,
			tx_view,
			languages,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
		)
		.with_initial_file(initial_file)
		.with_event_tap(tx_event);
		let thread = thread::spawn(move || engine.run());
		RunningTestEngine {
			commands,
			events,
			_views: views,
			thread,
		}
	}

	fn recv_event(events: &mpsc::Receiver<EngineEvent>) -> EngineEvent {
		events
			.recv_timeout(Duration::from_secs(2))
			.expect("engine event")
	}

	fn recv_viewport_until(
		views: &mpsc::Receiver<crate::uast::projection::Viewport>,
		timeout: Duration,
		mut accept: impl FnMut(&crate::uast::projection::Viewport) -> bool,
	) -> crate::uast::projection::Viewport {
		let deadline = Instant::now() + timeout;
		let mut last = None;
		loop {
			match views.recv_timeout(Duration::from_millis(250)) {
				Ok(viewport) if accept(&viewport) => return viewport,
				Ok(viewport) if Instant::now() < deadline => {
					last = Some((
						viewport.status_message.clone(),
						viewport
							.panes
							.first()
							.and_then(|pane| pane.file_name.clone()),
					));
				}
				Err(mpsc::RecvTimeoutError::Timeout) if Instant::now() < deadline => {}
				Ok(_) => unreachable!(),
				Err(error) => panic!("viewport unavailable: {error}; last={last:?}"),
			}
		}
	}

	fn temp_rune_path(name: &str) -> PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system clock")
			.as_nanos();
		std::env::temp_dir().join(format!(
			"baryon-runtime-{name}-{}-{nonce}.rn",
			std::process::id()
		))
	}

	#[derive(Debug, Clone)]
	struct LoopSubscriber;

	#[derive(Debug, Clone, PartialEq, Eq)]
	enum LoopSubscriberMessage {
		Baseline { buffers: usize, views: usize },
		Event(EventKind),
	}

	struct LoopSubscriberDispatcher {
		tx: mpsc::Sender<LoopSubscriberMessage>,
	}

	impl SubscriberDispatcher<EditorSession> for LoopSubscriberDispatcher {
		fn owner_is_live(&self, _owner: crate::core::RegistryOwnerId) -> bool {
			true
		}

		fn initialize(
			&mut self,
			_context: &mut EditorSession,
			invocation: SubscriberInvocation,
			baseline: &Arc<SubscriberBaseline>,
		) -> SubscriberOutcome {
			assert!(invocation.action::<LoopSubscriber>().is_some());
			self.tx
				.send(LoopSubscriberMessage::Baseline {
					buffers: baseline.buffers.len(),
					views: baseline.views.len(),
				})
				.expect("subscriber test receiver");
			SubscriberOutcome::delivered()
		}

		fn deliver(
			&mut self,
			_context: &mut EditorSession,
			invocation: SubscriberInvocation,
			event: &EngineEvent,
		) -> SubscriberOutcome {
			assert!(invocation.action::<LoopSubscriber>().is_some());
			self.tx
				.send(LoopSubscriberMessage::Event(
					event.kind().expect("ordinary subscriber event"),
				))
				.expect("subscriber test receiver");
			SubscriberOutcome::delivered()
		}
	}

	#[test]
	fn real_loop_initializes_and_routes_language_neutral_subscribers() {
		let (commands, _input, internal, requests) = crate::engine::request::channel();
		let (tx_view, views) = mpsc::channel();
		let (tx_subscriber, subscriber_messages) = mpsc::channel();
		let registry = Arc::new(UastRegistry::new(16_384));
		let (tx_notify, _notifications) = mpsc::channel();
		let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
			registry.clone(),
			tx_notify,
		));
		let languages = Arc::new(super::super::language::LanguageRegistry::new());
		let engine = Engine::new(
			registry,
			resolver,
			requests,
			internal,
			tx_view,
			languages,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
		)
		.with_subscriber_setup(|session| {
			let owner = session
				.runtime_registries
				.allocate_owner()
				.expect("subscriber owner");
			let mut batch = RegistrationBatch::new(owner);
			batch.subscriptions.push(RuntimeSubscriptionRegistration {
				kinds: EventKindSet::from_kinds([EventKind::BufferOpened, EventKind::ViewCreated]),
				action: ActionLease::new(owner, LoopSubscriber),
			});
			session
				.runtime_registries
				.commit_owner_batch(batch)
				.expect("publish subscriber");
		})
		.with_subscriber_dispatcher(LoopSubscriberDispatcher { tx: tx_subscriber });
		let thread = thread::spawn(move || engine.run());

		assert_eq!(
			subscriber_messages
				.recv_timeout(Duration::from_secs(2))
				.expect("startup baseline"),
			LoopSubscriberMessage::Baseline {
				buffers: 1,
				views: 1,
			}
		);
		commands.send(EditorCommand::DocTabNew).unwrap();
		assert_eq!(
			subscriber_messages
				.recv_timeout(Duration::from_secs(2))
				.expect("buffer event"),
			LoopSubscriberMessage::Event(EventKind::BufferOpened)
		);
		assert_eq!(
			subscriber_messages
				.recv_timeout(Duration::from_secs(2))
				.expect("view event"),
			LoopSubscriberMessage::Event(EventKind::ViewCreated)
		);
		commands.send(EditorCommand::Quit).unwrap();
		thread.join().unwrap();
		drop(views);
	}

	#[test]
	fn rune_typed_subscriber_never_falls_through_to_the_boxed_dispatcher() {
		let (commands, _input, internal, requests) = crate::engine::request::channel();
		let (tx_view, views) = mpsc::channel();
		let (tx_subscriber, subscriber_messages) = mpsc::channel();
		let registry = Arc::new(UastRegistry::new(16_384));
		let (tx_notify, _notifications) = mpsc::channel();
		let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
			registry.clone(),
			tx_notify,
		));
		let languages = Arc::new(super::super::language::LanguageRegistry::new());
		let engine = Engine::new(
			registry,
			resolver,
			requests,
			internal,
			tx_view,
			languages,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
		)
		.with_subscriber_setup(|session| {
			let owner = session
				.runtime_registries
				.allocate_owner()
				.expect("subscriber owner");
			let mut batch = RegistrationBatch::new(owner);
			batch.subscriptions.push(RuntimeSubscriptionRegistration {
				kinds: EventKindSet::one(EventKind::BufferOpened),
				action: ActionLease::new(
					owner,
					RuneAutocmdRef {
						initialize: RuneCallbackRef {
							owner,
							callback_id: 0,
						},
						event: RuneCallbackRef {
							owner,
							callback_id: 1,
						},
						buffer_filter: None,
					},
				),
			});
			session
				.runtime_registries
				.commit_owner_batch(batch)
				.expect("publish Rune-typed subscriber");
		})
		.with_subscriber_dispatcher(LoopSubscriberDispatcher { tx: tx_subscriber });
		let thread = thread::spawn(move || engine.run());

		views
			.recv_timeout(Duration::from_secs(2))
			.expect("binding-unavailable status render");
		assert!(subscriber_messages.try_recv().is_err());
		commands.send(EditorCommand::Quit).unwrap();
		thread.join().unwrap();
	}

	#[test]
	fn rune_and_language_neutral_subscribers_route_side_by_side() {
		let init = temp_rune_path("subscriber-routing");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::autocmd(
        editor,
        [baryon::EventKind::BufferOpened],
        |editor, baseline| {},
        |editor, event| {},
    );
}
"#,
		)
		.expect("write Rune subscriber init");
		let (commands, _input, internal, requests) = crate::engine::request::channel();
		let (tx_view, views) = mpsc::channel();
		let (tx_subscriber, subscriber_messages) = mpsc::channel();
		let registry = Arc::new(UastRegistry::new(16_384));
		let (tx_notify, _notifications) = mpsc::channel();
		let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
			registry.clone(),
			tx_notify,
		));
		let languages = Arc::new(super::super::language::LanguageRegistry::new());
		let engine = Engine::new(
			registry,
			resolver,
			requests,
			internal,
			tx_view,
			languages,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
		)
		.with_rune_host_config(Some(super::super::rune_host::RuneHostConfig::new(
			init.clone(),
		)))
		.with_subscriber_setup(|session| {
			let owner = session
				.runtime_registries
				.allocate_owner()
				.expect("subscriber owner");
			let mut batch = RegistrationBatch::new(owner);
			batch.subscriptions.push(RuntimeSubscriptionRegistration {
				kinds: EventKindSet::one(EventKind::BufferOpened),
				action: ActionLease::new(owner, LoopSubscriber),
			});
			session
				.runtime_registries
				.commit_owner_batch(batch)
				.expect("publish fallback subscriber");
		})
		.with_subscriber_dispatcher(LoopSubscriberDispatcher { tx: tx_subscriber });
		let thread = thread::spawn(move || engine.run());

		assert!(matches!(
			subscriber_messages.recv_timeout(Duration::from_secs(2)),
			Ok(LoopSubscriberMessage::Baseline { .. })
		));
		commands.send(EditorCommand::DocTabNew).unwrap();
		assert_eq!(
			subscriber_messages
				.recv_timeout(Duration::from_secs(2))
				.expect("fallback event beside Rune autocmd"),
			LoopSubscriberMessage::Event(EventKind::BufferOpened)
		);
		commands.send(EditorCommand::Quit).unwrap();
		thread.join().unwrap();
		drop(views);
		let _ = fs::remove_file(init);
	}

	#[test]
	fn real_loop_drains_lifecycle_and_edit_events_in_commit_order() {
		let engine = spawn_test_engine(None);
		engine.commands.send(EditorCommand::DocTabNew).unwrap();
		engine
			.commands
			.send(EditorCommand::InsertChar('x'))
			.unwrap();
		engine.commands.send(EditorCommand::Undo).unwrap();
		engine.commands.send(EditorCommand::DocTabClose).unwrap();
		engine.commands.send(EditorCommand::Quit).unwrap();

		let RunningTestEngine { events, thread, .. } = engine;
		thread.join().unwrap();
		let events = events.try_iter().collect::<Vec<_>>();

		let [
			EngineEvent::BufferOpened { buffer, .. },
			EngineEvent::ViewCreated {
				view,
				buffer: created_buffer,
			},
			EngineEvent::FocusedTextViewChanged {
				from: Some(initial_view),
				to: Some(focused_view),
			},
			EngineEvent::BufferEdited {
				buffer: edited_buffer,
				acting_view,
				..
			},
			EngineEvent::BufferEdited {
				buffer: undo_buffer,
				acting_view: undo_view,
				deleted_bytes: 1,
				inserted_bytes: 0,
				..
			},
			EngineEvent::FocusedTextViewChanged {
				from: Some(closed_focus),
				to: Some(restored_view),
			},
			EngineEvent::ViewClosed {
				view: closed_view,
				buffer: closed_view_buffer,
			},
			EngineEvent::BufferClosed {
				buffer: closed_buffer,
			},
		] = events.as_slice()
		else {
			panic!("unexpected event sequence: {events:#?}");
		};

		assert_eq!(buffer, created_buffer);
		assert_eq!(view, focused_view);
		assert_eq!(edited_buffer, buffer);
		assert_eq!(acting_view, view);
		assert_eq!(undo_buffer, buffer);
		assert_eq!(undo_view, view);
		assert_eq!(closed_focus, view);
		assert_eq!(restored_view, initial_view);
		assert_eq!(closed_view, view);
		assert_eq!(closed_view_buffer, buffer);
		assert_eq!(closed_buffer, buffer);
	}

	#[test]
	fn startup_file_lifecycle_drains_before_the_first_request() {
		let path = crate::engine::test_support::temp_test_path("event-startup.txt");
		std::fs::write(&path, b"startup\n").unwrap();
		let engine = spawn_test_engine(Some(path.to_string_lossy().into_owned()));

		let opened = recv_event(&engine.events);
		let retargeted = recv_event(&engine.events);
		let closed = recv_event(&engine.events);
		assert!(matches!(
			(opened, retargeted, closed),
			(
				EngineEvent::BufferOpened { buffer: new_buffer, .. },
				EngineEvent::ViewRetargeted {
					view,
					old_buffer,
					new_buffer: retarget_buffer,
				},
				EngineEvent::BufferClosed { buffer: closed_buffer },
			) if new_buffer == retarget_buffer
				&& view == ViewId::new(1)
				&& old_buffer == BufferId::new(1)
				&& closed_buffer == old_buffer
		));

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn goto_definition_uses_the_live_rust_document_and_moves_the_exact_view() {
		let source = "fn main() { target(); }\nfn target() {}\n";
		let engine = spawn_test_engine(None);
		engine
			.commands
			.send(EditorCommand::ReplaceDocument {
				text: source.to_string(),
				language: Some("rust".to_string()),
				virtual_path: Some("live.rs".to_string()),
			})
			.expect("install live Rust source");
		let mut cursor = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport
				.panes
				.first()
				.is_some_and(|pane| !pane.tokens.is_empty())
		})
		.panes[0]
			.cursor_abs_byte;
		while cursor < DocByte::new(source.find("target").expect("target call") as u64) {
			engine
				.commands
				.send(EditorCommand::MoveCursor(MoveDirection::NextWord))
				.expect("move to target call");
			cursor = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
				viewport
					.panes
					.first()
					.is_some_and(|pane| pane.cursor_abs_byte > cursor)
			})
			.panes[0]
				.cursor_abs_byte;
		}
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");

		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(30), |viewport| {
			viewport.status_message.as_deref() == Some("Jumped to definition")
		});
		let pane = viewport
			.panes
			.iter()
			.find(|pane| pane.pane_id == viewport.focused_pane_id)
			.expect("focused pane");
		assert_eq!(
			pane.cursor_abs_byte,
			DocByte::new(source.rfind("target").expect("definition name") as u64)
		);
		engine
			.commands
			.send(EditorCommand::MoveCursor(MoveDirection::Top))
			.expect("move before saved origin");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.panes[0].cursor_abs_byte == DocByte::ZERO
		});
		engine
			.commands
			.send(EditorCommand::InsertChar(' '))
			.expect("edit before saved origin");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.panes[0].cursor_abs_byte == DocByte::new(1) && viewport.panes[0].is_dirty
		});
		engine
			.commands
			.send(EditorCommand::JumpBack)
			.expect("jump back to invocation");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref() == Some("Jumped back")
		});
		assert_eq!(
			viewport.panes[0].cursor_abs_byte,
			DocByte::new(cursor.get() + 1)
		);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
	}

	#[test]
	fn goto_definition_navigates_a_real_two_file_rust_workspace_end_to_end() {
		let directory = crate::engine::test_support::temp_test_path("goto-engine-workspace");
		let source_dir = directory.join("src");
		std::fs::create_dir_all(&source_dir).expect("create workspace fixture");
		std::fs::write(
			directory.join("Cargo.toml"),
			"[package]\nname = \"goto_engine_fixture\"\nversion = \"0.1.0\"\nedition = \"2024\"\n",
		)
		.expect("write manifest");
		let main_path = source_dir.join("main.rs");
		let target_path = source_dir.join("other.rs");
		let source = "mod other;\nfn main() { other::target(); }\n";
		std::fs::write(&target_path, "pub fn target() {}\n").expect("write target");
		let engine = spawn_test_engine(None);
		engine
			.commands
			.send(EditorCommand::ReplaceDocument {
				text: source.to_string(),
				language: Some("rust".to_string()),
				virtual_path: Some("main.rs".to_string()),
			})
			.expect("install live main source");
		let initial = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport
				.panes
				.first()
				.is_some_and(|pane| !pane.tokens.is_empty())
		});
		let source_buffer = initial.panes[0].buffer_id;
		let mut cursor = initial.panes[0].cursor_abs_byte;
		let target_call = DocByte::new(source.rfind("target").expect("target call") as u64);
		while cursor < target_call {
			engine
				.commands
				.send(EditorCommand::MoveCursor(MoveDirection::NextWord))
				.expect("move to target call");
			cursor = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
				viewport
					.panes
					.first()
					.is_some_and(|pane| pane.cursor_abs_byte > cursor)
			})
			.panes[0]
				.cursor_abs_byte;
		}
		engine
			.commands
			.send(EditorCommand::WriteFileAs(
				main_path.to_string_lossy().into_owned(),
			))
			.expect("save main source");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.panes.first().is_some_and(|pane| {
				pane.file_name
					.as_deref()
					.is_some_and(|name| name.ends_with("main.rs"))
					&& !pane.is_dirty
			})
		});
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(30), |viewport| {
			viewport.status_message.as_deref() == Some("Jumped to definition")
		});
		assert!(
			viewport.panes[0]
				.file_name
				.as_deref()
				.is_some_and(|name| name.ends_with("other.rs"))
		);
		assert_eq!(viewport.panes[0].cursor_abs_byte, DocByte::new(7));
		engine
			.commands
			.send(EditorCommand::JumpBack)
			.expect("jump back across retained source buffer");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref() == Some("Jumped back")
		});
		assert_eq!(viewport.panes[0].buffer_id, source_buffer);
		assert_eq!(viewport.panes[0].cursor_abs_byte, target_call);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_dir_all(directory);
	}

	fn install_navigation_test_source(engine: &RunningTestEngine, text: String) -> BufferId {
		engine
			.commands
			.send(EditorCommand::ReplaceDocument {
				text,
				language: Some("navigation-test".to_string()),
				virtual_path: Some("source.nav".to_string()),
			})
			.expect("install navigation source");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport
				.panes
				.first()
				.is_some_and(|pane| !pane.tokens.is_empty())
		})
		.panes[0]
			.buffer_id
	}

	#[test]
	fn goto_definition_refuses_to_replace_a_dirty_source_for_cross_file_navigation() {
		let target = crate::engine::test_support::temp_test_path("goto-dirty-target");
		std::fs::write(&target, "dest").expect("write target");
		let engine = spawn_test_engine(None);
		let source_buffer =
			install_navigation_test_source(&engine, format!("target={}\n", target.display()));
		engine
			.commands
			.send(EditorCommand::InsertText("#".to_string()))
			.expect("dirty source");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.panes.first().is_some_and(|pane| pane.is_dirty)
		});
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref()
				== Some("Definition is in another file; write or discard source changes first")
		});
		assert_eq!(viewport.panes[0].buffer_id, source_buffer);
		engine
			.commands
			.send(EditorCommand::JumpBack)
			.expect("failed definition must not record history");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref() == Some("No older jump")
		});
		assert_eq!(viewport.panes[0].buffer_id, source_buffer);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(target);
	}

	#[test]
	fn goto_definition_rejects_a_response_after_the_source_state_changes() {
		let target = crate::engine::test_support::temp_test_path("goto-stale-target");
		std::fs::write(&target, "dest").expect("write target");
		let engine = spawn_test_engine(None);
		let source_buffer =
			install_navigation_test_source(&engine, format!("slow\ntarget={}\n", target.display()));
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		engine
			.commands
			.send(EditorCommand::InsertText("#".to_string()))
			.expect("change source");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref()
				== Some("Definition result discarded: the source document changed")
		});
		assert_eq!(viewport.panes[0].buffer_id, source_buffer);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(target);
	}

	#[test]
	fn goto_definition_rejects_a_response_after_the_requesting_view_is_retargeted() {
		let target = crate::engine::test_support::temp_test_path("goto-retarget-target");
		let replacement = crate::engine::test_support::temp_test_path("goto-retarget-replacement");
		std::fs::write(&target, "dest").expect("write target");
		std::fs::write(&replacement, "replacement").expect("write replacement");
		let engine = spawn_test_engine(None);
		let _ =
			install_navigation_test_source(&engine, format!("slow\ntarget={}\n", target.display()));
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		engine
			.commands
			.send(EditorCommand::LoadFile(replacement.display().to_string()))
			.expect("retarget requesting view");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref()
				== Some("Definition result discarded: the requesting view changed documents")
		});
		assert!(viewport.panes[0].file_name.as_deref().is_some_and(|name| {
			name.ends_with(
				replacement
					.file_name()
					.and_then(|name| name.to_str())
					.unwrap(),
			)
		}));

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(target);
		let _ = std::fs::remove_file(replacement);
	}

	#[test]
	fn goto_definition_rejects_a_response_after_the_requesting_view_is_closed() {
		let target = crate::engine::test_support::temp_test_path("goto-closed-target");
		std::fs::write(&target, "dest").expect("write target");
		let engine = spawn_test_engine(None);
		let source_buffer = install_navigation_test_source(
			&engine,
			format!("slow\ntarget={}\n", target.display()),
		);
		let source_pane = recv_viewport_until(
			&engine._views,
			Duration::from_secs(5),
			|viewport| viewport.panes.first().is_some_and(|pane| pane.buffer_id == source_buffer),
		)
		.panes[0]
			.pane_id;
		engine
			.commands
			.send(EditorCommand::NewPane {
				direction: SplitDirection::Vertical,
				language: None,
			})
			.expect("open surviving pane");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.panes.len() == 2
		});
		engine
			.commands
			.send(EditorCommand::FocusPane(source_pane))
			.expect("focus source");
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		engine
			.commands
			.send(EditorCommand::ClosePane)
			.expect("close requesting view");
		let viewport =
			recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
				viewport.status_message.as_deref()
					== Some("Definition result discarded: the requesting view was closed")
			});
		assert_eq!(viewport.panes.len(), 1);
		assert!(
			viewport
				.panes
				.iter()
				.all(|pane| pane.buffer_id != source_buffer)
		);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(target);
	}

	#[test]
	fn goto_definition_retargets_the_requesting_view_for_a_clean_cross_file_target() {
		let target = crate::engine::test_support::temp_test_path("goto-clean-target");
		std::fs::write(&target, "dest").expect("write target");
		let engine = spawn_test_engine(None);
		let _ = install_navigation_test_source(&engine, format!("target={}\n", target.display()));
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref() == Some("Jumped to definition")
		});
		assert!(viewport.panes[0].file_name.as_deref().is_some_and(|name| {
			name.ends_with(target.file_name().and_then(|name| name.to_str()).unwrap())
		}));
		assert_eq!(viewport.panes[0].cursor_abs_byte, DocByte::ZERO);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(target);
	}

	#[test]
	fn goto_definition_focuses_an_already_visible_target_without_losing_dirty_source() {
		let target = crate::engine::test_support::temp_test_path("goto-visible-target");
		std::fs::write(&target, "dest").expect("write target");
		let engine = spawn_test_engine(None);
		let source_buffer =
			install_navigation_test_source(&engine, format!("target={}\n", target.display()));
		let source_pane = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport
				.panes
				.first()
				.is_some_and(|pane| pane.buffer_id == source_buffer)
		})
		.panes[0]
			.pane_id;
		engine
			.commands
			.send(EditorCommand::NewPane {
				direction: SplitDirection::Vertical,
				language: None,
			})
			.expect("open target pane");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.panes.len() == 2
		});
		engine
			.commands
			.send(EditorCommand::LoadFile(target.display().to_string()))
			.expect("load visible target");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.panes.iter().any(|pane| {
				pane.file_name.as_deref().is_some_and(|name| {
					name.ends_with(target.file_name().and_then(|name| name.to_str()).unwrap())
				})
			})
		});
		engine
			.commands
			.send(EditorCommand::FocusPane(source_pane))
			.expect("focus source");
		engine
			.commands
			.send(EditorCommand::InsertText("#".to_string()))
			.expect("dirty source");
		recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport
				.panes
				.iter()
				.any(|pane| pane.buffer_id == source_buffer && pane.is_dirty)
		});
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref() == Some("Jumped to definition")
		});
		let focused = viewport
			.panes
			.iter()
			.find(|pane| pane.pane_id == viewport.focused_pane_id)
			.expect("focused target");
		assert!(focused.file_name.as_deref().is_some_and(|name| {
			name.ends_with(target.file_name().and_then(|name| name.to_str()).unwrap())
		}));
		assert!(
			viewport
				.panes
				.iter()
				.any(|pane| pane.buffer_id == source_buffer && pane.is_dirty)
		);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(target);
	}

	#[test]
	fn goto_definition_sorts_multiple_targets_and_reports_the_count() {
		let directory = crate::engine::test_support::temp_test_path("goto-multiple");
		std::fs::create_dir_all(&directory).expect("create target directory");
		let first = directory.join("a.txt");
		let second = directory.join("b.txt");
		std::fs::write(&first, "first").expect("write first target");
		std::fs::write(&second, "second").expect("write second target");
		let engine = spawn_test_engine(None);
		let _ = install_navigation_test_source(
			&engine,
			format!("target={}\nsecond={}\n", first.display(), second.display()),
		);
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definitions");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref() == Some("Jumped to definition (2 definitions)")
		});
		assert!(
			viewport.panes[0]
				.file_name
				.as_deref()
				.is_some_and(|name| name.ends_with("a.txt"))
		);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_dir_all(directory);
	}

	#[test]
	fn a_newer_goto_definition_request_supersedes_an_older_response() {
		let directory = crate::engine::test_support::temp_test_path("goto-supersede");
		std::fs::create_dir_all(&directory).expect("create target directory");
		let first = directory.join("a.txt");
		let second = directory.join("b.txt");
		std::fs::write(&first, "first").expect("write first target");
		std::fs::write(&second, "second").expect("write second target");
		NAVIGATION_SEQUENCE.store(0, Ordering::SeqCst);
		let engine = spawn_test_engine(None);
		let _ = install_navigation_test_source(
			&engine,
			format!(
				"sequence\nfirst={}\nsecond={}\n",
				first.display(),
				second.display()
			),
		);
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request first definition");
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request newer definition");
		let viewport =
			recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
				viewport.status_message.as_deref() == Some("Jumped to definition")
					&& viewport.panes[0]
						.file_name
						.as_deref()
						.is_some_and(|name| name.ends_with("b.txt"))
			});
		assert!(
			viewport.panes[0]
				.file_name
				.as_deref()
				.is_some_and(|name| name.ends_with("b.txt"))
		);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_dir_all(directory);
	}

	#[test]
	fn goto_definition_reports_an_unsupported_provider_without_touching_the_document() {
		let engine = spawn_test_engine(None);
		engine
			.commands
			.send(EditorCommand::ReplaceDocument {
				text: "unsupported".to_string(),
				language: Some("navigation-test".to_string()),
				virtual_path: Some("unsupported.nav".to_string()),
			})
			.expect("install source");
		let source_buffer =
			recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
				viewport
					.panes
					.first()
					.is_some_and(|pane| !pane.tokens.is_empty())
			})
			.panes[0]
				.buffer_id;
		engine
			.commands
			.send(EditorCommand::GotoDefinition)
			.expect("request definition");
		let viewport = recv_viewport_until(&engine._views, Duration::from_secs(5), |viewport| {
			viewport.status_message.as_deref()
				== Some("Go to definition is unsupported for this language")
		});
		assert_eq!(viewport.panes[0].buffer_id, source_buffer);

		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
	}

	#[test]
	fn substitute_fires_one_buffer_transition_through_the_shared_coordinator() {
		let engine = spawn_test_engine(None);
		engine
			.commands
			.send(EditorCommand::InsertText("foo foo".to_string()))
			.unwrap();
		let insertion = recv_event(&engine.events);
		assert!(
			matches!(
				insertion,
				EngineEvent::BufferEdited {
					delta_count: 1,
					inserted_bytes: 7,
					..
				}
			),
			"unexpected insertion event: {insertion:?}"
		);
		engine
			.commands
			.send(EditorCommand::SubstituteAll {
				pattern: "foo".to_string(),
				replacement: "bar".to_string(),
				range: crate::engine::CommandRange::WholeFile,
				flags: crate::engine::SubstituteFlags {
					global: true,
					..crate::engine::SubstituteFlags::default()
				},
			})
			.unwrap();
		let substitution = recv_event(&engine.events);
		assert!(
			matches!(
				substitution,
				EngineEvent::BufferEdited {
					delta_count: 1,
					deleted_bytes: 7,
					inserted_bytes: 7,
					..
				}
			),
			"unexpected substitution event: {substitution:?}"
		);
		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
	}

	#[test]
	fn reload_fires_buffer_replaced_instead_of_fabricating_edit_deltas() {
		let path = crate::engine::test_support::temp_test_path("event-reload.txt");
		std::fs::write(&path, b"before\n").unwrap();
		let engine = spawn_test_engine(Some(path.to_string_lossy().into_owned()));
		for _ in 0..3 {
			let _ = recv_event(&engine.events);
		}
		std::fs::write(&path, b"after\n").unwrap();
		engine
			.commands
			.send(EditorCommand::ReloadFile { force: true })
			.unwrap();
		assert!(matches!(
			recv_event(&engine.events),
			EngineEvent::BufferReplaced {
				buffer,
				old_state,
				new_state,
			} if buffer == BufferId::new(2) && old_state != new_state
		));
		assert!(engine.events.try_recv().is_err());
		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn focus_events_project_synthetic_views_to_none() {
		let engine = spawn_test_engine(None);
		engine
			.commands
			.send(EditorCommand::OpenFileBrowser)
			.unwrap();
		assert_eq!(
			recv_event(&engine.events),
			EngineEvent::FocusedTextViewChanged {
				from: Some(ViewId::new(1)),
				to: None,
			}
		);
		assert!(engine.events.try_recv().is_err());
		engine.commands.send(EditorCommand::ClosePane).unwrap();
		assert_eq!(
			recv_event(&engine.events),
			EngineEvent::FocusedTextViewChanged {
				from: None,
				to: Some(ViewId::new(1)),
			}
		);
		assert!(engine.events.try_recv().is_err());
		engine.commands.send(EditorCommand::Quit).unwrap();
		engine.thread.join().unwrap();
	}

	#[test]
	fn hydration_completion_is_suppressed_after_its_buffer_closes() {
		let registry = UastRegistry::new(256);
		let mut session = EditorSession::new(
			&registry,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
			None,
		);
		let buffer = BufferId::new(1);
		assert!(handle_hydration_completed(&mut session, buffer));
		assert_eq!(
			session.events.take_pass().into_iter().collect::<Vec<_>>(),
			vec![EngineEvent::HydrationCompleted { buffer }]
		);
		session.buffers.remove(buffer);
		assert!(!handle_hydration_completed(&mut session, buffer));
		assert!(session.events.take_pass().is_empty());
	}

	#[derive(Debug, Clone)]
	struct LoopJobAction;

	#[test]
	fn delivery_status_merge_preserves_both_diagnostics() {
		let services = Arc::new(Mutex::new(HostServices::default()));
		let before = lock_services(&services).messages.sequence();
		let mut ui_status = None;
		merge_delivery_statuses(
			&services,
			&mut ui_status,
			Some(BoundedStatus::fixed("subscriber failure")),
			Some(BoundedStatus::fixed("job failure")),
		);
		assert_eq!(ui_status.as_deref(), Some("subscriber failure"));
		assert_eq!(
			lock_services(&services)
				.messages
				.latest_since(before)
				.as_deref(),
			Some("job failure")
		);
	}

	#[test]
	fn allocation_failure_suppresses_only_the_event_rearm() {
		assert!(!should_rearm_event_drain(true, true));
		assert!(should_rearm_event_drain(false, true));
		assert!(!should_rearm_event_drain(true, false));
	}

	struct LoopJobDispatcher {
		tx: mpsc::Sender<(crate::core::JobId, super::super::jobs::JobOutcome, Vec<u8>)>,
		spawn_followup: bool,
	}

	impl JobDispatcher<EditorSession> for LoopJobDispatcher {
		fn owner_is_live(&self, _owner: crate::core::RegistryOwnerId) -> bool {
			true
		}

		fn complete(
			&mut self,
			context: &mut EditorSession,
			invocation: JobInvocation,
			completion: &JobCompletion,
		) -> SubscriberOutcome {
			assert!(invocation.action::<LoopJobAction>().is_some());
			if self.spawn_followup {
				self.spawn_followup = false;
				context
					.spawn_job(
						invocation.owner(),
						super::super::jobs::JobSpec {
							program: PathBuf::from("/bin/printf"),
							args: vec!["followup".into()],
							cwd: None,
							stdin: None,
							timeout: Duration::from_secs(1),
							completion: Some(ActionLease::new(invocation.owner(), LoopJobAction)),
						},
					)
					.expect("spawn follow-up job from completion callback");
			}
			self.tx
				.send((
					invocation.job(),
					completion.outcome.clone(),
					completion.stdout.to_vec(),
				))
				.expect("job test receiver");
			SubscriberOutcome::delivered()
		}
	}

	#[test]
	fn idle_run_loop_delivers_jobs_and_a_callback_can_chain_a_followup() {
		let (commands, _input, internal, requests) = crate::engine::request::channel();
		let (tx_view, _views) = mpsc::channel();
		let (tx_job, rx_job) = mpsc::channel();
		let registry = Arc::new(UastRegistry::new(16_384));
		let (tx_notify, _notifications) = mpsc::channel();
		let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
			registry.clone(),
			tx_notify,
		));
		let languages = Arc::new(super::super::language::LanguageRegistry::new());
		let engine = Engine::new(
			registry,
			resolver,
			requests,
			internal,
			tx_view,
			languages,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
		)
		.with_subscriber_setup(|session| {
			let owner = session
				.runtime_registries
				.allocate_owner()
				.expect("job owner");
			session
				.runtime_registries
				.commit_owner_batch(RegistrationBatch::new(owner))
				.expect("publish job owner");
			let spec = super::super::jobs::JobSpec {
				program: PathBuf::from("/bin/printf"),
				args: vec!["job-ok".into()],
				cwd: None,
				stdin: None,
				timeout: Duration::from_secs(1),
				completion: Some(ActionLease::new(owner, LoopJobAction)),
			};
			session.spawn_job(owner, spec).expect("spawn loop job");
		})
		.with_job_dispatcher(LoopJobDispatcher {
			tx: tx_job,
			spawn_followup: true,
		});
		let thread = thread::spawn(move || engine.run());
		let (_job, outcome, stdout) = rx_job
			.recv_timeout(Duration::from_secs(2))
			.expect("job delivered while engine idle");
		assert_eq!(
			outcome,
			super::super::jobs::JobOutcome::Exited { status: 0 }
		);
		assert_eq!(stdout, b"job-ok");
		let (_job, outcome, stdout) = rx_job
			.recv_timeout(Duration::from_secs(2))
			.expect("follow-up job delivered");
		assert_eq!(
			outcome,
			super::super::jobs::JobOutcome::Exited { status: 0 }
		);
		assert_eq!(stdout, b"followup");
		commands.send(EditorCommand::Quit).expect("quit");
		thread.join().expect("engine thread");
	}

	struct UnexpectedRuneJobFallback {
		called: mpsc::Sender<()>,
	}

	impl JobDispatcher<EditorSession> for UnexpectedRuneJobFallback {
		fn owner_is_live(&self, _owner: crate::core::RegistryOwnerId) -> bool {
			true
		}

		fn complete(
			&mut self,
			_context: &mut EditorSession,
			_invocation: JobInvocation,
			_completion: &JobCompletion,
		) -> SubscriberOutcome {
			let _ = self.called.send(());
			SubscriberOutcome::delivered()
		}
	}

	#[test]
	fn rune_job_payloads_fail_closed_without_reaching_the_boxed_fallback() {
		let (commands, _input, internal, requests) = crate::engine::request::channel();
		let (tx_view, views) = mpsc::channel();
		let (tx_called, rx_called) = mpsc::channel();
		let registry = Arc::new(UastRegistry::new(16_384));
		let (tx_notify, _notifications) = mpsc::channel();
		let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
			registry.clone(),
			tx_notify,
		));
		let languages = Arc::new(super::super::language::LanguageRegistry::new());
		let engine = Engine::new(
			registry,
			resolver,
			requests,
			internal,
			tx_view,
			languages,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
		)
		.with_subscriber_setup(|session| {
			let owner = session
				.runtime_registries
				.allocate_owner()
				.expect("job owner");
			session
				.runtime_registries
				.commit_owner_batch(RegistrationBatch::new(owner))
				.expect("publish job owner");
			session
				.spawn_job(
					owner,
					super::super::jobs::JobSpec {
						program: PathBuf::from("/bin/true"),
						args: Vec::new(),
						cwd: None,
						stdin: None,
						timeout: Duration::from_secs(1),
						completion: Some(ActionLease::new(
							owner,
							RuneCallbackRef {
								owner,
								callback_id: 0,
							},
						)),
					},
				)
				.expect("spawn Rune-shaped job");
		})
		.with_job_dispatcher(UnexpectedRuneJobFallback { called: tx_called });
		let thread = thread::spawn(move || engine.run());
		let deadline = Instant::now() + Duration::from_secs(2);
		loop {
			let viewport = views
				.recv_timeout(deadline.saturating_duration_since(Instant::now()))
				.expect("Rune job failure render");
			if viewport.status_message.as_deref() == Some("Rune job binding is unavailable") {
				break;
			}
		}
		assert!(rx_called.try_recv().is_err());
		commands.send(EditorCommand::Quit).expect("quit");
		thread.join().expect("engine thread");
	}

	#[cfg(target_os = "linux")]
	#[test]
	fn typed_quit_stays_prompt_with_a_live_stubborn_process_tree() {
		let mut samples = Vec::new();
		for sample in 0..3 {
			let (commands, input, internal, requests) = crate::engine::request::channel();
			let (tx_view, views) = mpsc::channel();
			let registry = Arc::new(UastRegistry::new(16_384));
			let (tx_notify, _notifications) = mpsc::channel();
			let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
				registry.clone(),
				tx_notify,
			));
			let languages = Arc::new(super::super::language::LanguageRegistry::new());
			let pidfile = std::env::temp_dir().join(format!(
				"baryon-typed-quit-job-{}-{sample}-{}",
				std::process::id(),
				SystemTime::now()
					.duration_since(UNIX_EPOCH)
					.expect("clock after epoch")
					.as_nanos()
			));
			let setup_pidfile = pidfile.clone();
			let engine = Engine::new(
				registry,
				resolver,
				requests,
				internal,
				tx_view,
				languages,
				"onedark".to_string(),
				false,
				false,
				true,
				false,
				true,
				true,
				SaveDurability::default(),
				UiColorOverrides::default(),
				BTreeMap::new(),
			)
			.with_subscriber_setup(move |session| {
				let owner = session
					.runtime_registries
					.allocate_owner()
					.expect("job owner");
				session
					.runtime_registries
					.commit_owner_batch(RegistrationBatch::new(owner))
					.expect("publish job owner");
				session
					.spawn_job(
						owner,
						super::super::jobs::JobSpec {
							program: PathBuf::from("/bin/sh"),
							args: vec![
								"-c".into(),
								format!(
									"trap '' TERM; (trap '' TERM; while :; do sleep 1; done) & echo $! > '{}'; wait",
									setup_pidfile.display()
								)
								.into(),
							],
							cwd: None,
							stdin: None,
							timeout: Duration::from_secs(30),
							completion: Some(ActionLease::new(owner, LoopJobAction)),
						},
					)
					.expect("spawn stubborn job");
			});
			let thread = thread::spawn(move || engine.run());
			let ready_by = Instant::now() + Duration::from_secs(2);
			while !pidfile.exists() && Instant::now() < ready_by {
				thread::sleep(Duration::from_millis(2));
			}
			assert!(pidfile.exists(), "stubborn child did not start");

			let started = Instant::now();
			for code in [KeyCode::Char(':'), KeyCode::Char('q'), KeyCode::Enter] {
				input
					.send(InputEvent::Key {
						code,
						modifiers: Modifiers::NONE,
					})
					.expect("submit :q");
			}
			thread.join().expect("engine thread");
			samples.push(started.elapsed());
			drop(commands);
			drop(views);
			let _ = fs::remove_file(pidfile);
		}
		samples.sort_unstable();
		let median = samples[1];
		assert!(
			median < Duration::from_millis(100),
			"median typed :q with a live job was {median:?}: {samples:?}"
		);
	}

	#[derive(Debug, Clone)]
	struct OrderedSubscriber(usize);

	#[derive(Debug, Clone, Copy, PartialEq, Eq)]
	enum OrderedDelivery {
		Event(usize),
		Job,
	}

	struct OrderedSubscriberDispatcher {
		tx: mpsc::Sender<OrderedDelivery>,
		initialized: usize,
	}

	impl SubscriberDispatcher<EditorSession> for OrderedSubscriberDispatcher {
		fn owner_is_live(&self, _owner: crate::core::RegistryOwnerId) -> bool {
			true
		}

		fn initialize(
			&mut self,
			context: &mut EditorSession,
			invocation: SubscriberInvocation,
			_baseline: &Arc<SubscriberBaseline>,
		) -> SubscriberOutcome {
			self.initialized += 1;
			if self.initialized == 257 {
				context.events.push(EngineEvent::BufferOpened {
					buffer: BufferId::new(1),
					state: StateId::ZERO,
				});
				context
					.spawn_job(
						invocation.owner(),
						super::super::jobs::JobSpec {
							program: PathBuf::from("/bin/true"),
							args: Vec::new(),
							cwd: None,
							stdin: None,
							timeout: Duration::from_secs(1),
							completion: Some(ActionLease::new(invocation.owner(), LoopJobAction)),
						},
					)
					.expect("spawn job at event-pass boundary");
			}
			SubscriberOutcome::delivered()
		}

		fn deliver(
			&mut self,
			_context: &mut EditorSession,
			invocation: SubscriberInvocation,
			_event: &EngineEvent,
		) -> SubscriberOutcome {
			let index = invocation
				.action::<OrderedSubscriber>()
				.expect("ordered subscriber payload")
				.0;
			self.tx
				.send(OrderedDelivery::Event(index))
				.expect("ordered delivery receiver");
			SubscriberOutcome::delivered()
		}
	}

	struct OrderedJobDispatcher {
		tx: mpsc::Sender<OrderedDelivery>,
	}

	impl JobDispatcher<EditorSession> for OrderedJobDispatcher {
		fn owner_is_live(&self, _owner: crate::core::RegistryOwnerId) -> bool {
			true
		}

		fn complete(
			&mut self,
			_context: &mut EditorSession,
			_invocation: JobInvocation,
			_completion: &JobCompletion,
		) -> SubscriberOutcome {
			self.tx
				.send(OrderedDelivery::Job)
				.expect("ordered delivery receiver");
			SubscriberOutcome::delivered()
		}
	}

	#[test]
	fn job_completion_waits_for_a_suspended_event_pass_boundary() {
		let (commands, _input, internal, requests) = crate::engine::request::channel();
		let (tx_view, _views) = mpsc::channel();
		let (tx_order, rx_order) = mpsc::channel();
		let registry = Arc::new(UastRegistry::new(16_384));
		let (tx_notify, _notifications) = mpsc::channel();
		let resolver = Arc::new(SvpResolver::new_blocking_for_tests(
			registry.clone(),
			tx_notify,
		));
		let languages = Arc::new(super::super::language::LanguageRegistry::new());
		let subscriber_tx = tx_order.clone();
		let engine = Engine::new(
			registry,
			resolver,
			requests,
			internal,
			tx_view,
			languages,
			"onedark".to_string(),
			false,
			false,
			true,
			false,
			true,
			true,
			SaveDurability::default(),
			UiColorOverrides::default(),
			BTreeMap::new(),
		)
		.with_subscriber_setup(|session| {
			let first = session.runtime_registries.allocate_owner().unwrap();
			let mut first_batch = RegistrationBatch::new(first);
			for index in 0..256 {
				first_batch
					.subscriptions
					.push(RuntimeSubscriptionRegistration {
						kinds: EventKindSet::one(EventKind::BufferOpened),
						action: ActionLease::new(first, OrderedSubscriber(index)),
					});
			}
			session
				.runtime_registries
				.commit_owner_batch(first_batch)
				.unwrap();

			let second = session.runtime_registries.allocate_owner().unwrap();
			let mut second_batch = RegistrationBatch::new(second);
			second_batch
				.subscriptions
				.push(RuntimeSubscriptionRegistration {
					kinds: EventKindSet::one(EventKind::BufferOpened),
					action: ActionLease::new(second, OrderedSubscriber(256)),
				});
			session
				.runtime_registries
				.commit_owner_batch(second_batch)
				.unwrap();
		})
		.with_subscriber_dispatcher(OrderedSubscriberDispatcher {
			tx: subscriber_tx,
			initialized: 0,
		})
		.with_job_dispatcher(OrderedJobDispatcher { tx: tx_order });
		let thread = thread::spawn(move || engine.run());
		let mut observed = Vec::new();
		for _ in 0..258 {
			match rx_order.recv_timeout(Duration::from_secs(2)) {
				Ok(delivery) => observed.push(delivery),
				Err(error) => panic!(
					"ordered event/job delivery stopped after {} entries: {error:?}",
					observed.len()
				),
			}
		}
		assert!(
			observed[..257]
				.iter()
				.all(|item| matches!(item, OrderedDelivery::Event(_)))
		);
		assert_eq!(observed[257], OrderedDelivery::Job);
		commands.send(EditorCommand::Quit).unwrap();
		thread.join().unwrap();
	}
}
