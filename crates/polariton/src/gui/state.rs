#[cfg(feature = "toron")]
use crate::adapters::common::DatabaseAdapter;

mod actions;
mod agents;
mod connection;
/// PRODUCT-0158: the application without a window.
pub(crate) mod headless;
pub(crate) mod notebook_actions;
mod publish;
mod surface;
mod welcome;
use crate::adapters::{
	common::{AdapterStage, ExecutionResult},
	driver::AdapterState,
};
use crate::configuration::{self, StartupData};
use crate::editor::{CodeEditor, MarkdownCellExecution};
use crate::fonts::{DEJAVU_SANS_MONO, POLARITON_PLOT_SANS_REGULAR};
#[cfg(all(test, toron_local))]
use crate::gui::table_skin::TableSkin;
#[cfg(all(test, toron_local))]
use crate::gui::table_skin::TableSkinProjectionError;
#[cfg(feature = "toron")]
use crate::gui::table_skin::{TableSkinCascade, TableSkinProjection, TableSkinSpec};
use crate::gui::{
	components,
	menu::MenuState,
	messages::{ExportFormat, Message, NotebookRuneFailure, NotebookRuneOutcome},
};
#[cfg(feature = "toron")]
use crate::plot::authoring_layout::{
	layered_plot_layout, resolved_plot_layout, styled_plot_layout,
};
#[cfg(feature = "toron")]
#[cfg(feature = "toron")]
use crate::plot::common::{PlotLayout, TickPlacementError};
#[cfg(feature = "toron")]
use crate::plot::core::PlotType;
use crate::plot::export::{AvifBackend, PngBackend};
#[cfg(feature = "toron")]
use crate::plot::factory::create_layered_plot_from_artifact;
use crate::plot::factory::create_plot_from_artifact;
#[cfg(feature = "toron")]
use crate::plot::grid::{PlotGrid, PlotGridKernel};
#[cfg(feature = "toron")]
use crate::plot::request::plan_with_categorical_palette;
#[cfg(feature = "toron")]
use crate::plot::request::{PlotRequest, PlotSource};
use crate::plot::request::{ResolvedPlot, plan};
#[cfg(feature = "toron")]
use crate::plot::spec::AuthoredPlotSpec;
#[cfg(feature = "toron")]
use crate::plot::spec::{
	CategoricalPaletteSelection, Channel, ColorIdentity, LayeredPlotSpec, Override, ScaleFamily,
	resolve_channels, resolve_layered_channels,
};
#[cfg(feature = "toron")]
use crate::plot::theme::PlotSettings;
use crate::plot::theme::PlotSettingsLayer;
use crate::result_registry::{ResultRegistry, parse_result_annotation};
use crate::rune_host::{self, RuneCapabilities};
#[cfg(feature = "toron")]
use crate::rune_relation_envelope::{
	ArtifactReuseWitness, DisposableRunePlotSink, PendingRunePresentation, PendingRuneView,
	RelationPlanKey, RuneEnvelopeError, RunePlotBody, RunePlotDomainSpans, RunePlotMinorTickSpans,
	RunePlotTickEverySpans, RunePlotTickStepSpans, RuneTableSink, RuneTableSkinIntent,
	RuneTableSkinProjection, RuneViewIntent, RuneViewScheduler, SpannedRuneViewIntent,
	StyledRunePlotSink, TransformedRuneRelation, TransformedRuneView,
};
use crate::rune_view::{LocalViewIdentity, ViewContentKind, ViewPresentation};
#[cfg(feature = "toron")]
use boson::TemporalInterval;
use iced::{Element, Size, Subscription, Task, application, event, keyboard, window};
use libaryon::core::{BufferId, SessionId};
#[cfg(feature = "toron")]
use libaryon::core::{SavedSession, SessionStore, Writable};
use libaryon::uast::projection::MarkdownCellOutputTarget;
#[cfg(feature = "toron")]
use polars::frame::DataFrame;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
#[cfg(feature = "toron")]
use tokio::sync::RwLock;

#[cfg(feature = "toron")]
#[derive(Clone)]
struct RetainedRelationGeneration {
	revision: u64,
	plan_key: RelationPlanKey,
	transformed: Arc<TransformedRuneView>,
	artifacts: Vec<Option<Arc<crate::result_artifact::ResultArtifact>>>,
}

#[cfg(feature = "toron")]
#[derive(Clone)]
struct PendingSettingsPreview {
	preview_generation: u64,
	panel_id: libaryon::engine::HostPanelId,
	commit: bool,
	source_buffer: BufferId,
	document_generation: libaryon::core::StateId,
	identity: LocalViewIdentity,
	plan: crate::settings_writeback::CandidateEditPlan,
	witness: ArtifactReuseWitness,
	artifact_generation: u64,
}

#[cfg(feature = "toron")]
#[derive(Clone)]
struct PendingSettingsCommit {
	panel_id: libaryon::engine::HostPanelId,
	source_buffer: BufferId,
	revision: u64,
	candidate: crate::settings_writeback::CompiledCandidate,
	identity: LocalViewIdentity,
	continuation: crate::gui::messages::RuneCategoricalContinuation,
}

#[cfg(feature = "toron")]
impl RetainedRelationGeneration {
	fn witness(&self, authoritative_revision: u64) -> Option<ArtifactReuseWitness> {
		(self.revision == authoritative_revision && self.artifacts.iter().all(Option::is_some))
			.then(|| ArtifactReuseWitness {
				baseline: self.plan_key.clone(),
				artifact_generation: self.revision,
			})
	}
}

/// A local Rune run in flight: the revision it settles as, the document
/// generation it read, and its cancellation. Sending or dropping `cancel`
/// (rerun, buffer close, window close) abandons the run on its worker.
struct LocalRun {
	revision: u64,
	generation: libaryon::core::StateId,
	cancel: Option<tokio::sync::oneshot::Sender<()>>,
}

impl LocalRun {
	fn cancel(mut self) {
		if let Some(cancel) = self.cancel.take() {
			let _ = cancel.send(());
		}
	}
}

pub(crate) struct AppState {
	welcome: crate::first_run::Welcome,
	welcome_action: Option<welcome::Pending>,
	reporting: crate::reporting::Reporter,
	surface: surface::Surface,
	notebook_actions: notebook_actions::NotebookActions,
	connection_generation: u64,
	/// ADR 0166 §3: the generation of the connect attempt in flight, if any.
	connect_in_flight: Option<u64>,
	agents: agents::Agents,
	menu_state: MenuState,
	code_editor: CodeEditor,
	status_msg: String,
	status_error: String,
	status_df_size: Option<(usize, usize)>,
	status_time_elapsed: Option<Duration>,
	status_detail: Option<StatusDetail>,
	status_detail_open: bool,
	adapter_state: AdapterState,
	code_started: Instant,
	/// PRODUCT-0159 gate 5: armed at start by `POLARITON_LOOP_LATENCY=1`.
	loop_latency: Option<LoopLatencyProbe>,
	is_maximized: bool,
	showing_settings: bool,
	show_column_types: bool,
	window_size: Size,
	result_registry: ResultRegistry,
	rune_buffer_revisions: HashMap<BufferId, u64>,
	/// The local Rune run each buffer has in flight (`rune_local`).
	local_rune_runs: HashMap<BufferId, LocalRun>,
	/// Each buffer's last committed local batch: its revision and the
	/// (panel, plot incarnation) of every pane, so a `run_source` settles
	/// once exactly these figures, not earlier ones in the same panels, are
	/// on the surface.
	local_rune_commits: HashMap<BufferId, (u64, Vec<(libaryon::engine::HostPanelId, u64)>)>,
	#[cfg(feature = "toron")]
	rune_document_generations: HashMap<(BufferId, u64), libaryon::core::StateId>,
	#[cfg(feature = "toron")]
	/// PRODUCT-0154 §5: the compiled source text beside its presentation model,
	/// so a figure can carry the view expression that authored it.
	rune_presentation_sources: HashMap<(BufferId, u64), Arc<str>>,
	#[cfg(feature = "toron")]
	rune_presentation_models:
		HashMap<(BufferId, u64), Arc<crate::presentation_registry::PresentationSourceModel>>,
	#[cfg(feature = "toron")]
	retained_rune_relations: HashMap<BufferId, RetainedRelationGeneration>,
	#[cfg(feature = "toron")]
	settings_preview_sequence: u64,
	#[cfg(feature = "toron")]
	settings_preview_latest: HashMap<libaryon::engine::HostPanelId, u64>,
	#[cfg(feature = "toron")]
	settings_preview_in_flight: Option<u64>,
	#[cfg(feature = "toron")]
	settings_preview_pending: HashMap<libaryon::engine::HostPanelId, PendingSettingsPreview>,
	document_request_sequence: u64,
	#[cfg(feature = "toron")]
	pending_settings_commits: HashMap<libaryon::core::RequestId, PendingSettingsCommit>,
	#[cfg(feature = "toron")]
	rune_view_scheduler: RuneViewScheduler,
	/// Session-local categorical continuation keyed by producer, semantic view,
	/// and the natural length of the selected ordered palette.
	#[cfg(feature = "toron")]
	rune_categorical_identities: HashMap<(BufferId, LocalViewIdentity, usize), ColorIdentity>,
	#[cfg(toron_local)]
	local_creation: crate::local_workspace::CreationGrant,
	#[cfg(toron_local)]
	local_workspace_owner: Option<crate::local_workspace::LocalWorkspaceOwner>,
	#[cfg(toron_local)]
	closing_local_workspace: bool,
	pending_runs: VecDeque<PendingRun>,
	session_id: Option<SessionId>,
	saved_close_state: SavedCloseState,
	/// PRODUCT-0134 §7: the saved-connection store, holding its own secret
	/// session — never libaryon's document session.
	profile_store: crate::profile_store::ProfileStore,
	/// Where the store file lives; a field so tests can isolate it. `None`
	/// only when no configuration directory exists at all.
	profile_store_path: Option<std::path::PathBuf>,
	/// PRODUCT-0134 §8: `Some` only between a store-access request and its
	/// unlock, cancel, or failure. A session that never touches the store
	/// never sees a prompt.
	profile_unlock: Option<crate::profile_store::UnlockPrompt>,
	/// PRODUCT-0154 §7: the authored package directory, open only while the
	/// user is publishing.
	publish_prompt: Option<crate::publication::PublishPrompt>,
	/// The operation that asked for the store, applied once unlock succeeds
	/// and dropped on cancel.
	pending_profile_operation: Option<crate::profile_store::ProfileOperation>,
	/// PRODUCT-0135 §4: the SQL run generation. A shared atomic so a dispatch
	/// task can re-read the current value at lock-acquisition time without
	/// touching the adapter lock. Runs capture it before their task first
	/// awaits the lock; cancel advances it before anything else.
	sql_run_generation: Arc<std::sync::atomic::AtomicU64>,
	/// The in-flight SQL task's abort handle. Dropping a plain handle does not
	/// abort; only an explicit `abort()` on cancel does.
	sql_abort: Option<iced::task::Handle>,
	/// PRODUCT-0134 §4: `Some(original_name)` while the configuration dialog
	/// edits a stored profile. Save then replaces in place (or renames, which
	/// refuses onto an existing name); `None` makes Save a create, which
	/// refuses an existing name. The operation carries the intent — there is
	/// no overwrite prompt.
	editing_profile: Option<String>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum SavedCloseState {
	Open,
	Failed,
	Frozen,
}

struct StatusDetail {
	summary: String,
	full: String,
	content: iced::widget::text_editor::Content,
}

impl StatusDetail {
	fn new(summary: String, full: String) -> Self {
		let content = iced::widget::text_editor::Content::with_text(&full);
		Self {
			summary,
			full,
			content,
		}
	}
}

impl Drop for AppState {
	fn drop(&mut self) {
		// The application state is process-final. Do not turn teardown of
		// rust-analyzer or another language worker into visible shutdown
		// latency; Baryon's terminal frontend follows the same ownership rule.
		self.code_editor.detach_for_process_exit();
	}
}

#[derive(Clone, Copy)]
enum PendingRunKind {
	Buffer,
	Snippet,
}

struct PendingRun {
	snapshot_request: u64,
	source_buffer: BufferId,
	language_id: libaryon::engine::LanguageId,
	kind: PendingRunKind,
}

#[cfg(feature = "toron")]
#[derive(Clone)]
enum PreparedRuneCategoricalPalette {
	Ready {
		selection: CategoricalPaletteSelection,
		identity: ColorIdentity,
	},
	Failed(String),
}

pub type Result = std::result::Result<(), Box<dyn std::error::Error>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum StartupTarget {
	Ephemeral { initial_file: Option<String> },
	CreateSaved,
	OpenSaved(SessionId),
}

enum StartupAuthority {
	Ephemeral {
		initial_file: Option<String>,
	},
	#[cfg(feature = "toron")]
	Saved {
		session_id: SessionId,
		saved: SavedSession<Writable>,
	},
}

fn resolve_startup(target: StartupTarget) -> std::result::Result<StartupAuthority, String> {
	match target {
		StartupTarget::Ephemeral { initial_file } => {
			Ok(StartupAuthority::Ephemeral { initial_file })
		}
		#[cfg(feature = "toron")]
		StartupTarget::CreateSaved => {
			let saved = SessionStore::platform_default()
				.map_err(|error| format!("cannot locate the saved-session store: {error}"))?
				.create()
				.map_err(|error| format!("cannot create a saved session: {error}"))?;
			let session_id = saved.snapshot().session_id();
			Ok(StartupAuthority::Saved { session_id, saved })
		}
		#[cfg(feature = "toron")]
		StartupTarget::OpenSaved(session_id) => {
			let saved = SessionStore::platform_default()
				.map_err(|error| format!("cannot locate saved session {session_id}: {error}"))?
				.open_writable(session_id)
				.map_err(|error| format!("cannot open saved session {session_id}: {error}"))?;
			Ok(StartupAuthority::Saved { session_id, saved })
		}
		#[cfg(not(feature = "toron"))]
		StartupTarget::CreateSaved | StartupTarget::OpenSaved(_) => {
			Err("saved sessions require Polariton's `toron` feature".to_owned())
		}
	}
}

/// Build a planned Rune plot without blocking Iced's update loop. Keeping the
/// resolved product intact through the task preserves its composition and
/// owned snapshot until the destination surface receives it.
fn build_resolved_plot_task(
	resolved: ResolvedPlot,
	height: u32,
	on_ready: impl FnOnce(
		ResolvedPlot,
		std::sync::Arc<dyn crate::plot::common::PlotKernel + Send + Sync>,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	Task::perform(
		async move {
			let spec = resolved.resolved_spec.clone();
			let data = resolved.data.as_ref().clone();
			let artifact = std::sync::Arc::clone(&resolved.artifact);
			let kernel = tokio::task::spawn_blocking(move || {
				create_plot_from_artifact(&spec, &data, &artifact, 1200, height)
			})
			.await
			.expect("Rune plot build task panicked");
			(resolved, kernel)
		},
		move |(resolved, kernel)| on_ready(resolved, kernel),
	)
}

enum RenderedPlotExport {
	Svg(crate::plot::export::SvgFigure),
	Png(PngBackend),
	Avif(AvifBackend),
}

struct PlotExportJob {
	filename: String,
	artifact: RenderedPlotExport,
}

impl PlotExportJob {
	fn write_to(self, root: &std::path::Path) {
		let path = root.join(self.filename);
		match self.artifact {
			RenderedPlotExport::Svg(svg) => {
				let _ = std::fs::write(path, svg.svg);
			}
			RenderedPlotExport::Png(backend) => backend.save(&path),
			RenderedPlotExport::Avif(backend) => backend.save(&path),
		}
	}
}

fn render_plot_export(
	mut snapshot: crate::editor::HostPlotExport,
	format: ExportFormat,
	ordinal: usize,
) -> std::result::Result<PlotExportJob, String> {
	if snapshot.displayed_selection_revision != snapshot.selection_revision {
		let recipe = snapshot.recipe.as_ref().ok_or_else(|| {
			"Export · refused: the displayed plot is behind its selection and has no rebuild recipe"
				.to_owned()
		})?;
		let kernel = recipe.rebuild(&snapshot.predicate).map_err(|error| {
			format!("Export · refused: the selected plot could not be rebuilt: {error}")
		})?;
		snapshot.kernel = kernel;
		snapshot.render_revision = snapshot.render_revision.wrapping_add(1);
	}
	let width = snapshot.width;
	let height = snapshot.height;
	let bounds = iced::Rectangle {
		x: 0.0,
		y: 0.0,
		width,
		height,
	};
	let widget = crate::plot::common::PlotWidget::new(
		snapshot.kernel.as_ref(),
		snapshot.title,
		20.0,
		snapshot.settings_model,
		snapshot.render_revision,
		snapshot.resize_render_suspended,
		crate::plot::common::PlotRenderLayer::Full,
	);
	Ok(match format {
		ExportFormat::SVG => PlotExportJob {
			filename: format!("plot_{ordinal}.svg"),
			artifact: RenderedPlotExport::Svg(crate::plot::export::render_svg(&widget, bounds)),
		},
		ExportFormat::PNG => {
			let mut backend = PngBackend::new(width as u32, height as u32);
			widget.render(&mut backend, bounds);
			PlotExportJob {
				filename: format!("plot_{ordinal}.png"),
				artifact: RenderedPlotExport::Png(backend),
			}
		}
		ExportFormat::AVIF => {
			let mut backend = AvifBackend::new(width as u32, height as u32);
			widget.render(&mut backend, bounds);
			PlotExportJob {
				filename: format!("plot_{ordinal}.avif"),
				artifact: RenderedPlotExport::Avif(backend),
			}
		}
	})
}

#[cfg(feature = "toron")]
fn build_facet_resolved_plot_task(
	resolved: ResolvedPlot,
	source: Arc<crate::result_artifact::ResultArtifact>,
	facet: Option<String>,
	figure_grid: crate::plot::grid::FigureGridLayoutSettings,
	layer_settings: Option<crate::plot::spec::LayerSettings>,
	on_ready: impl FnOnce(
		ResolvedPlot,
		std::result::Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String>,
		crate::plot::recipe::PlotRecipe,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	let recipe = crate::plot::recipe::PlotRecipe::facet(
		Arc::clone(&source),
		Arc::new(resolved.resolved_spec.clone()),
		facet.clone(),
		figure_grid,
		layer_settings.clone(),
		1200,
		720,
	);
	Task::perform(
		async move {
			let building = resolved.clone();
			let result = tokio::task::spawn_blocking(move || {
				build_facet_resolved_plot_kernel(
					&building,
					source,
					facet,
					figure_grid,
					layer_settings,
				)
			})
			.await
			.map_err(|error| format!("facet plot build task panicked: {error}"))
			.and_then(|result| result);
			(resolved, result, recipe)
		},
		move |(resolved, result, recipe)| on_ready(resolved, result, recipe),
	)
}

/// The kernel a resolved (optionally faceted, optionally styled) plot builds
/// over `source`. Blocking; the Toron path runs it on a worker task and a
/// local batch runs it in its own worker before committing.
#[cfg(feature = "toron")]
fn build_facet_resolved_plot_kernel(
	resolved: &ResolvedPlot,
	source: Arc<crate::result_artifact::ResultArtifact>,
	facet: Option<String>,
	figure_grid: crate::plot::grid::FigureGridLayoutSettings,
	layer_settings: Option<crate::plot::spec::LayerSettings>,
) -> std::result::Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String> {
	let frame = source.projection().map_err(|error| error.to_string())?;
	let stamped = crate::plot::spec::StampedResult {
		data: source,
		provenance: resolved.resolved_spec.provenance.clone(),
	};
	let spec = Arc::new(resolved.resolved_spec.clone());
	let grid = match facet {
		Some(facet) => PlotGrid::value_facet_derived(spec, &stamped, facet)
			.map_err(|error| error.to_string())?,
		None => PlotGrid::single(spec, frame),
	};
	match layer_settings {
		Some(settings) => PlotGridKernel::from_styled_grid_with_figure_grid_from_artifact(
			grid,
			figure_grid,
			settings,
			stamped.data.as_ref(),
			1200,
			720,
		),
		None => Ok(PlotGridKernel::from_grid_with_figure_grid_from_artifact(
			grid,
			figure_grid,
			stamped.data.as_ref(),
			1200,
			720,
		)),
	}
	.map(|kernel| Arc::new(kernel) as Arc<dyn crate::plot::common::PlotKernel + Send + Sync>)
}

#[cfg(feature = "toron")]
fn build_facet_layered_plot_task(
	specification: crate::plot::spec::LayeredPlotSpec,
	source: Arc<crate::result_artifact::ResultArtifact>,
	facet: Option<String>,
	figure_grid: crate::plot::grid::FigureGridLayoutSettings,
	on_ready: impl FnOnce(
		std::result::Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String>,
		crate::plot::recipe::PlotRecipe,
	) -> Message
	+ Send
	+ 'static,
) -> Task<Message> {
	let recipe = crate::plot::recipe::PlotRecipe::layered(
		Arc::clone(&source),
		specification.clone(),
		facet.clone(),
		figure_grid,
		1200,
		720,
	);
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				build_facet_layered_plot_kernel(&specification, source, facet, figure_grid)
			})
			.await
			.map_err(|error| format!("facet layered plot build task panicked: {error}"))
			.and_then(|result| result)
		},
		move |result| on_ready(result, recipe),
	)
}

/// The kernel a layered plot builds over `source`; blocking, like
/// `build_facet_resolved_plot_kernel`.
#[cfg(feature = "toron")]
fn build_facet_layered_plot_kernel(
	specification: &crate::plot::spec::LayeredPlotSpec,
	source: Arc<crate::result_artifact::ResultArtifact>,
	facet: Option<String>,
	figure_grid: crate::plot::grid::FigureGridLayoutSettings,
) -> std::result::Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String> {
	let frame = source.projection().map_err(|error| error.to_string())?;
	let stamped = crate::plot::spec::StampedResult {
		data: source,
		provenance: crate::plot::spec::PlotProvenance {
			source_name: "workspace relation".to_owned(),
			revision: None,
		},
	};
	let binding = resolve_layered_channels(specification, frame.schema().as_ref())
		.map_err(|error| error.to_string())?;
	let resolved = crate::plot::spec::resolve_layered_domains(binding, &stamped)
		.map_err(|error| error.to_string())?;
	let body = Arc::new(resolved.layers.first().spec.clone());
	let grid = match facet {
		Some(facet) => PlotGrid::value_facet_derived(body, &stamped, facet)
			.map_err(|error| error.to_string())?,
		None => PlotGrid::single(body, Arc::clone(&frame)),
	};
	PlotGridKernel::from_grid_with_factory_and_figure_grid(
		grid,
		figure_grid,
		1200,
		720,
		|_, data| {
			create_layered_plot_from_artifact(specification, data, stamped.data.as_ref(), 1200, 720)
		},
	)
	.map(|kernel| Arc::new(kernel) as Arc<dyn crate::plot::common::PlotKernel + Send + Sync>)
}

fn subscription(state: &AppState) -> Subscription<Message> {
	let events = event::listen_with(|ev, _status, _window| match ev {
		event::Event::Keyboard(keyboard::Event::KeyPressed {
			key: keyboard::Key::Named(keyboard::key::Named::Enter),
			modifiers,
			..
		}) if modifiers.shift() || modifiers.control() => {
			if modifiers.shift() {
				Some(Message::RunSnippet)
			} else {
				Some(Message::Run)
			}
		}
		_ => None,
	});
	let mut subscriptions = vec![
		events,
		state.code_editor.subscription().map(Message::CodeEditEvent),
		window::resize_events().map(|(_, size)| Message::CodeEditResize(size)),
	];
	if state.loop_latency.is_some() {
		subscriptions
			.push(iced::time::every(LoopLatencyProbe::PERIOD).map(Message::LoopLatencyTick));
	}
	subscriptions.push(state.agents.subscription());
	Subscription::batch(subscriptions)
}

/// PRODUCT-0159 gate 5: an event-loop latency probe. A tick subscription is
/// armed at application start, before any fixture work. iced's timer starts
/// one period after the subscription, skips missed ticks, and yields tokio's
/// own scheduled instant for each delivered tick; that instant travels in the
/// message. Two measures per delivered tick, on one clock: *lateness*, now
/// minus the tick's scheduled instant, and *skipped*, the gap between this
/// tick's scheduled instant and the previous one beyond one period. A stall
/// shows in one or the other whichever instant tokio reports for the tick
/// that follows it, so the gate reads their maximum, *unserviced*. Nothing
/// is derived from a count of handled ticks, so skipped ticks never
/// accumulate into drift. The first tick's arrival after arming is reported
/// separately, not as lateness, because the timer's start is not the
/// arming instant.
struct LoopLatencyProbe {
	armed: Instant,
	first_handled: Option<Duration>,
	previous_scheduled: Option<Instant>,
	previous_lateness: Duration,
	ticks: u64,
	max_unserviced: Duration,
	over_100ms: u64,
}

impl LoopLatencyProbe {
	const PERIOD: Duration = Duration::from_millis(16);

	fn from_environment() -> Option<Self> {
		crate::plot::common::loop_latency_trace().map(|armed| {
			eprintln!("POLARITON_LOOP_LATENCY armed");
			Self::new(armed)
		})
	}

	fn new(armed: Instant) -> Self {
		Self {
			armed,
			first_handled: None,
			previous_scheduled: None,
			previous_lateness: Duration::ZERO,
			ticks: 0,
			max_unserviced: Duration::ZERO,
			over_100ms: 0,
		}
	}

	fn tick(&mut self, scheduled: Instant) {
		if let Some(unserviced) = self.observe(scheduled, Instant::now())
			&& unserviced == self.max_unserviced
		{
			self.report("new maximum");
		}
	}

	/// The pure half: returns the tick's unserviced time when it set a new
	/// maximum. Tested with synthetic instants for delayed and skipped
	/// delivery under both tokio reporting conventions.
	fn observe(&mut self, scheduled: Instant, now: Instant) -> Option<Duration> {
		self.ticks += 1;
		// The first tick is also measured against arming: the timer starts one
		// period after the subscription, so anything beyond that is time the
		// loop did not service events before the subscription even ran. It is
		// reported separately and it counts toward the maximum.
		let mut startup = Duration::ZERO;
		if self.first_handled.is_none() {
			let since_arming = now.saturating_duration_since(self.armed);
			self.first_handled = Some(since_arming);
			startup = since_arming.saturating_sub(Self::PERIOD);
			eprintln!(
				"POLARITON_LOOP_LATENCY first tick handled {:.1} ms after arming (timer starts one period after subscription; {:.1} ms counted as unserviced)",
				since_arming.as_secs_f64() * 1000.0,
				startup.as_secs_f64() * 1000.0
			);
		}
		let lateness = now.saturating_duration_since(scheduled);
		// The gap to the previous scheduled instant, net of the stall the
		// previous tick already reported as lateness, so one stall is counted
		// once whichever instant tokio reports for the tick after it.
		let skipped = self
			.previous_scheduled
			.map(|previous| {
				scheduled
					.saturating_duration_since(previous)
					.saturating_sub(Self::PERIOD)
					.saturating_sub(self.previous_lateness)
			})
			.unwrap_or(Duration::ZERO);
		self.previous_scheduled = Some(scheduled);
		self.previous_lateness = lateness;
		let unserviced = lateness.max(skipped).max(startup);
		if unserviced > Duration::from_millis(100) {
			self.over_100ms += 1;
		}
		if unserviced > self.max_unserviced {
			self.max_unserviced = unserviced;
			return Some(unserviced);
		}
		None
	}

	fn report(&self, why: &str) {
		eprintln!(
			"POLARITON_LOOP_LATENCY {why}: max_unserviced_ms={:.1} over_100ms={} ticks={}",
			self.max_unserviced.as_secs_f64() * 1000.0,
			self.over_100ms,
			self.ticks
		);
	}
}

impl Drop for LoopLatencyProbe {
	fn drop(&mut self) {
		self.report("exit");
	}
}

pub fn run(startup_data: StartupData, target: StartupTarget, accepting_agents: bool) -> Result {
	let size = Size::new(startup_data.window_size.0, startup_data.window_size.1);
	let authority = resolve_startup(target)
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidInput, error))?;
	let root = crate::reporting::state_dir();
	let show_welcome = root.as_ref().is_some_and(|root| {
		crate::first_run::Welcome::eligible(
			root,
			matches!(
				&authority,
				StartupAuthority::Ephemeral { initial_file: None }
			),
		)
	});
	let destination = startup_data.reporting.clone();
	let mut initial = new_with_authority_mode(startup_data, authority, show_welcome)
		.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?;
	if let Some(root) = root {
		initial.0.reporting = crate::reporting::Reporter::start(root.clone(), destination.clone());
		initial
			.0
			.reporting
			.emit(crate::reporting::Step::RunOpened, None, false);
		if show_welcome {
			initial.0.welcome.marker = Some(root.join("first-run-shown"));
		}
	}
	initial.0.welcome.reporting_on = destination.is_some();
	initial
		.0
		.advertise_window(accepting_agents)
		.map_err(std::io::Error::other)?;
	let initial = std::sync::Mutex::new(Some(initial));
	let initialize = move || {
		initial
			.lock()
			.expect("Polariton boot state lock poisoned")
			.take()
			.expect("Polariton boot state may be consumed exactly once")
	};
	application(initialize, update, view)
		.subscription(subscription)
		.theme(components::theme())
		.title("Polariton")
		// Canvas geometry (plot marks, gridlines, legend swatches) is
		// tessellated triangles; without MSAA every curved edge is aliased
		// hard enough to read as rasterization. Text is unaffected either way.
		.antialiasing(true)
		.font(DEJAVU_SANS_MONO)
		// PRODUCT-0117: plot canvas text names this bundled proportional face
		// explicitly. Loading it here makes that operation portable without
		// changing Iced's application default for editor and GUI text.
		.font(POLARITON_PLOT_SANS_REGULAR)
		.window(window::Settings {
			decorations: false,
			maximized: false,
			min_size: Some(Size::new(1280.0, 720.0)),
			position: window::Position::Centered,
			resizable: true,
			size,
			transparent: false,
			..Default::default()
		})
		.run()?;
	Ok(())
}

#[cfg(test)]
fn new(startup_data: StartupData, initial_file: Option<String>) -> (AppState, Task<Message>) {
	new_with_authority(startup_data, StartupAuthority::Ephemeral { initial_file })
		.expect("ephemeral startup cannot refuse")
}

#[cfg(test)]
fn new_with_authority(
	startup_data: StartupData,
	authority: StartupAuthority,
) -> std::result::Result<(AppState, Task<Message>), String> {
	new_with_discovery(startup_data, authority, true)
}
fn new_with_discovery(
	startup_data: StartupData,
	authority: StartupAuthority,
	discover: bool,
) -> std::result::Result<(AppState, Task<Message>), String> {
	new_with_options(startup_data, authority, false, discover)
}
fn new_with_authority_mode(
	startup_data: StartupData,
	authority: StartupAuthority,
	welcome: bool,
) -> std::result::Result<(AppState, Task<Message>), String> {
	new_with_options(startup_data, authority, welcome, !welcome)
}
fn new_with_options(
	startup_data: StartupData,
	authority: StartupAuthority,
	welcome: bool,
	_discover: bool,
) -> std::result::Result<(AppState, Task<Message>), String> {
	let window_size = Size::new(startup_data.window_size.0, startup_data.window_size.1);
	let init_error = startup_data.init_error.clone();
	let init_summary = init_error
		.as_ref()
		.map(|_| "init.rn · configuration failed".to_owned())
		.unwrap_or_default();
	let init_detail = init_error.map(|error| StatusDetail::new(init_summary.clone(), error));
	let init_script = configuration::default_init_path();
	let pane_divider_reservation = startup_data.pane_divider_reservation;
	let workspace_size = workspace_bounds(window_size).size();
	let (mut code_editor, session_id) = match authority {
		StartupAuthority::Ephemeral { initial_file } => (
			match initial_file {
				Some(path) => CodeEditor::open_initial_file_with_init(
					path,
					pane_divider_reservation,
					init_script,
					workspace_size,
				),
				None => CodeEditor::new_with_init(
					"",
					pane_divider_reservation,
					init_script,
					workspace_size,
				),
			},
			None,
		),
		#[cfg(feature = "toron")]
		StartupAuthority::Saved { session_id, saved } => (
			CodeEditor::open_saved_with_init(
				saved,
				pane_divider_reservation,
				init_script,
				workspace_size,
			)
			.map_err(|error| format!("cannot restore saved session {session_id}: {error}"))?,
			Some(session_id),
		),
	};
	code_editor.set_show_column_types(startup_data.show_column_types);
	if welcome {
		code_editor.open_welcome_notebook(crate::first_run::EXAMPLE);
	}
	let mut state = AppState {
		welcome: crate::first_run::Welcome::default(),
		welcome_action: None,
		reporting: crate::reporting::Reporter::default(),
		surface: surface::Surface::default(),
		notebook_actions: notebook_actions::NotebookActions::default(),
		connection_generation: 0,
		connect_in_flight: None,
		agents: agents::Agents::default(),
		menu_state: MenuState::default(),
		code_editor,
		status_msg: "".to_string(),
		status_error: init_summary,
		status_df_size: None,
		status_time_elapsed: None,
		status_detail: init_detail,
		status_detail_open: false,
		adapter_state: AdapterState::default(),
		code_started: Instant::now(),
		loop_latency: LoopLatencyProbe::from_environment(),
		is_maximized: false,
		showing_settings: false,
		show_column_types: startup_data.show_column_types,
		window_size,
		result_registry: ResultRegistry::default(),
		rune_buffer_revisions: HashMap::new(),
		local_rune_runs: HashMap::new(),
		local_rune_commits: HashMap::new(),
		#[cfg(feature = "toron")]
		rune_document_generations: HashMap::new(),
		#[cfg(feature = "toron")]
		rune_presentation_models: HashMap::new(),
		#[cfg(feature = "toron")]
		rune_presentation_sources: HashMap::new(),
		#[cfg(feature = "toron")]
		retained_rune_relations: HashMap::new(),
		#[cfg(feature = "toron")]
		settings_preview_sequence: 0,
		#[cfg(feature = "toron")]
		settings_preview_latest: HashMap::new(),
		#[cfg(feature = "toron")]
		settings_preview_in_flight: None,
		#[cfg(feature = "toron")]
		settings_preview_pending: HashMap::new(),
		document_request_sequence: 0,
		#[cfg(feature = "toron")]
		pending_settings_commits: HashMap::new(),
		#[cfg(feature = "toron")]
		rune_view_scheduler: RuneViewScheduler::default(),
		#[cfg(feature = "toron")]
		rune_categorical_identities: HashMap::new(),
		#[cfg(toron_local)]
		local_creation: crate::local_workspace::CreationGrant::default(),
		#[cfg(toron_local)]
		local_workspace_owner: None,
		#[cfg(toron_local)]
		closing_local_workspace: false,
		pending_runs: VecDeque::new(),
		session_id,
		saved_close_state: SavedCloseState::Open,
		profile_store: crate::profile_store::ProfileStore::default(),
		profile_store_path: crate::profile_store::ProfileStore::default_path(),
		profile_unlock: None,
		publish_prompt: None,
		pending_profile_operation: None,
		editing_profile: None,
		sql_run_generation: Arc::new(std::sync::atomic::AtomicU64::new(0)),
		sql_abort: None,
	};
	state.welcome.active = welcome;
	state.welcome.original_source = crate::first_run::EXAMPLE.into();
	sync_workspace_viewport(&mut state);
	// Eagerly open the session scratch database so SQL always has a target;
	// an in-memory SQLite open is effectively instant.
	let scratch = Task::perform(AdapterState::open_scratch(), Message::ScratchReady);
	#[cfg(toron_local)]
	{
		let discovery = if _discover {
			Task::perform(
				nilket_local::discover(None),
				Message::LocalWorkspaceDiscovered,
			)
		} else {
			Task::none()
		};
		Ok((state, Task::batch([scratch, discovery])))
	}
	#[cfg(not(toron_local))]
	{
		Ok((state, scratch))
	}
}

fn view(app_state: &AppState) -> Element<'_, Message> {
	let status_detail = app_state.status_detail.as_ref().and_then(|detail| {
		let visible_summary = if app_state.status_error.is_empty() {
			&app_state.status_msg
		} else {
			&app_state.status_error
		};
		(detail.summary == *visible_summary).then_some(detail)
	});
	let screen = components::main_screen(
		&app_state.menu_state,
		&app_state.code_editor,
		&app_state.status_msg,
		&app_state.status_error,
		app_state.status_df_size,
		app_state.status_time_elapsed,
		app_state.code_editor.busy_spinner_phase(),
		&app_state.adapter_state,
		app_state.showing_settings,
		app_state.show_column_types,
		status_detail.map(|detail| (detail.full.as_str(), &detail.content)),
		app_state.status_detail_open,
		#[cfg(toron_local)]
		app_state.local_creation.offer(),
		#[cfg(not(toron_local))]
		None,
		app_state.session_id.is_some(),
		app_state.saved_close_state == SavedCloseState::Failed,
		app_state.profile_unlock.as_ref(),
		app_state.publish_prompt.as_ref(),
		// `None` while locked: names live inside the ciphertext, so no list
		// can render before unlock (PRODUCT-0134 §8).
		app_state
			.profile_store
			.names()
			.ok()
			.map(|names| names.into_iter().map(str::to_owned).collect()),
		// PRODUCT-0135 §6: the Cancel affordance exists only while a run is
		// in flight.
		app_state.sql_abort.is_some(),
		app_state.agents.status(),
	);
	welcome::view(app_state, screen)
}

fn session_identity_text(session_id: SessionId) -> String {
	session_id.to_string()
}

fn compact_sql_diagnostic(message: &str, source_start_line: Option<usize>) -> (String, String) {
	let mut body = message.trim();
	if let Some((prefix, rest)) = body.split_once(" parse error: ")
		&& !prefix.contains('\n')
	{
		body = rest;
	}
	if let Some(rest) = body.strip_prefix("sql parser error: ") {
		body = rest;
	}

	let mut parser_line = None;
	let mut parser_column = None;
	let mut core = body;
	if let Some((before, location)) = body.rsplit_once(" at Line: ")
		&& let Some((line, column)) = location.split_once(", Column: ")
	{
		parser_line = line.trim().parse::<usize>().ok();
		parser_column = column
			.split_whitespace()
			.next()
			.and_then(|column| column.parse::<usize>().ok());
		core = before;
	}

	let core = core.split_whitespace().collect::<Vec<_>>().join(" ");
	let core = if let Some(rest) = core.strip_prefix("Expected: ") {
		if let Some((expected, found)) = rest.split_once(", found: ") {
			format!("expected {expected}, found '{}'", found.trim_matches('\''))
		} else {
			format!("expected {rest}")
		}
	} else {
		core
	};
	let location = match (parser_line, parser_column) {
		(Some(line), Some(column)) => {
			let line = source_start_line
				.map(|source| source.saturating_add(line.saturating_sub(1)))
				.unwrap_or(line);
			format!("line {line}, col {column} · ")
		}
		(Some(line), None) => {
			let line = source_start_line
				.map(|source| source.saturating_add(line.saturating_sub(1)))
				.unwrap_or(line);
			format!("line {line} · ")
		}
		_ => String::new(),
	};
	let summary = format!("SQL · {location}{core}");
	let full = if source_start_line.is_some() && parser_line.is_some() {
		format!("{summary}\n\nAdapter diagnostic:\n{message}")
	} else {
		message.to_owned()
	};
	(summary, full)
}

fn set_sql_status(app_state: &mut AppState, summary: impl Into<String>) {
	app_state.status_msg = summary.into();
	app_state.status_error.clear();
	app_state.status_detail = None;
	app_state.status_detail_open = false;
}

fn set_sql_error(app_state: &mut AppState, message: String, source_start_line: Option<usize>) {
	let (summary, full) = compact_sql_diagnostic(&message, source_start_line);
	app_state.status_msg.clear();
	app_state.status_error = summary.clone();
	app_state.status_detail = Some(StatusDetail::new(summary, full));
	app_state.status_detail_open = false;
}

fn compact_rune_diagnostic(message: &str) -> (String, String) {
	let lines = message
		.lines()
		.map(str::trim)
		.filter(|line| !line.is_empty())
		.collect::<Vec<_>>();
	let first = lines.first().copied().unwrap_or("unknown failure");
	let first = first.split_whitespace().collect::<Vec<_>>().join(" ");
	let first = first.strip_prefix("Error: ").unwrap_or(&first);
	let first = if let Some(rest) = first.strip_prefix("Rune view syntax at bytes ")
		&& let Some((span, diagnostic)) = rest.split_once(": ")
	{
		format!("bytes {span} · {diagnostic}")
	} else if let Some(rest) = first.strip_prefix("Rune view syntax: ") {
		rest.to_owned()
	} else {
		first.to_owned()
	};
	let more = lines.len().saturating_sub(1);
	let summary = if more == 0 {
		format!("Rune · {first}")
	} else {
		format!(
			"Rune · {first} · {more} more diagnostic{}",
			if more == 1 { "" } else { "s" }
		)
	};
	(summary, message.to_owned())
}

fn set_rune_error(app_state: &mut AppState, message: String) {
	let (summary, full) = compact_rune_diagnostic(&message);
	app_state.status_msg.clear();
	app_state.status_error = summary.clone();
	app_state.status_detail = Some(StatusDetail::new(summary, full));
	app_state.status_detail_open = false;
}

fn clear_transient_execution_status_on_editor_activity(
	app_state: &mut AppState,
	message: &crate::editor::EditorMsg,
) {
	let is_activity = matches!(
		message,
		crate::editor::EditorMsg::ActivateDocTab { .. }
			| crate::editor::EditorMsg::Click
			| crate::editor::EditorMsg::CloseDocTab { .. }
			| crate::editor::EditorMsg::Input(_)
			| crate::editor::EditorMsg::MinimapGoto { .. }
			| crate::editor::EditorMsg::Paste(_)
			| crate::editor::EditorMsg::Scroll(_)
	);
	if is_activity
		&& (app_state.status_msg.starts_with("SQL · ")
			|| app_state.status_error.starts_with("SQL · ")
			|| app_state.status_msg.starts_with("Rune · ")
			|| app_state.status_error.starts_with("Rune · ")
			|| app_state.status_msg.starts_with("Selection · ")
			|| app_state.status_error.starts_with("Selection · "))
	{
		app_state.status_msg.clear();
		app_state.status_error.clear();
		app_state.status_detail = None;
		app_state.status_detail_open = false;
	}
}

impl AppState {
	fn current_sql_generation(&self) -> u64 {
		self.sql_run_generation
			.load(std::sync::atomic::Ordering::SeqCst)
	}
}

/// PRODUCT-0135 §4: the one dispatch future both SQL paths share. The run's
/// generation was captured before this future was built — and therefore
/// before it first awaits the adapter lock — and is re-checked once the lock
/// is acquired, so a task that waited across a cancellation dies at the gate
/// instead of dispatching.
async fn dispatch_sql_gated(
	db: Arc<tokio::sync::RwLock<dyn crate::adapters::common::DatabaseAdapter>>,
	code: String,
	generation: u64,
	current: Arc<std::sync::atomic::AtomicU64>,
	producer: String,
) -> ExecutionResult {
	let mut guard = db.write().await;
	if current.load(std::sync::atomic::Ordering::SeqCst) != generation {
		return ExecutionResult::None;
	}
	let result = guard.dispatch(&code).await;
	// PRODUCT-0154 §4: provenance is a birth fact bound where the adapter is
	// known, never inferred later from whatever adapter is selected then.
	if let ExecutionResult::Rows { artifact, .. } = &result {
		artifact.bind_producer(&producer);
	}
	result
}

/// PRODUCT-0135 §4/§5: cancellation's host half, in the contract's order —
/// the generation advances first, before any signal or teardown, so by the
/// time any guard releases every waiting task is already stale; then the
/// undispatched queue drains. The in-flight abort and the engine-side signal
/// join in the cancellation surface stage.
fn cancel_sql_generation(app_state: &mut AppState) {
	app_state
		.sql_run_generation
		.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
	app_state.pending_runs.clear();
}

fn run_code(
	app_state: &mut AppState,
	source_buffer: BufferId,
	code: String,
	annotation_source: &str,
) -> Task<Message> {
	let Some(db) = app_state.adapter_state.sql_connection() else {
		set_sql_error(
			app_state,
			"No SQL target: scratch database unavailable.".to_owned(),
			None,
		);
		return Task::none();
	};
	if code.trim().is_empty() {
		set_sql_status(app_state, "SQL · nothing to run on the current line");
		return Task::none();
	}
	let publish_name = match parse_result_annotation(annotation_source) {
		Ok(name) => name,
		Err(message) => {
			set_sql_error(app_state, message, None);
			return Task::none();
		}
	};
	let db = db.clone();
	let connection = app_state.adapter_state.connection_identity();
	app_state.code_editor.retire_result_panel_publication();
	let publish = publish_name.map(|name| {
		if let Some(publication) = app_state.result_registry.publication(&name) {
			app_state.code_editor.retire_publication(publication);
		}
		app_state
			.result_registry
			.begin(&name, source_buffer, connection)
	});
	set_sql_status(app_state, "SQL · running…");
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	let generation = app_state.current_sql_generation();
	let current = Arc::clone(&app_state.sql_run_generation);
	let (task, handle) = Task::perform(
		dispatch_sql_gated(
			db,
			code,
			generation,
			current,
			app_state
				.adapter_state
				.selection
				.adapter_type_str()
				.to_owned(),
		),
		move |result| Message::RunResult {
			result,
			publish,
			generation,
		},
	)
	.abortable();
	app_state.sql_abort = Some(handle);
	Task::batch([
		app_state
			.code_editor
			.take_selection_tasks()
			.map(Message::CodeEditEvent),
		task,
	])
}

fn run_language_code(
	app_state: &mut AppState,
	source_buffer: BufferId,
	language_id: libaryon::engine::LanguageId,
	code: String,
	annotation_source: &str,
	document_generation: libaryon::core::StateId,
) -> Task<Message> {
	match language_id.as_str() {
		"sql" => run_code(app_state, source_buffer, code, annotation_source),
		"rune" => run_rune_code_at_generation(app_state, source_buffer, code, document_generation),
		language => {
			app_state.status_msg = format!("No executor is registered for language: {language}");
			Task::none()
		}
	}
}

#[cfg(all(test, feature = "toron"))]
fn run_rune_code(app_state: &mut AppState, source_buffer: BufferId, code: String) -> Task<Message> {
	run_rune_code_at_generation(
		app_state,
		source_buffer,
		code,
		libaryon::core::StateId::ZERO,
	)
}

fn run_rune_code_at_generation(
	app_state: &mut AppState,
	source_buffer: BufferId,
	code: String,
	document_generation: libaryon::core::StateId,
) -> Task<Message> {
	// Any new run of the buffer, even of empty text, supersedes the previous
	// one: cancel a local run in flight, and advance the revision so a result
	// already queued (which cancellation cannot retract) fails its check.
	if let Some(run) = app_state.local_rune_runs.remove(&source_buffer) {
		run.cancel();
	}
	let revision = *app_state
		.rune_buffer_revisions
		.entry(source_buffer)
		.and_modify(|revision| *revision = revision.saturating_add(1))
		.or_insert(1);
	if code.trim().is_empty() {
		app_state.status_msg = "Nothing to run in the current Rune buffer.".into();
		return Task::none();
	}
	if crate::rune_local::mode(&code).is_local() {
		return run_local_rune_code(app_state, source_buffer, revision, code, document_generation);
	}
	#[cfg(feature = "toron")]
	{
		app_state
			.rune_document_generations
			.retain(|(buffer, candidate), _| *buffer != source_buffer || *candidate >= revision);
		app_state
			.rune_document_generations
			.insert((source_buffer, revision), document_generation);
		let work = PendingRunePresentation::new(source_buffer, revision, code);
		let Some(work) = app_state.rune_view_scheduler.submit_presentation(work) else {
			app_state.status_msg = "Rune presentation queued newest revision…".into();
			return Task::none();
		};
		start_rune_presentation_task(app_state, work)
	}
	#[cfg(not(feature = "toron"))]
	{
		// Only the Toron presentation path pins the document generation.
		let _ = document_generation;
		run_plain_rune_code(app_state, source_buffer, revision, code)
	}
}

/// A source with its own `main` runs here on rnx, never through Toron. Its
/// outcome settles through the same message as a plain buffer, so the one-plot
/// rule, stale-revision checks and `run_source` completion are shared.
fn run_local_rune_code(
	app_state: &mut AppState,
	source_buffer: BufferId,
	revision: u64,
	code: String,
	document_generation: libaryon::core::StateId,
) -> Task<Message> {
	// The generation this run reads: the caller's, or the buffer's current one
	// when the caller did not pin it. Its output is published only while the
	// buffer is still open at that generation.
	let generation = if document_generation == libaryon::core::StateId::ZERO {
		match app_state.code_editor.document_snapshot(source_buffer) {
			Some((_, generation)) => generation,
			None => {
				set_rune_error(app_state, "the source text is not yet projected".to_owned());
				return Task::none();
			}
		}
	} else {
		document_generation
	};
	let capabilities = RuneCapabilities {
		results: match app_state.result_registry.snapshot() {
			Ok(results) => results,
			Err(error) => {
				app_state.status_error = error.to_string();
				return Task::none();
			}
		},
		..RuneCapabilities::default()
	};
	let (cancel, cancelled) = tokio::sync::oneshot::channel();
	app_state.local_rune_runs.insert(
		source_buffer,
		LocalRun {
			revision,
			generation,
			cancel: Some(cancel),
		},
	);
	// The colour identities this buffer's views retained, snapshotted for the
	// worker; the commit writes back what the batch continued.
	#[cfg(feature = "toron")]
	let colours: HashMap<(LocalViewIdentity, usize), ColorIdentity> = app_state
		.rune_categorical_identities
		.iter()
		.filter(|((buffer, _, _), _)| *buffer == source_buffer)
		.map(|((_, identity, len), colour)| ((identity.clone(), *len), colour.clone()))
		.collect();
	app_state.status_msg = "Rune running…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	let name = format!("<buffer {}>", source_buffer.get());
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				let execution = crate::rune_local::execute(&name, &code, capabilities, cancelled)?;
				// Stage the whole batch here, off the update loop: a plan or
				// kernel that fails fails the batch before any pane changes.
				// Plot requests and views sets build in the order the source
				// produced them, which is the order their panes take.
				enum Output {
					Request(usize, crate::plot::request::PlotRequest),
					#[cfg(feature = "toron")]
					Views(crate::rune_host::LocalViewSet),
				}
				let mut outputs: Vec<(usize, Output)> = execution
					.requests
					.into_iter()
					.enumerate()
					.zip(execution.request_order)
					.map(|((index, request), order)| (order, Output::Request(index, request)))
					.collect();
				#[cfg(feature = "toron")]
				outputs.extend(execution.views.into_iter().map(|set| (set.order, Output::Views(set))));
				outputs.sort_by_key(|(order, _)| *order);
				let mut panes = Vec::with_capacity(outputs.len());
				for (_, output) in outputs {
					match output {
						Output::Request(index, request) => {
							let resolved = plan(request)?;
							let kernel = crate::plot::factory::create_plot_from_artifact(
								&resolved.resolved_spec,
								&resolved.data,
								&resolved.artifact,
								1200,
								1200,
							);
							let recipe = crate::plot::recipe::PlotRecipe::single(
								Arc::clone(&resolved.artifact),
								resolved.resolved_spec.clone(),
								1200,
								720,
							);
							panes.push(crate::gui::messages::BuiltPane {
								identity: LocalViewIdentity::Anonymous(index),
								presentation: ViewPresentation {
									content_kind: ViewContentKind::Plot,
									display_label: resolved.source_name.clone(),
									authored_title: crate::plot::spec::Presence::Inherit,
								},
								settings: PlotSettingsLayer::default(),
								kernel,
								recipe,
								#[cfg(feature = "toron")]
								continuation: None,
							});
						}
						#[cfg(feature = "toron")]
						Output::Views(set) => panes.extend(build_local_views(set, &colours)?),
					}
				}
				Ok(crate::gui::messages::RuneBufferOutcome {
					display: execution.display,
					plot: None,
					panes: Some(panes),
				})
			})
			.await
			.map_err(|error| format!("Rune task panicked: {error}"))?
		},
		move |result| Message::RunRuneBufferResult {
			source_buffer,
			revision,
			result,
		},
	)
}

/// Whether a local run's output may still be published. `None` when the
/// revision is not a local run. A run whose buffer closed or changed since
/// it read the text is removed and reported, and its output is discarded
/// before any pane is prepared or attached.
fn local_run_is_current(
	app_state: &mut AppState,
	source_buffer: BufferId,
	revision: u64,
) -> Option<bool> {
	let run = app_state
		.local_rune_runs
		.get(&source_buffer)
		.filter(|run| run.revision == revision)?;
	let current = app_state
		.code_editor
		.document_snapshot(source_buffer)
		.is_some_and(|(_, generation)| generation == run.generation);
	if !current {
		if let Some(run) = app_state.local_rune_runs.remove(&source_buffer) {
			run.cancel();
		}
		app_state.status_msg =
			"The source changed while Rune ran; its output was discarded.".to_owned();
		app_state
			.code_editor
			.mark_rune_view_producer_stale(source_buffer);
	}
	Some(current)
}

/// Cancel local runs whose buffer is no longer open, and invalidate their
/// pending results, which are then dropped by the revision check.
fn reap_closed_local_runs(app_state: &mut AppState) {
	let closed: Vec<BufferId> = app_state
		.local_rune_runs
		.keys()
		.filter(|buffer| app_state.code_editor.document_snapshot(**buffer).is_none())
		.copied()
		.collect();
	for buffer in closed {
		if let Some(run) = app_state.local_rune_runs.remove(&buffer) {
			run.cancel();
		}
		app_state.rune_buffer_revisions.remove(&buffer);
		app_state.local_rune_commits.remove(&buffer);
	}
}

/// Resolve one `frame.views([...])` set through the same planner a view
/// source's relation uses, and build every kernel. Blocking; any failure,
/// including a table view (not supported locally yet), fails the batch.
#[cfg(feature = "toron")]
fn build_local_views(
	set: crate::rune_host::LocalViewSet,
	colours: &HashMap<(LocalViewIdentity, usize), ColorIdentity>,
) -> std::result::Result<Vec<crate::gui::messages::BuiltPane>, String> {
	use crate::gui::messages::RuneRelationSinkOutput;
	let palettes = categorical_palettes_for(&set.views, |identity, len| {
		colours.get(&(identity.clone(), len)).cloned()
	});
	// Resolution errors (an unknown skin, a missing column) are located at the
	// `view(...)` call that declared the failing view.
	let (source_name, source) = (set.source_name, set.source);
	let spans: HashMap<LocalViewIdentity, usize> = set
		.views
		.iter()
		.map(|view| (view.identity.clone(), view.span.start as usize))
		.collect();
	let at = |identity: &LocalViewIdentity, message: String| {
		crate::rune_local::locate(
			&source_name,
			&source,
			spans.get(identity).copied().unwrap_or(0),
			&message,
		)
	};
	let outcome = rune_view_outcome_with_palettes(set.artifact, set.views, palettes)?;
	let mut panes = Vec::with_capacity(outcome.sinks.len());
	for sink in outcome.sinks {
		let name = match &sink.identity {
			LocalViewIdentity::Named(name) => name.clone(),
			LocalViewIdentity::Anonymous(index) => index.to_string(),
		};
		let output = sink
			.output
			.map_err(|error| at(&sink.identity, format!("view \"{name}\": {error}")))?;
		let (settings, kernel, recipe, continuation) = match output {
			RuneRelationSinkOutput::Table(_) => {
				return Err(format!(
					"view \"{name}\" is a table; a local source publishes plot views only for now"
				));
			}
			RuneRelationSinkOutput::Plot(resolved, settings, continuation) => {
				let recipe = crate::plot::recipe::PlotRecipe::facet(
					Arc::clone(&sink.source),
					Arc::new(resolved.resolved_spec.clone()),
					sink.facet.clone(),
					sink.figure_grid,
					None,
					1200,
					720,
				);
				let kernel = build_facet_resolved_plot_kernel(
					&resolved,
					Arc::clone(&sink.source),
					sink.facet.clone(),
					sink.figure_grid,
					None,
				)
				.map_err(|error| format!("view \"{name}\": {error}"))?;
				(settings, kernel, recipe, Some(continuation))
			}
			RuneRelationSinkOutput::StyledPlot(resolved, layer, settings, continuation) => {
				let recipe = crate::plot::recipe::PlotRecipe::facet(
					Arc::clone(&sink.source),
					Arc::new(resolved.resolved_spec.clone()),
					sink.facet.clone(),
					sink.figure_grid,
					Some(layer.clone()),
					1200,
					720,
				);
				let kernel = build_facet_resolved_plot_kernel(
					&resolved,
					Arc::clone(&sink.source),
					sink.facet.clone(),
					sink.figure_grid,
					Some(layer),
				)
				.map_err(|error| format!("view \"{name}\": {error}"))?;
				(settings, kernel, recipe, Some(continuation))
			}
			RuneRelationSinkOutput::LayeredPlot(specification, settings) => {
				let recipe = crate::plot::recipe::PlotRecipe::layered(
					Arc::clone(&sink.source),
					specification.clone(),
					sink.facet.clone(),
					sink.figure_grid,
					1200,
					720,
				);
				let kernel = build_facet_layered_plot_kernel(
					&specification,
					Arc::clone(&sink.source),
					sink.facet.clone(),
					sink.figure_grid,
				)
				.map_err(|error| format!("view \"{name}\": {error}"))?;
				(settings, kernel, recipe, None)
			}
		};
		panes.push(crate::gui::messages::BuiltPane {
			identity: sink.identity,
			presentation: sink.presentation,
			settings,
			kernel,
			recipe,
			continuation,
		});
	}
	Ok(panes)
}

/// Why a local source's plot settings cannot be edited from the pane.
#[cfg(feature = "toron")]
const LOCAL_WRITEBACK_REFUSAL: &str =
	"settings of a local source are edited in its source; rerun it to apply them";

/// Replace the buffer's panes with a local run's batch in one step: exactly
/// these panes, in publication order, titled by their frames. Panes the
/// previous batch had beyond these are retired.
fn commit_local_panes(
	app_state: &mut AppState,
	source_buffer: BufferId,
	revision: u64,
	panes: Vec<crate::gui::messages::BuiltPane>,
) {
	let views = panes
		.iter()
		.map(|pane| (pane.identity.clone(), pane.presentation.clone()))
		.collect::<Vec<_>>();
	app_state
		.code_editor
		.prepare_rune_view_panels(source_buffer, &views, revision);
	let count = panes.len();
	let mut committed = Vec::with_capacity(count);
	for pane in panes {
		let panel_id = app_state
			.code_editor
			.rune_view_panel_id(source_buffer, &pane.identity)
			.expect("a committed local pane was prepared above");
		app_state.code_editor.set_rune_view_plot_with_recipe(
			panel_id,
			pane.presentation.display_label,
			pane.kernel,
			pane.settings,
			pane.recipe,
		);
		// A local source's figures are not source-attached: its presentation
		// may be computed from fetched data, and a preview could not rerun
		// `main` without fetching again. Settings stay read-only, and say why.
		#[cfg(feature = "toron")]
		if matches!(pane.identity, LocalViewIdentity::Named(_)) {
			app_state
				.code_editor
				.set_plot_writeback_status(panel_id, Some(LOCAL_WRITEBACK_REFUSAL.to_owned()));
		}
		#[cfg(feature = "toron")]
		if let Some(continuation) = pane.continuation {
			app_state.rune_categorical_identities.insert(
				(source_buffer, pane.identity.clone(), continuation.palette_len),
				continuation.identity,
			);
		}
		if let Some(incarnation) = app_state.code_editor.host_plot_incarnation(panel_id) {
			committed.push((panel_id, incarnation));
		}
	}
	app_state
		.local_rune_commits
		.insert(source_buffer, (revision, committed));
	app_state.status_msg = match count {
		0 => "Rune finished.".to_owned(),
		1 => "Plotted 1 pane.".to_owned(),
		count => format!("Plotted {count} panes."),
	};
}

fn run_plain_rune_code(
	app_state: &mut AppState,
	source_buffer: BufferId,
	revision: u64,
	code: String,
) -> Task<Message> {
	let capabilities = RuneCapabilities {
		results: match app_state.result_registry.snapshot() {
			Ok(results) => results,
			Err(error) => {
				app_state.status_error = error.to_string();
				return Task::none();
			}
		},
		..RuneCapabilities::default()
	};
	app_state.status_msg = "Rune running…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				let mut execution = rune_host::execute_body(&code, capabilities)?;
				let plot = match execution.requests.len() {
					0 => None,
					1 => Some(plan(execution.requests.remove(0))?),
					count => {
						return Err(format!(
							"Rune buffer published {count} plots; one buffer currently owns one plot pane."
						));
					}
				};
				Ok(crate::gui::messages::RuneBufferOutcome {
					display: execution.display,
					plot,
					panes: None,
				})
			})
			.await
			.map_err(|error| format!("Rune task panicked: {error}"))?
		},
		move |result| Message::RunRuneBufferResult {
			source_buffer,
			revision,
			result,
		},
	)
}

#[cfg(feature = "toron")]
fn start_rune_presentation_task(
	app_state: &mut AppState,
	work: PendingRunePresentation,
) -> Task<Message> {
	let source_buffer = work.source_buffer;
	let revision = work.revision;
	let source = work.source.clone();
	app_state.status_msg = "Rune presentation compiling…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				work.is_current()
					.then(|| crate::rune_view_compiler::compile(&work.source))
			})
			.await
			.unwrap_or_else(|error| {
				Some(Err(RuneEnvelopeError::Invalid(format!(
					"Rune presentation task panicked: {error}"
				))))
			})
		},
		move |result| Message::RunRunePresentationResult {
			source_buffer,
			revision,
			source,
			result,
		},
	)
}

#[cfg(feature = "toron")]
fn run_rune_relation_view_at_revision(
	app_state: &mut AppState,
	source_buffer: BufferId,
	revision: u64,
	view: TransformedRuneView,
) -> Task<Message> {
	let work = PendingRuneView {
		source_buffer,
		revision,
		view,
	};
	let presentation = work
		.view
		.presentation_views()
		.into_iter()
		.map(|view| (view.identity.clone(), view.intent.presentation()))
		.collect::<Vec<_>>();
	app_state
		.code_editor
		.prepare_rune_view_panels(source_buffer, &presentation, revision);
	let retirements = app_state
		.code_editor
		.take_selection_tasks()
		.map(Message::CodeEditEvent);
	let Some(work) = app_state.rune_view_scheduler.submit(work) else {
		app_state.status_msg = "Rune workspace queued newest view…".into();
		return retirements;
	};
	Task::batch([retirements, start_rune_relation_task(app_state, work)])
}

#[cfg(feature = "toron")]
fn start_rune_relation_task(app_state: &mut AppState, work: PendingRuneView) -> Task<Message> {
	let Some(connection) = app_state.adapter_state.rune_connection().cloned() else {
		#[cfg(toron_local)]
		let message = app_state.adapter_state.local_workspace_unavailable();
		// ADR 0165: without local supervision the only Toron is a remote one.
		#[cfg(not(toron_local))]
		let message = "no Toron connection: connect to a Toron endpoint first".to_owned();
		app_state
			.code_editor
			.mark_rune_view_producer_stale(work.source_buffer);
		set_rune_error(app_state, message);
		app_state.rune_view_scheduler.cancel(work.source_buffer);
		return Task::none();
	};
	let PendingRuneView {
		source_buffer,
		revision,
		view,
	} = work;
	let relation_count = view.relation_count();
	let plan_key = RelationPlanKey::from_view(&view);
	app_state.retained_rune_relations.insert(
		source_buffer,
		RetainedRelationGeneration {
			revision,
			plan_key,
			transformed: Arc::new(view.clone()),
			artifacts: vec![None; relation_count],
		},
	);
	app_state
		.rune_presentation_models
		.retain(|(buffer, candidate), _| *buffer != source_buffer || *candidate >= revision);
	app_state.rune_presentation_models.insert(
		(source_buffer, revision),
		Arc::new(view.presentation_source_model.clone()),
	);
	app_state
		.rune_presentation_sources
		.retain(|(buffer, candidate), _| *buffer != source_buffer || *candidate >= revision);
	app_state
		.rune_presentation_sources
		.insert((source_buffer, revision), Arc::from(view.source.as_str()));
	app_state.status_msg = format!(
		"Rune workspace running {relation_count} independent quer{}…",
		if relation_count == 1 { "y" } else { "ies" }
	);
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	let tasks = view
		.into_relations()
		.enumerate()
		.map(|(execution_index, view)| {
			let connection = connection.clone();
			let origin = view.origin.clone();
			let palettes = prepare_rune_categorical_palettes(app_state, source_buffer, &view);
			let views = view
				.views
				.iter()
				.map(|view| view.identity.clone())
				.collect::<Vec<_>>();
			Task::perform(
				dispatch_rune_relation_with_palettes(connection, view, palettes),
				move |result| Message::RunRuneViewSetResult {
					source_buffer,
					revision,
					execution_index,
					origin,
					views,
					result,
				},
			)
		});
	Task::batch(tasks)
}

#[cfg(feature = "toron")]
fn start_settings_preview_task(
	app_state: &mut AppState,
	work: PendingSettingsPreview,
) -> Task<Message> {
	app_state.settings_preview_in_flight = Some(work.preview_generation);
	let PendingSettingsPreview {
		preview_generation,
		panel_id,
		commit,
		source_buffer,
		document_generation,
		identity,
		plan,
		witness,
		artifact_generation,
	} = work;
	Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				crate::settings_writeback::compile_candidate(plan, &witness, artifact_generation)
			})
			.await
			.unwrap_or_else(|error| {
				Err(crate::settings_writeback::CandidatePreviewRefusal::Compile(
					format!("candidate presentation task panicked: {error}"),
				))
			})
		},
		move |result| Message::SettingsCandidateCompiled {
			preview_generation,
			panel_id,
			commit,
			source_buffer,
			document_generation,
			relation_revision: artifact_generation,
			identity,
			result,
		},
	)
}

#[cfg(feature = "toron")]
fn start_next_settings_preview_task(app_state: &mut AppState) -> Task<Message> {
	let Some(panel_id) = app_state
		.settings_preview_pending
		.iter()
		.min_by_key(|(_, work)| work.preview_generation)
		.map(|(panel_id, _)| *panel_id)
	else {
		return Task::none();
	};
	let work = app_state
		.settings_preview_pending
		.remove(&panel_id)
		.expect("selected pending preview remains queued");
	start_settings_preview_task(app_state, work)
}

#[cfg(feature = "toron")]
fn refuse_settings_preview(
	app_state: &mut AppState,
	panel_id: libaryon::engine::HostPanelId,
	message: String,
) {
	app_state.code_editor.discard_plot_source_preview(panel_id);
	app_state
		.code_editor
		.set_plot_writeback_status(panel_id, Some(message));
}

#[cfg(feature = "toron")]
fn validated_displayed_pane_value(
	settings: &crate::plot::theme::PlotSettings,
	key: crate::presentation_registry::SettingKey,
	displayed: &str,
) -> Option<String> {
	let resolved = settings.resolve();
	crate::presentation_registry::inspector_rows(settings, &resolved, None, None)
		.into_iter()
		.find(|row| {
			row.key == key
				&& row.provenance == crate::presentation_registry::ProvenanceTier::PaneOverride
				&& matches!(
					key,
					crate::presentation_registry::SettingKey::XAxisBounds
						| crate::presentation_registry::SettingKey::YAxisBounds
				)
		})
		.map(|row| row.value.to_string())
		.filter(|current| current == displayed)
}

#[cfg(feature = "toron")]
fn queue_settings_preview(
	app_state: &mut AppState,
	gesture: crate::editor::PlotWritebackGesture,
) -> Task<Message> {
	use crate::editor::PlotWritebackGesture;
	use crate::settings_writeback::{WritebackAction, parse_editor_value, plan_writeback};
	let gesture_panel = match &gesture {
		PlotWritebackGesture::Preview { panel_id, .. }
		| PlotWritebackGesture::Commit { panel_id, .. }
		| PlotWritebackGesture::Reset { panel_id, .. }
		| PlotWritebackGesture::Cancel { panel_id } => *panel_id,
	};
	if app_state
		.pending_settings_commits
		.values()
		.any(|pending| pending.panel_id == gesture_panel)
	{
		app_state
			.code_editor
			.set_plot_writeback_status(gesture_panel, Some("committing…".to_owned()));
		return Task::none();
	}

	let (panel_id, key, commit, action, input, displayed_pane_value) = match gesture {
		PlotWritebackGesture::Preview {
			panel_id,
			key,
			value,
		} => (
			panel_id,
			key,
			false,
			WritebackAction::Set,
			Some(value),
			None,
		),
		PlotWritebackGesture::Commit {
			panel_id,
			key,
			displayed_pane_value,
		} => {
			let input = app_state
				.code_editor
				.plot_writeback_context(panel_id)
				.and_then(|context| context.drafts.get(&key).cloned());
			(
				panel_id,
				key,
				true,
				WritebackAction::Set,
				input,
				displayed_pane_value,
			)
		}
		PlotWritebackGesture::Reset { panel_id, key } => {
			(panel_id, key, true, WritebackAction::Reset, None, None)
		}
		PlotWritebackGesture::Cancel { panel_id } => {
			app_state.settings_preview_sequence =
				app_state.settings_preview_sequence.saturating_add(1);
			app_state
				.settings_preview_latest
				.insert(panel_id, app_state.settings_preview_sequence);
			app_state.settings_preview_pending.remove(&panel_id);
			app_state.code_editor.discard_plot_source_preview(panel_id);
			return Task::none();
		}
	};
	let Some(context) = app_state.code_editor.plot_writeback_context(panel_id) else {
		refuse_settings_preview(
			app_state,
			panel_id,
			"read-only · source is no longer attached".to_owned(),
		);
		return Task::none();
	};
	let input = input.or_else(|| {
		displayed_pane_value.and_then(|displayed| {
			validated_displayed_pane_value(&context.settings, key, &displayed)
		})
	});
	let source_buffer = context.attachment.source.buffer_id;
	let Some((source, document_generation)) = app_state
		.code_editor
		.document_snapshot(source_buffer)
		.map(|(source, generation)| (source.to_owned(), generation))
	else {
		refuse_settings_preview(
			app_state,
			panel_id,
			"read-only · source snapshot unavailable".to_owned(),
		);
		return Task::none();
	};
	let occurrence_exists = context
		.attachment
		.source
		.model
		.occurrences
		.iter()
		.any(|occurrence| {
			occurrence.key == key
				&& occurrence.target == crate::presentation_registry::setting_spec(key).target
				&& occurrence.owners.contains(&context.attachment.identity)
		});
	let authored_without_occurrence = !occurrence_exists
		&& crate::presentation_registry::setting_spec(key)
			.authored_projection
			.is_some_and(|projection| projection(&context.settings.authored_spec));
	let value = match (action, input.as_deref()) {
		(WritebackAction::Set, Some(input)) => match parse_editor_value(key, input) {
			Ok(value) => Some(value),
			Err(error) => {
				refuse_settings_preview(app_state, panel_id, error.to_string());
				return Task::none();
			}
		},
		(WritebackAction::Set, None) => {
			refuse_settings_preview(
				app_state,
				panel_id,
				"enter a value before committing".to_owned(),
			);
			return Task::none();
		}
		(WritebackAction::Reset, _) => None,
	};
	let plan = match plan_writeback(
		&source,
		Some(&context.attachment),
		None,
		key,
		action,
		value.as_ref(),
		authored_without_occurrence,
	) {
		Ok(plan) => plan,
		Err(error) => {
			refuse_settings_preview(app_state, panel_id, error.to_string());
			return Task::none();
		}
	};
	let Some(authoritative_revision) = app_state.rune_buffer_revisions.get(&source_buffer).copied()
	else {
		refuse_settings_preview(
			app_state,
			panel_id,
			"read-only · no authoritative relation publication".to_owned(),
		);
		return Task::none();
	};
	let Some(retained) = app_state.retained_rune_relations.get(&source_buffer) else {
		refuse_settings_preview(
			app_state,
			panel_id,
			"read-only · relation artifact was not retained".to_owned(),
		);
		return Task::none();
	};
	if !retained
		.transformed
		.presentation_order
		.contains(&context.attachment.identity)
	{
		refuse_settings_preview(
			app_state,
			panel_id,
			"read-only · producing view is no longer current".to_owned(),
		);
		return Task::none();
	}
	let Some(witness) = retained.witness(authoritative_revision) else {
		refuse_settings_preview(
			app_state,
			panel_id,
			"this change requires running the relation again".to_owned(),
		);
		return Task::none();
	};
	app_state.settings_preview_sequence = app_state.settings_preview_sequence.saturating_add(1);
	let preview_generation = app_state.settings_preview_sequence;
	app_state
		.settings_preview_latest
		.insert(panel_id, preview_generation);
	let work = PendingSettingsPreview {
		preview_generation,
		panel_id,
		commit,
		source_buffer,
		document_generation,
		identity: context.attachment.identity,
		plan,
		witness,
		artifact_generation: authoritative_revision,
	};
	if app_state.settings_preview_in_flight.is_some() {
		app_state.settings_preview_pending.insert(panel_id, work);
		Task::none()
	} else {
		start_settings_preview_task(app_state, work)
	}
}

#[cfg(feature = "toron")]
fn settle_settings_commit(app_state: &mut AppState, result: libaryon::engine::DocumentEditResult) {
	let Some(pending) = app_state
		.pending_settings_commits
		.remove(&result.request_id)
	else {
		return;
	};
	if result.buffer_id != pending.source_buffer {
		app_state
			.code_editor
			.discard_plot_source_preview(pending.panel_id);
		app_state.code_editor.set_plot_writeback_status(
			pending.panel_id,
			Some("commit refused · acknowledgement targeted another buffer".to_owned()),
		);
		return;
	}
	match result.outcome {
		libaryon::engine::DocumentEditOutcome::Applied { new_generation } => {
			let candidate_source = pending.candidate.plan.candidate_source.clone();
			let promoted = app_state.code_editor.promote_plot_source_preview(
				pending.panel_id,
				new_generation,
				candidate_source,
			);
			if !promoted {
				app_state.status_error.clear();
				app_state.status_msg =
					"Plot setting committed; a newer plot publication superseded its preview."
						.to_owned();
				return;
			}
			let model = Arc::new(
				pending
					.candidate
					.transformed
					.presentation_source_model
					.clone(),
			);
			app_state
				.rune_document_generations
				.insert((pending.source_buffer, pending.revision), new_generation);
			app_state
				.rune_presentation_models
				.insert((pending.source_buffer, pending.revision), model);
			if let Some(retained) = app_state
				.retained_rune_relations
				.get_mut(&pending.source_buffer)
				&& retained.revision == pending.revision
			{
				retained.plan_key = RelationPlanKey::from_view(&pending.candidate.transformed);
				retained.transformed = Arc::new(pending.candidate.transformed);
			}
			app_state.rune_categorical_identities.insert(
				(
					pending.source_buffer,
					pending.identity,
					pending.continuation.palette_len,
				),
				pending.continuation.identity,
			);
			app_state.status_error.clear();
			app_state.status_msg = "Plot setting committed to Rune source.".to_owned();
		}
		libaryon::engine::DocumentEditOutcome::Refused { reason } => {
			app_state
				.code_editor
				.discard_plot_source_preview(pending.panel_id);
			app_state.code_editor.set_plot_writeback_status(
				pending.panel_id,
				Some(format!("commit refused · {reason}")),
			);
			app_state.status_error = format!("Plot setting commit refused: {reason}");
		}
	}
}

#[cfg(feature = "toron")]
fn resolved_rune_categorical_selection(
	authored_settings: &PlotSettingsLayer,
) -> std::result::Result<CategoricalPaletteSelection, String> {
	let settings = PlotSettings {
		authored_spec: authored_settings.clone(),
		..PlotSettings::default()
	};
	let identity = settings.resolve_categorical_palette_identity();
	CategoricalPaletteSelection::from_theme_name(identity.as_str())
		.map_err(|error| error.to_string())
}

#[cfg(feature = "toron")]
fn resolve_rune_plot_skin(
	mut authored_settings: PlotSettingsLayer,
) -> std::result::Result<PlotSettingsLayer, String> {
	let settings = PlotSettings {
		authored_spec: authored_settings.clone(),
		..PlotSettings::default()
	};
	if let Some(selection) = settings.resolve_plot_skin() {
		authored_settings.resolved_skin = Override::Set(
			crate::plot::chrome::ResolvedPlotSkin::from_theme_name(
				selection.name.as_str(),
				selection.projection,
			)
			.map_err(|error| error.to_string())?,
		);
	}
	Ok(authored_settings)
}

#[cfg(feature = "toron")]
fn authored_rune_plot_spec(sink: &DisposableRunePlotSink) -> AuthoredPlotSpec {
	if let Some(fields) = sink.mark_fields.as_ref() {
		return match fields.to_mark_fields() {
			crate::plot::spec::MarkFields::BoxPlot(fields) => AuthoredPlotSpec::box_plot(fields),
			crate::plot::spec::MarkFields::Heatmap(fields) => AuthoredPlotSpec::heatmap(fields),
			crate::plot::spec::MarkFields::Hexbin(fields) => AuthoredPlotSpec::hexbin(fields),
			crate::plot::spec::MarkFields::FillBetween(fields) => {
				AuthoredPlotSpec::fill_between(fields)
			}
			crate::plot::spec::MarkFields::Candlestick(fields) => {
				AuthoredPlotSpec::candlestick(fields)
			}
			crate::plot::spec::MarkFields::Bubble(fields) => AuthoredPlotSpec::bubble(fields),
			crate::plot::spec::MarkFields::Funnel(fields) => AuthoredPlotSpec::funnel(fields),
			crate::plot::spec::MarkFields::RadialDial(fields) => {
				AuthoredPlotSpec::radial_dial(fields)
			}
			crate::plot::spec::MarkFields::Pie(fields) => AuthoredPlotSpec::pie(fields),
			crate::plot::spec::MarkFields::Violin(fields) => AuthoredPlotSpec::violin(fields),
			crate::plot::spec::MarkFields::Radar(fields) => AuthoredPlotSpec::radar(fields),
			crate::plot::spec::MarkFields::Parallel(fields) => AuthoredPlotSpec::parallel(fields),
		};
	}
	if sink.composition.primary_plot_type() == PlotType::Histogram {
		return AuthoredPlotSpec::histogram(
			sink.x.clone(),
			sink.interval_end
				.clone()
				.expect("Rune histogram carries its explicit bin_end"),
			sink.y.clone(),
		);
	}
	AuthoredPlotSpec::xy_color(
		sink.composition.clone(),
		sink.x.clone(),
		sink.y.clone(),
		sink.category.clone(),
	)
}

#[cfg(feature = "toron")]
fn with_rune_continuous_intent(
	spec: AuthoredPlotSpec,
	intent: Option<crate::plot::continuous_color::ContinuousScaleIntent>,
) -> AuthoredPlotSpec {
	match intent {
		Some(intent) => spec.with_continuous_intent(intent),
		None => spec,
	}
}

#[cfg(feature = "toron")]
fn box_plot_failure_role(
	data: &DataFrame,
	fields: &crate::plot::spec::BoxPlotFields,
) -> Option<crate::plot::kernels::boxplot::BoxPlotFieldRole> {
	use crate::plot::kernels::boxplot::BoxPlotFieldRole;
	let authored = AuthoredPlotSpec::box_plot(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		for (role, field) in [
			(BoxPlotFieldRole::Category, &fields.category),
			(BoxPlotFieldRole::WhiskerLow, &fields.whisker_low),
			(BoxPlotFieldRole::Q1, &fields.q1),
			(BoxPlotFieldRole::Median, &fields.median),
			(BoxPlotFieldRole::Q3, &fields.q3),
			(BoxPlotFieldRole::WhiskerHigh, &fields.whisker_high),
			(BoxPlotFieldRole::Outlier, &fields.outlier),
		] {
			if error.field == *field || error.reason.contains(role.name()) {
				return Some(role);
			}
		}
		return None;
	}
	crate::plot::kernels::boxplot::prepare_box_plot_data(data, fields)
		.err()
		.and_then(|error| error.field_role())
}

#[cfg(feature = "toron")]
fn heatmap_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::HeatmapFields,
	origins: &crate::rune_relation_envelope::HeatmapFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::heatmap(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if error.field == fields.x {
			origins.x.clone()
		} else if error.field == fields.y {
			origins.y.clone()
		} else if error.field == fields.value {
			origins.value.clone()
		} else {
			mark.clone()
		};
	}
	match crate::plot::kernels::heatmap::prepare_heatmap_data(
		data,
		&fields.x,
		&fields.y,
		&fields.value,
	) {
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::NonFiniteValue { .. }) => {
			origins.value.clone()
		}
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::NullX { .. }) => {
			origins.x.clone()
		}
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::NullY { .. }) => {
			origins.y.clone()
		}
		Err(crate::plot::kernels::heatmap_cells::HeatmapCellsError::DuplicateCell { .. }) => {
			origins.keys()
		}
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn hexbin_failure_span(
	data: &DataFrame,
	source_token: crate::result_artifact::ArtifactToken,
	fields: &crate::plot::spec::HexbinFields,
	origins: &crate::rune_relation_envelope::HexbinFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let for_channel = |channel| match channel {
		crate::plot::kernels::hexbin_viewport::HexbinChannel::X => origins.x.clone(),
		crate::plot::kernels::hexbin_viewport::HexbinChannel::Y => origins.y.clone(),
	};
	let authored = AuthoredPlotSpec::hexbin(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return match error.attribution {
			crate::plot::spec::FieldAttribution::Channel(Channel::X) => origins.x.clone(),
			crate::plot::spec::FieldAttribution::Channel(Channel::Y) => origins.y.clone(),
			_ => mark.clone(),
		};
	}
	match crate::plot::kernels::hexbin_viewport::prepare_hexbin_observations(
		data,
		&fields.x,
		&fields.y,
		source_token,
	) {
		Err(error) => {
			use crate::plot::kernels::hexbin_viewport::HexbinPreparationError;
			let channel = match error {
				HexbinPreparationError::MissingColumn { channel, .. }
				| HexbinPreparationError::TemporalColumn { channel, .. }
				| HexbinPreparationError::UnsupportedColumn { channel, .. }
				| HexbinPreparationError::NullValue { channel, .. }
				| HexbinPreparationError::NonFiniteValue { channel, .. }
				| HexbinPreparationError::InexactValue { channel, .. } => channel,
			};
			for_channel(channel)
		}
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn fill_between_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::FillBetweenFields,
	origins: &crate::rune_relation_envelope::FillBetweenFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::fill_between(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return match error.attribution {
			crate::plot::spec::FieldAttribution::Channel(Channel::X) => origins.x.clone(),
			crate::plot::spec::FieldAttribution::Channel(Channel::Y) => origins.lower.clone(),
			crate::plot::spec::FieldAttribution::Role("upper") => origins.upper.clone(),
			crate::plot::spec::FieldAttribution::Role("middle") => {
				origins.middle.clone().unwrap_or_else(|| mark.clone())
			}
			_ => mark.clone(),
		};
	}
	match crate::plot::kernels::fill_between::prepare_fill_between_data(
		data,
		&fields.x,
		&fields.lower,
		&fields.upper,
		fields.middle.as_deref(),
	) {
		Err(error) => origins
			.for_role(error.role())
			.unwrap_or_else(|| mark.clone()),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn candlestick_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::CandlestickFields,
	origins: &crate::rune_relation_envelope::CandlestickFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::candlestick(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return match error.attribution {
			crate::plot::spec::FieldAttribution::Channel(Channel::X) => origins.x.clone(),
			crate::plot::spec::FieldAttribution::Channel(Channel::Y) => origins.open.clone(),
			crate::plot::spec::FieldAttribution::Role("high") => origins.high.clone(),
			crate::plot::spec::FieldAttribution::Role("low") => origins.low.clone(),
			crate::plot::spec::FieldAttribution::Role("close") => origins.close.clone(),
			_ => mark.clone(),
		};
	}
	match crate::plot::kernels::candlestick::prepare_candlestick_data(
		data,
		&fields.x,
		&fields.open,
		&fields.high,
		&fields.low,
		&fields.close,
	) {
		Err(error) => origins.for_role(error.role()),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn bubble_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::BubbleFields,
	origins: &crate::rune_relation_envelope::BubbleFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	use crate::plot::kernels::bubble::BubblePreparationError;
	use crate::plot::kernels::bubble_observations::BubbleObservationsError;
	use crate::plot::spec::{BubbleNumericField, BubblePreparationChannel};

	let role_for_field = |field: &str| {
		[
			(
				BubblePreparationChannel::X,
				fields.numeric.get(BubbleNumericField::X),
			),
			(
				BubblePreparationChannel::Y,
				fields.numeric.get(BubbleNumericField::Y),
			),
			(
				BubblePreparationChannel::Size,
				fields.numeric.get(BubbleNumericField::Size),
			),
			(
				BubblePreparationChannel::Color,
				fields.numeric.get(BubbleNumericField::Color),
			),
		]
		.into_iter()
		.find_map(|(role, candidate)| (candidate == field).then_some(role))
		.or_else(|| {
			fields
				.label
				.as_deref()
				.filter(|candidate| *candidate == field)
				.map(|_| BubblePreparationChannel::Label)
		})
	};
	let authored = AuthoredPlotSpec::bubble(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return role_for_field(&error.field)
			.and_then(|role| origins.for_role(role))
			.unwrap_or_else(|| mark.clone());
	}
	let result = crate::plot::kernels::bubble::prepare_bubble_data(data, fields);
	let role = match result {
		Err(
			BubblePreparationError::MissingColumn { channel, .. }
			| BubblePreparationError::UncastableColumn { channel, .. },
		) => Some(channel),
		Err(BubblePreparationError::Observations(
			BubbleObservationsError::NullValue { channel, .. }
			| BubbleObservationsError::NonFiniteValue { channel, .. },
		)) => Some(channel.into()),
		Ok(_) => None,
	};
	role.and_then(|role| origins.for_role(role))
		.unwrap_or_else(|| mark.clone())
}

#[cfg(feature = "toron")]
fn funnel_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::FunnelFields,
	origins: &crate::rune_relation_envelope::FunnelFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::funnel(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if error.field == fields.stage {
			origins.stage.clone()
		} else if error.field == fields.value {
			origins.value.clone()
		} else {
			mark.clone()
		};
	}
	match crate::plot::kernels::funnel::prepare_funnel_data(data, &fields.stage, &fields.value) {
		Err(
			crate::plot::kernels::funnel::FunnelPreparationError::MissingColumn {
				channel: crate::plot::kernels::funnel::FunnelPreparationChannel::Stage,
				..
			}
			| crate::plot::kernels::funnel::FunnelPreparationError::UncastableColumn {
				channel: crate::plot::kernels::funnel::FunnelPreparationChannel::Stage,
				..
			},
		) => origins.stage.clone(),
		Err(crate::plot::kernels::funnel::FunnelPreparationError::Stages(
			crate::plot::kernels::funnel_stages::FunnelStagesError::NullStage { .. }
			| crate::plot::kernels::funnel_stages::FunnelStagesError::DuplicateStage { .. },
		)) => origins.stage.clone(),
		Err(
			crate::plot::kernels::funnel::FunnelPreparationError::MissingColumn { .. }
			| crate::plot::kernels::funnel::FunnelPreparationError::UncastableColumn { .. }
			| crate::plot::kernels::funnel::FunnelPreparationError::Stages(_),
		) => origins.value.clone(),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn radial_dial_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::RadialDialFields,
	origins: &crate::rune_relation_envelope::RadialDialFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::radial_dial(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if fields.label.as_deref() == Some(error.field.as_str()) {
			origins.label.clone().unwrap_or_else(|| mark.clone())
		} else if error.field == fields.value {
			origins.value.clone()
		} else if error.field == fields.maximum {
			origins.maximum.clone()
		} else {
			mark.clone()
		};
	}
	use crate::plot::kernels::radial_dial::{
		RadialDialPreparationError, RadialDialPreparationField,
	};
	use crate::plot::kernels::radial_dial_readings::RadialDialReadingsError;
	match crate::plot::kernels::radial_dial::prepare_radial_dial_data(
		data,
		fields.label.as_deref(),
		&fields.value,
		&fields.maximum,
	) {
		Err(
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Category,
				..
			}
			| RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Category,
				..
			}
			| RadialDialPreparationError::Readings(
				RadialDialReadingsError::NullCategory { .. }
				| RadialDialReadingsError::DuplicateCategory { .. },
			),
		) => origins.label.clone().unwrap_or_else(|| mark.clone()),
		Err(
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Value,
				..
			}
			| RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Value,
				..
			}
			| RadialDialPreparationError::Readings(
				RadialDialReadingsError::NullValue { .. }
				| RadialDialReadingsError::InexactValue { .. }
				| RadialDialReadingsError::NonFiniteValue { .. }
				| RadialDialReadingsError::NegativeValue { .. }
				| RadialDialReadingsError::ValueExceedsMaximum { .. },
			),
		) => origins.value.clone(),
		Err(
			RadialDialPreparationError::MissingColumn {
				field_role: RadialDialPreparationField::Maximum,
				..
			}
			| RadialDialPreparationError::UnsupportedColumn {
				field_role: RadialDialPreparationField::Maximum,
				..
			}
			| RadialDialPreparationError::Readings(
				RadialDialReadingsError::NullMaximum { .. }
				| RadialDialReadingsError::InexactMaximum { .. }
				| RadialDialReadingsError::NonFiniteMaximum { .. }
				| RadialDialReadingsError::NonPositiveMaximum { .. },
			),
		) => origins.maximum.clone(),
		Err(RadialDialPreparationError::Readings(
			RadialDialReadingsError::UngroupedMultipleRows { .. },
		))
		| Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn pie_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::PieFields,
	origins: &crate::rune_relation_envelope::PieFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::pie(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return if error.field == fields.category {
			origins.category.clone()
		} else if error.field == fields.value {
			origins.value.clone()
		} else {
			mark.clone()
		};
	}
	use crate::plot::kernels::pie::{PiePreparationError, PiePreparationField};
	use crate::plot::kernels::pie_slices::PieSlicesError;
	match crate::plot::kernels::pie::prepare_pie_data(data, Some(&fields.category), &fields.value) {
		Err(
			PiePreparationError::MissingColumn {
				field_role: PiePreparationField::Category,
				..
			}
			| PiePreparationError::UnsupportedColumn {
				field_role: PiePreparationField::Category,
				..
			}
			| PiePreparationError::Slices(
				PieSlicesError::NullCategory { .. } | PieSlicesError::DuplicateCategory { .. },
			),
		) => origins.category.clone(),
		Err(
			PiePreparationError::MissingColumn {
				field_role: PiePreparationField::Value,
				..
			}
			| PiePreparationError::UnsupportedColumn {
				field_role: PiePreparationField::Value,
				..
			}
			| PiePreparationError::Slices(
				PieSlicesError::NullValue { .. }
				| PieSlicesError::InexactValue { .. }
				| PieSlicesError::NonFiniteValue { .. }
				| PieSlicesError::NegativeValue { .. }
				| PieSlicesError::NonFiniteTotal { .. }
				| PieSlicesError::NonPositiveTotal { .. },
			),
		) => origins.value.clone(),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn violin_failure_span(
	data: &DataFrame,
	fields: &crate::plot::spec::ViolinFields,
	origins: &crate::rune_relation_envelope::ViolinFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = AuthoredPlotSpec::violin(fields.clone());
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return match error.attribution {
			crate::plot::spec::FieldAttribution::Role("group") => origins.group.clone(),
			crate::plot::spec::FieldAttribution::Role("evaluation_value") => {
				origins.evaluation_value.clone()
			}
			crate::plot::spec::FieldAttribution::Role("density") => origins.density.clone(),
			crate::plot::spec::FieldAttribution::Role("median") => origins.median.clone(),
			_ => mark.clone(),
		};
	}
	use crate::plot::kernels::violin_density::ViolinDensityError;
	match crate::plot::kernels::violin::prepare_violin_density_data(
		data,
		&fields.group,
		&fields.evaluation_value,
		&fields.density,
		&fields.median,
	) {
		Err(ViolinDensityError::NullGroup { .. }) => origins.group.clone(),
		Err(
			ViolinDensityError::NullValue { field, .. }
			| ViolinDensityError::InexactValue { field, .. },
		) => match field.as_str() {
			"evaluation_value" => origins.evaluation_value.clone(),
			"density" => origins.density.clone(),
			"median" => origins.median.clone(),
			_ => mark.clone(),
		},
		Err(ViolinDensityError::GridMismatch { .. } | ViolinDensityError::TooFewPoints { .. }) => {
			origins.evaluation_value.clone()
		}
		Err(ViolinDensityError::MedianDisagreement { .. }) => origins.median.clone(),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn multivariate_failure_span(
	data: &DataFrame,
	plot_type: PlotType,
	fields: &crate::plot::spec::MultivariateFields,
	origins: &crate::rune_relation_envelope::MultivariateFieldOrigins,
	mark: &std::ops::Range<u32>,
) -> std::ops::Range<u32> {
	let authored = match plot_type {
		PlotType::Radar => AuthoredPlotSpec::radar(fields.clone()),
		PlotType::Parallel => AuthoredPlotSpec::parallel(fields.clone()),
		_ => unreachable!("multivariate diagnostic admitted {plot_type:?}"),
	};
	if let Err(error) = resolve_channels(&authored, data.schema().as_ref()) {
		return origins
			.dimension(fields, &error.field)
			.or_else(|| {
				(fields.category() == Some(error.field.as_str()))
					.then(|| origins.category.clone())
					.flatten()
			})
			.unwrap_or_else(|| mark.clone());
	}
	use crate::plot::kernels::multivariate_rows::{
		MultivariatePreparationError, MultivariateRowsError, prepare_multivariate_rows,
	};
	match prepare_multivariate_rows(data, fields.dimensions(), fields.category()) {
		Err(
			MultivariatePreparationError::MissingDimension { field }
			| MultivariatePreparationError::UnsupportedDimension { field }
			| MultivariatePreparationError::Rows(
				MultivariateRowsError::NullValue { field, .. }
				| MultivariateRowsError::InexactValue { field, .. }
				| MultivariateRowsError::NonFiniteValue { field, .. },
			),
		) => origins
			.dimension(fields, &field)
			.unwrap_or_else(|| mark.clone()),
		Err(
			MultivariatePreparationError::MissingCategory { .. }
			| MultivariatePreparationError::UnsupportedCategory { .. }
			| MultivariatePreparationError::Rows(MultivariateRowsError::NullCategory { .. }),
		) => origins.category.clone().unwrap_or_else(|| mark.clone()),
		Ok(_) => mark.clone(),
	}
}

#[cfg(feature = "toron")]
fn resolved_rune_axis_channels(
	body: &RunePlotBody,
	schema: &polars::prelude::Schema,
) -> std::result::Result<[crate::plot::spec::BoundChannel; 2], String> {
	let select = |channels: &[crate::plot::spec::BoundChannel]| {
		[Channel::X, Channel::Y].map(|channel| {
			channels
				.iter()
				.find(|binding| binding.channel == channel)
				.cloned()
				.ok_or_else(|| format!("plot has no resolved {channel} axis"))
		})
	};
	let resolved = match body {
		RunePlotBody::Single(sink) => {
			let authored = authored_rune_plot_spec(sink);
			let binding = resolve_channels(&authored, schema).map_err(|error| error.to_string())?;
			select(binding.channels())
		}
		RunePlotBody::StyledSingle(styled) => {
			let sink = &styled.sink;
			let authored = authored_rune_plot_spec(sink);
			let authored = if styled.stacked {
				authored.with_stacked_bar()
			} else {
				authored
			};
			let authored = if styled.percent {
				authored.with_percent()
			} else {
				authored
			};
			let binding = resolve_channels(&authored, schema).map_err(|error| error.to_string())?;
			select(binding.channels())
		}
		RunePlotBody::Layered(specification) => {
			let binding = resolve_layered_channels(specification, schema)
				.map_err(|error| error.to_string())?;
			select(binding.layers.first().channels())
		}
	};
	Ok([resolved[0].clone()?, resolved[1].clone()?])
}

#[cfg(feature = "toron")]
fn resolved_rune_axis_scales(
	body: &RunePlotBody,
	schema: &polars::prelude::Schema,
) -> std::result::Result<(ScaleFamily, ScaleFamily), String> {
	let [x, y] = resolved_rune_axis_channels(body, schema)?;
	Ok((x.scale, y.scale))
}

#[cfg(feature = "toron")]
fn resolved_rune_axis_data_types(
	body: &RunePlotBody,
	schema: &polars::prelude::Schema,
) -> std::result::Result<(polars::prelude::DataType, polars::prelude::DataType), String> {
	let [x, y] = resolved_rune_axis_channels(body, schema)?;
	let data_type = |field: &str| {
		schema
			.get(field)
			.cloned()
			.ok_or_else(|| format!("plot field `{field}` is absent from the schema"))
	};
	Ok((data_type(&x.field)?, data_type(&y.field)?))
}

#[cfg(feature = "toron")]
fn validate_rune_numeric_domain_applicability(
	body: &RunePlotBody,
	authored_settings: &PlotSettingsLayer,
	domain_spans: &RunePlotDomainSpans,
	tick_step_spans: &RunePlotTickStepSpans,
	tick_every_spans: &RunePlotTickEverySpans,
	minor_tick_spans: &RunePlotMinorTickSpans,
	schema: &polars::prelude::Schema,
) -> std::result::Result<(), (std::ops::Range<u32>, String)> {
	if domain_spans.x.is_none()
		&& domain_spans.y.is_none()
		&& tick_step_spans.x.is_none()
		&& tick_step_spans.y.is_none()
		&& tick_every_spans.x.is_none()
		&& tick_every_spans.y.is_none()
		&& minor_tick_spans.x.is_none()
		&& minor_tick_spans.y.is_none()
	{
		return Ok(());
	}
	let Ok((x_scale, y_scale)) = resolved_rune_axis_scales(body, schema) else {
		// Established planning owns channel-resolution failures. This check only
		// rejects a successfully resolved axis that is not numeric.
		return Ok(());
	};
	let percent_value_axis = match body {
		RunePlotBody::StyledSingle(styled) if styled.percent => {
			match styled.sink.composition.primary_plot_type() {
				PlotType::StackedArea => Some("y"),
				PlotType::Bar if matches!(x_scale, ScaleFamily::Linear) => Some("x"),
				PlotType::Bar => Some("y"),
				_ => None,
			}
		}
		_ => None,
	};
	if percent_value_axis == Some("x")
		&& let Some(span) = domain_spans.x.as_ref()
	{
		return Err((
			span.clone(),
			"x_axis().bounds(...) cannot override the fixed 0..100 percent domain".to_owned(),
		));
	}
	if percent_value_axis == Some("y")
		&& let Some(span) = domain_spans.y.as_ref()
	{
		return Err((
			span.clone(),
			"y_axis().bounds(...) cannot override the fixed 0..100 percent domain".to_owned(),
		));
	}
	let numeric = |scale| matches!(scale, ScaleFamily::Linear | ScaleFamily::Log);
	if let Some(span) = domain_spans.x.as_ref()
		&& !numeric(x_scale)
	{
		return Err((
			span.clone(),
			format!(
				"x_axis().bounds(...) requires a numeric x axis; this figure resolves x as {x_scale:?}"
			),
		));
	}
	if let Some(span) = tick_step_spans.x.as_ref()
		&& !matches!(x_scale, ScaleFamily::Linear)
	{
		return Err((
			span.clone(),
			format!(
				"x_axis().tick_step(...) requires a linear numeric x axis; this figure resolves x as {x_scale:?}"
			),
		));
	}
	if let Some(span) = domain_spans.y.as_ref()
		&& !numeric(y_scale)
	{
		return Err((
			span.clone(),
			format!(
				"y_axis().bounds(...) requires a numeric y axis; this figure resolves y as {y_scale:?}"
			),
		));
	}
	if let Some(span) = tick_step_spans.y.as_ref()
		&& !matches!(y_scale, ScaleFamily::Linear)
	{
		return Err((
			span.clone(),
			format!(
				"y_axis().tick_step(...) requires a linear numeric y axis; this figure resolves y as {y_scale:?}"
			),
		));
	}
	if tick_every_spans.x.is_some() || tick_every_spans.y.is_some() {
		let Ok((x_type, y_type)) = resolved_rune_axis_data_types(body, schema) else {
			return Ok(());
		};
		let guides = match &authored_settings.guides {
			Override::Set(guides) => guides,
			Override::Inherit => unreachable!("tick_every always authors guide settings"),
		};
		let validate = |axis: &str,
		                span: &Option<std::ops::Range<u32>>,
		                data_type: &polars::prelude::DataType,
		                placement: crate::plot::common::TickPlacement|
		 -> std::result::Result<(), (std::ops::Range<u32>, String)> {
			let Some(span) = span else {
				return Ok(());
			};
			let scale = if axis == "x" { x_scale } else { y_scale };
			if !matches!(scale, ScaleFamily::Time) {
				return Err((
					span.clone(),
					format!(
						"{axis}_axis().tick_every(...) requires a temporal {axis} axis; this figure resolves {axis} as {scale:?}"
					),
				));
			}
			let crate::plot::common::TickPlacement::Temporal(stride) = placement else {
				unreachable!("tick_every span always selects temporal placement")
			};
			let supported = matches!(
				(data_type, stride.interval()),
				(
					polars::prelude::DataType::Date,
					TemporalInterval::CalendarMonths(_)
				) | (
					polars::prelude::DataType::Datetime(_, _),
					TemporalInterval::FixedNanos(_)
				) | (
					polars::prelude::DataType::Datetime(_, _),
					TemporalInterval::CalendarMonths(_)
				) | (
					polars::prelude::DataType::Time,
					TemporalInterval::FixedNanos(_)
				)
			);
			if !supported {
				return Err((
					span.clone(),
					format!(
						"{axis}_axis().tick_every(...) interval does not apply to this {data_type:?} axis"
					),
				));
			}
			Ok(())
		};
		validate("x", &tick_every_spans.x, &x_type, guides.x_tick_placement)?;
		validate("y", &tick_every_spans.y, &y_type, guides.y_tick_placement)?;
	}
	if let Some(span) = minor_tick_spans.x.as_ref()
		&& !matches!(x_scale, ScaleFamily::Linear)
	{
		return Err((
			span.clone(),
			format!(
				"x_axis().minor_ticks(...) requires a linear numeric x axis; this figure resolves x as {x_scale:?}"
			),
		));
	}
	if let Some(span) = minor_tick_spans.y.as_ref()
		&& !matches!(y_scale, ScaleFamily::Linear)
	{
		return Err((
			span.clone(),
			format!(
				"y_axis().minor_ticks(...) requires a linear numeric y axis; this figure resolves y as {y_scale:?}"
			),
		));
	}
	Ok(())
}

#[cfg(feature = "toron")]
fn validate_rune_tick_capacity(
	layout: &PlotLayout,
	settings: &crate::plot::common::ResolvedPlotSettings,
	tick_step_spans: &RunePlotTickStepSpans,
	tick_every_spans: &RunePlotTickEverySpans,
	minor_tick_spans: &RunePlotMinorTickSpans,
) -> std::result::Result<(), (std::ops::Range<u32>, String)> {
	if tick_step_spans.x.is_none()
		&& tick_step_spans.y.is_none()
		&& tick_every_spans.x.is_none()
		&& tick_every_spans.y.is_none()
		&& minor_tick_spans.x.is_none()
		&& minor_tick_spans.y.is_none()
	{
		return Ok(());
	}
	let check_temporal = |placement: crate::plot::common::TickPlacement,
	                      axis: &str,
	                      domain: Option<&crate::plot::common::TemporalAxisDomain>,
	                      span: &Option<std::ops::Range<u32>>|
	 -> std::result::Result<(), (std::ops::Range<u32>, String)> {
		let Some(span) = span else {
			return Ok(());
		};
		let crate::plot::common::TickPlacement::Temporal(stride) = placement else {
			unreachable!("tick_every span always selects temporal placement")
		};
		let Some(domain) = domain else {
			return Err((
				span.clone(),
				format!("{axis}_axis().tick_every(...) requires an exact temporal axis domain"),
			));
		};
		crate::plot::common::select_temporal_ticks(domain, stride).map_err(|error| {
			(
				span.clone(),
				format!("{axis}_axis().tick_every(...) cannot select temporal ticks: {error:?}"),
			)
		})?;
		Ok(())
	};
	match layout {
		PlotLayout::Cartesian {
			x_temporal_domain,
			y_temporal_domain,
			..
		} => {
			check_temporal(
				settings.x_tick_placement,
				"x",
				x_temporal_domain.as_ref(),
				&tick_every_spans.x,
			)?;
			check_temporal(
				settings.y_tick_placement,
				"y",
				y_temporal_domain.as_ref(),
				&tick_every_spans.y,
			)?;
		}
		_ if tick_every_spans.x.is_some() || tick_every_spans.y.is_some() => {
			let span = tick_every_spans
				.x
				.clone()
				.or_else(|| tick_every_spans.y.clone())
				.expect("authored tick_every has a span");
			return Err((
				span,
				"tick_every(...) requires a Cartesian temporal axis layout".to_owned(),
			));
		}
		_ => {}
	}
	let check = |placement: crate::plot::common::TickPlacement,
	             axis: &str,
	             range: (f64, f64),
	             span: &Option<std::ops::Range<u32>>|
	 -> std::result::Result<(), (std::ops::Range<u32>, String)> {
		if span.is_none() {
			return Ok(());
		}
		if let Err(error) = placement.positions(range) {
			let message = match error {
				TickPlacementError::Capacity {
					step,
					range,
					requested,
					limit,
				} => format!(
					"{axis}_axis().tick_step({step}) over {range:?} generates {requested} positions, exceeding the {limit}-position capacity"
				),
				TickPlacementError::InvalidDomain { range } => {
					format!("{axis}_axis().tick_step(...) has no valid resolved domain {range:?}")
				}
				TickPlacementError::UnsupportedAxis { axis_type } => {
					format!("{axis}_axis().tick_step(...) does not apply to {axis_type:?} axes")
				}
				TickPlacementError::MinorCapacity { .. } => unreachable!(),
				TickPlacementError::TemporalRequiresExactDomain => unreachable!(),
			};
			return Err((span.clone().unwrap(), message));
		}
		Ok(())
	};
	let check_minor = |placement: crate::plot::common::TickPlacement,
	                   axis: &str,
	                   range: (f64, f64),
	                   minor_ticks: u32,
	                   span: &Option<std::ops::Range<u32>>|
	 -> std::result::Result<(), (std::ops::Range<u32>, String)> {
		let Some(span) = span else {
			return Ok(());
		};
		let major = placement.positions(range).map_err(|error| {
			let message = match error {
				TickPlacementError::Capacity {
					step: _,
					range,
					requested,
					limit,
				} => format!(
					"{axis}_axis().minor_ticks({minor_ticks}) with {placement:?} over {range:?} generates {requested} major positions, exceeding the {limit}-position capacity"
				),
				TickPlacementError::InvalidDomain { range } => {
					format!("{axis}_axis().minor_ticks({minor_ticks}) has no valid resolved domain {range:?}")
				}
				TickPlacementError::UnsupportedAxis { axis_type } => {
					format!("{axis}_axis().minor_ticks({minor_ticks}) does not apply to {axis_type:?} axes")
				}
				TickPlacementError::MinorCapacity { .. } => unreachable!(),
				TickPlacementError::TemporalRequiresExactDomain => unreachable!(),
			};
			(span.clone(), message)
		})?;
		placement
			.minor_positions(&major, minor_ticks)
			.map_err(|error| {
				let TickPlacementError::MinorCapacity {
					placement,
					minor_ticks,
					range,
					requested,
					limit,
				} = error
				else {
					unreachable!()
				};
				(
					span.clone(),
					format!(
						"{axis}_axis().minor_ticks({minor_ticks}) with {placement:?} over {range:?} generates {requested} total positions, exceeding the {limit}-position capacity"
					),
				)
			})
			.map(|_| ())
	};
	match *layout {
		PlotLayout::Cartesian {
			x_range, y_range, ..
		} => {
			check(settings.x_tick_placement, "x", x_range, &tick_step_spans.x)?;
			check(settings.y_tick_placement, "y", y_range, &tick_step_spans.y)?;
			check_minor(
				settings.x_tick_placement,
				"x",
				x_range,
				settings.x_minor_ticks,
				&minor_tick_spans.x,
			)?;
			check_minor(
				settings.y_tick_placement,
				"y",
				y_range,
				settings.y_minor_ticks,
				&minor_tick_spans.y,
			)
		}
		PlotLayout::CategoricalX { y_range, .. } => {
			check(settings.y_tick_placement, "y", y_range, &tick_step_spans.y)?;
			check_minor(
				settings.y_tick_placement,
				"y",
				y_range,
				settings.y_minor_ticks,
				&minor_tick_spans.y,
			)
		}
		PlotLayout::CategoricalY { x_range, .. } => {
			check(settings.x_tick_placement, "x", x_range, &tick_step_spans.x)?;
			check_minor(
				settings.x_tick_placement,
				"x",
				x_range,
				settings.x_minor_ticks,
				&minor_tick_spans.x,
			)
		}
		_ => Ok(()),
	}
}

#[cfg(feature = "toron")]
fn prepare_rune_categorical_palettes(
	app_state: &AppState,
	source_buffer: BufferId,
	view: &TransformedRuneRelation,
) -> HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette> {
	categorical_palettes_for(&view.views, |identity, len| {
		app_state
			.rune_categorical_identities
			.get(&(source_buffer, identity.clone(), len))
			.cloned()
	})
}

/// Each plot view's categorical palette, with the colour identity retained
/// for it (by view identity and palette length) or a fresh one.
#[cfg(feature = "toron")]
fn categorical_palettes_for(
	views: &[SpannedRuneViewIntent],
	retained: impl Fn(&LocalViewIdentity, usize) -> Option<ColorIdentity>,
) -> HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette> {
	views
		.iter()
		.filter_map(|view| {
			let RuneViewIntent::Plot(plot) = &view.intent else {
				return None;
			};
			let prepared = match resolved_rune_categorical_selection(&plot.authored_settings) {
				Ok(selection) => PreparedRuneCategoricalPalette::Ready {
					identity: retained(&view.identity, selection.len()).unwrap_or_default(),
					selection,
				},
				Err(error) => PreparedRuneCategoricalPalette::Failed(error),
			};
			Some((view.identity.clone(), prepared))
		})
		.collect()
}

#[cfg(all(test, feature = "toron"))]
fn default_rune_categorical_palettes(
	views: &[SpannedRuneViewIntent],
) -> HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette> {
	views
		.iter()
		.filter_map(|view| {
			let RuneViewIntent::Plot(plot) = &view.intent else {
				return None;
			};
			let prepared = match resolved_rune_categorical_selection(&plot.authored_settings) {
				Ok(selection) => PreparedRuneCategoricalPalette::Ready {
					selection,
					identity: ColorIdentity::default(),
				},
				Err(error) => PreparedRuneCategoricalPalette::Failed(error),
			};
			Some((view.identity.clone(), prepared))
		})
		.collect()
}

#[cfg(feature = "toron")]
async fn dispatch_rune_relation_with_palettes(
	connection: Arc<RwLock<dyn DatabaseAdapter>>,
	view: TransformedRuneRelation,
	palettes: HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette>,
) -> std::result::Result<crate::gui::messages::RuneRelationCompletion, String> {
	use crate::adapters::common::RuneDispatchSuccess;
	use crate::gui::messages::RuneRelationCompletion;
	let result = connection.write().await.dispatch_rune(&view.source).await;
	match result {
		// PRODUCT-0137: a receipt never passes through the view-set path —
		// that path projects immediately, and there is no artifact here.
		Ok(RuneDispatchSuccess::Materialized(receipt)) => {
			Ok(RuneRelationCompletion::Materialized(receipt))
		}
		// A terminal relation that returned rows asked for nothing. Refuse
		// with the terminal-view guidance rather than dropping the rows.
		Ok(RuneDispatchSuccess::Relation(_)) if view.views.is_empty() => Err(
			"Rune relation produced rows but no view; add a terminal view such as \
			 `.views([table()])`, or `.materialize(\"name\")` to keep it."
				.to_owned(),
		),
		Ok(RuneDispatchSuccess::Relation(artifact)) => {
			artifact.bind_producer("Toron");
			rune_view_outcome_with_palettes(artifact, view.views, palettes)
				.map(RuneRelationCompletion::Views)
		}
		Err(failure) => {
			let location = failure
				.span
				.and_then(|span| {
					view.source_map.map_span(toron_client::RuneSourceSpan {
						start: span.start,
						end: span.end,
					})
				})
				.unwrap_or_else(|| view.source_map.diagnostic_fallback());
			Err(format!(
				"Rune {:?} at bytes {}..{}: {}",
				failure.kind, location.start, location.end, failure.message
			))
		}
	}
}

#[cfg(all(test, toron_local))]
async fn dispatch_rune_relation(
	connection: Arc<RwLock<dyn DatabaseAdapter>>,
	view: TransformedRuneRelation,
) -> std::result::Result<crate::gui::messages::RuneRelationCompletion, String> {
	let palettes = default_rune_categorical_palettes(&view.views);
	dispatch_rune_relation_with_palettes(connection, view, palettes).await
}

#[cfg(feature = "toron")]
fn rune_table_skin_authored(
	intent: RuneTableSkinIntent,
) -> crate::plot::spec::Override<TableSkinSpec> {
	match intent {
		RuneTableSkinIntent::Default => crate::plot::spec::Override::Inherit,
		RuneTableSkinIntent::NamedTheme {
			name,
			projection: RuneTableSkinProjection::PaletteForward,
		} => crate::plot::spec::Override::Set(TableSkinSpec::new(
			name,
			TableSkinProjection::PaletteForward,
		)),
		RuneTableSkinIntent::NamedTheme {
			name,
			projection: RuneTableSkinProjection::Duotone,
		} => {
			crate::plot::spec::Override::Set(TableSkinSpec::new(name, TableSkinProjection::Duotone))
		}
	}
}

#[cfg(feature = "toron")]
fn resolve_rune_table(
	intent: RuneTableSink,
) -> std::result::Result<crate::gui::messages::RuneTablePresentation, String> {
	let completion = TableSkinCascade {
		authored: rune_table_skin_authored(intent.skin),
		..TableSkinCascade::default()
	}
	.resolve_through_authored()
	.map_err(|error| error.to_string())?;
	Ok(crate::gui::messages::RuneTablePresentation {
		skin: completion.skin,
		authored_show_column_types: intent.show_column_types,
	})
}

#[cfg(all(test, toron_local))]
fn resolve_rune_table_skin_with(
	intent: RuneTableSkinIntent,
	mut resolve: impl FnMut(
		&str,
		TableSkinProjection,
	) -> std::result::Result<TableSkin, TableSkinProjectionError>,
) -> std::result::Result<TableSkin, String> {
	TableSkinCascade {
		authored: rune_table_skin_authored(intent),
		..TableSkinCascade::default()
	}
	.resolve_through_authored_with(|spec| resolve(&spec.identity, spec.projection))
	.map(|completion| completion.skin)
	.map_err(|error| error.to_string())
}

#[cfg(feature = "toron")]
fn rune_view_outcome_with_palettes(
	result: Arc<crate::result_artifact::ResultArtifact>,
	views: Vec<SpannedRuneViewIntent>,
	mut palettes: HashMap<LocalViewIdentity, PreparedRuneCategoricalPalette>,
) -> std::result::Result<crate::gui::messages::RuneRelationViewSetOutcome, String> {
	use crate::gui::messages::{
		RuneCategoricalContinuation, RuneRelationSinkOutcome, RuneRelationSinkOutput,
		RuneRelationViewSetOutcome,
	};
	let frame = result.projection().map_err(|error| error.to_string())?;
	let sinks = views
		.into_iter()
		.map(|view| {
			let presentation = view.intent.presentation();
			let identity = view.identity;
			let figure_grid = match &view.intent {
				RuneViewIntent::Plot(plot) => {
					crate::plot::grid::FigureGridLayoutSettings::resolve(plot.facet_gutter)
				}
				RuneViewIntent::Table(_) => crate::plot::grid::FigureGridLayoutSettings::default(),
			};
			let (facet, facet_span) = match &view.intent {
				RuneViewIntent::Plot(plot) => (plot.facet.clone(), plot.facet_span.clone()),
				RuneViewIntent::Table(_) => (None, None),
			};
			let diagnostic_sink = match &view.intent {
				RuneViewIntent::Plot(plot) => match &plot.body {
					RunePlotBody::Single(sink) => Some(sink),
					RunePlotBody::StyledSingle(styled) => Some(&styled.sink),
					RunePlotBody::Layered(_) => None,
				},
				RuneViewIntent::Table(_) => None,
			};
			let box_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.box_plot()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let heatmap_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.heatmap()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let hexbin_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.hexbin()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let fill_between_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.fill_between()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let candlestick_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.candlestick()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let bubble_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.bubble()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let funnel_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.funnel()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let radial_dial_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.radial_dial()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let pie_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.pie()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let violin_diagnostic = diagnostic_sink.and_then(|sink| {
				let (fields, origins) = sink.mark_fields.as_ref()?.violin()?;
				Some((fields.clone(), origins.clone(), sink.origin.clone()))
			});
			let multivariate_diagnostic = diagnostic_sink.and_then(|sink| {
				let (plot_type, fields, origins) = sink.mark_fields.as_ref()?.multivariate()?;
				Some((
					plot_type,
					fields.clone(),
					origins.clone(),
					sink.origin.clone(),
				))
			});
			let body_output_span = match &view.intent {
				RuneViewIntent::Plot(plot) => match &plot.body {
					RunePlotBody::Single(sink)
						if sink.composition.primary_plot_type() == PlotType::Histogram =>
					{
						Some(sink.origin.clone())
					}
					RunePlotBody::StyledSingle(styled)
						if styled.sink.composition.primary_plot_type() == PlotType::Histogram =>
					{
						Some(styled.sink.origin.clone())
					}
					_ => None,
				},
				RuneViewIntent::Table(_) => None,
			};
			let (
				output,
				tick_step_output_span,
				minor_tick_output_span,
				continuous_intent_output_span,
			) = match view.intent {
				RuneViewIntent::Table(table) => (
					resolve_rune_table(table).map(RuneRelationSinkOutput::Table),
					None,
					RunePlotMinorTickSpans::default(),
					None,
				),
				RuneViewIntent::Plot(plot) => {
					let crate::rune_relation_envelope::RunePlotIntent {
						body,
						facet: plot_facet,
						facet_gutter: _,
						authored_settings,
						plot_skin_span,
						facet_span: _,
						domain_spans,
						tick_step_spans,
						tick_every_spans,
						minor_tick_spans,
						continuous_intent,
						continuous_intent_span,
					} = plot;
					let authored_settings = match resolve_rune_plot_skin(authored_settings) {
						Ok(settings) => settings,
						Err(error) => {
							return RuneRelationSinkOutcome {
								identity,
								presentation,
								span: plot_skin_span.unwrap_or(view.span),
								facet: plot_facet.clone(),
								figure_grid,
								source: std::sync::Arc::clone(&result),
								output: Err(error),
							};
						}
					};
					if let Err((span, error)) = validate_rune_numeric_domain_applicability(
						&body,
						&authored_settings,
						&domain_spans,
						&tick_step_spans,
						&tick_every_spans,
						&minor_tick_spans,
						frame.schema().as_ref(),
					) {
						return RuneRelationSinkOutcome {
							identity,
							presentation,
							span,
							facet: plot_facet.clone(),
							figure_grid,
							source: std::sync::Arc::clone(&result),
							output: Err(error),
						};
					}
					let prepared = palettes.remove(&identity).unwrap_or_else(|| {
						PreparedRuneCategoricalPalette::Failed(
							"plot destination has no prepared categorical palette".to_owned(),
						)
					});
					let PreparedRuneCategoricalPalette::Ready {
						selection,
						identity: prior_identity,
					} = prepared
					else {
						let PreparedRuneCategoricalPalette::Failed(error) = prepared else {
							unreachable!()
						};
						return RuneRelationSinkOutcome {
							identity,
							presentation,
							span: view.span,
							facet: plot_facet.clone(),
							figure_grid,
							source: std::sync::Arc::clone(&result),
							output: Err(error),
						};
					};
					let palette_len = selection.len();
					let resolved_tick_settings = PlotSettings {
						authored_spec: authored_settings.clone(),
						..PlotSettings::default()
					}
					.resolve();
					let tick_step_output_span = tick_step_spans
						.x
						.clone()
						.or_else(|| tick_step_spans.y.clone())
						.or_else(|| tick_every_spans.x.clone())
						.or_else(|| tick_every_spans.y.clone())
						.or_else(|| minor_tick_spans.x.clone())
						.or_else(|| minor_tick_spans.y.clone());
					let has_authored_minor_ticks =
						minor_tick_spans.x.is_some() || minor_tick_spans.y.is_some();
					let output = match body {
						RunePlotBody::Single(plot) => {
							let authored_spec = with_rune_continuous_intent(
								authored_rune_plot_spec(&plot),
								continuous_intent,
							);
							plan_with_categorical_palette(
								PlotRequest {
									source: PlotSource::Materialized {
										name: "workspace relation".to_owned(),
										data: std::sync::Arc::clone(&result),
									},
									authored_spec,
								},
								&selection,
								&prior_identity,
							)
							.and_then(|(resolved, next_identity)| {
								if tick_step_spans.x.is_none()
									&& tick_step_spans.y.is_none()
									&& tick_every_spans.x.is_none()
									&& tick_every_spans.y.is_none()
									&& !has_authored_minor_ticks
								{
									return Ok(RuneRelationSinkOutput::Plot(
										resolved,
										authored_settings,
										RuneCategoricalContinuation {
											palette_len,
											identity: next_identity,
										},
									));
								}
								let layout = resolved_plot_layout(
									&resolved.resolved_spec,
									&resolved.data,
									&resolved_tick_settings,
								);
								validate_rune_tick_capacity(
									&layout,
									&resolved_tick_settings,
									&tick_step_spans,
									&tick_every_spans,
									&minor_tick_spans,
								)
								.map_err(|(_, error)| error)?;
								Ok(RuneRelationSinkOutput::Plot(
									resolved,
									authored_settings,
									RuneCategoricalContinuation {
										palette_len,
										identity: next_identity,
									},
								))
							})
						}
						RunePlotBody::StyledSingle(styled) => match styled.into_layer() {
							Ok(layer) => {
								let specification = LayeredPlotSpec::new(layer);
								if tick_step_spans.x.is_none()
									&& tick_step_spans.y.is_none()
									&& tick_every_spans.x.is_none()
									&& tick_every_spans.y.is_none()
									&& !has_authored_minor_ticks
								{
									Ok(RuneRelationSinkOutput::LayeredPlot(
										specification,
										authored_settings,
									))
								} else {
									layered_plot_layout(
										&specification,
										frame.as_ref(),
										&resolved_tick_settings,
									)
									.and_then(|layout| {
										validate_rune_tick_capacity(
											&layout,
											&resolved_tick_settings,
											&tick_step_spans,
											&tick_every_spans,
											&minor_tick_spans,
										)
										.map_err(|(_, error)| error)
										.map(|_| {
											RuneRelationSinkOutput::LayeredPlot(
												specification,
												authored_settings,
											)
										})
									})
								}
							}
							Err(styled) => {
								let StyledRunePlotSink {
									sink,
									settings,
									stacked,
									percent,
								} = styled;
								let authored_spec = with_rune_continuous_intent(
									authored_rune_plot_spec(&sink),
									continuous_intent,
								);
								let authored_spec = if stacked {
									authored_spec.with_stacked_bar()
								} else {
									authored_spec
								};
								let authored_spec = if percent {
									authored_spec.with_percent()
								} else {
									authored_spec
								};
								plan_with_categorical_palette(
									PlotRequest {
										source: PlotSource::Materialized {
											name: "workspace relation".to_owned(),
											data: std::sync::Arc::clone(&result),
										},
										authored_spec,
									},
									&selection,
									&prior_identity,
								)
								.and_then(|(resolved, next_identity)| {
									if tick_step_spans.x.is_none()
										&& tick_step_spans.y.is_none() && tick_every_spans
										.x
										.is_none() && tick_every_spans.y.is_none()
										&& !has_authored_minor_ticks
									{
										return Ok(RuneRelationSinkOutput::StyledPlot(
											resolved,
											settings,
											authored_settings,
											RuneCategoricalContinuation {
												palette_len,
												identity: next_identity,
											},
										));
									}
									styled_plot_layout(
										&resolved.resolved_spec,
										&resolved.data,
										&resolved_tick_settings,
									)
									.and_then(|layout| {
										validate_rune_tick_capacity(
											&layout,
											&resolved_tick_settings,
											&tick_step_spans,
											&tick_every_spans,
											&minor_tick_spans,
										)
										.map_err(|(_, error)| error)
										.map(|_| {
											RuneRelationSinkOutput::StyledPlot(
												resolved,
												settings,
												authored_settings,
												RuneCategoricalContinuation {
													palette_len,
													identity: next_identity,
												},
											)
										})
									})
								})
							}
						},
						RunePlotBody::Layered(specification) => {
							if tick_step_spans.x.is_none()
								&& tick_step_spans.y.is_none()
								&& tick_every_spans.x.is_none()
								&& tick_every_spans.y.is_none()
								&& !has_authored_minor_ticks
							{
								Ok(RuneRelationSinkOutput::LayeredPlot(
									specification,
									authored_settings,
								))
							} else {
								layered_plot_layout(
									&specification,
									frame.as_ref(),
									&resolved_tick_settings,
								)
								.and_then(|layout| {
									validate_rune_tick_capacity(
										&layout,
										&resolved_tick_settings,
										&tick_step_spans,
										&tick_every_spans,
										&minor_tick_spans,
									)
									.map_err(|(_, error)| error)
									.map(|_| {
										RuneRelationSinkOutput::LayeredPlot(
											specification,
											authored_settings,
										)
									})
								})
							}
						}
					};
					(
						output,
						tick_step_output_span,
						minor_tick_spans,
						continuous_intent_span,
					)
				}
			};
			let continuous_output_span = output.as_ref().err().and_then(|error| {
				(error.contains("authored continuous colour anchor"))
					.then(|| continuous_intent_output_span.clone())
					.flatten()
			});
			let output_span = output.as_ref().err().and_then(|error| {
				if error.contains(".minor_ticks") {
					minor_tick_output_span
						.x
						.clone()
						.or_else(|| minor_tick_output_span.y.clone())
						.or(tick_step_output_span)
				} else {
					tick_step_output_span
						.or_else(|| minor_tick_output_span.x.clone())
						.or_else(|| minor_tick_output_span.y.clone())
				}
			});
			let box_output_span = output.as_ref().err().and_then(|_| {
				box_diagnostic.as_ref().map(|(fields, origins, mark)| {
					box_plot_failure_role(frame.as_ref(), fields)
						.map(|role| origins.for_role(role))
						.unwrap_or_else(|| mark.clone())
				})
			});
			let heatmap_output_span = output.as_ref().err().and_then(|_| {
				heatmap_diagnostic.as_ref().map(|(fields, origins, mark)| {
					heatmap_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let hexbin_output_span = output.as_ref().err().and_then(|_| {
				hexbin_diagnostic.as_ref().map(|(fields, origins, mark)| {
					hexbin_failure_span(
						frame.as_ref(),
						result.artifact_token(),
						fields,
						origins,
						mark,
					)
				})
			});
			let fill_between_output_span = output.as_ref().err().and_then(|_| {
				fill_between_diagnostic
					.as_ref()
					.map(|(fields, origins, mark)| {
						fill_between_failure_span(frame.as_ref(), fields, origins, mark)
					})
			});
			let candlestick_output_span = output.as_ref().err().and_then(|_| {
				candlestick_diagnostic
					.as_ref()
					.map(|(fields, origins, mark)| {
						candlestick_failure_span(frame.as_ref(), fields, origins, mark)
					})
			});
			let bubble_output_span = output.as_ref().err().and_then(|_| {
				bubble_diagnostic.as_ref().map(|(fields, origins, mark)| {
					bubble_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let funnel_output_span = output.as_ref().err().and_then(|_| {
				funnel_diagnostic.as_ref().map(|(fields, origins, mark)| {
					funnel_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let radial_dial_output_span = output.as_ref().err().and_then(|_| {
				radial_dial_diagnostic
					.as_ref()
					.map(|(fields, origins, mark)| {
						radial_dial_failure_span(frame.as_ref(), fields, origins, mark)
					})
			});
			let pie_output_span = output.as_ref().err().and_then(|_| {
				pie_diagnostic.as_ref().map(|(fields, origins, mark)| {
					pie_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let violin_output_span = output.as_ref().err().and_then(|_| {
				violin_diagnostic.as_ref().map(|(fields, origins, mark)| {
					violin_failure_span(frame.as_ref(), fields, origins, mark)
				})
			});
			let multivariate_output_span = output.as_ref().err().and_then(|_| {
				multivariate_diagnostic
					.as_ref()
					.map(|(plot_type, fields, origins, mark)| {
						multivariate_failure_span(frame.as_ref(), *plot_type, fields, origins, mark)
					})
			});
			RuneRelationSinkOutcome {
				identity,
				presentation,
				span: continuous_output_span
					.or(output_span)
					.or(box_output_span)
					.or(heatmap_output_span)
					.or(hexbin_output_span)
					.or(fill_between_output_span)
					.or(candlestick_output_span)
					.or(bubble_output_span)
					.or(funnel_output_span)
					.or(radial_dial_output_span)
					.or(pie_output_span)
					.or(violin_output_span)
					.or(multivariate_output_span)
					.or(body_output_span)
					.or(facet_span)
					.unwrap_or(view.span),
				facet,
				figure_grid,
				source: std::sync::Arc::clone(&result),
				output,
			}
		})
		.collect();
	Ok(RuneRelationViewSetOutcome { result, sinks })
}

#[cfg(all(test, toron_local))]
fn rune_view_outcome(
	frame: DataFrame,
	views: Vec<SpannedRuneViewIntent>,
) -> crate::gui::messages::RuneRelationViewSetOutcome {
	let palettes = default_rune_categorical_palettes(&views);
	rune_view_outcome_with_palettes(
		crate::result_artifact::ResultArtifact::from_polars(Arc::new(frame)),
		views,
		palettes,
	)
	.expect("test frame projects")
}

fn queue_plain_run(
	app_state: &mut AppState,
	language_id: libaryon::engine::LanguageId,
	kind: PendingRunKind,
) -> Task<Message> {
	let Some(source_buffer) = app_state.code_editor.active_buffer_id() else {
		app_state.status_error = "The focused source has no buffer identity.".to_owned();
		return Task::none();
	};
	let Some(snapshot_request) = app_state.code_editor.request_execution_snapshot() else {
		app_state.status_error = "The editor could not snapshot the focused source.".to_owned();
		return Task::none();
	};
	app_state.pending_runs.push_back(PendingRun {
		snapshot_request,
		source_buffer,
		language_id,
		kind,
	});
	app_state.status_msg = "Preparing code…".into();
	Task::none()
}

fn finish_pending_runs(app_state: &mut AppState) -> Task<Message> {
	let mut tasks = Vec::new();
	while let Some(snapshot_request) = app_state
		.pending_runs
		.front()
		.map(|pending| pending.snapshot_request)
	{
		let Some(snapshot) = app_state
			.code_editor
			.take_execution_snapshot(snapshot_request)
		else {
			break;
		};
		let pending = app_state
			.pending_runs
			.pop_front()
			.expect("pending run disappeared");
		if snapshot.buffer_id != pending.source_buffer {
			app_state.status_error =
				"The source pane changed before its execution snapshot completed.".to_owned();
			continue;
		}
		let code = match pending.kind {
			PendingRunKind::Buffer => snapshot.text.clone(),
			PendingRunKind::Snippet => {
				let Some(code) = app_state.code_editor.selected_or_current_line() else {
					app_state.status_msg = "No editor text is available to run.".into();
					continue;
				};
				code
			}
		};
		tasks.push(run_language_code(
			app_state,
			pending.source_buffer,
			pending.language_id,
			code,
			&snapshot.text,
			snapshot.state_id,
		));
	}
	Task::batch(tasks)
}

fn run_markdown_cell(app_state: &mut AppState, cell: MarkdownCellExecution) -> Task<Message> {
	match cell.language_id.as_str() {
		"sql" => run_sql_markdown_cell(app_state, cell),
		"rune" => run_rune_markdown_cell(app_state, cell),
		language => {
			app_state.status_msg = format!("Unsupported notebook cell language: {language}");
			Task::none()
		}
	}
}

fn run_sql_markdown_cell(app_state: &mut AppState, cell: MarkdownCellExecution) -> Task<Message> {
	let Some(db) = app_state.adapter_state.sql_connection() else {
		set_sql_error(
			app_state,
			"No SQL target: scratch database unavailable.".to_owned(),
			Some(cell.source_start_line),
		);
		return Task::none();
	};
	if cell.code.trim().is_empty() {
		set_sql_status(app_state, "SQL · nothing to run in this cell");
		return Task::none();
	}
	let db = db.clone();
	let identity = cell.identity.clone();
	let presentation = cell.presentation;
	let source_start_line = cell.source_start_line;
	set_sql_status(app_state, "SQL · running…");
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	// Starting any SQL cell retires its prior named snapshot immediately.
	// Presentation only decides where the new result appears; it must not let a
	// Rune cell observe stale data while this query is in flight.
	app_state
		.code_editor
		.clear_markdown_cell_artifact(&cell.identity);
	let generation = app_state.current_sql_generation();
	let current = Arc::clone(&app_state.sql_run_generation);
	let (task, handle) = Task::perform(
		dispatch_sql_gated(
			db,
			cell.code,
			generation,
			current,
			app_state
				.adapter_state
				.selection
				.adapter_type_str()
				.to_owned(),
		),
		move |result| Message::RunMarkdownCellResult {
			identity,
			presentation,
			source_start_line,
			result,
			generation,
		},
	)
	.abortable();
	app_state.sql_abort = Some(handle);
	Task::batch([
		app_state
			.code_editor
			.take_selection_tasks()
			.map(Message::CodeEditEvent),
		task,
	])
}

fn resolve_notebook_rune_execution(
	mut execution: rune_host::RuneExecution,
) -> std::result::Result<NotebookRuneOutcome, NotebookRuneFailure> {
	let receipts = std::mem::take(&mut execution.receipts);
	let plot = match execution.requests.len() {
		1 => plan(execution.requests.remove(0)).map(Some),
		0 if receipts.is_empty() => {
			Err("Rune cell did not publish a plot; call `.show()?`.".to_owned())
		}
		0 => Ok(None),
		_ => Err(
			"Rune cell published multiple plots; one cell currently owns one output.".to_owned(),
		),
	};
	match plot {
		Ok(plot) => Ok(NotebookRuneOutcome { plot, receipts }),
		Err(message) => Err(NotebookRuneFailure { message, receipts }),
	}
}

fn run_rune_markdown_cell(app_state: &mut AppState, cell: MarkdownCellExecution) -> Task<Message> {
	if cell.code.trim().is_empty() {
		app_state.status_msg = "Nothing to run in this Rune cell.".into();
		return Task::none();
	}
	let snapshots = app_state.code_editor.named_notebook_cell_data();
	let results = match app_state.result_registry.snapshot() {
		Ok(results) => results,
		Err(error) => {
			app_state.status_error = error.to_string();
			return Task::none();
		}
	};
	let identity = cell.identity.clone();
	let presentation = cell.presentation;
	app_state.status_msg = "Building notebook plot…".into();
	app_state.status_error.clear();
	app_state.status_time_elapsed = None;
	app_state.code_started = Instant::now();
	if presentation.output == MarkdownCellOutputTarget::Inline {
		app_state.code_editor.begin_markdown_cell(identity.clone());
	}
	let task = Task::perform(
		async move {
			tokio::task::spawn_blocking(move || {
				let execution = rune_host::execute_body(
					&cell.code,
					RuneCapabilities {
						cells: snapshots,
						results,
					},
				)
				.map_err(|message| NotebookRuneFailure {
					message,
					receipts: Vec::new(),
				})?;
				resolve_notebook_rune_execution(execution)
			})
			.await
			.map_err(|error| NotebookRuneFailure {
				message: format!("Rune notebook task panicked: {error}"),
				receipts: Vec::new(),
			})?
		},
		move |result| Message::RunRuneCellResult {
			identity,
			presentation,
			result,
		},
	);
	Task::batch([
		app_state
			.code_editor
			.take_selection_tasks()
			.map(Message::CodeEditEvent),
		task,
	])
}

fn export_receipt_status(receipts: &[rune_host::ExportReceipt]) -> String {
	if receipts.len() == 1 {
		return receipts[0].status_message();
	}
	let rows = receipts.iter().map(|receipt| receipt.rows).sum::<usize>();
	format!(
		"Exported {} files ({} rows total): {}.",
		receipts.len(),
		rows,
		receipts
			.iter()
			.map(|receipt| receipt.path.as_str())
			.collect::<Vec<_>>()
			.join(", ")
	)
}

fn begin_window_shutdown(_app_state: &mut AppState) -> Task<Message> {
	{
		_app_state.agents.closing = true;
		_app_state.agents.disable();
	}
	#[cfg(toron_local)]
	{
		if _app_state.closing_local_workspace {
			return Task::none();
		}
		if let Some(owner) = _app_state.local_workspace_owner.take() {
			_app_state.closing_local_workspace = true;
			_app_state.status_error.clear();
			_app_state.status_msg =
				"Stopping the local workspace: draining Toron before Wendelon…".into();
			return Task::perform(
				async move {
					tokio::task::spawn_blocking(move || {
						owner.stop(crate::local_workspace::STOP_DEADLINE)
					})
					.await
					.map_or_else(
						|error| format!("local workspace stop task failed: {error}"),
						|outcome| format!("{outcome:?}"),
					)
				},
				Message::LocalWorkspaceStopped,
			);
		}
	}
	window::latest().and_then(window::close)
}

fn update(app_state: &mut AppState, message: Message) -> Task<Message> {
	app_state.observe_welcome_message(&message);
	let task = update_inner(app_state, message);
	reap_closed_local_runs(app_state);
	app_state.settle_local_batch();
	app_state.expire_notebook_action();
	let welcome_task = app_state.advance_welcome();
	app_state.capture_surface_baselines();
	app_state.refresh_agents();
	Task::batch([task, welcome_task])
}

fn update_inner(app_state: &mut AppState, message: Message) -> Task<Message> {
	// ADR 0166 §3: selection changes mint a generation here; connect attempts
	// and disconnects mint theirs inside the shared connection operation.
	if matches!(&message, Message::AdapterSelected(_)) {
		app_state.next_connection_generation();
	}
	#[cfg(toron_local)]
	if matches!(&message, Message::LocalWorkspaceAttached { .. }) {
		app_state.next_connection_generation();
	}
	// Gate closure must also work while a saved window is finishing shutdown.
	match message {
		Message::NotebookTask { id, message } => return app_state.notebook_task(id, *message),
		Message::NotebookDataLoaded { id, result } => {
			return app_state.notebook_data_loaded(id, result);
		}

		#[cfg(toron_local)]
		Message::WelcomeWorkspace => {
			app_state.welcome.workspace_requested = true;
			return Task::perform(
				nilket_local::discover(None),
				Message::LocalWorkspaceDiscovered,
			);
		}
		#[cfg(toron_local)]
		Message::WelcomeSeeded(result) => {
			match result {
				Ok(()) => {
					app_state
						.code_editor
						.open_welcome_rune(welcome::WORKSPACE_SOURCE);
					app_state.welcome.stage = 4;
				}
				Err(e) => {
					app_state.status_error = e;
					app_state.reporting.emit(
						crate::reporting::Step::WorkspaceFailed,
						Some(crate::reporting::Reason::ExampleSeedFailed),
						true,
					);
				}
			}
			return Task::none();
		}
		Message::WelcomePath(path) => {
			app_state.welcome.path = path;
			return Task::none();
		}
		Message::WelcomeOpen => return app_state.start_welcome_open(),
		Message::AgentInbound(inbound) => {
			return app_state.agent_inbound(inbound);
		}
		Message::AgentTick(now) => {
			app_state.finish_agent_resizes(now);
			return Task::none();
		}
		Message::AgentPublishFinished {
			id,
			reply,
			mut receipt,
			root,
			result,
		} => {
			match result {
				Ok(published) => {
					receipt.publish = Some(published);
					reply.answer(crate::live::Event::Completed { id, receipt });
				}
				Err(reason) => {
					receipt.fail(crate::driver::Failure::Publish {
						reason: reason.clone(),
						root: root.clone(),
					});
					reply.answer(crate::live::Event::Failed {
						id,
						reason: "publish_failed".into(),
						error: Some(reason),
						root: Some(root),
						receipt,
					});
				}
			}
			return Task::none();
		}
		Message::ToggleAgents => {
			if let Err(error) = app_state.toggle_agents() {
				app_state.status_error = error;
			}
			return Task::none();
		}
		_ => {}
	}

	#[cfg(toron_local)]
	if app_state.saved_close_state == SavedCloseState::Frozen
		&& !matches!(&message, Message::LocalWorkspaceStopped(_))
	{
		return Task::none();
	}
	match message {
		Message::AgentInbound(_) | Message::AgentTick(_) | Message::AgentPublishFinished { .. } => {
			unreachable!()
		}
		Message::WelcomePath(_) | Message::WelcomeOpen => {
			unreachable!()
		}
		#[cfg(toron_local)]
		Message::WelcomeWorkspace | Message::WelcomeSeeded(_) => unreachable!(),
		Message::ToggleAgents => unreachable!(),
		Message::ToggleStatusDetail => {
			if app_state.status_detail.is_some() {
				app_state.status_detail_open = !app_state.status_detail_open;
			}
		}
		Message::CloseStatusDetail => {
			app_state.status_detail_open = false;
		}
		Message::CheckpointSession => {
			#[cfg(feature = "toron")]
			{
				if app_state.session_id.is_none() {
					app_state.status_error = "This editor is not a saved session.".to_owned();
					return Task::none();
				}
				let colours = crate::session_persistence::RuntimeColours::new(
					app_state.rune_categorical_identities.clone(),
				);
				match app_state.code_editor.checkpoint_session(&colours) {
					Ok(()) => {
						app_state.saved_close_state = SavedCloseState::Open;
						app_state.status_error.clear();
						app_state.status_msg = "Session checkpointed".to_owned();
					}
					Err(error) => {
						app_state.status_msg.clear();
						app_state.status_error = format!("Session checkpoint failed: {error}");
					}
				}
			}
		}
		Message::NotebookTask { .. } | Message::NotebookDataLoaded { .. } => unreachable!(),
		Message::CopySessionId => {
			if let Some(session_id) = app_state.session_id {
				return iced::clipboard::write(session_identity_text(session_id));
			}
		}
		Message::CloseWithoutCheckpoint => {
			if app_state.session_id.is_some()
				&& app_state.saved_close_state == SavedCloseState::Failed
			{
				app_state.saved_close_state = SavedCloseState::Frozen;
				app_state.pending_runs.clear();
				return begin_window_shutdown(app_state);
			}
		}
		Message::CopyStatusDetail => {
			if let Some(detail) = app_state.status_detail.as_ref() {
				return iced::clipboard::write(detail.full.clone());
			}
		}
		Message::StatusDetailAction(action) => {
			if !action.is_edit()
				&& let Some(detail) = app_state.status_detail.as_mut()
			{
				detail.content.perform(action);
			}
		}
		Message::CloseWindow => {
			#[cfg(toron_local)]
			{
				if app_state.closing_local_workspace {
					return Task::none();
				}
				if app_state.session_id.is_some() {
					let colours = crate::session_persistence::RuntimeColours::new(
						app_state.rune_categorical_identities.clone(),
					);
					match app_state.code_editor.checkpoint_session(&colours) {
						Ok(()) => {
							app_state.saved_close_state = SavedCloseState::Frozen;
							app_state.pending_runs.clear();
							return begin_window_shutdown(app_state);
						}
						Err(error) => {
							app_state.saved_close_state = SavedCloseState::Failed;
							app_state.status_msg.clear();
							app_state.status_error = if error.is_stale() {
								format!(
									"Session close checkpoint failed because the workspace changed before the FIFO head; close again to retry: {error}"
								)
							} else {
								format!("Session close checkpoint failed: {error}")
							};
							return Task::none();
						}
					}
				}
			}
			return begin_window_shutdown(app_state);
		}
		Message::Menu(menu_message) => {
			if let Some(action) = app_state.menu_state.update(menu_message)
				&& let Some(message) = menu_action_to_message(&action)
			{
				return update(app_state, message);
			}
		}
		Message::DragWindow => {
			return window::latest().and_then(window::drag);
		}
		Message::CodeEditEvent(edit_event) => {
			clear_transient_execution_status_on_editor_activity(app_state, &edit_event);
			let task = app_state
				.code_editor
				.update(edit_event)
				.map(Message::CodeEditEvent);
			if let Some(selection) = app_state.code_editor.take_selection_status() {
				app_state.status_msg = selection;
				app_state.status_error.clear();
				app_state.status_detail = None;
				app_state.status_detail_open = false;
			}
			#[cfg(feature = "toron")]
			if let Some(colours) = app_state.code_editor.take_restored_colour_meanings() {
				app_state.rune_categorical_identities = colours.into_map();
			}
			#[cfg(feature = "toron")]
			if let Some(error) = app_state.code_editor.take_session_restore_error() {
				app_state.status_error = error;
			}
			if app_state.code_editor.take_quit_request() {
				return update(app_state, Message::CloseWindow);
			}
			let acknowledgements = app_state
				.code_editor
				.take_document_edit_results()
				.collect::<Vec<_>>();
			let mut notebook_tasks = Vec::new();
			for acknowledgement in acknowledgements {
				if let Some(task) = app_state.notebook_edit_ack(&acknowledgement) {
					notebook_tasks.push(task);
				} else {
					#[cfg(feature = "toron")]
					settle_settings_commit(app_state, acknowledgement);
				}
			}

			let run_task =
				Task::batch([finish_pending_runs(app_state), Task::batch(notebook_tasks)]);
			#[cfg(feature = "toron")]
			let settings_tasks = {
				let gestures = app_state
					.code_editor
					.take_plot_writeback_gestures()
					.collect::<Vec<_>>();
				gestures
					.into_iter()
					.map(|gesture| queue_settings_preview(app_state, gesture))
					.collect::<Vec<_>>()
			};
			#[cfg(feature = "toron")]
			return Task::batch(
				std::iter::once(task)
					.chain(std::iter::once(run_task))
					.chain(settings_tasks),
			);
			#[cfg(not(feature = "toron"))]
			return Task::batch([task, run_task]);
		}
		Message::CodeEditResize(size) => {
			app_state.window_size = size;
			sync_workspace_viewport(app_state);
		}
		Message::LoopLatencyTick(scheduled) => {
			if let Some(probe) = app_state.loop_latency.as_mut() {
				probe.tick(scheduled);
			}
		}
		Message::MaximizeWindow => {
			app_state.is_maximized = !app_state.is_maximized;
			let is_maximized = app_state.is_maximized;
			return window::latest().and_then(move |id| window::maximize(id, is_maximized));
		}
		Message::MinimizeWindow => {
			return window::latest().and_then(move |id| window::minimize(id, true));
		}
		Message::ResizeWindow(direction) => {
			return window::latest().and_then(move |id| window::drag_resize(id, direction));
		}
		Message::Connect => match app_state.adapter_state.stage {
			// PRODUCT-0134: `connect:new` stays a session-scoped, unsaved
			// connection; it never edits a stored profile.
			AdapterStage::None => {
				app_state.editing_profile = None;
				app_state.adapter_state.stage = AdapterStage::Unselected;
				app_state.status_msg = "Select an adapter.".into();
			}
			_ => {
				app_state.adapter_state.stage = AdapterStage::None;
				app_state.status_msg = "No adapter selected.".into();
			}
		},
		Message::AdapterSelected(adapter_selection) => {
			app_state.status_msg = format!("Adapter selected: {:?}", adapter_selection);
			app_state.adapter_state.select(adapter_selection);
		}
		// PRODUCT-0134 §8/§9: the profile-store unlock flow. The prompt opens
		// only on an access request; cancel and failure leave the application
		// fully usable with no adapter connected — the scratch target is never
		// silently substituted for a saved profile, and the store is never
		// cleared, rewritten, or migrated on a failed open.
		Message::ProfileStoreAccessRequested => {
			return request_profile_access(
				app_state,
				crate::profile_store::ProfileOperation::Browse,
			);
		}
		Message::ProfileSaveRequested => {
			return request_profile_access(
				app_state,
				crate::profile_store::ProfileOperation::SaveCurrent,
			);
		}
		Message::ProfileConnectRequested(name) => {
			return request_profile_access(
				app_state,
				crate::profile_store::ProfileOperation::Connect(name),
			);
		}
		Message::ProfileEditRequested(name) => {
			return request_profile_access(
				app_state,
				crate::profile_store::ProfileOperation::Edit(name),
			);
		}
		Message::ProfileDeleteRequested(name) => {
			return request_profile_access(
				app_state,
				crate::profile_store::ProfileOperation::Delete(name),
			);
		}
		Message::ProfilePassphraseChanged(value) => {
			if let Some(prompt) = &mut app_state.profile_unlock {
				prompt.passphrase = zeroize::Zeroizing::new(value);
			}
		}
		Message::ProfilePassphraseConfirmationChanged(value) => {
			if let Some(prompt) = &mut app_state.profile_unlock {
				prompt.confirmation = zeroize::Zeroizing::new(value);
			}
		}
		Message::ProfileUnlockSubmitted => {
			let Some(prompt) = &app_state.profile_unlock else {
				return Task::none();
			};
			let Some(path) = app_state.profile_store_path.clone() else {
				return Task::none();
			};
			let creating = prompt.creating;
			if prompt.passphrase.is_empty() {
				if let Some(prompt) = &mut app_state.profile_unlock {
					prompt.error = Some("A passphrase is required.".to_owned());
				}
				return Task::none();
			}
			// The first-write flow refuses a mismatched double entry before
			// any file exists: a typo here would seal an unopenable store.
			if creating && *prompt.passphrase != *prompt.confirmation {
				if let Some(prompt) = &mut app_state.profile_unlock {
					prompt.error = Some("Passphrases do not match.".to_owned());
				}
				return Task::none();
			}
			let passphrase = zeroize::Zeroizing::new(prompt.passphrase.as_bytes().to_vec());
			let outcome = if creating {
				app_state.profile_store.initialize(&path, passphrase)
			} else {
				app_state.profile_store.unlock(&path, passphrase)
			};
			match outcome {
				Ok(()) => {
					app_state.profile_unlock = None;
					app_state.status_msg = if creating {
						"Profile store created.".into()
					} else {
						"Profile store unlocked.".into()
					};
					app_state.status_error.clear();
					if let Some(operation) = app_state.pending_profile_operation.take() {
						return apply_profile_operation(app_state, operation);
					}
				}
				Err(error) => {
					// Retry stays available; the sealed file is untouched.
					if let Some(prompt) = &mut app_state.profile_unlock {
						prompt.error = Some(error);
					}
				}
			}
		}
		Message::ProfileUnlockCancelled => {
			app_state.profile_unlock = None;
			app_state.pending_profile_operation = None;
			// Routine interaction feedback, not an error: the saved-profile
			// surface is disabled while locked, everything else still works.
			app_state.status_msg =
				"Profile store stays locked; ad hoc connections remain available.".into();
		}
		// PRODUCT-0135 §1/§4/§5: Cancel. The generation advances before the
		// abort and before any engine-side signal — edge two of the ordering
		// contract — so every lock-waiting task is stale by the time a guard
		// releases. Refusal does nothing at all except say so.
		Message::CancelRun => {
			if app_state.sql_abort.is_none() {
				set_sql_status(app_state, "SQL · nothing to cancel");
				return Task::none();
			}
			match app_state.adapter_state.active_cancel_capability() {
				crate::adapters::common::CancelCapability::Refuse => {
					set_sql_status(
						app_state,
						"SQL · this source cannot be cancelled; the query runs to completion",
					);
				}
				crate::adapters::common::CancelCapability::StopWaiting => {
					cancel_sql_generation(app_state);
					if let Some(handle) = app_state.sql_abort.take() {
						handle.abort();
					}
					set_sql_status(
						app_state,
						"SQL · stopped waiting; the engine may still be finishing",
					);
				}
				crate::adapters::common::CancelCapability::Engine(signal) => {
					cancel_sql_generation(app_state);
					if let Some(handle) = app_state.sql_abort.take() {
						handle.abort();
					}
					set_sql_status(app_state, "SQL · cancelling…");
					return Task::perform(
						async move { signal.signal().await },
						Message::CancelSignalSettled,
					);
				}
			}
		}
		Message::CancelSignalSettled(result) => match result {
			Ok(()) => set_sql_status(app_state, "SQL · cancelled"),
			Err(error) => set_sql_status(
				app_state,
				format!("SQL · stopped waiting; the engine cancel failed: {error}"),
			),
		},
		Message::AdapterAdHocChanged(value) => {
			app_state.adapter_state.ad_hoc_connection_string = value;
			app_state.status_msg = "Configure adapter.".into();
		}
		Message::AdapterConfigurationChanged(key, value) => {
			app_state.adapter_state.fields.insert(key, value);
			app_state.status_msg = "Configure adapter.".into();
		}
		Message::AdapterConfigurationSubmitted => {
			// PRODUCT-0134 §1 and ADR 0166 §2: the form enters the same validation
			// and connection operation the `connect` command does.
			if let Err(error) = app_state.validate_connection_submission() {
				app_state.status_error = error;
				return Task::none();
			}
			return app_state.begin_connect();
		}
		Message::AdapterConnected { generation, result } => {
			return app_state.connect_settled(generation, result);
		}
		Message::AdapterDisconnectRequested => {
			let generation = app_state.connection_generation;
			if let Err(error) = app_state.disconnect_external(generation) {
				app_state.status_error = error;
			}
			return Task::none();
		}
		#[cfg(toron_local)]
		Message::LocalWorkspaceDiscovered(workspace) => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			app_state.adapter_state.local_workspace = Some(workspace.clone());
			app_state.local_creation.observe(&workspace);
			let nilket_local::LocalWorkspace::Ready(ready) = workspace else {
				app_state.status_error = workspace.to_string();
				app_state.status_msg = app_state.local_creation.offer().map_or_else(
					|| "SQL scratch remains active.".to_owned(),
					|root| {
						format!(
							"Local workspace is absent. Start a session-bound workspace at {}?",
							root.display()
						)
					},
				);
				return Task::none();
			};
			app_state.status_error.clear();
			app_state.status_msg = "Attaching Toron workspace…".to_owned();
			let endpoint = ready.sql_endpoint.to_string();
			let message_workspace = ready.clone();
			return Task::perform(
				async move {
					crate::adapters::toron::ToronAdapter::connect(&endpoint)
						.await
						.map(|adapter| {
							let cancellation = adapter.cancel_capability();
							(
								Arc::new(RwLock::new(adapter))
									as Arc<RwLock<dyn crate::adapters::common::DatabaseAdapter>>,
								cancellation,
							)
						})
						.map_err(|error| error.to_string())
				},
				move |connection| Message::LocalWorkspaceAttached {
					workspace: message_workspace,
					connection,
				},
			);
		}
		#[cfg(toron_local)]
		Message::LocalWorkspaceAttached {
			workspace,
			connection,
		} => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			match connection {
				Ok((connection, cancellation)) => {
					app_state.adapter_state.cancellation = cancellation;
					app_state.adapter_state.selection =
						crate::adapters::driver::AdapterSelection::Toron;
					app_state.adapter_state.configuration =
						crate::adapters::driver::AdapterConfiguration::Toron {
							endpoint: workspace.sql_endpoint.to_string(),
							// The local workspace is loopback: no TLS fields (ADR 0165 §4).
							server_name: String::new(),
							client_cert: String::new(),
							client_key: String::new(),
							ca_cert: String::new(),
						};
					app_state.adapter_state.name = "local workspace".to_owned();
					app_state.adapter_state.connection = Some(connection);
					app_state.adapter_state.stage = AdapterStage::Connected;
					app_state.adapter_state.local_workspace =
						Some(nilket_local::LocalWorkspace::Ready(workspace.clone()));
					app_state.status_error.clear();
					app_state.status_msg = "Joined Toron workspace.".to_owned();
				}
				Err(cause) => {
					let unhealthy = nilket_local::LocalWorkspace::Unhealthy {
						identity: workspace.identity,
						cause,
					};
					app_state.status_error = unhealthy.to_string();
					app_state.status_msg = "SQL scratch remains active.".into();
					app_state.adapter_state.connection = None;
					app_state.adapter_state.local_workspace = Some(unhealthy);
				}
			}
		}
		#[cfg(toron_local)]
		Message::StartLocalWorkspace => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			let Some(profile_root) = app_state.local_creation.accept() else {
				return Task::none();
			};
			app_state.status_error.clear();
			app_state.status_msg = format!(
				"Starting local workspace at {} under this Polariton session…",
				profile_root.display()
			);
			let owner = crate::local_workspace::LocalWorkspaceOwner::launch(profile_root.clone());
			let monitor = owner.monitor();
			app_state.local_workspace_owner = Some(owner);
			return Task::perform(
				crate::local_workspace::watch_until_ready(
					profile_root,
					monitor,
					crate::local_workspace::STARTUP_DEADLINE,
				),
				|outcome| match outcome {
					crate::local_workspace::StartupOutcome::Ready(ready) => {
						Message::LocalWorkspaceStartupSettled(Ok(ready))
					}
					crate::local_workspace::StartupOutcome::Failed { observed, cause } => {
						Message::LocalWorkspaceStartupSettled(Err((observed, cause)))
					}
				},
			);
		}
		#[cfg(toron_local)]
		Message::LocalWorkspaceStartupSettled(result) => {
			if app_state.closing_local_workspace {
				return Task::none();
			}
			match result {
				Ok(ready) => {
					return update(
						app_state,
						Message::LocalWorkspaceDiscovered(nilket_local::LocalWorkspace::Ready(
							ready,
						)),
					);
				}
				Err((observed, cause)) => {
					app_state.adapter_state.local_workspace = Some(observed);
					app_state.status_error = cause;
					app_state.status_msg =
					"Local workspace creation failed; the one-shot grant was spent. SQL scratch remains active."
						.into();
				}
			}
		}
		#[cfg(toron_local)]
		Message::LocalWorkspaceStopped(outcome) => {
			if !app_state.closing_local_workspace {
				return Task::none();
			}
			app_state.status_msg = format!("Local workspace stopped ({outcome}).");
			return window::latest().and_then(window::close);
		}
		Message::ScratchReady(scratch) => {
			if scratch.is_none() {
				app_state.status_error = "Scratch database failed to open.".to_string();
			}
			app_state.adapter_state.scratch = scratch;
		}
		Message::Run => {
			// In a notebook, Run always means "run the cell at the cursor",
			// even mid-selection — otherwise a selection would fall through to
			// dispatching the whole Markdown document as one SQL string.
			if let Some(cell) = app_state.code_editor.markdown_cell_at_cursor() {
				return run_markdown_cell(app_state, cell);
			}
			if app_state.code_editor.is_notebook() {
				app_state.status_msg = "Place the cursor in a SQL cell to run it.".into();
				return Task::none();
			}
			let Some(language_id) = app_state.code_editor.active_language_id() else {
				app_state.status_msg = "The focused pane has no executable language.".into();
				return Task::none();
			};
			return queue_plain_run(app_state, language_id, PendingRunKind::Buffer);
		}
		Message::RunSnippet => {
			if let Some(cell) = app_state.code_editor.selected_markdown_cell() {
				return run_markdown_cell(app_state, cell);
			}
			let Some(language_id) = app_state.code_editor.active_language_id() else {
				app_state.status_msg = "The focused pane has no executable language.".into();
				return Task::none();
			};
			return queue_plain_run(app_state, language_id, PendingRunKind::Snippet);
		}
		Message::RunResult {
			result: er,
			publish,
			generation,
		} => {
			// PRODUCT-0135 §4: a cancelled generation's late result changes
			// nothing — checked before status, grid, or registry effects. The
			// named claim concludes exactly as a failed run's does: unbound.
			if generation != app_state.current_sql_generation() {
				if let Some(claim) = publish.as_ref() {
					let _ = app_state.result_registry.conclude_unbound(claim);
				}
				return Task::none();
			}
			app_state.sql_abort = None;
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			if let Some(claim) = publish.as_ref() {
				match &er {
					ExecutionResult::Rows { artifact, .. } => {
						let _ = app_state
							.result_registry
							.publish(claim, std::sync::Arc::clone(artifact));
					}
					_ => {
						let _ = app_state.result_registry.conclude_unbound(claim);
					}
				}
			}
			match er {
				ExecutionResult::Affected(rows_affected) => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {rows_affected} row{} affected",
							if rows_affected == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::Batch { statements } => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {statements} statement{} completed",
							if statements == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::CommandCompleted(_) => {
					set_sql_status(app_state, "SQL · command completed");
				}
				ExecutionResult::Err(msg) => {
					set_sql_error(app_state, msg, None);
				}
				ExecutionResult::Rows {
					artifact,
					statements,
				} => {
					if publish.is_none() {
						app_state.code_editor.bind_result_panel_artifact(&artifact);
					}
					let (rows, cols) = artifact.shape();
					let df = match artifact.projection() {
						Ok(frame) => frame,
						Err(error) => {
							set_sql_error(app_state, error.to_string(), None);
							return Task::none();
						}
					};
					app_state.code_editor.begin_query();
					app_state
						.code_editor
						.set_query_result_from_artifact(df.as_ref().clone(), Arc::clone(&artifact));
					app_state.status_df_size = Some((rows, cols));
					set_sql_status(
						app_state,
						if statements > 1 {
							format!("SQL · {statements} statements completed")
						} else {
							"SQL · query completed".to_owned()
						},
					);
				}
				ExecutionResult::None => {
					set_sql_status(app_state, "SQL · no statements executed");
				}
			}
		}
		Message::RunRuneBufferResult {
			source_buffer,
			revision,
			result,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			match local_run_is_current(app_state, source_buffer, revision) {
				Some(false) => return Task::none(),
				// Settled: a local batch arrives with every kernel built.
				Some(true) => {
					app_state.local_rune_runs.remove(&source_buffer);
				}
				None => {}
			}
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				Ok(outcome) => {
					app_state.status_error.clear();
					app_state.code_editor.begin_query();
					app_state.code_editor.set_query_message(
						outcome
							.display
							.unwrap_or_else(|| "Rune finished.".to_owned()),
					);
					if let Some(panes) = outcome.panes {
						commit_local_panes(app_state, source_buffer, revision, panes);
						return app_state
							.code_editor
							.take_selection_tasks()
							.map(Message::CodeEditEvent);
					}
					if let Some(resolved) = outcome.plot {
						let (panel_id, display_label) = {
							let identity = LocalViewIdentity::Anonymous(0);
							let display_label = resolved.resolved_spec.composition.title();
							let presentation = ViewPresentation {
								content_kind: ViewContentKind::Plot,
								display_label: display_label.clone(),
								authored_title: crate::plot::spec::Presence::Inherit,
							};
							app_state.code_editor.prepare_rune_view_panels(
								source_buffer,
								&[(identity.clone(), presentation)],
								revision,
							);
							let panel_id = app_state
								.code_editor
								.rune_view_panel_id(source_buffer, &identity)
								.expect("plain Rune output has a prepared anonymous host panel");
							(panel_id, display_label)
						};
						app_state.status_msg =
							format!("Building plot from {}…", resolved.source_name);
						let build =
							build_resolved_plot_task(resolved, 1200, move |resolved, kernel| {
								Message::RuneBufferPlotReady {
									source_buffer,
									revision,
									panel_id,
									display_label,
									resolved,
									kernel,
								}
							});
						return Task::batch([
							app_state
								.code_editor
								.take_selection_tasks()
								.map(Message::CodeEditEvent),
							build,
						]);
					}
					app_state.code_editor.prepare_rune_view_panels(
						source_buffer,
						&[] as &[(LocalViewIdentity, ViewPresentation)],
						revision,
					);
					app_state.status_msg = "Rune finished.".to_owned();
					return app_state
						.code_editor
						.take_selection_tasks()
						.map(Message::CodeEditEvent);
				}
				Err(message) => {
					app_state
						.code_editor
						.mark_rune_view_producer_stale(source_buffer);
					set_rune_error(app_state, message);
				}
			}
		}
		#[cfg(feature = "toron")]
		Message::RunRunePresentationResult {
			source_buffer,
			revision,
			source,
			result,
		} => {
			let completion = app_state
				.rune_view_scheduler
				.complete_presentation(source_buffer, revision);
			if let Some(next) = completion.next {
				return start_rune_presentation_task(app_state, next);
			}
			if !completion.accepted
				|| app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision)
			{
				return Task::none();
			}
			let Some(result) = result else {
				panic!("an accepted presentation revision must have invoked the local VM");
			};
			match result {
				Ok(view) => {
					return run_rune_relation_view_at_revision(
						app_state,
						source_buffer,
						revision,
						view,
					);
				}
				Err(
					error @ (RuneEnvelopeError::Invalid(_)
					| RuneEnvelopeError::InvalidView { .. }
					| RuneEnvelopeError::DuplicateName { .. }),
				) => {
					app_state
						.code_editor
						.mark_rune_view_producer_stale(source_buffer);
					set_rune_error(app_state, error.to_string());
				}
				Err(RuneEnvelopeError::RecognizedNoOutput) => {
					app_state.code_editor.prepare_rune_view_panels(
						source_buffer,
						&[] as &[(LocalViewIdentity, ViewPresentation)],
						revision,
					);
					app_state.status_msg = "Rune view set is bound; no output root emitted.".into();
					app_state.status_error.clear();
					return app_state
						.code_editor
						.take_selection_tasks()
						.map(Message::CodeEditEvent);
				}
				Err(RuneEnvelopeError::NoTerminalView | RuneEnvelopeError::Parse(_)) => {
					return run_plain_rune_code(app_state, source_buffer, revision, source);
				}
			}
		}
		#[cfg(feature = "toron")]
		Message::RunRuneViewSetResult {
			source_buffer,
			revision,
			execution_index,
			origin,
			views,
			result,
		} => {
			let lineage = app_state
				.retained_rune_relations
				.get(&source_buffer)
				.and_then(|retained| retained.transformed.executions.get(execution_index))
				.and_then(|execution| execution.lineage.clone());
			if let Some(next) = app_state
				.rune_view_scheduler
				.complete(source_buffer, &origin)
			{
				return start_rune_relation_task(app_state, next);
			}
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			if let Ok(crate::gui::messages::RuneRelationCompletion::Views(outcome)) = &result
				&& let Some(retained) = app_state.retained_rune_relations.get_mut(&source_buffer)
				&& retained.revision == revision
				&& let Some(slot) = retained.artifacts.get_mut(execution_index)
			{
				app_state.code_editor.bind_rune_artifact(
					source_buffer,
					lineage.as_ref(),
					&outcome.result,
				);
				*slot = Some(Arc::clone(&outcome.result));
			}
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				// PRODUCT-0137 §7: routine status naming the resolved table,
				// confirming the certificate was recorded, and reporting rows
				// written. No panels, no grid — a materialization is a
				// complete success that presents nothing.
				Ok(crate::gui::messages::RuneRelationCompletion::Materialized(receipt)) => {
					app_state.status_error.clear();
					// The reported identity must round-trip into `ctx.table(...)`,
					// which takes a bare name — `TableRef::new` does not split a
					// qualified string. The namespace is therefore named beside
					// the table, never dotted into it.
					app_state.status_msg = format!(
						"Materialized table \"{}\" in namespace \"{}\" · {} row{} written · certificate recorded (boson v{})",
						receipt.table_name,
						receipt.namespace,
						receipt.rows_written,
						if receipt.rows_written == 1 { "" } else { "s" },
						receipt.certificate_format_version,
					);
				}
				Ok(crate::gui::messages::RuneRelationCompletion::Views(outcome)) => {
					use crate::gui::messages::RuneRelationSinkOutput;
					let sink_count = outcome.sinks.len();
					let mut errors = Vec::new();
					let mut plot_tasks = Vec::new();
					for mut sink in outcome.sinks {
						let source = match outcome.result.projection() {
							Ok(frame) => crate::result_artifact::ResultArtifact::from_polars(frame),
							Err(error) => {
								errors.push(error.to_string());
								continue;
							}
						};
						// PRODUCT-0154 §4: the sink's artifact is this relation's data under
						// its own identity, but its producer is a birth fact of the relation
						// and carries over; a rebuilt frame must not forget who produced it.
						if let Some(producer) = outcome.result.producer() {
							source.bind_producer(producer);
						}
						app_state.code_editor.bind_rune_artifact(
							source_buffer,
							lineage.as_ref(),
							&source,
						);
						sink.source = source;
						let panel_id = app_state
							.code_editor
							.rune_view_panel_id(source_buffer, &sink.identity)
							.expect("a dispatched view has a prepared host panel");
						let sink_facet = sink.facet.clone();
						let figure_grid = sink.figure_grid;
						match sink.output {
							Ok(RuneRelationSinkOutput::Table(presentation)) => {
								let frame = match sink.source.projection() {
									Ok(frame) => frame,
									Err(error) => {
										errors.push(error.to_string());
										continue;
									}
								};
								let rows = frame.height();
								let cols = frame.width();
								app_state
									.code_editor
									.set_rune_view_table_with_presentation_from_artifact(
										panel_id,
										frame.as_ref().clone(),
										presentation.skin,
										presentation.authored_show_column_types,
										Arc::clone(&sink.source),
									);
								app_state.status_df_size = Some((rows, cols));
							}
							Ok(RuneRelationSinkOutput::Plot(
								resolved,
								authored_settings,
								continuation,
							)) => {
								let identity = sink.identity.clone();
								let presentation = sink.presentation;
								if let Some(facet) = sink_facet {
									let span = sink.span;
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										Some(facet),
										figure_grid,
										None,
										move |resolved, result, recipe| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
												recipe: recipe.clone(),
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result: Err(error),
												recipe,
											},
										},
									));
								} else {
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										None,
										figure_grid,
										None,
										move |resolved, result, recipe| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
												recipe: recipe.clone(),
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span: sink.span,
												result: Err(error),
												recipe,
											},
										},
									));
								}
							}
							Ok(RuneRelationSinkOutput::StyledPlot(
								resolved,
								layer_settings,
								authored_settings,
								continuation,
							)) => {
								let identity = sink.identity.clone();
								let presentation = sink.presentation;
								let span = sink.span;
								if let Some(facet) = sink_facet {
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										Some(facet),
										figure_grid,
										Some(layer_settings),
										move |resolved, result, recipe| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
												recipe: recipe.clone(),
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result: Err(error),
												recipe,
											},
										},
									));
								} else {
									plot_tasks.push(build_facet_resolved_plot_task(
										resolved,
										Arc::clone(&sink.source),
										None,
										figure_grid,
										Some(layer_settings),
										move |resolved, result, recipe| match result {
											Ok(kernel) => Message::RuneRelationPlotReady {
												source_buffer,
												revision,
												panel_id,
												identity,
												presentation,
												authored_settings,
												continuation,
												resolved,
												kernel,
												recipe: recipe.clone(),
											},
											Err(error) => Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result: Err(error),
												recipe,
											},
										},
									));
								}
							}
							Ok(RuneRelationSinkOutput::LayeredPlot(
								specification,
								authored_settings,
							)) => {
								let presentation = sink.presentation;
								let span = sink.span;
								if let Some(facet) = sink_facet {
									plot_tasks.push(build_facet_layered_plot_task(
										specification,
										sink.source,
										Some(facet),
										figure_grid,
										move |result, recipe| {
											Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result,
												recipe,
											}
										},
									));
								} else {
									plot_tasks.push(build_facet_layered_plot_task(
										specification,
										sink.source,
										None,
										figure_grid,
										move |result, recipe| {
											Message::RuneRelationLayeredPlotReady {
												source_buffer,
												revision,
												panel_id,
												presentation,
												authored_settings,
												span,
												result,
												recipe,
											}
										},
									));
								}
							}
							Err(message) => {
								let message = format!(
									"{} at bytes {}..{}: {message}",
									sink.presentation.display_label, sink.span.start, sink.span.end
								);
								app_state.code_editor.fail_rune_view(panel_id);
								errors.push(message);
							}
						}
					}
					app_state.status_msg = format!(
						"Rune workspace rendered {sink_count} shared-result view{}.",
						if sink_count == 1 { "" } else { "s" }
					);
					if errors.is_empty() {
						app_state.status_error.clear();
					} else {
						set_rune_error(app_state, errors.join("\n"));
					}
					return Task::batch(plot_tasks);
				}
				Err(message) => {
					for identity in views {
						if let Some(panel_id) = app_state
							.code_editor
							.rune_view_panel_id(source_buffer, &identity)
						{
							app_state.code_editor.fail_rune_view(panel_id);
						}
					}
					set_rune_error(app_state, message);
				}
			}
		}
		Message::RuneBufferPlotReady {
			source_buffer,
			revision,
			panel_id,
			display_label,
			resolved,
			kernel,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			match local_run_is_current(app_state, source_buffer, revision) {
				Some(false) => return Task::none(),
				Some(true) => {
					app_state.local_rune_runs.remove(&source_buffer);
				}
				None => {}
			}
			let recipe = crate::plot::recipe::PlotRecipe::single(
				Arc::clone(&resolved.artifact),
				resolved.resolved_spec.clone(),
				1200,
				720,
			);
			app_state.code_editor.set_rune_view_plot_with_recipe(
				panel_id,
				display_label,
				kernel,
				PlotSettingsLayer::default(),
				recipe,
			);
			app_state.status_error.clear();
			app_state.status_msg = format!("Plotted {}.", resolved.source_name);
		}
		#[cfg(feature = "toron")]
		Message::RuneRelationPlotReady {
			source_buffer,
			revision,
			panel_id,
			identity,
			presentation,
			authored_settings,
			continuation,
			resolved,
			kernel,
			recipe,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			let source_attachment = app_state
				.rune_presentation_models
				.get(&(source_buffer, revision))
				.cloned()
				.zip(
					app_state
						.rune_document_generations
						.get(&(source_buffer, revision))
						.copied(),
				)
				.map(|(model, document_generation)| {
					let expression = app_state
						.rune_presentation_sources
						.get(&(source_buffer, revision))
						.and_then(|source| {
							let slot = model
								.view_slots
								.iter()
								.find(|slot| slot.owner == identity)?;
							source
								.get(
									slot.expression_span.start as usize
										..slot.expression_span.end as usize,
								)
								.map(str::to_owned)
						});
					crate::presentation_registry::PlotSourceAttachment {
						identity: identity.clone(),
						source: crate::presentation_registry::StampedPresentationSourceModel {
							buffer_id: source_buffer,
							document_generation,
							model,
						},
						expression,
					}
				});
			let plot_type = Some(resolved.resolved_spec.composition.primary_plot_type());
			app_state
				.code_editor
				.set_rune_view_plot_with_source_and_recipe(
					panel_id,
					presentation.display_label,
					kernel,
					authored_settings,
					source_attachment,
					plot_type,
					recipe,
				);
			app_state.rune_categorical_identities.insert(
				(source_buffer, identity, continuation.palette_len),
				continuation.identity,
			);
			app_state.status_msg = format!("Plotted {}.", resolved.source_name);
		}
		#[cfg(feature = "toron")]
		Message::RuneRelationLayeredPlotReady {
			source_buffer,
			revision,
			panel_id,
			presentation,
			authored_settings,
			span,
			result,
			recipe,
		} => {
			if app_state.rune_buffer_revisions.get(&source_buffer) != Some(&revision) {
				return Task::none();
			}
			match result {
				Ok(kernel) => {
					app_state.code_editor.set_rune_view_plot_with_recipe(
						panel_id,
						presentation.display_label.clone(),
						kernel,
						authored_settings,
						recipe,
					);
					app_state.status_msg = format!("Built {}.", presentation.display_label);
				}
				Err(error) => {
					app_state.code_editor.fail_rune_view(panel_id);
					set_rune_error(
						app_state,
						format!(
							"{} at bytes {}..{}: {error}",
							presentation.display_label, span.start, span.end
						),
					);
				}
			}
		}
		#[cfg(feature = "toron")]
		Message::SettingsCandidateCompiled {
			preview_generation,
			panel_id,
			commit,
			source_buffer,
			document_generation,
			relation_revision,
			identity,
			result,
		} => {
			if app_state.settings_preview_in_flight == Some(preview_generation) {
				app_state.settings_preview_in_flight = None;
			}
			let next = start_next_settings_preview_task(app_state);
			if app_state.settings_preview_latest.get(&panel_id) != Some(&preview_generation) {
				return next;
			}
			let candidate = match result {
				Ok(candidate) => candidate,
				Err(crate::settings_writeback::CandidatePreviewRefusal::RelationReuseRequired) => {
					refuse_settings_preview(
						app_state,
						panel_id,
						"this change requires running the relation again".to_owned(),
					);
					return next;
				}
				Err(crate::settings_writeback::CandidatePreviewRefusal::Compile(error)) => {
					refuse_settings_preview(
						app_state,
						panel_id,
						format!("preview refused · {error}"),
					);
					return next;
				}
			};
			let Some(retained) = app_state.retained_rune_relations.get(&source_buffer) else {
				return next;
			};
			let candidate_key = RelationPlanKey::from_view(&candidate.transformed);
			let reuse_is_current = app_state
				.rune_buffer_revisions
				.get(&source_buffer)
				.copied()
				.filter(|revision| *revision == relation_revision)
				.and_then(|revision| {
					retained
						.witness(revision)
						.map(|witness| (revision, witness))
				})
				.is_some_and(|(revision, witness)| witness.proves(&candidate_key, revision));
			if !reuse_is_current {
				refuse_settings_preview(
					app_state,
					panel_id,
					"this change requires running the relation again".to_owned(),
				);
				return next;
			}
			let Some((execution_index, execution)) = candidate
				.transformed
				.executions
				.iter()
				.enumerate()
				.find(|(_, execution)| {
					execution.views.iter().any(|view| view.identity == identity)
				})
			else {
				refuse_settings_preview(
					app_state,
					panel_id,
					"preview refused · producing view disappeared".to_owned(),
				);
				return next;
			};
			let Some(artifact) = retained
				.artifacts
				.get(execution_index)
				.and_then(Option::as_ref)
				.cloned()
			else {
				refuse_settings_preview(
					app_state,
					panel_id,
					"this change requires running the relation again".to_owned(),
				);
				return next;
			};
			let palettes = prepare_rune_categorical_palettes(app_state, source_buffer, execution);
			let outcome = match rune_view_outcome_with_palettes(
				artifact,
				execution.views.clone(),
				palettes,
			) {
				Ok(outcome) => outcome,
				Err(error) => {
					refuse_settings_preview(
						app_state,
						panel_id,
						format!("preview refused · {error}"),
					);
					return next;
				}
			};
			let Some(sink) = outcome
				.sinks
				.into_iter()
				.find(|sink| sink.identity == identity)
			else {
				refuse_settings_preview(
					app_state,
					panel_id,
					"preview refused · producing view was not rendered".to_owned(),
				);
				return next;
			};
			let presentation = sink.presentation;
			let facet = sink.facet;
			let figure_grid = sink.figure_grid;
			let source = sink.source;
			let build = match sink.output {
				Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
					resolved,
					authored_settings,
					continuation,
				)) => {
					let candidate = candidate.clone();
					let identity = identity.clone();
					build_facet_resolved_plot_task(
						resolved,
						source,
						facet,
						figure_grid,
						None,
						move |resolved, kernel, _recipe| Message::SettingsPreviewPlotReady {
							preview_generation,
							panel_id,
							commit,
							source_buffer,
							document_generation,
							relation_revision,
							identity,
							candidate,
							presentation,
							authored_settings,
							continuation,
							resolved,
							kernel,
						},
					)
				}
				Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
					resolved,
					layer_settings,
					authored_settings,
					continuation,
				)) => {
					let candidate = candidate.clone();
					let identity = identity.clone();
					build_facet_resolved_plot_task(
						resolved,
						source,
						facet,
						figure_grid,
						Some(layer_settings),
						move |resolved, kernel, _recipe| Message::SettingsPreviewPlotReady {
							preview_generation,
							panel_id,
							commit,
							source_buffer,
							document_generation,
							relation_revision,
							identity,
							candidate,
							presentation,
							authored_settings,
							continuation,
							resolved,
							kernel,
						},
					)
				}
				Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(..)) => {
					refuse_settings_preview(
						app_state,
						panel_id,
						"per-layer settings editing is not supported".to_owned(),
					);
					Task::none()
				}
				Ok(crate::gui::messages::RuneRelationSinkOutput::Table(_)) => {
					refuse_settings_preview(
						app_state,
						panel_id,
						"preview refused · producing view is not a plot".to_owned(),
					);
					Task::none()
				}
				Err(error) => {
					refuse_settings_preview(
						app_state,
						panel_id,
						format!("preview refused · {error}"),
					);
					Task::none()
				}
			};
			return Task::batch([next, build]);
		}
		#[cfg(feature = "toron")]
		Message::SettingsPreviewPlotReady {
			preview_generation,
			panel_id,
			commit,
			source_buffer,
			document_generation,
			relation_revision,
			identity,
			candidate,
			presentation,
			authored_settings,
			continuation,
			resolved,
			kernel,
		} => {
			if app_state.settings_preview_latest.get(&panel_id) != Some(&preview_generation) {
				return Task::none();
			}
			let still_attached = app_state
				.code_editor
				.plot_writeback_context(panel_id)
				.is_some_and(|context| {
					context.attachment.source.buffer_id == source_buffer
						&& context.attachment.source.document_generation == document_generation
						&& context.attachment.identity == identity
				}) && app_state.rune_buffer_revisions.get(&source_buffer)
				== Some(&relation_revision);
			if !still_attached {
				app_state.code_editor.discard_plot_source_preview(panel_id);
				app_state.code_editor.set_plot_writeback_status(
					panel_id,
					Some("preview refused · source attachment changed".to_owned()),
				);
				return Task::none();
			}
			let kernel = match kernel {
				Ok(kernel) => kernel,
				Err(error) => {
					refuse_settings_preview(
						app_state,
						panel_id,
						format!("preview refused · {error}"),
					);
					return Task::none();
				}
			};
			let attachment = crate::presentation_registry::PlotSourceAttachment {
				identity: identity.clone(),
				source: crate::presentation_registry::StampedPresentationSourceModel {
					buffer_id: source_buffer,
					document_generation,
					model: Arc::new(candidate.transformed.presentation_source_model.clone()),
				},
				expression: app_state
					.code_editor
					.plot_writeback_context(panel_id)
					.and_then(|context| context.attachment.expression.clone()),
			};
			let plot_type = Some(resolved.resolved_spec.composition.primary_plot_type());
			app_state.code_editor.show_plot_source_preview(
				panel_id,
				presentation.display_label,
				kernel,
				authored_settings,
				attachment,
				plot_type,
			);
			if commit {
				app_state.document_request_sequence =
					app_state.document_request_sequence.saturating_add(1);
				let request_id =
					libaryon::core::RequestId::new(app_state.document_request_sequence);
				let revision = app_state.rune_buffer_revisions[&source_buffer];
				if app_state.code_editor.apply_document_edits(
					request_id,
					source_buffer,
					document_generation,
					candidate.plan.edits.clone(),
				) {
					app_state.pending_settings_commits.insert(
						request_id,
						PendingSettingsCommit {
							panel_id,
							source_buffer,
							revision,
							candidate,
							identity,
							continuation,
						},
					);
					app_state
						.code_editor
						.set_plot_writeback_status(panel_id, Some("committing…".to_owned()));
				} else {
					app_state.code_editor.discard_plot_source_preview(panel_id);
					app_state.code_editor.set_plot_writeback_status(
						panel_id,
						Some("commit refused · editor unavailable".to_owned()),
					);
				}
			}
		}
		Message::RunMarkdownCellResult {
			identity,
			presentation,
			source_start_line,
			result,
			generation,
		} => {
			// PRODUCT-0135 §4: stale-generation discard before any effect —
			// the cell's prior artifact was already retired at dispatch, and a
			// cancelled run leaves it retired, per the registry's own rule.
			if generation != app_state.current_sql_generation() {
				return Task::none();
			}
			app_state.sql_abort = None;
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				ExecutionResult::Rows {
					artifact,
					statements,
				} => {
					app_state
						.code_editor
						.bind_notebook_artifact(&identity, &artifact);
					let (rows, cols) = artifact.shape();
					let df = match artifact.projection() {
						Ok(frame) => frame,
						Err(error) => {
							set_sql_error(app_state, error.to_string(), Some(source_start_line));
							return Task::none();
						}
					};
					// Presentation never decides data availability: a named SQL cell
					// remains a Rune input even when it renders in a pane or nowhere.
					app_state
						.code_editor
						.retain_markdown_cell_artifact(identity.clone(), Arc::clone(&artifact));
					match presentation.output {
						MarkdownCellOutputTarget::Inline => app_state
							.code_editor
							.set_markdown_cell_table(identity, df.as_ref().clone()),
						MarkdownCellOutputTarget::Pane => {
							app_state.code_editor.begin_query();
							app_state.code_editor.set_query_result_from_artifact(
								df.as_ref().clone(),
								Arc::clone(&artifact),
							)
						}
						MarkdownCellOutputTarget::None => {}
					}
					app_state.status_df_size = Some((rows, cols));
					set_sql_status(
						app_state,
						if statements > 1 {
							format!("SQL · {statements} statements completed")
						} else {
							"SQL · cell query completed".to_owned()
						},
					);
				}
				ExecutionResult::Err(message) => {
					set_sql_error(app_state, message, Some(source_start_line));
				}
				ExecutionResult::Affected(rows) => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {rows} row{} affected",
							if rows == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::CommandCompleted(_) => {
					set_sql_status(app_state, "SQL · command completed");
				}
				ExecutionResult::Batch { statements } => {
					set_sql_status(
						app_state,
						format!(
							"SQL · {statements} statement{} completed",
							if statements == 1 { "" } else { "s" }
						),
					);
				}
				ExecutionResult::None => {
					set_sql_status(app_state, "SQL · no statements executed");
				}
			}
		}
		Message::RunRuneCellResult {
			identity,
			presentation,
			result,
		} => {
			app_state.status_time_elapsed = Some(app_state.code_started.elapsed());
			match result {
				Ok(NotebookRuneOutcome {
					plot: Some(resolved),
					receipts,
				}) => {
					app_state
						.code_editor
						.bind_notebook_artifact(&identity, &resolved.artifact);
					app_state.status_error.clear();
					app_state.status_msg = format!("Building plot from {}…", resolved.source_name);
					return build_resolved_plot_task(resolved, 720, move |resolved, kernel| {
						Message::NotebookPlotReady {
							identity,
							presentation,
							resolved,
							kernel,
							receipts,
						}
					});
				}
				Ok(NotebookRuneOutcome {
					plot: None,
					receipts,
				}) => {
					app_state.status_error.clear();
					app_state.status_msg = export_receipt_status(&receipts);
				}
				Err(NotebookRuneFailure { message, receipts }) => {
					set_rune_error(app_state, message.clone());
					if !receipts.is_empty() {
						app_state.status_msg = export_receipt_status(&receipts);
					}
					if presentation.output == MarkdownCellOutputTarget::Inline {
						app_state.code_editor.set_markdown_cell_error(
							identity,
							"failed · see status bar".to_owned(),
						);
					}
				}
			}
		}
		Message::NotebookPlotReady {
			identity,
			presentation,
			resolved,
			kernel,
			receipts,
		} => {
			let composition = resolved.resolved_spec.composition.clone();
			let recipe = crate::plot::recipe::PlotRecipe::single(
				Arc::clone(&resolved.artifact),
				resolved.resolved_spec.clone(),
				1200,
				720,
			);
			match presentation.output {
				MarkdownCellOutputTarget::Inline => app_state
					.code_editor
					.set_markdown_cell_plot_with_recipe(identity, composition, kernel, recipe),
				MarkdownCellOutputTarget::Pane => {
					app_state.code_editor.begin_query();
					app_state.code_editor.set_query_plot_with_recipe(
						composition.title(),
						kernel,
						recipe,
					);
				}
				MarkdownCellOutputTarget::None => {}
			}
			app_state.status_error.clear();
			app_state.status_msg = if receipts.is_empty() {
				"Plot cell finished.".to_owned()
			} else {
				format!("Plot cell finished. {}", export_receipt_status(&receipts))
			};
		}
		Message::Export(format) => {
			let snapshots = app_state.code_editor.host_plot_exports();
			if snapshots.is_empty() {
				app_state.status_msg = "No live plots to export.".to_owned();
				return Task::none();
			}
			let count = snapshots.len();
			app_state.status_msg = format!("Exporting {count} plots as {format}...");
			return Task::perform(
				async move {
					tokio::task::spawn_blocking(move || -> std::result::Result<(), String> {
						for (index, snapshot) in snapshots.into_iter().enumerate() {
							let job = render_plot_export(snapshot, format, index + 1)?;
							job.write_to(std::path::Path::new("."));
						}
						Ok(())
					})
					.await
					.map_err(|error| format!("Export · task failed: {error}"))?
				},
				move |result| match result {
					Ok(()) => Message::ExportDone(count, format),
					Err(error) => Message::ExportFailed(error),
				},
			);
		}
		Message::ExportDone(count, format) => {
			app_state.status_msg = format!("Exported {count} plots as {format}.");
		}
		Message::ExportFailed(error) => {
			app_state.status_msg = "Export refused.".to_owned();
			app_state.status_error = error;
		}
		Message::PublishRequested => {
			app_state.publish_prompt = Some(crate::publication::PublishPrompt::default());
		}
		Message::PublishPathChanged(path) => {
			if let Some(prompt) = &mut app_state.publish_prompt {
				prompt.path = path;
				prompt.error = None;
			}
		}
		Message::PublishCancelled => {
			app_state.publish_prompt = None;
		}
		Message::PublishSubmitted => {
			let Some(prompt) = app_state.publish_prompt.clone() else {
				return Task::none();
			};
			let path = prompt.path.trim().to_owned();
			if path.is_empty() {
				if let Some(prompt) = &mut app_state.publish_prompt {
					prompt.error = Some("A directory path is required.".to_owned());
				}
				return Task::none();
			}
			let job = match app_state.prepare_publish(std::path::Path::new(&path)) {
				Ok(job) => job,
				Err(error) => {
					if let Some(prompt) = &mut app_state.publish_prompt {
						prompt.error = Some(error.to_string());
					}
					return Task::none();
				}
			};
			app_state.status_msg = format!("Publishing to {path}...");
			return Task::perform(
				async move {
					tokio::task::spawn_blocking(move || -> std::result::Result<String, String> {
						let package = job.run().map_err(|error| error.to_string())?;
						Ok(format!("{path} · {}", package.digest()))
					})
					.await
					.map_err(|error| format!("Publish · task failed: {error}"))?
				},
				|result| match result {
					Ok(receipt) => Message::PublishDone(receipt),
					Err(error) => Message::PublishFailed(error),
				},
			);
		}
		Message::PublishDone(receipt) => {
			app_state.publish_prompt = None;
			app_state.status_msg = format!("Published {receipt}");
		}
		Message::PublishFailed(error) => {
			if let Some(prompt) = &mut app_state.publish_prompt {
				prompt.error = Some(error.clone());
			}
			app_state.status_msg = "Publish refused.".to_owned();
			app_state.status_error = error;
		}
		Message::ConnectionNameChanged(name) => {
			app_state.adapter_state.name = name;
		}
		Message::OpenSettings => {
			app_state.showing_settings = true;
		}
		Message::CloseSettings => {
			app_state.showing_settings = false;
		}
		Message::ToggleShowColumnTypes(val) => {
			app_state.show_column_types = val;
			app_state.code_editor.set_show_column_types(val);
			app_state.status_msg = format!(
				"Settings · column types {} for this session",
				if val { "shown" } else { "hidden" },
			);
		}
	}
	Task::none()
}

/// PRODUCT-0134 §8: run a store operation now when the store is unlocked,
/// otherwise hold it pending and open the passphrase prompt. This is the only
/// way a prompt opens, so a session that never touches the store is never
/// prompted.
fn request_profile_access(
	app_state: &mut AppState,
	operation: crate::profile_store::ProfileOperation,
) -> Task<Message> {
	if app_state.profile_store.is_unlocked() {
		return apply_profile_operation(app_state, operation);
	}
	let Some(path) = app_state.profile_store_path.clone() else {
		app_state.status_error =
			"No configuration directory is available for connection profiles.".into();
		return Task::none();
	};
	app_state.pending_profile_operation = Some(operation);
	app_state.profile_unlock = Some(crate::profile_store::UnlockPrompt::for_store(path.exists()));
	Task::none()
}

fn apply_profile_operation(
	app_state: &mut AppState,
	operation: crate::profile_store::ProfileOperation,
) -> Task<Message> {
	use crate::profile_store::{ProfileOperation, SavedProfile};
	let Some(path) = app_state.profile_store_path.clone() else {
		return Task::none();
	};
	match operation {
		ProfileOperation::Browse => {
			// Unlocking was the operation: the Connect menu now lists names.
		}
		ProfileOperation::SaveCurrent => {
			// PRODUCT-0134 §1: a profile is named fields, never a connection
			// string. Saving while the ad hoc kind is active would have to
			// parse one into the other, which no path may do.
			if !app_state
				.adapter_state
				.ad_hoc_connection_string
				.trim()
				.is_empty()
			{
				app_state.status_error =
					"Profiles use named fields; clear the ad hoc connection string first.".into();
				return Task::none();
			}
			let adapter_type = app_state.adapter_state.selection.adapter_type_str();
			if adapter_type == "None" {
				app_state.status_error = "Select and configure an adapter first.".into();
				return Task::none();
			}
			let name = app_state.adapter_state.name.trim().to_owned();
			let profile = SavedProfile {
				adapter_type: adapter_type.to_owned(),
				fields: app_state.adapter_state.fields.clone(),
			};
			let outcome = match app_state.editing_profile.clone() {
				None => app_state.profile_store.save_new(&path, &name, profile),
				Some(original) if original == name => {
					app_state.profile_store.replace(&path, &name, profile)
				}
				Some(original) => app_state
					.profile_store
					.rename(&path, &original, &name)
					.and_then(|()| app_state.profile_store.replace(&path, &name, profile)),
			};
			match outcome {
				Ok(()) => {
					app_state.editing_profile = Some(name.clone());
					app_state.status_msg = format!("Profile saved: {name}");
					app_state.status_error.clear();
				}
				Err(error) => app_state.status_error = error,
			}
		}
		ProfileOperation::Connect(name) => match load_profile(app_state, &name) {
			Ok(configuration) => {
				let task = app_state.begin_connect_with(configuration);
				app_state.status_msg = format!("Connecting profile: {name}");
				return task;
			}
			Err(error) => app_state.status_error = error,
		},
		ProfileOperation::Edit(name) => match load_profile(app_state, &name) {
			Ok(_configuration) => {
				// The dialog edits fields; the configuration is rebuilt on
				// submit. Opening it is the whole operation.
				app_state.adapter_state.stage = AdapterStage::Unconfigured;
				app_state.status_msg = format!("Editing profile: {name}");
				app_state.status_error.clear();
			}
			Err(error) => app_state.status_error = error,
		},
		ProfileOperation::Delete(name) => match app_state.profile_store.delete(&path, &name) {
			Ok(()) => {
				if app_state.editing_profile.as_deref() == Some(name.as_str()) {
					app_state.editing_profile = None;
				}
				app_state.status_msg = format!("Profile deleted: {name}");
				app_state.status_error.clear();
			}
			Err(error) => app_state.status_error = error,
		},
	}
	Task::none()
}

/// Load a stored profile into the adapter state — selection, dialog Name
/// (which is the profile identity, PRODUCT-0134 §4), and the field map — and
/// rebuild its configuration through the one saved path, `from_saved`.
fn load_profile(
	app_state: &mut AppState,
	name: &str,
) -> std::result::Result<crate::adapters::driver::AdapterConfiguration, String> {
	let profile = app_state.profile_store.get(name)?.clone();
	let Some(selection) =
		crate::adapters::driver::AdapterSelection::from_type_str(&profile.adapter_type)
	else {
		return Err(format!(
			"Profile {name} names an adapter this build does not include: {}",
			profile.adapter_type
		));
	};
	let configuration = crate::adapters::driver::AdapterConfiguration::from_saved(
		&profile.adapter_type,
		&profile.fields,
	);
	app_state.adapter_state.selection = selection;
	app_state.adapter_state.name = name.to_owned();
	app_state.adapter_state.fields = profile.fields;
	app_state.adapter_state.ad_hoc_connection_string.clear();
	app_state.editing_profile = Some(name.to_owned());
	Ok(configuration)
}

fn menu_action_to_message(action: &str) -> Option<Message> {
	match action {
		"connect:new" => Some(Message::Connect),
		"connect:profiles" => Some(Message::ProfileStoreAccessRequested),
		"connect:save-profile" => Some(Message::ProfileSaveRequested),
		"connect:disconnect" => Some(Message::AdapterDisconnectRequested),
		"code:run" => Some(Message::Run),
		"code:run-snippet" => Some(Message::RunSnippet),
		"code:cancel" => Some(Message::CancelRun),
		"settings:preferences" => Some(Message::OpenSettings),
		"session:checkpoint" => Some(Message::CheckpointSession),
		"session:copy-id" => Some(Message::CopySessionId),
		"session:close-without-checkpoint" => Some(Message::CloseWithoutCheckpoint),
		"plot:publish" => Some(Message::PublishRequested),
		"plot:agents" => Some(Message::ToggleAgents),
		_ => {
			// Profile names ride after a fixed prefix and are taken verbatim,
			// so any name the store accepts is addressable from the menu.
			if let Some(name) = action.strip_prefix("connect:profile-connect:") {
				return Some(Message::ProfileConnectRequested(name.to_owned()));
			}
			if let Some(name) = action.strip_prefix("connect:profile-edit:") {
				return Some(Message::ProfileEditRequested(name.to_owned()));
			}
			if let Some(name) = action.strip_prefix("connect:profile-delete:") {
				return Some(Message::ProfileDeleteRequested(name.to_owned()));
			}
			if let Some(format) = action.strip_prefix("plot:export:") {
				return parse_export_format(format).map(Message::Export);
			}
			None
		}
	}
}

/// PRODUCT-0154 §4 and §8: what the package may carry — the producer kind
/// and a non-secret label — and, separately, everything the guard must
/// refuse: every secure adapter field value entered or unlocked in this
/// session, the ad hoc connection string, and this machine's home and
/// working directories.
fn publish_request(app_state: &AppState) -> crate::publication::PublishRequest {
	let selection = &app_state.adapter_state.selection;
	let mut secrets = Vec::new();
	for descriptor in crate::adapters::driver::fields_for(selection) {
		if descriptor.is_secure
			&& let Some(value) = app_state.adapter_state.fields.get(descriptor.key)
		{
			secrets.push(value.clone());
		}
	}
	secrets.push(app_state.adapter_state.ad_hoc_connection_string.clone());
	if let Ok(names) = app_state.profile_store.names() {
		let names: Vec<String> = names.into_iter().map(str::to_owned).collect();
		for name in names {
			let Ok(profile) = app_state.profile_store.get(&name) else {
				continue;
			};
			let Some(kind) =
				crate::adapters::driver::AdapterSelection::from_type_str(&profile.adapter_type)
			else {
				continue;
			};
			for descriptor in crate::adapters::driver::fields_for(&kind) {
				if descriptor.is_secure
					&& let Some(value) = profile.fields.get(descriptor.key)
				{
					secrets.push(value.clone());
				}
			}
		}
	}
	let mut forbidden_paths = Vec::new();
	if let Ok(home) = std::env::var("HOME") {
		forbidden_paths.push(home);
	}
	if let Ok(cwd) = std::env::current_dir() {
		forbidden_paths.push(cwd.display().to_string());
	}
	crate::publication::PublishRequest {
		published_at: chrono::Utc::now().to_rfc3339(),
		secrets,
		forbidden_paths,
	}
}

fn parse_export_format(format: &str) -> Option<ExportFormat> {
	match format {
		"SVG" => Some(ExportFormat::SVG),
		"PNG" => Some(ExportFormat::PNG),
		"AVIF" => Some(ExportFormat::AVIF),
		_ => None,
	}
}

/// The window rectangle occupied by Baryon's workspace: the window minus the
/// application chrome and status bar.
fn workspace_bounds(size: Size) -> iced::Rectangle {
	iced::Rectangle {
		x: 10.0,
		y: 71.0,
		width: (size.width - 20.0).max(1.0),
		height: (size.height - 113.0).max(1.0),
	}
}

/// Keep libaryon's terminal-cell viewport aligned with its direct workspace
/// surface. There is no Polariton-owned outer split layout to reconcile.
fn sync_workspace_viewport(app_state: &mut AppState) {
	let bounds = workspace_bounds(app_state.window_size);
	app_state.code_editor.resize(bounds.width, bounds.height);
}

#[cfg(test)]
mod tests {
	#[cfg(any(not(feature = "toron"), toron_local))]
	use super::resolve_startup;
	#[cfg(toron_local)]
	use super::{AdapterState, view};
	#[cfg(toron_local)]
	use super::{
		SavedCloseState, StartupAuthority, dispatch_rune_relation, new_with_authority,
		resolve_rune_table_skin_with, rune_view_outcome,
	};
	use super::{
		clear_transient_execution_status_on_editor_activity, compact_rune_diagnostic,
		compact_sql_diagnostic, new, resolve_notebook_rune_execution, session_identity_text,
		set_rune_error, set_sql_error, update,
	};
	use crate::adapters::common::ExecutionResult;
	#[cfg(toron_local)]
	use crate::adapters::common::{
		AdapterStage, DatabaseAdapter, RuneDispatchFailure, RuneDispatchFailureKind,
	};
	use crate::configuration::StartupData;
	use crate::gui::messages::{Message, NotebookRuneFailure, NotebookRuneOutcome};
	#[cfg(feature = "toron")]
	use crate::gui::table_skin::{TableSkin, TableSkinProjection, TableSkinSpec};
	#[cfg(toron_local)]
	use crate::plot::common::PlotLayout;
	#[cfg(toron_local)]
	use crate::plot::core::PlotType;
	#[cfg(toron_local)]
	use crate::plot::factory::{create_layered_plot, create_styled_plot};
	#[cfg(feature = "toron")]
	use crate::plot::factory::{
		create_layered_plot_from_artifact, create_plot, create_plot_from_artifact,
		create_styled_plot_from_artifact,
	};
	#[cfg(toron_local)]
	use crate::plot::grid::PlotGrid;
	#[cfg(toron_local)]
	use crate::plot::{
		common::{CoordinateTransformer, PlotKernel, ResolvedPlotSettings},
		export::SvgBackend,
	};
	use crate::rune_host::ExportReceipt;
	#[cfg(feature = "toron")]
	use crate::rune_relation_envelope::{
		LocalViewIdentity, PendingRuneView, RuneViewForm, TransformedRuneRelation,
		transform as transform_rune_view,
	};
	#[cfg(toron_local)]
	use crate::rune_relation_envelope::{
		RuneTableSkinIntent, RuneTableSkinProjection, TransformedRuneView,
	};
	#[cfg(feature = "toron")]
	use libaryon::core::BufferId;
	#[cfg(toron_local)]
	use nilket_local::{
		Artifact, Evidence, LocalWorkspace, ObservationContext, ReadyWorkspace, StaleCause,
		VersionedArtifact, WorkspaceIdentity,
	};
	#[cfg(feature = "toron")]
	use std::collections::HashMap;
	#[cfg(feature = "toron")]
	use std::sync::Arc;
	#[cfg(toron_local)]
	use std::sync::Mutex;
	#[cfg(toron_local)]
	use tokio::sync::RwLock;

	#[cfg(feature = "toron")]
	fn complete_rune_presentation(
		state: &mut super::AppState,
		source_buffer: BufferId,
		source: &str,
	) -> iced::Task<Message> {
		let revision = *state
			.rune_buffer_revisions
			.get(&source_buffer)
			.expect("run assigned a revision");
		update(
			state,
			Message::RunRunePresentationResult {
				source_buffer,
				revision,
				source: source.to_owned(),
				result: Some(crate::rune_view_compiler::compile(source)),
			},
		)
	}

	#[cfg(toron_local)]
	fn render_marks(kernel: &dyn PlotKernel) -> String {
		render_marks_with_settings(kernel, ResolvedPlotSettings::default())
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0150_commit_revalidates_the_current_pane_provenance() {
		use crate::plot::spec::Override;
		use crate::plot::theme::{PlotDomainOverrides, PlotSettings};
		use crate::presentation_registry::SettingKey;

		let mut settings = PlotSettings::default();
		settings.pane_override.domains = Override::Set(PlotDomainOverrides {
			x_min: Some(1.0),
			x_max: Some(9.0),
			y_min: Some(2.0),
			y_max: Some(8.0),
		});
		assert_eq!(
			super::validated_displayed_pane_value(&settings, SettingKey::XAxisBounds, "1, 9"),
			Some("1, 9".to_owned())
		);
		assert_eq!(
			super::validated_displayed_pane_value(&settings, SettingKey::XAxisBounds, "0, 10"),
			None,
			"a stale displayed value cannot be promoted"
		);
		settings.pane_override.domains = Override::Inherit;
		assert_eq!(
			super::validated_displayed_pane_value(&settings, SettingKey::XAxisBounds, "1, 9"),
			None,
			"a delayed message cannot promote after pane provenance disappears"
		);
	}

	#[test]
	fn product_0071_copied_identity_remains_the_exact_cli_authority() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let id = "1234567890abcdef1234567890abcdef".parse().unwrap();
		state.session_id = Some(id);
		let copied = session_identity_text(id);
		assert_eq!(copied.len(), 32);
		assert_eq!(copied, "1234567890abcdef1234567890abcdef");
		assert_eq!(
			copied.parse(),
			Ok(id),
			"copied identity selects the same session"
		);
		state.code_editor.shutdown();
	}

	#[cfg(not(feature = "toron"))]
	#[test]
	fn product_0063_no_toron_build_refuses_saved_authority_explicitly() {
		let Err(error) = resolve_startup(super::StartupTarget::CreateSaved) else {
			panic!("a no-toron build must refuse saved authority")
		};
		assert_eq!(error, "saved sessions require Polariton's `toron` feature");
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0063_saved_startup_checkpoint_close_and_reopen_use_production_authority() {
		const CHILD: &str = "POLARITON_PRODUCT_0063_SURFACE_CHILD";
		if std::env::var_os(CHILD).is_none() {
			let state_home = std::env::temp_dir().join(format!(
				"polariton-product-0063-{}-{}",
				std::process::id(),
				std::time::SystemTime::now()
					.duration_since(std::time::UNIX_EPOCH)
					.unwrap()
					.as_nanos()
			));
			let status = std::process::Command::new(std::env::current_exe().unwrap())
				.args([
					"--exact",
					"gui::state::tests::product_0063_saved_startup_checkpoint_close_and_reopen_use_production_authority",
					"--nocapture",
				])
				.env(CHILD, "1")
				.env("XDG_STATE_HOME", &state_home)
				.env("HOME", &state_home)
				.status()
				.expect("spawn isolated session-surface fixture");
			let _ = std::fs::remove_dir_all(&state_home);
			assert!(status.success(), "isolated session-surface fixture failed");
			return;
		}

		let authority = resolve_startup(super::StartupTarget::CreateSaved).unwrap();
		let session_id = match &authority {
			StartupAuthority::Saved { session_id, .. } => *session_id,
			StartupAuthority::Ephemeral { .. } => panic!("create must resolve saved authority"),
		};
		let (mut state, startup) = new_with_authority(StartupData::default(), authority).unwrap();
		drop(startup);
		assert_eq!(state.session_id, Some(session_id));

		let contention = resolve_startup(super::StartupTarget::OpenSaved(session_id));
		let Err(contention) = contention else {
			panic!("the production resolver must not switch authority on contention")
		};
		assert!(contention.contains(&session_id.to_string()));
		assert!(contention.contains("already open incompatibly"));
		let ordinary = resolve_startup(super::StartupTarget::Ephemeral { initial_file: None })
			.expect("saved sessions in the store never change default authority");
		assert!(matches!(
			ordinary,
			StartupAuthority::Ephemeral { initial_file: None }
		));

		drop(update(&mut state, Message::CheckpointSession));
		assert_eq!(state.saved_close_state, SavedCloseState::Open);
		assert!(state.status_error.contains("viewport is not ready"));
		assert_eq!(
			update(&mut state, Message::CloseWithoutCheckpoint).units(),
			0,
			"a failed manual checkpoint never arms destructive close"
		);

		let first_close = update(&mut state, Message::CloseWindow);
		assert_eq!(
			first_close.units(),
			0,
			"unready checkpoint keeps the window open"
		);
		assert_eq!(state.saved_close_state, SavedCloseState::Failed);
		assert!(state.status_error.contains("viewport is not ready"));

		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state.status_msg != "Session checkpointed" && std::time::Instant::now() < deadline {
			state.code_editor.poll();
			drop(update(&mut state, Message::CheckpointSession));
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(state.status_msg, "Session checkpointed");
		assert_eq!(state.saved_close_state, SavedCloseState::Open);
		drop(update(
			&mut state,
			Message::CodeEditEvent(crate::editor::EditorMsg::EngineReady),
		));
		assert!(state.rune_categorical_identities.is_empty());
		let producer = state.code_editor.active_buffer_id().unwrap();
		let stale_view = LocalViewIdentity::Named("stale-witness".to_owned());
		let companion_view = LocalViewIdentity::Named("companion-table".to_owned());
		state.code_editor.prepare_rune_view_panels(
			producer,
			&[
				(stale_view.clone(), RuneViewForm::Bar),
				(companion_view.clone(), RuneViewForm::Table),
			],
			1,
		);
		let durable_panel = state
			.code_editor
			.rune_view_panel_id(producer, &stale_view)
			.unwrap();
		let companion_panel = state
			.code_editor
			.rune_view_panel_id(producer, &companion_view)
			.unwrap();
		use crate::plot::spec::{
			AllocationSpaceSnapshot, ColorAssignmentSnapshot, ColorIdentity, ColorIdentitySnapshot,
			ColorLayerIdentity, ColorSpaceSnapshot, LayerIdentity,
		};
		let authored_colour = ColorIdentity::from_persistence_snapshot(ColorIdentitySnapshot {
			assignments: vec![ColorAssignmentSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::HorizontalBar,
				}),
				category: "Atlas".to_owned(),
				position: 2,
			}],
			spaces: vec![ColorSpaceSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::HorizontalBar,
				}),
				space: AllocationSpaceSnapshot::CategoricalPalette { len: 4 },
			}],
		})
		.unwrap();
		state
			.rune_categorical_identities
			.insert((producer, stale_view.clone(), 4), authored_colour);
		let stale_close = update(&mut state, Message::CloseWindow);
		assert_eq!(stale_close.units(), 0);
		assert_eq!(state.saved_close_state, SavedCloseState::Failed);
		assert!(state.status_error.contains("close again to retry"));
		assert!(state.status_error.contains("stale application snapshot"));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while (state
			.code_editor
			.rune_view_panel_layout_signature(producer, &stale_view)
			.is_none()
			|| state
				.code_editor
				.rune_view_panel_layout_signature(producer, &companion_view)
				.is_none())
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert!(
			state
				.code_editor
				.rune_view_panel_layout_signature(producer, &stale_view)
				.is_some(),
			"next projected census contains the new panel before deliberate retry"
		);
		let durable_layout = state
			.code_editor
			.rune_view_panel_persistence_signature(producer, &stale_view)
			.unwrap();
		let companion_layout = state
			.code_editor
			.rune_view_panel_persistence_signature(producer, &companion_view)
			.unwrap();
		assert_eq!(
			durable_layout.0, companion_layout.0,
			"one Rune producer owns one durable panel group"
		);

		use libaryon::engine::{InputEvent, KeyCode, Modifiers};
		for key in ":pv rune".chars() {
			drop(update(
				&mut state,
				Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
					code: KeyCode::Char(key),
					modifiers: Modifiers::NONE,
				})),
			));
		}
		drop(update(
			&mut state,
			Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Enter,
				modifiers: Modifiers::NONE,
			})),
		));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.active_language_id()
			.is_none_or(|language| language.as_str() != "rune")
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state.code_editor.active_language_id().map(|id| id.as_str()),
			Some("rune")
		);
		const DIRTY_CLOSE_BYTES: &str = "// dirty close checkpoint";
		for key in std::iter::once('i').chain(DIRTY_CLOSE_BYTES.chars()) {
			drop(update(
				&mut state,
				Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
					code: KeyCode::Char(key),
					modifiers: Modifiers::NONE,
				})),
			));
		}
		drop(update(
			&mut state,
			Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Esc,
				modifiers: Modifiers::NONE,
			})),
		));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while !state
			.code_editor
			.cached_content()
			.contains(DIRTY_CLOSE_BYTES)
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert!(
			state
				.code_editor
				.cached_content()
				.contains(DIRTY_CLOSE_BYTES)
		);
		drop(update(&mut state, Message::Run));
		assert_eq!(state.pending_runs.len(), 1, "run waits on its snapshot");

		let owner = crate::local_workspace::LocalWorkspaceOwner::launch_command(
			std::process::Command::new("/usr/bin/true"),
			nix::sys::signal::Signal::SIGINT,
		);
		state.local_workspace_owner = Some(owner);
		let close = update(&mut state, Message::CloseWindow);
		assert_eq!(close.units(), 1, "saved close emits one bounded drain task");
		assert_eq!(state.saved_close_state, SavedCloseState::Frozen);
		assert!(state.closing_local_workspace);
		assert!(
			state.pending_runs.is_empty(),
			"undispatched run is abandoned"
		);
		let status = state.status_msg.clone();
		assert_eq!(update(&mut state, Message::Run).units(), 0);
		assert_eq!(
			update(
				&mut state,
				Message::RunResult {
					result: ExecutionResult::Err("late completion".to_owned()),
					publish: None,
					generation: 0,
				},
			)
			.units(),
			0
		);
		assert_eq!(update(&mut state, Message::ScratchReady(None)).units(), 0);
		assert_eq!(
			state.status_msg, status,
			"run initiation and asynchronous completions are discarded"
		);
		assert!(state.status_error.is_empty());
		let finish = update(
			&mut state,
			Message::LocalWorkspaceStopped("Stopped".to_owned()),
		);
		assert_eq!(
			finish.units(),
			1,
			"drain completion alone closes the window"
		);
		assert!(state.status_msg.contains("Local workspace stopped"));
		drop(finish);
		drop(close);
		state.code_editor.shutdown();
		drop(state);

		let reopened = resolve_startup(super::StartupTarget::OpenSaved(session_id)).unwrap();
		let (mut state, startup) = new_with_authority(StartupData::default(), reopened).unwrap();
		drop(startup);
		assert_eq!(state.session_id, Some(session_id));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while (state
			.code_editor
			.rune_view_producer_for_panel(durable_panel, &stale_view)
			.is_none()
			|| state
				.code_editor
				.rune_view_producer_for_panel(companion_panel, &companion_view)
				.is_none()
			|| state.rune_categorical_identities.is_empty())
			&& std::time::Instant::now() < deadline
		{
			drop(update(
				&mut state,
				Message::CodeEditEvent(crate::editor::EditorMsg::EngineReady),
			));
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		let reopened_producer = state
			.code_editor
			.rune_view_producer_for_panel(durable_panel, &stale_view)
			.expect("restored panel meaning identifies its producer");
		assert!(
			state
				.code_editor
				.cached_content()
				.contains(DIRTY_CLOSE_BYTES),
			"successful close persists the dirty focused scratch before teardown"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_producer_for_panel(companion_panel, &companion_view),
			Some(reopened_producer),
			"both restored named panels join to the same durable producer"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &stale_view),
			Some(durable_panel),
			"production reopen installs the exact durable named-panel meaning"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(reopened_producer, &stale_view),
			Some("fresh"),
			"startup restores placement and meaning without executing Rune"
		);
		assert!(
			!state
				.code_editor
				.rune_view_panel_is_plot(reopened_producer, &stale_view)
				&& !state
					.code_editor
					.rune_view_panel_is_table(reopened_producer, &stale_view),
			"fresh is the registry spelling for an unresolved restored placeholder"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &companion_view),
			Some(companion_panel)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(reopened_producer, &companion_view),
			Some("fresh")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_persistence_signature(reopened_producer, &stale_view),
			Some(durable_layout),
			"the named plot returns to its exact group and logical placement"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_persistence_signature(reopened_producer, &companion_view),
			Some(companion_layout),
			"the named table returns to its exact group and logical placement"
		);
		let canonical_colour = ColorIdentity::from_persistence_snapshot(ColorIdentitySnapshot {
			assignments: vec![ColorAssignmentSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::Bar,
				}),
				category: "Atlas".to_owned(),
				position: 2,
			}],
			spaces: vec![ColorSpaceSnapshot {
				field: "product".to_owned(),
				layer: ColorLayerIdentity::from_authored_layer(&LayerIdentity {
					ordinal: 0,
					plot_type: PlotType::Bar,
				}),
				space: AllocationSpaceSnapshot::CategoricalPalette { len: 4 },
			}],
		})
		.unwrap();
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(reopened_producer, stale_view.clone(), 4)),
			Some(&canonical_colour),
			"orientation changes geometry, never durable colour identity; restored keys: {:?}",
			state.rune_categorical_identities.keys().collect::<Vec<_>>()
		);
		state.code_editor.prepare_rune_view_panels(
			reopened_producer,
			&[
				(stale_view.clone(), RuneViewForm::Bar),
				(companion_view.clone(), RuneViewForm::Table),
			],
			2,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &stale_view),
			Some(durable_panel),
			"explicit rerun resolves in the restored panel"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(reopened_producer, &companion_view),
			Some(companion_panel),
			"both explicit rerun outputs resolve in place"
		);
		let discard_only_view = LocalViewIdentity::Named("discard-only".to_owned());
		state.code_editor.prepare_rune_view_panels(
			reopened_producer,
			&[(discard_only_view, RuneViewForm::Table)],
			3,
		);
		assert_eq!(update(&mut state, Message::CloseWindow).units(), 0);
		assert_eq!(state.saved_close_state, SavedCloseState::Failed);
		let discard = update(&mut state, Message::CloseWithoutCheckpoint);
		assert_eq!(discard.units(), 1, "explicit discard emits the close task");
		assert_eq!(state.saved_close_state, SavedCloseState::Frozen);
		drop(discard);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0063_sigkill_is_not_a_close_checkpoint() {
		const MODE: &str = "POLARITON_PRODUCT_0063_CRASH_MODE";
		const SESSION_FILE: &str = "POLARITON_PRODUCT_0063_SESSION_FILE";
		const SESSION_ID: &str = "POLARITON_PRODUCT_0063_SESSION_ID";
		const UNCOMMITTED: &str = "not checkpointed before sigkill";

		match std::env::var(MODE).as_deref() {
			Ok("holder") => {
				let authority = resolve_startup(super::StartupTarget::CreateSaved).unwrap();
				let session_id = match &authority {
					StartupAuthority::Saved { session_id, .. } => *session_id,
					StartupAuthority::Ephemeral { .. } => panic!("holder must be saved"),
				};
				let (mut state, startup) =
					new_with_authority(StartupData::default(), authority).unwrap();
				drop(startup);
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while state.status_msg != "Session checkpointed"
					&& std::time::Instant::now() < deadline
				{
					state.code_editor.poll();
					drop(update(&mut state, Message::CheckpointSession));
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				assert_eq!(state.status_msg, "Session checkpointed");

				use libaryon::engine::{InputEvent, KeyCode, Modifiers};
				for key in std::iter::once('i').chain(UNCOMMITTED.chars()) {
					drop(update(
						&mut state,
						Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
							code: KeyCode::Char(key),
							modifiers: Modifiers::NONE,
						})),
					));
				}
				drop(update(
					&mut state,
					Message::CodeEditEvent(crate::editor::EditorMsg::Input(InputEvent::Key {
						code: KeyCode::Esc,
						modifiers: Modifiers::NONE,
					})),
				));
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while !state.code_editor.cached_content().contains(UNCOMMITTED)
					&& std::time::Instant::now() < deadline
				{
					state.code_editor.poll();
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				assert!(state.code_editor.cached_content().contains(UNCOMMITTED));
				std::fs::write(
					std::env::var_os(SESSION_FILE).unwrap(),
					session_id.to_string(),
				)
				.unwrap();
				loop {
					std::thread::park();
				}
			}
			Ok("verify") => {
				let session_id = std::env::var(SESSION_ID).unwrap().parse().unwrap();
				let authority =
					resolve_startup(super::StartupTarget::OpenSaved(session_id)).unwrap();
				let (mut state, startup) =
					new_with_authority(StartupData::default(), authority).unwrap();
				drop(startup);
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while state.code_editor.cached_content().contains(UNCOMMITTED)
					|| state.code_editor.active_buffer_id().is_none()
				{
					assert!(
						std::time::Instant::now() < deadline,
						"reopen must project the predecessor"
					);
					state.code_editor.poll();
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				assert!(
					!state.code_editor.cached_content().contains(UNCOMMITTED),
					"SIGKILL and Drop publish no edits"
				);
				state.code_editor.shutdown();
			}
			Ok(other) => panic!("unknown crash fixture mode {other}"),
			Err(_) => {
				let root = std::env::temp_dir().join(format!(
					"polariton-product-0063-crash-{}-{}",
					std::process::id(),
					std::time::SystemTime::now()
						.duration_since(std::time::UNIX_EPOCH)
						.unwrap()
						.as_nanos()
				));
				std::fs::create_dir_all(&root).unwrap();
				let session_file = root.join("session-id");
				let exact = "gui::state::tests::product_0063_sigkill_is_not_a_close_checkpoint";
				let mut holder = std::process::Command::new(std::env::current_exe().unwrap())
					.args(["--exact", exact, "--nocapture"])
					.env(MODE, "holder")
					.env(SESSION_FILE, &session_file)
					.env("XDG_STATE_HOME", &root)
					.env("HOME", &root)
					.spawn()
					.unwrap();
				let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
				while !session_file.exists() && std::time::Instant::now() < deadline {
					assert!(
						holder.try_wait().unwrap().is_none(),
						"holder exited before signal"
					);
					std::thread::sleep(std::time::Duration::from_millis(5));
				}
				let session_id = std::fs::read_to_string(&session_file).unwrap();
				nix::sys::signal::kill(
					nix::unistd::Pid::from_raw(holder.id() as i32),
					nix::sys::signal::Signal::SIGKILL,
				)
				.unwrap();
				let status = holder.wait().unwrap();
				assert!(!status.success(), "holder must die by SIGKILL");

				let verifier = std::process::Command::new(std::env::current_exe().unwrap())
					.args(["--exact", exact, "--nocapture"])
					.env(MODE, "verify")
					.env(SESSION_ID, session_id.trim())
					.env("XDG_STATE_HOME", &root)
					.env("HOME", &root)
					.status()
					.unwrap();
				let _ = std::fs::remove_dir_all(&root);
				assert!(
					verifier.success(),
					"the lock releases and the predecessor reopens"
				);
			}
		}
	}

	#[test]
	fn product_0136_notebook_receipts_survive_without_a_plot_and_beside_plot_errors() {
		use libaryon::uast::projection::{
			MarkdownCellCodeVisibility, MarkdownCellIdentity, MarkdownCellOutputTarget,
			MarkdownCellPresentation,
		};

		let identity = MarkdownCellIdentity::Explicit {
			id: "export".to_owned(),
		};
		let presentation = MarkdownCellPresentation {
			output: MarkdownCellOutputTarget::None,
			code: MarkdownCellCodeVisibility::Show,
		};
		let receipt = ExportReceipt {
			path: "exports/sales.csv".to_owned(),
			rows: 3,
		};
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		drop(update(
			&mut state,
			Message::RunRuneCellResult {
				identity: identity.clone(),
				presentation,
				result: Ok(NotebookRuneOutcome {
					plot: None,
					receipts: vec![receipt.clone()],
				}),
			},
		));
		assert_eq!(state.status_error, "");
		assert_eq!(state.status_msg, receipt.status_message());

		drop(update(
			&mut state,
			Message::RunRuneCellResult {
				identity,
				presentation,
				result: Err(NotebookRuneFailure {
					message: "plot columns are incompatible".to_owned(),
					receipts: vec![receipt.clone()],
				}),
			},
		));
		assert!(state.status_error.contains("plot columns are incompatible"));
		assert_eq!(state.status_msg, receipt.status_message());
		state.code_editor.shutdown();
	}

	#[test]
	fn product_0136_notebook_refuses_only_when_it_produces_nothing() {
		let empty = resolve_notebook_rune_execution(crate::rune_host::RuneExecution {
			request_order: Vec::new(),
			#[cfg(feature = "toron")]
			views: Vec::new(),
			display: None,
			requests: Vec::new(),
			receipts: Vec::new(),
		})
		.expect_err("an empty cell keeps the plot-era refusal");
		assert_eq!(
			empty.message,
			"Rune cell did not publish a plot; call `.show()?`."
		);
		assert!(empty.receipts.is_empty());

		let exported = resolve_notebook_rune_execution(crate::rune_host::RuneExecution {
			request_order: Vec::new(),
			#[cfg(feature = "toron")]
			views: Vec::new(),
			display: None,
			requests: Vec::new(),
			receipts: vec![ExportReceipt {
				path: "export.csv".to_owned(),
				rows: 1,
			}],
		})
		.expect("an export-only cell is a product");
		assert!(exported.plot.is_none());
		assert_eq!(exported.receipts.len(), 1);
	}

	#[cfg(toron_local)]
	fn render_marks_with_settings(
		kernel: &dyn PlotKernel,
		settings: ResolvedPlotSettings,
	) -> String {
		let bounds = iced::Rectangle::new(iced::Point::ORIGIN, iced::Size::new(100.0, 100.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let mut backend = SvgBackend::new(100.0, 100.0);
		kernel.plot(
			&mut backend,
			bounds,
			&transform,
			iced::advanced::mouse::Cursor::Unavailable,
			settings,
		);
		backend.finish()
	}
	#[cfg(toron_local)]
	fn local_identity() -> WorkspaceIdentity {
		WorkspaceIdentity {
			profile_root: "/tmp/polariton-product-0005-fixture".into(),
			profile_uuid: "00000000-0000-0000-0000-000000000001".parse().unwrap(),
			session_nonce: "00000000-0000-0000-0000-000000000002".parse().unwrap(),
			owner_pid: 42,
			started_unix_ms: 7,
		}
	}

	#[cfg(toron_local)]
	struct RecordingRuneAdapter {
		calls: Arc<Mutex<Vec<String>>>,
		fail_table: Option<&'static str>,
	}

	#[cfg(toron_local)]
	#[async_trait::async_trait]
	impl DatabaseAdapter for RecordingRuneAdapter {
		async fn dispatch(&mut self, _code: &str) -> ExecutionResult {
			ExecutionResult::Err("SQL dispatch is unused".to_owned())
		}

		async fn dispatch_rune(
			&mut self,
			source: &str,
		) -> crate::adapters::common::RuneDispatchResult {
			self.calls
				.lock()
				.expect("recording lock")
				.push(source.to_owned());
			if self
				.fail_table
				.is_some_and(|table| source.contains(&format!("table(\"{table}\")")))
			{
				return Err(RuneDispatchFailure {
					kind: RuneDispatchFailureKind::Execution,
					message: "fixture origin failed".to_owned(),
					span: None,
				});
			}
			Ok(crate::adapters::common::RuneDispatchSuccess::Relation(
				crate::result_artifact::ResultArtifact::test_frame(
					polars::df!("x" => [1_i64], "y" => [2_i64]).expect("fixture frame"),
				),
			))
		}
	}

	/// PRODUCT-0137 §7: a materialization completes as a receipt — routine
	/// status naming the resolved table, rows written, and the certificate's
	/// recorded format version. No grid, no panels, no empty relation, and
	/// `status_error` stays clear.
	#[cfg(feature = "toron")]
	#[test]
	fn product_0137_materialization_renders_a_routine_receipt() {
		use crate::adapters::common::MaterializationReceipt;
		use crate::gui::messages::RuneRelationCompletion;
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.rune_buffer_revisions.insert(BufferId::new(7), 1);
		// Prime the scheduler exactly as production does: a completion is
		// only ever delivered for a program the scheduler started.
		let view = crate::rune_relation_envelope::transform(r#"ket.table("fixture")"#)
			.expect("terminal ket relation");
		let origin = view
			.executions
			.first()
			.expect("one execution")
			.origin
			.clone();
		state
			.rune_view_scheduler
			.submit(crate::rune_relation_envelope::PendingRuneView {
				source_buffer: BufferId::new(7),
				revision: 1,
				view,
			});
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer: BufferId::new(7),
				revision: 1,
				execution_index: 0,
				origin,
				views: Vec::new(),
				result: Ok(RuneRelationCompletion::Materialized(
					MaterializationReceipt {
						namespace: "public".to_owned(),
						table_name: "fixture".to_owned(),
						certificate_format_version: 1,
						rows_written: 1_000_000,
					},
				)),
			},
		));
		assert!(state.status_error.is_empty(), "a receipt is routine status");
		// The identity must be separated, not dotted: a reader pastes the
		// table name into `ctx.table(...)`, which does not split a qualified
		// string, so a dotted render would name something that cannot
		// round-trip.
		assert!(
			state
				.status_msg
				.contains(r#"table "fixture" in namespace "public""#),
			"{}",
			state.status_msg
		);
		assert!(
			!state.status_msg.contains("public.fixture"),
			"the namespace must never be dotted into the table name: {}",
			state.status_msg
		);
		assert!(state.status_msg.contains("1000000"), "{}", state.status_msg);
		assert!(
			state.status_msg.contains("certificate recorded"),
			"{}",
			state.status_msg
		);
		assert!(state.status_msg.contains("v1"), "{}", state.status_msg);
		assert!(
			state.status_df_size.is_none(),
			"a materialization never populates the grid"
		);
		state.code_editor.shutdown();
	}

	/// PRODUCT-0137: a terminal relation that returned rows asked for
	/// nothing. It refuses with the terminal-view guidance rather than
	/// dropping the rows silently.
	#[cfg(toron_local)]
	#[test]
	fn product_0137_terminal_relation_with_rows_refuses_rather_than_dropping() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let connection: Arc<RwLock<dyn DatabaseAdapter>> =
			Arc::new(RwLock::new(RecordingRuneAdapter {
				calls: Arc::new(Mutex::new(Vec::new())),
				fail_table: None,
			}));
		let relation = crate::rune_relation_envelope::transform(r#"ket.table("fixture")"#)
			.expect("terminal ket relation")
			.into_relations()
			.next()
			.expect("one relation");
		assert!(relation.views.is_empty());
		// `RuneRelationCompletion` carries no `Debug`, so unwrap the error
		// explicitly rather than through `expect_err`.
		let error = match runtime.block_on(dispatch_rune_relation(connection, relation)) {
			Err(error) => error,
			Ok(_) => panic!("rows with no view must refuse"),
		};
		assert!(error.contains("no view"), "{error}");
		assert!(error.contains("materialize"), "{error}");
	}
	/// PRODUCT-0137: tests that expect a view set say so; production
	/// consumers match both completion variants explicitly.
	#[cfg(toron_local)]
	fn expect_views(
		completion: crate::gui::messages::RuneRelationCompletion,
	) -> crate::gui::messages::RuneRelationViewSetOutcome {
		match completion {
			crate::gui::messages::RuneRelationCompletion::Views(outcome) => outcome,
			crate::gui::messages::RuneRelationCompletion::Materialized(receipt) => {
				panic!(
					"expected views, got a materialization of {}",
					receipt.table_name
				)
			}
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0084_rune_transport_preserves_one_artifact_allocation() {
		let relation =
			transform_rune_view("workspace.table(\"t\").views([table(), scatter(\"x\", \"y\")])")
				.expect("Rune relation fixture")
				.into_relations()
				.next()
				.expect("one relation");
		let artifact = crate::result_artifact::ResultArtifact::test_frame(
			polars::df!("x" => [1_i64], "y" => [2_i64]).expect("fixture frame"),
		);
		let transport: crate::adapters::common::RuneDispatchResult = Ok(
			crate::adapters::common::RuneDispatchSuccess::Relation(Arc::clone(&artifact)),
		);
		let Ok(transported) = transport else {
			panic!("Rune transport accepts the artifact");
		};
		let transported = transported.expect_relation();
		assert!(Arc::ptr_eq(&artifact, &transported));
		let palettes = super::default_rune_categorical_palettes(&relation.views);
		let outcome = super::rune_view_outcome_with_palettes(transported, relation.views, palettes)
			.expect("views resolve");
		assert!(Arc::ptr_eq(&artifact, &outcome.result));
		assert!(
			outcome
				.sinks
				.iter()
				.all(|sink| Arc::ptr_eq(&artifact, &sink.source))
		);
	}

	#[cfg(feature = "toron")]
	pub(super) fn complete_relation_in_head(
		state: &mut super::AppState,
		source_buffer: libaryon::core::BufferId,
		revision: u64,
		relation: TransformedRuneRelation,
		frame: polars::frame::DataFrame,
	) -> Vec<(LocalViewIdentity, crate::plot::request::ResolvedPlot)> {
		enum ReadyPlot {
			Single(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::request::ResolvedPlot,
				crate::plot::theme::PlotSettingsLayer,
				crate::gui::messages::RuneCategoricalContinuation,
			),
			Styled(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::request::ResolvedPlot,
				crate::plot::spec::LayerSettings,
				crate::plot::theme::PlotSettingsLayer,
				crate::gui::messages::RuneCategoricalContinuation,
			),
			Layered(
				crate::rune_relation_envelope::ViewPresentation,
				crate::plot::spec::LayeredPlotSpec,
				crate::plot::theme::PlotSettingsLayer,
				Arc<crate::result_artifact::ResultArtifact>,
				std::ops::Range<u32>,
			),
		}
		let origin = relation.origin.clone();
		let views = relation
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		let palettes = super::prepare_rune_categorical_palettes(state, source_buffer, &relation);
		let outcome = super::rune_view_outcome_with_palettes(
			crate::result_artifact::ResultArtifact::test_frame(frame),
			relation.views,
			palettes,
		)
		.expect("fixture projects");
		let plots = outcome
			.sinks
			.iter()
			.filter_map(|sink| match &sink.output {
				Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
					resolved,
					authored_settings,
					continuation,
				)) => Some((
					sink.identity.clone(),
					ReadyPlot::Single(
						sink.presentation.clone(),
						resolved.clone(),
						authored_settings.clone(),
						continuation.clone(),
					),
				)),
				Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
					resolved,
					layer_settings,
					authored_settings,
					continuation,
				)) => Some((
					sink.identity.clone(),
					ReadyPlot::Styled(
						sink.presentation.clone(),
						resolved.clone(),
						layer_settings.clone(),
						authored_settings.clone(),
						continuation.clone(),
					),
				)),
				Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(
					specification,
					authored_settings,
				)) => Some((
					sink.identity.clone(),
					ReadyPlot::Layered(
						sink.presentation.clone(),
						specification.clone(),
						authored_settings.clone(),
						Arc::clone(&sink.source),
						sink.span.clone(),
					),
				)),
				_ => None,
			})
			.collect::<Vec<_>>();
		let resolved_plots = plots
			.iter()
			.filter_map(|(identity, plot)| match plot {
				ReadyPlot::Single(_, resolved, _, _) | ReadyPlot::Styled(_, resolved, _, _, _) => {
					Some((identity.clone(), resolved.clone()))
				}
				ReadyPlot::Layered(..) => None,
			})
			.collect::<Vec<_>>();

		drop(update(
			state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision,
				execution_index: 0,
				origin,
				views,
				result: Ok(crate::gui::messages::RuneRelationCompletion::Views(outcome)),
			},
		));
		for (identity, plot) in plots {
			let panel_id = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("prepared plot panel");
			match plot {
				ReadyPlot::Single(presentation, resolved, authored_settings, continuation) => {
					let recipe = crate::plot::recipe::PlotRecipe::single(
						Arc::clone(&resolved.artifact),
						resolved.resolved_spec.clone(),
						1200,
						720,
					);
					let kernel = create_plot_from_artifact(
						&resolved.resolved_spec,
						&resolved.data,
						&resolved.artifact,
						1200,
						720,
					);
					drop(update(
						state,
						Message::RuneRelationPlotReady {
							source_buffer,
							revision,
							panel_id,
							identity,
							presentation,
							authored_settings,
							continuation,
							resolved,
							kernel,
							recipe,
						},
					));
				}
				ReadyPlot::Styled(
					presentation,
					resolved,
					layer_settings,
					authored_settings,
					continuation,
				) => {
					let recipe = crate::plot::recipe::PlotRecipe::facet(
						Arc::clone(&resolved.artifact),
						Arc::new(resolved.resolved_spec.clone()),
						None,
						crate::plot::grid::FigureGridLayoutSettings::default(),
						Some(layer_settings.clone()),
						1200,
						720,
					);
					let kernel = create_styled_plot_from_artifact(
						&resolved.resolved_spec,
						&resolved.data,
						&resolved.artifact,
						layer_settings,
					)
					.expect("styled plot kernel");
					drop(update(
						state,
						Message::RuneRelationPlotReady {
							source_buffer,
							revision,
							panel_id,
							identity,
							presentation,
							authored_settings,
							continuation,
							resolved,
							kernel,
							recipe,
						},
					));
				}
				ReadyPlot::Layered(
					presentation,
					specification,
					authored_settings,
					source,
					span,
				) => {
					let recipe = crate::plot::recipe::PlotRecipe::layered(
						Arc::clone(&source),
						specification.clone(),
						None,
						crate::plot::grid::FigureGridLayoutSettings::default(),
						1200,
						720,
					);
					let result = source
						.projection()
						.map_err(|error| error.to_string())
						.and_then(|frame| {
							create_layered_plot_from_artifact(
								&specification,
								frame.as_ref(),
								source.as_ref(),
								1200,
								720,
							)
						});
					drop(update(
						state,
						Message::RuneRelationLayeredPlotReady {
							source_buffer,
							revision,
							panel_id,
							presentation,
							authored_settings,
							span,
							result,
							recipe,
						},
					));
				}
			}
		}
		resolved_plots
	}

	#[cfg(feature = "toron")]
	fn run_one_relation_program(
		state: &mut super::AppState,
		source_buffer: BufferId,
		revision: u64,
		source: &str,
		frame: polars::frame::DataFrame,
	) -> Vec<(LocalViewIdentity, crate::plot::request::ResolvedPlot)> {
		let program = transform_rune_view(source).expect("Rune relation program");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, revision);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program.clone(),
				})
				.is_some(),
			"revision must be schedulable"
		);
		complete_relation_in_head(
			state,
			source_buffer,
			revision,
			program.into_relations().next().expect("one relation"),
			frame,
		)
	}

	#[cfg(feature = "toron")]
	fn band_mapping(
		resolved: &crate::plot::request::ResolvedPlot,
	) -> Vec<(String, usize, (u8, u8, u8))> {
		resolved
			.resolved_spec
			.band_fills
			.iter()
			.map(|assignment| {
				(
					assignment.category.clone(),
					assignment.position.index(),
					(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
					),
				)
			})
			.collect()
	}

	#[cfg(feature = "toron")]
	fn categorical_bar_frame(categories: &[&str]) -> polars::frame::DataFrame {
		polars::frame::DataFrame::new(
			categories.len(),
			vec![
				polars::prelude::Column::new("cat".into(), categories.to_vec()),
				polars::prelude::Column::new(
					"val".into(),
					(1..=categories.len() as i64).collect::<Vec<_>>(),
				),
			],
		)
		.expect("categorical bar frame")
	}

	#[cfg(toron_local)]
	fn non_ready_local_workspaces() -> Vec<LocalWorkspace> {
		let identity = local_identity();
		let root = identity.profile_root.clone();
		let runtime_context = ObservationContext::Runtime(identity.clone());
		vec![
			LocalWorkspace::Absent {
				profile_root: root.clone(),
			},
			LocalWorkspace::Starting {
				profile_root: root,
				owner_pid: identity.owner_pid,
			},
			LocalWorkspace::Unhealthy {
				identity: identity.clone(),
				cause: "fixture health failure".to_owned(),
			},
			LocalWorkspace::Stale {
				identity: identity.clone(),
				cause: StaleCause::PidRecycled,
			},
			LocalWorkspace::Corrupt {
				context: runtime_context.clone(),
				artifact: Artifact::RuntimeManifest,
				cause: "fixture corruption".to_owned(),
			},
			LocalWorkspace::Incompatible {
				context: runtime_context.clone(),
				artifact: VersionedArtifact::RuntimeManifest,
				found: 2,
				supported: 1,
			},
			LocalWorkspace::Unverifiable {
				context: runtime_context,
				evidence: Evidence::StableIdentity,
				cause: "fixture churn".to_owned(),
			},
		]
	}

	#[cfg(toron_local)]
	#[test]
	fn local_discovery_attaches_only_ready_workspaces() {
		for workspace in non_ready_local_workspaces() {
			let is_absent = matches!(workspace, LocalWorkspace::Absent { .. });
			let expected_status = workspace.to_string();
			let (mut state, startup) = new(StartupData::default(), None);
			drop(startup);
			let attach = update(
				&mut state,
				Message::LocalWorkspaceDiscovered(workspace.clone()),
			);
			assert_eq!(
				attach.units(),
				0,
				"non-Ready discovery must construct no connection client: {workspace}"
			);
			assert!(matches!(state.adapter_state.stage, AdapterStage::None));
			assert!(state.adapter_state.connection.is_none());
			assert_eq!(state.adapter_state.local_workspace, Some(workspace));
			assert_eq!(state.status_error, expected_status);
			assert_eq!(state.local_creation.offer().is_some(), is_absent);
			if is_absent {
				assert!(state.status_msg.contains("Start a session-bound workspace"));
				let offer_frame = view(&state);
				drop(offer_frame);
			} else {
				assert_eq!(state.status_msg, "SQL scratch remains active.");
			}
			state.code_editor.shutdown();
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let ready = ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity: local_identity(),
		};
		let attach = update(
			&mut state,
			Message::LocalWorkspaceDiscovered(LocalWorkspace::Ready(ready.clone())),
		);
		assert_eq!(
			attach.units(),
			1,
			"Ready is the positive control for connection construction"
		);
		assert_eq!(state.status_msg, "Attaching Toron workspace…");
		assert!(
			!state
				.status_msg
				.contains(&ready.identity.session_nonce.to_string()),
			"ordinary chrome must not expose the Toron workspace nonce"
		);
		assert!(state.local_creation.offer().is_none());
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn first_frame_does_not_wait_for_local_discovery() {
		let (mut state, startup) = new(StartupData::default(), None);
		assert_eq!(
			startup.units(),
			2,
			"scratch open and local discovery must remain independent async work"
		);
		assert!(state.adapter_state.local_workspace.is_none());
		let first_frame = view(&state);
		drop(first_frame);
		assert!(
			state.adapter_state.local_workspace.is_none(),
			"rendering must not synchronously await discovery"
		);
		drop(startup);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0071_joined_toron_workspace_chrome_omits_the_nonce() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let workspace = ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity: local_identity(),
		};
		let nonce = workspace.identity.session_nonce.to_string();
		let connection: Arc<RwLock<dyn DatabaseAdapter>> =
			Arc::new(RwLock::new(RecordingRuneAdapter {
				calls: Arc::new(Mutex::new(Vec::new())),
				fail_table: None,
			}));

		assert_eq!(
			update(
				&mut state,
				Message::LocalWorkspaceAttached {
					workspace,
					connection: Ok((
						connection,
						crate::adapters::common::CancelCapability::StopWaiting
					)),
				},
			)
			.units(),
			0
		);
		assert_eq!(state.status_msg, "Joined Toron workspace.");
		assert!(
			!state.status_msg.contains(&nonce),
			"ordinary chrome must not expose the Toron workspace nonce"
		);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn unavailable_workspace_keeps_scratch_explicitly_attributed() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.adapter_state.scratch = runtime.block_on(AdapterState::open_scratch());
		let workspace = LocalWorkspace::Absent {
			profile_root: "/tmp/polariton-product-0005-absent".into(),
		};
		let expected_status = workspace.to_string();
		let attempt = update(&mut state, Message::LocalWorkspaceDiscovered(workspace));
		assert_eq!(attempt.units(), 0);
		assert!(state.adapter_state.sql_connection().is_some());
		assert!(state.adapter_state.scratch_is_active());
		assert_eq!(state.adapter_state.connection_identity(), "scratch");
		assert_eq!(state.status_error, expected_status);
		assert!(state.status_msg.contains("Start a session-bound workspace"));
		assert!(state.local_creation.offer().is_some());
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn spent_creation_failure_does_not_reoffer_or_retry() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let root = std::path::PathBuf::from("/tmp/polariton-product-0006-spent");
		let absent = LocalWorkspace::Absent {
			profile_root: root.clone(),
		};
		drop(update(
			&mut state,
			Message::LocalWorkspaceDiscovered(absent.clone()),
		));
		assert_eq!(state.local_creation.accept(), Some(root));
		let settled = update(
			&mut state,
			Message::LocalWorkspaceStartupSettled(Err((
				absent,
				"controlled startup failure".to_owned(),
			))),
		);
		assert_eq!(settled.units(), 0, "failure must arm no retry or timer");
		assert!(state.local_creation.offer().is_none());
		assert_eq!(state.status_error, "controlled startup failure");
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn matching_ready_observation_without_a_handle_confers_no_stop_authority() {
		let root = std::env::temp_dir().join(format!(
			"polariton-product-0006-attached-{}",
			std::process::id()
		));
		std::fs::create_dir_all(&root).unwrap();
		let signal = root.join("signal");
		let mut child = std::process::Command::new("/bin/sh")
			.arg("-c")
			.arg("trap 'printf signaled > \"$1\"; exit 0' INT; while :; do sleep 0.02; done")
			.arg("attached-fixture")
			.arg(&signal)
			.spawn()
			.unwrap();
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let mut identity = local_identity();
		identity.owner_pid = child.id();
		state.adapter_state.local_workspace = Some(LocalWorkspace::Ready(ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity,
		}));
		assert!(state.local_workspace_owner.is_none());
		let close = update(&mut state, Message::CloseWindow);
		assert_eq!(
			close.units(),
			1,
			"ephemeral close without an owned workspace must emit the window-close task immediately"
		);
		assert!(!state.closing_local_workspace);
		drop(close);
		std::thread::sleep(std::time::Duration::from_millis(50));
		assert!(child.try_wait().unwrap().is_none());
		assert!(!signal.exists());
		child.kill().unwrap();
		child.wait().unwrap();
		std::fs::remove_dir_all(root).unwrap();
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn close_with_an_owned_workspace_enters_visible_bounded_shutdown() {
		let command = std::process::Command::new("/usr/bin/true");
		let owner = crate::local_workspace::LocalWorkspaceOwner::launch_command(
			command,
			nix::sys::signal::Signal::SIGINT,
		);
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.local_workspace_owner = Some(owner);
		let close = update(&mut state, Message::CloseWindow);
		assert_eq!(close.units(), 1, "owned close must wait before closing");
		assert!(state.closing_local_workspace);
		assert!(state.local_workspace_owner.is_none());
		assert!(state.status_error.is_empty());
		assert!(state.status_msg.contains("draining Toron before Wendelon"));
		let stopping = state.status_msg.clone();
		let absent = LocalWorkspace::Absent {
			profile_root: "/tmp/polariton-product-0006-closing".into(),
		};
		state.local_creation.observe(&absent);
		assert_eq!(update(&mut state, Message::StartLocalWorkspace).units(), 0);
		assert!(state.local_creation.offer().is_some());
		assert_eq!(
			update(&mut state, Message::LocalWorkspaceDiscovered(absent)).units(),
			0
		);
		assert_eq!(
			update(
				&mut state,
				Message::LocalWorkspaceStartupSettled(Err((
					LocalWorkspace::Absent {
						profile_root: "/tmp/polariton-product-0006-closing".into(),
					},
					"late failure".to_owned(),
				))),
			)
			.units(),
			0
		);
		assert_eq!(
			update(
				&mut state,
				Message::LocalWorkspaceAttached {
					workspace: ReadyWorkspace {
						sql_endpoint: "127.0.0.1:9".parse().unwrap(),
						identity: local_identity(),
					},
					connection: Err("late attach".to_owned()),
				},
			)
			.units(),
			0
		);
		assert_eq!(state.status_msg, stopping);
		assert!(state.status_error.is_empty());
		assert_eq!(
			update(&mut state, Message::CloseWindow).units(),
			0,
			"a second close must not start another stop"
		);
		let finish = update(
			&mut state,
			Message::LocalWorkspaceStopped("Stopped".to_owned()),
		);
		assert_eq!(
			finish.units(),
			1,
			"the ephemeral post-drain exit must emit the window-close task"
		);
		assert!(state.status_msg.contains("Local workspace stopped"));
		drop(close);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn startup_success_enters_the_existing_attach_path_only_after_ready() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let ready = ReadyWorkspace {
			sql_endpoint: "127.0.0.1:9".parse().unwrap(),
			identity: local_identity(),
		};
		assert!(state.adapter_state.local_workspace.is_none());
		let attach = update(
			&mut state,
			Message::LocalWorkspaceStartupSettled(Ok(ready.clone())),
		);
		assert_eq!(attach.units(), 1);
		assert_eq!(
			state.adapter_state.local_workspace,
			Some(LocalWorkspace::Ready(ready))
		);
		assert!(state.adapter_state.connection.is_none());
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn rune_relation_results_feed_the_existing_table_and_plot_surfaces() {
		let frame = polars::df!(
			"cat" => ["a", "b"],
			"x" => [1_i64, 2],
			"y" => [3_i64, 5],
		)
		.expect("fixture frame");
		let source = "let r = workspace.table(\"t\"); table(r); let marker = 1; plot.bar(r, \"cat\", \"y\"); let tail = marker + 1; plot.scatter(r, \"x\", \"y\", \"cat\"); plot.line(r, \"x\", \"y\")";
		let views = transform_rune_view(source)
			.expect("view envelope")
			.into_relations()
			.next()
			.expect("one relation")
			.views;
		let outcome = rune_view_outcome(frame, views);
		assert_eq!(outcome.sinks.len(), 4);
		for sink in &outcome.sinks {
			assert!(std::sync::Arc::ptr_eq(&outcome.result, &sink.source));
		}
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(table_skin)) =
			&outcome.sinks[0].output
		else {
			panic!("first sink must remain the default table");
		};
		assert_eq!(table_skin.skin, TableSkin::DEFAULT);
		assert_eq!(
			table_skin.authored_show_column_types,
			crate::plot::spec::Override::Inherit
		);
		for (sink, expected) in
			outcome.sinks[1..]
				.iter()
				.zip([PlotType::Bar, PlotType::Scatter, PlotType::Line])
		{
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(plot, _, _)) = &sink.output
			else {
				panic!("expected resolved plot");
			};
			assert_eq!(plot.data.height(), 2);
			assert_eq!(
				plot.resolved_spec.composition.primary_plot_type(),
				expected,
				"the Toron producer must carry the resolved specification"
			);
			assert!(
				plot.resolved_spec
					.channel(crate::plot::spec::Channel::X)
					.is_some()
			);
			assert!(
				plot.resolved_spec
					.channel(crate::plot::spec::Channel::Y)
					.is_some()
			);
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0052_completion_retains_authored_type_intent_without_resolving_application_state() {
		let relation = transform_rune_view(
			"workspace.table(\"t\").views([table().show_types(), table().skin(\"nord\").show_types(), table()])",
		)
		.expect("three table presentations")
		.into_relations()
		.next()
		.expect("one relation");
		let outcome = rune_view_outcome(
			polars::df!("value" => [1_i64]).expect("table frame"),
			relation.views,
		);
		let presentations = outcome
			.sinks
			.iter()
			.map(|sink| {
				let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(presentation)) =
					&sink.output
				else {
					panic!("every sink must complete as a table");
				};
				presentation
			})
			.collect::<Vec<_>>();
		assert_eq!(
			presentations[0].authored_show_column_types,
			crate::plot::spec::Override::Set(true)
		);
		assert_eq!(presentations[0].skin, TableSkin::DEFAULT);
		assert_eq!(
			presentations[1].authored_show_column_types,
			crate::plot::spec::Override::Set(true)
		);
		assert_eq!(
			presentations[1].skin,
			TableSkin::from_theme_name("nord", TableSkinProjection::PaletteForward).unwrap()
		);
		assert_eq!(
			presentations[2].authored_show_column_types,
			crate::plot::spec::Override::Inherit
		);
		assert_eq!(presentations[2].skin, TableSkin::DEFAULT);
	}

	#[cfg(toron_local)]
	#[test]
	fn multi_origin_dispatch_is_independent_while_each_origin_keeps_shared_identity() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let calls = Arc::new(Mutex::new(Vec::new()));
		let connection: Arc<RwLock<dyn DatabaseAdapter>> =
			Arc::new(RwLock::new(RecordingRuneAdapter {
				calls: calls.clone(),
				fail_table: None,
			}));

		let same_origin = transform_rune_view(
			"let r = workspace.table(\"a\"); table(r); plot.bar(r, \"x\", \"y\")",
		)
		.expect("same-origin fan-out");
		assert_eq!(same_origin.relation_count(), 1);
		let same_outcome = runtime
			.block_on(dispatch_rune_relation(
				connection.clone(),
				same_origin.into_relations().next().expect("one origin"),
			))
			.map(expect_views)
			.expect("same origin succeeds");
		assert_eq!(same_outcome.sinks.len(), 2);
		assert!(
			same_outcome
				.sinks
				.iter()
				.all(|sink| Arc::ptr_eq(&same_outcome.result, &sink.source))
		);

		let multi = transform_rune_view(
			"workspace.table(\"a\").views([bar(\"x\", \"y\")]); workspace.table(\"b\").views([bar(\"x\", \"y\")])",
		)
		.expect("two anonymous bars over two origins");
		assert_eq!(multi.relation_count(), 2);
		let mut relations = multi.into_relations().collect::<Vec<_>>();
		let second = relations.pop().expect("second origin");
		let first = relations.pop().expect("first origin");
		let second_outcome = runtime
			.block_on(dispatch_rune_relation(connection.clone(), second))
			.map(expect_views)
			.expect("second origin succeeds first");
		let first_outcome = runtime
			.block_on(dispatch_rune_relation(connection.clone(), first))
			.map(expect_views)
			.expect("first origin succeeds second");
		assert_eq!(
			(
				&second_outcome.sinks[0].identity,
				&first_outcome.sinks[0].identity,
			),
			(
				&LocalViewIdentity::Anonymous(1),
				&LocalViewIdentity::Anonymous(0),
			)
		);
		assert_eq!(calls.lock().expect("recording lock").len(), 3);
	}

	#[cfg(toron_local)]
	#[test]
	fn one_origin_failure_does_not_cancel_its_sibling_dispatch() {
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let calls = Arc::new(Mutex::new(Vec::new()));
		let connection: Arc<RwLock<dyn DatabaseAdapter>> =
			Arc::new(RwLock::new(RecordingRuneAdapter {
				calls: calls.clone(),
				fail_table: Some("bad"),
			}));
		let program = transform_rune_view(
			"workspace.table(\"good\").views([table()]); workspace.table(\"bad\").views([table()])",
		)
		.expect("two table origins");
		let mut results = program
			.into_relations()
			.map(|relation| runtime.block_on(dispatch_rune_relation(connection.clone(), relation)))
			.collect::<Vec<_>>();

		assert!(results.remove(0).is_ok());
		assert!(results.remove(0).is_err());
		assert_eq!(calls.lock().expect("recording lock").len(), 2);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn bound_view_set_stops_without_dispatch_while_view_free_rune_still_runs_locally() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(74);
		let bound_source = "let v = workspace.table(\"t\").views([table()]);";
		let bound = super::run_rune_code(&mut state, source_buffer, bound_source.to_owned());
		assert_eq!(bound.units(), 1);
		assert_eq!(
			complete_rune_presentation(&mut state, source_buffer, bound_source).units(),
			0
		);
		assert_eq!(state.rune_view_scheduler.submitted, 0);
		assert_eq!(state.rune_view_scheduler.presentation_submitted, 1);
		assert_eq!(
			state.status_msg,
			"Rune view set is bound; no output root emitted."
		);

		let local_source = "let answer = 42; answer";
		let local = super::run_rune_code(&mut state, source_buffer, local_source.into());
		assert_eq!(
			local.units(),
			1,
			"plain Rune first enters presentation preflight"
		);
		let local = complete_rune_presentation(&mut state, source_buffer, local_source);
		assert_eq!(
			local.units(),
			1,
			"view-free Rune still reaches the local host"
		);
		assert_eq!(state.rune_buffer_revisions.get(&source_buffer), Some(&2));
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn duplicate_view_names_refuse_through_the_head_with_both_spans() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(75);
		let source = "workspace.table(\"a\").views([view(\"same\", table())]); workspace.table(\"b\").views([view(\"same\", bar(\"x\", \"y\"))])";
		let first = source.find("\"same\"").expect("first name");
		let second = source.rfind("\"same\"").expect("second name");

		let task = super::run_rune_code(&mut state, source_buffer, source.to_owned());

		assert_eq!(task.units(), 1);
		assert_eq!(
			complete_rune_presentation(&mut state, source_buffer, source).units(),
			0
		);
		assert_eq!(state.rune_view_scheduler.submitted, 0);
		assert_eq!(state.rune_view_scheduler.presentation_submitted, 1);
		assert!(
			state
				.status_error
				.contains(&format!("{first}..{}", first + 6))
		);
		assert!(
			state
				.status_error
				.contains(&format!("{second}..{}", second + 6))
		);
		assert!(state.status_error.starts_with("Rune · "));
		let detail = state
			.status_detail
			.as_ref()
			.expect("Rune diagnostic detail");
		assert_eq!(detail.summary, state.status_error);
		assert!(detail.full.contains(&format!("{first}..{}", first + 6)));
		assert!(detail.full.contains(&format!("{second}..{}", second + 6)));
		assert!(
			!state.code_editor.has_query_surface(),
			"a Rune envelope error must not manufacture a result pane"
		);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn bad_plot_field_fails_only_its_spanned_sink_while_table_survives() {
		let source = "let r = workspace.table(\"t\"); table(r); plot.bar(r, \"missing\", \"y\")";
		let transformed = transform_rune_view(source).expect("view envelope");
		let origin = transformed.origin.clone();
		let views = transformed
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		let outcome = rune_view_outcome(
			polars::df!("y" => [1_i64, 2]).expect("fixture frame"),
			transformed.views.clone(),
		);
		assert_eq!(outcome.sinks.len(), 2);
		assert_eq!(outcome.sinks[0].presentation, RuneViewForm::Table.into());
		assert!(outcome.sinks[0].output.is_ok());
		assert_eq!(outcome.sinks[1].presentation, RuneViewForm::Bar.into());
		let Err(error) = &outcome.sinks[1].output else {
			panic!("bad field must fail only this sink");
		};
		assert!(
			error.contains("missing"),
			"field attribution was lost: {error}"
		);
		let span = outcome.sinks[1].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"plot.bar(r, \"missing\", \"y\")"
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(73);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		let presentation = transformed
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: transformed,
				})
				.is_some()
		);
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 1,
				execution_index: 0,
				origin,
				views: views.clone(),
				result: Ok(crate::gui::messages::RuneRelationCompletion::Views(outcome)),
			},
		));
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &views[0]),
			"the plot failure must not replace the sibling table"
		);
		assert!(state.status_error.contains("plot.bar at bytes"));
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0035_authored_table_skin_resolution_is_per_sink_and_keeps_shared_identity() {
		let source = r#"workspace.table("t").views([
			table().skin("terrain_rs10"),
			table(),
			table().skin("dark2_s1"),
			line("x", "y")
		])"#;
		let relation = transform_rune_view(source)
			.expect("theme-projected table sinks")
			.into_relations()
			.next()
			.expect("one shared origin");
		let outcome = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [3_i64, 4]).expect("fixture frame"),
			relation.views,
		);
		assert_eq!(outcome.sinks.len(), 4);
		for sink in &outcome.sinks {
			assert!(Arc::ptr_eq(&outcome.result, &sink.source));
		}

		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(styled)) =
			&outcome.sinks[0].output
		else {
			panic!("styled table must resolve");
		};
		assert_eq!(
			styled.skin,
			TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
				.unwrap()
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(default)) =
			&outcome.sinks[1].output
		else {
			panic!("bare table must resolve");
		};
		assert_eq!(default.skin, TableSkin::DEFAULT);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(authored)) =
			&outcome.sinks[2].output
		else {
			panic!("defined authored palette must render");
		};
		assert_eq!(
			authored.skin,
			TableSkin::from_theme_name("dark2_s1", TableSkinProjection::PaletteForward).unwrap()
		);
		assert!(!authored.skin.passes_contrast());
		assert!(matches!(
			outcome.sinks[3].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(..))
		));
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0035_authored_projection_resolves_once_without_advice_or_fallback() {
		fn themed(name: &str, projection: RuneTableSkinProjection) -> RuneTableSkinIntent {
			RuneTableSkinIntent::NamedTheme {
				name: name.to_owned(),
				projection,
			}
		}

		fn resolve_traced(
			intent: RuneTableSkinIntent,
		) -> (
			std::result::Result<TableSkin, String>,
			Vec<(String, TableSkinProjection)>,
		) {
			let mut calls = Vec::new();
			let result = resolve_rune_table_skin_with(intent, |name, projection| {
				calls.push((name.to_owned(), projection));
				TableSkin::from_theme_name(name, projection)
			});
			(result, calls)
		}

		let (default, calls) = resolve_traced(RuneTableSkinIntent::Default);
		assert_eq!(default, Ok(TableSkin::DEFAULT));
		assert!(calls.is_empty());

		let (palette_forward, calls) =
			resolve_traced(themed("yellows", RuneTableSkinProjection::PaletteForward));
		assert_eq!(
			palette_forward,
			TableSkin::from_theme_name("yellows", TableSkinProjection::PaletteForward)
				.map_err(|error| error.to_string())
		);
		assert_eq!(
			calls,
			[("yellows".to_owned(), TableSkinProjection::PaletteForward)]
		);

		let (duotone, calls) = resolve_traced(themed("yellows", RuneTableSkinProjection::Duotone));
		assert_eq!(
			duotone,
			TableSkin::from_theme_name("yellows", TableSkinProjection::Duotone)
				.map_err(|error| error.to_string())
		);
		assert!(!duotone.unwrap().passes_contrast());
		assert_eq!(
			calls,
			[("yellows".to_owned(), TableSkinProjection::Duotone)]
		);

		let (unknown, calls) = resolve_traced(themed(
			"definitely-not-a-theme",
			RuneTableSkinProjection::Duotone,
		));
		assert_eq!(
			unknown,
			Err("unknown table skin theme \"definitely-not-a-theme\"".to_owned())
		);
		assert_eq!(
			calls,
			[(
				"definitely-not-a-theme".to_owned(),
				TableSkinProjection::Duotone
			)]
		);
		let (unknown, calls) = resolve_traced(themed(
			"definitely-not-a-theme",
			RuneTableSkinProjection::PaletteForward,
		));
		assert_eq!(
			unknown,
			Err("unknown table skin theme \"definitely-not-a-theme\"".to_owned())
		);
		assert_eq!(
			calls,
			[(
				"definitely-not-a-theme".to_owned(),
				TableSkinProjection::PaletteForward
			)]
		);

		let (duotone_success, calls) =
			resolve_traced(themed("terrain_rs10", RuneTableSkinProjection::Duotone));
		assert_eq!(
			duotone_success,
			TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::Duotone)
				.map_err(|error| error.to_string())
		);
		assert_eq!(
			calls,
			[("terrain_rs10".to_owned(), TableSkinProjection::Duotone)]
		);

		let (duotone_below, calls) =
			resolve_traced(themed("dark2_s1", RuneTableSkinProjection::Duotone));
		assert_eq!(
			duotone_below,
			TableSkin::from_theme_name("dark2_s1", TableSkinProjection::Duotone)
				.map_err(|error| error.to_string())
		);
		assert!(!duotone_below.unwrap().passes_contrast());
		assert_eq!(
			calls,
			[("dark2_s1".to_owned(), TableSkinProjection::Duotone)]
		);

		let (palette_forward_below, calls) =
			resolve_traced(themed("dark2_s1", RuneTableSkinProjection::PaletteForward));
		assert_eq!(
			palette_forward_below,
			TableSkin::from_theme_name("dark2_s1", TableSkinProjection::PaletteForward)
				.map_err(|error| error.to_string())
		);
		assert!(!palette_forward_below.unwrap().passes_contrast());
		assert_eq!(
			calls,
			[("dark2_s1".to_owned(), TableSkinProjection::PaletteForward)]
		);
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0045_pane_table_skin_restyles_only_the_completed_draw() {
		let source_buffer = BufferId::new(111);
		let identity = LocalViewIdentity::Named("table".to_owned());
		let program = transform_rune_view(
			"workspace.table(\"t\").views([view(\"table\", table().skin(\"terrain_rs10\"))])",
		)
		.expect("named authored table");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		let frame = polars::df!("value" => [1_i64, 2]).expect("table frame");
		let baked = TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
			.expect("authored baked skin");
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.expect("prepared table panel");
		state
			.code_editor
			.set_rune_view_table_with_skin(panel_id, frame.clone(), baked);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &identity),
			Some(baked)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_draw_skin(source_buffer, &identity),
			Some(baked)
		);

		let pane = TableSkinSpec::new("yellows", TableSkinProjection::Duotone);
		let pane_skin = pane.resolve().expect("valid pane test producer");
		state
			.code_editor
			.set_test_rune_view_pane_table_skin(source_buffer, &identity, pane);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &identity),
			Some(baked),
			"the pane tier must not mutate the completed artifact"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_draw_skin(source_buffer, &identity),
			Some(pane_skin)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table(source_buffer, &identity),
			Some(&frame),
			"the pane tier must not replace the completed frame"
		);
		state
			.code_editor
			.clear_test_rune_view_pane_table_skin(source_buffer, &identity);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_draw_skin(source_buffer, &identity),
			Some(baked),
			"removing the pane tier restores the baked artifact without a rerun"
		);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0027_focused_high_density_shape_parses_and_completes_every_sink() {
		let source = r#"ket.table("test1")
	.sort([col("x").asc()])
	.views([
		table().skin("terrain_rs10"),
		figure([
			title("Plot Layer Test"),
			subtitle("Toron was here"),
			layers([
				line("x", "y")
					.stroke_width(8.0),
				scatter("x", "y")
					.point_size(8.0)])])])
ket.table("test2") // Comment Test1.
	.sort([col("cat").asc()])
	.views([
		table(), // Comment Test2.
		figure([
			title("SQL = 💩"),
			subtitle("Wendelon was here"),
			bar("cat", "val")
				.bar_width(0.98)])])"#;
		let relations = transform_rune_view(source)
			.expect("focused accepted high-density shape")
			.into_relations()
			.collect::<Vec<_>>();
		assert_eq!(relations.len(), 2);

		let first = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [3_i64, 4]).expect("test1 frame"),
			relations[0].views.clone(),
		);
		assert_eq!(first.sinks.len(), 2);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(skin)) = &first.sinks[0].output
		else {
			panic!("test1 table must complete with its skin");
		};
		assert_eq!(
			skin.skin,
			TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
				.unwrap()
		);
		assert!(matches!(
			first.sinks[1].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(
				_,
				_
			))
		));

		let second = rune_view_outcome(
			polars::df!("cat" => ["a", "b"], "val" => [3_i64, 4]).expect("test2 frame"),
			relations[1].views.clone(),
		);
		assert_eq!(second.sinks.len(), 2);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Table(skin)) = &second.sinks[0].output
		else {
			panic!("test2 table must remain default-skinned");
		};
		assert_eq!(skin.skin, TableSkin::DEFAULT);
		assert!(matches!(
			second.sinks[1].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(..))
		));
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0035_projection_changes_reuse_panels_and_below_threshold_skin_replaces_stale() {
		fn program(theme: &str, duotone: bool) -> TransformedRuneView {
			let projection = if duotone { ".duotone()" } else { "" };
			transform_rune_view(&format!(
				"workspace.table(\"t\").views([view(\"styled\", table().skin(\"{theme}\"){projection}), view(\"plain\", table()), view(\"bad\", table().skin(\"dark2_s1\"))])"
			))
			.expect("three named tables")
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(91);
		let styled = LocalViewIdentity::Named("styled".to_owned());
		let plain = LocalViewIdentity::Named("plain".to_owned());
		let bad = LocalViewIdentity::Named("bad".to_owned());
		let previous_bad_skin =
			TableSkin::from_theme_name("terrain_r", TableSkinProjection::Duotone).unwrap();

		let first = program("terrain_rs10", true);
		let presentation = first
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		let panel_ids = [&styled, &plain, &bad].map(|identity| {
			state
				.code_editor
				.rune_view_panel_id(source_buffer, identity)
				.expect("prepared named panel")
		});
		state.code_editor.set_rune_view_table_with_skin(
			panel_ids[2],
			polars::df!("previous" => [1_i64]).expect("previous bad frame"),
			previous_bad_skin,
		);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: first.clone(),
				})
				.is_some()
		);
		let relation = first.into_relations().next().unwrap();
		let outcome = rune_view_outcome(
			polars::df!("value" => [2_i64]).expect("first frame"),
			relation.views,
		);
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 1,
				execution_index: 0,
				origin: relation.origin,
				views: vec![styled.clone(), plain.clone(), bad.clone()],
				result: Ok(crate::gui::messages::RuneRelationCompletion::Views(outcome)),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &styled),
			Some(TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::Duotone).unwrap())
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &plain),
			Some(TableSkin::DEFAULT)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &bad),
			Some(
				TableSkin::from_theme_name("dark2_s1", TableSkinProjection::PaletteForward)
					.unwrap()
			),
			"defined authored projection must replace the prior artifact"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &bad),
			Some("fresh")
		);
		assert!(!state.status_error.contains("table at bytes"));

		let second = program("terrain_rs10", false);
		let presentation = second
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 2);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 2);
		assert_eq!(
			[&styled, &plain, &bad].map(|identity| state
				.code_editor
				.rune_view_panel_id(source_buffer, identity)
				.unwrap()),
			panel_ids,
			"skin changes must not replace destinations"
		);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 2,
					view: second.clone(),
				})
				.is_some()
		);
		let relation = second.into_relations().next().unwrap();
		let outcome = rune_view_outcome(
			polars::df!("value" => [3_i64]).expect("second frame"),
			relation.views,
		);
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 2,
				execution_index: 0,
				origin: relation.origin,
				views: vec![styled.clone(), plain, bad],
				result: Ok(crate::gui::messages::RuneRelationCompletion::Views(outcome)),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_table_skin(source_buffer, &styled),
			Some(
				TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::PaletteForward)
					.unwrap()
			)
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &styled),
			Some(panel_ids[0])
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 3);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0014_compatibility_crosses_the_factory_and_returns_to_the_outer_span() {
		let source = "workspace.table(\"t\").views([layers([line(\"x\", \"y\"), scatter(\"other_x\", \"y\")])])";
		let relation = transform_rune_view(source)
			.expect("surface grammar accepts differing fields")
			.into_relations()
			.next()
			.expect("one relation");
		let span = relation.views[0].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"layers([line(\"x\", \"y\"), scatter(\"other_x\", \"y\")])"
		);
		let outcome = rune_view_outcome(
			polars::df!(
				"x" => [1.0_f64, 2.0],
				"other_x" => [3.0_f64, 4.0],
				"y" => [5.0_f64, 6.0],
			)
			.expect("fixture frame"),
			relation.views,
		);
		let [sink] = outcome.sinks.as_slice() else {
			panic!("one layers sink must remain one outcome")
		};
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
			&sink.output
		else {
			panic!("the envelope must hand a layered specification to the head")
		};
		let projected = sink.source.projection().expect("fixture projection");
		let Err(error) = create_layered_plot(specification, projected.as_ref(), 1200, 720) else {
			panic!("PRODUCT-0013 must own incompatible field refusal")
		};
		assert!(error.contains("layer 2"));
		assert!(error.contains("x"));
		assert!(error.contains("other_x"));

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(74);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		let identity = sink.identity.clone();
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(identity.clone(), sink.presentation.clone())],
			revision,
		);
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.expect("prepared layered destination");
		let recipe = crate::plot::recipe::PlotRecipe::layered(
			Arc::clone(&sink.source),
			specification.clone(),
			None,
			crate::plot::grid::FigureGridLayoutSettings::default(),
			1200,
			720,
		);
		drop(update(
			&mut state,
			Message::RuneRelationLayeredPlotReady {
				source_buffer,
				revision,
				panel_id,
				presentation: sink.presentation.clone(),
				authored_settings: crate::plot::theme::PlotSettingsLayer::default(),
				span: span.clone(),
				result: Err(error.clone()),
				recipe,
			},
		));
		let detail = state.status_detail.as_ref().expect("full Rune diagnostic");
		assert!(
			detail
				.full
				.contains(&format!("at bytes {}..{}: {error}", span.start, span.end))
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_failed(source_buffer, &identity)
		);
		assert!(
			!state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &identity)
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0014_layered_view_completes_in_a_label_only_host_panel() {
		let source = r#"
workspace.table("t").views([
    table(),
    bar("cat", "val"),
    line("x", "y"),
    scatter("x", "y"),
    view("trend", layers([line("x", "y"), scatter("x", "y")]))])
"#;
		let program = transform_rune_view(source).expect("layered host program");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		assert_eq!(
			presentation
				.iter()
				.map(|(_, presentation)| presentation.display_label.as_str())
				.collect::<Vec<_>>(),
			[
				"table",
				"plot.bar",
				"plot.line",
				"plot.scatter",
				"Line + Scatter"
			]
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(75);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, revision);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program.clone(),
				})
				.is_some()
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			revision,
			program.into_relations().next().expect("one relation"),
			polars::df!(
				"cat" => ["a", "b"],
				"val" => [3_i64, 5],
				"x" => [1.0_f64, 2.0],
				"y" => [2.0_f64, 4.0],
			)
			.expect("host fixture"),
		);

		for (identity, expected) in [
			(LocalViewIdentity::Anonymous(1), "plot.bar"),
			(LocalViewIdentity::Anonymous(2), "plot.line"),
			(LocalViewIdentity::Anonymous(3), "plot.scatter"),
			(
				LocalViewIdentity::Named("trend".to_owned()),
				"Line + Scatter",
			),
		] {
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_display_label(source_buffer, &identity),
				Some(expected)
			);
		}
		assert!(
			state.code_editor.rune_view_panel_is_plot(
				source_buffer,
				&LocalViewIdentity::Named("trend".to_owned())
			),
			"the layered kernel must reach ordinary host plot content"
		);

		let trend = LocalViewIdentity::Named("trend".to_owned());
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.rune_view_panel_projected_title(source_buffer, &trend)
			.is_none()
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &trend),
			Some("trend"),
			"panel chrome follows identity while the canvas follows display_label"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &trend),
			Some("Line + Scatter")
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0014_named_layer_edits_refresh_one_identity_and_destination() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(76);
		let identity = LocalViewIdentity::Named("trend".to_owned());
		let mut first_panel = None;
		for (revision, source, expected_label) in [
			(
				1,
				"workspace.table(\"t\").views([view(\"trend\", layers([line(\"x\", \"y\"), scatter(\"x\", \"y\")]))])",
				"Line + Scatter",
			),
			(
				2,
				"workspace.table(\"t\").views([view(\"trend\", layers([scatter(\"x\", \"y\"), line(\"x\", \"y\")]))])",
				"Scatter + Line",
			),
			(
				3,
				"workspace.table(\"t\").views([view(\"trend\", layers([line(\"x\", \"y\")]))])",
				"Line",
			),
		] {
			let program = transform_rune_view(source).expect("named layered revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation.len(), 1, "layers are not views");
			assert_eq!(presentation[0].0, identity);
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let panel_id = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("named layered panel");
			if let Some(first) = first_panel {
				assert_eq!(panel_id, first, "layer edits retain the destination");
			} else {
				first_panel = Some(panel_id);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				polars::df!("x" => [1.0_f64, 2.0], "y" => [2.0_f64, 4.0]).expect("layer frame"),
			);
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_display_label(source_buffer, &identity),
				Some(expected_label)
			);
		}
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0014_one_layer_spelling_keeps_anonymous_identity_and_svg() {
		let bare_source = "workspace.table(\"t\").views([line(\"x\", \"y\")])";
		let layered_source = "workspace.table(\"t\").views([layers([line(\"x\", \"y\")])])";
		let figure_source = "workspace.table(\"t\").views([figure([line(\"x\", \"y\")])])";
		let frame = polars::df!("x" => [1.0_f64, 2.0], "y" => [2.0_f64, 4.0]).expect("line frame");

		let bare_relation = transform_rune_view(bare_source)
			.expect("bare line")
			.into_relations()
			.next()
			.expect("one relation");
		let bare_outcome = rune_view_outcome(frame.clone(), bare_relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _)) =
			&bare_outcome.sinks[0].output
		else {
			panic!("bare line must use the established plot request")
		};
		let bare_kernel = create_plot(&resolved.resolved_spec, &resolved.data, 100, 100);

		let layered_relation = transform_rune_view(layered_source)
			.expect("one-layer surface")
			.into_relations()
			.next()
			.expect("one relation");
		let layered_outcome = rune_view_outcome(frame.clone(), layered_relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
			&layered_outcome.sinks[0].output
		else {
			panic!("layers spelling must retain the layered request")
		};
		let layered_kernel = create_layered_plot(specification, &frame, 100, 100)
			.expect("one-layer PRODUCT-0013 path");
		let figure_relation = transform_rune_view(figure_source)
			.expect("one-body figure")
			.into_relations()
			.next()
			.expect("one relation");
		let figure_outcome = rune_view_outcome(frame.clone(), figure_relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(figure, _, _)) =
			&figure_outcome.sinks[0].output
		else {
			panic!("one-body figure must retain the single plot path")
		};
		let figure_kernel = create_plot(&figure.resolved_spec, &figure.data, 100, 100);
		assert_eq!(
			render_marks(bare_kernel.as_ref()),
			render_marks(layered_kernel.as_ref())
		);
		assert_eq!(
			render_marks(bare_kernel.as_ref()),
			render_marks(figure_kernel.as_ref())
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(77);
		let identity = LocalViewIdentity::Anonymous(0);
		let mut panel = None;
		for (revision, source) in [(1, bare_source), (2, layered_source), (3, figure_source)] {
			let program = transform_rune_view(source).expect("line spelling revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("anonymous line panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "spelling is not identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
		}
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.line")
		);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0018_point_size_reaches_every_host_path_and_preserves_the_view() {
		let frame = polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
			.expect("scatter frame");
		let every_position = transform_rune_view(
			r#"workspace.table("t").views([
				layers([scatter("x", "y").point_size(2.0)]),
				scatter("x", "y").point_size(2.0),
				figure([title("Sized"), scatter("x", "y").point_size(2.0)]),
			])"#,
		)
		.expect("all proven modified-mark positions")
		.into_relations()
		.next()
		.expect("one relation");
		let every_outcome = rune_view_outcome(frame.clone(), every_position.views);
		assert_eq!(every_outcome.sinks.len(), 3);
		for sink in &every_outcome.sinks {
			assert!(Arc::ptr_eq(&every_outcome.result, &sink.source));
			let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
				&sink.output
			else {
				panic!("every modified mark must use the proved layer-aware output")
			};
			let mut layers = specification.layers.iter();
			let layer = layers.next().expect("one authored layer");
			assert!(
				layers.next().is_none(),
				"each spelling must lower to exactly one authored layer"
			);
			assert_eq!(
				layer.settings.point_size,
				crate::plot::spec::Override::Set(crate::plot::spec::PointSize::new(2.0).unwrap())
			);
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(84);
		let identity = LocalViewIdentity::Named("points".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"points\", scatter(\"x\", \"y\"))])",
			"workspace.table(\"t\").views([view(\"points\", scatter(\"x\", \"y\").point_size(1.0))])",
			"workspace.table(\"t\").views([view(\"points\", scatter(\"x\", \"y\").point_size(2.0))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("point-size lifecycle revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			assert_eq!(presentation[0].1.display_label, "plot.scatter");
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable point-size panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "point size is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed point-size canvas"),
			);
		}
		assert_eq!(svgs[0], svgs[1], "factor 1.0 must preserve bare output");
		assert!(
			svgs[2].contains("A 6.00 6.00"),
			"factor 2.0 must reach the six-pixel host rendering: {}",
			svgs[2]
		);
		assert_ne!(svgs[1], svgs[2]);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.scatter")
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0046_category_scatter_point_size_uses_the_standalone_carrier() {
		let frame = polars::df!(
			"units" => [10.0_f64, 20.0, 30.0],
			"revenue" => [40.0_f64, 80.0, 120.0],
			"product" => ["A", "B", "C"],
		)
		.expect("categorical scatter frame");
		let program = transform_rune_view(
			r#"workspace.table("t").views([figure([
				scatter("units", "revenue", "product").point_size(2.0),
			])])"#,
		)
		.expect("styled categorical scatter");
		let relation = program
			.clone()
			.into_relations()
			.next()
			.expect("one relation");
		let outcome = rune_view_outcome(frame.clone(), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
			resolved,
			layer_settings,
			_,
			_,
		)) = &outcome.sinks[0].output
		else {
			panic!("a styled categorical scatter must use the standalone carrier")
		};
		assert_eq!(
			resolved.resolved_spec.composition.primary_plot_type(),
			crate::plot::core::PlotType::Scatter
		);
		assert_eq!(
			layer_settings.point_size,
			crate::plot::spec::Override::Set(crate::plot::spec::PointSize::new(2.0).unwrap())
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(110);
		let identity = LocalViewIdentity::Named("points".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"points\", scatter(\"units\", \"revenue\", \"product\").point_size(1.0))])",
			"workspace.table(\"t\").views([view(\"points\", scatter(\"units\", \"revenue\", \"product\").point_size(2.0))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("categorical point-size revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable categorical point-size panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "point size is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed categorical point-size canvas"),
			);
		}
		assert!(svgs[1].contains("A 6.00 6.00"));
		assert_ne!(svgs[0], svgs[1]);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0020_stroke_width_reaches_host_and_combines_with_point_size() {
		let frame = polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
			.expect("line frame");
		let positions = transform_rune_view(
			r#"workspace.table("t").views([
				layers([line("x", "y").stroke_width(2.0)]),
				line("x", "y").stroke_width(2.0),
				figure([title("Wide"), line("x", "y").stroke_width(2.0)]),
			])"#,
		)
		.expect("all proven stroke-width positions")
		.into_relations()
		.next()
		.expect("one relation");
		let position_outcome = rune_view_outcome(frame.clone(), positions.views);
		assert_eq!(position_outcome.sinks.len(), 3);
		for sink in &position_outcome.sinks {
			assert!(Arc::ptr_eq(&position_outcome.result, &sink.source));
			let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, _)) =
				&sink.output
			else {
				panic!("every styled line must use the layer-aware output")
			};
			let mut layers = specification.layers.iter();
			let layer = layers.next().expect("one line layer");
			assert!(layers.next().is_none());
			assert_eq!(layer.mark, crate::plot::spec::LayerMark::Line);
			assert_eq!(
				layer.settings.stroke_width,
				crate::plot::spec::Override::Set(crate::plot::spec::StrokeWidth::new(2.0).unwrap())
			);
		}

		let combo = transform_rune_view(
			r#"workspace.table("t").views([figure([
				title("High density"),
				subtitle("Two independent Layer settings"),
				layers([
					line("x", "y").stroke_width(2.0),
					scatter("x", "y").point_size(2.0),
				]),
			])])"#,
		)
		.expect("high-density combo")
		.into_relations()
		.next()
		.expect("one relation");
		let combo_outcome = rune_view_outcome(frame.clone(), combo.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, settings)) =
			&combo_outcome.sinks[0].output
		else {
			panic!("combo remains one layered output")
		};
		let layers = specification.layers.iter().collect::<Vec<_>>();
		assert_eq!(
			layers.iter().map(|layer| layer.mark).collect::<Vec<_>>(),
			[
				crate::plot::spec::LayerMark::Line,
				crate::plot::spec::LayerMark::Scatter
			]
		);
		assert!(matches!(
			layers[0].settings.stroke_width,
			crate::plot::spec::Override::Set(_)
		));
		assert!(matches!(
			layers[1].settings.point_size,
			crate::plot::spec::Override::Set(_)
		));
		assert!(matches!(
			settings.title,
			crate::plot::spec::Presence::Show(_)
		));
		assert!(matches!(
			settings.subtitle,
			crate::plot::spec::Presence::Show(_)
		));

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(85);
		let identity = LocalViewIdentity::Named("line".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"line\", line(\"x\", \"y\"))])",
			"workspace.table(\"t\").views([view(\"line\", line(\"x\", \"y\").stroke_width(1.0))])",
			"workspace.table(\"t\").views([view(\"line\", line(\"x\", \"y\").stroke_width(2.0))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("stroke-width lifecycle revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			assert_eq!(presentation[0].1.display_label, "plot.line");
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable line panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "stroke width is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed line canvas"),
			);
		}
		assert_eq!(svgs[0], svgs[1], "factor 1.0 must preserve bare line");
		assert!(svgs[2].contains("stroke-width=\"4\""));
		assert_ne!(svgs[1], svgs[2]);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.line")
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0022_bar_width_reaches_the_shared_result_and_real_host_panel() {
		let frame = polars::df!("cat" => ["A", "B"], "val" => [1.0_f64, 2.0]).expect("bar frame");
		let positions = transform_rune_view(
			r#"workspace.table("t").views([
				bar("cat", "val").bar_width(0.5),
				figure([title("Narrow bars"), bar("cat", "val").bar_width(0.5)]),
			])"#,
		)
		.expect("direct and figure bar width")
		.into_relations()
		.next()
		.expect("one relation");
		let outcome = rune_view_outcome(frame.clone(), positions.views);
		assert_eq!(outcome.sinks.len(), 2);
		for (index, sink) in outcome.sinks.iter().enumerate() {
			assert!(Arc::ptr_eq(&outcome.result, &sink.source));
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
				resolved,
				settings,
				authored,
				_,
			)) = &sink.output
			else {
				panic!("styled bar must use the standalone styled-plot path")
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				crate::plot::core::PlotType::Bar
			);
			assert_eq!(
				settings.bar_width,
				crate::plot::spec::Override::Set(crate::plot::spec::BarWidth::new(0.5).unwrap())
			);
			assert_eq!(
				matches!(authored.title, crate::plot::spec::Presence::Show(_)),
				index == 1
			);
		}

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(86);
		let identity = LocalViewIdentity::Named("bars".to_owned());
		let sources = [
			"workspace.table(\"t\").views([view(\"bars\", bar(\"cat\", \"val\"))])",
			"workspace.table(\"t\").views([view(\"bars\", bar(\"cat\", \"val\").bar_width(0.9))])",
			"workspace.table(\"t\").views([view(\"bars\", bar(\"cat\", \"val\").bar_width(0.5))])",
		];
		let mut panel = None;
		let mut svgs = Vec::new();
		for (offset, source) in sources.into_iter().enumerate() {
			let revision = offset as u64 + 1;
			let program = transform_rune_view(source).expect("bar-width lifecycle revision");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			assert_eq!(presentation[0].0, identity);
			assert_eq!(presentation[0].1.display_label, "plot.bar");
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			let current = state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable bar panel");
			if let Some(first) = panel {
				assert_eq!(current, first, "bar width is not view identity");
			} else {
				panel = Some(current);
			}
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			complete_relation_in_head(
				&mut state,
				source_buffer,
				revision,
				program.into_relations().next().expect("one relation"),
				frame.clone(),
			);
			svgs.push(
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, &identity)
					.expect("completed bar canvas"),
			);
		}
		assert_eq!(svgs[0], svgs[1], "0.9 must preserve the bare bar");
		assert_ne!(svgs[1], svgs[2], "0.5 must visibly narrow the bars");
		let bounds = iced::Rectangle::new(iced::Point::ORIGIN, iced::Size::new(100.0, 100.0));
		assert!(
			state
				.code_editor
				.rune_view_panel_plot_hover(
					source_buffer,
					&identity,
					bounds,
					iced::Point::new(20.0, 75.0),
				)
				.is_some()
		);
		assert_eq!(
			state.code_editor.rune_view_panel_plot_hover(
				source_buffer,
				&identity,
				bounds,
				iced::Point::new(10.0, 75.0),
			),
			None
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identity),
			Some("plot.bar")
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0016_figure_headings_reach_canvas_and_source_only_chrome() {
		let source = r#"workspace.table("t").views([
			view("stable", figure([title("Named canvas"), line("x", "y")])),
			figure([title("Revenue 📈"), subtitle("Private detail"), line("x", "y")]),
			figure([subtitle("Subtitle only"), line("x", "y")]),
			figure([title("Revenue"), line("x", "y")]),
			figure([title("Overlay"), subtitle("Both marks"), layers([line("x", "y"), scatter("x", "y")])]),
		])"#;
		let program = transform_rune_view(source).expect("figure surface");
		let relation = program.clone().into_relations().next().expect("one origin");
		let identities = relation
			.views
			.iter()
			.map(|view| view.identity.clone())
			.collect::<Vec<_>>();
		let presentations = relation
			.views
			.iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(82);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations, revision);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program,
				})
				.is_some()
		);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while identities.iter().any(|identity| {
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, identity)
				.is_none()
		}) && std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		let chrome = identities
			.iter()
			.map(|identity| {
				state
					.code_editor
					.rune_view_panel_projected_title(source_buffer, identity)
					.expect("projected figure panel")
					.to_owned()
			})
			.collect::<Vec<_>>();
		assert_eq!(
			chrome,
			[
				"stable",
				"Revenue 📈 2",
				"plot.line 3",
				"Revenue 4",
				"Overlay 5"
			]
		);
		assert!(chrome.iter().all(|title| {
			!title.contains("Private detail")
				&& !title.contains("Subtitle only")
				&& !title.contains("Both marks")
		}));

		complete_relation_in_head(
			&mut state,
			source_buffer,
			revision,
			relation,
			polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
				.expect("figure frame"),
		);
		let canvases = identities
			.iter()
			.map(|identity| {
				state
					.code_editor
					.rune_view_panel_plot_svg(source_buffer, identity)
					.expect("completed figure canvas")
			})
			.collect::<Vec<_>>();
		for (svg, expected) in canvases.iter().zip([
			"Named canvas",
			"Revenue 📈",
			"plot.line",
			"Revenue",
			"Overlay",
		]) {
			assert!(svg.contains(expected), "canvas must contain {expected}");
		}
		assert!(canvases[1].contains("Private detail"));
		assert!(canvases[2].contains("Subtitle only"));
		assert!(canvases[4].contains("Both marks"));
		assert!(!canvases[1].contains("plot.line"));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &identities[4]),
			Some("Line + Scatter")
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0016_heading_lifecycle_keeps_destination_layout_and_atomic_canvas() {
		let anonymous = LocalViewIdentity::Anonymous(0);
		let named = LocalViewIdentity::Named("stable".to_owned());
		let identities = [anonymous.clone(), named.clone()];
		let frame = polars::df!("x" => [1.0_f64, 2.0, 3.0], "y" => [2.0_f64, 4.0, 3.0])
			.expect("lifecycle frame");
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		let source_buffer = loop {
			state.code_editor.poll();
			if let Some(buffer_id) = state.code_editor.active_buffer_id() {
				break buffer_id;
			}
			assert!(
				std::time::Instant::now() < deadline,
				"source buffer must project before the lifecycle assay"
			);
			std::thread::sleep(std::time::Duration::from_millis(5));
		};

		let program_a = transform_rune_view(
			r#"workspace.table("t").views([
				figure([title("A"), subtitle("A detail"), line("x", "y")]),
				view("stable", figure([title("Named A"), subtitle("Named detail"), line("x", "y")]))
			])"#,
		)
		.expect("revision A");
		let presentations_a = program_a
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations_a, 1);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: program_a.clone(),
				})
				.is_some()
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			1,
			program_a.into_relations().next().expect("one A relation"),
			frame.clone(),
		);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while identities.iter().any(|identity| {
			state
				.code_editor
				.rune_view_panel_layout_signature(source_buffer, identity)
				.is_none()
		}) && std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		let panels = identities.clone().map(|identity| {
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity)
				.expect("stable panel")
		});
		let layout = identities.clone().map(|identity| {
			state
				.code_editor
				.rune_view_panel_layout_signature(source_buffer, &identity)
				.expect("stable layout")
		});
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &anonymous),
			Some("A 1")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &named),
			Some("stable")
		);
		for (identity, title, subtitle) in [
			(&anonymous, "A", "A detail"),
			(&named, "Named A", "Named detail"),
		] {
			let svg = state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, identity)
				.expect("A canvas");
			assert!(svg.contains(title));
			assert!(svg.contains(subtitle));
		}

		let program_b = transform_rune_view(
			r#"workspace.table("t").views([
				figure([title("B"), subtitle("B detail"), line("x", "y")]),
				view("stable", figure([title("Named B"), subtitle("Changed"), line("x", "y")]))
			])"#,
		)
		.expect("revision B");
		let presentations_b = program_b
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 2);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations_b, 2);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 2,
					view: program_b.clone(),
				})
				.is_some()
		);
		let relation_b = program_b.into_relations().next().expect("one B relation");
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 2,
				execution_index: 0,
				origin: relation_b.origin,
				views: relation_b
					.views
					.into_iter()
					.map(|view| view.identity)
					.collect(),
				result: Err("revision B failed".to_owned()),
			},
		));
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.rune_view_panel_projected_title(source_buffer, &anonymous)
			!= Some("B 1")
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &anonymous),
			Some("B 1"),
			"chrome follows successfully analyzed source"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &named),
			Some("stable")
		);
		for (identity, title, subtitle) in [
			(&anonymous, "A", "A detail"),
			(&named, "Named A", "Named detail"),
		] {
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_status(source_buffer, identity),
				Some("stale")
			);
			let svg = state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, identity)
				.expect("stale A canvas");
			assert!(svg.contains(title));
			assert!(svg.contains(subtitle));
			assert!(!svg.contains("B detail"));
			assert!(!svg.contains("Named B"));
		}

		let program_c = transform_rune_view(
			"workspace.table(\"t\").views([line(\"x\", \"y\"), view(\"stable\", line(\"x\", \"y\"))])",
		)
		.expect("revision C");
		let presentations_c = program_c
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.presentation()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, 3);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentations_c, 3);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 3,
					view: program_c.clone(),
				})
				.is_some()
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			3,
			program_c.into_relations().next().expect("one C relation"),
			frame,
		);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state
			.code_editor
			.rune_view_panel_projected_title(source_buffer, &anonymous)
			!= Some("plot.line 1")
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &anonymous),
			Some("plot.line 1")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_projected_title(source_buffer, &named),
			Some("stable")
		);
		for (index, identity) in identities.iter().enumerate() {
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_id(source_buffer, identity),
				Some(panels[index])
			);
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_layout_signature(source_buffer, identity),
				Some(layout[index].clone()),
				"descriptor refresh and completion preserve the output-group layout"
			);
			assert_eq!(
				state
					.code_editor
					.rune_view_panel_status(source_buffer, identity),
				Some("fresh")
			);
			let svg = state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, identity)
				.expect("C canvas");
			assert!(svg.contains("plot.line"));
			assert!(!svg.contains("A detail"));
			assert!(!svg.contains("B detail"));
		}
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn dogfood_multi_origin_views_populate_four_host_panels_without_a_dashboard() {
		let source = r#"
ket.table("test1")
	.sort([col("x").asc()])
	.views([
		line("x", "y"),
		table()])
ket.table("test2")
	.sort([col("cat").asc()])
	.views([
		bar("cat", "val"),
		table()])
"#;
		let program = transform_rune_view(source).expect("dogfood view program");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		assert_eq!(
			presentation,
			vec![
				(LocalViewIdentity::Anonymous(0), RuneViewForm::Line),
				(LocalViewIdentity::Anonymous(1), RuneViewForm::Table),
				(LocalViewIdentity::Anonymous(2), RuneViewForm::Bar),
				(LocalViewIdentity::Anonymous(3), RuneViewForm::Table),
			]
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(80);
		let revision = 1;
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, revision);
		let panel_ids = presentation
			.iter()
			.map(|(identity, _)| {
				state
					.code_editor
					.rune_view_panel_id(source_buffer, identity)
					.expect("view panel")
			})
			.collect::<Vec<_>>();
		assert!(panel_ids.windows(2).all(|ids| ids[0] < ids[1]));
		assert!(panel_ids[0].get() >= (1_u64 << 63));
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program.clone(),
				})
				.is_some()
		);

		let mut relations = program.into_relations().collect::<Vec<_>>();
		relations.reverse();
		for relation in relations {
			let frame = if relation.origin.contains("test1") {
				polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).expect("test1 frame")
			} else {
				polars::df!("cat" => ["a", "b"], "val" => [3_i64, 5]).expect("test2 frame")
			};
			complete_relation_in_head(&mut state, source_buffer, revision, relation, frame);
		}

		let first_table = state
			.code_editor
			.rune_view_panel_table(source_buffer, &LocalViewIdentity::Anonymous(1))
			.expect("first table panel");
		let second_table = state
			.code_editor
			.rune_view_panel_table(source_buffer, &LocalViewIdentity::Anonymous(3))
			.expect("second table panel");
		assert!(first_table.column("x").is_ok());
		assert!(first_table.column("cat").is_err());
		assert!(second_table.column("cat").is_ok());
		assert!(second_table.column("x").is_err());
		assert_ne!(panel_ids[1], panel_ids[3]);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(0))
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_display_label(source_buffer, &LocalViewIdentity::Anonymous(0)),
			Some("plot.line"),
			"the first dogfood view must reach the existing line renderer"
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(2)),
			"bar panel did not resolve: {}",
			state.status_error
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn execution_grouping_cannot_reorder_interleaved_host_panels() {
		let source = r#"
let a = workspace.table("a");
let b = workspace.table("b");
table(a);
plot.scatter(b, "x", "y");
plot.bar(a, "x", "y");
"#;
		let program = transform_rune_view(source).expect("interleaved origins");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		assert_eq!(
			presentation
				.iter()
				.map(|(_, form)| *form)
				.collect::<Vec<_>>(),
			vec![
				RuneViewForm::Table,
				RuneViewForm::Scatter,
				RuneViewForm::Bar
			]
		);

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(81);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		let ids = presentation
			.iter()
			.map(|(identity, _)| {
				state
					.code_editor
					.rune_view_panel_id(source_buffer, identity)
					.expect("interleaved panel")
			})
			.collect::<Vec<_>>();
		assert!(ids.windows(2).all(|pair| pair[0] < pair[1]));
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 1,
					view: program.clone(),
				})
				.is_some()
		);

		let mut relations = program.into_relations().collect::<Vec<_>>();
		let b = relations
			.iter()
			.position(|relation| relation.origin.contains("table(\"b\")"))
			.map(|index| relations.remove(index))
			.expect("b relation");
		complete_relation_in_head(
			&mut state,
			source_buffer,
			1,
			b,
			polars::df!("x" => [1_i64, 2], "y" => [3_i64, 4]).expect("b frame"),
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(1))
		);
		complete_relation_in_head(
			&mut state,
			source_buffer,
			1,
			relations.pop().expect("a relation"),
			polars::df!("x" => ["left", "right"], "y" => [7_i64, 8]).expect("a frame"),
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &LocalViewIdentity::Anonymous(0))
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &LocalViewIdentity::Anonymous(2)),
			"bar panel did not resolve: {}",
			state.status_error
		);
		assert_eq!(
			ids,
			(0..3)
				.map(|position| {
					state
						.code_editor
						.rune_view_panel_id(source_buffer, &LocalViewIdentity::Anonymous(position))
						.expect("stable presentation panel")
				})
				.collect::<Vec<_>>()
		);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn view_identity_reuses_retargets_or_orphans_one_panel_as_declared() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(82);
		let stable = LocalViewIdentity::Named("stable".to_owned());
		let anonymous = LocalViewIdentity::Anonymous(0);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(stable.clone(), RuneViewForm::Table)],
			1,
		);
		let stable_panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &stable)
			.expect("stable panel");
		state.code_editor.set_rune_view_table(
			stable_panel,
			polars::df!("x" => [1_i64], "y" => [2_i64]).expect("initial table"),
		);
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(stable.clone(), RuneViewForm::Scatter)],
			2,
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &stable),
			Some(stable_panel),
			"a named form change keeps the destination"
		);
		let changed = transform_rune_view(
			"workspace.table(\"t\").views([view(\"stable\", scatter(\"x\", \"y\"))])",
		)
		.expect("named scatter")
		.into_relations()
		.next()
		.expect("one relation");
		let changed_outcome = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).expect("plot frame"),
			changed.views,
		);
		let resolved = changed_outcome
			.sinks
			.into_iter()
			.find_map(|sink| match sink.output {
				Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _)) => {
					Some(resolved)
				}
				_ => None,
			})
			.expect("resolved scatter");
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		state.code_editor.set_rune_view_plot(
			stable_panel,
			"plot.scatter".to_owned(),
			kernel,
			crate::plot::theme::PlotSettingsLayer::default(),
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &stable)
		);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[
				(stable.clone(), RuneViewForm::Scatter),
				(anonymous.clone(), RuneViewForm::Table),
			],
			3,
		);
		let anonymous_panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &anonymous)
			.expect("anonymous panel");
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[
				(stable.clone(), RuneViewForm::Scatter),
				(anonymous.clone(), RuneViewForm::Bar),
			],
			4,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &anonymous),
			Some(anonymous_panel),
			"anonymous identity is its presentation slot"
		);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(anonymous.clone(), RuneViewForm::Bar)],
			5,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &stable),
			Some("orphaned")
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &stable),
			Some(stable_panel),
			"orphaning retains the destination"
		);
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(stable.clone(), RuneViewForm::Table)],
			6,
		);
		assert_eq!(
			state.code_editor.rune_view_panel_id(source_buffer, &stable),
			Some(stable_panel),
		);
		state.code_editor.set_rune_view_table(
			stable_panel,
			polars::df!("restored" => [true]).expect("restored table"),
		);

		let invalid =
			"workspace.table(\"t\").views([view(\"same\", table()), view(\"same\", table())])";
		assert_eq!(
			super::run_rune_code(&mut state, source_buffer, invalid.to_owned()).units(),
			1
		);
		assert_eq!(
			complete_rune_presentation(&mut state, source_buffer, invalid).units(),
			0
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &stable),
			Some("stale"),
			"an envelope failure is not deletion"
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &stable),
			"the diagnostic belongs to the toolbar, not over prior panel content"
		);
		assert!(state.status_error.starts_with("Rune · "));
		assert!(state.status_detail.is_some());

		let first_failure = LocalViewIdentity::Named("first-failure".to_owned());
		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[(first_failure.clone(), RuneViewForm::Table)],
			7,
		);
		let failed_panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &first_failure)
			.expect("first-failure panel");
		state.code_editor.fail_rune_view(failed_panel);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_failed(source_buffer, &first_failure)
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn failed_origin_keeps_prior_content_stale_while_its_sibling_refreshes() {
		let source = r#"
workspace.table("good").views([view("good", table())]);
workspace.table("bad").views([view("bad", table())]);
"#;
		let program = transform_rune_view(source).expect("isolated named origins");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		let good = LocalViewIdentity::Named("good".to_owned());
		let bad = LocalViewIdentity::Named("bad".to_owned());
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(83);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 1);
		for identity in [&good, &bad] {
			let panel = state
				.code_editor
				.rune_view_panel_id(source_buffer, identity)
				.expect("named panel");
			state.code_editor.set_rune_view_table(
				panel,
				polars::df!("previous" => [1_i64]).expect("previous content"),
			);
		}
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, 2);
		state.rune_buffer_revisions.insert(source_buffer, 2);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision: 2,
					view: program.clone(),
				})
				.is_some()
		);
		let mut relations = program.into_relations().collect::<Vec<_>>();
		let bad_relation = relations
			.iter()
			.position(|relation| relation.origin.contains("table(\"bad\")"))
			.map(|index| relations.remove(index))
			.expect("bad relation");
		drop(update(
			&mut state,
			Message::RunRuneViewSetResult {
				source_buffer,
				revision: 2,
				execution_index: 0,
				origin: bad_relation.origin,
				views: bad_relation
					.views
					.into_iter()
					.map(|view| view.identity)
					.collect(),
				result: Err("bad origin failed".to_owned()),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &bad),
			Some("stale")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &good),
			Some("running")
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_is_table(source_buffer, &bad),
			"stale preserves the prior frame"
		);

		complete_relation_in_head(
			&mut state,
			source_buffer,
			2,
			relations.pop().expect("good relation"),
			polars::df!("current" => [2_i64]).expect("fresh sibling content"),
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &good),
			Some("fresh")
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &bad),
			Some("stale")
		);
		assert!(
			state
				.code_editor
				.rune_view_panel_table(source_buffer, &good)
				.expect("fresh good table")
				.column("current")
				.is_ok()
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn fenced_sql_diagnostics_report_document_coordinates() {
		let raw = "SQLite parse error: sql parser error: Expected: column name or constraint definition, found: ) at Line: 4, Column: 1";
		let (summary, full) = compact_sql_diagnostic(raw, Some(6));

		assert_eq!(
			summary,
			"SQL · line 9, col 1 · expected column name or constraint definition, found ')'"
		);
		assert!(full.starts_with(&summary));
		assert!(full.contains(raw));
	}

	#[test]
	fn rune_diagnostics_compact_the_toolbar_and_keep_full_hover_detail() {
		let raw = "Rune view syntax at bytes 67..81: unknown view sink; expected scatter(...), bar(...), line(...), or table()";
		let (summary, full) = compact_rune_diagnostic(raw);

		assert_eq!(
			summary,
			"Rune · bytes 67..81 · unknown view sink; expected scatter(...), bar(...), line(...), or table()"
		);
		assert_eq!(full, raw);
	}

	#[test]
	fn product_0075_plain_plot_owns_anonymous_zero_and_orphans_when_removed() {
		use crate::gui::messages::RuneBufferOutcome;
		use crate::plot::composition::PlotComposition;
		use crate::plot::core::PlotType;
		use crate::plot::factory::create_plot;
		use crate::plot::request::{PlotRequest, PlotSource, plan};
		use crate::plot::spec::AuthoredPlotSpec;
		use crate::rune_view::{LocalViewIdentity, ViewContentKind, ViewPresentation};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(7500);
		let identity = LocalViewIdentity::Anonymous(0);
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "plain".to_owned(),
				data: crate::result_artifact::ResultArtifact::test_frame(
					polars::df!("x" => [1.0_f64, 2.0], "y" => [3.0_f64, 5.0]).expect("plain frame"),
				),
			},
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
			),
		})
		.expect("plain plot resolves");
		let ready = resolved.clone();
		let display_label = ready.resolved_spec.composition.title();
		let kernel = create_plot(&ready.resolved_spec, &ready.data, 1200, 720);

		state.rune_buffer_revisions.insert(source_buffer, 1);
		assert_eq!(
			update(
				&mut state,
				Message::RunRuneBufferResult {
					source_buffer,
					revision: 1,
					result: Ok(RuneBufferOutcome {
						panes: None,
						display: None,
						plot: Some(resolved),
					}),
				},
			)
			.units(),
			1,
			"one plain plot schedules one kernel build"
		);
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.expect("plain Anonymous(0) panel");
		assert_ne!(
			panel_id.get(),
			2,
			"plain output never reuses the retired role"
		);
		assert_eq!(state.code_editor.rune_view_panel_count(source_buffer), 1);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &identity),
			Some("running")
		);

		drop(update(
			&mut state,
			Message::RuneBufferPlotReady {
				source_buffer,
				revision: 1,
				panel_id,
				display_label,
				resolved: ready,
				kernel,
			},
		));
		assert!(
			state
				.code_editor
				.rune_view_panel_is_plot(source_buffer, &identity)
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &identity),
			Some("fresh")
		);

		state.rune_buffer_revisions.insert(source_buffer, 2);
		drop(update(
			&mut state,
			Message::RunRuneBufferResult {
				source_buffer,
				revision: 2,
				result: Ok(RuneBufferOutcome {
					panes: None,
					display: Some("plain value".to_owned()),
					plot: None,
				}),
			},
		));
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_status(source_buffer, &identity),
			Some("orphaned"),
			"removing the sole plot reconciles an empty view set"
		);

		state.code_editor.prepare_rune_view_panels(
			source_buffer,
			&[
				(
					identity.clone(),
					ViewPresentation {
						content_kind: ViewContentKind::Plot,
						display_label: "plot.scatter".to_owned(),
						authored_title: crate::plot::spec::Presence::Inherit,
					},
				),
				(
					LocalViewIdentity::Named("named".to_owned()),
					ViewPresentation {
						content_kind: ViewContentKind::Plot,
						display_label: "plot.scatter".to_owned(),
						authored_title: crate::plot::spec::Presence::Inherit,
					},
				),
			],
			3,
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity),
			Some(panel_id),
			"a relation's first anonymous sink reuses the plain output identity"
		);
		assert_ne!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &LocalViewIdentity::Named("named".to_owned())),
			Some(panel_id),
			"a named sink remains a distinct semantic identity"
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn product_0075_product_0149_markdown_plot_exports_exact_raster_data() {
		use crate::gui::messages::ExportFormat;
		use crate::plot::composition::PlotComposition;
		use crate::plot::core::PlotType;
		use crate::plot::factory::create_plot;
		use crate::plot::request::{PlotRequest, PlotSource, plan};
		use crate::plot::spec::AuthoredPlotSpec;
		use libaryon::uast::projection::{
			MarkdownCellCodeVisibility, MarkdownCellIdentity, MarkdownCellOutputTarget,
			MarkdownCellPresentation,
		};
		use std::sync::Arc;

		fn fixture() -> (
			crate::plot::request::ResolvedPlot,
			Arc<dyn crate::plot::common::PlotKernel + Send + Sync>,
		) {
			let resolved = plan(PlotRequest {
				source: PlotSource::Materialized {
					name: "markdown".to_owned(),
					data: crate::result_artifact::ResultArtifact::test_frame(
						polars::df!("x" => [1.0_f64, 2.0], "y" => [2.0_f64, 3.0])
							.expect("markdown frame"),
					),
				},
				authored_spec: AuthoredPlotSpec::xy(
					PlotComposition::single(PlotType::Scatter),
					"x",
					"y",
				),
			})
			.expect("markdown plot resolves");
			let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 720, 720);
			(resolved, kernel)
		}

		let identity = MarkdownCellIdentity::Explicit {
			id: "chart".to_owned(),
		};
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let (resolved, kernel) = fixture();
		drop(update(
			&mut state,
			Message::NotebookPlotReady {
				identity: identity.clone(),
				presentation: MarkdownCellPresentation {
					output: MarkdownCellOutputTarget::Pane,
					code: MarkdownCellCodeVisibility::Show,
				},
				resolved,
				kernel,
				receipts: vec![ExportReceipt {
					path: "markdown.csv".to_owned(),
					rows: 2,
				}],
			},
		));
		assert_eq!(
			state.status_msg,
			"Plot cell finished. Exported 2 rows to markdown.csv."
		);
		let deadline = std::time::Instant::now() + std::time::Duration::from_secs(10);
		while state.code_editor.host_plot_exports().is_empty()
			&& std::time::Instant::now() < deadline
		{
			state.code_editor.poll();
			std::thread::sleep(std::time::Duration::from_millis(5));
		}
		assert_eq!(
			state.code_editor.host_plot_exports().len(),
			1,
			"pane-targeted Markdown plot occupies the shared visible Results panel"
		);
		let export_root = std::env::temp_dir().join(format!(
			"polariton-product-0075-export-{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&export_root);
		std::fs::create_dir(&export_root).expect("create export fixture directory");
		for (ordinal, format, extension) in [
			(1, ExportFormat::SVG, "svg"),
			(2, ExportFormat::PNG, "png"),
			(3, ExportFormat::AVIF, "avif"),
		] {
			let baseline_ordinal = ordinal + 10;
			let mut baseline = state.code_editor.host_plot_exports().remove(0);
			baseline.width = 160.0;
			baseline.height = 100.0;
			baseline.settings_model.library_defaults.scatter_render_mode =
				crate::plot::common::ScatterRenderMode::Rasterized;
			super::render_plot_export(baseline, format, baseline_ordinal)
				.expect("baseline export")
				.write_to(&export_root);
			let baseline_bytes =
				std::fs::read(export_root.join(format!("plot_{baseline_ordinal}.{extension}")))
					.expect("baseline export artifact");

			let mut snapshot = state.code_editor.host_plot_exports().remove(0);
			snapshot.width = 160.0;
			snapshot.height = 100.0;
			snapshot.settings_model.pane_override.domains =
				crate::plot::spec::Override::Set(crate::plot::theme::PlotDomainOverrides {
					x_min: Some(1.25),
					x_max: Some(1.75),
					y_min: Some(2.25),
					y_max: Some(2.75),
				});
			let viewport = snapshot.settings_model.resolve();
			assert_eq!(
				(
					viewport.x_min,
					viewport.x_max,
					viewport.y_min,
					viewport.y_max
				),
				(Some(1.25), Some(1.75), Some(2.25), Some(2.75)),
				"PRODUCT-0150 exports consume the on-screen pane viewport in every format"
			);
			snapshot.settings_model.library_defaults.scatter_render_mode =
				crate::plot::common::ScatterRenderMode::Rasterized;
			super::render_plot_export(snapshot, format, ordinal)
				.expect("viewport export")
				.write_to(&export_root);
			let path = export_root.join(format!("plot_{ordinal}.{extension}"));
			let bytes = std::fs::read(&path).expect("rendered export artifact");
			assert!(bytes.len() > 32, "{format} export is non-empty");
			assert_ne!(
				bytes, baseline_bytes,
				"{format} rendered bytes must reflect the pane viewport, not only carry it in the snapshot"
			);
			match format {
				ExportFormat::SVG => {
					let svg = String::from_utf8(bytes).unwrap();
					assert!(svg.contains("<svg"), "SVG export carries the plot document");
					assert!(
						svg.contains("data:image/png;base64"),
						"exact raster export contains data pixels, never a preparing marker"
					);
					assert!(!svg.contains("preparing raster"));
				}
				ExportFormat::PNG => assert_eq!(&bytes[..8], b"\x89PNG\r\n\x1a\n"),
				ExportFormat::AVIF => assert!(
					bytes.windows(8).any(|window| window == b"ftypavif"),
					"AVIF export carries the expected container brand"
				),
			}
		}
		std::fs::remove_dir_all(&export_root).expect("remove export fixture directory");
		state.code_editor.shutdown();

		for output in [
			MarkdownCellOutputTarget::Inline,
			MarkdownCellOutputTarget::None,
		] {
			let (mut control, startup) = new(StartupData::default(), None);
			drop(startup);
			let (resolved, kernel) = fixture();
			drop(update(
				&mut control,
				Message::NotebookPlotReady {
					identity: identity.clone(),
					presentation: MarkdownCellPresentation {
						output,
						code: MarkdownCellCodeVisibility::Show,
					},
					resolved,
					kernel,
					receipts: Vec::new(),
				},
			));
			assert!(
				!control.code_editor.has_query_surface(),
				"{output:?} must not manufacture the shared Results panel"
			);
			assert_eq!(
				control.code_editor.markdown_cell_is_plot(&identity),
				output == MarkdownCellOutputTarget::Inline,
				"inline retains native content while none retains no output"
			);
			control.code_editor.shutdown();
		}
	}

	#[cfg(feature = "toron")]
	#[test]
	fn rune_execution_error_uses_toolbar_without_manufacturing_a_result_pane() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = libaryon::core::BufferId::new(97);
		state.rune_buffer_revisions.insert(source_buffer, 1);
		let raw = "execution failed at row 2";

		let _ = update(
			&mut state,
			Message::RunRuneBufferResult {
				source_buffer,
				revision: 1,
				result: Err(raw.to_owned()),
			},
		);

		assert_eq!(state.status_error, "Rune · execution failed at row 2");
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.full.as_str()),
			Some(raw)
		);
		assert!(!state.code_editor.has_query_surface());
		state.code_editor.shutdown();
	}

	#[test]
	fn product_0151_pending_export_rebuilds_the_dispatch_revision_exactly() {
		use crate::gui::messages::ExportFormat;
		use crate::plot::composition::PlotComposition;
		use crate::plot::core::PlotType;
		use crate::plot::factory::create_plot_from_artifact;
		use crate::plot::request::{PlotRequest, PlotSource, plan};
		use crate::plot::spec::{AuthoredPlotSpec, SemanticType};
		use crate::selection::{
			ConstraintForm, ExactScalar, FieldConstraint, FieldRef, InputIdentity, Predicate,
			ScalarType, SelectionRevision,
		};
		use std::collections::HashSet;
		use std::sync::Arc;

		let artifact = crate::result_artifact::ResultArtifact::from_polars(Arc::new(
			polars::df!("x" => [1_i64, 2], "y" => [2_i64, 3]).expect("export frame"),
		));
		artifact.bind_input_identity(InputIdentity::mint());
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: "selection export".into(),
				data: Arc::clone(&artifact),
			},
			authored_spec: AuthoredPlotSpec::xy(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
			),
		})
		.expect("resolved export");
		let recipe = crate::plot::recipe::PlotRecipe::single(
			Arc::clone(&artifact),
			resolved.resolved_spec.clone(),
			320,
			220,
		);
		let unfiltered =
			create_plot_from_artifact(&resolved.resolved_spec, &resolved.data, &artifact, 320, 220);
		let predicate = |value| Predicate {
			fields: vec![FieldConstraint {
				field: FieldRef::new("x", SemanticType::Quantitative, ScalarType::Int),
				form: ConstraintForm::Values(HashSet::from([ExactScalar::Int(value)])),
			}],
		};
		let revision = SelectionRevision::ZERO.next();
		let snapshot =
			|kernel, predicate, displayed_selection_revision| crate::editor::HostPlotExport {
				status: "fresh",
				title: "selection export".into(),
				kernel,
				settings_model: crate::plot::theme::PlotSettings::default(),
				render_revision: 0,
				resize_render_suspended: false,
				width: 320.0,
				height: 220.0,
				recipe: Some(recipe.clone()),
				predicate,
				selection_revision: revision,
				displayed_selection_revision,
				layout: crate::publication::GridRect {
					col: 0,
					row: 0,
					width: 1,
					height: 1,
				},
				view_name: None,
				plot_type: None,
				publication: None,
				authored_expression: None,
				document: None,
			};
		let svg = |snapshot| match super::render_plot_export(snapshot, ExportFormat::SVG, 1)
			.expect("selection export")
			.artifact
		{
			super::RenderedPlotExport::Svg(svg) => svg.svg,
			_ => unreachable!("SVG requested"),
		};

		let dispatched = predicate(1);
		let pending = svg(snapshot(
			Arc::clone(&unfiltered),
			dispatched.clone(),
			SelectionRevision::ZERO,
		));
		let settled_kernel = recipe.rebuild(&dispatched).expect("settled revision");
		let settled = svg(snapshot(settled_kernel, dispatched, revision));
		assert_eq!(pending, settled);
		assert!(!pending.contains("preparing raster"));

		let later_kernel = recipe.rebuild(&predicate(2)).expect("later revision");
		let later = svg(snapshot(later_kernel, predicate(2), revision));
		assert_ne!(
			pending, later,
			"a later live selection cannot change the dispatch-time export snapshot"
		);

		let invalid = Predicate {
			fields: vec![FieldConstraint {
				field: FieldRef::new("missing", SemanticType::Quantitative, ScalarType::Int),
				form: ConstraintForm::Values(HashSet::from([ExactScalar::Int(1)])),
			}],
		};
		let refusal = super::render_plot_export(
			snapshot(Arc::clone(&unfiltered), invalid, SelectionRevision::ZERO),
			ExportFormat::SVG,
			1,
		)
		.err()
		.expect("a failed exact rebuild refuses export");
		assert!(refusal.starts_with("Export · refused:"));
		assert!(refusal.contains("missing"));
	}

	#[test]
	fn rune_configuration_failure_is_projected_without_blocking_startup() {
		let startup_data = StartupData {
			init_error: Some("init.rn compile error: broken fixture".to_owned()),
			..StartupData::default()
		};
		let (mut state, startup) = new(startup_data, None);
		drop(startup);

		assert_eq!(state.status_error, "init.rn · configuration failed");
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.full.as_str()),
			Some("init.rn compile error: broken fixture"),
		);
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0030_rune_default_authored_palette_and_unused_settings_are_exact() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1030);
		let categories = ["A", "B", "C", "D", "E", "F"];

		let default = run_one_relation_program(
			&mut state,
			source_buffer,
			1,
			"workspace.table(\"t\").views([view(\"plot\", bar(\"cat\", \"val\"))])",
			categorical_bar_frame(&categories),
		);
		assert!(
			default[0]
				.1
				.resolved_spec
				.channel(crate::plot::spec::Channel::Color)
				.is_none()
		);
		assert_eq!(
			band_mapping(&default[0].1),
			[
				("A".to_owned(), 0, (68, 1, 83)),
				("B".to_owned(), 4, (60, 69, 130)),
				("C".to_owned(), 8, (41, 125, 140)),
				("D".to_owned(), 11, (52, 162, 127)),
				("E".to_owned(), 15, (119, 205, 88)),
				("F".to_owned(), 19, (253, 231, 36)),
			]
		);

		let tab20c = run_one_relation_program(
			&mut state,
			source_buffer,
			2,
			r#"workspace.table("t").views([view("plot", figure([
				title("Categories"), palette("tab20c"), bar("cat", "val")
			]))])"#,
			categorical_bar_frame(&categories),
		);
		assert_eq!(
			band_mapping(&tab20c[0].1),
			[
				("A".to_owned(), 0, (49, 130, 189)),
				("B".to_owned(), 4, (230, 85, 13)),
				("C".to_owned(), 8, (49, 163, 84)),
				("D".to_owned(), 11, (199, 233, 192)),
				("E".to_owned(), 15, (218, 218, 235)),
				("F".to_owned(), 19, (150, 150, 150)),
			]
		);
		let panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &LocalViewIdentity::Named("plot".to_owned()))
			.expect("stable palette panel");

		let yellows = run_one_relation_program(
			&mut state,
			source_buffer,
			3,
			r#"workspace.table("t").views([view("plot", figure([
				palette("yellows"), bar("cat", "val")
			]))])"#,
			categorical_bar_frame(&categories),
		);
		assert_eq!(
			band_mapping(&yellows[0].1),
			[
				("A".to_owned(), 0, (252, 252, 204)),
				("B".to_owned(), 4, (252, 237, 139)),
				("C".to_owned(), 8, (252, 222, 75)),
				("D".to_owned(), 11, (236, 198, 42)),
				("E".to_owned(), 15, (194, 150, 21)),
				("F".to_owned(), 19, (153, 102, 0)),
			],
			"a weak authored choice reaches the artifact without aesthetic admission"
		);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &LocalViewIdentity::Named("plot".to_owned())),
			Some(panel)
		);

		for (revision, body) in [(4, "line(\"x\", \"y\")"), (5, "scatter(\"x\", \"y\")")] {
			let source = format!(
				"workspace.table(\"t\").views([view(\"plot\", figure([palette(\"tab20c\"), {body}]))])"
			);
			let plots = run_one_relation_program(
				&mut state,
				source_buffer,
				revision,
				&source,
				polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap(),
			);
			assert!(plots[0].1.resolved_spec.band_fills.is_empty());
			assert!(
				plots[0]
					.1
					.resolved_spec
					.channel(crate::plot::spec::Channel::Color)
					.is_none()
			);
		}

		let scatter = run_one_relation_program(
			&mut state,
			source_buffer,
			6,
			r#"workspace.table("t").views([view("plot", figure([
				palette("tab20c"), scatter("x", "y", "cat")
			]))])"#,
			polars::df!(
				"x" => [1_i64, 2, 3, 4, 5, 6],
				"y" => [6_i64, 5, 4, 3, 2, 1],
				"cat" => categories,
			)
			.unwrap(),
		);
		assert!(scatter[0].1.resolved_spec.band_fills.is_empty());
		assert_eq!(
			scatter[0]
				.1
				.resolved_spec
				.channel(crate::plot::spec::Channel::Color)
				.expect("authored categorical color channel")
				.colors
				.iter()
				.map(|assignment| (
					assignment.category.as_str(),
					assignment.position.index(),
					(
						assignment.color.red,
						assignment.color.green,
						assignment.color.blue,
					),
				))
				.collect::<Vec<_>>(),
			[
				("A", 0, (49, 130, 189)),
				("B", 4, (230, 85, 13)),
				("C", 8, (49, 163, 84)),
				("D", 11, (199, 233, 192)),
				("E", 15, (218, 218, 235)),
				("F", 19, (150, 150, 150)),
			]
		);

		let layered = run_one_relation_program(
			&mut state,
			source_buffer,
			7,
			r#"workspace.table("t").views([view("plot", figure([
				palette("tab20c"), layers([line("x", "y"), scatter("x", "y")])
			]))])"#,
			polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap(),
		);
		assert!(
			layered.is_empty(),
			"layered content has no categorical proposal"
		);
		assert!(
			state.code_editor.rune_view_panel_is_plot(
				source_buffer,
				&LocalViewIdentity::Named("plot".to_owned())
			),
			"an unused valid palette cannot block layered completion"
		);

		for (palette, expected) in [("not-a-theme", "not-a-theme"), ("nord", "categorical")] {
			let source = format!(
				"workspace.table(\"t\").views([figure([palette(\"{palette}\"), line(\"x\", \"y\")])])"
			);
			let relation = transform_rune_view(&source)
				.expect("selection failure is below the envelope")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!("x" => [1_i64], "y" => [2_i64]).unwrap(),
				relation.views,
			);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("structurally impossible selection must refuse")
			};
			assert!(error.contains(expected), "unhelpful error: {error}");
		}

		for (palette, cardinality, palette_limit) in
			[("dark2", 9, 8), ("tab20c", 13, 20), ("dark2", 13, 8)]
		{
			let source = format!(
				"workspace.table(\"t\").views([figure([palette(\"{palette}\"), bar(\"cat\", \"val\")])])"
			);
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let categories = (0..cardinality)
				.map(|index| format!("c{index}"))
				.collect::<Vec<_>>();
			let category_refs = categories.iter().map(String::as_str).collect::<Vec<_>>();
			let outcome = rune_view_outcome(categorical_bar_frame(&category_refs), relation.views);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("capacity failure cannot fall back or truncate")
			};
			assert!(error.contains(&format!("requests {cardinality} identities")));
			assert!(error.contains("geometry limit 12"));
			assert!(error.contains(&format!("palette `{palette}` limit {palette_limit}")));
		}
		state.code_editor.shutdown();
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0036_numeric_bounds_apply_per_axis_and_discrete_bounds_never_disappear() {
		let y_source = "workspace.table(\"t\").views([figure([y_axis().bounds(-1.0, 1.0), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(y_source)
			.expect("numeric y bound")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B", "C"]), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("numeric bar y bound must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(settings.x_min, None);
		assert_eq!(settings.x_max, None);
		assert_eq!((settings.y_min, settings.y_max), (Some(-1.0), Some(1.0)));
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::CategoricalX { y_range, .. } if y_range == (-1.0, 1.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		let x_source = "workspace.table(\"t\").views([figure([x_axis().bounds(0.0, 1.0), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(x_source)
			.expect("structurally valid x bound")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical x bound must refuse");
		};
		assert!(error.contains("requires a numeric x axis"), "{error}");
		assert!(error.contains("Band"), "{error}");
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&x_source[span.start as usize..span.end as usize],
			".bounds(0.0, 1.0)"
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0055_horizontal_bar_keeps_physical_axes_and_family_label_through_completion() {
		let source = r#"workspace.table("t").views([figure([
			x_axis().bounds(0, 10), x_axis().tick_step(1), x_axis().minor_ticks(2),
			bar("value", "category", "series")
		])])"#;
		let relation = transform_rune_view(source)
			.expect("horizontal bar remains the bar surface")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!(
				"category" => ["East", "East", "West", "West"],
				"value" => [2.0, 4.0, 6.0, 8.0],
				"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			)
			.unwrap(),
			relation.views,
		);
		let sink = &outcome.sinks[0];
		assert_eq!(
			sink.presentation.display_label, "plot.bar",
			"label names authored family"
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&sink.output
		else {
			panic!("pair-resolved horizontal bar must complete");
		};
		assert_eq!(
			resolved.resolved_spec.composition.primary_plot_type(),
			PlotType::HorizontalBar
		);
		assert_eq!(
			resolved.resolved_spec.composition.title(),
			"Horizontal Bar Plot",
			"composition names resolved geometry"
		);
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!((settings.x_min, settings.x_max), (Some(0.0), Some(10.0)));
		assert_eq!(
			settings.x_tick_placement,
			crate::plot::common::TickPlacement::step(1.0).unwrap()
		);
		assert_eq!(settings.x_minor_ticks, 2);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::CategoricalY { x_range, .. } if x_range == (0.0, 10.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		for modifier in [".bounds(0, 10)", ".tick_step(1)", ".minor_ticks(2)"] {
			let refusing = format!(
				"workspace.table(\"t\").views([figure([y_axis(){modifier}, bar(\"value\", \"category\")])])"
			);
			let relation = transform_rune_view(&refusing)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!("category" => ["East"], "value" => [2.0]).unwrap(),
				relation.views,
			);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("categorical y modifier must refuse: {modifier}");
			};
			assert!(error.contains("Band"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&refusing[span.start as usize..span.end as usize], modifier);
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0064_stacked_members_keep_family_label_and_physical_axis_rules() {
		let frame = polars::df!(
			"category" => ["East", "East", "West", "West"],
			"value" => [2.0, 4.0, 6.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();
		for (source, expected, title, expected_layout) in [
			(
				r#"workspace.table("t").views([figure([
					y_axis().bounds(0, 20), y_axis().tick_step(2), y_axis().minor_ticks(2),
					bar("category", "value", "series").stacked()
				])])"#,
				PlotType::StackedBar,
				"Stacked Bar Plot",
				"vertical",
			),
			(
				r#"workspace.table("t").views([figure([
					x_axis().bounds(0, 20), x_axis().tick_step(2), x_axis().minor_ticks(2),
					bar("value", "category", "series").stacked()
				])])"#,
				PlotType::HorizontalStackedBar,
				"Horizontal Stacked Bar Plot",
				"horizontal",
			),
		] {
			let relation = transform_rune_view(source)
				.expect("stacked bar stays inside the authored Bar family")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let sink = &outcome.sinks[0];
			assert_eq!(
				sink.presentation.display_label, "plot.bar",
				"the label names authored intent and must survive stacking"
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
				resolved,
				layer,
				authored,
				_,
			)) = &sink.output
			else {
				panic!("stacked bar must complete through the standalone styled path")
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				expected
			);
			assert_eq!(
				resolved.resolved_spec.composition.title(),
				title,
				"composition names resolved geometry and must differ from the family label"
			);
			let settings = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			};
			let resolved_settings = settings.resolve_for_layer(layer);
			let kernel = create_styled_plot(&resolved.resolved_spec, &resolved.data, layer.clone())
				.expect("canonical stacked member reaches the styled factory");
			match (expected_layout, kernel.layout(resolved_settings.clone())) {
				("vertical", PlotLayout::CategoricalX { y_range, .. }) => {
					assert_eq!(y_range, (0.0, 20.0));
					assert_eq!(
						resolved_settings.y_tick_placement,
						crate::plot::common::TickPlacement::step(2.0).unwrap()
					);
					assert_eq!(resolved_settings.y_minor_ticks, 2);
				}
				("horizontal", PlotLayout::CategoricalY { x_range, .. }) => {
					assert_eq!(x_range, (0.0, 20.0));
					assert_eq!(
						resolved_settings.x_tick_placement,
						crate::plot::common::TickPlacement::step(2.0).unwrap()
					);
					assert_eq!(resolved_settings.x_minor_ticks, 2);
				}
				_ => panic!("wrong stacked physical layout"),
			}
			assert!(
				render_marks_with_settings(kernel.as_ref(), resolved_settings).contains("<svg")
			);
		}

		for (modifier, bar) in [
			(
				"x_axis().bounds(0, 20)",
				"bar(\"category\", \"value\").stacked()",
			),
			(
				"y_axis().bounds(0, 20)",
				"bar(\"value\", \"category\").stacked()",
			),
		] {
			let source = format!("workspace.table(\"t\").views([figure([{modifier}, {bar}])])");
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("category-axis stacked setting must refuse")
			};
			assert!(error.contains("Band"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				modifier
					.strip_prefix("x_axis()")
					.or_else(|| modifier.strip_prefix("y_axis()"))
					.unwrap()
			);
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0072_percent_completion_fixes_value_domain_and_refuses_bounds() {
		let frame = polars::df!(
			"category" => ["East", "East", "West", "West"],
			"value" => [30.0, 90.0, 20.0, 20.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
			"x" => [0.0, 0.0, 1.0, 1.0],
		)
		.unwrap();
		for (source, expected, axis) in [
			(
				r#"workspace.table("t").views([figure([
					y_axis().tick_step(25), bar("category", "value", "series").stacked().percent()
				])])"#,
				PlotType::StackedBar,
				"y",
			),
			(
				r#"workspace.table("t").views([figure([
					x_axis().tick_step(25), bar("value", "category", "series").percent().stacked()
				])])"#,
				PlotType::HorizontalStackedBar,
				"x",
			),
			(
				r#"workspace.table("t").views([figure([
					y_axis().tick_step(25), area("x", "value", "series").percent()
				])])"#,
				PlotType::StackedArea,
				"y",
			),
		] {
			let relation = transform_rune_view(source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(
				resolved,
				layer,
				authored,
				_,
			)) = &outcome.sinks[0].output
			else {
				panic!("percent mark must complete through the styled path")
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				expected
			);
			assert_eq!(
				resolved.resolved_spec.stack_normalization,
				crate::plot::spec::StackNormalization::Percent
			);
			let model = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			};
			let settings = model.resolve_for_layer(layer);
			let kernel = create_styled_plot(&resolved.resolved_spec, &resolved.data, layer.clone())
				.expect("both percent families reach the styled factory");
			match (axis, kernel.layout(settings.clone())) {
				(
					"y",
					PlotLayout::CategoricalX {
						y_range,
						y_axis_type,
						..
					},
				) => {
					assert_eq!(y_range, (0.0, 100.0));
					assert_eq!(y_axis_type, crate::plot::common::AxisType::Percent);
				}
				(
					"x",
					PlotLayout::CategoricalY {
						x_range,
						x_axis_type,
						..
					},
				) => {
					assert_eq!(x_range, (0.0, 100.0));
					assert_eq!(x_axis_type, crate::plot::common::AxisType::Percent);
				}
				(
					"y",
					PlotLayout::Cartesian {
						y_range,
						y_axis_type,
						..
					},
				) => {
					assert_eq!(y_range, (0.0, 100.0));
					assert_eq!(y_axis_type, crate::plot::common::AxisType::Percent);
				}
				_ => panic!("wrong percent layout"),
			}
		}

		for (modifier, mark) in [
			(
				"y_axis().bounds(0, 100)",
				"bar(\"category\", \"value\", \"series\").stacked().percent()",
			),
			(
				"x_axis().bounds(0, 100)",
				"bar(\"value\", \"category\", \"series\").stacked().percent()",
			),
			(
				"y_axis().bounds(0, 100)",
				"area(\"x\", \"value\", \"series\").percent()",
			),
		] {
			let source = format!("workspace.table(\"t\").views([figure([{modifier}, {mark}])])");
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(frame.clone(), relation.views);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("percent value bounds must refuse")
			};
			assert!(error.contains("fixed 0..100 percent domain"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				modifier
					.strip_prefix("x_axis()")
					.or_else(|| modifier.strip_prefix("y_axis()"))
					.unwrap()
			);
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0070_area_reaches_completion_with_cartesian_guides_and_fixed_orientation() {
		let source = r#"workspace.table("t").views([figure([
			y_axis().bounds(0, 20), y_axis().tick_step(2), y_axis().minor_ticks(2),
			area("x", "value", "series")
		])])"#;
		let relation = transform_rune_view(source)
			.expect("area surface envelope")
			.into_relations()
			.next()
			.unwrap();
		let frame = polars::df!(
			"x" => [1.0_f64, 1.0, 2.0, 2.0],
			"value" => [2.0_f64, 4.0, 6.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();
		let outcome = rune_view_outcome(frame, relation.views);
		let sink = &outcome.sinks[0];
		assert_eq!(sink.presentation.display_label, "plot.area");
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&sink.output
		else {
			panic!("area must complete through the standalone plot path")
		};
		assert_eq!(
			resolved.resolved_spec.composition.primary_plot_type(),
			PlotType::StackedArea
		);
		assert_eq!(
			resolved.resolved_spec.composition.title(),
			"Stacked Area Plot"
		);
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!((settings.y_min, settings.y_max), (Some(0.0), Some(20.0)));
		assert_eq!(
			settings.y_tick_placement,
			crate::plot::common::TickPlacement::step(2.0).unwrap()
		);
		assert_eq!(settings.y_minor_ticks, 2);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::Cartesian { y_range, .. } if y_range == (0.0, 20.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		let reversed = "workspace.table(\"t\").views([area(\"category\", \"value\")])";
		let relation = transform_rune_view(reversed)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("category" => ["East"], "value" => [2.0_f64]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical area x must refuse rather than rotate")
		};
		assert!(
			error.contains("category") && error.contains("incompatible"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&reversed[span.start as usize..span.end as usize],
			"area(\"category\", \"value\")",
			"the area sink span carries the resolved-field refusal"
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0055_pair_refusal_is_attributed_to_the_complete_bar_sink() {
		let source = "workspace.table(\"t\").views([bar(\"revenue\", \"profit\")])";
		let relation = transform_rune_view(source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("revenue" => [1.0], "profit" => [2.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("two quantitative bar axes must refuse as a pair");
		};
		assert!(error.contains(
			"bar requires exactly one quantitative value axis and one non-quantitative category axis; x `revenue` resolved as Quantitative, y `profit` resolved as Quantitative"
		));
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"bar(\"revenue\", \"profit\")"
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0058_bound_fields_resolve_before_bar_pair_and_channel_resolution() {
		let bound_source = r#"
let x_field = "value";
let y_field = "category";
let series_field = "series";
workspace.table("t").views([bar(x_field, y_field, series_field)])
"#;
		let inline_source =
			"workspace.table(\"t\").views([bar(\"value\", \"category\", \"series\")])";
		let frame = polars::df!(
			"category" => ["East", "East", "West", "West"],
			"value" => [2.0, 4.0, 6.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();

		let complete = |source: &str| {
			let relation = transform_rune_view(source)
				.expect("static fields lower before dispatch")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame.clone(), relation.views)
		};
		let bound = complete(bound_source);
		let inline = complete(inline_source);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(bound_plot, _, _)) =
			&bound.sinks[0].output
		else {
			panic!("bound horizontal bar must complete")
		};
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(inline_plot, _, _)) =
			&inline.sinks[0].output
		else {
			panic!("inline horizontal bar must complete")
		};

		use crate::plot::spec::{AuthoredChannel, Channel, OptionalAuthoredChannel};
		assert_eq!(
			bound_plot.authored_spec.x.field,
			AuthoredChannel::Named("value".to_owned())
		);
		assert_eq!(
			bound_plot.authored_spec.y.field,
			AuthoredChannel::Named("category".to_owned())
		);
		assert_eq!(
			bound_plot.authored_spec.color.field,
			OptionalAuthoredChannel::Named("series".to_owned())
		);
		assert_eq!(
			bound_plot.resolved_spec.composition.primary_plot_type(),
			PlotType::HorizontalBar
		);
		assert_eq!(
			bound_plot
				.resolved_spec
				.channel(Channel::X)
				.unwrap()
				.binding
				.field,
			"value"
		);
		assert_eq!(
			bound_plot
				.resolved_spec
				.channel(Channel::Y)
				.unwrap()
				.binding
				.field,
			"category"
		);
		assert_eq!(
			bound_plot
				.resolved_spec
				.channel(Channel::Color)
				.unwrap()
				.binding
				.field,
			"series"
		);
		assert_eq!(
			bound_plot.resolved_spec, inline_plot.resolved_spec,
			"binding identifiers must not enter the authored or resolved plot"
		);

		let invalid_source = r#"
let x_field = "revenue";
let y_field = "profit";
workspace.table("t").views([bar(x_field, y_field)])
"#;
		let relation = transform_rune_view(invalid_source)
			.expect("both bindings resolve statically")
			.into_relations()
			.next()
			.unwrap();
		let invalid = rune_view_outcome(
			polars::df!("revenue" => [1.0], "profit" => [2.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &invalid.sinks[0].output else {
			panic!("two quantitative bound fields must refuse as a pair")
		};
		assert!(
			error.contains(
				"x `revenue` resolved as Quantitative, y `profit` resolved as Quantitative"
			),
			"{error}"
		);
		let span = invalid.sinks[0].span.clone();
		assert_eq!(
			&invalid_source[span.start as usize..span.end as usize],
			"bar(x_field, y_field)"
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0058_bound_third_fields_share_inline_color_resolution_for_every_mark() {
		let frame = polars::df!(
			"cat" => ["East", "East", "West", "West"],
			"value" => [2.0, 4.0, 6.0, 8.0],
			"x" => [1.0, 2.0, 1.0, 2.0],
			"y" => [3.0, 4.0, 7.0, 8.0],
			"series" => ["Atlas", "Beacon", "Atlas", "Beacon"],
		)
		.unwrap();
		for (mark, x, y) in [
			("bar", "cat", "value"),
			("line", "x", "y"),
			("scatter", "x", "y"),
		] {
			let inline_source =
				format!("workspace.table(\"t\").views([{mark}(\"{x}\", \"{y}\", \"series\")])");
			let bound_source = format!(
				r#"
let first_field = "{x}";
let second_field = "{y}";
let color_field = "series";
workspace.table("t").views([{mark}(first_field, second_field, color_field)])
"#
			);
			let complete = |source: &str| {
				let relation = transform_rune_view(source)
					.expect("bound categorical field")
					.into_relations()
					.next()
					.unwrap();
				rune_view_outcome(frame.clone(), relation.views)
			};
			let inline = complete(&inline_source);
			let bound = complete(&bound_source);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(inline_plot, _, _)) =
				&inline.sinks[0].output
			else {
				panic!("inline {mark} must complete")
			};
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(bound_plot, _, _)) =
				&bound.sinks[0].output
			else {
				panic!("bound {mark} must complete")
			};

			use crate::plot::spec::{Channel, OptionalAuthoredChannel};
			assert_eq!(
				bound_plot.authored_spec.color.field,
				OptionalAuthoredChannel::Named("series".to_owned())
			);
			let bound_color = bound_plot
				.resolved_spec
				.channel(Channel::Color)
				.expect("bound third field becomes a resolved color channel");
			assert_eq!(bound_color.binding.field, "series");
			assert_eq!(bound_color.colors.len(), 2);
			assert_eq!(
				bound_plot.resolved_spec, inline_plot.resolved_spec,
				"bound {mark} must inherit exact inline colors and domains"
			);
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0055_horizontal_pair_reaches_every_bar_spelling_and_its_register() {
		let sources = [
			"plot.bar(workspace.table(\"t\"), \"value\", \"category\", \"series\")",
			"workspace.table(\"t\").bar(\"value\", \"category\", \"series\")",
			"workspace.table(\"t\").views([bar(\"value\", \"category\", \"series\")])",
			"workspace.table(\"t\").views([view(\"bars\", bar(\"value\", \"category\", \"series\"))])",
			"workspace.table(\"t\").views([figure([bar(\"value\", \"category\", \"series\")])])",
		];
		for source in sources {
			let relation = transform_rune_view(source)
				.expect("every established bar spelling admits the horizontal pair")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!(
					"category" => ["East"],
					"value" => [2.0],
					"series" => ["Atlas"],
				)
				.unwrap(),
				relation.views,
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _)) =
				&outcome.sinks[0].output
			else {
				panic!("horizontal spelling did not complete: {source}");
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				PlotType::HorizontalBar
			);
			assert_eq!(outcome.sinks[0].presentation.display_label, "plot.bar");
		}

		for source in [
			"workspace.table(\"t\").views([figure([bar(\"value\", \"category\").bar_width(0.5).category_gap(0.2)])])",
			"workspace.table(\"t\").views([figure([bar(\"value\", \"category\", \"series\").category_gap(0.2).bar_width(0.5)])])",
		] {
			let relation = transform_rune_view(source)
				.expect("the whole bar register is orientation-independent")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!(
					"category" => ["East"],
					"value" => [2.0],
					"series" => ["Atlas"],
				)
				.unwrap(),
				relation.views,
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::StyledPlot(resolved, layer, _, _)) =
				&outcome.sinks[0].output
			else {
				panic!("modified horizontal bar did not complete: {source}");
			};
			assert_eq!(
				resolved.resolved_spec.composition.primary_plot_type(),
				PlotType::HorizontalBar
			);
			assert_eq!(
				layer.bar_width,
				crate::plot::spec::Override::Set(crate::plot::spec::BarWidth::new(0.5).unwrap())
			);
			assert_eq!(
				layer.category_gap,
				crate::plot::spec::Override::Set(crate::plot::spec::CategoryGap::new(0.2).unwrap())
			);
			let kernel = create_styled_plot(&resolved.resolved_spec, &resolved.data, layer.clone())
				.expect("resolved horizontal member reaches the styled bar factory");
			assert!(matches!(
				kernel.layout(ResolvedPlotSettings::default()),
				PlotLayout::CategoricalY { .. }
			));
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0036_exclusion_and_arbitrarily_small_positive_spans_render_exactly() {
		for (minimum, maximum) in [(100.0, 200.0), (0.0, 1e-300)] {
			let source = format!(
				"workspace.table(\"t\").views([figure([x_axis().bounds({minimum}, {maximum}), line(\"x\", \"y\")])])"
			);
			let relation = transform_rune_view(&source)
				.expect("strictly ordered finite bounds")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(
				polars::df!("x" => [1.0_f64, 6.0], "y" => [1.0_f64, 6.0]).unwrap(),
				relation.views,
			);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
				&outcome.sinks[0].output
			else {
				panic!("defined finite bounds must complete: {minimum}..{maximum}");
			};
			let settings = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			}
			.resolve();
			assert_eq!(
				(settings.x_min, settings.x_max),
				(Some(minimum), Some(maximum))
			);
			let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
			assert!(matches!(
				kernel.layout(settings.clone()),
				PlotLayout::Cartesian { x_range, .. } if x_range == (minimum, maximum)
			));
			let svg = render_marks_with_settings(kernel.as_ref(), settings);
			assert!(svg.contains("<svg"));
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0036_layered_bounds_reach_one_shared_cartesian_domain() {
		let source = r#"workspace.table("t").views([figure([
			x_axis().bounds(-2.0, 2.0), y_axis().bounds(0.0, 8.0),
			layers([line("x", "y"), scatter("x", "y")])
		])])"#;
		let relation = transform_rune_view(source)
			.expect("bounded layered figure")
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [-1.0_f64, 1.0], "y" => [2.0_f64, 6.0]).unwrap(),
			relation.views,
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(specification, authored)) =
			&outcome.sinks[0].output
		else {
			panic!("bounded layered plot must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		let projected = outcome.sinks[0]
			.source
			.projection()
			.expect("fixture projection");
		let kernel = create_layered_plot(specification, projected.as_ref(), 1200, 720).unwrap();
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::Cartesian { x_range, y_range, .. }
				if x_range == (-2.0, 2.0) && y_range == (0.0, 8.0)
		));
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0038_tick_step_reaches_the_existing_renderer_with_and_without_bounds() {
		let bounded_source = "workspace.table(\"t\").views([figure([y_axis().bounds(0, 7), y_axis().tick_step(1), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(bounded_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B", "C"]), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("bounded tick step must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(
			settings.y_tick_placement,
			crate::plot::common::TickPlacement::step(1.0).unwrap()
		);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings.clone()),
			PlotLayout::CategoricalX { y_range, .. } if y_range == (0.0, 7.0)
		));
		assert_eq!(
			settings.y_tick_placement.positions((0.0, 7.0)).unwrap(),
			(0..=7).map(f64::from).collect::<Vec<_>>()
		);
		assert!(render_marks_with_settings(kernel.as_ref(), settings).contains("<svg"));

		let unbounded_source =
			"workspace.table(\"t\").views([figure([y_axis().tick_step(1), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(unbounded_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [1.0_f64, 6.0], "y" => [1.4_f64, 8.6]).unwrap(),
			relation.views,
		);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(_, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("data-derived tick step must complete");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(
			settings.y_tick_placement.positions((1.4, 8.6)).unwrap(),
			(2..=8).map(f64::from).collect::<Vec<_>>()
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0038_tick_step_refuses_discrete_and_dense_requests_at_their_spans() {
		use polars::prelude::{IntoColumn, NamedFrom};

		let discrete_source = "workspace.table(\"t\").views([figure([x_axis().tick_step(1), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(discrete_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical tick step must refuse");
		};
		assert!(
			error.contains(".tick_step(...)") && error.contains("Band"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&discrete_source[span.start as usize..span.end as usize],
			".tick_step(1)"
		);

		let dense_source = "workspace.table(\"t\").views([figure([y_axis().tick_step(1e-9), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(dense_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("dense tick step must refuse");
		};
		assert!(
			error.contains("0.000000001") && error.contains("capacity"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&dense_source[span.start as usize..span.end as usize],
			".tick_step(1e-9)"
		);

		let temporal_source =
			"workspace.table(\"t\").views([figure([x_axis().tick_step(1), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(temporal_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let temporal_x = polars::prelude::Series::new("x".into(), &[0_i32, 1_i32])
			.cast(&polars::prelude::DataType::Date)
			.unwrap();
		let frame = polars::prelude::DataFrame::new(
			2,
			vec![
				temporal_x.into_column(),
				polars::prelude::Series::new("y".into(), &[0.0_f64, 1.0]).into_column(),
			],
		)
		.unwrap();
		let outcome = rune_view_outcome(frame, relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("temporal tick step must refuse");
		};
		assert!(
			error.contains(".tick_step(...)") && error.contains("Time"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&temporal_source[span.start as usize..span.end as usize],
			".tick_step(1)"
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0130_tick_every_reaches_temporal_axes_and_refuses_invalid_destinations() {
		use polars::prelude::{IntoColumn, NamedFrom};

		let source = "workspace.table(\"t\").views([figure([x_axis().tick_every(\"1 month\"), line(\"day\", \"value\")])])";
		let relation = transform_rune_view(source)
			.expect("temporal tick spelling parses")
			.into_relations()
			.next()
			.expect("one relation");
		let days = polars::prelude::Series::new("day".into(), &[19_723_i32, 19_754, 19_783])
			.cast(&polars::prelude::DataType::Date)
			.expect("date source");
		let frame = polars::prelude::DataFrame::new(
			3,
			vec![
				days.into_column(),
				polars::prelude::Series::new("value".into(), &[1.0_f64, 2.0, 3.0]).into_column(),
			],
		)
		.expect("frame");
		let outcome = rune_view_outcome(frame.clone(), relation.views);
		let (resolved, authored) = match &outcome.sinks[0].output {
			Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) => {
				(resolved, authored)
			}
			Err(error) => panic!("calendar tick_every must reach a Date axis: {error}"),
			_ => panic!("calendar tick_every must produce a Cartesian plot"),
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert!(matches!(
			settings.x_tick_placement,
			crate::plot::common::TickPlacement::Temporal(_)
		));
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 1200, 720);
		assert!(matches!(
			kernel.layout(settings),
			PlotLayout::Cartesian {
				x_temporal_domain: Some(crate::plot::common::TemporalAxisDomain::Date { .. }),
				..
			}
		));

		for (source, frame, expected) in [
			(
				"workspace.table(\"t\").views([figure([x_axis().tick_every(\"1 day\"), line(\"x\", \"value\")])])",
				polars::df!("x" => [0.0_f64, 1.0], "value" => [1.0_f64, 2.0]).unwrap(),
				"requires a temporal",
			),
			(
				"workspace.table(\"t\").views([figure([x_axis().tick_every(\"1 hour\"), line(\"day\", \"value\")])])",
				frame,
				"does not apply",
			),
		] {
			let relation = transform_rune_view(source)
				.expect("surface spelling parses")
				.into_relations()
				.next()
				.expect("one relation");
			let outcome = rune_view_outcome(frame, relation.views);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid temporal tick_every must refuse")
			};
			assert!(error.contains(expected), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert!(
				&source[span.start as usize..span.end as usize].starts_with(".tick_every"),
				"the modifier owns the refusal"
			);
		}
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0130_tick_every_reaches_a_styled_single_temporal_layer() {
		use polars::prelude::{IntoColumn, NamedFrom};

		let source = "workspace.table(\"t\").views([figure([x_axis().tick_every(\"1 month\"), line(\"day\", \"value\").stroke_width(2)])])";
		let relation = transform_rune_view(source)
			.expect("styled temporal spelling parses")
			.into_relations()
			.next()
			.expect("one relation");
		let days = polars::prelude::Series::new("day".into(), &[19_723_i32, 19_754, 19_783])
			.cast(&polars::prelude::DataType::Date)
			.expect("date source");
		let frame = polars::prelude::DataFrame::new(
			3,
			vec![
				days.into_column(),
				polars::prelude::Series::new("value".into(), &[1.0_f64, 2.0, 3.0]).into_column(),
			],
		)
		.expect("frame");
		let outcome = rune_view_outcome(frame, relation.views);
		if let Err(error) = &outcome.sinks[0].output {
			panic!("styled single temporal layers must pass tick_every admission: {error}");
		}
		assert!(matches!(
			&outcome.sinks[0].output,
			Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(..)
				| crate::gui::messages::RuneRelationSinkOutput::StyledPlot(..)
				| crate::gui::messages::RuneRelationSinkOutput::LayeredPlot(..))
		));
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0039_minor_ticks_toggle_and_combined_capacity_are_attributed() {
		use polars::prelude::{IntoColumn, NamedFrom};

		let off_source = "workspace.table(\"t\").views([figure([y_axis().minor_ticks(0), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(off_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(_, authored, _)) =
			&outcome.sinks[0].output
		else {
			panic!("zero minor count must render");
		};
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		assert_eq!(settings.y_minor_ticks, 0);
		assert_eq!(settings.x_minor_ticks, 4);

		let discrete_source = "workspace.table(\"t\").views([figure([x_axis().minor_ticks(1), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(discrete_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B"]), relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("categorical minor ticks must refuse");
		};
		assert!(error.contains(".minor_ticks(...)") && error.contains("Band"));
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&discrete_source[span.start as usize..span.end as usize],
			".minor_ticks(1)"
		);

		let temporal_source =
			"workspace.table(\"t\").views([figure([x_axis().minor_ticks(1), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(temporal_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let temporal_x = polars::prelude::Series::new("x".into(), &[0_i32, 1_i32])
			.cast(&polars::prelude::DataType::Date)
			.unwrap();
		let frame = polars::prelude::DataFrame::new(
			2,
			vec![
				temporal_x.into_column(),
				polars::prelude::Series::new("y".into(), &[0.0_f64, 1.0]).into_column(),
			],
		)
		.unwrap();
		let outcome = rune_view_outcome(frame, relation.views);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("temporal minor ticks must refuse");
		};
		assert!(error.contains(".minor_ticks(...)") && error.contains("Time"));

		let major_alone = "workspace.table(\"t\").views([figure([y_axis().bounds(0, 7), y_axis().tick_step(0.001), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(major_alone)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		assert!(outcome.sinks[0].output.is_ok());

		let minor_alone = "workspace.table(\"t\").views([figure([y_axis().bounds(0, 7), y_axis().minor_ticks(9), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(minor_alone)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		assert!(outcome.sinks[0].output.is_ok());

		let combined = "workspace.table(\"t\").views([figure([y_axis().bounds(0, 7), y_axis().tick_step(0.001), y_axis().minor_ticks(9), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(combined)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("combined minor lattice must refuse");
		};
		assert!(
			error.contains(".minor_ticks(9)")
				&& error.contains("Step")
				&& error.contains("70001")
				&& error.contains("capacity"),
			"{error}"
		);
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&combined[span.start as usize..span.end as usize],
			".minor_ticks(9)"
		);

		let huge = "workspace.table(\"t\").views([figure([y_axis().minor_ticks(4294967295), line(\"x\", \"y\")])])";
		let relation = transform_rune_view(huge)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [0.0_f64, 7.0], "y" => [0.0_f64, 7.0]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("huge minor count must refuse before allocation");
		};
		assert!(
			error.contains("4294967295") && error.contains("capacity"),
			"{error}"
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0042_facet_reaches_the_head_as_structural_intent() {
		let source = "workspace.table(\"t\").views([figure([facet(\"cat\"), title(\"Revenue\"), bar(\"cat\", \"val\")])])";
		let relation = transform_rune_view(source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(categorical_bar_frame(&["A", "B", "A"]), relation.views);
		assert_eq!(outcome.sinks[0].facet.as_deref(), Some("cat"));
		let crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _) =
			outcome.sinks[0].output.clone().expect("facet resolves")
		else {
			panic!("facet bar should use the regular resolved plot path");
		};
		let stamped = crate::plot::spec::StampedResult {
			data: Arc::clone(&outcome.result),
			provenance: resolved.resolved_spec.provenance.clone(),
		};
		let grid = PlotGrid::value_facet_derived(
			Arc::new(resolved.resolved_spec.clone()),
			&stamped,
			"cat",
		)
		.expect("facet grid");
		assert_eq!(grid.cells.len(), 2);
		assert_eq!(
			grid.arrangement,
			crate::plot::grid::GridArrangement {
				rows: 1,
				columns: 2
			}
		);
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0080_product_0033_ordered_skin_installs_chrome_and_mark_style_atomically() {
		fn completed(
			skin: Option<&str>,
			duotone: bool,
			palette: &str,
		) -> (crate::plot::request::ResolvedPlot, ResolvedPlotSettings) {
			let skin = skin
				.map(|name| {
					format!(
						"skin(\"{name}\"){}, ",
						if duotone { ".duotone()" } else { "" }
					)
				})
				.unwrap_or_default();
			let source = format!(
				"workspace.table(\"t\").views([figure([{skin}palette(\"{palette}\"), bar(\"cat\", \"val\")])])"
			);
			let relation = transform_rune_view(&source)
				.unwrap()
				.into_relations()
				.next()
				.unwrap();
			let outcome =
				rune_view_outcome(categorical_bar_frame(&["A", "B", "C"]), relation.views);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, authored, _)) =
				&outcome.sinks[0].output
			else {
				panic!("expected completed plot")
			};
			let settings = crate::plot::theme::PlotSettings {
				authored_spec: authored.clone(),
				..crate::plot::theme::PlotSettings::default()
			}
			.resolve();
			(resolved.clone(), settings)
		}

		let (reversed, reversed_settings) = completed(Some("viridis_r"), false, "viridis_r");
		let (shifted, shifted_settings) = completed(Some("viridis_s5"), false, "viridis_s5");
		let reversed_bands = band_mapping(&reversed);
		let shifted_bands = band_mapping(&shifted);
		assert_eq!(
			reversed_bands
				.iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			shifted_bands
				.iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>()
		);
		assert_ne!(
			reversed_bands, shifted_bands,
			"ordered data swatches recolor"
		);
		assert_ne!(
			reversed_settings.chrome, shifted_settings.chrome,
			"ordinary skin preserves resolved ordering"
		);
		let (_, reversed_duotone) = completed(Some("viridis_r"), true, "viridis_r");
		let (_, shifted_duotone) = completed(Some("viridis_s5"), true, "viridis_s5");
		assert_eq!(
			reversed_duotone.chrome, shifted_duotone.chrome,
			"explicit Duotone intentionally consumes the unchanged set"
		);
		assert_ne!(
			reversed_settings.mark_style, shifted_settings.mark_style,
			"authored skins now resolve marks with furniture"
		);
		assert_eq!(
			reversed_duotone.mark_style, shifted_duotone.mark_style,
			"Duotone projects one atomic skin value"
		);

		let (_, nord_tab20c) = completed(Some("nord"), false, "tab20c");
		let (yellows, nord_yellows) = completed(Some("nord"), false, "yellows");
		assert_eq!(nord_tab20c.chrome, nord_yellows.chrome);
		assert_eq!(
			band_mapping(&yellows)
				.into_iter()
				.map(|(_, _, color)| color)
				.collect::<Vec<_>>(),
			[(252, 252, 204), (247, 210, 48), (153, 102, 0)],
			"awkward data colors are accepted without a chrome/data contrast gate"
		);

		let (_, omitted) = completed(None, false, "tab20c");
		assert_eq!(omitted.chrome, crate::plot::chrome::PlotChrome::DEFAULT);

		let (_, below_threshold) = completed(Some("tab20c_s9"), false, "tab20c");
		assert_eq!(
			below_threshold.chrome.weakest_contrast().role(),
			"title text"
		);
		assert!(!below_threshold.chrome.passes_contrast());
	}

	#[cfg(toron_local)]
	#[test]
	fn product_0032_skin_failures_are_destination_local() {
		let source = r#"workspace.table("t").views([
			view("bad", figure([skin("not-a-theme"), line("x", "y")])),
			view("good", figure([skin("nord"), line("x", "y")]))
		])"#;
		let relation = transform_rune_view(source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let outcome = rune_view_outcome(
			polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap(),
			relation.views,
		);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("invalid skin must fail its destination")
		};
		assert!(error.contains("unknown plot skin"));
		let span = &outcome.sinks[0].span;
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			"skin(\"not-a-theme\")"
		);
		assert!(outcome.sinks[1].output.is_ok());
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0032_skin_lifecycle_reuses_and_atomically_stales_one_host_panel() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1032);
		let identity = LocalViewIdentity::Named("plot".to_owned());
		let source = |skin: Option<&str>| {
			let skin = skin
				.map(|name| format!("skin(\"{name}\"), "))
				.unwrap_or_default();
			format!(
				"workspace.table(\"t\").views([view(\"plot\", figure([{skin}line(\"x\", \"y\")]))])"
			)
		};
		let frame = || polars::df!("x" => [1_i64, 2], "y" => [2_i64, 4]).unwrap();

		run_one_relation_program(&mut state, source_buffer, 1, &source(Some("nord")), frame());
		let panel = state
			.code_editor
			.rune_view_panel_id(source_buffer, &identity)
			.unwrap();
		let nord_svg = state
			.code_editor
			.rune_view_panel_plot_svg(source_buffer, &identity)
			.unwrap();

		let failed = run_one_relation_program(
			&mut state,
			source_buffer,
			2,
			&source(Some("not-a-theme")),
			frame(),
		);
		assert!(failed.is_empty());
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_plot_svg(source_buffer, &identity)
				.unwrap(),
			nord_svg,
			"failed projection retains content and chrome as one stale pair"
		);

		run_one_relation_program(
			&mut state,
			source_buffer,
			3,
			&source(Some("viridis")),
			frame(),
		);
		let viridis_svg = state
			.code_editor
			.rune_view_panel_plot_svg(source_buffer, &identity)
			.unwrap();
		assert_ne!(viridis_svg, nord_svg);

		run_one_relation_program(&mut state, source_buffer, 4, &source(None), frame());
		let default_svg = state
			.code_editor
			.rune_view_panel_plot_svg(source_buffer, &identity)
			.unwrap();
		assert_ne!(default_svg, viridis_svg);
		assert_eq!(
			state
				.code_editor
				.rune_view_panel_id(source_buffer, &identity),
			Some(panel)
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0030_live_identity_commits_with_content_and_retargets_by_length() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1031);
		let source = |palette: &str| {
			format!(
				"workspace.table(\"t\").views([view(\"bars\", figure([palette(\"{palette}\"), bar(\"cat\", \"val\")]))])"
			)
		};

		for (revision, categories, expected) in [
			(1, vec!["A"], vec![("A", 0)]),
			(2, vec!["A", "B"], vec![("A", 0), ("B", 19)]),
			(3, vec!["A", "B", "C"], vec![("A", 0), ("B", 19), ("C", 9)]),
		] {
			let plots = run_one_relation_program(
				&mut state,
				source_buffer,
				revision,
				&source("tab20c"),
				categorical_bar_frame(&categories),
			);
			assert_eq!(
				band_mapping(&plots[0].1)
					.into_iter()
					.map(|(category, position, _)| (category, position))
					.collect::<Vec<_>>(),
				expected
					.into_iter()
					.map(|(category, position)| (category.to_owned(), position))
					.collect::<Vec<_>>()
			);
		}

		let identity = LocalViewIdentity::Named("bars".to_owned());
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 20))
				.expect("twenty-position bank")
				.len(),
			3
		);
		let filtered = run_one_relation_program(
			&mut state,
			source_buffer,
			4,
			&source("tab20c"),
			categorical_bar_frame(&["A", "C"]),
		);
		assert_eq!(
			band_mapping(&filtered[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[("A".to_owned(), 0), ("C".to_owned(), 9)]
		);
		let returned = run_one_relation_program(
			&mut state,
			source_buffer,
			5,
			&source("tab20c"),
			categorical_bar_frame(&["B"]),
		);
		assert_eq!(band_mapping(&returned[0].1)[0].1, 19);

		for (revision, palette, expected) in [
			(
				6,
				"tab20c_r",
				[
					(0, (150, 150, 150)),
					(19, (49, 130, 189)),
					(9, (161, 217, 155)),
				],
			),
			(
				7,
				"tab20c_s1",
				[(0, (150, 150, 150)), (19, (63, 0, 125)), (9, (49, 163, 84))],
			),
			(
				8,
				"viridis",
				[(0, (68, 1, 83)), (10, (39, 151, 136)), (19, (253, 231, 36))],
			),
		] {
			let plots = run_one_relation_program(
				&mut state,
				source_buffer,
				revision,
				&source(palette),
				categorical_bar_frame(&["A", "B", "C"]),
			);
			assert_eq!(
				band_mapping(&plots[0].1),
				[
					("A".to_owned(), expected[0].0, expected[0].1),
					("B".to_owned(), expected[1].0, expected[1].1),
					("C".to_owned(), expected[2].0, expected[2].1),
				]
			);
		}

		let dark2 = run_one_relation_program(
			&mut state,
			source_buffer,
			9,
			&source("dark2"),
			categorical_bar_frame(&["A", "B", "C"]),
		);
		assert_eq!(
			band_mapping(&dark2[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[
				("A".to_owned(), 0),
				("B".to_owned(), 4),
				("C".to_owned(), 7)
			]
		);
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 8))
				.unwrap()
				.len(),
			3
		);

		let resumed = run_one_relation_program(
			&mut state,
			source_buffer,
			10,
			&source("tab20c"),
			categorical_bar_frame(&["A", "B", "C"]),
		);
		assert_eq!(
			band_mapping(&resumed[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[
				("A".to_owned(), 0),
				("B".to_owned(), 10),
				("C".to_owned(), 19)
			]
		);

		let failed = run_one_relation_program(
			&mut state,
			source_buffer,
			11,
			&source("dark2"),
			categorical_bar_frame(&["A", "B", "C", "D", "E", "F", "G", "H", "I"]),
		);
		assert!(failed.is_empty(), "capacity failure cannot install content");
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 8))
				.unwrap()
				.len(),
			3,
			"failed resolution cannot advance continuation"
		);

		let stale_source = source("tab20c");
		let relation = transform_rune_view(&stale_source)
			.unwrap()
			.into_relations()
			.next()
			.unwrap();
		let presentation = relation.views[0].intent.presentation();
		let stale_identity = relation.views[0].identity.clone();
		let palettes = super::prepare_rune_categorical_palettes(&state, source_buffer, &relation);
		let outcome = super::rune_view_outcome_with_palettes(
			crate::result_artifact::ResultArtifact::test_frame(categorical_bar_frame(&[
				"A", "B", "C", "D",
			])),
			relation.views,
			palettes,
		)
		.expect("fixture projects");
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			stale_resolved,
			stale_settings,
			stale_continuation,
		)) = outcome.sinks[0].output.clone()
		else {
			panic!("stale proposal")
		};
		let panel_id = state
			.code_editor
			.rune_view_panel_id(source_buffer, &stale_identity)
			.unwrap();
		state.rune_buffer_revisions.insert(source_buffer, 13);
		let kernel = create_plot(
			&stale_resolved.resolved_spec,
			&stale_resolved.data,
			1200,
			720,
		);
		let recipe = crate::plot::recipe::PlotRecipe::single(
			Arc::clone(&stale_resolved.artifact),
			stale_resolved.resolved_spec.clone(),
			1200,
			720,
		);
		drop(update(
			&mut state,
			Message::RuneRelationPlotReady {
				source_buffer,
				revision: 12,
				panel_id,
				identity: stale_identity,
				presentation,
				authored_settings: stale_settings,
				continuation: stale_continuation,
				resolved: stale_resolved,
				kernel,
				recipe,
			},
		));
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(source_buffer, identity.clone(), 20))
				.unwrap()
				.len(),
			3,
			"superseded content cannot reserve a position"
		);

		let after_stale = run_one_relation_program(
			&mut state,
			source_buffer,
			13,
			&source("tab20c"),
			categorical_bar_frame(&["A", "B", "C", "E"]),
		);
		assert_eq!(
			band_mapping(&after_stale[0].1)
				.into_iter()
				.map(|(category, position, _)| (category, position))
				.collect::<Vec<_>>(),
			[
				("A".to_owned(), 0),
				("B".to_owned(), 10),
				("C".to_owned(), 19),
				("E".to_owned(), 5),
			]
		);
		state.code_editor.shutdown();
	}

	#[cfg(feature = "toron")]
	#[test]
	fn product_0030_origins_and_views_keep_independent_banks_across_completion_order() {
		let source = r#"
			workspace.table("left").views([view("left-bars", figure([
				palette("tab20c"), bar("cat", "val")
			]))])
			workspace.table("right").views([view("right-bars", figure([
				palette("tab20c"), bar("cat", "val")
			]))])
		"#;
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(1032);

		let mut run = |revision: u64, reverse: bool, left: &[&str], right: &[&str]| {
			let program = transform_rune_view(source).expect("two-origin categorical program");
			let presentation = program
				.presentation_views()
				.into_iter()
				.map(|view| (view.identity.clone(), view.intent.presentation()))
				.collect::<Vec<_>>();
			state.rune_buffer_revisions.insert(source_buffer, revision);
			state
				.code_editor
				.prepare_rune_view_panels(source_buffer, &presentation, revision);
			assert!(
				state
					.rune_view_scheduler
					.submit(PendingRuneView {
						source_buffer,
						revision,
						view: program.clone(),
					})
					.is_some()
			);
			let mut relations = program.into_relations().collect::<Vec<_>>();
			if reverse {
				relations.reverse();
			}
			let mut mappings = HashMap::new();
			for relation in relations {
				let is_left = relation.origin.contains("table(\"left\")");
				let frame = categorical_bar_frame(if is_left { left } else { right });
				for (identity, resolved) in
					complete_relation_in_head(&mut state, source_buffer, revision, relation, frame)
				{
					mappings.insert(identity, band_mapping(&resolved));
				}
			}
			mappings
		};

		let first = run(1, true, &["L"], &["R"]);
		assert_eq!(
			first[&LocalViewIdentity::Named("left-bars".to_owned())][0].1,
			0
		);
		assert_eq!(
			first[&LocalViewIdentity::Named("right-bars".to_owned())][0].1,
			0
		);
		let second = run(2, false, &["L", "M", "N"], &["R", "S"]);
		assert_eq!(
			second[&LocalViewIdentity::Named("left-bars".to_owned())]
				.iter()
				.map(|(_, position, _)| *position)
				.collect::<Vec<_>>(),
			[0, 19, 9]
		);
		assert_eq!(
			second[&LocalViewIdentity::Named("right-bars".to_owned())]
				.iter()
				.map(|(_, position, _)| *position)
				.collect::<Vec<_>>(),
			[0, 19]
		);
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(
					source_buffer,
					LocalViewIdentity::Named("left-bars".to_owned()),
					20,
				))
				.unwrap()
				.len(),
			3
		);
		assert_eq!(
			state
				.rune_categorical_identities
				.get(&(
					source_buffer,
					LocalViewIdentity::Named("right-bars".to_owned()),
					20,
				))
				.unwrap()
				.len(),
			2
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn non_tabular_sql_completion_stays_out_of_result_surfaces() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);

		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Batch { statements: 3 },
				publish: None,
				generation: 0,
			},
		);

		assert_eq!(state.status_msg, "SQL · 3 statements completed");
		assert!(
			!state.code_editor.has_query_surface(),
			"a successful command batch must not manufacture a result pane"
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn tabular_sql_completion_claims_the_result_surface() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let frame = polars::df!("x" => [1_i64, 2]).expect("frame");

		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Rows {
					artifact: crate::result_artifact::ResultArtifact::test_frame(frame),
					statements: 3,
				},
				publish: None,
				generation: 0,
			},
		);

		assert_eq!(state.status_msg, "SQL · 3 statements completed");
		assert!(
			state.code_editor.has_query_surface(),
			"a DataFrame is an artifact and must own a result surface"
		);
		state.code_editor.shutdown();
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0079_histogram_resolves_values_spans_order_and_skin_without_color_identity() {
		use crate::plot::chrome::ResolvedPlotSkin;
		use crate::plot::spec::Channel;
		use crate::plot::theme::PlotSettings;
		use polars::frame::DataFrame;
		let source = r#"workspace.table("bins").views([figure([
    skin("nord"),
    palette("tab20c"),
    histogram("bin_start", "bin_end", "count")
])])"#;
		let complete = |source: &str, frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("histogram source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"bin_start" => [0.0, 2.0],
			"bin_end" => [1.0, 3.0],
			"count" => [4_i64, 7_i64],
		)
		.unwrap();
		let outcome = complete(source, valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			resolved,
			authored,
			continuation,
		)) = &outcome.sinks[0].output
		else {
			panic!("valid histogram must resolve");
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Histogram { bin_start, bin_end, count }
				if bin_start == "bin_start" && bin_end == "bin_end" && count == "count"
		));
		assert!(resolved.resolved_spec.band_fills.is_empty());
		assert!(resolved.resolved_spec.channel(Channel::Color).is_none());
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let settings = PlotSettings {
			authored_spec: authored.clone(),
			..PlotSettings::default()
		}
		.resolve();
		assert_eq!(
			settings.mark_style.fill,
			ResolvedPlotSkin::from_theme_name(
				"nord",
				crate::plot::chrome::PlotChromeProjection::Ordered,
			)
			.unwrap()
			.mark_style
			.fill
		);
		let without_palette = source.replace("    palette(\"tab20c\"),\n", "");
		let no_palette = complete(&without_palette, valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			no_palette_resolved,
			no_palette_authored,
			_,
		)) = &no_palette.sinks[0].output
		else {
			panic!("histogram without palette must resolve");
		};
		assert!(no_palette_resolved.resolved_spec.band_fills.is_empty());
		assert_eq!(
			PlotSettings {
				authored_spec: no_palette_authored.clone(),
				..PlotSettings::default()
			}
			.resolve()
			.mark_style
			.fill,
			settings.mark_style.fill,
			"palette metadata cannot affect an unencoded histogram fill"
		);

		for (frame, expected) in [
			(
				polars::df!("bin_start" => [0.0], "bin_end" => [1.0], "count" => [1.5]).unwrap(),
				"integer dtype",
			),
			(
				polars::df!("bin_start" => [0.0], "bin_end" => [1.0], "count" => [-1_i64]).unwrap(),
				"negative at row 0: -1",
			),
			(
				polars::df!("bin_start" => ["bad"], "bin_end" => [1.0], "count" => [1_i64])
					.unwrap(),
				"`bin_start` could not resolve: Nominal is incompatible with Histogram",
			),
			(
				polars::df!("bin_start" => [0.0], "bin_end" => [1.0]).unwrap(),
				"field is absent",
			),
			(
				polars::df!(
					"bin_start" => [Some(0.0), None],
					"bin_end" => [1.0, 2.0],
					"count" => [1_i64, 1_i64],
				)
				.unwrap(),
				"bin_start `bin_start` is null at row 1",
			),
			(
				polars::df!(
					"bin_start" => [0.0, 1.0],
					"bin_end" => [Some(1.0), None],
					"count" => [1_i64, 1_i64],
				)
				.unwrap(),
				"bin_end `bin_end` is null at row 1",
			),
			(
				polars::df!(
					"bin_start" => [0.0, 1.0],
					"bin_end" => [1.0, 2.0],
					"count" => [Some(1_i64), None],
				)
				.unwrap(),
				"count `count` is null at row 1",
			),
		] {
			let outcome = complete(source, frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid histogram must refuse");
			};
			assert!(error.contains(expected), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				"histogram(\"bin_start\", \"bin_end\", \"count\")"
			);
			assert!(complete(source, valid.clone()).sinks[0].output.is_ok());
		}

		let descending = polars::df!(
			"bin_start" => [2.0, 0.0],
			"bin_end" => [3.0, 1.0],
			"count" => [7_i64, 4_i64],
		)
		.unwrap();
		let outcome = complete(source, descending);
		let Err(error) = &outcome.sinks[0].output else {
			panic!("descending histogram must refuse");
		};
		assert!(error.contains("unordered interval") && error.contains("sort the relation"));
		let ascending = complete(source, valid);
		assert!(ascending.sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0082_box_resolves_aliases_values_and_failure_scope() {
		use crate::plot::common::AxisType;
		use crate::plot::theme::PlotSettings;
		use polars::frame::DataFrame;
		let source = r#"workspace.table("box_probe").views([box("group", "lo", "p25", "mid", "p75", "hi", "point")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("box source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"group" => ["none", "all"],
			"lo" => [0_i32, 10],
			"p25" => [1.0_f32, 11.0],
			"mid" => [2_i64, 12],
			"p75" => [3.0_f64, 13.0],
			"hi" => [4_u32, 14],
			"point" => [None, Some(20_i64)],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid box must resolve");
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::BoxPlot { category, whisker_low, q1, median, q3, whisker_high, outlier }
				if (category, whisker_low, q1, median, q3, whisker_high, outlier)
					== (&"group".to_owned(), &"lo".to_owned(), &"p25".to_owned(), &"mid".to_owned(), &"p75".to_owned(), &"hi".to_owned(), &"point".to_owned())
		));
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 600, 400);
		assert_eq!(kernel.x_label(), "group");
		assert_eq!(kernel.y_label(), "");
		assert!(matches!(
			kernel.layout(PlotSettings::default().resolve()),
			PlotLayout::CategoricalX {
				y_axis_type: AxisType::Linear,
				..
			}
		));

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid box rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "mid" => [2.0], "p75" => [3.0],
				"hi" => [4.0], "point" => [None::<f64>],
			)
			.unwrap(),
			"\"p25\"",
			"absent",
		);
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "p25" => ["bad"], "mid" => [2.0],
				"p75" => [3.0], "hi" => [4.0], "point" => [None::<f64>],
			)
			.unwrap(),
			"\"p25\"",
			"quantitative",
		);
		assert_span(
			polars::df!(
				"group" => [None::<&str>], "lo" => [0.0], "p25" => [1.0], "mid" => [2.0],
				"p75" => [3.0], "hi" => [4.0], "point" => [None::<f64>],
			)
			.unwrap(),
			"\"group\"",
			"non-null",
		);
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0_i64], "p25" => [1_i64], "mid" => [2_i64],
				"p75" => [3_i64], "hi" => [4_i64], "point" => [Some(9_007_199_254_740_993_i64)],
			)
			.unwrap(),
			"\"point\"",
			"9007199254740993",
		);
		let mark = "box(\"group\", \"lo\", \"p25\", \"mid\", \"p75\", \"hi\", \"point\")";
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "p25" => [3.0], "mid" => [2.0],
				"p75" => [4.0], "hi" => [5.0], "point" => [None::<f64>],
			)
			.unwrap(),
			mark,
			"does not satisfy",
		);
		assert_span(
			polars::df!(
				"group" => ["A"], "lo" => [0.0], "p25" => [1.0], "mid" => [2.0],
				"p75" => [3.0], "hi" => [4.0], "point" => [Some(2.5)],
			)
			.unwrap(),
			mark,
			"inside the whisker span",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0092_heatmap_resolves_and_attributes_binding_and_cell_refusals() {
		use polars::frame::DataFrame;
		let source =
			r#"workspace.table("heat_probe").views([heatmap("region", "quarter", "margin")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("heatmap source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"region" => ["north", "north", "south"],
			"quarter" => ["q1", "q2", "q1"],
			"margin" => [-10_i64, 5_i64, 20_i64],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid heatmap must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Heatmap { x, y, value }
				if (x.as_str(), y.as_str(), value.as_str()) == ("region", "quarter", "margin")
		));
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid heatmap rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!(
				"region" => [1_i64], "quarter" => ["q1"], "margin" => [1.0],
			)
			.unwrap(),
			"\"region\"",
			"incompatible with Heatmap",
		);
		assert_span(
			polars::df!(
				"region" => ["north"], "quarter" => ["q1"], "margin" => ["bad"],
			)
			.unwrap(),
			"\"margin\"",
			"quantitative",
		);
		assert_span(
			polars::df!(
				"region" => ["north"], "quarter" => ["q1"], "margin" => [f64::NAN],
			)
			.unwrap(),
			"\"margin\"",
			"non-finite",
		);
		assert_span(
			polars::df!(
				"region" => [None::<&str>], "quarter" => ["q1"], "margin" => [1.0],
			)
			.unwrap(),
			"\"region\"",
			"x key is null",
		);
		assert_span(
			polars::df!(
				"region" => ["north"], "quarter" => [None::<&str>], "margin" => [1.0],
			)
			.unwrap(),
			"\"quarter\"",
			"y key is null",
		);
		assert_span(
			polars::df!(
				"region" => ["north", "north"], "quarter" => ["q1", "q1"],
				"margin" => [1.0, 2.0],
			)
			.unwrap(),
			"\"region\", \"quarter\"",
			"duplicated at rows 1 and 2",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0144_hexbin_resolves_without_identity_and_attributes_both_failure_stations() {
		use polars::frame::DataFrame;
		use polars::prelude::{Column, DataType};
		let source = r#"workspace.table("hexbin_probe").views([hexbin("x", "y")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("hexbin source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"x" => [0.0_f64, 0.1, 1.0],
			"y" => [0.0_f64, 0.1, 1.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid authored hexbin must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Hexbin { x, y }
				if x == "x" && y == "y"
		));
		assert_eq!(resolved.resolved_spec.color_job, None);
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid hexbin rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(polars::df!("y" => [1.0_f64]).unwrap(), "\"x\"", "absent");
		assert_span(polars::df!("x" => [1.0_f64]).unwrap(), "\"y\"", "absent");
		assert_span(
			polars::df!("x" => ["nominal"], "y" => [1.0_f64]).unwrap(),
			"\"x\"",
			"incompatible with Hexbin",
		);
		assert_span(
			polars::df!("x" => [1.0_f64], "y" => ["nominal"]).unwrap(),
			"\"y\"",
			"incompatible with Hexbin",
		);
		for temporal in [
			Column::new("value".into(), [1_i32])
				.cast(&DataType::Date)
				.unwrap(),
			Column::new("value".into(), [1_i64])
				.cast(&DataType::Datetime(
					polars::prelude::TimeUnit::Milliseconds,
					None,
				))
				.unwrap(),
			Column::new("value".into(), [1_i64])
				.cast(&DataType::Datetime(
					polars::prelude::TimeUnit::Milliseconds,
					polars::prelude::TimeZone::opt_try_new(Some("UTC")).unwrap(),
				))
				.unwrap(),
			Column::new("value".into(), [1_i64])
				.cast(&DataType::Time)
				.unwrap(),
		] {
			assert_span(
				DataFrame::new(
					1,
					vec![
						temporal.clone().with_name("x".into()),
						Column::new("y".into(), [1.0_f64]),
					],
				)
				.unwrap(),
				"\"x\"",
				"incompatible with Hexbin",
			);
			assert_span(
				DataFrame::new(
					1,
					vec![
						Column::new("x".into(), [1.0_f64]),
						temporal.with_name("y".into()),
					],
				)
				.unwrap(),
				"\"y\"",
				"incompatible with Hexbin",
			);
		}
		assert_span(
			polars::df!("x" => [None::<f64>], "y" => [1.0_f64]).unwrap(),
			"\"x\"",
			"null at row 1",
		);
		assert_span(
			polars::df!("x" => [1.0_f64], "y" => [f64::NAN]).unwrap(),
			"\"y\"",
			"non-finite at row 1",
		);
		assert_span(
			polars::df!("x" => [9_007_199_254_740_993_i64], "y" => [1_i64]).unwrap(),
			"\"x\"",
			"cannot be represented exactly",
		);
		assert!(complete(valid.clone()).sinks[0].output.is_ok());

		let resolve = |source: &str| {
			let relation = transform_rune_view(source)
				.expect("bound hexbin source compiles")
				.into_relations()
				.next()
				.unwrap();
			let outcome = rune_view_outcome(valid.clone(), relation.views);
			let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, _)) =
				&outcome.sinks[0].output
			else {
				panic!("bound hexbin resolves")
			};
			resolved.resolved_spec.clone()
		};
		let inline = resolve(r#"workspace.table("hexbin_probe").views([hexbin("x", "y")])"#);
		let bound = resolve(
			r#"let horizontal = "x"; let vertical = "y"; workspace.table("hexbin_probe").views([hexbin(horizontal, vertical)])"#,
		);
		assert_eq!(bound, inline);
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0146_fill_between_attributes_resolution_preparation_and_structure() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("fill_between_probe").views([fill_between("x", "low", "high", "mid")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("fill-between source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"x" => [1.0_f64, 2.0],
			"low" => [0.0_f64, 1.0],
			"high" => [2.0_f64, 3.0],
			"mid" => [1.0_f64, 2.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid authored fill-between must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::FillBetween {
				x,
				lower,
				upper,
				middle: Some(middle),
			} if x == "x" && lower == "low" && upper == "high" && middle == "mid"
		));
		assert_eq!(resolved.resolved_spec.color_job, None);
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid fill-between rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!("low" => [0.0], "high" => [2.0], "mid" => [1.0]).unwrap(),
			"\"x\"",
			"absent",
		);
		assert_span(
			polars::df!("x" => [1.0], "high" => [2.0], "mid" => [1.0]).unwrap(),
			"\"low\"",
			"absent",
		);
		assert_span(
			polars::df!("x" => [1.0], "low" => [0.0], "mid" => [1.0]).unwrap(),
			"\"high\"",
			"absent",
		);
		assert_span(
			polars::df!("x" => [1.0], "low" => [0.0], "high" => [2.0]).unwrap(),
			"\"mid\"",
			"absent",
		);
		assert_span(
			polars::df!("x" => ["unordered"], "low" => [0.0], "high" => [2.0], "mid" => [1.0])
				.unwrap(),
			"\"x\"",
			"incompatible with FillBetween",
		);
		assert_span(
			polars::df!("x" => [1.0], "low" => [None::<f64>], "high" => [2.0], "mid" => [1.0])
				.unwrap(),
			"\"low\"",
			"null at row 1",
		);
		assert_span(
			polars::df!("x" => [2.0, 1.0], "low" => [0.0, 0.0], "high" => [2.0, 2.0], "mid" => [1.0, 1.0]).unwrap(),
			"\"x\"",
			"must increase strictly",
		);
		assert_span(
			polars::df!("x" => [1.0], "low" => [3.0], "high" => [2.0], "mid" => [2.5]).unwrap(),
			"\"low\"",
			"band crosses at row 1",
		);
		assert_span(
			polars::df!("x" => [1.0], "low" => [0.0], "high" => [2.0], "mid" => [3.0]).unwrap(),
			"\"mid\"",
			"middle escapes the band at row 1",
		);
		for (field, expected, fragment) in [
			("low", "\"low\"", "incompatible with FillBetween"),
			("high", "\"high\"", "requires a quantitative dtype"),
			("mid", "\"mid\"", "requires a quantitative dtype"),
		] {
			let temporal = polars::prelude::Column::new(field.into(), [1_i32])
				.cast(&polars::prelude::DataType::Date)
				.unwrap();
			let mut columns = vec![
				polars::prelude::Column::new("x".into(), [1.0_f64]),
				polars::prelude::Column::new("low".into(), [0.0_f64]),
				polars::prelude::Column::new("high".into(), [2.0_f64]),
				polars::prelude::Column::new("mid".into(), [1.0_f64]),
			];
			let index = columns
				.iter()
				.position(|column| column.name().as_str() == field)
				.unwrap();
			columns[index] = temporal;
			assert_span(DataFrame::new(1, columns).unwrap(), expected, fragment);
		}
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0148_candlestick_attributes_resolution_preparation_and_structure() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("candlestick_probe").views([candlestick("x", "open", "high", "low", "close")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("candlestick source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"x" => [1.0_f64, 2.0],
			"open" => [10.0_f64, 11.0],
			"high" => [12.0_f64, 13.0],
			"low" => [9.0_f64, 10.0],
			"close" => [11.0_f64, 12.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid authored candlestick must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Candlestick { x, open, high, low, close }
				if x == "x" && open == "open" && high == "high" && low == "low" && close == "close"
		));
		assert_eq!(resolved.resolved_spec.color_job, None);
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);

		let assert_span = |frame: DataFrame, expected: &str, fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid candlestick rendered")
			};
			assert!(error.contains(fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};
		assert_span(
			polars::df!(
				"x" => [1.0], "open" => [10.0], "low" => [9.0], "close" => [11.0]
			)
			.unwrap(),
			"\"high\"",
			"absent",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "open" => [10.0], "high" => [12.0], "low" => [9.0],
				"close" => [None::<f64>]
			)
			.unwrap(),
			"\"close\"",
			"null at row 1",
		);
		assert_span(
			polars::df!(
				"x" => [2.0, 1.0], "open" => [10.0, 10.0], "high" => [12.0, 12.0],
				"low" => [9.0, 9.0], "close" => [11.0, 11.0]
			)
			.unwrap(),
			"\"x\"",
			"must increase strictly",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "open" => [10.0], "high" => [12.0], "low" => [11.0],
				"close" => [10.5]
			)
			.unwrap(),
			"\"low\"",
			"lower bound fails",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "open" => [10.0], "high" => [10.5], "low" => [9.0],
				"close" => [11.0]
			)
			.unwrap(),
			"\"high\"",
			"upper bound fails",
		);
		for (field, expected) in [
			("open", "\"open\""),
			("high", "\"high\""),
			("low", "\"low\""),
			("close", "\"close\""),
		] {
			let temporal = polars::prelude::Column::new(field.into(), [1_i32])
				.cast(&polars::prelude::DataType::Date)
				.unwrap();
			let mut columns = vec![
				polars::prelude::Column::new("x".into(), [1.0_f64]),
				polars::prelude::Column::new("open".into(), [10.0_f64]),
				polars::prelude::Column::new("high".into(), [12.0_f64]),
				polars::prelude::Column::new("low".into(), [9.0_f64]),
				polars::prelude::Column::new("close".into(), [11.0_f64]),
			];
			let index = columns
				.iter()
				.position(|column| column.name().as_str() == field)
				.unwrap();
			columns[index] = temporal;
			assert_span(
				DataFrame::new(1, columns).unwrap(),
				expected,
				if field == "open" { "Temporal" } else { "Date" },
			);
		}
	}

	#[test]
	fn product_0146_checked_in_probe_is_idempotent_and_crosses_at_row_two() {
		let source = include_str!("../../../../uat/fill_between_probe.sql");
		assert!(source.contains("DROP TABLE IF EXISTS fill_between_probe;"));
		assert!(source.contains("DROP TABLE IF EXISTS fill_between_probe_crossed;"));
		let crossed = polars::df!(
			"x" => [1.0_f64, 2.0, 3.0],
			"low" => [10.0_f64, 16.0, 12.0],
			"high" => [14.0_f64, 15.0, 17.0],
		)
		.unwrap();
		let error = crate::plot::kernels::fill_between::prepare_fill_between_data(
			&crossed, "x", "low", "high", None,
		)
		.expect_err("probe's second row crosses");
		assert!(matches!(
			error,
			crate::plot::kernels::fill_between::FillBetweenPreparationError::CrossedBand {
				row: 2,
				..
			}
		));
	}

	#[test]
	fn product_0148_checked_in_probe_is_idempotent_and_tears_high_at_row_two() {
		let source = include_str!("../../../../uat/candlestick_probe.sql");
		assert!(source.contains("DROP TABLE IF EXISTS candlestick_probe;"));
		assert!(source.contains("DROP TABLE IF EXISTS candlestick_probe_torn;"));
		let torn = polars::df!(
			"x" => [1.0_f64, 2.0, 3.0],
			"open" => [100.0_f64, 104.0, 101.0],
			"high" => [106.0_f64, 101.0, 105.0],
			"low" => [98.0_f64, 96.0, 97.0],
			"close" => [104.0_f64, 103.0, 101.0],
		)
		.unwrap();
		let error = crate::plot::kernels::candlestick::prepare_candlestick_data(
			&torn, "x", "open", "high", "low", "close",
		)
		.expect_err("probe's second row violates the high bound");
		assert!(matches!(
			error,
			crate::plot::kernels::candlestick::CandlestickPreparationError::UpperBoundViolation {
				row: 2,
				..
			}
		));
	}

	#[test]
	fn product_0144_checked_in_probe_has_repeated_but_not_total_cell_occupancy() {
		use crate::plot::kernels::hexbin_viewport::{
			HexLatticeSpec, HexRadiusPx, HexbinPreparationGeneration, HexbinPreparationRequest,
			MaterializedHexbinPreparer, prepare_hexbin_observations,
		};
		let source = include_str!("../../../../uat/hexbin_probe.sql");
		assert!(source.contains("DROP TABLE IF EXISTS hexbin_probe;"));
		assert!(source.contains("DROP TABLE IF EXISTS hexbin_probe_temporal;"));
		let frame = polars::df!(
			"x" => [0.00_f64, 0.01, 0.02, 0.01, 0.42, 0.43, 0.44, 0.43, 0.78, 0.79, 0.80, 1.00, 0.10, 1.00],
			"y" => [0.00_f64, 0.01, 0.01, 0.02, 0.38, 0.39, 0.38, 0.40, 0.82, 0.81, 0.83, 0.10, 1.00, 1.00],
		)
		.unwrap();
		let token = crate::result_artifact::ArtifactToken::mint();
		let observations =
			std::sync::Arc::new(prepare_hexbin_observations(&frame, "x", "y", token).unwrap());
		let preparer = MaterializedHexbinPreparer::new(std::sync::Arc::clone(&observations));
		let request = HexbinPreparationRequest {
			source_token: token,
			generation: HexbinPreparationGeneration::INITIAL,
			x_field: "x".to_owned(),
			y_field: "y".to_owned(),
			lattice: HexLatticeSpec::new(
				(0.0, 1.0),
				(0.0, 1.0),
				600.0,
				400.0,
				HexRadiusPx::DEFAULT,
			)
			.unwrap(),
		};
		let artifact = preparer.prepare(&request).unwrap();
		assert_eq!(artifact.total_count(), frame.height() as u64);
		assert!(artifact.max_count() > 1);
		assert!(artifact.max_count() < frame.height() as u64);
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0098_funnel_resolves_identity_and_attributes_every_refusal() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("funnel_probe").views([funnel("stage", "value")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("funnel source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"stage" => ["Visited", "Trial", "Paid"],
			"value" => [100.0_f64, 60.0, 30.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid funnel must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Funnel { stage: Some(stage), value }
				if stage == "stage" && value == "value"
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 600, 400);
		assert_eq!(
			kernel
				.legend_content(&crate::plot::common::ResolvedPlotSettings::default())
				.unwrap()
				.sections()
				.count(),
			1
		);

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid funnel rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!("stage" => [None::<&str>], "value" => [1.0]).unwrap(),
			"\"stage\"",
			"null at row 1",
		);
		assert_span(
			polars::df!("stage" => ["A", "A"], "value" => [2.0, 1.0]).unwrap(),
			"\"stage\"",
			"duplicated at rows 1 and 2",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [None::<f64>]).unwrap(),
			"\"value\"",
			"null at row 1",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [f64::NAN]).unwrap(),
			"\"value\"",
			"non-finite",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [-1.0]).unwrap(),
			"\"value\"",
			"negative",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [9_007_199_254_740_993_i64]).unwrap(),
			"\"value\"",
			"cannot round-trip",
		);
		assert_span(
			polars::df!("stage" => ["A"], "value" => [0.0]).unwrap(),
			"\"value\"",
			"must be positive at row 1",
		);
		assert_span(
			polars::df!("stage" => ["A", "B"], "value" => [1.0, 2.0]).unwrap(),
			"\"value\"",
			"rises between rows 1 and 2",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0100_radial_dial_resolves_identity_and_attributes_every_refusal() {
		use polars::frame::DataFrame;
		let grouped = r#"workspace.table("radial_dial_probe").views([radial_dial("value", "maximum", "label")])"#;
		let ungrouped =
			r#"workspace.table("radial_dial_probe").views([radial_dial("value", "maximum")])"#;
		let complete = |source: &str, frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("radial-dial source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"label" => ["Zulu", "Alpha"],
			"value" => [4.0_f64, 2.0],
			"maximum" => [8.0_f64, 10.0],
		)
		.unwrap();
		let outcome = complete(grouped, valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid radial dial must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::RadialDial {
				label: Some(label), value, maximum
			} if (label.as_str(), value.as_str(), maximum.as_str()) == ("label", "value", "maximum")
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());

		let assert_span = |source: &str, frame: DataFrame, expected: &str, fragment: &str| {
			let outcome = complete(source, frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid radial dial rendered")
			};
			assert!(error.contains(fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		for (frame, expected, fragment) in [
			(
				polars::df!("value" => [1.0], "maximum" => [2.0]).unwrap(),
				"\"label\"",
				"absent",
			),
			(
				polars::df!("label" => [1_i64], "value" => [1.0], "maximum" => [2.0]).unwrap(),
				"\"label\"",
				"incompatible",
			),
			(
				polars::df!("label" => ["A"], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"absent",
			),
			(
				polars::df!("label" => ["A"], "value" => ["bad"], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"incompatible with RadialDial",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0]).unwrap(),
				"\"maximum\"",
				"absent",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => ["bad"]).unwrap(),
				"\"maximum\"",
				"Quantitative",
			),
			(
				polars::df!("label" => [None::<&str>], "value" => [1.0], "maximum" => [2.0])
					.unwrap(),
				"\"label\"",
				"null at row 1",
			),
			(
				polars::df!("label" => ["A"], "value" => [None::<f64>], "maximum" => [2.0])
					.unwrap(),
				"\"value\"",
				"null at row 1",
			),
			(
				polars::df!("label" => ["A"], "value" => [f64::NAN], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"non-finite",
			),
			(
				polars::df!("label" => ["A"], "value" => [-1.0], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"negative",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => [None::<f64>])
					.unwrap(),
				"\"maximum\"",
				"null at row 1",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => [f64::INFINITY])
					.unwrap(),
				"\"maximum\"",
				"non-finite",
			),
			(
				polars::df!("label" => ["A"], "value" => [1.0], "maximum" => [0.0]).unwrap(),
				"\"maximum\"",
				"not positive",
			),
			(
				polars::df!("label" => ["A"], "value" => [3.0], "maximum" => [2.0]).unwrap(),
				"\"value\"",
				"exceeds its maximum",
			),
			(
				polars::df!("label" => ["A", "A"], "value" => [1.0, 1.0], "maximum" => [2.0, 2.0])
					.unwrap(),
				"\"label\"",
				"duplicated at rows 1 and 2",
			),
		] {
			assert_span(grouped, frame, expected, fragment);
		}
		assert_span(
			grouped,
			polars::df!(
				"label" => ["A"],
				"value" => [9_007_199_254_740_993_i64],
				"maximum" => [9_007_199_254_740_994_i64],
			)
			.unwrap(),
			"\"value\"",
			"exactly as f64",
		);
		assert_span(
			grouped,
			polars::df!(
				"label" => ["A"],
				"value" => [1_i64],
				"maximum" => [9_007_199_254_740_993_i64],
			)
			.unwrap(),
			"\"maximum\"",
			"exactly as f64",
		);
		assert_span(
			ungrouped,
			polars::df!("value" => [1.0, 1.0], "maximum" => [2.0, 2.0]).unwrap(),
			"radial_dial(\"value\", \"maximum\")",
			"more than one reading at row 2",
		);
		assert!(complete(grouped, valid).sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0103_pie_resolves_identity_and_attributes_slice_refusals() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("pie_probe").views([pie("category", "value")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("pie source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"category" => ["Zulu", "Alpha"],
			"value" => [4.0_f64, 2.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid pie must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Pie { category: Some(category), value }
				if category == "category" && value == "value"
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());

		let assert_span = |frame: DataFrame, expected: &str, fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid pie rendered")
			};
			assert!(error.contains(fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};
		assert_span(
			polars::df!("category" => [1_i64], "value" => [1.0]).unwrap(),
			"\"category\"",
			"incompatible with Pie",
		);
		assert_span(
			polars::df!("category" => ["A", "A"], "value" => [2.0, 1.0]).unwrap(),
			"\"category\"",
			".aggregate(",
		);
		assert_span(
			polars::df!("category" => ["A"], "value" => [f64::NAN]).unwrap(),
			"\"value\"",
			"non-finite",
		);
		assert_span(
			polars::df!("category" => ["A"], "value" => [0.0]).unwrap(),
			"\"value\"",
			"zero total",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0105_multivariate_resolution_keeps_dimension_and_modifier_attribution() {
		use polars::frame::DataFrame;
		let source =
			r#"workspace.table("radar_probe").views([radar("a", "b", "c").category("group")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("radar source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"group" => ["B", "A"],
			"a" => [1.0_f64, 2.0],
			"b" => [2.0_f64, 3.0],
			"c" => [3.0_f64, 4.0],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid radar must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Radar { dimensions, category }
				if dimensions == &["a", "b", "c"] && category.as_deref() == Some("group")
		));
		let snapshot = continuation.identity.persistence_snapshot();
		assert!(snapshot.assignments.is_empty());
		assert!(snapshot.spaces.is_empty());

		for (frame, expected, fragment) in [
			(
				polars::df!(
					"group" => ["A"], "a" => [1.0], "b" => [2.0], "c" => [None::<f64>]
				)
				.unwrap(),
				"\"c\"",
				"field `c` is null at row 1",
			),
			(
				polars::df!(
					"group" => [None::<&str>], "a" => [1.0], "b" => [2.0], "c" => [3.0]
				)
				.unwrap(),
				".category(\"group\")",
				"category field `group` is null at row 1",
			),
		] {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid radar rendered")
			};
			assert!(error.contains(fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		}
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0095_bubble_resolves_and_attributes_all_five_roles() {
		use polars::frame::DataFrame;
		let source = r#"workspace.table("bubble_probe").views([bubble("x", "y", "size", "color", "label")])"#;
		let complete = |frame: DataFrame| {
			let relation = transform_rune_view(source)
				.expect("bubble source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame, relation.views)
		};
		let valid = polars::df!(
			"x" => [1.0, 2.0, 3.0],
			"y" => [2_i64, 3, 4],
			"size" => [10.0, 100.0, 50.0],
			"color" => [-30.0, 40.0, 0.0],
			"label" => [Some("first"), None, Some("third")],
		)
		.unwrap();
		let outcome = complete(valid.clone());
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(resolved, _, continuation)) =
			&outcome.sinks[0].output
		else {
			panic!("valid bubble must resolve")
		};
		assert!(matches!(
			resolved.resolved_spec.kernel_fields.as_ref(),
			crate::plot::spec::ResolvedKernelFields::Bubble { x, y, size, color, label }
				if (x.as_str(), y.as_str(), size.as_str(), color.as_str(), label.as_deref())
					== ("x", "y", "size", "color", Some("label"))
		));
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let kernel =
			crate::plot::factory::create_plot(&resolved.resolved_spec, &resolved.data, 800, 480);
		let settings = crate::plot::common::ResolvedPlotSettings::default();
		let legend = kernel
			.legend_content(&settings)
			.expect("authored bubble has its combined legend");
		let sections = legend.sections().collect::<Vec<_>>();
		assert!(matches!(
			sections.as_slice(),
			[
				crate::plot::legend::LegendSection::Gradient { stops, .. },
				crate::plot::legend::LegendSection::SizeSample { low: 10.0, high: 100.0, .. }
			] if stops.first_value() == -30.0 && stops.last_value() == 40.0
		));
		assert!(crate::plot::legend::measure_legend(&legend, &settings).width > 250.0);

		let assert_span = |frame: DataFrame, expected: &str, error_fragment: &str| {
			let outcome = complete(frame);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("invalid bubble rendered")
			};
			assert!(error.contains(error_fragment), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(&source[span.start as usize..span.end as usize], expected);
		};

		assert_span(
			polars::df!(
				"x" => ["bad"], "y" => [2.0], "size" => [10.0], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"x\"",
			"incompatible with Bubble",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => ["bad"], "size" => [10.0], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"y\"",
			"incompatible with Bubble",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => ["bad"], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"size\"",
			"plot size field",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => ["bad"],
				"label" => ["first"],
			)
			.unwrap(),
			"\"color\"",
			"plot color field",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => [1.0],
				"label" => [7_i64],
			)
			.unwrap(),
			"\"label\"",
			"plot label field",
		);

		assert_span(
			polars::df!(
				"x" => [Some(1.0), None], "y" => [2.0, 3.0], "size" => [10.0, 20.0],
				"color" => [1.0, 2.0], "label" => [Some("first"), None],
			)
			.unwrap(),
			"\"x\"",
			"null at row 2",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [f64::NAN], "size" => [10.0], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"y\"",
			"non-finite at row 1",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [None::<f64>], "color" => [1.0],
				"label" => ["first"],
			)
			.unwrap(),
			"\"size\"",
			"null at row 1",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => [f64::INFINITY],
				"label" => ["first"],
			)
			.unwrap(),
			"\"color\"",
			"non-finite at row 1",
		);
		assert_span(
			polars::df!(
				"x" => [1.0], "y" => [2.0], "size" => [10.0], "color" => [1.0],
			)
			.unwrap(),
			"\"label\"",
			"absent",
		);
		assert!(complete(valid).sinks[0].output.is_ok());
	}

	#[test]
	#[cfg(toron_local)]
	fn product_0091_rune_continuous_intent_reaches_planning_and_modifier_diagnostics() {
		use crate::plot::colors::ColorTheme;
		use crate::plot::continuous_color::{ContinuousAnchor, ContinuousScaleIntent};
		use crate::plot::spec::ColorJob;

		let frame = polars::df!(
			"x" => ["a", "b"], "y" => ["q", "q"], "value" => [-50.0, 100.0],
		)
		.unwrap();
		let complete = |source: &str| {
			let relation = transform_rune_view(source)
				.expect("continuous colour source compiles")
				.into_relations()
				.next()
				.unwrap();
			rune_view_outcome(frame.clone(), relation.views)
		};
		let valid = r#"workspace.table("heat_probe").views([figure([
			diverging_palette("green_purples"),
			heatmap("x", "y", "value").diverging()
		])])"#;
		let outcome = complete(valid);
		let Ok(crate::gui::messages::RuneRelationSinkOutput::Plot(
			resolved,
			authored_settings,
			continuation,
		)) = &outcome.sinks[0].output
		else {
			panic!("diverging heatmap must resolve")
		};
		assert_eq!(
			resolved.resolved_spec.continuous_intent,
			Some(ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor: ContinuousAnchor::DefaultZero,
			})
		);
		assert_eq!(
			authored_settings.diverging_ramp,
			crate::plot::spec::Override::Set(ColorTheme::GreenPurples)
		);
		assert!(
			continuation
				.identity
				.persistence_snapshot()
				.assignments
				.is_empty()
		);
		let settings = crate::plot::theme::PlotSettings {
			authored_spec: authored_settings.clone(),
			..crate::plot::theme::PlotSettings::default()
		}
		.resolve();
		let kernel = create_plot(&resolved.resolved_spec, &resolved.data, 800, 480);
		let legend = kernel.legend_content(&settings).expect("diverging guide");
		let crate::plot::legend::LegendSection::Gradient { stops, .. } =
			legend.sections().next().expect("gradient section")
		else {
			panic!("heatmap guide is not a gradient")
		};
		assert_eq!(stops.values().collect::<Vec<_>>(), [-50.0, 0.0, 100.0]);
		assert_eq!(stops.sample_normalized(0.5), iced::Color::WHITE);

		for (modifier, fragment) in [
			("diverging_at(500.0)", "strictly inside"),
			("diverging_at(-50.0)", "strictly inside"),
		] {
			let source = format!(
				r#"workspace.table("heat_probe").views([heatmap("x", "y", "value").{modifier}])"#
			);
			let outcome = complete(&source);
			let Err(error) = &outcome.sinks[0].output else {
				panic!("out-of-domain anchor rendered")
			};
			assert!(error.contains(fragment), "{error}");
			assert!(!error.contains("plot anchor field"), "{error}");
			let span = outcome.sinks[0].span.clone();
			assert_eq!(
				&source[span.start as usize..span.end as usize],
				format!(".{modifier}")
			);
		}

		let constant = polars::df!("x" => ["a"], "y" => ["q"], "value" => [0.0]).unwrap();
		let complete_constant = |modifier: &str| {
			let source = format!(
				r#"workspace.table("heat_probe").views([heatmap("x", "y", "value").{modifier}])"#
			);
			let relation = transform_rune_view(&source)
				.expect("constant source compiles")
				.into_relations()
				.next()
				.unwrap();
			(source, rune_view_outcome(constant.clone(), relation.views))
		};
		assert!(complete_constant("diverging()").1.sinks[0].output.is_ok());
		let (source, outcome) = complete_constant("diverging_at(0.0)");
		let Err(error) = &outcome.sinks[0].output else {
			panic!("authored anchor on a degenerate domain rendered")
		};
		assert!(error.contains("degenerate domain"), "{error}");
		let span = outcome.sinks[0].span.clone();
		assert_eq!(
			&source[span.start as usize..span.end as usize],
			".diverging_at(0.0)"
		);
	}

	#[test]
	fn sql_error_keeps_full_detail_without_manufacturing_a_result_pane() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let raw = "SQLite parse error: sql parser error: Expected: an SQL statement, found: viz at Line: 1, Column: 1";

		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Err(raw.to_owned()),
				publish: None,
				generation: 0,
			},
		);

		assert_eq!(
			state.status_error,
			"SQL · line 1, col 1 · expected an SQL statement, found 'viz'"
		);
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.full.as_str()),
			Some(raw)
		);
		assert!(!state.code_editor.has_query_surface());
		state.code_editor.shutdown();
	}

	#[test]
	fn diagnostic_popover_content_is_selectable_but_read_only() {
		use iced::widget::text_editor::{Action, Edit};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let raw = "database refused the statement";
		let _ = update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Err(raw.to_owned()),
				publish: None,
				generation: 0,
			},
		);
		let _ = update(&mut state, Message::ToggleStatusDetail);
		assert!(state.status_detail_open);

		let _ = update(&mut state, Message::StatusDetailAction(Action::SelectAll));
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.and_then(|detail| detail.content.selection()),
			Some(raw.to_owned())
		);
		let _ = update(
			&mut state,
			Message::StatusDetailAction(Action::Edit(Edit::Insert('x'))),
		);
		assert_eq!(
			state
				.status_detail
				.as_ref()
				.map(|detail| detail.content.text()),
			Some(raw.to_owned())
		);
		state.code_editor.shutdown();
	}

	#[test]
	fn editor_activity_retires_transient_execution_feedback() {
		use crate::editor::EditorMsg;
		use libaryon::engine::{InputEvent, KeyCode, Modifiers};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		set_sql_error(&mut state, "syntax error".to_owned(), None);

		clear_transient_execution_status_on_editor_activity(
			&mut state,
			&EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char('j'),
				modifiers: Modifiers::NONE,
			}),
		);

		assert!(state.status_error.is_empty());
		assert!(state.status_detail.is_none());

		set_rune_error(&mut state, "Rune failed".to_owned());
		clear_transient_execution_status_on_editor_activity(
			&mut state,
			&EditorMsg::Input(InputEvent::Key {
				code: KeyCode::Char('k'),
				modifiers: Modifiers::NONE,
			}),
		);

		assert!(state.status_error.is_empty());
		assert!(state.status_detail.is_none());
		state.code_editor.shutdown();
	}

	fn profile_store_test_path(tag: &str) -> std::path::PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"polariton-0134-state-{tag}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).expect("temp dir");
		dir.join("connection_profiles.enc")
	}

	/// PRODUCT-0134 §8/§9: no prompt without a store-access request, and
	/// cancelling leaves the application fully usable — routine status, no
	/// error tone, no adapter connected, scratch not substituted.
	#[test]
	fn product_0134_unlock_prompts_only_on_request_and_cancel_is_routine() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let path = profile_store_test_path("cancel");
		state.profile_store_path = Some(path.clone());
		assert!(
			state.profile_unlock.is_none(),
			"a session that never accesses the store is never prompted"
		);

		drop(update(&mut state, Message::ProfileStoreAccessRequested));
		let prompt = state.profile_unlock.as_ref().expect("prompt opened");
		assert!(prompt.creating, "a missing store enters the creation flow");

		drop(update(&mut state, Message::ProfileUnlockCancelled));
		assert!(state.profile_unlock.is_none());
		assert!(
			state.status_error.is_empty(),
			"cancelling an unlock is not an error"
		);
		assert!(state.status_msg.contains("ad hoc"));
		assert!(state.adapter_state.connection.is_none());
		assert!(!path.exists(), "cancel writes nothing");
		let _ = std::fs::remove_dir_all(path.parent().unwrap());
		state.code_editor.shutdown();
	}

	/// PRODUCT-0134 §8: creation is a distinct flow — a mismatched double
	/// entry refuses without writing a file, because a mistyped creation
	/// passphrase would seal a store nobody can open.
	#[test]
	fn product_0134_creation_double_entry_mismatch_writes_no_file() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let path = profile_store_test_path("mismatch");
		state.profile_store_path = Some(path.clone());

		drop(update(&mut state, Message::ProfileStoreAccessRequested));
		drop(update(
			&mut state,
			Message::ProfilePassphraseChanged("one".into()),
		));
		drop(update(
			&mut state,
			Message::ProfilePassphraseConfirmationChanged("two".into()),
		));
		drop(update(&mut state, Message::ProfileUnlockSubmitted));
		let prompt = state.profile_unlock.as_ref().expect("prompt stays open");
		assert_eq!(prompt.error.as_deref(), Some("Passphrases do not match."));
		assert!(!path.exists(), "a mismatch writes no file");
		assert!(!state.profile_store.is_unlocked());

		drop(update(
			&mut state,
			Message::ProfilePassphraseConfirmationChanged("one".into()),
		));
		drop(update(&mut state, Message::ProfileUnlockSubmitted));
		assert!(state.profile_unlock.is_none(), "matching entries create");
		assert!(state.profile_store.is_unlocked());
		assert!(path.exists());
		let _ = std::fs::remove_dir_all(path.parent().unwrap());
		state.code_editor.shutdown();
	}

	/// PRODUCT-0134 §9: a wrong passphrase reports, keeps the prompt open for
	/// retry, and leaves the sealed store byte-identical.
	#[test]
	fn product_0134_wrong_passphrase_retries_and_the_store_survives() {
		let path = profile_store_test_path("retry");
		let mut seeded = crate::profile_store::ProfileStore::default();
		seeded
			.initialize(&path, zeroize::Zeroizing::new(b"right".to_vec()))
			.expect("seed store");
		drop(seeded);
		let sealed = std::fs::read(&path).expect("sealed bytes");

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.profile_store_path = Some(path.clone());
		drop(update(&mut state, Message::ProfileStoreAccessRequested));
		assert!(
			!state.profile_unlock.as_ref().expect("prompt").creating,
			"an existing store enters the unlock flow"
		);
		drop(update(
			&mut state,
			Message::ProfilePassphraseChanged("wrong".into()),
		));
		drop(update(&mut state, Message::ProfileUnlockSubmitted));
		let prompt = state.profile_unlock.as_ref().expect("prompt stays open");
		assert!(prompt.error.is_some(), "the failure is reported");
		assert!(!state.profile_store.is_unlocked());
		assert_eq!(
			std::fs::read(&path).expect("sealed bytes"),
			sealed,
			"a failed open never touches the store file"
		);

		drop(update(
			&mut state,
			Message::ProfilePassphraseChanged("right".into()),
		));
		drop(update(&mut state, Message::ProfileUnlockSubmitted));
		assert!(state.profile_unlock.is_none(), "retry succeeds");
		assert!(state.profile_store.is_unlocked());
		let _ = std::fs::remove_dir_all(path.parent().unwrap());
		state.code_editor.shutdown();
	}

	/// PRODUCT-0134 stage 4: a profile saved in one process is selectable and
	/// connects in the next. The pending operation survives the unlock prompt,
	/// `from_saved` is the one reconstruction path, and the status identity is
	/// the profile name.
	#[test]
	fn product_0134_profile_saved_in_one_process_connects_in_the_next() {
		let path = profile_store_test_path("cross-process");

		let (mut first, startup) = new(StartupData::default(), None);
		drop(startup);
		first.profile_store_path = Some(path.clone());
		first
			.profile_store
			.initialize(&path, zeroize::Zeroizing::new(b"pw".to_vec()))
			.expect("create store");
		drop(update(
			&mut first,
			Message::AdapterSelected(crate::adapters::driver::AdapterSelection::SQLite),
		));
		drop(update(
			&mut first,
			Message::ConnectionNameChanged("local-mem".into()),
		));
		drop(update(
			&mut first,
			Message::AdapterConfigurationChanged("connection_string".into(), "memory".into()),
		));
		drop(update(&mut first, Message::ProfileSaveRequested));
		assert_eq!(first.status_msg, "Profile saved: local-mem");
		assert_eq!(first.editing_profile.as_deref(), Some("local-mem"));
		first.code_editor.shutdown();
		drop(first);

		// The next process: locked store, connect drives the unlock prompt,
		// and the pending operation applies on success.
		let (mut second, startup) = new(StartupData::default(), None);
		drop(startup);
		second.profile_store_path = Some(path.clone());
		drop(update(
			&mut second,
			Message::ProfileConnectRequested("local-mem".into()),
		));
		assert!(second.profile_unlock.is_some(), "locked store prompts");
		drop(update(
			&mut second,
			Message::ProfilePassphraseChanged("pw".into()),
		));
		drop(update(&mut second, Message::ProfileUnlockSubmitted));
		assert!(second.profile_unlock.is_none());
		assert!(matches!(
			&second.adapter_state.configuration,
			crate::adapters::driver::AdapterConfiguration::SQLite { connection_string }
				if connection_string == "memory"
		));
		assert!(matches!(
			second.adapter_state.stage,
			crate::adapters::common::AdapterStage::Configured
		));
		assert_eq!(second.adapter_state.connection_identity(), "local-mem");

		// The reconstructed configuration genuinely connects.
		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		runtime
			.block_on(crate::adapters::driver::AdapterState::connect(
				second.adapter_state.configuration.clone(),
			))
			.expect("saved profile connects");
		let _ = std::fs::remove_dir_all(path.parent().unwrap());
		second.code_editor.shutdown();
	}

	/// PRODUCT-0134 §1/§4 at the surface: saving with the ad hoc kind active
	/// refuses; a create onto an existing name refuses after `connect:new`
	/// cleared the editing identity; deleting removes the profile from the
	/// store (and with it the menu listing, which renders `names()`).
	#[test]
	fn product_0134_save_refusals_and_delete_follow_the_identity_rules() {
		let path = profile_store_test_path("surface-rules");
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.profile_store_path = Some(path.clone());
		state
			.profile_store
			.initialize(&path, zeroize::Zeroizing::new(b"pw".to_vec()))
			.expect("create store");

		drop(update(
			&mut state,
			Message::AdapterSelected(crate::adapters::driver::AdapterSelection::SQLite),
		));
		drop(update(
			&mut state,
			Message::ConnectionNameChanged("scratchpad".into()),
		));
		drop(update(
			&mut state,
			Message::AdapterConfigurationChanged("connection_string".into(), "memory".into()),
		));

		drop(update(
			&mut state,
			Message::AdapterAdHocChanged("postgresql://u@h/db".into()),
		));
		drop(update(&mut state, Message::ProfileSaveRequested));
		assert!(
			state.status_error.contains("named fields"),
			"a profile is never a connection string: {}",
			state.status_error
		);
		drop(update(
			&mut state,
			Message::AdapterAdHocChanged(String::new()),
		));

		drop(update(&mut state, Message::ProfileSaveRequested));
		assert_eq!(state.status_msg, "Profile saved: scratchpad");

		// connect:new is session-scoped and unsaved: it clears the editing
		// identity, so saving the same name again is a create, which refuses.
		drop(update(&mut state, Message::Connect));
		drop(update(&mut state, Message::Connect));
		assert!(state.editing_profile.is_none());
		drop(update(
			&mut state,
			Message::AdapterSelected(crate::adapters::driver::AdapterSelection::SQLite),
		));
		drop(update(
			&mut state,
			Message::ConnectionNameChanged("scratchpad".into()),
		));
		drop(update(
			&mut state,
			Message::AdapterConfigurationChanged("connection_string".into(), "memory".into()),
		));
		drop(update(&mut state, Message::ProfileSaveRequested));
		assert!(
			state.status_error.contains("already exists"),
			"create never silently replaces: {}",
			state.status_error
		);

		drop(update(
			&mut state,
			Message::ProfileDeleteRequested("scratchpad".into()),
		));
		assert_eq!(state.status_msg, "Profile deleted: scratchpad");
		assert!(state.profile_store.names().expect("names").is_empty());
		let _ = std::fs::remove_dir_all(path.parent().unwrap());
		state.code_editor.shutdown();
	}

	/// PRODUCT-0134 §4: rename is edit plus a changed Name — the original
	/// identity is renamed, never duplicated, and renaming onto an existing
	/// name refuses.
	#[test]
	fn product_0134_edit_with_a_changed_name_renames_the_profile() {
		let path = profile_store_test_path("rename");
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.profile_store_path = Some(path.clone());
		state
			.profile_store
			.initialize(&path, zeroize::Zeroizing::new(b"pw".to_vec()))
			.expect("create store");
		drop(update(
			&mut state,
			Message::AdapterSelected(crate::adapters::driver::AdapterSelection::SQLite),
		));
		drop(update(
			&mut state,
			Message::ConnectionNameChanged("old-name".into()),
		));
		drop(update(
			&mut state,
			Message::AdapterConfigurationChanged("connection_string".into(), "memory".into()),
		));
		drop(update(&mut state, Message::ProfileSaveRequested));
		assert_eq!(state.status_msg, "Profile saved: old-name");

		drop(update(
			&mut state,
			Message::ProfileEditRequested("old-name".into()),
		));
		assert_eq!(state.editing_profile.as_deref(), Some("old-name"));
		drop(update(
			&mut state,
			Message::ConnectionNameChanged("new-name".into()),
		));
		drop(update(&mut state, Message::ProfileSaveRequested));
		assert_eq!(state.status_msg, "Profile saved: new-name");
		assert_eq!(state.profile_store.names().expect("names"), ["new-name"]);
		let _ = std::fs::remove_dir_all(path.parent().unwrap());
		state.code_editor.shutdown();
	}

	/// PRODUCT-0135 §4, edge one plus the lock-waiter gate: a task whose
	/// generation was captured before it awaited the adapter lock never
	/// dispatches once that generation is stale.
	#[test]
	fn product_0135_stale_generation_task_never_dispatches() {
		use crate::adapters::common::DatabaseAdapter;
		use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

		struct CountingAdapter(std::sync::Arc<AtomicUsize>);
		#[async_trait::async_trait]
		impl DatabaseAdapter for CountingAdapter {
			async fn dispatch(&mut self, _code: &str) -> ExecutionResult {
				self.0.fetch_add(1, Ordering::SeqCst);
				ExecutionResult::Affected(1)
			}
		}

		let runtime = tokio::runtime::Runtime::new().expect("runtime");
		let dispatches = std::sync::Arc::new(AtomicUsize::new(0));
		let db: std::sync::Arc<tokio::sync::RwLock<dyn DatabaseAdapter>> = std::sync::Arc::new(
			tokio::sync::RwLock::new(CountingAdapter(dispatches.clone())),
		);

		// The run captured generation 0; cancellation advanced to 1 while the
		// task waited. It must die at the gate.
		let current = std::sync::Arc::new(AtomicU64::new(1));
		let result = runtime.block_on(super::dispatch_sql_gated(
			db.clone(),
			"select 1".into(),
			0,
			current.clone(),
			"sqlite".to_owned(),
		));
		assert!(matches!(result, ExecutionResult::None));
		assert_eq!(dispatches.load(Ordering::SeqCst), 0, "no dispatch");

		// A fresh generation dispatches normally.
		let result = runtime.block_on(super::dispatch_sql_gated(
			db,
			"select 1".into(),
			1,
			current,
			"sqlite".to_owned(),
		));
		assert!(matches!(result, ExecutionResult::Affected(1)));
		assert_eq!(dispatches.load(Ordering::SeqCst), 1);
	}

	/// PRODUCT-0135 §4: a cancelled generation's late result changes nothing —
	/// status, timer, grid size, and the named claim each asserted, for both
	/// completion paths.
	#[test]
	fn product_0135_late_results_from_a_cancelled_generation_change_nothing() {
		use libaryon::uast::projection::{
			MarkdownCellCodeVisibility, MarkdownCellIdentity, MarkdownCellOutputTarget,
			MarkdownCellPresentation,
		};
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);

		let claim = state.result_registry.begin(
			"late",
			libaryon::core::BufferId::new(7),
			"scratch".to_owned(),
		);
		super::cancel_sql_generation(&mut state);

		let frame = polars::df!("x" => [1_i32]).expect("frame");
		drop(update(
			&mut state,
			Message::RunResult {
				result: ExecutionResult::Rows {
					artifact: crate::result_artifact::ResultArtifact::test_frame(frame.clone()),
					statements: 1,
				},
				publish: Some(claim),
				generation: 0,
			},
		));
		assert!(state.status_msg.is_empty(), "no status effect");
		assert!(state.status_time_elapsed.is_none(), "no timer effect");
		assert!(state.status_df_size.is_none(), "no grid effect");
		assert!(
			state.result_registry.provenance("late").is_none(),
			"the cancelled claim concludes unbound, as a failure does"
		);

		drop(update(
			&mut state,
			Message::RunMarkdownCellResult {
				identity: MarkdownCellIdentity::Explicit {
					id: "cell".to_owned(),
				},
				presentation: MarkdownCellPresentation {
					output: MarkdownCellOutputTarget::Inline,
					code: MarkdownCellCodeVisibility::Show,
				},
				source_start_line: 1,
				result: ExecutionResult::Rows {
					artifact: crate::result_artifact::ResultArtifact::test_frame(frame),
					statements: 1,
				},
				generation: 0,
			},
		));
		assert!(state.status_msg.is_empty());
		assert!(state.status_time_elapsed.is_none());
		assert!(state.status_df_size.is_none());
		assert!(
			state.code_editor.named_notebook_cell_data().is_empty(),
			"a cancelled cell result never reaches the notebook"
		);
		state.code_editor.shutdown();
	}

	fn idle_abort_handle() -> iced::task::Handle {
		let (_task, handle): (iced::Task<Message>, iced::task::Handle) =
			iced::Task::perform(std::future::ready(()), |()| Message::CancelRun).abortable();
		handle
	}

	/// PRODUCT-0135 §1/§4/§5: each capability's outcome, in the routine
	/// status lane — and Refusal does nothing at all except say so.
	#[test]
	fn product_0135_cancel_outcomes_follow_the_capability() {
		use crate::adapters::common::{CancelCapability, DatabaseAdapter, EngineCancel};

		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);

		// Nothing in flight: routine feedback, no generation movement.
		let before = state.current_sql_generation();
		drop(update(&mut state, Message::CancelRun));
		assert_eq!(state.status_msg, "SQL · nothing to cancel");
		assert_eq!(state.current_sql_generation(), before);

		// Stopped waiting: no external connection, so the scratch row governs.
		state.sql_abort = Some(idle_abort_handle());
		drop(update(&mut state, Message::CancelRun));
		assert_eq!(state.current_sql_generation(), before + 1);
		assert!(state.sql_abort.is_none(), "the client future was abandoned");
		assert!(
			state.status_msg.contains("stopped waiting"),
			"{}",
			state.status_msg
		);
		assert!(state.status_error.is_empty(), "routine lane, not an error");

		// Refusal: nothing moves — no advance, no abort, no drain.
		struct InertAdapter;
		#[async_trait::async_trait]
		impl DatabaseAdapter for InertAdapter {
			async fn dispatch(&mut self, _code: &str) -> ExecutionResult {
				ExecutionResult::None
			}
		}
		state.adapter_state.connection =
			Some(std::sync::Arc::new(tokio::sync::RwLock::new(InertAdapter)));
		state.adapter_state.cancellation = CancelCapability::Refuse;
		state.sql_abort = Some(idle_abort_handle());
		state.pending_runs.push_back(super::PendingRun {
			snapshot_request: 9,
			source_buffer: libaryon::core::BufferId::new(9),
			language_id: libaryon::engine::supported_languages()
				.into_iter()
				.map(|language| language.id)
				.find(|id| id.as_str() == "sql")
				.expect("sql language registered"),
			kind: super::PendingRunKind::Buffer,
		});
		let before = state.current_sql_generation();
		drop(update(&mut state, Message::CancelRun));
		assert_eq!(state.current_sql_generation(), before, "no advance");
		assert!(state.sql_abort.is_some(), "no abort");
		assert_eq!(state.pending_runs.len(), 1, "no drain");
		assert!(
			state.status_msg.contains("cannot be cancelled"),
			"{}",
			state.status_msg
		);
		assert!(state.status_error.is_empty());
		state.pending_runs.clear();

		// Engine: the generation advances before the signal task is even
		// built (edge two), and the settled outcomes use the honest wording.
		struct RecordingCancel(std::sync::Arc<std::sync::atomic::AtomicUsize>);
		#[async_trait::async_trait]
		impl EngineCancel for RecordingCancel {
			async fn signal(&self) -> Result<(), String> {
				self.0.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
				Ok(())
			}
		}
		let signals = std::sync::Arc::new(std::sync::atomic::AtomicUsize::new(0));
		state.adapter_state.cancellation =
			CancelCapability::Engine(std::sync::Arc::new(RecordingCancel(signals.clone())));
		state.sql_abort = Some(idle_abort_handle());
		let before = state.current_sql_generation();
		let task = update(&mut state, Message::CancelRun);
		assert_eq!(
			state.current_sql_generation(),
			before + 1,
			"the generation advanced before the signal task was returned"
		);
		assert_eq!(state.status_msg, "SQL · cancelling…");
		assert_eq!(task.units(), 1, "the engine signal rides one task");

		drop(update(&mut state, Message::CancelSignalSettled(Ok(()))));
		assert_eq!(state.status_msg, "SQL · cancelled");
		drop(update(
			&mut state,
			Message::CancelSignalSettled(Err("connection reset".to_owned())),
		));
		assert!(
			state.status_msg.contains("stopped waiting"),
			"{}",
			state.status_msg
		);
		assert!(state.status_error.is_empty());
		state.code_editor.shutdown();
	}

	/// PRODUCT-0135 §4/§5: the host half of cancel advances the generation and
	/// drains the undispatched queue.
	#[test]
	fn product_0135_cancel_advances_generation_and_drains_the_queue() {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		state.pending_runs.push_back(super::PendingRun {
			snapshot_request: 1,
			source_buffer: libaryon::core::BufferId::new(7),
			language_id: libaryon::engine::supported_languages()
				.into_iter()
				.map(|language| language.id)
				.find(|id| id.as_str() == "sql")
				.expect("sql language registered"),
			kind: super::PendingRunKind::Buffer,
		});
		let before = state.current_sql_generation();
		super::cancel_sql_generation(&mut state);
		assert_eq!(state.current_sql_generation(), before + 1);
		assert!(state.pending_runs.is_empty());
		state.code_editor.shutdown();
	}
}

#[cfg(test)]
mod product_0159_probe_tests {
	use super::*;

	fn ms(value: u64) -> Duration {
		Duration::from_millis(value)
	}

	/// Delayed and skipped delivery under both tokio conventions: the stall
	/// is measured once, at its true length, and never accumulates.
	#[test]
	fn product_0159_probe_measures_a_stall_once_under_either_tick_convention() {
		let base = Instant::now();
		// Convention A: the tick delivered after a stall carries its original,
		// missed deadline, so lateness is the stall.
		let mut a = LoopLatencyProbe::new(base);
		for n in 1..=10_u64 {
			assert!(
				a.observe(base + ms(16 * n), base + ms(16 * n + 1))
					.is_none() || n == 1
			);
		}
		let stalled = a.observe(base + ms(16 * 11), base + ms(16 * 11 + 2_000));
		assert_eq!(stalled, Some(ms(2_000)));
		// After the stall the timer skips to the next multiple after now; on-time
		// ticks from there set no new maximum and the maximum stays the stall.
		for n in 0..1_000_u64 {
			let scheduled = base + ms(16 * 11 + 2_000 + 16 * (n + 1));
			assert!(a.observe(scheduled, scheduled + ms(1)).is_none());
		}
		assert_eq!(a.max_unserviced, ms(2_000));
		assert_eq!(a.over_100ms, 1);

		// Convention B: the tick delivered after a stall carries the rescheduled
		// instant near now, so lateness is small and the skipped gap is the stall.
		let mut b = LoopLatencyProbe::new(base);
		b.observe(base + ms(16), base + ms(17));
		let stalled = b.observe(base + ms(16 + 2_000), base + ms(16 + 2_001));
		// The gap is 2,000 ms minus one period, net of the previous tick's
		// 1 ms lateness.
		assert_eq!(stalled, Some(ms(2_000 - 16 - 1)));
		for n in 1..=1_000_u64 {
			let scheduled = base + ms(16 + 2_000 + 16 * n);
			assert!(b.observe(scheduled, scheduled + ms(1)).is_none());
		}
		assert_eq!(b.max_unserviced, ms(2_000 - 16 - 1));
		assert_eq!(b.over_100ms, 1);
	}

	/// The first tick is reported against arming and counts toward the
	/// maximum beyond one period: an on-time first tick sets nothing, a late
	/// first tick counts, and a subscription that started late with an
	/// on-time first tick counts the startup delay.
	#[test]
	fn product_0159_probe_first_tick_counts_startup_delay() {
		let base = Instant::now();
		let mut probe = LoopLatencyProbe::new(base);
		assert!(probe.observe(base + ms(16), base + ms(16)).is_none());
		assert_eq!(probe.first_handled, Some(ms(16)));
		assert_eq!(probe.max_unserviced, Duration::ZERO);
		let mut late = LoopLatencyProbe::new(base);
		assert_eq!(late.observe(base + ms(16), base + ms(316)), Some(ms(300)));
		// Delayed subscription startup: the timer began two seconds after
		// arming and its first tick arrived exactly on its own schedule.
		let mut stalled_start = LoopLatencyProbe::new(base);
		assert_eq!(
			stalled_start.observe(base + ms(2_016), base + ms(2_016)),
			Some(ms(2_000))
		);
		assert_eq!(stalled_start.over_100ms, 1);
		for n in 1..=100_u64 {
			let scheduled = base + ms(2_016 + 16 * n);
			assert!(stalled_start.observe(scheduled, scheduled).is_none());
		}
		assert_eq!(stalled_start.max_unserviced, ms(2_000));
	}
}

/// PRODUCT-0158: the driver against host panels this test fills itself.
#[cfg(all(test, feature = "toron"))]
mod product_0158_driver_tests {
	use super::tests::complete_relation_in_head;
	use super::{StartupData, new};
	use crate::driver::{Command, FigureRef, Outcome, Refusal, Session};
	use crate::gui::HeadlessApplication;
	use crate::gui::messages::Message;
	use crate::gui::plot_pane::{HostPlotState, PlotPaneMsg};
	use crate::plot::series::SeriesVisibilityAction;
	use crate::plot::theme::PlotSettingsLayer;
	use crate::rune_relation_envelope::{PendingRuneView, transform as transform_rune_view};
	use libaryon::core::BufferId;
	use std::sync::Arc;
	use std::time::Duration;

	const SOURCE: &str = r#"
let rows = workspace
	.table("trio")
	.sort([asc(col("period")), asc(col("series"))]);
rows.views([
	view("lines", figure([
		title("Trio"),
		line("period", "value", "series"),
	])),
]);
"#;

	fn frame() -> polars::frame::DataFrame {
		polars::df!(
			"period" => [1_i64, 2, 1, 2, 1, 2],
			"value" => [1.0, 2.0, 3.0, 4.0, 5.0, 6.0],
			"series" => ["a", "a", "b", "b", "c", "c"]
		)
		.expect("frame")
	}

	/// Run `SOURCE` at `revision` into the application's host panels, as the
	/// dogfood tests do, and return the panel ids in view order.
	fn run_source(state: &mut super::AppState, source_buffer: BufferId, revision: u64) {
		let program = transform_rune_view(SOURCE).expect("view program");
		let presentation = program
			.presentation_views()
			.into_iter()
			.map(|view| (view.identity.clone(), view.intent.form()))
			.collect::<Vec<_>>();
		state.rune_buffer_revisions.insert(source_buffer, revision);
		state
			.code_editor
			.prepare_rune_view_panels(source_buffer, &presentation, revision);
		assert!(
			state
				.rune_view_scheduler
				.submit(PendingRuneView {
					source_buffer,
					revision,
					view: program.clone(),
				})
				.is_some()
		);
		for relation in program.into_relations() {
			complete_relation_in_head(state, source_buffer, revision, relation, frame());
		}
	}

	pub(super) fn session() -> (Session, BufferId) {
		let (mut state, startup) = new(StartupData::default(), None);
		drop(startup);
		let source_buffer = BufferId::new(80);
		run_source(&mut state, source_buffer, 1);
		let mut application = HeadlessApplication::from_state(state).expect("headless");
		application
			.settle_until(Duration::from_secs(20), |state| {
				state.code_editor.host_plot_exports().len() == 1
			})
			.expect("the view's pane projects");
		(Session::new(application), source_buffer)
	}

	/// Orphan exclusion is shared, not local-only: a Toron view pane its
	/// source stopped publishing is described as orphaned and not published.
	#[test]
	fn a_toron_view_orphaned_by_its_source_is_labelled_and_not_published() {
		use crate::rune_view::{LocalViewIdentity, ViewPresentation};
		let (mut session, buffer) = session();
		let fresh = session.apply(&Command::Describe {}).figures.unwrap();
		assert_eq!(fresh.len(), 1);
		assert_eq!(fresh[0].status, "fresh");

		// A rerun whose program no longer declares the view orphans its pane.
		let application = session.application_mut();
		application
			.state_mut()
			.code_editor
			.prepare_rune_view_panels(buffer, &[] as &[(LocalViewIdentity, ViewPresentation)], 2);
		application
			.settle_until(Duration::from_secs(20), |state| {
				state
					.code_editor
					.host_plot_exports()
					.iter()
					.all(|export| export.status == "orphaned")
			})
			.unwrap();
		let orphaned = session.apply(&Command::Describe {}).figures.unwrap();
		assert_eq!(orphaned.len(), 1, "the orphan stays on screen");
		assert_eq!(orphaned[0].status, "orphaned");

		let directory = std::env::temp_dir().join(format!(
			"polariton-toron-orphan-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		let receipt = session.apply(&Command::Publish {
			directory: directory.display().to_string(),
		});
		assert_eq!(receipt.outcome, Outcome::Refused, "{receipt:?}");
		assert!(!directory.exists());
	}

	#[test]
	fn product_0160_baseline_precedes_client_and_is_replaced_on_rerun() {
		let (mut first, buffer) = session();
		let initial = first.loaded()[0].clone();
		first.apply(&Command::Hide {
			figure: FigureRef::Incarnation(initial.figure.clone()),
			series: 1,
		});
		// Joining after a human/agent edit must not relabel live state as loaded.
		let mut joined = Session::new(first.into_application());
		let snapshot = joined.apply(&Command::Describe {}).figures.unwrap();
		assert_eq!(snapshot[0].loaded, initial.loaded);
		assert_eq!(snapshot[0].live.hidden, vec![1]);
		let application = joined.application_mut();
		run_source(application.state_mut(), buffer, 2);
		application
			.settle_until(Duration::from_secs(20), |state| {
				state
					.code_editor
					.host_plot_exports_with_identity()
					.iter()
					.any(|(_, incarnation, _)| incarnation.token().to_string() != initial.figure)
			})
			.unwrap();
		let snapshot = application.describe_surface();
		assert_ne!(snapshot[0].figure, initial.figure);
		assert!(snapshot[0].loaded.as_ref().unwrap().hidden.is_empty());
		assert_eq!(
			snapshot[0].loaded.as_ref().unwrap().size,
			snapshot[0].live.size
		);
		assert!(snapshot[0].loaded.as_ref().unwrap().size.width > 0.0);
	}

	#[test]
	fn product_0160_same_view_name_in_two_sources_requires_an_incarnation() {
		let (mut session, _) = session();
		for by_name in [true, false] {
			let mut exports = session.application_mut().exports_with_identity();
			exports[0].2.view_name = Some("shared-name".into());
			let mut second = session.application_mut().exports_with_identity().remove(0);
			second.2.view_name = Some("shared-name".into());
			second.1 = crate::selection::SurfaceIncarnation::mint();
			let token = second.1.token().to_string();
			exports.push(second);
			let reference = if by_name {
				FigureRef::View("shared-name".into())
			} else {
				FigureRef::Incarnation(token)
			};
			let result = super::actions::resolve_figure(exports, &Default::default(), &reference);
			if by_name {
				assert!(
					matches!(result, Err(Refusal::AmbiguousFigure { candidates, .. }) if candidates.len() == 2)
				);
			} else {
				assert!(result.is_ok());
			}
		}
	}

	fn live_command(session: &mut Session, command: Command, id: u64) -> Vec<crate::live::Event> {
		let app = session.application_mut();
		let (message, _sink, mut answers) = app.state().agent_request_for_test(command, id);
		app.dispatch(message);
		let mut events = Vec::new();
		app.settle_until(Duration::from_secs(10), |_| {
			while let Ok(event) = answers.try_recv() {
				events.push(event);
			}
			events.iter().any(crate::live::Event::is_final)
		})
		.expect("request has a final answer");
		events
	}

	#[test]
	fn product_0160_human_and_agent_hide_commute_on_the_legend_path() {
		for human_first in [false, true] {
			let (mut session, _) = session();
			let root = session
				.application_mut()
				.state_mut()
				.enable_agents_for_test();
			let (panel, incarnation, export) =
				session.application_mut().exports_with_identity().remove(0);
			let widget = crate::publication::widget_for(&export, &export.settings_model);
			let identity = widget.series_registry().identities()[1].clone();
			let human = Message::CodeEditEvent(crate::editor::EditorMsg::HostPlot {
				panel_id: panel,
				message: PlotPaneMsg::SeriesVisibility(SeriesVisibilityAction::Hide(identity)),
			});
			if human_first {
				session.application_mut().dispatch(human.clone());
			}
			let events = live_command(
				&mut session,
				Command::Hide {
					figure: FigureRef::Incarnation(incarnation.token().to_string()),
					series: 1,
				},
				1,
			);
			assert!(matches!(
				&events[0],
				crate::live::Event::Applied { terminal: true, .. }
			));
			if !human_first {
				session.application_mut().dispatch(human);
			}
			let figures = session.application_mut().describe_surface();
			assert_eq!(figures[0].live.hidden, vec![1]);
			assert!(figures[0].loaded.as_ref().unwrap().hidden.is_empty());
			drop(session);
			std::fs::remove_dir_all(root).unwrap();
		}
	}

	#[test]
	fn product_0160_live_actions_and_publish_share_batch_receipts() {
		use crate::live::Event;
		let (mut session, _) = session();
		let root = session
			.application_mut()
			.state_mut()
			.enable_agents_for_test();
		let token = session.loaded()[0].figure.clone();
		for (id, command) in [
			Command::Hide {
				figure: FigureRef::Incarnation(token.clone()),
				series: 1,
			},
			Command::Show {
				figure: FigureRef::Incarnation(token.clone()),
				series: 1,
			},
			Command::Isolate {
				figure: FigureRef::Incarnation(token.clone()),
				series: 2,
			},
			Command::Restore {
				figure: FigureRef::Incarnation(token.clone()),
			},
			Command::Describe {},
		]
		.into_iter()
		.enumerate()
		{
			let events = live_command(&mut session, command.clone(), id as u64 + 1);
			let Event::Applied {
				terminal: true,
				receipt,
				..
			} = &events[0]
			else {
				panic!("{events:?}")
			};
			// Each verb is idempotent here. The batch call reports exactly the
			// same state and shape from the same shared action path.
			assert_eq!(*receipt, session.apply(&command));
		}
		let events = live_command(
			&mut session,
			Command::Hide {
				figure: FigureRef::Incarnation(token),
				series: 99,
			},
			10,
		);
		assert!(
			matches!(&events[0], Event::Refused { receipt: Some(receipt), .. } if matches!(receipt.refusal, Some(Refusal::UnknownSeries { .. })))
		);
		let live_root = root.join("live-package");
		let batch_root = root.join("batch-package");
		let events = live_command(
			&mut session,
			Command::Publish {
				directory: live_root.display().to_string(),
			},
			11,
		);
		assert!(matches!(
			&events[0],
			Event::Applied {
				terminal: false,
				..
			}
		));
		let Event::Completed { receipt, .. } = events.last().unwrap() else {
			panic!("{events:?}")
		};
		let live = receipt.publish.as_ref().unwrap();
		let batch = session
			.apply(&Command::Publish {
				directory: batch_root.display().to_string(),
			})
			.publish
			.unwrap();
		assert_eq!(live.digest, batch.digest);
		assert_eq!(live.figures, batch.figures);
		for file in walk(&live_root) {
			let relative = file.strip_prefix(&live_root).unwrap();
			if relative != std::path::Path::new("manifest.json") {
				assert_eq!(
					std::fs::read(&file).unwrap(),
					std::fs::read(batch_root.join(relative)).unwrap()
				);
			}
		}
		// A real write failure after admission reports the attempted root.
		let blocked = root.join("regular-file");
		std::fs::write(&blocked, "occupied").unwrap();
		let attempted = blocked.join("package");
		let events = live_command(
			&mut session,
			Command::Publish {
				directory: attempted.display().to_string(),
			},
			12,
		);
		assert!(matches!(
			&events[0],
			Event::Applied {
				terminal: false,
				..
			}
		));
		assert!(
			matches!(events.last().unwrap(), Event::Failed { root: Some(actual), .. } if actual == &attempted.display().to_string())
		);
		drop(session);
		std::fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn product_0160_resize_finishes_supersedes_and_survives_disconnect() {
		use crate::live::Event;
		let (mut session, buffer) = session();
		let root = session
			.application_mut()
			.state_mut()
			.enable_agents_for_test();
		let initial = session.loaded()[0].clone();
		let command = |width, height| Command::Resize {
			figure: FigureRef::Incarnation(initial.figure.clone()),
			width,
			height,
		};
		let events = live_command(
			&mut session,
			command(initial.size.width, initial.size.height),
			1,
		);
		assert!(matches!(&events[0], Event::Applied { terminal: true, .. }));
		let events = live_command(
			&mut session,
			command(initial.size.width, initial.size.height / 2.0),
			2,
		);
		assert!(
			matches!(
				&events[0],
				Event::Applied {
					terminal: false,
					..
				}
			),
			"{events:?}"
		);
		assert!(
			matches!(events.last().unwrap(), Event::Completed { receipt, .. } if receipt.state.as_ref().unwrap().size.height != initial.size.height)
		);
		let app = session.application_mut();
		let before = app.describe_surface()[0].size;
		let (message, sink, mut answers) = app
			.state()
			.agent_request_for_test(command(before.width, before.height / 2.0), 3);
		drop(super::update(app.state_mut(), message));
		assert!(matches!(
			answers.try_recv().unwrap(),
			Event::Applied {
				terminal: false,
				..
			}
		));
		sink.close("test disconnect");
		app.settle_until(Duration::from_secs(10), |state| {
			state.describe_surface()[0].size.height != before.height
		})
		.unwrap();
		// Admission has sent a drag, but its projection has not been consumed.
		let before = app.describe_surface()[0].size;
		let (message, _sink, mut answers) = app
			.state()
			.agent_request_for_test(command(before.width, before.height + 100.0), 4);
		drop(super::update(app.state_mut(), message));
		assert!(matches!(
			answers.try_recv().unwrap(),
			Event::Applied {
				terminal: false,
				..
			}
		));
		run_source(app.state_mut(), buffer, 2);
		drop(super::update(
			app.state_mut(),
			Message::AgentTick(std::time::Instant::now()),
		));
		assert!(
			matches!(answers.try_recv().unwrap(), Event::Superseded { current: Some(current), .. } if current != initial.figure)
		);
		let events = live_command(
			&mut session,
			Command::Hide {
				figure: FigureRef::Incarnation(initial.figure),
				series: 0,
			},
			5,
		);
		assert!(
			matches!(&events[0], Event::Refused { receipt: Some(receipt), .. } if matches!(receipt.refusal, Some(Refusal::StaleFigure { .. })))
		);
		drop(session);
		std::fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn product_0160_resize_deadline_reports_observed_size_without_rollback() {
		use crate::live::Event;
		let (mut session, _) = session();
		let root = session
			.application_mut()
			.state_mut()
			.enable_agents_for_test();
		let initial = session.loaded()[0].clone();
		let app = session.application_mut();
		let (message, _sink, mut answers) = app.state().agent_request_for_test(
			Command::Resize {
				figure: FigureRef::Incarnation(initial.figure),
				width: initial.size.width,
				height: initial.size.height + 100.0,
			},
			1,
		);
		drop(super::update(app.state_mut(), message));
		assert!(matches!(
			answers.try_recv().unwrap(),
			Event::Applied {
				terminal: false,
				..
			}
		));
		// Do not poll the engine: no new size has been observed. Advance only
		// the completion clock, the same path the live timer delivers.
		drop(super::update(
			app.state_mut(),
			Message::AgentTick(std::time::Instant::now() + Duration::from_secs(6)),
		));
		assert!(
			matches!(answers.try_recv().unwrap(), Event::Failed { reason, receipt, .. }
			if reason == "resize_deadline" && receipt.state.as_ref().unwrap().size == initial.size)
		);
		drop(session);
		std::fs::remove_dir_all(root).unwrap();
	}

	/// PRODUCT-0158 gates 2 and 3: the loaded state carries no hidden set, the
	/// driver's actions leave the pane exactly where the legend's would, with
	/// the same idempotence and the same revision bumps, and an unknown series
	/// is refused with the table's bound.
	#[test]
	fn product_0158_actions_are_the_legends_and_loaded_is_not_live() {
		let (mut session, _) = session();
		let loaded = session.loaded().to_vec();
		assert_eq!(loaded.len(), 1);
		// The in-head completion installs the panel without its named-view
		// attachment, so these tests address the figure by incarnation; the
		// view form is exercised by the fixture drive in the evidence record.
		let figure = FigureRef::Incarnation(loaded[0].figure.clone());
		assert!(loaded[0].loaded.as_ref().unwrap().hidden.is_empty());
		assert!(loaded[0].live.hidden.is_empty());

		// The legend's own path, on the same kernel: PRODUCT-0156 gate 8's
		// sequences plus the repeated and unknown cases.
		let (_, _, export) = session
			.application_mut()
			.exports_with_identity()
			.pop()
			.expect("export");
		let ids = export.kernel.series_identities();
		assert_eq!(ids.len(), 3);
		let mut direct = HostPlotState::new(
			"lines".to_owned(),
			Arc::clone(&export.kernel),
			PlotSettingsLayer::default(),
		);
		let revision_at_start = export.render_revision;
		let direct_at_start = direct.render_revision;
		let steps: Vec<(Command, Option<SeriesVisibilityAction>)> = vec![
			(
				Command::Hide {
					figure: figure.clone(),
					series: 0,
				},
				Some(SeriesVisibilityAction::Hide(ids[0].clone())),
			),
			(
				Command::Hide {
					figure: figure.clone(),
					series: 0,
				},
				Some(SeriesVisibilityAction::Hide(ids[0].clone())),
			),
			(
				Command::Show {
					figure: figure.clone(),
					series: 0,
				},
				Some(SeriesVisibilityAction::Show(ids[0].clone())),
			),
			(
				Command::Isolate {
					figure: figure.clone(),
					series: 2,
				},
				Some(SeriesVisibilityAction::Isolate(ids[2].clone())),
			),
			(
				Command::Isolate {
					figure: figure.clone(),
					series: 9,
				},
				None,
			),
			(
				Command::Restore {
					figure: figure.clone(),
				},
				Some(SeriesVisibilityAction::RestoreAll),
			),
			(
				Command::Restore {
					figure: figure.clone(),
				},
				Some(SeriesVisibilityAction::RestoreAll),
			),
		];
		for (command, direct_action) in steps {
			let receipt = session.apply(&command);
			match direct_action {
				Some(action) => {
					direct.update(PlotPaneMsg::SeriesVisibility(action));
					assert_eq!(receipt.outcome, Outcome::Applied, "{command:?}");
					let expected = ids
						.iter()
						.enumerate()
						.filter(|(_, id)| direct.plot_settings.hidden_series.contains(id))
						.map(|(ordinal, _)| ordinal as u32)
						.collect::<Vec<_>>();
					assert_eq!(
						receipt.state.as_ref().map(|state| state.hidden.clone()),
						Some(expected),
						"{command:?}"
					);
				}
				None => {
					assert_eq!(receipt.outcome, Outcome::Refused);
					assert!(
						matches!(
							receipt.refusal,
							Some(Refusal::UnknownSeries {
								series: 9,
								series_count: 3,
								..
							})
						),
						"{:?}",
						receipt.refusal
					);
				}
			}
			let (_, _, live) = session
				.application_mut()
				.exports_with_identity()
				.pop()
				.expect("export");
			assert_eq!(
				live.render_revision - revision_at_start,
				direct.render_revision - direct_at_start,
				"revision bumps agree after {command:?}"
			);
		}
	}

	/// PRODUCT-0158 gate 3: a command carrying the incarnation of a figure
	/// that has since been replaced is refused with the current one; the view
	/// name still resolves.
	#[test]
	fn product_0158_stale_incarnations_are_refused_with_the_current_one() {
		let (mut session, source_buffer) = session();
		let old = session.loaded()[0].figure.clone();
		run_source(session.application_mut().state_mut(), source_buffer, 2);
		session
			.application_mut()
			.settle_until(Duration::from_secs(20), |state| {
				state
					.code_editor
					.host_plot_exports_with_identity()
					.first()
					.is_some_and(|(_, incarnation, _)| incarnation.token().to_string() != old)
			})
			.expect("the rerun installs a new incarnation");
		let receipt = session.apply(&Command::Hide {
			figure: FigureRef::Incarnation(old.clone()),
			series: 0,
		});
		assert_eq!(receipt.outcome, Outcome::Refused);
		let Some(Refusal::StaleFigure { figure, current }) = receipt.refusal else {
			panic!("{:?}", receipt.refusal);
		};
		assert_eq!(figure, FigureRef::Incarnation(old.clone()));
		assert_ne!(current, old);
		let receipt = session.apply(&Command::Hide {
			figure: FigureRef::Incarnation(current.clone()),
			series: 0,
		});
		assert_eq!(receipt.outcome, Outcome::Applied);
		assert_eq!(receipt.figure.as_deref(), Some(current.as_str()));
		let receipt = session.apply(&Command::Restore {
			figure: FigureRef::Incarnation(current.clone()),
		});
		assert_eq!(receipt.figure.as_deref(), Some(current.as_str()));
	}

	/// PRODUCT-0158 gate 4: for the same application and the same hidden
	/// set, the driver's package and the menu's package have the same identity
	/// digest and byte-identical figure files.
	#[test]
	fn product_0158_publish_is_the_menus() {
		let (mut session, _) = session();
		let figure = FigureRef::Incarnation(session.loaded()[0].figure.clone());
		session.apply(&Command::Hide { figure, series: 1 });
		let scratch = std::env::temp_dir().join(format!(
			"polariton-0158-publish-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		let menu_dir = scratch.join("menu");
		let driver_dir = scratch.join("driver");
		std::fs::create_dir_all(&scratch).expect("scratch");

		let application = session.application_mut();
		application.dispatch(Message::PublishRequested);
		application.dispatch(Message::PublishPathChanged(
			menu_dir.to_str().expect("utf-8").to_owned(),
		));
		application.dispatch(Message::PublishSubmitted);
		application
			.settle_until(Duration::from_secs(30), |state| {
				state.status_msg.starts_with("Published ") || state.status_msg.contains("failed")
			})
			.expect("the menu's publish finishes");
		let status = application.state().status_msg.clone();
		let menu_digest = status
			.rsplit(" · ")
			.next()
			.unwrap_or_else(|| panic!("no digest in {status:?}"))
			.to_owned();

		let receipt = session.apply(&Command::Publish {
			directory: driver_dir.to_str().expect("utf-8").to_owned(),
		});
		assert_eq!(receipt.outcome, Outcome::Applied, "{:?}", receipt.refusal);
		let published = receipt.publish.expect("publish receipt");
		assert_eq!(published.digest, menu_digest);
		assert_eq!(published.figures.len(), 1);

		let mut compared = 0;
		for entry in walk(&menu_dir) {
			let relative = entry.strip_prefix(&menu_dir).expect("relative");
			if relative == std::path::Path::new("manifest.json") {
				continue;
			}
			let theirs = std::fs::read(driver_dir.join(relative)).expect("driver file");
			assert_eq!(
				std::fs::read(&entry).expect("menu file"),
				theirs,
				"{relative:?}"
			);
			compared += 1;
		}
		assert!(compared > 0);
		std::fs::remove_dir_all(&scratch).ok();
	}

	/// PRODUCT-0158 §3: a resize the pane already satisfies is complete, not a
	/// wait for a change that never comes.
	#[test]
	fn product_0158_resize_to_the_current_size_is_complete() {
		let (mut session, _) = session();
		let (panel, _, export) = session.application_mut().exports_with_identity().remove(0);
		let outcome = session.application_mut().resize_pane(
			panel,
			export.width - 0.1,
			export.height - 0.1,
			Duration::from_millis(100),
		);
		assert!(
			outcome.is_ok(),
			"already satisfied resize refused: {outcome:?}"
		);
	}

	/// PRODUCT-0158 §2: the loaded snapshot is the panes as they were when
	/// the application opened - reported, never assumed - and it survives
	/// later commands.
	#[test]
	fn product_0158_loaded_is_the_initial_live_state_and_survives_commands() {
		let (mut session, _) = session();
		let figure = session.loaded()[0].clone();
		assert_eq!(figure.loaded.as_ref(), Some(&figure.live));
		let baseline = figure.loaded.clone().unwrap();
		assert!(baseline.size.width > 0.0 && baseline.size.height > 0.0);
		assert!(baseline.hidden.is_empty());
		let token = FigureRef::Incarnation(figure.figure.clone());
		session.apply(&Command::Hide {
			figure: token,
			series: 0,
		});
		let receipt = session.apply(&Command::Describe {});
		let described = &receipt.figures.expect("describe")[0];
		assert_eq!(described.loaded, figure.loaded);
		assert_eq!(described.live.hidden, vec![0]);
	}

	/// PRODUCT-0158 §2 and gate 2: `describe --session` opens a saved session
	/// through the window's own resolver, and what it loads carries no hidden
	/// set. The store is isolated in a child process, as the saved-session
	/// tests do, because its location is process-global.
	// Saved sessions need the Unix session store.
	#[cfg(unix)]
	#[test]
	fn product_0158_describe_opens_a_saved_session() {
		const CHILD: &str = "POLARITON_PRODUCT_0158_SESSION_CHILD";
		if std::env::var_os(CHILD).is_none() {
			let state_home = std::env::temp_dir().join(format!(
				"polariton-product-0158-session-{}-{}",
				std::process::id(),
				crate::selection::SurfaceIncarnation::mint().token()
			));
			let status = std::process::Command::new(std::env::current_exe().unwrap())
				.args([
					"--exact",
					"gui::state::product_0158_driver_tests::product_0158_describe_opens_a_saved_session",
					"--nocapture",
				])
				.env(CHILD, "1")
				.env("XDG_STATE_HOME", &state_home)
				.env("HOME", &state_home)
				.status()
				.expect("spawn the isolated saved-session fixture");
			let _ = std::fs::remove_dir_all(&state_home);
			assert!(status.success(), "isolated saved-session fixture failed");
			return;
		}
		let authority = super::resolve_startup(super::StartupTarget::CreateSaved).unwrap();
		let super::StartupAuthority::Saved { session_id, .. } = &authority else {
			panic!("create must resolve saved authority");
		};
		let session_id = *session_id;
		drop(authority);
		let target = crate::driver::Target::Session(session_id.to_string());
		assert_eq!(
			target.startup().expect("a session id resolves"),
			super::StartupTarget::OpenSaved(session_id)
		);
		let mut application = HeadlessApplication::open_target(
			target.startup().expect("target"),
			StartupData::default(),
		)
		.expect("the driver opens the saved session");
		assert_eq!(application.state().session_id, Some(session_id));
		run_source(application.state_mut(), BufferId::new(80), 1);
		application
			.settle_until(Duration::from_secs(20), |state| {
				state.code_editor.host_plot_exports().len() == 1
			})
			.expect("the view's pane projects");
		let session = Session::new(application);
		let figure = &session.loaded()[0];
		let baseline = figure.loaded.as_ref().expect("baseline");
		assert!(baseline.hidden.is_empty());
		assert_eq!(baseline.size, figure.live.size);
		assert!(figure.live.size.width > 0.0);
	}

	/// PRODUCT-0160 §4 on the batch receipt: a resize the engine clamps to
	/// nothing runs and reaches its deadline, so the receipt says `failed`
	/// with the observed size, not `refused`, which means nothing ran.
	#[test]
	fn product_0160_batch_resize_deadline_is_a_failure_with_the_observed_size() {
		let (mut session, _) = session();
		let loaded = session.loaded()[0].clone();
		let figure = FigureRef::Incarnation(loaded.figure.clone());
		// The harness pane has a vertical neighbour only, so ask for a height
		// the engine clamps; the first request moves the divider as far as it
		// goes, the second moves nothing and runs out its deadline.
		let huge = |figure: FigureRef| Command::Resize {
			figure,
			width: loaded.size.width,
			height: 100_000.0,
		};
		let first = session.apply(&huge(figure.clone()));
		assert!(first.refusal.is_none(), "{:?}", first.refusal);
		let described = session.apply(&Command::Describe {});
		let clamped = described.figures.expect("figures")[0].live.size;
		let second = session.apply(&huge(figure));
		assert_eq!(second.outcome, Outcome::Failed);
		assert!(matches!(
			second.failure,
			Some(crate::driver::Failure::ResizeDeadline { .. })
		));
		assert!(second.refusal.is_none());
		assert_eq!(second.state.expect("observed state").size, clamped);
	}

	/// PRODUCT-0160 §4 on the batch receipt: a publish whose job ran and
	/// whose write did not finish says `failed` with the root it attempted;
	/// a publish that never ran still says `refused`.
	#[test]
	fn product_0160_batch_publish_write_failure_is_a_failure_with_the_root() {
		let (mut session, _) = session();
		let scratch = std::env::temp_dir().join(format!(
			"polariton-0160-batch-fail-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		std::fs::create_dir_all(&scratch).expect("scratch");
		let blocker = scratch.join("not-a-directory");
		std::fs::write(&blocker, b"x").expect("blocker");
		let root = blocker.join("package");
		let receipt = session.apply(&Command::Publish {
			directory: root.display().to_string(),
		});
		assert_eq!(receipt.outcome, Outcome::Failed);
		assert!(receipt.publish.is_none());
		assert!(receipt.refusal.is_none());
		let Some(crate::driver::Failure::Publish {
			root: attempted, ..
		}) = receipt.failure
		else {
			panic!("{:?}", receipt.failure);
		};
		assert_eq!(attempted, root.display().to_string());
		let refused = session.apply(&Command::Publish {
			directory: String::new(),
		});
		assert_eq!(refused.outcome, Outcome::Refused);
		assert!(refused.failure.is_none());
		std::fs::remove_dir_all(&scratch).ok();
	}

	/// PRODUCT-0160 §4: a successful receipt and a refused receipt serialize
	/// exactly as before the `failed` outcome existed.
	#[test]
	fn product_0160_applied_and_refused_receipts_are_unchanged_by_failed() {
		let (mut session, _) = session();
		let figure = FigureRef::Incarnation(session.loaded()[0].figure.clone());
		let applied = session.apply(&Command::Hide {
			figure: figure.clone(),
			series: 0,
		});
		assert_eq!(applied.outcome, Outcome::Applied);
		assert!(applied.failure.is_none() && applied.refusal.is_none());
		let refused = session.apply(&Command::Hide { figure, series: 9 });
		assert_eq!(refused.outcome, Outcome::Refused);
		assert!(refused.failure.is_none() && refused.refusal.is_some());
	}

	/// PRODUCT-0160 §6 follow-up: a figure whose load baseline is missing is
	/// reported with `loaded` absent. Nothing panics on the update thread and
	/// nothing mints a baseline from the state it observes.
	#[test]
	fn product_0160_missing_baseline_is_reported_absent_not_fabricated() {
		let (mut session, _) = session();
		let figure = session.loaded()[0].clone();
		assert!(figure.loaded.is_some());
		let token: u64 = figure.figure.parse().unwrap();
		assert!(
			session
				.application_mut()
				.state_mut()
				.forget_baseline_for_test(token)
		);
		let reference = FigureRef::Incarnation(figure.figure.clone());
		session.apply(&Command::Hide {
			figure: reference.clone(),
			series: 0,
		});
		// Twice, with engine ticks and legend messages between, so every
		// capture pass had the chance to mint a baseline from the edited state
		// and did not.
		for pass in 0..2 {
			let described = session.apply(&Command::Describe {});
			assert_eq!(described.outcome, Outcome::Applied);
			let figures = described.figures.expect("figures");
			assert_eq!(figures[0].figure, figure.figure);
			assert!(
				figures[0].loaded.is_none(),
				"pass {pass}: no baseline is minted from observation"
			);
			assert_eq!(figures[0].live.hidden, vec![0]);
			session
				.application_mut()
				.settle_until(Duration::from_millis(50), |_| false)
				.ok();
			session.apply(&Command::Show {
				figure: reference.clone(),
				series: 0,
			});
			session.apply(&Command::Hide {
				figure: reference.clone(),
				series: 0,
			});
		}
		// A replacement incarnation is a first appearance and captures its own.
		run_source(session.application_mut().state_mut(), BufferId::new(80), 2);
		session
			.application_mut()
			.settle_until(Duration::from_secs(20), |state| {
				state
					.code_editor
					.host_plot_exports_with_identity()
					.iter()
					.any(|(_, incarnation, _)| incarnation.token() != token)
			})
			.expect("the rerun installs a new incarnation");
		let described = session.apply(&Command::Describe {});
		let figures = described.figures.expect("figures");
		let replacement = figures
			.iter()
			.find(|description| description.figure != figure.figure)
			.expect("the replacement is described");
		let baseline = replacement
			.loaded
			.as_ref()
			.expect("a new incarnation captures its own baseline");
		assert!(baseline.hidden.is_empty());
	}

	fn walk(root: &std::path::Path) -> Vec<std::path::PathBuf> {
		let mut files = Vec::new();
		let mut pending = vec![root.to_path_buf()];
		while let Some(dir) = pending.pop() {
			for entry in std::fs::read_dir(&dir).expect("read dir") {
				let path = entry.expect("entry").path();
				if path.is_dir() {
					pending.push(path);
				} else {
					files.push(path);
				}
			}
		}
		files.sort();
		files
	}
}

#[cfg(test)]
mod local_rune_tests;
