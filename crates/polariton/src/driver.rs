//! PRODUCT-0158: the agent-operable application surface, batch form.
//!
//! `polariton drive <source.rn> --commands <commands.json> --receipt
//! <receipt.json>` opens the application headless, runs the source, and
//! applies a script of commands: `describe`, the legend's visibility actions
//! by series ordinal, `resize`, and `publish`. Every command is addressed to a
//! figure by its incarnation token and answered in a receipt.
//!
//! This module is a client. It constructs no kernel, no settings model, and
//! no package: it reads the exports the editor already makes, sends the pane
//! messages the legend already sends, and publishes through the call the menu
//! already makes. An architecture guard holds it to that.

use crate::editor::HostPlotExport;
use crate::gui::HeadlessApplication;
use crate::plot::common::ScatterRenderMode;
#[cfg(test)]
use crate::plot::series::SeriesVisibilityAction;
use crate::presentation_registry::inspector_rows;
use crate::publication::{
	PresentationSetting, PublishError, SERIES_BOUND, series_table, widget_for,
};
use crate::selection::SurfaceIncarnation;
use libaryon::engine::HostPanelId;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::Path;
use std::time::Duration;

pub(crate) const RECEIPT_SCHEMA_VERSION: u32 = 1;

/// PRODUCT-0158 §2: what an invocation opens - a source file, as `polariton
/// <source>` would, or a saved session, as `polariton --session <id>` would.
#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum Target {
	Source(std::path::PathBuf),
	Session(String),
}

impl Target {
	fn describe(&self) -> String {
		match self {
			Target::Source(path) => path.display().to_string(),
			Target::Session(id) => format!("session:{id}"),
		}
	}

	pub(crate) fn startup(&self) -> Result<crate::gui::StartupTarget, String> {
		match self {
			Target::Source(path) => Ok(crate::gui::StartupTarget::Ephemeral {
				initial_file: Some(
					path.to_str()
						.ok_or_else(|| format!("source path is not UTF-8: {}", path.display()))?
						.to_owned(),
				),
			}),
			Target::Session(id) => Ok(crate::gui::StartupTarget::OpenSaved(
				id.parse()
					.map_err(|error| format!("invalid session id {id:?}: {error}"))?,
			)),
		}
	}
}
/// How long a run may take to attach its workspace, run every relation, and
/// settle. The fixture showcase's million-point scatter fits well inside it.
const RUN_DEADLINE: Duration = Duration::from_secs(600);

/// The commands file: one script, applied in order in one application.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(deny_unknown_fields)]
pub(crate) struct Commands {
	pub(crate) commands: Vec<Command>,
}

/// PRODUCT-0158 §3: how a command names a figure. An incarnation token is
/// what a describe reported and is refused once the figure is replaced; a
/// view name is the authored identity, resolved to whatever incarnation
/// stands in its pane when the command runs, so a script written before the
/// application ran can still address its figures. Either way the receipt
/// names the incarnation acted on.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum FigureRef {
	Incarnation(String),
	View(String),
}

/// PRODUCT-0158 §3 and §4: the verbs. Each is the GUI's own action.
#[derive(Clone, Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "snake_case", deny_unknown_fields)]
pub(crate) enum Command {
	Describe {},
	DescribeNotebook {
		#[serde(default)]
		buffer: Option<String>,
	},
	OpenData {
		path: String,
	},
	ReplaceCell {
		target: CellRef,
		source: String,
	},
	RunCell {
		target: CellRef,
	},
	Hide {
		figure: FigureRef,
		series: u32,
	},
	Show {
		figure: FigureRef,
		series: u32,
	},
	Isolate {
		figure: FigureRef,
		series: u32,
	},
	Restore {
		figure: FigureRef,
	},
	Resize {
		figure: FigureRef,
		width: f32,
		height: f32,
	},
	Publish {
		directory: String,
	},
	/// ADR 0166: connect the window's external adapter through the same
	/// operation the connection form uses. `fields` are the form's fields.
	Connect {
		adapter: String,
		#[serde(default)]
		name: String,
		#[serde(default)]
		fields: BTreeMap<String, String>,
	},
	/// Tear down the external adapter connection named by `generation`; a
	/// superseded generation is refused, never applied to its replacement.
	Disconnect {
		generation: u64,
	},
	ConnectionStatus {},
	/// ADR 0169: execute the open Rune source against the window's current
	/// connection; one execution, identified and completed on its own terms.
	RunSource {},
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(deny_unknown_fields)]
pub(crate) struct CellRef {
	pub buffer: String,
	pub generation: String,
	pub cell: u32,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NotebookCell {
	pub cell: u32,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub name: Option<String>,
	pub language: String,
	pub source: String,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct NotebookDescription {
	pub buffer: String,
	pub generation: String,
	pub cells: Vec<NotebookCell>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct DataDescription {
	pub table: String,
	pub columns: Vec<(String, String)>,
}

/// ADR 0169 §2: when the batch driver executes the source. Chosen by the
/// invocation, never inferred from the script, and stated in the receipt.
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "kebab-case")]
pub(crate) enum ExecutionMode {
	/// PRODUCT-0158's documented behavior: run after opening, before the
	/// first command.
	OnOpen,
	/// Open only; the source runs where the script says `run_source`.
	ByCommand,
}

/// ADR 0169 §3: the execution a `run_source` started, as the window
/// identified it. A receipt reports this execution's outcome and no other's.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ExecutionDescription {
	/// The source buffer, as `describe_notebook` names buffers.
	pub buffer: String,
	/// The document generation the execution read.
	pub generation: String,
	/// The database connection generation (ADR 0166 §3) it ran against.
	pub connection_generation: u64,
	/// The window's execution id for this run of the buffer.
	pub id: u64,
}

/// ADR 0168: a SQL cell's answer, bounded. The preview holds the leading
/// rows as the grid shows them, each cell null or text; `rows` is the whole
/// relation's height. When a bound cut the preview, `truncated` is true and
/// `truncation_reasons` names every bound that applied.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResultSummary {
	pub columns: Vec<ResultColumn>,
	pub rows: u64,
	pub preview: Vec<Vec<Option<String>>>,
	pub truncated: bool,
	pub truncation_reasons: Vec<TruncationReason>,
}
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ResultColumn {
	pub name: String,
	pub dtype: String,
}
#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum TruncationReason {
	/// More rows exist than the preview holds.
	Rows,
	/// The byte budget ended the preview before the row bound.
	Bytes,
	/// Not every column fit the budget; the preview is empty.
	Columns,
	/// A single value was cut to fit the remaining budget.
	Value,
}

impl Command {
	#[cfg(test)]
	fn figure(&self) -> Option<&FigureRef> {
		match self {
			Command::Describe {}
			| Command::Publish { .. }
			| Command::DescribeNotebook { .. }
			| Command::OpenData { .. }
			| Command::ReplaceCell { .. }
			| Command::RunCell { .. }
			| Command::Connect { .. }
			| Command::Disconnect { .. }
			| Command::ConnectionStatus {}
			| Command::RunSource {} => None,
			Command::Hide { figure, .. }
			| Command::Show { figure, .. }
			| Command::Isolate { figure, .. }
			| Command::Restore { figure }
			| Command::Resize { figure, .. } => Some(figure),
		}
	}
}

/// PRODUCT-0158 §5: the receipt. Every command appears with its outcome; a
/// refusal of the whole invocation names its reason and lists no commands as
/// applied.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct Receipt {
	pub(crate) schema_version: u32,
	pub(crate) source: String,
	/// ADR 0169 §2: how this invocation executed the source.
	pub(crate) execution: ExecutionMode,
	/// The figures as loaded, before any command ran.
	pub(crate) loaded: Vec<FigureDescription>,
	pub(crate) commands: Vec<CommandReceipt>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) refusal: Option<Refusal>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct CommandReceipt {
	pub(crate) command: Command,
	pub(crate) outcome: Outcome,
	/// The incarnation the command acted on, when it named one.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) figure: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) refusal: Option<Refusal>,
	/// Present only with outcome `failed`.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) failure: Option<Failure>,
	/// The figure's live state after the command, for commands that act on
	/// one.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) state: Option<LiveState>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) figures: Option<Vec<FigureDescription>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) publish: Option<PublishReceipt>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) notebook: Option<NotebookDescription>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) data: Option<DataDescription>,
	/// ADR 0168: the answer of the SQL cell this run executed, bounded.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) result: Option<ResultSummary>,
	/// ADR 0169: the execution a `run_source` started.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) execution: Option<ExecutionDescription>,
	/// ADR 0166: the external connection as the adapter owner reports it.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) connection: Option<ConnectionDescription>,
}

/// The adapter owner's view of the external connection: what is selected,
/// whether it is connected, and the database connection generation that
/// `disconnect` must name. `error` is the last connection failure, verbatim.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq, Eq)]
pub(crate) struct ConnectionDescription {
	pub adapter: String,
	pub name: String,
	pub stage: String,
	pub generation: u64,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub error: Option<String>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
	Applied,
	/// Nothing ran.
	Refused,
	/// PRODUCT-0160 §4: the command ran and its effect did not finish as
	/// asked. `failure` says how; `state` or `failure.root` says what was
	/// observed, and nothing claims a rollback.
	Failed,
}

/// Why a command that ran did not finish as asked.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum Failure {
	Notebook {
		message: String,
	},
	/// The divider drag moved nothing within the deadline; `state` on the
	/// receipt is the size the pane had when the deadline passed.
	ResizeDeadline {
		figure: String,
	},
	/// The pane's incarnation changed, or the pane left the viewport, before
	/// the resize could be observed; `current` names the replacement.
	ResizeSuperseded {
		figure: String,
		#[serde(skip_serializing_if = "Option::is_none")]
		current: Option<String>,
	},
	/// The publish job ran and its write did not finish; `root` is the
	/// directory it wrote into, which may hold partial files.
	Publish {
		reason: PublishRefusal,
		root: String,
	},
	/// ADR 0166: the connection attempt ran and did not connect; `message`
	/// is what the adapter reported, which is what the form would show.
	Connection {
		message: String,
	},
}

/// Structured, never prose alone.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum Refusal {
	Notebook {
		message: String,
	},
	/// The commands file did not parse; nothing ran.
	CommandsUnreadable {
		message: String,
	},
	/// The application could not open or run the source.
	ApplicationFailed {
		message: String,
	},
	/// The token names no figure in the application.
	UnknownFigure {
		figure: FigureRef,
	},
	/// Multiple open sources can author the same view name. Name an incarnation.
	AmbiguousFigure {
		figure: FigureRef,
		candidates: Vec<String>,
	},
	/// The token named a figure that has since been replaced; `current` is
	/// the incarnation that stands in its pane now.
	StaleFigure {
		figure: FigureRef,
		current: String,
	},
	/// The ordinal is not in the figure's series table.
	UnknownSeries {
		figure: String,
		series: u32,
		series_count: u32,
	},
	/// The figure has no series table to act on.
	NoSeriesTable {
		figure: String,
		reason: String,
	},
	/// The resize could not be dispatched or did not take.
	ResizeRefused {
		figure: String,
		message: String,
	},
	/// The publish action refused, with the publisher's own reason.
	PublishRefused {
		reason: PublishRefusal,
	},
	/// ADR 0166: the connection operation refused before any transport was
	/// chosen: an unknown adapter, a partial TLS configuration, a busy owner,
	/// or a stale generation.
	ConnectionRefused {
		message: String,
	},
}

/// `PublishError`, one variant per refusal, so a receipt can be matched on.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "reason")]
pub(crate) enum PublishRefusal {
	NoFigures,
	Republished { figure: String },
	NoRecipe { figure: String },
	Rebuild { figure: String, error: String },
	Data { figure: String, error: String },
	Secret { file: String, field: String },
	SeriesLabel { figure: String },
	Encoding { error: String },
	Write { error: String },
}

impl From<&PublishError> for PublishRefusal {
	fn from(error: &PublishError) -> Self {
		match error {
			PublishError::NoFigures => PublishRefusal::NoFigures,
			PublishError::Republished { figure } => PublishRefusal::Republished {
				figure: figure.clone(),
			},
			PublishError::NoRecipe { figure } => PublishRefusal::NoRecipe {
				figure: figure.clone(),
			},
			PublishError::Rebuild { figure, error } => PublishRefusal::Rebuild {
				figure: figure.clone(),
				error: error.clone(),
			},
			PublishError::Data { figure, error } => PublishRefusal::Data {
				figure: figure.clone(),
				error: error.clone(),
			},
			PublishError::Secret { file, field } => PublishRefusal::Secret {
				file: file.clone(),
				field: field.clone(),
			},
			PublishError::SeriesLabel { figure } => PublishRefusal::SeriesLabel {
				figure: figure.clone(),
			},
			PublishError::Encoding(error) => PublishRefusal::Encoding {
				error: error.clone(),
			},
			PublishError::Write(error) => PublishRefusal::Write {
				error: error.clone(),
			},
		}
	}
}

/// PRODUCT-0158 §2: one pane's figure as the inspector, the series table,
/// and the renderer see it.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct FigureDescription {
	/// The incarnation token commands address.
	pub(crate) figure: String,
	pub(crate) pane: u64,
	pub(crate) title: String,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) view: Option<String>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) family: Option<String>,
	pub(crate) size: Size,
	/// The series table as `series.json` would carry it, or why there is
	/// none.
	pub(crate) series: SeriesReport,
	/// PRODUCT-0158 §2: what the loaded application carries. A saved session
	/// restores no hidden set and a source has none, so this is always empty;
	/// it is reported, not assumed, so a describe that ever found one would
	/// show it.
	/// Absent only when the application holds no load baseline for this
	/// incarnation. That is an invariant failure, reported as absence rather
	/// than fabricated from the state observed now (PRODUCT-0160 §6).
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) loaded: Option<LiveState>,
	/// The state after every command so far in this invocation.
	pub(crate) live: LiveState,
	/// The scatter render mode at this pane's plot area, from the renderer's
	/// own resolution; absent for figures that have none.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) render_mode: Option<String>,
	/// The inspector's rows, keyed by label, with their provenance tiers.
	pub(crate) settings: BTreeMap<String, PresentationSetting>,
}

#[derive(Clone, Copy, Debug, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct Size {
	pub(crate) width: f32,
	pub(crate) height: f32,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "status")]
pub(crate) enum SeriesReport {
	/// The table the publisher would write, verbatim.
	Table { table: serde_json::Value },
	/// No table: the figure identifies no series, or a label failed to join.
	Absent { reason: String },
}

#[derive(Clone, Debug, Default, Serialize, Deserialize, PartialEq)]
pub(crate) struct LiveState {
	/// Hidden series by ordinal, in ordinal order.
	pub(crate) hidden: Vec<u32>,
	/// The pane's plot area in pixels.
	pub(crate) size: Size,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct PublishReceipt {
	pub(crate) root: String,
	pub(crate) digest: String,
	pub(crate) figures: Vec<PublishedFigure>,
}

#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub(crate) struct PublishedFigure {
	pub(crate) id: String,
	pub(crate) title: String,
	pub(crate) visibility: serde_json::Value,
	pub(crate) inspection: serde_json::Value,
}

/// `polariton drive`: parse the commands, open and run the source, apply the
/// commands, write the receipt. A commands file that does not parse refuses
/// the invocation before the application opens.
pub(crate) fn drive(
	source: &Path,
	commands: &Path,
	receipt: &Path,
	execution: ExecutionMode,
) -> Result<(), String> {
	let target = Target::Source(source.to_path_buf());
	let script = match read_commands(commands) {
		Ok(script) => script,
		Err(message) => {
			let refused = Receipt {
				schema_version: RECEIPT_SCHEMA_VERSION,
				source: target.describe(),
				execution,
				loaded: Vec::new(),
				commands: Vec::new(),
				refusal: Some(Refusal::CommandsUnreadable { message }),
			};
			write_receipt(receipt, &refused)?;
			return Err(format!("commands refused; see {}", receipt.display()));
		}
	};
	let receipt_document = run(&target, &script, execution);
	write_receipt(receipt, &receipt_document)?;
	match &receipt_document.refusal {
		Some(refusal) => Err(format!("{refusal:?}; see {}", receipt.display())),
		None => Ok(()),
	}
}

/// `polariton describe`: a receipt with one describe and nothing else,
/// written to stdout.
pub(crate) fn describe(target: &Target) -> Result<(), String> {
	let script = Commands {
		commands: vec![Command::Describe {}],
	};
	let receipt_document = run(target, &script, ExecutionMode::OnOpen);
	let text = serde_json::to_string_pretty(&receipt_document)
		.map_err(|error| format!("cannot encode the receipt: {error}"))?;
	println!("{text}");
	match &receipt_document.refusal {
		Some(refusal) => Err(format!("{refusal:?}")),
		None => Ok(()),
	}
}

pub(crate) fn read_commands(path: &Path) -> Result<Commands, String> {
	let bytes =
		std::fs::read(path).map_err(|error| format!("cannot read {}: {error}", path.display()))?;
	serde_json::from_slice(&bytes).map_err(|error| format!("{}: {error}", path.display()))
}

fn write_receipt(path: &Path, receipt: &Receipt) -> Result<(), String> {
	let text = serde_json::to_string_pretty(receipt)
		.map_err(|error| format!("cannot encode the receipt: {error}"))?;
	std::fs::write(path, text).map_err(|error| format!("cannot write {}: {error}", path.display()))
}

/// Open, run, and apply. Never panics on a refusal: the receipt carries it.
pub(crate) fn run(target: &Target, script: &Commands, execution: ExecutionMode) -> Receipt {
	let mut receipt = Receipt {
		schema_version: RECEIPT_SCHEMA_VERSION,
		source: target.describe(),
		execution,
		loaded: Vec::new(),
		commands: Vec::new(),
		refusal: None,
	};
	let startup = crate::configuration::load_startup_data();
	let opened = target
		.startup()
		.and_then(|startup_target| HeadlessApplication::open_target(startup_target, startup));
	let mut application = match opened {
		Ok(application) => application,
		Err(message) => {
			receipt.refusal = Some(Refusal::ApplicationFailed { message });
			return receipt;
		}
	};
	// ADR 0169 §2: on-open keeps PRODUCT-0158's order; by-command opens only
	// and leaves execution to the script's `run_source`.
	if execution == ExecutionMode::OnOpen {
		if let Err(message) = application.run_source(RUN_DEADLINE) {
			receipt.refusal = Some(Refusal::ApplicationFailed { message });
			return receipt;
		}
	} else if let Err(message) = application.open_source(RUN_DEADLINE) {
		receipt.refusal = Some(Refusal::ApplicationFailed { message });
		return receipt;
	}
	let mut session = Session::new(application);
	receipt.loaded = session.loaded().to_vec();
	for command in &script.commands {
		receipt.commands.push(session.apply(command));
	}
	receipt
}

/// One invocation's application and the figures it loaded, against which
/// every later command is resolved.
pub(crate) struct Session {
	application: HeadlessApplication,
	loaded: Vec<FigureDescription>,
}

impl Session {
	pub(crate) fn new(application: HeadlessApplication) -> Self {
		let loaded = application.describe_surface();
		Self {
			application,
			loaded,
		}
	}

	pub(crate) fn loaded(&self) -> &[FigureDescription] {
		&self.loaded
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn into_application(self) -> HeadlessApplication {
		self.application
	}

	#[cfg(all(test, feature = "toron"))]
	pub(crate) fn application_mut(&mut self) -> &mut HeadlessApplication {
		&mut self.application
	}

	pub(crate) fn apply(&mut self, command: &Command) -> CommandReceipt {
		self.application.apply_command(command)
	}
}

impl CommandReceipt {
	pub(crate) fn new(command: &Command) -> Self {
		Self {
			command: command.clone(),
			outcome: Outcome::Applied,
			figure: None,
			refusal: None,
			failure: None,
			state: None,
			figures: None,
			publish: None,
			notebook: None,
			data: None,
			result: None,
			execution: None,
			connection: None,
		}
	}
	pub(crate) fn refuse(&mut self, refusal: Refusal) {
		self.outcome = Outcome::Refused;
		self.refusal = Some(refusal);
	}

	/// PRODUCT-0160 §4: the command ran; keep whatever was observed.
	pub(crate) fn fail(&mut self, failure: Failure) {
		self.outcome = Outcome::Failed;
		self.failure = Some(failure);
	}
}

pub(crate) fn published_receipt(
	directory: &Path,
	package: &crate::publication::Package,
) -> PublishReceipt {
	PublishReceipt {
		root: directory.display().to_string(),
		digest: package.digest(),
		figures: package
			.manifest
			.figures
			.iter()
			.map(|figure| PublishedFigure {
				id: figure.id.clone(),
				title: figure.title.clone(),
				visibility: serde_json::to_value(&figure.visibility).unwrap_or_default(),
				inspection: serde_json::to_value(&figure.inspection).unwrap_or_default(),
			})
			.collect(),
	}
}

fn export_bounds(export: &HostPlotExport) -> iced::Rectangle {
	iced::Rectangle {
		x: 0.0,
		y: 0.0,
		width: export.width,
		height: export.height,
	}
}

/// PRODUCT-0158 §2: the series table exactly as the publisher derives it -
/// the same widget, coverage, hidden set, legend geometry, and bound.
pub(crate) fn series_table_of(
	export: &HostPlotExport,
) -> Result<crate::publication::SeriesTable, String> {
	let bounds = export_bounds(export);
	let widget = widget_for(export, &export.settings_model);
	let registry = widget.series_registry().clone();
	let covered = widget.series_covered(bounds);
	let hidden = widget.hidden_series().clone();
	let legend = widget.legend_hit_geometry(crate::coordinate::Rect::from_iced_frame(bounds));
	drop(widget);
	let identified = registry.identities().len();
	if identified == 0 {
		return Err("the figure identifies no series".to_owned());
	}
	let actionable = covered && identified <= SERIES_BOUND;
	series_table(export, &registry, &hidden, legend.as_ref(), actionable)
		.ok_or_else(|| "a series label did not join its identity".to_owned())
}

pub(crate) fn live_state_of(export: &HostPlotExport) -> LiveState {
	let widget = widget_for(export, &export.settings_model);
	let registry = widget.series_registry();
	let hidden = widget.hidden_series();
	LiveState {
		size: Size {
			width: export.width,
			height: export.height,
		},
		hidden: registry
			.identities()
			.iter()
			.enumerate()
			.filter(|(_, identity)| hidden.contains(identity))
			.map(|(ordinal, _)| ordinal as u32)
			.collect(),
	}
}

/// PRODUCT-0158 §2: one figure. Everything here is read from the export the
/// editor made and the resolutions the renderer and the inspector make.
pub(crate) fn describe_export(
	panel: HostPanelId,
	incarnation: SurfaceIncarnation,
	export: &HostPlotExport,
) -> FigureDescription {
	let bounds = export_bounds(export);
	let series = match series_table_of(export) {
		Ok(table) => SeriesReport::Table {
			table: serde_json::to_value(&table).unwrap_or_default(),
		},
		Err(reason) => SeriesReport::Absent { reason },
	};
	let widget = widget_for(export, &export.settings_model);
	let render_mode = widget.scatter_render_mode(bounds).map(|mode| {
		match mode {
			ScatterRenderMode::Auto => "auto",
			ScatterRenderMode::Vector => "vector",
			ScatterRenderMode::Downsampled => "downsampled",
			ScatterRenderMode::Rasterized => "rasterized",
		}
		.to_owned()
	});
	drop(widget);
	let resolved = export.kernel.resolve_settings(&export.settings_model);
	let settings = inspector_rows(&export.settings_model, &resolved, export.plot_type, None)
		.into_iter()
		.map(|row| {
			(
				row.label.to_owned(),
				PresentationSetting {
					section: format!("{:?}", row.section),
					value: crate::publication::setting_value_json(&row.value),
					provenance: row.provenance.to_string(),
				},
			)
		})
		.collect();
	FigureDescription {
		figure: incarnation.token().to_string(),
		pane: panel.get(),
		title: export.title.clone(),
		view: export.view_name.clone(),
		family: export.plot_type.map(crate::publication::family_name),
		size: Size {
			width: export.width,
			height: export.height,
		},
		series,
		loaded: None,
		live: live_state_of(export),
		render_mode,
		settings,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::series::HiddenSeries;
	use crate::plot::spec::{AuthoredPlotSpec, Override};
	use crate::publication::tests_support::{artifact, fixture_with_spec, publish, request};
	use crate::result_artifact::ResultArtifact;
	use std::sync::Arc;

	fn colored(title: &str, artifact: &Arc<ResultArtifact>, plot_type: PlotType) -> HostPlotExport {
		fixture_with_spec(
			title,
			artifact,
			plot_type,
			0,
			AuthoredPlotSpec::xy_color(
				PlotComposition::single(plot_type),
				"x",
				"y",
				Some("series".to_owned()),
			),
		)
	}

	fn series_artifact() -> Arc<ResultArtifact> {
		let artifact = ResultArtifact::from_polars(Arc::new(
			polars::df!(
				"series" => ["a", "a", "b", "b", "c", "c"],
				"x" => [0.0, 1.0, 0.0, 1.0, 0.0, 1.0],
				"y" => [0.0, 0.0, 1.0, 1.0, 2.0, 2.0]
			)
			.expect("frame"),
		));
		artifact.bind_input_identity(crate::selection::InputIdentity::mint());
		artifact
	}

	fn hide(export: &mut HostPlotExport, indices: &[usize]) {
		let ids = export.kernel.series_identities();
		let mut hidden = HiddenSeries::default();
		for index in indices {
			hidden.apply(&SeriesVisibilityAction::Hide(ids[*index].clone()), &ids);
		}
		export.settings_model.pane_override.hidden_series = Override::Set(hidden);
	}

	fn describe(export: &HostPlotExport) -> FigureDescription {
		describe_export(
			HostPanelId::new(7),
			SurfaceIncarnation::from_token(42),
			export,
		)
	}

	/// PRODUCT-0158 gate 1: the settings a describe reports are the inspector's
	/// rows - same labels, sections, values, and provenance tiers - and the
	/// series table is the one `series.json` carries, byte for byte.
	#[test]
	fn product_0158_describe_is_the_inspector_and_the_series_table() {
		let artifact = series_artifact();
		let mut export = colored("Points", &artifact, PlotType::Scatter);
		hide(&mut export, &[1]);
		let description = describe(&export);
		assert_eq!(description.figure, "42");
		assert_eq!(description.pane, 7);
		assert_eq!(description.family.as_deref(), Some("scatter"));

		let resolved = export.kernel.resolve_settings(&export.settings_model);
		let rows = inspector_rows(&export.settings_model, &resolved, export.plot_type, None);
		assert!(!rows.is_empty());
		assert_eq!(description.settings.len(), rows.len());
		for row in rows {
			let reported = description
				.settings
				.get(row.label)
				.unwrap_or_else(|| panic!("describe lacks the inspector row {}", row.label));
			assert_eq!(reported.section, format!("{:?}", row.section));
			assert_eq!(reported.provenance, row.provenance.to_string());
			assert_eq!(
				reported.value,
				crate::publication::setting_value_json(&row.value)
			);
		}

		let package = publish(
			vec![colored("Points", &artifact, PlotType::Scatter)],
			&request(),
		);
		let (file, bytes) = package
			.files
			.iter()
			.find(|(name, _)| name.ends_with("/series.json"))
			.expect("the published figure carries a series table");
		let published: serde_json::Value = serde_json::from_slice(bytes).expect("series.json");
		let SeriesReport::Table { table } =
			describe(&colored("Points", &artifact, PlotType::Scatter)).series
		else {
			panic!("describe reports no table");
		};
		assert_eq!(table, published, "describe and {file} disagree");

		let SeriesReport::Table { table } = description.series else {
			panic!("describe reports no table for the hidden export");
		};
		assert_eq!(table["initially_hidden"], serde_json::json!([1]));
		assert_eq!(description.live.hidden, vec![1]);
		assert!(
			description.loaded.is_none(),
			"describe_export carries no baseline of its own"
		);
	}

	/// PRODUCT-0158 §2: a figure that identifies no series reports why, and
	/// the render mode is present only for a scatter.
	#[test]
	fn product_0158_describe_reports_absence_and_render_mode() {
		let plain =
			crate::publication::tests_support::fixture("Plain", &artifact(), PlotType::Line, 0);
		let description = describe(&plain);
		assert!(matches!(description.series, SeriesReport::Absent { .. }));
		assert_eq!(description.render_mode, None);
		// PRODUCT-0160 §6 follow-up: a description with no baseline serializes
		// without a `loaded` key rather than with a fabricated one.
		assert!(description.loaded.is_none());
		let json = serde_json::to_value(&description).expect("json");
		assert!(json.get("loaded").is_none() && json.get("live").is_some());
		let scatter = colored("Points", &series_artifact(), PlotType::Scatter);
		assert_eq!(describe(&scatter).render_mode.as_deref(), Some("vector"));
	}

	/// PRODUCT-0158 §3 and §5: the commands file and the receipt have the
	/// shapes an agent writes and reads; a figure is named by incarnation or
	/// by view, and every refusal is structured.
	#[test]
	fn product_0158_commands_and_refusals_are_structured() {
		let script: Commands = serde_json::from_str(
			r#"{"commands":[
				{"describe":{}},
				{"hide":{"figure":{"incarnation":"3"},"series":1}},
				{"isolate":{"figure":{"view":"trend-lines"},"series":0}},
				{"restore":{"figure":{"view":"trend-lines"}}},
				{"resize":{"figure":{"incarnation":"3"},"width":360,"height":360}},
				{"publish":{"directory":"/tmp/out"}},
				{"connect":{"adapter":"Toron","name":"lab","fields":{"endpoint":"192.0.2.1:7742"}}},
				{"connect":{"adapter":"SQLite"}},
				{"disconnect":{"generation":4}},
				{"connection_status":{}}
			]}"#,
		)
		.expect("commands parse");
		assert_eq!(script.commands.len(), 10);
		// ADR 0166: the connection verbs are on the surface, name and fields
		// optional, and a disconnect always names its generation.
		assert_eq!(
			script.commands[7],
			Command::Connect {
				adapter: "SQLite".to_owned(),
				name: String::new(),
				fields: BTreeMap::new()
			}
		);
		assert_eq!(script.commands[8], Command::Disconnect { generation: 4 });
		assert!(
			serde_json::from_str::<Commands>(r#"{"commands":[{"disconnect":{}}]}"#).is_err(),
			"a disconnect without a generation is not a command"
		);
		assert_eq!(
			script.commands[1],
			Command::Hide {
				figure: FigureRef::Incarnation("3".to_owned()),
				series: 1
			}
		);
		assert_eq!(
			script.commands[2].figure(),
			Some(&FigureRef::View("trend-lines".to_owned()))
		);
		assert!(
			serde_json::from_str::<Commands>(r#"{"commands":[{"set":{"figure":{"view":"x"}}}]}"#)
				.is_err(),
			"a settings verb is not on the surface"
		);
		assert!(
			serde_json::from_str::<Commands>(
				r#"{"commands":[{"hide":{"figure":"3","series":1}}]}"#
			)
			.is_err(),
			"a bare token is not a figure reference"
		);
		let refusal = serde_json::to_value(Refusal::StaleFigure {
			figure: FigureRef::Incarnation("3".to_owned()),
			current: "9".to_owned(),
		})
		.expect("refusal");
		assert_eq!(
			refusal,
			serde_json::json!({"kind":"stale_figure","figure":{"incarnation":"3"},"current":"9"})
		);
		let publish = serde_json::to_value(Refusal::PublishRefused {
			reason: PublishRefusal::from(&PublishError::Republished {
				figure: "f".to_owned(),
			}),
		})
		.expect("refusal");
		assert_eq!(
			publish,
			serde_json::json!({"kind":"publish_refused","reason":{"reason":"republished","figure":"f"}})
		);
	}

	/// PRODUCT-0160 §4: the `failed` outcome adds a key only to failed
	/// receipts; applied and refused receipts serialize exactly as before.
	#[test]
	fn product_0160_failed_outcome_leaves_applied_and_refused_receipts_unchanged() {
		let keys = |receipt: &CommandReceipt| {
			serde_json::to_value(receipt)
				.expect("json")
				.as_object()
				.expect("object")
				.keys()
				.cloned()
				.collect::<Vec<_>>()
		};
		let applied = CommandReceipt::new(&Command::Describe {});
		assert_eq!(keys(&applied), ["command", "outcome"]);
		assert_eq!(
			serde_json::to_value(&applied).unwrap()["outcome"],
			"applied"
		);
		let mut refused = CommandReceipt::new(&Command::Describe {});
		refused.refuse(Refusal::UnknownFigure {
			figure: FigureRef::View("x".to_owned()),
		});
		assert_eq!(keys(&refused), ["command", "outcome", "refusal"]);
		let mut failed = CommandReceipt::new(&Command::Describe {});
		failed.fail(Failure::ResizeDeadline {
			figure: "3".to_owned(),
		});
		let failed = serde_json::to_value(&failed).unwrap();
		assert_eq!(failed["outcome"], "failed");
		assert_eq!(
			failed["failure"],
			serde_json::json!({"kind": "resize_deadline", "figure": "3"})
		);
	}

	/// PRODUCT-0158 gate 5: a commands file that does not parse refuses the
	/// whole invocation before the application opens - the receipt names the
	/// parse error, lists no commands, and describes nothing.
	#[test]
	fn product_0158_unreadable_commands_refuse_before_any_action() {
		let dir = std::env::temp_dir().join(format!(
			"polariton-0158-refuse-{}-{}",
			std::process::id(),
			SurfaceIncarnation::mint().token()
		));
		std::fs::create_dir_all(&dir).expect("scratch dir");
		let commands = dir.join("commands.json");
		std::fs::write(
			&commands,
			br#"{"commands":[{"hide":{"figure":{"view":"x"}}}]}"#,
		)
		.expect("write commands");
		let receipt = dir.join("receipt.json");
		let outcome = drive(
			Path::new("/nonexistent/source.rn"),
			&commands,
			&receipt,
			ExecutionMode::OnOpen,
		);
		assert!(outcome.is_err());
		let written: Receipt =
			serde_json::from_slice(&std::fs::read(&receipt).expect("receipt")).expect("parse");
		assert!(matches!(
			written.refusal,
			Some(Refusal::CommandsUnreadable { .. })
		));
		assert!(written.commands.is_empty());
		assert!(written.loaded.is_empty());
		std::fs::remove_dir_all(&dir).ok();
	}

	/// ADR 0169 §2: the mode is the invocation's. On a head without a local
	/// workspace, on-open cannot execute a Rune source and refuses the
	/// invocation as before; by-command opens it and applies the script,
	/// with nothing executed and the receipt naming the mode.
	#[cfg(not(toron_local))]
	#[test]
	fn adr0169_by_command_opens_without_executing_and_on_open_keeps_its_refusal() {
		let dir = std::env::temp_dir().join(format!(
			"polariton-0169-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		std::fs::create_dir_all(&dir).unwrap();
		let source = dir.join("source.rn");
		std::fs::write(&source, "let x = 1;\n").unwrap();
		let script = Commands {
			commands: vec![Command::Describe {}],
		};
		let target = Target::Source(source.clone());
		let by_command = run(&target, &script, ExecutionMode::ByCommand);
		assert_eq!(by_command.execution, ExecutionMode::ByCommand);
		assert!(by_command.refusal.is_none(), "{by_command:?}");
		assert!(by_command.loaded.is_empty(), "nothing executed on open");
		assert_eq!(by_command.commands.len(), 1);
		assert_eq!(by_command.commands[0].outcome, Outcome::Applied);
		let on_open = run(&target, &script, ExecutionMode::OnOpen);
		assert_eq!(on_open.execution, ExecutionMode::OnOpen);
		assert!(
			matches!(on_open.refusal, Some(Refusal::ApplicationFailed { .. })),
			"{on_open:?}"
		);
		let _ = std::fs::remove_dir_all(&dir);
	}
}
