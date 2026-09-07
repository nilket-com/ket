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
use crate::gui::plot_pane::PlotPaneMsg;
use crate::plot::common::ScatterRenderMode;
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
const SETTLE_DEADLINE: Duration = Duration::from_secs(30);

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
}

impl Command {
	fn figure(&self) -> Option<&FigureRef> {
		match self {
			Command::Describe {} | Command::Publish { .. } => None,
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
	/// The figure's live state after the command, for commands that act on
	/// one.
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) state: Option<LiveState>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) figures: Option<Vec<FigureDescription>>,
	#[serde(skip_serializing_if = "Option::is_none")]
	pub(crate) publish: Option<PublishReceipt>,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize, PartialEq, Eq)]
#[serde(rename_all = "snake_case")]
pub(crate) enum Outcome {
	Applied,
	Refused,
}

/// Structured, never prose alone.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(rename_all = "snake_case", tag = "kind")]
pub(crate) enum Refusal {
	/// The commands file did not parse; nothing ran.
	CommandsUnreadable { message: String },
	/// The application could not open or run the source.
	ApplicationFailed { message: String },
	/// The token names no figure in the application.
	UnknownFigure { figure: FigureRef },
	/// The token named a figure that has since been replaced; `current` is
	/// the incarnation that stands in its pane now.
	StaleFigure { figure: FigureRef, current: String },
	/// The ordinal is not in the figure's series table.
	UnknownSeries {
		figure: String,
		series: u32,
		series_count: u32,
	},
	/// The figure has no series table to act on.
	NoSeriesTable { figure: String, reason: String },
	/// The resize could not be dispatched or did not take.
	ResizeRefused { figure: String, message: String },
	/// The publish action refused, with the publisher's own reason.
	PublishRefused { reason: PublishRefusal },
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
	pub(crate) loaded: LiveState,
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
pub(crate) fn drive(source: &Path, commands: &Path, receipt: &Path) -> Result<(), String> {
	let target = Target::Source(source.to_path_buf());
	let script = match read_commands(commands) {
		Ok(script) => script,
		Err(message) => {
			let refused = Receipt {
				schema_version: RECEIPT_SCHEMA_VERSION,
				source: target.describe(),
				loaded: Vec::new(),
				commands: Vec::new(),
				refusal: Some(Refusal::CommandsUnreadable { message }),
			};
			write_receipt(receipt, &refused)?;
			return Err(format!("commands refused; see {}", receipt.display()));
		}
	};
	let receipt_document = run(&target, &script);
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
	let receipt_document = run(target, &script);
	let text = serde_json::to_string_pretty(&receipt_document)
		.map_err(|error| format!("cannot encode the receipt: {error}"))?;
	println!("{text}");
	match &receipt_document.refusal {
		Some(refusal) => Err(format!("{refusal:?}")),
		None => Ok(()),
	}
}

fn read_commands(path: &Path) -> Result<Commands, String> {
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
pub(crate) fn run(target: &Target, script: &Commands) -> Receipt {
	let mut receipt = Receipt {
		schema_version: RECEIPT_SCHEMA_VERSION,
		source: target.describe(),
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
	if let Err(message) = application.run_source(RUN_DEADLINE) {
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

	#[cfg(test)]
	pub(crate) fn into_application(self) -> HeadlessApplication {
		self.application
	}

	#[cfg(test)]
	pub(crate) fn application_mut(&mut self) -> &mut HeadlessApplication {
		&mut self.application
	}

	pub(crate) fn apply(&mut self, command: &Command) -> CommandReceipt {
		let mut answer = CommandReceipt {
			command: command.clone(),
			outcome: Outcome::Applied,
			figure: None,
			refusal: None,
			state: None,
			figures: None,
			publish: None,
		};
		match command {
			Command::Describe {} => {
				answer.figures = Some(self.describe_live());
			}
			Command::Hide { figure, series }
			| Command::Show { figure, series }
			| Command::Isolate { figure, series } => {
				answer.acted(self.visibility(command, figure, *series));
			}
			Command::Restore { figure } => answer.acted(self.restore(figure)),
			Command::Resize {
				figure,
				width,
				height,
			} => answer.acted(self.resize(figure, *width, *height)),
			Command::Publish { directory } => match self.publish(Path::new(directory)) {
				Ok(published) => answer.publish = Some(published),
				Err(refusal) => answer.refuse(refusal),
			},
		}
		answer
	}

	fn describe_live(&self) -> Vec<FigureDescription> {
		self.application.describe_surface()
	}

	/// Resolve a figure reference to the pane it names now. A token that is
	/// no longer live but was loaded by this invocation is stale, and the
	/// refusal names the incarnation now in its pane.
	fn resolve(&self, figure: &FigureRef) -> Result<Resolved, Refusal> {
		let mut exports = self.application.exports_with_identity();
		let position = match figure {
			FigureRef::Incarnation(token) => {
				let parsed: u64 = token.parse().map_err(|_| Refusal::UnknownFigure {
					figure: figure.clone(),
				})?;
				let live = exports
					.iter()
					.position(|(_, incarnation, _)| incarnation.token() == parsed);
				if live.is_none()
					&& let Some(loaded) = self.loaded.iter().find(|loaded| loaded.figure == *token)
					&& let Some((_, current, _)) = exports
						.iter()
						.find(|(panel, _, _)| panel.get() == loaded.pane)
				{
					return Err(Refusal::StaleFigure {
						figure: figure.clone(),
						current: current.token().to_string(),
					});
				}
				live
			}
			FigureRef::View(name) => exports
				.iter()
				.position(|(_, _, export)| export.view_name.as_deref() == Some(name)),
		};
		let Some(position) = position else {
			return Err(Refusal::UnknownFigure {
				figure: figure.clone(),
			});
		};
		let (panel, incarnation, export) = exports.swap_remove(position);
		Ok(Resolved {
			panel,
			incarnation,
			export,
		})
	}

	fn visibility(
		&mut self,
		command: &Command,
		figure: &FigureRef,
		series: u32,
	) -> Result<(String, LiveState), Refusal> {
		let resolved = self.resolve(figure)?;
		let token = resolved.incarnation.token().to_string();
		series_table_of(&resolved.export).map_err(|reason| Refusal::NoSeriesTable {
			figure: token.clone(),
			reason,
		})?;
		let widget = widget_for(&resolved.export, &resolved.export.settings_model);
		let identities = widget.series_registry().identities().to_vec();
		drop(widget);
		let identity =
			identities
				.get(series as usize)
				.cloned()
				.ok_or_else(|| Refusal::UnknownSeries {
					figure: token.clone(),
					series,
					series_count: identities.len() as u32,
				})?;
		let action = match command {
			Command::Hide { .. } => SeriesVisibilityAction::Hide(identity),
			Command::Show { .. } => SeriesVisibilityAction::Show(identity),
			Command::Isolate { .. } => SeriesVisibilityAction::Isolate(identity),
			_ => unreachable!("visibility is only called for hide, show, and isolate"),
		};
		self.application
			.plot_message(resolved.panel, PlotPaneMsg::SeriesVisibility(action));
		Ok((token, self.live_state(resolved.panel)))
	}

	fn restore(&mut self, figure: &FigureRef) -> Result<(String, LiveState), Refusal> {
		let resolved = self.resolve(figure)?;
		self.application.plot_message(
			resolved.panel,
			PlotPaneMsg::SeriesVisibility(SeriesVisibilityAction::RestoreAll),
		);
		Ok((
			resolved.incarnation.token().to_string(),
			self.live_state(resolved.panel),
		))
	}

	fn resize(
		&mut self,
		figure: &FigureRef,
		width: f32,
		height: f32,
	) -> Result<(String, LiveState), Refusal> {
		let resolved = self.resolve(figure)?;
		let token = resolved.incarnation.token().to_string();
		self.application
			.resize_pane(resolved.panel, width, height, SETTLE_DEADLINE)
			.map_err(|message| Refusal::ResizeRefused {
				figure: token.clone(),
				message,
			})?;
		Ok((token, self.live_state(resolved.panel)))
	}

	fn publish(&mut self, directory: &Path) -> Result<PublishReceipt, Refusal> {
		let package =
			self.application
				.publish(directory)
				.map_err(|error| Refusal::PublishRefused {
					reason: PublishRefusal::from(&error),
				})?;
		Ok(PublishReceipt {
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
		})
	}

	/// The pane's state now. A pane that has no export any more reports the
	/// default, which the receipt's outcome already explains.
	fn live_state(&self, panel: HostPanelId) -> LiveState {
		self.application
			.exports_with_identity()
			.iter()
			.find(|(id, _, _)| *id == panel)
			.map(|(_, _, export)| live_state_of(export))
			.unwrap_or_default()
	}
}

struct Resolved {
	panel: HostPanelId,
	incarnation: SurfaceIncarnation,
	export: HostPlotExport,
}

impl CommandReceipt {
	fn acted(&mut self, result: Result<(String, LiveState), Refusal>) {
		match result {
			Ok((figure, state)) => {
				self.figure = Some(figure);
				self.state = Some(state);
			}
			Err(refusal) => self.refuse(refusal),
		}
	}

	fn refuse(&mut self, refusal: Refusal) {
		self.outcome = Outcome::Refused;
		self.refusal = Some(refusal);
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
fn series_table_of(export: &HostPlotExport) -> Result<crate::publication::SeriesTable, String> {
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
		loaded: LiveState::default(),
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
		assert_eq!(description.loaded.hidden, Vec::<u32>::new());
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
				{"publish":{"directory":"/tmp/out"}}
			]}"#,
		)
		.expect("commands parse");
		assert_eq!(script.commands.len(), 6);
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
		let outcome = drive(Path::new("/nonexistent/source.rn"), &commands, &receipt);
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
}
