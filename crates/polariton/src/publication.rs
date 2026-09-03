//! PRODUCT-0154: the published dashboard package.
//!
//! A package is a directory with a versioned, content-addressed manifest. This
//! module owns the package's DTOs and their explicit conversion from runtime
//! types, canonical encodings, the publication guard, the HTML shell, and the
//! deliberately dumb server. Runtime plot, editor, and selection types never
//! implement `Serialize`; placement identities, artifact tokens, and renderer
//! vocabulary cannot enter a package.

use crate::editor::HostPlotExport;
use crate::plot::common::{PlotRenderLayer, PlotWidget};
use crate::plot::export::{SvgBackend, xml_escape};
use crate::plot::recipe::PlotConstruction;
use crate::plot::spec::{ResolvedChannel, ResolvedPlotSpec, ScaleFamily, SemanticType};
use crate::presentation_registry::{SettingValue, inspector_rows};
use crate::selection::{Predicate, Publication};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

pub(crate) const MANIFEST_SCHEMA_VERSION: u32 = 1;
pub(crate) const FIGURE_SCHEMA_VERSION: u32 = 1;
pub(crate) const MANIFEST_FILE: &str = "manifest.json";
pub(crate) const SHELL_FILE: &str = "index.html";
pub(crate) const FONT_FILE: &str = "fonts/PolaritonPlotSans-Regular.ttf";
pub(crate) const FONT_LICENCE_FILE: &str = "fonts/NotoSans-OFL-1.1.txt";
pub(crate) const FONT_FAMILY: &str = "Polariton Plot Sans";
const FONT_LICENCE: &str = include_str!("../assets/fonts/NotoSans-OFL-1.1.txt");
const UNKNOWN_PRODUCER: &str = "unknown";

// ---------------------------------------------------------------------------
// Manifest and figure DTOs. Field order is irrelevant: canonical JSON sorts.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Manifest {
	pub(crate) schema_version: u32,
	pub(crate) publisher: Publisher,
	/// Event metadata. Excluded from the identity projection: publishing the
	/// same dashboard twice is the same package at two moments.
	pub(crate) published_at: String,
	pub(crate) figures: Vec<FigureEntry>,
	pub(crate) datasets: Vec<DatasetEntry>,
	pub(crate) fonts: Vec<FontEntry>,
	pub(crate) certificates: Vec<CertificateEntry>,
	pub(crate) files: BTreeMap<String, FileEntry>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Publisher {
	pub(crate) name: String,
	pub(crate) version: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FigureEntry {
	pub(crate) id: String,
	/// `named` when the id derives from a named view, `ordinal` when it is
	/// the export ordinal because the view was anonymous or unattached.
	pub(crate) identity: String,
	/// PRODUCT-0010 §5's durable document identity when the source buffer had
	/// one; absent for untitled or unattached sources.
	pub(crate) document: Option<u64>,
	pub(crate) title: String,
	pub(crate) subtitle: Option<String>,
	pub(crate) family: String,
	pub(crate) dataset: String,
	pub(crate) layout: GridRect,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct GridRect {
	pub(crate) col: u32,
	pub(crate) row: u32,
	pub(crate) width: u32,
	pub(crate) height: u32,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct DatasetEntry {
	pub(crate) sha256: String,
	pub(crate) rows: u64,
	pub(crate) fields: Vec<String>,
	/// Every distinct provenance assertion made for this content. Two
	/// figures over byte-identical data from different producers share the
	/// file and keep both assertions.
	pub(crate) provenances: Vec<Provenance>,
}

/// Source-neutral provenance. Never an endpoint, a path, a credential, or an
/// adapter field value; a certificate is a reference to an embedded file
/// under `certificates/` and is absent when none exists.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Provenance {
	pub(crate) producer: String,
	pub(crate) source_name: String,
	pub(crate) lineage: Option<String>,
	pub(crate) revision: Option<u64>,
	pub(crate) certificate: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FontEntry {
	pub(crate) family: String,
	pub(crate) file: String,
	pub(crate) licence: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct CertificateEntry {
	pub(crate) dataset: String,
	pub(crate) file: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FileEntry {
	pub(crate) bytes: u64,
	pub(crate) sha256: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FigureDefinition {
	pub(crate) schema_version: u32,
	pub(crate) family: String,
	pub(crate) construction: String,
	pub(crate) facet: Option<String>,
	/// The view expression exactly as authored, when the figure was born from
	/// Rune source; absent for figures without an attached source.
	pub(crate) authored_expression: Option<String>,
	pub(crate) channels: Vec<FigureChannel>,
	/// Session selection is never published: the figure is the authored
	/// figure over its full dataset, so data and ink always agree.
	pub(crate) selection: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FigureChannel {
	pub(crate) channel: String,
	pub(crate) field: String,
	pub(crate) semantic_type: String,
	pub(crate) scale: String,
	pub(crate) layer: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FigurePresentation {
	pub(crate) schema_version: u32,
	pub(crate) settings: BTreeMap<String, PresentationSetting>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct PresentationSetting {
	pub(crate) section: String,
	pub(crate) value: serde_json::Value,
	pub(crate) provenance: String,
}

// ---------------------------------------------------------------------------
// Canonical encodings and digests.
// ---------------------------------------------------------------------------

/// Sorted keys, no insignificant whitespace, one trailing newline. `serde_json`
/// maps are `BTreeMap`s in this workspace, so re-serializing through `Value`
/// sorts every object.
pub(crate) fn canonical_json<T: Serialize>(value: &T) -> Result<Vec<u8>, String> {
	let value = serde_json::to_value(value).map_err(|error| error.to_string())?;
	let mut bytes = serde_json::to_vec(&value).map_err(|error| error.to_string())?;
	bytes.push(b'\n');
	Ok(bytes)
}

pub(crate) fn sha256_hex(bytes: &[u8]) -> String {
	let digest = Sha256::digest(bytes);
	let mut hex = String::with_capacity(64);
	for byte in digest {
		use std::fmt::Write as _;
		let _ = write!(hex, "{byte:02x}");
	}
	hex
}

impl Manifest {
	/// The identity projection: the manifest with its event metadata cleared.
	/// The package digest is the digest of this projection, so identity is
	/// content and only content.
	pub(crate) fn identity_projection(&self) -> Manifest {
		let mut projection = self.clone();
		projection.published_at = String::new();
		projection
	}
}

// ---------------------------------------------------------------------------
// Vocabulary. Stable strings, never `Debug` output.
// ---------------------------------------------------------------------------

fn semantic_type_name(semantic: SemanticType) -> &'static str {
	match semantic {
		SemanticType::Nominal => "nominal",
		SemanticType::Ordinal => "ordinal",
		SemanticType::Quantitative => "quantitative",
		SemanticType::Temporal => "temporal",
	}
}

fn scale_family_name(scale: ScaleFamily) -> &'static str {
	match scale {
		ScaleFamily::Point => "point",
		ScaleFamily::Band => "band",
		ScaleFamily::Linear => "linear",
		ScaleFamily::Log => "log",
		ScaleFamily::Time => "time",
		ScaleFamily::CategoricalColor => "categorical-color",
		ScaleFamily::SequentialColor => "sequential-color",
		ScaleFamily::DivergingColor => "diverging-color",
	}
}

pub(crate) fn family_name(plot_type: crate::plot::core::PlotType) -> String {
	let debug = format!("{plot_type:?}");
	let mut name = String::with_capacity(debug.len() + 4);
	for (index, ch) in debug.chars().enumerate() {
		if ch.is_ascii_uppercase() {
			if index > 0 {
				name.push('-');
			}
			name.push(ch.to_ascii_lowercase());
		} else {
			name.push(ch);
		}
	}
	name
}

// ---------------------------------------------------------------------------
// Assembly.
// ---------------------------------------------------------------------------

/// What the interface supplies beyond the exports: the publish moment, and,
/// separately, everything the guard must refuse — every secure adapter field
/// value entered or unlocked in this session, the ad hoc connection string,
/// and this machine's home and working directories.
#[derive(Clone, Debug, Default)]
pub(crate) struct PublishRequest {
	pub(crate) published_at: String,
	pub(crate) secrets: Vec<String>,
	pub(crate) forbidden_paths: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum PublishError {
	NoFigures,
	Republished { figure: String },
	NoRecipe { figure: String },
	Rebuild { figure: String, error: String },
	Data { figure: String, error: String },
	Secret { file: String, field: String },
	Encoding(String),
	Write(String),
}

impl std::fmt::Display for PublishError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::NoFigures => write!(f, "Publish · refused: no live plots to publish"),
			Self::Republished { figure } => write!(
				f,
				"Publish · refused: {figure} republished while the package was assembled; retry"
			),
			Self::NoRecipe { figure } => write!(
				f,
				"Publish · refused: {figure} has no rebuild recipe, so its data cannot be published"
			),
			Self::Rebuild { figure, error } => {
				write!(
					f,
					"Publish · refused: {figure} could not be rebuilt: {error}"
				)
			}
			Self::Data { figure, error } => {
				write!(
					f,
					"Publish · refused: {figure} data could not be encoded: {error}"
				)
			}
			Self::Secret { file, field } => write!(
				f,
				"Publish · refused: {file} ({field}) would carry a secret, a local path, or an endpoint"
			),
			Self::Encoding(error) => write!(f, "Publish · refused: {error}"),
			Self::Write(error) => write!(f, "Publish · refused: {error}"),
		}
	}
}

/// A finished package held in memory: every file by relative path, and the
/// manifest that lists them. Writing is a separate step so a package can be
/// digested, guarded, and compared without touching disk.
#[derive(Clone, Debug)]
pub(crate) struct Package {
	pub(crate) manifest: Manifest,
	pub(crate) files: BTreeMap<String, Vec<u8>>,
}

impl Package {
	/// Content identity: the digest of the manifest's identity projection.
	pub(crate) fn digest(&self) -> String {
		canonical_json(&self.manifest.identity_projection())
			.map(|bytes| sha256_hex(&bytes))
			.unwrap_or_default()
	}

	/// PRODUCT-0136's floors: the path is authored, an existing directory
	/// refuses, a failed write is a diagnostic.
	pub(crate) fn write(&self, root: &Path) -> Result<(), PublishError> {
		std::fs::create_dir(root).map_err(|error| {
			PublishError::Write(format!("{} could not be created: {error}", root.display()))
		})?;
		for (relative, bytes) in &self.files {
			let path = root.join(relative);
			if let Some(parent) = path.parent() {
				std::fs::create_dir_all(parent).map_err(|error| {
					PublishError::Write(format!("{}: {error}", parent.display()))
				})?;
			}
			std::fs::write(&path, bytes)
				.map_err(|error| PublishError::Write(format!("{}: {error}", path.display())))?;
		}
		Ok(())
	}
}

/// Every string that will be serialized into the package, named by the file
/// and field it belongs to, checked structurally before any escaping happens.
struct Guarded<'a> {
	needles: Vec<&'a str>,
}

impl<'a> Guarded<'a> {
	fn new(request: &'a PublishRequest) -> Self {
		Self {
			needles: request
				.secrets
				.iter()
				.chain(request.forbidden_paths.iter())
				.map(String::as_str)
				.filter(|needle| !needle.trim().is_empty())
				.collect(),
		}
	}

	fn check(&self, file: &str, field: &str, value: &str) -> Result<(), PublishError> {
		let refuse = || PublishError::Secret {
			file: file.to_owned(),
			field: field.to_owned(),
		};
		if self.needles.iter().any(|needle| value.contains(needle)) {
			return Err(refuse());
		}
		if looks_like_path_or_endpoint(value) {
			return Err(refuse());
		}
		Ok(())
	}
}

/// An absolute path on any platform, a home-relative path, or a URL with a
/// scheme. Data-derived labels never legitimately look like these.
pub(crate) fn looks_like_path_or_endpoint(value: &str) -> bool {
	let trimmed = value.trim();
	if trimmed.starts_with('/') || trimmed.starts_with("~/") || trimmed.starts_with("\\\\") {
		return true;
	}
	let mut chars = trimmed.chars();
	if let (Some(drive), Some(':'), Some(sep)) = (chars.next(), chars.next(), chars.next()) {
		if drive.is_ascii_alphabetic() && (sep == '\\' || sep == '/') {
			return true;
		}
	}
	trimmed.contains("://")
}

/// The consistency witness: tokens captured from the live panels before the
/// exports were taken. A figure whose token moved between capture and read
/// republished, and the package refuses rather than mixing two publications.
pub(crate) fn assemble(
	witness: &[Option<Publication>],
	exports: Vec<HostPlotExport>,
	request: &PublishRequest,
) -> Result<Package, PublishError> {
	if exports.is_empty() {
		return Err(PublishError::NoFigures);
	}
	let guarded = Guarded::new(request);
	let mut files: BTreeMap<String, Vec<u8>> = BTreeMap::new();
	let mut figures = Vec::new();
	let mut datasets: BTreeMap<String, DatasetEntry> = BTreeMap::new();
	let mut used_ids = std::collections::HashSet::new();

	for (index, mut export) in exports.into_iter().enumerate() {
		let figure_name = export.title.clone();
		let captured = witness.get(index).copied().flatten();
		if captured.map(|p| p.token) != export.publication.map(|p| p.token) {
			return Err(PublishError::Republished {
				figure: figure_name,
			});
		}
		let recipe = export
			.recipe
			.clone()
			.ok_or_else(|| PublishError::NoRecipe {
				figure: figure_name.clone(),
			})?;
		// The authored figure over its full dataset: a displayed selection
		// filters ink but never the packaged data, so the two must agree.
		if !export.predicate.fields.is_empty()
			|| export.displayed_selection_revision != export.selection_revision
		{
			let kernel =
				recipe
					.rebuild(&Predicate::default())
					.map_err(|error| PublishError::Rebuild {
						figure: figure_name.clone(),
						error: error.to_string(),
					})?;
			export.kernel = kernel;
			export.render_revision = export.render_revision.wrapping_add(1);
		}

		// Identity: reserve the final id, however many collisions.
		let (base, identity) = match export.view_name.as_deref() {
			Some(name) => (slug(name), "named"),
			None => (format!("figure-{}", index + 1), "ordinal"),
		};
		let mut id = base.clone();
		let mut attempt = 2;
		while !used_ids.insert(id.clone()) {
			id = format!("{base}-{attempt}");
			attempt += 1;
		}
		let figure_dir = format!("figures/{id}");

		// Data, shared by digest, provenance travelling with the artifact.
		let ipc = recipe
			.artifact
			.canonical_ipc()
			.map_err(|error| PublishError::Data {
				figure: figure_name.clone(),
				error: error.to_string(),
			})?;
		let dataset_digest = sha256_hex(&ipc);
		let (revision, source_name, mut definition) =
			definition_from_construction(&recipe.construction);
		definition.authored_expression = export.authored_expression.clone();
		let provenance = Provenance {
			producer: recipe
				.artifact
				.producer()
				.unwrap_or(UNKNOWN_PRODUCER)
				.to_owned(),
			source_name,
			lineage: None,
			revision,
			certificate: None,
		};
		guarded.check(MANIFEST_FILE, "provenance.producer", &provenance.producer)?;
		guarded.check(
			MANIFEST_FILE,
			"provenance.source_name",
			&provenance.source_name,
		)?;
		let entry = datasets
			.entry(dataset_digest.clone())
			.or_insert_with(|| DatasetEntry {
				sha256: dataset_digest.clone(),
				rows: recipe.artifact.height() as u64,
				fields: recipe
					.artifact
					.schema()
					.fields()
					.iter()
					.map(|field| field.name().to_owned())
					.collect(),
				provenances: Vec::new(),
			});
		if !entry.provenances.contains(&provenance) {
			entry.provenances.push(provenance);
		}
		files.insert(format!("data/{dataset_digest}.arrow"), ipc);

		// Definition and presentation, checked field by field before encoding.
		let definition_file = format!("{figure_dir}/definition.json");
		guarded.check(MANIFEST_FILE, "figure.title", &export.title)?;
		if let Some(expression) = &definition.authored_expression {
			guarded.check(&definition_file, "authored_expression", expression)?;
		}
		for channel in &definition.channels {
			guarded.check(&definition_file, "channel.field", &channel.field)?;
		}
		let resolved = export.kernel.resolve_settings(&export.settings_model);
		let mut settings = BTreeMap::new();
		let mut subtitle = None;
		let presentation_file = format!("{figure_dir}/presentation.json");
		for row in inspector_rows(&export.settings_model, &resolved, export.plot_type, None) {
			let value = setting_value_json(&row.value);
			if let SettingValue::Text(text) | SettingValue::Choice(text) = &row.value {
				guarded.check(&presentation_file, row.label, text)?;
				if row.label == "Subtitle" && !text.is_empty() {
					subtitle = Some(text.clone());
				}
			}
			settings.insert(
				row.label.to_owned(),
				PresentationSetting {
					section: format!("{:?}", row.section),
					value,
					provenance: row.provenance.to_string(),
				},
			);
		}
		if let Some(subtitle) = &subtitle {
			guarded.check(MANIFEST_FILE, "figure.subtitle", subtitle)?;
		}
		let presentation = FigurePresentation {
			schema_version: FIGURE_SCHEMA_VERSION,
			settings,
		};

		// Fallback render. Labels drawn from data are checked in their raw
		// form here; the SVG escapes them afterwards.
		let svg = render_figure_svg(&export);
		let svg_file = format!("{figure_dir}/figure.svg");
		for label in svg_text_contents(&svg) {
			guarded.check(&svg_file, "text", &unescape_xml(&label))?;
		}
		let family = export
			.plot_type
			.map_or_else(|| "unknown".to_owned(), family_name);
		figures.push(FigureEntry {
			id: id.clone(),
			identity: identity.to_owned(),
			document: export.document,
			title: export.title.clone(),
			subtitle,
			family: family.clone(),
			dataset: dataset_digest,
			layout: export.layout,
		});
		files.insert(
			definition_file,
			canonical_json(&FigureDefinition {
				family,
				..definition
			})
			.map_err(PublishError::Encoding)?,
		);
		files.insert(
			presentation_file,
			canonical_json(&presentation).map_err(PublishError::Encoding)?,
		);
		files.insert(svg_file, svg.into_bytes());
	}

	files.insert(
		FONT_FILE.to_owned(),
		crate::fonts::POLARITON_PLOT_SANS_REGULAR.to_vec(),
	);
	files.insert(
		FONT_LICENCE_FILE.to_owned(),
		FONT_LICENCE.as_bytes().to_vec(),
	);
	files.insert(
		SHELL_FILE.to_owned(),
		shell_html(&figures, &files).into_bytes(),
	);

	let mut manifest = Manifest {
		schema_version: MANIFEST_SCHEMA_VERSION,
		publisher: Publisher {
			name: "polariton".to_owned(),
			version: env!("CARGO_PKG_VERSION").to_owned(),
		},
		published_at: request.published_at.clone(),
		figures,
		datasets: datasets.into_values().collect(),
		fonts: vec![FontEntry {
			family: FONT_FAMILY.to_owned(),
			file: FONT_FILE.to_owned(),
			licence: FONT_LICENCE_FILE.to_owned(),
		}],
		certificates: Vec::new(),
		files: BTreeMap::new(),
	};
	for (relative, bytes) in &files {
		manifest.files.insert(
			relative.clone(),
			FileEntry {
				bytes: bytes.len() as u64,
				sha256: sha256_hex(bytes),
			},
		);
	}
	let manifest_bytes = canonical_json(&manifest).map_err(PublishError::Encoding)?;
	files.insert(MANIFEST_FILE.to_owned(), manifest_bytes);

	// A second, mechanical pass over the serialized text: raw needles and
	// their JSON and XML escaped forms, so escaping cannot hide a value the
	// structural pass did not see.
	guard(&files, request)?;
	Ok(Package { manifest, files })
}

/// The serialized-text half of the publication guard.
pub(crate) fn guard(
	files: &BTreeMap<String, Vec<u8>>,
	request: &PublishRequest,
) -> Result<(), PublishError> {
	let mut needles: Vec<String> = Vec::new();
	for needle in request.secrets.iter().chain(request.forbidden_paths.iter()) {
		if needle.trim().is_empty() {
			continue;
		}
		needles.push(needle.clone());
		needles.push(xml_escape(needle));
		if let Ok(json) = serde_json::to_string(needle) {
			needles.push(json.trim_matches('"').to_owned());
		}
	}
	if needles.is_empty() {
		return Ok(());
	}
	for (relative, bytes) in files {
		if relative.starts_with("data/") || relative.starts_with("fonts/") {
			continue;
		}
		let text = String::from_utf8_lossy(bytes);
		if needles.iter().any(|needle| text.contains(needle.as_str())) {
			return Err(PublishError::Secret {
				file: relative.clone(),
				field: "serialized text".to_owned(),
			});
		}
	}
	Ok(())
}

fn definition_from_construction(
	construction: &PlotConstruction,
) -> (Option<u64>, String, FigureDefinition) {
	let mut definition = FigureDefinition {
		schema_version: FIGURE_SCHEMA_VERSION,
		family: String::new(),
		construction: String::new(),
		facet: None,
		authored_expression: None,
		channels: Vec::new(),
		selection: "none".to_owned(),
	};
	let resolved: Option<&ResolvedPlotSpec> = match construction {
		PlotConstruction::Single { spec, .. } => {
			definition.construction = "single".to_owned();
			Some(spec)
		}
		PlotConstruction::Facet { spec, facet, .. } => {
			definition.construction = "facet".to_owned();
			definition.facet = facet.clone();
			Some(spec.as_ref())
		}
		PlotConstruction::Layered {
			specification,
			facet,
			..
		} => {
			definition.construction = "layered".to_owned();
			definition.facet = facet.clone();
			for (index, layer) in specification.layers.iter().enumerate() {
				let layer_name = format!("layer-{index}:{}", family_name(layer.mark.plot_type()));
				let mut push = |channel: &str, field: Option<&str>| {
					if let Some(field) = field {
						definition.channels.push(FigureChannel {
							channel: channel.to_owned(),
							field: field.to_owned(),
							semantic_type: "authored".to_owned(),
							scale: "authored".to_owned(),
							layer: layer_name.clone(),
						});
					}
				};
				push("x", Some(authored_field(&layer.encoding.x.field)));
				push("y", Some(authored_field(&layer.encoding.y.field)));
				let color = match &layer.encoding.color.field {
					crate::plot::spec::OptionalAuthoredChannel::Named(name) => Some(name.as_str()),
					crate::plot::spec::OptionalAuthoredChannel::Auto => Some("auto"),
					crate::plot::spec::OptionalAuthoredChannel::Absent => None,
				};
				push("color", color);
			}
			None
		}
	};
	match resolved {
		Some(spec) => {
			definition.channels = spec.channels.iter().map(binding).collect();
			(
				spec.provenance.revision,
				spec.provenance.source_name.clone(),
				definition,
			)
		}
		None => (None, "layered".to_owned(), definition),
	}
}

fn authored_field(channel: &crate::plot::spec::AuthoredChannel) -> &str {
	match channel {
		crate::plot::spec::AuthoredChannel::Named(name) => name.as_str(),
		crate::plot::spec::AuthoredChannel::Auto => "auto",
	}
}

fn binding(channel: &ResolvedChannel) -> FigureChannel {
	let bound = &channel.binding;
	FigureChannel {
		channel: bound.channel.to_string(),
		field: bound.field.clone(),
		semantic_type: semantic_type_name(bound.semantic_type).to_owned(),
		scale: scale_family_name(bound.scale).to_owned(),
		layer: format!(
			"layer-{}:{}",
			bound.layer.ordinal,
			family_name(bound.layer.plot_type)
		),
	}
}

fn setting_value_json(value: &SettingValue) -> serde_json::Value {
	match value {
		SettingValue::Text(text) | SettingValue::Choice(text) => {
			serde_json::Value::String(text.clone())
		}
		SettingValue::Number(number) => serde_json::json!(number),
		SettingValue::Integer(integer) => serde_json::json!(integer),
		SettingValue::Toggle(toggle) => serde_json::Value::Bool(*toggle),
		SettingValue::Pair(a, b) => serde_json::json!([a, b]),
		SettingValue::Unavailable => serde_json::Value::Null,
	}
}

fn render_figure_svg(export: &HostPlotExport) -> String {
	let bounds = iced::Rectangle {
		x: 0.0,
		y: 0.0,
		width: export.width,
		height: export.height,
	};
	let widget = PlotWidget::new(
		export.kernel.as_ref(),
		export.title.clone(),
		20.0,
		export.settings_model.clone(),
		export.render_revision,
		export.resize_render_suspended,
		PlotRenderLayer::Full,
	);
	let mut backend = SvgBackend::new(export.width, export.height);
	widget.render(&mut backend, bounds);
	backend.finish()
}

/// The text content of every `<text>` element the backend emitted, still
/// escaped, in document order.
fn svg_text_contents(svg: &str) -> Vec<String> {
	svg.split("<text ")
		.skip(1)
		.filter_map(|rest| {
			let start = rest.find('>')? + 1;
			let end = rest[start..].find("</text>")? + start;
			Some(rest[start..end].to_owned())
		})
		.collect()
}

fn unescape_xml(text: &str) -> String {
	text.replace("&lt;", "<")
		.replace("&gt;", ">")
		.replace("&quot;", "\"")
		.replace("&apos;", "'")
		.replace("&amp;", "&")
}

fn slug(name: &str) -> String {
	let mut slug = String::with_capacity(name.len());
	let mut last_dash = true;
	for ch in name.chars() {
		if ch.is_ascii_alphanumeric() {
			slug.push(ch.to_ascii_lowercase());
			last_dash = false;
		} else if !last_dash {
			slug.push('-');
			last_dash = true;
		}
	}
	let slug = slug.trim_end_matches('-').to_owned();
	if slug.is_empty() {
		"view".to_owned()
	} else {
		slug
	}
}

// ---------------------------------------------------------------------------
// The HTML shell: a document, no script.
// ---------------------------------------------------------------------------

pub(crate) fn html_escape(text: &str) -> String {
	let mut out = String::with_capacity(text.len());
	for ch in text.chars() {
		match ch {
			'&' => out.push_str("&amp;"),
			'<' => out.push_str("&lt;"),
			'>' => out.push_str("&gt;"),
			'"' => out.push_str("&quot;"),
			'\'' => out.push_str("&#39;"),
			_ => out.push(ch),
		}
	}
	out
}

fn shell_html(figures: &[FigureEntry], files: &BTreeMap<String, Vec<u8>>) -> String {
	let columns = figures
		.iter()
		.map(|figure| figure.layout.col + figure.layout.width)
		.max()
		.unwrap_or(1)
		.max(1);
	let mut html = String::new();
	html.push_str("<!DOCTYPE html>\n<html lang=\"en\">\n<head>\n<meta charset=\"utf-8\">\n");
	html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\">\n");
	html.push_str("<title>Polariton dashboard</title>\n<style>\n");
	html.push_str(&format!(
		"@font-face {{ font-family: \"{FONT_FAMILY}\"; src: url(\"{FONT_FILE}\") format(\"truetype\"); }}\n"
	));
	html.push_str("html, body { margin: 0; background: #1a1a1a; color: #e6e6e6; }\n");
	html.push_str(&format!(
		"body {{ font-family: \"{FONT_FAMILY}\", sans-serif; }}\n"
	));
	html.push_str(&format!(
		".dashboard {{ display: grid; grid-template-columns: repeat({columns}, 1fr); gap: 8px; padding: 8px; }}\n"
	));
	html.push_str(
		".figure { min-width: 0; }\n.figure svg { width: 100%; height: auto; display: block; }\n",
	);
	html.push_str("</style>\n</head>\n<body>\n<main class=\"dashboard\">\n");
	for figure in figures {
		let svg = files
			.get(&format!("figures/{}/figure.svg", figure.id))
			.map(|bytes| String::from_utf8_lossy(bytes).into_owned())
			.unwrap_or_default();
		html.push_str(&format!(
			"<figure class=\"figure\" id=\"{}\" style=\"grid-column: {} / span {}; grid-row: {} / span {};\" aria-label=\"{}\">\n",
			html_escape(&figure.id),
			figure.layout.col + 1,
			figure.layout.width.max(1),
			figure.layout.row + 1,
			figure.layout.height.max(1),
			html_escape(&figure.title),
		));
		html.push_str(&svg);
		html.push_str("\n</figure>\n");
	}
	html.push_str("</main>\n</body>\n</html>\n");
	html
}

// ---------------------------------------------------------------------------
// The deliberately dumb server.
// ---------------------------------------------------------------------------

/// A package as served. Every listed file is verified against the manifest
/// at open — a regular file, no symlink, a safe relative path, the listed
/// length and digest — and pinned in memory. Nothing is read from disk after
/// open, so a mutated or swapped file can never be served under a stale tag.
pub(crate) struct ServedPackage {
	manifest_bytes: Vec<u8>,
	manifest: Manifest,
	files: BTreeMap<String, Vec<u8>>,
}

pub(crate) struct ServedResponse {
	pub(crate) status: u16,
	pub(crate) headers: Vec<(&'static str, String)>,
	pub(crate) body: Vec<u8>,
}

fn safe_relative_path(relative: &str) -> bool {
	!relative.is_empty()
		&& !relative.contains('\\')
		&& !relative.starts_with('/')
		&& relative
			.split('/')
			.all(|segment| !segment.is_empty() && segment != "." && segment != "..")
}

impl ServedPackage {
	pub(crate) fn open(root: &Path) -> Result<Self, String> {
		let manifest_bytes = std::fs::read(root.join(MANIFEST_FILE))
			.map_err(|error| format!("{} is not a package: {error}", root.display()))?;
		let manifest: Manifest = serde_json::from_slice(&manifest_bytes)
			.map_err(|error| format!("{} manifest is invalid: {error}", root.display()))?;
		if manifest.schema_version != MANIFEST_SCHEMA_VERSION {
			return Err(format!(
				"package schema {} is not the served schema {}",
				manifest.schema_version, MANIFEST_SCHEMA_VERSION
			));
		}
		let mut files = BTreeMap::new();
		for (relative, entry) in &manifest.files {
			if !safe_relative_path(relative) {
				return Err(format!("manifest lists an unsafe path {relative:?}"));
			}
			let path = root.join(relative);
			let metadata =
				std::fs::symlink_metadata(&path).map_err(|error| format!("{relative}: {error}"))?;
			if !metadata.file_type().is_file() {
				return Err(format!("{relative} is not a regular file"));
			}
			let bytes = std::fs::read(&path).map_err(|error| format!("{relative}: {error}"))?;
			if bytes.len() as u64 != entry.bytes || sha256_hex(&bytes) != entry.sha256 {
				return Err(format!("{relative} does not match its manifest entry"));
			}
			files.insert(relative.clone(), bytes);
		}
		Ok(Self {
			manifest_bytes,
			manifest,
			files,
		})
	}

	pub(crate) fn respond(&self, method: &str, target: &str) -> ServedResponse {
		if method != "GET" && method != "HEAD" {
			return plain(405, "method not allowed");
		}
		let path = target
			.split('?')
			.next()
			.unwrap_or("")
			.trim_start_matches('/');
		let relative = if path.is_empty() {
			if self.files.contains_key(SHELL_FILE) {
				SHELL_FILE
			} else {
				MANIFEST_FILE
			}
		} else {
			path
		};
		if !safe_relative_path(relative) {
			return plain(404, "not found");
		}
		let (body, etag) = if relative == MANIFEST_FILE {
			(&self.manifest_bytes, sha256_hex(&self.manifest_bytes))
		} else {
			match (self.files.get(relative), self.manifest.files.get(relative)) {
				(Some(body), Some(entry)) => (body, entry.sha256.clone()),
				_ => return plain(404, "not found"),
			}
		};
		let body = if method == "HEAD" {
			Vec::new()
		} else {
			body.clone()
		};
		ServedResponse {
			status: 200,
			headers: vec![
				("content-type", content_type(relative).to_owned()),
				("etag", format!("\"{etag}\"")),
				("cache-control", "no-transform".to_owned()),
			],
			body,
		}
	}
}

fn plain(status: u16, text: &str) -> ServedResponse {
	ServedResponse {
		status,
		headers: vec![("content-type", "text/plain; charset=utf-8".to_owned())],
		body: text.as_bytes().to_vec(),
	}
}

fn content_type(relative: &str) -> &'static str {
	match relative.rsplit('.').next() {
		Some("html") => "text/html; charset=utf-8",
		Some("svg") => "image/svg+xml",
		Some("json") => "application/json",
		Some("arrow") => "application/vnd.apache.arrow.stream",
		Some("ttf") => "font/ttf",
		Some("txt") => "text/plain; charset=utf-8",
		_ => "application/octet-stream",
	}
}

/// `polariton serve <package> [--bind host:port]`. Loopback only under this
/// record: PRODUCT-0154 §10 decides no exposure beyond localhost.
pub(crate) fn serve(root: PathBuf, bind: std::net::SocketAddr) -> Result<(), String> {
	use http_body_util::Full;
	use hyper::server::conn::http1;
	use hyper::service::service_fn;
	use hyper_util::rt::TokioIo;
	use std::sync::Arc;

	if !bind.ip().is_loopback() {
		return Err(format!(
			"serve binds loopback addresses only under PRODUCT-0154; {bind} is not loopback"
		));
	}
	let package = Arc::new(ServedPackage::open(&root)?);
	let runtime = tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
		.map_err(|error| error.to_string())?;
	runtime.block_on(async move {
		let listener = tokio::net::TcpListener::bind(bind)
			.await
			.map_err(|error| format!("cannot bind {bind}: {error}"))?;
		eprintln!("serving {} at http://{bind}/", root.display());
		loop {
			let (stream, _) = listener
				.accept()
				.await
				.map_err(|error| format!("accept failed: {error}"))?;
			let package = Arc::clone(&package);
			tokio::spawn(async move {
				let service = service_fn(move |request: hyper::Request<hyper::body::Incoming>| {
					let package = Arc::clone(&package);
					async move {
						let served =
							package.respond(request.method().as_str(), request.uri().path());
						let mut response = hyper::Response::builder().status(served.status);
						for (name, value) in served.headers {
							response = response.header(name, value);
						}
						response.body(Full::new(bytes::Bytes::from(served.body)))
					}
				});
				let _ = http1::Builder::new()
					.serve_connection(TokioIo::new(stream), service)
					.await;
			});
		}
	})
}

/// The publish prompt: an authored directory and the last refusal.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct PublishPrompt {
	pub(crate) path: String,
	pub(crate) error: Option<String>,
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::export::PngBackend;
	use crate::plot::request::{PlotRequest, PlotSource, plan};
	use crate::plot::spec::AuthoredPlotSpec;
	use crate::plot::theme::PlotSettings;
	use crate::result_artifact::{ArtifactToken, ResultArtifact};
	use crate::selection::{InputIdentity, SelectionRevision};
	use std::sync::Arc;

	fn artifact_with(producer: &str) -> Arc<ResultArtifact> {
		let artifact = ResultArtifact::from_polars(Arc::new(
			polars::df!("x" => [1_i64, 2, 3], "y" => [2_i64, 3, 5]).expect("frame"),
		));
		artifact.bind_input_identity(InputIdentity::mint());
		artifact.bind_producer(producer);
		artifact
	}

	fn artifact() -> Arc<ResultArtifact> {
		artifact_with("sqlite")
	}

	fn try_fixture(
		title: &str,
		artifact: &Arc<ResultArtifact>,
		plot_type: PlotType,
		ordinal: u32,
	) -> Option<HostPlotExport> {
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: title.to_owned(),
				data: Arc::clone(artifact),
			},
			authored_spec: AuthoredPlotSpec::xy(PlotComposition::single(plot_type), "x", "y"),
		})
		.ok()?;
		let recipe = crate::plot::recipe::PlotRecipe::single(
			Arc::clone(artifact),
			resolved.resolved_spec.clone(),
			320,
			220,
		);
		let kernel = crate::plot::factory::create_plot_from_artifact(
			&resolved.resolved_spec,
			&resolved.data,
			artifact,
			320,
			220,
		);
		Some(HostPlotExport {
			title: title.to_owned(),
			kernel,
			settings_model: PlotSettings::default(),
			render_revision: 0,
			resize_render_suspended: false,
			width: 320.0,
			height: 220.0,
			recipe: Some(recipe),
			predicate: Predicate::default(),
			selection_revision: SelectionRevision::ZERO,
			displayed_selection_revision: SelectionRevision::ZERO,
			layout: GridRect {
				col: ordinal * 2,
				row: 0,
				width: 2,
				height: 2,
			},
			view_name: Some(title.to_owned()),
			plot_type: Some(plot_type),
			publication: Some(artifact.publication()),
			authored_expression: Some(format!("scatter(\"x\", \"y\") // {title}")),
			document: Some(7),
		})
	}

	fn fixture(
		title: &str,
		artifact: &Arc<ResultArtifact>,
		plot_type: PlotType,
		ordinal: u32,
	) -> HostPlotExport {
		try_fixture(title, artifact, plot_type, ordinal).expect("fixture plans")
	}

	fn request_at(published_at: &str) -> PublishRequest {
		PublishRequest {
			published_at: published_at.to_owned(),
			secrets: Vec::new(),
			forbidden_paths: Vec::new(),
		}
	}

	fn request() -> PublishRequest {
		request_at("2026-09-03T00:00:00Z")
	}

	fn witness_of(exports: &[HostPlotExport]) -> Vec<Option<Publication>> {
		exports.iter().map(|export| export.publication).collect()
	}

	fn publish(exports: Vec<HostPlotExport>, request: &PublishRequest) -> Package {
		let witness = witness_of(&exports);
		assemble(&witness, exports, request).expect("package")
	}

	fn figure_svg(package: &Package, id: &str) -> String {
		String::from_utf8(package.files[&format!("figures/{id}/figure.svg")].clone())
			.expect("utf-8 svg")
	}

	fn temp_root(label: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"polariton-product-0154-{label}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.map_or(0, |elapsed| elapsed.as_nanos())
		))
	}

	#[test]
	fn product_0154_identity_is_content_and_moves_only_with_ink() {
		let artifact = artifact();
		let first = publish(
			vec![fixture("Average score", &artifact, PlotType::Scatter, 0)],
			&request_at("2026-09-03T00:00:00Z"),
		);
		let later = publish(
			vec![fixture("Average score", &artifact, PlotType::Scatter, 0)],
			&request_at("2026-09-04T12:34:56Z"),
		);
		assert_ne!(
			first.files[MANIFEST_FILE], later.files[MANIFEST_FILE],
			"the moment is recorded"
		);
		assert_eq!(first.digest(), later.digest(), "the identity is content");
		assert_eq!(
			first.manifest.identity_projection(),
			later.manifest.identity_projection()
		);
		assert_eq!(first.manifest.figures[0].identity, "named");
		assert_eq!(first.manifest.figures[0].id, "average-score");
		assert_eq!(first.manifest.figures[0].document, Some(7));

		let relabeled = publish(
			vec![fixture("Average score!", &artifact, PlotType::Scatter, 0)],
			&request(),
		);
		assert_ne!(relabeled.digest(), first.digest());
		assert_eq!(
			relabeled.manifest.datasets[0].sha256, first.manifest.datasets[0].sha256,
			"a label changes ink, never data"
		);
	}

	#[test]
	fn product_0154_duplicate_view_names_reserve_every_id() {
		let artifact = artifact();
		let package = publish(
			vec![
				fixture("same", &artifact, PlotType::Scatter, 0),
				fixture("same", &artifact, PlotType::Line, 1),
				fixture("same-2", &artifact, PlotType::Scatter, 2),
			],
			&request(),
		);
		let ids: Vec<&str> = package
			.manifest
			.figures
			.iter()
			.map(|f| f.id.as_str())
			.collect();
		assert_eq!(ids, ["same", "same-2", "same-2-2"]);
		let unique: std::collections::HashSet<&str> = ids.iter().copied().collect();
		assert_eq!(unique.len(), 3);
		assert_eq!(
			package
				.files
				.keys()
				.filter(|path| path.ends_with("/figure.svg"))
				.count(),
			3,
			"no figure overwrote another"
		);
		let mut anonymous = fixture("Untitled", &artifact, PlotType::Scatter, 3);
		anonymous.view_name = None;
		let package = publish(vec![anonymous], &request());
		assert_eq!(package.manifest.figures[0].id, "figure-1");
		assert_eq!(package.manifest.figures[0].identity, "ordinal");
		assert_eq!(slug("  Ünïcode--Name!! "), "n-code-name");
	}

	#[test]
	fn product_0154_provenance_travels_with_each_artifact_and_shares_content() {
		let sqlite = artifact_with("sqlite");
		let postgres = artifact_with("postgres");
		let unstamped = ResultArtifact::from_polars(Arc::new(
			polars::df!("x" => [9_i64], "y" => [9_i64]).expect("frame"),
		));
		unstamped.bind_input_identity(InputIdentity::mint());
		let package = publish(
			vec![
				fixture("From sqlite", &sqlite, PlotType::Scatter, 0),
				fixture("From postgres", &postgres, PlotType::Line, 1),
				fixture("Unstamped", &unstamped, PlotType::Scatter, 2),
			],
			&request(),
		);
		assert_eq!(
			package.manifest.datasets.len(),
			2,
			"equal content shares one file"
		);
		let shared = package
			.manifest
			.datasets
			.iter()
			.find(|dataset| dataset.rows == 3)
			.expect("shared dataset");
		let producers: Vec<&str> = shared
			.provenances
			.iter()
			.map(|provenance| provenance.producer.as_str())
			.collect();
		assert_eq!(
			producers,
			["sqlite", "postgres"],
			"both assertions survive dedup"
		);
		assert!(
			shared
				.provenances
				.iter()
				.all(|p| p.certificate.is_none() && p.lineage.is_none())
		);
		let lone = package
			.manifest
			.datasets
			.iter()
			.find(|dataset| dataset.rows == 1)
			.expect("unstamped dataset");
		assert_eq!(lone.provenances[0].producer, UNKNOWN_PRODUCER);
		let derived = ResultArtifact::from_polars_derived(
			Arc::new(polars::df!("x" => [1_i64]).expect("frame")),
			&sqlite,
		);
		assert_eq!(
			derived.producer(),
			Some("sqlite"),
			"derived artifacts inherit"
		);
	}

	#[test]
	fn product_0154_labels_round_trip_through_an_xml_parser() {
		let title = "a < b & \"c\" 'd' ☃";
		let package = publish(
			vec![fixture(title, &artifact(), PlotType::Scatter, 0)],
			&request(),
		);
		let svg = figure_svg(&package, "a-b-c-d");
		let document = roxmltree::Document::parse(&svg).expect("the SVG is well-formed XML");
		let texts: Vec<String> = document
			.descendants()
			.filter(|node| node.has_tag_name("text"))
			.map(|node| node.text().unwrap_or("").to_owned())
			.collect();
		assert!(texts.contains(&title.to_owned()), "parsed texts: {texts:?}");
		assert!(
			document
				.descendants()
				.filter(|node| node.has_tag_name("text"))
				.all(|node| node.attribute("font-family") == Some(FONT_FAMILY))
		);
		assert_eq!(xml_escape("<&>\"'"), "&lt;&amp;&gt;&quot;&apos;");
	}

	#[test]
	fn product_0154_package_font_reproduces_the_png_export_for_every_admitted_family() {
		let numeric = artifact();
		let categorical = ResultArtifact::from_polars(Arc::new(
			polars::df!("x" => ["Atlas", "Beacon", "Cinder"], "y" => [2_i64, 3, 5]).expect("frame"),
		));
		categorical.bind_input_identity(InputIdentity::mint());
		let mut covered = Vec::new();
		for (plot_type, artifact) in PlotType::ALL
			.iter()
			.flat_map(|plot_type| [(*plot_type, &numeric), (*plot_type, &categorical)])
		{
			if covered.contains(&plot_type) {
				continue;
			}
			let Some(export) = try_fixture("Cohort spread", artifact, plot_type, 0) else {
				continue;
			};
			let bounds = iced::Rectangle {
				x: 0.0,
				y: 0.0,
				width: export.width,
				height: export.height,
			};
			let widget = PlotWidget::new(
				export.kernel.as_ref(),
				export.title.clone(),
				20.0,
				export.settings_model.clone(),
				export.render_revision,
				export.resize_render_suspended,
				PlotRenderLayer::Full,
			);
			let mut png = PngBackend::new(export.width as u32, export.height as u32);
			widget.render(&mut png, bounds);
			let export_pixels = png.pixmap().data().to_vec();

			let package = publish(vec![export], &request());
			let svg = figure_svg(&package, "cohort-spread");
			let mut package_only = resvg::usvg::fontdb::Database::new();
			package_only.load_font_data(package.files[FONT_FILE].clone());
			let options = resvg::usvg::Options {
				fontdb: Arc::new(package_only),
				..resvg::usvg::Options::default()
			};
			let tree = resvg::usvg::Tree::from_str(&svg, &options).expect("svg parses");
			let mut pixmap =
				tiny_skia::Pixmap::new(tree.size().width() as u32, tree.size().height() as u32)
					.expect("pixmap");
			resvg::render(
				&tree,
				tiny_skia::Transform::identity(),
				&mut pixmap.as_mut(),
			);
			assert_eq!(
				pixmap.data(),
				export_pixels.as_slice(),
				"{plot_type:?}: the package font must reproduce the PNG export"
			);
			assert!(
				export_pixels.iter().any(|byte| *byte != 0),
				"{plot_type:?} has ink"
			);
			covered.push(plot_type);
		}
		for required in [PlotType::Scatter, PlotType::Line, PlotType::Bar] {
			assert!(covered.contains(&required), "covered: {covered:?}");
		}
	}

	#[test]
	fn product_0154_guard_refuses_secrets_paths_and_endpoints_with_controls() {
		let artifact = artifact();
		// A secret whose raw form never appears once escaped.
		let secret = "pa\"ss<word>&more";
		let mut request_with_secret = request();
		request_with_secret.secrets.push(secret.to_owned());
		let exports = vec![fixture(
			&format!("{secret} sales"),
			&artifact,
			PlotType::Scatter,
			0,
		)];
		let witness = witness_of(&exports);
		match assemble(&witness, exports, &request_with_secret) {
			Err(PublishError::Secret { file, field }) => {
				assert_eq!(file, MANIFEST_FILE);
				assert!(
					field == "figure.title" || field == "provenance.source_name",
					"the structural pass names the field: {field}"
				)
			}
			other => panic!("secret must refuse structurally: {other:?}"),
		}
		// The serialized-text pass alone also catches an escaped needle.
		let mut files = BTreeMap::new();
		files.insert(
			"x.json".to_owned(),
			serde_json::to_vec(&secret).expect("json"),
		);
		assert!(matches!(
			guard(&files, &request_with_secret),
			Err(PublishError::Secret { .. })
		));
		files.insert("x.json".to_owned(), xml_escape(secret).into_bytes());
		assert!(matches!(
			guard(&files, &request_with_secret),
			Err(PublishError::Secret { .. })
		));

		for hostile in [
			"/home/tester/sales",
			"~/sales",
			"C:\\Users\\x",
			"postgres://db/x",
		] {
			let exports = vec![fixture(hostile, &artifact, PlotType::Scatter, 0)];
			let witness = witness_of(&exports);
			assert!(
				matches!(
					assemble(&witness, exports, &request()),
					Err(PublishError::Secret { .. })
				),
				"{hostile} must refuse"
			);
		}
		assert!(!looks_like_path_or_endpoint("Average score by cohort"));
		let clean = publish(
			vec![fixture("hunter2 sales", &artifact, PlotType::Scatter, 0)],
			&request(),
		);
		assert!(
			clean.files.contains_key(MANIFEST_FILE),
			"positive control: unguarded value passes"
		);
	}

	#[test]
	fn product_0154_witness_and_recipe_refusals_name_the_figure() {
		let artifact = artifact();
		let exports = vec![fixture("Density", &artifact, PlotType::Scatter, 0)];
		let stale = vec![Some(Publication {
			input: InputIdentity::mint(),
			token: ArtifactToken::mint(),
		})];
		assert_eq!(
			assemble(&stale, exports, &request()).err(),
			Some(PublishError::Republished {
				figure: "Density".to_owned()
			})
		);
		let mut without_recipe = fixture("Density", &artifact, PlotType::Scatter, 0);
		without_recipe.recipe = None;
		let witness = witness_of(std::slice::from_ref(&without_recipe));
		assert_eq!(
			assemble(&witness, vec![without_recipe], &request()).err(),
			Some(PublishError::NoRecipe {
				figure: "Density".to_owned()
			})
		);
		assert_eq!(
			assemble(&[], Vec::new(), &request()).err(),
			Some(PublishError::NoFigures)
		);
	}

	#[test]
	fn product_0154_a_selected_figure_publishes_the_authored_figure_over_its_full_data() {
		use crate::plot::spec::SemanticType;
		use crate::selection::{
			ConstraintForm, ExactScalar, FieldConstraint, FieldRef, ScalarType,
		};
		use std::collections::HashSet;
		let artifact = artifact();
		let unfiltered = publish(
			vec![fixture("Points", &artifact, PlotType::Scatter, 0)],
			&request(),
		);
		let mut selected = fixture("Points", &artifact, PlotType::Scatter, 0);
		selected.predicate = Predicate {
			fields: vec![FieldConstraint {
				field: FieldRef::new("x", SemanticType::Quantitative, ScalarType::Int),
				form: ConstraintForm::Values(HashSet::from([ExactScalar::Int(1)])),
			}],
		};
		selected.selection_revision = SelectionRevision::ZERO.next();
		let filtered = publish(vec![selected], &request());
		assert_eq!(
			filtered.digest(),
			unfiltered.digest(),
			"selection never changes the package"
		);
		let definition: FigureDefinition =
			serde_json::from_slice(&filtered.files["figures/points/definition.json"])
				.expect("definition");
		assert_eq!(definition.selection, "none");
		assert_eq!(
			definition.authored_expression.as_deref(),
			Some("scatter(\"x\", \"y\") // Points")
		);
		assert_eq!(definition.channels[0].semantic_type, "quantitative");
		assert_eq!(definition.channels[0].layer, "layer-0:scatter");
	}

	#[test]
	fn product_0154_shell_is_a_document_on_the_grid_without_script() {
		let artifact = artifact();
		let package = publish(
			vec![
				fixture("Left", &artifact, PlotType::Scatter, 0),
				fixture("Right", &artifact, PlotType::Line, 1),
			],
			&request(),
		);
		let shell = String::from_utf8(package.files[SHELL_FILE].clone()).expect("html");
		assert!(!shell.to_ascii_lowercase().contains("<script"));
		assert!(shell.contains("grid-column: 1 / span 2; grid-row: 1 / span 2;"));
		assert!(shell.contains("grid-column: 3 / span 2; grid-row: 1 / span 2;"));
		assert!(shell.contains(&format!("@font-face {{ font-family: \"{FONT_FAMILY}\"")));
		assert!(shell.contains(&figure_svg(&package, "left")));
		assert!(package.manifest.fonts[0].file == FONT_FILE);
		assert!(package.files.contains_key(FONT_LICENCE_FILE));
	}

	#[test]
	fn product_0154_server_pins_verified_bytes_and_refuses_everything_else() {
		let artifact = artifact();
		let package = publish(
			vec![fixture("Served", &artifact, PlotType::Scatter, 0)],
			&request(),
		);
		let root = temp_root("serve");
		package.write(&root).expect("first write");
		assert!(
			matches!(package.write(&root), Err(PublishError::Write(_))),
			"create never clobbers"
		);
		let served = ServedPackage::open(&root).expect("open");

		let home = served.respond("GET", "/");
		assert_eq!(home.status, 200);
		assert!(
			home.headers
				.iter()
				.any(|(k, v)| *k == "content-type" && v.starts_with("text/html"))
		);
		let manifest = served.respond("GET", "/manifest.json?x=1");
		assert_eq!(manifest.status, 200);
		assert!(manifest.headers.contains(&(
			"etag",
			format!("\"{}\"", sha256_hex(&package.files[MANIFEST_FILE]))
		)));
		assert_eq!(manifest.body, package.files[MANIFEST_FILE]);
		let head = served.respond("HEAD", "/index.html");
		assert_eq!((head.status, head.body.len()), (200, 0));
		for hostile in [
			"/../manifest.json",
			"/figures/served/",
			"/figures\\served\\figure.svg",
			"/./index.html",
			"/nope.svg",
		] {
			assert_eq!(served.respond("GET", hostile).status, 404, "{hostile}");
		}
		assert_eq!(served.respond("POST", "/").status, 405);

		// Mutation after open is never served; mutation before open refuses.
		std::fs::write(root.join(SHELL_FILE), b"<html>tampered</html>").expect("tamper");
		assert_eq!(
			served.respond("GET", "/index.html").body,
			package.files[SHELL_FILE]
		);
		assert!(
			ServedPackage::open(&root).is_err(),
			"a tampered file fails verification"
		);

		// A symlinked listed file refuses at open.
		let linked = temp_root("symlink");
		package.write(&linked).expect("second write");
		let target = linked.join(SHELL_FILE);
		std::fs::remove_file(&target).expect("remove");
		std::os::unix::fs::symlink(root.join(MANIFEST_FILE), &target).expect("symlink");
		assert!(
			ServedPackage::open(&linked).is_err(),
			"symlinks never escape the root"
		);

		assert!(
			serve(root.clone(), "0.0.0.0:7743".parse().expect("addr")).is_err(),
			"non-loopback binds refuse"
		);
		let _ = std::fs::remove_dir_all(&root);
		let _ = std::fs::remove_dir_all(&linked);
	}
}
