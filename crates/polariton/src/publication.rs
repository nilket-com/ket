//! PRODUCT-0154: the published dashboard package.
//!
//! A package is a directory with a versioned, content-addressed manifest. This
//! module owns the package's DTOs and their explicit conversion from runtime
//! types, canonical encodings, the publication guard, the HTML shell, and the
//! deliberately dumb server. Runtime plot, editor, and selection types never
//! implement `Serialize`; placement identities, artifact tokens, and renderer
//! vocabulary cannot enter a package.

use crate::editor::HostPlotExport;
use crate::plot::common::{
	HitDimensionality, HitRegion, PlotHit, PlotRenderLayer, PlotWidget, ResolutionPolicy,
	ResolutionStage, TieRule,
};
use crate::plot::export::xml_escape;
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
pub(crate) const INSPECT_FILE: &str = "inspect.js";
/// PRODUCT-0155 §6: the one executable script, byte-pinned into the shell and
/// into the package. Its source contains no literal `<` or `&`.
pub(crate) const INSPECT_JS: &str = include_str!("../assets/web/inspect.js");
pub(crate) const PROJECTION_SCHEMA_VERSION: u32 = 1;
pub(crate) const PROJECTION_BYTE_BOUND: usize = 1024 * 1024;
pub(crate) const PROJECTION_REGION_BOUND: usize = 32_768;
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
	/// PRODUCT-0155 §7: whether the figure carries a hit projection, and if
	/// not, exactly why and how far over the bound it was.
	pub(crate) inspection: Inspection,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "kebab-case")]
pub(crate) enum Inspection {
	Projected {
		regions: u64,
		bytes: u64,
	},
	Omitted {
		reason: String,
		observed: Option<u64>,
	},
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

// PRODUCT-0155 §1–§3: the hit projection. Narrow hover DTOs and exact
// regions in `viewBox` units, resolved in stages.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Projection {
	pub(crate) schema_version: u32,
	pub(crate) viewbox: [f64; 2],
	pub(crate) stages: Vec<Stage>,
	pub(crate) hits: Vec<Hit>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Stage {
	pub(crate) policy: String,
	pub(crate) tie: String,
	pub(crate) within: Option<RectRegion>,
	/// Subtracted from the pointer, in 32-bit arithmetic, before any region
	/// compare: the desktop's own plot-area translation.
	pub(crate) origin: [f64; 2],
	pub(crate) regions: Vec<Region>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "kebab-case")]
pub(crate) enum Region {
	Rect {
		x0: f64,
		y0: f64,
		x1: f64,
		y1: f64,
		inclusive_max: bool,
		ordinal: u32,
		hit: usize,
	},
	Circle {
		cx: f64,
		cy: f64,
		r: f64,
		ordinal: u32,
		hit: usize,
	},
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RectRegion {
	pub(crate) x0: f64,
	pub(crate) y0: f64,
	pub(crate) x1: f64,
	pub(crate) y1: f64,
	pub(crate) inclusive_max: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Hit {
	pub(crate) mark_family: String,
	pub(crate) dimensionality: String,
	pub(crate) fields: Vec<HitField>,
	pub(crate) facet: Option<HitFacet>,
	pub(crate) presentation: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HitField {
	pub(crate) label: String,
	pub(crate) semantic_type: String,
	pub(crate) value: HitValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct HitFacet {
	pub(crate) field: String,
	pub(crate) value: String,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "kebab-case")]
pub(crate) enum HitValue {
	Nominal {
		value: String,
	},
	Ordinal {
		value: String,
	},
	Quantitative {
		value: f64,
		display: String,
	},
	Temporal {
		value: f64,
		display: String,
	},
	/// A decimal string: a count may exceed what a browser number holds.
	Count {
		value: String,
	},
	Percentage {
		value: f64,
		display: String,
	},
	Interval {
		start: Box<HitValue>,
		end: Box<HitValue>,
		end_inclusive: bool,
	},
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

	/// PRODUCT-0155 §1: every string a hit carries — prose, labels, nominal and
	/// ordinal values, and the facet field and value — is a data-derived
	/// string and is checked structurally before it is serialized.
	fn check_hit(&self, file: &str, hit: &Hit) -> Result<(), PublishError> {
		self.check(file, "presentation", &hit.presentation)?;
		for field in &hit.fields {
			self.check(file, "field.label", &field.label)?;
			check_value(self, file, &field.value)?;
		}
		if let Some(facet) = &hit.facet {
			self.check(file, "facet.field", &facet.field)?;
			self.check(file, "facet.value", &facet.value)?;
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
	let mut embedded: BTreeMap<String, String> = BTreeMap::new();
	// PRODUCT-0154 §5: layout is relative placement. The desktop workspace's
	// absolute cells — the editor columns a dashboard sat beside — are not
	// content, so every rectangle is rebased to the published bounding box.
	let origin_col = exports
		.iter()
		.map(|export| export.layout.col)
		.min()
		.unwrap_or(0);
	let origin_row = exports
		.iter()
		.map(|export| export.layout.row)
		.min()
		.unwrap_or(0);

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
		for label in svg_text_contents(&svg.svg) {
			guarded.check(&svg_file, "text", &unescape_xml(&label))?;
		}
		// PRODUCT-0155: the hit projection, or the reason there is none.
		let outcome = match project_export(&export) {
			Some(stages) => project(stages, export.width, export.height),
			None => ProjectionOutcome::Omitted {
				reason: "no-regions".to_owned(),
				observed: None,
			},
		};
		let hits_file = format!("{figure_dir}/hits.json");
		let inspection = match outcome {
			ProjectionOutcome::Projected(projection) => {
				for hit in &projection.hits {
					guarded.check_hit(&hits_file, hit)?;
				}
				let bytes = canonical_json(&projection).map_err(PublishError::Encoding)?;
				let regions = projection
					.stages
					.iter()
					.map(|stage| stage.regions.len() as u64)
					.sum();
				let inspection = Inspection::Projected {
					regions,
					bytes: bytes.len() as u64,
				};
				embedded.insert(id.clone(), embed_json(&bytes));
				files.insert(hits_file, bytes);
				inspection
			}
			ProjectionOutcome::Omitted { reason, observed } => {
				Inspection::Omitted { reason, observed }
			}
		};
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
			layout: GridRect {
				col: export.layout.col - origin_col,
				row: export.layout.row - origin_row,
				width: export.layout.width.max(1),
				height: export.layout.height.max(1),
			},
			inspection,
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
		files.insert(svg_file, svg.svg.into_bytes());
	}

	files.insert(
		FONT_FILE.to_owned(),
		crate::fonts::POLARITON_PLOT_SANS_REGULAR.to_vec(),
	);
	files.insert(
		FONT_LICENCE_FILE.to_owned(),
		FONT_LICENCE.as_bytes().to_vec(),
	);
	files.insert(INSPECT_FILE.to_owned(), INSPECT_JS.as_bytes().to_vec());
	files.insert(
		SHELL_FILE.to_owned(),
		shell_html(&figures, &files, &embedded).into_bytes(),
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

fn render_figure_svg(export: &HostPlotExport) -> crate::plot::export::SvgFigure {
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
	crate::plot::export::render_svg(&widget, bounds)
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
// PRODUCT-0155: the hit projection, built from the widget's stages.
// ---------------------------------------------------------------------------

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ProjectionOutcome {
	Projected(Projection),
	Omitted {
		reason: String,
		observed: Option<u64>,
	},
}

fn finite(value: f32) -> Result<f64, ()> {
	if value.is_finite() {
		Ok(f64::from(value))
	} else {
		Err(())
	}
}

pub(crate) fn hit_dto(hit: &PlotHit) -> Hit {
	Hit {
		mark_family: hit.mark_family.as_str().to_owned(),
		dimensionality: match hit.dimensionality {
			HitDimensionality::Point => "point",
			HitDimensionality::Line => "line",
			HitDimensionality::Area => "area",
		}
		.to_owned(),
		fields: hit
			.fields
			.iter()
			.map(|field| HitField {
				label: field.label.clone(),
				semantic_type: semantic_type_name(field.semantic_type).to_owned(),
				value: hit_value_dto(&field.value),
			})
			.collect(),
		facet: hit.facet.as_ref().map(|facet| HitFacet {
			field: facet.field.clone(),
			value: facet.value.clone(),
		}),
		presentation: hit.to_string(),
	}
}

fn hit_value_dto(value: &crate::plot::common::HitValue) -> HitValue {
	use crate::plot::common::HitValue as Desktop;
	match value {
		Desktop::Nominal(value) => HitValue::Nominal {
			value: value.clone(),
		},
		Desktop::Ordinal(value) => HitValue::Ordinal {
			value: value.clone(),
		},
		Desktop::Quantitative { value, display } => HitValue::Quantitative {
			value: *value,
			display: display.clone(),
		},
		Desktop::Temporal { value, display } => HitValue::Temporal {
			value: *value,
			display: display.clone(),
		},
		Desktop::Count(count) => HitValue::Count {
			value: count.to_string(),
		},
		Desktop::Percentage { value, display } => HitValue::Percentage {
			value: *value,
			display: display.clone(),
		},
		Desktop::Interval {
			start,
			end,
			end_inclusive,
		} => HitValue::Interval {
			start: Box::new(hit_value_dto(start)),
			end: Box::new(hit_value_dto(end)),
			end_inclusive: *end_inclusive,
		},
	}
}

fn hit_value_is_finite(value: &HitValue) -> bool {
	match value {
		HitValue::Quantitative { value, .. }
		| HitValue::Temporal { value, .. }
		| HitValue::Percentage { value, .. } => value.is_finite(),
		HitValue::Interval { start, end, .. } => {
			hit_value_is_finite(start) && hit_value_is_finite(end)
		}
		_ => true,
	}
}

/// The stages the widget answered, projected: hits deduplicated in
/// stage-then-region order of first appearance, geometry exact and finite,
/// or the reason the figure publishes without a projection.
pub(crate) fn project(stages: Vec<ResolutionStage>, width: f32, height: f32) -> ProjectionOutcome {
	let omitted = |reason: &str, observed: Option<u64>| ProjectionOutcome::Omitted {
		reason: reason.to_owned(),
		observed,
	};
	let region_count: usize = stages.iter().map(|stage| stage.regions.len()).sum();
	if region_count > PROJECTION_REGION_BOUND {
		return omitted("region-limit", Some(region_count as u64));
	}
	let mut hits: Vec<Hit> = Vec::new();
	let mut hit_index: BTreeMap<Vec<u8>, usize> = BTreeMap::new();
	let mut out_stages = Vec::with_capacity(stages.len());
	for stage in stages {
		let within = match stage.within {
			Some(HitRegion::Rectangle {
				x0,
				y0,
				x1,
				y1,
				inclusive_max,
			}) => Some(match rect_region(x0, y0, x1, y1, inclusive_max) {
				Ok(rect) => rect,
				Err(()) => return omitted("non-finite-geometry", None),
			}),
			Some(HitRegion::Circle { .. }) => return omitted("gate-shape", None),
			None => None,
		};
		let mut regions = Vec::with_capacity(stage.regions.len());
		for entry in stage.regions {
			let dto = hit_dto(&entry.hit);
			if !dto
				.fields
				.iter()
				.all(|field| hit_value_is_finite(&field.value))
			{
				return omitted("non-finite-value", None);
			}
			let key = match canonical_json(&dto) {
				Ok(key) => key,
				Err(_) => return omitted("encoding", None),
			};
			let index = match hit_index.get(&key) {
				Some(index) => *index,
				None => {
					hits.push(dto);
					hit_index.insert(key, hits.len() - 1);
					hits.len() - 1
				}
			};
			let region = match entry.region {
				HitRegion::Rectangle {
					x0,
					y0,
					x1,
					y1,
					inclusive_max,
				} => match rect_region(x0, y0, x1, y1, inclusive_max) {
					Ok(rect) => Region::Rect {
						x0: rect.x0,
						y0: rect.y0,
						x1: rect.x1,
						y1: rect.y1,
						inclusive_max,
						ordinal: entry.ordinal,
						hit: index,
					},
					Err(()) => return omitted("non-finite-geometry", None),
				},
				HitRegion::Circle { center, radius } => {
					match (finite(center.x), finite(center.y), finite(radius)) {
						(Ok(cx), Ok(cy), Ok(r)) => Region::Circle {
							cx,
							cy,
							r,
							ordinal: entry.ordinal,
							hit: index,
						},
						_ => return omitted("non-finite-geometry", None),
					}
				}
			};
			regions.push(region);
		}
		let origin = match (finite(stage.origin.x), finite(stage.origin.y)) {
			(Ok(x), Ok(y)) => [x, y],
			_ => return omitted("non-finite-geometry", None),
		};
		out_stages.push(Stage {
			policy: match stage.policy {
				ResolutionPolicy::FirstInOrder => "first-in-order",
				ResolutionPolicy::NearestCentreWithin => "nearest-centre-within",
			}
			.to_owned(),
			tie: match stage.tie {
				TieRule::LaterWins => "later-wins",
				TieRule::LowerOrdinalWins => "lower-ordinal-wins",
			}
			.to_owned(),
			within,
			origin,
			regions,
		});
	}
	let projection = Projection {
		schema_version: PROJECTION_SCHEMA_VERSION,
		viewbox: [f64::from(width), f64::from(height)],
		stages: out_stages,
		hits,
	};
	match canonical_json(&projection) {
		Ok(bytes) if bytes.len() > PROJECTION_BYTE_BOUND => {
			omitted("byte-limit", Some(bytes.len() as u64))
		}
		Ok(_) => ProjectionOutcome::Projected(projection),
		Err(_) => omitted("encoding", None),
	}
}

fn rect_region(x0: f32, y0: f32, x1: f32, y1: f32, inclusive_max: bool) -> Result<RectRegion, ()> {
	Ok(RectRegion {
		x0: finite(x0)?,
		y0: finite(y0)?,
		x1: finite(x1)?,
		y1: finite(y1)?,
		inclusive_max,
	})
}

/// The widget's projection for an export, in the SVG's `viewBox` space.
pub(crate) fn project_export(export: &HostPlotExport) -> Option<Vec<ResolutionStage>> {
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
	widget.hit_projection(bounds)
}

/// PRODUCT-0155 §6: the embedded form of the canonical bytes — every `<` as
/// `<` and every `&` as `&` — total and reversible, so no label can
/// close the data element or start an entity.
pub(crate) fn embed_json(canonical: &[u8]) -> String {
	let text = String::from_utf8_lossy(canonical);
	text.replace('<', "\\u003c").replace('&', "\\u0026")
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

/// PRODUCT-0155 §6: the shell is valid HTML and well-formed XML at once. Void
/// elements are closed XML-style; the one executable script is the pinned
/// asset, named by hash in the content security policy; each figure's
/// projection sits beside its SVG in a non-executable data element.
pub(crate) fn script_hash_b64() -> String {
	use base64::Engine as _;
	base64::engine::general_purpose::STANDARD.encode(Sha256::digest(INSPECT_JS.as_bytes()))
}

fn shell_html(
	figures: &[FigureEntry],
	files: &BTreeMap<String, Vec<u8>>,
	embedded: &BTreeMap<String, String>,
) -> String {
	let columns = figures
		.iter()
		.map(|figure| figure.layout.col + figure.layout.width)
		.max()
		.unwrap_or(1)
		.max(1);
	let mut html = String::new();
	html.push_str(
		"<!DOCTYPE html>\n<html xmlns=\"http://www.w3.org/1999/xhtml\" lang=\"en\">\n<head>\n",
	);
	html.push_str("<meta charset=\"utf-8\"/>\n");
	html.push_str("<meta name=\"viewport\" content=\"width=device-width, initial-scale=1\"/>\n");
	html.push_str(&format!(
		"<meta http-equiv=\"Content-Security-Policy\" content=\"default-src 'none'; style-src 'unsafe-inline'; font-src 'self'; img-src data:; script-src 'sha256-{}'\"/>\n",
		script_hash_b64()
	));
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
	html.push_str(".figure { min-width: 0; margin: 0; }\n.figure svg { width: 100%; height: auto; display: block; }\n");
	html.push_str(".polariton-tooltip { position: fixed; pointer-events: none; background: #f2f2f2; color: #1a1a1a; padding: 4px 8px; border-radius: 3px; font-size: 13px; white-space: pre; z-index: 10; }\n");
	html.push_str(".polariton-tooltip[hidden] { display: none; }\n");
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
		html.push('\n');
		if let Some(projection) = embedded.get(&figure.id) {
			html.push_str("<script type=\"application/json\" class=\"polariton-hits\">");
			html.push_str(projection);
			html.push_str("</script>\n");
		}
		html.push_str("</figure>\n");
	}
	html.push_str("</main>\n<script>");
	html.push_str(INSPECT_JS);
	html.push_str("</script>\n</body>\n</html>\n");
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
mod tests_support {
	pub(super) use super::tests::{artifact, fixture, publish, request};
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

	pub(super) fn artifact() -> Arc<ResultArtifact> {
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

	pub(super) fn fixture(
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

	pub(super) fn request() -> PublishRequest {
		request_at("2026-09-03T00:00:00Z")
	}

	fn witness_of(exports: &[HostPlotExport]) -> Vec<Option<Publication>> {
		exports.iter().map(|export| export.publication).collect()
	}

	pub(super) fn publish(exports: Vec<HostPlotExport>, request: &PublishRequest) -> Package {
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
	fn product_0154_shell_is_a_document_on_the_grid_with_one_pinned_script() {
		let artifact = artifact();
		let package = publish(
			vec![
				fixture("Left", &artifact, PlotType::Scatter, 0),
				fixture("Right", &artifact, PlotType::Line, 1),
			],
			&request(),
		);
		let shell = String::from_utf8(package.files[SHELL_FILE].clone()).expect("html");
		// PRODUCT-0155 §6: exactly one executable script, byte-identical to the
		// pinned asset; every other script element is a JSON data element.
		let executable: Vec<&str> = shell.split("<script>").skip(1).collect();
		assert_eq!(executable.len(), 1);
		assert_eq!(executable[0].split("</script>").next(), Some(INSPECT_JS));
		assert!(shell.contains(&format!("script-src 'sha256-{}'", script_hash_b64())));
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

#[cfg(test)]
mod layout_tests {
	use super::tests_support::*;
	use super::*;
	use crate::plot::core::PlotType;

	/// The dogfood finding: a dashboard beside a 98-column editor published its
	/// figures at columns 99–197 and left half the browser blank. Layout is
	/// relative placement; the workspace's absolute cells are not content.
	#[test]
	fn product_0154_layout_is_rebased_to_the_published_bounding_box() {
		let artifact = artifact();
		let mut left = fixture("Left", &artifact, PlotType::Scatter, 0);
		left.layout = GridRect {
			col: 99,
			row: 3,
			width: 49,
			height: 20,
		};
		let mut right = fixture("Right", &artifact, PlotType::Line, 1);
		right.layout = GridRect {
			col: 148,
			row: 3,
			width: 49,
			height: 20,
		};
		let mut below = fixture("Below", &artifact, PlotType::Scatter, 2);
		below.layout = GridRect {
			col: 99,
			row: 23,
			width: 98,
			height: 20,
		};
		let package = publish(vec![left, right, below], &request());
		let layouts: Vec<GridRect> = package.manifest.figures.iter().map(|f| f.layout).collect();
		assert_eq!(
			layouts,
			[
				GridRect {
					col: 0,
					row: 0,
					width: 49,
					height: 20
				},
				GridRect {
					col: 49,
					row: 0,
					width: 49,
					height: 20
				},
				GridRect {
					col: 0,
					row: 20,
					width: 98,
					height: 20
				},
			]
		);
		let shell = String::from_utf8(package.files[SHELL_FILE].clone()).expect("html");
		assert!(
			shell.contains("grid-template-columns: repeat(98, 1fr)"),
			"{shell}"
		);
		assert!(shell.contains("grid-column: 1 / span 49; grid-row: 1 / span 20;"));
		assert!(shell.contains("grid-column: 50 / span 49; grid-row: 1 / span 20;"));
		assert!(shell.contains("grid-column: 1 / span 98; grid-row: 21 / span 20;"));
		let unshifted = publish(
			vec![
				fixture("Left", &artifact, PlotType::Scatter, 0),
				fixture("Right", &artifact, PlotType::Line, 1),
			],
			&request(),
		);
		assert_eq!(
			unshifted.manifest.figures[0].layout.col, 0,
			"already-rebased input is unchanged"
		);
	}
}

#[cfg(test)]
mod product_0155_tests {
	use super::tests_support::*;
	use super::*;
	use crate::plot::common::PlotKernel;
	use crate::plot::core::PlotType;
	use crate::plot::kernels::boxplot::{BoxPlotKernel, BoxPlotPreparedData};
	use crate::plot::kernels::boxplot_tukey::{TukeyBoxRow, TukeyBoxes};
	use crate::plot::kernels::bubble::{BubblePlotKernel, prepare_bubble_data};
	use crate::plot::kernels::heatmap::{HeatmapPlotKernel, prepare_heatmap_data};
	use crate::plot::spec::{BubbleFieldNames, BubbleFields};
	use crate::plot::theme::PlotSettings;
	use boa_engine::{Context, Source};
	use iced::advanced::mouse::Cursor;
	use iced::{Point, Rectangle};
	use std::sync::Arc;

	const WIDTH: f32 = 420.0;
	const HEIGHT: f32 = 300.0;

	fn bounds() -> Rectangle {
		Rectangle {
			x: 0.0,
			y: 0.0,
			width: WIDTH,
			height: HEIGHT,
		}
	}

	fn widget<'a>(kernel: &'a (dyn PlotKernel + Send + Sync), title: &str) -> PlotWidget<'a> {
		PlotWidget::new(
			kernel,
			title.to_owned(),
			20.0,
			PlotSettings::default(),
			0,
			false,
			PlotRenderLayer::Full,
		)
	}

	fn heatmap_kernel(suffix: &str) -> HeatmapPlotKernel {
		let frame = polars::df!(
			"x" => ["Adopt", "Adopt", "Discover", "Discover", "Expand", "Expand"],
			"y" => [format!("Clarity{suffix}"), "Reach".to_owned(), format!("Clarity{suffix}"), "Reach".to_owned(), format!("Clarity{suffix}"), "Reach".to_owned()],
			"v" => [1.0_f64, 4.0, 2.0, 8.0, 3.0, 5.0],
		)
		.expect("frame");
		HeatmapPlotKernel {
			prepared_data: Arc::new(prepare_heatmap_data(&frame, "x", "y", "v").expect("heatmap")),
		}
	}

	fn bubble_kernel() -> BubblePlotKernel {
		// Two observations share a centre so containment overlaps; the desktop
		// answers the first in prepared order.
		let frame = polars::df!(
			"x" => [1.0_f64, 5.0, 5.0, 9.0],
			"y" => [1.0_f64, 5.0, 5.0, 9.0],
			"size" => [1.0_f64, 3.0, 1.0, 2.0],
			"color" => [10.0_f64, 20.0, 30.0, 40.0],
			"label" => ["A", "B", "C", "D"],
		)
		.expect("frame");
		let fields = BubbleFields::new(
			BubbleFieldNames::new("x", "y", "size", "color"),
			Some("label".to_owned()),
		);
		BubblePlotKernel {
			prepared_data: Arc::new(prepare_bubble_data(&frame, &fields).expect("bubble")),
		}
	}

	fn box_kernel() -> BoxPlotKernel {
		let row = |category: &str, outlier: Option<f64>| TukeyBoxRow {
			category: category.to_owned(),
			whisker_low: 10.0,
			q1: 20.0,
			median: 30.0,
			q3: 40.0,
			whisker_high: 50.0,
			outlier,
		};
		let boxes = TukeyBoxes::new(vec![
			row("Atlas", Some(70.0)),
			row("Atlas", Some(70.0)),
			row("Atlas", Some(80.0)),
			row("Beacon", None),
			row("Cinder", Some(5.0)),
		])
		.expect("boxes");
		BoxPlotKernel {
			prepared_data: Arc::new(BoxPlotPreparedData::new(
				boxes,
				crate::plot::common::AxisType::Linear,
				"Cohort".to_owned(),
				"Score".to_owned(),
			)),
		}
	}

	/// The shipped asset, executed: no DOM, so the installer never runs, and
	/// the resolver is called with the projection the package carries.
	fn asset_resolve(projection: &str, x: f64, y: f64) -> (i64, u64) {
		let mut context = Context::default();
		context
			.eval(Source::from_bytes(INSPECT_JS))
			.expect("the asset loads without a document");
		let script = format!(
			"var __r = polaritonResolve(JSON.parse({}), {x:?}, {y:?}); [__r.index, __r.ops]",
			serde_json::to_string(projection).expect("json string")
		);
		let value = context
			.eval(Source::from_bytes(&script))
			.expect("the resolver runs");
		let array = value.as_object().expect("array");
		let index = array
			.get(0, &mut context)
			.expect("index")
			.to_number(&mut context)
			.expect("number");
		let ops = array
			.get(1, &mut context)
			.expect("ops")
			.to_number(&mut context)
			.expect("number");
		(index as i64, ops as u64)
	}

	fn desktop(widget: &PlotWidget<'_>, x: f64, y: f64) -> Option<Hit> {
		widget
			.structured_hit_at(bounds(), Cursor::Available(Point::new(x as f32, y as f32)))
			.map(|hit| hit_dto(&hit))
	}

	fn projection_for(widget: &PlotWidget<'_>) -> Projection {
		match project(
			widget.hit_projection(bounds()).expect("regions"),
			WIDTH,
			HEIGHT,
		) {
			ProjectionOutcome::Projected(projection) => projection,
			other => panic!("expected a projection: {other:?}"),
		}
	}

	/// Uniform coverage plus every region's boundary, just inside and just
	/// outside, plus the midpoints between circle centres — equal distances
	/// by construction whenever 32-bit arithmetic lands on them.
	fn vectors(projection: &Projection) -> Vec<(f64, f64)> {
		let mut vectors = Vec::new();
		for row in 0..32 {
			for col in 0..32 {
				vectors.push((
					f64::from(WIDTH) * (f64::from(col) + 0.5) / 32.0,
					f64::from(HEIGHT) * (f64::from(row) + 0.5) / 32.0,
				));
			}
		}
		let mut centres = Vec::new();
		for stage in &projection.stages {
			let (ox, oy) = (stage.origin[0], stage.origin[1]);
			for region in &stage.regions {
				match region {
					Region::Rect { x0, y0, x1, y1, .. } => {
						let (x0, y0, x1, y1) = (x0 + ox, y0 + oy, x1 + ox, y1 + oy);
						for (px, py) in [(x1, y1), (x0, y0), ((x0 + x1) / 2.0, y1)] {
							vectors.push((px - 0.01, py - 0.01));
							vectors.push((px + 0.01, py + 0.01));
							vectors.push((px, py));
						}
					}
					Region::Circle { cx, cy, r, .. } => {
						let (cx, cy) = (&(cx + ox), &(cy + oy));
						centres.push((*cx, *cy));
						for delta in [-0.02, -0.0005, 0.0, 0.0005, 0.02] {
							vectors.push((cx + r + delta, *cy));
							vectors.push((*cx, cy - r + delta));
						}
						vectors.push((*cx, *cy));
					}
				}
			}
		}
		for (i, a) in centres.iter().enumerate() {
			for b in centres.iter().skip(i + 1) {
				vectors.push(((a.0 + b.0) / 2.0, (a.1 + b.1) / 2.0));
			}
		}
		vectors.truncate(1000.max(vectors.len().min(4000)));
		vectors
	}

	fn assert_parity(kernel: &(dyn PlotKernel + Send + Sync), family: &str) {
		let widget = widget(kernel, family);
		let projection = projection_for(&widget);
		let regions: usize = projection
			.stages
			.iter()
			.map(|stage| stage.regions.len())
			.sum();
		assert!(regions <= PROJECTION_REGION_BOUND);
		let json = String::from_utf8(canonical_json(&projection).expect("json")).expect("utf-8");
		let mut disagreements = Vec::new();
		let mut answered = 0;
		for (x, y) in vectors(&projection) {
			let (index, ops) = asset_resolve(&json, x, y);
			assert!(ops <= 65_536, "{family}: {ops} operations at ({x}, {y})");
			let asset = if index < 0 {
				None
			} else {
				Some(projection.hits[index as usize].clone())
			};
			let desktop = desktop(&widget, x, y);
			if asset.is_some() {
				answered += 1;
			}
			if asset != desktop {
				disagreements.push((x, y, asset, desktop));
			}
		}
		assert!(
			answered > 0,
			"{family}: positive control — some vector must hit"
		);
		assert!(
			disagreements.is_empty(),
			"{family}: {} disagreements, first: {:?}",
			disagreements.len(),
			disagreements.first()
		);
	}

	#[test]
	fn product_0155_heatmap_parity_through_the_shipped_asset() {
		assert_parity(&heatmap_kernel(""), "heatmap");
	}

	#[test]
	fn product_0155_bubble_parity_through_the_shipped_asset() {
		assert_parity(&bubble_kernel(), "bubble");
	}

	#[test]
	fn product_0155_box_parity_through_the_shipped_asset() {
		let kernel = box_kernel();
		let widget = widget(&kernel, "box");
		let projection = projection_for(&widget);
		assert_eq!(projection.stages.len(), 2, "outliers then boxes");
		assert_eq!(projection.stages[0].policy, "nearest-centre-within");
		assert_eq!(projection.stages[0].tie, "lower-ordinal-wins");
		assert_eq!(projection.stages[1].policy, "first-in-order");
		assert_parity(&kernel, "box");
	}

	#[test]
	fn product_0155_scatter_parity_through_the_shipped_asset() {
		let artifact = ResultArtifactFixture::scatter_with_equidistant_pair();
		let export = fixture("Points", &artifact, PlotType::Scatter, 0);
		let widget = widget(export.kernel.as_ref(), "Points");
		let projection = projection_for(&widget);
		assert_eq!(projection.stages[0].policy, "nearest-centre-within");
		assert_eq!(projection.stages[0].tie, "later-wins");
		assert_parity(export.kernel.as_ref(), "scatter");
	}

	struct ResultArtifactFixture;
	impl ResultArtifactFixture {
		fn scatter_with_equidistant_pair() -> Arc<crate::result_artifact::ResultArtifact> {
			let artifact = crate::result_artifact::ResultArtifact::from_polars(Arc::new(
				polars::df!("x" => [1_i64, 3, 3, 5, 7], "y" => [2_i64, 2, 2, 6, 1]).expect("frame"),
			));
			artifact.bind_input_identity(crate::selection::InputIdentity::mint());
			artifact
		}
	}

	#[test]
	fn product_0155_projection_is_deterministic_and_prose_moves_without_regions() {
		let first = projection_for(&widget(&heatmap_kernel(""), "h"));
		let again = projection_for(&widget(&heatmap_kernel(""), "h"));
		assert_eq!(
			canonical_json(&first).unwrap(),
			canonical_json(&again).unwrap()
		);
		let relabeled = projection_for(&widget(&heatmap_kernel("!"), "h"));
		assert_eq!(
			first.stages, relabeled.stages,
			"regions never move with a label"
		);
		assert_ne!(first.hits, relabeled.hits, "prose moves with the label");
	}

	#[test]
	fn product_0155_bounds_omit_the_projection_and_publish_the_rest() {
		let many = |n: i64| {
			let artifact = crate::result_artifact::ResultArtifact::from_polars(Arc::new(
				polars::df!(
					"x" => (0..n).map(|i| (i % 200) as f64).collect::<Vec<_>>(),
					"y" => (0..n).map(|i| (i / 200) as f64).collect::<Vec<_>>()
				)
				.expect("frame"),
			));
			artifact.bind_input_identity(crate::selection::InputIdentity::mint());
			artifact
		};
		let over_regions = publish(
			vec![fixture("Dense", &many(40_000), PlotType::Scatter, 0)],
			&request(),
		);
		assert_eq!(
			over_regions.manifest.figures[0].inspection,
			Inspection::Omitted {
				reason: "region-limit".to_owned(),
				observed: Some(40_000)
			}
		);
		assert!(!over_regions.files.contains_key("figures/dense/hits.json"));
		let over_bytes = publish(
			vec![fixture("Dense", &many(20_000), PlotType::Scatter, 0)],
			&request(),
		);
		match &over_bytes.manifest.figures[0].inspection {
			Inspection::Omitted { reason, observed } => {
				assert_eq!(reason, "byte-limit");
				assert!(observed.unwrap() > PROJECTION_BYTE_BOUND as u64);
			}
			other => panic!("expected the byte bound: {other:?}"),
		}
		let deferred = publish(
			vec![fixture("Trend", &artifact(), PlotType::Line, 0)],
			&request(),
		);
		assert_eq!(
			deferred.manifest.figures[0].inspection,
			Inspection::Omitted {
				reason: "no-regions".to_owned(),
				observed: None
			}
		);
		let projected = publish(
			vec![fixture("Points", &artifact(), PlotType::Scatter, 0)],
			&request(),
		);
		assert!(matches!(
			projected.manifest.figures[0].inspection,
			Inspection::Projected { regions: 3, .. }
		));
		assert!(projected.files.contains_key("figures/points/hits.json"));
		assert!(projected.files.contains_key(INSPECT_FILE));
		assert!(projected.manifest.files.contains_key(INSPECT_FILE));
	}

	fn parse_shell(shell: &str) -> roxmltree::Document<'_> {
		roxmltree::Document::parse_with_options(
			shell,
			roxmltree::ParsingOptions {
				allow_dtd: true,
				..roxmltree::ParsingOptions::default()
			},
		)
		.expect("the shell is well-formed XML")
	}

	/// PRODUCT-0155 §6, gate 6: hostile strings in the *projected data* — the
	/// hit prose and a nominal value — and in a label reach the shell as data
	/// and as text; the data element parses as JSON and canonicalizes back to
	/// the complete file bytes; exactly one executable script exists.
	#[test]
	fn product_0155_shell_is_a_polyglot_with_one_pinned_script_and_reversible_data() {
		let artifact = artifact();
		let title = "</script><script>alert(1)</script> Tom & Jerry <b>";
		let package = publish(
			vec![fixture(title, &artifact, PlotType::Scatter, 0)],
			&request(),
		);
		let id = package.manifest.figures[0].id.clone();
		let hostile = "</script><script>alert(2)</script> & \\u003c literal";
		let mut projection: Projection =
			serde_json::from_slice(&package.files[&format!("figures/{id}/hits.json")])
				.expect("projection parses");
		projection.hits[0].presentation = hostile.to_owned();
		projection.hits[0].fields[0].value = HitValue::Nominal {
			value: hostile.to_owned(),
		};
		let canonical = canonical_json(&projection).expect("canonical");
		let mut embedded = BTreeMap::new();
		embedded.insert(id.clone(), embed_json(&canonical));
		let shell = shell_html(&package.manifest.figures, &package.files, &embedded);

		let document = parse_shell(&shell);
		let scripts: Vec<_> = document
			.descendants()
			.filter(|node| node.has_tag_name(("http://www.w3.org/1999/xhtml", "script")))
			.collect();
		let executable: Vec<_> = scripts
			.iter()
			.filter(|node| node.attribute("type").is_none())
			.collect();
		assert_eq!(executable.len(), 1, "exactly one executable script element");
		assert_eq!(executable[0].text(), Some(INSPECT_JS));
		let data: Vec<_> = scripts
			.iter()
			.filter(|node| node.attribute("type") == Some("application/json"))
			.collect();
		assert_eq!(data.len(), 1);
		let embedded_text = data[0].text().expect("data element text");
		assert!(
			!embedded_text.contains("</script"),
			"no raw closer inside the data element"
		);
		let decoded: Projection =
			serde_json::from_str(embedded_text).expect("embedded JSON parses");
		assert_eq!(
			canonical_json(&decoded).expect("canonical again"),
			canonical,
			"the embedding canonicalizes back to the complete file bytes"
		);
		assert_eq!(
			decoded.hits[0].presentation, hostile,
			"hostile prose survives intact"
		);
		let texts: Vec<&str> = document
			.descendants()
			.filter(|node| node.has_tag_name(("http://www.w3.org/2000/svg", "text")))
			.filter_map(|node| node.text())
			.collect();
		assert!(
			texts.contains(&title),
			"the hostile label survives as text: {texts:?}"
		);
		let meta: Vec<_> = document
			.descendants()
			.filter(|node| node.has_tag_name(("http://www.w3.org/1999/xhtml", "meta")))
			.collect();
		assert_eq!(meta.len(), 3, "void elements parse as XML");
		assert!(shell.contains(&format!("'sha256-{}'", script_hash_b64())));
		assert!(!INSPECT_JS.contains('<') && !INSPECT_JS.contains('&'));
		// The shell assemble writes carries the complete canonical bytes too.
		let real_shell = String::from_utf8(package.files[SHELL_FILE].clone()).expect("utf-8");
		let real = parse_shell(&real_shell);
		let real_data = real
			.descendants()
			.find(|node| node.attribute("type") == Some("application/json"))
			.and_then(|node| node.text())
			.expect("assembled data element");
		let real_decoded: Projection =
			serde_json::from_str(real_data).expect("assembled JSON parses");
		let again = canonical_json(&real_decoded).expect("canonical");
		let file = &package.files[&format!("figures/{id}/hits.json")];
		if again != *file {
			let at = again
				.iter()
				.zip(file.iter())
				.position(|(a, b)| a != b)
				.unwrap_or(0);
			let window = |bytes: &[u8]| {
				String::from_utf8_lossy(&bytes[at.saturating_sub(60)..(at + 60).min(bytes.len())])
					.into_owned()
			};
			panic!(
				"assembled embedding diverges at byte {at} ({} vs {} bytes)\nagain: {}\nfile:  {}",
				again.len(),
				file.len(),
				window(&again),
				window(file)
			);
		}
	}

	/// PRODUCT-0154 §8 extended by PRODUCT-0155 §1: facet metadata and value
	/// displays pass through the structural guard, with a positive control.
	#[test]
	fn product_0155_facet_metadata_and_displays_are_guarded_with_a_control() {
		let mut request = request();
		request.secrets.push("hunter2".to_owned());
		let guarded = Guarded::new(&request);
		let clean = Hit {
			mark_family: "scatter".to_owned(),
			dimensionality: "point".to_owned(),
			fields: vec![HitField {
				label: "X".to_owned(),
				semantic_type: "quantitative".to_owned(),
				value: HitValue::Quantitative {
					value: 1.0,
					display: "1".to_owned(),
				},
			}],
			facet: Some(HitFacet {
				field: "cohort".to_owned(),
				value: "Atlas".to_owned(),
			}),
			presentation: "X: 1".to_owned(),
		};
		assert!(
			guarded.check_hit("hits.json", &clean).is_ok(),
			"positive control"
		);
		let mut facet_value = clean.clone();
		facet_value.facet.as_mut().unwrap().value = "hunter2".to_owned();
		assert!(matches!(
			guarded.check_hit("hits.json", &facet_value),
			Err(PublishError::Secret { field, .. }) if field == "facet.value"
		));
		let mut facet_field = clean.clone();
		facet_field.facet.as_mut().unwrap().field = "/home/tester/cohort".to_owned();
		assert!(matches!(
			guarded.check_hit("hits.json", &facet_field),
			Err(PublishError::Secret { field, .. }) if field == "facet.field"
		));
		let mut display = clean.clone();
		display.fields[0].value = HitValue::Quantitative {
			value: 1.0,
			display: "postgres://db/x".to_owned(),
		};
		assert!(matches!(
			guarded.check_hit("hits.json", &display),
			Err(PublishError::Secret { field, .. }) if field == "field.display"
		));
	}
}

fn check_value(guarded: &Guarded<'_>, file: &str, value: &HitValue) -> Result<(), PublishError> {
	match value {
		HitValue::Nominal { value } | HitValue::Ordinal { value } => {
			guarded.check(file, "field.value", value)
		}
		HitValue::Quantitative { display, .. }
		| HitValue::Temporal { display, .. }
		| HitValue::Percentage { display, .. } => guarded.check(file, "field.display", display),
		HitValue::Count { .. } => Ok(()),
		HitValue::Interval { start, end, .. } => {
			check_value(guarded, file, start)?;
			check_value(guarded, file, end)
		}
	}
}
