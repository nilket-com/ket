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
/// Schema 2 since PRODUCT-0157 §6: a region names its series ordinal.
pub(crate) const PROJECTION_SCHEMA_VERSION: u32 = 2;
pub(crate) const PROJECTION_BYTE_BOUND: usize = 1024 * 1024;
pub(crate) const PROJECTION_REGION_BOUND: usize = 32_768;
/// PRODUCT-0157 §2: the series table and its bound.
pub(crate) const SERIES_SCHEMA_VERSION: u32 = 1;
pub(crate) const SERIES_BOUND: usize = 4_096;
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
	/// PRODUCT-0157 §2: whether a series table exists and whether the browser
	/// may act on it — two separate facts.
	pub(crate) visibility: Visibility,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "kebab-case")]
pub(crate) enum Visibility {
	/// Covered, identified, within the bound: the table is written and the
	/// browser dispatches the four actions.
	Actionable { series: u64 },
	/// The table is written for the reader; the legend dispatches nothing.
	Inert { series: u64, reason: String },
	/// No table: no identified series, or over the bound.
	Absent {
		reason: String,
		observed: Option<u64>,
	},
}

// PRODUCT-0157 §2: the series table. Ordinals are the handle; the value
// crosses only as the display-bearing hit value. No exact scalar, no scalar
// type, no zone or scale, and no selection candidate ever enters this file.

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeriesTable {
	pub(crate) schema_version: u32,
	pub(crate) viewbox: [f64; 2],
	pub(crate) actions: SeriesActions,
	/// PRODUCT-0157 §3: the publisher's hidden ordinals at the publish moment,
	/// applied once as the browser's starting state. A snapshot, never
	/// persistence.
	pub(crate) initially_hidden: Vec<u32>,
	pub(crate) series: Vec<SeriesRow>,
	pub(crate) legend: Option<LegendTable>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields, tag = "kind", rename_all = "kebab-case")]
pub(crate) enum SeriesActions {
	Available,
	Unavailable { reason: String },
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeriesRow {
	pub(crate) ordinal: u32,
	/// The label preparation formatted, joined to the ordinal by identity.
	pub(crate) label: String,
	pub(crate) field: SeriesField,
	/// The partition value as the tagged display DTO. For temporal values the
	/// number is the source's own tick count in its unit; the display string
	/// is authoritative, exactly as for a hit.
	pub(crate) value: HitValue,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct SeriesField {
	pub(crate) label: String,
	pub(crate) semantic_type: String,
}

/// The legend's bounds and one rectangle per entry, in `viewBox` units, from
/// the same geometry that draws the legend and routes the desktop's presses.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegendTable {
	pub(crate) x0: f64,
	pub(crate) y0: f64,
	pub(crate) x1: f64,
	pub(crate) y1: f64,
	pub(crate) entries: Vec<LegendEntryRect>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct LegendEntryRect {
	pub(crate) x0: f64,
	pub(crate) y0: f64,
	pub(crate) x1: f64,
	pub(crate) y1: f64,
	/// The ordinal this entry addresses; absent for an entry without an
	/// identity or with an ambiguous union identity.
	pub(crate) series: Option<u32>,
}

// PRODUCT-0157 §2: end of the series table.

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
		/// PRODUCT-0157 §6: the series ordinal, skipped while hidden.
		series: Option<u32>,
	},
	Circle {
		cx: f64,
		cy: f64,
		r: f64,
		ordinal: u32,
		hit: usize,
		series: Option<u32>,
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
	Republished {
		figure: String,
	},
	NoRecipe {
		figure: String,
	},
	Rebuild {
		figure: String,
		error: String,
	},
	Data {
		figure: String,
		error: String,
	},
	Secret {
		file: String,
		field: String,
	},
	/// PRODUCT-0157 guardrail 3: a registry identity preparation did not
	/// label. Labels are never recovered by string matching.
	SeriesLabel {
		figure: String,
	},
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
			Self::SeriesLabel { figure } => write!(
				f,
				"Publish · refused: {figure} has a series its preparation did not label"
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
	if let (Some(drive), Some(':'), Some(sep)) = (chars.next(), chars.next(), chars.next())
		&& drive.is_ascii_alphabetic()
		&& (sep == '\\' || sep == '/')
	{
		return true;
	}
	trimmed.contains("://")
}

/// The consistency witness: tokens captured from the live panels before the
/// exports were taken. A figure whose token moved between capture and read
/// republished, and the package refuses rather than mixing two publications.
/// PRODUCT-0158 §4: the publish action as one call - assemble under the
/// witness and write to `path` - so the menu and the driver cannot differ in
/// what they do between the witness and the directory.
pub(crate) fn assemble_and_write(
	witness: &[Option<Publication>],
	exports: Vec<HostPlotExport>,
	request: &PublishRequest,
	path: &Path,
) -> Result<Package, PublishError> {
	let package = assemble(witness, exports, request)?;
	package.write(path)?;
	Ok(package)
}

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
	let mut embedded_series: BTreeMap<String, String> = BTreeMap::new();
	let mut initially_hidden: BTreeMap<String, Vec<u32>> = BTreeMap::new();
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

		// PRODUCT-0157 §1 and §3: an actionable figure — covered, identified,
		// within the table bound — is exported complete, with the desktop's
		// hidden set carried as the browser's starting state. Every other
		// figure publishes with the desktop's visibility applied.
		let export_bounds = iced::Rectangle {
			x: 0.0,
			y: 0.0,
			width: export.width,
			height: export.height,
		};
		let desktop_widget = widget_for(&export, &export.settings_model);
		let registry = desktop_widget.series_registry().clone();
		let covered = desktop_widget.series_covered(export_bounds);
		let desktop_hidden = desktop_widget.hidden_series().clone();
		drop(desktop_widget);
		let identified = registry.identities().len();
		let within_bound = identified <= SERIES_BOUND;
		let actionable = covered && identified > 0 && within_bound;
		let mut render_model = export.settings_model.clone();
		if actionable {
			render_model.pane_override.hidden_series = crate::plot::spec::Override::Inherit;
		}

		// Fallback render. Labels drawn from data are checked in their raw
		// form here; the SVG escapes them afterwards.
		let svg = render_figure_svg(&export, &render_model);
		// PRODUCT-0156 §5 kept the ordinal-to-identity mapping on the export;
		// PRODUCT-0157 §2 is the package that carries it. The desktop widget's
		// registry decided actionability above and must be the same registry.
		debug_assert_eq!(svg.series, registry, "one registry per figure");
		let svg_file = format!("{figure_dir}/figure.svg");
		for label in svg_text_contents(&svg.svg) {
			guarded.check(&svg_file, "text", &unescape_xml(&label))?;
		}

		// PRODUCT-0157 §2: the series table, or the reason there is none.
		let series_file = format!("{figure_dir}/series.json");
		let visibility = if identified == 0 {
			Visibility::Absent {
				reason: "no-identified-series".to_owned(),
				observed: None,
			}
		} else if !within_bound {
			Visibility::Absent {
				reason: "series-limit".to_owned(),
				observed: Some(identified as u64),
			}
		} else {
			let legend = widget_for(&export, &render_model)
				.legend_hit_geometry(crate::coordinate::Rect::from_iced_frame(export_bounds));
			let table = series_table(
				&export,
				&svg.series,
				&desktop_hidden,
				legend.as_ref(),
				actionable,
			)
			.ok_or_else(|| PublishError::SeriesLabel {
				figure: figure_name.clone(),
			})?;
			for row in &table.series {
				guarded.check(&series_file, "series.label", &row.label)?;
				guarded.check(&series_file, "series.field.label", &row.field.label)?;
				check_value(&guarded, &series_file, &row.value)?;
			}
			let bytes = canonical_json(&table).map_err(PublishError::Encoding)?;
			embedded_series.insert(id.clone(), embed_json(&bytes));
			if actionable {
				initially_hidden.insert(id.clone(), table.initially_hidden.clone());
			}
			files.insert(series_file, bytes);
			if actionable {
				Visibility::Actionable {
					series: identified as u64,
				}
			} else {
				Visibility::Inert {
					series: identified as u64,
					reason: "raster".to_owned(),
				}
			}
		};

		// PRODUCT-0155: the hit projection, or the reason there is none.
		let outcome = match project_export(&export, &render_model) {
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
			visibility,
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
		shell_html(
			&figures,
			&files,
			&embedded,
			&embedded_series,
			&initially_hidden,
		)
		.into_bytes(),
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

pub(crate) fn setting_value_json(value: &SettingValue) -> serde_json::Value {
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

/// The widget an export renders, projects, and measures through, over the
/// settings model the caller chose: the desktop's, or PRODUCT-0157 §3's
/// complete model with the hidden set cleared.
pub(crate) fn widget_for<'a>(
	export: &'a HostPlotExport,
	settings_model: &crate::plot::theme::PlotSettings,
) -> PlotWidget<'a> {
	PlotWidget::new(
		export.kernel.as_ref(),
		export.title.clone(),
		20.0,
		settings_model.clone(),
		export.render_revision,
		export.resize_render_suspended,
		PlotRenderLayer::Full,
	)
}

fn render_figure_svg(
	export: &HostPlotExport,
	settings_model: &crate::plot::theme::PlotSettings,
) -> crate::plot::export::SvgFigure {
	let bounds = iced::Rectangle {
		x: 0.0,
		y: 0.0,
		width: export.width,
		height: export.height,
	};
	crate::plot::export::render_svg(&widget_for(export, settings_model), bounds)
}

/// PRODUCT-0157 §2: the series table. The registry is the authority for
/// ordinals and order; labels and field metadata come from preparation and
/// are joined by exact identity equality. Returns none when a registry
/// identity has no prepared label, which guardrail 3 makes a refusal.
pub(crate) fn series_table(
	export: &HostPlotExport,
	registry: &crate::plot::series::SeriesRegistry,
	desktop_hidden: &crate::plot::series::HiddenSeries,
	legend: Option<&crate::plot::legend::LegendHitGeometry>,
	actionable: bool,
) -> Option<SeriesTable> {
	let labels = export.kernel.series_labels();
	let mut series = Vec::with_capacity(registry.identities().len());
	let mut initially_hidden = Vec::new();
	for (ordinal, identity) in registry.identities().iter().enumerate() {
		let label = labels.iter().find(|label| label.identity == *identity)?;
		if desktop_hidden.contains(identity) {
			initially_hidden.push(ordinal as u32);
		}
		series.push(SeriesRow {
			ordinal: ordinal as u32,
			label: label.label.clone(),
			field: SeriesField {
				label: identity.field.name.as_str().to_owned(),
				semantic_type: semantic_type_name(identity.field.semantic_type).to_owned(),
			},
			value: partition_value_dto(&identity.value, identity.field.semantic_type, &label.label),
		});
	}
	let legend = legend.map(|geometry| LegendTable {
		x0: f64::from(geometry.bounds.x),
		y0: f64::from(geometry.bounds.y),
		x1: f64::from(geometry.bounds.x + geometry.bounds.width),
		y1: f64::from(geometry.bounds.y + geometry.bounds.height),
		entries: geometry
			.entries
			.iter()
			.map(|(rectangle, identity)| LegendEntryRect {
				x0: f64::from(rectangle.x),
				y0: f64::from(rectangle.y),
				x1: f64::from(rectangle.x + rectangle.width),
				y1: f64::from(rectangle.y + rectangle.height),
				// An inert figure's legend addresses nothing (§5).
				series: identity
					.as_ref()
					.filter(|_| actionable)
					.and_then(|identity| registry.group(identity))
					.map(|group| group.ordinal() as u32),
			})
			.collect(),
	});
	Some(SeriesTable {
		schema_version: SERIES_SCHEMA_VERSION,
		viewbox: [f64::from(export.width), f64::from(export.height)],
		actions: if actionable {
			SeriesActions::Available
		} else {
			SeriesActions::Unavailable {
				reason: "raster".to_owned(),
			}
		},
		initially_hidden,
		series,
		legend,
	})
}

/// The partition value as the display-bearing DTO. Nothing exact crosses:
/// no unit, zone, or scale, only the kind, a number where the kind has one,
/// and the label preparation formatted as the display.
fn partition_value_dto(
	value: &crate::selection::ExactScalar,
	semantic_type: SemanticType,
	display: &str,
) -> HitValue {
	use crate::selection::ExactScalar;
	let categorical = |text: String| match semantic_type {
		SemanticType::Ordinal => HitValue::Ordinal { value: text },
		_ => HitValue::Nominal { value: text },
	};
	match value {
		ExactScalar::Str(text) => categorical(text.clone()),
		ExactScalar::Bool(flag) => categorical(flag.to_string()),
		ExactScalar::Int(int) => HitValue::Quantitative {
			value: *int as f64,
			display: display.to_owned(),
		},
		ExactScalar::UInt(int) => HitValue::Quantitative {
			value: *int as f64,
			display: display.to_owned(),
		},
		ExactScalar::Float(float) => HitValue::Quantitative {
			value: *float,
			display: display.to_owned(),
		},
		ExactScalar::Decimal { mantissa, scale } => HitValue::Quantitative {
			value: *mantissa as f64 / 10f64.powi(i32::from(*scale)),
			display: display.to_owned(),
		},
		ExactScalar::Date { days } => HitValue::Temporal {
			value: f64::from(*days),
			display: display.to_owned(),
		},
		ExactScalar::Datetime { ticks, .. } | ExactScalar::Duration { ticks, .. } => {
			HitValue::Temporal {
				value: *ticks as f64,
				display: display.to_owned(),
			}
		}
		ExactScalar::Time { nanos } => HitValue::Temporal {
			value: *nanos as f64,
			display: display.to_owned(),
		},
	}
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
						series: entry.series.map(|group| group.ordinal() as u32),
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
							series: entry.series.map(|group| group.ordinal() as u32),
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
pub(crate) fn project_export(
	export: &HostPlotExport,
	settings_model: &crate::plot::theme::PlotSettings,
) -> Option<Vec<ResolutionStage>> {
	let bounds = iced::Rectangle {
		x: 0.0,
		y: 0.0,
		width: export.width,
		height: export.height,
	};
	widget_for(export, settings_model).hit_projection(bounds)
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
	embedded_series: &BTreeMap<String, String>,
	initially_hidden: &BTreeMap<String, Vec<u32>>,
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
	// PRODUCT-0157 §5: hiding is one class on an ordinal-addressed group. A
	// hidden series draws nothing; its legend sample dims to the desktop's
	// own quarter opacity. Nothing else in the SVG is touched.
	html.push_str("g[data-series].polariton-hidden { display: none; }\n");
	html.push_str("g[data-legend].polariton-hidden { opacity: 0.25; }\n");
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
		// PRODUCT-0157 §3: the snapshot is applied at emission, so a hidden
		// series never paints before the script runs. `figure.svg` stays the
		// complete render; only the inlined copy carries the class.
		let svg = match initially_hidden.get(&figure.id) {
			Some(hidden) => snapshot_classes(&svg, hidden),
			None => svg,
		};
		// Every inlined SVG shares one document id namespace; a clip id must
		// resolve to its own figure's rectangle, never the first on the page.
		let svg = scope_svg_ids(&svg, &figure.id);
		html.push_str(&svg);
		html.push('\n');
		if let Some(projection) = embedded.get(&figure.id) {
			html.push_str("<script type=\"application/json\" class=\"polariton-hits\">");
			html.push_str(projection);
			html.push_str("</script>\n");
		}
		if let Some(table) = embedded_series.get(&figure.id) {
			html.push_str("<script type=\"application/json\" class=\"polariton-series\">");
			html.push_str(table);
			html.push_str("</script>\n");
		}
		html.push_str("</figure>\n");
	}
	html.push_str("</main>\n<script>");
	html.push_str(INSPECT_JS);
	html.push_str("</script>\n</body>\n</html>\n");
	html
}

/// The SVG backend mints `clip_N` per figure. Inlined together, the browser
/// would resolve every reference to the first definition on the page, so
/// the inlined copy prefixes each id and each reference with the figure id.
/// `figure.svg` stays as rendered.
fn scope_svg_ids(svg: &str, figure_id: &str) -> String {
	// Only the two forms `SvgBackend::with_clip` emits: the definition's id
	// attribute and the group's reference attribute. A text node cannot hold
	// either, because the backend escapes `<` in text.
	let scope = html_escape(figure_id);
	svg.replace(
		"<clipPath id=\"clip_",
		&format!("<clipPath id=\"{scope}-clip_"),
	)
	.replace(
		"<g clip-path=\"url(#clip_",
		&format!("<g clip-path=\"url(#{scope}-clip_"),
	)
	// PRODUCT-0159 §3: a translucent mark batch defines one circle and
	// references it per mark; both halves are scoped the same way.
	.replace("<circle id=\"mark_", &format!("<circle id=\"{scope}-mark_"))
	.replace("<use href=\"#mark_", &format!("<use href=\"#{scope}-mark_"))
}

/// The inlined SVG with the hidden class on every group an actionable
/// figure's snapshot hides: the same class the script toggles, on the same
/// ordinal-addressed groups, and nothing else.
fn snapshot_classes(svg: &str, hidden: &[u32]) -> String {
	let mut out = svg.to_owned();
	for ordinal in hidden {
		for attribute in ["data-series", "data-legend"] {
			out = out.replace(
				&format!("<g {attribute}=\"s{ordinal}\">"),
				&format!("<g {attribute}=\"s{ordinal}\" class=\"polariton-hidden\">"),
			);
		}
	}
	out
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
pub(crate) mod tests_support {
	pub(crate) use super::tests::{artifact, fixture, fixture_with_spec, publish, request};
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

	pub(crate) fn artifact() -> Arc<ResultArtifact> {
		artifact_with("sqlite")
	}

	fn try_fixture(
		title: &str,
		artifact: &Arc<ResultArtifact>,
		plot_type: PlotType,
		ordinal: u32,
	) -> Option<HostPlotExport> {
		try_fixture_with_spec(
			title,
			artifact,
			plot_type,
			ordinal,
			AuthoredPlotSpec::xy(PlotComposition::single(plot_type), "x", "y"),
		)
	}

	/// PRODUCT-0157 fixtures author a series channel; the spec is the caller's.
	pub(crate) fn fixture_with_spec(
		title: &str,
		artifact: &Arc<ResultArtifact>,
		plot_type: PlotType,
		ordinal: u32,
		authored_spec: AuthoredPlotSpec,
	) -> HostPlotExport {
		try_fixture_with_spec(title, artifact, plot_type, ordinal, authored_spec)
			.expect("fixture plans")
	}

	fn try_fixture_with_spec(
		title: &str,
		artifact: &Arc<ResultArtifact>,
		plot_type: PlotType,
		ordinal: u32,
		authored_spec: AuthoredPlotSpec,
	) -> Option<HostPlotExport> {
		let resolved = plan(PlotRequest {
			source: PlotSource::Materialized {
				name: title.to_owned(),
				data: Arc::clone(artifact),
			},
			authored_spec,
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
			status: "fresh",
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

	pub(crate) fn fixture(
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

	pub(crate) fn request() -> PublishRequest {
		request_at("2026-09-03T00:00:00Z")
	}

	fn witness_of(exports: &[HostPlotExport]) -> Vec<Option<Publication>> {
		exports.iter().map(|export| export.publication).collect()
	}

	pub(crate) fn publish(exports: Vec<HostPlotExport>, request: &PublishRequest) -> Package {
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
		// The inlined copy is the file with its ids scoped to the figure.
		assert!(shell.contains(&scope_svg_ids(&figure_svg(&package, "left"), "left")));
		assert!(!figure_svg(&package, "left").contains("left-clip_"));
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

		// A symlinked listed file refuses at open. Creating a symlink on Windows
		// needs a privilege an ordinary test run does not have.
		#[cfg(unix)]
		{
			let linked = temp_root("symlink");
			package.write(&linked).expect("second write");
			let target = linked.join(SHELL_FILE);
			std::fs::remove_file(&target).expect("remove");
			std::os::unix::fs::symlink(root.join(MANIFEST_FILE), &target).expect("symlink");
			assert!(
				ServedPackage::open(&linked).is_err(),
				"symlinks never escape the root"
			);
			let _ = std::fs::remove_dir_all(&linked);
		}

		assert!(
			serve(root.clone(), "0.0.0.0:7743".parse().expect("addr")).is_err(),
			"non-loopback binds refuse"
		);
		let _ = std::fs::remove_dir_all(&root);
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
		let shell = shell_html(
			&package.manifest.figures,
			&package.files,
			&embedded,
			&BTreeMap::new(),
			&BTreeMap::new(),
		);

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

/// PRODUCT-0157: the category-visible published dashboard.
#[cfg(test)]
mod product_0157_tests {
	use super::tests_support::*;
	use super::*;
	use crate::coordinate::Rect;
	use crate::plot::common::{PlotKernel, ScatterRenderMode};
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::kernels::line::{LinePlotKernel, prepare_line_data};
	use crate::plot::series::{HiddenSeries, SeriesVisibilityAction};
	use crate::plot::spec::{AuthoredPlotSpec, LegendPosition, Override, Presence};
	use crate::plot::theme::PlotSettings;
	use crate::result_artifact::ResultArtifact;
	use crate::selection::InputIdentity;
	use boa_engine::{Context, JsValue, Source};
	use iced::advanced::mouse::Cursor;
	use iced::{Point, Rectangle};
	use std::sync::Arc;

	fn series_artifact(frame: polars::prelude::DataFrame) -> Arc<ResultArtifact> {
		let artifact = ResultArtifact::from_polars(Arc::new(frame));
		artifact.bind_input_identity(InputIdentity::mint());
		artifact
	}

	fn abc() -> Arc<ResultArtifact> {
		series_artifact(
			polars::df!(
				"series" => ["a", "a", "b", "b", "c", "c"],
				"x" => [0.0, 1.0, 0.0, 1.0, 0.0, 1.0],
				"y" => [0.0, 0.0, 1.0, 1.0, 2.0, 2.0]
			)
			.expect("frame"),
		)
	}

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

	fn hide(export: &mut HostPlotExport, indices: &[usize]) {
		let ids = export.kernel.series_identities();
		let mut hidden = HiddenSeries::default();
		for index in indices {
			hidden.apply(&SeriesVisibilityAction::Hide(ids[*index].clone()), &ids);
		}
		export.settings_model.pane_override.hidden_series = Override::Set(hidden);
	}

	fn export_bounds(export: &HostPlotExport) -> Rectangle {
		Rectangle {
			x: 0.0,
			y: 0.0,
			width: export.width,
			height: export.height,
		}
	}

	fn table_of(package: &Package, id: &str) -> SeriesTable {
		serde_json::from_slice(&package.files[&format!("figures/{id}/series.json")])
			.expect("series table parses")
	}

	fn projection_of(package: &Package, id: &str) -> Projection {
		serde_json::from_slice(&package.files[&format!("figures/{id}/hits.json")])
			.expect("projection parses")
	}

	fn hand_export(kernel: Arc<dyn PlotKernel + Send + Sync>) -> HostPlotExport {
		HostPlotExport {
			status: "fresh",
			title: "hand".to_owned(),
			kernel,
			settings_model: PlotSettings::default(),
			render_revision: 0,
			resize_render_suspended: false,
			width: 320.0,
			height: 220.0,
			recipe: None,
			predicate: Predicate::default(),
			selection_revision: crate::selection::SelectionRevision::ZERO,
			displayed_selection_revision: crate::selection::SelectionRevision::ZERO,
			layout: GridRect {
				col: 0,
				row: 0,
				width: 2,
				height: 2,
			},
			view_name: None,
			plot_type: None,
			publication: None,
			authored_expression: None,
			document: None,
		}
	}

	/// The table the writer would produce for a hand-built export, through the
	/// same widget, registry, legend geometry, and join.
	fn hand_table(export: &HostPlotExport, actionable: bool) -> SeriesTable {
		let widget = widget_for(export, &export.settings_model);
		let registry = widget.series_registry().clone();
		let hidden = widget.hidden_series().clone();
		let legend = widget.legend_hit_geometry(Rect::from_iced_frame(export_bounds(export)));
		series_table(export, &registry, &hidden, legend.as_ref(), actionable).expect("labelled")
	}

	/// The shipped asset in the engine, no document, calling one pure function.
	fn asset_eval(script: &str) -> (Context, JsValue) {
		let mut context = Context::default();
		context
			.eval(Source::from_bytes(INSPECT_JS))
			.expect("the asset loads without a document");
		let value = context
			.eval(Source::from_bytes(script))
			.expect("the asset function runs");
		(context, value)
	}

	fn js_numbers(context: &mut Context, value: &JsValue) -> Vec<u32> {
		let array = value.as_object().expect("array");
		let length = array
			.get(boa_engine::js_string!("length"), context)
			.expect("length")
			.to_number(context)
			.expect("number") as usize;
		(0..length)
			.map(|index| {
				array
					.get(index as u32, context)
					.expect("element")
					.to_number(context)
					.expect("number") as u32
			})
			.collect()
	}

	fn asset_sequence(table: &SeriesTable, hidden: &[u32], events: &str) -> Vec<u32> {
		let script = format!(
			"polaritonLegendSequence(JSON.parse({}), {:?}, {events})",
			serde_json::to_string(&serde_json::to_string(table).unwrap()).unwrap(),
			hidden
		);
		let (mut context, value) = asset_eval(&script);
		js_numbers(&mut context, &value)
	}

	fn asset_resolve(projection: &str, x: f64, y: f64, hidden: &[u32]) -> (i64, u64) {
		let script = format!(
			"var __r = polaritonResolve(JSON.parse({}), {x:?}, {y:?}, polaritonHiddenSet({hidden:?})); [__r.index, __r.ops]",
			serde_json::to_string(projection).unwrap()
		);
		let (mut context, value) = asset_eval(&script);
		let pair = js_numbers(&mut context, &value);
		let array = value.as_object().unwrap();
		let index = array
			.get(0, &mut context)
			.unwrap()
			.to_number(&mut context)
			.unwrap();
		(index as i64, u64::from(pair[1]))
	}

	/// Gate 1: two identities that print alike are two rows with two ordinals,
	/// no exact field anywhere, and labels joined by identity.
	#[test]
	fn product_0157_series_table_is_a_handle_not_a_disclosure() {
		use crate::plot::common::{AxisType, TimeUnit, format_label};
		use polars::prelude::{Column, DataType};
		let mut frame = polars::df!("x" => [0.0, 1.0], "y" => [1.0, 2.0]).unwrap();
		frame
			.with_column(
				Column::new("series".into(), [1_i64, 2])
					.cast(&DataType::Datetime(
						polars::prelude::TimeUnit::Nanoseconds,
						None,
					))
					.unwrap(),
			)
			.unwrap();
		let mut prepared = prepare_line_data(&frame, "series", "x", "y");
		for (index, series) in prepared.series.iter_mut().enumerate() {
			series.name = format_label(
				(index + 1) as f64,
				&AxisType::Datetime {
					unit: TimeUnit::Nanoseconds,
					zone: None,
				},
			);
		}
		assert_eq!(prepared.series[0].name, prepared.series[1].name);
		let export = hand_export(Arc::new(LinePlotKernel::new(Arc::new(prepared))));
		let table = hand_table(&export, true);
		assert_eq!(table.series.len(), 2);
		assert_eq!(table.series[0].ordinal, 0);
		assert_eq!(table.series[1].ordinal, 1);
		assert_eq!(table.series[0].label, table.series[1].label);
		assert_eq!(table.series[0].field.label, "series");
		// The field keeps the role the desktop minted for the partition channel;
		// the value keeps its own kind.
		assert_eq!(table.series[0].field.semantic_type, "nominal");
		match (&table.series[0].value, &table.series[1].value) {
			(HitValue::Temporal { value: a, display }, HitValue::Temporal { value: b, .. }) => {
				assert_ne!(a, b, "distinct ticks");
				assert_eq!(display, &table.series[0].label);
			}
			other => panic!("temporal display DTOs expected: {other:?}"),
		}
		let text = String::from_utf8(canonical_json(&table).unwrap()).unwrap();
		for exact in ["scalar_type", "zone", "scale", "candidate", "ticks", "unit"] {
			assert!(
				!text.contains(&format!("\"{exact}\"")),
				"{exact} crossed: {text}"
			);
		}
		let legend = table.legend.expect("legend");
		assert_eq!(legend.entries.len(), 2);
		assert_eq!(legend.entries[0].series, Some(0));
		assert_eq!(legend.entries[1].series, Some(1));

		// Labels move with their identity, never by position or string.
		let frame = polars::df!("series" => ["alpha", "alpha", "beta", "beta"], "x" => [0.0, 1.0, 0.0, 1.0], "y" => [0.0, 0.0, 1.0, 1.0]).unwrap();
		let mut swapped = prepare_line_data(&frame, "series", "x", "y");
		let first = swapped.series[0].name.clone();
		swapped.series[0].name = swapped.series[1].name.clone();
		swapped.series[1].name = first;
		let plain = hand_table(
			&hand_export(Arc::new(LinePlotKernel::new(Arc::new(prepare_line_data(
				&frame, "series", "x", "y",
			))))),
			true,
		);
		let swapped = hand_table(
			&hand_export(Arc::new(LinePlotKernel::new(Arc::new(swapped)))),
			true,
		);
		assert_eq!(plain.series[0].label, "alpha");
		assert_eq!(swapped.series[0].label, "beta");
		assert_eq!(plain.series[0].value, swapped.series[0].value);
		assert_eq!(plain.series[0].ordinal, swapped.series[0].ordinal);
		assert_eq!(plain.legend, swapped.legend);

		// An ambiguous union entry addresses nothing; its identities keep rows.
		use crate::plot::kernels::multivariate_rows::{
			MultivariateInputRow, MultivariateRows, MultivariateValueCell,
		};
		use crate::plot::kernels::radar::RadarPlotKernel;
		use crate::plot::spec::MultivariateFields;
		let fields = MultivariateFields::new(
			vec!["x".into(), "y".into(), "z".into()],
			Some("series".into()),
		);
		let rows = Arc::new(
			MultivariateRows::from_rows(
				&fields,
				(0..2)
					.map(|index| MultivariateInputRow {
						category: Some("same label".into()),
						values: vec![MultivariateValueCell::Exact(index as f64 + 1.0); 3],
					})
					.collect(),
			)
			.unwrap(),
		);
		let frame = polars::df!("series" => [Some(false), Some(true)]).unwrap();
		let radar = RadarPlotKernel {
			prepared_data: rows,
			row_identities: Vec::new(),
			category_colors: None,
		}
		.with_series_source(&frame, Some("series"));
		let ambiguous = hand_table(&hand_export(Arc::new(radar)), true);
		assert_eq!(ambiguous.series.len(), 2);
		let legend = ambiguous.legend.expect("legend");
		assert_eq!(legend.entries.len(), 1);
		assert_eq!(legend.entries[0].series, None);
	}

	/// Gate 2 and gate 3: byte-identical tables across publishes, and legend
	/// rectangles equal to the desktop's geometry in gutter and overlay
	/// placements.
	#[test]
	fn product_0157_table_is_deterministic_and_legend_is_the_desktop_geometry() {
		let artifact = abc();
		let first = publish(
			vec![colored("Trend", &artifact, PlotType::Line)],
			&request(),
		);
		let again = publish(
			vec![colored("Trend", &artifact, PlotType::Line)],
			&request(),
		);
		assert_eq!(
			first.files["figures/trend/series.json"],
			again.files["figures/trend/series.json"]
		);
		assert_eq!(
			first.manifest.figures[0].visibility,
			Visibility::Actionable { series: 3 }
		);
		for overlay in [false, true] {
			let mut export = colored("Trend", &artifact, PlotType::Line);
			if overlay {
				export.settings_model.authored_spec.legend =
					Presence::Show(LegendPosition::new(0.5, 0.5).unwrap());
			}
			let desktop = widget_for(&export, &export.settings_model)
				.legend_hit_geometry(Rect::from_iced_frame(export_bounds(&export)))
				.expect("legend");
			let package = publish(vec![export], &request());
			let table = table_of(&package, "trend");
			let legend = table.legend.expect("legend");
			assert_eq!(legend.entries.len(), desktop.entries.len());
			for (packaged, (rectangle, _)) in legend.entries.iter().zip(&desktop.entries) {
				assert_eq!(packaged.x0, f64::from(rectangle.x));
				assert_eq!(packaged.y0, f64::from(rectangle.y));
				assert_eq!(packaged.x1, f64::from(rectangle.x + rectangle.width));
				assert_eq!(packaged.y1, f64::from(rectangle.y + rectangle.height));
			}
			assert_eq!(legend.x0, f64::from(desktop.bounds.x));
			assert_eq!(
				legend.y1,
				f64::from(desktop.bounds.y + desktop.bounds.height)
			);
			assert_eq!(table.viewbox, [320.0, 220.0]);
		}
	}

	/// Gate 4: a figure published with a series hidden carries every series'
	/// marks, swatch group, and regions, and names the hidden ordinal as the
	/// starting state; nothing else differs from the unhidden publish.
	#[test]
	fn product_0157_actionable_figures_publish_complete_with_a_snapshot() {
		let artifact = abc();
		let mut hidden = colored("Points", &artifact, PlotType::Scatter);
		hide(&mut hidden, &[0]);
		let desktop_model = hidden.settings_model.clone();
		let hidden = publish(vec![hidden], &request());
		let shown = publish(
			vec![colored("Points", &artifact, PlotType::Scatter)],
			&request(),
		);
		let svg = String::from_utf8(hidden.files["figures/points/figure.svg"].clone()).unwrap();
		assert!(svg.contains("data-series=\"s0\""));
		assert!(svg.contains("data-legend=\"s0\""));
		assert_eq!(
			hidden.files["figures/points/figure.svg"],
			shown.files["figures/points/figure.svg"]
		);
		assert_eq!(
			hidden.files["figures/points/hits.json"],
			shown.files["figures/points/hits.json"]
		);
		let projection = projection_of(&hidden, "points");
		assert!(projection.stages[0].regions.iter().any(|region| matches!(
			region,
			Region::Circle {
				series: Some(0),
				..
			}
		)));
		let table = table_of(&hidden, "points");
		assert_eq!(table.initially_hidden, vec![0]);
		assert_eq!(table.actions, SeriesActions::Available);
		let mut shown_table = table_of(&shown, "points");
		assert_eq!(shown_table.initially_hidden, Vec::<u32>::new());
		shown_table.initially_hidden = vec![0];
		assert_eq!(shown_table, table, "only the snapshot differs");
		for (name, bytes) in &shown.files {
			if name.ends_with("series.json") || name == MANIFEST_FILE || name == SHELL_FILE {
				continue;
			}
			assert_eq!(&hidden.files[name], bytes, "{name} differs");
		}
		// The starting state is the snapshot, applied before any gesture.
		assert_eq!(
			asset_sequence(&table, &table.initially_hidden, "[]"),
			vec![0]
		);
		// The desktop pane's model was cloned, never cleared.
		assert!(matches!(
			desktop_model.pane_override.hidden_series,
			Override::Set(ref set) if !set.is_empty()
		));
		let shell = String::from_utf8(hidden.files[SHELL_FILE].clone()).unwrap();
		assert_eq!(shell.matches("class=\"polariton-series\"").count(), 1);
		// The snapshot is in the emitted markup, before any script runs; the
		// packaged SVG stays complete and class-free.
		assert!(shell.contains("<g data-series=\"s0\" class=\"polariton-hidden\">"));
		assert!(shell.contains("<g data-legend=\"s0\" class=\"polariton-hidden\">"));
		assert!(!shell.contains("<g data-series=\"s1\" class="));
		assert!(!svg.contains("polariton-hidden"));
		let unhidden = String::from_utf8(shown.files[SHELL_FILE].clone()).unwrap();
		assert!(!unhidden.contains("class=\"polariton-hidden\""));
	}

	/// Gate 5: the shipped asset's legend sequence and the desktop's legend
	/// action produce the same hidden set for every gesture in §4.
	#[test]
	fn product_0157_gesture_parity_through_the_shipped_asset() {
		let artifact = abc();
		for overlay in [false, true] {
			let mut export = colored("Trend", &artifact, PlotType::Line);
			if overlay {
				export.settings_model.authored_spec.legend =
					Presence::Show(LegendPosition::new(0.5, 0.5).unwrap());
			}
			let widget = widget_for(&export, &export.settings_model);
			let geometry = widget
				.legend_hit_geometry(Rect::from_iced_frame(export_bounds(&export)))
				.expect("legend");
			let universe = widget.series_registry().identities().to_vec();
			let registry = widget.series_registry().clone();
			let centre = |index: usize| {
				let (rect, _) = &geometry.entries[index];
				(rect.x + rect.width / 2.0, rect.y + rect.height / 2.0)
			};
			let background = (
				geometry.bounds.x + geometry.bounds.width - 1.0,
				geometry.bounds.y + 1.0,
			);
			drop(widget);
			let package = publish(vec![export], &request());
			let table = table_of(&package, "trend");
			let (ax, ay) = centre(0);
			let (bx, by) = centre(1);
			let (cx, cy) = centre(2);
			let (gx, gy) = background;
			#[derive(Clone, Copy)]
			enum Ev {
				Down(f32, f32, bool),
				Up(f32, f32),
				Cancel,
				Blur,
				Alt(f32, f32),
				ShiftCtrl(f32, f32),
				ShiftMeta(f32, f32),
			}
			let sequences: Vec<Vec<Ev>> = vec![
				vec![Ev::Down(ax, ay, false), Ev::Up(ax, ay)],
				vec![
					Ev::Down(ax, ay, false),
					Ev::Up(ax, ay),
					Ev::Down(ax, ay, false),
					Ev::Up(ax, ay),
				],
				vec![Ev::Down(bx, by, true)],
				vec![Ev::Down(bx, by, true), Ev::Down(bx, by, true)],
				vec![
					Ev::Down(bx, by, true),
					Ev::Up(bx, by),
					Ev::Down(bx, by, false),
					Ev::Up(bx, by),
				],
				vec![Ev::Down(ax, ay, false), Ev::Up(bx, by)],
				vec![Ev::Down(ax, ay, false), Ev::Cancel, Ev::Up(ax, ay)],
				vec![Ev::Down(ax, ay, false), Ev::Blur, Ev::Up(ax, ay)],
				vec![Ev::Down(gx, gy, false), Ev::Up(gx, gy)],
				vec![Ev::Alt(ax, ay), Ev::Up(ax, ay)],
				vec![Ev::ShiftCtrl(ax, ay), Ev::Up(ax, ay)],
				vec![Ev::ShiftMeta(bx, by)],
				vec![
					Ev::ShiftCtrl(ax, ay),
					Ev::Down(ax, ay, false),
					Ev::Up(ax, ay),
				],
				vec![
					Ev::Down(cx, cy, true),
					Ev::Down(ax, ay, false),
					Ev::Up(ax, ay),
					Ev::Down(bx, by, true),
				],
			];
			for initial in [Vec::new(), vec![0_u32], vec![1, 2]] {
				for sequence in &sequences {
					// Desktop: the same presses through legend geometry and the
					// pane's action algebra.
					let mut desktop = HiddenSeries::default();
					for ordinal in &initial {
						desktop.apply(
							&SeriesVisibilityAction::Hide(universe[*ordinal as usize].clone()),
							&universe,
						);
					}
					let mut armed: Option<usize> = None;
					for event in sequence {
						match *event {
							Ev::Down(x, y, shift) => {
								armed = None;
								let Some((_, identity)) = geometry.entry_at(Point::new(x, y))
								else {
									continue;
								};
								let Some(identity) = identity else { continue };
								if shift {
									let action = desktop.legend_action(identity, true, &universe);
									desktop.apply(&action, &universe);
								} else {
									armed = geometry
										.entries
										.iter()
										.position(|(rect, _)| rect.contains(Point::new(x, y)));
								}
							}
							Ev::Up(x, y) => {
								let Some(pressed) = armed.take() else {
									continue;
								};
								let released = geometry
									.entries
									.iter()
									.position(|(rect, _)| rect.contains(Point::new(x, y)));
								if released != Some(pressed) {
									continue;
								}
								let identity = geometry.entries[pressed].1.as_ref().unwrap();
								let action = desktop.legend_action(identity, false, &universe);
								desktop.apply(&action, &universe);
							}
							Ev::Cancel
							| Ev::Blur
							| Ev::Alt(..)
							| Ev::ShiftCtrl(..)
							| Ev::ShiftMeta(..) => armed = None,
						}
					}
					let mut expected: Vec<u32> = universe
						.iter()
						.filter(|identity| desktop.contains(identity))
						.map(|identity| registry.group(identity).unwrap().ordinal() as u32)
						.collect();
					expected.sort_unstable();
					// Asset: the pure sequence over the packaged table.
					let events: Vec<String> = sequence
						.iter()
						.map(|event| match *event {
							Ev::Down(x, y, shift) => {
								format!("{{type:\"down\",x:{x:?},y:{y:?},shift:{shift}}}")
							}
							Ev::Up(x, y) => format!("{{type:\"up\",x:{x:?},y:{y:?},shift:false}}"),
							Ev::Cancel => "{type:\"cancel\"}".to_owned(),
							Ev::Blur => "{type:\"blur\"}".to_owned(),
							Ev::Alt(x, y) => {
								format!("{{type:\"down\",x:{x:?},y:{y:?},shift:false,alt:true}}")
							}
							Ev::ShiftCtrl(x, y) => {
								format!("{{type:\"down\",x:{x:?},y:{y:?},shift:true,ctrl:true}}")
							}
							Ev::ShiftMeta(x, y) => {
								format!("{{type:\"down\",x:{x:?},y:{y:?},shift:true,meta:true}}")
							}
						})
						.collect();
					let actual =
						asset_sequence(&table, &initial, &format!("[{}]", events.join(",")));
					assert_eq!(actual, expected, "overlay {overlay}, initial {initial:?}");
				}
			}
			// Unknown and repeated actions are no-ops in the asset's algebra.
			let same = format!(
				"var t = JSON.parse({}); var h = [0]; var u = polaritonUniverse(t); [polaritonApplyVisibility(h, {{kind:\"hide\", ordinal: 0}}, u) === h, polaritonApplyVisibility(h, {{kind:\"hide\", ordinal: 9}}, u) === h, polaritonApplyVisibility(h, {{kind:\"isolate\", ordinal: 9}}, u) === h, polaritonApplyVisibility([], {{kind:\"restore\", ordinal: 0}}, u).length]",
				serde_json::to_string(&serde_json::to_string(&table).unwrap()).unwrap()
			);
			let (mut context, value) = asset_eval(&same);
			let array = value.as_object().unwrap();
			for index in 0..3 {
				assert!(
					array.get(index, &mut context).unwrap().to_boolean(),
					"no-op {index} returns the same array"
				);
			}
		}
	}

	/// Gate 6: with a series hidden the asset answers what the desktop answers
	/// under the same hidden set, including none; with nothing hidden the
	/// answers are PRODUCT-0155's; the operation count is under the bound.
	#[test]
	fn product_0157_hits_obey_visibility_through_the_shipped_asset() {
		let artifact = series_artifact(
			polars::df!(
				"series" => ["a", "a", "b", "b", "b", "c"],
				"x" => [1.0, 3.0, 3.0, 5.0, 7.0, 3.0],
				"y" => [2.0, 2.0, 2.0, 6.0, 1.0, 2.0]
			)
			.expect("frame"),
		);
		let complete = colored("Points", &artifact, PlotType::Scatter);
		let stages = project_export(&complete, &complete.settings_model).expect("regions");
		let ProjectionOutcome::Projected(projection) =
			project(stages, complete.width, complete.height)
		else {
			panic!("projected")
		};
		let json = String::from_utf8(canonical_json(&projection).unwrap()).unwrap();
		let mut vectors = Vec::new();
		for stage in &projection.stages {
			for region in &stage.regions {
				if let Region::Circle { cx, cy, r, .. } = region {
					let (cx, cy) = (cx + stage.origin[0], cy + stage.origin[1]);
					for delta in [-0.02, 0.0, 0.02] {
						vectors.push((cx + r + delta, cy));
						vectors.push((cx, cy - r + delta));
					}
					vectors.push((cx, cy));
				}
			}
		}
		let step = 17;
		for i in 0..(1000 - vectors.len()) {
			let x = ((i * step) % 320) as f64 + 0.5;
			let y = ((i * step * 7) % 220) as f64 + 0.5;
			vectors.push((x, y));
		}
		assert!(vectors.len() >= 1000);
		let bounds = export_bounds(&complete);
		for hidden_indices in [vec![], vec![0], vec![0, 2]] {
			let mut desktop_export = colored("Points", &artifact, PlotType::Scatter);
			hide(&mut desktop_export, &hidden_indices);
			let widget = widget_for(&desktop_export, &desktop_export.settings_model);
			let hidden: Vec<u32> = hidden_indices.iter().map(|i| *i as u32).collect();
			let mut answered = 0;
			for (x, y) in &vectors {
				let (index, ops) = asset_resolve(&json, *x, *y, &hidden);
				assert!(ops <= 98_304, "{ops} operations");
				let asset = (index >= 0).then(|| projection.hits[index as usize].clone());
				let desktop = widget
					.structured_hit_at(bounds, Cursor::Available(Point::new(*x as f32, *y as f32)))
					.map(|hit| hit_dto(&hit));
				if asset.is_some() {
					answered += 1;
				}
				assert_eq!(asset, desktop, "hidden {hidden:?} at ({x}, {y})");
			}
			assert!(answered > 0, "positive control: something is hit");
		}
		// Nothing hidden: the resolver ignores the series field entirely.
		let (index, ops) = asset_resolve(&json, 1.0, 1.0, &[]);
		let (again, ops_again) = asset_resolve(&json, 1.0, 1.0, &[]);
		assert_eq!((index, ops), (again, ops_again));
	}

	/// Gate 7: the shell declares exactly the two rules; the legend group wraps
	/// the sample of every actionable entry and no label text.
	#[test]
	fn product_0157_ink_changes_by_class_alone() {
		let artifact = abc();
		let package = publish(
			vec![colored("Trend", &artifact, PlotType::Line)],
			&request(),
		);
		let shell = String::from_utf8(package.files[SHELL_FILE].clone()).unwrap();
		let style = shell
			.split("<style>")
			.nth(1)
			.unwrap()
			.split("</style>")
			.next()
			.unwrap();
		assert_eq!(style.matches("polariton-hidden").count(), 2);
		assert!(shell.contains("g[data-series].polariton-hidden { display: none; }"));
		assert!(shell.contains("g[data-legend].polariton-hidden { opacity: 0.25; }"));
		let svg = String::from_utf8(package.files["figures/trend/figure.svg"].clone()).unwrap();
		let table = table_of(&package, "trend");
		let actionable = table
			.legend
			.as_ref()
			.unwrap()
			.entries
			.iter()
			.filter(|entry| entry.series.is_some())
			.count();
		assert_eq!(actionable, 3);
		assert_eq!(svg.matches("<g data-legend=\"s").count(), 3);
		for ordinal in 0..3 {
			let open = format!("<g data-legend=\"s{ordinal}\">");
			let start = svg.find(&open).expect("legend group");
			let body = &svg[start + open.len()..];
			let end = body.find("</g>").expect("closed");
			let group = &body[..end];
			assert!(
				!group.contains("<text"),
				"label text stays outside: {group}"
			);
			assert!(!group.contains("data-"), "only the ordinal names the group");
		}
	}

	/// Gate 8: coverage decides. A rasterized scatter is inert with its table
	/// present; a mixed layered figure sharing an identity is inert as a whole
	/// and keeps the desktop's hiding in its ink and projection; no identified
	/// series and the series bound publish with `absent`, the over-bound
	/// figure still hiding what the desktop hid.
	#[test]
	fn product_0157_coverage_decides_and_mixed_figures_are_inert() {
		let artifact = abc();
		let mut raster = colored("Dense", &artifact, PlotType::Scatter);
		raster.settings_model.library_defaults.scatter_render_mode = ScatterRenderMode::Rasterized;
		hide(&mut raster, &[0]);
		let package = publish(vec![raster], &request());
		assert_eq!(
			package.manifest.figures[0].visibility,
			Visibility::Inert {
				series: 3,
				reason: "raster".to_owned()
			}
		);
		let table = table_of(&package, "dense");
		assert_eq!(
			table.actions,
			SeriesActions::Unavailable {
				reason: "raster".to_owned()
			}
		);
		assert_eq!(table.initially_hidden, vec![0]);
		assert!(
			table
				.legend
				.as_ref()
				.unwrap()
				.entries
				.iter()
				.all(|entry| entry.series.is_none())
		);
		let svg = String::from_utf8(package.files["figures/dense/figure.svg"].clone()).unwrap();
		assert_eq!(svg.matches("<image ").count(), 1);
		assert!(!svg.contains("data-series="));
		// An inert snapshot is the desktop's own ink: no class is emitted, so
		// its already-dimmed swatch is never dimmed again.
		let shell = String::from_utf8(package.files[SHELL_FILE].clone()).unwrap();
		assert!(!shell.contains("class=\"polariton-hidden\""));
		assert!(
			matches!(
				package.manifest.figures[0].inspection,
				Inspection::Projected { .. }
			),
			"dense: {:?}",
			package.manifest.figures[0].inspection
		);
		let projection = projection_of(&package, "dense");
		assert!(projection.stages[0].regions.iter().all(|region| !matches!(
			region,
			Region::Circle {
				series: Some(0),
				..
			}
		)));
		assert!(projection.stages[0].regions.iter().any(|region| matches!(
			region,
			Region::Circle {
				series: Some(1),
				..
			}
		)));
		assert_eq!(
			asset_sequence(&table, &[], "[{type:\"down\",x:0,y:0,shift:true}]"),
			Vec::<u32>::new()
		);

		// Mixed: grouped line marks and a rasterized scatter share identity "a".
		use crate::plot::common::AxisType;
		use crate::plot::kernels::{
			layered::{LayerRenderer, LayeredPlotKernel},
			scatter::prepare_scatter_data,
		};
		use crate::plot::spec::{LayerSettings, NonEmpty, ResolvedLayout};
		let frame = polars::df!("series" => ["a", "a", "b", "b"], "x" => [0.0, 1.0, 0.0, 1.0], "y" => [0.0, 0.0, 1.0, 1.0]).unwrap();
		let mut layers = NonEmpty::new(LayerRenderer::line(
			Arc::new(prepare_line_data(&frame, "series", "x", "y")),
			LayerSettings::default(),
		));
		layers.push(LayerRenderer::scatter(
			Arc::new(prepare_scatter_data(&frame, "series", "x", "y")),
			LayerSettings::default(),
		));
		let kernel = LayeredPlotKernel::new(
			layers,
			ResolvedLayout {
				viewport_width: 320,
				viewport_height: 220,
				x_range: (0.0, 1.0),
				y_range: (0.0, 1.0),
			},
			AxisType::Linear,
			AxisType::Linear,
			None,
			None,
			"x".to_owned(),
			"y".to_owned(),
		);
		let mut mixed = hand_export(Arc::new(kernel));
		let bounds = export_bounds(&mixed);
		assert!(widget_for(&mixed, &mixed.settings_model).series_covered(bounds));
		mixed.settings_model.library_defaults.scatter_render_mode = ScatterRenderMode::Rasterized;
		assert!(!widget_for(&mixed, &mixed.settings_model).series_covered(bounds));
		hide(&mut mixed, &[0]);
		let table = hand_table(&mixed, false);
		assert_eq!(table.series.len(), 2, "one row per identity across layers");
		assert!(
			table
				.legend
				.unwrap()
				.entries
				.iter()
				.all(|entry| entry.series.is_none())
		);
		let svg = render_figure_svg(&mixed, &mixed.settings_model).svg;
		assert!(
			!svg.contains("data-series=\"s0\""),
			"the hidden line marks stay absent"
		);
		assert!(svg.contains("data-series=\"s1\""));

		// No identified series: absent, no table.
		let plain = publish(
			vec![fixture("Points", &artifact, PlotType::Scatter, 0)],
			&request(),
		);
		assert_eq!(
			plain.manifest.figures[0].visibility,
			Visibility::Absent {
				reason: "no-identified-series".to_owned(),
				observed: None
			}
		);
		assert!(!plain.files.contains_key("figures/points/series.json"));

		// Over the bound, with a series hidden on the desktop: absent, no
		// table, and the hidden series stays out of the ink and the regions.
		let n = SERIES_BOUND + 1;
		let many = series_artifact(
			polars::df!(
				"series" => (0..n).map(|i| format!("s{i}")).collect::<Vec<_>>(),
				"x" => (0..n).map(|i| (i % 64) as f64).collect::<Vec<_>>(),
				"y" => (0..n).map(|i| (i / 64) as f64).collect::<Vec<_>>()
			)
			.expect("frame"),
		);
		// The planner authors a colour channel over a bounded palette; the
		// kernel is prepared directly so the registry holds every identity.
		let mut over = fixture("Many", &many, PlotType::Scatter, 0);
		over.kernel = Arc::new(crate::plot::kernels::scatter::ScatterPlotKernel::new(
			Arc::new(prepare_scatter_data(
				&many.projection().unwrap(),
				"series",
				"x",
				"y",
			)),
		));
		hide(&mut over, &[0]);
		let package = publish(vec![over], &request());
		assert_eq!(
			package.manifest.figures[0].visibility,
			Visibility::Absent {
				reason: "series-limit".to_owned(),
				observed: Some(n as u64)
			}
		);
		assert!(!package.files.contains_key("figures/many/series.json"));
		let svg = String::from_utf8(package.files["figures/many/figure.svg"].clone()).unwrap();
		assert!(!svg.contains("data-series=\"s0\""));
		assert!(svg.contains("data-series=\"s1\""));
		// The entry stays, drawn in the desktop's hidden state (PRODUCT-0156 §3).
		assert!(svg.contains("data-legend=\"s0\""));
		// Its projection is over PRODUCT-0155's byte bound on its own, so the
		// regions are checked at the seam the writer uses for an inert figure:
		// the desktop's own settings, hidden set applied.
		let mut over = fixture("Many", &many, PlotType::Scatter, 0);
		over.kernel = Arc::new(crate::plot::kernels::scatter::ScatterPlotKernel::new(
			Arc::new(prepare_scatter_data(
				&many.projection().unwrap(),
				"series",
				"x",
				"y",
			)),
		));
		hide(&mut over, &[0]);
		let stages = project_export(&over, &over.settings_model).expect("regions");
		let regions: usize = stages.iter().map(|stage| stage.regions.len()).sum();
		assert_eq!(regions, n - 1, "the hidden series' region is absent");
		assert!(
			stages[0]
				.regions
				.iter()
				.all(|entry| { entry.series.map(|group| group.ordinal()) != Some(0) })
		);
	}
}

/// PRODUCT-0157 review findings: the installed handlers and the worst-case
/// resolver, exercised in the engine rather than inferred from the pure
/// functions.
#[cfg(test)]
mod product_0157_lifecycle_tests {
	use super::tests_support::*;
	use super::*;
	use crate::plot::common::ScatterRenderMode;
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::series::{HiddenSeries, SeriesVisibilityAction};
	use crate::plot::spec::{AuthoredPlotSpec, Override};
	use crate::result_artifact::ResultArtifact;
	use crate::selection::InputIdentity;
	use boa_engine::{Context, Source};
	use std::sync::Arc;

	/// A document stub with exactly the surface the installer touches:
	/// element creation, one figure per package figure, the SVG's bounding
	/// rectangle equal to its `viewBox` so pointer mapping is the identity,
	/// ordinal-addressed groups with a class list, recorded listeners, and
	/// pointer capture. Nothing here computes; it records.
	const DOM_STUB: &str = r#"
var __log = [];
function __El(tag) {
	var self = this;
	this.tag = tag; this.classes = {}; this.listeners = {}; this.hidden = false;
	this.style = {}; this.textContent = ""; this.className = "";
	this.classList = {
		add: function (c) { self.classes[c] = true; },
		remove: function (c) { delete self.classes[c]; },
		contains: function (c) { return self.classes[c] === true; }
	};
	this.addEventListener = function (type, fn) {
		if (!self.listeners[type]) { self.listeners[type] = []; }
		self.listeners[type].push(fn);
	};
	this.dispatch = function (type, ev) {
		var fns = self.listeners[type] || [];
		for (var i = 0; i in fns; i++) { fns[i](ev); }
	};
}
function __Figure(hits, series, width, height) {
	var self = this;
	this.svg = new __El("svg");
	this.svg.groups = {};
	this.svg.captured = [];
	this.svg.viewBox = { baseVal: { width: width, height: height } };
	this.svg.getBoundingClientRect = function () { return { left: 0, top: 0, width: width, height: height }; };
	this.svg.setPointerCapture = function (id) { self.svg.captured.push(id); };
	this.svg.querySelectorAll = function (selector) {
		var out = [];
		var parts = selector.split(", ");
		for (var p = 0; p in parts; p++) {
			var key = parts[p];
			if (!self.svg.groups[key]) { self.svg.groups[key] = new __El("g"); }
			out.push(self.svg.groups[key]);
		}
		return out;
	};
	this.querySelector = function (selector) {
		if (selector === "svg") { return self.svg; }
		if (selector === "script.polariton-hits") { return hits === null ? null : { textContent: hits }; }
		if (selector === "script.polariton-series") { return series === null ? null : { textContent: series }; }
		return null;
	};
	this.groupHidden = function (attribute, ordinal) {
		var g = self.svg.groups["[" + attribute + "=\"s" + ordinal + "\"]"];
		if (!g) { return null; }
		return g.classes["polariton-hidden"] === true;
	};
}
var __figures = [];
var window = new __El("window");
var document = {
	readyState: "complete",
	body: new __El("body"),
	createElement: function (tag) { return new __El(tag); },
	addEventListener: function () {},
	querySelectorAll: function (selector) { return selector === "figure.figure" ? __figures : []; }
};
document.body.appendChild = function () {};
function __pointer(x, y, mods) {
	mods = mods || {};
	return {
		button: mods.button === undefined ? 0 : mods.button,
		clientX: x, clientY: y, pointerId: 7,
		shiftKey: !!mods.shift, altKey: !!mods.alt, ctrlKey: !!mods.ctrl, metaKey: !!mods.meta,
		preventDefault: function () {}
	};
}
"#;

	fn series_artifact(frame: polars::prelude::DataFrame) -> Arc<ResultArtifact> {
		let artifact = ResultArtifact::from_polars(Arc::new(frame));
		artifact.bind_input_identity(InputIdentity::mint());
		artifact
	}

	fn abc() -> Arc<ResultArtifact> {
		series_artifact(
			polars::df!(
				"series" => ["a", "a", "b", "b", "c", "c"],
				"x" => [0.0, 1.0, 0.0, 1.0, 0.0, 1.0],
				"y" => [0.0, 0.0, 1.0, 1.0, 2.0, 2.0]
			)
			.expect("frame"),
		)
	}

	fn colored(title: &str, artifact: &Arc<ResultArtifact>) -> HostPlotExport {
		fixture_with_spec(
			title,
			artifact,
			PlotType::Scatter,
			0,
			AuthoredPlotSpec::xy_color(
				PlotComposition::single(PlotType::Scatter),
				"x",
				"y",
				Some("series".to_owned()),
			),
		)
	}

	fn js_string(text: &str) -> String {
		serde_json::to_string(text).unwrap()
	}
	type FigureStub<'a> = (Option<&'a [u8]>, Option<&'a [u8]>, f32, f32);

	/// Install the asset over stub figures, then run `script` and return its
	/// value as JSON text.
	fn installed(figures: &[FigureStub<'_>], script: &str) -> String {
		let mut context = Context::default();
		context
			.eval(Source::from_bytes(DOM_STUB))
			.expect("the stub loads");
		for (hits, series, width, height) in figures {
			let hits = hits.map_or("null".to_owned(), |bytes| {
				js_string(std::str::from_utf8(bytes).unwrap())
			});
			let series = series.map_or("null".to_owned(), |bytes| {
				js_string(std::str::from_utf8(bytes).unwrap())
			});
			context
				.eval(Source::from_bytes(&format!(
					"__figures.push(new __Figure({hits}, {series}, {width:?}, {height:?}));"
				)))
				.expect("figure stub");
		}
		context
			.eval(Source::from_bytes(INSPECT_JS))
			.expect("the asset installs over the stub");
		let value = context
			.eval(Source::from_bytes(&format!("JSON.stringify({script})")))
			.expect("the probe runs");
		value
			.to_string(&mut context)
			.expect("string")
			.to_std_string_escaped()
	}

	/// Finding 1, 3, 4, and 5 through the installed handlers: the snapshot
	/// is applied on install for an actionable figure and never for an inert
	/// one; a release outside the SVG disarms the press; forbidden modifiers
	/// with shift do nothing; the ordinary toggle and isolate still work.
	#[test]
	fn product_0157_installed_handlers_own_the_sequence_through_a_document_stub() {
		let artifact = abc();
		let mut export = colored("Points", &artifact);
		let ids = export.kernel.series_identities();
		let mut hidden = HiddenSeries::default();
		hidden.apply(&SeriesVisibilityAction::Hide(ids[0].clone()), &ids);
		export.settings_model.pane_override.hidden_series = Override::Set(hidden);
		let package = publish(vec![export], &request());
		let table: SeriesTable =
			serde_json::from_slice(&package.files["figures/points/series.json"]).unwrap();
		let legend = table.legend.as_ref().unwrap();
		let centre = |index: usize| {
			let e = &legend.entries[index];
			((e.x0 + e.x1) / 2.0, (e.y0 + e.y1) / 2.0)
		};
		let (ax, ay) = centre(0);
		let (bx, by) = centre(1);
		let outside_legend = (1.0, 1.0);
		assert!(
			legend.x0 > outside_legend.0 || legend.y0 > outside_legend.1,
			"the probe point is outside the legend"
		);
		let figures = [(
			Some(package.files["figures/points/hits.json"].as_slice()),
			Some(package.files["figures/points/series.json"].as_slice()),
			320.0_f32,
			220.0_f32,
		)];
		let probe = format!(
			r#"(function () {{
	var f = __figures[0]; var svg = f.svg; var steps = {{}};
	steps.install = [f.groupHidden("data-series", 0), f.groupHidden("data-legend", 0), f.groupHidden("data-series", 1)];
	steps.listeners = Object.keys(svg.listeners).sort();
	svg.dispatch("pointerdown", __pointer({ax:?}, {ay:?}));
	svg.dispatch("pointerup", __pointer({ax:?}, {ay:?}));
	steps.toggle = [f.groupHidden("data-series", 0), f.groupHidden("data-legend", 0), svg.captured.length];
	svg.dispatch("pointerdown", __pointer({ax:?}, {ay:?}));
	window.dispatch("pointerup", {{}});
	svg.dispatch("pointerdown", __pointer({ox:?}, {oy:?}));
	svg.dispatch("pointerup", __pointer({ax:?}, {ay:?}));
	steps.staleRelease = [f.groupHidden("data-series", 0)];
	svg.dispatch("pointerdown", __pointer({bx:?}, {by:?}, {{shift: true, ctrl: true}}));
	svg.dispatch("pointerup", __pointer({bx:?}, {by:?}));
	steps.shiftCtrl = [f.groupHidden("data-series", 0), f.groupHidden("data-series", 1), f.groupHidden("data-series", 2)];
	svg.dispatch("pointerdown", __pointer({bx:?}, {by:?}, {{shift: true}}));
	svg.dispatch("pointerup", __pointer({bx:?}, {by:?}));
	steps.isolate = [f.groupHidden("data-series", 0), f.groupHidden("data-series", 1), f.groupHidden("data-series", 2), f.groupHidden("data-legend", 2)];
	svg.dispatch("pointerdown", __pointer({ax:?}, {ay:?}));
	svg.dispatch("pointercancel", {{}});
	svg.dispatch("pointerup", __pointer({ax:?}, {ay:?}));
	steps.cancelled = [f.groupHidden("data-series", 0)];
	svg.dispatch("pointerdown", __pointer({ax:?}, {ay:?}));
	window.dispatch("blur", {{}});
	svg.dispatch("pointerup", __pointer({ax:?}, {ay:?}));
	steps.blurred = [f.groupHidden("data-series", 0)];
	return steps;
}})()"#,
			ox = outside_legend.0,
			oy = outside_legend.1,
		);
		let steps: serde_json::Value = serde_json::from_str(&installed(&figures, &probe)).unwrap();
		assert_eq!(steps["install"], serde_json::json!([true, true, false]));
		assert_eq!(
			steps["listeners"],
			serde_json::json!([
				"lostpointercapture",
				"pointercancel",
				"pointerdown",
				"pointerleave",
				"pointermove",
				"pointerup"
			])
		);
		assert_eq!(
			steps["toggle"],
			serde_json::json!([false, false, 1]),
			"a bare toggle shows s0"
		);
		assert_eq!(
			steps["staleRelease"],
			serde_json::json!([false]),
			"a release outside the SVG disarms; the later release over the entry toggles nothing"
		);
		assert_eq!(steps["shiftCtrl"], serde_json::json!([false, false, false]));
		assert_eq!(
			steps["isolate"],
			serde_json::json!([true, false, true, true])
		);
		assert_eq!(steps["cancelled"], serde_json::json!([true]));
		assert_eq!(steps["blurred"], serde_json::json!([true]));

		// An inert figure: no class on install, no gesture listeners.
		let mut raster = colored("Dense", &artifact);
		raster.settings_model.library_defaults.scatter_render_mode = ScatterRenderMode::Rasterized;
		let ids = raster.kernel.series_identities();
		let mut hidden = HiddenSeries::default();
		hidden.apply(&SeriesVisibilityAction::Hide(ids[0].clone()), &ids);
		raster.settings_model.pane_override.hidden_series = Override::Set(hidden);
		let package = publish(vec![raster], &request());
		let figures = [(
			package
				.files
				.get("figures/dense/hits.json")
				.map(Vec::as_slice),
			Some(package.files["figures/dense/series.json"].as_slice()),
			320.0_f32,
			220.0_f32,
		)];
		let probe = r#"(function () {
	var f = __figures[0];
	return [Object.keys(f.svg.groups).length, Object.keys(f.svg.listeners).sort()];
})()"#;
		let inert: serde_json::Value = serde_json::from_str(&installed(&figures, probe)).unwrap();
		assert_eq!(
			inert,
			serde_json::json!([0, ["pointerleave", "pointermove"]]),
			"no group is touched and no gesture is installed"
		);
	}

	/// Finding 2: the resolver at the bound with the maximum hidden set does
	/// the counted work and nothing proportional to the hidden count. The
	/// projection is 32,768 circles cycling through 4,096 series ordinals;
	/// 4,095 of them hidden leaves one series answering.
	#[test]
	fn product_0157_resolver_at_the_bound_with_the_maximum_hidden_set() {
		let mut regions = Vec::with_capacity(PROJECTION_REGION_BOUND);
		for index in 0..PROJECTION_REGION_BOUND {
			regions.push(Region::Circle {
				cx: (index % 256) as f64 * 4.0 + 2.0,
				cy: (index / 256) as f64 * 4.0 + 2.0,
				r: 1.5,
				ordinal: index as u32,
				hit: index % 2,
				series: Some((index % SERIES_BOUND) as u32),
			});
		}
		let projection = Projection {
			schema_version: PROJECTION_SCHEMA_VERSION,
			viewbox: [1024.0, 512.0],
			stages: vec![Stage {
				policy: "nearest-centre-within".to_owned(),
				tie: "later-wins".to_owned(),
				within: None,
				origin: [0.0, 0.0],
				regions,
			}],
			hits: vec![
				Hit {
					mark_family: "scatter".to_owned(),
					dimensionality: "point".to_owned(),
					fields: Vec::new(),
					facet: None,
					presentation: "even".to_owned(),
				},
				Hit {
					mark_family: "scatter".to_owned(),
					dimensionality: "point".to_owned(),
					fields: Vec::new(),
					facet: None,
					presentation: "odd".to_owned(),
				},
			],
		};
		let json = String::from_utf8(canonical_json(&projection).unwrap()).unwrap();
		let hidden: Vec<u32> = (0..(SERIES_BOUND as u32 - 1)).collect();
		let mut context = Context::default();
		context.eval(Source::from_bytes(INSPECT_JS)).unwrap();
		context
			.eval(Source::from_bytes(&format!(
				"var __p = JSON.parse({}); var __h = polaritonHiddenSet({hidden:?});",
				serde_json::to_string(&json).unwrap()
			)))
			.unwrap();
		// The one shown series is ordinal 4095: region 4095 sits at column 255,
		// row 15. A pointer there answers it; a pointer on a hidden region's
		// centre answers nothing; both count exactly the per-region budget.
		let shown_centre = (
			(4095 % 256) as f64 * 4.0 + 2.0,
			(4095 / 256) as f64 * 4.0 + 2.0,
		);
		let hidden_centre = (2.0, 2.0);
		for ((x, y), expected) in [(shown_centre, 1_i64), (hidden_centre, -1)] {
			let value = context
				.eval(Source::from_bytes(&format!(
					"var __r = polaritonResolve(__p, {x:?}, {y:?}, __h); [__r.index, __r.ops]"
				)))
				.unwrap();
			let array = value.as_object().unwrap();
			let index = array
				.get(0, &mut context)
				.unwrap()
				.to_number(&mut context)
				.unwrap() as i64;
			let ops = array
				.get(1, &mut context)
				.unwrap()
				.to_number(&mut context)
				.unwrap() as u64;
			assert_eq!(index, expected, "at ({x}, {y})");
			assert!(ops <= 98_304, "{ops} operations at ({x}, {y})");
			assert!(
				ops >= PROJECTION_REGION_BOUND as u64,
				"every region was visited once"
			);
		}
		// The membership structure is one lookup per region: a second full
		// pass with an empty hidden set counts the same order of work, so the
		// hidden count cannot be a multiplier on the counted operations.
		let value = context
			.eval(Source::from_bytes(
				"var __e = polaritonResolve(__p, 2.0, 2.0, polaritonHiddenSet([])); __e.ops",
			))
			.unwrap();
		let empty_ops = value.to_number(&mut context).unwrap() as u64;
		assert!(empty_ops <= 98_304);
	}
}

/// The published shell inlines every figure's SVG into one document, so SVG
/// ids share one namespace. A clip id resolved to another figure's rectangle
/// clipped a wide figure's axes to a narrow figure's width in the browser.
#[cfg(test)]
mod shell_id_scope_tests {
	use super::tests_support::*;
	use super::*;
	use crate::plot::core::PlotType;

	#[test]
	fn shell_scopes_svg_ids_per_figure_and_every_reference_resolves_within_its_figure() {
		let artifact = artifact();
		let mut narrow = fixture("Narrow", &artifact, PlotType::Scatter, 0);
		narrow.width = 200.0;
		// A label that spells a clip reference is text, and text is never scoped.
		let label = "Wide url(#clip_0) id=\"clip_0\"";
		let mut wide = fixture(label, &artifact, PlotType::Line, 1);
		wide.width = 900.0;
		let package = publish(vec![narrow, wide], &request());
		let shell = String::from_utf8(package.files[SHELL_FILE].clone()).unwrap();
		// Every id in the document is unique.
		let ids: Vec<&str> = shell
			.split("id=\"")
			.skip(1)
			.filter_map(|rest| rest.split('"').next())
			.collect();
		let mut unique = ids.clone();
		unique.sort_unstable();
		unique.dedup();
		assert_eq!(ids.len(), unique.len(), "duplicate ids: {ids:?}");
		// Every clip reference names an id defined by its own figure, and both
		// figures clip: the positive control for the assay.
		let mut clipping_figures = 0;
		let wide_id = package.manifest.figures[1].id.clone();
		for id in ["narrow", wide_id.as_str()] {
			let open = format!("<figure class=\"figure\" id=\"{id}\"");
			let start = shell.find(&open).expect("figure element");
			let body = &shell[start..];
			let end = body.find("</figure>").expect("closed figure");
			let body = &body[..end];
			let references: Vec<&str> = body
				.split("clip-path=\"url(#")
				.skip(1)
				.filter_map(|rest| rest.split(')').next())
				.collect();
			if !references.is_empty() {
				clipping_figures += 1;
			}
			for reference in references {
				assert!(
					reference.starts_with(&format!("{id}-clip_")),
					"{id} references {reference}"
				);
				assert!(
					!reference.contains('"'),
					"{id} reference is an attribute value"
				);
				assert!(
					body.contains(&format!("id=\"{reference}\"")),
					"{id} defines {reference} in its own SVG"
				);
			}
		}
		assert_eq!(clipping_figures, 2, "both fixtures clip their axis pass");
		// The packaged file is the render as made; only the inlined copy is scoped.
		let file =
			String::from_utf8(package.files[&format!("figures/{wide_id}/figure.svg")].clone())
				.unwrap();
		assert!(file.contains("<clipPath id=\"clip_0\">"));
		assert!(!file.contains(&format!("{wide_id}-clip_")));
		// The label survives unchanged as text in the shell and in the file.
		let escaped = xml_escape(label);
		assert!(
			shell.contains(&format!(">{escaped}</text>")),
			"shell text: {escaped}"
		);
		assert!(file.contains(&format!(">{escaped}</text>")));
		assert!(shell.contains(&format!("aria-label=\"{}\"", html_escape(label))));
	}
}

/// PRODUCT-0159 §3: a translucent mark batch's definition and references
/// are scoped per figure in the shell like clip ids.
#[cfg(test)]
mod product_0159_shell_tests {
	use super::*;

	#[test]
	fn product_0159_shell_scopes_mark_definitions_and_references() {
		let svg = "<defs><circle id=\"mark_0\" r=\"3.00\" /></defs>\n<use href=\"#mark_0\" x=\"1.00\" y=\"2.00\" />\n<text>href=\"#mark_0\" id=\"mark_0\"</text>";
		let scoped = scope_svg_ids(svg, "dense");
		assert!(scoped.contains("<circle id=\"dense-mark_0\""));
		assert!(scoped.contains("<use href=\"#dense-mark_0\""));
		assert!(
			scoped.contains("<text>href=\"#mark_0\" id=\"mark_0\"</text>"),
			"text is never scoped"
		);
	}
}
