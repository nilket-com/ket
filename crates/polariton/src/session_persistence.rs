//! Polariton's opaque PRODUCT-0062 saved-session owner.
//!
//! This module is deliberately a schema boundary. Runtime plotting and editor
//! types do not implement `Serialize`; conversion is explicit so placement,
//! renderer vocabulary, and process-local identities cannot enter by accident.
use crate::plot::spec::{
	AllocationSpaceSnapshot, AuthoredFamilyId, ColorAssignmentSnapshot, ColorIdentity,
	ColorIdentitySnapshot, ColorLayerIdentity, ColorSpaceSnapshot,
};
use crate::rune_relation_envelope::LocalViewIdentity;
use libaryon::core::{BufferId, DocumentId};
use libaryon::engine::{HostPanelGroupId, HostPanelId};
use libaryon::uast::projection::SharedIdentityCensus;
use serde::{Deserialize, Serialize};
use std::collections::{BTreeSet, HashMap};
use std::fmt;

pub(crate) const OWNER_NAME: &str = "polariton_session";
const INITIAL_CLOSED_SCHEMA: u32 = 1;
const AREA_CLOSED_SCHEMA: u32 = 2;
pub(crate) const SCHEMA_VERSION: u32 = 3;
pub(crate) const MAX_OWNER_BYTES: usize = 1024 * 1024;
pub(crate) const DYNAMIC_PANEL_FLOOR: u64 = 1_u64 << 63;
pub(crate) const GROUP_FLOOR: u64 = 1;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct DurableMeanings {
	pub panels: HashMap<(DocumentId, LocalViewIdentity), HostPanelId>,
	pub groups: HashMap<DocumentId, HostPanelGroupId>,
	pub colours: HashMap<(DocumentId, LocalViewIdentity, usize), ColorIdentity>,
	pub next_view_panel_id: u64,
	pub next_view_group_id: u64,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct RuntimeMeanings {
	pub panels: HashMap<(BufferId, LocalViewIdentity), HostPanelId>,
	pub groups: HashMap<BufferId, HostPanelGroupId>,
	pub colours: RuntimeColours,
	pub next_view_panel_id: u64,
	pub next_view_group_id: u64,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub(crate) struct RuntimeColours(HashMap<(BufferId, LocalViewIdentity, usize), ColorIdentity>);

impl RuntimeColours {
	pub(crate) fn new(
		values: HashMap<(BufferId, LocalViewIdentity, usize), ColorIdentity>,
	) -> Self {
		Self(values)
	}

	pub(crate) fn as_map(&self) -> &HashMap<(BufferId, LocalViewIdentity, usize), ColorIdentity> {
		&self.0
	}

	pub(crate) fn into_map(self) -> HashMap<(BufferId, LocalViewIdentity, usize), ColorIdentity> {
		self.0
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum SessionPersistenceError {
	Refused(String),
}

impl fmt::Display for SessionPersistenceError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Refused(reason) => write!(formatter, "Polariton session refused: {reason}"),
		}
	}
}

impl std::error::Error for SessionPersistenceError {}

impl SessionPersistenceError {
	fn refused(reason: impl Into<String>) -> Self {
		Self::Refused(reason.into())
	}
}

impl DurableMeanings {
	pub(crate) fn encode(&self) -> Result<Vec<u8>, SessionPersistenceError> {
		validate_marks(self)?;
		let payload = Payload::from_meanings(self)?;
		let bytes = serde_json::to_vec(&payload)
			.map_err(|error| SessionPersistenceError::refused(error.to_string()))?;
		if bytes.len() > MAX_OWNER_BYTES {
			return Err(SessionPersistenceError::refused(format!(
				"owner payload is {} bytes (limit is {MAX_OWNER_BYTES})",
				bytes.len()
			)));
		}
		Ok(bytes)
	}

	pub(crate) fn decode(
		schema_version: u32,
		bytes: &[u8],
	) -> Result<Self, SessionPersistenceError> {
		Self::decode_with_bound(schema_version, bytes, MAX_OWNER_BYTES)
	}

	fn decode_with_bound(
		schema_version: u32,
		bytes: &[u8],
		bound: usize,
	) -> Result<Self, SessionPersistenceError> {
		ensure_supported_schema(schema_version, SCHEMA_VERSION)?;
		let family_policy = FamilyAdmissionPolicy::for_schema(schema_version)?;
		if bytes.len() > bound {
			return Err(SessionPersistenceError::refused(format!(
				"owner payload is {} bytes (limit is {bound})",
				bytes.len()
			)));
		}
		let mut deserializer = serde_json::Deserializer::from_slice(bytes);
		let payload = Payload::deserialize(&mut deserializer)
			.map_err(|error| SessionPersistenceError::refused(error.to_string()))?;
		deserializer.end().map_err(|error| {
			SessionPersistenceError::refused(format!("trailing content: {error}"))
		})?;
		let meanings = payload.into_meanings(family_policy)?;
		validate_marks(&meanings)?;
		Ok(meanings)
	}

	/// Join all three relations at once after libaryon has projected both its
	/// durable document bindings and its complete shared-identity census.
	pub(crate) fn join_runtime(
		&self,
		document_bindings: &HashMap<DocumentId, BufferId>,
		census: &SharedIdentityCensus,
	) -> Result<RuntimeMeanings, SessionPersistenceError> {
		let used_documents = self
			.panels
			.keys()
			.map(|(document, _)| *document)
			.chain(self.groups.keys().copied())
			.chain(self.colours.keys().map(|(document, _, _)| *document))
			.collect::<BTreeSet<_>>();
		if !used_documents.is_subset(&census.documents) {
			return Err(SessionPersistenceError::refused(
				"payload names a document absent from the restored census",
			));
		}
		if !self
			.panels
			.values()
			.all(|panel| census.host_panels.contains(panel))
		{
			return Err(SessionPersistenceError::refused(
				"payload names a panel absent from the restored census",
			));
		}
		if !self
			.groups
			.values()
			.all(|group| census.host_panel_groups.contains(group))
		{
			return Err(SessionPersistenceError::refused(
				"payload names a panel group absent from the restored census",
			));
		}
		if !used_documents
			.iter()
			.all(|document| document_bindings.contains_key(document))
		{
			return Err(SessionPersistenceError::refused(
				"restored document binding is incomplete",
			));
		}

		let panels = self
			.panels
			.iter()
			.map(|((document, view), panel)| {
				(
					(*document_bindings.get(document).unwrap(), view.clone()),
					*panel,
				)
			})
			.collect();
		let groups = self
			.groups
			.iter()
			.map(|(document, group)| (*document_bindings.get(document).unwrap(), *group))
			.collect();
		let colours = RuntimeColours::new(
			self.colours
				.iter()
				.map(|((document, view, len), identity)| {
					(
						(
							*document_bindings.get(document).unwrap(),
							view.clone(),
							*len,
						),
						identity.clone(),
					)
				})
				.collect(),
		);
		Ok(RuntimeMeanings {
			panels,
			groups,
			colours,
			next_view_panel_id: self.next_view_panel_id,
			next_view_group_id: self.next_view_group_id,
		})
	}
}

fn ensure_supported_schema(
	schema_version: u32,
	maximum_supported: u32,
) -> Result<(), SessionPersistenceError> {
	if !(INITIAL_CLOSED_SCHEMA..=maximum_supported).contains(&schema_version) {
		return Err(SessionPersistenceError::refused(format!(
			"unsupported schema version {schema_version}"
		)));
	}
	Ok(())
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FamilyAdmissionPolicy {
	InitialClosedFamilies,
	AreaClosedFamilies,
	ExtensibleFamilies,
}

impl FamilyAdmissionPolicy {
	fn for_schema(schema_version: u32) -> Result<Self, SessionPersistenceError> {
		match schema_version {
			INITIAL_CLOSED_SCHEMA => Ok(Self::InitialClosedFamilies),
			AREA_CLOSED_SCHEMA => Ok(Self::AreaClosedFamilies),
			3..=SCHEMA_VERSION => Ok(Self::ExtensibleFamilies),
			other => Err(SessionPersistenceError::refused(format!(
				"unsupported schema version {other}"
			))),
		}
	}

	fn admits(self, family: &AuthoredFamilyId) -> bool {
		match self {
			Self::InitialClosedFamilies => matches!(family.as_str(), "scatter" | "bar" | "line"),
			Self::AreaClosedFamilies => {
				matches!(family.as_str(), "scatter" | "bar" | "line" | "area")
			}
			Self::ExtensibleFamilies => true,
		}
	}
}

/// Project process-local producer keys onto durable documents. Shared panel
/// and group meanings are census-filtered; colour allocation is filtered only
/// by durable producer identity because it owns no shared token.
pub(crate) fn capture_runtime(
	panels: &HashMap<(BufferId, LocalViewIdentity), HostPanelId>,
	groups: &HashMap<BufferId, HostPanelGroupId>,
	colours: &RuntimeColours,
	buffer_documents: &HashMap<BufferId, DocumentId>,
	census: &SharedIdentityCensus,
	next_view_panel_id: u64,
	next_view_group_id: u64,
) -> Result<DurableMeanings, SessionPersistenceError> {
	let panels = panels
		.iter()
		.filter_map(|((buffer, view), panel)| {
			let document = buffer_documents.get(buffer)?;
			census
				.host_panels
				.contains(panel)
				.then_some(((*document, view.clone()), *panel))
		})
		.collect();
	let groups = groups
		.iter()
		.filter_map(|(buffer, group)| {
			let document = buffer_documents.get(buffer)?;
			census
				.host_panel_groups
				.contains(group)
				.then_some((*document, *group))
		})
		.collect();
	let colours = colours
		.as_map()
		.iter()
		.filter_map(|((buffer, view, len), identity)| {
			buffer_documents
				.get(buffer)
				.map(|document| ((*document, view.clone(), *len), identity.clone()))
		})
		.collect();
	let meanings = DurableMeanings {
		panels,
		groups,
		colours,
		next_view_panel_id,
		next_view_group_id,
	};
	validate_capture_membership(&meanings, census)?;
	validate_marks(&meanings)?;
	Ok(meanings)
}

pub(crate) fn validate_capture_membership(
	meanings: &DurableMeanings,
	census: &SharedIdentityCensus,
) -> Result<(), SessionPersistenceError> {
	let documents = meanings
		.panels
		.keys()
		.map(|(document, _)| *document)
		.chain(meanings.groups.keys().copied())
		.chain(meanings.colours.keys().map(|(document, _, _)| *document));
	if !documents
		.into_iter()
		.all(|document| census.documents.contains(&document))
	{
		return Err(SessionPersistenceError::refused(
			"encoded document is absent from the checkpoint witness",
		));
	}
	if !meanings
		.panels
		.values()
		.all(|panel| census.host_panels.contains(panel))
	{
		return Err(SessionPersistenceError::refused(
			"encoded panel is absent from the checkpoint witness",
		));
	}
	if !meanings
		.groups
		.values()
		.all(|group| census.host_panel_groups.contains(group))
	{
		return Err(SessionPersistenceError::refused(
			"encoded panel group is absent from the checkpoint witness",
		));
	}
	Ok(())
}

fn validate_marks(meanings: &DurableMeanings) -> Result<(), SessionPersistenceError> {
	let panel_high = meanings.panels.values().map(|id| id.get()).max();
	if meanings.next_view_panel_id < DYNAMIC_PANEL_FLOOR {
		return Err(SessionPersistenceError::refused(
			"next panel identity is below the dynamic namespace floor",
		));
	}
	if panel_high.is_some_and(|high| meanings.next_view_panel_id <= high) {
		return Err(SessionPersistenceError::refused(
			"next panel identity does not dominate encoded panels",
		));
	}
	if meanings.next_view_panel_id == u64::MAX {
		return Err(SessionPersistenceError::refused(
			"dynamic panel identity namespace is exhausted",
		));
	}
	let group_high = meanings.groups.values().map(|id| id.get()).max();
	if meanings.next_view_group_id < GROUP_FLOOR {
		return Err(SessionPersistenceError::refused(
			"next panel-group identity is below its floor",
		));
	}
	if group_high.is_some_and(|high| meanings.next_view_group_id <= high) {
		return Err(SessionPersistenceError::refused(
			"next panel-group identity does not dominate encoded groups",
		));
	}
	if meanings.next_view_group_id == u64::MAX {
		return Err(SessionPersistenceError::refused(
			"panel-group identity namespace is exhausted",
		));
	}
	Ok(())
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Payload {
	next_view_panel_id: u64,
	next_view_group_id: u64,
	panels: Vec<PanelMeaning>,
	groups: Vec<GroupMeaning>,
	colours: Vec<ColourMeaning>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct PanelMeaning {
	document: u64,
	view: ViewIdentity,
	panel: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct GroupMeaning {
	document: u64,
	group: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColourMeaning {
	document: u64,
	view: ViewIdentity,
	palette_len: u64,
	identity: ColourIdentityDto,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum ViewIdentity {
	Named { name: String },
	Anonymous { ordinal: u64 },
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColourIdentityDto {
	assignments: Vec<ColourAssignment>,
	spaces: Vec<ColourSpace>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColourAssignment {
	field: String,
	layer: DurableLayer,
	category: String,
	position: u64,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct ColourSpace {
	field: String,
	layer: DurableLayer,
	space: AllocationSpace,
}

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct DurableLayer {
	ordinal: u64,
	family: String,
}

#[derive(Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
enum AllocationSpace {
	Legacy { len: u64 },
	CategoricalPalette { len: u64 },
}

impl Payload {
	fn from_meanings(meanings: &DurableMeanings) -> Result<Self, SessionPersistenceError> {
		let mut panels = meanings
			.panels
			.iter()
			.map(|((document, view), panel)| PanelMeaning {
				document: document.get(),
				view: ViewIdentity::from_runtime(view),
				panel: panel.get(),
			})
			.collect::<Vec<_>>();
		panels.sort_by(|left, right| {
			left.document
				.cmp(&right.document)
				.then_with(|| left.view.cmp_for_sort(&right.view))
				.then_with(|| left.panel.cmp(&right.panel))
		});
		let mut groups = meanings
			.groups
			.iter()
			.map(|(document, group)| GroupMeaning {
				document: document.get(),
				group: group.get(),
			})
			.collect::<Vec<_>>();
		groups.sort_by_key(|entry| (entry.document, entry.group));
		let mut colours = meanings
			.colours
			.iter()
			.map(|((document, view, palette_len), identity)| {
				Ok(ColourMeaning {
					document: document.get(),
					view: ViewIdentity::from_runtime(view),
					palette_len: to_u64(*palette_len, "palette length")?,
					identity: ColourIdentityDto::from_runtime(identity)?,
				})
			})
			.collect::<Result<Vec<_>, SessionPersistenceError>>()?;
		colours.sort_by(|left, right| {
			left.document
				.cmp(&right.document)
				.then_with(|| left.view.cmp_for_sort(&right.view))
				.then_with(|| left.palette_len.cmp(&right.palette_len))
		});
		Ok(Self {
			next_view_panel_id: meanings.next_view_panel_id,
			next_view_group_id: meanings.next_view_group_id,
			panels,
			groups,
			colours,
		})
	}

	fn into_meanings(
		self,
		family_policy: FamilyAdmissionPolicy,
	) -> Result<DurableMeanings, SessionPersistenceError> {
		let mut panels = HashMap::new();
		for entry in self.panels {
			if entry.panel == 0 {
				return Err(SessionPersistenceError::refused("panel identity is zero"));
			}
			let key = (document(entry.document)?, entry.view.into_runtime()?);
			if panels.insert(key, HostPanelId::new(entry.panel)).is_some() {
				return Err(SessionPersistenceError::refused("duplicate panel meaning"));
			}
		}
		let mut groups = HashMap::new();
		for entry in self.groups {
			if entry.group == 0 {
				return Err(SessionPersistenceError::refused(
					"panel-group identity is zero",
				));
			}
			if groups
				.insert(
					document(entry.document)?,
					HostPanelGroupId::new(entry.group),
				)
				.is_some()
			{
				return Err(SessionPersistenceError::refused("duplicate group meaning"));
			}
		}
		let mut colours = HashMap::new();
		for entry in self.colours {
			let palette_len = to_usize(entry.palette_len, "palette length")?;
			if palette_len == 0 {
				return Err(SessionPersistenceError::refused("palette length is zero"));
			}
			let key = (
				document(entry.document)?,
				entry.view.into_runtime()?,
				palette_len,
			);
			let identity = entry.identity.into_runtime(family_policy)?;
			if identity
				.persistence_snapshot()
				.spaces
				.iter()
				.any(|space| match space.space {
					AllocationSpaceSnapshot::Legacy { len }
					| AllocationSpaceSnapshot::CategoricalPalette { len } => len != palette_len,
				}) {
				return Err(SessionPersistenceError::refused(
					"colour allocation length disagrees with its palette key",
				));
			}
			if colours.insert(key, identity).is_some() {
				return Err(SessionPersistenceError::refused("duplicate colour meaning"));
			}
		}
		Ok(DurableMeanings {
			panels,
			groups,
			colours,
			next_view_panel_id: self.next_view_panel_id,
			next_view_group_id: self.next_view_group_id,
		})
	}
}

impl ViewIdentity {
	fn from_runtime(value: &LocalViewIdentity) -> Self {
		match value {
			LocalViewIdentity::Named(name) => Self::Named { name: name.clone() },
			LocalViewIdentity::Anonymous(ordinal) => Self::Anonymous {
				ordinal: *ordinal as u64,
			},
		}
	}

	fn into_runtime(self) -> Result<LocalViewIdentity, SessionPersistenceError> {
		Ok(match self {
			Self::Named { name } => LocalViewIdentity::Named(name),
			Self::Anonymous { ordinal } => {
				LocalViewIdentity::Anonymous(to_usize(ordinal, "view ordinal")?)
			}
		})
	}

	fn cmp_for_sort(&self, other: &Self) -> std::cmp::Ordering {
		match (self, other) {
			(Self::Named { name: left }, Self::Named { name: right }) => left.cmp(right),
			(Self::Named { .. }, Self::Anonymous { .. }) => std::cmp::Ordering::Less,
			(Self::Anonymous { .. }, Self::Named { .. }) => std::cmp::Ordering::Greater,
			(Self::Anonymous { ordinal: left }, Self::Anonymous { ordinal: right }) => {
				left.cmp(right)
			}
		}
	}
}

impl ColourIdentityDto {
	fn from_runtime(identity: &ColorIdentity) -> Result<Self, SessionPersistenceError> {
		let snapshot = identity.persistence_snapshot();
		Ok(Self {
			assignments: snapshot
				.assignments
				.into_iter()
				.map(|entry| {
					Ok(ColourAssignment {
						field: entry.field,
						layer: DurableLayer::from_runtime(entry.layer)?,
						category: entry.category,
						position: to_u64(entry.position, "palette position")?,
					})
				})
				.collect::<Result<_, SessionPersistenceError>>()?,
			spaces: snapshot
				.spaces
				.into_iter()
				.map(|entry| {
					Ok(ColourSpace {
						field: entry.field,
						layer: DurableLayer::from_runtime(entry.layer)?,
						space: match entry.space {
							AllocationSpaceSnapshot::Legacy { len } => AllocationSpace::Legacy {
								len: to_u64(len, "allocation length")?,
							},
							AllocationSpaceSnapshot::CategoricalPalette { len } => {
								AllocationSpace::CategoricalPalette {
									len: to_u64(len, "allocation length")?,
								}
							}
						},
					})
				})
				.collect::<Result<_, SessionPersistenceError>>()?,
		})
	}

	fn into_runtime(
		self,
		family_policy: FamilyAdmissionPolicy,
	) -> Result<ColorIdentity, SessionPersistenceError> {
		let assignments = self
			.assignments
			.into_iter()
			.map(|entry| {
				Ok(ColorAssignmentSnapshot {
					field: entry.field,
					layer: entry.layer.into_runtime(family_policy)?,
					category: entry.category,
					position: to_usize(entry.position, "palette position")?,
				})
			})
			.collect::<Result<_, SessionPersistenceError>>()?;
		let spaces = self
			.spaces
			.into_iter()
			.map(|entry| {
				Ok(ColorSpaceSnapshot {
					field: entry.field,
					layer: entry.layer.into_runtime(family_policy)?,
					space: match entry.space {
						AllocationSpace::Legacy { len } => AllocationSpaceSnapshot::Legacy {
							len: to_usize(len, "allocation length")?,
						},
						AllocationSpace::CategoricalPalette { len } => {
							AllocationSpaceSnapshot::CategoricalPalette {
								len: to_usize(len, "allocation length")?,
							}
						}
					},
				})
			})
			.collect::<Result<_, SessionPersistenceError>>()?;
		ColorIdentity::from_persistence_snapshot(ColorIdentitySnapshot {
			assignments,
			spaces,
		})
		.map_err(SessionPersistenceError::refused)
	}
}

impl DurableLayer {
	fn from_runtime(layer: ColorLayerIdentity) -> Result<Self, SessionPersistenceError> {
		Ok(Self {
			ordinal: to_u64(layer.ordinal, "layer ordinal")?,
			family: layer.family.as_str().to_owned(),
		})
	}

	fn into_runtime(
		self,
		family_policy: FamilyAdmissionPolicy,
	) -> Result<ColorLayerIdentity, SessionPersistenceError> {
		let family =
			AuthoredFamilyId::parse(self.family).map_err(SessionPersistenceError::refused)?;
		if !family_policy.admits(&family) {
			return Err(SessionPersistenceError::refused(format!(
				"authored family {} is unsupported by this schema's family policy",
				family.as_str()
			)));
		}
		Ok(ColorLayerIdentity {
			ordinal: to_usize(self.ordinal, "layer ordinal")?,
			family,
		})
	}
}

fn document(value: u64) -> Result<DocumentId, SessionPersistenceError> {
	DocumentId::new(value).map_err(|error| SessionPersistenceError::refused(error.to_string()))
}

fn to_usize(value: u64, label: &str) -> Result<usize, SessionPersistenceError> {
	usize::try_from(value)
		.map_err(|_| SessionPersistenceError::refused(format!("{label} does not fit usize")))
}

fn to_u64(value: usize, label: &str) -> Result<u64, SessionPersistenceError> {
	u64::try_from(value)
		.map_err(|_| SessionPersistenceError::refused(format!("{label} does not fit u64")))
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::core::PlotType;
	use crate::plot::spec::{AllocationSpaceSnapshot, ColorIdentitySnapshot, LayerIdentity};

	fn empty_meanings() -> DurableMeanings {
		DurableMeanings {
			panels: HashMap::new(),
			groups: HashMap::new(),
			colours: HashMap::new(),
			next_view_panel_id: DYNAMIC_PANEL_FLOOR,
			next_view_group_id: GROUP_FLOOR,
		}
	}

	fn colour_identity(plot_type: PlotType, legacy: bool) -> ColorIdentity {
		let layer = ColorLayerIdentity::from_authored_layer(&LayerIdentity {
			ordinal: usize::from(legacy),
			plot_type,
		});
		let space = if legacy {
			AllocationSpaceSnapshot::Legacy { len: 4 }
		} else {
			AllocationSpaceSnapshot::CategoricalPalette { len: 4 }
		};
		ColorIdentity::from_persistence_snapshot(ColorIdentitySnapshot {
			assignments: vec![ColorAssignmentSnapshot {
				field: "product".into(),
				layer: layer.clone(),
				category: "Atlas".into(),
				position: 2,
			}],
			spaces: vec![ColorSpaceSnapshot {
				field: "product".into(),
				layer,
				space,
			}],
		})
		.unwrap()
	}

	fn colour_identity_for_family(family: AuthoredFamilyId) -> ColorIdentity {
		let layer = ColorLayerIdentity { ordinal: 0, family };
		ColorIdentity::from_persistence_snapshot(ColorIdentitySnapshot {
			assignments: vec![ColorAssignmentSnapshot {
				field: "series".into(),
				layer: layer.clone(),
				category: "Atlas".into(),
				position: 2,
			}],
			spaces: vec![ColorSpaceSnapshot {
				field: "series".into(),
				layer,
				space: AllocationSpaceSnapshot::CategoricalPalette { len: 4 },
			}],
		})
		.unwrap()
	}

	#[test]
	fn product_0062_codec_is_strict_bounded_and_deterministic() {
		assert_eq!(MAX_OWNER_BYTES, 1_048_576);
		let document = DocumentId::new(7).unwrap();
		let mut meanings = empty_meanings();
		meanings.panels.insert(
			(document, LocalViewIdentity::Named("bars".into())),
			HostPanelId::new(DYNAMIC_PANEL_FLOOR),
		);
		meanings.panels.insert(
			(document, LocalViewIdentity::Anonymous(3)),
			HostPanelId::new(DYNAMIC_PANEL_FLOOR + 1),
		);
		meanings.groups.insert(document, HostPanelGroupId::new(4));
		for (index, (family, legacy)) in [
			(PlotType::Scatter, false),
			(PlotType::Bar, true),
			(PlotType::Line, false),
		]
		.into_iter()
		.enumerate()
		{
			meanings.colours.insert(
				(document, LocalViewIdentity::Anonymous(index), 4),
				colour_identity(family, legacy),
			);
		}
		meanings.next_view_panel_id = DYNAMIC_PANEL_FLOOR + 2;
		meanings.next_view_group_id = 5;
		let bytes = meanings.encode().unwrap();
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);
		assert_eq!(
			DurableMeanings::decode(INITIAL_CLOSED_SCHEMA, &bytes).unwrap(),
			meanings,
			"the initial closed-family policy remains readable for its three families"
		);
		assert_eq!(meanings.encode().unwrap(), bytes);
		assert!(DurableMeanings::decode(4, &bytes).is_err());
		assert!(DurableMeanings::decode_with_bound(1, &bytes, 4).is_err());
		let unknown = br#"{"next_view_panel_id":9223372036854775808,"next_view_group_id":1,"panels":[],"groups":[],"colours":[],"placement":0}"#;
		assert!(DurableMeanings::decode(1, unknown).is_err());
		let trailing = [bytes.as_slice(), b" true"].concat();
		assert!(DurableMeanings::decode(1, &trailing).is_err());
		let duplicate = br#"{"next_view_panel_id":9223372036854775808,"next_view_panel_id":9223372036854775809,"next_view_group_id":1,"panels":[],"groups":[],"colours":[]}"#;
		assert!(DurableMeanings::decode(1, duplicate).is_err());
		let zero = br#"{"next_view_panel_id":9223372036854775809,"next_view_group_id":1,"panels":[{"document":0,"view":{"kind":"named","name":"x"},"panel":9223372036854775808}],"groups":[],"colours":[]}"#;
		assert!(DurableMeanings::decode(1, zero).is_err());
	}

	#[test]
	fn product_0070_area_closed_policy_adds_area_without_weakening_initial_policy() {
		assert_eq!(
			SCHEMA_VERSION, 3,
			"writers publish the extensible-family owner grammar"
		);
		let document = DocumentId::new(1).unwrap();
		let mut meanings = empty_meanings();
		meanings.colours.insert(
			(document, LocalViewIdentity::Named("area".into()), 4),
			colour_identity(PlotType::StackedArea, false),
		);
		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert!(text.contains("\"family\":\"area\""), "{text}");
		assert_eq!(DurableMeanings::decode(2, &bytes).unwrap(), meanings);
		let relabelled = DurableMeanings::decode(1, &bytes).unwrap_err();
		assert!(
			relabelled
				.to_string()
				.contains("authored family area is unsupported by this schema's family policy"),
			"{relabelled}"
		);

		let old_reader =
			ensure_supported_schema(AREA_CLOSED_SCHEMA, INITIAL_CLOSED_SCHEMA).unwrap_err();
		assert!(
			old_reader
				.to_string()
				.contains("unsupported schema version 2"),
			"the initial closed-family reader must refuse the area-era envelope rather than dropping area meanings: {old_reader}"
		);

		let mut initial = empty_meanings();
		initial.colours.insert(
			(document, LocalViewIdentity::Named("line".into()), 4),
			colour_identity(PlotType::Line, false),
		);
		let initial_bytes = initial.encode().unwrap();
		assert_eq!(DurableMeanings::decode(1, &initial_bytes).unwrap(), initial);
		assert_eq!(DurableMeanings::decode(2, &initial_bytes).unwrap(), initial);
	}

	#[test]
	fn product_0086_extensible_policy_preserves_unknown_family_through_runtime_capture() {
		let document = DocumentId::new(86).unwrap();
		let buffer = BufferId::new(8);
		let view = LocalViewIdentity::Named("future".into());
		let family = AuthoredFamilyId::parse("future_family".into()).unwrap();
		assert!(!AuthoredFamilyId::ALL.contains(&family));
		let identity = colour_identity_for_family(family);
		let mut meanings = empty_meanings();
		meanings
			.colours
			.insert((document, view.clone(), 4), identity.clone());
		let bytes = meanings.encode().unwrap();
		assert!(DurableMeanings::decode(1, &bytes).is_err());
		assert!(DurableMeanings::decode(2, &bytes).is_err());
		assert_eq!(DurableMeanings::decode(3, &bytes).unwrap(), meanings);

		let census = SharedIdentityCensus {
			documents: [document].into_iter().collect(),
			host_panels: BTreeSet::new(),
			host_panel_groups: BTreeSet::new(),
		};
		let runtime = meanings
			.join_runtime(&[(document, buffer)].into_iter().collect(), &census)
			.unwrap();
		assert_eq!(
			runtime.colours.as_map().get(&(buffer, view.clone(), 4)),
			Some(&identity)
		);
		let recaptured = capture_runtime(
			&HashMap::new(),
			&HashMap::new(),
			&runtime.colours,
			&[(buffer, document)].into_iter().collect(),
			&census,
			DYNAMIC_PANEL_FLOOR,
			GROUP_FLOOR,
		)
		.unwrap();
		let reencoded = recaptured.encode().unwrap();
		assert_eq!(DurableMeanings::decode(3, &reencoded).unwrap(), meanings);
		assert!(
			String::from_utf8(reencoded)
				.unwrap()
				.contains("\"family\":\"future_family\"")
		);

		let dropped = capture_runtime(
			&HashMap::new(),
			&HashMap::new(),
			&runtime.colours,
			&HashMap::new(),
			&SharedIdentityCensus::default(),
			DYNAMIC_PANEL_FLOOR,
			GROUP_FLOOR,
		)
		.unwrap();
		assert!(dropped.colours.is_empty());
	}

	#[test]
	fn product_0098_funnel_family_round_trips_only_under_the_extensible_policy() {
		let document = DocumentId::new(98).unwrap();
		let buffer = BufferId::new(98);
		let view = LocalViewIdentity::Named("funnel".into());
		let identity = colour_identity(PlotType::Funnel, false);
		let mut meanings = empty_meanings();
		meanings
			.colours
			.insert((document, view.clone(), 4), identity.clone());
		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert!(text.contains("\"family\":\"funnel\""), "{text}");
		for schema in [INITIAL_CLOSED_SCHEMA, AREA_CLOSED_SCHEMA] {
			let error = DurableMeanings::decode(schema, &bytes).unwrap_err();
			assert!(
				error.to_string().contains(
					"authored family funnel is unsupported by this schema's family policy"
				),
				"{error}"
			);
		}
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);

		let census = SharedIdentityCensus {
			documents: [document].into_iter().collect(),
			host_panels: BTreeSet::new(),
			host_panel_groups: BTreeSet::new(),
		};
		let runtime = meanings
			.join_runtime(&[(document, buffer)].into_iter().collect(), &census)
			.unwrap();
		assert_eq!(
			runtime.colours.as_map().get(&(buffer, view.clone(), 4)),
			Some(&identity)
		);
		let recaptured = capture_runtime(
			&HashMap::new(),
			&HashMap::new(),
			&runtime.colours,
			&[(buffer, document)].into_iter().collect(),
			&census,
			DYNAMIC_PANEL_FLOOR,
			GROUP_FLOOR,
		)
		.unwrap();
		assert_eq!(recaptured, meanings);
	}

	#[test]
	fn product_0100_radial_dial_family_round_trips_only_under_the_extensible_policy() {
		let document = DocumentId::new(100).unwrap();
		let buffer = BufferId::new(100);
		let view = LocalViewIdentity::Named("radial-dial".into());
		let identity = colour_identity(PlotType::RadialDial, false);
		let mut meanings = empty_meanings();
		meanings
			.colours
			.insert((document, view.clone(), 4), identity.clone());
		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert!(text.contains("\"family\":\"radial_dial\""), "{text}");
		for schema in [INITIAL_CLOSED_SCHEMA, AREA_CLOSED_SCHEMA] {
			let error = DurableMeanings::decode(schema, &bytes).unwrap_err();
			assert!(
				error.to_string().contains(
					"authored family radial_dial is unsupported by this schema's family policy"
				),
				"{error}"
			);
		}
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);

		let census = SharedIdentityCensus {
			documents: [document].into_iter().collect(),
			host_panels: BTreeSet::new(),
			host_panel_groups: BTreeSet::new(),
		};
		let runtime = meanings
			.join_runtime(&[(document, buffer)].into_iter().collect(), &census)
			.unwrap();
		assert_eq!(
			runtime.colours.as_map().get(&(buffer, view.clone(), 4)),
			Some(&identity)
		);
		let recaptured = capture_runtime(
			&HashMap::new(),
			&HashMap::new(),
			&runtime.colours,
			&[(buffer, document)].into_iter().collect(),
			&census,
			DYNAMIC_PANEL_FLOOR,
			GROUP_FLOOR,
		)
		.unwrap();
		assert_eq!(recaptured, meanings);
	}

	#[test]
	fn product_0100_existing_session_without_radial_dial_is_byte_identical() {
		let meanings = empty_meanings();
		assert_eq!(
			meanings.encode().unwrap(),
			br#"{"next_view_panel_id":9223372036854775808,"next_view_group_id":1,"panels":[],"groups":[],"colours":[]}"#
		);
	}

	#[test]
	fn product_0086_family_admission_policies_and_token_validation_are_exact() {
		let document = DocumentId::new(86).unwrap();
		let mut meanings = empty_meanings();
		meanings.colours.insert(
			(document, LocalViewIdentity::Named("area".into()), 4),
			colour_identity(PlotType::StackedArea, false),
		);
		let area = meanings.encode().unwrap();
		assert!(DurableMeanings::decode(1, &area).is_err());
		assert!(DurableMeanings::decode(2, &area).is_ok());
		assert!(DurableMeanings::decode(3, &area).is_ok());

		let future = String::from_utf8(area.clone())
			.unwrap()
			.replace("\"area\"", "\"future_family\"");
		assert!(DurableMeanings::decode(1, future.as_bytes()).is_err());
		assert!(DurableMeanings::decode(2, future.as_bytes()).is_err());
		assert!(DurableMeanings::decode(3, future.as_bytes()).is_ok());
		for invalid in [
			"",
			"Area",
			"9area",
			"two words",
			"two-words",
			"two.words",
			"é",
		] {
			let malformed = String::from_utf8(area.clone())
				.unwrap()
				.replace("\"area\"", &format!("\"{invalid}\""));
			assert!(
				DurableMeanings::decode(3, malformed.as_bytes()).is_err(),
				"{invalid}"
			);
		}
		for valid in ["a".to_owned(), "a".repeat(64)] {
			let accepted = String::from_utf8(area.clone())
				.unwrap()
				.replace("\"area\"", &format!("\"{valid}\""));
			assert!(
				DurableMeanings::decode(3, accepted.as_bytes()).is_ok(),
				"{valid}"
			);
		}
		let oversized = String::from_utf8(area)
			.unwrap()
			.replace("\"area\"", &format!("\"{}\"", "a".repeat(65)));
		assert!(DurableMeanings::decode(3, oversized.as_bytes()).is_err());
		assert!(ensure_supported_schema(3, 2).is_err());
	}

	#[test]
	fn product_0075_plain_anonymous_zero_uses_the_existing_owner_vocabulary() {
		let document = DocumentId::new(75).unwrap();
		let mut meanings = empty_meanings();
		meanings.panels.insert(
			(document, LocalViewIdentity::Anonymous(0)),
			HostPanelId::new(DYNAMIC_PANEL_FLOOR),
		);
		meanings
			.groups
			.insert(document, HostPanelGroupId::new(GROUP_FLOOR));
		meanings.next_view_panel_id = DYNAMIC_PANEL_FLOOR + 1;
		meanings.next_view_group_id = GROUP_FLOOR + 1;

		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert!(text.contains("\"kind\":\"anonymous\""), "{text}");
		assert!(text.contains("\"ordinal\":0"), "{text}");
		assert!(
			!text.contains("plain"),
			"no plain-only durable identity: {text}"
		);
		assert!(
			!text.contains("dashboard"),
			"no retired role enters meanings: {text}"
		);
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);
		assert_eq!(
			SCHEMA_VERSION, 3,
			"PRODUCT-0075 changes population, not grammar"
		);
	}

	#[test]
	fn product_0079_histogram_panel_has_owner_meaning_and_no_colour_footprint() {
		let document = DocumentId::new(79).unwrap();
		let view = LocalViewIdentity::Named("histogram".into());
		let panel = HostPanelId::new(DYNAMIC_PANEL_FLOOR);
		let mut meanings = empty_meanings();
		meanings.panels.insert((document, view), panel);
		meanings
			.groups
			.insert(document, HostPanelGroupId::new(GROUP_FLOOR));
		meanings.next_view_panel_id = panel.get() + 1;
		meanings.next_view_group_id = GROUP_FLOOR + 1;

		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert_eq!(SCHEMA_VERSION, 3);
		assert!(
			text.contains("\"name\":\"histogram\"")
				&& text.contains(&format!("\"panel\":{}", panel.get())),
			"the semantic panel mapping is durable: {text}"
		);
		assert!(text.contains("\"colours\":[]"), "{text}");
		assert!(
			!text.contains("plot_type")
				&& !text.contains("authored_family")
				&& !text.contains("\"family\":"),
			"the owner payload carries no histogram or authored-family vocabulary: {text}"
		);
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);
	}

	#[test]
	fn product_0082_box_panel_has_owner_meaning_and_no_colour_footprint() {
		let document = DocumentId::new(82).unwrap();
		let view = LocalViewIdentity::Named("box".into());
		let panel = HostPanelId::new(DYNAMIC_PANEL_FLOOR);
		let mut meanings = empty_meanings();
		meanings.panels.insert((document, view), panel);
		meanings
			.groups
			.insert(document, HostPanelGroupId::new(GROUP_FLOOR));
		meanings.next_view_panel_id = panel.get() + 1;
		meanings.next_view_group_id = GROUP_FLOOR + 1;

		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert_eq!(SCHEMA_VERSION, 3);
		assert!(
			text.contains("\"name\":\"box\"")
				&& text.contains(&format!("\"panel\":{}", panel.get()))
		);
		assert!(text.contains("\"colours\":[]"), "{text}");
		assert!(
			!text.contains("plot_type")
				&& !text.contains("authored_family")
				&& !text.contains("\"family\":")
		);
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);
	}

	#[test]
	fn product_0092_heatmap_panel_has_owner_meaning_and_no_colour_footprint() {
		let document = DocumentId::new(92).unwrap();
		let view = LocalViewIdentity::Named("heatmap".into());
		let panel = HostPanelId::new(DYNAMIC_PANEL_FLOOR);
		let mut meanings = empty_meanings();
		meanings.panels.insert((document, view), panel);
		meanings
			.groups
			.insert(document, HostPanelGroupId::new(GROUP_FLOOR));
		meanings.next_view_panel_id = panel.get() + 1;
		meanings.next_view_group_id = GROUP_FLOOR + 1;

		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert_eq!(SCHEMA_VERSION, 3);
		assert!(
			text.contains("\"name\":\"heatmap\"")
				&& text.contains(&format!("\"panel\":{}", panel.get()))
		);
		assert!(text.contains("\"colours\":[]"), "{text}");
		assert!(
			!text.contains("plot_type")
				&& !text.contains("authored_family")
				&& !text.contains("\"family\":")
		);
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);
	}

	#[test]
	fn product_0095_bubble_panel_has_owner_meaning_and_no_colour_footprint() {
		let document = DocumentId::new(95).unwrap();
		let view = LocalViewIdentity::Named("bubble".into());
		let panel = HostPanelId::new(DYNAMIC_PANEL_FLOOR);
		let mut meanings = empty_meanings();
		meanings.panels.insert((document, view), panel);
		meanings
			.groups
			.insert(document, HostPanelGroupId::new(GROUP_FLOOR));
		meanings.next_view_panel_id = panel.get() + 1;
		meanings.next_view_group_id = GROUP_FLOOR + 1;

		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert_eq!(SCHEMA_VERSION, 3);
		assert!(
			text.contains("\"name\":\"bubble\"")
				&& text.contains(&format!("\"panel\":{}", panel.get()))
		);
		assert!(text.contains("\"colours\":[]"), "{text}");
		assert!(
			!text.contains("plot_type")
				&& !text.contains("authored_family")
				&& !text.contains("\"family\":")
		);
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);
	}

	#[test]
	fn product_0085_axis_title_stays_in_source_and_not_owner_meanings() {
		let document = DocumentId::new(85).unwrap();
		let view = LocalViewIdentity::Named("axis-titled-box".into());
		let panel = HostPanelId::new(DYNAMIC_PANEL_FLOOR);
		let mut meanings = empty_meanings();
		meanings.panels.insert((document, view), panel);
		meanings
			.groups
			.insert(document, HostPanelGroupId::new(GROUP_FLOOR));
		meanings.next_view_panel_id = panel.get() + 1;
		meanings.next_view_group_id = GROUP_FLOOR + 1;

		let bytes = meanings.encode().unwrap();
		let text = String::from_utf8(bytes.clone()).unwrap();
		assert_eq!(SCHEMA_VERSION, 3);
		assert!(
			!text.contains("x_label") && !text.contains("y_label"),
			"{text}"
		);
		assert_eq!(
			DurableMeanings::decode(SCHEMA_VERSION, &bytes).unwrap(),
			meanings
		);
	}

	#[test]
	fn product_0062_colour_family_is_authored_and_rotation_independent() {
		let document = DocumentId::new(1).unwrap();
		let mut meanings = empty_meanings();
		meanings.colours.insert(
			(document, LocalViewIdentity::Named("vertical".into()), 4),
			colour_identity(PlotType::Bar, false),
		);
		meanings.colours.insert(
			(document, LocalViewIdentity::Named("horizontal".into()), 4),
			colour_identity(PlotType::HorizontalBar, false),
		);
		let text = String::from_utf8(meanings.encode().unwrap()).unwrap();
		assert_eq!(text.matches("\"family\":\"bar\"").count(), 4);
		assert!(!text.contains("horizontalbar"));
		for bad in ["horizontalbar", "stackedbar", "unknown"] {
			let invalid = text.replacen("\"bar\"", &format!("\"{bad}\""), 1);
			assert!(DurableMeanings::decode(1, invalid.as_bytes()).is_err());
		}
	}

	#[test]
	fn product_0062_palette_positions_are_revalidated_against_their_space() {
		let bytes = br#"{"next_view_panel_id":9223372036854775808,"next_view_group_id":1,"panels":[],"groups":[],"colours":[{"document":1,"view":{"kind":"named","name":"v"},"palette_len":4,"identity":{"assignments":[{"field":"f","layer":{"ordinal":0,"family":"line"},"category":"A","position":4}],"spaces":[{"field":"f","layer":{"ordinal":0,"family":"line"},"space":{"kind":"categorical_palette","len":4}}]}}]}"#;
		assert!(DurableMeanings::decode(1, bytes).is_err());
	}

	#[test]
	fn product_0062_rekeying_uses_document_identity_not_open_order_or_path() {
		let first = DocumentId::new(10).unwrap();
		let second = DocumentId::new(20).unwrap();
		let view = LocalViewIdentity::Named("trend".into());
		let mut meanings = empty_meanings();
		meanings
			.panels
			.insert((first, view.clone()), HostPanelId::new(DYNAMIC_PANEL_FLOOR));
		meanings.next_view_panel_id += 1;
		let census = SharedIdentityCensus {
			documents: [first, second].into_iter().collect(),
			host_panels: [HostPanelId::new(DYNAMIC_PANEL_FLOOR)]
				.into_iter()
				.collect(),
			host_panel_groups: BTreeSet::new(),
		};
		let reopened = [(second, BufferId::new(1)), (first, BufferId::new(2))]
			.into_iter()
			.collect();
		let runtime = meanings.join_runtime(&reopened, &census).unwrap();
		assert_eq!(
			runtime.panels.get(&(BufferId::new(2), view)),
			Some(&HostPanelId::new(DYNAMIC_PANEL_FLOOR))
		);
		let alien = [(DocumentId::new(30).unwrap(), BufferId::new(2))]
			.into_iter()
			.collect();
		assert!(meanings.join_runtime(&alien, &census).is_err());
	}

	#[test]
	fn product_0062_allocator_marks_are_structural_state() {
		let document = DocumentId::new(1).unwrap();
		let mut meanings = empty_meanings();
		meanings.panels.insert(
			(document, LocalViewIdentity::Named("bars".into())),
			HostPanelId::new(DYNAMIC_PANEL_FLOOR + 7),
		);
		meanings.next_view_panel_id = DYNAMIC_PANEL_FLOOR + 7;
		assert!(meanings.encode().is_err());
		meanings.next_view_panel_id += 1;
		assert!(meanings.encode().is_ok());
		meanings.next_view_panel_id = u64::MAX;
		assert!(meanings.encode().is_err());
	}

	#[test]
	fn product_0062_capture_filters_shared_identity_not_colour_lifetime() {
		let buffer = BufferId::new(9);
		let document = DocumentId::new(4).unwrap();
		let view = LocalViewIdentity::Named("closed".into());
		let closed = HostPanelId::new(DYNAMIC_PANEL_FLOOR);
		let stale = HostPanelId::new(DYNAMIC_PANEL_FLOOR + 1);
		let panels = [
			((buffer, view.clone()), closed),
			((buffer, LocalViewIdentity::Named("stale".into())), stale),
		]
		.into_iter()
		.collect();
		let colours = [((buffer, view.clone(), 4), ColorIdentity::default())]
			.into_iter()
			.collect();
		let bindings = [(buffer, document)].into_iter().collect();
		let census = SharedIdentityCensus {
			documents: [document].into_iter().collect(),
			host_panels: [stale].into_iter().collect(),
			host_panel_groups: BTreeSet::new(),
		};
		let captured = capture_runtime(
			&panels,
			&HashMap::new(),
			&RuntimeColours::new(colours),
			&bindings,
			&census,
			DYNAMIC_PANEL_FLOOR + 2,
			1,
		)
		.unwrap();
		assert!(!captured.panels.values().any(|panel| *panel == closed));
		assert!(captured.panels.values().any(|panel| *panel == stale));
		assert!(captured.colours.contains_key(&(document, view, 4)));
	}

	#[test]
	fn product_0062_post_spawn_join_is_total() {
		let document = DocumentId::new(1).unwrap();
		let mut meanings = empty_meanings();
		meanings.panels.insert(
			(document, LocalViewIdentity::Named("v".into())),
			HostPanelId::new(DYNAMIC_PANEL_FLOOR),
		);
		meanings.next_view_panel_id += 1;
		let bindings = [(document, BufferId::new(88))].into_iter().collect();
		assert!(
			meanings
				.join_runtime(&bindings, &SharedIdentityCensus::default())
				.is_err()
		);
		let census = SharedIdentityCensus {
			documents: [document].into_iter().collect(),
			host_panels: [HostPanelId::new(DYNAMIC_PANEL_FLOOR)]
				.into_iter()
				.collect(),
			host_panel_groups: BTreeSet::new(),
		};
		let runtime = meanings.join_runtime(&bindings, &census).unwrap();
		assert_eq!(runtime.panels.len(), 1);
	}

	#[test]
	fn product_0062_schema_has_no_placement_or_renderer_vocabulary() {
		let text = String::from_utf8(empty_meanings().encode().unwrap()).unwrap();
		for forbidden in [
			"buffer_id",
			"pane_id",
			"view_id",
			"tab_index",
			"ratio",
			"descriptor",
			"plot_type",
		] {
			assert!(
				!text.contains(forbidden),
				"schema must not encode {forbidden}"
			);
		}
	}
}
