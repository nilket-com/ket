//! PRODUCT-0156: exact series identity and pane-local visibility algebra.

use crate::plot::spec::SemanticType;
use crate::selection::{ExactScalar, FieldRef};
use polars::prelude::{Column, DataType};
use std::collections::HashSet;

/// A printed entry addresses an identity only when every contributing row
/// has that same identity. Missing and conflicting identities stay inert.
pub(crate) fn unanimous_identity<'a>(
	identities: impl IntoIterator<Item = Option<&'a SeriesIdentity>>,
) -> Option<SeriesIdentity> {
	let mut identities = identities.into_iter();
	let first = identities.next()??;
	identities
		.all(|identity| identity == Some(first))
		.then(|| first.clone())
}

pub fn draw_series(
	backend: &mut dyn crate::plot::common::PlotBackend,
	identity: Option<&SeriesIdentity>,
	settings: &crate::plot::common::ResolvedPlotSettings,
	f: &mut dyn FnMut(&mut dyn crate::plot::common::PlotBackend),
) {
	if !settings.hidden_series.is_visible(identity) {
		return;
	}
	if let Some(group) = identity.and_then(|identity| settings.series_registry.group(identity)) {
		backend.with_series(group, f);
	} else {
		f(backend);
	}
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SeriesIdentity {
	pub field: FieldRef,
	pub value: ExactScalar,
}

impl SeriesIdentity {
	/// Read the partition's original column, never its displayed label or a
	/// projected coordinate. Categorical physical codes are not identities.
	pub fn from_column(column: &Column, row: usize, semantic_type: SemanticType) -> Option<Self> {
		let string_column;
		let source = if matches!(
			column.dtype(),
			DataType::Categorical(..) | DataType::Enum(..)
		) {
			string_column = column.cast(&DataType::String).ok()?;
			&string_column
		} else {
			column
		};
		let value = ExactScalar::from_any_value(source.get(row).ok()?, source.dtype())?;
		Some(Self {
			field: FieldRef::new(column.name().as_str(), semantic_type, value.scalar_type()),
			value,
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeriesVisibilityAction {
	Hide(SeriesIdentity),
	Show(SeriesIdentity),
	Isolate(SeriesIdentity),
	RestoreAll,
}

#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct HiddenSeries(HashSet<SeriesIdentity>);

impl HiddenSeries {
	pub fn contains(&self, identity: &SeriesIdentity) -> bool {
		self.0.contains(identity)
	}

	pub fn is_empty(&self) -> bool {
		self.0.is_empty()
	}

	pub fn description(&self) -> String {
		let mut entries: Vec<_> = self
			.0
			.iter()
			.map(|identity| format!("{} = {:?}", identity.field.name.as_str(), identity.value))
			.collect();
		entries.sort();
		entries.join(", ")
	}

	pub fn is_visible(&self, identity: Option<&SeriesIdentity>) -> bool {
		identity.is_none_or(|identity| !self.contains(identity))
	}

	/// Returns true only for a state transition, so stale and repeated
	/// messages cannot invalidate a render or install a meaningless override.
	pub fn apply(&mut self, action: &SeriesVisibilityAction, universe: &[SeriesIdentity]) -> bool {
		match action {
			SeriesVisibilityAction::RestoreAll => {
				let changed = !self.is_empty();
				self.0.clear();
				changed
			}
			SeriesVisibilityAction::Hide(identity) if universe.contains(identity) => {
				self.0.insert(identity.clone())
			}
			SeriesVisibilityAction::Show(identity) if universe.contains(identity) => {
				self.0.remove(identity)
			}
			SeriesVisibilityAction::Isolate(identity) if universe.contains(identity) => {
				let hidden = universe
					.iter()
					.filter(|other| *other != identity)
					.cloned()
					.collect();
				if self.0 == hidden {
					return false;
				}
				self.0 = hidden;
				true
			}
			_ => false,
		}
	}

	pub fn legend_action(
		&self,
		identity: &SeriesIdentity,
		isolate: bool,
		universe: &[SeriesIdentity],
	) -> SeriesVisibilityAction {
		if isolate {
			if !self.contains(identity)
				&& universe
					.iter()
					.all(|other| other == identity || self.contains(other))
			{
				SeriesVisibilityAction::RestoreAll
			} else {
				SeriesVisibilityAction::Isolate(identity.clone())
			}
		} else if self.contains(identity) {
			SeriesVisibilityAction::Show(identity.clone())
		} else {
			SeriesVisibilityAction::Hide(identity.clone())
		}
	}
}

/// Opaque figure ordinal, minted only by the complete figure registry.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SeriesGroup(usize);

impl SeriesGroup {
	pub fn ordinal(self) -> usize {
		self.0
	}
}

/// First occurrence in prepared layer/cell/series order determines the
/// ordinal. This registry must be built before applying visibility.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct SeriesRegistry {
	identities: Vec<SeriesIdentity>,
}

impl SeriesRegistry {
	pub fn new(identities: impl IntoIterator<Item = SeriesIdentity>) -> Self {
		let mut seen = HashSet::new();
		Self {
			identities: identities
				.into_iter()
				.filter(|identity| seen.insert(identity.clone()))
				.collect(),
		}
	}

	pub fn identities(&self) -> &[SeriesIdentity] {
		&self.identities
	}

	pub fn group(&self, identity: &SeriesIdentity) -> Option<SeriesGroup> {
		self.identities
			.iter()
			.position(|other| other == identity)
			.map(SeriesGroup)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0156_exact_identity_and_visibility_transitions() {
		let column = Column::new(
			"category".into(),
			[
				Some(9_007_199_254_740_992i64),
				Some(9_007_199_254_740_993),
				None,
			],
		);
		let a = SeriesIdentity::from_column(&column, 0, SemanticType::Nominal).unwrap();
		let b = SeriesIdentity::from_column(&column, 1, SemanticType::Nominal).unwrap();
		assert_ne!(a, b);
		assert!(SeriesIdentity::from_column(&column, 2, SemanticType::Nominal).is_none());
		let registry = SeriesRegistry::new([a.clone(), b.clone(), a.clone()]);
		assert_eq!(registry.identities().len(), 2);
		assert_eq!(registry.group(&b).unwrap().ordinal(), 1);
		let mut hidden = HiddenSeries::default();
		let universe = registry.identities();
		assert!(hidden.apply(&SeriesVisibilityAction::Hide(a.clone()), universe));
		assert!(!hidden.apply(&SeriesVisibilityAction::Hide(a.clone()), universe));
		assert!(hidden.is_visible(None));
		assert_eq!(
			hidden.legend_action(&b, true, universe),
			SeriesVisibilityAction::RestoreAll
		);
		assert!(hidden.apply(&SeriesVisibilityAction::Isolate(a.clone()), universe));
		assert!(hidden.is_visible(Some(&a)));
		assert!(!hidden.is_visible(Some(&b)));
		let unknown = SeriesIdentity {
			field: FieldRef::new("other", SemanticType::Nominal, a.value.scalar_type()),
			value: a.value.clone(),
		};
		assert!(!hidden.apply(&SeriesVisibilityAction::Isolate(unknown), universe));
		assert!(hidden.apply(&SeriesVisibilityAction::Show(b), universe));
		assert!(!hidden.apply(&SeriesVisibilityAction::RestoreAll, universe));
		assert_eq!(registry.group(&a).unwrap().ordinal(), 0);
	}
}
