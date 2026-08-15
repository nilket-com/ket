use std::collections::BTreeMap;
use std::fmt;

/// One numeric cell as it arrives from the relation, before validation.
///
/// `Inexact` carries the rejected rendering so a refusal can name the value the
/// author actually wrote, rather than a lossy reconstruction of it.
#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ViolinDensityCell {
	Null,
	Exact(f64),
	Inexact(String),
}

/// A single delivered row: one group, one evaluation point, its density, and the
/// group's median carried alongside by the relation-side join (PRODUCT-0107 §2).
#[derive(Clone, Debug, PartialEq)]
pub(crate) struct ViolinDensityInputRow {
	pub(crate) group: Option<String>,
	pub(crate) evaluation_value: ViolinDensityCell,
	pub(crate) density: ViolinDensityCell,
	pub(crate) median: ViolinDensityCell,
}

/// How a density becomes a rendered half-width (PRODUCT-0107 §3).
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum ViolinWidthNormalization {
	/// One scale for the whole figure, so widths are comparable across groups.
	#[default]
	Figure,
	/// Each violin reaches full width regardless of its neighbours. Selected
	/// deliberately by a caller — PRODUCT-0108 §6 gives it no authored spelling —
	/// because it discards the cross-group comparison the shared evaluation
	/// extent exists to support.
	#[allow(dead_code)] // Supported for callers; PRODUCT-0108 deliberately adds no Rune token.
	PerViolin,
}

/// One group's validated curve.
#[derive(Clone, Debug, PartialEq)]
pub struct ViolinCurve {
	label: String,
	median: f64,
	densities: Vec<f64>,
}

impl ViolinCurve {
	pub fn label(&self) -> &str {
		&self.label
	}

	pub fn median(&self) -> f64 {
		self.median
	}

	/// Densities in grid order, positionally aligned with
	/// [`ViolinDensity::grid`]. Unnormalized, exactly as Toron ADR 552 emits
	/// them: converting these to a width is the kernel's decision, not this
	/// substrate's.
	#[cfg(test)]
	pub(crate) fn densities(&self) -> &[f64] {
		&self.densities
	}
}

/// Validated violin observations prepared from Toron ADR 552's density relation.
///
/// Construction is crate-scoped so the grid, the curves, the category order, and
/// the figure-wide peak cannot be supplied as independent truths (PRODUCT-0107
/// §1). Every derived value here is computed from the same validated rows.
#[derive(Clone, Debug, PartialEq)]
pub struct ViolinDensity {
	grid: Vec<f64>,
	curves: Vec<ViolinCurve>,
	peak_density: f64,
	refused_groups: Vec<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub(crate) enum ViolinDensityError {
	/// A row with no category identity cannot be placed on a categorical axis
	/// (PRODUCT-0107 §4).
	NullGroup {
		row: usize,
	},
	NullValue {
		field: String,
		row: usize,
	},
	InexactValue {
		field: String,
		row: usize,
		value: String,
	},
	/// Toron ADR 552 evaluates every group on one shared grid, so groups that
	/// disagree are structurally incoherent.
	///
	/// This says nothing about provenance. Two independent executions over
	/// unchanged input would agree on an extent and pass, and that is correct:
	/// nothing in the delivered rows distinguishes them. Same-invocation
	/// composition is an authored contract (PRODUCT-0107 §2), not something this
	/// guard can establish.
	GridMismatch {
		group: String,
		expected: usize,
		actual: usize,
	},
	/// The median is joined per group, so every row of a group carries the same
	/// value. Two values mean the join was wrong.
	MedianDisagreement {
		group: String,
	},
	/// A curve needs at least two points to have a shape (PRODUCT-0107 §7).
	TooFewPoints {
		group: String,
		points: usize,
	},
}

impl ViolinDensity {
	pub(crate) fn from_rows(rows: Vec<ViolinDensityInputRow>) -> Result<Self, ViolinDensityError> {
		// Keyed and ordered here rather than trusting arrival order. Toron
		// delivers key-sorted rows, and a substrate that inferred its category
		// order from row order would look correct on any fixture whose authored
		// order happened to sort the same way (PRODUCT-0107 §6).
		let mut grouped: BTreeMap<String, (Vec<(f64, f64)>, f64)> = BTreeMap::new();
		let mut refused_groups: Vec<String> = Vec::new();

		for (index, row) in rows.into_iter().enumerate() {
			let one_based = index + 1;
			let Some(group) = row.group else {
				return Err(ViolinDensityError::NullGroup { row: one_based });
			};
			let evaluation_value =
				validated_cell("evaluation_value", one_based, row.evaluation_value)?;
			let density = validated_cell("density", one_based, row.density)?;
			let median = validated_cell("median", one_based, row.median)?;

			// A group carrying a non-finite value is declined whole rather than
			// drawn from its finite parts (PRODUCT-0107 §5). Toron ADR 552 makes
			// non-finiteness sticky per group, so one bad cell condemns the
			// group and nothing else. The refusal is recorded so the absence is
			// inspectable instead of silent.
			if !evaluation_value.is_finite() || !density.is_finite() || !median.is_finite() {
				grouped.remove(&group);
				if !refused_groups.contains(&group) {
					refused_groups.push(group);
				}
				continue;
			}
			if refused_groups.contains(&group) {
				continue;
			}

			match grouped.get_mut(&group) {
				Some((points, existing_median)) => {
					if *existing_median != median {
						return Err(ViolinDensityError::MedianDisagreement { group });
					}
					points.push((evaluation_value, density));
				}
				None => {
					grouped.insert(group, (vec![(evaluation_value, density)], median));
				}
			}
		}

		if grouped.is_empty() {
			return Ok(Self {
				grid: Vec::new(),
				curves: Vec::new(),
				peak_density: 0.0,
				refused_groups,
			});
		}

		// The first group in category order establishes the grid; every other
		// group is checked against it rather than assumed to match.
		let mut grid: Vec<f64> = Vec::new();
		let mut curves = Vec::new();
		let mut peak_density = 0.0_f64;
		for (label, (mut points, median)) in grouped {
			points.sort_by(|left, right| {
				left.0
					.partial_cmp(&right.0)
					.expect("validated densities are finite")
			});
			if points.len() < 2 {
				return Err(ViolinDensityError::TooFewPoints {
					group: label,
					points: points.len(),
				});
			}
			let group_grid = points.iter().map(|(value, _)| *value).collect::<Vec<_>>();
			if grid.is_empty() {
				grid = group_grid;
			} else if grid != group_grid {
				return Err(ViolinDensityError::GridMismatch {
					group: label,
					expected: grid.len(),
					actual: group_grid.len(),
				});
			}
			let densities = points
				.into_iter()
				.map(|(_, density)| density)
				.collect::<Vec<f64>>();
			for density in &densities {
				peak_density = peak_density.max(*density);
			}
			curves.push(ViolinCurve {
				label,
				median,
				densities,
			});
		}

		Ok(Self {
			grid,
			curves,
			peak_density,
			refused_groups,
		})
	}

	/// The shared evaluation grid, ascending. One grid for the whole figure, per
	/// Toron ADR 552's shared extent.
	pub fn grid(&self) -> &[f64] {
		&self.grid
	}

	/// Curves in the substrate's declared category order.
	pub fn curves(&self) -> impl ExactSizeIterator<Item = &ViolinCurve> {
		self.curves.iter()
	}

	#[cfg(test)]
	pub(crate) fn category_position(&self, label: &str) -> Option<usize> {
		self.curves.iter().position(|curve| curve.label == label)
	}

	/// Highest density across every rendered group.
	///
	/// This is what a figure-wide normalization divides by (PRODUCT-0107 §3).
	/// It is derived here, with the curves, so a kernel cannot normalize against
	/// a peak that came from somewhere else.
	pub fn peak_density(&self) -> f64 {
		self.peak_density
	}

	/// Density-to-width scaling for the rendered figure (PRODUCT-0107 §3).
	///
	/// The default divides every group by the figure's peak, so a group's width
	/// is comparable to its neighbour's — the comparability Toron ADR 552 bought
	/// on the value axis by sharing one evaluation extent. `PerViolin` restores
	/// the older convention where every violin reaches full width; it is a real
	/// convention and stays available, but it is asked for rather than assumed.
	pub fn normalized_widths(&self, mode: ViolinWidthNormalization) -> Vec<Vec<f64>> {
		self.curves
			.iter()
			.map(|curve| {
				let divisor = match mode {
					ViolinWidthNormalization::Figure => self.peak_density,
					ViolinWidthNormalization::PerViolin => {
						curve.densities.iter().copied().fold(0.0_f64, f64::max)
					}
				};
				if divisor <= 0.0 {
					// Every density in scope is zero. Scaling by a zero peak
					// would be a division, not a decision; a flat curve is the
					// honest rendering.
					return vec![0.0; curve.densities.len()];
				}
				curve
					.densities
					.iter()
					.map(|density| density / divisor)
					.collect()
			})
			.collect()
	}

	/// Groups declined for carrying a non-finite value, in encounter order.
	///
	/// Absence is not zero: a caller that needs to tell "declined" from "never
	/// present" reads this rather than inferring it from a missing curve.
	#[cfg(test)]
	pub(crate) fn refused_groups(&self) -> &[String] {
		&self.refused_groups
	}
}

fn validated_cell(
	field: &str,
	row: usize,
	cell: ViolinDensityCell,
) -> Result<f64, ViolinDensityError> {
	match cell {
		ViolinDensityCell::Null => Err(ViolinDensityError::NullValue {
			field: field.to_owned(),
			row,
		}),
		ViolinDensityCell::Inexact(value) => Err(ViolinDensityError::InexactValue {
			field: field.to_owned(),
			row,
			value,
		}),
		ViolinDensityCell::Exact(value) => Ok(value),
	}
}

impl fmt::Display for ViolinDensityError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NullGroup { row } => write!(
				f,
				"violin density row {row} has no group: a row without category identity cannot be placed on a categorical axis"
			),
			Self::NullValue { field, row } => {
				write!(f, "violin density row {row} has a null {field}")
			}
			Self::InexactValue { field, row, value } => write!(
				f,
				"violin density row {row} has a {field} that is not exactly representable: {value}"
			),
			Self::GridMismatch {
				group,
				expected,
				actual,
			} => write!(
				f,
				"violin density group {group} carries {actual} evaluation points against the figure's {expected}: every group shares one evaluation grid"
			),
			Self::MedianDisagreement { group } => write!(
				f,
				"violin density group {group} carries more than one median: the median is joined per group, so two values mean the join was wrong"
			),
			Self::TooFewPoints { group, points } => write!(
				f,
				"violin density group {group} carries {points} evaluation points: a curve needs at least two to have a shape"
			),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn cell(value: f64) -> ViolinDensityCell {
		ViolinDensityCell::Exact(value)
	}

	fn row(group: &str, evaluation_value: f64, density: f64, median: f64) -> ViolinDensityInputRow {
		ViolinDensityInputRow {
			group: Some(group.to_owned()),
			evaluation_value: cell(evaluation_value),
			density: cell(density),
			median: cell(median),
		}
	}

	/// Two groups on one grid with different peaks.
	///
	/// The taller group sorts **first** deliberately. With the tallest last, a
	/// substrate that simply overwrote the peak per group would still land on the
	/// right number, and every assertion about a figure-wide peak would pass
	/// against exactly the per-group behaviour PRODUCT-0107 §3 replaces.
	fn two_groups() -> Vec<ViolinDensityInputRow> {
		vec![
			row("east", 0.0, 0.10, 1.0),
			row("east", 1.0, 0.80, 1.0),
			row("east", 2.0, 0.10, 1.0),
			row("west", 0.0, 0.05, 5.0),
			row("west", 1.0, 0.40, 5.0),
			row("west", 2.0, 0.05, 5.0),
		]
	}

	#[test]
	fn prepared_curves_carry_one_shared_grid_and_their_own_medians() {
		let prepared = ViolinDensity::from_rows(two_groups()).unwrap();
		assert_eq!(prepared.grid(), &[0.0, 1.0, 2.0]);
		let curves = prepared.curves().collect::<Vec<_>>();
		assert_eq!(curves.len(), 2);
		assert_eq!(curves[0].label(), "east");
		assert_eq!(curves[0].median(), 1.0);
		assert_eq!(curves[1].label(), "west");
		assert_eq!(curves[1].median(), 5.0);
		assert_eq!(curves[0].densities().len(), prepared.grid().len());
	}

	#[test]
	fn peak_density_is_the_figure_wide_maximum_not_a_per_group_one() {
		// The value a figure-wide normalization divides by. Taking it per group
		// is exactly the renderer behaviour PRODUCT-0107 §3 replaces, so a
		// substrate that returned 0.40 for east would defeat that decision
		// before the kernel ever saw the numbers.
		let prepared = ViolinDensity::from_rows(two_groups()).unwrap();
		assert_eq!(prepared.peak_density(), 0.80);
	}

	#[test]
	fn category_order_is_declared_not_taken_from_row_order() {
		let mut rows = two_groups();
		rows.reverse();
		let prepared = ViolinDensity::from_rows(rows).unwrap();
		let labels = prepared
			.curves()
			.map(|curve| curve.label().to_owned())
			.collect::<Vec<_>>();
		assert_eq!(labels, vec!["east".to_owned(), "west".to_owned()]);
		assert_eq!(prepared.category_position("west"), Some(1));
		assert_eq!(prepared.grid(), &[0.0, 1.0, 2.0]);
	}

	#[test]
	fn a_null_group_is_refused_rather_than_rendered_under_a_placeholder() {
		let mut rows = two_groups();
		rows[0].group = None;
		let error = ViolinDensity::from_rows(rows).unwrap_err();
		assert_eq!(error, ViolinDensityError::NullGroup { row: 1 });
		assert!(error.to_string().contains("category identity"));
	}

	#[test]
	fn a_non_finite_group_is_declined_whole_and_recorded() {
		let mut rows = two_groups();
		rows[1].density = cell(f64::NAN);
		let prepared = ViolinDensity::from_rows(rows).unwrap();

		let labels = prepared
			.curves()
			.map(|curve| curve.label().to_owned())
			.collect::<Vec<_>>();
		assert_eq!(
			labels,
			vec!["west".to_owned()],
			"the poisoned group is declined whole, not drawn from its finite points"
		);
		assert_eq!(prepared.refused_groups(), &["east".to_owned()]);
		assert_eq!(
			prepared.peak_density(),
			0.40,
			"a declined group contributes nothing to the figure-wide peak — east's 0.80 is gone with it"
		);
	}

	#[test]
	fn a_group_that_never_arrived_is_distinguishable_from_one_that_was_declined() {
		// Absence is not zero, and neither is it refusal. A caller must be able
		// to tell a degenerate group Toron omitted from one this substrate
		// declined.
		let prepared = ViolinDensity::from_rows(two_groups()).unwrap();
		assert!(prepared.refused_groups().is_empty());
		assert!(prepared.category_position("north").is_none());
	}

	#[test]
	fn groups_that_disagree_about_the_grid_are_refused() {
		let mut rows = two_groups();
		rows.push(row("west", 3.0, 0.01, 5.0));
		let error = ViolinDensity::from_rows(rows).unwrap_err();
		assert!(
			matches!(error, ViolinDensityError::GridMismatch { ref group, .. } if group == "west")
		);
		assert!(error.to_string().contains("one evaluation grid"));
	}

	#[test]
	fn two_medians_in_one_group_are_refused_as_a_bad_join() {
		let mut rows = two_groups();
		rows[1].median = cell(2.0);
		let error = ViolinDensity::from_rows(rows).unwrap_err();
		assert_eq!(
			error,
			ViolinDensityError::MedianDisagreement {
				group: "east".to_owned()
			}
		);
		assert!(error.to_string().contains("join was wrong"));
	}

	#[test]
	fn a_single_point_group_cannot_form_a_curve() {
		let rows = vec![row("east", 0.0, 0.1, 1.0)];
		let error = ViolinDensity::from_rows(rows).unwrap_err();
		assert_eq!(
			error,
			ViolinDensityError::TooFewPoints {
				group: "east".to_owned(),
				points: 1,
			}
		);
	}

	#[test]
	fn null_and_inexact_cells_name_their_field_and_row() {
		let mut rows = two_groups();
		rows[2].density = ViolinDensityCell::Null;
		let error = ViolinDensity::from_rows(rows).unwrap_err();
		assert_eq!(
			error,
			ViolinDensityError::NullValue {
				field: "density".to_owned(),
				row: 3,
			}
		);

		let mut rows = two_groups();
		rows[4].median = ViolinDensityCell::Inexact("9007199254740993".to_owned());
		let error = ViolinDensity::from_rows(rows).unwrap_err();
		assert!(matches!(
			error,
			ViolinDensityError::InexactValue { ref field, row: 5, .. } if field == "median"
		));
	}

	#[test]
	fn figure_normalization_keeps_group_widths_comparable() {
		// PRODUCT-0107 §3's point, and the control that fails against the
		// renderer's per-violin divide. `east` peaks at 0.80 and `west` at 0.40,
		// so under one figure-wide scale west must render half as wide.
		let prepared = ViolinDensity::from_rows(two_groups()).unwrap();
		let widths = prepared.normalized_widths(ViolinWidthNormalization::Figure);

		let east_peak = widths[0].iter().copied().fold(0.0_f64, f64::max);
		let west_peak = widths[1].iter().copied().fold(0.0_f64, f64::max);
		assert_eq!(
			east_peak, 1.0,
			"the figure's tallest group reaches full width"
		);
		assert_eq!(
			west_peak, 0.5,
			"a group with half the density renders half as wide"
		);
		assert_ne!(
			east_peak, west_peak,
			"equal widths would discard the comparison the shared extent exists for"
		);
	}

	#[test]
	fn per_violin_normalization_is_reachable_and_does_the_opposite() {
		// A mode indistinguishable from the default is not a choice.
		let prepared = ViolinDensity::from_rows(two_groups()).unwrap();
		let widths = prepared.normalized_widths(ViolinWidthNormalization::PerViolin);

		let east_peak = widths[0].iter().copied().fold(0.0_f64, f64::max);
		let west_peak = widths[1].iter().copied().fold(0.0_f64, f64::max);
		assert_eq!(east_peak, 1.0);
		assert_eq!(
			west_peak, 1.0,
			"every violin reaches full width when this mode is selected"
		);
	}

	#[test]
	fn normalization_preserves_a_group_s_shape_whichever_mode_is_selected() {
		// §3's invariant: normalization scales a curve, it never reshapes one.
		// An operation that changed relative densities inside a group would be a
		// statistic wearing a renderer's clothes.
		let prepared = ViolinDensity::from_rows(two_groups()).unwrap();
		let figure = prepared.normalized_widths(ViolinWidthNormalization::Figure);
		let per_violin = prepared.normalized_widths(ViolinWidthNormalization::PerViolin);

		for (figure_curve, per_violin_curve) in figure.iter().zip(&per_violin) {
			let figure_ratio = figure_curve[0] / figure_curve[1];
			let per_violin_ratio = per_violin_curve[0] / per_violin_curve[1];
			assert!(
				(figure_ratio - per_violin_ratio).abs() < 1e-12,
				"within a group the shape is identical under either mode"
			);
		}
	}

	#[test]
	fn a_declined_group_contributes_no_width_and_does_not_rescale_the_rest() {
		let mut rows = two_groups();
		rows[1].density = cell(f64::NAN);
		let prepared = ViolinDensity::from_rows(rows).unwrap();
		let widths = prepared.normalized_widths(ViolinWidthNormalization::Figure);

		assert_eq!(widths.len(), 1, "only the surviving group has a curve");
		assert_eq!(
			widths[0].iter().copied().fold(0.0_f64, f64::max),
			1.0,
			"the survivor is scaled against the figure it is actually in"
		);
	}

	#[test]
	fn an_all_zero_figure_renders_flat_rather_than_dividing_by_zero() {
		let rows = vec![
			row("east", 0.0, 0.0, 1.0),
			row("east", 1.0, 0.0, 1.0),
			row("east", 2.0, 0.0, 1.0),
		];
		let prepared = ViolinDensity::from_rows(rows).unwrap();
		let widths = prepared.normalized_widths(ViolinWidthNormalization::Figure);
		assert_eq!(widths[0], vec![0.0, 0.0, 0.0]);
		assert!(widths[0].iter().all(|width| width.is_finite()));
	}

	#[test]
	fn an_empty_relation_prepares_nothing_rather_than_failing() {
		let prepared = ViolinDensity::from_rows(Vec::new()).unwrap();
		assert_eq!(prepared.curves().len(), 0);
		assert!(prepared.grid().is_empty());
		assert_eq!(prepared.peak_density(), 0.0);
	}
}
