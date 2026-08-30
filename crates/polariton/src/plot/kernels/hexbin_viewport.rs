//! PRODUCT-0143's executor-neutral viewport-prepared hexbin substrate.
//!
//! This module owns validation, the screen-space lattice, point assignment, and
//! sparse aggregation. `hexbin.rs` is deliberately only a consumer of the
//! tagged artifact produced here.

use crate::plot::exact_numeric::{ExactF64Error, exact_f64};
use crate::result_artifact::ArtifactToken;
use polars::prelude::{DataFrame, DataType};
use std::collections::HashMap;
use std::fmt;
use std::hash::{Hash, Hasher};
use std::sync::Arc;

const MAX_PLOT_EXTENT_PX: f32 = 1_000_000.0;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HexbinChannel {
	X,
	Y,
}

impl fmt::Display for HexbinChannel {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::X => "x",
			Self::Y => "y",
		})
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum HexbinPreparationError {
	MissingColumn {
		channel: HexbinChannel,
		field: String,
	},
	TemporalColumn {
		channel: HexbinChannel,
		field: String,
		dtype: DataType,
	},
	UnsupportedColumn {
		channel: HexbinChannel,
		field: String,
		dtype: DataType,
	},
	NullValue {
		channel: HexbinChannel,
		field: String,
		row: usize,
	},
	NonFiniteValue {
		channel: HexbinChannel,
		field: String,
		row: usize,
		value: f64,
	},
	InexactValue {
		channel: HexbinChannel,
		field: String,
		row: usize,
		value: String,
	},
}

impl fmt::Display for HexbinPreparationError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::MissingColumn { channel, field } => {
				write!(formatter, "hexbin {channel} field `{field}` is absent")
			}
			Self::TemporalColumn {
				channel,
				field,
				dtype,
			} => write!(
				formatter,
				"hexbin {channel} field `{field}` is temporal ({dtype:?}); PRODUCT-0143 admits quantitative coordinates only"
			),
			Self::UnsupportedColumn {
				channel,
				field,
				dtype,
			} => write!(
				formatter,
				"hexbin {channel} field `{field}` requires a quantitative dtype, got {dtype:?}"
			),
			Self::NullValue {
				channel,
				field,
				row,
			} => write!(
				formatter,
				"hexbin {channel} field `{field}` is null at row {row}"
			),
			Self::NonFiniteValue {
				channel,
				field,
				row,
				value,
			} => write!(
				formatter,
				"hexbin {channel} field `{field}` is non-finite at row {row}: {value}"
			),
			Self::InexactValue {
				channel,
				field,
				row,
				value,
			} => write!(
				formatter,
				"hexbin {channel} field `{field}` cannot be represented exactly at row {row}: {value}"
			),
		}
	}
}

impl std::error::Error for HexbinPreparationError {}

#[derive(Clone, Copy, Debug, PartialEq)]
struct HexbinObservation {
	x: f64,
	y: f64,
}

/// Validated materialized observations. All fields are private so the source
/// token, rows, ranges, and source-count guide bound cannot disagree.
#[derive(Clone, Debug)]
pub struct HexbinObservations {
	source_token: ArtifactToken,
	x_field: String,
	y_field: String,
	rows: Vec<HexbinObservation>,
	x_range: (f64, f64),
	y_range: (f64, f64),
}

#[allow(dead_code)] // The executor-neutral contract exposes future remote-adapter queries.
impl HexbinObservations {
	pub fn source_token(&self) -> ArtifactToken {
		self.source_token
	}

	pub fn x_field(&self) -> &str {
		&self.x_field
	}

	pub fn y_field(&self) -> &str {
		&self.y_field
	}

	pub fn source_count(&self) -> u64 {
		u64::try_from(self.rows.len()).expect("a materialized row count fits u64")
	}

	pub fn is_empty(&self) -> bool {
		self.rows.is_empty()
	}

	pub fn x_range(&self) -> (f64, f64) {
		self.x_range
	}

	pub fn y_range(&self) -> (f64, f64) {
		self.y_range
	}
}

fn temporal_dtype(dtype: &DataType) -> bool {
	matches!(
		dtype,
		DataType::Date | DataType::Datetime(_, _) | DataType::Time
	)
}

/// Validate both complete columns before scanning rows, then validate every
/// row before any viewport can filter it (PRODUCT-0143 §5).
pub fn prepare_hexbin_observations(
	df: &DataFrame,
	x_field: &str,
	y_field: &str,
	source_token: ArtifactToken,
) -> Result<HexbinObservations, HexbinPreparationError> {
	let mut columns = Vec::with_capacity(2);
	for (channel, field) in [(HexbinChannel::X, x_field), (HexbinChannel::Y, y_field)] {
		let column = df
			.column(field)
			.map_err(|_| HexbinPreparationError::MissingColumn {
				channel,
				field: field.to_owned(),
			})?;
		if temporal_dtype(column.dtype()) {
			return Err(HexbinPreparationError::TemporalColumn {
				channel,
				field: field.to_owned(),
				dtype: column.dtype().clone(),
			});
		}
		if !column.dtype().is_numeric() {
			return Err(HexbinPreparationError::UnsupportedColumn {
				channel,
				field: field.to_owned(),
				dtype: column.dtype().clone(),
			});
		}
		columns.push(column.as_materialized_series());
	}

	let mut rows = Vec::with_capacity(df.height());
	let mut x_range = (f64::INFINITY, f64::NEG_INFINITY);
	let mut y_range = (f64::INFINITY, f64::NEG_INFINITY);
	for row_index in 0..df.height() {
		let mut coordinates = [0.0; 2];
		for (ordinal, (channel, field)) in
			[(HexbinChannel::X, x_field), (HexbinChannel::Y, y_field)]
				.into_iter()
				.enumerate()
		{
			let value = columns[ordinal]
				.get(row_index)
				.expect("hexbin coordinate row is in frame bounds");
			let original = value.to_string();
			let value = match exact_f64(value) {
				Ok(value) => value,
				Err(ExactF64Error::Null) => {
					return Err(HexbinPreparationError::NullValue {
						channel,
						field: field.to_owned(),
						row: row_index + 1,
					});
				}
				Err(ExactF64Error::Inexact) => {
					return Err(HexbinPreparationError::InexactValue {
						channel,
						field: field.to_owned(),
						row: row_index + 1,
						value: original,
					});
				}
			};
			if !value.is_finite() {
				return Err(HexbinPreparationError::NonFiniteValue {
					channel,
					field: field.to_owned(),
					row: row_index + 1,
					value,
				});
			}
			coordinates[ordinal] = value;
		}
		let [x, y] = coordinates;
		x_range = (x_range.0.min(x), x_range.1.max(x));
		y_range = (y_range.0.min(y), y_range.1.max(y));
		rows.push(HexbinObservation { x, y });
	}

	if rows.is_empty() {
		x_range = (0.0, 1.0);
		y_range = (0.0, 1.0);
	} else {
		x_range = padded_range(x_range);
		y_range = padded_range(y_range);
	}

	Ok(HexbinObservations {
		source_token,
		x_field: x_field.to_owned(),
		y_field: y_field.to_owned(),
		rows,
		x_range,
		y_range,
	})
}

fn padded_range(range: (f64, f64)) -> (f64, f64) {
	if range.0 == range.1 {
		let lower = range.0.next_down();
		let upper = range.1.next_up();
		return match (lower.is_finite(), upper.is_finite()) {
			(true, true) => (lower, upper),
			(true, false) => (lower, range.1),
			(false, true) => (range.0, upper),
			(false, false) => unreachable!("a validated finite value has a finite neighbour"),
		};
	}
	let span = range.1 - range.0;
	if !span.is_finite() {
		return range;
	}
	let padding = span.max(0.001) * 0.1;
	let expanded = (range.0 - padding, range.1 + padding);
	if expanded.0.is_finite() && expanded.1.is_finite() {
		expanded
	} else {
		range
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct HexRadiusPx(f32);

impl Eq for HexRadiusPx {}

impl Hash for HexRadiusPx {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.0.to_bits().hash(state);
	}
}

#[allow(dead_code)] // The authored radius is closed, but boundary tests and future executors validate it.
impl HexRadiusPx {
	pub const MIN: f32 = 1.0;
	pub const MAX: f32 = 10_000.0;
	pub const DEFAULT: Self = Self(10.0);

	pub fn new(radius: f32) -> Result<Self, HexLatticeError> {
		if radius.is_finite() && (Self::MIN..=Self::MAX).contains(&radius) {
			Ok(Self(canonical_zero_f32(radius)))
		} else {
			Err(HexLatticeError::InvalidRadius(radius))
		}
	}

	pub const fn get(self) -> f32 {
		self.0
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HexOrientation {
	PointyTop,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum HexOrigin {
	PlotLocalZero,
}

#[derive(Clone, Copy, Debug)]
pub struct HexLatticeSpec {
	x_domain: (f64, f64),
	y_domain: (f64, f64),
	plot_width_px: f32,
	plot_height_px: f32,
	radius_px: HexRadiusPx,
	orientation: HexOrientation,
	origin: HexOrigin,
}

impl PartialEq for HexLatticeSpec {
	fn eq(&self, other: &Self) -> bool {
		self.x_domain.0.to_bits() == other.x_domain.0.to_bits()
			&& self.x_domain.1.to_bits() == other.x_domain.1.to_bits()
			&& self.y_domain.0.to_bits() == other.y_domain.0.to_bits()
			&& self.y_domain.1.to_bits() == other.y_domain.1.to_bits()
			&& self.plot_width_px.to_bits() == other.plot_width_px.to_bits()
			&& self.plot_height_px.to_bits() == other.plot_height_px.to_bits()
			&& self.radius_px == other.radius_px
			&& self.orientation == other.orientation
			&& self.origin == other.origin
	}
}

impl Eq for HexLatticeSpec {}

impl Hash for HexLatticeSpec {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.x_domain.0.to_bits().hash(state);
		self.x_domain.1.to_bits().hash(state);
		self.y_domain.0.to_bits().hash(state);
		self.y_domain.1.to_bits().hash(state);
		self.plot_width_px.to_bits().hash(state);
		self.plot_height_px.to_bits().hash(state);
		self.radius_px.hash(state);
		self.orientation.hash(state);
		self.origin.hash(state);
	}
}

#[allow(dead_code)] // InvalidRadius is reachable through the closed bounded constructor.
#[derive(Clone, Debug, PartialEq)]
pub enum HexLatticeError {
	InvalidDomain {
		axis: HexbinChannel,
		range: (f64, f64),
	},
	InvalidExtent {
		width: f32,
		height: f32,
	},
	InvalidRadius(f32),
	AxialOverflow,
}

impl fmt::Display for HexLatticeError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::InvalidDomain { axis, range } => write!(
				formatter,
				"hexbin {axis} domain must be finite and ascending, got {:?}",
				range
			),
			Self::InvalidExtent { width, height } => write!(
				formatter,
				"hexbin plot extent must be finite and within 0..={MAX_PLOT_EXTENT_PX}, got {width} x {height}"
			),
			Self::InvalidRadius(radius) => write!(
				formatter,
				"hexbin radius must be finite and within {}..={}, got {radius}",
				HexRadiusPx::MIN,
				HexRadiusPx::MAX
			),
			Self::AxialOverflow => formatter.write_str(
				"hexbin lattice extent and radius would overflow signed 32-bit axial coordinates",
			),
		}
	}
}

impl std::error::Error for HexLatticeError {}

#[allow(dead_code)] // Tags expose every identity member for correspondence checks.
impl HexLatticeSpec {
	pub fn new(
		x_domain: (f64, f64),
		y_domain: (f64, f64),
		plot_width_px: f32,
		plot_height_px: f32,
		radius_px: HexRadiusPx,
	) -> Result<Self, HexLatticeError> {
		let x_domain = validate_domain(HexbinChannel::X, x_domain)?;
		let y_domain = validate_domain(HexbinChannel::Y, y_domain)?;
		if !plot_width_px.is_finite()
			|| !plot_height_px.is_finite()
			|| plot_width_px < 0.0
			|| plot_height_px < 0.0
			|| plot_width_px > MAX_PLOT_EXTENT_PX
			|| plot_height_px > MAX_PLOT_EXTENT_PX
		{
			return Err(HexLatticeError::InvalidExtent {
				width: plot_width_px,
				height: plot_height_px,
			});
		}
		let spec = Self {
			x_domain,
			y_domain,
			plot_width_px: canonical_zero_f32(plot_width_px),
			plot_height_px: canonical_zero_f32(plot_height_px),
			radius_px,
			orientation: HexOrientation::PointyTop,
			origin: HexOrigin::PlotLocalZero,
		};
		for (x, y) in [
			(0.0, 0.0),
			(f64::from(plot_width_px), 0.0),
			(0.0, f64::from(plot_height_px)),
			(f64::from(plot_width_px), f64::from(plot_height_px)),
		] {
			if fractional_axial(x, y, radius_px).0.abs() > f64::from(i32::MAX - 2)
				|| fractional_axial(x, y, radius_px).1.abs() > f64::from(i32::MAX - 2)
			{
				return Err(HexLatticeError::AxialOverflow);
			}
		}
		Ok(spec)
	}

	pub fn x_domain(self) -> (f64, f64) {
		self.x_domain
	}

	pub fn y_domain(self) -> (f64, f64) {
		self.y_domain
	}

	pub fn plot_width_px(self) -> f32 {
		self.plot_width_px
	}

	pub fn plot_height_px(self) -> f32 {
		self.plot_height_px
	}

	pub fn radius_px(self) -> HexRadiusPx {
		self.radius_px
	}

	pub fn orientation(self) -> HexOrientation {
		self.orientation
	}

	pub fn origin(self) -> HexOrigin {
		self.origin
	}

	pub fn pixel_for_data(self, x: f64, y: f64) -> Option<(f64, f64)> {
		if x < self.x_domain.0 || x > self.x_domain.1 || y < self.y_domain.0 || y > self.y_domain.1
		{
			return None;
		}
		let x_t = (x - self.x_domain.0) / (self.x_domain.1 - self.x_domain.0);
		let y_t = (y - self.y_domain.0) / (self.y_domain.1 - self.y_domain.0);
		Some((
			x_t * f64::from(self.plot_width_px),
			(1.0 - y_t) * f64::from(self.plot_height_px),
		))
	}

	pub fn data_for_pixel(self, x: f64, y: f64) -> (f64, f64) {
		let x_t = if self.plot_width_px > 0.0 {
			x / f64::from(self.plot_width_px)
		} else {
			0.0
		};
		let y_t = if self.plot_height_px > 0.0 {
			1.0 - y / f64::from(self.plot_height_px)
		} else {
			0.0
		};
		(
			self.x_domain.0 + x_t * (self.x_domain.1 - self.x_domain.0),
			self.y_domain.0 + y_t * (self.y_domain.1 - self.y_domain.0),
		)
	}

	pub fn axial_for_pixel(self, x: f64, y: f64) -> Option<(i32, i32)> {
		nearest_axial(x, y, self.radius_px)
	}

	pub fn center_for_axial(self, q: i32, r: i32) -> (f64, f64) {
		let radius = f64::from(self.radius_px.get());
		let sqrt_3 = 3.0_f64.sqrt();
		(
			radius * sqrt_3 * (f64::from(q) + f64::from(r) / 2.0),
			radius * 1.5 * f64::from(r),
		)
	}
}

fn validate_domain(axis: HexbinChannel, range: (f64, f64)) -> Result<(f64, f64), HexLatticeError> {
	if !range.0.is_finite() || !range.1.is_finite() || range.0 >= range.1 {
		return Err(HexLatticeError::InvalidDomain { axis, range });
	}
	Ok((canonical_zero_f64(range.0), canonical_zero_f64(range.1)))
}

fn canonical_zero_f32(value: f32) -> f32 {
	if value == 0.0 { 0.0 } else { value }
}

fn canonical_zero_f64(value: f64) -> f64 {
	if value == 0.0 { 0.0 } else { value }
}

fn fractional_axial(x: f64, y: f64, radius: HexRadiusPx) -> (f64, f64) {
	let radius = f64::from(radius.get());
	let sqrt_3 = 3.0_f64.sqrt();
	(
		(sqrt_3 / 3.0 * x - y / 3.0) / radius,
		(2.0 / 3.0 * y) / radius,
	)
}

/// The sole PRODUCT-0143 pixel-to-cell operation, shared by preparation and
/// hit-testing. Component ties round to even; correction ties prefer q, r, s.
pub fn nearest_axial(x: f64, y: f64, radius: HexRadiusPx) -> Option<(i32, i32)> {
	if !x.is_finite() || !y.is_finite() {
		return None;
	}
	let (q_fraction, r_fraction) = fractional_axial(x, y, radius);
	let s_fraction = -q_fraction - r_fraction;
	let mut q = q_fraction.round_ties_even();
	let mut r = r_fraction.round_ties_even();
	let s = s_fraction.round_ties_even();
	let q_error = (q - q_fraction).abs();
	let r_error = (r - r_fraction).abs();
	let s_error = (s - s_fraction).abs();
	if q_error >= r_error && q_error >= s_error {
		q = -r - s;
	} else if r_error >= s_error {
		r = -q - s;
	}
	if q < f64::from(i32::MIN)
		|| q > f64::from(i32::MAX)
		|| r < f64::from(i32::MIN)
		|| r > f64::from(i32::MAX)
	{
		None
	} else {
		Some((q as i32, r as i32))
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub struct HexbinPreparationGeneration(u64);

#[allow(dead_code)] // Remote scheduling will serialize the opaque generation value.
impl HexbinPreparationGeneration {
	pub const INITIAL: Self = Self(1);

	pub fn next(self) -> Self {
		Self(self.0.checked_add(1).expect("hexbin generation exhausted"))
	}

	pub const fn get(self) -> u64 {
		self.0
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HexbinPreparationRequest {
	pub source_token: ArtifactToken,
	pub generation: HexbinPreparationGeneration,
	pub x_field: String,
	pub y_field: String,
	pub lattice: HexLatticeSpec,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct HexbinViewportArtifact {
	source_token: ArtifactToken,
	generation: HexbinPreparationGeneration,
	lattice: HexLatticeSpec,
	bins: HashMap<(i32, i32), u64>,
	max_count: u64,
}

#[allow(dead_code)] // Consumers and correspondence gates intentionally query the same artifact.
impl HexbinViewportArtifact {
	pub fn source_token(&self) -> ArtifactToken {
		self.source_token
	}

	pub fn generation(&self) -> HexbinPreparationGeneration {
		self.generation
	}

	pub fn lattice(&self) -> HexLatticeSpec {
		self.lattice
	}

	pub fn bins(&self) -> &HashMap<(i32, i32), u64> {
		&self.bins
	}

	pub fn count(&self, q: i32, r: i32) -> Option<u64> {
		self.bins.get(&(q, r)).copied()
	}

	pub fn max_count(&self) -> u64 {
		self.max_count
	}

	pub fn total_count(&self) -> u64 {
		self.bins.values().copied().sum()
	}

	pub fn matches(&self, request: &HexbinPreparationRequest) -> bool {
		self.source_token == request.source_token
			&& self.generation == request.generation
			&& self.lattice == request.lattice
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HexbinExecutorError {
	SourceMismatch,
	FieldMismatch,
	AxialOverflow,
	CountOverflow,
}

impl fmt::Display for HexbinExecutorError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::SourceMismatch => "hexbin preparation request names a different source",
			Self::FieldMismatch => "hexbin preparation request names different coordinate fields",
			Self::AxialOverflow => "hexbin point assignment overflowed axial coordinates",
			Self::CountOverflow => "hexbin bin count overflowed u64",
		})
	}
}

impl std::error::Error for HexbinExecutorError {}

#[derive(Clone, Debug)]
pub struct MaterializedHexbinPreparer {
	observations: Arc<HexbinObservations>,
}

impl MaterializedHexbinPreparer {
	pub fn new(observations: Arc<HexbinObservations>) -> Self {
		Self { observations }
	}

	pub fn observations(&self) -> &Arc<HexbinObservations> {
		&self.observations
	}

	pub fn prepare(
		&self,
		request: &HexbinPreparationRequest,
	) -> Result<HexbinViewportArtifact, HexbinExecutorError> {
		if request.source_token != self.observations.source_token {
			return Err(HexbinExecutorError::SourceMismatch);
		}
		if request.x_field != self.observations.x_field
			|| request.y_field != self.observations.y_field
		{
			return Err(HexbinExecutorError::FieldMismatch);
		}
		let mut bins = HashMap::<(i32, i32), u64>::new();
		for observation in &self.observations.rows {
			let Some((pixel_x, pixel_y)) =
				request.lattice.pixel_for_data(observation.x, observation.y)
			else {
				continue;
			};
			let axial = request
				.lattice
				.axial_for_pixel(pixel_x, pixel_y)
				.ok_or(HexbinExecutorError::AxialOverflow)?;
			let count = bins.entry(axial).or_default();
			*count = count
				.checked_add(1)
				.ok_or(HexbinExecutorError::CountOverflow)?;
		}
		let max_count = bins.values().copied().max().unwrap_or(0);
		Ok(HexbinViewportArtifact {
			source_token: request.source_token,
			generation: request.generation,
			lattice: request.lattice,
			bins,
			max_count,
		})
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::result_artifact::ResultArtifact;
	use polars::df;
	use polars::prelude::{IntoColumn, IntoLazy, NamedFrom, Series, TimeUnit};

	fn observations(frame: DataFrame) -> Arc<HexbinObservations> {
		let result = ResultArtifact::test_frame(frame.clone());
		Arc::new(
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token())
				.expect("valid observations"),
		)
	}

	fn request(
		observations: &HexbinObservations,
		x_domain: (f64, f64),
		y_domain: (f64, f64),
		width: f32,
		height: f32,
	) -> HexbinPreparationRequest {
		HexbinPreparationRequest {
			source_token: observations.source_token(),
			generation: HexbinPreparationGeneration::INITIAL,
			x_field: observations.x_field().to_owned(),
			y_field: observations.y_field().to_owned(),
			lattice: HexLatticeSpec::new(x_domain, y_domain, width, height, HexRadiusPx::DEFAULT)
				.expect("valid lattice"),
		}
	}

	#[test]
	fn product_0143_materialized_counts_are_sparse_and_conserved() {
		let observations = observations(
			df!(
				"x" => [-0.9_f64, -0.9, 0.0, 0.9, 1.0, 2.0],
				"y" => [-0.9_f64, -0.9, 0.0, 0.9, 1.0, 2.0],
			)
			.unwrap(),
		);
		let preparer = MaterializedHexbinPreparer::new(Arc::clone(&observations));
		let request = request(&observations, (-1.0, 1.0), (-1.0, 1.0), 400.0, 200.0);
		let artifact = preparer.prepare(&request).unwrap();
		assert!(artifact.matches(&request));
		assert_eq!(artifact.total_count(), 5);
		assert!(artifact.bins().values().all(|count| *count > 0));
	}

	#[test]
	fn product_0143_resize_and_domain_changes_rebin() {
		let observations = observations(df!("x" => [0.26_f64], "y" => [0.51_f64]).unwrap());
		let preparer = MaterializedHexbinPreparer::new(Arc::clone(&observations));
		let narrow = request(&observations, (0.0, 1.0), (0.0, 1.0), 100.0, 100.0);
		let wide = request(&observations, (0.0, 1.0), (0.0, 1.0), 300.0, 100.0);
		let zoomed = request(&observations, (0.2, 0.8), (0.2, 0.8), 100.0, 100.0);
		let narrow_bin = *preparer
			.prepare(&narrow)
			.unwrap()
			.bins()
			.keys()
			.next()
			.unwrap();
		let wide_bin = *preparer
			.prepare(&wide)
			.unwrap()
			.bins()
			.keys()
			.next()
			.unwrap();
		let zoomed_bin = *preparer
			.prepare(&zoomed)
			.unwrap()
			.bins()
			.keys()
			.next()
			.unwrap();
		assert_ne!(narrow_bin, wide_bin);
		assert_ne!(narrow_bin, zoomed_bin);
	}

	#[test]
	fn product_0143_validation_precedes_viewport_filtering() {
		let frame = df!(
			"x" => [Some(0.0_f64), None],
			"y" => [Some(0.0_f64), Some(10_000.0)],
		)
		.unwrap();
		let result = ResultArtifact::test_frame(frame.clone());
		assert!(matches!(
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()),
			Err(HexbinPreparationError::NullValue {
				channel: HexbinChannel::X,
				row: 2,
				..
			})
		));
	}

	#[test]
	fn product_0143_non_finite_and_inexact_rows_refuse_with_channel_and_row() {
		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			let frame = df!("x" => [0.0_f64, value], "y" => [1.0_f64, 2.0]).unwrap();
			let result = ResultArtifact::test_frame(frame.clone());
			assert!(matches!(
				prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()),
				Err(HexbinPreparationError::NonFiniteValue {
					channel: HexbinChannel::X,
					row: 2,
					..
				})
			));
		}

		let frame = df!(
			"x" => [9_007_199_254_740_993_u64],
			"y" => [1.0_f64],
		)
		.unwrap();
		let result = ResultArtifact::test_frame(frame.clone());
		assert!(matches!(
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()),
			Err(HexbinPreparationError::InexactValue {
				channel: HexbinChannel::X,
				row: 1,
				..
			})
		));
	}

	#[test]
	fn product_0143_temporal_columns_refuse_explicitly() {
		let frame = df!(
			"x" => [1_i32, 2_i32],
			"y" => [3.0_f64, 4.0_f64],
		)
		.unwrap()
		.lazy()
		.with_column(polars::prelude::col("x").cast(DataType::Date))
		.collect()
		.unwrap();
		let result = ResultArtifact::test_frame(frame.clone());
		assert!(matches!(
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()),
			Err(HexbinPreparationError::TemporalColumn {
				channel: HexbinChannel::X,
				..
			})
		));
	}

	#[test]
	fn product_0143_all_temporal_dtypes_refuse_in_both_channels() {
		for dtype in [
			DataType::Date,
			DataType::Datetime(TimeUnit::Milliseconds, None),
			DataType::Time,
		] {
			for channel in [HexbinChannel::X, HexbinChannel::Y] {
				let temporal = Series::new("temporal".into(), [1_i64, 2])
					.cast(&dtype)
					.unwrap();
				let numeric = Series::new("numeric".into(), [3.0_f64, 4.0]);
				let (x, y) = match channel {
					HexbinChannel::X => (temporal, numeric),
					HexbinChannel::Y => (numeric, temporal),
				};
				let frame = DataFrame::new(2, vec![x.into_column(), y.into_column()]).unwrap();
				let result = ResultArtifact::test_frame(frame.clone());
				assert!(matches!(
					prepare_hexbin_observations(
						&frame,
						frame.get_column_names()[0].as_str(),
						frame.get_column_names()[1].as_str(),
						result.artifact_token(),
					),
					Err(HexbinPreparationError::TemporalColumn {
						channel: actual,
						..
					}) if actual == channel
				));
			}
		}
	}

	#[test]
	fn product_0143_empty_source_is_honest_and_missing_schema_still_refuses() {
		let frame = df!("x" => Vec::<f64>::new(), "y" => Vec::<f64>::new()).unwrap();
		let result = ResultArtifact::test_frame(frame.clone());
		let observations = Arc::new(
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()).unwrap(),
		);
		assert!(observations.is_empty());
		assert_eq!(observations.source_count(), 0);
		let artifact = MaterializedHexbinPreparer::new(Arc::clone(&observations))
			.prepare(&request(
				&observations,
				(0.0, 1.0),
				(0.0, 1.0),
				100.0,
				100.0,
			))
			.unwrap();
		assert!(artifact.bins().is_empty());
		assert_eq!(artifact.max_count(), 0);

		let missing = DataFrame::empty();
		let result = ResultArtifact::test_frame(missing.clone());
		assert!(matches!(
			prepare_hexbin_observations(&missing, "x", "y", result.artifact_token()),
			Err(HexbinPreparationError::MissingColumn {
				channel: HexbinChannel::X,
				..
			})
		));
	}

	#[test]
	fn product_0143_column_and_row_fault_precedence_is_deterministic() {
		let frame = df!("x" => [f64::NAN], "y" => ["not quantitative"]).unwrap();
		let result = ResultArtifact::test_frame(frame.clone());
		assert!(matches!(
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()),
			Err(HexbinPreparationError::UnsupportedColumn {
				channel: HexbinChannel::Y,
				..
			})
		));

		let frame = df!("x" => [Some(1.0_f64), None], "y" => [f64::NAN, 2.0]).unwrap();
		let result = ResultArtifact::test_frame(frame.clone());
		assert!(matches!(
			prepare_hexbin_observations(&frame, "x", "y", result.artifact_token()),
			Err(HexbinPreparationError::NonFiniteValue {
				channel: HexbinChannel::Y,
				row: 1,
				..
			})
		));
	}

	#[test]
	fn product_0143_artifacts_refuse_other_sources_and_lattices() {
		let frame = df!("x" => [0.5_f64], "y" => [0.5_f64]).unwrap();
		let first = observations(frame.clone());
		let second = observations(frame);
		let first_request = request(&first, (0.0, 1.0), (0.0, 1.0), 100.0, 100.0);
		let other_lattice = request(&first, (0.0, 1.0), (0.0, 1.0), 200.0, 100.0);
		let artifact = MaterializedHexbinPreparer::new(Arc::clone(&first))
			.prepare(&first_request)
			.unwrap();
		assert!(artifact.matches(&first_request));
		assert!(!artifact.matches(&other_lattice));
		assert_eq!(artifact.generation(), first_request.generation);
		assert!(matches!(
			MaterializedHexbinPreparer::new(second).prepare(&first_request),
			Err(HexbinExecutorError::SourceMismatch)
		));
	}

	#[test]
	fn product_0143_radius_and_lattice_bounds_refuse_invalid_values() {
		for radius in [0.0, f32::NAN, f32::INFINITY, HexRadiusPx::MAX * 2.0] {
			assert!(matches!(
				HexRadiusPx::new(radius),
				Err(HexLatticeError::InvalidRadius(value)) if value.to_bits() == radius.to_bits()
			));
		}
		let lattice = HexLatticeSpec::new(
			(-1.0, 1.0),
			(-2.0, 2.0),
			500.0,
			250.0,
			HexRadiusPx::new(10.0).unwrap(),
		)
		.unwrap();
		assert_eq!(lattice.x_domain(), (-1.0, 1.0));
		assert_eq!(lattice.y_domain(), (-2.0, 2.0));
		assert_eq!(lattice.plot_height_px(), 250.0);
		assert_eq!(lattice.orientation(), HexOrientation::PointyTop);
		assert_eq!(lattice.origin(), HexOrigin::PlotLocalZero);
	}

	#[test]
	fn product_0143_shared_rounding_is_ties_to_even_with_q_priority() {
		let radius = HexRadiusPx::DEFAULT;
		let sqrt_3 = 3.0_f64.sqrt();
		// q_frac = 0.5, r_frac = 0.0, s_frac = -0.5. q and s tie for the
		// largest correction; PRODUCT-0143 gives q priority.
		let x = f64::from(radius.get()) * sqrt_3 / 2.0;
		assert_eq!(nearest_axial(x, 0.0, radius), Some((0, 0)));
	}

	#[test]
	fn product_0143_regular_pixel_hex_has_six_equal_radii() {
		let lattice =
			HexLatticeSpec::new((0.0, 1.0), (0.0, 100.0), 800.0, 200.0, HexRadiusPx::DEFAULT)
				.unwrap();
		let center = lattice.center_for_axial(7, 9);
		let vertices = (0..6)
			.map(|corner| {
				let angle = (60.0 * f64::from(corner) - 30.0).to_radians();
				(
					center.0 + f64::from(HexRadiusPx::DEFAULT.get()) * angle.cos(),
					center.1 + f64::from(HexRadiusPx::DEFAULT.get()) * angle.sin(),
				)
			})
			.collect::<Vec<_>>();
		let distances = vertices
			.iter()
			.map(|vertex| ((vertex.0 - center.0).powi(2) + (vertex.1 - center.1).powi(2)).sqrt())
			.collect::<Vec<_>>();
		assert!(
			distances
				.iter()
				.all(|distance| (distance - 10.0).abs() < 1e-10)
		);
		let side_lengths = (0..6)
			.map(|index| {
				let from = vertices[index];
				let to = vertices[(index + 1) % 6];
				((to.0 - from.0).powi(2) + (to.1 - from.1).powi(2)).sqrt()
			})
			.collect::<Vec<_>>();
		assert!(
			side_lengths
				.iter()
				.all(|distance| (distance - 10.0).abs() < 1e-10)
		);
	}

	#[test]
	fn product_0143_radius_stays_screen_space_while_data_extent_changes() {
		let normalized =
			HexLatticeSpec::new((0.0, 1.0), (0.0, 1.0), 500.0, 500.0, HexRadiusPx::DEFAULT)
				.unwrap();
		let wide_domain = HexLatticeSpec::new(
			(0.0, 100.0),
			(0.0, 10.0),
			500.0,
			500.0,
			HexRadiusPx::DEFAULT,
		)
		.unwrap();
		assert_eq!(normalized.radius_px(), wide_domain.radius_px());
		let normalized_extent =
			normalized.data_for_pixel(10.0, 0.0).0 - normalized.data_for_pixel(0.0, 0.0).0;
		let wide_extent =
			wide_domain.data_for_pixel(10.0, 0.0).0 - wide_domain.data_for_pixel(0.0, 0.0).0;
		assert!(wide_extent > normalized_extent * 50.0);
	}
}
