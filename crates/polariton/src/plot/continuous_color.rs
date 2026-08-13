use crate::plot::colors::ColorTheme;
use crate::plot::spec::ColorJob;
use crate::plot::theme::PlotThemeSpec;
use iced::Color;
use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct ContinuousRamps {
	pub sequential: ColorTheme,
	pub diverging: ColorTheme,
}

impl ContinuousRamps {
	pub fn from_theme(theme: PlotThemeSpec) -> Self {
		Self {
			sequential: theme.sequential_ramp,
			diverging: theme.diverging_ramp,
		}
	}

	pub fn ramp(self, job: ColorJob) -> Option<ColorTheme> {
		match job {
			ColorJob::Sequential => Some(self.sequential),
			ColorJob::Diverging => Some(self.diverging),
			ColorJob::Categorical | ColorJob::Status => None,
		}
	}
}

impl Default for ContinuousRamps {
	fn default() -> Self {
		Self::from_theme(PlotThemeSpec::default())
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
#[allow(dead_code)] // Diverging and authored origins arrive with CI-02; PRODUCT-0089 gates them now.
pub enum ContinuousAnchor {
	None,
	DefaultZero,
	Authored(f64),
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ContinuousScaleIntent {
	pub job: ColorJob,
	pub anchor: ContinuousAnchor,
}

impl ContinuousScaleIntent {
	pub const SEQUENTIAL: Self = Self {
		job: ColorJob::Sequential,
		anchor: ContinuousAnchor::None,
	};
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ContinuousColorScale {
	domain: (f64, f64),
	job: ColorJob,
	ramp: ColorTheme,
	anchor: ContinuousAnchor,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub enum ContinuousColorScaleError {
	NonContinuousJob(ColorJob),
	NonFiniteDomain { low: f64, high: f64 },
	DescendingDomain { low: f64, high: f64 },
	SequentialAnchor(ContinuousAnchor),
	NonFiniteAuthoredAnchor(f64),
	AuthoredAnchorOutsideDomain { anchor: f64, low: f64, high: f64 },
	AuthoredAnchorOnDegenerateDomain { anchor: f64, value: f64 },
}

impl ContinuousColorScale {
	pub fn new(
		domain: (f64, f64),
		intent: ContinuousScaleIntent,
		ramps: ContinuousRamps,
	) -> Result<Self, ContinuousColorScaleError> {
		let ramp = ramps
			.ramp(intent.job)
			.ok_or(ContinuousColorScaleError::NonContinuousJob(intent.job))?;
		let (low, high) = domain;
		if !low.is_finite() || !high.is_finite() {
			return Err(ContinuousColorScaleError::NonFiniteDomain { low, high });
		}
		if low > high {
			return Err(ContinuousColorScaleError::DescendingDomain { low, high });
		}
		match (intent.job, intent.anchor) {
			(ColorJob::Sequential, ContinuousAnchor::None) => {}
			(ColorJob::Sequential, anchor) => {
				return Err(ContinuousColorScaleError::SequentialAnchor(anchor));
			}
			(ColorJob::Diverging, ContinuousAnchor::Authored(anchor)) => {
				if !anchor.is_finite() {
					return Err(ContinuousColorScaleError::NonFiniteAuthoredAnchor(anchor));
				}
				if low == high {
					return Err(
						ContinuousColorScaleError::AuthoredAnchorOnDegenerateDomain {
							anchor,
							value: low,
						},
					);
				}
				if anchor <= low || anchor >= high {
					return Err(ContinuousColorScaleError::AuthoredAnchorOutsideDomain {
						anchor,
						low,
						high,
					});
				}
			}
			(ColorJob::Diverging, ContinuousAnchor::None | ContinuousAnchor::DefaultZero) => {}
			(ColorJob::Categorical | ColorJob::Status, _) => {
				unreachable!("non-continuous jobs are refused before anchor validation")
			}
		}
		Ok(Self {
			domain,
			job: intent.job,
			ramp,
			anchor: intent.anchor,
		})
	}

	pub fn ramp(&self) -> ColorTheme {
		self.ramp
	}

	pub fn is_degenerate(&self) -> bool {
		self.domain.0 == self.domain.1
	}

	fn active_anchor(&self) -> Option<f64> {
		match self.anchor {
			ContinuousAnchor::Authored(anchor) => Some(anchor),
			ContinuousAnchor::DefaultZero if self.domain.0 < 0.0 && 0.0 < self.domain.1 => {
				Some(0.0)
			}
			ContinuousAnchor::None | ContinuousAnchor::DefaultZero => None,
		}
	}

	pub fn normalize(&self, value: f64) -> f64 {
		assert!(
			value.is_finite(),
			"PRODUCT-0089 requires finite values at the continuous colour sampling boundary"
		);
		let (low, high) = self.domain;
		if low == high {
			return 0.0;
		}
		let value = value.clamp(low, high);
		if let Some(anchor) = self.active_anchor() {
			if value <= anchor {
				0.5 * (value - low) / (anchor - low)
			} else {
				0.5 + 0.5 * (value - anchor) / (high - anchor)
			}
		} else {
			(value - low) / (high - low)
		}
	}

	pub fn value_at_normalized(&self, t: f64) -> f64 {
		assert!(
			t.is_finite(),
			"PRODUCT-0089 requires finite normalized guide positions"
		);
		let (low, high) = self.domain;
		if low == high {
			return low;
		}
		let t = t.clamp(0.0, 1.0);
		if let Some(anchor) = self.active_anchor() {
			if t <= 0.5 {
				low + (t / 0.5) * (anchor - low)
			} else {
				anchor + ((t - 0.5) / 0.5) * (high - anchor)
			}
		} else {
			low + t * (high - low)
		}
	}

	pub fn color(&self, value: f64) -> Color {
		self.ramp.get_color(self.normalize(value) as f32)
	}
}

impl fmt::Display for ContinuousColorScaleError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::NonContinuousJob(job) => {
				write!(formatter, "{job:?} is not a continuous colour job")
			}
			Self::NonFiniteDomain { low, high } => {
				write!(
					formatter,
					"continuous colour domain {low}..{high} is not finite"
				)
			}
			Self::DescendingDomain { low, high } => {
				write!(
					formatter,
					"continuous colour domain {low}..{high} is descending"
				)
			}
			Self::SequentialAnchor(anchor) => {
				write!(
					formatter,
					"sequential colour does not admit anchor {anchor:?}"
				)
			}
			Self::NonFiniteAuthoredAnchor(anchor) => {
				write!(
					formatter,
					"authored continuous colour anchor {anchor} is not finite"
				)
			}
			Self::AuthoredAnchorOutsideDomain { anchor, low, high } => write!(
				formatter,
				"authored continuous colour anchor {anchor} must be strictly inside {low}..{high}"
			),
			Self::AuthoredAnchorOnDegenerateDomain { anchor, value } => write!(
				formatter,
				"authored continuous colour anchor {anchor} cannot apply to degenerate domain {value}"
			),
		}
	}
}

impl std::error::Error for ContinuousColorScaleError {}

#[cfg(test)]
mod tests {
	use super::*;

	fn scale(domain: (f64, f64), anchor: ContinuousAnchor) -> ContinuousColorScale {
		ContinuousColorScale::new(
			domain,
			ContinuousScaleIntent {
				job: ColorJob::Diverging,
				anchor,
			},
			ContinuousRamps {
				sequential: ColorTheme::Viridis,
				diverging: ColorTheme::GreenPurples,
			},
		)
		.unwrap()
	}

	#[test]
	fn product_0089_piecewise_scale_uses_both_arms_and_round_trips() {
		let scale = scale((-50.0, 100.0), ContinuousAnchor::DefaultZero);
		assert_eq!(scale.normalize(-50.0), 0.0);
		assert_eq!(scale.normalize(-25.0), 0.25);
		assert_eq!(scale.normalize(0.0), 0.5);
		assert_eq!(scale.normalize(25.0), 0.625);
		assert_eq!(scale.normalize(100.0), 1.0);
		for (t, value) in [
			(0.0, -50.0),
			(0.25, -25.0),
			(0.5, 0.0),
			(0.75, 50.0),
			(1.0, 100.0),
		] {
			assert_eq!(scale.value_at_normalized(t), value);
		}
		let budget = 4.0 * f64::EPSILON * 150.0;
		for value in [-37.0, -1.0, 1.0, 73.0] {
			let round_trip = scale.value_at_normalized(scale.normalize(value));
			assert!(
				(round_trip - value).abs() <= budget,
				"{value}: {round_trip}"
			);
		}
	}

	#[test]
	fn product_0089_jobs_anchor_provenance_and_degenerate_domains_are_total() {
		let ramps = ContinuousRamps::default();
		let theme = PlotThemeSpec::default();
		assert_eq!(ramps.sequential, theme.sequential_ramp);
		assert_eq!(ramps.diverging, theme.diverging_ramp);
		for job in [ColorJob::Categorical, ColorJob::Status] {
			assert_eq!(
				ContinuousColorScale::new(
					(0.0, 1.0),
					ContinuousScaleIntent {
						job,
						anchor: ContinuousAnchor::None
					},
					ramps,
				),
				Err(ContinuousColorScaleError::NonContinuousJob(job))
			);
		}
		for job in [ColorJob::Sequential, ColorJob::Diverging] {
			assert!(
				ContinuousColorScale::new(
					(0.0, 1.0),
					ContinuousScaleIntent {
						job,
						anchor: ContinuousAnchor::None
					},
					ramps,
				)
				.is_ok()
			);
		}
		for anchor in [
			ContinuousAnchor::DefaultZero,
			ContinuousAnchor::Authored(0.5),
		] {
			assert!(matches!(
				ContinuousColorScale::new(
					(0.0, 1.0),
					ContinuousScaleIntent {
						job: ColorJob::Sequential,
						anchor
					},
					ramps,
				),
				Err(ContinuousColorScaleError::SequentialAnchor(_))
			));
		}
		let fallback = scale((10.0, 50.0), ContinuousAnchor::DefaultZero);
		assert_eq!(fallback.normalize(30.0), 0.5);
		for anchor in [-1.0, 0.0, 0.5, 1.0, 2.0] {
			let result = ContinuousColorScale::new(
				(0.0, 1.0),
				ContinuousScaleIntent {
					job: ColorJob::Diverging,
					anchor: ContinuousAnchor::Authored(anchor),
				},
				ramps,
			);
			if anchor == 0.5 {
				assert!(result.is_ok());
			} else {
				assert!(matches!(
					result,
					Err(ContinuousColorScaleError::AuthoredAnchorOutsideDomain { .. })
				));
			}
		}
		for anchor in [ContinuousAnchor::None, ContinuousAnchor::DefaultZero] {
			let scale = ContinuousColorScale::new(
				(4.0, 4.0),
				ContinuousScaleIntent {
					job: ColorJob::Diverging,
					anchor,
				},
				ramps,
			)
			.unwrap();
			assert_eq!(scale.normalize(4.0), 0.0);
		}
		assert!(matches!(
			ContinuousColorScale::new(
				(4.0, 4.0),
				ContinuousScaleIntent {
					job: ColorJob::Diverging,
					anchor: ContinuousAnchor::Authored(4.0),
				},
				ramps,
			),
			Err(ContinuousColorScaleError::AuthoredAnchorOnDegenerateDomain { .. })
		));
		for domain in [(f64::NAN, 1.0), (0.0, f64::INFINITY)] {
			assert!(matches!(
				ContinuousColorScale::new(domain, ContinuousScaleIntent::SEQUENTIAL, ramps,),
				Err(ContinuousColorScaleError::NonFiniteDomain { .. })
			));
		}
		assert!(matches!(
			ContinuousColorScale::new((2.0, 1.0), ContinuousScaleIntent::SEQUENTIAL, ramps,),
			Err(ContinuousColorScaleError::DescendingDomain { .. })
		));
	}

	#[test]
	fn product_0089_outside_values_clamp_and_nonfinite_values_panic() {
		let scale = scale((0.0, 1.0), ContinuousAnchor::DefaultZero);
		assert_eq!(scale.normalize(-1e300), 0.0);
		assert_eq!(scale.normalize(1e300), 1.0);
		for value in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
			let panic = std::panic::catch_unwind(|| scale.normalize(value))
				.expect_err("non-finite values violate the prepared-data invariant");
			let message = panic
				.downcast_ref::<String>()
				.map(String::as_str)
				.or_else(|| panic.downcast_ref::<&str>().copied())
				.unwrap_or("non-string panic");
			assert!(message.contains("PRODUCT-0089"), "{message}");
		}
	}
}
