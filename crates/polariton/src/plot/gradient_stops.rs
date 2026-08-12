use iced::Color;

/// Validated positioned colours for a continuous guide.
///
/// This type is public because it is carried by the public `LegendSection`
/// vocabulary. Its representation and construction stay crate-owned so every
/// renderer receives a finite, strictly ascending sequence.
#[derive(Clone, Debug, PartialEq)]
pub struct GradientStops {
	stops: Vec<GradientStop>,
	sampling: GradientSampling,
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct GradientStop {
	value: f64,
	position: f32,
	color: Color,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum GradientSampling {
	/// Arbitrary validated data stops use their normalized positions.
	Positioned,
	/// Canonical ramps retain the mark sampler's multiply-and-fract arithmetic.
	Even,
	/// Legacy reversed ramps retain their `base(1.0 - t)` arithmetic exactly.
	EvenReversed,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum GradientStopsError {
	TooFew,
	NonFinite { index: usize },
	NotAscending { index: usize },
}

impl GradientStops {
	pub(crate) fn new(stops: Vec<(f64, Color)>) -> Result<Self, GradientStopsError> {
		Self::validate(stops, GradientSampling::Positioned)
	}

	pub(crate) fn projected(
		low: f64,
		high: f64,
		canonical: Vec<(f32, Color)>,
		reversed_sampling: bool,
	) -> Result<Self, GradientStopsError> {
		let positions: Vec<f32> = canonical.iter().map(|(position, _)| *position).collect();
		let stops = canonical
			.into_iter()
			.map(|(position, color)| (low + f64::from(position) * (high - low), color))
			.collect();
		let mut projected = Self::new(stops)?;
		for (stop, position) in projected.stops.iter_mut().zip(positions) {
			stop.position = position;
		}
		projected.sampling = if reversed_sampling {
			GradientSampling::EvenReversed
		} else {
			GradientSampling::Even
		};
		Ok(projected)
	}

	fn validate(
		stops: Vec<(f64, Color)>,
		sampling: GradientSampling,
	) -> Result<Self, GradientStopsError> {
		if stops.len() < 2 {
			return Err(GradientStopsError::TooFew);
		}
		let mut validated = Vec::with_capacity(stops.len());
		for (index, (value, color)) in stops.into_iter().enumerate() {
			if !value.is_finite() {
				return Err(GradientStopsError::NonFinite { index });
			}
			if validated
				.last()
				.is_some_and(|previous: &(f64, Color)| value <= previous.0)
			{
				return Err(GradientStopsError::NotAscending { index });
			}
			validated.push((value, color));
		}
		let low = validated[0].0;
		let high = validated[validated.len() - 1].0;
		let span = high - low;
		let stops = validated
			.into_iter()
			.map(|(value, color)| GradientStop {
				value,
				position: ((value - low) / span) as f32,
				color,
			})
			.collect();
		Ok(Self { stops, sampling })
	}

	#[cfg(test)]
	pub(crate) fn len(&self) -> usize {
		self.stops.len()
	}

	pub(crate) fn first_value(&self) -> f64 {
		self.stops[0].value
	}

	pub(crate) fn last_value(&self) -> f64 {
		self.stops[self.stops.len() - 1].value
	}

	#[cfg(test)]
	pub(crate) fn values(&self) -> impl Iterator<Item = f64> + '_ {
		self.stops.iter().map(|stop| stop.value)
	}

	pub(crate) fn sample_normalized(&self, t: f32) -> Color {
		let t = t.clamp(0.0, 1.0);
		match self.sampling {
			GradientSampling::Positioned => self.sample_positioned(t),
			GradientSampling::Even => self.sample_even(t),
			GradientSampling::EvenReversed => self.sample_reversed(t),
		}
	}

	fn sample_positioned(&self, t: f32) -> Color {
		if t <= self.stops[0].position {
			return self.stops[0].color;
		}
		let last = self.stops.len() - 1;
		if t >= self.stops[last].position {
			return self.stops[last].color;
		}
		let upper = self.stops.partition_point(|stop| stop.position < t);
		let lower = upper - 1;
		let a = self.stops[lower];
		let b = self.stops[upper];
		let t = (t - a.position) / (b.position - a.position);
		lerp_color(a.color, b.color, t)
	}

	fn sample_even(&self, t: f32) -> Color {
		let last = self.stops.len() - 1;
		let scaled = t * last as f32;
		let index = scaled.floor() as usize;
		if index >= last {
			return self.stops[last].color;
		}
		lerp_color(
			self.stops[index].color,
			self.stops[index + 1].color,
			scaled.fract(),
		)
	}

	fn sample_reversed(&self, t: f32) -> Color {
		let last = self.stops.len() - 1;
		let scaled = (1.0 - t) * last as f32;
		let index = scaled.floor() as usize;
		if index >= last {
			return self.stops[0].color;
		}
		let fraction = scaled.fract();
		let a = self.stops[last - index].color;
		let b = self.stops[last - index - 1].color;
		lerp_color(a, b, fraction)
	}
}

fn lerp_color(a: Color, b: Color, t: f32) -> Color {
	Color {
		r: a.r + (b.r - a.r) * t,
		g: a.g + (b.g - a.g) * t,
		b: a.b + (b.b - a.b) * t,
		a: a.a + (b.a - a.a) * t,
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0087_stop_validation_is_total_and_constant_ramps_survive() {
		let color = Color::from_rgb(0.2, 0.3, 0.4);
		assert_eq!(GradientStops::new(vec![]), Err(GradientStopsError::TooFew));
		assert_eq!(
			GradientStops::new(vec![(0.0, color)]),
			Err(GradientStopsError::TooFew)
		);
		assert_eq!(
			GradientStops::new(vec![(0.0, color), (f64::NAN, color)]),
			Err(GradientStopsError::NonFinite { index: 1 })
		);
		assert_eq!(
			GradientStops::new(vec![(1.0, color), (0.0, color)]),
			Err(GradientStopsError::NotAscending { index: 1 })
		);
		assert_eq!(
			GradientStops::new(vec![(1.0, color), (1.0, color)]),
			Err(GradientStopsError::NotAscending { index: 1 })
		);
		let constant = GradientStops::new(vec![(0.0, color), (1.0, color)])
			.expect("distinct positions may carry one constant colour");
		assert_eq!(constant.sample_normalized(0.5), color);
	}
}
