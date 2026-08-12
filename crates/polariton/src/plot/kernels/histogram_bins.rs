use std::fmt;

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramBin {
	pub start: f64,
	pub end: f64,
	pub count: u64,
}

#[derive(Clone, Debug, PartialEq)]
pub struct HistogramBins {
	bins: Vec<HistogramBin>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HistogramBinsError {
	Unordered { index: usize },
	Overlapping { index: usize },
	EmptyInterval { index: usize },
	ReversedInterval { index: usize },
	NonFiniteStart { index: usize },
	NonFiniteEnd { index: usize },
}

impl HistogramBins {
	pub fn new(bins: Vec<HistogramBin>) -> Result<Self, HistogramBinsError> {
		for (index, bin) in bins.iter().enumerate() {
			if !bin.start.is_finite() {
				return Err(HistogramBinsError::NonFiniteStart { index });
			}
			if !bin.end.is_finite() {
				return Err(HistogramBinsError::NonFiniteEnd { index });
			}
			if bin.start == bin.end {
				return Err(HistogramBinsError::EmptyInterval { index });
			}
			if bin.start > bin.end {
				return Err(HistogramBinsError::ReversedInterval { index });
			}
			if let Some(previous) = index.checked_sub(1).and_then(|index| bins.get(index)) {
				if bin.start < previous.start {
					return Err(HistogramBinsError::Unordered { index });
				}
				if bin.start < previous.end {
					return Err(HistogramBinsError::Overlapping { index });
				}
			}
		}
		Ok(Self { bins })
	}

	pub fn iter(&self) -> impl Iterator<Item = &HistogramBin> {
		self.bins.iter()
	}

	pub fn find(&self, value: f64) -> Option<&HistogramBin> {
		self.bins
			.iter()
			.find(|bin| value >= bin.start && value < bin.end)
	}

	pub fn ranges(&self) -> ((f64, f64), (f64, f64)) {
		let x_range = self
			.bins
			.first()
			.zip(self.bins.last())
			.map(|(first, last)| (first.start, last.end))
			.unwrap_or((0.0, 1.0));
		let max_count = self.bins.iter().map(|bin| bin.count).max().unwrap_or(0);
		let y_range = if max_count == 0 {
			(0.0, 1.0)
		} else {
			(0.0, max_count as f64)
		};
		(x_range, y_range)
	}

	pub fn max_positive_count(&self) -> Option<u64> {
		self.bins
			.iter()
			.map(|bin| bin.count)
			.max()
			.filter(|count| *count > 0)
	}
}

impl fmt::Display for HistogramBinsError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		let (invariant, index) = match self {
			Self::Unordered { index } => ("unordered interval", index),
			Self::Overlapping { index } => ("overlapping interval", index),
			Self::EmptyInterval { index } => ("empty interval", index),
			Self::ReversedInterval { index } => ("reversed interval", index),
			Self::NonFiniteStart { index } => ("non-finite interval start", index),
			Self::NonFiniteEnd { index } => ("non-finite interval end", index),
		};
		write!(formatter, "histogram bin {index} has {invariant}")
	}
}

impl std::error::Error for HistogramBinsError {}
