/// Complete renderer vocabulary. PRODUCT-0075 retired the GUI picker but kept
/// every kernel; variants without a Rune producer remain direct-test substrate.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub enum PlotType {
	Bar,
	BoxPlot,
	Bubble,
	Candlestick,
	FillBetween,
	Funnel,
	Heatmap,
	Hexbin,
	Histogram,
	HorizontalBar,
	HorizontalStackedBar,
	Line,
	Parallel,
	Pie,
	Radar,
	RadialDial,
	Scatter,
	StackedArea,
	StackedBar,
	Violin,
}

impl PlotType {
	/// Exhaustive renderer catalog, guarded against drift from the enum by the
	/// PRODUCT-0075 architecture test. It is not picker data.
	#[allow(dead_code)]
	pub const ALL: [PlotType; 20] = [
		PlotType::Bar,
		PlotType::BoxPlot,
		PlotType::Bubble,
		PlotType::Candlestick,
		PlotType::FillBetween,
		PlotType::Funnel,
		PlotType::Heatmap,
		PlotType::Hexbin,
		PlotType::Histogram,
		PlotType::HorizontalBar,
		PlotType::HorizontalStackedBar,
		PlotType::Line,
		PlotType::Parallel,
		PlotType::Pie,
		PlotType::Radar,
		PlotType::RadialDial,
		PlotType::Scatter,
		PlotType::StackedArea,
		PlotType::StackedBar,
		PlotType::Violin,
	];

	/// Minimum field count required for the mark to possess drawable geometry.
	/// Shared by authored and automatic multivariate resolution (PRODUCT-0105).
	pub(crate) const fn minimum_multivariate_dimensions(self) -> Option<usize> {
		match self {
			Self::Radar => Some(3),
			Self::Parallel => Some(2),
			_ => None,
		}
	}

	pub(crate) fn validate_multivariate_dimension_count(self, count: usize) -> Result<(), String> {
		let Some(minimum) = self.minimum_multivariate_dimensions() else {
			return Err(format!("{self} is not a multivariate mark"));
		};
		(count >= minimum).then_some(()).ok_or_else(|| {
			format!("{self} requires at least {minimum} numeric dimensions; found {count}")
		})
	}
}

impl std::fmt::Display for PlotType {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			PlotType::Bar => write!(f, "Bar Plot"),
			PlotType::BoxPlot => write!(f, "Box Plot"),
			PlotType::Bubble => write!(f, "Bubble Plot"),
			PlotType::Candlestick => write!(f, "Candlestick Plot"),
			PlotType::FillBetween => write!(f, "Fill Between Plot"),
			PlotType::Funnel => write!(f, "Funnel Plot"),
			PlotType::Heatmap => write!(f, "Heatmap"),
			PlotType::Hexbin => write!(f, "Hexbin Plot"),
			PlotType::Histogram => write!(f, "Histogram Plot"),
			PlotType::HorizontalBar => write!(f, "Horizontal Bar Plot"),
			PlotType::HorizontalStackedBar => write!(f, "Horizontal Stacked Bar Plot"),
			PlotType::Line => write!(f, "Line Plot"),
			PlotType::Parallel => write!(f, "Parallel Coordinates Plot"),
			PlotType::Pie => write!(f, "Pie Plot"),
			PlotType::Radar => write!(f, "Radar Plot"),
			PlotType::RadialDial => write!(f, "Radial Dial Plot"),
			PlotType::Scatter => write!(f, "Scatter Plot"),
			PlotType::StackedArea => write!(f, "Stacked Area Plot"),
			PlotType::StackedBar => write!(f, "Stacked Bar Plot"),
			PlotType::Violin => write!(f, "Violin Plot"),
		}
	}
}
