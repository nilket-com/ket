#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
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
