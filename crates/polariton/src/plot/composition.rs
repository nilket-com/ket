use crate::plot::core::PlotType;

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlotComposition {
	pub layers: Vec<PlotLayer>,
}

impl PlotComposition {
	pub fn single(plot_type: PlotType) -> Self {
		Self {
			layers: vec![PlotLayer::builtin(plot_type)],
		}
	}

	pub fn primary_plot_type(&self) -> PlotType {
		self.layers
			.first()
			.map(|layer| layer.plot_type)
			.unwrap_or(PlotType::Line)
	}

	pub fn set_single(&mut self, plot_type: PlotType) {
		self.layers.clear();
		self.layers.push(PlotLayer::builtin(plot_type));
	}

	pub fn can_add_layer_type(&self, plot_type: PlotType) -> bool {
		match (self.primary_plot_type(), plot_type) {
			(PlotType::Line, PlotType::FillBetween) => !self
				.layers
				.iter()
				.any(|layer| layer.plot_type == PlotType::FillBetween),
			(PlotType::Line, PlotType::Scatter) => !self
				.layers
				.iter()
				.any(|layer| layer.plot_type == PlotType::Scatter),
			(PlotType::FillBetween, PlotType::Line) => !self.has_line_overlay(),
			(PlotType::Scatter, PlotType::Line) => !self.has_line_overlay(),
			_ => false,
		}
	}

	pub fn add_layer(&mut self, plot_type: PlotType) -> bool {
		if !self.can_add_layer_type(plot_type) {
			return false;
		}
		if self.primary_plot_type() == PlotType::Line && plot_type == PlotType::FillBetween {
			self.layers = vec![
				PlotLayer::builtin(PlotType::FillBetween),
				PlotLayer::builtin(PlotType::Line),
			];
			return true;
		}
		if self.primary_plot_type() == PlotType::Line && plot_type == PlotType::Scatter {
			self.layers = vec![
				PlotLayer::builtin(PlotType::Scatter),
				PlotLayer::builtin(PlotType::Line),
			];
			return true;
		}
		self.layers.push(PlotLayer::builtin(plot_type));
		true
	}

	pub fn has_line_overlay(&self) -> bool {
		self.layers
			.iter()
			.skip(1)
			.any(|layer| layer.plot_type == PlotType::Line)
	}

	pub fn is_scatter_with_line_overlay(&self) -> bool {
		self.primary_plot_type() == PlotType::Scatter && self.has_line_overlay()
	}

	pub fn is_fill_between_with_line_overlay(&self) -> bool {
		self.primary_plot_type() == PlotType::FillBetween && self.has_line_overlay()
	}

	pub fn title(&self) -> String {
		let mut labels = self
			.layers
			.iter()
			.map(|layer| layer.plot_type.to_string())
			.collect::<Vec<_>>();
		labels.dedup();
		labels.join(" + ")
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct PlotLayer {
	pub plot_type: PlotType,
}

impl PlotLayer {
	pub fn builtin(plot_type: PlotType) -> Self {
		Self { plot_type }
	}
}
