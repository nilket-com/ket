use crate::plot::common::{
	AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel, ResolvedPlotSettings,
};
use crate::plot::kernels::boxplot::{BoxPlotKernel, BoxPlotPreparedData, BoxStats};
use crate::plot::kernels::candlestick::{CandlestickPlotKernel, CandlestickPreparedData};
use crate::plot::kernels::funnel::{FunnelPlotKernel, FunnelPreparedData};
use crate::plot::kernels::stacked_area::{StackedAreaPlotKernel, StackedAreaPreparedData};
use crate::plot::kernels::violin::{ViolinPlotKernel, ViolinPreparedData};
use crate::plot::legend::LegendProvenance;
use crate::plot::spec::StackNormalization;
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::{Stroke, Style, Text};
use iced::{Color, Point, Rectangle, Size, Vector};
use std::sync::Arc;

#[derive(Clone, Debug, PartialEq)]
enum BankOperation {
	FillPath(Color),
	FillRectangle(Color),
	Stroke(Color, u32),
	Text(Color),
}

#[derive(Default)]
struct BankBackend {
	operations: Vec<BankOperation>,
}

impl PlotBackend for BankBackend {
	fn stroke_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), stroke: Stroke) {
		let color = match stroke.style {
			Style::Solid(color) => color,
			Style::Gradient(_) => panic!("PRODUCT-0080 bank contains only solid legacy strokes"),
		};
		self.operations
			.push(BankOperation::Stroke(color, stroke.width.to_bits()));
	}

	fn fill_path(&mut self, _f: &dyn Fn(&mut dyn PathBuilder), color: Color) {
		self.operations.push(BankOperation::FillPath(color));
	}

	fn fill_rectangle(&mut self, _top_left: Point, _size: Size, color: Color) {
		self.operations.push(BankOperation::FillRectangle(color));
	}

	fn fill_text(&mut self, text: Text) {
		self.operations.push(BankOperation::Text(text.color));
	}

	fn translate(&mut self, _translation: Vector) {}
	fn rotate(&mut self, _angle: f32) {}

	fn with_save(&mut self, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		f(self);
	}

	fn with_clip(&mut self, _bounds: Rectangle, f: &mut dyn FnMut(&mut dyn PlotBackend)) {
		f(self);
	}
}

fn render(kernel: &dyn PlotKernel) -> Vec<BankOperation> {
	let settings = ResolvedPlotSettings::default();
	let bounds = Rectangle::new(Point::ORIGIN, Size::new(240.0, 160.0));
	let layout = kernel.layout(settings.clone());
	let transform = CoordinateTransformer::new(&layout, bounds);
	let mut backend = BankBackend::default();
	kernel.plot(
		&mut backend,
		bounds,
		&transform,
		Cursor::Unavailable,
		settings,
	);
	backend.operations
}

#[test]
fn product_0080_five_kernel_default_operation_bank_is_unchanged_and_roles_are_distinct() {
	let settings = ResolvedPlotSettings::default();
	let ink = Color::WHITE;
	let translucent_ink = Color { a: 0.2, ..ink };
	let midpoint = settings.color_theme.get_color(0.5);

	let area = StackedAreaPlotKernel {
		prepared_data: Arc::new(StackedAreaPreparedData {
			categories: vec!["A".to_owned(), "B".to_owned()],
			unique_xs: vec![0.0, 1.0],
			category_values: vec![vec![1.0, 2.0], vec![2.0, 1.0]],
			totals: vec![3.0, 3.0],
			x_range: (0.0, 1.0),
			y_range: (0.0, 3.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_label: String::new(),
			y_label: String::new(),
			series_provenance: LegendProvenance::Named,
		}),
		series_colors: Vec::new(),
		normalization: StackNormalization::Absolute,
	};
	assert_eq!(
		render(&area),
		vec![
			BankOperation::FillPath(settings.color_theme.get_color(0.0)),
			BankOperation::Stroke(ink, 0.5_f32.to_bits()),
			BankOperation::FillPath(settings.color_theme.get_color(1.0)),
			BankOperation::Stroke(ink, 0.5_f32.to_bits()),
		]
	);

	let boxplot = BoxPlotKernel {
		prepared_data: Arc::new(BoxPlotPreparedData {
			categories: vec!["A".to_owned()],
			stats: vec![BoxStats {
				min: 0.0,
				q1: 1.0,
				median: 2.0,
				q3: 3.0,
				max: 4.0,
			}],
			y_range: (0.0, 4.0),
			y_axis_type: AxisType::Linear,
			x_label: String::new(),
			y_label: String::new(),
		}),
	};
	assert_eq!(
		render(&boxplot),
		vec![
			BankOperation::FillRectangle(midpoint),
			BankOperation::Stroke(ink, 1.0_f32.to_bits()),
			BankOperation::Stroke(ink, 3.5_f32.to_bits()),
			BankOperation::Stroke(ink, 1.0_f32.to_bits()),
			BankOperation::Stroke(ink, 3.5_f32.to_bits()),
		]
	);

	let violin = ViolinPlotKernel {
		prepared_data: Arc::new(ViolinPreparedData {
			categories: vec!["A".to_owned()],
			y_range: (0.0, 2.0),
			medians: vec![1.0],
			kde_data: vec![0.5, 1.0, 0.5],
			tex_height_bins: 3,
			x_label: String::new(),
			y_label: String::new(),
		}),
	};
	assert_eq!(
		render(&violin),
		vec![
			BankOperation::FillPath(midpoint),
			BankOperation::Stroke(ink, 2.5_f32.to_bits()),
			BankOperation::Stroke(ink, 4.0_f32.to_bits()),
		]
	);

	let candlestick = CandlestickPlotKernel {
		prepared_data: Arc::new(CandlestickPreparedData {
			x: vec![0.5],
			open: vec![2.0],
			high: vec![4.0],
			low: vec![1.0],
			close: vec![3.0],
			x_range: (0.0, 1.0),
			y_range: (0.0, 5.0),
			x_axis_type: AxisType::Linear,
			y_axis_type: AxisType::Linear,
			x_label: String::new(),
			y_label: String::new(),
		}),
	};
	let bullish = settings.color_theme.get_color(1.0);
	assert_eq!(
		render(&candlestick),
		vec![
			BankOperation::Stroke(bullish, 3.0_f32.to_bits()),
			BankOperation::FillRectangle(bullish),
			BankOperation::Stroke(ink, 2.0_f32.to_bits()),
		]
	);

	let funnel = FunnelPlotKernel {
		prepared_data: Arc::new(FunnelPreparedData {
			stages: vec!["A".to_owned(), "B".to_owned()],
			values: vec![10.0, 5.0],
			total_max: 10.0,
			x_label: String::new(),
			y_label: String::new(),
		}),
	};
	assert_eq!(
		render(&funnel),
		vec![
			BankOperation::FillPath(settings.color_theme.get_color(0.0)),
			BankOperation::Stroke(translucent_ink, 1.0_f32.to_bits()),
			BankOperation::Text(ink),
			BankOperation::FillPath(settings.color_theme.get_color(1.0)),
			BankOperation::Stroke(translucent_ink, 1.0_f32.to_bits()),
			BankOperation::Text(ink),
		]
	);
}
