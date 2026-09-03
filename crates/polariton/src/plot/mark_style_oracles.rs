use crate::plot::common::{
	AxisType, CoordinateTransformer, PathBuilder, PlotBackend, PlotKernel, ResolvedPlotSettings,
};
use crate::plot::kernels::boxplot::{BoxPlotKernel, BoxPlotPreparedData};
use crate::plot::kernels::boxplot_tukey::{TukeyBoxRow, TukeyBoxes};
use crate::plot::kernels::candlestick::{CandlestickPlotKernel, CandlestickPreparedData};
use crate::plot::kernels::funnel::{FunnelPlotKernel, prepare_funnel_data};
use crate::plot::kernels::stacked_area::{StackedAreaPlotKernel, StackedAreaPreparedData};
use crate::plot::kernels::violin::ViolinPlotKernel;
use crate::plot::kernels::violin_density::{
	ViolinDensity, ViolinDensityCell, ViolinDensityInputRow,
};
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

	fn fill_text_raw(&mut self, text: Text) {
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
fn product_0080_0082_0083_default_operation_bank_tracks_the_box_role_split() {
	let settings = ResolvedPlotSettings::default();
	let ink = Color::WHITE;
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
			x_temporal_domain: None,
			y_temporal_domain: None,
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
			boxes: TukeyBoxes::new(vec![TukeyBoxRow {
				category: "A".to_owned(),
				whisker_low: 0.0,
				q1: 1.0,
				median: 2.0,
				q3: 3.0,
				whisker_high: 4.0,
				outlier: None,
			}])
			.expect("valid coincident Tukey box"),
			y_range: (0.0, 4.0),
			y_axis_type: AxisType::Linear,
			x_label: String::new(),
			y_label: String::new(),
			selection_candidates: Default::default(),
		}),
	};
	assert_eq!(
		render(&boxplot),
		vec![
			BankOperation::FillRectangle(settings.mark_style.fill.iced()),
			BankOperation::Stroke(settings.mark_style.fill_detail.iced(), 1.0_f32.to_bits()),
			BankOperation::Stroke(settings.mark_style.fill_detail.iced(), 3.5_f32.to_bits()),
			BankOperation::Stroke(ink, 1.0_f32.to_bits()),
			BankOperation::Stroke(ink, 3.5_f32.to_bits()),
		]
	);

	let violin = ViolinPlotKernel {
		prepared_data: Arc::new(
			ViolinDensity::from_rows(vec![
				ViolinDensityInputRow {
					group: Some("A".to_owned()),
					evaluation_value: ViolinDensityCell::Exact(0.0),
					density: ViolinDensityCell::Exact(0.5),
					median: ViolinDensityCell::Exact(1.0),
				},
				ViolinDensityInputRow {
					group: Some("A".to_owned()),
					evaluation_value: ViolinDensityCell::Exact(1.0),
					density: ViolinDensityCell::Exact(1.0),
					median: ViolinDensityCell::Exact(1.0),
				},
				ViolinDensityInputRow {
					group: Some("A".to_owned()),
					evaluation_value: ViolinDensityCell::Exact(2.0),
					density: ViolinDensityCell::Exact(0.5),
					median: ViolinDensityCell::Exact(1.0),
				},
			])
			.expect("valid violin density fixture"),
		),
		category_colors: None,
		x_label: String::new(),
		y_label: String::new(),
		selection_candidates: Default::default(),
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
			x_temporal_domain: None,
			x_label: String::new(),
			y_label: "open / high / low / close".to_owned(),
			minimum_x_delta: None,
			selection_candidates: Vec::new(),
		}),
	};
	assert_eq!(
		render(&candlestick),
		vec![
			BankOperation::Stroke(settings.mark_style.decoration.iced(), 3.0_f32.to_bits()),
			BankOperation::Stroke(settings.mark_style.fill.iced(), 1.0_f32.to_bits()),
		]
	);

	let funnel_frame = polars::df!("stage" => ["A", "B"], "value" => [10.0_f64, 5.0])
		.expect("funnel oracle frame");
	let funnel = FunnelPlotKernel {
		prepared_data: Arc::new(
			prepare_funnel_data(&funnel_frame, "stage", "value").expect("valid funnel oracle"),
		),
		stage_colors: None,
	};
	assert_eq!(
		render(&funnel),
		vec![
			BankOperation::FillPath(settings.color_theme.get_color(0.0)),
			BankOperation::Text(ink),
			BankOperation::FillPath(settings.color_theme.get_color(1.0)),
			BankOperation::Text(ink),
			BankOperation::Stroke(settings.mark_style.separator.iced(), 1.0_f32.to_bits()),
			BankOperation::Stroke(settings.mark_style.fill_detail.iced(), 1.0_f32.to_bits()),
		]
	);
}
