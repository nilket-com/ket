//! PRODUCT-0156 cross-surface acceptance evidence.
use super::common::{PlotCanvasState, PlotKernel, PlotRenderLayer, PlotWidget};
use super::kernels::line::{LinePlotKernel, prepare_line_data};
use super::series::SeriesVisibilityAction;
use super::spec::{LegendPosition, Presence};
use super::theme::{PlotSettings, PlotSettingsLayer};
use crate::coordinate::Rect;
use crate::gui::plot_pane::{HostPlotState, PlotPaneMsg};
use iced::advanced::mouse::Cursor;
use iced::widget::canvas::Program;
use iced::{Event, Point, Rectangle, Size, keyboard, mouse};
use std::sync::Arc;

fn line() -> Arc<dyn PlotKernel + Send + Sync> {
	let frame = polars::df!("series" => ["alpha", "alpha", "beta", "beta"], "x" => [0.0, 1.0, 0.0, 1.0], "y" => [0.0, 0.0, 1.0, 1.0]).unwrap();
	Arc::new(LinePlotKernel::new(Arc::new(prepare_line_data(
		&frame, "series", "x", "y",
	))))
}

fn event(
	widget: &PlotWidget<'_>,
	state: &mut PlotCanvasState,
	event: Event,
	bounds: Rectangle,
	point: Point,
) -> Option<PlotPaneMsg> {
	Program::update(widget, state, &event, bounds, Cursor::Available(point))
		.and_then(|action| action.into_inner().0)
}

#[test]
fn product_0156_legend_gestures_and_direct_actions_agree() {
	for overlay in [false, true] {
		let kernel = line();
		let mut authored = PlotSettingsLayer::default();
		if overlay {
			authored.legend = Presence::Show(LegendPosition::new(0.5, 0.5).unwrap());
		}
		let mut host =
			HostPlotState::new("figure".to_owned(), Arc::clone(&kernel), authored.clone());
		let mut direct = HostPlotState::new("figure".to_owned(), Arc::clone(&kernel), authored);
		let identities = kernel.series_identities();
		// Non-zero window origin guards against mixing window and widget coordinates.
		let bounds = Rectangle::new(Point::new(40.0, 70.0), Size::new(800.0, 600.0));
		for (shift, expected) in [
			(false, SeriesVisibilityAction::Hide(identities[0].clone())),
			(false, SeriesVisibilityAction::Show(identities[0].clone())),
			(true, SeriesVisibilityAction::Isolate(identities[0].clone())),
			(true, SeriesVisibilityAction::RestoreAll),
		] {
			let widget = PlotWidget::new(
				kernel.as_ref(),
				"figure".to_owned(),
				20.0,
				host.plot_settings_model.clone(),
				host.render_revision,
				false,
				PlotRenderLayer::Full,
			)
			.with_viewport_state(host.viewport_stamp, host.viewport_motion);
			let geometry = widget
				.legend_hit_geometry(Rect::from_iced_frame(bounds))
				.unwrap();
			let entry = geometry.entries[0].0;
			let point = Point::new(bounds.x + entry.x + 5.0, bounds.y + entry.y + 5.0);
			let mut canvas = PlotCanvasState::default();
			if shift {
				event(
					&widget,
					&mut canvas,
					Event::Keyboard(keyboard::Event::ModifiersChanged(
						keyboard::Modifiers::SHIFT,
					)),
					bounds,
					point,
				);
			}
			let pressed = event(
				&widget,
				&mut canvas,
				Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)),
				bounds,
				point,
			);
			let released = event(
				&widget,
				&mut canvas,
				Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)),
				bounds,
				point,
			);
			let message = if shift {
				assert!(released.is_none());
				pressed
			} else {
				assert!(pressed.is_none());
				released
			};
			let Some(PlotPaneMsg::SeriesVisibility(action)) = message else {
				panic!("legend owns the press, not pan/brush")
			};
			assert_eq!(action, expected);
			host.update(PlotPaneMsg::SeriesVisibility(action));
			direct.update(PlotPaneMsg::SeriesVisibility(expected));
			assert_eq!(
				host.plot_settings.hidden_series,
				direct.plot_settings.hidden_series
			);
			assert_eq!(host.render_revision, direct.render_revision);
		}
		assert!(host.plot_settings.hidden_series.is_empty());
		let old_revision = host.render_revision;
		host.update(PlotPaneMsg::SeriesVisibility(
			SeriesVisibilityAction::RestoreAll,
		));
		assert_eq!(host.render_revision, old_revision);
	}
}

#[test]
fn product_0156_legend_cancel_and_hidden_legend() {
	let kernel = line();
	let model = PlotSettings::default();
	let widget = PlotWidget::new(
		kernel.as_ref(),
		String::new(),
		20.0,
		model.clone(),
		0,
		false,
		PlotRenderLayer::Full,
	);
	let bounds = Rectangle::new(Point::ORIGIN, Size::new(800.0, 600.0));
	let rect = widget
		.legend_hit_geometry(Rect::from_iced_frame(bounds))
		.unwrap()
		.entries[0]
		.0;
	let point = Point::new(rect.x + 5.0, rect.y + 5.0);
	for lose_focus in [false, true] {
		let mut state = PlotCanvasState::default();
		assert!(
			event(
				&widget,
				&mut state,
				Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)),
				bounds,
				point
			)
			.is_none()
		);
		if lose_focus {
			event(
				&widget,
				&mut state,
				Event::Window(iced::window::Event::Unfocused),
				bounds,
				point,
			);
		}
		assert!(!matches!(
			event(
				&widget,
				&mut state,
				Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)),
				bounds,
				if lose_focus { point } else { Point::ORIGIN }
			),
			Some(PlotPaneMsg::SeriesVisibility(_))
		));
	}
	let mut model = model;
	model.authored_spec.legend = Presence::Hide;
	let widget = PlotWidget::new(
		kernel.as_ref(),
		String::new(),
		20.0,
		model,
		0,
		false,
		PlotRenderLayer::Full,
	);
	assert!(
		widget
			.legend_hit_geometry(Rect::from_iced_frame(bounds))
			.is_none()
	);
}

#[test]
fn product_0156_export_registry_includes_hidden_series_and_pane_reset() {
	let kernel = line();
	let mut host = HostPlotState::new(
		"figure".to_owned(),
		Arc::clone(&kernel),
		PlotSettingsLayer::default(),
	);
	let identities = kernel.series_identities();
	host.update(PlotPaneMsg::SeriesVisibility(SeriesVisibilityAction::Hide(
		identities[0].clone(),
	)));
	let widget = PlotWidget::new(
		kernel.as_ref(),
		"figure".to_owned(),
		20.0,
		host.plot_settings_model.clone(),
		host.render_revision,
		false,
		PlotRenderLayer::Full,
	);
	let svg = super::export::render_svg(
		&widget,
		Rectangle::new(Point::ORIGIN, Size::new(800.0, 600.0)),
	);
	assert_eq!(svg.series.identities(), identities);
	assert!(!svg.svg.contains("data-series=\"s0\""));
	assert!(svg.svg.contains("data-series=\"s1\""));
	host.replace(
		"replacement".to_owned(),
		line(),
		PlotSettingsLayer::default(),
	);
	assert!(host.plot_settings.hidden_series.is_empty());
}

#[test]
fn product_0156_area_keeps_lower_band_gap_and_percent_totals() {
	use super::common::{CoordinateTransformer, ResolvedPlotSettings};
	use super::kernels::stacked_area::{StackedAreaPlotKernel, prepare_stacked_area_data};
	use super::series::SeriesRegistry;
	use super::spec::{Override, StackNormalization};
	let frame = polars::df!("series" => ["lower", "upper", "lower", "upper"], "x" => [0.0, 0.0, 1.0, 1.0], "y" => [2.0, 3.0, 2.0, 3.0]).unwrap();
	let prepared = Arc::new(prepare_stacked_area_data(&frame, "series", "x", "y"));
	for normalization in [StackNormalization::Absolute, StackNormalization::Percent] {
		let kernel = StackedAreaPlotKernel {
			prepared_data: Arc::clone(&prepared),
			series_colors: vec![],
			normalization,
		};
		let mut settings = ResolvedPlotSettings {
			series_registry: SeriesRegistry::new(kernel.series_identities()),
			..Default::default()
		};
		let universe = settings.series_registry.identities().to_vec();
		let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
		let layout = kernel.layout(settings.clone());
		let transform = CoordinateTransformer::new(&layout, bounds);
		let render = |settings: &ResolvedPlotSettings| {
			let mut backend = super::export::SvgBackend::new(200.0, 100.0);
			kernel.plot(
				&mut backend,
				bounds,
				&transform,
				Cursor::Unavailable,
				settings.clone(),
			);
			backend.finish()
		};
		let before = render(&settings);
		settings.hidden_series.apply(
			&SeriesVisibilityAction::Hide(universe[0].clone()),
			&universe,
		);
		let hidden = render(&settings);
		assert!(!hidden.contains("data-series=\"s0\""));
		let upper = before
			.split("<g data-series=\"s1\">")
			.nth(1)
			.unwrap()
			.split("</g>")
			.next()
			.unwrap();
		assert!(hidden.contains(upper));
		let mut model = PlotSettings::default();
		model.pane_override.hidden_series = Override::Set(settings.hidden_series.clone());
		let lower_y = match normalization {
			StackNormalization::Absolute => 1.0,
			StackNormalization::Percent => 20.0,
		};
		assert!(
			kernel
				.hit_with_settings(
					&transform,
					Cursor::Available(transform.cartesian(0.5, lower_y)),
					&model
				)
				.is_none()
		);
		settings
			.hidden_series
			.apply(&SeriesVisibilityAction::RestoreAll, &universe);
		assert_eq!(before, render(&settings));
		assert!(Arc::ptr_eq(&prepared, &kernel.prepared_data));
	}
}

#[test]
fn product_0156_png_grouping_is_pixel_identical() {
	use super::common::{CoordinateTransformer, MarkRenderer, ResolvedPlotSettings};
	use super::kernels::line::LineMarkRenderer;
	use super::series::SeriesRegistry;
	let frame = polars::df!("series" => ["a", "a", "b", "b"], "x" => [0.0, 1.0, 0.0, 1.0], "y" => [0.0, 0.0, 1.0, 1.0]).unwrap();
	let prepared = Arc::new(prepare_line_data(&frame, "series", "x", "y"));
	let kernel = LinePlotKernel::new(Arc::clone(&prepared));
	let renderer = LineMarkRenderer::new(prepared);
	let mut settings = ResolvedPlotSettings::default();
	let bounds = Rectangle::new(Point::ORIGIN, Size::new(200.0, 100.0));
	let layout = kernel.layout(settings.clone());
	let transform = CoordinateTransformer::new(&layout, bounds);
	let render = |settings: &ResolvedPlotSettings| {
		let mut backend = super::export::PngBackend::new(200, 100);
		renderer.draw(&mut backend, bounds, &transform, settings.clone());
		backend.pixmap().data().to_vec()
	};
	let without_seam = render(&settings);
	settings.series_registry = SeriesRegistry::new(kernel.series_identities());
	assert_eq!(without_seam, render(&settings));
}

#[test]
fn product_0156_multivariate_rows_group_by_exact_boolean() {
	use super::kernels::{
		multivariate_rows::prepare_multivariate_rows, parallel::ParallelPlotKernel,
		radar::RadarPlotKernel,
	};
	let frame = polars::df!("series" => [false, true, false], "x" => [1.0, 2.0, 3.0], "y" => [2.0, 3.0, 1.0], "z" => [3.0, 1.0, 2.0]).unwrap();
	let prepared = Arc::new(
		prepare_multivariate_rows(
			&frame,
			&["x".to_owned(), "y".to_owned(), "z".to_owned()],
			Some("series"),
		)
		.unwrap(),
	);
	let kernels: Vec<Box<dyn PlotKernel>> = vec![
		Box::new(
			ParallelPlotKernel {
				row_identities: Vec::new(),
				prepared_data: Arc::clone(&prepared),
				category_colors: None,
			}
			.with_series_source(&frame, Some("series")),
		),
		Box::new(
			RadarPlotKernel {
				row_identities: Vec::new(),
				prepared_data: prepared,
				category_colors: None,
			}
			.with_series_source(&frame, Some("series")),
		),
	];
	for kernel in kernels {
		let ids = kernel.series_identities();
		assert_eq!(ids[0], ids[2]);
		assert_ne!(ids[0], ids[1]);
		let mut model = PlotSettings::default();
		let mut hidden = super::series::HiddenSeries::default();
		hidden.apply(&SeriesVisibilityAction::Hide(ids[0].clone()), &ids);
		model.pane_override.hidden_series = super::spec::Override::Set(hidden);
		let widget = PlotWidget::new(
			kernel.as_ref(),
			String::new(),
			20.0,
			model,
			0,
			false,
			PlotRenderLayer::Full,
		);
		let export = super::export::render_svg(
			&widget,
			Rectangle::new(Point::ORIGIN, Size::new(800.0, 600.0)),
		);
		assert!(!export.svg.contains("data-series=\"s0\""));
		assert!(export.svg.contains("data-series=\"s1\""));
		assert_eq!(export.series.identities().len(), 2);
	}
}

#[test]
fn product_0156_multivariate_label_collisions_are_inert_in_both_orders() {
	use super::kernels::{
		multivariate_rows::{MultivariateInputRow, MultivariateRows, MultivariateValueCell},
		parallel::ParallelPlotKernel,
		radar::RadarPlotKernel,
	};
	use super::legend::LegendSection;
	use super::spec::MultivariateFields;
	// Validation currently admits string/boolean categories. Supply a shared
	// printed label explicitly to exercise the kernel's collision boundary,
	// without expanding the validator's accepted category types.
	let fields = MultivariateFields::new(
		vec!["x".into(), "y".into(), "z".into()],
		Some("series".into()),
	);
	let prepared = Arc::new(
		MultivariateRows::from_rows(
			&fields,
			(0..3)
				.map(|index| MultivariateInputRow {
					category: Some("same label".into()),
					values: vec![MultivariateValueCell::Exact(index as f64 + 1.0); 3],
				})
				.collect(),
		)
		.unwrap(),
	);
	for values in [
		[Some(false), Some(true), Some(false)],
		[Some(true), Some(false), Some(true)],
		[None, Some(false), Some(false)],
		[Some(false), None, Some(false)],
		[Some(false); 3],
	] {
		let frame = polars::df!("series" => values).unwrap();
		let kernels: Vec<Box<dyn PlotKernel>> = vec![
			Box::new(
				ParallelPlotKernel {
					prepared_data: Arc::clone(&prepared),
					row_identities: Vec::new(),
					category_colors: None,
				}
				.with_series_source(&frame, Some("series")),
			),
			Box::new(
				RadarPlotKernel {
					prepared_data: Arc::clone(&prepared),
					row_identities: Vec::new(),
					category_colors: None,
				}
				.with_series_source(&frame, Some("series")),
			),
		];
		for kernel in kernels {
			let ids = kernel.series_identities();
			let content = kernel
				.legend_content(&PlotSettings::default().resolve())
				.unwrap();
			let (section, rest) = content.0.into_parts();
			assert!(rest.is_empty());
			let LegendSection::Entries(entries) = section else {
				panic!("entries")
			};
			let (entry, rest) = entries.into_parts();
			assert!(rest.is_empty());
			assert_eq!(
				entry.identity,
				(values == [Some(false); 3]).then(|| ids[0].clone())
			);
			let render = |model: PlotSettings| {
				super::export::render_svg(
					&PlotWidget::new(
						kernel.as_ref(),
						String::new(),
						20.0,
						model,
						0,
						false,
						PlotRenderLayer::Full,
					),
					Rectangle::new(Point::ORIGIN, Size::new(800.0, 600.0)),
				)
			};
			let original = render(PlotSettings::default());
			for identity in &ids {
				let ordinal = original.series.group(identity).unwrap().ordinal();
				let attribute = format!("data-series=\"s{ordinal}\"");
				assert!(original.svg.contains(&attribute));
				let mut model = PlotSettings::default();
				let mut hidden = super::series::HiddenSeries::default();
				hidden.apply(&SeriesVisibilityAction::Hide(identity.clone()), &ids);
				model.pane_override.hidden_series = super::spec::Override::Set(hidden);
				assert!(
					!render(model).svg.contains(&attribute),
					"ambiguous entries still allow direct identity actions"
				);
			}
		}
	}
}

#[test]
fn product_0156_formatted_temporal_labels_do_not_address_series() {
	use super::common::{AxisType, format_label};
	use polars::prelude::{Column, DataType, TimeUnit};
	let mut frame = polars::df!("x" => [0.0, 1.0], "y" => [1.0, 2.0]).unwrap();
	frame
		.with_column(
			Column::new("series".into(), [1_i64, 2])
				.cast(&DataType::Datetime(TimeUnit::Nanoseconds, None))
				.unwrap(),
		)
		.unwrap();
	let mut prepared = prepare_line_data(&frame, "series", "x", "y");
	// Exercise the real temporal formatter while preserving the identities
	// minted from the original column before any display transformation.
	for (index, series) in prepared.series.iter_mut().enumerate() {
		series.name = format_label(
			(index + 1) as f64,
			&AxisType::Datetime {
				unit: super::common::TimeUnit::Nanoseconds,
				zone: None,
			},
		);
	}
	assert_eq!(prepared.series[0].name, prepared.series[1].name);
	assert_ne!(prepared.series[0].identity, prepared.series[1].identity);
	let kernel = LinePlotKernel::new(Arc::new(prepared));
	let ids = kernel.series_identities();
	let mut model = PlotSettings::default();
	let mut hidden = super::series::HiddenSeries::default();
	hidden.apply(&SeriesVisibilityAction::Hide(ids[0].clone()), &ids);
	model.pane_override.hidden_series = super::spec::Override::Set(hidden);
	let widget = PlotWidget::new(
		&kernel,
		String::new(),
		20.0,
		model,
		0,
		false,
		PlotRenderLayer::Full,
	);
	let svg = super::export::render_svg(
		&widget,
		Rectangle::new(Point::ORIGIN, Size::new(800.0, 600.0)),
	);
	assert!(!svg.svg.contains("data-series=\"s0\""));
	assert!(svg.svg.contains("data-series=\"s1\""));
}

#[test]
fn product_0156_viewport_and_visibility_preserve_one_another() {
	use super::spec::Override;
	let kernel = line();
	let mut host = HostPlotState::new(
		String::new(),
		Arc::clone(&kernel),
		PlotSettingsLayer::default(),
	);
	let ids = kernel.series_identities();
	let domains = super::theme::PlotDomainOverrides {
		x_min: Some(0.25),
		x_max: Some(0.75),
		y_min: None,
		y_max: None,
	};
	host.update(PlotPaneMsg::ViewportChanged {
		stamp: host.viewport_stamp.next(),
		domains,
		quiet_settle: false,
	});
	host.update(PlotPaneMsg::SeriesVisibility(SeriesVisibilityAction::Hide(
		ids[0].clone(),
	)));
	assert_eq!(
		host.plot_settings_model.pane_override.domains,
		Override::Set(domains)
	);
	host.update(PlotPaneMsg::ViewportReset(host.viewport_stamp.next()));
	assert!(host.plot_settings.hidden_series.contains(&ids[0]));
	host.update(PlotPaneMsg::SeriesVisibility(
		SeriesVisibilityAction::RestoreAll,
	));
	assert!(matches!(
		host.plot_settings_model.pane_override.hidden_series,
		Override::Inherit
	));
}

#[test]
fn product_0156_layers_share_identity_across_distinct_legend_samples() {
	use super::common::AxisType;
	use super::kernels::{
		layered::{LayerRenderer, LayeredPlotKernel},
		scatter::prepare_scatter_data,
	};
	use super::spec::{LayerSettings, NonEmpty, Override, ResolvedLayout};
	let frame = polars::df!("series" => ["a", "a", "b", "b"], "x" => [0.0, 1.0, 0.0, 1.0], "y" => [0.0, 0.0, 1.0, 1.0]).unwrap();
	let mut layers = NonEmpty::new(LayerRenderer::line(
		Arc::new(prepare_line_data(&frame, "series", "x", "y")),
		LayerSettings::default(),
	));
	layers.push(LayerRenderer::scatter(
		Arc::new(prepare_scatter_data(&frame, "series", "x", "y")),
		LayerSettings::default(),
	));
	let kernel = LayeredPlotKernel::new(
		layers,
		ResolvedLayout {
			viewport_width: 800,
			viewport_height: 600,
			x_range: (0.0, 1.0),
			y_range: (0.0, 1.0),
		},
		AxisType::Linear,
		AxisType::Linear,
		None,
		None,
		"x".to_owned(),
		"y".to_owned(),
	);
	let bounds = Rectangle::new(Point::ORIGIN, Size::new(800.0, 600.0));
	let render = |model| {
		let widget = PlotWidget::new(
			&kernel,
			String::new(),
			20.0,
			model,
			0,
			false,
			PlotRenderLayer::Full,
		);
		(
			super::export::render_svg(&widget, bounds),
			widget
				.legend_hit_geometry(Rect::from_iced_frame(bounds))
				.unwrap(),
		)
	};
	let (before, geometry) = render(PlotSettings::default());
	assert_eq!(before.series.identities().len(), 2);
	assert_eq!(before.svg.matches("data-series=\"s0\"").count(), 2);
	assert_eq!(geometry.entries.len(), 4);
	let mut model = PlotSettings::default();
	let mut hidden = super::series::HiddenSeries::default();
	hidden.apply(
		&SeriesVisibilityAction::Hide(before.series.identities()[0].clone()),
		before.series.identities(),
	);
	model.pane_override.hidden_series = Override::Set(hidden);
	let (after, after_geometry) = render(model);
	assert!(!after.svg.contains("data-series=\"s0\""));
	assert_eq!(after.svg.matches("data-series=\"s1\"").count(), 2);
	assert_eq!(geometry.entries, after_geometry.entries);
	assert_eq!(before.series, after.series);
}

#[test]
fn product_0156_legend_background_and_modifiers_never_start_plot_gestures() {
	let kernel = line();
	let mut model = PlotSettings::default();
	model.authored_spec.legend = Presence::Show(LegendPosition::new(0.5, 0.5).unwrap());
	let widget = PlotWidget::new(
		kernel.as_ref(),
		String::new(),
		20.0,
		model,
		0,
		false,
		PlotRenderLayer::Full,
	)
	.with_settings_inspector_gesture();
	let bounds = Rectangle::new(Point::ORIGIN, Size::new(800.0, 600.0));
	let geometry = widget
		.legend_hit_geometry(Rect::from_iced_frame(bounds))
		.unwrap();
	for modifiers in [
		keyboard::Modifiers::ALT,
		keyboard::Modifiers::CTRL,
		keyboard::Modifiers::LOGO,
		keyboard::Modifiers::SHIFT | keyboard::Modifiers::ALT,
	] {
		let mut state = PlotCanvasState::default();
		let point = Point::new(geometry.entries[0].0.x + 5.0, geometry.entries[0].0.y + 5.0);
		event(
			&widget,
			&mut state,
			Event::Keyboard(keyboard::Event::ModifiersChanged(modifiers)),
			bounds,
			point,
		);
		let press = Program::update(
			&widget,
			&mut state,
			&Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)),
			bounds,
			Cursor::Available(point),
		)
		.expect("legend captures modified press");
		assert!(press.into_inner().0.is_none());
		assert!(
			event(
				&widget,
				&mut state,
				Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)),
				bounds,
				point
			)
			.is_none()
		);
	}
	let background = Point::new(geometry.bounds.x + 2.0, geometry.bounds.y + 2.0);
	let mut state = PlotCanvasState::default();
	let press = Program::update(
		&widget,
		&mut state,
		&Event::Mouse(mouse::Event::ButtonPressed(mouse::Button::Left)),
		bounds,
		Cursor::Available(background),
	)
	.expect("legend background captures press");
	assert!(press.into_inner().0.is_none());
	assert!(
		event(
			&widget,
			&mut state,
			Event::Mouse(mouse::Event::ButtonReleased(mouse::Button::Left)),
			bounds,
			background
		)
		.is_none()
	);
}
