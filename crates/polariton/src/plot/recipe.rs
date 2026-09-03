//! Rebuildable plot construction retained beside a live kernel.
//!
//! PRODUCT-0151 makes filtering a re-preparation of the original publication,
//! never a mutation of an already-filtered kernel. This type is the owned
//! recipe needed to repeat the exact factory path off the UI thread.

use crate::plot::factory::{create_layered_plot_from_artifact, create_plot_from_artifact};
use crate::plot::grid::{FigureGridLayoutSettings, PlotGrid, PlotGridKernel};
use crate::plot::spec::{
	LayerSettings, LayeredPlotSpec, PlotProvenance, ResolvedPlotSpec, StampedResult,
};
use crate::result_artifact::ResultArtifact;
use crate::selection::{Predicate, Publication};
use std::sync::Arc;

#[derive(Clone)]
pub enum PlotConstruction {
	Single {
		spec: ResolvedPlotSpec,
		width: u32,
		height: u32,
	},
	Facet {
		spec: Arc<ResolvedPlotSpec>,
		facet: Option<String>,
		figure_grid: FigureGridLayoutSettings,
		layer_settings: Option<LayerSettings>,
		width: u32,
		height: u32,
	},
	Layered {
		specification: LayeredPlotSpec,
		facet: Option<String>,
		figure_grid: FigureGridLayoutSettings,
		width: u32,
		height: u32,
	},
}

#[derive(Clone)]
pub struct PlotRecipe {
	pub artifact: Arc<ResultArtifact>,
	pub publication: Publication,
	pub construction: PlotConstruction,
}

impl PlotRecipe {
	pub fn single(
		artifact: Arc<ResultArtifact>,
		spec: ResolvedPlotSpec,
		width: u32,
		height: u32,
	) -> Self {
		Self {
			publication: artifact.publication(),
			artifact,
			construction: PlotConstruction::Single {
				spec,
				width,
				height,
			},
		}
	}

	pub fn facet(
		artifact: Arc<ResultArtifact>,
		spec: Arc<ResolvedPlotSpec>,
		facet: Option<String>,
		figure_grid: FigureGridLayoutSettings,
		layer_settings: Option<LayerSettings>,
		width: u32,
		height: u32,
	) -> Self {
		Self {
			publication: artifact.publication(),
			artifact,
			construction: PlotConstruction::Facet {
				spec,
				facet,
				figure_grid,
				layer_settings,
				width,
				height,
			},
		}
	}

	pub fn layered(
		artifact: Arc<ResultArtifact>,
		specification: LayeredPlotSpec,
		facet: Option<String>,
		figure_grid: FigureGridLayoutSettings,
		width: u32,
		height: u32,
	) -> Self {
		Self {
			publication: artifact.publication(),
			artifact,
			construction: PlotConstruction::Layered {
				specification,
				facet,
				figure_grid,
				width,
				height,
			},
		}
	}

	pub fn rebuild(
		&self,
		predicate: &Predicate,
	) -> Result<Arc<dyn crate::plot::common::PlotKernel + Send + Sync>, String> {
		let unfiltered = self
			.artifact
			.projection()
			.map_err(|error| error.to_string())?;
		let filtered = predicate
			.filter_frame(&unfiltered)
			.map_err(|error| error.to_string())?;
		let filtered_artifact =
			ResultArtifact::from_polars_derived(Arc::new(filtered.clone()), &self.artifact);
		match &self.construction {
			PlotConstruction::Single {
				spec,
				width,
				height,
			} => Ok(create_plot_from_artifact(
				spec,
				&filtered,
				&filtered_artifact,
				*width,
				*height,
			)),
			PlotConstruction::Facet {
				spec,
				facet,
				figure_grid,
				layer_settings,
				width,
				height,
			} => {
				let stamped = StampedResult {
					data: Arc::clone(&filtered_artifact),
					provenance: spec.provenance.clone(),
				};
				let grid = match facet {
					Some(field) => PlotGrid::value_facet_derived(Arc::clone(spec), &stamped, field)
						.map_err(|error| error.to_string())?,
					None => PlotGrid::single(Arc::clone(spec), Arc::new(filtered.clone())),
				};
				let kernel = match layer_settings {
					Some(settings) => {
						PlotGridKernel::from_styled_grid_with_figure_grid_from_artifact(
							grid,
							*figure_grid,
							settings.clone(),
							&filtered_artifact,
							*width,
							*height,
						)?
					}
					None => PlotGridKernel::from_grid_with_figure_grid_from_artifact(
						grid,
						*figure_grid,
						&filtered_artifact,
						*width,
						*height,
					),
				};
				Ok(Arc::new(kernel))
			}
			PlotConstruction::Layered {
				specification,
				facet,
				figure_grid,
				width,
				height,
			} => {
				if facet.is_none() {
					return create_layered_plot_from_artifact(
						specification,
						&filtered,
						&filtered_artifact,
						*width,
						*height,
					);
				}
				let binding = crate::plot::spec::resolve_layered_channels(
					specification,
					filtered.schema().as_ref(),
				)
				.map_err(|error| error.to_string())?;
				let stamped = StampedResult {
					data: Arc::clone(&filtered_artifact),
					provenance: PlotProvenance {
						source_name: "selection rebuild".to_owned(),
						revision: None,
					},
				};
				let resolved = crate::plot::spec::resolve_layered_domains(binding, &stamped)
					.map_err(|error| error.to_string())?;
				let body = Arc::new(resolved.layers.first().spec.clone());
				let grid = PlotGrid::value_facet_derived(
					body,
					&stamped,
					facet.as_deref().expect("facet branch"),
				)
				.map_err(|error| error.to_string())?;
				let kernel = PlotGridKernel::from_grid_with_factory_and_figure_grid(
					grid,
					*figure_grid,
					*width,
					*height,
					|_, data| {
						create_layered_plot_from_artifact(
							specification,
							data,
							&filtered_artifact,
							*width,
							*height,
						)
					},
				)?;
				Ok(Arc::new(kernel))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::plot::common::{PlotLayout, ResolvedPlotSettings};
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::spec::{
		AuthoredPlotSpec, ColorIdentity, HexbinFields, PlotProvenance, StampedResult,
		resolve_channels, resolve_domains,
	};
	use crate::selection::{
		ConstraintForm, ExactScalar, FieldConstraint, FieldRef, InputIdentity, ScalarType,
		SelectionAxisFields, SourceField,
	};
	use polars::prelude::*;
	use std::collections::HashSet;
	use std::time::Instant;

	fn resolved(artifact: &Arc<ResultArtifact>, authored: AuthoredPlotSpec) -> ResolvedPlotSpec {
		let frame = artifact.projection().expect("projection");
		let binding = resolve_channels(&authored, frame.schema().as_ref()).expect("channels");
		resolve_domains(
			binding,
			&StampedResult {
				data: Arc::clone(artifact),
				provenance: PlotProvenance {
					source_name: "selection recipe fixture".into(),
					revision: None,
				},
			},
			&ColorIdentity::default(),
		)
		.expect("domains")
		.0
	}

	fn category_predicate(value: &str) -> Predicate {
		Predicate {
			fields: vec![FieldConstraint {
				field: FieldRef::new(
					"category",
					crate::plot::spec::SemanticType::Nominal,
					ScalarType::Str,
				),
				form: ConstraintForm::Values(HashSet::from([ExactScalar::Str(value.into())])),
			}],
		}
	}

	#[test]
	fn product_0151_recipe_rebuilds_each_revision_from_the_unfiltered_artifact() {
		let frame = polars::df!(
			"x" => [1.0_f64, 2.0, 3.0],
			"y" => [10.0_f64, 20.0, 30.0],
			"category" => ["a", "b", "a"],
		)
		.expect("recipe fixture");
		let artifact = Arc::new(ResultArtifact::from_polars(Arc::new(frame)));
		artifact.bind_input_identity(InputIdentity::mint());
		let spec = resolved(
			&artifact,
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
		);
		let recipe = PlotRecipe::single(Arc::clone(&artifact), spec, 800, 600);
		let first = recipe
			.rebuild(&category_predicate("a"))
			.expect("first rebuild");
		let second = recipe
			.rebuild(&category_predicate("b"))
			.expect("second rebuild");
		let PlotLayout::Cartesian { x_range: first, .. } =
			first.layout(ResolvedPlotSettings::default())
		else {
			panic!("scatter layout")
		};
		let PlotLayout::Cartesian {
			x_range: second, ..
		} = second.layout(ResolvedPlotSettings::default())
		else {
			panic!("scatter layout")
		};
		assert!(first.0 <= 1.0 && first.1 >= 3.0);
		assert!(second.0 <= 2.0 && second.1 >= 2.0);
		assert_ne!(
			second,
			(0.0, 1.0),
			"second rebuild did not compound over the first"
		);
		assert_eq!(recipe.publication, artifact.publication());
	}

	#[test]
	#[ignore = "PRODUCT-0151 stage-0 cost exhibit; run in debug and release profiles"]
	fn product_0151_exhibit_one_million_scatter_filter_rebuild_and_projection() {
		let count = 1_000_000_usize;
		let frame = DataFrame::new(
			count,
			vec![
				Column::new("x".into(), (0..count as i64).collect::<Vec<_>>()),
				Column::new(
					"y".into(),
					(0..count as i64)
						.map(|value| value % 997)
						.collect::<Vec<_>>(),
				),
				Column::new(
					"category".into(),
					(0..count)
						.map(|row| if row % 2 == 0 { "even" } else { "odd" })
						.collect::<Vec<_>>(),
				),
			],
		)
		.expect("million-row fixture");
		let artifact = Arc::new(ResultArtifact::from_polars(Arc::new(frame)));
		artifact.bind_input_identity(InputIdentity::mint());
		let spec = resolved(
			&artifact,
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
		);
		let recipe = PlotRecipe::single(Arc::clone(&artifact), spec, 1200, 800);
		let started = Instant::now();
		let kernel = recipe
			.rebuild(&category_predicate("even"))
			.expect("filtered rebuild");
		let rebuild = started.elapsed();
		let axis = SelectionAxisFields::from_recipe(&recipe, kernel.as_ref());
		let x = axis.x.expect("x field");
		let predicate = Predicate {
			fields: vec![FieldConstraint {
				field: x,
				form: ConstraintForm::Values(HashSet::from([ExactScalar::Int(500_000)])),
			}],
		};
		let started = Instant::now();
		let anchors = kernel
			.project_predicate(&predicate)
			.expect("scatter admission");
		let projection = started.elapsed();
		assert_eq!(anchors.len(), 1);
		let layout = kernel.layout(ResolvedPlotSettings::default());
		let transform = crate::plot::common::CoordinateTransformer::new(
			&layout,
			iced::Rectangle::new(iced::Point::ORIGIN, iced::Size::new(1200.0, 800.0)),
		);
		let started = Instant::now();
		for _ in 0..10 {
			assert!(anchors[0].reproject(&transform).is_some());
		}
		let ten_overlay_frames = started.elapsed();

		let hexbin_spec = resolved(
			&artifact,
			AuthoredPlotSpec::hexbin(HexbinFields {
				x: "x".into(),
				y: "y".into(),
			}),
		);
		let hexbin = PlotRecipe::single(Arc::clone(&artifact), hexbin_spec, 1200, 800);
		let started = Instant::now();
		let _ = hexbin
			.rebuild(&Predicate {
				fields: vec![FieldConstraint {
					field: FieldRef::new(
						"x",
						crate::plot::spec::SemanticType::Quantitative,
						ScalarType::Int,
					),
					form: ConstraintForm::Interval {
						lower: ExactScalar::Int(100),
						upper: ExactScalar::Int(200),
					},
				}],
			})
			.expect("hexbin rebuild");
		let hexbin_rebuild = started.elapsed();
		eprintln!(
			"PRODUCT-0151 1M: scatter rebuild={rebuild:?}, projection={projection:?}, ten overlay reprojections={ten_overlay_frames:?}, hexbin rebuild={hexbin_rebuild:?}"
		);
	}

	#[test]
	fn product_0151_recipe_fields_always_name_projected_columns() {
		let frame = polars::df!("x" => [1_i64], "y" => [2_i64]).expect("frame");
		let artifact = Arc::new(ResultArtifact::from_polars(Arc::new(frame)));
		artifact.bind_input_identity(InputIdentity::mint());
		let spec = resolved(
			&artifact,
			AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x", "y"),
		);
		let kernel =
			create_plot_from_artifact(&spec, &artifact.projection().unwrap(), &artifact, 400, 300);
		let recipe = PlotRecipe::single(Arc::clone(&artifact), spec, 400, 300);
		let fields = SelectionAxisFields::from_recipe(&recipe, kernel.as_ref());
		let projected = artifact.projection().unwrap();
		for field in [fields.x, fields.y].into_iter().flatten() {
			assert!(projected.column(field.name.as_str()).is_ok());
		}
		assert!(
			projected
				.column(SourceField::new("Count").as_str())
				.is_err()
		);
	}
}
