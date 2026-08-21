use crate::adapters::common::{
	AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult, MaterializationReceipt,
	RuneDispatchFailure, RuneDispatchFailureKind, RuneDispatchResult, RuneDispatchSuccess,
};
use crate::result_artifact::{ArrowNullabilityPolicy, ResultArtifact};
use async_trait::async_trait;

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "endpoint",
	placeholder: "host:port",
	default_value: None,
	field_type: &AdapterFieldType::Text,
	is_secure: false,
}];

/// PRODUCT-0135 §2: the dispatch-minted query identity, shared between the
/// adapter and its cancellation capability through this slot's own mutex —
/// never through the adapter lock.
pub(crate) type CurrentQuery = std::sync::Arc<std::sync::Mutex<Option<toron_client::QueryId>>>;

/// RAII stamp: minting writes the slot, and the guard clears it however the
/// dispatch ends, so a completed query can never be "cancelled" late.
pub(crate) struct QueryStamp {
	slot: CurrentQuery,
	pub id: toron_client::QueryId,
}

pub(crate) fn stamp_query(slot: &CurrentQuery) -> QueryStamp {
	let id = toron_client::QueryId::new();
	*slot.lock().expect("query slot") = Some(id);
	QueryStamp {
		slot: std::sync::Arc::clone(slot),
		id,
	}
}

impl Drop for QueryStamp {
	fn drop(&mut self) {
		*self.slot.lock().expect("query slot") = None;
	}
}

/// The engine-side signal: a client clone plus the shared slot. Cancelling
/// with no query in flight is a routine no-op — the cancel raced completion.
struct ToronCancel {
	client: toron_client::ToronClient,
	current: CurrentQuery,
}

#[async_trait]
impl crate::adapters::common::EngineCancel for ToronCancel {
	async fn signal(&self) -> Result<(), String> {
		let id = *self.current.lock().expect("query slot");
		match id {
			Some(id) => self
				.client
				.cancel(id)
				.await
				.map_err(|error| format!("Toron cancel: {error}")),
			None => Ok(()),
		}
	}
}

pub struct ToronAdapter {
	client: toron_client::ToronClient,
	current_query: CurrentQuery,
}

impl ToronAdapter {
	pub async fn connect(endpoint: &str) -> Result<Self, toron_client::ClientError> {
		let client = toron_client::ToronClient::loopback(endpoint)?;
		client.health().await?;
		Ok(Self {
			client,
			current_query: CurrentQuery::default(),
		})
	}

	/// PRODUCT-0135 §2: taken at connect time, before the adapter goes behind
	/// its lock; the returned capability never touches that lock again.
	pub fn cancel_capability(&self) -> crate::adapters::common::CancelCapability {
		crate::adapters::common::CancelCapability::Engine(std::sync::Arc::new(ToronCancel {
			client: self.client.clone(),
			current: std::sync::Arc::clone(&self.current_query),
		}))
	}
}

#[async_trait]
impl DatabaseAdapter for ToronAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let stamp = stamp_query(&self.current_query);
		let result = match self
			.client
			.execute_with_query_id(stamp.id, code, "UTC")
			.await
		{
			Ok(result) => result,
			Err(error) => return ExecutionResult::Err(error.to_string()),
		};
		if result.batches.is_empty() {
			return if result.row_count == 0 {
				ExecutionResult::CommandCompleted("Command completed.".to_owned())
			} else {
				ExecutionResult::Affected(result.row_count)
			};
		}
		match ResultArtifact::from_arrow_ipc(result.batches, ArrowNullabilityPolicy::Preserve) {
			Ok(artifact) => ExecutionResult::Rows {
				artifact,
				statements: 1,
			},
			Err(error) => ExecutionResult::Err(format!("Toron result conversion failed: {error}")),
		}
	}

	async fn dispatch_rune(&mut self, source: &str) -> RuneDispatchResult {
		let result = self
			.client
			.execute_rune(source)
			.await
			.map_err(|error| match error {
				toron_client::ClientError::Rune(failure) => {
					use toron_client::RuneFailureKind;
					let kind = match failure.kind {
						RuneFailureKind::Compile => RuneDispatchFailureKind::Compile,
						RuneFailureKind::Runtime => RuneDispatchFailureKind::Runtime,
						RuneFailureKind::Sandbox(_) => RuneDispatchFailureKind::Sandbox,
						RuneFailureKind::VersionSkew(_) => RuneDispatchFailureKind::VersionSkew,
						RuneFailureKind::Execution(_) => RuneDispatchFailureKind::Execution,
					};
					RuneDispatchFailure {
						kind,
						message: failure.message,
						span: failure.span.map(|span| span.start..span.end),
					}
				}
				other => RuneDispatchFailure {
					kind: RuneDispatchFailureKind::Transport,
					message: other.to_string(),
					span: None,
				},
			})?;
		// PRODUCT-0137: both wire outcomes are mapped explicitly. No `_` arm,
		// so a future variant fails the build rather than being silently
		// rendered as one of these two.
		match result {
			toron_client::RuneQueryResult::Batches(batches) => {
				ResultArtifact::from_arrow_ipc(batches.batches, ArrowNullabilityPolicy::Preserve)
					.map(RuneDispatchSuccess::Relation)
					.map_err(|error| RuneDispatchFailure {
						kind: RuneDispatchFailureKind::Execution,
						message: format!("Toron Rune result conversion failed: {error}"),
						span: None,
					})
			}
			toron_client::RuneQueryResult::Materialized(receipt) => {
				Ok(RuneDispatchSuccess::Materialized(MaterializationReceipt {
					namespace: receipt.namespace,
					table_name: receipt.table_name,
					certificate_format_version: receipt.certificate_format_version,
					rows_written: receipt.rows_written,
				}))
			}
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{DatabaseAdapter, ExecutionResult, ToronAdapter};
	use crate::plot::common::{PlotRenderLayer, PlotWidget, ResolvedPlotSettings};
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::export::SvgBackend;
	use crate::plot::factory::{create_authored_plot, create_automatic_plot};
	use crate::plot::spec::AuthoredPlotSpec;
	use crate::rune_relation_envelope::{
		RunePlotBody, RuneViewIntent, transform as transform_rune_view,
	};
	use iced::Rectangle;
	use std::path::PathBuf;

	/// PRODUCT-0135 §2: the dispatch-minted identity is visible to the
	/// capability exactly while a dispatch is in flight, and cleared however
	/// the dispatch ends — no live deployment required.
	#[test]
	fn product_0135_query_stamp_fills_and_clears_the_shared_slot() {
		let slot = super::CurrentQuery::default();
		assert!(slot.lock().expect("slot").is_none());
		{
			let stamp = super::stamp_query(&slot);
			assert_eq!(*slot.lock().expect("slot"), Some(stamp.id));
		}
		assert!(
			slot.lock().expect("slot").is_none(),
			"a finished dispatch can never be cancelled late"
		);
	}

	// These gates share one live tenant. Serializing them keeps the tenant's
	// bounded query budget from turning test parallelism into rotating failures.
	static LIVE_TORON_TEST_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

	fn live_endpoint() -> String {
		std::env::var("TORON_SQL_ENDPOINT")
			.unwrap_or_else(|_| nilket_local::SQL_ENDPOINT.to_owned())
	}

	async fn dispatch_live_fixture(adapter: &mut ToronAdapter, sql: &str) -> ExecutionResult {
		for attempt in 0..20 {
			let result = adapter.dispatch(sql).await;
			let exhausted = matches!(
				&result,
				ExecutionResult::Err(error) if error.contains("ResourceExhausted")
			);
			if !exhausted || attempt == 19 {
				return result;
			}
			tokio::time::sleep(std::time::Duration::from_millis(10)).await;
		}
		unreachable!("bounded live-fixture retry always returns")
	}

	#[tokio::test]
	#[ignore = "requires a live Toron SQL endpoint"]
	async fn live_toron_query_reaches_the_existing_plot_kernel() {
		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let table = format!("polariton_live_plot_{}", std::process::id());
		let _ = dispatch_live_fixture(&mut adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
		let outcome: Result<(), String> = async {
			for statement in [
				format!("CREATE TABLE {table} (x BIGINT, y BIGINT)"),
				format!("INSERT INTO {table} VALUES (1, 2), (2, 4), (3, 8), (4, 16)"),
			] {
				if let ExecutionResult::Err(error) =
					dispatch_live_fixture(&mut adapter, &statement).await
				{
					return Err(format!("self-seed failed: {error}"));
				}
			}
			let ExecutionResult::Rows {
				artifact,
				statements,
			} = dispatch_live_fixture(
				&mut adapter,
				&format!("SELECT x, y FROM {table} ORDER BY x"),
			)
			.await
			else {
				return Err("Toron SELECT did not return rows".to_owned());
			};
			let frame = artifact.projection().map_err(|error| error.to_string())?;
			if statements != 1 || frame.shape() != (4, 2) {
				return Err(format!(
					"self-seeded query returned statements={statements}, shape={:?}",
					frame.shape()
				));
			}

			let composition = PlotComposition::single(PlotType::Scatter);
			let kernel = create_automatic_plot(composition, &frame, 800, 480)?;
			let widget = PlotWidget {
				kernel: kernel.as_ref(),
				title: "Toron SQL live gate".to_owned(),
				padding: 0.0,
				settings: ResolvedPlotSettings::default(),
				settings_model: crate::plot::theme::PlotSettings::default(),
				render_revision: 0,
				resize_render_suspended: false,
				layer: PlotRenderLayer::Full,
			};
			let mut backend = SvgBackend::new(800.0, 480.0);
			widget.render(
				&mut backend,
				Rectangle::new([0.0, 0.0].into(), [800.0, 480.0].into()),
			);
			let svg = backend.finish();
			if !svg.contains("<path") || svg.len() <= 1_000 {
				return Err("plot artifact was not substantively rendered".to_owned());
			}

			let output = std::env::var_os("TORON_FIRST_CONTACT_PLOT")
				.map(PathBuf::from)
				.unwrap_or_else(|| std::env::temp_dir().join("toron-first-contact.svg"));
			std::fs::write(&output, svg)
				.map_err(|error| format!("write Toron plot artifact: {error}"))?;
			eprintln!("Toron first-contact plot: {}", output.display());
			Ok(())
		}
		.await;
		let cleanup =
			dispatch_live_fixture(&mut adapter, &format!("DROP TABLE IF EXISTS {table}")).await;
		if let ExecutionResult::Err(error) = cleanup {
			panic!("fixture cleanup failed: {error}");
		}
		outcome.expect("self-contained Toron SQL live gate");
	}

	#[tokio::test]
	#[ignore = "requires a live Toron SQL endpoint"]
	async fn live_product_0084_nullable_schema_survives_the_toron_transport() {
		use crate::result_artifact::FieldNullability;

		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let result = adapter
			.dispatch(concat!(
				"SELECT COUNT(*) AS required_value, ",
				"NULLIF(COUNT(*), 9999) AS optional_value ",
				"FROM demo_sales GROUP BY region LIMIT 1",
			))
			.await;
		let ExecutionResult::Rows { artifact, .. } = result else {
			panic!("asymmetric query must return rows, got {result:?}")
		};
		assert_eq!(
			artifact
				.schema()
				.field("required_value")
				.unwrap()
				.nullability(),
			FieldNullability::Known(false)
		);
		assert_eq!(
			artifact
				.schema()
				.field("optional_value")
				.unwrap()
				.nullability(),
			FieldNullability::Known(true)
		);
		assert!(!artifact.row_is_null("required_value", 0).unwrap());
		assert!(!artifact.row_is_null("optional_value", 0).unwrap());
		assert!(!artifact.projection_is_built());
	}

	#[tokio::test]
	#[ignore = "requires the dogfood test2 relation on a live local Toron"]
	async fn live_rune_relation_reaches_table_bar_and_scatter_surfaces() {
		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let source = "let r = workspace.table(\"test2\").sort([col(\"cat\").asc()]); table(r); let marker = 1; plot.bar(r, \"cat\", \"val\"); let tail = marker + 1; plot.scatter(r, \"cat\", \"val\")";
		let view = transform_rune_view(source).expect("fan-out view envelope");
		let view = view.into_relations().next().expect("one relation");
		let artifact = adapter
			.dispatch_rune(&view.source)
			.await
			.expect("one Rune relation result");
		let frame = artifact.expect_relation().projection().expect("Toron projection");
		assert!(frame.height() > 0, "dogfood relation is nonempty");
		assert_eq!(view.views.len(), 3);
		for view in view.views {
			match view.intent {
				RuneViewIntent::Table(_) => {
					assert!(frame.column("cat").is_ok());
					assert!(frame.column("val").is_ok());
				}
				RuneViewIntent::Plot(plot) => match plot.body {
					RunePlotBody::Single(plot) => {
						let expected_x = plot.x.clone();
						let expected_y = plot.y.clone();
						let projected = frame
							.select([plot.x.as_str(), plot.y.as_str()])
							.expect("project plot fields");
						let kernel = create_authored_plot(
							AuthoredPlotSpec::xy(plot.composition, plot.x, plot.y),
							&projected,
							800,
							480,
						)
						.expect("resolve relation plot fields");
						assert_eq!(kernel.x_label(), expected_x);
						assert_eq!(kernel.y_label(), expected_y);
						let widget = PlotWidget {
							kernel: kernel.as_ref(),
							title: "Rune relation first contact".to_owned(),
							padding: 0.0,
							settings: ResolvedPlotSettings::default(),
							settings_model: crate::plot::theme::PlotSettings::default(),
							render_revision: 0,
							resize_render_suspended: false,
							layer: PlotRenderLayer::Full,
						};
						let mut backend = SvgBackend::new(800.0, 480.0);
						widget.render(
							&mut backend,
							Rectangle::new([0.0, 0.0].into(), [800.0, 480.0].into()),
						);
						assert!(backend.finish().contains("<path"));
					}
					RunePlotBody::StyledSingle(_) | RunePlotBody::Layered(_) => {
						panic!("this legacy fixture contains no layered sink")
					}
				},
			}
		}
	}

	/// PRODUCT-0137 evidence slice: one real workflow end to end, timed.
	///
	/// Generates a reproducible relation, materializes it, reopens it through
	/// the ordinary `ket.table(...)` view path, and renders a scatter — the
	/// first exercise of the fifth arc's whole chain in one process, and the
	/// first measurement behind `ResolvedPlotSettings`' scatter thresholds
	/// (50k vector / 200k downsample / 1M raster), which have never had a
	/// benchmark harness.
	///
	/// Run with a live local workspace:
	///   cargo test -p polariton --bin polariton \
	///     live_scatter_threshold_evidence -- --ignored --nocapture
	#[tokio::test]
	#[ignore = "requires a live local Toron workspace"]
	async fn live_scatter_threshold_evidence() {
		use std::time::Instant;

		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");

		// Just below, at, and above each scatter boundary.
		const CARDINALITIES: &[i64] = &[49_999, 50_000, 199_999, 200_000, 999_999, 1_000_000];
		const SEED: i64 = 20260821;

		println!("\ncardinality,materialize_ms,transfer_prepare_ms,first_render_ms,steady_render_ms,rows");
		for &n in CARDINALITIES {
			let table = format!("evidence_scatter_{n}");
			let _ = adapter
				.dispatch(&format!("DROP TABLE IF EXISTS {table}"))
				.await;

			// Stage 1 — generation and materialization, routed through the
			// production compiler so this exercises PRODUCT-0137's terminal
			// `ket` route rather than hand-feeding the adapter.
			let materialize_source = format!(
				"ket.range({n}).project_named([\
				 seeded_random(lit_i64({SEED}), col(\"ordinal\")).named(\"x\"), \
				 seeded_random(lit_i64({}), col(\"ordinal\")).named(\"y\")\
				 ]).materialize(\"{table}\")",
				SEED + 1
			);
			let materialize_view = transform_rune_view(&materialize_source)
				.expect("terminal ket relation compiles")
				.into_relations()
				.next()
				.expect("one relation");
			assert!(
				materialize_view.views.is_empty(),
				"a materialize carries no presentation slots"
			);
			let started = Instant::now();
			let outcome = adapter
				.dispatch_rune(&materialize_view.source)
				.await
				.expect("materialize succeeds");
			let materialize_ms = started.elapsed().as_secs_f64() * 1000.0;
			let crate::adapters::common::RuneDispatchSuccess::Materialized(receipt) = outcome else {
				panic!("materialize must report a receipt, not a relation");
			};
			assert_eq!(receipt.rows_written, n as u64);
			assert_eq!(receipt.certificate_format_version, 1);

			// Stage 2 — reopen through the ordinary view path, transfer and
			// prepare the plot. This is the round trip a user actually takes.
			let view_source = format!(
				"ket.table(\"{table}\").views([scatter(\"x\", \"y\")])"
			);
			let view = transform_rune_view(&view_source)
				.expect("view envelope")
				.into_relations()
				.next()
				.expect("one relation");
			let started = Instant::now();
			let artifact = adapter
				.dispatch_rune(&view.source)
				.await
				.expect("relation reopens")
				.expect_relation();
			let frame = artifact.projection().expect("projection");
			let projected = frame.select(["x", "y"]).expect("project plot fields");
			let kernel = create_authored_plot(
				AuthoredPlotSpec::xy(PlotComposition::single(PlotType::Scatter), "x".to_owned(), "y".to_owned()),
				&projected,
				1200,
				720,
			)
			.expect("scatter kernel");
			let transfer_prepare_ms = started.elapsed().as_secs_f64() * 1000.0;
			assert_eq!(frame.height(), n as usize);

			let widget = PlotWidget {
				kernel: kernel.as_ref(),
				title: format!("{n} points"),
				padding: 0.0,
				settings: ResolvedPlotSettings::default(),
				settings_model: crate::plot::theme::PlotSettings::default(),
				render_revision: 0,
				resize_render_suspended: false,
				layer: PlotRenderLayer::Full,
			};
			let bounds = Rectangle::new([0.0, 0.0].into(), [1200.0, 720.0].into());

			// Stage 3 — first render, then steady state. Separate backends so
			// no accumulated buffer flatters the second measurement.
			let started = Instant::now();
			let mut backend = SvgBackend::new(1200.0, 720.0);
			widget.render(&mut backend, bounds);
			let first_render_ms = started.elapsed().as_secs_f64() * 1000.0;
			assert!(backend.finish().contains("<path") || n == 0);

			let mut steady = Vec::new();
			for _ in 0..5 {
				let started = Instant::now();
				let mut backend = SvgBackend::new(1200.0, 720.0);
				widget.render(&mut backend, bounds);
				steady.push(started.elapsed().as_secs_f64() * 1000.0);
				let _ = backend.finish();
			}
			steady.sort_by(|a, b| a.partial_cmp(b).expect("finite timings"));
			let steady_render_ms = steady[steady.len() / 2];

			println!(
				"{n},{materialize_ms:.1},{transfer_prepare_ms:.1},{first_render_ms:.1},{steady_render_ms:.1},{}",
				receipt.rows_written
			);

			let _ = adapter
				.dispatch(&format!("DROP TABLE IF EXISTS {table}"))
				.await;
		}
	}

	#[tokio::test]
	#[ignore = "requires the PRODUCT-0103 pie_probe relation on a live local Toron"]
	async fn live_product_0103_aggregate_and_raw_handles_remain_distinct() {
		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let source = include_str!("../../../../../uat/test32.rn");
		let view = transform_rune_view(source).expect("PRODUCT-0103 live exhibit envelope");
		let relations = view.into_relations().collect::<Vec<_>>();
		assert_eq!(relations.len(), 2);

		for relation in relations {
			let artifact = adapter
				.dispatch_rune(&relation.source)
				.await
				.unwrap_or_else(|error| {
					panic!(
						"PRODUCT-0103 relation failed for origin {}: {error:?}\n{}",
						relation.origin, relation.source
					)
				});
			let frame = artifact.expect_relation().projection().expect("Toron projection");
			if relation.origin.contains(".aggregate(") {
				assert_eq!(frame.height(), 3, "aggregated pie relation");
			} else {
				assert_eq!(frame.height(), 6, "raw pie relation");
			}
		}
	}

	#[tokio::test]
	#[ignore = "requires the PRODUCT-0108 violin_probe relation on a live local Toron"]
	async fn live_product_0108_relation_side_violin_renders_without_the_lonely_group() {
		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let source = include_str!("../../../../../uat/test34.rn");
		let relation = transform_rune_view(source)
			.expect("PRODUCT-0108 live exhibit envelope")
			.into_relations()
			.next()
			.expect("one violin relation");
		let artifact = adapter
			.dispatch_rune(&relation.source)
			.await
			.unwrap_or_else(|error| {
				panic!(
					"PRODUCT-0108 relation failed for origin {}: {error:?}\n{}",
					relation.origin, relation.source
				)
			});
		let frame = artifact.expect_relation().projection().expect("Toron violin projection");
		assert_eq!(
			frame.height(),
			288,
			"three groups times 96 evaluation points"
		);
		let groups = frame
			.column("group")
			.expect("density relation carries group")
			.as_materialized_series()
			.str()
			.expect("group is text")
			.into_no_null_iter()
			.collect::<std::collections::BTreeSet<_>>();
		assert_eq!(
			groups,
			std::collections::BTreeSet::from(["diffuse", "plateau", "tight"])
		);
		assert!(!groups.contains("lonely"));

		let violin = relation
			.views
			.iter()
			.find_map(|view| match &view.intent {
				RuneViewIntent::Plot(plot) => match &plot.body {
					RunePlotBody::Single(sink) => sink.mark_fields.as_ref()?.violin(),
					RunePlotBody::StyledSingle(styled) => {
						styled.sink.mark_fields.as_ref()?.violin()
					}
					RunePlotBody::Layered(_) => None,
				},
				RuneViewIntent::Table(_) => None,
			})
			.expect("the exhibit carries one violin sink");
		let kernel =
			create_authored_plot(AuthoredPlotSpec::violin(violin.0.clone()), &frame, 800, 480)
				.expect("the delivered density relation resolves into a violin kernel");
		let widget = PlotWidget {
			kernel: kernel.as_ref(),
			title: "PRODUCT-0108 violin".to_owned(),
			padding: 0.0,
			settings: ResolvedPlotSettings::default(),
			settings_model: crate::plot::theme::PlotSettings::default(),
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = SvgBackend::new(800.0, 480.0);
		widget.render(
			&mut backend,
			Rectangle::new([0.0, 0.0].into(), [800.0, 480.0].into()),
		);
		let svg = backend.finish();
		assert!(
			svg.contains("<path"),
			"violin geometry reached the SVG backend"
		);
	}

	#[tokio::test]
	#[ignore = "requires the dogfood test1 relation on a live local Toron"]
	async fn live_product_0012_line_view_reaches_the_existing_kernel() {
		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let source = "ket.table(\"test1\").sort([col(\"x\").asc()]).views([line(\"x\", \"y\")])";
		let relation = transform_rune_view(source)
			.expect("PRODUCT-0012 line envelope")
			.into_relations()
			.next()
			.expect("one relation");
		let artifact = adapter
			.dispatch_rune(&relation.source)
			.await
			.expect("one Rune relation result");
		let frame = artifact.expect_relation().projection().expect("Toron projection");
		let [view] = relation.views.as_slice() else {
			panic!("line fixture must produce exactly one view");
		};
		let RuneViewIntent::Plot(plot) = &view.intent else {
			panic!("line fixture must produce a plot");
		};
		let RunePlotBody::Single(plot) = &plot.body else {
			panic!("line fixture must produce one line mark");
		};
		assert_eq!(plot.composition.primary_plot_type(), PlotType::Line);
		let projected = frame
			.select([plot.x.as_str(), plot.y.as_str()])
			.expect("project distinct line fields");
		let kernel = create_authored_plot(
			AuthoredPlotSpec::xy(plot.composition.clone(), plot.x.clone(), plot.y.clone()),
			&projected,
			800,
			480,
		)
		.expect("resolve authored line fields");
		assert_eq!(kernel.x_label(), "x");
		assert_eq!(kernel.y_label(), "y");

		let widget = PlotWidget {
			kernel: kernel.as_ref(),
			title: "Rune line view".to_owned(),
			padding: 0.0,
			settings: ResolvedPlotSettings::default(),
			settings_model: crate::plot::theme::PlotSettings::default(),
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = SvgBackend::new(800.0, 480.0);
		widget.render(
			&mut backend,
			Rectangle::new([0.0, 0.0].into(), [800.0, 480.0].into()),
		);
		assert!(backend.finish().contains("<path"));
	}

	#[tokio::test]
	#[ignore = "requires the dogfood test2 relation on a live local Toron"]
	async fn live_product_0008_view_surface_preserves_axis_identity() {
		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let source = "ket.table(\"test2\").sort([col(\"cat\").asc()]).views([table(), bar(\"cat\", \"val\"), scatter(\"cat\", \"val\")])";
		let view = transform_rune_view(source).expect("PRODUCT-0008 view envelope");
		let view = view.into_relations().next().expect("one relation");
		let artifact = adapter
			.dispatch_rune(&view.source)
			.await
			.expect("one Rune relation result");
		let frame = artifact.expect_relation().projection().expect("Toron projection");
		assert!(frame.height() > 0, "dogfood relation is nonempty");
		assert_eq!(view.views.len(), 3);
		for view in view.views {
			match view.intent {
				RuneViewIntent::Table(_) => {
					assert!(frame.column("cat").is_ok());
					assert!(frame.column("val").is_ok());
				}
				RuneViewIntent::Plot(plot) => match plot.body {
					RunePlotBody::Single(plot) => {
						let expected_x = plot.x.clone();
						let expected_y = plot.y.clone();
						let projected = frame
							.select([plot.x.as_str(), plot.y.as_str()])
							.expect("project plot fields");
						let kernel = create_authored_plot(
							AuthoredPlotSpec::xy(plot.composition, plot.x, plot.y),
							&projected,
							800,
							480,
						)
						.expect("resolve relation plot fields");
						assert_eq!(kernel.x_label(), expected_x);
						assert_eq!(kernel.y_label(), expected_y);
					}
					RunePlotBody::StyledSingle(_) | RunePlotBody::Layered(_) => {
						panic!("this legacy fixture contains no layered sink")
					}
				},
			}
		}
	}

	#[tokio::test]
	#[ignore = "requires a live Toron SQL endpoint"]
	async fn live_toron_preserves_errors_and_accepts_cancel_frames() {
		let _live_gate = LIVE_TORON_TEST_LOCK.lock().await;
		let client =
			toron_client::ToronClient::loopback(live_endpoint()).expect("configure Toron client");
		client.health().await.expect("connect to Toron");
		let error = client
			.execute("SELEC this is deliberately invalid")
			.await
			.expect_err("invalid SQL is refused");
		assert!(matches!(
			error,
			toron_client::ClientError::Remote {
				code: toron_client::SqlErrorCode::SyntaxError,
				..
			}
		));
		client
			.cancel(toron_client::QueryId::from_u128(0x746f726f6e))
			.await
			.expect("Toron accepts the cancellation frame");
	}
}
