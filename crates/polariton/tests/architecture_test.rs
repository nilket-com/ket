use std::path::Path;

#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{ProductBoundary, assert_product_dependency_boundary};

fn envelope_leaks(source: &str, is_envelope: bool) -> Vec<&'static str> {
	let mut findings = Vec::new();
	if !is_envelope {
		for (needle, label) in [
			(
				"ast::EmptyBlock",
				"terminal-expression parsing escaped the envelope module",
			),
			(
				"ENTRYPOINT_PREFIX",
				"view-source rewriting escaped the terminal-envelope module",
			),
		] {
			if source.contains(needle) {
				findings.push(label);
			}
		}
	}
	for (needle, label) in [
		("RuneDataFrame", "Toron relation type entered Polariton"),
		("LogicalPlan", "Toron private plan entered Polariton"),
	] {
		if source.contains(needle) {
			findings.push(label);
		}
	}
	findings
}

#[test]
fn product_0001_polariton_reaches_nilket_only_through_the_public_client() {
	assert_product_dependency_boundary(
		Path::new(env!("CARGO_MANIFEST_DIR")),
		ProductBoundary::Polariton,
	);
}

#[test]
fn product_0003_terminal_envelope_is_the_only_relation_view_transformation_site() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut findings = Vec::new();
	let mut pending = vec![root];
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(&path).expect("read Polariton source") {
			let entry = entry.expect("source entry");
			let path = entry.path();
			if path.is_dir() {
				pending.push(path);
				continue;
			}
			if path.extension().and_then(|ext| ext.to_str()) != Some("rs") {
				continue;
			}
			let source = std::fs::read_to_string(&path).expect("read source");
			let is_envelope = path.file_name().and_then(|name| name.to_str())
				== Some("rune_relation_envelope.rs");
			for finding in envelope_leaks(&source, is_envelope) {
				findings.push(format!("{}: {finding}", path.display()));
			}
		}
	}
	assert!(
		findings.is_empty(),
		"PRODUCT-0003 containment violation:\n{}",
		findings.join("\n")
	);
	assert_eq!(
		envelope_leaks(
			"let _: ast::EmptyBlock; fn leaked(_: LogicalPlan) {}",
			false
		),
		vec![
			"terminal-expression parsing escaped the envelope module",
			"Toron private plan entered Polariton"
		],
		"positive control: the containment assay must observe a synthetic violation"
	);
}

fn product_0005_private_discovery_leaks(source: &str) -> Vec<&'static str> {
	let mut findings = Vec::new();
	for (needle, label) in [
		(
			"runtime.json",
			"private runtime-manifest path entered Polariton",
		),
		(
			"127.0.0.1:7742",
			"canonical local endpoint was duplicated in Polariton",
		),
		(
			"struct RuntimeManifest",
			"runtime schema was redefined in Polariton",
		),
		(
			"struct ProfileMarker",
			"profile schema was redefined in Polariton",
		),
	] {
		if source.contains(needle) {
			findings.push(label);
		}
	}
	findings
}

#[test]
fn product_0005_polariton_uses_only_public_local_workspace_discovery() {
	let manifest =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
			.expect("read Polariton manifest");
	assert!(
		manifest.contains("nilket-local"),
		"PRODUCT-0005 requires Polariton to consume the public discovery crate"
	);

	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut pending = vec![root];
	let mut findings = Vec::new();
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(&path).expect("read Polariton source") {
			let path = entry.expect("source entry").path();
			if path.is_dir() {
				pending.push(path);
			} else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
				let source = std::fs::read_to_string(&path).expect("read Polariton source file");
				for finding in product_0005_private_discovery_leaks(&source) {
					findings.push(format!("{}: {finding}", path.display()));
				}
			}
		}
	}
	assert!(
		findings.is_empty(),
		"PRODUCT-0005 discovery authority violation:\n{}",
		findings.join("\n")
	);
	assert_eq!(
		product_0005_private_discovery_leaks(
			"struct RuntimeManifest; const ENDPOINT: &str = \"127.0.0.1:7742\";"
		),
		vec![
			"canonical local endpoint was duplicated in Polariton",
			"runtime schema was redefined in Polariton",
		],
		"positive control: PRODUCT-0005 guard must observe private schema and endpoint copies"
	);
}

#[test]
fn product_0006_creation_authority_stays_feature_gated_and_out_of_discovery() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manifest =
		std::fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read Polariton manifest");
	assert_product_dependency_boundary(manifest_dir, ProductBoundary::Polariton);
	assert!(
		manifest.contains("default = [\"toron\"]"),
		"the dogfood Polariton build must include Toron unless explicitly reduced"
	);
	assert!(manifest.contains("nix = {"));
	assert!(manifest.contains("optional = true"));
	assert!(manifest.contains("\"dep:nix\""));

	let main =
		std::fs::read_to_string(manifest_dir.join("src/main.rs")).expect("read Polariton main");
	assert!(
		main.contains("#[cfg(feature = \"toron\")]\nmod local_workspace;"),
		"PRODUCT-0006 process authority must disappear without the Toron feature"
	);
	let owner = std::fs::read_to_string(manifest_dir.join("src/local_workspace.rs"))
		.expect("read session owner");
	for required in [
		"std::thread",
		"set_pdeathsig(Some(parent_death_signal))",
		"Signal::SIGINT",
		"process_group(0)",
		"getppid() != parent",
		"nilket_local::discover",
	] {
		assert!(
			owner.contains(required),
			"PRODUCT-0006 owner lost required lifecycle evidence {required}"
		);
	}
	assert!(
		!owner.contains("LocalReady::parse") && !owner.contains("LOCAL_READY v="),
		"creation must not infer readiness from supervisor output"
	);

	let discovery =
		std::fs::read_to_string(manifest_dir.join("../../nilket/crates/nilket-local/src/lib.rs"))
			.expect("read PRODUCT-0005 discovery source");
	let production = discovery
		.split("#[cfg(test)]")
		.next()
		.expect("production discovery source");
	for forbidden in ["Command::new", "set_pdeathsig", "libc::kill", "try_lock"] {
		assert!(
			!production.contains(forbidden),
			"PRODUCT-0006 leaked lifecycle authority into discovery: {forbidden}"
		);
	}
	assert!(
		"Command::new(\"nilket\"); set_pdeathsig(Some(Signal::SIGINT));".contains("Command::new"),
		"positive control: PRODUCT-0006 lifecycle assay must fire"
	);
}

fn product_0007_spec_leaks(source: &str, is_spec: bool) -> Vec<&'static str> {
	let mut findings = Vec::new();
	for (needle, label) in [
		(
			"crate::plot::kernels",
			"plot specification depends on a kernel",
		),
		(
			"crate::plot::common",
			"plot specification depends on renderer state",
		),
		(
			"libaryon",
			"plot specification depends on the interface engine",
		),
	] {
		if is_spec && source.contains(needle) {
			findings.push(label);
		}
	}
	if !is_spec && source.contains("ChannelBinding") {
		findings.push("intermediate channel binding escaped the specification module");
	}
	findings
}

fn product_0007_local_transform_leaks(source: &str) -> Vec<&'static str> {
	let mut findings = Vec::new();
	for (needle, label) in [
		("LazyFrame", "lazy relation plan"),
		("LazyGroupBy", "grouped relation plan"),
		("JoinArgs", "join"),
		("SortMultipleOptions", "sort"),
		("DataFrame::filter", "filter"),
	] {
		if source.contains(needle) {
			findings.push(label);
		}
	}
	findings
}

fn product_0007_render_inference_leaks(source: &str) -> Vec<&'static str> {
	let mut findings = Vec::new();
	for (needle, label) in [
		("numeric_cols", "numeric dtype partition"),
		("string_cols", "string dtype partition"),
		("df.columns()", "positional frame-column scan"),
		("unwrap_or(x)", "x substituted for another channel"),
		("unwrap_or(y)", "y substituted for another channel"),
		("unwrap_or(open)", "open substituted for another channel"),
		("unwrap_or(high)", "high substituted for another channel"),
		("unwrap_or(low)", "low substituted for another channel"),
	] {
		if source.contains(needle) {
			findings.push(label);
		}
	}
	findings
}

#[test]
fn product_0007_plot_spec_stays_product_side_and_cannot_execute_relations() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec_path = src.join("plot/spec/mod.rs");
	let spec = std::fs::read_to_string(&spec_path).expect("read PRODUCT-0007 specification");
	assert!(
		product_0007_spec_leaks(&spec, true).is_empty(),
		"PRODUCT-0007 specification crossed the renderer/engine boundary"
	);
	assert!(
		product_0007_local_transform_leaks(&spec).is_empty(),
		"PRODUCT-0007 specification grew a local relation engine: {:?}",
		product_0007_local_transform_leaks(&spec)
	);

	let mut pending = vec![src];
	let mut leaks = Vec::new();
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(&path).expect("read Polariton source") {
			let path = entry.expect("source entry").path();
			if path.is_dir() {
				pending.push(path);
			} else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
				&& path != spec_path
			{
				let source = std::fs::read_to_string(&path).expect("read source");
				for finding in product_0007_spec_leaks(&source, false) {
					leaks.push(format!("{}: {finding}", path.display()));
				}
			}
		}
	}
	assert!(
		leaks.is_empty(),
		"PRODUCT-0007 intermediate type escaped:\n{}",
		leaks.join("\n")
	);

	assert_eq!(
		product_0007_spec_leaks("use crate::plot::kernels;", true),
		vec!["plot specification depends on a kernel"],
		"positive control: PRODUCT-0007 dependency guard must fire"
	);
	assert_eq!(
		product_0007_local_transform_leaks("let _: LazyFrame; let _: JoinArgs;"),
		vec!["lazy relation plan", "join"],
		"positive control: PRODUCT-0007 local-execution guard must fire"
	);
	assert_eq!(
		product_0007_spec_leaks("fn render(_: ChannelBinding) {}", false),
		vec!["intermediate channel binding escaped the specification module"],
		"positive control: PRODUCT-0007 intermediate-type guard must fire"
	);
}

#[test]
fn product_0007_rendering_consumes_resolved_fields_without_inference() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plot");
	let mut sources = vec![src.join("factory.rs")];
	for entry in std::fs::read_dir(src.join("kernels")).expect("read plot kernels") {
		let path = entry.expect("kernel entry").path();
		if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
			sources.push(path);
		}
	}
	let mut leaks = Vec::new();
	for path in sources {
		let source = std::fs::read_to_string(&path).expect("read rendering source");
		for finding in product_0007_render_inference_leaks(&source) {
			leaks.push(format!("{}: {finding}", path.display()));
		}
	}
	assert!(
		leaks.is_empty(),
		"PRODUCT-0007 rendering inferred or substituted a channel:\n{}",
		leaks.join("\n")
	);
	assert_eq!(
		product_0007_render_inference_leaks(
			"let numeric_cols = df.columns(); let y = values.get(1).unwrap_or(x);"
		),
		vec![
			"numeric dtype partition",
			"positional frame-column scan",
			"x substituted for another channel"
		],
		"positive control: PRODUCT-0007 rendering guard must fire"
	);
}

#[test]
fn product_0007_stage_three_stays_session_scoped_and_keeps_ui_out_of_data_color() {
	let plot = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plot");
	let durable = std::fs::read_to_string(plot.join("durable.rs")).expect("read durable plots");
	let durable_production = durable
		.split("#[cfg(test)]")
		.next()
		.expect("durable production source");
	for forbidden in [
		"std::fs",
		"serde",
		"toml",
		"workspace artifact",
		"toron_client",
		"RuneView",
	] {
		assert!(
			!durable_production.contains(forbidden),
			"PRODUCT-0007 session durability acquired persistence or execution authority: {forbidden}"
		);
	}
	for required in [
		"DurabilityMode::Live",
		"DurabilityMode::Pinned",
		"ResolvedLayout::from_spec",
		"RevisionObservation::Unavailable",
		"RevisionObservation::Unverifiable",
	] {
		assert!(
			durable_production.contains(required),
			"PRODUCT-0007 durability lost required evidence: {required}"
		);
	}

	let theme = std::fs::read_to_string(plot.join("theme.rs")).expect("read plot theme");
	let common = std::fs::read_to_string(plot.join("common.rs")).expect("read renderer settings");
	assert!(theme.contains("pub struct PlotSettings {"));
	assert!(common.contains("pub struct ResolvedPlotSettings {"));
	assert!(
		!common.contains("pub struct PlotSettings {"),
		"the flattened renderer value must not masquerade as the five-layer settings model"
	);
	let bridge = theme
		.split("pub fn chrome_from_ui_palette")
		.nth(1)
		.and_then(|tail| tail.split("#[derive").next())
		.expect("isolated UI palette bridge");
	for role in ["background", "axis", "gridline", "label"] {
		assert!(bridge.contains(role), "plot chrome bridge lost {role}");
	}
	for forbidden in [
		"categorical_ramp",
		"sequential_ramp",
		"diverging_ramp",
		"status_ramp",
	] {
		assert!(
			!bridge.contains(forbidden),
			"UiPalette crossed into data-encoding color through {forbidden}"
		);
	}
	let layer_order = [
		"library_defaults",
		"application_theme",
		"authored_spec",
		"layer",
		"pane_override",
	];
	let mut cursor = 0;
	for layer in layer_order {
		let offset = theme[cursor..]
			.find(layer)
			.unwrap_or_else(|| panic!("missing PRODUCT-0007 cascade layer {layer}"));
		cursor += offset + layer.len();
	}

	assert!(
		"let bytes = serde::to_vec(&plot); std::fs::write(path, bytes);".contains("std::fs"),
		"positive control: session-scope guard must recognize persistence"
	);
	assert!(
		"fn bridge(p: UiPalette) { categorical_ramp = p.accent; }".contains("categorical_ramp"),
		"positive control: data-color bridge guard must recognize a ramp"
	);

	let rune_surface = ["rune_host.rs", "rune_relation_envelope.rs"]
		.into_iter()
		.map(|name| {
			std::fs::read_to_string(plot.parent().unwrap().join(name)).expect("read Rune surface")
		})
		.collect::<String>();
	// PRODUCT-0008 accepts `.views([...])`; the other two spellings remain
	// withdrawn or explicitly undecided and therefore stay guarded here.
	for unaccepted in ["EncodedViewSet", "SinkSpec"] {
		assert!(
			!rune_surface.contains(unaccepted),
			"PRODUCT-0007 lowered an unaccepted Rune spelling: {unaccepted}"
		);
	}
}

#[test]
fn product_0010_relation_views_complete_without_dashboard_routing() {
	let source =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/gui/state.rs"))
			.expect("read Polariton GUI state");
	let handler = source
		.split("\n\t\tMessage::RuneRelationPlotReady {")
		.nth(1)
		.and_then(|tail| tail.split("Message::RunMarkdownCellResult {").next())
		.expect("isolate Rune relation plot completion handler");
	assert!(
		handler.contains("set_rune_view_plot"),
		"PRODUCT-0010 plots must resolve through their ordinary host panel"
	);
	for forbidden in ["open_dashboard", "app_state.dashboard"] {
		assert!(
			!handler.contains(forbidden),
			"PRODUCT-0010 relation completion reacquired dashboard routing: {forbidden}"
		);
	}
	assert!(
		"open_dashboard(); app_state.dashboard.apply_plot()".contains("open_dashboard"),
		"positive control: dashboard-routing assay must observe the old path"
	);
}

#[test]
fn product_0018_rune_surface_keeps_point_size_out_of_prepared_data() {
	let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let scatter = std::fs::read_to_string(source.join("plot/kernels/scatter.rs"))
		.expect("read scatter kernel");
	assert!(
		!scatter.contains("point_size_px"),
		"point size must not return to ScatterPreparedData or preparation arguments"
	);
	let envelope = std::fs::read_to_string(source.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	assert!(
		envelope.contains("\"point_size\"")
			&& envelope.contains("PointSize::new")
			&& envelope.contains("LayerSettings"),
		"PRODUCT-0018 must validate the closed Rune spelling through PRODUCT-0017's typed handoff"
	);
}

#[test]
fn product_0019_stroke_width_is_render_state_not_prepared_line_data() {
	let source = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plot/kernels/line.rs");
	let line = std::fs::read_to_string(source).expect("read line kernel");
	let prepared = line
		.split("pub struct LinePreparedData {")
		.nth(1)
		.and_then(|tail| tail.split("pub fn prepare_line_data").next())
		.expect("isolate LinePreparedData");
	assert!(
		!prepared.contains("stroke_width") && !prepared.contains("StrokeWidth"),
		"stroke width must remain render-time state outside LinePreparedData"
	);
	assert!(
		line.contains("settings.stroke_width.width_px()"),
		"the line renderer must consume resolved stroke width"
	);
}

#[test]
fn product_0020_extends_the_closed_typed_modifier_register() {
	let envelope = std::fs::read_to_string(
		Path::new(env!("CARGO_MANIFEST_DIR")).join("src/rune_relation_envelope.rs"),
	)
	.expect("read Rune envelope");
	assert!(
		envelope.contains("\"stroke_width\"")
			&& envelope.contains("StrokeWidth::new")
			&& envelope.contains("stroke_width.map_or(Override::Inherit, Override::Set)"),
		"PRODUCT-0020 must lower the named modifier through PRODUCT-0019's typed handoff"
	);
	assert!(
		!envelope.contains("HashMap<String, LayerModifier"),
		"the second setting does not authorize an open modifier map"
	);
}

#[test]
fn product_0021_bar_width_is_render_state_without_bar_layer_admission() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let bar = std::fs::read_to_string(root.join("plot/kernels/bar.rs")).expect("read bar kernel");
	let prepared = bar
		.split("pub struct BarPreparedData {")
		.nth(1)
		.and_then(|tail| tail.split("}\n").next())
		.expect("BarPreparedData body");
	assert!(
		!prepared.contains("bar_width") && !prepared.contains("BarWidth"),
		"bar width must remain render state, not prepared relation data"
	);
	assert_eq!(
		bar.matches("bar_rectangles(transform").count(),
		2,
		"paint and hover must consume the same bar rectangle calculation"
	);

	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let marks = spec
		.split("pub enum LayerMark {")
		.nth(1)
		.and_then(|tail| tail.split("}\n").next())
		.expect("LayerMark body");
	assert!(marks.contains("Line") && marks.contains("Scatter"));
	assert!(
		!marks.contains("Bar"),
		"styleability must not admit bar layering"
	);
}

#[test]
fn product_0022_bar_width_extends_only_the_closed_styled_single_register() {
	let envelope = std::fs::read_to_string(
		Path::new(env!("CARGO_MANIFEST_DIR")).join("src/rune_relation_envelope.rs"),
	)
	.expect("read Rune envelope");
	assert!(
		envelope.contains("pub(crate) struct StyledRunePlotSink")
			&& envelope.contains("\"bar_width\"")
			&& envelope.contains("BarWidth::new")
			&& envelope.contains("bar_width.map_or(Override::Inherit, Override::Set)"),
		"PRODUCT-0022 must use the general styled-single carrier and typed substrate"
	);
	assert!(
		!envelope.contains("HashMap<String, LayerModifier"),
		"the third setting does not authorize open modifier dispatch"
	);
}

#[test]
fn product_0023_table_skin_is_closed_complete_and_separate_from_other_color_models() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let skin = std::fs::read_to_string(root.join("gui/table_skin.rs")).expect("read table skin");
	let fields = skin
		.split("pub struct TableSkin {")
		.nth(1)
		.and_then(|tail| tail.split("}\n").next())
		.expect("TableSkin body");
	for role in [
		"body_background",
		"header_background",
		"gutter_background",
		"row_even_background",
		"row_odd_background",
		"gridline",
		"cell_text",
		"header_text",
		"type_text",
		"gutter_text",
		"selection_fill",
		"scrollbar_thumb",
	] {
		assert!(
			fields.contains(&format!("pub {role}: TableRgba")),
			"missing {role}"
		);
	}
	assert_eq!(
		fields.matches(": TableRgba").count(),
		12,
		"the resolved skin must contain exactly the accepted roles"
	);
	assert!(!fields.contains("Option<") && !fields.contains("HashMap"));
	assert!(
		!skin.contains("ColorTheme")
			&& !skin.contains("PlotThemeSpec")
			&& !skin.contains("UiPalette"),
		"table artifact skin must remain separate from ramps and editor chrome"
	);
}

#[test]
fn product_0023_table_draw_reads_only_its_required_resolved_skin() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let table = std::fs::read_to_string(root.join("gui/table.rs")).expect("read table widget");
	let production = table
		.split("#[cfg(test)]")
		.next()
		.expect("production table source");
	assert!(
		production
			.contains("pub fn new(data_frame: &'a DataFrame, row_offset: usize, skin: TableSkin)")
	);
	assert!(production.contains("skin: TableSkin"));
	assert!(
		!production.contains("colors::")
			&& !production.contains("UiPalette")
			&& !production.contains("ColorTheme"),
		"the table draw path must consume only its resolved artifact skin"
	);
}

#[test]
fn product_0023_table_artifact_is_the_atomic_host_and_markdown_boundary() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read editor");
	let production = editor
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production editor source");
	let artifact = production
		.split("struct TableArtifact {")
		.nth(1)
		.and_then(|tail| tail.split("}\n").next())
		.expect("TableArtifact body");
	assert!(artifact.contains("frame: Box<DataFrame>"));
	assert!(artifact.contains("skin: TableSkin"));
	assert_eq!(
		artifact.matches(':').count(),
		2,
		"the artifact owns one frame and one skin"
	);
	assert_eq!(
		production.matches("Table(TableArtifact)").count(),
		2,
		"host panels and markdown cells must share the atomic artifact"
	);
	assert_eq!(
		production
			.matches("Table::new(artifact.frame.as_ref(), 0, artifact.skin)")
			.count(),
		2,
		"both consumers must render the skin stored beside the frame"
	);
	assert!(
		production.contains("produced.push(preview_row(markdown_cell_output_surface("),
		"the Markdown table artifact must remain inside the existing preview-row wrapper"
	);

	let libaryon = Path::new(env!("CARGO_MANIFEST_DIR")).join("../libaryon/src");
	let mut pending = vec![libaryon];
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(path).expect("read libaryon source") {
			let path = entry.expect("libaryon entry").path();
			if path.is_dir() {
				pending.push(path);
			} else if path.extension().and_then(|extension| extension.to_str()) == Some("rs") {
				let source = std::fs::read_to_string(&path).expect("read libaryon file");
				assert!(
					!source.contains("TableSkin") && !source.contains("TableArtifact"),
					"table artifact chrome crossed into editor protocol at {}",
					path.display()
				);
			}
		}
	}
}

#[test]
fn product_0025_theme_projection_and_table_register_keep_their_authority_boundaries() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let skin = std::fs::read_to_string(root.join("gui/table_skin.rs")).expect("read table skin");
	let skin_production = skin
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production table-skin source");
	assert!(skin_production.contains("Theme::try_new(name)"));
	assert!(skin_production.contains("theme.palette()"));
	assert!(!skin_production.contains("THEME_SPECS"));
	assert!(!skin_production.contains("parse_theme_modifiers"));
	assert!(!skin_production.contains("UiPalette"));

	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let envelope_production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production envelope source");
	assert!(envelope_production.contains("pub(crate) struct RuneTableSink"));
	assert!(envelope_production.contains("pub(crate) skin_name: Option<String>"));
	assert!(envelope_production.contains("\"skin\" =>"));
	assert!(!envelope_production.contains("TableSkin"));
	assert!(!envelope_production.contains("ResolvedThemePalette"));

	let messages = std::fs::read_to_string(root.join("gui/messages.rs")).expect("read messages");
	assert!(messages.contains("Table(TableSkin)"));
	let state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let state_production = state
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production GUI state source");
	assert!(
		state_production
			.contains("TableSkin::from_theme_name(&name, TableSkinProjection::Duotone)")
	);
	assert!(state_production.contains("set_rune_view_table_with_skin"));

	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read editor");
	let editor_production = editor
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production editor source");
	assert!(editor_production.contains("set_query_result_with_skin(frame, TableSkin::DEFAULT)"));
	assert!(
		editor_production
			.contains("set_markdown_cell_table_with_skin(identity, frame, TableSkin::DEFAULT)")
	);
	assert!(!editor_production.contains("TableSkin::from_theme_name"));
}

#[test]
fn product_0026_projection_choice_stays_explicit_and_outside_rune_and_editor_chrome() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let skin = std::fs::read_to_string(root.join("gui/table_skin.rs")).expect("read table skin");
	let skin_production = skin
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production table-skin source");
	assert!(
		skin_production
			.contains("pub enum TableSkinProjection {\n\tPaletteForward,\n\tDuotone,\n}")
	);
	assert!(!skin_production.contains("impl Default for TableSkinProjection"));
	assert!(skin_production.contains("projection: TableSkinProjection"));
	assert_eq!(skin_production.matches("Theme::try_new(name)").count(), 1);
	assert!(skin_production.contains("fn project_duotone("));
	assert!(skin_production.contains("fn project_palette_forward("));
	assert!(!skin_production.contains("pub fn project_duotone("));
	assert!(!skin_production.contains("pub fn project_palette_forward("));
	assert!(!skin_production.contains("UiPalette"));

	let state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let state_production = state
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production GUI state source");
	assert_eq!(
		state_production
			.matches("TableSkin::from_theme_name")
			.count(),
		1
	);
	assert_eq!(
		state_production
			.matches("TableSkinProjection::Duotone")
			.count(),
		1
	);
	assert!(!state_production.contains("TableSkinProjection::PaletteForward"));

	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let envelope_production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production Rune envelope source");
	for forbidden in [
		"TableSkinProjection",
		"PaletteForward",
		"Duotone",
		"duotone",
	] {
		assert!(
			!envelope_production.contains(forbidden),
			"Rune envelope acquired table projection vocabulary: {forbidden}"
		);
	}

	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read editor");
	let editor_production = editor
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production editor source");
	assert!(editor_production.contains("set_query_result_with_skin(frame, TableSkin::DEFAULT)"));
	assert!(
		editor_production
			.contains("set_markdown_cell_table_with_skin(identity, frame, TableSkin::DEFAULT)")
	);
	for forbidden in ["TableSkinProjection", "PaletteForward", "Duotone"] {
		assert!(
			!editor_production.contains(forbidden),
			"editor artifact path acquired projection policy: {forbidden}"
		);
	}
}
