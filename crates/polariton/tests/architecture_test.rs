use std::path::Path;

#[allow(dead_code)]
#[path = "../src/coordinate.rs"]
mod coordinate;

#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{ProductBoundary, assert_product_dependency_boundary};

fn crate_root_modules(
	source: &str,
	end_marker: &str,
) -> std::collections::BTreeMap<String, Option<String>> {
	let mut modules = std::collections::BTreeMap::new();
	let mut pending_cfg = None;
	for line in source
		.split(end_marker)
		.next()
		.expect("bounded crate-root module list")
		.lines()
	{
		let line = line.trim();
		if line.starts_with("#[cfg(") {
			pending_cfg = Some(line.to_owned());
			continue;
		}
		if line.starts_with("#[path =")
			|| line.is_empty()
			|| line.starts_with("//")
			|| line.starts_with("#![")
		{
			continue;
		}
		if let Some(name) = line
			.strip_prefix("mod ")
			.and_then(|line| line.strip_suffix(';'))
		{
			assert!(
				modules
					.insert(name.to_owned(), pending_cfg.take())
					.is_none(),
				"duplicate root module {name}"
			);
		} else {
			pending_cfg = None;
		}
	}
	modules
}

#[test]
fn product_0057_render_evidence_mirrors_the_binary_module_graph() {
	let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
	let main = std::fs::read_to_string(manifest.join("src/main.rs")).expect("read binary root");
	let harness = std::fs::read_to_string(manifest.join("tests/product_0057_render_evidence.rs"))
		.expect("read PRODUCT-0057 render harness");
	assert_eq!(
		crate_root_modules(&harness, "use gui::"),
		crate_root_modules(&main, "fn main()"),
		"PRODUCT-0057 path-includes production modules; every binary-root module and feature gate must be mirrored so the opt-in GPU target stays compilable"
	);
}

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
fn product_0062_persistence_schema_has_no_placement_or_renderer_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let persistence = std::fs::read_to_string(root.join("session_persistence.rs"))
		.expect("read PRODUCT-0062 schema boundary");
	let dto = persistence
		.split("struct Payload {")
		.nth(1)
		.expect("payload DTO")
		.split("impl Payload {")
		.next()
		.expect("bounded DTO vocabulary");
	for forbidden in [
		"BufferId",
		"PaneId",
		"libaryon::core::ViewId",
		"PlotType",
		"PaneRect",
		"HostPanelDescriptor",
		"ratio",
		"tab_index",
		"placement",
	] {
		assert!(
			!dto.contains(forbidden),
			"PRODUCT-0062 DTO acquired placement or renderer authority through {forbidden}"
		);
	}
	assert!(dto.contains("enum AuthoredFamily"));
	for family in ["Scatter", "Bar", "Line", "Area"] {
		assert!(
			dto.contains(family),
			"missing durable authored family {family}"
		);
	}

	let mut serialization_sites = Vec::new();
	let mut pending = vec![root];
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(path).expect("read Polariton source") {
			let path = entry.expect("source entry").path();
			if path.is_dir() {
				pending.push(path);
			} else if path.extension().is_some_and(|extension| extension == "rs") {
				let source = std::fs::read_to_string(&path).expect("read source");
				if source.contains("use serde::") || source.contains("serde_json::") {
					serialization_sites.push(path);
				}
			}
		}
	}
	assert_eq!(
		serialization_sites,
		vec![Path::new(env!("CARGO_MANIFEST_DIR")).join("src/session_persistence.rs")],
		"serde belongs only to Polariton's one opaque session owner"
	);
}

#[test]
fn product_0001_polariton_reaches_nilket_only_through_the_public_client() {
	assert_product_dependency_boundary(
		Path::new(env!("CARGO_MANIFEST_DIR")),
		ProductBoundary::Polariton,
	);
}

#[test]
fn product_0049_coordinate_vocabulary_stays_closed_and_opaque() {
	use coordinate::{ALL_SPACES, SpaceName};

	let coordinate =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/coordinate.rs"))
			.expect("read coordinate vocabulary");

	assert_eq!(
		ALL_SPACES,
		&[
			SpaceName::Window,
			SpaceName::Widget,
			SpaceName::Cell,
			SpaceName::Plot,
			SpaceName::Content,
		],
		"the architecture gate must consume the real closed vocabulary"
	);
	assert!(
		coordinate.contains("mod sealed {")
			&& coordinate.contains("pub trait Space: sealed::Sealed"),
		"Space must remain sealed"
	);
	assert!(
		!coordinate.contains("Deref")
			&& !coordinate.contains("pub raw:")
			&& !coordinate.contains("pub space:"),
		"Pt and Rect must not expose their iced representation"
	);
	for assertion in [
		"size_of::<Point>()] = [(); size_of::<Pt<Window>>()",
		"align_of::<Point>()] = [(); align_of::<Pt<Window>>()",
		"size_of::<Rectangle>()] = [(); size_of::<Rect<Window>>()",
		"align_of::<Rectangle>()] = [(); align_of::<Rect<Window>>()",
	] {
		assert!(
			coordinate.contains(assertion),
			"missing static zero-cost assertion: {assertion}"
		);
	}
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
		// PRODUCT-0029 supersedes the former blanket `libaryon` ban: the
		// specification resolver may consume its resolved categorical-palette
		// fact. Dedicated PRODUCT-0028/0029 guards confine that exception and
		// still prohibit engine, renderer, and artifact policy here.
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
	let production = bar
		.split("#[cfg(test)]")
		.next()
		.expect("production bar kernel source");
	let prepared = production
		.split("pub struct BarPreparedData {")
		.nth(1)
		.and_then(|tail| tail.split("}\n").next())
		.expect("BarPreparedData body");
	assert!(
		!prepared.contains("bar_width")
			&& !prepared.contains("BarWidth")
			&& !prepared.contains("category_gap")
			&& !prepared.contains("CategoryGap"),
		"bar geometry settings must remain render state, not prepared relation data"
	);
	assert_eq!(
		production.matches(".bar_rectangles(").count(),
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
fn product_0053_category_gap_stays_in_the_closed_bar_register() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	assert!(
		envelope.contains("\"category_gap\"")
			&& envelope.contains("CategoryGap::new")
			&& envelope.contains("category_gap.map_or(Override::Inherit, Override::Set)"),
		"PRODUCT-0053 must lower the named modifier through the closed typed bar register"
	);
	assert!(
		!envelope.contains("HashMap<String, LayerModifier"),
		"category gap does not authorize open modifier dispatch"
	);
	// PRODUCT-0064 supersedes 0053's dashboard-only negative boundary by
	// making stacked bars an authored member of this same closed register.
	// Its geometry and hit tests prove the accepted equations behaviorally.
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
	assert!(artifact.contains("authored_show_column_types: Override<bool>"));
	assert_eq!(
		artifact.matches(':').count(),
		3,
		"the artifact owns one frame, one skin, and one authored type intent"
	);
	assert_eq!(
		production.matches("Table(TableArtifact)").count(),
		2,
		"host panels and markdown cells must share the atomic artifact"
	);
	assert!(
		production.contains("self.table_skin_at_draw(panel.panel_id, artifact.skin)"),
		"PRODUCT-0045 permits only the host pane to flatten its draw-time tier"
	);
	assert!(
		production.contains("Table::new(artifact.frame.as_ref(), 0, artifact.skin)"),
		"the Markdown consumer still renders the skin stored beside the frame"
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
	assert!(envelope_production.contains("pub(crate) skin: RuneTableSkinIntent"));
	assert!(envelope_production.contains("pub(crate) enum RuneTableSkinIntent"));
	assert!(envelope_production.contains("\"skin\" =>"));
	assert!(!envelope_production.contains("use crate::gui::table_skin"));
	assert!(!envelope_production.contains("TableSkin::"));
	assert!(!envelope_production.contains("TableRgba"));
	assert!(!envelope_production.contains("ResolvedThemePalette"));

	let messages = std::fs::read_to_string(root.join("gui/messages.rs")).expect("read messages");
	assert!(messages.contains("Table(RuneTablePresentation)"));
	let state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let state_production = state
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production GUI state source");
	assert!(state_production.contains("resolve_rune_table"));
	assert!(state_production.contains("TableSkinCascade"));
	assert!(state_production.contains("set_rune_view_table_with_presentation"));

	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read editor");
	let editor_production = editor
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production editor source");
	assert!(
		editor_production
			.contains("set_query_result_with_skin(frame, TableSkinCascade::library_floor())")
	);
	assert!(editor_production.contains(
		"set_markdown_cell_table_with_skin(identity, frame, TableSkinCascade::library_floor())"
	));
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
	assert!(state_production.contains("TableSkinCascade"));
	assert!(state_production.contains("TableSkinSpec::new"));
	assert!(state_production.contains("TableSkinProjection::Duotone"));
	assert!(state_production.contains("TableSkinProjection::PaletteForward"));

	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let envelope_production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production Rune envelope source");
	for forbidden in [
		"use crate::gui::table_skin",
		"TableSkin::",
		"TableRgba",
		"ResolvedThemePalette",
	] {
		assert!(
			!envelope_production.contains(forbidden),
			"Rune envelope acquired resolved GUI table vocabulary: {forbidden}"
		);
	}

	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read editor");
	let editor_production = editor
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production editor source");
	assert!(
		editor_production
			.contains("set_query_result_with_skin(frame, TableSkinCascade::library_floor())")
	);
	assert!(editor_production.contains(
		"set_markdown_cell_table_with_skin(identity, frame, TableSkinCascade::library_floor())"
	));
	for forbidden in ["TableSkinProjection", "PaletteForward", "Duotone"] {
		assert!(
			!editor_production.contains(forbidden),
			"editor artifact path acquired projection policy: {forbidden}"
		);
	}
}

#[test]
fn product_0027_surface_keeps_projection_intent_bounded_and_plot_registers_closed() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let envelope_production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production Rune envelope source");
	assert!(
		envelope_production.contains(
			"pub(crate) enum RuneTableSkinProjection {\n\tPaletteForward,\n\tDuotone,\n}"
		)
	);
	assert!(envelope_production.contains(
		"pub(crate) enum RuneTableSkinIntent {\n\t#[default]\n\tDefault,\n\tNamedTheme {"
	));
	assert!(envelope_production.contains("\"duotone\" =>"));
	assert!(envelope_production.contains("\"duotone() takes no arguments\""));
	assert!(envelope_production.contains("if duotone && skin_name.is_none()"));
	assert!(envelope_production.contains("\"duotone() requires skin(string_literal)\""));

	let mark_register = envelope_production
		.split("fn parse_modified_mark(")
		.nth(1)
		.expect("mark register")
		.split("fn parse_layer_modifier_factor(")
		.next()
		.expect("bounded mark register source");
	assert!(!mark_register.contains("\"skin\" =>"));
	assert!(!mark_register.contains("\"duotone\" =>"));

	let state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let state_production = state
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production GUI state source");
	assert!(state_production.contains("projection: RuneTableSkinProjection::PaletteForward"));
	assert!(state_production.contains("projection: RuneTableSkinProjection::Duotone"));
	assert!(
		!state_production
			.contains("the default palette-forward projection succeeds; remove .duotone()")
	);
	assert!(!state_production.contains("TableSkinProjection::default"));
	assert!(!state_production.contains("unwrap_or(TableSkinProjection"));
}

#[test]
fn product_0035_authored_table_contrast_is_observable_but_never_an_admission_gate() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let skin = std::fs::read_to_string(root.join("gui/table_skin.rs")).expect("read table skin");
	let skin_production = skin
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production table-skin source");
	assert!(skin_production.contains("pub fn visible_contrast_checks"));
	assert!(skin_production.contains("pub fn passes_contrast"));
	assert!(!skin_production.contains("UnsuitableDuotone"));
	assert!(!skin_production.contains("UnsuitablePaletteForward"));
	assert!(!skin_production.contains("validate_projected_skin"));

	let state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let state_production = state
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production GUI state source");
	assert!(!state_production.contains("remove .duotone()"));
	assert!(!state_production.contains("passes_contrast"));
	assert!(!state_production.contains("weakest_contrast"));
}

#[test]
fn product_0036_numeric_bounds_have_one_figure_producer_and_no_magnitude_gate() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let envelope_production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production Rune envelope source");
	let figure_parser = envelope_production
		.split("fn parse_figure(")
		.nth(1)
		.expect("figure parser")
		.split("fn parse_figure_bounds(")
		.next()
		.expect("bounded figure parser");
	assert!(figure_parser.contains("\"x_bounds\" =>"));
	assert!(figure_parser.contains("\"y_bounds\" =>"));
	assert_eq!(
		figure_parser
			.matches("authored_settings.domains = Override::Set")
			.count(),
		1,
		"both axes must lower through one grouped domain override"
	);

	let bounds_parser = envelope_production
		.split("fn parse_figure_bounds(")
		.nth(1)
		.expect("bounds parser")
		.split("fn parse_signed_numeric_literal(")
		.next()
		.expect("bounded bounds parser");
	assert!(bounds_parser.contains("!minimum.is_finite() || !maximum.is_finite()"));
	assert!(bounds_parser.contains("minimum == maximum"));
	assert!(bounds_parser.contains("minimum > maximum"));
	for forbidden in ["EPSILON", ".abs()", "maximum - minimum", "minimum_span"] {
		assert!(
			!bounds_parser.contains(forbidden),
			"numeric bounds acquired a magnitude admission rule through {forbidden}"
		);
	}

	let mark_parser = envelope_production
		.split("fn parse_modified_mark(")
		.nth(1)
		.expect("mark parser")
		.split("fn parse_layer_modifier_factor(")
		.next()
		.expect("bounded mark parser");
	assert!(!mark_parser.contains("x_bounds"));
	assert!(!mark_parser.contains("y_bounds"));

	let state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let state_production = state
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production GUI state source");
	assert!(state_production.contains("validate_rune_numeric_domain_applicability"));
	assert!(state_production.contains("requires a numeric x axis"));
	assert!(state_production.contains("requires a numeric y axis"));
}

#[test]
fn product_0037_tick_positions_have_one_closed_generator_and_no_surface_caller() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let common_production = common
		.split("#[cfg(test)]\nmod product_0031_pre_extraction_oracle")
		.next()
		.expect("plot common production");
	assert!(common_production.contains("pub enum TickPlacement"));
	assert!(common_production.contains("pub fn positions("));
	assert!(common_production.contains("pub fn minor_positions("));
	assert_eq!(
		common_production
			.matches("self.major_tick_positions(")
			.count(),
		10,
		"the generator helper plus nine major consumers must be present"
	);
	assert_eq!(
		common_production
			.matches("self.minor_tick_positions(")
			.count(),
		7,
		"all seven minor consumers must be present"
	);
	for forbidden in [
		"self.settings.x_ticks",
		"self.settings.y_ticks",
		"i as f64 / self.settings",
		"j as f64 / self.settings",
		"k as f64 / self.settings",
	] {
		assert!(
			!common_production.contains(forbidden),
			"tick site retained {forbidden}"
		);
	}

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	let theme_production = theme
		.split("#[cfg(test)]")
		.next()
		.expect("plot theme production");
	assert!(theme_production.contains("x_tick_placement: TickPlacement"));
	assert!(theme_production.contains("y_tick_placement: TickPlacement"));
	assert!(!theme_production.contains("x_ticks:"));
	assert!(!theme_production.contains("y_ticks:"));
}

#[test]
fn product_0038_tick_step_is_the_only_surface_producer_of_step_placement() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("Rune envelope production");
	let figure_parser = production
		.split("fn parse_figure(")
		.nth(1)
		.expect("figure parser")
		.split("fn parse_figure_bounds(")
		.next()
		.expect("bounded figure parser");
	assert!(figure_parser.contains("\"x_tick_step\" =>"));
	assert!(figure_parser.contains("\"y_tick_step\" =>"));
	assert_eq!(
		figure_parser
			.matches("authored_settings.guides = Override::Set")
			.count(),
		1
	);
	assert!(production.contains("fn parse_figure_tick_step("));
	assert!(production.contains("step must be strictly positive"));

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let common_production = common
		.split("#[cfg(test)]\nmod product_0031_pre_extraction_oracle")
		.next()
		.expect("plot common production");
	assert_eq!(common_production.matches("TickPlacement::step(").count(), 0);
}

#[test]
fn product_0039_minor_ticks_have_one_surface_producer_and_checked_total_capacity() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("Rune envelope production");
	let figure_parser = production
		.split("fn parse_figure(")
		.nth(1)
		.expect("figure parser")
		.split("fn parse_figure_bounds(")
		.next()
		.expect("bounded figure parser");
	assert!(figure_parser.contains("\"x_minor_ticks\" =>"));
	assert!(figure_parser.contains("\"y_minor_ticks\" =>"));
	assert!(production.contains("fn parse_figure_minor_ticks("));
	assert!(production.contains("count must be a non-negative integer"));

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let common_production = common
		.split("#[cfg(test)]\nmod product_0031_pre_extraction_oracle")
		.next()
		.expect("plot common production");
	assert!(common_production.contains("MinorCapacity"));
	assert!(common_production.contains("checked_mul(minor_intervals as usize)"));
	assert!(common_production.contains("checked_add(minor_count)"));
}

#[test]
fn product_0040_static_literal_bindings_resolve_only_at_the_envelope_boundary() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let production = envelope
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("Rune envelope production");
	assert!(production.contains("fn resolve_static_literal("));
	assert!(production.contains("fn resolve_static_literal_text("));
	assert!(production.contains("statement_start < use_start"));
	assert!(production.contains("binding {name:?} does not resolve to a literal"));
	assert!(production.contains("fn parse_figure_heading("));
	assert!(production.contains("fn parse_figure_bounds("));
	assert!(production.contains("fn parse_figure_tick_step("));
	assert!(production.contains("fn parse_figure_minor_ticks("));
	for forbidden in [
		"parse_all::<ast::EmptyBlock>(initializer",
		"rune::runtime",
		"rune::Vm",
	] {
		assert!(
			!production.contains(forbidden),
			"static literal resolution evaluated Rune through {forbidden}"
		);
	}
}

#[test]
fn product_0028_categorical_palettes_reach_only_the_product_0029_spec_consumer() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let libaryon_theme = manifest_dir.join("../libaryon/src/engine/theme.rs");
	let theme = std::fs::read_to_string(&libaryon_theme).expect("read libaryon theme source");
	let production = theme
		.split("#[cfg(test)]")
		.next()
		.expect("libaryon theme production source");

	assert_eq!(
		production.matches("parse_theme_modifiers(name)").count(),
		1,
		"PRODUCT-0028 must consume the existing modifier parser exactly once"
	);
	assert!(
		production.contains("ResolvedCategoricalPalette {\n\tfirst: Rgb,\n\trest: Box<[Rgb]>,"),
		"categorical palettes must keep private structurally non-empty storage"
	);
	for forbidden in [
		"pub first:",
		"pub rest:",
		"pub fn from_",
		"dedup",
		"CategoricalCapacityFailure",
		"geometry_limit",
		"ColorJob",
		"ColorIdentity",
		"PlotType",
	] {
		assert!(
			!production.contains(forbidden),
			"PRODUCT-0028 crossed or weakened its authority boundary through {forbidden}"
		);
	}

	let declared = production
		.lines()
		.filter(|line| line.contains("ThemeSpec::curated_categorical("))
		.map(|line| line.split('"').nth(1).expect("categorical theme name"))
		.collect::<Vec<_>>();
	assert_eq!(
		declared,
		[
			"dark2", "glowmax", "kelly", "prism", "tab20", "tab20b", "tab20c", "vapor",
		],
		"only curated palettes with an explicit accepted natural declaration are capable"
	);
	assert!(production.contains("categorical: None"));
	assert!(production.contains("categorical: Some(categorical)"));
	assert!(
		production.contains("categorical.map(|declaration|")
			&& production.contains("sampled_colors[1..].iter().copied()"),
		"categorical resolution must consume explicit curated declarations or accepted continuous samples"
	);

	let polariton_src = manifest_dir.join("src");
	let permitted_consumer = polariton_src.join("plot/spec/mod.rs");
	let mut pending = vec![polariton_src];
	let mut leaks = Vec::new();
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(path).expect("read Polariton source") {
			let path = entry.expect("Polariton source entry").path();
			if path.is_dir() {
				pending.push(path);
			} else if path.extension().and_then(|extension| extension.to_str()) == Some("rs")
				&& path != permitted_consumer
			{
				let source =
					std::fs::read_to_string(&path).expect("read Polariton production source");
				let source = source
					.split("#[cfg(test)]")
					.next()
					.expect("Polariton production source");
				for forbidden in ["ResolvedCategoricalPalette"] {
					if source.contains(forbidden) {
						leaks.push(format!("{}: {forbidden}", path.display()));
					}
				}
			}
		}
	}
	assert!(
		leaks.is_empty(),
		"PRODUCT-0028 categorical authority escaped PRODUCT-0029's spec resolver:\n{}",
		leaks.join("\n")
	);
	let spec = std::fs::read_to_string(&permitted_consumer).expect("read plot spec consumer");
	assert!(spec.contains("CategoricalPaletteSelection"));
	assert_eq!(spec.matches("Theme::try_new(identity)").count(), 1);
	assert!(spec.contains("theme.categorical_palette().cloned()"));
	assert!(
		"fn plot(p: ResolvedCategoricalPalette) { let _ = p.categorical_palette(); }"
			.contains("ResolvedCategoricalPalette"),
		"positive control: PRODUCT-0028 consumer assay must observe the forbidden API"
	);
}

#[test]
fn product_0029_positions_and_exact_band_fills_stay_inside_the_plot_boundary() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let production = spec
		.split("#[cfg(test)]")
		.next()
		.expect("plot spec production source");
	assert!(production.contains("pub struct PalettePosition(usize);"));
	assert!(production.contains("assignments: HashMap<ColorIdentityKey, PalettePosition>"));
	assert!(production.contains("pub band_fills: Vec<BandFillAssignment>"));
	assert!(production.contains("PRODUCT-0029 compatibility entrypoint"));
	assert!(production.contains("PRODUCT-0029's explicit categorical palette path"));
	assert!(production.contains("keeps the authored default in the Rune head"));
	assert!(production.contains("ResolvedKernelFields::Bar { category, .. }"));
	assert!(production.contains("channel.binding.channel == Channel::Color"));

	for relative in ["plot/factory.rs", "plot/kernels/bar.rs"] {
		let source = std::fs::read_to_string(root.join(relative)).expect("read render boundary");
		let source = source
			.split("#[cfg(test)]")
			.next()
			.expect("render production source");
		for forbidden in [
			"Theme::try_new",
			"ResolvedCategoricalPalette",
			"CategoricalPaletteSelection",
			"ColorIdentity",
			"evenly_spread_positions",
			"maximin_position",
		] {
			assert!(
				!source.contains(forbidden),
				"{relative} acquired palette/allocator policy through {forbidden}"
			);
		}
	}

	for relative in ["rune_relation_envelope.rs", "plot/durable.rs"] {
		let source = std::fs::read_to_string(root.join(relative)).expect("read caller boundary");
		for forbidden in [
			"resolve_domains_with_categorical_palette",
			"CategoricalPaletteSelection",
			"BandFillAssignment",
		] {
			assert!(
				!source.contains(forbidden),
				"{relative} selected PRODUCT-0029 before PRODUCT-0030 through {forbidden}"
			);
		}
	}

	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	assert!(request.contains("pub fn plan_with_categorical_palette("));
	assert!(request.contains("resolve_domains_with_categorical_palette("));
	assert!(request.contains("selection: &CategoricalPaletteSelection"));
	assert!(request.contains("identity: &ColorIdentity"));
	assert!(request.contains("Result<(ResolvedPlot, ColorIdentity), String>"));

	let head = std::fs::read_to_string(root.join("gui/state.rs")).expect("read Rune head");
	assert!(head.contains("rune_categorical_identities"));
	assert!(head.contains("plan_with_categorical_palette("));
	assert!(head.contains("resolve_categorical_palette_identity()"));
	assert!(!head.contains("Theme::try_new"));
	assert!(!head.contains("ResolvedCategoricalPalette"));
}

#[test]
fn product_0030_surface_default_and_continuation_stay_at_the_rune_head_boundary() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope =
		std::fs::read_to_string(root.join("rune_relation_envelope.rs")).expect("read envelope");
	let figure_parser = envelope
		.split("fn parse_figure(")
		.nth(1)
		.expect("figure parser")
		.split("fn parse_figure_heading(")
		.next()
		.expect("bounded figure parser");
	assert!(figure_parser.contains("\"palette\" =>"));
	assert!(figure_parser.contains("authored_settings.categorical_palette"));
	for forbidden in [
		"CategoricalPaletteSelection",
		"ResolvedCategoricalPalette",
		"ColorIdentity",
		"RuneCategoricalContinuation",
		"Theme::try_new",
	] {
		assert!(
			!figure_parser.contains(forbidden),
			"Rune normal form acquired resolved/head state through {forbidden}"
		);
	}
	let mark_register = envelope
		.split("fn parse_modified_mark(")
		.nth(1)
		.expect("mark register")
		.split("fn parse_layer_modifier_factor(")
		.next()
		.expect("bounded mark register");
	assert!(!mark_register.contains("\"palette\""));
	assert!(!mark_register.contains("\"skin\""));

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot settings");
	assert!(theme.contains("DEFAULT_CATEGORICAL_PALETTE: &'static str = \"viridis\""));
	assert!(theme.contains("resolve_categorical_palette_identity"));
	assert!(!theme.contains("Theme::try_new"));

	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let explicit = request
		.split("pub fn plan_with_categorical_palette(")
		.nth(1)
		.expect("explicit request")
		.split("fn project_named_channels(")
		.next()
		.expect("explicit request body");
	assert!(!explicit.contains("viridis"));
	assert!(!explicit.contains("Theme::try_new"));
	assert!(explicit.contains("selection: &CategoricalPaletteSelection"));
	assert!(explicit.contains("identity: &ColorIdentity"));

	let head = std::fs::read_to_string(root.join("gui/state.rs")).expect("read Rune head");
	assert!(head.contains("(BufferId, LocalViewIdentity, usize), ColorIdentity"));
	assert!(head.contains("prepare_rune_categorical_palettes("));
	assert!(head.contains("plan_with_categorical_palette("));
	let head_production = head
		.split("\n#[cfg(test)]\nmod tests")
		.next()
		.expect("head production source");
	let ready = head_production
		.split("Message::RuneRelationPlotReady {")
		.last()
		.expect("ready handler");
	let stale_check = ready
		.find("rune_buffer_revisions.get(&source_buffer) != Some(&revision)")
		.expect("stale check");
	let content = ready.find("set_rune_view_plot(").expect("content install");
	let continuation = ready
		.find("rune_categorical_identities.insert(")
		.expect("continuation commit");
	assert!(stale_check < content && content < continuation);

	for relative in ["plot/factory.rs", "plot/kernels/bar.rs", "editor.rs"] {
		let source = std::fs::read_to_string(root.join(relative)).expect("read unchanged boundary");
		let source = source
			.split("#[cfg(test)]")
			.next()
			.expect("production boundary");
		for forbidden in [
			"RuneCategoricalContinuation",
			"CategoricalPaletteSelection",
			"ColorIdentity",
		] {
			assert!(
				!source.contains(forbidden),
				"{relative} acquired Rune categorical continuation through {forbidden}"
			);
		}
	}
}

#[test]
fn product_0031_plot_chrome_is_closed_and_mark_ink_stays_separate() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let chrome =
		std::fs::read_to_string(root.join("plot/chrome.rs")).expect("read plot chrome model");
	let fields = chrome
		.split("pub struct PlotChrome {")
		.nth(1)
		.expect("PlotChrome declaration")
		.split('}')
		.next()
		.expect("PlotChrome fields");
	assert_eq!(fields.matches("pub ").count(), 11);
	for forbidden in [
		"Option<",
		"HashMap",
		"BTreeMap",
		"Theme",
		"UiPalette",
		"TableSkin",
	] {
		assert!(
			!fields.contains(forbidden),
			"PlotChrome acquired {forbidden}"
		);
	}

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let production = common
		.split("#[cfg(test)]")
		.next()
		.expect("plot common production");
	for retired in [
		"pub background_color:",
		"pub decoration_color:",
		"settings.background_color",
		"settings.decoration_color",
	] {
		assert!(!production.contains(retired), "renderer retained {retired}");
	}
	assert!(production.contains("pub chrome: PlotChrome"));
	assert!(production.contains("pub mark_decoration_color: Color"));

	for relative in [
		"plot/kernels/boxplot.rs",
		"plot/kernels/violin.rs",
		"plot/kernels/candlestick.rs",
		"plot/kernels/funnel.rs",
	] {
		let source = std::fs::read_to_string(root.join(relative)).expect("read mark kernel");
		let mark_draw = source
			.split("fn draw_legend(")
			.next()
			.expect("mark draw before legend");
		assert!(mark_draw.contains("mark_decoration_color"));
		assert!(
			!mark_draw.contains("settings.chrome"),
			"{relative} mark drawing acquired plot chrome"
		);
	}
}

#[test]
fn product_0033_plot_skin_projection_choice_is_bounded_and_renderer_free() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	let layer = theme
		.split("pub struct PlotSettingsLayer {")
		.nth(1)
		.expect("settings layer")
		.split('}')
		.next()
		.expect("settings fields");
	assert!(layer.contains("plot_skin: Override<PlotSkinSelection>"));
	assert!(layer.contains("chrome: Override<PlotChrome>"));

	let chrome = std::fs::read_to_string(root.join("plot/chrome.rs")).expect("read chrome");
	let production = chrome
		.split("#[cfg(test)]")
		.next()
		.expect("chrome production");
	assert_eq!(production.matches("from_theme_name").count(), 1);
	assert_eq!(production.matches("Theme::try_new").count(), 1);
	assert!(!production.contains("TableSkin"));
	assert!(!production.contains("UiPalette"));
	assert!(!production.contains("UnsuitableTheme"));
	assert_eq!(
		production
			.matches("pub enum PlotChromeProjection {")
			.count(),
		1
	);
	assert!(production.contains("Ordered"));
	assert!(production.contains("Duotone"));

	let envelope =
		std::fs::read_to_string(root.join("rune_relation_envelope.rs")).expect("read envelope");
	let figure_parser = envelope
		.split("fn parse_figure(")
		.nth(1)
		.expect("figure parser")
		.split("fn parse_figure_heading(")
		.next()
		.expect("bounded parser");
	assert!(figure_parser.contains("authored_settings.plot_skin"));
	let skin_parser = envelope
		.split("fn parse_figure_skin(")
		.nth(1)
		.expect("plot skin parser")
		.split("fn parse_figure_heading(")
		.next()
		.expect("bounded plot skin parser");
	assert_eq!(skin_parser.matches("\"duotone\"").count(), 1);
	assert!(!skin_parser.contains("TableSkin"));

	for relative in ["plot/spec/mod.rs", "plot/factory.rs"] {
		let source = std::fs::read_to_string(root.join(relative)).expect("read plot boundary");
		let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
		assert!(
			!production.contains("plot_skin"),
			"{relative} acquired skin identity"
		);
		assert!(
			!production.contains("PlotChrome::from_theme_name"),
			"{relative} resolves names below the completion head"
		);
	}
}

#[test]
fn product_0057_rendering_oracle_licenses_are_schema_bound() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manifest =
		std::fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read Polariton manifest");
	let production_dependencies = manifest
		.split("[dev-dependencies]")
		.next()
		.expect("production dependencies before development dependencies");
	assert!(
		!production_dependencies.contains("iced_wgpu"),
		"PRODUCT-0057's direct renderer dependency must stay test-only"
	);
	for required in [
		"product-0057-render-evidence = []",
		"iced_wgpu = \"0.14.0\"",
		"name = \"product_0057_render_evidence\"",
		"required-features = [\"product-0057-render-evidence\"]",
		"harness = false",
	] {
		assert!(
			manifest.contains(required),
			"missing evidence boundary: {required}"
		);
	}

	let source_root = manifest_dir.join("src");
	let common = std::fs::read_to_string(source_root.join("plot/common.rs"))
		.expect("read plot common source");
	for license in [
		"Backend-independent authored plot operations.",
		"cannot by itself prove production compositing",
		"PRODUCT-0057 named oracle `plot::common::tests::RecordingBackend`",
		"discards",
		"clip bounds",
	] {
		assert!(
			common.contains(license),
			"missing common oracle license: {license}"
		);
	}
	let common_recorder = common
		.split("struct RecordingBackend")
		.nth(1)
		.expect("named common recorder");
	assert!(common_recorder.contains("strokes: Vec<Vec<Point>>"));
	assert!(common_recorder.contains("texts: Vec<Text>"));
	assert!(common_recorder.contains("self.with_save(f);"));
	assert!(common_recorder.contains("fn fill_path") && common_recorder.contains("{}"));

	let table = std::fs::read_to_string(source_root.join("gui/table.rs"))
		.expect("read table renderer source");
	for license in [
		"PRODUCT-0057 named oracle `gui::table::tests::RecordingRenderer`",
		"text clip bounds",
		"does not shape or rasterize glyphs",
	] {
		assert!(
			table.contains(license),
			"missing table oracle license: {license}"
		);
	}
	let table_recorder = table
		.split("struct RecordingRenderer")
		.nth(1)
		.expect("named table recorder");
	for recorded in ["operations:", "active_layers:", "clip_bounds"] {
		assert!(
			table_recorder.contains(recorded),
			"table recorder no longer records {recorded}"
		);
	}

	for (relative, qualified) in [
		(
			"plot/kernels/heatmap.rs",
			"plot::kernels::heatmap::tests::RectangleBackend",
		),
		(
			"plot/kernels/stacked_bar.rs",
			"plot::kernels::stacked_bar::tests::RectangleBackend",
		),
		(
			"plot/factory.rs",
			"plot::factory::product_0013_baseline_tests::RectangleBackend",
		),
		("plot/grid.rs", "plot::grid::tests::RectangleBackend"),
	] {
		let source = std::fs::read_to_string(source_root.join(relative))
			.expect("read rectangle oracle source");
		assert_eq!(source.matches("struct RectangleBackend").count(), 1);
		assert!(
			source.contains(qualified),
			"{relative} lacks its qualified oracle name"
		);
		let recorder = source
			.split("struct RectangleBackend")
			.nth(1)
			.expect("rectangle recorder");
		assert!(recorder.contains("rectangles: Vec<(Point, Size"));
		assert!(recorder.contains("self.rectangles.push((top_left, size, color));"));
	}

	let export =
		std::fs::read_to_string(source_root.join("plot/export.rs")).expect("read export backends");
	for license in [
		"Serialized SVG oracle.",
		"cannot prove production-canvas compositing",
		"SVG-derived PNG exporter",
		"SVG-derived AVIF exporter",
	] {
		assert!(
			export.contains(license),
			"missing export oracle license: {license}"
		);
	}
	assert_eq!(export.matches("svg_backend: SvgBackend,\n").count(), 2);
	assert_eq!(
		export
			.matches("let svg_str = self.svg_backend.finish();")
			.count(),
		2,
		"PNG and AVIF must both rasterize the SVG stream"
	);
}
