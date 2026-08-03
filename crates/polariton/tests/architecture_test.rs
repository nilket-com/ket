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
	for unaccepted in ["EncodedViewSet", "SinkSpec", ".views("] {
		assert!(
			!rune_surface.contains(unaccepted),
			"PRODUCT-0007 lowered an unaccepted Rune spelling: {unaccepted}"
		);
	}
}
