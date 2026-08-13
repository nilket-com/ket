use std::path::Path;

#[allow(dead_code)]
#[path = "../src/coordinate.rs"]
mod coordinate;

#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{ProductBoundary, assert_product_dependency_boundary};

fn rust_files(root: &Path) -> Vec<std::path::PathBuf> {
	let mut pending = vec![root.to_path_buf()];
	let mut files = Vec::new();
	while let Some(path) = pending.pop() {
		for entry in std::fs::read_dir(path).expect("read source directory") {
			let path = entry.expect("read source entry").path();
			if path.is_dir() {
				pending.push(path);
			} else if path.extension().and_then(|value| value.to_str()) == Some("rs") {
				files.push(path);
			}
		}
	}
	files.sort();
	files
}

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

fn envelope_leaks(source: &str, is_transport_boundary: bool) -> Vec<&'static str> {
	let mut findings = Vec::new();
	if !is_transport_boundary {
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
	assert!(dto.contains("family: String"));
	assert!(!dto.contains("enum AuthoredFamily"));

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
fn product_0086_live_family_catalog_is_source_complete_and_renderer_free() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs"))
		.expect("read PRODUCT-0086 colour lineage");
	let family_impl = spec
		.split("impl AuthoredFamilyId {")
		.nth(1)
		.expect("AuthoredFamilyId implementation")
		.split("impl ColorLayerIdentity {")
		.next()
		.expect("bounded family implementation");
	let constants = family_impl
		.lines()
		.filter_map(|line| {
			let line = line.trim();
			let rest = line.strip_prefix("pub(crate) const ")?;
			let (name, declaration) = rest.split_once(':')?;
			declaration
				.trim_start()
				.starts_with("Self =")
				.then_some(name)
		})
		.collect::<Vec<_>>();
	assert_eq!(constants, ["SCATTER", "BAR", "LINE", "AREA"]);
	let all = family_impl
		.split("pub(crate) const ALL:")
		.nth(1)
		.expect("AuthoredFamilyId::ALL")
		.split("= [")
		.nth(1)
		.expect("ALL array value")
		.split("];")
		.next()
		.expect("bounded ALL declaration");
	for constant in &constants {
		assert!(
			all.contains(&format!("Self::{constant}")),
			"family constant {constant} is absent from AuthoredFamilyId::ALL"
		);
	}
	assert_eq!(all.matches("Self::").count(), constants.len());

	let color_identity = spec
		.split("struct ColorIdentityKey")
		.nth(1)
		.expect("colour identity key")
		.split("pub struct ColorIdentity")
		.next()
		.expect("bounded colour-key region");
	assert!(color_identity.contains("layer: ColorLayerIdentity"));
	assert!(!color_identity.contains("PlotType"));
	let production_spec = spec
		.split("#[cfg(test)]")
		.next()
		.expect("production plot specification region");
	assert_eq!(
		production_spec
			.matches("pub(crate) fn from_authored_layer")
			.count(),
		1,
		"authored plot types must enter colour lineage through one conversion authority"
	);
	assert_eq!(
		production_spec
			.matches("ColorLayerIdentity::from_authored_layer")
			.count(),
		2,
		"both categorical allocation paths must use the conversion authority"
	);

	let persistence = std::fs::read_to_string(root.join("session_persistence.rs"))
		.expect("read PRODUCT-0086 owner codec");
	let production = persistence
		.split("#[cfg(test)]")
		.next()
		.expect("production persistence region");
	assert!(production.contains("family: String"));
	assert!(!production.contains("enum AuthoredFamily"));
	assert!(!production.contains("PlotType::"));
}

#[test]
fn product_0086_wire_numbers_do_not_become_session_code_vocabulary() {
	fn words(source: &str) -> Vec<String> {
		source
			.split(|character: char| !character.is_ascii_alphanumeric() && character != '_')
			.flat_map(|token| token.split('_'))
			.filter(|token| !token.is_empty())
			.map(str::to_ascii_lowercase)
			.collect()
	}

	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let persistence =
		std::fs::read_to_string(root.join("session_persistence.rs")).expect("read session owner");
	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read owner assertions");
	for (path, source) in [
		("session_persistence.rs", persistence.as_str()),
		("editor.rs", editor.as_str()),
	] {
		let tokens = words(source);
		for (index, token) in tokens.iter().enumerate() {
			assert!(
				!matches!(token.as_str(), "v1" | "v2" | "v3"),
				"{path} contains number-as-era token {token}"
			);
			if matches!(token.as_str(), "version" | "schema")
				&& let Some(next) = tokens.get(index + 1)
			{
				assert!(
					!matches!(next.as_str(), "one" | "two" | "three"),
					"{path} contains number-as-era phrase {token}_{next}"
				);
			}
		}
	}
	assert!(persistence.contains("SCHEMA_VERSION: u32 = 3"));
	for wire in [1, 2, 3] {
		assert!(
			persistence.contains(&format!("DurableMeanings::decode({wire},")),
			"codec fixtures must retain explicit wire value {wire}"
		);
	}
}

#[test]
fn product_0087_gradient_guides_have_one_stop_and_construction_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plot");
	let legend = std::fs::read_to_string(root.join("legend.rs")).expect("read legend owner");
	let production_legend = legend
		.split("#[cfg(test)]")
		.next()
		.expect("production legend region");
	assert_eq!(production_legend.matches("fn sample_gradient").count(), 1);
	assert_eq!(
		production_legend
			.matches("sample_gradient(stops, t)")
			.count(),
		2,
		"ordinary and paired gradient paths must share one sampler"
	);
	assert_eq!(production_legend.matches("fn gradient_section").count(), 1);
	for retired in ["low_color", "high_color"] {
		assert!(
			!production_legend.contains(retired),
			"retired endpoint authority survives as {retired}"
		);
	}
	assert!(!production_legend.contains("ColorTheme::"));

	let mut producer_calls = 0;
	for file in [
		"kernels/bubble.rs",
		"kernels/heatmap.rs",
		"kernels/hexbin.rs",
	] {
		let source = std::fs::read_to_string(root.join(file)).expect("read gradient producer");
		let production = source
			.split("#[cfg(test)]")
			.next()
			.expect("production kernel region");
		let calls = production.matches("legend::gradient_section(").count();
		assert_eq!(calls, 1, "{file} must use the shared constructor once");
		assert!(!production.contains("LegendSection::Gradient {"));
		producer_calls += calls;
	}
	assert_eq!(producer_calls, 3);

	let colors = std::fs::read_to_string(root.join("colors.rs")).expect("read ramp authority");
	let production_colors = colors
		.split("#[cfg(test)]")
		.next()
		.expect("production colour region");
	assert!(production_colors.contains("self.definition().sample(t)"));
	assert!(production_colors.contains("pub(crate) fn canonical_stops"));

	let stops = std::fs::read_to_string(root.join("gradient_stops.rs"))
		.expect("read validated stop sequence");
	let owner = stops
		.split("#[cfg(test)]")
		.next()
		.expect("production stop region");
	assert!(owner.contains("pub struct GradientStops"));
	assert!(owner.contains("stops: Vec<GradientStop>"));
	assert!(!owner.contains("pub stops:"));
}

#[test]
fn product_0088_heatmap_keeps_one_validated_sparse_cell_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let heatmap =
		std::fs::read_to_string(root.join("plot/kernels/heatmap.rs")).expect("read heatmap kernel");
	let cells = std::fs::read_to_string(root.join("plot/kernels/heatmap_cells.rs"))
		.expect("read heatmap cell authority");
	let request =
		std::fs::read_to_string(root.join("plot/request.rs")).expect("read plot planning");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let production_heatmap = heatmap
		.split("#[cfg(test)]")
		.next()
		.expect("production heatmap region");

	assert_eq!(
		production_heatmap
			.matches("HeatmapCells::from_rows(rows)?")
			.count(),
		1
	);
	assert!(production_heatmap.contains("let Some(value) = self.prepared_data.cells.value(i, j)"));
	assert!(production_heatmap.contains("self.prepared_data.cells.domain()"));
	for retired in [
		"values[xi][yi]",
		"vec![vec![0.0f64",
		"unwrap_or(0.0)",
		"max_val",
		"val / max",
	] {
		assert!(
			!production_heatmap.contains(retired),
			"PRODUCT-0088 retired heatmap authority survives as {retired}"
		);
	}
	assert!(cells.contains("pub struct HeatmapCells"));
	assert!(cells.contains("x_categories: Vec<String>"));
	assert!(!cells.contains("pub x_categories:"));
	assert!(cells.contains("pub(crate) fn from_rows("));
	assert!(
		request.contains("prepare_heatmap_data(data, x, y, value)")
			&& request.contains("map_err(|error| error.to_string())?")
	);
	assert!(
		factory.contains("PRODUCT-0088 planning validated this heatmap snapshot"),
		"factory reconstruction must rely on the planning-time validation of the same snapshot"
	);

	for file in [
		"plot/kernels/bubble.rs",
		"plot/colors.rs",
		"plot/legend.rs",
		"plot/gradient_stops.rs",
	] {
		let source = std::fs::read_to_string(root.join(file))
			.unwrap_or_else(|error| panic!("read {file}: {error}"));
		assert!(!source.contains("HeatmapCells"));
		assert!(!source.contains("PRODUCT-0088"));
	}
}

#[test]
fn product_0089_continuous_colour_has_one_scale_and_no_decomposed_guide_path() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plot");
	let scale = std::fs::read_to_string(root.join("continuous_color.rs"))
		.expect("read continuous colour authority");
	let theme = std::fs::read_to_string(root.join("theme.rs")).expect("read theme cascade");
	let gradient =
		std::fs::read_to_string(root.join("gradient_stops.rs")).expect("read gradient projection");
	let legend = std::fs::read_to_string(root.join("legend.rs")).expect("read guide constructor");

	assert!(scale.contains("pub struct ContinuousColorScale"));
	assert!(scale.contains("pub fn normalize(&self, value: f64) -> f64"));
	assert!(scale.contains("pub fn value_at_normalized(&self, t: f64) -> f64"));
	assert!(scale.contains("value.is_finite()"));
	assert!(!scale.contains("debug_assert!"));
	assert!(theme.contains("settings.continuous_ramps = ContinuousRamps::from_theme(theme);"));
	assert!(theme.contains("settings.color_theme = theme.sequential_ramp;"));
	assert!(gradient.contains("pub(crate) fn projected(scale: &ContinuousColorScale)"));
	assert!(legend.contains("scale: &ContinuousColorScale"));
	for retired in [
		"projected(\n\t\tlow:",
		"gradient_section(\n\tlabel: String,\n\tlow:",
	] {
		assert!(!gradient.contains(retired) && !legend.contains(retired));
	}

	for kernel in ["heatmap.rs", "bubble.rs", "hexbin.rs"] {
		let source = std::fs::read_to_string(root.join("kernels").join(kernel))
			.unwrap_or_else(|error| panic!("read {kernel}: {error}"));
		let production = source
			.split("#[cfg(test)]")
			.next()
			.expect("production kernel region");
		assert_eq!(
			production.matches("ContinuousColorScale::new(").count(),
			1,
			"{kernel} must construct only in its helper"
		);
		assert_eq!(
			production.matches("self.continuous_scale(").count(),
			2,
			"{kernel} paint and guide must use the same helper"
		);
		assert_eq!(
			production
				.matches("PRODUCT-0089 planning validated")
				.count(),
			1,
			"{kernel} must expect only in its helper"
		);
		assert!(!production.contains("ColorJob::Diverging"));
		assert!(!production.contains("ContinuousAnchor::Authored"));
		assert!(!production.contains("ColorIdentity"));
	}
}

#[test]
fn product_0090_bubble_keeps_one_validated_observation_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let bubble =
		std::fs::read_to_string(root.join("plot/kernels/bubble.rs")).expect("read bubble kernel");
	let observations = std::fs::read_to_string(root.join("plot/kernels/bubble_observations.rs"))
		.expect("read bubble observation authority");
	let request =
		std::fs::read_to_string(root.join("plot/request.rs")).expect("read plot planning");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let production_bubble = bubble
		.split("#[cfg(test)]")
		.next()
		.expect("production bubble region");

	assert_eq!(
		production_bubble
			.matches("BubbleObservations::from_rows(")
			.count(),
		1
	);
	assert!(production_bubble.contains("pub observations: BubbleObservations"));
	for retired in ["unwrap_or(0.0)", "unwrap_or(1.0)", "Series::new_empty"] {
		assert!(
			!production_bubble.contains(retired),
			"PRODUCT-0090 retired bubble fabrication survives as {retired}"
		);
	}
	assert!(observations.contains("pub struct BubbleObservations"));
	assert!(observations.contains("observations: Vec<BubbleObservation>"));
	assert!(!observations.contains("pub observations:"));
	assert!(observations.contains("pub(crate) fn from_rows("));
	assert!(
		request.contains("prepare_bubble_data(")
			&& request.contains("map_err(|error| error.to_string())?")
	);
	assert!(
		factory.contains("PRODUCT-0090 planning validated this bubble snapshot"),
		"factory reconstruction must rely on planning validation of the same snapshot"
	);
}

#[test]
fn product_0092_heatmap_surface_keeps_one_explicit_three_role_pipeline() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read plot request");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");
	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read relation envelope");
	let host = std::fs::read_to_string(root.join("gui/state.rs")).expect("read Rune host");
	let session = std::fs::read_to_string(root.join("session_persistence.rs"))
		.expect("read session persistence");

	assert!(spec.contains("pub struct HeatmapFields"));
	assert!(spec.contains("pub fn all(&self) -> [&str; 3]"));
	assert!(spec.contains("fn resolve_heatmap_channels("));
	assert!(spec.contains("(PlotType::Heatmap, Channel::X | Channel::Y)"));
	assert!(spec.contains("PlotType::Bar | PlotType::BoxPlot | PlotType::Heatmap"));
	assert!(spec.contains("channel: Channel::Color"));
	assert!(request.contains("if let Some(heatmap_fields) = spec.heatmap_fields()"));
	assert!(request.contains("for field in heatmap_fields.all()"));
	assert!(presentation.contains("(\"__heatmap\", raw_heatmap as RawFunction)"));
	assert!(presentation.contains("fn raw_heatmap("));
	assert!(compiler.contains("\"heatmap\","));
	assert!(compiler.contains("\"heatmap\" => Some(3)"));
	assert!(envelope.contains("pub(crate) heatmap_fields: Option<"));
	assert!(envelope.contains("pub(crate) heatmap_origins: Option<HeatmapFieldOrigins>"));
	assert!(host.contains("fn heatmap_failure_span("));
	assert!(host.contains("heatmap_output_span"));

	let production_heatmap = std::fs::read_to_string(root.join("plot/kernels/heatmap.rs"))
		.expect("read heatmap kernel")
		.split("#[cfg(test)]")
		.next()
		.expect("production heatmap region")
		.to_owned();
	assert!(production_heatmap.contains("ContinuousScaleIntent::SEQUENTIAL"));
	assert!(!production_heatmap.contains("ColorJob::Diverging"));
	assert!(!production_heatmap.contains("ContinuousAnchor::Authored"));
	assert!(!session.contains("PlotType::Heatmap"));
	assert!(!session.contains("AuthoredFamilyId::HEATMAP"));
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
			let boundary = path.file_name().and_then(|name| name.to_str());
			let is_transport_boundary = matches!(
				boundary,
				Some("rune_relation_envelope.rs" | "rune_view_compiler.rs")
			);
			for finding in envelope_leaks(&source, is_transport_boundary) {
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
	let presentation = std::fs::read_to_string(source.join("rune_presentation.rs"))
		.expect("read typed Rune presentation VM");
	assert!(
		presentation.contains("(\"__point_size\", raw_mark_point_size as RawFunction)")
			&& presentation.contains("PointSize::new")
			&& presentation.contains("sink.settings.point_size = Override::Set(value)"),
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
	let presentation = std::fs::read_to_string(
		Path::new(env!("CARGO_MANIFEST_DIR")).join("src/rune_presentation.rs"),
	)
	.expect("read typed Rune presentation VM");
	assert!(
		presentation.contains("(\"__stroke_width\", raw_mark_stroke_width as RawFunction)")
			&& presentation.contains("StrokeWidth::new")
			&& presentation.contains("sink.settings.stroke_width = Override::Set(value)"),
		"PRODUCT-0020 must lower the named modifier through PRODUCT-0019's typed handoff"
	);
	assert!(
		!presentation.contains("HashMap<String, LayerModifier"),
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
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read transported intent");
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed Rune presentation VM");
	assert!(
		envelope.contains("pub(crate) struct StyledRunePlotSink")
			&& presentation.contains("(\"__bar_width\", raw_mark_bar_width as RawFunction)")
			&& presentation.contains("BarWidth::new")
			&& presentation.contains("sink.settings.bar_width = Override::Set(value)"),
		"PRODUCT-0022 must use the general styled-single carrier and typed substrate"
	);
	assert!(
		!presentation.contains("HashMap<String, LayerModifier"),
		"the third setting does not authorize open modifier dispatch"
	);
}

#[test]
fn product_0053_category_gap_stays_in_the_closed_bar_register() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed Rune presentation VM");
	assert!(
		presentation.contains("(\"__category_gap\", raw_mark_category_gap as RawFunction)")
			&& presentation.contains("CategoryGap::new")
			&& presentation.contains("sink.settings.category_gap = Override::Set(value)"),
		"PRODUCT-0053 must lower the named modifier through the closed typed bar register"
	);
	assert!(
		!presentation.contains("HashMap<String, LayerModifier"),
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
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	assert!(presentation.contains("(\"__skin\", raw_table_skin as RawFunction)"));
	assert!(presentation.contains("table.skin_name = Some(name.to_owned())"));
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
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	assert!(presentation.contains("(\"__duotone\", raw_table_duotone as RawFunction)"));
	assert!(presentation.contains("\"duotone() takes no arguments\""));
	assert!(presentation.contains("if table.duotone && table.skin_name.is_none()"));
	assert!(presentation.contains("\"duotone() requires skin(string)\""));
	assert!(presentation.contains(".build_associated::<RuneTable>()"));
	assert!(presentation.contains(".build_associated::<RuneMark>()"));
	let mark_register = presentation
		.split("for (name, function) in [\n\t\t(\"__point_size\"")
		.nth(1)
		.expect("typed mark register")
		.split(".build_associated::<RuneMark>()")
		.next()
		.expect("bounded typed mark register");
	assert!(!mark_register.contains("raw_table_skin"));
	assert!(!mark_register.contains("raw_table_duotone"));

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
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	let production = presentation
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production presentation source");
	let bounds = production
		.split("fn bounds_element(")
		.nth(1)
		.expect("bounds constructor")
		.split("fn numeric_pair_element(")
		.next()
		.expect("bounded bounds constructor");
	assert!(production.contains("raw_x_bounds"));
	assert!(production.contains("raw_y_bounds"));
	assert_eq!(
		production
			.matches("authored_settings.domains = Override::Set")
			.count(),
		1,
		"both axes must lower through one grouped domain override"
	);
	assert!(bounds.contains("!minimum.is_finite() || !maximum.is_finite()"));
	assert!(bounds.contains("minimum == maximum"));
	assert!(bounds.contains("minimum > maximum"));
	for forbidden in ["EPSILON", ".abs()", "maximum - minimum", "minimum_span"] {
		assert!(
			!bounds.contains(forbidden),
			"numeric bounds acquired a magnitude admission rule through {forbidden}"
		);
	}
	let mark_register = production
		.split("for (name, function) in [\n\t\t(\"__point_size\"")
		.nth(1)
		.expect("typed mark register")
		.split(".build_associated::<RuneMark>()")
		.next()
		.expect("bounded typed mark register");
	assert!(!mark_register.contains("x_bounds"));
	assert!(!mark_register.contains("y_bounds"));

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
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	let production = presentation
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("typed presentation production");
	assert!(production.contains("raw_x_tick_step"));
	assert!(production.contains("raw_y_tick_step"));
	assert_eq!(
		production
			.matches("authored_settings.guides = Override::Set")
			.count(),
		1
	);
	assert!(production.contains("fn numeric_scalar_element("));
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
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	let production = presentation
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("typed presentation production");
	assert!(production.contains("raw_x_minor_ticks"));
	assert!(production.contains("raw_y_minor_ticks"));
	assert!(production.contains("fn minor_ticks_element("));
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
	let compiler = std::fs::read_to_string(root.join("rune_view_compiler.rs"))
		.expect("read static shell compiler");
	let production = compiler
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("static shell production");
	assert!(production.contains("fn is_static_literal_expression("));
	assert!(production.contains("definition.statement.start < use_start"));
	assert!(production.contains("definition.literal"));
	assert!(production.contains("only static literal and alias closures may cross both VMs"));
	for forbidden in ["rune::runtime", "rune::Vm", "execute_generated(source"] {
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
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	let figure_decoder = presentation
		.split("fn merge_element(")
		.nth(1)
		.expect("figure element decoder")
		.split("fn finalize_mark_body(")
		.next()
		.expect("bounded figure decoder");
	assert!(figure_decoder.contains("FigureElementKind::Palette"));
	assert!(figure_decoder.contains("authored_settings.categorical_palette"));
	for forbidden in [
		"CategoricalPaletteSelection",
		"ResolvedCategoricalPalette",
		"ColorIdentity",
		"RuneCategoricalContinuation",
		"Theme::try_new",
	] {
		assert!(
			!figure_decoder.contains(forbidden),
			"Rune normal form acquired resolved/head state through {forbidden}"
		);
	}
	let mark_register = presentation
		.split("for (name, function) in [\n\t\t(\"__point_size\"")
		.nth(1)
		.expect("typed mark register")
		.split(".build_associated::<RuneMark>()")
		.next()
		.expect("bounded typed mark register");
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
fn product_0080_product_0083_plot_chrome_and_mark_style_are_separate_closed_tiers() {
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
	assert!(!fields.contains("mark_fill"));
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
	let mark_fields = chrome
		.split("pub struct PlotMarkStyle {")
		.nth(1)
		.expect("PlotMarkStyle declaration")
		.split('}')
		.next()
		.expect("PlotMarkStyle fields");
	assert_eq!(mark_fields.matches("pub ").count(), 5);
	for role in [
		"fill",
		"decoration",
		"fill_detail",
		"label_text",
		"separator",
	] {
		assert!(
			mark_fields.contains(&format!("pub {role}: PlotRgba")),
			"missing mark-style role {role}"
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
	assert!(production.contains("pub mark_style: PlotMarkStyle"));
	assert!(!production.contains("mark_decoration_color"));

	for (relative, roles) in [
		(
			"plot/kernels/boxplot.rs",
			&["mark_style.decoration", "mark_style.fill_detail"][..],
		),
		("plot/kernels/violin.rs", &["mark_style.decoration"][..]),
		(
			"plot/kernels/candlestick.rs",
			&["mark_style.decoration"][..],
		),
		(
			"plot/kernels/funnel.rs",
			&["mark_style.decoration", "mark_style.label_text"][..],
		),
	] {
		let source = std::fs::read_to_string(root.join(relative)).expect("read mark kernel");
		let mark_draw = source
			.split("fn draw_legend(")
			.next()
			.expect("mark draw before legend");
		for role in roles {
			assert!(
				mark_draw.contains(role),
				"{relative} did not consume {role}"
			);
		}
		assert!(
			!mark_draw.contains("settings.chrome"),
			"{relative} mark drawing acquired plot chrome"
		);
	}
	let area = std::fs::read_to_string(root.join("plot/kernels/stacked_area.rs"))
		.expect("read reachable stacked-area kernel");
	let area_draw = area.split("#[cfg(test)]").next().expect("area production");
	assert!(area_draw.contains("mark_style.decoration"));

	let histogram =
		std::fs::read_to_string(root.join("plot/kernels/histogram.rs")).expect("histogram kernel");
	let production = histogram
		.split("#[cfg(test)]")
		.next()
		.expect("histogram production");
	assert!(production.contains("settings.mark_style.fill"));
	assert!(production.contains("settings.mark_style.separator"));
	assert!(!production.contains("color_theme.get_color"));
	assert!(production.contains("stroke_path"));
	assert!(!production.contains("rgba(0, 0, 0, 0.2)"));
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
	assert!(layer.contains("resolved_skin: Override<ResolvedPlotSkin>"));

	let chrome = std::fs::read_to_string(root.join("plot/chrome.rs")).expect("read chrome");
	let production = chrome
		.split("#[cfg(test)]")
		.next()
		.expect("chrome production");
	assert_eq!(
		production.matches("pub fn from_theme_name(").count(),
		2,
		"the atomic skin projection and chrome compatibility projection are the only public entries"
	);
	assert!(
		production.contains(
			"ResolvedPlotSkin::from_theme_name(name, projection).map(|skin| skin.chrome)"
		)
	);
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

	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	let figure_decoder = presentation
		.split("fn decode_figure(")
		.nth(1)
		.expect("figure decoder")
		.split("fn presentation_value_origin(")
		.next()
		.expect("bounded figure decoder");
	assert!(figure_decoder.contains("PlotSkinSelection::new(name, skin.projection)"));
	let skin_modifier = presentation
		.split("fn skin_duotone(")
		.nth(1)
		.expect("plot skin modifier")
		.split("fn presentation_table(")
		.next()
		.expect("bounded plot skin modifier");
	assert!(skin_modifier.contains("PlotChromeProjection::Duotone"));
	assert!(!skin_modifier.contains("TableSkin"));

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

#[test]
fn product_0071_chrome_consumes_the_guarded_title_status_and_menu_models() {
	let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let state = std::fs::read_to_string(source_root.join("gui/state.rs"))
		.expect("read Polariton GUI state");
	assert_eq!(
		state.matches(".title(\"Polariton\")").count(),
		1,
		"PRODUCT-0071 native application title must be the product constant"
	);
	assert!(
		!state.contains("fn application_title"),
		"PRODUCT-0063's unconsumed dynamic title helper must not return"
	);
	assert!(
		!state.contains("Polariton — session"),
		"opaque durable identity must not return to application chrome"
	);
	let copy_branch = state
		.split("Message::CopySessionId =>")
		.nth(1)
		.expect("copy-session action")
		.split("Message::MenuAction")
		.next()
		.expect("bounded copy-session action");
	assert!(copy_branch.contains("iced::clipboard::write(session_identity_text(session_id))"));

	let components = std::fs::read_to_string(source_root.join("gui/components.rs"))
		.expect("read Polariton GUI components");
	let title_bar = components
		.split("pub fn title_bar")
		.nth(1)
		.expect("custom title bar")
		.split("pub fn main_screen")
		.next()
		.expect("bounded custom title bar");
	assert_eq!(
		title_bar.matches("text(\"Polariton\")").count(),
		1,
		"custom title bar must consume the same constant product identity"
	);
	assert!(
		!title_bar.contains("session"),
		"custom title bar must not receive persistence presentation state"
	);

	let main_screen = components
		.split("pub fn main_screen")
		.nth(1)
		.expect("main screen")
		.split("fn status_detail_overlay")
		.next()
		.expect("bounded main screen");
	let status_binding = main_screen
		.split("let status_bar: Element<'a, Message> =")
		.nth(1)
		.expect("production status model binding")
		.split("let status_bar: Element<'a, Message> =")
		.next()
		.expect("bounded production status model binding");
	assert_eq!(status_binding.matches("app_status_bar(").count(), 1);
	assert!(
		status_binding.contains(")\n\t.view();"),
		"main screen must render the StatusBar model returned by app_status_bar"
	);
	assert!(
		!main_screen.contains("durable session"),
		"main screen must not reconstruct the tested status label"
	);

	let status_builder = components
		.split("fn app_status_bar(")
		.nth(1)
		.expect("application status builder")
		.split("pub fn menu_bar")
		.next()
		.expect("bounded application status builder");
	assert_eq!(
		status_builder
			.matches("Segment::text(\"durable session\")")
			.count(),
		1,
		"durable attachment text must have one production producer"
	);

	let menu_bar = components
		.split("pub fn menu_bar")
		.nth(1)
		.expect("menu bar")
		.split("fn build_menu_roots")
		.next()
		.expect("bounded menu bar");
	assert!(menu_bar.contains("MenuBar::new("));
	assert!(
		menu_bar.contains("build_menu_roots(saved_session, close_without_checkpoint)"),
		"MenuBar must consume the tested MenuRoot model without reconstructing it"
	);
}

#[test]
fn product_0072_percent_is_scale_intent_not_cascade_or_durable_identity() {
	let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(source_root.join("plot/spec/mod.rs"))
		.expect("read plot specification source");
	let normalization = spec
		.split("pub enum StackNormalization")
		.nth(1)
		.expect("closed stack-normalization vocabulary")
		.split("pub struct LayerIdentity")
		.next()
		.expect("bounded stack-normalization vocabulary");
	assert!(normalization.contains("Absolute"));
	assert!(normalization.contains("Percent"));
	let layer_settings = spec
		.split("pub struct LayerSettings")
		.nth(1)
		.expect("layer settings")
		.split("impl Default for LayerSettings")
		.next()
		.expect("bounded layer settings");
	assert!(
		!layer_settings.contains("StackNormalization") && !layer_settings.contains("percent"),
		"PRODUCT-0072 normalization is authored scale intent, never a presentation setting"
	);

	let theme = std::fs::read_to_string(source_root.join("plot/theme.rs"))
		.expect("read plot presentation cascade");
	assert!(
		!theme.contains("StackNormalization"),
		"themes and pane overrides cannot select a mark's value unit"
	);
	let persistence = std::fs::read_to_string(source_root.join("session_persistence.rs"))
		.expect("read Polariton persistence schema");
	assert!(
		!persistence.contains("StackNormalization"),
		"percent scale must not extend the durable authored-family schema"
	);
}

#[test]
fn product_0073_discovery_is_a_terminal_read_only_store_census() {
	let source_root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let main =
		std::fs::read_to_string(source_root.join("main.rs")).expect("read Polariton entrypoint");
	let action = main
		.split("enum ApplicationAction")
		.nth(1)
		.expect("terminal action vocabulary")
		.split("fn action_from_args")
		.next()
		.expect("bounded terminal action vocabulary");
	assert!(action.contains("Gui(gui::StartupTarget)"));
	assert!(action.contains("ListSessions"));

	let listing = main
		.split("fn list_saved_sessions() -> gui::Result")
		.nth(1)
		.expect("saved-session terminal action")
		.split("fn render_session_discovery")
		.next()
		.expect("bounded saved-session terminal action");
	assert!(listing.contains("SessionStore::platform_default()?.discover()?"));
	for forbidden in [
		"gui::run",
		"load_startup_data",
		"open_writable",
		"create(",
		"owner_payload",
		"set_owner_payload",
		"delete",
		"autosave",
	] {
		assert!(
			!listing.contains(forbidden),
			"PRODUCT-0073 terminal discovery acquired forbidden authority: {forbidden}"
		);
	}
	let store = std::fs::read_to_string(
		Path::new(env!("CARGO_MANIFEST_DIR")).join("../libaryon/src/core/session_store.rs"),
	)
	.expect("read head-neutral session store");
	let discovery = store
		.split("pub fn discover(&self)")
		.nth(1)
		.expect("head-neutral discovery operation")
		.split("pub fn create(&self)")
		.next()
		.expect("bounded head-neutral discovery operation");
	assert!(discovery.contains("open_existing_root"));
	assert!(discovery.contains("list_dir_raw"));
	assert!(discovery.contains("self.open_locked(id, LockMode::Shared)"));
	for forbidden in [
		"ensure_root",
		"open_writable",
		"cleanup_unreferenced",
		"commit(",
		"set_owner_payload",
		"owner_payload(",
		"remove_entry_at",
		"unlink_at",
		"rename_at",
	] {
		assert!(
			!discovery.contains(forbidden),
			"PRODUCT-0073 store discovery acquired forbidden authority: {forbidden}"
		);
	}

	let gui = std::fs::read_to_string(source_root.join("gui/state.rs"))
		.expect("read GUI startup vocabulary");
	let startup = gui
		.split("pub(crate) enum StartupTarget")
		.nth(1)
		.expect("GUI startup vocabulary")
		.split("enum StartupAuthority")
		.next()
		.expect("bounded GUI startup vocabulary");
	assert_eq!(startup.matches("Ephemeral").count(), 1);
	assert_eq!(startup.matches("CreateSaved").count(), 1);
	assert_eq!(startup.matches("OpenSaved").count(), 1);
	assert!(!startup.contains("List"));

	let readme = std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("README.md"))
		.expect("read Polariton README");
	for claim in [
		"polariton --session list",
		"polariton --session 24631c77d510daf33ae6331e1f4776ce",
		"document metadata is intentionally unknown",
		"has uncheckpointed changes",
	] {
		assert!(
			readme.contains(claim),
			"missing discovery documentation: {claim}"
		);
	}
}

#[test]
fn product_0074_polariton_selects_zero_divider_reservation() {
	let editor =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/editor.rs"))
			.expect("read Polariton editor configuration");
	let config = editor
		.split("fn polariton_engine_config(")
		.nth(1)
		.expect("Polariton engine configuration")
		.split("impl Drop for CodeEditor")
		.next()
		.expect("bounded engine configuration");
	assert!(config.contains("divider_reservation: PaneDividerReservation::NONE"));
	assert!(
		!config.contains("SPLIT_DIVIDER_COLS") && !config.contains("SPLIT_DIVIDER_ROWS"),
		"the GUI head must not inherit terminal reservation constants"
	);
}

#[test]
fn product_0075_nested_dashboard_is_absent_and_renderer_catalog_is_exact() {
	let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
	let source_root = manifest.join("src");
	assert!(!source_root.join("gui/dashboard").exists());
	assert!(!source_root.join("gui/pane_grid").exists());

	let editor = std::fs::read_to_string(source_root.join("editor.rs")).expect("read editor");
	let gui = std::fs::read_to_string(source_root.join("gui/state.rs")).expect("read GUI state");
	let messages =
		std::fs::read_to_string(source_root.join("gui/messages.rs")).expect("read GUI messages");
	let components =
		std::fs::read_to_string(source_root.join("gui/components.rs")).expect("read GUI chrome");
	fn production(source: &str) -> &str {
		source
			.split("\n#[cfg(test)]\nmod tests {")
			.next()
			.expect("production source before test module")
	}
	for (name, source) in [
		("editor", production(&editor)),
		("state", production(&gui)),
		("messages", production(&messages)),
		("components", production(&components)),
	] {
		for forbidden in [
			"DashboardState",
			"DashboardMsg",
			"DashboardEffect",
			"PlotPaneState",
			"rune_plot_panes",
			"plot:new:",
		] {
			assert!(
				!source.contains(forbidden),
				"PRODUCT-0075 retired dashboard symbol remains in {name}: {forbidden}"
			);
		}
	}
	let app_state = gui
		.split("struct AppState {")
		.nth(1)
		.expect("AppState declaration")
		.split("\n}")
		.next()
		.expect("bounded AppState declaration");
	assert!(
		!app_state.contains("data_frame"),
		"dashboard-derived DataFrame cache must not survive PRODUCT-0075"
	);
	assert_eq!(editor.matches("HostPanelId::new(2)").count(), 1);
	assert!(editor.contains("RETIRED_DASHBOARD_PANEL"));

	let core = std::fs::read_to_string(source_root.join("plot/core.rs"))
		.expect("read renderer vocabulary");
	let enum_body = core
		.split("pub enum PlotType {")
		.nth(1)
		.expect("PlotType declaration")
		.split('}')
		.next()
		.expect("bounded PlotType declaration");
	let variants = enum_body
		.lines()
		.map(str::trim)
		.filter(|line| !line.is_empty())
		.map(|line| line.trim_end_matches(',').to_owned())
		.collect::<std::collections::BTreeSet<_>>();
	let catalog_body = core
		.split("pub const ALL:")
		.nth(1)
		.expect("renderer catalog")
		.split("];")
		.next()
		.expect("bounded renderer catalog");
	let entries = catalog_body
		.lines()
		.filter_map(|line| {
			line.trim()
				.strip_prefix("PlotType::")
				.map(|variant| variant.trim_end_matches(',').to_owned())
		})
		.collect::<Vec<_>>();
	let catalog = entries
		.iter()
		.cloned()
		.collect::<std::collections::BTreeSet<_>>();
	assert_eq!(
		entries.len(),
		catalog.len(),
		"renderer catalog has a duplicate"
	);
	assert_eq!(
		catalog, variants,
		"PlotType::ALL is PRODUCT-0075's exact renderer catalog, not picker residue"
	);
}

#[test]
fn product_0076_presentation_has_one_typed_semantic_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune transport");
	let compiler = std::fs::read_to_string(root.join("rune_view_compiler.rs"))
		.expect("read static shell compiler");
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read typed presentation VM");
	let gui_state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	fn production(source: &str) -> &str {
		source
			.split("\n#[cfg(test)]\nmod tests {")
			.next()
			.expect("production source before tests")
	}
	for retired in [
		"fn parse_figure(",
		"fn parse_modified_mark(",
		"fn parse_modified_table(",
		"fn parse_layer(",
		"fn parse_figure_bounds(",
	] {
		assert!(
			!production(&envelope).contains(retired),
			"PRODUCT-0076 retired AST semantic recognizer remains: {retired}"
		);
	}
	assert!(production(&gui_state).contains("crate::rune_view_compiler::compile(&work.source)"));
	assert!(compiler.contains("execute_generated("));
	assert!(compiler.contains("PresentationMapKind::Synthetic"));
	assert!(presentation.contains("build_associated::<RuneMark>()"));
	assert!(presentation.contains("PRESENTATION_INSTRUCTION_BUDGET"));
	assert!(
		!compiler.contains("match method {\n\t\t\"stacked\"")
			&& !compiler.contains("match method {\n\t\t\"percent\""),
		"stacked and percent semantics belong to registered Rune methods"
	);
}

#[test]
fn product_0076_showcase_and_seed_are_repository_owned() {
	let crate_root = Path::new(env!("CARGO_MANIFEST_DIR"));
	let compiler = std::fs::read_to_string(crate_root.join("src/rune_view_compiler.rs"))
		.expect("read static shell compiler");
	let showcase = std::fs::read_to_string(crate_root.join("../../uat/test21.rn"))
		.expect("read repository-owned PRODUCT-0076 showcase");
	let seed = std::fs::read_to_string(crate_root.join("../../uat/demo_sales.sql"))
		.expect("read repository-owned PRODUCT-0076 seed");

	assert!(
		compiler.contains("include_str!(\"../../../uat/test21.rn\")"),
		"the compiled showcase gate must consume the repository-owned UAT artifact"
	);
	assert!(
		!compiler.contains("../../../../uat/") && !compiler.contains("/home/me/work/uat"),
		"a build or gate must not depend on an unversioned sibling UAT directory"
	);
	assert_eq!(
		showcase.matches("view(\"").count(),
		12,
		"the canonical showcase retains all twelve bar forms"
	);
	assert!(
		seed.contains("DROP TABLE IF EXISTS demo_sales;")
			&& seed.contains("CREATE TABLE demo_sales")
			&& ["'Q1'", "'Q2'", "'Q3'", "'Q4'"]
				.into_iter()
				.all(|quarter| seed.contains(quarter)),
		"the canonical seed is idempotent and makes all four facet quarters observable"
	);
}

#[test]
fn product_0077_facet_tick_containment_does_not_measure_or_resize_layout() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plot");
	let grid = std::fs::read_to_string(root.join("grid.rs")).expect("read facet-grid layout");
	let common = std::fs::read_to_string(root.join("common.rs")).expect("read plot chrome");

	assert!(
		grid.contains("const GRID_GAP: f32 = 12.0;"),
		"PRODUCT-0077 contains labels without changing facet geometry"
	);
	let grid_layout = grid
		.split("fn axis_text_visibility(")
		.nth(1)
		.expect("facet text decision")
		.split("fn resolved_categories(")
		.next()
		.expect("bounded facet text and geometry path");
	let anchor_decision = common
		.split("pub(crate) struct AxisTextAnchoring")
		.nth(1)
		.expect("numeric endpoint anchor decision")
		.split("pub(crate) struct AxisTextVisibility")
		.next()
		.expect("bounded numeric endpoint anchor decision");
	for (name, source) in [
		("facet grid layout", grid_layout),
		("numeric anchor decision", anchor_decision),
	] {
		for forbidden in ["Paragraph", "glyph", "font", "measure_text", "text_extent"] {
			assert!(
				!source.contains(forbidden),
				"PRODUCT-0077 {name} acquired text measurement through {forbidden}"
			);
		}
	}
}

#[test]
fn product_0078_histogram_keeps_one_interval_authority_after_the_0079_surface() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let histogram = std::fs::read_to_string(root.join("plot/kernels/histogram.rs"))
		.expect("read histogram kernel");
	let bins = std::fs::read_to_string(root.join("plot/kernels/histogram_bins.rs"))
		.expect("read validated histogram intervals");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");

	for retired in ["bin_counts", "pub max_count:"] {
		assert!(
			![
				histogram.as_str(),
				bins.as_str(),
				factory.as_str(),
				spec.as_str()
			]
			.into_iter()
			.any(|source| source.contains(retired)),
			"PRODUCT-0078 retired histogram authority remains: {retired}"
		);
	}
	for index_arithmetic in ["/ num_bins", ".min(num_bins", "bin_idx"] {
		assert!(
			!histogram.contains(index_arithmetic) && !bins.contains(index_arithmetic),
			"PRODUCT-0078 histogram interval path retains value-to-index arithmetic: {index_arithmetic}"
		);
	}
	assert!(
		spec.contains("Histogram Plot has no producer; explicit interval fields are required"),
		"PRODUCT-0079 must not reinstate automatic raw-value binning"
	);
	assert!(
		histogram.contains("HistogramBins::new(bins)")
			&& factory.contains("histogram::prepare_histogram_data(")
			&& factory.contains("df, bin_start, bin_end, count,"),
		"PRODUCT-0079 must ingest explicit interval fields through PRODUCT-0078's sole authority"
	);

	let prepared = histogram
		.split("pub struct HistogramPreparedData {")
		.nth(1)
		.expect("histogram prepared data")
		.split('}')
		.next()
		.expect("bounded histogram prepared data");
	assert!(prepared.contains("pub bins: HistogramBins"));
	assert!(
		!prepared.contains("range") && !prepared.contains("domain"),
		"histogram domains must be derived from the validated interval sequence"
	);

	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read Rune presentation runtime");
	assert!(
		presentation.contains("histogram(...) takes bin_start, bin_end, and count strings")
			&& spec.contains("ResolvedKernelFields::Histogram"),
		"PRODUCT-0079 must add only the explicit three-field producer"
	);
}

#[test]
fn product_0082_box_plot_keeps_one_tukey_authority_and_explicit_producer() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let boxplot = std::fs::read_to_string(root.join("plot/kernels/boxplot.rs"))
		.expect("read box plot kernel");
	let tukey = std::fs::read_to_string(root.join("plot/kernels/boxplot_tukey.rs"))
		.expect("read validated Tukey substrate");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");

	assert!(
		boxplot.contains("pub fn prepare_box_plot_data(")
			&& factory.contains("ResolvedKernelFields::BoxPlot")
			&& factory.contains("boxplot::prepare_box_plot_data(df, &fields)"),
		"PRODUCT-0082 must ingest explicit Tukey fields through PRODUCT-0081's authority"
	);
	let resolved_fields = spec
		.split("pub enum ResolvedKernelFields")
		.nth(1)
		.expect("resolved kernel vocabulary")
		.split("pub struct ResolvedLayout")
		.next()
		.expect("bounded resolved kernel vocabulary");
	assert!(resolved_fields.contains("BoxPlot {"));
	assert!(
		spec.contains("Box Plot has no producer; seven explicit Tukey fields are required"),
		"automatic BoxPlot resolution must retain a typed producerless refusal"
	);
	for retired in ["pub min:", "pub max:", "stats.min", "stats.max"] {
		assert!(
			!boxplot.contains(retired) && !tukey.contains(retired),
			"PRODUCT-0081 retains silently redefined extrema vocabulary: {retired}"
		);
	}
	assert!(
		boxplot.contains("settings.mark_style.fill.iced()")
			&& boxplot.contains("settings.mark_style.decoration.iced()")
			&& !boxplot.contains("color_theme.get_color")
			&& tukey.contains("pub fn new(rows: Vec<TukeyBoxRow>)"),
		"validated Tukey rows and resolved mark roles must be the surviving substrate"
	);
}

#[test]
fn product_0084_result_artifact_is_the_single_truth_boundary() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let artifact =
		std::fs::read_to_string(root.join("result_artifact.rs")).expect("read result artifact");
	let common =
		std::fs::read_to_string(root.join("adapters/common.rs")).expect("read adapter carriers");
	let registry =
		std::fs::read_to_string(root.join("result_registry.rs")).expect("read result registry");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");

	assert!(common.contains("artifact: Arc<ResultArtifact>"));
	assert!(common.contains("Result<Arc<ResultArtifact>, RuneDispatchFailure>"));
	assert!(registry.contains("artifact: Arc<ResultArtifact>"));
	assert!(spec.contains("pub data: Arc<crate::result_artifact::ResultArtifact>"));
	assert!(artifact.contains("enum ResultBacking"));
	assert!(artifact.contains("Arrow(ArrowBacking)"));
	assert!(artifact.contains("Polars(Arc<DataFrame>)"));
	assert!(artifact.contains("projection: OnceLock<Result<Arc<DataFrame>, String>>"));

	let toron =
		std::fs::read_to_string(root.join("adapters/toron/mod.rs")).expect("read Toron adapter");
	let bigquery =
		std::fs::read_to_string(root.join("adapters/bigquery.rs")).expect("read BigQuery adapter");
	assert!(toron.contains("ArrowNullabilityPolicy::Preserve"));
	assert!(bigquery.contains("ArrowNullabilityPolicy::Unknown"));
	for adapter in ["sqlite.rs", "postgres.rs", "mysql.rs", "parquet.rs"] {
		let source = std::fs::read_to_string(root.join("adapters").join(adapter))
			.unwrap_or_else(|error| panic!("read {adapter}: {error}"));
		assert!(
			source.contains("ResultArtifact::from_polars("),
			"PRODUCT-0084 {adapter} must enter through the policy-free Polars constructor"
		);
		assert!(!source.contains("from_arrow_ipc"));
	}

	let mut projection_decode_sites = Vec::new();
	for path in rust_files(&root) {
		let source = std::fs::read_to_string(&path).expect("read Rust source");
		if source.contains("polars::io::ipc") || source.contains("IpcStreamReader") {
			projection_decode_sites.push(path);
		}
	}
	assert_eq!(projection_decode_sites, [root.join("result_artifact.rs")]);
}

#[test]
fn product_0084_keeps_dataframe_at_the_kernel_boundary_only() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let kernels = root.join("plot/kernels");
	let mut prepare_signatures = 0_usize;
	for path in rust_files(&kernels) {
		let source = std::fs::read_to_string(path).expect("read kernel");
		for line in source
			.lines()
			.filter(|line| line.contains("pub fn prepare_"))
		{
			prepare_signatures += 1;
			let tail = source
				.split_once(line)
				.expect("located prepare signature")
				.1
				.lines()
				.take(8)
				.collect::<String>();
			assert!(
				line.contains("DataFrame") || tail.contains("&DataFrame"),
				"PRODUCT-0084 changed a prepared-data consumer: {line}"
			);
		}
	}
	assert!(
		prepare_signatures > 10,
		"kernel inventory must be substantive"
	);
}

#[test]
fn product_0085_keeps_one_semantic_axis_title_derivation_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let stacked_bar = std::fs::read_to_string(root.join("plot/kernels/stacked_bar.rs"))
		.expect("read stacked bar");
	let stacked_area = std::fs::read_to_string(root.join("plot/kernels/stacked_area.rs"))
		.expect("read stacked area");
	assert!(common.contains("pub enum AxisTitleTransform"));
	assert!(common.contains("pub fn derive_axis_title("));
	for (name, source) in [("stacked bar", stacked_bar), ("stacked area", stacked_area)] {
		let production = source
			.split("#[cfg(test)]")
			.next()
			.expect("production kernel section");
		assert!(
			production.contains("derive_axis_title("),
			"PRODUCT-0085 {name} must consume the shared derivation authority"
		);
		assert!(
			!production.contains("(% of total)"),
			"PRODUCT-0085 {name} must not carry a local title formatter"
		);
	}

	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation VM");
	for label in ["x_label", "y_label"] {
		assert!(
			compiler.contains(&format!("\"{label}\",")),
			"PRODUCT-0085 {label} must participate in presentation partitioning"
		);
		assert!(
			presentation.contains(&format!("\"__{label}\"")),
			"PRODUCT-0085 {label} must be registered in the local Rune VM"
		);
	}
}
