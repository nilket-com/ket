use std::path::Path;

#[allow(dead_code)]
#[path = "../src/coordinate.rs"]
mod coordinate;

#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{ProductBoundary, assert_product_dependency_boundary};

fn presentation_registry(root: &Path) -> String {
	std::fs::read_to_string(root.join("presentation_registry.rs"))
		.expect("read PRODUCT-0141 presentation registry")
}

fn registry_callable<'a>(
	registry: &'a str,
	name: &str,
	position: &str,
	handler: &str,
) -> Option<&'a str> {
	registry.split("callable!(").skip(1).find(|block| {
		let block = block.split("\n\t),").next().unwrap_or(block);
		block.contains(&format!("\"{name}\""))
			&& block.contains(&format!("\n\t\t{position},"))
			&& block.contains(&format!("\n\t\t{handler},"))
	})
}

fn registry_has(registry: &str, name: &str, position: &str) -> bool {
	registry.split("callable!(").skip(1).any(|block| {
		let block = block.split("\n\t),").next().unwrap_or(block);
		block.contains(&format!("\"{name}\"")) && block.contains(&format!("\n\t\t{position},"))
	})
}

fn assert_registered_handler(root: &Path, name: &str, position: &str, handler: &str, raw: &str) {
	let registry = presentation_registry(root);
	assert!(
		registry_callable(&registry, name, position, handler).is_some(),
		"registry is missing {position} {name} -> {handler}"
	);
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read Rune presentation evaluator");
	assert!(
		presentation.contains(&format!("HandlerId::{handler} => {raw}")),
		"raw handler map is missing {handler} -> {raw}"
	);
}

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
	serialization_sites.sort();
	// PRODUCT-0062 confined serde to the opaque session owner; PRODUCT-0134
	// sanctions exactly one more durable schema, the encrypted profile store.
	let mut expected = vec![
		Path::new(env!("CARGO_MANIFEST_DIR")).join("src/profile_store.rs"),
		// PRODUCT-0154 §2: the published dashboard package owns its own DTOs.
		Path::new(env!("CARGO_MANIFEST_DIR")).join("src/publication.rs"),
		Path::new(env!("CARGO_MANIFEST_DIR")).join("src/session_persistence.rs"),
	];
	expected.sort();
	assert_eq!(
		serialization_sites, expected,
		"serde belongs only to Polariton's declared durable-schema owners: the \
		 opaque session owner (PRODUCT-0062) and the encrypted profile store \
		 (PRODUCT-0134)"
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
	assert_eq!(
		constants,
		[
			"SCATTER",
			"BAR",
			"LINE",
			"AREA",
			"FUNNEL",
			"RADIAL_DIAL",
			"PIE",
			"RADAR",
			"PARALLEL",
			"VIOLIN"
		]
	);
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
	for (file, constructor, expected_calls) in [
		("kernels/bubble.rs", "legend::gradient_section(", 1),
		("kernels/heatmap.rs", "legend::gradient_section(", 1),
		("kernels/hexbin.rs", "count_gradient_section(", 2),
	] {
		let source = std::fs::read_to_string(root.join(file)).expect("read gradient producer");
		let production = source
			.split("#[cfg(test)]\nmod tests")
			.next()
			.expect("production kernel region");
		let calls = production.matches(constructor).count();
		assert_eq!(
			calls, expected_calls,
			"{file} must use the shared constructor for each honest guide role"
		);
		assert!(!production.contains("LegendSection::Gradient {"));
		producer_calls += calls;
	}
	assert_eq!(producer_calls, 4);

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
			.split("#[cfg(test)]\nmod tests")
			.next()
			.expect("production kernel region");
		assert_eq!(
			production.matches("ContinuousColorScale::new(").count(),
			1,
			"{kernel} must construct only in its helper"
		);
		if kernel == "hexbin.rs" {
			assert_eq!(
				production.matches("Self::scale(").count(),
				3,
				"hexbin paint, actual guide, and reservation must use the same helper"
			);
			assert_eq!(
				production.matches("PRODUCT-0143 finite u64 count").count(),
				1
			);
		} else {
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
		}
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
	assert!(spec.contains("pub enum MarkFields"));
	assert!(spec.contains("Self::Heatmap(fields) => fields.all().get(index).copied()"));
	assert!(spec.contains("fn resolve_heatmap_channels("));
	assert!(spec.contains("(PlotType::Heatmap, Channel::X | Channel::Y)"));
	assert!(spec.contains("PlotType::Bar | PlotType::BoxPlot | PlotType::Heatmap"));
	assert!(spec.contains("attribution: FieldAttribution::Channel(Channel::Color)"));
	assert!(request.contains("if let Some(mark_fields) = spec.mark_fields()"));
	assert!(request.contains("for field in mark_fields.all()"));
	assert_registered_handler(&root, "heatmap", "Free", "Heatmap", "raw_heatmap");
	assert!(presentation.contains("fn raw_heatmap("));
	assert!(compiler.contains("\"heatmap\","));
	assert!(compiler.contains("\"heatmap\" => Some(&[3])"));
	assert!(envelope.contains("pub(crate) mark_fields: Option<SpannedMarkFields>"));
	assert!(envelope.contains("Self::Heatmap { fields, origins }"));
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
	assert_registered_handler(
		&source,
		"point_size",
		"Method",
		"MarkPointSize",
		"raw_mark_point_size",
	);
	assert!(
		presentation.contains("PointSize::new")
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
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	assert_registered_handler(
		&root,
		"stroke_width",
		"Method",
		"MarkStrokeWidth",
		"raw_mark_stroke_width",
	);
	assert!(
		presentation.contains("StrokeWidth::new")
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
	assert_registered_handler(
		&root,
		"bar_width",
		"Method",
		"MarkBarWidth",
		"raw_mark_bar_width",
	);
	assert!(
		envelope.contains("pub(crate) struct StyledRunePlotSink")
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
	assert_registered_handler(
		&root,
		"category_gap",
		"Method",
		"MarkCategoryGap",
		"raw_mark_category_gap",
	);
	assert!(
		presentation.contains("CategoryGap::new")
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
	// PRODUCT-0151 keeps the table atomic while widening what crosses that
	// boundary: the unfiltered artifact/recipe identity and selection display
	// state now travel beside the original three presentation fields.
	assert_eq!(
		artifact
			.lines()
			.map(str::trim)
			.filter(|line| line.contains(':'))
			.map(|line| line.split(':').next().unwrap())
			.collect::<Vec<_>>(),
		[
			"frame",
			"artifact",
			"publication",
			"selection_enabled",
			"selection_revision",
			"selection_pending",
			"displayed_filter_predicate",
			"surface_incarnation",
			"selection_predicate",
			"content_revision",
			"skin",
			"authored_show_column_types",
		],
		"the table artifact remains the complete atomic table boundary"
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
	assert_registered_handler(&root, "skin", "Method", "TableSkin", "raw_table_skin");
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
	assert_registered_handler(
		&root,
		"duotone",
		"Method",
		"TableDuotone",
		"raw_table_duotone",
	);
	assert!(presentation.contains("\"duotone() takes no arguments\""));
	assert!(presentation.contains("if table.duotone && table.skin_name.is_none()"));
	assert!(presentation.contains("\"duotone() requires skin(string)\""));
	assert!(presentation.contains(".build_associated::<RuneTable>()"));
	assert!(presentation.contains(".build_associated::<RuneMark>()"));
	let registry = presentation_registry(&root);
	assert!(
		registry_callable(&registry, "point_size", "Method", "MarkPointSize")
			.expect("point-size registry entry")
			.contains("MARK")
	);
	for (name, handler) in [("skin", "TableSkin"), ("duotone", "TableDuotone")] {
		let callable =
			registry_callable(&registry, name, "Method", handler).expect("table registry entry");
		assert!(callable.contains("TABLE_ROUTE"));
		assert!(!callable.contains("MARK,"));
	}

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
		.split("fn raw_axis_bounds(")
		.nth(1)
		.expect("axis bounds constructor")
		.split("fn raw_axis_tick_step(")
		.next()
		.expect("bounded axis bounds constructor");
	assert!(production.contains("raw_axis_bounds"));
	assert!(production.contains("FigureElementKind::Axis(axis)"));
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
	let registry = presentation_registry(&root);
	let bounds_callable = registry_callable(&registry, "bounds", "Method", "AxisBounds")
		.expect("axis bounds registry entry");
	assert!(bounds_callable.contains("FIGURE_AND_SKIN"));
	assert!(!bounds_callable.contains("MARK,"));
	assert!(!registry_has(&registry, "x_bounds", "Free"));
	assert!(!registry_has(&registry, "y_bounds", "Free"));

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
	assert!(production.contains("raw_axis_tick_step"));
	assert_eq!(
		production
			.matches("authored_settings.guides = Override::Set")
			.count(),
		1
	);
	assert!(production.contains("fn raw_axis_tick_step("));
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
	assert!(production.contains("raw_axis_minor_ticks"));
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
	let registry = presentation_registry(&root);
	for (name, handler) in [("palette", "Palette"), ("skin", "Skin")] {
		let callable = registry_callable(&registry, name, "Free", handler)
			.expect("figure-setting registry entry");
		assert!(callable.contains("FREE"));
		assert!(!callable.contains("MARK,"));
	}

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
	let content = ready
		.find("set_rune_view_plot_with_source_and_recipe(")
		.expect("content install with presentation source facts");
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
		.split("pub struct ResolvedPlotSettings {")
		.nth(1)
		.and_then(|tail| tail.split("#[cfg(test)]").next())
		.expect("bounded resolved-settings production region");
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
			&[
				"mark_style.separator",
				"mark_style.fill_detail",
				"mark_style.label_text",
			][..],
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
	// PRODUCT-0135 §6: the model renders through the action-mapped view so
	// the Cancel segment is pressable; it is still the same returned model.
	assert!(
		status_binding.contains(")\n\t.view_with_action("),
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
		menu_bar
			.contains("build_menu_roots(saved_session, close_without_checkpoint, profile_names)"),
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
	let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
	let configuration = std::fs::read_to_string(manifest.join("src/configuration.rs"))
		.expect("read Polariton startup configuration");
	assert!(configuration.contains("pane_divider_reservation: PaneDividerReservation::NONE"));
	assert!(
		!configuration.contains("pane_divider_reservation: PaneDividerReservation::default()"),
		"Polariton's missing-config fallback must not inherit the terminal default"
	);
	let editor = std::fs::read_to_string(manifest.join("src/editor.rs"))
		.expect("read Polariton editor configuration");
	let config = editor
		.split("fn polariton_engine_config(")
		.nth(1)
		.expect("Polariton engine configuration")
		.split("impl Drop for CodeEditor")
		.next()
		.expect("bounded engine configuration");
	assert!(config.contains("divider_reservation,"));
	assert!(
		!config.contains("SPLIT_DIVIDER_COLS") && !config.contains("SPLIT_DIVIDER_ROWS"),
		"the GUI head must not inherit terminal reservation constants"
	);
	let divider_interaction = editor
		.split("fn divider_visual_distance_px(")
		.nth(1)
		.expect("divider interaction helpers")
		.split("fn drag_activation_reached(")
		.next()
		.expect("bounded divider interaction helpers");
	assert!(
		!divider_interaction.contains("divider_reservation"),
		"Polariton may consume projected divider rectangles but must not derive geometry from reservation fields"
	);
	assert!(
		divider_interaction.contains("pane_pixel_rect(divider.hit_rect)"),
		"the graphical head narrows only the engine-published hit rectangle"
	);
	assert!(
		!editor.contains("SetDividerReservation"),
		"the startup preference must not grow an inert live editor command"
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
fn product_0141_registry_is_the_only_surface_inventory_and_the_inspector_is_read_only() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let registry = presentation_registry(&root);
	let compiler = std::fs::read_to_string(root.join("rune_view_compiler.rs"))
		.expect("read presentation compiler");
	let evaluator = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read presentation evaluator");
	let inspector = std::fs::read_to_string(root.join("gui/plot_pane/view.rs"))
		.expect("read settings inspector");

	for fact in [
		"pub(crate) struct CallableSpec",
		"registration_routes:",
		"accepted_receivers:",
		"pub(crate) struct SettingSpec",
		"pub(crate) enum SettingKey",
		"pub(crate) struct PresentationSourceModel",
	] {
		assert!(registry.contains(fact), "registry is missing {fact}");
	}
	assert!(!compiler.contains("const PRESENTATION_FREE"));
	assert!(!compiler.contains("const PRESENTATION_METHODS"));
	assert!(compiler.contains("presentation_registry::is_presentation_callable"));
	assert!(evaluator.contains("for callable in CALLABLES"));
	assert!(evaluator.contains("fn raw_handler(handler: HandlerId)"));
	assert!(inspector.contains("inspector_rows("));
	for forbidden in ["ApplyDocumentEdits", "WritebackSpec", "ReplaceDocument"] {
		assert!(
			!registry.contains(forbidden) && !inspector.contains(forbidden),
			"ADR 0142 writeback authority leaked into ADR 0141 through {forbidden}"
		);
	}
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
		grid.contains("pub const DEFAULT_FACET_GUTTER: f32 = 12.0;"),
		"PRODUCT-0110 preserves PRODUCT-0077's default facet geometry while making the divider a figure-grid input"
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
	// PRODUCT-0137 widened Rune dispatch's success to a closed enum so a
	// materialization can report a receipt instead of a fabricated empty
	// relation. PRODUCT-0084's claim is unchanged and asserted here in its
	// new shape: the *relation* half still carries the artifact and nothing
	// else, so the artifact remains the single truth boundary for rows.
	assert!(common.contains("Result<RuneDispatchSuccess, RuneDispatchFailure>"));
	assert!(common.contains("Relation(Arc<ResultArtifact>)"));
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

	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation VM");
	assert_registered_handler(&root, "x_axis", "Free", "XAxis", "raw_x_axis");
	assert_registered_handler(&root, "y_axis", "Free", "YAxis", "raw_y_axis");
	assert_registered_handler(&root, "label", "Method", "AxisLabel", "raw_axis_label");
	assert!(presentation.contains("raw_axis_label"));
}

#[test]
fn product_0093_legend_geometry_has_one_authority_without_backend_measurement() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let legend = std::fs::read_to_string(root.join("plot/legend.rs")).expect("read legend");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot backend");

	assert!(legend.contains("fn legend_geometry("));
	let measure = legend
		.split("pub(crate) fn measure_legend(")
		.nth(1)
		.expect("legend measurement")
		.split("pub(crate) fn legend_layout(")
		.next()
		.expect("bounded legend measurement");
	assert!(measure.contains("legend_geometry(content, settings).size"));
	let render = legend
		.split("pub fn render_legend(")
		.nth(1)
		.expect("legend renderer")
		.split("#[cfg(test)]")
		.next()
		.expect("bounded legend renderer");
	assert!(render.contains("let geometry = legend_geometry(content, settings);"));
	for retired in [
		"let width = 250.0",
		"x + width - 70.0",
		"let bar_x = x + padding",
	] {
		assert!(
			!render.contains(retired),
			"PRODUCT-0093 retains a second legend geometry authority: {retired}"
		);
	}

	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.expect("plot backend trait")
		.split("pub struct IcedBackend")
		.next()
		.expect("bounded plot backend trait");
	for forbidden in ["measure_text", "text_extent", "glyph_extent"] {
		assert!(
			!backend.contains(forbidden),
			"PRODUCT-0093 must not add backend glyph measurement: {forbidden}"
		);
	}
}

#[test]
fn product_0094_mark_field_transport_is_exhaustive_coupled_and_identity_neutral() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let bubble_fields = std::fs::read_to_string(root.join("plot/spec/bubble_fields.rs"))
		.expect("read bubble field vocabulary");
	let observations = std::fs::read_to_string(root.join("plot/kernels/bubble_observations.rs"))
		.expect("read bubble substrate");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read Rune presentation");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read factory");

	let mark_fields = spec
		.split("pub enum MarkFields")
		.nth(1)
		.expect("semantic mark fields")
		.split("pub struct AuthoredPlotSpec")
		.next()
		.expect("bounded semantic mark fields");
	for variant in [
		"BoxPlot(BoxPlotFields)",
		"Heatmap(HeatmapFields)",
		"Bubble(BubbleFields)",
	] {
		assert!(mark_fields.contains(variant));
	}
	assert!(mark_fields.contains("pub struct MarkFieldIter<'a>"));
	assert!(mark_fields.contains("fields: &'a MarkFields"));
	assert!(!mark_fields.contains("Box<dyn Iterator>"));

	let projection = request
		.split("fn project_named_channels(")
		.nth(1)
		.expect("named-channel projection")
		.split("#[cfg(test)]")
		.next()
		.expect("bounded named-channel projection");
	assert!(projection.contains("if let Some(mark_fields) = spec.mark_fields()"));
	assert!(projection.contains("for field in mark_fields.all()"));
	for retired in [
		"box_plot_fields",
		"heatmap_fields",
		"MarkFields::BoxPlot",
		"MarkFields::Heatmap",
	] {
		assert!(
			!projection.contains(retired),
			"PRODUCT-0094 projection retains a per-mark path: {retired}"
		);
	}

	let sink = envelope
		.split("pub(crate) struct DisposableRunePlotSink")
		.nth(1)
		.expect("Rune plot sink")
		.split("impl Eq for DisposableRunePlotSink")
		.next()
		.expect("bounded Rune plot sink and equality");
	assert!(sink.contains("pub(crate) mark_fields: Option<SpannedMarkFields>"));
	for retired in [
		"box_plot_fields",
		"box_plot_origins",
		"heatmap_fields",
		"heatmap_origins",
	] {
		assert!(!sink.contains(retired));
	}
	assert!(sink.contains("left.semantic_eq(right)"));

	let spanned = envelope
		.split("pub(crate) enum SpannedMarkFields")
		.nth(1)
		.expect("spanned mark fields")
		.split("#[derive(Clone, Debug, PartialEq, Eq)]")
		.next()
		.expect("bounded spanned mark fields");
	assert!(
		spanned.contains("BoxPlot {")
			&& spanned.contains("Heatmap {")
			&& spanned.contains("Bubble {")
	);
	let preceding = envelope
		.split("pub(crate) enum SpannedMarkFields")
		.next()
		.expect("spanned prefix")
		.rsplit('\n')
		.take(3)
		.collect::<Vec<_>>()
		.join("\n");
	assert!(!preceding.contains("PartialEq"));
	let semantic_eq = spanned
		.split("fn semantic_eq(")
		.nth(1)
		.expect("borrowed semantic equality")
		.split("\n\t}")
		.next()
		.expect("bounded semantic equality");
	assert!(!semantic_eq.contains("clone") && !semantic_eq.contains("to_mark_fields"));

	assert!(bubble_fields.contains("pub struct BubbleFieldNames"));
	assert!(bubble_fields.contains("pub struct BubbleFields"));
	assert!(bubble_fields.contains("numeric: BubbleFieldNames"));
	for duplicate in ["x: String", "y: String", "size: String", "color: String"] {
		assert_eq!(
			bubble_fields.matches(duplicate).count(),
			1,
			"duplicate {duplicate}"
		);
		assert!(!observations.contains(duplicate));
	}
	assert_eq!(presentation.matches("SpannedMarkFields::Bubble").count(), 1);
	assert_registered_handler(&root, "bubble", "Free", "Bubble", "raw_bubble");
	assert!(factory.contains("let fields = crate::plot::spec::BoxPlotFields {"));
}

#[test]
fn product_0095_bubble_surface_keeps_roles_projection_and_kernel_boundaries_structural() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let bubble_fields = std::fs::read_to_string(root.join("plot/spec/bubble_fields.rs"))
		.expect("read bubble fields");
	let bubble =
		std::fs::read_to_string(root.join("plot/kernels/bubble.rs")).expect("read bubble kernel");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");

	let raw_bubble = presentation
		.split("fn raw_bubble(")
		.nth(1)
		.expect("bubble Rune constructor")
		.split("\nfn ")
		.next()
		.expect("bounded bubble Rune constructor");
	assert_registered_handler(&root, "bubble", "Free", "Bubble", "raw_bubble");
	assert!(!raw_bubble.contains("OptionalAuthoredChannel::Auto"));
	assert!(raw_bubble.contains("SpannedMarkFields::Bubble"));

	let resolver = spec
		.split("fn resolve_bubble_channels(")
		.nth(1)
		.expect("bubble channel resolver")
		.split("\nfn ")
		.next()
		.expect("bounded bubble resolver");
	assert!(resolver.contains("FieldAttribution::Role(role.as_str())"));
	assert!(!resolver.contains("FieldAttribution::Channel(Channel::Color)"));
	assert_eq!(
		bubble_fields
			.matches("enum BubblePreparationChannel")
			.count(),
		1
	);
	assert!(!bubble.contains("enum BubblePreparationChannel"));
	assert!(bubble_fields.contains("formatter.write_str(self.as_str())"));
	assert!(
		bubble_fields
			.contains("formatter.write_str(BubblePreparationChannel::from(*self).as_str())")
	);

	let projection = request
		.split("fn project_named_channels(")
		.nth(1)
		.expect("named-channel projection")
		.split("#[cfg(test)]")
		.next()
		.expect("bounded named-channel projection");
	assert!(projection.contains("for field in mark_fields.all()"));
	assert!(!projection.contains("MarkFields::Bubble"));
	assert!(!projection.contains("bubble_fields"));

	let preparation = bubble
		.split("pub fn prepare_bubble_data(")
		.nth(1)
		.expect("bubble preparation")
		.split("#[cfg(test)]")
		.next()
		.expect("bounded bubble preparation");
	assert!(preparation.starts_with("\n\tdf: &DataFrame,\n\tfields: &BubbleFields,"));
	assert!(!preparation.contains("MarkFields"));
	let production = bubble
		.split("#[cfg(test)]")
		.next()
		.expect("production bubble kernel");
	assert_eq!(
		production
			.matches("let radius = 2.0 + t_size * 28.0;")
			.count(),
		2
	);
	assert!(!production.contains("ColorJob::Diverging"));
	assert!(!production.contains("ContinuousAnchor::Authored"));

	let formatter = spec
		.split("impl fmt::Display for ChannelError")
		.nth(1)
		.expect("channel error formatter")
		.split("impl std::error::Error")
		.next()
		.expect("bounded channel error formatter");
	assert!(formatter.contains("FieldAttribution::Channel(channel)"));
	assert!(formatter.contains("plot {channel} channel"));
	assert!(formatter.contains("FieldAttribution::Role(role)"));
	assert!(formatter.contains("plot {role} field"));
}

#[test]
fn product_0098_funnel_surface_has_one_durable_assignment_and_transport_path() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let funnel =
		std::fs::read_to_string(root.join("plot/kernels/funnel.rs")).expect("read funnel kernel");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");
	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read relation envelope");
	let host = std::fs::read_to_string(root.join("gui/state.rs")).expect("read host");
	let session = std::fs::read_to_string(root.join("session_persistence.rs"))
		.expect("read session persistence");

	assert!(spec.contains("pub struct FunnelFields"));
	assert!(spec.contains("MarkFields::Funnel"));
	assert!(spec.contains("Self::Funnel(fields) => fields.all().get(index).copied()"));
	assert!(spec.contains("fn resolve_funnel_channels("));
	assert!(spec.contains("FieldAttribution::Role(\"stage\")"));
	assert!(spec.contains("FieldAttribution::Role(\"value\")"));
	assert!(spec.contains("pub(crate) const FUNNEL: Self"));
	assert!(spec.contains("PlotType::Funnel => AuthoredFamilyId::FUNNEL"));
	assert!(spec.contains("PRODUCT-0064 requires stacked bar colour lineage"));
	assert!(spec.contains("no live authored categorical-family conversion"));

	let projection = request
		.split("fn project_named_channels(")
		.nth(1)
		.expect("named-channel projection")
		.split("#[cfg(test)]")
		.next()
		.expect("bounded projection");
	assert!(projection.contains("for field in mark_fields.all()"));
	assert!(!projection.contains("MarkFields::Funnel"));
	assert!(!projection.contains("funnel_fields"));

	assert_registered_handler(&root, "funnel", "Free", "Funnel", "raw_funnel");
	assert_eq!(presentation.matches("SpannedMarkFields::Funnel").count(), 1);
	assert!(compiler.contains("\"funnel\" => Some(&[2])"));
	assert!(envelope.contains("Self::Funnel { fields, origins }"));
	assert!(host.contains("fn funnel_failure_span("));
	assert!(host.contains("funnel_output_span"));

	let production = funnel
		.split("#[cfg(test)]")
		.next()
		.expect("production funnel kernel");
	assert!(production.contains("fn stage_color("));
	assert_eq!(production.matches("self.stage_color(").count(), 2);
	assert!(production.contains("self.stage_colors"));
	assert!(
		production.contains("PRODUCT-0098 authored funnel stages have complete colour assignments")
	);
	assert!(production.contains("settings.mark_style.separator"));
	assert!(production.contains("settings.mark_style.fill_detail"));
	assert!(production.contains("settings.mark_style.label_text"));
	assert!(factory.contains("spec\n\t\t\t\t.channel(Channel::X)"));
	assert!(factory.contains("stage_colors,"));

	assert!(session.contains("Self::ExtensibleFamilies => true"));
	assert!(session.contains("\"scatter\" | \"bar\" | \"line\" | \"area\""));
	assert!(!session.contains("INITIAL_CLOSED_SCHEMA => Ok(Self::ExtensibleFamilies)"));
	assert!(!session.contains("AREA_CLOSED_SCHEMA => Ok(Self::ExtensibleFamilies)"));
}

#[test]
fn product_0091_continuous_authoring_has_only_the_two_recorded_origins_and_no_status_surface() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read Rune presentation");
	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune envelope");
	let production = presentation
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production presentation region");
	assert_registered_handler(
		&root,
		"sequential_palette",
		"Free",
		"SequentialPalette",
		"raw_sequential_palette",
	);
	assert_registered_handler(
		&root,
		"diverging_palette",
		"Free",
		"DivergingPalette",
		"raw_diverging_palette",
	);
	assert_registered_handler(
		&root,
		"diverging",
		"Method",
		"MarkDiverging",
		"raw_mark_diverging",
	);
	assert_registered_handler(
		&root,
		"diverging_at",
		"Method",
		"MarkDivergingAt",
		"raw_mark_diverging_at",
	);
	assert_eq!(production.matches("ColorJob::Diverging").count(), 1);
	assert_eq!(production.matches("ContinuousAnchor::Authored").count(), 1);
	assert!(!production.contains("FieldAttribution::Role(\"anchor\")"));
	let registry = presentation_registry(&root);
	assert!(!registry.contains("status_palette"));
	assert!(!production.contains("status_palette"));
	assert!(registry_has(&registry, "diverging", "Method"));
	assert!(registry_has(&registry, "diverging_at", "Method"));

	assert!(theme.contains("pub sequential_ramp: Override<ColorTheme>"));
	assert!(theme.contains("pub diverging_ramp: Override<ColorTheme>"));
	assert!(theme.contains("apply_layer(&mut settings, &self.layer, false)"));
	assert!(spec.contains("pub continuous_anchor:"));
	assert!(spec.contains("pub continuous_intent:"));
	for forbidden in ["continuous_intent", "continuous_anchor", "ColorJob"] {
		let mark_fields = spec
			.split("pub enum MarkFields")
			.nth(1)
			.expect("MarkFields declaration")
			.split("pub struct AuthoredPlotSpec")
			.next()
			.expect("MarkFields region");
		assert!(!mark_fields.contains(forbidden));
		let spanned = envelope
			.split("pub(crate) enum SpannedMarkFields")
			.nth(1)
			.expect("SpannedMarkFields declaration")
			.split("impl SpannedMarkFields")
			.next()
			.expect("SpannedMarkFields region");
		assert!(!spanned.contains(forbidden));
	}
}

#[test]
fn product_0096_funnel_has_one_exact_numeric_authority_and_no_legacy_fallbacks() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let funnel =
		std::fs::read_to_string(root.join("plot/kernels/funnel.rs")).expect("read funnel kernel");
	let boxplot = std::fs::read_to_string(root.join("plot/kernels/boxplot.rs"))
		.expect("read box plot kernel");
	let exact = std::fs::read_to_string(root.join("plot/exact_numeric.rs"))
		.expect("read exact numeric authority");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read plot request");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");

	for forbidden in [
		"total_max",
		"into_no_null_iter",
		"Series::new_empty",
		"format!(\"Stage {}\"",
		"DataType::Float32",
	] {
		assert!(
			!funnel.contains(forbidden),
			"legacy funnel path remains: {forbidden}"
		);
	}
	assert_eq!(exact.matches("fn exact_f64(").count(), 1);
	assert!(!boxplot.contains("fn exact_f64("));
	assert!(boxplot.contains("use crate::plot::exact_numeric::{ExactF64Error, exact_f64};"));
	assert!(funnel.contains("use crate::plot::exact_numeric::{ExactF64Error, exact_f64};"));
	assert!(request.contains("prepare_funnel_data("));
	assert!(factory.contains("PRODUCT-0096 planning validated this funnel stage collection"));
	assert_eq!(factory.matches("prepare_funnel_data(").count(), 1);
	assert!(!funnel.contains("sort"));
}

#[test]
fn product_0097_widget_hit_geometry_has_closed_typed_trust_boundaries() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let coordinate =
		std::fs::read_to_string(root.join("coordinate.rs")).expect("read coordinate vocabulary");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read plot grid");
	let funnel =
		std::fs::read_to_string(root.join("plot/kernels/funnel.rs")).expect("read funnel kernel");
	let radial = std::fs::read_to_string(root.join("plot/kernels/radial_dial.rs"))
		.expect("read radial dial kernel");
	let pie = std::fs::read_to_string(root.join("plot/kernels/pie.rs")).expect("read pie kernel");

	assert!(common.contains(
		"fn widget_hit_geometry(&self, cursor: Pt<Widget>) -> (Rect<Widget>, Pt<Widget>)"
	));
	assert_eq!(coordinate.matches("from_plot_kernel_cursor").count(), 1);
	assert_eq!(funnel.matches("from_plot_kernel_cursor").count(), 1);
	assert_eq!(radial.matches("from_plot_kernel_cursor").count(), 1);
	assert_eq!(pie.matches("from_plot_kernel_cursor").count(), 1);
	assert_eq!(coordinate.matches("from_transformer_bounds").count(), 1);
	assert_eq!(common.matches("from_transformer_bounds").count(), 1);
	assert_eq!(funnel.matches("widget_hit_geometry").count(), 1);
	assert_eq!(radial.matches("widget_hit_geometry").count(), 1);
	assert_eq!(pie.matches("widget_hit_geometry").count(), 1);
	// Count the four admitted production consumers in their owning files. A
	// workspace-wide textual count would also count behavioral fixtures embedded
	// beside production code and would turn new typed-geometry coverage into a
	// false architecture widening.
	assert_eq!(
		coordinate.matches("from_plot_kernel_cursor").count()
			+ funnel.matches("from_plot_kernel_cursor").count()
			+ radial.matches("from_plot_kernel_cursor").count()
			+ pie.matches("from_plot_kernel_cursor").count(),
		4
	);
	let common_coordinate_boundary = common
		.split("pub enum TimeUnit")
		.nth(1)
		.and_then(|tail| tail.split("#[cfg(test)]").next())
		.expect("bounded coordinate-transform production region");
	assert_eq!(
		coordinate.matches("from_transformer_bounds").count()
			+ common_coordinate_boundary
				.matches("from_transformer_bounds")
				.count(),
		2
	);
	assert_eq!(
		common_coordinate_boundary
			.matches("widget_hit_geometry")
			.count() + funnel.matches("widget_hit_geometry").count()
			+ radial.matches("widget_hit_geometry").count()
			+ pie.matches("widget_hit_geometry").count(),
		4
	);
	assert!(!coordinate.contains("fn from_iced("));

	assert!(!grid.contains("Pt::<Cell>::from_widget"));
	assert!(!grid.contains("Pt::<Plot>::from_widget"));
	assert!(!grid.contains("_cell_cursor"));
	assert!(grid.contains("Cursor::Available(local_cursor)"));
	assert!(!funnel.contains("transform.bounds"));
	assert!(!radial.contains("transform.bounds"));
	assert!(!pie.contains("transform.bounds"));
	for source in [&funnel, &radial, &pie] {
		assert!(!source.contains("cursor_pos.offset("));
		assert!(!source.contains("bounds.offset("));
	}

	let trait_region = common
		.split("pub trait PlotKernel")
		.nth(1)
		.expect("PlotKernel trait")
		.split("pub struct PlotWidget")
		.next()
		.expect("bounded PlotKernel trait");
	assert!(trait_region.contains("cursor: Cursor"));
	assert!(!trait_region.contains("Pt<Widget>"));
}

#[test]
fn product_0099_radial_dial_reads_once_through_one_validated_collection() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let radial = std::fs::read_to_string(root.join("plot/kernels/radial_dial.rs"))
		.expect("read radial dial kernel");
	let readings = std::fs::read_to_string(root.join("plot/kernels/radial_dial_readings.rs"))
		.expect("read radial dial readings");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read plot request");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let exact = std::fs::read_to_string(root.join("plot/exact_numeric.rs"))
		.expect("read exact numeric authority");

	let preparation = radial
		.split("pub(crate) fn prepare_radial_dial_data(")
		.nth(1)
		.expect("radial-dial preparation")
		.split("fn radial_category_dtype")
		.next()
		.expect("bounded radial-dial preparation");
	for forbidden in [
		".filter(",
		".unique(",
		".sort(",
		".get(0)",
		"unwrap_or",
		"unwrap_or_else",
		"df.clone()",
		"No Data",
		"All Data",
	] {
		assert!(
			!preparation.contains(forbidden),
			"legacy radial-dial fabrication remains: {forbidden}"
		);
	}
	assert!(preparation.contains("for row in 0..df.height()"));
	assert!(preparation.contains("RadialDialReadings::from_rows("));
	assert!(radial.contains("pub readings: RadialDialReadings"));
	for parallel in [
		"pub categories: Vec<String>",
		"pub values: Vec<f64>",
		"pub max_values: Vec<f64>",
	] {
		assert!(!radial.contains(parallel));
	}
	assert!(readings.contains("pub struct RadialDialReadings"));
	assert!(readings.contains("readings: Vec<RadialDialReading>"));
	assert_eq!(exact.matches("fn exact_f64(").count(), 1);
	assert!(radial.contains("use crate::plot::exact_numeric::{ExactF64Error, exact_f64};"));
	assert!(request.contains("prepare_radial_dial_data("));
	assert!(
		factory.contains("PRODUCT-0099 planning validated this radial-dial reading collection")
	);
	assert_eq!(factory.matches("prepare_radial_dial_data(").count(), 1);
}

#[test]
fn product_0100_radial_dial_surface_reuses_transport_identity_and_one_colour_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read factory");
	let radial = std::fs::read_to_string(root.join("plot/kernels/radial_dial.rs"))
		.expect("read radial-dial kernel");
	let readings = std::fs::read_to_string(root.join("plot/kernels/radial_dial_readings.rs"))
		.expect("read radial-dial readings");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");
	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");
	let envelope =
		std::fs::read_to_string(root.join("rune_relation_envelope.rs")).expect("read envelope");
	let host = std::fs::read_to_string(root.join("gui/state.rs")).expect("read host");

	assert!(spec.contains("pub struct RadialDialFields"));
	assert!(spec.contains("MarkFields::RadialDial"));
	assert!(spec.contains("Self::RadialDial(fields) => match index"));
	assert!(spec.contains("fn resolve_radial_dial_channels("));
	for role in ["label", "value", "maximum"] {
		assert!(spec.contains(&format!("FieldAttribution::Role(\"{role}\")")));
	}
	assert!(spec.contains("pub(crate) const RADIAL_DIAL: Self"));
	assert_eq!(
		spec.matches("PlotType::RadialDial => AuthoredFamilyId::RADIAL_DIAL")
			.count(),
		1,
		"PRODUCT-0086 persistence costs exactly one conversion arm"
	);
	assert!(spec.contains("PRODUCT-0064 requires stacked bar colour lineage"));
	assert!(spec.contains("no live authored categorical-family conversion"));

	let projection = request
		.split("fn project_named_channels(")
		.nth(1)
		.expect("named projection")
		.split("#[cfg(test)]")
		.next()
		.expect("bounded projection");
	assert!(projection.contains("for field in mark_fields.all()"));
	assert!(!projection.contains("MarkFields::RadialDial"));
	assert!(!projection.contains("radial_dial_fields"));

	let production = radial
		.split("#[cfg(test)]")
		.next()
		.expect("production radial-dial kernel");
	assert!(production.contains("fn category_color("));
	assert_eq!(production.matches("self.category_color(").count(), 2);
	assert!(production.contains("self.category_colors"));
	assert!(
		production.contains(
			"PRODUCT-0100 authored radial-dial categories have complete colour assignments"
		)
	);
	assert!(factory.contains("category_colors: spec"));
	assert!(!readings.contains("All Data"));

	assert_registered_handler(
		&root,
		"radial_dial",
		"Free",
		"RadialDial",
		"raw_radial_dial",
	);
	assert_eq!(
		presentation
			.matches("SpannedMarkFields::RadialDial")
			.count(),
		1
	);
	assert!(compiler.contains("\"radial_dial\" => Some(&[2, 3])"));
	assert!(envelope.contains("Self::RadialDial { fields, origins }"));
	assert!(host.contains("fn radial_dial_failure_span("));
	assert!(host.contains("radial_dial_output_span"));
}

#[test]
fn product_0101_path_builder_has_one_connected_arc_contract_and_two_live_callers() {
	let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = manifest.join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let export = std::fs::read_to_string(root.join("plot/export.rs")).expect("read export");
	let pie = std::fs::read_to_string(root.join("plot/kernels/pie.rs")).expect("read pie");
	let radial = std::fs::read_to_string(root.join("plot/kernels/radial_dial.rs"))
		.expect("read radial dial");
	let evidence = std::fs::read_to_string(manifest.join("tests/product_0057_render_evidence.rs"))
		.expect("read render evidence");

	assert!(common.contains("pub(crate) const PATH_ARC_TOLERANCE: f64 = 0.1;"));
	assert!(common.contains("pub(crate) struct ArcPathState"));
	assert!(common.contains("pub(crate) fn begin_arc(&mut self, arc: CircularArc) -> ArcJoin"));
	let iced = common
		.split("impl<'a> PathBuilder for IcedPathBuilder<'a>")
		.nth(1)
		.expect("iced path adapter")
		.split("impl<'a> PlotBackend for IcedBackend<'a>")
		.next()
		.expect("bounded iced path adapter");
	assert!(iced.contains("self.state.begin_arc(arc)"));
	assert!(iced.contains("append_iter(PATH_ARC_TOLERANCE)"));
	assert!(iced.contains("self.builder.bezier_curve_to("));
	assert!(!iced.contains("self.builder.arc("));

	let svg = export
		.split("impl PathBuilder for SvgPathBuilder")
		.nth(1)
		.expect("svg path adapter")
		.split("impl SvgBackend")
		.next()
		.expect("bounded svg path adapter");
	assert!(svg.contains("self.state.begin_arc(arc)"));
	assert!(svg.contains("ArcJoin::Move => 'M'"));
	assert!(svg.contains("ArcJoin::Line => 'L'"));
	assert!(svg.contains("let join = match join"));

	let mut callers = Vec::new();
	for entry in std::fs::read_dir(root.join("plot/kernels")).expect("read kernel directory") {
		let entry = entry.expect("kernel entry");
		if entry
			.path()
			.extension()
			.and_then(|extension| extension.to_str())
			!= Some("rs")
		{
			continue;
		}
		let source = std::fs::read_to_string(entry.path()).expect("read kernel source");
		let production = source.split("#[cfg(test)]").next().unwrap_or(&source);
		let count = production.matches("builder.arc_to(").count();
		if count > 0 {
			callers.push((entry.file_name().to_string_lossy().into_owned(), count));
		}
	}
	callers.sort();
	assert_eq!(
		callers,
		vec![("pie.rs".to_owned(), 1), ("radial_dial.rs".to_owned(), 4)],
		"PRODUCT-0101 production arc caller census"
	);
	assert!(pie.contains("builder.move_to(geometry.center);\n\t\t\t\tbuilder.arc_to("));
	assert_eq!(pie.matches("builder.line_to(geometry.center);").count(), 1);
	assert_eq!(radial.matches("builder.arc_to(").count(), 4);
	assert!(!radial.contains("builder.line_to("));

	for pin in [
		"PRODUCT_0101 arc_contract disposition=automated",
		"pie_luminous=12336/11191",
		"joined_diff=1301/1147",
		"decomposition_diff=76/25",
		"tolerance=0.1",
		"annulus=connected",
	] {
		assert!(evidence.contains(pin), "missing render-evidence pin: {pin}");
	}
}

#[test]
fn product_0102_pie_has_one_validated_slice_and_geometry_authority() {
	let manifest = Path::new(env!("CARGO_MANIFEST_DIR"));
	let root = manifest.join("src");
	let pie = std::fs::read_to_string(root.join("plot/kernels/pie.rs")).expect("read pie kernel");
	let slices = std::fs::read_to_string(root.join("plot/kernels/pie_slices.rs"))
		.expect("read pie slice authority");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read plot request");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let exact = std::fs::read_to_string(root.join("plot/exact_numeric.rs"))
		.expect("read exact numeric authority");

	let production = pie
		.split("#[cfg(test)]")
		.next()
		.expect("bounded production pie kernel");
	let preparation = production
		.split("pub(crate) fn prepare_pie_data(")
		.nth(1)
		.expect("pie preparation")
		.split("fn pie_category_dtype(")
		.next()
		.expect("bounded pie preparation");

	assert!(production.contains("pub slices: PieSlices"));
	assert!(!production.contains("pub categories: Vec"));
	assert!(!production.contains("pub values: Vec"));
	assert!(!production.contains("cumulative_angles"));
	assert!(!production.contains("total_sum"));
	assert_eq!(production.matches("PieGeometry::new(").count(), 2);
	assert!(production.contains("let local = Point::new(cursor.x() - bounds.x()"));
	assert!(!production.contains("bounds.center()"));
	assert!(!production.contains("to_radians"));
	assert!(production.contains("use crate::plot::exact_numeric::{ExactF64Error, exact_f64};"));
	assert!(exact.contains("pub(crate) fn exact_f64("));

	for forbidden in [
		"No Data",
		"Row {}",
		"unwrap_or(",
		"unwrap_or_else(",
		".sum()",
		".unique()",
		".sort(",
		".lazy()",
		".filter(",
		".select(",
	] {
		assert!(
			!preparation.contains(forbidden),
			"PRODUCT-0102 preparation retained `{forbidden}`"
		);
	}
	assert!(!preparation.contains("for category"));

	assert!(slices.contains("pub struct PieSlices"));
	assert!(slices.contains("fn from_rows("));
	assert!(slices.contains("boundaries.push(std::f64::consts::TAU)"));
	assert!(slices.contains(".aggregate([col(\\\"") && slices.contains("sum(col(\\\""));
	assert!(request.contains("prepare_pie_data(data, category.as_deref(), value)"));
	assert!(factory.contains("PRODUCT-0102 planning validated this pie slice collection"));
	assert_eq!(factory.matches("prepare_pie_data(").count(), 1);
}

#[test]
fn product_0103_pie_surface_reuses_transport_and_has_one_colour_and_guide_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read factory");
	let pie = std::fs::read_to_string(root.join("plot/kernels/pie.rs")).expect("read pie kernel");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");
	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");
	let envelope =
		std::fs::read_to_string(root.join("rune_relation_envelope.rs")).expect("read envelope");
	let host = std::fs::read_to_string(root.join("gui/state.rs")).expect("read host");

	assert!(spec.contains("pub struct PieFields"));
	assert!(spec.contains("MarkFields::Pie"));
	assert!(spec.contains("Self::Pie(fields) => fields.all().get(index).copied()"));
	assert!(spec.contains("fn resolve_pie_channels("));
	assert!(spec.contains("FieldAttribution::Role(\"category\")"));
	assert!(spec.contains("FieldAttribution::Role(\"value\")"));
	assert!(spec.contains("pub(crate) const PIE: Self"));
	assert_eq!(
		spec.matches("PlotType::Pie => AuthoredFamilyId::PIE")
			.count(),
		1,
		"PRODUCT-0086 persistence costs exactly one conversion arm"
	);
	let authored_category_sites = spec
		.match_indices("let authored_categories = matches!(")
		.map(|(start, _)| &spec[start..(start + 420).min(spec.len())])
		.collect::<Vec<_>>();
	assert_eq!(authored_category_sites.len(), 2);
	for site in authored_category_sites {
		for plot_type in ["Funnel", "RadialDial", "Pie", "Radar", "Parallel"] {
			assert!(site.contains(&format!("PlotType::{plot_type}")));
		}
	}

	let projection = request
		.split("fn project_named_channels(")
		.nth(1)
		.expect("named projection")
		.split("#[cfg(test)]")
		.next()
		.expect("bounded projection");
	assert!(projection.contains("for field in mark_fields.all()"));
	assert!(!projection.contains("MarkFields::Pie"));
	assert!(!projection.contains("pie_fields"));

	let production = pie
		.split("#[cfg(test)]")
		.next()
		.expect("production pie kernel");
	assert!(production.contains("fn slice_color("));
	assert_eq!(production.matches("self.slice_color(").count(), 2);
	assert!(production.contains("self.category_colors"));
	assert!(production.contains("settings.mark_style.separator.iced()"));
	assert!(!production.contains("settings.chrome.canvas_background.iced()"));
	assert!(production.contains("LegendProvenance::Synthetic"));
	assert!(!production.contains("apply_policy("));
	assert!(factory.contains("category_colors: spec"));

	assert_registered_handler(&root, "pie", "Free", "Pie", "raw_pie");
	assert_eq!(presentation.matches("SpannedMarkFields::Pie").count(), 1);
	assert!(compiler.contains("\"pie\" => Some(&[2])"));
	assert!(envelope.contains("Self::Pie { fields, origins }"));
	assert!(host.contains("fn pie_failure_span("));
	assert!(host.contains("pie_output_span"));
}

#[test]
fn product_0108_violin_surface_consumes_one_relation_side_density_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read factory");
	let kernel =
		std::fs::read_to_string(root.join("plot/kernels/violin.rs")).expect("read violin kernel");
	let density = std::fs::read_to_string(root.join("plot/kernels/violin_density.rs"))
		.expect("read violin density substrate");
	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");
	let envelope =
		std::fs::read_to_string(root.join("rune_relation_envelope.rs")).expect("read envelope");
	let host = std::fs::read_to_string(root.join("gui/state.rs")).expect("read host");

	assert!(spec.contains("pub struct ViolinFields"));
	assert!(spec.contains("MarkFields::Violin"));
	assert!(spec.contains("fn resolve_violin_channels("));
	assert!(spec.contains("violin cannot be resolved from a schema"));
	assert!(spec.contains("kernel_density(...) joined to grouped approx_quantile(...)"));
	assert!(spec.contains("a text or boolean (Nominal) dtype"));
	assert_registered_handler(&root, "violin", "Free", "Violin", "raw_violin");
	assert!(compiler.contains("\"violin\" => Some(&[4])"));
	assert!(envelope.contains("Self::Violin { fields, origins }"));
	assert!(host.contains("fn violin_failure_span("));

	assert!(factory.contains("prepare_violin_density_data("));
	assert!(factory.contains("PRODUCT-0108 planning validated this violin density relation"));
	assert!(kernel.contains("Arc<ViolinDensity>"));
	assert!(kernel.contains("normalized_widths(ViolinWidthNormalization::Figure)"));
	for forbidden in ["compute_kde", ".group_by(", ".lazy()", "DataType"] {
		assert!(
			!kernel.contains(forbidden),
			"PRODUCT-0108 violin kernel retained renderer-side statistic `{forbidden}`"
		);
	}
	assert!(!density.contains("#![allow(dead_code)]"));
}

#[test]
fn product_0104_radar_and_parallel_share_one_honest_multivariate_row_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let rows = std::fs::read_to_string(root.join("plot/kernels/multivariate_rows.rs"))
		.expect("read multivariate row authority");
	let radar =
		std::fs::read_to_string(root.join("plot/kernels/radar.rs")).expect("read radar kernel");
	let parallel = std::fs::read_to_string(root.join("plot/kernels/parallel.rs"))
		.expect("read parallel kernel");
	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read factory");

	let radar_production = radar
		.split("#[cfg(test)]")
		.next()
		.expect("production radar");
	let parallel_production = parallel
		.split("#[cfg(test)]")
		.next()
		.expect("production parallel");
	let row_production = rows
		.split("#[cfg(test)]")
		.next()
		.expect("production multivariate rows");

	for kernel in [radar_production, parallel_production] {
		assert!(kernel.contains("Arc<MultivariateRows>"));
		assert!(kernel.contains("fn display_ranges(&self)"));
		for removed in [
			"PreparedData",
			"row_categories",
			"num_categories",
			"cat_to_idx",
			"unwrap_or(&0)",
		] {
			assert!(!kernel.contains(removed), "kernel retained `{removed}`");
		}
	}
	assert!(radar_production.contains("difference * 0.1"));
	assert!(parallel_production.contains(".map(<[(f64, f64)]>::to_vec)"));
	assert!(radar_production.contains("builder.close()"));
	assert!(parallel_production.contains("builder.line_to(p)"));

	assert!(row_production.contains("pub struct MultivariateRows"));
	assert!(row_production.contains("category: Option<String>"));
	assert!(row_production.contains("BTreeSet"));
	assert!(row_production.contains("exact_f64(value)"));
	for forbidden in [
		"No Data",
		"All Data",
		"Series::new_empty",
		".unique()",
		".sort(",
		"unwrap_or(&0)",
		"iced::Point",
		"PathBuilder",
	] {
		assert!(
			!row_production.contains(forbidden),
			"PRODUCT-0104 shared authority retained `{forbidden}`"
		);
	}

	assert!(request.contains(
		"ResolvedKernelFields::Parallel {\n\t\tdimensions,\n\t\tcategory,\n\t}\n\t| crate::plot::spec::ResolvedKernelFields::Radar"
	));
	assert_eq!(
		factory
			.matches("prepare_multivariate_rows(df, dimensions, category.as_deref())")
			.count(),
		2
	);
	assert_eq!(
		factory
			.matches("PRODUCT-0104 planning validated this multivariate row collection")
			.count(),
		2
	);
}

#[test]
fn product_0105_variadic_multivariate_surface_has_one_transport_admission_and_colour_path() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let fields = std::fs::read_to_string(root.join("plot/spec/multivariate_fields.rs"))
		.expect("read multivariate field carrier");
	let rows = std::fs::read_to_string(root.join("plot/kernels/multivariate_rows.rs"))
		.expect("read multivariate rows");
	let radar =
		std::fs::read_to_string(root.join("plot/kernels/radar.rs")).expect("read radar kernel");
	let parallel = std::fs::read_to_string(root.join("plot/kernels/parallel.rs"))
		.expect("read parallel kernel");
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");
	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read factory");

	assert!(spec.contains("pub struct MarkFieldIter<'a>"));
	let iterator = spec
		.split("pub struct MarkFieldIter<'a>")
		.nth(1)
		.and_then(|rest| rest.split("impl MarkFields").next())
		.expect("concrete iterator block");
	assert!(!iterator.contains("Vec<"));
	assert!(!iterator.contains("Box<dyn Iterator"));
	assert!(!spec.contains("[Option<&str>; 7]"));
	assert!(spec.contains("Self::Radar(fields) | Self::Parallel(fields)"));
	assert!(fields.contains("pub struct MultivariateFields"));
	assert!(rows.contains("use crate::plot::spec::MultivariateFields;"));
	assert!(!rows.contains("pub struct MultivariateFields"));

	assert_registered_handler(&root, "radar", "Free", "Radar", "raw_radar");
	assert_registered_handler(&root, "parallel", "Free", "Parallel", "raw_parallel");
	assert_registered_handler(
		&root,
		"category",
		"Method",
		"MarkCategory",
		"raw_mark_category",
	);
	assert!(presentation.contains("(values.len() - 2) % 3 != 0"));
	assert!(presentation.contains("(3n + 2 values)"));
	assert!(presentation.contains("validate_multivariate_dimension_count(field_count)"));
	let category = presentation
		.split("fn mark_category(")
		.nth(1)
		.and_then(|rest| rest.split("fn ").next())
		.expect("category modifier");
	assert!(category.contains("SpannedMarkFields::Radar"));
	assert!(category.contains("SpannedMarkFields::Parallel"));
	assert!(!category.contains("mark.modified = true"));
	assert!(compiler.contains("matches!(name.as_str(), \"radar\" | \"parallel\")"));

	assert_eq!(
		spec.matches("validate_multivariate_dimension_count(")
			.count(),
		3
	);
	assert_eq!(
		spec.matches("PlotType::Radar => AuthoredFamilyId::RADAR")
			.count(),
		1
	);
	assert_eq!(
		spec.matches("PlotType::Parallel => AuthoredFamilyId::PARALLEL")
			.count(),
		1
	);
	assert!(spec.contains("pub(crate) const ALL: [Self; 10]"));
	let authored_blocks = spec.matches("let authored_categories = matches!(").count();
	assert_eq!(authored_blocks, 2);
	for plot_type in ["PlotType::Radar", "PlotType::Parallel"] {
		assert_eq!(spec.matches(plot_type).filter(|_| true).count() > 0, true);
		for block in spec.split("let authored_categories = matches!(").skip(1) {
			let block = block.split(';').next().expect("authored category block");
			assert!(
				block.contains(plot_type),
				"missing {plot_type} from authored categories"
			);
		}
	}

	for kernel in [&radar, &parallel] {
		let production = kernel
			.split("#[cfg(test)]")
			.next()
			.expect("kernel production");
		assert!(production.contains("fn category_color("));
		assert_eq!(production.matches("self.category_color(").count(), 2);
		assert!(production.contains("self.category_colors"));
	}
	assert_eq!(
		factory
			.matches("category_colors: category.as_ref().and_then")
			.count(),
		2
	);
}

#[test]
fn product_0106_vertical_guides_have_one_reservation_authority_and_no_measurement_seam() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read plot grid");
	let radar =
		std::fs::read_to_string(root.join("plot/kernels/radar.rs")).expect("read radar kernel");
	let parallel = std::fs::read_to_string(root.join("plot/kernels/parallel.rs"))
		.expect("read parallel kernel");

	assert_eq!(common.matches("struct VerticalGuideBands").count(), 1);
	let authority = common
		.split("struct VerticalGuideBands")
		.nth(1)
		.and_then(|rest| rest.split("pub struct PlotCanvasState").next())
		.expect("PRODUCT-0106 vertical guide authority");
	for required in [
		"settings.title_offset + settings.title_size",
		"settings.subtitle_offset + settings.subtitle_size",
		"Self::FACET_LABEL_TOP_INSET + Self::facet_label_size(settings).pixels()",
		"fn grid_tiled_area(",
		"fn grid_cell_plot_area(",
	] {
		assert!(
			authority.contains(required),
			"missing `{required}` from authority"
		);
	}
	assert!(common.contains("VerticalGuideBands::resolve_with_requirements_and_x_label("));

	let grid_production = grid
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("grid production");
	assert!(
		grid.contains("#[cfg(test)]\nfn grid_tiled_area("),
		"the legacy tiled-area helper is test-only; production must receive requirements"
	);
	let cell_local = grid_production
		.split("fn grid_plot_area(")
		.nth(1)
		.and_then(|rest| rest.split("fn grid_plot_extent(").next())
		.expect("bounded cell-local geometry");
	assert_eq!(
		cell_local.matches("VerticalGuideBands::resolve(").count(),
		1,
		"cell-local facet reservations remain the sole production legacy resolve call"
	);
	assert_eq!(
		grid_production
			.matches("VerticalGuideBands::resolve_with_requirements_and_x_label(")
			.count(),
		1,
		"the figure-wide tiled area receives requirements and its actual axis-title presence before cell geometry"
	);
	assert!(
		grid_production.contains("fn grid_x_label_visible("),
		"grid-wide bottom geometry derives title presence from the same rendered axis title"
	);
	assert!(grid_production.contains(".grid_tiled_area(outer, settings)"));
	assert!(grid_production.contains(".grid_cell_plot_area(cell_bounds, settings)"));
	assert!(!grid_production.contains("PANEL_LABEL_TOP_INSET"));
	assert!(!grid_production.contains("grid_heading_band"));

	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.and_then(|rest| rest.split("pub struct IcedBackend").next())
		.expect("PlotBackend trait");
	for forbidden in ["measure_text", "glyph_extent", "text_extent"] {
		assert!(
			!backend.contains(forbidden),
			"PRODUCT-0106 added `{forbidden}`"
		);
	}

	let radar_production = radar
		.split("#[cfg(test)]")
		.next()
		.expect("radar production");
	assert_eq!(
		radar_production
			.matches("if i == 0 && num_circles > 0")
			.count(),
		1
	);
	assert_eq!(radar_production.matches("let angle =").count(), 3);
	assert!(!parallel.contains("VerticalGuideBands"));
}

#[test]
fn product_0109_palette_kind_is_carried_and_continuous_allocation_is_stateless() {
	let polariton = Path::new(env!("CARGO_MANIFEST_DIR"));
	let spec =
		std::fs::read_to_string(polariton.join("src/plot/spec/mod.rs")).expect("read plot spec");
	let theme = std::fs::read_to_string(polariton.join("../libaryon/src/engine/theme.rs"))
		.expect("read libaryon theme catalogue");

	let resolved_theme = theme
		.split("pub struct Theme")
		.nth(1)
		.and_then(|rest| rest.split("impl Theme").next())
		.expect("resolved theme payload");
	assert!(resolved_theme.contains("family: ThemeFamily"));
	let theme_impl = theme
		.split("impl Theme")
		.nth(1)
		.and_then(|rest| rest.split("fn fill_gruvbox").next())
		.expect("resolved theme implementation");
	assert!(theme_impl.contains("pub const fn family(&self) -> ThemeFamily"));

	let selection = spec
		.split("pub struct CategoricalPaletteSelection")
		.nth(1)
		.and_then(|rest| {
			rest.split("pub enum CategoricalPaletteSelectionError")
				.next()
		})
		.expect("categorical palette selection");
	assert!(selection.contains("allocation_kind: CategoricalAllocationKind"));
	assert!(
		selection
			.contains("ThemeFamily::Continuous => CategoricalAllocationKind::ContinuousDerived")
	);
	assert!(selection.contains("ThemeFamily::Curated => CategoricalAllocationKind::Curated"));
	assert!(!selection.contains("identity =="));

	let resolver = spec
		.split("fn resolve_explicit_positions(")
		.nth(1)
		.and_then(|rest| rest.split("fn resolve_explicit_category_colors(").next())
		.expect("categorical position resolver");
	assert!(resolver.contains("CategoricalAllocationKind::ContinuousDerived"));
	assert!(resolver.contains("clear_color_lineage(identity, &lineage)"));
	assert!(resolver.contains("zip(evenly_spread_positions(categories.len(), selection.len()))"));
	let clear = spec
		.split("fn clear_color_lineage(")
		.nth(1)
		.and_then(|rest| rest.split("fn resolve_explicit_positions(").next())
		.expect("continuous lineage deletion");
	assert!(clear.contains(".assignments"));
	assert!(clear.contains("identity.spaces.remove(lineage)"));
}

#[test]
fn product_0121_facet_gutter_member_reuses_the_existing_figure_grid_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read grid layout");
	let production_grid = grid
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("production grid layout");
	assert!(!production_grid.contains("GRID_GAP"));
	assert!(production_grid.contains("pub struct FigureGridLayoutSettings"));
	assert!(production_grid.contains("requested.min(outer.width() / columns.max(1.0))"));
	assert!(production_grid.contains("requested.min(outer.height() / rows.max(1.0))"));
	assert!(
		production_grid
			.matches("grid_cell_bounds_with_settings(")
			.count() >= 5,
		"paint, overlay, hover, and shared extent must consume one resolved gutter"
	);

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot settings");
	assert!(
		!theme.contains("facet_gutter"),
		"a shared inter-facet divider must not become plot padding or a plot settings field"
	);
	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read Rune surface");
	assert!(!presentation.contains("__facet_gutter"));
	assert!(presentation.contains("__gutter"));
	assert!(presentation.contains("gutter(...) requires a facet(...) receiver"));
	assert!(presentation.contains("element.facet_gutter = Some(value as f32)"));
}

#[test]
fn product_0112_hover_typography_stays_typed_and_off_canvas() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	assert!(spec.contains("pub struct HoverTextSize(f32)"));
	assert!(spec.contains("pub const MIN_PX: f32 = 6.0"));
	assert!(spec.contains("pub const MAX_PX: f32 = 96.0"));

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	assert!(theme.contains("pub hover_size: HoverTextSize"));
	assert!(theme.contains("pub hover_size: Override<HoverTextSize>"));
	assert!(theme.contains("settings.hover_size = value.hover_size"));
	assert!(theme.contains("settings.hover_size = value;"));

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.and_then(|rest| rest.split("pub trait PlotKernel").next())
		.expect("bounded plot backend contract");
	// Existing backend documentation and kernel APIs legitimately mention
	// hover. PRODUCT-0112 forbids only this new, widget-owned setting from
	// crossing that seam.
	assert!(!backend.contains("HoverTextSize"));
	assert!(!backend.contains("hover_size"));
	assert!(common.contains("pub hover_size: HoverTextSize"));
	let kernel_dir = root.join("plot/kernels");
	for entry in std::fs::read_dir(&kernel_dir).expect("read plot kernels") {
		let entry = entry.expect("read kernel directory entry");
		let path = entry.path();
		if path.extension().is_some_and(|extension| extension == "rs") {
			let source = std::fs::read_to_string(&path).expect("read kernel source");
			assert!(
				!source.contains("hover_size"),
				"{} must not consume the widget-owned hover size",
				path.display()
			);
		}
	}

	let view =
		std::fs::read_to_string(root.join("gui/plot_pane/view.rs")).expect("read plot pane view");
	let production_view = view
		.split("\n#[cfg(test)]")
		.next()
		.expect("production plot-pane view section");
	assert!(production_view.contains("text(info).size(hover_text_pixels(&state.plot_settings))"));

	let registry = presentation_registry(&root);
	assert!(registry_has(&registry, "hover_size", "Free"));
	assert!(
		!registry_has(&registry, "font_size", "Free"),
		"hover_size remains the standalone spelling because hover has no receiver"
	);

	let session = std::fs::read_to_string(root.join("session_persistence.rs"))
		.expect("read session persistence");
	assert!(session.contains("struct DurableLayer"));
	assert!(!session.contains("HoverTextSize"));
	assert!(!session.contains("hover_size"));
}

#[test]
fn product_0122_axis_and_tick_typography_is_typed_partial_and_keeps_legend_blocked() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	for text_size in ["TitleTextSize", "SubtitleTextSize"] {
		assert!(
			spec.contains(&format!("pub struct {text_size}(f32)")),
			"{text_size} stays a private bounded scalar"
		);
	}
	for text_size in [
		"XLabelTextSize",
		"YLabelTextSize",
		"XTickTextSize",
		"YTickTextSize",
	] {
		assert!(
			spec.contains(text_size) && spec.contains("macro_rules! axis_text_size"),
			"{text_size} is generated only by the private bounded-newtype template"
		);
	}

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	assert!(theme.contains("pub title_size: TitleTextSize"));
	assert!(theme.contains("pub subtitle_size: SubtitleTextSize"));
	assert!(theme.contains("pub title_size: Override<TitleTextSize>"));
	assert!(theme.contains("pub subtitle_size: Override<SubtitleTextSize>"));
	for slot in [
		"pub x_label_size: Override<XLabelTextSize>",
		"pub y_label_size: Override<YLabelTextSize>",
		"pub x_tick_size: Override<XTickTextSize>",
		"pub y_tick_size: Override<YTickTextSize>",
	] {
		assert!(
			theme.contains(slot),
			"each exposed role keeps its own partial slot: {slot}"
		);
	}

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	assert!(common.contains("settings.title_size.pixels()"));
	assert!(common.contains("settings.subtitle_size.pixels()"));
	assert!(common.contains("top_px.y - self.settings.x_label_size.pixels()"));
	assert!(common.contains("size: iced::Pixels(self.settings.x_label_size.pixels())"));
	assert!(!common.contains("size: iced::Pixels(22.0)"));
	assert!(!common.contains("(settings.legend_size + 2.0).max(10.0)"));
	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.and_then(|rest| rest.split("pub trait PlotKernel").next())
		.expect("bounded plot backend contract");
	assert!(!backend.contains("TextSize"));
	assert!(!backend.contains("text_extent"));

	let registry = presentation_registry(&root);
	assert!(registry_has(&registry, "font_size", "Method"));
	for blocked in ["font_size", "legend_size"] {
		assert!(
			!registry_has(&registry, blocked, "Free"),
			"{blocked} is not a new top-level spelling"
		);
	}
	for member in ["x_axis", "y_axis"] {
		assert!(registry_has(&registry, member, "Free"));
	}
	for modifier in ["label", "label_size", "tick_size"] {
		assert!(registry_has(&registry, modifier, "Method"));
	}
	for retired in ["x_tick_size", "y_tick_size", "x_label", "y_label"] {
		assert!(!registry_has(&registry, retired, "Free"));
	}

	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");
	assert!(presentation.contains("TextHandle(TextHandleKind)"));
	assert!(presentation.contains("a bare text member requires text or .font_size(...)"));
	assert!(presentation.contains("padding() requires at least one edge modifier"));
	assert!(presentation.contains("authored_settings.y_label_size = Override::Set(size);"));
	assert!(presentation.contains("authored_settings.x_tick_size = Override::Set(size);"));
}

#[test]
fn product_0124_facet_label_typography_is_typed_member_owned_and_independent_from_legend() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	assert!(spec.contains("FacetLabelTextSize"));
	assert!(spec.contains("\"facet label size\""));
	assert!(spec.contains("FacetLabelTextSizeError"));

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	assert!(theme.contains("pub facet_label_size: FacetLabelTextSize"));
	assert!(theme.contains("pub facet_label_size: Override<FacetLabelTextSize>"));
	assert!(theme.contains("settings.facet_label_size = value.facet_label_size"));
	assert!(theme.contains("settings.facet_label_size = value;"));
	assert!(theme.contains("pub legend_size: LegendTextSize"));

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot geometry");
	assert!(common.contains("pub facet_label_size: FacetLabelTextSize"));
	assert!(common.contains("Self::facet_label_size(settings).pixels()"));
	assert!(common.contains("settings.facet_label_size"));
	assert!(!common.contains("(settings.legend_size + 2.0).max(10.0)"));

	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read plot grid");
	assert!(grid.contains("VerticalGuideBands::facet_label_size(&resolved).pixels()"));
	assert!(!grid.contains("legend_size"));

	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read Rune surface");
	assert!(presentation.contains("fn raw_figure_label_size("));
	assert!(presentation.contains("FacetLabelTextSize::new"));
	assert!(presentation.contains("facet(...).label_size(...) may appear at most once"));
	assert!(presentation.contains("authored_settings.facet_label_size = Override::Set(size);"));
	assert!(
		presentation
			.contains("label_size(...) requires an x_axis(), y_axis(), or facet(...) receiver")
	);

	let registry = presentation_registry(&root);
	assert!(!registry_has(&registry, "facet_label_size", "Free"));
	assert!(registry_has(&registry, "label_size", "Method"));
}

#[test]
fn product_0125_legend_member_owns_typed_legend_text_without_measurement() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	assert!(spec.contains("LegendTextSize"));
	assert!(spec.contains("\"legend text size\""));

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	assert!(theme.contains("pub legend_size: LegendTextSize"));
	assert!(theme.contains("pub legend_size: Override<LegendTextSize>"));
	assert!(theme.contains("settings.legend_size = value.legend_size"));
	assert!(theme.contains("settings.legend_size = value;"));
	assert!(theme.contains("pub facet_label_size: FacetLabelTextSize"));

	let legend =
		std::fs::read_to_string(root.join("plot/legend.rs")).expect("read legend renderer");
	assert!(legend.contains("settings.legend_size.pixels()"));
	assert!(!legend.contains("iced::Pixels(16.0)"));
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.and_then(|rest| rest.split("pub trait PlotKernel").next())
		.expect("bounded plot backend contract");
	assert!(!backend.contains("text_extent"));

	let registry = presentation_registry(&root);
	assert!(registry_has(&registry, "legend", "Free"));
	for retired in ["legend_at", "hide_legend"] {
		assert!(!registry_has(&registry, retired, "Free"));
	}
	for method in ["at", "hide", "font_size"] {
		assert!(registry_has(&registry, method, "Method"));
	}

	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read Rune surface");
	assert!(presentation.contains("fn raw_legend("));
	assert!(presentation.contains("fn raw_legend_at("));
	assert!(presentation.contains("fn raw_legend_hide("));
	assert!(presentation.contains("legend() requires .at(...), .hide(), or .font_size(...)"));
	assert!(presentation.contains("authored_settings.legend_size = Override::Set(size);"));
	assert!(!presentation.contains("fn raw_hide_legend("));
}

#[test]
fn product_0123_axis_members_are_the_only_axis_surface_and_merge_fieldwise() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let registry = presentation_registry(&root);
	for member in ["x_axis", "y_axis"] {
		assert!(registry_has(&registry, member, "Free"));
	}
	for modifier in [
		"label",
		"label_size",
		"bounds",
		"tick_step",
		"minor_ticks",
		"tick_size",
		"tick_rotation",
	] {
		assert!(registry_has(&registry, modifier, "Method"));
	}
	for retired in [
		"x_bounds",
		"y_bounds",
		"x_tick_step",
		"y_tick_step",
		"x_minor_ticks",
		"y_minor_ticks",
		"x_label",
		"y_label",
		"x_tick_rotation",
		"x_tick_size",
		"y_tick_size",
	] {
		assert!(
			!registry_has(&registry, retired, "Free"),
			"retired axis spelling remains in the free vocabulary: {retired}"
		);
	}

	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read presentation");
	let production = presentation
		.split("\n#[cfg(test)]\nmod tests {")
		.next()
		.expect("production Rune presentation");
	for raw in [
		"fn raw_x_axis(",
		"fn raw_y_axis(",
		"fn raw_axis_label(",
		"fn raw_figure_label_size(",
		"fn raw_axis_bounds(",
		"fn raw_axis_tick_step(",
		"fn raw_axis_minor_ticks(",
		"fn raw_axis_tick_size(",
		"fn raw_axis_tick_rotation(",
		"fn merge_axis(",
	] {
		assert!(
			production.contains(raw),
			"missing axis-member authority: {raw}"
		);
	}
	for retired in [
		"fn raw_x_bounds(",
		"fn raw_y_bounds(",
		"fn raw_x_tick_step(",
		"fn raw_y_tick_step(",
		"fn raw_x_minor_ticks(",
		"fn raw_y_minor_ticks(",
		"fn raw_x_label(",
		"fn raw_y_label(",
		"fn raw_x_tick_rotation(",
		"fn raw_x_tick_size(",
		"fn raw_y_tick_size(",
	] {
		assert!(
			!production.contains(retired),
			"retired axis producer remains: {retired}"
		);
	}
	assert!(production.contains("AxisProperties"));
	assert!(production.contains("properties.is_empty()"));
	assert!(production.contains("&mut parts.x_bounds,"));
	assert!(production.contains("&mut parts.y_bounds,"));
	assert!(production.contains("tick_rotation(...) is available on x_axis(), not y_axis()"));

	let exhibit = std::fs::read_to_string(root.join("../../../uat/test38.rn"))
		.expect("read PRODUCT-0123 axis exhibit");
	assert!(exhibit.contains("x_axis()"));
	assert!(exhibit.contains("y_axis().label(\"Revenue · $k\").label_size(18)"));
	assert!(exhibit.contains("y_axis().bounds(0, 50).tick_step(10).minor_ticks(1).tick_size(12)"));
}

#[test]
fn product_0114_band_requirements_precede_and_feed_shared_geometry() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	assert!(common.contains("pub(crate) struct BandRequirements"));
	assert!(common.contains("pub(crate) enum AxisBandRequirement"));
	assert!(common.contains("from a realized tick set"));
	assert!(common.contains("requirements: &BandRequirements"));
	assert!(common.contains("requirements.estimated_x_width"));
	assert!(common.contains("requirements.estimated_y_width"));

	for entrypoint in [
		"fn render_plot_layer_in_widget",
		"fn render_overlay_layer_in_widget",
		"let widget_bounds = Rect::<Widget>::from_iced_frame(bounds);",
	] {
		let section = common
			.split(entrypoint)
			.nth(1)
			.expect("single-plot entrypoint")
			.split("let transform")
			.next()
			.expect("bounded single-plot geometry setup");
		let layout = section
			.find("let layout = self.kernel.layout")
			.expect("layout first");
		let requirements = section
			.find("let requirements = BandRequirements::from_layout")
			.expect("requirements after layout");
		let area = section
			.find(".plot_area(")
			.expect("plot area after requirements");
		assert!(layout < requirements && requirements < area);
	}

	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read plot grid");
	assert!(grid.contains("fn grid_tiled_area_with_requirements"));
	let composite = grid
		.split("impl PlotComposite for PlotGridKernel")
		.nth(1)
		.expect("grid composite implementation");
	for entrypoint in [
		"fn render_plot_layer(",
		"fn render_overlay_layer(",
		"fn hit(",
		// PRODUCT-0155 §3: the projection traversal derives geometry exactly as
		// the hit does; it is held to the same order here.
		"fn hit_regions(",
	] {
		let section = composite
			.split(entrypoint)
			.nth(1)
			.expect("grid entrypoint")
			.split("for (index, cell)")
			.next()
			.expect("bounded grid geometry setup");
		let layouts = section
			.find("let layouts = self.cell_layouts")
			.expect("layouts first");
		let requirements = section
			.find("BandRequirements::across_layouts")
			.expect("grid aggregates requirements");
		let tiled = section
			.find("grid_tiled_area_with_requirements")
			.expect("tiled geometry reads aggregated requirements");
		assert!(layouts < requirements && requirements < tiled);

		// PRODUCT-0139 §6, composite half: cell geometry resolves per cell, and
		// all three entry points must say so identically. They re-derive this
		// sequence independently, so a rule enforced in one of them and not the
		// others is the same divergence the step-0 audit found between
		// `settings` and `settings_model`.
		assert!(
			section.contains("self.cell_layouts(settings)"),
			"{entrypoint} must pass the settings model, not a figure-wide resolution"
		);
	}

	// The per-cell plot rectangle — the raster's width and height — is cell
	// geometry too, and is resolved once per cell in each entry point's loop.
	// PRODUCT-0149 adds the interactive request and draw traversals to the three
	// exact/overlay/hit traversals banked here; PRODUCT-0150 adds the gesture
	// viewport-geometry traversal; PRODUCT-0151 adds selection geometry,
	// click-admission, and retained-emphasis traversals. PRODUCT-0155 adds the hit-projection
	// traversal. PRODUCT-0157 adds the series-coverage traversal.
	// Each independently
	// resolves the cell's plot rectangle through its kernel.
	assert_eq!(
		composite
			.matches("cell.kernel.resolve_settings(settings)")
			.count(),
		11,
		"all eleven grid entry points resolve the cell's plot area through its own kernel"
	);
	assert!(
		!composite.contains("grid_plot_area(cell_bounds, cell.cell.label.as_deref(), &resolved)"),
		"no grid entry point may take a cell's plot area from the figure-wide resolution"
	);
}

#[test]
fn product_0114_estimates_are_declared_without_measurement() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let estimates =
		std::fs::read_to_string(root.join("plot/text_estimate.rs")).expect("read text estimates");
	for declaration in [
		"FORMATTED_LABEL_ADVANCE_EM",
		"OUT_OF_PARTITION_LABEL_ADVANCE_EM",
		"scientific `e`",
		"per-character-class budgeting",
	] {
		assert!(
			estimates.contains(declaration),
			"PRODUCT-0114 estimate contract must declare {declaration}"
		);
	}

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot backend");
	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.and_then(|rest| rest.split("pub trait PlotKernel").next())
		.expect("bounded plot backend contract");
	for forbidden in ["measure_text", "text_extent", "glyph_extent"] {
		assert!(
			!backend.contains(forbidden),
			"PRODUCT-0114 added backend text measurement: {forbidden}"
		);
	}
}

#[test]
fn product_0117_declares_one_plot_text_face_and_a_total_static_ascii_budget() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let fonts = std::fs::read_to_string(root.join("fonts/mod.rs")).expect("read plot fonts");
	for declaration in [
		"POLARITON_PLOT_SANS_REGULAR",
		"PolaritonPlotSans-Regular.ttf",
		"PLOT_TEXT_FONT",
		"Font::with_name(\"Polariton Plot Sans\")",
		"NotoSans-OFL-1.1.txt",
	] {
		assert!(
			fonts.contains(declaration),
			"PRODUCT-0117 plot face declaration must retain {declaration}"
		);
	}

	let text_estimate =
		std::fs::read_to_string(root.join("plot/text_estimate.rs")).expect("read text estimates");
	for declaration in [
		"OUT_OF_PARTITION_LABEL_ADVANCE_EM",
		"fn ascii_label_advance_em",
		"' '..='~'",
		"PRODUCT-0117 censused every",
	] {
		assert!(
			text_estimate.contains(declaration),
			"PRODUCT-0117 static estimate contract must retain {declaration}"
		);
	}

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot backend");
	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.and_then(|rest| rest.split("pub trait PlotKernel").next())
		.expect("bounded plot backend contract");
	assert!(backend.contains("text.font = PLOT_TEXT_FONT;"));
	assert!(backend.contains("self.fill_text_raw(text);"));

	let plot_root = root.join("plot");
	let direct_overrides = rust_files(&plot_root)
		.into_iter()
		.filter_map(|path| std::fs::read_to_string(path).ok())
		.map(|source| {
			source
				.lines()
				.filter(|line| line.contains("fn fill_text(&mut self"))
				.count()
		})
		.sum::<usize>();
	assert_eq!(
		direct_overrides, 1,
		"PRODUCT-0117 permits only PlotBackend's central font-stamping method; an implementation override would bypass the declared face"
	);

	let state = std::fs::read_to_string(root.join("gui/state.rs")).expect("read gui state");
	assert!(state.contains(".font(POLARITON_PLOT_SANS_REGULAR)"));
	assert!(
		!state.contains(".default_font(PLOT_TEXT_FONT)"),
		"PRODUCT-0117 must not make plot text the application default"
	);
}

#[test]
fn product_0115_rotation_is_typed_and_axis_overlays_share_one_policy() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	assert!(spec.contains("pub struct TickRotation(f32)"));
	assert!(spec.contains("pub const MIN_DEGREES: f32 = -90.0"));
	assert!(spec.contains("pub const MAX_DEGREES: f32 = 90.0"));
	assert!(spec.contains("pub fn radians(self) -> f32"));

	let theme = std::fs::read_to_string(root.join("plot/theme.rs")).expect("read plot theme");
	assert!(theme.contains("pub x_tick_rotation: TickRotation"));
	assert!(theme.contains("pub x_tick_rotation: Override<TickRotation>"));
	assert!(theme.contains("settings.x_tick_rotation = value.x_tick_rotation"));
	assert!(theme.contains("settings.x_tick_rotation = value;"));

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let rotation = common
		.split("pub(crate) struct AxisTextAnchoring")
		.nth(1)
		.and_then(|rest| rest.split("fn resolve_canvas_text").next())
		.expect("production x-tick placement section");
	assert!(rotation.contains("struct XTickTextPlacement"));
	assert!(rotation.contains("fn resolve("));
	assert!(common.contains("pub(crate) struct XAxisGuideLayout"));
	let guides = common
		.split("pub(crate) struct XAxisGuideLayout")
		.nth(1)
		.and_then(|rest| rest.split("pub(crate) fn facet_label_size").next())
		.expect("bounded shared x-axis guide authority");
	for declaration in [
		"pub(crate) tick_end: f32",
		"pub(crate) title_top: Option<f32>",
		"pub(crate) fn x_axis_guides(",
		"tick_end + Self::x_axis_title_gap(settings)",
		"map(|top| top + settings.x_label_size.pixels())",
		"unwrap_or(tick_end)",
	] {
		assert!(
			guides.contains(declaration),
			"PRODUCT-0115 keeps x-axis title reservation and placement in one guide authority: {declaration}"
		);
	}
	let axis_labels = common
		.split("pub(crate) fn draw_axis_labels")
		.nth(1)
		.and_then(|rest| rest.split("impl<'a> Program<PlotMessage>").next())
		.expect("bounded axis-title drawing authority");
	assert!(axis_labels.contains("VerticalGuideBands::x_axis_guides(requirements, true"));
	assert!(axis_labels.contains("+ title_top,"));
	assert!(
		!axis_labels.contains("+ self.settings.x_label_padding,"),
		"PRODUCT-0115 does not place a rotated figure's title from an independent constant"
	);
	let requirements = common
		.split("impl AxisBandRequirement")
		.nth(1)
		.and_then(|rest| rest.split("pub struct CoordinateTransformer").next())
		.expect("bounded formatted-axis reservation estimator");
	assert!(
		requirements
			.contains("estimate_formatted_label_width(&format_label(value, axis_type), font_size)")
	);
	assert!(requirements.contains("estimate_ascii_label_width(label, font_size)"));
	for (entrypoint, next) in [
		(
			"pub(crate) fn draw_cartesian_axes",
			"pub(crate) fn draw_categorical_grid",
		),
		(
			"pub(crate) fn draw_categorical_axes",
			"pub(crate) fn draw_categorical_y_grid",
		),
		(
			"pub(crate) fn draw_categorical_xy_axes",
			"pub(crate) fn draw_parallel_axes",
		),
	] {
		let section = common
			.split(entrypoint)
			.nth(1)
			.and_then(|rest| rest.split(next).next())
			.expect("bounded x-axis draw site");
		assert_eq!(
			section.matches("XTickTextPlacement::resolve(").count(),
			1,
			"{entrypoint} consumes the one placement authority"
		);
	}

	let overlay = common
		.split("fn render_overlay_layer_in_widget")
		.nth(1)
		.and_then(|rest| rest.split("fn render_global_headings").next())
		.expect("bounded ordinary overlay");
	assert_eq!(
		overlay.matches("backend.with_clip(bounds").count(),
		1,
		"the ordinary overlay receives the explicit figure clip once"
	);
	let backend = common
		.split("pub trait PlotBackend")
		.nth(1)
		.and_then(|rest| rest.split("pub trait PlotKernel").next())
		.expect("bounded plot backend contract");
	assert!(!backend.contains("measure_text"));
	assert!(!backend.contains("text_extent"));

	let registry = presentation_registry(&root);
	assert!(registry_has(&registry, "x_axis", "Free"));
	assert!(registry_has(&registry, "tick_rotation", "Method"));
	assert!(!registry_has(&registry, "x_tick_rotation", "Free"));
	assert!(!registry_has(&registry, "x_label_rotation", "Free"));
	assert!(!registry_has(&registry, "y_tick_rotation", "Free"));
}

#[test]
fn product_0127_formatted_requirements_carry_selected_pre_layout_ticks() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let requirement = common
		.split("pub(crate) enum AxisBandRequirement")
		.nth(1)
		.and_then(|rest| rest.split("pub struct CoordinateTransformer").next())
		.expect("bounded axis-band requirement authority");
	for declaration in [
		"placement: TickPlacement",
		".positions(*domain)",
		"unwrap_or_default()",
		"estimate_formatted_label_width(&format_label(value, axis_type), font_size)",
	] {
		assert!(
			requirement.contains(declaration),
			"PRODUCT-0127 formatted requirements retain {declaration}"
		);
	}
	assert!(
		!common.contains("fn formatted_label_candidates"),
		"PRODUCT-0127 retires branch-boundary sampling instead of keeping it as a second requirement authority"
	);

	for entrypoint in [
		"fn render_plot_layer_in_widget",
		"fn render_overlay_layer_in_widget",
		"let widget_bounds = Rect::<Widget>::from_iced_frame(bounds);",
	] {
		let section = common
			.split(entrypoint)
			.nth(1)
			.expect("single-plot entrypoint")
			.split("let transform")
			.next()
			.expect("bounded single-plot geometry setup");
		assert!(
			section.contains("BandRequirements::from_layout(&layout, &self.settings)"),
			"{entrypoint} pairs the layout with its resolved tick placement before geometry"
		);
	}

	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read plot grid");
	assert!(
		grid.matches("BandRequirements::across_layouts(").count() >= 5
			&& grid.matches("&resolved,").count() >= 4,
		"PRODUCT-0127 passes one resolved placement policy into every production grid requirement aggregation"
	);
}

#[test]
fn product_0119_estimates_reserve_space_without_moving_x_tick_ink() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let placement = common
		.split("struct XTickTextPlacement")
		.nth(1)
		.and_then(|rest| rest.split("pub(crate) struct AxisTextVisibility").next())
		.expect("bounded rotated tick placement authority");
	for declaration in ["fn leading_alignment", "fn minimum_vertical_extent"] {
		assert!(
			placement.contains(declaration),
			"PRODUCT-0119 keeps PRODUCT-0116's rotated pivot authority: {declaration}"
		);
	}
	assert!(
		!placement.contains("y_offset"),
		"PRODUCT-0119 forbids estimated vertical tick displacement"
	);
	assert!(
		!placement.contains("x_offset"),
		"PRODUCT-0119 deletes estimate-derived horizontal tick displacement"
	);

	for (entrypoint, next, x_origin, y_origin) in [
		(
			"pub(crate) fn draw_cartesian_axes",
			"pub(crate) fn draw_categorical_grid",
			"p_bottom.x,",
			"p_bottom.y + self.settings.x_label_offset",
		),
		(
			"pub(crate) fn draw_categorical_axes",
			"pub(crate) fn draw_categorical_y_grid",
			"center_px.x,",
			"center_px.y + self.settings.x_label_offset",
		),
		(
			"pub(crate) fn draw_categorical_xy_axes",
			"pub(crate) fn draw_parallel_axes",
			"tick_x,",
			"tick_y + self.settings.x_label_offset",
		),
	] {
		let section = common
			.split(entrypoint)
			.nth(1)
			.and_then(|rest| rest.split(next).next())
			.expect("bounded rotated x-axis draw site");
		assert!(
			section.contains(x_origin) && section.contains(y_origin),
			"PRODUCT-0119 emits the raw tick x and shared baseline y from {entrypoint}"
		);
		assert!(
			!section.contains("placement.y_offset"),
			"PRODUCT-0116 does not restore vertical correction from {entrypoint}"
		);
	}
}

#[test]
fn product_0119_x_tick_labels_follow_their_ticks_and_keep_figure_clips() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let coordinate =
		std::fs::read_to_string(root.join("coordinate.rs")).expect("read coordinate vocabulary");
	for removed in [
		"from_transformer_point(raw: Point)",
		"from_widget(figure: Rect<Widget>, plot_area: Rect<Widget>)",
	] {
		assert!(
			!coordinate.contains(removed),
			"PRODUCT-0119 deletes PRODUCT-0118's now-unused containment transport: {removed}"
		);
	}

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let placement = common
		.split("struct XTickTextPlacement")
		.nth(1)
		.and_then(|rest| rest.split("pub(crate) struct AxisTextVisibility").next())
		.expect("bounded x-tick placement authority");
	for removed in [
		"XTickEndpoint",
		"containment",
		"x_offset",
		"estimated_width",
	] {
		assert!(
			!placement.contains(removed),
			"PRODUCT-0119 keeps no dormant endpoint placement authority: {removed}"
		);
	}

	let overlay = common
		.split("fn render_overlay_layer_in_widget")
		.nth(1)
		.and_then(|rest| rest.split("fn render_global_headings").next())
		.expect("bounded ordinary overlay");
	assert_eq!(
		overlay.matches("backend.with_clip(bounds").count(),
		1,
		"the single-plot overlay keeps its unconditional figure-edge clip"
	);
	for draw in [
		"self.draw_cartesian_axes(",
		"self.draw_categorical_axes(",
		"self.draw_categorical_xy_axes(",
	] {
		let section = overlay
			.split(draw)
			.nth(1)
			.expect("ordinary x-axis draw path");
		assert!(
			!section.contains("containment,"),
			"{draw} has no endpoint-bound transport to make one label free-moving"
		);
	}

	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read grid");
	let grid_overlay = grid
		.split("fn render_overlay_layer(")
		.nth(1)
		.and_then(|rest| rest.split("fn render_global_headings").next())
		.expect("bounded grid overlay");
	assert!(
		grid_overlay.contains("backend.with_clip(bounds.into_iced()"),
		"PRODUCT-0119 retains PRODUCT-0048's figure-wide grid clip"
	);
	for draw in [
		"helper.draw_cartesian_axes(",
		"helper.draw_categorical_axes(",
		"helper.draw_categorical_xy_axes(",
	] {
		let section = grid_overlay
			.split(draw)
			.nth(1)
			.expect("grid x-axis draw path");
		assert!(
			!section.contains("containment,"),
			"{draw} cannot substitute a cell edge for a label's tick"
		);
	}
}

/// PRODUCT-0126 gate 4: the left band and the y-axis title are one authority.
///
/// This is a direct-shape source guard in the PRODUCT-0077 idiom: it proves the
/// title draw path names the shared guide and not the raw offset, not that no
/// helper it calls could reintroduce one. That limit is the instrument's, and
/// the behavioural sequence gates in `plot/common.rs` carry the geometry claim.
#[test]
fn product_0126_the_y_axis_title_draw_path_reads_only_the_shared_left_guide() {
	let root = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read common");
	let draw_axis_labels = common
		.split("fn draw_axis_labels(")
		.nth(1)
		.and_then(|rest| rest.split("\nimpl<'a> Program<PlotMessage>").next())
		.expect("bounded draw_axis_labels body");

	// The positive control: without this the absence assertion below would pass
	// for a body that had simply stopped drawing the title at all.
	assert!(
		draw_axis_labels.contains("VerticalGuideBands::y_axis_guides("),
		"the title draw path must derive its placement from the shared left guide"
	);
	assert!(
		!draw_axis_labels.contains("y_label_padding"),
		"PRODUCT-0126 leaves no independent y_label_padding placement authority"
	);

	// The reservation is the only production consumer of the raw offset, and it
	// reads it inside the guide that also places the title. Every other
	// production mention must be the field's own declaration or default.
	// Bound on the test *modules*, not on `#[cfg(test)]`: the latter also marks
	// inline test-only helpers far above them and would truncate production.
	let production = common
		.split("\n#[cfg(test)]\nmod product_0150_gesture_tests")
		.next()
		.expect("bounded production source");
	let reads: Vec<&str> = production
		.lines()
		.map(str::trim)
		.filter(|line| line.contains("y_label_padding"))
		.filter(|line| {
			!line.starts_with("//")
				&& !line.starts_with("///")
				&& *line != "pub y_label_padding: f32,"
				&& *line != "y_label_padding: 85.0,"
		})
		.collect();
	assert_eq!(
		reads,
		vec![".y_label_padding"],
		"y_label_padding survives as exactly one guide-owned floor"
	);
}

#[test]
fn product_0135_sql_results_reach_the_host_through_exactly_two_paths() {
	// PRODUCT-0135 stage 0 census: the generation gate's discard rule is only
	// as good as this enumeration. SQL dispatches exactly twice — the plain /
	// snippet path (`run_code`) and the notebook-cell path
	// (`run_sql_markdown_cell`) — and each concludes through its own message
	// handler. A third `dispatch` site is a new completion path that must be
	// stamped, so its appearance fails this census rather than shipping
	// unstamped.
	let state =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("src/gui/state.rs"))
			.expect("read state.rs");
	let production = state
		.split("\n#[cfg(test)]\nmod ")
		.next()
		.expect("bounded production source");
	// Stage 1 strengthened the census: both SQL paths now share one gated
	// dispatch future, so exactly one raw dispatch site exists — inside
	// `dispatch_sql_gated` — and both dispatch call sites route through it.
	let dispatch_sites = production.matches("guard.dispatch(&").count();
	assert_eq!(
		dispatch_sites, 1,
		"SQL dispatch must flow through the one generation-gated future; a \
		 second raw dispatch site is an unstamped completion path"
	);
	assert_eq!(
		production.matches("dispatch_sql_gated(").count(),
		3,
		"one definition and exactly two call sites — the plain/snippet path \
		 and the notebook-cell path"
	);
	assert!(production.contains("Message::RunResult {"));
	assert!(production.contains("Message::RunMarkdownCellResult {"));
}

#[test]
fn product_0136_csv_export_keeps_arrow_canonical_and_polars_csv_unadmitted() {
	let manifest_dir = Path::new(env!("CARGO_MANIFEST_DIR"));
	let manifest =
		std::fs::read_to_string(manifest_dir.join("Cargo.toml")).expect("read Polariton manifest");
	let polars = manifest
		.lines()
		.find(|line| line.starts_with("polars = "))
		.expect("Polariton Polars dependency");
	assert!(manifest.contains("arrow-csv = \"58.3.0\""));
	assert!(
		!polars.contains("csv"),
		"PRODUCT-0136 permits Arrow CSV only; Polars csv remains unadmitted: {polars}"
	);

	let artifact = std::fs::read_to_string(manifest_dir.join("src/result_artifact.rs"))
		.expect("read result artifact");
	assert!(artifact.contains("arrow_csv::Writer"));
	assert!(artifact.contains("IpcStreamWriter"));
	assert!(artifact.contains("fn write_polars_csv"));
}

#[test]
fn product_0143_hexbin_renderer_consumes_only_tagged_viewport_artifacts() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let kernel = std::fs::read_to_string(root.join("plot/kernels/hexbin.rs"))
		.expect("read PRODUCT-0143 hexbin renderer");
	let production = kernel
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("bounded hexbin production renderer");
	assert!(production.contains("HexbinViewportArtifact"));
	assert!(production.contains("artifact.bins()"));
	assert!(production.contains("artifact.lattice()"));
	for forbidden in [
		"DataFrame",
		"polars::",
		"group_by",
		"q_frac",
		"fractional_axial",
		"round_ties_even",
		"prepare_hexbin_observations",
	] {
		assert!(
			!production.contains(forbidden),
			"PRODUCT-0143 renderer regained preparation authority through {forbidden}"
		);
	}

	let viewport = std::fs::read_to_string(root.join("plot/kernels/hexbin_viewport.rs"))
		.expect("read PRODUCT-0143 viewport substrate");
	let viewport_production = viewport
		.split("#[cfg(test)]\nmod tests")
		.next()
		.expect("bounded viewport production");
	assert_eq!(
		viewport_production
			.matches("pub struct HexLatticeSpec")
			.count(),
		1
	);
	assert_eq!(
		viewport_production
			.matches("pub struct HexbinPreparationRequest")
			.count(),
		1
	);
	assert_eq!(
		viewport_production
			.matches("pub struct HexbinViewportArtifact")
			.count(),
		1
	);
	assert_eq!(
		viewport_production
			.matches("pub struct MaterializedHexbinPreparer")
			.count(),
		1
	);
	assert_eq!(viewport_production.matches("fn nearest_axial(").count(), 1);
	assert_eq!(viewport_production.matches("round_ties_even()").count(), 3);
	assert!(!viewport_production.contains("group_by"));
	assert!(!viewport_production.contains("toron"));
	assert!(!viewport_production.contains("Deferred"));
}

#[test]
fn product_0143_authority_survives_product_0144_precise_surface_inversions() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let hexbin_branch = factory
		.split("ResolvedKernelFields::Hexbin")
		.nth(1)
		.expect("hexbin factory branch")
		.split("ResolvedKernelFields::Line")
		.next()
		.expect("bounded hexbin factory branch");
	assert!(hexbin_branch.contains("prepare_hexbin_observations"));
	assert!(hexbin_branch.contains("source_token"));
	assert!(!hexbin_branch.contains("ArtifactToken::mint"));
	assert!(!hexbin_branch.contains("0.02"));
	assert!(!factory.contains("prepare_hexbin_data"));
	assert!(factory.contains("create_plot_from_artifact"));
	assert!(factory.contains("source.artifact_token()"));
	let gui = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI plot host");
	let planned_plot_build = gui
		.split("fn build_resolved_plot_task")
		.nth(1)
		.expect("planned plot build task")
		.split("enum RenderedPlotExport")
		.next()
		.expect("bounded planned plot build task");
	assert!(planned_plot_build.contains("create_plot_from_artifact"));
	assert!(planned_plot_build.contains("resolved.artifact"));
	assert!(!planned_plot_build.contains("create_plot(&"));

	let registry = presentation_registry(&root);
	assert!(
		registry_callable(&registry, "hexbin", "Free", "Hexbin").is_some(),
		"PRODUCT-0144 must expose the accepted free constructor"
	);
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let mark_fields = spec
		.split("pub enum MarkFields")
		.nth(1)
		.expect("MarkFields declaration")
		.split("pub struct AuthoredPlotSpec")
		.next()
		.expect("bounded MarkFields");
	assert!(mark_fields.contains("Hexbin"));
	assert!(spec.contains("pub fn hexbin"));
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune relation envelope");
	let spanned = envelope
		.split("pub(crate) enum SpannedMarkFields")
		.nth(1)
		.expect("SpannedMarkFields declaration")
		.split("impl SpannedMarkFields")
		.next()
		.expect("bounded SpannedMarkFields");
	assert!(spanned.contains("Hexbin"));
	assert!(envelope.contains("pub(crate) fn hexbin"));
}

#[test]
fn product_0144_keeps_hexbin_out_of_layers_and_colour_job_words() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let layer_mark = spec
		.split("pub enum LayerMark")
		.nth(1)
		.expect("LayerMark declaration")
		.split("impl LayerMark")
		.next()
		.expect("bounded LayerMark");
	assert!(!layer_mark.contains("Hexbin"));

	let presentation =
		std::fs::read_to_string(root.join("rune_presentation.rs")).expect("read Rune presentation");
	let diverging = presentation
		.split("fn mark_diverging(")
		.nth(1)
		.expect("diverging modifier")
		.split("fn mark_category(")
		.next()
		.expect("bounded diverging modifier");
	assert!(diverging.contains("PlotType::Heatmap | PlotType::Bubble"));
	assert!(!diverging.contains("PlotType::Hexbin"));

	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune relation envelope");
	let into_layer = envelope
		.split("pub(crate) fn into_layer")
		.nth(1)
		.expect("into_layer")
		.split("pub(crate) struct RuneViewIntent")
		.next()
		.expect("bounded into_layer");
	assert!(into_layer.contains("PlotType::Hexbin"));
	assert!(into_layer.contains("Err(self)"));
}

#[test]
fn product_0144_diagnostics_reuse_the_retained_artifact_token() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let gui = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let failure = gui
		.split("fn hexbin_failure_span")
		.nth(1)
		.expect("hexbin diagnostic mapper")
		.split("fn bubble_failure_span")
		.next()
		.expect("bounded hexbin mapper");
	assert!(failure.contains("source_token"));
	assert!(failure.contains("prepare_hexbin_observations"));
	assert!(!failure.contains("ArtifactToken::mint"));
	assert!(!failure.contains("error.field =="));
	let output = gui
		.split("let hexbin_output_span")
		.nth(1)
		.expect("hexbin output-span wiring")
		.split("let bubble_output_span")
		.next()
		.expect("bounded hexbin output span");
	assert!(output.contains("result.artifact_token()"));
}

#[test]
fn product_0143_count_guide_contract_is_closed_and_reservation_precedes_preparation() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let legend = std::fs::read_to_string(root.join("plot/legend.rs"))
		.expect("read PRODUCT-0143 legend contract");
	let count_labels = legend
		.split("fn count(maximum: u64)")
		.nth(1)
		.expect("count label formatter")
		.split("fn count_envelope_width")
		.next()
		.expect("bounded count label formatter");
	assert!(count_labels.contains("low: \"0\".to_string()"));
	assert!(count_labels.contains("high: maximum.to_string()"));
	for forbidden in ["format!", "tick", "separator", "suffix", "abbreviat"] {
		assert!(!count_labels.contains(forbidden));
	}
	let envelope = legend
		.split("fn count_envelope_width")
		.nth(1)
		.expect("count envelope")
		.split("struct GradientGeometry")
		.next()
		.expect("bounded count envelope");
	assert!(envelope.contains("maximum.to_string().len()"));
	assert!(envelope.contains("'0'..='9'"));
	assert!(envelope.contains("f32::max"));

	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let plot_layer = common
		.split("fn render_plot_layer_in_widget")
		.nth(1)
		.expect("ordinary plot layer")
		.split("pub fn render_overlay_layer")
		.next()
		.expect("bounded ordinary plot layer");
	let reservation = plot_layer
		.find("legend_reservation_content")
		.expect("legend reservation call");
	let final_transform = plot_layer
		.find("CoordinateTransformer::new")
		.expect("final transform");
	let preparation = plot_layer
		.find("prepare_viewport")
		.expect("viewport preparation");
	let paint = plot_layer.find("plot_with_motion").expect("mark paint");
	assert!(reservation < final_transform);
	assert!(final_transform < preparation);
	assert!(preparation < paint);
}

#[test]
fn product_0145_fill_between_substrate_has_one_validated_colour_and_hit_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let fill = std::fs::read_to_string(root.join("plot/kernels/fill_between.rs"))
		.expect("read fill-between kernel");
	let production = fill
		.split("#[cfg(test)]")
		.next()
		.expect("fill-between production");
	assert!(production.contains("y_mid: Option<Vec<f64>>"));
	assert!(production.contains("settings.mark_style.fill.iced()"));
	assert!(production.contains("settings.mark_style.fill_detail.iced()"));
	assert!(!production.contains("get_color"));
	assert!(!production.contains("vec![0.0;"));
	assert!(production.contains("OrderedX::Temporal"));
	assert!(production.contains("partition_point"));
	assert!(production.contains("prepare_fill_between_data"));
	assert!(production.contains("Result<FillBetweenPreparedData, FillBetweenPreparationError>"));

	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read plot request");
	let validation = request
		.split("ResolvedKernelFields::FillBetween")
		.nth(1)
		.expect("fill-between planning branch")
		.split("ResolvedKernelFields::Histogram")
		.next()
		.expect("bounded fill-between planning branch");
	assert!(validation.contains("prepare_fill_between_data"));

	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");
	let branch = factory
		.split("ResolvedKernelFields::FillBetween")
		.nth(1)
		.expect("fill-between factory branch")
		.split("fn create_bar_kernel")
		.next()
		.expect("bounded fill-between factory branch");
	assert!(branch.contains("PRODUCT-0145 planning validated fill-between preparation"));
	assert!(!factory.contains("FillBetweenLinePlotKernel"));
	assert!(!factory.contains("is_fill_between_with_line_overlay"));

	let composition =
		std::fs::read_to_string(root.join("plot/composition.rs")).expect("read composition");
	assert!(!composition.contains("(PlotType::Line, PlotType::FillBetween)"));
	assert!(!composition.contains("(PlotType::FillBetween, PlotType::Line)"));
	assert!(composition.contains("(PlotType::Line, PlotType::Scatter)"));
	assert!(composition.contains("(PlotType::Scatter, PlotType::Line)"));
	assert!(!root.join("plot/kernels/fill_between_line.rs").exists());
}

#[test]
fn product_0145_authority_survives_product_0146_precise_surface_inversions() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let registry = presentation_registry(&root);
	let callable = registry_callable(&registry, "fill_between", "Free", "FillBetween")
		.expect("PRODUCT-0146 registers fill_between exactly once");
	assert!(callable.contains("3..=4"));
	let all_marks = registry
		.split("const ALL_MARKS")
		.nth(1)
		.expect("ALL_MARKS settings inventory")
		.split("const POINT_MARKS")
		.next()
		.expect("bounded ALL_MARKS inventory");
	assert!(all_marks.contains("PlotType::FillBetween"));

	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let mark_fields = spec
		.split("pub enum MarkFields")
		.nth(1)
		.expect("MarkFields declaration")
		.split("pub struct AuthoredPlotSpec")
		.next()
		.expect("bounded MarkFields");
	assert!(mark_fields.contains("FillBetween"));
	assert!(spec.contains("pub fn fill_between"));
	let automatic = spec
		.split("PlotType::FillBetween => {")
		.nth(1)
		.expect("automatic fill-between arm")
		.split("\n\t})")
		.next()
		.expect("bounded automatic arm");
	assert!(automatic.contains("name.as_str() != x"));
	assert!(automatic.contains("middle: quantitative.get(2).cloned()"));
	assert!(automatic.contains("lower: required(0, \"lower\")?"));
	assert!(automatic.contains("upper: required(1, \"upper\")?"));

	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune relation transport");
	let spanned = envelope
		.split("pub(crate) enum SpannedMarkFields")
		.nth(1)
		.expect("SpannedMarkFields declaration")
		.split("impl SpannedMarkFields")
		.next()
		.expect("bounded SpannedMarkFields declaration");
	assert!(spanned.contains("FillBetween {"));
}

#[test]
fn product_0146_fill_between_surface_has_one_transport_and_no_colour_or_layer_widening() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let registry = presentation_registry(&root);
	assert_eq!(
		registry.match_indices("\"fill_between\"").count(),
		1,
		"fill_between has one callable spelling"
	);
	let compiler = std::fs::read_to_string(root.join("rune_view_compiler.rs"))
		.expect("read Rune view compiler");
	assert!(compiler.contains("\"fill_between\" => Some(&[3, 4])"));
	assert!(compiler.contains("| \"fill_between\""));
	assert!(compiler.contains("HandlerId::FillBetween"));

	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read Rune presentation evaluator");
	assert!(presentation.contains("HandlerId::FillBetween => raw_fill_between"));
	assert!(presentation.contains("PlotType::FillBetween => \"fill_between\""));
	let diverging = presentation
		.split("fn mark_diverging")
		.nth(1)
		.expect("mark_diverging function")
		.split("fn mark_category")
		.next()
		.expect("bounded mark_diverging function");
	assert!(!diverging.contains("PlotType::FillBetween"));

	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune relation envelope");
	assert_eq!(
		envelope
			.matches("PlotType::FillBetween => \"plot.fill_between\"")
			.count(),
		2
	);
	assert!(envelope.contains("| PlotType::FillBetween"));
	assert!(!envelope.contains("LayerMark::FillBetween"));

	let gui = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let diagnostic = gui
		.split("fn fill_between_failure_span")
		.nth(1)
		.expect("fill-between diagnostic mapper")
		.split("fn bubble_failure_span")
		.next()
		.expect("bounded fill-between diagnostic mapper");
	assert!(diagnostic.contains("prepare_fill_between_data"));
	assert!(diagnostic.contains("error.role()"));
	assert!(!diagnostic.contains("error.field =="));
	assert!(!diagnostic.contains("artifact_token"));
	assert!(!diagnostic.contains("ArtifactToken::mint"));

	let settings = registry
		.split("const ALL_MARKS")
		.nth(1)
		.expect("ALL_MARKS")
		.split("const NO_DEPENDENCIES")
		.next()
		.expect("bounded applicability constants");
	assert_eq!(settings.matches("PlotType::FillBetween").count(), 1);
	assert!(settings.contains("const STROKE_MARKS"));
	assert!(
		!settings
			.split("const STROKE_MARKS")
			.nth(1)
			.expect("STROKE_MARKS")
			.split("const BAR_MARKS")
			.next()
			.expect("bounded STROKE_MARKS")
			.contains("PlotType::FillBetween")
	);
}

#[test]
fn product_0147_candlestick_substrate_has_one_validated_geometric_colour_authority() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let candle = std::fs::read_to_string(root.join("plot/kernels/candlestick.rs"))
		.expect("read candlestick kernel");
	let production = candle
		.split("#[cfg(test)]")
		.next()
		.expect("candlestick production");
	assert!(production.contains("Result<CandlestickPreparedData, CandlestickPreparationError>"));
	assert!(production.contains("OrderedX::Temporal"));
	assert!(production.contains("partition_point"));
	assert!(production.contains("next_down()"));
	assert!(production.contains("next_up()"));
	assert!(production.contains("transform.bounds.width * BODY_WIDTH_FRACTION"));
	assert!(production.contains("settings.mark_style.fill.iced()"));
	assert!(production.contains("settings.mark_style.decoration.iced()"));
	assert!(production.contains("Direction::Unchanged"));
	assert!(!production.contains("get_color"));
	assert!(!production.contains("category(0, \"x\")"));

	let request = std::fs::read_to_string(root.join("plot/request.rs")).expect("read request");
	let validation = request
		.split("ResolvedKernelFields::Candlestick")
		.nth(1)
		.expect("candlestick planning branch")
		.split("ResolvedKernelFields::Histogram")
		.next()
		.expect("bounded candlestick planning branch");
	assert!(validation.contains("prepare_candlestick_data"));

	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read factory");
	let branch = factory
		.split("ResolvedKernelFields::Candlestick")
		.nth(1)
		.expect("candlestick factory branch")
		.split("ResolvedKernelFields::Funnel")
		.next()
		.expect("bounded candlestick factory branch");
	assert!(branch.contains("PRODUCT-0147 planning validated candlestick preparation"));

	let oracle = std::fs::read_to_string(root.join("plot/mark_style_oracles.rs"))
		.expect("read mark-style oracle");
	let candle_bank = oracle
		.split("let candlestick =")
		.nth(1)
		.expect("candlestick operation bank")
		.split("let funnel_frame")
		.next()
		.expect("bounded candlestick operation bank");
	assert!(candle_bank.contains("mark_style.decoration"));
	assert!(candle_bank.contains("mark_style.fill"));
	assert!(!candle_bank.contains("color_theme.get_color"));
}

#[test]
fn product_0147_authority_survives_product_0148_precise_surface_inversions() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let automatic = spec
		.split("PlotType::Candlestick => {")
		.nth(1)
		.expect("automatic candlestick arm")
		.split("PlotType::Funnel")
		.next()
		.expect("bounded automatic candlestick arm");
	assert!(automatic.contains("let x = number(0, \"x\")?"));
	assert!(automatic.contains("name.as_str() != x"));
	assert!(automatic.contains("open: required(0, \"open\")?"));
	assert!(automatic.contains("close: required(3, \"close\")?"));
	assert!(!automatic.contains("category(0, \"x\")"));

	let registry = presentation_registry(&root);
	assert!(registry_callable(&registry, "candlestick", "Free", "Candlestick").is_some());
	let mark_fields = spec
		.split("pub enum MarkFields")
		.nth(1)
		.expect("MarkFields declaration")
		.split("pub struct AuthoredPlotSpec")
		.next()
		.expect("bounded MarkFields");
	assert!(mark_fields.contains("Candlestick"));
	assert!(spec.contains("pub fn candlestick"));

	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune relation envelope");
	let spanned = envelope
		.split("pub(crate) enum SpannedMarkFields")
		.nth(1)
		.expect("SpannedMarkFields declaration")
		.split("impl SpannedMarkFields")
		.next()
		.expect("bounded SpannedMarkFields");
	assert!(spanned.contains("Candlestick"));
}

#[test]
fn product_0148_candlestick_surface_has_one_transport_and_no_colour_or_layer_widening() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let registry = presentation_registry(&root);
	let presentation = std::fs::read_to_string(root.join("rune_presentation.rs"))
		.expect("read Rune presentation evaluator");
	let compiler = std::fs::read_to_string(root.join("rune_view_compiler.rs"))
		.expect("read Rune view compiler");
	let envelope = std::fs::read_to_string(root.join("rune_relation_envelope.rs"))
		.expect("read Rune relation envelope");
	let spec = std::fs::read_to_string(root.join("plot/spec/mod.rs")).expect("read plot spec");
	let gui = std::fs::read_to_string(root.join("gui/state.rs")).expect("read GUI state");
	let kernel = std::fs::read_to_string(root.join("plot/kernels/candlestick.rs"))
		.expect("read candlestick substrate");

	let callable = registry_callable(&registry, "candlestick", "Free", "Candlestick")
		.expect("candlestick registry row");
	assert!(callable.contains("5..=5"));
	assert_eq!(registry.matches("\"candlestick\",").count(), 1);
	assert!(registry.contains("PlotType::Candlestick"));
	let all_marks = registry
		.split("const ALL_MARKS")
		.nth(1)
		.and_then(|rest| rest.split("const POINT_MARKS").next())
		.expect("ALL_MARKS list");
	assert!(all_marks.contains("PlotType::Candlestick"));
	for list in ["const STROKE_MARKS", "const BAR_MARKS", "const STACK_MARKS"] {
		let body = registry
			.split(list)
			.nth(1)
			.and_then(|rest| rest.split(";").next())
			.expect("settings membership list");
		assert!(!body.contains("PlotType::Candlestick"), "{list}");
	}
	assert!(presentation.contains("HandlerId::Candlestick => raw_candlestick"));
	assert!(presentation.contains("PlotType::Candlestick => \"candlestick\""));
	assert!(compiler.contains("\"candlestick\" => Some(&[5])"));
	assert!(compiler.contains("| HandlerId::Candlestick"));
	assert!(envelope.contains("PlotType::Candlestick => \"plot.candlestick\".to_owned()"));
	assert_eq!(
		envelope
			.matches("PlotType::Candlestick => \"plot.candlestick\".to_owned()")
			.count(),
		2
	);
	let layer = envelope
		.split("pub(crate) fn into_layer")
		.nth(1)
		.and_then(|rest| rest.split("impl RunePlotIntent").next())
		.expect("into_layer boundary");
	assert!(layer.contains("| PlotType::Candlestick"));
	assert!(spec.contains("fn resolve_candlestick_channels("));
	assert!(spec.contains("color_job: None"));
	assert!(gui.contains("fn candlestick_failure_span("));
	let mapper = gui
		.split("fn candlestick_failure_span(")
		.nth(1)
		.and_then(|rest| rest.split("fn bubble_failure_span(").next())
		.expect("bounded candlestick diagnostic mapper");
	assert!(mapper.contains("prepare_candlestick_data("));
	assert!(!mapper.contains("ArtifactToken::mint"));
	assert!(!mapper.contains("error.field =="));
	let production = kernel
		.split("#[cfg(test)]")
		.next()
		.expect("candlestick production");
	assert!(!production.contains("get_color"));
}

#[test]
fn product_0149_interactive_rasters_request_in_update_and_only_draw_published_pixels() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let scatter =
		std::fs::read_to_string(root.join("plot/kernels/scatter.rs")).expect("read scatter kernel");
	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read plot grid");

	let update = common
		.split("fn update(")
		.nth(1)
		.and_then(|rest| rest.split("fn mouse_interaction(").next())
		.expect("bounded canvas update");
	let redraw = update.find("RedrawRequested").expect("redraw request seam");
	let data_return = update
		.find("if self.layer == PlotRenderLayer::Data")
		.expect("data-layer return");
	assert!(
		redraw < data_return,
		"raster wants publish before the data canvas returns"
	);
	assert!(update.contains("PlotMessage::RasterRequested(wants)"));
	let dedup = update
		.find("!coordinator.is_current(&wants)")
		.expect("pre-publication coordinator deduplication");
	let publish = update
		.find("PlotMessage::RasterRequested(wants)")
		.expect("raster request publication");
	assert!(
		dedup < publish,
		"deduplication must precede UI message publication"
	);
	assert!(common.contains("with_raster_coordinator"));

	let renderer = scatter
		.split("pub struct ScatterMarkRenderer")
		.nth(1)
		.and_then(|rest| rest.split("enum ResolvedScatterRenderMode").next())
		.expect("scatter renderer fields");
	assert!(!renderer.contains("raster_cache"));
	let interactive = scatter
		.split("pub(crate) fn draw_interactive(")
		.nth(1)
		.and_then(|rest| rest.split("impl MarkRenderer").next())
		.expect("interactive scatter draw");
	assert!(!interactive.contains("build_raster_rgba"));
	assert!(interactive.contains("coordinator.bitmap(slot)"));
	let grid_interactive = grid
		.split("fn render_plot_layer_interactive(")
		.nth(1)
		.and_then(|rest| rest.split("fn render_overlay_layer(").next())
		.expect("interactive grid render");
	assert!(grid_interactive.contains("cell.kernel.plot_interactive("));
	assert!(!grid_interactive.contains("plot_with_settings("));
}

#[test]
fn product_0149_coordinators_own_identity_admission_and_exact_export_boundaries() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let raster = std::fs::read_to_string(root.join("plot/raster.rs")).expect("read raster core");
	let scatter =
		std::fs::read_to_string(root.join("plot/kernels/scatter.rs")).expect("read scatter kernel");
	let host =
		std::fs::read_to_string(root.join("gui/plot_pane/state.rs")).expect("read host plot state");
	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read editor");
	let gui = std::fs::read_to_string(root.join("gui/state.rs")).expect("read gui state");
	let factory = std::fs::read_to_string(root.join("plot/factory.rs")).expect("read plot factory");

	assert!(host.contains("pub raster_coordinator: crate::plot::raster::RasterCoordinator"));
	let notebook = editor
		.split("struct NotebookPlotSurface")
		.nth(1)
		.and_then(|rest| rest.split("impl NotebookPlotSurface").next())
		.expect("notebook plot state");
	assert!(notebook.contains("raster_coordinator: crate::plot::raster::RasterCoordinator"));
	assert!(raster.contains("current_slot_request: Arc<AtomicU64>"));
	assert!(raster.contains("pub fn is_current(&self, wants: &[ScatterRasterWant]) -> bool"));
	assert!(scatter.contains("current.load(Ordering::Acquire) != request"));
	assert!(raster.contains("(parallelism.get() / 2).max(1)"));
	assert!(raster.contains("unwrap_or(1)"));
	assert!(!raster.contains("JoinHandle::abort"));
	assert!(factory.contains("prepare_scatter_data_resolved_with_subject"));
	assert!(factory.contains("source.artifact_token()"));

	let export = gui
		.split("Message::Export(format) =>")
		.nth(1)
		.and_then(|rest| rest.split("Message::ExportDone").next())
		.expect("export update arm");
	let blocking = export
		.find("spawn_blocking")
		.expect("blocking export context");
	let rendering = export
		.find("render_plot_export(snapshot")
		.expect("export rendering call");
	assert!(
		blocking < rendering,
		"rendering itself belongs inside the blocking task"
	);
}

#[test]
fn product_0150_viewport_identity_motion_and_composite_policy_are_production_wired() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read plot grid");
	let host =
		std::fs::read_to_string(root.join("gui/plot_pane/state.rs")).expect("read host state");
	let view = std::fs::read_to_string(root.join("gui/plot_pane/view.rs")).expect("read host view");
	let editor = std::fs::read_to_string(root.join("editor.rs")).expect("read editor");

	assert!(common.contains("ViewportMotion::Moving"));
	assert!(common.contains("let started = chain_stamp.next()"));
	assert!(common.contains("PlotMessage::ViewportStarted(started)"));
	assert!(common.contains("PlotMessage::ViewportChanged"));
	assert!(common.contains("PlotMessage::ViewportSettled(drag.stamp)"));
	assert!(common.contains("viewport_local_stamp: Option<ViewportStamp>"));
	assert!(common.contains("let chain_stamp = state.sync_viewport_chain(stamp)"));
	assert!(common.contains("let next = drag.stamp.next()"));
	assert!(common.contains("if self.viewport_motion == ViewportMotion::Moving"));
	assert!(common.contains("const VIEWPORT_ZOOM_FACTOR: f64 = 1.1"));
	assert!(common.contains("const VIEWPORT_RELATIVE_FLOOR: f64 = 1.0 / 1_073_741_824.0"));
	assert!(common.contains("const VIEWPORT_ABSOLUTE_ULPS: f64 = 1024.0"));
	assert!(host.contains("pub viewport_stamp: ViewportStamp"));
	assert!(host.contains("if stamp == self.viewport_stamp"));
	assert!(view.matches("with_viewport_state").count() >= 2);
	assert!(editor.contains(
		"const VIEWPORT_QUIET_SETTLE: std::time::Duration = std::time::Duration::from_millis(150)"
	));
	assert_eq!(editor.matches("sleep(VIEWPORT_QUIET_SETTLE)").count(), 2);
	assert!(editor.contains("settings_model: crate::plot::theme::PlotSettings"));
	assert!(editor.contains("self.send(EditorCommand::FocusPane(pane_id))"));

	let interactive = grid
		.split("fn render_plot_layer_interactive(")
		.nth(1)
		.and_then(|rest| rest.split("fn render_overlay_layer(").next())
		.expect("interactive composite branch");
	assert!(interactive.contains("cell.kernel.prepare_viewport(&transform, motion)"));
	assert!(interactive.contains("RasterReusePolicy::Refused"));
	let overlay = grid
		.split("fn render_overlay_layer(")
		.nth(1)
		.and_then(|rest| rest.split("fn hit(").next())
		.expect("composite overlay branch");
	assert!(overlay.contains("cell.kernel.prepare_viewport(&transform, motion)"));
}

#[test]
fn product_0150_gestures_only_author_the_pane_tier_and_thresholds_remain_unpriced() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read plot common");
	let host =
		std::fs::read_to_string(root.join("gui/plot_pane/state.rs")).expect("read host state");
	let scatter =
		std::fs::read_to_string(root.join("plot/kernels/scatter.rs")).expect("read scatter");
	let view = std::fs::read_to_string(root.join("gui/plot_pane/view.rs")).expect("read plot view");
	let gui = std::fs::read_to_string(root.join("gui/state.rs")).expect("read gui state");

	let gesture = common
		.split("if let Some(stamp) = self.viewport_stamp")
		.nth(1)
		.and_then(|rest| rest.split("match event {").nth(1))
		.and_then(|rest| rest.split("match event {").next())
		.expect("viewport gesture update region");
	assert!(!gesture.contains("PlotWritebackGesture"));
	assert!(!gesture.contains("plan_writeback"));
	assert!(host.contains("plot_settings_model.pane_override.domains = Override::Set(domains)"));
	assert!(host.contains("plot_settings_model.pane_override.domains = Override::Inherit"));
	assert!(view.contains("ProvenanceTier::PaneOverride"));
	assert!(view.contains("SettingKey::XAxisBounds"));
	assert!(view.contains("SettingKey::YAxisBounds"));
	assert!(gui.contains("validated_displayed_pane_value(&context.settings, key, &displayed)"));
	assert!(gui.contains("ProvenanceTier::PaneOverride"));
	assert!(common.contains("scatter_max_vector_points: 50_000"));
	assert!(common.contains("scatter_downsample_target: 200_000"));
	assert!(scatter.contains("pixel_budget.saturating_mul(8)"));
	assert!(scatter.contains("fn resolve_render_mode("));
}

#[test]
fn product_0151_click_selection_admission_is_a_closed_kernel_table() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src/plot/kernels");
	let mut admitted = std::fs::read_dir(&root)
		.expect("read kernel directory")
		.filter_map(Result::ok)
		.filter(|entry| entry.path().extension().is_some_and(|ext| ext == "rs"))
		.filter_map(|entry| {
			let source = std::fs::read_to_string(entry.path()).ok()?;
			source
				.contains("fn project_predicate(")
				.then(|| entry.file_name().to_string_lossy().into_owned())
		})
		.collect::<Vec<_>>();
	admitted.sort();
	assert_eq!(
		admitted,
		[
			"bar.rs",
			"boxplot.rs",
			"candlestick.rs",
			"fill_between.rs",
			"heatmap.rs",
			"histogram.rs",
			"layered.rs",
			"scatter.rs",
			"violin.rs",
		],
		"a new click-selecting kernel is an ADR 0151 admission-table amendment"
	);
	for refused in [
		"line.rs",
		"hexbin.rs",
		"pie.rs",
		"funnel.rs",
		"radar.rs",
		"parallel.rs",
		"radial_dial.rs",
		"stacked_area.rs",
	] {
		let source = std::fs::read_to_string(root.join(refused)).expect("read refused kernel");
		assert!(!source.contains("fn project_predicate("), "{refused}");
	}
}

#[test]
fn product_0151_identity_grid_and_in_process_boundaries_are_pinned() {
	let root = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let selection = std::fs::read_to_string(root.join("selection.rs")).expect("read selection");
	let grid = std::fs::read_to_string(root.join("plot/grid.rs")).expect("read grid");
	let common = std::fs::read_to_string(root.join("plot/common.rs")).expect("read common");
	let compiler =
		std::fs::read_to_string(root.join("rune_view_compiler.rs")).expect("read compiler");

	assert!(
		!selection.contains("adapter"),
		"selection never enters an engine adapter"
	);
	assert!(
		!grid.contains("ArtifactToken::mint"),
		"facets inherit their artifact token"
	);
	assert!(grid.contains("from_grid_with_figure_grid_from_artifact"));
	assert!(common.contains("pixel_to_temporal"));
	assert!(compiler.contains("RelationLineage"));
	assert!(compiler.contains("relation_lineage"));
	let composite_draw = grid
		.split("fn render_projected_emphasis(")
		.nth(1)
		.and_then(|rest| rest.split("fn render_plot_layer(").next())
		.expect("composite emphasis draw region");
	assert!(
		!composite_draw.contains("project_predicate("),
		"facet redraws only reproject anchors retained at selection time"
	);
	let selection_gesture = common
		.split("if self.selection_axis_fields.is_some()")
		.nth(1)
		.and_then(|rest| {
			rest.split("if let Some(stamp) = self.viewport_stamp")
				.next()
		})
		.expect("selection gesture region");
	assert!(!selection_gesture.contains("PlotWritebackGesture"));
	assert!(!selection_gesture.contains("plan_writeback"));
}

/// PRODUCT-0152 §2: the plot kit and the presentation surface consume the
/// temporal interval literal from `boson`, never from the Toron client, so the
/// reduced head compiles. The guard is mechanical: it never infers `cfg` scope
/// from text. The four sources below name the client for nothing else; the
/// state file keeps its gated Toron operations and loses only the literal.
#[test]
fn product_0152_plot_and_presentation_sources_never_name_the_toron_client() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let mut banned = rust_files(&src.join("plot"));
	for file in [
		"rune_presentation.rs",
		"presentation_registry.rs",
		"settings_writeback.rs",
	] {
		banned.push(src.join(file));
	}
	for path in banned {
		let source = std::fs::read_to_string(&path).expect("read Polariton source");
		assert!(
			!source.contains("toron_client"),
			"PRODUCT-0152: {} names the Toron client; the temporal literal lives in boson",
			path.display()
		);
	}
	let state = std::fs::read_to_string(src.join("gui/state.rs")).expect("read Polariton state");
	assert!(
		!state.contains("toron_client::TemporalInterval"),
		"PRODUCT-0152: gui/state.rs imports the temporal literal from the Toron client"
	);
	assert!(
		state.contains("use boson::TemporalInterval;"),
		"positive control: gui/state.rs still consumes the literal, so the guard is exercised"
	);
}

/// PRODUCT-0154 §2: the package DTOs never acquire placement, token, or
/// renderer authority, and the runtime types they convert from never learn
/// to serialize themselves.
#[test]
fn product_0154_package_dtos_stay_free_of_placement_token_and_renderer_authority() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let publication =
		std::fs::read_to_string(src.join("publication.rs")).expect("read publication");
	let dtos = publication
		.split("// Manifest and figure DTOs")
		.nth(1)
		.expect("DTO region start")
		.split("// Canonical encodings")
		.next()
		.expect("DTO region end");
	for forbidden in [
		"BufferId",
		"HostPanelId",
		"PaneRect",
		"ArtifactToken",
		"PlotType",
		"PlotKernel",
		"iced",
		"tab_index",
		"placement",
	] {
		assert!(
			!dtos.contains(forbidden),
			"PRODUCT-0154 DTO acquired runtime authority through {forbidden}"
		);
	}
	for runtime in [
		"plot/spec/mod.rs",
		"plot/recipe.rs",
		"plot/theme.rs",
		"plot/common.rs",
		"editor.rs",
		"selection.rs",
	] {
		let source = std::fs::read_to_string(src.join(runtime)).expect("read runtime source");
		assert!(
			!source.contains("Serialize"),
			"PRODUCT-0154: {runtime} must not serialize itself; the package owns its DTOs"
		);
	}
}

/// PRODUCT-0154 §9: the shell is a document. The emitter contains no script
/// element, so no package can either.
#[test]
fn product_0154_shell_emitter_inlines_one_pinned_script() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let publication =
		std::fs::read_to_string(src.join("publication.rs")).expect("read publication");
	let shell = publication
		.split("fn shell_html(")
		.nth(1)
		.expect("shell emitter")
		.split("\n}\n")
		.next()
		.expect("shell emitter body");
	// PRODUCT-0155 §6: the emitter inlines exactly one executable script, the
	// pinned asset, and names its hash in the policy; void elements close.
	assert_eq!(
		shell.matches("<script>").count(),
		1,
		"one executable script element"
	);
	assert!(shell.contains("html.push_str(INSPECT_JS);"));
	assert!(shell.contains("script-src 'sha256-"));
	assert!(
		shell.contains("charset=\\\"utf-8\\\"/>"),
		"void elements are XML-closed"
	);
	assert!(
		shell.contains("@font-face"),
		"positive control: the emitter is the one read"
	);
	// PRODUCT-0157 §5: exactly the two hiding rules, both by class alone.
	assert_eq!(shell.matches("polariton-hidden").count(), 2);
	assert!(shell.contains("g[data-series].polariton-hidden { display: none; }"));
	assert!(shell.contains("g[data-legend].polariton-hidden { opacity: 0.25; }"));
}

/// PRODUCT-0157 §2 and gate 1: the series table is a handle. Its DTOs name no
/// exact scalar, scalar type, zone, scale, or selection candidate, and the
/// assay fires on a specimen row that does.
#[test]
fn product_0157_series_table_dtos_carry_no_exact_identity_with_a_positive_control() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let publication =
		std::fs::read_to_string(src.join("publication.rs")).expect("read publication");
	let region = publication
		.split("// PRODUCT-0157 §2: the series table.")
		.nth(1)
		.expect("series DTO region start")
		.split("// PRODUCT-0157 §2: end of the series table.")
		.next()
		.expect("series DTO region end");
	// Code only: the prose that names what must not cross is not a crossing.
	let code: String = region
		.lines()
		.filter(|line| !line.trim_start().starts_with("//"))
		.collect::<Vec<_>>()
		.join("\n");
	let dtos = code.as_str();
	fn violations(source: &str) -> Vec<&'static str> {
		let mut found = Vec::new();
		for (needle, name) in [
			("ExactScalar", "ExactScalar"),
			("ScalarType", "ScalarType"),
			("scalar_type", "scalar_type"),
			("zone", "zone"),
			("scale:", "scale"),
			("SelectionCandidate", "SelectionCandidate"),
			("candidate", "candidate"),
			("FieldRef", "FieldRef"),
		] {
			if source.contains(needle) {
				found.push(name);
			}
		}
		found
	}
	assert!(violations(dtos).is_empty(), "{:?}", violations(dtos));
	assert!(
		dtos.contains("pub(crate) struct SeriesRow"),
		"the region is the one read"
	);
	let specimen = "pub(crate) struct SeriesRow { value: ExactScalar, scalar_type: String, zone: Option<String>, candidate: u32 }";
	assert_eq!(
		violations(specimen),
		["ExactScalar", "scalar_type", "zone", "candidate"],
		"positive control: the exact-identity assay must fire"
	);
}

/// PRODUCT-0153 gate 3 and PRODUCT-0154 §9: no boundary that renders or views
/// Polariton figures may depend on a browser plot authority, and the assay
/// visibly fires on a specimen.
#[test]
fn product_0154_browser_plot_authority_is_refused_with_a_positive_control() {
	let specimen = r#"
[dependencies]
vega-lite = "5"
charting = { package = "altair", version = "1" }
serde = "1"
"#;
	let violations =
		manifest_dependency_guard::forbidden_dependencies(specimen, "", ProductBoundary::Polariton);
	assert_eq!(
		violations,
		vec![
			"charting (package = altair)".to_owned(),
			"vega-lite".to_owned()
		],
		"positive control: the browser plot-authority assay must fire"
	);
	let clean = r#"
[dependencies]
serde = "1"
"#;
	assert!(
		manifest_dependency_guard::forbidden_dependencies(clean, "", ProductBoundary::Polariton)
			.is_empty()
	);
}

/// PRODUCT-0155 §6: the inspection asset is lookup glue. It names no DOM
/// markup sink, no network or evaluation primitive, and — because it is
/// inlined into a polyglot shell — no literal `<` or `&`. The specimen proves
/// the assay fires.
#[test]
fn product_0155_inspection_asset_is_glue_with_a_positive_control() {
	let asset = std::fs::read_to_string(
		Path::new(env!("CARGO_MANIFEST_DIR")).join("assets/web/inspect.js"),
	)
	.expect("read inspect.js");
	fn violations(source: &str) -> Vec<&'static str> {
		let mut found = Vec::new();
		for (needle, name) in [
			("innerHTML", "innerHTML"),
			("outerHTML", "outerHTML"),
			("insertAdjacentHTML", "insertAdjacentHTML"),
			("document.write", "document.write"),
			("eval(", "eval"),
			("Function(", "Function"),
			("fetch(", "fetch"),
			("XMLHttpRequest", "XMLHttpRequest"),
			("WebSocket", "WebSocket"),
			("import(", "dynamic import"),
			// PRODUCT-0157 §5: the one permitted write is a class toggle on an
			// ordinal-addressed group; no other attribute write, no geometry read.
			("setAttribute", "setAttribute"),
			("getAttribute", "getAttribute"),
			("getBBox", "getBBox"),
			("getCTM", "getCTM"),
			("getTotalLength", "getTotalLength"),
			("getPointAtLength", "getPointAtLength"),
			("pathSegList", "path data"),
			(".style.d", "path data"),
			("<", "literal less-than"),
			("&", "literal ampersand"),
		] {
			if source.contains(needle) {
				found.push(name);
			}
		}
		found
	}
	assert!(violations(&asset).is_empty(), "{:?}", violations(&asset));
	assert!(asset.contains("textContent"), "prose is set as text");
	assert!(
		asset.contains("Math.fround"),
		"distances are the desktop's 32-bit arithmetic"
	);
	let specimen = "var t = a < b; el.innerHTML = fetch(url); g.setAttribute(\"d\", p.getBBox());";
	assert_eq!(
		violations(specimen),
		[
			"innerHTML",
			"fetch",
			"setAttribute",
			"getBBox",
			"literal less-than"
		],
		"positive control: the glue assay must fire"
	);
}

/// PRODUCT-0155 gate 1: the test-suite JavaScript engine is exactly the one
/// the record names, default features off, a dev-dependency only, and no
/// other engine is admitted.
#[test]
fn product_0155_javascript_engine_is_boa_dev_only_without_default_features() {
	let manifest =
		std::fs::read_to_string(Path::new(env!("CARGO_MANIFEST_DIR")).join("Cargo.toml"))
			.expect("read Polariton manifest");
	let (dependencies, dev) = manifest
		.split_once("[dev-dependencies]")
		.expect("dev-dependencies section");
	assert!(
		!dependencies.contains("boa_engine"),
		"the engine never ships: dev-dependency only"
	);
	assert!(
		dev.contains("boa_engine = { version = \"0.22.0\", default-features = false }"),
		"the engine is boa_engine 0.22.0 with default features off"
	);
	for other in [
		"rquickjs",
		"deno_core",
		"v8",
		"quickjs",
		"javascriptcore",
		"spidermonkey",
	] {
		assert!(!manifest.contains(other), "no second engine: {other}");
	}
}

/// PRODUCT-0159 gate 6: the record moves nothing about the raster gate or the
/// coupled downsample pair, and the mark batch is scatter's alone.
#[test]
fn product_0159_gate_constants_and_render_mode_are_retained_and_the_batch_is_scatters() {
	let src = Path::new(env!("CARGO_MANIFEST_DIR")).join("src");
	let common = std::fs::read_to_string(src.join("plot/common.rs")).expect("read common");
	for retained in [
		"scatter_max_vector_points: 50_000,",
		"scatter_downsample_target: 200_000,",
		"scatter_raster_threshold: 1_000_000,",
	] {
		assert!(
			common.contains(retained),
			"PRODUCT-0140 §2 retained {retained}"
		);
	}
	let scatter =
		std::fs::read_to_string(src.join("plot/kernels/scatter.rs")).expect("read scatter");
	let gate = scatter
		.split("fn resolve_render_mode(")
		.nth(1)
		.expect("resolve_render_mode")
		.split("\n}\n")
		.next()
		.expect("gate body");
	for retained in [
		"ScatterRenderMode::Vector => ResolvedScatterRenderMode::Vector,",
		"if total_points <= settings.scatter_max_vector_points as usize {",
		"let raster_threshold = settings.scatter_raster_threshold as usize;",
		"&& total_points > pixel_budget.saturating_mul(8)",
	] {
		assert!(
			gate.contains(retained),
			"PRODUCT-0140 §2 retained `{retained}`"
		);
	}
	let kernels = src.join("plot/kernels");
	let mut callers = Vec::new();
	for entry in std::fs::read_dir(&kernels).expect("kernels") {
		let path = entry.expect("entry").path();
		if path.extension().is_some_and(|ext| ext == "rs") {
			let source = std::fs::read_to_string(&path).expect("read kernel");
			if source.contains(".fill_marks(") {
				callers.push(path.file_name().unwrap().to_string_lossy().into_owned());
			}
		}
	}
	assert_eq!(
		callers,
		["scatter.rs"],
		"the batch is scatter's alone in this cut"
	);
	assert!(
		!scatter.contains("for point in &sampled {\n\t\t\t\t\t\t\t\tbuilder.circle("),
		"no compound circle path remains in the downsampled arm"
	);
}
