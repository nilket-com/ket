#[path = "../../../tests/support/manifest_dependency_guard.rs"]
mod manifest_dependency_guard;

use manifest_dependency_guard::{ProductBoundary, assert_product_dependency_boundary};
use std::path::Path;

#[test]
fn ket_0006_keeps_boson_on_the_interchange_boundary() {
	assert_product_dependency_boundary(
		Path::new(env!("CARGO_MANIFEST_DIR")),
		ProductBoundary::Boson,
	);
}
