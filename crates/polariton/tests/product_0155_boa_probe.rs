//! PRODUCT-0155 gate 1's engine, probed: the shipped resolver must run under
//! `boa_engine` with default features off, in both heads, with no DOM.

use boa_engine::{Context, Source};

#[test]
fn product_0155_boa_runs_plain_ecmascript_without_default_features() {
	let mut context = Context::default();
	let value = context
		.eval(Source::from_bytes(
			"const r = { x: 1.5, y: 2.5, w: 3, h: 4 }; const p = { x: 2, y: 3 };\n\
			 (p.x >= r.x && p.x < r.x + r.w && p.y >= r.y && p.y < r.y + r.h) ? 'inside' : 'outside'",
		))
		.expect("evaluates");
	assert_eq!(
		value
			.to_string(&mut context)
			.expect("string")
			.to_std_string_escaped(),
		"inside"
	);
}
