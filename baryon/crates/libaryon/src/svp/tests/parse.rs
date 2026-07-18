use crate::core::DocByte;
use crate::svp::rust_parse::*;
use ra_ap_syntax::{AstNode, SourceFile, TextRange, TextSize};

#[test]
fn test_local_to_global_projection() {
	let text = "fn test() {}";
	let parse = SourceFile::parse(text, ra_ap_syntax::Edition::Edition2024);

	let tree = ViewportTree {
		offset_map: vec![(0, DocByte::new(1_000_000_000))],
		fragment_start: 500, // 500 bytes into the local chunk
		tree: parse.tree().syntax().clone(),
	};

	// TextRange from index 10 to 20
	let local_range = TextRange::new(TextSize::from(10), TextSize::from(20));
	let (global_start, global_end) = tree.local_to_global(local_range);

	assert_eq!(global_start, DocByte::new(1_000_000_510));
	assert_eq!(global_end, DocByte::new(1_000_000_520));
}
