#[test]
fn product_0081_tukey_boxes_exclude_unvalidated_mutation() {
	let cases = trybuild::TestCases::new();
	cases.pass("tests/ui/boxplot_tukey_construct.rs");
	cases.compile_fail("tests/ui/boxplot_tukey_private.rs");
}
