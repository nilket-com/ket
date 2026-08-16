//! Declared text-extent budgets for layout reservations.

/// Conservative advance for labels produced by `format_label`.
///
/// Its supported alphabet is digits, sign, decimal point, scientific `e`,
/// percent, and the `-`, `:`, space, and `T` separators emitted by temporal
/// formatting. It deliberately estimates rather than measures glyphs.
pub(crate) const FORMATTED_LABEL_ADVANCE_EM: f32 = 0.75;

/// Conservative starting advance for printable ASCII category labels.
///
/// Labels outside this alphabet remain renderable but may exceed their
/// reservation. PRODUCT-0114 names per-character-class budgeting as the next
/// refinement before backend measurement becomes warranted.
pub(crate) const ASCII_LABEL_ADVANCE_EM: f32 = 1.0;

pub(crate) fn estimate_formatted_label_width(text: &str, font_size: f32) -> f32 {
	text.chars().count() as f32 * font_size * FORMATTED_LABEL_ADVANCE_EM
}

pub(crate) fn estimate_ascii_label_width(text: &str, font_size: f32) -> f32 {
	text.chars().count() as f32 * font_size * ASCII_LABEL_ADVANCE_EM
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0114_declared_budgets_are_alphabet_scoped_not_universal() {
		assert_eq!(estimate_formatted_label_width("1.0e6%", 20.0), 90.0);
		assert_eq!(estimate_ascii_label_width("WWW", 20.0), 60.0);

		// A category value is data, so it remains renderable outside printable
		// ASCII. It deliberately receives no hidden wider reservation: fixed
		// advances cannot promise that a CJK glyph, emoji, or combining sequence
		// fits without a declared per-class refinement or real measurement.
		assert_eq!(
			estimate_ascii_label_width("雪", 20.0),
			estimate_ascii_label_width("W", 20.0)
		);
	}
}
