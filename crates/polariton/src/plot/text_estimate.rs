//! Declared text-extent budgets for layout reservations.

/// Conservative advance for labels produced by `format_label`.
///
/// Its supported alphabet is digits, sign, decimal point, scientific `e`,
/// percent, and the `-`, `:`, space, and `T` separators emitted by temporal
/// formatting. It deliberately estimates rather than measures glyphs.
pub(crate) const FORMATTED_LABEL_ADVANCE_EM: f32 = 0.75;

/// Printable-ASCII class budgets for [`crate::fonts::PLOT_TEXT_FONT`].
///
/// PRODUCT-0117 censused every `U+0020..=U+007E` glyph using Iced's actual
/// ASCII shaping path at 18 px, then rounded each class upward to the next
/// 0.05 em. The classes are exhaustive over that partition. They are neither
/// average advances nor universal font metrics; PRODUCT-0114 named
/// per-character-class budgeting as the refinement before measurement.
const ASCII_TINY_ADVANCE_EM: f32 = 0.25;
const ASCII_NARROW_ADVANCE_EM: f32 = 0.30;
const ASCII_SLIM_ADVANCE_EM: f32 = 0.35;
const ASCII_COMPACT_ADVANCE_EM: f32 = 0.40;
const ASCII_SMALL_ADVANCE_EM: f32 = 0.45;
const ASCII_MEDIUM_NARROW_ADVANCE_EM: f32 = 0.50;
const ASCII_MEDIUM_ADVANCE_EM: f32 = 0.55;
const ASCII_STANDARD_ADVANCE_EM: f32 = 0.60;
const ASCII_BROAD_ADVANCE_EM: f32 = 0.65;
const ASCII_WIDE_ADVANCE_EM: f32 = 0.75;
const ASCII_EXTRA_WIDE_ADVANCE_EM: f32 = 0.80;
const ASCII_PERCENT_ADVANCE_EM: f32 = 0.85;
const ASCII_AT_ADVANCE_EM: f32 = 0.90;
const ASCII_WIDEST_ADVANCE_EM: f32 = 0.95;

/// Conservative fallback for text outside printable ASCII.
///
/// This deliberately preserves PRODUCT-0114's former one-em behaviour for
/// glyphs that may use host fallback. It is broad, static, and is never a
/// hidden route into a narrower ASCII class.
pub(crate) const OUT_OF_PARTITION_LABEL_ADVANCE_EM: f32 = 1.0;

pub(crate) fn estimate_formatted_label_width(text: &str, font_size: f32) -> f32 {
	text.chars().count() as f32 * font_size * FORMATTED_LABEL_ADVANCE_EM
}

pub(crate) fn estimate_ascii_label_width(text: &str, font_size: f32) -> f32 {
	text.chars()
		.map(|character| ascii_label_advance_em(character) * font_size)
		.sum()
}

/// Returns the static budget for one character.
///
/// The match is intentionally total at the public boundary: printable ASCII
/// gets one—and only one—censused class; every other scalar uses the declared
/// broad fallback.
pub(crate) fn ascii_label_advance_em(character: char) -> f32 {
	match character {
		'\'' => ASCII_TINY_ADVANCE_EM,
		' ' | '!' | '(' | ')' | ',' | '.' | ':' | ';' | 'J' | 'i' | 'j' | 'l'
		| '`' => ASCII_NARROW_ADVANCE_EM,
		'-' | '[' | ']' | 'I' | 'f' => ASCII_SLIM_ADVANCE_EM,
		'/' | '\\' | '{' | '}' | 't' => ASCII_COMPACT_ADVANCE_EM,
		'"' | '?' | '_' | 'r' => ASCII_SMALL_ADVANCE_EM,
		'c' | 's' | 'z' => ASCII_MEDIUM_NARROW_ADVANCE_EM,
		'F' | 'L' | 'S' | 'k' | 'v' | 'x' | 'y' => ASCII_MEDIUM_ADVANCE_EM,
		'$' | '+' | '0'..='9' | '<' | '=' | '>' | '^' | '~' | 'E' | 'T' | 'X'
		| 'Y' | 'Z' => ASCII_STANDARD_ADVANCE_EM,
		'*' | '|' | '#' | 'A' | 'B' | 'C' | 'K' | 'P' | 'R' | 'V' | 'a' | 'b' | 'd'
		| 'e' | 'g' | 'h' | 'n' | 'o' | 'p' | 'q' | 'u' => ASCII_BROAD_ADVANCE_EM,
		'&' | 'D' | 'G' | 'H' | 'U' => ASCII_WIDE_ADVANCE_EM,
		'N' | 'O' | 'Q' | 'w' => ASCII_EXTRA_WIDE_ADVANCE_EM,
		'%' => ASCII_PERCENT_ADVANCE_EM,
		'@' => ASCII_AT_ADVANCE_EM,
		'M' | 'W' | 'm' => ASCII_WIDEST_ADVANCE_EM,
		_ => OUT_OF_PARTITION_LABEL_ADVANCE_EM,
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::fonts::{PLOT_TEXT_FONT, POLARITON_PLOT_SANS_REGULAR};
	use iced::advanced::graphics::text::{Paragraph as IcedParagraph, font_system};
	use iced::advanced::text::{LineHeight, Paragraph as _, Shaping, Text, Wrapping};
	use iced::{Pixels, Size, alignment};
	use std::borrow::Cow;

	fn product_0117_measure(text: &str, size: f32) -> f32 {
		font_system()
			.write()
			.expect("global font system lock")
			.load_font(Cow::Borrowed(POLARITON_PLOT_SANS_REGULAR));
		IcedParagraph::with_text(Text {
			content: text,
			bounds: Size::new(f32::INFINITY, f32::INFINITY),
			size: Pixels(size),
			line_height: LineHeight::default(),
			font: PLOT_TEXT_FONT,
			align_x: alignment::Horizontal::Left.into(),
			align_y: alignment::Vertical::Top,
			// Match canvas::Text's default: Iced uses basic shaping for ASCII
			// and advances to fallback-capable shaping only outside it.
			shaping: Shaping::default(),
			wrapping: Wrapping::None,
		})
		.min_width()
	}

	#[test]
	fn product_0117_ascii_partition_is_exhaustive_and_censused_against_its_face() {
		for character in ' '..='~' {
			let budget = ascii_label_advance_em(character);
			let actual = product_0117_measure(&character.to_string(), 18.0) / 18.0;
			assert!(
				actual <= budget + 0.000_1,
				"PRODUCT-0117 {character:?} realized {actual:.4} em exceeds its {budget:.2} em class"
			);
			assert!(
				budget < OUT_OF_PARTITION_LABEL_ADVANCE_EM,
				"PRODUCT-0117 printable {character:?} fell through to the broad fallback"
			);
		}
	}

	#[test]
	fn product_0117_ascii_basic_shaping_does_not_escape_the_class_sum() {
		for left in ' '..='~' {
			for right in ' '..='~' {
				let text = format!("{left}{right}");
				let actual = product_0117_measure(&text, 18.0);
				let budget = estimate_ascii_label_width(&text, 18.0);
				assert!(
					actual <= budget + 0.001,
					"PRODUCT-0117 ASCII pair {text:?} realized {actual:.4}px exceeds {budget:.4}px"
				);
			}
		}
	}

	#[test]
	fn product_0117_partition_reduces_ascii_and_keeps_non_ascii_on_the_named_fallback() {
		assert_eq!(estimate_formatted_label_width("1.0e6%", 20.0), 90.0);
		assert_eq!(estimate_ascii_label_width("WWW", 20.0), 57.0);
		assert_eq!(estimate_ascii_label_width("iii", 20.0), 18.0);
		assert!(
			estimate_ascii_label_width("international partner enablement and certification", 20.0)
				< 900.0,
			"PRODUCT-0117 lower-case prose must no longer pay W's budget for every character"
		);

		for text in ["雪", "🧭"] {
			assert_eq!(
				estimate_ascii_label_width(text, 20.0),
				text.chars().count() as f32 * 20.0 * OUT_OF_PARTITION_LABEL_ADVANCE_EM,
				"PRODUCT-0117 {text:?} must take the named broad fallback"
			);
		}
		assert_eq!(
			estimate_ascii_label_width("e\u{301}", 20.0)
				- estimate_ascii_label_width("e", 20.0),
			20.0 * OUT_OF_PARTITION_LABEL_ADVANCE_EM,
			"PRODUCT-0117 a combining mark must take the broad fallback beside its ASCII base"
		);
	}

	#[test]
	fn product_0117_pinned_primary_face_preserves_cjk_fallback() {
		font_system()
			.write()
			.expect("global font system lock")
			.load_font(Cow::Borrowed(POLARITON_PLOT_SANS_REGULAR));
		let paragraph = IcedParagraph::with_text(Text {
			content: "雪",
			bounds: Size::new(f32::INFINITY, f32::INFINITY),
			size: Pixels(18.0),
			line_height: LineHeight::default(),
			font: PLOT_TEXT_FONT,
			align_x: alignment::Horizontal::Left.into(),
			align_y: alignment::Vertical::Top,
			shaping: Shaping::default(),
			wrapping: Wrapping::None,
		});
		let glyph = paragraph
			.buffer()
			.layout_runs()
			.next()
			.expect("PRODUCT-0117 CJK paragraph has a layout run")
			.glyphs
			.first()
			.expect("PRODUCT-0117 CJK paragraph has a glyph");
		assert_ne!(glyph.glyph_id, 0, "PRODUCT-0117 CJK fallback must not shape tofu");
		let mut system = font_system().write().expect("global font system lock");
		let face = system
			.raw()
			.db()
			.face(glyph.font_id)
			.expect("PRODUCT-0117 shaped CJK glyph resolves a face");
		assert!(
			face.families
				.iter()
				.all(|(family, _)| family != "Polariton Plot Sans"),
			"PRODUCT-0117 CJK glyph must use fallback, not a missing primary-face glyph"
		);
	}
}
