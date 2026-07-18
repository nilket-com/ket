use crate::core::DocByte;
use crate::core::Rgb;
use crate::svp::diagnostic::{DiagnosticSeverity, DiagnosticSpan};
use crate::svp::rust_highlight::{CATEGORY_COUNT, HighlightSpan};

pub struct HighlightProjector {
	spans: Vec<HighlightSpan>,
	theme_colors: [Option<Rgb>; CATEGORY_COUNT],
}

impl HighlightProjector {
	pub fn new(mut spans: Vec<HighlightSpan>, theme_colors: [Option<Rgb>; CATEGORY_COUNT]) -> Self {
		spans.sort_by_key(|span| span.start);
		Self {
			spans,
			theme_colors,
		}
	}

	pub fn style_for_byte(&self, byte_offset: DocByte) -> Option<Rgb> {
		let idx = self.spans.partition_point(|span| span.start <= byte_offset);
		if idx > 0 {
			let span = &self.spans[idx - 1];
			if byte_offset < span.end {
				return self.theme_colors[span.category as usize];
			}
		}
		None
	}
}

pub struct DiagnosticProjector {
	spans: Vec<DiagnosticSpan>,
}

impl DiagnosticProjector {
	pub fn new(mut spans: Vec<DiagnosticSpan>) -> Self {
		spans.sort_by_key(|span| span.start);
		Self { spans }
	}

	pub fn severity_for_range(&self, start: DocByte, end: DocByte) -> Option<DiagnosticSeverity> {
		if start >= end {
			return None;
		}

		let idx = self.spans.partition_point(|span| span.start < end);
		let mut severity: Option<DiagnosticSeverity> = None;
		for span in self.spans[..idx].iter().rev() {
			if span.end <= start {
				break;
			}
			severity = Some(severity.map_or(span.severity, |curr| curr.max(span.severity)));
		}
		severity
	}
}

#[cfg(test)]
mod tests {
	use super::DiagnosticProjector;
	use crate::core::DocByte;
	use crate::svp::diagnostic::{DiagnosticSeverity, DiagnosticSpan};

	#[test]
	fn diagnostic_projector_returns_highest_overlapping_severity() {
		let projector = DiagnosticProjector::new(vec![
			DiagnosticSpan::new(
				DocByte::new(0),
				DocByte::new(4),
				DiagnosticSeverity::WeakWarning,
			),
			DiagnosticSpan::new(DocByte::new(2), DocByte::new(6), DiagnosticSeverity::Error),
		]);

		assert_eq!(
			projector.severity_for_range(DocByte::new(3), DocByte::new(4)),
			Some(DiagnosticSeverity::Error),
		);
		assert_eq!(
			projector.severity_for_range(DocByte::new(0), DocByte::new(1)),
			Some(DiagnosticSeverity::WeakWarning),
		);
		assert_eq!(
			projector.severity_for_range(DocByte::new(7), DocByte::new(8)),
			None,
		);
	}
}
