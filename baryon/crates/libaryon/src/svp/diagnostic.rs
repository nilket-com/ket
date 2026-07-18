use crate::core::DocByte;

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub enum DiagnosticSeverity {
	WeakWarning,
	Warning,
	Error,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct DiagnosticSpan {
	pub start: DocByte,
	pub end: DocByte,
	pub severity: DiagnosticSeverity,
}

impl DiagnosticSpan {
	pub const fn new(start: DocByte, end: DocByte, severity: DiagnosticSeverity) -> Self {
		Self {
			start,
			end,
			severity,
		}
	}
}
