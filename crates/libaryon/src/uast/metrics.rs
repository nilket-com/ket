/// ==========================================
/// LAYER 2: STRUCTURAL METRICS & TOPOLOGY
/// ==========================================
/// Structural metrics decoupled from semantic meaning, critical for
/// fast rope-like traversal, line/column resolution, and bounding-box queries.
#[derive(Debug, Clone, Copy, Default)]
pub struct SpanMetrics {
	pub byte_length: u64,
	pub newlines: u64,
}
