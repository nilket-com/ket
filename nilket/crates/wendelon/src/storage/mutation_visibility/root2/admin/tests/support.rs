pub(super) fn hex(s: &str) -> Vec<u8> {
	assert_eq!(s.len() % 2, 0);
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).unwrap())
		.collect()
}
pub(super) fn record_vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("record_vectors.json")).unwrap()
}
