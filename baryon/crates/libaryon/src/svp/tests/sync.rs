use crate::svp::sync::*;

#[test]
fn test_valid_boundaries() {
	let buf = b"skip\n\nvalid code\n\nmore skip";
	let bounds = find_safe_parse_boundaries(buf).unwrap();
	assert_eq!(bounds, (0, 27));
	assert_eq!(&buf[bounds.0..bounds.1], b"skip\n\nvalid code\n\nmore skip");
}

#[test]
fn test_invalid_utf8() {
	let mut buf = b"skip\n\nvalid code\n\nmore skip".to_vec();
	buf[8] = 0xFF; // Inject invalid UTF-8 in "valid code"
	let bounds = find_safe_parse_boundaries(&buf);
	// Retracts from end until it hits a valid UTF-8 boundary before the 0xFF at index 8.
	// Index 8 is invalid, index 7 (' ') is valid.
	assert_eq!(bounds, Some((0, 8)));
}

#[test]
fn test_no_boundaries() {
	let buf = b"skip\nvalid code\nmore skip";
	let bounds = find_safe_parse_boundaries(buf);
	// Start 0, End 25
	assert_eq!(bounds, Some((0, 25)));
}

#[test]
fn test_single_boundary() {
	let buf = b"skip\n\nvalid code\nmore skip";
	let bounds = find_safe_parse_boundaries(buf);
	assert_eq!(bounds, Some((0, 26)));
}
