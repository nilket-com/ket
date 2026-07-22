#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct Rgb {
	pub r: u8,
	pub g: u8,
	pub b: u8,
}

impl Rgb {
	pub const fn new(r: u8, g: u8, b: u8) -> Self {
		Self { r, g, b }
	}

	pub fn from_hex(hex: &str) -> Option<Self> {
		let hex = hex.trim_start_matches('#');
		match hex.len() {
			3..=5 => {
				let r = u8::from_str_radix(&hex[0..1].repeat(2), 16).ok()?;
				let g = u8::from_str_radix(&hex[1..2].repeat(2), 16).ok()?;
				let b = u8::from_str_radix(&hex[2..3].repeat(2), 16).ok()?;
				Some(Self::new(r, g, b))
			}
			6 => {
				let r = u8::from_str_radix(&hex[0..2], 16).ok()?;
				let g = u8::from_str_radix(&hex[2..4], 16).ok()?;
				let b = u8::from_str_radix(&hex[4..6], 16).ok()?;
				Some(Self::new(r, g, b))
			}
			_ => None,
		}
	}

	pub fn to_hex(self) -> String {
		format!("#{:02X}{:02X}{:02X}", self.r, self.g, self.b)
	}
}

pub const DEFAULT_SEARCH_HIGHLIGHT_HEX: &str = "#4CFF00";
pub const SEARCH_MATCH_BG: Rgb = Rgb::new(76, 255, 0);

#[cfg(test)]
mod tests {
	use super::Rgb;

	#[test]
	fn parses_supported_hex_forms() {
		assert_eq!(Rgb::from_hex("#FFF"), Some(Rgb::new(255, 255, 255)));
		assert_eq!(Rgb::from_hex("#000"), Some(Rgb::new(0, 0, 0)));
		assert_eq!(Rgb::from_hex("#f80"), Some(Rgb::new(255, 136, 0)));
		assert_eq!(Rgb::from_hex("ABC"), Some(Rgb::new(170, 187, 204)));
		assert_eq!(Rgb::from_hex("#1234"), Some(Rgb::new(17, 34, 51)));
		assert_eq!(Rgb::from_hex("#FFF0"), Some(Rgb::new(255, 255, 255)));
		assert_eq!(Rgb::from_hex("#12345"), Some(Rgb::new(17, 34, 51)));
		assert_eq!(Rgb::from_hex("#1A2B3C"), Some(Rgb::new(26, 43, 60)));
		assert_eq!(Rgb::from_hex("#000000"), Some(Rgb::new(0, 0, 0)));
		assert_eq!(Rgb::from_hex("#FFFFFF"), Some(Rgb::new(255, 255, 255)));
		assert_eq!(Rgb::from_hex("1A2B3C"), Some(Rgb::new(26, 43, 60)));
	}

	#[test]
	fn rejects_invalid_hex() {
		for invalid in ["", "#1", "#12", "#GG", "#GGG", "#1234567"] {
			assert_eq!(Rgb::from_hex(invalid), None, "{invalid}");
		}
	}

	#[test]
	fn formats_canonical_hex() {
		let color = Rgb::new(26, 43, 60);
		let hex = color.to_hex();
		assert_eq!(hex, "#1A2B3C");
		assert_eq!(Rgb::from_hex(&hex), Some(color));
	}
}
