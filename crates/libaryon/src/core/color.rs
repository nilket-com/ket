use std::hash::{Hash, Hasher};

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

	pub fn as_srgb(self) -> Srgb {
		Srgb {
			red: self.r as f64 / 255.0,
			green: self.g as f64 / 255.0,
			blue: self.b as f64 / 255.0,
		}
	}

	pub fn relative_luminance(self) -> f64 {
		self.as_srgb().relative_luminance()
	}

	pub fn contrast_ratio(self, other: Self) -> f64 {
		self.as_srgb().contrast_ratio(other.as_srgb())
	}
}

/// A validated encoded-sRGB color with normalized channels.
///
/// Channels are finite and lie in the inclusive range `0.0..=1.0`. Construction
/// canonicalizes negative zero, so equality, hashing, and serialized bit patterns
/// agree for numerically identical visible colors.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct Srgb {
	red: f64,
	green: f64,
	blue: f64,
}

impl Srgb {
	pub fn new(red: f64, green: f64, blue: f64) -> Option<Self> {
		fn channel(value: f64) -> Option<f64> {
			if !value.is_finite() || !(0.0..=1.0).contains(&value) {
				return None;
			}
			Some(if value == 0.0 { 0.0 } else { value })
		}

		Some(Self {
			red: channel(red)?,
			green: channel(green)?,
			blue: channel(blue)?,
		})
	}

	pub const fn red(self) -> f64 {
		self.red
	}

	pub const fn green(self) -> f64 {
		self.green
	}

	pub const fn blue(self) -> f64 {
		self.blue
	}

	pub fn relative_luminance(self) -> f64 {
		fn linear(channel: f64) -> f64 {
			if channel <= 0.04045 {
				channel / 12.92
			} else {
				((channel + 0.055) / 1.055).powf(2.4)
			}
		}

		0.2126 * linear(self.red) + 0.7152 * linear(self.green) + 0.0722 * linear(self.blue)
	}

	pub fn contrast_ratio(self, other: Self) -> f64 {
		let first = self.relative_luminance();
		let second = other.relative_luminance();
		let (lighter, darker) = if first >= second {
			(first, second)
		} else {
			(second, first)
		};
		(lighter + 0.05) / (darker + 0.05)
	}
}

impl Eq for Srgb {}

impl Hash for Srgb {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.red.to_bits().hash(state);
		self.green.to_bits().hash(state);
		self.blue.to_bits().hash(state);
	}
}

pub const DEFAULT_SEARCH_HIGHLIGHT_HEX: &str = "#4CFF00";
pub const SEARCH_MATCH_BG: Rgb = Rgb::new(76, 255, 0);

#[cfg(test)]
mod tests {
	use super::{Rgb, Srgb};
	use std::collections::hash_map::DefaultHasher;
	use std::hash::{Hash, Hasher};

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

	#[test]
	fn product_0024_srgb_construction_validates_and_canonicalizes_channels() {
		let endpoints = Srgb::new(0.0, 1.0, 0.5).expect("inclusive endpoints");
		assert_eq!(endpoints.red(), 0.0);
		assert_eq!(endpoints.green(), 1.0);
		assert_eq!(endpoints.blue(), 0.5);

		let negative_zero = Srgb::new(-0.0, 0.0, -0.0).expect("negative zero is valid");
		assert_eq!(negative_zero.red().to_bits(), 0.0f64.to_bits());
		assert_eq!(negative_zero.blue().to_bits(), 0.0f64.to_bits());
		let positive_zero = Srgb::new(0.0, 0.0, 0.0).unwrap();
		assert_eq!(negative_zero, positive_zero);
		let mut negative_hash = DefaultHasher::new();
		negative_zero.hash(&mut negative_hash);
		let mut positive_hash = DefaultHasher::new();
		positive_zero.hash(&mut positive_hash);
		assert_eq!(negative_hash.finish(), positive_hash.finish());

		for invalid in [
			[f64::NAN, 0.0, 0.0],
			[f64::INFINITY, 0.0, 0.0],
			[f64::NEG_INFINITY, 0.0, 0.0],
			[-f64::EPSILON, 0.0, 0.0],
			[1.0 + f64::EPSILON, 0.0, 0.0],
			[0.0, f64::NAN, 0.0],
			[0.0, 0.0, f64::INFINITY],
		] {
			assert_eq!(Srgb::new(invalid[0], invalid[1], invalid[2]), None);
		}
	}

	#[test]
	fn product_0024_srgb_luminance_and_contrast_match_reference_vectors() {
		let black = Srgb::new(0.0, 0.0, 0.0).unwrap();
		let white = Srgb::new(1.0, 1.0, 1.0).unwrap();
		assert_eq!(black.relative_luminance(), 0.0);
		assert_eq!(white.relative_luminance(), 1.0);
		assert_eq!(black.contrast_ratio(white), 21.0);
		assert_eq!(white.contrast_ratio(black), 21.0);
		assert_eq!(black.contrast_ratio(black), 1.0);

		let threshold = Srgb::new(0.04045, 0.04045, 0.04045).unwrap();
		let above = Srgb::new(0.04046, 0.04046, 0.04046).unwrap();
		assert!((threshold.relative_luminance() - 0.0031308049535603713).abs() < 1e-15);
		assert!((above.relative_luminance() - 0.003131594552688991).abs() < 1e-15);

		let gray = Srgb::new(119.0 / 255.0, 119.0 / 255.0, 119.0 / 255.0).unwrap();
		assert!((gray.contrast_ratio(white) - 4.478089453577214).abs() < 1e-12);
	}

	#[test]
	fn product_0024_rgb_delegates_without_changing_byte_values() {
		let foreground = Rgb::new(26, 43, 60);
		let background = Rgb::new(230, 231, 232);
		let normalized = foreground.as_srgb();
		assert_eq!(normalized.red(), 26.0 / 255.0);
		assert_eq!(normalized.green(), 43.0 / 255.0);
		assert_eq!(normalized.blue(), 60.0 / 255.0);
		assert_eq!(
			foreground.relative_luminance(),
			normalized.relative_luminance()
		);
		assert_eq!(
			foreground.contrast_ratio(background),
			normalized.contrast_ratio(background.as_srgb())
		);
	}
}
