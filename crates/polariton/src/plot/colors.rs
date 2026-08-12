use iced::Color;

// Legacy renderer-ramp vocabulary retained until the separately docketed
// ColorTheme retirement; PRODUCT-0075 removes only its dashboard editor.
#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum ColorTheme {
	Black,
	BlueGreen,
	BlueRed,
	BlueWhiteRed,
	Blues,
	BluesR,
	Cividis,
	CividisR,
	GreenBlue,
	GreenPurples,
	GreenRed,
	Greens,
	GreensR,
	Grey,
	Greys,
	GreysR,
	Inferno,
	InfernoR,
	Magma,
	MagmaR,
	Oranges,
	OrangesR,
	PurpleGreens,
	Purples,
	PurplesR,
	Rainbow,
	RainbowR,
	RedBlue,
	RedGreen,
	RedWhiteBlue,
	Reds,
	RedsR,
	#[default]
	Viridis,
	ViridisR,
	White,
	Yellows,
	YellowsR,
}

impl ColorTheme {
	#[allow(dead_code)]
	pub const ALL: [ColorTheme; 37] = [
		ColorTheme::Black,
		ColorTheme::BlueGreen,
		ColorTheme::BlueRed,
		ColorTheme::BlueWhiteRed,
		ColorTheme::Blues,
		ColorTheme::BluesR,
		ColorTheme::Cividis,
		ColorTheme::CividisR,
		ColorTheme::GreenBlue,
		ColorTheme::GreenPurples,
		ColorTheme::GreenRed,
		ColorTheme::Greens,
		ColorTheme::GreensR,
		ColorTheme::Grey,
		ColorTheme::Greys,
		ColorTheme::GreysR,
		ColorTheme::Inferno,
		ColorTheme::InfernoR,
		ColorTheme::Magma,
		ColorTheme::MagmaR,
		ColorTheme::Oranges,
		ColorTheme::OrangesR,
		ColorTheme::PurpleGreens,
		ColorTheme::Purples,
		ColorTheme::PurplesR,
		ColorTheme::Rainbow,
		ColorTheme::RainbowR,
		ColorTheme::RedBlue,
		ColorTheme::RedGreen,
		ColorTheme::RedWhiteBlue,
		ColorTheme::Reds,
		ColorTheme::RedsR,
		ColorTheme::Viridis,
		ColorTheme::ViridisR,
		ColorTheme::White,
		ColorTheme::Yellows,
		ColorTheme::YellowsR,
	];

	pub fn get_color(&self, t: f32) -> Color {
		self.definition().sample(t)
	}

	/// Canonical normalized stops used by both mark sampling and PRODUCT-0087
	/// guide construction. The colours and direction come from the same
	/// definition as `get_color`, including legacy reversal semantics.
	pub(crate) fn canonical_stops(&self) -> Vec<(f32, Color)> {
		let definition = self.definition();
		let last = definition.colors.len() - 1;
		(0..=last)
			.map(|index| {
				let position = index as f32 / last as f32;
				let color_index = if definition.reversed {
					last - index
				} else {
					index
				};
				let (r, g, b) = definition.colors[color_index];
				(position, Color::from_rgb(r, g, b))
			})
			.collect()
	}

	pub(crate) fn uses_reversed_sampling(&self) -> bool {
		self.definition().reversed
	}

	fn definition(&self) -> RampDefinition {
		use ColorTheme::*;
		match self {
			Black => RampDefinition::forward(BLACK),
			BlueGreen => RampDefinition::reversed(GREEN_BLUE),
			BlueRed => RampDefinition::reversed(RED_BLUE),
			BlueWhiteRed => RampDefinition::reversed(RED_WHITE_BLUE),
			Blues => RampDefinition::forward(BLUES),
			BluesR => RampDefinition::reversed(BLUES),
			Cividis => RampDefinition::forward(CIVIDIS),
			CividisR => RampDefinition::reversed(CIVIDIS),
			GreenBlue => RampDefinition::forward(GREEN_BLUE),
			GreenPurples => RampDefinition::forward(GREEN_PURPLES),
			GreenRed => RampDefinition::reversed(RED_GREEN),
			Greens => RampDefinition::forward(GREENS),
			// Preserve the shipped `greens_rev` behaviour, which delegates to
			// `blues(1.0 - t)` rather than to `greens`.
			GreensR => RampDefinition::reversed(BLUES),
			Grey => RampDefinition::forward(GREY),
			Greys => RampDefinition::forward(GREYS),
			GreysR => RampDefinition::reversed(GREYS),
			Inferno => RampDefinition::forward(INFERNO),
			InfernoR => RampDefinition::reversed(INFERNO),
			Magma => RampDefinition::forward(MAGMA),
			MagmaR => RampDefinition::reversed(MAGMA),
			Oranges => RampDefinition::forward(ORANGES),
			OrangesR => RampDefinition::reversed(ORANGES),
			PurpleGreens => RampDefinition::reversed(GREEN_PURPLES),
			Purples => RampDefinition::forward(PURPLES),
			PurplesR => RampDefinition::reversed(PURPLES),
			Rainbow => RampDefinition::forward(RAINBOW),
			RainbowR => RampDefinition::reversed(RAINBOW),
			RedBlue => RampDefinition::forward(RED_BLUE),
			RedGreen => RampDefinition::forward(RED_GREEN),
			RedWhiteBlue => RampDefinition::forward(RED_WHITE_BLUE),
			Reds => RampDefinition::forward(REDS),
			RedsR => RampDefinition::reversed(REDS),
			Viridis => RampDefinition::forward(VIRIDIS),
			ViridisR => RampDefinition::reversed(VIRIDIS),
			White => RampDefinition::forward(WHITE),
			Yellows => RampDefinition::forward(YELLOWS),
			YellowsR => RampDefinition::reversed(YELLOWS),
		}
	}
}

impl std::fmt::Display for ColorTheme {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			ColorTheme::Black => write!(f, "Black"),
			ColorTheme::BlueGreen => write!(f, "BlueGreen"),
			ColorTheme::BlueRed => write!(f, "BlueRed"),
			ColorTheme::BlueWhiteRed => write!(f, "BlueWhiteRed"),
			ColorTheme::Blues => write!(f, "Blues"),
			ColorTheme::BluesR => write!(f, "BluesR"),
			ColorTheme::Cividis => write!(f, "Cividis"),
			ColorTheme::CividisR => write!(f, "CividisR"),
			ColorTheme::GreenBlue => write!(f, "GreenBlue"),
			ColorTheme::GreenPurples => write!(f, "GreenPurples"),
			ColorTheme::GreenRed => write!(f, "GreenRed"),
			ColorTheme::Greens => write!(f, "Greens"),
			ColorTheme::GreensR => write!(f, "GreensR"),
			ColorTheme::Grey => write!(f, "Grey"),
			ColorTheme::Greys => write!(f, "Greys"),
			ColorTheme::GreysR => write!(f, "GreysR"),
			ColorTheme::Inferno => write!(f, "Inferno"),
			ColorTheme::InfernoR => write!(f, "InfernoR"),
			ColorTheme::Magma => write!(f, "Magma"),
			ColorTheme::MagmaR => write!(f, "MagmaR"),
			ColorTheme::Oranges => write!(f, "Oranges"),
			ColorTheme::OrangesR => write!(f, "OrangesR"),
			ColorTheme::PurpleGreens => write!(f, "PurpleGreens"),
			ColorTheme::Purples => write!(f, "Purples"),
			ColorTheme::PurplesR => write!(f, "PurplesR"),
			ColorTheme::Rainbow => write!(f, "Rainbow"),
			ColorTheme::RainbowR => write!(f, "RainbowR"),
			ColorTheme::RedBlue => write!(f, "RedBlue"),
			ColorTheme::RedGreen => write!(f, "RedGreen"),
			ColorTheme::RedWhiteBlue => write!(f, "RedWhiteBlue"),
			ColorTheme::Reds => write!(f, "Reds"),
			ColorTheme::RedsR => write!(f, "RedsR"),
			ColorTheme::Viridis => write!(f, "Viridis"),
			ColorTheme::ViridisR => write!(f, "ViridisR"),
			ColorTheme::White => write!(f, "White"),
			ColorTheme::Yellows => write!(f, "Yellows"),
			ColorTheme::YellowsR => write!(f, "YellowsR"),
		}
	}
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
	a + (b - a) * t
}

fn sample_even_stops(t: f32, colors: &[(f32, f32, f32)]) -> Color {
	let t = t.clamp(0.0, 1.0) * (colors.len() - 1) as f32;
	let i = t.floor() as usize;
	let frac = t.fract();
	if i >= colors.len() - 1 {
		let (r, g, b) = colors[colors.len() - 1];
		return Color::from_rgb(r, g, b);
	}
	let (r1, g1, b1) = colors[i];
	let (r2, g2, b2) = colors[i + 1];
	Color::from_rgb(lerp(r1, r2, frac), lerp(g1, g2, frac), lerp(b1, b2, frac))
}

#[derive(Clone, Copy)]
struct RampDefinition {
	colors: &'static [(f32, f32, f32)],
	reversed: bool,
}

impl RampDefinition {
	const fn forward(colors: &'static [(f32, f32, f32)]) -> Self {
		Self {
			colors,
			reversed: false,
		}
	}

	const fn reversed(colors: &'static [(f32, f32, f32)]) -> Self {
		Self {
			colors,
			reversed: true,
		}
	}

	fn sample(self, t: f32) -> Color {
		let t = t.clamp(0.0, 1.0);
		let t = if self.reversed { 1.0 - t } else { t };
		sample_even_stops(t, self.colors)
	}
}

const BLACK: &[(f32, f32, f32)] = &[(0.0, 0.0, 0.0), (0.0, 0.0, 0.0)];
const GREY: &[(f32, f32, f32)] = &[(0.5, 0.5, 0.5), (0.5, 0.5, 0.5)];
const WHITE: &[(f32, f32, f32)] = &[(1.0, 1.0, 1.0), (1.0, 1.0, 1.0)];
const GREYS: &[(f32, f32, f32)] = &[(0.0, 0.0, 0.0), (1.0, 1.0, 1.0)];
const RAINBOW: &[(f32, f32, f32)] = &[
	(1.0, 0.0, 0.0),
	(1.0, 1.0, 0.0),
	(0.0, 1.0, 0.0),
	(0.0, 1.0, 1.0),
	(0.0, 0.0, 1.0),
	(1.0, 0.0, 1.0),
];
const VIRIDIS: &[(f32, f32, f32)] = &[
	(0.267, 0.004, 0.329),
	(0.231, 0.322, 0.545),
	(0.129, 0.569, 0.553),
	(0.369, 0.789, 0.384),
	(0.993, 0.906, 0.145),
];
const INFERNO: &[(f32, f32, f32)] = &[
	(0.001, 0.005, 0.022),
	(0.337, 0.070, 0.463),
	(0.733, 0.203, 0.333),
	(0.976, 0.588, 0.133),
	(0.988, 0.996, 0.643),
];
const MAGMA: &[(f32, f32, f32)] = &[
	(0.00, 0.00, 0.02),
	(0.31, 0.07, 0.48),
	(0.71, 0.18, 0.50),
	(0.98, 0.56, 0.45),
	(0.99, 0.99, 0.75),
];
const CIVIDIS: &[(f32, f32, f32)] = &[
	(0.00, 0.13, 0.30),
	(0.30, 0.40, 0.48),
	(0.48, 0.48, 0.48),
	(0.73, 0.70, 0.30),
	(0.99, 0.99, 0.25),
];
const REDS: &[(f32, f32, f32)] = &[(0.99, 0.88, 0.82), (0.98, 0.38, 0.28), (0.40, 0.00, 0.05)];
const YELLOWS: &[(f32, f32, f32)] = &[(0.99, 0.99, 0.80), (0.99, 0.85, 0.20), (0.60, 0.40, 0.00)];
const BLUES: &[(f32, f32, f32)] = &[(0.93, 0.95, 0.98), (0.42, 0.68, 0.84), (0.03, 0.19, 0.42)];
const ORANGES: &[(f32, f32, f32)] = &[(0.99, 0.91, 0.80), (0.99, 0.55, 0.15), (0.50, 0.15, 0.00)];
const PURPLES: &[(f32, f32, f32)] = &[(0.95, 0.94, 0.97), (0.62, 0.57, 0.76), (0.25, 0.00, 0.49)];
const GREENS: &[(f32, f32, f32)] = &[(0.93, 0.98, 0.90), (0.25, 0.70, 0.35), (0.00, 0.27, 0.08)];
const RED_BLUE: &[(f32, f32, f32)] = &[(1.0, 0.0, 0.0), (0.6, 0.2, 0.8), (0.0, 0.0, 1.0)];
const GREEN_BLUE: &[(f32, f32, f32)] = &[(0.0, 1.0, 0.0), (0.0, 1.0, 1.0), (0.0, 0.0, 1.0)];
const RED_GREEN: &[(f32, f32, f32)] = &[(1.0, 0.0, 0.0), (1.0, 1.0, 0.0), (0.0, 1.0, 0.0)];
const RED_WHITE_BLUE: &[(f32, f32, f32)] =
	&[(0.70, 0.01, 0.15), (1.00, 1.00, 1.00), (0.02, 0.19, 0.46)];
const GREEN_PURPLES: &[(f32, f32, f32)] =
	&[(0.15, 0.44, 0.12), (1.00, 1.00, 1.00), (0.35, 0.11, 0.48)];

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0087_every_theme_is_bit_exact_against_the_stage_zero_bank() {
		const SAMPLES: [f32; 17] = [
			0.0, 0.1, 0.125, 0.2, 0.25, 0.3, 0.375, 0.4, 0.5, 0.6, 0.625, 0.7, 0.75, 0.8, 0.875,
			0.9, 1.0,
		];
		const BANKED: [u64; 37] = [
			0x3ce97e2a1675c8f5,
			0x86496bdb3368666d,
			0x536d97a4ca2ed862,
			0x16f37dcc014cf60c,
			0x23514e15642108e1,
			0xdf1703253035b2b5,
			0xbf8186c1c7adc665,
			0xb59756155c7b2b4f,
			0xf81973f1ee8fa9cc,
			0xe9ffaa29887d2f3b,
			0x4f3dfbcc2fd1aadd,
			0xb025258140b5d5c3,
			0xdf1703253035b2b5,
			0x7eb57a4b4d75c8f5,
			0xa37081aae6481022,
			0x5ce457ba16c5644d,
			0x707e40498fbdaf38,
			0xca6e4f1cf2df79e4,
			0x983ec7549e7578b4,
			0xbabe5035efa7830e,
			0x6f6cadb9b2a77c72,
			0x5e35b787dcefd59d,
			0xb72122a80136894c,
			0x75be8844e8edab05,
			0xbe270a06202ace58,
			0x3d03a1d8d535c8f5,
			0x3123a2d561fb6086,
			0x7c0dbfe60d08a08c,
			0x58abce6b8e0d14c2,
			0x407a598b25623df3,
			0x8bffaad1e8e76da3,
			0x40735ae8dcf2e93d,
			0xffddd269dcb718d9,
			0x6950e146836e7e9a,
			0x08f912d6d5f5c8f5,
			0x625638ddea012ed1,
			0x87a2b94efb5efe69,
		];
		for (theme, expected) in ColorTheme::ALL.into_iter().zip(BANKED) {
			let mut hash = 0xcbf29ce484222325_u64;
			for t in SAMPLES {
				let color = theme.get_color(t);
				for bits in [
					color.r.to_bits(),
					color.g.to_bits(),
					color.b.to_bits(),
					color.a.to_bits(),
				] {
					hash ^= u64::from(bits);
					hash = hash.wrapping_mul(0x100000001b3);
				}
			}
			assert_eq!(hash, expected, "{theme:?} changed its banked colour bits");
		}
	}

	#[test]
	fn product_0087_canonical_midpoints_and_constant_ramps_are_explicit() {
		let green_purples = ColorTheme::GreenPurples.canonical_stops();
		assert_eq!(green_purples.len(), 3);
		assert_eq!(green_purples[1], (0.5, Color::WHITE));
		let viridis = ColorTheme::Viridis.canonical_stops();
		assert_eq!(viridis.len(), 5);
		assert_eq!(viridis[2], (0.5, Color::from_rgb(0.129, 0.569, 0.553)));
		for theme in [ColorTheme::Black, ColorTheme::Grey, ColorTheme::White] {
			let stops = theme.canonical_stops();
			assert_eq!(stops.len(), 2);
			assert_eq!(stops[0].1, stops[1].1);
		}
	}
}
