use iced::Color;

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
		let t = t.clamp(0.0, 1.0);
		match self {
			ColorTheme::Black => black(t),
			ColorTheme::BlueGreen => blue_green(t),
			ColorTheme::BlueRed => blue_red(t),
			ColorTheme::BlueWhiteRed => blue_white_red(t),
			ColorTheme::Blues => blues(t),
			ColorTheme::BluesR => blues_rev(t),
			ColorTheme::Cividis => cividis(t),
			ColorTheme::CividisR => cividis_rev(t),
			ColorTheme::GreenBlue => green_blue(t),
			ColorTheme::GreenPurples => green_purples(t),
			ColorTheme::GreenRed => green_red(t),
			ColorTheme::Greens => greens(t),
			ColorTheme::GreensR => greens_rev(t),
			ColorTheme::Grey => grey(t),
			ColorTheme::Greys => greys(t),
			ColorTheme::GreysR => greys_rev(t),
			ColorTheme::Inferno => inferno(t),
			ColorTheme::InfernoR => inferno_rev(t),
			ColorTheme::Magma => magma(t),
			ColorTheme::MagmaR => magma_rev(t),
			ColorTheme::Oranges => oranges(t),
			ColorTheme::OrangesR => oranges_rev(t),
			ColorTheme::PurpleGreens => purple_greens(t),
			ColorTheme::Purples => purples(t),
			ColorTheme::PurplesR => purples_rev(t),
			ColorTheme::Rainbow => rainbow(t),
			ColorTheme::RainbowR => rainbow_rev(t),
			ColorTheme::RedBlue => red_blue(t),
			ColorTheme::RedGreen => red_green(t),
			ColorTheme::RedWhiteBlue => red_white_blue(t),
			ColorTheme::Reds => reds(t),
			ColorTheme::RedsR => reds_rev(t),
			ColorTheme::Viridis => viridis(t),
			ColorTheme::ViridisR => viridis_rev(t),
			ColorTheme::White => white(t),
			ColorTheme::Yellows => yellows(t),
			ColorTheme::YellowsR => yellows_rev(t),
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

pub fn rainbow(t: f32) -> Color {
	let h = t.clamp(0.0, 1.0) * 5.0;
	let x = 1.0 - (h % 2.0 - 1.0).abs();
	let (r, g, b) = match h as i32 {
		0 => (1.0, x, 0.0),
		1 => (x, 1.0, 0.0),
		2 => (0.0, 1.0, x),
		3 => (0.0, x, 1.0),
		_ => (x, 0.0, 1.0),
	};
	Color::from_rgb(r, g, b)
}

pub fn rainbow_rev(t: f32) -> Color {
	rainbow(1.0 - t)
}

pub fn greys(t: f32) -> Color {
	Color::from_rgb(t, t, t)
}

pub fn greys_rev(t: f32) -> Color {
	greys(1.0 - t)
}

pub fn black(_t: f32) -> Color {
	Color::BLACK
}

pub fn grey(_t: f32) -> Color {
	Color::from_rgb(0.5, 0.5, 0.5)
}

pub fn white(_t: f32) -> Color {
	Color::WHITE
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
	a + (b - a) * t
}

fn map_color(t: f32, colors: &[(f32, f32, f32)]) -> Color {
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

pub fn viridis(t: f32) -> Color {
	map_color(
		t,
		&[
			(0.267, 0.004, 0.329),
			(0.231, 0.322, 0.545),
			(0.129, 0.569, 0.553),
			(0.369, 0.789, 0.384),
			(0.993, 0.906, 0.145),
		],
	)
}

pub fn viridis_rev(t: f32) -> Color {
	viridis(1.0 - t)
}

pub fn inferno(t: f32) -> Color {
	map_color(
		t,
		&[
			(0.001, 0.005, 0.022),
			(0.337, 0.070, 0.463),
			(0.733, 0.203, 0.333),
			(0.976, 0.588, 0.133),
			(0.988, 0.996, 0.643),
		],
	)
}

pub fn inferno_rev(t: f32) -> Color {
	inferno(1.0 - t)
}

pub fn magma(t: f32) -> Color {
	map_color(
		t,
		&[
			(0.00, 0.00, 0.02),
			(0.31, 0.07, 0.48),
			(0.71, 0.18, 0.50),
			(0.98, 0.56, 0.45),
			(0.99, 0.99, 0.75),
		],
	)
}

pub fn magma_rev(t: f32) -> Color {
	magma(1.0 - t)
}

pub fn cividis(t: f32) -> Color {
	map_color(
		t,
		&[
			(0.00, 0.13, 0.30),
			(0.30, 0.40, 0.48),
			(0.48, 0.48, 0.48),
			(0.73, 0.70, 0.30),
			(0.99, 0.99, 0.25),
		],
	)
}

pub fn cividis_rev(t: f32) -> Color {
	cividis(1.0 - t)
}

pub fn reds(t: f32) -> Color {
	map_color(
		t,
		&[(0.99, 0.88, 0.82), (0.98, 0.38, 0.28), (0.40, 0.00, 0.05)],
	)
}

pub fn reds_rev(t: f32) -> Color {
	reds(1.0 - t)
}

pub fn yellows(t: f32) -> Color {
	map_color(
		t,
		&[(0.99, 0.99, 0.80), (0.99, 0.85, 0.20), (0.60, 0.40, 0.00)],
	)
}

pub fn yellows_rev(t: f32) -> Color {
	yellows(1.0 - t)
}

pub fn blues(t: f32) -> Color {
	map_color(
		t,
		&[(0.93, 0.95, 0.98), (0.42, 0.68, 0.84), (0.03, 0.19, 0.42)],
	)
}

pub fn blues_rev(t: f32) -> Color {
	blues(1.0 - t)
}

pub fn oranges(t: f32) -> Color {
	map_color(
		t,
		&[(0.99, 0.91, 0.80), (0.99, 0.55, 0.15), (0.50, 0.15, 0.00)],
	)
}

pub fn oranges_rev(t: f32) -> Color {
	oranges(1.0 - t)
}

pub fn purples(t: f32) -> Color {
	map_color(
		t,
		&[(0.95, 0.94, 0.97), (0.62, 0.57, 0.76), (0.25, 0.00, 0.49)],
	)
}

pub fn purples_rev(t: f32) -> Color {
	purples(1.0 - t)
}

pub fn greens(t: f32) -> Color {
	map_color(
		t,
		&[(0.93, 0.98, 0.90), (0.25, 0.70, 0.35), (0.00, 0.27, 0.08)],
	)
}

pub fn greens_rev(t: f32) -> Color {
	blues(1.0 - t)
}

pub fn red_blue(t: f32) -> Color {
	map_color(t, &[(1.0, 0.0, 0.0), (0.6, 0.2, 0.8), (0.0, 0.0, 1.0)])
}

pub fn blue_red(t: f32) -> Color {
	red_blue(1.0 - t)
}

pub fn green_blue(t: f32) -> Color {
	map_color(t, &[(0.0, 1.0, 0.0), (0.0, 1.0, 1.0), (0.0, 0.0, 1.0)])
}

pub fn blue_green(t: f32) -> Color {
	green_blue(1.0 - t)
}

pub fn red_green(t: f32) -> Color {
	map_color(t, &[(1.0, 0.0, 0.0), (1.0, 1.0, 0.0), (0.0, 1.0, 0.0)])
}
pub fn green_red(t: f32) -> Color {
	red_green(1.0 - t)
}

pub fn red_white_blue(t: f32) -> Color {
	map_color(
		t,
		&[(0.70, 0.01, 0.15), (1.00, 1.00, 1.00), (0.02, 0.19, 0.46)],
	)
}

pub fn blue_white_red(t: f32) -> Color {
	red_white_blue(1.0 - t)
}

pub fn green_purples(t: f32) -> Color {
	map_color(
		t,
		&[(0.15, 0.44, 0.12), (1.00, 1.00, 1.00), (0.35, 0.11, 0.48)],
	)
}

pub fn purple_greens(t: f32) -> Color {
	green_purples(1.0 - t)
}

pub fn color_to_hex(color: Color) -> String {
	format!(
		"#{:02X}{:02X}{:02X}",
		(color.r * 255.0) as u8,
		(color.g * 255.0) as u8,
		(color.b * 255.0) as u8
	)
}

pub fn hex_to_color(hex: &str) -> Option<Color> {
	let hex = hex.trim_start_matches('#');
	match hex.len() {
		3 => {
			let r = u8::from_str_radix(&hex[0..1].repeat(2), 16).ok()? as f32 / 255.0;
			let g = u8::from_str_radix(&hex[1..2].repeat(2), 16).ok()? as f32 / 255.0;
			let b = u8::from_str_radix(&hex[2..3].repeat(2), 16).ok()? as f32 / 255.0;
			Some(Color::from_rgb(r, g, b))
		}
		4 => {
			let r = u8::from_str_radix(&hex[0..1].repeat(2), 16).ok()? as f32 / 255.0;
			let g = u8::from_str_radix(&hex[1..2].repeat(2), 16).ok()? as f32 / 255.0;
			let b = u8::from_str_radix(&hex[2..3].repeat(2), 16).ok()? as f32 / 255.0;
			let a = u8::from_str_radix(&hex[3..4].repeat(2), 16).ok()? as f32 / 255.0;
			Some(Color::from_rgba(r, g, b, a))
		}
		6 => {
			let r = u8::from_str_radix(&hex[0..2], 16).ok()? as f32 / 255.0;
			let g = u8::from_str_radix(&hex[2..4], 16).ok()? as f32 / 255.0;
			let b = u8::from_str_radix(&hex[4..6], 16).ok()? as f32 / 255.0;
			Some(Color::from_rgb(r, g, b))
		}
		8 => {
			let r = u8::from_str_radix(&hex[0..2], 16).ok()? as f32 / 255.0;
			let g = u8::from_str_radix(&hex[2..4], 16).ok()? as f32 / 255.0;
			let b = u8::from_str_radix(&hex[4..6], 16).ok()? as f32 / 255.0;
			let a = u8::from_str_radix(&hex[6..8], 16).ok()? as f32 / 255.0;
			Some(Color::from_rgba(r, g, b, a))
		}
		_ => None,
	}
}

pub fn contrast_color(bg: Color) -> Color {
	let luminance = 0.299 * bg.r + 0.587 * bg.g + 0.114 * bg.b;
	if luminance > 0.5 {
		Color::BLACK
	} else {
		Color::WHITE
	}
}
