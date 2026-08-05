use crate::core::Rgb;
use crate::svp::rust_highlight::{CATEGORY_COUNT, TokenCategory};

const THEMED_CATEGORIES: [TokenCategory; 20] = [
	TokenCategory::Keyword,
	TokenCategory::String,
	TokenCategory::Comment,
	TokenCategory::Type,
	TokenCategory::Function,
	TokenCategory::Number,
	TokenCategory::Punctuation,
	TokenCategory::Variable,
	TokenCategory::Constant,
	TokenCategory::Macro,
	TokenCategory::Module,
	TokenCategory::Lifetime,
	TokenCategory::Attribute,
	TokenCategory::Operator,
	TokenCategory::SelfKeyword,
	TokenCategory::BuiltinType,
	TokenCategory::MutableVariable,
	TokenCategory::Method,
	TokenCategory::Crate,
	TokenCategory::Whitespace,
];

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct ResolvedThemePalette([Rgb; THEMED_CATEGORIES.len()]);

impl ResolvedThemePalette {
	pub const fn colors(&self) -> &[Rgb; THEMED_CATEGORIES.len()] {
		&self.0
	}

	pub fn color(&self, index: usize) -> Option<Rgb> {
		self.0.get(index).copied()
	}

	pub fn most_contrasting_pair(&self) -> ResolvedThemePair {
		let mut selected = (0, 1);
		let mut selected_ratio = self.0[0].contrast_ratio(self.0[1]);

		for first in 0..self.0.len() - 1 {
			for second in first + 1..self.0.len() {
				let ratio = self.0[first].contrast_ratio(self.0[second]);
				if ratio > selected_ratio {
					selected = (first, second);
					selected_ratio = ratio;
				}
			}
		}

		let (first, second) = selected;
		let first_luminance = self.0[first].relative_luminance();
		let second_luminance = self.0[second].relative_luminance();
		let (darker_index, lighter_index) = if first_luminance <= second_luminance {
			(first, second)
		} else {
			(second, first)
		};

		ResolvedThemePair {
			darker_index,
			darker: self.0[darker_index],
			lighter_index,
			lighter: self.0[lighter_index],
			contrast_ratio: selected_ratio,
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ResolvedThemePair {
	darker_index: usize,
	darker: Rgb,
	lighter_index: usize,
	lighter: Rgb,
	contrast_ratio: f64,
}

impl ResolvedThemePair {
	pub const fn darker_index(self) -> usize {
		self.darker_index
	}

	pub const fn darker(self) -> Rgb {
		self.darker
	}

	pub const fn lighter_index(self) -> usize {
		self.lighter_index
	}

	pub const fn lighter(self) -> Rgb {
		self.lighter
	}

	pub const fn contrast_ratio(self) -> f64 {
		self.contrast_ratio
	}
}

fn set_color(map: &mut [Option<Rgb>; CATEGORY_COUNT], category: TokenCategory, color: Rgb) {
	map[category as usize] = Some(color);
}

fn parse_theme_modifiers(name: &str) -> (&str, bool, bool, bool, usize) {
	let Some((base, suffix)) = name.rsplit_once('_') else {
		return (name, false, false, false, 0);
	};

	if suffix.is_empty() {
		return (name, false, false, false, 0);
	}

	let mut reverse = false;
	let mut clip_low = false;
	let mut clip_high = false;
	let mut shift = 0;

	let chars: Vec<char> = suffix.chars().collect();
	let mut i = 0;
	while i < chars.len() {
		match chars[i] {
			'r' => reverse = true,
			'c' => clip_low = true,
			'C' => clip_high = true,
			's' => {
				i += 1;
				let mut num_str = String::new();
				while i < chars.len() && chars[i].is_ascii_digit() {
					num_str.push(chars[i]);
					i += 1;
				}
				if !num_str.is_empty() {
					shift = num_str.parse().unwrap_or(0);
				}
				continue;
			}
			_ => return (name, false, false, false, 0),
		}
		i += 1;
	}

	(base, reverse, clip_low, clip_high, shift)
}

pub struct Theme {
	pub syntax_colors: [Option<Rgb>; CATEGORY_COUNT],
}

impl Theme {
	pub fn try_new(name: &str) -> Result<Self, String> {
		let mut colors = [None; CATEGORY_COUNT];
		let (raw_map_name, reverse, clip_low, clip_high, shift) = parse_theme_modifiers(name);
		let map_name = if raw_map_name.is_empty() {
			"viridis".to_string()
		} else {
			raw_map_name.to_lowercase()
		};
		let Some(spec) = theme_spec(&map_name) else {
			return Err(format!("Unknown theme: {}", name));
		};

		if let ThemePalette::Discrete(fill) = spec.palette {
			fill(&mut colors);
			if reverse || shift > 0 {
				let assigned_cats: Vec<TokenCategory> = THEMED_CATEGORIES
					.iter()
					.copied()
					.filter(|&cat| colors[cat as usize].is_some())
					.collect();
				let mut assigned_colors: Vec<Rgb> = assigned_cats
					.iter()
					.filter_map(|&cat| colors[cat as usize])
					.collect();
				if reverse {
					assigned_colors.reverse();
				}
				if shift > 0 && !assigned_colors.is_empty() {
					let mid = shift % assigned_colors.len();
					assigned_colors.rotate_right(mid);
				}
				for (cat, color) in assigned_cats.into_iter().zip(assigned_colors) {
					colors[cat as usize] = Some(color);
				}
			}
			return Ok(Self {
				syntax_colors: colors,
			});
		}

		let ThemePalette::Continuous(sampler) = spec.palette else {
			unreachable!("discrete themes returned above")
		};

		let start = if clip_low { 0.2 } else { 0.0 };
		let end = if clip_high { 0.8 } else { 1.0 };
		let step_scale = end - start;
		let mut sampled_colors = [Rgb::new(0, 0, 0); THEMED_CATEGORIES.len()];

		for (i, color) in sampled_colors.iter_mut().enumerate() {
			let t = i as f32 / (THEMED_CATEGORIES.len() as f32 - 1.0);
			*color = sampler(start + (t * step_scale));
		}

		if reverse {
			sampled_colors.reverse();
		}

		if shift > 0 {
			let mid = shift % sampled_colors.len();
			sampled_colors.rotate_right(mid);
		}

		for (cat, color) in THEMED_CATEGORIES.iter().copied().zip(sampled_colors) {
			colors[cat as usize] = Some(color);
		}

		Ok(Self {
			syntax_colors: colors,
		})
	}

	fn fill_gruvbox(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let red = Rgb::new(251, 73, 52);
		let green = Rgb::new(184, 187, 38);
		let yellow = Rgb::new(250, 189, 47);
		let blue = Rgb::new(131, 165, 152);
		let purple = Rgb::new(211, 134, 155);
		let aqua = Rgb::new(142, 192, 124);
		let orange = Rgb::new(254, 128, 25);
		let gray = Rgb::new(146, 131, 116);
		let fg = Rgb::new(235, 219, 178);

		set_color(map, TokenCategory::Keyword, red);
		set_color(map, TokenCategory::String, green);
		set_color(map, TokenCategory::Comment, gray);
		set_color(map, TokenCategory::Type, aqua);
		set_color(map, TokenCategory::Function, yellow);
		set_color(map, TokenCategory::Number, purple);
		set_color(map, TokenCategory::Punctuation, fg);
		set_color(map, TokenCategory::BodyText, fg);
		set_color(map, TokenCategory::Variable, fg);
		set_color(map, TokenCategory::Constant, purple);
		set_color(map, TokenCategory::Macro, aqua);
		set_color(map, TokenCategory::Module, blue);
		set_color(map, TokenCategory::Lifetime, blue);
		set_color(map, TokenCategory::Attribute, aqua);
		set_color(map, TokenCategory::Operator, orange);
		set_color(map, TokenCategory::SelfKeyword, red);
		set_color(map, TokenCategory::BuiltinType, aqua);
		set_color(map, TokenCategory::MutableVariable, orange);
		set_color(map, TokenCategory::Method, yellow);
		set_color(map, TokenCategory::Crate, blue);
		set_color(map, TokenCategory::Whitespace, Rgb::new(60, 56, 54));
	}

	fn fill_nord(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let frost_1 = Rgb::new(143, 188, 187);
		let frost_2 = Rgb::new(136, 192, 208);
		let frost_3 = Rgb::new(129, 161, 193);
		let frost_4 = Rgb::new(94, 129, 172);
		let aurora_red = Rgb::new(191, 97, 106);
		let aurora_orange = Rgb::new(208, 135, 112);
		let aurora_yellow = Rgb::new(235, 203, 139);
		let aurora_green = Rgb::new(163, 190, 140);
		let aurora_purple = Rgb::new(180, 142, 173);
		let snow_storm_1 = Rgb::new(216, 222, 233);
		let polar_night_4 = Rgb::new(76, 86, 106);

		set_color(map, TokenCategory::Keyword, frost_3);
		set_color(map, TokenCategory::String, aurora_green);
		set_color(map, TokenCategory::Comment, polar_night_4);
		set_color(map, TokenCategory::Type, frost_1);
		set_color(map, TokenCategory::Function, aurora_yellow);
		set_color(map, TokenCategory::Number, aurora_purple);
		set_color(map, TokenCategory::Punctuation, snow_storm_1);
		set_color(map, TokenCategory::BodyText, snow_storm_1);
		set_color(map, TokenCategory::Variable, snow_storm_1);
		set_color(map, TokenCategory::Constant, aurora_purple);
		set_color(map, TokenCategory::Macro, frost_4);
		set_color(map, TokenCategory::Module, frost_3);
		set_color(map, TokenCategory::Lifetime, aurora_red);
		set_color(map, TokenCategory::Attribute, frost_1);
		set_color(map, TokenCategory::Operator, frost_2);
		set_color(map, TokenCategory::SelfKeyword, aurora_red);
		set_color(map, TokenCategory::BuiltinType, frost_1);
		set_color(map, TokenCategory::MutableVariable, aurora_orange);
		set_color(map, TokenCategory::Method, frost_2);
		set_color(map, TokenCategory::Crate, frost_3);
		set_color(map, TokenCategory::Whitespace, Rgb::new(59, 66, 82));
	}

	fn fill_solarized(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let base01 = Rgb::new(88, 110, 117);
		let base0 = Rgb::new(131, 148, 150);
		let yellow = Rgb::new(181, 137, 0);
		let orange = Rgb::new(203, 75, 22);
		let red = Rgb::new(220, 50, 47);
		let magenta = Rgb::new(211, 54, 130);
		let violet = Rgb::new(108, 113, 196);
		let blue = Rgb::new(38, 139, 210);
		let cyan = Rgb::new(42, 161, 152);
		let green = Rgb::new(133, 153, 0);

		set_color(map, TokenCategory::Keyword, green);
		set_color(map, TokenCategory::String, cyan);
		set_color(map, TokenCategory::Comment, base01);
		set_color(map, TokenCategory::Type, yellow);
		set_color(map, TokenCategory::Function, blue);
		set_color(map, TokenCategory::Number, magenta);
		set_color(map, TokenCategory::Punctuation, base0);
		set_color(map, TokenCategory::BodyText, base0);
		set_color(map, TokenCategory::Variable, base0);
		set_color(map, TokenCategory::Constant, magenta);
		set_color(map, TokenCategory::Macro, blue);
		set_color(map, TokenCategory::Module, violet);
		set_color(map, TokenCategory::Lifetime, violet);
		set_color(map, TokenCategory::Attribute, yellow);
		set_color(map, TokenCategory::Operator, orange);
		set_color(map, TokenCategory::SelfKeyword, red);
		set_color(map, TokenCategory::BuiltinType, yellow);
		set_color(map, TokenCategory::MutableVariable, orange);
		set_color(map, TokenCategory::Method, blue);
		set_color(map, TokenCategory::Crate, violet);
		set_color(map, TokenCategory::Whitespace, Rgb::new(7, 54, 66));
	}

	fn fill_pretty(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let fg = Rgb::new(206, 206, 238);
		let comment = Rgb::new(106, 106, 106);
		let constant = Rgb::new(255, 204, 0);
		let number = Rgb::new(255, 128, 0);
		let keyword = Rgb::new(255, 0, 255);
		let type_col = Rgb::new(0, 255, 128);
		let function = Rgb::new(255, 255, 0);
		let whitespace = Rgb::new(68, 68, 68);

		set_color(map, TokenCategory::Keyword, keyword);
		set_color(map, TokenCategory::String, constant);
		set_color(map, TokenCategory::Comment, comment);
		set_color(map, TokenCategory::Type, type_col);
		set_color(map, TokenCategory::Function, function);
		set_color(map, TokenCategory::Number, number);
		set_color(map, TokenCategory::Punctuation, fg);
		set_color(map, TokenCategory::BodyText, fg);
		set_color(map, TokenCategory::Variable, type_col);
		set_color(map, TokenCategory::Constant, constant);
		set_color(map, TokenCategory::Macro, function);
		set_color(map, TokenCategory::Module, type_col);
		set_color(map, TokenCategory::Lifetime, keyword);
		set_color(map, TokenCategory::Attribute, type_col);
		set_color(map, TokenCategory::Operator, fg);
		set_color(map, TokenCategory::SelfKeyword, type_col);
		set_color(map, TokenCategory::BuiltinType, type_col);
		set_color(map, TokenCategory::MutableVariable, type_col);
		set_color(map, TokenCategory::Method, function);
		set_color(map, TokenCategory::Crate, type_col);
		set_color(map, TokenCategory::Whitespace, whitespace);
	}

	fn fill_classic(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let fg = Rgb::new(206, 206, 238);
		let comment = Rgb::new(106, 106, 106);
		let constant = Rgb::new(255, 204, 0);
		let number = Rgb::new(255, 128, 0);
		let keyword = Rgb::new(0, 255, 255);
		let type_col = Rgb::new(0, 255, 128);
		let function = Rgb::new(255, 255, 0);
		let whitespace = Rgb::new(68, 68, 68);

		set_color(map, TokenCategory::Keyword, keyword);
		set_color(map, TokenCategory::String, constant);
		set_color(map, TokenCategory::Comment, comment);
		set_color(map, TokenCategory::Type, type_col);
		set_color(map, TokenCategory::Function, function);
		set_color(map, TokenCategory::Number, number);
		set_color(map, TokenCategory::Punctuation, fg);
		set_color(map, TokenCategory::BodyText, fg);
		set_color(map, TokenCategory::Variable, type_col);
		set_color(map, TokenCategory::Constant, constant);
		set_color(map, TokenCategory::Macro, function);
		set_color(map, TokenCategory::Module, type_col);
		set_color(map, TokenCategory::Lifetime, keyword);
		set_color(map, TokenCategory::Attribute, type_col);
		set_color(map, TokenCategory::Operator, fg);
		set_color(map, TokenCategory::SelfKeyword, type_col);
		set_color(map, TokenCategory::BuiltinType, type_col);
		set_color(map, TokenCategory::MutableVariable, type_col);
		set_color(map, TokenCategory::Method, function);
		set_color(map, TokenCategory::Crate, type_col);
		set_color(map, TokenCategory::Whitespace, whitespace);
	}

	fn fill_tab20(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(31, 119, 180);
		let c02 = Rgb::new(174, 199, 232);
		let c03 = Rgb::new(255, 127, 14);
		let c04 = Rgb::new(255, 187, 120);
		let c05 = Rgb::new(44, 160, 44);
		let c06 = Rgb::new(152, 223, 138);
		let c07 = Rgb::new(214, 39, 40);
		let c08 = Rgb::new(255, 152, 150);
		let c09 = Rgb::new(148, 103, 189);
		let c10 = Rgb::new(197, 176, 213);
		let c11 = Rgb::new(140, 86, 75);
		let c12 = Rgb::new(196, 156, 148);
		let c13 = Rgb::new(227, 119, 194);
		let c14 = Rgb::new(247, 182, 210);
		let c15 = Rgb::new(127, 127, 127);
		let c16 = Rgb::new(199, 199, 199);
		let c17 = Rgb::new(188, 189, 34);
		let c18 = Rgb::new(219, 219, 141);
		let c19 = Rgb::new(23, 190, 207);
		let c20 = Rgb::new(158, 218, 229);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c15);
		set_color(map, TokenCategory::Type, c03);
		set_color(map, TokenCategory::Function, c04);
		set_color(map, TokenCategory::Number, c05);
		set_color(map, TokenCategory::Punctuation, c16);
		set_color(map, TokenCategory::BodyText, c16);
		set_color(map, TokenCategory::Variable, c06);
		set_color(map, TokenCategory::Constant, c07);
		set_color(map, TokenCategory::Macro, c08);
		set_color(map, TokenCategory::Module, c09);
		set_color(map, TokenCategory::Lifetime, c10);
		set_color(map, TokenCategory::Attribute, c11);
		set_color(map, TokenCategory::Operator, c12);
		set_color(map, TokenCategory::SelfKeyword, c13);
		set_color(map, TokenCategory::BuiltinType, c14);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_tab20b(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(57, 59, 121);
		let c02 = Rgb::new(82, 84, 163);
		let c03 = Rgb::new(107, 110, 207);
		let c04 = Rgb::new(156, 158, 222);
		let c05 = Rgb::new(99, 121, 57);
		let c06 = Rgb::new(140, 162, 82);
		let c07 = Rgb::new(181, 207, 107);
		let c08 = Rgb::new(206, 219, 156);
		let c09 = Rgb::new(140, 109, 49);
		let c10 = Rgb::new(189, 158, 57);
		let c11 = Rgb::new(231, 186, 82);
		let c12 = Rgb::new(231, 203, 148);
		let c13 = Rgb::new(132, 60, 57);
		let c14 = Rgb::new(173, 73, 74);
		let c15 = Rgb::new(214, 97, 107);
		let c16 = Rgb::new(231, 150, 156);
		let c17 = Rgb::new(123, 65, 115);
		let c18 = Rgb::new(165, 81, 148);
		let c19 = Rgb::new(206, 109, 189);
		let c20 = Rgb::new(222, 158, 214);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c05);
		set_color(map, TokenCategory::Comment, c16);
		set_color(map, TokenCategory::Type, c02);
		set_color(map, TokenCategory::Function, c06);
		set_color(map, TokenCategory::Number, c09);
		set_color(map, TokenCategory::Punctuation, c12);
		set_color(map, TokenCategory::BodyText, c12);
		set_color(map, TokenCategory::Variable, c07);
		set_color(map, TokenCategory::Constant, c13);
		set_color(map, TokenCategory::Macro, c14);
		set_color(map, TokenCategory::Module, c03);
		set_color(map, TokenCategory::Lifetime, c04);
		set_color(map, TokenCategory::Attribute, c10);
		set_color(map, TokenCategory::Operator, c11);
		set_color(map, TokenCategory::SelfKeyword, c17);
		set_color(map, TokenCategory::BuiltinType, c18);
		set_color(map, TokenCategory::MutableVariable, c08);
		set_color(map, TokenCategory::Method, c19);
		set_color(map, TokenCategory::Crate, c15);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_tab20c(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(49, 130, 189);
		let c02 = Rgb::new(107, 174, 214);
		let c03 = Rgb::new(158, 202, 225);
		let c04 = Rgb::new(198, 219, 239);
		let c05 = Rgb::new(230, 85, 13);
		let c06 = Rgb::new(253, 141, 60);
		let c07 = Rgb::new(253, 174, 107);
		let c08 = Rgb::new(253, 208, 162);
		let c09 = Rgb::new(49, 163, 84);
		let c10 = Rgb::new(116, 196, 118);
		let c11 = Rgb::new(161, 217, 155);
		let c12 = Rgb::new(199, 233, 192);
		let c13 = Rgb::new(117, 107, 177);
		let c14 = Rgb::new(158, 154, 200);
		let c15 = Rgb::new(188, 189, 220);
		let c16 = Rgb::new(218, 218, 235);
		let c17 = Rgb::new(166, 54, 3);
		let c18 = Rgb::new(0, 109, 44);
		let c19 = Rgb::new(63, 0, 125);
		let c20 = Rgb::new(150, 150, 150);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c09);
		set_color(map, TokenCategory::Comment, c20);
		set_color(map, TokenCategory::Type, c02);
		set_color(map, TokenCategory::Function, c10);
		set_color(map, TokenCategory::Number, c05);
		set_color(map, TokenCategory::Punctuation, c16);
		set_color(map, TokenCategory::BodyText, c16);
		set_color(map, TokenCategory::Variable, c11);
		set_color(map, TokenCategory::Constant, c17);
		set_color(map, TokenCategory::Macro, c06);
		set_color(map, TokenCategory::Module, c13);
		set_color(map, TokenCategory::Lifetime, c14);
		set_color(map, TokenCategory::Attribute, c18);
		set_color(map, TokenCategory::Operator, c07);
		set_color(map, TokenCategory::SelfKeyword, c19);
		set_color(map, TokenCategory::BuiltinType, c03);
		set_color(map, TokenCategory::MutableVariable, c08);
		set_color(map, TokenCategory::Method, c12);
		set_color(map, TokenCategory::Crate, c15);
		set_color(map, TokenCategory::Whitespace, c04);
	}

	fn fill_dark2(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let green = Rgb::new(27, 158, 119);
		let orange = Rgb::new(217, 95, 2);
		let purple = Rgb::new(117, 112, 179);
		let pink = Rgb::new(231, 41, 138);
		let lime = Rgb::new(102, 166, 30);
		let yellow = Rgb::new(230, 171, 2);
		let brown = Rgb::new(166, 118, 29);
		let gray = Rgb::new(102, 102, 102);

		set_color(map, TokenCategory::String, green);
		set_color(map, TokenCategory::Variable, orange);
		set_color(map, TokenCategory::MutableVariable, orange);
		set_color(map, TokenCategory::SelfKeyword, orange);
		set_color(map, TokenCategory::Number, purple);
		set_color(map, TokenCategory::Constant, purple);
		set_color(map, TokenCategory::Keyword, pink);
		set_color(map, TokenCategory::Operator, pink);
		set_color(map, TokenCategory::Macro, pink);
		set_color(map, TokenCategory::Type, lime);
		set_color(map, TokenCategory::BuiltinType, lime);
		set_color(map, TokenCategory::Function, yellow);
		set_color(map, TokenCategory::Method, yellow);
		set_color(map, TokenCategory::Module, brown);
		set_color(map, TokenCategory::Crate, brown);
		set_color(map, TokenCategory::Lifetime, brown);
		set_color(map, TokenCategory::Attribute, brown);
		set_color(map, TokenCategory::Comment, gray);
		set_color(map, TokenCategory::Punctuation, gray);
		set_color(map, TokenCategory::BodyText, gray);
		set_color(map, TokenCategory::Whitespace, Rgb::new(60, 60, 60));
	}

	fn fill_dracula(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let fg = Rgb::new(248, 248, 242);
		let comment = Rgb::new(98, 114, 164);
		let red = Rgb::new(255, 85, 85);
		let orange = Rgb::new(255, 184, 108);
		let yellow = Rgb::new(241, 250, 140);
		let green = Rgb::new(80, 250, 123);
		let purple = Rgb::new(189, 147, 249);
		let cyan = Rgb::new(139, 233, 253);
		let pink = Rgb::new(255, 121, 198);
		let bg = Rgb::new(40, 42, 54);

		set_color(map, TokenCategory::Keyword, pink);
		set_color(map, TokenCategory::String, yellow);
		set_color(map, TokenCategory::Comment, comment);
		set_color(map, TokenCategory::Type, cyan);
		set_color(map, TokenCategory::Function, green);
		set_color(map, TokenCategory::Number, purple);
		set_color(map, TokenCategory::Punctuation, fg);
		set_color(map, TokenCategory::BodyText, fg);
		set_color(map, TokenCategory::Variable, fg);
		set_color(map, TokenCategory::Constant, purple);
		set_color(map, TokenCategory::Macro, cyan);
		set_color(map, TokenCategory::Module, cyan);
		set_color(map, TokenCategory::Lifetime, red);
		set_color(map, TokenCategory::Attribute, green);
		set_color(map, TokenCategory::Operator, pink);
		set_color(map, TokenCategory::SelfKeyword, red);
		set_color(map, TokenCategory::BuiltinType, cyan);
		set_color(map, TokenCategory::MutableVariable, orange);
		set_color(map, TokenCategory::Method, green);
		set_color(map, TokenCategory::Crate, cyan);
		set_color(map, TokenCategory::Whitespace, bg);
	}

	fn fill_one_dark(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let red = Rgb::new(224, 108, 117);
		let green = Rgb::new(152, 195, 121);
		let yellow = Rgb::new(229, 192, 123);
		let blue = Rgb::new(97, 175, 239);
		let magenta = Rgb::new(198, 120, 221);
		let cyan = Rgb::new(86, 182, 194);
		let white = Rgb::new(171, 178, 191);
		let comment = Rgb::new(92, 99, 112);
		let orange = Rgb::new(209, 154, 102);

		set_color(map, TokenCategory::Keyword, magenta);
		set_color(map, TokenCategory::String, green);
		set_color(map, TokenCategory::Comment, comment);
		set_color(map, TokenCategory::Type, yellow);
		set_color(map, TokenCategory::Function, blue);
		set_color(map, TokenCategory::Number, orange);
		set_color(map, TokenCategory::Punctuation, white);
		set_color(map, TokenCategory::BodyText, white);
		set_color(map, TokenCategory::Variable, red);
		set_color(map, TokenCategory::Constant, orange);
		set_color(map, TokenCategory::Macro, cyan);
		set_color(map, TokenCategory::Module, blue);
		set_color(map, TokenCategory::Lifetime, magenta);
		set_color(map, TokenCategory::Attribute, white);
		set_color(map, TokenCategory::Operator, cyan);
		set_color(map, TokenCategory::SelfKeyword, red);
		set_color(map, TokenCategory::BuiltinType, cyan);
		set_color(map, TokenCategory::MutableVariable, orange);
		set_color(map, TokenCategory::Method, blue);
		set_color(map, TokenCategory::Crate, cyan);
		set_color(map, TokenCategory::Whitespace, Rgb::new(40, 44, 52));
	}

	fn fill_tokyo_night(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let fg = Rgb::new(192, 202, 245);
		let comment = Rgb::new(86, 95, 137);
		let cyan = Rgb::new(125, 207, 255);
		let blue = Rgb::new(122, 162, 247);
		let magenta = Rgb::new(187, 154, 247);
		let orange = Rgb::new(255, 158, 100);
		let green = Rgb::new(158, 206, 106);
		let red = Rgb::new(247, 118, 142);
		let bg = Rgb::new(26, 27, 38);

		set_color(map, TokenCategory::Keyword, magenta);
		set_color(map, TokenCategory::String, green);
		set_color(map, TokenCategory::Comment, comment);
		set_color(map, TokenCategory::Type, cyan);
		set_color(map, TokenCategory::Function, blue);
		set_color(map, TokenCategory::Number, orange);
		set_color(map, TokenCategory::Punctuation, fg);
		set_color(map, TokenCategory::BodyText, fg);
		set_color(map, TokenCategory::Variable, fg);
		set_color(map, TokenCategory::Constant, orange);
		set_color(map, TokenCategory::Macro, blue);
		set_color(map, TokenCategory::Module, blue);
		set_color(map, TokenCategory::Lifetime, red);
		set_color(map, TokenCategory::Attribute, cyan);
		set_color(map, TokenCategory::Operator, fg);
		set_color(map, TokenCategory::SelfKeyword, red);
		set_color(map, TokenCategory::BuiltinType, cyan);
		set_color(map, TokenCategory::MutableVariable, orange);
		set_color(map, TokenCategory::Method, blue);
		set_color(map, TokenCategory::Crate, blue);
		set_color(map, TokenCategory::Whitespace, bg);
	}

	fn fill_kanagawa(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let fuji_white = Rgb::new(220, 215, 186);
		let fuji_gray = Rgb::new(114, 113, 105);
		let oni_violet = Rgb::new(149, 127, 184);
		let spring_green = Rgb::new(152, 187, 108);
		let crystal_blue = Rgb::new(126, 156, 216);
		let wave_aqua = Rgb::new(122, 168, 159);
		let sakura_pink = Rgb::new(210, 126, 153);
		let surimi_orange = Rgb::new(255, 160, 102);
		let carp_yellow = Rgb::new(192, 163, 110);
		let sumi_ink = Rgb::new(84, 84, 100);

		set_color(map, TokenCategory::Keyword, oni_violet);
		set_color(map, TokenCategory::String, spring_green);
		set_color(map, TokenCategory::Comment, fuji_gray);
		set_color(map, TokenCategory::Type, wave_aqua);
		set_color(map, TokenCategory::Function, crystal_blue);
		set_color(map, TokenCategory::Number, sakura_pink);
		set_color(map, TokenCategory::Punctuation, sumi_ink);
		set_color(map, TokenCategory::BodyText, sumi_ink);
		set_color(map, TokenCategory::Variable, fuji_white);
		set_color(map, TokenCategory::Constant, surimi_orange);
		set_color(map, TokenCategory::Macro, oni_violet);
		set_color(map, TokenCategory::Module, crystal_blue);
		set_color(map, TokenCategory::Lifetime, surimi_orange);
		set_color(map, TokenCategory::Attribute, wave_aqua);
		set_color(map, TokenCategory::Operator, carp_yellow);
		set_color(map, TokenCategory::SelfKeyword, oni_violet);
		set_color(map, TokenCategory::BuiltinType, wave_aqua);
		set_color(map, TokenCategory::MutableVariable, surimi_orange);
		set_color(map, TokenCategory::Method, crystal_blue);
		set_color(map, TokenCategory::Crate, wave_aqua);
		set_color(map, TokenCategory::Whitespace, Rgb::new(31, 31, 40));
	}

	fn fill_glowmax(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(255, 0, 255);
		let c02 = Rgb::new(0, 255, 0);
		let c03 = Rgb::new(128, 128, 128);
		let c04 = Rgb::new(0, 255, 255);
		let c05 = Rgb::new(255, 255, 0);
		let c06 = Rgb::new(255, 128, 0);
		let c07 = Rgb::new(255, 255, 255);
		let c08 = Rgb::new(0, 127, 255);
		let c09 = Rgb::new(255, 0, 127);
		let c10 = Rgb::new(127, 255, 0);
		let c11 = Rgb::new(191, 0, 255);
		let c12 = Rgb::new(0, 255, 127);
		let c13 = Rgb::new(127, 0, 255);
		let c14 = Rgb::new(255, 191, 0);
		let c15 = Rgb::new(0, 191, 255);
		let c16 = Rgb::new(255, 0, 0);
		let c17 = Rgb::new(0, 255, 191);
		let c18 = Rgb::new(255, 255, 127);
		let c19 = Rgb::new(191, 191, 0);
		let c20 = Rgb::new(64, 64, 64);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_kelly(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(255, 179, 0);
		let c02 = Rgb::new(128, 62, 117);
		let c03 = Rgb::new(166, 189, 215);
		let c04 = Rgb::new(255, 104, 0);
		let c05 = Rgb::new(193, 0, 32);
		let c06 = Rgb::new(206, 162, 98);
		let c07 = Rgb::new(129, 112, 102);
		let c08 = Rgb::new(0, 125, 52);
		let c09 = Rgb::new(246, 118, 142);
		let c10 = Rgb::new(0, 83, 138);
		let c11 = Rgb::new(255, 122, 92);
		let c12 = Rgb::new(83, 55, 122);
		let c13 = Rgb::new(255, 142, 0);
		let c14 = Rgb::new(179, 40, 81);
		let c15 = Rgb::new(244, 200, 0);
		let c16 = Rgb::new(127, 24, 13);
		let c17 = Rgb::new(147, 170, 0);
		let c18 = Rgb::new(89, 51, 21);
		let c19 = Rgb::new(35, 44, 22);
		let c20 = Rgb::new(18, 18, 18);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_prism(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(255, 0, 64);
		let c02 = Rgb::new(0, 255, 128);
		let c03 = Rgb::new(76, 76, 76);
		let c04 = Rgb::new(0, 128, 255);
		let c05 = Rgb::new(255, 215, 0);
		let c06 = Rgb::new(255, 85, 0);
		let c07 = Rgb::new(224, 224, 224);
		let c08 = Rgb::new(128, 255, 0);
		let c09 = Rgb::new(255, 0, 255);
		let c10 = Rgb::new(0, 255, 255);
		let c11 = Rgb::new(123, 104, 238);
		let c12 = Rgb::new(154, 205, 50);
		let c13 = Rgb::new(186, 85, 211);
		let c14 = Rgb::new(255, 165, 0);
		let c15 = Rgb::new(205, 92, 92);
		let c16 = Rgb::new(32, 178, 170);
		let c17 = Rgb::new(218, 112, 214);
		let c18 = Rgb::new(184, 134, 11);
		let c19 = Rgb::new(95, 158, 160);
		let c20 = Rgb::new(26, 26, 26);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_turbo(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(233, 30, 99);
		let c02 = Rgb::new(76, 175, 80);
		let c03 = Rgb::new(117, 117, 117);
		let c04 = Rgb::new(33, 150, 243);
		let c05 = Rgb::new(255, 235, 59);
		let c06 = Rgb::new(156, 39, 176);
		let c07 = Rgb::new(189, 189, 189);
		let c08 = Rgb::new(244, 67, 54);
		let c09 = Rgb::new(255, 152, 0);
		let c10 = Rgb::new(0, 188, 212);
		let c11 = Rgb::new(63, 81, 181);
		let c12 = Rgb::new(0, 150, 136);
		let c13 = Rgb::new(103, 58, 183);
		let c14 = Rgb::new(255, 193, 7);
		let c15 = Rgb::new(205, 220, 57);
		let c16 = Rgb::new(3, 169, 244);
		let c17 = Rgb::new(139, 195, 74);
		let c18 = Rgb::new(255, 87, 34);
		let c19 = Rgb::new(96, 125, 139);
		let c20 = Rgb::new(38, 50, 56);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_amazon(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(255, 69, 0);
		let c02 = Rgb::new(50, 205, 50);
		let c03 = Rgb::new(47, 79, 79);
		let c04 = Rgb::new(30, 144, 255);
		let c05 = Rgb::new(255, 215, 0);
		let c06 = Rgb::new(139, 69, 19);
		let c07 = Rgb::new(255, 105, 180);
		let c08 = Rgb::new(0, 250, 154);
		let c09 = Rgb::new(218, 112, 214);
		let c10 = Rgb::new(238, 232, 170);
		let c11 = Rgb::new(64, 224, 208);
		let c12 = Rgb::new(255, 140, 0);
		let c13 = Rgb::new(147, 112, 219);
		let c14 = Rgb::new(0, 206, 209);
		let c15 = Rgb::new(173, 255, 47);
		let c16 = Rgb::new(255, 20, 147);
		let c17 = Rgb::new(123, 104, 238);
		let c18 = Rgb::new(240, 230, 140);
		let c19 = Rgb::new(34, 139, 34);
		let c20 = Rgb::new(18, 18, 18);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_arcade(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(230, 0, 18);
		let c02 = Rgb::new(0, 103, 184);
		let c03 = Rgb::new(64, 64, 64);
		let c04 = Rgb::new(249, 215, 28);
		let c05 = Rgb::new(0, 255, 0);
		let c06 = Rgb::new(255, 0, 255);
		let c07 = Rgb::new(0, 255, 255);
		let c08 = Rgb::new(255, 255, 255);
		let c09 = Rgb::new(255, 165, 0);
		let c10 = Rgb::new(128, 0, 128);
		let c11 = Rgb::new(0, 0, 255);
		let c12 = Rgb::new(0, 128, 0);
		let c13 = Rgb::new(165, 42, 42);
		let c14 = Rgb::new(128, 128, 128);
		let c15 = Rgb::new(255, 192, 203);
		let c16 = Rgb::new(255, 255, 0);
		let c17 = Rgb::new(0, 0, 128);
		let c18 = Rgb::new(128, 128, 0);
		let c19 = Rgb::new(0, 128, 128);
		let c20 = Rgb::new(18, 18, 18);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_nebula(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(45, 0, 70);
		let c02 = Rgb::new(234, 234, 234);
		let c03 = Rgb::new(100, 100, 100);
		let c04 = Rgb::new(192, 38, 211);
		let c05 = Rgb::new(37, 99, 235);
		let c06 = Rgb::new(249, 115, 22);
		let c07 = Rgb::new(75, 0, 130);
		let c08 = Rgb::new(255, 0, 255);
		let c09 = Rgb::new(0, 255, 255);
		let c10 = Rgb::new(255, 215, 0);
		let c11 = Rgb::new(255, 69, 0);
		let c12 = Rgb::new(148, 0, 211);
		let c13 = Rgb::new(0, 206, 209);
		let c14 = Rgb::new(255, 20, 147);
		let c15 = Rgb::new(127, 255, 0);
		let c16 = Rgb::new(30, 144, 255);
		let c17 = Rgb::new(255, 140, 0);
		let c18 = Rgb::new(186, 85, 211);
		let c19 = Rgb::new(0, 250, 154);
		let c20 = Rgb::new(18, 18, 18);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}

	fn fill_vapor(map: &mut [Option<Rgb>; CATEGORY_COUNT]) {
		let c01 = Rgb::new(255, 113, 206);
		let c02 = Rgb::new(1, 205, 254);
		let c03 = Rgb::new(80, 80, 80);
		let c04 = Rgb::new(255, 251, 150);
		let c05 = Rgb::new(185, 103, 255);
		let c06 = Rgb::new(5, 255, 161);
		let c07 = Rgb::new(255, 255, 255);
		let c08 = Rgb::new(255, 32, 121);
		let c09 = Rgb::new(8, 247, 254);
		let c10 = Rgb::new(254, 83, 187);
		let c11 = Rgb::new(245, 211, 0);
		let c12 = Rgb::new(113, 34, 250);
		let c13 = Rgb::new(255, 124, 0);
		let c14 = Rgb::new(19, 202, 145);
		let c15 = Rgb::new(255, 148, 114);
		let c16 = Rgb::new(182, 255, 0);
		let c17 = Rgb::new(229, 9, 20);
		let c18 = Rgb::new(157, 0, 255);
		let c19 = Rgb::new(0, 255, 159);
		let c20 = Rgb::new(18, 18, 18);

		set_color(map, TokenCategory::Keyword, c01);
		set_color(map, TokenCategory::String, c02);
		set_color(map, TokenCategory::Comment, c03);
		set_color(map, TokenCategory::Type, c04);
		set_color(map, TokenCategory::Function, c05);
		set_color(map, TokenCategory::Number, c06);
		set_color(map, TokenCategory::Punctuation, c07);
		set_color(map, TokenCategory::BodyText, c07);
		set_color(map, TokenCategory::Variable, c08);
		set_color(map, TokenCategory::Constant, c09);
		set_color(map, TokenCategory::Macro, c10);
		set_color(map, TokenCategory::Module, c11);
		set_color(map, TokenCategory::Lifetime, c12);
		set_color(map, TokenCategory::Attribute, c13);
		set_color(map, TokenCategory::Operator, c14);
		set_color(map, TokenCategory::SelfKeyword, c15);
		set_color(map, TokenCategory::BuiltinType, c16);
		set_color(map, TokenCategory::MutableVariable, c17);
		set_color(map, TokenCategory::Method, c18);
		set_color(map, TokenCategory::Crate, c19);
		set_color(map, TokenCategory::Whitespace, c20);
	}
}

fn lerp(a: f32, b: f32, t: f32) -> f32 {
	a + (b - a) * t
}

fn map_color(t: f32, colors: &[(f32, f32, f32)]) -> Rgb {
	let t = t.clamp(0.0, 1.0) * (colors.len() - 1) as f32;
	let i = t.floor() as usize;
	let frac = t.fract();
	if i >= colors.len() - 1 {
		let (r, g, b) = colors[colors.len() - 1];
		return Rgb::new((r * 255.0) as u8, (g * 255.0) as u8, (b * 255.0) as u8);
	}
	let (r1, g1, b1) = colors[i];
	let (r2, g2, b2) = colors[i + 1];
	Rgb::new(
		(lerp(r1, r2, frac) * 255.0) as u8,
		(lerp(g1, g2, frac) * 255.0) as u8,
		(lerp(b1, b2, frac) * 255.0) as u8,
	)
}

// --- Colormap Definitions from Polariz Reference ---

pub fn rainbow(t: f32) -> Rgb {
	let h = t.clamp(0.0, 1.0) * 5.0;
	let x = 1.0 - (h % 2.0 - 1.0).abs();
	let (r, g, b) = match h as i32 {
		0 => (1.0, x, 0.0),
		1 => (x, 1.0, 0.0),
		2 => (0.0, 1.0, x),
		3 => (0.0, x, 1.0),
		_ => (x, 0.0, 1.0),
	};
	Rgb::new((r * 255.0) as u8, (g * 255.0) as u8, (b * 255.0) as u8)
}

pub fn greys(t: f32) -> Rgb {
	let v = (t * 255.0) as u8;
	Rgb::new(v, v, v)
}

pub fn black(_t: f32) -> Rgb {
	Rgb::new(0, 0, 0)
}

pub fn grey(_t: f32) -> Rgb {
	Rgb::new(128, 128, 128)
}

pub fn white(_t: f32) -> Rgb {
	Rgb::new(255, 255, 255)
}

pub fn viridis(t: f32) -> Rgb {
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

pub fn inferno(t: f32) -> Rgb {
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

pub fn magma(t: f32) -> Rgb {
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

pub fn plasma(t: f32) -> Rgb {
	map_color(
		t,
		&[
			(0.050, 0.030, 0.528),
			(0.493, 0.012, 0.658),
			(0.796, 0.279, 0.471),
			(0.953, 0.675, 0.242),
			(0.940, 0.975, 0.131),
		],
	)
}

pub fn cividis(t: f32) -> Rgb {
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

pub fn reds(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.99, 0.88, 0.82), (0.98, 0.38, 0.28), (0.40, 0.00, 0.05)],
	)
}

pub fn yellows(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.99, 0.99, 0.80), (0.99, 0.85, 0.20), (0.60, 0.40, 0.00)],
	)
}

pub fn blues(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.93, 0.95, 0.98), (0.42, 0.68, 0.84), (0.03, 0.19, 0.42)],
	)
}

pub fn oranges(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.99, 0.91, 0.80), (0.99, 0.55, 0.15), (0.50, 0.15, 0.00)],
	)
}

pub fn purples(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.95, 0.94, 0.97), (0.62, 0.57, 0.76), (0.25, 0.00, 0.49)],
	)
}

pub fn greens(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.93, 0.98, 0.90), (0.25, 0.70, 0.35), (0.00, 0.27, 0.08)],
	)
}

pub fn red_blue(t: f32) -> Rgb {
	map_color(t, &[(1.0, 0.0, 0.0), (0.6, 0.2, 0.8), (0.0, 0.0, 1.0)])
}

pub fn green_blue(t: f32) -> Rgb {
	map_color(t, &[(0.0, 1.0, 0.0), (0.0, 1.0, 1.0), (0.0, 0.0, 1.0)])
}

pub fn red_green(t: f32) -> Rgb {
	map_color(t, &[(1.0, 0.0, 0.0), (1.0, 1.0, 0.0), (0.0, 1.0, 0.0)])
}

pub fn red_white_blue(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.70, 0.01, 0.15), (1.00, 1.00, 1.00), (0.02, 0.19, 0.46)],
	)
}

pub fn green_purples(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.15, 0.44, 0.12), (1.00, 1.00, 1.00), (0.35, 0.11, 0.48)],
	)
}

pub fn blue_green(t: f32) -> Rgb {
	map_color(t, &[(0.0, 0.0, 1.0), (0.0, 1.0, 1.0), (0.0, 1.0, 0.0)])
}

pub fn blue_red(t: f32) -> Rgb {
	map_color(t, &[(0.0, 0.0, 1.0), (0.6, 0.2, 0.8), (1.0, 0.0, 0.0)])
}

pub fn blue_white_red(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.02, 0.19, 0.46), (1.00, 1.00, 1.00), (0.70, 0.01, 0.15)],
	)
}

pub fn green_red(t: f32) -> Rgb {
	map_color(t, &[(0.0, 1.0, 0.0), (1.0, 1.0, 0.0), (1.0, 0.0, 0.0)])
}

pub fn purple_greens(t: f32) -> Rgb {
	map_color(
		t,
		&[(0.35, 0.11, 0.48), (1.00, 1.00, 1.00), (0.15, 0.44, 0.12)],
	)
}

pub fn ocean(t: f32) -> Rgb {
	map_color(
		t,
		&[
			(0.0, 0.0, 0.0),
			(0.0, 0.0, 0.5),
			(0.0, 0.5, 0.5),
			(0.0, 1.0, 0.5),
			(1.0, 1.0, 1.0),
		],
	)
}

pub fn terrain(t: f32) -> Rgb {
	map_color(
		t,
		&[
			(0.2, 0.2, 0.6),
			(0.0, 0.6, 1.0),
			(0.0, 0.8, 0.4),
			(1.0, 1.0, 0.6),
			(0.5, 0.36, 0.33),
			(1.0, 1.0, 1.0),
		],
	)
}

pub fn spring(t: f32) -> Rgb {
	let s = t.clamp(0.0, 1.0);
	Rgb::new(255, (s * 255.0) as u8, (255.0 - s * 255.0) as u8)
}

pub fn summer(t: f32) -> Rgb {
	let s = t.clamp(0.0, 1.0);
	Rgb::new(
		0,
		((0.5 + s * 0.5) * 255.0) as u8,
		((0.4 + s * 0.2) * 255.0) as u8,
	)
}

pub fn autumn(t: f32) -> Rgb {
	let s = t.clamp(0.0, 1.0);
	Rgb::new(255, (s * 255.0) as u8, 0)
}

pub fn winter(t: f32) -> Rgb {
	let s = t.clamp(0.0, 1.0);
	Rgb::new(0, (s * 255.0) as u8, ((1.0 - s * 0.5) * 255.0) as u8)
}

type ThemeColors = [Option<Rgb>; CATEGORY_COUNT];

#[derive(Clone, Copy)]
enum ThemePalette {
	Discrete(fn(&mut ThemeColors)),
	Continuous(fn(f32) -> Rgb),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ThemeFamily {
	Curated,
	Continuous,
}

pub struct ThemeSpec {
	pub name: &'static str,
	pub aliases: &'static [&'static str],
	palette: ThemePalette,
}

impl ThemeSpec {
	const fn curated(
		name: &'static str,
		aliases: &'static [&'static str],
		fill: fn(&mut ThemeColors),
	) -> Self {
		Self {
			name,
			aliases,
			palette: ThemePalette::Discrete(fill),
		}
	}

	const fn continuous(
		name: &'static str,
		aliases: &'static [&'static str],
		sample: fn(f32) -> Rgb,
	) -> Self {
		Self {
			name,
			aliases,
			palette: ThemePalette::Continuous(sample),
		}
	}

	pub const fn family(&self) -> ThemeFamily {
		match self.palette {
			ThemePalette::Discrete(_) => ThemeFamily::Curated,
			ThemePalette::Continuous(_) => ThemeFamily::Continuous,
		}
	}
}

pub static THEME_SPECS: &[ThemeSpec] = &[
	ThemeSpec::curated("amazon", &[], Theme::fill_amazon),
	ThemeSpec::curated("arcade", &[], Theme::fill_arcade),
	ThemeSpec::curated("classic", &["original"], Theme::fill_classic),
	ThemeSpec::curated("dark2", &[], Theme::fill_dark2),
	ThemeSpec::curated("dracula", &[], Theme::fill_dracula),
	ThemeSpec::curated("glowmax", &[], Theme::fill_glowmax),
	ThemeSpec::curated("gruvbox", &[], Theme::fill_gruvbox),
	ThemeSpec::curated("kanagawa", &[], Theme::fill_kanagawa),
	ThemeSpec::curated("kelly", &[], Theme::fill_kelly),
	ThemeSpec::curated("nebula", &[], Theme::fill_nebula),
	ThemeSpec::curated("nord", &[], Theme::fill_nord),
	ThemeSpec::curated("onedark", &["one_dark"], Theme::fill_one_dark),
	ThemeSpec::curated("pretty", &[], Theme::fill_pretty),
	ThemeSpec::curated("prism", &[], Theme::fill_prism),
	ThemeSpec::curated("solarized", &["solarized_dark"], Theme::fill_solarized),
	ThemeSpec::curated("tab20", &[], Theme::fill_tab20),
	ThemeSpec::curated("tab20b", &[], Theme::fill_tab20b),
	ThemeSpec::curated("tab20c", &[], Theme::fill_tab20c),
	ThemeSpec::curated("tokyonight", &["tokyo_night"], Theme::fill_tokyo_night),
	ThemeSpec::curated("turbo", &[], Theme::fill_turbo),
	ThemeSpec::curated("vapor", &[], Theme::fill_vapor),
	ThemeSpec::continuous("autumn", &[], autumn),
	ThemeSpec::continuous("black", &[], black),
	ThemeSpec::continuous("bluegreen", &[], blue_green),
	ThemeSpec::continuous("bluered", &[], blue_red),
	ThemeSpec::continuous("blues", &[], blues),
	ThemeSpec::continuous("bluewhitered", &[], blue_white_red),
	ThemeSpec::continuous("cividis", &[], cividis),
	ThemeSpec::continuous("greenblue", &[], green_blue),
	ThemeSpec::continuous("greenpurples", &[], green_purples),
	ThemeSpec::continuous("greenred", &[], green_red),
	ThemeSpec::continuous("greens", &[], greens),
	ThemeSpec::continuous("grey", &["gray"], grey),
	ThemeSpec::continuous("greys", &["grays"], greys),
	ThemeSpec::continuous("inferno", &[], inferno),
	ThemeSpec::continuous("magma", &[], magma),
	ThemeSpec::continuous("ocean", &[], ocean),
	ThemeSpec::continuous("oranges", &[], oranges),
	ThemeSpec::continuous("plasma", &[], plasma),
	ThemeSpec::continuous("purplegreens", &[], purple_greens),
	ThemeSpec::continuous("purples", &[], purples),
	ThemeSpec::continuous("rainbow", &[], rainbow),
	ThemeSpec::continuous("redblue", &[], red_blue),
	ThemeSpec::continuous("redgreen", &[], red_green),
	ThemeSpec::continuous("reds", &[], reds),
	ThemeSpec::continuous("redwhiteblue", &[], red_white_blue),
	ThemeSpec::continuous("spring", &[], spring),
	ThemeSpec::continuous("summer", &[], summer),
	ThemeSpec::continuous("terrain", &[], terrain),
	ThemeSpec::continuous("viridis", &[], viridis),
	ThemeSpec::continuous("white", &[], white),
	ThemeSpec::continuous("winter", &[], winter),
	ThemeSpec::continuous("yellows", &[], yellows),
];

fn theme_spec(name: &str) -> Option<&'static ThemeSpec> {
	THEME_SPECS
		.iter()
		.find(|spec| name == spec.name || spec.aliases.contains(&name))
}

pub fn canonical_theme_names() -> impl Iterator<Item = &'static str> {
	THEME_SPECS.iter().map(|spec| spec.name)
}

#[cfg(test)]
mod tests {
	use super::{
		ResolvedThemePalette, THEME_SPECS, THEMED_CATEGORIES, Theme, canonical_theme_names, plasma,
		viridis,
	};
	use crate::core::Rgb;
	use crate::svp::rust_highlight::{CATEGORY_COUNT, TokenCategory};
	use std::collections::HashSet;

	const PRODUCT_0024_SYNTAX_SNAPSHOT: &str =
		include_str!("snapshots/product-0024-theme-syntax.txt");

	const ALL_CATEGORIES: [TokenCategory; CATEGORY_COUNT] = [
		TokenCategory::Keyword,
		TokenCategory::String,
		TokenCategory::Comment,
		TokenCategory::Type,
		TokenCategory::Function,
		TokenCategory::Number,
		TokenCategory::Punctuation,
		TokenCategory::Variable,
		TokenCategory::Constant,
		TokenCategory::Macro,
		TokenCategory::Module,
		TokenCategory::Lifetime,
		TokenCategory::Attribute,
		TokenCategory::Operator,
		TokenCategory::SelfKeyword,
		TokenCategory::BuiltinType,
		TokenCategory::MutableVariable,
		TokenCategory::Method,
		TokenCategory::Crate,
		TokenCategory::Whitespace,
		TokenCategory::Unclassified,
		TokenCategory::GitStaged,
		TokenCategory::GitUnstaged,
		TokenCategory::GitUntracked,
		TokenCategory::GitRename,
		TokenCategory::BodyText,
	];

	fn color_for(theme: &Theme, category: TokenCategory) -> Rgb {
		theme.syntax_colors[category as usize].expect("themed color")
	}

	fn product_0024_theme_expressions() -> Vec<String> {
		let mut expressions = Vec::new();
		for spec in THEME_SPECS {
			for name in std::iter::once(spec.name).chain(spec.aliases.iter().copied()) {
				expressions.push(name.to_string());
				expressions.push(format!("{name}_r"));
				expressions.push(format!("{name}_c"));
				expressions.push(format!("{name}_C"));
				for shift in 0..THEMED_CATEGORIES.len() {
					expressions.push(format!("{name}_s{shift}"));
				}
			}
		}
		expressions.extend(
			["viridis_cCr", "viridis_rcC", "classic_rC", "glowmax_rs1"]
				.into_iter()
				.map(str::to_string),
		);
		expressions
	}

	fn product_0024_serialize_theme_syntax() -> String {
		let mut snapshot = String::new();
		for expression in product_0024_theme_expressions() {
			let theme = Theme::try_new(&expression)
				.unwrap_or_else(|error| panic!("snapshot expression {expression}: {error}"));
			snapshot.push_str(&expression);
			for category in ALL_CATEGORIES {
				snapshot.push('\t');
				snapshot.push_str(&format!("{category:?}="));
				match theme.syntax_colors[category as usize] {
					Some(color) => snapshot.push_str(&color.to_hex()),
					None => snapshot.push('-'),
				}
			}
			snapshot.push('\n');
		}
		snapshot
	}

	#[test]
	fn product_0024_complete_pre_refactor_theme_syntax_oracle() {
		let actual = product_0024_serialize_theme_syntax();
		if std::env::var_os("UPDATE_PRODUCT_0024_THEME_SYNTAX").is_some() {
			std::fs::write(
				concat!(
					env!("CARGO_MANIFEST_DIR"),
					"/src/engine/snapshots/product-0024-theme-syntax.txt"
				),
				&actual,
			)
			.expect("write PRODUCT-0024 theme syntax snapshot");
			return;
		}
		assert_eq!(actual, PRODUCT_0024_SYNTAX_SNAPSHOT);
	}

	#[test]
	fn product_0024_records_the_existing_modifier_applicability_matrix() {
		let nord = Theme::try_new("nord").expect("nord theme").syntax_colors;
		assert_eq!(Theme::try_new("nord_c").unwrap().syntax_colors, nord);
		assert_eq!(Theme::try_new("nord_C").unwrap().syntax_colors, nord);

		let terrain = Theme::try_new("terrain").unwrap().syntax_colors;
		assert_ne!(Theme::try_new("terrain_c").unwrap().syntax_colors, terrain);
		assert_ne!(Theme::try_new("terrain_C").unwrap().syntax_colors, terrain);
	}

	#[test]
	fn product_0024_resolved_palette_is_fixed_complete_and_immutable() {
		let colors = [Rgb::new(17, 34, 51); 20];
		let palette = ResolvedThemePalette(colors);
		assert_eq!(palette.colors(), &colors);
		assert_eq!(palette.color(0), Some(colors[0]));
		assert_eq!(palette.color(19), Some(colors[19]));
		assert_eq!(palette.color(20), None);
		assert_eq!(std::mem::size_of::<ResolvedThemePalette>(), 20 * 3);
	}

	#[test]
	fn product_0024_contrast_pair_chooses_unique_maximum_and_orients_by_luminance() {
		let gray = Rgb::new(127, 127, 127);
		let mut colors = [gray; 20];
		colors[2] = Rgb::new(255, 255, 255);
		colors[17] = Rgb::new(0, 0, 0);
		let palette = ResolvedThemePalette(colors);
		let pair = palette.most_contrasting_pair();

		assert_eq!(pair.darker_index(), 17);
		assert_eq!(pair.darker(), colors[17]);
		assert_eq!(pair.lighter_index(), 2);
		assert_eq!(pair.lighter(), colors[2]);
		assert_eq!(pair.contrast_ratio(), 21.0);
	}

	#[test]
	fn product_0024_contrast_pair_breaks_maximum_ties_by_source_indices() {
		let gray = Rgb::new(127, 127, 127);
		let mut colors = [gray; 20];
		colors[2] = Rgb::new(0, 0, 0);
		colors[3] = Rgb::new(0, 0, 0);
		colors[7] = Rgb::new(255, 255, 255);
		colors[8] = Rgb::new(255, 255, 255);
		let palette = ResolvedThemePalette(colors);
		let pair = palette.most_contrasting_pair();

		assert_eq!((pair.darker_index(), pair.lighter_index()), (2, 7));
		assert_eq!(pair.darker(), colors[2]);
		assert_eq!(pair.lighter(), colors[7]);
	}

	#[test]
	fn product_0024_flat_palette_is_valid_and_orients_equal_luminance_by_lower_index() {
		let color = Rgb::new(73, 73, 73);
		let palette = ResolvedThemePalette([color; 20]);
		let pair = palette.most_contrasting_pair();

		assert_eq!((pair.darker_index(), pair.lighter_index()), (0, 1));
		assert_eq!(pair.darker(), color);
		assert_eq!(pair.lighter(), color);
		assert_eq!(pair.contrast_ratio(), 1.0);
	}

	#[test]
	fn registry_names_and_aliases_are_unique_and_instantiable() {
		let mut names = HashSet::new();
		for spec in THEME_SPECS {
			assert!(names.insert(spec.name), "duplicate theme {}", spec.name);
			assert!(
				Theme::try_new(spec.name).is_ok(),
				"invalid theme {}",
				spec.name
			);
			assert!(
				Theme::try_new(&format!("{}_r", spec.name)).is_ok(),
				"theme {} rejects modifiers",
				spec.name
			);
			for alias in spec.aliases {
				assert!(names.insert(*alias), "duplicate theme alias {alias}");
				assert!(Theme::try_new(alias).is_ok(), "invalid alias {alias}");
				assert!(
					Theme::try_new(&format!("{alias}_r")).is_ok(),
					"alias {alias} rejects modifiers"
				);
			}
		}
		assert_eq!(
			canonical_theme_names().collect::<Vec<_>>(),
			THEME_SPECS.iter().map(|spec| spec.name).collect::<Vec<_>>()
		);
	}

	#[test]
	fn continuous_themes_default_to_full_domain() {
		let theme = Theme::try_new("plasma").expect("plasma theme");

		assert_eq!(color_for(&theme, TokenCategory::Keyword), plasma(0.0));
		assert_eq!(color_for(&theme, TokenCategory::Whitespace), plasma(1.0));
	}

	#[test]
	fn unordered_clip_and_reverse_modifiers_compose_after_sampling() {
		let theme = Theme::try_new("viridis_cCr").expect("viridis_cCr theme");
		let permuted = Theme::try_new("viridis_rcC").expect("viridis_rcC theme");

		assert_eq!(color_for(&theme, TokenCategory::Keyword), viridis(0.8));
		assert_eq!(color_for(&theme, TokenCategory::Whitespace), viridis(0.2));
		assert_eq!(theme.syntax_colors, permuted.syntax_colors);
	}

	#[test]
	fn discrete_themes_ignore_clip_modifiers_but_still_reverse() {
		let reversed = Theme::try_new("classic_r").expect("classic_r theme");
		let clipped_reversed = Theme::try_new("classic_rC").expect("classic_rC theme");

		assert_eq!(reversed.syntax_colors, clipped_reversed.syntax_colors);
	}

	#[test]
	fn new_discrete_themes_can_be_instantiated() {
		assert!(Theme::try_new("amazon").is_ok());
		assert!(Theme::try_new("arcade").is_ok());
		assert!(Theme::try_new("glowmax").is_ok());
		assert!(Theme::try_new("kelly").is_ok());
		assert!(Theme::try_new("nebula").is_ok());
		assert!(Theme::try_new("prism").is_ok());
		assert!(Theme::try_new("turbo").is_ok());
		assert!(Theme::try_new("vapor").is_ok());
	}

	#[test]
	fn shift_modifier_rotates_discrete_colors() {
		let t0 = Theme::try_new("glowmax").unwrap();
		let t1 = Theme::try_new("glowmax_s1").unwrap();
		let t20 = Theme::try_new("glowmax_s20").unwrap();

		// t20 should be identical to t0 (20 categories)
		assert_eq!(t0.syntax_colors, t20.syntax_colors);

		// t1[i] should be t0[i-1] (rotated right)
		// Keyword is first, String is second.
		assert_eq!(
			t1.syntax_colors[TokenCategory::String as usize],
			t0.syntax_colors[TokenCategory::Keyword as usize]
		);
	}

	#[test]
	fn shift_and_reverse_compose() {
		let trs = Theme::try_new("glowmax_rs1").unwrap();
		let mut expected = Theme::try_new("glowmax").unwrap().syntax_colors;

		// Manual expectation: reverse then rotate_right(1)
		let assigned_cats: Vec<TokenCategory> = THEMED_CATEGORIES.to_vec();
		let mut colors: Vec<Rgb> = assigned_cats
			.iter()
			.map(|&cat| expected[cat as usize].unwrap())
			.collect();
		colors.reverse();
		colors.rotate_right(1);

		for (cat, color) in assigned_cats.into_iter().zip(colors) {
			expected[cat as usize] = Some(color);
		}

		assert_eq!(trs.syntax_colors, expected);
	}

	#[test]
	fn rev_suffix_is_no_longer_supported() {
		assert!(Theme::try_new("viridis_rev").is_err());
	}
}
