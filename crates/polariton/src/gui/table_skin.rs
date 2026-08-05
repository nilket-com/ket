//! Complete, resolved color treatment for the interior of a table artifact.
//!
//! This model is deliberately independent of editor chrome and plot data ramps.

use libaryon::core::{Rgb, Srgb};
use libaryon::engine::{ResolvedThemePalette, Theme};

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TableRgba {
	red: u8,
	green: u8,
	blue: u8,
	alpha: f32,
}

impl TableRgba {
	pub const fn opaque(red: u8, green: u8, blue: u8) -> Self {
		Self {
			red,
			green,
			blue,
			alpha: 1.0,
		}
	}

	pub const fn translucent(red: u8, green: u8, blue: u8, alpha: f32) -> Self {
		Self {
			red,
			green,
			blue,
			alpha,
		}
	}

	pub const fn red(self) -> u8 {
		self.red
	}

	pub const fn green(self) -> u8 {
		self.green
	}

	pub const fn blue(self) -> u8 {
		self.blue
	}

	pub const fn alpha(self) -> f32 {
		self.alpha
	}

	const fn opaque_rgb(color: Rgb) -> Self {
		Self::opaque(color.r, color.g, color.b)
	}

	const fn translucent_rgb(color: Rgb, alpha: f32) -> Self {
		Self::translucent(color.r, color.g, color.b, alpha)
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct TableSkin {
	pub body_background: TableRgba,
	pub header_background: TableRgba,
	pub gutter_background: TableRgba,
	pub row_even_background: TableRgba,
	pub row_odd_background: TableRgba,
	pub gridline: TableRgba,
	pub cell_text: TableRgba,
	pub header_text: TableRgba,
	pub type_text: TableRgba,
	pub gutter_text: TableRgba,
	pub selection_fill: TableRgba,
	pub scrollbar_thumb: TableRgba,
}

impl TableSkin {
	pub const DEFAULT: Self = Self {
		body_background: TableRgba::opaque(1, 1, 1),
		header_background: TableRgba::opaque(4, 4, 4),
		gutter_background: TableRgba::opaque(4, 4, 4),
		row_even_background: TableRgba::opaque(20, 8, 38),
		row_odd_background: TableRgba::opaque(40, 20, 70),
		gridline: TableRgba::translucent(250, 250, 250, 0.1),
		cell_text: TableRgba::opaque(230, 230, 230),
		header_text: TableRgba::opaque(220, 220, 220),
		type_text: TableRgba::opaque(180, 150, 220),
		gutter_text: TableRgba::opaque(220, 220, 220),
		selection_fill: TableRgba::translucent(140, 0, 250, 0.25),
		scrollbar_thumb: TableRgba::translucent(140, 0, 250, 0.5),
	};

	/// Every statically constructed production skin participates in the contrast
	/// assay. Theme-projected skins are validated before construction returns.
	pub const BUILT_INS: [Self; 1] = [Self::DEFAULT];

	pub fn from_theme_name(name: &str) -> Result<Self, TableSkinProjectionError> {
		let theme = Theme::try_new(name).map_err(|_| TableSkinProjectionError::UnknownTheme {
			name: name.to_owned(),
		})?;
		let skin = project_theme_palette(theme.palette());
		validate_projected_skin(name, skin)?;
		Ok(skin)
	}
}

impl Default for TableSkin {
	fn default() -> Self {
		Self::DEFAULT
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableSkinProjectionError {
	UnknownTheme {
		name: String,
	},
	UnsuitableTheme {
		name: String,
		role: &'static str,
		ratio: f64,
		minimum: f64,
	},
}

impl std::fmt::Display for TableSkinProjectionError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::UnknownTheme { name } => write!(formatter, "unknown table skin theme {name:?}"),
			Self::UnsuitableTheme {
				name,
				role,
				ratio,
				minimum,
			} => write!(
				formatter,
				"theme {name:?} is unsuitable for table skin role {role}: {ratio:.3}:1 is below {minimum:.1}:1"
			),
		}
	}
}

impl std::error::Error for TableSkinProjectionError {}

const TABLE_TEXT_CONTRAST_MINIMUM: f64 = 4.5;
const TABLE_TEXT_CONTRAST_ROLES: [&str; 7] = [
	"cell text on even row",
	"cell text on odd row",
	"header text on header",
	"type text on header",
	"gutter text on gutter",
	"cell text on selected even row",
	"cell text on selected odd row",
];

fn mix_encoded_srgb(base: Rgb, accent: Rgb, amount: f64) -> Rgb {
	debug_assert!((0.0..=1.0).contains(&amount));
	let channel = |base: u8, accent: u8| {
		(base as f64 * (1.0 - amount) + accent as f64 * amount).round() as u8
	};
	Rgb::new(
		channel(base.r, accent.r),
		channel(base.g, accent.g),
		channel(base.b, accent.b),
	)
}

fn project_theme_palette(palette: &ResolvedThemePalette) -> TableSkin {
	let colors = palette.colors();
	let anchors = palette.most_contrasting_pair();
	let dark = anchors.darker();
	let light = anchors.lighter();
	let header = mix_encoded_srgb(dark, colors[2], 0.08);
	let readable_type = colors
		.iter()
		.copied()
		.find(|color| color.contrast_ratio(header) >= TABLE_TEXT_CONTRAST_MINIMUM)
		.unwrap_or(light);

	TableSkin {
		body_background: TableRgba::opaque_rgb(dark),
		header_background: TableRgba::opaque_rgb(header),
		gutter_background: TableRgba::opaque_rgb(mix_encoded_srgb(dark, colors[3], 0.06)),
		row_even_background: TableRgba::opaque_rgb(mix_encoded_srgb(dark, colors[0], 0.03)),
		row_odd_background: TableRgba::opaque_rgb(mix_encoded_srgb(dark, colors[1], 0.07)),
		gridline: TableRgba::translucent_rgb(colors[4], 0.25),
		cell_text: TableRgba::opaque_rgb(light),
		header_text: TableRgba::opaque_rgb(light),
		type_text: TableRgba::opaque_rgb(readable_type),
		gutter_text: TableRgba::opaque_rgb(light),
		selection_fill: TableRgba::translucent_rgb(colors[5], 0.18),
		scrollbar_thumb: TableRgba::translucent_rgb(colors[6], 0.60),
	}
}

fn visible_srgb(color: TableRgba) -> Srgb {
	Rgb::new(color.red, color.green, color.blue).as_srgb()
}

fn composite_over(foreground: TableRgba, background: Srgb) -> Srgb {
	let alpha = foreground.alpha as f64;
	let channel = |foreground: u8, background: f64| {
		foreground as f64 / 255.0 * alpha + background * (1.0 - alpha)
	};
	Srgb::new(
		channel(foreground.red, background.red()),
		channel(foreground.green, background.green()),
		channel(foreground.blue, background.blue()),
	)
	.expect("validated table colors composite into normalized sRGB")
}

fn text_contrast_ratio(text: TableRgba, background: Srgb) -> f64 {
	composite_over(text, background).contrast_ratio(background)
}

fn text_contrast_ratios(skin: TableSkin) -> [f64; 7] {
	let even = visible_srgb(skin.row_even_background);
	let odd = visible_srgb(skin.row_odd_background);
	let header = visible_srgb(skin.header_background);
	let gutter = visible_srgb(skin.gutter_background);
	[
		text_contrast_ratio(skin.cell_text, even),
		text_contrast_ratio(skin.cell_text, odd),
		text_contrast_ratio(skin.header_text, header),
		text_contrast_ratio(skin.type_text, header),
		text_contrast_ratio(skin.gutter_text, gutter),
		text_contrast_ratio(skin.cell_text, composite_over(skin.selection_fill, even)),
		text_contrast_ratio(skin.cell_text, composite_over(skin.selection_fill, odd)),
	]
}

fn validate_projected_skin(name: &str, skin: TableSkin) -> Result<(), TableSkinProjectionError> {
	let weakest = TABLE_TEXT_CONTRAST_ROLES
		.into_iter()
		.zip(text_contrast_ratios(skin))
		.min_by(|(_, left), (_, right)| left.total_cmp(right))
		.expect("a table skin has seven required contrast roles");
	if weakest.1 < TABLE_TEXT_CONTRAST_MINIMUM {
		return Err(TableSkinProjectionError::UnsuitableTheme {
			name: name.to_owned(),
			role: weakest.0,
			ratio: weakest.1,
			minimum: TABLE_TEXT_CONTRAST_MINIMUM,
		});
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;
	use libaryon::engine::THEME_SPECS;
	use std::collections::HashSet;

	const PRODUCT_0026_DUOTONE_ORACLE: &str =
		include_str!("snapshots/product-0026-duotone-table-skins.txt");

	fn product_0026_theme_expressions() -> Vec<String> {
		let mut expressions = Vec::with_capacity(THEME_SPECS.len() * 2 * 2 * 2 * 20);
		for spec in THEME_SPECS {
			for reverse in [false, true] {
				for clip_low in [false, true] {
					for clip_high in [false, true] {
						for shift in 0..20 {
							let mut suffix = String::new();
							if reverse {
								suffix.push('r');
							}
							if clip_low {
								suffix.push('c');
							}
							if clip_high {
								suffix.push('C');
							}
							if shift != 0 {
								suffix.push('s');
								suffix.push_str(&shift.to_string());
							}
							expressions.push(if suffix.is_empty() {
								spec.name.to_owned()
							} else {
								format!("{}_{}", spec.name, suffix)
							});
						}
					}
				}
			}
		}
		expressions
	}

	fn product_0026_rgba(name: &str, color: TableRgba) -> String {
		format!(
			"{name}=#{:02X}{:02X}{:02X}@{:08X}",
			color.red,
			color.green,
			color.blue,
			color.alpha.to_bits()
		)
	}

	fn product_0026_serialize_duotone_oracle() -> String {
		let mut snapshot = String::new();
		for expression in product_0026_theme_expressions() {
			snapshot.push_str(&expression);
			match TableSkin::from_theme_name(&expression) {
				Ok(skin) => {
					snapshot.push_str("\tOK");
					for role in [
						("body", skin.body_background),
						("header", skin.header_background),
						("gutter", skin.gutter_background),
						("even", skin.row_even_background),
						("odd", skin.row_odd_background),
						("grid", skin.gridline),
						("cell_text", skin.cell_text),
						("header_text", skin.header_text),
						("type_text", skin.type_text),
						("gutter_text", skin.gutter_text),
						("selection", skin.selection_fill),
						("scrollbar", skin.scrollbar_thumb),
					] {
						snapshot.push('\t');
						snapshot.push_str(&product_0026_rgba(role.0, role.1));
					}
				}
				Err(TableSkinProjectionError::UnknownTheme { name }) => {
					panic!("registered expression {expression} became unknown as {name}");
				}
				Err(error @ TableSkinProjectionError::UnsuitableTheme { .. }) => {
					let TableSkinProjectionError::UnsuitableTheme {
						name,
						role,
						ratio,
						minimum,
					} = &error
					else {
						unreachable!();
					};
					snapshot.push_str(&format!(
						"\tERR\tname={name}\trole={role}\tratio={:016X}\tminimum={:016X}\tdisplay={error}",
						ratio.to_bits(),
						minimum.to_bits()
					));
				}
			}
			snapshot.push('\n');
		}
		snapshot
	}

	#[test]
	fn product_0026_complete_pre_refactor_duotone_oracle() {
		let expressions = product_0026_theme_expressions();
		assert_eq!(expressions.len(), 8_480);
		assert_eq!(
			expressions.iter().collect::<HashSet<_>>().len(),
			8_480,
			"every canonical modifier state must have one unique spelling"
		);

		let actual = product_0026_serialize_duotone_oracle();
		assert_eq!(actual.lines().count(), 8_480);
		assert_eq!(actual.bytes().filter(|byte| *byte == b'\n').count(), 8_480);
		if std::env::var_os("UPDATE_PRODUCT_0026_DUOTONE_SKINS").is_some() {
			let path = std::path::Path::new(env!("CARGO_MANIFEST_DIR"))
				.join("src/gui/snapshots/product-0026-duotone-table-skins.txt");
			std::fs::create_dir_all(path.parent().expect("snapshot directory"))
				.expect("create snapshot directory");
			std::fs::write(path, &actual).expect("write PRODUCT-0026 Duotone snapshot");
		}
		assert_eq!(actual, PRODUCT_0026_DUOTONE_ORACLE);
	}

	#[test]
	fn product_0026_pre_refactor_named_duotone_anchors_are_readable() {
		let terrain = TableSkin::from_theme_name("terrain_rs10").unwrap();
		let roles = [
			terrain.body_background,
			terrain.header_background,
			terrain.gutter_background,
			terrain.row_even_background,
			terrain.row_odd_background,
			terrain.gridline,
			terrain.cell_text,
			terrain.header_text,
			terrain.type_text,
			terrain.gutter_text,
			terrain.selection_fill,
			terrain.scrollbar_thumb,
		];
		let expected = [
			(0x33, 0x33, 0x99, 1.00_f32),
			(0x2F, 0x3F, 0x97, 1.00),
			(0x30, 0x3B, 0x9A, 1.00),
			(0x34, 0x38, 0x98, 1.00),
			(0x31, 0x3E, 0x96, 1.00),
			(0x00, 0xA9, 0xCE, 0.25),
			(0xFF, 0xFF, 0xFF, 1.00),
			(0xFF, 0xFF, 0xFF, 1.00),
			(0x5D, 0xDE, 0x78, 1.00),
			(0xFF, 0xFF, 0xFF, 1.00),
			(0x00, 0x9B, 0xF6, 0.18),
			(0x0A, 0x83, 0xE9, 0.60),
		];
		for (role, expected) in roles.into_iter().zip(expected) {
			assert_eq!(
				(role.red, role.green, role.blue, role.alpha.to_bits()),
				(expected.0, expected.1, expected.2, expected.3.to_bits())
			);
		}
		assert_eq!(
			text_contrast_ratios(terrain),
			[
				9.699402088387815,
				9.291850309460296,
				9.215605551928507,
				5.347313350977389,
				9.468500117805823,
				7.929932506425018,
				7.603379704659476,
			]
		);

		let Err(TableSkinProjectionError::UnsuitableTheme { role, ratio, .. }) =
			TableSkin::from_theme_name("yellows")
		else {
			panic!("yellows must expose the pre-refactor Duotone failure");
		};
		assert_eq!(role, "cell text on selected odd row");
		assert_eq!(ratio, 3.1750332126087506);
	}

	#[test]
	fn product_0023_every_builtin_meets_text_contrast_after_declared_alpha_compositing() {
		for skin in TableSkin::BUILT_INS {
			for ratio in text_contrast_ratios(skin) {
				assert!(
					ratio >= 4.5,
					"table text contrast {ratio:.3}:1 is below 4.5:1"
				);
			}
		}
	}

	#[test]
	fn product_0023_selection_compositing_consumes_the_skin_declared_alpha() {
		let opaque_selection = TableRgba::opaque(0, 0, 0);
		let skin = TableSkin {
			selection_fill: opaque_selection,
			..TableSkin::DEFAULT
		};
		let ratios = text_contrast_ratios(skin);
		let direct = text_contrast_ratio(skin.cell_text, visible_srgb(opaque_selection));
		assert_eq!(ratios[5], direct);
		assert_eq!(ratios[6], direct);

		let translucent = text_contrast_ratios(TableSkin::DEFAULT);
		assert_ne!(translucent[5], direct);
		assert_ne!(translucent[6], direct);
	}

	#[test]
	fn product_0024_fractional_composites_retain_product_0023_ratios_without_quantization() {
		let expected = [
			15.401817251018779,
			13.158388521894944,
			14.950831546071543,
			8.139026492916567,
			14.950831546071543,
			12.81745356023819,
			10.921309527435287,
		];
		for (actual, expected) in text_contrast_ratios(TableSkin::DEFAULT)
			.into_iter()
			.zip(expected)
		{
			assert!((actual - expected).abs() < 1e-12, "{actual} != {expected}");
		}

		let even = visible_srgb(TableSkin::DEFAULT.row_even_background);
		let selected = composite_over(TableRgba::translucent(1, 2, 3, 0.3), even);
		assert_ne!(selected.red() * 255.0, (selected.red() * 255.0).round());
		assert_ne!(selected.green() * 255.0, (selected.green() * 255.0).round());
		assert_ne!(selected.blue() * 255.0, (selected.blue() * 255.0).round());
	}

	#[test]
	fn product_0025_terrain_rs10_projects_every_role_and_passes_visible_contrast() {
		let skin = TableSkin::from_theme_name("terrain_rs10").expect("dogfood table skin");
		for ratio in text_contrast_ratios(skin) {
			assert!(ratio >= TABLE_TEXT_CONTRAST_MINIMUM, "{ratio}");
		}
		assert_ne!(skin, TableSkin::DEFAULT);
		for role in [
			skin.body_background,
			skin.header_background,
			skin.gutter_background,
			skin.row_even_background,
			skin.row_odd_background,
			skin.gridline,
			skin.cell_text,
			skin.header_text,
			skin.type_text,
			skin.gutter_text,
			skin.selection_fill,
			skin.scrollbar_thumb,
		] {
			assert!(role.alpha() > 0.0);
		}
	}

	#[test]
	fn product_0025_projection_distinguishes_unknown_and_unsuitable_themes() {
		assert_eq!(
			TableSkin::from_theme_name("definitely-not-a-theme"),
			Err(TableSkinProjectionError::UnknownTheme {
				name: "definitely-not-a-theme".to_owned(),
			})
		);

		let Err(TableSkinProjectionError::UnsuitableTheme {
			name,
			ratio,
			minimum,
			..
		}) = TableSkin::from_theme_name("black")
		else {
			panic!("a flat palette must be unsuitable rather than unknown");
		};
		assert_eq!(name, "black");
		assert_eq!(ratio, 1.0);
		assert_eq!(minimum, 4.5);
	}

	#[test]
	fn product_0025_every_successful_projection_has_complete_visible_contrast() {
		let mut successes = 0;
		for spec in THEME_SPECS {
			for expression in [
				spec.name.to_owned(),
				format!("{}_r", spec.name),
				format!("{}_s1", spec.name),
			] {
				match TableSkin::from_theme_name(&expression) {
					Ok(skin) => {
						successes += 1;
						for ratio in text_contrast_ratios(skin) {
							assert!(
								ratio >= TABLE_TEXT_CONTRAST_MINIMUM,
								"{expression}: {ratio}"
							);
						}
					}
					Err(TableSkinProjectionError::UnsuitableTheme { .. }) => {}
					Err(error) => panic!("registered expression {expression} failed: {error}"),
				}
			}
		}
		assert!(
			successes > 3,
			"projection success coverage must be non-vacuous"
		);
	}

	#[test]
	fn product_0025_order_permutations_remain_visible_in_table_roles() {
		let base = TableSkin::from_theme_name("terrain").unwrap();
		let reverse = TableSkin::from_theme_name("terrain_r").unwrap();
		let shift = TableSkin::from_theme_name("terrain_s1").unwrap();
		assert_ne!(base, reverse);
		assert_ne!(base, shift);
		assert_ne!(reverse, shift);
	}
}
