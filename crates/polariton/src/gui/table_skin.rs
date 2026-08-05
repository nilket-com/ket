//! Complete, resolved color treatment for the interior of a table artifact.
//!
//! This model is deliberately independent of editor chrome and plot data ramps.

use libaryon::core::{Rgb, Srgb};
use libaryon::engine::{ResolvedThemePair, Theme};

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

	pub fn from_theme_name(
		name: &str,
		projection: TableSkinProjection,
	) -> Result<Self, TableSkinProjectionError> {
		let theme = Theme::try_new(name).map_err(|_| TableSkinProjectionError::UnknownTheme {
			name: name.to_owned(),
		})?;
		match projection {
			TableSkinProjection::Duotone => {
				let anchors = DuotoneAnchors::from(theme.palette().most_contrasting_pair());
				project_duotone(theme.palette().colors(), anchors).map_err(|facts| {
					TableSkinProjectionError::UnsuitableDuotone {
						name: name.to_owned(),
						facts,
					}
				})
			}
			TableSkinProjection::PaletteForward => {
				project_palette_forward(theme.palette().colors()).map_err(|facts| {
					TableSkinProjectionError::UnsuitablePaletteForward {
						name: name.to_owned(),
						facts,
					}
				})
			}
		}
	}
}

impl Default for TableSkin {
	fn default() -> Self {
		Self::DEFAULT
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableSkinProjection {
	PaletteForward,
	Duotone,
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct VisibleContrastFailure {
	role: &'static str,
	ratio: f64,
	minimum: f64,
}

impl VisibleContrastFailure {
	pub const fn role(self) -> &'static str {
		self.role
	}

	pub const fn ratio(self) -> f64 {
		self.ratio
	}

	pub const fn minimum(self) -> f64 {
		self.minimum
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
struct DuotoneAnchors {
	darker_index: usize,
	darker: Rgb,
	lighter_index: usize,
	lighter: Rgb,
	contrast_ratio: f64,
}

impl From<ResolvedThemePair> for DuotoneAnchors {
	fn from(pair: ResolvedThemePair) -> Self {
		Self {
			darker_index: pair.darker_index(),
			darker: pair.darker(),
			lighter_index: pair.lighter_index(),
			lighter: pair.lighter(),
			contrast_ratio: pair.contrast_ratio(),
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct DuotoneProjectionFailure {
	anchors: DuotoneAnchors,
	weakest: VisibleContrastFailure,
}

impl DuotoneProjectionFailure {
	pub const fn darker_index(self) -> usize {
		self.anchors.darker_index
	}

	pub const fn darker(self) -> Rgb {
		self.anchors.darker
	}

	pub const fn lighter_index(self) -> usize {
		self.anchors.lighter_index
	}

	pub const fn lighter(self) -> Rgb {
		self.anchors.lighter
	}

	pub const fn anchor_contrast_ratio(self) -> f64 {
		self.anchors.contrast_ratio
	}

	pub const fn weakest(self) -> VisibleContrastFailure {
		self.weakest
	}
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TableTextGroup {
	Cell,
	Header,
	Type,
	Gutter,
}

impl TableTextGroup {
	const fn label(self) -> &'static str {
		match self {
			Self::Cell => "cell text",
			Self::Header => "header text",
			Self::Type => "type text",
			Self::Gutter => "gutter text",
		}
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct NeutralCandidateLimit {
	ink: Rgb,
	limiting_surface: &'static str,
	ratio: f64,
}

impl NeutralCandidateLimit {
	pub const fn ink(self) -> Rgb {
		self.ink
	}

	pub const fn limiting_surface(self) -> &'static str {
		self.limiting_surface
	}

	pub const fn ratio(self) -> f64 {
		self.ratio
	}
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct PaletteForwardProjectionFailure {
	group: TableTextGroup,
	near_black: NeutralCandidateLimit,
	near_white: NeutralCandidateLimit,
	minimum: f64,
}

impl PaletteForwardProjectionFailure {
	pub const fn group(self) -> TableTextGroup {
		self.group
	}

	pub const fn near_black(self) -> NeutralCandidateLimit {
		self.near_black
	}

	pub const fn near_white(self) -> NeutralCandidateLimit {
		self.near_white
	}

	pub const fn minimum(self) -> f64 {
		self.minimum
	}
}

#[derive(Clone, Debug, PartialEq)]
pub enum TableSkinProjectionError {
	UnknownTheme {
		name: String,
	},
	UnsuitableDuotone {
		name: String,
		facts: DuotoneProjectionFailure,
	},
	UnsuitablePaletteForward {
		name: String,
		facts: PaletteForwardProjectionFailure,
	},
}

impl std::fmt::Display for TableSkinProjectionError {
	fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::UnknownTheme { name } => write!(formatter, "unknown table skin theme {name:?}"),
			Self::UnsuitableDuotone { name, facts } => write!(
				formatter,
				"theme {name:?} is unsuitable for table skin role {}: {:.3}:1 is below {:.1}:1",
				facts.weakest.role, facts.weakest.ratio, facts.weakest.minimum
			),
			Self::UnsuitablePaletteForward { name, facts } => write!(
				formatter,
				"theme {name:?} is unsuitable for palette-forward {}: near-black reaches {:.3}:1 on {}, near-white reaches {:.3}:1 on {}; both are below {:.1}:1",
				facts.group.label(),
				facts.near_black.ratio,
				facts.near_black.limiting_surface,
				facts.near_white.ratio,
				facts.near_white.limiting_surface,
				facts.minimum
			),
		}
	}
}

impl std::error::Error for TableSkinProjectionError {}

const TABLE_TEXT_CONTRAST_MINIMUM: f64 = 4.5;
const PALETTE_FORWARD_NEAR_BLACK: Rgb = Rgb::new(16, 16, 16);
const PALETTE_FORWARD_NEAR_WHITE: Rgb = Rgb::new(245, 245, 245);
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

fn project_duotone(
	colors: &[Rgb; 20],
	anchors: DuotoneAnchors,
) -> Result<TableSkin, DuotoneProjectionFailure> {
	let dark = anchors.darker;
	let light = anchors.lighter;
	let header = mix_encoded_srgb(dark, colors[2], 0.08);
	let readable_type = colors
		.iter()
		.copied()
		.find(|color| color.contrast_ratio(header) >= TABLE_TEXT_CONTRAST_MINIMUM)
		.unwrap_or(light);

	let skin = TableSkin {
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
	};
	validate_projected_skin(skin)
		.map_err(|weakest| DuotoneProjectionFailure { anchors, weakest })?;
	Ok(skin)
}

#[derive(Clone, Copy)]
struct VisibleSurface {
	label: &'static str,
	color: Srgb,
}

#[derive(Clone, Copy)]
struct NeutralSelection {
	group: TableTextGroup,
	ink: Rgb,
	near_black: NeutralCandidateLimit,
	near_white: NeutralCandidateLimit,
}

fn candidate_limit(ink: Rgb, surfaces: &[VisibleSurface]) -> NeutralCandidateLimit {
	let limiting = surfaces
		.iter()
		.map(|surface| (surface.label, ink.as_srgb().contrast_ratio(surface.color)))
		.min_by(|left, right| left.1.total_cmp(&right.1))
		.expect("a text role is drawn on at least one surface");
	NeutralCandidateLimit {
		ink,
		limiting_surface: limiting.0,
		ratio: limiting.1,
	}
}

fn preferred_neutral(
	near_black: NeutralCandidateLimit,
	near_white: NeutralCandidateLimit,
) -> NeutralCandidateLimit {
	if near_black.ratio >= near_white.ratio {
		near_black
	} else {
		near_white
	}
}

fn select_neutral(group: TableTextGroup, surfaces: &[VisibleSurface]) -> NeutralSelection {
	let near_black = candidate_limit(PALETTE_FORWARD_NEAR_BLACK, surfaces);
	let near_white = candidate_limit(PALETTE_FORWARD_NEAR_WHITE, surfaces);
	let selected = preferred_neutral(near_black, near_white);
	NeutralSelection {
		group,
		ink: selected.ink,
		near_black,
		near_white,
	}
}

fn text_group_for_role(role: &str) -> TableTextGroup {
	if role.starts_with("cell text") {
		TableTextGroup::Cell
	} else if role.starts_with("header text") {
		TableTextGroup::Header
	} else if role.starts_with("type text") {
		TableTextGroup::Type
	} else if role.starts_with("gutter text") {
		TableTextGroup::Gutter
	} else {
		unreachable!("validated table role has a text group: {role}")
	}
}

fn project_palette_forward(
	colors: &[Rgb; 20],
) -> Result<TableSkin, PaletteForwardProjectionFailure> {
	let row_even_background = TableRgba::opaque_rgb(colors[3]);
	let row_odd_background = TableRgba::opaque_rgb(colors[4]);
	let header_background = TableRgba::opaque_rgb(colors[1]);
	let gutter_background = TableRgba::opaque_rgb(colors[2]);
	let selection_fill = TableRgba::translucent_rgb(colors[6], 0.18);

	let even = visible_srgb(row_even_background);
	let odd = visible_srgb(row_odd_background);
	let selected_even = composite_over(selection_fill, even);
	let selected_odd = composite_over(selection_fill, odd);
	let cell = select_neutral(
		TableTextGroup::Cell,
		&[
			VisibleSurface {
				label: "even row",
				color: even,
			},
			VisibleSurface {
				label: "odd row",
				color: odd,
			},
			VisibleSurface {
				label: "selected even row",
				color: selected_even,
			},
			VisibleSurface {
				label: "selected odd row",
				color: selected_odd,
			},
		],
	);
	let header_surface = [VisibleSurface {
		label: "header",
		color: visible_srgb(header_background),
	}];
	let header = select_neutral(TableTextGroup::Header, &header_surface);
	let type_text = select_neutral(TableTextGroup::Type, &header_surface);
	let gutter = select_neutral(
		TableTextGroup::Gutter,
		&[VisibleSurface {
			label: "gutter",
			color: visible_srgb(gutter_background),
		}],
	);

	let skin = TableSkin {
		body_background: TableRgba::opaque_rgb(colors[0]),
		header_background,
		gutter_background,
		row_even_background,
		row_odd_background,
		gridline: TableRgba::translucent_rgb(colors[5], 0.25),
		cell_text: TableRgba::opaque_rgb(cell.ink),
		header_text: TableRgba::opaque_rgb(header.ink),
		type_text: TableRgba::opaque_rgb(type_text.ink),
		gutter_text: TableRgba::opaque_rgb(gutter.ink),
		selection_fill,
		scrollbar_thumb: TableRgba::translucent_rgb(colors[7], 0.60),
	};

	if let Err(weakest) = validate_projected_skin(skin) {
		let failed_group = text_group_for_role(weakest.role());
		let failed = [cell, header, type_text, gutter]
			.into_iter()
			.find(|selection| selection.group == failed_group)
			.expect("every validated text role has a neutral selection");
		return Err(PaletteForwardProjectionFailure {
			group: failed.group,
			near_black: failed.near_black,
			near_white: failed.near_white,
			minimum: TABLE_TEXT_CONTRAST_MINIMUM,
		});
	}

	Ok(skin)
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

fn validate_projected_skin(skin: TableSkin) -> Result<(), VisibleContrastFailure> {
	let weakest = TABLE_TEXT_CONTRAST_ROLES
		.into_iter()
		.zip(text_contrast_ratios(skin))
		.min_by(|(_, left), (_, right)| left.total_cmp(right))
		.expect("a table skin has seven required contrast roles");
	if weakest.1 < TABLE_TEXT_CONTRAST_MINIMUM {
		return Err(VisibleContrastFailure {
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
			match TableSkin::from_theme_name(&expression, TableSkinProjection::Duotone) {
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
				Err(error @ TableSkinProjectionError::UnsuitableDuotone { .. }) => {
					let TableSkinProjectionError::UnsuitableDuotone { name, facts } = &error else {
						unreachable!();
					};
					let weakest = facts.weakest();
					snapshot.push_str(&format!(
						"\tERR\tname={name}\trole={role}\tratio={:016X}\tminimum={:016X}\tdisplay={error}",
						weakest.ratio().to_bits(),
						weakest.minimum().to_bits(),
						role = weakest.role()
					));
				}
				Err(TableSkinProjectionError::UnsuitablePaletteForward { .. }) => {
					panic!("Duotone oracle reached PaletteForward failure")
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
		let terrain =
			TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::Duotone).unwrap();
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

		let Err(TableSkinProjectionError::UnsuitableDuotone { facts, .. }) =
			TableSkin::from_theme_name("yellows", TableSkinProjection::Duotone)
		else {
			panic!("yellows must expose the pre-refactor Duotone failure");
		};
		assert_eq!(facts.weakest().role(), "cell text on selected odd row");
		assert_eq!(facts.weakest().ratio(), 3.1750332126087506);
	}

	#[test]
	fn product_0026_yellows_separates_duotone_from_palette_forward() {
		let Err(TableSkinProjectionError::UnsuitableDuotone { facts, .. }) =
			TableSkin::from_theme_name("yellows", TableSkinProjection::Duotone)
		else {
			panic!("yellows must retain its Duotone refusal");
		};
		assert_eq!(facts.darker_index(), 19);
		assert_eq!(facts.darker(), Rgb::new(0x99, 0x66, 0x00));
		assert_eq!(facts.lighter_index(), 0);
		assert_eq!(facts.lighter(), Rgb::new(0xFC, 0xFC, 0xCC));
		assert!(facts.anchor_contrast_ratio() > TABLE_TEXT_CONTRAST_MINIMUM);
		assert_eq!(facts.weakest().role(), "cell text on selected odd row");
		assert_eq!(facts.weakest().ratio(), 3.1750332126087506);

		let theme = Theme::try_new("yellows").unwrap();
		let colors = theme.palette().colors();
		let skin = TableSkin::from_theme_name("yellows", TableSkinProjection::PaletteForward)
			.expect("neutral ink makes yellows a valid palette-forward skin");
		assert_eq!(skin.body_background, TableRgba::opaque_rgb(colors[0]));
		assert_eq!(skin.header_background, TableRgba::opaque_rgb(colors[1]));
		assert_eq!(skin.gutter_background, TableRgba::opaque_rgb(colors[2]));
		assert_eq!(skin.row_even_background, TableRgba::opaque_rgb(colors[3]));
		assert_eq!(skin.row_odd_background, TableRgba::opaque_rgb(colors[4]));
		assert_eq!(skin.gridline, TableRgba::translucent_rgb(colors[5], 0.25));
		assert_eq!(
			skin.selection_fill,
			TableRgba::translucent_rgb(colors[6], 0.18)
		);
		assert_eq!(
			skin.scrollbar_thumb,
			TableRgba::translucent_rgb(colors[7], 0.60)
		);
		for ink in [
			skin.cell_text,
			skin.header_text,
			skin.type_text,
			skin.gutter_text,
		] {
			assert_eq!(ink, TableRgba::opaque_rgb(PALETTE_FORWARD_NEAR_BLACK));
		}
		for ratio in text_contrast_ratios(skin) {
			assert!(ratio >= TABLE_TEXT_CONTRAST_MINIMUM, "{ratio}");
		}
	}

	#[test]
	fn product_0026_neutral_selection_is_maximin_and_ties_choose_near_black() {
		let dark = VisibleSurface {
			label: "dark",
			color: Rgb::new(40, 40, 40).as_srgb(),
		};
		let selection = select_neutral(TableTextGroup::Header, &[dark]);
		assert_eq!(selection.ink, PALETTE_FORWARD_NEAR_WHITE);
		assert!(selection.near_white.ratio > selection.near_black.ratio);

		let tied_black = NeutralCandidateLimit {
			ink: PALETTE_FORWARD_NEAR_BLACK,
			limiting_surface: "black limit",
			ratio: 5.0,
		};
		let tied_white = NeutralCandidateLimit {
			ink: PALETTE_FORWARD_NEAR_WHITE,
			limiting_surface: "white limit",
			ratio: 5.0,
		};
		assert_eq!(preferred_neutral(tied_black, tied_white), tied_black);
	}

	#[test]
	fn product_0026_split_polarity_palette_fails_both_mechanisms_with_distinct_facts() {
		let darker = Rgb::new(89, 89, 89);
		let lighter = Rgb::new(149, 149, 149);
		let mut colors = [darker; 20];
		colors[4] = lighter;
		let anchors = DuotoneAnchors {
			darker_index: 0,
			darker,
			lighter_index: 4,
			lighter,
			contrast_ratio: darker.contrast_ratio(lighter),
		};
		let duotone = project_duotone(&colors, anchors).expect_err("insufficient anchor range");
		assert!(duotone.anchor_contrast_ratio() < TABLE_TEXT_CONTRAST_MINIMUM);
		assert!(duotone.weakest().ratio() < TABLE_TEXT_CONTRAST_MINIMUM);

		let palette_forward =
			project_palette_forward(&colors).expect_err("one cell ink spans opposite polarities");
		assert_eq!(palette_forward.group(), TableTextGroup::Cell);
		assert!(palette_forward.near_black().ratio() < TABLE_TEXT_CONTRAST_MINIMUM);
		assert!(palette_forward.near_white().ratio() < TABLE_TEXT_CONTRAST_MINIMUM);
		assert_eq!(palette_forward.near_black().limiting_surface(), "even row");
		assert_eq!(palette_forward.near_white().limiting_surface(), "odd row");
	}

	#[test]
	fn product_0026_every_canonical_projection_is_valid_or_mechanism_specific() {
		let mut duotone_successes = 0;
		let mut palette_forward_successes = 0;
		for expression in product_0026_theme_expressions() {
			for projection in [
				TableSkinProjection::Duotone,
				TableSkinProjection::PaletteForward,
			] {
				match TableSkin::from_theme_name(&expression, projection) {
					Ok(skin) => {
						assert!(
							validate_projected_skin(skin).is_ok(),
							"{expression} {projection:?}"
						);
						match projection {
							TableSkinProjection::Duotone => duotone_successes += 1,
							TableSkinProjection::PaletteForward => palette_forward_successes += 1,
						}
					}
					Err(TableSkinProjectionError::UnsuitableDuotone { .. }) => {
						assert_eq!(projection, TableSkinProjection::Duotone)
					}
					Err(TableSkinProjectionError::UnsuitablePaletteForward { .. }) => {
						assert_eq!(projection, TableSkinProjection::PaletteForward)
					}
					Err(error) => panic!("registered expression {expression}: {error}"),
				}
			}
		}
		assert!(duotone_successes > 0);
		assert!(palette_forward_successes > 0);
	}

	#[test]
	fn product_0026_palette_forward_observes_final_role_order() {
		let base =
			TableSkin::from_theme_name("greens", TableSkinProjection::PaletteForward).unwrap();
		let reverse =
			TableSkin::from_theme_name("greens_r", TableSkinProjection::PaletteForward).unwrap();
		let shift =
			TableSkin::from_theme_name("greens_s1", TableSkinProjection::PaletteForward).unwrap();
		assert_ne!(base, reverse);
		assert_ne!(base, shift);
		assert_ne!(reverse, shift);
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
		let skin = TableSkin::from_theme_name("terrain_rs10", TableSkinProjection::Duotone)
			.expect("dogfood table skin");
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
			TableSkin::from_theme_name("definitely-not-a-theme", TableSkinProjection::Duotone),
			Err(TableSkinProjectionError::UnknownTheme {
				name: "definitely-not-a-theme".to_owned(),
			})
		);

		let Err(TableSkinProjectionError::UnsuitableDuotone { name, facts }) =
			TableSkin::from_theme_name("black", TableSkinProjection::Duotone)
		else {
			panic!("a flat palette must be unsuitable rather than unknown");
		};
		assert_eq!(name, "black");
		assert_eq!(facts.weakest().ratio(), 1.0);
		assert_eq!(facts.weakest().minimum(), 4.5);
		assert_eq!(
			TableSkinProjectionError::UnsuitableDuotone { name, facts }.to_string(),
			"theme \"black\" is unsuitable for table skin role cell text on even row: 1.000:1 is below 4.5:1"
		);
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
				match TableSkin::from_theme_name(&expression, TableSkinProjection::Duotone) {
					Ok(skin) => {
						successes += 1;
						for ratio in text_contrast_ratios(skin) {
							assert!(
								ratio >= TABLE_TEXT_CONTRAST_MINIMUM,
								"{expression}: {ratio}"
							);
						}
					}
					Err(TableSkinProjectionError::UnsuitableDuotone { .. }) => {}
					Err(TableSkinProjectionError::UnsuitablePaletteForward { .. }) => {
						panic!("Duotone request returned PaletteForward facts")
					}
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
		let base = TableSkin::from_theme_name("terrain", TableSkinProjection::Duotone).unwrap();
		let reverse =
			TableSkin::from_theme_name("terrain_r", TableSkinProjection::Duotone).unwrap();
		let shift = TableSkin::from_theme_name("terrain_s1", TableSkinProjection::Duotone).unwrap();
		assert_ne!(base, reverse);
		assert_ne!(base, shift);
		assert_ne!(reverse, shift);
	}
}
