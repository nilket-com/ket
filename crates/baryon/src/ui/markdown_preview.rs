use font8x8::{BASIC_FONTS, UnicodeFonts};
use fontdue::{Font, FontSettings};
use image::{DynamicImage, Rgba, RgbaImage};
#[cfg(test)]
use libaryon::core::BufferId;
use libaryon::core::{
	Coverage, PixelDelta, PixelHeight, PixelWidth, PixelX, PixelY, PreviewFingerprint, Rgb, ViewId,
};
use libaryon::engine::UiPalette;
use libaryon::svp::markdown_blocks::{
	ListMarker, MarkdownBlock, MarkdownInlineStyle, MarkdownInlineText, line_start_byte,
	parse_markdown_blocks, source_line_count, source_line_for_byte,
};
use libaryon::svp::markdown_highlight::{MarkdownConstruct, markdown_construct_color};
use libaryon::svp::rust_highlight::CATEGORY_COUNT;
use libaryon::uast::projection::MarkdownPreviewSnapshot;
use ratatui::{Frame, buffer::Buffer, layout::Rect, style::Style};
use ratatui_image::{Image, Resize, picker::Picker, protocol::Protocol};
use regex_automata::{meta::Regex, util::syntax};
use std::{
	collections::HashMap,
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	sync::mpsc,
	thread,
};

#[cfg(test)]
const BG_RGBA: Rgba<u8> = Rgba([18, 18, 18, 255]);
const TEXT_RGBA: Rgba<u8> = Rgba([224, 229, 235, 255]);
const MUTED_RGBA: Rgba<u8> = Rgba([142, 151, 166, 255]);
const HEADING_RGBA: Rgba<u8> = Rgba([244, 247, 250, 255]);
const QUOTE_RGBA: Rgba<u8> = Rgba([120, 200, 200, 255]);
const CODE_TEXT_RGBA: Rgba<u8> = Rgba([210, 225, 240, 255]);
const CODE_BG_RGBA: Rgba<u8> = Rgba([24, 28, 36, 255]);
const TABLE_ROW_ALT_RGBA: Rgba<u8> = Rgba([22, 24, 29, 255]);
const RULE_RGBA: Rgba<u8> = Rgba([74, 84, 102, 255]);
const BODY_GLYPH_W: u32 = 8;
const BODY_GLYPH_H: u32 = 8;

#[derive(Clone)]
struct PreviewRequest {
	view_id: ViewId,
	fingerprint: PreviewFingerprint,
	snapshot: MarkdownPreviewSnapshot,
	area: Rect,
	font_size: (u16, u16),
	theme_colors: [Option<Rgb>; CATEGORY_COUNT],
	search: PreviewSearchConfig,
	ui_palette: UiPalette,
}

#[derive(Clone)]
struct PreviewSearchConfig {
	pattern: Option<String>,
	case_insensitive: bool,
	highlight: Rgb,
}

struct PreviewSearch {
	regex: Regex,
	background: Rgba<u8>,
	foreground: Rgba<u8>,
}

impl PreviewSearch {
	fn new(config: &PreviewSearchConfig) -> Option<Self> {
		let pattern = config
			.pattern
			.as_deref()
			.filter(|pattern| !pattern.is_empty())?;
		let regex = Regex::builder()
			.syntax(syntax::Config::new().case_insensitive(config.case_insensitive))
			.build(pattern)
			.ok()?;
		Some(Self {
			regex,
			background: color_rgba(config.highlight),
			foreground: color_rgba(contrasting_text_rgb(config.highlight)),
		})
	}
}

struct PreviewResponse {
	view_id: ViewId,
	fingerprint: PreviewFingerprint,
	size: (u16, u16),
	protocol: Option<Protocol>,
}

struct ViewEntry {
	requested_fingerprint: Option<PreviewFingerprint>,
	pending_fingerprint: Option<PreviewFingerprint>,
	current_fingerprint: Option<PreviewFingerprint>,
	current_size: Option<(u16, u16)>,
	current_protocol: Option<Protocol>,
}

impl ViewEntry {
	fn new() -> Self {
		Self {
			requested_fingerprint: None,
			pending_fingerprint: None,
			current_fingerprint: None,
			current_size: None,
			current_protocol: None,
		}
	}
}

pub(super) struct MarkdownPreviewController {
	tx_request: mpsc::Sender<PreviewRequest>,
	rx_ready: mpsc::Receiver<PreviewResponse>,
	views: HashMap<ViewId, ViewEntry>,
	font_size: (u16, u16),
}

impl MarkdownPreviewController {
	pub fn new() -> Self {
		let picker = Picker::from_query_stdio().unwrap_or_else(|_| Picker::halfblocks());
		let font_size = picker.font_size();
		let font_size = (font_size.width, font_size.height);
		let (tx_request, rx_request) = mpsc::channel::<PreviewRequest>();
		let (tx_ready, rx_ready) = mpsc::channel::<PreviewResponse>();

		thread::spawn(move || {
			let text_renderer = TextRenderer::load();
			while let Ok(mut request) = rx_request.recv() {
				while let Ok(next_request) = rx_request.try_recv() {
					request = next_request;
				}

				let image = render_markdown_preview_image_with_renderer(
					&request.snapshot,
					request.area,
					request.font_size,
					&request.theme_colors,
					&request.search,
					request.ui_palette,
					&text_renderer,
				);
				let protocol = picker
					.new_protocol(image, request.area.into(), Resize::Fit(None))
					.ok();
				if tx_ready
					.send(PreviewResponse {
						view_id: request.view_id,
						fingerprint: request.fingerprint,
						size: (request.area.width, request.area.height),
						protocol,
					})
					.is_err()
				{
					break;
				}
			}
		});

		Self {
			tx_request,
			rx_ready,
			views: HashMap::new(),
			font_size,
		}
	}

	pub fn request_preview(
		&mut self,
		view_id: ViewId,
		snapshot: &MarkdownPreviewSnapshot,
		area: Rect,
		theme_colors: [Option<Rgb>; CATEGORY_COUNT],
		search_pattern: Option<&str>,
		search_case_insensitive: bool,
		ui_palette: UiPalette,
	) {
		if area.width == 0 || area.height == 0 {
			return;
		}

		let search = PreviewSearchConfig {
			pattern: search_pattern.map(str::to_string),
			case_insensitive: search_case_insensitive,
			highlight: ui_palette.search_highlight,
		};
		let fingerprint = preview_request_fingerprint(
			snapshot,
			area,
			self.font_size,
			&theme_colors,
			&search,
			ui_palette,
		);
		let entry = self.views.entry(view_id).or_insert_with(ViewEntry::new);
		if entry.requested_fingerprint == Some(fingerprint)
			|| entry.pending_fingerprint == Some(fingerprint)
			|| entry.current_fingerprint == Some(fingerprint)
		{
			entry.requested_fingerprint = Some(fingerprint);
			return;
		}

		entry.requested_fingerprint = Some(fingerprint);
		entry.pending_fingerprint = Some(fingerprint);
		let _ = self.tx_request.send(PreviewRequest {
			view_id,
			fingerprint,
			snapshot: snapshot.clone(),
			area,
			font_size: self.font_size,
			theme_colors,
			search,
			ui_palette,
		});
	}

	pub fn poll(&mut self) -> bool {
		let mut updated = false;
		while let Ok(response) = self.rx_ready.try_recv() {
			let Some(entry) = self.views.get_mut(&response.view_id) else {
				continue;
			};
			if entry.requested_fingerprint == Some(response.fingerprint) {
				entry.current_fingerprint = Some(response.fingerprint);
				entry.current_size = Some(response.size);
				entry.current_protocol = response.protocol;
				entry.pending_fingerprint = None;
				updated = true;
			}
		}
		updated
	}

	pub fn render(&self, view_id: ViewId, frame: &mut Frame<'_>, area: Rect) -> bool {
		if area.width == 0 || area.height == 0 {
			return false;
		}
		let Some(entry) = self.views.get(&view_id) else {
			return false;
		};
		if entry.current_size != Some((area.width, area.height)) {
			return false;
		}
		let Some(protocol) = &entry.current_protocol else {
			return false;
		};
		frame.render_widget(Image::new(protocol), area);
		true
	}

	pub fn has_pending_preview(&self, view_id: ViewId) -> bool {
		self.views
			.get(&view_id)
			.is_some_and(|entry| entry.pending_fingerprint.is_some())
	}

	pub fn retain_views<F: Fn(ViewId) -> bool>(&mut self, keep: F) {
		self.views.retain(|view_id, _| keep(*view_id));
	}
}

pub(super) fn render_markdown_preview_fallback(
	buf: &mut Buffer,
	area: Rect,
	pending: bool,
	ui_palette: UiPalette,
) {
	if area.width == 0 || area.height == 0 {
		return;
	}

	let style = Style::default()
		.bg(crate::ui::ratatui_color(ui_palette.preview_surface))
		.fg(crate::ui::ratatui_color(ui_palette.preview_text));
	for row in 0..area.height {
		for col in 0..area.width {
			if let Some(cell) = buf.cell_mut((area.x + col, area.y + row)) {
				cell.set_char(' ').set_style(style);
			}
		}
	}

	let message = if pending {
		"Rendering Markdown preview..."
	} else {
		"Markdown image preview unavailable"
	};
	let row = area.y + area.height / 2;
	let col = area
		.x
		.saturating_add(area.width.saturating_sub(message.len() as u16) / 2);
	for (idx, ch) in message.chars().enumerate() {
		if idx >= area.width as usize {
			break;
		}
		if let Some(cell) = buf.cell_mut((col + idx as u16, row)) {
			cell.set_char(ch).set_style(style);
		}
	}
}

fn preview_request_fingerprint(
	snapshot: &MarkdownPreviewSnapshot,
	area: Rect,
	font_size: (u16, u16),
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	search: &PreviewSearchConfig,
	ui_palette: UiPalette,
) -> PreviewFingerprint {
	let mut hasher = DefaultHasher::new();
	area.width.hash(&mut hasher);
	area.height.hash(&mut hasher);
	font_size.hash(&mut hasher);
	snapshot.hash(&mut hasher);
	hash_theme_colors(theme_colors, &mut hasher);
	search.pattern.hash(&mut hasher);
	search.case_insensitive.hash(&mut hasher);
	let (r, g, b) = color_components(search.highlight);
	r.hash(&mut hasher);
	g.hash(&mut hasher);
	b.hash(&mut hasher);
	for color in [
		ui_palette.preview_surface,
		ui_palette.preview_text,
		ui_palette.accent,
	] {
		let (r, g, b) = color_components(color);
		r.hash(&mut hasher);
		g.hash(&mut hasher);
		b.hash(&mut hasher);
	}
	PreviewFingerprint::new(hasher.finish())
}

fn hash_theme_colors(theme_colors: &[Option<Rgb>; CATEGORY_COUNT], hasher: &mut DefaultHasher) {
	for color in theme_colors {
		match color {
			Some(color) => {
				1u8.hash(hasher);
				let (r, g, b) = color_components(*color);
				r.hash(hasher);
				g.hash(hasher);
				b.hash(hasher);
			}
			None => 0u8.hash(hasher),
		}
	}
}

#[derive(Clone, Copy)]
struct MarkdownPreviewPalette {
	body_text: Rgba<u8>,
	muted: Rgba<u8>,
	heading: Rgba<u8>,
	bullet_marker: Rgba<u8>,
	ordered_marker: Rgba<u8>,
	task_marker: Rgba<u8>,
	quote: Rgba<u8>,
	inline_code: Rgba<u8>,
	code_block: Rgba<u8>,
	code_bg: Rgba<u8>,
	table_row_alt: Rgba<u8>,
	rule: Rgba<u8>,
}

impl MarkdownPreviewPalette {
	/// Every text color resolves through [`markdown_construct_color`], the same
	/// table the Markdown source highlighter classifies its tokens with, so a
	/// construct is one color in a document, in its preview, and in Polariton.
	/// Backgrounds are chrome — no construct produces them.
	fn from_theme(
		theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
		preview_text: Rgb,
		accent: Rgb,
	) -> Self {
		let resolved = |construct: MarkdownConstruct, fallback: Rgb| -> Rgba<u8> {
			color_rgba(markdown_construct_color(construct, theme_colors, fallback))
		};
		Self {
			body_text: resolved(MarkdownConstruct::Body, preview_text),
			muted: resolved(MarkdownConstruct::Quote, rgba_color(MUTED_RGBA)),
			heading: resolved(MarkdownConstruct::Heading, rgba_color(HEADING_RGBA)),
			bullet_marker: resolved(MarkdownConstruct::BulletMarker, accent),
			ordered_marker: resolved(MarkdownConstruct::OrderedMarker, accent),
			// A task box is the item's own marker on the left, not a construct
			// of its own, so it follows the bullet rather than inventing a color.
			task_marker: resolved(MarkdownConstruct::BulletMarker, accent),
			quote: resolved(MarkdownConstruct::Quote, rgba_color(QUOTE_RGBA)),
			inline_code: resolved(MarkdownConstruct::InlineCode, rgba_color(CODE_TEXT_RGBA)),
			code_block: resolved(MarkdownConstruct::CodeText, rgba_color(CODE_TEXT_RGBA)),
			code_bg: CODE_BG_RGBA,
			table_row_alt: TABLE_ROW_ALT_RGBA,
			rule: resolved(MarkdownConstruct::Rule, rgba_color(RULE_RGBA)),
		}
	}
}

fn rgba_color(color: Rgba<u8>) -> Rgb {
	Rgb::new(color.0[0], color.0[1], color.0[2])
}

fn color_rgba(color: Rgb) -> Rgba<u8> {
	let (r, g, b) = color_components(color);
	Rgba([r, g, b, 255])
}

fn color_components(color: Rgb) -> (u8, u8, u8) {
	(color.r, color.g, color.b)
}

fn contrasting_text_rgb(background: Rgb) -> Rgb {
	let luminance = u32::from(background.r) * 299
		+ u32::from(background.g) * 587
		+ u32::from(background.b) * 114;
	if luminance >= 128_000 {
		Rgb::new(0, 0, 0)
	} else {
		Rgb::new(255, 255, 255)
	}
}

#[cfg(test)]
fn render_markdown_preview_image(
	snapshot: &MarkdownPreviewSnapshot,
	area: Rect,
	font_size: (u16, u16),
) -> DynamicImage {
	render_markdown_preview_image_with_theme(snapshot, area, font_size, &[None; CATEGORY_COUNT])
}

#[cfg(test)]
fn render_markdown_preview_image_with_theme(
	snapshot: &MarkdownPreviewSnapshot,
	area: Rect,
	font_size: (u16, u16),
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
) -> DynamicImage {
	let text_renderer = TextRenderer::load();
	render_markdown_preview_image_with_renderer(
		snapshot,
		area,
		font_size,
		theme_colors,
		&PreviewSearchConfig {
			pattern: None,
			case_insensitive: false,
			highlight: libaryon::core::SEARCH_MATCH_BG,
		},
		UiPalette::default(),
		&text_renderer,
	)
}

#[cfg(test)]
fn render_markdown_preview_image_with_search(
	snapshot: &MarkdownPreviewSnapshot,
	area: Rect,
	font_size: (u16, u16),
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	search: &PreviewSearchConfig,
) -> DynamicImage {
	let text_renderer = TextRenderer::load();
	render_markdown_preview_image_with_renderer(
		snapshot,
		area,
		font_size,
		theme_colors,
		search,
		UiPalette::default(),
		&text_renderer,
	)
}

fn render_markdown_preview_image_with_renderer(
	snapshot: &MarkdownPreviewSnapshot,
	area: Rect,
	font_size: (u16, u16),
	theme_colors: &[Option<Rgb>; CATEGORY_COUNT],
	search: &PreviewSearchConfig,
	ui_palette: UiPalette,
	text_renderer: &TextRenderer,
) -> DynamicImage {
	let font_w = PixelWidth::new(font_size.0.max(1) as u32);
	let font_h = PixelHeight::new(font_size.1.max(1) as u32);
	let width_px = font_w * u32::from(area.width.max(1));
	let height_px = font_h * u32::from(area.height.max(1));
	let mut image = RgbaImage::from_pixel(
		width_px.get(),
		height_px.get(),
		color_rgba(ui_palette.preview_surface),
	);
	let metrics = FontMetrics {
		cell_w: font_w,
		cell_h: font_h,
		body_scale_x: (font_w.get() / BODY_GLYPH_W).max(1),
		body_scale_y: (font_h.get() / BODY_GLYPH_H).max(1),
		body_role: if snapshot.serif {
			FontRole::Serif
		} else {
			FontRole::Sans
		},
	};
	let padding_x = font_w.get().max(6);
	let padding_y = PixelHeight::new((font_h.get() / 2).max(4));
	let blocks = parse_markdown_blocks(&snapshot.source_text);
	let layout = build_preview_layout(
		&snapshot.source_text,
		&blocks,
		&metrics,
		width_px,
		height_px,
		padding_x,
		padding_y,
		snapshot.source_scroll_line.get(),
		snapshot.source_viewport_line_count,
	);
	let palette = MarkdownPreviewPalette::from_theme(
		theme_colors,
		ui_palette.preview_text,
		ui_palette.accent,
	);
	let visible_bottom = (layout.scroll_px + height_px).boundary_before();
	let mut canvas = PreviewCanvas::new(&mut image, layout.scroll_px, PreviewSearch::new(search));

	for (block, block_layout) in blocks.iter().zip(layout.blocks.iter()) {
		if block_layout.y_end <= layout.scroll_px.boundary_before()
			|| block_layout.y_start.boundary_before() >= visible_bottom
		{
			continue;
		}
		draw_markdown_block(
			&mut canvas,
			padding_x,
			block_layout.y_start.get(),
			block,
			&metrics,
			&palette,
			text_renderer,
		);
	}

	DynamicImage::ImageRgba8(image)
}

struct PreviewLayout {
	blocks: Vec<BlockLayout>,
	scroll_px: PixelY,
}

struct BlockLayout {
	y_start: PixelY,
	y_end: PixelHeight,
}

fn build_preview_layout(
	source: &str,
	blocks: &[MarkdownBlock],
	metrics: &FontMetrics,
	width_px: PixelWidth,
	height_px: PixelHeight,
	padding_x: u32,
	padding_y: PixelHeight,
	source_scroll_line: u64,
	source_viewport_line_count: u32,
) -> PreviewLayout {
	let mut y = padding_y;
	let mut layouts = Vec::with_capacity(blocks.len());
	for block in blocks {
		let y_start = y.start_position();
		y = PixelHeight::new(measure_markdown_block(
			width_px.get(),
			padding_x,
			y.get(),
			block,
			metrics,
		));
		layouts.push(BlockLayout { y_start, y_end: y });
	}

	let document_height = y.saturating_add(padding_y.get());
	let anchor_y = preview_anchor_y_for_source_line(source, source_scroll_line, blocks, &layouts)
		.unwrap_or_else(|| padding_y.start_position());
	let max_scroll_px = document_height
		.saturating_sub(height_px.get())
		.start_position();
	let source_at_bottom = source_scroll_line > 0
		&& source_scroll_line.saturating_add(u64::from(source_viewport_line_count.max(1)))
			>= source_line_count(source);
	let scroll_px = if source_at_bottom {
		max_scroll_px
	} else {
		PixelY::new(anchor_y.get().saturating_sub(padding_y.get())).min(max_scroll_px)
	};

	PreviewLayout {
		blocks: layouts,
		scroll_px,
	}
}

fn preview_anchor_y_for_source_line(
	source: &str,
	target_line: u64,
	blocks: &[MarkdownBlock],
	layouts: &[BlockLayout],
) -> Option<PixelY> {
	let target_byte = line_start_byte(source, target_line);
	for (block, layout) in blocks.iter().zip(layouts.iter()) {
		let range = block.source_range();
		if range.end <= target_byte {
			continue;
		}
		let start_line = source_line_for_byte(source, range.start);
		let end_line = source_line_for_byte(source, range.end.saturating_sub(1));
		let line_span = end_line.saturating_sub(start_line).saturating_add(1).max(1);
		let line_offset = target_line
			.saturating_sub(start_line)
			.min(line_span.saturating_sub(1));
		let block_height = layout
			.y_end
			.saturating_sub(layout.y_start.boundary_before().get());
		let offset_px = ((u128::from(block_height.get()) * u128::from(line_offset))
			/ u128::from(line_span)) as u32;
		return Some(layout.y_start + PixelHeight::new(offset_px));
	}
	layouts.last().map(|layout| layout.y_end.start_position())
}

#[derive(Clone, Copy)]
struct FontMetrics {
	cell_w: PixelWidth,
	cell_h: PixelHeight,
	body_scale_x: u32,
	body_scale_y: u32,
	body_role: FontRole,
}

struct PreviewCanvas<'a> {
	image: &'a mut RgbaImage,
	scroll_px: PixelY,
	search: Option<PreviewSearch>,
}

impl<'a> PreviewCanvas<'a> {
	fn new(image: &'a mut RgbaImage, scroll_px: PixelY, search: Option<PreviewSearch>) -> Self {
		Self {
			image,
			scroll_px,
			search,
		}
	}

	fn width(&self) -> u32 {
		self.image.width()
	}

	fn height(&self) -> u32 {
		self.image.height()
	}

	fn visible_bottom(&self) -> u32 {
		(self.scroll_px + PixelHeight::new(self.height()))
			.boundary_before()
			.get()
	}
}

/// The four faces of one family. A configured font need not have all of them,
/// which is why selection is a fallback chain rather than a lookup.
struct FontFaces {
	regular: Option<Font>,
	bold: Option<Font>,
	italic: Option<Font>,
	bold_italic: Option<Font>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FaceChoice {
	Regular,
	Bold,
	Italic,
	BoldItalic,
}

/// Which faces a family actually provides.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
struct FaceAvailability {
	regular: bool,
	bold: bool,
	italic: bool,
	bold_italic: bool,
}

impl FontFaces {
	fn load(db: &fontdb::Database, families: &[fontdb::Family<'_>]) -> Self {
		Self {
			regular: load_preview_font(db, families, fontdb::Weight::NORMAL, fontdb::Style::Normal),
			bold: load_preview_font(db, families, fontdb::Weight::BOLD, fontdb::Style::Normal),
			italic: load_preview_font(db, families, fontdb::Weight::NORMAL, fontdb::Style::Italic),
			bold_italic: load_preview_font(
				db,
				families,
				fontdb::Weight::BOLD,
				fontdb::Style::Italic,
			),
		}
	}

	fn availability(&self) -> FaceAvailability {
		FaceAvailability {
			regular: self.regular.is_some(),
			bold: self.bold.is_some(),
			italic: self.italic.is_some(),
			bold_italic: self.bold_italic.is_some(),
		}
	}

	fn face(&self, choice: FaceChoice) -> Option<&Font> {
		match choice {
			FaceChoice::Regular => self.regular.as_ref(),
			FaceChoice::Bold => self.bold.as_ref(),
			FaceChoice::Italic => self.italic.as_ref(),
			FaceChoice::BoldItalic => self.bold_italic.as_ref(),
		}
	}

	/// The face to draw with, and whether the rasterizer must embolden it.
	fn pick(&self, bold: bool, italic: bool) -> Option<(&Font, bool)> {
		let (choice, synthesize_bold) = choose_face(bold, italic, self.availability())?;
		self.face(choice).map(|font| (font, synthesize_bold))
	}
}

/// Face selection, kept free of font data so the policy can be tested.
///
/// Weight can be synthesized by emboldening; slant cannot, so a missing italic
/// costs the slant while a missing bold does not cost the weight. Bold italic
/// therefore prefers a real italic it can embolden over a real bold it cannot
/// slant.
fn choose_face(
	bold: bool,
	italic: bool,
	available: FaceAvailability,
) -> Option<(FaceChoice, bool)> {
	let regular = available.regular.then_some((FaceChoice::Regular, false));
	match (bold, italic) {
		(true, true) => available
			.bold_italic
			.then_some((FaceChoice::BoldItalic, false))
			.or_else(|| available.italic.then_some((FaceChoice::Italic, true)))
			.or_else(|| available.bold.then_some((FaceChoice::Bold, false)))
			.or_else(|| available.regular.then_some((FaceChoice::Regular, true))),
		(true, false) => available
			.bold
			.then_some((FaceChoice::Bold, false))
			.or_else(|| available.regular.then_some((FaceChoice::Regular, true))),
		(false, true) => available
			.italic
			.then_some((FaceChoice::Italic, false))
			.or(regular),
		(false, false) => regular,
	}
}

struct TextRenderer {
	sans: FontFaces,
	serif: FontFaces,
	mono: FontFaces,
}

impl TextRenderer {
	fn load() -> Self {
		let mut db = fontdb::Database::new();
		db.load_system_fonts();

		let sans_families = [
			fontdb::Family::Name("Inter"),
			fontdb::Family::Name("Arial"),
			fontdb::Family::Name("Helvetica"),
			fontdb::Family::Name("DejaVu Sans"),
			fontdb::Family::Name("Liberation Sans"),
			fontdb::Family::Name("Noto Sans"),
			fontdb::Family::SansSerif,
		];
		let mono_families = [
			fontdb::Family::Name("JetBrains Mono"),
			fontdb::Family::Name("Fira Code"),
			fontdb::Family::Name("DejaVu Sans Mono"),
			fontdb::Family::Name("Liberation Mono"),
			fontdb::Family::Name("Noto Sans Mono"),
			fontdb::Family::Name("Courier New"),
			fontdb::Family::Monospace,
		];
		let serif_families = [
			fontdb::Family::Name("Charter"),
			fontdb::Family::Name("Georgia"),
			fontdb::Family::Name("Cambria"),
			fontdb::Family::Name("Times New Roman"),
			fontdb::Family::Name("DejaVu Serif"),
			fontdb::Family::Name("Liberation Serif"),
			fontdb::Family::Name("Noto Serif"),
			fontdb::Family::Serif,
		];

		Self {
			sans: FontFaces::load(&db, &sans_families),
			serif: FontFaces::load(&db, &serif_families),
			mono: FontFaces::load(&db, &mono_families),
		}
	}

	/// Sans is the last resort for every role: a missing serif or mono family
	/// should cost the typeface, not the text.
	fn font_for(&self, spec: TextSpec) -> Option<(&Font, bool)> {
		let primary = match spec.role {
			FontRole::Sans => &self.sans,
			FontRole::Serif => &self.serif,
			FontRole::Mono => &self.mono,
		};
		primary
			.pick(spec.bold, spec.italic)
			.or_else(|| self.sans.pick(spec.bold, spec.italic))
	}
}

fn load_preview_font(
	db: &fontdb::Database,
	families: &[fontdb::Family<'_>],
	weight: fontdb::Weight,
	style: fontdb::Style,
) -> Option<Font> {
	let query = fontdb::Query {
		families,
		weight,
		style,
		..fontdb::Query::default()
	};
	let id = db.query(&query)?;
	db.with_face_data(id, |data, face_index| {
		Font::from_bytes(
			data,
			FontSettings {
				collection_index: face_index,
				..FontSettings::default()
			},
		)
		.ok()
	})
	.flatten()
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum FontRole {
	Sans,
	Serif,
	Mono,
}

#[derive(Clone, Copy)]
struct TextSpec {
	px_size: f32,
	advance_w: u32,
	glyph_h: u32,
	line_gap: u32,
	color: Rgba<u8>,
	bold: bool,
	italic: bool,
	role: FontRole,
	fallback_scale_x: u32,
	fallback_scale_y: u32,
}

impl TextSpec {
	fn body(metrics: &FontMetrics, color: Rgba<u8>) -> Self {
		Self {
			px_size: (metrics.cell_h.get() as f32 * 0.86).max(8.0),
			advance_w: metrics.cell_w.get().max(1),
			glyph_h: metrics.cell_h.get().max(1),
			line_gap: (metrics.cell_h.get() / 4).max(2),
			color,
			bold: false,
			italic: false,
			role: metrics.body_role,
			fallback_scale_x: metrics.body_scale_x,
			fallback_scale_y: metrics.body_scale_y,
		}
	}

	fn bold_body(metrics: &FontMetrics, color: Rgba<u8>) -> Self {
		Self {
			bold: true,
			..Self::body(metrics, color)
		}
	}

	fn code(metrics: &FontMetrics, color: Rgba<u8>) -> Self {
		Self {
			role: FontRole::Mono,
			..Self::body(metrics, color)
		}
	}

	fn bold_code(metrics: &FontMetrics, color: Rgba<u8>) -> Self {
		Self {
			bold: true,
			role: FontRole::Mono,
			..Self::body(metrics, color)
		}
	}

	fn heading(metrics: &FontMetrics, level: u8, color: Rgba<u8>) -> Self {
		let (px_scale, advance_cells, glyph_cells, fallback_scale_x, fallback_scale_y) = match level
		{
			1 => (1.9, 2, 2, 2, 2),
			2 => (1.6, 2, 2, 2, 2),
			3 => (1.35, 1, 2, 1, 2),
			_ => (1.0, 1, 1, 1, 1),
		};
		Self {
			px_size: (metrics.cell_h.get() as f32 * px_scale).max(8.0),
			advance_w: metrics.cell_w.get().saturating_mul(advance_cells).max(1),
			glyph_h: metrics.cell_h.get().saturating_mul(glyph_cells).max(1),
			line_gap: (metrics.cell_h.get() / 3).max(3),
			color,
			bold: true,
			italic: false,
			role: metrics.body_role,
			fallback_scale_x: metrics.body_scale_x.saturating_mul(fallback_scale_x).max(1),
			fallback_scale_y: metrics.body_scale_y.saturating_mul(fallback_scale_y).max(1),
		}
	}

	fn glyph_w(self) -> u32 {
		self.advance_w.max(1)
	}

	fn glyph_h(self) -> u32 {
		self.glyph_h.max(1)
	}

	fn line_h(self) -> u32 {
		self.glyph_h().saturating_add(self.line_gap)
	}
}

fn measure_markdown_block(
	width: u32,
	x: u32,
	y: u32,
	block: &MarkdownBlock,
	metrics: &FontMetrics,
) -> u32 {
	match block {
		MarkdownBlock::Heading { level, text, .. } => {
			measure_heading(width, x, y, *level, text, metrics)
		}
		MarkdownBlock::Paragraph { text, .. } => {
			measure_wrapped_inline_text(width, x, y, text, TextSpec::body(metrics, TEXT_RGBA))
				.saturating_add(metrics.cell_h.get() / 3)
		}
		MarkdownBlock::CodeBlock { info, text, .. } => {
			measure_code_block(width, x, y, info.as_deref(), text, metrics)
		}
		MarkdownBlock::Quote { text, .. } => {
			let body_x = x.saturating_add(metrics.cell_w.get());
			measure_wrapped_inline_text(width, body_x, y, text, TextSpec::body(metrics, MUTED_RGBA))
				.saturating_add(metrics.cell_h.get() / 3)
		}
		MarkdownBlock::ListItem {
			depth,
			checked,
			text,
			..
		} => {
			let marker_x = list_marker_x(x, *depth, metrics, width);
			let text_x = marker_x
				.saturating_add(if checked.is_some() {
					metrics.cell_w.get().saturating_mul(4)
				} else {
					metrics.cell_w.get().saturating_mul(3)
				})
				.min(width.saturating_sub(1));
			measure_wrapped_inline_text(width, text_x, y, text, TextSpec::body(metrics, TEXT_RGBA))
				.saturating_add(metrics.cell_h.get() / 3)
		}
		MarkdownBlock::Rule { .. } => y
			.saturating_add(metrics.cell_h.get() / 2)
			.saturating_add(metrics.cell_h.get()),
		MarkdownBlock::Table { rows, .. } => measure_table(width, x, y, rows, metrics),
		MarkdownBlock::Html { text, .. } => {
			measure_code_block(width, x, y, Some("html"), text, metrics)
		}
	}
}

fn measure_heading(
	width: u32,
	x: u32,
	y: u32,
	level: u8,
	text: &str,
	metrics: &FontMetrics,
) -> u32 {
	let spec = TextSpec::heading(metrics, level, HEADING_RGBA);
	let y = y.saturating_add(if level <= 2 {
		metrics.cell_h.get() / 2
	} else {
		metrics.cell_h.get() / 4
	});
	measure_wrapped_text(width, x, y, text, spec).saturating_add(metrics.cell_h.get() / 2)
}

fn measure_code_block(
	width: u32,
	x: u32,
	mut y: u32,
	info: Option<&str>,
	text: &str,
	metrics: &FontMetrics,
) -> u32 {
	if let Some(info) = info.filter(|info| !info.trim().is_empty()) {
		let label = format!("[{}]", info.trim());
		y = measure_wrapped_text(
			width,
			x.saturating_add(metrics.cell_w.get()),
			y,
			&label,
			TextSpec::body(metrics, MUTED_RGBA),
		);
	}
	for _ in text.lines() {
		y = measure_code_line(y, metrics);
	}
	if text.is_empty() {
		y = measure_code_line(y, metrics);
	}
	y.saturating_add(metrics.cell_h.get() / 3)
}

fn measure_code_line(y: u32, metrics: &FontMetrics) -> u32 {
	let spec = TextSpec::code(metrics, Rgba([210, 225, 240, 255]));
	let block_pad_y = (metrics.cell_h.get() / 5).max(2);
	let block_bottom = y.saturating_add(spec.glyph_h()).saturating_add(block_pad_y);
	block_bottom.saturating_add(metrics.cell_h.get() / 5)
}

fn measure_table(
	_width: u32,
	_x: u32,
	mut y: u32,
	rows: &[Vec<MarkdownInlineText>],
	metrics: &FontMetrics,
) -> u32 {
	if rows.is_empty() {
		return y;
	}
	let spec = TextSpec::code(metrics, TEXT_RGBA);
	let columns = rows.iter().map(|row| row.len()).max().unwrap_or(0);
	if columns == 0 {
		return y;
	}
	// Cells clip instead of wrapping, so table height is independent of column widths.
	for row_idx in 0..rows.len() {
		y = y.saturating_add(spec.line_h());
		if row_idx == 0 {
			y = y.saturating_add(metrics.cell_h.get() / 4);
		}
	}
	y.saturating_add(metrics.cell_h.get() / 2)
}

fn table_column_widths(
	rows: &[Vec<MarkdownInlineText>],
	available_chars: usize,
) -> (Vec<usize>, usize) {
	const MIN_CELL_WIDTH: usize = 4;

	let columns = rows.iter().map(|row| row.len()).max().unwrap_or(0);
	if columns == 0 {
		return (Vec::new(), 0);
	}
	let available_chars = available_chars.max(1);
	let gutter: usize = if columns == 1 {
		0
	} else if available_chars >= columns.saturating_add((columns - 1).saturating_mul(2)) {
		2
	} else if available_chars >= columns.saturating_add(columns - 1) {
		1
	} else {
		0
	};
	let content_space = available_chars
		.saturating_sub(gutter.saturating_mul(columns.saturating_sub(1)))
		.max(columns);
	let min_width = MIN_CELL_WIDTH.min(content_space / columns).max(1);
	// A single outlier should influence the table without consuming nearly all
	// of it. Extra space is distributed separately after measuring demand.
	let demand_cap = content_space.div_ceil(2).max(min_width);
	let desired = (0..columns)
		.map(|column| {
			rows.iter()
				.filter_map(|row| row.get(column))
				.map(MarkdownInlineText::char_count)
				.max()
				.unwrap_or(0)
				.max(min_width)
				.min(demand_cap)
		})
		.collect::<Vec<_>>();
	let desired_total = desired.iter().copied().sum::<usize>();

	if desired_total <= content_space {
		let mut widths = desired.clone();
		let extra = content_space - desired_total;
		let shared = extra / columns;
		for width in &mut widths {
			*width = width.saturating_add(shared);
		}
		let mut order = (0..columns).collect::<Vec<_>>();
		order.sort_unstable_by_key(|&column| std::cmp::Reverse(desired[column]));
		for &column in order.iter().take(extra % columns) {
			widths[column] = widths[column].saturating_add(1);
		}
		return (widths, gutter);
	}

	let mut widths = vec![min_width; columns];
	let remaining = content_space.saturating_sub(min_width.saturating_mul(columns));
	let weights = desired
		.iter()
		.map(|width| width.saturating_sub(min_width))
		.collect::<Vec<_>>();
	let weight_total = weights.iter().copied().sum::<usize>();
	if remaining == 0 || weight_total == 0 {
		return (widths, gutter);
	}

	let mut assigned = 0usize;
	let mut remainders = Vec::with_capacity(columns);
	for (column, &weight) in weights.iter().enumerate() {
		let scaled = (remaining as u128) * (weight as u128);
		let share = (scaled / weight_total as u128) as usize;
		widths[column] = widths[column].saturating_add(share);
		assigned = assigned.saturating_add(share);
		remainders.push((scaled % weight_total as u128, desired[column], column));
	}
	remainders.sort_unstable_by_key(|&(remainder, demand, _)| {
		(std::cmp::Reverse(remainder), std::cmp::Reverse(demand))
	});
	for &(_, _, column) in remainders.iter().take(remaining.saturating_sub(assigned)) {
		widths[column] = widths[column].saturating_add(1);
	}

	(widths, gutter)
}

fn measure_wrapped_inline_text(
	width: u32,
	x: u32,
	mut y: u32,
	text: &MarkdownInlineText,
	spec: TextSpec,
) -> u32 {
	if x >= width {
		return y;
	}
	let max_width = width.saturating_sub(x).max(spec.glyph_w());
	let max_chars = (max_width / spec.glyph_w()).max(1) as usize;
	let mut line_len = 0usize;

	for word in inline_words(text) {
		let mut remaining = word.text;
		let mut leading_space = word.leading_space;
		while !remaining.is_empty() {
			let remaining_len = remaining.chars().count();
			let space_len = usize::from(leading_space && line_len > 0);
			let candidate_len = line_len + space_len + remaining_len;
			if line_len > 0 && candidate_len > max_chars {
				y = y.saturating_add(spec.line_h());
				line_len = 0;
				leading_space = false;
				continue;
			}

			let available = if line_len == 0 {
				max_chars
			} else {
				max_chars.saturating_sub(line_len + space_len)
			}
			.max(1);
			if remaining_len <= available {
				line_len += space_len;
				line_len += remaining_len;
				break;
			}

			y = y.saturating_add(spec.line_h());
			line_len = 0;
			remaining = skip_chars(&remaining, available);
			leading_space = false;
		}
	}

	if line_len > 0 {
		y = y.saturating_add(spec.line_h());
	}
	y
}

fn measure_wrapped_text(width: u32, x: u32, mut y: u32, text: &str, spec: TextSpec) -> u32 {
	if x >= width {
		return y;
	}
	let max_width = width.saturating_sub(x).max(spec.glyph_w());
	let max_chars = (max_width / spec.glyph_w()).max(1) as usize;
	let mut current = String::new();

	for word in text.split_whitespace() {
		let candidate_len = if current.is_empty() {
			word.chars().count()
		} else {
			current.chars().count() + 1 + word.chars().count()
		};
		if !current.is_empty() && candidate_len > max_chars {
			y = y.saturating_add(spec.line_h());
			current.clear();
		}
		if !current.is_empty() {
			current.push(' ');
		}
		current.push_str(word);
		while current.chars().count() > max_chars {
			current = current.chars().skip(max_chars).collect::<String>();
			y = y.saturating_add(spec.line_h());
		}
	}

	if !current.is_empty() {
		y = y.saturating_add(spec.line_h());
	}
	y
}

fn draw_markdown_block(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	block: &MarkdownBlock,
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	match block {
		MarkdownBlock::Heading { level, text, .. } => {
			draw_heading(canvas, x, y, *level, text, metrics, palette, text_renderer)
		}
		MarkdownBlock::Paragraph { text, .. } => draw_wrapped_inline_text(
			canvas,
			x,
			y,
			text,
			TextSpec::body(metrics, palette.body_text),
			TextSpec::code(metrics, palette.inline_code),
			text_renderer,
		)
		.saturating_add(metrics.cell_h.get() / 3),
		MarkdownBlock::CodeBlock { info, text, .. } => draw_code_block(
			canvas,
			x,
			y,
			info.as_deref(),
			text,
			metrics,
			palette,
			text_renderer,
		),
		MarkdownBlock::Quote { text, .. } => {
			draw_quote(canvas, x, y, text, metrics, palette, text_renderer)
		}
		MarkdownBlock::ListItem {
			depth,
			marker,
			checked,
			text,
			..
		} => draw_list_item(
			canvas,
			x,
			y,
			*depth,
			marker,
			*checked,
			text,
			metrics,
			palette,
			text_renderer,
		),
		MarkdownBlock::Rule { .. } => {
			let y = y.saturating_add(metrics.cell_h.get() / 2);
			let rule_right = canvas.width().saturating_sub(x);
			fill_rect(canvas, x, y, rule_right, y.saturating_add(2), palette.rule);
			y.saturating_add(metrics.cell_h.get())
		}
		MarkdownBlock::Table { rows, .. } => {
			draw_table(canvas, x, y, rows, metrics, palette, text_renderer)
		}
		MarkdownBlock::Html { text, .. } => draw_code_block(
			canvas,
			x,
			y,
			Some("html"),
			text,
			metrics,
			palette,
			text_renderer,
		),
	}
}

fn draw_heading(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	level: u8,
	text: &str,
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	let spec = TextSpec::heading(metrics, level, palette.heading);
	let y = y.saturating_add(if level <= 2 {
		metrics.cell_h.get() / 2
	} else {
		metrics.cell_h.get() / 4
	});
	let next_y = draw_wrapped_text(canvas, x, y, text, spec, text_renderer);
	if level <= 2 && next_y < canvas.visible_bottom() {
		let rule_y = next_y.saturating_sub(spec.line_gap / 2);
		fill_rect(
			canvas,
			x,
			rule_y,
			canvas.width().saturating_sub(x),
			rule_y.saturating_add(1),
			palette.rule,
		);
	}
	next_y.saturating_add(metrics.cell_h.get() / 2)
}

fn draw_quote(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	text: &MarkdownInlineText,
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	let bar_w = (metrics.cell_w.get() / 4).max(2);
	let body_x = x.saturating_add(metrics.cell_w.get());
	let before_y = y;
	let next_y = draw_wrapped_inline_text(
		canvas,
		body_x,
		y,
		text,
		TextSpec::body(metrics, palette.muted),
		TextSpec::code(metrics, palette.inline_code),
		text_renderer,
	);
	fill_rect(
		canvas,
		x,
		before_y,
		x.saturating_add(bar_w),
		next_y,
		palette.quote,
	);
	next_y.saturating_add(metrics.cell_h.get() / 3)
}

fn draw_list_item(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	depth: u16,
	marker: &ListMarker,
	checked: Option<bool>,
	text: &MarkdownInlineText,
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	let marker_color = match (checked, marker) {
		(Some(_), _) => palette.task_marker,
		(None, ListMarker::Unordered) => palette.bullet_marker,
		(None, ListMarker::Ordered(_)) => palette.ordered_marker,
	};
	let marker_spec = TextSpec::bold_body(metrics, marker_color);
	let text_spec = TextSpec::body(metrics, palette.body_text);
	let code_spec = TextSpec::code(metrics, palette.inline_code);
	let marker_x = list_marker_x(x, depth, metrics, canvas.width());
	let text_x = marker_x
		.saturating_add(if checked.is_some() {
			metrics.cell_w.get().saturating_mul(4)
		} else {
			metrics.cell_w.get().saturating_mul(3)
		})
		.min(canvas.width().saturating_sub(1));
	if let Some(checked) = checked {
		draw_checkbox(canvas, marker_x, y, checked, metrics, marker_color);
	} else if matches!(marker, ListMarker::Unordered) {
		let dot = (metrics.cell_h.get() / 3).max(4);
		let dot_y = y.saturating_add(metrics.cell_h.get() / 3);
		fill_rect(
			canvas,
			marker_x.saturating_add(metrics.cell_w.get() / 2),
			dot_y,
			marker_x
				.saturating_add(metrics.cell_w.get() / 2)
				.saturating_add(dot),
			dot_y.saturating_add(dot),
			marker_color,
		);
	} else {
		draw_text(
			text_renderer,
			canvas,
			marker_x,
			y,
			&marker_label(marker),
			marker_spec,
		);
	}
	draw_wrapped_inline_text(canvas, text_x, y, text, text_spec, code_spec, text_renderer)
		.saturating_add(metrics.cell_h.get() / 3)
}

fn list_marker_x(x: u32, depth: u16, metrics: &FontMetrics, width: u32) -> u32 {
	x.saturating_add(
		metrics
			.cell_w
			.get()
			.saturating_mul(2)
			.saturating_mul(depth as u32),
	)
	.min(width.saturating_sub(1))
}

fn draw_checkbox(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	checked: bool,
	metrics: &FontMetrics,
	color: Rgba<u8>,
) {
	let size = metrics
		.cell_h
		.get()
		.saturating_mul(2)
		.saturating_div(3)
		.max(7);
	let top = y.saturating_add((metrics.cell_h.get().saturating_sub(size)) / 2);
	let left = x.saturating_add(metrics.cell_w.get() / 3);
	fill_rect(
		canvas,
		left,
		top,
		left.saturating_add(size),
		top.saturating_add(1),
		color,
	);
	fill_rect(
		canvas,
		left,
		top.saturating_add(size.saturating_sub(1)),
		left.saturating_add(size),
		top.saturating_add(size),
		color,
	);
	fill_rect(
		canvas,
		left,
		top,
		left.saturating_add(1),
		top.saturating_add(size),
		color,
	);
	fill_rect(
		canvas,
		left.saturating_add(size.saturating_sub(1)),
		top,
		left.saturating_add(size),
		top.saturating_add(size),
		color,
	);
	if checked {
		let mid_y = top.saturating_add(size / 2);
		for step in 0..(size / 3).max(1) {
			fill_rect(
				canvas,
				left.saturating_add(2 + step),
				mid_y.saturating_add(step / 2),
				left.saturating_add(3 + step),
				mid_y.saturating_add(1 + step / 2),
				color,
			);
		}
		for step in 0..(size / 2).max(1) {
			fill_rect(
				canvas,
				left.saturating_add(size / 3 + step),
				mid_y.saturating_sub(step / 2),
				left.saturating_add(size / 3 + step + 1),
				mid_y.saturating_sub(step / 2).saturating_add(1),
				color,
			);
		}
	}
}

fn marker_label(marker: &ListMarker) -> String {
	match marker {
		ListMarker::Unordered => "-".to_string(),
		ListMarker::Ordered(number) => format!("{}.", number),
	}
}

fn draw_code_block(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	mut y: u32,
	info: Option<&str>,
	text: &str,
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	if let Some(info) = info.filter(|info| !info.trim().is_empty()) {
		let label = format!("[{}]", info.trim());
		y = draw_wrapped_text(
			canvas,
			x.saturating_add(metrics.cell_w.get()),
			y,
			&label,
			TextSpec::body(metrics, palette.muted),
			text_renderer,
		);
	}
	for line in text.lines() {
		y = draw_code_line(canvas, x, y, line, metrics, palette, text_renderer);
	}
	if text.is_empty() {
		y = draw_code_line(canvas, x, y, "", metrics, palette, text_renderer);
	}
	y.saturating_add(metrics.cell_h.get() / 3)
}

fn draw_code_line(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	line: &str,
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	let spec = TextSpec::code(metrics, palette.code_block);
	let block_pad_x = (metrics.cell_w.get() / 2).max(4);
	let block_pad_y = (metrics.cell_h.get() / 5).max(2);
	let block_top = y.saturating_sub(block_pad_y / 2);
	let block_bottom = y.saturating_add(spec.glyph_h()).saturating_add(block_pad_y);
	fill_rect(
		canvas,
		x.saturating_sub(block_pad_x / 2),
		block_top,
		canvas.width().saturating_sub(x / 2),
		block_bottom,
		palette.code_bg,
	);
	draw_text(
		text_renderer,
		canvas,
		x.saturating_add(block_pad_x),
		y.saturating_add(block_pad_y / 2),
		line,
		spec,
	);
	block_bottom.saturating_add(metrics.cell_h.get() / 5)
}

fn draw_table(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	mut y: u32,
	rows: &[Vec<MarkdownInlineText>],
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	if rows.is_empty() {
		return y;
	}
	let spec = TextSpec::code(metrics, palette.body_text);
	let header_spec = TextSpec::bold_code(metrics, palette.heading);
	let inline_code_spec = TextSpec::code(metrics, palette.inline_code);
	let columns = rows.iter().map(|row| row.len()).max().unwrap_or(0);
	if columns == 0 {
		return y;
	}
	let available_chars = canvas
		.width()
		.saturating_sub(x)
		.saturating_div(spec.glyph_w().max(1))
		.max(1) as usize;
	let (column_widths, gutter_chars) = table_column_widths(rows, available_chars);
	for (row_idx, row) in rows.iter().enumerate() {
		let row_top = y;
		let row_bottom = y.saturating_add(spec.line_h());
		if let Some(background) = table_row_background(row_idx, palette) {
			fill_rect(
				canvas,
				x.saturating_sub(metrics.cell_w.get() / 3),
				if row_idx == 0 {
					row_top.saturating_sub(metrics.cell_h.get() / 5)
				} else {
					row_top
				},
				canvas.width().saturating_sub(x / 2),
				row_bottom,
				background,
			);
		}
		let mut cell_offset = 0usize;
		for (col, &cell_width) in column_widths.iter().enumerate() {
			let empty_cell = MarkdownInlineText::new();
			let cell_text = row.get(col).unwrap_or(&empty_cell);
			let clipped = clipped_inline_chars(cell_text, cell_width);
			let cell_x = x.saturating_add(cell_offset as u32 * spec.glyph_w());
			draw_inline_text(
				text_renderer,
				canvas,
				cell_x,
				y,
				&clipped,
				if row_idx == 0 { header_spec } else { spec },
				inline_code_spec,
			);
			cell_offset = cell_offset
				.saturating_add(cell_width)
				.saturating_add(gutter_chars);
		}
		y = row_bottom;
		if row_idx == 0 {
			fill_rect(
				canvas,
				x.saturating_sub(metrics.cell_w.get() / 3),
				y,
				canvas.width().saturating_sub(x / 2),
				y.saturating_add(1),
				palette.rule,
			);
			y = y.saturating_add(metrics.cell_h.get() / 4);
		}
		if y >= canvas.visible_bottom() {
			break;
		}
	}
	y.saturating_add(metrics.cell_h.get() / 2)
}

fn table_row_background(row_idx: usize, palette: &MarkdownPreviewPalette) -> Option<Rgba<u8>> {
	match row_idx {
		0 => Some(palette.code_bg),
		index if index % 2 == 0 => Some(palette.table_row_alt),
		_ => None,
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct MarkdownInlineWord {
	style: MarkdownInlineStyle,
	text: String,
	leading_space: bool,
}

fn inline_words(text: &MarkdownInlineText) -> Vec<MarkdownInlineWord> {
	let mut words = Vec::new();
	let mut pending_space = false;
	for span in text.spans() {
		let mut word = String::new();
		for ch in span.text.chars() {
			if ch.is_whitespace() {
				if !word.is_empty() {
					words.push(MarkdownInlineWord {
						style: span.style,
						text: std::mem::take(&mut word),
						leading_space: pending_space && !words.is_empty(),
					});
				}
				pending_space = !words.is_empty();
				continue;
			}
			word.push(ch);
		}
		if !word.is_empty() {
			words.push(MarkdownInlineWord {
				style: span.style,
				text: word,
				leading_space: pending_space && !words.is_empty(),
			});
			pending_space = false;
		}
	}
	words
}

fn clipped_inline_chars(text: &MarkdownInlineText, max_chars: usize) -> MarkdownInlineText {
	if text.char_count() <= max_chars {
		return text.clone();
	}
	let keep = max_chars.saturating_sub(1).max(1);
	let mut remaining = keep;
	let mut clipped = MarkdownInlineText::new();
	for span in text.spans() {
		if remaining == 0 {
			break;
		}
		let span_chars = span.text.chars().count();
		let take = remaining.min(span_chars);
		clipped.push(span.style, &take_chars(&span.text, take));
		remaining = remaining.saturating_sub(take);
	}
	clipped.push(MarkdownInlineStyle::TEXT, "~");
	clipped
}

fn take_chars(text: &str, count: usize) -> String {
	text.chars().take(count).collect()
}

fn skip_chars(text: &str, count: usize) -> String {
	text.chars().skip(count).collect()
}

fn draw_wrapped_inline_text(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	mut y: u32,
	text: &MarkdownInlineText,
	body_spec: TextSpec,
	inline_code_spec: TextSpec,
	text_renderer: &TextRenderer,
) -> u32 {
	if x >= canvas.width() || y >= canvas.visible_bottom() {
		return y;
	}
	let max_width = canvas.width().saturating_sub(x).max(body_spec.glyph_w());
	let max_chars = (max_width / body_spec.glyph_w()).max(1) as usize;
	let mut line = Vec::new();
	let mut line_len = 0usize;

	for word in inline_words(text) {
		let style = word.style;
		let mut remaining = word.text;
		let mut leading_space = word.leading_space;
		while !remaining.is_empty() {
			let remaining_len = remaining.chars().count();
			let space_len = usize::from(leading_space && line_len > 0);
			let candidate_len = line_len + space_len + remaining_len;
			if line_len > 0 && candidate_len > max_chars {
				draw_inline_words(
					text_renderer,
					canvas,
					x,
					y,
					&line,
					body_spec,
					inline_code_spec,
				);
				y = y.saturating_add(body_spec.line_h());
				line.clear();
				line_len = 0;
				leading_space = false;
				if y >= canvas.visible_bottom() {
					return y;
				}
				continue;
			}

			let available = if line_len == 0 {
				max_chars
			} else {
				max_chars.saturating_sub(line_len + space_len)
			}
			.max(1);
			if remaining_len <= available {
				line_len += space_len;
				line_len += remaining_len;
				line.push(MarkdownInlineWord {
					style,
					text: remaining,
					leading_space: space_len > 0,
				});
				break;
			}

			let chunk = take_chars(&remaining, available);
			line.push(MarkdownInlineWord {
				style,
				text: chunk,
				leading_space: space_len > 0,
			});
			draw_inline_words(
				text_renderer,
				canvas,
				x,
				y,
				&line,
				body_spec,
				inline_code_spec,
			);
			y = y.saturating_add(body_spec.line_h());
			line.clear();
			line_len = 0;
			remaining = skip_chars(&remaining, available);
			leading_space = false;
			if y >= canvas.visible_bottom() {
				return y;
			}
		}
	}

	if !line.is_empty() {
		draw_inline_words(
			text_renderer,
			canvas,
			x,
			y,
			&line,
			body_spec,
			inline_code_spec,
		);
		y = y.saturating_add(body_spec.line_h());
	}
	y
}

fn draw_inline_words(
	text_renderer: &TextRenderer,
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	words: &[MarkdownInlineWord],
	body_spec: TextSpec,
	inline_code_spec: TextSpec,
) {
	let mut cursor = x;
	for (idx, word) in words.iter().enumerate() {
		if idx > 0 && word.leading_space {
			cursor =
				cursor.saturating_add(draw_text(text_renderer, canvas, cursor, y, " ", body_spec));
		}
		let spec = inline_text_spec(word.style, body_spec, inline_code_spec);
		cursor = cursor.saturating_add(draw_text(
			text_renderer,
			canvas,
			cursor,
			y,
			&word.text,
			spec,
		));
	}
}

fn draw_inline_text(
	text_renderer: &TextRenderer,
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	text: &MarkdownInlineText,
	body_spec: TextSpec,
	inline_code_spec: TextSpec,
) {
	let mut cursor = x;
	for span in text.spans() {
		let spec = inline_text_spec(span.style, body_spec, inline_code_spec);
		cursor = cursor.saturating_add(draw_text(
			text_renderer,
			canvas,
			cursor,
			y,
			&span.text,
			spec,
		));
	}
}

/// Inline style selects the face: strong is weight, emphasis is slant, and a
/// run that is both asks for both. `TextRenderer` resolves that against what
/// the configured family actually provides.
fn inline_text_spec(
	style: MarkdownInlineStyle,
	body_spec: TextSpec,
	inline_code_spec: TextSpec,
) -> TextSpec {
	let spec = if style.code {
		inline_code_spec
	} else {
		body_spec
	};
	TextSpec {
		bold: spec.bold || style.strong,
		italic: spec.italic || style.emphasis,
		..spec
	}
}

fn draw_wrapped_text(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	mut y: u32,
	text: &str,
	spec: TextSpec,
	text_renderer: &TextRenderer,
) -> u32 {
	if x >= canvas.width() || y >= canvas.visible_bottom() {
		return y;
	}
	let max_width = canvas.width().saturating_sub(x).max(spec.glyph_w());
	let max_chars = (max_width / spec.glyph_w()).max(1) as usize;
	let mut current = String::new();

	for word in text.split_whitespace() {
		let candidate_len = if current.is_empty() {
			word.chars().count()
		} else {
			current.chars().count() + 1 + word.chars().count()
		};
		if !current.is_empty() && candidate_len > max_chars {
			draw_text(text_renderer, canvas, x, y, &current, spec);
			y = y.saturating_add(spec.line_h());
			current.clear();
			if y >= canvas.visible_bottom() {
				return y;
			}
		}
		if !current.is_empty() {
			current.push(' ');
		}
		current.push_str(word);
		while current.chars().count() > max_chars {
			let chunk = current.chars().take(max_chars).collect::<String>();
			draw_text(text_renderer, canvas, x, y, &chunk, spec);
			current = current.chars().skip(max_chars).collect::<String>();
			y = y.saturating_add(spec.line_h());
			if y >= canvas.visible_bottom() {
				return y;
			}
		}
	}

	if !current.is_empty() {
		draw_text(text_renderer, canvas, x, y, &current, spec);
		y = y.saturating_add(spec.line_h());
	}
	y
}

fn draw_text(
	text_renderer: &TextRenderer,
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	text: &str,
	spec: TextSpec,
) -> u32 {
	let matches = canvas
		.search
		.as_ref()
		.map(|search| {
			search
				.regex
				.find_iter(text)
				.filter(|matched| matched.start() < matched.end())
				.map(|matched| (matched.start(), matched.end()))
				.collect::<Vec<_>>()
		})
		.unwrap_or_default();
	if matches.is_empty() {
		return draw_plain_text(text_renderer, canvas, x, y, text, spec);
	}

	let (background, foreground) = canvas
		.search
		.as_ref()
		.map(|search| (search.background, search.foreground))
		.expect("search exists when matches were collected");
	let mut cursor = x;
	let mut byte = 0usize;
	for (start, end) in matches {
		if start > byte {
			cursor = cursor.saturating_add(draw_plain_text(
				text_renderer,
				canvas,
				cursor,
				y,
				&text[byte..start],
				spec,
			));
		}
		let matched = &text[start..end];
		let width = measure_text_width(text_renderer, matched, spec);
		fill_rect(
			canvas,
			cursor,
			y,
			cursor.saturating_add(width),
			y.saturating_add(spec.glyph_h()),
			background,
		);
		cursor = cursor.saturating_add(draw_plain_text(
			text_renderer,
			canvas,
			cursor,
			y,
			matched,
			TextSpec {
				color: foreground,
				..spec
			},
		));
		byte = end;
	}
	if byte < text.len() {
		cursor = cursor.saturating_add(draw_plain_text(
			text_renderer,
			canvas,
			cursor,
			y,
			&text[byte..],
			spec,
		));
	}
	cursor.saturating_sub(x)
}

fn draw_plain_text(
	text_renderer: &TextRenderer,
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	text: &str,
	spec: TextSpec,
) -> u32 {
	if let Some((font, synthetic_bold)) = text_renderer.font_for(spec) {
		return draw_rasterized_text(canvas, x, y, text, spec, font, synthetic_bold);
	}
	draw_bitmap_text(canvas, x, y, text, spec)
}

fn measure_text_width(text_renderer: &TextRenderer, text: &str, spec: TextSpec) -> u32 {
	let Some((font, _)) = text_renderer.font_for(spec) else {
		return text.chars().fold(0u32, |width, ch| {
			width.saturating_add(
				spec.glyph_w()
					.saturating_mul(if ch == '\t' { 4 } else { 1 }),
			)
		});
	};

	let mut width = 0.0f32;
	let mut previous = None;
	for ch in text.chars() {
		if ch == '\t' {
			width += spec.glyph_w().saturating_mul(4) as f32;
			previous = None;
			continue;
		}
		if let Some(previous) = previous {
			width += font
				.horizontal_kern(previous, ch, spec.px_size)
				.unwrap_or(0.0);
		}
		let display = if ch.is_control() { '?' } else { ch };
		let metrics = font.metrics(display, spec.px_size);
		width += metrics.advance_width.max(1.0);
		previous = Some(display);
	}
	width.max(0.0).ceil().min(u32::MAX as f32) as u32
}

fn draw_rasterized_text(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	text: &str,
	spec: TextSpec,
	font: &Font,
	synthetic_bold: bool,
) -> u32 {
	let mut pen_x = x as f32;
	let baseline = text_baseline(font, y, spec);
	let mut previous = None;
	for ch in text.chars() {
		if pen_x >= canvas.width() as f32 || y >= canvas.visible_bottom() {
			break;
		}
		if ch == '\t' {
			pen_x += spec.glyph_w().saturating_mul(4) as f32;
			previous = None;
			continue;
		}
		if let Some(previous) = previous {
			pen_x += font
				.horizontal_kern(previous, ch, spec.px_size)
				.unwrap_or(0.0);
		}

		let display = if ch.is_control() { '?' } else { ch };
		let (glyph_metrics, bitmap) = font.rasterize(display, spec.px_size);
		let glyph_x = PixelDelta::new(pen_x.round() as i32 + glyph_metrics.xmin);
		let glyph_y = PixelDelta::new(
			baseline
				.saturating_sub(glyph_metrics.ymin)
				.saturating_sub(glyph_metrics.height as i32),
		);
		draw_rasterized_glyph(
			canvas,
			glyph_x,
			glyph_y,
			&glyph_metrics,
			&bitmap,
			spec.color,
		);
		if let (true, Some(bold_glyph_x)) = (synthetic_bold, glyph_x.checked_add_unsigned(1)) {
			draw_rasterized_glyph(
				canvas,
				bold_glyph_x,
				glyph_y,
				&glyph_metrics,
				&bitmap,
				spec.color,
			);
		}

		pen_x += glyph_metrics.advance_width.max(1.0);
		previous = Some(display);
	}
	let end_x = pen_x.max(x as f32).ceil().min(u32::MAX as f32) as u32;
	end_x.saturating_sub(x)
}

fn text_baseline(font: &Font, y: u32, spec: TextSpec) -> i32 {
	if let Some(line_metrics) = font.horizontal_line_metrics(spec.px_size) {
		let content_h = (line_metrics.ascent - line_metrics.descent).ceil().max(1.0);
		let top_pad = (spec.glyph_h() as f32 - content_h).max(0.0) / 2.0;
		(y as f32 + top_pad + line_metrics.ascent).round() as i32
	} else {
		(y as f32 + spec.px_size * 0.85).round() as i32
	}
}

fn draw_rasterized_glyph(
	canvas: &mut PreviewCanvas<'_>,
	glyph_x: PixelDelta,
	glyph_y: PixelDelta,
	metrics: &fontdue::Metrics,
	bitmap: &[u8],
	color: Rgba<u8>,
) {
	for row in 0..metrics.height {
		for col in 0..metrics.width {
			let Some(coverage) = bitmap.get(row.saturating_mul(metrics.width).saturating_add(col))
			else {
				continue;
			};
			if *coverage == 0 {
				continue;
			}
			let Some(x_delta) = glyph_x.checked_add_unsigned(col as u32) else {
				continue;
			};
			let Some(y_delta) = glyph_y.checked_add_unsigned(row as u32) else {
				continue;
			};
			let Some(x) = PixelX::ZERO.checked_offset(x_delta) else {
				continue;
			};
			let Some(y) = PixelY::ZERO.checked_offset(y_delta) else {
				continue;
			};
			blend_coverage_pixel(canvas, x, y, color, Coverage::new(*coverage));
		}
	}
}

fn blend_coverage_pixel(
	canvas: &mut PreviewCanvas<'_>,
	x: PixelX,
	y: PixelY,
	color: Rgba<u8>,
	coverage: Coverage,
) {
	if x.get() >= canvas.width() {
		return;
	}
	let logical_y = y.boundary_before();
	let scroll_px = canvas.scroll_px.boundary_before();
	if logical_y < scroll_px || logical_y.get() >= canvas.visible_bottom() {
		return;
	}
	let screen_y = logical_y.saturating_sub(scroll_px.get()).get();
	let coverage = coverage.get();
	let alpha = (coverage as u32)
		.saturating_mul(color[3] as u32)
		.saturating_add(127)
		/ 255;
	if alpha == 0 {
		return;
	}
	let inverse = 255u32.saturating_sub(alpha);
	let existing = *canvas.image.get_pixel(x.get(), screen_y);
	let blended = Rgba([
		((color[0] as u32 * alpha + existing[0] as u32 * inverse + 127) / 255) as u8,
		((color[1] as u32 * alpha + existing[1] as u32 * inverse + 127) / 255) as u8,
		((color[2] as u32 * alpha + existing[2] as u32 * inverse + 127) / 255) as u8,
		255,
	]);
	canvas.image.put_pixel(x.get(), screen_y, blended);
}

fn draw_bitmap_text(
	canvas: &mut PreviewCanvas<'_>,
	mut x: u32,
	y: u32,
	text: &str,
	spec: TextSpec,
) -> u32 {
	let start_x = x;
	for ch in text.chars() {
		if x >= canvas.width() || y >= canvas.visible_bottom() {
			break;
		}
		if ch == '\t' {
			x = x.saturating_add(spec.glyph_w().saturating_mul(4));
			continue;
		}
		draw_glyph(canvas, x, y, display_char(ch), spec);
		if spec.bold {
			draw_glyph(canvas, x.saturating_add(1), y, display_char(ch), spec);
		}
		x = x.saturating_add(spec.glyph_w());
	}
	x.saturating_sub(start_x)
}

fn draw_glyph(canvas: &mut PreviewCanvas<'_>, x: u32, y: u32, ch: char, spec: TextSpec) {
	if ch == ' ' {
		return;
	}
	let glyph = BASIC_FONTS
		.get(ch)
		.or_else(|| BASIC_FONTS.get('?'))
		.unwrap_or([0; 8]);
	for (row, bits) in glyph.iter().copied().enumerate() {
		for col in 0..8 {
			if bits & (1 << col) == 0 {
				continue;
			}
			let px = x.saturating_add((col as u32).saturating_mul(spec.fallback_scale_x));
			let py = y.saturating_add((row as u32).saturating_mul(spec.fallback_scale_y));
			fill_rect(
				canvas,
				px,
				py,
				px.saturating_add(spec.fallback_scale_x),
				py.saturating_add(spec.fallback_scale_y),
				spec.color,
			);
		}
	}
}

fn fill_rect(canvas: &mut PreviewCanvas<'_>, x0: u32, y0: u32, x1: u32, y1: u32, color: Rgba<u8>) {
	let scroll_px = canvas.scroll_px.get();
	if y1 <= scroll_px || y0 >= canvas.visible_bottom() {
		return;
	}
	let x0 = x0.min(canvas.width());
	let x1 = x1.min(canvas.width());
	let y0 = y0.max(scroll_px);
	let y1 = y1.min(canvas.visible_bottom());
	for y in y0..y1 {
		let screen_y = y.saturating_sub(scroll_px);
		for x in x0..x1 {
			canvas.image.put_pixel(x, screen_y, color);
		}
	}
}

fn display_char(ch: char) -> char {
	if ch.is_ascii() && !ch.is_control() {
		ch
	} else {
		'?'
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use libaryon::core::DocLine;
	use libaryon::svp::rust_highlight::TokenCategory;

	fn inline_text(text: &str) -> MarkdownInlineText {
		let mut inline = MarkdownInlineText::new();
		inline.push(MarkdownInlineStyle::TEXT, text);
		inline
	}

	fn row_plain_text(row: &[MarkdownInlineText]) -> Vec<String> {
		row.iter().map(MarkdownInlineText::plain_text).collect()
	}

	#[test]
	fn parser_builds_preview_blocks_with_ranges() {
		let blocks = parse_markdown_blocks(
			"# Title\n\n- [x] done\n2. next\n\n| A | B |\n| --- | --- |\n| 1 | 2 |\n\n```rust\nfn main() {}\n```\n",
		);
		assert!(matches!(
			blocks.first(),
			Some(MarkdownBlock::Heading { level: 1, text, .. }) if text == "Title"
		));
		assert!(blocks.iter().any(|block| matches!(
			block,
			MarkdownBlock::ListItem {
				marker: ListMarker::Unordered,
				checked: Some(true),
				text,
				..
			} if text.plain_text() == "done"
		)));
		assert!(blocks.iter().any(|block| matches!(
			block,
			MarkdownBlock::ListItem {
				marker: ListMarker::Ordered(2),
				text,
				..
			} if text.plain_text() == "next"
		)));
		assert!(blocks.iter().any(|block| matches!(
			block,
			MarkdownBlock::Table { rows, .. }
				if rows.len() == 2 && row_plain_text(&rows[0]) == vec!["A".to_string(), "B".to_string()]
		)));
		assert!(blocks.iter().any(|block| matches!(
			block,
			MarkdownBlock::CodeBlock {
				info: Some(info),
				language: Some(language),
				text,
				..
			} if info == "rust" && language == "rust" && text.contains("fn main")
		)));
		assert!(
			blocks
				.iter()
				.all(|block| block.source_range().start < block.source_range().end)
		);
	}

	#[test]
	fn table_columns_allocate_width_from_content_demand() {
		let rows = vec![
			vec![inline_text("ID"), inline_text("Description")],
			vec![
				inline_text("1"),
				inline_text("This column needs substantially more room than the ID"),
			],
		];
		let (widths, gutter) = table_column_widths(&rows, 60);

		assert_eq!(gutter, 2);
		assert_eq!(widths.iter().sum::<usize>() + gutter, 60);
		assert!(widths[1] >= widths[0].saturating_mul(2));
		assert!(widths[0] >= 4);
	}

	#[test]
	fn table_columns_stay_balanced_for_similar_content() {
		let rows = vec![vec![inline_text("left"), inline_text("rght")]];
		let (widths, gutter) = table_column_widths(&rows, 60);

		assert_eq!(widths, vec![29, 29]);
		assert_eq!(gutter, 2);
	}

	#[test]
	fn table_columns_bound_outliers_and_compress_to_available_width() {
		let rows = vec![vec![
			inline_text("customer key"),
			inline_text("order status"),
			inline_text("ship date"),
			inline_text(&"comment ".repeat(100)),
		]];
		let (widths, gutter) = table_column_widths(&rows, 80);

		assert_eq!(widths.iter().sum::<usize>() + gutter * 3, 80);
		assert!(widths.iter().all(|width| *width >= 4));
		assert!(widths[3] > widths[0]);
		assert!(widths[0] > 4, "the outlier must not starve other columns");
	}

	#[test]
	fn inline_words_preserve_adjacent_span_boundaries_without_extra_spaces() {
		let mut text = MarkdownInlineText::new();
		text.push(MarkdownInlineStyle::TEXT, "a");
		text.push(MarkdownInlineStyle::CODE, "b");
		text.push(MarkdownInlineStyle::TEXT, " c");

		assert_eq!(
			inline_words(&text),
			vec![
				MarkdownInlineWord {
					style: MarkdownInlineStyle::TEXT,
					text: "a".to_string(),
					leading_space: false,
				},
				MarkdownInlineWord {
					style: MarkdownInlineStyle::CODE,
					text: "b".to_string(),
					leading_space: false,
				},
				MarkdownInlineWord {
					style: MarkdownInlineStyle::TEXT,
					text: "c".to_string(),
					leading_space: true,
				}
			]
		);
	}

	fn all_faces() -> FaceAvailability {
		FaceAvailability {
			regular: true,
			bold: true,
			italic: true,
			bold_italic: true,
		}
	}

	#[test]
	fn each_inline_style_selects_its_own_face() {
		let available = all_faces();

		assert_eq!(
			choose_face(false, false, available),
			Some((FaceChoice::Regular, false)),
			"body text"
		);
		assert_eq!(
			choose_face(true, false, available),
			Some((FaceChoice::Bold, false)),
			"strong"
		);
		assert_eq!(
			choose_face(false, true, available),
			Some((FaceChoice::Italic, false)),
			"emphasis"
		);
		assert_eq!(
			choose_face(true, true, available),
			Some((FaceChoice::BoldItalic, false)),
			"strong emphasis"
		);
	}

	/// Weight can be synthesized, slant cannot. A family missing a face should
	/// cost as little as possible, and never cost the text itself.
	#[test]
	fn missing_faces_fall_back_without_losing_the_text() {
		let regular_only = FaceAvailability {
			regular: true,
			bold: false,
			italic: false,
			bold_italic: false,
		};
		assert_eq!(
			choose_face(true, false, regular_only),
			Some((FaceChoice::Regular, true)),
			"a missing bold is emboldened from regular"
		);
		assert_eq!(
			choose_face(false, true, regular_only),
			Some((FaceChoice::Regular, false)),
			"a missing italic renders upright rather than not at all, and is \
			 never faked by emboldening"
		);
		assert_eq!(
			choose_face(true, true, regular_only),
			Some((FaceChoice::Regular, true)),
			"both missing: keep the weight, lose the slant"
		);

		let no_bold_italic = FaceAvailability {
			regular: true,
			bold: true,
			italic: true,
			bold_italic: false,
		};
		assert_eq!(
			choose_face(true, true, no_bold_italic),
			Some((FaceChoice::Italic, true)),
			"embolden a real italic rather than drop the slant"
		);

		let bold_only_upright = FaceAvailability {
			regular: true,
			bold: true,
			italic: false,
			bold_italic: false,
		};
		assert_eq!(
			choose_face(true, true, bold_only_upright),
			Some((FaceChoice::Bold, false)),
			"with no italic at all, a real bold beats an emboldened regular"
		);

		let nothing = FaceAvailability {
			regular: false,
			bold: false,
			italic: false,
			bold_italic: false,
		};
		assert_eq!(
			choose_face(false, false, nothing),
			None,
			"no faces at all falls through to the bitmap renderer"
		);
	}

	#[test]
	fn inline_style_reaches_the_face_selection() {
		let metrics = FontMetrics {
			cell_w: PixelWidth::new(8),
			cell_h: PixelHeight::new(16),
			body_scale_x: 1,
			body_scale_y: 2,
			body_role: FontRole::Sans,
		};
		let body = TextSpec::body(&metrics, TEXT_RGBA);
		let code = TextSpec::code(&metrics, CODE_TEXT_RGBA);
		let spec_for = |style| inline_text_spec(style, body, code);

		let emphasis = spec_for(MarkdownInlineStyle {
			emphasis: true,
			..MarkdownInlineStyle::TEXT
		});
		assert!(emphasis.italic && !emphasis.bold);

		let strong_emphasis = spec_for(MarkdownInlineStyle {
			strong: true,
			emphasis: true,
			..MarkdownInlineStyle::TEXT
		});
		assert!(strong_emphasis.bold && strong_emphasis.italic);

		let emphasized_code = spec_for(MarkdownInlineStyle {
			emphasis: true,
			..MarkdownInlineStyle::CODE
		});
		assert!(emphasized_code.italic);
		assert_eq!(
			emphasized_code.role,
			FontRole::Mono,
			"emphasis does not cost inline code its face"
		);

		let plain = spec_for(MarkdownInlineStyle::TEXT);
		assert!(!plain.bold && !plain.italic);
	}

	#[test]
	fn strong_selects_the_bold_face_and_code_keeps_the_mono_face() {
		let metrics = FontMetrics {
			cell_w: PixelWidth::new(8),
			cell_h: PixelHeight::new(16),
			body_scale_x: 1,
			body_scale_y: 2,
			body_role: FontRole::Sans,
		};
		let body = TextSpec::body(&metrics, TEXT_RGBA);
		let code = TextSpec::code(&metrics, CODE_TEXT_RGBA);

		let plain = inline_text_spec(MarkdownInlineStyle::TEXT, body, code);
		assert!(!plain.bold);
		assert_eq!(plain.role, FontRole::Sans);

		let strong = inline_text_spec(
			MarkdownInlineStyle {
				strong: true,
				..MarkdownInlineStyle::TEXT
			},
			body,
			code,
		);
		assert!(strong.bold, "strong text renders in the bold face");
		assert_eq!(strong.role, FontRole::Sans);

		let strong_code = inline_text_spec(
			MarkdownInlineStyle {
				strong: true,
				..MarkdownInlineStyle::CODE
			},
			body,
			code,
		);
		assert!(strong_code.bold);
		assert_eq!(
			strong_code.role,
			FontRole::Mono,
			"a bold inline-code run stays monospaced"
		);

		// Emphasis has no face of its own yet, so it must at least not change
		// the one it is drawn in.
		let emphasis = inline_text_spec(
			MarkdownInlineStyle {
				emphasis: true,
				..MarkdownInlineStyle::TEXT
			},
			body,
			code,
		);
		assert!(!emphasis.bold);
		assert_eq!(emphasis.role, FontRole::Sans);
	}

	#[test]
	fn inline_wrap_does_not_inject_space_between_adjacent_spans() {
		let mut text = MarkdownInlineText::new();
		text.push(MarkdownInlineStyle::TEXT, "a");
		text.push(MarkdownInlineStyle::CODE, "b");
		let metrics = FontMetrics {
			cell_w: PixelWidth::new(8),
			cell_h: PixelHeight::new(16),
			body_scale_x: 1,
			body_scale_y: 2,
			body_role: FontRole::Sans,
		};
		let spec = TextSpec::body(&metrics, TEXT_RGBA);

		assert_eq!(
			measure_wrapped_inline_text(spec.glyph_w().saturating_mul(2), 0, 0, &text, spec),
			spec.line_h()
		);
	}

	#[test]
	fn layout_maps_source_scroll_line_into_rendered_block_pixels() {
		let mut source = String::from("# Title\n\n```text\n");
		for idx in 0..30 {
			source.push_str(&format!("line {idx}\n"));
		}
		source.push_str("```\n\nAfter\n");
		let blocks = parse_markdown_blocks(&source);
		let metrics = FontMetrics {
			cell_w: PixelWidth::new(8),
			cell_h: PixelHeight::new(16),
			body_scale_x: 1,
			body_scale_y: 2,
			body_role: FontRole::Sans,
		};
		let layout = build_preview_layout(
			&source,
			&blocks,
			&metrics,
			PixelWidth::new(640),
			PixelHeight::new(96),
			8,
			PixelHeight::new(8),
			18,
			8,
		);
		let code_idx = blocks
			.iter()
			.position(|block| matches!(block, MarkdownBlock::CodeBlock { .. }))
			.expect("code block");
		let code_layout = &layout.blocks[code_idx];

		assert!(layout.scroll_px > code_layout.y_start);
		assert!(layout.scroll_px.boundary_before() < code_layout.y_end);
	}

	#[test]
	fn layout_scrolls_to_rendered_bottom_when_source_viewport_is_at_eof() {
		let mut source = String::new();
		for section in 0..16 {
			source.push_str(&format!("## Section {section}\n\n"));
			source.push_str("This paragraph has enough words to wrap in the preview pane and make the rendered document taller than the source viewport.\n\n");
		}
		let blocks = parse_markdown_blocks(&source);
		let metrics = FontMetrics {
			cell_w: PixelWidth::new(8),
			cell_h: PixelHeight::new(16),
			body_scale_x: 1,
			body_scale_y: 2,
			body_role: FontRole::Sans,
		};
		let width = PixelWidth::new(320);
		let height = PixelHeight::new(128);
		let padding_y = PixelHeight::new(8);
		let layout = build_preview_layout(
			&source,
			&blocks,
			&metrics,
			width,
			height,
			8,
			padding_y,
			source_line_count(&source).saturating_sub(12),
			12,
		);
		let document_height = layout
			.blocks
			.last()
			.map(|block| block.y_end)
			.unwrap_or(padding_y)
			.saturating_add(padding_y.get());

		assert_eq!(
			layout.scroll_px,
			document_height
				.saturating_sub(height.get())
				.start_position()
		);
	}

	#[test]
	fn render_image_contains_non_background_pixels() {
		let snapshot = MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			source_path: Some("README.md".to_string()),
			source_text: "# Title\n\n- item\n\n```rust\nfn main() {}\n```\n".to_string(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		};
		let image = render_markdown_preview_image(
			&snapshot,
			Rect {
				x: 0,
				y: 0,
				width: 80,
				height: 24,
			},
			(8, 16),
		)
		.to_rgba8();
		assert!(image.pixels().any(|pixel| *pixel != BG_RGBA));
	}

	#[test]
	fn render_image_highlights_search_matches_with_custom_color() {
		let snapshot = MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			source_path: Some("README.md".to_string()),
			source_text: "Alpha needle omega\n\n- another NEEDLE\n".to_string(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		};
		let search = PreviewSearchConfig {
			pattern: Some("needle".to_string()),
			case_insensitive: true,
			highlight: Rgb::new(12, 200, 240),
		};
		let image = render_markdown_preview_image_with_search(
			&snapshot,
			Rect {
				x: 0,
				y: 0,
				width: 80,
				height: 24,
			},
			(8, 16),
			&[None; CATEGORY_COUNT],
			&search,
		)
		.to_rgba8();

		assert!(
			image
				.pixels()
				.any(|pixel| *pixel == Rgba([12, 200, 240, 255]))
		);
	}

	#[test]
	fn preview_palette_maps_markdown_roles_to_theme_tokens() {
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::String as usize] = Some(Rgb::new(11, 22, 33));
		theme_colors[TokenCategory::Comment as usize] = Some(Rgb::new(44, 55, 66));
		theme_colors[TokenCategory::Type as usize] = Some(Rgb::new(77, 88, 99));
		theme_colors[TokenCategory::Operator as usize] = Some(Rgb::new(111, 122, 133));
		theme_colors[TokenCategory::Number as usize] = Some(Rgb::new(122, 133, 144));
		theme_colors[TokenCategory::Constant as usize] = Some(Rgb::new(133, 144, 155));
		theme_colors[TokenCategory::Attribute as usize] = Some(Rgb::new(144, 155, 166));
		theme_colors[TokenCategory::Function as usize] = Some(Rgb::new(155, 166, 177));
		theme_colors[TokenCategory::Punctuation as usize] = Some(Rgb::new(166, 177, 188));

		let ui_palette = UiPalette::default();
		let palette = MarkdownPreviewPalette::from_theme(
			&theme_colors,
			ui_palette.preview_text,
			ui_palette.accent,
		);

		// Asserted against the shared table rather than against literals: the
		// point is that this preview cannot drift from the source highlighter
		// or from Polariton, not that a heading is any particular color.
		let expect = |construct, fallback| {
			color_rgba(markdown_construct_color(construct, &theme_colors, fallback))
		};
		assert_eq!(
			palette.body_text,
			color_rgba(ui_palette.preview_text),
			"prose has no category on either side, so it stays default"
		);
		assert_eq!(
			palette.heading,
			expect(MarkdownConstruct::Heading, ui_palette.accent)
		);
		assert_eq!(
			palette.quote,
			expect(MarkdownConstruct::Quote, ui_palette.accent)
		);
		assert_eq!(
			palette.muted,
			expect(MarkdownConstruct::Quote, ui_palette.accent)
		);
		assert_eq!(
			palette.bullet_marker,
			expect(MarkdownConstruct::BulletMarker, ui_palette.accent)
		);
		assert_eq!(
			palette.ordered_marker,
			expect(MarkdownConstruct::OrderedMarker, ui_palette.accent)
		);
		assert_eq!(
			palette.task_marker, palette.bullet_marker,
			"a task box follows its item's marker"
		);
		assert_eq!(
			palette.inline_code,
			expect(MarkdownConstruct::InlineCode, ui_palette.accent),
			"inline code is String here exactly as it is in the document"
		);
		assert_eq!(
			palette.code_block,
			expect(MarkdownConstruct::CodeText, ui_palette.accent)
		);
		assert_eq!(
			palette.rule,
			expect(MarkdownConstruct::Rule, ui_palette.accent)
		);
		assert_eq!(palette.code_bg, CODE_BG_RGBA);
		assert_eq!(palette.table_row_alt, TABLE_ROW_ALT_RGBA);
	}

	#[test]
	fn preview_request_fingerprint_includes_theme_and_search_state() {
		let snapshot = MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			source_path: Some("README.md".to_string()),
			source_text: "plain body text\n".to_string(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		};
		let area = Rect {
			x: 0,
			y: 0,
			width: 80,
			height: 24,
		};
		let base_theme = [None; CATEGORY_COUNT];
		let mut themed = base_theme;
		themed[TokenCategory::String as usize] = Some(Rgb::new(255, 0, 0));
		let no_search = PreviewSearchConfig {
			pattern: None,
			case_insensitive: false,
			highlight: libaryon::core::SEARCH_MATCH_BG,
		};
		let search = PreviewSearchConfig {
			pattern: Some("body".to_string()),
			case_insensitive: false,
			highlight: Rgb::new(12, 200, 240),
		};
		let ui_palette = UiPalette::default();

		assert_ne!(
			preview_request_fingerprint(
				&snapshot,
				area,
				(8, 16),
				&base_theme,
				&no_search,
				ui_palette,
			),
			preview_request_fingerprint(&snapshot, area, (8, 16), &themed, &no_search, ui_palette,)
		);
		assert_ne!(
			preview_request_fingerprint(
				&snapshot,
				area,
				(8, 16),
				&base_theme,
				&no_search,
				ui_palette,
			),
			preview_request_fingerprint(&snapshot, area, (8, 16), &base_theme, &search, ui_palette,)
		);

		let mut serif_snapshot = snapshot.clone();
		serif_snapshot.serif = true;
		assert_ne!(
			preview_request_fingerprint(
				&snapshot,
				area,
				(8, 16),
				&base_theme,
				&no_search,
				ui_palette,
			),
			preview_request_fingerprint(
				&serif_snapshot,
				area,
				(8, 16),
				&base_theme,
				&no_search,
				ui_palette,
			)
		);

		assert_ne!(
			preview_request_fingerprint(
				&snapshot,
				area,
				(8, 16),
				&base_theme,
				&no_search,
				ui_palette,
			),
			preview_request_fingerprint(
				&snapshot,
				area,
				(8, 16),
				&base_theme,
				&no_search,
				UiPalette {
					preview_surface: Rgb::new(1, 2, 3),
					preview_text: Rgb::new(4, 5, 6),
					accent: Rgb::new(7, 8, 9),
					..ui_palette
				},
			)
		);

		let moved_area = Rect {
			x: 17,
			y: 9,
			..area
		};
		let resized_area = Rect { width: 81, ..area };
		let fingerprint = preview_request_fingerprint(
			&snapshot,
			area,
			(8, 16),
			&base_theme,
			&no_search,
			ui_palette,
		);
		assert_eq!(
			fingerprint,
			preview_request_fingerprint(
				&snapshot,
				moved_area,
				(8, 16),
				&base_theme,
				&no_search,
				ui_palette,
			)
		);
		assert_ne!(
			fingerprint,
			preview_request_fingerprint(
				&snapshot,
				resized_area,
				(8, 16),
				&base_theme,
				&no_search,
				ui_palette,
			)
		);
	}

	#[test]
	fn preview_image_and_terminal_fallback_share_custom_surface_and_text() {
		let snapshot = MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			source_path: None,
			source_text: String::new(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 3,
		};
		let area = Rect::new(0, 0, 20, 3);
		let surface = Rgb::new(1, 2, 3);
		let text = Rgb::new(4, 5, 6);
		let search = PreviewSearchConfig {
			pattern: None,
			case_insensitive: false,
			highlight: UiPalette::default().search_highlight,
		};
		let palette = UiPalette {
			preview_surface: surface,
			preview_text: text,
			accent: Rgb::new(7, 8, 9),
			..UiPalette::default()
		};
		let preview_palette = MarkdownPreviewPalette::from_theme(
			&[None; CATEGORY_COUNT],
			palette.preview_text,
			palette.accent,
		);
		assert_eq!(preview_palette.body_text, color_rgba(text));
		assert_eq!(preview_palette.bullet_marker, color_rgba(palette.accent));
		let image = render_markdown_preview_image_with_renderer(
			&snapshot,
			area,
			(8, 16),
			&[None; CATEGORY_COUNT],
			&search,
			palette,
			&TextRenderer::load(),
		)
		.to_rgba8();
		assert!(image.pixels().all(|pixel| *pixel == color_rgba(surface)));

		let mut buffer = Buffer::empty(area);
		render_markdown_preview_fallback(&mut buffer, area, true, palette);
		for row in 0..area.height {
			for col in 0..area.width {
				assert_eq!(buffer[(col, row)].bg, crate::ui::ratatui_color(surface));
				assert_eq!(buffer[(col, row)].fg, crate::ui::ratatui_color(text));
			}
		}
	}

	#[test]
	fn serif_preview_uses_serif_for_prose_but_not_code() {
		let metrics = FontMetrics {
			cell_w: PixelWidth::new(8),
			cell_h: PixelHeight::new(16),
			body_scale_x: 1,
			body_scale_y: 2,
			body_role: FontRole::Serif,
		};

		assert!(matches!(
			TextSpec::body(&metrics, TEXT_RGBA).role,
			FontRole::Serif
		));
		assert!(matches!(
			TextSpec::heading(&metrics, 1, HEADING_RGBA).role,
			FontRole::Serif
		));
		assert!(matches!(
			TextSpec::code(&metrics, CODE_TEXT_RGBA).role,
			FontRole::Mono
		));
	}

	#[test]
	fn rendered_tables_use_alternating_data_row_backgrounds() {
		let palette = MarkdownPreviewPalette::from_theme(
			&[None; CATEGORY_COUNT],
			UiPalette::default().preview_text,
			UiPalette::default().accent,
		);
		assert_eq!(table_row_background(0, &palette), Some(CODE_BG_RGBA));
		assert_eq!(table_row_background(1, &palette), None);
		assert_eq!(table_row_background(2, &palette), Some(TABLE_ROW_ALT_RGBA));

		let snapshot = MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			source_path: Some("results.md".to_string()),
			source_text: "| Key | Value |\n| --- | --- |\n| first | 1 |\n| second | 2 |\n"
				.to_string(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		};
		let image = render_markdown_preview_image(
			&snapshot,
			Rect {
				x: 0,
				y: 0,
				width: 80,
				height: 24,
			},
			(8, 16),
		)
		.to_rgba8();

		assert!(image.pixels().any(|pixel| *pixel == TABLE_ROW_ALT_RGBA));
	}

	#[test]
	fn render_image_draws_body_text_in_the_default_foreground() {
		let snapshot = MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			source_path: Some("README.md".to_string()),
			source_text: "plain body text\n".to_string(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		};
		// A loud String color must not leak onto prose: the document does not
		// color its own body text, so neither does the preview.
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::String as usize] = Some(Rgb::new(255, 0, 0));

		let image = render_markdown_preview_image_with_theme(
			&snapshot,
			Rect {
				x: 0,
				y: 0,
				width: 80,
				height: 24,
			},
			(8, 16),
			&theme_colors,
		)
		.to_rgba8();

		assert!(
			image.pixels().any(|pixel| *pixel != BG_RGBA),
			"body text is drawn"
		);
		assert!(
			!image.pixels().any(|pixel| {
				*pixel != BG_RGBA
					&& pixel[0] > pixel[1].saturating_add(8)
					&& pixel[0] > pixel[2].saturating_add(8)
			}),
			"prose must not pick up the String color"
		);
	}

	#[test]
	fn render_image_uses_distinct_theme_colors_for_list_markers_and_inline_code() {
		let snapshot = MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			source_path: Some("README.md".to_string()),
			source_text: "- item `code`\n1. next\n".to_string(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		};
		let mut theme_colors = [None; CATEGORY_COUNT];
		// Inline code takes String, the category the document gives it.
		theme_colors[TokenCategory::String as usize] = Some(Rgb::new(255, 0, 0));
		theme_colors[TokenCategory::Operator as usize] = Some(Rgb::new(0, 0, 255));
		theme_colors[TokenCategory::Number as usize] = Some(Rgb::new(255, 0, 255));

		let image = render_markdown_preview_image_with_theme(
			&snapshot,
			Rect {
				x: 0,
				y: 0,
				width: 160,
				height: 48,
			},
			(8, 16),
			&theme_colors,
		)
		.to_rgba8();

		assert!(image.pixels().any(|pixel| {
			*pixel != BG_RGBA
				&& pixel[0] > pixel[1].saturating_add(8)
				&& pixel[0] > pixel[2].saturating_add(8)
		}));
		assert!(image.pixels().any(|pixel| {
			*pixel != BG_RGBA
				&& pixel[2] > pixel[0].saturating_add(8)
				&& pixel[2] > pixel[1].saturating_add(8)
		}));
		assert!(image.pixels().any(|pixel| {
			*pixel != BG_RGBA
				&& pixel[0] > pixel[1].saturating_add(8)
				&& pixel[2] > pixel[1].saturating_add(8)
		}));
	}
}
