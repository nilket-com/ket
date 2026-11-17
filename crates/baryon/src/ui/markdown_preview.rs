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
use libaryon::svp::markdown_table_layout::{
	MarkdownInlineWord, inline_line_count, inline_words, table_column_widths, wrap_inline_words,
};
use libaryon::svp::rust_highlight::{CATEGORY_COUNT, HighlightSpan};
use libaryon::uast::projection::MarkdownPreviewSnapshot;
use ratatui::{Frame, buffer::Buffer, layout::Rect, style::Style};
use ratatui_image::{Image, Resize, picker::Picker, protocol::Protocol};
use regex_automata::{meta::Regex, util::syntax};
use std::{
	borrow::Cow,
	collections::HashMap,
	collections::hash_map::DefaultHasher,
	hash::{Hash, Hasher},
	ops::Range,
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
	pub fn new(picker: Picker) -> Self {
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
		code_advance: font_w.get(),
	};
	// Only wrapped layout depends on the true mono advance; without wrap the
	// preview keeps its cell-width column arithmetic, pixel for pixel.
	let metrics = if snapshot.wrap_enabled {
		FontMetrics {
			code_advance: measured_code_advance(text_renderer, &metrics),
			..metrics
		}
	} else {
		metrics
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
		snapshot.wrap_enabled,
	);
	let palette = MarkdownPreviewPalette::from_theme(
		theme_colors,
		ui_palette.preview_text,
		ui_palette.accent,
	);
	let visible_bottom = (layout.scroll_px + height_px).boundary_before();
	let mut canvas = PreviewCanvas::new(&mut image, layout.scroll_px, PreviewSearch::new(search));
	canvas.code_highlighting = Some(CodeHighlighting {
		source: &snapshot.source_text,
		spans: &snapshot.code_highlights,
		theme_colors,
	});

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
			snapshot.wrap_enabled,
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
	wrap: bool,
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
			wrap,
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
	/// Whole pixels one monospace glyph is allowed to advance the pen by,
	/// measured from the loaded font ([`measured_code_advance`]). Column
	/// arithmetic for code rows and table cells uses this, not `cell_w`: a mono
	/// face at 0.86 of the cell height usually advances a fraction of a pixel
	/// more than a terminal cell, which over a hundred columns is several
	/// glyphs.
	code_advance: u32,
}

struct PreviewCanvas<'a> {
	image: &'a mut RgbaImage,
	scroll_px: PixelY,
	search: Option<PreviewSearch>,
	code_highlighting: Option<CodeHighlighting<'a>>,
}

impl<'a> PreviewCanvas<'a> {
	fn new(image: &'a mut RgbaImage, scroll_px: PixelY, search: Option<PreviewSearch>) -> Self {
		Self {
			image,
			scroll_px,
			search,
			code_highlighting: None,
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

/// The engine's highlight spans for the previewed document and the theme they
/// resolve through: the same spans and colors the source pane paints, so a
/// fenced Rune (or Rust, or SQL) body is colored in the preview exactly as it
/// is in the document. The engine owns language detection inside fences; the
/// preview only maps the spans' source bytes onto the lines it draws.
#[derive(Clone, Copy)]
struct CodeHighlighting<'a> {
	source: &'a str,
	spans: &'a [HighlightSpan],
	theme_colors: &'a [Option<Rgb>; CATEGORY_COUNT],
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
	wrap: bool,
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
			measure_code_block(width, x, y, info.as_deref(), text, metrics, wrap)
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
		MarkdownBlock::Table { rows, .. } => measure_table(width, x, y, rows, metrics, wrap),
		MarkdownBlock::Html { text, .. } => {
			measure_code_block(width, x, y, Some("html"), text, metrics, wrap)
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
	wrap: bool,
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
	let columns = code_row_columns(width, x, metrics);
	for line in text.lines() {
		for _ in code_line_rows(line, columns, wrap) {
			y = measure_code_line(y, metrics);
		}
	}
	if text.is_empty() {
		y = measure_code_line(y, metrics);
	}
	y.saturating_add(metrics.cell_h.get() / 3)
}

/// Columns of a tab stop when a wrapped code line is laid out.
const CODE_TAB_STOP: usize = 4;

fn code_block_pad_x(metrics: &FontMetrics) -> u32 {
	(metrics.cell_w.get() / 2).max(4)
}

/// The widest advance of a printable ASCII glyph (space included: spaces are
/// drawn as their own fragments) across the four monospace
/// faces (regular, bold, italic, bold italic: table cells carry emphasis),
/// rounded UP to whole pixels; `glyph_w` when no font is loaded (the bitmap
/// fallback advances exactly that).
///
/// Rounding once, up, is what makes the bound hold for drawn text, not just
/// for one continuous run: [`draw_inline_words`] draws every word and every
/// space with its own [`draw_text`], each returning its width rounded up, so
/// a fragment of `k` glyphs is at most `ceil(k * a) <= k * ceil(a)` wide and a
/// line of `n` glyphs in any number of fragments is at most `n * ceil(a)`.
fn measured_code_advance(text_renderer: &TextRenderer, metrics: &FontMetrics) -> u32 {
	let code = TextSpec::code(metrics, TEXT_RGBA);
	[(false, false), (true, false), (false, true), (true, true)]
		.into_iter()
		.map(|(bold, italic)| TextSpec {
			bold,
			italic,
			..code
		})
		.map(|spec| match text_renderer.font_for(spec) {
			Some((font, _)) => (' '..='~')
				.map(|ch| font.metrics(ch, spec.px_size).advance_width)
				.fold(1.0f32, f32::max)
				.ceil() as u32,
			None => spec.glyph_w(),
		})
		.max()
		.unwrap_or(1)
		.max(1)
}

/// Whole code glyphs that fit in `px` pixels; at least one.
fn code_columns_in(px: u32, metrics: &FontMetrics) -> usize {
	(px / metrics.code_advance.max(1)).max(1) as usize
}

/// The x offset of the code column `column` from a row's left edge. Text of
/// the columns before it, however it was split into fragments, ends at or
/// before this offset.
fn code_column_x(column: usize, metrics: &FontMetrics) -> u32 {
	(column as u32).saturating_mul(metrics.code_advance.max(1))
}

/// How many code glyphs fit on one code-block row whose block starts at `x`.
///
/// The row's text starts one pad inside the block, and the block's right edge
/// is `width - x / 2` (see [`draw_code_line`]); half a pad of breathing room is
/// kept before that edge so a full row never touches it.
fn code_row_columns(width: u32, x: u32, metrics: &FontMetrics) -> usize {
	let pad_x = code_block_pad_x(metrics);
	let text_left = x.saturating_add(pad_x);
	let text_right = width.saturating_sub(x / 2).saturating_sub(pad_x / 2);
	code_columns_in(text_right.saturating_sub(text_left), metrics)
}

/// The rows one source line of a code (or HTML) block occupies.
///
/// This is the single splitter both [`measure_code_block`] and
/// [`draw_code_block`] use, so a block's measured height always equals the
/// height it draws: the preview's scroll anchor depends on that.
///
/// Without wrap the line is one row, drawn verbatim and clipped at the pane
/// edge, exactly as before wrap existed. With wrap, tabs expand to stops of
/// [`CODE_TAB_STOP`] columns (a tab has no width of its own to count) and the
/// line breaks on character boundaries every `columns` characters; code is
/// whitespace-significant, so nothing is trimmed or re-flowed at word
/// boundaries. Continuation rows repeat the line's leading indentation, capped
/// at half a row, so a wrapped statement stays visibly inside its line. Every
/// row consumes at least one character, so the split always terminates.
fn code_line_rows(line: &str, columns: usize, wrap: bool) -> Vec<Cow<'_, str>> {
	if !wrap {
		return vec![Cow::Borrowed(line)];
	}
	code_line_cells(line, columns, wrap)
		.into_iter()
		.map(|row| Cow::Owned(row.iter().map(|(ch, _)| ch).collect::<String>()))
		.collect()
}

/// One drawn column of a code row: the glyph, and the index of the line's
/// character it shows. A tab's expansion shows the tab; a continuation row's
/// hanging indent shows no character of the line, so it carries `None`.
type CodeCell = (char, Option<usize>);

/// [`code_line_rows`], keeping each glyph's origin in the line so the
/// highlight color of the character it came from can follow it across tab
/// expansion and wrapping. The row strings are exactly those of
/// [`code_line_rows`].
fn code_line_cells(line: &str, columns: usize, wrap: bool) -> Vec<Vec<CodeCell>> {
	if !wrap {
		return vec![
			line.chars()
				.enumerate()
				.map(|(index, ch)| (ch, Some(index)))
				.collect(),
		];
	}
	let columns = columns.max(1);
	let cells = expand_code_tabs(line);
	if cells.len() <= columns {
		return vec![cells];
	}
	let indent = cells.iter().take_while(|(ch, _)| *ch == ' ').count();
	let hang = indent.min(columns / 2);
	let continuation = columns - hang;
	let mut rows = vec![cells[..columns].to_vec()];
	let mut start = columns;
	while start < cells.len() {
		let end = start.saturating_add(continuation).min(cells.len());
		let mut row = Vec::with_capacity(hang + (end - start));
		row.extend(std::iter::repeat_n((' ', None), hang));
		row.extend_from_slice(&cells[start..end]);
		rows.push(row);
		start = end;
	}
	rows
}

fn expand_code_tabs(line: &str) -> Vec<CodeCell> {
	let mut expanded = Vec::with_capacity(line.len());
	let mut column = 0usize;
	for (index, ch) in line.chars().enumerate() {
		if ch == '\t' {
			let spaces = CODE_TAB_STOP - column % CODE_TAB_STOP;
			expanded.extend(std::iter::repeat_n((' ', Some(index)), spaces));
			column += spaces;
		} else {
			expanded.push((ch, Some(index)));
			column += 1;
		}
	}
	expanded
}

/// Where each line of a code block's body begins in the source, so the
/// engine's highlight spans, which are in source coordinates, can color it.
///
/// The parser's body has any container indentation or quote marker stripped,
/// so a body line is located as the suffix of its source line rather than by
/// counting bytes. A line that is not such a suffix gets `None` and is drawn
/// in the plain code color: an uncolored line is better than a misaligned one.
fn code_body_line_starts(source: &str, range: &Range<usize>, body: &str) -> Vec<Option<usize>> {
	let Some(block) = source.get(range.clone()) else {
		return Vec::new();
	};
	// Same rule as Polariton's `fence_body_start`: a fence's body starts on
	// the line after its opening; an indented block's body is the block.
	let body_start = if block.starts_with("```") || block.starts_with("~~~") {
		match block.find('\n') {
			Some(newline) => range.start + newline + 1,
			None => return Vec::new(),
		}
	} else {
		range.start
	};
	let mut offset = body_start;
	let mut source_lines = source[body_start..].split_inclusive('\n');
	body.lines()
		.map(|line| {
			let source_line = source_lines.next()?;
			let line_start = offset;
			offset += source_line.len();
			let content = source_line
				.strip_suffix('\n')
				.map_or(source_line, |content| {
					content.strip_suffix('\r').unwrap_or(content)
				});
			content
				.ends_with(line)
				.then(|| line_start + content.len() - line.len())
		})
		.collect()
}

/// The color of each character of one body line: the theme color of the last
/// engine span covering it, as the source pane and Polariton resolve it, or
/// `default` where no span with a themed category does.
fn code_line_colors(
	line: &str,
	line_start: Option<usize>,
	highlighting: Option<CodeHighlighting<'_>>,
	default: Rgba<u8>,
) -> Vec<Rgba<u8>> {
	let (Some(line_start), Some(highlighting)) = (line_start, highlighting) else {
		return vec![default; line.chars().count()];
	};
	let start = line_start as u64;
	let end = start.saturating_add(line.len() as u64);
	let spans = highlighting
		.spans
		.iter()
		.filter(|span| span.start.get() < end && start < span.end.get())
		.collect::<Vec<_>>();
	let mut byte = start;
	line.chars()
		.map(|ch| {
			let color = spans
				.iter()
				.rev()
				.find(|span| span.start.get() <= byte && byte < span.end.get())
				.and_then(|span| highlighting.theme_colors[span.category as usize])
				.map_or(default, color_rgba);
			byte = byte.saturating_add(ch.len_utf8() as u64);
			color
		})
		.collect()
}

/// A code row's text and its color runs, `(byte in row, color)`, ascending
/// from 0, one run per change of color.
fn code_row_runs(
	row: &[CodeCell],
	colors: &[Rgba<u8>],
	default: Rgba<u8>,
) -> (String, Vec<(usize, Rgba<u8>)>) {
	let mut text = String::with_capacity(row.len());
	let mut runs: Vec<(usize, Rgba<u8>)> = Vec::new();
	for &(ch, origin) in row {
		let color = origin
			.and_then(|index| colors.get(index).copied())
			.unwrap_or(default);
		if runs.last().is_none_or(|&(_, last)| last != color) {
			runs.push((text.len(), color));
		}
		text.push(ch);
	}
	(text, runs)
}

fn measure_code_line(y: u32, metrics: &FontMetrics) -> u32 {
	let spec = TextSpec::code(metrics, Rgba([210, 225, 240, 255]));
	let block_pad_y = (metrics.cell_h.get() / 5).max(2);
	let block_bottom = y.saturating_add(spec.glyph_h()).saturating_add(block_pad_y);
	block_bottom.saturating_add(metrics.cell_h.get() / 5)
}

fn measure_table(
	width: u32,
	x: u32,
	mut y: u32,
	rows: &[Vec<MarkdownInlineText>],
	metrics: &FontMetrics,
	wrap: bool,
) -> u32 {
	if rows.is_empty() {
		return y;
	}
	let spec = TextSpec::code(metrics, TEXT_RGBA);
	let columns = rows.iter().map(|row| row.len()).max().unwrap_or(0);
	if columns == 0 {
		return y;
	}
	// Without wrap, cells clip, so table height is independent of column
	// widths. With wrap, a row is as tall as its tallest wrapped cell, counted
	// by `inline_line_count`, which breaks exactly as the `wrapped_table_row`
	// that draw_table draws.
	let column_widths = if wrap {
		table_column_widths(rows, table_available_chars(width, x, metrics)).0
	} else {
		Vec::new()
	};
	for (row_idx, row) in rows.iter().enumerate() {
		let lines = if wrap {
			column_widths
				.iter()
				.enumerate()
				.filter_map(|(column, &width)| {
					row.get(column).map(|cell| inline_line_count(cell, width))
				})
				.max()
				.unwrap_or(0)
				.max(1)
		} else {
			1
		};
		y = y.saturating_add(spec.line_h().saturating_mul(lines as u32));
		if row_idx == 0 {
			y = y.saturating_add(metrics.cell_h.get() / 4);
		}
	}
	y.saturating_add(metrics.cell_h.get() / 2)
}

fn table_available_chars(width: u32, x: u32, metrics: &FontMetrics) -> usize {
	code_columns_in(width.saturating_sub(x), metrics)
}

/// A wrapped table row: for each column, the lines its cell's text occupies
/// inside that column's width. An empty or missing cell has no lines.
fn wrapped_table_row(
	row: &[MarkdownInlineText],
	column_widths: &[usize],
) -> Vec<Vec<Vec<MarkdownInlineWord>>> {
	column_widths
		.iter()
		.enumerate()
		.map(|(col, &cell_width)| {
			row.get(col)
				.map(|cell| wrap_inline_words(cell, cell_width))
				.unwrap_or_default()
		})
		.collect()
}

/// A wrapped row is as tall as its tallest cell, and never less than one line.
fn table_row_line_count(cells: &[Vec<Vec<MarkdownInlineWord>>]) -> usize {
	cells.iter().map(Vec::len).max().unwrap_or(0).max(1)
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
	wrap: bool,
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
		MarkdownBlock::CodeBlock {
			info,
			text,
			source_range,
			..
		} => draw_code_block(
			canvas,
			x,
			y,
			info.as_deref(),
			text,
			Some(source_range),
			metrics,
			palette,
			text_renderer,
			wrap,
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
			draw_table(canvas, x, y, rows, metrics, palette, text_renderer, wrap)
		}
		MarkdownBlock::Html { text, .. } => draw_code_block(
			canvas,
			x,
			y,
			Some("html"),
			text,
			None,
			metrics,
			palette,
			text_renderer,
			wrap,
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

/// Draw a code block. With a `source_range`, its body is colored by the
/// engine's highlight spans; without one (raw HTML) it is plain code.
fn draw_code_block(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	mut y: u32,
	info: Option<&str>,
	text: &str,
	source_range: Option<&Range<usize>>,
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
	wrap: bool,
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
	let columns = code_row_columns(canvas.width(), x, metrics);
	let highlighting = canvas.code_highlighting;
	let line_starts = source_range
		.zip(highlighting)
		.map(|(range, highlighting)| code_body_line_starts(highlighting.source, range, text))
		.unwrap_or_default();
	for (index, line) in text.lines().enumerate() {
		let line_start = line_starts.get(index).copied().flatten();
		let colors = code_line_colors(line, line_start, highlighting, palette.code_block);
		for row in code_line_cells(line, columns, wrap) {
			let (row, runs) = code_row_runs(&row, &colors, palette.code_block);
			y = draw_code_line(canvas, x, y, &row, &runs, metrics, palette, text_renderer);
		}
	}
	if text.is_empty() {
		y = draw_code_line(canvas, x, y, "", &[], metrics, palette, text_renderer);
	}
	y.saturating_add(metrics.cell_h.get() / 3)
}

fn draw_code_line(
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	line: &str,
	runs: &[(usize, Rgba<u8>)],
	metrics: &FontMetrics,
	palette: &MarkdownPreviewPalette,
	text_renderer: &TextRenderer,
) -> u32 {
	let spec = TextSpec::code(metrics, palette.code_block);
	let block_pad_x = code_block_pad_x(metrics);
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
	draw_text_runs(
		text_renderer,
		canvas,
		x.saturating_add(block_pad_x),
		y.saturating_add(block_pad_y / 2),
		line,
		spec,
		runs,
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
	wrap: bool,
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
	let available_chars = table_available_chars(canvas.width(), x, metrics);
	let (column_widths, gutter_chars) = table_column_widths(rows, available_chars);
	for (row_idx, row) in rows.iter().enumerate() {
		let wrapped = wrap.then(|| wrapped_table_row(row, &column_widths));
		let lines = wrapped.as_deref().map_or(1, table_row_line_count);
		let row_top = y;
		let row_bottom = y.saturating_add(spec.line_h().saturating_mul(lines as u32));
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
		let cell_spec = if row_idx == 0 { header_spec } else { spec };
		let mut cell_offset = 0usize;
		for (col, &cell_width) in column_widths.iter().enumerate() {
			let cell_x = x.saturating_add(code_column_x(cell_offset, metrics));
			if let Some(wrapped) = wrapped.as_ref() {
				let mut line_y = y;
				for line in &wrapped[col] {
					draw_inline_words(
						text_renderer,
						canvas,
						cell_x,
						line_y,
						line,
						cell_spec,
						inline_code_spec,
					);
					line_y = line_y.saturating_add(spec.line_h());
				}
			} else {
				let empty_cell = MarkdownInlineText::new();
				let cell_text = row.get(col).unwrap_or(&empty_cell);
				let clipped = clipped_inline_chars(cell_text, cell_width);
				draw_inline_text(
					text_renderer,
					canvas,
					cell_x,
					y,
					&clipped,
					cell_spec,
					inline_code_spec,
				);
			}
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
	draw_text_runs(text_renderer, canvas, x, y, text, spec, &[])
}

/// Draw `text` with its color changing at each `(byte, color)` run start
/// (ascending, on character boundaries); bytes before the first run take
/// `spec.color`. Search matches are found over the whole text, not per run,
/// so a match spanning two highlight tokens is still one match, drawn in the
/// search colors over whatever the runs said.
fn draw_text_runs(
	text_renderer: &TextRenderer,
	canvas: &mut PreviewCanvas<'_>,
	x: u32,
	y: u32,
	text: &str,
	spec: TextSpec,
	runs: &[(usize, Rgba<u8>)],
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
	if matches.is_empty() && runs.len() <= 1 && runs.first().is_none_or(|&(byte, _)| byte == 0) {
		let color = runs.first().map_or(spec.color, |&(_, color)| color);
		return draw_plain_text(
			text_renderer,
			canvas,
			x,
			y,
			text,
			TextSpec { color, ..spec },
		);
	}

	let search_colors = canvas
		.search
		.as_ref()
		.map(|search| (search.background, search.foreground));
	let mut cuts = runs
		.iter()
		.map(|&(byte, _)| byte)
		.chain(matches.iter().flat_map(|&(start, end)| [start, end]))
		.chain([0, text.len()])
		.filter(|&byte| byte <= text.len())
		.collect::<Vec<_>>();
	cuts.sort_unstable();
	cuts.dedup();
	let mut cursor = x;
	for cut in cuts.windows(2) {
		let (start, end) = (cut[0], cut[1]);
		let segment = &text[start..end];
		let matched = matches
			.iter()
			.any(|&(match_start, match_end)| match_start <= start && end <= match_end);
		let color = match search_colors.filter(|_| matched) {
			Some((background, foreground)) => {
				let width = measure_text_width(text_renderer, segment, spec);
				fill_rect(
					canvas,
					cursor,
					y,
					cursor.saturating_add(width),
					y.saturating_add(spec.glyph_h()),
					background,
				);
				foreground
			}
			None => runs
				.iter()
				.rev()
				.find(|&&(byte, _)| byte <= start)
				.map_or(spec.color, |&(_, color)| color),
		};
		cursor = cursor.saturating_add(draw_plain_text(
			text_renderer,
			canvas,
			cursor,
			y,
			segment,
			TextSpec { color, ..spec },
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
	use libaryon::core::{DocByte, DocLine};
	use libaryon::svp::rust_highlight::TokenCategory;

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
			code_advance: 8,
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
			code_advance: 8,
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
			code_advance: 8,
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
			code_advance: 8,
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
			true,
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
			code_advance: 8,
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
			true,
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
			wrap_enabled: true,
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
			wrap_enabled: true,
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
			wrap_enabled: true,
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
			wrap_enabled: true,
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
			code_advance: 8,
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
			wrap_enabled: true,
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
			wrap_enabled: true,
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
			wrap_enabled: true,
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

	fn wrap_test_metrics() -> FontMetrics {
		FontMetrics {
			cell_w: PixelWidth::new(8),
			cell_h: PixelHeight::new(16),
			body_scale_x: 1,
			body_scale_y: 2,
			body_role: FontRole::Sans,
			code_advance: 8,
		}
	}

	/// Draws one block at (8, 8) on a canvas tall enough that nothing is cut
	/// off at the visible bottom, and returns the y the block ends at.
	fn drawn_block_end(block: &MarkdownBlock, width: u32, wrap: bool) -> u32 {
		let metrics = wrap_test_metrics();
		let palette = MarkdownPreviewPalette::from_theme(
			&[None; CATEGORY_COUNT],
			UiPalette::default().preview_text,
			UiPalette::default().accent,
		);
		let mut image = RgbaImage::from_pixel(width, 8192, BG_RGBA);
		let mut canvas = PreviewCanvas::new(&mut image, PixelY::new(0), None);
		draw_markdown_block(
			&mut canvas,
			8,
			8,
			block,
			&metrics,
			&palette,
			&TextRenderer::load(),
			wrap,
		)
	}

	fn wrap_test_snapshot(source_text: &str, wrap_enabled: bool) -> MarkdownPreviewSnapshot {
		MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			wrap_enabled,
			source_path: Some("notes.md".to_string()),
			source_text: source_text.to_string(),
			code_highlights: Vec::new(),
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		}
	}

	fn row_texts(rows: Vec<Cow<'_, str>>) -> Vec<String> {
		rows.into_iter().map(Cow::into_owned).collect()
	}

	#[test]
	fn code_line_without_wrap_is_one_verbatim_row() {
		let line = "\tlet total = alpha + beta + gamma + delta; // long";
		assert_eq!(row_texts(code_line_rows(line, 8, false)), vec![line]);
	}

	#[test]
	fn wrapped_code_line_breaks_on_char_boundaries_and_keeps_its_indent() {
		assert_eq!(
			row_texts(code_line_rows("    abcdefghij", 8, true)),
			vec!["    abcd", "    efgh", "    ij"]
		);
		// The indentation a continuation repeats is capped at half a row, so
		// a deeply indented line still advances.
		assert_eq!(
			row_texts(code_line_rows("          abcdef", 8, true)),
			vec!["        ", "      ab", "    cdef"]
		);
		// A line that fits is one row, unchanged.
		assert_eq!(row_texts(code_line_rows("fits", 8, true)), vec!["fits"]);
	}

	#[test]
	fn wrapped_code_line_expands_tabs_to_four_column_stops() {
		assert_eq!(row_texts(code_line_rows("\tx", 80, true)), vec!["    x"]);
		assert_eq!(row_texts(code_line_rows("ab\tc", 80, true)), vec!["ab  c"]);
		assert_eq!(
			row_texts(code_line_rows("\tabcdef", 6, true)),
			vec!["    ab", "   cde", "   f"]
		);
	}

	#[test]
	fn wrapped_code_line_splits_multibyte_text_by_character() {
		assert_eq!(
			row_texts(code_line_rows("ééééé", 2, true)),
			vec!["éé", "éé", "é"]
		);
	}

	#[test]
	fn wrapped_code_line_terminates_at_degenerate_widths() {
		assert_eq!(
			row_texts(code_line_rows("abc", 0, true)),
			vec!["a", "b", "c"]
		);
		assert_eq!(
			row_texts(code_line_rows("  ab", 1, true)),
			vec![" ", " ", "a", "b"]
		);
	}

	#[test]
	fn measured_height_equals_drawn_height_for_wrapped_and_clipped_blocks() {
		let long = "x".repeat(300);
		let source = format!(
			"```sql\nselect {long}\n\tfrom t\n```\n\n<div>{long}</div>\n\n| Key | Value |\n| --- | --- |\n| first | {long} |\n| second | short |\n"
		);
		let blocks = parse_markdown_blocks(&source);
		assert!(
			blocks
				.iter()
				.any(|block| matches!(block, MarkdownBlock::CodeBlock { .. }))
		);
		assert!(
			blocks
				.iter()
				.any(|block| matches!(block, MarkdownBlock::Table { .. }))
		);
		let metrics = wrap_test_metrics();
		let width = 320;
		for block in &blocks {
			let mut heights = Vec::new();
			for wrap in [false, true] {
				let measured = measure_markdown_block(width, 8, 8, block, &metrics, wrap);
				let drawn = drawn_block_end(block, width, wrap);
				assert_eq!(measured, drawn, "wrap={wrap} block={block:?}");
				heights.push(measured);
			}
			assert!(
				heights[1] > heights[0],
				"a block with a 300-character line grows when wrapped: {block:?}"
			);
		}
	}

	#[test]
	fn wrap_setting_changes_the_rendered_preview_and_its_fingerprint() {
		let source = format!("```text\n{}\n```\n", "wrapped ".repeat(40));
		let area = Rect {
			x: 0,
			y: 0,
			width: 40,
			height: 24,
		};
		let clipped =
			render_markdown_preview_image(&wrap_test_snapshot(&source, false), area, (8, 16))
				.to_rgba8();
		let wrapped =
			render_markdown_preview_image(&wrap_test_snapshot(&source, true), area, (8, 16))
				.to_rgba8();
		assert_ne!(clipped, wrapped);

		// Code-block background rows: the wrapped line occupies more of them.
		let code_rows = |image: &RgbaImage| {
			(0..image.height())
				.filter(|&y| image.get_pixel(image.width() / 2, y) == &CODE_BG_RGBA)
				.count()
		};
		assert!(code_rows(&wrapped) > code_rows(&clipped));

		let search = PreviewSearchConfig {
			pattern: None,
			case_insensitive: false,
			highlight: libaryon::core::SEARCH_MATCH_BG,
		};
		let fingerprint = |wrap| {
			preview_request_fingerprint(
				&wrap_test_snapshot(&source, wrap),
				area,
				(8, 16),
				&[None; CATEGORY_COUNT],
				&search,
				UiPalette::default(),
			)
		};
		assert_ne!(fingerprint(false), fingerprint(true));
	}

	#[test]
	fn code_columns_fit_the_loaded_mono_font_advance() {
		let text_renderer = TextRenderer::load();
		let mut metrics = wrap_test_metrics();
		metrics.code_advance = measured_code_advance(&text_renderer, &metrics);
		assert!(metrics.code_advance >= 1);
		let width = 640;
		let x = 8;
		let pad_x = code_block_pad_x(&metrics);
		let text_px = width - x / 2 - pad_x / 2 - (x + pad_x);
		let columns = code_row_columns(width, x, &metrics);
		let code = TextSpec::code(&metrics, TEXT_RGBA);
		let faces =
			[(false, false), (true, false), (false, true), (true, true)].map(|(bold, italic)| {
				TextSpec {
					bold,
					italic,
					..code
				}
			});
		for spec in faces {
			// The worst case for per-fragment rounding: one-glyph words with a
			// space between each, every fragment measured (and drawn) alone,
			// as draw_inline_words draws them.
			for n in 1..=columns {
				let fragments = (0..n)
					.map(|idx| if idx % 2 == 0 { "W" } else { " " })
					.map(|fragment| measure_text_width(&text_renderer, fragment, spec))
					.sum::<u32>();
				assert!(
					fragments <= code_column_x(n, &metrics),
					"{n} one-glyph fragments fit their {n} columns"
				);
			}
			for sample in ['M', 'W', '0', '_', 'i'] {
				let row = sample.to_string().repeat(columns);
				assert!(
					measure_text_width(&text_renderer, &row, spec) <= text_px,
					"a full row of {columns} {sample:?} fits {text_px}px (advance {})",
					metrics.code_advance
				);
				// Table cells start at code_column_x, which never lies inside
				// the text of the cells before it.
				for n in 1..=columns {
					let prefix = sample.to_string().repeat(n);
					assert!(
						measure_text_width(&text_renderer, &prefix, spec)
							<= code_column_x(n, &metrics)
					);
				}
			}
		}
	}

	/// Every body line of every code block the parser finds in `source`
	/// maps to a source offset where exactly that line's text sits.
	fn assert_code_lines_located(source: &str) -> Vec<Vec<usize>> {
		let mut located = Vec::new();
		for block in parse_markdown_blocks(source) {
			let MarkdownBlock::CodeBlock {
				text, source_range, ..
			} = block
			else {
				continue;
			};
			let starts = code_body_line_starts(source, &source_range, &text);
			assert_eq!(starts.len(), text.lines().count(), "{source:?}");
			let mut block_starts = Vec::new();
			for (line, start) in text.lines().zip(starts) {
				let start = start.unwrap_or_else(|| panic!("{line:?} unlocated in {source:?}"));
				assert_eq!(&source[start..start + line.len()], line, "{source:?}");
				block_starts.push(start);
			}
			located.push(block_starts);
		}
		assert!(!located.is_empty(), "no code block in {source:?}");
		located
	}

	#[test]
	fn code_body_lines_are_located_in_source_through_parser_ranges() {
		let top = "# Title\n\n```rune\nfn a() {}\nlet b = 1;\n```\n";
		assert_eq!(
			assert_code_lines_located(top),
			vec![vec![top.find("fn a").unwrap(), top.find("let b").unwrap()]]
		);
		// A fence indented two spaces inside a list item: the parser strips
		// the container indent from the body, the source keeps it.
		let listed = "- item\n\n  ```rune\n  let x = 1;\n    nested();\n  ```\n";
		assert_eq!(
			assert_code_lines_located(listed),
			vec![vec![
				listed.find("let x").unwrap(),
				listed.find("  nested").unwrap()
			]]
		);
		let quoted = "> ```rune\n> let q = 2;\n> ```\n";
		assert_eq!(
			assert_code_lines_located(quoted),
			vec![vec![quoted.find("let q").unwrap()]]
		);
		let crlf = "text\r\n\r\n```rune\r\nlet a = 1;\r\nlet b = 2;\r\n```\r\n";
		assert_eq!(
			assert_code_lines_located(crlf),
			vec![vec![
				crlf.find("let a").unwrap(),
				crlf.find("let b").unwrap()
			]]
		);
		let utf8 = "# Über é\n\n```rune\nlet é = \"ü→∘\";\nfn ƒ() {}\n```\n";
		assert_eq!(
			assert_code_lines_located(utf8),
			vec![vec![
				utf8.find("let é").unwrap(),
				utf8.find("fn ƒ").unwrap()
			]]
		);
		let indented = "para\n\n    let x;\n    let y;\n";
		assert_eq!(
			assert_code_lines_located(indented),
			vec![vec![
				indented.find("let x").unwrap(),
				indented.find("let y").unwrap()
			]]
		);
	}

	#[test]
	fn code_line_colors_take_the_last_themed_span_at_each_byte() {
		let source = "```rune\nlet é = 1;\n```\n";
		let start = source.find("let").unwrap();
		let at = |text: &str| DocByte::new(source.find(text).unwrap() as u64);
		let spans = vec![
			HighlightSpan::new(at("let"), at(" é"), TokenCategory::Keyword),
			HighlightSpan::new(at("é"), at(" ="), TokenCategory::Comment),
			// Later spans win, as on the source pane and in Polariton.
			HighlightSpan::new(at("é"), at(" ="), TokenCategory::Variable),
			HighlightSpan::new(at("1"), at(";"), TokenCategory::Number),
		];
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(1, 0, 0));
		theme_colors[TokenCategory::Comment as usize] = Some(Rgb::new(2, 0, 0));
		theme_colors[TokenCategory::Variable as usize] = Some(Rgb::new(3, 0, 0));
		// Number has no theme color: the default shows through.
		let default = Rgba([9, 9, 9, 255]);
		let highlighting = CodeHighlighting {
			source,
			spans: &spans,
			theme_colors: &theme_colors,
		};
		let colors = code_line_colors("let é = 1;", Some(start), Some(highlighting), default);
		let reds = colors.iter().map(|color| color.0[0]).collect::<Vec<_>>();
		assert_eq!(reds, vec![1, 1, 1, 9, 3, 9, 9, 9, 9, 9]);
		assert_eq!(
			code_line_colors("let é = 1;", None, Some(highlighting), default),
			vec![default; 10],
			"an unlocated line is drawn plain"
		);
	}

	#[test]
	fn code_row_colors_follow_characters_across_tab_expansion_and_wrap() {
		let line = "\tabcdef";
		let rows = code_line_cells(line, 6, true);
		assert_eq!(
			rows.iter()
				.map(|row| row.iter().map(|(ch, _)| ch).collect::<String>())
				.collect::<Vec<_>>(),
			row_texts(code_line_rows(line, 6, true)),
			"cells draw exactly the rows layout measured"
		);
		let red = |r| Rgba([r, 0, 0, 255]);
		let colors = (1..=7).map(red).collect::<Vec<_>>();
		let default = red(0);
		let (first, first_runs) = code_row_runs(&rows[0], &colors, default);
		assert_eq!(first, "    ab");
		assert_eq!(first_runs, vec![(0, red(1)), (4, red(2)), (5, red(3))]);
		let (second, second_runs) = code_row_runs(&rows[1], &colors, default);
		assert_eq!(second, "   cde");
		assert_eq!(
			second_runs,
			vec![(0, default), (3, red(4)), (4, red(5)), (5, red(6))],
			"the hanging indent shows no character, so it is plain"
		);
	}

	fn rune_fence_snapshot(code_highlights: Vec<HighlightSpan>) -> MarkdownPreviewSnapshot {
		MarkdownPreviewSnapshot {
			source_buffer_id: BufferId::new(1),
			source_view_id: ViewId::new(1),
			serif: false,
			wrap_enabled: true,
			source_path: Some("notes.md".to_string()),
			source_text: RUNE_FENCE.to_string(),
			code_highlights,
			theme_colors: [None; CATEGORY_COUNT],
			cells: Vec::new(),
			source_scroll_line: DocLine::ZERO,
			source_viewport_line_count: 24,
		}
	}

	const RUNE_FENCE: &str = "```rune\nfn fetch_add(counter) {\n    let old = 1;\n}\n```\n";

	fn rune_fence_spans() -> Vec<HighlightSpan> {
		let at = |text: &str| DocByte::new(RUNE_FENCE.find(text).unwrap() as u64);
		vec![
			HighlightSpan::new(at("fn "), at(" fetch"), TokenCategory::Keyword),
			HighlightSpan::new(at("fetch"), at("(counter"), TokenCategory::Function),
			HighlightSpan::new(at("let"), at(" old"), TokenCategory::Keyword),
		]
	}

	fn rune_theme() -> [Option<Rgb>; CATEGORY_COUNT] {
		let mut theme_colors = [None; CATEGORY_COUNT];
		theme_colors[TokenCategory::Keyword as usize] = Some(Rgb::new(0, 255, 0));
		theme_colors[TokenCategory::Function as usize] = Some(Rgb::new(0, 0, 255));
		theme_colors
	}

	fn rune_area() -> Rect {
		Rect {
			x: 0,
			y: 0,
			width: 80,
			height: 12,
		}
	}

	#[test]
	fn render_image_colors_fenced_rune_with_engine_highlights() {
		let greenish = |pixel: &Rgba<u8>| {
			pixel[1] > pixel[0].saturating_add(64) && pixel[1] > pixel[2].saturating_add(64)
		};
		let bluish = |pixel: &Rgba<u8>| {
			pixel[2] > pixel[0].saturating_add(64) && pixel[2] > pixel[1].saturating_add(64)
		};
		let plain = render_markdown_preview_image_with_theme(
			&rune_fence_snapshot(Vec::new()),
			rune_area(),
			(8, 16),
			&rune_theme(),
		)
		.to_rgba8();
		assert!(
			!plain.pixels().any(greenish) && !plain.pixels().any(bluish),
			"without spans the fence is one code color"
		);

		let highlighted = render_markdown_preview_image_with_theme(
			&rune_fence_snapshot(rune_fence_spans()),
			rune_area(),
			(8, 16),
			&rune_theme(),
		)
		.to_rgba8();
		assert!(highlighted.pixels().any(greenish), "keywords take Keyword");
		assert!(
			highlighted.pixels().any(bluish),
			"the function takes Function"
		);
	}

	#[test]
	fn render_image_search_match_spans_highlight_tokens() {
		let search_rgba = Rgba([12, 200, 240, 255]);
		let search = PreviewSearchConfig {
			// Spans the Keyword, the space, and the Function tokens.
			pattern: Some("fn fetch".to_string()),
			case_insensitive: false,
			highlight: Rgb::new(12, 200, 240),
		};
		let render = |spans| {
			render_markdown_preview_image_with_search(
				&rune_fence_snapshot(spans),
				rune_area(),
				(8, 16),
				&rune_theme(),
				&search,
			)
			.to_rgba8()
		};
		let match_pixels = |image: &RgbaImage| {
			image
				.pixels()
				.filter(|pixel| **pixel == search_rgba)
				.count()
		};
		let plain = match_pixels(&render(Vec::new()));
		let highlighted = match_pixels(&render(rune_fence_spans()));
		assert!(plain > 0);
		assert!(
			highlighted * 10 >= plain * 9,
			"a match across tokens is still one highlighted match: {highlighted} vs {plain}"
		);
	}
}
