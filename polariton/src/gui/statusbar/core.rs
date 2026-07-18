use std::time::Duration;

use iced::widget::{Space, column, container, progress_bar, row, text};
use iced::{Alignment, Background, Border, Color, Element, Length, Shadow};

use crate::gui::colors;
use crate::gui::statusbar::spinner;

const STATUS_BAR_HEIGHT: f32 = 26.0;
const SEGMENT_HEIGHT: f32 = 18.0;
const STATUS_BAR_INSET_X: f32 = 4.0;
const STATUS_BAR_INSET_BOTTOM: f32 = 4.0;
const LEFT_LANE_PORTION: u16 = 3;
const RIGHT_LANE_PORTION: u16 = 2;
const PROGRESS_WIDTH: f32 = 74.0;
const PROGRESS_GIRTH: f32 = 10.0;
const SEGMENT_FONT_SIZE: f32 = 12.0;
const SEGMENT_CHAR_WIDTH: f32 = 7.0;
const SEGMENT_HORIZONTAL_PADDING: f32 = 12.0;
const SPINNER_LABEL_GAP: f32 = 5.0;
const LABEL_VALUE_GAP: f32 = 4.0;
const PROGRESS_GAP: f32 = 6.0;
const SPINNER_SIZE_HINT: f32 = 16.0;

#[derive(Debug, Clone, Copy)]
struct LayoutConfig {
	inset_x: f32,
	bottom_gap: f32,
	left_portion: u16,
	right_portion: u16,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub enum Tone {
	#[default]
	Normal,
	Accent,
	Success,
	Warning,
	Danger,
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SegmentWidth {
	#[default]
	Compact,
	Fixed(u16),
	Chars { chars: u16, slot: SlotKind },
	FillPortion(u16),
}

#[allow(dead_code)]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub(crate) enum SlotKind {
	#[default]
	Auto,
	Text,
	LabelValue,
	Spinner,
	Progress,
}

#[derive(Debug, Clone, Copy)]
pub struct StatusBarStyle {
	pub rail_background: Color,
	pub rail_separator: Color,
	pub segment_background: Color,
	pub segment_border: Color,
	pub progress_background: Color,
	pub progress_bar: Color,
	pub text_normal: Color,
	pub text_accent: Color,
	pub text_success: Color,
	pub text_warning: Color,
	pub text_danger: Color,
}

#[allow(dead_code)]
#[derive(Debug, Clone)]
pub enum Segment {
	Text {
		value: String,
		tone: Tone,
		width: SegmentWidth,
		max_chars: Option<usize>,
	},
	LabelValue {
		label: String,
		value: String,
		tone: Tone,
		width: SegmentWidth,
		max_chars: Option<usize>,
	},
	Spinner {
		label: String,
		phase: usize,
		tone: Tone,
		width: SegmentWidth,
		max_chars: Option<usize>,
	},
	Progress {
		label: String,
		value: f32,
		value_text: String,
		tone: Tone,
		width: SegmentWidth,
	},
}

#[derive(Debug, Clone)]
pub struct StatusBar {
	left: Vec<Segment>,
	right: Vec<Segment>,
	layout: LayoutConfig,
	style: StatusBarStyle,
}

#[allow(dead_code)]
impl Segment {
	pub fn text(value: impl Into<String>) -> Self {
		Self::Text {
			value: value.into(),
			tone: Tone::Normal,
			width: SegmentWidth::Compact,
			max_chars: None,
		}
	}

	pub fn toned_text(value: impl Into<String>, tone: Tone) -> Self {
		Self::Text {
			value: value.into(),
			tone,
			width: SegmentWidth::Compact,
			max_chars: None,
		}
	}

	pub fn label_value(
		label: impl Into<String>,
		value: impl Into<String>,
		tone: Tone,
	) -> Self {
		Self::LabelValue {
			label: label.into(),
			value: value.into(),
			tone,
			width: SegmentWidth::Compact,
			max_chars: None,
		}
	}

	pub fn spinner(label: impl Into<String>, phase: usize, tone: Tone) -> Self {
		Self::Spinner {
			label: label.into(),
			phase,
			tone,
			width: SegmentWidth::Compact,
			max_chars: None,
		}
	}

	pub fn progress_percent(label: impl Into<String>, value: f32, tone: Tone) -> Self {
		let value = value.clamp(0.0, 1.0);
		Self::Progress {
			label: label.into(),
			value,
			value_text: format!("{:.0}%", value * 100.0),
			tone,
			width: SegmentWidth::Compact,
		}
	}

	pub fn timer(label: impl Into<String>, duration: Duration, tone: Tone) -> Self {
		Self::label_value(label, format_elapsed(duration), tone)
	}

	pub fn max_chars(mut self, max_chars: usize) -> Self {
		match &mut self {
			Self::Text { max_chars: slot, .. }
			| Self::LabelValue { max_chars: slot, .. }
			| Self::Spinner { max_chars: slot, .. } => *slot = Some(max_chars.max(1)),
			Self::Progress { .. } => {}
		}
		self
	}

	pub fn fixed_width(mut self, width: u16) -> Self {
		self.set_width(SegmentWidth::Fixed(width.max(1)));
		self
	}

	pub fn reserve_chars(mut self, chars: u16) -> Self {
		self.set_width(SegmentWidth::Chars {
			chars: chars.max(1),
			slot: SlotKind::Auto,
		});
		self
	}

	pub fn reserve_chars_as(mut self, chars: u16, slot: SlotKind) -> Self {
		self.set_width(SegmentWidth::Chars {
			chars: chars.max(1),
			slot,
		});
		self
	}

	pub fn fill_portion(mut self, portion: u16) -> Self {
		self.set_width(SegmentWidth::FillPortion(portion.max(1)));
		self
	}

	fn set_width(&mut self, width: SegmentWidth) {
		match self {
			Self::Text { width: slot, .. }
			| Self::LabelValue { width: slot, .. }
			| Self::Spinner { width: slot, .. }
			| Self::Progress { width: slot, .. } => *slot = width,
		}
	}
}

impl StatusBar {
	pub fn new() -> Self {
		Self {
			left: Vec::new(),
			right: Vec::new(),
			layout: LayoutConfig::default(),
			style: StatusBarStyle::default(),
		}
	}

	pub fn left(mut self, segment: Segment) -> Self {
		self.left.push(segment);
		self
	}

	pub fn right(mut self, segment: Segment) -> Self {
		self.right.push(segment);
		self
	}

	pub fn inset(mut self, inset_x: f32, bottom_gap: f32) -> Self {
		self.layout.inset_x = inset_x.max(0.0);
		self.layout.bottom_gap = bottom_gap.max(0.0);
		self
	}

	pub fn lane_split(mut self, left_portion: u16, right_portion: u16) -> Self {
		self.layout.left_portion = left_portion.max(1);
		self.layout.right_portion = right_portion.max(1);
		self
	}

	pub fn style(mut self, style: StatusBarStyle) -> Self {
		self.style = style;
		self
	}

	pub fn view<Message: 'static>(&self) -> Element<'static, Message> {
		let style = self.style;
		let layout = self.layout;
		let left = container(lane::<Message>(&self.left, &style))
			.width(Length::FillPortion(layout.left_portion))
			.height(Length::Fixed(STATUS_BAR_HEIGHT))
			.align_y(Alignment::Center)
			.clip(true);
		let right = container(lane::<Message>(&self.right, &style))
			.width(Length::FillPortion(layout.right_portion))
			.height(Length::Fixed(STATUS_BAR_HEIGHT))
			.align_right(Length::Fill)
			.align_y(Alignment::Center)
			.clip(true);
		let rail = column![
			container(Space::new())
				.height(Length::Fixed(1.0))
				.style(move |_theme| container::Style {
					background: Some(Background::Color(style.rail_separator)),
					..Default::default()
				}),
			container(
				row![left, Space::new().width(Length::Fixed(4.0)), right]
					.align_y(Alignment::Center)
					.spacing(0)
					.clip(true),
			)
			.width(Length::Fill)
			.height(Length::Fixed(STATUS_BAR_HEIGHT))
			.padding([3, 5])
			.clip(true)
			.style(move |_theme| container::Style {
				background: Some(Background::Color(style.rail_background)),
				..Default::default()
			})
		]
		.width(Length::Fill);
		column![
			container(rail)
				.padding([0, layout.inset_x as u16])
				.width(Length::Fill),
			Space::new().height(Length::Fixed(layout.bottom_gap))
		]
		.width(Length::Fill)
		.into()
	}
}

fn lane<Message: 'static>(
	segments: &[Segment],
	style: &StatusBarStyle,
) -> Element<'static, Message> {
	let mut content = row![].align_y(Alignment::Center).spacing(4);
	for segment in segments.iter().cloned() {
		content = content.push(segment_view(segment, *style));
	}
	content.height(Length::Fixed(SEGMENT_HEIGHT)).clip(true).into()
}

fn segment_view<Message: 'static>(
	segment: Segment,
	style: StatusBarStyle,
) -> Element<'static, Message> {
	let width = segment.width();
	let reserved_width = width.reserved_pixels(&segment);
	match segment {
		Segment::Text {
			value,
			tone,
			max_chars,
			..
		} => apply_width(
			segment_shell(
				text(ellipsize(&value, max_chars))
					.size(SEGMENT_FONT_SIZE)
					.color(tone.text(&style)),
				style,
			),
			width,
			reserved_width,
		),
		Segment::LabelValue {
			label,
			value,
			tone,
			max_chars,
			..
		} => {
			let (label, value) = ellipsize_pair(&label, &value, max_chars);
			apply_width(
				segment_shell(
					row![
						text(label).size(SEGMENT_FONT_SIZE).color(style.text_normal),
						text(value).size(SEGMENT_FONT_SIZE).color(tone.text(&style))
					]
					.spacing(4)
					.align_y(Alignment::Center),
					style,
				),
				width,
				reserved_width,
			)
		}
		Segment::Spinner {
			label,
			phase,
			tone,
			max_chars,
			..
		} => apply_width(
			segment_shell(
				row![
					spinner::view(phase, tone, style),
					text(ellipsize(&label, max_chars))
						.size(SEGMENT_FONT_SIZE)
						.color(tone.text(&style))
				]
				.spacing(5)
				.align_y(Alignment::Center),
				style,
			),
			width,
			reserved_width,
		),
		Segment::Progress {
			label,
			value,
			value_text,
			tone,
			..
		} => apply_width(
			segment_shell(
				row![
					text(label).size(SEGMENT_FONT_SIZE).color(style.text_normal),
					container(
						progress_bar(0.0..=1.0, value)
							.length(Length::Fixed(PROGRESS_WIDTH))
							.girth(Length::Fixed(PROGRESS_GIRTH))
							.style(move |_theme| iced::widget::progress_bar::Style {
								background: Background::Color(style.progress_background),
								bar: Background::Color(style.progress_bar),
								border: Border {
									color: style.segment_border,
									width: 1.0,
									radius: 2.0.into(),
								},
							}),
					)
					.width(PROGRESS_WIDTH)
					.align_y(Alignment::Center),
					text(value_text)
						.size(SEGMENT_FONT_SIZE)
						.color(tone.text(&style))
				]
				.spacing(6)
				.align_y(Alignment::Center),
				style,
			),
			width,
			reserved_width,
		),
	}
}

fn segment_shell<Message: 'static>(
	content: impl Into<Element<'static, Message>>,
	style: StatusBarStyle,
) -> Element<'static, Message> {
	container(content)
		.height(Length::Fixed(SEGMENT_HEIGHT))
		.padding([1, 6])
		.align_y(Alignment::Center)
		.clip(true)
		.style(move |_theme| container::Style {
			background: Some(Background::Color(style.segment_background)),
			border: Border {
				color: style.segment_border,
				width: 1.0,
				radius: 3.0.into(),
			},
			shadow: Shadow::default(),
			..Default::default()
		})
		.into()
}

fn apply_width<'a, Message: 'static>(
	content: impl Into<Element<'a, Message>>,
	width: SegmentWidth,
	reserved_width: f32,
) -> Element<'a, Message> {
	match width {
		SegmentWidth::Compact => container(content).clip(true).into(),
		SegmentWidth::Fixed(width) => container(content)
			.width(Length::Fixed(f32::from(width)))
			.clip(true)
			.into(),
		SegmentWidth::Chars { .. } => container(content)
			.width(Length::Fixed(reserved_width))
			.clip(true)
			.into(),
		SegmentWidth::FillPortion(portion) => container(content)
			.width(Length::FillPortion(portion))
			.clip(true)
			.into(),
	}
}

fn ellipsize(value: &str, max_chars: Option<usize>) -> String {
	match max_chars {
		Some(limit) => ellipsize_to_limit(value, limit),
		None => value.to_owned(),
	}
}

fn ellipsize_pair(label: &str, value: &str, max_chars: Option<usize>) -> (String, String) {
	let Some(limit) = max_chars else {
		return (label.to_owned(), value.to_owned());
	};
	let label_chars = label.chars().count();
	if label_chars + 1 >= limit {
		return (ellipsize_to_limit(label, limit.saturating_sub(1)), String::new());
	}
	let value_limit = limit.saturating_sub(label_chars + 1);
	(label.to_owned(), ellipsize_to_limit(value, value_limit))
}

fn ellipsize_to_limit(value: &str, limit: usize) -> String {
	let len = value.chars().count();
	if len <= limit {
		return value.to_owned();
	}
	if limit <= 1 {
		return "…".to_owned();
	}
	let mut truncated = value.chars().take(limit - 1).collect::<String>();
	truncated.push('…');
	truncated
}

impl Tone {
	pub(crate) fn text(self, style: &StatusBarStyle) -> Color {
		match self {
			Self::Normal => style.text_normal,
			Self::Accent => style.text_accent,
			Self::Success => style.text_success,
			Self::Warning => style.text_warning,
			Self::Danger => style.text_danger,
		}
	}
}

impl Segment {
	fn width(&self) -> SegmentWidth {
		match self {
			Self::Text { width, .. }
			| Self::LabelValue { width, .. }
			| Self::Spinner { width, .. }
			| Self::Progress { width, .. } => *width,
		}
	}

	fn reserved_pixels(&self, chars: u16, slot: SlotKind) -> f32 {
		let chars = f32::from(chars) * SEGMENT_CHAR_WIDTH;
		let chrome = match slot.resolve(self) {
			SlotKind::Auto | SlotKind::Text => SEGMENT_HORIZONTAL_PADDING,
			SlotKind::LabelValue => SEGMENT_HORIZONTAL_PADDING + LABEL_VALUE_GAP,
			SlotKind::Spinner => {
				SEGMENT_HORIZONTAL_PADDING + SPINNER_SIZE_HINT + SPINNER_LABEL_GAP
			}
			SlotKind::Progress => {
				SEGMENT_HORIZONTAL_PADDING + PROGRESS_WIDTH + (PROGRESS_GAP * 2.0)
			}
		};
		(chars + chrome).ceil()
	}
}

impl SegmentWidth {
	fn reserved_pixels(self, segment: &Segment) -> f32 {
		match self {
			SegmentWidth::Chars { chars, slot } => segment.reserved_pixels(chars, slot),
			_ => 0.0,
		}
	}
}

impl SlotKind {
	fn resolve(self, segment: &Segment) -> Self {
		match self {
			SlotKind::Auto => match segment {
				Segment::Text { .. } => SlotKind::Text,
				Segment::LabelValue { .. } => SlotKind::LabelValue,
				Segment::Spinner { .. } => SlotKind::Spinner,
				Segment::Progress { .. } => SlotKind::Progress,
			},
			other => other,
		}
	}
}

impl Default for LayoutConfig {
	fn default() -> Self {
		Self {
			inset_x: STATUS_BAR_INSET_X,
			bottom_gap: STATUS_BAR_INSET_BOTTOM,
			left_portion: LEFT_LANE_PORTION,
			right_portion: RIGHT_LANE_PORTION,
		}
	}
}

impl Default for StatusBarStyle {
	fn default() -> Self {
		Self {
			rail_background: colors::STATUS_BAR_RAIL_BACKGROUND,
			rail_separator: colors::STATUS_BAR_RAIL_SEPARATOR,
			segment_background: colors::STATUS_BAR_SEGMENT_BACKGROUND,
			segment_border: colors::STATUS_BAR_SEGMENT_BORDER,
			progress_background: colors::PROGRESS_BAR_TRACK_BACKGROUND,
			progress_bar: colors::PROGRESS_BAR_FILL,
			text_normal: colors::STATUS_BAR_TEXT,
			text_accent: colors::STATUS_BAR_TEXT_ACCENT,
			text_success: colors::STATUS_BAR_TEXT_SUCCESS,
			text_warning: colors::STATUS_BAR_TEXT_WARNING,
			text_danger: colors::STATUS_BAR_TEXT_DANGER,
		}
	}
}

#[allow(dead_code)]
pub fn format_elapsed(duration: Duration) -> String {
	let total_ms = duration.as_millis() as u64;
	let millis = total_ms % 1000;
	let seconds = total_ms / 1000;
	let minutes = seconds / 60;
	let seconds = seconds % 60;
	if minutes > 0 {
		format!("{minutes:02}:{seconds:02}.{millis:03}")
	} else {
		format!("{seconds}.{millis:03}s")
	}
}
