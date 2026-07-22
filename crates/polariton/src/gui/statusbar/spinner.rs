use iced::mouse;
use iced::widget::canvas::{self, Canvas, Frame, Geometry, Path, Stroke};
use iced::{Color, Element, Length, Point, Rectangle, Renderer, Theme};

use crate::gui::statusbar::core::{StatusBarStyle, Tone};

const DOT_COUNT: usize = 8;
const FRAMES_PER_STEP: usize = 3;
const CYCLE_FRAMES: usize = DOT_COUNT * FRAMES_PER_STEP;
const SWEEP_FRAMES: usize = DOT_COUNT;
const DOT_RADIUS: f32 = 1.5;
const SPINNER_SIZE: f32 = 16.0;
const SPINNER_ALPHA: f32 = 0.8;

pub fn view<Message: 'static>(
	phase: usize,
	tone: Tone,
	style: StatusBarStyle,
) -> Element<'static, Message> {
	Canvas::new(Spinner { phase, tone, style })
		.width(Length::Fixed(SPINNER_SIZE))
		.height(Length::Fixed(SPINNER_SIZE))
		.into()
}

struct Spinner {
	phase: usize,
	tone: Tone,
	style: StatusBarStyle,
}

impl<Message> canvas::Program<Message> for Spinner {
	type State = ();

	fn draw(
		&self,
		_state: &Self::State,
		renderer: &Renderer,
		_theme: &Theme,
		bounds: Rectangle,
		_cursor: mouse::Cursor,
	) -> Vec<Geometry<Renderer>> {
		let mut frame = Frame::new(renderer, bounds.size());
		let center = Point::new(bounds.width * 0.5, bounds.height * 0.5);
		let orbit = bounds.width.min(bounds.height) * 0.36;
		let total = CYCLE_FRAMES + SWEEP_FRAMES;
		let frame_phase = self.phase % total;
		let dot_color = self.tone.spinner_dot(&self.style);
		let accent = self.tone.text(&self.style);

		for index in 0..DOT_COUNT {
			let progress = index as f32 / DOT_COUNT as f32;
			let angle = progress * std::f32::consts::TAU - std::f32::consts::FRAC_PI_2;
			let point = Point::new(
				center.x + orbit * angle.cos(),
				center.y + orbit * angle.sin(),
			);
			let alpha = chase_alpha(index, frame_phase);
			frame.fill(
				&Path::circle(point, DOT_RADIUS),
				Color {
					a: alpha,
					..dot_color
				},
			);
		}

		if frame_phase >= CYCLE_FRAMES {
			let sweep = (frame_phase - CYCLE_FRAMES) as f32 / SWEEP_FRAMES as f32;
			let angle = sweep * std::f32::consts::TAU - std::f32::consts::FRAC_PI_2;
			let reach = orbit + 1.5;
			let start = Point::new(
				center.x + (orbit * 0.25) * angle.cos(),
				center.y + (orbit * 0.25) * angle.sin(),
			);
			let end = Point::new(
				center.x + reach * angle.cos(),
				center.y + reach * angle.sin(),
			);
			let trail = Color {
				a: 0.9 - sweep * 0.3,
				..accent
			};
			frame.stroke(
				&Path::line(start, end),
				Stroke::default().with_width(1.7).with_color(trail),
			);
		}

		vec![frame.into_geometry()]
	}
}

fn chase_alpha(index: usize, frame_phase: usize) -> f32 {
	let lead = (frame_phase / FRAMES_PER_STEP) % DOT_COUNT;
	let subframe = (frame_phase % FRAMES_PER_STEP) as f32 / FRAMES_PER_STEP as f32;
	let distance = (lead + DOT_COUNT - index) % DOT_COUNT;
	match distance {
		0 => 0.95 - subframe * 0.12,
		1 => 0.58 + subframe * 0.18,
		2 => 0.3 + subframe * 0.08,
		3 => 0.16,
		_ => 0.07,
	}
}

impl Tone {
	pub(crate) fn spinner_dot(self, style: &StatusBarStyle) -> Color {
		match self {
			Tone::Normal => Color {
				a: SPINNER_ALPHA,
				..style.text_normal
			},
			Tone::Accent => Color {
				a: SPINNER_ALPHA,
				..style.text_accent
			},
			Tone::Success => Color {
				a: SPINNER_ALPHA,
				..style.text_success
			},
			Tone::Warning => Color {
				a: SPINNER_ALPHA,
				..style.text_warning
			},
			Tone::Danger => Color {
				a: SPINNER_ALPHA,
				..style.text_danger
			},
		}
	}
}
