use super::{DashboardMsg, DashboardState};
use crate::gui::pane_grid::{ChromeVisibility, PaneGrid, Style as PaneStyle};
use crate::gui::plot_pane;
use iced::{
	Element,
	widget::{center, text},
};

pub fn view(state: &DashboardState) -> Element<'_, DashboardMsg> {
	let Some(panes) = &state.panes else {
		return center(text("")).into();
	};

	let content = panes
		.panes()
		.iter()
		.filter_map(|&id| {
			let plot_state = state.content.get(&id)?;
			Some((id, plot_pane::view(id, plot_state)))
		})
		.collect();
	let style = PaneStyle {
		chrome: ChromeVisibility::OnHover,
		..PaneStyle::default()
	};
	PaneGrid::new(panes, content, DashboardMsg::Pane)
		.style(style)
		.into()
}
