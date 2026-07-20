pub mod clipboard;
mod core;
mod diff;
mod document;
mod event_bus;
mod event_subscribers;
mod file_browser;
mod folding;
mod git;
mod host;
mod host_event;
pub mod index;
mod input;
// The engine-level job substrate is intentionally wired before the Rune jobs
// binding supplies its first production spawn caller.
#[allow(dead_code)]
mod jobs;
pub mod language;
mod layout;
mod markdown_image_preview;
mod minimap;
mod protocol;
mod render;
pub(crate) mod request;
mod rune_host;
mod runtime;
// The language-neutral ownership and action boundary feeds the effective
// command, option, keymap, and event-subscriber catalogs.
#[allow(dead_code)]
mod runtime_registry;
mod semantic;
mod session;
pub mod settings;
mod shadow;
pub mod structural_graft;
mod support;
#[cfg(test)]
mod test_support;
pub mod theme;
pub mod uast_query;
pub mod ui_colors;
pub mod undo;

#[cfg(test)]
pub(crate) use self::core::MAX_SEMANTIC_BYTES;
pub use self::document::selection::resolve_visual_ranges;
pub use self::host::{EngineConfig, EngineHandle, supported_languages};
pub use self::host_event::{HostEvent, HostPanelDescriptor, HostPanelId, PanelRevision};
pub use self::input::{InputCompletionStatus, InputEvent, InputProjection, KeyCode, Modifiers};
pub use self::input::{help_topic_hint, render_cli_help};
pub use self::layout::{GutterNumbering, VisibleRow};
pub use self::language::{InlineAnnotation, InlineAnnotationKind};
pub use self::layout::{
	ViewportAnchor, clamp_cursor_to_viewport_anchor, collect_visible_rows, cursor_view_metrics,
	gutter_width_for_max_line, pan_viewport_anchor_to_keep_cursor_visible, scroll_viewport_anchor,
	viewport_geometry, viewport_geometry_for_viewport,
	viewport_geometry_for_viewport_with_numbering, viewport_geometry_for_viewport_with_options,
	viewport_geometry_with_options, viewport_max_line_on_screen,
};
pub use self::minimap::{
	EMPTY_PREVIEW_BIN, MinimapOverlay, MinimapSnapshot, OverviewSnapshot, PREVIEW_BIN_COLUMNS,
	PreviewRow, PreviewSnapshot,
};
pub use self::protocol::{
	CommandRange, ConfirmAction, EditorCommand, EditorMode, MouseIntent, MoveDirection,
	PaneFocusDirection, PaneResizeDirection, SplitDirection, SubstituteFlags, VerticalDirection,
	VisualKind, WordMotion,
};
pub use self::request::{CommandSender, InputSender};
pub use self::theme::{THEME_SPECS, Theme, ThemeFamily, canonical_theme_names};
pub use self::ui_colors::{
	ResolvedUiColor, UI_COLOR_COUNT, UI_COLOR_SPECS, UiColorDefault, UiColorKey, UiColorOverrides,
	UiColorSource, UiColorSpec, UiPalette, parse_ui_rgb,
};
