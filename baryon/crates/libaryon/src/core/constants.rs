pub const TAB_SIZE: u32 = 4;
pub const DEFAULT_VIEWPORT_WIDTH: u16 = 80;
pub const DEFAULT_VIEWPORT_LINES: u32 = 50;
pub const DOCTAB_BAR_ROWS: u16 = 1;
pub const STATUS_BAR_ROWS: u16 = 1;
pub const EDITOR_CHROME_ROWS: u16 = DOCTAB_BAR_ROWS + STATUS_BAR_ROWS;
/// Cells reserved between side-by-side panes. The diff view uses this space
/// for its bridge canvas, so it is wider than a conventional separator.
pub const SPLIT_DIVIDER_COLS: u32 = 4;
/// Rows reserved between vertically stacked panes.
pub const SPLIT_DIVIDER_ROWS: u32 = 1;
