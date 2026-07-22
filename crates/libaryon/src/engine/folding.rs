mod set;
mod state;
mod synthetic;
mod viewport;

#[cfg(test)]
mod fold_storage_benchmark;

pub(crate) use self::set::{FoldEdit, FoldSet, FoldSnapshot};
pub(crate) use self::state::{set_subtree_fold_state, unfold_ancestor_chain};
#[cfg(test)]
pub(crate) use self::synthetic::materialize_fold_boundary_at_cursor;
pub(crate) use self::synthetic::resolve_fold_boundary_at_cursor_tracked;
pub(crate) use self::viewport::{
	clamp_cursor_line_to_viewport, doc_line_for_visual_index, max_visual_line_index,
	pan_scroll_y_to_keep_cursor_visible, scroll_viewport, snap_line_to_visible_boundary,
	visual_line_index_for_doc_line,
};
#[cfg(test)]
mod test_support;
