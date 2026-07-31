#![allow(
	clippy::collapsible_if,
	clippy::collapsible_else_if,
	clippy::bad_bit_mask,
	clippy::derivable_impls,
	clippy::doc_lazy_continuation,
	clippy::enum_variant_names,
	clippy::field_reassign_with_default,
	clippy::filter_map_bool_then,
	clippy::io_other_error,
	clippy::iter_kv_map,
	clippy::large_enum_variant,
	clippy::manual_abs_diff,
	clippy::manual_clamp,
	clippy::manual_inspect,
	clippy::manual_is_multiple_of,
	clippy::manual_map,
	clippy::match_like_matches_macro,
	clippy::needless_lifetimes,
	clippy::needless_borrow,
	clippy::needless_match,
	clippy::needless_option_as_deref,
	clippy::needless_range_loop,
	clippy::new_without_default,
	clippy::nonminimal_bool,
	clippy::print_with_newline,
	clippy::question_mark,
	clippy::redundant_closure,
	clippy::redundant_at_rest_pattern,
	clippy::redundant_locals,
	clippy::single_match,
	clippy::too_many_arguments,
	clippy::type_complexity,
	clippy::unnecessary_lazy_evaluations,
	clippy::while_let_on_iterator
)]

pub mod audit;
pub mod backup;
pub mod build_info;
pub mod cli;
pub mod client;
pub mod config;
pub mod consensus;
pub mod crypto;
pub mod governance;
pub mod hash;
pub mod integration;
pub mod metrics;
pub mod network;
pub mod node;
pub mod schema;
pub mod simulator;
pub mod storage;
