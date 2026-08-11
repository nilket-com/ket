use crate::plot::spec::Presence;
use std::sync::Arc;

// The no-Toron build uses only Plot and Anonymous; the complete identity
// vocabulary remains always built so plain Rune and relation Rune share one
// representation rather than feature-dependent types.
#[allow(dead_code)]
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub(crate) enum ViewContentKind {
	Table,
	Plot,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ViewPresentation {
	pub(crate) content_kind: ViewContentKind,
	pub(crate) display_label: String,
	pub(crate) authored_title: Presence<Arc<String>>,
}

#[allow(dead_code)]
#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub(crate) enum LocalViewIdentity {
	Named(String),
	Anonymous(usize),
}
