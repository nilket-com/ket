use super::language::{DefinitionResult, LanguageId};
use crate::core::{BufferId, DocByte, RequestId, StateId, ViewId};
use crate::ecs::NodeId;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct NavigationStamp {
	pub(crate) request_id: RequestId,
	pub(crate) buffer_id: BufferId,
	pub(crate) root: NodeId,
	pub(crate) state_id: StateId,
	pub(crate) view_id: ViewId,
	pub(crate) cursor: DocByte,
}

#[derive(Debug)]
pub(crate) struct GotoDefinitionRequest {
	pub(crate) stamp: NavigationStamp,
	pub(crate) content: Vec<u8>,
	pub(crate) file_path: String,
	pub(crate) language_id: LanguageId,
}

#[derive(Debug)]
pub(crate) struct NavigationResolved {
	pub(crate) stamp: NavigationStamp,
	pub(crate) result: DefinitionResult,
}
