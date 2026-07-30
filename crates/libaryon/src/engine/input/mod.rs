mod cli;
mod command_registry;
mod help;
mod history;
mod keymap;
mod machine;
mod option_registry;
mod prompt;
mod runtime_catalog;
pub(crate) mod runtime_keymap;
mod runtime_options;
mod types;

#[cfg(test)]
mod builtin_catalog_tests;
#[cfg(test)]
mod machine_dispatch_tests;
#[cfg(test)]
mod machine_normal_tests;
#[cfg(test)]
mod machine_test_frontend;
#[cfg(test)]
mod machine_visual_tests;
#[cfg(test)]
mod runtime_catalog_tests;
#[cfg(test)]
mod runtime_keymap_tests;
#[cfg(test)]
mod runtime_option_tests;

pub use cli::{help_topic_hint, render_cli_help};
pub(crate) use machine::{InputContext, InputDispatch, InputRuntime};
pub(crate) use option_registry::rune_option_key;
pub(crate) use prompt::{
	PathCompletionResponse, PromptCursorMovement, backspace_at_prompt_cursor,
	delete_at_prompt_cursor, insert_at_prompt_cursor, move_prompt_cursor,
};
pub(crate) use runtime_catalog::{
	protected_runtime_command_tokens, protected_runtime_option_tokens,
};
pub use types::{InputCompletionStatus, InputEvent, InputProjection, KeyCode, Modifiers};
