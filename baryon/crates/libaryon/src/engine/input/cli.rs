pub fn render_cli_help(topic: &str, program_name: &str) -> Result<String, String> {
	super::help::resolve_topic(topic).map(|topic| super::help::render_plain(&topic, program_name))
}

pub fn help_topic_hint() -> String {
	super::help::topic_hint()
}
