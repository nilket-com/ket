use std::process::ExitCode;

const RESERVED_MESSAGE_SUFFIX: &str = "is reserved; production execution is not wired in ADR 040";

pub fn run<I>(program: &str, args: I) -> ExitCode
where
	I: IntoIterator<Item = String>,
{
	let args: Vec<String> = args.into_iter().collect();
	match args.as_slice() {
		[] => {
			print_usage(program);
			ExitCode::from(2)
		}
		[flag] if flag == "--help" || flag == "-h" => {
			print_usage(program);
			ExitCode::SUCCESS
		}
		[flag] if flag == "--version" || flag == "-V" => {
			println!("toron {}", env!("CARGO_PKG_VERSION"));
			ExitCode::SUCCESS
		}
		[verb] if verb == "start" => reserved("start"),
		[verb, rest @ ..] if verb == "sql" && !rest.is_empty() => reserved("sql"),
		[verb] if verb == "sql" => reserved("sql"),
		[verb] if verb == "inspect" => reserved("inspect"),
		[verb, _rest @ ..] if verb == "inspect" => reserved("inspect"),
		[unknown, ..] => {
			eprintln!("unknown toron command: {unknown}");
			print_usage(program);
			ExitCode::from(2)
		}
	}
}

fn reserved(verb: &str) -> ExitCode {
	eprintln!("toron {verb} {RESERVED_MESSAGE_SUFFIX}");
	ExitCode::from(2)
}

fn print_usage(program: &str) {
	eprintln!(
		"usage:\n  {program} start\n  {program} sql <statement...>\n  {program} inspect [target]\n\nAll Toron role commands are reserved in ADR 040."
	);
}
