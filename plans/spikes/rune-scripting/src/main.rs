use rune::runtime::Value;
use rune::{Context, Source, Sources, Vm};
use std::sync::Arc;
mod host;
mod session;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

fn compile(context: &Context, source: &str) -> Result<rune::Unit> {
	let mut sources = Sources::new();
	sources.insert(Source::memory(source)?)?;
	let mut diagnostics = rune::Diagnostics::new();
	let result = rune::prepare(&mut sources)
		.with_context(context)
		.with_diagnostics(&mut diagnostics)
		.build();
	match result {
		Ok(unit) => Ok(unit),
		Err(error) => Err(format!("{error}: {diagnostics:?}\nGenerated source:\n{source}").into()),
	}
}

fn call(context: &Context, source: &str, state: Value) -> Result<Value> {
	let unit = compile(context, source)?;
	let mut vm = Vm::new(Arc::new(context.runtime()?), Arc::new(unit));
	rune::runtime::budget::with(2_000_000, || vm.call(["main"], (state,)))
		.call()
		.map_err(|e| e.to_string().into())
}
fn display(value: &Value) -> String {
	serde_json::to_string(value).unwrap_or_else(|e| format!("<serialization error: {e}>"))
}

fn main() -> Result<()> {
	let mut context = Context::with_default_modules()?;
	host::install(&mut context)?;
	let args: Vec<String> = std::env::args().skip(1).collect();
	if args.first().is_some_and(|s| s == "eval") {
		let value =
			session::Session::new().eval(&context, args.get(1).ok_or("eval needs source")?)?;
		println!("{}", display(&value));
		return Ok(());
	}
	if args.first().is_some_and(|s| s == "repl") {
		use std::io::{BufRead, IsTerminal, Write};
		let mut session = session::Session::new();
		let terminal = std::io::stdin().is_terminal();
		let mut pending = None::<String>;
		if terminal {
			print!("Rune scripting spike (:begin/:end, :reset, :quit)\n> ");
			std::io::stdout().flush()?;
		}
		for line in std::io::stdin().lock().lines() {
			let line = line?;
			let input = match line.as_str() {
				":quit" => break,
				":reset" => {
					session = session::Session::new();
					pending = None;
					continue;
				}
				":begin" => {
					pending = Some(String::new());
					continue;
				}
				":end" => pending.take().ok_or(":end without :begin")?,
				_ => {
					if let Some(input) = &mut pending {
						input.push_str(&line);
						input.push('\n');
						continue;
					}
					line
				}
			};
			match session.eval(&context, &input) {
				Ok(value) => println!("{}", display(&value)),
				Err(e) => eprintln!("{e}"),
			}
			if terminal {
				print!("> ");
				std::io::stdout().flush()?;
			}
		}
		return Ok(());
	}
	if args.first().is_some_and(|s| s == "run") {
		let source = std::fs::read_to_string(args.get(1).ok_or("run needs a file")?)?;
		let arguments = serde_json::from_str(&serde_json::to_string(&args[2..])?)?;
		let value = call(&context, &source, arguments)?;
		match rune::from_value::<std::result::Result<Value, Value>>(value.clone()) {
			Ok(Ok(value)) => println!("{}", display(&value)),
			Ok(Err(error)) => {
				return Err(format!("script returned Err: {}", display(&error)).into());
			}
			Err(_) => println!("{}", display(&value)),
		}
		return Ok(());
	}
	let state = call(
		&context,
		r#"
        struct Boxed { value }
        fn helper() { 10 }
        pub fn main(_) {
            let shared = [1];
            let closure = || helper() + shared[0];
            #{shared, closure, instance: Boxed {value: 7}, function: helper}
        }
    "#,
		Value::empty(),
	)?;
	let result = call(
		&context,
		r#"
        struct Boxed { value }
        fn helper() { 20 }
        impl Boxed { fn read(self) { self.value } }
        pub fn main(state) {
            let c = state.closure; let f = state.function;
            (c(), f(), helper(), state.instance.value,
             state.instance is Boxed, state.instance.read())
        }
    "#,
		state.clone(),
	)?;
	assert_eq!(display(&result), "[11,10,20,7,true,7]");
	println!(
		"retained closure/function, new function, old struct field/type/method: {}",
		display(&result)
	);
	assert!(compile(&context, "pub fn main(s) { let x = ; }").is_err());
	let result = call(
		&context,
		"pub fn main(s) { let c = s.closure; c() }",
		state.clone(),
	)?;
	assert_eq!(display(&result), "11");
	println!("after compile failure: {result:?}");
	let failure = call(
		&context,
		"pub fn main(s) { s.shared.push(2); panic!(\"after mutation\"); }",
		state.clone(),
	);
	println!("runtime failure: {}", failure.unwrap_err());
	println!(
		"shared object after failed input: {}",
		display(&call(
			&context,
			"pub fn main(s) { s.shared }",
			state.clone()
		)?)
	);
	for (name, source) in [
		(
			"changed struct shape",
			"struct Boxed { other, value } pub fn main(s) { (s.instance is Boxed, s.instance.value) }",
		),
		(
			"new field on old instance",
			"struct Boxed { other, value } pub fn main(s) { s.instance.other }",
		),
		(
			"old closure without declarations",
			"pub fn main(s) { s.shared[0] = 5; let c = s.closure; c() }",
		),
	] {
		match call(&context, source, state.clone()) {
			Ok(value) => println!("{name}: {}", display(&value)),
			Err(e) => println!("{name}: {e}"),
		}
	}
	host::process_checks(&context)?;
	session::checks(&context)?;
	Ok(())
}
