//! Minimal trusted-local host for the spike, not a proposed standard library.
use rune::{Context, Module, runtime::Value};
use std::io::{Read, Write};
use std::process::{Command, Stdio};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

static INTERRUPTED: AtomicBool = AtomicBool::new(false);
extern "C" fn interrupt(_: libc::c_int) {
	INTERRUPTED.store(true, Ordering::Relaxed);
}

fn error(e: impl std::fmt::Display) -> String {
	e.to_string()
}
fn json_parse(text: &str) -> Result<Value, String> {
	serde_json::from_str(text).map_err(error)
}
fn json_stringify(value: Value) -> Result<String, String> {
	serde_json::to_string(&value).map_err(error)
}
fn file_read(path: &str) -> Result<String, String> {
	let mut bytes = Vec::new();
	std::fs::File::open(path)
		.map_err(error)?
		.take(8 * 1024 * 1024 + 1)
		.read_to_end(&mut bytes)
		.map_err(error)?;
	if bytes.len() > 8 * 1024 * 1024 {
		return Err("file exceeds spike's 8 MiB limit".into());
	}
	String::from_utf8(bytes).map_err(error)
}
fn file_write(path: &str, text: &str) -> Result<(), String> {
	std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(path)
		.map_err(error)?
		.write_all(text.as_bytes())
		.map_err(error)
}
fn mkdir(path: &str) -> Result<(), String> {
	std::fs::create_dir(path).map_err(error)
}
fn absolute(path: &str) -> Result<String, String> {
	std::fs::canonicalize(path)
		.map_err(error)?
		.into_os_string()
		.into_string()
		.map_err(|_| "non-UTF8 path".into())
}
fn capture(mut input: impl Read) -> (Vec<u8>, bool) {
	let mut captured = Vec::new();
	let mut chunk = [0; 8192];
	let mut truncated = false;
	while let Ok(n) = input.read(&mut chunk) {
		if n == 0 {
			break;
		}
		let keep = n.min((2 * 1024 * 1024usize).saturating_sub(captured.len()));
		captured.extend_from_slice(&chunk[..keep]);
		truncated |= keep < n;
	}
	(captured, truncated)
}
fn process(program: &str, arguments: Value, timeout_ms: u64) -> Result<Value, String> {
	use std::os::unix::process::CommandExt;
	let values = arguments
		.borrow_ref::<rune::runtime::Vec>()
		.map_err(error)?;
	let args: Vec<String> = values
		.iter()
		.map(|v| v.borrow_string_ref().map(|s| s.to_string()).map_err(error))
		.collect::<Result<_, _>>()?;
	if timeout_ms == 0 || timeout_ms > 90_000 {
		return Err("deadline must be 1..90000 ms".into());
	}
	INTERRUPTED.store(false, Ordering::Relaxed);
	let mut child = Command::new(program)
		.args(args)
		.stdin(Stdio::null())
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.process_group(0)
		.spawn()
		.map_err(error)?;
	let pid = child.id() as i32;
	let stdout = child.stdout.take().unwrap();
	let stderr = child.stderr.take().unwrap();
	let out = std::thread::spawn(move || capture(stdout));
	let err = std::thread::spawn(move || capture(stderr));
	let deadline = Instant::now() + Duration::from_millis(timeout_ms);
	let mut timed_out = false;
	let mut cancelled = false;
	let status = loop {
		if let Some(status) = child.try_wait().map_err(error)? {
			break status;
		}
		cancelled = INTERRUPTED.load(Ordering::Relaxed);
		timed_out = Instant::now() >= deadline;
		if timed_out || cancelled {
			unsafe {
				libc::kill(-pid, libc::SIGKILL);
			}
			break child.wait().map_err(error)?;
		}
		std::thread::sleep(Duration::from_millis(5));
	};
	// Children remaining in this process group may otherwise keep pipes open.
	unsafe {
		libc::kill(-pid, libc::SIGKILL);
	}
	let (out, out_truncated) = out.join().map_err(|_| "stdout reader panicked")?;
	let (err, err_truncated) = err.join().map_err(|_| "stderr reader panicked")?;
	json_parse(
		&serde_json::json!({
			"code": status.code(), "timed_out": timed_out, "cancelled": cancelled,
			"stdout": String::from_utf8_lossy(&out), "stderr": String::from_utf8_lossy(&err),
			"truncated": out_truncated || err_truncated,
		})
		.to_string(),
	)
}
pub fn install(context: &mut Context) -> super::Result<()> {
	unsafe {
		libc::signal(libc::SIGINT, interrupt as *const () as libc::sighandler_t);
	}
	let mut module = Module::with_crate("host")?;
	module.function("json_parse", json_parse).build()?;
	module.function("json_stringify", json_stringify).build()?;
	module.function("read", file_read).build()?;
	module.function("write_new", file_write).build()?;
	module.function("mkdir", mkdir).build()?;
	module.function("absolute", absolute).build()?;
	module.function("process", process).build()?;
	context.install(module)?;
	Ok(())
}

pub fn process_checks(context: &Context) -> super::Result<()> {
	for (label, source) in [
		(
			"exit status",
			r#"pub fn main(_) { host::process("/bin/sh", ["-c", "exit 7"], 1000)? }"#,
		),
		(
			"deadline",
			r#"pub fn main(_) { host::process("/bin/sh", ["-c", "sleep 10 & wait"], 40)? }"#,
		),
		(
			"capture cap",
			r#"pub fn main(_) { let r = host::process("/usr/bin/head", ["-c", "3000000", "/dev/zero"], 1000)?; (r.truncated, r.stdout.len()) }"#,
		),
		(
			"interruption",
			r#"pub fn main(_) { host::process("/bin/sh", ["-c", "kill -INT $PPID; sleep 10"], 1000)? }"#,
		),
	] {
		let result = super::call(context, source, Value::empty())?;
		let value = serde_json::to_value(&result)?;
		match label {
			"exit status" => assert_eq!(value["code"], 7),
			"deadline" => {
				assert_eq!(value["timed_out"], true);
				assert!(value["code"].is_null());
			}
			"capture cap" => assert_eq!(value, serde_json::json!([true, 2097152])),
			"interruption" => {
				assert_eq!(value["cancelled"], true);
				assert!(value["code"].is_null());
			}
			_ => unreachable!(),
		}
		println!("{label}: {}", super::display(&result));
	}
	Ok(())
}
