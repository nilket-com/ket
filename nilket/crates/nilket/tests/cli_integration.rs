use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Child, Command, Output, Stdio};
use std::sync::Arc;
use std::sync::Mutex;
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use nilket::local::LocalReady;
use nix::sys::signal::{Signal, kill};
use nix::unistd::Pid;

use wendelon::audit::{
	AuditAuthority, AuditEvent, AuditOperation, AuditRootSigner, AuditSink, LocalAuditSink,
	emit_signed_chain_root,
};
use wendelon::config::render_example_config;
use wendelon::schema::NodeId;

fn nilket(args: &[&str]) -> Output {
	Command::new(env!("CARGO_BIN_EXE_nilket"))
		.args(args)
		.output()
		.expect("run nilket")
}

fn nilket_command() -> Command {
	Command::new(env!("CARGO_BIN_EXE_nilket"))
}

fn stdout(output: &Output) -> String {
	String::from_utf8(output.stdout.clone()).expect("stdout is utf8")
}

fn stderr(output: &Output) -> String {
	String::from_utf8(output.stderr.clone()).expect("stderr is utf8")
}

fn fresh_dir(tag: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("system time after epoch")
		.as_nanos();
	let dir =
		std::env::temp_dir().join(format!("nilket-cli-{tag}-{}-{}", std::process::id(), nanos));
	std::fs::create_dir_all(&dir).expect("create temp dir");
	dir
}

const DOGFOOD_SQL: &str = include_str!("fixtures/toron_dogfood.sql");
static LIVE_LOCAL_LOCK: Mutex<()> = Mutex::new(());

fn dogfood_statements() -> Vec<String> {
	DOGFOOD_SQL
		.lines()
		.map(str::trim)
		.filter(|line| !line.is_empty() && !line.starts_with("--"))
		.map(|line| {
			line.strip_suffix(';')
				.unwrap_or_else(|| panic!("contract statement lacks semicolon: {line}"))
				.to_owned()
		})
		.collect()
}

struct LiveLocal {
	child: Child,
	ready: LocalReady,
	child_pids: Vec<u32>,
}

impl LiveLocal {
	fn spawn(profile: &std::path::Path, stderr_log: &std::path::Path) -> Self {
		let stderr = std::fs::OpenOptions::new()
			.create(true)
			.append(true)
			.open(stderr_log)
			.expect("open local-profile stderr log");
		let mut child = nilket_command()
			.args([
				"local",
				"--data-dir",
				profile.to_str().expect("utf8 live profile path"),
			])
			.stdout(Stdio::piped())
			.stderr(stderr)
			.spawn()
			.expect("spawn live local profile");
		let stdout = child.stdout.take().expect("local profile stdout");
		let (sender, receiver) = std::sync::mpsc::sync_channel(1);
		thread::spawn(move || {
			let mut line = String::new();
			let result = BufReader::new(stdout).read_line(&mut line).map(|_| line);
			let _ = sender.send(result);
		});
		let line = receiver
			.recv_timeout(Duration::from_secs(60))
			.unwrap_or_else(|error| {
				let _ = child.kill();
				let _ = child.wait();
				panic!("local profile did not publish readiness: {error}")
			})
			.expect("read local readiness line");
		let ready =
			LocalReady::parse(&line).unwrap_or_else(|error| panic!("parse {line:?}: {error}"));
		let child_pids = wait_for_direct_children(child.id(), 2, Duration::from_secs(10));
		assert_eq!(child_pids.len(), 2, "real local profile owns two roles");
		assert_ne!(child_pids[0], child_pids[1]);
		Self {
			child,
			ready,
			child_pids,
		}
	}

	fn stop_gracefully(mut self) {
		kill(Pid::from_raw(self.child.id() as i32), Signal::SIGINT)
			.expect("signal local supervisor");
		let status = wait_for_child(&mut self.child, Duration::from_secs(90));
		assert!(status.success(), "local supervisor exit: {status}");
		for pid in &self.child_pids {
			assert!(
				wait_for_process_exit(*pid, Duration::from_secs(5)),
				"role process {pid} survived graceful supervisor exit"
			);
		}
	}
}

impl Drop for LiveLocal {
	fn drop(&mut self) {
		if self.child.try_wait().ok().flatten().is_none() {
			let _ = self.child.kill();
			let _ = self.child.wait();
		}
	}
}

fn direct_children(pid: u32) -> Vec<u32> {
	std::fs::read_to_string(format!("/proc/{pid}/task/{pid}/children"))
		.unwrap_or_default()
		.split_ascii_whitespace()
		.filter_map(|value| value.parse().ok())
		.collect()
}

fn wait_for_direct_children(pid: u32, count: usize, timeout: Duration) -> Vec<u32> {
	let deadline = Instant::now() + timeout;
	loop {
		let children = direct_children(pid);
		if children.len() == count || Instant::now() >= deadline {
			return children;
		}
		thread::sleep(Duration::from_millis(25));
	}
}

fn wait_for_child(child: &mut Child, timeout: Duration) -> std::process::ExitStatus {
	let deadline = Instant::now() + timeout;
	loop {
		if let Some(status) = child.try_wait().expect("poll local supervisor") {
			return status;
		}
		if Instant::now() >= deadline {
			let _ = child.kill();
			return child.wait().expect("reap timed-out local supervisor");
		}
		thread::sleep(Duration::from_millis(25));
	}
}

fn wait_for_process_exit(pid: u32, timeout: Duration) -> bool {
	let deadline = Instant::now() + timeout;
	loop {
		if !std::path::Path::new(&format!("/proc/{pid}")).exists() {
			return true;
		}
		if Instant::now() >= deadline {
			return false;
		}
		thread::sleep(Duration::from_millis(25));
	}
}

fn run_live_sql(endpoint: std::net::SocketAddr, statement: &str) -> Output {
	nilket_command()
		.args([
			"toron",
			"sql",
			"--endpoint",
			&endpoint.to_string(),
			statement,
		])
		.output()
		.expect("run dogfood SQL")
}

fn assert_sql_success(endpoint: std::net::SocketAddr, statement: &str) -> String {
	let output = run_live_sql(endpoint, statement);
	assert!(
		output.status.success(),
		"statement={statement:?} {output:?}"
	);
	stdout(&output)
}

fn assert_sql_scalar(endpoint: std::net::SocketAddr, statement: &str, expected: &str) {
	let rendered = assert_sql_success(endpoint, statement);
	assert!(
		rendered
			.lines()
			.any(|line| line.split('|').any(|cell| cell.trim() == expected)),
		"missing exact scalar {expected:?} in {rendered}"
	);
}

fn assert_executed_or_typed_refusal(endpoint: std::net::SocketAddr, statement: &str) {
	let output = run_live_sql(endpoint, statement);
	if output.status.success() {
		eprintln!("dogfood executed: {statement}");
		return;
	}
	let refusal = stderr(&output);
	assert_eq!(output.status.code(), Some(1), "{output:?}");
	assert!(refusal.contains("Toron "), "untyped refusal: {refusal}");
	assert!(
		!refusal.contains("connection"),
		"connectivity failure: {refusal}"
	);
	assert!(
		!refusal.contains("timed out"),
		"timeout is not a SQL refusal: {refusal}"
	);
	eprintln!("dogfood typed refusal: {statement}: {}", refusal.trim());
}

fn minimal_toron_config(task_listener_bind: &str) -> String {
	format!(
		r#"[node]
worker_id = "00000000-0000-0000-0000-000000000042"
tenant_id = 1
drain_timeout_seconds = 1

[worker]
task_listener_bind = "{task_listener_bind}"
memory_budget_bytes = 1048576

[storage.wendelon]
bootstrap_peers = [
  {{ node_id = 1, address = "127.0.0.1:7001" }},
]

[catalog]

[observability]
level = "info"
format = "pretty"
"#
	)
}

fn minimal_toron_config_with_sql(task_listener_bind: &str, sql_listener_bind: &str) -> String {
	let mut config = minimal_toron_config(task_listener_bind);
	config.push_str(&format!(
		r#"
[sql]
transport = "quic"
listener_bind = "{sql_listener_bind}"
statement_timeout_seconds = 60

[coordinator]
include_self_worker = true
"#
	));
	config
}

fn spawn_toron_sql_daemon(tag: &str) -> (Child, String) {
	let dir = fresh_dir(tag);
	let path = dir.join("toron.toml");
	std::fs::write(
		&path,
		minimal_toron_config_with_sql("127.0.0.1:0", "127.0.0.1:0"),
	)
	.expect("write toron config");
	let mut child = nilket_command()
		.args(["toron", "start", path.to_str().expect("utf8 config path")])
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("spawn nilket toron start");
	let stdout = child.stdout.take().expect("child stdout");
	let mut reader = BufReader::new(stdout);
	let mut ready_line = String::new();
	let mut sql_ready_line = String::new();
	reader.read_line(&mut ready_line).expect("read READY line");
	reader
		.read_line(&mut sql_ready_line)
		.expect("read SQL_READY line");
	assert!(
		ready_line.starts_with("READY 127.0.0.1:"),
		"unexpected READY line: {ready_line:?}"
	);
	let sql_endpoint = sql_ready_line
		.strip_prefix("SQL_READY ")
		.unwrap_or_else(|| panic!("unexpected SQL_READY line: {sql_ready_line:?}"))
		.trim()
		.to_string();
	(child, sql_endpoint)
}

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

#[test]
fn nilket_no_args_selects_local_and_reports_missing_profile_location_honestly() {
	let output = nilket_command()
		.env_remove("XDG_DATA_HOME")
		.env_remove("HOME")
		.output()
		.expect("run bare nilket");

	assert_eq!(output.status.code(), Some(1), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert!(
		stderr(&output).contains("pass --data-dir PATH"),
		"{output:?}"
	);
}

#[test]
fn nilket_unknown_root_command_exits_nonzero_with_usage() {
	let output = nilket(&["frobnicate"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	let stderr = stderr(&output);
	assert!(
		stderr.contains("unknown nilket command: frobnicate"),
		"{stderr}"
	);
	assert!(stderr.contains("wendelon"), "{stderr}");
	assert!(stderr.contains("toron"), "{stderr}");
}

#[test]
fn nilket_version_prints_single_coordinate() {
	let output = nilket(&["--version"]);

	assert!(output.status.success(), "{output:?}");
	assert_eq!(stdout(&output), "nilket 0.1.0\n");
	assert!(stderr(&output).is_empty(), "{output:?}");
}

#[test]
fn nilket_root_sql_still_rejected() {
	let output = nilket(&["sql", "SELECT 1"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	let stderr = stderr(&output);
	assert!(stderr.contains("unknown nilket command: sql"), "{stderr}");
	assert!(stderr.contains("wendelon"), "{stderr}");
	assert!(stderr.contains("toron"), "{stderr}");
}

#[test]
fn nilket_toron_sql_requires_statement() {
	let output = nilket(&["toron", "sql"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert_eq!(
		stderr(&output),
		"toron sql requires <statement...> or --file <path>\n"
	);
}

#[test]
fn nilket_toron_start_requires_config_path() {
	let output = nilket(&["toron", "start"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert_eq!(
		stderr(&output),
		"toron start requires <path/to/toron.toml>\n"
	);
}

#[test]
fn nilket_toron_start_rejects_missing_config_file() {
	let dir = fresh_dir("toron-missing-config");
	let missing = dir.join("missing.toml");
	let output = nilket(&[
		"toron",
		"start",
		missing.to_str().expect("utf8 missing config path"),
	]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	let stderr = stderr(&output);
	assert!(
		stderr.contains(&format!("failed to load {}", missing.display())),
		"{stderr}"
	);
}

#[test]
fn nilket_toron_start_rejects_non_loopback_bind() {
	let dir = fresh_dir("toron-non-loopback");
	let path = dir.join("toron.toml");
	std::fs::write(&path, minimal_toron_config("0.0.0.0:0")).expect("write toron config");

	let output = nilket(&["toron", "start", path.to_str().expect("utf8 config path")]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert!(
		stderr(&output).contains(
			"worker listener requires authenticated transport for non-loopback bind address"
		),
		"{output:?}"
	);
}

#[test]
fn nilket_toron_sql_dials_endpoint_from_flag() {
	let (mut child, endpoint) = spawn_toron_sql_daemon("toron-sql-flag");
	let output = nilket_command()
		.args([
			"toron",
			"sql",
			"--endpoint",
			&endpoint,
			"SELEC * FROM missing",
		])
		.env("TORON_SQL_ENDPOINT", "127.0.0.1:9")
		.output()
		.expect("run nilket toron sql");
	let _ = child.kill();
	let _ = child.wait();

	assert_eq!(output.status.code(), Some(1), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert!(stderr(&output).contains("Toron SyntaxError"), "{output:?}");
}

#[test]
fn nilket_toron_sql_dials_endpoint_from_env() {
	let (mut child, endpoint) = spawn_toron_sql_daemon("toron-sql-env");
	let output = nilket_command()
		.args(["toron", "sql", "SELEC * FROM missing"])
		.env("TORON_SQL_ENDPOINT", endpoint)
		.output()
		.expect("run nilket toron sql");
	let _ = child.kill();
	let _ = child.wait();

	assert_eq!(output.status.code(), Some(1), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert!(stderr(&output).contains("Toron SyntaxError"), "{output:?}");
}

#[test]
fn nilket_toron_sql_reports_connection_failure() {
	let output = nilket(&[
		"toron",
		"sql",
		"--endpoint",
		"127.0.0.1:9",
		"SELECT * FROM missing",
	]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert!(
		stderr(&output).contains("Toron connection timed out"),
		"{output:?}"
	);
}

#[test]
fn nilket_toron_sql_refuses_positional_script_before_connecting() {
	let output = nilket(&[
		"toron",
		"sql",
		"--endpoint",
		"127.0.0.1:9",
		"SELECT 1; SELECT 2",
	]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert_eq!(
		stderr(&output),
		"Toron SQL execution accepts exactly one statement per request; use `nilket toron sql --file <path>` for a script\n"
	);
}

#[test]
fn nilket_toron_sql_file_rejects_invalid_sources_before_connecting() {
	let dir = fresh_dir("toron-sql-file-invalid");
	let empty = dir.join("empty.sql");
	let comments = dir.join("comments.sql");
	let non_utf8 = dir.join("non-utf8.sql");
	let unterminated = dir.join("unterminated.sql");
	let missing = dir.join("missing.sql");
	std::fs::write(&empty, "").expect("write empty SQL");
	std::fs::write(&comments, "-- only\n/* comments */;").expect("write comments SQL");
	std::fs::write(&non_utf8, [0xff]).expect("write non-UTF-8 SQL");
	std::fs::write(&unterminated, "SELECT * FROM first; SELECT 'unterminated")
		.expect("write unterminated SQL");

	for (path, expected) in [
		(&empty, "contains no SQL statements"),
		(&comments, "contains no SQL statements"),
		(&non_utf8, "not valid UTF-8"),
		(&unterminated, "could not be tokenized"),
		(&missing, "No such file"),
		(&dir, "toron sql --file"),
	] {
		let output = nilket(&[
			"toron",
			"sql",
			"--endpoint",
			"127.0.0.1:9",
			"--file",
			path.to_str().expect("utf8 fixture path"),
		]);
		assert_eq!(output.status.code(), Some(2), "{output:?}");
		assert!(stdout(&output).is_empty(), "{output:?}");
		assert!(stderr(&output).contains(expected), "{output:?}");
		assert!(!stderr(&output).contains("timed out"), "{output:?}");
	}
}

#[test]
fn nilket_toron_sql_file_first_failure_keeps_ordinary_exit_classification() {
	let (mut child, endpoint) = spawn_toron_sql_daemon("toron-sql-file-first-failure");
	let dir = fresh_dir("toron-sql-file-first-failure-source");
	let path = dir.join("first-fails.sql");
	std::fs::write(
		&path,
		"SELEC * FROM missing; SELECT * FROM never_dispatched;",
	)
	.expect("write first-failure SQL file");
	let output = nilket(&[
		"toron",
		"sql",
		"--endpoint",
		&endpoint,
		"--file",
		path.to_str().expect("utf8 SQL path"),
	]);
	let _ = child.kill();
	let _ = child.wait();

	assert_eq!(output.status.code(), Some(1), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	let error = stderr(&output);
	assert!(
		error.contains("statement 1/2 failed after 0 completed statements"),
		"{error}"
	);
	assert!(error.contains("Toron SyntaxError"), "{error}");
	assert!(!error.contains("statement 2/2"), "{error}");
}

#[test]
#[ignore = "requires Linux io_uring and the fixed local-profile ports"]
fn nilket_toron_sql_file_executes_in_order_and_attributes_results() {
	let _serial = LIVE_LOCAL_LOCK.lock().expect("live-profile test lock");
	let harness = fresh_dir("toron-sql-file-success");
	let profile = harness.join("profile");
	let log = harness.join("local.stderr.log");
	let local = LiveLocal::spawn(&profile, &log);
	let path = harness.join("four.sql");
	std::fs::write(
		&path,
		"CREATE TABLE file_one (id BIGINT NOT NULL, label TEXT);\nINSERT INTO file_one (id, label) VALUES (1, 'one'), (2, 'two');\nCREATE TABLE file_two (id BIGINT NOT NULL, label TEXT);\nINSERT INTO file_two (id, label) VALUES (1, 'one'), (2, 'two'), (3, 'three');",
	)
		.expect("write SQL file");
	let output = nilket_command()
		.args([
			"toron",
			"sql",
			"--endpoint",
			&local.ready.sql_endpoint.to_string(),
			"--file",
			path.to_str().expect("utf8 SQL path"),
		])
		.output()
		.expect("run four-statement SQL file");

	assert!(output.status.success(), "{output:?}");
	assert!(stderr(&output).is_empty(), "{output:?}");
	let output_text = stdout(&output);
	for heading in [
		"statement 1/4",
		"statement 2/4",
		"statement 3/4",
		"statement 4/4",
	] {
		assert_eq!(output_text.matches(heading).count(), 1, "{output_text}");
	}
	assert_eq!(output_text.matches("OK").count(), 4, "{output_text}");
	assert_sql_scalar(
		local.ready.sql_endpoint,
		"SELECT COUNT(*) FROM file_one",
		"2",
	);
	assert_sql_scalar(
		local.ready.sql_endpoint,
		"SELECT COUNT(*) FROM file_two",
		"3",
	);
	local.stop_gracefully();
	std::fs::remove_dir_all(harness).expect("remove SQL-file success fixture");
}

#[test]
#[ignore = "requires Linux io_uring and the fixed local-profile ports"]
fn nilket_toron_sql_file_stops_with_partial_application_diagnostic() {
	let _serial = LIVE_LOCAL_LOCK.lock().expect("live-profile test lock");
	let harness = fresh_dir("toron-sql-file-partial");
	let profile = harness.join("profile");
	let log = harness.join("local.stderr.log");
	let local = LiveLocal::spawn(&profile, &log);
	let path = harness.join("partial.sql");
	let failing = "INSERT INTO missing\n  VALUES (1)";
	std::fs::write(
		&path,
		format!(
			"CREATE TABLE partial_one (id BIGINT NOT NULL);\nINSERT INTO partial_one (id) VALUES (1), (2);\n{failing};\nCREATE TABLE partial_four (id BIGINT NOT NULL);"
		),
	)
	.expect("write partial SQL file");
	let output = nilket_command()
		.args([
			"toron",
			"sql",
			"--endpoint",
			&local.ready.sql_endpoint.to_string(),
			"-f",
			path.to_str().expect("utf8 SQL path"),
		])
		.output()
		.expect("run partial SQL file");

	assert_eq!(output.status.code(), Some(3), "{output:?}");
	let output_text = stdout(&output);
	assert!(output_text.contains("statement 1/4"), "{output:?}");
	assert!(output_text.contains("statement 2/4"), "{output:?}");
	assert!(!output_text.contains("statement 3/4"), "{output:?}");
	assert!(!output_text.contains("statement 4/4"), "{output:?}");
	let error = stderr(&output);
	assert!(
		error.contains("statement 3/4 failed after 2 completed statements"),
		"{error}"
	);
	assert!(
		error.contains("prior effects were not rolled back"),
		"{error}"
	);
	assert!(
		error.contains(&format!(
			"--- failing SQL ---\n{failing}\n--- end failing SQL ---"
		)),
		"{error}"
	);
	assert!(error.contains("Toron CatalogError"), "{error}");
	assert_sql_scalar(
		local.ready.sql_endpoint,
		"SELECT COUNT(*) FROM partial_one",
		"2",
	);
	let tables = assert_sql_success(
		local.ready.sql_endpoint,
		"SELECT * FROM information_schema.tables",
	);
	assert!(!tables.contains("partial_four"), "{tables}");
	local.stop_gracefully();
	std::fs::remove_dir_all(harness).expect("remove SQL-file partial fixture");
}

#[test]
fn nilket_toron_inspect_routes_to_toron_cli() {
	let output = nilket(&["toron", "inspect", "workers"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert_eq!(
		stderr(&output),
		"toron inspect does not accept positional arguments\n"
	);
}

#[test]
fn nilket_wendelon_inspect_routes_to_wendelon_cli() {
	let output = nilket(&["wendelon", "inspect"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert_eq!(
		stderr(&output),
		"wendelon inspect requires --server-name, --cert, --key, and --ca\n"
	);
}

#[test]
fn nilket_toron_print_example_config() {
	let output = nilket(&["toron", "--print-example-config"]);

	assert!(output.status.success(), "{output:?}");
	assert_eq!(stdout(&output), toron::config::render_example_config());
	assert!(stderr(&output).is_empty(), "{output:?}");
}

#[test]
fn nilket_toron_write_example_config_refuses_overwrite() {
	let dir = fresh_dir("toron-write-example");
	let path = dir.join("toron.toml");
	let path_str = path.to_str().expect("utf8 config path");

	let first = nilket(&["toron", "--write-example-config", path_str]);
	assert!(first.status.success(), "{first:?}");
	assert_eq!(
		std::fs::read_to_string(&path).expect("read written config"),
		toron::config::render_example_config()
	);

	let second = nilket(&["toron", "--write-example-config", path_str]);
	assert_eq!(second.status.code(), Some(2), "{second:?}");
	assert!(
		stderr(&second).contains("failed to write starter config"),
		"{second:?}"
	);
}

#[test]
fn toron_daemon_prints_ready_endpoint() {
	let dir = fresh_dir("toron-ready");
	let path = dir.join("toron.toml");
	std::fs::write(&path, minimal_toron_config("127.0.0.1:0")).expect("write toron config");
	let mut child = nilket_command()
		.args(["toron", "start", path.to_str().expect("utf8 config path")])
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("spawn nilket toron start");
	let stdout = child.stdout.take().expect("child stdout");
	let mut reader = BufReader::new(stdout);
	let mut line = String::new();
	reader.read_line(&mut line).expect("read READY line");
	let _ = child.kill();
	let _ = child.wait();

	assert!(
		line.starts_with("READY 127.0.0.1:"),
		"unexpected READY line: {line:?}"
	);
}

#[test]
fn toron_daemon_prints_sql_ready_endpoint() {
	let dir = fresh_dir("toron-sql-ready");
	let path = dir.join("toron.toml");
	std::fs::write(
		&path,
		minimal_toron_config_with_sql("127.0.0.1:0", "127.0.0.1:0"),
	)
	.expect("write toron config");
	let mut child = nilket_command()
		.args(["toron", "start", path.to_str().expect("utf8 config path")])
		.stdout(Stdio::piped())
		.stderr(Stdio::piped())
		.spawn()
		.expect("spawn nilket toron start");
	let stdout = child.stdout.take().expect("child stdout");
	let mut reader = BufReader::new(stdout);
	let mut ready_line = String::new();
	let mut sql_ready_line = String::new();
	reader.read_line(&mut ready_line).expect("read READY line");
	reader
		.read_line(&mut sql_ready_line)
		.expect("read SQL_READY line");
	let _ = child.kill();
	let _ = child.wait();

	assert!(
		ready_line.starts_with("READY 127.0.0.1:"),
		"unexpected READY line: {ready_line:?}"
	);
	assert!(
		sql_ready_line.starts_with("SQL_READY 127.0.0.1:"),
		"unexpected SQL_READY line: {sql_ready_line:?}"
	);
}

#[test]
fn nilket_wendelon_sql_is_rejected_by_wendelon() {
	let output = nilket(&["wendelon", "sql", "SELECT 1"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	let stderr = stderr(&output);
	assert!(stderr.contains("nilket wendelon <path/to/wendelon.toml>"));
	assert!(!stderr.contains("unknown nilket command"), "{stderr}");
}

#[test]
fn nilket_wendelon_example_config_matches_existing_behavior() {
	fn key_bytes(text: &str) -> [u8; 32] {
		assert_eq!(
			text.len(),
			64,
			"signing material must encode exactly 32 bytes"
		);
		assert!(text.bytes().all(|byte| byte.is_ascii_hexdigit()));
		std::array::from_fn(|index| {
			u8::from_str_radix(&text[2 * index..2 * index + 2], 16).unwrap()
		})
	}
	fn validated_non_key_output(text: &str, previous: &mut Vec<[u8; 32]>) -> String {
		use wendelon::network::{
			audit_root_verifying_key_bytes, dev_audit_root_signing_secret,
			dev_lease_signing_secret, dev_transfer_signing_secret, lease_verifying_key_bytes,
			transfer_verifying_key_bytes,
		};
		let config: toml::Value = text.parse().expect("rendered config must be valid TOML");
		let node_id = u32::try_from(config["node"]["node_id"].as_integer().unwrap()).unwrap();
		let identity = &config["identity"];
		assert_eq!(identity["allow_dev_keys"].as_bool(), Some(false));
		let roles: [(&str, &str, fn([u8; 32]) -> [u8; 32]); 3] = [
			(
				"lease_signing_secret",
				"trusted_lease_signers",
				lease_verifying_key_bytes,
			),
			(
				"transfer_signing_secret",
				"trusted_transfer_signers",
				transfer_verifying_key_bytes,
			),
			(
				"audit_root_signing_secret",
				"trusted_audit_root_keys",
				audit_root_verifying_key_bytes,
			),
		];
		let known_dev = [
			dev_lease_signing_secret(node_id),
			dev_transfer_signing_secret(node_id),
			dev_audit_root_signing_secret(node_id),
		];
		let mut normalized = text.to_owned();
		for (secret_field, trusted_field, verifying_key) in roles {
			let secret_hex = identity[secret_field].as_str().unwrap();
			let secret = key_bytes(secret_hex);
			assert_ne!(secret, [0; 32]);
			assert!(!known_dev.contains(&secret), "rendered a public dev key");
			assert!(
				!previous.contains(&secret),
				"a role or invocation reused a secret"
			);
			previous.push(secret);
			let trusted = identity[trusted_field].as_table().unwrap();
			assert_eq!(trusted.len(), 1, "starter must enroll only its own node");
			let public_hex = trusted[&node_id.to_string()].as_str().unwrap();
			assert_eq!(key_bytes(public_hex), verifying_key(secret));
			// Keep exact delegation coverage for every other byte, including
			// comments/formatting. Mask only the six independently validated values.
			for (value, marker) in [(secret_hex, secret_field), (public_hex, trusted_field)] {
				let quoted = format!("\"{value}\"");
				assert_eq!(normalized.matches(&quoted).count(), 1);
				normalized = normalized.replace(&quoted, &format!("\"<{marker}>\""));
			}
		}
		normalized
	}

	let mut previous = Vec::new();
	let expected = validated_non_key_output(&render_example_config(), &mut previous);
	// Two actual CLI invocations must each provision independent keys; a
	// hard-coded wrapper config must not pass merely by differing from the API.
	for _ in 0..2 {
		let output = nilket(&["wendelon", "--print-example-config"]);
		assert!(output.status.success(), "{output:?}");
		assert!(stderr(&output).is_empty(), "{output:?}");
		assert_eq!(
			validated_non_key_output(&stdout(&output), &mut previous),
			expected
		);
	}
	assert_eq!(previous.len(), 9);
}

#[test]
fn nilket_wendelon_audit_verify_round_trips() {
	let dir = fresh_dir("audit");
	let good_path = dir.join("good.ndjson");
	let sink: Arc<dyn AuditSink> = Arc::new(LocalAuditSink::open(&good_path).unwrap());
	sink.emit(AuditEvent::new(
		AuditAuthority::NodeLocal,
		AuditOperation::CryptoStartup,
		node(41),
		"selected_cipher=aes256-gcm",
	))
	.unwrap();
	sink.emit(
		AuditEvent::new(
			AuditAuthority::RaftLog,
			AuditOperation::ControlRingEpochAdvanced,
			node(41),
			"advanced epoch",
		)
		.with_raft_log_index(401),
	)
	.unwrap();
	let signer = AuditRootSigner::from_seed([41; 32]);
	emit_signed_chain_root(&*sink, node(41), &signer).unwrap();

	let trust_path = dir.join("trust.json");
	std::fs::write(
		&trust_path,
		format!(
			r#"{{"version":1,"keys":{{"n-41":"{}"}}}}"#,
			signer.verifying_key_hex()
		),
	)
	.unwrap();
	let unanchored = nilket(&["wendelon", "audit", "verify", good_path.to_str().unwrap()]);
	assert_eq!(unanchored.status.code(), Some(1));
	assert!(stdout(&unanchored).contains("audit_root_unanchored"));
	assert!(stdout(&unanchored).contains("\"signed_roots_verified\": 0"));

	let good_output = nilket(&[
		"wendelon",
		"audit",
		"verify",
		good_path.to_str().expect("utf8 path"),
		"--trusted-audit-roots",
		trust_path.to_str().unwrap(),
	]);
	assert!(good_output.status.success(), "{good_output:?}");
	let good_stdout = stdout(&good_output);
	assert!(good_stdout.contains("\"signed_roots_verified\": 1"));
	assert!(good_stdout.contains("\"last_materialized_raft_log_index\": 401"));
	assert!(good_stdout.contains("\"issues\": []"));

	let bad_path = dir.join("bad.ndjson");
	std::fs::write(&bad_path, "{not-json\n").expect("write bad audit fixture");
	let bad_output = nilket(&[
		"wendelon",
		"audit",
		"verify",
		bad_path.to_str().expect("utf8 path"),
	]);
	assert_eq!(bad_output.status.code(), Some(1), "{bad_output:?}");
	assert!(stdout(&bad_output).contains("\"code\": \"malformed_json\""));
}

#[test]
#[ignore = "requires Linux io_uring and the fixed local-profile ports"]
fn local_profile_dogfood_contract_survives_restart_and_exposes_catalog() {
	let _serial = LIVE_LOCAL_LOCK.lock().expect("live-profile test lock");
	let harness = fresh_dir("local-profile-dogfood");
	let profile = harness.join("profile");
	let log = harness.join("local.stderr.log");
	let statements = dogfood_statements();
	assert_eq!(statements.len(), 10, "contract fixture shape changed");

	let first = LiveLocal::spawn(&profile, &log);
	toron_client::blocking_health_loopback(&first.ready.sql_endpoint.to_string())
		.expect("public Toron health probe");
	assert_sql_success(first.ready.sql_endpoint, &statements[0]);
	assert_eq!(
		assert_sql_success(first.ready.sql_endpoint, &statements[1]),
		"OK rows=3\n"
	);
	let first_session = first.ready.session_nonce;
	let profile_marker = std::fs::read(profile.join("profile.toml")).expect("profile marker");
	first.stop_gracefully();

	let second = LiveLocal::spawn(&profile, &log);
	assert_ne!(second.ready.session_nonce, first_session);
	assert_eq!(
		std::fs::read(profile.join("profile.toml")).expect("retained profile marker"),
		profile_marker,
		"restart must retain profile provenance"
	);
	let rows = assert_sql_success(second.ready.sql_endpoint, &statements[2]);
	for value in ["one", "two", "three", "(3 rows)"] {
		assert!(rows.contains(value), "missing {value:?} in {rows}");
	}
	let schemata = assert_sql_success(second.ready.sql_endpoint, &statements[3]);
	assert!(schemata.contains("default"), "{schemata}");
	for statement in &statements[4..6] {
		let catalog = assert_sql_success(second.ready.sql_endpoint, statement);
		assert!(catalog.contains("dogfood"), "{statement}: {catalog}");
	}
	for statement in &statements[6..] {
		assert_executed_or_typed_refusal(second.ready.sql_endpoint, statement);
	}
	second.stop_gracefully();
	std::fs::remove_dir_all(harness).expect("remove dogfood fixture");
}

#[test]
#[ignore = "requires Linux io_uring and the fixed local-profile ports"]
fn local_profile_parent_death_reaps_roles_and_allows_stale_manifest_restart() {
	let _serial = LIVE_LOCAL_LOCK.lock().expect("live-profile test lock");
	let harness = fresh_dir("local-profile-parent-death");
	let profile = harness.join("profile");
	let log = harness.join("local.stderr.log");
	let mut first = LiveLocal::spawn(&profile, &log);
	let role_pids = first.child_pids.clone();
	kill(Pid::from_raw(first.child.id() as i32), Signal::SIGKILL).expect("kill local supervisor");
	let status = wait_for_child(&mut first.child, Duration::from_secs(5));
	assert!(!status.success(), "SIGKILL fixture unexpectedly succeeded");
	for pid in role_pids {
		assert!(
			wait_for_process_exit(pid, Duration::from_secs(10)),
			"parent-death contract left role {pid} alive"
		);
	}
	drop(first);

	let restarted = LiveLocal::spawn(&profile, &log);
	toron_client::blocking_health_loopback(&restarted.ready.sql_endpoint.to_string())
		.expect("restart health after stale runtime manifest");
	restarted.stop_gracefully();
	std::fs::remove_dir_all(harness).expect("remove parent-death fixture");
}
