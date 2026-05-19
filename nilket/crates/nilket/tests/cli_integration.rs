use std::io::{BufRead, BufReader};
use std::path::PathBuf;
use std::process::{Command, Output, Stdio};
use std::sync::Arc;
use std::time::{SystemTime, UNIX_EPOCH};

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

fn node(id: u32) -> NodeId {
	NodeId { id, _reserved: 0 }
}

#[test]
fn nilket_no_args_exits_nonzero_with_usage() {
	let output = nilket(&[]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	let stderr = stderr(&output);
	assert!(stderr.contains("wendelon"), "{stderr}");
	assert!(stderr.contains("toron"), "{stderr}");
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
fn nilket_root_sql_is_rejected() {
	let output = nilket(&["sql", "SELECT 1"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	let stderr = stderr(&output);
	assert!(stderr.contains("unknown nilket command: sql"), "{stderr}");
	assert!(stderr.contains("wendelon"), "{stderr}");
	assert!(stderr.contains("toron"), "{stderr}");
}

#[test]
fn nilket_toron_sql_reaches_toron_branch() {
	let output = nilket(&["toron", "sql", "SELECT 1"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	assert!(stdout(&output).is_empty(), "{output:?}");
	assert_eq!(
		stderr(&output),
		"toron sql is reserved; production execution is not wired in ADR 040\n"
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
			"toron worker listener requires authenticated transport for non-loopback bind address"
		),
		"{output:?}"
	);
}

#[test]
fn nilket_toron_sql_and_inspect_still_reserved() {
	for (args, stderr_expected) in [
		(
			&["toron", "sql", "SELECT 1"][..],
			"toron sql is reserved; production execution is not wired in ADR 040\n",
		),
		(
			&["toron", "inspect", "workers"][..],
			"toron inspect is reserved; production execution is not wired in ADR 040\n",
		),
	] {
		let output = nilket(args);
		assert_eq!(output.status.code(), Some(2), "{output:?}");
		assert!(stdout(&output).is_empty(), "{output:?}");
		assert_eq!(stderr(&output), stderr_expected);
	}
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
fn nilket_wendelon_sql_is_rejected_by_wendelon() {
	let output = nilket(&["wendelon", "sql", "SELECT 1"]);

	assert_eq!(output.status.code(), Some(2), "{output:?}");
	let stderr = stderr(&output);
	assert!(stderr.contains("nilket wendelon <path/to/wendelon.toml>"));
	assert!(!stderr.contains("unknown nilket command"), "{stderr}");
}

#[test]
fn nilket_wendelon_example_config_matches_existing_behavior() {
	let output = nilket(&["wendelon", "--print-example-config"]);

	assert!(output.status.success(), "{output:?}");
	assert_eq!(stdout(&output), render_example_config());
	assert!(stderr(&output).is_empty(), "{output:?}");
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

	let good_output = nilket(&[
		"wendelon",
		"audit",
		"verify",
		good_path.to_str().expect("utf8 path"),
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
