//! Engine startup latency (crates/libaryon/plans/impl/startup-latency.md).
//!
//! Per scenario it records `EngineHandle::spawn` (to M2, spawn returned), M2-M2r (`Resize`
//! submitted), M2r-M3 (resize submitted to the first usable viewport, the asserted span) and M3-M3t
//! (first viewport with document text). Budgets are asserted only in a quiet lane:
//! `cargo-ctrl`'s controlled lane (`CARGO_CTRL_LANE=controlled`) or an explicit
//! `BARYON_STARTUP_LATENCY_ENFORCE=1`. Elsewhere every number is still measured and printed, and
//! the run says it did not enforce.

use libaryon::core::SessionStore;
use libaryon::engine::settings::EngineSettings;
use libaryon::engine::viewport_channel::ViewportReceiver;
use libaryon::engine::{EditorCommand, EngineConfig, EngineHandle};
use libaryon::uast::Viewport;
use std::fmt::Write as _;
use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SAMPLES: usize = 9;
const WIDTH: u16 = 100;
const HEIGHT: u16 = 30;
const DEADLINE: Duration = Duration::from_secs(30);
const GIB: u64 = 1 << 30;

/// Median M2r-M3 budgets: about 3x the baseline medians measured on the development machine, and
/// never under 10 ms, where scheduler noise dominates. Tighten after controlled-run history.
/// `(scenario, debug ms, release ms)`; override one with `BARYON_STARTUP_BUDGET_<SCENARIO>_MS`.
const READY_BUDGETS: &[(&str, u64, u64)] = &[
	("S0", 25, 10),
	("S1", 55, 15),
	("S2", 40, 20),
	("S4", 55, 30),
	("S5", 30, 10),
];

/// S2 (production-like config) may cost at most this many times S0 (the floor), `(debug, release)`.
/// Measured ratios: 1.8 debug, 4.1 release, where S0 is a millisecond and noise dominates.
/// Override with `BARYON_STARTUP_S2_FACTOR`.
const S2_OVER_S0_FACTOR: (f64, f64) = (5.0, 12.0);

/// Median `EngineHandle::spawn` budget for every scenario, `(debug ms, release ms)`. Saved-session
/// restore (S5) runs inside spawn: 5.8 ms debug and 1.0 ms release, against 0.1-0.6 ms elsewhere.
/// Override with `BARYON_STARTUP_SPAWN_BUDGET_MS`.
const SPAWN_BUDGET: (u64, u64) = (25, 10);

/// One startup, split at each milestone so a regression names its phase.
struct Sample {
	/// `EngineHandle::spawn` until it returned (M2).
	spawn: Duration,
	/// M2 until `Resize` was submitted (M2r).
	submit: Duration,
	/// M2r until the first usable viewport (M3). Asserted.
	ready: Duration,
	/// M3 until the first viewport with document text (M3t), for file scenarios.
	text: Option<Duration>,
}

struct Report {
	scenario: String,
	spawn: Vec<Duration>,
	submit: Vec<Duration>,
	ready: Vec<Duration>,
	text: Vec<Duration>,
}

impl Report {
	fn new(scenario: &str) -> Self {
		Self {
			scenario: scenario.to_string(),
			spawn: Vec::new(),
			submit: Vec::new(),
			ready: Vec::new(),
			text: Vec::new(),
		}
	}

	fn push(&mut self, sample: Sample) {
		self.spawn.push(sample.spawn);
		self.submit.push(sample.submit);
		self.ready.push(sample.ready);
		self.text.extend(sample.text);
	}

	fn ready_median(&self) -> Duration {
		Stats::of(&self.ready).median
	}
}

struct Stats {
	min: Duration,
	median: Duration,
	p90: Duration,
}

impl Stats {
	fn of(samples: &[Duration]) -> Self {
		let mut sorted = samples.to_vec();
		sorted.sort_unstable();
		let p90 = ((sorted.len() * 9).div_ceil(10)).saturating_sub(1);
		Self {
			min: sorted[0],
			median: sorted[sorted.len() / 2],
			p90: sorted[p90],
		}
	}
}

struct Run {
	root: PathBuf,
	enforced: bool,
	profile: &'static str,
	git: String,
	failures: Vec<String>,
}

impl Run {
	fn new() -> Self {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("clock after epoch")
			.as_nanos();
		let root = std::env::temp_dir().join(format!(
			"baryon-startup-latency-{}-{nonce}",
			std::process::id()
		));
		fs::create_dir_all(&root).expect("create scratch root");
		Self {
			root,
			enforced: std::env::var("CARGO_CTRL_LANE").as_deref() == Ok("controlled")
				|| std::env::var("BARYON_STARTUP_LATENCY_ENFORCE").as_deref() == Ok("1"),
			profile: if cfg!(debug_assertions) {
				"debug"
			} else {
				"release"
			},
			git: git_describe(),
			failures: Vec::new(),
		}
	}

	/// Remove the line-index sidecars and the engine state between samples.
	fn reset_caches(&self) {
		let _ = fs::remove_dir_all(self.root.join("cache"));
	}

	fn emit(&self, report: &Report) {
		for (milestone, samples) in [
			("spawn", &report.spawn),
			("M2-M2r", &report.submit),
			("M2r-M3", &report.ready),
			("M3-M3t", &report.text),
		] {
			if samples.is_empty() {
				continue;
			}
			let stats = Stats::of(samples);
			let mut values = String::new();
			for (index, sample) in samples.iter().enumerate() {
				let separator = if index == 0 { "" } else { "," };
				write!(values, "{separator}{:.3}", ms(*sample)).expect("format sample");
			}
			println!(
				"{{\"suite\":\"libaryon-startup\",\"scenario\":\"{}\",\"milestone\":\"{milestone}\",\
				 \"samples_ms\":[{values}],\"min_ms\":{:.3},\"median_ms\":{:.3},\"p90_ms\":{:.3},\
				 \"git\":\"{}\",\"profile\":\"{}\",\"enforced\":{}}}",
				report.scenario,
				ms(stats.min),
				ms(stats.median),
				ms(stats.p90),
				self.git,
				self.profile,
				self.enforced
			);
		}
	}

	fn check_budget(&mut self, report: &Report) {
		let spawn_budget = Duration::from_millis(
			env_number("BARYON_STARTUP_SPAWN_BUDGET_MS").unwrap_or(if cfg!(debug_assertions) {
				SPAWN_BUDGET.0
			} else {
				SPAWN_BUDGET.1
			}),
		);
		let spawn = Stats::of(&report.spawn).median;
		if spawn > spawn_budget {
			self.failures.push(format!(
				"{} median spawn {spawn:?} exceeded {spawn_budget:?} ({}; override with \
				 BARYON_STARTUP_SPAWN_BUDGET_MS)",
				report.scenario, self.profile
			));
		}
		let Some(&(_, debug, release)) = READY_BUDGETS
			.iter()
			.find(|(scenario, _, _)| *scenario == report.scenario)
		else {
			return;
		};
		let variable = format!("BARYON_STARTUP_BUDGET_{}_MS", report.scenario);
		let budget =
			Duration::from_millis(env_number(&variable).unwrap_or(if cfg!(debug_assertions) {
				debug
			} else {
				release
			}));
		let median = report.ready_median();
		if median > budget {
			self.failures.push(format!(
				"{} median M2r-M3 {median:?} exceeded {budget:?} ({}; override with {variable})",
				report.scenario, self.profile
			));
		}
	}

	fn check_ratio(&mut self, floor: &Report, configured: &Report) {
		let factor = env_number("BARYON_STARTUP_S2_FACTOR").unwrap_or(if cfg!(debug_assertions) {
			S2_OVER_S0_FACTOR.0
		} else {
			S2_OVER_S0_FACTOR.1
		});
		let floor = floor.ready_median();
		let configured = configured.ready_median();
		if configured.as_secs_f64() > floor.as_secs_f64() * factor {
			self.failures.push(format!(
				"S2 median M2r-M3 {configured:?} is more than {factor}x S0 {floor:?} \
				 (override with BARYON_STARTUP_S2_FACTOR)"
			));
		}
	}
}

impl Drop for Run {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.root);
	}
}

fn ms(duration: Duration) -> f64 {
	duration.as_secs_f64() * 1e3
}

fn env_number<T: std::str::FromStr>(variable: &str) -> Option<T> {
	let value = std::env::var(variable).ok()?;
	Some(
		value
			.parse()
			.unwrap_or_else(|_| panic!("{variable}={value} is not a number")),
	)
}

fn git_describe() -> String {
	Command::new("git")
		.args(["describe", "--always", "--dirty"])
		.current_dir(env!("CARGO_MANIFEST_DIR"))
		.output()
		.ok()
		.filter(|output| output.status.success())
		.and_then(|output| String::from_utf8(output.stdout).ok())
		.map(|describe| describe.trim().to_string())
		.unwrap_or_else(|| "unknown".to_string())
}

fn usable(viewport: &Viewport) -> bool {
	viewport.focused_pane().is_some()
}

fn has_text(viewport: &Viewport) -> bool {
	viewport
		.focused_pane()
		.is_some_and(|pane| pane.tokens.iter().any(|token| !token.text.is_empty()))
}

fn receive(viewports: &ViewportReceiver, until: Instant, scenario: &str, what: &str) -> Viewport {
	viewports
		.recv_timeout(until.saturating_duration_since(Instant::now()))
		.unwrap_or_else(|error| panic!("{scenario}: no {what} within {DEADLINE:?} ({error})"))
}

/// Spawn, submit the head's first `Resize`, and time the first usable viewport and (when
/// `wants_text`) the first viewport carrying document text.
fn sample(scenario: &str, spawn: impl FnOnce() -> EngineHandle, wants_text: bool) -> Sample {
	let started = Instant::now();
	let mut engine = spawn();
	let spawned = Instant::now();
	let commands = engine.command_sender();
	let viewports = engine.take_viewports().expect("viewport receiver");
	commands
		.send(EditorCommand::Resize(WIDTH, HEIGHT))
		.expect("submit resize");
	let submitted = Instant::now();
	let until = started + DEADLINE;
	let first = receive(&viewports, until, scenario, "viewport");
	let usable_at = Instant::now();
	// baryon sets no initial_viewport, so a frame rendered before the resize has the 80-column
	// fallback geometry. Refuse it: such a frame would make M3 init-driven, not resize-driven.
	let width = first
		.focused_pane()
		.map(|pane| pane.rect.width.get())
		.unwrap_or_default();
	assert!(
		usable(&first) && width == u32::from(WIDTH),
		"{scenario}: first viewport is not the resize projection (focused pane width {width}); \
		 startup scripts in this test must not request a render"
	);
	// The startup scripts print nothing, so any status here is an init or restore failure: a
	// script that failed to compile would otherwise look like a fast startup.
	assert_eq!(
		first.status_message, None,
		"{scenario}: startup reported a status"
	);
	let mut text = (wants_text && has_text(&first)).then_some(Duration::ZERO);
	while wants_text && text.is_none() {
		let viewport = receive(&viewports, until, scenario, "viewport with document text");
		if has_text(&viewport) {
			text = Some(usable_at.elapsed());
		}
	}
	engine.shutdown().expect("join engine");
	Sample {
		spawn: spawned - started,
		submit: submitted - spawned,
		ready: usable_at - submitted,
		text,
	}
}

fn quiet_config() -> EngineConfig {
	EngineConfig {
		persistent_history: false,
		..EngineConfig::default()
	}
}

fn rust_fixture(path: &Path) {
	let mut source = String::new();
	let mut index = 0;
	while source.len() < 10 * 1024 {
		writeln!(
			source,
			"/// Adds {index} to a running total.\npub fn add_{index}(total: u64) -> u64 {{\n\
			 \tlet step = {index}u64;\n\ttotal.wrapping_add(step)\n}}\n"
		)
		.expect("format fixture");
		index += 1;
	}
	fs::write(path, source).expect("write Rust fixture");
}

/// A user init script: commands, mappings and options, none of which request a render.
fn init_script(path: &Path, helpers: usize) {
	let mut script = String::new();
	for index in 0..helpers {
		writeln!(
			script,
			"fn helper_{index}(value) {{\n    let doubled = value * 2;\n    \
			 let shifted = doubled + {index};\n    shifted\n}}\n"
		)
		.expect("format helper");
	}
	script.push_str("pub fn main(editor) {\n");
	for index in 0..16 {
		writeln!(
			script,
			"    baryon::command(editor, \"startup{index}\", |editor, args, bang| {{ \"ok\" }});"
		)
		.expect("format command");
	}
	for key in ["Q", "K", "J", "H", "L", "Y", "Z", "U"] {
		writeln!(
			script,
			"    baryon::map(editor, \"normal\", \"{key}\", |editor| {{ \"mapped\" }});"
		)
		.expect("format mapping");
	}
	script.push_str(
		"    baryon::option(editor, \"startupoption\", \"string\", |editor, call| { \"value\" });\n}\n",
	);
	fs::write(path, script).expect("write init script");
}

fn seed_history(state_home: &Path) {
	// macOS roots engine state under HOME, not XDG_STATE_HOME.
	let history_dir = if cfg!(target_os = "macos") {
		state_home.join("Library/Application Support/baryon/state")
	} else {
		state_home.join("baryon")
	};
	fs::create_dir_all(&history_dir).expect("create state directory");
	let history = (0..1_000)
		.map(|index| format!("command-{index:04}\n"))
		.collect::<String>();
	fs::write(history_dir.join("command_history"), history).expect("seed command history");
}

/// A sparse file: `HEAD` at offset 0 and `TAILMARK` at the end, as `ci/huge-file-check.sh` builds.
/// Returns false when the filesystem did not preserve sparsity.
fn sparse_fixture(path: &Path, bytes: u64) -> bool {
	use std::io::{Seek, SeekFrom, Write};
	let mut file = fs::File::create(path).expect("create sparse fixture");
	file.set_len(bytes).expect("size sparse fixture");
	file.write_all(b"HEAD").expect("write head marker");
	file.seek(SeekFrom::Start(bytes - 8)).expect("seek tail");
	file.write_all(b"TAILMARK").expect("write tail marker");
	file.sync_all().expect("sync sparse fixture");
	allocated_bytes(path) <= 16 * 1024 * 1024
}

#[cfg(unix)]
fn allocated_bytes(path: &Path) -> u64 {
	use std::os::unix::fs::MetadataExt;
	fs::metadata(path).expect("stat sparse fixture").blocks() * 512
}

#[cfg(not(unix))]
fn allocated_bytes(_path: &Path) -> u64 {
	u64::MAX
}

fn measure(
	run: &Run,
	scenario: &str,
	wants_text: bool,
	mut spawn: impl FnMut() -> EngineHandle,
) -> Report {
	let mut report = Report::new(scenario);
	for _ in 0..SAMPLES {
		run.reset_caches();
		report.push(sample(scenario, &mut spawn, wants_text));
	}
	run.emit(&report);
	report
}

/// S3 characterization: sizes are sampled interleaved so they share cache conditions. Reported,
/// never asserted.
fn characterize_sparse(run: &Run) {
	let mut sizes = vec![
		("S3-1MiB", 1 << 20),
		("S3-1GiB", GIB),
		("S3-16GiB", 16 * GIB),
	];
	if std::env::var("BARYON_STARTUP_S3_64GIB").as_deref() == Ok("1") {
		sizes.push(("S3-64GiB", 64 * GIB));
	}
	let mut fixtures = Vec::new();
	for (scenario, bytes) in sizes {
		let path = run.root.join(format!("{scenario}.bin"));
		if !sparse_fixture(&path, bytes) {
			println!("{scenario}: skipped, the filesystem did not preserve sparsity");
			let _ = fs::remove_file(&path);
			continue;
		}
		fixtures.push((Report::new(scenario), path, bytes));
	}
	for _ in 0..SAMPLES {
		for (report, path, _) in &mut fixtures {
			run.reset_caches();
			let scenario = report.scenario.clone();
			let file = path.to_string_lossy().into_owned();
			report.push(sample(
				&scenario,
				|| {
					EngineHandle::spawn(EngineConfig {
						initial_file: Some(file),
						..quiet_config()
					})
				},
				true,
			));
		}
	}
	for (report, _, _) in &fixtures {
		run.emit(report);
	}
	let medians: Vec<(u64, Duration)> = fixtures
		.iter()
		.map(|(report, _, bytes)| (*bytes, report.ready_median()))
		.collect();
	if let (Some(&(small, small_ms)), Some(&(large, large_ms))) = (medians.first(), medians.last())
		&& large > small
	{
		let slope = (ms(large_ms) - ms(small_ms)) / ((large - small) as f64 / GIB as f64);
		println!("S3: M2r-M3 slope {slope:.3} ms per GiB (reported, not asserted)");
	}
	for (_, path, _) in fixtures {
		let _ = fs::remove_file(path);
	}
}

#[cfg(unix)]
fn saved_session() -> libaryon::core::SessionId {
	let store = SessionStore::platform_default().expect("session store");
	let saved = store.create().expect("create saved session");
	let id = saved.snapshot().session_id();
	let mut engine = EngineHandle::spawn_saved(quiet_config(), saved).expect("start saved engine");
	let commands = engine.command_sender();
	let viewports = engine.take_viewports().expect("viewport receiver");
	commands
		.send(EditorCommand::Resize(WIDTH, HEIGHT))
		.expect("resize");
	let text = "saved session line\n".repeat(512);
	commands
		.send(EditorCommand::InsertText(text))
		.expect("insert saved text");
	let until = Instant::now() + DEADLINE;
	while !has_text(&receive(
		&viewports,
		until,
		"S5 setup",
		"viewport with text",
	)) {}
	engine.checkpoint().expect("checkpoint saved session");
	engine.shutdown().expect("stop saving engine");
	id
}

#[test]
fn engine_startup_latency_stays_within_budget() {
	// libtest prints `test <name> ... ` without a newline; keep each JSON record on its own line.
	println!();
	let mut run = Run::new();
	let state_home = run.root.join("state");
	let config_home = run.root.join("config");
	// This integration-test process owns the environment for its sole test, so no concurrently
	// running Rust code can observe the swap.
	unsafe {
		std::env::set_var("HOME", &state_home);
		std::env::set_var("XDG_STATE_HOME", &state_home);
		std::env::set_var("XDG_CONFIG_HOME", &config_home);
		std::env::set_var("XDG_CACHE_HOME", run.root.join("cache"));
		std::env::remove_var("DISPLAY");
		std::env::remove_var("WAYLAND_DISPLAY");
	}

	let floor = measure(&run, "S0", false, || EngineHandle::spawn(quiet_config()));

	let rust = run.root.join("fixture.rs");
	rust_fixture(&rust);
	let rust_file = rust.to_string_lossy().into_owned();
	let small_file = measure(&run, "S1", true, || {
		EngineHandle::spawn(EngineConfig {
			initial_file: Some(rust_file.clone()),
			..quiet_config()
		})
	});

	seed_history(&state_home);
	let production_init = run.root.join("production.rn");
	init_script(&production_init, 40);
	let configured = measure(&run, "S2", false, || {
		EngineHandle::spawn(EngineConfig {
			init_script: Some(production_init.clone()),
			persistent_history: true,
			..EngineConfig::from(EngineSettings::default())
		})
	});

	characterize_sparse(&run);

	let large_init = run.root.join("large.rn");
	init_script(&large_init, 400);
	let large_script = measure(&run, "S4", false, || {
		EngineHandle::spawn(EngineConfig {
			init_script: Some(large_init.clone()),
			..quiet_config()
		})
	});

	let mut reports = vec![floor, small_file, configured, large_script];
	#[cfg(unix)]
	{
		let id = saved_session();
		let store = SessionStore::platform_default().expect("session store");
		reports.push(measure(&run, "S5", true, || {
			let saved = store.open_writable(id).expect("reopen saved session");
			EngineHandle::spawn_saved(quiet_config(), saved).expect("restore saved engine")
		}));
	}

	for report in &reports {
		run.check_budget(report);
	}
	run.check_ratio(&reports[0], &reports[2]);
	let failures = std::mem::take(&mut run.failures);
	if !run.enforced {
		for failure in &failures {
			println!("not enforced: {failure}");
		}
		println!(
			"startup latency budgets NOT enforced: run in cargo-ctrl's controlled lane or set \
			 BARYON_STARTUP_LATENCY_ENFORCE=1"
		);
		return;
	}
	assert!(
		failures.is_empty(),
		"startup latency regressions:\n{}",
		failures.join("\n")
	);
}
