//! Terminal-head startup latency (crates/libaryon/plans/impl/startup-latency.md, section 4.2).
//!
//! Runs the real `baryon` binary under a 120x40 pty and measures M0-M4 (exec to the first
//! content-bearing paint observed) and M4-M5 (typed text echoed in the output).
//!
//! Both are output-stream proxies, not screen state: the harness strips escape sequences and looks
//! for markers, with no terminal emulator. M4 holds once the fixture's first and last visible lines
//! have both been drawn after the alternate screen was entered; they could come from different
//! redraws, and cursor and erase semantics are ignored. M5 holds once the typed marker appears in
//! output written after M4.
//!
//! The harness answers the terminal queries baryon's image pickers send at startup, as a terminal
//! would; without answers each picker waits out its 2 s timeout. Budgets are asserted only when
//! `BARYON_STARTUP_LATENCY_ENFORCE=1` (`ci/startup-latency-check.sh` sets it and runs this against
//! a release build).
#![cfg(target_os = "linux")]

use std::fmt::Write as _;
use std::fs::{self, File};
use std::io::{Read, Write};
use std::os::fd::{FromRawFd, OwnedFd};
use std::os::unix::process::CommandExt;
use std::path::{Path, PathBuf};
use std::process::{Child, Command, Stdio};
use std::sync::mpsc::{self, Receiver};
use std::thread;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

const SAMPLES: usize = 9;
const ROWS: u16 = 40;
const COLS: u16 = 120;
const DEADLINE: Duration = Duration::from_secs(30);
const TOP_MARKER: &str = "STARTUPTOPMARKER";
const BOTTOM_MARKER: &str = "STARTUPBOTTOMMARKER";
const TYPED_MARKER: &str = "zqstartupkey";
const ENTER_ALTERNATE_SCREEN: &[u8] = b"\x1b[?1049h";

/// Median budgets, about 3x the baseline medians measured on the development machine (debug: S1
/// M0-M4 28, S2 45, M4-M5 21 ms; release: M0-M4 21, M4-M5 17 ms).
/// `(scenario, milestone, debug ms, release ms)`.
/// Override one with `BARYON_STARTUP_PTY_BUDGET_<SCENARIO>_<M0M4|M4M5>_MS`.
const BUDGETS: &[(&str, &str, u64, u64)] = &[
	("S1", "M0-M4", 90, 65),
	("S1", "M4-M5", 65, 55),
	("S2", "M0-M4", 140, 65),
	("S2", "M4-M5", 65, 55),
];

/// Replies a terminal without sixel or kitty graphics sends to the queries baryon writes at
/// startup: cell size, primary device attributes, device status (which ends each image-picker
/// query) and the cursor position ratatui reads. Kitty's graphics query gets no reply.
const QUERY_REPLIES: &[(&[u8], &[u8])] = &[
	(b"\x1b[16t", b"\x1b[6;20;10t"),
	(b"\x1b[c", b"\x1b[?62;22c"),
	(b"\x1b[5n", b"\x1b[0n"),
	(b"\x1b[6n", b"\x1b[1;1R"),
];

struct Pty {
	master: File,
	output: Receiver<Vec<u8>>,
	child: Child,
	raw: Vec<u8>,
	scanned: usize,
}

impl Pty {
	fn spawn(command: &mut Command) -> Self {
		let mut master = -1;
		let mut slave = -1;
		let size = libc::winsize {
			ws_row: ROWS,
			ws_col: COLS,
			ws_xpixel: 0,
			ws_ypixel: 0,
		};
		// SAFETY: openpty writes two new descriptors into the out-parameters; null name and
		// termios pointers are permitted.
		let status = unsafe {
			libc::openpty(
				&mut master,
				&mut slave,
				std::ptr::null_mut(),
				std::ptr::null(),
				&size,
			)
		};
		assert_eq!(status, 0, "openpty: {}", std::io::Error::last_os_error());
		// SAFETY: both descriptors were just opened and are owned here.
		let (master, slave) =
			unsafe { (OwnedFd::from_raw_fd(master), OwnedFd::from_raw_fd(slave)) };
		let stdio = |fd: &OwnedFd| Stdio::from(fd.try_clone().expect("clone pty slave"));
		command
			.stdin(stdio(&slave))
			.stdout(stdio(&slave))
			.stderr(stdio(&slave));
		// SAFETY: setsid and ioctl are async-signal-safe; the child makes the pty its controlling
		// terminal, as a terminal emulator does.
		unsafe {
			command.pre_exec(|| {
				if libc::setsid() == -1 || libc::ioctl(0, libc::TIOCSCTTY as _, 0) == -1 {
					return Err(std::io::Error::last_os_error());
				}
				Ok(())
			});
		}
		let child = command.spawn().expect("spawn baryon");
		drop(slave);
		let master = File::from(master);
		let mut reader = master.try_clone().expect("clone pty master");
		let (tx, output) = mpsc::channel();
		thread::spawn(move || {
			let mut buffer = [0u8; 16 * 1024];
			// The read fails with EIO once the child has exited and closed the slave.
			while let Ok(read) = reader.read(&mut buffer) {
				if read == 0 || tx.send(buffer[..read].to_vec()).is_err() {
					break;
				}
			}
		});
		Self {
			master,
			output,
			child,
			raw: Vec::new(),
			scanned: 0,
		}
	}

	/// Read until `done` holds for the screen text drawn after the alternate screen was entered
	/// and after stream offset `from`, answering terminal queries as they arrive.
	fn wait_for(
		&mut self,
		scenario: &str,
		what: &str,
		until: Instant,
		from: usize,
		done: impl Fn(&str) -> bool,
	) {
		loop {
			if let Some(start) = find(&self.raw, ENTER_ALTERNATE_SCREEN)
				&& done(&screen_text(&self.raw[start.max(from)..]))
			{
				return;
			}
			let remaining = until.saturating_duration_since(Instant::now());
			let chunk = self.output.recv_timeout(remaining).unwrap_or_else(|error| {
				panic!(
					"{scenario}: no {what} within {DEADLINE:?} ({error}); last output: {:?}",
					String::from_utf8_lossy(&self.raw[self.raw.len().saturating_sub(400)..])
				)
			});
			self.raw.extend_from_slice(&chunk);
			self.answer_queries();
		}
	}

	/// Reply to each known query in stream order. A query cut off at the end of the stream waits
	/// for the next chunk.
	fn answer_queries(&mut self) {
		while let Some(offset) = self.raw[self.scanned..]
			.iter()
			.position(|byte| *byte == 0x1b)
		{
			let at = self.scanned + offset;
			let rest = &self.raw[at..];
			if let Some((query, reply)) = QUERY_REPLIES
				.iter()
				.find(|(query, _)| rest.starts_with(query))
			{
				self.master.write_all(reply).expect("answer terminal query");
				self.scanned = at + query.len();
			} else if QUERY_REPLIES
				.iter()
				.any(|(query, _)| query.starts_with(rest))
			{
				return;
			} else {
				self.scanned = at + 1;
			}
		}
		self.scanned = self.raw.len();
	}

	fn type_text(&mut self, text: &str) {
		self.master
			.write_all(text.as_bytes())
			.expect("type into pty");
	}
}

impl Drop for Pty {
	fn drop(&mut self) {
		let _ = self.child.kill();
		let _ = self.child.wait();
	}
}

fn find(haystack: &[u8], needle: &[u8]) -> Option<usize> {
	haystack
		.windows(needle.len())
		.position(|window| window == needle)
}

/// The printable text of a terminal stream with escape sequences removed. Cells ratatui draws
/// consecutively stay contiguous, so a marker drawn on one row appears whole. This recognizes
/// emitted bytes; it does not model the screen.
fn screen_text(stream: &[u8]) -> String {
	let text = String::from_utf8_lossy(stream);
	let mut out = String::with_capacity(text.len());
	let mut chars = text.chars().peekable();
	while let Some(ch) = chars.next() {
		if ch != '\x1b' {
			if !ch.is_control() {
				out.push(ch);
			}
			continue;
		}
		match chars.next() {
			// CSI: parameters, then one final byte in @..~.
			Some('[') => {
				for ch in chars.by_ref() {
					if ('@'..='~').contains(&ch) {
						break;
					}
				}
			}
			// OSC, DCS, APC, PM, SOS: until BEL or ST (ESC \).
			Some(']' | 'P' | '_' | '^' | 'X') => {
				while let Some(ch) = chars.next() {
					if ch == '\x07' || (ch == '\x1b' && chars.next_if_eq(&'\\').is_some()) {
						break;
					}
				}
			}
			_ => {}
		}
	}
	out
}

struct Scratch {
	root: PathBuf,
}

impl Scratch {
	fn new() -> Self {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("clock after epoch")
			.as_nanos();
		let root =
			std::env::temp_dir().join(format!("baryon-startup-pty-{}-{nonce}", std::process::id()));
		fs::create_dir_all(&root).expect("create scratch root");
		Self { root }
	}
}

impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::remove_dir_all(&self.root);
	}
}

/// Thirty short lines with markers on the first and last, so both fit a 40-row screen.
fn fixture(path: &Path) {
	let mut source = format!("// {TOP_MARKER}\n");
	for index in 1..29 {
		writeln!(source, "let value_{index} = {index};").expect("format fixture");
	}
	writeln!(source, "// {BOTTOM_MARKER}").expect("format fixture");
	fs::write(path, source).expect("write fixture");
}

fn production_config(root: &Path) {
	let config = root.join("config/baryon");
	fs::create_dir_all(&config).expect("create config directory");
	let mut script = String::from("pub fn main(editor) {\n");
	for index in 0..16 {
		writeln!(
			script,
			"    baryon::command(editor, \"startup{index}\", |editor, args, bang| {{ \"ok\" }});"
		)
		.expect("format command");
	}
	script.push_str("    baryon::map(editor, \"normal\", \"Q\", |editor| { \"mapped\" });\n}\n");
	fs::write(config.join("init.rn"), script).expect("write init.rn");
	let state = root.join("state/baryon");
	fs::create_dir_all(&state).expect("create state directory");
	let history = (0..1_000)
		.map(|index| format!("command-{index:04}\n"))
		.collect::<String>();
	fs::write(state.join("command_history"), history).expect("seed command history");
}

fn baryon(root: &Path, file: &Path) -> Command {
	let mut command = Command::new(env!("CARGO_BIN_EXE_baryon"));
	command
		.arg(file)
		.current_dir(root)
		.env_clear()
		.env("PATH", std::env::var_os("PATH").unwrap_or_default())
		.env("TERM", "xterm-256color")
		.env("LANG", "C.UTF-8")
		.env("HOME", root.join("home"))
		.env("XDG_CONFIG_HOME", root.join("config"))
		.env("XDG_STATE_HOME", root.join("state"))
		.env("XDG_CACHE_HOME", root.join("cache"));
	command
}

struct Sample {
	first_paint: Duration,
	first_input: Duration,
}

fn sample(scenario: &str, root: &Path, file: &Path) -> Sample {
	let _ = fs::remove_dir_all(root.join("cache"));
	let exec = Instant::now();
	let mut pty = Pty::spawn(&mut baryon(root, file));
	let until = exec + DEADLINE;
	pty.wait_for(
		scenario,
		"first content-bearing paint",
		until,
		0,
		|screen| screen.contains(TOP_MARKER) && screen.contains(BOTTOM_MARKER),
	);
	let painted = Instant::now();
	let drawn = pty.raw.len();
	pty.type_text(&format!(":{TYPED_MARKER}"));
	pty.wait_for(scenario, "typed text echoed", until, drawn, |screen| {
		screen.contains(TYPED_MARKER)
	});
	Sample {
		first_paint: painted - exec,
		first_input: painted.elapsed(),
	}
}

fn stats(samples: &mut [Duration]) -> (Duration, Duration, Duration) {
	samples.sort_unstable();
	let p90 = ((samples.len() * 9).div_ceil(10)).saturating_sub(1);
	(samples[0], samples[samples.len() / 2], samples[p90])
}

fn ms(duration: Duration) -> f64 {
	duration.as_secs_f64() * 1e3
}

fn budget(scenario: &str, milestone: &str) -> Option<Duration> {
	let &(_, _, debug, release) = BUDGETS
		.iter()
		.find(|(name, step, _, _)| *name == scenario && *step == milestone)?;
	let variable = format!(
		"BARYON_STARTUP_PTY_BUDGET_{scenario}_{}_MS",
		milestone.replace('-', "")
	);
	let millis = match std::env::var(&variable) {
		Ok(value) => value
			.parse()
			.unwrap_or_else(|_| panic!("{variable}={value} is not a number")),
		Err(_) if cfg!(debug_assertions) => debug,
		Err(_) => release,
	};
	Some(Duration::from_millis(millis))
}

#[test]
fn terminal_startup_latency_stays_within_budget() {
	let enforced = std::env::var("BARYON_STARTUP_LATENCY_ENFORCE").as_deref() == Ok("1");
	let profile = if cfg!(debug_assertions) {
		"debug"
	} else {
		"release"
	};
	// libtest prints `test <name> ... ` without a newline; keep each JSON record on its own line.
	println!();
	let mut failures = Vec::new();
	for scenario in ["S1", "S2"] {
		let scratch = Scratch::new();
		let file = scratch.root.join("fixture.rs");
		fixture(&file);
		if scenario == "S2" {
			production_config(&scratch.root);
		}
		let mut paint = Vec::new();
		let mut input = Vec::new();
		for _ in 0..SAMPLES {
			let sample = sample(scenario, &scratch.root, &file);
			paint.push(sample.first_paint);
			input.push(sample.first_input);
		}
		for (milestone, samples) in [("M0-M4", &mut paint), ("M4-M5", &mut input)] {
			let values = samples
				.iter()
				.map(|sample| format!("{:.3}", ms(*sample)))
				.collect::<Vec<_>>()
				.join(",");
			let (min, median, p90) = stats(samples);
			println!(
				"{{\"suite\":\"baryon-startup-pty\",\"scenario\":\"{scenario}\",\"milestone\":\"{milestone}\",\
				 \"samples_ms\":[{values}],\"min_ms\":{:.3},\"median_ms\":{:.3},\"p90_ms\":{:.3},\
				 \"profile\":\"{profile}\",\"enforced\":{enforced}}}",
				ms(min),
				ms(median),
				ms(p90)
			);
			if let Some(budget) = budget(scenario, milestone)
				&& median > budget
			{
				failures.push(format!(
					"{scenario} median {milestone} {median:?} exceeded {budget:?} ({profile})"
				));
			}
		}
	}
	if !enforced {
		for failure in &failures {
			println!("not enforced: {failure}");
		}
		println!("terminal startup budgets NOT enforced: set BARYON_STARTUP_LATENCY_ENFORCE=1");
		return;
	}
	assert!(
		failures.is_empty(),
		"terminal startup regressions:\n{}",
		failures.join("\n")
	);
}
