//! T-TTY over a pseudo-terminal: the line is read with no echo, and the terminal settings are
//! identical afterwards on success, error, interruption and panic.
use crate::custody::tty::{EchoOff, MAX_SECRET_LEN, Terminal, is_quiet};
use crate::custody::{CustodyError, sys};
use std::fs::File;
use std::io::{Read, Write};
use std::time::{Duration, Instant};

fn same(a: &libc::termios, b: &libc::termios) -> bool {
	a.c_iflag == b.c_iflag
		&& a.c_oflag == b.c_oflag
		&& a.c_cflag == b.c_cflag
		&& a.c_lflag == b.c_lflag
		&& a.c_cc == b.c_cc
}

/// Waits (bounded) until the slave's echo is off, so input written afterwards is not echoed or
/// flushed by the mode change.
fn wait_for_echo_off(slave: &File) {
	let deadline = Instant::now() + Duration::from_secs(10);
	while sys::tcgetattr(slave).unwrap().c_lflag & libc::ECHO != 0 {
		assert!(Instant::now() < deadline, "echo was never turned off");
		std::thread::sleep(Duration::from_millis(5));
	}
}

/// Reads from the master until `needle` has been seen (bounded by the pty staying open).
fn read_until(master: &File, needle: &[u8]) -> Vec<u8> {
	let mut seen = Vec::new();
	let mut chunk = [0_u8; 256];
	while !seen.windows(needle.len()).any(|w| w == needle) {
		let n = (&*master).read(&mut chunk).unwrap();
		assert!(n > 0, "the pty closed before {needle:?}");
		seen.extend_from_slice(&chunk[..n]);
	}
	seen
}

/// Runs `read_secret` on the slave while the master types `input` once echo is off.
fn entry(input: &[u8], max: usize) -> (Result<Vec<u8>, CustodyError>, Vec<u8>, bool) {
	let (master, slave) = sys::open_pty().unwrap();
	let before = sys::tcgetattr(&slave).unwrap();
	let terminal = Terminal::from_file(slave.try_clone().unwrap()).unwrap();
	let reader = std::thread::spawn(move || {
		terminal
			.read_secret("Passphrase: ", max)
			.map(|secret| secret.as_slice().to_vec())
	});
	wait_for_echo_off(&slave);
	(&master).write_all(input).unwrap();
	let result = reader.join().unwrap();
	let shown = read_until(&master, b"Passphrase: ");
	let shown = if shown.ends_with(b"\n") {
		shown
	} else {
		[shown, read_until(&master, b"\n")].concat()
	};
	let restored = same(&before, &sys::tcgetattr(&slave).unwrap());
	(result, shown, restored)
}

const SECRET: &[u8] = b"correct horse battery staple";

#[test]
fn a_line_is_read_without_echo_and_the_terminal_is_restored() {
	let (result, shown, restored) = entry(b"correct horse battery staple\r", 64);
	assert_eq!(result.unwrap(), SECRET);
	assert!(
		!shown.windows(SECRET.len()).any(|w| w == SECRET),
		"echoed: {}",
		String::from_utf8_lossy(&shown)
	);
	assert!(restored);
}

#[test]
fn with_echo_on_the_master_sees_the_typed_bytes() {
	// Control for the echo probe above: the same pty with its default settings echoes.
	let (master, slave) = sys::open_pty().unwrap();
	assert_ne!(sys::tcgetattr(&slave).unwrap().c_lflag & libc::ECHO, 0);
	(&master)
		.write_all(b"correct horse battery staple\r")
		.unwrap();
	let shown = read_until(&master, SECRET);
	assert!(shown.windows(SECRET.len()).any(|w| w == SECRET));
}

#[test]
fn erase_and_kill_edit_the_line() {
	let (result, _, restored) = entry(b"wrong\x15ab\xc3\xa9X\x7f\x7fc\n", 64);
	assert_eq!(result.unwrap(), b"abc");
	assert!(restored);
}

#[test]
fn the_interrupt_character_ends_entry_after_restoring_the_terminal() {
	for abort in [b'\x03', b'\x1c', b'\x1a'] {
		let (result, _, restored) = entry(&[b'a', b'b', abort], 64);
		assert!(
			matches!(result, Err(CustodyError::Interrupted)),
			"{abort}: {result:?}"
		);
		assert!(restored, "{abort}");
	}
}

#[test]
fn end_of_file_on_an_empty_line_ends_entry_and_restores_the_terminal() {
	let (result, _, restored) = entry(b"\x04", 64);
	assert!(
		matches!(result, Err(CustodyError::EndOfInput)),
		"{result:?}"
	);
	assert!(restored);
}

#[test]
fn an_overlong_line_is_refused_without_its_bytes_and_restores_the_terminal() {
	let (result, _, restored) = entry(b"correct horse battery staple\n", 8);
	let error = result.unwrap_err();
	assert!(
		matches!(error, CustodyError::TooLong { max: 8 }),
		"{error:?}"
	);
	for text in [error.to_string(), format!("{error:?}")] {
		assert!(!text.contains("correct"), "{text}");
	}
	assert!(restored);
}

#[test]
fn a_panic_while_echo_is_off_restores_the_terminal() {
	let (_master, slave) = sys::open_pty().unwrap();
	let before = sys::tcgetattr(&slave).unwrap();
	let unwound = std::panic::catch_unwind(|| {
		let _quiet = EchoOff::engage(&slave).unwrap();
		assert_eq!(sys::tcgetattr(&slave).unwrap().c_lflag & libc::ECHO, 0);
		panic!("unwinding with echo off");
	});
	assert!(unwound.is_err());
	assert!(same(&before, &sys::tcgetattr(&slave).unwrap()));
}

#[test]
fn a_source_that_is_not_a_terminal_is_refused_and_a_terminal_is_admitted() {
	let dir = crate::custody::test_dir::TestDir::new("kg-tty");
	let file = File::create(dir.join("plain")).unwrap();
	assert!(matches!(
		Terminal::from_file(file),
		Err(CustodyError::NotATerminal)
	));
	let (reader, _writer) = std::io::pipe().unwrap();
	let pipe = File::from(std::os::fd::OwnedFd::from(reader));
	assert!(matches!(
		Terminal::from_file(pipe),
		Err(CustodyError::NotATerminal)
	));
	let (_master, slave) = sys::open_pty().unwrap();
	Terminal::from_file(slave).unwrap();
}

#[test]
fn settings_count_as_quiet_only_with_echo_canonical_mode_and_signals_all_off() {
	let (_master, slave) = sys::open_pty().unwrap();
	let mut quiet = sys::tcgetattr(&slave).unwrap();
	quiet.c_lflag &= !(libc::ECHO | libc::ICANON | libc::ISIG);
	assert!(is_quiet(&quiet));
	for flag in [libc::ECHO, libc::ICANON, libc::ISIG] {
		let mut loud = quiet;
		loud.c_lflag |= flag;
		assert!(!is_quiet(&loud), "{flag:#x}");
	}
	// The pty's defaults are not quiet; engaging makes them so and the read-back agrees.
	assert!(!is_quiet(&sys::tcgetattr(&slave).unwrap()));
	let guard = EchoOff::engage(&slave).unwrap();
	assert!(is_quiet(&sys::tcgetattr(&slave).unwrap()));
	drop(guard);
	assert!(!is_quiet(&sys::tcgetattr(&slave).unwrap()));
}

#[test]
fn a_larger_max_is_capped_at_the_secret_limit() {
	let limit = MAX_SECRET_LEN;
	let line = |len: usize| [vec![b'k'; len], b"\r".to_vec()].concat();
	let (result, _, restored) = entry(&line(limit + 1), usize::MAX);
	assert!(
		matches!(result, Err(CustodyError::TooLong { max }) if max == limit),
		"{result:?}"
	);
	assert!(restored);
	// Control: exactly the limit is read whole under the same oversized `max`.
	let (result, _, restored) = entry(&line(limit), usize::MAX);
	assert_eq!(result.unwrap().len(), limit);
	assert!(restored);
}
