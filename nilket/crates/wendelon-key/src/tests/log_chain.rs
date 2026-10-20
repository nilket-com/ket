//! T-LOG and T-LEFT: every operation that reaches the locked directory appends one chained line,
//! refusals and wrong passphrases included; an edited middle line breaks the chain and the next
//! operation refuses; leftovers are reported, logged and kept.
use super::support::*;
use crate::log::verify_chain;

#[test]
fn every_operation_appends_one_chained_line_and_an_edit_breaks_the_chain() {
	let fx = Fixture::new();
	let mut io = fx.io();
	io.say(&[PRIMARY, PRIMARY]).seed(ADMIN_SEED);
	io.ok(&[
		"generate",
		"--dir",
		&fx.dir(),
		"--role",
		"admin",
		"--intended-epoch",
		"0",
		"--out",
		"a.p",
	]);
	io.say(&[SECONDARY]);
	io.refused(
		&["show-public", "--dir", &fx.dir(), "--key", "a.p"],
		"wrong passphrase",
	);
	io.refused(
		&["show-public", "--dir", &fx.dir(), "--key", "missing"],
		"missing",
	);
	io.say(&[PRIMARY]);
	let out = io.ok(&["show-public", "--dir", &fx.dir(), "--key", "a.p"]);
	assert_eq!(out.trim(), hex(&public(&ADMIN_SEED)));
	let log = fx.log();
	let (lines, head) = verify_chain(&log).unwrap();
	let ops: Vec<(&str, &str)> = lines
		.iter()
		.map(|l| (l.operation.as_str(), l.outcome.as_str()))
		.collect();
	assert_eq!(ops.len(), 4, "{ops:?}");
	assert_eq!(ops[0], ("generate", "ok"));
	assert!(
		ops[1].1.starts_with("refused: a.p: wrong passphrase"),
		"{ops:?}"
	);
	assert!(ops[2].1.starts_with("refused:"), "{ops:?}");
	assert_eq!(ops[3], ("show-public", "ok"));
	assert!(lines[0].identity.is_some() && lines[0].outputs.len() == 1);
	assert!(String::from_utf8_lossy(&io.err).contains(&format!("custody.log head {}", hex(&head))));
	// An edit of a middle line (here its outcome) breaks the chain at the next line.
	let text = String::from_utf8(log).unwrap();
	let edited = text.replacen("\"outcome\":\"ok\"", "\"outcome\":\"OK\"", 1);
	assert_ne!(edited, text);
	std::fs::write(fx.keys.join("custody.log"), &edited).unwrap();
	assert!(matches!(
		verify_chain(edited.as_bytes()),
		Err(crate::error::ToolError::LogChain { line: 2 })
	));
	io.say(&[PRIMARY]);
	io.refused(
		&["show-public", "--dir", &fx.dir(), "--key", "a.p"],
		"hash chain breaks at line 2",
	);
	assert_eq!(
		std::fs::read_to_string(fx.keys.join("custody.log")).unwrap(),
		edited,
		"nothing appended"
	);
	// A removed middle line breaks it too; the unmodified log verifies (control above).
	let mut kept: Vec<&str> = text.lines().collect();
	kept.remove(1);
	let removed = format!("{}\n", kept.join("\n"));
	assert!(verify_chain(removed.as_bytes()).is_err());
}

#[test]
fn a_leftover_temp_file_is_reported_logged_and_kept() {
	let fx = Fixture::new();
	let mut io = fx.io();
	let (ok, _, err) = io.run(&["show-public", "--dir", &fx.dir(), "--unauthenticated"]);
	assert!(ok && !err.contains("leftover"), "{err}");
	let leftover = fx.keys.join(".wrolekey.tmp.4242.0");
	std::fs::write(&leftover, [1, 2, 3]).unwrap();
	let (ok, _, err) = io.run(&["show-public", "--dir", &fx.dir(), "--unauthenticated"]);
	assert!(
		ok && err.contains("leftover temp files") && err.contains(".wrolekey.tmp.4242.0"),
		"{err}"
	);
	let (lines, _) = verify_chain(&fx.log()).unwrap();
	assert!(lines[0].leftovers.is_empty());
	assert_eq!(lines[1].leftovers, [".wrolekey.tmp.4242.0"]);
	assert_eq!(std::fs::read(&leftover).unwrap(), [1, 2, 3]);
}

#[test]
fn a_failed_host_probe_and_a_busy_child_lock_are_logged_once_the_lock_is_held() {
	let fx = Fixture::new();
	let mut io = fx.io();
	io.host_fails = true;
	io.refused(
		&["show-public", "--dir", &fx.dir(), "--unauthenticated"],
		"probe failed",
	);
	io.host_fails = false;
	std::fs::create_dir(fx.keys.join("pending")).unwrap();
	std::fs::set_permissions(
		fx.keys.join("pending"),
		std::os::unix::fs::PermissionsExt::from_mode(0o700),
	)
	.unwrap();
	let pending =
		key_guard::custody::open_custody_dir_anchored(&fx.keys.join("pending"), &fx.root).unwrap();
	let held = pending.lock().unwrap();
	io.refused(
		&["show-public", "--dir", &fx.dir(), "--unauthenticated"],
		"locked by another operation",
	);
	drop(held);
	// Control: with the probe working and the child free, the same operation succeeds.
	io.ok(&["show-public", "--dir", &fx.dir(), "--unauthenticated"]);
	let (lines, _) = verify_chain(&fx.log()).unwrap();
	let outcomes: Vec<&str> = lines.iter().map(|l| l.outcome.as_str()).collect();
	assert_eq!(outcomes.len(), 3, "{outcomes:?}");
	assert!(
		outcomes[0].starts_with("refused:") && outcomes[0].contains("probe failed"),
		"{outcomes:?}"
	);
	assert!(
		outcomes[1].contains("locked by another operation"),
		"{outcomes:?}"
	);
	assert_eq!(outcomes[2], "ok");
}

#[test]
fn a_wrong_passphrase_line_carries_the_unauthenticated_identity() {
	let fx = Fixture::new();
	let mut io = fx.io();
	io.say(&[PRIMARY, PRIMARY]).seed(ADMIN_SEED);
	io.ok(&[
		"generate",
		"--dir",
		&fx.dir(),
		"--role",
		"admin",
		"--intended-epoch",
		"0",
		"--out",
		"a.p",
	]);
	io.say(&[SECONDARY]);
	io.refused(
		&["show-public", "--dir", &fx.dir(), "--key", "a.p"],
		"wrong passphrase",
	);
	io.say(&[PRIMARY]);
	io.ok(&["show-public", "--dir", &fx.dir(), "--key", "a.p"]);
	let (lines, _) = verify_chain(&fx.log()).unwrap();
	let generated = lines[0]
		.identity
		.clone()
		.expect("generate logs the identity");
	assert_eq!(lines[1].unauthenticated_identity.as_ref(), Some(&generated));
	assert_eq!(lines[1].identity, None);
	// Control: a successful decryption logs the authenticated identity and no unauthenticated one.
	assert_eq!(lines[2].identity.as_ref(), Some(&generated));
	assert_eq!(lines[2].unauthenticated_identity, None);
	assert!(
		String::from_utf8(fx.log())
			.unwrap()
			.contains("\"unauthenticated_identity\":")
	);
}
