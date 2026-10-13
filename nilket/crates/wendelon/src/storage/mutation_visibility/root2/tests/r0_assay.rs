//! R0 isolated cost instrument: cfg(test) parent, ignored assay; no live wiring.
use super::*;
use crate::storage::mutation_visibility::tests::b1_assay::{r0_counters, r0_reset_peak};
use serde_json::{Value, json};
use std::{hint::black_box, time::Instant};
mod models;
use models::*;

fn cpu_ns() -> Option<u64> {
	let mut t = libc::timespec {
		tv_sec: 0,
		tv_nsec: 0,
	};
	if unsafe { libc::clock_gettime(libc::CLOCK_PROCESS_CPUTIME_ID, &mut t) } != 0 {
		return None;
	}
	u64::try_from(t.tv_sec)
		.ok()?
		.checked_mul(1_000_000_000)?
		.checked_add(u64::try_from(t.tv_nsec).ok()?)
}
fn emit(v: Value) {
	println!("R0_JSON {}", v);
}
fn measured<T>(name: &str, clock: Instant, f: impl FnOnce() -> T, verify: impl FnOnce(&T)) {
	emit(json!({"event":"operation_start","arm":name,"at_ns":clock.elapsed().as_nanos() as u64}));
	r0_reset_peak();
	let base = r0_counters();
	let cpu_start = cpu_ns();
	let start = clock.elapsed().as_nanos() as u64;
	let output = black_box(f());
	let end = clock.elapsed().as_nanos() as u64;
	let cpu_end = cpu_ns();
	let retained = r0_counters();
	verify(&output); // Correctness/hashing is outside the measured interval.
	drop(output);
	let after = r0_counters();
	emit(
		json!({"event":"operation", "arm":name,"start_ns":start,"end_ns":end,
		"cpu_start_ns":cpu_start,"cpu_end_ns":cpu_end,"live_before":base[0],
		"live_output":retained[0],"peak_live":retained[1],"allocations":retained[2]-base[2],
		"requested_bytes":retained[3]-base[3],"after_drop_live":after[0],
		"scope":"operation only; input retained; correctness then drop outside timing"}),
	);
	assert_eq!(
		after[0], base[0],
		"allocation remains after output/correctness drop: {name}"
	);
}

#[test]
#[ignore = "R0 instrument: only after frozen review; fresh process, --exact --test-threads=1"]
fn r0_isolated_cost_assay() {
	let case = std::env::var("WENDELON_R0_CASE").expect("declared case");
	let repetition: usize = std::env::var("WENDELON_R0_REPEAT")
		.unwrap()
		.parse()
		.unwrap();
	assert!((1..=7).contains(&repetition));
	let clock = Instant::now();
	emit(json!({"event":"start","case":case,"repetition":repetition,"schema":1}));
	let root = model(&case);
	let bytes = root.encode().unwrap();
	let decoded = UnverifiedRoot2::decode(&bytes).unwrap();
	assert_eq!(decoded.encode().unwrap(), bytes);
	drop(decoded);
	let u = root.usage().unwrap();
	emit(
		json!({"event":"fixture","case":case,"parents":u.parents,"held":u.held,"history":u.history,
		"registry":u.registry,"lineages":root.state.lineages.len(),"cuts":root.state.cuts.len(),
		"selectors":root.state.selectors.len(),"authorities":root.state.authorities.len(),
		"encoded_bytes":bytes.len(),"encoded_sha256":hex_digest(hash(b"",&bytes)),
		"setup_end_ns":clock.elapsed().as_nanos() as u64}),
	);
	measured("empty", clock, || black_box(0u64), |v| assert_eq!(*v, 0));
	measured(
		"encode",
		clock,
		|| root.encode().unwrap(),
		|v| assert_eq!(*v, bytes),
	);
	measured(
		"decode",
		clock,
		|| UnverifiedRoot2::decode(&bytes).unwrap(),
		|v| assert_eq!(v.encode().unwrap(), bytes),
	);
	measured(
		"validate",
		clock,
		|| validate_state(&root.state).unwrap(),
		|_| {},
	);
	measured(
		"clone",
		clock,
		|| root.clone(),
		|v| assert!(v.state == root.state),
	);
	for arm in arms(&root) {
		let Arm {
			name,
			command,
			expectation,
			limits,
			same_entry,
		} = arm;
		let input = if let Some(limits) = limits {
			root.clone().lower_limits(limits)
		} else {
			root.clone()
		};
		let entry = TrustedEntry2::fixture(
			input.state.generation,
			input.state.cluster,
			if same_entry {
				input.covered()
			} else {
				l(input.covered().index + 1)
			},
			&command,
		)
		.unwrap();
		let verified = VerifiedCommand2::fixture(
			input.state.generation,
			input.state.cluster,
			&command,
			&[original()],
			1,
		)
		.unwrap();
		// Replay fingerprint must be the original entry, not an invented same-log event.
		let mut input = input;
		if same_entry {
			let (_, _, _, fingerprint, digest) = entry.fields();
			input.state.fingerprint = fingerprint;
			input.state.command_digest = digest;
		}
		emit(
			json!({"event":"arm_input","arm":name,"command_sha256":hex_digest(command.digest(input.state.generation,input.state.cluster).unwrap()),
			"expectation":expectation,"lowered_limits":limits.is_some(),"input_sha256":hex_digest(hash(b"", &input.encode().unwrap())),"source":"separate unchanged model clone; prepared trust tokens"}),
		);
		measured(
			name,
			clock,
			|| Root2::reduce(Some(&input), &entry, &verified, &command).unwrap(),
			|(out, result)| {
				validate_state(&out.state).unwrap();
				let label = result_label(result);
				assert_eq!(label, expectation, "wrong arm outcome: {name}");
			},
		);
	}
	for (arm, reason) in inapplicable(&root) {
		emit(json!({"event":"inapplicable","arm":arm,"reason":reason}));
	}
	// Malformed input refusal is correctness only, never averaged into cost arms.
	let mut bad = bytes.clone();
	let last = bad.len() - 1;
	bad[last] ^= 1;
	assert!(UnverifiedRoot2::decode(&bad).is_err());
	for n in [0, 7, 47] {
		assert!(UnverifiedRoot2::decode(&bytes[..n]).is_err());
	}
	emit(
		json!({"event":"end","case":case,"repetition":repetition,"at_ns":clock.elapsed().as_nanos() as u64,"correctness_refusals":4}),
	);
}
fn result_label(v: &ApplyResult2) -> &'static str {
	match v {
		ApplyResult2::Covered => "Covered",
		ApplyResult2::AlreadyProcessed => "AlreadyProcessed",
		ApplyResult2::Denied { reason: 2, .. } => "Conflict",
		ApplyResult2::Denied { reason: 1, .. } => "Denied",
		ApplyResult2::Denied { reason: 10, .. } => "Capacity",
		ApplyResult2::Granted(_) => "Granted",
		ApplyResult2::Parent {
			new_effect: true, ..
		} => "TerminalEffect",
		ApplyResult2::Parent {
			record,
			new_effect: false,
		} => match record.terminal {
			Some(Terminal2::Refused { .. }) => "Refused",
			Some(Terminal2::Accepted { .. }) => "AcceptedReplay",
			None => "Reserved",
		},
		_ => "Unexpected",
	}
}
fn hex_digest(d: Digest2) -> String {
	d.iter().map(|b| format!("{b:02x}")).collect()
}

#[test]
fn r0_small_models_and_arm_semantics_are_valid_without_timing() {
	for case in [
		"E", "P1", "P256", "R-small", "H-small", "B-small", "X-small",
	] {
		let r = model(case);
		validate_state(&r.state).unwrap();
		let b = r.encode().unwrap();
		assert_eq!(UnverifiedRoot2::decode(&b).unwrap().encode().unwrap(), b);
		for a in arms(&r) {
			let input = if let Some(l) = a.limits {
				r.clone().lower_limits(l)
			} else {
				r.clone()
			};
			if a.same_entry {
				continue;
			}
			let (_, v) = next(&input, &a.command, &[original()], 1);
			assert_eq!(result_label(&v), a.expectation, "{} {}", case, a.name);
		}
	}
}

#[test]
#[ignore = "construction-only review feasibility; no timings/counters/cost samples"]
fn r0_full_case_feasibility_without_measurement() {
	for case in CASES {
		let r = model(case);
		validate_state(&r.state).unwrap();
		let b = r.encode().unwrap();
		let u = UnverifiedRoot2::decode(&b).unwrap();
		assert_eq!(u.encode().unwrap(), b);
		let usage = r.usage().unwrap();
		println!(
			"R0_FEASIBILITY {}",
			json!({"case":case,"parents":usage.parents,"held":usage.held,"history":usage.history,"registry":usage.registry,
			"lineages":r.state.lineages.len(),"cuts":r.state.cuts.len(),"selectors":r.state.selectors.len(),"authorities":r.state.authorities.len(),"encoded":b.len(),"sha256":hex_digest(hash(b"",&b))})
		);
		for a in arms(&r) {
			let mut input = if let Some(l) = a.limits {
				r.clone().lower_limits(l)
			} else {
				r.clone()
			};
			let entry = TrustedEntry2::fixture(
				input.state.generation,
				input.state.cluster,
				if a.same_entry {
					input.covered()
				} else {
					l(input.covered().index + 1)
				},
				&a.command,
			)
			.unwrap();
			if a.same_entry {
				let (_, _, _, fp, d) = entry.fields();
				input.state.fingerprint = fp;
				input.state.command_digest = d;
			}
			let verified = VerifiedCommand2::fixture(
				input.state.generation,
				input.state.cluster,
				&a.command,
				&[original()],
				1,
			)
			.unwrap();
			let (output, result) =
				Root2::reduce(Some(&input), &entry, &verified, &a.command).unwrap();
			assert_eq!(result_label(&result), a.expectation, "{} {}", case, a.name);
			validate_state(&output.state).unwrap();
			drop(output);
			println!(
				"R0_ARM_FEASIBILITY {}",
				json!({"case":case,"event":"arm_input","arm":a.name,"command_sha256":hex_digest(a.command.digest(input.state.generation,input.state.cluster).unwrap()),"expectation":a.expectation,"lowered_limits":a.limits.is_some(),"input_sha256":hex_digest(hash(b"",&input.encode().unwrap())),"source":"separate unchanged model clone; prepared trust tokens"})
			);
		}
		for (arm, reason) in inapplicable(&r) {
			println!(
				"R0_OMITTED_FEASIBILITY {}",
				json!({"case":case,"arm":arm,"reason":reason})
			);
		}
	}
}

mod g2;
