//! ADR 591: CPU affinity as a recorded coordinate item.
//!
//! On Linux affinity is a per-thread attribute. A new thread inherits its creator's mask and a
//! child process inherits the creating thread's, so the mask must be applied on the main thread
//! before the runtime's workers exist and before any child is spawned. The kernel applies a
//! request intersected with the CPUs the caller may use, so a successful call does not establish
//! that the request was honoured; the mask is read back and must equal the request exactly.

use std::collections::BTreeSet;
use std::sync::OnceLock;

pub const CPU_AFFINITY_ENV: &str = "TORON_TPCH_CPU_AFFINITY";

/// What the harness requested, fixed once at startup and stamped on every row and header.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AffinityCoordinate {
	/// The normalized cpulist, `None` when nothing was requested and nothing applied.
	pub requested: Option<String>,
	/// `explicit` when the environment named a mask, `default` otherwise.
	pub provenance: &'static str,
}

static COORDINATE: OnceLock<AffinityCoordinate> = OnceLock::new();

/// The coordinate as applied at startup; `default` with no request when the process never
/// applied one (a library test, a binary that does not call [`apply_from_environment`]).
pub fn affinity_coordinate() -> AffinityCoordinate {
	COORDINATE.get().cloned().unwrap_or(AffinityCoordinate {
		requested: None,
		provenance: "default",
	})
}

/// Read `TORON_TPCH_CPU_AFFINITY`, apply it to the calling thread, verify the readback, and fix
/// the coordinate for the process. Call first thing in `main`, before the runtime is built.
pub fn apply_from_environment() -> Result<AffinityCoordinate, String> {
	let coordinate = match resolve_environment(std::env::var(CPU_AFFINITY_ENV))? {
		Some(requested) => {
			apply_affinity(&requested)?;
			AffinityCoordinate {
				requested: Some(format_cpulist(&requested)),
				provenance: "explicit",
			}
		}
		None => AffinityCoordinate {
			requested: None,
			provenance: "default",
		},
	};
	COORDINATE
		.set(coordinate.clone())
		.map_err(|_| "CPU affinity coordinate was already fixed for this process".to_string())?;
	Ok(coordinate)
}

/// Only an absent variable selects the default. A present value that is empty, malformed, or not
/// Unicode is a refusal: a coordinate that was named and could not be honoured must not fall
/// through to "nothing requested".
pub fn resolve_environment(
	value: Result<String, std::env::VarError>,
) -> Result<Option<BTreeSet<usize>>, String> {
	match value {
		Err(std::env::VarError::NotPresent) => Ok(None),
		Err(std::env::VarError::NotUnicode(raw)) => {
			Err(format!("{CPU_AFFINITY_ENV}={raw:?}: value is not Unicode"))
		}
		Ok(text) => parse_cpulist(&text)
			.map(Some)
			.map_err(|error| format!("{CPU_AFFINITY_ENV}={text:?}: {error}")),
	}
}

/// Parse the kernel's cpulist syntax (`0-15`, `0-7,16-23`, `3,1,2`) into a set.
pub fn parse_cpulist(text: &str) -> Result<BTreeSet<usize>, String> {
	let mut cpus = BTreeSet::new();
	for item in text.split(',') {
		let item = item.trim();
		if item.is_empty() {
			return Err("empty cpulist item".to_string());
		}
		let (low, high) = match item.split_once('-') {
			Some((low, high)) => (parse_cpu(low)?, parse_cpu(high)?),
			None => {
				let cpu = parse_cpu(item)?;
				(cpu, cpu)
			}
		};
		if low > high {
			return Err(format!("descending cpulist range {item:?}"));
		}
		cpus.extend(low..=high);
	}
	if cpus.is_empty() {
		return Err("empty cpulist".to_string());
	}
	Ok(cpus)
}

fn parse_cpu(text: &str) -> Result<usize, String> {
	let text = text.trim();
	if text.is_empty() {
		return Err("empty cpu id".to_string());
	}
	let cpu = text
		.parse::<usize>()
		.map_err(|_| format!("cpu id {text:?} is not a number"))?;
	if cpu >= libc::CPU_SETSIZE as usize {
		return Err(format!(
			"cpu id {cpu} is at or above CPU_SETSIZE {}",
			libc::CPU_SETSIZE
		));
	}
	Ok(cpu)
}

/// Render a set as a normalized cpulist: ascending, ranges collapsed.
pub fn format_cpulist(cpus: &BTreeSet<usize>) -> String {
	let mut parts = Vec::new();
	let mut iter = cpus.iter().copied().peekable();
	while let Some(start) = iter.next() {
		let mut end = start;
		while iter.peek() == Some(&(end + 1)) {
			end = iter.next().expect("peeked");
		}
		parts.push(if start == end {
			start.to_string()
		} else {
			format!("{start}-{end}")
		});
	}
	parts.join(",")
}

/// The calling thread's affinity mask.
pub fn current_affinity() -> Result<BTreeSet<usize>, String> {
	// SAFETY: cpu_set_t is plain data; sched_getaffinity fills it for the calling thread (pid 0).
	let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
	let status =
		unsafe { libc::sched_getaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &mut set) };
	if status != 0 {
		return Err(format!(
			"sched_getaffinity: {}",
			std::io::Error::last_os_error()
		));
	}
	let mut cpus = BTreeSet::new();
	for cpu in 0..libc::CPU_SETSIZE as usize {
		// SAFETY: cpu is below CPU_SETSIZE.
		if unsafe { libc::CPU_ISSET(cpu, &set) } {
			cpus.insert(cpu);
		}
	}
	Ok(cpus)
}

/// The calling thread's mask as a cpulist, for rows and headers.
pub fn observed_cpulist() -> Option<String> {
	current_affinity().ok().map(|cpus| format_cpulist(&cpus))
}

/// Apply `requested` to the calling thread and require the readback to equal it. Linux
/// intersects the request with the caller's allowed CPUs and reports success; a narrowed mask
/// is refused here with both cpulists named.
pub fn apply_affinity(requested: &BTreeSet<usize>) -> Result<(), String> {
	// SAFETY: cpu_set_t is plain data and every cpu id was bounded by CPU_SETSIZE at parse.
	let mut set: libc::cpu_set_t = unsafe { std::mem::zeroed() };
	for &cpu in requested {
		unsafe { libc::CPU_SET(cpu, &mut set) };
	}
	let status =
		unsafe { libc::sched_setaffinity(0, std::mem::size_of::<libc::cpu_set_t>(), &set) };
	if status != 0 {
		return Err(format!(
			"sched_setaffinity({}): {}",
			format_cpulist(requested),
			std::io::Error::last_os_error()
		));
	}
	let observed = current_affinity()?;
	if &observed != requested {
		return Err(format!(
			"cpu affinity request {} was narrowed by the host to {}; refusing to run under a mask other than the one requested",
			format_cpulist(requested),
			format_cpulist(&observed)
		));
	}
	Ok(())
}

#[cfg(test)]
mod tests {
	use super::*;

	fn set(cpus: &[usize]) -> BTreeSet<usize> {
		cpus.iter().copied().collect()
	}

	#[test]
	fn cpulists_parse_and_normalize() {
		assert_eq!(format_cpulist(&parse_cpulist("0-15").unwrap()), "0-15");
		assert_eq!(
			format_cpulist(&parse_cpulist("0-7,16-23").unwrap()),
			"0-7,16-23"
		);
		assert_eq!(format_cpulist(&parse_cpulist("3,1,2").unwrap()), "1-3");
		assert_eq!(
			format_cpulist(&parse_cpulist(" 4 , 0-1 ").unwrap()),
			"0-1,4"
		);
		assert_eq!(parse_cpulist("2,2,3").unwrap(), set(&[2, 3]));
		assert_eq!(format_cpulist(&set(&[0, 2, 3, 4, 9])), "0,2-4,9");
		for bad in ["", " ", ",", "a", "5-3", "1-", "-1", "0-15,", "1024"] {
			assert!(parse_cpulist(bad).is_err(), "{bad:?} must be refused");
		}
	}

	/// A subset of the current mask, so the test holds on any host with at least two CPUs.
	fn subset_of_current() -> Option<(BTreeSet<usize>, BTreeSet<usize>)> {
		let original = current_affinity().unwrap();
		if original.len() < 2 {
			return None;
		}
		let mut subset = original.clone();
		let last = *subset.iter().next_back().unwrap();
		subset.remove(&last);
		Some((original, subset))
	}

	#[test]
	fn an_applied_mask_reaches_runtime_workers_and_child_processes() {
		let Some((original, subset)) = subset_of_current() else {
			return;
		};
		apply_affinity(&subset).unwrap();
		assert_eq!(current_affinity().unwrap(), subset, "calling thread");
		let on_thread = std::thread::spawn(current_affinity)
			.join()
			.unwrap()
			.unwrap();
		assert_eq!(on_thread, subset, "a thread spawned afterwards");
		let runtime = tokio::runtime::Builder::new_multi_thread()
			.worker_threads(2)
			.build()
			.unwrap();
		let on_worker = runtime
			.block_on(async { tokio::spawn(async { current_affinity() }).await.unwrap() })
			.unwrap();
		assert_eq!(on_worker, subset, "a runtime worker built afterwards");
		let output = std::process::Command::new("sh")
			.arg("-c")
			.arg("grep Cpus_allowed_list /proc/self/status")
			.output()
			.unwrap();
		let text = String::from_utf8_lossy(&output.stdout);
		let child = text
			.split(':')
			.nth(1)
			.map(str::trim)
			.and_then(|list| parse_cpulist(list).ok())
			.unwrap_or_default();
		assert_eq!(child, subset, "an executed child: {text:?}");
		apply_affinity(&original).unwrap();
	}

	#[test]
	fn a_request_the_host_narrows_is_refused_with_both_lists_named() {
		let original = current_affinity().unwrap();
		// SAFETY: sysconf has no preconditions.
		let configured = unsafe { libc::sysconf(libc::_SC_NPROCESSORS_CONF) };
		let Ok(absent) = usize::try_from(configured) else {
			return;
		};
		if absent >= libc::CPU_SETSIZE as usize {
			return;
		}
		let mut widened = original.clone();
		widened.insert(absent);
		let error = apply_affinity(&widened).unwrap_err();
		assert!(
			error.contains(&format_cpulist(&widened)) && error.contains("narrowed"),
			"{error}"
		);
		assert_eq!(current_affinity().unwrap(), original);
	}

	#[test]
	fn only_an_absent_variable_selects_the_default() {
		use std::env::VarError;
		assert_eq!(
			resolve_environment(Err(VarError::NotPresent)).unwrap(),
			None
		);
		assert_eq!(
			resolve_environment(Ok("0-1".to_string())).unwrap(),
			Some(set(&[0, 1]))
		);
		for present in ["", " ", "\t", "x", "0-", "16-15"] {
			let error = resolve_environment(Ok(present.to_string())).unwrap_err();
			assert!(error.starts_with(CPU_AFFINITY_ENV), "{present:?}: {error}");
		}
		let raw =
			<std::ffi::OsString as std::os::unix::ffi::OsStringExt>::from_vec(vec![0x66, 0xff]);
		let error = resolve_environment(Err(VarError::NotUnicode(raw))).unwrap_err();
		assert!(error.contains("not Unicode"), "{error}");
	}

	#[test]
	fn the_coordinate_defaults_when_nothing_was_applied() {
		assert_eq!(affinity_coordinate().provenance, "default");
		assert!(observed_cpulist().is_some());
	}
}
