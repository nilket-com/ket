//! ADR 603: DuckDB's memory limit as a recorded coordinate item.
//!
//! The harness issued no `memory_limit` before this record, so DuckDB ran under its default (80%
//! of physical memory). An explicit limit is emitted as `SET memory_limit` and read back with
//! `current_setting('memory_limit')`. DuckDB reports the setting rounded for display (one decimal
//! of the largest binary unit, truncated: `2GB` reads back as `1.8 GiB`), so the request and the
//! read-back are compared at that display precision, not as exact bytes.

use std::sync::OnceLock;

pub const DUCKDB_MEMORY_LIMIT_ENV: &str = "TORON_TPCH_DUCKDB_MEMORY_LIMIT";

/// A limit as requested: the normalized spelling DuckDB is given, and its size in bytes.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbMemoryLimit {
	pub normalized: String,
	pub bytes: u64,
}

/// What the harness requested, fixed once and stamped on the header and every DuckDB row.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct DuckDbMemoryCoordinate {
	/// `None` when nothing was requested: DuckDB then runs under its own default.
	pub requested: Option<DuckDbMemoryLimit>,
	/// `explicit` when the environment named a limit, `default` otherwise.
	pub provenance: &'static str,
}

static COORDINATE: OnceLock<Result<DuckDbMemoryCoordinate, String>> = OnceLock::new();

/// The coordinate from `TORON_TPCH_DUCKDB_MEMORY_LIMIT`, read once per process. A present value
/// that is empty, malformed, or not Unicode is a refusal, not a fall-through to the default.
pub fn memory_limit_coordinate() -> Result<DuckDbMemoryCoordinate, String> {
	COORDINATE
		.get_or_init(|| {
			resolve_environment(std::env::var(DUCKDB_MEMORY_LIMIT_ENV)).map(|requested| {
				DuckDbMemoryCoordinate {
					provenance: if requested.is_some() {
						"explicit"
					} else {
						"default"
					},
					requested,
				}
			})
		})
		.clone()
}

pub fn resolve_environment(
	value: Result<String, std::env::VarError>,
) -> Result<Option<DuckDbMemoryLimit>, String> {
	match value {
		Err(std::env::VarError::NotPresent) => Ok(None),
		Err(std::env::VarError::NotUnicode(raw)) => Err(format!(
			"{DUCKDB_MEMORY_LIMIT_ENV}={raw:?}: value is not Unicode"
		)),
		Ok(text) => parse_memory_limit(&text)
			.map(Some)
			.map_err(|error| format!("{DUCKDB_MEMORY_LIMIT_ENV}={text:?}: {error}")),
	}
}

const UNITS: [(&str, u64); 8] = [
	("KiB", 1 << 10),
	("MiB", 1 << 20),
	("GiB", 1 << 30),
	("TiB", 1 << 40),
	("KB", 1_000),
	("MB", 1_000_000),
	("GB", 1_000_000_000),
	("TB", 1_000_000_000_000),
];

/// A request: a whole number and a unit (`2GiB`, `1536MiB`, `2GB`), normalized without a space.
pub fn parse_memory_limit(text: &str) -> Result<DuckDbMemoryLimit, String> {
	let trimmed = text.trim();
	let split = trimmed
		.find(|c: char| !c.is_ascii_digit())
		.ok_or_else(|| "no unit (expected KiB, MiB, GiB, TiB, KB, MB, GB, or TB)".to_string())?;
	let (number, unit) = trimmed.split_at(split);
	let unit = unit.trim_start();
	if number.is_empty() {
		return Err("no size before the unit".to_string());
	}
	let count: u64 = number
		.parse()
		.map_err(|_| format!("size {number:?} is not a whole number"))?;
	let &(unit_name, multiplier) = UNITS
		.iter()
		.find(|(name, _)| *name == unit)
		.ok_or_else(|| format!("unit {unit:?} is not KiB, MiB, GiB, TiB, KB, MB, GB, or TB"))?;
	let bytes = count
		.checked_mul(multiplier)
		.ok_or_else(|| "size overflows u64 bytes".to_string())?;
	if bytes == 0 {
		return Err("size is zero".to_string());
	}
	Ok(DuckDbMemoryLimit {
		normalized: format!("{count}{unit_name}"),
		bytes,
	})
}

const DISPLAY_UNITS: [(&str, u64); 4] = [
	("TiB", 1 << 40),
	("GiB", 1 << 30),
	("MiB", 1 << 20),
	("KiB", 1 << 10),
];

/// A byte count as DuckDB 1.5.4 displays a memory setting: one decimal of the largest binary
/// unit it reaches, truncated (`2,000,000,000` bytes is `1.8 GiB`); below a KiB, `N bytes`.
pub fn duckdb_display(bytes: u64) -> String {
	for (name, unit) in DISPLAY_UNITS {
		if bytes >= unit {
			let whole = bytes / unit;
			let tenth = (bytes % unit) * 10 / unit;
			return format!("{whole}.{tenth} {name}");
		}
	}
	format!("{bytes} bytes")
}

/// A read-back in DuckDB's display form, to bytes at its displayed precision.
pub fn parse_display(text: &str) -> Option<u64> {
	let (number, unit) = text.trim().split_once(' ')?;
	if unit == "bytes" {
		return number.parse().ok();
	}
	let (_, multiplier) = DISPLAY_UNITS.iter().find(|(name, _)| *name == unit)?;
	let (whole, tenth) = number.split_once('.')?;
	let whole: u64 = whole.parse().ok()?;
	let tenth: u64 = tenth.parse().ok().filter(|tenth| *tenth < 10)?;
	whole
		.checked_mul(*multiplier)?
		.checked_add(tenth * multiplier / 10)
}

/// Whether DuckDB's read-back is the request, at the precision DuckDB displays it.
pub fn read_back_agrees(requested: &DuckDbMemoryLimit, observed: &str) -> bool {
	duckdb_display(requested.bytes) == observed.trim()
}

/// ADR 603: the profiler settings for the diagnostic sweep, never used on a timed child. The
/// profile reports the query's peak temporary-directory size (its spill) and peak buffer memory.
pub const PROFILE_SETTINGS: &str = r#"{"SYSTEM_PEAK_TEMP_DIR_SIZE": "true", "SYSTEM_PEAK_BUFFER_MEMORY": "true", "LATENCY": "true"}"#;

/// One profiled child's readings from DuckDB's JSON profile.
#[derive(Debug, Clone, PartialEq, serde::Serialize)]
pub struct ProfileReadings {
	pub peak_temp_dir_bytes: u64,
	pub peak_buffer_bytes: u64,
	pub latency_seconds: f64,
}

/// Read the sweep's readings from a JSON profile; every field must be present and numeric.
pub fn parse_profile(profile: &serde_json::Value) -> Result<ProfileReadings, String> {
	let field = |name: &str| {
		profile
			.get(name)
			.ok_or_else(|| format!("DuckDB profile has no {name}"))
	};
	let bytes = |name: &str| {
		field(name)?
			.as_u64()
			.ok_or_else(|| format!("DuckDB profile {name} is not a byte count"))
	};
	Ok(ProfileReadings {
		peak_temp_dir_bytes: bytes("system_peak_temp_dir_size")?,
		peak_buffer_bytes: bytes("system_peak_buffer_memory")?,
		latency_seconds: field("latency")?
			.as_f64()
			.ok_or_else(|| "DuckDB profile latency is not a number".to_string())?,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn requests_normalize_to_bytes() {
		for (text, normalized, bytes) in [
			("2GiB", "2GiB", 2u64 << 30),
			(" 2 GiB ", "2GiB", 2 << 30),
			("1536MiB", "1536MiB", 1536 << 20),
			("2GB", "2GB", 2_000_000_000),
			("512KiB", "512KiB", 512 << 10),
		] {
			let limit = parse_memory_limit(text).unwrap();
			assert_eq!(limit.normalized, normalized, "{text}");
			assert_eq!(limit.bytes, bytes, "{text}");
		}
	}

	#[test]
	fn malformed_requests_are_refused() {
		for text in [
			"",
			"  ",
			"2",
			"GiB",
			"2 gib",
			"2.5GiB",
			"-2GiB",
			"0GiB",
			"2GiBs",
			"99999999999TiB",
		] {
			assert!(parse_memory_limit(text).is_err(), "{text:?}");
		}
	}

	#[test]
	fn the_environment_refuses_a_named_value_it_cannot_honour() {
		assert_eq!(
			resolve_environment(Err(std::env::VarError::NotPresent)).unwrap(),
			None
		);
		assert_eq!(
			resolve_environment(Ok("2GiB".to_string())).unwrap(),
			Some(DuckDbMemoryLimit {
				normalized: "2GiB".to_string(),
				bytes: 2 << 30,
			})
		);
		for text in ["", " ", "two", "2"] {
			let error = resolve_environment(Ok(text.to_string())).unwrap_err();
			assert!(error.contains(DUCKDB_MEMORY_LIMIT_ENV), "{error}");
		}
		#[cfg(unix)]
		{
			use std::os::unix::ffi::OsStringExt;
			let raw = std::ffi::OsString::from_vec(vec![0xff, 0xfe]);
			assert!(resolve_environment(Err(std::env::VarError::NotUnicode(raw))).is_err());
		}
	}

	/// The display strings DuckDB 1.5.4 returned on bigfoot for these requests.
	#[test]
	fn display_matches_duckdb_and_the_read_back_agrees_at_its_precision() {
		for (text, displayed) in [
			("2GiB", "2.0 GiB"),
			("2GB", "1.8 GiB"),
			("1536MiB", "1.5 GiB"),
		] {
			let limit = parse_memory_limit(text).unwrap();
			assert_eq!(duckdb_display(limit.bytes), displayed, "{text}");
			assert!(read_back_agrees(&limit, displayed), "{text}");
		}
		let two_gib = parse_memory_limit("2GiB").unwrap();
		assert_eq!(
			parse_display("2.0 GiB"),
			Some(two_gib.bytes),
			"2GiB round-trips to its bytes"
		);
		assert!(
			!read_back_agrees(&two_gib, "1.8 GiB"),
			"a disagreement is not accepted"
		);
		assert!(!read_back_agrees(&two_gib, "98.5 GiB"));
		assert_eq!(
			parse_display("98.5 GiB"),
			Some(98 * (1 << 30) + (1 << 30) * 5 / 10)
		);
		assert_eq!(parse_display("512 bytes"), Some(512));
		assert_eq!(duckdb_display(512), "512 bytes");
		for text in ["", "2.0", "2.0 gib", "2.0GiB", "x.0 GiB", "2.10 GiB"] {
			assert_eq!(parse_display(text), None, "{text:?}");
		}
	}

	/// The readings DuckDB 1.5.4 produced on bigfoot for a 40M-row sort under 100 MiB (spilled)
	/// and under 8 GiB (did not).
	#[test]
	fn profiles_report_the_forced_spill_and_its_absence() {
		let spilled = serde_json::json!({"latency": 3.139960614,
			"system_peak_buffer_memory": 135622656, "system_peak_temp_dir_size": 198410240});
		assert_eq!(
			parse_profile(&spilled).unwrap(),
			ProfileReadings {
				peak_temp_dir_bytes: 198_410_240,
				peak_buffer_bytes: 135_622_656,
				latency_seconds: 3.139960614,
			}
		);
		let in_memory = serde_json::json!({"latency": 2.597177239,
			"system_peak_buffer_memory": 1086291968, "system_peak_temp_dir_size": 0});
		assert_eq!(parse_profile(&in_memory).unwrap().peak_temp_dir_bytes, 0);
		for broken in [
			serde_json::json!({"latency": 1.0, "system_peak_buffer_memory": 1}),
			serde_json::json!({"latency": 1.0, "system_peak_buffer_memory": 1,
				"system_peak_temp_dir_size": "0"}),
			serde_json::json!({"system_peak_buffer_memory": 1, "system_peak_temp_dir_size": 0}),
		] {
			assert!(parse_profile(&broken).is_err(), "{broken}");
		}
	}
}
