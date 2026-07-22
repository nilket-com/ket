//! Cargo manifest dependency annotations backed by the crates.io API.
//!
//! This deliberately runs only in the semantic worker.  Rendering consumes the
//! cached projection and never performs network I/O.

use std::env;
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

use semver::{Version, VersionReq};
use serde::Deserialize;

use crate::core::DocLine;
use crate::engine::language::{InlineAnnotation, InlineAnnotationKind};

const CACHE_TTL: Duration = Duration::from_secs(24 * 60 * 60);
const REQUEST_TIMEOUT: Duration = Duration::from_secs(5);

#[derive(Debug)]
struct Dependency<'a> {
	name: &'a str,
	version_requirement: &'a str,
	line: usize,
}

#[derive(Deserialize)]
struct CrateResponse {
	#[serde(rename = "crate")]
	krate: CrateMetadata,
}

#[derive(Deserialize)]
struct CrateMetadata {
	max_version: String,
}

/// Return update hints for direct registry dependencies in a Cargo manifest.
pub fn annotations(source: &str, file_path: &str) -> Vec<InlineAnnotation> {
	if Path::new(file_path)
		.file_name()
		.and_then(|name| name.to_str())
		!= Some("Cargo.toml")
	{
		return Vec::new();
	}

	let agent: ureq::Agent = ureq::Agent::config_builder()
		.timeout_global(Some(REQUEST_TIMEOUT))
		.build()
		.into();

	extract_dependencies(source)
		.into_iter()
		.filter_map(|dependency| annotate_dependency(&agent, dependency))
		.collect()
}

fn annotate_dependency(
	agent: &ureq::Agent,
	dependency: Dependency<'_>,
) -> Option<InlineAnnotation> {
	let latest = cached_or_fetch(agent, dependency.name)?;
	let latest_version = Version::parse(&latest).ok()?;
	let requirement = VersionReq::parse(dependency.version_requirement).ok()?;
	let (text, kind) = if requirement.matches(&latest_version) {
		(format!("✓ {latest}"), InlineAnnotationKind::Current)
	} else {
		(format!("↑ {latest}"), InlineAnnotationKind::Upgrade)
	};
	Some(InlineAnnotation {
		line: DocLine::new(dependency.line as u64),
		text,
		kind,
	})
}

fn extract_dependencies(source: &str) -> Vec<Dependency<'_>> {
	let mut in_dependencies = false;
	let mut dependencies = Vec::new();
	for (line, raw) in source.lines().enumerate() {
		let trimmed = raw.trim();
		if let Some(header) = trimmed
			.strip_prefix('[')
			.and_then(|rest| rest.strip_suffix(']'))
		{
			in_dependencies = dependency_table(header);
			continue;
		}
		if !in_dependencies || trimmed.starts_with('#') {
			continue;
		}
		let Some((name, value)) = trimmed.split_once('=') else {
			continue;
		};
		let name = name.trim().trim_matches('"');
		let value = value.trim();
		let version = quoted_value(value).or_else(|| inline_version(value));
		if !name.is_empty()
			&& let Some(version_requirement) = version
		{
			dependencies.push(Dependency {
				name,
				version_requirement,
				line,
			});
		}
	}
	dependencies
}

fn dependency_table(header: &str) -> bool {
	let final_segment = header.rsplit('.').next().unwrap_or(header);
	matches!(
		final_segment,
		"dependencies" | "dev-dependencies" | "build-dependencies"
	)
}

fn quoted_value(value: &str) -> Option<&str> {
	let value = value.trim();
	value
		.strip_prefix('"')?
		.split_once('"')
		.map(|(version, _)| version)
}

fn inline_version(value: &str) -> Option<&str> {
	let value = value.strip_prefix('{')?.strip_suffix('}')?;
	let (_, version) = value.split_once("version")?;
	let (_, after_equals) = version.split_once('=')?;
	quoted_value(after_equals.trim_start())
}

fn cached_or_fetch(agent: &ureq::Agent, crate_name: &str) -> Option<String> {
	let path = cache_path(crate_name)?;
	if let Some(version) = read_fresh_cache(&path) {
		return Some(version);
	}
	let url = format!("https://crates.io/api/v1/crates/{crate_name}");
	let body = agent
		.get(&url)
		.call()
		.ok()?
		.body_mut()
		.read_to_string()
		.ok()?;
	let response: CrateResponse = serde_json::from_str(&body).ok()?;
	write_cache(&path, &response.krate.max_version);
	Some(response.krate.max_version)
}

fn cache_path(crate_name: &str) -> Option<PathBuf> {
	let root = env::var_os("XDG_CACHE_HOME")
		.map(PathBuf::from)
		.or_else(|| env::var_os("HOME").map(|home| PathBuf::from(home).join(".cache")))?;
	Some(root.join("baryon").join("crates").join(crate_name))
}

fn read_fresh_cache(path: &Path) -> Option<String> {
	let content = fs::read_to_string(path).ok()?;
	let (timestamp, version) = content.trim().split_once('\n')?;
	let timestamp = timestamp.parse::<u64>().ok()?;
	let age = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.ok()?
		.as_secs()
		.saturating_sub(timestamp);
	(age <= CACHE_TTL.as_secs()).then(|| version.to_owned())
}

fn write_cache(path: &Path, version: &str) {
	let Some(parent) = path.parent() else { return };
	if fs::create_dir_all(parent).is_ok() {
		let timestamp = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.map_or(0, |time| time.as_secs());
		let _ = fs::write(path, format!("{timestamp}\n{version}\n"));
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn extracts_workspace_target_and_inline_dependencies() {
		let source = r#"
[workspace.dependencies]
foo = "1.0"
bar = { version = "2.0", features = ["x"] }
[target.'cfg(unix)'.build-dependencies]
baz = "3"
[package]
name = "ignored"
"#;
		let dependencies = extract_dependencies(source);
		assert_eq!(dependencies.len(), 3);
		assert_eq!(dependencies[1].name, "bar");
		assert_eq!(dependencies[1].version_requirement, "2.0");
	}

	#[test]
	fn decodes_the_crates_io_response_shape() {
		let response: CrateResponse =
			serde_json::from_str(r#"{"crate":{"max_version":"0.0.342"}}"#)
				.expect("crates.io response should decode");
		assert_eq!(response.krate.max_version, "0.0.342");
	}
}
