//! Transparent product routing for ket (PRODUCT-0004).

#![cfg_attr(not(target_os = "linux"), allow(dead_code))]

use std::ffi::{OsStr, OsString};
use std::fmt;
use std::fs::{self, OpenOptions};
use std::io::{self, Write};
use std::path::{Path, PathBuf};
use std::process::Command;
use std::sync::atomic::{AtomicU64, Ordering};

#[cfg(target_os = "linux")]
use std::os::linux::fs::MetadataExt;
#[cfg(target_os = "linux")]
use std::os::unix::fs::PermissionsExt;
#[cfg(target_os = "linux")]
use std::os::unix::process::CommandExt;

const CONFIG_DIRECTORY: &str = "ket";
const CONFIG_FILE: &str = "router.conf";
const CONFIG_VERSION: &str = "1";
const FACTORY_DEFAULT: DefaultTarget = DefaultTarget::Baryon;

static TEMP_SEQUENCE: AtomicU64 = AtomicU64::new(0);

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Route {
	Baryon,
	Polariton,
	Nilket,
	Toron,
	Wendelon,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DefaultTarget {
	Baryon,
	Polariton,
	Nilket,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
struct RouteSpec {
	route: Route,
	name: &'static str,
	leaf: &'static str,
	fixed_prefix: &'static [&'static str],
}

// PRODUCT-0004: this is the single authority for explicit route grammar.
const ROUTE_SPECS: [RouteSpec; 5] = [
	RouteSpec {
		route: Route::Baryon,
		name: "baryon",
		leaf: "baryon",
		fixed_prefix: &[],
	},
	RouteSpec {
		route: Route::Polariton,
		name: "polariton",
		leaf: "polariton",
		fixed_prefix: &[],
	},
	RouteSpec {
		route: Route::Nilket,
		name: "nilket",
		leaf: "nilket",
		fixed_prefix: &[],
	},
	RouteSpec {
		route: Route::Toron,
		name: "toron",
		leaf: "nilket",
		fixed_prefix: &["toron"],
	},
	RouteSpec {
		route: Route::Wendelon,
		name: "wendelon",
		leaf: "nilket",
		fixed_prefix: &["wendelon"],
	},
];

impl Route {
	fn spec(self) -> &'static RouteSpec {
		ROUTE_SPECS
			.iter()
			.find(|spec| spec.route == self)
			.expect("every Route must have exactly one ROUTE_SPECS entry")
	}
}

impl DefaultTarget {
	fn parse(value: &str) -> Result<Self, RouterError> {
		match value {
			"baryon" => Ok(Self::Baryon),
			"polariton" => Ok(Self::Polariton),
			"nilket" => Ok(Self::Nilket),
			"toron" | "wendelon" => Err(RouterError::new(format!(
				"{value} is an expert role and cannot be the bare ket default; use ket {value} explicitly"
			))),
			_ => Err(RouterError::new(format!(
				"unknown default target {value:?}; expected baryon, polariton, or nilket"
			))),
		}
	}

	fn as_str(self) -> &'static str {
		match self {
			Self::Baryon => "baryon",
			Self::Polariton => "polariton",
			Self::Nilket => "nilket",
		}
	}

	fn route(self) -> Route {
		match self {
			Self::Baryon => Route::Baryon,
			Self::Polariton => Route::Polariton,
			Self::Nilket => Route::Nilket,
		}
	}
}

#[derive(Debug)]
pub struct RouterError {
	message: String,
}

impl RouterError {
	fn new(message: impl Into<String>) -> Self {
		Self {
			message: message.into(),
		}
	}
}

impl fmt::Display for RouterError {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(&self.message)
	}
}

impl std::error::Error for RouterError {}

#[derive(Debug, Eq, PartialEq)]
enum RouterCommand {
	Help,
	Version,
	ShowDefault,
	SetDefault(DefaultTarget),
	ResetDefault,
	ExplicitRoute { route: Route, args: Vec<OsString> },
	DefaultRoute { args: Vec<OsString> },
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum DefaultSource {
	Factory,
	Override,
}

#[derive(Debug, Eq, PartialEq)]
struct EffectiveDefault {
	target: DefaultTarget,
	source: DefaultSource,
}

#[derive(Debug)]
struct Invocation {
	route: Route,
	program: PathBuf,
	args: Vec<OsString>,
}

pub fn run<I>(args: I) -> Result<(), RouterError>
where
	I: IntoIterator<Item = OsString>,
{
	let mut args = args.into_iter();
	let _program = args.next();
	let command = parse_command(args.collect())?;

	match command {
		RouterCommand::Help => {
			print!("{}", help_text());
			Ok(())
		}
		RouterCommand::Version => {
			println!("ket {}", env!("CARGO_PKG_VERSION"));
			Ok(())
		}
		RouterCommand::ShowDefault => {
			let path = required_config_path()?;
			let effective = effective_default_at(&path)?;
			println!(
				"default={} source={} path={}",
				effective.target.as_str(),
				match effective.source {
					DefaultSource::Factory => "factory",
					DefaultSource::Override => "override",
				},
				path.display()
			);
			Ok(())
		}
		RouterCommand::SetDefault(target) => {
			let path = required_config_path()?;
			set_default_at(&path, target)?;
			println!("default={} path={}", target.as_str(), path.display());
			Ok(())
		}
		RouterCommand::ResetDefault => {
			let path = required_config_path()?;
			reset_default_at(&path)?;
			println!(
				"default={} source=factory path={}",
				FACTORY_DEFAULT.as_str(),
				path.display()
			);
			Ok(())
		}
		RouterCommand::ExplicitRoute { route, args } => execute_route(route, args),
		RouterCommand::DefaultRoute { args } => {
			let target = match config_path_from_environment() {
				Some(path) => effective_default_at(&path)?.target,
				None => FACTORY_DEFAULT,
			};
			execute_route(target.route(), args)
		}
	}
}

fn parse_command(args: Vec<OsString>) -> Result<RouterCommand, RouterError> {
	let Some(first) = args.first() else {
		return Ok(RouterCommand::DefaultRoute { args });
	};

	if first == "--help" {
		return require_no_tail(&args, RouterCommand::Help, "--help");
	}
	if first == "--version" {
		return require_no_tail(&args, RouterCommand::Version, "--version");
	}
	if first == "default" {
		return parse_default_command(&args[1..]);
	}
	if let Some(spec) = route_spec_named(first) {
		return Ok(RouterCommand::ExplicitRoute {
			route: spec.route,
			args: args[1..].to_vec(),
		});
	}

	Ok(RouterCommand::DefaultRoute { args })
}

fn require_no_tail(
	args: &[OsString],
	command: RouterCommand,
	name: &str,
) -> Result<RouterCommand, RouterError> {
	if args.len() == 1 {
		Ok(command)
	} else {
		Err(RouterError::new(format!(
			"{name} does not accept arguments; use an explicit route to pass it to a target"
		)))
	}
}

fn parse_default_command(args: &[OsString]) -> Result<RouterCommand, RouterError> {
	match args {
		[] => Ok(RouterCommand::ShowDefault),
		[value] if value == "reset" => Ok(RouterCommand::ResetDefault),
		[value] => {
			let value = value.to_str().ok_or_else(|| {
				RouterError::new("the default target must be UTF-8: baryon, polariton, or nilket")
			})?;
			Ok(RouterCommand::SetDefault(DefaultTarget::parse(value)?))
		}
		_ => Err(RouterError::new(
			"usage: ket default [baryon|polariton|nilket|reset]",
		)),
	}
}

fn route_spec_named(name: &OsStr) -> Option<&'static RouteSpec> {
	ROUTE_SPECS.iter().find(|spec| name == spec.name)
}

fn help_text() -> String {
	let mut text = String::from(
		"Usage: ket [ROUTE] [ARGS...]\n\nWith no route, launch the configured default (factory: baryon).\n\nRoutes:\n",
	);
	for spec in ROUTE_SPECS {
		let description = match spec.route {
			Route::Baryon => "focused editor",
			Route::Polariton => "data workbench",
			Route::Nilket => "database deployment",
			Route::Toron => "expert compute role (through nilket)",
			Route::Wendelon => "expert storage role (through nilket)",
		};
		text.push_str(&format!("  {:<11} {description}\n", spec.name));
	}
	text.push_str(
		"\nConfiguration:\n  default                     Show the effective default\n  default baryon|polariton|nilket\n                              Set the default\n  default reset               Restore factory baryon\n",
	);
	text
}

fn config_path_from_environment() -> Option<PathBuf> {
	config_path_from(
		std::env::var_os("XDG_CONFIG_HOME"),
		std::env::var_os("HOME"),
	)
}

fn config_path_from(xdg: Option<OsString>, home: Option<OsString>) -> Option<PathBuf> {
	let base = xdg
		.filter(|value| !value.is_empty())
		.map(PathBuf::from)
		.or_else(|| {
			home.filter(|value| !value.is_empty())
				.map(|value| PathBuf::from(value).join(".config"))
		})?;
	Some(base.join(CONFIG_DIRECTORY).join(CONFIG_FILE))
}

fn required_config_path() -> Result<PathBuf, RouterError> {
	config_path_from_environment().ok_or_else(|| {
		RouterError::new("cannot resolve persistent configuration: set XDG_CONFIG_HOME or HOME")
	})
}

fn effective_default_at(path: &Path) -> Result<EffectiveDefault, RouterError> {
	match read_default_at(path)? {
		Some(target) => Ok(EffectiveDefault {
			target,
			source: DefaultSource::Override,
		}),
		None => Ok(EffectiveDefault {
			target: FACTORY_DEFAULT,
			source: DefaultSource::Factory,
		}),
	}
}

fn read_default_at(path: &Path) -> Result<Option<DefaultTarget>, RouterError> {
	let metadata = match fs::symlink_metadata(path) {
		Ok(metadata) => metadata,
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Ok(None),
		Err(error) => return Err(path_error("inspect", path, error)),
	};
	if metadata.file_type().is_symlink() || !metadata.file_type().is_file() {
		return Err(RouterError::new(format!(
			"configuration {} must be a regular, non-symlink file",
			path.display()
		)));
	}
	let bytes = fs::read(path).map_err(|error| path_error("read", path, error))?;
	parse_config(&bytes).map(Some)
}

fn parse_config(bytes: &[u8]) -> Result<DefaultTarget, RouterError> {
	let text = std::str::from_utf8(bytes)
		.map_err(|_| RouterError::new("router configuration must be UTF-8"))?;
	if !text.ends_with('\n') {
		return Err(RouterError::new(
			"router configuration must end with a newline",
		));
	}
	let lines: Vec<&str> = text[..text.len() - 1].split('\n').collect();
	if lines.len() != 2 {
		return Err(RouterError::new(
			"router configuration must contain exactly version and default lines",
		));
	}
	let version = parse_config_line(lines[0], "version")?;
	if version != CONFIG_VERSION {
		return Err(RouterError::new(format!(
			"unsupported router configuration version {version:?}; supported version is {CONFIG_VERSION}"
		)));
	}
	let target = parse_config_line(lines[1], "default")?;
	DefaultTarget::parse(target)
}

fn parse_config_line<'a>(line: &'a str, expected_key: &str) -> Result<&'a str, RouterError> {
	let Some((key, value)) = line.split_once('=') else {
		return Err(RouterError::new(format!(
			"router configuration line must be {expected_key}=VALUE"
		)));
	};
	if key != expected_key || value.is_empty() || value.contains('=') {
		return Err(RouterError::new(format!(
			"router configuration expected {expected_key}=VALUE, found {line:?}"
		)));
	}
	Ok(value)
}

fn render_config(target: DefaultTarget) -> String {
	format!("version={CONFIG_VERSION}\ndefault={}\n", target.as_str())
}

fn set_default_at(path: &Path, target: DefaultTarget) -> Result<(), RouterError> {
	let _existing = read_default_at(path)?;
	let parent = path
		.parent()
		.ok_or_else(|| RouterError::new("router configuration path has no parent"))?;
	fs::create_dir_all(parent).map_err(|error| path_error("create", parent, error))?;

	let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
	let temp = parent.join(format!(
		".{CONFIG_FILE}.tmp-{}-{sequence}",
		std::process::id()
	));
	publish_default_at(path, target, &temp)
}

fn publish_default_at(path: &Path, target: DefaultTarget, temp: &Path) -> Result<(), RouterError> {
	let mut file = OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(temp)
		.map_err(|error| path_error("create", temp, error))?;
	// Arm cleanup only after create_new proves this process owns the file.
	let mut cleanup = TempFileCleanup::new(temp.to_path_buf());
	file.write_all(render_config(target).as_bytes())
		.map_err(|error| path_error("write", temp, error))?;
	file.sync_all()
		.map_err(|error| path_error("flush", temp, error))?;
	drop(file);
	fs::rename(temp, path).map_err(|error| path_error("publish", path, error))?;
	cleanup.disarm();
	Ok(())
}

fn reset_default_at(path: &Path) -> Result<(), RouterError> {
	if read_default_at(path)?.is_none() {
		return Ok(());
	}
	fs::remove_file(path).map_err(|error| path_error("reset", path, error))
}

struct TempFileCleanup {
	path: PathBuf,
	armed: bool,
}

impl TempFileCleanup {
	fn new(path: PathBuf) -> Self {
		Self { path, armed: true }
	}

	fn disarm(&mut self) {
		self.armed = false;
	}
}

impl Drop for TempFileCleanup {
	fn drop(&mut self) {
		if self.armed {
			let _ = fs::remove_file(&self.path);
		}
	}
}

fn path_error(action: &str, path: &Path, error: io::Error) -> RouterError {
	RouterError::new(format!("cannot {action} {}: {error}", path.display()))
}

#[cfg(target_os = "linux")]
fn execute_route(route: Route, args: Vec<OsString>) -> Result<(), RouterError> {
	let spec = route.spec();
	let current_exe = std::env::current_exe().map_err(|error| {
		RouterError::new(format!("cannot resolve running ket executable: {error}"))
	})?;
	let program = resolve_leaf(spec, &current_exe, std::env::var_os("PATH"))?;
	let invocation = Invocation {
		route,
		program,
		args,
	};
	let mut command = build_command(&invocation);
	let error = command.exec();
	Err(RouterError::new(format!(
		"failed to exec {} route at {}: {error}",
		invocation.route.spec().name,
		invocation.program.display()
	)))
}

#[cfg(not(target_os = "linux"))]
fn execute_route(_route: Route, _args: Vec<OsString>) -> Result<(), RouterError> {
	Err(RouterError::new(
		"PRODUCT-0004 supports transparent process replacement on Linux only in v1",
	))
}

fn build_command(invocation: &Invocation) -> Command {
	let spec = invocation.route.spec();
	let mut command = Command::new(&invocation.program);
	command.args(spec.fixed_prefix.iter().copied());
	command.args(&invocation.args);
	command
}

#[cfg(target_os = "linux")]
fn resolve_leaf(
	spec: &RouteSpec,
	current_exe: &Path,
	path: Option<OsString>,
) -> Result<PathBuf, RouterError> {
	let current_metadata = fs::metadata(current_exe)
		.map_err(|error| path_error("inspect running ket executable", current_exe, error))?;
	let parent = current_exe.parent().ok_or_else(|| {
		RouterError::new(format!(
			"running ket executable has no parent: {}",
			current_exe.display()
		))
	})?;
	let sibling = parent.join(spec.leaf);
	match inspect_candidate(&sibling, &current_metadata) {
		Candidate::Absent => {}
		Candidate::Valid => return Ok(sibling),
		Candidate::SelfCycle => {
			return Err(RouterError::new(format!(
				"{} route resolves sibling {} back to ket itself",
				spec.name,
				sibling.display()
			)));
		}
		Candidate::Invalid(reason) => {
			return Err(RouterError::new(format!(
				"{} route found invalid coherent sibling {}: {reason}; refusing PATH fallback",
				spec.name,
				sibling.display()
			)));
		}
	}

	let mut rejected = Vec::new();
	if let Some(path) = path.filter(|value| !value.is_empty()) {
		for directory in std::env::split_paths(&path) {
			let candidate = directory.join(spec.leaf);
			match inspect_candidate(&candidate, &current_metadata) {
				Candidate::Absent => {}
				Candidate::Valid => return Ok(candidate),
				Candidate::SelfCycle => {
					return Err(RouterError::new(format!(
						"{} route resolves PATH candidate {} back to ket itself",
						spec.name,
						candidate.display()
					)));
				}
				Candidate::Invalid(reason) => {
					rejected.push(format!("{} ({reason})", candidate.display()));
				}
			}
		}
	}

	let rejected = if rejected.is_empty() {
		String::new()
	} else {
		format!("; rejected PATH candidates: {}", rejected.join(", "))
	};
	Err(RouterError::new(format!(
		"{} route requires executable {:?}; no sibling at {} and no usable PATH candidate{rejected}. Install {} or place it beside ket/on PATH",
		spec.name,
		spec.leaf,
		sibling.display(),
		spec.leaf
	)))
}

#[cfg(target_os = "linux")]
enum Candidate {
	Absent,
	Valid,
	SelfCycle,
	Invalid(String),
}

#[cfg(target_os = "linux")]
fn inspect_candidate(path: &Path, current: &fs::Metadata) -> Candidate {
	match fs::symlink_metadata(path) {
		Ok(_) => {}
		Err(error) if error.kind() == io::ErrorKind::NotFound => return Candidate::Absent,
		Err(error) => return Candidate::Invalid(error.to_string()),
	}
	let metadata = match fs::metadata(path) {
		Ok(metadata) => metadata,
		Err(error) => return Candidate::Invalid(error.to_string()),
	};
	if !metadata.is_file() {
		return Candidate::Invalid("not a regular file".to_owned());
	}
	if metadata.st_dev() == current.st_dev() && metadata.st_ino() == current.st_ino() {
		return Candidate::SelfCycle;
	}
	if metadata.permissions().mode() & 0o111 == 0 {
		return Candidate::Invalid("not executable".to_owned());
	}
	Candidate::Valid
}

#[cfg(all(test, target_os = "linux"))]
mod tests {
	use super::*;
	use std::os::unix::ffi::{OsStrExt, OsStringExt};
	use std::os::unix::fs::{PermissionsExt, symlink};

	fn os(value: &str) -> OsString {
		OsString::from(value)
	}

	fn temp_dir(label: &str) -> PathBuf {
		let sequence = TEMP_SEQUENCE.fetch_add(1, Ordering::Relaxed);
		let path =
			std::env::temp_dir().join(format!("ket-{label}-{}-{sequence}", std::process::id()));
		fs::create_dir(&path).expect("create isolated test directory");
		path
	}

	fn executable(path: &Path) {
		fs::write(path, b"fixture").unwrap();
		let mut permissions = fs::metadata(path).unwrap().permissions();
		permissions.set_mode(0o755);
		fs::set_permissions(path, permissions).unwrap();
	}

	#[test]
	fn route_table_is_complete_unique_and_role_honest() {
		assert_eq!(ROUTE_SPECS.len(), 5);
		for (index, left) in ROUTE_SPECS.iter().enumerate() {
			assert!(
				ROUTE_SPECS[index + 1..]
					.iter()
					.all(|right| left.route != right.route && left.name != right.name)
			);
		}
		assert_eq!(Route::Toron.spec().leaf, "nilket");
		assert_eq!(Route::Toron.spec().fixed_prefix, &["toron"]);
		assert_eq!(Route::Wendelon.spec().leaf, "nilket");
		assert_eq!(Route::Wendelon.spec().fixed_prefix, &["wendelon"]);
	}

	#[test]
	fn grammar_routes_explicit_and_unknown_arguments_to_the_default() {
		assert_eq!(
			parse_command(vec![os("polariton"), os("file.sql")]).unwrap(),
			RouterCommand::ExplicitRoute {
				route: Route::Polariton,
				args: vec![os("file.sql")],
			}
		);
		assert_eq!(
			parse_command(vec![os("file.rs"), os("+20")]).unwrap(),
			RouterCommand::DefaultRoute {
				args: vec![os("file.rs"), os("+20")],
			}
		);
		assert!(matches!(
			parse_command(vec![os("--help")]).unwrap(),
			RouterCommand::Help
		));
		assert!(parse_command(vec![os("--help"), os("extra")]).is_err());
	}

	#[test]
	fn default_eligibility_refuses_roles_but_explicit_routes_remain() {
		for allowed in ["baryon", "polariton", "nilket"] {
			assert!(DefaultTarget::parse(allowed).is_ok());
		}
		for role in ["toron", "wendelon"] {
			assert!(DefaultTarget::parse(role).is_err());
			assert!(route_spec_named(OsStr::new(role)).is_some());
		}
	}

	#[test]
	fn config_path_obeys_xdg_then_home_and_empty_xdg_is_unset() {
		assert_eq!(
			config_path_from(Some(os("/x")), Some(os("/h"))).unwrap(),
			PathBuf::from("/x/ket/router.conf")
		);
		assert_eq!(
			config_path_from(Some(OsString::new()), Some(os("/h"))).unwrap(),
			PathBuf::from("/h/.config/ket/router.conf")
		);
		assert!(config_path_from(None, None).is_none());
	}

	#[test]
	fn visible_default_round_trips_and_reset_is_idempotent() {
		let root = temp_dir("config-roundtrip");
		let path = root.join("ket/router.conf");
		assert_eq!(
			effective_default_at(&path).unwrap().source,
			DefaultSource::Factory
		);
		for target in [
			DefaultTarget::Baryon,
			DefaultTarget::Polariton,
			DefaultTarget::Nilket,
		] {
			set_default_at(&path, target).unwrap();
			assert_eq!(read_default_at(&path).unwrap(), Some(target));
			assert_eq!(fs::read_to_string(&path).unwrap(), render_config(target));
		}
		reset_default_at(&path).unwrap();
		reset_default_at(&path).unwrap();
		assert_eq!(read_default_at(&path).unwrap(), None);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn failed_atomic_write_preserves_prior_config_and_foreign_temp() {
		let root = temp_dir("config-failed-write");
		let path = root.join("ket/router.conf");
		set_default_at(&path, DefaultTarget::Baryon).unwrap();
		let collision = root.join("ket/.router.conf.collision");
		fs::write(&collision, b"foreign").unwrap();

		assert!(publish_default_at(&path, DefaultTarget::Polariton, &collision).is_err());
		assert_eq!(read_default_at(&path).unwrap(), Some(DefaultTarget::Baryon));
		assert_eq!(fs::read(&collision).unwrap(), b"foreign");
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn malformed_configuration_never_becomes_factory_default_or_resettable() {
		let malformed = [
			b"".as_slice(),
			b"version=1\ndefault=baryon",
			b"version =1\ndefault=baryon\n",
			b"version=2\ndefault=baryon\n",
			b"version=1\ndefault=toron\n",
			b"version=1\ndefault=baryon\nextra=x\n",
			b"version=1\nversion=1\n",
			b"\xff\n",
		];
		for (index, bytes) in malformed.into_iter().enumerate() {
			let root = temp_dir(&format!("config-bad-{index}"));
			let path = root.join("router.conf");
			fs::write(&path, bytes).unwrap();
			assert!(effective_default_at(&path).is_err());
			assert!(reset_default_at(&path).is_err());
			assert!(path.exists(), "refusal must preserve evidence");
			fs::remove_dir_all(root).unwrap();
		}
	}

	#[test]
	fn configuration_symlinks_and_non_files_refuse() {
		let root = temp_dir("config-types");
		let real = root.join("real");
		fs::write(&real, render_config(DefaultTarget::Baryon)).unwrap();
		let link = root.join("link");
		symlink(&real, &link).unwrap();
		assert!(read_default_at(&link).is_err());
		assert!(read_default_at(&root).is_err());
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn route_command_preserves_non_utf8_arguments_and_role_prefixes() {
		let raw = OsString::from_vec(vec![b'a', 0x80, b'z']);
		let invocation = Invocation {
			route: Route::Toron,
			program: PathBuf::from("/tmp/nilket"),
			args: vec![raw.clone(), os("tail")],
		};
		let command = build_command(&invocation);
		let actual: Vec<Vec<u8>> = command
			.get_args()
			.map(|arg| arg.as_bytes().to_vec())
			.collect();
		assert_eq!(
			actual,
			vec![b"toron".to_vec(), raw.as_bytes().to_vec(), b"tail".to_vec()]
		);
	}

	#[test]
	fn sibling_precedes_path_and_invalid_sibling_refuses_fallback() {
		let root = temp_dir("resolution");
		let sibling_dir = root.join("sibling");
		let path_dir = root.join("path");
		fs::create_dir_all(&sibling_dir).unwrap();
		fs::create_dir_all(&path_dir).unwrap();
		let current = sibling_dir.join("ket");
		executable(&current);
		executable(&sibling_dir.join("baryon"));
		executable(&path_dir.join("baryon"));
		assert_eq!(
			resolve_leaf(
				Route::Baryon.spec(),
				&current,
				Some(path_dir.clone().into())
			)
			.unwrap(),
			sibling_dir.join("baryon")
		);

		let mut permissions = fs::metadata(sibling_dir.join("baryon"))
			.unwrap()
			.permissions();
		permissions.set_mode(0o644);
		fs::set_permissions(sibling_dir.join("baryon"), permissions).unwrap();
		let error = resolve_leaf(Route::Baryon.spec(), &current, Some(path_dir.into()))
			.unwrap_err()
			.to_string();
		assert!(error.contains("refusing PATH fallback"));
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn absent_sibling_uses_path_and_missing_target_is_honest() {
		let root = temp_dir("path-fallback");
		let sibling_dir = root.join("sibling");
		let path_dir = root.join("path");
		fs::create_dir_all(&sibling_dir).unwrap();
		fs::create_dir_all(&path_dir).unwrap();
		let current = sibling_dir.join("ket");
		executable(&current);
		executable(&path_dir.join("polariton"));
		assert_eq!(
			resolve_leaf(
				Route::Polariton.spec(),
				&current,
				Some(path_dir.clone().into())
			)
			.unwrap(),
			path_dir.join("polariton")
		);
		let error = resolve_leaf(Route::Baryon.spec(), &current, Some(path_dir.into()))
			.unwrap_err()
			.to_string();
		assert!(error.contains("Install baryon"));
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn direct_and_symlinked_self_cycles_refuse() {
		let root = temp_dir("self-cycle");
		let current = root.join("ket");
		executable(&current);
		fs::hard_link(&current, root.join("baryon")).unwrap();
		assert!(
			resolve_leaf(Route::Baryon.spec(), &current, None)
				.unwrap_err()
				.to_string()
				.contains("back to ket itself")
		);
		fs::remove_file(root.join("baryon")).unwrap();
		symlink(&current, root.join("baryon")).unwrap();
		assert!(
			resolve_leaf(Route::Baryon.spec(), &current, None)
				.unwrap_err()
				.to_string()
				.contains("back to ket itself")
		);
		fs::remove_dir_all(root).unwrap();
	}

	#[test]
	fn help_is_generated_from_every_route() {
		let help = help_text();
		for spec in ROUTE_SPECS {
			assert!(help.contains(spec.name));
		}
	}
}
