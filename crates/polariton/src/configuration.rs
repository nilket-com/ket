//! Rune-only configuration for Polariton-owned preferences.
//!
//! `~/.config/polariton/init.rn` is user-authored code, never machine-written
//! state. Missing or broken configuration cannot prevent the application from
//! opening: compiled defaults remain in force, with any operations completed
//! before a runtime error retained for the session.

use rune::alloc::limit;
use rune::runtime::budget;
use rune::runtime::{Value, VmResult};
use rune::{Any, Context, Module, Source, Sources, Vm};
use std::env;
use std::ffi::OsString;
use std::io::Read;
use std::path::{Path, PathBuf};
use std::sync::{Arc, Mutex};

const MAX_INIT_BYTES: usize = 4 * 1024 * 1024;
const COMPILE_ALLOCATION_CAP: usize = 256 * 1024 * 1024;
const STARTUP_ALLOCATION_CAP: usize = 256 * 1024 * 1024;
const STARTUP_INSTRUCTION_BUDGET: usize = 50_000_000;
const DEFAULT_WINDOW_SIZE: (f32, f32) = (1920.0, 1080.0);

#[derive(Clone, Debug, PartialEq)]
pub struct StartupData {
	pub window_size: (f32, f32),
	pub show_column_types: bool,
	pub init_error: Option<String>,
}

impl Default for StartupData {
	fn default() -> Self {
		Self {
			window_size: DEFAULT_WINDOW_SIZE,
			show_column_types: false,
			init_error: None,
		}
	}
}

#[derive(Any, Clone)]
#[rune(item = ::polariton, name = Config)]
struct RuneConfig {
	startup: Arc<Mutex<StartupData>>,
}

/// Resolve Polariton's sole durable, user-authored configuration source.
pub fn default_init_path() -> Option<PathBuf> {
	config_dir(SettingsPathEnv::current()).map(|path| path.join("polariton").join("init.rn"))
}

/// Load the default startup script. Configuration failures are data rather
/// than process failures so the GUI can open and present the diagnostic.
pub fn load_startup_data() -> StartupData {
	default_init_path().map_or_else(StartupData::default, |path| load_from_path(&path))
}

fn load_from_path(path: &Path) -> StartupData {
	let mut startup = StartupData::default();
	let source = match read_source(path) {
		Ok(Some(source)) => source,
		Ok(None) => return startup,
		Err(error) => {
			startup.init_error = Some(error);
			return startup;
		}
	};
	let shared = Arc::new(Mutex::new(startup));
	let result = execute_init(path, source, shared.clone());
	let mut startup = shared
		.lock()
		.map(|startup| startup.clone())
		.unwrap_or_default();
	if let Err(error) = result {
		startup.init_error = Some(error);
	}
	startup
}

fn read_source(path: &Path) -> Result<Option<String>, String> {
	let file = match std::fs::File::open(path) {
		Ok(file) => file,
		Err(error) if error.kind() == std::io::ErrorKind::NotFound => return Ok(None),
		Err(error) => {
			return Err(format!("Could not read {}: {error}", path.display()));
		}
	};
	let mut bytes = Vec::with_capacity(MAX_INIT_BYTES.min(64 * 1024));
	file.take((MAX_INIT_BYTES + 1) as u64)
		.read_to_end(&mut bytes)
		.map_err(|error| format!("Could not read {}: {error}", path.display()))?;
	if bytes.len() > MAX_INIT_BYTES {
		return Err(format!(
			"{} exceeds the 4 MiB init.rn limit",
			path.display()
		));
	}
	String::from_utf8(bytes)
		.map(Some)
		.map_err(|_| format!("{} is not valid UTF-8", path.display()))
}

fn execute_init(
	path: &Path,
	source: String,
	startup: Arc<Mutex<StartupData>>,
) -> Result<(), String> {
	let context = limit::with(COMPILE_ALLOCATION_CAP, build_context).call()?;
	let runtime = Arc::new(
		context
			.runtime()
			.map_err(|error| format!("init.rn runtime context error: {error}"))?,
	);
	let mut sources = Sources::new();
	sources
		.insert(Source::new(path.to_string_lossy(), source).map_err(|error| error.to_string())?)
		.map_err(|error| error.to_string())?;
	let unit = limit::with(COMPILE_ALLOCATION_CAP, || {
		rune::prepare(&mut sources)
			.with_context(&context)
			.build()
			.map_err(|error| format!("init.rn compile error: {error}"))
	})
	.call()?;
	let mut vm = Vm::new(runtime, Arc::new(unit));
	let value = limit::with(STARTUP_ALLOCATION_CAP, || {
		budget::with(STARTUP_INSTRUCTION_BUDGET, || {
			vm.call(["main"], (RuneConfig { startup },))
		})
		.call()
		.map_err(|error| format!("init.rn runtime error: {error}"))
	})
	.call()?;
	decode_return(value)
}

fn build_context() -> Result<Context, String> {
	let mut context =
		Context::with_config(false).map_err(|error| format!("Rune context error: {error}"))?;
	let mut module =
		Module::with_crate("polariton").map_err(|error| format!("Rune module error: {error}"))?;
	module
		.ty::<RuneConfig>()
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("show_column_types", config_show_column_types)
		.map_err(|error| format!("Rune module error: {error}"))?;
	module
		.associated_function("window_size", config_window_size)
		.map_err(|error| format!("Rune module error: {error}"))?;
	context
		.install(module)
		.map_err(|error| format!("Rune context error: {error}"))?;
	Ok(context)
}

fn config_show_column_types(config: &RuneConfig, value: Value) -> VmResult<Result<(), String>> {
	let value = match value.as_bool() {
		Ok(value) => value,
		Err(_) => {
			return VmResult::Ok(Err("show_column_types expects a boolean".to_owned()));
		}
	};
	VmResult::Ok(with_startup(config, |startup| {
		startup.show_column_types = value;
	}))
}

fn config_window_size(
	config: &RuneConfig,
	width: Value,
	height: Value,
) -> VmResult<Result<(), String>> {
	let width = match dimension(&width, "window width") {
		Ok(width) => width,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	let height = match dimension(&height, "window height") {
		Ok(height) => height,
		Err(error) => return VmResult::Ok(Err(error)),
	};
	VmResult::Ok(with_startup(config, |startup| {
		startup.window_size = (width, height);
	}))
}

fn dimension(value: &Value, name: &str) -> Result<f32, String> {
	let value = value
		.as_signed()
		.ok()
		.and_then(|value| u32::try_from(value).ok())
		.or_else(|| {
			value
				.as_unsigned()
				.ok()
				.and_then(|value| u32::try_from(value).ok())
		})
		.filter(|value| *value > 0)
		.ok_or_else(|| format!("{name} expects a positive integer"))?;
	Ok(value as f32)
}

fn with_startup(config: &RuneConfig, update: impl FnOnce(&mut StartupData)) -> Result<(), String> {
	let mut startup = config
		.startup
		.lock()
		.map_err(|_| "Polariton config lock was poisoned".to_owned())?;
	update(&mut startup);
	Ok(())
}

fn decode_return(value: Value) -> Result<(), String> {
	let original = value.clone();
	match value.downcast::<Result<Value, Value>>() {
		Ok(Ok(value)) => expect_unit(value),
		Ok(Err(error)) => match error.into_string() {
			Ok(message) => Err(format!("init.rn error: {message}")),
			Err(_) => Err("init.rn returned a non-string error".to_owned()),
		},
		Err(_) => expect_unit(original),
	}
}

fn expect_unit(value: Value) -> Result<(), String> {
	value
		.into_unit()
		.map_err(|_| "init.rn main must return unit or Result<(), String>".to_owned())
}

fn config_dir(env: SettingsPathEnv) -> Option<PathBuf> {
	if cfg!(target_os = "windows") {
		non_empty_path(env.appdata).or_else(|| {
			non_empty_path(env.userprofile).map(|home| home.join("AppData").join("Roaming"))
		})
	} else if cfg!(target_os = "macos") {
		non_empty_path(env.home).map(|home| home.join("Library").join("Application Support"))
	} else {
		non_empty_path(env.xdg_config_home)
			.or_else(|| non_empty_path(env.home).map(|home| home.join(".config")))
	}
}

#[derive(Debug, Default)]
struct SettingsPathEnv {
	xdg_config_home: Option<OsString>,
	home: Option<OsString>,
	appdata: Option<OsString>,
	userprofile: Option<OsString>,
}

impl SettingsPathEnv {
	fn current() -> Self {
		Self {
			xdg_config_home: env::var_os("XDG_CONFIG_HOME"),
			home: env::var_os("HOME"),
			appdata: env::var_os("APPDATA"),
			userprofile: env::var_os("USERPROFILE"),
		}
	}
}

fn non_empty_path(value: Option<OsString>) -> Option<PathBuf> {
	value.filter(|value| !value.is_empty()).map(PathBuf::from)
}

#[cfg(test)]
mod tests {
	use super::{SettingsPathEnv, StartupData, config_dir, load_from_path};
	use std::ffi::OsString;
	use std::path::PathBuf;
	use std::sync::atomic::{AtomicU64, Ordering};

	fn temp_path(label: &str) -> PathBuf {
		static NEXT: AtomicU64 = AtomicU64::new(1);
		std::env::temp_dir().join(format!(
			"polariton-{label}-{}-{}.rn",
			std::process::id(),
			NEXT.fetch_add(1, Ordering::Relaxed),
		))
	}

	#[test]
	fn missing_init_uses_compiled_defaults_without_creating_a_file() {
		let path = temp_path("missing-init");
		let startup = load_from_path(&path);
		assert_eq!(startup, StartupData::default());
		assert!(!path.exists());
	}

	#[test]
	#[cfg(all(not(target_os = "windows"), not(target_os = "macos")))]
	fn xdg_config_home_owns_the_linux_init_location() {
		let directory = config_dir(SettingsPathEnv {
			xdg_config_home: Some(OsString::from("/tmp/polariton-xdg")),
			home: Some(OsString::from("/tmp/polariton-home")),
			..SettingsPathEnv::default()
		});
		assert_eq!(directory, Some(PathBuf::from("/tmp/polariton-xdg")));
	}

	#[test]
	fn init_configures_the_first_window_and_table_presentation() {
		let path = temp_path("configured-init");
		std::fs::write(
			&path,
			r#"
pub fn main(config) {
    config.window_size(
        1600,
        900,
    )?;
    config.show_column_types(true)?;
}
"#,
		)
		.expect("write init");

		let startup = load_from_path(&path);
		assert_eq!(startup.window_size, (1600.0, 900.0));
		assert!(startup.show_column_types);
		assert_eq!(startup.init_error, None);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn runtime_error_keeps_operations_that_already_completed() {
		let path = temp_path("partial-init");
		std::fs::write(
			&path,
			r#"
pub fn main(config) {
    config.show_column_types(true)?;
    config.window_size(0, 900)?;
}
"#,
		)
		.expect("write init");

		let startup = load_from_path(&path);
		assert!(startup.show_column_types);
		assert_eq!(startup.window_size, StartupData::default().window_size);
		assert!(
			startup
				.init_error
				.as_deref()
				.is_some_and(|error| error.contains("positive integer")),
		);
		let _ = std::fs::remove_file(path);
	}

	#[test]
	fn compile_error_is_reported_without_losing_defaults() {
		let path = temp_path("broken-init");
		std::fs::write(&path, "pub fn main(").expect("write init");

		let startup = load_from_path(&path);
		assert_eq!(startup.window_size, StartupData::default().window_size);
		assert!(!startup.show_column_types);
		assert!(
			startup
				.init_error
				.as_deref()
				.is_some_and(|error| error.contains("compile error")),
		);
		let _ = std::fs::remove_file(path);
	}
}
