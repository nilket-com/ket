use super::EditorCommand;
use super::core::Engine;
use super::language::{LanguageInfo, LanguageRegistry};
use super::request::{CommandSender, InputSender, InternalEvent, InternalSender};
use super::settings::EngineSettings;
use crate::core::expand_path;
use crate::ecs::registry::UastRegistry;
use crate::svp::bash_provider::BashProvider;
use crate::svp::csv_provider::{CsvProvider, TsvProvider};
use crate::svp::fs_provider::FsProvider;
use crate::svp::html_provider::HtmlProvider;
use crate::svp::json_provider::JsonProvider;
use crate::svp::markdown_provider::MarkdownProvider;
use crate::svp::python_provider::PythonProvider;
use crate::svp::resolver::SvpResolver;
use crate::svp::rune_provider::RuneProvider;
use crate::svp::rust_provider::RustProvider;
use crate::svp::sql_provider::SqlProvider;
use crate::svp::toml_provider::TomlProvider;
use crate::uast::projection::Viewport;
use std::path::PathBuf;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, mpsc};
use std::thread;

#[derive(Debug, Clone, Default, PartialEq, Eq)]
/// Settings and optional initial document used to start an engine instance.
pub struct EngineConfig {
	pub settings: EngineSettings,
	pub initial_file: Option<String>,
	/// Optional Rune startup script. Library embedders must opt in explicitly.
	pub init_script: Option<PathBuf>,
}

impl From<EngineSettings> for EngineConfig {
	fn from(settings: EngineSettings) -> Self {
		Self {
			settings,
			initial_file: None,
			init_script: None,
		}
	}
}

/// Owns a running engine thread and its command/projection channels.
///
/// A viewport receiver has one consumer, so [`Self::take_viewports`] transfers it to the host.
/// Call [`Self::shutdown`] for deterministic graceful shutdown. Dropping the handle requests a
/// non-destructive termination without waiting, preserving recovery state on abnormal host exits.
pub struct EngineHandle {
	commands: CommandSender,
	input: InputSender,
	internal: InternalSender,
	viewports: Option<mpsc::Receiver<Viewport>>,
	engine_thread: Option<thread::JoinHandle<()>>,
	bridge_thread: Option<thread::JoinHandle<()>>,
	terminate: Arc<AtomicBool>,
	terminate_on_drop: bool,
}

impl EngineHandle {
	pub fn spawn(config: EngineConfig) -> Self {
		let (commands, input, internal, rx_request) = super::request::channel();
		let (tx_view, rx_view) = mpsc::channel::<Viewport>();
		let (tx_io_notify, rx_io_notify) =
			mpsc::channel::<crate::svp::resolver::ResolverNotification>();

		// Component pages allocate lazily, so this is address-space headroom rather
		// than an up-front multi-million-node allocation.
		let registry = Arc::new(UastRegistry::new(16_000_000));
		let resident_mib = std::env::var("BARYON_RESIDENT_MIB")
			.ok()
			.and_then(|value| value.parse::<u64>().ok())
			.unwrap_or(64)
			.max(4);
		let resident_high = resident_mib.saturating_mul(1024 * 1024);
		registry.set_resident_budget(resident_high, resident_high.saturating_mul(3) / 4);
		let resolver = Arc::new(SvpResolver::new_with_notifications(
			registry.clone(),
			tx_io_notify,
		));
		let internal_bridge = internal.clone();
		let bridge_thread = thread::Builder::new()
			.name("svp-refresh-bridge".into())
			.spawn(move || {
				while let Ok(notification) = rx_io_notify.recv() {
					let event = match notification {
						crate::svp::resolver::ResolverNotification::Refresh => {
							InternalEvent::Refresh
						}
						crate::svp::resolver::ResolverNotification::HydrationCompleted(buffer) => {
							InternalEvent::HydrationCompleted(buffer)
						}
					};
					if internal_bridge.send(event).is_err() {
						break;
					}
				}
			})
			.expect("failed to spawn SVP refresh bridge");

		let lang_registry = Arc::new(build_language_registry());
		let EngineConfig {
			settings,
			initial_file,
			init_script,
		} = config;
		let initial_file = initial_file.map(|path| {
			let expanded = expand_path(&path);
			expanded.to_string_lossy().into_owned()
		});
		let engine = Engine::new(
			registry,
			resolver,
			rx_request,
			internal.clone(),
			tx_view,
			lang_registry,
			settings.theme_name,
			settings.minimap_enabled,
			settings.wrap_enabled,
			settings.line_numbers_enabled,
			settings.relative_line_numbers_enabled,
			settings.visible_whitespace,
			settings.reload_enabled,
			settings.save_durability,
			settings.ui_colors,
			settings.language_themes,
		)
		.with_initial_file(initial_file)
		.with_rune_host_config(init_script.map(super::rune_host::RuneHostConfig::new));
		let terminate = engine.termination_handle();
		let engine_thread = thread::spawn(move || engine.run());

		Self {
			commands,
			input,
			internal,
			viewports: Some(rx_view),
			engine_thread: Some(engine_thread),
			bridge_thread: Some(bridge_thread),
			terminate,
			terminate_on_drop: true,
		}
	}

	pub fn command_sender(&self) -> CommandSender {
		self.commands.clone()
	}

	pub fn input_sender(&self) -> InputSender {
		self.input.clone()
	}

	pub fn take_viewports(&mut self) -> Option<mpsc::Receiver<Viewport>> {
		self.viewports.take()
	}

	pub fn shutdown(mut self) -> thread::Result<()> {
		self.terminate_on_drop = false;
		let _ = self.commands.send(EditorCommand::Quit);
		let engine_result = self
			.engine_thread
			.take()
			.map_or(Ok(()), thread::JoinHandle::join);
		let bridge_result = self
			.bridge_thread
			.take()
			.map_or(Ok(()), thread::JoinHandle::join);
		engine_result.and(bridge_result)
	}

	/// Detach the engine without waiting for queued work.
	///
	/// The caller is responsible for having already requested termination. This matches the
	/// standalone TUI's historical process-owned engine behavior.
	pub fn detach(mut self) {
		self.terminate_on_drop = false;
	}
}

impl Drop for EngineHandle {
	fn drop(&mut self) {
		if self.terminate_on_drop {
			self.terminate.store(true, Ordering::Release);
			// A full queue already guarantees that the engine will wake. Never let
			// handle destruction wait for capacity merely to enqueue the wake-up.
			let _ = self.internal.try_send(InternalEvent::Refresh);
		}
	}
}

pub(crate) fn build_language_registry() -> LanguageRegistry {
	let mut registry = LanguageRegistry::new();
	registry.register(BashProvider);
	registry.register(RustProvider::new());
	registry.register(PythonProvider);
	registry.register(RuneProvider);
	registry.register(SqlProvider);
	registry.register(TomlProvider);
	registry.register(MarkdownProvider);
	registry.register(HtmlProvider);
	registry.register(JsonProvider);
	registry.register(CsvProvider);
	registry.register(TsvProvider);
	registry.register_internal(FsProvider);
	registry
}

/// Languages available to user documents in the default Baryon engine.
///
/// This is built from the same provider registrations used when the engine
/// starts, keeping front-end help pages synchronized with runtime support.
pub fn supported_languages() -> Vec<LanguageInfo> {
	build_language_registry().supported_languages()
}

#[cfg(test)]
mod tests {
	use super::{EngineConfig, EngineHandle, build_language_registry, supported_languages};
	use crate::core::DocLine;
	use crate::engine::shadow::{discard_shadow_session, load_shadow_session, shadow_dir_for_path};
	use crate::engine::{
		CommandRange, EditorCommand, InputEvent, KeyCode, Modifiers, SubstituteFlags,
	};
	use crate::uast::projection::Viewport;
	use std::fmt::Write as _;
	use std::fs;
	use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

	fn visible_text(viewport: &Viewport) -> Vec<u8> {
		viewport
			.focused_pane()
			.expect("focused pane")
			.tokens
			.iter()
			.flat_map(|token| token.text.iter().copied())
			.collect()
	}

	fn recv_viewport_until(
		viewports: &std::sync::mpsc::Receiver<Viewport>,
		description: &str,
		mut predicate: impl FnMut(&Viewport) -> bool,
	) -> Viewport {
		let deadline = Instant::now() + Duration::from_secs(20);
		loop {
			let viewport = viewports
				.recv_timeout(deadline.saturating_duration_since(Instant::now()))
				.unwrap_or_else(|_| panic!("timed out waiting for {description}"));
			if predicate(&viewport) {
				return viewport;
			}
			assert!(
				Instant::now() < deadline,
				"timed out waiting for {description}"
			);
		}
	}

	fn multi_extent_fixture(marker: &str, line_count: usize) -> Vec<u8> {
		let mut contents = String::with_capacity(line_count * 40);
		for line in 0..line_count {
			writeln!(&mut contents, "{marker}-{line:06}-{marker}").expect("write fixture line");
		}
		contents.into_bytes()
	}

	fn temp_path(name: &str, extension: &str) -> std::path::PathBuf {
		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system clock")
			.as_nanos();
		std::env::temp_dir().join(format!(
			"baryon-{name}-{}-{nonce}.{extension}",
			std::process::id()
		))
	}

	fn type_ex_command(input: &super::InputSender, command: &str) {
		for code in std::iter::once(KeyCode::Char(':'))
			.chain(command.chars().map(KeyCode::Char))
			.chain(std::iter::once(KeyCode::Enter))
		{
			input
				.send(InputEvent::Key {
					code,
					modifiers: Modifiers::NONE,
				})
				.expect("type ex command");
		}
	}

	#[test]
	fn default_registry_includes_bash_names() {
		let registry = build_language_registry();
		for name in ["bash", "sh", "bats"] {
			assert_eq!(registry.resolve_language_id(name), Some("bash"));
			assert!(registry.completion_candidates().contains(&name.to_string()));
		}
	}

	#[test]
	fn default_registry_exposes_only_document_languages() {
		let languages = supported_languages();
		assert!(languages.iter().any(|language| language.id == "rust"));
		assert!(languages.iter().any(|language| language.id == "markdown"));
		assert!(!languages.iter().any(|language| language.id == "fs"));
	}

	#[test]
	fn rune_init_output_is_projected_without_touching_stdout() {
		let init = temp_path("rune-init-output", "rn");
		fs::write(
			&init,
			"pub fn main(editor) { println!(\"hello from init.rn\"); }",
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("render initialized engine");
		let viewport = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("initialized viewport");
		assert_eq!(
			viewport.status_message.as_deref(),
			Some("hello from init.rn")
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_init_runs_before_the_initial_file_load() {
		let init = temp_path("rune-init-order", "rn");
		let initial_file = temp_path("rune-init-order-target", "txt");
		fs::write(&initial_file, "loaded afterward\n").expect("write initial file");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::test_assert_init_before_file(editor);
    println!("init-before-file");
}
"#,
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(initial_file.to_string_lossy().into_owned()),
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let viewport = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		assert_eq!(viewport.status_message.as_deref(), Some("init-before-file"));
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
		let _ = fs::remove_file(initial_file);
	}

	#[test]
	fn rune_buffer_handles_use_the_explicit_guarded_editor_abi() {
		let init = temp_path("rune-buffer-abi", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    let buffer = match baryon::focused_buffer(editor) {
        Ok(value) => value,
        Err(_) => panic("focused buffer"),
    };
    let view = match baryon::focused_view(editor) {
        Ok(value) => value,
        Err(_) => panic("focused view"),
    };
    let same = match view.buffer(editor) {
        Ok(value) => value,
        Err(_) => panic("view buffer"),
    };
    if buffer != same || buffer.clone() != same {
        panic("handle equality or clone failed");
    }
    let bytes = match buffer.total_bytes(editor) {
        Ok(value) => value,
        Err(_) => panic("total bytes"),
    };
    let version = match buffer.version(editor) {
        Ok(value) => value,
        Err(_) => panic("version"),
    };
    if bytes != 0 || version != version.clone() {
        panic("initial buffer contract failed");
    }
    let views = match buffer.views(editor) {
        Ok(value) => value,
        Err(_) => panic("buffer views"),
    };
    if views.len() != 1 {
        panic("buffer view traversal failed");
    }
    println!("buffer-abi-ok");
}
"#,
		)
		.expect("write Rune buffer ABI init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let viewport = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		assert_eq!(viewport.status_message.as_deref(), Some("buffer-abi-ok"));
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_buffer_reads_cursors_and_declarative_edits_run_end_to_end() {
		let init = temp_path("rune-buffer-data", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    let buffer = match baryon::focused_buffer(editor) {
        Ok(value) => value,
        Err(_) => panic("focused buffer"),
    };
    let view = match baryon::focused_view(editor) {
        Ok(value) => value,
        Err(_) => panic("focused view"),
    };
    match view.replace_text(editor, 1, 0, "x") {
        Err(error) => {
            match error.kind() {
                baryon::BufferErrorKind::Edit(baryon::EditErrorKind::RangeInvalid) => (),
                _ => panic("wrong invalid-range error kind"),
            }
            if error.detail() != Some(1) || error.maximum() != Some(0) {
                panic("invalid-range details were lost");
            }
        },
        Ok(_) => panic("reversed edit range was accepted"),
    }
    let before = match buffer.version(editor) {
        Ok(value) => value,
        Err(_) => panic("version before"),
    };
    let edits = [baryon::Edit::text(0, 0, "alpha\nbeta\n")];
    let after = match view.edit_if(editor, before.clone(), edits) {
        Ok(value) => value,
        Err(_) => panic("edit batch"),
    };
    if after == before {
        panic("edit did not advance the version");
    }
    match view.replace_text_if(editor, before.clone(), (0, 99), "stale") {
        Err(error) => match error.kind() {
            baryon::BufferErrorKind::StaleVersion => (),
            _ => panic("stale optimistic edit lost error precedence"),
        },
        Ok(_) => panic("stale optimistic edit was accepted"),
    }
    let text = match buffer.read_text(editor, 0, 11, baryon::OnInvalid::Strict) {
        Ok(value) => value,
        Err(_) => panic("strict text read"),
    };
    if text != "alpha\nbeta\n" {
        panic("text round trip");
    }
    let chunks = match buffer.read_chunks(editor, 0, 11, 5) {
        Ok(value) => value,
        Err(_) => panic("chunk cursor"),
    };
    let first = match chunks.next(editor) {
        Ok(Some(value)) => value,
        _ => panic("first chunk"),
    };
    if first.len() != 5 {
        panic("chunk size");
    }
    let lines = match buffer.lines(editor, 3) {
        Ok(value) => value,
        Err(_) => panic("line cursor"),
    };
    let first_line = match lines.next(editor) {
        Ok(Some(value)) => value,
        _ => panic("first line"),
    };
    if first_line.start != 0 || first_line.end != 6 || first_line.bytes.len() != 3 || !first_line.truncated {
        panic("line page");
    }
    match lines.next(editor) {
        Ok(Some(_)) => (),
        _ => panic("second line"),
    }
    let final_line = match lines.next(editor) {
        Ok(Some(value)) => value,
        _ => panic("trailing empty line"),
    };
    if final_line.start != 11 || final_line.end != 11 || final_line.bytes.len() != 0 || final_line.truncated {
        panic("trailing empty line page");
    }
    match lines.next(editor) {
        Ok(None) => (),
        _ => panic("line cursor exhaustion"),
    }
    let caret = match view.primary_caret(editor) {
        Ok(value) => value,
        Err(_) => panic("primary caret"),
    };
    if caret.byte != 11 {
        panic("caret fanout");
    }
    match view.replace_bytes(editor, 0, 1, b"\xff") {
        Ok(_) => (),
        Err(_) => panic("byte replacement"),
    }
    let raw = match buffer.read_bytes(editor, 0, 1) {
        Ok(value) => value,
        Err(_) => panic("byte read"),
    };
    if raw[0] != 255 {
        panic("byte-exact round trip");
    }
    match buffer.read_text(editor, 0, 1, baryon::OnInvalid::Strict) {
        Err(error) => {
            match error.kind() {
                baryon::BufferErrorKind::Read(baryon::ReadErrorKind::InvalidUtf8) => (),
                _ => panic("wrong strict decode error kind"),
            }
            if error.detail() != Some(0) {
                panic("wrong strict decode offset");
            }
        },
        Ok(_) => panic("strict decoding accepted invalid UTF-8"),
    }
    let byte_version = match buffer.version(editor) {
        Ok(value) => value,
        Err(_) => panic("byte version"),
    };
    match view.replace_text_if(editor, byte_version, (0, 1), "z") {
        Ok(_) => (),
        Err(_) => panic("optimistic replacement convenience"),
    }
    println!("buffer-data-ok");
}
"#,
		)
		.expect("write Rune buffer data init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let viewports = handle.take_viewports().expect("viewport receiver");
		let viewport = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("Rune buffer data result");
		assert_eq!(viewport.status_message.as_deref(), Some("buffer-data-ok"));
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_buffer_reads_trip_the_aggregate_host_quota_before_an_extra_read() {
		let init = temp_path("rune-buffer-quota", "rn");
		let initial_file = temp_path("rune-buffer-quota-target", "txt");
		fs::write(&initial_file, vec![b'x'; 1024 * 1024]).expect("write quota fixture");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::command(editor, "readquota", |editor, args, bang| {
        let buffer = match baryon::focused_buffer(editor) {
            Ok(value) => value,
            Err(_) => panic("focused buffer"),
        };
        let i = 0;
        while i < 33 {
            match buffer.read_bytes(editor, 0, 1048576) {
                Ok(_) => (),
                Err(_) => panic("engine read unexpectedly failed"),
            };
            i += 1;
        }
    });
}
"#,
		)
		.expect("write Rune quota init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(initial_file.to_string_lossy().into_owned()),
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		type_ex_command(&input, "readquota");
		let viewport = recv_viewport_until(&viewports, "Rune read quota error", |viewport| {
			viewport
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("read-byte quota exhausted"))
		});
		assert!(
			viewport
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("read-byte quota exhausted")),
			"unexpected status: {:?}",
			viewport.status_message
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
		let _ = fs::remove_file(initial_file);
	}

	#[test]
	fn broken_rune_init_reports_diagnostics_but_editor_starts() {
		let init = temp_path("rune-init-broken", "rn");
		fs::write(&init, "pub fn main(").expect("write broken Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("render initialized engine");
		let viewport = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("editor starts after Rune compile failure");
		assert!(
			viewport
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("Rune")),
			"unexpected status: {:?}",
			viewport.status_message
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_init_can_publish_and_invoke_a_command_callback() {
		let init = temp_path("rune-init-command", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::command(editor, "runehello", |editor, args, bang| {
		baryon::test_set_status(editor, "mutated-by-stored-callback");
    });
}
"#,
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("render initialized engine");
		let initialized = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("initialized viewport");
		assert_eq!(initialized.status_message, None, "Rune init must publish");
		type_ex_command(&input, "runehello payload");
		let viewport = recv_viewport_until(&viewports, "Rune callback result", |viewport| {
			viewport.status_message.as_deref() == Some("mutated-by-stored-callback")
		});
		assert_eq!(
			viewport.status_message.as_deref(),
			Some("mutated-by-stored-callback")
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_job_callback_runs_through_the_real_loop() {
		let init = temp_path("rune-job", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::command(editor, "runejob", |editor, args, bang| {
        let spec = match baryon::JobSpec::new("/bin/echo", ["job-output"]) {
            Ok(spec) => spec,
            Err(_) => panic("job spec"),
        };
        let job = match baryon::spawn(editor, spec, |editor, job, result| {
            let output = match result.stdout(editor) {
                Ok(output) => output,
                Err(_) => panic("job output"),
            };
            if output != b"job-output\n" {
                panic("job output mismatch");
            }
            baryon::test_set_status(editor, "rune-job-done");
        }) {
            Ok(job) => job,
            Err(_) => panic("spawn job"),
        };
        if job != job.clone() {
            panic("job clone/equality contract");
        }
    });
}
"#,
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let initialized = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		assert_eq!(
			initialized.status_message, None,
			"Rune job script must compile"
		);
		type_ex_command(&input, "runejob");
		recv_viewport_until(&viewports, "Rune job callback", |viewport| {
			viewport.status_message.as_deref() == Some("rune-job-done")
		});
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_job_can_cancel_a_callback_local_reservation() {
		let init = temp_path("rune-job-cancel", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::command(editor, "runejobcancel", |editor, args, bang| {
        let spec = match baryon::JobSpec::new("/bin/sh", ["-c", "sleep 1"]) {
            Ok(spec) => spec,
            Err(_) => panic("job spec"),
        };
        let job = match baryon::spawn(editor, spec, |editor, job, result| {
            panic("cancelled reservation must not complete");
        }) {
            Ok(job) => job,
            Err(_) => panic("spawn job"),
        };
        match job.status(editor) {
            Ok(baryon::JobStatus::Queued) => (),
            _ => panic("reserved job must be queued"),
        }
        match job.cancel(editor) {
            Ok(()) => (),
            Err(_) => panic("cancel reservation"),
        }
        if job.status(editor).is_ok() {
            panic("cancelled reservation must become stale");
        }
        baryon::test_set_status(editor, "rune-job-cancelled");
    });
}
"#,
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let initialized = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		assert_eq!(
			initialized.status_message, None,
			"Rune job script must compile"
		);
		type_ex_command(&input, "runejobcancel");
		recv_viewport_until(&viewports, "Rune job cancellation", |viewport| {
			viewport.status_message.as_deref() == Some("rune-job-cancelled")
		});
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_callback_error_discards_all_staged_jobs() {
		let init = temp_path("rune-job-error", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::command(editor, "runejoberror", |editor, args, bang| {
        let spec = match baryon::JobSpec::new("/bin/true", []) {
            Ok(spec) => spec,
            Err(_) => panic("job spec"),
        };
        match baryon::spawn(editor, spec, |editor, job, result| {
            baryon::test_set_status(editor, "staged-job-ran-after-vm-error");
        }) {
            Ok(_) => (),
            Err(_) => panic("spawn job"),
        }
        panic("intentional callback error");
    });
}
"#,
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let initialized = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		assert_eq!(
			initialized.status_message, None,
			"Rune job script must compile"
		);
		type_ex_command(&input, "runejoberror");
		let reported = recv_viewport_until(&viewports, "Rune callback error", |viewport| {
			viewport
				.status_message
				.as_deref()
				.is_some_and(|message| message.contains("intentional callback error"))
		});
		assert!(
			reported.status_message.is_some(),
			"the callback error must be reported"
		);
		assert!(
			viewports.recv_timeout(Duration::from_millis(500)).is_err(),
			"a discarded staged job must not deliver after its callback errors"
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_autocmd_initializes_and_delivers_through_the_real_loop() {
		let init = temp_path("rune-init-autocmd", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::autocmd(
        editor,
        [baryon::EventKind::BufferEdited],
        |editor, baseline| {
            if baseline.buffers(editor).len() != 1 ||
                baseline.views(editor).len() != 1 ||
                baseline.focused() == None || baseline.sources(editor).len() != 1 {
                panic("wrong startup baseline");
            }
            let view = match baryon::focused_view(editor) {
                Ok(value) => value,
                Err(_) => panic("focused view"),
            };
            match view.replace_text(editor, 0, 0, "A") {
                Ok(_) => (),
                Err(_) => panic("autocmd initialize edit"),
            }
        },
        |editor, event| {
            if event.kind() != baryon::EventKind::BufferEdited ||
                event.buffer() == None || event.acting_view() == None ||
                event.old_version() == event.new_version() ||
                event.delta_count() != Some(1) || event.inserted_bytes() != Some(1) {
                panic("wrong typed autocmd event");
            }
            let buffer = match baryon::focused_buffer(editor) {
                Ok(value) => value,
                Err(_) => panic("focused buffer in autocmd"),
            };
            let total = match buffer.total_bytes(editor) {
                Ok(value) => value,
                Err(_) => panic("autocmd buffer length"),
            };
            if total == 1 {
                let view = match baryon::focused_view(editor) {
                    Ok(value) => value,
                    Err(_) => panic("focused view in autocmd"),
                };
                match view.replace_text(editor, 1, 1, "B") {
                    Ok(_) => (),
                    Err(_) => panic("callback-raised edit"),
                }
            } else if total == 2 {
                baryon::test_set_status(editor, "rune-autocmd-delivered");
            } else {
                panic("callback-raised event chain did not advance by one pass");
            }
        },
    );
}
"#,
		)
		.expect("write Rune autocmd init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let viewport = recv_viewport_until(&viewports, "Rune autocmd delivery", |viewport| {
			viewport.status_message.as_deref() == Some("rune-autocmd-delivered")
		});
		assert_eq!(
			viewport.status_message.as_deref(),
			Some("rune-autocmd-delivered")
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_autocmd_rebuilds_from_baseline_after_events_are_lost() {
		let init = temp_path("rune-autocmd-overflow", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::autocmd(
        editor,
        [baryon::EventKind::BufferOpened],
        |editor, baseline| {
            if baryon::test_seed_event_overflow_once(editor) {
                ()
            } else {
                if baseline.buffers(editor).len() != 1 {
                    panic("replacement baseline is wrong");
                }
                baryon::test_set_status(editor, "rune-autocmd-rebuilt");
            }
        },
        |editor, event| { panic("loss marker leaked as an ordinary event"); },
    );
}
"#,
		)
		.expect("write Rune overflow init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let viewport = recv_viewport_until(&viewports, "Rune autocmd rebuild", |viewport| {
			viewport.status_message.as_deref() == Some("rune-autocmd-rebuilt")
		});
		assert_eq!(
			viewport.status_message.as_deref(),
			Some("rune-autocmd-rebuilt")
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_autocmd_exposes_opaque_source_owner_pairs_in_baselines_and_events() {
		let init = temp_path("rune-autocmd-source-event", "rn");
		let sourced = temp_path("rune-autocmd-source-target", "rn");
		fs::write(&sourced, "pub fn main(editor) { let _ = editor; }")
			.expect("write sourced Rune file");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::autocmd(
        editor,
        [baryon::EventKind::SourceLoaded],
        |editor, baseline| {
            let sources = baseline.sources(editor);
            if sources.len() != 1 || sources[0].0 != sources[0].0.clone() ||
                sources[0].1 != sources[0].1.clone() {
                panic("source baseline identity is wrong");
            }
        },
        |editor, event| {
            if event.kind() != baryon::EventKind::SourceLoaded ||
                event.source() == None || event.owner() == None {
                panic("source event identity is wrong");
            }
            baryon::test_set_status(editor, "rune-source-event-delivered");
        },
    );
}
"#,
		)
		.expect("write Rune source-event init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		commands
			.send(EditorCommand::SourceRune(
				sourced.to_string_lossy().into_owned(),
			))
			.unwrap();
		let viewport = recv_viewport_until(&viewports, "Rune source autocmd", |viewport| {
			viewport.status_message.as_deref() == Some("rune-source-event-delivered")
		});
		assert_eq!(
			viewport.status_message.as_deref(),
			Some("rune-source-event-delivered")
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
		let _ = fs::remove_file(sourced);
	}

	#[test]
	fn rune_autocmd_reload_atomically_replaces_the_owner_generation() {
		let configured_init = temp_path("rune-autocmd-reload-init", "rn");
		let source = temp_path("rune-autocmd-reload-source", "rn");
		let write_source = |label: &str| {
			fs::write(
				&source,
				format!(
					r#"
pub fn main(editor) {{
    baryon::autocmd(
        editor,
        [baryon::EventKind::BufferEdited],
        |editor, baseline| {{}},
        |editor, event| {{ baryon::test_set_status(editor, "{label}"); }},
    );
}}
"#,
				),
			)
			.expect("write Rune autocmd generation");
		};
		write_source("autocmd-generation-one");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(configured_init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();

		commands
			.send(EditorCommand::SourceRune(
				source.to_string_lossy().into_owned(),
			))
			.unwrap();
		recv_viewport_until(&viewports, "first autocmd generation", |viewport| {
			viewport.status_message.as_deref() == Some("Rune source loaded")
		});
		commands
			.send(EditorCommand::InsertText("a".to_string()))
			.unwrap();
		recv_viewport_until(&viewports, "first autocmd callback", |viewport| {
			viewport.status_message.as_deref() == Some("autocmd-generation-one")
		});

		write_source("autocmd-generation-two");
		commands
			.send(EditorCommand::SourceRune(
				source.to_string_lossy().into_owned(),
			))
			.unwrap();
		recv_viewport_until(&viewports, "replacement autocmd generation", |viewport| {
			viewport.status_message.as_deref() == Some("Rune source loaded")
		});
		commands
			.send(EditorCommand::InsertText("b".to_string()))
			.unwrap();
		let viewport =
			recv_viewport_until(&viewports, "replacement autocmd callback", |viewport| {
				viewport.status_message.as_deref() == Some("autocmd-generation-two")
			});
		assert_eq!(
			viewport.status_message.as_deref(),
			Some("autocmd-generation-two")
		);

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(source);
	}

	#[test]
	fn rune_autocmd_can_defer_unloading_its_own_generation() {
		let init = temp_path("rune-autocmd-self-unload", "rn");
		let escaped = init
			.to_string_lossy()
			.replace('\\', "\\\\")
			.replace('"', "\\\"");
		fs::write(
			&init,
			format!(
				r#"
pub fn main(editor) {{
    baryon::autocmd(
        editor,
        [baryon::EventKind::BufferEdited],
        |editor, baseline| {{}},
        |editor, event| {{
            baryon::unload(editor, "{escaped}");
            baryon::test_set_status(editor, "autocmd-self-unloaded");
        }},
    );
}}
"#,
			),
		)
		.expect("write self-unloading autocmd");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		commands
			.send(EditorCommand::InsertText("a".to_string()))
			.unwrap();
		recv_viewport_until(&viewports, "self-unloading autocmd", |viewport| {
			viewport.status_message.as_deref() == Some("autocmd-self-unloaded")
		});

		commands
			.send(EditorCommand::UnloadRune(
				init.to_string_lossy().into_owned(),
			))
			.unwrap();
		let retired = recv_viewport_until(&viewports, "retired autocmd source", |viewport| {
			viewport
				.status_message
				.as_deref()
				.is_some_and(|status| status.contains("not loaded"))
		});
		assert!(retired.status_message.unwrap().contains("not loaded"));
		commands
			.send(EditorCommand::InsertText("b".to_string()))
			.unwrap();
		let after = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("post-unload edit render");
		assert_ne!(
			after.status_message.as_deref(),
			Some("autocmd-self-unloaded"),
			"retired autocmd fired again"
		);

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_autocmd_buffer_filter_skips_other_buffers_without_entering_the_vm() {
		let init = temp_path("rune-autocmd-buffer-filter", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    let buffer = match baryon::focused_buffer(editor) {
        Ok(value) => value,
        Err(_) => panic("focused buffer"),
    };
    baryon::autocmd_for_buffer(
        editor,
        buffer,
        [baryon::EventKind::BufferEdited],
        |editor, baseline| {},
        |editor, event| { baryon::test_set_status(editor, "filtered-buffer-fired"); },
    );
}
"#,
		)
		.expect("write filtered autocmd");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();

		commands.send(EditorCommand::DocTabNew).unwrap();
		commands
			.send(EditorCommand::InsertText("other-buffer".to_string()))
			.unwrap();
		let other = recv_viewport_until(&viewports, "other buffer edit", |viewport| {
			viewport
				.tokens
				.iter()
				.flat_map(|token| token.text.iter().copied())
				.eq(b"other-buffer".iter().copied())
		});
		assert_ne!(
			other.status_message.as_deref(),
			Some("filtered-buffer-fired"),
			"buffer filter entered the callback for another buffer"
		);

		commands.send(EditorCommand::DocTabPrev).unwrap();
		commands
			.send(EditorCommand::InsertText("owned-buffer".to_string()))
			.unwrap();
		let owned = recv_viewport_until(&viewports, "filtered buffer edit", |viewport| {
			viewport.status_message.as_deref() == Some("filtered-buffer-fired")
		});
		assert_eq!(
			owned.status_message.as_deref(),
			Some("filtered-buffer-fired")
		);

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_autocmd_rejects_buffer_filters_for_bufferless_kinds() {
		let init = temp_path("rune-autocmd-invalid-filter", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    let buffer = match baryon::focused_buffer(editor) {
        Ok(value) => value,
        Err(_) => panic("focused buffer"),
    };
    baryon::autocmd_for_buffer(
        editor,
        buffer,
        [baryon::EventKind::SourceLoaded],
        |editor, baseline| {},
        |editor, event| {},
    );
}
"#,
		)
		.expect("write invalid filtered autocmd");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let viewport = recv_viewport_until(&viewports, "invalid autocmd filter", |viewport| {
			viewport
				.status_message
				.as_deref()
				.is_some_and(|status| status.contains("buffer-less"))
		});
		assert!(viewport.status_message.unwrap().contains("buffer-less"));
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_options_and_mappings_use_their_fixed_callback_abis() {
		let init = temp_path("rune-init-option-map", "rn");
		fs::write(
			&init,
			r#"
pub fn main(editor) {
    baryon::option(editor, "runeoption", "string", |editor, call| { "option-value" });
    baryon::map(editor, "normal", "Q", |editor| { "mapping-fired" });
}
"#,
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let initialized = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		assert_eq!(initialized.status_message, None);

		type_ex_command(&input, "set runeoption?");
		recv_viewport_until(&viewports, "Rune option query", |viewport| {
			viewport.status_message.as_deref() == Some("option-value")
		});
		input
			.send(InputEvent::Key {
				code: KeyCode::Char('Q'),
				modifiers: Modifiers::NONE,
			})
			.unwrap();
		recv_viewport_until(&viewports, "Rune mapping callback", |viewport| {
			viewport.status_message.as_deref() == Some("mapping-fired")
		});
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn rune_source_reload_is_atomic_and_unload_retires_the_generation() {
		let configured_init = temp_path("rune-configured-init", "rn");
		let source = temp_path("rune-live-source", "rn");
		let write_source = |result: &str| {
			fs::write(
				&source,
				format!(
					r#"
pub fn main(editor) {{
    baryon::command(editor, "livecallback", |editor, args, bang| {{ "{result}" }});
}}
"#
				),
			)
			.expect("write Rune source");
		};
		write_source("generation-one");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(configured_init),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("render engine");
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();

		commands
			.send(EditorCommand::SourceRune(
				source.to_string_lossy().into_owned(),
			))
			.expect("source first generation");
		recv_viewport_until(&viewports, "first Rune source", |viewport| {
			viewport.status_message.as_deref() == Some("Rune source loaded")
		});
		type_ex_command(&input, "livecallback");
		recv_viewport_until(&viewports, "first Rune callback", |viewport| {
			viewport.status_message.as_deref() == Some("generation-one")
		});

		write_source("generation-two");
		commands
			.send(EditorCommand::SourceRune(
				source.to_string_lossy().into_owned(),
			))
			.expect("reload Rune source");
		recv_viewport_until(&viewports, "second Rune source", |viewport| {
			viewport.status_message.as_deref() == Some("Rune source loaded")
		});
		type_ex_command(&input, "livecallback");
		recv_viewport_until(&viewports, "replacement Rune callback", |viewport| {
			viewport.status_message.as_deref() == Some("generation-two")
		});

		fs::write(&source, "pub fn main(").expect("write broken reload");
		commands
			.send(EditorCommand::SourceRune(
				source.to_string_lossy().into_owned(),
			))
			.expect("request broken reload");
		recv_viewport_until(&viewports, "broken reload diagnostic", |viewport| {
			viewport
				.status_message
				.as_deref()
				.is_some_and(|status| status.contains("Rune"))
		});
		type_ex_command(&input, "livecallback");
		recv_viewport_until(&viewports, "last good callback", |viewport| {
			viewport.status_message.as_deref() == Some("generation-two")
		});

		commands
			.send(EditorCommand::UnloadRune(
				source.to_string_lossy().into_owned(),
			))
			.expect("unload Rune source");
		recv_viewport_until(&viewports, "Rune source unload", |viewport| {
			viewport.status_message.as_deref() == Some("Rune source unloaded")
		});
		type_ex_command(&input, "livecallback");
		recv_viewport_until(&viewports, "retired Rune command", |viewport| {
			viewport
				.status_message
				.as_deref()
				.is_some_and(|status| status.contains("Unknown command"))
		});

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(source);
	}

	#[test]
	fn rune_callback_defers_unloading_its_own_generation_until_return() {
		let init = temp_path("rune-self-unload", "rn");
		let path = init
			.to_string_lossy()
			.replace('\\', "\\\\")
			.replace('"', "\\\"");
		fs::write(
			&init,
			format!(
				r#"
pub fn main(editor) {{
    baryon::command(editor, "selfunload", |editor, args, bang| {{
        baryon::unload(editor, "{path}");
        "callback-returned"
    }});
}}
"#
			),
		)
		.expect("write self-unloading Rune source");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();

		type_ex_command(&input, "selfunload");
		recv_viewport_until(&viewports, "self-unloading callback return", |viewport| {
			viewport.status_message.as_deref() == Some("callback-returned")
		});
		type_ex_command(&input, "selfunload");
		recv_viewport_until(&viewports, "self-unloaded Rune command", |viewport| {
			viewport
				.status_message
				.as_deref()
				.is_some_and(|status| status.contains("Unknown command"))
		});

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn sourcing_the_configured_init_path_replaces_its_generation() {
		let init = temp_path("rune-init-identity", "rn");
		let write_init = |result: &str| {
			fs::write(
				&init,
				format!(
					r#"
pub fn main(editor) {{
    baryon::command(editor, "initidentity", |editor, args, bang| {{ "{result}" }});
}}
"#
				),
			)
			.expect("write configured Rune init");
		};
		write_init("initial-generation");
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		type_ex_command(&input, "initidentity");
		recv_viewport_until(&viewports, "initial init generation", |viewport| {
			viewport.status_message.as_deref() == Some("initial-generation")
		});

		write_init("replacement-generation");
		commands
			.send(EditorCommand::SourceRune(
				init.to_string_lossy().into_owned(),
			))
			.unwrap();
		recv_viewport_until(&viewports, "configured init replacement", |viewport| {
			viewport.status_message.as_deref() == Some("Rune source loaded")
		});
		type_ex_command(&input, "initidentity");
		recv_viewport_until(&viewports, "replacement init generation", |viewport| {
			viewport.status_message.as_deref() == Some("replacement-generation")
		});

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
	}

	#[test]
	fn stale_rune_compile_is_rejected_across_unload_and_same_path_request() {
		let configured_init = temp_path("rune-aba-init", "rn");
		let source = temp_path("rune-aba-source", "rn");
		let script = |result: &str, padding: usize| {
			let mut source = format!(
				r#"
pub fn main(editor) {{
    baryon::command(editor, "abacallback", |editor, args, bang| {{ "{result}" }});
}}
"#
			);
			source.push_str(&"// compile padding\n".repeat(padding));
			source
		};
		fs::write(&source, script("stale-generation", 50_000)).unwrap();
		let mut handle = EngineHandle::spawn(EngineConfig {
			init_script: Some(configured_init),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let input = handle.input_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands.send(EditorCommand::Resize(80, 24)).unwrap();
		let _ = viewports.recv_timeout(Duration::from_secs(10)).unwrap();
		let path = source.to_string_lossy().into_owned();
		commands
			.send(EditorCommand::SourceRune(path.clone()))
			.unwrap();
		commands
			.send(EditorCommand::UnloadRune(path.clone()))
			.unwrap();
		fs::write(&source, script("fresh-generation", 0)).unwrap();
		commands.send(EditorCommand::SourceRune(path)).unwrap();
		recv_viewport_until(&viewports, "fresh post-ABA source", |viewport| {
			viewport.status_message.as_deref() == Some("Rune source loaded")
		});
		type_ex_command(&input, "abacallback");
		recv_viewport_until(&viewports, "fresh post-ABA callback", |viewport| {
			viewport.status_message.as_deref() == Some("fresh-generation")
		});
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(source);
	}

	#[cfg(unix)]
	#[test]
	fn initial_file_error_wins_over_rune_init_output() {
		let init = temp_path("rune-init-priority", "rn");
		let loop_path = temp_path("rune-initial-symlink-loop", "txt");
		std::os::unix::fs::symlink(&loop_path, &loop_path).expect("create symlink loop");
		fs::write(
			&init,
			"pub fn main(editor) { println!(\"lower-priority init output\"); }",
		)
		.expect("write Rune init");
		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(loop_path.to_string_lossy().into_owned()),
			init_script: Some(init.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("render initialized engine");
		let viewport = viewports
			.recv_timeout(Duration::from_secs(10))
			.expect("viewport after failed initial load");
		assert_ne!(
			viewport.status_message.as_deref(),
			Some("lower-priority init output")
		);
		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(init);
		let _ = fs::remove_file(loop_path);
	}

	#[test]
	fn public_host_loads_and_projects_a_file() {
		let path = std::env::temp_dir().join(format!(
			"baryon-headless-{}-{}.txt",
			std::process::id(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system clock")
				.as_nanos()
		));
		fs::write(&path, "alpha beta\n").expect("write fixture");

		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(path.to_string_lossy().into_owned()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("resize engine");

		let deadline = Instant::now() + Duration::from_secs(10);
		loop {
			let viewport = viewports
				.recv_timeout(deadline.saturating_duration_since(Instant::now()))
				.expect("projected viewport");
			let text = viewport
				.focused_pane()
				.expect("focused pane")
				.tokens
				.iter()
				.flat_map(|token| token.text.iter().copied())
				.collect::<Vec<_>>();
			if text == b"alpha beta\n" {
				break;
			}
			assert!(Instant::now() < deadline, "loaded viewport not projected");
		}

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(path);
	}

	#[test]
	fn public_host_edits_and_saves_an_unnamed_document() {
		let path = std::env::temp_dir().join(format!(
			"baryon-headless-unnamed-{}-{}.txt",
			std::process::id(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system clock")
				.as_nanos()
		));
		let mut handle = EngineHandle::spawn(EngineConfig::default());
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("resize engine");

		let initial = viewports
			.recv_timeout(Duration::from_secs(2))
			.expect("initial unnamed viewport");
		assert_eq!(initial.file_name, None);
		assert_eq!(initial.file_size, 0);

		commands
			.send(EditorCommand::InsertText("hello from baryon".to_string()))
			.expect("edit unnamed document");
		let edited = viewports
			.recv_timeout(Duration::from_secs(2))
			.expect("edited unnamed viewport");
		let text = edited
			.focused_pane()
			.expect("focused unnamed pane")
			.tokens
			.iter()
			.flat_map(|token| token.text.iter().copied())
			.collect::<Vec<_>>();
		assert_eq!(text, b"hello from baryon");
		assert!(edited.is_dirty);

		commands
			.send(EditorCommand::WriteFile)
			.expect("try unnamed write");
		let deadline = Instant::now() + Duration::from_secs(2);
		let unnamed_write = loop {
			let viewport = viewports
				.recv_timeout(deadline.saturating_duration_since(Instant::now()))
				.expect("unnamed write response");
			if !viewport.busy {
				break viewport;
			}
		};
		assert_eq!(
			unnamed_write.status_message.as_deref(),
			Some("No file name")
		);

		let path_name = path.to_string_lossy().into_owned();
		commands
			.send(EditorCommand::WriteFileAs(path_name.clone()))
			.expect("write unnamed document as file");
		let deadline = Instant::now() + Duration::from_secs(2);
		let saved = loop {
			let viewport = viewports
				.recv_timeout(deadline.saturating_duration_since(Instant::now()))
				.expect("saved unnamed viewport");
			if !viewport.busy && viewport.file_name.as_deref() == Some(path_name.as_str()) {
				break viewport;
			}
		};
		assert_eq!(fs::read(&path).expect("read saved document"), text);
		assert!(!saved.is_dirty);

		handle.shutdown().expect("join engine");
		let _ = fs::remove_file(path);
	}

	#[test]
	fn saving_multiple_documents_keeps_their_cold_backings_isolated() {
		const LINE_COUNT: usize = 50_000;
		const TARGET_LINE: u64 = 40_000;
		const A_MARKER: &str = "AAAA-alpha-AAAA";
		const B_MARKER: &str = "BBBB-bravo-BBBB";

		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system clock")
			.as_nanos();
		let temp = std::env::temp_dir();
		let path_a = temp.join(format!(
			"baryon-device-isolation-a-{}-{nonce}.txt",
			std::process::id()
		));
		let path_b = temp.join(format!(
			"baryon-device-isolation-b-{}-{nonce}.txt",
			std::process::id()
		));
		let contents_a = multi_extent_fixture(A_MARKER, LINE_COUNT);
		let contents_b = multi_extent_fixture(B_MARKER, LINE_COUNT);
		assert_eq!(contents_a.len(), contents_b.len());
		fs::write(&path_a, &contents_a).expect("write document A");
		fs::write(&path_b, &contents_b).expect("write document B");

		let path_a_name = path_a.to_string_lossy().into_owned();
		let path_b_name = path_b.to_string_lossy().into_owned();
		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(path_a_name.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(100, 30))
			.expect("resize engine");

		recv_viewport_until(&viewports, "document A to load", |viewport| {
			viewport.file_name.as_deref() == Some(path_a_name.as_str())
				&& visible_text(viewport)
					.windows(A_MARKER.len())
					.any(|window| window == A_MARKER.as_bytes())
		});
		commands
			.send(EditorCommand::WriteFile)
			.expect("save document A");
		recv_viewport_until(&viewports, "document A to save", |viewport| {
			viewport.file_name.as_deref() == Some(path_a_name.as_str())
				&& viewport
					.status_message
					.as_deref()
					.is_some_and(|status| status.contains("written"))
		});

		commands
			.send(EditorCommand::DocTabEdit(path_b_name.clone()))
			.expect("open document B");
		recv_viewport_until(&viewports, "document B to load", |viewport| {
			viewport.file_name.as_deref() == Some(path_b_name.as_str())
				&& visible_text(viewport)
					.windows(B_MARKER.len())
					.any(|window| window == B_MARKER.as_bytes())
		});
		commands
			.send(EditorCommand::WriteFile)
			.expect("save document B");
		recv_viewport_until(&viewports, "document B to save", |viewport| {
			viewport.file_name.as_deref() == Some(path_b_name.as_str())
				&& viewport
					.status_message
					.as_deref()
					.is_some_and(|status| status.contains("written"))
		});

		commands
			.send(EditorCommand::DocTabPrev)
			.expect("return to document A");
		recv_viewport_until(&viewports, "document A tab to reactivate", |viewport| {
			viewport.file_name.as_deref() == Some(path_a_name.as_str())
		});
		commands
			.send(EditorCommand::ScrollViewportToLine(DocLine::new(
				TARGET_LINE,
			)))
			.expect("read a cold extent from document A");
		let cold_a = recv_viewport_until(&viewports, "document A cold extent", |viewport| {
			if viewport.file_name.as_deref() != Some(path_a_name.as_str())
				|| viewport.viewport_start_line < DocLine::new(TARGET_LINE.saturating_sub(30))
			{
				return false;
			}
			let text = visible_text(viewport);
			text.windows(A_MARKER.len())
				.any(|window| window == A_MARKER.as_bytes())
				|| text
					.windows(B_MARKER.len())
					.any(|window| window == B_MARKER.as_bytes())
		});

		commands
			.send(EditorCommand::WriteFile)
			.expect("save document A after its cold read");
		recv_viewport_until(&viewports, "document A second save", |viewport| {
			viewport.file_name.as_deref() == Some(path_a_name.as_str())
				&& viewport
					.status_message
					.as_deref()
					.is_some_and(|status| status.contains("written"))
		});
		handle.shutdown().expect("join engine");

		assert!(
			visible_text(&cold_a)
				.windows(A_MARKER.len())
				.any(|window| window == A_MARKER.as_bytes()),
			"document A resolved bytes from another saved document"
		);
		assert_eq!(
			fs::read(&path_a).expect("read document A after second save"),
			contents_a,
			"saving A after a cold read must not write B's cached bytes into A"
		);

		let _ = fs::remove_file(path_a);
		let _ = fs::remove_file(path_b);
	}

	/// Opening a non-UTF-8 document and letting its semantic graft run must not
	/// change the document's bytes, and saving it must write the original bytes back.
	///
	/// The semantic pipeline used to send the reactor a `String::from_utf8_lossy` of
	/// the live bytes and graft the reactor's (lossy) bytes back in, so a latin-1 CSV
	/// was silently rewritten to U+FFFD and truncated — on screen and, on the next
	/// save, on disk.
	#[test]
	fn semantic_graft_preserves_a_non_utf8_document_on_screen_and_on_disk() {
		// A latin-1 CSV: "José" and "Zürich" are not valid UTF-8. `.csv` selects the
		// CSV provider, whose structural topology is what triggers the graft.
		let mut original = b"name,city\n".to_vec();
		original.extend_from_slice(b"Jos\xe9,Z\xfcrich\n");
		original.extend_from_slice(b"Ana,M\xe1laga\n");

		let nonce = SystemTime::now()
			.duration_since(UNIX_EPOCH)
			.expect("system clock")
			.as_nanos();
		let path = std::env::temp_dir().join(format!(
			"baryon-graft-non-utf8-{}-{nonce}.csv",
			std::process::id()
		));
		fs::write(&path, &original).expect("write non-UTF-8 csv");
		let path_name = path.to_string_lossy().into_owned();

		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(path_name.clone()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(100, 30))
			.expect("resize engine");

		// The corrupt bytes, had they been grafted: é -> U+FFFD, ü -> U+FFFD.
		let replacement = "\u{FFFD}".as_bytes();
		let names_a_row = |text: &[u8]| {
			text.windows(3).any(|w| w == b"Jos") && text.windows(2).any(|w| w == b"Z\xfc")
		};

		// Wait for the load, then drain refreshes through a settle window well past the
		// reactor's latency. Every loaded frame is checked: if the graft corrupts the
		// document, the frame it lands on carries U+FFFD and fails here immediately.
		recv_viewport_until(&viewports, "non-UTF-8 csv to load", |viewport| {
			viewport.file_name.as_deref() == Some(path_name.as_str())
				&& names_a_row(&visible_text(viewport))
		});
		let settle = Instant::now() + Duration::from_secs(2);
		while Instant::now() < settle {
			match viewports.recv_timeout(settle.saturating_duration_since(Instant::now())) {
				Ok(viewport) if viewport.file_name.as_deref() == Some(path_name.as_str()) => {
					let text = visible_text(&viewport);
					assert!(
						!text.windows(replacement.len()).any(|w| w == replacement),
						"semantic graft rewrote the on-screen bytes to U+FFFD"
					);
				}
				Ok(_) => {}
				Err(_) => break,
			}
		}

		commands
			.send(EditorCommand::WriteFile)
			.expect("save non-UTF-8 csv");
		recv_viewport_until(&viewports, "non-UTF-8 csv to save", |viewport| {
			viewport.file_name.as_deref() == Some(path_name.as_str())
				&& viewport
					.status_message
					.as_deref()
					.is_some_and(|status| status.contains("written"))
		});
		handle.shutdown().expect("join engine");

		assert_eq!(
			fs::read(&path).expect("read saved non-UTF-8 csv"),
			original,
			"saving a grafted non-UTF-8 document must write its original bytes"
		);
		let _ = fs::remove_file(path);
	}

	#[test]
	fn corrupt_startup_shadow_falls_back_to_an_editable_unnamed_document() {
		let path = std::env::temp_dir().join(format!(
			"baryon-headless-corrupt-shadow-{}-{}.txt",
			std::process::id(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system clock")
				.as_nanos()
		));
		fs::write(&path, "original contents\n").expect("write original document");
		let shadow_dir = shadow_dir_for_path(&path).expect("shadow directory");
		fs::create_dir_all(&shadow_dir).expect("create corrupt shadow directory");
		fs::write(
			shadow_dir.join("manifest"),
			"version\t1\ncurrent_index\t0\nnext_generation_id\t1\ngeneration\t0\t0\tmissing\n",
		)
		.expect("write corrupt shadow manifest");

		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(path.to_string_lossy().into_owned()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("resize engine");

		let fallback = viewports
			.recv_timeout(Duration::from_secs(2))
			.expect("fallback viewport");
		assert_eq!(fallback.file_name, None);
		assert_eq!(fallback.file_size, 0);
		assert_eq!(
			fallback.status_message.as_deref(),
			Some("Shadow manifest missing original_path")
		);

		commands
			.send(EditorCommand::InsertText("editable fallback".to_string()))
			.expect("edit fallback document");
		let edited = viewports
			.recv_timeout(Duration::from_secs(2))
			.expect("edited fallback viewport");
		let text = edited
			.focused_pane()
			.expect("focused fallback pane")
			.tokens
			.iter()
			.flat_map(|token| token.text.iter().copied())
			.collect::<Vec<_>>();
		assert_eq!(text, b"editable fallback");

		handle.shutdown().expect("join engine");
		let _ = fs::remove_dir_all(shadow_dir);
		let _ = fs::remove_file(path);
	}

	#[test]
	fn dropping_a_host_never_waits_for_engine_threads() {
		let handle = EngineHandle::spawn(EngineConfig::default());
		let started = Instant::now();
		drop(handle);
		assert!(
			started.elapsed() < Duration::from_millis(100),
			"dropping an engine host must be non-blocking"
		);
	}

	#[test]
	fn dropping_a_host_preserves_unsaved_shadow_recovery() {
		let path = std::env::temp_dir().join(format!(
			"baryon-headless-shadow-{}-{}.txt",
			std::process::id(),
			SystemTime::now()
				.duration_since(UNIX_EPOCH)
				.expect("system clock")
				.as_nanos()
		));
		fs::write(&path, "Chase,Account,North\n".repeat(80)).expect("write fixture");

		let mut handle = EngineHandle::spawn(EngineConfig {
			initial_file: Some(path.to_string_lossy().into_owned()),
			..EngineConfig::default()
		});
		let commands = handle.command_sender();
		let viewports = handle.take_viewports().expect("viewport receiver");
		commands
			.send(EditorCommand::Resize(80, 24))
			.expect("resize engine");

		let deadline = Instant::now() + Duration::from_secs(10);
		loop {
			let viewport = viewports
				.recv_timeout(deadline.saturating_duration_since(Instant::now()))
				.expect("initial viewport");
			if viewport.focused_pane().is_some_and(|pane| {
				pane.tokens
					.iter()
					.any(|token| token.text.starts_with(b"Chase"))
			}) {
				break;
			}
		}

		commands
			.send(EditorCommand::SubstituteAll {
				pattern: "Chase".to_string(),
				replacement: "Bank".to_string(),
				range: CommandRange::WholeFile,
				flags: SubstituteFlags::default(),
			})
			.expect("substitute engine document");
		loop {
			let viewport = viewports
				.recv_timeout(deadline.saturating_duration_since(Instant::now()))
				.expect("substituted viewport");
			if viewport.focused_pane().is_some_and(|pane| {
				pane.tokens
					.iter()
					.any(|token| token.text.starts_with(b"Bank"))
			}) {
				break;
			}
		}

		drop(handle);
		while commands.refresh_for_test().is_ok() {
			assert!(
				Instant::now() < deadline,
				"engine did not terminate after dropping its host"
			);
			std::thread::yield_now();
		}

		let shadow = load_shadow_session(&path)
			.expect("load recovery shadow")
			.expect("abnormal host drop must preserve recovery shadow");
		discard_shadow_session(&shadow).expect("clean recovery shadow");
		let _ = fs::remove_file(path);
	}
}
