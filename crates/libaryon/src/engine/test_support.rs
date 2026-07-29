use super::core::{Engine, FILE_DEVICE_ID};
use super::language::{GreenNode, LanguageRegistry};
use super::request::{CommandSender, InputSender};
use super::structural_graft::{apply_graft, graft_allocation_count};
use crate::ecs::UastRegistry;
use crate::engine::UiColorOverrides;
use crate::engine::settings::SaveDurability;
use crate::svp::fs_provider::FsProvider;
use crate::svp::rust_provider::build_rust_green_tree;
use crate::svp::{SvpPointer, SvpResolver};
use crate::uast::Viewport;
use crate::uast::kind::SemanticKind;
use crate::uast::metrics::SpanMetrics;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::{Arc, mpsc};
use std::thread;
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(crate) fn build_document(text: &str) -> (UastRegistry, crate::ecs::NodeId) {
	let registry = UastRegistry::new(32);
	let mut chunk = registry.reserve_chunk(2).expect("OOM");
	let newlines = text.bytes().filter(|&b| b == b'\n').count() as u64;
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: text.len() as u64,
			newlines,
		},
	);
	let leaf = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: text.len() as u64,
			newlines,
		},
	);
	chunk.append_local_child(root, leaf);
	unsafe {
		*registry.virtual_data[leaf.index()].get() = Some(text.as_bytes().to_vec());
	}
	(registry, root)
}

pub(crate) fn build_grafted_rust_document(text: &str) -> (UastRegistry, crate::ecs::NodeId) {
	let green = build_rust_green_tree(text);
	let newlines = text.bytes().filter(|&b| b == b'\n').count() as u64;
	let capacity = graft_allocation_count(&green)
		.saturating_mul(2)
		.saturating_add(64)
		.try_into()
		.expect("test graft fits u32 registry capacity");
	let registry = UastRegistry::new(capacity);
	let root = registry.alloc_test_node();
	unsafe {
		*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
		*registry.metrics[root.index()].get() = SpanMetrics {
			byte_length: text.len() as u64,
			newlines,
		};
	}
	apply_graft(&registry, root, &green, text.as_bytes()).expect("test graft is byte-identical");
	(registry, root)
}

/// Grafted topology probe with a synthetic group before a real function:
///
/// ```text
/// document root
/// └─ Group (transparent spine)
///    ├─ Token [0..1]
///    ├─ Group [1..10]
///    │  └─ Token [1..10]
///    └─ RelationalTable [10..30]
///       └─ Token [10..30]
/// ```
///
/// The returned leaf is inside the synthetic group. The returned function is
/// the later semantic boundary used to verify `]m` skips topology-only nodes.
pub(crate) fn build_grouped_semantic_document() -> (
	UastRegistry,
	crate::ecs::NodeId,
	crate::ecs::NodeId,
	crate::ecs::NodeId,
) {
	let source = b"pgggggggggffffffffffffffffffff";
	let green = GreenNode {
		kind: SemanticKind::Group,
		provider_kind: crate::uast::ProviderKindId::NONE,
		byte_len: source.len() as u32,
		children: vec![
			GreenNode {
				kind: SemanticKind::Token,
				provider_kind: crate::uast::ProviderKindId::NONE,
				byte_len: 1,
				children: Vec::new(),
			},
			GreenNode {
				kind: SemanticKind::Group,
				provider_kind: crate::uast::ProviderKindId::NONE,
				byte_len: 9,
				children: vec![GreenNode {
					kind: SemanticKind::Token,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: 9,
					children: Vec::new(),
				}],
			},
			GreenNode {
				kind: SemanticKind::RelationalTable,
				provider_kind: crate::uast::ProviderKindId::NONE,
				byte_len: 20,
				children: vec![GreenNode {
					kind: SemanticKind::Token,
					provider_kind: crate::uast::ProviderKindId::NONE,
					byte_len: 20,
					children: Vec::new(),
				}],
			},
		],
	};
	assert_eq!(green.byte_len as usize, source.len());

	let registry = UastRegistry::new(16);
	let root = registry.alloc_test_node();
	unsafe {
		*registry.kinds[root.index()].get() = SemanticKind::RelationalTable;
		*registry.metrics[root.index()].get() = SpanMetrics {
			byte_length: source.len() as u64,
			newlines: 0,
		};
	}
	let spine = apply_graft(&registry, root, &green, source).expect("test graft is byte-identical");
	let prefix = registry.get_first_child(spine).expect("group prefix");
	let group = registry.get_next_sibling(prefix).expect("synthetic group");
	let grouped_leaf = registry.get_first_child(group).expect("grouped leaf");
	let function = registry.get_next_sibling(group).expect("semantic function");

	(registry, root, grouped_leaf, function)
}

pub(crate) fn build_split_virtual_document(parts: &[&str]) -> (UastRegistry, crate::ecs::NodeId) {
	let registry = UastRegistry::new((parts.len() as u32) + 4);
	let total_bytes = parts.iter().map(|part| part.len() as u64).sum::<u64>();
	let total_newlines = parts
		.iter()
		.map(|part| part.as_bytes().iter().filter(|&&b| b == b'\n').count() as u64)
		.sum();
	let mut chunk = registry
		.reserve_chunk((parts.len() as u32) + 1)
		.expect("OOM");
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: total_bytes,
			newlines: total_newlines,
		},
	);

	for part in parts {
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			None,
			SpanMetrics {
				byte_length: part.len() as u64,
				newlines: part.as_bytes().iter().filter(|&&b| b == b'\n').count() as u64,
			},
		);
		chunk.append_local_child(root, leaf);
		unsafe {
			*registry.virtual_data[leaf.index()].get() = Some(part.as_bytes().to_vec());
		}
	}

	(registry, root)
}

pub(crate) fn build_foldable_document() -> (UastRegistry, crate::ecs::NodeId, crate::ecs::NodeId) {
	let registry = UastRegistry::new(16);
	let mut chunk = registry.reserve_chunk(6).expect("OOM");
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: 17,
			newlines: 3,
		},
	);
	let head = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 5,
			newlines: 1,
		},
	);
	let folded = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: 8,
			newlines: 2,
		},
	);
	let folded_a = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 4,
			newlines: 1,
		},
	);
	let folded_b = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 4,
			newlines: 1,
		},
	);
	let tail = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 4,
			newlines: 0,
		},
	);
	chunk.append_local_child(root, head);
	chunk.append_local_child(root, folded);
	chunk.append_local_child(root, tail);
	chunk.append_local_child(folded, folded_a);
	chunk.append_local_child(folded, folded_b);
	unsafe {
		*registry.virtual_data[head.index()].get() = Some(b"head\n".to_vec());
		*registry.virtual_data[folded_a.index()].get() = Some(b"one\n".to_vec());
		*registry.virtual_data[folded_b.index()].get() = Some(b"two\n".to_vec());
		*registry.virtual_data[tail.index()].get() = Some(b"tail".to_vec());
	}
	(registry, root, folded)
}

pub(crate) fn build_root_with_rows() -> (
	UastRegistry,
	crate::ecs::NodeId,
	crate::ecs::NodeId,
	crate::ecs::NodeId,
) {
	let registry = UastRegistry::new(8);
	let mut chunk = registry.reserve_chunk(5).expect("OOM");
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: 6,
			newlines: 2,
		},
	);
	let row_a = chunk.spawn_node(
		SemanticKind::RelationalRow,
		None,
		SpanMetrics {
			byte_length: 3,
			newlines: 1,
		},
	);
	let row_b = chunk.spawn_node(
		SemanticKind::RelationalRow,
		None,
		SpanMetrics {
			byte_length: 3,
			newlines: 1,
		},
	);
	let cell_a = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 3,
			newlines: 1,
		},
	);
	let cell_b = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 3,
			newlines: 1,
		},
	);
	chunk.append_local_child(root, row_a);
	chunk.append_local_child(root, row_b);
	chunk.append_local_child(row_a, cell_a);
	chunk.append_local_child(row_b, cell_b);
	unsafe {
		*registry.virtual_data[cell_a.index()].get() = Some(b"a\n".to_vec());
		*registry.virtual_data[cell_b.index()].get() = Some(b"b\n".to_vec());
	}
	(registry, root, row_a, row_b)
}

pub(crate) fn build_line_document(line_count: u32) -> (UastRegistry, crate::ecs::NodeId) {
	let mut text = String::new();
	for _ in 0..line_count {
		text.push_str("x\n");
	}
	build_document(&text)
}

pub(crate) fn temp_test_path(name: &str) -> PathBuf {
	let nanos = SystemTime::now()
		.duration_since(UNIX_EPOCH)
		.expect("time should move forward")
		.as_nanos();
	std::env::temp_dir().join(format!("baryon-{}-{}-{}", name, std::process::id(), nanos))
}

pub(crate) fn build_exact_metric_physical_document(
	chunks: &[&str],
) -> (UastRegistry, crate::ecs::NodeId, PathBuf) {
	let path = temp_test_path("core-exact-metric-physical");
	let registry = UastRegistry::new((chunks.len() + 1) as u32 + 4);
	let mut chunk = registry
		.reserve_chunk((chunks.len() + 1) as u32)
		.expect("OOM");
	let total_len = chunks.iter().map(|part| part.len() as u64).sum();
	let total_newlines = chunks
		.iter()
		.map(|part| part.bytes().filter(|&b| b == b'\n').count() as u64)
		.sum();
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: total_len,
			newlines: total_newlines,
		},
	);

	let mut byte_offset = 0u64;
	for part in chunks {
		let leaf = chunk.spawn_node(
			SemanticKind::Token,
			Some(SvpPointer {
				lba: byte_offset / 512,
				byte_length: part.len() as u32,
				device_id: 78,
				head_trim: (byte_offset % 512) as u16,
			}),
			SpanMetrics {
				byte_length: part.len() as u64,
				newlines: part.bytes().filter(|&b| b == b'\n').count() as u64,
			},
		);
		chunk.append_local_child(root, leaf);
		registry.set_terminal_newline(leaf, part.as_bytes().last() == Some(&b'\n'));
		byte_offset += part.len() as u64;
	}

	std::fs::write(&path, chunks.concat()).expect("write temp file");
	registry
		.register_device_path(78, path.to_str().expect("utf8 path"))
		.expect("register backing");
	registry.install_root_child_line_index(root);
	(registry, root, path)
}

pub(crate) fn spawn_test_engine(
	initial_minimap_enabled: bool,
	initial_wrap_enabled: bool,
	_settings_path: Option<PathBuf>,
) -> (
	CommandSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	let (tx_cmd, _input, rx_view, handle) = spawn_test_engine_with_input(
		initial_minimap_enabled,
		initial_wrap_enabled,
		_settings_path,
	);
	(tx_cmd, rx_view, handle)
}

pub(crate) fn spawn_test_engine_with_input(
	initial_minimap_enabled: bool,
	initial_wrap_enabled: bool,
	_settings_path: Option<PathBuf>,
) -> (
	CommandSender,
	InputSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	let (tx_cmd, input, tx_internal, rx_request) = super::request::channel();
	let (tx_view, rx_view) = mpsc::channel::<Viewport>();
	let (tx_io_notify, _rx_io_notify) = mpsc::channel::<()>();
	let registry = Arc::new(UastRegistry::new(1_000_000));
	let resolver = Arc::new(SvpResolver::new(registry.clone(), tx_io_notify));
	let mut lang_registry = LanguageRegistry::new();
	lang_registry.register(FsProvider);
	let lang_registry = Arc::new(lang_registry);
	let engine = Engine::new(
		registry,
		resolver,
		rx_request,
		tx_internal,
		tx_view,
		lang_registry,
		"onedark".to_string(),
		initial_minimap_enabled,
		initial_wrap_enabled,
		true,
		false,
		true,
		true,
		SaveDurability::default(),
		UiColorOverrides::default(),
		BTreeMap::new(),
	);
	let handle = thread::spawn(move || engine.run());
	(tx_cmd, input, rx_view, handle)
}

/// Like `spawn_test_engine` but registers the Rust language provider so that
/// the semantic pipeline (parsing + structural graft) runs for `.rs` files.
pub(crate) fn spawn_test_engine_with_rust(
	_settings_path: Option<PathBuf>,
) -> (
	CommandSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	spawn_test_engine_with_rust_and_language_themes(BTreeMap::new(), _settings_path)
}

pub(crate) fn spawn_test_engine_with_rust_and_language_themes(
	language_themes: BTreeMap<String, String>,
	_settings_path: Option<PathBuf>,
) -> (
	CommandSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	use crate::svp::rust_provider::RustProvider;

	let (tx_cmd, _input, tx_internal, rx_request) = super::request::channel();
	let (tx_view, rx_view) = mpsc::channel::<Viewport>();
	let (tx_io_notify, _rx_io_notify) = mpsc::channel::<()>();
	let registry = Arc::new(UastRegistry::new(1_000_000));
	let resolver = Arc::new(SvpResolver::new(registry.clone(), tx_io_notify));
	let mut lang_registry = LanguageRegistry::new();
	lang_registry.register(FsProvider);
	lang_registry.register(RustProvider::new());
	let lang_registry = Arc::new(lang_registry);
	let engine = Engine::new(
		registry,
		resolver,
		rx_request,
		tx_internal,
		tx_view,
		lang_registry,
		"onedark".to_string(),
		false,
		false,
		true,
		false,
		true,
		true,
		SaveDurability::default(),
		UiColorOverrides::default(),
		language_themes,
	);
	let handle = thread::spawn(move || engine.run());
	(tx_cmd, rx_view, handle)
}

/// Like `spawn_test_engine` but registers the CSV/TSV providers so that
/// lexical highlighting and structural grafting run for `.csv` / `.tsv` files.
pub(crate) fn spawn_test_engine_with_csv(
	_settings_path: Option<PathBuf>,
) -> (
	CommandSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	use crate::svp::csv_provider::{CsvProvider, TsvProvider};

	let (tx_cmd, _input, tx_internal, rx_request) = super::request::channel();
	let (tx_view, rx_view) = mpsc::channel::<Viewport>();
	let (tx_io_notify, _rx_io_notify) = mpsc::channel::<()>();
	let registry = Arc::new(UastRegistry::new(1_000_000));
	let resolver = Arc::new(SvpResolver::new(registry.clone(), tx_io_notify));
	let mut lang_registry = LanguageRegistry::new();
	lang_registry.register(FsProvider);
	lang_registry.register(CsvProvider);
	lang_registry.register(TsvProvider);
	let lang_registry = Arc::new(lang_registry);
	let engine = Engine::new(
		registry,
		resolver,
		rx_request,
		tx_internal,
		tx_view,
		lang_registry,
		"onedark".to_string(),
		false,
		false,
		true,
		false,
		true,
		true,
		SaveDurability::default(),
		UiColorOverrides::default(),
		BTreeMap::new(),
	);
	let handle = thread::spawn(move || engine.run());
	(tx_cmd, rx_view, handle)
}

pub(crate) fn spawn_test_engine_with_sql() -> (
	CommandSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	use crate::svp::sql_provider::SqlProvider;

	let (tx_cmd, _input, tx_internal, rx_request) = super::request::channel();
	let (tx_view, rx_view) = mpsc::channel::<Viewport>();
	let (tx_io_notify, _rx_io_notify) = mpsc::channel::<()>();
	let registry = Arc::new(UastRegistry::new(1_000_000));
	let resolver = Arc::new(SvpResolver::new(registry.clone(), tx_io_notify));
	let mut lang_registry = LanguageRegistry::new();
	lang_registry.register(FsProvider);
	lang_registry.register(SqlProvider);
	let lang_registry = Arc::new(lang_registry);
	let engine = Engine::new(
		registry,
		resolver,
		rx_request,
		tx_internal,
		tx_view,
		lang_registry,
		"onedark".to_string(),
		false,
		false,
		true,
		false,
		true,
		true,
		SaveDurability::default(),
		UiColorOverrides::default(),
		BTreeMap::new(),
	);
	let handle = thread::spawn(move || engine.run());
	(tx_cmd, rx_view, handle)
}

pub(crate) fn spawn_test_engine_with_python_and_rune() -> (
	CommandSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	use crate::svp::python_provider::PythonProvider;
	use crate::svp::rune_provider::RuneProvider;

	let (tx_cmd, _input, tx_internal, rx_request) = super::request::channel();
	let (tx_view, rx_view) = mpsc::channel::<Viewport>();
	let (tx_io_notify, _rx_io_notify) = mpsc::channel::<()>();
	let registry = Arc::new(UastRegistry::new(1_000_000));
	let resolver = Arc::new(SvpResolver::new(registry.clone(), tx_io_notify));
	let mut lang_registry = LanguageRegistry::new();
	lang_registry.register(FsProvider);
	lang_registry.register(PythonProvider);
	lang_registry.register(RuneProvider);
	let lang_registry = Arc::new(lang_registry);
	let engine = Engine::new(
		registry,
		resolver,
		rx_request,
		tx_internal,
		tx_view,
		lang_registry,
		"onedark".to_string(),
		false,
		false,
		true,
		false,
		true,
		true,
		SaveDurability::default(),
		UiColorOverrides::default(),
		BTreeMap::new(),
	);
	let handle = thread::spawn(move || engine.run());
	(tx_cmd, rx_view, handle)
}

/// Like `spawn_test_engine` but registers the Markdown provider so that `.md`
/// files receive the same language identity as the full app.
pub(crate) fn spawn_test_engine_with_markdown(
	_settings_path: Option<PathBuf>,
) -> (
	CommandSender,
	mpsc::Receiver<Viewport>,
	thread::JoinHandle<()>,
) {
	use crate::svp::markdown_provider::MarkdownProvider;

	let (tx_cmd, _input, tx_internal, rx_request) = super::request::channel();
	let (tx_view, rx_view) = mpsc::channel::<Viewport>();
	let (tx_io_notify, _rx_io_notify) = mpsc::channel::<()>();
	let registry = Arc::new(UastRegistry::new(1_000_000));
	let resolver = Arc::new(SvpResolver::new(registry.clone(), tx_io_notify));
	let mut lang_registry = LanguageRegistry::new();
	lang_registry.register(FsProvider);
	lang_registry.register(MarkdownProvider);
	let lang_registry = Arc::new(lang_registry);
	let engine = Engine::new(
		registry,
		resolver,
		rx_request,
		tx_internal,
		tx_view,
		lang_registry,
		"onedark".to_string(),
		false,
		false,
		true,
		false,
		true,
		true,
		SaveDurability::default(),
		UiColorOverrides::default(),
		BTreeMap::new(),
	);
	let handle = thread::spawn(move || engine.run());
	(tx_cmd, rx_view, handle)
}

pub(crate) fn recv_view(rx_view: &mpsc::Receiver<Viewport>) -> Viewport {
	rx_view
		.recv_timeout(Duration::from_secs(2))
		.expect("expected viewport update")
}

pub(crate) fn build_mixed_save_document() -> (UastRegistry, crate::ecs::NodeId) {
	let registry = UastRegistry::new(16);
	let mut chunk = registry.reserve_chunk(4).expect("OOM");
	let root = chunk.spawn_node(
		SemanticKind::RelationalTable,
		None,
		SpanMetrics {
			byte_length: 12,
			newlines: 0,
		},
	);
	let first = chunk.spawn_node(
		SemanticKind::Token,
		Some(SvpPointer {
			lba: 0,
			byte_length: 5,
			device_id: FILE_DEVICE_ID,
			head_trim: 0,
		}),
		SpanMetrics {
			byte_length: 5,
			newlines: 0,
		},
	);
	let middle = chunk.spawn_node(
		SemanticKind::Token,
		None,
		SpanMetrics {
			byte_length: 2,
			newlines: 0,
		},
	);
	let last = chunk.spawn_node(
		SemanticKind::Token,
		Some(SvpPointer {
			lba: 0,
			byte_length: 5,
			device_id: FILE_DEVICE_ID,
			head_trim: 5,
		}),
		SpanMetrics {
			byte_length: 5,
			newlines: 0,
		},
	);
	chunk.append_local_child(root, first);
	chunk.append_local_child(root, middle);
	chunk.append_local_child(root, last);
	unsafe {
		*registry.virtual_data[middle.index()].get() = Some(b"ZZ".to_vec());
	}
	registry.metrics_inflated[middle.index()].store(true, std::sync::atomic::Ordering::Release);
	registry.set_terminal_newline(middle, false);
	(registry, root)
}
