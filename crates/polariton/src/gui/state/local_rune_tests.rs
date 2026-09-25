//! Local Rune runs against the real editor: buffer close cancels a fetch,
//! and an edit after the run read its source discards the run's output.

use super::headless::HeadlessApplication;
use super::*;
use libaryon::core::{DocByte, RequestId};
use libaryon::engine::DocumentTextEdit;
use std::io::{Read, Write};
use std::net::TcpListener;
use std::sync::mpsc;

const DEADLINE: Duration = Duration::from_secs(20);

struct Fixture {
	root: std::path::PathBuf,
	app: HeadlessApplication,
	buffer: BufferId,
}

impl Drop for Fixture {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.root);
	}
}

impl Fixture {
	fn open(name: &str, source: &str) -> Self {
		let root = std::env::temp_dir().join(format!(
			"polariton-local-rune-{name}-{}",
			std::process::id()
		));
		let _ = std::fs::remove_dir_all(&root);
		std::fs::create_dir_all(&root).unwrap();
		let path = root.join("source.rn");
		std::fs::write(&path, source).unwrap();
		let mut app = HeadlessApplication::open_target(
			StartupTarget::Ephemeral {
				initial_file: Some(path.display().to_string()),
			},
			crate::configuration::StartupData::default(),
		)
		.unwrap();
		app.settle_until(DEADLINE, |s| {
			s.code_editor
				.active_buffer_id()
				.is_some_and(|buffer| s.code_editor.document_snapshot(buffer).is_some())
				&& s.code_editor
					.active_language_id()
					.is_some_and(|l| l.as_str() == "rune")
		})
		.unwrap();
		let buffer = app.state().code_editor.active_buffer_id().unwrap();
		Self { root, app, buffer }
	}

	fn generation(&self) -> libaryon::core::StateId {
		self.app
			.state()
			.code_editor
			.document_snapshot(self.buffer)
			.unwrap()
			.1
	}

	/// A person's edit: a comment line at the top of the source.
	fn edit(&mut self) {
		let before = self.generation();
		assert!(self.app.state().code_editor.apply_document_edits(
			RequestId::new(u64::MAX),
			self.buffer,
			before,
			vec![DocumentTextEdit {
				start: DocByte::new(0),
				end: DocByte::new(0),
				replacement: "// edited\n".into(),
			}],
		));
		self.app.state_mut().code_editor.request_content();
		let buffer = self.buffer;
		self.app
			.settle_until(DEADLINE, |s| {
				s.code_editor
					.document_snapshot(buffer)
					.is_some_and(|(_, generation)| generation != before)
			})
			.unwrap();
	}

	/// Run the source, driving the application until its fetch has reached
	/// the server: running needs the engine's text snapshot first.
	fn run_until_requested(&mut self, server: &Server) {
		self.app.dispatch(Message::Run);
		self.app
			.settle_until(DEADLINE, |_| server.requested.try_recv().is_ok())
			.expect("the fetch reached the server");
	}

	/// Let in-flight work settle without asserting on it.
	fn quiesce(&mut self) {
		let _ = self.app.settle_until(Duration::from_millis(500), |_| false);
	}

	fn plots(&self) -> usize {
		self.app.state().code_editor.host_plot_exports().len()
	}
}

/// A loopback server for one request. `requested` fires once the request has
/// arrived; the reply waits for `reply` (or never comes if it is dropped);
/// `released` fires when the client drops the connection.
struct Server {
	url: String,
	requested: mpsc::Receiver<()>,
	reply: mpsc::Sender<&'static str>,
	released: mpsc::Receiver<()>,
}

fn server() -> Server {
	let listener = TcpListener::bind("127.0.0.1:0").unwrap();
	let address = listener.local_addr().unwrap();
	let (requested_tx, requested) = mpsc::channel();
	let (reply, reply_rx) = mpsc::channel::<&'static str>();
	let (released_tx, released) = mpsc::channel();
	std::thread::spawn(move || {
		if let Ok((mut stream, _)) = listener.accept() {
			let mut buffer = [0_u8; 4096];
			let _ = stream.read(&mut buffer);
			let _ = requested_tx.send(());
			let _ = stream.set_read_timeout(Some(Duration::from_millis(50)));
			loop {
				if let Ok(body) = reply_rx.try_recv() {
					let _ = write!(
						stream,
						"HTTP/1.1 200 OK\r\ncontent-length: {}\r\nconnection: close\r\n\r\n{body}",
						body.len()
					);
					return;
				}
				match stream.read(&mut buffer) {
					Ok(0) => break,
					Ok(_) => {}
					Err(error)
						if matches!(
							error.kind(),
							std::io::ErrorKind::WouldBlock | std::io::ErrorKind::TimedOut
						) => {}
					Err(_) => break,
				}
			}
			let _ = released_tx.send(());
		}
	});
	Server {
		url: format!("http://{address}"),
		requested,
		reply,
		released,
	}
}

fn fetch_and_plot(url: &str) -> String {
	format!(
		r#"pub async fn main(workspace, viz) {{
	let body = http::get("{url}/data").await?.body;
	let rows = [];
	for line in body.split("\n") {{
		let parts = [];
		for part in line.split(",") {{ parts.push(part); }}
		if parts.len() == 2 {{
			rows.push(#{{x: std::f64::parse(parts[0])?, y: std::f64::parse(parts[1])?}});
		}}
	}}
	viz.plot(viz.frame("probe", rows)?).scatter("x", "y").line().show()?;
}}
"#
	)
}

#[test]
fn closing_the_source_cancels_its_fetch_and_publishes_nothing() {
	let server = server();
	let mut f = Fixture::open("close", &fetch_and_plot(&server.url));
	f.run_until_requested(&server);
	assert!(f.app.state().local_rune_runs.contains_key(&f.buffer));

	// The last tab of the only pane cannot close; beside another pane it
	// closes with its pane, as a person closing the source would see.
	f.app.state_mut().code_editor.split_new_pane();
	f.quiesce();
	assert!(f.app.state().local_rune_runs.contains_key(&f.buffer));
	f.app.state().code_editor.close_buffer_tabs(f.buffer);
	let buffer = f.buffer;
	f.app
		.settle_until(DEADLINE, |s| !s.local_rune_runs.contains_key(&buffer))
		.expect("closing the source ends its run");
	// rnx's HTTP deadline is 30 s; the server seeing the connection dropped
	// promptly is the proof the close revoked the request.
	server
		.released
		.recv_timeout(Duration::from_secs(5))
		.expect("the close released the connection");
	f.quiesce();
	assert_eq!(f.plots(), 0);
	assert!(!f.app.state().rune_buffer_revisions.contains_key(&buffer));
}

#[test]
fn an_edit_during_the_fetch_discards_its_output() {
	let server = server();
	let mut f = Fixture::open("fetch-edit", &fetch_and_plot(&server.url));
	f.run_until_requested(&server);
	f.edit();
	server.reply.send("1,2\n2,4\n3,8\n").unwrap();
	let buffer = f.buffer;
	f.app
		.settle_until(DEADLINE, |s| !s.local_rune_runs.contains_key(&buffer))
		.expect("the run settles");
	f.quiesce();
	assert_eq!(f.plots(), 0);
	assert!(
		f.app.state().status_msg.contains("discarded"),
		"{}",
		f.app.state().status_msg
	);
}

#[test]
fn an_unedited_fetch_publishes_its_pane() {
	let server = server();
	let mut f = Fixture::open("fetch-control", &fetch_and_plot(&server.url));
	f.run_until_requested(&server);
	server.reply.send("1,2\n2,4\n3,8\n").unwrap();
	f.app
		.settle_until(DEADLINE, |s| s.code_editor.host_plot_exports().len() == 1)
		.expect("the pane is published");
	assert!(!f.app.state().local_rune_runs.contains_key(&f.buffer));
}

/// A real planned plot from a local source, with no fetch.
fn probe_plot() -> crate::plot::request::ResolvedPlot {
	let (_cancel, cancelled) = tokio::sync::oneshot::channel();
	let mut execution = crate::rune_local::execute(
		"<probe>",
		"pub fn main(workspace, viz) { viz.plot(viz.frame(\"probe\", [#{x: 1, y: 2.0}, #{x: 2, y: 3.0}])?).scatter(\"x\", \"y\").line().show()?; }",
		RuneCapabilities::default(),
		cancelled,
	)
	.unwrap();
	crate::plot::request::plan(execution.requests.remove(0)).unwrap()
}

/// Record a local run of the buffer at `revision`, at its current generation.
fn in_flight(f: &mut Fixture, revision: u64) {
	let buffer = f.buffer;
	let generation = f.generation();
	let state = f.app.state_mut();
	state.rune_buffer_revisions.insert(buffer, revision);
	state.local_rune_runs.insert(
		buffer,
		LocalRun {
			revision,
			generation,
			cancel: None,
		},
	);
}

fn kernel(
	resolved: &crate::plot::request::ResolvedPlot,
) -> Arc<dyn crate::plot::common::PlotKernel + Send + Sync> {
	crate::plot::factory::create_plot_from_artifact(
		&resolved.resolved_spec,
		&resolved.data,
		&resolved.artifact,
		1200,
		1200,
	)
}

fn built(resolved: &crate::plot::request::ResolvedPlot) -> crate::gui::messages::BuiltPane {
	crate::gui::messages::BuiltPane {
		identity: LocalViewIdentity::Anonymous(0),
		presentation: ViewPresentation {
			content_kind: ViewContentKind::Plot,
			display_label: resolved.source_name.clone(),
			authored_title: crate::plot::spec::Presence::Inherit,
		},
		settings: Default::default(),
		kernel: kernel(resolved),
		recipe: crate::plot::recipe::PlotRecipe::single(
			Arc::clone(&resolved.artifact),
			resolved.resolved_spec.clone(),
			1200,
			720,
		),
		#[cfg(feature = "toron")]
		continuation: None,
	}
}

/// A local run's settled batch: every kernel built, as the worker sends it.
fn batch(f: &Fixture, revision: u64, resolved: &crate::plot::request::ResolvedPlot) -> Message {
	Message::RunRuneBufferResult {
		source_buffer: f.buffer,
		revision,
		result: Ok(crate::gui::messages::RuneBufferOutcome {
			display: None,
			plot: None,
			panes: Some(vec![built(resolved)]),
		}),
	}
}

/// Clear the buffer's run as a person running emptied text does.
fn run_empty(f: &mut Fixture) {
	let buffer = f.buffer;
	let _ = run_rune_code_at_generation(
		f.app.state_mut(),
		buffer,
		String::new(),
		libaryon::core::StateId::ZERO,
	);
}

#[test]
fn an_edit_before_the_batch_commits_discards_it() {
	let mut f = Fixture::open("batch-edit", "pub fn main(workspace, viz) {}\n");
	let resolved = probe_plot();
	in_flight(&mut f, 7);
	f.edit();
	f.app.dispatch(batch(&f, 7, &resolved));
	f.quiesce();
	assert_eq!(f.plots(), 0);
	assert!(!f.app.state().local_rune_runs.contains_key(&f.buffer));
	assert!(
		f.app.state().status_msg.contains("discarded"),
		"{}",
		f.app.state().status_msg
	);
}

#[test]
fn an_unedited_batch_commits() {
	let mut f = Fixture::open("batch-control", "pub fn main(workspace, viz) {}\n");
	let resolved = probe_plot();
	in_flight(&mut f, 7);
	f.app.dispatch(batch(&f, 7, &resolved));
	f.app
		.settle_until(DEADLINE, |s| s.code_editor.host_plot_exports().len() == 1)
		.expect("the batch is committed");
	assert!(!f.app.state().local_rune_runs.contains_key(&f.buffer));
}

#[test]
fn an_empty_run_retracts_a_queued_batch() {
	let mut f = Fixture::open("empty-queued", "pub fn main(workspace, viz) {}\n");
	let resolved = probe_plot();
	in_flight(&mut f, 7);
	let queued = batch(&f, 7, &resolved);
	run_empty(&mut f);
	f.app.dispatch(queued);
	f.quiesce();
	assert_eq!(f.plots(), 0);
	assert!(
		f.app
			.state()
			.code_editor
			.rune_view_panel_id(f.buffer, &LocalViewIdentity::Anonymous(0))
			.is_none(),
		"no pane was prepared for the retracted batch"
	);
}

/// The plain path still builds its one plot after settling; an empty run
/// must retract that plot even once its kernel is built.
#[test]
fn an_empty_run_retracts_an_already_built_plain_plot() {
	let mut f = Fixture::open("empty-built", "pub fn main(workspace, viz) {}\n");
	let resolved = probe_plot();
	let buffer = f.buffer;
	let state = f.app.state_mut();
	state.rune_buffer_revisions.insert(buffer, 7);
	let _build = update(
		state,
		Message::RunRuneBufferResult {
			source_buffer: buffer,
			revision: 7,
			result: Ok(crate::gui::messages::RuneBufferOutcome {
				display: None,
				plot: Some(resolved.clone()),
				panes: None,
			}),
		},
	);
	let panel_id = state
		.code_editor
		.rune_view_panel_id(buffer, &LocalViewIdentity::Anonymous(0))
		.expect("the pane was prepared");
	let ready = Message::RuneBufferPlotReady {
		source_buffer: buffer,
		revision: 7,
		panel_id,
		display_label: "probe".into(),
		kernel: kernel(&resolved),
		resolved,
	};
	run_empty(&mut f);
	f.app.dispatch(ready);
	f.quiesce();
	assert_eq!(f.plots(), 0);
	assert_eq!(f.app.state().rune_buffer_revisions.get(&f.buffer), Some(&8));
}

/// A source publishing one pane per `(title, points)`; `fail` makes it
/// return an error after publishing, so the batch must not commit.
fn panes_source(panes: &[(&str, usize)], fail: bool) -> String {
	let mut body = String::from("pub fn main(workspace, viz) {\n");
	for (title, points) in panes {
		body.push_str("\tlet rows = [];\n");
		body.push_str(&format!(
			"\tfor i in 0..{points} {{ rows.push(#{{x: i, y: i * 2, series: if i % 2 == 0 {{ \"even\" }} else {{ \"odd\" }}}}); }}\n"
		));
		body.push_str(&format!(
			"\tviz.plot(viz.frame(\"{title}\", rows)?).scatter(\"x\", \"y\", \"series\").line().show()?;\n"
		));
	}
	if fail {
		body.push_str("\tErr(\"late failure\")\n");
	}
	body.push_str("}\n");
	body
}

impl Fixture {
	fn replace_source(&mut self, text: &str) {
		let (old, before) = {
			let (old, generation) = self
				.app
				.state()
				.code_editor
				.document_snapshot(self.buffer)
				.unwrap();
			(old.len() as u64, generation)
		};
		assert!(self.app.state().code_editor.apply_document_edits(
			RequestId::new(u64::MAX - 1),
			self.buffer,
			before,
			vec![DocumentTextEdit {
				start: DocByte::new(0),
				end: DocByte::new(old),
				replacement: text.into(),
			}],
		));
		self.app.state_mut().code_editor.request_content();
		let buffer = self.buffer;
		self.app
			.settle_until(DEADLINE, |s| {
				s.code_editor
					.document_snapshot(buffer)
					.is_some_and(|(_, generation)| generation != before)
			})
			.unwrap();
	}

	/// Run the source and wait for its batch to settle, one way or the other.
	fn run_settled(&mut self) {
		let buffer = self.buffer;
		let before = self.app.state().rune_buffer_revisions.get(&buffer).copied();
		self.app.dispatch(Message::Run);
		self.app
			.settle_until(DEADLINE, |s| {
				s.rune_buffer_revisions.get(&buffer).copied() != before
					&& !s.local_rune_runs.contains_key(&buffer)
			})
			.unwrap();
		self.quiesce();
	}

	/// The authored view names of the exported figures, in export order.
	#[cfg(feature = "toron")]
	fn view_names(&self) -> Vec<String> {
		self.app
			.state()
			.code_editor
			.host_plot_exports()
			.into_iter()
			.filter_map(|export| export.view_name)
			.collect()
	}

	fn titles(&self) -> Vec<String> {
		self.app
			.state()
			.code_editor
			.host_plot_exports()
			.into_iter()
			.map(|export| export.title)
			.collect()
	}
}

impl Fixture {
	/// Each pane this buffer owns, in publication order, with its status.
	fn pane_statuses(&self, upto: usize) -> Vec<Option<&'static str>> {
		(0..upto)
			.map(|index| {
				self.app
					.state()
					.code_editor
					.rune_view_panel_status(self.buffer, &LocalViewIdentity::Anonymous(index))
			})
			.collect()
	}
}

/// Panes a batch no longer publishes are orphaned, the product's retirement
/// for a view that disappeared (records 0014/0015): kept, marked, until reset.
#[test]
fn a_batch_replaces_the_panes_whole() {
	let mut f = Fixture::open(
		"batch-panes",
		&panes_source(&[("ten", 4), ("thirty", 5), ("spread", 6)], false),
	);
	f.run_settled();
	assert_eq!(f.titles(), vec!["ten", "thirty", "spread"]);
	assert_eq!(f.pane_statuses(3), vec![Some("fresh"); 3]);

	// A smaller successful batch retires the excess.
	f.replace_source(&panes_source(&[("ten", 3)], false));
	f.run_settled();
	assert_eq!(
		f.pane_statuses(3),
		vec![Some("fresh"), Some("orphaned"), Some("orphaned")]
	);

	// A failed batch changes no pane's content and publishes nothing new.
	let before = f.titles();
	f.replace_source(&panes_source(&[("a", 2), ("b", 2)], true));
	f.run_settled();
	assert_eq!(f.titles(), before);
	assert_ne!(f.pane_statuses(1), vec![Some("orphaned")]);
	assert!(
		f.app.state().status_error.contains("late failure"),
		"{}",
		f.app.state().status_error
	);

	// A successful batch of nothing retires every pane it owned.
	f.replace_source("pub fn main(workspace, viz) {}\n");
	f.run_settled();
	assert_eq!(f.pane_statuses(3), vec![Some("orphaned"); 3]);
}

#[test]
fn run_source_settles_after_the_whole_batch() {
	use crate::driver::{Command, Outcome};
	let mut f = Fixture::open("batch-receipt", &panes_source(&[("one", 3), ("two", 4)], false));
	let receipt = f.app.apply_command(&Command::RunSource {});
	assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
	assert_eq!(receipt.figures.as_ref().map(Vec::len), Some(2), "{receipt:?}");
}

mod agent_api {
	use super::*;
	use crate::driver::{Command, CommandReceipt, Outcome};

	fn statuses(receipt: &CommandReceipt) -> Vec<(String, String)> {
		let mut figures: Vec<(String, String)> = receipt
			.figures
			.as_ref()
			.expect("the receipt describes figures")
			.iter()
			.map(|figure| (figure.title.clone(), figure.status.clone()))
			.collect();
		figures.sort();
		figures
	}

	fn published(f: &mut Fixture, name: &str) -> std::result::Result<usize, CommandReceipt> {
		let directory = f.root.join(name);
		let receipt = f.app.apply_command(&Command::Publish {
			directory: directory.display().to_string(),
		});
		if receipt.outcome != Outcome::Applied {
			return Err(receipt);
		}
		Ok(std::fs::read_dir(directory.join("figures")).unwrap().count())
	}

	fn run(f: &mut Fixture) -> CommandReceipt {
		let receipt = f.app.apply_command(&Command::RunSource {});
		assert_eq!(receipt.outcome, Outcome::Applied, "{receipt:?}");
		receipt
	}

	#[test]
	fn receipts_describe_and_publish_only_current_output_as_current() {
		let mut f = Fixture::open(
			"api-panes",
			&panes_source(&[("ten", 4), ("thirty", 5), ("spread", 6)], false),
		);
		let three = run(&mut f);
		assert_eq!(
			statuses(&three),
			vec![
				("spread".into(), "fresh".into()),
				("ten".into(), "fresh".into()),
				("thirty".into(), "fresh".into()),
			]
		);
		assert_eq!(published(&mut f, "three").unwrap(), 3);

		f.replace_source(&panes_source(&[("ten", 3)], false));
		let one = run(&mut f);
		let orphaned = vec![
			("spread".into(), "orphaned".into()),
			("ten".into(), "fresh".into()),
			("thirty".into(), "orphaned".into()),
		];
		assert_eq!(statuses(&one), orphaned);
		let described = f.app.apply_command(&Command::Describe {});
		assert_eq!(statuses(&described), orphaned);
		assert_eq!(published(&mut f, "one").unwrap(), 1, "orphans are not published");

		f.replace_source("pub fn main(workspace, viz) {}\n");
		let none = run(&mut f);
		assert!(
			statuses(&none).iter().all(|(_, status)| status == "orphaned"),
			"{:?}",
			statuses(&none)
		);
		let refused = published(&mut f, "none").unwrap_err();
		assert_eq!(refused.outcome, Outcome::Refused, "{refused:?}");
	}

	#[test]
	fn a_same_count_rerun_reports_the_new_figures() {
		let mut f = Fixture::open("api-rerun", &panes_source(&[("a", 3), ("b", 4)], false));
		let first = run(&mut f);
		let before: std::collections::HashSet<String> = first
			.figures
			.as_ref()
			.unwrap()
			.iter()
			.map(|figure| figure.figure.clone())
			.collect();
		assert_eq!(before.len(), 2);

		f.replace_source(&panes_source(&[("a", 7), ("b", 9)], false));
		let second = run(&mut f);
		let figures = second.figures.as_ref().unwrap();
		assert_eq!(figures.len(), 2, "{second:?}");
		for figure in figures {
			assert_eq!(figure.status, "fresh");
			assert!(
				!before.contains(&figure.figure),
				"figure {} is a previous batch's incarnation",
				figure.figure
			);
		}
	}
}

/// A local source declaring its panes with the shared view vocabulary.
#[cfg(feature = "toron")]
fn views_source(views: &[(&str, &str)]) -> String {
	let mut body = String::from(
		"pub fn main(workspace, viz) {\n\tlet rows = [#{x: 1, y: 2.0, s: \"a\"}, #{x: 2, y: 3.0, s: \"b\"}];\n\tlet frame = viz.frame(\"f\", rows)?;\n\tframe.views([\n",
	);
	for (name, y) in views {
		body.push_str(&format!(
			"\t\tview(\"{name}\", figure([title(\"{name}\"), palette(\"tab20c\"), line(\"x\", \"{y}\", \"s\")])),\n"
		));
	}
	body.push_str("\t])?;\n}\n");
	body
}

#[cfg(feature = "toron")]
#[test]
fn a_views_batch_commits_named_panes_that_refuse_writeback() {
	let mut f = Fixture::open("views-commit", &views_source(&[("alpha", "y"), ("beta", "y")]));
	f.run_settled();
	assert_eq!(f.view_names(), vec!["alpha", "beta"]);
	for name in ["alpha", "beta"] {
		let panel = f
			.app
			.state()
			.code_editor
			.rune_view_panel_id(f.buffer, &LocalViewIdentity::Named(name.to_owned()))
			.expect("a named pane");
		let editor = &f.app.state().code_editor;
		assert!(editor.plot_writeback_context(panel).is_none(), "{name} is not source-attached");
		assert_eq!(
			editor.plot_writeback_status(panel).as_deref(),
			Some(LOCAL_WRITEBACK_REFUSAL)
		);
	}
}

#[cfg(feature = "toron")]
#[test]
fn a_bad_view_fails_the_whole_batch_and_keeps_prior_panes() {
	let mut f = Fixture::open("views-fail", &views_source(&[("alpha", "y")]));
	f.run_settled();
	assert_eq!(f.view_names(), vec!["alpha"]);
	// The second view names a column the frame lacks: nothing commits.
	f.replace_source(&views_source(&[("gamma", "y"), ("delta", "missing")]));
	f.run_settled();
	assert_eq!(f.view_names(), vec!["alpha"]);
	assert!(
		f.app.state().status_error.contains("missing"),
		"{}",
		f.app.state().status_error
	);
}

/// A skin is resolved with the batch, after `main` ran; its error is still
/// located at the view that named it, and nothing commits.
#[cfg(feature = "toron")]
#[test]
fn an_unknown_skin_fails_the_batch_at_its_view() {
	let source = "pub fn main(workspace, viz) {\n\tlet frame = viz.frame(\"f\", [#{x: 1, y: 2.0}])?;\n\tframe.views([view(\"a\", figure([skin(\"not_a_skin\"), line(\"x\", \"y\")]))])?;\n}\n";
	let mut f = Fixture::open("views-skin", source);
	f.run_settled();
	assert!(f.view_names().is_empty());
	let error = &f.app.state().status_error;
	assert!(error.contains(":3:"), "{error}");
	assert!(error.contains("not_a_skin"), "{error}");
}

/// Mixed output styles keep the order the source produced them in.
#[cfg(feature = "toron")]
#[test]
fn show_and_views_keep_source_order() {
	let source = "pub fn main(workspace, viz) {\n\tlet rows = [#{x: 1, y: 2.0}, #{x: 2, y: 3.0}];\n\tviz.plot(viz.frame(\"first\", rows)?).scatter(\"x\", \"y\").show()?;\n\tviz.frame(\"f\", rows)?.views([view(\"middle\", figure([line(\"x\", \"y\")]))])?;\n\tviz.plot(viz.frame(\"last\", rows)?).scatter(\"x\", \"y\").show()?;\n}\n";
	let mut f = Fixture::open("mixed-order", source);
	f.run_settled();
	let order: Vec<String> = f
		.app
		.state()
		.code_editor
		.host_plot_exports()
		.into_iter()
		.map(|export| export.view_name.unwrap_or(export.title))
		.collect();
	assert_eq!(order, vec!["first", "middle", "last"]);
}
