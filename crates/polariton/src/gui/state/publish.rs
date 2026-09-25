//! One captured publish job, shared by the menu, batch, and live clients.

use super::{AppState, publish_request};
use crate::editor::HostPlotExport;
use crate::publication::{Package, PublishError, PublishRequest};
use crate::selection::Publication;
use std::path::{Path, PathBuf};

pub(super) struct PublishJob {
	witness: Vec<Option<Publication>>,
	exports: Vec<HostPlotExport>,
	request: PublishRequest,
	pub(super) root: PathBuf,
	kept: Option<(
		crate::reporting::Reporter,
		crate::reporting::Reason,
		std::sync::Arc<std::sync::atomic::AtomicBool>,
	)>,
}

impl AppState {
	pub(super) fn prepare_publish(&self, root: &Path) -> Result<PublishJob, PublishError> {
		let witness = self.code_editor.host_plot_publications();
		// An orphaned pane is kept on screen, but its source no longer
		// publishes it, so it is left out of the package. Stale and running
		// panes keep their established eligibility; every figure's status is
		// reported by describe and the receipts.
		let exports = self
			.code_editor
			.host_plot_exports()
			.into_iter()
			.filter(|export| export.status != "orphaned")
			.collect::<Vec<_>>();
		if exports.is_empty() {
			return Err(PublishError::NoFigures);
		}
		if root.as_os_str().is_empty() {
			return Err(PublishError::Write("a directory path is required".into()));
		}
		Ok(PublishJob {
			witness,
			exports,
			request: publish_request(self),
			root: root.to_owned(),
			kept: self
				.welcome
				.own_kind
				.filter(|_| self.welcome.rendered && self.welcome.changed)
				.map(|kind| (self.reporting.clone(), kind, self.welcome.kept.clone())),
		})
	}
}

impl PublishJob {
	pub(super) fn run(self) -> Result<Package, PublishError> {
		let package = crate::publication::assemble_and_write(
			&self.witness,
			self.exports,
			&self.request,
			&self.root,
		)?;
		if let Some((reporter, kind, kept)) = self.kept
			&& !kept.swap(true, std::sync::atomic::Ordering::Relaxed)
		{
			reporter.emit(crate::reporting::Step::FirstAnalysisKept, Some(kind), false);
		}
		Ok(package)
	}
}

#[cfg(all(test, unix, feature = "toron"))]
mod tests {
	use super::*;
	#[test]
	fn product_0160_partial_publish_failure_keeps_files_and_reports_the_root() {
		use crate::driver::{Command, CommandReceipt, PublishRefusal};
		let (mut session, _) = super::super::product_0158_driver_tests::session();
		let root = std::env::temp_dir().join(format!(
			"p16-partial-{}-{}",
			std::process::id(),
			crate::selection::SurfaceIncarnation::mint().token()
		));
		let mut job = session
			.application_mut()
			.state()
			.prepare_publish(&root)
			.unwrap();
		// The publisher uses the authored name as a path component. Data sorts
		// before figures; this overlong component fails only after data landed.
		job.exports[0].view_name = Some("x".repeat(256));
		let error = match job.run() {
			Err(error) => error,
			Ok(_) => panic!("expected a path-component write failure"),
		};
		assert!(matches!(error, PublishError::Write(_)));
		assert!(root.join("data").is_dir());
		assert!(
			std::fs::read_dir(root.join("data"))
				.unwrap()
				.next()
				.is_some()
		);
		let (_, reply, mut answers) = crate::live::transport::test_request();
		let receipt = CommandReceipt::new(&Command::Publish {
			directory: root.display().to_string(),
		});
		session
			.application_mut()
			.dispatch(super::super::Message::AgentPublishFinished {
				id: 9,
				reply,
				receipt,
				root: root.display().to_string(),
				result: Err(PublishRefusal::from(&error)),
			});
		assert!(
			matches!(answers.try_recv().unwrap(), crate::live::Event::Failed { id: 9, root: Some(actual), receipt, .. }
			if actual == root.display().to_string() && receipt.outcome == crate::driver::Outcome::Failed)
		);
		assert!(
			root.join("data").is_dir(),
			"failure does not claim rollback"
		);
		std::fs::remove_dir_all(root).unwrap();
	}
}
