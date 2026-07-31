mod client;
mod polars_compat;

use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use async_trait::async_trait;

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "endpoint",
	value: "127.0.0.1:7742",
	field_type: &AdapterFieldType::Text,
	is_secure: false,
}];

pub struct ToronAdapter {
	client: client::ToronClient,
}

impl ToronAdapter {
	pub async fn connect(endpoint: &str) -> Result<Self, client::ClientError> {
		Ok(Self {
			client: client::ToronClient::connect(endpoint).await?,
		})
	}
}

#[async_trait]
impl DatabaseAdapter for ToronAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let result = match self.client.execute(code).await {
			Ok(result) => result,
			Err(error) => return ExecutionResult::Err(error.to_string()),
		};
		if result.batches.is_empty() {
			return if result.row_count == 0 {
				ExecutionResult::CommandCompleted("Command completed.".to_owned())
			} else {
				ExecutionResult::Affected(result.row_count)
			};
		}
		match polars_compat::decode_batches(&result.batches) {
			Ok(frame) => ExecutionResult::Rows {
				frame,
				statements: 1,
			},
			Err(error) => ExecutionResult::Err(format!("Toron result conversion failed: {error}")),
		}
	}
}

#[cfg(test)]
mod tests {
	use super::{DatabaseAdapter, ExecutionResult, ToronAdapter, client};
	use crate::plot::common::{PlotRenderLayer, PlotSettings, PlotWidget};
	use crate::plot::composition::PlotComposition;
	use crate::plot::core::PlotType;
	use crate::plot::export::SvgBackend;
	use crate::plot::factory::create_plot;
	use iced::Rectangle;
	use std::path::PathBuf;

	fn live_endpoint() -> String {
		std::env::var("TORON_SQL_ENDPOINT").unwrap_or_else(|_| "127.0.0.1:7742".to_owned())
	}

	#[tokio::test]
	#[ignore = "requires a live Toron SQL endpoint"]
	async fn live_toron_query_reaches_the_existing_plot_kernel() {
		let mut adapter = ToronAdapter::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let ExecutionResult::Rows { frame, statements } = adapter
			.dispatch("SELECT x, y FROM polariton_first_contact ORDER BY x")
			.await
		else {
			panic!("Toron SELECT must return rows");
		};
		assert_eq!(statements, 1);
		assert_eq!(frame.shape(), (4, 2));

		let composition = PlotComposition::single(PlotType::Scatter);
		let kernel = create_plot(&composition, &frame, 800, 480);
		let widget = PlotWidget {
			kernel: kernel.as_ref(),
			title: "Toron first contact".to_owned(),
			padding: 0.0,
			settings: PlotSettings::default(),
			render_revision: 0,
			resize_render_suspended: false,
			layer: PlotRenderLayer::Full,
		};
		let mut backend = SvgBackend::new(800.0, 480.0);
		widget.render(
			&mut backend,
			Rectangle::new([0.0, 0.0].into(), [800.0, 480.0].into()),
		);
		let svg = backend.finish();
		assert!(
			svg.contains("<path"),
			"plot artifact contains drawn geometry"
		);
		assert!(
			svg.len() > 1_000,
			"plot artifact is substantively non-empty"
		);

		let output = std::env::var_os("TORON_FIRST_CONTACT_PLOT")
			.map(PathBuf::from)
			.unwrap_or_else(|| std::env::temp_dir().join("toron-first-contact.svg"));
		std::fs::write(&output, svg).expect("write Toron plot artifact");
		eprintln!("Toron first-contact plot: {}", output.display());
	}

	#[tokio::test]
	#[ignore = "requires a live Toron SQL endpoint"]
	async fn live_toron_preserves_errors_and_accepts_cancel_frames() {
		let client = client::ToronClient::connect(&live_endpoint())
			.await
			.expect("connect to Toron");
		let error = client
			.execute("SELEC this is deliberately invalid")
			.await
			.expect_err("invalid SQL is refused");
		assert!(matches!(
			error,
			client::ClientError::Remote {
				code: client::SqlErrorCode::SyntaxError,
				..
			}
		));
		client
			.cancel(client::QueryId::from_u128(0x746f726f6e))
			.await
			.expect("Toron accepts the cancellation frame");
	}
}
