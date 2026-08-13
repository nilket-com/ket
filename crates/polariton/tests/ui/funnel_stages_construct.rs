#[path = "../../src/plot/kernels/funnel_stages.rs"]
mod funnel_stages;

use funnel_stages::{FunnelStageRow, FunnelStages, FunnelValueCell};

fn main() {
	let stages = FunnelStages::from_rows(
		"stage",
		"value",
		vec![FunnelStageRow {
			stage: Some("A".to_owned()),
			value: FunnelValueCell::Exact(10.0),
		}],
	)
	.expect("valid funnel stages");
	assert_eq!(stages.first_value(), Some(10.0));
}
