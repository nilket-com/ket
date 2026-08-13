#[path = "../../src/plot/kernels/funnel_stages.rs"]
mod funnel_stages;

use funnel_stages::FunnelStages;

fn main() {
	let _ = FunnelStages { stages: Vec::new() };
}
