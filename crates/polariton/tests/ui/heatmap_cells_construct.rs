#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/plot/kernels/heatmap_cells.rs"]
mod heatmap_cells;

use heatmap_cells::{HeatmapCellRow, HeatmapCells};

fn main() {
	let cells = HeatmapCells::from_rows(vec![HeatmapCellRow {
		x: Some("x".to_owned()),
		y: Some("y".to_owned()),
		value: Some(7.0),
	}])
	.expect("valid cell relation");
	assert_eq!(cells.domain(), (7.0, 7.0));
	assert_eq!(cells.value(0, 0), Some(7.0));
}
