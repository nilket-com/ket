#[path = "../../src/plot/kernels/heatmap_cells.rs"]
mod heatmap_cells;

use heatmap_cells::HeatmapCells;

fn main() {
	let _ = HeatmapCells {
		x_categories: vec!["x".to_owned()],
		y_categories: vec!["y".to_owned()],
		values: vec![vec![Some(7.0)]],
		domain: (7.0, 7.0),
	};
}
