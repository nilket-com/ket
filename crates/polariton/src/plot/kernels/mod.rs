pub mod bar;
pub mod boxplot;
pub mod bubble;
pub mod candlestick;
pub mod fill_between;
pub mod fill_between_line;
pub mod funnel;
pub mod heatmap;
pub mod hexbin;
// PRODUCT-0078 preserves the validated renderer substrate while its authored
// producer is deliberately absent until the histogram surface record.
#[allow(dead_code)]
pub mod histogram;
#[allow(dead_code)]
pub mod histogram_bins;
pub mod layered;
pub mod line;
pub mod parallel;
pub mod pie;
pub mod radar;
pub mod radial_dial;
pub mod scatter;
pub mod scatter_line;
pub mod stacked_area;
pub mod stacked_bar;
pub mod violin;
