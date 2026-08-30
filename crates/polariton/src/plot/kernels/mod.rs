pub mod bar;
// PRODUCT-0081 preserves the validated renderer substrate while its authored
// producer is deliberately absent until the box-plot surface record.
#[allow(dead_code)]
pub mod boxplot;
#[allow(dead_code)]
pub mod boxplot_tukey;
pub mod bubble;
pub mod bubble_observations;
pub mod candlestick;
pub mod fill_between;
pub mod fill_between_line;
pub mod funnel;
pub mod funnel_stages;
pub mod heatmap;
pub mod heatmap_cells;
pub mod hexbin;
pub mod hexbin_viewport;
// PRODUCT-0078 preserves the validated renderer substrate while its authored
// producer is deliberately absent until the histogram surface record.
#[allow(dead_code)]
pub mod histogram;
#[allow(dead_code)]
pub mod histogram_bins;
pub mod layered;
pub mod line;
pub mod multivariate_rows;
pub mod parallel;
pub mod pie;
pub mod pie_slices;
pub mod radar;
pub mod radial_dial;
pub mod radial_dial_readings;
pub mod scatter;
pub mod scatter_line;
pub mod stacked_area;
pub mod stacked_bar;
pub mod violin;
pub mod violin_density;
