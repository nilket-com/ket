/// Ordered field vocabulary shared by radar and parallel coordinates.
///
/// The order of `dimensions` is authored geometry. `category` is optional
/// grouping rather than another dimension (PRODUCT-0105).
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct MultivariateFields {
	dimensions: Vec<String>,
	category: Option<String>,
}

impl MultivariateFields {
	pub fn new(dimensions: Vec<String>, category: Option<String>) -> Self {
		Self {
			dimensions,
			category,
		}
	}

	pub fn dimensions(&self) -> &[String] {
		&self.dimensions
	}

	pub fn category(&self) -> Option<&str> {
		self.category.as_deref()
	}

	pub(crate) fn set_category(&mut self, category: String) {
		self.category = Some(category);
	}
}
