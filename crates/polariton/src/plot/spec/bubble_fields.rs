use std::fmt;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum BubbleNumericField {
	X,
	Y,
	Size,
	Color,
}

impl fmt::Display for BubbleNumericField {
	fn fmt(&self, formatter: &mut fmt::Formatter<'_>) -> fmt::Result {
		formatter.write_str(match self {
			Self::X => "x",
			Self::Y => "y",
			Self::Size => "size",
			Self::Color => "color",
		})
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BubbleFieldNames {
	x: String,
	y: String,
	size: String,
	color: String,
}

impl BubbleFieldNames {
	pub(crate) fn new(x: &str, y: &str, size: &str, color: &str) -> Self {
		Self {
			x: x.to_owned(),
			y: y.to_owned(),
			size: size.to_owned(),
			color: color.to_owned(),
		}
	}

	pub(crate) fn get(&self, field: BubbleNumericField) -> &str {
		match field {
			BubbleNumericField::X => &self.x,
			BubbleNumericField::Y => &self.y,
			BubbleNumericField::Size => &self.size,
			BubbleNumericField::Color => &self.color,
		}
	}
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct BubbleFields {
	pub(crate) numeric: BubbleFieldNames,
	pub(crate) label: Option<String>,
}

impl BubbleFields {
	pub(crate) fn new(numeric: BubbleFieldNames, label: Option<String>) -> Self {
		Self { numeric, label }
	}
}
