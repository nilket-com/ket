#[derive(Debug, Clone)]
pub struct MenuRoot {
	pub id: String,
	pub label: String,
	pub items: Vec<MenuItem>,
}

#[derive(Debug, Clone)]
pub enum MenuItem {
	Action {
		id: String,
		label: String,
	},
	Submenu {
		id: String,
		label: String,
		items: Vec<MenuItem>,
	},
	Separator,
}
