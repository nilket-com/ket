#[derive(Debug, Default)]
pub struct MenuState {
	open_root: Option<String>,
	open_path: Vec<String>,
}

impl MenuState {
	pub fn is_root_open(&self, id: &str) -> bool {
		self.open_root.as_deref() == Some(id)
	}

	pub fn is_submenu_open(&self, depth: usize, id: &str) -> bool {
		self.open_path.get(depth).is_some_and(|open| open == id)
	}

	pub fn open_root(&self) -> Option<&str> {
		self.open_root.as_deref()
	}

	pub fn open_path(&self) -> &[String] {
		&self.open_path
	}

	pub fn set_open_root(&mut self, id: String) {
		self.open_root = Some(id);
		self.open_path.clear();
	}

	pub fn set_open_submenu(&mut self, depth: usize, id: String) {
		self.open_path.truncate(depth);
		self.open_path.push(id);
	}

	pub fn trim_path(&mut self, depth: usize) {
		self.open_path.truncate(depth);
	}

	pub fn close(&mut self) {
		self.open_root = None;
		self.open_path.clear();
	}
}
