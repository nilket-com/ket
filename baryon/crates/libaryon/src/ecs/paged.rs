use std::ops::Index;
use std::sync::OnceLock;

/// Lazily allocated, stable-address storage for one ECS component.
///
/// Node IDs map directly to a page and slot. Pages never move after publication,
/// so existing lock-free `registry.component[node.index()]` access remains valid
/// while small sessions pay only for pages they actually touch.
pub struct PagedArray<T> {
	pages: Box<[OnceLock<Box<[T]>>]>,
	capacity: usize,
	init: fn() -> T,
}

impl<T> PagedArray<T> {
	pub const PAGE_SIZE: usize = 16 * 1024;

	pub fn new(capacity: usize, init: fn() -> T) -> Self {
		let page_count = capacity.div_ceil(Self::PAGE_SIZE);
		Self {
			pages: (0..page_count)
				.map(|_| OnceLock::new())
				.collect::<Vec<_>>()
				.into_boxed_slice(),
			capacity,
			init,
		}
	}

	pub fn allocated_pages(&self) -> usize {
		self.pages
			.iter()
			.filter(|page| page.get().is_some())
			.count()
	}

	fn page(&self, page_index: usize) -> &[T] {
		self.pages[page_index].get_or_init(|| {
			(0..Self::PAGE_SIZE)
				.map(|_| (self.init)())
				.collect::<Vec<_>>()
				.into_boxed_slice()
		})
	}
}

impl<T> Index<usize> for PagedArray<T> {
	type Output = T;

	fn index(&self, index: usize) -> &Self::Output {
		assert!(index < self.capacity, "paged component index out of bounds");
		let page_index = index / Self::PAGE_SIZE;
		let slot = index % Self::PAGE_SIZE;
		&self.page(page_index)[slot]
	}
}

#[cfg(test)]
mod tests {
	use super::PagedArray;
	use std::sync::Arc;
	use std::sync::atomic::{AtomicU64, Ordering};

	#[test]
	fn pages_allocate_on_demand_and_keep_direct_indexing() {
		let values = PagedArray::new(PagedArray::<u64>::PAGE_SIZE * 3, || 7u64);
		assert_eq!(values.allocated_pages(), 0);
		assert_eq!(values[0], 7);
		assert_eq!(values.allocated_pages(), 1);
		assert_eq!(values[PagedArray::<u64>::PAGE_SIZE * 2], 7);
		assert_eq!(values.allocated_pages(), 2);
	}

	#[test]
	fn concurrent_access_is_stable_across_page_boundaries() {
		let values = Arc::new(PagedArray::new(
			PagedArray::<AtomicU64>::PAGE_SIZE * 2,
			|| AtomicU64::new(0),
		));
		let left = PagedArray::<AtomicU64>::PAGE_SIZE - 1;
		let right = PagedArray::<AtomicU64>::PAGE_SIZE;
		let writer_values = values.clone();
		let writer = std::thread::spawn(move || {
			writer_values[left].store(11, Ordering::Release);
			writer_values[right].store(22, Ordering::Release);
		});
		writer.join().unwrap();
		assert_eq!(values[left].load(Ordering::Acquire), 11);
		assert_eq!(values[right].load(Ordering::Acquire), 22);
	}
}
