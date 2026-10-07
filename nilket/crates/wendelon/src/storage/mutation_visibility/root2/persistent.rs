//! Private immutable ordered AVL. Every update copies a bounded search path.
use std::{cmp::Ordering, sync::Arc};
#[derive(Clone)]
struct Node<K, V> {
	key: K,
	value: Arc<V>,
	left: Option<Arc<Self>>,
	right: Option<Arc<Self>>,
	height: u16,
	count: usize,
}
pub(super) struct Map<K, V>(Option<Arc<Node<K, V>>>);
impl<K, V> Clone for Map<K, V> {
	fn clone(&self) -> Self {
		Self(self.0.clone())
	}
}
impl<K, V> Default for Map<K, V> {
	fn default() -> Self {
		Self(None)
	}
}
impl<K: Ord + Clone, V: Clone> Map<K, V> {
	pub fn new() -> Self {
		Self::default()
	}
	pub fn len(&self) -> usize {
		count(&self.0)
	}
	pub fn get(&self, key: &K) -> Option<&V> {
		let mut at = self.0.as_deref();
		while let Some(n) = at {
			match key.cmp(&n.key) {
				Ordering::Less => at = n.left.as_deref(),
				Ordering::Greater => at = n.right.as_deref(),
				Ordering::Equal => return Some(&n.value),
			}
		}
		None
	}
	pub fn contains_key(&self, key: &K) -> bool {
		self.get(key).is_some()
	}
	pub fn insert(&mut self, key: K, value: impl Into<V>) {
		self.0 = Some(insert(&self.0, key, Arc::new(value.into())));
	}
	pub fn get_mut(&mut self, key: &K) -> Option<&mut V> {
		// Used only in the private candidate. No old node or record is ever mutated.
		if !self.contains_key(key) {
			return None;
		}
		fn descend<'a, K: Ord + Clone, V: Clone>(
			at: &'a mut Option<Arc<Node<K, V>>>,
			key: &K,
		) -> &'a mut V {
			let n = Arc::make_mut(at.as_mut().expect("located key"));
			match key.cmp(&n.key) {
				Ordering::Less => descend(&mut n.left, key),
				Ordering::Greater => descend(&mut n.right, key),
				Ordering::Equal => Arc::make_mut(&mut n.value),
			}
		}
		Some(descend(&mut self.0, key))
	}
	pub fn iter(&self) -> Iter<'_, K, V> {
		let mut i = Iter { stack: Vec::new() };
		i.left(self.0.as_deref());
		i
	}
	pub fn values(&self) -> impl Iterator<Item = &V> + '_ {
		self.iter().map(|(_, v)| v)
	}
	pub fn last_key_value(&self) -> Option<(&K, &V)> {
		let mut at = self.0.as_deref()?;
		while let Some(r) = at.right.as_deref() {
			at = r;
		}
		Some((&at.key, &at.value))
	}
	pub fn upper_bound(&self, key: &K) -> Option<(&K, &V)> {
		let mut at = self.0.as_deref();
		let mut found = None;
		while let Some(n) = at {
			if &n.key <= key {
				found = Some((&n.key, n.value.as_ref()));
				at = n.right.as_deref();
			} else {
				at = n.left.as_deref();
			}
		}
		found
	}
	#[cfg(test)]
	pub fn shares_root(&self, other: &Self) -> bool {
		match (&self.0, &other.0) {
			(Some(a), Some(b)) => Arc::ptr_eq(a, b),
			(None, None) => true,
			_ => false,
		}
	}
	#[cfg(test)]
	pub fn shares_value(&self, other: &Self, key: &K) -> bool {
		fn find<'a, K: Ord, V>(map: &'a Map<K, V>, key: &K) -> Option<&'a Arc<V>> {
			let mut at = map.0.as_deref();
			while let Some(n) = at {
				match key.cmp(&n.key) {
					Ordering::Less => at = n.left.as_deref(),
					Ordering::Greater => at = n.right.as_deref(),
					Ordering::Equal => return Some(&n.value),
				}
			}
			None
		}
		match (find(self, key), find(other, key)) {
			(Some(a), Some(b)) => Arc::ptr_eq(a, b),
			_ => false,
		}
	}
	#[cfg(test)]
	pub fn clear(&mut self) {
		self.0 = None;
	}
	#[cfg(test)]
	pub fn remove(&mut self, key: &K) -> Option<V> {
		let value = self.get(key)?.clone();
		*self = self
			.iter()
			.filter(|(k, _)| *k != key)
			.map(|(k, v)| (k.clone(), v.clone()))
			.collect();
		Some(value)
	}
	#[cfg(test)]
	pub fn audit(&self) {
		fn check<K: Ord + Clone, V>(
			at: &Option<Arc<Node<K, V>>>,
			lo: Option<&K>,
			hi: Option<&K>,
		) -> (u16, usize) {
			let Some(n) = at else { return (0, 0) };
			assert!(lo.is_none_or(|k| k < &n.key) && hi.is_none_or(|k| &n.key < k));
			let (lh, lc) = check(&n.left, lo, Some(&n.key));
			let (rh, rc) = check(&n.right, Some(&n.key), hi);
			assert!(lh.abs_diff(rh) <= 1);
			assert_eq!(n.height, 1 + lh.max(rh));
			assert_eq!(n.count, 1 + lc + rc);
			(n.height, n.count)
		}
		check(&self.0, None, None);
	}
}
fn height<K, V>(n: &Option<Arc<Node<K, V>>>) -> u16 {
	n.as_ref().map_or(0, |n| n.height)
}
fn count<K, V>(n: &Option<Arc<Node<K, V>>>) -> usize {
	n.as_ref().map_or(0, |n| n.count)
}
fn node<K, V>(
	key: K,
	value: Arc<V>,
	left: Option<Arc<Node<K, V>>>,
	right: Option<Arc<Node<K, V>>>,
) -> Arc<Node<K, V>> {
	let height = height(&left)
		.max(height(&right))
		.checked_add(1)
		.expect("bounded tree height");
	let count = count(&left)
		.checked_add(count(&right))
		.and_then(|n| n.checked_add(1))
		.expect("bounded tree count");
	Arc::new(Node {
		key,
		value,
		left,
		right,
		height,
		count,
	})
}
fn balanced<K: Clone, V>(
	key: K,
	value: Arc<V>,
	left: Option<Arc<Node<K, V>>>,
	right: Option<Arc<Node<K, V>>>,
) -> Arc<Node<K, V>> {
	if height(&left) > height(&right) + 1 {
		let l = left.as_ref().unwrap();
		if height(&l.left) >= height(&l.right) {
			return node(
				l.key.clone(),
				l.value.clone(),
				l.left.clone(),
				Some(node(key, value, l.right.clone(), right)),
			);
		}
		let m = l.right.as_ref().unwrap();
		return node(
			m.key.clone(),
			m.value.clone(),
			Some(node(
				l.key.clone(),
				l.value.clone(),
				l.left.clone(),
				m.left.clone(),
			)),
			Some(node(key, value, m.right.clone(), right)),
		);
	}
	if height(&right) > height(&left) + 1 {
		let r = right.as_ref().unwrap();
		if height(&r.right) >= height(&r.left) {
			return node(
				r.key.clone(),
				r.value.clone(),
				Some(node(key, value, left, r.left.clone())),
				r.right.clone(),
			);
		}
		let m = r.left.as_ref().unwrap();
		return node(
			m.key.clone(),
			m.value.clone(),
			Some(node(key, value, left, m.left.clone())),
			Some(node(
				r.key.clone(),
				r.value.clone(),
				m.right.clone(),
				r.right.clone(),
			)),
		);
	}
	node(key, value, left, right)
}
fn insert<K: Ord + Clone, V>(
	at: &Option<Arc<Node<K, V>>>,
	key: K,
	value: Arc<V>,
) -> Arc<Node<K, V>> {
	let Some(n) = at else {
		return node(key, value, None, None);
	};
	match key.cmp(&n.key) {
		Ordering::Less => balanced(
			n.key.clone(),
			n.value.clone(),
			Some(insert(&n.left, key, value)),
			n.right.clone(),
		),
		Ordering::Greater => balanced(
			n.key.clone(),
			n.value.clone(),
			n.left.clone(),
			Some(insert(&n.right, key, value)),
		),
		Ordering::Equal => node(key, value, n.left.clone(), n.right.clone()),
	}
}
pub(super) struct Iter<'a, K, V> {
	stack: Vec<&'a Node<K, V>>,
}
impl<'a, K, V> Iter<'a, K, V> {
	fn left(&mut self, mut at: Option<&'a Node<K, V>>) {
		while let Some(n) = at {
			self.stack.push(n);
			at = n.left.as_deref();
		}
	}
}
impl<'a, K, V> Iterator for Iter<'a, K, V> {
	type Item = (&'a K, &'a V);
	fn next(&mut self) -> Option<Self::Item> {
		let n = self.stack.pop()?;
		self.left(n.right.as_deref());
		Some((&n.key, &n.value))
	}
}
impl<'a, K: Ord + Clone, V: Clone> IntoIterator for &'a Map<K, V> {
	type Item = (&'a K, &'a V);
	type IntoIter = Iter<'a, K, V>;
	fn into_iter(self) -> Self::IntoIter {
		self.iter()
	}
}
impl<K: Ord + Clone, V: Clone> FromIterator<(K, V)> for Map<K, V> {
	fn from_iter<T: IntoIterator<Item = (K, V)>>(xs: T) -> Self {
		let mut m = Self::new();
		for (k, v) in xs {
			m.insert(k, v);
		}
		m
	}
}
impl<K: Ord + Clone, V: Clone + PartialEq> PartialEq for Map<K, V> {
	fn eq(&self, o: &Self) -> bool {
		self.len() == o.len() && self.iter().eq(o.iter())
	}
}
impl<K: Ord + Clone, V: Clone + Eq> Eq for Map<K, V> {}
impl<K, V> std::fmt::Debug for Map<K, V> {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		f.write_str("Map { redacted }")
	}
}
