//! Test-only exact-entry fixtures. Composition scaffolding, not a production carrier.
use crate::storage::mutation_visibility::root2::command::Command2;
use crate::storage::mutation_visibility::root2::entry_identity::EntryPosition2;
use crate::storage::mutation_visibility::root2::origin::EntryClass2;
use crate::storage::mutation_visibility::root2::types::Digest2;
use std::{cell::RefCell, collections::BTreeMap, marker::PhantomData, rc::Rc};
type Key = (u64, u32, u64, Digest2);
type Fixtures = BTreeMap<Key, (EntryClass2, Command2)>;
thread_local! { static ROOT2_FIXTURE: RefCell<Option<Fixtures>> = const { RefCell::new(None) }; }
fn key(p: EntryPosition2, fp: Digest2) -> Key {
	(p.term, p.node, p.index, fp)
}
/// The scope itself must stay on its installing thread; callers use current-thread runtimes.
pub(in crate::storage::mutation_visibility::root2) struct Root2FixtureScope(PhantomData<Rc<()>>);
impl Root2FixtureScope {
	pub(in crate::storage::mutation_visibility::root2) fn install(
		entries: Vec<(EntryPosition2, Digest2, EntryClass2, Command2)>,
	) -> Self {
		let mut map = Fixtures::new();
		for (p, fp, class, command) in entries {
			assert!(
				map.insert(key(p, fp), (class, command)).is_none(),
				"duplicate fixture"
			);
		}
		ROOT2_FIXTURE.with(|v| {
			let mut v = v.borrow_mut();
			assert!(v.is_none(), "nested fixture scope");
			*v = Some(map);
		});
		Self(PhantomData)
	}
}
impl Drop for Root2FixtureScope {
	fn drop(&mut self) {
		ROOT2_FIXTURE.with(|v| *v.borrow_mut() = None);
	}
}
pub(super) fn class(p: EntryPosition2, fp: Digest2) -> Option<EntryClass2> {
	ROOT2_FIXTURE.with(|v| v.borrow().as_ref()?.get(&key(p, fp)).map(|(c, _)| *c))
}
pub(in crate::storage::mutation_visibility::root2) fn command(
	p: EntryPosition2,
	fp: Digest2,
) -> Option<Command2> {
	ROOT2_FIXTURE.with(|v| {
		v.borrow()
			.as_ref()?
			.get(&key(p, fp))
			.map(|(_, c)| c.clone())
	})
}
