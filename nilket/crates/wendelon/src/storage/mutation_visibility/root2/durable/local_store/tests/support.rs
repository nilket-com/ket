use super::*;
pub(super) struct Temp {
	pub(super) parent: PathBuf,
	pub(super) root: PathBuf,
}
impl Temp {
	pub(super) fn new() -> Self {
		let parent = std::env::temp_dir().join(format!("r2-local-{}", uuid::Uuid::new_v4()));
		std::fs::create_dir(&parent).unwrap();
		let root = parent.join("store");
		Self { parent, root }
	}
}
impl Drop for Temp {
	fn drop(&mut self) {
		let _ = std::fs::remove_dir_all(&self.parent);
	}
}
pub(super) fn fixture(name: &str) -> Vec<u8> {
	crate::storage::mutation_visibility::root2::durable::tests::fixture(name)
}
pub(super) fn inputs() -> (
	ContainerId,
	UnverifiedData2,
	LoaderCapability2,
	Arc<crate::storage::mutation_visibility::commitment::TestProofKey>,
	ParentEntry,
) {
	let (data, cap, key) =
		crate::storage::mutation_visibility::root2::durable::loader::tests::raw(false);
	let (claim, reservation) = &data.bundle.claims[0];
	let id = ContainerId {
		generation: claim.manifest.identity.generation,
		cluster: [0x12; 16],
		store: [0x31; 16],
	};
	let local = NodeRef2 {
		node: claim.original.lease.holder,
		incarnation: claim.original.incarnation,
	};
	let entry = ParentEntry {
		observations: 0,
		owner: JournalIdentity2 {
			generation: id.generation,
			cluster: id.cluster,
			identity: claim.manifest.identity,
			claim: claim.digest().unwrap(),
			reservation: *reservation,
		},
		data: [0x41; 16],
		index: [0x42; 16],
		credit: Credit::freeze(&data, 128, 1, local).unwrap(),
	};
	(id, data, cap, Arc::new(key), entry)
}
pub(super) async fn initialized(temp: &Temp) -> (Arc<Model>, Arc<LocalStore>, ParentEntry) {
	let (id, _, _, _, entry) = inputs();
	let model = Model::new(temp.root.clone());
	let store = LocalStore::create(temp.root.clone(), id, model.clone(), model.clone())
		.await
		.unwrap();
	(model, store, entry)
}
pub(super) async fn admitted(temp: &Temp) -> (Arc<Model>, Arc<LocalStore>, ParentEntry) {
	let (model, store, entry) = initialized(temp).await;
	store.allocate(entry.clone()).await.unwrap();
	model.observed("allocated");
	(model, store, entry)
}
pub(super) fn writes(trace: &[Event]) -> Vec<(u64, usize)> {
	trace
		.iter()
		.filter_map(|e| match e {
			Event::Submitted(Op::Write, label) => {
				let fields = label.split(':').collect::<Vec<_>>();
				Some((fields[1].parse().unwrap(), fields[2].parse().unwrap()))
			}
			_ => None,
		})
		.collect()
}
