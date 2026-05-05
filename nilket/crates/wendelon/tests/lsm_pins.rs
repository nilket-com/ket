use wendelon::schema::SlabId;
use wendelon::storage::{AcquireError, SlabPinTable};

#[test]
fn pin_count_increments_and_decrements_with_guard_lifetime() {
	let pins = SlabPinTable::default();
	let slab = SlabId::new(7);

	{
		let guard = pins.acquire(&[slab]).unwrap();
		assert_eq!(guard.slabs(), &[slab]);
		assert_eq!(pins.pin_count(slab), 1);
		{
			let _second = pins.acquire(&[slab]).unwrap();
			assert_eq!(pins.pin_count(slab), 2);
		}
		assert_eq!(pins.pin_count(slab), 1);
	}

	assert_eq!(pins.pin_count(slab), 0);
}

#[test]
fn bulk_acquire_is_unique_and_atomic() {
	let pins = SlabPinTable::default();
	let a = SlabId::new(1);
	let b = SlabId::new(2);
	assert!(pins.mark_delete_pending(b));

	let err = pins.acquire(&[a, a, b]).unwrap_err();

	assert_eq!(err, AcquireError::SlabAlreadyDeletePending { slab_id: b });
	assert_eq!(pins.pin_count(a), 0);
	assert_eq!(pins.pin_count(b), 0);
}

#[test]
fn delete_pending_acquire_fails_until_cleared() {
	let pins = SlabPinTable::default();
	let slab = SlabId::new(10);

	assert!(pins.mark_delete_pending(slab));
	assert!(matches!(
		pins.acquire(&[slab]),
		Err(AcquireError::SlabAlreadyDeletePending { slab_id }) if slab_id == slab
	));

	pins.clear_delete_pending(slab);
	let _guard = pins.acquire(&[slab]).unwrap();
	assert_eq!(pins.pin_count(slab), 1);
}

#[test]
fn delete_pending_waits_for_last_pin() {
	let pins = SlabPinTable::default();
	let slab = SlabId::new(11);

	let guard = pins.acquire(&[slab]).unwrap();
	assert!(!pins.mark_delete_pending(slab));
	assert!(!pins.is_delete_pending(slab));

	drop(guard);
	assert!(pins.mark_delete_pending(slab));
	assert!(pins.is_delete_pending(slab));
}
