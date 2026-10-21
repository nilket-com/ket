//! A safe custom receiver waker may panic; that cannot authorize freeing another request.
use super::*;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Wake, Waker};

struct PanicOnce(AtomicBool);
impl Wake for PanicOnce {
	fn wake(self: Arc<Self>) {
		if !self.0.swap(true, Ordering::SeqCst) {
			panic!("receiver waker panic during quarantine");
		}
	}
}

#[test]
fn a_panicking_receiver_cannot_free_other_kernel_buffers() {
	if !quarantine_child(
		"waker-panic",
		concat!(
			module_path!(),
			"::a_panicking_receiver_cannot_free_other_kernel_buffers"
		)
		.trim_start_matches("wendelon::"),
	) {
		return;
	}
	let dir = crate::test_support::TestDir::new("uring-waker-panic");
	let owner = DataDirectoryOwner::acquire_existing(&dir).unwrap();
	let registry = FinalityObservationRegistry::new();
	let PendingRead {
		mut ring,
		mut pending,
		mut reply,
		_writer,
		file,
		buffer_dropped,
	} = pending_read(&owner, &registry);
	let second_dropped = Arc::new(AtomicBool::new(false));
	let mut buf = AlignedBuf::new(DIRECT_IO_ALIGN);
	buf.drop_probe = Some(second_dropped.clone());
	let (second_reply, mut second_receiver) = oneshot::channel();
	let held = file.upgrade().unwrap();
	push_request(
		&mut ring,
		&mut 2,
		&mut pending,
		QueuedIoRequest {
			request: IoRequest {
				op: IoOp::Read,
				fd: held.as_raw_fd(),
				buf: Some(buf),
				offset: 0,
				reply: second_reply,
			},
			file: held,
			observation: Some(registry.begin(WorkScope::Node, WorkKind::StoragePublication)),
			owner: Some(owner.clone()),
			permit: Some(owner.write_permit().unwrap()),
		},
	);
	assert_eq!(ring.submit().unwrap(), 1);
	assert_eq!(ring.completion().len(), 0);
	drop(owner);
	let wake = Arc::new(PanicOnce(AtomicBool::new(false)));
	let waker = Waker::from(wake.clone());
	let mut context = Context::from_waker(&waker);
	assert!(Pin::new(&mut reply).poll(&mut context).is_pending());
	assert!(
		Pin::new(&mut second_receiver)
			.poll(&mut context)
			.is_pending()
	);
	let panic = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(pending)));
	assert!(panic.is_err());
	assert!(wake.0.load(Ordering::SeqCst));
	assert!(!buffer_dropped.load(Ordering::Acquire));
	assert!(!second_dropped.load(Ordering::Acquire));
	assert!(file.upgrade().is_some());
	assert!(DataDirectoryOwner::acquire_existing(&dir).is_err());
	println!("quarantine control passed");
}
