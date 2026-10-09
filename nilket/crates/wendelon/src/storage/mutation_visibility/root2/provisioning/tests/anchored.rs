use super::super::anchored::INJECTED_OPEN_ERRNO;
use super::super::anchored::*;
use std::ffi::CString;
use std::fs;
use std::io::Write;
use std::os::unix::fs::{PermissionsExt, symlink};
use std::path::{Path, PathBuf};
use std::process::Stdio;
use std::sync::atomic::{AtomicUsize, Ordering};

static NEXT: AtomicUsize = AtomicUsize::new(0);

// A fresh directory unique to this process and call; removed on drop.
struct Scratch(PathBuf);
impl Scratch {
	fn new() -> Self {
		let n = NEXT.fetch_add(1, Ordering::SeqCst);
		let p = std::env::temp_dir().join(format!("anchored-{}-{n}", std::process::id()));
		let _ = fs::remove_dir_all(&p);
		fs::create_dir_all(&p).unwrap();
		Self(p)
	}
	fn file(&self, rel: &str, bytes: &[u8]) -> PathBuf {
		let p = self.0.join(rel);
		if let Some(d) = p.parent() {
			fs::create_dir_all(d).unwrap();
		}
		fs::write(&p, bytes).unwrap();
		p
	}
	fn anchor(&self) -> AnchorDir2 {
		AnchorDir2::open(&self.0).unwrap()
	}
}
impl Drop for Scratch {
	fn drop(&mut self) {
		let _ = fs::set_permissions(&self.0, fs::Permissions::from_mode(0o700));
		let _ = fs::remove_dir_all(&self.0);
	}
}
fn c(s: &str) -> CString {
	CString::new(s).unwrap()
}
// True when any descriptor of this process still resolves to `path`.
fn open_to(path: &Path) -> bool {
	let target = fs::canonicalize(path).unwrap_or_else(|_| path.to_path_buf());
	fs::read_dir("/proc/self/fd")
		.unwrap()
		.filter_map(|e| fs::read_link(e.ok()?.path()).ok())
		.any(|l| l == target || l == path)
}
fn read(s: &Scratch, rel: &str, max: usize) -> Result<Vec<u8>, AnchoredError2> {
	AnchoredFile2::open(&s.anchor(), &c(rel))?.read_bounded(max)
}

#[test]
fn bounded_reads_exact_limits_and_zero() {
	let s = Scratch::new();
	s.file("a", b"hello");
	s.file("empty", b"");
	assert_eq!(read(&s, "a", 5), Ok(b"hello".to_vec()));
	assert_eq!(read(&s, "a", 4), Err(AnchoredError2::TooLarge));
	assert_eq!(read(&s, "a", 64), Ok(b"hello".to_vec()));
	assert_eq!(read(&s, "empty", 0), Ok(Vec::new()));
	assert_eq!(read(&s, "a", 0), Err(AnchoredError2::TooLarge));
	// Limits that cannot be represented are refused before any allocation or read.
	assert_eq!(read(&s, "a", usize::MAX), Err(AnchoredError2::InvalidLimit));
	assert_eq!(
		read(&s, "a", isize::MAX as usize),
		Err(AnchoredError2::InvalidLimit)
	);
}
#[test]
fn growth_after_open_is_still_bounded_by_the_limit() {
	let s = Scratch::new();
	let p = s.file("grow", b"");
	let f = AnchoredFile2::open(&s.anchor(), &c("grow")).unwrap();
	fs::OpenOptions::new()
		.append(true)
		.open(&p)
		.unwrap()
		.write_all(b"0123456789")
		.unwrap();
	assert_eq!(f.read_bounded(10), Ok(b"0123456789".to_vec()));
	let f = AnchoredFile2::open(&s.anchor(), &c("grow")).unwrap();
	fs::OpenOptions::new()
		.append(true)
		.open(&p)
		.unwrap()
		.write_all(b"X")
		.unwrap();
	assert_eq!(f.read_bounded(10), Err(AnchoredError2::TooLarge));
}
#[test]
fn symlinks_escapes_and_absolute_paths_are_refused_at_open() {
	let s = Scratch::new();
	s.file("real/file", b"x");
	symlink(s.0.join("real/file"), s.0.join("leaf")).unwrap();
	symlink(s.0.join("real"), s.0.join("mid")).unwrap();
	let a = s.anchor();
	assert_eq!(
		AnchoredFile2::open(&a, &c("leaf")).err(),
		Some(AnchoredError2::Symlink)
	);
	assert_eq!(
		AnchoredFile2::open(&a, &c("mid/file")).err(),
		Some(AnchoredError2::Symlink)
	);
	assert_eq!(
		AnchoredFile2::open(&a, &c("../x")).err(),
		Some(AnchoredError2::Escape)
	);
	let abs = s.0.join("real/file");
	assert_eq!(
		AnchoredFile2::open(&a, &c(abs.to_str().unwrap())).err(),
		Some(AnchoredError2::Escape)
	);
	// A directory swapped for a symlink after the anchor was opened.
	fs::rename(s.0.join("real"), s.0.join("real2")).unwrap();
	symlink(s.0.join("real2"), s.0.join("real")).unwrap();
	assert_eq!(
		AnchoredFile2::open(&a, &c("real/file")).err(),
		Some(AnchoredError2::Symlink)
	);
	assert_eq!(
		AnchoredFile2::open(&a, &c("missing")).err(),
		Some(AnchoredError2::NotFound)
	);
}
#[test]
fn non_regular_targets_are_refused_without_blocking() {
	let s = Scratch::new();
	fs::create_dir(s.0.join("dir")).unwrap();
	let fifo = s.0.join("fifo");
	assert!(
		std::process::Command::new("mkfifo")
			.arg(&fifo)
			.stdout(Stdio::null())
			.stderr(Stdio::null())
			.status()
			.unwrap()
			.success()
	);
	let a = s.anchor();
	assert_eq!(
		AnchoredFile2::open(&a, &c("dir")).err(),
		Some(AnchoredError2::NotRegular)
	);
	// No writer: a blocking open would hang; the watchdog bounds the test.
	let (tx, rx) = std::sync::mpsc::channel();
	let dir = s.0.clone();
	std::thread::spawn(move || {
		let a = AnchorDir2::open(&dir).unwrap();
		tx.send(AnchoredFile2::open(&a, &c("fifo")).err()).unwrap();
	});
	let got = rx
		.recv_timeout(std::time::Duration::from_secs(10))
		.expect("FIFO open blocked");
	assert_eq!(got, Some(AnchoredError2::NotRegular));
	assert!(!open_to(&fifo), "refused FIFO descriptor released");
	let dev = s.0.join("dev");
	let made = std::process::Command::new("mknod")
		.arg(&dev)
		.arg("c")
		.arg("1")
		.arg("3")
		.stdout(Stdio::null())
		.stderr(Stdio::null())
		.status();
	if made.map(|st| st.success()).unwrap_or(false) {
		assert_eq!(
			AnchoredFile2::open(&a, &c("dev")).err(),
			Some(AnchoredError2::NotRegular)
		);
	} else {
		println!(
			"ANCHORED_SKIP device: mknod of a character device is not permitted for this user"
		);
	}
}
#[test]
fn permission_and_forced_errors_map_to_their_variants() {
	let s = Scratch::new();
	let p = s.file("secret", b"x");
	fs::set_permissions(&p, fs::Permissions::from_mode(0o000)).unwrap();
	if is_root() {
		println!("ANCHORED_SKIP permission: running as root");
	} else {
		assert_eq!(read(&s, "secret", 8), Err(AnchoredError2::Permission));
	}
	let a = s.anchor();
	for (errno, want) in [
		(libc::ENOSYS, AnchoredError2::Unsupported),
		(libc::EINVAL, AnchoredError2::Unsupported),
		(libc::E2BIG, AnchoredError2::Unsupported),
		(libc::EAGAIN, AnchoredError2::Io(libc::EAGAIN)),
	] {
		INJECTED_OPEN_ERRNO.with(|f| f.set(Some(errno)));
		assert_eq!(AnchoredFile2::open(&a, &c("secret")).err(), Some(want));
	}
}
fn is_root() -> bool {
	std::env::var("USER").map(|u| u == "root").unwrap_or(false)
		|| fs::metadata("/proc/self")
			.map(|m| std::os::unix::fs::MetadataExt::uid(&m) == 0)
			.unwrap_or(false)
}
#[test]
fn descriptors_are_released_and_anchor_drop_does_not_break_an_open_file() {
	let s = Scratch::new();
	let p = s.file("keep", b"abc");
	let a = s.anchor();
	let f = AnchoredFile2::open(&a, &c("keep")).unwrap();
	drop(a);
	assert!(open_to(&p));
	assert_eq!(f.read_bounded(3), Ok(b"abc".to_vec()));
	assert!(!open_to(&p), "descriptor released after a successful read");
	let f = AnchoredFile2::open(&s.anchor(), &c("keep")).unwrap();
	assert_eq!(f.read_bounded(1), Err(AnchoredError2::TooLarge));
	assert!(!open_to(&p), "descriptor released after TooLarge");
	let f = AnchoredFile2::open(&s.anchor(), &c("keep")).unwrap();
	drop(f);
	assert!(!open_to(&p), "descriptor released on drop");
}
