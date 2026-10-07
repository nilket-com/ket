//! Logical persistence model, not a power-loss experiment. Namespace sync never syncs data.
use super::*;
use crate::storage::{
	file_ops::{BlockingFileOps, DirectoryEntry, OpenFlags},
	io_uring::AlignedBuf,
};
use async_trait::async_trait;
use std::{
	fs, io,
	os::unix::fs::{FileExt, MetadataExt},
	sync::Mutex as StdMutex,
};
#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum Op {
	Open,
	Read,
	Write,
	FileSync,
	DirSync,
	List,
	Length,
	Lock,
	Space,
	Mkdir,
	Rename,
}
#[derive(Clone)]
pub(super) enum Fault {
	Before,
	After,
	Short(usize),
	Interrupted,
	Zero,
	ListEntry(usize),
	Gate(Arc<Gate>),
	GateAfter(Arc<Gate>),
	GateError(Arc<Gate>, bool),
	Code(i32),
	CodeAfter(i32),
}
pub(super) struct Gate {
	pub entered: tokio::sync::Notify,
	pub release: tokio::sync::Notify,
}
#[derive(Clone, Debug)]
pub(super) enum Event {
	Submitted(Op, String),
	Completed(Op, String),
	FileDurable(u64),
	NameDurable(String),
	Observed(String),
}
#[derive(Clone)]
struct Name {
	directory: bool,
	inode: u64,
}
#[derive(Default)]
struct State {
	trace: Vec<Event>,
	calls: usize,
	fault: Option<(usize, Fault)>,
	bytes: BTreeMap<u64, Vec<u8>>,
	directories: BTreeMap<PathBuf, BTreeMap<String, Name>>,
}
pub(super) struct Model {
	pub root: PathBuf,
	space_bytes: StdMutex<Option<u64>>,
	state: StdMutex<State>,
	base: BlockingFileOps,
}
impl Model {
	pub fn new(root: PathBuf) -> Arc<Self> {
		Arc::new(Self {
			root,
			space_bytes: StdMutex::new(None),
			state: StdMutex::new(State::default()),
			base: BlockingFileOps,
		})
	}
	pub fn space_bytes(&self, n: u64) {
		*self.space_bytes.lock().unwrap() = Some(n);
	}
	pub fn trace(&self) -> Vec<Event> {
		self.state.lock().unwrap().trace.clone()
	}
	pub fn count(&self) -> usize {
		self.state.lock().unwrap().calls
	}
	pub fn fault(&self, at: usize, f: Fault) {
		self.state.lock().unwrap().fault = Some((at, f))
	}
	pub fn observed(&self, label: &str) {
		self.state
			.lock()
			.unwrap()
			.trace
			.push(Event::Observed(label.into()))
	}
	async fn begin(&self, op: Op, label: String) -> io::Result<Option<Fault>> {
		let fault = {
			let mut s = self.state.lock().unwrap();
			s.calls += 1;
			s.trace.push(Event::Submitted(op, label));
			if s.fault.as_ref().is_some_and(|f| f.0 == s.calls) {
				s.fault.take().map(|(_, f)| f)
			} else {
				None
			}
		};
		match &fault {
			Some(Fault::Before) => return Err(injected()),
			Some(Fault::Code(c)) => return Err(io::Error::from_raw_os_error(*c)),
			Some(Fault::Interrupted) => return Err(io::Error::from(io::ErrorKind::Interrupted)),
			Some(Fault::GateError(g, false)) => {
				g.entered.notify_one();
				g.release.notified().await;
				return Err(injected());
			}
			Some(Fault::Gate(g)) => {
				g.entered.notify_one();
				g.release.notified().await;
			}
			_ => {}
		}
		Ok(fault)
	}
	async fn end(&self, op: Op, label: String, fault: Option<Fault>) -> io::Result<()> {
		self.state
			.lock()
			.unwrap()
			.trace
			.push(Event::Completed(op, label));
		match fault {
			Some(Fault::After) => Err(injected()),
			Some(Fault::CodeAfter(c)) => Err(io::Error::from_raw_os_error(c)),
			Some(Fault::GateError(g, true)) => {
				g.entered.notify_one();
				g.release.notified().await;
				Err(injected())
			}
			Some(Fault::GateAfter(g)) => {
				g.entered.notify_one();
				g.release.notified().await;
				Ok(())
			}
			_ => Ok(()),
		}
	}
	fn identity(handle: &FileHandle) -> io::Result<u64> {
		// Test bookkeeping may clone; production adapter counts every descriptor clone.
		Ok(handle.try_clone()?.metadata()?.ino())
	}
	fn snapshot_file(&self, h: &FileHandle) -> io::Result<()> {
		let f = h.try_clone()?;
		let m = f.metadata()?;
		let mut bytes = vec![0; m.len() as usize];
		f.read_exact_at(&mut bytes, 0)?;
		let mut s = self.state.lock().unwrap();
		s.bytes.insert(m.ino(), bytes);
		s.trace.push(Event::FileDurable(m.ino()));
		Ok(())
	}
	fn snapshot_names(&self, p: &Path) -> io::Result<()> {
		let mut names = BTreeMap::new();
		for e in fs::read_dir(p)? {
			let e = e?;
			let m = e.metadata()?;
			names.insert(
				e.file_name().into_string().map_err(|_| injected())?,
				Name {
					directory: m.is_dir(),
					inode: m.ino(),
				},
			);
		}
		let mut s = self.state.lock().unwrap();
		s.directories.insert(p.to_path_buf(), names);
		s.trace.push(Event::NameDurable(p.display().to_string()));
		Ok(())
	}
	pub fn allow_early_names(&self) -> io::Result<()> {
		fn visit(m: &Model, p: &Path) -> io::Result<()> {
			if !p.exists() {
				return Ok(());
			}
			m.snapshot_names(p)?;
			for e in fs::read_dir(p)? {
				let e = e?;
				if e.file_type()?.is_dir() {
					visit(m, &e.path())?;
				}
			}
			Ok(())
		}
		self.snapshot_names(self.root.parent().unwrap())?;
		visit(self, &self.root)
	}
	pub fn crash(&self, allow_unsynced: bool, partial: Option<usize>) -> io::Result<()> {
		let mut state = self.state.lock().unwrap();
		if allow_unsynced {
			fn collect(p: &Path, s: &mut State, partial: Option<usize>) -> io::Result<()> {
				if !p.exists() {
					return Ok(());
				}
				for e in fs::read_dir(p)? {
					let e = e?;
					let m = e.metadata()?;
					if m.is_dir() {
						collect(&e.path(), s, partial)?
					} else {
						let mut b = fs::read(e.path())?;
						if let Some(n) = partial {
							b.truncate(
								n.max(s.bytes.get(&m.ino()).map_or(0, Vec::len))
									.min(b.len()),
							)
						}
						s.bytes.insert(m.ino(), b);
					}
				}
				Ok(())
			}
			collect(&self.root, &mut state, partial)?;
		}
		if self.root.exists() {
			fs::remove_dir_all(&self.root)?
		}
		let parent = self.root.parent().unwrap();
		let basename = self.root.file_name().unwrap().to_str().unwrap();
		let root_name = state
			.directories
			.get(parent)
			.and_then(|d| d.get(basename))
			.cloned();
		if root_name.is_none() {
			state.fault = None;
			return Ok(());
		}
		fs::create_dir(&self.root)?;
		fn restore(p: &Path, s: &State) -> io::Result<()> {
			if let Some(names) = s.directories.get(p) {
				for (name, obj) in names {
					let to = p.join(name);
					if obj.directory {
						fs::create_dir(&to)?;
						restore(&to, s)?
					} else {
						fs::write(
							to,
							s.bytes.get(&obj.inode).map(Vec::as_slice).unwrap_or(&[]),
						)?
					}
				}
			}
			Ok(())
		}
		restore(&self.root, &state)?;
		state.fault = None;
		// Rebound handles have fresh OS inode identities. Rebaseline durability on their new ids.
		state.bytes.clear();
		state.directories.clear();
		fn baseline(p: &Path, s: &mut State) -> io::Result<()> {
			let mut names = BTreeMap::new();
			for e in fs::read_dir(p)? {
				let e = e?;
				let m = e.metadata()?;
				let name = e.file_name().into_string().map_err(|_| injected())?;
				names.insert(
					name,
					Name {
						directory: m.is_dir(),
						inode: m.ino(),
					},
				);
				if m.is_dir() {
					baseline(&e.path(), s)?
				} else {
					s.bytes.insert(m.ino(), fs::read(e.path())?);
				}
			}
			s.directories.insert(p.to_path_buf(), names);
			Ok(())
		}
		baseline(&self.root, &mut state)?;
		let root_inode = fs::metadata(&self.root)?.ino();
		state
			.directories
			.entry(parent.to_path_buf())
			.or_default()
			.insert(
				basename.into(),
				Name {
					directory: true,
					inode: root_inode,
				},
			);
		Ok(())
	}
}
fn injected() -> io::Error {
	io::Error::other("R2 test injected completion/namespace failure")
}
#[async_trait]
impl LocalAdapter for Model {
	async fn list(&self, p: &Path, limit: usize) -> io::Result<Vec<Entry>> {
		let label = p.display().to_string();
		let f = self.begin(Op::List, label.clone()).await?;
		let out = LinuxAdapter.list(p, limit).await?;
		if let Some(Fault::ListEntry(n)) = &f {
			if *n <= out.len() {
				return Err(injected());
			}
		}
		self.end(Op::List, label, f).await?;
		Ok(out)
	}
	async fn mkdir(&self, p: &Path) -> io::Result<()> {
		let label = p.display().to_string();
		let f = self.begin(Op::Mkdir, label.clone()).await?;
		LinuxAdapter.mkdir(p).await?;
		self.end(Op::Mkdir, label, f).await
	}
	async fn open_regular(&self, p: &Path, c: bool, w: bool) -> io::Result<FileHandle> {
		let label = p.display().to_string();
		let f = self.begin(Op::Open, label.clone()).await?;
		let h = LinuxAdapter.open_regular(p, c, w).await?;
		self.end(Op::Open, label, f).await?;
		Ok(h)
	}
	async fn length(&self, h: &FileHandle) -> io::Result<u64> {
		let label = Self::identity(h)?.to_string();
		let f = self.begin(Op::Length, label.clone()).await?;
		let n = LinuxAdapter.length(h).await?;
		self.end(Op::Length, label, f).await?;
		Ok(n)
	}
	async fn lock(&self, h: &FileHandle) -> io::Result<()> {
		let label = Self::identity(h)?.to_string();
		let f = self.begin(Op::Lock, label.clone()).await?;
		LinuxAdapter.lock(h).await?;
		self.end(Op::Lock, label, f).await
	}
	async fn space(&self, _: &Path) -> io::Result<Space> {
		let f = self.begin(Op::Space, "space".into()).await?;
		self.end(Op::Space, "space".into(), f).await?;
		Ok(Space {
			bytes: self.space_bytes.lock().unwrap().unwrap_or(1 << 44),
			inodes: 1 << 30,
		})
	}
}
#[async_trait]
impl AsyncFileOps for Model {
	async fn open(&self, p: &Path, flags: OpenFlags) -> io::Result<FileHandle> {
		self.open_regular(p, flags.create_new, flags.write).await
	}
	async fn read_plain_at(&self, h: &FileHandle, o: u64, n: usize) -> io::Result<Vec<u8>> {
		let label = format!("{}:{o}:{n}", Self::identity(h)?);
		let f = self.begin(Op::Read, label.clone()).await?;
		let n = if let Some(Fault::Short(v)) = &f {
			n.min(*v)
		} else {
			n
		};
		let b = if matches!(f, Some(Fault::Zero)) {
			Vec::new()
		} else {
			self.base.read_plain_at(h, o, n).await?
		};
		self.end(Op::Read, label, f).await?;
		Ok(b)
	}
	async fn write_plain_at(&self, h: &FileHandle, o: u64, mut b: Vec<u8>) -> io::Result<usize> {
		let label = format!("{}:{o}:{}", Self::identity(h)?, b.len());
		let f = self.begin(Op::Write, label.clone()).await?;
		if let Some(Fault::Short(n)) = &f {
			b.truncate(*n)
		}
		let n = if matches!(f, Some(Fault::Zero)) {
			0
		} else {
			self.base.write_plain_at(h, o, b).await?
		};
		self.end(Op::Write, label, f).await?;
		Ok(n)
	}
	async fn fsync(&self, h: &FileHandle) -> io::Result<()> {
		let label = Self::identity(h)?.to_string();
		let f = self.begin(Op::FileSync, label.clone()).await?;
		self.base.fsync(h).await?;
		self.snapshot_file(h)?;
		self.end(Op::FileSync, label, f).await
	}
	async fn fsync_dir(&self, p: &Path) -> io::Result<()> {
		let label = p.display().to_string();
		let f = self.begin(Op::DirSync, label.clone()).await?;
		self.base.fsync_dir(p).await?;
		self.snapshot_names(p)?;
		self.end(Op::DirSync, label, f).await
	}
	async fn rename(&self, a: &Path, b: &Path) -> io::Result<()> {
		let label = format!("{} -> {}", a.display(), b.display());
		let f = self.begin(Op::Rename, label.clone()).await?;
		self.base.rename(a, b).await?;
		self.end(Op::Rename, label, f).await
	}
	async fn read_at(
		&self,
		_: &FileHandle,
		_: u64,
		_: AlignedBuf,
	) -> io::Result<(usize, AlignedBuf)> {
		Err(injected())
	}
	async fn write_at(
		&self,
		_: &FileHandle,
		_: u64,
		_: AlignedBuf,
	) -> io::Result<(usize, AlignedBuf)> {
		Err(injected())
	}
	async fn fdatasync(&self, _: &FileHandle) -> io::Result<()> {
		Err(injected())
	}
	async fn read_all(&self, _: &Path) -> io::Result<Vec<u8>> {
		Err(injected())
	}
	async fn write_all(&self, _: &Path, _: &[u8]) -> io::Result<()> {
		Err(injected())
	}
	async fn file_len(&self, _: &Path) -> io::Result<Option<u64>> {
		Err(injected())
	}
	async fn set_len(&self, _: &FileHandle, _: u64) -> io::Result<()> {
		Err(injected())
	}
	async fn create_dir_all(&self, _: &Path) -> io::Result<()> {
		Err(injected())
	}
	async fn read_dir(&self, _: &Path) -> io::Result<Vec<DirectoryEntry>> {
		Err(injected())
	}
	async fn unlink(&self, _: &Path) -> io::Result<()> {
		Err(injected())
	}
}
