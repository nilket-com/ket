//! Open a private key file only when its access control admits no one but
//! its owner, and hand back the very handle that was checked.
//!
//! ADR 0165 §3. The check runs on the opened handle, never on the path, so a
//! file swapped between check and read is never read. On Unix the file must
//! be owned by the running user with mode `0600`. On Windows the file must be
//! owned by the running user, `SYSTEM`, or `BUILTIN\Administrators` — an owner
//! can rewrite the DACL at will, so a foreign owner is refused whatever the
//! DACL says — the DACL must be present (a null DACL grants everyone), and
//! every access entry, inherited ones included, must be an allow entry for one
//! of those same three principals. This crate carries the unsafe FFI so
//! `tls-pki` can keep denying unsafe code.

#[cfg(target_os = "linux")]
pub mod custody;

use std::fs::File;
use std::path::{Path, PathBuf};

#[derive(Debug, thiserror::Error)]
pub enum KeyAccessError {
	#[error("private key {path} cannot be opened: {source}")]
	Open {
		path: PathBuf,
		#[source]
		source: std::io::Error,
	},
	#[error("private key {path} access control cannot be read: {reason}")]
	Inspect { path: PathBuf, reason: String },
	#[error("private key {path} is not private: {reason}")]
	NotPrivate { path: PathBuf, reason: String },
}

/// Open `path` read-only and return the handle if, and only if, the file's
/// access control admits nobody beyond its owner.
pub fn open_private_key_owner_only(path: &Path) -> Result<File, KeyAccessError> {
	let file = File::open(path).map_err(|source| KeyAccessError::Open {
		path: path.to_path_buf(),
		source,
	})?;
	platform::check_owner_only(&file, path)?;
	Ok(file)
}

#[cfg(unix)]
mod platform {
	use super::KeyAccessError;
	use std::fs::File;
	use std::os::unix::fs::MetadataExt;
	use std::path::Path;

	pub(super) fn check_owner_only(file: &File, path: &Path) -> Result<(), KeyAccessError> {
		let metadata = file.metadata().map_err(|error| KeyAccessError::Inspect {
			path: path.to_path_buf(),
			reason: error.to_string(),
		})?;
		if !metadata.is_file() {
			return Err(KeyAccessError::NotPrivate {
				path: path.to_path_buf(),
				reason: "not a regular file".to_owned(),
			});
		}
		// SAFETY: geteuid has no preconditions and cannot fail.
		let euid = unsafe { libc::geteuid() };
		if metadata.uid() != euid {
			return Err(KeyAccessError::NotPrivate {
				path: path.to_path_buf(),
				reason: format!(
					"owned by uid {}, not the running user {euid}",
					metadata.uid()
				),
			});
		}
		let mode = metadata.mode() & 0o777;
		if mode != 0o600 {
			return Err(KeyAccessError::NotPrivate {
				path: path.to_path_buf(),
				reason: format!("mode {mode:o}; expected 600"),
			});
		}
		Ok(())
	}
}

#[cfg(windows)]
mod platform {
	use super::KeyAccessError;
	use std::fs::File;
	use std::os::windows::io::AsRawHandle;
	use std::path::Path;
	use std::ptr;
	use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HANDLE, HLOCAL, LocalFree};
	use windows_sys::Win32::Security::Authorization::{GetSecurityInfo, SE_FILE_OBJECT};
	use windows_sys::Win32::Security::{
		ACCESS_ALLOWED_ACE, ACE_HEADER, ACL, CreateWellKnownSid, DACL_SECURITY_INFORMATION, GetAce,
		GetLengthSid, GetTokenInformation, IsValidSid, OWNER_SECURITY_INFORMATION,
		SECURITY_DESCRIPTOR, TOKEN_QUERY, TOKEN_USER, TokenUser, WinBuiltinAdministratorsSid,
		WinLocalSystemSid,
	};
	use windows_sys::Win32::System::Threading::{GetCurrentProcess, OpenProcessToken};

	const SECURITY_MAX_SID_SIZE: usize = 68;
	// winnt.h: the only ACE type that grants access.
	const ACCESS_ALLOWED_ACE_TYPE: u8 = 0;
	fn lookup(reason: String) -> KeyAccessError {
		KeyAccessError::Inspect {
			path: "<current process token>".into(),
			reason,
		}
	}

	pub(super) fn check_owner_only(file: &File, path: &Path) -> Result<(), KeyAccessError> {
		let inspect = |reason: String| KeyAccessError::Inspect {
			path: path.to_path_buf(),
			reason,
		};
		let not_private = |reason: String| KeyAccessError::NotPrivate {
			path: path.to_path_buf(),
			reason,
		};
		let metadata = file
			.metadata()
			.map_err(|error| inspect(error.to_string()))?;
		if !metadata.is_file() {
			return Err(not_private("not a regular file".to_owned()));
		}

		let descriptor = read_descriptor(file).map_err(inspect)?;
		let principals = current_principals()?;
		admit(&descriptor, &principals).map_err(not_private)
	}

	/// The parts of a file's security descriptor the policy judges, copied
	/// out so judgement needs no live pointers.
	pub(super) struct Descriptor {
		pub(crate) owner: Vec<u8>,
		/// `None` is a null DACL; otherwise `(ace_type, sid)` per entry.
		pub(crate) entries: Option<Vec<(u8, Vec<u8>)>>,
	}

	pub(super) struct Principals {
		pub(crate) user: Vec<u8>,
		pub(crate) system: Vec<u8>,
		pub(crate) administrators: Vec<u8>,
	}

	impl Principals {
		fn admits(&self, sid: &[u8]) -> bool {
			sid == self.user.as_slice()
				|| sid == self.system.as_slice()
				|| sid == self.administrators.as_slice()
		}
	}

	/// The policy itself, free of FFI so the foreign-owner case is testable
	/// without the privilege to hand a file to another account.
	pub(super) fn admit(descriptor: &Descriptor, principals: &Principals) -> Result<(), String> {
		// The owner can rewrite the DACL at will, so an acceptable DACL under
		// a foreign owner is only acceptable until that owner changes it.
		if !principals.admits(&descriptor.owner) {
			return Err(
				"owned by a principal other than the running user, SYSTEM, or Administrators"
					.to_owned(),
			);
		}
		let Some(entries) = &descriptor.entries else {
			// A null DACL is "no access control": everyone has full access.
			return Err("null DACL grants access to everyone".to_owned());
		};
		for (index, (ace_type, sid)) in entries.iter().enumerate() {
			if *ace_type != ACCESS_ALLOWED_ACE_TYPE {
				return Err(format!(
					"access entry {index} is type {ace_type}, not an allow entry"
				));
			}
			if !principals.admits(sid) {
				return Err(format!(
					"access entry {index} admits a principal other than the owner, SYSTEM, or Administrators"
				));
			}
		}
		Ok(())
	}

	/// The named-pipe policy (ADR 0166 §4), stricter than the key policy: the
	/// running user owns the instance and is the only principal any entry
	/// admits. SYSTEM and Administrators are not admitted: nothing in the
	/// agent transport runs as either, so nothing should be able to reach it
	/// as either.
	pub(super) fn admit_pipe(descriptor: &Descriptor, user: &[u8]) -> Result<(), String> {
		if descriptor.owner != user {
			return Err("owned by a principal other than the running user".to_owned());
		}
		let Some(entries) = &descriptor.entries else {
			return Err("null DACL grants access to everyone".to_owned());
		};
		for (index, (ace_type, sid)) in entries.iter().enumerate() {
			if *ace_type != ACCESS_ALLOWED_ACE_TYPE {
				return Err(format!(
					"access entry {index} is type {ace_type}, not an allow entry"
				));
			}
			if sid != user {
				return Err(format!(
					"access entry {index} admits a principal other than the running user"
				));
			}
		}
		Ok(())
	}

	pub(super) fn current_principals() -> Result<Principals, KeyAccessError> {
		Ok(Principals {
			user: current_user_sid()?,
			system: well_known_sid(WinLocalSystemSid)?,
			administrators: well_known_sid(WinBuiltinAdministratorsSid)?,
		})
	}

	pub(super) fn read_descriptor(file: &File) -> Result<Descriptor, String> {
		// SAFETY: the handle is open for the lifetime of `file`.
		unsafe { read_handle_descriptor(file.as_raw_handle() as HANDLE) }
	}

	/// # Safety
	/// `handle` must be an open handle to a file-system object (a file, a
	/// directory, or a named pipe) for the duration of the call.
	pub(super) unsafe fn read_handle_descriptor(handle: HANDLE) -> Result<Descriptor, String> {
		let mut owner: *mut core::ffi::c_void = ptr::null_mut();
		let mut dacl: *mut ACL = ptr::null_mut();
		let mut descriptor: *mut SECURITY_DESCRIPTOR = ptr::null_mut();
		// SAFETY: the handle is open for the lifetime of `file`; out-pointers
		// are valid; the descriptor is released below with LocalFree.
		let status = unsafe {
			GetSecurityInfo(
				handle,
				SE_FILE_OBJECT,
				OWNER_SECURITY_INFORMATION | DACL_SECURITY_INFORMATION,
				&mut owner,
				ptr::null_mut(),
				&mut dacl,
				ptr::null_mut(),
				&mut descriptor as *mut _ as *mut *mut _,
			)
		};
		if status != ERROR_SUCCESS {
			return Err(format!("GetSecurityInfo failed with {status}"));
		}
		let result = copy_descriptor(owner, dacl);
		// SAFETY: descriptor was allocated by GetSecurityInfo for us to free;
		// owner and dacl point inside it and are not used after this.
		unsafe { LocalFree(descriptor as HLOCAL) };
		result
	}

	fn copy_descriptor(
		owner: *mut core::ffi::c_void,
		dacl: *mut ACL,
	) -> Result<Descriptor, String> {
		// SAFETY: owner points inside the live descriptor.
		if owner.is_null() || unsafe { IsValidSid(owner) } == 0 {
			return Err("file has no valid owner".to_owned());
		}
		// SAFETY: a valid SID's length is readable through GetLengthSid.
		let owner = unsafe { copy_sid(owner) };
		if dacl.is_null() {
			return Ok(Descriptor {
				owner,
				entries: None,
			});
		}
		// SAFETY: dacl is a valid ACL inside the live descriptor.
		let count = unsafe { (*dacl).AceCount };
		let mut entries = Vec::with_capacity(usize::from(count));
		for index in 0..u32::from(count) {
			let mut ace: *mut core::ffi::c_void = ptr::null_mut();
			// SAFETY: index is below AceCount; ace receives a pointer into dacl.
			if unsafe { GetAce(dacl, index, &mut ace) } == 0 {
				return Err(format!("GetAce({index}) failed"));
			}
			// SAFETY: every ACE begins with an ACE_HEADER.
			let header = unsafe { *(ace as *const ACE_HEADER) };
			// Every ACE type this policy could admit or refuse places its SID at
			// the ACCESS_ALLOWED_ACE offset; object ACEs differ, but those are
			// refused by type before the SID is read.
			let sid = if header.AceType == ACCESS_ALLOWED_ACE_TYPE {
				// SAFETY: the type check guarantees the ACCESS_ALLOWED_ACE layout,
				// whose SidStart is the first DWORD of a variable-length SID.
				let sid = unsafe { ptr::addr_of!((*(ace as *const ACCESS_ALLOWED_ACE)).SidStart) }
					as *mut core::ffi::c_void;
				// SAFETY: sid points inside the ACE, which is inside the live ACL.
				if unsafe { IsValidSid(sid) } == 0 {
					return Err(format!("access entry {index} has an invalid SID"));
				}
				// SAFETY: validated just above.
				unsafe { copy_sid(sid) }
			} else {
				Vec::new()
			};
			entries.push((header.AceType, sid));
		}
		Ok(Descriptor {
			owner,
			entries: Some(entries),
		})
	}

	/// # Safety
	/// `sid` must point to a valid SID.
	unsafe fn copy_sid(sid: *mut core::ffi::c_void) -> Vec<u8> {
		// SAFETY: the caller guarantees a valid SID, whose length GetLengthSid reports.
		let length = unsafe { GetLengthSid(sid) } as usize;
		// SAFETY: a valid SID occupies `length` bytes from its start.
		unsafe { std::slice::from_raw_parts(sid as *const u8, length) }.to_vec()
	}

	fn well_known_sid(kind: i32) -> Result<Vec<u8>, KeyAccessError> {
		let mut sid = vec![0u8; SECURITY_MAX_SID_SIZE];
		let mut size = SECURITY_MAX_SID_SIZE as u32;
		// SAFETY: the buffer is SECURITY_MAX_SID_SIZE bytes, the documented maximum.
		if unsafe {
			CreateWellKnownSid(kind, ptr::null_mut(), sid.as_mut_ptr() as *mut _, &mut size)
		} == 0
		{
			return Err(lookup(format!("CreateWellKnownSid({kind}) failed")));
		}
		sid.truncate(size as usize);
		Ok(sid)
	}

	fn current_user_sid() -> Result<Vec<u8>, KeyAccessError> {
		let inspect = lookup;
		let mut token: HANDLE = ptr::null_mut();
		// SAFETY: querying the current process token needs no preconditions.
		if unsafe { OpenProcessToken(GetCurrentProcess(), TOKEN_QUERY, &mut token) } == 0 {
			return Err(inspect("OpenProcessToken failed".to_owned()));
		}
		let mut needed = 0u32;
		// SAFETY: a null buffer with zero length asks for the needed size.
		unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut needed) };
		let mut buffer = vec![0u8; needed as usize];
		// SAFETY: the buffer is exactly the size the previous call asked for.
		let ok = unsafe {
			GetTokenInformation(
				token,
				TokenUser,
				buffer.as_mut_ptr() as *mut _,
				needed,
				&mut needed,
			)
		};
		// SAFETY: token came from OpenProcessToken above.
		unsafe { windows_sys::Win32::Foundation::CloseHandle(token) };
		if ok == 0 {
			return Err(inspect("GetTokenInformation(TokenUser) failed".to_owned()));
		}
		// SAFETY: the buffer holds a TOKEN_USER whose Sid points inside it.
		let token_user = unsafe { &*(buffer.as_ptr() as *const TOKEN_USER) };
		let sid = token_user.User.Sid;
		// SAFETY: GetLengthSid on a SID GetTokenInformation returned.
		let length = unsafe { GetLengthSid(sid) } as usize;
		// SAFETY: sid points to `length` bytes inside `buffer`.
		Ok(unsafe { std::slice::from_raw_parts(sid as *const u8, length) }.to_vec())
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn scratch(name: &str) -> PathBuf {
		std::env::temp_dir().join(format!(
			"key-guard-{name}-{}-{}",
			std::process::id(),
			std::thread::current()
				.name()
				.unwrap_or("t")
				.replace("::", "-")
		))
	}

	#[cfg(unix)]
	mod unix {
		use super::*;
		use std::os::unix::fs::PermissionsExt;

		#[test]
		fn owner_only_mode_is_admitted_and_the_same_handle_is_returned() {
			let path = scratch("ok");
			std::fs::write(&path, b"secret").unwrap();
			std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
			let mut file = open_private_key_owner_only(&path).expect("admit 0600");
			let mut contents = String::new();
			std::io::Read::read_to_string(&mut file, &mut contents).unwrap();
			assert_eq!(contents, "secret");
			let _ = std::fs::remove_file(&path);
		}

		#[test]
		fn group_or_world_readable_is_refused() {
			for mode in [0o640, 0o604, 0o644, 0o660, 0o700] {
				let path = scratch(&format!("mode{mode:o}"));
				std::fs::write(&path, b"secret").unwrap();
				std::fs::set_permissions(&path, std::fs::Permissions::from_mode(mode)).unwrap();
				let error = open_private_key_owner_only(&path).expect_err("refuse");
				assert!(
					matches!(error, KeyAccessError::NotPrivate { .. }),
					"{mode:o}: {error}"
				);
				let _ = std::fs::remove_file(&path);
			}
		}

		#[test]
		fn a_directory_is_refused() {
			let path = scratch("dir");
			std::fs::create_dir_all(&path).unwrap();
			std::fs::set_permissions(&path, std::fs::Permissions::from_mode(0o600)).unwrap();
			let error = open_private_key_owner_only(&path).expect_err("refuse");
			assert!(
				matches!(error, KeyAccessError::NotPrivate { .. }),
				"{error}"
			);
			let _ = std::fs::remove_dir(&path);
		}
	}

	#[test]
	fn a_missing_file_reports_open_failure() {
		let path = scratch("missing");
		let error = open_private_key_owner_only(&path).expect_err("missing");
		assert!(matches!(error, KeyAccessError::Open { .. }), "{error}");
	}

	#[cfg(windows)]
	mod windows {
		use super::*;
		use std::process::Command;

		#[test]
		fn a_private_directory_makes_its_files_owner_only() {
			let dir = scratch("private-dir");
			let _ = std::fs::remove_dir_all(&dir);
			ensure_private_directory(&dir).expect("create private directory");
			ensure_private_directory(&dir).expect("an existing private directory passes");
			let key = dir.join("inherited.key");
			std::fs::write(&key, b"secret").unwrap();
			open_private_key_owner_only(&key).expect("inherits the owner-only ACL");
			// Widening the directory itself is refused on the next check.
			icacls(&dir, &["/grant", "*S-1-5-32-545:(R)"]);
			let error = ensure_private_directory(&dir).expect_err("refuse Users on the directory");
			assert!(
				matches!(error, KeyAccessError::NotPrivate { .. }),
				"{error}"
			);
			let _ = std::fs::remove_dir_all(&dir);
		}

		/// `icacls` is the operator-facing tool for exactly these ACLs, so the
		/// tests set them the way an operator would.
		fn icacls(path: &Path, args: &[&str]) {
			let status = Command::new("icacls")
				.arg(path)
				.args(args)
				.output()
				.expect("run icacls");
			assert!(
				status.status.success(),
				"icacls {args:?}: {}",
				String::from_utf8_lossy(&status.stdout)
			);
		}

		fn current_user() -> String {
			let output = Command::new("whoami").output().expect("whoami");
			String::from_utf8(output.stdout).unwrap().trim().to_owned()
		}

		fn owner_only_file(name: &str) -> PathBuf {
			let path = scratch(name);
			std::fs::write(&path, b"secret").unwrap();
			// Drop inheritance and every inherited entry, then grant the owner alone.
			icacls(&path, &["/inheritance:r"]);
			icacls(&path, &["/grant:r", &format!("{}:(R,W)", current_user())]);
			path
		}

		#[test]
		fn owner_only_acl_is_admitted_and_the_same_handle_is_returned() {
			let path = owner_only_file("ok");
			let mut file = open_private_key_owner_only(&path).expect("admit owner-only ACL");
			let mut contents = String::new();
			std::io::Read::read_to_string(&mut file, &mut contents).unwrap();
			assert_eq!(contents, "secret");
			let _ = std::fs::remove_file(&path);
		}

		#[test]
		fn system_and_administrators_entries_are_admitted() {
			let path = owner_only_file("sys-admin");
			icacls(&path, &["/grant", "*S-1-5-18:(F)", "*S-1-5-32-544:(F)"]);
			open_private_key_owner_only(&path).expect("admit SYSTEM and Administrators");
			let _ = std::fs::remove_file(&path);
		}

		#[test]
		fn an_entry_for_users_is_refused() {
			let path = owner_only_file("users");
			icacls(&path, &["/grant", "*S-1-5-32-545:(R)"]);
			let error = open_private_key_owner_only(&path).expect_err("refuse Users");
			assert!(
				matches!(error, KeyAccessError::NotPrivate { .. }),
				"{error}"
			);
			let _ = std::fs::remove_file(&path);
		}

		#[test]
		fn inherited_entries_are_not_ignored() {
			// A fresh temp file inherits the user profile's ACL, which carries
			// entries beyond the owner (SYSTEM, Administrators, and often more).
			// Whatever it inherits, an inherited entry for Everyone must refuse.
			let path = scratch("inherited");
			std::fs::write(&path, b"secret").unwrap();
			icacls(&path, &["/grant", "*S-1-1-0:(R)"]);
			let error = open_private_key_owner_only(&path).expect_err("refuse Everyone");
			assert!(
				matches!(error, KeyAccessError::NotPrivate { .. }),
				"{error}"
			);
			let _ = std::fs::remove_file(&path);
		}

		// Policy cases that need another account's ownership are judged on copied
		// descriptors: no privilege can hand a test file to a foreign owner.
		mod policy {
			use crate::platform::{Descriptor, Principals, admit};

			fn principals() -> Principals {
				Principals {
					user: b"user".to_vec(),
					system: b"system".to_vec(),
					administrators: b"admins".to_vec(),
				}
			}

			fn owner_only(owner: &[u8]) -> Descriptor {
				Descriptor {
					owner: owner.to_vec(),
					entries: Some(vec![(0, b"user".to_vec())]),
				}
			}

			#[test]
			fn a_foreign_owner_is_refused_even_with_an_acceptable_dacl() {
				let error = admit(&owner_only(b"someone-else"), &principals()).unwrap_err();
				assert!(error.contains("owned by a principal other than"), "{error}");
			}

			#[test]
			fn the_pipe_policy_admits_the_running_user_alone() {
				use crate::platform::admit_pipe;
				admit_pipe(&owner_only(b"user"), b"user").unwrap();
				// What the key policy admits, the pipe policy does not.
				for owner in [b"system".as_slice(), b"admins"] {
					let error = admit_pipe(&owner_only(owner), b"user").unwrap_err();
					assert!(
						error.contains("owned by a principal other than the running user"),
						"{error}"
					);
				}
				for extra in [b"system".as_slice(), b"admins", b"everyone"] {
					let descriptor = Descriptor {
						owner: b"user".to_vec(),
						entries: Some(vec![(0, b"user".to_vec()), (0, extra.to_vec())]),
					};
					let error = admit_pipe(&descriptor, b"user").unwrap_err();
					assert!(error.contains("access entry 1 admits"), "{error}");
				}
				let null = Descriptor {
					owner: b"user".to_vec(),
					entries: None,
				};
				assert!(admit_pipe(&null, b"user").is_err());
				let deny = Descriptor {
					owner: b"user".to_vec(),
					entries: Some(vec![(1, b"user".to_vec())]),
				};
				assert!(
					admit_pipe(&deny, b"user")
						.unwrap_err()
						.contains("not an allow entry")
				);
			}

			#[test]
			fn the_user_system_and_administrators_may_own_the_key() {
				for owner in [b"user".as_slice(), b"system", b"admins"] {
					admit(&owner_only(owner), &principals()).unwrap();
				}
			}

			#[test]
			fn a_deny_entry_is_refused_by_type_before_its_sid_is_judged() {
				let descriptor = Descriptor {
					owner: b"user".to_vec(),
					entries: Some(vec![(1, Vec::new())]),
				};
				let error = admit(&descriptor, &principals()).unwrap_err();
				assert!(error.contains("not an allow entry"), "{error}");
			}

			#[test]
			fn ownership_is_judged_before_the_dacl() {
				let descriptor = Descriptor {
					owner: b"someone-else".to_vec(),
					entries: None,
				};
				let error = admit(&descriptor, &principals()).unwrap_err();
				assert!(error.contains("owned by"), "{error}");
			}
		}

		#[test]
		fn a_null_dacl_is_refused() {
			let path = scratch("null-dacl");
			std::fs::write(&path, b"secret").unwrap();
			// SDDL "D:NO_ACCESS_CONTROL" sets a null DACL.
			let status = Command::new("powershell")
				.args([
					"-NoProfile",
					"-Command",
					&format!(
						"$a = Get-Acl -LiteralPath '{}'; $a.SetSecurityDescriptorSddlForm('D:NO_ACCESS_CONTROL'); Set-Acl -LiteralPath '{}' -AclObject $a",
						path.display(),
						path.display()
					),
				])
				.status()
				.expect("powershell");
			assert!(status.success());
			let error = open_private_key_owner_only(&path).expect_err("refuse null DACL");
			assert!(
				matches!(error, KeyAccessError::NotPrivate { ref reason, .. } if reason.contains("null DACL")),
				"{error}"
			);
			let _ = std::fs::remove_file(&path);
		}
	}
}

/// A directory that only its owner may enter or list: the contract for an
/// agent runtime directory. It is distinct from the private-key contract (a
/// key is a regular file; this must be a directory) but judged by the same
/// principals. On Unix the directory is created `0700`, must be owned by the
/// running user, and must not be a symlink. On Windows it is created with a
/// protected, inheritable owner-only DACL, so files written inside satisfy
/// [`open_private_key_owner_only`] without ACL work of their own, and an
/// existing directory must already pass that judgement.
pub fn ensure_private_directory(path: &Path) -> Result<(), KeyAccessError> {
	directory::ensure_private_directory(path)
}

#[cfg(unix)]
mod directory {
	use super::KeyAccessError;
	use std::os::unix::fs::{DirBuilderExt, MetadataExt, PermissionsExt};
	use std::path::Path;

	pub(super) fn ensure_private_directory(path: &Path) -> Result<(), KeyAccessError> {
		let inspect = |reason: String| KeyAccessError::Inspect {
			path: path.to_path_buf(),
			reason,
		};
		std::fs::DirBuilder::new()
			.recursive(true)
			.mode(0o700)
			.create(path)
			.map_err(|error| inspect(error.to_string()))?;
		let metadata =
			std::fs::symlink_metadata(path).map_err(|error| inspect(error.to_string()))?;
		// SAFETY: geteuid has no preconditions and cannot fail.
		let euid = unsafe { libc::geteuid() };
		if !metadata.is_dir() || metadata.uid() != euid {
			return Err(KeyAccessError::NotPrivate {
				path: path.to_path_buf(),
				reason: "must be a directory owned by the running user and not a symlink"
					.to_owned(),
			});
		}
		std::fs::set_permissions(path, std::fs::Permissions::from_mode(0o700))
			.map_err(|error| inspect(error.to_string()))
	}
}

#[cfg(windows)]
mod directory {
	use super::KeyAccessError;
	use super::platform::{Principals, admit, current_principals, read_descriptor};
	use std::os::windows::fs::OpenOptionsExt;
	use std::os::windows::io::AsRawHandle;
	use std::path::Path;
	use std::ptr;
	use windows_sys::Win32::Foundation::{ERROR_SUCCESS, HANDLE};
	use windows_sys::Win32::Security::Authorization::{SE_FILE_OBJECT, SetSecurityInfo};
	use windows_sys::Win32::Security::{
		ACL, AddAccessAllowedAceEx, CONTAINER_INHERIT_ACE, DACL_SECURITY_INFORMATION,
		InitializeAcl, OBJECT_INHERIT_ACE, PROTECTED_DACL_SECURITY_INFORMATION,
	};

	const ACL_REVISION: u32 = 2;
	// winbase.h: required to open a directory handle at all.
	const FILE_FLAG_BACKUP_SEMANTICS: u32 = 0x0200_0000;
	const READ_CONTROL: u32 = 0x0002_0000;
	const WRITE_DAC: u32 = 0x0004_0000;
	const GENERIC_ALL: u32 = 0x1000_0000;

	pub(super) fn ensure_private_directory(path: &Path) -> Result<(), KeyAccessError> {
		let inspect = |reason: String| KeyAccessError::Inspect {
			path: path.to_path_buf(),
			reason,
		};
		let created = !path.exists();
		std::fs::create_dir_all(path).map_err(|error| inspect(error.to_string()))?;
		let metadata =
			std::fs::symlink_metadata(path).map_err(|error| inspect(error.to_string()))?;
		if !metadata.is_dir() {
			return Err(KeyAccessError::NotPrivate {
				path: path.to_path_buf(),
				reason: "must be a directory and not a reparse point".to_owned(),
			});
		}
		// A directory handle needs backup semantics to open at all; reading the
		// descriptor needs READ_CONTROL and setting it needs WRITE_DAC, which the
		// owner always holds.
		let handle = std::fs::OpenOptions::new()
			.access_mode(READ_CONTROL | WRITE_DAC)
			.custom_flags(FILE_FLAG_BACKUP_SEMANTICS)
			.open(path)
			.map_err(|error| inspect(error.to_string()))?;
		let principals = current_principals()?;
		if created {
			set_owner_only_dacl(&handle, &principals).map_err(inspect)?;
		}
		let descriptor = read_descriptor(&handle).map_err(inspect)?;
		admit(&descriptor, &principals).map_err(|reason| KeyAccessError::NotPrivate {
			path: path.to_path_buf(),
			reason,
		})
	}

	/// One allow entry per admitted principal, inherited by everything
	/// created below, and protected from the parent's inheritance.
	fn set_owner_only_dacl(handle: &std::fs::File, principals: &Principals) -> Result<(), String> {
		let sids = [
			&principals.user,
			&principals.system,
			&principals.administrators,
		];
		// ACL header plus, per entry, the ACE header and mask plus the SID.
		let size: u32 = 8 + sids.iter().map(|sid| 8 + sid.len() as u32).sum::<u32>() + 64;
		let mut buffer = vec![0u8; size as usize];
		let acl = buffer.as_mut_ptr() as *mut ACL;
		// SAFETY: the buffer is `size` bytes and exclusively ours.
		if unsafe { InitializeAcl(acl, size, ACL_REVISION) } == 0 {
			return Err("InitializeAcl failed".to_owned());
		}
		for sid in sids {
			// SAFETY: acl was initialized above; sid is a valid SID copy.
			if unsafe {
				AddAccessAllowedAceEx(
					acl,
					ACL_REVISION,
					OBJECT_INHERIT_ACE | CONTAINER_INHERIT_ACE,
					GENERIC_ALL,
					sid.as_ptr() as *mut _,
				)
			} == 0
			{
				return Err("AddAccessAllowedAceEx failed".to_owned());
			}
		}
		// SAFETY: the handle is open for the lifetime of `handle`; acl is valid.
		let status = unsafe {
			SetSecurityInfo(
				handle.as_raw_handle() as HANDLE,
				SE_FILE_OBJECT,
				DACL_SECURITY_INFORMATION | PROTECTED_DACL_SECURITY_INFORMATION,
				ptr::null_mut(),
				ptr::null_mut(),
				acl,
				ptr::null_mut(),
			)
		};
		if status != ERROR_SUCCESS {
			return Err(format!("SetSecurityInfo failed with {status}"));
		}
		Ok(())
	}
}

/// Windows-only helpers for a same-user named pipe (ADR 0166 §4): a
/// self-relative security descriptor owned by and admitting the running user
/// alone, and a check that a connected pipe client's token belongs to the
/// running user.
#[cfg(windows)]
pub mod windows {
	use super::KeyAccessError;
	use super::platform::current_principals;
	use std::ptr;
	use windows_sys::Win32::Foundation::{HANDLE, HLOCAL, LocalFree};
	use windows_sys::Win32::Security::Authorization::{
		ConvertSidToStringSidW, ConvertStringSecurityDescriptorToSecurityDescriptorW,
		SDDL_REVISION_1,
	};
	use windows_sys::Win32::Security::{
		GetSecurityDescriptorLength, GetTokenInformation, TOKEN_QUERY, TOKEN_USER, TokenUser,
	};

	fn inspect(reason: String) -> KeyAccessError {
		KeyAccessError::Inspect {
			path: "<named pipe>".into(),
			reason,
		}
	}

	fn sid_string(sid: &[u8]) -> Result<String, KeyAccessError> {
		let mut text: *mut u16 = ptr::null_mut();
		// SAFETY: sid is a valid SID copy; text receives a LocalAlloc'd string.
		if unsafe { ConvertSidToStringSidW(sid.as_ptr() as *mut _, &mut text) } == 0 {
			return Err(inspect("ConvertSidToStringSidW failed".to_owned()));
		}
		let mut length = 0;
		// SAFETY: the string is NUL-terminated.
		while unsafe { *text.add(length) } != 0 {
			length += 1;
		}
		// SAFETY: `length` UTF-16 units precede the terminator.
		let value = String::from_utf16_lossy(unsafe { std::slice::from_raw_parts(text, length) });
		// SAFETY: releasing what ConvertSidToStringSidW allocated.
		unsafe { LocalFree(text as HLOCAL) };
		Ok(value)
	}

	/// The named-pipe server instance behind `pipe` is owned by the running
	/// user and admits that user alone (ADR 0166 §4): the pipe policy, not
	/// the private-key policy.
	///
	/// # Safety
	/// `pipe` must be an open named-pipe handle for the duration of the call.
	pub unsafe fn pipe_admits_current_user_only(pipe: HANDLE) -> Result<(), KeyAccessError> {
		// SAFETY: forwarded from the caller.
		let descriptor =
			unsafe { super::platform::read_handle_descriptor(pipe) }.map_err(inspect)?;
		let principals = current_principals()?;
		super::platform::admit_pipe(&descriptor, &principals.user).map_err(|reason| {
			KeyAccessError::NotPrivate {
				path: "<named pipe>".into(),
				reason,
			}
		})
	}

	/// Returns the bytes of a descriptor for creating the agent pipe: owned by
	/// the running user, with a protected DACL admitting that user alone. The
	/// owner is explicit because, left unset, an elevated administrator's
	/// token makes `BUILTIN\Administrators` the pipe's ordinary owner. Keep
	/// the bytes alive for as long as the `SECURITY_ATTRIBUTES` that points at
	/// them.
	pub fn current_user_only_security_descriptor() -> Result<Vec<u8>, KeyAccessError> {
		let principals = current_principals()?;
		let user = sid_string(&principals.user)?;
		let sddl = format!("O:{user}D:P(A;;GA;;;{user})");
		let mut wide: Vec<u16> = sddl.encode_utf16().chain(Some(0)).collect();
		let mut descriptor: *mut core::ffi::c_void = ptr::null_mut();
		let mut size = 0_u32;
		// SAFETY: wide is NUL-terminated; descriptor receives a LocalAlloc'd
		// self-relative descriptor of `size` bytes.
		if unsafe {
			ConvertStringSecurityDescriptorToSecurityDescriptorW(
				wide.as_mut_ptr(),
				SDDL_REVISION_1,
				&mut descriptor as *mut _ as *mut _,
				&mut size,
			)
		} == 0
		{
			return Err(inspect("SDDL conversion failed".to_owned()));
		}
		// SAFETY: descriptor is valid; its length is what the conversion reported.
		let length = unsafe { GetSecurityDescriptorLength(descriptor as *mut _) } as usize;
		// SAFETY: `length` bytes are readable at descriptor.
		let bytes = unsafe { std::slice::from_raw_parts(descriptor as *const u8, length) }.to_vec();
		// SAFETY: releasing the conversion's allocation.
		unsafe { LocalFree(descriptor as HLOCAL) };
		Ok(bytes)
	}

	/// The client at the far end of the named-pipe server instance `pipe` runs
	/// as the current user: its process token's user SID equals ours. This is
	/// a token comparison, not impersonation.
	///
	/// # Safety
	/// `pipe` must be an open, connected named-pipe server handle for the
	/// duration of the call.
	pub unsafe fn pipe_client_is_current_user(pipe: HANDLE) -> Result<bool, KeyAccessError> {
		use windows_sys::Win32::Foundation::CloseHandle;
		use windows_sys::Win32::System::Pipes::GetNamedPipeClientProcessId;
		use windows_sys::Win32::System::Threading::{
			OpenProcess, OpenProcessToken, PROCESS_QUERY_LIMITED_INFORMATION,
		};
		let mut pid = 0_u32;
		// SAFETY: pipe is a connected server end owned by the caller.
		if unsafe { GetNamedPipeClientProcessId(pipe, &mut pid) } == 0 {
			return Err(inspect("GetNamedPipeClientProcessId failed".to_owned()));
		}
		// SAFETY: querying a process object needs no memory preconditions.
		let process = unsafe { OpenProcess(PROCESS_QUERY_LIMITED_INFORMATION, 0, pid) };
		if process.is_null() {
			return Err(inspect(format!("cannot open pipe client process {pid}")));
		}
		let mut token: HANDLE = ptr::null_mut();
		// SAFETY: process is a valid handle from OpenProcess.
		let opened = unsafe { OpenProcessToken(process, TOKEN_QUERY, &mut token) } != 0;
		// SAFETY: closing the handle we opened.
		unsafe { CloseHandle(process) };
		if !opened {
			return Err(inspect(
				"OpenProcessToken failed for the pipe client".to_owned(),
			));
		}
		let result = token_user_sid(token);
		// SAFETY: closing the token we opened.
		unsafe { CloseHandle(token) };
		let client = sid_string(&result?)?;
		let own = sid_string(&current_principals()?.user)?;
		Ok(client == own)
	}

	fn token_user_sid(token: HANDLE) -> Result<Vec<u8>, KeyAccessError> {
		let mut needed = 0_u32;
		// SAFETY: a null buffer with zero length asks for the needed size.
		unsafe { GetTokenInformation(token, TokenUser, ptr::null_mut(), 0, &mut needed) };
		let mut buffer = vec![0u8; needed as usize];
		// SAFETY: the buffer is the size the previous call asked for.
		if unsafe {
			GetTokenInformation(
				token,
				TokenUser,
				buffer.as_mut_ptr() as *mut _,
				needed,
				&mut needed,
			)
		} == 0
		{
			return Err(inspect("GetTokenInformation(TokenUser) failed".to_owned()));
		}
		// SAFETY: the buffer holds a TOKEN_USER whose Sid points inside it.
		let token_user = unsafe { &*(buffer.as_ptr() as *const TOKEN_USER) };
		// SAFETY: GetLengthSid on a SID GetTokenInformation returned.
		let length =
			unsafe { windows_sys::Win32::Security::GetLengthSid(token_user.User.Sid) } as usize;
		// SAFETY: sid points to `length` bytes inside `buffer`.
		Ok(
			unsafe { std::slice::from_raw_parts(token_user.User.Sid as *const u8, length) }
				.to_vec(),
		)
	}
}
