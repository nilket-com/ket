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
		let principals = Principals {
			user: current_user_sid()?,
			system: well_known_sid(WinLocalSystemSid)?,
			administrators: well_known_sid(WinBuiltinAdministratorsSid)?,
		};
		admit(&descriptor, &principals).map_err(not_private)
	}

	/// The parts of a file's security descriptor the policy judges, copied
	/// out so judgement needs no live pointers.
	pub(crate) struct Descriptor {
		pub(crate) owner: Vec<u8>,
		/// `None` is a null DACL; otherwise `(ace_type, sid)` per entry.
		pub(crate) entries: Option<Vec<(u8, Vec<u8>)>>,
	}

	pub(crate) struct Principals {
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
	pub(crate) fn admit(descriptor: &Descriptor, principals: &Principals) -> Result<(), String> {
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

	fn read_descriptor(file: &File) -> Result<Descriptor, String> {
		let mut owner: *mut core::ffi::c_void = ptr::null_mut();
		let mut dacl: *mut ACL = ptr::null_mut();
		let mut descriptor: *mut SECURITY_DESCRIPTOR = ptr::null_mut();
		// SAFETY: the handle is open for the lifetime of `file`; out-pointers
		// are valid; the descriptor is released below with LocalFree.
		let status = unsafe {
			GetSecurityInfo(
				file.as_raw_handle() as HANDLE,
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
