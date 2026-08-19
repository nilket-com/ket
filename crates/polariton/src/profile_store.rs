//! PRODUCT-0134 §4–§7: the encrypted saved-connection-profile store.
//!
//! One sealed `BARYENC\0` file beside `init.rn` holds a map keyed by a
//! user-chosen profile name; each value is an adapter type plus the field map
//! the configuration editor produced. Profile names live inside the
//! ciphertext, so listing requires the passphrase — accepted by the record,
//! not worked around. The store owns its own [`SecretSession`], deliberately
//! separate from libaryon's encrypted-document session (§7): unlocking a text
//! document must never unlock a database credential, and the reverse.

use libaryon::secrets::SecretSession;
use serde::{Deserialize, Serialize};
use std::collections::BTreeMap;
use std::path::PathBuf;
use zeroize::Zeroizing;

pub(crate) const SCHEMA_VERSION: u32 = 1;

/// One saved profile: an adapter type plus the field map of PRODUCT-0134 §3.
/// There is deliberately no `Debug` derive — a profile carries secure field
/// values, and the redaction story belongs to [`AdapterConfiguration`]'s
/// manual `Debug`, not to a second one here.
///
/// [`AdapterConfiguration`]: crate::adapters::driver::AdapterConfiguration
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct SavedProfile {
	pub adapter_type: String,
	pub fields: BTreeMap<String, String>,
}

#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct StorePayload {
	schema_version: u32,
	profiles: BTreeMap<String, SavedProfile>,
}

/// The encrypted store. All profile operations require an unlocked session;
/// the passphrase prompt timing (first store access) and the first-write
/// double-entry creation flow are the interface's concern, not this type's.
#[derive(Default)]
pub struct ProfileStore {
	session: SecretSession,
	profiles: BTreeMap<String, SavedProfile>,
}

const LOCKED: &str = "The connection profile store is locked.";

fn valid_name(name: &str) -> Result<&str, String> {
	let trimmed = name.trim();
	if trimmed.is_empty() {
		return Err("A profile name is required.".to_owned());
	}
	Ok(trimmed)
}

impl ProfileStore {
	/// The store file beside `init.rn`, under the same platform rule.
	pub fn default_path() -> Option<PathBuf> {
		crate::configuration::default_init_path()
			.map(|init| init.with_file_name("connection_profiles.enc"))
	}

	pub fn is_unlocked(&self) -> bool {
		self.session.is_unlocked()
	}

	/// Unlock against the sealed file at `path`. A wrong passphrase fails the
	/// authenticated open, locks the session again, and leaves the file and
	/// any previously loaded state untouched. A missing file refuses: creating
	/// a store is [`ProfileStore::initialize`], a distinct flow, so a typo in
	/// a store path can never silently mint an empty store.
	pub fn unlock(&mut self, path: &PathBuf, passphrase: Zeroizing<Vec<u8>>) -> Result<(), String> {
		let contents = std::fs::read(path)
			.map_err(|error| format!("could not read the profile store: {error}"))?;
		self.session.unlock(passphrase)?;
		let plaintext = match self.session.open(&contents) {
			Ok(plaintext) => plaintext,
			Err(error) => {
				self.session.lock();
				return Err(error);
			}
		};
		let payload: StorePayload = match serde_json::from_slice(&plaintext) {
			Ok(payload) => payload,
			Err(error) => {
				self.session.lock();
				return Err(format!("profile store payload: {error}"));
			}
		};
		if payload.schema_version > SCHEMA_VERSION {
			self.session.lock();
			return Err(format!(
				"profile store schema {} is newer than this build supports",
				payload.schema_version
			));
		}
		self.profiles = payload.profiles;
		Ok(())
	}

	/// Establish a new store with a chosen passphrase. Refuses when the file
	/// already exists — creation never overwrites a store it cannot open.
	/// The double-entry confirmation belongs to the prompt, upstream.
	pub fn initialize(
		&mut self,
		path: &PathBuf,
		passphrase: Zeroizing<Vec<u8>>,
	) -> Result<(), String> {
		if path.exists() {
			return Err("A profile store already exists; unlock it instead.".to_owned());
		}
		self.session.unlock(passphrase)?;
		self.profiles.clear();
		self.persist(path)
	}

	pub fn lock(&mut self) {
		self.session.lock();
		self.profiles.clear();
	}

	pub fn names(&self) -> Result<Vec<&str>, String> {
		self.require_unlocked()?;
		Ok(self.profiles.keys().map(String::as_str).collect())
	}

	pub fn get(&self, name: &str) -> Result<&SavedProfile, String> {
		self.require_unlocked()?;
		self.profiles
			.get(valid_name(name)?)
			.ok_or_else(|| format!("No saved profile named {name}."))
	}

	/// PRODUCT-0134 §4: a create never silently replaces a stored credential.
	pub fn save_new(
		&mut self,
		path: &PathBuf,
		name: &str,
		profile: SavedProfile,
	) -> Result<(), String> {
		self.require_unlocked()?;
		let name = valid_name(name)?;
		if self.profiles.contains_key(name) {
			return Err(format!(
				"A profile named {name} already exists; edit it instead."
			));
		}
		self.profiles.insert(name.to_owned(), profile);
		self.persist(path)
	}

	/// PRODUCT-0134 §4: the identity being edited is the identity written.
	pub fn replace(
		&mut self,
		path: &PathBuf,
		name: &str,
		profile: SavedProfile,
	) -> Result<(), String> {
		self.require_unlocked()?;
		let name = valid_name(name)?;
		if !self.profiles.contains_key(name) {
			return Err(format!("No saved profile named {name}."));
		}
		self.profiles.insert(name.to_owned(), profile);
		self.persist(path)
	}

	/// PRODUCT-0134 §4: renaming onto an existing name refuses, for the same
	/// reason a create does.
	pub fn rename(&mut self, path: &PathBuf, from: &str, to: &str) -> Result<(), String> {
		self.require_unlocked()?;
		let from = valid_name(from)?;
		let to = valid_name(to)?;
		if from == to {
			return Ok(());
		}
		if self.profiles.contains_key(to) {
			return Err(format!("A profile named {to} already exists."));
		}
		let Some(profile) = self.profiles.remove(from) else {
			return Err(format!("No saved profile named {from}."));
		};
		self.profiles.insert(to.to_owned(), profile);
		self.persist(path)
	}

	pub fn delete(&mut self, path: &PathBuf, name: &str) -> Result<(), String> {
		self.require_unlocked()?;
		let name = valid_name(name)?;
		if self.profiles.remove(name).is_none() {
			return Err(format!("No saved profile named {name}."));
		}
		self.persist(path)
	}

	fn require_unlocked(&self) -> Result<(), String> {
		if self.session.is_unlocked() {
			Ok(())
		} else {
			Err(LOCKED.to_owned())
		}
	}

	fn persist(&self, path: &PathBuf) -> Result<(), String> {
		let payload = StorePayload {
			schema_version: SCHEMA_VERSION,
			profiles: self.profiles.clone(),
		};
		let bytes = serde_json::to_vec(&payload)
			.map_err(|error| format!("profile store payload: {error}"))?;
		self.session.write(path, &bytes)
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn temp_store_path(tag: &str) -> PathBuf {
		let dir = std::env::temp_dir().join(format!(
			"polariton-0134-store-{tag}-{}-{}",
			std::process::id(),
			std::time::SystemTime::now()
				.duration_since(std::time::UNIX_EPOCH)
				.unwrap()
				.as_nanos()
		));
		std::fs::create_dir_all(&dir).expect("temp dir");
		dir.join("connection_profiles.enc")
	}

	fn passphrase(text: &str) -> Zeroizing<Vec<u8>> {
		Zeroizing::new(text.as_bytes().to_vec())
	}

	fn postgres_profile() -> SavedProfile {
		SavedProfile {
			adapter_type: "Postgres".to_owned(),
			fields: BTreeMap::from([
				("host".to_owned(), "db.example.com".to_owned()),
				("port".to_owned(), "6432".to_owned()),
				("database".to_owned(), "analytics".to_owned()),
				("user".to_owned(), "user".to_owned()),
				("password".to_owned(), "sekrit".to_owned()),
			]),
		}
	}

	fn cleanup(path: &PathBuf) {
		let _ = std::fs::remove_dir_all(path.parent().unwrap());
	}

	#[test]
	fn product_0134_store_round_trips_a_profile_across_instances() {
		let path = temp_store_path("roundtrip");
		let mut store = ProfileStore::default();
		store
			.initialize(&path, passphrase("open sesame"))
			.expect("initialize");
		store
			.save_new(&path, "prod-warehouse", postgres_profile())
			.expect("save");
		drop(store);

		let mut reopened = ProfileStore::default();
		reopened
			.unlock(&path, passphrase("open sesame"))
			.expect("unlock");
		assert_eq!(reopened.names().expect("names"), ["prod-warehouse"]);
		let profile = reopened.get("prod-warehouse").expect("profile");
		assert!(*profile == postgres_profile());

		let configuration = crate::adapters::driver::AdapterConfiguration::from_saved(
			&profile.adapter_type,
			&profile.fields,
		);
		assert!(matches!(
			configuration,
			crate::adapters::driver::AdapterConfiguration::PostgresFields {
				host, port, database, user, password
			} if host == "db.example.com" && port == "6432" && database == "analytics"
				&& user == "user" && password == "sekrit"
		));
		cleanup(&path);
	}

	#[test]
	fn product_0134_store_refuses_create_rename_collisions_and_empty_names() {
		let path = temp_store_path("collisions");
		let mut store = ProfileStore::default();
		store.initialize(&path, passphrase("pw")).expect("initialize");
		store
			.save_new(&path, "prod", postgres_profile())
			.expect("save");

		let error = store
			.save_new(&path, "prod", postgres_profile())
			.expect_err("create on an existing name refuses");
		assert!(error.contains("already exists"), "{error}");
		let error = store
			.save_new(&path, "  prod  ", postgres_profile())
			.expect_err("names compare after trimming");
		assert!(error.contains("already exists"), "{error}");

		store
			.save_new(&path, "staging", postgres_profile())
			.expect("second profile");
		let error = store
			.rename(&path, "staging", "prod")
			.expect_err("rename onto an existing name refuses");
		assert!(error.contains("already exists"), "{error}");

		let error = store
			.save_new(&path, "   ", postgres_profile())
			.expect_err("an all-whitespace name refuses");
		assert!(error.contains("required"), "{error}");

		store
			.replace(&path, "prod", postgres_profile())
			.expect("edit replaces in place");
		let error = store
			.replace(&path, "absent", postgres_profile())
			.expect_err("replace requires an existing identity");
		assert!(error.contains("No saved profile"), "{error}");

		store.delete(&path, "staging").expect("delete");
		assert_eq!(store.names().expect("names"), ["prod"]);
		cleanup(&path);
	}

	/// The exact-match rule is chosen, not incidental: `prod` and `Prod` are
	/// two profiles (PRODUCT-0134 §4).
	#[test]
	fn product_0134_store_names_are_exact_after_trimming() {
		let path = temp_store_path("exact");
		let mut store = ProfileStore::default();
		store.initialize(&path, passphrase("pw")).expect("initialize");
		store
			.save_new(&path, "prod", postgres_profile())
			.expect("lower");
		store
			.save_new(&path, "Prod", postgres_profile())
			.expect("upper coexists");
		assert_eq!(store.names().expect("names"), ["Prod", "prod"]);
		cleanup(&path);
	}

	#[test]
	fn product_0134_store_failed_open_changes_nothing_and_stays_locked() {
		let path = temp_store_path("failed-open");
		let mut store = ProfileStore::default();
		store.initialize(&path, passphrase("right")).expect("initialize");
		store
			.save_new(&path, "prod", postgres_profile())
			.expect("save");
		drop(store);
		let sealed = std::fs::read(&path).expect("sealed bytes");

		let mut reopened = ProfileStore::default();
		let error = reopened
			.unlock(&path, passphrase("wrong"))
			.expect_err("wrong passphrase refuses");
		assert!(!error.is_empty());
		assert!(!reopened.is_unlocked(), "a failed open locks again");
		assert!(reopened.names().is_err(), "locked stores answer nothing");
		assert_eq!(
			std::fs::read(&path).expect("sealed bytes"),
			sealed,
			"a failed open never clears, rewrites, or migrates the store"
		);

		reopened
			.unlock(&path, passphrase("right"))
			.expect("retry succeeds");
		assert_eq!(reopened.names().expect("names"), ["prod"]);
		cleanup(&path);
	}

	#[test]
	fn product_0134_store_initialize_refuses_an_existing_file_and_locked_refuses_writes() {
		let path = temp_store_path("initialize");
		let mut store = ProfileStore::default();
		store.initialize(&path, passphrase("pw")).expect("initialize");
		drop(store);

		let mut second = ProfileStore::default();
		let error = second
			.initialize(&path, passphrase("other"))
			.expect_err("initialize refuses an existing store");
		assert!(error.contains("already exists"), "{error}");

		let error = second
			.save_new(&path, "prod", postgres_profile())
			.expect_err("locked stores refuse writes");
		assert_eq!(error, LOCKED);
		cleanup(&path);
	}

	#[cfg(unix)]
	#[test]
	fn product_0134_store_file_is_private() {
		use std::os::unix::fs::PermissionsExt;
		let path = temp_store_path("private");
		let mut store = ProfileStore::default();
		store.initialize(&path, passphrase("pw")).expect("initialize");
		let mode = std::fs::metadata(&path).expect("metadata").permissions().mode();
		assert_eq!(mode & 0o777, 0o600, "store file must be owner-only");
		cleanup(&path);
	}

	#[test]
	fn product_0134_store_sealed_bytes_disclose_no_names_or_credentials() {
		let path = temp_store_path("sealed");
		let mut store = ProfileStore::default();
		store.initialize(&path, passphrase("pw")).expect("initialize");
		store
			.save_new(&path, "prod-warehouse", postgres_profile())
			.expect("save");
		let sealed = std::fs::read(&path).expect("sealed bytes");
		for needle in [b"prod-warehouse".as_slice(), b"sekrit", b"db.example.com"] {
			assert!(
				!sealed.windows(needle.len()).any(|window| window == needle),
				"sealed store must not disclose profile names or field values"
			);
		}
		cleanup(&path);
	}
}
