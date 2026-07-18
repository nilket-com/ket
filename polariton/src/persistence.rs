use tokio_rusqlite::{rusqlite, Connection};

#[derive(Clone, Debug)]
pub struct SavedConnection {
	pub id: i64,
	pub name: String,
	pub adapter_type: String,
	pub config_value: String,
}

#[derive(Clone, Debug)]
pub struct SavedStatement {
	pub id: i64,
	pub name: String,
	pub code: String,
}

#[derive(Clone, Default)]
pub struct StartupData {
	pub window_size: Option<(f32, f32)>,
	pub salt: Vec<u8>,
	pub is_password_protected: bool,
	pub show_column_types: bool,
}

const SALT_LEN: usize = 32;
const NONCE_LEN: usize = 12;
const KEY_LEN: usize = 32;

fn random_bytes<const N: usize>() -> [u8; N] {
	rand::random()
}

pub fn derive_key(password: &str, salt: &[u8]) -> [u8; KEY_LEN] {
	use argon2::{Algorithm, Argon2, Params, Version};
	let params = Params::new(65536, 1, 1, Some(KEY_LEN)).expect("valid argon2 params");
	let argon2 = Argon2::new(Algorithm::Argon2id, Version::V0x13, params);
	let mut key = [0u8; KEY_LEN];
	argon2
		.hash_password_into(password.as_bytes(), salt, &mut key)
		.expect("argon2 key derivation failed");
	key
}

fn encrypt(key: &[u8; KEY_LEN], plaintext: &[u8]) -> Vec<u8> {
	use chacha20poly1305::{
		aead::{Aead, KeyInit},
		ChaCha20Poly1305, Key, Nonce,
	};
	let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
	let nonce_bytes = random_bytes::<NONCE_LEN>();
	let nonce = Nonce::from_slice(&nonce_bytes);
	let ciphertext = cipher.encrypt(nonce, plaintext).expect("encryption failed");
	let mut result = Vec::with_capacity(NONCE_LEN + ciphertext.len());
	result.extend_from_slice(&nonce_bytes);
	result.extend_from_slice(&ciphertext);
	result
}

fn decrypt(key: &[u8; KEY_LEN], data: &[u8]) -> Result<Vec<u8>, String> {
	use chacha20poly1305::{
		aead::{Aead, KeyInit},
		ChaCha20Poly1305, Key, Nonce,
	};
	if data.len() < NONCE_LEN {
		return Err("ciphertext too short".to_string());
	}
	let (nonce_bytes, ciphertext) = data.split_at(NONCE_LEN);
	let cipher = ChaCha20Poly1305::new(Key::from_slice(key));
	let nonce = Nonce::from_slice(nonce_bytes);
	cipher
		.decrypt(nonce, ciphertext)
		.map_err(|_| "incorrect password".to_string())
}

fn to_hex(bytes: &[u8]) -> String {
	bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn from_hex(s: &str) -> Option<Vec<u8>> {
	if !s.len().is_multiple_of(2) {
		return None;
	}
	(0..s.len())
		.step_by(2)
		.map(|i| u8::from_str_radix(&s[i..i + 2], 16).ok())
		.collect()
}

fn config_dir() -> std::path::PathBuf {
	let mut pth = std::env::home_dir().unwrap_or_default();
	pth.push(".config/polariton/");
	std::fs::create_dir_all(&pth).ok();
	pth
}

fn public_db_path() -> std::path::PathBuf {
	config_dir().join("public.sqlite")
}

fn private_enc_path() -> std::path::PathBuf {
	config_dir().join("private.sqlite.enc")
}

fn private_shadow_path() -> std::path::PathBuf {
	config_dir().join("private.sqlite.enc.tmp")
}

fn db_to_bytes(db: &rusqlite::Connection) -> Vec<u8> {
	use std::ffi::c_char;
	unsafe {
		let mut size: rusqlite::ffi::sqlite3_int64 = 0;
		let ptr = rusqlite::ffi::sqlite3_serialize(
			db.handle(),
			b"main\0".as_ptr() as *const c_char,
			&mut size,
			0,
		);
		if ptr.is_null() || size <= 0 {
			return vec![];
		}
		let bytes = std::slice::from_raw_parts(ptr as *const u8, size as usize).to_vec();
		rusqlite::ffi::sqlite3_free(ptr as *mut _);
		bytes
	}
}

fn bytes_to_db(db: &rusqlite::Connection, data: Vec<u8>) -> rusqlite::Result<()> {
	use std::ffi::c_char;
	if data.is_empty() {
		return Ok(());
	}
	let size = data.len() as rusqlite::ffi::sqlite3_int64;
	unsafe {
		let buf = rusqlite::ffi::sqlite3_malloc64(size as _) as *mut u8;
		if buf.is_null() {
			return Err(rusqlite::Error::SqliteFailure(
				rusqlite::ffi::Error::new(rusqlite::ffi::SQLITE_NOMEM),
				None,
			));
		}
		std::ptr::copy_nonoverlapping(data.as_ptr(), buf, size as usize);
		let rc = rusqlite::ffi::sqlite3_deserialize(
			db.handle(),
			b"main\0".as_ptr() as *const c_char,
			buf,
			size,
			size,
			3u32,
		);
		if rc != rusqlite::ffi::SQLITE_OK {
			rusqlite::ffi::sqlite3_free(buf as *mut _);
			return Err(rusqlite::Error::SqliteFailure(
				rusqlite::ffi::Error::new(rc),
				None,
			));
		}
	}
	Ok(())
}

async fn open_public() -> Connection {
	let conn = Connection::open(public_db_path()).await.unwrap();
	conn.call(|db| {
		db.execute_batch(
			"CREATE TABLE IF NOT EXISTS settings (
				key TEXT PRIMARY KEY,
				value TEXT NOT NULL
			)",
		)?;
		Ok::<(), rusqlite::Error>(())
	})
	.await
	.unwrap();
	conn
}

pub async fn load_startup_data() -> StartupData {
	let conn = open_public().await;
	let (window_size, salt_hex, is_password_protected, show_column_types) = conn
		.call(|db| {
			let get = |key: &str| -> Option<String> {
				db.query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
					row.get::<_, String>(0)
				})
				.ok()
			};
			let window_size = get("window_size").and_then(|s| {
				let mut parts = s.splitn(2, ',');
				let w = parts.next()?.parse::<f32>().ok()?;
				let h = parts.next()?.parse::<f32>().ok()?;
				Some((w, h))
			});
			let salt_hex = get("salt");
			let is_password_protected = get("is_password_protected")
				.and_then(|s| s.parse::<bool>().ok())
				.unwrap_or(false);
			let show_column_types = get("show_column_types")
				.and_then(|s| s.parse::<bool>().ok())
				.unwrap_or(false);
			Ok::<_, rusqlite::Error>((window_size, salt_hex, is_password_protected, show_column_types))
		})
		.await
		.unwrap_or_default();
	let salt = match salt_hex.and_then(|h| from_hex(&h)) {
		Some(s) => s,
		None => {
			let new_salt = random_bytes::<SALT_LEN>();
			let hex = to_hex(&new_salt);
			conn.call(move |db| {
				db.execute(
					"INSERT OR REPLACE INTO settings (key, value) VALUES ('salt', ?1)",
					[hex.as_str()],
				)?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.ok();
			new_salt.to_vec()
		}
	};
	StartupData {
		window_size,
		salt,
		is_password_protected,
		show_column_types,
	}
}

pub async fn save_show_column_types(val: bool) {
	let conn = open_public().await;
	let value = val.to_string();
	conn.call(move |db| {
		db.execute(
			"INSERT OR REPLACE INTO settings (key, value) VALUES ('show_column_types', ?1)",
			[value.as_str()],
		)?;
		Ok::<(), rusqlite::Error>(())
	})
	.await
	.ok();
}

pub async fn save_window_size(width: f32, height: f32) {
	let conn = open_public().await;
	let value = format!("{},{}", width, height);
	conn.call(move |db| {
		db.execute(
			"INSERT OR REPLACE INTO settings (key, value) VALUES ('window_size', ?1)",
			[value.as_str()],
		)?;
		Ok::<(), rusqlite::Error>(())
	})
	.await
	.ok();
}

pub async fn save_is_password_protected(val: bool) {
	let conn = open_public().await;
	let value = val.to_string();
	conn.call(move |db| {
		db.execute(
			"INSERT OR REPLACE INTO settings (key, value) VALUES ('is_password_protected', ?1)",
			[value.as_str()],
		)?;
		Ok::<(), rusqlite::Error>(())
	})
	.await
	.ok();
}

#[derive(Clone)]
pub struct PrivateDb {
	conn: Connection,
	key: [u8; KEY_LEN],
}

impl PrivateDb {
	pub async fn open(salt: &[u8], password: &str) -> Result<Self, String> {
		let key = derive_key(password, salt);
		let enc_path = private_enc_path();
		let plaintext: Vec<u8> = if enc_path.exists() {
			let encrypted = std::fs::read(&enc_path)
				.map_err(|e| format!("failed to read private database: {e}"))?;
			if encrypted.is_empty() {
				vec![]
			} else {
				decrypt(&key, &encrypted)?
			}
		} else {
			vec![]
		};
		let conn = Connection::open_in_memory()
			.await
			.map_err(|e| format!("failed to create in-memory database: {e}"))?;
		if !plaintext.is_empty() {
			conn.call(move |db| {
				bytes_to_db(db, plaintext)?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.map_err(|e| format!("failed to deserialize database: {e}"))?;
		}
		conn.call(|db| {
			db.execute_batch(
				"CREATE TABLE IF NOT EXISTS connections (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					name TEXT NOT NULL,
					adapter_type TEXT NOT NULL,
					config_value TEXT NOT NULL
				);
				CREATE TABLE IF NOT EXISTS statements (
					id INTEGER PRIMARY KEY AUTOINCREMENT,
					name TEXT NOT NULL,
					code TEXT NOT NULL
				)",
			)?;
			Ok::<(), rusqlite::Error>(())
		})
		.await
		.map_err(|e| format!("failed to initialise schema: {e}"))?;
		Ok(PrivateDb { conn, key })
	}

	async fn persist(&self) -> Result<(), String> {
		let plaintext = self
			.conn
			.call(|db| Ok::<Vec<u8>, rusqlite::Error>(db_to_bytes(db)))
			.await
			.map_err(|e| format!("serialization failed: {e}"))?;
		let encrypted = encrypt(&self.key, &plaintext);
		let shadow = private_shadow_path();
		let enc_path = private_enc_path();
		std::fs::write(&shadow, &encrypted)
			.map_err(|e| format!("failed to write shadow file: {e}"))?;
		std::fs::rename(&shadow, &enc_path)
			.map_err(|e| format!("failed to rename shadow to encrypted file: {e}"))?;
		Ok(())
	}

	pub async fn rekey(self, new_key: [u8; KEY_LEN]) -> Result<Self, String> {
		let db = PrivateDb {
			conn: self.conn,
			key: new_key,
		};
		db.persist().await?;
		Ok(db)
	}

	pub async fn load_connections(&self) -> Vec<SavedConnection> {
		self.conn
			.call(|db| {
				let mut stmt = db.prepare(
					"SELECT id, name, adapter_type, config_value FROM connections ORDER BY id",
				)?;
				let rows = stmt
					.query_map([], |row| {
						Ok(SavedConnection {
							id: row.get(0)?,
							name: row.get(1)?,
							adapter_type: row.get(2)?,
							config_value: row.get(3)?,
						})
					})?
					.filter_map(|r| r.ok())
					.collect();
				Ok::<Vec<SavedConnection>, rusqlite::Error>(rows)
			})
			.await
			.unwrap_or_default()
	}

	pub async fn save_connection(
		&self,
		name: String,
		adapter_type: String,
		config_value: String,
	) -> Vec<SavedConnection> {
		self.conn
			.call(move |db| {
				db.execute(
					"INSERT INTO connections (name, adapter_type, config_value) VALUES (?1, ?2, ?3)",
					(name.as_str(), adapter_type.as_str(), config_value.as_str()),
				)?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.ok();
		self.persist().await.ok();
		self.load_connections().await
	}

	pub async fn update_connection(
		&self,
		id: i64,
		name: String,
		config_value: String,
	) -> Vec<SavedConnection> {
		self.conn
			.call(move |db| {
				db.execute(
					"UPDATE connections SET name = ?1, config_value = ?2 WHERE id = ?3",
					(name.as_str(), config_value.as_str(), id),
				)?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.ok();
		self.persist().await.ok();
		self.load_connections().await
	}

	pub async fn delete_connection(&self, id: i64) -> Vec<SavedConnection> {
		self.conn
			.call(move |db| {
				db.execute("DELETE FROM connections WHERE id = ?1", [id])?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.ok();
		self.persist().await.ok();
		self.load_connections().await
	}

	pub async fn load_statements(&self) -> Vec<SavedStatement> {
		self.conn
			.call(|db| {
				let mut stmt = db.prepare("SELECT id, name, code FROM statements ORDER BY id")?;
				let rows = stmt
					.query_map([], |row| {
						Ok(SavedStatement {
							id: row.get(0)?,
							name: row.get(1)?,
							code: row.get(2)?,
						})
					})?
					.filter_map(|r| r.ok())
					.collect();
				Ok::<Vec<SavedStatement>, rusqlite::Error>(rows)
			})
			.await
			.unwrap_or_default()
	}

	pub async fn save_statement(&self, name: String, code: String) -> Vec<SavedStatement> {
		self.conn
			.call(move |db| {
				db.execute(
					"INSERT INTO statements (name, code) VALUES (?1, ?2)",
					(name.as_str(), code.as_str()),
				)?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.ok();
		self.persist().await.ok();
		self.load_statements().await
	}

	pub async fn update_statement(&self, id: i64, name: String, code: String) -> Vec<SavedStatement> {
		self.conn
			.call(move |db| {
				db.execute(
					"UPDATE statements SET name = ?1, code = ?2 WHERE id = ?3",
					(name.as_str(), code.as_str(), id),
				)?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.ok();
		self.persist().await.ok();
		self.load_statements().await
	}

	pub async fn delete_statement(&self, id: i64) -> Vec<SavedStatement> {
		self.conn
			.call(move |db| {
				db.execute("DELETE FROM statements WHERE id = ?1", [id])?;
				Ok::<(), rusqlite::Error>(())
			})
			.await
			.ok();
		self.persist().await.ok();
		self.load_statements().await
	}
}
