//! Non-secret Polariton preferences.
//!
//! Editor documents and future connection profiles belong to libaryon. This
//! module deliberately persists only presentation preferences in plaintext.

use tokio_rusqlite::{Connection, rusqlite};

#[derive(Clone, Default)]
pub struct StartupData {
	pub window_size: Option<(f32, f32)>,
	pub show_column_types: bool,
}

fn config_dir() -> std::path::PathBuf {
	let mut path = std::env::home_dir().unwrap_or_default();
	path.push(".config/polariton/");
	std::fs::create_dir_all(&path).ok();
	path
}

fn public_db_path() -> std::path::PathBuf {
	config_dir().join("public.sqlite")
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
	let (window_size, show_column_types) = conn
		.call(|db| {
			let get = |key: &str| -> Option<String> {
				db.query_row("SELECT value FROM settings WHERE key = ?1", [key], |row| {
					row.get::<_, String>(0)
				})
				.ok()
			};
			let window_size = get("window_size").and_then(|value| {
				let mut parts = value.splitn(2, ',');
				Some((parts.next()?.parse().ok()?, parts.next()?.parse().ok()?))
			});
			let show_column_types = get("show_column_types")
				.and_then(|value| value.parse().ok())
				.unwrap_or(false);
			Ok::<_, rusqlite::Error>((window_size, show_column_types))
		})
		.await
		.unwrap_or_default();
	StartupData {
		window_size,
		show_column_types,
	}
}

pub async fn save_show_column_types(value: bool) {
	save_setting("show_column_types", value.to_string()).await;
}

pub async fn save_window_size(width: f32, height: f32) {
	save_setting("window_size", format!("{width},{height}")).await;
}

async fn save_setting(key: &'static str, value: String) {
	let conn = open_public().await;
	conn.call(move |db| {
		db.execute(
			"INSERT OR REPLACE INTO settings (key, value) VALUES (?1, ?2)",
			[key, value.as_str()],
		)?;
		Ok::<(), rusqlite::Error>(())
	})
	.await
	.ok();
}
