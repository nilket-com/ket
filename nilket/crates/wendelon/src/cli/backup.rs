//! ADR 0736: the backup operator commands.
//!
//! ```text
//! backup export  --to <new dir> <connection> --storage <tenant>=<cert>,<key>... --passphrase-file <path>
//! backup verify  <dir> --passphrase-file <path>
//! backup restore <dir> <target connection> --storage <tenant>=<cert>,<key>... --passphrase-file <path>
//! backup release --id <hex> --entered-at <index> <connection>
//! backup hold-status <connection>
//! ```
//!
//! `<connection>` is the operator's (`--bootstrap --cert --key --ca
//! [--cluster-domain]`). Each `--storage` names one tenant's storage
//! certificate, used for that tenant's table reads (export) or writes
//! (restore) over the same bootstrap, CA and domain. The passphrase is read
//! from a file, which may be a descriptor (`/dev/fd/3`); one trailing newline
//! is dropped and an empty passphrase is refused. `restore` verifies the whole
//! backup (R1) before it connects to the target.

use std::collections::BTreeMap;
use std::path::PathBuf;
use std::process::ExitCode;

use super::index::{ConnectionOptions, connect, parse_connection};
use crate::logical_backup::export::{self, Clients};
use crate::logical_backup::hold::{self, Bounds, Release};
use crate::logical_backup::restore;
use crate::schema::TenantId;

#[derive(Clone, Debug, PartialEq, Eq)]
struct Storage {
	tenant_id: TenantId,
	cert_path: PathBuf,
	key_path: PathBuf,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum BackupCommand {
	Export {
		dir: PathBuf,
		connection: ConnectionOptions,
		storage: Vec<Storage>,
		passphrase_file: PathBuf,
	},
	Verify {
		dir: PathBuf,
		passphrase_file: PathBuf,
	},
	Restore {
		dir: PathBuf,
		connection: ConnectionOptions,
		storage: Vec<Storage>,
		passphrase_file: PathBuf,
	},
	Release {
		id: u128,
		entered_at: u64,
		connection: ConnectionOptions,
	},
	HoldStatus {
		connection: ConnectionOptions,
	},
}

pub(super) fn run(args: &[String]) -> ExitCode {
	let command = match parse_args(args) {
		Ok(command) => command,
		Err(message) => {
			eprintln!("{message}");
			return ExitCode::from(2);
		}
	};
	let runtime = match tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("failed to initialize Wendelon backup runtime: {error}");
			return ExitCode::from(2);
		}
	};
	match runtime.block_on(run_async(command)) {
		Ok(()) => ExitCode::SUCCESS,
		Err(message) => {
			eprintln!("wendelon backup: {message}");
			ExitCode::from(1)
		}
	}
}

async fn run_async(command: BackupCommand) -> Result<(), String> {
	match command {
		BackupCommand::Export {
			dir,
			connection,
			storage,
			passphrase_file,
		} => {
			let passphrase = read_passphrase(&passphrase_file)?;
			let clients = clients(&connection, &storage).await?;
			let report = export::export(&clients, &dir, &passphrase)
				.await
				.map_err(|error| error.to_string())?;
			println!(
				"published backup {:#x} at {}: {} tables, {} rows, {} chunks; roster cut {}, \
				 continuity cut {}; hold (id {:#x}, entered at {}) released",
				report.backup_id,
				dir.display(),
				report.tables,
				report.rows,
				report.chunks,
				report.cut,
				report.continuity_cut,
				report.hold.0,
				report.hold.1
			);
		}
		BackupCommand::Verify {
			dir,
			passphrase_file,
		} => {
			let passphrase = read_passphrase(&passphrase_file)?;
			let verified = restore::verify(&dir, &passphrase).map_err(|error| error.to_string())?;
			let manifest = &verified.manifest;
			println!(
				"verified backup {:#x}: {} tenants, {} tables, {} rows, {} chunks",
				manifest.backup_id,
				manifest.roster.tenants.len(),
				manifest.tables.len(),
				manifest.tables.iter().map(|table| table.rows).sum::<u64>(),
				manifest.chunks.len()
			);
		}
		BackupCommand::Restore {
			dir,
			connection,
			storage,
			passphrase_file,
		} => {
			let passphrase = read_passphrase(&passphrase_file)?;
			// R1 before anything reaches the target.
			let verified = restore::verify(&dir, &passphrase).map_err(|error| error.to_string())?;
			let clients = clients(&connection, &storage).await?;
			let report = restore::restore(&clients, &dir, &verified)
				.await
				.map_err(|error| error.to_string())?;
			println!(
				"restored and verified backup {:#x}: {} tables, {} rows, {} chunks, at target cut \
				 {}.\nThe restored cluster is verified, not yet in service. Old operation ids have \
				 no authority here: callers drop, never replay, the old installation's unresolved \
				 work. Only now issue application credentials and publish the endpoints.",
				report.backup_id, report.tables, report.rows, report.chunks, report.verified_cut
			);
		}
		BackupCommand::Release {
			id,
			entered_at,
			connection,
		} => {
			let client = bounded_connect(&connection, SETUP_BOUND, "operator connection").await?;
			match hold::release(&client, (id, entered_at), Bounds::DEFAULT).await {
				Release::Released => {
					println!("released the backup hold (id {id:#x}, entered at {entered_at})")
				}
				Release::Unconfirmed { why } => {
					return Err(format!(
						"the release was not confirmed and may or may not have applied ({why}). \
						 `backup hold-status` shows the current hold"
					));
				}
			}
		}
		BackupCommand::HoldStatus { connection } => {
			let client = bounded_connect(&connection, SETUP_BOUND, "operator connection").await?;
			match hold::read(&client, Bounds::DEFAULT)
				.await
				.map_err(|error| error.to_string())?
			{
				Some((id, entered_at)) => {
					println!("backup hold held: --id {id:#x} --entered-at {entered_at}")
				}
				None => println!("no backup hold is held"),
			}
		}
	}
	Ok(())
}

async fn clients(connection: &ConnectionOptions, storage: &[Storage]) -> Result<Clients, String> {
	let admin = bounded_connect(connection, SETUP_BOUND, "operator connection").await?;
	let mut data = BTreeMap::new();
	for entry in storage {
		let options = ConnectionOptions {
			cert_path: entry.cert_path.clone(),
			key_path: entry.key_path.clone(),
			..connection.clone()
		};
		let what = format!("storage connection for tenant {}", entry.tenant_id.get());
		let client = bounded_connect(&options, SETUP_BOUND, &what).await?;
		data.insert(entry.tenant_id, client);
	}
	Ok(Clients { admin, data })
}

/// How long one connection's setup (credentials, transport, first topology
/// read) may take before a backup command gives up on it.
const SETUP_BOUND: std::time::Duration = std::time::Duration::from_secs(30);

async fn bounded_connect(
	options: &ConnectionOptions,
	budget: std::time::Duration,
	what: &str,
) -> Result<std::sync::Arc<crate::client::core::WendelonClient>, String> {
	bounded_setup(connect(options), budget, what).await
}

/// `setup`, unless it does not finish within `budget`.
async fn bounded_setup<T>(
	setup: impl std::future::Future<Output = std::io::Result<T>>,
	budget: std::time::Duration,
	what: &str,
) -> Result<T, String> {
	match tokio::time::timeout(budget, setup).await {
		Ok(Ok(value)) => Ok(value),
		Ok(Err(error)) => Err(format!("{what}: {error}")),
		Err(_) => Err(format!("{what}: not set up within {} s", budget.as_secs())),
	}
}

fn read_passphrase(path: &std::path::Path) -> Result<Vec<u8>, String> {
	let mut bytes = std::fs::read(path)
		.map_err(|error| format!("passphrase file {}: {error}", path.display()))?;
	if bytes.last() == Some(&b'\n') {
		bytes.pop();
	}
	if bytes.is_empty() {
		return Err("the passphrase is empty".to_owned());
	}
	Ok(bytes)
}

fn parse_storage(value: &str) -> Result<Storage, String> {
	let (tenant, files) = value
		.split_once('=')
		.ok_or_else(|| "--storage must be <tenant>=<cert>,<key>".to_owned())?;
	let (cert, key) = files
		.split_once(',')
		.ok_or_else(|| "--storage must be <tenant>=<cert>,<key>".to_owned())?;
	let tenant_id = tenant
		.parse::<u64>()
		.map(TenantId::new)
		.map_err(|error| format!("invalid --storage tenant: {error}"))?;
	if tenant_id == TenantId::ZERO {
		return Err("the default tenant 0 has no storage certificate".to_owned());
	}
	Ok(Storage {
		tenant_id,
		cert_path: PathBuf::from(cert),
		key_path: PathBuf::from(key),
	})
}

fn parse_args(args: &[String]) -> Result<BackupCommand, String> {
	let operation = args.first().ok_or_else(|| {
		"wendelon backup requires export, verify, restore, release or hold-status".to_owned()
	})?;
	let mut cursor = 1usize;
	// verify and restore name the backup directory first.
	let positional = match operation.as_str() {
		"verify" | "restore" => {
			cursor = 2;
			Some(PathBuf::from(args.get(1).ok_or_else(|| {
				format!("wendelon backup {operation} requires the backup directory")
			})?))
		}
		_ => None,
	};
	let mut to = None;
	let mut storage = Vec::new();
	let mut passphrase_file = None;
	let mut id = None;
	let mut entered_at = None;
	let mut connection_args = Vec::new();
	while let Some(flag) = args.get(cursor) {
		let value = args
			.get(cursor + 1)
			.cloned()
			.ok_or_else(|| format!("wendelon backup {flag} requires a value"))?;
		match flag.as_str() {
			"--to" => to = Some(PathBuf::from(value)),
			"--storage" => storage.push(parse_storage(&value)?),
			"--passphrase-file" => passphrase_file = Some(PathBuf::from(value)),
			"--id" => {
				id = Some(
					u128::from_str_radix(value.trim_start_matches("0x"), 16)
						.map_err(|error| format!("invalid --id: {error}"))?,
				)
			}
			"--entered-at" => {
				entered_at = Some(
					value
						.parse::<u64>()
						.map_err(|error| format!("invalid --entered-at: {error}"))?,
				)
			}
			"--bootstrap" | "--cert" | "--key" | "--ca" | "--cluster-domain" => {
				connection_args.push(flag.clone());
				connection_args.push(value);
			}
			unknown => return Err(format!("unknown wendelon backup option: {unknown}")),
		}
		cursor += 2;
	}
	let mut tenants = std::collections::BTreeSet::new();
	if let Some(duplicate) = storage
		.iter()
		.find(|entry| !tenants.insert(entry.tenant_id))
	{
		return Err(format!(
			"--storage names tenant {} twice",
			duplicate.tenant_id.get()
		));
	}
	let passphrase = || {
		passphrase_file
			.clone()
			.ok_or_else(|| format!("wendelon backup {operation} requires --passphrase-file"))
	};
	let connection = || parse_connection("wendelon backup", &connection_args);
	let none_of = |what: &[(&str, bool)]| match what.iter().find(|(_, given)| *given) {
		Some((flag, _)) => Err(format!("wendelon backup {operation} takes no {flag}")),
		None => Ok(()),
	};
	match operation.as_str() {
		"export" => {
			none_of(&[
				("--id", id.is_some()),
				("--entered-at", entered_at.is_some()),
			])?;
			Ok(BackupCommand::Export {
				dir: to.ok_or_else(|| "wendelon backup export requires --to".to_owned())?,
				connection: connection()?,
				storage,
				passphrase_file: passphrase()?,
			})
		}
		"verify" => {
			none_of(&[
				("--to", to.is_some()),
				("--storage", !storage.is_empty()),
				("--id", id.is_some()),
				("--entered-at", entered_at.is_some()),
				("connection options", !connection_args.is_empty()),
			])?;
			Ok(BackupCommand::Verify {
				dir: positional.expect("verify has a directory"),
				passphrase_file: passphrase()?,
			})
		}
		"restore" => {
			none_of(&[
				("--to", to.is_some()),
				("--id", id.is_some()),
				("--entered-at", entered_at.is_some()),
			])?;
			Ok(BackupCommand::Restore {
				dir: positional.expect("restore has a directory"),
				connection: connection()?,
				storage,
				passphrase_file: passphrase()?,
			})
		}
		"release" => {
			none_of(&[
				("--to", to.is_some()),
				("--storage", !storage.is_empty()),
				("--passphrase-file", passphrase_file.is_some()),
			])?;
			Ok(BackupCommand::Release {
				id: id.ok_or_else(|| "wendelon backup release requires --id".to_owned())?,
				entered_at: entered_at
					.ok_or_else(|| "wendelon backup release requires --entered-at".to_owned())?,
				connection: connection()?,
			})
		}
		"hold-status" => {
			none_of(&[
				("--to", to.is_some()),
				("--storage", !storage.is_empty()),
				("--passphrase-file", passphrase_file.is_some()),
				("--id", id.is_some()),
				("--entered-at", entered_at.is_some()),
			])?;
			Ok(BackupCommand::HoldStatus {
				connection: connection()?,
			})
		}
		other => Err(format!("unknown wendelon backup operation: {other}")),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn args(list: &[&str]) -> Vec<String> {
		list.iter().map(|s| (*s).to_owned()).collect()
	}

	const CONNECTION: &[&str] = &[
		"--bootstrap",
		"1@127.0.0.1:7001",
		"--cert",
		"operator.crt",
		"--key",
		"operator.key",
		"--ca",
		"node-ca.crt",
	];

	#[test]
	fn each_operation_takes_exactly_its_own_options() {
		let mut export = args(&["export", "--to", "/b", "--passphrase-file", "/dev/fd/3"]);
		export.extend(args(CONNECTION));
		export.extend(args(&[
			"--storage",
			"1=s.crt,s.key",
			"--storage",
			"2=t.crt,t.key",
		]));
		let BackupCommand::Export {
			dir,
			storage,
			connection,
			..
		} = parse_args(&export).unwrap()
		else {
			panic!("export");
		};
		assert_eq!(dir, PathBuf::from("/b"));
		assert_eq!(connection.bootstrap_node_id, 1);
		assert_eq!(
			storage
				.iter()
				.map(|entry| entry.tenant_id.get())
				.collect::<Vec<_>>(),
			vec![1, 2]
		);
		assert!(matches!(
			parse_args(&args(&["verify", "/b", "--passphrase-file", "p"])).unwrap(),
			BackupCommand::Verify { .. }
		));
		let mut release = args(&["release", "--id", "0x736", "--entered-at", "42"]);
		release.extend(args(CONNECTION));
		assert!(matches!(
			parse_args(&release).unwrap(),
			BackupCommand::Release {
				id: 0x736,
				entered_at: 42,
				..
			}
		));

		// Refusals.
		let mut verify_with_connection = args(&["verify", "/b", "--passphrase-file", "p"]);
		verify_with_connection.extend(args(CONNECTION));
		assert!(parse_args(&verify_with_connection).is_err());
		assert!(
			parse_args(&args(&["verify", "/b"])).is_err(),
			"no passphrase"
		);
		let mut tenant_zero = args(&["restore", "/b", "--passphrase-file", "p"]);
		tenant_zero.extend(args(CONNECTION));
		tenant_zero.extend(args(&["--storage", "0=s.crt,s.key"]));
		assert!(
			parse_args(&tenant_zero).is_err(),
			"tenant 0 has no storage cert"
		);
		let mut twice = export.clone();
		twice.extend(args(&["--storage", "1=u.crt,u.key"]));
		assert!(parse_args(&twice).is_err(), "a tenant named twice");
		let mut release_with_passphrase = release.clone();
		release_with_passphrase.extend(args(&["--passphrase-file", "p"]));
		assert!(parse_args(&release_with_passphrase).is_err());
		assert!(parse_args(&args(&["export", "--to", "/b", "--passphrase-file", "p"])).is_err());
		assert!(parse_args(&args(&["prune"])).is_err());
		let mut status = args(&["hold-status"]);
		status.extend(args(CONNECTION));
		assert!(matches!(
			parse_args(&status).unwrap(),
			BackupCommand::HoldStatus { .. }
		));
		status.extend(args(&["--id", "1"]));
		assert!(parse_args(&status).is_err());
	}

	/// A connection setup that never completes ends at its budget.
	#[tokio::test]
	async fn a_setup_that_never_completes_ends_at_its_budget() {
		let started = std::time::Instant::now();
		let answer = bounded_setup(
			std::future::pending::<std::io::Result<()>>(),
			std::time::Duration::from_millis(200),
			"operator connection",
		)
		.await;
		assert_eq!(
			answer,
			Err("operator connection: not set up within 0 s".to_owned())
		);
		assert!(started.elapsed() < std::time::Duration::from_secs(5));
	}

	#[test]
	fn the_passphrase_loses_one_trailing_newline_and_may_not_be_empty() {
		let dir = crate::test_support::TestDir::new("backup-passphrase");
		let path = dir.to_path_buf().join("p");
		std::fs::write(&path, b"secret\n").unwrap();
		assert_eq!(read_passphrase(&path).unwrap(), b"secret");
		std::fs::write(&path, b"\n").unwrap();
		assert!(read_passphrase(&path).is_err());
	}
}
