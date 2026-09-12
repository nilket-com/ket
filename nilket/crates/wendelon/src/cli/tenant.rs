//! ADR 0167: tenant provisioning as an operator command. `create`, `get`,
//! and `list` are the operator surface over the existing tenant
//! administration RPC; the Raft-committed handler and its role policy stay
//! the authority for whether a tenant comes to exist.

use std::process::ExitCode;

use super::index::{ConnectionOptions, connect, parse_connection};
use crate::network::control_server::{TenantAdminRequestWire, TenantAdminTenantWire};
use crate::node::DEFAULT_TENANT_QUOTA_BYTES;
use crate::schema::TenantId;

#[derive(Clone, Debug, PartialEq, Eq)]
struct TenantOptions {
	connection: ConnectionOptions,
	operation: TenantOperation,
	json: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum TenantOperation {
	Create {
		tenant_id: TenantId,
		quota_bytes: u64,
		/// `None` takes the cluster's replication factor once connected.
		replication_factor: Option<u32>,
	},
	Get {
		tenant_id: TenantId,
	},
	List,
}

pub(super) fn run(args: &[String]) -> ExitCode {
	let options = match parse_args(args) {
		Ok(options) => options,
		Err(message) => {
			eprintln!("{message}");
			return ExitCode::from(2);
		}
	};
	let runtime = match tokio::runtime::Builder::new_current_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("failed to initialize Wendelon tenant runtime: {error}");
			return ExitCode::from(2);
		}
	};
	match runtime.block_on(run_async(options)) {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			eprintln!("wendelon tenant: {error}");
			ExitCode::from(1)
		}
	}
}

async fn run_async(options: TenantOptions) -> std::io::Result<()> {
	let client = connect(&options.connection).await?;
	match options.operation {
		TenantOperation::Create {
			tenant_id,
			quota_bytes,
			replication_factor,
		} => {
			let replication_factor = match replication_factor {
				Some(explicit) => explicit,
				None => u32::try_from(client.replication_factor()).map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidData,
						"cluster replication factor does not fit the request",
					)
				})?,
			};
			let response = client
				.tenant_admin(&TenantAdminRequestWire::Create {
					target_tenant_id: tenant_id,
					quota_bytes,
					replication_factor,
				})
				.await?;
			let tenant = response.tenant.ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::InvalidData,
					"the cluster accepted the tenant but returned no record",
				)
			})?;
			if options.json {
				println!("{}", render_json(&[tenant]));
			} else {
				println!("created tenant {}", render_line(&tenant));
			}
		}
		TenantOperation::Get { tenant_id } => {
			let response = client
				.tenant_admin(&TenantAdminRequestWire::Get {
					target_tenant_id: tenant_id,
				})
				.await?;
			let tenant = response.tenant.ok_or_else(|| {
				std::io::Error::new(
					std::io::ErrorKind::NotFound,
					format!("tenant {} does not exist", tenant_id.get()),
				)
			})?;
			if options.json {
				println!("{}", render_json(&[tenant]));
			} else {
				println!("{}", render_line(&tenant));
			}
		}
		TenantOperation::List => {
			let response = client.tenant_admin(&TenantAdminRequestWire::List).await?;
			if options.json {
				println!("{}", render_json(&response.tenants));
			} else {
				for tenant in &response.tenants {
					println!("{}", render_line(tenant));
				}
			}
		}
	}
	Ok(())
}

fn render_line(tenant: &TenantAdminTenantWire) -> String {
	let quota = if tenant.quota_bytes == u64::MAX {
		"unlimited".to_owned()
	} else {
		format!("{} bytes", tenant.quota_bytes)
	};
	format!(
		"{}\t{:?}\tquota {quota}\tused {} bytes\treplication {}",
		tenant.tenant_id.get(),
		tenant.state,
		tenant.used_bytes,
		tenant.replication_factor
	)
}

fn render_json(tenants: &[TenantAdminTenantWire]) -> String {
	serde_json::to_string_pretty(tenants).expect("tenant records serialize")
}

fn parse_args(args: &[String]) -> Result<TenantOptions, String> {
	let operation_name = args
		.first()
		.ok_or_else(|| "wendelon tenant requires create, get, or list".to_owned())?;
	let mut tenant_id = None;
	let mut quota_bytes = None;
	let mut replication_factor = None;
	let mut json = false;
	let mut connection_args = Vec::new();
	let mut cursor = 1usize;
	while let Some(flag) = args.get(cursor) {
		let value = |name: &str| {
			args.get(cursor + 1)
				.cloned()
				.ok_or_else(|| format!("wendelon tenant {name} requires a value"))
		};
		match flag.as_str() {
			"--tenant" => {
				tenant_id = Some(
					value("--tenant")?
						.parse::<u64>()
						.map(TenantId::new)
						.map_err(|error| format!("invalid tenant id: {error}"))?,
				)
			}
			"--quota-bytes" => {
				quota_bytes = Some(
					value("--quota-bytes")?
						.parse::<u64>()
						.map_err(|error| format!("invalid --quota-bytes: {error}"))?,
				)
			}
			"--replication-factor" => {
				replication_factor = Some(
					value("--replication-factor")?
						.parse::<u32>()
						.map_err(|error| format!("invalid --replication-factor: {error}"))?,
				)
			}
			"--json" => {
				json = true;
				cursor += 1;
				continue;
			}
			"--bootstrap" | "--cert" | "--key" | "--ca" | "--cluster-domain" => {
				connection_args.push(flag.clone());
				connection_args.push(value(flag)?);
			}
			unknown => return Err(format!("unknown wendelon tenant option: {unknown}")),
		}
		cursor += 2;
	}
	let connection = parse_connection("wendelon tenant", &connection_args)?;
	let operation = match operation_name.as_str() {
		"create" => TenantOperation::Create {
			tenant_id: tenant_id
				.ok_or_else(|| "wendelon tenant create requires --tenant".to_owned())?,
			quota_bytes: quota_bytes.unwrap_or(DEFAULT_TENANT_QUOTA_BYTES),
			replication_factor,
		},
		"get" => TenantOperation::Get {
			tenant_id: tenant_id
				.ok_or_else(|| "wendelon tenant get requires --tenant".to_owned())?,
		},
		"list" => TenantOperation::List,
		other => return Err(format!("unknown wendelon tenant operation: {other}")),
	};
	if matches!(operation, TenantOperation::List) && tenant_id.is_some() {
		return Err("wendelon tenant list takes no --tenant".to_owned());
	}
	Ok(TenantOptions {
		connection,
		operation,
		json,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn args(operation: &str, extra: &[&str]) -> Vec<String> {
		[
			operation,
			"--bootstrap",
			"1@127.0.0.1:7001",
			"--cert",
			"operator.crt",
			"--key",
			"operator.key",
			"--ca",
			"operator-ca.crt",
		]
		.iter()
		.chain(extra)
		.map(|s| (*s).to_owned())
		.collect()
	}

	#[test]
	fn adr0167_create_defaults_to_the_bootstrap_tenants_quota_and_the_clusters_replication() {
		let parsed = parse_args(&args("create", &["--tenant", "1"])).unwrap();
		assert_eq!(
			parsed.operation,
			TenantOperation::Create {
				tenant_id: TenantId::new(1),
				quota_bytes: u64::MAX,
				replication_factor: None,
			}
		);
		assert_eq!(parsed.connection.bootstrap_node_id, 1);
		let explicit = parse_args(&args(
			"create",
			&[
				"--tenant",
				"9",
				"--quota-bytes",
				"4096",
				"--replication-factor",
				"3",
				"--json",
			],
		))
		.unwrap();
		assert!(explicit.json);
		assert_eq!(
			explicit.operation,
			TenantOperation::Create {
				tenant_id: TenantId::new(9),
				quota_bytes: 4096,
				replication_factor: Some(3),
			}
		);
	}

	#[test]
	fn adr0167_get_and_list_are_explicit_and_the_identity_is_required() {
		assert!(matches!(
			parse_args(&args("get", &["--tenant", "4"]))
				.unwrap()
				.operation,
			TenantOperation::Get { .. }
		));
		assert!(matches!(
			parse_args(&args("list", &[])).unwrap().operation,
			TenantOperation::List
		));
		assert!(parse_args(&args("list", &["--tenant", "4"])).is_err());
		assert!(
			parse_args(&args("create", &[])).is_err(),
			"create needs --tenant"
		);
		let no_identity: Vec<String> = ["list", "--bootstrap", "1@127.0.0.1:7001"]
			.iter()
			.map(|s| (*s).to_owned())
			.collect();
		assert!(
			parse_args(&no_identity).is_err(),
			"an operator identity is required"
		);
		assert!(parse_args(&args("suspend", &["--tenant", "4"])).is_err());
	}

	#[test]
	fn adr0167_lines_name_the_unlimited_quota() {
		let tenant = TenantAdminTenantWire {
			tenant_id: TenantId::new(1),
			quota_bytes: u64::MAX,
			used_bytes: 0,
			replication_factor: 1,
			state: crate::consensus::core::TenantLifecycleState::Active,
			active_tenant_kek_id: None,
			tenant_kek_accounting_complete: false,
			tenant_kek_versions: Vec::new(),
		};
		assert_eq!(
			render_line(&tenant),
			"1\tActive\tquota unlimited\tused 0 bytes\treplication 1"
		);
	}
}
