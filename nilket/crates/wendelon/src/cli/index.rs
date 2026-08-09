use std::collections::HashMap;
use std::path::PathBuf;
use std::process::ExitCode;
use std::sync::Arc;

use tokio::sync::watch;
use uuid::Uuid;

use crate::client::{
	ClusterPeerWire, ClusterStatusWire, ControlRingTopologySource, WendelonClient,
	WendelonClientConfig,
};
use crate::integration::compute::{
	SchemaGeneration, SchemaIntentId, SchemaMutationKind, SchemaObjectKind, SchemaObjectRef,
	SchemaUpdateEnvelope,
};
use crate::network::factory::TransportProviderFactory;
use crate::network::pool::BufPool;
use crate::network::quic_transport::{
	QuicTransportProvider, canonical_server_name_resolver, endpoint_from_tls,
};
use crate::network::tls::{DEFAULT_CLUSTER_DOMAIN, TlsMaterial};
use crate::network::transport::{
	CapabilityResolver, EpochCapabilityResolver, NodeRole, NodeRoleSet, TransportProvider,
	TransportTier, TransportTierSet,
};
use crate::schema::{
	IndexObjectId, NodeId, SecondaryIndexDescriptor, SecondaryIndexDropRequest,
	SecondaryIndexStatus, TenantId, encode_secondary_index_descriptor,
	encode_secondary_index_drop_request,
};

#[derive(Clone, Debug, PartialEq, Eq)]
struct ConnectionOptions {
	bootstrap_node_id: u32,
	bootstrap_address: String,
	cert_path: PathBuf,
	key_path: PathBuf,
	ca_path: PathBuf,
	cluster_domain: String,
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct IndexOptions {
	connection: ConnectionOptions,
	tenant_id: TenantId,
	operation: IndexOperation,
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum IndexOperation {
	Create {
		namespace: String,
		table_name: String,
		index_name: String,
		column_name: String,
		index_object_id: Option<IndexObjectId>,
	},
	Drop {
		index_object_id: IndexObjectId,
	},
	Status {
		json: bool,
	},
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
			eprintln!("failed to initialize Wendelon index runtime: {error}");
			return ExitCode::from(2);
		}
	};
	match runtime.block_on(run_async(options)) {
		Ok(()) => ExitCode::SUCCESS,
		Err(error) => {
			eprintln!("wendelon index: {error}");
			ExitCode::from(1)
		}
	}
}

async fn run_async(options: IndexOptions) -> std::io::Result<()> {
	let client = connect(&options.connection).await?;
	match options.operation {
		IndexOperation::Create {
			namespace,
			table_name,
			index_name,
			column_name,
			index_object_id,
		} => {
			let table = client
				.get_table_schema_descriptor(options.tenant_id, &namespace, &table_name)
				.await?
				.ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::NotFound,
						format!("table does not exist: {namespace}.{table_name}"),
					)
				})?;
			let index_object_id =
				index_object_id.unwrap_or_else(|| IndexObjectId::new(*Uuid::new_v4().as_bytes()));
			let descriptor = SecondaryIndexDescriptor::for_table_column(
				index_object_id,
				index_name,
				&table,
				&column_name,
			)?;
			let envelope = operator_envelope(
				options.tenant_id,
				&descriptor.namespace,
				&descriptor.index_name,
				SchemaMutationKind::Create,
				encode_secondary_index_descriptor(&descriptor)?,
			);
			client.submit_schema_update_envelope(&envelope).await?;
			println!(
				"created secondary index {}",
				format_index_id(index_object_id)
			);
		}
		IndexOperation::Drop { index_object_id } => {
			let status = client
				.list_secondary_indexes(options.tenant_id)
				.await?
				.into_iter()
				.find(|status| status.descriptor.index_object_id == index_object_id)
				.ok_or_else(|| {
					std::io::Error::new(
						std::io::ErrorKind::NotFound,
						"secondary index does not exist",
					)
				})?;
			let envelope = operator_envelope(
				options.tenant_id,
				&status.descriptor.namespace,
				&status.descriptor.index_name,
				SchemaMutationKind::Drop,
				encode_secondary_index_drop_request(&SecondaryIndexDropRequest {
					index_object_id,
				})?,
			);
			client.submit_schema_update_envelope(&envelope).await?;
			println!(
				"dropping secondary index {}",
				format_index_id(index_object_id)
			);
		}
		IndexOperation::Status { json } => {
			let statuses = client.list_secondary_indexes(options.tenant_id).await?;
			render_statuses(&statuses, json)?;
		}
	}
	Ok(())
}

fn operator_envelope(
	tenant_id: TenantId,
	namespace: &str,
	index_name: &str,
	mutation: SchemaMutationKind,
	payload: Vec<u8>,
) -> SchemaUpdateEnvelope {
	SchemaUpdateEnvelope::wendelon_operator_intent(
		tenant_id,
		SchemaObjectRef {
			kind: SchemaObjectKind::Index,
			namespace: namespace.to_owned(),
			name: index_name.to_owned(),
		},
		SchemaGeneration::ZERO,
		SchemaGeneration::new(1),
		SchemaIntentId::new(*Uuid::new_v4().as_bytes()),
		mutation,
		payload,
	)
}

async fn connect(options: &ConnectionOptions) -> std::io::Result<Arc<WendelonClient>> {
	let material = TlsMaterial::load(&options.cert_path, &options.key_path, &options.ca_path)?;
	let endpoint = Arc::new(endpoint_from_tls(&material, "0.0.0.0:0".parse().unwrap())?);
	let provider: Arc<dyn TransportProvider> =
		Arc::new(QuicTransportProvider::with_cluster_domain(
			endpoint,
			canonical_server_name_resolver(options.cluster_domain.clone()),
			options.cluster_domain.clone(),
		));
	let local_tiers = TransportTierSet::empty().with(TransportTier::QuicIoUring);
	let mut providers = HashMap::new();
	providers.insert(TransportTier::QuicIoUring, provider);
	let role_bits = NodeRoleSet::empty()
		.with(NodeRole::Data)
		.with(NodeRole::Control)
		.as_u8();
	let bootstrap = ClusterStatusWire {
		epoch: 0,
		leader: Some(options.bootstrap_node_id),
		replication_factor: 1,
		peers: vec![ClusterPeerWire {
			node_id: options.bootstrap_node_id,
			address: options.bootstrap_address.clone(),
			tier_bits: local_tiers.as_u8(),
			role_bits,
			capability_bits: 0,
		}],
	};
	let seed = bootstrap.to_membership();
	let (_tx, rx) = watch::channel(Arc::new(seed));
	let resolver: Arc<dyn CapabilityResolver> = Arc::new(EpochCapabilityResolver::new(rx.clone()));
	let topology_factory =
		TransportProviderFactory::new(local_tiers, providers.clone(), resolver, rx);
	let topology = Arc::new(ControlRingTopologySource::new(
		topology_factory,
		BufPool::new(64, 8 * 1024 * 1024),
		vec![NodeId {
			id: options.bootstrap_node_id,
			_reserved: 0,
		}],
	));
	let client = WendelonClient::new(WendelonClientConfig {
		local_tiers,
		providers,
		bootstrap,
		topology,
		pool_max_retained: 64,
		pool_max_cap_bytes: 8 * 1024 * 1024,
	});
	client.refresh_topology().await?;
	Ok(client)
}

fn render_statuses(statuses: &[SecondaryIndexStatus], json: bool) -> std::io::Result<()> {
	if json {
		println!(
			"{}",
			serde_json::to_string_pretty(statuses)
				.map_err(|error| std::io::Error::new(std::io::ErrorKind::InvalidData, error))?
		);
		return Ok(());
	}
	if statuses.is_empty() {
		println!("no secondary indexes");
		return Ok(());
	}
	for status in statuses {
		println!(
			"{}\t{}\t{}.{}\t{}\t{}",
			format_index_id(status.descriptor.index_object_id),
			format!("{:?}", status.state).to_lowercase(),
			status.descriptor.namespace,
			status.descriptor.index_name,
			status.descriptor.key.column_name,
			status
				.replicas
				.iter()
				.map(|replica| format!(
					"{}:{}:gaps={}",
					replica.replica_id.id,
					if replica.initial_backfill_complete {
						"complete"
					} else {
						"building"
					},
					replica.coverage_gap_slabs.len()
				))
				.collect::<Vec<_>>()
				.join(",")
		);
	}
	Ok(())
}

fn format_index_id(id: IndexObjectId) -> String {
	Uuid::from_bytes(id.into_bytes()).hyphenated().to_string()
}

fn parse_index_id(value: &str) -> Result<IndexObjectId, String> {
	Uuid::parse_str(value)
		.map(|id| IndexObjectId::new(*id.as_bytes()))
		.map_err(|error| format!("invalid secondary index id: {error}"))
}

fn parse_args(args: &[String]) -> Result<IndexOptions, String> {
	let operation_name = args
		.first()
		.ok_or_else(|| "wendelon index requires create, drop, or status".to_owned())?;
	let mut bootstrap = None;
	let mut cert_path = None;
	let mut key_path = None;
	let mut ca_path = None;
	let mut cluster_domain = DEFAULT_CLUSTER_DOMAIN.to_owned();
	let mut tenant_id = None;
	let mut namespace = None;
	let mut table_name = None;
	let mut index_name = None;
	let mut column_name = None;
	let mut index_object_id = None;
	let mut json = false;
	let mut cursor = 1usize;
	while let Some(flag) = args.get(cursor) {
		let value = |name: &str| {
			args.get(cursor + 1)
				.cloned()
				.ok_or_else(|| format!("wendelon index {name} requires a value"))
		};
		match flag.as_str() {
			"--bootstrap" => bootstrap = Some(value("--bootstrap")?),
			"--cert" => cert_path = Some(PathBuf::from(value("--cert")?)),
			"--key" => key_path = Some(PathBuf::from(value("--key")?)),
			"--ca" => ca_path = Some(PathBuf::from(value("--ca")?)),
			"--cluster-domain" => cluster_domain = value("--cluster-domain")?,
			"--tenant" => {
				tenant_id = Some(
					value("--tenant")?
						.parse::<u64>()
						.map(TenantId::new)
						.map_err(|error| format!("invalid tenant id: {error}"))?,
				)
			}
			"--namespace" => namespace = Some(value("--namespace")?),
			"--table" => table_name = Some(value("--table")?),
			"--name" => index_name = Some(value("--name")?),
			"--column" => column_name = Some(value("--column")?),
			"--id" => index_object_id = Some(parse_index_id(&value("--id")?)?),
			"--json" => {
				json = true;
				cursor += 1;
				continue;
			}
			unknown => return Err(format!("unknown wendelon index option: {unknown}")),
		}
		cursor += 2;
	}
	let bootstrap = bootstrap.ok_or_else(|| "wendelon index requires --bootstrap".to_owned())?;
	let (node_id, address) = bootstrap
		.split_once('@')
		.ok_or_else(|| "--bootstrap must be <node-id>@<address>".to_owned())?;
	let connection = ConnectionOptions {
		bootstrap_node_id: node_id
			.parse::<u32>()
			.map_err(|error| format!("invalid bootstrap node id: {error}"))?,
		bootstrap_address: address.to_owned(),
		cert_path: cert_path.ok_or_else(|| "wendelon index requires --cert".to_owned())?,
		key_path: key_path.ok_or_else(|| "wendelon index requires --key".to_owned())?,
		ca_path: ca_path.ok_or_else(|| "wendelon index requires --ca".to_owned())?,
		cluster_domain,
	};
	let tenant_id = tenant_id.ok_or_else(|| "wendelon index requires --tenant".to_owned())?;
	let operation = match operation_name.as_str() {
		"create" => IndexOperation::Create {
			namespace: namespace
				.ok_or_else(|| "wendelon index create requires --namespace".to_owned())?,
			table_name: table_name
				.ok_or_else(|| "wendelon index create requires --table".to_owned())?,
			index_name: index_name
				.ok_or_else(|| "wendelon index create requires --name".to_owned())?,
			column_name: column_name
				.ok_or_else(|| "wendelon index create requires --column".to_owned())?,
			index_object_id,
		},
		"drop" => IndexOperation::Drop {
			index_object_id: index_object_id
				.ok_or_else(|| "wendelon index drop requires --id".to_owned())?,
		},
		"status" => IndexOperation::Status { json },
		other => return Err(format!("unknown wendelon index operation: {other}")),
	};
	Ok(IndexOptions {
		connection,
		tenant_id,
		operation,
	})
}

#[cfg(test)]
mod tests {
	use super::*;

	fn common_args(operation: &str) -> Vec<String> {
		[
			operation,
			"--bootstrap",
			"7@127.0.0.1:7001",
			"--cert",
			"operator.pem",
			"--key",
			"operator.key",
			"--ca",
			"ca.pem",
			"--tenant",
			"52",
		]
		.into_iter()
		.map(str::to_owned)
		.collect()
	}

	#[test]
	fn parses_named_create_surface_without_a_hand_built_descriptor() {
		let mut args = common_args("create");
		args.extend(
			[
				"--namespace",
				"public",
				"--table",
				"metrics",
				"--name",
				"metrics_name_idx",
				"--column",
				"name",
			]
			.into_iter()
			.map(str::to_owned),
		);
		let parsed = parse_args(&args).unwrap();
		assert_eq!(parsed.tenant_id, TenantId::new(52));
		assert!(matches!(
			parsed.operation,
			IndexOperation::Create {
				index_object_id: None,
				..
			}
		));
	}

	#[test]
	fn status_json_and_drop_id_are_explicit() {
		let mut status = common_args("status");
		status.push("--json".to_owned());
		assert!(matches!(
			parse_args(&status).unwrap().operation,
			IndexOperation::Status { json: true }
		));

		let mut drop_args = common_args("drop");
		drop_args.extend([
			"--id".to_owned(),
			"12345678-1234-5678-1234-567812345678".to_owned(),
		]);
		assert!(matches!(
			parse_args(&drop_args).unwrap().operation,
			IndexOperation::Drop { .. }
		));
	}
}
