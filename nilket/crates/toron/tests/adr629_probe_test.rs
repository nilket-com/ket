//! ADR 629 A2: public-client adapter only. No listener, scheduler or storage changes.
//! Intent durability is owned by the external ledger before this probe is invoked.
use std::io::Write;
use std::path::PathBuf;
use std::time::Instant;

use arrow::array::{Array, BooleanArray, Int32Array, Int64Array, StringArray, UInt64Array};
use arrow::datatypes::DataType;
use arrow::ipc::reader::StreamReader;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use toron_client::{
	MutationCommitKnowledge, MutationStopReason, QueryId, ResolvedOperationOutcome, ToronClient,
	ToronClientTls,
};
use uuid::Uuid;

#[derive(Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct ProbeSpec {
	endpoint: String,
	server_name: String,
	cert: PathBuf,
	key: PathBuf,
	ca: PathBuf,
	query_id: Uuid,
	operation: Operation,
	evidence: PathBuf,
}

#[derive(Deserialize, Serialize)]
#[serde(tag = "kind", deny_unknown_fields)]
enum Operation {
	#[serde(rename = "execute")]
	Execute { sql: String, timezone: String },
	#[serde(rename = "resolve")]
	Resolve,
	#[serde(rename = "health")]
	Health,
}

fn rows(bytes: &[Vec<u8>]) -> Result<Vec<Value>, String> {
	let mut result = Vec::new();
	for raw in bytes {
		let reader = StreamReader::try_new(raw.as_slice(), None).map_err(|e| e.to_string())?;
		for batch in reader {
			let batch = batch.map_err(|e| e.to_string())?;
			let schema = batch.schema();
			let mut seen = std::collections::HashSet::new();
			for field in schema.fields() {
				if !seen.insert(field.name()) {
					return Err("duplicate response column".into());
				}
			}
			for index in 0..batch.num_rows() {
				let mut row = serde_json::Map::new();
				for (field, array) in schema.fields().iter().zip(batch.columns()) {
					let value = if array.is_null(index) {
						Value::Null
					} else {
						match field.data_type() {
							DataType::Int64 => json!(
								array
									.as_any()
									.downcast_ref::<Int64Array>()
									.unwrap()
									.value(index)
							),
							DataType::Int32 => json!(
								array
									.as_any()
									.downcast_ref::<Int32Array>()
									.unwrap()
									.value(index)
							),
							DataType::UInt64 => json!(
								array
									.as_any()
									.downcast_ref::<UInt64Array>()
									.unwrap()
									.value(index)
							),
							DataType::Utf8 => json!(
								array
									.as_any()
									.downcast_ref::<StringArray>()
									.unwrap()
									.value(index)
							),
							DataType::Boolean => json!(
								array
									.as_any()
									.downcast_ref::<BooleanArray>()
									.unwrap()
									.value(index)
							),
							other => return Err(format!("unreviewed response scalar: {other:?}")),
						}
					};
					row.insert(field.name().clone(), value);
				}
				result.push(Value::Object(row));
			}
		}
	}
	Ok(result)
}

fn stop_raw(value: MutationStopReason) -> u8 {
	match value {
		MutationStopReason::None => 0,
		MutationStopReason::Deadline => 1,
		MutationStopReason::NoProgress => 2,
		MutationStopReason::AttemptsExhausted => 3,
		MutationStopReason::Other(x) => x,
	}
}

fn knowledge_raw(value: MutationCommitKnowledge) -> u8 {
	match value {
		MutationCommitKnowledge::Unspecified => 0,
		MutationCommitKnowledge::Committed => 1,
		MutationCommitKnowledge::KnownNotCommitted => 2,
		MutationCommitKnowledge::Indeterminate => 3,
		MutationCommitKnowledge::Other(x) => x,
	}
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
#[ignore = "ADR 629 A2 adapter; requires reviewed spec and authenticated endpoint"]
async fn adr629_public_client_probe() -> Result<(), Box<dyn std::error::Error>> {
	run_spec(&std::fs::read(std::env::var("TORON_ADR629_SPEC")?)?).await
}

pub(crate) async fn run_spec(bytes: &[u8]) -> Result<(), Box<dyn std::error::Error>> {
	let spec: ProbeSpec = serde_json::from_slice(bytes)?;
	if spec.query_id.get_version_num() != 7 {
		return Err("ledger must supply a v7 QueryId".into());
	}
	let mut evidence = std::fs::OpenOptions::new()
		.write(true)
		.create_new(true)
		.open(&spec.evidence)?;
	let started = Instant::now();
	let tls = ToronClientTls {
		server_name: spec.server_name.clone(),
		cert_path: spec.cert.clone(),
		key_path: spec.key.clone(),
		ca_path: spec.ca.clone(),
	};
	let result = match ToronClient::authenticated(&spec.endpoint, &tls) {
		Err(e) => json!({"kind":"configuration_error", "error":e.to_string()}),
		Ok(client) => {
			let id = QueryId::from_uuid(spec.query_id);
			match &spec.operation {
				Operation::Execute { sql, timezone } => {
					match client.execute_with_query_id(id, sql, timezone).await {
						Ok(r) => {
							let raw_hex: Vec<String> = r
								.batches
								.iter()
								.map(|b| b.iter().map(|x| format!("{x:02x}")).collect())
								.collect();
							match rows(&r.batches) {
								Ok(decoded) => {
									json!({"kind":"success", "row_count":r.row_count, "rows":decoded, "batches_hex":raw_hex})
								}
								Err(e) => {
									json!({"kind":"decode_error", "row_count":r.row_count, "error":e, "batches_hex":raw_hex})
								}
							}
						}
						Err(e) => {
							json!({"kind":"error", "error":e.to_string(), "typed_error":format!("{e:?}")})
						}
					}
				}
				Operation::Resolve => match client.resolve_mutation(id).await {
					Ok(ResolvedOperationOutcome::Mutation(r)) => {
						json!({"kind":"resolution", "operation_query_id":r.operation_query_id.get().to_string(),
						"resolution":format!("{:?}",r.resolution), "write_status":r.write_status,
						"stop_reason":stop_raw(r.stop_reason), "commit_knowledge":knowledge_raw(r.commit_knowledge),
						"delete_counts":r.delete_counts})
					}
					Ok(ResolvedOperationOutcome::Ctas(_)) => {
						json!({"kind":"protocol_error", "error":"CTAS resolution outside frozen store workload"})
					}
					Err(e) => {
						json!({"kind":"error", "error":e.to_string(), "typed_error":format!("{e:?}")})
					}
				},
				Operation::Health => match client.health().await {
					Ok(()) => json!({"kind":"healthy"}),
					Err(e) => {
						json!({"kind":"error", "error":e.to_string(), "typed_error":format!("{e:?}")})
					}
				},
			}
		}
	};
	let record =
		json!({"version":1,"spec":spec,"elapsed_us":started.elapsed().as_micros(),"result":result});
	serde_json::to_writer(&mut evidence, &record)?;
	evidence.write_all(b"\n")?;
	evidence.sync_all()?;
	// rc proves libtest execution, not SQL success. The reader evaluates recorded results.
	Ok(())
}

#[test]
fn adr629_response_scalar_encoding_is_exact_and_refuses_unreviewed_types() {
	use arrow::array::{ArrayRef, Float64Array};
	use arrow::datatypes::{Field, Schema};
	use arrow::ipc::writer::StreamWriter;
	use arrow::record_batch::RecordBatch;
	use std::sync::Arc;
	let encode = |a: ArrayRef| {
		let schema = Arc::new(Schema::new(vec![Field::new(
			"id",
			a.data_type().clone(),
			true,
		)]));
		let b = RecordBatch::try_new(schema.clone(), vec![a]).unwrap();
		let mut bytes = Vec::new();
		{
			let mut w = StreamWriter::try_new(&mut bytes, &schema).unwrap();
			w.write(&b).unwrap();
			w.finish().unwrap();
		}
		bytes
	};
	assert_eq!(
		rows(&[encode(Arc::new(Int64Array::from(vec![
			Some(i64::MAX),
			None
		])))])
		.unwrap(),
		vec![json!({"id":i64::MAX}), json!({"id":null})]
	);
	assert!(
		rows(&[encode(Arc::new(Float64Array::from(vec![1.0])))])
			.unwrap_err()
			.contains("unreviewed")
	);
	assert!(rows(&[vec![0, 1, 2]]).is_err());
}

#[test]
#[ignore = "ADR 629 development-only config parsing; opens no listeners"]
fn adr629_real_config_loaders_report_the_topology_precondition() {
	let root = PathBuf::from(
		std::env::var("TORON_ADR629_CONFIGS").expect("isolated config test directory"),
	);
	for host in ["bigfoot", "strix", "optiplex"] {
		let path = root.join(format!("{host}.toron.toml"));
		let error = toron::config::load_toron_bootstrap_config(&path)
			.expect_err("current loader refuses direct-LAN client bind");
		assert!(
			error
				.to_string()
				.contains("storage.wendelon.client_bind must remain loopback"),
			"{error}"
		);
		let control = root.join(format!("{host}.toron.loopback-control.toml"));
		std::fs::write(
			&control,
			std::fs::read_to_string(path).unwrap().replace(
				"client_bind = \"0.0.0.0:0\"",
				"client_bind = \"127.0.0.1:0\"",
			),
		)
		.unwrap();
		let t = toron::config::load_toron_bootstrap_config(&control)
			.expect("loopback bind parsing control");
		assert_eq!(t.coordinator.static_workers.len(), 2);
		assert!(t.storage.wendelon.tls.is_some());
		let w = wendelon::config::load_node_bootstrap_config(
			root.join(format!("{host}.wendelon.toml")),
		)
		.expect("Wendelon config loader");
		assert_eq!(w.rf, 3);
		println!(
			"ADR629_CONFIG host={host} toron_lan_bind_rejected=true loopback_control_parsed=true wendelon_parsed=true requested_rf=3 startup_not_exercised=true placement_observed=false"
		);
	}
}
