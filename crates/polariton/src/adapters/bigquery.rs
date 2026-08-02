use crate::adapters::common::{AdapterField, AdapterFieldType, DatabaseAdapter, ExecutionResult};
use crate::adapters::sql_compound::{CompoundTail, plan};
use async_trait::async_trait;
use gcloud_auth::project::Config as AuthConfig;
use gcloud_auth::token::DefaultTokenSourceProvider;
use gcloud_bigquery::client::Client;
use gcloud_bigquery::http::job::get::GetJobRequest;
use gcloud_bigquery::http::job::get_query_results::GetQueryResultsRequest;
use gcloud_bigquery::http::job::query::QueryRequest;
use gcloud_bigquery::http::job::{JobReference, JobType};
use gcloud_bigquery::http::table::TableReference;
use gcloud_gax::conn::{ConnectionManager, ConnectionOptions, Environment};
use gcloud_googleapis::cloud::bigquery::storage::v1::{
	CreateReadSessionRequest, DataFormat, ReadRowsRequest, ReadSession,
	big_query_read_client::BigQueryReadClient, read_rows_response, read_session,
};
use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::io::ipc::IpcStreamReader;
use sqlparser::dialect::BigQueryDialect;
use std::io::Cursor;

pub const FIELDS: &[AdapterField] = &[AdapterField {
	key: "project_id",
	placeholder: "",
	default_value: None,
	field_type: &AdapterFieldType::Text,
	is_secure: false,
}];

pub struct BigQueryAdapter {
	pub client: Client,
	pub project_id: String,
}

type BoxError = Box<dyn std::error::Error + Send + Sync>;
const DIALECT: BigQueryDialect = BigQueryDialect {};

const STORAGE_AUDIENCE: &str = "https://bigquerystorage.googleapis.com/";
const STORAGE_DOMAIN: &str = "bigquerystorage.googleapis.com";
const STORAGE_SCOPES: &[&str] = &[
	"https://www.googleapis.com/auth/bigquery",
	"https://www.googleapis.com/auth/cloud-platform",
];

#[async_trait]
impl DatabaseAdapter for BigQueryAdapter {
	async fn dispatch(&mut self, code: &str) -> ExecutionResult {
		let planned = match plan(&DIALECT, code) {
			Ok(planned) => planned,
			Err(err) => return ExecutionResult::Err(format!("BigQuery parse error: {}", err)),
		};
		match planned.tail {
			CompoundTail::Empty => ExecutionResult::None,
			CompoundTail::Command(code) => bq_execute(&self.client, &self.project_id, code)
				.await
				.map(|()| {
					if planned.statements > 1 {
						ExecutionResult::Batch {
							statements: planned.statements,
						}
					} else {
						ExecutionResult::CommandCompleted("Command completed.".to_owned())
					}
				})
				.unwrap_or_else(|error| ExecutionResult::Err(error.to_string())),
			CompoundTail::Query { prefix, query } => {
				if planned.statements > 1
					&& let Err(error) = bq_execute(&self.client, &self.project_id, prefix).await
				{
					return ExecutionResult::Err(error.to_string());
				}
				bq_query_to_df(&self.client, &self.project_id, query)
					.await
					.map(|frame| ExecutionResult::Rows {
						frame,
						statements: planned.statements,
					})
					.unwrap_or_else(|error| ExecutionResult::Err(error.to_string()))
			}
		}
	}
}

async fn bq_execute(client: &Client, project_id: &str, sql: &str) -> Result<(), BoxError> {
	let request = QueryRequest {
		query: sql.to_owned(),
		use_legacy_sql: false,
		..Default::default()
	};
	let response = client.job().query(project_id, &request).await?;
	if !response.job_complete {
		poll_until_complete(client, &response.job_reference).await?;
	}
	Ok(())
}

async fn bq_query_to_df(
	client: &Client,
	project_id: &str,
	sql: &str,
) -> Result<DataFrame, BoxError> {
	let request = QueryRequest {
		query: sql.to_string(),
		use_legacy_sql: false,
		..Default::default()
	};
	let initial = client.job().query(project_id, &request).await?;
	let job_ref = initial.job_reference;
	if !initial.job_complete {
		poll_until_complete(client, &job_ref).await?;
	}
	let job = client
		.job()
		.get(
			&job_ref.project_id,
			&job_ref.job_id,
			&GetJobRequest {
				location: job_ref.location.clone(),
			},
		)
		.await?;
	let dest_table = match &job.configuration.job {
		JobType::Query(q) => q
			.destination_table
			.clone()
			.ok_or("No destination table in query job")?,
		_ => return Err("Not a query job".into()),
	};
	storage_read_to_df(project_id, &dest_table).await
}

async fn poll_until_complete(client: &Client, job_ref: &JobReference) -> Result<(), BoxError> {
	let req = GetQueryResultsRequest {
		max_results: Some(0),
		timeout_ms: Some(60_000),
		location: job_ref.location.clone(),
		..Default::default()
	};
	loop {
		let result = client
			.job()
			.get_query_results(&job_ref.project_id, &job_ref.job_id, &req)
			.await?;
		if result.job_complete {
			return Ok(());
		}
	}
}

async fn storage_read_to_df(
	project_id: &str,
	table: &TableReference,
) -> Result<DataFrame, BoxError> {
	let ts = DefaultTokenSourceProvider::new(
		AuthConfig::default()
			.with_audience(STORAGE_AUDIENCE)
			.with_scopes(STORAGE_SCOPES),
	)
	.await?;
	let conn_options = ConnectionOptions::default();
	let conn_mgr = ConnectionManager::new(
		1,
		STORAGE_DOMAIN,
		STORAGE_AUDIENCE,
		&Environment::GoogleCloud(Box::new(ts)),
		&conn_options,
	)
	.await?;
	let mut bq_read = BigQueryReadClient::new(conn_mgr.conn());
	let read_session = bq_read
		.create_read_session(CreateReadSessionRequest {
			parent: format!("projects/{}", project_id),
			read_session: Some(ReadSession {
				table: table.resource(),
				data_format: DataFormat::Arrow as i32,
				..Default::default()
			}),
			max_stream_count: 1,
			..Default::default()
		})
		.await?
		.into_inner();
	let schema_bytes = match read_session.schema {
		Some(read_session::Schema::ArrowSchema(s)) => s.serialized_schema,
		_ => return Err("No Arrow schema in BigQuery read session".into()),
	};
	let mut ipc_bytes = schema_bytes;
	for stream in &read_session.streams {
		let mut rows = bq_read
			.read_rows(ReadRowsRequest {
				read_stream: stream.name.clone(),
				offset: 0,
			})
			.await?
			.into_inner();
		while let Some(response) = rows.message().await? {
			if let Some(read_rows_response::Rows::ArrowRecordBatch(batch)) = response.rows {
				ipc_bytes.extend_from_slice(&batch.serialized_record_batch);
			}
		}
	}
	Ok(IpcStreamReader::new(Cursor::new(ipc_bytes)).finish()?)
}
