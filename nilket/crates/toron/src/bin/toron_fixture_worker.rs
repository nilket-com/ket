use arrow::array::{Float64Array, Int32Array, Int64Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::collections::HashMap;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use toron::distributed::identity::WorkerId;
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::worker_registry::WorkerEndpoint;
use toron::error::StorageError;
use toron::ir::TableId;
use toron::server::ToronWorker;
use toron::server::memory::MemoryManager;
use toron::storage::{ScanRequest, ScanStream, StorageScan};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let worker_id = worker_id_from_args()?;
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
	let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
	let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
		Arc::new(FixtureStorageScan::new()?),
		Arc::new(MemoryManager::new(32, 1024 * 1024)),
		Arc::new(InMemoryPartitionStore::new()),
		worker_id,
		endpoint.clone(),
	));

	let mut stdout = tokio::io::stdout();
	stdout
		.write_all(format!("READY {}\n", endpoint.as_str()).as_bytes())
		.await?;
	stdout.flush().await?;

	loop {
		let (stream, _) = listener.accept().await?;
		let worker = worker.clone();
		tokio::spawn(async move {
			if let Err(error) = worker.handle_connection(stream).await {
				eprintln!("fixture worker connection failed: {error}");
			}
		});
	}
}

struct FixtureStorageScan {
	tables: HashMap<TableId, RecordBatch>,
}

impl FixtureStorageScan {
	fn new() -> Result<Self, arrow::error::ArrowError> {
		let vals_schema = Arc::new(Schema::new(vec![
			Field::new("val", DataType::Int32, false),
			Field::new("float_val", DataType::Float64, true),
			Field::new("category", DataType::Utf8, true),
			Field::new("sample", DataType::Float64, true),
		]));
		let orders_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("customer_id", DataType::Int64, false),
		]));
		let customers_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("region", DataType::Utf8, true),
		]));
		let set_left_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("set_key", DataType::Int64, true),
		]));
		let set_right_schema = Arc::new(Schema::new(vec![
			Field::new("id", DataType::Int64, false),
			Field::new("set_key", DataType::Int64, true),
		]));
		let vals = RecordBatch::try_new(
			vals_schema,
			vec![
				Arc::new(Int32Array::from((0..16).collect::<Vec<i32>>())),
				Arc::new(Float64Array::from(
					(0..16)
						.map(|row| Some(row as f64 + 0.25))
						.collect::<Vec<_>>(),
				)),
				Arc::new(StringArray::from(
					(0..16)
						.map(|row| Some(if row < 9 { "fence" } else { "zero" }))
						.collect::<Vec<_>>(),
				)),
				Arc::new(Float64Array::from(vec![
					Some(-6.0),
					Some(-1.0),
					Some(0.0),
					Some(1.0),
					Some(2.0),
					Some(3.0),
					Some(4.0),
					Some(5.0),
					Some(10.0),
					Some(0.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(5.0),
					Some(10.0),
				])),
			],
		)?;
		let orders = RecordBatch::try_new(
			orders_schema,
			vec![
				Arc::new(Int64Array::from(vec![100, 101, 102, 103])),
				Arc::new(Int64Array::from(vec![1, 2, 2, 3])),
			],
		)?;
		// ADR 233 cardinal: both matched customers sit in "west", but their ids hash to different
		// join partitions (cust 2 -> p0, cust 3 -> p3 under XxHash3_64 % 4), so "west"'s rows are
		// produced by two join workers; its SUM/COUNT must merge partials across producers at the shuffle.
		let customers = RecordBatch::try_new(
			customers_schema,
			vec![
				Arc::new(Int64Array::from(vec![2, 3])),
				Arc::new(StringArray::from(vec![Some("west"), Some("west")])),
			],
		)?;
		let set_left = RecordBatch::try_new(
			set_left_schema,
			vec![
				Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
				Arc::new(Int64Array::from(vec![Some(7), None, Some(9), None])),
			],
		)?;
		let set_right = RecordBatch::try_new(
			set_right_schema,
			vec![
				Arc::new(Int64Array::from(vec![10, 11, 12])),
				Arc::new(Int64Array::from(vec![None, Some(9), Some(10)])),
			],
		)?;

		Ok(Self {
			tables: HashMap::from([
				(TableId(1), vals),
				(TableId(2), orders),
				(TableId(3), customers),
				(TableId(4), set_left),
				(TableId(5), set_right),
			]),
		})
	}
}

#[async_trait::async_trait]
impl StorageScan for FixtureStorageScan {
	async fn scan(&self, request: ScanRequest) -> Result<ScanStream, StorageError> {
		let mut batch =
			self.tables
				.get(&request.table_id)
				.cloned()
				.ok_or(StorageError::TableNotFound {
					table: request.table_id,
				})?;
		// ADR 238: honor a pushed predicate in the source-column domain (before projection),
		// reusing the production PBC evaluator — the same `wendelon::schema::pbc::execute` the real
		// in-memory scan uses (storage.rs). Fail loud on an unevaluable program; never return
		// silently-unfiltered rows.
		if let Some(pbc) = &request.pbc_predicate {
			batch = wendelon::schema::pbc::execute(pbc, &batch).map_err(|error| {
				StorageError::InvalidPbcProgram {
					message: format!("{error:?}"),
				}
			})?;
		}
		if let Some(columns) = request.projected_columns {
			let indices = columns
				.into_iter()
				.map(|column| column.0 as usize)
				.collect::<Vec<_>>();
			batch = batch.project(&indices)?;
		}
		if let Some((morsel_id, total_morsels)) = request.morsel {
			let total_rows = batch.num_rows();
			let rows_per_morsel =
				(total_rows + total_morsels as usize - 1) / total_morsels as usize;
			let start = morsel_id.0 as usize * rows_per_morsel;
			let end = std::cmp::min(start + rows_per_morsel, total_rows);
			batch = if start < total_rows {
				batch.slice(start, end - start)
			} else {
				batch.slice(0, 0)
			};
		}
		Ok(ScanStream::from_batches(
			batch.schema(),
			request.session_id,
			request.query_id,
			toron::storage::ScanOrigin::BaseRelation,
			vec![batch],
		))
	}
}

fn worker_id_from_args() -> Result<WorkerId, Box<dyn Error>> {
	let mut args = std::env::args().skip(1);
	let mut worker_u128 = None;
	while let Some(arg) = args.next() {
		if arg == "--worker-u128" {
			worker_u128 = args.next().map(|value| value.parse::<u128>()).transpose()?;
		}
	}
	let worker_u128 = worker_u128.ok_or_else(|| {
		std::io::Error::new(
			std::io::ErrorKind::InvalidInput,
			"missing required --worker-u128 argument",
		)
	})?;
	Ok(WorkerId::from_u128(worker_u128))
}
