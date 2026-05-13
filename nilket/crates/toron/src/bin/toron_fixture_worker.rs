use arrow::array::Int32Array;
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;
use std::error::Error;
use std::sync::Arc;
use tokio::io::AsyncWriteExt;

use toron::distributed::identity::WorkerId;
use toron::distributed::partition_store::InMemoryPartitionStore;
use toron::distributed::worker_registry::WorkerEndpoint;
use toron::server::ToronWorker;
use toron::server::memory::MemoryManager;
use toron::storage::InMemoryStorageScan;

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
	let worker_id = worker_id_from_args()?;
	let schema = Arc::new(Schema::new(vec![Field::new("val", DataType::Int32, false)]));
	let batch = RecordBatch::try_new(
		schema,
		vec![Arc::new(Int32Array::from((0..16).collect::<Vec<i32>>()))],
	)?;
	let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await?;
	let endpoint = WorkerEndpoint::new(listener.local_addr()?.to_string());
	let worker = Arc::new(ToronWorker::with_partition_store_and_endpoint(
		Arc::new(InMemoryStorageScan { table_data: batch }),
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
