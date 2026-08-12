#[path = "../../src/result_artifact.rs"]
mod result_artifact;

use arrow_array::{ArrayRef, Int32Array, RecordBatch};
use arrow_ipc::writer::StreamWriter;
use arrow_schema::{DataType, Field, Schema};
use result_artifact::{ArrowNullabilityPolicy, ResultArtifact};
use std::sync::Arc;

fn main() {
	let schema = Arc::new(Schema::new(vec![Field::new("x", DataType::Int32, false)]));
	let batch = RecordBatch::try_new(
		Arc::clone(&schema),
		vec![Arc::new(Int32Array::from(vec![1, 2])) as ArrayRef],
	)
	.unwrap();
	let mut bytes = Vec::new();
	{
		let mut writer = StreamWriter::try_new(&mut bytes, schema.as_ref()).unwrap();
		writer.write(&batch).unwrap();
		writer.finish().unwrap();
	}
	let artifact =
		ResultArtifact::from_arrow_ipc(vec![bytes], ArrowNullabilityPolicy::Preserve).unwrap();
	assert_eq!(artifact.shape(), (2, 1));
}
