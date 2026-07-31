//! Temporary Toron Arrow-IPC to Polars compatibility quarantine.
//!
//! This module exists to die when ADR 0005's Arrow-native analytics seam
//! lands. Toron is the first adapter that should migrate to that seam; no
//! Toron-specific shape may escape this file into plotting or editor code.

use polars::frame::DataFrame;
use polars::io::SerReader;
use polars::io::ipc::IpcStreamReader;
use std::io::Cursor;

pub(super) fn decode_batches(batches: &[Vec<u8>]) -> Result<DataFrame, String> {
	let mut frames = batches.iter().map(|bytes| {
		IpcStreamReader::new(Cursor::new(bytes.as_slice()))
			.finish()
			.map_err(|error| error.to_string())
	});
	let Some(first) = frames.next() else {
		return Ok(DataFrame::default());
	};
	let mut result = first?;
	for frame in frames {
		result
			.vstack_mut(&frame?)
			.map_err(|error| error.to_string())?;
	}
	Ok(result)
}

#[cfg(test)]
mod tests {
	use super::decode_batches;
	use polars::io::SerWriter;
	use polars::io::ipc::IpcStreamWriter;

	fn encode(mut frame: polars::frame::DataFrame) -> Vec<u8> {
		let mut bytes = Vec::new();
		IpcStreamWriter::new(&mut bytes)
			.finish(&mut frame)
			.expect("encode IPC stream");
		bytes
	}

	#[test]
	fn arrow_ipc_batches_cross_only_the_compatibility_quarantine() {
		let one = polars::df!("x" => [1_i64, 2], "y" => [1_i64, 4]).expect("frame");
		let two = polars::df!("x" => [3_i64], "y" => [9_i64]).expect("frame");
		let decoded = decode_batches(&[encode(one), encode(two)]).expect("decode");
		assert_eq!(decoded.shape(), (3, 2));
		assert_eq!(
			decoded.column("y").expect("y").i64().expect("i64").get(2),
			Some(9)
		);
	}
}
