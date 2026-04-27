use arrow::record_batch::RecordBatch;
use arrow::util::display::array_value_to_string;
use std::env;
use std::fs;
use std::io::{Error, ErrorKind};
use std::path::Path;
use wendelon::schema::SlabRecordHeader;
use wendelon::storage::ipc::record_batch_compression_policy_from_ipc;
use wendelon::storage::multirow::{
	decode_batch, has_storage_synthetic_columns, strip_storage_synthetic_columns,
};

const SLAB_RECORD_MAGIC: u32 = 0x534C4142;

fn main() -> std::io::Result<()> {
	let mut args = env::args().skip(1).collect::<Vec<_>>();
	if args.is_empty() || args.iter().any(|arg| arg == "-h" || arg == "--help") {
		print_usage();
		return Ok(());
	}

	let (source, header, frame) = if args.first().map(String::as_str) == Some("--slab") {
		if args.len() != 4 || args.get(2).map(String::as_str) != Some("--offset") {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"expected: dump_slab_record --slab <path> --offset <byte-offset>",
			));
		}
		let path = args.remove(1);
		let offset = args
			.remove(2)
			.parse::<u64>()
			.map_err(|err| Error::new(ErrorKind::InvalidInput, format!("invalid offset: {err}")))?;
		let (header, frame) = read_slab_record(Path::new(&path), offset)?;
		(format!("{path}@{offset}"), Some(header), frame)
	} else {
		if args.len() != 1 {
			return Err(Error::new(
				ErrorKind::InvalidInput,
				"expected one frame path, or --slab <path> --offset <byte-offset>",
			));
		}
		let path = args.remove(0);
		let frame = fs::read(&path)?;
		(path, None, frame)
	};

	dump_frame(&source, header, &frame)
}

fn print_usage() {
	eprintln!("usage:");
	eprintln!("  dump_slab_record <record-body.ipc>");
	eprintln!("  dump_slab_record --slab <slab.wnd> --offset <byte-offset>");
}

fn read_slab_record(path: &Path, offset: u64) -> std::io::Result<(SlabRecordHeader, Vec<u8>)> {
	let bytes = fs::read(path)?;
	let offset = usize::try_from(offset)
		.map_err(|_| Error::new(ErrorKind::InvalidInput, "offset does not fit usize"))?;
	let header_len = std::mem::size_of::<SlabRecordHeader>();
	if bytes.len().saturating_sub(offset) < header_len {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"slab bytes end before the record header",
		));
	}
	let header =
		bytemuck::pod_read_unaligned::<SlabRecordHeader>(&bytes[offset..offset + header_len]);
	if header.magic != SLAB_RECORD_MAGIC {
		return Err(Error::new(
			ErrorKind::InvalidData,
			format!("slab record magic mismatch: 0x{:08x}", header.magic),
		));
	}
	let frame_start = offset
		.checked_add(header_len)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame start overflow"))?;
	let frame_end = frame_start
		.checked_add(header.frame_len as usize)
		.ok_or_else(|| Error::new(ErrorKind::InvalidData, "frame end overflow"))?;
	if frame_end > bytes.len() {
		return Err(Error::new(
			ErrorKind::UnexpectedEof,
			"slab bytes end before the full frame body",
		));
	}
	Ok((header, bytes[frame_start..frame_end].to_vec()))
}

fn dump_frame(source: &str, header: Option<SlabRecordHeader>, frame: &[u8]) -> std::io::Result<()> {
	let codec = record_batch_compression_policy_from_ipc(frame)?;
	let batch = decode_batch(frame)?;

	println!("source: {source}");
	if let Some(header) = header {
		println!("slab_header:");
		println!("  tenant_id: {:?}", header.tenant_id);
		println!("  key_identity: {}", hex(header.key_identity.as_bytes()));
		println!("  hlc: {:?}", header.hlc);
		println!("  frame_len: {}", header.frame_len);
		println!("  checksum: 0x{:08x}", header.checksum);
	}
	println!("record_body:");
	println!("  codec: {}", codec.codec_label());
	println!("  frame_bytes: {}", frame.len());
	println!("  rows: {}", batch.num_rows());
	println!(
		"  storage_synthetic_columns: {}",
		has_storage_synthetic_columns(&batch)
	);
	println!("  schema:");
	for (index, field) in batch.schema().fields().iter().enumerate() {
		println!(
			"    {index}: name={} type={:?} nullable={}",
			field.name(),
			field.data_type(),
			field.is_nullable()
		);
	}
	let user_batch = strip_storage_synthetic_columns(&batch)?;
	if has_storage_synthetic_columns(&batch) {
		println!("  user_visible_schema:");
		for (index, field) in user_batch.schema().fields().iter().enumerate() {
			println!(
				"    {index}: name={} type={:?} nullable={}",
				field.name(),
				field.data_type(),
				field.is_nullable()
			);
		}
	}
	print_user_visible_preview(&user_batch)?;
	Ok(())
}

fn print_user_visible_preview(batch: &RecordBatch) -> std::io::Result<()> {
	const PREVIEW_ROWS: usize = 8;
	println!("  user_visible_rows: {}", batch.num_rows());
	if batch.num_columns() == 0 || batch.num_rows() == 0 {
		return Ok(());
	}

	println!("  user_visible_preview:");
	let header = batch
		.schema()
		.fields()
		.iter()
		.map(|field| field.name().as_str())
		.collect::<Vec<_>>()
		.join(" | ");
	println!("    {header}");
	let rows = batch.num_rows().min(PREVIEW_ROWS);
	for row in 0..rows {
		let mut values = Vec::with_capacity(batch.num_columns());
		for column in batch.columns() {
			let value = array_value_to_string(column.as_ref(), row).map_err(|err| {
				Error::new(
					ErrorKind::InvalidData,
					format!("failed to format Arrow value: {err}"),
				)
			})?;
			values.push(value);
		}
		println!("    {}", values.join(" | "));
	}
	if batch.num_rows() > rows {
		println!("    ... {} more rows", batch.num_rows() - rows);
	}
	Ok(())
}

fn hex(bytes: &[u8]) -> String {
	const HEX: &[u8; 16] = b"0123456789abcdef";
	let mut out = String::with_capacity(bytes.len() * 2);
	for byte in bytes {
		out.push(HEX[(byte >> 4) as usize] as char);
		out.push(HEX[(byte & 0x0f) as usize] as char);
	}
	out
}
