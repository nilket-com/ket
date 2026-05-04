use tokio::io::AsyncWriteExt;
use wendelon::network::core::{HRFT_MAGIC, read_frame_header};
use wendelon::schema::{FRAME_FLAG_STREAM_END, FrameHeader, HRFT_ENVELOPE_VERSION, MsgType};

#[tokio::test]
async fn known_mask_accepts_only_stream_end_flag() {
	let cases = [
		(0x0001, false),
		(0x0002, false),
		(0x0003, false),
		(FRAME_FLAG_STREAM_END, true),
		(FRAME_FLAG_STREAM_END | 0x0001, false),
	];

	for (flags, should_accept) in cases {
		let result = read_header_with_flags(flags).await;
		assert_eq!(
			result.is_ok(),
			should_accept,
			"unexpected known-mask result for flags=0x{flags:04x}: {result:?}"
		);
	}
}

async fn read_header_with_flags(flags: u16) -> std::io::Result<FrameHeader> {
	let header = FrameHeader {
		magic: HRFT_MAGIC,
		version: HRFT_ENVELOPE_VERSION,
		msg_type: MsgType::READ_RESP.0,
		flags,
		stream_id: 9,
		meta_len: 0,
		arrow_len: 0,
		trailing_len: 0,
	};
	let (mut tx, mut rx) = tokio::io::duplex(64);
	tx.write_all(bytemuck::bytes_of(&header)).await.unwrap();
	drop(tx);
	read_frame_header(&mut rx).await
}
