use wendelon::crypto::{
	CryptoManager, EncryptedTieredSlab, SLAB_AEAD_TAG_BYTES, SLAB_ENCRYPTION_CHUNK_SIZES,
	TieredOffsetIndexEntry, TieredPlaintextRange, TieredSlabObjectMetadata, test_support,
};
use wendelon::schema::{
	ChunkSizeBytes, IdentityPrefix, RecordBodyByteLen, RecordBodyByteOffset, SlabId, TenantId,
};

const PLAINTEXT_BYTES: usize = 100 * 1024 * 1024;

fn encrypt_fixture(
	crypto: &CryptoManager,
	slab_id: SlabId,
	chunk_size: ChunkSizeBytes,
	plaintext: &[u8],
) -> EncryptedTieredSlab {
	crypto
		.encrypt_tiered_slab_object(
			slab_id,
			&TieredSlabObjectMetadata {
				tenant_id: TenantId::ZERO,
				created_hlc: 61,
				chunk_size_bytes: chunk_size,
				offset_index: vec![TieredOffsetIndexEntry {
					key_locator: IdentityPrefix::ZERO,
					byte_offset: 0,
					byte_length: plaintext.len().try_into().unwrap(),
					flags: 0,
				}],
				column_stats_block: None,
				has_row_tombstone_records: false,
			},
			plaintext,
		)
		.unwrap()
}

#[test]
fn adr061_dense_100mib_closed_set_chunks_coalesce_to_one_ciphertext_range() {
	let plaintext = vec![0x5a; PLAINTEXT_BYTES];
	let crypto = test_support::seeded_dev_crypto_manager(TenantId::ZERO).unwrap();
	let mut tested_chunk_sizes = Vec::new();

	for chunk_size_bytes in SLAB_ENCRYPTION_CHUNK_SIZES {
		let chunk_size = ChunkSizeBytes::new(chunk_size_bytes as u32);
		let chunk_count = PLAINTEXT_BYTES.div_ceil(chunk_size.get() as usize);
		let slab_id = SlabId::new(61_100 + u64::from(chunk_size.get()));
		tested_chunk_sizes.push(chunk_size);

		let encrypted = encrypt_fixture(&crypto, slab_id, chunk_size, &plaintext);
		assert_eq!(encrypted.chunk_size_bytes, chunk_size);
		assert_eq!(encrypted.chunk_count as usize, chunk_count);

		let object_size = encrypted.bytes.len() as u64;
		let tail_start = encrypted.bytes.len().saturating_sub(64 * 1024);
		let mut requested = Vec::new();
		let mut offset = 0_usize;
		while offset < PLAINTEXT_BYTES {
			let len = (8 * 1024 * 1024).min(PLAINTEXT_BYTES - offset);
			requested.push(TieredPlaintextRange::new(
				RecordBodyByteOffset::new(offset as u64),
				RecordBodyByteLen::new(len as u64),
			));
			offset += len;
		}
		let plan = crypto
			.plan_tiered_multi_range_read(
				slab_id,
				object_size,
				&encrypted.bytes[tail_start..],
				None,
				0..PLAINTEXT_BYTES,
				&requested,
			)
			.unwrap();

		let ranges = plan.ciphertext_ranges_in_object();
		assert_eq!(
			ranges.len(),
			1,
			"dense reads must coalesce {} chunks into one GET-sized range at {} bytes/chunk",
			chunk_count,
			chunk_size.get()
		);
		assert_eq!(plan.chunk_runs().len(), 1);
		assert_eq!(plan.chunk_runs()[0].first_chunk().get(), 0);
		assert_eq!(
			plan.chunk_runs()[0].chunk_count().get() as usize,
			chunk_count
		);
		assert_eq!(
			(ranges[0].end - ranges[0].start) as usize,
			PLAINTEXT_BYTES + chunk_count * SLAB_AEAD_TAG_BYTES
		);

		let start = ranges[0].start as usize;
		let end = ranges[0].end as usize;
		let decrypted = plan.decrypt(&[&encrypted.bytes[start..end]]).unwrap();
		assert_eq!(decrypted.concat(), plaintext);
	}

	assert!(tested_chunk_sizes.contains(&ChunkSizeBytes::new(16 * 1024)));
	assert_eq!(tested_chunk_sizes.len(), SLAB_ENCRYPTION_CHUNK_SIZES.len());
}

#[test]
fn adr061_clustered_sparse_100mib_closed_set_chunks_coalesce_per_region() {
	let plaintext = (0..PLAINTEXT_BYTES)
		.map(|index| (index % 251) as u8)
		.collect::<Vec<_>>();
	let crypto = test_support::seeded_dev_crypto_manager(TenantId::ZERO).unwrap();

	for chunk_size_bytes in SLAB_ENCRYPTION_CHUNK_SIZES {
		let chunk_size = ChunkSizeBytes::new(chunk_size_bytes as u32);
		let slab_id = SlabId::new(62_100 + u64::from(chunk_size.get()));
		let encrypted = encrypt_fixture(&crypto, slab_id, chunk_size, &plaintext);
		let object_size = encrypted.bytes.len() as u64;
		let tail_start = encrypted.bytes.len().saturating_sub(64 * 1024);
		let requested = (0..5)
			.map(|region| {
				TieredPlaintextRange::new(
					RecordBodyByteOffset::new((region * 20 * 1024 * 1024) as u64),
					RecordBodyByteLen::new((1024 * 1024) as u64),
				)
			})
			.collect::<Vec<_>>();

		let plan = crypto
			.plan_tiered_multi_range_read(
				slab_id,
				object_size,
				&encrypted.bytes[tail_start..],
				None,
				0..PLAINTEXT_BYTES,
				&requested,
			)
			.unwrap();

		let ranges = plan.ciphertext_ranges_in_object();
		assert_eq!(
			ranges.len(),
			requested.len(),
			"clustered sparse reads should coalesce within each region, not across gaps"
		);
		assert_eq!(plan.chunk_runs().len(), requested.len());
		for (run, request) in plan.chunk_runs().iter().zip(&requested) {
			let first_chunk = request.start.get() / u64::from(chunk_size.get());
			let chunk_count = request.len.get() / u64::from(chunk_size.get());
			assert_eq!(run.first_chunk().get(), first_chunk);
			assert_eq!(u64::from(run.chunk_count().get()), chunk_count);
		}
		for (range, request) in ranges.iter().zip(&requested) {
			let chunk_count = request.len.get() as usize / chunk_size.get() as usize;
			assert_eq!(
				(range.end - range.start) as usize,
				chunk_count * (chunk_size.get() as usize + SLAB_AEAD_TAG_BYTES)
			);
		}

		let chunks = ranges
			.iter()
			.map(|range| &encrypted.bytes[range.start as usize..range.end as usize])
			.collect::<Vec<_>>();
		let decrypted = plan.decrypt(&chunks).unwrap();
		let expected = requested
			.iter()
			.flat_map(|range| {
				let start = range.start.get() as usize;
				let end = start + range.len.get() as usize;
				plaintext[start..end].iter().copied()
			})
			.collect::<Vec<_>>();
		assert_eq!(decrypted.concat(), expected);
	}
}
