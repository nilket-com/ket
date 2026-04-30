use std::hint::black_box;
use std::time::{Duration, Instant};

use wendelon::crypto::{
	CryptoManager, EncryptedTieredSlab, SLAB_ENCRYPTION_CHUNK_BYTES, TieredOffsetIndexEntry,
	TieredSlabObjectMetadata, test_support,
};
use wendelon::schema::{IdentityPrefix, SlabId, TenantId};

struct BenchCase {
	label: &'static str,
	slab_id: SlabId,
	bytes: usize,
	iterations: usize,
}

struct BenchMeasurement {
	mode: &'static str,
	elapsed: Duration,
	iterations: usize,
}

fn encrypt_bench_tiered_slab(
	crypto: &CryptoManager,
	slab_id: SlabId,
	plaintext: &[u8],
) -> std::io::Result<EncryptedTieredSlab> {
	crypto.encrypt_tiered_slab_object(
		slab_id,
		&TieredSlabObjectMetadata {
			tenant_id: TenantId::ZERO,
			created_hlc: 0,
			offset_index: vec![TieredOffsetIndexEntry {
				key_locator: IdentityPrefix::ZERO,
				byte_offset: 0,
				byte_length: u32::try_from(plaintext.len()).map_err(|_| {
					std::io::Error::new(
						std::io::ErrorKind::InvalidInput,
						"benchmark slab too large",
					)
				})?,
				flags: 0,
			}],
			column_stats_block: None,
			has_row_tombstone_records: false,
		},
		plaintext,
	)
}

fn main() -> Result<(), Box<dyn std::error::Error>> {
	let crypto = test_support::seeded_dev_crypto_manager(TenantId::ZERO)?;

	let cases = [
		BenchCase {
			label: "serial-64-chunks",
			slab_id: SlabId::new(10_001),
			bytes: 64 * SLAB_ENCRYPTION_CHUNK_BYTES,
			iterations: 64,
		},
		BenchCase {
			label: "threshold-128-chunks",
			slab_id: SlabId::new(10_002),
			bytes: 128 * SLAB_ENCRYPTION_CHUNK_BYTES,
			iterations: 32,
		},
		BenchCase {
			label: "parallel-512-chunks",
			slab_id: SlabId::new(10_003),
			bytes: 512 * SLAB_ENCRYPTION_CHUNK_BYTES,
			iterations: 8,
		},
	];

	let available_parallelism = std::thread::available_parallelism()
		.map(|parallelism| parallelism.get())
		.unwrap_or(1);
	println!(
		"slab_crypto chunk_bytes={} available_parallelism={available_parallelism}",
		SLAB_ENCRYPTION_CHUNK_BYTES,
	);

	for case in cases {
		let plaintext = patterned_payload(case.bytes);
		let encrypt = bench_encrypt(&crypto, &case, &plaintext)?;
		let decrypt = bench_decrypt_warm_cache(&crypto, &case, &plaintext)?;
		print_measurement(&case, &encrypt);
		print_measurement(&case, &decrypt);
	}

	Ok(())
}

fn bench_encrypt(
	crypto: &CryptoManager,
	case: &BenchCase,
	plaintext: &[u8],
) -> std::io::Result<BenchMeasurement> {
	let mut elapsed = Duration::ZERO;
	for _ in 0..case.iterations {
		let started = Instant::now();
		let encrypted = encrypt_bench_tiered_slab(crypto, case.slab_id, black_box(plaintext))?;
		elapsed += started.elapsed();
		black_box(encrypted.bytes.len());
	}
	Ok(BenchMeasurement {
		mode: "encrypt",
		elapsed,
		iterations: case.iterations,
	})
}

fn bench_decrypt_warm_cache(
	crypto: &CryptoManager,
	case: &BenchCase,
	plaintext: &[u8],
) -> std::io::Result<BenchMeasurement> {
	let encrypted = encrypt_bench_tiered_slab(crypto, case.slab_id, plaintext)?;
	let warmed = crypto.decrypt_tiered_slab(case.slab_id, &encrypted.bytes)?;
	if warmed.plaintext != plaintext {
		return Err(std::io::Error::other(
			"warm decrypt validation failed before slab crypto benchmark",
		));
	}

	let mut elapsed = Duration::ZERO;
	for _ in 0..case.iterations {
		let started = Instant::now();
		let decrypted = crypto.decrypt_tiered_slab(case.slab_id, black_box(&encrypted.bytes))?;
		elapsed += started.elapsed();
		black_box(decrypted.plaintext.len());
	}
	Ok(BenchMeasurement {
		mode: "decrypt-warm-cache",
		elapsed,
		iterations: case.iterations,
	})
}

fn print_measurement(case: &BenchCase, measurement: &BenchMeasurement) {
	let chunk_count = case.bytes.div_ceil(SLAB_ENCRYPTION_CHUNK_BYTES);
	let avg_ns = measurement.elapsed.as_nanos() / measurement.iterations as u128;
	let throughput_mib_per_s = (case.bytes * measurement.iterations) as f64
		/ (1024.0 * 1024.0)
		/ measurement.elapsed.as_secs_f64();
	println!(
		"slab_crypto case={} mode={} slab_bytes={} chunk_count={} iterations={} avg_ns={} throughput_mib_per_s={throughput_mib_per_s:.2}",
		case.label, measurement.mode, case.bytes, chunk_count, measurement.iterations, avg_ns,
	);
}

fn patterned_payload(len: usize) -> Vec<u8> {
	let mut payload = vec![0_u8; len];
	for (idx, byte) in payload.iter_mut().enumerate() {
		*byte = (idx as u8).wrapping_mul(31).wrapping_add(17);
	}
	payload
}
