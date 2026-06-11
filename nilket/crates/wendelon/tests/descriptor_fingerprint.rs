use arrow::datatypes::{DataType, Field, Schema};
use wendelon::schema::{
	LocalityPolicy, PrimaryKeyDescriptorV1, RecordBodyCompressionPolicy, RowIdentityPolicy,
	SchemaEvolutionPolicyV1, TableObjectId, TableSchemaDescriptor,
};

fn descriptor(id_byte: u8, table_name: &str) -> TableSchemaDescriptor {
	let schema = Schema::new(vec![
		Field::new("id", DataType::Int64, false),
		Field::new("region", DataType::LargeUtf8, true),
	]);
	TableSchemaDescriptor::new(
		TableObjectId::new([id_byte; 16]),
		1,
		"tests",
		table_name,
		&schema,
		RowIdentityPolicy::PrimaryKey(PrimaryKeyDescriptorV1::single("id")),
		LocalityPolicy::default_table_spread(),
		SchemaEvolutionPolicyV1::AdditiveOnly,
	)
	.unwrap()
}

fn hex(bytes: &[u8; 32]) -> String {
	let mut out = String::with_capacity(64);
	for byte in bytes {
		out.push_str(&format!("{byte:02x}"));
	}
	out
}

// These goldens are the wire-stability tripwire for the descriptor fingerprint
// preimages. The preimage components are postcard-encoded, and postcard is
// POSITIONAL: appending a field to a preimage-feeding type (even
// `#[serde(default)]`, even left empty) re-encodes every value and shifts the
// fingerprint of EVERY descriptor — `#[serde(default)]` only relaxes decode of
// self-describing formats, it does not make the encoding stable. The rule:
// a cut that changes any preimage input type must EITHER re-pin the physical
// golden here in the same cut (accepting that previously persisted
// fingerprints no longer validate), OR bump `PHYSICAL_DESCRIPTOR_DOMAIN` to a
// new version when old durable descriptors must stay valid. A silent break is
// never acceptable — that is exactly what this test exists to catch.
#[test]
fn golden_vectors_are_pinned() {
	let descriptor = descriptor(0x51, "descriptor_fingerprint");
	assert_eq!(
		hex(descriptor.logical_schema_fingerprint.as_bytes()),
		"f74744e3383e722e352f6f76536f357ee33bc99b2af9ae2355228de24d1c712c"
	);
	// Re-pinned after ADRs 156/159/164/165 appended four `ColumnStatsPolicyV1`
	// fields (heavy_hitters / range_distributions / column_group_top_k /
	// column_group_ndv) without re-pinning: the stats-policy postcard encoding
	// grew from three empty vecs to seven, shifting the physical fingerprint of
	// every descriptor. Verified by reproducing the OLD golden (824e1e09…913b)
	// byte-for-byte from the pre-ADR-156 three-vec encoding — the drift is
	// exactly those fields, nothing else.
	assert_eq!(
		hex(descriptor.physical_descriptor_fingerprint.as_bytes()),
		"07ca6eea0ec4b5fe95f1abbb07835a44b5f3a915e12ebdb29d5f8fadbb11e78b"
	);
}

#[test]
fn physical_retune_keeps_logical_and_changes_physical() {
	let descriptor = descriptor(0x52, "physical_retune");
	let mut retuned = descriptor.clone();
	retuned.record_body_compression_policy = RecordBodyCompressionPolicy::Lz4Frame;
	retuned.refresh_descriptor_fingerprints().unwrap();

	assert_eq!(
		retuned.logical_schema_fingerprint,
		descriptor.logical_schema_fingerprint
	);
	assert_ne!(
		retuned.physical_descriptor_fingerprint,
		descriptor.physical_descriptor_fingerprint
	);
}

#[test]
fn generation_and_contract_are_outside_both_preimages() {
	let descriptor = descriptor(0x53, "generation_contract");
	let mut relabeled = descriptor.clone();
	relabeled.schema_generation += 1;
	relabeled.contract_version += 1;
	relabeled.refresh_descriptor_fingerprints().unwrap();

	assert_eq!(
		relabeled.logical_schema_fingerprint,
		descriptor.logical_schema_fingerprint
	);
	assert_eq!(
		relabeled.physical_descriptor_fingerprint,
		descriptor.physical_descriptor_fingerprint
	);
}

#[test]
fn table_identity_affects_logical_and_stacked_physical() {
	let left = descriptor(0x54, "identity_left");
	let right = descriptor(0x55, "identity_left");
	let renamed = descriptor(0x54, "identity_right");

	assert_ne!(
		left.logical_schema_fingerprint,
		right.logical_schema_fingerprint
	);
	assert_ne!(
		left.physical_descriptor_fingerprint,
		right.physical_descriptor_fingerprint
	);
	assert_ne!(
		left.logical_schema_fingerprint,
		renamed.logical_schema_fingerprint
	);
	assert_ne!(
		left.physical_descriptor_fingerprint,
		renamed.physical_descriptor_fingerprint
	);
}
