use super::super::errors::{ManifestRole3, ReuseField3, RolePair3};
use super::*;
fn vectors() -> serde_json::Value {
	serde_json::from_str(include_str!("profile_three_vectors.json")).unwrap()
}
fn role(s: &str) -> ManifestRole3 {
	match s {
		"Admin" => ManifestRole3::Admin,
		"Governance" => ManifestRole3::Governance,
		"Recovery" => ManifestRole3::Recovery,
		_ => panic!("role"),
	}
}
fn error(s: &str) -> AdminManifestError2 {
	use AdminManifestError2::*;
	let parts: Vec<_> = s.split(':').collect();
	match parts.as_slice() {
		["OverReadBound"] => OverReadBound,
		["Length"] => Length,
		["Magic"] => Magic,
		["UnsupportedFormat"] => UnsupportedFormat,
		["UnsupportedProfile"] => UnsupportedProfile,
		["ProfileLength"] => ProfileLength,
		["ZeroField"] => ZeroField,
		["Namespace"] => Namespace,
		["Action", r] => ActionThree(role(r)),
		["Key", r] => KeyThree(role(r)),
		["Reuse", p, f] => ReuseThree {
			pair: match *p {
				"AdminGovernance" => RolePair3::AdminGovernance,
				"AdminRecovery" => RolePair3::AdminRecovery,
				"GovernanceRecovery" => RolePair3::GovernanceRecovery,
				_ => panic!("pair"),
			},
			field: match *f {
				"Id" => ReuseField3::Id,
				"Key" => ReuseField3::Key,
				_ => panic!("field"),
			},
		},
		_ => panic!("unknown error {s}"),
	}
}
#[test]
fn independent_profile_three_bytes_pin_and_parts() {
	let v = vectors();
	for row in v["valid"].as_array().unwrap() {
		let b = hex(row["bytes"].as_str().unwrap());
		let m = AdminManifestThree3::decode(&b).unwrap();
		assert_eq!(m.encode().unwrap().as_slice(), b);
		let any = AnyAdminManifest2::decode(&b).unwrap();
		assert_eq!(any.profile(), ManifestProfile2::Three);
		let canonical = any.encode().unwrap();
		assert_eq!(canonical.as_slice(), b);
		assert_eq!(
			any_manifest_digest2(&canonical).as_slice(),
			hex(row["pin"].as_str().unwrap())
		);
		assert_eq!(m.recovery().actions(), 31);
		assert_eq!(
			m.recovery().id().as_slice(),
			hex(v["recovery_id"].as_str().unwrap())
		);
		assert_eq!(
			m.recovery().key().as_slice(),
			hex(v["recovery_key"].as_str().unwrap())
		);
		let built = AdminManifestThree3::new(
			m.base().clone(),
			m.governance(),
			RecoveryRole3::new(m.recovery().id(), m.recovery().key()).unwrap(),
		)
		.unwrap();
		assert!(built == m);
	}
}
#[test]
fn profile_three_all_independent_refusals_and_precedence() {
	let v = vectors();
	let errors = v["errors"].as_array().unwrap();
	assert_eq!(errors.len(), 64);
	for row in errors {
		let b = hex(row["bytes"].as_str().unwrap());
		let before = b.clone();
		assert_eq!(
			AdminManifestThree3::decode(&b).err(),
			Some(error(row["error"].as_str().unwrap())),
			"{}",
			row["name"]
		);
		assert_eq!(b, before);
	}
}
#[test]
fn old_profile_decoders_do_not_accept_recovery_or_change_old_unknown_profile_errors() {
	let v = vectors();
	let b = hex(v["valid"][0]["bytes"].as_str().unwrap());
	assert_eq!(
		AdminManifest2::decode(&b).err(),
		Some(AdminManifestError2::Length)
	);
	assert_eq!(
		AdminManifestTwo2::decode(&b).err(),
		Some(AdminManifestError2::Length)
	);
	let old3 = &b[..189];
	assert_eq!(
		AdminManifestTwo2::decode(old3).err(),
		Some(AdminManifestError2::UnsupportedProfile)
	);
	assert_eq!(
		manifest_profile2(old3),
		Err(AdminManifestError2::UnsupportedProfile)
	);
	assert_eq!(
		AnyAdminManifest2::decode(old3).err(),
		Some(AdminManifestError2::ProfileLength)
	);
}
/// T-INLINE: the profile-3 encoder writes the base layout itself. The independent pins are
/// unchanged, every valid vector round-trips, and the 64 refusal rows keep their errors (above).
#[test]
fn profile_three_encoder_writes_the_base_layout_directly() {
	let v = vectors();
	let pins: Vec<(&str, &str)> = v["valid"]
		.as_array()
		.unwrap()
		.iter()
		.map(|r| (r["name"].as_str().unwrap(), r["pin"].as_str().unwrap()))
		.collect();
	assert_eq!(
		pins,
		[
			(
				"raw",
				"5e20fe4a0cbe7864be914954ca04f96f1f93f8265baae1a56f8b56403ce83db4"
			),
			(
				"table",
				"dc3bafdeed525e80326d7439b66ef46b317e785f12b5d1ac875fb474651e7f6c"
			),
		]
	);
	assert_eq!(v["errors"].as_array().unwrap().len(), 64);
	for row in v["valid"].as_array().unwrap() {
		let b = hex(row["bytes"].as_str().unwrap());
		let m = AdminManifestThree3::decode(&b).unwrap();
		let encoded = m.encode().unwrap();
		assert_eq!(encoded.as_slice(), b);
		assert_eq!(
			any_manifest_digest2(&AnyAdminManifest2::Three(m.clone()).encode().unwrap()).as_slice(),
			hex(row["pin"].as_str().unwrap())
		);
		// Bridge, deleted with the profile-1 codec in c2: the inline head equals the profile-1
		// encoding of the same base with only the profile word changed.
		let mut head = m.base().encode().unwrap();
		head[10..12].copy_from_slice(&3u16.to_le_bytes());
		assert_eq!(encoded[..137], head);
	}
}
