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
