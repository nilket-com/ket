use std::fs;

fn main() {
	println!("cargo:rerun-if-changed=../../Cargo.lock");
	let lock = fs::read_to_string("../../Cargo.lock").expect("failed to read Cargo.lock");
	let version = find_package_version(&lock, "aws-lc-rs")
		.expect("aws-lc-rs missing from Cargo.lock; required for FIPS module version reporting");
	println!("cargo:rustc-env=WENDELON_AWS_LC_RS_VERSION={version}");
}

fn find_package_version(lock: &str, package: &str) -> Option<String> {
	let mut matched_package = false;
	for line in lock.lines() {
		let trimmed = line.trim();
		if trimmed == "[[package]]" {
			matched_package = false;
			continue;
		}
		if let Some(name) = trimmed.strip_prefix("name = ") {
			matched_package = parse_lock_string(name) == package;
			continue;
		}
		if matched_package && let Some(version) = trimmed.strip_prefix("version = ") {
			return Some(parse_lock_string(version));
		}
	}
	None
}

fn parse_lock_string(value: &str) -> String {
	value.trim().trim_matches('"').to_string()
}
