//! `toron_local` is the `toron-local` feature on a Linux target. Local Toron
//! supervision is Linux-only (ADR 0165) and its dependencies are Linux-only, so
//! elsewhere it compiles out, the feature still enables the remote `toron`
//! client, and the default build works.

fn main() {
	println!("cargo::rerun-if-changed=build.rs");
	println!("cargo::rustc-check-cfg=cfg(toron_local)");
	let feature = std::env::var_os("CARGO_FEATURE_TORON_LOCAL").is_some();
	let linux = std::env::var("CARGO_CFG_TARGET_OS").is_ok_and(|os| os == "linux");
	if feature && linux {
		println!("cargo::rustc-cfg=toron_local");
	}
}
