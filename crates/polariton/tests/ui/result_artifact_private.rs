#![allow(dead_code)] // standalone fixture: compiles a partial module by design
#[path = "../../src/result_artifact.rs"]
mod result_artifact;

use result_artifact::ArrowBacking;

fn main() {
	let _: Option<ArrowBacking> = None;
}
