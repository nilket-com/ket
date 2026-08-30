#![allow(dead_code)] // standalone fixture: compiles a partial module by design
mod plot {
	pub(crate) mod exact_numeric {
		use polars::prelude::AnyValue;

		pub(crate) enum ExactF64Error {
			Null,
			Inexact,
		}

		pub(crate) fn exact_f64(_value: AnyValue<'_>) -> Result<f64, ExactF64Error> {
			unimplemented!()
		}
	}
}

mod result_artifact {
	#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
	pub struct ArtifactToken(u64);

	impl ArtifactToken {
		pub(crate) const fn fixture() -> Self {
			Self(1)
		}
	}
}

#[path = "../../src/plot/kernels/hexbin_viewport.rs"]
mod hexbin_viewport;

use hexbin_viewport::prepare_hexbin_observations;
use polars::df;
use result_artifact::ArtifactToken;

fn main() {
	if false {
		let frame = df!("x" => [1.0_f64], "y" => [2.0_f64]).unwrap();
		let observations =
			prepare_hexbin_observations(&frame, "x", "y", ArtifactToken::fixture()).unwrap();
		assert_eq!(observations.source_count(), 1);
	}
}
