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

use hexbin_viewport::HexbinObservations;
use result_artifact::ArtifactToken;

fn main() {
	let _ = HexbinObservations {
		source_token: ArtifactToken::fixture(),
		x_field: "x".to_owned(),
		y_field: "y".to_owned(),
		rows: Vec::new(),
		x_range: (0.0, 1.0),
		y_range: (0.0, 1.0),
	};
}
