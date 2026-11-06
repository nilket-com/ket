//! A strict reader for the Prometheus text the node renders. A sample line
//! that does not parse makes the whole scrape malformed: no value is guessed.

use std::collections::BTreeMap;

#[derive(Clone, Debug, Default, PartialEq)]
pub struct Scrape {
	/// `name{labels}` exactly as rendered -> value.
	pub samples: BTreeMap<String, f64>,
	/// Family name -> declared `# TYPE`.
	pub types: BTreeMap<String, String>,
}

impl Scrape {
	pub fn parse(text: &str) -> Result<Self, String> {
		let mut scrape = Self::default();
		for (n, line) in text.lines().enumerate() {
			let line = line.trim_end();
			if line.is_empty() {
				continue;
			}
			if let Some(rest) = line.strip_prefix("# TYPE ") {
				let mut it = rest.split_whitespace();
				match (it.next(), it.next()) {
					(Some(name), Some(kind)) => {
						scrape.types.insert(name.to_string(), kind.to_string());
					}
					_ => return Err(format!("line {}: malformed TYPE", n + 1)),
				}
				continue;
			}
			if line.starts_with('#') {
				continue;
			}
			let split = line
				.rfind(' ')
				.ok_or_else(|| format!("line {}: no value", n + 1))?;
			let (key, value) = (&line[..split], line[split + 1..].trim());
			let value = match value {
				"+Inf" => f64::INFINITY,
				"-Inf" => f64::NEG_INFINITY,
				v => v
					.parse::<f64>()
					.map_err(|_| format!("line {}: bad value {v:?}", n + 1))?,
			};
			if key.is_empty() || scrape.samples.insert(key.to_string(), value).is_some() {
				return Err(format!("line {}: empty or duplicate series {key:?}", n + 1));
			}
		}
		Ok(scrape)
	}

	pub fn get(&self, series: &str) -> Option<f64> {
		self.samples.get(series).copied()
	}

	/// The family a series belongs to, with its declared type.
	fn family_type(&self, series: &str) -> Option<&str> {
		let name = series.split('{').next().unwrap_or(series);
		if let Some(t) = self.types.get(name) {
			return Some(t.as_str());
		}
		for suffix in ["_bucket", "_count", "_sum"] {
			if let Some(base) = name.strip_suffix(suffix)
				&& let Some(t) = self.types.get(base)
				&& t == "histogram"
			{
				return Some("histogram");
			}
		}
		None
	}

	/// Series whose value must never decrease within one process: counters
	/// and cumulative histograms. The `wendelon_test_*` instrument is a
	/// current population and block extrema, so it is exempt.
	pub fn is_monotone(&self, series: &str) -> bool {
		if series.starts_with("wendelon_test_") {
			return false;
		}
		matches!(self.family_type(series), Some("counter" | "histogram"))
	}

	/// A required quantity (counter, count, bytes): `None` when absent, an
	/// error unless it is finite, nonnegative, integral and exactly
	/// representable (<= 2^53). Raw text is kept elsewhere; this is the only
	/// way acceptance arithmetic reads a sample.
	pub fn quantity(&self, series: &str) -> Result<Option<u64>, String> {
		match self.get(series) {
			None => Ok(None),
			Some(v) => as_quantity(v)
				.map(Some)
				.map_err(|e| format!("{series}: {e}")),
		}
	}

	/// Cumulative buckets of histogram `name`, validated: every `le` parses
	/// (a finite edge or `+Inf`), edges strictly increase, `+Inf` is last
	/// and present, and the cumulative counts are quantities that never
	/// decrease. Returns (edge, cumulative) pairs, or why they are invalid.
	pub fn buckets(&self, name: &str) -> Result<Vec<(f64, u64)>, String> {
		let prefix = format!("{name}_bucket{{");
		let mut out = Vec::new();
		for (key, value) in self.samples.range(prefix.clone()..) {
			let Some(labels) = key.strip_prefix(&prefix) else {
				break;
			};
			let le = labels
				.strip_prefix("le=\"")
				.and_then(|l| l.strip_suffix("\"}"))
				.ok_or_else(|| format!("{key}: bucket without a sole le label"))?;
			let edge = match le {
				"+Inf" => f64::INFINITY,
				le => match le.parse::<f64>() {
					Ok(e) if e.is_finite() => e,
					_ => return Err(format!("{key}: bad le {le:?}")),
				},
			};
			let count = as_quantity(*value).map_err(|e| format!("{key}: {e}"))?;
			out.push((edge, count));
		}
		out.sort_by(|a, b| a.0.total_cmp(&b.0));
		if out.is_empty() {
			return Ok(out);
		}
		for pair in out.windows(2) {
			if pair[0].0 == pair[1].0 {
				return Err(format!("{name}: duplicate bucket edge {}", pair[0].0));
			}
			if pair[1].1 < pair[0].1 {
				return Err(format!(
					"{name}: cumulative count falls from {} at le={} to {} at le={}",
					pair[0].1, pair[0].0, pair[1].1, pair[1].0
				));
			}
		}
		if !out.last().is_some_and(|b| b.0.is_infinite()) {
			return Err(format!("{name}: no +Inf bucket"));
		}
		Ok(out)
	}
}

/// Finite, nonnegative, integral, and exactly representable in an f64.
pub fn as_quantity(v: f64) -> Result<u64, String> {
	if !v.is_finite() {
		return Err(format!("not finite ({v})"));
	}
	if v < 0.0 {
		return Err(format!("negative ({v})"));
	}
	if v.fract() != 0.0 {
		return Err(format!("not integral ({v})"));
	}
	if v > 9_007_199_254_740_992.0 {
		return Err(format!("beyond exact integer range ({v})"));
	}
	Ok(v as u64)
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn parses_labels_types_and_infinity() {
		let s = Scrape::parse(
			"# HELP a_total x\n# TYPE a_total counter\na_total{reason=\"x y\"} 3\n\
			 # TYPE h histogram\nh_bucket{le=\"1\"} 1\nh_bucket{le=\"+Inf\"} 2\nh_count 2\n",
		)
		.unwrap();
		assert_eq!(s.get("a_total{reason=\"x y\"}"), Some(3.0));
		assert!(s.is_monotone("a_total{reason=\"x y\"}"));
		assert!(s.is_monotone("h_count"));
		assert_eq!(s.buckets("h").unwrap(), vec![(1.0, 1), (f64::INFINITY, 2)]);
	}

	#[test]
	fn quantities_reject_nan_inf_negative_and_fractions() {
		let s = Scrape::parse("a 1\nb NaN\nc +Inf\nd -1\ne 1.5\n").unwrap();
		assert_eq!(s.quantity("a"), Ok(Some(1)));
		assert_eq!(s.quantity("zz"), Ok(None));
		for k in ["b", "c", "d", "e"] {
			assert!(s.quantity(k).is_err(), "{k}");
		}
	}

	#[test]
	fn invalid_histograms_are_errors() {
		let falls = Scrape::parse("h_bucket{le=\"1\"} 5\nh_bucket{le=\"+Inf\"} 4\n").unwrap();
		assert!(falls.buckets("h").is_err());
		let no_inf = Scrape::parse("h_bucket{le=\"1\"} 5\n").unwrap();
		assert!(no_inf.buckets("h").is_err());
		let nan = Scrape::parse("h_bucket{le=\"1\"} NaN\nh_bucket{le=\"+Inf\"} 4\n").unwrap();
		assert!(nan.buckets("h").is_err());
		let bad_le = Scrape::parse("h_bucket{le=\"x\"} 1\nh_bucket{le=\"+Inf\"} 4\n").unwrap();
		assert!(bad_le.buckets("h").is_err());
	}

	#[test]
	fn a_bad_sample_is_malformed_not_skipped() {
		assert!(Scrape::parse("a_total 1\nb_total x\n").is_err());
		assert!(Scrape::parse("a_total 1\na_total 2\n").is_err());
	}
}
