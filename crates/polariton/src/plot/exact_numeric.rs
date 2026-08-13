use polars::prelude::AnyValue;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(crate) enum ExactF64Error {
	Null,
	Inexact,
}

pub(crate) fn exact_f64(value: AnyValue<'_>) -> Result<f64, ExactF64Error> {
	let converted = match value {
		AnyValue::Int8(value) => exact_signed(value as i128),
		AnyValue::Int16(value) => exact_signed(value as i128),
		AnyValue::Int32(value) => exact_signed(value as i128),
		AnyValue::Int64(value) => exact_signed(value as i128),
		AnyValue::Int128(value) => exact_signed(value),
		AnyValue::UInt8(value) => exact_unsigned(value as u128),
		AnyValue::UInt16(value) => exact_unsigned(value as u128),
		AnyValue::UInt32(value) => exact_unsigned(value as u128),
		AnyValue::UInt64(value) => exact_unsigned(value as u128),
		AnyValue::UInt128(value) => exact_unsigned(value),
		AnyValue::Float16(value) => Some(f32::from(value) as f64),
		AnyValue::Float32(value) => Some(value as f64),
		AnyValue::Float64(value) => Some(value),
		AnyValue::Decimal(value, _precision, scale) => exact_decimal(value, scale),
		AnyValue::Null => return Err(ExactF64Error::Null),
		_ => None,
	};
	converted.ok_or(ExactF64Error::Inexact)
}

fn exact_signed(value: i128) -> Option<f64> {
	let converted = value as f64;
	(integer_magnitude_is_exact(value.unsigned_abs()) && converted.is_finite()).then_some(converted)
}

fn exact_unsigned(value: u128) -> Option<f64> {
	let converted = value as f64;
	(integer_magnitude_is_exact(value) && converted.is_finite()).then_some(converted)
}

fn exact_decimal(value: i128, scale: usize) -> Option<f64> {
	let scale_u32 = u32::try_from(scale).ok()?;
	let scale_i32 = i32::try_from(scale).ok()?;
	let five = 5_i128.checked_pow(scale_u32)?;
	if value % five != 0 {
		return None;
	}
	let numerator = value / five;
	let converted = numerator as f64 / 2_f64.powi(scale_i32);
	(integer_magnitude_is_exact(numerator.unsigned_abs()) && converted.is_finite())
		.then_some(converted)
}

fn integer_magnitude_is_exact(value: u128) -> bool {
	if value == 0 {
		return true;
	}
	let significant_bits = u128::BITS - value.leading_zeros() - value.trailing_zeros();
	significant_bits <= f64::MANTISSA_DIGITS
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn product_0096_exact_f64_distinguishes_the_integer_boundary() {
		assert_eq!(
			exact_f64(AnyValue::Int64(9_007_199_254_740_992)),
			Ok(9_007_199_254_740_992.0)
		);
		assert_eq!(
			exact_f64(AnyValue::Int64(9_007_199_254_740_993)),
			Err(ExactF64Error::Inexact)
		);
		assert_eq!(
			exact_f64(AnyValue::Int64(9_007_199_254_740_994)),
			Ok(9_007_199_254_740_994.0)
		);
	}
}
