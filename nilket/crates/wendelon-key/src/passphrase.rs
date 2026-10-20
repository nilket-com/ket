//! Passphrase policy (K3): UTF-8, NFC-normalised, at least 16 characters. Entry itself is
//! key-guard's terminal read; there is no argv, environment, file or stdin path.
use crate::error::ToolError;
use crate::io::Io;
use key_guard::custody::{MAX_SECRET_LEN, SecretBuf};
use unicode_normalization::UnicodeNormalization;
use zeroize::Zeroize;

pub(crate) const MIN_CHARS: usize = 16;
/// NFC of a UTF-8 string is at most three times its length.
const NFC_GROWTH: usize = 3;

/// The NFC form of `raw`, in a new secret buffer, if it has at least `MIN_CHARS` characters.
/// The normaliser's own small working buffer is not zeroed (stated limit, like the KDF's).
pub(crate) fn normalize(raw: &SecretBuf) -> Result<SecretBuf, ToolError> {
	let text = std::str::from_utf8(raw.as_slice()).map_err(|_| ToolError::PassphraseNotUtf8)?;
	let mut out = SecretBuf::with_capacity(raw.len() * NFC_GROWTH + 4);
	let mut chars = 0;
	for c in text.nfc() {
		let mut utf8 = [0_u8; 4];
		let pushed = c
			.encode_utf8(&mut utf8)
			.as_bytes()
			.iter()
			.try_for_each(|b| out.push(*b));
		utf8.zeroize();
		pushed.map_err(|_| ToolError::PassphraseTooLong)?;
		chars += 1;
	}
	if chars < MIN_CHARS {
		return Err(ToolError::PassphraseTooShort { chars });
	}
	Ok(out)
}

/// One existing passphrase, normalised.
pub(crate) fn existing(io: &mut dyn Io, what: &str) -> Result<SecretBuf, ToolError> {
	normalize(&io.secret(&format!("{what}: "), MAX_SECRET_LEN)?)
}

/// A new passphrase, asked twice; the two entries must be equal after normalisation.
pub(crate) fn new(io: &mut dyn Io, what: &str) -> Result<SecretBuf, ToolError> {
	let first = existing(io, &format!("New {what}"))?;
	let second = existing(io, &format!("Repeat the new {what}"))?;
	if first.as_slice() != second.as_slice() {
		return Err(ToolError::PassphraseMismatch);
	}
	Ok(first)
}
