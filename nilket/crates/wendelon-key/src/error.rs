//! The tool's one error type. No variant carries a passphrase, a seed or a derived key: values
//! are names, public keys, digests and typed reasons only (pinned by T-SECRET).
use key_guard::custody::CustodyError;
use wendelon::storage::mutation_visibility::root2::operator_proof::{ContextRefusal, ProofRefusal};

#[derive(Debug, thiserror::Error)]
pub(crate) enum ToolError {
	#[error("{0}")]
	Usage(String),
	#[error(transparent)]
	Custody(#[from] CustodyError),
	#[error("{what}: {source}")]
	Read {
		what: String,
		#[source]
		source: std::io::Error,
	},
	#[error("{what} is larger than {max} bytes")]
	TooLarge { what: String, max: u64 },
	#[error("{name} is not a key file: {reason}")]
	Format { name: String, reason: &'static str },
	#[error("{name}: wrong passphrase, or the file is damaged")]
	Decrypt { name: String },
	#[error("{name}: the decrypted seed does not derive the header's public key")]
	KeyMismatch { name: String },
	#[error("the passphrase is not UTF-8")]
	PassphraseNotUtf8,
	#[error(
		"the passphrase has {chars} characters after NFC normalisation; at least 16 are required"
	)]
	PassphraseTooShort { chars: usize },
	#[error("the passphrase is longer than the entry limit")]
	PassphraseTooLong,
	#[error("the two passphrase entries differ")]
	PassphraseMismatch,
	#[error("the backup passphrase must differ from the primary passphrase")]
	BackupPassphraseReused,
	#[error("the context export is refused: {0:?}")]
	Context(ContextRefusal),
	#[error("the key is refused: {0:?}")]
	Key(ProofRefusal),
	#[error("{0}")]
	Refused(String),
	#[error(
		"this host fails the recovery-role checks ({0}); run on an offline custody host, or pass --override-host-checks (logged)"
	)]
	Host(String),
	#[error("the confirmation does not match; nothing was changed")]
	Confirmation,
	#[error("custody.log: the hash chain breaks at line {line}; nothing was changed")]
	LogChain { line: usize },
	#[error("custody.log: {0}")]
	Log(String),
	#[error("random bytes are unavailable")]
	Random,
	#[error("the key derivation failed")]
	Kdf,
	#[error("the encryption failed")]
	Seal,
}

impl ToolError {
	pub(crate) fn read(what: impl Into<String>) -> impl FnOnce(std::io::Error) -> Self {
		let what = what.into();
		move |source| Self::Read { what, source }
	}
	pub(crate) fn refused(why: impl Into<String>) -> Self {
		Self::Refused(why.into())
	}
}
