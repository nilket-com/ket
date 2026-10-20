//! `generate`: a new role key, written Pending. The seed is fresh randomness (never the
//! operator's), and the public key passes the facade's new-key rules before anything is written.
use super::{overridden, role};
use crate::args::Args;
use crate::context;
use crate::envelope::{self, Header, NEW_LOG_N, NEW_P, State, fingerprint, hex};
use crate::error::ToolError;
use crate::io::Io;
use crate::passphrase;
use crate::session::{self, fresh, key_name, must_be_free};
use wendelon::storage::mutation_visibility::root2::operator_proof::check_new_role_key;

pub(super) fn run(io: &mut dyn Io, a: &Args) -> Result<(), ToolError> {
	let role = role(a)?;
	let epoch = a.number("--intended-epoch")?;
	let out = key_name(a.value("--out")?)?;
	let sources = match (a.opt("--context"), a.opt("--trusted-manifest")) {
		(Some(_), Some(m)) => Some((a.paths("--context"), std::path::Path::new(m))),
		(None, None) => None,
		_ => {
			return Err(ToolError::Usage(
				"--context and --trusted-manifest go together".into(),
			));
		}
	};
	session::run(io, a.path("--dir")?, "generate", |io, s| {
		s.host_checks(role, overridden(a))?;
		let verified = match &sources {
			Some((contexts, manifest)) => Some(context::load(contexts, manifest)?.remove(0)),
			None => None,
		};
		if let Some(v) = &verified {
			let active = v.active()[usize::from(role - 1)].0;
			if epoch <= active {
				return Err(ToolError::refused(format!(
					"intended epoch {epoch} is not after the role's active epoch {active}; it could never bind"
				)));
			}
		}
		must_be_free(s.dir, out)?;
		let passphrase = passphrase::new(io, "passphrase")?;
		let seed = io.new_seed()?;
		let key = envelope::public_key(&seed);
		check_new_role_key(verified.as_ref(), role, &key).map_err(ToolError::Key)?;
		let header = fresh(
			io,
			Header {
				state: State::Pending,
				log_n: NEW_LOG_N,
				p: NEW_P,
				salt: [0; 32],
				nonce: [0; 12],
				role,
				epoch,
				context: verified.as_ref().map(|v| *v.context()),
				key,
				backup_of: None,
			},
		)?;
		s.identity(&header);
		let file = envelope::seal(&header, &seed, &passphrase)?;
		s.lock.create_file(out, &file)?;
		s.output(out, &file);
		writeln!(io.out(), "{}\nfingerprint {}", hex(&key), fingerprint(&key))
			.map_err(ToolError::read("stdout"))
	})
}
