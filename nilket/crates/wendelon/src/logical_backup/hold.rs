//! ADR 0736: the operator tool's side of the backup hold, every wait bounded.
//!
//! Only a positive answer is taken as proof. Any other outcome of a call (no
//! answer within its budget, a lost answer, or any error, whatever its kind or
//! text) proves nothing about the command: a Begin or a Release may have
//! committed, for instance when the leader's proposal was indeterminate and
//! the handler answered with an error. No failure is read as a refusal. So an unconfirmed entry is followed by one bounded read of
//! the current hold; when the hold is this tool's own (its id), it is released
//! with its exact token. Whatever is left is reported with the id and, when
//! known, the token, for `backup hold-status` and `backup release`.

use crate::client::core::WendelonClient;
use crate::network::control_server::{BackupAdminExtensionWire, TenantAdminRequestWire};
use std::io::{Error, ErrorKind};
use std::time::Duration;

/// The tool's budgets. Each is absolute for the work it covers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct Bounds {
	/// All roster reads of one wait for a settled cut, together.
	pub settle: Duration,
	/// One hold entry, read or release, including its connection.
	pub call: Duration,
	/// One table's read (open and drain) on export, or verification on restore.
	pub table: Duration,
}

impl Bounds {
	pub const DEFAULT: Self = Self {
		settle: Duration::from_secs(120),
		call: Duration::from_secs(30),
		table: Duration::from_secs(600),
	};
}

/// What a release came to.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Release {
	/// The cluster answered that it released the hold.
	Released,
	/// Anything else: it may or may not have applied. A read of the current
	/// hold (`backup hold-status`) says whether the token is still held.
	Unconfirmed { why: String },
}

/// What a read of the current hold found after an unconfirmed entry.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum AfterUnconfirmedEntry {
	/// No hold, or another's: by the read's cut this attempt held nothing.
	NotHeldByThis { current: Option<(u128, u64)> },
	/// This entry had applied; its hold was then released with its token.
	ReleasedOwn { entered_at: u64 },
	/// This entry had applied; its release was not confirmed.
	OwnReleaseNotConfirmed { entered_at: u64, release: Release },
	/// The read itself did not answer.
	ReadFailed { why: String },
}

/// Why the tool holds no hold of its own.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum HoldError {
	/// The entry was not answered positively (`why`); `after` is what the
	/// follow-up read found and did. Another hold being held, or a caller that
	/// may not hold, shows as `NotHeldByThis` with the reason in `why`.
	Unconfirmed {
		id: u128,
		why: String,
		after: AfterUnconfirmedEntry,
	},
}

impl std::fmt::Display for HoldError {
	fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
		match self {
			Self::Unconfirmed { id, why, after } => {
				write!(
					f,
					"entering backup hold {id:#x} was not confirmed ({why}); "
				)?;
				match after {
					AfterUnconfirmedEntry::NotHeldByThis { current } => write!(
						f,
						"a read then found {current:?}: this attempt held nothing by that read; \
						 should a hold with id {id:#x} still appear, release it with `backup \
						 release --id {id:#x} --entered-at <index from backup hold-status>`"
					),
					AfterUnconfirmedEntry::ReleasedOwn { entered_at } => write!(
						f,
						"it had applied (entered at {entered_at}) and was released with its token"
					),
					AfterUnconfirmedEntry::OwnReleaseNotConfirmed {
						entered_at,
						release,
					} => write!(
						f,
						"it had applied (entered at {entered_at}); its release was {release:?}: new \
						 writes may stay refused until `backup release --id {id:#x} --entered-at \
						 {entered_at}`"
					),
					AfterUnconfirmedEntry::ReadFailed { why } => write!(
						f,
						"the current hold could not be read ({why}): check `backup hold-status`; \
						 a hold with id {id:#x} is this one"
					),
				}
			}
		}
	}
}

async fn call(
	client: &WendelonClient,
	request: TenantAdminRequestWire,
	budget: Duration,
) -> std::io::Result<BackupAdminExtensionWire> {
	match tokio::time::timeout(budget, client.backup_admin(&request)).await {
		Ok(answer) => answer.map(|(_, extension)| extension),
		Err(_) => Err(Error::new(
			ErrorKind::TimedOut,
			format!("no answer within {} s", budget.as_secs()),
		)),
	}
}

/// The current hold's token, read linearizably at the leader.
pub async fn read(client: &WendelonClient, bounds: Bounds) -> std::io::Result<Option<(u128, u64)>> {
	Ok(
		call(client, TenantAdminRequestWire::GetBackupHold, bounds.call)
			.await?
			.hold,
	)
}

/// Releases the hold whose token is exactly `token`.
pub async fn release(client: &WendelonClient, token: (u128, u64), bounds: Bounds) -> Release {
	let request = TenantAdminRequestWire::ReleaseBackupHold {
		id: token.0,
		entered_at: token.1,
	};
	match call(client, request, bounds.call).await {
		Ok(_) => Release::Released,
		Err(error) => Release::Unconfirmed {
			why: error.to_string(),
		},
	}
}

/// Enters the hold `id`; on an unconfirmed answer, reads the current hold and
/// releases it when it is this one.
pub async fn enter(
	client: &WendelonClient,
	id: u128,
	bounds: Bounds,
) -> Result<(u128, u64), HoldError> {
	let why = match call(
		client,
		TenantAdminRequestWire::BeginBackupHold { id },
		bounds.call,
	)
	.await
	{
		Ok(BackupAdminExtensionWire {
			hold: Some(token), ..
		}) if token.0 == id => return Ok(token),
		Ok(other) => format!("the cluster answered without this hold's token: {other:?}"),
		Err(error) => error.to_string(),
	};
	let after = match read(client, bounds).await {
		Ok(Some((held, entered_at))) if held == id => {
			match release(client, (id, entered_at), bounds).await {
				Release::Released => AfterUnconfirmedEntry::ReleasedOwn { entered_at },
				release => AfterUnconfirmedEntry::OwnReleaseNotConfirmed {
					entered_at,
					release,
				},
			}
		}
		Ok(current) => AfterUnconfirmedEntry::NotHeldByThis { current },
		Err(error) => AfterUnconfirmedEntry::ReadFailed {
			why: error.to_string(),
		},
	};
	Err(HoldError::Unconfirmed { id, why, after })
}

#[cfg(test)]
mod tests {
	use crate::logical_backup::roster::RosterRefusal;
	use crate::network::control_server::{
		BackupAdminExtensionWire, TenantAdminResponseWire, decode_tenant_admin_answer,
	};

	fn response() -> TenantAdminResponseWire {
		TenantAdminResponseWire {
			accepted: true,
			reason_code: 0,
			leader_hint: Some(41),
			tenant: None,
			tenants: Vec::new(),
			message: Some("m".into()),
		}
	}

	/// The ordinary response keeps its shape: an answer without an extension
	/// decodes as before, an answer with one decodes for an older decoder of
	/// the response alone, and the extension decodes for the backup tool.
	#[test]
	fn the_backup_extension_follows_an_unchanged_response() {
		let plain = crate::network::wire::encode(&response()).unwrap();
		let (decoded, extension) = decode_tenant_admin_answer(&plain).unwrap();
		assert_eq!(
			(decoded, extension),
			(response(), BackupAdminExtensionWire::default())
		);

		let extension = BackupAdminExtensionWire {
			hold: Some((0x736, 9)),
			roster: Some(Err(RosterRefusal::KvDataPresent)),
		};
		let mut with = plain.clone();
		with.extend_from_slice(&crate::network::wire::encode(&extension).unwrap());
		// A decoder of the response alone.
		assert_eq!(
			crate::network::wire::decode::<TenantAdminResponseWire>(&with).unwrap(),
			response()
		);
		assert_eq!(
			decode_tenant_admin_answer(&with).unwrap(),
			(response(), extension)
		);
		// Anything after the extension is refused.
		let mut trailing = with.clone();
		trailing.push(0);
		assert!(decode_tenant_admin_answer(&trailing).is_err());
	}
}
