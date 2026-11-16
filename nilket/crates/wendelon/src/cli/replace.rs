//! ADR 0104 (wendelon): replacing a voter that lost its disk.
//!
//! ```text
//! replace begin --slot <node-id> [--token <hex>] <connection>
//! replace status --token <hex> <connection>
//! replace provision --data-dir <dir> --token <hex>
//! ```
//!
//! `begin` opens the replacement of `slot`'s voter and prints its token (a
//! fresh random one unless `--token` repeats an earlier begin). `provision`
//! writes that token beside the replacement's fresh, empty store; the node
//! then starts control-only, is granted a new Raft instance bound to its
//! store's birth, and the leader adds, promotes and records it. `status`
//! reads the replacement's record. `<connection>` is the operator's
//! (`--bootstrap --cert --key --ca [--cluster-domain]`).

use std::future::Future;
use std::io::{ErrorKind, Write};
use std::path::PathBuf;
use std::process::ExitCode;
use std::time::Duration;

use super::index::{ConnectionOptions, connect, parse_connection};
use crate::network::control_server::{
	ReplacementAdminExtensionWire, ReplacementReportWire, TenantAdminRequestWire,
	TenantAdminResponseWire,
};

/// How long one begin or status exchange may take: connection setup, request
/// and answer together.
const EXCHANGE_BOUND: Duration = Duration::from_secs(30);

#[derive(Clone, Debug, PartialEq, Eq)]
enum ReplaceCommand {
	Begin {
		slot: u32,
		token: Option<u128>,
		connection: ConnectionOptions,
	},
	Status {
		token: u128,
		connection: ConnectionOptions,
	},
	Provision {
		data_dir: PathBuf,
		token: u128,
	},
}

/// How an exchange ended without an answer to act on.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Failure {
	/// The cluster answered with a refusal.
	Refused(String),
	/// No answer within the bound, or none that could be read: the request may
	/// have been applied.
	Unknown(String),
}

pub(super) fn run(args: &[String]) -> ExitCode {
	let command = match parse_args(args) {
		Ok(command) => command,
		Err(message) => {
			eprintln!("{message}");
			return ExitCode::from(2);
		}
	};
	let runtime = match tokio::runtime::Builder::new_multi_thread()
		.enable_all()
		.build()
	{
		Ok(runtime) => runtime,
		Err(error) => {
			eprintln!("failed to initialize Wendelon replace runtime: {error}");
			return ExitCode::from(2);
		}
	};
	match runtime.block_on(run_async(command)) {
		Ok(()) => ExitCode::SUCCESS,
		Err(message) => {
			eprintln!("{message}");
			ExitCode::from(1)
		}
	}
}

/// The one report line, in a fixed form a script can read:
/// `replacement <token>: slot <slot> replacing instance <r_old>: <phase>`, where
/// the phase is `begun`, `reserved <instance>` or `done <instance>`.
fn report(token: u128, replacement: &ReplacementReportWire) -> String {
	use crate::consensus::decisions::instances::ReplacementPhase;
	let phase = match replacement.phase {
		ReplacementPhase::Begun => "begun".to_owned(),
		ReplacementPhase::Reserved { r_new } => format!("reserved {r_new}"),
		ReplacementPhase::Done { r_new } => format!("done {r_new}"),
	};
	// ADR 0105 (wendelon): the promoted store, once there is one.
	let promoted = replacement
		.promoted
		.map(|store| format!("; promoted store {:032x}", store.get()))
		.unwrap_or_default();
	// ADR 0106 (wendelon): the recorded recovery, once there is one.
	let recovered = replacement
		.recovered
		.filter(|_| replacement.bound_key.is_some())
		.map(|(gid, k)| format!("; recovered gid {gid} through {k}"))
		.unwrap_or_default();
	let bound = replacement
		.bound_key
		.filter(|_| replacement.promoted.is_some())
		.map(|key| {
			let hex: String = key.iter().map(|byte| format!("{byte:02x}")).collect();
			format!("; bound key {hex}")
		})
		.unwrap_or_default();
	// ADR 0108 (wendelon): the recorded activation, after the recovery only.
	let activated = replacement
		.activated
		.filter(|_| !recovered.is_empty())
		.map(|(through, gid)| format!("; activated through {through} at gid {gid}"))
		.unwrap_or_default();
	// ADR 0107 (wendelon): the cluster's current certificate, after the recorded
	// recovery only.
	let certified = replacement
		.certified
		.filter(|_| !recovered.is_empty())
		.map(|(gid, t)| format!("; certified gid {gid} through {t}"))
		.unwrap_or_default();
	format!(
		"replacement {token:032x}: slot {} replacing instance {}: {phase}{promoted}{bound}{recovered}{activated}{certified}",
		replacement.slot, replacement.r_old
	)
}

/// The commands that name one replacement again: a repeated begin with the
/// same token is answered with the replacement already recorded.
fn identity(slot: u32, token: u128) -> String {
	format!(
		"retry with `nilket wendelon replace begin --slot {slot} --token {token:032x} <connection>`, \
		 or read it with `nilket wendelon replace status --token {token:032x} <connection>`"
	)
}

/// One replacement exchange, connection setup included, ended at `bound`.
async fn exchange(
	connection: &ConnectionOptions,
	request: TenantAdminRequestWire,
	bound: Duration,
) -> Result<ReplacementAdminExtensionWire, Failure> {
	bounded(bound, async move {
		let client = connect(connection).await?;
		client.replacement_admin(&request).await
	})
	.await
}

/// `exchange` ends at `bound` however far it got: dial, request or answer. A
/// definite refusal is told apart from everything else, which is unknown.
async fn bounded(
	bound: Duration,
	exchange: impl Future<
		Output = std::io::Result<(TenantAdminResponseWire, ReplacementAdminExtensionWire)>,
	>,
) -> Result<ReplacementAdminExtensionWire, Failure> {
	match tokio::time::timeout(bound, exchange).await {
		Err(_) => Err(Failure::Unknown(format!("no answer within {bound:?}"))),
		Ok(Ok((_, extension))) => Ok(extension),
		Ok(Err(error)) => match error.kind() {
			ErrorKind::PermissionDenied
			| ErrorKind::NotFound
			| ErrorKind::AlreadyExists
			| ErrorKind::InvalidInput => Err(Failure::Refused(error.to_string())),
			_ => Err(Failure::Unknown(error.to_string())),
		},
	}
}

/// Opens `slot`'s replacement under `token`. The token, and how to name the
/// replacement again, are written to `out` and flushed before anything is
/// sent, so a begin whose answer is lost is never left without its token.
async fn begin<Submit, Submitted>(
	out: &mut impl Write,
	slot: u32,
	token: u128,
	send: Submit,
) -> Result<ReplacementReportWire, String>
where
	Submit: FnOnce(TenantAdminRequestWire) -> Submitted,
	Submitted: Future<Output = Result<ReplacementAdminExtensionWire, Failure>>,
{
	writeln!(
		out,
		"replacement token {token:032x} for slot {slot}; {}",
		identity(slot, token)
	)
	.and_then(|()| out.flush())
	.map_err(|error| {
		format!("not submitted: the token {token:032x} could not be written first: {error}")
	})?;
	match send(TenantAdminRequestWire::BeginReplacement { token, slot }).await {
		Ok(extension) => extension.replacement.ok_or_else(|| {
			format!(
				"the answer to begin {token:032x} carries no replacement record; {}",
				identity(slot, token)
			)
		}),
		Err(Failure::Refused(why)) => Err(format!("begin {token:032x} refused: {why}")),
		Err(Failure::Unknown(why)) => Err(format!(
			"the outcome of begin {token:032x} is unknown ({why}); {}",
			identity(slot, token)
		)),
	}
}

/// The replacement recorded under `token`.
async fn status<Submit, Submitted>(
	token: u128,
	send: Submit,
) -> Result<ReplacementReportWire, String>
where
	Submit: FnOnce(TenantAdminRequestWire) -> Submitted,
	Submitted: Future<Output = Result<ReplacementAdminExtensionWire, Failure>>,
{
	match send(TenantAdminRequestWire::GetReplacement { token }).await {
		Ok(ReplacementAdminExtensionWire {
			replacement: Some(replacement),
		}) => Ok(replacement),
		Ok(_) | Err(Failure::Refused(_)) => Err(format!("no replacement {token:032x} is recorded")),
		Err(Failure::Unknown(why)) => Err(format!(
			"replacement {token:032x}: no answer to read ({why})"
		)),
	}
}

fn fresh_token() -> Result<u128, String> {
	use rand::TryRng;
	let mut bytes = [0u8; 16];
	rand::rngs::SysRng
		.try_fill_bytes(&mut bytes)
		.map_err(|error| format!("no OS randomness for a token: {error}"))?;
	Ok(u128::from_be_bytes(bytes))
}

async fn run_async(command: ReplaceCommand) -> Result<(), String> {
	match command {
		ReplaceCommand::Begin {
			slot,
			token,
			connection,
		} => {
			let token = match token {
				Some(token) => token,
				None => fresh_token()?,
			};
			let replacement = begin(&mut std::io::stdout(), slot, token, |request| {
				exchange(&connection, request, EXCHANGE_BOUND)
			})
			.await?;
			println!("{}", report(token, &replacement));
		}
		ReplaceCommand::Status { token, connection } => {
			let replacement = status(token, |request| {
				exchange(&connection, request, EXCHANGE_BOUND)
			})
			.await?;
			println!("{}", report(token, &replacement));
		}
		ReplaceCommand::Provision { data_dir, token } => {
			crate::node::provision_replacement_token(&data_dir, token)
				.map_err(|error| format!("provision {}: {error}", data_dir.display()))?;
			println!(
				"provisioned replacement {token:032x} in {}; start the node to run it control-only",
				data_dir.display()
			);
		}
	}
	Ok(())
}

fn parse_token(value: &str) -> Result<u128, String> {
	u128::from_str_radix(value.trim_start_matches("0x"), 16)
		.map_err(|error| format!("invalid --token: {error}"))
}

fn parse_args(args: &[String]) -> Result<ReplaceCommand, String> {
	let operation = args
		.first()
		.ok_or_else(|| "wendelon replace requires begin, status or provision".to_owned())?;
	let mut slot = None;
	let mut token = None;
	let mut data_dir = None;
	let mut connection_args = Vec::new();
	let mut cursor = 1usize;
	while let Some(flag) = args.get(cursor) {
		let value = args
			.get(cursor + 1)
			.cloned()
			.ok_or_else(|| format!("wendelon replace {flag} requires a value"))?;
		match flag.as_str() {
			"--slot" => {
				slot = Some(
					value
						.parse::<u32>()
						.map_err(|error| format!("invalid --slot: {error}"))?,
				)
			}
			"--token" => token = Some(parse_token(&value)?),
			"--data-dir" => data_dir = Some(PathBuf::from(value)),
			"--bootstrap" | "--cert" | "--key" | "--ca" | "--cluster-domain" => {
				connection_args.push(flag.clone());
				connection_args.push(value);
			}
			unknown => return Err(format!("unknown wendelon replace option: {unknown}")),
		}
		cursor += 2;
	}
	let connection = || parse_connection("wendelon replace", &connection_args);
	match operation.as_str() {
		"begin" => {
			if data_dir.is_some() {
				return Err("wendelon replace begin takes no --data-dir".into());
			}
			Ok(ReplaceCommand::Begin {
				slot: slot.ok_or("wendelon replace begin requires --slot")?,
				token,
				connection: connection()?,
			})
		}
		"status" => {
			if slot.is_some() || data_dir.is_some() {
				return Err("wendelon replace status takes only --token and a connection".into());
			}
			Ok(ReplaceCommand::Status {
				token: token.ok_or("wendelon replace status requires --token")?,
				connection: connection()?,
			})
		}
		"provision" => {
			if slot.is_some() || !connection_args.is_empty() {
				return Err("wendelon replace provision takes only --data-dir and --token".into());
			}
			Ok(ReplaceCommand::Provision {
				data_dir: data_dir.ok_or("wendelon replace provision requires --data-dir")?,
				token: token.ok_or("wendelon replace provision requires --token")?,
			})
		}
		other => Err(format!("unknown wendelon replace operation: {other}")),
	}
}

#[cfg(test)]
mod tests {
	use super::*;

	fn args(list: &[&str]) -> Vec<String> {
		list.iter().map(|s| (*s).to_owned()).collect()
	}

	const CONNECTION: &[&str] = &[
		"--bootstrap",
		"1@127.0.0.1:7001",
		"--cert",
		"operator.crt",
		"--key",
		"operator.key",
		"--ca",
		"node-ca.crt",
	];

	#[test]
	fn each_operation_takes_exactly_its_own_options() {
		let mut begin = args(&["begin", "--slot", "3"]);
		begin.extend(args(CONNECTION));
		assert!(matches!(
			parse_args(&begin).unwrap(),
			ReplaceCommand::Begin {
				slot: 3,
				token: None,
				..
			}
		));
		let mut status = args(&["status", "--token", "0104"]);
		status.extend(args(CONNECTION));
		assert!(matches!(
			parse_args(&status).unwrap(),
			ReplaceCommand::Status { token: 0x104, .. }
		));
		assert!(matches!(
			parse_args(&args(&["provision", "--data-dir", "/d", "--token", "0104"])).unwrap(),
			ReplaceCommand::Provision { token: 0x104, .. }
		));
		let mut provision = args(&["provision", "--data-dir", "/d", "--token", "0104"]);
		provision.extend(args(CONNECTION));
		assert!(parse_args(&provision).is_err());
		assert!(parse_args(&args(&["begin"])).is_err());
		assert!(parse_args(&args(&["status", "--token", "zz"])).is_err());
	}

	// The exchanges' bounds and the begin's token, over the real client on the
	// simulated network: a scripted leader records a begin and can lose its
	// answer or answer late. No cluster runs; the reducer's own replay of a
	// begin under the same token is covered with the reducer.

	use crate::client::{
		ClusterPeerWire, ClusterStatusWire, StaticTopologySource, WendelonClient,
		WendelonClientConfig,
	};
	use crate::consensus::decisions::instances::ReplacementPhase;
	use crate::network::core::{HRFT_MAGIC, HrftFrame};
	use crate::network::pool::{BufPool, PooledBuf};
	use crate::network::server::{HrftService, RequestContext};
	use crate::network::transport::{
		NodeRole, NodeRoleSet, TransportProvider, TransportTier, TransportTierSet,
	};
	use crate::schema::{FrameHeader, MsgType, NodeId};
	use crate::simulator::network::{SimulatedTransportNetwork, SimulatedTransportProvider};
	use std::collections::HashMap;
	use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
	use std::sync::{Arc, Mutex};
	use std::time::Instant;

	const LEADER: u32 = 1;
	const SLOT: u32 = 3;
	const TOKEN: u128 = 0x0104_0000_0000_0000_0000_0000_0000_00aa;
	const BOUND: Duration = Duration::from_millis(600);
	const SLACK: Duration = Duration::from_millis(1500);
	const TIER: TransportTier = TransportTier::QuicIoUring;

	/// What the operator's terminal shows, readable by the scripted leader.
	#[derive(Clone, Default)]
	struct Terminal(Arc<Mutex<Vec<u8>>>);

	impl Terminal {
		fn text(&self) -> String {
			String::from_utf8(self.0.lock().unwrap().clone()).unwrap()
		}
	}

	impl Write for Terminal {
		fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
			self.0.lock().unwrap().extend_from_slice(bytes);
			Ok(bytes.len())
		}

		fn flush(&mut self) -> std::io::Result<()> {
			Ok(())
		}
	}

	struct Closed;

	impl Write for Closed {
		fn write(&mut self, _: &[u8]) -> std::io::Result<usize> {
			Err(std::io::Error::new(ErrorKind::BrokenPipe, "closed"))
		}

		fn flush(&mut self) -> std::io::Result<()> {
			Err(std::io::Error::new(ErrorKind::BrokenPipe, "closed"))
		}
	}

	/// A leader that records a begin under its token (a repeated begin finds
	/// the record) and answers from the record, unless told to lose the next
	/// answer or to answer late.
	#[derive(Default)]
	struct Leader {
		terminal: Terminal,
		records: Mutex<HashMap<u128, ReplacementReportWire>>,
		lose_next_answer: AtomicBool,
		answer_after: Mutex<Duration>,
		requests: AtomicUsize,
		/// For each begin received: whether its token was already on the
		/// operator's terminal.
		token_shown_first: Mutex<Vec<bool>>,
	}

	#[async_trait::async_trait]
	impl HrftService for Leader {
		async fn handle_frame(
			&self,
			_context: RequestContext,
			frame: HrftFrame,
		) -> std::io::Result<Option<HrftFrame>> {
			assert_eq!(frame.header.msg_type, MsgType::TENANT_ADMIN_REQ.0);
			self.requests.fetch_add(1, Ordering::SeqCst);
			let request: TenantAdminRequestWire = crate::network::wire::decode(&frame.meta)?;
			let replacement = match request {
				TenantAdminRequestWire::BeginReplacement { token, slot } => {
					self.token_shown_first
						.lock()
						.unwrap()
						.push(self.terminal.text().contains(&format!("{token:032x}")));
					Some(*self.records.lock().unwrap().entry(token).or_insert(
						ReplacementReportWire {
							slot,
							r_old: slot,
							phase: ReplacementPhase::Begun,
							promoted: None,
							bound_key: None,
							recovered: None,
							certified: None,
							activated: None,
						},
					))
				}
				TenantAdminRequestWire::GetReplacement { token } => {
					self.records.lock().unwrap().get(&token).copied()
				}
				other => panic!("unexpected request {other:?}"),
			};
			if self.lose_next_answer.swap(false, Ordering::SeqCst) {
				std::future::pending::<()>().await;
			}
			let delay = *self.answer_after.lock().unwrap();
			tokio::time::sleep(delay).await;
			let mut meta = crate::network::wire::encode(&TenantAdminResponseWire {
				accepted: replacement.is_some(),
				reason_code: if replacement.is_some() {
					crate::network::control_server::CONTROL_REASON_GRANTED
				} else {
					crate::network::control_server::CONTROL_REASON_INVALID_TARGET
				},
				leader_hint: Some(LEADER),
				tenant: None,
				tenants: Vec::new(),
				message: None,
			})?;
			meta.extend_from_slice(&crate::network::wire::encode(
				&ReplacementAdminExtensionWire { replacement },
			)?);
			Ok(Some(HrftFrame {
				header: FrameHeader {
					magic: HRFT_MAGIC,
					version: 1,
					msg_type: MsgType::TENANT_ADMIN_RESP.0,
					flags: 0,
					stream_id: frame.header.stream_id,
					meta_len: meta.len() as u32,
					arrow_len: 0,
					trailing_len: 0,
				},
				meta: PooledBuf::orphan(meta),
				arrow_payload: None,
				trailing: None,
			}))
		}
	}

	fn leader_and_client() -> (Arc<Leader>, Arc<WendelonClient>, Terminal) {
		let terminal = Terminal::default();
		let leader = Arc::new(Leader {
			terminal: terminal.clone(),
			..Leader::default()
		});
		let network = Arc::new(SimulatedTransportNetwork::default());
		let address = "replace-leader:7001";
		network.publish(
			NodeId {
				id: LEADER,
				_reserved: 0,
			},
			address,
			leader.clone(),
		);
		let provider: Arc<dyn TransportProvider> = Arc::new(SimulatedTransportProvider::new(
			NodeId {
				id: 99,
				_reserved: 0,
			},
			network,
			BufPool::new(64, 1024 * 1024),
			TIER,
		));
		let status = ClusterStatusWire {
			epoch: 1,
			leader: Some(LEADER),
			replication_factor: 1,
			peers: vec![ClusterPeerWire {
				node_id: LEADER,
				address: address.to_owned(),
				tier_bits: TransportTierSet::empty().with(TIER).as_u8(),
				role_bits: NodeRoleSet::empty()
					.with(NodeRole::Data)
					.with(NodeRole::Control)
					.as_u8(),
				capability_bits: 0,
			}],
		};
		let client = WendelonClient::new(WendelonClientConfig {
			local_tiers: TransportTierSet::empty().with(TIER),
			providers: [(TIER, provider)].into(),
			bootstrap: status.clone(),
			topology: Arc::new(StaticTopologySource::new(status)),
			pool_max_retained: 64,
			pool_max_cap_bytes: 1024 * 1024,
		});
		(leader, client, terminal)
	}

	fn over(
		client: &Arc<WendelonClient>,
	) -> impl FnOnce(
		TenantAdminRequestWire,
	) -> std::pin::Pin<
		Box<dyn Future<Output = Result<ReplacementAdminExtensionWire, Failure>>>,
	> {
		let client = client.clone();
		move |request| {
			Box::pin(async move {
				bounded(
					BOUND,
					async move { client.replacement_admin(&request).await },
				)
				.await
			})
		}
	}

	#[tokio::test]
	async fn a_begin_whose_answer_is_lost_has_shown_its_token_and_its_retry_finds_the_record() {
		let (leader, client, mut terminal) = leader_and_client();
		leader.lose_next_answer.store(true, Ordering::SeqCst);
		let started = Instant::now();
		let lost = begin(&mut terminal, SLOT, TOKEN, over(&client)).await;
		let elapsed = started.elapsed();
		let message = lost.expect_err("the answer was lost");
		let token = format!("{TOKEN:032x}");
		assert!(message.contains("unknown"), "{message}");
		assert!(
			message.contains(&format!("--slot {SLOT} --token {token}"))
				&& message.contains(&format!("status --token {token}")),
			"the failure names the retry and the status read: {message}"
		);
		assert!(elapsed < BOUND + SLACK, "ended after {elapsed:?}");
		let committed = leader.records.lock().unwrap().get(&TOKEN).copied();
		assert!(committed.is_some(), "the begin was recorded");
		assert_eq!(*leader.token_shown_first.lock().unwrap(), vec![true]);
		assert!(terminal.text().contains(&token), "{}", terminal.text());

		let retried = begin(&mut terminal, SLOT, TOKEN, over(&client))
			.await
			.expect("the retry is answered");
		assert_eq!(Some(retried), committed, "the same replacement");
		assert_eq!(leader.records.lock().unwrap().len(), 1);
		assert_eq!(
			status(TOKEN, over(&client)).await.expect("recorded"),
			retried
		);
	}

	#[tokio::test]
	async fn a_begin_that_cannot_show_its_token_sends_nothing() {
		let (leader, client, _) = leader_and_client();
		let message = begin(&mut Closed, SLOT, TOKEN, over(&client))
			.await
			.expect_err("nothing may be sent");
		assert!(message.starts_with("not submitted"), "{message}");
		assert_eq!(leader.requests.load(Ordering::SeqCst), 0);
	}

	#[tokio::test]
	async fn an_answer_after_the_bound_is_unknown() {
		let (leader, client, mut terminal) = leader_and_client();
		*leader.answer_after.lock().unwrap() = BOUND + Duration::from_millis(400);
		let started = Instant::now();
		let message = begin(&mut terminal, SLOT, TOKEN, over(&client))
			.await
			.expect_err("answered late");
		let elapsed = started.elapsed();
		assert!(message.contains("unknown"), "{message}");
		assert!(
			elapsed >= BOUND && elapsed < BOUND + SLACK,
			"ended after {elapsed:?}"
		);
		let message = status(TOKEN, over(&client))
			.await
			.expect_err("answered late");
		assert!(message.contains("no answer to read"), "{message}");
		assert_eq!(leader.requests.load(Ordering::SeqCst), 2);
	}

	/// The command's own exchange, connection setup included, against an
	/// address where nothing ever answers: the dial cannot complete.
	#[tokio::test]
	async fn an_exchange_whose_dial_never_completes_ends_at_its_bound() {
		let silent = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
		let pki = std::path::Path::new(env!("CARGO_MANIFEST_DIR")).join("tests/support/pki");
		let connection = parse_connection(
			"test",
			&args(&[
				"--bootstrap",
				&format!("{LEADER}@{}", silent.local_addr().unwrap()),
				"--cert",
				pki.join("operator.crt").to_str().unwrap(),
				"--key",
				pki.join("operator.key").to_str().unwrap(),
				"--ca",
				pki.join("ca.crt").to_str().unwrap(),
			]),
		)
		.unwrap();
		let started = Instant::now();
		let outcome = exchange(
			&connection,
			TenantAdminRequestWire::GetReplacement { token: TOKEN },
			BOUND,
		)
		.await;
		let elapsed = started.elapsed();
		assert!(
			matches!(&outcome, Err(Failure::Unknown(why)) if why.contains("no answer within")),
			"{outcome:?}"
		);
		assert!(
			elapsed >= BOUND && elapsed < BOUND + SLACK,
			"ended after {elapsed:?}"
		);
		drop(silent);
	}

	/// The report line's fixed form, which the real-process workflow parses.
	#[test]
	fn the_report_line_has_its_fixed_form() {
		let line = |phase| {
			report(
				TOKEN,
				&ReplacementReportWire {
					slot: SLOT,
					r_old: SLOT,
					phase,
					promoted: None,
					bound_key: None,
					recovered: None,
					certified: None,
					activated: None,
				},
			)
		};
		let prefix = format!("replacement {TOKEN:032x}: slot 3 replacing instance 3: ");
		assert_eq!(line(ReplacementPhase::Begun), format!("{prefix}begun"));
		assert_eq!(
			line(ReplacementPhase::Reserved { r_new: 1 << 31 }),
			format!("{prefix}reserved 2147483648")
		);
		assert_eq!(
			line(ReplacementPhase::Done { r_new: 1 << 31 }),
			format!("{prefix}done 2147483648")
		);
		// ADR 0105 (wendelon): a promoted store follows its phase.
		assert_eq!(
			report(
				TOKEN,
				&ReplacementReportWire {
					slot: SLOT,
					r_old: SLOT,
					phase: ReplacementPhase::Done { r_new: 1 << 31 },
					promoted: Some(crate::consensus::StoreIncarnation::from_raw(0x0105)),
					bound_key: None,
					recovered: None,
					certified: None,
					activated: None,
				},
			),
			format!("{prefix}done 2147483648; promoted store 00000000000000000000000000000105")
		);
		// ADR 0106, ADR 0107 (wendelon): the recorded recovery follows the bound
		// key, and the current certificate follows the recovery, never alone.
		let done = |recovered, certified| {
			report(
				TOKEN,
				&ReplacementReportWire {
					slot: SLOT,
					r_old: SLOT,
					phase: ReplacementPhase::Done { r_new: 1 << 31 },
					promoted: Some(crate::consensus::StoreIncarnation::from_raw(0x0105)),
					bound_key: Some([0xab; 32]),
					recovered,
					certified,
					activated: None,
				},
			)
		};
		let bound = format!(
			"{prefix}done 2147483648; promoted store 00000000000000000000000000000105; bound key {}",
			"ab".repeat(32)
		);
		assert_eq!(done(None, Some((3, 90))), bound);
		assert_eq!(
			done(Some((2, 41)), None),
			format!("{bound}; recovered gid 2 through 41")
		);
		assert_eq!(
			done(Some((2, 41)), Some((3, 90))),
			format!("{bound}; recovered gid 2 through 41; certified gid 3 through 90")
		);
	}

	/// A replacement answer: the ordinary response, then the replacement's own
	/// extension; a rejection carries none, and nothing may follow it.
	#[test]
	fn a_replacement_answer_carries_its_record_in_its_own_extension() {
		use crate::network::control_server::decode_replacement_admin_answer;
		let response = TenantAdminResponseWire {
			accepted: true,
			reason_code: 0,
			leader_hint: Some(LEADER),
			tenant: None,
			tenants: Vec::new(),
			message: None,
		};
		let plain = crate::network::wire::encode(&response).unwrap();
		assert_eq!(
			decode_replacement_admin_answer(&plain).unwrap(),
			(response.clone(), ReplacementAdminExtensionWire::default())
		);
		let extension = ReplacementAdminExtensionWire {
			replacement: Some(ReplacementReportWire {
				slot: SLOT,
				r_old: SLOT,
				phase: ReplacementPhase::Done {
					r_new: crate::consensus::decisions::instances::ALLOCATED_BASE,
				},
				promoted: Some(crate::consensus::StoreIncarnation::from_raw(0x0105)),
				bound_key: None,
				recovered: None,
				certified: None,
				activated: None,
			}),
		};
		let mut answer = plain.clone();
		answer.extend_from_slice(&crate::network::wire::encode(&extension).unwrap());
		assert_eq!(
			decode_replacement_admin_answer(&answer).unwrap(),
			(response, extension)
		);
		answer.push(0);
		assert!(decode_replacement_admin_answer(&answer).is_err());
	}
}
