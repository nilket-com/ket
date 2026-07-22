//! Language-neutral ownership and action boundary for runtime extensions.
//!
//! Effective command, option, keymap, and event-subscriber adapters consume these declarations.
//! This module owns the invariants that must not be reimplemented by each catalog:
//! monotonic owners and revisions, bounded atomic owner snapshots, atomic owner
//! lifecycle transitions, and owned type-erased action leases.

use super::event_bus::EventKindSet;
use super::input::{KeyCode, Modifiers};
use crate::core::{RegistryOwnerId, RegistryRev, SubscriptionId};
use std::any::{Any, type_name};
use std::cell::Cell;
use std::collections::{BTreeMap, HashMap};
use std::fmt;
use std::mem::size_of;

pub(crate) const MAX_TRANSACTION_ENTRIES: usize = 4_096;
pub(crate) const MAX_SESSION_ENTRIES: usize = 16_384;
pub(crate) const MAX_TRANSACTION_METADATA_BYTES: usize = 1024 * 1024;
pub(crate) const MAX_SESSION_METADATA_BYTES: usize = 8 * 1024 * 1024;
pub(crate) const MAX_NAME_BYTES: usize = 128;
pub(crate) const MAX_ALIASES: usize = 16;
pub(crate) const MAX_SUMMARY_OR_USAGE_BYTES: usize = 512;
pub(crate) const MAX_HELP_BYTES: usize = 16 * 1024;
pub(crate) const MAX_COMPLETION_CANDIDATES: usize = 1_024;
pub(crate) const MAX_COMPLETION_BYTES: usize = 64 * 1024;
pub(crate) const MAX_MAPPING_SEQUENCE: usize = 16;
pub(crate) const MAX_OPTION_QUERY_DISPLAY_BYTES: usize = 4 * 1024;
pub(crate) const MAX_SUBSCRIBERS_PER_OWNER: usize = 256;
pub(crate) const MAX_SUBSCRIBERS_TOTAL: usize = 4_096;

trait ActionPayload: fmt::Debug + Send + Sync {
	fn clone_box(&self) -> Box<dyn ActionPayload>;
	fn as_any(&self) -> &dyn Any;
	fn type_name(&self) -> &'static str;
}

impl<T> ActionPayload for T
where
	T: Any + Clone + fmt::Debug + Send + Sync,
{
	fn clone_box(&self) -> Box<dyn ActionPayload> {
		Box::new(self.clone())
	}

	fn as_any(&self) -> &dyn Any {
		self
	}

	fn type_name(&self) -> &'static str {
		type_name::<T>()
	}
}

/// An owned, executor-specific action association.
///
/// The registry can clone and move the association without knowing whether it
/// contains a callback-store key, an Arc-backed callable, or another host
/// representation. Only the executor downcasts the payload.
pub(crate) struct ActionLease {
	owner: RegistryOwnerId,
	payload: Box<dyn ActionPayload>,
}

impl ActionLease {
	pub(crate) fn new<T>(owner: RegistryOwnerId, payload: T) -> Self
	where
		T: Any + Clone + fmt::Debug + Send + Sync,
	{
		Self {
			owner,
			payload: Box::new(payload),
		}
	}

	pub(crate) const fn owner(&self) -> RegistryOwnerId {
		self.owner
	}

	pub(crate) fn downcast_ref<T: Any>(&self) -> Option<&T> {
		self.payload.as_any().downcast_ref()
	}
}

impl Clone for ActionLease {
	fn clone(&self) -> Self {
		Self {
			owner: self.owner,
			payload: self.payload.clone_box(),
		}
	}
}

impl fmt::Debug for ActionLease {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		f.debug_struct("ActionLease")
			.field("owner", &self.owner)
			.field("payload_type", &self.payload.type_name())
			.finish_non_exhaustive()
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum CommandArgRule {
	None,
	Optional,
	Required,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RuntimeCompletion {
	None,
	Path,
	Language,
	Theme,
	Static(Vec<String>),
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeCommandRegistration {
	pub(crate) name: String,
	pub(crate) aliases: Vec<String>,
	pub(crate) bang: bool,
	pub(crate) args: CommandArgRule,
	pub(crate) usage: String,
	pub(crate) summary: String,
	pub(crate) help: String,
	pub(crate) completion: RuntimeCompletion,
	pub(crate) action: ActionLease,
}

/// Owned command metadata exposed to effective-catalog consumers.
///
/// Action associations are deliberately absent: help, completion,
/// suggestions, and introspection cannot acquire an invocation capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeCommandInfo {
	pub(crate) owner: RegistryOwnerId,
	pub(crate) precedence: u64,
	pub(crate) name: String,
	pub(crate) aliases: Vec<String>,
	pub(crate) bang: bool,
	pub(crate) args: CommandArgRule,
	pub(crate) usage: String,
	pub(crate) summary: String,
	pub(crate) help: String,
	pub(crate) completion: RuntimeCompletion,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) enum MappingMode {
	Normal,
	Visual,
	Insert,
	CommandLine,
	Search,
	Confirm,
	FileBrowser,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash)]
pub(crate) struct KeyStroke {
	pub(crate) code: KeyCode,
	pub(crate) modifiers: Modifiers,
}

impl KeyStroke {
	pub(crate) const fn new(code: KeyCode, modifiers: Modifiers) -> Self {
		Self { code, modifiers }
	}
}

#[derive(Debug, Clone)]
pub(crate) enum RuntimeMappingTarget {
	Command(String),
	Action(ActionLease),
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeMappingRegistration {
	pub(crate) mode: MappingMode,
	pub(crate) sequence: Vec<KeyStroke>,
	pub(crate) summary: String,
	pub(crate) target: RuntimeMappingTarget,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeMappingTargetKind {
	Command,
	Action,
}

/// Owned effective mapping metadata without its action capability.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeMappingInfo {
	pub(crate) owner: RegistryOwnerId,
	pub(crate) precedence: u64,
	pub(crate) mode: MappingMode,
	pub(crate) sequence: Vec<KeyStroke>,
	pub(crate) summary: String,
	pub(crate) target: RuntimeMappingTargetKind,
}

pub(crate) enum ResolvedMappingTarget {
	Command(String),
	Action(InvocationSnapshot),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RuntimeOptionKind {
	Bool,
	Integer,
	String { require_non_empty: bool },
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeOptionRegistration {
	pub(crate) name: String,
	pub(crate) aliases: Vec<String>,
	pub(crate) kind: RuntimeOptionKind,
	pub(crate) usage: String,
	pub(crate) summary: String,
	pub(crate) help: String,
	pub(crate) completion_candidates: Vec<String>,
	pub(crate) action: ActionLease,
}

#[derive(Debug, Clone)]
pub(crate) struct RuntimeSubscriptionRegistration {
	pub(crate) kinds: EventKindSet,
	pub(crate) action: ActionLease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum SubscriptionDeliveryState {
	NeedsBaseline { consecutive_failures: u8 },
	Live,
	Disabled,
}

#[derive(Debug, Clone)]
struct RuntimeSubscription {
	id: SubscriptionId,
	kinds: EventKindSet,
	action: ActionLease,
	state: SubscriptionDeliveryState,
}

#[derive(Debug, Clone)]
pub(crate) struct SubscriberInvocation {
	id: SubscriptionId,
	action: ActionLease,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct LiveSubscriptionSnapshot {
	pub(crate) id: SubscriptionId,
	pub(crate) kinds: EventKindSet,
}

impl SubscriberInvocation {
	pub(crate) const fn id(&self) -> SubscriptionId {
		self.id
	}

	pub(crate) const fn owner(&self) -> RegistryOwnerId {
		self.action.owner()
	}

	pub(crate) fn action<T: Any>(&self) -> Option<&T> {
		self.action.downcast_ref()
	}
}

/// Owned option metadata exposed to effective-catalog consumers.
///
/// As with [`RuntimeCommandInfo`], the action association is deliberately
/// absent so help, completion, and introspection cannot invoke the option.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct RuntimeOptionInfo {
	pub(crate) owner: RegistryOwnerId,
	pub(crate) precedence: u64,
	pub(crate) name: String,
	pub(crate) aliases: Vec<String>,
	pub(crate) kind: RuntimeOptionKind,
	pub(crate) usage: String,
	pub(crate) summary: String,
	pub(crate) help: String,
	pub(crate) completion_candidates: Vec<String>,
}

/// One owner's complete candidate declaration set.
///
/// Publishing a batch replaces that owner's previous declarations across all
/// four registries. The owned vectors make the commit an infallible swap after
/// validation; no partially-published cross-registry state is representable.
#[derive(Debug)]
pub(crate) struct RegistrationBatch {
	pub(crate) owner: RegistryOwnerId,
	pub(crate) commands: Vec<RuntimeCommandRegistration>,
	pub(crate) mappings: Vec<RuntimeMappingRegistration>,
	pub(crate) options: Vec<RuntimeOptionRegistration>,
	pub(crate) subscriptions: Vec<RuntimeSubscriptionRegistration>,
}

impl RegistrationBatch {
	pub(crate) fn new(owner: RegistryOwnerId) -> Self {
		Self {
			owner,
			commands: Vec::new(),
			mappings: Vec::new(),
			options: Vec::new(),
			subscriptions: Vec::new(),
		}
	}

	fn entry_count(&self) -> Result<usize, RegistryError> {
		self.commands
			.len()
			.checked_add(self.mappings.len())
			.and_then(|count| count.checked_add(self.options.len()))
			.and_then(|count| count.checked_add(self.subscriptions.len()))
			.ok_or(RegistryError::MetadataAccountingOverflow)
	}

	fn metadata_bytes(&self) -> Result<usize, RegistryError> {
		let mut total = 0usize;
		for command in &self.commands {
			total = checked_metadata_add(total, command.metadata_bytes()?)?;
		}
		for mapping in &self.mappings {
			total = checked_metadata_add(total, mapping.metadata_bytes()?)?;
		}
		for option in &self.options {
			total = checked_metadata_add(total, option.metadata_bytes()?)?;
		}
		Ok(total)
	}
}

impl RuntimeCommandRegistration {
	fn metadata_bytes(&self) -> Result<usize, RegistryError> {
		// Caps account for script-controlled owned metadata, not Vec/String
		// allocator bookkeeping or the executor-owned action payload.
		let mut total = string_list_bytes(&self.name, &self.aliases)?;
		total = checked_metadata_add(total, self.usage.len())?;
		total = checked_metadata_add(total, self.summary.len())?;
		total = checked_metadata_add(total, self.help.len())?;
		if let RuntimeCompletion::Static(candidates) = &self.completion {
			total = checked_metadata_add(total, strings_bytes(candidates)?)?;
		}
		Ok(total)
	}
}

impl RuntimeMappingRegistration {
	fn metadata_bytes(&self) -> Result<usize, RegistryError> {
		let mut total = self
			.sequence
			.len()
			.checked_mul(size_of::<KeyStroke>())
			.ok_or(RegistryError::MetadataAccountingOverflow)?;
		total = checked_metadata_add(total, self.summary.len())?;
		if let RuntimeMappingTarget::Command(command) = &self.target {
			total = checked_metadata_add(total, command.len())?;
		}
		Ok(total)
	}
}

impl RuntimeOptionRegistration {
	fn metadata_bytes(&self) -> Result<usize, RegistryError> {
		let mut total = string_list_bytes(&self.name, &self.aliases)?;
		total = checked_metadata_add(total, self.usage.len())?;
		total = checked_metadata_add(total, self.summary.len())?;
		total = checked_metadata_add(total, self.help.len())?;
		checked_metadata_add(total, strings_bytes(&self.completion_candidates)?)
	}
}

fn string_list_bytes(name: &str, aliases: &[String]) -> Result<usize, RegistryError> {
	checked_metadata_add(name.len(), strings_bytes(aliases)?)
}

fn strings_bytes(strings: &[String]) -> Result<usize, RegistryError> {
	strings.iter().try_fold(0usize, |total, value| {
		checked_metadata_add(total, value.len())
	})
}

fn checked_metadata_add(left: usize, right: usize) -> Result<usize, RegistryError> {
	left.checked_add(right)
		.ok_or(RegistryError::MetadataAccountingOverflow)
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegistryNamespace {
	Command,
	Option,
}

impl fmt::Display for RegistryNamespace {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::Command => f.write_str("command"),
			Self::Option => f.write_str("option"),
		}
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum RegistryOccupant {
	Builtin,
	Owner(RegistryOwnerId),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum RegistryError {
	OwnerIdsExhausted,
	OwnerPrecedenceExhausted,
	RegistryRevisionExhausted,
	SubscriptionIdsExhausted,
	UnknownOwner(RegistryOwnerId),
	OwnerCannotBeDiscarded(RegistryOwnerId),
	ReplacementOwnerIsNotFresh {
		retired: RegistryOwnerId,
		replacement: RegistryOwnerId,
	},
	TooManyTransactionEntries {
		actual: usize,
		limit: usize,
	},
	TooManySessionEntries {
		actual: usize,
		limit: usize,
	},
	TooManyOwnerSubscribers {
		actual: usize,
		limit: usize,
	},
	TooManySessionSubscribers {
		actual: usize,
		limit: usize,
	},
	TooMuchTransactionMetadata {
		actual: usize,
		limit: usize,
	},
	TooMuchSessionMetadata {
		actual: usize,
		limit: usize,
	},
	MetadataAccountingOverflow,
	InvalidName {
		field: &'static str,
		value: String,
	},
	TooManyAliases {
		actual: usize,
		limit: usize,
	},
	FieldTooLong {
		field: &'static str,
		actual: usize,
		limit: usize,
	},
	TooManyCompletionCandidates {
		actual: usize,
		limit: usize,
	},
	TooMuchCompletionMetadata {
		actual: usize,
		limit: usize,
	},
	ActionOwnerMismatch {
		batch_owner: RegistryOwnerId,
		action_owner: RegistryOwnerId,
	},
	EmptySubscriptionKinds,
	NameCollision {
		namespace: RegistryNamespace,
		token: String,
		occupant: RegistryOccupant,
	},
	EmptyMappingSequence,
	MappingSequenceTooLong {
		actual: usize,
		limit: usize,
	},
	UnsupportedMappingKey {
		index: usize,
	},
	NonCanonicalMappingModifiers {
		index: usize,
	},
	DuplicateMapping {
		owner: RegistryOwnerId,
		mode: MappingMode,
		sequence: Vec<KeyStroke>,
	},
	MappingPrefixConflict {
		mode: MappingMode,
		prefix: Vec<KeyStroke>,
		prefix_occupant: RegistryOccupant,
		sequence: Vec<KeyStroke>,
		sequence_occupant: RegistryOccupant,
	},
}

impl fmt::Display for RegistryError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::OwnerIdsExhausted => f.write_str("Runtime registry owner IDs are exhausted"),
			Self::OwnerPrecedenceExhausted => {
				f.write_str("Runtime registry owner precedence slots are exhausted")
			}
			Self::RegistryRevisionExhausted => {
				f.write_str("Runtime registry revisions are exhausted")
			}
			Self::SubscriptionIdsExhausted => f.write_str("Runtime subscription IDs are exhausted"),
			Self::UnknownOwner(owner) => write!(f, "Unknown runtime registry owner {owner}"),
			Self::OwnerCannotBeDiscarded(owner) => write!(
				f,
				"Runtime registry owner {owner} was published or is not empty"
			),
			Self::ReplacementOwnerIsNotFresh {
				retired,
				replacement,
			} => write!(
				f,
				"Replacement owner {replacement} must be newer than retired owner {retired}"
			),
			Self::TooManyTransactionEntries { actual, limit } => write!(
				f,
				"Owner transaction has {actual} entries; the limit is {limit}"
			),
			Self::TooManySessionEntries { actual, limit } => write!(
				f,
				"Runtime registry would have {actual} entries; the session limit is {limit}"
			),
			Self::TooManyOwnerSubscribers { actual, limit } => write!(
				f,
				"Owner transaction has {actual} event subscribers; the limit is {limit}"
			),
			Self::TooManySessionSubscribers { actual, limit } => write!(
				f,
				"Runtime registry would have {actual} event subscribers; the session limit is {limit}"
			),
			Self::TooMuchTransactionMetadata { actual, limit } => write!(
				f,
				"Owner transaction has {actual} metadata bytes; the limit is {limit}"
			),
			Self::TooMuchSessionMetadata { actual, limit } => write!(
				f,
				"Runtime registry would have {actual} metadata bytes; the session limit is {limit}"
			),
			Self::MetadataAccountingOverflow => {
				f.write_str("Runtime registry metadata accounting overflowed")
			}
			Self::InvalidName { field, value } => {
				write!(f, "Invalid runtime {field} {value:?}")
			}
			Self::TooManyAliases { actual, limit } => {
				write!(f, "Entry has {actual} aliases; the limit is {limit}")
			}
			Self::FieldTooLong {
				field,
				actual,
				limit,
			} => write!(
				f,
				"Runtime {field} has {actual} bytes; the limit is {limit}"
			),
			Self::TooManyCompletionCandidates { actual, limit } => write!(
				f,
				"Entry has {actual} completion candidates; the limit is {limit}"
			),
			Self::TooMuchCompletionMetadata { actual, limit } => write!(
				f,
				"Entry has {actual} completion bytes; the limit is {limit}"
			),
			Self::ActionOwnerMismatch {
				batch_owner,
				action_owner,
			} => write!(
				f,
				"Owner {batch_owner} cannot register an action belonging to owner {action_owner}"
			),
			Self::EmptySubscriptionKinds => {
				f.write_str("Runtime event subscription must select at least one event kind")
			}
			Self::NameCollision {
				namespace,
				token,
				occupant,
			} => match occupant {
				RegistryOccupant::Builtin => {
					write!(
						f,
						"Runtime {namespace} name {token:?} collides with a builtin"
					)
				}
				RegistryOccupant::Owner(owner) => write!(
					f,
					"Runtime {namespace} name {token:?} is already owned by {owner}"
				),
			},
			Self::EmptyMappingSequence => f.write_str("Runtime mapping sequence cannot be empty"),
			Self::MappingSequenceTooLong { actual, limit } => write!(
				f,
				"Runtime mapping has {actual} strokes; the limit is {limit}"
			),
			Self::UnsupportedMappingKey { index } => {
				write!(f, "Runtime mapping stroke {index} is unsupported")
			}
			Self::NonCanonicalMappingModifiers { index } => write!(
				f,
				"Runtime mapping stroke {index} has non-canonical character modifiers"
			),
			Self::DuplicateMapping {
				owner,
				mode,
				sequence,
			} => write!(
				f,
				"Owner {owner} registered duplicate {mode:?} mapping {sequence:?}"
			),
			Self::MappingPrefixConflict {
				mode,
				prefix,
				prefix_occupant,
				sequence,
				sequence_occupant,
			} => write!(
				f,
				"{mode:?} mapping {prefix:?} ({prefix_occupant:?}) is a prefix of {sequence:?} ({sequence_occupant:?})"
			),
		}
	}
}

impl std::error::Error for RegistryError {}

#[derive(Default)]
struct OwnerEntries {
	commands: Vec<RuntimeCommandRegistration>,
	mappings: Vec<RuntimeMappingRegistration>,
	options: Vec<RuntimeOptionRegistration>,
	subscriptions: Vec<RuntimeSubscription>,
	metadata_bytes: usize,
}

impl OwnerEntries {
	fn entry_count(&self) -> usize {
		self.commands.len() + self.mappings.len() + self.options.len() + self.subscriptions.len()
	}
}

fn owner_entries_from_batch(
	batch: RegistrationBatch,
	first_subscription_id: SubscriptionId,
	metadata_bytes: usize,
) -> OwnerEntries {
	let RegistrationBatch {
		owner: _,
		commands,
		mappings,
		options,
		subscriptions,
	} = batch;
	let mut next_id = first_subscription_id;
	let subscriptions = subscriptions
		.into_iter()
		.map(|registration| {
			let id = next_id;
			next_id = next_id.next();
			RuntimeSubscription {
				id,
				kinds: registration.kinds,
				action: registration.action,
				state: SubscriptionDeliveryState::NeedsBaseline {
					consecutive_failures: 0,
				},
			}
		})
		.collect();
	OwnerEntries {
		commands,
		mappings,
		options,
		subscriptions,
		metadata_bytes,
	}
}

struct OwnerState {
	precedence: u64,
	entries: OwnerEntries,
	published: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct RuntimeRegistrySummary {
	pub(crate) revision: RegistryRev,
	pub(crate) owner_count: usize,
	pub(crate) command_count: usize,
	pub(crate) mapping_count: usize,
	pub(crate) option_count: usize,
	pub(crate) subscription_count: usize,
	pub(crate) metadata_bytes: usize,
}

pub(crate) struct RuntimeRegistries {
	revision: RegistryRev,
	next_owner_id: RegistryOwnerId,
	next_subscription_id: SubscriptionId,
	next_precedence: u64,
	owners: BTreeMap<RegistryOwnerId, OwnerState>,
	entry_count: usize,
	metadata_bytes: usize,
}

impl Default for RuntimeRegistries {
	fn default() -> Self {
		Self::new()
	}
}

impl RuntimeRegistries {
	pub(crate) fn new() -> Self {
		Self {
			revision: RegistryRev::ZERO,
			next_owner_id: RegistryOwnerId::new(1),
			next_subscription_id: SubscriptionId::new(1),
			next_precedence: 0,
			owners: BTreeMap::new(),
			entry_count: 0,
			metadata_bytes: 0,
		}
	}

	pub(crate) const fn revision(&self) -> RegistryRev {
		self.revision
	}

	pub(crate) fn allocate_owner(&mut self) -> Result<RegistryOwnerId, RegistryError> {
		if self.next_owner_id.get() == u64::MAX {
			return Err(RegistryError::OwnerIdsExhausted);
		}
		if self.next_precedence == u64::MAX {
			return Err(RegistryError::OwnerPrecedenceExhausted);
		}
		let owner = self.next_owner_id;
		// Registry owners stop below u64::MAX because that value is reserved for
		// engine-owned JobRuntime consumers such as the Git status producer.
		debug_assert!(owner.get() < u64::MAX);
		self.next_owner_id = self.next_owner_id.next();
		let previous = self.owners.insert(
			owner,
			OwnerState {
				precedence: self.next_precedence,
				entries: OwnerEntries::default(),
				published: false,
			},
		);
		debug_assert!(previous.is_none(), "monotonic owner ID must be vacant");
		self.next_precedence += 1;
		Ok(owner)
	}

	pub(crate) fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
		self.owners.contains_key(&owner)
	}

	pub(crate) fn owner_precedence(&self, owner: RegistryOwnerId) -> Option<u64> {
		self.owners.get(&owner).map(|state| state.precedence)
	}

	/// Remove a candidate that has never participated in catalog publication.
	///
	/// This is revision-neutral because unpublished empty owners have never
	/// affected the effective catalog. IDs remain monotonic and are not reused.
	pub(crate) fn discard_unpublished_empty_owner(
		&mut self,
		owner: RegistryOwnerId,
	) -> Result<(), RegistryError> {
		let Some(state) = self.owners.get(&owner) else {
			return Err(RegistryError::UnknownOwner(owner));
		};
		if state.published || state.entries.entry_count() != 0 {
			return Err(RegistryError::OwnerCannotBeDiscarded(owner));
		}
		self.owners
			.remove(&owner)
			.expect("preflighted unpublished owner must remain live");
		Ok(())
	}

	/// Retire one owner and all of its declarations as a single effective-view
	/// change. Once the revision preflight succeeds, cleanup cannot fail.
	pub(crate) fn remove_owner(
		&mut self,
		owner: RegistryOwnerId,
	) -> Result<RegistryRev, RegistryError> {
		let Some(state) = self.owners.get(&owner) else {
			return Err(RegistryError::UnknownOwner(owner));
		};
		let next_revision = self.next_revision()?;
		let next_entry_count = self
			.entry_count
			.checked_sub(state.entries.entry_count())
			.expect("registry entry accounting must include every live owner");
		let next_metadata_bytes = self
			.metadata_bytes
			.checked_sub(state.entries.metadata_bytes)
			.expect("registry metadata accounting must include every live owner");

		self.owners
			.remove(&owner)
			.expect("preflighted owner must remain live");
		self.entry_count = next_entry_count;
		self.metadata_bytes = next_metadata_bytes;
		self.revision = next_revision;
		Ok(next_revision)
	}

	/// Publish a fresh source generation while retiring its predecessor.
	///
	/// The replacement owner is allocated before source evaluation so its action
	/// leases can be staged in `batch`. Validation observes the candidate state
	/// after removing both owners. A successful commit inherits the retired
	/// generation's precedence slot; failure changes neither generation.
	pub(crate) fn replace_owner_generation(
		&mut self,
		retired_owner: RegistryOwnerId,
		batch: RegistrationBatch,
	) -> Result<RegistryRev, RegistryError> {
		let Some(retired) = self.owners.get(&retired_owner) else {
			return Err(RegistryError::UnknownOwner(retired_owner));
		};
		if batch.owner.get() <= retired_owner.get() {
			return Err(RegistryError::ReplacementOwnerIsNotFresh {
				retired: retired_owner,
				replacement: batch.owner,
			});
		}
		if !self.owners.contains_key(&batch.owner) {
			return Err(RegistryError::UnknownOwner(batch.owner));
		}

		let retired_precedence = retired.precedence;
		let replaced_owners = [retired_owner, batch.owner];
		let metrics = self.validate_batch(&batch, &replaced_owners)?;
		let next_revision = self.next_revision()?;
		let replacement_owner = batch.owner;
		let entries = owner_entries_from_batch(
			batch,
			self.next_subscription_id,
			metrics.transaction_metadata,
		);

		self.owners
			.remove(&retired_owner)
			.expect("preflighted retired owner must remain live");
		let replacement = self
			.owners
			.get_mut(&replacement_owner)
			.expect("preflighted replacement owner must remain live");
		replacement.precedence = retired_precedence;
		replacement.entries = entries;
		replacement.published = true;
		self.entry_count = metrics.session_entries;
		self.metadata_bytes = metrics.session_metadata;
		self.next_subscription_id = metrics.next_subscription_id;
		self.revision = next_revision;
		Ok(next_revision)
	}

	pub(crate) fn summary(&self) -> RuntimeRegistrySummary {
		RuntimeRegistrySummary {
			revision: self.revision,
			owner_count: self.owners.len(),
			command_count: self
				.owners
				.values()
				.map(|state| state.entries.commands.len())
				.sum(),
			mapping_count: self
				.owners
				.values()
				.map(|state| state.entries.mappings.len())
				.sum(),
			option_count: self
				.owners
				.values()
				.map(|state| state.entries.options.len())
				.sum(),
			subscription_count: self.subscription_count(),
			metadata_bytes: self.metadata_bytes,
		}
	}

	pub(crate) fn commit_owner_batch(
		&mut self,
		batch: RegistrationBatch,
	) -> Result<RegistryRev, RegistryError> {
		let Some(current_entries) = self
			.owners
			.get(&batch.owner)
			.map(|state| state.entries.entry_count())
		else {
			return Err(RegistryError::UnknownOwner(batch.owner));
		};
		let replaced_owners = [batch.owner];
		let metrics = self.validate_batch(&batch, &replaced_owners)?;
		if current_entries == 0 && metrics.transaction_entries == 0 {
			self.owners
				.get_mut(&batch.owner)
				.expect("validated owner must remain live")
				.published = true;
			return Ok(self.revision);
		}
		let next_revision = self.next_revision()?;

		let owner = batch.owner;
		let entries = owner_entries_from_batch(
			batch,
			self.next_subscription_id,
			metrics.transaction_metadata,
		);
		let state = self
			.owners
			.get_mut(&owner)
			.expect("validated owner must remain live");
		state.entries = entries;
		state.published = true;
		self.entry_count = metrics.session_entries;
		self.metadata_bytes = metrics.session_metadata;
		self.next_subscription_id = metrics.next_subscription_id;
		self.revision = next_revision;
		Ok(self.revision)
	}

	fn next_revision(&self) -> Result<RegistryRev, RegistryError> {
		if self.revision.get() == u64::MAX {
			return Err(RegistryError::RegistryRevisionExhausted);
		}
		Ok(self.revision.next())
	}

	fn subscription_id_after(&self, count: usize) -> Result<SubscriptionId, RegistryError> {
		let count = u64::try_from(count).map_err(|_| RegistryError::SubscriptionIdsExhausted)?;
		self.next_subscription_id
			.get()
			.checked_add(count)
			.map(SubscriptionId::new)
			.ok_or(RegistryError::SubscriptionIdsExhausted)
	}

	pub(crate) fn subscription_count(&self) -> usize {
		self.owners
			.values()
			.map(|state| state.entries.subscriptions.len())
			.sum()
	}

	pub(crate) fn has_needs_baseline(&self) -> bool {
		self.owners.values().any(|owner| {
			owner.entries.subscriptions.iter().any(|subscription| {
				matches!(
					subscription.state,
					SubscriptionDeliveryState::NeedsBaseline { .. }
				)
			})
		})
	}

	pub(crate) fn snapshot_subscriptions(
		&self,
		live: &mut Vec<LiveSubscriptionSnapshot>,
		needs_baseline: &mut Vec<SubscriptionId>,
	) {
		live.clear();
		needs_baseline.clear();
		for subscription in self
			.owners
			.values()
			.flat_map(|owner| &owner.entries.subscriptions)
		{
			match subscription.state {
				SubscriptionDeliveryState::NeedsBaseline { .. } => {
					needs_baseline.push(subscription.id);
				}
				SubscriptionDeliveryState::Live => live.push(LiveSubscriptionSnapshot {
					id: subscription.id,
					kinds: subscription.kinds,
				}),
				SubscriptionDeliveryState::Disabled => {}
			}
		}
		live.sort_unstable_by_key(|snapshot| snapshot.id);
		needs_baseline.sort_unstable();
	}

	pub(crate) fn live_subscription_invocation(
		&self,
		id: SubscriptionId,
	) -> Option<SubscriberInvocation> {
		let subscription = self.subscription(id)?;
		matches!(subscription.state, SubscriptionDeliveryState::Live).then(|| {
			SubscriberInvocation {
				id,
				action: subscription.action.clone(),
			}
		})
	}

	pub(crate) fn baseline_subscription_invocation(
		&self,
		id: SubscriptionId,
	) -> Option<SubscriberInvocation> {
		let subscription = self.subscription(id)?;
		matches!(
			subscription.state,
			SubscriptionDeliveryState::NeedsBaseline { .. }
		)
		.then(|| SubscriberInvocation {
			id,
			action: subscription.action.clone(),
		})
	}

	pub(crate) fn mark_subscription_live(&mut self, id: SubscriptionId) -> bool {
		let Some(subscription) = self.subscription_mut(id) else {
			return false;
		};
		if !matches!(
			subscription.state,
			SubscriptionDeliveryState::NeedsBaseline { .. }
		) {
			return false;
		}
		subscription.state = SubscriptionDeliveryState::Live;
		true
	}

	pub(crate) fn record_subscription_baseline_failure(
		&mut self,
		id: SubscriptionId,
		max_attempts: u8,
	) -> Option<SubscriptionDeliveryState> {
		let subscription = self.subscription_mut(id)?;
		let SubscriptionDeliveryState::NeedsBaseline {
			consecutive_failures,
		} = subscription.state
		else {
			return Some(subscription.state);
		};
		let failures = consecutive_failures.saturating_add(1);
		subscription.state = if failures >= max_attempts {
			SubscriptionDeliveryState::Disabled
		} else {
			SubscriptionDeliveryState::NeedsBaseline {
				consecutive_failures: failures,
			}
		};
		Some(subscription.state)
	}

	pub(crate) fn demote_live_subscriptions(&mut self) {
		for subscription in self
			.owners
			.values_mut()
			.flat_map(|owner| &mut owner.entries.subscriptions)
		{
			if matches!(subscription.state, SubscriptionDeliveryState::Live) {
				subscription.state = SubscriptionDeliveryState::NeedsBaseline {
					consecutive_failures: 0,
				};
			}
		}
	}

	pub(crate) fn subscription_state(
		&self,
		id: SubscriptionId,
	) -> Option<SubscriptionDeliveryState> {
		self.subscription(id).map(|subscription| subscription.state)
	}

	pub(crate) fn subscription_is_owned_by(
		&self,
		id: SubscriptionId,
		owner: RegistryOwnerId,
	) -> bool {
		self.subscription(id)
			.is_some_and(|subscription| subscription.action.owner() == owner)
	}

	fn subscription(&self, id: SubscriptionId) -> Option<&RuntimeSubscription> {
		self.owners
			.values()
			.flat_map(|owner| &owner.entries.subscriptions)
			.find(|subscription| subscription.id == id)
	}

	fn subscription_mut(&mut self, id: SubscriptionId) -> Option<&mut RuntimeSubscription> {
		self.owners
			.values_mut()
			.flat_map(|owner| &mut owner.entries.subscriptions)
			.find(|subscription| subscription.id == id)
	}

	pub(crate) fn command_invocation(
		&self,
		spelling: &str,
		args: &str,
		bang: bool,
	) -> Option<InvocationSnapshot> {
		self.owners
			.values()
			.flat_map(|state| &state.entries.commands)
			.find(|command| {
				command.name == spelling || command.aliases.iter().any(|a| a == spelling)
			})
			.map(|command| {
				InvocationSnapshot::new(
					self.revision,
					command.action.clone(),
					DynamicInvocation::Command {
						canonical_name: command.name.clone(),
						spelling: spelling.to_string(),
						args: args.to_string(),
						bang,
					},
				)
			})
	}

	pub(crate) fn command(&self, spelling: &str) -> Option<&RuntimeCommandRegistration> {
		self.owners
			.values()
			.flat_map(|state| &state.entries.commands)
			.find(|command| {
				command.name == spelling || command.aliases.iter().any(|alias| alias == spelling)
			})
	}

	pub(crate) fn option(&self, spelling: &str) -> Option<(&RuntimeOptionRegistration, bool)> {
		self.owners
			.values()
			.flat_map(|state| &state.entries.options)
			.find_map(|option| {
				if option.name == spelling || option.aliases.iter().any(|alias| alias == spelling) {
					return Some((option, false));
				}
				if option.kind == RuntimeOptionKind::Bool
					&& let Some(positive) = spelling.strip_prefix("no")
					&& (option.name == positive
						|| option.aliases.iter().any(|alias| alias == positive))
				{
					return Some((option, true));
				}
				None
			})
	}

	pub(crate) fn option_invocation(
		&self,
		spelling: &str,
		operation: OptionInvocation,
	) -> Option<InvocationSnapshot> {
		let (option, _) = self.option(spelling)?;
		Some(InvocationSnapshot::new(
			self.revision,
			option.action.clone(),
			DynamicInvocation::Option {
				canonical_name: option.name.clone(),
				operation,
			},
		))
	}

	pub(crate) fn command_infos(&self) -> Vec<RuntimeCommandInfo> {
		let mut commands = self
			.commands()
			.map(|(owner, precedence, command)| RuntimeCommandInfo {
				owner,
				precedence,
				name: command.name.clone(),
				aliases: command.aliases.clone(),
				bang: command.bang,
				args: command.args,
				usage: command.usage.clone(),
				summary: command.summary.clone(),
				help: command.help.clone(),
				completion: command.completion.clone(),
			})
			.collect::<Vec<_>>();
		commands.sort_by(|left, right| left.name.cmp(&right.name));
		commands
	}

	pub(crate) fn option_infos(&self) -> Vec<RuntimeOptionInfo> {
		let mut options = self
			.options()
			.map(|(owner, precedence, option)| RuntimeOptionInfo {
				owner,
				precedence,
				name: option.name.clone(),
				aliases: option.aliases.clone(),
				kind: option.kind,
				usage: option.usage.clone(),
				summary: option.summary.clone(),
				help: option.help.clone(),
				completion_candidates: option.completion_candidates.clone(),
			})
			.collect::<Vec<_>>();
		options.sort_by(|left, right| left.name.cmp(&right.name));
		options
	}

	pub(crate) fn mapping_infos(&self) -> Vec<RuntimeMappingInfo> {
		let mut effective = BTreeMap::new();
		for (owner, precedence, mapping) in self.mappings() {
			let key = (mapping.mode, mapping.sequence.clone());
			let replace = effective
				.get(&key)
				.is_none_or(|current: &RuntimeMappingInfo| current.precedence < precedence);
			if replace {
				effective.insert(
					key,
					RuntimeMappingInfo {
						owner,
						precedence,
						mode: mapping.mode,
						sequence: mapping.sequence.clone(),
						summary: mapping.summary.clone(),
						target: match &mapping.target {
							RuntimeMappingTarget::Command(_) => RuntimeMappingTargetKind::Command,
							RuntimeMappingTarget::Action(_) => RuntimeMappingTargetKind::Action,
						},
					},
				);
			}
		}
		effective.into_values().collect()
	}

	pub(crate) fn mapping_target(
		&self,
		mode: MappingMode,
		sequence: &[KeyStroke],
	) -> Option<ResolvedMappingTarget> {
		let (_, _, mapping) = self
			.mappings()
			.filter(|(_, _, mapping)| mapping.mode == mode && mapping.sequence == sequence)
			.max_by_key(|(_, precedence, _)| *precedence)?;
		Some(match &mapping.target {
			RuntimeMappingTarget::Command(command) => {
				ResolvedMappingTarget::Command(command.clone())
			}
			RuntimeMappingTarget::Action(action) => {
				ResolvedMappingTarget::Action(InvocationSnapshot::new(
					self.revision,
					action.clone(),
					DynamicInvocation::Mapping {
						mode,
						sequence: sequence.to_vec(),
					},
				))
			}
		})
	}

	pub(crate) fn commands(
		&self,
	) -> impl Iterator<Item = (RegistryOwnerId, u64, &RuntimeCommandRegistration)> {
		self.owners.iter().flat_map(|(owner, state)| {
			state
				.entries
				.commands
				.iter()
				.map(move |command| (*owner, state.precedence, command))
		})
	}

	pub(crate) fn mappings(
		&self,
	) -> impl Iterator<Item = (RegistryOwnerId, u64, &RuntimeMappingRegistration)> {
		self.owners.iter().flat_map(|(owner, state)| {
			state
				.entries
				.mappings
				.iter()
				.map(move |mapping| (*owner, state.precedence, mapping))
		})
	}

	pub(crate) fn options(
		&self,
	) -> impl Iterator<Item = (RegistryOwnerId, u64, &RuntimeOptionRegistration)> {
		self.owners.iter().flat_map(|(owner, state)| {
			state
				.entries
				.options
				.iter()
				.map(move |option| (*owner, state.precedence, option))
		})
	}

	fn validate_batch(
		&self,
		batch: &RegistrationBatch,
		replaced_owners: &[RegistryOwnerId],
	) -> Result<BatchMetrics, RegistryError> {
		let transaction_entries = batch.entry_count()?;
		if transaction_entries > MAX_TRANSACTION_ENTRIES {
			return Err(RegistryError::TooManyTransactionEntries {
				actual: transaction_entries,
				limit: MAX_TRANSACTION_ENTRIES,
			});
		}

		for command in &batch.commands {
			validate_command(batch.owner, command)?;
		}
		for mapping in &batch.mappings {
			validate_mapping(batch.owner, mapping)?;
		}
		for option in &batch.options {
			validate_option(batch.owner, option)?;
		}
		if batch.subscriptions.len() > MAX_SUBSCRIBERS_PER_OWNER {
			return Err(RegistryError::TooManyOwnerSubscribers {
				actual: batch.subscriptions.len(),
				limit: MAX_SUBSCRIBERS_PER_OWNER,
			});
		}
		for subscription in &batch.subscriptions {
			validate_subscription(batch.owner, subscription)?;
		}

		let transaction_metadata = batch.metadata_bytes()?;
		if transaction_metadata > MAX_TRANSACTION_METADATA_BYTES {
			return Err(RegistryError::TooMuchTransactionMetadata {
				actual: transaction_metadata,
				limit: MAX_TRANSACTION_METADATA_BYTES,
			});
		}

		let removed_entries = replaced_owners.iter().try_fold(0usize, |total, owner| {
			let state = self
				.owners
				.get(owner)
				.ok_or(RegistryError::UnknownOwner(*owner))?;
			total
				.checked_add(state.entries.entry_count())
				.ok_or(RegistryError::MetadataAccountingOverflow)
		})?;
		let removed_metadata = replaced_owners.iter().try_fold(0usize, |total, owner| {
			let state = self
				.owners
				.get(owner)
				.ok_or(RegistryError::UnknownOwner(*owner))?;
			total
				.checked_add(state.entries.metadata_bytes)
				.ok_or(RegistryError::MetadataAccountingOverflow)
		})?;
		let session_entries = self
			.entry_count
			.checked_sub(removed_entries)
			.and_then(|count| count.checked_add(transaction_entries))
			.ok_or(RegistryError::MetadataAccountingOverflow)?;
		if session_entries > MAX_SESSION_ENTRIES {
			return Err(RegistryError::TooManySessionEntries {
				actual: session_entries,
				limit: MAX_SESSION_ENTRIES,
			});
		}
		let removed_subscribers = replaced_owners.iter().try_fold(0usize, |total, owner| {
			let state = self
				.owners
				.get(owner)
				.ok_or(RegistryError::UnknownOwner(*owner))?;
			total
				.checked_add(state.entries.subscriptions.len())
				.ok_or(RegistryError::MetadataAccountingOverflow)
		})?;
		let session_subscribers = self
			.subscription_count()
			.checked_sub(removed_subscribers)
			.and_then(|count| count.checked_add(batch.subscriptions.len()))
			.ok_or(RegistryError::MetadataAccountingOverflow)?;
		if session_subscribers > MAX_SUBSCRIBERS_TOTAL {
			return Err(RegistryError::TooManySessionSubscribers {
				actual: session_subscribers,
				limit: MAX_SUBSCRIBERS_TOTAL,
			});
		}
		let session_metadata = self
			.metadata_bytes
			.checked_sub(removed_metadata)
			.and_then(|bytes| bytes.checked_add(transaction_metadata))
			.ok_or(RegistryError::MetadataAccountingOverflow)?;
		if session_metadata > MAX_SESSION_METADATA_BYTES {
			return Err(RegistryError::TooMuchSessionMetadata {
				actual: session_metadata,
				limit: MAX_SESSION_METADATA_BYTES,
			});
		}

		self.validate_command_namespace(batch, replaced_owners)?;
		self.validate_option_namespace(batch, replaced_owners)?;
		self.validate_runtime_mapping_prefixes(batch, replaced_owners)?;
		super::input::runtime_keymap::validate_builtin_compatibility(batch.owner, &batch.mappings)?;
		let next_subscription_id = self.subscription_id_after(batch.subscriptions.len())?;

		Ok(BatchMetrics {
			transaction_entries,
			transaction_metadata,
			session_entries,
			session_metadata,
			next_subscription_id,
		})
	}

	fn validate_command_namespace(
		&self,
		batch: &RegistrationBatch,
		replaced_owners: &[RegistryOwnerId],
	) -> Result<(), RegistryError> {
		let mut occupied = HashMap::new();
		for token in super::input::protected_runtime_command_tokens() {
			occupied.insert(token, RegistryOccupant::Builtin);
		}
		for (owner, _, command) in self
			.commands()
			.filter(|(owner, _, _)| !replaced_owners.contains(owner))
		{
			insert_name(
				&mut occupied,
				RegistryNamespace::Command,
				&command.name,
				RegistryOccupant::Owner(owner),
			)?;
			for alias in &command.aliases {
				insert_name(
					&mut occupied,
					RegistryNamespace::Command,
					alias,
					RegistryOccupant::Owner(owner),
				)?;
			}
		}
		for command in &batch.commands {
			insert_name(
				&mut occupied,
				RegistryNamespace::Command,
				&command.name,
				RegistryOccupant::Owner(batch.owner),
			)?;
			for alias in &command.aliases {
				insert_name(
					&mut occupied,
					RegistryNamespace::Command,
					alias,
					RegistryOccupant::Owner(batch.owner),
				)?;
			}
		}
		Ok(())
	}

	fn validate_option_namespace(
		&self,
		batch: &RegistrationBatch,
		replaced_owners: &[RegistryOwnerId],
	) -> Result<(), RegistryError> {
		let mut occupied = HashMap::new();
		for token in super::input::protected_runtime_option_tokens() {
			occupied.insert(token, RegistryOccupant::Builtin);
		}
		for (owner, _, option) in self
			.options()
			.filter(|(owner, _, _)| !replaced_owners.contains(owner))
		{
			for token in option_tokens(option) {
				insert_name(
					&mut occupied,
					RegistryNamespace::Option,
					&token,
					RegistryOccupant::Owner(owner),
				)?;
			}
		}
		for option in &batch.options {
			for token in option_tokens(option) {
				insert_name(
					&mut occupied,
					RegistryNamespace::Option,
					&token,
					RegistryOccupant::Owner(batch.owner),
				)?;
			}
		}
		Ok(())
	}

	fn validate_runtime_mapping_prefixes(
		&self,
		batch: &RegistrationBatch,
		replaced_owners: &[RegistryOwnerId],
	) -> Result<(), RegistryError> {
		let mut terminals: HashMap<MappingMode, HashMap<Vec<KeyStroke>, Vec<RegistryOwnerId>>> =
			HashMap::new();
		let existing = self
			.mappings()
			.filter(|(owner, _, _)| !replaced_owners.contains(owner))
			.map(|(owner, _, mapping)| (owner, mapping));
		let candidate = batch.mappings.iter().map(|mapping| (batch.owner, mapping));
		for (owner, mapping) in existing.chain(candidate) {
			let owners = terminals
				.entry(mapping.mode)
				.or_default()
				.entry(mapping.sequence.clone())
				.or_default();
			if owners.contains(&owner) {
				return Err(RegistryError::DuplicateMapping {
					owner,
					mode: mapping.mode,
					sequence: mapping.sequence.clone(),
				});
			}
			owners.push(owner);
		}
		for (mode, mode_terminals) in &terminals {
			for sequence in mode_terminals.keys() {
				for prefix_len in 1..sequence.len() {
					let prefix = &sequence[..prefix_len];
					if let Some(prefix_owners) = mode_terminals.get(prefix) {
						return Err(RegistryError::MappingPrefixConflict {
							mode: *mode,
							prefix: prefix.to_vec(),
							prefix_occupant: RegistryOccupant::Owner(prefix_owners[0]),
							sequence: sequence.clone(),
							sequence_occupant: RegistryOccupant::Owner(mode_terminals[sequence][0]),
						});
					}
				}
			}
		}
		Ok(())
	}
}

struct BatchMetrics {
	transaction_entries: usize,
	transaction_metadata: usize,
	session_entries: usize,
	session_metadata: usize,
	next_subscription_id: SubscriptionId,
}

fn insert_name(
	occupied: &mut HashMap<String, RegistryOccupant>,
	namespace: RegistryNamespace,
	token: &str,
	occupant: RegistryOccupant,
) -> Result<(), RegistryError> {
	if let Some(existing) = occupied.get(token) {
		return Err(RegistryError::NameCollision {
			namespace,
			token: token.to_string(),
			occupant: *existing,
		});
	}
	occupied.insert(token.to_string(), occupant);
	Ok(())
}

fn option_tokens(option: &RuntimeOptionRegistration) -> Vec<String> {
	let mut tokens = Vec::with_capacity(
		(option.aliases.len() + 1)
			* if option.kind == RuntimeOptionKind::Bool {
				2
			} else {
				1
			},
	);
	tokens.push(option.name.clone());
	tokens.extend(option.aliases.iter().cloned());
	if option.kind == RuntimeOptionKind::Bool {
		tokens.push(format!("no{}", option.name));
		tokens.extend(option.aliases.iter().map(|alias| format!("no{alias}")));
	}
	tokens
}

fn validate_command(
	owner: RegistryOwnerId,
	command: &RuntimeCommandRegistration,
) -> Result<(), RegistryError> {
	validate_name("command name", &command.name)?;
	validate_aliases(&command.aliases)?;
	validate_bounded_field("command usage", &command.usage, MAX_SUMMARY_OR_USAGE_BYTES)?;
	validate_bounded_field(
		"command summary",
		&command.summary,
		MAX_SUMMARY_OR_USAGE_BYTES,
	)?;
	validate_bounded_field("command help", &command.help, MAX_HELP_BYTES)?;
	if let RuntimeCompletion::Static(candidates) = &command.completion {
		validate_completion_candidates(candidates)?;
	}
	validate_action_owner(owner, &command.action)
}

fn validate_mapping(
	owner: RegistryOwnerId,
	mapping: &RuntimeMappingRegistration,
) -> Result<(), RegistryError> {
	if mapping.sequence.is_empty() {
		return Err(RegistryError::EmptyMappingSequence);
	}
	if mapping.sequence.len() > MAX_MAPPING_SEQUENCE {
		return Err(RegistryError::MappingSequenceTooLong {
			actual: mapping.sequence.len(),
			limit: MAX_MAPPING_SEQUENCE,
		});
	}
	for (index, stroke) in mapping.sequence.iter().enumerate() {
		if matches!(stroke.code, KeyCode::Unsupported) {
			return Err(RegistryError::UnsupportedMappingKey { index });
		}
		if matches!(stroke.code, KeyCode::Char(_)) && stroke.modifiers.contains(Modifiers::SHIFT) {
			return Err(RegistryError::NonCanonicalMappingModifiers { index });
		}
	}
	validate_bounded_field(
		"mapping summary",
		&mapping.summary,
		MAX_SUMMARY_OR_USAGE_BYTES,
	)?;
	if let RuntimeMappingTarget::Action(action) = &mapping.target {
		validate_action_owner(owner, action)?;
	}
	Ok(())
}

fn validate_option(
	owner: RegistryOwnerId,
	option: &RuntimeOptionRegistration,
) -> Result<(), RegistryError> {
	validate_name("option name", &option.name)?;
	validate_aliases(&option.aliases)?;
	validate_bounded_field("option usage", &option.usage, MAX_SUMMARY_OR_USAGE_BYTES)?;
	validate_bounded_field(
		"option summary",
		&option.summary,
		MAX_SUMMARY_OR_USAGE_BYTES,
	)?;
	validate_bounded_field("option help", &option.help, MAX_HELP_BYTES)?;
	validate_completion_candidates(&option.completion_candidates)?;
	validate_action_owner(owner, &option.action)
}

fn validate_subscription(
	owner: RegistryOwnerId,
	subscription: &RuntimeSubscriptionRegistration,
) -> Result<(), RegistryError> {
	if subscription.kinds.is_empty() {
		return Err(RegistryError::EmptySubscriptionKinds);
	}
	validate_action_owner(owner, &subscription.action)
}

fn validate_name(field: &'static str, value: &str) -> Result<(), RegistryError> {
	if value.len() > MAX_NAME_BYTES {
		return Err(RegistryError::FieldTooLong {
			field,
			actual: value.len(),
			limit: MAX_NAME_BYTES,
		});
	}
	let mut bytes = value.bytes();
	let valid = bytes.next().is_some_and(|byte| byte.is_ascii_alphabetic())
		&& bytes.all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'_' | b'-' | b'.'));
	if !valid {
		return Err(RegistryError::InvalidName {
			field,
			value: value.to_string(),
		});
	}
	Ok(())
}

fn validate_aliases(aliases: &[String]) -> Result<(), RegistryError> {
	if aliases.len() > MAX_ALIASES {
		return Err(RegistryError::TooManyAliases {
			actual: aliases.len(),
			limit: MAX_ALIASES,
		});
	}
	for alias in aliases {
		validate_name("alias", alias)?;
	}
	Ok(())
}

fn validate_bounded_field(
	field: &'static str,
	value: &str,
	limit: usize,
) -> Result<(), RegistryError> {
	if value.len() > limit {
		return Err(RegistryError::FieldTooLong {
			field,
			actual: value.len(),
			limit,
		});
	}
	Ok(())
}

fn validate_completion_candidates(candidates: &[String]) -> Result<(), RegistryError> {
	if candidates.len() > MAX_COMPLETION_CANDIDATES {
		return Err(RegistryError::TooManyCompletionCandidates {
			actual: candidates.len(),
			limit: MAX_COMPLETION_CANDIDATES,
		});
	}
	let bytes = strings_bytes(candidates)?;
	if bytes > MAX_COMPLETION_BYTES {
		return Err(RegistryError::TooMuchCompletionMetadata {
			actual: bytes,
			limit: MAX_COMPLETION_BYTES,
		});
	}
	Ok(())
}

fn validate_action_owner(
	batch_owner: RegistryOwnerId,
	action: &ActionLease,
) -> Result<(), RegistryError> {
	if action.owner() != batch_owner {
		return Err(RegistryError::ActionOwnerMismatch {
			batch_owner,
			action_owner: action.owner(),
		});
	}
	Ok(())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OptionValue {
	Bool(bool),
	Integer(i64),
	String(String),
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum OptionInvocation {
	Set(OptionValue),
	Query,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum DynamicInvocation {
	Command {
		canonical_name: String,
		spelling: String,
		args: String,
		bang: bool,
	},
	Mapping {
		mode: MappingMode,
		sequence: Vec<KeyStroke>,
	},
	Option {
		canonical_name: String,
		operation: OptionInvocation,
	},
}

/// Everything needed to invoke after registry resolution has released its
/// borrow. Replacing or removing the source entry cannot alter this snapshot.
#[derive(Debug, Clone)]
pub(crate) struct InvocationSnapshot {
	pub(crate) registry_revision: RegistryRev,
	pub(crate) invocation: DynamicInvocation,
	action: ActionLease,
}

impl InvocationSnapshot {
	pub(crate) fn new(
		registry_revision: RegistryRev,
		action: ActionLease,
		invocation: DynamicInvocation,
	) -> Self {
		Self {
			registry_revision,
			invocation,
			action,
		}
	}

	pub(crate) const fn owner(&self) -> RegistryOwnerId {
		self.action.owner()
	}

	pub(crate) fn action<T: Any>(&self) -> Option<&T> {
		self.action.downcast_ref()
	}
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum ActionError {
	ReentrantInvocation,
	RetiredOwner(RegistryOwnerId),
	MissingQueryDisplay,
	UnexpectedQueryDisplay,
	QueryDisplayTooLarge { actual: usize, limit: usize },
	Failed(String),
}

impl fmt::Display for ActionError {
	fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
		match self {
			Self::ReentrantInvocation => {
				f.write_str("Dynamic actions cannot invoke another dynamic action synchronously")
			}
			Self::RetiredOwner(owner) => {
				write!(f, "Dynamic action owner {owner} is no longer live")
			}
			Self::MissingQueryDisplay => {
				f.write_str("Dynamic option query did not return a display value")
			}
			Self::UnexpectedQueryDisplay => {
				f.write_str("Only a dynamic option query may return a query display value")
			}
			Self::QueryDisplayTooLarge { actual, limit } => write!(
				f,
				"Dynamic option query returned {actual} bytes; the limit is {limit}"
			),
			Self::Failed(message) => f.write_str(message),
		}
	}
}

impl std::error::Error for ActionError {}

pub(crate) fn validate_option_query_display(value: String) -> Result<String, ActionError> {
	if value.len() > MAX_OPTION_QUERY_DISPLAY_BYTES {
		return Err(ActionError::QueryDisplayTooLarge {
			actual: value.len(),
			limit: MAX_OPTION_QUERY_DISPLAY_BYTES,
		});
	}
	Ok(value)
}

pub(crate) trait DynamicActionExecutor<C> {
	type Output;

	fn owner_is_live(&self, owner: RegistryOwnerId) -> bool;

	fn invoke(
		&mut self,
		context: &mut C,
		invocation: InvocationSnapshot,
	) -> Result<Self::Output, ActionError>;
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub(crate) struct DynamicActionOutcome {
	pub(crate) status_message: Option<String>,
	pub(crate) option_query_display: Option<String>,
	pub(crate) needs_render: bool,
}

pub(crate) fn finalize_dynamic_action_outcome(
	is_option_query: bool,
	mut outcome: DynamicActionOutcome,
) -> Result<DynamicActionOutcome, ActionError> {
	match (is_option_query, outcome.option_query_display.take()) {
		(true, Some(display)) => {
			outcome.status_message = Some(validate_option_query_display(display)?);
			outcome.needs_render = true;
			Ok(outcome)
		}
		(true, None) => Err(ActionError::MissingQueryDisplay),
		(false, Some(_)) => Err(ActionError::UnexpectedQueryDisplay),
		(false, None) => Ok(outcome),
	}
}

/// Default executor used before a language host is attached.
pub(crate) struct UnavailableActionExecutor;

impl<C> DynamicActionExecutor<C> for UnavailableActionExecutor {
	type Output = DynamicActionOutcome;

	fn owner_is_live(&self, _owner: RegistryOwnerId) -> bool {
		true
	}

	fn invoke(
		&mut self,
		_context: &mut C,
		_invocation: InvocationSnapshot,
	) -> Result<Self::Output, ActionError> {
		Err(ActionError::Failed(
			"Runtime action executor is not attached".to_string(),
		))
	}
}

/// Engine-thread guard shared by every dynamic action entry point.
#[derive(Default)]
pub(crate) struct InvocationGate {
	active: Cell<bool>,
}

impl InvocationGate {
	pub(crate) fn invoke_guarded<T>(
		&self,
		owner: RegistryOwnerId,
		owner_is_live: bool,
		invoke: impl FnOnce() -> T,
	) -> Result<T, ActionError> {
		if self.active.get() {
			return Err(ActionError::ReentrantInvocation);
		}
		if !owner_is_live {
			return Err(ActionError::RetiredOwner(owner));
		}
		self.active.set(true);
		let _reset = InvocationReset {
			active: &self.active,
		};
		Ok(invoke())
	}

	pub(crate) fn invoke_dynamic<C, E>(
		&self,
		executor: &mut E,
		context: &mut C,
		invocation: InvocationSnapshot,
	) -> Result<DynamicActionOutcome, ActionError>
	where
		E: DynamicActionExecutor<C, Output = DynamicActionOutcome> + ?Sized,
	{
		let is_option_query = matches!(
			&invocation.invocation,
			DynamicInvocation::Option {
				operation: OptionInvocation::Query,
				..
			}
		);
		let outcome = self.invoke(executor, context, invocation)?;
		finalize_dynamic_action_outcome(is_option_query, outcome)
	}

	pub(crate) fn invoke<C, E>(
		&self,
		executor: &mut E,
		context: &mut C,
		invocation: InvocationSnapshot,
	) -> Result<E::Output, ActionError>
	where
		E: DynamicActionExecutor<C> + ?Sized,
	{
		let owner = invocation.owner();
		let owner_is_live = executor.owner_is_live(owner);
		self.invoke_guarded(owner, owner_is_live, || {
			executor.invoke(context, invocation)
		})?
	}
}

struct InvocationReset<'a> {
	active: &'a Cell<bool>,
}

impl Drop for InvocationReset<'_> {
	fn drop(&mut self) {
		self.active.set(false);
	}
}

#[cfg(test)]
mod tests {
	use super::*;
	use crate::engine::event_bus::{EventKind, EventKindSet};
	use std::collections::HashSet;
	use std::rc::Rc;

	#[derive(Debug, Clone, PartialEq, Eq)]
	enum FakeAction {
		Record(String),
		ReplaceSelf,
		RemoveSelf,
		Reenter,
		Fail(String),
	}

	fn command(
		owner: RegistryOwnerId,
		name: impl Into<String>,
		action: FakeAction,
	) -> RuntimeCommandRegistration {
		let name = name.into();
		RuntimeCommandRegistration {
			usage: format!(":{name}"),
			summary: format!("Run {name}"),
			help: format!("Invoke the {name} test action."),
			name,
			aliases: Vec::new(),
			bang: true,
			args: CommandArgRule::Optional,
			completion: RuntimeCompletion::None,
			action: ActionLease::new(owner, action),
		}
	}

	fn option(
		owner: RegistryOwnerId,
		name: impl Into<String>,
		kind: RuntimeOptionKind,
	) -> RuntimeOptionRegistration {
		let name = name.into();
		RuntimeOptionRegistration {
			usage: format!(":set {name}"),
			summary: format!("Set {name}"),
			help: format!("Set the {name} test option."),
			name,
			aliases: Vec::new(),
			kind,
			completion_candidates: Vec::new(),
			action: ActionLease::new(owner, FakeAction::Record("option".to_string())),
		}
	}

	fn stroke(ch: char) -> KeyStroke {
		KeyStroke::new(KeyCode::Char(ch), Modifiers::NONE)
	}

	fn mapping(
		owner: RegistryOwnerId,
		mode: MappingMode,
		sequence: Vec<KeyStroke>,
	) -> RuntimeMappingRegistration {
		RuntimeMappingRegistration {
			mode,
			sequence,
			summary: "Test mapping".to_string(),
			target: RuntimeMappingTarget::Action(ActionLease::new(
				owner,
				FakeAction::Record("mapping".to_string()),
			)),
		}
	}

	fn subscription(
		owner: RegistryOwnerId,
		kinds: EventKindSet,
		label: impl Into<String>,
	) -> RuntimeSubscriptionRegistration {
		RuntimeSubscriptionRegistration {
			kinds,
			action: ActionLease::new(owner, FakeAction::Record(label.into())),
		}
	}

	fn subscription_batch(
		owner: RegistryOwnerId,
		subscriptions: Vec<RuntimeSubscriptionRegistration>,
	) -> RegistrationBatch {
		let mut batch = RegistrationBatch::new(owner);
		batch.subscriptions = subscriptions;
		batch
	}

	fn source_generation_batch(owner: RegistryOwnerId, label: &str) -> RegistrationBatch {
		let mut mapping = mapping(owner, MappingMode::Normal, vec![stroke('Q')]);
		mapping.target = RuntimeMappingTarget::Action(ActionLease::new(
			owner,
			FakeAction::Record(format!("{label}-mapping")),
		));
		let mut option = option(
			owner,
			"source_option",
			RuntimeOptionKind::String {
				require_non_empty: false,
			},
		);
		option.action = ActionLease::new(owner, FakeAction::Record(format!("{label}-option")));
		RegistrationBatch {
			owner,
			commands: vec![command(
				owner,
				"source_command",
				FakeAction::Record(format!("{label}-command")),
			)],
			mappings: vec![mapping],
			options: vec![option],
			subscriptions: Vec::new(),
		}
	}

	fn mapping_action_snapshot(registries: &RuntimeRegistries) -> InvocationSnapshot {
		match registries.mapping_target(MappingMode::Normal, &[stroke('Q')]) {
			Some(ResolvedMappingTarget::Action(invocation)) => invocation,
			Some(ResolvedMappingTarget::Command(_)) => panic!("test mapping must target an action"),
			None => panic!("test mapping must resolve"),
		}
	}

	fn command_batch(
		owner: RegistryOwnerId,
		commands: Vec<RuntimeCommandRegistration>,
	) -> RegistrationBatch {
		RegistrationBatch {
			owner,
			commands,
			mappings: Vec::new(),
			options: Vec::new(),
			subscriptions: Vec::new(),
		}
	}

	fn many_commands(owner: RegistryOwnerId, prefix: &str, count: usize) -> RegistrationBatch {
		let commands = (0..count)
			.map(|index| {
				command(
					owner,
					format!("{prefix}_{index}"),
					FakeAction::Record(index.to_string()),
				)
			})
			.collect();
		command_batch(owner, commands)
	}

	fn metadata_sized_batch(
		owner: RegistryOwnerId,
		prefix: &str,
		target: usize,
	) -> RegistrationBatch {
		let mut commands = (0..65)
			.map(|index| {
				let mut command = command(
					owner,
					format!("{prefix}_{index}"),
					FakeAction::Record(index.to_string()),
				);
				command.usage.clear();
				command.summary.clear();
				command.help.clear();
				command
			})
			.collect::<Vec<_>>();
		let base = commands
			.iter()
			.map(|command| command.name.len())
			.sum::<usize>();
		assert!(target >= base);
		let mut remaining = target - base;
		for command in &mut commands {
			let bytes = remaining.min(MAX_HELP_BYTES);
			command.help = "h".repeat(bytes);
			remaining -= bytes;
		}
		assert_eq!(remaining, 0, "target must fit within 65 help bodies");
		let batch = command_batch(owner, commands);
		assert_eq!(batch.metadata_bytes().expect("metadata"), target);
		batch
	}

	#[test]
	fn subscriptions_publish_with_monotonic_ids_and_revision_neutral_delivery_state() {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");
		let kinds = EventKindSet::from_kinds([EventKind::BufferOpened, EventKind::BufferEdited]);
		registries
			.commit_owner_batch(subscription_batch(
				owner,
				vec![
					subscription(owner, kinds, "first"),
					subscription(owner, EventKindSet::one(EventKind::BufferClosed), "second"),
				],
			))
			.expect("publish subscriptions");
		assert_eq!(registries.summary().subscription_count, 2);
		assert_eq!(registries.summary().revision, RegistryRev::new(1));

		let mut live = Vec::new();
		let mut needs = Vec::new();
		registries.snapshot_subscriptions(&mut live, &mut needs);
		assert!(live.is_empty());
		assert_eq!(needs, [SubscriptionId::new(1), SubscriptionId::new(2)]);
		assert!(registries.mark_subscription_live(SubscriptionId::new(1)));
		assert_eq!(
			registries.record_subscription_baseline_failure(SubscriptionId::new(2), 1),
			Some(SubscriptionDeliveryState::Disabled)
		);
		assert_eq!(registries.summary().revision, RegistryRev::new(1));
		registries.snapshot_subscriptions(&mut live, &mut needs);
		assert_eq!(live.len(), 1);
		assert_eq!(live[0].id, SubscriptionId::new(1));
		assert!(live[0].kinds.contains(EventKind::BufferEdited));
		assert!(needs.is_empty());
	}

	#[test]
	fn generation_replacement_never_reuses_subscription_identity() {
		let mut registries = RuntimeRegistries::new();
		let retired = registries.allocate_owner().expect("retired owner");
		registries
			.commit_owner_batch(subscription_batch(
				retired,
				vec![subscription(
					retired,
					EventKindSet::one(EventKind::BufferOpened),
					"old",
				)],
			))
			.expect("publish retired generation");
		let old_id = SubscriptionId::new(1);
		let replacement = registries.allocate_owner().expect("replacement owner");
		registries
			.replace_owner_generation(
				retired,
				subscription_batch(
					replacement,
					vec![subscription(
						replacement,
						EventKindSet::one(EventKind::BufferOpened),
						"new",
					)],
				),
			)
			.expect("replace generation");
		assert!(registries.subscription_state(old_id).is_none());
		assert_eq!(
			registries.subscription_state(SubscriptionId::new(2)),
			Some(SubscriptionDeliveryState::NeedsBaseline {
				consecutive_failures: 0
			})
		);
	}

	#[test]
	fn subscription_validation_is_atomic_and_id_exhaustion_consumes_nothing() {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");
		let before = registries.summary();
		let error = registries
			.commit_owner_batch(subscription_batch(
				owner,
				vec![subscription(owner, EventKindSet::EMPTY, "empty")],
			))
			.expect_err("empty event set");
		assert_eq!(error, RegistryError::EmptySubscriptionKinds);
		assert_eq!(registries.summary(), before);
		assert_eq!(registries.next_subscription_id, SubscriptionId::new(1));

		let other = registries.allocate_owner().expect("other owner");
		let error = registries
			.commit_owner_batch(subscription_batch(
				owner,
				vec![subscription(
					other,
					EventKindSet::one(EventKind::BufferOpened),
					"wrong owner",
				)],
			))
			.expect_err("wrong owner lease");
		assert!(matches!(error, RegistryError::ActionOwnerMismatch { .. }));
		assert_eq!(registries.next_subscription_id, SubscriptionId::new(1));
		let over_owner_cap = (0..=MAX_SUBSCRIBERS_PER_OWNER)
			.map(|index| {
				subscription(
					owner,
					EventKindSet::one(EventKind::BufferOpened),
					format!("owner-overflow-{index}"),
				)
			})
			.collect();
		let error = registries
			.commit_owner_batch(subscription_batch(owner, over_owner_cap))
			.expect_err("owner subscriber cap");
		assert_eq!(
			error,
			RegistryError::TooManyOwnerSubscribers {
				actual: MAX_SUBSCRIBERS_PER_OWNER + 1,
				limit: MAX_SUBSCRIBERS_PER_OWNER,
			}
		);
		assert_eq!(registries.next_subscription_id, SubscriptionId::new(1));

		let before_exhaustion = registries.summary();
		registries.next_subscription_id = SubscriptionId::new(u64::MAX);
		let error = registries
			.commit_owner_batch(subscription_batch(
				owner,
				vec![subscription(
					owner,
					EventKindSet::one(EventKind::BufferOpened),
					"exhausted",
				)],
			))
			.expect_err("subscription ids exhausted");
		assert_eq!(error, RegistryError::SubscriptionIdsExhausted);
		assert_eq!(registries.summary(), before_exhaustion);
		assert_eq!(
			registries.next_subscription_id,
			SubscriptionId::new(u64::MAX)
		);
	}

	#[test]
	fn subscriber_caps_accept_the_exact_session_boundary() {
		let mut registries = RuntimeRegistries::new();
		let kinds = EventKindSet::one(EventKind::BufferOpened);
		for owner_index in 0..(MAX_SUBSCRIBERS_TOTAL / MAX_SUBSCRIBERS_PER_OWNER) {
			let owner = registries.allocate_owner().expect("owner");
			let subscriptions = (0..MAX_SUBSCRIBERS_PER_OWNER)
				.map(|index| subscription(owner, kinds, format!("{owner_index}:{index}")))
				.collect();
			registries
				.commit_owner_batch(subscription_batch(owner, subscriptions))
				.expect("exact session boundary");
		}
		assert_eq!(registries.subscription_count(), MAX_SUBSCRIBERS_TOTAL);

		let overflow = registries.allocate_owner().expect("overflow owner");
		let error = registries
			.commit_owner_batch(subscription_batch(
				overflow,
				vec![subscription(overflow, kinds, "overflow")],
			))
			.expect_err("session subscriber cap");
		assert_eq!(
			error,
			RegistryError::TooManySessionSubscribers {
				actual: MAX_SUBSCRIBERS_TOTAL + 1,
				limit: MAX_SUBSCRIBERS_TOTAL,
			}
		);
	}

	#[test]
	fn owner_ids_precedence_and_revisions_are_monotonic() {
		let mut registries = RuntimeRegistries::new();
		let first = registries.allocate_owner().expect("first owner");
		let second = registries.allocate_owner().expect("second owner");
		assert_eq!(first, RegistryOwnerId::new(1));
		assert_eq!(second, RegistryOwnerId::new(2));
		assert_eq!(registries.owner_precedence(first), Some(0));
		assert_eq!(registries.owner_precedence(second), Some(1));
		assert_eq!(registries.revision(), RegistryRev::ZERO);

		assert_eq!(
			registries
				.commit_owner_batch(RegistrationBatch::new(first))
				.expect("empty owner snapshot"),
			RegistryRev::ZERO,
			"an empty snapshot replacing an empty snapshot is a no-op"
		);
		let rev = registries
			.commit_owner_batch(command_batch(
				first,
				vec![command(
					first,
					"owner_command",
					FakeAction::Record("first".to_string()),
				)],
			))
			.expect("publish owner");
		assert_eq!(rev, RegistryRev::new(1));
		assert_eq!(registries.owner_precedence(first), Some(0));

		let third = registries.allocate_owner().expect("third owner");
		assert_eq!(third, RegistryOwnerId::new(3));
		assert_eq!(registries.owner_precedence(third), Some(2));
		assert!(registries.owner_is_live(first));
	}

	#[test]
	fn only_never_published_empty_candidates_can_be_discarded_revision_neutrally() {
		let mut registries = RuntimeRegistries::new();
		let candidate = registries.allocate_owner().expect("candidate");
		let successor = registries.allocate_owner().expect("successor");
		assert_eq!(registries.revision(), RegistryRev::ZERO);
		registries
			.discard_unpublished_empty_owner(candidate)
			.expect("discard unpublished candidate");
		assert!(!registries.owner_is_live(candidate));
		assert_eq!(registries.revision(), RegistryRev::ZERO);
		assert_eq!(successor, RegistryOwnerId::new(2), "IDs are not reused");

		let published_empty = registries.allocate_owner().expect("published empty");
		registries
			.commit_owner_batch(RegistrationBatch::new(published_empty))
			.expect("publish empty generation");
		assert_eq!(registries.revision(), RegistryRev::ZERO);
		assert_eq!(
			registries.discard_unpublished_empty_owner(published_empty),
			Err(RegistryError::OwnerCannotBeDiscarded(published_empty))
		);
		assert!(registries.owner_is_live(published_empty));

		let published_nonempty = registries.allocate_owner().expect("published nonempty");
		registries
			.commit_owner_batch(command_batch(
				published_nonempty,
				vec![command(
					published_nonempty,
					"published",
					FakeAction::Record("published".to_string()),
				)],
			))
			.expect("publish nonempty generation");
		assert_eq!(
			registries.discard_unpublished_empty_owner(published_nonempty),
			Err(RegistryError::OwnerCannotBeDiscarded(published_nonempty))
		);
	}

	#[test]
	fn one_owner_snapshot_publishes_catalog_registries_atomically() {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");
		let mut batch = RegistrationBatch::new(owner);
		batch.commands.push(command(
			owner,
			"atomic_command",
			FakeAction::Record("command".to_string()),
		));
		batch
			.mappings
			.push(mapping(owner, MappingMode::Normal, vec![stroke('Q')]));
		batch
			.options
			.push(option(owner, "atomic_option", RuntimeOptionKind::Integer));

		assert_eq!(
			registries.commit_owner_batch(batch).expect("publish"),
			RegistryRev::new(1)
		);
		assert_eq!(
			registries.summary(),
			RuntimeRegistrySummary {
				revision: RegistryRev::new(1),
				owner_count: 1,
				command_count: 1,
				mapping_count: 1,
				option_count: 1,
				subscription_count: 0,
				metadata_bytes: registries.metadata_bytes,
			}
		);

		let mut replacement = RegistrationBatch::new(owner);
		replacement.commands.push(command(
			owner,
			"replacement",
			FakeAction::Record("replacement".to_string()),
		));
		registries
			.commit_owner_batch(replacement)
			.expect("replace complete snapshot");
		let summary = registries.summary();
		assert_eq!(summary.command_count, 1);
		assert_eq!(summary.mapping_count, 0);
		assert_eq!(summary.option_count, 0);
		assert_eq!(summary.revision, RegistryRev::new(2));
	}

	#[test]
	fn source_generation_replacement_is_atomic_and_inherits_precedence() {
		let mut registries = RuntimeRegistries::new();
		let retired = registries.allocate_owner().expect("retired generation");
		let overlay = registries.allocate_owner().expect("unrelated overlay");
		let replacement = registries.allocate_owner().expect("replacement generation");
		registries
			.commit_owner_batch(source_generation_batch(retired, "old"))
			.expect("old generation");

		let stale_command = registries
			.command_invocation("source_command", "", false)
			.expect("old command snapshot");
		let stale_option = registries
			.option_invocation("source_option", OptionInvocation::Query)
			.expect("old option snapshot");
		let stale_mapping = mapping_action_snapshot(&registries);

		let mut overlay_batch = RegistrationBatch::new(overlay);
		let mut overlay_mapping = mapping(overlay, MappingMode::Normal, vec![stroke('Q')]);
		overlay_mapping.target = RuntimeMappingTarget::Action(ActionLease::new(
			overlay,
			FakeAction::Record("overlay-mapping".to_string()),
		));
		overlay_batch.mappings.push(overlay_mapping);
		registries
			.commit_owner_batch(overlay_batch)
			.expect("unrelated mapping layer");

		assert_eq!(registries.owner_precedence(retired), Some(0));
		assert_eq!(registries.owner_precedence(overlay), Some(1));
		assert_eq!(registries.owner_precedence(replacement), Some(2));
		assert_eq!(registries.mapping_infos()[0].owner, overlay);
		let before_replacement = registries.summary();
		assert_eq!(
			registries
				.replace_owner_generation(retired, source_generation_batch(replacement, "new"),)
				.expect("replace source generation"),
			RegistryRev::new(3)
		);

		assert!(!registries.owner_is_live(retired));
		assert!(registries.owner_is_live(replacement));
		assert_eq!(registries.owner_precedence(replacement), Some(0));
		assert_eq!(registries.owner_precedence(overlay), Some(1));
		assert_eq!(registries.mapping_infos()[0].owner, overlay);
		assert_eq!(registries.command_infos()[0].owner, replacement);
		assert_eq!(registries.option_infos()[0].owner, replacement);
		assert_eq!(
			registries.summary(),
			RuntimeRegistrySummary {
				revision: RegistryRev::new(3),
				owner_count: 2,
				command_count: 1,
				mapping_count: 2,
				option_count: 1,
				subscription_count: 0,
				metadata_bytes: before_replacement.metadata_bytes,
			}
		);

		assert_eq!(
			registries.remove_owner(overlay).expect("remove overlay"),
			RegistryRev::new(4)
		);
		assert_eq!(registries.mapping_infos()[0].owner, replacement);

		let gate = Rc::new(InvocationGate::default());
		let mut context = FakeContext {
			registries,
			calls: Vec::new(),
			nested_error: None,
		};
		let mut executor = FakeExecutor {
			live: HashSet::from([replacement]),
			gate: Rc::clone(&gate),
		};
		for stale in [stale_command, stale_option, stale_mapping] {
			assert_eq!(
				gate.invoke(&mut executor, &mut context, stale),
				Err(ActionError::RetiredOwner(retired))
			);
		}

		let new_command = context
			.registries
			.command_invocation("source_command", "", false)
			.expect("new command");
		let new_option = context
			.registries
			.option_invocation("source_option", OptionInvocation::Query)
			.expect("new option");
		let new_mapping = mapping_action_snapshot(&context.registries);
		for (invocation, expected) in [
			(new_command, "new-command"),
			(new_option, "new-option"),
			(new_mapping, "new-mapping"),
		] {
			assert_eq!(
				gate.invoke(&mut executor, &mut context, invocation)
					.expect("invoke replacement action"),
				expected
			);
		}
		assert_eq!(context.calls, ["new-command", "new-option", "new-mapping"]);

		let next = context
			.registries
			.allocate_owner()
			.expect("next logical owner");
		assert_eq!(next, RegistryOwnerId::new(4));
		assert_eq!(context.registries.owner_precedence(next), Some(3));
	}

	#[test]
	fn failed_generation_replacement_keeps_the_old_generation_fully_effective() {
		let mut registries = RuntimeRegistries::new();
		let retired = registries.allocate_owner().expect("retired generation");
		let blocker = registries.allocate_owner().expect("blocking owner");
		registries
			.commit_owner_batch(source_generation_batch(retired, "old"))
			.expect("old generation");
		registries
			.commit_owner_batch(command_batch(
				blocker,
				vec![command(
					blocker,
					"occupied_command",
					FakeAction::Record("blocker".to_string()),
				)],
			))
			.expect("blocking command");
		let replacement = registries.allocate_owner().expect("replacement generation");
		let before = registries.summary();
		let mut candidate = source_generation_batch(replacement, "new");
		candidate.commands.push(command(
			replacement,
			"occupied_command",
			FakeAction::Record("collision".to_string()),
		));

		assert_eq!(
			registries
				.replace_owner_generation(retired, candidate)
				.expect_err("candidate collision"),
			RegistryError::NameCollision {
				namespace: RegistryNamespace::Command,
				token: "occupied_command".to_string(),
				occupant: RegistryOccupant::Owner(blocker),
			}
		);
		assert_eq!(registries.summary(), before);
		assert!(registries.owner_is_live(retired));
		assert!(registries.owner_is_live(replacement));
		assert_eq!(registries.owner_precedence(retired), Some(0));
		assert_eq!(registries.owner_precedence(replacement), Some(2));
		assert!(
			registries
				.command_infos()
				.iter()
				.any(|command| command.name == "source_command" && command.owner == retired)
		);
		assert_eq!(registries.option_infos()[0].owner, retired);
		assert_eq!(registries.mapping_infos()[0].owner, retired);
		assert_eq!(
			registries
				.command_invocation("source_command", "", false)
				.expect("old command remains")
				.action::<FakeAction>(),
			Some(&FakeAction::Record("old-command".to_string()))
		);
		assert_eq!(
			registries
				.option_invocation("source_option", OptionInvocation::Query)
				.expect("old option remains")
				.action::<FakeAction>(),
			Some(&FakeAction::Record("old-option".to_string()))
		);
		assert_eq!(
			mapping_action_snapshot(&registries).action::<FakeAction>(),
			Some(&FakeAction::Record("old-mapping".to_string()))
		);

		let before_not_fresh = registries.summary();
		assert_eq!(
			registries
				.replace_owner_generation(retired, source_generation_batch(retired, "same"))
				.expect_err("same owner is not a fresh generation"),
			RegistryError::ReplacementOwnerIsNotFresh {
				retired,
				replacement: retired,
			}
		);
		assert_eq!(registries.summary(), before_not_fresh);
	}

	#[test]
	fn builtin_and_cross_owner_name_collisions_leave_state_untouched() {
		let mut registries = RuntimeRegistries::new();
		let first = registries.allocate_owner().expect("first");
		let second = registries.allocate_owner().expect("second");
		let mut first_command = command(
			first,
			"plugin_command",
			FakeAction::Record("first".to_string()),
		);
		first_command.aliases.push("plugin_alias".to_string());
		let mut first_option = option(first, "plugin_option", RuntimeOptionKind::Bool);
		first_option.aliases.push("plugin_opt".to_string());
		let mut initial = RegistrationBatch::new(first);
		initial.commands.push(first_command);
		initial.options.push(first_option);
		registries
			.commit_owner_batch(initial)
			.expect("initial owner");
		let before = registries.summary();

		for name in ["w", "wrap", "sort"] {
			let error = registries
				.commit_owner_batch(command_batch(
					second,
					vec![command(
						second,
						name,
						FakeAction::Record("collision".to_string()),
					)],
				))
				.expect_err("builtin command collision");
			assert!(matches!(
				error,
				RegistryError::NameCollision {
					namespace: RegistryNamespace::Command,
					occupant: RegistryOccupant::Builtin,
					..
				}
			));
			assert_eq!(registries.summary(), before);
		}

		let mut colliding = command(
			second,
			"other_command",
			FakeAction::Record("collision".to_string()),
		);
		colliding.aliases.push("plugin_alias".to_string());
		let error = registries
			.commit_owner_batch(command_batch(second, vec![colliding]))
			.expect_err("cross-owner alias collision");
		assert!(matches!(
			error,
			RegistryError::NameCollision {
				occupant: RegistryOccupant::Owner(owner),
				..
			} if owner == first
		));
		assert_eq!(registries.summary(), before);

		for name in ["theme", "colors.accent", "background", "noplugin_option"] {
			let batch = RegistrationBatch {
				owner: second,
				commands: Vec::new(),
				mappings: Vec::new(),
				options: vec![option(second, name, RuntimeOptionKind::Bool)],
				subscriptions: Vec::new(),
			};
			let error = registries
				.commit_owner_batch(batch)
				.expect_err("option collision");
			assert!(matches!(error, RegistryError::NameCollision { .. }));
			assert_eq!(registries.summary(), before);
		}
	}

	#[test]
	fn invalid_cross_registry_batch_cannot_publish_its_valid_entries() {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");
		let before = registries.summary();
		let mut batch = RegistrationBatch::new(owner);
		batch.commands.push(command(
			owner,
			"valid_command",
			FakeAction::Record("valid".to_string()),
		));
		batch
			.mappings
			.push(mapping(owner, MappingMode::Normal, vec![stroke('Z')]));
		batch.options.push(option(
			owner,
			"theme",
			RuntimeOptionKind::String {
				require_non_empty: true,
			},
		));
		assert!(matches!(
			registries.commit_owner_batch(batch),
			Err(RegistryError::NameCollision { .. })
		));
		assert_eq!(registries.summary(), before);
		assert!(
			registries
				.command_invocation("valid_command", "", false)
				.is_none()
		);
	}

	#[test]
	fn actions_must_belong_to_the_batch_owner() {
		let mut registries = RuntimeRegistries::new();
		let first = registries.allocate_owner().expect("first");
		let second = registries.allocate_owner().expect("second");
		let before = registries.summary();
		let error = registries
			.commit_owner_batch(command_batch(
				first,
				vec![command(
					second,
					"wrong_owner",
					FakeAction::Record("wrong".to_string()),
				)],
			))
			.expect_err("foreign action");
		assert_eq!(
			error,
			RegistryError::ActionOwnerMismatch {
				batch_owner: first,
				action_owner: second,
			}
		);
		assert_eq!(registries.summary(), before);
	}

	#[test]
	fn runtime_mapping_layers_allow_exact_overrides_but_reject_prefixes() {
		let mut registries = RuntimeRegistries::new();
		let first = registries.allocate_owner().expect("first");
		let second = registries.allocate_owner().expect("second");
		let sequence = vec![stroke('g'), stroke('d')];
		let mut first_batch = RegistrationBatch::new(first);
		first_batch
			.mappings
			.push(mapping(first, MappingMode::Normal, sequence.clone()));
		registries
			.commit_owner_batch(first_batch)
			.expect("first terminal");

		let mut exact_layer = RegistrationBatch::new(second);
		exact_layer
			.mappings
			.push(mapping(second, MappingMode::Normal, sequence.clone()));
		registries
			.commit_owner_batch(exact_layer)
			.expect("exact override layer");
		assert_eq!(registries.summary().mapping_count, 2);
		let before = registries.summary();

		let mut prefix = RegistrationBatch::new(second);
		prefix
			.mappings
			.push(mapping(second, MappingMode::Normal, vec![stroke('g')]));
		assert!(matches!(
			registries.commit_owner_batch(prefix),
			Err(RegistryError::MappingPrefixConflict { .. })
		));
		assert_eq!(registries.summary(), before);

		let mut duplicate = RegistrationBatch::new(second);
		duplicate
			.mappings
			.push(mapping(second, MappingMode::Visual, vec![stroke('x')]));
		duplicate
			.mappings
			.push(mapping(second, MappingMode::Visual, vec![stroke('x')]));
		assert!(matches!(
			registries.commit_owner_batch(duplicate),
			Err(RegistryError::DuplicateMapping { .. })
		));
		assert_eq!(registries.summary(), before);
	}

	#[test]
	fn names_aliases_fields_candidates_and_sequences_enforce_exact_caps() {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");

		let max_name = "a".repeat(MAX_NAME_BYTES);
		registries
			.commit_owner_batch(command_batch(
				owner,
				vec![command(
					owner,
					max_name,
					FakeAction::Record("max-name".to_string()),
				)],
			))
			.expect("name at cap");
		let before = registries.summary();
		let error = registries
			.commit_owner_batch(command_batch(
				owner,
				vec![command(
					owner,
					"a".repeat(MAX_NAME_BYTES + 1),
					FakeAction::Record("long-name".to_string()),
				)],
			))
			.expect_err("name over cap");
		assert!(matches!(error, RegistryError::FieldTooLong { .. }));
		assert_eq!(registries.summary(), before);

		let mut aliases = command(
			owner,
			"alias_boundary",
			FakeAction::Record("aliases".to_string()),
		);
		aliases.aliases = (0..MAX_ALIASES)
			.map(|index| format!("alias_{index}"))
			.collect();
		registries
			.commit_owner_batch(command_batch(owner, vec![aliases.clone()]))
			.expect("aliases at cap");
		aliases.aliases.push("one_too_many".to_string());
		assert!(matches!(
			registries.commit_owner_batch(command_batch(owner, vec![aliases])),
			Err(RegistryError::TooManyAliases { .. })
		));

		let mut fields = command(
			owner,
			"field_boundary",
			FakeAction::Record("fields".to_string()),
		);
		fields.usage = "u".repeat(MAX_SUMMARY_OR_USAGE_BYTES);
		fields.summary = "s".repeat(MAX_SUMMARY_OR_USAGE_BYTES);
		fields.help = "h".repeat(MAX_HELP_BYTES);
		registries
			.commit_owner_batch(command_batch(owner, vec![fields.clone()]))
			.expect("fields at caps");
		fields.help.push('h');
		assert!(matches!(
			registries.commit_owner_batch(command_batch(owner, vec![fields])),
			Err(RegistryError::FieldTooLong { .. })
		));

		let mut candidates = command(
			owner,
			"candidate_count",
			FakeAction::Record("candidates".to_string()),
		);
		candidates.completion =
			RuntimeCompletion::Static(vec![String::new(); MAX_COMPLETION_CANDIDATES]);
		registries
			.commit_owner_batch(command_batch(owner, vec![candidates.clone()]))
			.expect("candidate count at cap");
		candidates.completion =
			RuntimeCompletion::Static(vec![String::new(); MAX_COMPLETION_CANDIDATES + 1]);
		assert!(matches!(
			registries.commit_owner_batch(command_batch(owner, vec![candidates])),
			Err(RegistryError::TooManyCompletionCandidates { .. })
		));

		let mut candidate_bytes = command(
			owner,
			"candidate_bytes",
			FakeAction::Record("candidate-bytes".to_string()),
		);
		candidate_bytes.completion =
			RuntimeCompletion::Static(vec!["c".repeat(MAX_COMPLETION_BYTES)]);
		registries
			.commit_owner_batch(command_batch(owner, vec![candidate_bytes.clone()]))
			.expect("completion bytes at cap");
		candidate_bytes.completion =
			RuntimeCompletion::Static(vec!["c".repeat(MAX_COMPLETION_BYTES + 1)]);
		assert!(matches!(
			registries.commit_owner_batch(command_batch(owner, vec![candidate_bytes])),
			Err(RegistryError::TooMuchCompletionMetadata { .. })
		));

		let mut sequence = RegistrationBatch::new(owner);
		sequence.mappings.push(mapping(
			owner,
			MappingMode::Normal,
			vec![stroke('Q'); MAX_MAPPING_SEQUENCE],
		));
		registries
			.commit_owner_batch(sequence)
			.expect("mapping sequence at cap");
		let mut too_long = RegistrationBatch::new(owner);
		too_long.mappings.push(mapping(
			owner,
			MappingMode::Normal,
			vec![stroke('Q'); MAX_MAPPING_SEQUENCE + 1],
		));
		assert!(matches!(
			registries.commit_owner_batch(too_long),
			Err(RegistryError::MappingSequenceTooLong { .. })
		));
	}

	#[test]
	fn invalid_name_grammar_and_unsupported_keys_are_rejected() {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");
		for name in ["", "1command", "has space", "bang!", "%sort", "λ"] {
			assert!(matches!(
				registries.commit_owner_batch(command_batch(
					owner,
					vec![command(
						owner,
						name,
						FakeAction::Record("invalid".to_string()),
					)],
				)),
				Err(RegistryError::InvalidName { .. })
			));
		}
		let mut batch = RegistrationBatch::new(owner);
		batch.mappings.push(mapping(
			owner,
			MappingMode::Normal,
			vec![KeyStroke::new(KeyCode::Unsupported, Modifiers::NONE)],
		));
		assert_eq!(
			registries.commit_owner_batch(batch).unwrap_err(),
			RegistryError::UnsupportedMappingKey { index: 0 }
		);
		assert_eq!(registries.revision(), RegistryRev::ZERO);
	}

	#[test]
	fn transaction_and_session_entry_caps_accept_boundary_and_reject_next() {
		let mut registries = RuntimeRegistries::new();
		let first = registries.allocate_owner().expect("first");
		registries
			.commit_owner_batch(many_commands(
				first,
				"owner1_entry",
				MAX_TRANSACTION_ENTRIES,
			))
			.expect("transaction at entry cap");
		let before = registries.summary();
		let error = registries
			.commit_owner_batch(many_commands(
				first,
				"too_many",
				MAX_TRANSACTION_ENTRIES + 1,
			))
			.expect_err("transaction over entry cap");
		assert_eq!(
			error,
			RegistryError::TooManyTransactionEntries {
				actual: MAX_TRANSACTION_ENTRIES + 1,
				limit: MAX_TRANSACTION_ENTRIES,
			}
		);
		assert_eq!(registries.summary(), before);

		for owner_number in 2..=4 {
			let owner = registries.allocate_owner().expect("session owner");
			registries
				.commit_owner_batch(many_commands(
					owner,
					&format!("owner{owner_number}_entry"),
					MAX_TRANSACTION_ENTRIES,
				))
				.expect("fill session entry cap");
		}
		assert_eq!(registries.summary().command_count, MAX_SESSION_ENTRIES);
		let overflow_owner = registries.allocate_owner().expect("overflow owner");
		let before = registries.summary();
		let error = registries
			.commit_owner_batch(many_commands(overflow_owner, "overflow_entry", 1))
			.expect_err("session entry overflow");
		assert_eq!(
			error,
			RegistryError::TooManySessionEntries {
				actual: MAX_SESSION_ENTRIES + 1,
				limit: MAX_SESSION_ENTRIES,
			}
		);
		assert_eq!(registries.summary(), before);
		registries
			.replace_owner_generation(
				first,
				many_commands(overflow_owner, "replacement_entry", MAX_TRANSACTION_ENTRIES),
			)
			.expect("full owner generation replaces its allocation at the session cap");
		assert_eq!(registries.summary().command_count, MAX_SESSION_ENTRIES);
		assert!(!registries.owner_is_live(first));
		assert_eq!(registries.owner_precedence(overflow_owner), Some(0));
	}

	#[test]
	fn transaction_and_session_metadata_caps_accept_boundary_and_reject_next() {
		let mut registries = RuntimeRegistries::new();
		let first = registries.allocate_owner().expect("first");
		registries
			.commit_owner_batch(metadata_sized_batch(
				first,
				"meta1",
				MAX_TRANSACTION_METADATA_BYTES,
			))
			.expect("transaction metadata at cap");
		let before = registries.summary();
		let error = registries
			.commit_owner_batch(metadata_sized_batch(
				first,
				"meta_over",
				MAX_TRANSACTION_METADATA_BYTES + 1,
			))
			.expect_err("transaction metadata over cap");
		assert_eq!(
			error,
			RegistryError::TooMuchTransactionMetadata {
				actual: MAX_TRANSACTION_METADATA_BYTES + 1,
				limit: MAX_TRANSACTION_METADATA_BYTES,
			}
		);
		assert_eq!(registries.summary(), before);

		for owner_number in 2..=8 {
			let owner = registries.allocate_owner().expect("metadata owner");
			registries
				.commit_owner_batch(metadata_sized_batch(
					owner,
					&format!("meta{owner_number}"),
					MAX_TRANSACTION_METADATA_BYTES,
				))
				.expect("fill session metadata cap");
		}
		assert_eq!(
			registries.summary().metadata_bytes,
			MAX_SESSION_METADATA_BYTES
		);
		let overflow_owner = registries.allocate_owner().expect("overflow owner");
		let before = registries.summary();
		let error = registries
			.commit_owner_batch(command_batch(
				overflow_owner,
				vec![command(
					overflow_owner,
					"metadata_overflow",
					FakeAction::Record("overflow".to_string()),
				)],
			))
			.expect_err("session metadata overflow");
		assert!(matches!(
			error,
			RegistryError::TooMuchSessionMetadata { .. }
		));
		assert_eq!(registries.summary(), before);
		registries
			.replace_owner_generation(
				first,
				metadata_sized_batch(
					overflow_owner,
					"meta_replacement",
					MAX_TRANSACTION_METADATA_BYTES,
				),
			)
			.expect("full owner generation replaces its metadata at the session cap");
		assert_eq!(
			registries.summary().metadata_bytes,
			MAX_SESSION_METADATA_BYTES
		);
		assert!(!registries.owner_is_live(first));
		assert_eq!(registries.owner_precedence(overflow_owner), Some(0));
	}

	#[test]
	fn allocator_and_revision_exhaustion_fail_before_mutation() {
		let mut registries = RuntimeRegistries::new();
		registries.next_owner_id = RegistryOwnerId::new(u64::MAX);
		assert_eq!(
			registries.allocate_owner().unwrap_err(),
			RegistryError::OwnerIdsExhausted
		);
		assert_eq!(registries.summary().owner_count, 0);

		registries.next_owner_id = RegistryOwnerId::new(1);
		registries.next_precedence = u64::MAX;
		assert_eq!(
			registries.allocate_owner().unwrap_err(),
			RegistryError::OwnerPrecedenceExhausted
		);
		assert_eq!(registries.summary().owner_count, 0);

		registries.next_precedence = 0;
		let owner = registries.allocate_owner().expect("owner");
		let replacement = registries.allocate_owner().expect("replacement");
		registries.revision = RegistryRev::new(u64::MAX);
		let before = registries.summary();
		assert_eq!(
			registries
				.commit_owner_batch(command_batch(
					owner,
					vec![command(
						owner,
						"revision_overflow",
						FakeAction::Record("overflow".to_string()),
					)],
				))
				.unwrap_err(),
			RegistryError::RegistryRevisionExhausted
		);
		assert_eq!(registries.summary(), before);
		assert_eq!(
			registries.remove_owner(owner).unwrap_err(),
			RegistryError::RegistryRevisionExhausted
		);
		assert_eq!(registries.summary(), before);
		assert_eq!(
			registries
				.replace_owner_generation(owner, source_generation_batch(replacement, "overflow"),)
				.unwrap_err(),
			RegistryError::RegistryRevisionExhausted
		);
		assert_eq!(registries.summary(), before);
	}

	struct FakeContext {
		registries: RuntimeRegistries,
		calls: Vec<String>,
		nested_error: Option<ActionError>,
	}

	struct FakeExecutor {
		live: HashSet<RegistryOwnerId>,
		gate: Rc<InvocationGate>,
	}

	impl DynamicActionExecutor<FakeContext> for FakeExecutor {
		type Output = String;

		fn owner_is_live(&self, owner: RegistryOwnerId) -> bool {
			self.live.contains(&owner)
		}

		fn invoke(
			&mut self,
			context: &mut FakeContext,
			invocation: InvocationSnapshot,
		) -> Result<Self::Output, ActionError> {
			let action = invocation
				.action::<FakeAction>()
				.cloned()
				.ok_or_else(|| ActionError::Failed("unexpected action payload".to_string()))?;
			match action {
				FakeAction::Record(label) => {
					context.calls.push(label.clone());
					Ok(label)
				}
				FakeAction::ReplaceSelf => {
					let owner = invocation.owner();
					context.calls.push("old".to_string());
					context
						.registries
						.commit_owner_batch(command_batch(
							owner,
							vec![command(
								owner,
								"self_replace",
								FakeAction::Record("new".to_string()),
							)],
						))
						.map_err(|error| ActionError::Failed(error.to_string()))?;
					Ok("old".to_string())
				}
				FakeAction::RemoveSelf => {
					let owner = invocation.owner();
					context.calls.push("removed".to_string());
					context
						.registries
						.remove_owner(owner)
						.map_err(|error| ActionError::Failed(error.to_string()))?;
					if !self.live.remove(&owner) {
						return Err(ActionError::Failed(
							"removed owner was absent from the action store".to_string(),
						));
					}
					Ok("removed".to_string())
				}
				FakeAction::Reenter => {
					let nested = InvocationSnapshot::new(
						invocation.registry_revision,
						ActionLease::new(
							invocation.owner(),
							FakeAction::Record("nested".to_string()),
						),
						DynamicInvocation::Mapping {
							mode: MappingMode::Normal,
							sequence: vec![stroke('n')],
						},
					);
					let gate = Rc::clone(&self.gate);
					context.nested_error = Some(
						gate.invoke(self, context, nested)
							.expect_err("nested action must be rejected"),
					);
					Ok("outer".to_string())
				}
				FakeAction::Fail(message) => Err(ActionError::Failed(message)),
			}
		}
	}

	fn fake_runtime() -> (FakeContext, FakeExecutor, RegistryOwnerId) {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");
		let gate = Rc::new(InvocationGate::default());
		(
			FakeContext {
				registries,
				calls: Vec::new(),
				nested_error: None,
			},
			FakeExecutor {
				live: HashSet::from([owner]),
				gate,
			},
			owner,
		)
	}

	#[test]
	fn owned_invocation_survives_self_replacement_and_next_resolution_sees_new_action() {
		let (mut context, mut executor, owner) = fake_runtime();
		context
			.registries
			.commit_owner_batch(command_batch(
				owner,
				vec![command(owner, "self_replace", FakeAction::ReplaceSelf)],
			))
			.expect("initial action");
		let invocation = context
			.registries
			.command_invocation("self_replace", "argument", true)
			.expect("owned invocation");
		assert_eq!(invocation.registry_revision, RegistryRev::new(1));
		assert_eq!(
			invocation.invocation,
			DynamicInvocation::Command {
				canonical_name: "self_replace".to_string(),
				spelling: "self_replace".to_string(),
				args: "argument".to_string(),
				bang: true,
			}
		);

		let gate = Rc::clone(&executor.gate);
		assert_eq!(
			gate.invoke(&mut executor, &mut context, invocation)
				.expect("old invocation"),
			"old"
		);
		let next = context
			.registries
			.command_invocation("self_replace", "", false)
			.expect("replacement invocation");
		assert_eq!(
			gate.invoke(&mut executor, &mut context, next)
				.expect("new invocation"),
			"new"
		);
		assert_eq!(context.calls, vec!["old", "new"]);
	}

	#[test]
	fn action_can_remove_its_owner_without_invalidating_the_running_snapshot() {
		let (mut context, mut executor, owner) = fake_runtime();
		let mut batch = RegistrationBatch::new(owner);
		batch
			.commands
			.push(command(owner, "self_remove", FakeAction::RemoveSelf));
		batch
			.mappings
			.push(mapping(owner, MappingMode::Normal, vec![stroke('Q')]));
		batch
			.options
			.push(option(owner, "self_remove_option", RuntimeOptionKind::Bool));
		context
			.registries
			.commit_owner_batch(batch)
			.expect("publish removable owner");
		let current = context
			.registries
			.command_invocation("self_remove", "", false)
			.expect("current invocation");
		let stale = context
			.registries
			.command_invocation("self_remove", "", false)
			.expect("second owned invocation");

		let gate = Rc::clone(&executor.gate);
		assert_eq!(
			gate.invoke(&mut executor, &mut context, current)
				.expect("running action survives self-removal"),
			"removed"
		);
		assert_eq!(context.calls, ["removed"]);
		assert_eq!(
			context.registries.summary(),
			RuntimeRegistrySummary {
				revision: RegistryRev::new(2),
				owner_count: 0,
				command_count: 0,
				mapping_count: 0,
				option_count: 0,
				subscription_count: 0,
				metadata_bytes: 0,
			}
		);
		assert!(!context.registries.owner_is_live(owner));
		assert!(
			context
				.registries
				.command_invocation("self_remove", "", false)
				.is_none()
		);
		assert!(context.registries.mapping_infos().is_empty());
		assert!(context.registries.option_infos().is_empty());
		assert_eq!(
			gate.invoke(&mut executor, &mut context, stale),
			Err(ActionError::RetiredOwner(owner))
		);

		let next = context
			.registries
			.allocate_owner()
			.expect("owner IDs are not recycled");
		assert_eq!(next, RegistryOwnerId::new(2));
		assert_eq!(context.registries.owner_precedence(next), Some(1));
	}

	#[test]
	fn invocation_gate_rejects_reentrancy_stale_owners_and_resets_after_errors() {
		let (mut context, mut executor, owner) = fake_runtime();
		let gate = Rc::clone(&executor.gate);
		let reenter = InvocationSnapshot::new(
			RegistryRev::ZERO,
			ActionLease::new(owner, FakeAction::Reenter),
			DynamicInvocation::Mapping {
				mode: MappingMode::Insert,
				sequence: vec![stroke('r')],
			},
		);
		assert_eq!(
			gate.invoke(&mut executor, &mut context, reenter)
				.expect("outer invocation"),
			"outer"
		);
		assert_eq!(context.nested_error, Some(ActionError::ReentrantInvocation));
		assert!(context.calls.is_empty(), "nested action must not run");

		let failed = InvocationSnapshot::new(
			RegistryRev::ZERO,
			ActionLease::new(owner, FakeAction::Fail("expected failure".to_string())),
			DynamicInvocation::Option {
				canonical_name: "fake".to_string(),
				operation: OptionInvocation::Set(OptionValue::Bool(true)),
			},
		);
		assert_eq!(
			gate.invoke(&mut executor, &mut context, failed),
			Err(ActionError::Failed("expected failure".to_string()))
		);
		let after_failure = InvocationSnapshot::new(
			RegistryRev::ZERO,
			ActionLease::new(owner, FakeAction::Record("after".to_string())),
			DynamicInvocation::Option {
				canonical_name: "fake".to_string(),
				operation: OptionInvocation::Query,
			},
		);
		assert_eq!(
			gate.invoke(&mut executor, &mut context, after_failure)
				.expect("gate reset"),
			"after"
		);

		executor.live.remove(&owner);
		let stale = InvocationSnapshot::new(
			RegistryRev::ZERO,
			ActionLease::new(owner, FakeAction::Record("stale".to_string())),
			DynamicInvocation::Command {
				canonical_name: "stale".to_string(),
				spelling: "stale".to_string(),
				args: String::new(),
				bang: false,
			},
		);
		assert_eq!(
			gate.invoke(&mut executor, &mut context, stale),
			Err(ActionError::RetiredOwner(owner))
		);
		assert_eq!(context.calls, vec!["after"]);
	}

	#[test]
	fn schema_carries_every_declarative_variant_without_exposing_actions() {
		let mut registries = RuntimeRegistries::new();
		let owner = registries.allocate_owner().expect("owner");
		let mut batch = RegistrationBatch::new(owner);
		for (index, (args, completion)) in [
			(CommandArgRule::None, RuntimeCompletion::Path),
			(CommandArgRule::Optional, RuntimeCompletion::Language),
			(CommandArgRule::Required, RuntimeCompletion::Theme),
			(
				CommandArgRule::Optional,
				RuntimeCompletion::Static(vec!["one".to_string(), "two".to_string()]),
			),
		]
		.into_iter()
		.enumerate()
		{
			let mut entry = command(
				owner,
				format!("variant_command_{index}"),
				FakeAction::Record(index.to_string()),
			);
			entry.args = args;
			entry.completion = completion;
			batch.commands.push(entry);
		}
		for (index, mode) in [
			MappingMode::Normal,
			MappingMode::Visual,
			MappingMode::Insert,
			MappingMode::CommandLine,
			MappingMode::Search,
			MappingMode::Confirm,
			MappingMode::FileBrowser,
		]
		.into_iter()
		.enumerate()
		{
			batch.mappings.push(RuntimeMappingRegistration {
				mode,
				sequence: vec![KeyStroke::new(
					KeyCode::Function((index + 1) as u8),
					Modifiers::CONTROL | Modifiers::ALT,
				)],
				summary: format!("{mode:?} mapping"),
				target: if index == 0 {
					RuntimeMappingTarget::Command("variant_command_0".to_string())
				} else {
					RuntimeMappingTarget::Action(ActionLease::new(
						owner,
						FakeAction::Record(index.to_string()),
					))
				},
			});
		}
		for (index, kind) in [
			RuntimeOptionKind::Bool,
			RuntimeOptionKind::Integer,
			RuntimeOptionKind::String {
				require_non_empty: false,
			},
			RuntimeOptionKind::String {
				require_non_empty: true,
			},
		]
		.into_iter()
		.enumerate()
		{
			batch
				.options
				.push(option(owner, format!("variant_option_{index}"), kind));
		}
		registries
			.commit_owner_batch(batch)
			.expect("all schema variants");
		assert_eq!(registries.summary().command_count, 4);
		assert_eq!(registries.summary().mapping_count, 7);
		assert_eq!(registries.summary().option_count, 4);
	}

	#[test]
	fn option_query_display_is_bounded_at_construction_boundary() {
		let accepted = "x".repeat(MAX_OPTION_QUERY_DISPLAY_BYTES);
		assert_eq!(
			validate_option_query_display(accepted.clone()).expect("at cap"),
			accepted
		);
		assert_eq!(
			validate_option_query_display("x".repeat(MAX_OPTION_QUERY_DISPLAY_BYTES + 1)),
			Err(ActionError::QueryDisplayTooLarge {
				actual: MAX_OPTION_QUERY_DISPLAY_BYTES + 1,
				limit: MAX_OPTION_QUERY_DISPLAY_BYTES,
			})
		);
		assert_eq!(OptionValue::Integer(-7), OptionValue::Integer(-7));
		assert_eq!(
			OptionValue::String("value".to_string()),
			OptionValue::String("value".to_string())
		);
	}
}
