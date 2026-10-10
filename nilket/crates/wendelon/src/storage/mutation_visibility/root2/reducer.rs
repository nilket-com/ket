use super::{codec::*, command::*, state::Root2, trust::*, types::*, validation::*};
use super::{
	incremental::{Derived2, cached_capacity, update},
	persistent::Map,
	stored::Cuts2,
};
fn bootstrap_root(
	g: [u8; 16],
	c: [u8; 16],
	l: Log2,
	fp: Digest2,
	d: Digest2,
	command: &Command2,
	verified: &VerifiedCommand2,
) -> Result2<(Root2, ApplyResult2)> {
	let Command2::Bootstrap {
		epoch,
		control,
		length,
	} = command
	else {
		return Err(RootError2::TrustMismatch);
	};
	if !verified.authorized() {
		return Err(RootError2::TrustMismatch);
	}
	let mut state = State2 {
		generation: g,
		cluster: c,
		bootstrap: l,
		covered: l,
		fingerprint: fp,
		command_digest: d,
		initial_epoch: *epoch,
		control_digest: *control,
		control_len: *length,
		selectors: Map::new(),
		authorities: Map::new(),
		permissions: Map::new(),
		lineages: Map::new(),
		cuts: Cuts2::default(),
		parents: Map::new(),
		derived: Derived2::default(),
	};
	state.derived = Derived2::build(&state)?;
	return Ok((
		Root2 {
			state,
			limits: Limits2::default(),
		},
		ApplyResult2::Registered(l),
	));
}
fn denied(reason: u16, reference: Option<Log2>) -> ApplyResult2 {
	ApplyResult2::Denied { reason, reference }
}
impl Root2 {
	pub fn reduce(
		root: Option<&Root2>,
		entry: &TrustedEntry2,
		verified: &VerifiedCommand2,
		command: &Command2,
	) -> Result2<(Root2, ApplyResult2)> {
		let (g, c, l, fp, d) = entry.fields();
		valid_log(l)?;
		let digest = command.digest(g, c)?;
		if digest != d || !verified.matches(g, c, d) {
			return Err(RootError2::TrustMismatch);
		}
		let Some(old) = root else {
			return bootstrap_root(g, c, l, fp, d, command, verified);
		};
		let s = &old.state;
		if (g, c) != (s.generation, s.cluster) {
			return Err(RootError2::TrustMismatch);
		}
		// Dormant: no issuer gate exists, so an Admission permission command has no result at all,
		// not even an exact-replay AlreadyProcessed.
		if let Command2::AdmissionPermission(_) = command {
			return Err(RootError2::TrustMismatch);
		}
		if l.index == s.covered.index {
			if (l, fp, d) != (s.covered, s.fingerprint, s.command_digest) {
				return Err(RootError2::TrustMismatch);
			}
			return Ok((old.clone(), ApplyResult2::AlreadyProcessed));
		}
		if s.covered.index.checked_add(1) != Some(l.index) {
			return Err(RootError2::LineageGap);
		}
		// Immutable path-copy candidate: prior roots and payloads remain unchanged.
		let mut next = old.clone();
		let mut result = if verified.authorized() {
			apply(&mut next, command, l, verified)?
		} else {
			denied(8, None)
		};
		next.state.covered = l;
		next.state.fingerprint = fp;
		next.state.command_digest = d;
		match update(s, &mut next.state, command) {
			Ok(()) => {}
			Err(RootError2::Limit) => {
				next = old.clone();
				next.state.covered = l;
				next.state.fingerprint = fp;
				next.state.command_digest = d;
				result = denied(11, None);
			}
			Err(e) => return Err(e),
		}
		cached_capacity(&next.state, next.limits)?;
		Ok((next, result))
	}
}
fn apply(root: &mut Root2, cmd: &Command2, l: Log2, v: &VerifiedCommand2) -> Result2<ApplyResult2> {
	let s = &mut root.state;
	let limits = root.limits;
	match cmd {
		Command2::Bootstrap { .. } => Ok(denied(1, Some(s.bootstrap))),
		Command2::Cover { .. } => Ok(ApplyResult2::Covered),
		Command2::EnrollSelector(b) => {
			let k = b.encode().map_err(|_| RootError2::Codec)?;
			if b.generation != s.generation {
				return Ok(denied(9, None));
			}
			if let Some(x) = s.selectors.get(&k) {
				return Ok(ApplyResult2::Registered(x.enrolled));
			}
			if s.selectors.len() >= limits.registrations
				|| s.derived
					.usage
					.registry
					.checked_add(68)
					.ok_or(RootError2::Overflow)?
					> limits.registry
			{
				return Ok(denied(11, None));
			}
			s.selectors.insert(
				k,
				Selector2 {
					binding: *b,
					enrolled: l,
				},
			);
			Ok(ApplyResult2::Registered(l))
		}
		Command2::EnrollAuthority {
			authority,
			node,
			key,
		} => {
			let k = authority_key(*authority)?;
			if let Some(a) = s.authorities.get(&k) {
				return Ok(if a.node == *node && a.public_key == *key {
					ApplyResult2::Registered(a.enrolled)
				} else {
					denied(2, Some(a.enrolled))
				});
			}
			if s.authorities.len() >= limits.registrations
				|| s.derived
					.usage
					.registry
					.checked_add(121)
					.ok_or(RootError2::Overflow)?
					> limits.registry
			{
				return Ok(denied(11, None));
			}
			s.authorities.insert(
				k,
				AuthorityRegistration2 {
					authority: *authority,
					node: *node,
					public_key: *key,
					enrolled: l,
					revoked: None,
				},
			);
			Ok(ApplyResult2::Registered(l))
		}
		Command2::RevokeAuthority(a) => {
			let key = authority_key(*a)?;
			let Some(a) = s.authorities.get(&key) else {
				return Ok(denied(9, None));
			};
			if let Some(first) = a.revoked {
				return Ok(ApplyResult2::Registered(first));
			}
			let a = s
				.authorities
				.get_mut(&key)
				.ok_or(RootError2::RootInvariant)?;
			let first = *a.revoked.get_or_insert(l);
			Ok(ApplyResult2::Registered(first))
		}
		Command2::Grant {
			lineage,
			incarnation,
			lease,
			predecessor,
		} => apply_grant(
			s,
			limits,
			OriginalGrant2 {
				lineage: *lineage,
				incarnation: *incarnation,
				lease: lease.clone(),
				log: l,
				predecessor: *predecessor,
			},
		),
		Command2::Release { lineage, latest } => {
			let Some(g) = s.lineages.get(lineage) else {
				return Ok(denied(9, None));
			};
			if let Some(closure) = g.closure {
				return Ok(ApplyResult2::Closed {
					lineage: *lineage,
					cause: closure.cause,
					first: closure.log,
				});
			}
			if g.latest.reference()? != *latest {
				return Ok(denied(7, Some(g.latest.log)));
			}
			let g = s
				.lineages
				.get_mut(lineage)
				.ok_or(RootError2::RootInvariant)?;
			g.closure = Some(Closure2 {
				cause: 1,
				log: l,
				successor: None,
			});
			Ok(ApplyResult2::Registered(l))
		}
		Command2::Cut {
			expected,
			next,
			configuration,
		} => {
			if *expected != epoch(s) || expected.checked_add(1) != Some(*next) {
				return Ok(denied(3, None));
			}
			if s.cuts.len() >= limits.cuts
				|| s.derived
					.usage
					.history
					.checked_add(72)
					.ok_or(RootError2::Overflow)?
					> limits.history
			{
				return Ok(denied(11, None));
			}
			s.cuts.push(EpochCut2 {
				prior: *expected,
				new: *next,
				log: l,
				configuration: *configuration,
			});
			let keys = s.lineages.iter().map(|(k, _)| *k).collect::<Vec<_>>();
			for key in keys {
				let g = s.lineages.get(&key).ok_or(RootError2::RootInvariant)?;
				if g.latest.lease.epoch == *expected && g.closure.is_none() {
					let g = s.lineages.get_mut(&key).ok_or(RootError2::RootInvariant)?;
					g.closure = Some(Closure2 {
						cause: 3,
						log: l,
						successor: None,
					})
				}
			}
			Ok(ApplyResult2::Registered(l))
		}
		Command2::Reserve(claim) => apply_reserve(s, limits, claim, l, v),
		// Unreachable: `reduce` refuses this command before replay, gap or authorization.
		Command2::AdmissionPermission(_) => Err(RootError2::TrustMismatch),
		Command2::Finalize {
			identity,
			digest,
			reservation,
			attestation,
		} => apply_terminal(
			s,
			identity,
			*digest,
			*reservation,
			l,
			Some(attestation),
			None,
			v,
		),
		Command2::Resolve {
			identity,
			digest,
			reservation,
			cause,
			anchor,
		} => apply_terminal(
			s,
			identity,
			*digest,
			*reservation,
			l,
			None,
			Some((*cause, *anchor)),
			v,
		),
	}
}
fn apply_grant(s: &mut State2, limits: Limits2, new: OriginalGrant2) -> Result2<ApplyResult2> {
	// These relations depend on the trusted entry, not the command framing.
	if new.lease.min_commit_index > new.log.index {
		return Ok(denied(17, None));
	}
	if new
		.predecessor
		.is_some_and(|p| p.lineage == new.lineage || p.log.index >= new.log.index)
	{
		return Ok(denied(
			7,
			s.lineages.get(&new.lineage).map(|g| g.latest.log),
		));
	}
	valid_original(&new)?;
	let l = &new.lease;
	if let Some(old) = s.lineages.get(&new.lineage) {
		if let Some(closure) = old.closure {
			return Ok(
				if same_lineage(&old.latest, &new) && old.latest.lease == new.lease {
					ApplyResult2::Closed {
						lineage: new.lineage,
						cause: closure.cause,
						first: closure.log,
					}
				} else {
					denied(4, Some(closure.log))
				},
			);
		}
	}
	if l.epoch != epoch(s) {
		return Ok(denied(3, None));
	}
	if let Some(old) = s.lineages.get(&new.lineage) {
		if same_lineage(&old.latest, &new) && old.latest.lease == new.lease {
			return Ok(ApplyResult2::Granted(old.clone()));
		}
		if !same_lineage(&old.latest, &new)
			|| !dominates(l, &old.latest.lease)
			|| (l.start == old.latest.lease.start && l.expiry == old.latest.lease.expiry)
		{
			return Ok(denied(5, Some(old.latest.log)));
		}
		let updated = LineageRecord2 {
			latest: new,
			closure: None,
		};
		s.lineages.insert(updated.latest.lineage, updated.clone());
		return Ok(ApplyResult2::Granted(updated));
	}
	for g in s.lineages.values() {
		let p = &g.latest.lease;
		if p.epoch == l.epoch
			&& p.range_start <= l.range_end
			&& l.range_start <= p.range_end
			&& (p.range_start != l.range_start || p.range_end != l.range_end)
		{
			return Ok(denied(6, Some(g.latest.log)));
		}
	}
	// A same-range predecessor, including a closed one, is mandatory when such history exists.
	let previous = s
		.lineages
		.values()
		.filter(|g| {
			g.latest.lease.range_start == l.range_start && g.latest.lease.range_end == l.range_end
		})
		.max_by_key(|g| g.latest.log.index);
	if let Some(previous) = previous {
		if new.predecessor != Some(previous.latest.reference()?) {
			return Ok(denied(7, Some(previous.latest.log)));
		}
		if previous.latest.lease.epoch == l.epoch
			&& (!dominates(l, &previous.latest.lease)
				|| (l.start == previous.latest.lease.start
					&& l.expiry == previous.latest.lease.expiry))
		{
			return Ok(denied(5, Some(previous.latest.log)));
		}
	} else if new.predecessor.is_some() {
		return Ok(denied(7, None));
	}
	if s.lineages.len() >= limits.lineages
		|| s.derived
			.usage
			.history
			.checked_add(298)
			.ok_or(RootError2::Overflow)?
			> limits.history
	{
		return Ok(denied(11, None));
	}
	if let Some(p) = new.predecessor {
		let prior = s
			.lineages
			.get_mut(&p.lineage)
			.ok_or(RootError2::RootInvariant)?;
		if prior.closure.is_none() {
			prior.closure = Some(Closure2 {
				cause: 2,
				log: new.log,
				successor: Some(new.lineage),
			})
		}
	}
	let g = LineageRecord2 {
		latest: new,
		closure: None,
	};
	s.lineages.insert(g.latest.lineage, g.clone());
	Ok(ApplyResult2::Granted(g))
}
fn apply_reserve(
	s: &mut State2,
	limits: Limits2,
	c: &ClaimProjection2,
	l: Log2,
	v: &VerifiedCommand2,
) -> Result2<ApplyResult2> {
	valid_claim(c)?;
	let id = c.manifest.identity;
	if id.generation != s.generation {
		return Ok(denied(9, None));
	}
	let key = id.encode().map_err(|_| RootError2::Codec)?;
	if let Some(p) = s.parents.get(&key) {
		return Ok(if p.claim.as_ref() == c {
			ApplyResult2::Parent {
				record: p.record(),
				new_effect: false,
			}
		} else {
			denied(2, Some(p.reservation))
		});
	}
	let Some(lineage) = s.lineages.get(&c.original.lineage) else {
		return Ok(denied(9, None));
	};
	if lineage.closure.is_some() {
		return Ok(denied(4, lineage.closure.map(|c| c.log)));
	}
	if c.original.lease.epoch != epoch(s) {
		return Ok(denied(3, None));
	}
	if !old_original_fits(&c.original, &lineage.latest)
		|| (c.original != lineage.latest && v.historical() != Some(c.original.reference()?.digest))
	{
		return Ok(denied(9, None));
	}
	let Some(a) = s.authorities.get(&authority_key(c.authority)?) else {
		return Ok(denied(9, None));
	};
	if a.revoked.is_some() {
		return Ok(denied(16, a.revoked));
	}
	if a.node
		!= (NodeRef2 {
			node: c.original.lease.holder,
			incarnation: c.original.incarnation,
		}) || !s.selectors.contains_key(
		&c.manifest
			.binding()
			.encode()
			.map_err(|_| RootError2::Codec)?,
	) {
		return Ok(denied(9, None));
	}
	if let Some((reason, reference)) = admission_permission(s, c, l.index)? {
		return Ok(denied(reason, reference));
	}
	let held = c.held_bytes()?;
	let u = s.derived.usage;
	if u.parents >= limits.slots
		|| u.held
			.checked_add(usize::try_from(held).map_err(|_| RootError2::Overflow)?)
			.ok_or(RootError2::Overflow)?
			> limits.parent
	{
		return Ok(denied(10, None));
	}
	let record = ParentRecord2 {
		claim: c.clone(),
		reservation: l,
		held_bytes: held,
		terminal: None,
	};
	s.parents.insert(key, record.clone());
	Ok(ApplyResult2::Parent {
		record,
		new_effect: false,
	})
}
fn apply_terminal(
	s: &mut State2,
	id: &crate::storage::mutation_visibility::MutationIdentity,
	digest: Digest2,
	reservation: Log2,
	l: Log2,
	a: Option<&HolderAttestation2>,
	resolve: Option<(u8, Log2)>,
	v: &VerifiedCommand2,
) -> Result2<ApplyResult2> {
	if id.generation != s.generation {
		return Ok(denied(9, None));
	}
	let key = id.encode().map_err(|_| RootError2::Codec)?;
	let Some(p) = s.parents.get(&key) else {
		return Ok(denied(12, None));
	};
	if p.claim.digest()? != digest || p.reservation != reservation {
		return Ok(denied(13, Some(p.reservation)));
	}
	if p.terminal.is_some() {
		return Ok(ApplyResult2::Parent {
			record: p.record(),
			new_effect: false,
		});
	}
	let closure = trigger(s, p)?;
	let terminal = if let Some((cause, anchor)) = closure {
		if resolve.is_some_and(|x| x != (cause, anchor)) {
			return Ok(denied(15, Some(anchor)));
		}
		Terminal2::Refused {
			first: l,
			cause,
			anchor,
		}
	} else {
		if resolve.is_some() {
			return Ok(denied(14, None));
		}
		let a = a.ok_or(RootError2::RootInvariant)?;
		if a.grant != p.claim.original.reference()?
			|| a.reservation != p.reservation
			|| a.claim_digest != digest
			|| a.authority != p.claim.authority
		{
			return Ok(ApplyResult2::RejectedEvidence(2));
		}
		if a.children.len() != p.claim.plans.len() {
			return Ok(ApplyResult2::RejectedEvidence(3));
		}
		if v.evidence() == 0 {
			return Ok(ApplyResult2::PendingEvidence(2));
		}
		if v.evidence() != 1 {
			return Ok(ApplyResult2::RejectedEvidence(1));
		}
		let holder = NodeRef2 {
			node: p.claim.original.lease.holder,
			incarnation: p.claim.original.incarnation,
		};
		for (c, plan) in a.children.iter().zip(&p.claim.plans) {
			if c.child != plan.child || c.nodes.iter().any(|n| !plan.nodes.contains(n)) {
				return Ok(ApplyResult2::RejectedEvidence(3));
			}
			if c.nodes.len() < plan.nodes.len() / 2 + 1 || !c.nodes.contains(&holder) {
				return Ok(ApplyResult2::PendingEvidence(1));
			}
		}
		Terminal2::Accepted {
			first: l,
			evidence: hash(b"VISROOT2-EVIDENCE\0", &a.encode()?),
		}
	};
	let p = s.parents.get_mut(&key).ok_or(RootError2::RootInvariant)?;
	p.terminal = Some(terminal);
	Ok(ApplyResult2::Parent {
		record: p.record(),
		new_effect: matches!(terminal, Terminal2::Accepted { .. }),
	})
}
