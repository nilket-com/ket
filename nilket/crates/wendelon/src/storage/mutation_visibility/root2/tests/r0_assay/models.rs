//! Deterministic bulk construction, excluded from measurements; no fake padding.
use super::*;
pub(super) const CASES: [&str; 9] = ["E", "P1", "P256", "P4096", "P32768", "B", "H", "R", "X"];
fn id(n: u64) -> [u8; 16] {
	let mut b = [0; 16];
	b[..8].copy_from_slice(&n.to_le_bytes());
	b[15] = 0x55;
	b
}

fn shaped_claim(mut c: ClaimProjection2, versions: usize, children: usize) -> ClaimProjection2 {
	assert!(children >= 1 && children <= 256 && versions >= children && versions <= 512);
	let child = c.manifest.children[0].clone();
	let version = child.versions[0].clone();
	let plan = c.plans[0].clone();
	let hlc = c.stamps[0].hlc;
	c.manifest.children.clear();
	c.plans.clear();
	c.stamps.clear();
	let mut global = 0u32;
	for i in 0..children {
		let count = if i == 0 { versions - children + 1 } else { 1 };
		let mut next = child.clone();
		next.ordinal = i as u32;
		next.versions.clear();
		for row in 0..count {
			let mut v = version.clone();
			v.ordinal = row as u32;
			v.key_token[..4].copy_from_slice(&global.to_le_bytes());
			global += 1;
			next.versions.push(v);
			c.stamps.push(VersionStamp2 {
				child: i as u32,
				row: row as u32,
				hlc,
			});
		}
		c.manifest.children.push(next);
		let mut next = plan.clone();
		next.child = i as u32;
		c.plans.push(next);
	}
	valid_claim(&c).unwrap();
	c
}
fn big_claim(c: ClaimProjection2) -> ClaimProjection2 {
	shaped_claim(c, 512, 1)
}
fn registries(r: &mut Root2, n: usize) {
	let selector = r.state.selectors.values().next().unwrap().clone();
	let authority = r.state.authorities.values().next().unwrap().clone();
	for i in 1..n {
		let mut s = selector.clone();
		s.binding.key_id = id(100_000 + i as u64);
		s.enrolled = l(100 + i as u64);
		r.state.selectors.insert(s.binding.encode().unwrap(), s);
		let mut a = authority.clone();
		a.authority.id = id(200_000 + i as u64);
		a.enrolled = l(5000 + i as u64);
		r.state
			.authorities
			.insert(authority_key(a.authority).unwrap(), a);
	}
}
fn history(r: &mut Root2, n: usize, cuts: usize) {
	// Disjoint single-point ranges in epoch1. One authority node is reused.
	let original = original();
	for i in 1..n {
		let mut o = original.clone();
		o.lineage = id(i as u64);
		o.log = l(10_000 + i as u64);
		o.lease.range_start = 1000 + i as u64;
		o.lease.range_end = o.lease.range_start;
		r.state.lineages.insert(
			o.lineage,
			LineageRecord2 {
				latest: o,
				closure: None,
			},
		);
	}
	if cuts > 0 {
		for i in 0..cuts {
			r.state.cuts.push(EpochCut2 {
				prior: 1 + i as u64,
				new: 2 + i as u64,
				log: l(3_000_000 + i as u64),
				configuration: id(i as u64 + 1).repeat(2).try_into().unwrap(),
			});
		}
		let keys = r.state.lineages.iter().map(|(k, _)| *k).collect::<Vec<_>>();
		for key in keys {
			let g = r.state.lineages.get_mut(&key).unwrap();
			g.closure = Some(Closure2 {
				cause: 3,
				log: r.state.cuts[0].log,
				successor: None,
			});
		}
		// Last lineage is a current-epoch open range for the renewal cost arm.
		if n > 1 {
			let g = r.state.lineages.get_mut(&id((n - 1) as u64)).unwrap();
			g.latest.lease.epoch = 1 + cuts as u64;
			g.latest.log = l(4_000_000);
			g.closure = None;
		}
	}
}
fn parents(r: &mut Root2, n: usize, big: bool) {
	let small = claim();
	let large = big_claim(claim());
	let small_bytes = small.held_bytes().unwrap() as usize;
	let large_bytes = large.held_bytes().unwrap() as usize;
	let big_count = if big && n > 32 {
		((PARENT_BYTES2 - n * small_bytes) / (large_bytes - small_bytes)).min(n)
	} else {
		if big { n } else { 0 }
	};
	let remainder = if big && n > 32 {
		PARENT_BYTES2 - (n * small_bytes + big_count * (large_bytes - small_bytes))
	} else {
		0
	};
	let mut special = None;
	if remainder > 0 {
		// Fixed exhaustive shape search, no padding. Try replacing one large or one small claim.
		'found: for base_big in [true, false] {
			if (base_big && big_count == 0) || (!base_big && big_count == n) {
				continue;
			}
			let baseline = if base_big { large_bytes } else { small_bytes };
			for children in 1..=256 {
				for versions in children..=512 {
					let size = small_bytes + 93 * (versions - 1) + 101 * (children - 1);
					if size == baseline + remainder {
						let c = shaped_claim(claim(), versions, children);
						assert_eq!(c.held_bytes().unwrap() as usize, size);
						special = Some((if base_big { big_count - 1 } else { n - 1 }, c));
						break 'found;
					}
				}
			}
		}
		assert!(
			special.is_some(),
			"declared exact held-byte fixture unreachable"
		);
	}
	let c = small.clone();
	let authority = authority_key(c.authority).unwrap();
	let mut revoked = r.state.authorities.get(&authority).unwrap().clone();
	revoked.authority.id = id(900_000);
	revoked.enrolled = l(9);
	revoked.revoked = Some(l(2_000_000));
	// Some parents already accepted before this revoke; some remain reserved;
	// some definitively refused after it. No invalid invented refusal cause.
	if n >= 3 {
		r.state
			.authorities
			.insert(authority_key(revoked.authority).unwrap(), revoked.clone());
	}
	for i in 0..n {
		let mut c = if let Some((index, c)) = &special {
			if *index == i {
				c.clone()
			} else if i < big_count {
				large.clone()
			} else {
				small.clone()
			}
		} else if i < big_count {
			large.clone()
		} else {
			small.clone()
		};
		c.manifest.identity.sequence = 1 + i as u64;
		if i % 3 == 2 || i % 6 == 3 {
			c.authority = revoked.authority;
		}
		let terminal = match i % 3 {
			1 => Some(Terminal2::Accepted {
				first: l(1_000_001 + 3 * i as u64),
				evidence: [0x33; 32],
			}),
			2 => Some(Terminal2::Refused {
				first: l(2_000_001 + i as u64),
				cause: 4,
				anchor: l(2_000_000),
			}),
			_ => None,
		};
		let p = ParentRecord2 {
			held_bytes: c.held_bytes().unwrap(),
			claim: c,
			reservation: l(1_000_000 + 3 * i as u64),
			terminal,
		};
		r.state
			.parents
			.insert(p.claim.manifest.identity.encode().unwrap(), p);
	}
}
pub(super) fn model(case: &str) -> Root2 {
	let mut r = root("root_reserved");
	r.state.parents.clear();
	let (p, b, h, c, reg) = match case {
		"E" => (0, false, 1, 0, 1),
		"P1" => (1, false, 1, 0, 1),
		"P256" => (256, false, 1, 0, 1),
		"P4096" => (4096, false, 1, 0, 1),
		"P32768" => (32768, false, 1, 0, 1),
		"H" => (0, false, 16384, 4096, 1),
		"R" => (0, false, 1, 0, 4096),
		"B" | "X" => {
			let bytes = big_claim(claim()).held_bytes().unwrap() as usize;
			(
				if case == "X" {
					PARENT_SLOTS2
				} else {
					PARENT_BYTES2 / bytes
						+ (PARENT_BYTES2 % bytes) / (claim().held_bytes().unwrap() as usize)
				},
				true,
				if case == "X" { 16384 } else { 1 },
				if case == "X" { 4096 } else { 0 },
				if case == "X" { 4096 } else { 1 },
			)
		}
		"R-small" => (0, false, 1, 0, 8),
		"H-small" => (0, false, 8, 4, 1),
		"B-small" => (4, true, 1, 0, 1),
		"X-small" => (4, true, 8, 4, 8),
		_ => panic!("undeclared R0 case"),
	};
	registries(&mut r, reg);
	parents(&mut r, p, b);
	if r.state.authorities.len() > REGISTRY_COUNT2 {
		let key = authority_key(AuthorityRef2 {
			id: id(200_000 + (reg - 1) as u64),
			revision: r
				.state
				.authorities
				.values()
				.next()
				.unwrap()
				.authority
				.revision,
		})
		.unwrap();
		r.state.authorities.remove(&key).unwrap();
	}
	history(&mut r, h, c);
	r.state.covered = l(5_000_000);
	r.state.fingerprint = [0x44; 32];
	r.state.command_digest = [0x66; 32];
	validate_state(&r.state).unwrap();
	r.state.derived =
		crate::storage::mutation_visibility::root2::incremental::Derived2::build(&r.state).unwrap();
	r
}
pub(super) struct Arm {
	pub name: &'static str,
	pub command: Command2,
	pub expectation: &'static str,
	pub limits: Option<Limits2>,
	pub same_entry: bool,
}
fn arm(name: &'static str, command: Command2, expectation: &'static str) -> Arm {
	Arm {
		name,
		command,
		expectation,
		limits: None,
		same_entry: false,
	}
}
fn candidate(r: &Root2) -> Option<ClaimProjection2> {
	let mut c = claim();
	c.manifest.identity.sequence = u64::MAX - 1;
	let g = r.state.lineages.get(&c.original.lineage)?;
	let a = r.state.authorities.get(&authority_key(c.authority).ok()?)?;
	if g.closure.is_some() || a.revoked.is_some() {
		return None;
	}
	c.original = g.latest.clone();
	c.stamps
		.iter_mut()
		.for_each(|s| s.hlc = c.original.lease.start);
	Some(c)
}
pub(super) fn arms(r: &Root2) -> Vec<Arm> {
	let cover = Command2::Cover {
		kind: 0,
		external: [1; 32],
	};
	let mut a = vec![
		arm("cover", cover.clone(), "Covered"),
		arm(
			"denied",
			Command2::Bootstrap {
				epoch: 1,
				control: [1; 32],
				length: 1,
			},
			"Denied",
		),
	];
	let mut replay = arm("already_processed", cover, "AlreadyProcessed");
	replay.same_entry = true;
	a.push(replay);
	if let Some(p) = r.state.parents.values().next() {
		let mut c = (*p.claim).clone();
		c.accounting_delta += 1;
		a.push(arm("conflict", Command2::Reserve(c), "Conflict"));
	}
	if let Some(p) = r.state.parents.values().find(|p| p.terminal.is_some()) {
		a.push(arm(
			"parent_terminal_replay",
			Command2::Reserve((*p.claim).clone()),
			match p.terminal {
				Some(Terminal2::Accepted { .. }) => "AcceptedReplay",
				Some(Terminal2::Refused { .. }) => "Refused",
				None => unreachable!(),
			},
		));
	}
	if let Some(c) = candidate(r) {
		let mut limits = Limits2::default();
		limits.slots = r.state.parents.len();
		let mut capacity = arm(
			"capacity_slots_equal",
			Command2::Reserve(c.clone()),
			"Capacity",
		);
		capacity.limits = Some(limits);
		a.push(capacity);
		let mut bytes = Limits2::default();
		bytes.parent = r.usage().unwrap().held + c.held_bytes().unwrap() as usize - 1;
		if bytes.parent <= PARENT_BYTES2 && r.state.parents.len() < PARENT_SLOTS2 {
			let mut x = arm(
				"capacity_bytes_plus_one",
				Command2::Reserve(c.clone()),
				"Capacity",
			);
			x.limits = Some(bytes);
			a.push(x);
		}
		if r.state.parents.len() < PARENT_SLOTS2
			&& r.usage().unwrap().held + c.held_bytes().unwrap() as usize <= PARENT_BYTES2
		{
			a.push(arm("reserve", Command2::Reserve(c), "Reserved"));
		}
	}
	for p in r.state.parents.values().filter(|p| p.terminal.is_none()) {
		if let Some((cause, anchor)) = trigger(&r.state, p).unwrap() {
			if !a.iter().any(|a| a.name == "resolve") {
				a.push(arm(
					"resolve",
					Command2::Resolve {
						identity: p.claim.manifest.identity,
						digest: p.claim.digest().unwrap(),
						reservation: p.reservation,
						cause,
						anchor,
					},
					"Refused",
				));
			}
		} else if !a.iter().any(|a| a.name == "finalize") {
			let mut att = attestation();
			att.reservation = p.reservation;
			att.claim_digest = p.claim.digest().unwrap();
			att.grant = p.claim.original.reference().unwrap();
			att.authority = p.claim.authority;
			a.push(arm(
				"finalize",
				Command2::Finalize {
					identity: p.claim.manifest.identity,
					digest: p.claim.digest().unwrap(),
					reservation: p.reservation,
					attestation: att,
				},
				"TerminalEffect",
			));
		}
	}
	if let Some(g) = r.state.lineages.values().find(|g| g.closure.is_none()) {
		let mut lease = g.latest.lease.clone();
		lease.start.physical_ms += 1;
		lease.expiry.physical_ms += 1;
		a.push(arm(
			"renewal",
			Command2::Grant {
				lineage: g.latest.lineage,
				incarnation: g.latest.incarnation,
				lease,
				predecessor: g.latest.predecessor,
			},
			"Granted",
		));
	}
	a
}
pub(super) fn inapplicable(r: &Root2) -> Vec<(&'static str, &'static str)> {
	let a = arms(r);
	let names = a.iter().map(|a| a.name).collect::<Vec<_>>();
	[
		"conflict",
		"parent_terminal_replay",
		"reserve",
		"capacity_slots_equal",
		"capacity_bytes_plus_one",
		"finalize",
		"resolve",
		"renewal",
	]
	.into_iter()
	.filter(|n| !names.contains(n))
	.map(|n| {
		(
			n,
			"declared model has no eligible input for this semantic arm; no replacement",
		)
	})
	.collect()
}
