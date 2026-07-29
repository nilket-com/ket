use super::*;

// ADR 512: reachable-key reduction for decorrelated scalar aggregates.
//
// A `LeftSemi` whose build side is a superset of the recombine's outer key
// domain eliminates only groups the plain-equality `LeftOuter` recombine can
// never match, so every aggregate in the pinned grouped-decorrelation shape is
// preserved — COUNT included, because its zero-versus-NULL distinction exists
// only for keys the outer side presents, and those groups are never
// eliminated. The rule engages only the grouped shape (correlation pairs
// mapping one-to-one onto group keys); the keyless `Cross`/global-scalar
// recombine never engages. Every precondition is proved structurally; any
// failure declines and leaves the plan byte-identical.

pub(super) struct ReachableKeySource {
	pub(super) source: LogicalPlan,
	pub(super) source_keys: Vec<Expr>,
}

fn plain_column(expr: &Expr) -> Option<&ColumnRef> {
	match expr {
		Expr::Column(column) => Some(column),
		_ => None,
	}
}

fn columns_structurally_equal(a: &ColumnRef, b: &ColumnRef) -> bool {
	a.name == b.name
		&& match (&a.qualifier, &b.qualifier) {
			(Some(left), Some(right)) => left == right,
			_ => true,
		}
}

/// A candidate semi build must be a chain of shrink-only, replay-stable nodes
/// over exactly one base relation. The chain re-executes as a second scan, so
/// every predicate and projection in it must be deterministic — a volatile
/// branch could produce a different subset on re-evaluation and break the
/// superset relationship. Returns whether the chain carries at least one
/// deterministic filter; an unfiltered base relation is domain-complete under
/// referential integrity and would reduce nothing.
fn base_chain_has_deterministic_filter(plan: &LogicalPlan) -> Option<bool> {
	match plan {
		LogicalPlan::Scan { .. } | LogicalPlan::ScanNamed { .. } => Some(false),
		LogicalPlan::Filter { input, predicate } => {
			if !predicate.is_deterministic() {
				return None;
			}
			base_chain_has_deterministic_filter(input).map(|_| true)
		}
		LogicalPlan::Distinct { input } => base_chain_has_deterministic_filter(input),
		LogicalPlan::Alias { input, .. } => base_chain_has_deterministic_filter(input),
		LogicalPlan::Project { input, exprs, .. } => {
			if exprs.iter().any(|expr| !expr.is_deterministic()) {
				return None;
			}
			base_chain_has_deterministic_filter(input)
		}
		_ => None,
	}
}

fn project_passthrough_column(
	exprs: &[Expr],
	output_names: &[Option<String>],
	column: &ColumnRef,
) -> Option<ColumnRef> {
	for (expr, output_name) in exprs.iter().zip(output_names) {
		let matches = match output_name {
			Some(name) => *name == column.name.0,
			None => plain_column(expr)
				.map(|source| columns_structurally_equal(source, column))
				.unwrap_or(false),
		};
		if matches {
			return plain_column(expr).cloned();
		}
	}
	None
}

impl<'a> Binder<'a> {
	async fn reduction_scope_owns_column(&self, plan: &LogicalPlan, column: &ColumnRef) -> bool {
		match self.logical_scope_for_outer_demotion(plan).await {
			Ok(Some(scope)) => matches!(
				scope.resolve_join_column(column, "reachable-key reduction"),
				Ok(Some(_))
			),
			_ => false,
		}
	}

	/// Trace one outer correlation key down the outer plan to a filtered
	/// base-relation chain whose value domain is a superset of the key's.
	/// Nodes above the source only remove rows, so they are skipped without
	/// any determinism requirement; an equality partner at an `Inner` join
	/// bounds the key across the pair, so the partner side is queued as a
	/// fallback trace. Any unrecognized shape ends that trace.
	async fn trace_key_to_filtered_base<'p>(
		&self,
		outer: &'p LogicalPlan,
		key: ColumnRef,
	) -> Option<(&'p LogicalPlan, ColumnRef)> {
		let mut candidates: Vec<(&'p LogicalPlan, ColumnRef)> = vec![(outer, key)];
		let mut steps = 0usize;
		while let Some((mut plan, mut column)) = candidates.pop() {
			loop {
				steps += 1;
				if steps > 64 {
					return None;
				}
				if base_chain_has_deterministic_filter(plan) == Some(true)
					&& self.reduction_scope_owns_column(plan, &column).await
				{
					return Some((plan, column));
				}
				match plan {
					LogicalPlan::Filter { input, .. }
					| LogicalPlan::Distinct { input }
					| LogicalPlan::Sort { input, .. }
					| LogicalPlan::Exchange { input, .. }
					| LogicalPlan::Take { input, .. } => plan = input,
					LogicalPlan::Project {
						input,
						exprs,
						output_names,
					} => {
						match project_passthrough_column(exprs, output_names, &column) {
							Some(mapped) => column = mapped,
							None => break,
						}
						plan = input;
					}
					LogicalPlan::Join {
						left,
						right,
						kind,
						on,
						..
					} => {
						if matches!(kind, JoinKind::Inner) {
							for (a, b) in on {
								if let (Some(a), Some(b)) = (plain_column(a), plain_column(b)) {
									if columns_structurally_equal(a, &column) {
										candidates.push((right, b.clone()));
									} else if columns_structurally_equal(b, &column) {
										candidates.push((left, a.clone()));
									}
								}
							}
						}
						let owns_left = self.reduction_scope_owns_column(left, &column).await;
						let owns_right = self.reduction_scope_owns_column(right, &column).await;
						plan = match (owns_left, owns_right) {
							(true, false) => left,
							(false, true) => right,
							_ => break,
						};
					}
					_ => break,
				}
			}
		}
		None
	}

	/// Derive the semi build for the given outer correlation keys. All keys
	/// must trace to the same filtered base-relation chain, or the rule
	/// declines.
	pub(super) async fn derive_reachable_key_source(
		&self,
		outer: &LogicalPlan,
		outer_keys: &[Expr],
	) -> Option<ReachableKeySource> {
		if outer_keys.is_empty() {
			return None;
		}
		let mut source_plan: Option<&LogicalPlan> = None;
		let mut source_keys = Vec::with_capacity(outer_keys.len());
		for key in outer_keys {
			let column = plain_column(key)?.clone();
			let (subtree, resolved) = self.trace_key_to_filtered_base(outer, column).await?;
			match source_plan {
				None => source_plan = Some(subtree),
				Some(existing) => {
					if !std::ptr::eq(existing, subtree) {
						return None;
					}
				}
			}
			source_keys.push(Expr::Column(resolved));
		}
		Some(ReachableKeySource {
			source: source_plan?.clone(),
			source_keys,
		})
	}

	/// Path B (deferred correlated scalar): reduce the inner before the
	/// grouped aggregate is constructed. `pairs` are the classified
	/// (outer key, inner key) correlation columns.
	pub(super) async fn reduce_deferred_scalar_inner(
		&self,
		outer: &LogicalPlan,
		inner: LogicalPlan,
		pairs: &[(Expr, Expr)],
	) -> LogicalPlan {
		if !self.config.scalar_aggregate_input_reduction || pairs.is_empty() {
			return inner;
		}
		let outer_keys: Vec<Expr> = pairs
			.iter()
			.map(|(outer_key, _)| outer_key.clone())
			.collect();
		let Some(reduced) = self.derive_reachable_key_source(outer, &outer_keys).await else {
			return inner;
		};
		LogicalPlan::Join {
			left: Box::new(inner),
			right: Box::new(reduced.source),
			kind: JoinKind::LeftSemi,
			on: pairs
				.iter()
				.map(|(_, inner_key)| inner_key.clone())
				.zip(reduced.source_keys)
				.collect(),
			on_filter: None,
			null_safe_keys: false,
			strategy_intent: JoinStrategyIntent::Auto,
		}
	}

	/// Path A (qualified correlated scalar): recognize the recombine shape the
	/// frontend constructs — `Alias[key_0..key_{n-1}, value](Aggregate(group,
	/// Exchange(group, inner)))` joined `LeftOuter` on plain-equality pairs
	/// covering the group keys one-to-one — and return the reduced right side.
	/// Declines an already-reduced inner, so the caller's re-entry terminates.
	pub(super) async fn try_reduce_scalar_recombine(
		&self,
		outer: &LogicalPlan,
		right: &LogicalPlan,
		on: &[(Expr, Expr)],
	) -> Option<LogicalPlan> {
		if !self.config.scalar_aggregate_input_reduction || on.is_empty() {
			return None;
		}
		// The auto-join placement may already have wrapped the recombine's
		// build side in an Exchange; recognize through it and preserve it on
		// the rebuilt side.
		let (right_exchange, right) = match right {
			LogicalPlan::Exchange {
				input,
				partition_count,
				key,
			} => (Some((*partition_count, key.clone())), input.as_ref()),
			other => (None, other),
		};
		let LogicalPlan::Alias {
			input,
			alias,
			columns,
		} = right
		else {
			return None;
		};
		let LogicalPlan::Aggregate {
			input: aggregate_input,
			group_expr,
			aggr_expr,
		} = input.as_ref()
		else {
			return None;
		};
		let LogicalPlan::Exchange {
			input: inner,
			partition_count,
			key,
		} = aggregate_input.as_ref()
		else {
			return None;
		};
		if matches!(
			inner.as_ref(),
			LogicalPlan::Join {
				kind: JoinKind::LeftSemi,
				..
			}
		) {
			return None;
		}
		let n = group_expr.len();
		if n == 0 || on.len() != n || aggr_expr.len() != 1 || columns.len() != n + 1 {
			return None;
		}
		if key.len() != n {
			return None;
		}
		for (exchange_key, group_key) in key.iter().zip(group_expr) {
			let (Some(exchange_key), Some(group_key)) =
				(plain_column(exchange_key), plain_column(group_key))
			else {
				return None;
			};
			if exchange_key != group_key {
				return None;
			}
		}
		for (index, column) in columns.iter().enumerate() {
			let expected = if index == n {
				"value".to_string()
			} else {
				format!("key_{index}")
			};
			if column != &expected {
				return None;
			}
		}
		let mut outer_keys = Vec::with_capacity(n);
		for (index, (outer_expr, alias_expr)) in on.iter().enumerate() {
			let alias_column = plain_column(alias_expr)?;
			if alias_column.name.0 != format!("key_{index}")
				|| alias_column.qualifier.as_ref() != Some(alias)
			{
				return None;
			}
			plain_column(outer_expr)?;
			outer_keys.push(outer_expr.clone());
		}
		let reduced = self.derive_reachable_key_source(outer, &outer_keys).await?;
		let rebuilt = LogicalPlan::Alias {
			input: Box::new(LogicalPlan::Aggregate {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(LogicalPlan::Join {
						left: inner.clone(),
						right: Box::new(reduced.source),
						kind: JoinKind::LeftSemi,
						on: group_expr
							.iter()
							.cloned()
							.zip(reduced.source_keys)
							.collect(),
						on_filter: None,
						null_safe_keys: false,
						strategy_intent: JoinStrategyIntent::Auto,
					}),
					partition_count: *partition_count,
					key: key.clone(),
				}),
				group_expr: group_expr.clone(),
				aggr_expr: aggr_expr.clone(),
			}),
			alias: alias.clone(),
			columns: columns.clone(),
		};
		Some(match right_exchange {
			Some((partition_count, key)) => LogicalPlan::Exchange {
				input: Box::new(rebuilt),
				partition_count,
				key,
			},
			None => rebuilt,
		})
	}
}
