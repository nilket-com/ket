use super::*;

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) struct OuterJoinDemotionReceipt {
	pub(super) from_kind: JoinKind,
	pub(super) to_kind: JoinKind,
	pub(super) conjunct_text: String,
	pub(super) propagation_path: Vec<JoinSideStep>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(super) enum JoinSideStep {
	Left,
	Right,
	Alias(TableRef),
	Project,
}

impl JoinSideStep {
	pub(super) fn label(&self) -> String {
		match self {
			Self::Left => "left".to_string(),
			Self::Right => "right".to_string(),
			Self::Alias(alias) => format!("alias({})", relation_key_label(alias)),
			Self::Project => "project".to_string(),
		}
	}
}

#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct NullRejectedSides {
	left: bool,
	right: bool,
}

#[derive(Debug, Clone, Default, PartialEq, Eq)]
struct ConjunctColumnSides {
	left: bool,
	right: bool,
	ambiguous: bool,
}

impl ConjunctColumnSides {
	fn side(side: LogicalJoinSide) -> Self {
		match side {
			LogicalJoinSide::Left => Self {
				left: true,
				right: false,
				ambiguous: false,
			},
			LogicalJoinSide::Right => Self {
				left: false,
				right: true,
				ambiguous: false,
			},
		}
	}

	fn ambiguous() -> Self {
		Self {
			left: false,
			right: false,
			ambiguous: true,
		}
	}

	fn merge(&mut self, other: Self) {
		self.left |= other.left;
		self.right |= other.right;
		self.ambiguous |= other.ambiguous;
	}
}

#[derive(Debug, Clone)]
struct RoutedConjuncts {
	input: LogicalPlan,
	residual: Vec<Expr>,
	receipts: Vec<OuterJoinDemotionReceipt>,
}

#[derive(Debug, Clone)]
struct RoutedExpr {
	original: Expr,
	routed: Expr,
}

impl NullRejectedSides {
	fn side(side: LogicalJoinSide) -> Self {
		match side {
			LogicalJoinSide::Left => Self {
				left: true,
				right: false,
			},
			LogicalJoinSide::Right => Self {
				left: false,
				right: true,
			},
		}
	}

	fn union(self, other: Self) -> Self {
		Self {
			left: self.left || other.left,
			right: self.right || other.right,
		}
	}

	fn intersect(self, other: Self) -> Self {
		Self {
			left: self.left && other.left,
			right: self.right && other.right,
		}
	}

	fn is_empty(self) -> bool {
		!self.left && !self.right
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum LogicalJoinSide {
	Left,
	Right,
}

impl<'a> Binder<'a> {
	pub(super) fn demote_null_rejected_outer_joins<'b>(
		&'b self,
		plan: &'b LogicalPlan,
	) -> Pin<
		Box<
			dyn Future<Output = Result<(LogicalPlan, Vec<OuterJoinDemotionReceipt>), BindingError>>
				+ Send
				+ 'b,
		>,
	> {
		Box::pin(async move {
			let mut receipts = Vec::new();
			let plan = self
				.demote_null_rejected_outer_joins_inner(plan, &mut receipts)
				.await?;
			Ok((plan, receipts))
		})
	}

	fn demote_null_rejected_outer_joins_inner<'b>(
		&'b self,
		plan: &'b LogicalPlan,
		receipts: &'b mut Vec<OuterJoinDemotionReceipt>,
	) -> Pin<Box<dyn Future<Output = Result<LogicalPlan, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Filter { input, predicate } => {
					let input = self
						.demote_null_rejected_outer_joins_inner(input, receipts)
						.await?;
					let conjuncts = split_logical_and_conjuncts(predicate);
					let routed = self
						.route_filter_conjuncts(input, conjuncts, Vec::new())
						.await?;
					receipts.extend(routed.receipts);
					Ok(wrap_residual_filter(routed.input, routed.residual))
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => Ok(LogicalPlan::Alias {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					alias: alias.clone(),
					columns: columns.clone(),
				}),
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => Ok(LogicalPlan::Project {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					exprs: exprs.clone(),
					output_names: output_names.clone(),
				}),
				LogicalPlan::Distinct { input } => Ok(LogicalPlan::Distinct {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
				}),
				LogicalPlan::Union { left, right } => Ok(LogicalPlan::Union {
					left: Box::new(
						self.demote_null_rejected_outer_joins_inner(left, receipts)
							.await?,
					),
					right: Box::new(
						self.demote_null_rejected_outer_joins_inner(right, receipts)
							.await?,
					),
				}),
				LogicalPlan::SetOpAll { left, right, kind } => Ok(LogicalPlan::SetOpAll {
					left: Box::new(
						self.demote_null_rejected_outer_joins_inner(left, receipts)
							.await?,
					),
					right: Box::new(
						self.demote_null_rejected_outer_joins_inner(right, receipts)
							.await?,
					),
					kind: *kind,
				}),
				LogicalPlan::Repeat { input, count } => Ok(LogicalPlan::Repeat {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					count: count.clone(),
				}),
				LogicalPlan::Aggregate {
					input,
					group_expr,
					aggr_expr,
				} => Ok(LogicalPlan::Aggregate {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					group_expr: group_expr.clone(),
					aggr_expr: aggr_expr.clone(),
				}),
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => Ok(LogicalPlan::Exchange {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					partition_count: *partition_count,
					key: key.clone(),
				}),
				LogicalPlan::Sort {
					input,
					order_by,
					limit,
					offset,
				} => Ok(LogicalPlan::Sort {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					order_by: order_by.clone(),
					limit: *limit,
					offset: *offset,
				}),
				LogicalPlan::Window {
					input,
					partition_by,
					order_by,
					functions,
				} => Ok(LogicalPlan::Window {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					partition_by: partition_by.clone(),
					order_by: order_by.clone(),
					functions: functions.clone(),
				}),
				LogicalPlan::Take { input, limit } => Ok(LogicalPlan::Take {
					input: Box::new(
						self.demote_null_rejected_outer_joins_inner(input, receipts)
							.await?,
					),
					limit: *limit,
				}),
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					null_safe_keys,
					strategy_intent,
				} => Ok(LogicalPlan::Join {
					left: Box::new(
						self.demote_null_rejected_outer_joins_inner(left, receipts)
							.await?,
					),
					right: Box::new(
						self.demote_null_rejected_outer_joins_inner(right, receipts)
							.await?,
					),
					kind: *kind,
					on: on.clone(),
					null_safe_keys: *null_safe_keys,
					strategy_intent: *strategy_intent,
				}),
				LogicalPlan::CreateTableAsSelect {
					table,
					if_not_exists,
					or_replace,
					source,
				} => Ok(LogicalPlan::CreateTableAsSelect {
					table: table.clone(),
					if_not_exists: *if_not_exists,
					or_replace: *or_replace,
					source: Box::new(
						self.demote_null_rejected_outer_joins_inner(source, receipts)
							.await?,
					),
				}),
				LogicalPlan::InsertSelect {
					table,
					columns,
					source,
					returning,
				} => Ok(LogicalPlan::InsertSelect {
					table: table.clone(),
					columns: columns.clone(),
					source: Box::new(
						self.demote_null_rejected_outer_joins_inner(source, receipts)
							.await?,
					),
					returning: returning.clone(),
				}),
				// ADR 255: opaque-local — a recursive CTE's body is bound separately; outer-join
				// demotion does not rewrite inside it, and its self-reference is a leaf.
				LogicalPlan::RecursiveCte { .. }
				| LogicalPlan::RecursiveRef { .. }
				| LogicalPlan::Scan { .. }
				| LogicalPlan::ScanNamed { .. }
				| LogicalPlan::Explain { .. }
				| LogicalPlan::ExplainAnalyze { .. }
				| LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::DropTable { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
				| LogicalPlan::Insert { .. } => Ok(plan.clone()),
			}
		})
	}

	fn logical_scope_for_outer_demotion<'b>(
		&'b self,
		plan: &'b LogicalPlan,
	) -> Pin<Box<dyn Future<Output = Result<Option<RelationScope>, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			match plan {
				LogicalPlan::Scan { table, projection } => {
					let binding = self.catalog.resolve_table_id(*table).await?;
					let schema =
						projected_schema_for_outer_demotion(&binding.schema, projection.as_ref());
					Ok(Some(RelationScope::empty(schema)))
				}
				LogicalPlan::ScanNamed { table, projection } => {
					if is_information_schema_namespace(table) {
						return Ok(None);
					}
					let binding = self.catalog.resolve_table(table).await?;
					let schema =
						projected_schema_for_outer_demotion(&binding.schema, projection.as_ref());
					Ok(Some(RelationScope::scan(table.clone(), &schema)))
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => {
					let Some(input) = self.logical_scope_for_outer_demotion(input).await? else {
						return Ok(None);
					};
					let schema = alias_schema_for_outer_demotion(&input.schema, columns)?;
					Ok(Some(RelationScope::alias(alias.clone(), &schema)))
				}
				LogicalPlan::Filter { input, .. }
				| LogicalPlan::Distinct { input }
				| LogicalPlan::Repeat { input, .. }
				| LogicalPlan::Exchange { input, .. }
				| LogicalPlan::Sort { input, .. }
				| LogicalPlan::Window { input, .. }
				| LogicalPlan::Take { input, .. } => self.logical_scope_for_outer_demotion(input).await,
				LogicalPlan::Union { .. } | LogicalPlan::SetOpAll { .. } => Ok(None),
				LogicalPlan::Join {
					left, right, kind, ..
				} => {
					let Some(left_scope) = self.logical_scope_for_outer_demotion(left).await?
					else {
						return Ok(None);
					};
					let Some(right_scope) = self.logical_scope_for_outer_demotion(right).await?
					else {
						return Ok(None);
					};
					let schema = join_schema(*kind, &left_scope.schema, &right_scope.schema);
					Ok(Some(RelationScope::join(
						*kind,
						&left_scope,
						&right_scope,
						&schema,
					)))
				}
				// ADR 255: opaque-local — a recursive CTE's internal schema is not resolved for
				// outer-demotion column routing (sealed local node); its self-reference is a leaf.
				LogicalPlan::RecursiveCte { .. }
				| LogicalPlan::RecursiveRef { .. }
				| LogicalPlan::Project { .. }
				| LogicalPlan::Aggregate { .. }
				| LogicalPlan::Explain { .. }
				| LogicalPlan::ExplainAnalyze { .. }
				| LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::DropTable { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
				| LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. } => Ok(None),
			}
		})
	}

	fn route_filter_conjuncts<'b>(
		&'b self,
		input: LogicalPlan,
		conjuncts: Vec<Expr>,
		path: Vec<JoinSideStep>,
	) -> Pin<Box<dyn Future<Output = Result<RoutedConjuncts, BindingError>> + Send + 'b>> {
		let conjuncts = conjuncts
			.into_iter()
			.map(|expr| RoutedExpr {
				original: expr.clone(),
				routed: expr,
			})
			.collect();
		self.route_filter_routed_conjuncts(input, conjuncts, path)
	}

	fn route_filter_routed_conjuncts<'b>(
		&'b self,
		input: LogicalPlan,
		conjuncts: Vec<RoutedExpr>,
		path: Vec<JoinSideStep>,
	) -> Pin<Box<dyn Future<Output = Result<RoutedConjuncts, BindingError>> + Send + 'b>> {
		Box::pin(async move {
			if conjuncts.is_empty() {
				return Ok(RoutedConjuncts {
					input,
					residual: Vec::new(),
					receipts: Vec::new(),
				});
			}
			match input {
				LogicalPlan::Join {
					left,
					right,
					kind: JoinKind::Inner,
					on,
					null_safe_keys,
					strategy_intent,
				} => {
					let Some(left_scope) = self.logical_scope_for_outer_demotion(&left).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind: JoinKind::Inner,
								on,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let Some(right_scope) = self.logical_scope_for_outer_demotion(&right).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind: JoinKind::Inner,
								on,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let mut left_conjuncts = Vec::new();
					let mut right_conjuncts = Vec::new();
					let mut residual = Vec::new();
					for conjunct in conjuncts {
						let sides =
							conjunct_column_sides(&conjunct.routed, &left_scope, &right_scope);
						if sides.left && !sides.right && !sides.ambiguous {
							left_conjuncts.push(conjunct);
						} else if sides.right && !sides.left && !sides.ambiguous {
							right_conjuncts.push(conjunct);
						} else {
							residual.push(conjunct.original);
						}
					}
					let mut left_path = path.clone();
					left_path.push(JoinSideStep::Left);
					let left = self
						.route_filter_routed_conjuncts(*left, left_conjuncts, left_path)
						.await?;
					let mut right_path = path;
					right_path.push(JoinSideStep::Right);
					let right = self
						.route_filter_routed_conjuncts(*right, right_conjuncts, right_path)
						.await?;
					residual.extend(left.residual);
					residual.extend(right.residual);
					let mut receipts = left.receipts;
					receipts.extend(right.receipts);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Join {
							left: Box::new(left.input),
							right: Box::new(right.input),
							kind: JoinKind::Inner,
							on,
							null_safe_keys,
							strategy_intent,
						},
						residual,
						receipts,
					})
				}
				LogicalPlan::Join {
					left,
					right,
					kind,
					on,
					null_safe_keys,
					strategy_intent,
				} => {
					let Some(left_scope) = self.logical_scope_for_outer_demotion(&left).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind,
								on,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let Some(right_scope) = self.logical_scope_for_outer_demotion(&right).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind,
								on,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let mut rejected = NullRejectedSides::default();
					let mut proof_sides = Vec::with_capacity(conjuncts.len());
					let mut ownership_sides = Vec::with_capacity(conjuncts.len());
					for conjunct in &conjuncts {
						let sides =
							null_rejecting_sides(&conjunct.routed, &left_scope, &right_scope);
						rejected = rejected.union(sides);
						proof_sides.push(sides);
						ownership_sides.push(conjunct_column_sides(
							&conjunct.routed,
							&left_scope,
							&right_scope,
						));
					}
					let Some(to_kind) = demoted_join_kind(kind, rejected) else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Join {
								left,
								right,
								kind,
								on,
								null_safe_keys,
								strategy_intent,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let demotion_sides = demotion_owned_sides(kind, to_kind, rejected);
					let mut left_push = Vec::new();
					let mut right_push = Vec::new();
					let mut residual = Vec::new();
					let mut receipt_text = Vec::new();
					for ((conjunct, proof), ownership) in
						conjuncts.into_iter().zip(proof_sides).zip(ownership_sides)
					{
						if !proof.intersect(demotion_sides).is_empty() {
							receipt_text.push(format!("{:?}", conjunct.original));
						}
						if demotion_sides.left
							&& proof.left && ownership.left
							&& !ownership.right && !ownership.ambiguous
						{
							left_push.push(conjunct.routed);
						} else if demotion_sides.right
							&& proof.right && ownership.right
							&& !ownership.left && !ownership.ambiguous
						{
							right_push.push(conjunct.routed);
						} else {
							residual.push(conjunct.original);
						}
					}
					let left = push_logical_filter_conjuncts(*left, left_push);
					let right = push_logical_filter_conjuncts(*right, right_push);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Join {
							left: Box::new(left),
							right: Box::new(right),
							kind: to_kind,
							on,
							null_safe_keys,
							strategy_intent,
						},
						residual,
						receipts: vec![OuterJoinDemotionReceipt {
							from_kind: kind,
							to_kind,
							conjunct_text: receipt_text.join(" AND "),
							propagation_path: path,
						}],
					})
				}
				LogicalPlan::Alias {
					input,
					alias,
					columns,
				} => {
					let Some(input_scope) = self.logical_scope_for_outer_demotion(&input).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Alias {
								input,
								alias,
								columns,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let alias_schema =
						alias_schema_for_outer_demotion(&input_scope.schema, &columns)?;
					let alias_scope = RelationScope::alias(alias.clone(), &alias_schema);
					let mut routed_conjuncts = Vec::new();
					let mut residual = Vec::new();
					for conjunct in conjuncts {
						match remap_expr_through_alias(
							&conjunct.routed,
							&alias,
							&alias_scope,
							&input_scope,
						)? {
							Some(routed) => routed_conjuncts.push(RoutedExpr {
								original: conjunct.original,
								routed,
							}),
							None => residual.push(conjunct.original),
						}
					}
					let mut wrapper_path = path;
					wrapper_path.push(JoinSideStep::Alias(alias.clone()));
					let routed = self
						.route_filter_routed_conjuncts(*input, routed_conjuncts, wrapper_path)
						.await?;
					residual.extend(routed.residual);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Alias {
							input: Box::new(routed.input),
							alias,
							columns,
						},
						residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Project {
					input,
					exprs,
					output_names,
				} => {
					let Some(input_scope) = self.logical_scope_for_outer_demotion(&input).await?
					else {
						return Ok(RoutedConjuncts {
							input: LogicalPlan::Project {
								input,
								exprs,
								output_names,
							},
							residual: routed_originals(conjuncts),
							receipts: Vec::new(),
						});
					};
					let map = project_passthrough_map(&exprs, &output_names, &input_scope)?;
					let mut routed_conjuncts = Vec::new();
					let mut residual = Vec::new();
					for conjunct in conjuncts {
						match remap_expr_through_project(&conjunct.routed, &map, &input_scope)? {
							Some(routed) => routed_conjuncts.push(RoutedExpr {
								original: conjunct.original,
								routed,
							}),
							None => residual.push(conjunct.original),
						}
					}
					let mut wrapper_path = path;
					wrapper_path.push(JoinSideStep::Project);
					let routed = self
						.route_filter_routed_conjuncts(*input, routed_conjuncts, wrapper_path)
						.await?;
					residual.extend(routed.residual);
					Ok(RoutedConjuncts {
						input: LogicalPlan::Project {
							input: Box::new(routed.input),
							exprs,
							output_names,
						},
						residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Distinct { input } => {
					let routed = self
						.route_filter_routed_conjuncts(*input, conjuncts, path)
						.await?;
					Ok(RoutedConjuncts {
						input: LogicalPlan::Distinct {
							input: Box::new(routed.input),
						},
						residual: routed.residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Exchange {
					input,
					partition_count,
					key,
				} => {
					let routed = self
						.route_filter_routed_conjuncts(*input, conjuncts, path)
						.await?;
					Ok(RoutedConjuncts {
						input: LogicalPlan::Exchange {
							input: Box::new(routed.input),
							partition_count,
							key,
						},
						residual: routed.residual,
						receipts: routed.receipts,
					})
				}
				LogicalPlan::Sort {
					input,
					order_by,
					limit: None,
					offset: 0,
				} => {
					let routed = self
						.route_filter_routed_conjuncts(*input, conjuncts, path)
						.await?;
					Ok(RoutedConjuncts {
						input: LogicalPlan::Sort {
							input: Box::new(routed.input),
							order_by,
							limit: None,
							offset: 0,
						},
						residual: routed.residual,
						receipts: routed.receipts,
					})
				}
				// ADR 255: opaque-local — filter conjuncts are not pushed into a sealed recursive CTE
				// fixpoint, and its self-reference is a leaf; routing stops here as for Scan.
				stopped @ (LogicalPlan::RecursiveCte { .. }
				| LogicalPlan::RecursiveRef { .. }
				| LogicalPlan::Scan { .. }
				| LogicalPlan::ScanNamed { .. }
				| LogicalPlan::Filter { .. }
				| LogicalPlan::Union { .. }
				| LogicalPlan::SetOpAll { .. }
				| LogicalPlan::Repeat { .. }
				| LogicalPlan::Aggregate { .. }
				| LogicalPlan::Sort { .. }
				| LogicalPlan::Window { .. }
				| LogicalPlan::Take { .. }
				| LogicalPlan::Explain { .. }
				| LogicalPlan::ExplainAnalyze { .. }
				| LogicalPlan::CreateTable { .. }
				| LogicalPlan::CreateLogicalType { .. }
				| LogicalPlan::CreateTableAsSelect { .. }
				| LogicalPlan::DropTable { .. }
				| LogicalPlan::Delete { .. }
				| LogicalPlan::Update { .. }
				| LogicalPlan::Insert { .. }
				| LogicalPlan::InsertSelect { .. }) => Ok(RoutedConjuncts {
					input: stopped,
					residual: routed_originals(conjuncts),
					receipts: Vec::new(),
				}),
			}
		})
	}
}

fn routed_originals(conjuncts: Vec<RoutedExpr>) -> Vec<Expr> {
	conjuncts
		.into_iter()
		.map(|conjunct| conjunct.original)
		.collect()
}

fn remap_expr_through_alias(
	expr: &Expr,
	alias: &TableRef,
	alias_scope: &RelationScope,
	input_scope: &RelationScope,
) -> Result<Option<Expr>, BindingError> {
	remap_expr_columns(expr, &mut |column_ref| {
		let Some(output_ordinal) = alias_output_ordinal(column_ref, alias, alias_scope)? else {
			return Ok(None);
		};
		let input_name = input_scope.schema.field(output_ordinal).name().clone();
		let remapped = ColumnRef::unqualified(input_name);
		if input_scope
			.resolve_join_column(&remapped, "alias input")
			.ok()
			.flatten()
			.is_none()
		{
			return Ok(None);
		}
		Ok(Some(remapped))
	})
}

fn alias_output_ordinal(
	column_ref: &ColumnRef,
	alias: &TableRef,
	alias_scope: &RelationScope,
) -> Result<Option<usize>, BindingError> {
	if let Some(qualifier) = &column_ref.qualifier {
		if qualifier != alias {
			return Ok(None);
		}
		return Ok(unique_schema_ordinal(
			&alias_scope.schema,
			&column_ref.name.0,
		));
	}
	let resolved = match alias_scope.resolve_join_column(column_ref, "alias output") {
		Ok(Some(resolved)) => resolved,
		Ok(None) | Err(_) => return Ok(None),
	};
	Ok(Some(resolved.column.ordinal.0 as usize))
}

fn project_passthrough_map(
	exprs: &[Expr],
	output_names: &[Option<String>],
	input_scope: &RelationScope,
) -> Result<HashMap<String, Option<ColumnRef>>, BindingError> {
	let mut map = HashMap::new();
	for (idx, output_name) in output_names.iter().enumerate() {
		let Some(output_name) = output_name else {
			continue;
		};
		let candidate = match exprs.get(idx) {
			Some(Expr::Column(column_ref))
				if input_scope
					.resolve_join_column(column_ref, "project input")
					.ok()
					.flatten()
					.is_some() =>
			{
				Some(column_ref.clone())
			}
			_ => None,
		};
		if map.insert(output_name.clone(), candidate).is_some() {
			map.insert(output_name.clone(), None);
		}
	}
	Ok(map)
}

fn remap_expr_through_project(
	expr: &Expr,
	map: &HashMap<String, Option<ColumnRef>>,
	input_scope: &RelationScope,
) -> Result<Option<Expr>, BindingError> {
	remap_expr_columns(expr, &mut |column_ref| {
		if column_ref.qualifier.is_some() {
			return Ok(None);
		}
		let Some(Some(remapped)) = map.get(&column_ref.name.0) else {
			return Ok(None);
		};
		if input_scope
			.resolve_join_column(remapped, "project input")
			.ok()
			.flatten()
			.is_none()
		{
			return Ok(None);
		}
		Ok(Some(remapped.clone()))
	})
}

fn unique_schema_ordinal(schema: &SchemaRef, name: &str) -> Option<usize> {
	let mut matches = schema
		.fields()
		.iter()
		.enumerate()
		.filter(|(_, field)| field.name() == name)
		.map(|(idx, _)| idx);
	let first = matches.next()?;
	if matches.next().is_some() {
		return None;
	}
	Some(first)
}

fn remap_expr_columns<F>(expr: &Expr, remap: &mut F) -> Result<Option<Expr>, BindingError>
where
	F: FnMut(&ColumnRef) -> Result<Option<ColumnRef>, BindingError>,
{
	macro_rules! child {
		($expr:expr) => {
			match remap_expr_columns($expr, remap)? {
				Some(expr) => expr,
				None => return Ok(None),
			}
		};
	}
	macro_rules! binary {
		($ctor:expr, $left:expr, $right:expr) => {
			$ctor(Box::new(child!($left)), Box::new(child!($right)))
		};
	}

	Ok(Some(match expr {
		Expr::Column(column_ref) => {
			let Some(column_ref) = remap(column_ref)? else {
				return Ok(None);
			};
			Expr::Column(column_ref)
		}
		Expr::Literal(value) => Expr::Literal(value.clone()),
		Expr::UntypedNull => Expr::UntypedNull,
		Expr::LogicalAscription { expr, logical_type } => Expr::LogicalAscription {
			expr: Box::new(child!(expr)),
			logical_type: *logical_type,
		},
		Expr::SqlLogicalCast {
			expr,
			logical_name,
			try_cast,
		} => Expr::SqlLogicalCast {
			expr: Box::new(child!(expr)),
			logical_name: logical_name.clone(),
			try_cast: *try_cast,
		},
		Expr::SqlConvert {
			expr,
			target_logical_name,
		} => Expr::SqlConvert {
			expr: Box::new(child!(expr)),
			target_logical_name: target_logical_name.clone(),
		},
		Expr::Cast(inner, data_type) => Expr::Cast(Box::new(child!(inner)), data_type.clone()),
		Expr::TryCast(inner, data_type) => {
			Expr::TryCast(Box::new(child!(inner)), data_type.clone())
		}
		Expr::Add(left, right) => binary!(Expr::Add, left, right),
		Expr::Sub(left, right) => binary!(Expr::Sub, left, right),
		Expr::Mul(left, right) => binary!(Expr::Mul, left, right),
		Expr::Div(left, right) => binary!(Expr::Div, left, right),
		Expr::Eq(left, right) => binary!(Expr::Eq, left, right),
		Expr::Neq(left, right) => binary!(Expr::Neq, left, right),
		Expr::Lt(left, right) => binary!(Expr::Lt, left, right),
		Expr::Lte(left, right) => binary!(Expr::Lte, left, right),
		Expr::Gt(left, right) => binary!(Expr::Gt, left, right),
		Expr::Gte(left, right) => binary!(Expr::Gte, left, right),
		Expr::And(left, right) => binary!(Expr::And, left, right),
		Expr::Or(left, right) => binary!(Expr::Or, left, right),
		Expr::Not(inner) => Expr::Not(Box::new(child!(inner))),
		Expr::InList {
			expr,
			list,
			negated,
		} => {
			let expr = Box::new(child!(expr));
			let mut remapped_list = Vec::with_capacity(list.len());
			for item in list {
				remapped_list.push(child!(item));
			}
			Expr::InList {
				expr,
				list: remapped_list,
				negated: *negated,
			}
		}
		Expr::Like {
			expr,
			pattern,
			negated,
			case_insensitive,
		} => Expr::Like {
			expr: Box::new(child!(expr)),
			pattern: pattern.clone(),
			negated: *negated,
			case_insensitive: *case_insensitive,
		},
		Expr::Case {
			when_then,
			else_expr,
		} => {
			let mut remapped = Vec::with_capacity(when_then.len());
			for (when_expr, then_expr) in when_then {
				remapped.push((Box::new(child!(when_expr)), Box::new(child!(then_expr))));
			}
			let else_expr = match else_expr {
				Some(expr) => Some(Box::new(child!(expr))),
				None => None,
			};
			Expr::Case {
				when_then: remapped,
				else_expr,
			}
		}
		Expr::IsNull(inner) => Expr::IsNull(Box::new(child!(inner))),
		Expr::IsNotNull(inner) => Expr::IsNotNull(Box::new(child!(inner))),
		Expr::TimeBucket(duration, inner) => Expr::TimeBucket(*duration, Box::new(child!(inner))),
		Expr::AtTimeZone(inner, timezone) => {
			Expr::AtTimeZone(Box::new(child!(inner)), timezone.clone())
		}
		Expr::Extract(field, inner) => Expr::Extract(*field, Box::new(child!(inner))),
		Expr::DateTrunc(field, inner) => Expr::DateTrunc(*field, Box::new(child!(inner))),
	}))
}

fn split_logical_and_conjuncts(expr: &Expr) -> Vec<Expr> {
	match expr {
		Expr::And(left, right) => {
			let mut conjuncts = split_logical_and_conjuncts(left);
			conjuncts.extend(split_logical_and_conjuncts(right));
			conjuncts
		}
		_ => vec![expr.clone()],
	}
}

fn combine_logical_and_conjuncts(conjuncts: Vec<Expr>) -> Option<Expr> {
	conjuncts
		.into_iter()
		.reduce(|left, right| Expr::And(Box::new(left), Box::new(right)))
}

fn push_logical_filter_conjuncts(input: LogicalPlan, conjuncts: Vec<Expr>) -> LogicalPlan {
	match combine_logical_and_conjuncts(conjuncts) {
		Some(predicate) => LogicalPlan::Filter {
			input: Box::new(input),
			predicate,
		},
		None => input,
	}
}

fn wrap_residual_filter(input: LogicalPlan, residual: Vec<Expr>) -> LogicalPlan {
	push_logical_filter_conjuncts(input, residual)
}

fn demoted_join_kind(kind: JoinKind, rejected: NullRejectedSides) -> Option<JoinKind> {
	match kind {
		JoinKind::LeftOuter if rejected.right => Some(JoinKind::Inner),
		JoinKind::RightOuter if rejected.left => Some(JoinKind::Inner),
		JoinKind::FullOuter if rejected.left && rejected.right => Some(JoinKind::Inner),
		JoinKind::FullOuter if rejected.left => Some(JoinKind::LeftOuter),
		JoinKind::FullOuter if rejected.right => Some(JoinKind::RightOuter),
		JoinKind::Inner
		| JoinKind::Cross
		| JoinKind::LeftOuter
		| JoinKind::RightOuter
		| JoinKind::FullOuter
		| JoinKind::LeftSemi
		| JoinKind::RightSemi
		| JoinKind::LeftAnti
		| JoinKind::RightAnti => None,
	}
}

fn demotion_owned_sides(
	from_kind: JoinKind,
	to_kind: JoinKind,
	rejected: NullRejectedSides,
) -> NullRejectedSides {
	match (from_kind, to_kind) {
		(JoinKind::LeftOuter, JoinKind::Inner) => NullRejectedSides {
			left: false,
			right: true,
		},
		(JoinKind::RightOuter, JoinKind::Inner) => NullRejectedSides {
			left: true,
			right: false,
		},
		(JoinKind::FullOuter, _) => rejected,
		_ => NullRejectedSides::default(),
	}
}

fn null_rejecting_sides(
	expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> NullRejectedSides {
	match expr {
		Expr::IsNotNull(inner) => null_rejecting_column_side(inner, left_scope, right_scope),
		Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right) => comparison_null_rejecting_side(left, right, left_scope, right_scope),
		Expr::Like {
			expr,
			negated: false,
			..
		} => null_rejecting_column_side(expr, left_scope, right_scope),
		Expr::InList {
			expr,
			list,
			negated: false,
		} if !list.is_empty() && list.iter().all(non_null_literal) => {
			null_rejecting_column_side(expr, left_scope, right_scope)
		}
		Expr::And(left, right) => null_rejecting_sides(left, left_scope, right_scope)
			.union(null_rejecting_sides(right, left_scope, right_scope)),
		Expr::Or(left, right) => null_rejecting_sides(left, left_scope, right_scope)
			.intersect(null_rejecting_sides(right, left_scope, right_scope)),
		Expr::Column(_)
		| Expr::Literal(_)
		| Expr::UntypedNull
		| Expr::LogicalAscription { .. }
		| Expr::SqlLogicalCast { .. }
		| Expr::SqlConvert { .. }
		| Expr::Cast(_, _)
		| Expr::TryCast(_, _)
		| Expr::Add(_, _)
		| Expr::Sub(_, _)
		| Expr::Mul(_, _)
		| Expr::Div(_, _)
		| Expr::Not(_)
		| Expr::InList { .. }
		| Expr::Like { .. }
		| Expr::Case { .. }
		| Expr::IsNull(_)
		| Expr::TimeBucket(_, _)
		| Expr::AtTimeZone(_, _)
		| Expr::Extract(_, _)
		| Expr::DateTrunc(_, _) => NullRejectedSides::default(),
	}
}

fn comparison_null_rejecting_side(
	left: &Expr,
	right: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> NullRejectedSides {
	if non_null_literal(right) {
		let sides = null_rejecting_column_side(left, left_scope, right_scope);
		if !sides.is_empty() {
			return sides;
		}
	}
	if non_null_literal(left) {
		return null_rejecting_column_side(right, left_scope, right_scope);
	}
	NullRejectedSides::default()
}

fn null_rejecting_column_side(
	expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> NullRejectedSides {
	let Expr::Column(column_ref) = expr else {
		return NullRejectedSides::default();
	};
	match column_side(column_ref, left_scope, right_scope) {
		Some(side) => NullRejectedSides::side(side),
		None => NullRejectedSides::default(),
	}
}

fn column_side(
	column_ref: &ColumnRef,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> Option<LogicalJoinSide> {
	let left = left_scope
		.resolve_join_column(column_ref, "left")
		.ok()
		.flatten();
	let right = right_scope
		.resolve_join_column(column_ref, "right")
		.ok()
		.flatten();
	match (left, right) {
		(Some(_), None) => Some(LogicalJoinSide::Left),
		(None, Some(_)) => Some(LogicalJoinSide::Right),
		(Some(_), Some(_)) | (None, None) => None,
	}
}

fn conjunct_column_sides(
	expr: &Expr,
	left_scope: &RelationScope,
	right_scope: &RelationScope,
) -> ConjunctColumnSides {
	match expr {
		Expr::Column(column_ref) => match column_side(column_ref, left_scope, right_scope) {
			Some(side) => ConjunctColumnSides::side(side),
			None => ConjunctColumnSides::ambiguous(),
		},
		Expr::Literal(_) | Expr::UntypedNull => ConjunctColumnSides::default(),
		Expr::LogicalAscription { expr, .. }
		| Expr::SqlLogicalCast { expr, .. }
		| Expr::SqlConvert { expr, .. } => conjunct_column_sides(expr, left_scope, right_scope),
		Expr::Cast(inner, _)
		| Expr::TryCast(inner, _)
		| Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::TimeBucket(_, inner)
		| Expr::AtTimeZone(inner, _)
		| Expr::Extract(_, inner)
		| Expr::DateTrunc(_, inner) => conjunct_column_sides(inner, left_scope, right_scope),
		Expr::Add(left, right)
		| Expr::Sub(left, right)
		| Expr::Mul(left, right)
		| Expr::Div(left, right)
		| Expr::Eq(left, right)
		| Expr::Neq(left, right)
		| Expr::Lt(left, right)
		| Expr::Lte(left, right)
		| Expr::Gt(left, right)
		| Expr::Gte(left, right)
		| Expr::And(left, right)
		| Expr::Or(left, right) => {
			let mut sides = conjunct_column_sides(left, left_scope, right_scope);
			sides.merge(conjunct_column_sides(right, left_scope, right_scope));
			sides
		}
		Expr::InList { expr, list, .. } => {
			let mut sides = conjunct_column_sides(expr, left_scope, right_scope);
			for item in list {
				sides.merge(conjunct_column_sides(item, left_scope, right_scope));
			}
			sides
		}
		Expr::Like { expr, .. } => conjunct_column_sides(expr, left_scope, right_scope),
		Expr::Case {
			when_then,
			else_expr,
		} => {
			let mut sides = ConjunctColumnSides::default();
			for (when_expr, then_expr) in when_then {
				sides.merge(conjunct_column_sides(when_expr, left_scope, right_scope));
				sides.merge(conjunct_column_sides(then_expr, left_scope, right_scope));
			}
			if let Some(else_expr) = else_expr {
				sides.merge(conjunct_column_sides(else_expr, left_scope, right_scope));
			}
			sides
		}
	}
}

fn non_null_literal(expr: &Expr) -> bool {
	matches!(expr, Expr::Literal(value) if !scalar_value_is_null(value))
}

fn scalar_value_is_null(value: &ScalarValue) -> bool {
	match value {
		ScalarValue::Boolean(value) => value.is_none(),
		ScalarValue::Int32(value) => value.is_none(),
		ScalarValue::Int64(value) => value.is_none(),
		ScalarValue::Date32(value) => value.is_none(),
		ScalarValue::Timestamp { value, .. } => value.is_none(),
		ScalarValue::Interval(value) => value.is_none(),
		ScalarValue::CalendarMonthInterval(value) => value.is_none(),
		ScalarValue::Float32(value) => value.is_none(),
		ScalarValue::Float64(value) => value.is_none(),
		ScalarValue::Decimal128 { value, .. } => value.is_none(),
		ScalarValue::Utf8(value) => value.is_none(),
	}
}

fn projected_schema_for_outer_demotion(
	schema: &SchemaRef,
	projection: Option<&Vec<String>>,
) -> SchemaRef {
	let Some(projection) = projection else {
		return schema.clone();
	};
	let fields = projection
		.iter()
		.filter_map(|name| schema.fields().iter().find(|field| field.name() == name))
		.map(|field| field.as_ref().clone())
		.collect::<Vec<_>>();
	Arc::new(Schema::new(fields))
}

fn alias_schema_for_outer_demotion(
	schema: &SchemaRef,
	columns: &[String],
) -> Result<SchemaRef, BindingError> {
	if columns.is_empty() {
		return Ok(schema.clone());
	}
	if columns.len() != schema.fields().len() {
		return Err(BindingError::UnsupportedExpression {
			message: format!(
				"alias column list arity mismatch: expected {}, got {}",
				schema.fields().len(),
				columns.len()
			),
		});
	}
	let fields = schema
		.fields()
		.iter()
		.zip(columns)
		.map(|(field, name)| {
			Field::new(name.clone(), field.data_type().clone(), field.is_nullable())
				.with_metadata(field.metadata().clone())
		})
		.collect::<Vec<_>>();
	Ok(Arc::new(Schema::new(fields)))
}
