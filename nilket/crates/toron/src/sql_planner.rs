use std::collections::{HashMap, HashSet};
use std::hash::{Hash, Hasher};
use std::sync::Arc;

use crate::aggregate_kernel;
use crate::catalog::TableRef;
use crate::error::FrontendError;
use crate::ir::{
	AggregateFunction, Expr, InsertLiteral, JoinKind, JoinStrategy, LogicalPlan, ScalarValue,
	SortExpr,
};
use arrow::datatypes::{DataType, Field, Schema};
use sqlparser::ast::{
	BinaryOperator, CastKind, ColumnDef, ColumnOption, ColumnOptionDef, CreateTable,
	DataType as SqlDataType, ExactNumberInfo, Expr as SqlExpr, Function, FunctionArg,
	FunctionArgExpr, FunctionArgumentList, FunctionArguments, GroupByExpr, HiveDistributionStyle,
	Ident, Insert, JoinConstraint, JoinOperator, ObjectName, OrderBy, Query, Select, SelectItem,
	SetExpr, Statement, TableAlias, TableFactor, TableObject, UnaryOperator, Value,
};
use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use wendelon::schema::{
	STORAGE_ROW_HLC_COLUMN, STORAGE_ROW_KEY_ID_COLUMN, STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN, STORAGE_ROW_TOMBSTONE_COLUMN,
};

fn unsupported(message: impl Into<String>) -> FrontendError {
	FrontendError::Unsupported {
		message: message.into(),
	}
}

const DEFAULT_JOIN_PARTITION_COUNT: u32 = 4;
// P0.3: grouped SQL reuses the join shuffle fanout until aggregate fanout has separate evidence.
const DEFAULT_AGGREGATE_PARTITION_COUNT: u32 = DEFAULT_JOIN_PARTITION_COUNT;
const AGGREGATE_ARGUMENT_SHAPE_MESSAGE: &str = "unsupported aggregate argument";
const NESTED_AGGREGATE_ARGUMENT_MESSAGE: &str =
	"aggregate arguments must not contain aggregate calls";
const STORAGE_SYNTHETIC_COLUMN_NAMES: &[&str] = &[
	STORAGE_ROW_HLC_COLUMN,
	STORAGE_ROW_KEY_ID_COLUMN,
	STORAGE_ROW_ROUTING_LOCALITY_COLUMN,
	STORAGE_ROW_TABLE_OBJECT_ID_COLUMN,
	STORAGE_ROW_TOMBSTONE_COLUMN,
];

fn validate_simple_aggregate_arg(arg: &FunctionArg) -> Result<(), FrontendError> {
	match arg {
		FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => validate_aggregate_argument(expr),
		_ => Err(unsupported(AGGREGATE_ARGUMENT_SHAPE_MESSAGE)),
	}
}

fn validate_aggregate_argument(expr: &SqlExpr) -> Result<(), FrontendError> {
	reject_nested_aggregate(expr)
}

fn reject_nested_aggregate(expr: &SqlExpr) -> Result<(), FrontendError> {
	match expr {
		SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
			Err(unsupported(NESTED_AGGREGATE_ARGUMENT_MESSAGE))
		}
		SqlExpr::Function(function) => {
			if let FunctionArguments::List(list) = &function.args {
				for arg in &list.args {
					match arg {
						FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
						| FunctionArg::Named {
							arg: FunctionArgExpr::Expr(expr),
							..
						}
						| FunctionArg::ExprNamed {
							arg: FunctionArgExpr::Expr(expr),
							..
						} => reject_nested_aggregate(expr)?,
						FunctionArg::Unnamed(FunctionArgExpr::Wildcard)
						| FunctionArg::Unnamed(FunctionArgExpr::QualifiedWildcard(_)) => {}
						_ => {}
					}
				}
			}
			Ok(())
		}
		SqlExpr::BinaryOp { left, right, .. } => {
			reject_nested_aggregate(left)?;
			reject_nested_aggregate(right)
		}
		SqlExpr::Cast { expr, .. } | SqlExpr::UnaryOp { expr, .. } => reject_nested_aggregate(expr),
		SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) | SqlExpr::Nested(inner) => {
			reject_nested_aggregate(inner)
		}
		_ => Ok(()),
	}
}

fn lower_cast_target_type(data_type: &SqlDataType) -> Result<DataType, FrontendError> {
	match data_type {
		SqlDataType::Int(_)
		| SqlDataType::Integer(_)
		| SqlDataType::Int4(_)
		| SqlDataType::Int32 => Ok(DataType::Int32),
		SqlDataType::BigInt(_) | SqlDataType::Int64 => Ok(DataType::Int64),
		SqlDataType::Real | SqlDataType::Float4 | SqlDataType::Float32 => Ok(DataType::Float32),
		SqlDataType::Double(ExactNumberInfo::None)
		| SqlDataType::DoublePrecision
		| SqlDataType::Float8
		| SqlDataType::Float64 => Ok(DataType::Float64),
		SqlDataType::Float(_) => Err(unsupported(
			"unsupported CAST target type: FLOAT (precision-parameter ambiguous; use REAL, DOUBLE PRECISION, FLOAT32, or FLOAT64)",
		)),
		other => Err(unsupported(format!(
			"unsupported CAST target type: {other}"
		))),
	}
}

fn lower_create_table_name(name: &ObjectName) -> Result<TableRef, FrontendError> {
	match name.0.as_slice() {
		[table] => Ok(TableRef::new(table.value.clone())),
		[schema, table] => Ok(TableRef::with_schema(
			schema.value.clone(),
			table.value.clone(),
		)),
		_ => Err(unsupported(
			"CREATE TABLE supports only one- or two-part table names",
		)),
	}
}

fn lower_insert_target_name(target: &TableObject) -> Result<TableRef, FrontendError> {
	match target {
		TableObject::TableName(name) => match name.0.as_slice() {
			[table] => Ok(TableRef::new(table.value.clone())),
			[schema, table] => Ok(TableRef::with_schema(
				schema.value.clone(),
				table.value.clone(),
			)),
			_ => Err(unsupported(
				"INSERT supports only one- or two-part table names",
			)),
		},
		TableObject::TableFunction(_) => Err(unsupported(
			"INSERT target must be a table name; table functions are not supported",
		)),
	}
}

fn lower_insert_column_list(columns: &[Ident]) -> Option<Vec<String>> {
	if columns.is_empty() {
		None
	} else {
		Some(columns.iter().map(|ident| ident.value.clone()).collect())
	}
}

enum LoweredInsertSource {
	Values(Vec<Vec<InsertLiteral>>),
	Select(LogicalPlan),
}

fn lower_insert_source(
	planner: &SqlPlanner,
	source: Option<&Query>,
) -> Result<LoweredInsertSource, FrontendError> {
	let query = source.ok_or_else(|| unsupported("INSERT requires a VALUES clause"))?;

	let values = match query.body.as_ref() {
		SetExpr::Values(values) => {
			if query.with.is_some()
				|| query.order_by.is_some()
				|| query.limit.is_some()
				|| !query.limit_by.is_empty()
				|| query.offset.is_some()
				|| query.fetch.is_some()
				|| !query.locks.is_empty()
				|| query.for_clause.is_some()
				|| query.settings.is_some()
				|| query.format_clause.is_some()
			{
				return Err(unsupported(
					"unsupported INSERT source: must be VALUES only",
				));
			}
			values
		}
		_ => return planner.lower_query(query).map(LoweredInsertSource::Select),
	};
	if values.explicit_row {
		return Err(unsupported(
			"unsupported INSERT clause: explicit ROW keyword",
		));
	}
	if values.rows.is_empty() {
		return Err(unsupported("INSERT VALUES must contain at least one row"));
	}
	if values.rows.iter().any(|row| row.is_empty()) {
		return Err(unsupported(
			"INSERT VALUES row must contain at least one literal",
		));
	}

	values
		.rows
		.iter()
		.map(|row| {
			row.iter()
				.map(lower_insert_literal)
				.collect::<Result<Vec<_>, _>>()
		})
		.collect::<Result<Vec<_>, _>>()
		.map(LoweredInsertSource::Values)
}

fn lower_insert_literal(expr: &SqlExpr) -> Result<InsertLiteral, FrontendError> {
	match expr {
		SqlExpr::Value(Value::Null) => Ok(InsertLiteral::Null),
		SqlExpr::Value(Value::Boolean(value)) => Ok(InsertLiteral::Boolean(*value)),
		SqlExpr::Value(Value::Number(text, _)) => parse_insert_number(text, false),
		SqlExpr::Value(Value::SingleQuotedString(value)) => Ok(InsertLiteral::Utf8(value.clone())),
		SqlExpr::UnaryOp {
			op: UnaryOperator::Minus,
			expr,
		} => match expr.as_ref() {
			SqlExpr::Value(Value::Number(text, _)) => parse_insert_number(text, true),
			_ => Err(unsupported(
				"INSERT VALUES must be literals; complex expressions not supported",
			)),
		},
		_ => Err(unsupported(
			"INSERT VALUES must be literals; complex expressions not supported",
		)),
	}
}

fn parse_insert_number(text: &str, negate: bool) -> Result<InsertLiteral, FrontendError> {
	if negate {
		if let Ok(value) = text.parse::<i64>() {
			return value
				.checked_neg()
				.map(InsertLiteral::Int64)
				.ok_or_else(|| {
					unsupported(format!("INSERT VALUES literal not representable: -{text}"))
				});
		}
		if let Ok(value) = text.parse::<u64>() {
			if value == (i64::MAX as u64) + 1 {
				return Ok(InsertLiteral::Int64(i64::MIN));
			}
			if value <= i64::MAX as u64 {
				return Ok(InsertLiteral::Int64(-(value as i64)));
			}
		}
	} else {
		if let Ok(value) = text.parse::<i64>() {
			return Ok(InsertLiteral::Int64(value));
		}
		if let Ok(value) = text.parse::<u64>() {
			return Ok(InsertLiteral::UInt64(value));
		}
	}

	text.parse::<f64>()
		.map(|value| InsertLiteral::Float64(if negate { -value } else { value }))
		.map_err(|_| unsupported(format!("INSERT VALUES literal not representable: {text}")))
}

fn lower_create_table_column(column: &ColumnDef) -> Result<Field, FrontendError> {
	let column_name = &column.name.value;

	if matches!(&column.data_type, SqlDataType::Unspecified) {
		return Err(unsupported("CREATE TABLE column type is required"));
	}
	if STORAGE_SYNTHETIC_COLUMN_NAMES.contains(&column_name.as_str()) {
		return Err(unsupported(format!(
			"CREATE TABLE column name is reserved for storage metadata: {column_name}"
		)));
	}
	if column.collation.is_some() {
		return Err(unsupported(
			"unsupported CREATE TABLE column option: COLLATE",
		));
	}

	let data_type = lower_create_table_data_type(&column.data_type)?;
	let nullable = lower_create_table_column_nullability(&column.options)?;

	Ok(Field::new(column_name.clone(), data_type, nullable))
}

fn lower_create_table_column_nullability(
	options: &[ColumnOptionDef],
) -> Result<bool, FrontendError> {
	let mut nullable = None;
	for option_def in options {
		if option_def.name.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE column option: named constraint",
			));
		}
		let new_nullable = match &option_def.option {
			ColumnOption::Null => true,
			ColumnOption::NotNull => false,
			ColumnOption::Default(_) => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: DEFAULT",
				));
			}
			ColumnOption::Unique { .. } => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: UNIQUE / PRIMARY KEY",
				));
			}
			ColumnOption::ForeignKey { .. } => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: REFERENCES",
				));
			}
			ColumnOption::Check(_) => {
				return Err(unsupported("unsupported CREATE TABLE column option: CHECK"));
			}
			ColumnOption::Generated { .. } => {
				return Err(unsupported(
					"unsupported CREATE TABLE column option: GENERATED",
				));
			}
			ColumnOption::Materialized(_)
			| ColumnOption::Ephemeral(_)
			| ColumnOption::Alias(_)
			| ColumnOption::DialectSpecific(_)
			| ColumnOption::CharacterSet(_)
			| ColumnOption::Comment(_)
			| ColumnOption::OnUpdate(_)
			| ColumnOption::Options(_)
			| ColumnOption::Identity(_)
			| ColumnOption::OnConflict(_)
			| ColumnOption::Policy(_)
			| ColumnOption::Tags(_) => {
				return Err(unsupported("unsupported CREATE TABLE column option"));
			}
		};
		if nullable.is_some() {
			return Err(unsupported(
				"CREATE TABLE column nullability is specified more than once",
			));
		}
		nullable = Some(new_nullable);
	}
	Ok(nullable.unwrap_or(true))
}

fn lower_create_table_data_type(data_type: &SqlDataType) -> Result<DataType, FrontendError> {
	match data_type {
		SqlDataType::Int(_)
		| SqlDataType::Integer(_)
		| SqlDataType::Int4(_)
		| SqlDataType::Int32 => Ok(DataType::Int32),
		SqlDataType::BigInt(_) | SqlDataType::Int8(_) | SqlDataType::Int64 => Ok(DataType::Int64),
		SqlDataType::Real | SqlDataType::Float4 | SqlDataType::Float32 => Ok(DataType::Float32),
		SqlDataType::Double(_)
		| SqlDataType::DoublePrecision
		| SqlDataType::Float8
		| SqlDataType::Float64 => Ok(DataType::Float64),
		SqlDataType::Text
		| SqlDataType::String(_)
		| SqlDataType::Varchar(_)
		| SqlDataType::Char(_)
		| SqlDataType::Character(_)
		| SqlDataType::CharVarying(_)
		| SqlDataType::CharacterVarying(_) => Ok(DataType::Utf8),
		SqlDataType::Boolean | SqlDataType::Bool => Ok(DataType::Boolean),
		SqlDataType::Decimal(_) | SqlDataType::Numeric(_) => {
			Err(unsupported("unsupported CREATE TABLE column type: DECIMAL"))
		}
		SqlDataType::Float(_) => Err(unsupported(
			"unsupported CREATE TABLE column type: FLOAT (precision ambiguous; use REAL or DOUBLE)",
		)),
		other => Err(unsupported(format!(
			"unsupported CREATE TABLE column type: {other}"
		))),
	}
}

fn check_unique_column_names(fields: &[Field]) -> Result<(), FrontendError> {
	let mut seen = HashSet::new();
	for field in fields {
		if !seen.insert(field.name().as_str()) {
			return Err(unsupported("CREATE TABLE column names must be unique"));
		}
	}
	Ok(())
}

pub struct SqlPlanner;

impl SqlPlanner {
	pub fn new() -> Self {
		Self
	}

	pub fn plan(&self, sql: &str) -> Result<LogicalPlan, FrontendError> {
		let dialect = GenericDialect {};
		let ast = Parser::parse_sql(&dialect, sql).map_err(|e| FrontendError::SqlParse {
			message: e.to_string(),
		})?;

		if ast.is_empty() {
			return Err(unsupported("empty SQL statement"));
		}

		match &ast[0] {
			Statement::Query(query) => self.lower_query(query),
			Statement::CreateTable(create_table) => self.lower_create_table(create_table),
			Statement::Insert(insert) => self.lower_insert(insert),
			_ => Err(unsupported(
				"unsupported SQL statement; only SELECT, CREATE TABLE, and INSERT are supported",
			)),
		}
	}

	fn lower_insert(&self, insert: &Insert) -> Result<LogicalPlan, FrontendError> {
		let Insert {
			or,
			ignore,
			into,
			table,
			table_alias,
			columns,
			overwrite,
			source,
			assignments,
			partitioned,
			after_columns,
			has_table_keyword,
			on,
			returning,
			replace_into,
			priority,
			insert_alias,
			settings,
			format_clause,
		} = insert;

		if or.is_some() {
			return Err(unsupported(
				"unsupported INSERT clause: OR (REPLACE/IGNORE/...)",
			));
		}
		if *ignore {
			return Err(unsupported("unsupported INSERT clause: IGNORE"));
		}
		if *overwrite {
			return Err(unsupported("unsupported INSERT clause: OVERWRITE"));
		}
		if !*into {
			return Err(unsupported(
				"unsupported INSERT syntax: missing INTO keyword",
			));
		}
		if *has_table_keyword {
			return Err(unsupported("unsupported INSERT clause: TABLE keyword"));
		}
		if table_alias.is_some() {
			return Err(unsupported("unsupported INSERT clause: target alias"));
		}
		if !assignments.is_empty() {
			return Err(unsupported("unsupported INSERT clause: SET assignments"));
		}
		if partitioned.is_some() {
			return Err(unsupported("unsupported INSERT clause: PARTITION"));
		}
		if !after_columns.is_empty() {
			return Err(unsupported("unsupported INSERT clause: AFTER columns"));
		}
		if on.is_some() {
			return Err(unsupported(
				"unsupported INSERT clause: ON CONFLICT / ON DUPLICATE KEY",
			));
		}
		if returning.is_some() {
			return Err(unsupported("unsupported INSERT clause: RETURNING"));
		}
		if *replace_into {
			return Err(unsupported("unsupported INSERT clause: REPLACE INTO"));
		}
		if priority.is_some() {
			return Err(unsupported("unsupported INSERT clause: priority"));
		}
		if insert_alias.is_some() {
			return Err(unsupported("unsupported INSERT clause: insert alias"));
		}
		if settings.is_some() {
			return Err(unsupported("unsupported INSERT clause: SETTINGS"));
		}
		if format_clause.is_some() {
			return Err(unsupported("unsupported INSERT clause: FORMAT"));
		}

		let table = lower_insert_target_name(table)?;
		let columns = lower_insert_column_list(columns);
		match lower_insert_source(self, source.as_deref())? {
			LoweredInsertSource::Values(rows) => Ok(LogicalPlan::Insert {
				table,
				columns,
				rows,
			}),
			LoweredInsertSource::Select(source) => Ok(LogicalPlan::InsertSelect {
				table,
				columns,
				source: Box::new(source),
			}),
		}
	}

	fn lower_create_table(&self, create_table: &CreateTable) -> Result<LogicalPlan, FrontendError> {
		let CreateTable {
			or_replace,
			temporary,
			external,
			global,
			if_not_exists,
			transient,
			volatile,
			name,
			columns,
			constraints,
			hive_distribution,
			hive_formats,
			table_properties,
			with_options,
			file_format,
			location,
			query,
			without_rowid,
			like,
			clone,
			engine,
			comment,
			auto_increment_offset,
			default_charset,
			collation,
			on_commit,
			on_cluster,
			primary_key,
			order_by,
			partition_by,
			cluster_by,
			clustered_by,
			options,
			strict,
			copy_grants,
			enable_schema_evolution,
			change_tracking,
			data_retention_time_in_days,
			max_data_extension_time_in_days,
			default_ddl_collation,
			with_aggregation_policy,
			with_row_access_policy,
			with_tags,
		} = create_table;

		if *or_replace {
			return Err(unsupported("unsupported CREATE TABLE clause: OR REPLACE"));
		}
		if *temporary {
			return Err(unsupported("unsupported CREATE TABLE clause: TEMPORARY"));
		}
		if *external {
			return Err(unsupported("unsupported CREATE TABLE clause: EXTERNAL"));
		}
		if global.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: GLOBAL/LOCAL"));
		}
		if *transient {
			return Err(unsupported("unsupported CREATE TABLE clause: TRANSIENT"));
		}
		if *volatile {
			return Err(unsupported("unsupported CREATE TABLE clause: VOLATILE"));
		}
		if !constraints.is_empty() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: table-level constraints",
			));
		}
		if hive_formats.as_ref().is_some_and(|formats| {
			formats.row_format.is_some()
				|| formats
					.serde_properties
					.as_ref()
					.is_some_and(|properties| !properties.is_empty())
				|| formats.storage.is_some()
				|| formats.location.is_some()
		}) {
			return Err(unsupported("unsupported CREATE TABLE clause: HIVE FORMATS"));
		}
		if !table_properties.is_empty() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: TBLPROPERTIES",
			));
		}
		if !with_options.is_empty() {
			return Err(unsupported("unsupported CREATE TABLE clause: WITH options"));
		}
		if file_format.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: STORED AS"));
		}
		if location.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LOCATION"));
		}
		if query.is_some() {
			return self.lower_create_table_as_select(create_table);
		}
		if *without_rowid {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: WITHOUT ROWID",
			));
		}
		if like.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LIKE"));
		}
		if clone.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLONE"));
		}
		if engine.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ENGINE"));
		}
		if comment.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: COMMENT"));
		}
		if auto_increment_offset.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: AUTO_INCREMENT",
			));
		}
		if default_charset.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DEFAULT CHARSET",
			));
		}
		if collation.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: COLLATE"));
		}
		if on_commit.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON COMMIT"));
		}
		if on_cluster.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON CLUSTER"));
		}
		if primary_key.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: table-level PRIMARY KEY",
			));
		}
		if order_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ORDER BY"));
		}
		if partition_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: PARTITION BY"));
		}
		if cluster_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTER BY"));
		}
		if clustered_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTERED BY"));
		}
		if options.as_ref().is_some_and(|options| !options.is_empty()) {
			return Err(unsupported("unsupported CREATE TABLE clause: OPTIONS"));
		}
		if *strict {
			return Err(unsupported("unsupported CREATE TABLE clause: STRICT"));
		}
		if *copy_grants {
			return Err(unsupported("unsupported CREATE TABLE clause: COPY GRANTS"));
		}
		if enable_schema_evolution.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ENABLE SCHEMA EVOLUTION",
			));
		}
		if change_tracking.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: CHANGE TRACKING",
			));
		}
		if data_retention_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DATA RETENTION",
			));
		}
		if max_data_extension_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: MAX DATA EXTENSION",
			));
		}
		if default_ddl_collation.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DEFAULT DDL COLLATION",
			));
		}
		if with_aggregation_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: AGGREGATION POLICY",
			));
		}
		if with_row_access_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ROW ACCESS POLICY",
			));
		}
		if with_tags.as_ref().is_some_and(|tags| !tags.is_empty()) {
			return Err(unsupported("unsupported CREATE TABLE clause: WITH TAGS"));
		}
		if !matches!(hive_distribution, HiveDistributionStyle::NONE) {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: Hive distribution",
			));
		}
		if columns.is_empty() {
			return Err(unsupported("CREATE TABLE requires at least one column"));
		}

		let table_ref = lower_create_table_name(name)?;
		let fields = columns
			.iter()
			.map(lower_create_table_column)
			.collect::<Result<Vec<_>, _>>()?;
		check_unique_column_names(&fields)?;
		let schema = Arc::new(Schema::new(fields));

		Ok(LogicalPlan::CreateTable {
			name: table_ref,
			schema,
			if_not_exists: *if_not_exists,
		})
	}

	fn lower_create_table_as_select(
		&self,
		create_table: &CreateTable,
	) -> Result<LogicalPlan, FrontendError> {
		let CreateTable {
			or_replace,
			temporary,
			external,
			global,
			if_not_exists,
			transient,
			volatile,
			name,
			columns,
			constraints,
			hive_distribution,
			hive_formats,
			table_properties,
			with_options,
			file_format,
			location,
			query,
			without_rowid,
			like,
			clone,
			engine,
			comment,
			auto_increment_offset,
			default_charset,
			collation,
			on_commit,
			on_cluster,
			primary_key,
			order_by,
			partition_by,
			cluster_by,
			clustered_by,
			options,
			strict,
			copy_grants,
			enable_schema_evolution,
			change_tracking,
			data_retention_time_in_days,
			max_data_extension_time_in_days,
			default_ddl_collation,
			with_aggregation_policy,
			with_row_access_policy,
			with_tags,
		} = create_table;

		if *if_not_exists {
			return Err(unsupported("CTAS does not support IF NOT EXISTS in v1"));
		}
		if !columns.is_empty() {
			return Err(unsupported(
				"CTAS does not support explicit column lists in v1; use SELECT aliases for column names",
			));
		}
		if *or_replace {
			return Err(unsupported("unsupported CREATE TABLE clause: OR REPLACE"));
		}
		if *temporary {
			return Err(unsupported("unsupported CREATE TABLE clause: TEMPORARY"));
		}
		if *external {
			return Err(unsupported("unsupported CREATE TABLE clause: EXTERNAL"));
		}
		if global.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: GLOBAL/LOCAL"));
		}
		if *transient {
			return Err(unsupported("unsupported CREATE TABLE clause: TRANSIENT"));
		}
		if *volatile {
			return Err(unsupported("unsupported CREATE TABLE clause: VOLATILE"));
		}
		if !constraints.is_empty() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: table-level constraints",
			));
		}
		if hive_formats.as_ref().is_some_and(|formats| {
			formats.row_format.is_some()
				|| formats
					.serde_properties
					.as_ref()
					.is_some_and(|properties| !properties.is_empty())
				|| formats.storage.is_some()
				|| formats.location.is_some()
		}) {
			return Err(unsupported("unsupported CREATE TABLE clause: HIVE FORMATS"));
		}
		if !table_properties.is_empty() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: TBLPROPERTIES",
			));
		}
		if !with_options.is_empty() {
			return Err(unsupported("unsupported CREATE TABLE clause: WITH options"));
		}
		if file_format.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: STORED AS"));
		}
		if location.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LOCATION"));
		}
		if *without_rowid {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: WITHOUT ROWID",
			));
		}
		if like.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: LIKE"));
		}
		if clone.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLONE"));
		}
		if engine.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ENGINE"));
		}
		if comment.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: COMMENT"));
		}
		if auto_increment_offset.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: AUTO_INCREMENT",
			));
		}
		if default_charset.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DEFAULT CHARSET",
			));
		}
		if collation.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: COLLATE"));
		}
		if on_commit.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON COMMIT"));
		}
		if on_cluster.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ON CLUSTER"));
		}
		if primary_key.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: table-level PRIMARY KEY",
			));
		}
		if order_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: ORDER BY"));
		}
		if partition_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: PARTITION BY"));
		}
		if cluster_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTER BY"));
		}
		if clustered_by.is_some() {
			return Err(unsupported("unsupported CREATE TABLE clause: CLUSTERED BY"));
		}
		if options.as_ref().is_some_and(|options| !options.is_empty()) {
			return Err(unsupported("unsupported CREATE TABLE clause: OPTIONS"));
		}
		if *strict {
			return Err(unsupported("unsupported CREATE TABLE clause: STRICT"));
		}
		if *copy_grants {
			return Err(unsupported("unsupported CREATE TABLE clause: COPY GRANTS"));
		}
		if enable_schema_evolution.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ENABLE SCHEMA EVOLUTION",
			));
		}
		if change_tracking.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: CHANGE TRACKING",
			));
		}
		if data_retention_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DATA RETENTION",
			));
		}
		if max_data_extension_time_in_days.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: MAX DATA EXTENSION",
			));
		}
		if default_ddl_collation.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: DEFAULT DDL COLLATION",
			));
		}
		if with_aggregation_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: AGGREGATION POLICY",
			));
		}
		if with_row_access_policy.is_some() {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: ROW ACCESS POLICY",
			));
		}
		if with_tags.as_ref().is_some_and(|tags| !tags.is_empty()) {
			return Err(unsupported("unsupported CREATE TABLE clause: WITH TAGS"));
		}
		if !matches!(hive_distribution, HiveDistributionStyle::NONE) {
			return Err(unsupported(
				"unsupported CREATE TABLE clause: Hive distribution",
			));
		}

		let query = query
			.as_deref()
			.ok_or_else(|| unsupported("CREATE TABLE AS SELECT requires a query"))?;
		let table = lower_create_table_name(name)?;
		let source = self.lower_query(query)?;
		Ok(LogicalPlan::CreateTableAsSelect {
			table,
			source: Box::new(source),
		})
	}

	fn lower_query(&self, query: &Query) -> Result<LogicalPlan, FrontendError> {
		let Query {
			with,
			body,
			order_by,
			limit,
			limit_by,
			offset,
			fetch,
			locks,
			for_clause,
			settings,
			format_clause,
		} = query;

		if with.is_some() {
			return Err(unsupported("WITH is not supported"));
		}
		if !limit_by.is_empty() {
			return Err(unsupported("LIMIT BY is not supported"));
		}
		if fetch.is_some() {
			return Err(unsupported("FETCH is not supported"));
		}
		if !locks.is_empty() {
			return Err(unsupported("locking clauses are not supported"));
		}
		if for_clause.is_some() {
			return Err(unsupported("FOR clause is not supported"));
		}
		if settings.is_some() {
			return Err(unsupported("query settings are not supported"));
		}
		if format_clause.is_some() {
			return Err(unsupported("FORMAT clause is not supported"));
		}
		if order_by.is_none() && (limit.is_some() || offset.is_some()) {
			return Err(unsupported(
				"LIMIT/OFFSET without ORDER BY is not supported",
			));
		}

		let LoweredSelect {
			plan,
			aggregate_output_catalog,
		} = match &**body {
			SetExpr::Select(select) => self.lower_select(select)?,
			_ => return Err(unsupported("unsupported query body")),
		};

		if let Some(order_by) = order_by {
			let order_by = self.lower_order_by(order_by, aggregate_output_catalog.as_ref())?;
			let (sort_input, order_by, final_projection) =
				if aggregate_output_catalog.is_some() && sort_needs_hidden_projection(&order_by) {
					add_hidden_sort_projection(plan, order_by, aggregate_output_catalog.as_ref())?
				} else {
					(plan, order_by, None)
				};
			let limit = limit
				.as_ref()
				.map(parse_non_negative_usize_literal)
				.transpose()?;
			let offset = offset
				.as_ref()
				.map(|offset| parse_non_negative_usize_literal(&offset.value))
				.transpose()?
				.unwrap_or(0);
			let sorted = LogicalPlan::Sort {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(sort_input),
					partition_count: 1,
					key: Vec::new(),
				}),
				order_by,
				limit,
				offset,
			};
			if let Some(final_projection) = final_projection {
				Ok(LogicalPlan::Project {
					input: Box::new(sorted),
					exprs: final_projection.exprs,
					output_names: final_projection.output_names,
				})
			} else {
				Ok(sorted)
			}
		} else {
			Ok(plan)
		}
	}

	fn lower_select(&self, select: &Select) -> Result<LoweredSelect, FrontendError> {
		let Select {
			select_token,
			distinct,
			top,
			top_before_distinct,
			projection,
			into,
			from,
			lateral_views,
			prewhere,
			selection,
			group_by,
			cluster_by,
			distribute_by,
			sort_by,
			having,
			named_window,
			qualify,
			window_before_qualify,
			value_table_mode,
			connect_by,
		} = select;
		let _parser_metadata_only = (select_token, top_before_distinct, window_before_qualify);

		if distinct.is_some() {
			return Err(unsupported("DISTINCT is not supported"));
		}
		if top.is_some() {
			return Err(unsupported("TOP is not supported"));
		}
		if into.is_some() {
			return Err(unsupported("SELECT INTO is not supported"));
		}
		if !lateral_views.is_empty() {
			return Err(unsupported("LATERAL VIEW is not supported"));
		}
		if prewhere.is_some() {
			return Err(unsupported("PREWHERE is not supported"));
		}
		if !cluster_by.is_empty() {
			return Err(unsupported("CLUSTER BY is not supported"));
		}
		if !distribute_by.is_empty() {
			return Err(unsupported("DISTRIBUTE BY is not supported"));
		}
		if !sort_by.is_empty() {
			return Err(unsupported("SORT BY is not supported"));
		}
		if !named_window.is_empty() {
			return Err(unsupported("WINDOW is not supported"));
		}
		if qualify.is_some() {
			return Err(unsupported("QUALIFY is not supported"));
		}
		if value_table_mode.is_some() {
			return Err(unsupported("SELECT AS VALUE/STRUCT is not supported"));
		}
		if connect_by.is_some() {
			return Err(unsupported("CONNECT BY is not supported"));
		}
		let group_keys = if group_by_is_empty(group_by) {
			None
		} else {
			Some(lower_group_by_keys(group_by)?)
		};
		if having.is_some()
			&& group_keys.is_none()
			&& !projection_contains_aggregate_call(projection)
		{
			return Err(unsupported(
				"HAVING requires GROUP BY or an aggregate projection",
			));
		}

		// 1. Handle FROM (Scan)
		if from.is_empty() {
			return Err(unsupported("SELECT without FROM is not supported"));
		}

		if from.len() > 1 {
			return Err(unsupported("comma joins are not supported"));
		}

		let table_with_joins = &from[0];
		if group_keys.is_some() && !table_with_joins.joins.is_empty() {
			return Err(unsupported("GROUP BY over joins is not supported"));
		}
		let mut plan = self.lower_table_factor(&table_with_joins.relation)?;
		for join in &table_with_joins.joins {
			let right = self.lower_table_factor(&join.relation)?;
			let (kind, on) = self.lower_join_operator(&join.join_operator)?;
			let (left_keys, right_keys) = self.join_exchange_keys(&on, &plan, &right)?;
			plan = LogicalPlan::Join {
				left: Box::new(LogicalPlan::Exchange {
					input: Box::new(plan),
					partition_count: DEFAULT_JOIN_PARTITION_COUNT,
					key: left_keys,
				}),
				right: Box::new(LogicalPlan::Exchange {
					input: Box::new(right),
					partition_count: DEFAULT_JOIN_PARTITION_COUNT,
					key: right_keys,
				}),
				kind,
				on,
				strategy: JoinStrategy::HashPartitioned,
			};
		}

		// 2. Handle WHERE (Filter / correlated EXISTS decorrelation)
		if let Some(selection) = selection {
			plan = self.lower_where(plan, selection)?;
		}
		if group_keys.is_some() && plan_contains_join(&plan) {
			return Err(unsupported("GROUP BY over joins is not supported"));
		}

		// 3. Handle SELECT list (Project or Aggregate)
		if let Some(group_keys) = group_keys {
			let group_expr = group_keys
				.iter()
				.map(|key| key.expr.clone())
				.collect::<Vec<_>>();
			let AggregateProjection {
				aggr_expr,
				catalog,
				final_projection,
			} = self.lower_aggregate_projection(
				projection,
				having.as_ref(),
				&group_keys,
				AggregateSqlMode::Grouped,
			)?;
			let having_predicate = having
				.as_ref()
				.map(|having| self.lower_having_predicate(having, &catalog))
				.transpose()?;
			if aggr_expr.is_empty() {
				return Err(unsupported("GROUP BY requires at least one aggregate call"));
			}
			let aggregate = LogicalPlan::Aggregate {
				input: Box::new(LogicalPlan::Exchange {
					input: Box::new(plan),
					partition_count: DEFAULT_AGGREGATE_PARTITION_COUNT,
					key: group_expr.clone(),
				}),
				group_expr,
				aggr_expr,
			};
			let plan = if let Some(predicate) = having_predicate {
				LogicalPlan::Filter {
					input: Box::new(aggregate),
					predicate,
				}
			} else {
				aggregate
			};
			let plan = if let Some(final_projection) = final_projection {
				LogicalPlan::Project {
					input: Box::new(plan),
					exprs: final_projection.exprs,
					output_names: final_projection.output_names,
				}
			} else {
				plan
			};
			return Ok(LoweredSelect {
				plan,
				aggregate_output_catalog: Some(catalog),
			});
		}

		if projection_contains_aggregate_call(projection) {
			// P0.5: accepted global aggregates keep the pre-ADR single-stage shape; no
			// distributed exchange is inserted for the accounting refactor.
			let AggregateProjection {
				aggr_expr,
				catalog,
				final_projection,
			} = self.lower_aggregate_projection(
				projection,
				having.as_ref(),
				&[],
				AggregateSqlMode::Global,
			)?;
			let aggregate = LogicalPlan::Aggregate {
				input: Box::new(plan),
				group_expr: vec![],
				aggr_expr,
			};
			let plan = if let Some(having) = having.as_ref() {
				let predicate = self.lower_having_predicate(having, &catalog)?;
				LogicalPlan::Filter {
					input: Box::new(aggregate),
					predicate,
				}
			} else {
				aggregate
			};
			let plan = if let Some(final_projection) = final_projection {
				LogicalPlan::Project {
					input: Box::new(plan),
					exprs: final_projection.exprs,
					output_names: final_projection.output_names,
				}
			} else {
				plan
			};
			return Ok(LoweredSelect {
				plan,
				aggregate_output_catalog: Some(catalog),
			});
		}

		if matches!(projection.as_slice(), [SelectItem::Wildcard(_)]) {
			return Ok(LoweredSelect {
				plan,
				aggregate_output_catalog: None,
			});
		}

		let mut exprs = Vec::new();
		let mut output_names = Vec::new();
		for item in projection {
			match item {
				SelectItem::UnnamedExpr(sql_expr) => {
					exprs.push(self.lower_expr(sql_expr)?);
					output_names.push(None);
				}
				SelectItem::ExprWithAlias { expr, alias } => {
					exprs.push(self.lower_expr(expr)?);
					output_names.push(Some(alias.value.clone()));
				}
				SelectItem::Wildcard(_) => {
					return Err(unsupported("mixed wildcard projections are not supported"));
				}
				_ => return Err(unsupported("unsupported select item")),
			}
		}

		Ok(LoweredSelect {
			plan: LogicalPlan::Project {
				input: Box::new(plan),
				exprs,
				output_names,
			},
			aggregate_output_catalog: None,
		})
	}

	fn lower_order_by(
		&self,
		order_by: &OrderBy,
		scope: Option<&AggregateOutputCatalog>,
	) -> Result<Vec<SortExpr>, FrontendError> {
		if order_by.interpolate.is_some() {
			return Err(unsupported("ORDER BY INTERPOLATE is not supported"));
		}
		if order_by.exprs.is_empty() {
			return Err(unsupported("ORDER BY requires at least one expression"));
		}

		order_by
			.exprs
			.iter()
			.map(|expr| {
				if expr.with_fill.is_some() {
					return Err(unsupported("ORDER BY WITH FILL is not supported"));
				}
				let sql_expr = if let Some(scope) = scope {
					self.rewrite_grouped_aggregate_calls(&expr.expr, scope, "ORDER BY")?
				} else {
					expr.expr.clone()
				};
				Ok(SortExpr {
					expr: self.lower_expr(&sql_expr)?,
					descending: expr.asc == Some(false),
					nulls_first: expr.nulls_first.unwrap_or(false),
				})
			})
			.collect()
	}

	fn lower_join_operator(
		&self,
		operator: &JoinOperator,
	) -> Result<(JoinKind, Vec<(Expr, Expr)>), FrontendError> {
		let (kind, constraint) = match operator {
			JoinOperator::Inner(constraint) => (JoinKind::Inner, constraint),
			JoinOperator::LeftOuter(constraint) => (JoinKind::LeftOuter, constraint),
			JoinOperator::RightOuter(constraint) => (JoinKind::RightOuter, constraint),
			JoinOperator::FullOuter(constraint) => (JoinKind::FullOuter, constraint),
			JoinOperator::Semi(constraint) | JoinOperator::LeftSemi(constraint) => {
				(JoinKind::LeftSemi, constraint)
			}
			JoinOperator::RightSemi(constraint) => (JoinKind::RightSemi, constraint),
			JoinOperator::Anti(constraint) | JoinOperator::LeftAnti(constraint) => {
				(JoinKind::LeftAnti, constraint)
			}
			JoinOperator::RightAnti(constraint) => (JoinKind::RightAnti, constraint),
			_ => {
				return Err(unsupported(
					"only INNER/OUTER/SEMI/ANTI JOIN ... ON is supported",
				));
			}
		};

		match constraint {
			JoinConstraint::On(expr) => Ok((kind, self.lower_join_on(expr)?)),
			_ => Err(unsupported("JOIN requires ON equality predicates")),
		}
	}

	fn lower_arg(&self, arg: &FunctionArg) -> Result<Expr, FrontendError> {
		match arg {
			FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => self.lower_expr(e),
			_ => Err(unsupported("unsupported function argument")),
		}
	}

	fn lower_aggregate_projection(
		&self,
		projection: &[SelectItem],
		having: Option<&SqlExpr>,
		group_keys: &[GroupKey],
		mode: AggregateSqlMode,
	) -> Result<AggregateProjection, FrontendError> {
		if matches!(mode, AggregateSqlMode::Global) {
			reject_global_aggregate_projection_columns(projection)?;
		}
		let mut catalog = AggregateOutputCatalog::new(group_keys);
		for item in projection {
			self.discover_select_aggregates(item, mode, &mut catalog)?;
		}
		if let Some(having) = having {
			reject_qualified_having_column_references(having)?;
			self.discover_aggregate_calls(having, mode, AggregateSource::Having, &mut catalog)?;
		}
		catalog.reject_having_alias_references(having)?;

		let final_projection = self.lower_final_aggregate_projection(projection, mode, &catalog)?;
		let project_required = final_projection.required;
		let aggr_expr = catalog.aggr_expr();
		let mut catalog = catalog;
		let final_output_names = if project_required {
			final_projection.derived_output_names()
		} else {
			catalog.natural_output_names()
		};
		catalog.set_final_output_names(final_output_names);

		Ok(AggregateProjection {
			aggr_expr,
			catalog,
			final_projection: project_required.then_some(FinalProjection {
				exprs: final_projection.exprs,
				output_names: final_projection.output_names,
			}),
		})
	}

	fn lower_having_predicate(
		&self,
		having: &SqlExpr,
		scope: &AggregateOutputCatalog,
	) -> Result<Expr, FrontendError> {
		reject_qualified_having_column_references(having)?;
		let rewritten = self.rewrite_grouped_aggregate_calls(having, scope, "HAVING")?;
		self.lower_expr(&rewritten)
	}

	fn rewrite_grouped_aggregate_calls(
		&self,
		expr: &SqlExpr,
		scope: &AggregateOutputCatalog,
		clause: &'static str,
	) -> Result<SqlExpr, FrontendError> {
		match expr {
			SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
				let output_name =
					self.resolve_grouped_aggregate_reference(function, scope, clause)?;
				Ok(SqlExpr::Identifier(Ident::new(output_name)))
			}
			SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
				left: Box::new(self.rewrite_grouped_aggregate_calls(left, scope, clause)?),
				op: op.clone(),
				right: Box::new(self.rewrite_grouped_aggregate_calls(right, scope, clause)?),
			}),
			SqlExpr::Cast {
				kind,
				expr,
				data_type,
				format,
			} => Ok(SqlExpr::Cast {
				kind: kind.clone(),
				expr: Box::new(self.rewrite_grouped_aggregate_calls(expr, scope, clause)?),
				data_type: data_type.clone(),
				format: format.clone(),
			}),
			SqlExpr::IsNull(inner) => Ok(SqlExpr::IsNull(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			SqlExpr::IsNotNull(inner) => Ok(SqlExpr::IsNotNull(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			SqlExpr::Nested(inner) => Ok(SqlExpr::Nested(Box::new(
				self.rewrite_grouped_aggregate_calls(inner, scope, clause)?,
			))),
			_ => Ok(expr.clone()),
		}
	}

	fn resolve_grouped_aggregate_reference(
		&self,
		function: &Function,
		scope: &AggregateOutputCatalog,
		clause: &'static str,
	) -> Result<String, FrontendError> {
		let (function, args) =
			self.lower_aggregate_function(function, AggregateSqlMode::Grouped)?;
		let signature = grouped_aggregate_signature(&function, &args)
			.ok_or_else(|| unsupported(aggregate_call_not_in_select_message(clause)))?;
		let outcome = match clause {
			"HAVING" => scope.resolve_for_having(&signature),
			"ORDER BY" => scope.resolve_for_order_by(&signature),
			_ => unreachable!("unsupported grouped aggregate clause: {clause}"),
		};
		match outcome {
			ResolveOutcome::Found(output_name) => Ok(output_name),
			ResolveOutcome::NotInSelect => {
				Err(unsupported(aggregate_call_not_in_select_message(clause)))
			}
			ResolveOutcome::Ambiguous => Err(unsupported(ambiguous_aggregate_call_message(clause))),
		}
	}

	fn discover_select_aggregates(
		&self,
		item: &SelectItem,
		mode: AggregateSqlMode,
		catalog: &mut AggregateOutputCatalog,
	) -> Result<(), FrontendError> {
		let (expr, alias) = select_item_expr_and_alias(item)?;
		if let Some(alias) = alias {
			catalog.add_select_alias(alias);
		}
		if let SqlExpr::Function(function) = strip_nested_sql_expr(expr) {
			if aggregate_function_name(&function.name).is_some() {
				let (function, args) = self.lower_aggregate_function(function, mode)?;
				let visible_name = alias
					.map(|alias| alias.value.clone())
					.unwrap_or_else(|| aggregate_output_name(&function).to_string());
				let index = catalog.add_requirement(function, args, AggregateSource::Select);
				catalog.mark_order_by_visible(index, visible_name);
				return Ok(());
			}
		}
		self.discover_aggregate_calls(expr, mode, AggregateSource::Select, catalog)
	}

	fn discover_aggregate_calls(
		&self,
		expr: &SqlExpr,
		mode: AggregateSqlMode,
		source: AggregateSource,
		catalog: &mut AggregateOutputCatalog,
	) -> Result<(), FrontendError> {
		match strip_nested_sql_expr(expr) {
			SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
				let (function, args) = self.lower_aggregate_function(function, mode)?;
				catalog.add_requirement(function, args, source);
				Ok(())
			}
			SqlExpr::Function(function) => {
				if let FunctionArguments::List(list) = &function.args {
					for arg in &list.args {
						match arg {
							FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
							| FunctionArg::Named {
								arg: FunctionArgExpr::Expr(expr),
								..
							}
							| FunctionArg::ExprNamed {
								arg: FunctionArgExpr::Expr(expr),
								..
							} => self.discover_aggregate_calls(expr, mode, source, catalog)?,
							FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
							_ => {}
						}
					}
				}
				Ok(())
			}
			SqlExpr::BinaryOp { left, right, .. } => {
				self.discover_aggregate_calls(left, mode, source, catalog)?;
				self.discover_aggregate_calls(right, mode, source, catalog)
			}
			SqlExpr::Cast { expr, .. } => {
				self.discover_aggregate_calls(expr, mode, source, catalog)
			}
			SqlExpr::UnaryOp { expr, .. }
			| SqlExpr::IsNull(expr)
			| SqlExpr::IsNotNull(expr)
			| SqlExpr::Nested(expr) => self.discover_aggregate_calls(expr, mode, source, catalog),
			_ => Ok(()),
		}
	}

	fn lower_final_aggregate_projection(
		&self,
		projection: &[SelectItem],
		mode: AggregateSqlMode,
		catalog: &AggregateOutputCatalog,
	) -> Result<LoweredAggregateProjection, FrontendError> {
		for item in projection {
			let (expr, _) = select_item_expr_and_alias(item)?;
			reject_qualified_aggregate_projection_references(expr)?;
		}
		let has_alias = projection
			.iter()
			.map(select_item_expr_and_alias)
			.collect::<Result<Vec<_>, _>>()?
			.iter()
			.any(|(_, alias)| alias.is_some());
		let has_computed_expression = projection
			.iter()
			.map(select_item_expr_and_alias)
			.collect::<Result<Vec<_>, _>>()?
			.iter()
			.any(|(expr, _)| select_expr_is_computed(expr));
		let natural_projection =
			aggregate_projection_matches_natural(projection, mode, catalog, self)?;
		let has_having_only_aggregate = catalog
			.entries
			.iter()
			.any(|entry| entry.in_having && !entry.in_select);
		let required = has_alias
			|| has_computed_expression
			|| !natural_projection
			|| has_having_only_aggregate;

		if !required {
			return Ok(LoweredAggregateProjection {
				exprs: Vec::new(),
				output_names: Vec::new(),
				required,
			});
		}

		let mut exprs = Vec::with_capacity(projection.len());
		let mut output_names = Vec::with_capacity(projection.len());
		for item in projection {
			let (sql_expr, alias) = select_item_expr_and_alias(item)?;
			let rewritten = self.rewrite_aggregate_calls_for_projection(sql_expr, mode, catalog)?;
			exprs.push(self.lower_expr(&rewritten)?);
			output_names.push(alias.map(|alias| alias.value.clone()));
		}

		Ok(LoweredAggregateProjection {
			exprs,
			output_names,
			required,
		})
	}

	fn rewrite_aggregate_calls_for_projection(
		&self,
		expr: &SqlExpr,
		mode: AggregateSqlMode,
		catalog: &AggregateOutputCatalog,
	) -> Result<SqlExpr, FrontendError> {
		match expr {
			SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
				let (function, args) = self.lower_aggregate_function(function, mode)?;
				let signature = grouped_aggregate_signature(&function, &args)
					.ok_or_else(|| unsupported("unsupported aggregate projection item"))?;
				match catalog.resolve_for_projection(&signature) {
					ResolveOutcome::Found(output_name) => {
						Ok(SqlExpr::Identifier(Ident::new(output_name)))
					}
					ResolveOutcome::NotInSelect => {
						Err(unsupported("unsupported aggregate projection item"))
					}
					ResolveOutcome::Ambiguous => Err(unsupported("ambiguous aggregate projection")),
				}
			}
			SqlExpr::BinaryOp { left, op, right } => Ok(SqlExpr::BinaryOp {
				left: Box::new(self.rewrite_aggregate_calls_for_projection(left, mode, catalog)?),
				op: op.clone(),
				right: Box::new(self.rewrite_aggregate_calls_for_projection(right, mode, catalog)?),
			}),
			SqlExpr::Cast {
				kind,
				expr,
				data_type,
				format,
			} => Ok(SqlExpr::Cast {
				kind: kind.clone(),
				expr: Box::new(self.rewrite_aggregate_calls_for_projection(expr, mode, catalog)?),
				data_type: data_type.clone(),
				format: format.clone(),
			}),
			SqlExpr::IsNull(inner) => Ok(SqlExpr::IsNull(Box::new(
				self.rewrite_aggregate_calls_for_projection(inner, mode, catalog)?,
			))),
			SqlExpr::IsNotNull(inner) => Ok(SqlExpr::IsNotNull(Box::new(
				self.rewrite_aggregate_calls_for_projection(inner, mode, catalog)?,
			))),
			SqlExpr::Nested(inner) => Ok(SqlExpr::Nested(Box::new(
				self.rewrite_aggregate_calls_for_projection(inner, mode, catalog)?,
			))),
			_ => Ok(expr.clone()),
		}
	}

	fn lower_aggregate_function(
		&self,
		function: &Function,
		mode: AggregateSqlMode,
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		let name = aggregate_function_name(&function.name)
			.ok_or_else(|| unsupported("unsupported aggregate function"))?;
		let args = aggregate_function_args(function)?;

		match name {
			"COUNT" => self.lower_count_aggregate(args),
			"SUM" => self.lower_sum_aggregate(args),
			"MIN" => self.lower_min_aggregate(args),
			"MAX" => self.lower_max_aggregate(args),
			"AVG" => self.lower_avg_aggregate(args),
			"LINEAR_REGRESS" => {
				if matches!(mode, AggregateSqlMode::Grouped) {
					return Err(unsupported(
						"experimental aggregate functions are not supported with GROUP BY",
					));
				}
				self.lower_linear_regress_aggregate(args)
			}
			"APPROX_QUANTILE" => {
				if matches!(mode, AggregateSqlMode::Grouped) {
					return Err(unsupported(
						"experimental aggregate functions are not supported with GROUP BY",
					));
				}
				self.lower_approx_quantile_aggregate(args)
			}
			_ => Err(unsupported("unsupported aggregate function")),
		}
	}

	fn lower_count_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("COUNT requires one argument"));
		}
		match &args[0] {
			FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {
				Ok((AggregateFunction::CountAll, vec![Expr::column("count")]))
			}
			FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) => {
				validate_aggregate_argument(expr)?;
				Ok((AggregateFunction::Count, vec![self.lower_expr(expr)?]))
			}
			_ => Err(unsupported(AGGREGATE_ARGUMENT_SHAPE_MESSAGE)),
		}
	}

	fn lower_sum_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("SUM requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Sum, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_min_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("MIN requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Min, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_max_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("MAX requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Max, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_avg_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 1 {
			return Err(unsupported("AVG requires one argument"));
		}
		validate_simple_aggregate_arg(&args[0])?;
		Ok((AggregateFunction::Avg, vec![self.lower_arg(&args[0])?]))
	}

	fn lower_linear_regress_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 2 {
			return Err(unsupported("LINEAR_REGRESS requires two arguments"));
		}
		Ok((
			AggregateFunction::LinearRegress,
			vec![self.lower_arg(&args[0])?, self.lower_arg(&args[1])?],
		))
	}

	fn lower_approx_quantile_aggregate(
		&self,
		args: &[FunctionArg],
	) -> Result<(AggregateFunction, Vec<Expr>), FrontendError> {
		if args.len() != 2 {
			return Err(unsupported("APPROX_QUANTILE requires two arguments"));
		}
		let val = self.lower_arg(&args[0])?;
		let q = match &args[1] {
			FunctionArg::Unnamed(FunctionArgExpr::Expr(SqlExpr::Value(Value::Number(n, _)))) => {
				n.parse::<f64>().map_err(|e| unsupported(e.to_string()))?
			}
			_ => {
				return Err(unsupported(
					"APPROX_QUANTILE expects a numeric literal for quantile",
				));
			}
		};
		Ok((AggregateFunction::ApproxQuantile(q), vec![val]))
	}

	fn lower_table_factor(&self, factor: &TableFactor) -> Result<LogicalPlan, FrontendError> {
		match factor {
			TableFactor::Table { name, alias, .. } => {
				let table = table_ref_from_object_name(name);
				let plan = LogicalPlan::ScanNamed {
					table,
					projection: None,
				};
				Ok(alias_plan(plan, alias.as_ref()))
			}
			_ => Err(unsupported("unsupported table factor")),
		}
	}

	fn lower_where(
		&self,
		mut plan: LogicalPlan,
		selection: &SqlExpr,
	) -> Result<LogicalPlan, FrontendError> {
		let mut residuals = Vec::new();
		for conjunct in split_top_level_and(selection) {
			match strip_nested_sql_expr(conjunct) {
				SqlExpr::Exists { subquery, negated } => {
					plan = self.lower_exists_join(plan, subquery, *negated)?;
				}
				_ => residuals.push(self.lower_expr(conjunct)?),
			}
		}

		if let Some(predicate) = combine_and_exprs(residuals) {
			plan = LogicalPlan::Filter {
				input: Box::new(plan),
				predicate,
			};
		}

		Ok(plan)
	}

	fn lower_exists_join(
		&self,
		outer_plan: LogicalPlan,
		subquery: &Query,
		negated: bool,
	) -> Result<LogicalPlan, FrontendError> {
		validate_exists_query_shape(subquery)?;
		let SetExpr::Select(select) = &*subquery.body else {
			return Err(unsupported("unsupported EXISTS subquery body"));
		};
		validate_exists_select_shape(select)?;
		validate_exists_projection(&select.projection)?;

		let table_with_joins = select
			.from
			.first()
			.expect("exists select shape has one FROM item");
		let mut inner_plan = self.lower_table_factor(&table_with_joins.relation)?;
		let outer_qualifiers = plan_qualifiers(&outer_plan);
		let inner_qualifiers = plan_qualifiers(&inner_plan);
		let mut on = Vec::new();
		let mut inner_residuals = Vec::new();

		if let Some(selection) = &select.selection {
			for conjunct in split_top_level_and(selection) {
				match self.classify_exists_conjunct(
					conjunct,
					&outer_qualifiers,
					&inner_qualifiers,
				)? {
					ExistsConjunct::CorrelatedPair { outer, inner } => on.push((outer, inner)),
					ExistsConjunct::InnerResidual(predicate) => inner_residuals.push(predicate),
				}
			}
		}

		if on.is_empty() {
			return Err(unsupported("uncorrelated EXISTS is not supported"));
		}

		if let Some(predicate) = combine_and_exprs(inner_residuals) {
			inner_plan = LogicalPlan::Filter {
				input: Box::new(inner_plan),
				predicate,
			};
		}

		let left_key = on.iter().map(|(outer, _)| outer.clone()).collect();
		let right_key = on.iter().map(|(_, inner)| inner.clone()).collect();
		Ok(LogicalPlan::Join {
			left: Box::new(LogicalPlan::Exchange {
				input: Box::new(outer_plan),
				partition_count: DEFAULT_JOIN_PARTITION_COUNT,
				key: left_key,
			}),
			right: Box::new(LogicalPlan::Exchange {
				input: Box::new(inner_plan),
				partition_count: DEFAULT_JOIN_PARTITION_COUNT,
				key: right_key,
			}),
			kind: if negated {
				JoinKind::LeftAnti
			} else {
				JoinKind::LeftSemi
			},
			on,
			strategy: JoinStrategy::HashPartitioned,
		})
	}

	fn classify_exists_conjunct(
		&self,
		conjunct: &SqlExpr,
		outer_qualifiers: &[TableRef],
		inner_qualifiers: &[TableRef],
	) -> Result<ExistsConjunct, FrontendError> {
		let conjunct = strip_nested_sql_expr(conjunct);
		if let SqlExpr::BinaryOp { left, op, right } = conjunct {
			if *op == BinaryOperator::Eq {
				let left_column = scoped_column_expr(left, outer_qualifiers, inner_qualifiers)?;
				let right_column = scoped_column_expr(right, outer_qualifiers, inner_qualifiers)?;
				match (left_column, right_column) {
					(Some((ColumnScope::Outer, outer)), Some((ColumnScope::Inner, inner)))
					| (Some((ColumnScope::Inner, inner)), Some((ColumnScope::Outer, outer))) => {
						return Ok(ExistsConjunct::CorrelatedPair { outer, inner });
					}
					_ => {}
				}
			}
		}

		let mut scope_refs = ScopeRefs::default();
		collect_scope_refs(
			conjunct,
			outer_qualifiers,
			inner_qualifiers,
			&mut scope_refs,
		)?;
		if scope_refs.outer || scope_refs.unsupported {
			return Err(unsupported("unsupported EXISTS subquery predicate"));
		}

		Ok(ExistsConjunct::InnerResidual(self.lower_expr(conjunct)?))
	}

	fn lower_join_on(&self, expr: &SqlExpr) -> Result<Vec<(Expr, Expr)>, FrontendError> {
		match expr {
			SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
				let mut pairs = self.lower_join_on(left)?;
				pairs.extend(self.lower_join_on(right)?);
				Ok(pairs)
			}
			SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::Eq => {
				Ok(vec![(self.lower_expr(left)?, self.lower_expr(right)?)])
			}
			_ => Err(unsupported("join ON only supports equality predicates")),
		}
	}

	fn lower_expr(&self, sql_expr: &SqlExpr) -> Result<Expr, FrontendError> {
		match sql_expr {
			SqlExpr::Identifier(ident) => Ok(Expr::column(ident.value.clone())),
			SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(Expr::qualified_column(
				TableRef::new(idents[0].value.clone()),
				idents[1].value.clone(),
			)),
			SqlExpr::Value(val) => match val {
				Value::Number(n, _) => {
					if let Ok(i) = n.parse::<i32>() {
						Ok(Expr::Literal(ScalarValue::Int32(Some(i))))
					} else if let Ok(f) = n.parse::<f64>() {
						Ok(Expr::Literal(ScalarValue::Float64(Some(f))))
					} else {
						Err(unsupported(format!("unsupported number literal: {n}")))
					}
				}
				Value::SingleQuotedString(s) => {
					Ok(Expr::Literal(ScalarValue::Utf8(Some(s.clone()))))
				}
				Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(Some(*b)))),
				Value::Null => Err(unsupported("explicit NULL literal not supported")),
				_ => Err(unsupported(format!("unsupported value: {val:?}"))),
			},
			SqlExpr::BinaryOp { left, op, right } => {
				let l = Box::new(self.lower_expr(left)?);
				let r = Box::new(self.lower_expr(right)?);
				match op {
					BinaryOperator::Eq => Ok(Expr::Eq(l, r)),
					BinaryOperator::NotEq => Ok(Expr::Neq(l, r)),
					BinaryOperator::Lt => Ok(Expr::Lt(l, r)),
					BinaryOperator::LtEq => Ok(Expr::Lte(l, r)),
					BinaryOperator::Gt => Ok(Expr::Gt(l, r)),
					BinaryOperator::GtEq => Ok(Expr::Gte(l, r)),
					BinaryOperator::And => Ok(Expr::And(l, r)),
					BinaryOperator::Or => Ok(Expr::Or(l, r)),
					BinaryOperator::Plus => Ok(Expr::Add(l, r)),
					BinaryOperator::Minus => Ok(Expr::Sub(l, r)),
					BinaryOperator::Multiply => Ok(Expr::Mul(l, r)),
					BinaryOperator::Divide => Ok(Expr::Div(l, r)),
					_ => Err(unsupported(format!("unsupported operator: {op:?}"))),
				}
			}
			SqlExpr::Cast {
				kind,
				expr,
				data_type,
				format,
			} => {
				if format.is_some() {
					return Err(unsupported("CAST FORMAT clause is not supported"));
				}
				match kind {
					CastKind::Cast => Ok(Expr::Cast(
						Box::new(self.lower_expr(expr)?),
						lower_cast_target_type(data_type)?,
					)),
					CastKind::TryCast | CastKind::SafeCast => Ok(Expr::TryCast(
						Box::new(self.lower_expr(expr)?),
						lower_cast_target_type(data_type)?,
					)),
					CastKind::DoubleColon => Err(unsupported(
						"unsupported cast syntax: ::; only CAST(... AS ...) is supported",
					)),
				}
			}
			SqlExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(self.lower_expr(expr)?))),
			SqlExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(self.lower_expr(expr)?))),
			SqlExpr::Nested(expr) => self.lower_expr(expr),
			SqlExpr::Function(f) if f.name.to_string().to_uppercase() == "TIME_BUCKET" => {
				let args = match &f.args {
					sqlparser::ast::FunctionArguments::List(l) => &l.args,
					_ => return Err(unsupported("TIME_BUCKET expects arguments")),
				};
				if args.len() != 2 {
					return Err(unsupported("TIME_BUCKET expects exactly two arguments"));
				}
				let ts = self.lower_arg(&args[0])?;
				let interval = match &args[1] {
					FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(
						SqlExpr::Value(Value::SingleQuotedString(s)),
					)) => match s.as_str() {
						"1m" => std::time::Duration::from_secs(60),
						"1h" => std::time::Duration::from_secs(3600),
						"1d" => std::time::Duration::from_secs(86400),
						_ => {
							return Err(unsupported(format!(
								"unsupported time_bucket interval: {s}"
							)));
						}
					},
					_ => return Err(unsupported("time_bucket expects a string literal interval")),
				};
				Ok(Expr::TimeBucket(interval, Box::new(ts)))
			}
			_ => Err(unsupported(format!("unsupported expression: {sql_expr:?}"))),
		}
	}

	fn join_exchange_keys(
		&self,
		on: &[(Expr, Expr)],
		left: &LogicalPlan,
		right: &LogicalPlan,
	) -> Result<(Vec<Expr>, Vec<Expr>), FrontendError> {
		let left_qualifiers = plan_qualifiers(left);
		let right_qualifiers = plan_qualifiers(right);
		let mut left_keys = Vec::with_capacity(on.len());
		let mut right_keys = Vec::with_capacity(on.len());

		for (first, second) in on {
			let first_side = expr_side(first, &left_qualifiers, &right_qualifiers);
			let second_side = expr_side(second, &left_qualifiers, &right_qualifiers);
			match (first_side, second_side) {
				(Some(JoinSide::Left), Some(JoinSide::Right))
				| (Some(JoinSide::Left), None)
				| (None, Some(JoinSide::Right))
				| (None, None) => {
					left_keys.push(first.clone());
					right_keys.push(second.clone());
				}
				(Some(JoinSide::Right), Some(JoinSide::Left))
				| (Some(JoinSide::Right), None)
				| (None, Some(JoinSide::Left)) => {
					left_keys.push(second.clone());
					right_keys.push(first.clone());
				}
				_ => {
					return Err(unsupported(
						"join ON equality must reference one column from each side",
					));
				}
			}
		}

		Ok((left_keys, right_keys))
	}
}

fn sort_needs_hidden_projection(order_by: &[SortExpr]) -> bool {
	order_by
		.iter()
		.any(|key| !sort_key_is_direct_output_reference(&key.expr))
}

fn sort_key_is_direct_output_reference(expr: &Expr) -> bool {
	matches!(expr, Expr::Column(_) | Expr::Literal(_))
}

fn validate_hidden_sort_key_refs(
	expr: &Expr,
	visible_names: &HashSet<String>,
) -> Result<(), FrontendError> {
	match expr {
		Expr::Column(column) => {
			if column.qualifier.is_some() || !visible_names.contains(&column.name.0) {
				return Err(unsupported(
					"ORDER BY computed aggregate expression must reference visible output columns",
				));
			}
			Ok(())
		}
		Expr::Literal(_) => Ok(()),
		Expr::Cast(inner, _) | Expr::TryCast(inner, _) => {
			validate_hidden_sort_key_refs(inner, visible_names)
		}
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
			validate_hidden_sort_key_refs(left, visible_names)?;
			validate_hidden_sort_key_refs(right, visible_names)
		}
		Expr::Not(inner)
		| Expr::IsNull(inner)
		| Expr::IsNotNull(inner)
		| Expr::TimeBucket(_, inner) => validate_hidden_sort_key_refs(inner, visible_names),
	}
}

fn add_hidden_sort_projection(
	plan: LogicalPlan,
	order_by: Vec<SortExpr>,
	catalog: Option<&AggregateOutputCatalog>,
) -> Result<(LogicalPlan, Vec<SortExpr>, Option<FinalProjection>), FrontendError> {
	let output_names = visible_output_names(&plan, catalog).ok_or_else(|| {
		unsupported("ORDER BY computed aggregate expression is not supported for this plan shape")
	})?;
	if has_duplicate_names(&output_names) {
		return Err(unsupported(
			"ORDER BY computed aggregate expression requires unique output names",
		));
	}

	let visible_names = output_names.iter().cloned().collect::<HashSet<_>>();
	let mut used_names = output_names.iter().cloned().collect::<HashSet<_>>();
	let mut projected_exprs = output_names
		.iter()
		.cloned()
		.map(Expr::column)
		.collect::<Vec<_>>();
	let mut projected_output_names = output_names
		.iter()
		.cloned()
		.map(Some)
		.collect::<Vec<Option<String>>>();
	let mut rewritten_order_by = Vec::with_capacity(order_by.len());

	for (idx, key) in order_by.into_iter().enumerate() {
		if sort_key_is_direct_output_reference(&key.expr) {
			rewritten_order_by.push(key);
			continue;
		}

		validate_hidden_sort_key_refs(&key.expr, &visible_names)?;
		let hidden_name = unique_hidden_sort_name(idx, &mut used_names);
		projected_exprs.push(key.expr);
		projected_output_names.push(Some(hidden_name.clone()));
		rewritten_order_by.push(SortExpr {
			expr: Expr::column(hidden_name),
			descending: key.descending,
			nulls_first: key.nulls_first,
		});
	}

	let final_projection = FinalProjection {
		exprs: output_names
			.iter()
			.cloned()
			.map(Expr::column)
			.collect::<Vec<_>>(),
		output_names: output_names.into_iter().map(Some).collect(),
	};
	Ok((
		LogicalPlan::Project {
			input: Box::new(plan),
			exprs: projected_exprs,
			output_names: projected_output_names,
		},
		rewritten_order_by,
		Some(final_projection),
	))
}

fn visible_output_names(
	plan: &LogicalPlan,
	catalog: Option<&AggregateOutputCatalog>,
) -> Option<Vec<String>> {
	match plan {
		LogicalPlan::Project {
			exprs,
			output_names,
			..
		} => Some(
			exprs
				.iter()
				.enumerate()
				.map(|(idx, expr)| {
					output_names[idx]
						.clone()
						.unwrap_or_else(|| logical_expr_output_name(expr, idx))
				})
				.collect(),
		),
		LogicalPlan::Filter { input, .. } => visible_output_names(input, catalog),
		LogicalPlan::Aggregate { .. } => catalog.map(AggregateOutputCatalog::natural_output_names),
		_ => None,
	}
}

fn has_duplicate_names(names: &[String]) -> bool {
	let mut seen = HashSet::new();
	names.iter().any(|name| !seen.insert(name))
}

fn unique_hidden_sort_name(idx: usize, used_names: &mut HashSet<String>) -> String {
	let mut candidate = format!("__toron_order_key_{idx}");
	let mut suffix = 0usize;
	while used_names.contains(&candidate) {
		suffix += 1;
		candidate = format!("__toron_order_key_{idx}_{suffix}");
	}
	used_names.insert(candidate.clone());
	candidate
}

#[derive(Debug, Clone)]
struct GroupKey {
	name: String,
	expr: Expr,
}

#[derive(Debug)]
struct LoweredSelect {
	plan: LogicalPlan,
	aggregate_output_catalog: Option<AggregateOutputCatalog>,
}

#[derive(Debug)]
struct AggregateProjection {
	aggr_expr: Vec<(AggregateFunction, Vec<Expr>)>,
	catalog: AggregateOutputCatalog,
	final_projection: Option<FinalProjection>,
}

#[derive(Debug)]
struct FinalProjection {
	exprs: Vec<Expr>,
	output_names: Vec<Option<String>>,
}

#[derive(Debug)]
struct LoweredAggregateProjection {
	exprs: Vec<Expr>,
	output_names: Vec<Option<String>>,
	required: bool,
}

impl LoweredAggregateProjection {
	fn derived_output_names(&self) -> Vec<String> {
		self.exprs
			.iter()
			.enumerate()
			.map(|(idx, expr)| {
				self.output_names[idx]
					.clone()
					.unwrap_or_else(|| logical_expr_output_name(expr, idx))
			})
			.collect()
	}
}

#[derive(Debug)]
struct AggregateOutputCatalog {
	group_keys: Vec<GroupKey>,
	entries: Vec<AggregateOutputEntry>,
	natural_output_name_counts: HashMap<String, usize>,
	final_output_name_counts: HashMap<String, usize>,
	select_aliases: HashSet<String>,
}

impl AggregateOutputCatalog {
	fn new(group_keys: &[GroupKey]) -> Self {
		let mut natural_output_name_counts = HashMap::new();
		for key in group_keys {
			*natural_output_name_counts
				.entry(key.name.clone())
				.or_insert(0) += 1;
		}

		Self {
			group_keys: group_keys.to_vec(),
			entries: Vec::new(),
			natural_output_name_counts,
			final_output_name_counts: HashMap::new(),
			select_aliases: HashSet::new(),
		}
	}

	fn add_requirement(
		&mut self,
		function: AggregateFunction,
		args: Vec<Expr>,
		source: AggregateSource,
	) -> usize {
		let signature = grouped_aggregate_signature(&function, &args);
		if let Some(signature) = signature.as_ref() {
			if let Some(index) = self
				.entries
				.iter()
				.position(|entry| entry.signature.as_ref() == Some(signature))
			{
				self.entries[index].mark_source(source);
				return index;
			}
		}

		let natural_name = aggregate_output_name(&function).to_string();
		*self
			.natural_output_name_counts
			.entry(natural_name.clone())
			.or_insert(0) += 1;
		let index = self.entries.len();
		self.entries.push(AggregateOutputEntry {
			function,
			args,
			signature,
			natural_name,
			in_select: matches!(source, AggregateSource::Select),
			in_having: matches!(source, AggregateSource::Having),
			order_by_visible_names: Vec::new(),
		});
		index
	}

	fn add_select_alias(&mut self, alias: &Ident) {
		self.select_aliases.insert(alias.value.clone());
	}

	fn mark_order_by_visible(&mut self, index: usize, output_name: String) {
		if let Some(entry) = self.entries.get_mut(index) {
			if !entry.order_by_visible_names.contains(&output_name) {
				entry.order_by_visible_names.push(output_name);
			}
		}
	}

	fn aggr_expr(&self) -> Vec<(AggregateFunction, Vec<Expr>)> {
		self.entries
			.iter()
			.map(|entry| (entry.function.clone(), entry.args.clone()))
			.collect()
	}

	fn natural_output_names(&self) -> Vec<String> {
		self.group_keys
			.iter()
			.map(|key| key.name.clone())
			.chain(self.entries.iter().map(|entry| entry.natural_name.clone()))
			.collect()
	}

	fn set_final_output_names(&mut self, names: Vec<String>) {
		self.final_output_name_counts.clear();
		for name in names {
			*self.final_output_name_counts.entry(name).or_insert(0) += 1;
		}
	}

	fn resolve_for_projection(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		self.resolve_natural(signature)
	}

	fn resolve_for_having(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		self.resolve_natural(signature)
	}

	fn resolve_natural(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		let Some(entry) = self.entry_for_signature(signature) else {
			return ResolveOutcome::NotInSelect;
		};
		if self
			.natural_output_name_counts
			.get(&entry.natural_name)
			.copied()
			.unwrap_or(0)
			> 1
		{
			return ResolveOutcome::Ambiguous;
		}
		ResolveOutcome::Found(entry.natural_name.clone())
	}

	fn resolve_for_order_by(&self, signature: &GroupedAggregateSignature) -> ResolveOutcome {
		let Some(entry) = self.entry_for_signature(signature) else {
			return ResolveOutcome::NotInSelect;
		};
		let [output_name] = entry.order_by_visible_names.as_slice() else {
			return if entry.order_by_visible_names.is_empty() {
				ResolveOutcome::NotInSelect
			} else {
				ResolveOutcome::Ambiguous
			};
		};
		if self
			.final_output_name_counts
			.get(output_name)
			.copied()
			.unwrap_or(0)
			> 1
		{
			return ResolveOutcome::Ambiguous;
		}
		ResolveOutcome::Found(output_name.clone())
	}

	fn entry_for_signature(
		&self,
		signature: &GroupedAggregateSignature,
	) -> Option<&AggregateOutputEntry> {
		self.entries
			.iter()
			.find(|entry| entry.signature.as_ref() == Some(signature))
	}

	fn reject_having_alias_references(
		&self,
		having: Option<&SqlExpr>,
	) -> Result<(), FrontendError> {
		let Some(having) = having else {
			return Ok(());
		};
		reject_having_alias_references(having, &self.select_aliases)
	}
}

#[derive(Debug)]
struct AggregateOutputEntry {
	function: AggregateFunction,
	args: Vec<Expr>,
	signature: Option<GroupedAggregateSignature>,
	natural_name: String,
	in_select: bool,
	in_having: bool,
	order_by_visible_names: Vec<String>,
}

impl AggregateOutputEntry {
	fn mark_source(&mut self, source: AggregateSource) {
		match source {
			AggregateSource::Select => self.in_select = true,
			AggregateSource::Having => self.in_having = true,
		}
	}
}

#[derive(Debug, Clone)]
struct GroupedAggregateSignature {
	kernel: aggregate_kernel::StableAggregateKernel,
	args: Vec<Expr>,
}

impl PartialEq for GroupedAggregateSignature {
	fn eq(&self, other: &Self) -> bool {
		self.kernel == other.kernel
			&& self.args.len() == other.args.len()
			&& self
				.args
				.iter()
				.zip(&other.args)
				.all(|(left, right)| expr_structural_eq(left, right))
	}
}

impl Eq for GroupedAggregateSignature {}

impl Hash for GroupedAggregateSignature {
	fn hash<H: Hasher>(&self, state: &mut H) {
		self.kernel.hash(state);
		self.args.len().hash(state);
		for arg in &self.args {
			hash_expr_structural(arg, state);
		}
	}
}

fn hash_expr_structural(expr: &Expr, state: &mut impl Hasher) {
	match expr {
		Expr::Column(column) => {
			0u8.hash(state);
			column.hash(state);
		}
		Expr::Literal(value) => {
			1u8.hash(state);
			hash_scalar_structural(value, state);
		}
		Expr::Cast(inner, data_type) => {
			2u8.hash(state);
			hash_expr_structural(inner, state);
			hash_data_type_structural(data_type, state);
		}
		Expr::TryCast(inner, data_type) => {
			3u8.hash(state);
			hash_expr_structural(inner, state);
			hash_data_type_structural(data_type, state);
		}
		Expr::Add(left, right) => {
			4u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Sub(left, right) => {
			5u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Mul(left, right) => {
			6u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Div(left, right) => {
			7u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Eq(left, right) => {
			8u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Neq(left, right) => {
			9u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Lt(left, right) => {
			10u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Lte(left, right) => {
			11u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Gt(left, right) => {
			12u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Gte(left, right) => {
			13u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::And(left, right) => {
			14u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Or(left, right) => {
			15u8.hash(state);
			hash_expr_structural(left, state);
			hash_expr_structural(right, state);
		}
		Expr::Not(inner) => {
			16u8.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::IsNull(inner) => {
			17u8.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::IsNotNull(inner) => {
			18u8.hash(state);
			hash_expr_structural(inner, state);
		}
		Expr::TimeBucket(interval, inner) => {
			19u8.hash(state);
			interval.hash(state);
			hash_expr_structural(inner, state);
		}
	}
}

fn expr_structural_eq(left: &Expr, right: &Expr) -> bool {
	match (left, right) {
		(Expr::Column(left), Expr::Column(right)) => left == right,
		(Expr::Literal(left), Expr::Literal(right)) => scalar_structural_eq(left, right),
		(Expr::Cast(left_inner, left_type), Expr::Cast(right_inner, right_type))
		| (Expr::TryCast(left_inner, left_type), Expr::TryCast(right_inner, right_type)) => {
			expr_structural_eq(left_inner, right_inner)
				&& data_type_structural_eq(left_type, right_type)
		}
		(Expr::Add(left_left, left_right), Expr::Add(right_left, right_right))
		| (Expr::Sub(left_left, left_right), Expr::Sub(right_left, right_right))
		| (Expr::Mul(left_left, left_right), Expr::Mul(right_left, right_right))
		| (Expr::Div(left_left, left_right), Expr::Div(right_left, right_right))
		| (Expr::Eq(left_left, left_right), Expr::Eq(right_left, right_right))
		| (Expr::Neq(left_left, left_right), Expr::Neq(right_left, right_right))
		| (Expr::Lt(left_left, left_right), Expr::Lt(right_left, right_right))
		| (Expr::Lte(left_left, left_right), Expr::Lte(right_left, right_right))
		| (Expr::Gt(left_left, left_right), Expr::Gt(right_left, right_right))
		| (Expr::Gte(left_left, left_right), Expr::Gte(right_left, right_right))
		| (Expr::And(left_left, left_right), Expr::And(right_left, right_right))
		| (Expr::Or(left_left, left_right), Expr::Or(right_left, right_right)) => {
			expr_structural_eq(left_left, right_left) && expr_structural_eq(left_right, right_right)
		}
		(Expr::Not(left), Expr::Not(right))
		| (Expr::IsNull(left), Expr::IsNull(right))
		| (Expr::IsNotNull(left), Expr::IsNotNull(right)) => expr_structural_eq(left, right),
		(Expr::TimeBucket(left_interval, left), Expr::TimeBucket(right_interval, right)) => {
			left_interval == right_interval && expr_structural_eq(left, right)
		}
		_ => false,
	}
}

fn hash_scalar_structural(value: &ScalarValue, state: &mut impl Hasher) {
	match value {
		ScalarValue::Boolean(value) => {
			0u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Int32(value) => {
			1u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Int64(value) => {
			2u8.hash(state);
			value.hash(state);
		}
		ScalarValue::Float32(value) => {
			3u8.hash(state);
			value.map(f32::to_bits).hash(state);
		}
		ScalarValue::Float64(value) => {
			4u8.hash(state);
			value.map(f64::to_bits).hash(state);
		}
		ScalarValue::Utf8(value) => {
			5u8.hash(state);
			value.hash(state);
		}
	}
}

fn scalar_structural_eq(left: &ScalarValue, right: &ScalarValue) -> bool {
	match (left, right) {
		(ScalarValue::Boolean(left), ScalarValue::Boolean(right)) => left == right,
		(ScalarValue::Int32(left), ScalarValue::Int32(right)) => left == right,
		(ScalarValue::Int64(left), ScalarValue::Int64(right)) => left == right,
		(ScalarValue::Float32(left), ScalarValue::Float32(right)) => {
			left.map(f32::to_bits) == right.map(f32::to_bits)
		}
		(ScalarValue::Float64(left), ScalarValue::Float64(right)) => {
			left.map(f64::to_bits) == right.map(f64::to_bits)
		}
		(ScalarValue::Utf8(left), ScalarValue::Utf8(right)) => left == right,
		_ => false,
	}
}

fn hash_data_type_structural(data_type: &DataType, state: &mut impl Hasher) {
	match data_type {
		DataType::Int32 => 0u8.hash(state),
		DataType::Int64 => 1u8.hash(state),
		DataType::Float32 => 2u8.hash(state),
		DataType::Float64 => 3u8.hash(state),
		other => {
			255u8.hash(state);
			format!("{other:?}").hash(state);
		}
	}
}

fn data_type_structural_eq(left: &DataType, right: &DataType) -> bool {
	left == right
}

#[derive(Debug, Clone, PartialEq, Eq)]
enum ResolveOutcome {
	Found(String),
	NotInSelect,
	Ambiguous,
}

#[derive(Debug, Clone, Copy)]
enum AggregateSource {
	Select,
	Having,
}

#[derive(Debug, Clone, Copy)]
enum AggregateSqlMode {
	Global,
	Grouped,
}

fn lower_group_by_keys(group_by: &GroupByExpr) -> Result<Vec<GroupKey>, FrontendError> {
	// P0.1: sqlparser 0.54 stores plain keys in Expressions and ClickHouse-style modifiers
	// beside them; GROUP BY ALL and every modifier are outside ADR 011.
	match group_by {
		GroupByExpr::Expressions(exprs, modifiers) => {
			if !modifiers.is_empty() {
				return Err(unsupported("GROUP BY modifiers are not supported"));
			}
			if exprs.is_empty() {
				return Err(unsupported("GROUP BY requires at least one key"));
			}
			exprs.iter().map(lower_group_by_key).collect()
		}
		GroupByExpr::All(_) => Err(unsupported("GROUP BY ALL is not supported")),
	}
}

fn lower_group_by_key(expr: &SqlExpr) -> Result<GroupKey, FrontendError> {
	match expr {
		SqlExpr::Identifier(ident) => {
			let name = ident.value.clone();
			Ok(GroupKey {
				name: name.clone(),
				expr: Expr::column(name),
			})
		}
		_ => Err(unsupported(
			"GROUP BY keys must be simple unqualified columns",
		)),
	}
}

fn reject_qualified_having_column_references(expr: &SqlExpr) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::CompoundIdentifier(_) => Err(unsupported(
			"qualified HAVING column references are not supported",
		)),
		SqlExpr::BinaryOp { left, right, .. } => {
			reject_qualified_having_column_references(left)?;
			reject_qualified_having_column_references(right)
		}
		SqlExpr::Cast { expr, .. } => reject_qualified_having_column_references(expr),
		SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) | SqlExpr::Nested(inner) => {
			reject_qualified_having_column_references(inner)
		}
		SqlExpr::Function(function) => {
			if let FunctionArguments::List(list) = &function.args {
				for arg in &list.args {
					if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
						reject_qualified_having_column_references(expr)?;
					}
				}
			}
			Ok(())
		}
		_ => Ok(()),
	}
}

fn reject_having_alias_references(
	expr: &SqlExpr,
	select_aliases: &HashSet<String>,
) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(ident) if select_aliases.contains(&ident.value) => {
			Err(unsupported(having_alias_message()))
		}
		SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => Ok(()),
		SqlExpr::BinaryOp { left, right, .. } => {
			reject_having_alias_references(left, select_aliases)?;
			reject_having_alias_references(right, select_aliases)
		}
		SqlExpr::Cast { expr, .. } => reject_having_alias_references(expr, select_aliases),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => reject_having_alias_references(expr, select_aliases),
		_ => Ok(()),
	}
}

fn reject_qualified_aggregate_projection_references(expr: &SqlExpr) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::CompoundIdentifier(_) => Err(unsupported(
			"qualified aggregate projection references are not supported",
		)),
		SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => {
			if let FunctionArguments::List(list) = &function.args {
				for arg in &list.args {
					if let FunctionArg::Unnamed(FunctionArgExpr::Expr(expr)) = arg {
						reject_qualified_aggregate_projection_references(expr)?;
					}
				}
			}
			Ok(())
		}
		SqlExpr::BinaryOp { left, right, .. } => {
			reject_qualified_aggregate_projection_references(left)?;
			reject_qualified_aggregate_projection_references(right)
		}
		SqlExpr::Cast { expr, .. } => reject_qualified_aggregate_projection_references(expr),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => reject_qualified_aggregate_projection_references(expr),
		_ => Ok(()),
	}
}

fn having_alias_message() -> &'static str {
	"HAVING does not resolve SELECT aliases; spell the aggregate explicitly"
}

fn aggregate_call_not_in_select_message(clause: &str) -> &'static str {
	match clause {
		"HAVING" => "HAVING aggregate-call must appear in SELECT list",
		"ORDER BY" => {
			"ORDER BY <aggregate-call> requires the aggregate to appear in the SELECT list"
		}
		other => unreachable!("unsupported grouped aggregate clause: {other}"),
	}
}

fn ambiguous_aggregate_call_message(clause: &str) -> &'static str {
	match clause {
		"HAVING" => "ambiguous HAVING aggregate-call",
		"ORDER BY" => "ambiguous ORDER BY aggregate-call",
		other => unreachable!("unsupported grouped aggregate clause: {other}"),
	}
}

fn select_item_expr_and_alias(
	item: &SelectItem,
) -> Result<(&SqlExpr, Option<&Ident>), FrontendError> {
	match item {
		SelectItem::UnnamedExpr(expr) => Ok((expr, None)),
		SelectItem::ExprWithAlias { expr, alias } => Ok((expr, Some(alias))),
		SelectItem::Wildcard(_) => Err(unsupported("mixed wildcard projections are not supported")),
		SelectItem::QualifiedWildcard(_, _) => Err(unsupported(
			"qualified wildcard projections are not supported",
		)),
	}
}

fn select_expr_is_computed(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => false,
		SqlExpr::Function(function) => aggregate_function_name(&function.name).is_none(),
		_ => true,
	}
}

fn logical_expr_output_name(expr: &Expr, idx: usize) -> String {
	match expr {
		Expr::Column(column) => column.name.0.clone(),
		_ => format!("col_{idx}"),
	}
}

fn projection_contains_aggregate_call(projection: &[SelectItem]) -> bool {
	projection.iter().any(select_item_contains_aggregate_call)
}

fn select_item_contains_aggregate_call(item: &SelectItem) -> bool {
	match item {
		SelectItem::UnnamedExpr(expr) => expr_contains_aggregate_call(expr),
		SelectItem::ExprWithAlias { expr, alias: _ } => expr_contains_aggregate_call(expr),
		SelectItem::QualifiedWildcard(_, _) | SelectItem::Wildcard(_) => false,
	}
}

fn expr_contains_aggregate_call(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Function(function) => aggregate_function_name(&function.name).is_some(),
		SqlExpr::BinaryOp { left, right, .. } => {
			expr_contains_aggregate_call(left) || expr_contains_aggregate_call(right)
		}
		SqlExpr::Cast { expr, .. } => expr_contains_aggregate_call(expr),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => expr_contains_aggregate_call(expr),
		_ => false,
	}
}

fn reject_global_aggregate_projection_columns(
	projection: &[SelectItem],
) -> Result<(), FrontendError> {
	for item in projection {
		let (expr, _) = select_item_expr_and_alias(item)?;
		if expr_contains_nonaggregate_column_reference(expr) {
			return Err(unsupported("unsupported aggregate projection item"));
		}
	}
	Ok(())
}

fn expr_contains_nonaggregate_column_reference(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => true,
		SqlExpr::Function(function) if aggregate_function_name(&function.name).is_some() => false,
		SqlExpr::Function(function) => {
			function_args_contain_nonaggregate_column_reference(function)
		}
		SqlExpr::BinaryOp { left, right, .. } => {
			expr_contains_nonaggregate_column_reference(left)
				|| expr_contains_nonaggregate_column_reference(right)
		}
		SqlExpr::Cast { expr, .. } => expr_contains_nonaggregate_column_reference(expr),
		SqlExpr::UnaryOp { expr, .. }
		| SqlExpr::IsNull(expr)
		| SqlExpr::IsNotNull(expr)
		| SqlExpr::Nested(expr) => expr_contains_nonaggregate_column_reference(expr),
		_ => false,
	}
}

fn function_args_contain_nonaggregate_column_reference(function: &Function) -> bool {
	let FunctionArguments::List(list) = &function.args else {
		return false;
	};
	list.args.iter().any(|arg| match arg {
		FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
		| FunctionArg::Named {
			arg: FunctionArgExpr::Expr(expr),
			..
		}
		| FunctionArg::ExprNamed {
			arg: FunctionArgExpr::Expr(expr),
			..
		} => expr_contains_nonaggregate_column_reference(expr),
		_ => false,
	})
}

fn aggregate_projection_matches_natural(
	projection: &[SelectItem],
	mode: AggregateSqlMode,
	catalog: &AggregateOutputCatalog,
	planner: &SqlPlanner,
) -> Result<bool, FrontendError> {
	if projection.len() != catalog.group_keys.len() + catalog.entries.len() {
		return Ok(false);
	}

	for item in projection {
		if matches!(item, SelectItem::ExprWithAlias { .. }) {
			return Ok(false);
		}
	}

	for (item, key) in projection.iter().zip(catalog.group_keys.iter()) {
		let SelectItem::UnnamedExpr(expr) = item else {
			return Ok(false);
		};
		let SqlExpr::Identifier(ident) = strip_nested_sql_expr(expr) else {
			return Ok(false);
		};
		if ident.value != key.name {
			return Ok(false);
		}
	}

	for (item, entry) in projection
		.iter()
		.skip(catalog.group_keys.len())
		.zip(&catalog.entries)
	{
		let SelectItem::UnnamedExpr(expr) = item else {
			return Ok(false);
		};
		let SqlExpr::Function(function) = strip_nested_sql_expr(expr) else {
			return Ok(false);
		};
		if aggregate_function_name(&function.name).is_none() {
			return Ok(false);
		}
		let (function, args) = planner.lower_aggregate_function(function, mode)?;
		let signature = grouped_aggregate_signature(&function, &args);
		if entry.signature.is_some() && signature != entry.signature {
			return Ok(false);
		}
		if entry.signature.is_none() && signature.is_some() {
			return Ok(false);
		}
	}

	Ok(true)
}

fn aggregate_function_name(name: &ObjectName) -> Option<&'static str> {
	let [ident] = name.0.as_slice() else {
		return None;
	};
	let upper = ident.value.to_ascii_uppercase();
	if aggregate_kernel::is_stable_aggregate_sql_name(&upper) {
		return aggregate_kernel::stable_aggregate_sql_name(&upper);
	}
	aggregate_kernel::experimental_aggregate_sql_name(&upper)
}

fn aggregate_output_name(function: &AggregateFunction) -> &'static str {
	if let Some(spec) = aggregate_kernel::stable_spec_for_function(function) {
		return spec.output_name;
	}
	match function {
		AggregateFunction::LinearRegress => "linreg",
		AggregateFunction::ApproxQuantile(_) => "quantile",
		AggregateFunction::CountAll
		| AggregateFunction::Count
		| AggregateFunction::Sum
		| AggregateFunction::Min
		| AggregateFunction::Max
		| AggregateFunction::Avg => unreachable!("stable aggregate metadata missing"),
	}
}

fn grouped_aggregate_signature(
	function: &AggregateFunction,
	args: &[Expr],
) -> Option<GroupedAggregateSignature> {
	let kernel = aggregate_kernel::stable_kernel_for_function(function)?;
	let spec = aggregate_kernel::stable_spec(kernel);
	if !spec.grouped_supported {
		return None;
	}
	match spec.arity {
		aggregate_kernel::AggregateArity::Zero => Some(GroupedAggregateSignature {
			kernel,
			args: Vec::new(),
		}),
		aggregate_kernel::AggregateArity::One if args.len() == 1 => {
			Some(GroupedAggregateSignature {
				kernel,
				args: vec![args[0].clone()],
			})
		}
		_ => None,
	}
}

fn aggregate_function_args(function: &Function) -> Result<&[FunctionArg], FrontendError> {
	// P0.4: global experimental aggregate calls still lower to the existing IR; the binder's
	// allow_experimental_kernels gate remains the authority for executing those kernels.
	if function.uses_odbc_syntax
		|| !matches!(function.parameters, FunctionArguments::None)
		|| function.filter.is_some()
		|| function.null_treatment.is_some()
		|| function.over.is_some()
		|| !function.within_group.is_empty()
	{
		return Err(unsupported(
			"aggregate function modifiers are not supported",
		));
	}

	let FunctionArguments::List(FunctionArgumentList {
		duplicate_treatment,
		args,
		clauses,
	}) = &function.args
	else {
		return Err(unsupported("aggregate function requires arguments"));
	};
	if duplicate_treatment.is_some() || !clauses.is_empty() {
		return Err(unsupported(
			"aggregate function modifiers are not supported",
		));
	}
	Ok(args)
}

fn plan_contains_join(plan: &LogicalPlan) -> bool {
	// P0.6: a recursive logical-plan walk is sufficient because ADR 009 decorrelation is
	// the only path that can add a Join under syntactically single-table grouped SQL.
	match plan {
		LogicalPlan::Scan {
			table: _,
			projection: _,
		}
		| LogicalPlan::ScanNamed {
			table: _,
			projection: _,
		} => false,
		LogicalPlan::Alias { input, alias: _ }
		| LogicalPlan::Filter {
			input,
			predicate: _,
		}
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate {
			input,
			group_expr: _,
			aggr_expr: _,
		}
		| LogicalPlan::Exchange {
			input,
			partition_count: _,
			key: _,
		}
		| LogicalPlan::Sort {
			input,
			order_by: _,
			limit: _,
			offset: _,
		} => plan_contains_join(input),
		LogicalPlan::Join {
			left: _,
			right: _,
			kind: _,
			on: _,
			strategy: _,
		} => true,
		LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateTableAsSelect { .. }
		| LogicalPlan::Insert { .. }
		| LogicalPlan::InsertSelect { .. } => false,
	}
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum JoinSide {
	Left,
	Right,
}

#[derive(Debug)]
enum ExistsConjunct {
	CorrelatedPair { outer: Expr, inner: Expr },
	InnerResidual(Expr),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum ColumnScope {
	Outer,
	Inner,
}

#[derive(Default)]
struct ScopeRefs {
	outer: bool,
	unsupported: bool,
}

fn split_top_level_and(expr: &SqlExpr) -> Vec<&SqlExpr> {
	let expr = strip_nested_sql_expr(expr);
	match expr {
		SqlExpr::BinaryOp { left, op, right } if *op == BinaryOperator::And => {
			let mut conjuncts = split_top_level_and(left);
			conjuncts.extend(split_top_level_and(right));
			conjuncts
		}
		_ => vec![expr],
	}
}

fn combine_and_exprs(exprs: Vec<Expr>) -> Option<Expr> {
	let mut exprs = exprs.into_iter();
	let first = exprs.next()?;
	Some(exprs.fold(first, |left, right| {
		Expr::And(Box::new(left), Box::new(right))
	}))
}

fn validate_exists_query_shape(query: &Query) -> Result<(), FrontendError> {
	if query.with.is_some()
		|| query.order_by.is_some()
		|| query.limit.is_some()
		|| !query.limit_by.is_empty()
		|| query.offset.is_some()
		|| query.fetch.is_some()
		|| !query.locks.is_empty()
		|| query.for_clause.is_some()
		|| query.settings.is_some()
		|| query.format_clause.is_some()
	{
		return Err(unsupported("unsupported EXISTS subquery shape"));
	}
	Ok(())
}

fn validate_exists_select_shape(select: &Select) -> Result<(), FrontendError> {
	if select.distinct.is_some()
		|| select.top.is_some()
		|| select.into.is_some()
		|| select.from.len() != 1
		|| !select.from[0].joins.is_empty()
		|| !select.lateral_views.is_empty()
		|| select.prewhere.is_some()
		|| !group_by_is_empty(&select.group_by)
		|| !select.cluster_by.is_empty()
		|| !select.distribute_by.is_empty()
		|| !select.sort_by.is_empty()
		|| select.having.is_some()
		|| !select.named_window.is_empty()
		|| select.qualify.is_some()
		|| select.value_table_mode.is_some()
		|| select.connect_by.is_some()
	{
		return Err(unsupported("unsupported EXISTS subquery shape"));
	}
	Ok(())
}

fn validate_exists_projection(projection: &[SelectItem]) -> Result<(), FrontendError> {
	for item in projection {
		match item {
			SelectItem::Wildcard(_) => {}
			SelectItem::UnnamedExpr(expr) if exists_projection_expr_supported(expr) => {}
			_ => return Err(unsupported("unsupported EXISTS subquery projection")),
		}
	}
	Ok(())
}

fn exists_projection_expr_supported(expr: &SqlExpr) -> bool {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_)
		| SqlExpr::CompoundIdentifier(_)
		| SqlExpr::Value(Value::Number(_, _))
		| SqlExpr::Value(Value::SingleQuotedString(_))
		| SqlExpr::Value(Value::Boolean(_)) => true,
		_ => false,
	}
}

fn group_by_is_empty(group_by: &GroupByExpr) -> bool {
	match group_by {
		GroupByExpr::Expressions(exprs, modifiers) => exprs.is_empty() && modifiers.is_empty(),
		GroupByExpr::All(_) => false,
	}
}

fn parse_non_negative_usize_literal(expr: &SqlExpr) -> Result<usize, FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Value(Value::Number(number, _)) => {
			if number.starts_with('-')
				|| number.contains('.')
				|| number.contains('e')
				|| number.contains('E')
			{
				return Err(unsupported("LIMIT/OFFSET expects a non-negative integer"));
			}
			number
				.parse::<usize>()
				.map_err(|_| unsupported("LIMIT/OFFSET expects a non-negative integer"))
		}
		_ => Err(unsupported("LIMIT/OFFSET expects a non-negative integer")),
	}
}

fn strip_nested_sql_expr(expr: &SqlExpr) -> &SqlExpr {
	match expr {
		SqlExpr::Nested(inner) => strip_nested_sql_expr(inner),
		_ => expr,
	}
}

fn scoped_column_expr(
	expr: &SqlExpr,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
) -> Result<Option<(ColumnScope, Expr)>, FrontendError> {
	let Some((qualifier, column)) = sql_column_ref(expr)? else {
		return Ok(None);
	};
	let scope = resolve_column_scope(&qualifier, outer_qualifiers, inner_qualifiers)?;
	Ok(Some((scope, Expr::qualified_column(qualifier, column))))
}

fn sql_column_ref(expr: &SqlExpr) -> Result<Option<(TableRef, String)>, FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::CompoundIdentifier(idents) if idents.len() == 2 => Ok(Some((
			TableRef::new(idents[0].value.clone()),
			idents[1].value.clone(),
		))),
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
			Err(unsupported("unsupported EXISTS column reference"))
		}
		_ => Ok(None),
	}
}

fn resolve_column_scope(
	qualifier: &TableRef,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
) -> Result<ColumnScope, FrontendError> {
	let outer_matches = outer_qualifiers
		.iter()
		.filter(|candidate| *candidate == qualifier)
		.count();
	let inner_matches = inner_qualifiers
		.iter()
		.filter(|candidate| *candidate == qualifier)
		.count();
	match (outer_matches, inner_matches) {
		(1, 0) => Ok(ColumnScope::Outer),
		(0, 1) => Ok(ColumnScope::Inner),
		_ => Err(unsupported("unsupported EXISTS column scope")),
	}
}

fn collect_scope_refs(
	expr: &SqlExpr,
	outer_qualifiers: &[TableRef],
	inner_qualifiers: &[TableRef],
	refs: &mut ScopeRefs,
) -> Result<(), FrontendError> {
	match strip_nested_sql_expr(expr) {
		SqlExpr::Identifier(_) | SqlExpr::CompoundIdentifier(_) => {
			let Some((qualifier, _)) = sql_column_ref(expr)? else {
				return Ok(());
			};
			match resolve_column_scope(&qualifier, outer_qualifiers, inner_qualifiers) {
				Ok(ColumnScope::Outer) => refs.outer = true,
				Ok(ColumnScope::Inner) => {}
				Err(_) => refs.unsupported = true,
			}
		}
		SqlExpr::Exists { .. } | SqlExpr::Subquery(_) => refs.unsupported = true,
		SqlExpr::BinaryOp { left, right, .. } => {
			collect_scope_refs(left, outer_qualifiers, inner_qualifiers, refs)?;
			collect_scope_refs(right, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::Cast { expr, .. } => {
			collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::IsNull(inner) | SqlExpr::IsNotNull(inner) => {
			collect_scope_refs(inner, outer_qualifiers, inner_qualifiers, refs)?;
		}
		SqlExpr::Function(function) => match &function.args {
			sqlparser::ast::FunctionArguments::List(list) => {
				for arg in &list.args {
					match arg {
						FunctionArg::Unnamed(FunctionArgExpr::Expr(expr))
						| FunctionArg::Named {
							arg: FunctionArgExpr::Expr(expr),
							..
						}
						| FunctionArg::ExprNamed {
							arg: FunctionArgExpr::Expr(expr),
							..
						} => {
							collect_scope_refs(expr, outer_qualifiers, inner_qualifiers, refs)?;
						}
						FunctionArg::Unnamed(FunctionArgExpr::Wildcard) => {}
						_ => refs.unsupported = true,
					}
				}
			}
			sqlparser::ast::FunctionArguments::Subquery(_) => refs.unsupported = true,
			_ => {}
		},
		SqlExpr::Value(_) => {}
		_ => {}
	}
	Ok(())
}

fn alias_plan(plan: LogicalPlan, alias: Option<&TableAlias>) -> LogicalPlan {
	match alias {
		Some(alias) => LogicalPlan::Alias {
			input: Box::new(plan),
			alias: TableRef::new(alias.name.value.clone()),
		},
		None => plan,
	}
}

fn plan_qualifiers(plan: &LogicalPlan) -> Vec<TableRef> {
	match plan {
		LogicalPlan::ScanNamed { table, .. } => vec![table.clone()],
		LogicalPlan::Alias { alias, .. } => vec![alias.clone()],
		LogicalPlan::Filter { input, .. }
		| LogicalPlan::Project { input, .. }
		| LogicalPlan::Aggregate { input, .. }
		| LogicalPlan::Exchange { input, .. }
		| LogicalPlan::Sort { input, .. } => plan_qualifiers(input),
		LogicalPlan::Join {
			left, right, kind, ..
		} => match kind {
			JoinKind::LeftSemi | JoinKind::LeftAnti => plan_qualifiers(left),
			JoinKind::RightSemi | JoinKind::RightAnti => plan_qualifiers(right),
			JoinKind::Inner | JoinKind::LeftOuter | JoinKind::RightOuter | JoinKind::FullOuter => {
				let mut qualifiers = plan_qualifiers(left);
				qualifiers.extend(plan_qualifiers(right));
				qualifiers
			}
		},
		LogicalPlan::Scan { .. }
		| LogicalPlan::CreateTable { .. }
		| LogicalPlan::CreateTableAsSelect { .. }
		| LogicalPlan::Insert { .. }
		| LogicalPlan::InsertSelect { .. } => Vec::new(),
	}
}

fn expr_side(expr: &Expr, left: &[TableRef], right: &[TableRef]) -> Option<JoinSide> {
	let Expr::Column(column_ref) = expr else {
		return None;
	};
	let qualifier = column_ref.qualifier.as_ref()?;
	if left.iter().any(|left_ref| left_ref == qualifier) {
		Some(JoinSide::Left)
	} else if right.iter().any(|right_ref| right_ref == qualifier) {
		Some(JoinSide::Right)
	} else {
		None
	}
}

fn table_ref_from_object_name(name: &ObjectName) -> TableRef {
	let parts = object_name_parts(name);
	match parts.as_slice() {
		[schema, table] => TableRef::with_schema(schema.clone(), table.clone()),
		[table] => TableRef::new(table.clone()),
		_ => TableRef::new(name.to_string()),
	}
}

fn object_name_parts(name: &ObjectName) -> Vec<String> {
	name.to_string()
		.split('.')
		.map(|part| part.trim_matches('"').to_string())
		.collect()
}

#[cfg(test)]
mod tests {
	use super::*;

	#[test]
	fn insert_table_function_target_rejects_after_parse() {
		let dialect = sqlparser::dialect::ClickHouseDialect {};
		let ast = Parser::parse_sql(&dialect, "INSERT INTO FUNCTION f() VALUES (1)")
			.expect("parse table-function insert");
		let Statement::Insert(insert) = &ast[0] else {
			panic!("expected insert");
		};

		let err = SqlPlanner::new()
			.lower_insert(insert)
			.expect_err("table function target should reject");

		assert!(
			err.to_string()
				.contains("table functions are not supported")
		);
	}

	#[test]
	fn classifier_recognizes_correlated_equi_pair() {
		let planner = SqlPlanner::new();
		let classified = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE dim.k = fact.k"),
				&[TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect("classify");

		match classified {
			ExistsConjunct::CorrelatedPair { outer, inner } => {
				assert_qualified_column(&outer, "fact", "k");
				assert_qualified_column(&inner, "dim", "k");
			}
			ExistsConjunct::InnerResidual(_) => panic!("expected correlated pair"),
		}
	}

	#[test]
	fn classifier_recognizes_inner_only_residual_including_literals() {
		let planner = SqlPlanner::new();
		for sql in [
			"SELECT * FROM fact WHERE dim.active = true",
			"SELECT * FROM fact WHERE dim.score > 10",
		] {
			let classified = planner
				.classify_exists_conjunct(
					&parse_selection(sql),
					&[TableRef::new("fact")],
					&[TableRef::new("dim")],
				)
				.expect("classify residual");

			assert!(
				matches!(classified, ExistsConjunct::InnerResidual(_)),
				"{sql}"
			);
		}
	}

	#[test]
	fn classifier_rejects_outer_only_subquery_predicate() {
		assert_classifier_rejects("SELECT * FROM fact WHERE fact.flag = true");
	}

	#[test]
	fn classifier_rejects_non_equi_and_computed_correlation() {
		assert_classifier_rejects("SELECT * FROM fact WHERE dim.k > fact.k");
		assert_classifier_rejects("SELECT * FROM fact WHERE dim.k + 1 = fact.k");
	}

	#[test]
	fn classifier_rejects_unqualified_ambiguous_correlation() {
		assert_classifier_rejects("SELECT * FROM fact WHERE k = fact.k");
		let planner = SqlPlanner::new();
		let err = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE t.k = t.k"),
				&[TableRef::new("t")],
				&[TableRef::new("t")],
			)
			.expect_err("same-table unaliased self-correlation rejects");
		assert!(err.to_string().contains("unsupported"));

		let err = planner
			.classify_exists_conjunct(
				&parse_selection("SELECT * FROM fact WHERE dim.k = fact.k"),
				&[TableRef::new("fact"), TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect_err("duplicate outer qualifier rejects");
		assert!(err.to_string().contains("unsupported"));
	}

	#[test]
	fn sql_inner_join_emits_explicit_hash_exchanges() {
		let plan = SqlPlanner::new()
			.plan("SELECT * FROM orders INNER JOIN customers ON orders.customer_id = customers.id")
			.expect("plan join");

		match plan {
			LogicalPlan::Join {
				left,
				right,
				kind,
				strategy,
				on,
			} => {
				assert_eq!(kind, JoinKind::Inner);
				assert_eq!(strategy, JoinStrategy::HashPartitioned);
				assert_eq!(on.len(), 1);
				assert!(matches!(
					*left,
					LogicalPlan::Exchange {
						partition_count: DEFAULT_JOIN_PARTITION_COUNT,
						..
					}
				));
				assert!(matches!(
					*right,
					LogicalPlan::Exchange {
						partition_count: DEFAULT_JOIN_PARTITION_COUNT,
						..
					}
				));
			}
			other => panic!("expected join plan, got {other:?}"),
		}
	}

	fn assert_classifier_rejects(sql: &str) {
		let planner = SqlPlanner::new();
		let err = planner
			.classify_exists_conjunct(
				&parse_selection(sql),
				&[TableRef::new("fact")],
				&[TableRef::new("dim")],
			)
			.expect_err("classifier should reject");
		assert!(err.to_string().contains("unsupported"), "{sql}: {err}");
	}

	fn parse_selection(sql: &str) -> SqlExpr {
		let dialect = GenericDialect {};
		let ast = Parser::parse_sql(&dialect, sql).expect("parse sql");
		let Statement::Query(query) = ast.into_iter().next().expect("statement") else {
			panic!("expected query");
		};
		let SetExpr::Select(select) = *query.body else {
			panic!("expected select");
		};
		select.selection.expect("selection")
	}

	fn assert_qualified_column(expr: &Expr, qualifier: &str, name: &str) {
		let Expr::Column(column) = expr else {
			panic!("expected column, got {expr:?}");
		};
		assert_eq!(column.qualifier.as_ref(), Some(&TableRef::new(qualifier)));
		assert_eq!(column.name.0, name);
	}
}
