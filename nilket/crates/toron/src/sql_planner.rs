use sqlparser::dialect::GenericDialect;
use sqlparser::parser::Parser;
use sqlparser::ast::{Statement, Query, SetExpr, Select, SelectItem, TableFactor, Expr as SqlExpr, BinaryOperator, Value, FunctionArg};
use crate::ir::{LogicalPlan, Expr, ScalarValue, TableId, AggregateFunction};

pub struct SqlPlanner;

impl SqlPlanner {
    pub fn new() -> Self {
        Self
    }

    pub fn plan(&self, sql: &str) -> Result<LogicalPlan, String> {
        let dialect = GenericDialect {};
        let ast = Parser::parse_sql(&dialect, sql).map_err(|e| e.to_string())?;

        if ast.is_empty() {
            return Err("Empty SQL statement".to_string());
        }

        match &ast[0] {
            Statement::Query(query) => self.lower_query(query),
            _ => Err("Unsupported SQL statement; only SELECT is supported in V0".to_string()),
        }
    }

    fn lower_query(&self, query: &Query) -> Result<LogicalPlan, String> {
        match &*query.body {
            SetExpr::Select(select) => self.lower_select(select),
            _ => Err("Unsupported query body".to_string()),
        }
    }

    fn lower_select(&self, select: &Select) -> Result<LogicalPlan, String> {
        // 1. Handle FROM (Scan)
        if select.from.is_empty() {
            return Err("SELECT without FROM is not supported".to_string());
        }
        
        let table_with_joins = &select.from[0];
        let table_name = match &table_with_joins.relation {
            TableFactor::Table { name, .. } => name.to_string(),
            _ => return Err("Unsupported table factor".to_string()),
        };

        // Mock table mapping for V0
        let table_id = match table_name.as_str() {
            "sales" => TableId(1),
            "events" => TableId(2),
            _ => TableId(0),
        };

        let mut plan = LogicalPlan::Scan {
            table: table_id,
            projection: None,
        };

        // 2. Handle WHERE (Filter)
        if let Some(selection) = &select.selection {
            let predicate = self.lower_expr(selection)?;
            plan = LogicalPlan::Filter {
                input: Box::new(plan),
                predicate,
            };
        }

        // 3. Handle SELECT list (Project or Aggregate)
        let mut aggr_exprs = Vec::new();
        let mut is_aggregate = false;

        for item in &select.projection {
            if let SelectItem::UnnamedExpr(SqlExpr::Function(f)) = item {
                let name = f.name.to_string().to_uppercase();
                let args = match &f.args {
                    sqlparser::ast::FunctionArguments::List(l) => &l.args,
                    _ => &vec![],
                };

                match name.as_str() {
                    "COUNT" => {
                        aggr_exprs.push((AggregateFunction::CountAll, vec![Expr::Column("count".to_string())]));
                        is_aggregate = true;
                    }
                    "SUM" => {
                        let arg = self.lower_arg(&args[0])?;
                        aggr_exprs.push((AggregateFunction::Sum, vec![arg]));
                        is_aggregate = true;
                    }
                    "LINEAR_REGRESS" => {
                        let y = self.lower_arg(&args[0])?;
                        let x = self.lower_arg(&args[1])?;
                        aggr_exprs.push((AggregateFunction::LinearRegress, vec![y, x]));
                        is_aggregate = true;
                    }
                    "APPROX_QUANTILE" => {
                        let val = self.lower_arg(&args[0])?;
                        let q = match &args[1] {
                            FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Value(Value::Number(n, _)))) => {
                                n.parse::<f64>().map_err(|e| e.to_string())?
                            }
                            _ => return Err("APPROX_QUANTILE expects a numeric literal for quantile".to_string()),
                        };
                        aggr_exprs.push((AggregateFunction::ApproxQuantile(q), vec![val]));
                        is_aggregate = true;
                    }
                    _ => {}
                }
            }
        }

        if is_aggregate {
            return Ok(LogicalPlan::Aggregate {
                input: Box::new(plan),
                group_expr: vec![],
                aggr_expr: aggr_exprs,
            });
        }

        let mut exprs = Vec::new();
        for item in &select.projection {
            match item {
                SelectItem::UnnamedExpr(sql_expr) => {
                    exprs.push(self.lower_expr(sql_expr)?);
                }
                SelectItem::Wildcard(_) => {
                    return Ok(plan);
                }
                _ => return Err("Unsupported select item".to_string()),
            }
        }

        Ok(LogicalPlan::Project {
            input: Box::new(plan),
            exprs,
        })
    }

    fn lower_arg(&self, arg: &FunctionArg) -> Result<Expr, String> {
        match arg {
            FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(e)) => self.lower_expr(e),
            _ => Err("Unsupported function argument".to_string()),
        }
    }

    fn lower_expr(&self, sql_expr: &SqlExpr) -> Result<Expr, String> {
        match sql_expr {
            SqlExpr::Identifier(ident) => Ok(Expr::Column(ident.value.clone())),
            SqlExpr::Value(val) => match val {
                Value::Number(n, _) => {
                    if let Ok(i) = n.parse::<i32>() {
                        Ok(Expr::Literal(ScalarValue::Int32(Some(i))))
                    } else if let Ok(f) = n.parse::<f64>() {
                        Ok(Expr::Literal(ScalarValue::Float64(Some(f))))
                    } else {
                        Err(format!("Unsupported number literal: {}", n))
                    }
                }
                Value::SingleQuotedString(s) => Ok(Expr::Literal(ScalarValue::Utf8(Some(s.clone())))),
                Value::Boolean(b) => Ok(Expr::Literal(ScalarValue::Boolean(Some(*b)))),
                Value::Null => Err("Explicit NULL literal not supported in V0".to_string()),
                _ => Err(format!("Unsupported value: {:?}", val)),
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
                    _ => Err(format!("Unsupported operator: {:?}", op)),
                }
            }
            SqlExpr::IsNull(expr) => Ok(Expr::IsNull(Box::new(self.lower_expr(expr)?))),
            SqlExpr::IsNotNull(expr) => Ok(Expr::IsNotNull(Box::new(self.lower_expr(expr)?))),
            SqlExpr::Nested(expr) => self.lower_expr(expr),
            SqlExpr::Function(f) if f.name.to_string().to_uppercase() == "TIME_BUCKET" => {
                let args = match &f.args {
                    sqlparser::ast::FunctionArguments::List(l) => &l.args,
                    _ => return Err("TIME_BUCKET expects arguments".to_string()),
                };
                let ts = self.lower_arg(&args[0])?;
                let interval = match &args[1] {
                    FunctionArg::Unnamed(sqlparser::ast::FunctionArgExpr::Expr(SqlExpr::Value(Value::SingleQuotedString(s)))) => {
                        match s.as_str() {
                            "1m" => std::time::Duration::from_secs(60),
                            "1h" => std::time::Duration::from_secs(3600),
                            "1d" => std::time::Duration::from_secs(86400),
                            _ => return Err(format!("Unsupported time_bucket interval: {}", s)),
                        }
                    }
                    _ => return Err("time_bucket expects a string literal interval".to_string()),
                };
                Ok(Expr::TimeBucket(interval, Box::new(ts)))
            }
            _ => Err(format!("Unsupported expression: {:?}", sql_expr)),
        }
    }
}
