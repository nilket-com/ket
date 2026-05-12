use rune::{Any, Module, Vm, Context, Sources, Source, Diagnostics};
use crate::ir::{LogicalPlan, Expr, ScalarValue, TableId, AggregateFunction};
use std::sync::Arc;

#[derive(Any, Debug, Clone)]
pub struct RuneDataFrame {
    pub(crate) plan: LogicalPlan,
}

#[derive(Any, Debug, Clone)]
pub struct RuneExpr {
    pub(crate) expr: Expr,
}

impl RuneDataFrame {
    pub fn new(table: u64) -> Self {
        Self {
            plan: LogicalPlan::Scan {
                table: TableId(table),
                projection: None,
            },
        }
    }

    pub fn filter(self, predicate: RuneExpr) -> Self {
        Self {
            plan: LogicalPlan::Filter {
                input: Box::new(self.plan),
                predicate: predicate.expr,
            },
        }
    }

    pub fn project(self, exprs: Vec<RuneExpr>) -> Self {
        Self {
            plan: LogicalPlan::Project {
                input: Box::new(self.plan),
                exprs: exprs.into_iter().map(|e| e.expr).collect(),
            },
        }
    }

    pub fn count(self) -> Self {
        Self {
            plan: LogicalPlan::Aggregate {
                input: Box::new(self.plan),
                group_expr: vec![],
                aggr_expr: vec![(AggregateFunction::CountAll, Expr::Column("count".to_string()))],
            },
        }
    }
}

#[derive(Any, Debug, Clone)]
pub struct ToronContext {
}

impl ToronContext {
    pub fn table(&self, name: String) -> RuneDataFrame {
        let id = match name.as_str() {
            "sales" => 1,
            "events" => 2,
            _ => 0,
        };
        RuneDataFrame::new(id)
    }
}

pub fn col(name: String) -> RuneExpr {
    RuneExpr { expr: Expr::Column(name) }
}

pub fn lit(val: i32) -> RuneExpr {
    RuneExpr { expr: Expr::Literal(ScalarValue::Int32(Some(val))) }
}

impl RuneExpr {
    pub fn gt(self, other: RuneExpr) -> RuneExpr {
        RuneExpr { expr: Expr::Gt(Box::new(self.expr), Box::new(other.expr)) }
    }

    pub fn eq(self, other: RuneExpr) -> RuneExpr {
        RuneExpr { expr: Expr::Eq(Box::new(self.expr), Box::new(other.expr)) }
    }
}

pub fn create_toron_module() -> Result<Module, rune::compile::ContextError> {
    let mut m = Module::new();
    m.ty::<RuneDataFrame>()?;
    m.ty::<RuneExpr>()?;
    m.ty::<ToronContext>()?;
    
    m.function(["col"], col).build()?;
    m.function(["lit"], lit).build()?;
    
    m.associated_function("table", ToronContext::table)?;
    m.associated_function("filter", RuneDataFrame::filter)?;
    m.associated_function("project", RuneDataFrame::project)?;
    m.associated_function("count", RuneDataFrame::count)?;
    
    // Expression methods
    m.associated_function("gt", RuneExpr::gt)?;
    m.associated_function("eq", RuneExpr::eq)?;
    
    Ok(m)
}

pub struct RuneEngine {
    context: Arc<Context>,
    runtime: Arc<rune::runtime::RuntimeContext>,
}

impl RuneEngine {
    pub fn new() -> Result<Self, Box<dyn std::error::Error>> {
        let mut context = Context::with_default_modules()?;
        context.install(create_toron_module()?)?;
        
        let runtime = Arc::new(context.runtime()?);
        Ok(Self {
            context: Arc::new(context),
            runtime,
        })
    }

    pub async fn execute_script(&self, script: &str, _fuel: u64) -> Result<LogicalPlan, String> {
        let mut sources = Sources::new();
        sources.insert(Source::new("script", script).map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;
        
        let mut diagnostics = Diagnostics::new();
        
        let unit = rune::prepare(&mut sources)
            .with_context(&self.context)
            .with_diagnostics(&mut diagnostics)
            .build();
            
        if !diagnostics.is_empty() {
            return Err(format!("Compilation failed: {:?}", diagnostics));
        }
        
        let unit = unit.map_err(|e| e.to_string())?;
        
        let mut vm = Vm::new(self.runtime.clone(), Arc::new(unit));
        // vm.set_fuel(Some(_fuel));
        
        let mut execution = vm.execute(["main"], (ToronContext {},))
            .map_err(|e| e.to_string())?;
            
        let output = execution.async_complete()
            .await
            .into_result()
            .map_err(|e| e.to_string())?;
            
        let df: RuneDataFrame = rune::from_value(output).map_err(|e| e.to_string())?;
        Ok(df.plan)
    }
}
