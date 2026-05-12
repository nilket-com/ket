pub mod ir;
pub mod planner;
pub mod exec;
pub mod storage;
pub mod pbc;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use arrow::array::{Int32Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::record_batch::RecordBatch;
    
    use crate::ir::{Expr, ScalarValue, LogicalPlan, AggregateFunction, TableId};
    use crate::storage::InMemoryStorageScan;
    use crate::exec::Executor;

    #[tokio::test]
    async fn test_m1_single_process_ir_execution() {
        // Build a RecordBatch fixture
        let schema = Arc::new(Schema::new(vec![
            Field::new("id", DataType::Int32, false),
            Field::new("amount", DataType::Int32, true),
            Field::new("status", DataType::Utf8, true),
        ]));

        let id_array = Int32Array::from(vec![1, 2, 3, 4, 5]);
        let amount_array = Int32Array::from(vec![Some(50), Some(150), None, Some(200), Some(80)]);
        let status_array = StringArray::from(vec![Some("active"), Some("active"), Some("inactive"), Some("active"), Some("pending")]);

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(id_array),
                Arc::new(amount_array),
                Arc::new(status_array),
            ],
        ).unwrap();

        let storage = InMemoryStorageScan { table_data: batch };
        let executor = Executor::new(&storage);

        // Construct IR manually: SELECT COUNT(*) WHERE amount > 100 AND status = 'active'
        // Scan -> Filter -> Aggregate
        
        let plan = LogicalPlan::Aggregate {
            aggr_expr: AggregateFunction::CountAll,
            input: Box::new(LogicalPlan::Filter {
                predicate: Expr::And(
                    Box::new(Expr::Gt(
                        Box::new(Expr::Column("amount".to_string())),
                        Box::new(Expr::Literal(ScalarValue::Int32(Some(100)))),
                    )),
                    Box::new(Expr::Eq(
                        Box::new(Expr::Column("status".to_string())),
                        Box::new(Expr::Literal(ScalarValue::Utf8(Some("active".to_string())))),
                    )),
                ),
                input: Box::new(LogicalPlan::Scan {
                    table: TableId(1),
                    projection: None,
                }),
            }),
        };

        // Execute
        let results = executor.execute(&plan).await.expect("Execution failed");
        
        // Assert output
        assert_eq!(results.len(), 1);
        let result_batch = &results[0];
        
        // Result should be a single row with 'count'
        assert_eq!(result_batch.num_rows(), 1);
        assert_eq!(result_batch.schema().field(0).name(), "count");
        
        let count_array = result_batch
            .column(0)
            .as_any()
            .downcast_ref::<arrow::array::Int64Array>()
            .unwrap();
            
        // amount > 100 AND status == 'active':
        // row 2: amount 150, status active -> matches
        // row 4: amount 200, status active -> matches
        // So expected count is 2.
        assert_eq!(count_array.value(0), 2);
    }

    #[tokio::test]
    async fn test_m1_null_semantics() {
        // Assert that IsNull and IsNotNull work
        let schema = Arc::new(Schema::new(vec![
            Field::new("amount", DataType::Int32, true),
        ]));

        let amount_array = Int32Array::from(vec![Some(50), None, Some(100)]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(amount_array)]).unwrap();

        let storage = InMemoryStorageScan { table_data: batch };
        let executor = Executor::new(&storage);

        // Filter: IsNull(amount) -> should yield 1 row
        let plan_null = LogicalPlan::Aggregate {
            aggr_expr: AggregateFunction::CountAll,
            input: Box::new(LogicalPlan::Filter {
                predicate: Expr::IsNull(Box::new(Expr::Column("amount".to_string()))),
                input: Box::new(LogicalPlan::Scan { table: TableId(1), projection: None }),
            }),
        };

        let results_null = executor.execute(&plan_null).await.unwrap();
        let count_null = results_null[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0);
        assert_eq!(count_null, 1);

        // Filter: IsNotNull(amount) -> should yield 2 rows
        let plan_not_null = LogicalPlan::Aggregate {
            aggr_expr: AggregateFunction::CountAll,
            input: Box::new(LogicalPlan::Filter {
                predicate: Expr::IsNotNull(Box::new(Expr::Column("amount".to_string()))),
                input: Box::new(LogicalPlan::Scan { table: TableId(1), projection: None }),
            }),
        };

        let results_not_null = executor.execute(&plan_not_null).await.unwrap();
        let count_not_null = results_not_null[0].column(0).as_any().downcast_ref::<arrow::array::Int64Array>().unwrap().value(0);
        assert_eq!(count_not_null, 2);
    }
}
