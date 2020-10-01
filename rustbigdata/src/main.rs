
use std::boxed::Box;
use std::sync::Arc;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::{Expr, ScalarValue};

use arrow::array::{Float64Array, Int32Array, StringArray};

fn main() {
    // // create local execution context
    let mut ctx = ExecutionContext::new();

    // // define schema for data source (csv file)
    let schema = Arc::new(Schema::new(vec![
        Field::new("c1", DataType::Utf8, false),
        Field::new("c2", DataType::UInt32, false),
        Field::new("c3", DataType::Int8, false),
        Field::new("c4", DataType::Int16, false),
        Field::new("c5", DataType::Int32, false),
        Field::new("c6", DataType::Int64, false),use std::boxed::Box;
use std::sync::Arc;

extern crate arrow;
extern crate datafusion;

use arrow::array::{Int32Array, StringArray};
use arrow::datatypes::{DataType, Field, Schema};
use arrow::record_batch::RecordBatch;

use datafusion::datasource::MemTable;
use datafusion::execution::context::ExecutionContext;
use datafusion::logicalplan::{Expr, ScalarValue};
        Field::new("c7", DataType::UInt8, false),
        Field::new("c8", DataType::UInt16, false),
        Field::new("c9", DataType::UInt32, false),
        Field::new("c10", DataType::UInt64, false),
        Field::new("c11", DataType::Float32, false),
        Field::new("c12", DataType::Float64, false),
        Field::new("c13", DataType::Utf8, false),
    ]));

    // register csv file with the execution context
    let csv_datasource = CsvDataSource::new(
        "../../testing/data/csv/aggregate_test_100.csv",
        schema.clone(),
        1024,
    );
    ctx.register_datasource("aggregate_test_100", Rc::new(RefCell::new(csv_datasource)));

    // execute the query
    let sql = "SELECT c1, MIN(c12), MAX(c12) FROM aggregate_test_100 WHERE c11 > 0.1 AND c11 < 0.9 GROUP BY c1";
    let relation = ctx.sql(&sql).unwrap();
    let mut results = relation.borrow_mut();

    // iterate over result batches
    while let Some(batch) = results.next().unwrap() {
        println!(
            "RecordBatch has {} rows and {} columns",
            batch.num_rows(),
            batch.num_columns()
        );

        let c1 = batch
            .column(0)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();

        let min = batch
            .column(1)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        let max = batch
            .column(2)
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();

        for i in 0..batch.num_rows() {
            let c1_value: String = String::from_utf8(c1.value(i).to_vec()).unwrap();

            println!("{}, Min: {}, Max: {}", c1_value, min.value(i), max.value(i),);
        }
    }
}
