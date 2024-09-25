// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use std::sync::Arc;

use datafusion::arrow::array::{UInt64Array, UInt8Array};
use datafusion::arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::datasource::MemTable;
use datafusion::error::Result;
use datafusion::prelude::SessionContext;
use arrow::util::pretty::print_batches;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::union::UnionExec;

#[tokio::main]
async fn main() -> Result<()> {
    let mem_table = create_memtable()?;

    // create local execution context
    let ctx = SessionContext::new();

    // Register the in-memory table containing the data
    ctx.register_table("users", Arc::new(mem_table))?;
    let df_old = ctx.sql("SELECT avg(foo) FROM users where id=1;").await?;
    let batches = df_old.clone().collect().await?;
    print_batches(&batches)?;

    let agg_old = {
        let plan = df_old.clone().create_physical_plan().await?;
        let exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();

        Arc::new(AggregateExec::try_new(
            AggregateMode::Partial,
            exec.group_expr().clone(),
            exec.aggr_expr().to_vec(),
            exec.filter_expr().to_vec(),
            exec.input().clone(),
            exec.input_schema(),
        )?)
    };

    let dataframe = ctx.sql("SELECT avg(foo) FROM users where id=4;").await?;
    let batches = dataframe.clone().collect().await?;
    print_batches(&batches)?;

    let plan = dataframe.clone().create_physical_plan().await?;
    // dbg!(&plan);
    // dbg!(plan.name());

    let exec = plan.as_any().downcast_ref::<AggregateExec>().unwrap();
    // dbg!(&exec);
    // dbg!(exec.input());

    let agg_new = Arc::new(AggregateExec::try_new(
        AggregateMode::Partial,
        exec.group_expr().clone(),
        exec.aggr_expr().to_vec(),
        exec.filter_expr().to_vec(),
        exec.input().clone(),
        exec.input_schema(),
    )?);

    // let previous_schema = results_previous.first().unwrap().schema().clone();
    // let previous_results = MemoryExec::try_new(
    //     &[results_previous],
    //     previous_schema,
    //     None,
    // ).map(Arc::new)?;

    let combined_input = Arc::new(UnionExec::new(vec![agg_old, agg_new]));
    let combined_input = Arc::new(CoalescePartitionsExec::new(combined_input));
    let input_schema = combined_input.schema();
    //
    // // dbg!(exec.group_expr());

    let final_agg = Arc::new(AggregateExec::try_new(
        AggregateMode::Final,
        exec.group_expr().clone(),
        exec.aggr_expr().to_vec(),
        vec![None],
        combined_input,
        input_schema,
    )?);

    // let batches = dataframe.collect().await.unwrap();

    let task_ctx = Arc::new(dataframe.task_ctx());
    let batches = collect(final_agg, task_ctx).await?;

    print_batches(&batches)?;

    Ok(())
}

fn create_memtable() -> Result<MemTable> {
    let schema = SchemaRef::new(Schema::new(vec![
        Field::new("id", DataType::UInt8, false),
        Field::new("foo", DataType::UInt64, true),
    ]));

    let id_array = UInt8Array::from(vec![1, 1, 1, 4, 5]);
    let account_array = UInt64Array::from(vec![9000, 9001, 9002, 9003, 9004]);

    let b = RecordBatch::try_new(
        schema.clone(),
        vec![Arc::new(id_array), Arc::new(account_array)],
    )?;

    MemTable::try_new(schema, vec![vec![b]])
}
