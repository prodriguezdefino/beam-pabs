/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

//! Main binary for the Row Schemas example pipeline.
//!
//! Run on Prism, the default runner:
//! ```text
//! cargo run -p row_schemas
//! ```

use beam::prelude::*;
use row_schemas::{FinancialSummary, build_row_schemas_pipeline, sample_customers};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::harness::init_logging();

    let pipeline = Pipeline::create(&PipelineOptions::from_args());
    let summary = build_row_schemas_pipeline(&pipeline, sample_customers());

    let _ = summary.map("LogSummary", |s: FinancialSummary| {
        tracing::info!(
            "Financial Summary: {} customers ({} premium), total balance: ${}, avg: ${}",
            s.total_customers,
            s.premium_customers,
            s.total_balance,
            s.average_balance
        );
        s
    });

    tracing::info!("Running row_schemas pipeline...");
    let result = pipeline.run().await?;
    tracing::info!(
        "Row schemas pipeline completed with state: {}",
        result.state
    );

    Ok(())
}
