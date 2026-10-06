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

//! Grades the students of a CSV file in three tiers.
//!
//! Run on Prism, the default runner:
//!
//! ```text
//! cargo run -p partition -- --input=scores.csv --output=/tmp/partition.txt
//! ```

use beam::prelude::*;
use partition::{Args, GradeStudents, Student};

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    beam::logging::init_logging();
    let (options, args) = beam::options::parse::<Args>();

    let p = Pipeline::create(&options);
    p.apply("ReadLines", textio::Read::new(&args.input))
        .par_do_fn("ParseStudents", |line: String, ctx| {
            ctx.emit(line.parse::<Student>()?)
        })
        .apply("GradeStudents", GradeStudents)
        .apply("WriteLines", textio::Write::new(&args.output));
    p.run().await?;

    Ok(())
}
