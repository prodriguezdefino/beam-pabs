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
//! Tests for user-defined [`DoFn`] implementations.

use beam::prelude::*;
use beam::transforms::ProcessContext;
use prism::PrismRunner;

/// Fails on a specific element.
#[derive(Clone)]
struct FailOn {
    bad: i64,
}

impl DoFn for FailOn {
    type In = i64;
    type Out = i64;

    fn process_element(&mut self, n: i64, out: &mut ProcessContext<i64>) -> Result {
        if n == self.bad {
            return Err(format!("refusing to process {n}").into());
        }
        out.emit(n)
    }
}

#[tokio::test]
async fn test_custom_dofn_error_fails_the_pipeline() {
    let p = Pipeline::new();

    p.apply(Create::new("Create", vec![1i64, 2, 3]))
        .apply(ParDo::new("FailOnTwo", FailOn { bad: 2 }));

    let err = p
        .run_with_runner(&PrismRunner::new())
        .await
        .expect_err("an error returned from DoFn::process_element must fail the pipeline");
    let message = format!("{err:?}");
    assert!(
        message.contains("refusing to process 2"),
        "the job error must carry the DoFn's own error: {message}"
    );
}
