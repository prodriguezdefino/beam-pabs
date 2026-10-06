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

use beam::options::PipelineOptions;
use fold_state_backed::{Args, build_pipeline, format_verification};
use testutils::read_shards;

#[test]
fn format_verification_table() {
    let output = format_verification("hot_key", (100, 4950), (100, 4950))
        .expect("matching folds should verify");
    assert!(output.starts_with("MATCH"));
    assert!(output.contains("elements=100"));
    assert!(output.contains("combiner_sum=4950"));
    assert!(output.contains("state_backed_sum=4950"));
    assert!(output.contains("expected_sum=4950"));

    let err_mismatch = format_verification("hot_key", (100, 4950), (100, 4000))
        .expect_err("diverging folds must fail verification");
    assert!(err_mismatch.to_string().contains("MISMATCH"));
    assert!(err_mismatch.to_string().contains("state_backed_sum=4000"));

    let err_wrong_sum = format_verification("hot_key", (100, 1), (100, 1))
        .expect_err("a sum that disagrees with the Gauss total must fail verification");
    assert!(err_wrong_sum.to_string().contains("MISMATCH"));
}

#[tokio::test]
async fn test_fold_state_backed_pipeline_execution() {
    let temp_dir = std::env::temp_dir().join(format!("beam_fold_test_{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let out_file = temp_dir.join("output.txt");

    let args = Args {
        num_elements: 50,
        payload_bytes: 128,
        output: out_file.to_str().unwrap().to_string(),
    };
    let p = build_pipeline(&PipelineOptions::default(), &args);
    let res = p.run().await;
    assert!(res.is_ok(), "Pipeline failed: {res:?}");

    let content = read_shards(&out_file);
    eprintln!("Output content:\n{content}");
    assert!(content.contains("MATCH|key=hot_key|elements=50"));
    let _ = std::fs::remove_dir_all(&temp_dir);
}
