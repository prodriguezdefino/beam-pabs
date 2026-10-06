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

use std::fs;

use streaming::{StreamingArgs, build_pipeline};
use testutils::read_shards;

#[test]
fn test_streaming_args_parsing() {
    let (_, end_args) = beam::options::parse_from::<StreamingArgs, _, _>([
        "streaming",
        "--end=10",
        "--interval_ms=100",
    ]);
    assert_eq!(end_args.impulse_interval_ms, 100);
    assert_eq!(end_args.limit, Some(10));
}

#[tokio::test]
async fn test_streaming_periodic_impulse_on_prism_runner() {
    let temp_dir = std::env::temp_dir().join(format!("beam_stream_imp_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).expect("tempdir creation must succeed");
    let temp_dir = temp_dir.canonicalize().expect("canonicalize must succeed");
    let out_file = temp_dir.join("output.txt");

    let (options, args) = beam::options::parse_from::<StreamingArgs, _, _>([
        "streaming",
        "--limit=3",
        "--impulse_interval_ms=20",
        "--window_size_secs=1",
        &format!("--output={}", out_file.to_str().unwrap()),
        "--runner=prism",
    ]);

    let p = build_pipeline(&options, &args);

    let result = p
        .run()
        .await
        .expect("Periodic impulse pipeline on prism runner must succeed");
    assert_eq!(result.state, "DONE");

    let content = read_shards(&out_file);
    assert!(!content.is_empty());
    let total_heartbeats: usize = content
        .lines()
        .filter(|line| line.contains("metric: 'heartbeat'"))
        .map(|line| {
            line.rsplit_once("count: ")
                .expect("line must contain count: ")
                .1
                .trim()
                .parse::<usize>()
                .expect("count must parse as usize")
        })
        .sum();
    assert_eq!(total_heartbeats, 3);
    let _ = fs::remove_dir_all(&temp_dir);
}
