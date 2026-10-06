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

use beam::runners::JobState;
use testutils::read_shards;
use windowed_wordcount::{WindowedWordCountArgs, build_pipeline};

#[tokio::test]
async fn test_windowed_wordcount_on_prism_runner() {
    let temp_dir = std::env::temp_dir().join(format!("beam_winwc_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).expect("tempdir creation must succeed");
    let temp_dir = temp_dir.canonicalize().expect("canonicalize must succeed");
    let in_file = temp_dir.join("input.txt");
    let out_file = temp_dir.join("output.txt");

    fs::write(&in_file, "hello window\nhello prism\nhello window\n")
        .expect("writing input file must succeed");

    let (options, args) = beam::options::parse_from::<WindowedWordCountArgs, _, _>([
        "windowed_wordcount",
        &format!(
            "--input={}",
            in_file.to_str().expect("valid utf-8 input path")
        ),
        &format!(
            "--output={}",
            out_file.to_str().expect("valid utf-8 output path")
        ),
        "--window_size=60",
        "--runner=prism",
    ]);

    let p = build_pipeline(&options, &args);
    let result = p
        .run()
        .await
        .expect("windowed wordcount pipeline on prism runner must succeed");
    assert_eq!(result.state, JobState::Done);

    let content = read_shards(&out_file);
    let mut lines: Vec<String> = content.lines().map(|s| s.to_string()).collect();
    lines.sort();
    assert_eq!(
        lines,
        vec![
            "[1970-01-01T00:16:00.000Z..1970-01-01T00:17:00.000Z) hello: 1".to_string(),
            "[1970-01-01T00:16:00.000Z..1970-01-01T00:17:00.000Z) prism: 1".to_string(),
            "[1970-01-01T00:18:00.000Z..1970-01-01T00:19:00.000Z) hello: 2".to_string(),
            "[1970-01-01T00:18:00.000Z..1970-01-01T00:19:00.000Z) window: 2".to_string(),
        ]
    );
    let _ = fs::remove_dir_all(&temp_dir);
}
