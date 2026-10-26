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

//! Compression round-trip tests for `tfrecordio::Read` and `tfrecordio::Write`.

use beam::prelude::*;
use beam_io_file::Compression;
use beam_io_tfrecord::tfrecordio;
use beam_testing::{TestPipeline, passert};

#[tokio::test]
async fn deflate_compressed_records_round_trip_with_auto_and_explicit_mode() {
    let dir = tempfile::tempdir().expect("create temp dir");
    let prefix = dir.path().join("records.tfrecord");
    let prefix_str = prefix.to_string_lossy().into_owned();

    let records: Vec<Vec<u8>> = (0..24)
        .map(|i| format!("deflate-tfrecord-item-{i:03}").into_bytes())
        .collect();

    let write = TestPipeline::new();
    write.apply("Create", Create::new(records.clone())).apply(
        "WriteDeflate",
        tfrecordio::Write::new(&prefix_str)
            .without_sharding()
            .with_compression(Compression::Deflate),
    );
    write.run().await.expect("write deflate TFRecord");

    let deflate_path = format!("{prefix_str}.deflate");

    let read_explicit = TestPipeline::new();
    let explicit_out = read_explicit.apply(
        "ReadExplicit",
        tfrecordio::Read::new(&deflate_path).with_compression(Compression::Deflate),
    );
    passert::that("ExplicitMatches", &explicit_out).contains_in_any_order(records.clone());
    read_explicit
        .run()
        .await
        .expect("read deflate TFRecord with explicit codec");

    let read_auto = TestPipeline::new();
    let auto_out = read_auto.apply("ReadAuto", tfrecordio::Read::new(&deflate_path));
    passert::that("AutoMatches", &auto_out).contains_in_any_order(records);
    read_auto
        .run()
        .await
        .expect("read deflate TFRecord with auto detection");
}
