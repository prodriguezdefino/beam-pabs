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

//! TFRecordIO pipelines on Prism.

use beam::io::file::Compression;
use beam::io::tfrecord::tfrecordio;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use prism::PrismRunner;
use tests::TempDir;

/// The TFRecord file of the records `foo` and `bar`, written by TensorFlow.
const FOO_BAR: [u8; 38] = [
    0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb0, 0x99, 0x49, 0x0e, 0x66, 0x6f, 0x6f, 0x61,
    0x8a, 0xbe, 0xfe, 0x03, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00, 0xb0, 0x99, 0x49, 0x0e, 0x62,
    0x61, 0x72, 0x46, 0x00, 0xe5, 0xc8,
];

async fn run(p: TestPipeline) {
    p.run_with(&PrismRunner::new())
        .await
        .expect("the pipeline and its assertions succeed");
}

#[tokio::test]
async fn records_written_by_write_are_read_back() {
    let dir = TempDir::new("beam-tfrecord-roundtrip");
    let records: Vec<Vec<u8>> = vec![
        b"alpha".to_vec(),
        Vec::new(),
        vec![0u8, 255, 7],
        b"omega".to_vec(),
    ];

    let write = TestPipeline::new();
    write.apply("Records", Create::new(records.clone())).apply(
        "Write",
        tfrecordio::Write::new(dir.join("data.tfrecord")).with_num_shards(2),
    );
    run(write).await;

    let read = TestPipeline::new();
    let read_back = read.apply("Read", tfrecordio::Read::new(dir.join("data-*.tfrecord")));
    passert::that("ReadBack", &read_back).contains_in_any_order(records);
    run(read).await;
}

#[tokio::test]
async fn read_files_reads_a_tensorflow_file() {
    let dir = TempDir::new("beam-tfrecord-golden");
    let path = dir.join("tensorflow_foo_bar.tfrecord");
    std::fs::write(&path, FOO_BAR).expect("write the file");

    let p = TestPipeline::new();
    let records = p
        .apply("Paths", Create::new(vec![path]))
        .apply("ReadFiles", tfrecordio::ReadFiles::new());
    passert::that("Records", &records).contains_in_any_order([b"foo".to_vec(), b"bar".to_vec()]);
    run(p).await;
}

#[tokio::test]
async fn compressed_records_written_by_write_are_read_back() {
    let records: Vec<Vec<u8>> = (0..50)
        .map(|i| format!("compressed-tfrecord-payload-{i:04}").into_bytes())
        .collect();

    for (codec, ext) in [(Compression::Gzip, ".gz"), (Compression::Zstd, ".zst")] {
        let dir = TempDir::new("beam-tfrecord-compressed");
        let write = TestPipeline::new();
        let written_paths = write.apply("Records", Create::new(records.clone())).apply(
            "WriteCompressed",
            tfrecordio::Write::new(dir.join("data.tfrecord"))
                .with_num_shards(2)
                .with_compression(codec),
        );
        let expected_paths = vec![
            dir.join(&format!("data-00000-of-00002.tfrecord{ext}")),
            dir.join(&format!("data-00001-of-00002.tfrecord{ext}")),
        ];
        passert::that("WrittenPaths", &written_paths).contains_in_any_order(expected_paths.clone());
        run(write).await;

        let read_auto = TestPipeline::new();
        let auto_records = read_auto.apply(
            "ReadAuto",
            tfrecordio::Read::new(dir.join(&format!("data-*.tfrecord{ext}"))),
        );
        passert::that("AutoReadBack", &auto_records).contains_in_any_order(records.clone());
        run(read_auto).await;

        let read_explicit = TestPipeline::new();
        let explicit_records = read_explicit
            .apply("Paths", Create::new(expected_paths))
            .apply(
                "ReadExplicit",
                tfrecordio::ReadFiles::new().with_compression(codec),
            );
        passert::that("ExplicitReadBack", &explicit_records).contains_in_any_order(records.clone());
        run(read_explicit).await;
    }
}
