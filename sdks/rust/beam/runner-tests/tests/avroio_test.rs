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

//! End-to-end AvroIO pipelines on PrismRunner.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::sync::{Arc, Mutex};

use beam::io::avro::avroio;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row};
use beam::transforms::{Create, Reshuffle};
use fluent::prelude::PCollectionExt;
use prism::PrismRunner;
use tests::TempDir;

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Event {
    id: i64,
    user: String,
    score: Option<f64>,
    tags: Vec<String>,
}

fn events(n: i64) -> Vec<Event> {
    (0..n)
        .map(|id| Event {
            id,
            user: format!("user-{}", id % 7),
            score: (id % 3 != 0).then_some(id as f64 * 1.5),
            tags: (0..id % 3).map(|t| format!("tag{t}")).collect(),
        })
        .collect()
}

async fn write_events(prefix: &str, items: Vec<Event>, shards: Option<u32>) -> Vec<String> {
    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&written);
    let p = Pipeline::new();
    let mut write = avroio::Write::<Event>::new(prefix).with_block_size(40);
    if let Some(s) = shards {
        write = write.with_num_shards(s);
    }
    p.apply("Create", Create::new(items))
        .apply("Write", write)
        .inspect("CaptureFilenames", move |f: &String| {
            sink.lock().unwrap().push(f.clone());
        });
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "write pipeline failed: {res:?}");
    let mut written = written.lock().unwrap().clone();
    written.sort();
    written
}

#[tokio::test]
async fn test_avro_write_then_read_round_trip() {
    for num_shards in [Some(3), None] {
        let tag = num_shards.map_or("auto", |_| "sharded");
        let dir = TempDir::new(&format!("round_trip_{tag}"));
        let items = events(120);
        let written = write_events(&dir.join("events"), items.clone(), num_shards).await;

        let files = dir.files_with_suffix(".avro");
        assert!(!files.is_empty(), "expected files, found {files:?}");
        if let Some(n) = num_shards {
            assert_eq!(
                files.len(),
                n as usize,
                "expected {n} shards, found {files:?}"
            );
            assert_eq!(written, files, "Write must output the final file names");
            assert!(files[0].ends_with(&format!("events-00000-of-{n:05}.avro")));
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&captured);
        let p = Pipeline::new();
        // Small splits force blocks of one file to be read by different splits.
        p.apply(
            "AvroIO.Read",
            avroio::Read::<Event>::new(dir.join("*.avro")).with_split_size(512),
        )
        .inspect("Capture", move |e: &Event| {
            sink.lock().unwrap().push(e.clone());
        });
        let res = p.run_with_runner(&PrismRunner::new()).await;
        assert!(res.is_ok(), "read pipeline failed: {res:?}");

        let mut read = captured.lock().unwrap().clone();
        read.sort_by_key(|e| e.id);
        assert_eq!(read, items);
    }
}

#[tokio::test]
async fn test_avro_read_row_files_with_discovered_schema() {
    let dir = TempDir::new("rows");
    let items = events(100);
    let files = write_events(&dir.join("rows"), items.clone(), Some(2)).await;

    let schema = avroio::schema_of(&files[0]).unwrap();
    assert_eq!(&schema, &**Event::beam_schema());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let typed_captured = Arc::new(Mutex::new(Vec::new()));
    let typed_sink = Arc::clone(&typed_captured);

    let p = Pipeline::new();
    let rows = p
        .apply("CreateRows", Create::new(files.clone()))
        .apply("AvroIO.ReadRowFiles", avroio::ReadRowFiles::new(schema));
    assert!(
        rows.row_schema().is_some(),
        "Row output must carry its schema"
    );
    rows.map("ToEvent", |row: Row| Event::from_row(&row).unwrap())
        .inspect("Capture", move |e: &Event| {
            sink.lock().unwrap().push(e.clone());
        });

    p.apply("CreateTyped", Create::new(files))
        .apply("AvroIO.ReadFiles", avroio::ReadFiles::<Event>::new())
        .inspect("CaptureTyped", move |e: &Event| {
            typed_sink.lock().unwrap().push(e.clone());
        });

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "read pipeline failed: {res:?}");

    let mut read = captured.lock().unwrap().clone();
    read.sort_by_key(|e| e.id);
    assert_eq!(read, items);

    let mut typed_read = typed_captured.lock().unwrap().clone();
    typed_read.sort_by_key(|e| e.id);
    assert_eq!(typed_read, items);
}

#[tokio::test]
async fn test_avro_empty_input_writes_valid_empty_file() {
    let dir = TempDir::new("empty");
    let prefix = dir.join("empty");
    let p = Pipeline::new();
    p.apply("Create", Create::new(Vec::<Event>::new())).apply(
        "AvroIO.Write",
        avroio::Write::<Event>::new(prefix.as_str()).without_sharding(),
    );
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "write pipeline failed: {res:?}");

    let path = format!("{prefix}.avro");
    assert_eq!(dir.files_with_suffix(".avro"), vec![path.clone()]);
    assert_eq!(&avroio::schema_of(&path).unwrap(), &**Event::beam_schema());
}

#[tokio::test]
async fn test_avro_rows_write_and_encoded_read_round_trip() {
    let dir = TempDir::new("row_copy");
    let items = events(80);
    let schema = Arc::clone(Event::beam_schema());
    let rows: Vec<Row> = items.iter().map(|e| e.to_row().unwrap()).collect();

    // Fixed shards group the rows by shard key, so the rows cross a GroupByKey.
    let p = Pipeline::new();
    p.apply("CreateRows", Create::new(rows))
        .with_row_schema(&schema)
        .apply(
            "AvroIO.WriteRows",
            avroio::WriteRows::new(dir.join("rows")).with_num_shards(2),
        );
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "row write pipeline failed: {res:?}");

    // The reader gives row coder bytes, and the Reshuffle keeps the row schema of its input.
    let p = Pipeline::new();
    p.apply(
        "AvroIO.ReadRowsEncoded",
        avroio::ReadRows::new(dir.join("rows*.avro"), Arc::clone(&schema)).with_encoded_rows(),
    )
    .apply("Reshuffle", Reshuffle::new())
    .apply(
        "AvroIO.CopyEncoded",
        avroio::WriteRows::new(dir.join("copied")).without_sharding(),
    );
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "encoded copy pipeline failed: {res:?}");

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let p = Pipeline::new();
    p.apply(
        "AvroIO.ReadCopied",
        avroio::Read::<Event>::new(dir.join("copied.avro")),
    )
    .inspect("Capture", move |e: &Event| {
        sink.lock().unwrap().push(e.clone());
    });
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "read copied pipeline failed: {res:?}");

    let mut read = captured.lock().unwrap().clone();
    read.sort_by_key(|e| e.id);
    assert_eq!(read, items);
}
