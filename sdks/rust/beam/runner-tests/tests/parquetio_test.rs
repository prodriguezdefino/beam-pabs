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

//! End-to-end ParquetIO pipelines on PrismRunner.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use std::fs;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use beam::io::parquet::parquetio;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row};
use beam::transforms::Create;
use fluent::prelude::PCollectionExt;
use prism::PrismRunner;

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

struct TempDir(PathBuf);

impl TempDir {
    fn new(name: &str) -> Self {
        let path =
            std::env::temp_dir().join(format!("beam_parquetio_{name}_{}", std::process::id()));
        let _ = fs::remove_dir_all(&path);
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    fn path(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }

    fn files_with_suffix(&self, suffix: &str) -> Vec<String> {
        let mut files: Vec<String> = fs::read_dir(&self.0)
            .unwrap()
            .map(|entry| entry.unwrap().path().to_str().unwrap().to_string())
            .filter(|path| path.ends_with(suffix))
            .collect();
        files.sort();
        files
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

async fn write_events(prefix: &str, items: Vec<Event>, shards: Option<u32>) -> Vec<String> {
    let written = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&written);
    let p = Pipeline::new();
    let mut write = parquetio::Write::new(
        "ParquetIO.Write",
        prefix,
        parquetio::ParquetSink::<Event>::new(),
    )
    .with_row_group_size(40);
    if let Some(s) = shards {
        write = write.with_num_shards(s);
    }
    p.apply(Create::new("Create", items)).apply(write).inspect(
        "CaptureFilenames",
        move |f: &String| {
            sink.lock().unwrap().push(f.clone());
        },
    );
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "write pipeline failed: {res:?}");
    let mut written = written.lock().unwrap().clone();
    written.sort();
    written
}

#[tokio::test]
async fn test_parquet_write_then_read_round_trip() {
    for num_shards in [Some(3), None] {
        let tag = num_shards.map_or("auto", |_| "sharded");
        let dir = TempDir::new(&format!("round_trip_{tag}"));
        let items = events(120);
        let written = write_events(&dir.path("events"), items.clone(), num_shards).await;

        let files = dir.files_with_suffix(".parquet");
        assert!(!files.is_empty(), "expected files, found {files:?}");
        if let Some(n) = num_shards {
            assert_eq!(
                files.len(),
                n as usize,
                "expected {n} shards, found {files:?}"
            );
            assert_eq!(written, files, "Write must output the final file names");
            assert!(files[0].ends_with(&format!("events-00000-of-{n:05}.parquet")));
        }

        let captured = Arc::new(Mutex::new(Vec::new()));
        let sink = Arc::clone(&captured);
        let p = Pipeline::new();
        // Small splits force row groups of one file to be read by different splits.
        p.apply(
            parquetio::Read::<Event>::new("ParquetIO.Read", dir.path("*.parquet"))
                .with_split_size(512),
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
async fn test_parquet_read_row_files_with_discovered_schema() {
    let dir = TempDir::new("rows");
    let items = events(100);
    let files = write_events(&dir.path("rows"), items.clone(), Some(2)).await;

    let schema = parquetio::schema_of(&files[0]).unwrap();
    assert_eq!(&schema, &**Event::beam_schema());

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let typed_captured = Arc::new(Mutex::new(Vec::new()));
    let typed_sink = Arc::clone(&typed_captured);

    let p = Pipeline::new();
    let rows =
        p.apply(Create::new("CreateRows", files.clone()))
            .apply(parquetio::ReadRowFiles::new(
                "ParquetIO.ReadRowFiles",
                schema,
            ));
    assert!(
        rows.row_schema().is_some(),
        "Row output must carry its schema"
    );
    rows.map("ToEvent", |row: Row| Event::from_row(&row).unwrap())
        .inspect("Capture", move |e: &Event| {
            sink.lock().unwrap().push(e.clone());
        });

    p.apply(Create::new("CreateTyped", files))
        .apply(parquetio::ReadFiles::<Event>::new("ParquetIO.ReadFiles"))
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
async fn test_parquet_empty_input_writes_valid_empty_file() {
    let dir = TempDir::new("empty");
    let prefix = dir.path("empty");
    let p = Pipeline::new();
    p.apply(Create::new("Create", Vec::<Event>::new())).apply(
        parquetio::Write::new(
            "ParquetIO.Write",
            prefix.as_str(),
            parquetio::ParquetSink::<Event>::new(),
        )
        .without_sharding(),
    );
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "write pipeline failed: {res:?}");

    let path = format!("{prefix}.parquet");
    assert_eq!(dir.files_with_suffix(".parquet"), vec![path.clone()]);
    assert_eq!(
        &parquetio::schema_of(&path).unwrap(),
        &**Event::beam_schema()
    );
}
