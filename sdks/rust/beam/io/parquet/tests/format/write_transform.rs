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

//! The ParquetIO.Write transform configuration.

use std::collections::BTreeMap;
use std::sync::Arc;

use beam::pipeline::Pipeline;
use beam::prelude::*;
use beam::schema::{FieldType, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use file::WriteFiles;
use parquet_io::parquet::basic::Compression as Codec;
use parquet_io::parquet::file::properties::WriterProperties;
use parquet_io::parquet::file::reader::FileReader;
use parquet_io::parquetio::{self, Compression};

use crate::common::{Simple, TempDir, column_codecs, open, simples, write_file};

fn display_data<T>(files: &WriteFiles<T>) -> BTreeMap<String, String> {
    let mut builder = DisplayDataBuilder::new();
    files.populate_display_data(&mut builder);
    builder
        .build()
        .into_iter()
        .map(|item| (item.key, item.value))
        .collect()
}

fn kv(pairs: &[(&str, &str)]) -> BTreeMap<String, String> {
    pairs
        .iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect()
}

#[test]
fn write_defaults_to_parquet_suffix_and_runner_sharding() {
    let write = parquetio::Write::new(
        "ParquetIO.Write",
        "/out/events",
        parquetio::ParquetSink::<Simple>::new(),
    );
    assert_eq!(
        display_data(&write.write_files()),
        kv(&[
            ("transform", "ParquetIO.Write"),
            ("filenamePrefix", "/out/events"),
            ("fileSuffix", ".parquet"),
            ("numShards", "0"),
            ("shardNameTemplate", "-SSSSS-of-NNNNN"),
        ])
    );
    assert!(!write.write_files().is_windowed_writes());
}

#[test]
fn write_builder_options_reach_write_files() {
    let files = parquetio::Write::new(
        "MyWrite",
        "/out/events",
        parquetio::ParquetSink::<Simple>::new(),
    )
    .with_suffix(".pq")
    .with_num_shards(7)
    .with_shard_template("-SS")
    .with_max_records_per_file(10)
    .with_max_bytes_per_file(4096)
    .with_temp_directory("/out/tmp")
    .with_windowed_writes()
    .write_files();
    assert!(files.is_windowed_writes());
    assert_eq!(
        display_data(&files),
        kv(&[
            ("transform", "MyWrite"),
            ("filenamePrefix", "/out/events"),
            ("fileSuffix", ".pq"),
            ("numShards", "7"),
            ("shardNameTemplate", "-SS"),
            ("maxRecordsPerFile", "10"),
            ("maxBytesPerFile", "4096"),
            ("windowedWrites", "true"),
        ])
    );
    let single = parquetio::Write::new(
        "ParquetIO.Write",
        "/out/one",
        parquetio::ParquetSink::<Simple>::new(),
    )
    .without_sharding();
    let dd = display_data(&single.write_files());
    assert_eq!(dd.get("numShards").map(String::as_str), Some("1"));
    assert_eq!(dd.get("shardNameTemplate"), None);
}

fn transform_names(p: &Pipeline) -> Vec<String> {
    let mut names: Vec<String> = p
        .to_proto()
        .components
        .unwrap()
        .transforms
        .values()
        .map(|t| t.unique_name.clone())
        .filter(|n| n.starts_with("ParquetIO.Write"))
        .collect();
    names.sort();
    names
}

#[test]
fn fixed_sharding_and_windowed_writes_change_the_expanded_graph() {
    let p = Pipeline::new();
    p.apply(Create::new("Create", simples()))
        .apply(parquetio::Write::new(
            "ParquetIO.Write",
            "/tmp/beam_parquet_graph/a",
            parquetio::ParquetSink::<Simple>::new(),
        ));
    assert_eq!(
        transform_names(&p),
        [
            "ParquetIO.Write",
            "ParquetIO.Write/Finalize",
            "ParquetIO.Write/FinalizeImpulse",
            "ParquetIO.Write/WriteBundles",
        ]
    );

    let p = Pipeline::new();
    p.apply(Create::new("Create", simples())).apply(
        parquetio::Write::new(
            "ParquetIO.Write",
            "/tmp/beam_parquet_graph/b",
            parquetio::ParquetSink::<Simple>::new(),
        )
        .with_num_shards(2)
        .with_windowed_writes(),
    );
    assert_eq!(
        transform_names(&p),
        [
            "ParquetIO.Write",
            "ParquetIO.Write/AssignShard",
            "ParquetIO.Write/Finalize",
            "ParquetIO.Write/GatherResults",
            "ParquetIO.Write/GroupByShard",
            "ParquetIO.Write/KeyResults",
            "ParquetIO.Write/WriteShards",
        ]
    );
}

#[test]
fn write_sink_options_are_applied_to_the_file() {
    let dir = TempDir::new("write_sink");
    let path = dir.file("out.parquet");
    let write = parquetio::Write::new(
        "ParquetIO.Write",
        "/unused",
        parquetio::ParquetSink::<Simple>::new(),
    )
    .with_compression(Compression::UNCOMPRESSED)
    .with_batch_size(3)
    .with_row_group_size(4);
    let items: Vec<Simple> = (0..10)
        .map(|i| Simple {
            id: i,
            ..simples()[0].clone()
        })
        .collect();
    write_file(write.sink(), &path, &items);
    let reader = open(&path);
    let meta = reader.metadata();
    let rows: Vec<i64> = meta.row_groups().iter().map(|rg| rg.num_rows()).collect();
    assert_eq!(rows, [4, 4, 2]);
    assert!(
        column_codecs(&path)
            .iter()
            .all(|c| *c == Codec::UNCOMPRESSED)
    );

    // Custom writer properties replace compression/row-group settings wholesale.
    let props = WriterProperties::builder()
        .set_compression(Codec::SNAPPY)
        .set_max_row_group_row_count(Some(5))
        .build();
    let write = parquetio::Write::new(
        "ParquetIO.Write",
        "/unused",
        parquetio::ParquetSink::<Simple>::new(),
    )
    .with_writer_properties(props);
    write_file(write.sink(), &path, &items);
    let reader = open(&path);
    let rows: Vec<i64> = reader
        .metadata()
        .row_groups()
        .iter()
        .map(|rg| rg.num_rows())
        .collect();
    assert_eq!(rows, [5, 5]);
    assert!(column_codecs(&path).iter().all(|c| *c == Codec::SNAPPY));
}

#[test]
fn row_sink_uses_the_given_schema() {
    let schema = Arc::new(
        Schema::builder()
            .field("k", FieldType::string())
            .nullable_field("v", FieldType::int32())
            .build(),
    );
    let write = parquetio::Write::new(
        "ParquetIO.Write",
        "/out/rows",
        parquetio::ParquetSink::for_rows(Arc::clone(&schema)),
    );
    assert_eq!(write.sink().schema(), &schema);
}
