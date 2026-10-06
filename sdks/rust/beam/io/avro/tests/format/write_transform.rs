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

//! The AvroIO.Write transform configuration.

use std::collections::BTreeMap;
use std::sync::Arc;

use avro_io::avroio::{self, AvroIoError, CompressionCodec, schema_of};
use beam::pipeline::Pipeline;
use beam::prelude::*;
use beam::schema::{FieldType, Schema};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use file::WriteFiles;

use crate::common::{Simple, parse_blocks, parse_header, simple, write_to_bytes};

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
fn write_defaults_to_avro_suffix_and_runner_sharding() {
    let write = avroio::Write::new(
        "AvroIO.Write",
        "/out/events",
        avroio::AvroSink::<Simple>::new(),
    );
    assert_eq!(
        display_data(&write.write_files()),
        kv(&[
            ("transform", "AvroIO.Write"),
            ("filenamePrefix", "/out/events"),
            ("fileSuffix", ".avro"),
            ("numShards", "0"),
            ("shardNameTemplate", "-SSSSS-of-NNNNN"),
        ])
    );
    assert!(!write.write_files().is_windowed_writes());
}

#[test]
fn write_builder_options_reach_write_files() {
    let write = avroio::Write::new("MyWrite", "/out/events", avroio::AvroSink::<Simple>::new())
        .with_suffix(".bin")
        .with_num_shards(4)
        .with_shard_template("-S-of-N")
        .with_max_records_per_file(1_000)
        .with_max_bytes_per_file(1 << 20)
        .with_windowed_writes();
    let files = write.write_files();
    assert!(files.is_windowed_writes());
    assert_eq!(files.prefix(), "/out/events");
    assert_eq!(
        display_data(&files),
        kv(&[
            ("transform", "MyWrite"),
            ("filenamePrefix", "/out/events"),
            ("fileSuffix", ".bin"),
            ("numShards", "4"),
            ("shardNameTemplate", "-S-of-N"),
            ("maxRecordsPerFile", "1000"),
            ("maxBytesPerFile", "1048576"),
            ("windowedWrites", "true"),
        ])
    );

    // without_sharding: one shard, empty template.
    let single = avroio::Write::new(
        "AvroIO.Write",
        "/out/one",
        avroio::AvroSink::<Simple>::new(),
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
        .filter(|n| n.starts_with("AvroIO.Write"))
        .collect();
    names.sort();
    names
}

#[test]
fn fixed_sharding_and_windowed_writes_change_the_expanded_graph() {
    let p = Pipeline::new();
    p.apply(Create::new("Create", vec![simple()]))
        .apply(avroio::Write::new(
            "AvroIO.Write",
            "/tmp/beam_avro_graph/a",
            avroio::AvroSink::<Simple>::new(),
        ));
    assert_eq!(
        transform_names(&p),
        [
            "AvroIO.Write",
            "AvroIO.Write/Finalize",
            "AvroIO.Write/FinalizeImpulse",
            "AvroIO.Write/WriteBundles",
        ]
    );

    let p = Pipeline::new();
    p.apply(Create::new("Create", vec![simple()])).apply(
        avroio::Write::new(
            "AvroIO.Write",
            "/tmp/beam_avro_graph/b",
            avroio::AvroSink::<Simple>::new(),
        )
        .with_num_shards(3)
        .with_windowed_writes(),
    );
    assert_eq!(
        transform_names(&p),
        [
            "AvroIO.Write",
            "AvroIO.Write/AssignShard",
            "AvroIO.Write/Finalize",
            "AvroIO.Write/GatherResults",
            "AvroIO.Write/GroupByShard",
            "AvroIO.Write/KeyResults",
            "AvroIO.Write/WriteShards",
        ]
    );
}

#[test]
fn write_with_compression_and_block_size_configure_the_sink() {
    let write = avroio::Write::new("AvroIO.Write", "/out/x", avroio::AvroSink::<Simple>::new())
        .with_compression(Some(CompressionCodec::Deflate))
        .with_block_size(1);
    let buf = write_to_bytes(write.sink(), &[simple(), simple(), simple()]);
    let header = parse_header(&buf);
    assert_eq!(header.meta("avro.codec"), "deflate");
    // Block size 1: three single-record blocks.
    let counts: Vec<i64> = parse_blocks(&buf, &header).iter().map(|b| b.0).collect();
    assert_eq!(counts, [1, 1, 1]);

    let plain = avroio::Write::new("AvroIO.Write", "/out/x", avroio::AvroSink::<Simple>::new())
        .with_compression(None);
    let buf = write_to_bytes(plain.sink(), &[simple()]);
    assert_eq!(parse_header(&buf).meta("avro.codec"), "null");

    // Default block size is 4096: 4097 elements make two blocks.
    let default = avroio::Write::new("AvroIO.Write", "/out/x", avroio::AvroSink::<Simple>::new());
    let items = vec![simple(); 4_097];
    let buf = write_to_bytes(default.sink(), &items);
    let header = parse_header(&buf);
    let counts: Vec<i64> = parse_blocks(&buf, &header).iter().map(|b| b.0).collect();
    assert_eq!(counts, [4_096, 1]);
}

#[test]
fn row_sink_uses_the_given_schema() {
    let schema = Arc::new(Schema::builder().field("k", FieldType::string()).build());
    let write = avroio::Write::new(
        "AvroIO.Write",
        "/out/rows",
        avroio::AvroSink::for_rows(Arc::clone(&schema)),
    );
    assert_eq!(write.sink().schema(), &schema);
    let buf = write_to_bytes(write.sink(), &[]);
    assert_eq!(
        parse_header(&buf).meta("avro.schema"),
        r#"{"fields":[{"name":"k","type":"string"}],"name":"topLevelRecord","type":"record"}"#
    );
}

#[test]
fn schema_of_missing_file_is_a_not_found_io_error() {
    match schema_of("/nonexistent/beam/avro/file.avro") {
        Err(AvroIoError::Io(err)) => {
            assert_eq!(err.kind(), std::io::ErrorKind::NotFound);
            // The local filesystem was found; the file itself was missing.
            assert!(
                !err.to_string().contains("No FileSystem registered"),
                "{err}"
            );
        }
        other => panic!("expected a NotFound I/O error, got {other:?}"),
    }
}
