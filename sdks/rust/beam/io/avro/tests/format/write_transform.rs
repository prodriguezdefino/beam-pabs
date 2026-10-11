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

//! Avro write transform configuration tests.

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
        .map(|item| (item.key, item.value.to_string()))
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
    let write = avroio::Write::<Simple>::new("/out/events");
    assert_eq!(
        display_data(&write.write_files()),
        kv(&[
            ("filenamePrefix", "/out/events"),
            ("stream_compression", "UNCOMPRESSED"),
            ("fileSuffix", ".avro"),
            ("numShards", "0"),
            ("shardNameTemplate", "-SSSSS-of-NNNNN"),
        ])
    );
    assert!(!write.write_files().is_windowed_writes());
}

#[test]
fn write_builder_options_reach_write_files() {
    let write = avroio::Write::<Simple>::new("/out/events")
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
            ("filenamePrefix", "/out/events"),
            ("stream_compression", "UNCOMPRESSED"),
            ("fileSuffix", ".bin"),
            ("numShards", "4"),
            ("shardNameTemplate", "-S-of-N"),
            ("maxRecordsPerFile", "1000"),
            ("maxBytesPerFile", "1048576"),
            ("windowedWrites", "true"),
        ])
    );

    // without_sharding: one shard, empty template.
    let single = avroio::Write::<Simple>::new("/out/one").without_sharding();
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
    p.apply("Create", Create::new(vec![simple()])).apply(
        "AvroIO.Write",
        avroio::Write::<Simple>::new("/tmp/beam_avro_graph/a"),
    );
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
    p.apply("Create", Create::new(vec![simple()])).apply(
        "AvroIO.Write",
        avroio::Write::<Simple>::new("/tmp/beam_avro_graph/b")
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
    let write = avroio::Write::<Simple>::new("/out/x")
        .with_compression(Some(CompressionCodec::Deflate))
        .with_block_size(1);
    let buf = write_to_bytes(&write.sink(), &[simple(), simple(), simple()]);
    let header = parse_header(&buf);
    assert_eq!(header.meta("avro.codec"), "deflate");
    // Block size 1: three single-record blocks.
    let counts: Vec<i64> = parse_blocks(&buf, &header).iter().map(|b| b.0).collect();
    assert_eq!(counts, [1, 1, 1]);

    let plain = avroio::Write::<Simple>::new("/out/x").with_compression(None);
    let buf = write_to_bytes(&plain.sink(), &[simple()]);
    assert_eq!(parse_header(&buf).meta("avro.codec"), "null");

    // Default block size is 4096: 4097 elements make two blocks.
    let default = avroio::Write::<Simple>::new("/out/x");
    let items = vec![simple(); 4_097];
    let buf = write_to_bytes(&default.sink(), &items);
    let header = parse_header(&buf);
    let counts: Vec<i64> = parse_blocks(&buf, &header).iter().map(|b| b.0).collect();
    assert_eq!(counts, [4_096, 1]);
}

#[test]
fn row_sink_uses_the_given_schema() {
    let schema = Arc::new(Schema::builder().field("k", FieldType::string()).build());
    let sink = avroio::AvroSink::for_rows(Arc::clone(&schema));
    assert_eq!(sink.schema(), &schema);
    let buf = write_to_bytes(&sink, &[]);
    assert_eq!(
        parse_header(&buf).meta("avro.schema"),
        r#"{"fields":[{"name":"k","type":"string"}],"name":"topLevelRecord","type":"record"}"#
    );
}

#[test]
fn write_rows_needs_a_row_schema_on_its_input() -> Result {
    let schema = Schema::builder().field("k", FieldType::string()).build();
    let p = Pipeline::new();
    let rows = p.apply("Rows", Create::new(Vec::<Row>::new()));
    let write = avroio::WriteRows::new("/out/rows");
    rows.apply("AvroIO.Write", write.clone());
    let err = p.validate().unwrap_err();
    assert!(
        err.to_string().contains("the input has no row schema"),
        "{err}"
    );

    let p = Pipeline::new();
    let rows = p.apply("Rows", Create::new(Vec::<Row>::new()));
    rows.with_row_schema(&schema).apply("AvroIO.Write", write);
    p.validate()?;
    Ok(())
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

/// Avro compresses inside the file, so `WriteFiles` rejects a stream codec.
#[test]
fn write_files_rejects_stream_compression() {
    let p = Pipeline::new();
    p.apply("Create", Create::new(Vec::<Simple>::new())).apply(
        "Write",
        WriteFiles::new("/out/events", avroio::AvroSink::<Simple>::new())
            .with_compression(file::Compression::Zstd),
    );
    let err = p.validate().expect_err("stream compression must fail");
    assert!(
        err.to_string().contains(
            "the sink does not accept ZSTD stream compression: Avro compresses inside the \
             file; set the codec with AvroSink::with_compression"
        ),
        "{err}"
    );
}
