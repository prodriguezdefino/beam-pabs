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

//! Throughput of reading a multi-row-group Iceberg Parquet data file.
//!
//! Writes one Parquet file with 8 row groups of 5,000 rows (40,000 rows) to a temporary
//! directory and benchmarks reading it through `read_task_with_tracker` and `read_task`.
//!
//! ```text
//! ./gradlew :sdks:rust:harnessBenchmark -Ppkg=apache-beam-io-iceberg -Pbench=read -PcargoProfile=worker
//! ```

use std::collections::HashMap;
use std::hint::black_box;
use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker, RestrictionTracker};
use beam_io_arrow::{FieldSelection, SchemaRowCodec};
use beam_io_iceberg::catalog::file_io;
use beam_io_iceberg::read::plan::plan_tasks;
use beam_io_iceberg::read::reader::{read_task, read_task_with_tracker, row_groups};
use beam_io_iceberg::storage::FileSystemStorageFactory;
use beam_io_iceberg::write::commit::{Commit, commit};
use beam_io_iceberg::write::encode::Encoder;
use beam_io_iceberg::write::files::TableWriter;
use beam_io_iceberg::write::table::prepare;
use beam_io_iceberg::{Catalog, DEFAULT_READ_BATCH_SIZE, ReadOptions, WriteOptions};
use criterion::{Criterion, Throughput, criterion_group, criterion_main};
use iceberg::CatalogBuilder;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalogBuilder};
use tempfile::TempDir;

const TABLE: &str = "ns.events";
const ROW_GROUPS: usize = 8;
const ROWS_PER_GROUP: usize = 5_000;
const TOTAL_ROWS: usize = ROW_GROUPS * ROWS_PER_GROUP;
const ROW_GROUP_BYTES: usize = 256 * 1024;

struct BenchFixture {
    _warehouse: TempDir,
    task: Vec<u8>,
    file_size: u64,
}

async fn setup_fixture() -> BenchFixture {
    let warehouse = TempDir::new().expect("temp dir");
    let catalog = MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(FileSystemStorageFactory))
        .load(
            "bench",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse.path().display()),
            )]),
        )
        .await
        .expect("memory catalog");

    let schema = Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("name", FieldType::string()),
        Field::new("score", FieldType::double()),
    ]));
    let write_options = WriteOptions::new(Catalog::rest("http://unused"), TABLE)
        .with_table_property("write.parquet.compression-codec", "uncompressed")
        .with_table_property(
            "write.parquet.row-group-size-bytes",
            ROW_GROUP_BYTES.to_string(),
        );
    let info = prepare(&catalog, &write_options, &schema)
        .await
        .expect("table prepared");
    let writer = TableWriter::new(&info, None).expect("table writer");
    let encoder = Encoder::new(
        Arc::clone(&schema),
        &FieldSelection::All,
        writer.metadata().current_schema(),
    )
    .expect("encoder");
    let codec = SchemaRowCodec::new(Arc::clone(&schema));
    let batches: Vec<_> = (0..ROW_GROUPS)
        .map(|group| {
            let start = (group * ROWS_PER_GROUP) as i64;
            let end = start + ROWS_PER_GROUP as i64;
            let rows: Vec<RowElement> = (start..end)
                .map(|id| {
                    let values = vec![
                        Some(FieldValue::Int64(id)),
                        Some(FieldValue::String(format!(
                            "item-{id:08}-payload-0123456789abcdef"
                        ))),
                        Some(FieldValue::Double(id as f64 * 0.5)),
                    ];
                    RowElement::Row(Row::new(Arc::clone(&schema), values).expect("row"))
                })
                .collect();
            let (_, batch) = encoder
                .encode(&codec, &rows)
                .expect("rows encode")
                .batch
                .expect("batch");
            batch
        })
        .collect();

    let io = file_io([]);
    let files = writer
        .write(io.clone(), "bench-rg".to_string(), batches)
        .await
        .expect("data file written");
    let [data_file] = files.as_slice() else {
        panic!("expected one data file, got {}", files.len());
    };
    let data_file_path = data_file.file_path().to_string();
    let file_size = data_file.file_size_in_bytes();

    let read_options = ReadOptions::new(Catalog::rest("http://unused"), TABLE);
    let ident = read_options.table_ident().expect("table ident");
    match commit(&catalog, &ident, files).await.expect("commit") {
        Commit::New(_) => {}
        Commit::Found(_) => panic!("expected a new snapshot"),
    }

    let groups = row_groups(&io, &data_file_path, file_size)
        .await
        .expect("row groups");
    assert!(
        groups.len() >= ROW_GROUPS,
        "expected >= {ROW_GROUPS} row groups, got {}",
        groups.len()
    );

    let tasks = plan_tasks(&catalog, &read_options, &FieldSelection::All)
        .await
        .expect("plan tasks");
    let [task] = <[Vec<u8>; 1]>::try_from(tasks).expect("one scan task");

    BenchFixture {
        _warehouse: warehouse,
        task,
        file_size,
    }
}

fn iceberg_read(c: &mut Criterion) {
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("tokio runtime");
    let fixture = runtime.block_on(setup_fixture());
    let io = file_io([]);
    let file_end = i64::try_from(fixture.file_size).expect("file size fits i64");

    let mut group = c.benchmark_group("iceberg_read");
    group.throughput(Throughput::Elements(TOTAL_ROWS as u64));

    group.bench_function("read_task_with_tracker_full_file", |b| {
        b.to_async(&runtime).iter(|| async {
            let tracker = OffsetRangeTracker::new(OffsetRange::new(0, file_end));
            let (rows, bytes) = read_task_with_tracker(
                &io,
                black_box(&fixture.task),
                &tracker,
                DEFAULT_READ_BATCH_SIZE,
                |batch| {
                    black_box(batch.num_rows());
                    Ok(())
                },
            )
            .await
            .expect("read succeeds");
            tracker.check_done().expect("tracker done");
            assert_eq!(rows, TOTAL_ROWS);
            black_box((rows, bytes))
        });
    });

    group.bench_function("read_task_with_tracker_four_splits", |b| {
        let step = file_end / 4 + 1;
        b.to_async(&runtime).iter(|| async {
            let mut total_rows = 0usize;
            for i in 0..4 {
                let start = i * step;
                let end = ((i + 1) * step).min(file_end);
                let tracker = OffsetRangeTracker::new(OffsetRange::new(start, end));
                let (rows, _) = read_task_with_tracker(
                    &io,
                    black_box(&fixture.task),
                    &tracker,
                    DEFAULT_READ_BATCH_SIZE,
                    |batch| {
                        black_box(batch.num_rows());
                        Ok(())
                    },
                )
                .await
                .expect("split read succeeds");
                tracker.check_done().expect("tracker done");
                total_rows += rows;
            }
            assert_eq!(total_rows, TOTAL_ROWS);
            black_box(total_rows)
        });
    });

    group.bench_function("read_task_full_range", |b| {
        b.to_async(&runtime).iter(|| async {
            let batches = read_task(&io, black_box(&fixture.task), DEFAULT_READ_BATCH_SIZE)
                .await
                .expect("read task succeeds");
            let rows: usize = batches.iter().map(|batch| batch.num_rows()).sum();
            assert_eq!(rows, TOTAL_ROWS);
            black_box(batches)
        });
    });

    group.finish();
}

criterion_group!(benches, iceberg_read);
criterion_main!(benches);
