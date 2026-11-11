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

//! Data files written to a temp warehouse of the `iceberg` memory catalog.

use std::collections::HashMap;
use std::fs::File;
use std::sync::Arc;

use beam::schema::{Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam_io_arrow::{EncodedBatch, SchemaRowCodec, millis_instant_type};
use beam_io_iceberg::catalog::file_io;
use beam_io_iceberg::encode::Encoder;
use beam_io_iceberg::storage::FileSystemStorageFactory;
use beam_io_iceberg::table::{TableInfo, prepare};
use beam_io_iceberg::write::TableWriter;
use beam_io_iceberg::{Catalog, METRICS, PartitionField, PartitionTransform, WriteOptions};
use iceberg::CatalogBuilder;
use iceberg::memory::{MEMORY_CATALOG_WAREHOUSE, MemoryCatalog, MemoryCatalogBuilder};
use iceberg::spec::DataFile;
use parquet::basic::Compression;
use parquet::file::reader::{FileReader, SerializedFileReader};
use tempfile::TempDir;

const HOUR_MILLIS: i64 = 3_600_000;

async fn memory_catalog(warehouse: &TempDir) -> MemoryCatalog {
    MemoryCatalogBuilder::default()
        .with_storage_factory(Arc::new(FileSystemStorageFactory))
        .load(
            "test",
            HashMap::from([(
                MEMORY_CATALOG_WAREHOUSE.to_string(),
                format!("file://{}", warehouse.path().display()),
            )]),
        )
        .await
        .expect("a memory catalog")
}

fn device_metric() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("id", FieldType::int64()),
        Field::new("timestamp_ms", millis_instant_type()),
        Field::new("device_id", FieldType::string()),
        Field::new("payload", FieldType::double()),
        Field::nullable("location", FieldType::string()),
    ]))
}

fn rows(
    schema: &Arc<Schema>,
    ids: std::ops::Range<i64>,
    millis: impl Fn(i64) -> i64,
) -> Vec<RowElement> {
    ids.map(|id| {
        RowElement::Row(
            Row::new(
                Arc::clone(schema),
                vec![
                    Some(FieldValue::Int64(id)),
                    Some(FieldValue::Int64(millis(id))),
                    Some(FieldValue::String(format!("device-{id}"))),
                    Some(FieldValue::Double(1.0)),
                    None,
                ],
            )
            .expect("a row"),
        )
    })
    .collect()
}

/// A table with `options`, its writer, and an encoder for its rows.
struct Fixture {
    _warehouse: TempDir,
    schema: Arc<Schema>,
    info: TableInfo,
    writer: TableWriter,
    encoder: Encoder,
}

impl Fixture {
    async fn new(options: WriteOptions) -> Self {
        let warehouse = TempDir::new().expect("a temp dir");
        let catalog = memory_catalog(&warehouse).await;
        let schema = device_metric();
        let info = prepare(&catalog, &options, &schema)
            .await
            .expect("the table is created");
        let writer = TableWriter::new(&info, None).expect("a writer");
        let encoder = Encoder::new(Arc::clone(&schema), writer.metadata().current_schema())
            .expect("an encoder");
        Self {
            _warehouse: warehouse,
            schema,
            info,
            writer,
            encoder,
        }
    }

    /// The encoded batch of `rows`.
    fn batch(&self, rows: &[RowElement]) -> EncodedBatch {
        let encoded = self
            .encoder
            .encode(&SchemaRowCodec::new(Arc::clone(&self.schema)), rows)
            .expect("the rows encode");
        let (_, ipc) = encoded.batch.expect("a batch");
        ipc
    }

    async fn write(&self, batches: impl IntoIterator<Item = EncodedBatch>) -> Vec<DataFile> {
        self.writer
            .write(file_io([]), "test-batch".to_string(), batches)
            .await
            .expect("the files are written")
    }
}

fn options() -> WriteOptions {
    WriteOptions::new(
        Catalog::rest("http://unused").with_warehouse("bl://projects/p/catalogs/c"),
        "ns.metrics",
    )
}

fn footer_compression(file: &DataFile) -> Compression {
    let path = file.file_path().trim_start_matches("file://");
    let reader = SerializedFileReader::new(File::open(path).expect("the file exists"))
        .expect("a Parquet file");
    reader.metadata().row_group(0).column(0).compression()
}

#[tokio::test(flavor = "multi_thread")]
async fn an_unpartitioned_batch_is_one_zstd_file_under_the_data_location() {
    let fixture = Fixture::new(options()).await;
    let batch = fixture.batch(&rows(&fixture.schema, 0..100, |id| id));

    let files = fixture.write([batch]).await;

    let [file] = files.as_slice() else {
        panic!("one file: {files:?}");
    };
    assert_eq!(file.record_count(), 100);
    assert!(
        file.file_path().starts_with(&format!(
            "{}/data/test-batch-",
            fixture.writer.metadata().location()
        )),
        "{}",
        file.file_path()
    );
    assert!(matches!(footer_compression(file), Compression::ZSTD(_)));
}

#[tokio::test(flavor = "multi_thread")]
async fn rows_of_three_hours_go_to_three_partition_files() {
    let fixture = Fixture::new(options().with_partition_fields([PartitionField::new(
        "timestamp_ms",
        PartitionTransform::Hour,
    )]))
    .await;
    let batch = fixture.batch(&rows(&fixture.schema, 0..30, |id| (id % 3) * HOUR_MILLIS));

    let files = fixture.write([batch]).await;

    assert_eq!(files.len(), 3, "{files:?}");
    assert_eq!(files.iter().map(DataFile::record_count).sum::<u64>(), 30);
    assert!(
        files
            .iter()
            .all(|file| file.file_path().contains("/timestamp_ms_hour=")),
        "{files:?}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn the_codec_comes_from_the_table_properties() {
    let fixture =
        Fixture::new(options().with_table_property("write.parquet.compression-codec", "gzip"))
            .await;
    let files = fixture
        .write([fixture.batch(&rows(&fixture.schema, 0..5, |id| id))])
        .await;
    assert!(matches!(
        footer_compression(&files[0]),
        Compression::GZIP(_)
    ));
}

#[tokio::test(flavor = "multi_thread")]
async fn a_small_target_size_rolls_to_more_files() {
    let fixture =
        Fixture::new(options().with_table_property("write.target-file-size-bytes", "1")).await;
    let batches: Vec<_> = (0..3)
        .map(|part| fixture.batch(&rows(&fixture.schema, part * 10..part * 10 + 10, |id| id)))
        .collect();

    let files = fixture.write(batches).await;

    assert!(files.len() > 1, "{files:?}");
    assert_eq!(files.iter().map(DataFile::record_count).sum::<u64>(), 30);
}

#[tokio::test(flavor = "multi_thread")]
async fn object_storage_puts_hash_directories_before_the_partition_path() {
    let fixture = Fixture::new(
        options()
            .with_partition_fields([PartitionField::new(
                "timestamp_ms",
                PartitionTransform::Hour,
            )])
            .with_table_property("write.object-storage.enabled", "true"),
    )
    .await;
    let files = fixture
        .write([fixture.batch(&rows(&fixture.schema, 0..3, |id| id))])
        .await;

    let data = format!("{}/data/", fixture.writer.metadata().location());
    let rest = files[0]
        .file_path()
        .strip_prefix(&data)
        .expect("the file is under the data location");
    let parts: Vec<&str> = rest.split('/').collect();
    assert_eq!(
        parts[..4].iter().map(|dir| dir.len()).collect::<Vec<_>>(),
        vec![4, 4, 4, 8],
        "{rest}"
    );
    assert!(
        parts[..4]
            .iter()
            .all(|dir| dir.chars().all(|c| c == '0' || c == '1'))
    );
    assert!(parts[4].starts_with("timestamp_ms_hour="), "{rest}");
}

#[tokio::test(flavor = "multi_thread")]
async fn a_data_file_meta_reads_back_the_same_file() {
    let fixture = Fixture::new(options().with_partition_fields([PartitionField::new(
        "timestamp_ms",
        PartitionTransform::Hour,
    )]))
    .await;
    let files = fixture
        .write([fixture.batch(&rows(&fixture.schema, 0..10, |id| id))])
        .await;

    let metas = fixture
        .writer
        .metas(files.clone())
        .expect("the files encode");

    assert_eq!(metas.len(), 1);
    assert_eq!(metas[0].table, fixture.info.table);
    let file = metas[0]
        .data_file(fixture.writer.metadata())
        .expect("the file decodes");
    assert_eq!(file, files[0]);
}

struct TableInfoReader {
    tag: String,
    encoded: bytes::Bytes,
}

impl beam::internals::SideInputReader for TableInfoReader {
    fn get_iterable<'a>(
        &'a self,
        tag: &str,
        _window: &[u8],
    ) -> Result<beam::internals::SideInputElements<'a>, beam::internals::SideInputError> {
        let items = if tag == self.tag {
            vec![Ok(self.encoded.clone())]
        } else {
            Vec::new()
        };
        Ok(Box::new(items.into_iter()))
    }

    fn get_multimap<'a>(
        &'a self,
        tag: &str,
        _window: &[u8],
        _key: &[u8],
    ) -> Result<beam::internals::SideInputElements<'a>, beam::internals::SideInputError> {
        Err(beam::internals::SideInputError::MissingReader {
            tag: tag.to_string(),
        })
    }

    fn window_mapping(
        &self,
        tag: &str,
    ) -> Result<&beam::values::SideInputWindowing, beam::internals::SideInputError> {
        Err(beam::internals::SideInputError::MissingReader {
            tag: tag.to_string(),
        })
    }
}

#[test]
fn write_data_files_fn_streams_multiple_batches_across_partitions() {
    use std::collections::BTreeSet;

    use beam::coders::{DefaultCoder, PaneInfo, ShardedKey, WindowedHeader};
    use beam::internals::{HandlerContext, MetricsContainer, MetricsScope};
    use beam::metrics::{LINEAGE_NAMESPACE, LINEAGE_SINKS, MetricResults};
    use beam::pipeline::Pipeline;
    use beam::transforms::{Create, DoFn};
    use beam_io_iceberg::data_file::DataFileMeta;
    use beam_io_iceberg::write::WriteDataFilesFn;

    let opts = options().with_partition_fields([PartitionField::new(
        "timestamp_ms",
        PartitionTransform::Hour,
    )]);
    let runtime = tokio::runtime::Builder::new_multi_thread()
        .enable_all()
        .build()
        .expect("a runtime");
    let fixture = runtime.block_on(Fixture::new(opts.clone()));

    let batches = vec![
        fixture.batch(&rows(&fixture.schema, 0..10, |_| 0)),
        fixture.batch(&rows(&fixture.schema, 10..25, |id| (id % 2) * HOUR_MILLIS)),
        fixture.batch(&rows(&fixture.schema, 25..35, |_| HOUR_MILLIS)),
    ];

    let pipeline = Pipeline::new();
    let view = pipeline
        .apply("Info", Create::new([fixture.info.clone()]))
        .as_singleton();
    let reader = TableInfoReader {
        tag: view.tag().to_string(),
        encoded: bytes::Bytes::from(fixture.info.encode().expect("the table info encodes")),
    };
    let mut step = WriteDataFilesFn::new(view, &opts).expect("a step");
    assert_eq!(step.side_inputs().len(), 1);

    let mut raw: Vec<Vec<u8>> = Vec::new();
    let header = WindowedHeader::global(0, PaneInfo::NO_FIRING);
    let container = Arc::new(MetricsContainer::new());
    {
        let _scope = MetricsScope::enter(Arc::clone(&container), Arc::from("WriteDataFiles"));
        let mut handler_ctx = HandlerContext::new(&mut raw)
            .with_side_inputs(Some(&reader))
            .with_header(&header);
        let mut ctx = handler_ctx.as_process_context::<DataFileMeta>();
        let key = ShardedKey::new(fixture.info.table.clone(), b"s0".to_vec());
        step.process_element((key, batches), &mut ctx)
            .expect("the batches write");
    }

    assert_eq!(METRICS, "iceberg");
    let results = MetricResults::from_container(&container);
    assert_eq!(
        results.counter_for_transform("WriteDataFiles", "iceberg", "data_files_written"),
        Some(2)
    );
    assert_eq!(
        results
            .distribution_for_transform("WriteDataFiles", "iceberg", "data_file_rows")
            .expect("row distribution")
            .sum,
        35
    );
    assert!(
        results
            .distribution_for_transform("WriteDataFiles", "iceberg", "data_file_bytes")
            .expect("byte distribution")
            .sum
            > 0
    );
    assert_eq!(
        results
            .string_set_for_transform("WriteDataFiles", LINEAGE_NAMESPACE, LINEAGE_SINKS)
            .expect("sink lineage")
            .values(),
        &BTreeSet::from(["iceberg:`bl://projects/p/catalogs/c`.ns.metrics".to_string()])
    );

    let files: Vec<DataFile> = raw
        .iter()
        .map(|bytes| {
            let meta = DataFileMeta::decode(bytes).expect("a meta");
            assert_eq!(meta.table, fixture.info.table);
            meta.data_file(fixture.writer.metadata())
                .expect("a data file")
        })
        .collect();
    assert_eq!(files.len(), 2, "{files:?}");
    assert_eq!(files.iter().map(DataFile::record_count).sum::<u64>(), 35);
}
