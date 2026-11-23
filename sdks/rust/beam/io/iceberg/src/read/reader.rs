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

//! The splittable step that reads the row groups of a data file, and the outputs it emits.

use std::marker::PhantomData;
use std::sync::{Arc, LazyLock};

use beam::coders::DefaultCoder;
use beam::metrics::Metrics;
use beam::schema::{BeamRow, Field, FieldType, FieldValue, Row, RowElement, Schema};
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
};
use beam::transforms::{Outputs, ProcessContext};
use beam_io_arrow::arrow_array::RecordBatch;
use beam_io_arrow::{BeamRowCodec, RowCodec, SchemaRowCodec, convert_rows, render_row};
use beam_io_file::{DEFAULT_SPLIT_SIZE, file_split_restriction};
use futures::TryStreamExt;
use iceberg::arrow::{ArrowReader, ArrowReaderBuilder};
use iceberg::io::FileIO;
use parquet::file::metadata::ParquetMetaDataReader;

use super::row_group_cache;
use super::task::ScanTask;
use crate::METRICS;
use crate::catalog::TableFileIo;
use crate::storage::tail_cache::{self, TAIL_CACHE_BYTES};

static FAILURE_SCHEMA: LazyLock<Arc<Schema>> = LazyLock::new(|| {
    Arc::new(Schema::new(vec![
        Field::new("failed_row", FieldType::string()),
        Field::new("error_message", FieldType::string()),
        Field::new("data_file", FieldType::string()),
    ]))
});

/// Schema of the failure output of a read: the row as text, the reason, and the data file.
pub fn read_failure_schema() -> &'static Arc<Schema> {
    &FAILURE_SCHEMA
}

/// The byte ranges `[start, end)` of the row groups of the Parquet file at `path`, in file
/// order. A row group starts at its first column chunk.
///
/// # Errors
///
/// Fails if the file does not read or its footer is not valid.
pub async fn row_groups(
    file_io: &FileIO,
    path: &str,
    size: u64,
) -> beam::Result<Arc<[(u64, u64)]>> {
    if let Some(cached) = row_group_cache::get(path, size) {
        return Ok(cached);
    }
    load_row_groups(file_io, path, size).await
}

async fn load_row_groups(
    file_io: &FileIO,
    path: &str,
    size: u64,
) -> beam::Result<Arc<[(u64, u64)]>> {
    tail_cache::invalidate_if_size_differs(path, size);
    let reader = file_io.new_input(path)?.reader().await?;
    let tail_start = size.saturating_sub(TAIL_CACHE_BYTES);
    let tail = reader.read(tail_start..size).await?;
    let invalid = || beam::Error::msg(format!("The Parquet footer of '{path}' is not valid"));
    if tail.len() as u64 != size - tail_start {
        return Err(invalid());
    }
    tail_cache::insert(path, size, tail_start, &tail);
    let trailer = tail
        .get(tail.len().saturating_sub(8)..)
        .ok_or_else(invalid)?;
    if trailer.len() < 8 || &trailer[4..] != b"PAR1" {
        return Err(invalid());
    }
    let footer = u64::from(u32::from_le_bytes([
        trailer[0], trailer[1], trailer[2], trailer[3],
    ]));
    let footer_start = size.checked_sub(footer + 8).ok_or_else(invalid)?;
    let footer_bytes = if footer_start >= tail_start {
        let start = usize::try_from(footer_start - tail_start)?;
        tail.slice(start..tail.len() - 8)
    } else {
        let full_tail = reader.read(footer_start..size).await?;
        if full_tail.len() as u64 != size - footer_start {
            return Err(invalid());
        }
        full_tail.slice(..full_tail.len() - 8)
    };
    let metadata = ParquetMetaDataReader::decode_metadata(&footer_bytes)?;
    let ranges: Arc<[(u64, u64)]> = metadata
        .row_groups()
        .iter()
        .filter(|group| group.num_rows() > 0)
        .filter_map(|group| {
            let ranges = group.columns().iter().map(|column| column.byte_range());
            let start = ranges.clone().map(|(offset, _)| offset).min()?;
            let end = ranges.map(|(offset, len)| offset + len).max()?;
            Some((start, end))
        })
        .collect();
    row_group_cache::insert(path, size, &ranges);
    Ok(ranges)
}

/// Fetches the Parquet tail into the worker tail cache when it is not cached yet, so the
/// Arrow reader serves its footer reads from memory.
async fn warm_tail(file_io: &FileIO, path: &str, size: u64) -> beam::Result {
    tail_cache::invalidate_if_size_differs(path, size);
    let tail_start = size.saturating_sub(TAIL_CACHE_BYTES);
    if size == 0 || tail_cache::has_tail(path, size, tail_start) {
        return Ok(());
    }
    let reader = file_io.new_input(path)?.reader().await?;
    let tail = reader.read(tail_start..size).await?;
    tail_cache::insert(path, size, tail_start, &tail);
    Ok(())
}

/// Builds one Iceberg Arrow reader for a task or split.
fn build_arrow_reader(file_io: &FileIO, batch_size: usize) -> beam::Result<ArrowReader> {
    Ok(ArrowReaderBuilder::new(
        file_io.clone(),
        iceberg::Runtime::new(beam::internals::io_runtime()?),
    )
    .with_batch_size(batch_size)
    .build())
}

/// Streams Arrow batches of `task` in `[start, start + length)` through `reader`, passing
/// each batch to `emit` as it arrives. Returns the count of rows emitted.
async fn stream_range(
    reader: &ArrowReader,
    task: &ScanTask,
    start: u64,
    length: u64,
    mut emit: impl FnMut(RecordBatch) -> beam::Result,
) -> beam::Result<usize> {
    let slice = task.slice(start, length)?;
    let mut stream = reader
        .clone()
        .read(Box::pin(futures::stream::once(async { Ok(slice) })))?
        .stream();
    let mut rows = 0;
    while let Some(batch) = stream.try_next().await? {
        rows += batch.num_rows();
        emit(batch)?;
    }
    Ok(rows)
}

/// Reads all rows of the encoded scan `task` with `file_io`.
///
/// # Errors
///
/// Fails if the task does not decode or a data file or delete file does not read.
pub async fn read_task(
    file_io: &FileIO,
    task: &[u8],
    batch_size: usize,
) -> beam::Result<Vec<RecordBatch>> {
    let task: ScanTask = serde_json::from_slice(task)?;
    let bytes = task.bytes()?;
    read_range(
        file_io,
        &task,
        u64::try_from(bytes.start)?,
        u64::try_from(bytes.end - bytes.start)?,
        batch_size,
    )
    .await
}

/// Reads the row groups of `task` whose start offsets `tracker` accepts, streaming each Arrow
/// batch to `emit` as it arrives. Returns `(rows_read, bytes_read)`.
///
/// # Errors
///
/// Fails if `task` does not decode, the Parquet footer is not valid, or reading or `emit`
/// fails.
pub async fn read_task_with_tracker(
    file_io: &FileIO,
    task: &[u8],
    tracker: &OffsetRangeTracker,
    batch_size: usize,
    emit: impl FnMut(RecordBatch) -> beam::Result,
) -> beam::Result<(usize, u64)> {
    let task: ScanTask = serde_json::from_slice(task)?;
    read_scan_task_with_tracker(file_io, &task, tracker, batch_size, emit).await
}

async fn read_scan_task_with_tracker(
    file_io: &FileIO,
    task: &ScanTask,
    tracker: &OffsetRangeTracker,
    batch_size: usize,
    mut emit: impl FnMut(RecordBatch) -> beam::Result,
) -> beam::Result<(usize, u64)> {
    let path = task.data_file_path.as_str();
    let (groups, cached) = match row_group_cache::get(path, task.file_size_in_bytes) {
        Some(groups) => (groups, true),
        None => (
            load_row_groups(file_io, path, task.file_size_in_bytes).await?,
            false,
        ),
    };
    let start = u64::try_from(tracker.current_restriction().start)?;
    let mut reader: Option<ArrowReader> = None;
    let (mut rows, mut bytes) = (0, 0);
    let mut stopped = false;
    for &(group_start, group_end) in groups.iter().filter(|(s, _)| *s >= start) {
        if !tracker.try_claim(&i64::try_from(group_start)?) {
            stopped = true;
            break;
        }
        let active = match reader.take() {
            Some(active) => active,
            None => {
                if cached {
                    warm_tail(file_io, path, task.file_size_in_bytes).await?;
                }
                build_arrow_reader(file_io, batch_size)?
            }
        };
        let length = group_end.saturating_sub(group_start);
        rows += stream_range(&active, task, group_start, length, &mut emit).await?;
        reader = Some(active);
        bytes += length;
    }
    if !stopped {
        // Claims the end, so the tracker knows that no row group is left.
        tracker.try_claim(&tracker.current_restriction().end);
    }
    Ok((rows, bytes))
}

async fn read_range(
    file_io: &FileIO,
    task: &ScanTask,
    start: u64,
    length: u64,
    batch_size: usize,
) -> beam::Result<Vec<RecordBatch>> {
    warm_tail(file_io, &task.data_file_path, task.file_size_in_bytes).await?;
    let reader = build_arrow_reader(file_io, batch_size)?;
    let mut batches = Vec::new();
    stream_range(&reader, task, start, length, |batch| {
        batches.push(batch);
        Ok(())
    })
    .await?;
    Ok(batches)
}

/// How a read emits the rows of an Arrow batch.
pub trait BatchOutput: Clone + Send + Sync + 'static {
    type Out: Outputs;

    /// Emits the rows of `batch`, from `data_file`.
    fn emit(
        &self,
        batch: &RecordBatch,
        data_file: &str,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result;
}

impl<T: BeamRow + DefaultCoder + Send + Sync + 'static> BatchOutput for BeamRowCodec<T> {
    type Out = T;

    fn emit(
        &self,
        batch: &RecordBatch,
        data_file: &str,
        ctx: &mut ProcessContext<'_, T>,
    ) -> beam::Result {
        self.decode_batch(batch)
            .map_err(|e| {
                beam::Error::from(e).context(format!("Cannot convert rows of '{data_file}'"))
            })?
            .into_iter()
            .try_for_each(|value| ctx.emit(value))
    }
}

impl BatchOutput for SchemaRowCodec {
    type Out = RowElement;

    fn emit(
        &self,
        batch: &RecordBatch,
        data_file: &str,
        ctx: &mut ProcessContext<'_, RowElement>,
    ) -> beam::Result {
        self.decode_batch(batch)
            .map_err(|e| {
                beam::Error::from(e).context(format!("Cannot convert rows of '{data_file}'"))
            })?
            .into_iter()
            .try_for_each(|row| ctx.emit(row))
    }
}

/// Emits the rows that convert, and a failure row for each row that does not.
pub struct WithErrors<C, T> {
    codec: C,
    _marker: PhantomData<fn() -> T>,
}

impl<C, T> WithErrors<C, T> {
    pub fn new(codec: C) -> Self {
        Self {
            codec,
            _marker: PhantomData,
        }
    }
}

impl<C: Clone, T> Clone for WithErrors<C, T> {
    fn clone(&self) -> Self {
        Self {
            codec: self.codec.clone(),
            _marker: PhantomData,
        }
    }
}

impl<C, T> BatchOutput for WithErrors<C, T>
where
    C: RowCodec<T> + Clone,
    T: DefaultCoder + Send + Sync + 'static,
{
    type Out = Result<T, Row>;

    fn emit(
        &self,
        batch: &RecordBatch,
        data_file: &str,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        convert_rows(batch, |rows| self.codec.decode_batch(rows))
            .into_iter()
            .try_for_each(|converted| match converted {
                Ok(value) => ctx.emit(Ok(value)),
                Err((_, row, error)) => {
                    let values = vec![
                        Some(FieldValue::String(render_row(&row))),
                        Some(FieldValue::String(error.to_string())),
                        Some(FieldValue::String(data_file.to_string())),
                    ];
                    let failure = Row::new(Arc::clone(&FAILURE_SCHEMA), values)?;
                    ctx.emit(Err(failure))
                }
            })
    }
}

/// Reads the row groups of one scan task. The restriction is a byte range of the data file,
/// and the step claims the start offset of each row group, so a runner can split a large file.
#[derive(Clone)]
pub(super) struct ReadTasksFn<O> {
    file_io: TableFileIo,
    batch_size: usize,
    output: O,
}

impl<O> ReadTasksFn<O> {
    pub(super) fn new(file_io: TableFileIo, batch_size: usize, output: O) -> Self {
        Self {
            file_io,
            batch_size,
            output,
        }
    }
}

impl<O: BatchOutput> SplittableDoFn for ReadTasksFn<O> {
    type In = Vec<u8>;
    type Out = O::Out;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, task: &Vec<u8>) -> beam::Result<OffsetRange> {
        serde_json::from_slice::<ScanTask>(task)?.bytes()
    }

    fn split_restriction(&self, _task: &Vec<u8>, restriction: &OffsetRange) -> Vec<OffsetRange> {
        file_split_restriction(restriction, DEFAULT_SPLIT_SIZE)
    }

    fn restriction_size(&self, _task: &Vec<u8>, restriction: &OffsetRange) -> f64 {
        restriction.size()
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        task: Vec<u8>,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, O::Out>,
    ) -> beam::Result<ProcessContinuation> {
        let task: ScanTask = serde_json::from_slice(&task)?;
        let file_io = self.file_io.get(&task.table_location)?;
        let path = task.data_file_path.as_str();
        let output = &self.output;
        // The downstream fused DoFns run inside `block_on_io`. It supports nested calls, so a
        // DoFn that blocks on I/O there does not deadlock.
        let (rows, bytes) = beam::internals::block_on_io(read_scan_task_with_tracker(
            &file_io,
            &task,
            tracker,
            self.batch_size,
            |batch| output.emit(&batch, path, ctx),
        ))??;
        Metrics::counter(METRICS, "rows_read").inc_by(i64::try_from(rows).unwrap_or(i64::MAX));
        Metrics::counter(METRICS, "bytes_read").inc_by(i64::try_from(bytes).unwrap_or(i64::MAX));
        Ok(ProcessContinuation::Stop)
    }
}
