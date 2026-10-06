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

//! Shared fixtures.

use std::fs;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicU32, Ordering};

use arrow_io::BeamRowCodec;
use beam::schema::BeamRow;
use beam::transforms::sdf::{OffsetRange, OffsetRangeTracker};
use chrono::{DateTime, NaiveDate, Utc};
use file::sink::FileSink;
use parquet_io::parquet::basic::Compression as Codec;
use parquet_io::parquet::file::reader::{FileReader, SerializedFileReader};
use parquet_io::parquetio::{ParquetRecordReader, ParquetSink};

pub(crate) struct TempDir(PathBuf);

impl TempDir {
    pub(crate) fn new(name: &str) -> Self {
        static COUNTER: AtomicU32 = AtomicU32::new(0);
        let path = std::env::temp_dir().join(format!(
            "beam_parquet_format_{name}_{}_{}",
            std::process::id(),
            COUNTER.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir_all(&path).unwrap();
        Self(path)
    }

    pub(crate) fn file(&self, name: &str) -> String {
        self.0.join(name).to_str().unwrap().to_string()
    }
}

impl Drop for TempDir {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

pub(crate) fn write_file<T: 'static>(sink: &ParquetSink<T>, path: &str, items: &[T]) {
    let mut writer = sink
        .open(Box::new(fs::File::create(path).unwrap()))
        .unwrap();
    for item in items {
        writer.write(item).unwrap();
    }
    writer.finish().unwrap();
}

pub(crate) fn open(path: &str) -> SerializedFileReader<fs::File> {
    SerializedFileReader::new(fs::File::open(path).unwrap()).unwrap()
}

pub(crate) fn read_with_reader<T: BeamRow + 'static>(path: &str) -> Result<Vec<T>, String> {
    let len = i64::try_from(fs::metadata(path).unwrap().len()).unwrap();
    let reader = ParquetRecordReader::new(Arc::new(BeamRowCodec::<T>::new()), 64);
    let tracker = OffsetRangeTracker::new(OffsetRange::new(0, len));
    let mut out = Vec::new();
    reader.read_with_tracker(path, &tracker, |item| {
        out.push(item);
        Ok(())
    })?;
    Ok(out)
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
pub(crate) struct Simple {
    pub(crate) id: i64,
    pub(crate) name: String,
    pub(crate) score: Option<f64>,
    pub(crate) day: NaiveDate,
    pub(crate) at: DateTime<Utc>,
}

pub(crate) fn simples() -> Vec<Simple> {
    vec![
        Simple {
            id: 1,
            name: "ab".into(),
            score: None,
            day: NaiveDate::from_ymd_opt(2024, 1, 1).unwrap(),
            at: DateTime::from_timestamp(1_700_000_000, 123_456_000).unwrap(),
        },
        Simple {
            id: -2,
            name: String::new(),
            score: Some(1.5),
            day: NaiveDate::from_ymd_opt(1969, 12, 31).unwrap(),
            at: DateTime::from_timestamp(-1, 999_999_000).unwrap(),
        },
    ]
}

pub(crate) fn column_codecs(path: &str) -> Vec<Codec> {
    let reader = open(path);
    let rg = reader.metadata().row_group(0);
    rg.columns().iter().map(|c| c.compression()).collect()
}
