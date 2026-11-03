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

//! A data file that a writer wrote and the commit step adds to the table.

use beam::schema::BeamRow;
use bytes::Bytes;
use iceberg::spec::{DataFile, TableMetadata, read_data_files_from_avro, write_data_files_to_avro};

/// One data file of a table, as the Avro `data_file` struct of an Iceberg manifest entry.
#[derive(Clone, Debug, PartialEq, Eq, BeamRow)]
pub struct DataFileMeta {
    pub table: String,
    pub data_file: Bytes,
}

impl DataFileMeta {
    /// The meta of `file`, a file of the default partition spec of `metadata`.
    ///
    /// # Errors
    ///
    /// Fails if the file does not encode with the partition type of the spec.
    pub fn new(table: &str, file: DataFile, metadata: &TableMetadata) -> beam::Result<Self> {
        let mut bytes = Vec::new();
        write_data_files_to_avro(
            &mut bytes,
            [file],
            metadata.default_partition_type(),
            metadata.format_version(),
        )?;
        Ok(Self {
            table: table.to_string(),
            data_file: Bytes::from(bytes),
        })
    }

    /// The data file, read with the schema and the default partition spec of `metadata`.
    ///
    /// # Errors
    ///
    /// Fails if the bytes do not hold one data file of that spec.
    pub fn data_file(&self, metadata: &TableMetadata) -> beam::Result<DataFile> {
        let files = read_data_files_from_avro(
            &mut self.data_file.as_ref(),
            metadata.current_schema(),
            metadata.default_partition_spec_id(),
            metadata.default_partition_type(),
            metadata.format_version(),
        )?;
        match <[DataFile; 1]>::try_from(files) {
            Ok([file]) => Ok(file),
            Err(files) => Err(beam::Error::msg(format!(
                "A data file meta of '{}' holds {} files, not 1",
                self.table,
                files.len()
            ))),
        }
    }
}
