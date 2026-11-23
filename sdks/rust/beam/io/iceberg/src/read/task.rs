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

//! The scan task of one data file, in a form that goes through a shuffle.
//!
//! `FileScanTask` holds its partition as an Iceberg struct, which has no serde form, so the
//! task keeps the partition as Iceberg JSON and builds it again on the worker.

use std::sync::Arc;

use beam::transforms::sdf::OffsetRange;
use iceberg::expr::BoundPredicate;
use iceberg::scan::{FileScanTask, FileScanTaskDeleteFile};
use iceberg::spec::{
    DataFileFormat, Literal, NameMapping, PartitionSpec, Schema as TableSchema, StructType, Type,
};
use serde::{Deserialize, Serialize};

/// A `FileScanTask` and the location of its table, which the worker needs for vended
/// credentials.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub(crate) struct ScanTask {
    pub(crate) table_location: String,
    pub(crate) file_size_in_bytes: u64,
    start: u64,
    length: u64,
    record_count: Option<u64>,
    first_row_id: Option<i64>,
    data_sequence_number: Option<i64>,
    pub(crate) data_file_path: String,
    data_file_format: DataFileFormat,
    schema: TableSchema,
    project_field_ids: Vec<i32>,
    predicate: Option<BoundPredicate>,
    pub(crate) deletes: Vec<FileScanTaskDeleteFile>,
    partition: Option<serde_json::Value>,
    partition_spec: Option<PartitionSpec>,
    name_mapping: Option<NameMapping>,
    unified_partition_type: Option<StructType>,
    case_sensitive: bool,
    key_metadata: Option<Vec<u8>>,
}

impl ScanTask {
    /// The task of `task`, a scan task of the table at `table_location`.
    pub(crate) fn new(table_location: &str, task: &FileScanTask) -> beam::Result<Self> {
        let partition = match task.partition() {
            None => None,
            Some(partition) => {
                let partition_type = partition_type(
                    task.partition_spec().map(Arc::as_ref),
                    task.unified_partition_type().map(Arc::as_ref),
                    task.schema(),
                )?;
                Some(Literal::Struct(partition.clone()).try_into_json(&partition_type)?)
            }
        };
        Ok(Self {
            table_location: table_location.to_string(),
            file_size_in_bytes: task.file_size_in_bytes(),
            start: task.start(),
            length: task.length(),
            record_count: task.record_count(),
            first_row_id: task.first_row_id(),
            data_sequence_number: task.data_sequence_number(),
            data_file_path: task.data_file_path().to_string(),
            data_file_format: task.data_file_format(),
            schema: task.schema().clone(),
            project_field_ids: task.project_field_ids().to_vec(),
            predicate: task.predicate().cloned(),
            deletes: task.deletes().to_vec(),
            partition,
            partition_spec: task.partition_spec().map(|spec| (**spec).clone()),
            name_mapping: task.name_mapping().map(|mapping| (**mapping).clone()),
            unified_partition_type: task.unified_partition_type().map(|t| (**t).clone()),
            case_sensitive: task.case_sensitive(),
            key_metadata: task.key_metadata().map(<[u8]>::to_vec),
        })
    }

    /// The byte range of the data file that the task reads.
    pub(crate) fn bytes(&self) -> beam::Result<OffsetRange> {
        let length = match self.length {
            0 => self.file_size_in_bytes.saturating_sub(self.start),
            length => length,
        };
        Ok(OffsetRange::new(
            i64::try_from(self.start)?,
            i64::try_from(self.start.saturating_add(length))?,
        ))
    }

    /// The `FileScanTask` that reads the row groups that start in `[start, start + length)`.
    pub(crate) fn slice(&self, start: u64, length: u64) -> beam::Result<FileScanTask> {
        let partition = match &self.partition {
            None => None,
            Some(json) => {
                let partition_type = partition_type(
                    self.partition_spec.as_ref(),
                    self.unified_partition_type.as_ref(),
                    &self.schema,
                )?;
                match Literal::try_from_json(json.clone(), &partition_type)? {
                    Some(Literal::Struct(value)) => Some(value),
                    None => None,
                    Some(_) => {
                        return Err(beam::Error::msg(
                            "The partition of a scan task is not an Iceberg struct",
                        ));
                    }
                }
            }
        };
        let task = self.clone();
        Ok(FileScanTask::builder()
            .with_file_size_in_bytes(task.file_size_in_bytes)
            .with_start(start)
            .with_length(length)
            .with_record_count(None)
            .with_first_row_id(task.first_row_id)
            .with_data_sequence_number(task.data_sequence_number)
            .with_data_file_path(task.data_file_path)
            .with_data_file_format(task.data_file_format)
            .with_schema(Arc::new(task.schema))
            .with_project_field_ids(task.project_field_ids)
            .with_predicate(task.predicate)
            .with_deletes(task.deletes)
            .with_partition(partition)
            .with_partition_spec(task.partition_spec.map(Arc::new))
            .with_name_mapping(task.name_mapping.map(Arc::new))
            .with_unified_partition_type(task.unified_partition_type.map(Arc::new))
            .with_case_sensitive(task.case_sensitive)
            .with_key_metadata(task.key_metadata.map(Vec::into_boxed_slice))
            .build()?)
    }
}

/// The struct type of the partition of a task.
fn partition_type(
    spec: Option<&PartitionSpec>,
    unified: Option<&StructType>,
    schema: &TableSchema,
) -> beam::Result<Type> {
    let struct_type = match (spec, unified) {
        (Some(spec), _) => spec.partition_type(schema)?,
        (None, Some(unified)) => unified.clone(),
        (None, None) => StructType::new(Vec::new()),
    };
    Ok(Type::Struct(struct_type))
}
