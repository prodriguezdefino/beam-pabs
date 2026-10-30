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

//! The options of a BigQuery write.

use std::time::Duration;

use super::projection::FieldSelection;
use crate::bigquery::table::TableRef;
use crate::bigquery::{CreateDisposition, WriteDisposition};

/// How long an append waits for its answer when the options set no time, as the Storage Write
/// client of the Java SDK waits for a request callback.
const DEFAULT_APPEND_TIMEOUT: Duration = Duration::from_secs(5 * 60);

/// How many times a row can arrive in the table.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Semantics {
    /// Each row arrives once. The write commits or flushes streams, and an unbounded write
    /// costs a shuffle.
    #[default]
    ExactlyOnce,
    /// A retried bundle can write its rows again. The write has no shuffle and the rows are
    /// visible at once.
    AtLeastOnce,
}

/// Where and how to write.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct WriteOptions {
    pub(super) table_spec: String,
    pub(super) create_disposition: CreateDisposition,
    pub(super) write_disposition: WriteDisposition,
    pub(super) semantics: Semantics,
    pub(super) triggering_frequency: Option<Duration>,
    pub(super) num_streams: u32,
    pub(super) keep: Option<Vec<String>>,
    pub(super) drop: Option<Vec<String>>,
    pub(super) only: Option<String>,
    pub(super) ignore_unknown_values: bool,
    pub(super) append_timeout: Duration,
    pub(super) kms_key: Option<String>,
    pub(super) clustering_fields: Vec<String>,
}

impl WriteOptions {
    /// Writes to the table `table_spec` (`[project:]dataset.table` or `project.dataset.table`).
    /// A spec without a project uses the `--project` of the pipeline.
    pub fn new(table_spec: impl Into<String>) -> Self {
        Self {
            table_spec: table_spec.into(),
            create_disposition: CreateDisposition::CreateIfNeeded,
            write_disposition: WriteDisposition::WriteAppend,
            semantics: Semantics::ExactlyOnce,
            triggering_frequency: None,
            num_streams: 0,
            keep: None,
            drop: None,
            only: None,
            ignore_unknown_values: false,
            append_timeout: DEFAULT_APPEND_TIMEOUT,
            kms_key: None,
            clustering_fields: Vec::new(),
        }
    }

    /// The default is [`CreateDisposition::CreateIfNeeded`].
    pub fn with_create_disposition(mut self, disposition: CreateDisposition) -> Self {
        self.create_disposition = disposition;
        self
    }

    /// The default is [`WriteDisposition::WriteAppend`]. The Storage Write API cannot replace
    /// the rows of a table, so [`validate`](Self::validate) rejects
    /// [`WriteDisposition::WriteTruncate`].
    pub fn with_write_disposition(mut self, disposition: WriteDisposition) -> Self {
        self.write_disposition = disposition;
        self
    }

    /// The default is [`Semantics::ExactlyOnce`].
    pub fn with_semantics(mut self, semantics: Semantics) -> Self {
        self.semantics = semantics;
        self
    }

    /// How often an exactly-once write of an unbounded input makes its rows visible. Setting
    /// it also selects the unbounded shape for a bounded input, as in `BigQueryIO` of the Java
    /// SDK. The default is 5 seconds.
    pub fn with_triggering_frequency(mut self, frequency: Duration) -> Self {
        self.triggering_frequency = Some(frequency);
        self
    }

    /// The shards of an exactly-once write of an unbounded input, one buffered stream each.
    /// The default, `0`, shards automatically: one shard for each DoFn copy that converts rows,
    /// as the auto-sharding of the Java SDK gives one for each worker thread.
    pub fn with_num_streams(mut self, count: u32) -> Self {
        self.num_streams = count;
        self
    }

    /// Writes only the input fields with these names. Set at most one of `keep`, `drop` and
    /// `only`.
    pub fn with_keep<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.keep = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Writes all input fields except the fields with these names.
    pub fn with_drop<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.drop = Some(fields.into_iter().map(Into::into).collect());
        self
    }

    /// Writes the fields of the input row field `field`, as the columns of the table. An input
    /// row where this field is null goes to the error output.
    pub fn with_only(mut self, field: impl Into<String>) -> Self {
        self.only = Some(field.into());
        self
    }

    /// Does not write the selected fields that the table has no column for. Without it, such
    /// a field fails the write. The rows that go to the error output keep these fields.
    pub fn with_ignore_unknown_values(mut self, ignore: bool) -> Self {
        self.ignore_unknown_values = ignore;
        self
    }

    /// How long an append waits for its answer. After this time, the writer opens a new
    /// connection and sends the append again. The default is 5 minutes.
    pub fn with_append_timeout(mut self, timeout: Duration) -> Self {
        self.append_timeout = timeout;
        self
    }

    /// The Cloud KMS key that encrypts a table that the write creates.
    pub fn with_kms_key(mut self, kms_key: impl Into<String>) -> Self {
        self.kms_key = Some(kms_key.into());
        self
    }

    /// The clustering columns of a table that the write creates.
    pub fn with_clustering_fields<I, S>(mut self, fields: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.clustering_fields = fields.into_iter().map(Into::into).collect();
        self
    }

    /// Checks the options without a call to BigQuery.
    pub fn validate(&self) -> beam::Result {
        TableRef::parse(&self.table_spec).map_err(beam::Error::from)?;
        self.field_selection()?;
        match (self.write_disposition, self.triggering_frequency) {
            (WriteDisposition::WriteTruncate, _) => Err(beam::Error::msg(
                "The Storage Write API cannot replace the rows of a table: use WRITE_APPEND or WRITE_EMPTY",
            )),
            (_, Some(frequency)) if frequency.is_zero() => Err(beam::Error::msg(
                "The triggering frequency of a BigQuery write must be positive",
            )),
            _ if self.append_timeout.is_zero() => Err(beam::Error::msg(
                "The append timeout of a BigQuery write must be positive",
            )),
            _ => Ok(()),
        }
    }
}

impl WriteOptions {
    /// The fields that `keep`, `drop` or `only` select. Fails if more than one is set.
    #[doc(hidden)]
    pub fn field_selection(&self) -> beam::Result<FieldSelection> {
        match (&self.keep, &self.drop, &self.only) {
            (None, None, None) => Ok(FieldSelection::All),
            (Some(keep), None, None) => Ok(FieldSelection::Keep(keep.clone())),
            (None, Some(drop), None) => Ok(FieldSelection::Drop(drop.clone())),
            (None, None, Some(only)) => Ok(FieldSelection::Only(only.clone())),
            _ => Err(beam::Error::msg(
                "Set at most one of keep, drop and only for a BigQuery write",
            )),
        }
    }
}
