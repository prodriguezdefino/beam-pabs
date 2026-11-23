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

//! Where and how a batch read reads an Iceberg table.

use beam::transforms::display_data::DisplayDataBuilder;
use beam_io_arrow::FieldSelection;
use iceberg::TableIdent;
use iceberg::expr::Predicate;

use crate::catalog::{Catalog, table_ident};

/// The rows in each Arrow batch that the read decodes, when the options set none.
pub const DEFAULT_READ_BATCH_SIZE: usize = 8_192;

/// The snapshot that a read reads.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub(crate) enum Snapshot {
    /// The current snapshot of the table.
    #[default]
    Current,
    /// The snapshot with this id.
    Id(i64),
    /// The last snapshot at or before this time, in milliseconds since the epoch.
    AsOf(i64),
}

/// Where and how to read an Iceberg table.
#[derive(Clone, Debug, PartialEq)]
pub struct ReadOptions {
    pub(crate) catalog: Catalog,
    pub(crate) table: String,
    pub(crate) columns: FieldSelection,
    pub(crate) filter: Option<Predicate>,
    pub(crate) snapshot: Snapshot,
    pub(crate) batch_size: usize,
}

impl ReadOptions {
    /// Reads the table `table` of `catalog`. `table` is `namespace.name`, and a nested
    /// namespace has more parts: `a.b.name`.
    pub fn new(catalog: Catalog, table: impl Into<String>) -> Self {
        Self {
            catalog,
            table: table.into(),
            columns: FieldSelection::All,
            filter: None,
            snapshot: Snapshot::Current,
            batch_size: DEFAULT_READ_BATCH_SIZE,
        }
    }

    /// Reads only these columns. Replaces an earlier column selection.
    pub fn with_keep<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = FieldSelection::Keep(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Reads all columns except these. Replaces an earlier column selection.
    pub fn with_drop<I, S>(mut self, columns: I) -> Self
    where
        I: IntoIterator<Item = S>,
        S: Into<String>,
    {
        self.columns = FieldSelection::Drop(columns.into_iter().map(Into::into).collect());
        self
    }

    /// Reads only the rows that match `filter`. The scan also skips the files and row groups
    /// whose statistics do not match.
    pub fn with_filter(mut self, filter: Predicate) -> Self {
        self.filter = Some(filter);
        self
    }

    /// Reads the snapshot with this id. Replaces an earlier snapshot choice.
    pub fn with_snapshot_id(mut self, snapshot_id: i64) -> Self {
        self.snapshot = Snapshot::Id(snapshot_id);
        self
    }

    /// Reads the last snapshot at or before `millis` milliseconds since the epoch. Replaces
    /// an earlier snapshot choice.
    pub fn with_as_of_timestamp_millis(mut self, millis: i64) -> Self {
        self.snapshot = Snapshot::AsOf(millis);
        self
    }

    /// The rows in each Arrow batch that the read decodes. Default: 8,192.
    pub fn with_batch_size(mut self, rows: usize) -> Self {
        self.batch_size = rows;
        self
    }

    /// The identifier of the table.
    ///
    /// # Errors
    ///
    /// Fails if the table has no namespace or has an empty part.
    pub fn table_ident(&self) -> beam::Result<TableIdent> {
        table_ident(&self.table)
    }

    /// Checks the options that need no catalog call.
    ///
    /// # Errors
    ///
    /// Fails if the table name is not valid or the batch size is zero.
    pub fn check(&self) -> beam::Result {
        self.table_ident()?;
        if self.batch_size == 0 {
            return Err(beam::Error::msg(
                "The batch size of the read must be greater than zero",
            ));
        }
        Ok(())
    }

    pub(crate) fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("catalog_uri", &self.catalog.uri);
        builder.add_text("table", &self.table);
        let columns = match &self.columns {
            FieldSelection::Keep(columns) => Some(("keep", columns.join(","))),
            FieldSelection::Drop(columns) => Some(("drop", columns.join(","))),
            FieldSelection::All | FieldSelection::Only(_) => None,
        };
        if let Some((key, value)) = columns {
            builder.add_text(key, value);
        }
        if let Some(filter) = &self.filter {
            builder.add_text("filter", filter.to_string());
        }
        match self.snapshot {
            Snapshot::Current => {}
            Snapshot::Id(id) => {
                builder.add_integer("snapshot_id", id);
            }
            Snapshot::AsOf(millis) => {
                builder.add_integer("as_of_timestamp_millis", millis);
            }
        }
    }
}
