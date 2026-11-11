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

//! What a write does when the table is missing or holds rows.

use std::fmt;
use std::str::FromStr;

/// Whether the write can create the table.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum CreateDisposition {
    /// Creates the table if it does not exist.
    #[default]
    CreateIfNeeded,
    /// Fails the write if the table does not exist.
    CreateNever,
}

impl CreateDisposition {
    /// The BigQuery name, for example `CREATE_IF_NEEDED`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::CreateIfNeeded => "CREATE_IF_NEEDED",
            Self::CreateNever => "CREATE_NEVER",
        }
    }
}

impl fmt::Display for CreateDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CreateDisposition {
    type Err = String;

    /// Parses a name of [`as_str`](Self::as_str), in any case.
    fn from_str(name: &str) -> Result<Self, String> {
        match name.to_ascii_uppercase().as_str() {
            "CREATE_IF_NEEDED" => Ok(Self::CreateIfNeeded),
            "CREATE_NEVER" => Ok(Self::CreateNever),
            _ => Err(format!(
                "Unknown create disposition '{name}': expected CREATE_IF_NEEDED or CREATE_NEVER"
            )),
        }
    }
}

/// What the write does when the table holds rows. The Storage Write API cannot replace the
/// rows of a table, so `WRITE_TRUNCATE` has no value here.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Default)]
pub enum WriteDisposition {
    /// Adds the rows to the table.
    #[default]
    WriteAppend,
    /// Fails the write if the table is not empty.
    WriteEmpty,
}

impl WriteDisposition {
    /// The BigQuery name, for example `WRITE_APPEND`.
    pub fn as_str(&self) -> &'static str {
        match self {
            Self::WriteAppend => "WRITE_APPEND",
            Self::WriteEmpty => "WRITE_EMPTY",
        }
    }
}

impl fmt::Display for WriteDisposition {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for WriteDisposition {
    type Err = String;

    /// Parses a name of [`as_str`](Self::as_str), in any case. `WRITE_TRUNCATE` gives an
    /// error that names Managed I/O.
    fn from_str(name: &str) -> Result<Self, String> {
        match name.to_ascii_uppercase().as_str() {
            "WRITE_APPEND" => Ok(Self::WriteAppend),
            "WRITE_EMPTY" => Ok(Self::WriteEmpty),
            "WRITE_TRUNCATE" => Err(
                "The Storage Write API cannot replace the rows of a table: use WRITE_APPEND or \
                 WRITE_EMPTY, or write with Managed I/O"
                    .to_string(),
            ),
            _ => Err(format!(
                "Unknown write disposition '{name}': expected WRITE_APPEND or WRITE_EMPTY"
            )),
        }
    }
}
