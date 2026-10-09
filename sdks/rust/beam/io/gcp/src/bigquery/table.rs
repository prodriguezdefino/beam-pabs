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

//! BigQuery table references, shared by the reads and the writes.

use std::io;

/// A BigQuery table, from a table spec `[project:]dataset.table` or `project.dataset.table`.
/// A project ID can hold a domain, as in `example.com:project:dataset.table`.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct TableRef {
    pub project: Option<String>,
    pub dataset: String,
    pub table: String,
}

impl TableRef {
    /// Parses a table spec. Returns [`io::ErrorKind::InvalidInput`] for other forms.
    pub fn parse(spec: &str) -> io::Result<Self> {
        let invalid = || {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid table spec '{spec}': expected [project:]dataset.table"),
            )
        };
        let (rest, table) = spec.rsplit_once('.').ok_or_else(invalid)?;
        let (project, dataset) = match rest.rfind([':', '.']) {
            Some(i) => (Some(&rest[..i]), &rest[i + 1..]),
            None => (None, rest),
        };
        if table.is_empty() || dataset.is_empty() || project.is_some_and(str::is_empty) {
            return Err(invalid());
        }
        Ok(Self {
            project: project.map(str::to_string),
            dataset: dataset.to_string(),
            table: table.to_string(),
        })
    }

    /// The resource name that the Storage Read API takes, with `default_project` for a spec
    /// without a project.
    pub fn resource_name(&self, default_project: Option<&str>) -> io::Result<String> {
        let project = self.project.as_deref().or(default_project).ok_or_else(|| {
            io::Error::new(
                io::ErrorKind::InvalidInput,
                format!(
                    "Table '{}.{}' has no project. Name it in the table spec or set a billing project",
                    self.dataset, self.table
                ),
            )
        })?;
        Ok(format!(
            "projects/{project}/datasets/{}/tables/{}",
            self.dataset, self.table
        ))
    }
}
