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

//! Bigtable read and write targets (tables, materialized views, authorized views) and their
//! `x-goog-request-params` routing headers.

use std::fmt::Write as _;

use serde::{Deserialize, Serialize};

use super::proto::{MutateRowsRequest, ReadRowsRequest, SampleRowKeysRequest};

/// The Bigtable resource that a read targets.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum ReadTarget {
    /// Reads from `projects/{project}/instances/{instance}/tables/{table}`.
    Table {
        project: String,
        instance: String,
        table: String,
    },
    /// Reads from `projects/{project}/instances/{instance}/materializedViews/{view}`.
    MaterializedView {
        project: String,
        instance: String,
        view: String,
    },
    /// Reads from `projects/{project}/instances/{instance}/tables/{table}/authorizedViews/{view}`.
    AuthorizedView {
        project: String,
        instance: String,
        table: String,
        view: String,
    },
}

impl ReadTarget {
    /// Creates a target for a table.
    pub fn table(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self::Table {
            project: project.into(),
            instance: instance.into(),
            table: table.into(),
        }
    }

    /// Creates a target for a materialized view.
    pub fn materialized_view(
        project: impl Into<String>,
        instance: impl Into<String>,
        view: impl Into<String>,
    ) -> Self {
        Self::MaterializedView {
            project: project.into(),
            instance: instance.into(),
            view: view.into(),
        }
    }

    /// Creates a target for an authorized view of a table.
    pub fn authorized_view(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
        view: impl Into<String>,
    ) -> Self {
        Self::AuthorizedView {
            project: project.into(),
            instance: instance.into(),
            table: table.into(),
            view: view.into(),
        }
    }

    /// Checks that all resource identifiers of the target are non-empty.
    pub fn validate(&self) -> beam::Result {
        match self {
            Self::Table {
                project,
                instance,
                table,
            } => {
                require_non_empty("project_id", project)?;
                require_non_empty("instance_id", instance)?;
                require_non_empty("table_id", table)
            }
            Self::MaterializedView {
                project,
                instance,
                view,
            } => {
                require_non_empty("project_id", project)?;
                require_non_empty("instance_id", instance)?;
                require_non_empty("materialized_view_id", view)
            }
            Self::AuthorizedView {
                project,
                instance,
                table,
                view,
            } => {
                require_non_empty("project_id", project)?;
                require_non_empty("instance_id", instance)?;
                require_non_empty("table_id", table)?;
                require_non_empty("authorized_view_id", view)
            }
        }
    }

    /// Returns the full resource name of the target.
    pub fn resource_name(&self) -> String {
        match self {
            Self::Table {
                project,
                instance,
                table,
            } => format!("projects/{project}/instances/{instance}/tables/{table}"),
            Self::MaterializedView {
                project,
                instance,
                view,
            } => format!("projects/{project}/instances/{instance}/materializedViews/{view}"),
            Self::AuthorizedView {
                project,
                instance,
                table,
                view,
            } => format!(
                "projects/{project}/instances/{instance}/tables/{table}/authorizedViews/{view}"
            ),
        }
    }

    /// Returns the `x-goog-request-params` header value for `self` and `app_profile`.
    pub fn routing_header(&self, app_profile: Option<&str>) -> String {
        let resource = match self {
            Self::Table {
                project,
                instance,
                table,
            }
            | Self::AuthorizedView {
                project,
                instance,
                table,
                ..
            } => {
                let table_name = format!("projects/{project}/instances/{instance}/tables/{table}");
                format!("table_name={}", percent_encode(&table_name))
            }
            Self::MaterializedView {
                project, instance, ..
            } => {
                let instance_name = format!("projects/{project}/instances/{instance}");
                format!("name={}", percent_encode(&instance_name))
            }
        };
        match app_profile.filter(|p| !p.is_empty()) {
            Some(profile) => format!("{resource}&app_profile_id={}", percent_encode(profile)),
            None => resource,
        }
    }

    /// Sets the target resource field and `app_profile` on a [`SampleRowKeysRequest`].
    pub fn apply_to_sample(&self, req: &mut SampleRowKeysRequest, app_profile: Option<&str>) {
        if let Some(profile) = app_profile.filter(|p| !p.is_empty()) {
            req.app_profile_id = profile.to_string();
        }
        let name = self.resource_name();
        match self {
            Self::Table { .. } => req.table_name = name,
            Self::MaterializedView { .. } => req.materialized_view_name = name,
            Self::AuthorizedView { .. } => req.authorized_view_name = name,
        }
    }

    /// Sets the target resource field and `app_profile` on a [`ReadRowsRequest`].
    pub fn apply_to_read(&self, req: &mut ReadRowsRequest, app_profile: Option<&str>) {
        if let Some(profile) = app_profile.filter(|p| !p.is_empty()) {
            req.app_profile_id = profile.to_string();
        }
        let name = self.resource_name();
        match self {
            Self::Table { .. } => req.table_name = name,
            Self::MaterializedView { .. } => req.materialized_view_name = name,
            Self::AuthorizedView { .. } => req.authorized_view_name = name,
        }
    }
}

/// The Bigtable resource that a write targets. Materialized views are read-only.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub enum WriteTarget {
    /// Writes to `projects/{project}/instances/{instance}/tables/{table}`.
    Table {
        project: String,
        instance: String,
        table: String,
    },
    /// Writes to `projects/{project}/instances/{instance}/tables/{table}/authorizedViews/{view}`.
    AuthorizedView {
        project: String,
        instance: String,
        table: String,
        view: String,
    },
}

impl WriteTarget {
    /// Creates a write target for a table.
    pub fn table(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
    ) -> Self {
        Self::Table {
            project: project.into(),
            instance: instance.into(),
            table: table.into(),
        }
    }

    /// Creates a write target for an authorized view of a table.
    pub fn authorized_view(
        project: impl Into<String>,
        instance: impl Into<String>,
        table: impl Into<String>,
        view: impl Into<String>,
    ) -> Self {
        Self::AuthorizedView {
            project: project.into(),
            instance: instance.into(),
            table: table.into(),
            view: view.into(),
        }
    }

    /// Checks that all resource identifiers of the target are non-empty.
    pub fn validate(&self) -> beam::Result {
        match self {
            Self::Table {
                project,
                instance,
                table,
            } => {
                require_non_empty("project_id", project)?;
                require_non_empty("instance_id", instance)?;
                require_non_empty("table_id", table)
            }
            Self::AuthorizedView {
                project,
                instance,
                table,
                view,
            } => {
                require_non_empty("project_id", project)?;
                require_non_empty("instance_id", instance)?;
                require_non_empty("table_id", table)?;
                require_non_empty("authorized_view_id", view)
            }
        }
    }

    /// Returns the full resource name of the target.
    pub fn resource_name(&self) -> String {
        match self {
            Self::Table {
                project,
                instance,
                table,
            } => format!("projects/{project}/instances/{instance}/tables/{table}"),
            Self::AuthorizedView {
                project,
                instance,
                table,
                view,
            } => format!(
                "projects/{project}/instances/{instance}/tables/{table}/authorizedViews/{view}"
            ),
        }
    }

    /// Returns the `x-goog-request-params` header value for `self` and `app_profile`.
    pub fn routing_header(&self, app_profile: Option<&str>) -> String {
        let (Self::Table {
            project,
            instance,
            table,
        }
        | Self::AuthorizedView {
            project,
            instance,
            table,
            ..
        }) = self;
        let table_name = format!("projects/{project}/instances/{instance}/tables/{table}");
        let resource = format!("table_name={}", percent_encode(&table_name));
        match app_profile.filter(|p| !p.is_empty()) {
            Some(profile) => format!("{resource}&app_profile_id={}", percent_encode(profile)),
            None => resource,
        }
    }

    /// Sets the target resource field and `app_profile` on a [`MutateRowsRequest`].
    pub fn apply_to_mutate(&self, req: &mut MutateRowsRequest, app_profile: Option<&str>) {
        if let Some(profile) = app_profile.filter(|p| !p.is_empty()) {
            req.app_profile_id = profile.to_string();
        }
        let name = self.resource_name();
        match self {
            Self::Table { .. } => req.table_name = name,
            Self::AuthorizedView { .. } => req.authorized_view_name = name,
        }
    }
}

fn require_non_empty(field: &str, value: &str) -> beam::Result {
    if value.is_empty() {
        Err(beam::Error::msg(format!("{field} must not be empty")))
    } else {
        Ok(())
    }
}

/// Percent-encodes `input` for `x-goog-request-params` according to RFC 3986 unreserved characters.
pub fn percent_encode(input: &str) -> String {
    input
        .bytes()
        .fold(String::with_capacity(input.len() + 8), |mut out, b| {
            match b {
                b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => {
                    out.push(b as char);
                }
                _ => {
                    let _ = write!(out, "%{b:02X}");
                }
            }
            out
        })
}
