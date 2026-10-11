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

//! Source and sink lineage metrics for data catalog tracking.

use std::borrow::Cow;
use std::sync::Arc;

use super::context::MetricsContainer;
use super::user::{Metrics, StringSet};

/// Metric namespace for source and sink lineage.
pub const LINEAGE_NAMESPACE: &str = "lineage";

/// Metric name for source lineage entries.
pub const LINEAGE_SOURCES: &str = "sources";

/// Metric name for sink lineage entries.
pub const LINEAGE_SINKS: &str = "sinks";

/// Records fully-qualified source or sink resource names into a [`StringSet`] metric.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct Lineage {
    metric: StringSet,
}

impl Lineage {
    /// Handle for source lineage (`lineage:sources`).
    pub fn sources() -> Self {
        Self {
            metric: Metrics::string_set(LINEAGE_NAMESPACE, LINEAGE_SOURCES),
        }
    }

    /// Handle for sink lineage (`lineage:sinks`).
    pub fn sinks() -> Self {
        Self {
            metric: Metrics::string_set(LINEAGE_NAMESPACE, LINEAGE_SINKS),
        }
    }

    /// Returns a copy bound to `container` and `transform_id`.
    pub fn bind(
        &self,
        container: Arc<MetricsContainer>,
        transform_id: impl Into<Arc<str>>,
    ) -> Self {
        Self {
            metric: self.metric.bind(container, transform_id),
        }
    }

    /// Records `{system}:{segments}` with reserved characters escaped in each segment.
    pub fn add<'a>(&self, system: &str, segments: impl IntoIterator<Item = &'a str>) {
        self.metric.add(&Self::format_fqn(system, None, segments));
    }

    /// Records `{system}:{subtype}:{segments}` with reserved characters escaped in each segment.
    pub fn add_with_subtype<'a>(
        &self,
        system: &str,
        subtype: &str,
        segments: impl IntoIterator<Item = &'a str>,
    ) {
        self.metric
            .add(&Self::format_fqn(system, Some(subtype), segments));
    }

    /// Formats a fully-qualified lineage name from `system`, optional `subtype` and `segments`.
    pub fn format_fqn<'a>(
        system: &str,
        subtype: Option<&str>,
        segments: impl IntoIterator<Item = &'a str>,
    ) -> String {
        let joined = segments
            .into_iter()
            .map(wrap_segment)
            .collect::<Vec<_>>()
            .join(".");
        match subtype {
            Some(subtype) => format!("{system}:{subtype}:{joined}"),
            None => format!("{system}:{joined}"),
        }
    }
}

fn is_reserved(ch: char) -> bool {
    matches!(ch, ':' | '.' | '`') || ch.is_whitespace()
}

fn wrap_segment(segment: &str) -> Cow<'_, str> {
    if !segment.chars().any(is_reserved) {
        return Cow::Borrowed(segment);
    }
    let escaped = segment.replace('`', "``");
    Cow::Owned(format!("`{escaped}`"))
}
