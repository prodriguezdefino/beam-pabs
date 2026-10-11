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

//! The typed options of a job, in the form that workers receive.
//!
//! An [`OptionsSnapshot`] holds the serde-serialized option groups that a driver parsed
//! with clap, keyed by group. Workers, display data and the flat view for portable runners
//! all come from the snapshot, never from parsing argument strings.

use std::collections::BTreeMap;
use std::collections::btree_map::Entry;
use std::path::Path;

use serde::de::DeserializeOwned;
use serde::{Deserialize, Serialize};
use serde_json::{Map, Value};

use crate::options::groups::OptionsError;
use crate::transforms::display_data::DisplayDataItem;

/// Pipeline option that sends the [`OptionsSnapshot`] of a job to its workers.
pub const SDK_OPTIONS_OPTION: &str = "rust_options";

/// The values of one option group, keyed by field name.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq)]
pub struct GroupSnapshot {
    namespace: String,
    values: Map<String, Value>,
}

impl GroupSnapshot {
    /// Serializes `group`, which must serialize to a JSON object. Display data uses
    /// `namespace`.
    pub fn of<G: Serialize>(namespace: &str, group: &G) -> Result<Self, OptionsError> {
        match serde_json::to_value(group) {
            Ok(Value::Object(values)) => Ok(Self {
                namespace: namespace.to_string(),
                values,
            }),
            Ok(other) => Err(OptionsError::Snapshot {
                message: format!(
                    "option group '{}' must serialize to a JSON object, got {other}",
                    std::any::type_name::<G>()
                ),
            }),
            Err(e) => Err(OptionsError::Snapshot {
                message: format!(
                    "option group '{}' failed to serialize: {e}",
                    std::any::type_name::<G>()
                ),
            }),
        }
    }

    /// Deserializes the group as `G`.
    pub fn restore<G: DeserializeOwned>(&self) -> Result<G, OptionsError> {
        serde_json::from_value(Value::Object(self.values.clone())).map_err(|e| {
            OptionsError::Snapshot {
                message: format!(
                    "option group '{}' failed to deserialize: {e}",
                    std::any::type_name::<G>()
                ),
            }
        })
    }

    /// Returns the display data namespace of this group.
    pub fn namespace(&self) -> &str {
        &self.namespace
    }

    /// Returns the values of the group, keyed by field name.
    pub fn values(&self) -> &Map<String, Value> {
        &self.values
    }

    /// `null` and empty lists mean "unset".
    fn set_values(&self) -> impl Iterator<Item = (&String, &Value)> {
        self.values.iter().filter(|(_, value)| is_set(value))
    }

    fn display_items(&self) -> impl Iterator<Item = DisplayDataItem> + '_ {
        self.set_values()
            .map(|(key, value)| display_item(key, &self.namespace, value))
    }
}

/// The typed options of a job, keyed by option group.
#[derive(Serialize, Deserialize, Clone, Debug, PartialEq, Default)]
#[serde(transparent)]
pub struct OptionsSnapshot {
    groups: BTreeMap<String, GroupSnapshot>,
}

impl OptionsSnapshot {
    /// Collects group snapshots, keyed by group.
    pub fn from_groups(groups: impl IntoIterator<Item = (String, GroupSnapshot)>) -> Self {
        Self {
            groups: groups.into_iter().collect(),
        }
    }

    /// Returns the snapshot of the group with key `key`, if the driver recorded one.
    pub fn group(&self, key: &str) -> Option<&GroupSnapshot> {
        self.groups.get(key)
    }

    /// Returns all groups in key order.
    pub fn groups(&self) -> impl Iterator<Item = (&str, &GroupSnapshot)> {
        self.groups.iter().map(|(key, group)| (key.as_str(), group))
    }

    /// Returns display data for each set option, typed by its JSON kind.
    pub fn display_data(&self) -> Vec<DisplayDataItem> {
        self.groups
            .values()
            .flat_map(GroupSnapshot::display_items)
            .collect()
    }

    /// Returns each set option keyed by field name only, the form that portable runners and
    /// harnesses of other SDKs read. Two groups can declare the same option; if their values
    /// differ, returns [`OptionsError::Conflict`] and does not choose one.
    pub fn flat_options(&self) -> Result<Map<String, Value>, OptionsError> {
        self.groups
            .values()
            .flat_map(GroupSnapshot::set_values)
            .try_fold(BTreeMap::new(), |mut acc, (key, value)| {
                match acc.entry(key.clone()) {
                    Entry::Vacant(slot) => {
                        slot.insert(value.clone());
                    }
                    Entry::Occupied(existing) if existing.get() == value => {}
                    Entry::Occupied(existing) => {
                        return Err(OptionsError::Conflict {
                            key: key.clone(),
                            first: existing.get().to_string(),
                            second: value.to_string(),
                        });
                    }
                }
                Ok(acc)
            })
            .map(|flat| flat.into_iter().collect())
    }

    /// Encodes this snapshot as a JSON *string* for transport, not as a nested protobuf
    /// `Struct`: all `Struct` numbers are `f64`, so an integer option would arrive as `32.0`
    /// and fail to deserialize into its `usize` field.
    pub fn encode(&self) -> String {
        serde_json::to_string(self).expect("an options snapshot is plain JSON")
    }

    /// Decodes a snapshot that [`encode`](Self::encode) produced.
    pub fn decode(encoded: &str) -> Result<Self, OptionsError> {
        serde_json::from_str(encoded).map_err(|e| OptionsError::Snapshot {
            message: format!("malformed options snapshot: {e}"),
        })
    }

    /// Reads a snapshot from a file that the container boot program wrote.
    pub fn load(path: &Path) -> Result<Self, OptionsError> {
        std::fs::read_to_string(path)
            .map_err(|e| OptionsError::Snapshot {
                message: format!("cannot read options snapshot {}: {e}", path.display()),
            })
            .and_then(|encoded| Self::decode(&encoded))
    }
}

fn is_set(value: &Value) -> bool {
    match value {
        Value::Null => false,
        Value::Array(items) => !items.is_empty(),
        _ => true,
    }
}

fn display_item(key: &str, namespace: &str, value: &Value) -> DisplayDataItem {
    match value {
        Value::Bool(b) => DisplayDataItem::boolean(key, namespace, *b),
        Value::Number(n) => n
            .as_i64()
            .map(|i| DisplayDataItem::integer(key, namespace, i))
            .unwrap_or_else(|| {
                DisplayDataItem::float(key, namespace, n.as_f64().unwrap_or_default())
            }),
        Value::String(s) => DisplayDataItem::text(key, namespace, s),
        Value::Array(items) => DisplayDataItem::text(
            key,
            namespace,
            items.iter().map(scalar_text).collect::<Vec<_>>().join(","),
        ),
        other => DisplayDataItem::text(key, namespace, other.to_string()),
    }
}

fn scalar_text(value: &Value) -> String {
    match value {
        Value::String(s) => s.clone(),
        other => other.to_string(),
    }
}
