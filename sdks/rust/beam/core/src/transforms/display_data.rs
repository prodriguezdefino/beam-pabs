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

//! Display data: static metadata that pipeline components (transforms, DoFns, I/O connectors,
//! pipeline options) publish for runner UIs, monitoring dashboards and diagnostic logs.
//! Components implement [`HasDisplayData`] to add items to a [`DisplayDataBuilder`].

use model::pipeline::{self as proto, labelled_payload};
use prost::Message;
use serde::{Deserialize, Serialize};

/// Runner API URN for labelled display data payloads.
pub const URN_DISPLAY_DATA_LABELLED: &str = "beam:display_data:labelled:v1";

/// One display data item of a pipeline component or option.
#[derive(Debug, Clone, PartialEq, Deserialize)]
#[serde(rename_all = "camelCase")]
pub struct DisplayDataItem {
    /// Identifier of the item.
    pub key: String,
    /// Namespace of the component or option group that owns the item.
    pub namespace: String,
    /// Data type name, for example "STRING", "INTEGER", "BOOLEAN" or "FLOAT".
    #[serde(rename = "type")]
    pub item_type: String,
    /// The value as a string.
    #[serde(deserialize_with = "deserialize_flexible_value")]
    pub value: String,
    /// Optional human-readable label for UI dashboards.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub label: Option<String>,
}

impl Serialize for DisplayDataItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let mut state = serializer.serialize_struct("DisplayDataItem", 5)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("namespace", &self.namespace)?;
        state.serialize_field("type", &self.item_type)?;
        match self.item_type.as_str() {
            "INTEGER" => {
                if let Ok(n) = self.value.parse::<i64>() {
                    state.serialize_field("value", &n)?;
                } else {
                    state.serialize_field("value", &self.value)?;
                }
            }
            "BOOLEAN" => {
                if let Ok(b) = self.value.parse::<bool>() {
                    state.serialize_field("value", &b)?;
                } else {
                    state.serialize_field("value", &self.value)?;
                }
            }
            "FLOAT" | "DOUBLE" => {
                if let Ok(f) = self.value.parse::<f64>() {
                    state.serialize_field("value", &f)?;
                } else {
                    state.serialize_field("value", &self.value)?;
                }
            }
            _ => state.serialize_field("value", &self.value)?,
        }
        if let Some(label) = &self.label {
            state.serialize_field("label", label)?;
        }
        state.end()
    }
}

fn deserialize_flexible_value<'de, D>(deserializer: D) -> Result<String, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let v = serde_json::Value::deserialize(deserializer)?;
    match v {
        serde_json::Value::String(s) => Ok(s),
        serde_json::Value::Number(n) => Ok(n.to_string()),
        serde_json::Value::Bool(b) => Ok(b.to_string()),
        other => Ok(other.to_string()),
    }
}

impl DisplayDataItem {
    /// Creates a STRING display data item.
    pub fn text(
        key: impl Into<String>,
        namespace: impl Into<String>,
        value: impl Into<String>,
    ) -> Self {
        Self {
            key: key.into(),
            namespace: namespace.into(),
            item_type: "STRING".to_string(),
            value: value.into(),
            label: None,
        }
    }

    /// Creates an INTEGER display data item.
    pub fn integer(key: impl Into<String>, namespace: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            namespace: namespace.into(),
            item_type: "INTEGER".to_string(),
            value: value.to_string(),
            label: None,
        }
    }

    /// Creates a BOOLEAN display data item.
    pub fn boolean(key: impl Into<String>, namespace: impl Into<String>, value: bool) -> Self {
        Self {
            key: key.into(),
            namespace: namespace.into(),
            item_type: "BOOLEAN".to_string(),
            value: value.to_string(),
            label: None,
        }
    }

    /// Creates a FLOAT display data item.
    pub fn float(key: impl Into<String>, namespace: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            namespace: namespace.into(),
            item_type: "FLOAT".to_string(),
            value: value.to_string(),
            label: None,
        }
    }

    /// Sets the human-readable label of this item.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Converts this item into a Runner API [`proto::DisplayData`] message. A value that does not
    /// parse as its declared type is sent as a string.
    pub fn to_proto(&self) -> proto::DisplayData {
        let value = match self.item_type.as_str() {
            "BOOLEAN" => self
                .value
                .parse::<bool>()
                .ok()
                .map(labelled_payload::Value::BoolValue),
            "INTEGER" => self
                .value
                .parse::<i64>()
                .ok()
                .map(labelled_payload::Value::IntValue),
            "FLOAT" | "DOUBLE" => self
                .value
                .parse::<f64>()
                .ok()
                .map(labelled_payload::Value::DoubleValue),
            _ => Some(labelled_payload::Value::StringValue(self.value.clone())),
        }
        .or_else(|| Some(labelled_payload::Value::StringValue(self.value.clone())));

        let payload = proto::LabelledPayload {
            label: self.label.clone().unwrap_or_default(),
            key: self.key.clone(),
            namespace: self.namespace.clone(),
            value,
        };

        proto::DisplayData {
            urn: URN_DISPLAY_DATA_LABELLED.to_string(),
            payload: payload.encode_to_vec(),
        }
    }

    /// Converts a Runner API [`proto::DisplayData`] message. For a URN other than
    /// [`URN_DISPLAY_DATA_LABELLED`], the item has the URN as key, the namespace `"urn"` and the
    /// payload as lossy UTF-8 text. Returns an error if a labelled payload does not decode.
    pub fn from_proto(proto: &proto::DisplayData) -> Result<Self, prost::DecodeError> {
        if proto.urn == URN_DISPLAY_DATA_LABELLED {
            let labelled = proto::LabelledPayload::decode(&*proto.payload)?;
            let (item_type, value) = match labelled.value {
                Some(labelled_payload::Value::BoolValue(b)) => {
                    ("BOOLEAN".to_string(), b.to_string())
                }
                Some(labelled_payload::Value::IntValue(i)) => {
                    ("INTEGER".to_string(), i.to_string())
                }
                Some(labelled_payload::Value::DoubleValue(f)) => {
                    ("FLOAT".to_string(), f.to_string())
                }
                Some(labelled_payload::Value::StringValue(s)) => ("STRING".to_string(), s),
                None => ("STRING".to_string(), String::new()),
            };

            let label = if labelled.label.is_empty() {
                None
            } else {
                Some(labelled.label)
            };

            Ok(Self {
                key: labelled.key,
                namespace: labelled.namespace,
                item_type,
                value,
                label,
            })
        } else {
            Ok(Self {
                key: proto.urn.clone(),
                namespace: "urn".to_string(),
                item_type: "STRING".to_string(),
                value: String::from_utf8_lossy(&proto.payload).to_string(),
                label: None,
            })
        }
    }
}

/// Collects the display data items of a component.
#[derive(Debug, Clone, Default, PartialEq)]
pub struct DisplayDataBuilder {
    default_namespace: String,
    items: Vec<DisplayDataItem>,
}

impl DisplayDataBuilder {
    /// Creates an empty display data builder.
    pub fn new() -> Self {
        Self::default()
    }

    /// Creates a builder with the given default namespace.
    pub fn with_namespace(namespace: impl Into<String>) -> Self {
        Self {
            default_namespace: namespace.into(),
            items: Vec::new(),
        }
    }

    /// Returns the current default namespace.
    pub fn namespace(&self) -> &str {
        &self.default_namespace
    }

    /// Sets the default namespace for the items that are added after this call.
    pub fn set_namespace(&mut self, namespace: impl Into<String>) -> &mut Self {
        self.default_namespace = namespace.into();
        self
    }

    /// Adds a string item using the builder's default namespace.
    pub fn add_text(&mut self, key: impl Into<String>, value: impl Into<String>) -> &mut Self {
        self.items
            .push(DisplayDataItem::text(key, &self.default_namespace, value));
        self
    }

    /// Adds a string item with a human-readable label.
    pub fn add_text_with_label(
        &mut self,
        key: impl Into<String>,
        value: impl Into<String>,
        label: impl Into<String>,
    ) -> &mut Self {
        self.items
            .push(DisplayDataItem::text(key, &self.default_namespace, value).with_label(label));
        self
    }

    /// Adds an integer item using the builder's default namespace.
    pub fn add_integer(&mut self, key: impl Into<String>, value: i64) -> &mut Self {
        self.items.push(DisplayDataItem::integer(
            key,
            &self.default_namespace,
            value,
        ));
        self
    }

    /// Adds an integer item with a human-readable label.
    pub fn add_integer_with_label(
        &mut self,
        key: impl Into<String>,
        value: i64,
        label: impl Into<String>,
    ) -> &mut Self {
        self.items
            .push(DisplayDataItem::integer(key, &self.default_namespace, value).with_label(label));
        self
    }

    /// Adds a boolean item using the builder's default namespace.
    pub fn add_boolean(&mut self, key: impl Into<String>, value: bool) -> &mut Self {
        self.items.push(DisplayDataItem::boolean(
            key,
            &self.default_namespace,
            value,
        ));
        self
    }

    /// Adds a boolean item with a human-readable label.
    pub fn add_boolean_with_label(
        &mut self,
        key: impl Into<String>,
        value: bool,
        label: impl Into<String>,
    ) -> &mut Self {
        self.items
            .push(DisplayDataItem::boolean(key, &self.default_namespace, value).with_label(label));
        self
    }

    /// Adds a floating point item using the builder's default namespace.
    pub fn add_float(&mut self, key: impl Into<String>, value: f64) -> &mut Self {
        self.items
            .push(DisplayDataItem::float(key, &self.default_namespace, value));
        self
    }

    /// Adds a floating point item with a human-readable label.
    pub fn add_float_with_label(
        &mut self,
        key: impl Into<String>,
        value: f64,
        label: impl Into<String>,
    ) -> &mut Self {
        self.items
            .push(DisplayDataItem::float(key, &self.default_namespace, value).with_label(label));
        self
    }

    /// Adds a display data item as it is.
    pub fn add_item(&mut self, item: DisplayDataItem) -> &mut Self {
        self.items.push(item);
        self
    }

    /// Returns the collected items.
    pub fn items(&self) -> &[DisplayDataItem] {
        &self.items
    }

    /// Consumes the builder and returns the collected items.
    pub fn build(self) -> Vec<DisplayDataItem> {
        self.items
    }

    /// Converts all collected items into Runner API [`proto::DisplayData`] messages.
    pub fn into_proto(self) -> Vec<proto::DisplayData> {
        self.items.iter().map(DisplayDataItem::to_proto).collect()
    }
}

/// A pipeline component (transform, DoFn, option group) that publishes static display data.
pub trait HasDisplayData {
    /// Adds the display data items of this component to `builder`.
    fn populate_display_data(&self, _builder: &mut DisplayDataBuilder) {}
}
