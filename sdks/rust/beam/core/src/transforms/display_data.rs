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

use std::fmt;

use model::pipeline::{self as proto, labelled_payload};
use prost::Message;
use serde::{Deserialize, Serialize};

/// Runner API URN for labelled display data payloads.
pub const URN_DISPLAY_DATA_LABELLED: &str = "beam:display_data:labelled:v1";

/// The typed value of a display data item.
#[derive(Debug, Clone, PartialEq)]
pub enum DisplayDataValue {
    /// Text value.
    Text(String),
    /// 64-bit signed integer value.
    Integer(i64),
    /// Boolean value.
    Boolean(bool),
    /// 64-bit float value.
    Float(f64),
}

impl DisplayDataValue {
    /// Returns the Runner API data type name for this value.
    pub const fn type_name(&self) -> &'static str {
        match self {
            Self::Text(_) => "STRING",
            Self::Integer(_) => "INTEGER",
            Self::Boolean(_) => "BOOLEAN",
            Self::Float(_) => "FLOAT",
        }
    }

    /// Returns a `&str` slice if this is a text value.
    pub fn as_str(&self) -> Option<&str> {
        match self {
            Self::Text(s) => Some(s.as_str()),
            _ => None,
        }
    }
}

impl fmt::Display for DisplayDataValue {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            Self::Text(s) => f.write_str(s),
            Self::Integer(i) => write!(f, "{i}"),
            Self::Boolean(b) => write!(f, "{b}"),
            Self::Float(fl) => write!(f, "{fl}"),
        }
    }
}

impl PartialEq<str> for DisplayDataValue {
    fn eq(&self, other: &str) -> bool {
        match self {
            Self::Text(s) => s.as_str() == other,
            _ => false,
        }
    }
}

impl PartialEq<&str> for DisplayDataValue {
    fn eq(&self, other: &&str) -> bool {
        self == *other
    }
}

/// One display data item of a pipeline component or option.
#[derive(Debug, Clone, PartialEq)]
pub struct DisplayDataItem {
    /// Identifier of the item.
    pub key: String,
    /// Namespace of the component or option group that owns the item.
    pub namespace: String,
    /// The typed value.
    pub value: DisplayDataValue,
    /// Optional human-readable label for UI dashboards.
    pub label: Option<String>,
}

impl DisplayDataItem {
    /// Returns the data type name of this item.
    pub fn item_type(&self) -> &'static str {
        self.value.type_name()
    }
}

impl Serialize for DisplayDataItem {
    fn serialize<S>(&self, serializer: S) -> Result<S::Ok, S::Error>
    where
        S: serde::Serializer,
    {
        use serde::ser::SerializeStruct;
        let field_count = if self.label.is_some() { 5 } else { 4 };
        let mut state = serializer.serialize_struct("DisplayDataItem", field_count)?;
        state.serialize_field("key", &self.key)?;
        state.serialize_field("namespace", &self.namespace)?;
        state.serialize_field("type", self.value.type_name())?;
        match &self.value {
            DisplayDataValue::Text(s) => state.serialize_field("value", s)?,
            DisplayDataValue::Integer(i) => state.serialize_field("value", i)?,
            DisplayDataValue::Boolean(b) => state.serialize_field("value", b)?,
            DisplayDataValue::Float(f) => state.serialize_field("value", f)?,
        }
        if let Some(label) = &self.label {
            state.serialize_field("label", label)?;
        }
        state.end()
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase")]
struct DisplayDataItemDe {
    key: String,
    namespace: String,
    #[serde(rename = "type")]
    item_type: Option<String>,
    value: serde_json::Value,
    label: Option<String>,
}

impl<'de> Deserialize<'de> for DisplayDataItem {
    fn deserialize<D>(deserializer: D) -> Result<Self, D::Error>
    where
        D: serde::Deserializer<'de>,
    {
        let helper = DisplayDataItemDe::deserialize(deserializer)?;
        let value = match helper.item_type.as_deref() {
            Some("INTEGER") => {
                let n = match &helper.value {
                    serde_json::Value::Number(num) => num.as_i64(),
                    serde_json::Value::String(s) => s.parse::<i64>().ok(),
                    _ => None,
                };
                n.map(DisplayDataValue::Integer)
                    .unwrap_or_else(|| DisplayDataValue::Text(helper.value.to_string()))
            }
            Some("BOOLEAN") => {
                let b = match &helper.value {
                    serde_json::Value::Bool(b) => Some(*b),
                    serde_json::Value::String(s) => s.parse::<bool>().ok(),
                    _ => None,
                };
                b.map(DisplayDataValue::Boolean)
                    .unwrap_or_else(|| DisplayDataValue::Text(helper.value.to_string()))
            }
            Some("FLOAT") | Some("DOUBLE") => {
                let f = match &helper.value {
                    serde_json::Value::Number(num) => num.as_f64(),
                    serde_json::Value::String(s) => s.parse::<f64>().ok(),
                    _ => None,
                };
                f.map(DisplayDataValue::Float)
                    .unwrap_or_else(|| DisplayDataValue::Text(helper.value.to_string()))
            }
            _ => match helper.value {
                serde_json::Value::String(s) => DisplayDataValue::Text(s),
                serde_json::Value::Number(num) => {
                    if let Some(i) = num.as_i64() {
                        DisplayDataValue::Integer(i)
                    } else if let Some(f) = num.as_f64() {
                        DisplayDataValue::Float(f)
                    } else {
                        DisplayDataValue::Text(num.to_string())
                    }
                }
                serde_json::Value::Bool(b) => DisplayDataValue::Boolean(b),
                other => DisplayDataValue::Text(other.to_string()),
            },
        };

        Ok(Self {
            key: helper.key,
            namespace: helper.namespace,
            value,
            label: helper.label,
        })
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
            value: DisplayDataValue::Text(value.into()),
            label: None,
        }
    }

    /// Creates an INTEGER display data item.
    pub fn integer(key: impl Into<String>, namespace: impl Into<String>, value: i64) -> Self {
        Self {
            key: key.into(),
            namespace: namespace.into(),
            value: DisplayDataValue::Integer(value),
            label: None,
        }
    }

    /// Creates a BOOLEAN display data item.
    pub fn boolean(key: impl Into<String>, namespace: impl Into<String>, value: bool) -> Self {
        Self {
            key: key.into(),
            namespace: namespace.into(),
            value: DisplayDataValue::Boolean(value),
            label: None,
        }
    }

    /// Creates a FLOAT display data item.
    pub fn float(key: impl Into<String>, namespace: impl Into<String>, value: f64) -> Self {
        Self {
            key: key.into(),
            namespace: namespace.into(),
            value: DisplayDataValue::Float(value),
            label: None,
        }
    }

    /// Sets the human-readable label of this item.
    pub fn with_label(mut self, label: impl Into<String>) -> Self {
        self.label = Some(label.into());
        self
    }

    /// Converts this item into a Runner API [`proto::DisplayData`] message.
    pub fn to_proto(&self) -> proto::DisplayData {
        let value = match &self.value {
            DisplayDataValue::Boolean(b) => Some(labelled_payload::Value::BoolValue(*b)),
            DisplayDataValue::Integer(i) => Some(labelled_payload::Value::IntValue(*i)),
            DisplayDataValue::Float(f) => Some(labelled_payload::Value::DoubleValue(*f)),
            DisplayDataValue::Text(s) => Some(labelled_payload::Value::StringValue(s.clone())),
        };

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

    /// Converts a Runner API [`proto::DisplayData`] message.
    pub fn from_proto(proto: &proto::DisplayData) -> Result<Self, prost::DecodeError> {
        if proto.urn == URN_DISPLAY_DATA_LABELLED {
            let labelled = proto::LabelledPayload::decode(&*proto.payload)?;
            let value = match labelled.value {
                Some(labelled_payload::Value::BoolValue(b)) => DisplayDataValue::Boolean(b),
                Some(labelled_payload::Value::IntValue(i)) => DisplayDataValue::Integer(i),
                Some(labelled_payload::Value::DoubleValue(f)) => DisplayDataValue::Float(f),
                Some(labelled_payload::Value::StringValue(s)) => DisplayDataValue::Text(s),
                None => DisplayDataValue::Text(String::new()),
            };

            let label = if labelled.label.is_empty() {
                None
            } else {
                Some(labelled.label)
            };

            Ok(Self {
                key: labelled.key,
                namespace: labelled.namespace,
                value,
                label,
            })
        } else {
            Ok(Self {
                key: proto.urn.clone(),
                namespace: "urn".to_string(),
                value: DisplayDataValue::Text(String::from_utf8_lossy(&proto.payload).to_string()),
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
