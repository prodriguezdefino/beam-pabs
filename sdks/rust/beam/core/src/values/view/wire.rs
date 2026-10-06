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

//! Serialized form of a view, so a registered `DoFn` can hold it.

use std::marker::PhantomData;
use std::sync::{Arc, OnceLock};

use beam_model::pipeline as proto;

use super::{PCollectionView, SideInputKind, SideInputWindowing, ViewKind, ViewWindowing};
use crate::schema::Schema;

/// How a view finds its side input window, as serialized.
#[derive(serde::Serialize, serde::Deserialize)]
enum WireWindowing {
    /// The `window_mapping_fn` of the view.
    Mapping { urn: String, payload: Vec<u8> },
    /// The runner gives the window mapping.
    FromRunner,
    /// The side PCollection's windowing is unsupported; pipeline validation reports it.
    Unsupported(String),
}

/// The serialized form of a view. `kind` and `element_type` are checked on read, so a type
/// mismatch fails when the function is built, not when an element is decoded.
#[derive(serde::Serialize, serde::Deserialize)]
struct WireView {
    tag: String,
    pcollection_id: String,
    coder_id: String,
    kind: SideInputKind,
    element_type: String,
    windowing: WireWindowing,
    /// The `Schema` proto of the rows in an element, if the coder has one.
    row_schema: Option<Vec<u8>>,
}

impl<T, Kind: ViewKind> serde::Serialize for PCollectionView<T, Kind> {
    fn serialize<S: serde::Serializer>(&self, serializer: S) -> Result<S::Ok, S::Error> {
        let windowing = match &self.windowing {
            ViewWindowing::Known(windowing) => {
                let spec = windowing.to_proto();
                WireWindowing::Mapping {
                    urn: spec.urn,
                    payload: spec.payload,
                }
            }
            ViewWindowing::FromRunner => WireWindowing::FromRunner,
            ViewWindowing::Unsupported(reason) => WireWindowing::Unsupported(reason.clone()),
        };
        WireView {
            tag: self.tag.clone(),
            pcollection_id: self.pcollection_id.clone(),
            coder_id: self.coder_id.clone(),
            kind: Kind::KIND,
            // Rust does not guarantee stability of type_name across compiler releases.
            // This check assumes launcher and worker run the same compiled binary.
            element_type: std::any::type_name::<T>().to_string(),
            windowing,
            row_schema: self
                .row_schema
                .as_ref()
                .map(|schema| schema.to_proto_bytes()),
        }
        .serialize(serializer)
    }
}

impl<'de, T, Kind: ViewKind> serde::Deserialize<'de> for PCollectionView<T, Kind> {
    fn deserialize<D: serde::Deserializer<'de>>(deserializer: D) -> Result<Self, D::Error> {
        use serde::de::Error;

        let wire = WireView::deserialize(deserializer)?;
        if wire.kind != Kind::KIND {
            return Err(D::Error::custom(format!(
                "side input '{}' is a {:?} view, read as a {:?} view",
                wire.tag,
                wire.kind,
                Kind::KIND
            )));
        }
        // Verify the element type. Type name stability requires identical binaries.
        let element_type = std::any::type_name::<T>();
        if wire.element_type != element_type {
            return Err(D::Error::custom(format!(
                "side input '{}' holds `{}`, read as `{element_type}`",
                wire.tag, wire.element_type
            )));
        }
        let windowing = match wire.windowing {
            WireWindowing::Mapping { urn, payload } => {
                SideInputWindowing::from_proto(&proto::FunctionSpec { urn, payload })
                    .map_or_else(ViewWindowing::Unsupported, ViewWindowing::Known)
            }
            WireWindowing::FromRunner => ViewWindowing::FromRunner,
            WireWindowing::Unsupported(reason) => ViewWindowing::Unsupported(reason),
        };
        let row_schema = wire
            .row_schema
            .map(|bytes| Schema::from_proto_bytes(&bytes).map(Arc::new))
            .transpose()
            .map_err(|e| D::Error::custom(format!("side input '{}': {e}", wire.tag)))?;
        Ok(Self {
            tag: wire.tag,
            pcollection_id: wire.pcollection_id,
            coder_id: wire.coder_id,
            windowing,
            row_schema,
            field_map: Arc::new(OnceLock::new()),
            _marker: PhantomData,
        })
    }
}
