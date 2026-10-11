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

//! Handler keys of transforms that an expansion service created.
//!
//! The expansion service writes an [`ExpandedSpec`] into each Rust `do_fn` that an expansion
//! creates. A worker has no pipeline for these transforms. It builds the expansion again from
//! the [`ReplayEntry`], once for each expansion, and finds the key there.

use std::collections::HashMap;
use std::sync::{Arc, LazyLock, Mutex, PoisonError};

use prost::Message;
use tracing::warn;

use beam::internals::TransformFn;
use model::pipeline as proto;

/// A `ParDoPayload.do_fn` whose payload is an [`ExpandedSpec`].
pub const URN_RUST_DOFN_EXPANDED: &str = "beam:dofn:rust:expanded:v1";

/// A handler key and the expansion that registers it.
#[derive(Clone, PartialEq, Message)]
pub struct ExpandedSpec {
    /// The bare handler key in the pipeline that `replay` builds.
    #[prost(string, tag = "1")]
    pub handler_key: String,
    #[prost(message, optional, tag = "2")]
    pub replay: Option<ReplayEntry>,
}

/// The arguments of one expansion. The same arguments build the same transforms and keys.
#[derive(Clone, PartialEq, Message)]
pub struct ReplayEntry {
    /// Identifier of the `SchemaTransformProvider`.
    #[prost(string, tag = "1")]
    pub provider: String,
    /// The provider's config schema, which encodes `config_row`.
    #[prost(message, optional, tag = "2")]
    pub config_schema: Option<proto::Schema>,
    #[prost(bytes = "vec", tag = "3")]
    pub config_row: Vec<u8>,
    /// The namespace of the expansion request. The caller sets it, so it can be empty and two
    /// callers can send the same one. `expansion_id` identifies the expansion.
    #[prost(string, tag = "4")]
    pub namespace: String,
    /// Input tag to PCollection id, as the provider received them.
    #[prost(map = "string, string", tag = "5")]
    pub inputs: HashMap<String, String>,
    /// The input PCollections with their coders and windowing strategies. The pipeline of
    /// the expansion starts with only these components.
    #[prost(message, optional, tag = "6")]
    pub components: Option<proto::Components>,
    /// Identifies the expansion. The expansion service makes a new one for each request.
    #[prost(string, tag = "7")]
    pub expansion_id: String,
}

/// Builds the handlers of a [`ReplayEntry`]. The expansion service registers one through
/// `inventory`.
pub struct ReplayRegistration {
    pub replay: fn(&ReplayEntry) -> Result<HashMap<String, TransformFn>, String>,
}

inventory::collect!(ReplayRegistration);

/// The fields of an [`ExpandedSpec`] that find cached handlers. Decoding skips the replay
/// components. The tags are the tags of [`ExpandedSpec`] and [`ReplayEntry`].
#[derive(Clone, PartialEq, Message)]
struct ExpandedSpecKey {
    #[prost(string, tag = "1")]
    handler_key: String,
    #[prost(message, optional, tag = "2")]
    replay: Option<ReplayId>,
}

#[derive(Clone, PartialEq, Message)]
struct ReplayId {
    #[prost(string, tag = "7")]
    expansion_id: String,
}

type Handlers = Arc<HashMap<String, TransformFn>>;

/// The handlers of one expansion, or `None` before a build succeeds.
type Cell = Arc<Mutex<Option<Handlers>>>;

/// The replayed handlers of each expansion in this process. Each expansion has its own
/// lock, so a build blocks only lookups of the same expansion.
static REPLAYED: LazyLock<Mutex<HashMap<String, Cell>>> = LazyLock::new(Mutex::default);

/// Returns the replayed handlers and the handler key of an expanded `spec`.
///
/// The first call for an expansion builds the handlers. A failed build is logged and not
/// kept, so the transform reports a missing handler and the next processor tries again.
pub(crate) fn replayed_handlers(spec: &proto::FunctionSpec) -> Option<(Handlers, String)> {
    let key = ExpandedSpecKey::decode(spec.payload.as_slice()).ok()?;
    let cell = Arc::clone(
        REPLAYED
            .lock()
            .unwrap_or_else(PoisonError::into_inner)
            .entry(key.replay?.expansion_id)
            .or_default(),
    );
    let mut cached = cell.lock().unwrap_or_else(PoisonError::into_inner);
    if let Some(handlers) = cached.as_ref() {
        return Some((Arc::clone(handlers), key.handler_key));
    }
    let entry = ExpandedSpec::decode(spec.payload.as_slice()).ok()?.replay?;
    let built = inventory::iter::<ReplayRegistration>
        .into_iter()
        .next()
        .ok_or_else(|| "no expansion replay is linked into this binary".to_string())
        .and_then(|registration| (registration.replay)(&entry));
    match built {
        Ok(handlers) => Some((
            Arc::clone(cached.insert(Arc::new(handlers))),
            key.handler_key,
        )),
        Err(e) => {
            warn!(
                "Replay of expansion '{}' of '{}' failed: {e}",
                entry.namespace, entry.provider
            );
            None
        }
    }
}
