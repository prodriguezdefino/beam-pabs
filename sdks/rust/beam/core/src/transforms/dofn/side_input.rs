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

//! Side input readers that a runner implements to serve views during bundle execution. The
//! view types, such as [`PCollectionView`](crate::values::PCollectionView), are in
//! [`crate::values`].

use model::pipeline as proto;

/// Returns the side input tags of a `ParDo` `PTransform`. Returns an empty set for other
/// transforms and for a payload that does not decode.
pub fn extract_side_input_tags(transform: &proto::PTransform) -> std::collections::HashSet<String> {
    use prost::Message;
    transform
        .spec
        .as_ref()
        .filter(|spec| spec.urn == crate::pipeline::URN_PAR_DO && !spec.payload.is_empty())
        .and_then(|spec| proto::ParDoPayload::decode(spec.payload.as_slice()).ok())
        .map(|p| p.side_inputs.into_keys().collect())
        .unwrap_or_default()
}

/// Reader that fetches encoded side input elements from a runner.
pub trait SideInputReader: Send + Sync {
    /// Fetches the encoded elements of an iterable or singleton side input in `window`.
    fn get_iterable(&self, tag: &str, window: &[u8]) -> Result<Vec<Vec<u8>>, String>;

    /// Fetches the encoded values for `key` of a multimap side input in `window`.
    fn get_multimap(&self, tag: &str, window: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>, String>;

    /// `get_iterable` for one transform. The default ignores the transform ID.
    fn get_iterable_for_transform(
        &self,
        _transform_id: &str,
        tag: &str,
        window: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        self.get_iterable(tag, window)
    }

    /// `get_multimap` for one transform. The default ignores the transform ID.
    fn get_multimap_for_transform(
        &self,
        _transform_id: &str,
        tag: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, String> {
        self.get_multimap(tag, window, key)
    }
}

/// A [`SideInputReader`] adapter scoped to one transform ID.
pub struct ScopedSideInputReader<'a> {
    reader: &'a dyn SideInputReader,
    transform_id: &'a str,
}

impl<'a> ScopedSideInputReader<'a> {
    pub fn new(reader: &'a dyn SideInputReader, transform_id: &'a str) -> Self {
        Self {
            reader,
            transform_id,
        }
    }
}

impl SideInputReader for ScopedSideInputReader<'_> {
    fn get_iterable(&self, tag: &str, window: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.reader
            .get_iterable_for_transform(self.transform_id, tag, window)
    }

    fn get_multimap(&self, tag: &str, window: &[u8], key: &[u8]) -> Result<Vec<Vec<u8>>, String> {
        self.reader
            .get_multimap_for_transform(self.transform_id, tag, window, key)
    }
}
