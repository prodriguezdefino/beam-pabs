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

//! Ordered list user state: values sorted by a timestamp.

use std::marker::PhantomData;
use std::ops::Range;
use std::sync::Arc;

use beam_model::pipeline as proto;

use super::backend::UserStateBackend;
use super::{AnyStateSpec, check_row_schema, state_spec};
use crate::Instant;
use crate::coders::DefaultCoder;
use crate::pipeline::Pipeline;
use crate::pipeline::constants::URN_USER_STATE_ORDERED_LIST;

/// Specification for an ordered list user state cell.
#[derive(Clone, Debug)]
pub(crate) struct OrderedListStateSpec<T> {
    name: String,
    _marker: PhantomData<T>,
}

impl<T: DefaultCoder> OrderedListStateSpec<T> {
    /// Creates an ordered list state specification with the given unique state ID.
    pub(crate) fn new(name: impl Into<String>) -> Self {
        Self {
            name: name.into(),
            _marker: PhantomData,
        }
    }
}

impl<T: DefaultCoder> AnyStateSpec for OrderedListStateSpec<T> {
    fn name(&self) -> &str {
        &self.name
    }

    fn register_and_encode(&self, pipeline: &Pipeline) -> proto::StateSpec {
        state_spec(
            proto::state_spec::Spec::OrderedListSpec(proto::OrderedListStateSpec {
                element_coder_id: T::register_coder(pipeline),
            }),
            URN_USER_STATE_ORDERED_LIST,
        )
    }
}

/// A typed handle to a persistent ordered list user state cell for a specific key and window.
pub struct OrderedListState<T> {
    backend: Arc<dyn UserStateBackend>,
    state_id: Arc<str>,
    window: Arc<[u8]>,
    key: Arc<[u8]>,
    _marker: PhantomData<T>,
}

impl<T: DefaultCoder> OrderedListState<T> {
    pub fn new(
        backend: Arc<dyn UserStateBackend>,
        state_id: impl Into<Arc<str>>,
        window: impl Into<Arc<[u8]>>,
        key: impl Into<Arc<[u8]>>,
    ) -> Self {
        Self {
            backend,
            state_id: state_id.into(),
            window: window.into(),
            key: key.into(),
            _marker: PhantomData,
        }
    }

    /// Adds `value` at `timestamp`.
    pub fn add(&mut self, timestamp: Instant, value: &T) -> crate::Result {
        check_row_schema(
            value,
            self.backend.state_schema(&self.state_id),
            &self.state_id,
        )?;
        let bytes = value.encode().map_err(|e| {
            crate::Error::from(e).context(format!(
                "Failed to encode ordered list state element for '{}'",
                self.state_id
            ))
        })?;
        Ok(self.backend.append_ordered_list_state(
            &self.state_id,
            &self.window,
            &self.key,
            timestamp.as_millis(),
            bytes,
        )?)
    }

    /// Reads all entries, sorted by timestamp.
    ///
    /// Queries the half-open range `[i64::MIN, i64::MAX)`, which excludes elements at `i64::MAX`.
    pub fn read(&self) -> crate::Result<Vec<(Instant, T)>> {
        self.read_millis(i64::MIN, i64::MAX)
    }

    /// Reads the entries with a timestamp in `range`, sorted by timestamp.
    pub fn read_range(&self, range: Range<Instant>) -> crate::Result<Vec<(Instant, T)>> {
        self.read_millis(range.start.as_millis(), range.end.as_millis())
    }

    fn read_millis(&self, from: i64, to: i64) -> crate::Result<Vec<(Instant, T)>> {
        let schema = self.backend.state_schema(&self.state_id);
        let raw_entries = self.backend.get_ordered_list_state(
            &self.state_id,
            &self.window,
            &self.key,
            from,
            to,
        )?;
        raw_entries
            .into_iter()
            .map(|(ts, bytes)| {
                T::decode_with_schema(&bytes, schema)
                    .map(|v| (Instant::from_millis(ts), v))
                    .map_err(|e| {
                        crate::Error::from(e).context(format!(
                            "Failed to decode ordered list state element for '{}'",
                            self.state_id
                        ))
                    })
            })
            .collect()
    }

    /// Clears the entries with a timestamp in `range`.
    pub fn clear_range(&mut self, range: Range<Instant>) -> crate::Result {
        Ok(self.backend.clear_ordered_list_range(
            &self.state_id,
            &self.window,
            &self.key,
            range.start.as_millis(),
            range.end.as_millis(),
        )?)
    }

    /// Clears all entries in this ordered list state cell.
    pub fn clear(&mut self) -> crate::Result {
        Ok(self
            .backend
            .clear_ordered_list_state(&self.state_id, &self.window, &self.key)?)
    }
}
