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

//! The user state backend of one transform in a bundle.

use std::sync::Arc;

use beam::internals::{UserStateBackend, UserStateError};
use beam::schema::Schema;

use crate::user_state::bundle::BundleUserState;

/// The user state of a bundle, seen by one transform.
pub(super) struct ScopedUserState {
    pub(super) bundle_state: BundleUserState,
    pub(super) transform_id: String,
}

impl UserStateBackend for ScopedUserState {
    fn get_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        self.bundle_state
            .get_state(&self.transform_id, state_id, window, key)
    }

    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .append_state(&self.transform_id, state_id, window, key, element)
    }

    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), UserStateError> {
        self.bundle_state
            .clear_state(&self.transform_id, state_id, window, key)
    }

    fn get_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        self.bundle_state
            .get_map_state(&self.transform_id, state_id, window, key, map_key)
    }

    fn get_map_keys(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        self.bundle_state
            .get_map_keys(&self.transform_id, state_id, window, key)
    }

    fn put_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .put_map_state(&self.transform_id, state_id, window, key, map_key, element)
    }

    fn append_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        self.bundle_state.append_map_state(
            &self.transform_id,
            state_id,
            window,
            key,
            map_key,
            element,
        )
    }

    fn remove_map_key(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .remove_map_key(&self.transform_id, state_id, window, key, map_key)
    }

    fn clear_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .clear_map_state(&self.transform_id, state_id, window, key)
    }

    fn get_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError> {
        self.bundle_state.get_ordered_list_state(
            &self.transform_id,
            state_id,
            window,
            key,
            from,
            to,
        )
    }

    fn append_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        timestamp: i64,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        self.bundle_state.append_ordered_list_state(
            &self.transform_id,
            state_id,
            window,
            key,
            timestamp,
            element,
        )
    }

    fn clear_ordered_list_range(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), UserStateError> {
        self.bundle_state.clear_ordered_list_range(
            &self.transform_id,
            state_id,
            window,
            key,
            from,
            to,
        )
    }

    fn clear_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        self.bundle_state
            .clear_ordered_list_state(&self.transform_id, state_id, window, key)
    }

    fn state_schema(&self, state_id: &str) -> Option<&Arc<Schema>> {
        self.bundle_state.state_schema(&self.transform_id, state_id)
    }

    fn map_key_schema(&self, state_id: &str) -> Option<&Arc<Schema>> {
        self.bundle_state
            .map_key_schema(&self.transform_id, state_id)
    }
}
