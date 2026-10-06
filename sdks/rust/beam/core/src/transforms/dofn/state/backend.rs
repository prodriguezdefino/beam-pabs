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

//! The storage interface of user state cells and its error type.

use std::sync::Arc;

use crate::schema::Schema;

/// An error during a user state operation.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum UserStateError {
    /// Runner returned an error for the state operation.
    #[error("{0}")]
    Runner(String),
    /// State element encoding or decoding failed.
    #[error("State format error: {0}")]
    Format(String),
    /// State cell is missing an active backend.
    #[error("No active UserStateBackend for state '{0}'")]
    MissingBackend(String),
    /// General or internal state failure.
    #[error("{0}")]
    Other(String),
}

/// Interface that reads and changes user state storage.
pub trait UserStateBackend: Send + Sync {
    /// Reads all elements in the state cell for `(state_id, window, key)`.
    fn get_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError>;

    /// Appends an encoded element to the state cell for `(state_id, window, key)`.
    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), UserStateError>;

    /// Clears the state cell for `(state_id, window, key)`.
    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), UserStateError>;

    /// Reads all values associated with `map_key` in the multimap state cell.
    fn get_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError>;

    /// Reads all distinct keys in the multimap state cell.
    fn get_map_keys(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError>;

    /// Puts a key-value entry in the map state cell and replaces any earlier entry.
    fn put_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError>;

    /// Adds a value to the values of `map_key` in the multimap state cell.
    fn append_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError>;

    /// Removes a key and its value from the map state cell.
    fn remove_map_key(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), UserStateError>;

    /// Clears all entries in the map state cell.
    fn clear_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError>;

    /// Reads sorted timestamp-value pairs within the half-open range `[from, to)`.
    fn get_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError>;

    /// Appends an encoded element with its timestamp to the ordered list state cell.
    fn append_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        timestamp: i64,
        element: Vec<u8>,
    ) -> Result<(), UserStateError>;

    /// Clears entries within the half-open range `[from, to)` from the ordered list state cell.
    fn clear_ordered_list_range(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), UserStateError>;

    /// Clears all entries in the ordered list state cell.
    fn clear_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError>;

    /// Returns the row schema of values in `state_id`, when the state coder carries one.
    fn state_schema(&self, _state_id: &str) -> Option<&Arc<Schema>> {
        None
    }

    /// Returns the row schema of map keys in `state_id`, when the key coder carries one.
    fn map_key_schema(&self, _state_id: &str) -> Option<&Arc<Schema>> {
        None
    }
}
