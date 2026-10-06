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

//! Helpers that more than one test module of this binary uses.

use std::collections::HashMap;
use std::sync::{Arc, Mutex};

use beam::internals::{UserStateBackend, UserStateError};
use beam::schema::Schema;

type StateMapKey = (String, Vec<u8>, Vec<u8>);
type StateCells = HashMap<StateMapKey, Vec<Vec<u8>>>;
type MapEntries = HashMap<Vec<u8>, Vec<Vec<u8>>>;
type OrderedListEntries = std::collections::BTreeMap<i64, Vec<Vec<u8>>>;

/// An in-memory user state backend. The row schemas of cells are optional.
#[derive(Default, Debug)]
pub(crate) struct TestStateBackend {
    cells: Mutex<StateCells>,
    map_cells: Mutex<HashMap<StateMapKey, MapEntries>>,
    ordered_list_cells: Mutex<HashMap<StateMapKey, OrderedListEntries>>,
    value_schemas: HashMap<String, Arc<Schema>>,
    key_schemas: HashMap<String, Arc<Schema>>,
}

impl TestStateBackend {
    /// Gives `schema` to the values of cell `state_id`.
    pub(crate) fn with_value_schema(mut self, state_id: &str, schema: Arc<Schema>) -> Self {
        self.value_schemas.insert(state_id.to_string(), schema);
        self
    }

    /// Gives `schema` to the map keys of cell `state_id`.
    pub(crate) fn with_key_schema(mut self, state_id: &str, schema: Arc<Schema>) -> Self {
        self.key_schemas.insert(state_id.to_string(), schema);
        self
    }
}

impl UserStateBackend for TestStateBackend {
    fn get_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let map_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self
            .cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        Ok(guard.get(&map_key).cloned().unwrap_or_default())
    }

    fn append_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let map_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        guard.entry(map_key).or_default().push(element);
        Ok(())
    }

    fn clear_state(&self, state_id: &str, window: &[u8], key: &[u8]) -> Result<(), UserStateError> {
        let map_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        guard.remove(&map_key);
        Ok(())
    }

    fn get_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self
            .map_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        if let Some(entries) = guard.get(&cell_key)
            && let Some(values) = entries.get(map_key)
        {
            return Ok(values.clone());
        }
        Ok(Vec::new())
    }

    fn get_map_keys(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self
            .map_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        Ok(guard
            .get(&cell_key)
            .map(|m| m.keys().cloned().collect())
            .unwrap_or_default())
    }

    fn put_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .map_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        guard
            .entry(cell_key)
            .or_default()
            .insert(map_key, vec![element]);
        Ok(())
    }

    fn append_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .map_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        guard
            .entry(cell_key)
            .or_default()
            .entry(map_key)
            .or_default()
            .push(element);
        Ok(())
    }

    fn remove_map_key(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .map_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        if let Some(entries) = guard.get_mut(&cell_key) {
            entries.remove(map_key);
        }
        Ok(())
    }

    fn clear_map_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .map_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        guard.remove(&cell_key);
        Ok(())
    }

    fn get_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        let Some(entries) = guard.get(&cell_key) else {
            return Ok(Vec::new());
        };
        let mut result = Vec::new();
        for (&ts, values) in entries.range(from..to) {
            for val in values {
                result.push((ts, val.clone()));
            }
        }
        Ok(result)
    }

    fn append_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        timestamp: i64,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        guard
            .entry(cell_key)
            .or_default()
            .entry(timestamp)
            .or_default()
            .push(element);
        Ok(())
    }

    fn clear_ordered_list_range(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        if let Some(entries) = guard.get_mut(&cell_key) {
            entries.retain(|&ts, _| ts < from || ts >= to);
        }
        Ok(())
    }

    fn clear_ordered_list_state(
        &self,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (state_id.to_string(), window.to_vec(), key.to_vec());
        let mut guard = self
            .ordered_list_cells
            .lock()
            .map_err(|e| UserStateError::Other(format!("Lock error: {e}")))?;
        guard.remove(&cell_key);
        Ok(())
    }

    fn state_schema(&self, state_id: &str) -> Option<&Arc<Schema>> {
        self.value_schemas.get(state_id)
    }

    fn map_key_schema(&self, state_id: &str) -> Option<&Arc<Schema>> {
        self.key_schemas.get(state_id)
    }
}
