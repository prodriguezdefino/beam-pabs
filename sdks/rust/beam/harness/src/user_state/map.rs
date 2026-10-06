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

//! Reads and writes of the map, set and multimap cells of a bundle.

use std::borrow::Cow;

use beam_model::fn_execution::{StateKey, state_key};

use beam::internals::UserStateError;

use crate::state::tables::with_cell;
use crate::user_state::bundle::BundleUserState;
use crate::user_state::cells::MapCell;

impl BundleUserState {
    pub(super) fn get_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        {
            let mut guard = self.inner.lock_map_cells()?;
            let cached = with_cell(
                &mut guard,
                cell_key,
                move || seed.unwrap_or_default(),
                |cell: &mut MapCell| cell.get_if_present(map_key),
            );
            if let Some(elements) = cached {
                return Ok(elements);
            }
        }
        let coders = self.inner.map_coders(transform_id, state_id)?;
        let framed_map_key = coders
            .key
            .framing
            .frame(Cow::Borrowed(map_key))
            .map_err(UserStateError::Format)?
            .into_owned();
        let state_key = StateKey {
            r#type: Some(state_key::Type::MultimapUserState(
                state_key::MultimapUserState {
                    transform_id: transform_id.to_string(),
                    user_state_id: state_id.to_string(),
                    window: window.to_vec(),
                    key: key.to_vec(),
                    map_key: framed_map_key,
                },
            )),
        };
        let raw_bytes = self
            .inner
            .channel
            .get(state_key)
            .map_err(UserStateError::Runner)?;
        let fetched = coders
            .value
            .framing
            .split(&raw_bytes)
            .map_err(UserStateError::Format)?;
        let mut guard = self.inner.lock_map_cells()?;
        let elements = with_cell(
            &mut guard,
            cell_key,
            MapCell::default,
            |cell: &mut MapCell| cell.install_fetched_entry(map_key, fetched),
        );
        Ok(elements)
    }

    pub(super) fn put_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            |cell: &mut MapCell| {
                cell.insert(map_key, element);
            },
        );
        Ok(())
    }

    pub(super) fn append_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: Vec<u8>,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            |cell: &mut MapCell| {
                cell.append(map_key, element);
            },
        );
        Ok(())
    }

    pub(super) fn remove_map_key(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        map_key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            |cell: &mut MapCell| {
                cell.remove(map_key);
            },
        );
        Ok(())
    }

    pub(super) fn clear_map_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        let mut guard = self.inner.lock_map_cells()?;
        with_cell(
            &mut guard,
            cell_key,
            move || seed.unwrap_or_default(),
            MapCell::clear,
        );
        Ok(())
    }

    pub(super) fn get_map_keys(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        let cell_key = (transform_id, state_id, window, key);
        let seed = self.inner.map_seed(cell_key)?;
        {
            let mut guard = self.inner.lock_map_cells()?;
            let cached = with_cell(
                &mut guard,
                cell_key,
                move || seed.unwrap_or_default(),
                |cell: &mut MapCell| cell.keys_if_present(),
            );
            if let Some(keys) = cached {
                return Ok(keys);
            }
        }
        let state_key = StateKey {
            r#type: Some(state_key::Type::MultimapKeysUserState(
                state_key::MultimapKeysUserState {
                    transform_id: transform_id.to_string(),
                    user_state_id: state_id.to_string(),
                    window: window.to_vec(),
                    key: key.to_vec(),
                },
            )),
        };
        let raw_bytes = self
            .inner
            .channel
            .get(state_key)
            .map_err(UserStateError::Runner)?;
        let fetched = self
            .inner
            .map_coders(transform_id, state_id)?
            .key
            .framing
            .split(&raw_bytes)
            .map_err(UserStateError::Format)?;
        let mut guard = self.inner.lock_map_cells()?;
        let keys = with_cell(
            &mut guard,
            cell_key,
            MapCell::default,
            |cell: &mut MapCell| cell.install_fetched_keys(fetched),
        );
        Ok(keys)
    }
}
