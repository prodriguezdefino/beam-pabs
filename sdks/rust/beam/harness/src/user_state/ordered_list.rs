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

//! Reads and writes of the ordered list cells of a bundle.

use model::fn_execution::{StateKey, state_key};

use beam::internals::UserStateError;

use crate::state::tables::with_cell;
use crate::user_state::bundle::BundleUserState;
use crate::user_state::cells::{OrderedListCell, split_ordered_list_elements};

impl BundleUserState {
    pub(super) fn get_ordered_list_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError> {
        let _coder_id = self
            .inner
            .get_ordered_list_state_coder(transform_id, state_id)?;
        let cell_key = (transform_id, state_id, window, key);
        {
            let mut guard = self.inner.lock_ordered_list_cells()?;
            let is_cleared = with_cell(
                &mut guard,
                cell_key,
                OrderedListCell::default,
                |cell: &mut OrderedListCell| cell.is_cleared(),
            );
            if is_cleared {
                return Ok(with_cell(
                    &mut guard,
                    cell_key,
                    OrderedListCell::default,
                    |cell: &mut OrderedListCell| cell.combine_with_fetched(from, to, Vec::new()),
                ));
            }
        }
        let state_key = StateKey {
            r#type: Some(state_key::Type::OrderedListUserState(
                state_key::OrderedListUserState {
                    transform_id: transform_id.to_string(),
                    user_state_id: state_id.to_string(),
                    window: window.to_vec(),
                    key: key.to_vec(),
                    range: Some(model::fn_execution::OrderedListRange {
                        start: from,
                        end: to,
                    }),
                },
            )),
        };
        let raw_bytes = self
            .inner
            .channel
            .get(state_key)
            .map_err(UserStateError::Runner)?;
        let fetched = if raw_bytes.is_empty() {
            Vec::new()
        } else {
            split_ordered_list_elements(&raw_bytes).map_err(UserStateError::Format)?
        };
        let mut guard = self.inner.lock_ordered_list_cells()?;
        let entries = with_cell(
            &mut guard,
            cell_key,
            OrderedListCell::default,
            |cell: &mut OrderedListCell| cell.combine_with_fetched(from, to, fetched),
        );
        Ok(entries)
    }

    pub(super) fn append_ordered_list_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        timestamp: i64,
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        let mut guard = self.inner.lock_ordered_list_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            OrderedListCell::default,
            |cell: &mut OrderedListCell| {
                cell.add(timestamp, element);
            },
        );
        Ok(())
    }

    pub(super) fn clear_ordered_list_range(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
        from: i64,
        to: i64,
    ) -> Result<(), UserStateError> {
        let mut guard = self.inner.lock_ordered_list_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            OrderedListCell::default,
            |cell: &mut OrderedListCell| {
                cell.clear_range(from, to);
            },
        );
        Ok(())
    }

    pub(super) fn clear_ordered_list_state(
        &self,
        transform_id: &str,
        state_id: &str,
        window: &[u8],
        key: &[u8],
    ) -> Result<(), UserStateError> {
        let mut guard = self.inner.lock_ordered_list_cells()?;
        with_cell(
            &mut guard,
            (transform_id, state_id, window, key),
            OrderedListCell::default,
            OrderedListCell::clear,
        );
        Ok(())
    }
}
