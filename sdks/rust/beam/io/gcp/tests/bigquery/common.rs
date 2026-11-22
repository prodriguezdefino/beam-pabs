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

//! State and side input fakes for the BigQuery write tests.

use std::sync::Mutex;

use beam::coders::DefaultCoder;
use beam::internals::{
    SideInputElements, SideInputError, SideInputReader, SingletonBytes, StateBag, UserStateBackend,
    UserStateError,
};
use beam::values::SideInputWindowing;
use beam_io_gcp::bigquery::write::buffered::StreamState;
use bytes::Bytes;

/// Single-cell in-memory state backend for `ShardState::stream` in timer tests.
pub(crate) struct SingleState(Mutex<Option<Vec<u8>>>);

impl SingleState {
    pub(crate) fn new(initial: &StreamState) -> Self {
        Self(Mutex::new(Some(
            initial.encode().expect("encodes the initial state"),
        )))
    }

    pub(crate) fn current(&self) -> Option<Vec<u8>> {
        self.0.lock().expect("no test thread panics").clone()
    }
}

fn unused_state<T>() -> Result<T, UserStateError> {
    Err(UserStateError::Other(
        "the test uses only ValueState".to_string(),
    ))
}

impl UserStateBackend for SingleState {
    fn get_state(&self, _: &str, _: &[u8], _: &[u8]) -> Result<StateBag, UserStateError> {
        let guard = self.0.lock().expect("no test thread panics");
        Ok(match guard.as_ref() {
            Some(bytes) => StateBag::from_single(Bytes::from(bytes.clone())),
            None => StateBag::default(),
        })
    }

    fn append_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        element: Vec<u8>,
    ) -> Result<(), UserStateError> {
        *self.0.lock().expect("no test thread panics") = Some(element);
        Ok(())
    }

    fn clear_state(&self, _: &str, _: &[u8], _: &[u8]) -> Result<(), UserStateError> {
        *self.0.lock().expect("no test thread panics") = None;
        Ok(())
    }

    fn get_map_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: &[u8],
    ) -> Result<Vec<Vec<u8>>, UserStateError> {
        unused_state()
    }

    fn get_map_keys(&self, _: &str, _: &[u8], _: &[u8]) -> Result<Vec<Vec<u8>>, UserStateError> {
        unused_state()
    }

    fn put_map_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: Vec<u8>,
        _: Vec<u8>,
    ) -> Result<(), UserStateError> {
        unused_state()
    }

    fn append_map_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: Vec<u8>,
        _: Vec<u8>,
    ) -> Result<(), UserStateError> {
        unused_state()
    }

    fn remove_map_key(&self, _: &str, _: &[u8], _: &[u8], _: &[u8]) -> Result<(), UserStateError> {
        unused_state()
    }

    fn clear_map_state(&self, _: &str, _: &[u8], _: &[u8]) -> Result<(), UserStateError> {
        unused_state()
    }

    fn get_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: i64,
    ) -> Result<Vec<(i64, Vec<u8>)>, UserStateError> {
        unused_state()
    }

    fn append_ordered_list_state(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: Vec<u8>,
    ) -> Result<(), UserStateError> {
        unused_state()
    }

    fn clear_ordered_list_range(
        &self,
        _: &str,
        _: &[u8],
        _: &[u8],
        _: i64,
        _: i64,
    ) -> Result<(), UserStateError> {
        unused_state()
    }

    fn clear_ordered_list_state(&self, _: &str, _: &[u8], _: &[u8]) -> Result<(), UserStateError> {
        unused_state()
    }
}

/// Side-input reader serving one encoded `Destination` singleton in the global window.
pub(crate) struct SingletonSideInput {
    pub(crate) value: Bytes,
    pub(crate) windowing: SideInputWindowing,
}

impl SideInputReader for SingletonSideInput {
    fn get_iterable<'a>(
        &'a self,
        _: &str,
        _: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        Ok(Box::new(std::iter::once(Ok(self.value.clone()))))
    }

    fn get_singleton(&self, _: &str, _: &[u8]) -> Result<SingletonBytes, SideInputError> {
        Ok(SingletonBytes::One(self.value.clone()))
    }

    fn get_multimap<'a>(
        &'a self,
        tag: &str,
        _: &[u8],
        _: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        Err(SideInputError::MissingReader {
            tag: tag.to_string(),
        })
    }

    fn get_multimap_keys<'a>(
        &'a self,
        tag: &str,
        _: &[u8],
    ) -> Result<SideInputElements<'a>, SideInputError> {
        Err(SideInputError::MissingReader {
            tag: tag.to_string(),
        })
    }

    fn window_mapping(&self, _: &str) -> Result<&SideInputWindowing, SideInputError> {
        Ok(&self.windowing)
    }
}
