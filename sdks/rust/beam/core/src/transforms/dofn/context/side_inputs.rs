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

//! Side input reads from a `DoFn`. Each accessor maps the window of the current element with
//! the window mapping function of the view, then reads through the
//! [`SideInputReader`](crate::transforms::dofn::side_input::SideInputReader) of the context.

use super::ProcessContext;
use crate::coders::DefaultCoder;
use crate::values::PCollectionView;

impl<T> ProcessContext<'_, T> {
    /// Reads a singleton side input. Returns an error if no side input reader is active or if
    /// the view does not hold exactly one element.
    pub fn side_input<S: DefaultCoder>(&self, view: &PCollectionView<S>) -> crate::Result<S> {
        let reader = self.reader.ok_or_else(|| {
            format!(
                "Side input '{}' not found (no active SideInputReader)",
                view.tag()
            )
        })?;
        let mapped = view.mapped_window(self.window())?;
        let elements = reader.get_iterable(view.tag(), &mapped)?;
        match elements.as_slice() {
            [] => Err(format!("Empty singleton side input for tag '{}'", view.tag()).into()),
            [single] => decode_side_input(single, view.tag()),
            many => Err(format!(
                "PCollection with more than one element ({}) accessed as a singleton for tag '{}'",
                many.len(),
                view.tag()
            )
            .into()),
        }
    }

    /// Reads all elements of an iterable side input.
    pub fn side_input_iter<S: DefaultCoder>(
        &self,
        view: &PCollectionView<S>,
    ) -> crate::Result<Vec<S>> {
        let reader = self.reader.ok_or_else(|| {
            format!(
                "Side input '{}' not found (no active SideInputReader)",
                view.tag()
            )
        })?;
        let mapped = view.mapped_window(self.window())?;
        reader
            .get_iterable(view.tag(), &mapped)?
            .iter()
            .map(|bytes| decode_side_input(bytes, view.tag()))
            .collect()
    }

    /// Reads all values for `key` from a multimap side input.
    pub fn side_input_map<K: DefaultCoder, V: DefaultCoder>(
        &self,
        view: &PCollectionView<(K, V)>,
        key: &K,
    ) -> crate::Result<Vec<V>> {
        let reader = self.reader.ok_or_else(|| {
            format!(
                "Side input '{}' not found (no active SideInputReader)",
                view.tag()
            )
        })?;
        let key_bytes = key
            .encode()
            .map_err(|e| crate::Error::from(e).context("Failed to encode key"))?;
        let mapped = view.mapped_window(self.window())?;
        reader
            .get_multimap(view.tag(), &mapped, &key_bytes)?
            .iter()
            .map(|bytes| decode_side_input(bytes, view.tag()))
            .collect()
    }
}

fn decode_side_input<S: DefaultCoder>(bytes: &[u8], tag: &str) -> crate::Result<S> {
    S::decode(bytes)
        .map_err(|e| crate::Error::from(e).context(format!("Failed to decode side input '{tag}'")))
}
