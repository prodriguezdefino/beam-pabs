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

use std::collections::HashMap;
use std::sync::Arc;

use beam::coders::{DefaultCoder, WindowedHeader};
use beam::internals::PerWindow;
use beam::transforms::{DoFn, FinishBundleContext, ProcessContext};

use super::writer::{OpenFile, WriterConfig};

type BundleFiles<T> = HashMap<Vec<u8>, BundleFile<T>>;

/// A bundle-scoped temporary file and the header its result is emitted under.
struct BundleFile<T> {
    file: OpenFile<T>,
    header: WindowedHeader,
}

/// Writes each bundle's elements to one temporary file per window (more if rolling).
pub(super) struct WriteBundlesFn<T> {
    writer: Arc<WriterConfig<T>>,
    open: BundleFiles<T>,
}

impl<T> WriteBundlesFn<T> {
    pub(super) fn new(writer: Arc<WriterConfig<T>>) -> Self {
        Self {
            writer,
            open: BundleFiles::default(),
        }
    }
}

/// A copy starts with no open files: they belong to the bundle that opened them.
impl<T> Clone for WriteBundlesFn<T> {
    fn clone(&self) -> Self {
        Self::new(Arc::clone(&self.writer))
    }
}

impl<T: DefaultCoder> DoFn for WriteBundlesFn<T> {
    // One temporary file per window, keyed by the encoded window, so each window is processed
    // on its own.
    type In = PerWindow<T>;
    type Out = Vec<u8>;

    fn start_bundle(&mut self) -> beam::Result {
        // Files left open by a failed bundle are abandoned; finalization never sees them.
        self.open.clear();
        Ok(())
    }

    fn process_element(
        &mut self,
        PerWindow(element): PerWindow<T>,
        out: &mut ProcessContext<Vec<u8>>,
    ) -> beam::Result {
        // Probe with the borrowed window: the key is only allocated when a file opens,
        // and the entry only leaves the map when its file fills.
        let header = beam::internals::element_header(out);
        let window = header.window_bytes();
        let full = match self.open.get_mut(window) {
            Some(open) => {
                open.file.write(&element)?;
                self.writer.is_full(&mut open.file)
            }
            None => {
                let mut file = self.writer.open(None, 0)?;
                file.write(&element)?;
                let full = self.writer.is_full(&mut file);
                let header = result_header(header)?;
                self.open
                    .insert(window.to_vec(), BundleFile { file, header });
                full
            }
        };
        if full && let Some(BundleFile { file, header }) = self.open.remove(window) {
            beam::internals::output_windowed(out, file.finish()?, &header).emit()?;
        }
        Ok(())
    }

    fn finish_bundle(&mut self, out: &mut FinishBundleContext<Vec<u8>>) -> beam::Result {
        self.open
            .drain()
            .try_for_each(|(_, BundleFile { file, header })| {
                beam::internals::finish_output_windowed(out, file.finish()?, &header).emit()
            })
    }
}

/// Header for a result emitted outside `process_element`, timestamped at the end of its
/// window so it is not late.
fn result_header(header: &WindowedHeader) -> beam::Result<WindowedHeader> {
    Ok(match header.single_window()?.interval() {
        Some(w) => header.rebuilt(
            w.max_timestamp().as_millis(),
            header.pane(),
            &header.metadata(),
        ),
        None => header.clone(),
    })
}
