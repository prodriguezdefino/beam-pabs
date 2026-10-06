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

use std::collections::BTreeMap;
use std::io::{self, BufWriter};
use std::sync::Arc;

use beam::coders::{BeamIterable, DefaultCoder, IntervalWindow, PaneInfo};
use beam::transforms::{DoFn, ProcessContext};
use beam::values::PCollectionView;

use super::writer::{FileResult, WriterConfig, decode_result};
use crate::filename_policy::{FileNamingContext, FilenamePolicy};
use crate::filesystem::{FileSystem, get_filesystem};

/// Names and moves the temporary files of one window firing into place.
///
/// Public only for the crate's integration tests; not part of the supported API.
#[doc(hidden)]
pub struct Finalizer<T> {
    pub prefix: String,
    pub suffix: String,
    pub shard_template: String,
    pub num_shards: u32,
    pub rolling: bool,
    pub windowed: bool,
    pub policy: Arc<dyn FilenamePolicy>,
    pub writer: Arc<WriterConfig<T>>,
}

impl<T: 'static> Finalizer<T> {
    /// Finalizes `results`, returning the final path of every file.
    pub fn finalize(
        &self,
        mut results: Vec<FileResult>,
        window: Option<IntervalWindow>,
        pane: Option<PaneInfo>,
    ) -> beam::Result<Vec<String>> {
        results.sort_by(|a, b| (a.1, &a.0).cmp(&(b.1, &b.0)));
        results.dedup();

        let naming = |index: u32, total: u32| {
            self.policy.file_path(
                &self.prefix,
                &FileNamingContext {
                    windowed: self.windowed,
                    window,
                    pane,
                    shard_index: index,
                    num_shards: total,
                    shard_template: &self.shard_template,
                    suffix: &self.suffix,
                },
            )
        };

        // Fixed sharding keeps each file at its shard index and creates empty files for
        // shards without data. If a shard produced more than one file, number by position.
        let by_shard = self.fixed_shard_layout(&results);

        let moves: Vec<(Option<String>, String)> = if let Some(by_shard) = by_shard {
            (0..self.num_shards)
                .map(|index| {
                    let target = naming(index, self.num_shards);
                    let temp = by_shard.get(&index).map(|r| r.0.clone());
                    (temp, target)
                })
                .collect()
        } else if results.is_empty() {
            // Nothing written: produce one empty file so empty input yields empty output.
            if !self.windowed {
                vec![(None, naming(0, 1))]
            } else {
                Vec::new()
            }
        } else {
            let total = u32::try_from(results.len())
                .map_err(|_| format!("Too many output files: {}", results.len()))?;
            (0..total)
                .zip(results.iter())
                .map(|(index, (temp, _))| (Some(temp.clone()), naming(index, total)))
                .collect()
        };

        moves
            .into_iter()
            .map(|(temp, target)| {
                match temp {
                    Some(temp) => move_into_place(&temp, &target)?,
                    None => self.write_empty(&target)?,
                }
                Ok(target)
            })
            .collect()
    }

    /// Maps shard index to result when fixed sharding produced one file per shard.
    fn fixed_shard_layout<'r>(
        &self,
        results: &'r [FileResult],
    ) -> Option<BTreeMap<u32, &'r FileResult>> {
        if self.num_shards == 0 || self.rolling {
            return None;
        }
        results
            .iter()
            .try_fold(BTreeMap::new(), |mut by_shard, result| {
                let shard = u32::try_from(result.1.0)
                    .ok()
                    .filter(|s| *s < self.num_shards)?;
                if by_shard.insert(shard, result).is_some() {
                    return None;
                }
                Some(by_shard)
            })
    }

    /// Writes an empty file (header and footer only) at `target`.
    fn write_empty(&self, target: &str) -> beam::Result {
        let fs = filesystem_for(target)?;
        let out = fs
            .open_write(target)
            .map_err(|e| beam::Error::from(e).context(format!("Failed to create '{target}'")))?;
        self.writer
            .sink
            .open(Box::new(BufWriter::new(out)))
            .and_then(|w| w.finish())
            .map_err(|e| e.context(format!("Failed to write empty file '{target}'")))
    }

    /// Removes the temporary directory and its contents. Call this only after the single
    /// finalization of a non-windowed write, when nothing else can write there.
    pub fn cleanup(&self) {
        let dir = &self.writer.temp_dir;
        let Ok(fs) = get_filesystem(dir) else {
            return;
        };
        match fs.match_files(&format!("{dir}/*")) {
            Ok(leftovers) => {
                for path in leftovers {
                    if let Err(e) = fs.remove(&path) {
                        tracing::warn!("WriteFiles: failed to remove temporary file '{path}': {e}");
                    }
                }
            }
            Err(e) => tracing::warn!("WriteFiles: failed to list temporary directory '{dir}': {e}"),
        }
        if let Err(e) = fs.remove_dir(dir) {
            tracing::warn!("WriteFiles: failed to remove temporary directory '{dir}': {e}");
        }
    }

    /// Removes the temporary directory if it is empty, for windowed writes. A writer that
    /// finds the directory missing recreates it.
    pub fn remove_temp_dir_if_empty(&self) {
        let dir = &self.writer.temp_dir;
        if let Ok(fs) = get_filesystem(dir) {
            let _ = fs.remove_dir(dir);
        }
    }
}

/// Renames `temp` to `target`, tolerating a rename already done by an earlier attempt.
fn move_into_place(temp: &str, target: &str) -> beam::Result {
    let fs = filesystem_for(temp)?;
    match fs.rename(temp, target) {
        Ok(()) => Ok(()),
        // A retried finalization finds its earlier renames already done.
        Err(e) if e.kind() == io::ErrorKind::NotFound && fs.exists(target).unwrap_or(false) => {
            Ok(())
        }
        Err(e) => Err(format!("Failed to move '{temp}' to '{target}': {e}").into()),
    }
}

fn filesystem_for(path: &str) -> beam::Result<Arc<dyn FileSystem>> {
    get_filesystem(path)
        .map_err(|e| beam::Error::from(e).context(format!("Failed to get filesystem for '{path}'")))
}

/// Puts every result of a window firing under one key.
#[derive(Clone)]
pub(super) struct KeyResultsFn;

impl DoFn for KeyResultsFn {
    type In = Vec<u8>;
    type Out = (i32, Vec<u8>);

    fn process_element(
        &mut self,
        result: Vec<u8>,
        out: &mut ProcessContext<(i32, Vec<u8>)>,
    ) -> beam::Result {
        out.emit((0, result))
    }
}

/// Finalizes one window firing of a windowed write.
pub(super) struct FinalizeWindowedFn<T> {
    pub(super) finalizer: Arc<Finalizer<T>>,
}

impl<T> Clone for FinalizeWindowedFn<T> {
    fn clone(&self) -> Self {
        Self {
            finalizer: Arc::clone(&self.finalizer),
        }
    }
}

impl<T: DefaultCoder> DoFn for FinalizeWindowedFn<T> {
    type In = (i32, BeamIterable<Vec<u8>>);
    type Out = String;

    fn process_element(
        &mut self,
        (_, encoded): (i32, BeamIterable<Vec<u8>>),
        out: &mut ProcessContext<String>,
    ) -> beam::Result {
        let results = encoded
            .try_into_iter()
            .map(|bytes| {
                let bytes = bytes
                    .map_err(|e| beam::Error::from(e).context("Failed to read file results"))?;
                decode_result(&bytes)
            })
            .collect::<beam::Result<Vec<_>>>()?;
        let targets = self
            .finalizer
            .finalize(results, out.interval_window(), Some(out.pane()))?;
        let finalizer = Arc::clone(&self.finalizer);
        out.register_finalizer(move || {
            tracing::debug!(
                "Bundle finalizer: removing temp directory if empty for windowed write"
            );
            finalizer.remove_temp_dir_if_empty();
            Ok(())
        });
        targets.into_iter().try_for_each(|t| out.emit(t))
    }

    fn requests_finalization(&self) -> bool {
        true
    }
}

/// Finalizes a non-windowed write, once, after all of its files are written.
pub(super) struct FinalizeUnwindowedFn<T> {
    pub(super) finalizer: Arc<Finalizer<T>>,
    pub(super) results: PCollectionView<Vec<u8>>,
}

impl<T> Clone for FinalizeUnwindowedFn<T> {
    fn clone(&self) -> Self {
        Self {
            finalizer: Arc::clone(&self.finalizer),
            results: self.results.clone(),
        }
    }
}

impl<T: DefaultCoder> DoFn for FinalizeUnwindowedFn<T> {
    type In = Vec<u8>;
    type Out = String;

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        out: &mut ProcessContext<String>,
    ) -> beam::Result {
        let results = out
            .side_input_iter(&self.results)?
            .iter()
            .map(|bytes| decode_result(bytes))
            .collect::<Result<Vec<_>, _>>()?;
        let targets = self.finalizer.finalize(results, None, None)?;
        let finalizer = Arc::clone(&self.finalizer);
        out.register_finalizer(move || {
            tracing::debug!("Bundle finalizer: executing cleanup for unwindowed write");
            finalizer.cleanup();
            Ok(())
        });
        targets.into_iter().try_for_each(|t| out.emit(t))
    }

    fn requests_finalization(&self) -> bool {
        true
    }
}
