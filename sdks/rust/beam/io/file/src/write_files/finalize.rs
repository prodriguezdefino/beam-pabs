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
use std::io;
use std::sync::Arc;

use beam::coders::{BeamIterable, DefaultCoder, PaneInfo};
use beam::transforms::{DoFn, ProcessContext, Windowed, WithFinalizer};
use beam::values::IterView;

use super::writer::{FileResult, WriterConfig, decode_result};
use crate::filename_policy::{FileNamingContext, FilenamePolicy, Windowing};
use crate::filesystem::{FileSystem, get_filesystem};
use crate::sink::FileOutput;

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
    pub policy: Arc<dyn FilenamePolicy>,
    pub writer: Arc<WriterConfig<T>>,
}

impl<T: 'static> Finalizer<T> {
    /// Finalizes `results`, returning the final path of every file.
    pub fn finalize(
        &self,
        mut results: Vec<FileResult>,
        windowing: Windowing,
    ) -> beam::Result<Vec<String>> {
        results.sort_by(|a, b| {
            (a.shard, a.sequence, &a.temp_path).cmp(&(b.shard, b.sequence, &b.temp_path))
        });
        results.dedup();

        let naming = |index: u32, total: u32| {
            let path = self.policy.file_path(
                &self.prefix,
                &FileNamingContext {
                    windowing,
                    shard_index: index,
                    num_shards: total,
                    shard_template: &self.shard_template,
                    suffix: &self.suffix,
                },
            );
            self.writer.compression.append_suffix(path)
        };

        // Fixed sharding keeps each file at its shard index and creates empty files for
        // shards without data. If a shard produced more than one file, number by position.
        let by_shard = self.fixed_shard_layout(&results);

        let moves: Vec<(Option<String>, String)> = if let Some(by_shard) = by_shard {
            (0..self.num_shards)
                .map(|index| {
                    let target = naming(index, self.num_shards);
                    let temp = by_shard.get(&index).map(|r| r.temp_path.clone());
                    (temp, target)
                })
                .collect()
        } else if results.is_empty() {
            // Nothing written: produce one empty file so empty input yields empty output.
            if matches!(windowing, Windowing::Unwindowed) {
                vec![(None, naming(0, 1))]
            } else {
                Vec::new()
            }
        } else {
            let total = u32::try_from(results.len())
                .map_err(|_| format!("Too many output files: {}", results.len()))?;
            (0..total)
                .zip(results.iter())
                .map(|(index, result)| (Some(result.temp_path.clone()), naming(index, total)))
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
                let shard = result.shard.filter(|s| *s < self.num_shards)?;
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
        let out = FileOutput::new(out, self.writer.compression).map_err(|e| {
            beam::Error::from(e).context(format!("Failed to initialize compression for '{target}'"))
        })?;
        self.writer
            .sink
            .open(out)
            .and_then(|w| w.finish())
            .and_then(|out| Ok(out.close()?))
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
        match get_filesystem(dir) {
            Ok(fs) => {
                if let Err(e) = fs.remove_dir(dir)
                    && e.kind() != io::ErrorKind::NotFound
                    && e.kind() != io::ErrorKind::DirectoryNotEmpty
                {
                    tracing::warn!("WriteFiles: failed to remove temporary directory '{dir}': {e}");
                }
            }
            Err(e) => {
                tracing::warn!(
                    "WriteFiles: failed to get filesystem for temporary directory '{dir}': {e}"
                );
            }
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

/// Keys each result by the index of the pane that wrote it, and keeps that pane with the
/// result. The finalizer names the files by this pane, not by the pane of the gathering.
#[derive(Clone)]
pub(super) struct KeyResultsFn;

impl DoFn for KeyResultsFn {
    type In = Vec<u8>;
    type Out = (i64, (Vec<u8>, Vec<u8>));

    fn process_element(
        &mut self,
        result: Vec<u8>,
        out: &mut ProcessContext<Self::Out>,
    ) -> beam::Result {
        let pane = out.pane();
        let mut encoded_pane = Vec::new();
        pane.encode(false, &mut encoded_pane)?;
        // A pane with no index is one unknown firing: its results share the key -1.
        let key = pane
            .index()
            .map_or(-1, |index| i64::try_from(index).unwrap_or(i64::MAX));
        out.emit((key, (encoded_pane, result)))
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
    type In = Windowed<(i64, BeamIterable<(Vec<u8>, Vec<u8>)>)>;
    type Out = WithFinalizer<String>;

    fn process_element(
        &mut self,
        Windowed {
            value: (_, encoded),
            window,
        }: Self::In,
        out: &mut ProcessContext<Self::Out>,
    ) -> beam::Result {
        let (panes, results): (Vec<Vec<u8>>, Vec<_>) = encoded
            .try_into_iter()
            .map(|item| {
                let (pane, bytes) =
                    item.map_err(|e| beam::Error::from(e).context("Failed to read file results"))?;
                Ok((pane, decode_result(&bytes)?))
            })
            .collect::<beam::Result<Vec<_>>>()?
            .into_iter()
            .unzip();
        // Every result of the group has the same pane: the group key is its index.
        let pane = match panes.first() {
            Some(encoded) => PaneInfo::decode(&mut encoded.as_slice())?.0,
            None => out.pane(),
        };
        let targets = self.finalizer.finalize(
            results,
            Windowing::Windowed {
                window: window.interval(),
                pane,
            },
        )?;
        let finalizer = Arc::clone(&self.finalizer);
        out.finalizer(move || {
            tracing::debug!(
                "Bundle finalizer: removing temp directory if empty for windowed write"
            );
            finalizer.remove_temp_dir_if_empty();
            Ok(())
        })
        .register()?;
        targets.into_iter().try_for_each(|t| out.emit(t.into()))
    }
}

/// Finalizes a non-windowed write, once, after all of its files are written.
pub(super) struct FinalizeUnwindowedFn<T> {
    pub(super) finalizer: Arc<Finalizer<T>>,
    pub(super) results: IterView<Vec<u8>>,
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
    type Out = WithFinalizer<String>;

    fn side_inputs(&self) -> Vec<&dyn beam::values::AnySideInput> {
        vec![&self.results]
    }

    fn process_element(
        &mut self,
        _impulse: Vec<u8>,
        out: &mut ProcessContext<Self::Out>,
    ) -> beam::Result {
        let results = out
            .side_input(&self.results)?
            .iter()?
            .map(|bytes| decode_result(&bytes?))
            .collect::<Result<Vec<_>, _>>()?;
        let targets = self.finalizer.finalize(results, Windowing::Unwindowed)?;
        let finalizer = Arc::clone(&self.finalizer);
        out.finalizer(move || {
            tracing::debug!("Bundle finalizer: executing cleanup for unwindowed write");
            finalizer.cleanup();
            Ok(())
        })
        .register()?;
        targets.into_iter().try_for_each(|t| out.emit(t.into()))
    }
}
