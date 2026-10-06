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

//! Fault-tolerant, sharded and rolling file writes, aligned with Beam's `WriteFiles`.
//!
//! # How a write executes
//!
//! Elements go to uniquely named *temporary* files, which are renamed to their final names
//! after every temporary file of a window is written. So a failed or retried bundle can
//! leave orphaned temporary files, but never a partial or duplicated final file.
//!
//! ```text
//!             ┌─ fixed shards ── AssignShard ─ GroupByKey ─ WriteShards ─┐
//! input ──────┤                                                          ├─ Finalize ─ filenames
//!             └─ runner-chosen ─ WriteBundles ───────────────────────────┘
//! ```
//!
//! * **Fixed sharding** ([`with_num_shards`](WriteFiles::with_num_shards)) spreads elements
//!   round-robin over `n` shard keys; each shard of each window becomes one file.
//!   [`without_sharding`](WriteFiles::without_sharding) writes exactly the requested path.
//! * **Runner-chosen sharding** (default, `num_shards == 0`) writes one file per bundle and
//!   window.
//! * **Rolling** ([`with_max_records_per_file`](WriteFiles::with_max_records_per_file),
//!   [`with_max_bytes_per_file`](WriteFiles::with_max_bytes_per_file)) starts a new file
//!   when the current one reaches a limit, in either sharding mode.
//!
//! Finalization numbers the files of each window and pane `0..n` and renames them to
//! [`FilenamePolicy::file_path`], by default `prefix-SSSSS-of-NNNNN.ext`. Without windowed
//! writes it runs once after the whole input is written, so an empty input still produces
//! its (empty) output files.
//!
//! # Temporary files
//!
//! * **Unwindowed writes** delete all orphaned temporary files and the temporary directory
//!   when the finalizer finishes.
//! * **Windowed writes** finalize each window and pane as the watermark passes. Each window
//!   removes only its own temporary files, and the directory is removed only when empty,
//!   to avoid races with concurrent bundles. Failed bundles can leave orphaned files.
//!
//! For windowed writes to object stores such as `gs://` or `s3://`, use a dedicated
//! [`with_temp_directory`](WriteFiles::with_temp_directory) prefix or the default
//! `.temp-beam-*` sibling, and add a bucket lifecycle rule that deletes objects older than
//! 1–3 days.

mod bundles;
mod composite;
mod finalize;
mod sharding;
mod writer;

use std::collections::HashMap;
use std::sync::Arc;

use model::pipeline as proto;

use crate::filename_policy::{DefaultFilenamePolicy, FilenamePolicy};
use crate::sink::FileSink;
use beam::coders::DefaultCoder;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{GroupByKey, PTransform, ParDo};
use beam::values::PCollection;
use beam::windowing::{GlobalWindows, WindowInto, is_globally_windowed};

use bundles::WriteBundlesFn;
pub(crate) use composite::{add_composite, transform_ids};
// Exposed only so that tests/ can exercise finalization (retry, shard fallback, cleanup).
#[doc(hidden)]
pub use finalize::Finalizer;
use finalize::{FinalizeUnwindowedFn, FinalizeWindowedFn, KeyResultsFn};
#[doc(hidden)]
pub use sharding::AssignShardFn;
use sharding::WriteShardsFn;
#[doc(hidden)]
pub use writer::{FileResult, WriterConfig};

/// Default shard template: `-00000-of-00004`.
pub const DEFAULT_SHARD_TEMPLATE: &str = "-SSSSS-of-NNNNN";

/// Writes a `PCollection<T>` to files through a [`FileSink`] and returns the final paths.
///
/// ```no_run
/// use file::{FormatSink, TextFormat, WriteFiles};
/// # use beam::values::PCollection;
/// # fn f(lines: PCollection<String>) {
/// let filenames = lines.apply(
///     WriteFiles::new("WriteParts", "/tmp/out/part", FormatSink::new(TextFormat))
///         .with_suffix(".txt")
///         .with_num_shards(4),
/// );
/// # }
/// ```
pub struct WriteFiles<T> {
    name: String,
    prefix: String,
    suffix: String,
    sink: Arc<dyn FileSink<T>>,
    num_shards: u32,
    shard_template: String,
    max_records_per_file: Option<u64>,
    max_bytes_per_file: Option<u64>,
    windowed_writes: bool,
    policy: Arc<dyn FilenamePolicy>,
    temp_directory: Option<String>,
}

impl<T> Clone for WriteFiles<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            suffix: self.suffix.clone(),
            sink: Arc::clone(&self.sink),
            num_shards: self.num_shards,
            shard_template: self.shard_template.clone(),
            max_records_per_file: self.max_records_per_file,
            max_bytes_per_file: self.max_bytes_per_file,
            windowed_writes: self.windowed_writes,
            policy: Arc::clone(&self.policy),
            temp_directory: self.temp_directory.clone(),
        }
    }
}

impl<T> std::fmt::Debug for WriteFiles<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteFiles")
            .field("name", &self.name)
            .field("prefix", &self.prefix)
            .field("suffix", &self.suffix)
            .field("num_shards", &self.num_shards)
            .field("shard_template", &self.shard_template)
            .field("max_records_per_file", &self.max_records_per_file)
            .field("max_bytes_per_file", &self.max_bytes_per_file)
            .field("windowed_writes", &self.windowed_writes)
            .field("temp_directory", &self.temp_directory)
            .finish_non_exhaustive()
    }
}

impl<T: 'static> WriteFiles<T> {
    /// Creates a write named `name` to `prefix` through `sink`.
    pub fn new<S: FileSink<T> + 'static>(
        name: impl Into<String>,
        prefix: impl Into<String>,
        sink: S,
    ) -> Self {
        Self {
            name: name.into(),
            prefix: prefix.into(),
            suffix: String::new(),
            sink: Arc::new(sink),
            num_shards: 0,
            shard_template: DEFAULT_SHARD_TEMPLATE.to_string(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            windowed_writes: false,
            policy: Arc::new(DefaultFilenamePolicy),
            temp_directory: None,
        }
    }

    /// Appends `suffix` (for example, `".json"`) to every output file.
    pub fn with_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.suffix = suffix.into();
        self
    }

    /// Sets a fixed shard count. Every shard of every window produces a file, empty shards
    /// too. `0` (the default) writes one file per bundle.
    pub fn with_num_shards(mut self, num_shards: u32) -> Self {
        self.num_shards = num_shards;
        self
    }

    /// Overrides the shard template; see [`format_shard_template`](crate::format_shard_template).
    pub fn with_shard_template(mut self, template: impl Into<String>) -> Self {
        self.shard_template = template.into();
        self
    }

    /// Writes a single file named exactly `prefix + suffix`.
    pub fn without_sharding(self) -> Self {
        self.with_num_shards(1).with_shard_template("")
    }

    /// Rolls to a new file when a file has written `max_records` elements.
    pub fn with_max_records_per_file(mut self, max_records: u64) -> Self {
        self.max_records_per_file = Some(max_records);
        self
    }

    /// Rolls to a new file when a file has written roughly `max_bytes` bytes.
    pub fn with_max_bytes_per_file(mut self, max_bytes: u64) -> Self {
        self.max_bytes_per_file = Some(max_bytes);
        self
    }

    /// Writes separate files per window and pane. Required for unbounded input. Orphaned
    /// temporary files are not deleted; see the module docs.
    pub fn with_windowed_writes(mut self) -> Self {
        self.windowed_writes = true;
        self
    }

    /// Configures how final file names are derived.
    pub fn with_filename_policy<P: FilenamePolicy>(mut self, policy: P) -> Self {
        self.policy = Arc::new(policy);
        self
    }

    /// Places temporary files under `dir` instead of next to the output. `dir` must be on
    /// the output's filesystem, because files are moved by rename.
    pub fn with_temp_directory(mut self, dir: impl Into<String>) -> Self {
        self.temp_directory = Some(dir.into());
        self
    }

    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Whether windowed writes are enabled.
    pub fn is_windowed_writes(&self) -> bool {
        self.windowed_writes
    }

    fn is_rolling(&self) -> bool {
        self.max_records_per_file.is_some() || self.max_bytes_per_file.is_some()
    }

    /// Default temporary directory: a uniquely named sibling of the output files.
    fn default_temp_directory(&self) -> String {
        let dir = self.prefix.rfind('/').map_or("", |i| &self.prefix[..=i]);
        format!("{dir}.temp-beam-{}", writer::unique_id())
    }
}

impl<T> HasDisplayData for WriteFiles<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("transform", &self.name);
        builder.add_text("filenamePrefix", &self.prefix);
        if !self.suffix.is_empty() {
            builder.add_text("fileSuffix", &self.suffix);
        }
        builder.add_integer("numShards", i64::from(self.num_shards));
        if !self.shard_template.is_empty() {
            builder.add_text("shardNameTemplate", &self.shard_template);
        }
        if let Some(max) = self.max_records_per_file {
            builder.add_integer("maxRecordsPerFile", saturating_i64(max));
        }
        if let Some(max) = self.max_bytes_per_file {
            builder.add_integer("maxBytesPerFile", saturating_i64(max));
        }
        if self.windowed_writes {
            builder.add_text("windowedWrites", "true");
        }
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for WriteFiles<T> {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<String> {
        let is_unbounded = input
            .pipeline()
            .lock()
            .components
            .pcollections
            .get(input.id())
            .is_some_and(|p| p.is_bounded == proto::is_bounded::Enum::Unbounded as i32);

        if is_unbounded && !self.windowed_writes {
            panic!(
                "{}: an unbounded PCollection must be written with windowed writes \
                 (.with_windowed_writes())",
                self.name
            );
        }

        let pipeline = input.pipeline().clone();
        let name = pipeline.unique_transform_name(&self.name);
        let existing = transform_ids(&pipeline);
        let input_id = input.id().to_string();

        // Without windowed writes every element belongs in the same set of files, so
        // collapse any upstream windowing first.
        let input = if !self.windowed_writes && !is_globally_windowed(input) {
            input.apply(WindowInto::new(
                format!("{name}/RewindowIntoGlobal"),
                GlobalWindows,
            ))
        } else {
            input.clone()
        };

        let writer = Arc::new(WriterConfig {
            sink: Arc::clone(&self.sink),
            temp_dir: self
                .temp_directory
                .clone()
                .unwrap_or_else(|| self.default_temp_directory()),
            max_records: self.max_records_per_file,
            max_bytes: self.max_bytes_per_file,
        });

        let results: PCollection<Vec<u8>> = if self.num_shards > 0 {
            input
                .apply(ParDo::new(
                    format!("{name}/AssignShard"),
                    AssignShardFn::<T>::new(self.num_shards),
                ))
                .apply(GroupByKey::<i32, T>::new(format!("{name}/GroupByShard")))
                .apply(ParDo::new(
                    format!("{name}/WriteShards"),
                    WriteShardsFn::<T> {
                        writer: Arc::clone(&writer),
                    },
                ))
        } else {
            input.apply(ParDo::new(
                format!("{name}/WriteBundles"),
                WriteBundlesFn::<T>::new(Arc::clone(&writer)),
            ))
        };

        let finalizer = Arc::new(Finalizer {
            prefix: self.prefix.clone(),
            suffix: self.suffix.clone(),
            shard_template: self.shard_template.clone(),
            num_shards: self.num_shards,
            rolling: self.is_rolling(),
            windowed: self.windowed_writes,
            policy: Arc::clone(&self.policy),
            writer,
        });

        let filenames = if self.windowed_writes {
            // One finalization per window firing: gather its files under a single key.
            results
                .apply(ParDo::new(format!("{name}/KeyResults"), KeyResultsFn))
                .apply(GroupByKey::<i32, Vec<u8>>::new(format!(
                    "{name}/GatherResults"
                )))
                .apply(ParDo::new(
                    format!("{name}/Finalize"),
                    FinalizeWindowedFn { finalizer },
                ))
        } else {
            // A single finalization driven by an impulse, reading every result as a side
            // input. Unlike a GroupByKey this still fires when nothing was written.
            let view = results.as_iter();
            let (_, impulse) = pipeline.add_impulse(&format!("{name}/FinalizeImpulse"));
            impulse.apply(
                ParDo::new(
                    format!("{name}/Finalize"),
                    FinalizeUnwindowedFn {
                        finalizer,
                        results: view.clone(),
                    },
                )
                .with_side_input(&view),
            )
        };

        let transform_id = add_composite(
            &pipeline,
            &name,
            &existing,
            HashMap::from([("in".to_string(), input_id)]),
            HashMap::from([("out".to_string(), filenames.id().to_string())]),
        );
        let mut builder = DisplayDataBuilder::with_namespace(&name);
        self.populate_display_data(&mut builder);
        pipeline.set_transform_display_data(&transform_id, builder.into_proto());

        filenames
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
