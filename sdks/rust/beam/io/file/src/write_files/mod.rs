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

//! Fault-tolerant, sharded, and rolling file writes.
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
mod finalize;
mod sharding;
mod writer;

use std::collections::HashMap;
use std::sync::Arc;

use crate::filename_policy::{DefaultFilenamePolicy, FilenamePolicy};
use crate::sink::FileSink;
use beam::coders::DefaultCoder;
use beam::internals::map_windowing_strategy;
use beam::pipeline::{PipelineError, URN_RUST_DOFN, URN_WRITE_FILES};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::{GroupByKey, PTransform, ParDo, WithKeys};
use beam::values::{IsBounded, PCollection};
use beam::windowing::{AccumulationMode, GlobalWindows, Trigger, WindowInto, is_globally_windowed};
use model::pipeline::{
    FunctionSpec, WindowingStrategy, WriteFilesPayload,
    accumulation_mode::Enum as AccumulationModeProto,
};
use prost::Message;

use bundles::WriteBundlesFn;
// Exposed only so that tests/ can exercise finalization (retry, shard fallback, cleanup).
#[doc(hidden)]
pub use finalize::Finalizer;
use finalize::{FinalizeUnwindowedFn, FinalizeWindowedFn, KeyResultsFn};
#[doc(hidden)]
pub use sharding::RoundRobinShards;
use sharding::WriteShardsFn;
#[doc(hidden)]
pub use writer::{FileResult, WriterConfig};

/// Options for the inner [`WriteFiles`] transform.
#[doc(hidden)]
#[derive(Clone)]
pub struct WriteFilesOptions {
    pub suffix: String,
    pub num_shards: u32,
    pub shard_template: String,
    pub max_records_per_file: Option<u64>,
    pub max_bytes_per_file: Option<u64>,
    pub windowed_writes: bool,
    pub policy: Arc<dyn FilenamePolicy>,
    pub temp_directory: Option<String>,
}

impl WriteFilesOptions {
    /// Builds options with `suffix` as the default file extension.
    pub fn with_default_suffix(suffix: &str) -> Self {
        Self {
            suffix: suffix.to_string(),
            num_shards: 0,
            shard_template: DEFAULT_SHARD_TEMPLATE.to_string(),
            max_records_per_file: None,
            max_bytes_per_file: None,
            windowed_writes: false,
            policy: Arc::new(DefaultFilenamePolicy),
            temp_directory: None,
        }
    }
}

impl Default for WriteFilesOptions {
    fn default() -> Self {
        Self::with_default_suffix("")
    }
}

impl std::fmt::Debug for WriteFilesOptions {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteFilesOptions")
            .field("suffix", &self.suffix)
            .field("num_shards", &self.num_shards)
            .field("shard_template", &self.shard_template)
            .field("max_records_per_file", &self.max_records_per_file)
            .field("max_bytes_per_file", &self.max_bytes_per_file)
            .field("windowed_writes", &self.windowed_writes)
            .field("policy", &"<FilenamePolicy>")
            .field("temp_directory", &self.temp_directory)
            .finish()
    }
}

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
    sink: Arc<dyn FileSink<T>>,
    options: WriteFilesOptions,
}

impl<T> Clone for WriteFiles<T> {
    fn clone(&self) -> Self {
        Self {
            name: self.name.clone(),
            prefix: self.prefix.clone(),
            sink: Arc::clone(&self.sink),
            options: self.options.clone(),
        }
    }
}

impl<T> std::fmt::Debug for WriteFiles<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("WriteFiles")
            .field("name", &self.name)
            .field("prefix", &self.prefix)
            .field("options", &self.options)
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
            sink: Arc::new(sink),
            options: WriteFilesOptions::default(),
        }
    }

    /// Sets inner write file options.
    #[doc(hidden)]
    pub fn with_options(mut self, options: WriteFilesOptions) -> Self {
        self.options = options;
        self
    }

    /// Appends `suffix` (for example, `".json"`) to every output file.
    pub fn with_suffix(mut self, suffix: impl Into<String>) -> Self {
        self.options.suffix = suffix.into();
        self
    }

    /// Sets a fixed shard count. Every shard of every window produces a file, empty shards
    /// too. `0` (the default) writes one file per bundle.
    pub fn with_num_shards(mut self, num_shards: u32) -> Self {
        self.options.num_shards = num_shards;
        self
    }

    /// Overrides the shard template; see [`format_shard_template`](crate::format_shard_template).
    pub fn with_shard_template(mut self, template: impl Into<String>) -> Self {
        self.options.shard_template = template.into();
        self
    }

    /// Writes a single file named exactly `prefix + suffix`.
    pub fn without_sharding(self) -> Self {
        self.with_num_shards(1).with_shard_template("")
    }

    /// Rolls to a new file when a file has written `max_records` elements.
    pub fn with_max_records_per_file(mut self, max_records: u64) -> Self {
        self.options.max_records_per_file = Some(max_records);
        self
    }

    /// Rolls to a new file when a file has written roughly `max_bytes` bytes.
    pub fn with_max_bytes_per_file(mut self, max_bytes: u64) -> Self {
        self.options.max_bytes_per_file = Some(max_bytes);
        self
    }

    /// Writes separate files per window and pane. Required for unbounded input. Orphaned
    /// temporary files are not deleted; see the module docs.
    pub fn with_windowed_writes(mut self) -> Self {
        self.options.windowed_writes = true;
        self
    }

    /// Configures how final file names are derived.
    pub fn with_filename_policy<P: FilenamePolicy>(mut self, policy: P) -> Self {
        self.options.policy = Arc::new(policy);
        self
    }

    /// Places temporary files under `dir` instead of next to the output. `dir` must be on
    /// the output's filesystem, because files are moved by rename.
    pub fn with_temp_directory(mut self, dir: impl Into<String>) -> Self {
        self.options.temp_directory = Some(dir.into());
        self
    }

    /// Returns the output file path prefix.
    pub fn prefix(&self) -> &str {
        &self.prefix
    }

    /// Whether windowed writes are enabled.
    pub fn is_windowed_writes(&self) -> bool {
        self.options.windowed_writes
    }

    fn is_rolling(&self) -> bool {
        self.options.max_records_per_file.is_some() || self.options.max_bytes_per_file.is_some()
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
        if !self.options.suffix.is_empty() {
            builder.add_text("fileSuffix", &self.options.suffix);
        }
        builder.add_integer("numShards", i64::from(self.options.num_shards));
        if !self.options.shard_template.is_empty() {
            builder.add_text("shardNameTemplate", &self.options.shard_template);
        }
        if let Some(max) = self.options.max_records_per_file {
            builder.add_integer("maxRecordsPerFile", saturating_i64(max));
        }
        if let Some(max) = self.options.max_bytes_per_file {
            builder.add_integer("maxBytesPerFile", saturating_i64(max));
        }
        if self.options.windowed_writes {
            builder.add_text("windowedWrites", "true");
        }
    }
}

impl<T: DefaultCoder> PTransform<PCollection<T>> for WriteFiles<T> {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<T>) -> PCollection<String> {
        let is_unbounded = input.is_bounded() == IsBounded::Unbounded;

        // A configuration error: record it, so `Pipeline::validate` fails, and build the
        // write anyway so the later transforms still attach to its output.
        if is_unbounded && !self.options.windowed_writes {
            input
                .pipeline()
                .record_construction_error(PipelineError::InvalidConstruction {
                    transform_id: self.name.clone(),
                    reason: "an unbounded PCollection must be written with windowed writes \
                             (.with_windowed_writes())"
                        .to_string(),
                });
        }

        let payload = WriteFilesPayload {
            sink: Some(FunctionSpec {
                urn: URN_RUST_DOFN.to_string(),
                payload: Vec::new(),
            }),
            format_function: None,
            windowed_writes: self.options.windowed_writes,
            runner_determined_sharding: self.options.num_shards == 0,
            side_inputs: HashMap::new(),
            auto_sharded: false,
        }
        .encode_to_vec();

        crate::composite::expand_with_spec(
            input,
            &self.name,
            URN_WRITE_FILES,
            payload,
            self,
            |pipeline, name| {
                // Without windowed writes every element belongs in the same set of files, so
                // collapse any upstream windowing first.
                let input = if !self.options.windowed_writes && !is_globally_windowed(input) {
                    // The default trigger fires once, when the input ends. An upstream trigger
                    // would write a set of files for each pane.
                    input.apply(
                        WindowInto::new(format!("{name}/RewindowIntoGlobal"), GlobalWindows)
                            .with_trigger(Trigger::Default)
                            .with_accumulation_mode(AccumulationMode::Discarding),
                    )
                } else {
                    input.clone()
                };

                let writer = Arc::new(WriterConfig {
                    sink: Arc::clone(&self.sink),
                    temp_dir: self
                        .options
                        .temp_directory
                        .clone()
                        .unwrap_or_else(|| self.default_temp_directory()),
                    max_records: self.options.max_records_per_file,
                    max_bytes: self.options.max_bytes_per_file,
                });

                let results: PCollection<Vec<u8>> = if self.options.num_shards > 0 {
                    let shards = RoundRobinShards::new(self.options.num_shards);
                    input
                        .apply(WithKeys::new(
                            format!("{name}/AssignShard"),
                            move |_: &T| shards.next_shard(),
                        ))
                        .apply(GroupByKey::new(format!("{name}/GroupByShard")))
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
                    suffix: self.options.suffix.clone(),
                    shard_template: self.options.shard_template.clone(),
                    num_shards: self.options.num_shards,
                    rolling: self.is_rolling(),
                    policy: Arc::clone(&self.options.policy),
                    writer,
                });

                if self.options.windowed_writes {
                    // Gather the files of each window and pane under the pane index. Each file
                    // must reach the finalizer once, so the gathering discards fired panes.
                    let results = map_windowing_strategy(results, |strategy| WindowingStrategy {
                        accumulation_mode: AccumulationModeProto::Discarding as i32,
                        ..strategy
                    });
                    results
                        .apply(ParDo::new(format!("{name}/KeyResults"), KeyResultsFn))
                        .apply(GroupByKey::<i64, (Vec<u8>, Vec<u8>)>::new(format!(
                            "{name}/GatherResults"
                        )))
                        .apply(ParDo::new(
                            format!("{name}/Finalize"),
                            FinalizeWindowedFn { finalizer },
                        ))
                } else {
                    // A single finalization driven by an impulse, reading every result as a side
                    // input. Unlike a GroupByKey this still fires when nothing was written.
                    let (_, impulse) = pipeline.add_impulse(&format!("{name}/FinalizeImpulse"));
                    impulse.apply(ParDo::new(
                        format!("{name}/Finalize"),
                        FinalizeUnwindowedFn {
                            finalizer,
                            results: results.as_iter(),
                        },
                    ))
                }
            },
        )
    }
}

fn saturating_i64(value: u64) -> i64 {
    i64::try_from(value).unwrap_or(i64::MAX)
}
