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

//! Naming of output files, aligned with Beam's `FilenamePolicy` and `ShardNameTemplate`.

use beam::coders::{IntervalWindow, PaneInfo, Timing};

/// Chooses the final path of each file that [`WriteFiles`](crate::WriteFiles) writes.
/// Closures `Fn(&str, &FileNamingContext<'_>) -> String` implement it:
///
/// ```
/// use file::{FileNamingContext, FilenamePolicy};
///
/// let policy = |prefix: &str, ctx: &FileNamingContext<'_>| {
///     format!("{prefix}{}{}", ctx.shard_string(), ctx.suffix)
/// };
/// # let _: &dyn FilenamePolicy = &policy;
/// ```
///
/// Every file of a write must get a distinct name: include
/// [`FileNamingContext::shard_string`], and for windowed writes the window and pane.
pub trait FilenamePolicy: Send + Sync + 'static {
    /// The final path of the file described by `ctx`, for an output `prefix`.
    fn file_path(&self, prefix: &str, ctx: &FileNamingContext<'_>) -> String;
}

impl<F> FilenamePolicy for F
where
    F: Fn(&str, &FileNamingContext<'_>) -> String + Send + Sync + 'static,
{
    fn file_path(&self, prefix: &str, ctx: &FileNamingContext<'_>) -> String {
        self(prefix, ctx)
    }
}

/// Everything known about an output file when its final name is chosen.
#[derive(Clone, Copy, Debug)]
pub struct FileNamingContext<'a> {
    /// Whether the write produces separate files per window and pane.
    pub windowed: bool,
    /// The window being written, when it is an interval window.
    pub window: Option<IntervalWindow>,
    /// The pane being written, when windowed.
    pub pane: Option<PaneInfo>,
    /// Zero-based index of this file among the files of its window and pane.
    pub shard_index: u32,
    /// Number of files written for this window and pane.
    pub num_shards: u32,
    /// Shard template such as `-SSSSS-of-NNNNN`; empty disables shard naming.
    pub shard_template: &'a str,
    /// Suffix appended after the shard, such as `.parquet`. May be empty.
    pub suffix: &'a str,
}

impl FileNamingContext<'_> {
    /// The shard template with its placeholders filled in, e.g. `-00001-of-00004`.
    pub fn shard_string(&self) -> String {
        format_shard_template(self.shard_template, self.shard_index, self.num_shards)
    }

    /// The window and pane as a fragment, e.g. `-0-60000` or `-0-60000-pane-2-late`. Empty
    /// when not windowed. The pane is omitted for a single on-time firing.
    pub fn window_string(&self) -> String {
        if !self.windowed {
            return String::new();
        }
        let window = self.window.map_or_else(
            || "-GlobalWindow".to_string(),
            |w| format!("-{}-{}", w.start_millis, w.end_millis),
        );
        let pane = match self.pane {
            Some(p)
                if !(p.is_first && p.is_last && p.timing == Timing::OnTime)
                    && p.timing != Timing::Unknown
                    && p != PaneInfo::NO_FIRING =>
            {
                let timing = match p.timing {
                    Timing::Early => "early",
                    Timing::OnTime => "on-time",
                    Timing::Late => "late",
                    Timing::Unknown => "unknown",
                };
                format!("-pane-{}-{timing}", p.index)
            }
            _ => String::new(),
        };
        format!("{window}{pane}")
    }
}

/// The default policy, following Beam's `prefix-window-pane-SSSSS-of-NNNNN.suffix`.
///
/// Window and shard fragments go before the suffix or, without a suffix, before the
/// prefix's extension: `out.txt` becomes `out-00000-of-00004.txt`.
#[derive(Clone, Copy, Debug, Default)]
pub struct DefaultFilenamePolicy;

impl FilenamePolicy for DefaultFilenamePolicy {
    fn file_path(&self, prefix: &str, ctx: &FileNamingContext<'_>) -> String {
        let middle = format!("{}{}", ctx.window_string(), ctx.shard_string());
        if ctx.suffix.is_empty() {
            insert_before_extension(prefix, &middle)
        } else {
            format!("{prefix}{middle}{}", ctx.suffix)
        }
    }
}

/// Expands a shard template, following Beam's `ShardNameTemplate` conventions.
///
/// Each run of `S` becomes the shard index and each run of `N` the shard count, zero-padded
/// to the run length: `-SSSSS-of-NNNNN` for shard 1 of 4 is `-00001-of-00004`.
pub fn format_shard_template(template: &str, shard_index: u32, num_shards: u32) -> String {
    let mut out = String::with_capacity(template.len());
    let mut chars = template.chars().peekable();
    while let Some(c) = chars.next() {
        let value = match c {
            'S' => shard_index,
            'N' => num_shards,
            other => {
                out.push(other);
                continue;
            }
        };
        let mut width = 1;
        while chars.next_if_eq(&c).is_some() {
            width += 1;
        }
        out.push_str(&format!("{value:0width$}"));
    }
    out
}

/// Inserts `text` before the extension of the file name in `path`, or appends it.
fn insert_before_extension(path: &str, text: &str) -> String {
    let file_start = path.rfind('/').map_or(0, |i| i + 1);
    match path[file_start..].rfind('.') {
        // A leading dot marks a hidden file, not an extension.
        Some(dot) if dot > 0 => {
            let at = file_start + dot;
            format!("{}{text}{}", &path[..at], &path[at..])
        }
        _ => format!("{path}{text}"),
    }
}
