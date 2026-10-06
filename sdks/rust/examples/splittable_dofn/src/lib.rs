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

//! A splittable DoFn that splits each text into its lines.
//!
//! [`SplitLines`] shows the parts of a [`SplittableDoFn`]:
//!
//! - the initial restriction: the byte range of the whole text, as an [`OffsetRange`];
//! - the initial split into chunks of `chunk_bytes`, which the runner can process in parallel;
//! - an [`OffsetRangeTracker`] that claims the start offset of each line;
//! - a self-checkpoint with [`ProcessContinuation::Resume`] after `lines_per_call` lines.
//!
//! A line belongs to the restriction that contains its start offset. So each line is
//! emitted once, for all split sizes.
//!
//! Run on Prism, the default runner:
//! ```text
//! cargo run -p splittable_dofn -- --input='/path/to/*.txt' --output=/tmp/lines.txt
//! ```

use std::iter;
use std::num::NonZeroUsize;

use beam::io::file::fileio::{self, ReadableFile};
use beam::prelude::*;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo,
};
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// The options of the example.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "splittable_dofn",
    about = "Apache Beam splittable DoFn example",
    version
)]
pub struct Args {
    /// Input file path or glob pattern. Each file is one text.
    #[arg(long)]
    pub input: String,

    /// Output file path for the lines.
    #[arg(long, default_value = "/tmp/splittable_dofn_output.txt")]
    pub output: String,

    /// Size in bytes of the initial splits of each text.
    #[arg(long, default_value_t = 64 * 1024)]
    pub chunk_bytes: i64,

    /// Number of lines to emit before a self-checkpoint.
    #[arg(long, default_value = "1000")]
    pub lines_per_call: NonZeroUsize,
}

impl PipelineOptionGroup for Args {}

/// Emits the lines of each input text. The restriction is a byte range of the text.
#[derive(Clone, Debug, Serialize, Deserialize, Register)]
pub struct SplitLines {
    chunk_bytes: i64,
    lines_per_call: NonZeroUsize,
}

impl SplitLines {
    /// Splits each text into chunks of `chunk_bytes` (no split if not positive), and
    /// checkpoints after `lines_per_call` lines.
    pub fn new(chunk_bytes: i64, lines_per_call: NonZeroUsize) -> Self {
        Self {
            chunk_bytes,
            lines_per_call,
        }
    }
}

impl SplittableDoFn for SplitLines {
    type In = String;
    type Out = String;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, text: &String) -> Result<OffsetRange> {
        Ok(OffsetRange::new(0, i64::try_from(text.len())?))
    }

    fn split_restriction(&self, _text: &String, restriction: &OffsetRange) -> Vec<OffsetRange> {
        restriction.sized_splits(self.chunk_bytes)
    }

    fn restriction_size(&self, _text: &String, restriction: &OffsetRange) -> f64 {
        restriction.size()
    }

    fn create_tracker(&self, restriction: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*restriction)
    }

    fn process_element(
        &mut self,
        text: String,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, String>,
    ) -> Result<ProcessContinuation> {
        Metrics::counter("splittable_dofn", "process_calls").inc();
        let from = usize::try_from(tracker.current_restriction().start)?;
        // A failed claim means that the line starts after the restriction: the work is done.
        let emitted = line_starts(&text, from)
            .take(self.lines_per_call.get())
            .take_while(|&start| i64::try_from(start).is_ok_and(|pos| tracker.try_claim(&pos)))
            .try_fold(0, |count, start| {
                ctx.emit(line_at(&text, start).to_owned())
                    .map(|()| count + 1)
            })?;
        Ok(if emitted == self.lines_per_call.get() {
            ProcessContinuation::Resume
        } else {
            ProcessContinuation::Stop
        })
    }
}

/// The start offsets of the lines that start at or after `from`. The last offset is at or
/// after `text.len()`, so a claim of it always fails.
fn line_starts(text: &str, from: usize) -> impl Iterator<Item = usize> + '_ {
    let first = match from {
        0 => 0,
        _ => text
            .as_bytes()
            .get(from - 1..)
            .and_then(|rest| rest.iter().position(|&b| b == b'\n'))
            .map_or(text.len(), |newline| from + newline),
    };
    iter::successors(Some(first), move |&start| {
        (start < text.len()).then(|| line_end(text, start) + 1)
    })
}

/// The offset of the newline that ends the line at `start`, or `text.len()`.
fn line_end(text: &str, start: usize) -> usize {
    text.as_bytes()
        .get(start..)
        .and_then(|rest| rest.iter().position(|&b| b == b'\n'))
        .map_or(text.len(), |newline| start + newline)
}

/// The line at `start`, without its newline. `start` is a line start, so it is on a
/// character boundary.
fn line_at(text: &str, start: usize) -> &str {
    text.get(start..line_end(text, start)).unwrap_or_default()
}

/// Reads the files of `args.input`, splits them into lines and writes the lines.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    p.apply("MatchFiles", fileio::Match::new(&args.input))
        .apply("ReadMatches", fileio::ReadMatches::new())
        .par_do_fn("ReadText", |file: ReadableFile, ctx| {
            ctx.emit(file.read_fully_as_utf8_string()?)
        })
        .apply(
            "SplitLines",
            SplittableParDo::new(SplitLines::new(args.chunk_bytes, args.lines_per_call)),
        )
        .apply("WriteLines", textio::Write::new(&args.output));
    p
}
