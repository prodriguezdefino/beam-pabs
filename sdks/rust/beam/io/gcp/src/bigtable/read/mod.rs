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

//! Cloud Bigtable read connector (`SampleRowKeys` + splittable `ReadRows`).
//!
//! [`ReadRows`] expands into a two-stage pipeline. First, `Impulse` -> [`SampleKeyRangesFn`] ->
//! `Reshuffle` streams `SampleRowKeys` and coalesces tablets into at most `max_initial_bundles`
//! super-ranges while keeping interior [`TabletBoundary`](restriction::TabletBoundary) split
//! points. Then [`SplittableParDo`] runs [`ReadRangesFn`], which splits super-ranges across
//! workers, supports tablet-boundary-first dynamic splitting ([`KeyRangeTracker::try_split`]),
//! claims `last_scanned_row_key` heartbeats during sparse `RowFilter` scans, and checkpoints
//! through [`ProcessContinuation::resume`](beam::transforms::sdf::ProcessContinuation::resume)
//! after `max_silent_scan_duration`.

#[doc(hidden)]
pub mod assembler;
#[doc(hidden)]
pub mod encode;
mod options;
mod output;
#[doc(hidden)]
pub mod reader;
#[doc(hidden)]
pub mod restriction;
#[doc(hidden)]
pub mod sample;
mod stream;

use std::marker::PhantomData;
use std::sync::Arc;

use beam::coders::DefaultCoder;
use beam::internals::PipelineInternals;
use beam::schema::{BeamRow, Row, RowElement};
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::sdf::SplittableParDo;
use beam::transforms::{
    FromRows, Impulse, PTransform, ParDo, Reshuffle, TryPTransform, WithFailures,
};
use beam::values::{PBegin, PCollection};

pub use self::options::{
    DEFAULT_MAX_SILENT_SCAN_DURATION, DEFAULT_READ_ATTEMPT_TIMEOUT, ReadOptions,
};
pub use self::reader::{ReadOutput, read_error_schema};
pub use self::sample::DEFAULT_MAX_INITIAL_BUNDLES;

use self::reader::ReadRangesFn;
use self::restriction::KeyRangeRestriction;
use self::sample::SampleKeyRangesFn;

use crate::bigtable::client::BigtableClient;

/// Cloud Bigtable `PTransform` that reads rows from a table, materialized view, or
/// authorized view using `SampleRowKeys` and splittable `ReadRows`.
#[derive(Clone)]
pub struct ReadRows {
    options: ReadOptions,
    client: Option<Arc<BigtableClient>>,
    encoded_rows: bool,
}

impl ReadRows {
    /// Creates a new [`ReadRows`] transform with `options`.
    pub fn new(options: ReadOptions) -> Self {
        Self {
            options,
            client: None,
            encoded_rows: false,
        }
    }

    /// Emits `RowElement::Encoded` (`beam:coder:row:v1` bytes) directly without constructing
    /// intermediate [`Row`] structs.
    pub fn with_encoded_rows(mut self) -> Self {
        self.encoded_rows = true;
        self
    }

    /// Uses a pre-configured [`BigtableClient`] (for example, in unit or integration tests).
    pub fn with_client(mut self, client: Arc<BigtableClient>) -> Self {
        self.client = Some(client);
        self
    }

    /// Reads values of a `#[derive(BeamRow)]` type instead of [`Row`].
    pub fn typed<T: BeamRow + DefaultCoder>(self) -> ReadTyped<T> {
        let rows = if T::matches_schema(&self.options.output_schema()) {
            self.with_encoded_rows()
        } else {
            self
        };
        ReadTyped {
            rows,
            _type: PhantomData,
        }
    }

    /// Routes row conversion and stream errors to [`WithFailures::failures`] with
    /// [`read_error_schema`].
    pub fn with_failures(self) -> ReadRowsWithFailures {
        ReadRowsWithFailures { read: self }
    }

    fn expand_ranges(&self, input: &PBegin) -> PCollection<KeyRangeRestriction> {
        input
            .pipeline()
            .apply("Impulse", Impulse::new())
            .apply(
                "SampleKeyRanges",
                ParDo::new(SampleKeyRangesFn {
                    target: self.options.target.clone(),
                    app_profile_id: self.options.app_profile_id.clone(),
                    start_key: self.options.start_key.clone(),
                    end_key: self.options.end_key.clone(),
                    max_initial_bundles: self.options.max_initial_bundles,
                    sub_split_tablets: self.options.sub_split_tablets,
                    attempt_timeout: self.options.attempt_timeout,
                    endpoint: self.options.endpoint.clone(),
                    client: self.client.clone(),
                }),
            )
            .apply("Reshuffle", Reshuffle::new())
    }
}

impl HasDisplayData for ReadRows {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.options.populate_display_data(builder);
    }
}

impl ReadRows {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        input.pipeline().record_display_data(self);
        self.options.validate()?;
        let schema = self.options.output_schema();
        let ranges = self.expand_ranges(input);
        let rows = ranges
            .apply(
                "ReadRanges",
                SplittableParDo::new(ReadRangesFn::<RowElement>::new(
                    self.options.clone(),
                    self.encoded_rows,
                    self.client.clone(),
                )),
            )
            .as_rows()
            .with_row_schema(&schema);
        Ok(rows)
    }
}

impl TryPTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;
    type Error = beam::Error;

    fn try_expand(self, input: &PBegin) -> beam::Result<PCollection<Row>> {
        self.try_expand_ref(input)
    }
}

/// An invalid [`ReadOptions`] is a construction error: [`Pipeline::validate`] returns it, so
/// the pipeline does not run.
///
/// [`Pipeline::validate`]: beam::pipeline::Pipeline::validate
impl PTransform<PBegin> for ReadRows {
    type Output = PCollection<Row>;

    fn expand(self, input: &PBegin) -> PCollection<Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            let coder_id = pipeline.register_row_coder(&self.options.output_schema());
            pipeline.invalid_construction_output(e, &coder_id)
        })
    }
}

/// A [`ReadRows`] that sends row conversion and stream errors to [`WithFailures::failures`].
/// See [`ReadRows::with_failures`].
#[derive(Clone)]
pub struct ReadRowsWithFailures {
    read: ReadRows,
}

impl HasDisplayData for ReadRowsWithFailures {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.read.options.populate_display_data(builder);
    }
}

impl ReadRowsWithFailures {
    fn try_expand_ref(&self, input: &PBegin) -> beam::Result<WithFailures<Row, Row>> {
        input.pipeline().record_display_data(self);
        let read = &self.read;
        read.options.validate()?;
        let schema = read.options.output_schema();
        let ranges = read.expand_ranges(input);
        let WithFailures { output, failures } = ranges.apply(
            "ReadRanges",
            SplittableParDo::new(ReadRangesFn::<Result<RowElement, Row>>::new(
                read.options.clone(),
                read.encoded_rows,
                read.client.clone(),
            )),
        );
        Ok(WithFailures {
            output: output.as_rows().with_row_schema(&schema),
            failures: failures.with_row_schema(read_error_schema()),
        })
    }
}

impl TryPTransform<PBegin> for ReadRowsWithFailures {
    type Output = WithFailures<Row, Row>;
    type Error = beam::Error;

    fn try_expand(self, input: &PBegin) -> beam::Result<WithFailures<Row, Row>> {
        self.try_expand_ref(input)
    }
}

impl PTransform<PBegin> for ReadRowsWithFailures {
    type Output = WithFailures<Row, Row>;

    fn expand(self, input: &PBegin) -> WithFailures<Row, Row> {
        self.try_expand_ref(input).unwrap_or_else(|e| {
            let pipeline = input.pipeline();
            let step = pipeline.scope_name().unwrap_or_default();
            let output_coder_id = pipeline.register_row_coder(&self.read.options.output_schema());
            let output = pipeline.invalid_construction_output(e, &output_coder_id);
            let coder_id = pipeline.register_row_coder(read_error_schema());
            let failures = pipeline.add_pcollection(
                &format!("{step}.failures"),
                &coder_id,
                beam::values::IsBounded::Bounded,
            );
            WithFailures { output, failures }
        })
    }
}

/// Typed wrapper around [`ReadRows`] that converts each [`Row`] into `T: BeamRow`.
pub struct ReadTyped<T> {
    rows: ReadRows,
    _type: PhantomData<fn() -> T>,
}

impl<T> Clone for ReadTyped<T> {
    fn clone(&self) -> Self {
        Self {
            rows: self.rows.clone(),
            _type: PhantomData,
        }
    }
}

impl<T> HasDisplayData for ReadTyped<T> {
    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        self.rows.options.populate_display_data(builder);
    }
}

impl<T: BeamRow + DefaultCoder> PTransform<PBegin> for ReadTyped<T> {
    type Output = PCollection<T>;

    fn expand(self, input: &PBegin) -> PCollection<T> {
        input.pipeline().record_display_data(&self);
        let rows = input.apply("ReadRows", self.rows);
        rows.apply("FromRows", FromRows::new())
    }
}
