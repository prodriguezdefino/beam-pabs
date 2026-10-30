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

//! Integration tests: native BigQuery Storage reads of a public table and of a query.
//!
//! The tests call BigQuery, so they carry `#[ignore]` and run only with
//! `./gradlew :sdks:rust:bigQueryIntegrationTest`. The task sets
//! `BEAM_TEST_PIPELINE_OPTIONS`, for example `--runner=prism --project=<project>`. The reads
//! bill `--project` with Application Default Credentials. `bigquery_storage_test` covers the
//! same paths with fake clients.
#![expect(clippy::unwrap_used, reason = "test fixtures panic on setup failure")]

use beam::io::gcp::bigquery::storage::read::{ArrowCompression, ReadOptions, ReadRows};
use beam::io::gcp::options::GcpOptions;
use beam::schema::{FieldType, Row, Schema};
use beam::testing::{TestPipeline, passert, test_pipeline_options};
use fluent::prelude::PCollectionExt;

/// Rows and the sum of `word_count` for the `hamlet` corpus.
const HAMLET_ROWS: usize = 5318;
const HAMLET_WORDS: i64 = 32446;

fn hamlet() -> ReadOptions {
    ReadOptions::new("bigquery-public-data.samples.shakespeare")
        .with_selected_fields(["word", "word_count"])
        .with_row_restriction("corpus = 'hamlet'")
}

fn hamlet_query() -> ReadOptions {
    ReadOptions::from_query(
        "SELECT word, word_count FROM `bigquery-public-data.samples.shakespeare` \
         WHERE corpus = 'hamlet'",
    )
}

#[test]
#[ignore = "calls BigQuery"]
fn the_schema_has_the_selected_columns() {
    let project = test_pipeline_options()
        .view_as::<GcpOptions>()
        .unwrap()
        .project
        .expect("set --project in BEAM_TEST_PIPELINE_OPTIONS");
    let table = Schema::builder()
        .field("word", FieldType::string())
        .field("word_count", FieldType::int64())
        .build();
    // The columns of a query result are nullable, also when the source columns are not.
    let query = Schema::builder()
        .nullable_field("word", FieldType::string())
        .nullable_field("word_count", FieldType::int64())
        .build();
    for (options, expected) in [(hamlet(), table), (hamlet_query(), query)] {
        let schema = options.with_billing_project(&project).schema().unwrap();
        assert_eq!(schema, expected);
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "calls BigQuery"]
async fn every_row_of_every_stream_is_read_once() {
    assert_reads_hamlet(hamlet().with_max_stream_count(3)).await;
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "calls BigQuery"]
async fn compressed_reads_return_the_same_rows() {
    for compression in [ArrowCompression::Lz4Frame, ArrowCompression::Zstd] {
        assert_reads_hamlet(
            hamlet()
                .with_max_stream_count(3)
                .with_compression(compression),
        )
        .await;
    }
}

#[tokio::test(flavor = "multi_thread")]
#[ignore = "calls BigQuery"]
async fn query_reads_return_the_same_rows() {
    assert_reads_hamlet(hamlet_query().with_max_stream_count(3)).await;
}

/// Reads `options` with the schema lookup of the transform, and the billing project from
/// `--project`.
async fn assert_reads_hamlet(options: ReadOptions) {
    let p = TestPipeline::new();
    let counts = p
        .apply("ReadHamlet", ReadRows::new(options))
        .map("WordCount", |row: Row| {
            row.get_i64("word_count").unwrap().unwrap()
        });
    passert::that("Hamlet", &counts).satisfies(|counts: &[i64]| {
        let words: i64 = counts.iter().sum();
        if counts.len() == HAMLET_ROWS && words == HAMLET_WORDS {
            Ok(())
        } else {
            Err(beam::Error::msg(format!(
                "read {} rows and {words} words, expected {HAMLET_ROWS} and {HAMLET_WORDS}",
                counts.len()
            )))
        }
    });
    p.run().await.unwrap();
}
