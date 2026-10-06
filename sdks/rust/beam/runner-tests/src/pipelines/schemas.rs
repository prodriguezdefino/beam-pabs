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

//! Schema-aware `Row` elements and the row coder.

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::FieldType;
use beam::testing::{TestPipeline, passert};

use crate::dofns::*;

/// URN of the portable schema'd row coder.
const ROW_CODER_URN: &str = "beam:coder:row:v1";

fn event(account: &str, amount: i64, tags: &[&str], verified: bool) -> AuditEvent {
    AuditEvent {
        account_id: account.to_string(),
        amount,
        tags: tags.iter().map(|t| t.to_string()).collect(),
        is_verified: verified,
    }
}

/// Validates `#[derive(BeamRow)]` elements travelling through the runner as
/// `beam:coder:row:v1`.
///
/// The events cross two runner-materialised boundaries: a `GroupByKey`, where they
/// are the values of a KV and then the elements of a grouped iterable, and a
/// `Reshuffle`, where they are bare elements. Each boundary encodes and decodes them
/// with the row coder, and the runner has to frame them, so a field written in the
/// wrong order or a mis-framed row fails the assertion (or the decode). The
/// collections' declared coder is checked first, so the test cannot silently fall
/// back to an opaque bytes coder.
pub fn build_row_schema_coder(p: &TestPipeline) {
    let events = p.apply(
        "Create",
        Create::new(vec![
            event("acct-1", 500, &["fintech", "vip"], true),
            event("acct-2", 1500, &["ecommerce"], false),
            event("acct-1", -20, &[], false),
        ]),
    );

    let grouped = events
        .key_by("KeyByAccount", |e: &AuditEvent| e.account_id.clone())
        .group_by_key("GroupByAccount");
    let adjusted = grouped
        .flat_map(
            "AddFee",
            |(_, group): (String, BeamIterable<AuditEvent>)| {
                group
                    .into_vec()
                    .expect("group values decode")
                    .into_iter()
                    .map(|e| AuditEvent {
                        amount: e.amount + 100,
                        ..e
                    })
                    .collect::<Vec<_>>()
            },
        )
        .reshuffle("Reshuffle");

    {
        let proto = p.to_proto();
        let components = proto.components.as_ref().expect("pipeline components");
        for pcoll in [&events, &adjusted] {
            let coder_id = &components.pcollections[pcoll.id()].coder_id;
            let urn = components.coders[coder_id]
                .spec
                .as_ref()
                .map(|s| s.urn.as_str());
            assert_eq!(
                urn,
                Some(ROW_CODER_URN),
                "PCollection {} must use the row coder",
                pcoll.id()
            );
        }
    }

    passert::that("AssertAdjusted", &adjusted).contains_in_any_order([
        event("acct-1", 600, &["fintech", "vip"], true),
        event("acct-2", 1600, &["ecommerce"], false),
        event("acct-1", 80, &[], false),
    ]);
}

/// A schema with the fields `string{n}_field` and `integer{n}_field`.
fn pair_schema(n: &str) -> Arc<Schema> {
    Arc::new(
        Schema::builder()
            .field(format!("string{n}_field"), FieldType::string())
            .field(format!("integer{n}_field"), FieldType::int32())
            .build(),
    )
}

/// A row of [`pair_schema`].
fn pair_row(schema: &Arc<Schema>, s: &str, i: i32) -> Result<Row> {
    Ok(Row::builder(Arc::clone(schema))
        .with_value(s.to_string())
        .with_value(i)
        .build()?)
}

/// Reads the fields of a row of [`pair_schema`]`(n)`.
fn read_pair(row: &Row, n: &str) -> Result<(String, i32)> {
    let s = row
        .get_string(&format!("string{n}_field"))?
        .ok_or("the string field is null")?;
    let i = row
        .get_i32(&format!("integer{n}_field"))?
        .ok_or("the integer field is null")?;
    Ok((s.to_string(), i))
}

/// The rows `a:1`, `b:2` and `c:3` of [`pair_schema`]`("")`.
fn pair_rows(p: &TestPipeline) -> PCollection<Row> {
    let schema = pair_schema("");
    let rows = [("a", 1), ("b", 2), ("c", 3)]
        .into_iter()
        .map(|(s, i)| pair_row(&schema, s, i))
        .collect::<Result<Vec<_>>>()
        .expect("the rows follow the schema");
    p.apply("Create", Create::new(rows))
}

/// Formats each row of [`pair_schema`]`(n)` as `string:integer`, after a shuffle, so the
/// rows cross the runner with their row coder.
fn format_pairs(rows: &PCollection<Row>, n: &str, name: &str) -> PCollection<String> {
    let n = n.to_string();
    rows.reshuffle(format!("{name}/Reshuffle")).par_do_fn(
        format!("{name}/Format"),
        move |row: Row, ctx| {
            let (s, i) = read_pair(&row, &n)?;
            ctx.emit(format!("{s}:{i}"))
        },
    )
}

/// Validates a `DoFn` that reads a `Row` and writes a `Row` of another schema. The output
/// keeps the schema that `with_row_schema` gives it.
///
/// Covers `ParDoSchemaTest.testReadAndWrite`.
pub fn build_schema_dofn_reads_and_writes_rows(p: &TestPipeline) {
    let output_schema = pair_schema("2");
    let row_schema = Arc::clone(&output_schema);
    let written = pair_rows(p)
        .par_do_fn("Rewrite", move |row: Row, ctx| {
            let (s, i) = read_pair(&row, "")?;
            ctx.emit(pair_row(&row_schema, &s, i)?)
        })
        .with_row_schema(&output_schema);
    assert_eq!(
        written.row_schema().as_deref(),
        Some(output_schema.as_ref())
    );
    passert::that("AssertRows", &format_pairs(&written, "2", "Read"))
        .contains_in_any_order(["a:1", "b:2", "c:3"].map(String::from));
}

/// The outputs of [`SplitRowsDoFn`]: one `Row` collection per schema.
#[derive(Outputs)]
enum RowOutputs {
    First(Row),
    Second(Row),
}

/// Writes each input row to two outputs, with the schemas `first` and `second`.
#[derive(Clone)]
struct SplitRowsDoFn {
    first: Arc<Schema>,
    second: Arc<Schema>,
}

impl DoFn for SplitRowsDoFn {
    type In = Row;
    type Out = RowOutputs;

    fn process_element(&mut self, row: Row, ctx: &mut ProcessContext<'_, Self::Out>) -> Result {
        let (s, i) = read_pair(&row, "")?;
        ctx.emit(RowOutputs::First(pair_row(&self.first, &s, i)?))?;
        ctx.emit(RowOutputs::Second(pair_row(&self.second, &s, i)?))
    }
}

/// Validates a multi-output `DoFn` that writes `Row`s of a different schema to each output.
///
/// Covers `ParDoSchemaTest.testReadAndWriteMultiOutput`.
pub fn build_schema_dofn_writes_rows_to_tagged_outputs(p: &TestPipeline) {
    let (first_schema, second_schema) = (pair_schema("2"), pair_schema("3"));
    let RowOutputsPCollections { first, second } = pair_rows(p).par_do(
        "Split",
        SplitRowsDoFn {
            first: Arc::clone(&first_schema),
            second: Arc::clone(&second_schema),
        },
    );
    let first = first.with_row_schema(&first_schema);
    let second = second.with_row_schema(&second_schema);
    passert::that("AssertFirst", &format_pairs(&first, "2", "ReadFirst"))
        .contains_in_any_order(["a:1", "b:2", "c:3"].map(String::from));
    passert::that("AssertSecond", &format_pairs(&second, "3", "ReadSecond"))
        .contains_in_any_order(["a:1", "b:2", "c:3"].map(String::from));
}

/// Validates that a `Filter` keeps the schema of its `Row` input, so the next stage decodes
/// the rows.
///
/// Covers `ParDoSchemaTest.testSchemasPassedThrough`.
pub fn build_schema_kept_through_filter(p: &TestPipeline) {
    let rows = pair_rows(p);
    let filtered = rows.filter("KeepAll", |_: &Row| true);
    assert_eq!(
        filtered.row_schema().as_deref(),
        Some(pair_schema("").as_ref())
    );
    passert::that("AssertRows", &format_pairs(&filtered, "", "Read"))
        .contains_in_any_order(["a:1", "b:2", "c:3"].map(String::from));
}
