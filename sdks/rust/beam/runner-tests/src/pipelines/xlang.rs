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

//! Cross-language pipelines: a transform expanded by an external expansion service,
//! executed by the runner in that service's environment.

use std::sync::Arc;

use beam::prelude::*;
use beam::schema::FieldType;
use beam::testing::{TestPipeline, passert};
use external::{
    ExternalOutputs, ExternalSource, ExternalTransform, encode_external_configuration_payload,
};

use crate::xlang_transforms::{
    URN_CGBK, URN_COMGL, URN_COMPK, URN_FLATTEN, URN_GBK, URN_MULTI, URN_PARTITION, URN_PREFIX,
};

/// Identifier of the `GenerateSequence` SchemaTransform.
pub const GENERATE_SEQUENCE_SCHEMA_TRANSFORM: &str =
    "beam:schematransform:org.apache.beam:generate_sequence:v1";

/// A `beam:transforms:xlang:test:*` transform of the expansion service at `endpoint`, with the
/// PCollections of `inputs` under their tags.
fn test_transform<'a>(
    urn: &str,
    endpoint: &str,
    payload: Vec<u8>,
    inputs: impl IntoIterator<Item = (&'a str, &'a str)>,
) -> ExternalTransform {
    inputs.into_iter().fold(
        ExternalTransform::new(urn, endpoint, payload),
        |transform, (tag, id)| transform.with_input(tag, id),
    )
}

/// Expands `transform` and returns its main output as `T`.
///
/// `ExternalSource` takes its inputs from `ExternalTransform::with_input`, so it also applies
/// a transform that has inputs. The expansion service picks the coder: `T` must match it.
fn expand_single<T: 'static>(
    p: &Pipeline,
    name: &str,
    transform: ExternalTransform,
) -> PCollection<T> {
    typed(&p.apply(name, ExternalSource::new(transform)))
}

/// Expands `transform` with the declared output `tags` and returns every output.
fn expand_multi(
    p: &Pipeline,
    name: &str,
    transform: ExternalTransform,
    tags: [&str; 2],
) -> ExternalOutputs {
    p.apply(
        name,
        ExternalSource::new(transform.with_output_tags(tags)).all_outputs(),
    )
}

/// The output `tag` of `outputs` as `T`.
fn output<T: 'static>(outputs: &ExternalOutputs, tag: &str) -> PCollection<T> {
    typed(&outputs.try_get(tag).expect("declared output tag"))
}

/// The same PCollection with the element type `T`. The coder stays the same.
fn typed<T: 'static>(rows: &PCollection<Row>) -> PCollection<T> {
    PCollection::new(
        rows.id().to_string(),
        rows.coder_id().to_string(),
        rows.pipeline().clone(),
    )
}

/// Formats a group as `key:v1,v2,...` with the values in sorted order.
fn format_groups(groups: &PCollection<(i64, BeamIterable<String>)>) -> PCollection<String> {
    groups.par_do_fn(
        "FormatGroups",
        |(key, values): (i64, BeamIterable<String>), ctx| {
            let mut values = values.into_vec().map_err(|e| e.to_string())?;
            values.sort();
            ctx.emit(format!("{key}:{}", values.join(",")))
        },
    )
}

fn strings(values: &[&str]) -> Vec<String> {
    values.iter().map(ToString::to_string).collect()
}

/// `beam:transforms:xlang:test:prefix` puts the configured `data` before each string.
pub fn build_xlang_prefix(p: &TestPipeline, endpoint: &str) {
    let schema = Arc::new(Schema::builder().field("data", FieldType::string()).build());
    let config = Row::builder(schema)
        .with_value("0".to_string())
        .build()
        .expect("prefix config row");
    let payload = encode_external_configuration_payload(&config).expect("prefix payload");
    let input = p.apply("Create", Create::new(strings(&["1", "2", "3"])));
    let transform = test_transform(URN_PREFIX, endpoint, payload, [("input", input.id())]);
    let prefixed = expand_single::<String>(p, "Prefix", transform);
    passert::that("AssertPrefixed", &prefixed).contains_in_any_order(strings(&["01", "02", "03"]));
}

/// `beam:transforms:xlang:test:multi` reads two main inputs and a singleton side input, and
/// writes two outputs.
pub fn build_xlang_multi_input_output_with_side_input(p: &TestPipeline, endpoint: &str) {
    let main1 = p.apply("CreateMain1", Create::new(strings(&["a", "bb"])));
    let main2 = p.apply("CreateMain2", Create::new(strings(&["x", "yy", "zzz"])));
    let side = p.apply("CreateSide", Create::new(strings(&["s"])));
    let inputs = [
        ("main1", main1.id()),
        ("main2", main2.id()),
        ("side", side.id()),
    ];
    let transform = test_transform(URN_MULTI, endpoint, Vec::new(), inputs);
    let outputs = expand_multi(p, "Multi", transform, ["main", "side"]);
    passert::that("AssertMain", &output::<String>(&outputs, "main"))
        .contains_in_any_order(strings(&["as", "bbs", "xs", "yys", "zzzs"]));
    passert::that("AssertSide", &output::<String>(&outputs, "side"))
        .contains_in_any_order(strings(&["ss"]));
}

/// `beam:transforms:xlang:test:gbk` groups key-value pairs by key.
pub fn build_xlang_group_by_key(p: &TestPipeline, endpoint: &str) {
    let input = p.apply(
        "Create",
        Create::new(vec![
            (0i64, "1".to_string()),
            (0i64, "2".to_string()),
            (1i64, "3".to_string()),
        ]),
    );
    let transform = test_transform(URN_GBK, endpoint, Vec::new(), [("input", input.id())]);
    let grouped = expand_single(p, "GroupByKey", transform);
    passert::that("AssertGroups", &format_groups(&grouped))
        .contains_in_any_order(strings(&["0:1,2", "1:3"]));
}

/// `beam:transforms:xlang:test:cgbk` groups the values of `col1` and `col2` by key into one list.
pub fn build_xlang_co_group_by_key(p: &TestPipeline, endpoint: &str) {
    let col1 = p.apply(
        "CreateCol1",
        Create::new(vec![
            (0i64, "1".to_string()),
            (0i64, "2".to_string()),
            (1i64, "3".to_string()),
        ]),
    );
    let col2 = p.apply(
        "CreateCol2",
        Create::new(vec![
            (0i64, "4".to_string()),
            (1i64, "5".to_string()),
            (1i64, "6".to_string()),
        ]),
    );
    let inputs = [("col1", col1.id()), ("col2", col2.id())];
    let transform = test_transform(URN_CGBK, endpoint, Vec::new(), inputs);
    let grouped = expand_single(p, "CoGroupByKey", transform);
    passert::that("AssertGroups", &format_groups(&grouped))
        .contains_in_any_order(strings(&["0:1,2,4", "1:3,5,6"]));
}

/// `beam:transforms:xlang:test:comgl` sums the integers.
pub fn build_xlang_combine_globally(p: &TestPipeline, endpoint: &str) {
    let input = p.apply("Create", Create::new(vec![1i64, 2, 3]));
    let transform = test_transform(URN_COMGL, endpoint, Vec::new(), [("input", input.id())]);
    let sum = expand_single::<i64>(p, "CombineGlobally", transform);
    passert::that("AssertSum", &sum).contains_in_any_order([6i64]);
}

/// `beam:transforms:xlang:test:compk` sums the integers of each key.
pub fn build_xlang_combine_per_key(p: &TestPipeline, endpoint: &str) {
    let input = p.apply(
        "Create",
        Create::new(vec![
            ("a".to_string(), 1i64),
            ("a".to_string(), 2),
            ("b".to_string(), 3),
        ]),
    );
    let transform = test_transform(URN_COMPK, endpoint, Vec::new(), [("input", input.id())]);
    let sums = expand_single::<(String, i64)>(p, "CombinePerKey", transform);
    passert::that("AssertSums", &sums)
        .contains_in_any_order([("a".to_string(), 3i64), ("b".to_string(), 3)]);
}

/// `beam:transforms:xlang:test:flatten` merges all inputs into one PCollection.
pub fn build_xlang_flatten(p: &TestPipeline, endpoint: &str) {
    let col1 = p.apply("CreateCol1", Create::new(vec![1i64, 2, 3]));
    let col2 = p.apply("CreateCol2", Create::new(vec![4i64, 5, 6]));
    let inputs = [("col1", col1.id()), ("col2", col2.id())];
    let transform = test_transform(URN_FLATTEN, endpoint, Vec::new(), inputs);
    let flat = expand_single::<i64>(p, "Flatten", transform);
    passert::that("AssertFlat", &flat).contains_in_any_order(1i64..=6);
}

/// `beam:transforms:xlang:test:partition` puts even integers in output `0` and odd integers in
/// output `1`.
pub fn build_xlang_partition(p: &TestPipeline, endpoint: &str) {
    let input = p.apply("Create", Create::new((1i64..=6).collect::<Vec<_>>()));
    let transform = test_transform(URN_PARTITION, endpoint, Vec::new(), [("input", input.id())]);
    let parts = expand_multi(p, "Partition", transform, ["0", "1"]);
    passert::that("AssertEven", &output::<i64>(&parts, "0")).contains_in_any_order([2i64, 4, 6]);
    passert::that("AssertOdd", &output::<i64>(&parts, "1")).contains_in_any_order([1i64, 3, 5]);
}

/// Builds a cross-language source: the `GenerateSequence` SchemaTransform,
/// expanded by the expansion service at `expansion_endpoint`, feeding Rust transforms.
///
/// The runner has to stage the foreign environment's artifacts, run its SDK harness
/// alongside the Rust one, and exchange schema'd rows across the language boundary.
/// Each value must arrive exactly once.
pub fn build_xlang_generate_sequence(p: &TestPipeline, expansion_endpoint: &str) {
    let rate_schema = Schema::builder()
        .field("elements", FieldType::int64())
        .nullable_field("seconds", FieldType::int64())
        .build();
    let schema = Arc::new(
        Schema::builder()
            .field("start", FieldType::int64())
            .nullable_field("end", FieldType::int64())
            .nullable_field("rate", FieldType::row(rate_schema))
            .build(),
    );
    let config = Row::builder(schema)
        .with_value(0i64)
        .with_value(10i64)
        .with_null()
        .build()
        .expect("GenerateSequence config row");
    let transform = ExternalTransform::schema_transform(
        GENERATE_SEQUENCE_SCHEMA_TRANSFORM,
        expansion_endpoint,
        &config,
    )
    .expect("GenerateSequence payload")
    .with_output_tags(["output"]);

    let rows = p.apply(
        "ExternalSource",
        ExternalSource::new(transform).with_output_tag("output"),
    );
    let values = rows.par_do_fn("RowValue", |row: Row, ctx| {
        let value = row
            .get_i64("value")
            .map_err(|e| e.to_string())?
            .ok_or("GenerateSequence row without a value")?;
        ctx.emit(value)
    });
    passert::that("AssertValues", &values).contains_in_any_order(0..10);
}
