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

//! Test transforms of the cross-language ValidatesRunner suites (`beam:transforms:xlang:test:*`).
//!
//! The suites assert the same results for every test expansion service, so each transform
//! must give the results that the suites expect.

use std::collections::HashMap;

use beam::pipeline::Pipeline;
use beam::schema::{FieldType, Row, Schema};
use beam::transforms::{
    CoGbkResult, CoGroupByKey, CombineGlobally, CombinePerKey, Flatten, GroupByKey,
    KeyedPCollectionTuple, Map, ParDo, Partition, Sum,
};
use beam::values::PCollectionList;
use expansion::{
    PCollectionId, SchemaTransformProvider, SchemaTransformRegistration, TransformInputs,
};

/// URN of the cross-language string prefixing test transform.
pub const URN_PREFIX: &str = "beam:transforms:xlang:test:prefix";
/// URN of the cross-language multi-output test transform.
pub const URN_MULTI: &str = "beam:transforms:xlang:test:multi";
/// URN of the cross-language GroupByKey test transform.
pub const URN_GBK: &str = "beam:transforms:xlang:test:gbk";
/// URN of the cross-language CoGroupByKey test transform.
pub const URN_CGBK: &str = "beam:transforms:xlang:test:cgbk";
/// URN of the cross-language CombineGlobally test transform.
pub const URN_COMGL: &str = "beam:transforms:xlang:test:comgl";
/// URN of the cross-language CombinePerKey test transform.
pub const URN_COMPK: &str = "beam:transforms:xlang:test:compk";
/// URN of the cross-language Flatten test transform.
pub const URN_FLATTEN: &str = "beam:transforms:xlang:test:flatten";
/// URN of the cross-language Partition test transform.
pub const URN_PARTITION: &str = "beam:transforms:xlang:test:partition";

type Inputs = TransformInputs;
type Outputs = beam::Result<HashMap<String, PCollectionId>>;

/// A test transform: its URN, its tags and the function that builds it.
struct TestTransform {
    urn: &'static str,
    description: &'static str,
    inputs: &'static [&'static str],
    outputs: &'static [&'static str],
    config: fn() -> Schema,
    build: fn(Row, &Inputs, &Pipeline) -> Outputs,
}

impl SchemaTransformProvider for TestTransform {
    fn identifier(&self) -> &'static str {
        self.urn
    }

    fn description(&self) -> &'static str {
        self.description
    }

    fn config_schema(&self) -> Schema {
        (self.config)()
    }

    fn input_tags(&self) -> Vec<String> {
        self.inputs.iter().map(ToString::to_string).collect()
    }

    fn output_tags(&self) -> Vec<String> {
        self.outputs.iter().map(ToString::to_string).collect()
    }

    fn build_transform(&self, config: Row, inputs: Inputs, pipeline: &mut Pipeline) -> Outputs {
        (self.build)(config, &inputs, pipeline)
    }
}

macro_rules! register {
    ($($field:ident: $value:expr),* $(,)?) => {
        expansion::inventory::submit! {
            SchemaTransformRegistration {
                provider: || Box::new(TestTransform { $($field: $value),* }),
            }
        }
    };
}

fn no_config() -> Schema {
    Schema::builder().build()
}

fn outputs<const N: usize>(ids: [(&str, &str); N]) -> HashMap<String, PCollectionId> {
    ids.into_iter()
        .map(|(tag, id)| (tag.to_string(), PCollectionId::from(id)))
        .collect()
}

register! {
    urn: URN_PREFIX,
    description: "Puts the configured `data` before each string.",
    inputs: &["input"],
    outputs: &["output"],
    config: || Schema::builder().field("data", FieldType::string()).build(),
    build: prefix,
}

fn prefix(config: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    let data = config.get_string("data")?.unwrap_or_default().to_string();
    let prefixed = inputs
        .get::<String>(pipeline, "input")?
        .apply("Prefix", Map::new(move |s: String| format!("{data}{s}")));
    Ok(outputs([("output", prefixed.id())]))
}

register! {
    urn: URN_MULTI,
    description: "Appends the `side` singleton to each string of `main1` and `main2`, and \
        doubles `side`.",
    inputs: &["main1", "main2", "side"],
    outputs: &["main", "side"],
    config: no_config,
    build: multi,
}

fn multi(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    let main = PCollectionList::of(inputs.get::<String>(pipeline, "main1")?)
        .and(inputs.get(pipeline, "main2")?)
        .apply("Flatten", Flatten::new());
    let side = inputs.get::<String>(pipeline, "side")?;
    let view = side.as_singleton();
    let main = main.apply(
        "AppendSide",
        ParDo::from_fn_with_side_inputs(&view, |s: String, suffix: String, ctx| {
            ctx.emit(s + &suffix)
        }),
    );
    let side = side.apply("Double", Map::new(|s: String| s.repeat(2)));
    Ok(outputs([("main", main.id()), ("side", side.id())]))
}

register! {
    urn: URN_GBK,
    description: "Groups key-value pairs by key.",
    inputs: &["input"],
    outputs: &["output"],
    config: no_config,
    build: group_by_key,
}

fn group_by_key(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    // The runner groups the elements, and the output coder comes from the input KV coder.
    // No Rust code decodes a key or a value, so the unit types are only placeholders.
    let grouped = inputs
        .get::<((), ())>(pipeline, "input")?
        .apply("GroupByKey", GroupByKey::new());
    Ok(outputs([("output", grouped.id())]))
}

register! {
    urn: URN_CGBK,
    description: "Groups the integer-keyed strings of `col1` and `col2` by key into one list.",
    inputs: &["col1", "col2"],
    outputs: &["output"],
    config: no_config,
    build: co_group_by_key,
}

fn co_group_by_key(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    let grouped =
        KeyedPCollectionTuple::of("col1", &inputs.get::<(i64, String)>(pipeline, "col1")?)
            .and("col2", &inputs.get::<(i64, String)>(pipeline, "col2")?)
            .apply("CoGroupByKey", CoGroupByKey::new());
    let merged = grouped.apply(
        "Merge",
        ParDo::from_fn(|(key, result): (i64, CoGbkResult), ctx| {
            let mut values = result.get_vec::<String>("col1")?;
            values.extend(result.get_vec::<String>("col2")?);
            ctx.emit((key, values))
        }),
    );
    Ok(outputs([("output", merged.id())]))
}

register! {
    urn: URN_COMGL,
    description: "Sums integers.",
    inputs: &["input"],
    outputs: &["output"],
    config: no_config,
    build: combine_globally,
}

fn combine_globally(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    let sum = inputs
        .get::<i64>(pipeline, "input")?
        .apply("Sum", CombineGlobally::new(Sum::new()));
    Ok(outputs([("output", sum.id())]))
}

register! {
    urn: URN_COMPK,
    description: "Sums integers for each string key.",
    inputs: &["input"],
    outputs: &["output"],
    config: no_config,
    build: combine_per_key,
}

fn combine_per_key(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    let sums = inputs
        .get::<(String, i64)>(pipeline, "input")?
        .apply("Sum", CombinePerKey::new(Sum::new()));
    Ok(outputs([("output", sums.id())]))
}

register! {
    urn: URN_FLATTEN,
    description: "Flattens all integer inputs.",
    inputs: &[],
    outputs: &["output"],
    config: no_config,
    build: flatten,
}

fn flatten(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    // The worker replays the same expansion, so the order of the inputs must not change.
    let list = inputs
        .tags()
        .try_fold(PCollectionList::empty(pipeline.clone()), |list, tag| {
            Ok::<_, beam::Error>(list.and(inputs.get::<i64>(pipeline, tag)?))
        })?;
    let flat = list.apply("Flatten", Flatten::new());
    Ok(outputs([("output", flat.id())]))
}

register! {
    urn: URN_PARTITION,
    description: "Puts even integers in output `0` and odd integers in output `1`.",
    inputs: &["input"],
    outputs: &["0", "1"],
    config: no_config,
    build: partition,
}

fn partition(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    let parts = inputs.get::<i64>(pipeline, "input")?.apply(
        "Partition",
        Partition::new(2, |n: &i64| n.rem_euclid(2) as usize),
    );
    Ok(outputs([("0", parts[0].id()), ("1", parts[1].id())]))
}
