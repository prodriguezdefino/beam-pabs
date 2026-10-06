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
use expansion::{PCollectionId, SchemaTransformProvider, SchemaTransformRegistration, input};

pub const URN_PREFIX: &str = "beam:transforms:xlang:test:prefix";
pub const URN_MULTI: &str = "beam:transforms:xlang:test:multi";
pub const URN_GBK: &str = "beam:transforms:xlang:test:gbk";
pub const URN_CGBK: &str = "beam:transforms:xlang:test:cgbk";
pub const URN_COMGL: &str = "beam:transforms:xlang:test:comgl";
pub const URN_COMPK: &str = "beam:transforms:xlang:test:compk";
pub const URN_FLATTEN: &str = "beam:transforms:xlang:test:flatten";
pub const URN_PARTITION: &str = "beam:transforms:xlang:test:partition";

type Inputs = HashMap<String, PCollectionId>;
type Outputs = Result<HashMap<String, PCollectionId>, String>;

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
        .map(|(tag, id)| (tag.to_string(), id.to_string()))
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
    let data = config
        .get_string("data")
        .map_err(|e| e.to_string())?
        .unwrap_or_default()
        .to_string();
    let prefixed = input::<String>(pipeline, inputs, "input")?
        .apply(Map::new("Prefix", move |s: String| format!("{data}{s}")));
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
    let main = PCollectionList::of(input::<String>(pipeline, inputs, "main1")?)
        .and(input(pipeline, inputs, "main2")?)
        .apply(Flatten::new("Flatten"));
    let side = input::<String>(pipeline, inputs, "side")?;
    let view = side.as_singleton();
    let side_value = view.clone();
    let main = main.apply(
        ParDo::from_fn("AppendSide", move |s: String, ctx| {
            let suffix = ctx.side_input(&side_value)?;
            ctx.emit(s + &suffix)
        })
        .with_side_input(&view),
    );
    let side = side.apply(Map::new("Double", |s: String| s.repeat(2)));
    Ok(outputs([("main", main.id()), ("side", side.id())]))
}

register! {
    urn: URN_GBK,
    description: "Groups integer-keyed strings by key.",
    inputs: &["input"],
    outputs: &["output"],
    config: no_config,
    build: group_by_key,
}

fn group_by_key(_: Row, inputs: &Inputs, pipeline: &Pipeline) -> Outputs {
    let grouped =
        input::<(i64, String)>(pipeline, inputs, "input")?.apply(GroupByKey::new("GroupByKey"));
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
        KeyedPCollectionTuple::of("col1", &input::<(i64, String)>(pipeline, inputs, "col1")?)
            .and("col2", &input::<(i64, String)>(pipeline, inputs, "col2")?)
            .apply(CoGroupByKey::new("CoGroupByKey"));
    let merged = grouped.apply(ParDo::from_fn(
        "Merge",
        |(key, result): (i64, CoGbkResult), ctx| {
            let mut values = result.get_vec::<String>("col1")?;
            values.extend(result.get_vec::<String>("col2")?);
            ctx.emit((key, values))
        },
    ));
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
    let sum = input::<i64>(pipeline, inputs, "input")?.apply(CombineGlobally::new("Sum", Sum));
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
    let sums =
        input::<(String, i64)>(pipeline, inputs, "input")?.apply(CombinePerKey::new("Sum", Sum));
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
    let mut tags: Vec<&String> = inputs.keys().collect();
    tags.sort();
    let list = tags
        .into_iter()
        .try_fold(PCollectionList::empty(pipeline.clone()), |list, tag| {
            Ok::<_, String>(list.and(input::<i64>(pipeline, inputs, tag)?))
        })?;
    let flat = list.apply(Flatten::new("Flatten"));
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
    let parts = input::<i64>(pipeline, inputs, "input")?.apply(Partition::new(
        "Partition",
        2,
        |n: &i64| n.rem_euclid(2) as usize,
    ));
    Ok(outputs([("0", parts[0].id()), ("1", parts[1].id())]))
}
