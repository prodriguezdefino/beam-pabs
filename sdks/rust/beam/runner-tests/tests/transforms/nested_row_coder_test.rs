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

//! Schema'd (`#[derive(BeamRow)]`) values nested inside composites on PrismRunner.
//!
//! Prism cannot interpret `beam:coder:row:v1`, so it length-prefixes every Row coder at
//! the bundle's data ports, including Rows nested in a `KV` or a grouped iterable. These
//! pipelines fail unless the harness honours those nested prefixes in both directions.

use std::sync::{Arc, Mutex};

use beam::coders::BeamIterable;
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, FieldType, Row, Schema};
use beam::transforms::{
    BagState, Create, GroupByKey, Map, NoTimers, ParDo, Reshuffle, State, StateRowSchemas,
    StatefulContext, StatefulDoFn, ValueState, WithKeys,
};
use beam_fluent::prelude::PCollectionExt;
use beam_runner_prism::PrismRunner;

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Pair<T> {
    key: String,
    value: T,
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Reading {
    id: i64,
    sensor: String,
    value: Option<f64>,
}

fn readings(n: i64) -> Vec<Reading> {
    (0..n)
        .map(|id| Reading {
            id,
            sensor: format!("sensor-{}", id % 4),
            value: (id % 5 != 0).then_some(id as f64 / 2.0),
        })
        .collect()
}

#[tokio::test]
async fn row_keyed_values_cross_a_fused_stage_boundary() {
    // KV<i32, Row> written to a data sink and read back by the next stage, without a
    // grouping in between that would hide a mis-framed element.
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);
    let p = Pipeline::new();
    p.apply("Create", Create::new(readings(20)))
        .apply("Key", Map::new(|r: Reading| (r.id as i32, r)))
        .apply("Reshuffle", Reshuffle::new())
        .inspect("Capture", move |kv: &(i32, Reading)| {
            sink.lock().unwrap().push(kv.clone());
        });
    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "pipeline failed: {res:?}");

    let mut actual = captured.lock().unwrap().clone();
    actual.sort_by_key(|kv| kv.0);
    let expected: Vec<(i32, Reading)> =
        readings(20).into_iter().map(|r| (r.id as i32, r)).collect();
    assert_eq!(actual, expected);
}

#[tokio::test]
async fn generic_beam_row_groups_by_key_on_prism() {
    let inputs = vec![
        Pair {
            key: "a".to_string(),
            value: 10_i64,
        },
        Pair {
            key: "b".to_string(),
            value: 5_i64,
        },
        Pair {
            key: "a".to_string(),
            value: 25_i64,
        },
    ];
    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);

    let p = Pipeline::new();
    p.apply("Create", Create::new(inputs))
        .apply("ByKey", WithKeys::new(|p: &Pair<i64>| p.key.clone()))
        .apply("Group", GroupByKey::new())
        .apply(
            "SumValues",
            Map::new(|(k, pairs): (String, BeamIterable<Pair<i64>>)| {
                let total: i64 = pairs.into_iter().map(|p| p.expect("pair").value).sum();
                Pair {
                    key: k,
                    value: total,
                }
            }),
        )
        .inspect("Capture", move |p: &Pair<i64>| {
            sink.lock().unwrap().push(p.clone());
        });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("generic Pair<i64> GroupByKey pipeline must succeed");

    let mut actual = captured.lock().unwrap().clone();
    actual.sort_by(|a, b| a.key.cmp(&b.key));
    assert_eq!(
        actual,
        vec![
            Pair {
                key: "a".to_string(),
                value: 35,
            },
            Pair {
                key: "b".to_string(),
                value: 5,
            },
        ]
    );
}

#[derive(State)]
struct DynamicRowAccumState {
    latest: ValueState<Row>,
    history: BagState<Row>,
}

#[derive(Clone)]
struct DynamicRowStatefulFn {
    schema: Arc<Schema>,
}

impl StatefulDoFn for DynamicRowStatefulFn {
    type Key = String;
    type Value = Row;
    type Out = (String, i64, i64);
    type State = DynamicRowAccumState;
    type Timer = NoTimers;

    fn process_element(
        &mut self,
        key: &String,
        value: Row,
        state: &mut DynamicRowAccumState,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        let prev = match state.latest.read()? {
            Some(row) => row.get_i64("amount")?.unwrap_or(0),
            None => 0,
        };
        let current = value.get_i64("amount")?.unwrap_or(0);
        let updated = Row::builder(Arc::clone(value.schema()))
            .with_value(key.as_str())
            .with_value(prev + current)
            .build()?;
        state.latest.write(&updated)?;
        state.history.append(&value)?;
        let seen = state.history.read()?;
        let latest = state
            .latest
            .read()?
            .and_then(|r| r.get_i64("amount").ok().flatten())
            .unwrap_or(0);
        ctx.emit((key.clone(), latest, seen.len() as i64))
    }

    fn state_row_schemas(&self) -> StateRowSchemas {
        StateRowSchemas::new()
            .with_value("latest", Arc::clone(&self.schema))
            .with_value("history", Arc::clone(&self.schema))
    }
}

#[tokio::test]
async fn stateful_dofn_reads_and_writes_dynamic_row_value_and_bag_state_on_prism() {
    let schema = Arc::new(
        Schema::builder()
            .field("acct", FieldType::string())
            .field("amount", FieldType::int64())
            .build(),
    );
    let make_row = |acct: &str, amount: i64| {
        (
            acct.to_string(),
            Row::builder(Arc::clone(&schema))
                .with_value(acct)
                .with_value(amount)
                .build()
                .expect("valid row"),
        )
    };
    let inputs = vec![
        make_row("u1", 10),
        make_row("u1", 20),
        make_row("u2", 7),
        make_row("u1", 5),
    ];

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);

    let p = Pipeline::new();
    p.apply("Create", Create::new(inputs))
        .apply(
            "AccumulateRows",
            ParDo::new(DynamicRowStatefulFn {
                schema: Arc::clone(&schema),
            }),
        )
        .inspect("Capture", move |out: &(String, i64, i64)| {
            sink.lock().unwrap().push(out.clone());
        });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("ValueState<Row> and BagState<Row> pipeline must succeed");

    let mut by_key_max: std::collections::BTreeMap<String, (i64, i64)> =
        std::collections::BTreeMap::new();
    for (k, sum, len) in captured.lock().unwrap().iter() {
        let entry = by_key_max.entry(k.clone()).or_insert((0, 0));
        if *len > entry.1 {
            *entry = (*sum, *len);
        }
    }
    assert_eq!(by_key_max.get("u1"), Some(&(35, 3)));
    assert_eq!(by_key_max.get("u2"), Some(&(7, 1)));
}

#[derive(Debug, Clone, PartialEq, Eq, Hash, BeamRow)]
struct RegionKey {
    region: String,
}

#[derive(Debug, Clone, PartialEq, BeamRow)]
struct Quota {
    limit: i64,
    burst: Option<i64>,
}

#[tokio::test]
async fn multimap_side_input_with_distinct_row_schemas_and_to_from_rows_on_prism() {
    let p = Pipeline::new();
    let quotas = p
        .apply(
            "Quotas",
            Create::new(vec![
                (
                    RegionKey {
                        region: "us".into(),
                    },
                    Quota {
                        limit: 100,
                        burst: Some(25),
                    },
                ),
                (
                    RegionKey {
                        region: "eu".into(),
                    },
                    Quota {
                        limit: 80,
                        burst: None,
                    },
                ),
            ]),
        )
        .as_multimap();

    let bonuses = p
        .apply(
            "Bonuses",
            Create::new(vec![(
                "us".to_string(),
                Quota {
                    limit: 5,
                    burst: None,
                },
            )]),
        )
        .as_iter();

    let captured = Arc::new(Mutex::new(Vec::new()));
    let sink = Arc::clone(&captured);

    p.apply(
        "Requests",
        Create::new(vec![
            Pair {
                key: "us".into(),
                value: 10_i64,
            },
            Pair {
                key: "eu".into(),
                value: 5_i64,
            },
        ]),
    )
    .to_rows("ToRows")
    .apply("ShuffleRows", Reshuffle::new())
    .from_rows::<Pair<i64>>("FromRows")
    .map_with_side_inputs(
        "LookupQuota",
        (quotas, bonuses),
        |req: Pair<i64>, (map, bonus_iter)| {
            let k = RegionKey {
                region: req.key.clone(),
            };
            let quota = map.get(&k)?.expect("quota exists");
            let bonus: i64 = bonus_iter
                .to_vec()?
                .into_iter()
                .filter(|(region, _)| region == &req.key)
                .map(|(_, q)| q.limit)
                .sum();
            Ok((
                req.key,
                req.value + quota.limit + quota.burst.unwrap_or(0) + bonus,
            ))
        },
    )
    .inspect("Capture", move |item: &(String, i64)| {
        sink.lock().unwrap().push(item.clone());
    });

    p.run_with_runner(&PrismRunner::new())
        .await
        .expect("multimap row side input and ToRows/FromRows pipeline must succeed");

    let mut out = captured.lock().unwrap().clone();
    out.sort();
    assert_eq!(out, vec![("eu".into(), 85), ("us".into(), 140)]);
}
