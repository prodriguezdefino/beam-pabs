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

mod support;

use std::sync::Arc;
use std::time::Duration;

use beam::coders::DefaultCoder;
use beam::internals::{HandlerContext, ResidualCollector};
use beam::pipeline::Pipeline;
use beam::pipeline::constants::{
    URN_REQUIREMENT_SPLITTABLE_DOFN, URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS,
};
use beam::prelude::*;
use beam::transforms::PeriodicImpulse;
use beam::transforms::display_data::{DisplayDataBuilder, HasDisplayData};
use beam::transforms::sdf::OffsetRange;
use model::pipeline::is_bounded::Enum as Bounded;
use testing::{TestPipeline, passert};

#[test]
fn test_periodic_impulse_builder_and_accessors() {
    let impulse = PeriodicImpulse::new("PeriodicImpulse", Duration::from_secs(10))
        .with_max_read_time(Duration::from_secs(60))
        .with_limit(6);

    let mut builder = DisplayDataBuilder::new();
    impulse.populate_display_data(&mut builder);
    let items: Vec<_> = builder
        .build()
        .into_iter()
        .map(|i| (i.key, i.value))
        .collect();
    let expected = [
        ("transform", "PeriodicImpulse"),
        ("interval_ms", "10000"),
        ("max_read_time_ms", "60000"),
        ("limit", "6"),
    ]
    .map(|(k, v)| (k.to_string(), v.to_string()));
    assert_eq!(items, expected);

    // Optional settings are omitted when unset.
    let mut builder = DisplayDataBuilder::new();
    PeriodicImpulse::new("PeriodicImpulse", Duration::from_millis(1))
        .populate_display_data(&mut builder);
    let keys: Vec<_> = builder.build().into_iter().map(|i| i.key).collect();
    assert_eq!(keys, ["transform", "interval_ms"]);
}

fn assert_generate_expansion(p: &Pipeline, pcoll_id: &str) {
    let proto = p.to_proto();
    assert!(
        proto
            .requirements
            .contains(&URN_REQUIREMENT_SPLITTABLE_DOFN.to_string())
    );
    // The output is produced by the SDF under PeriodicImpulse/Generate, fed by the
    // PeriodicImpulse/Impulse root.
    let generate = support::transform(&proto, "PeriodicImpulse/Generate");
    assert!(
        generate.outputs.values().any(|o| o == pcoll_id),
        "Generate must produce the PeriodicImpulse output"
    );
    let impulse_out = support::single_output(&proto, "PeriodicImpulse/Impulse");
    assert_eq!(
        generate.inputs.values().collect::<Vec<_>>(),
        [&impulse_out.to_string()]
    );
    assert_eq!(support::pcoll_coder(&proto, pcoll_id), "varint");
}

#[test]
fn test_periodic_impulse_pipeline_expansion() {
    let p = Pipeline::new();
    let impulses = p.apply(
        PeriodicImpulse::new("PeriodicImpulse", Duration::from_millis(500))
            .with_limit(10)
            .with_max_read_time(Duration::from_secs(5)),
    );
    assert_eq!(
        p.lock().components.pcollections[impulses.id()].is_bounded,
        Bounded::Bounded as i32,
        "with_limit makes the output bounded"
    );
    assert_generate_expansion(&p, impulses.id());

    let p = Pipeline::new();
    let unbounded = p.apply(PeriodicImpulse::new(
        "PeriodicImpulse",
        Duration::from_millis(500),
    ));
    assert_eq!(
        p.lock().components.pcollections[unbounded.id()].is_bounded,
        Bounded::Unbounded as i32,
        "without a limit the output is unbounded"
    );
    assert_generate_expansion(&p, unbounded.id());
}

#[test]
fn periodic_impulse_waits_one_interval_between_ticks() {
    let p = Pipeline::new();
    let _ = p.apply(PeriodicImpulse::new("PeriodicImpulse", Duration::from_secs(10)).with_limit(5));

    let (handler_id, handler) = p
        .transform_handlers()
        .into_iter()
        .find(|(k, _)| k.contains("Generate"))
        .expect("Generate handler must be registered");
    let stage = handler
        .stage_handler(URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS)
        .expect("ProcessSizedElements stage handler must exist");

    let process = |element: &[u8]| {
        let residuals = Arc::new(ResidualCollector::new());
        let mut sink = Vec::<Vec<u8>>::new();
        let mut ctx = HandlerContext::new(&mut sink)
            .with_transform_id(&handler_id)
            .with_residual_collector(Some(&residuals));
        stage.instantiate().process(element, &mut ctx).unwrap();
        let ticks: Vec<i64> = sink.iter().map(|b| i64::decode(b).unwrap()).collect();
        let residuals = residuals.drain();
        assert_eq!(residuals.len(), 1, "one checkpoint per call");
        let r = residuals.into_iter().next().unwrap();
        let ((_, rest), _) = <((Vec<u8>, OffsetRange), f64)>::decode(&r.element).unwrap();
        (ticks, r.delay, (rest.start, rest.end), r.element)
    };

    // Tick 0 is due immediately. With split size 1, the SDF then checkpoints at once.
    let element = ((Vec::<u8>::new(), OffsetRange::new(0, 5)), 1.0)
        .encode()
        .unwrap();
    let (ticks, delay, rest, residual) = process(&element);
    assert_eq!(ticks, [0]);
    assert_eq!(delay, None);
    assert_eq!(rest, (1, 5));

    // On resume, tick 1 is not due until one interval after the start. Nothing is emitted,
    // and the runner is asked to resume in about 10 s.
    let (ticks, delay, rest, _) = process(&residual);
    assert!(ticks.is_empty(), "{ticks:?}");
    let delay = delay.expect("resume delay");
    assert!(
        delay > Duration::from_secs(8) && delay <= Duration::from_secs(10),
        "{delay:?}"
    );
    assert_eq!(rest, (1, 5));
}

#[tokio::test]
async fn test_periodic_impulse_slowly_changing_dimension_pattern() {
    let p = TestPipeline::new();

    // A periodic impulse that refreshes a dimension table three times.
    let snapshots = p
        .apply(PeriodicImpulse::new("PeriodicImpulse", Duration::from_millis(10)).with_limit(3))
        .apply(Map::new("ToDimensionSnapshot", |tick: i64| {
            format!("dimension_v{tick}")
        }));

    passert::that("AssertSnapshots", &snapshots)
        .contains_in_any_order(["dimension_v0", "dimension_v1", "dimension_v2"].map(String::from));
    p.run().await.expect("pipeline and assertions");
}
