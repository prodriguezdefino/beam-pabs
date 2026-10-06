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

//! Tests that `BatchedDoFnAdapter` passes each `DoFn` call on to its `BatchedDoFn`.

use std::sync::{Arc, Mutex};

use beam::coders::DefaultCoder;
use beam::internals::{HandlerContext, PipelineInternals};
use beam::prelude::*;
use beam::transforms::display_data::{DisplayDataBuilder, DisplayDataItem, DisplayDataValue};
use beam::transforms::{BatchedDoFn, BatchedDoFnAdapter, FinishBundleContext};

/// Sums each batch, and records each lifecycle call in a log that all copies share.
#[derive(Clone, Default)]
struct SumBatches {
    calls: Arc<Mutex<Vec<&'static str>>>,
}

impl SumBatches {
    fn record(&self, call: &'static str) -> Result {
        self.calls.lock().map_err(|e| e.to_string())?.push(call);
        Ok(())
    }
}

impl BatchedDoFn for SumBatches {
    type InBatch = Vec<i64>;
    type OutBatch = Vec<i64>;

    fn setup(&mut self) -> Result {
        self.record("setup")
    }

    fn start_bundle(&mut self) -> Result {
        self.record("start_bundle")
    }

    fn process_batch(&mut self, batch: Vec<i64>, ctx: &mut ProcessContext<'_, Vec<i64>>) -> Result {
        self.record("process_batch")?;
        ctx.emit(vec![batch.iter().sum()])
    }

    fn finish_bundle(&mut self, _ctx: &mut FinishBundleContext<'_, Vec<i64>>) -> Result {
        self.record("finish_bundle")
    }

    fn teardown(&mut self) -> Result {
        self.record("teardown")
    }

    fn populate_display_data(&self, builder: &mut DisplayDataBuilder) {
        builder.add_text("kind", "sum");
    }
}

#[test]
fn adapter_passes_every_call_to_the_batched_dofn() {
    let batched = SumBatches::default();
    let calls = Arc::clone(&batched.calls);
    let p = Pipeline::new();
    let _ = p
        .apply("Batches", Create::new(vec![vec![1_i64, 2]]))
        .apply("Sum", ParDo::new(BatchedDoFnAdapter::new(batched)));

    let components = p.to_proto().components.unwrap();
    let (id, transform) = components
        .transforms
        .iter()
        .find(|(_, t)| t.unique_name == "Sum")
        .unwrap();
    let display: Vec<_> = transform
        .display_data
        .iter()
        .map(|item| DisplayDataItem::from_proto(item).unwrap())
        .map(|item| (item.key, item.value))
        .collect();
    assert!(display.contains(&(
        "kind".to_string(),
        DisplayDataValue::Text("sum".to_string())
    )));

    let mut stage = p.transform_handlers().remove(id).unwrap().instantiate();
    let mut sink = Vec::<Vec<u8>>::new();
    stage.setup().unwrap();
    stage.start_bundle().unwrap();
    for batch in [vec![1_i64, 2], vec![10, 20, 30]] {
        stage
            .process(
                &batch.encode().unwrap(),
                &mut HandlerContext::new(&mut sink),
            )
            .unwrap();
    }
    stage
        .finish_bundle(&mut HandlerContext::new(&mut sink))
        .unwrap();
    stage.teardown().unwrap();

    let sums: Vec<Vec<i64>> = sink
        .iter()
        .map(|bytes| Vec::decode(bytes).unwrap())
        .collect();
    assert_eq!(sums, vec![vec![3], vec![60]]);
    assert_eq!(
        *calls.lock().unwrap(),
        [
            "setup",
            "start_bundle",
            "process_batch",
            "process_batch",
            "finish_bundle",
            "teardown"
        ]
    );
}
