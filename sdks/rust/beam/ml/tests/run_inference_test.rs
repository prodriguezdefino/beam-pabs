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

//! Integration tests for RunInference, micro-batching, and Dead-Letter Queue routing on Prism runner.

use beam::transforms::VecBatchConverter;
use beam_ml::{BatchBounds, InferenceArgs, ModelHandler, PredictionResult, RunInference};
use fluent::prelude::*;
use testing::{TestPipeline, passert};

#[derive(Clone, Debug)]
struct TestUppercaseModel {
    prefix: String,
}

#[derive(Clone, Debug, Default)]
struct TestUppercaseModelHandler {
    batch_size: usize,
}

impl ModelHandler<String, String> for TestUppercaseModelHandler {
    type Model = TestUppercaseModel;
    type Batch = Vec<String>;
    type Converter = VecBatchConverter<String>;

    fn load_model(&self) -> Result<Self::Model> {
        Ok(TestUppercaseModel {
            prefix: "OUT:".to_string(),
        })
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        model: &Self::Model,
        _inference_args: Option<&InferenceArgs>,
    ) -> Result<Vec<String>> {
        let results = batch
            .iter()
            .map(|s| format!("{}{}", model.prefix, s.to_uppercase()))
            .collect();
        Ok(results)
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        BatchBounds::new(1, self.batch_size)
    }
}

#[tokio::test]
async fn run_inference_executes_batched_predictions() {
    let p = TestPipeline::new();

    let words = p.apply(Create::new(
        "Inputs",
        vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
            "date".to_string(),
        ],
    ));

    let predictions = words.apply(RunInference::new(
        "Inference",
        TestUppercaseModelHandler { batch_size: 2 },
    ));

    passert::that("AssertPredictions", &predictions).contains_in_any_order([
        PredictionResult::new("apple".to_string(), "OUT:APPLE".to_string()),
        PredictionResult::new("banana".to_string(), "OUT:BANANA".to_string()),
        PredictionResult::new("cherry".to_string(), "OUT:CHERRY".to_string()),
        PredictionResult::new("date".to_string(), "OUT:DATE".to_string()),
    ]);

    p.run().await.expect("pipeline succeeds");
}

#[derive(Clone, Debug, Default)]
struct FailingModelHandler;

impl ModelHandler<String, String> for FailingModelHandler {
    type Model = ();
    type Batch = Vec<String>;
    type Converter = VecBatchConverter<String>;

    fn load_model(&self) -> Result<Self::Model> {
        Ok(())
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        _model: &Self::Model,
        _inference_args: Option<&InferenceArgs>,
    ) -> Result<Vec<String>> {
        for item in batch {
            if item.contains("fail") {
                return Err("Simulated model error for fail item".into());
            }
        }
        Ok(batch.iter().map(|s| s.to_uppercase()).collect())
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }

    fn get_batch_bounds(&self) -> BatchBounds {
        BatchBounds::new(1, 1)
    }
}

#[tokio::test]
async fn run_inference_dlq_routes_failed_elements() {
    let p = TestPipeline::new();

    let inputs = p.apply(Create::new(
        "MixedInputs",
        vec![
            "good_item".to_string(),
            "fail_item".to_string(),
            "another_good".to_string(),
        ],
    ));

    let outputs = inputs
        .apply(RunInference::new("InferWithDLQ", FailingModelHandler).with_exception_handling());

    passert::that("PAssert", &outputs.output).contains_in_any_order([
        PredictionResult::new("good_item".to_string(), "GOOD_ITEM".to_string()),
        PredictionResult::new("another_good".to_string(), "ANOTHER_GOOD".to_string()),
    ]);

    let failed_inputs = outputs.failures.map("ExtractFailedInput", |f| f.input);
    passert::that("AssertFailedInputs", &failed_inputs)
        .contains_in_any_order(["fail_item".to_string()]);

    p.run().await.expect("pipeline with DLQ succeeds");
}
