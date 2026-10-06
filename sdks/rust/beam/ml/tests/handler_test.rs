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

//! Unit tests for [`BatchBounds`], [`InferenceArgs`], [`ModelHandler`] defaults,
//! [`KeyedModelHandler`] and [`PredictionResultCoder`].

use std::collections::HashMap;
use std::sync::Mutex;
use std::time::Duration;

use beam::coders::{Coder, CoderRegistry, Context, DefaultCoder, URN_KV};
use beam::transforms::VecBatchConverter;
use beam_ml::{
    BatchBounds, InferenceArgs, KeyedModelHandler, ModelHandler, PredictionResult,
    PredictionResultCoder,
};

/// Handler relying on every provided `ModelHandler` method.
#[derive(Clone, Debug)]
struct DefaultsHandler;

impl ModelHandler<String, String> for DefaultsHandler {
    type Model = ();
    type Batch = Vec<String>;
    type Converter = VecBatchConverter<String>;

    fn load_model(&self) -> beam::Result<Self::Model> {
        Ok(())
    }

    fn run_inference(
        &self,
        batch: &Self::Batch,
        _model: &Self::Model,
        _inference_args: Option<&InferenceArgs>,
    ) -> beam::Result<Vec<String>> {
        Ok(batch.clone())
    }

    fn get_batch_converter(&self) -> Self::Converter {
        VecBatchConverter::new()
    }
}

/// Records registrations and hands out sequential ids.
#[derive(Default)]
struct RecordingRegistry {
    calls: Mutex<Vec<(String, Vec<String>)>>,
}

impl CoderRegistry for RecordingRegistry {
    fn register_coder(&self, urn: &str, component_coder_ids: Vec<String>) -> String {
        let mut calls = self.calls.lock().expect("registry lock");
        calls.push((urn.to_string(), component_coder_ids));
        format!("coder_{}", calls.len())
    }
}

#[test]
fn test_batch_bounds_with_duration_keeps_sizes() {
    let bounds = BatchBounds::new(2, 8).with_duration(Duration::from_millis(7));
    assert_eq!(
        bounds,
        BatchBounds {
            min_batch_size: 2,
            max_batch_size: 8,
            max_batch_duration: Some(Duration::from_millis(7)),
        }
    );
}

#[test]
fn test_batch_bounds_with_duration_secs() {
    let bounds = BatchBounds::new(3, 4).with_duration_secs(0.25);
    assert_eq!(bounds.min_batch_size, 3);
    assert_eq!(bounds.max_batch_size, 4);
    assert_eq!(bounds.max_batch_duration, Some(Duration::from_millis(250)));
}

#[test]
fn test_batch_bounds_new_has_no_duration() {
    assert_eq!(BatchBounds::new(1, 1).max_batch_duration, None);
}

#[test]
#[should_panic(expected = "min_batch_size must be >= 1")]
fn test_batch_bounds_rejects_zero_min() {
    BatchBounds::new(0, 4);
}

#[test]
#[should_panic(expected = "max_batch_size must be >= min_batch_size")]
fn test_batch_bounds_rejects_max_below_min() {
    BatchBounds::new(5, 4);
}

#[test]
fn test_inference_args_store_and_lookup() {
    let args = InferenceArgs::new()
        .with("temperature", "0.5")
        .with("top_k", "40");
    assert_eq!(args.get("temperature"), Some("0.5"));
    assert_eq!(args.get("top_k"), Some("40"));
    assert_eq!(args.get("missing"), None);

    let expected: HashMap<String, String> = [("temperature", "0.5"), ("top_k", "40")]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v.to_string()))
        .collect();
    assert_eq!(args.all(), &expected);
    assert!(InferenceArgs::new().all().is_empty());
}

#[test]
fn test_inference_args_later_value_wins() {
    let args = InferenceArgs::new().with("k", "a").with("k", "b");
    assert_eq!(args.get("k"), Some("b"));
    assert_eq!(args.all().len(), 1);
}

#[test]
fn test_model_handler_defaults() {
    let mut handler = DefaultsHandler;
    assert_eq!(handler.get_batch_bounds(), BatchBounds::default());
    assert_eq!(handler.model_id(), None);
    handler
        .update_model_path("gs://bucket/model")
        .expect("default update_model_path is a no-op");
}

#[test]
fn test_batch_bounds_default_values() {
    assert_eq!(
        BatchBounds::default(),
        BatchBounds {
            min_batch_size: 1,
            max_batch_size: 64,
            max_batch_duration: Some(Duration::from_millis(50)),
        }
    );
}

#[test]
fn test_keyed_model_handler_debug_and_inner() {
    let keyed: KeyedModelHandler<String, String, String, DefaultsHandler> =
        KeyedModelHandler::new(DefaultsHandler);
    assert_eq!(
        format!("{keyed:?}"),
        "KeyedModelHandler { inner: DefaultsHandler }"
    );
    assert_eq!(keyed.inner().model_id(), None);
}

#[test]
fn test_prediction_result_coder_urn_and_roundtrip() {
    let coder = PredictionResult::<String, i64>::coder();
    assert_eq!(Coder::<PredictionResult<String, i64>>::urn(&coder), URN_KV);

    let value = PredictionResult::new("input".to_string(), -42i64);
    for context in [Context::Nested, Context::WholeStream] {
        let mut buf = Vec::new();
        coder.encode(&value, &mut buf, context).expect("encode");
        assert!(!buf.is_empty());
        let decoded = coder.decode(&mut buf.as_slice(), context).expect("decode");
        assert_eq!(decoded, value);
    }
}

#[test]
fn test_prediction_result_coder_new_matches_default() {
    let coder = PredictionResultCoder::new(String::coder(), String::coder());
    let value = PredictionResult::new("a".to_string(), "b".to_string());
    let mut via_new = Vec::new();
    coder
        .encode(&value, &mut via_new, Context::Nested)
        .expect("encode");
    let mut via_element = Vec::new();
    value.encode_element(&mut via_element).expect("encode");
    assert_eq!(via_new, via_element);
    assert_eq!(
        PredictionResult::<String, String>::decode_element(&mut via_element.as_slice())
            .expect("decode"),
        value
    );
}

#[test]
fn test_prediction_result_registers_kv_of_components() {
    let registry = RecordingRegistry::default();
    let id = PredictionResult::<String, String>::register_coder(&registry);
    let calls = registry.calls.lock().expect("registry lock");
    let (urn, components) = calls.last().expect("kv registration");
    assert_eq!(urn, URN_KV);
    assert_eq!(components.len(), 2);
    assert_eq!(id, format!("coder_{}", calls.len()));
}
