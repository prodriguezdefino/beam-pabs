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

use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};

use beam::internals::ElementSink;
use beam::internals::{BundleFinalizerCollector, HandlerContext};
use beam::pipeline::Pipeline;
use beam::pipeline::constants::URN_REQUIREMENT_BUNDLE_FINALIZATION;
use beam::transforms::ProcessContext;
use beam::transforms::{Create, DoFn, ParDo};
use prost::Message;

struct TestSink;

impl ElementSink for TestSink {
    fn push(&mut self, _element: Vec<u8>) -> Result<(), String> {
        Ok(())
    }
}

#[test]
fn test_bundle_finalizer_collector_basic() {
    let collector = BundleFinalizerCollector::new();
    assert!(!collector.has_callbacks());

    let executed = Arc::new(AtomicBool::new(false));
    let exec_clone = Arc::clone(&executed);

    collector.register_callback(Box::new(move || {
        exec_clone.store(true, Ordering::SeqCst);
        Ok(())
    }));

    assert!(collector.has_callbacks());

    let callbacks = collector.drain();
    assert!(!collector.has_callbacks());
    assert_eq!(callbacks.len(), 1);

    for cb in callbacks {
        assert!(cb().is_ok());
    }
    assert!(executed.load(Ordering::SeqCst));
}

#[test]
fn test_process_context_register_finalizer() {
    let mut sink = TestSink;
    let collector = Arc::new(BundleFinalizerCollector::new());
    let ctx: ProcessContext<'_, String> =
        ProcessContext::new(&mut sink).with_bundle_finalizer(Some(&collector));

    let call_count = Arc::new(AtomicUsize::new(0));

    let count_1 = Arc::clone(&call_count);
    ctx.register_finalizer(move || {
        count_1.fetch_add(1, Ordering::SeqCst);
        Ok(())
    });

    let count_2 = Arc::clone(&call_count);
    ctx.register_finalizer(move || {
        count_2.fetch_add(10, Ordering::SeqCst);
        Ok(())
    });

    assert!(collector.has_callbacks());
    let callbacks = collector.drain();
    assert_eq!(callbacks.len(), 2);

    for cb in callbacks {
        assert!(cb().is_ok());
    }
    assert_eq!(call_count.load(Ordering::SeqCst), 11);
}

#[test]
fn test_handler_context_projects_bundle_finalizer() {
    let mut sink = TestSink;
    let collector = Arc::new(BundleFinalizerCollector::new());
    let mut h_ctx = HandlerContext::new(&mut sink).with_bundle_finalizer(Some(&collector));

    let p_ctx: ProcessContext<'_, i32> = h_ctx.as_process_context();

    let committed = Arc::new(Mutex::new(Vec::new()));
    let committed_clone = Arc::clone(&committed);

    p_ctx.register_finalizer(move || {
        committed_clone
            .lock()
            .unwrap()
            .push("tx_1_committed".to_string());
        Ok(())
    });

    assert!(collector.has_callbacks());
    let callbacks = collector.drain();
    for cb in callbacks {
        cb().unwrap();
    }
    assert_eq!(*committed.lock().unwrap(), vec!["tx_1_committed"]);
}

/// Registers a callback per element and requests bundle finalization.
#[derive(Clone)]
struct FinalizingFn;

impl DoFn for FinalizingFn {
    type In = String;
    type Out = String;

    fn process_element(
        &mut self,
        element: String,
        ctx: &mut ProcessContext<'_, String>,
    ) -> beam::Result {
        ctx.register_finalizer(|| Ok(()));
        ctx.emit(element)
    }

    fn requests_finalization(&self) -> bool {
        true
    }
}

/// Never registers a callback and does not request bundle finalization.
#[derive(Clone)]
struct PlainFn;

impl DoFn for PlainFn {
    type In = String;
    type Out = String;

    fn process_element(
        &mut self,
        element: String,
        ctx: &mut ProcessContext<'_, String>,
    ) -> beam::Result {
        ctx.emit(element)
    }
}

/// Returns the `requests_finalization` flag of the named `ParDo` transform.
fn requests_finalization(proto: &model::pipeline::Pipeline, name: &str) -> bool {
    let transform = proto
        .components
        .as_ref()
        .expect("components")
        .transforms
        .values()
        .find(|t| t.unique_name == name)
        .unwrap_or_else(|| panic!("no transform named {name}"));
    let payload = &transform.spec.as_ref().expect("spec").payload;
    model::pipeline::ParDoPayload::decode(payload.as_slice())
        .expect("ParDo payload")
        .requests_finalization
}

#[test]
fn finalizing_dofn_requests_finalization_from_the_runner() {
    let p = Pipeline::new();
    let words = p.apply(Create::new("Create", vec!["a".to_string()]));
    let _ = words.apply(ParDo::new("Finalizing", FinalizingFn));
    let _ = words.apply(ParDo::new("Plain", PlainFn));

    let proto = p.to_proto();
    // Runners send FinalizeBundle only for transforms that set this flag.
    // Without this flag, the runner drops the registered callbacks silently.
    assert!(requests_finalization(&proto, "Finalizing"));
    assert!(!requests_finalization(&proto, "Plain"));
    assert!(
        proto
            .requirements
            .contains(&URN_REQUIREMENT_BUNDLE_FINALIZATION.to_string()),
        "a runner that cannot finalize bundles must reject the pipeline: {:?}",
        proto.requirements
    );
}

#[test]
fn pipeline_without_finalizing_dofns_does_not_require_finalization() {
    let p = Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec!["a".to_string()]))
        .apply(ParDo::new("Plain", PlainFn));

    assert!(
        !p.to_proto()
            .requirements
            .contains(&URN_REQUIREMENT_BUNDLE_FINALIZATION.to_string())
    );
}
