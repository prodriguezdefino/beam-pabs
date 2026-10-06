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

//! Bundle finalization declared by `WithFinalizer`: the context methods, the expiry of each
//! callback, and the payload flag and requirement of each function kind.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::{Duration, Instant as Clock};

use beam::coders::DefaultCoder;
use beam::internals::{BundleFinalizerCollector, BundleHandler, DoFnHandler, HandlerContext};
use beam::pipeline::URN_REQUIREMENT_BUNDLE_FINALIZATION;
use beam::prelude::*;
use beam::transforms::{DEFAULT_FINALIZER_EXPIRY, WithFinalizer};
use prost::Message;

/// Counts each element and each callback that runs.
#[derive(Clone, Default)]
struct Counts {
    callbacks: Arc<AtomicUsize>,
}

impl Counts {
    fn callback(&self) -> impl FnOnce() -> Result + Send + 'static {
        let callbacks = Arc::clone(&self.callbacks);
        move || {
            callbacks.fetch_add(1, Ordering::SeqCst);
            Ok(())
        }
    }

    fn ran(&self) -> usize {
        self.callbacks.load(Ordering::SeqCst)
    }
}

/// Registers one callback per element, one with a short expiry, and one in `finish_bundle`.
#[derive(Clone, Default)]
struct Acknowledging {
    counts: Counts,
}

impl DoFn for Acknowledging {
    type In = String;
    type Out = WithFinalizer<String>;

    fn process_element(&mut self, id: String, ctx: &mut ProcessContext<'_, Self::Out>) -> Result {
        ctx.finalizer(self.counts.callback()).register()?;
        ctx.finalizer(self.counts.callback())
            .expire_after(Duration::from_secs(5))
            .register()?;
        ctx.emit(id.into())
    }

    fn finish_bundle(&mut self, ctx: &mut FinishBundleContext<'_, Self::Out>) -> Result {
        ctx.finalizer(self.counts.callback()).register()
    }
}

/// Emits with no finalization.
#[derive(Clone)]
struct Plain;

impl DoFn for Plain {
    type In = String;
    type Out = String;

    fn process_element(&mut self, id: String, ctx: &mut ProcessContext<'_, String>) -> Result {
        ctx.emit(id)
    }
}

#[test]
fn callbacks_carry_their_deadline_and_run_once_drained() {
    let collector = Arc::new(BundleFinalizerCollector::new());
    let fun = Acknowledging::default();
    let mut handler = DoFnHandler::new(fun.clone());
    let mut sink = Vec::<Vec<u8>>::new();
    let element = DefaultCoder::encode(&"a".to_string()).expect("encode");
    let before = Clock::now();
    {
        let mut ctx = HandlerContext::new(&mut sink).with_bundle_finalizer(Some(&collector));
        BundleHandler::process(&mut handler, &element, &mut ctx).expect("process");
        handler.finish_bundle(&mut ctx).expect("finish_bundle");
    }
    let after = Clock::now();

    let pending = collector.drain();
    let deadlines: Vec<Duration> = pending.iter().map(|p| p.deadline - before).collect();
    assert_eq!(deadlines.len(), 3);
    let window = after - before;
    let within =
        |deadline: Duration, expiry: Duration| deadline >= expiry && deadline <= expiry + window;
    assert!(
        within(deadlines[0], DEFAULT_FINALIZER_EXPIRY),
        "{deadlines:?}"
    );
    assert!(
        within(deadlines[1], Duration::from_secs(5)),
        "{deadlines:?}"
    );
    assert!(
        within(deadlines[2], DEFAULT_FINALIZER_EXPIRY),
        "{deadlines:?}"
    );
    assert!(!collector.has_callbacks());

    assert_eq!(fun.counts.ran(), 0, "registering runs nothing");
    pending
        .into_iter()
        .try_for_each(|p| (p.callback)())
        .expect("callbacks");
    assert_eq!(fun.counts.ran(), 3);
    assert_eq!(sink.len(), 1, "the element is emitted");
}

#[test]
fn a_callback_expires_only_after_its_deadline() {
    let collector = Arc::new(BundleFinalizerCollector::new());
    let mut sink = Vec::<Vec<u8>>::new();
    let mut ctx = HandlerContext::new(&mut sink).with_bundle_finalizer(Some(&collector));
    let ctx: ProcessContext<'_, WithFinalizer<String>> = ctx.as_process_context();
    ctx.finalizer(|| Ok(()))
        .expire_after(Duration::ZERO)
        .register()
        .expect("register");
    let [pending] = collector.drain().try_into().ok().expect("one callback");
    assert!(!pending.is_expired(pending.deadline));
    assert!(pending.is_expired(pending.deadline + Duration::from_nanos(1)));
}

#[test]
fn register_fails_when_the_runner_gave_no_finalizer() {
    let mut handler = DoFnHandler::new(Acknowledging::default());
    let mut sink = Vec::<Vec<u8>>::new();
    let element = DefaultCoder::encode(&"a".to_string()).expect("encode");
    let error = BundleHandler::process(&mut handler, &element, &mut HandlerContext::new(&mut sink))
        .expect_err("no finalizer");
    assert!(error.contains("no finalizer"), "{error}");
    assert!(
        sink.is_empty(),
        "the element after the failed register is not emitted"
    );
}

/// The `requests_finalization` flag of the transform named `name`.
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

fn requires_finalization(proto: &model::pipeline::Pipeline) -> bool {
    proto
        .requirements
        .contains(&URN_REQUIREMENT_BUNDLE_FINALIZATION.to_string())
}

#[test]
fn the_payload_flag_and_the_requirement_follow_the_output_type() {
    let p = Pipeline::new();
    let words = p.apply(Create::new("Create", vec!["a".to_string()]));
    let _ = words.apply(ParDo::new("Acknowledging", Acknowledging::default()));
    let _ = words.apply(ParDo::new("Plain", Plain));

    let proto = p.to_proto();
    assert!(requests_finalization(&proto, "Acknowledging"));
    assert!(!requests_finalization(&proto, "Plain"));
    assert!(requires_finalization(&proto), "{:?}", proto.requirements);
}

#[test]
fn a_pipeline_with_no_finalizing_function_does_not_require_finalization() {
    let p = Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec!["a".to_string()]))
        .apply(ParDo::new("Plain", Plain));
    assert!(!requires_finalization(&p.to_proto()));
}

/// Sends each word to `output` or `errors`, and declares finalization.
#[derive(Clone)]
struct Routing;

impl DoFn for Routing {
    type In = String;
    type Out = WithFinalizer<std::result::Result<String, String>>;

    fn process_element(&mut self, word: String, ctx: &mut ProcessContext<'_, Self::Out>) -> Result {
        ctx.emit(Ok(word).into())
    }
}

#[test]
fn with_finalizer_has_the_collections_and_tags_of_its_outputs() {
    let p = Pipeline::new();
    let WithFailures { output, failures } = p
        .apply(Create::new("Create", vec!["a".to_string()]))
        .apply(ParDo::new("Routing", Routing));
    let proto = p.to_proto();
    let transform = proto
        .components
        .as_ref()
        .expect("components")
        .transforms
        .values()
        .find(|t| t.unique_name == "Routing")
        .expect("Routing");
    assert_eq!(
        transform.outputs.get("output"),
        Some(&output.id().to_string())
    );
    assert_eq!(
        transform.outputs.get("errors"),
        Some(&failures.id().to_string())
    );
}
