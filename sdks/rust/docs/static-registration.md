<!--
    Licensed to the Apache Software Foundation (ASF) under one
    or more contributor license agreements.  See the NOTICE file
    distributed with this work for additional information
    regarding copyright ownership.  The ASF licenses this file
    to you under the Apache License, Version 2.0 (the
    "License"); you may not use this file except in compliance
    with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

    Unless required by applicable law or agreed to in writing,
    software distributed under the License is distributed on an
    "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
    KIND, either express or implied.  See the License for the
    specific language governing permissions and limitations
    under the License.
-->

# Static Registration of User Code (future work)

**Status:** not started. This note records a known gap with the other Beam SDKs.
It also lists the work needed to close the gap. It is not a design.

## The gap

To execute a bundle, an SDK worker must convert the transforms in the pipeline
proto back into code.

| SDK | How the worker finds the user code | Rebuilds the pipeline? |
|---|---|:--:|
| Java | The DoFn is Java-serialized into `ParDoPayload` and deserialized on the worker. | No |
| Python | The DoFn is pickled into the payload. | No |
| Go | Functions and DoFn types are registered by name at startup (`register.Function…`, `register.DoFn…`). The payload carries the name plus the struct's serialized fields. | No |
| **Rust** | Closures cannot be serialized. The payload carries only a handler key, and the worker gets the key → closure map by **running the pipeline construction again**. | **Yes** |

### How Rust works today

- The same binary is the launcher and the worker. On a containerized runner, the
  user's `main` runs again in the worker container. It builds the pipeline again
  and then serves bundles over the Fn API.
- Construction registers one handler per transform with
  `Pipeline::register_transform_handler(key, handler)`. The key is the transform
  id. For combine and window_into stages, the key comes from the stage
  (`combine_stage_key`, `window_into_handler_key`). The handler is a `TransformFn`
  closure. It captures the user's closure or DoFn value, the coders, the
  side-input wiring and other values.
- The harness (`bundle_processor::handlers::lookup_handler`) resolves each
  transform in a `ProcessBundleDescriptor` against that in-memory map. Flatten,
  MapWindows and the standard window functions need no map entry: the harness
  has built-in handlers for them. A custom window function resolves through
  `window_into_handler_key`.
- Transforms that the Rust expansion service created for another SDK have no
  user `main` on the worker. Their `do_fn` has the URN
  `beam:dofn:rust:expanded:v1` and carries a replay entry, and the worker
  builds that expansion again instead of a user pipeline
  ([`harness::replay`](../beam/harness/src/replay.rs)).
  This is still a rebuild, not static registration.

### Consequences

- The worker must build the pipeline **exactly** as the launcher built it. Each
  transform must get the same id. Construction must not depend on values that
  differ between the two processes (environment, time, randomness, local files).
- Code that is not a `main` needs more machinery, so that the worker knows which
  pipeline to rebuild. ValidatesRunner tests are the main example. The runner-tests
  crate uses a test registry and a `--vr_test` pipeline option for this
  (see [validates-runner.md](validates-runner.md)).
- Construction-time side effects run twice: once on the launcher and once on each worker.

## What static registration would involve

The target is the model of the Go SDK. The worker links the user code and finds
it by a stable name from the proto. The worker never builds a pipeline.

- **Stable identity for user code.** A closure has no name that is valid across
  processes, so user code that runs on workers must be a named type or function.
  A derive or macro (for example `#[derive(DoFn)]` or `register_dofn!`) would submit
  a `type name → factory` entry through `inventory`. The SDK already uses `inventory`
  for runners, option groups, secret resolvers, file systems, SchemaTransform
  providers and the expansion replay.
- **Serialized state.** The fields of a DoFn go into `ParDoPayload.do_fn.payload`
  (serde), and the worker decodes them. So registered DoFns must implement
  `Serialize + DeserializeOwned`.
- **Closures.** `map(|x| x * factor)` cannot be registered statically. There are two options:
  - Remove closure support for code that runs on workers. This is the Go rule:
    state lives in struct fields.
  - **Hybrid**: registered, serializable DoFns go through the registry. Closures
    keep the current rebuild path.
- **Built-in transforms.** Each transform that registers a handler needs a serializable form:
  - map, filter and flat_map wrappers
  - Combine (CombineFns and their accumulators)
  - window_into. This is the easiest: the window fn spec is already in the payload.
  - side inputs, state and timers, splittable DoFn
  - TestStream
  - PAssert. Its matchers capture the expected values, so they must become serializable.
- **Coders.** Standard coders already have a URN. Custom Rust-typed coders would
  need registration by type name.
- **Harness.** `lookup_handler` resolves from the global registry and the decoded
  payload, not from the handler map of the pipeline. The handler map stays only
  for the hybrid fallback, if the SDK keeps one.
- **Worker entry point.** A generic worker `main` links the user crates and serves
  bundles. It constructs no pipeline. For ValidatesRunner, the test worker
  (`runner-tests/src/worker.rs`) becomes a few lines, and `--vr_test` and the
  registry lookup are removed. The registry stays useful for writing tests: it
  holds the build step, the expectation and the shared driver.

## Open questions

- Hybrid or registration-only? The answer depends mainly on how much of the
  closure ergonomics of the fluent API must stay.
- Stability of names across releases. Type names change when code is refactored,
  so an explicit `#[beam(name = "...")]` override may be needed. Update
  compatibility on Dataflow depends on these names.
- Serialization format for DoFn state: serde_json for debuggability, or a binary
  format. How to version the format.
- Cross-language: the Rust expansion service already provides Rust transforms,
  and the worker replays each expansion (`beam:dofn:rust:expanded:v1`). A
  provider whose transforms all use registered DoFns could emit registered
  specs instead and need no replay.

## Scope

This work changes the core SDK API. It touches `core`, `harness`, `fluent`,
`testing`, `io`, `ml` and the examples, and it affects every user pipeline. It
needs a design review before implementation. Deliver it in increments: start
with registered DoFns behind the hybrid fallback.
