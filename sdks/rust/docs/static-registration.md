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

# Static Registration of User Code

**Status:** the SDK uses a hybrid model. Registered function types (`DoFn`,
`StatefulDoFn`, `SplittableDoFn`, `CombineFn`) and `Create` travel in the
pipeline graph. Closures and the generic SDK transforms are found by building
the pipeline again on the worker. This note describes both paths, why the
rebuild stays, and the open items.

## Background

To execute a bundle, an SDK worker must convert the transforms in the pipeline
proto back into code.

| SDK | How the worker finds the user code | Rebuilds the pipeline? |
|---|---|:--:|
| Java | The DoFn is Java-serialized into `ParDoPayload` and deserialized on the worker. | No |
| Python | The DoFn is pickled into the payload. | No |
| Go | Functions and DoFn types are registered by name at startup (`register.Function…`, `register.DoFn…`). The payload carries the name plus the struct's serialized fields. | No |
| **Rust** | A registered function type travels as a stable id plus its serialized fields. Closures cannot be serialized, so for them the payload carries only a handler key, and the worker gets the key → closure map by **running the pipeline construction again**. | **Partly** |

## How the worker resolves a function spec

The harness (`bundle_processor::handlers::resolve_key_spec`) dispatches on the
URN of `ParDoPayload.do_fn`, or of `CombinePayload.combine_fn` for the stages
that a runner lifts:

| URN | Payload | How the worker gets the handler |
|---|---|---|
| `beam:dofn:rust:registered:v1` | Stable id and the serialized fields of a `DoFn`, `StatefulDoFn` or `SplittableDoFn` | Finds the type in the link-time registry and deserializes it. |
| `beam:dofn:rust:registered_combine:v1` | Stable id and fields of a `CombineFn`, the key coder, and the stage | Builds the stage of the `CombineFn` over encoded keys. |
| `beam:combine_fn:rust:registered:v1` | The same, without a stage | Builds the stage that the runner lifted. |
| `beam:dofn:rust:create:v1` | The encoded elements of a `Create` | Emits the elements. |
| `beam:dofn:rust:expanded:v1` | A replay entry | Builds the expansion of the Rust expansion service again ([`harness::replay`](../beam/harness/src/replay.rs)). |
| `beam:dofn:rust:v1`, `beam:combine_fn:rust_handler_key:v1` | A handler key | Looks up the key in the handler map of the rebuilt pipeline. |

Flatten, MapWindows and the standard window functions need no user code: the
harness has built-in handlers for them.

## Registered functions

`#[derive(Register)]` registers a `DoFn`, `StatefulDoFn`, `SplittableDoFn` or
`CombineFn` type. The type must also implement `serde::Serialize` and
`serde::Deserialize`, because its fields travel in the graph.

```rust
use beam::prelude::*;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Register)]
struct ScaleBy {
    factor: i64,
    #[serde(skip)]
    client: Option<Client>,
}

impl DoFn for ScaleBy {
    type In = i64;
    type Out = i64;

    fn setup(&mut self) -> Result {
        self.client = Some(Client::connect()?);
        Ok(())
    }

    fn process_element(&mut self, element: i64, ctx: &mut ProcessContext<'_, i64>) -> Result {
        ctx.emit(element * self.factor)
    }
}
```

- **Identity.** The derive submits a `FnRegistration` (id, `TypeId`, encode
  and build functions) through `inventory`. The id defaults to the module path
  and the type name. `#[beam(id = "...")]` overrides it. Set it only when one
  binary builds the graph and a different binary runs it, and the type moves
  between the two. Generic types cannot be registered: Rust has no generic
  statics, so each registration names one concrete type.
- **Kinds.** The registration builds a handler for a `DoFn`, `StatefulDoFn` or
  `SplittableDoFn`, or the stages of a `CombineFn`. The compiler picks the kind
  from the trait that the type implements.
- **Construction.** `ParDo`, `SplittableParDo` and `CombinePerKey` find the type
  of their function in the registry by `TypeId` and write the id and the fields
  (serde_json) into the function spec.
- **Fields.** A field with `#[serde(skip)]` gets its `Default` value on the
  worker. Build connections and other process-local values in `setup`.
- **Errors.** If two types share an id, or the fields do not serialize,
  `Pipeline::validate` reports `PipelineError::InvalidConstruction`. The
  transform keeps a rebuild handler, so the graph stays complete. On the worker,
  an id with no registration fails the bundle; the worker binary must link the
  crate that defines the type.
- **State, timers and side inputs.** A `StatefulDoFn` declares its state cells
  and timer families as types (`#[derive(State)]`, `#[derive(Timers)]`), and any
  `DoFn`, `StatefulDoFn` or `SplittableDoFn` returns its side-input views from
  `side_inputs(&self)`. Side-input views implement `Serialize` and
  `Deserialize`, and `ParDo` / `SplittableParDo` write the specs from the
  function value, so registered `DoFn`s with state, timers or side inputs need
  nothing else on the worker.
- **Splittable DoFns.** The registered handler gives the expanded SDF stages,
  as the rebuild handler does.
- **No registration.** A type without the derive uses the rebuild path. It
  needs no `serde` implementation.

### Combine stages over encoded keys

The registry knows the `CombineFn` type, but not the key type of the
`CombinePerKey`. So the spec carries the key coder and every coder that it refers
to, and the stages keep each key as the bytes that its coder wrote. The key
coder finds where each key ends; the value decodes with the static types of the
`CombineFn`. The stages are the same `DoFn`s as on the rebuild path, with the
key type `EncodedKey`, which writes its bytes unchanged. So the output is the
`KV` encoding of the original key type.

- The partial combine and the merge after the shuffle are `ParDo`s with
  `beam:dofn:rust:registered_combine:v1` specs, for runners that do not lift
  the combine.
- The `CombinePayload` holds a `beam:combine_fn:rust:registered:v1` spec. A
  runner that lifts the combine gives the stage (precombine, merge, extract).
- The key coder must be one that the SDK can delimit (`skips_exactly`): the
  standard coders and the `KV`, iterable, nullable and length prefix coders of
  them. For another key coder, the `CombinePerKey` takes the rebuild path.

## The rebuild path

- The same binary is the launcher and the worker. On a containerized runner, the
  user's `main` runs again in the worker container. It builds the pipeline again
  and then serves bundles over the Fn API.
- Construction registers one handler per transform with
  `Pipeline::register_transform_handler(key, handler)`. The key is the transform
  id. For combine and window_into stages, the key comes from the stage
  (`combine_stage_key`, `window_into_handler_key`). The handler is a `TransformFn`
  closure. It captures the user's closure or function value, the coders, the
  side-input wiring and other values.
- Transforms that the Rust expansion service created for another SDK have no
  user `main` on the worker. The worker builds that expansion again instead of
  a user pipeline. This is still a rebuild.

These items use the rebuild path:

- closures in `map`, `filter`, `flat_map` and the other fluent wrappers
- unregistered function types
- `CombinePerKey` with a key coder that the SDK cannot delimit
- generic SDK transforms, for example the `WriteFiles` `DoFn`s
- the built-in `CombineFn`s (`Sum`, `Max`, `Min`). They are not registered, so
  `Count` and WordCount keep the path that the benchmarks measure.
- custom window functions
- `TestStream` and `PAssert`. The `PAssert` matchers capture the expected values.

### Constraints of the rebuild path

- The worker must build the pipeline **exactly** as the launcher built it. Each
  transform must get the same id. Construction must not depend on values that
  differ between the two processes (environment, time, randomness, local files).
- Code that is not a `main` needs more machinery, so that the worker knows which
  pipeline to rebuild. ValidatesRunner tests are the main example. The runner-tests
  crate uses a test registry and a `--vr_test` pipeline option for this
  (see [validates-runner.md](validates-runner.md)).
- Construction-time side effects run twice: once on the launcher and once on each worker.

While any transform of a pipeline uses the rebuild path, the worker runs the
user's `main`. Registered transforms skip the handler map, and their fields keep
the values of the launcher, but they do not remove the rebuild.

## Why the rebuild stays

The worker can reach compiled code only through a name, and Rust names only
concrete types.

- **Closures.** `map(|x| x * factor)` has no name that is valid across
  processes, and its captured values do not serialize.
- **Generic SDK transforms.** Most SDK `DoFn`s are generic over the element
  types of the user (`WriteFiles<T>`, the fluent wrappers). The registry cannot
  hold one entry for each instantiation. Encoded bytes replace a type only where
  the code does not decode it, as with combine keys.

A worker that never rebuilds needs a different API: code that runs on workers is
always a named, serializable type, and generic transforms are registered for
each instantiation, as in the Go SDK. That is a separate design decision; this
SDK keeps the closures of the fluent API.

## Open items

- **Custom window functions.** A custom `WindowFn` already writes its own URN
  and payload, but the trait has no way to build it from them, so the worker
  uses the rebuild handler.
- **Graph check.** The worker does not verify that its rebuilt graph matches the
  graph of the launcher. A missing handler key already fails the bundle with an
  error that names the cause. A divergence with the same keys is not detected.
- **Cross-language.** A Rust expansion-service provider whose transforms all use
  registered types could emit registered specs and need no replay.
- **Serialization format.** The fields use serde_json for debuggability. A
  binary format is smaller. The format has no version field.
