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

A worker finds the code of a transform in one of two ways:

- **Registration.** A registered function type, or the elements of a `Create`, travels in the pipeline graph. The worker builds the function from the graph.
- **Rebuild.** The worker runs the pipeline construction again and finds the handler by key. Closures and generic SDK transforms use this path.

While one transform of a pipeline uses the rebuild path, the worker runs the user's `main`. [sdk-parity.md](sdk-parity.md) compares this with the other SDKs.

## Status

| Capability | Status | Note |
|---|:--:|---|
| Registered `DoFn`, `StatefulDoFn`, `SplittableDoFn` | ✅ | `#[derive(Register)]`. State, timers and side inputs need nothing else. |
| Registered `CombineFn` | ✅ | Any key coder that the SDK can delimit. |
| SDK combine functions | ✅ | Standard element types. `register!` adds other types. |
| `Create` | ✅ | The spec holds the encoded elements. |
| Format version of registered fields | ✅ | A worker rejects another version with a clear error. |
| Graph check on the worker | ✅ | Prism and Dataflow record the graph; the worker compares it before it serves bundles. |
| Rust expansion service | 🟡 | An expansion of registered functions needs no replay. The IO providers use unregistered `DoFn`s, so their expansions use replay. |
| Closures, generic SDK transforms | Rebuild | By design: they have no name across processes. |
| Custom `WindowFn` | ❌ | The trait cannot build a window function from its URN and payload, so it uses the rebuild path. |

## How the worker resolves a function spec

The harness (`bundle_processor::handlers::resolve_key_spec`) dispatches on the URN of `ParDoPayload.do_fn`, or of `CombinePayload.combine_fn` for the combine stages:

| URN | Payload | Worker action |
|---|---|---|
| `beam:dofn:rust:registered:v1` | Id, fields format and fields of a `DoFn`, `StatefulDoFn` or `SplittableDoFn` | Finds the type in the link-time registry and deserializes it. |
| `beam:combine_fn:rust:registered:v1` | Id, fields format and fields of a `CombineFn`, and the key coder | Builds the combine stage over encoded keys. |
| `beam:dofn:rust:create:v1` | The encoded elements of a `Create` | Emits the elements. |
| `beam:dofn:rust:expanded:v1` | A replay entry | Builds the expansion of the Rust expansion service again ([`harness::replay`](../beam/harness/src/replay.rs)). |
| `beam:dofn:rust:v1`, `beam:combine_fn:rust:v1` | A handler key | Looks up the key in the handler map of the rebuilt pipeline. |

Flatten, MapWindows and the standard window functions need no user code. The harness has built-in handlers for them.

## Register a type

A registered type implements `serde::Serialize` and `serde::Deserialize`, because its fields travel in the graph.

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

| Topic | Rule |
|---|---|
| Id | The module path and the type name. `#[beam(id = "...")]` overrides it. Set it only when the launcher and the worker are different binaries and the type moves between them. |
| Generic types | Each registration names one concrete type. `beam::register!(Type<Args>)` registers one instantiation, with the same id rule. |
| Kind | The trait that the type implements selects the handler: `DoFn`, `StatefulDoFn`, `SplittableDoFn`, or the stages of a `CombineFn`. |
| Construction | `ParDo`, `SplittableParDo` and `CombinePerKey` find the type by `TypeId` and write the id and the fields (serde_json) into the spec. |
| Fields | A `#[serde(skip)]` field gets its `Default` value on the worker. Build connections and other process-local values in `setup`. |
| Format version | The spec carries the fields format (`fields_format`). A worker with another format fails the bundle with an error that names both formats. Build the launcher and the worker with the same SDK version. |
| Errors | Two types with one id, or fields that do not serialize, give `PipelineError::InvalidConstruction` at `Pipeline::validate`; the transform keeps a rebuild handler. On the worker, an id with no registration fails the bundle: the worker binary must link the crate of the type. |
| State, timers, side inputs | `#[derive(State)]`, `#[derive(Timers)]` and the views of `side_inputs(&self)` travel in the spec. |
| Splittable `DoFn` | The registered handler gives the expanded SDF stages. |
| No derive | The type uses the rebuild path and needs no `serde` implementation. |
| Diagnostics | `Pipeline::rebuild_path_transforms()` lists the transforms that use the rebuild path. The launcher logs them at submit. |

### SDK combine functions

The SDK registers its combine functions for these element types, with ids that start with `beam::transforms::`:

| Combine functions | Element types |
|---|---|
| `Sum`, `Max`, `Min`, `Mean` | `i32`, `i64`, `f64`, `Decimal` |
| `Sample`, `AnySample`, `ApproximateUnique` | `i32`, `i64`, `f64`, `bool`, `String`, `Vec<u8>`, `Decimal` |
| `Top`, `ApproximateQuantiles` with `Natural` or `Reversed` | the same types, without `f64` (not `Ord`) |

For another element type, register the instantiation, for example `beam::register!(Top<MyScore, Natural>)`. A closure ordering, `Compose`, `On` and `MapOutput` hold closures, so they use the rebuild path.

### Combine keys

The registry knows the `CombineFn` type, not the key type `K`. The spec carries a `StandardKey` tag, the key coder and the coders that the key coder refers to.

| Key | How the stages run |
|---|---|
| Standard (`()`, `bool`, `i32`, `i64`, `String`, `Vec<u8>`) | The precombine before the shuffle runs over `K`, also in the first phase of a hot key fanout. The other stages run over `EncodedKey`. |
| Composite and row (`(K1, K2)`, `Option<K>`, `Vec<T>`, `#[derive(BeamRow, Clone, PartialEq, Eq, Hash)]` structs) | Over `EncodedKey`. The key coder delimits each key (`skips_exactly`). |
| Unknown coder URN | Registered only inside `LengthPrefixCoder`. Without it, `CombinePerKey` uses the rebuild path. |

Each typed key adds handlers for every registered `CombineFn` to every binary, so only the precombine stage has them. The URN of the stage transform selects `combine_grouped_values` or a lifted stage (precombine, merge, extract, convert to accumulators).

## The rebuild path

The launcher and the worker are the same binary. On a containerized runner, the user's `main` runs again in the worker container, builds the pipeline and serves bundles over the Fn API. Construction registers one handler for each transform with `Pipeline::register_transform_handler(key, handler)`. The key is the transform id; combine and window_into stages use `stage_key` and `window_into_handler_key`.

These transforms use the rebuild path:

- closures in `map`, `filter`, `flat_map` and the other fluent methods
- unregistered function types
- generic SDK transforms, for example the `WriteFiles` `DoFn`s
- SDK combine functions over element types that the SDK does not register
- combine functions that hold closures, and `CombinePerKey` with a key coder that the SDK cannot delimit
- custom window functions
- `TestStream` and `PAssert`: the matchers capture the expected values

### Requirements

- Build the pipeline on the worker exactly as on the launcher: every transform gets the same id. Construction must not depend on values that differ between the processes, such as the environment, the time, random values or local files.
- Construction runs on the launcher and again on each worker, so its side effects run more than once.
- Code that is not a `main` must tell the worker which pipeline to build. The runner-tests crate uses a test registry and the `--vr_test` option ([validates-runner.md](validates-runner.md)).
- Submit the job with `Pipeline::options`. The job options carry the launcher facts that the worker needs: the cross-language records and the graph record.

### Cross-language transforms

A worker does not expand cross-language transforms. Each output gets a placeholder with no windowing strategy. Its boundedness and its coder, with the row schema, come from the launcher through the job options. Do not add or skip a transform on the windowing of a cross-language output.

The Rust expansion service serves expansions to other SDKs, so the worker has no user `main` for them. The service writes a replay entry into each spec that holds a handler key, and the worker builds that expansion again. Registered specs and `Create` pass through unchanged, so an expansion of registered functions needs no replay.

### Graph check

The Prism and Dataflow runners record each transform that resolves through the handler map in the job options: its id, spec URN, handler key, and input and output PCollection ids. Before a worker serves bundles, it compares its rebuilt graph with the record.

- A difference fails the worker with `RunnerError::GraphMismatch`. The error names the first transform id that differs and the part that differs.
- A worker input that is a cross-language placeholder matches any launcher input with the same tag, because the worker does not know the PCollection ids of an expansion.
- Coder and windowing strategy ids are not in the record: a process-wide counter gives them.
- Registered transforms and `Create` are not in the record: their code travels in the graph.
- Without a record, the worker skips the check.

## Limits

- The graph check cannot see a different closure under the same name with the same wiring.
- Prism LOOPBACK workers run in the launcher process. They do not rebuild, so they do not check.
- Expansion-service replay workers are not checked.
- Registered transforms do not remove the rebuild while another transform of the pipeline uses it.

## Why the rebuild stays

The worker reaches compiled code only through a name, and Rust names only concrete types.

- **Closures.** `map(|x| x * factor)` has no name across processes, and its captured values do not serialize.
- **Generic SDK transforms.** Most SDK `DoFn`s are generic over the element types of the user (`WriteFiles<T>`, the fluent methods). The registry holds one entry for each instantiation. Encoded bytes replace a type only where the code does not decode it, as with combine keys.

A worker that never rebuilds needs an API where all worker code is a named, serializable type, and the user registers each instantiation of a generic transform. This SDK keeps the closures of the fluent API.
