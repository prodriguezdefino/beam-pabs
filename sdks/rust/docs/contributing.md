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

# Contributing to the Rust SDK

This page gives the rules for code, public API and documentation in the Rust SDK.
Reviewers check each change against them. [build.md](build.md) gives the build and test
tasks, and [benchmarks/README.md](../benchmarks/README.md) gives the benchmarks.

The workspace needs Rust 1.98 or later (`rust-version` in
[`Cargo.toml`](../Cargo.toml), edition 2024). The repository has no
`rust-toolchain` file. The `beam_rust_builder` image uses `rust:1.98`.

## Public API Changes

Every public API change follows these rules:

- **No aliases or compatibility shims.** A rename removes the old name. The same
  change updates every caller, example and doc.
- **One public path per item.** Pipeline items are in their module
  (`transforms`, `transforms::sdf`, `windowing`, `coders`, ...). Runner and
  harness plumbing (`BundleHandler`, `ElementSink`, `HandlerContext`, ...) is
  only in `beam::internals`. The preludes are the only additional path.
- **Fluent methods mirror core transforms.** Each method is the snake_case name
  of exactly one core transform (`.group_by_key` → `GroupByKey`). It adds no
  semantics. Documented exceptions:
  - `map_with_side_inputs`, a `ParDo` that maps each element with side input
    values.
  - One `*_globally` and one `*_per_key` method for each SDK combine function
    (`.sum_per_key` → `CombinePerKey` with `Sum`). A method returns `Result`
    when the constructor of its combine function does (`approximate_*`).
  - One method for each set operation (`.intersect_all` → `Sets::intersect_all`).
  - `key_by`, a `WithKeys` with a key function.
  - `par_do_fn`, a `ParDo` of a closure `DoFn` (`ParDo::from_fn`).
  - `fold_globally` and `fold_per_key`, a combine with `FoldCombineFn`.
  - `fold_values`, a `GroupByKey` followed by a `ParDo` that folds the values of
    each key. It has no merge function.
  - `par_do_batch`, a composite of `BatchElements` and a `ParDo` of a
    `BatchedDoFn`. `par_do_batch_elementwise` adds an `ExplodeBatch` step that
    emits each output element.
- **Fluent covers common cases.** For advanced configuration, use
  `.apply(name, Transform::new(..))`, which also chains. Do not add fluent variants
  for advanced configuration.
- **Construction.** Build transforms with `new(required..)` and `.with_*()`
  builders. The name of a transform is given at `apply(name, transform)`, not to
  the constructor. Value types (e.g. `FixedWindows::of`) are exempt. Every
  setter that takes a value uses `with_*`. A setter without a value that turns
  off a default uses `without_*` (`without_defaults`, `without_sharding`).
- **Errors.** User hooks (`DoFn`, `CombineFn`, state, timers) return `beam::Result`.
- **Prelude.** `beam::prelude` contains only the items that a typical pipeline author uses.

## Programming style

Write functional, data-oriented, idiomatic Rust that follows current practice.

- **Functional.** Prefer pure functions and immutable bindings. Use iterator
  chains (`map`, `filter`, `fold`) and expressions (`match`, `if let`) instead of
  loops that change local state. Keep side effects at the edges of a module.
- **Data-oriented.** Model the domain with plain data types first, then write
  functions that transform them. Use `enum`s for closed sets of states, so that
  invalid states cannot be represented. Keep data in contiguous collections and
  avoid pointer-heavy object graphs and deep trait hierarchies.
- **Pattern matching.** Use `match` to branch on the shape of data, not chains
  of `if` with type or flag checks. Match exhaustively and do not add a `_` arm
  to an `enum` that the crate owns, so that a new variant is a compile error.
  Destructure structs, tuples and slices in `let`, function parameters and match
  arms instead of reading fields one at a time. Use guards (`if`), bindings
  (`name @ ..`) and or-patterns (`A | B`) to keep each case in one arm. Use
  `if let` or `let`-`else` when only one case is important. Branch on a `bool` with
  `if` and `else`, not with a `match` that has `true` and `false` arms.
- **No clones, no copies.** Borrow when you can and move when you must. Do not
  `clone` to satisfy the borrow checker; change the ownership design instead.
  Pass `&str`, `&[T]` and `impl Iterator` instead of `String`, `Vec<T>` or
  collected copies. Use `Cow` when a value is only sometimes changed, and
  `bytes::Bytes` or slices to share buffers without copying them. Reuse buffers
  in hot paths instead of allocating per element. A `clone` in a hot path needs
  a comment that says why it is necessary.
- **Avoid locks.** Design so that data has one owner. Move it between tasks
  through channels, or split it so that each worker owns its part. Prefer
  immutable shared data (`Arc<T>` without a lock) and atomics over `Mutex` and
  `RwLock`. If a lock is necessary, keep the critical section short, never hold
  it across `.await` or I/O, and do not nest locks.
- **Errors as values.** Return `Result` or `Option` and propagate with `?`. Do
  not call `unwrap`, `expect` or `panic!` outside tests, unless an invariant makes
  the failure impossible; state the invariant in the `expect` message.
  `PTransform::expand` cannot return an error: record a configuration error with
  `Pipeline::record_invalid_construction` (then `validate` fails). Use
  `Pipeline::invalid_construction_output` when the transform must return an output
  after that error.
  Implement `TryPTransform` for an expansion that can fail outside the program, such as a
  cross-language expansion. Then users get the error from `try_apply`.
- **Types carry meaning.** Use newtypes for identifiers and units. Use
  `From`/`TryFrom`, `Default`, `Display` and other standard traits instead of
  custom conversion methods.
- **Small generic surface.** Prefer `impl Trait` and generics over `dyn Trait`
  unless type erasure is necessary. Keep trait bounds at the point of use.
- **Reuse first.** Before you write a codec, a format mapping, a client, a retry loop
  or a helper, search for one that exists. Search the workspace and `Cargo.lock`. Use
  the first of these that fits: code in the workspace, a feature of a dependency that
  the workspace has, a maintained crate, new code. For Google services, look at the
  Google client libraries first.
  A format mapping (`Row` to and from bytes) goes in a format crate under `beam/io/`, and
  a connector only adapts it. Code that two crates need goes in the crate that both use.
- **Current Rust.** Use the workspace edition and its idioms: `let`-`else`,
  `async fn` in traits where it fits, `std::sync::LazyLock` instead of
  `lazy_static`. Code must pass `./gradlew :sdks:rust:check` with no warnings.
- **Tests in `tests/`.** Put tests in the `tests` directory of the crate, not in
  a `#[cfg(test)]` module in `src`. To test an internal module, make it
  `#[doc(hidden)] pub` and keep the user API unchanged.
- **Test layout.** Cargo builds each top-level entry of `tests/` as one binary. Make one
  binary for each area of `src` (`tests/<area>/main.rs`, one submodule per topic). Keep
  tests that start a runtime, a mock server or Prism in a different binary from fast pure
  tests. Put helpers in a `common.rs` submodule of the binary that uses them.
- **No `unsafe`** without a `// SAFETY:` comment that states why the code is
  sound.
- **Test placement.** Some transforms and combine functions are built from other
  transforms. Test them with unit tests in their crate and with Prism tests in
  `runner-tests/tests`. Add a ValidatesRunner test only for a feature that a
  runner must support. These features are primitive transforms, combine lifting,
  state and timers, windowing, and coders that cross the Fn API.
- **Skipped tests.** Mark a test that needs an external service, library or
  variable with `#[ignore = "reason"]`. Do not return early from `#[test]`;
  `--ignored` must fail when the prerequisite is missing.

## Lint checks

`./gradlew :sdks:rust:check` runs rustfmt and clippy with `-D warnings`, so each lint below
fails the build. The lints are in `[workspace.lints]` of [`Cargo.toml`](../Cargo.toml),
and each crate turns them on with `[lints] workspace = true`. The generated `apache-beam-model`
crate is the only exception. Reviewers check the other rules.

| Rule | Lints |
|---|---|
| Errors as values | `unwrap_used`, `expect_used`, `panic`, `missing_panics_doc` |
| No clones, no copies | `redundant_clone`, `clone_on_ref_ptr`, `needless_pass_by_value`, `cloned_instead_of_copied`, `large_types_passed_by_value`, `rc_buffer` |
| Pattern matching | `match_bool`, `match_wildcard_for_single_variants`, `manual_let_else`, `redundant_else`, `if_not_else` |
| Avoid locks | `await_holding_lock` (clippy default) |
| No `unsafe` without `// SAFETY:` | `undocumented_unsafe_blocks` |
| Current Rust | `uninlined_format_args`, `manual_let_else`, `semicolon_if_nothing_returned` |
| One public path per item | `unreachable_pub`, `unused_qualifications` |
| No leftover debug code | `dbg_macro`, `todo` |
| Formatting | rustfmt with [`rustfmt.toml`](../rustfmt.toml) (100 columns) |

- **Tests.** Tests and benches can use `unwrap`, `expect` and `panic!`. `clippy.toml` and
  `gradle/lint.gradle` allow them there.
- **Exceptions.** Suppress a lint with `#[expect(clippy::<lint>, reason = "...")]`, not
  `#[allow]`. `allow_attributes_without_reason` asks for the reason, and a stale `#[expect]`
  gives a warning. For `expect_used`, the reason states the invariant.
- **Limits.** `match_wildcard_for_single_variants` finds a `_` arm only when it stands for
  one variant. Reviewers check the other `_` arms on crate enums.
- **New lints.** Count the warnings of a lint before you turn it on:
  `./gradlew :sdks:rust:clippyLintReport -PclippyLints=<lint>,<lint>`.

## Comments and documentation

Comments and rustdoc follow the writing rules of
[ASD-STE100 Simplified Technical English](https://www.asd-ste100.org/about_STE.html).
Beam, Rust and protocol terms are permitted technical names.

- **Short sentences.** At most 20 words for an instruction and 25 words for a
  description. One topic per sentence.
- **Active voice.** Write instructions in the imperative: "Call `finish` before
  `drop`", "Do not share the sink".
- **One word, one meaning.** Use the same term for the same thing in all files.
- **Say what the code cannot.** Write a comment for a constraint, a contract or a
  reason that is not clear from the code. Do not repeat what the code does.
- **No numbered steps.** If the order is important, say so in words or split the
  code into functions.
- **No history.** Do not write "used to", "now", "previously", "no longer" or
  "Regression:". Write the rule that is true for the current code. A test
  comment states the behavior that the test checks.
- **This SDK only.** Do not compare with other SDKs ("like Java", "same as Go",
  "the equivalent of"). Name another SDK only when the behavior depends on it.
  Examples are a cross-language transform that the code calls, and a wire format,
  URN or protocol rule that other SDKs must decode. Tests in `beam/runner-tests` mirror
  the cross-SDK `ValidatesRunner` suites and may cite the upstream test name.
  Comparisons between SDKs go in [`sdk-parity.md`](sdk-parity.md).
- **Plain words.** Use the simplest word that is correct. Do not use: thus, hence,
  therefore, obey, ensure, utilize, leverage, facilitate, via, prior to,
  subsequent, terminate, approximately, sufficient, robust, seamless,
  comprehensive, crucial, essential, simply, merely, basically, actually,
  effectively, notably, furthermore, moreover, additionally, "in order to",
  "note that", "stem from". Use so, follow, make sure, use, help, through,
  before, then, stop, about, enough, also, to.
- **Short.** A comment is shorter than the code it explains. Fill lines to about
  90 columns: one short sentence does not need its own line.
- **Rustdoc.** Start with a one-line summary. Do not repeat the item name
  ("Returns the key." on `fn key`). Add `# Errors`, `# Panics` or `# Safety` only
  when the condition is not clear from the signature and the summary.
