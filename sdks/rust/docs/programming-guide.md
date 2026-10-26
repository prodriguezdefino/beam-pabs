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

# Rust SDK Programming Guide

This guide describes the Rust SDK: pipelines, transforms, side inputs, grouping,
windowing, state and timers, errors, metrics, schemas and I/O. For installation
and crate layout, see the [SDK README](../README.md). For the details of each
item, see the rustdoc.

## Two styles, one vocabulary

Each transform is a struct. You build it with `new(required…)` and configure it with
`.with_*()` builders. You give it a name when you apply it. You can apply it in two
equivalent ways:

- **apply style**: `pcoll.apply("Name", Map::new(f))`. This works for each
  transform, including your own `PTransform`s. It returns the output, so you can chain calls.
- **fluent style**: `pcoll.map("Name", f)`. Each method is the snake_case name of
  one core transform and covers the common case. For other cases (side inputs on
  several views, state, timers, multi-output), use `.apply(...)`.

```rust
use beam::prelude::*;
```

`beam::prelude` brings in both styles: the core transforms and traits, plus the
fluent extension traits (`PCollectionExt`, `CombineExt`, `PCollectionKeyedExt`,
`KeyedCombineExt`, `PCollectionListExt`) and `textio`. You can mix the two styles in
one chain.

## Transform names and scopes

Each applied transform has a name: the first argument of `apply` (or of the fluent
method). The runner shows it in its UI and in the step names of metrics.

A transform that applies other transforms is a composite. Its children get its full name
as a prefix: `words.apply("Count", CountPerElement::new())` adds `Count/PairWithOne` and
`Count/Sum`, and `Count/Sum` adds `Count/Sum/GroupByKey`. Names nest to any depth
(`Outer/Inner/Leaf`). At the root of the pipeline, a name stays as you give it. A primitive
transform, such as a `ParDo` or a `GroupByKey`, is one node with the name of its `apply`.

Your own `PTransform` works the same way. Inside `expand`, give each step a name in the
scope of the transform:

```rust
struct CleanWords;

impl PTransform<PCollection<String>> for CleanWords {
    type Output = PCollection<String>;

    fn expand(&self, input: &PCollection<String>) -> PCollection<String> {
        input
            .apply("NotEmpty", Filter::new(|w: &String| !w.is_empty()))
            .apply("Lower", Map::new(|w: String| w.to_lowercase()))
    }
}

// Adds `CleanTitles/NotEmpty` and `CleanTitles/Lower`, and the same steps under `CleanBodies`.
let titles = raw_titles.apply("CleanTitles", CleanWords);
let bodies = raw_bodies.apply("CleanBodies", CleanWords);
```

`apply` is on `PCollection`, `PCollectionList`, `KeyedPCollectionTuple`, `PBegin` and
`Pipeline`. `try_apply(name, transform)` applies a `TryPTransform` and returns its error.

Sibling transforms in a scope need distinct names. If two transforms get the same full
name, `Pipeline::validate` and `Pipeline::run` fail with
`PipelineError::DuplicateTransformName`, which gives the full name. The same name in two
different scopes is correct: `CleanTitles/Lower` and `CleanBodies/Lower` do not collide.

## Pipelines

```rust
use beam::prelude::*;

// Default options: runs on the Prism runner.
let p = Pipeline::new();
```

With options from the command line (standard Beam flags plus your own group):

```rust
use beam::prelude::*;

#[derive(clap::Args, serde::Serialize, serde::Deserialize, Clone, Debug)]
struct Args {
    #[arg(long, default_value = "/tmp/output.txt")]
    output: String,
}
impl PipelineOptionGroup for Args {}

#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
    let (options, args) = beam::options::parse::<Args>();
    let p = Pipeline::create(&options);
    p.apply("Lines", Create::new(vec!["a b".to_string(), "c".to_string()]))
        .apply("Write", textio::Write::new(&args.output));
    p.run().await?;
    Ok(())
}
```

Full example: [examples/wordcount](../examples/wordcount/src/lib.rs) (`Args`, `build_pipeline`).
Your crate needs `clap`, `serde` and `tokio` as dependencies for this pattern.

- `--runner` selects the runner (`prism` by default, `dataflow`, …).
- `p.run()` validates the graph, submits it and waits as the options specify.
- `p.run_with_runner(&runner)` runs on a runner instance. Use it for tests and custom runners.
- `PipelineOptions::parse()` parses only the standard flags;
  `PipelineOptions::with_runner("prism")` builds options in code.

## Element-wise transforms

`Map`, `FlatMap`, `Filter` and `Inspect` take plain closures (`Filter` and
`Inspect` get `&T`). The sources in the prelude are `Create::new(elements)`,
`GenerateSequence::new(start)` (`.with_end(n)` for a bounded sequence,
`.with_rate(..)` / `.with_period(..)` for a throttled unbounded one) and
`PeriodicImpulse::new(interval)`.

Apply style:

```rust
let lines = p.apply("Lines", Create::new(vec!["to be or".to_string(), "not".to_string()]));
let words = lines.apply("Split", FlatMap::new(|l: String| {
    l.split_whitespace().map(String::from).collect::<Vec<_>>()
}));
let long = words.apply("Long", Filter::new(|w: &String| w.len() > 2));
let upper = long.apply("Upper", Map::new(|w: String| w.to_uppercase()));
```

Fluent style:

```rust
let upper = p
    .apply("Lines", Create::new(vec!["to be or".to_string(), "not".to_string()]))
    .flat_map("Split", |l: String| l.split_whitespace().map(String::from).collect::<Vec<_>>())
    .filter("Long", |w: &String| w.len() > 2)
    .map("Upper", |w: String| w.to_uppercase());
```

| Core transform | Fluent method |
|---|---|
| `apply(name, Map::new(f))` | `.map(name, f)` |
| `apply(name, FlatMap::new(f))` | `.flat_map(name, f)` |
| `apply(name, Filter::new(pred))` | `.filter(name, pred)` |
| `apply(name, Inspect::new(f))` | `.inspect(name, f)` |
| `apply(name, ParDo::new(do_fn))` | `.par_do(name, do_fn)` |
| `apply(name, ParDo::from_fn(f))` | `.par_do_fn(name, f)` |
| `apply(name, TryMap::new(f))` | `.try_map(name, f)` |
| `Map` producing `(key_fn(&x), x)` | `.key_by(name, key_fn)` |
| `apply(name, CountPerElement::new())` | `.count_per_element(name)` |
| `apply(name, CountGlobally::new())` | `.count_globally(name)` |
| `apply(name, CombineGlobally::new(fn))` | `.combine_globally(name, fn)` |
| `apply(name, Reshuffle::new())` | `.reshuffle(name)` |
| `apply(name, Partition::new(n, f))` | `.partition(name, n, f)` |
| `apply(name, Flatten::new())` on a `PCollectionList` | `.flatten(name, &[&other])`, or `list.flatten(name)` |
| `apply(name, WindowInto::new(window_fn))` | `.window_into(name, window_fn)` |
| `apply(name, BatchElements::new(min, max))` | `.batch_elements(name, max)` (min 1) |
| `BatchElements` + a [`BatchedDoFn`](accelerated-workloads.md#batcheddofn) | `.par_do_batch(name, max, do_fn)` / `.par_do_batch_elementwise(name, max, do_fn)` |
| `apply(name, GroupByKey::new())` | `.group_by_key(name)` |
| `apply(name, CombinePerKey::new(fn))` | `.combine_per_key(name, fn)` |
| `apply(name, CoGroupByKey::new())` | `.co_group_by_key(name, &other)` |
| `InnerJoin` / `LeftJoin` / `RightJoin` / `FullOuterJoin` | `.inner_join` / `.left_join` / `.right_join` / `.full_outer_join` |
| `BroadcastInnerJoin` / `BroadcastLeftJoin` | `.broadcast_inner_join` / `.broadcast_left_join` |

## ParDo and `DoFn`

A [`DoFn`](../beam/core/src/transforms/dofn/pardo.rs) processes one element at a time
through a [`ProcessContext`](../beam/core/src/transforms/dofn/context/mod.rs).

- `DoFn` requires `Clone + Send + Sync + 'static`. The worker clones the
  prototype one time per bundle processor, so a copy never runs two bundles at
  the same time.
- Each hook takes `&mut self`. Keep per-bundle state in plain fields, with no lock.
- Each hook returns `beam::Result`, so `?` works on I/O, parse and coder errors.

A `DoFn` struct:

```rust
#[derive(Clone)]
struct SplitWords {
    min_len: usize,
}

impl DoFn for SplitWords {
    type In = String;
    type Out = String;

    fn process_element(&mut self, line: String, ctx: &mut ProcessContext<'_, String>) -> beam::Result {
        line.split_whitespace()
            .filter(|w| w.len() >= self.min_len)
            .try_for_each(|w| ctx.emit(w.to_string()))
    }
}

let words = lines.apply("SplitWords", ParDo::new(SplitWords { min_len: 3 })); // apply
let words = lines.par_do("SplitWords", SplitWords { min_len: 3 });            // fluent
```

A closure that needs the context (timestamps, windows, several outputs per element):

```rust
let words = lines.apply("Split", ParDo::from_fn(|line: String, ctx| {
    line.split_whitespace().try_for_each(|w| ctx.emit(w.to_string()))
}));

let words = lines.par_do_fn("Split", |line: String, ctx| {
    line.split_whitespace().try_for_each(|w| ctx.emit(w.to_string()))
});

// Take Windowed<T> to also receive the window:
let tagged = windowed.par_do_fn("TagWindow", |Windowed { value, window }: Windowed<String>, ctx| {
    ctx.emit(format!("{window} {value}"))
});
```

`map`, `flat_map` and `filter` stay pure functions of the element. If the
closure needs `ctx`, use `ParDo::from_fn` / `.par_do_fn`. If it keeps state
across elements or bundles, use a `DoFn` struct.

### Windows

The input type declares whether a function reads the window. A function that
takes `T` never sees one: an element in several windows (after
`SlidingWindows`) is processed once, and its outputs stay in all of those
windows. A function that takes `Windowed<T>` receives `value` and a
`BoundedWindow` (global or interval), once per window. This applies to closures
and to `DoFn` and `SplittableDoFn` structs (`type In = Windowed<T>`) alike; an SDF's
restriction methods then see the window too. A `StatefulDoFn` always runs one window at a time, so
`process_element` and `on_timer` both read it with `ctx.window()`.

A bare closure parameter needs its type (`|s: String, ctx|`), because `T` and
`Windowed<T>` are both valid inputs. Patterns infer: `|(k, v), ctx|`,
`|Windowed { value, window }, ctx|`.

### Lifecycle

`setup` → (`start_bundle` → `process_element`* / `on_timer`* → `finish_bundle`)* →
`teardown`. All hooks other than `process_element` have default no-op implementations.

```rust
#[derive(Clone)]
struct BatchWriter {
    endpoint: String,
    buffer: Vec<String>,
}

impl DoFn for BatchWriter {
    type In = String;
    type Out = String;

    fn start_bundle(&mut self) -> beam::Result {
        self.buffer.clear();
        Ok(())
    }

    fn process_element(&mut self, record: String, _ctx: &mut ProcessContext<'_, String>) -> beam::Result {
        self.buffer.push(record);
        if self.buffer.len() >= 500 {
            write_batch(&self.endpoint, &std::mem::take(&mut self.buffer))?;
        }
        Ok(())
    }

    fn finish_bundle(&mut self, _ctx: &mut FinishBundleContext<'_, String>) -> beam::Result {
        write_batch(&self.endpoint, &std::mem::take(&mut self.buffer))
    }
}
```

`finish_bundle` has no current element, so its `FinishBundleContext` makes every
output name its window and timestamp:
`ctx.emit(v, window, timestamp)`. To flush buffered elements into their own
windows, take `Windowed<T>` as input and keep each window with its value.

To run code after the runner commits a bundle (for example, to acknowledge
messages or commit offsets), declare `type Out = WithFinalizer<O>`. The function
keeps the outputs of `O` and emits each value wrapped: `ctx.emit(v.into())`. Its
`ProcessContext` and `FinishBundleContext` get
`ctx.finalizer(|| { …; Ok(()) }).register()?`; other functions have no
`finalizer`, so a missing declaration does not compile. `DoFn`, `StatefulDoFn`
and `SplittableDoFn` declare it the same way.

A callback expires 10 minutes after it is registered (`DEFAULT_FINALIZER_EXPIRY`);
`.expire_after(duration)` sets another time. It runs at most once, after the
runner commits the outputs and residuals of the bundle, and only before it
expires. It can also never run, so use it to acknowledge or release, never for
work that the output needs. `populate_display_data` adds display data for runner
UIs.

### Emitting output

| Call | Effect |
|---|---|
| `ctx.emit(v)` | Main output, inheriting timestamp, windows and pane |
| `ctx.emit_all(iter)` | `emit` for each item |
| `ctx.output(v).at(ts).emit()` | Main output with event timestamp `ts`, a `beam::Instant` |

The builder also offers `.with_pane(..)`, `.with_metadata(..)`
and, for single metadata fields, `.with_drain(..)`, `.with_value_kind(..)` and
`.with_trace(traceparent, tracestate)`.
Per-element facts: `ctx.timestamp()`, `ctx.pane()`, `ctx.metadata()`, `ctx.is_draining()`.

Event time is a [`beam::Instant`](../beam/core/src/time.rs) (milliseconds since
the epoch); add a `std::time::Duration` to move it, and
`window.max_timestamp()` is the last instant of a window.

### Several outputs

`type Out` declares the outputs, and so what applying the function returns
([outputs.rs](../beam/core/src/transforms/dofn/outputs.rs)):

| `type Out` | Emit | `ParDo` returns |
|---|---|---|
| `T` | `ctx.emit(t)` | `PCollection<T>` |
| `Result<T, F>` | `ctx.emit(Ok(t))`, `ctx.emit(Err(f))` | `WithFailures<T, F>` (`output`, `failures`) |
| enum with `#[derive(Outputs)]` | `ctx.emit(Enum::Variant(v))` | `<Enum>PCollections`, one field per variant |

Each output has its own element type and coder, and the compiler checks both:

```rust
#[derive(Outputs)]
enum BySize {
    Short(String),
    Long(String),
}

// inside ClassifyFn (type Out = BySize):
// ctx.emit(if word.len() > 5 { BySize::Long(word) } else { BySize::Short(word) })

let BySizePCollections { short, long } = words.par_do("BySize", ClassifyFn);
```

Field names are the variant names in snake case, and so are the output tags;
`#[output(id = "...")]` sets another tag. `Partition` keeps returning a
`PCollectionList` for a number of outputs chosen at run time.

## Side inputs

A view says how a side `PCollection` is read, and so what the function
receives ([view.rs](../beam/core/src/values/view.rs)):

| View | Made with | Value |
|---|---|---|
| `SingletonView<T>` | `as_singleton()` | `T` |
| `IterView<T>` | `as_iter()` | `SideIter<T>`: `iter()` (lazy), `to_vec()` |
| `MultimapView<K, V>` | `as_multimap()` | `SideMap<K, V>`: `get(&k)`, `get_all(&k)` |

Tuples of 2 to 6 views, arrays and `Vec`s of views give their values in the same
shape. A function with side inputs runs once per window, in that window.

Closures receive the values directly:

```rust
let factor = p.apply("Factor", Create::new(vec![3_i64])).as_singleton();
let stop = p.apply("Stop", Create::new(vec!["should".to_string()])).as_iter();

let scaled = amounts.map_with_side_inputs("Scale", &factor, |x, f| Ok(x * f));

let kept = words.par_do_fn_with_side_inputs("Keep", (&factor, &stop), |w: String, (min, stop), ctx| {
    if w.len() >= min as usize && !stop.to_vec()?.contains(&w) {
        ctx.emit(w)?;
    }
    Ok(())
});
```

Apply style: `ParDo::from_fn_with_side_inputs(name, views, f)`.

A `DoFn` struct (also `StatefulDoFn` and `SplittableDoFn`) keeps the views as
fields, returns them from `side_inputs()`, and reads them with
`ctx.side_input(&view)`. Views are serializable, so `#[derive(Register)]` works:

```rust
#[derive(Clone, Serialize, Deserialize, Register)]
struct KeepWordsFn {
    min_len: SingletonView<i64>,
    stop: IterView<String>,
}

impl DoFn for KeepWordsFn {
    type In = String;
    type Out = String;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.min_len, &self.stop]
    }

    fn process_element(&mut self, w: String, ctx: &mut ProcessContext<'_, String>) -> beam::Result {
        let min = ctx.side_input(&self.min_len)? as usize;
        if w.len() >= min && !ctx.side_input(&self.stop)?.to_vec()?.contains(&w) {
            ctx.emit(w)
        } else {
            Ok(())
        }
    }
}

let kept = words.par_do("Keep", KeepWordsFn { min_len, stop });
```

Full example: [examples/side_inputs](../examples/side_inputs/src/main.rs).

## Grouping, combining and joins

Apply style:

```rust
let grouped = scores.apply("Group", GroupByKey::new());               // (String, BeamIterable<i64>)
let totals = scores.apply("SumPerTeam", CombinePerKey::new(Sum::new())); // (String, i64)
let total = amounts.apply("Total", CombineGlobally::new(Sum::new()));    // i64
let counts = words.apply("Count", CountPerElement::new());               // (String, i64)
```

Fluent style:

```rust
let grouped = scores.group_by_key("Group");
let totals = scores.combine_per_key("SumPerTeam", Sum::new());
let total = amounts.combine_globally("Total", Sum::new());
let counts = words.count_per_element("Count");
```

A `BeamIterable<V>` yields `beam::Result<V>`: a value read from runner state can fail.
Write `for v in values { let v = v?; .. }`, `values.into_iter().sum::<beam::Result<i64>>()?`
or `values.into_vec()?`. It is read once, so it is not `Clone`.

Built-in combine functions. Use them with `CombinePerKey` or `CombineGlobally`, or
with the fluent shortcut (`_globally` on any collection, `_per_key` on `(K, V)`):

| Combine function | Input | Output | Fluent shortcut |
|---|---|---|---|
| `Sum::new()`, `Max::new()`, `Min::new()` | `i32`, `i64`, `f64` or `Decimal` | the input type | `.sum_globally(name)`, `.max_per_key(name)`, ... |
| `Mean::new()` | `i32`, `i64`, `f64` or `Decimal` | `f64`, or `Decimal` for `Decimal` | `.mean_globally(name)`, `.mean_per_key(name)` |
| `Top::largest(n)`, `Top::smallest(n)`, `Top::of(n, compare)` | any `T` (`Ord` for `largest` / `smallest`) | `Vec<T>`, best first | `.top_globally(name, n)`, `.smallest_per_key(name, n)`, ... |
| `Sample::new(n)` | any `T` | `Vec<T>`: a uniform random sample | `.sample_globally(name, n)`, `.sample_per_key(name, n)` |
| `AnySample::new(n)` | any `T` | `Vec<T>`: any `n` elements | — |
| `ApproximateUnique::with_sample_size(n)?` | any `T` | `i64`: the estimated number of distinct elements | `.approximate_unique_globally(name, n)?`, ... |
| `ApproximateQuantiles::new(n)?` | `T: Ord` | `Vec<T>`: `n` quantiles from the minimum to the maximum | `.approximate_quantiles_per_key(name, n)?`, ... |

```rust
let best = scores.top_per_key("BestPerTeam", 3); // (String, Vec<i64>)
let mean = amounts.mean_globally("MeanAmount");    // f64
```

The input sets the numeric type, so `Sum::new()` adds `i64` scores or `f64` amounts.
`Sum` returns an error on integer or decimal overflow. `Mean` adds integers as `f64`, so it
does not overflow. To compute several aggregates in one pass, use `Compose`, and `on` to
read each value from a field:

```rust
let stats = trips.combine_per_key(
    "TripStats",
    Compose::new((
        Sum::new().on(|_: &Trip| 1i64),
        Mean::new().on(|t: &Trip| t.miles),
        Top::largest(3).on(|t: &Trip| t.seconds),
    ))
    .map_output(|(trips, mean_miles, longest)| TripStats { trips, mean_miles, longest }),
);
```

For a custom aggregation, implement [`CombineFn`](../beam/core/src/transforms/combine.rs).
Each hook returns `beam::Result`; an error fails the bundle, and the runner retries it.

`CombinePerKey` expands to `GroupByKey` → `combine_grouped_values` and carries
the `CombineFn` in its payload. A runner that lifts it (Prism, Dataflow) runs a
partial combine before the shuffle, so only about one accumulator per key per
bundle crosses the shuffle. On windowed input, use
`CombineGlobally::new(..).without_defaults()` (fluent: `.combine_globally_without_defaults`).
The fluent `fold_per_key` / `fold_globally` build a `CombineFn` from a zero, a
fold closure and a merge closure. `fold_values(name, zero, fold)` has no merge
closure: it groups with `GroupByKey` and folds each group in sequence, so all
elements cross the shuffle.

### CoGroupByKey and joins

Apply style:

```rust
let grouped = KeyedPCollectionTuple::of("emails", &emails)
    .and("phones", &phones)
    .apply("CoGroup", CoGroupByKey::new());             // (String, CoGbkResult)
// per group: result.get::<String>("emails")? (BeamIterable) or result.get_vec::<String>("emails")?

let joined = emails.apply("Join", InnerJoin::new(&phones));          // (K, (V, V2))
let left = emails.apply("Left", LeftJoin::new(&phones));             // (K, (V, Option<V2>))
let bcast = orders.apply("Enrich", BroadcastLeftJoin::new(&users));  // no shuffle of `orders`
```

Fluent style:

```rust
let grouped = emails.co_group_by_key("CoGroup", &phones); // (K, (BeamIterable<V>, BeamIterable<V2>))

let joined = emails.inner_join("Join", &phones);
let left = emails.left_join("Left", &phones);
let bcast = orders.broadcast_left_join("Enrich", &users);
```

| Join | Output value |
|---|---|
| `InnerJoin` / `inner_join` | `(V, V2)` |
| `LeftJoin` / `left_join` | `(V, Option<V2>)` |
| `RightJoin` / `right_join` | `(Option<V>, V2)` |
| `FullOuterJoin` / `full_outer_join` | `(Option<V>, Option<V2>)` |
| `BroadcastInnerJoin` / `broadcast_inner_join` | `(V, V2)`, right side as a multimap side input |
| `BroadcastLeftJoin` / `broadcast_left_join` | `(V, Option<V2>)`, right side as a multimap side input |

Full example: [examples/join](../examples/join/src/lib.rs).

### Distinct and set operations

`Distinct::new()` (fluent `.distinct(name)`) keeps one element of each group of
equal elements, in each window. `DistinctBy::new(key_fn)`
(fluent `.distinct_by(name, key_fn)`) keeps one element for each key. Both use
`CombinePerKey`, so each worker removes duplicates before the shuffle.

`Sets` combines the input with another collection of the same element type, as in
SQL. The `_distinct` forms give each element once; the `_all` forms keep the
duplicates (all of them for a union, the smaller count for an intersection, the
difference of the counts for `except_all`):

```rust
let both = tags.apply("Common", Sets::intersect_distinct(&popular)); // apply style
let niche = tags.except_distinct("Niche", &popular);                 // fluent
let all = tags.union_all("AllTags", &popular);
```

For more than two collections, chain the operations. Both collections must have the
same windowing.

## Windowing and triggers

Window functions, `WindowInto` and triggers are in the prelude and in `beam::windowing`.

Apply style, with a trigger, accumulation and allowed lateness:

```rust
use std::time::Duration;

let windowed = events.apply("Fixed", WindowInto::new(FixedWindows::of(Duration::from_secs(60)))
        .with_trigger(
            Trigger::after_end_of_window()
                .with_early_firings(Trigger::after_processing_time(Duration::from_secs(10)))
                .with_late_firings(Trigger::after_count(1)),
        )
        .with_accumulation_mode(AccumulationMode::Accumulating)
        .with_allowed_lateness(Duration::from_secs(300)),
);
```

Fluent style (window function only):

```rust
use std::time::Duration;

let fixed = events.window_into("Fixed", FixedWindows::of(Duration::from_secs(60)));
let sliding = events.window_into(
    "Sliding",
    SlidingWindows::of(Duration::from_secs(60)).every(Duration::from_secs(10)),
);
let sessions = events.window_into("Sessions", Sessions::with_gap_duration(Duration::from_secs(300)));
```

- **Window functions**: `FixedWindows`, `SlidingWindows`, `Sessions` (merged per key at
  `GroupByKey`), `GlobalWindows` (the default).
- **Triggers**: `after_end_of_window()`, which alone takes `.with_early_firings(..)` /
  `.with_late_firings(..)` (it returns an `AfterWatermark`), `after_count(n)`, `after_processing_time(d)`,
  `repeatedly(t)`, `after_each`, `after_all`, `after_any`, `or_finally`, `always`, `never`.
- **`WindowInto` builders**: `with_trigger`, `with_accumulation_mode`
  (`beam::windowing::AccumulationMode::{Accumulating, Discarding}`),
  `with_allowed_lateness`, `with_output_time`
  (`beam::windowing::OutputTime::{EndOfWindow, EarliestInPane, LatestInPane}`),
  `with_closing_behavior`, `with_on_time_behavior`.
- **Timestamps**: assign event time with `ctx.output(v).at(ts).emit()`.

`ParDo`, `CombinePerKey`, `GroupByKey`, `Flatten` and the file sinks keep
windows and panes from end to end.

## State and timers

Implement `StatefulDoFn` on keyed `PCollection<(K, V)>` inputs. State and timer
declarations live on structs and enums derived with `#[derive(State)]` and
`#[derive(Timers)]`. State and timers are scoped to the key and window of each
element.

| Cell | Definition | Operations |
|---|---|---|
| `ValueState<V>` | `field: ValueState<V>` | `read`, `write`, `clear` |
| `BagState<V>` | `field: BagState<V>` | `read`, `append`, `clear` |
| `MapState<K, V>` | `field: MapState<K, V>` | `get`, `put`, `remove`, `keys`, `entries`, `clear` |
| `SetState<T>` | `field: SetState<T>` | `contains`, `insert`, `remove`, `read`, `clear` |
| `OrderedListState<V>` | `field: OrderedListState<V>` | `read`, `read_range`, `add`, `clear`, `clear_range` |
| `CombiningState<CF>` | `field: CombiningState<Sum<i64>>` | `add`, `read`, `add_accumulator`, `read_accumulator`, `is_empty`, `clear` |

`CombiningState<CF>` folds its inputs with the `CombineFn` `CF` and stores one
accumulator. The cell builds `CF` with `Default`, as `Sum`, `Max`, `Min` and `Mean` do.

`#[derive(Timers)]` defines timer families on an enum. Variants with `#[timer(event_time)]`
or `#[timer(processing_time)]` declare event-time or processing-time families. A tuple
variant with `(String)` accepts dynamic tags. `ctx.timer(t)` on `StatefulContext` sets or
clears a timer:

| Call | Effect |
|---|---|
| `ctx.timer(t).at(instant)?` | Fires at `instant` in the timer's time domain |
| `ctx.timer(t).after(duration)?` | Fires `duration` after the element timestamp or the firing time of an event-time timer (event time), or after the clock (processing time) |
| `ctx.timer(t).hold(instant).at(..)?` | Holds the output watermark at `instant` instead of the default |
| `ctx.timer(t).clear()?` | Clears the timer |

The default hold is the firing time for event-time timers and the element
timestamp for processing-time timers. In `on_timer`, `ctx.timestamp()` is the hold and
outputs carry it; `ctx.fire_timestamp()` is the firing time. An end-of-window timer is
`ctx.timer(t).at(ctx.window()?.max_timestamp())?`.

```rust
#[derive(State)]
struct IdleState {
    count: ValueState<i64>,
}

#[derive(Timers)]
enum IdleTimers {
    #[timer(event_time)]
    Expiry(String),
}

#[derive(Clone)]
struct IdleCounter;

impl StatefulDoFn for IdleCounter {
    type Key = String;
    type Value = String;
    type Out = (String, i64);
    type State = IdleState;
    type Timer = IdleTimers;

    fn process_element(
        &mut self,
        _key: &Self::Key,
        _event: Self::Value,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        let n = state.count.read()?.unwrap_or(0) + 1;
        state.count.write(n)?;
        ctx.timer(IdleTimers::Expiry("idle".to_string()))
            .after(Duration::from_secs(10 * 60))
    }

    fn on_timer(
        &mut self,
        timer: Self::Timer,
        key: &Self::Key,
        state: &mut Self::State,
        ctx: &mut StatefulContext<'_, Self>,
    ) -> beam::Result {
        match timer {
            IdleTimers::Expiry(tag) if tag == "idle" => {
                let n = state.count.read()?.unwrap_or(0);
                state.count.clear()?;
                ctx.emit((key.clone(), n))
            }
            _ => Ok(()),
        }
    }
}

let idle = events.apply("IdleCounter", ParDo::new(IdleCounter));
```

State reads and writes go through the Fn API State service. They are committed
when the bundle completes. Full examples:
[examples/state_conformance](../examples/state_conformance/src/lib.rs) and
[examples/gaming](../examples/gaming/src/lib.rs).

## Registered functions

A worker runs the user's `main` again and rebuilds the pipeline to find the user
code of each transform. `#[derive(Register)]` puts a `DoFn`, `StatefulDoFn`,
`SplittableDoFn` or `CombineFn` type into the pipeline graph instead: the
worker builds it from its id and its serialized fields. The fields keep the
values that the launcher gave them. The type also implements `serde::Serialize`
and `serde::Deserialize`.

```rust
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize, Register)]
struct ScaledSum {
    factor: i64,
}

impl CombineFn for ScaledSum {
    type Input = i64;
    type Accum = i64;
    type Output = i64;

    fn create_accumulator(&self) -> beam::Result<i64> { Ok(0) }
    fn add_input(&self, acc: i64, input: i64) -> beam::Result<i64> { Ok(acc + input * self.factor) }
    fn merge_accumulators(&self, accs: Vec<i64>) -> beam::Result<i64> { Ok(accs.into_iter().sum()) }
    fn extract_output(&self, acc: i64) -> beam::Result<i64> { Ok(acc) }
}

let totals = pairs.apply("Totals", CombinePerKey::new(ScaledSum { factor: 10 }));
```

- A field with `#[serde(skip)]` gets its `Default` value on the worker. Build
  connections and other process-local values in `setup`.
- The id is the module path and the type name. Set `#[beam(id = "...")]` only
  when one binary builds the graph and a different binary runs it.
- Generic types need one registration per instantiation:
  `beam::register!(Top<MyScore, Natural>)`, with the same id rule and an optional
  `id = "..."`.
- The SDK combine functions are already registered for the standard element
  types (`Sum<i64>`, `Top<String, Natural>`, `Sample<f64>` and so on). A `Top` or
  `ApproximateQuantiles` with a closure ordering, and `Compose`, use the rebuild
  path.
- If the fields do not serialize, or two types share an id, `validate` fails.
- A `CombinePerKey` takes the graph path only when the worker can find where
  each encoded key ends: keys of the standard coders (`String`, `Vec<u8>`,
  `i64`, `i32`, `f64`, `bool`, `()`, and tuples, `Vec<T>` and `Option<T>` of
  those). For other keys, it stays on the rebuild path.
- Closures and unregistered types stay on the rebuild path. While a pipeline has
  any of them, the worker still runs `main`, so pipeline construction must not
  depend on values that differ between the launcher and the workers.
  `Pipeline::rebuild_path_transforms()` lists them, and the launcher logs them at
  submission. See [static-registration.md](static-registration.md).

## Errors and dead-letter outputs

Fallible hooks return `beam::Result<T = ()>`. `beam::Error` converts from any
`std::error::Error` and from `String` / `&str`. It keeps the source chain. To add
context, call `.context("…")`. An `Err` fails the bundle, and the runner retries the bundle.

To send bad elements to a dead-letter output, use `TryMap` (fluent `.try_map`),
or a function whose `type Out` is `Result<T, F>`. Both return
[`WithFailures<U, F>`](../beam/core/src/transforms/failure.rs) with `output` and
`failures`. `failures_to(name, sink)` applies `sink` under the name `name` to the failures and returns the
output, so the chain continues. The default failure type `Failure<T>` holds the
input and the `Display` text of the error.

Apply style:

```rust
let parsed = lines.apply("Parse", TryMap::new(|s: &String| s.trim().parse::<i64>()));
parsed
    .failures // PCollection<Failure<String>>: fields `input` and `error`
    .apply("FormatDlq", Map::new(|f: Failure<String>| format!("{}: {}", f.input, f.error)))
    .apply("WriteDlq", textio::Write::new("/tmp/dlq").with_suffix(".txt"));
let doubled = parsed.output.apply("Double", Map::new(|n: i64| n * 2));

// custom failure element
let parsed = lines.apply("Parse", TryMap::new(|s: &String| s.trim().parse::<i64>())
        .exceptions_via(|e| format!("{}: {}", e.element, e.exception)),
);
```

Fluent style:

```rust
let doubled = lines
    .try_map("Parse", |s: &String| s.trim().parse::<i64>())
    // Any PTransform<PCollection<Failure<String>>> works as the sink.
    .failures_to("LogDlq", Inspect::new(|f: &Failure<String>| {
        eprintln!("bad line {:?}: {}", f.input, f.error)
    }))
    .map("Double", |n: i64| n * 2);
```

A `DoFn` that decides per element emits `Ok` or `Err`:

```rust
#[derive(Clone)]
struct ParseFn;

impl DoFn for ParseFn {
    type In = String;
    type Out = Result<i64, Failure<String>>;

    fn process_element(&mut self, line: String, ctx: &mut ProcessContext<'_, Self::Out>) -> beam::Result {
        ctx.emit(match line.trim().parse::<i64>() {
            Ok(n) => Ok(n),
            Err(e) => Err(Failure::new(line, e.to_string())),
        })
    }
}

let parsed: WithFailures<i64, Failure<String>> = lines.apply("Parse", ParDo::new(ParseFn));
```

The error type is any element type, so a connector can emit the error rows that
a Java schema transform expects. The outputs are tagged `output` and `errors`,
as in Java. An `Err` returned by the hook (not emitted) still fails the bundle:
use it for errors that are not about one element (a lost connection, a model that
did not load).

## Metrics

Create user metrics by namespace and name, anywhere in user code:

```rust
Metrics::counter("wordcount", "lines").inc();
Metrics::distribution("wordcount", "line_len").update(line.len() as i64);
Metrics::gauge("wordcount", "last_count").set(count);
```

After a run, query the metrics from the result:

```rust
let result = p.run().await?;
if let Some(metrics) = result.metrics() {
    let lines = metrics.counter("wordcount", "lines").unwrap_or(0);
}
```

The query types (`MetricResults`, `MetricFilter`, `MetricResult`) are in `beam::metrics`.
See [metrics.md](metrics.md).

## Schemas

`#[derive(BeamRow)]` maps a struct with named fields to a Beam schema and the portable
`beam:coder:row:v1` coder. `#[derive(BeamEnum)]` maps a fieldless enum to `STRING`.
Both derives are in the prelude (default `derive` feature).

```rust
use beam::prelude::*;
use chrono::{DateTime, NaiveDate, Utc};
use rust_decimal::Decimal;

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, BeamEnum)]
pub enum Tier {
    Basic,
    Premium,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct Address {
    pub street: String,
    pub city: String,
}

#[derive(Clone, Debug, PartialEq, BeamRow)]
pub struct Customer {
    pub id: i64,
    pub name: String,
    pub tier: Tier,
    pub address: Address,          // nested row
    pub balance: Decimal,          // beam:logical_type:decimal:v1
    pub signup: NaiveDate,         // beam:logical_type:date:v1
    pub last_login: DateTime<Utc>, // beam:logical_type:micros_instant:v1
    pub tags: Vec<String>,
    pub phone: Option<String>,     // nullable
}
```

- The logical-type fields need `chrono` and `rust_decimal` as direct dependencies of
  your crate. Full example: [examples/row_schemas](../examples/row_schemas/src/lib.rs).
- Field attributes: `#[beam(rename = "..")]`, `#[beam(skip)]`, `#[beam(bytes)]`,
  `#[beam(encoding_position = N)]`. Container attributes: `#[beam(id = "..")]`, and
  `#[beam(crate = "..")]` to override the path of the `beam` crate.
- A `BeamRow` type is a usual element type in all transforms.
- Conversions (trait `BeamRow`): `value.to_row()?`, `Customer::from_row(&row)?`,
  `value.to_row_bytes()?`, `Customer::from_row_bytes(&bytes)?`, `Customer::beam_schema()`.
- Dynamic rows: `Row::builder(schema)`, `row.get_string("name")?`, `row.get_i64(..)?`
  (also `get_i32`, `get_f64`, `get_bool`, `get_bytes`, `get_row`),
  `Row::from_row_bytes(&schema, &bytes)?`.

## Splittable DoFns

A splittable DoFn divides large or unbounded work across workers by restriction.
Implement [`SplittableDoFn`](../beam/core/src/transforms/dofn/sdf/splittable_dofn.rs)
with a [`RestrictionTracker`](../beam/core/src/transforms/dofn/sdf/tracker.rs),
and apply it with `SplittableParDo`. Import all of these from `beam::transforms::sdf`.

```rust
use beam::prelude::*;
use beam::transforms::sdf::{
    OffsetRange, OffsetRangeTracker, ProcessContinuation, RestrictionTracker, SplittableDoFn,
    SplittableParDo,
};

#[derive(Clone)]
struct Expand;

impl SplittableDoFn for Expand {
    type In = i64;
    type Out = i64;
    type Restriction = OffsetRange;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, n: &i64) -> beam::Result<OffsetRange> {
        Ok(OffsetRange::new(0, *n))
    }

    fn split_restriction(&self, _n: &i64, r: &OffsetRange) -> Vec<OffsetRange> {
        r.sized_splits(1_000)
    }

    fn create_tracker(&self, r: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*r)
    }

    fn process_element(
        &mut self,
        _n: i64,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, i64>,
    ) -> beam::Result<ProcessContinuation> {
        let mut i = tracker.current_restriction().start;
        while tracker.try_claim(&i) {
            ctx.emit(i)?;
            i += 1;
        }
        Ok(ProcessContinuation::stop())
    }
}

let out = sizes.apply("Expand", SplittableParDo::new(Expand));
```

- `process_element` and the lifecycle hooks (`setup`, `start_bundle`, `finish_bundle`,
  `teardown`) take `&mut self`, so the function can keep bundle state without locks.
  The lifecycle hooks have defaults.
- The restriction methods (`initial_restriction`, `split_restriction`,
  `restriction_size`, `truncate_restriction`, `create_tracker`) take `&self`. The
  dynamic splitter of the runner calls `restriction_size` on its own copy of the
  function, from a different thread, while `process_element` runs. State that both
  copies must see goes behind an `Arc`.
- An unbounded SDF returns `IsBounded::Unbounded` from `boundedness`. It yields with
  `ProcessContinuation::resume()` / `resume_after(d)`.
- `type Out` declares the outputs as for a `DoFn`, so a reader can emit
  `Result<Row, ErrorRow>` straight to an error output.
- A window-observing SDF processes the windows of an element one by one. A
  checkpoint or a dynamic split hands the rest of the current window and the
  windows not processed yet to the runner.
- Watermarks: wrap a tracker as `WatermarkedTracker::new(tracker, estimator)`.
  Use a [`WatermarkEstimator`](../beam/core/src/windowing/watermark.rs) from
  `beam::windowing` (`ManualWatermarkEstimator`,
  `TimestampObservingWatermarkEstimator`, `WallTimeWatermarkEstimator`).
- A `SplittableParDo` expands into the standard portable SDF stages, so runners
  can split dynamically. `GenerateSequence` and `textio::Read` use SDFs.

## File I/O (`beam::io`)

`textio` is in the prelude. The other file I/O items are in `beam::io`
(`beam::io::file::fileio`, `beam::io::file::WriteFiles`, …).

### Writes

Each file sink runs on [`WriteFiles`](../beam/io/file/src/write_files/mod.rs).
Elements go to temporary files with unique names. When all files are written,
the temporary files get their final names. A failed or retried bundle can leave
orphaned temporary files. It never leaves a partial or duplicate output file.

```rust
use beam::io::{FormatSink, TextFormat, WriteFiles};
use beam::prelude::*;

// Runner-chosen file count: /out/words-00000-of-0000N.txt …
lines.apply("WriteLines", textio::Write::new("/out/words").with_suffix(".txt"));

// Exactly 4 files, each rolling over every 100k records.
lines.apply("WriteLines", textio::Write::new("/out/words")
        .with_num_shards(4)
        .with_max_records_per_file(100_000),
);

// A single file named exactly /out/summary.txt.
summary.apply("WriteSummary", textio::Write::new("/out/summary.txt").without_sharding());

// Any FileSink; the output is the final file names.
let written: PCollection<String> = lines.apply("WriteFiles", WriteFiles::new("gs://bucket/out/part", FormatSink::new(TextFormat))
        .with_suffix(".txt")
        .with_num_shards(8),
);
```

| Option | Effect |
|---|---|
| *(default)* | One file per bundle; the runner's parallelism decides the count |
| `with_num_shards(n)` | Exactly `n` files per window and pane; empty shards still get a file |
| `without_sharding()` | One file, named exactly `prefix + suffix` |
| `with_max_records_per_file` / `with_max_bytes_per_file` | Roll to a new file at a limit |
| `with_shard_template("-SSSSS-of-NNNNN")` | `S` and `N` runs become the zero-padded index and count |
| `with_windowed_writes()` | Separate files per window and pane; required for unbounded input |
| `with_filename_policy(p)` | Custom names from a [`FilenamePolicy`](../beam/io/file/src/filename_policy.rs) |
| `with_temp_directory(dir)` | Where temporary files go |

A format with per-file state (Parquet row groups) implements
[`FileSink`](../beam/io/file/src/sink.rs). A stateless line format implements
`FileFormat`, and you wrap it in a `FormatSink`.

Temporary files follow these rules:

- **Unwindowed writes** finalize one time, after all input is written. Empty
  input also produces empty files. The SDK deletes the temporary directory and
  the orphaned files.
- **Windowed writes** finalize per window. The SDK removes the temporary
  directory only if it is empty. It does not delete orphaned files from failed
  bundles. On object stores, add a lifecycle rule on the `.temp-beam-*` prefix
  (or the `with_temp_directory` path). The rule must delete objects older than 1–3 days.

### Whole-file reads

`fileio::Match` and `fileio::ReadMatches` discover files by pattern and open them on workers:

```rust
use beam::io::fileio::{self, EmptyMatchTreatment, ReadableFile};

let docs = p
    .apply("MatchDocs", fileio::Match::new("gs://bucket/docs/*.json")
            .with_empty_match_treatment(EmptyMatchTreatment::Allow),
    )
    .apply("ReadMatches", fileio::ReadMatches::new())
    .par_do_fn("Parse", |f: ReadableFile, ctx| ctx.emit(parse(&f.read_fully_as_bytes()?)));
```

- `Match` emits one `FileMetadata` (path, size, last-modified) per file, and
  reshuffles them across workers. By default, an empty match fails
  (`EmptyMatchTreatment::Disallow`). `Allow` and `AllowIfWildcard` relax this rule.
- `ReadMatches` converts `FileMetadata` into `ReadableFile`
  (`open`, `open_range`, `read_fully_as_bytes`, `read_fully_as_utf8_string`).
- `FileMetadata` and `ReadableFile` are rows with a schema.

### Parquet and Avro

`beam::io::parquet::parquetio` (feature `parquet`) and `beam::io::avro::avroio`
(feature `avro`) read and write `#[derive(BeamRow)]` types or schema-aware `Row`s
through arrow-rs. Reads are splittable: by row group for Parquet, by sync marker for Avro.

```rust
use beam::io::avro::avroio;
use beam::io::parquet::parquetio;

let events: PCollection<Event> = p.apply("ReadEvents", parquetio::Read::new("/data/events/*.parquet"));
events.apply("WriteEvents", parquetio::Write::new("/out/events")
        .with_suffix(".parquet")
        .with_num_shards(4),
);

let records: PCollection<Record> = p.apply("ReadRecords", avroio::Read::new("/data/*.avro"));
records.apply("WriteRecords", avroio::Write::new("/out/records").with_suffix(".avro"));

// Schema known only at runtime: read generic Rows, and write them with their schema.
let schema = parquetio::schema_of("/data/events/part-0.parquet")?;
let rows: PCollection<Row> = p.apply("ReadRows", parquetio::ReadRows::new("/data/events/*.parquet", schema));
rows.apply("WriteRows", avroio::WriteRows::new("/out/rows"));
```

`ReadFiles<T>` / `ReadRowFiles` read the files in a `PCollection<String>` of paths.
`WriteRows` takes the schema from the row coder of its input. A transform that passes its
elements through (`Filter`, `Reshuffle`, `GroupByKey`, `WithKeys`, ...) keeps that coder.
Call `with_row_schema` on the output of a `ParDo` or `Map` that makes rows.
`ReadRows::with_encoded_rows` gives each row as its row coder bytes. Use it when the rows go
to another SDK or cross a fusion break.

## Cross-language

### Automatic expansion service (`beam::external::expansionx`)

This module resolves the Java expansion service JAR (a local Beam build, else
`~/.apache_beam/cache/jars`, else a download from Maven Central), starts it with
`java -jar <jar> <port>` and stops it on drop. It retries on a fresh port if a
port race occurs. Transforms take an expansion service *target*; an
`autojava:` target starts a service automatically:

```rust
use beam::external::expansionx;

// Returns "autojava::sdks:java:io:google-cloud-platform:expansion-service:runExpansionService".
let target = expansionx::use_automated_java_expansion_service(
    ":sdks:java:io:google-cloud-platform:expansion-service:runExpansionService",
);
```

### Serving Rust transforms (`apache-beam-expansion`)

Other SDKs can expand Rust transforms over gRPC:

- [`ExpansionServiceServer`](../beam/expansion/src/lib.rs) implements
  `discover_schema_transform` and `expand`.
- `register_schema_transform!` collects
  [`SchemaTransformProvider`](../beam/expansion/src/lib.rs) implementations across crates
  at link time.
- The `beam-expansion-service` binary ([main.rs](../beam/expansion/src/main.rs)) serves
  them over TCP (default port `8097`) or in a worker pool.

### Managed I/O (`beam::io::managed`)

`ManagedRead` and `ManagedWrite` expand Java's `beam:transform:managed:v1`
SchemaTransform through the automated expansion service. Connectors: `ICEBERG`,
`KAFKA`, `BIGQUERY`, `POSTGRES`, `MYSQL`, `SQL_SERVER` (read and write) and
`DELTA` (read). For the configuration keys, see the
[Managed I/O page](https://beam.apache.org/documentation/io/managed-io/).

```rust
use std::collections::BTreeMap;

use beam::io::managed::{self, ManagedRead, ManagedWrite};
use beam::prelude::*;

let rows: PCollection<Row> = p.apply("Managed Read(ICEBERG)", ManagedRead::new(managed::ICEBERG)
        .with_config_entry("table", "db.events")
        .with_config_entry("catalog_name", "local")
        .with_config_entry("catalog_properties", BTreeMap::from([("type", "hadoop"), ("warehouse", "gs://b/w")])),
);
rows.apply("Managed Write(POSTGRES)", ManagedWrite::new(managed::POSTGRES).with_config_url("gs://b/postgres.yaml"));
```

- **Config**: `with_config` takes any `Serialize` map (struct, `BTreeMap`,
  `serde_json::Value`). `with_config_entry` sets one key. `with_yaml_config` and
  `with_config_url` pass YAML through without change. The SDK sends structured
  config as JSON, so Java's YAML 1.1 loader never coerces strings such as
  `off` or `0123`.
- **Expansion service**: the SDK selects it per connector. Iceberg, Kafka and
  Delta use the Java I/O service. BigQuery and JDBC use the GCP service. To
  override, call `with_expansion_service`.
- **Typed connectors**: `KafkaRead` / `KafkaWrite` (`beam::io::kafka`) and the
  BigQuery transforms keep typed builders. They expand through Managed with
  `to_managed()`. `BigQueryWrite::to_managed()` returns `None` for write methods
  that have no Managed equivalent (only `Auto` and `StorageApiAtLeastOnce` map).
  `managed::row_to_config` converts a SchemaTransform config `Row` into Managed config.
- **Extra outputs and errors**: `ManagedRead::with_all_outputs()` and
  `ManagedWrite::with_outputs()` return all outputs as `ExternalOutputs`
  (`get` / `try_get` / `tags`). An Iceberg write emits `managed::SNAPSHOTS`.
  `with_error_handling("errors")` sends failed records to an `errors` output.
  `KafkaRead`, `KafkaWrite` and `BigQueryWrite` also have this method. Output
  tags are declared at construction, because workers rebuild the pipeline
  without an expansion service. Expansion fails if Java does not produce a declared tag.

```rust
let written = rows.apply("Managed Write(ICEBERG)", ManagedWrite::new(managed::ICEBERG).with_config(cfg).with_outputs());
let snapshots = written.try_get(managed::SNAPSHOTS)?;

let read = p.apply("ReadEvents", KafkaRead::new("broker:9092", "t").with_error_handling("errors").to_managed()?.with_all_outputs());
let (records, errors) = (read.try_get(managed::OUTPUT)?, read.try_get("errors")?);
```

## Resource hints

Resource hints specify accelerators, minimum RAM, CPU count or bundle
concurrency.
[`ResourceHints`](../beam/core/src/pipeline/resources.rs) and the `URN_RESOURCE_*`
constants are in `beam::pipeline`.

| Hint | URN | Merge (outer, inner) |
|---|---|---|
| `with_accelerator("type:nvidia-tesla-t4;count:1;install-nvidia-driver")` | `beam:resources:accelerator:v1` | inner wins |
| `with_min_ram_bytes(n)` / `with_min_ram("4GB")?` | `beam:resources:min_ram_bytes:v1` | max |
| `with_cpu_count(n)` | `beam:resources:cpu_count:v1` | max |
| `with_max_active_bundles_per_worker(n)` | `beam:resources:max_active_bundles_per_worker:v1` | sum |
| `with_hint(urn, payload)` | custom | inner wins |

```bash
cargo run --bin my_pipeline -- \
  --runner=dataflow \
  --resource_hints min_ram_bytes=16GB,cpu_count=4
```

```rust
use beam::ml::RunInference;
use beam::pipeline::ResourceHints;
use beam::prelude::*;
use beam::transforms::WithResourceHintsExt;

// Pipeline-wide
let p = Pipeline::create(&options).with_resource_hints(ResourceHints::new().with_cpu_count(4));

// One transform: any PTransform, through WithResourceHintsExt
let predictions = input.apply("Predict", RunInference::new(handler).with_resource_hints(
        ResourceHints::new()
            .with_accelerator("type:nvidia-tesla-t4;count:1;install-nvidia-driver")
            .with_min_ram_bytes(16 * 1024 * 1024 * 1024),
    ),
);

// Every transform added while the guard is alive
{
    let _guard = p.enter_resource_hints_scope(ResourceHints::new().with_cpu_count(8));
    let out = input.apply("StepA", StepA).apply("StepB", StepB);
}
```

## Testing (`beam::testing`)

Enable the `testing` feature for tests:

```toml
[dev-dependencies]
beam = { package = "apache-beam", path = "../path/to/beam/sdks/rust/beam", features = ["prism", "testing"] }
```

`passert` adds assertions *into* the pipeline. A failed assertion fails the job.
`TestStream` replays elements, watermark advances and processing-time advances,
so late data and trigger firings are deterministic.

```rust
use beam::prelude::*;
use beam::testing::{passert, TestPipeline, TestStream};
use std::time::Duration;

let p = TestPipeline::new();
let counts = p
    .apply("Events", TestStream::new()
            .add_timestamped_elements([("a".to_string(), 1_000), ("b".to_string(), 2_000)])
            .advance_watermark_to(15_000)
            .add_timestamped_elements([("a".to_string(), 3_000)]) // late: dropped
            .advance_watermark_to_infinity(),
    )
    .window_into("Fixed", FixedWindows::of(Duration::from_secs(10)))
    .count_per_element("Count");

passert::that("AssertCounts", &counts)
    .in_on_time_pane(IntervalWindow::new(0, 10_000))
    .contains_in_any_order([("a".to_string(), 1), ("b".to_string(), 1)]);

p.run().await?; // fails unless every assertion ran and passed
```

- **`TestPipeline`** dereferences to `Pipeline`. It reads options from
  `BEAM_TEST_PIPELINE_OPTIONS` (for example `--runner=prism`);
  `TestPipeline::with_options(options)` ignores the environment.
  `run_with(&runner)` runs on a runner instance. `run` makes sure that all
  assertions ran and passed. If you drop a `TestPipeline` that did not run, it
  panics. For graph-only tests, call `without_run_enforcement()` to disable this check.
- **Plain `Pipeline`**: check the assertions yourself with
  `passert::verify_assertions(&result, &passert::assertion_names(&p))`.
- **Assertions**: `contains_in_any_order`, `contains`, `empty`, `not_empty`,
  `has_count`, `all`, `satisfies`. For one-element collections, use
  `that_singleton(..).is_equal_to(..)`. For `GroupByKey` output (value order
  unspecified), use `that_grouped`. To check each element with its window, use
  `that_windowed(..).contains_in_any_order([(value, IntervalWindow::new(..)), …])`.
- **Windows and panes**: `in_window`, `in_on_time_pane`, `in_final_pane`,
  `in_early_panes`, `in_late_panes`.
- **`TestStream` events**: `add_elements` (at the current watermark),
  `add_timestamped_elements`, `advance_watermark_to`, `advance_processing_time`,
  `advance_watermark_to_infinity`.
- An assertion on an empty collection also runs. On unbounded input, it runs
  when the watermark closes its window, so end a `TestStream` with
  `advance_watermark_to_infinity()`.
- With `AccumulationMode::Accumulating`, a window emits one more pane with its total
  when it expires. Expect this pane in `in_window`, or check it with `in_final_pane`.
- `TestStream` (`beam:transform:teststream:v1`) runs on the Prism runner.
  Assertions run on all runners.

Worked examples:

- [wordcount_test.rs](../examples/wordcount/tests/wordcount_test.rs): a single step and a
  composite `PTransform` on in-memory input.
- [leaderboard_test.rs](../examples/leaderboard/tests/leaderboard_test.rs): early,
  on-time, late and droppably late panes with `TestStream`.

## Where advanced items live

The prelude contains the items that a usual pipeline uses. Each other item has one path:

| Module | Contents |
|---|---|
| `beam::transforms` | All core transforms, including `Outputs`, `ExplodeBatch`, `BatchedDoFn`, `Timer`, `WithResourceHintsExt` |
| `beam::transforms::failure` | `Failure`, `WithFailures`, `ExceptionElement` |
| `beam::transforms::sdf` | `SplittableDoFn`, `SplittableParDo`, `RestrictionTracker`, `OffsetRange`, `OffsetRangeTracker`, `WatermarkedTracker`, `ProcessContinuation` |
| `beam::windowing` | Window functions, `WindowInto`, triggers, `OutputTime`, watermark estimators |
| `beam::pipeline` | `ResourceHints`, `URN_RESOURCE_*` |
| `beam::metrics` | Metrics query API: `MetricResults`, `MetricFilter`, `MetricResult` |
| `beam::coders` | Coders, `WindowedHeader`, `PaneInfo` |
| `beam::options` | `parse`, `parse_from`, `try_parse_from`, option groups |
| `beam::io::*` | `beam::io::file::textio`, `beam::io::file::fileio`, `beam::io::file::WriteFiles`, `beam::io::parquet::parquetio`, `beam::io::avro::avroio`, `beam::io::kafka`, `beam::io::managed` |
| `beam::ml` | `RunInference`, `ModelHandler`, ONNX / Candle / remote handlers ([guide](accelerated-workloads.md)) |
| `beam::testing` | `passert`, `TestPipeline`, `TestStream` |
| `beam::internals` | Runner plumbing: `BundleHandler`, `ElementSink`, `HandlerContext`, `DoFnHandler`, `ParDoRegistration`, SDF handlers |
