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

Each transform is a struct. You build it with `new(name, required…)` and
configure it with `.with_*()` builders. You can apply it in two equivalent ways:

- **apply style**: `pcoll.apply(Map::new("Name", f))`. This works for each
  transform, including your own `PTransform`s. It returns the output, so you can chain calls.
- **fluent style**: `pcoll.map("Name", f)`. Each method is the snake_case name of
  one core transform and covers the common case. For other cases (side inputs on
  several views, state, timers, multi-output), use `.apply(...)`.

```rust
use beam::prelude::*;
```

`beam::prelude` brings in both styles: the core transforms and traits, plus the
fluent extension traits (`PCollectionExt`, `PCollectionKeyedExt`,
`PCollectionListExt`) and `textio`. You can mix the two styles in one chain.

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
    p.apply(Create::new("Lines", vec!["a b".to_string(), "c".to_string()]))
        .apply(textio::Write::new("Write", &args.output));
    p.run().await?;
    Ok(())
}
```

Full example: [examples/wordcount](../examples/wordcount/src/lib.rs) (`Args`, `build_pipeline`).
Your crate needs `clap`, `serde` and `tokio` as dependencies for this pattern.

- `--runner` selects the runner (`prism` by default, `dataflow`, …).
- `p.run()` validates the graph, submits it and waits as the options specify.
- `p.run_with_runner(&runner)` runs on a runner instance. Use it for tests and custom runners.
- `PipelineOptions::from_args()` parses only the standard flags;
  `PipelineOptions::with_runner("prism")` builds options in code.

## Element-wise transforms

`Map`, `FlatMap`, `Filter` and `Inspect` take plain closures (`Filter` and
`Inspect` get `&T`). The sources in the prelude are `Create::new(name, elements)`,
`GenerateSequence::new(name, start)` (`.with_end(n)` for a bounded sequence,
`.with_rate(..)` / `.with_period(..)` for a throttled unbounded one) and
`PeriodicImpulse::new(name, interval)`.

Apply style:

```rust
let lines = p.apply(Create::new("Lines", vec!["to be or".to_string(), "not".to_string()]));
let words = lines.apply(FlatMap::new("Split", |l: String| {
    l.split_whitespace().map(String::from).collect::<Vec<_>>()
}));
let long = words.apply(Filter::new("Long", |w: &String| w.len() > 2));
let upper = long.apply(Map::new("Upper", |w: String| w.to_uppercase()));
```

Fluent style:

```rust
let upper = p
    .apply(Create::new("Lines", vec!["to be or".to_string(), "not".to_string()]))
    .flat_map("Split", |l: String| l.split_whitespace().map(String::from).collect::<Vec<_>>())
    .filter("Long", |w: &String| w.len() > 2)
    .map("Upper", |w: String| w.to_uppercase());
```

| Core transform | Fluent method |
|---|---|
| `Map::new(name, f)` | `.map(name, f)` |
| `FlatMap::new(name, f)` | `.flat_map(name, f)` |
| `Filter::new(name, pred)` | `.filter(name, pred)` |
| `Inspect::new(name, f)` | `.inspect(name, f)` |
| `ParDo::new(name, do_fn)` | `.par_do(name, do_fn)` |
| `ParDo::from_fn(name, f)` | `.par_do_fn(name, f)` |
| `ParDoMulti::new(name, tags, do_fn)` | `.par_do_multi_tags(name, tags, do_fn)` / `.par_do_multi(name, n, do_fn)` |
| `TryMap::new(name, f)` | `.try_map(name, f)` |
| `Map` producing `(key_fn(&x), x)` | `.key_by(name, key_fn)` |
| `CountPerElement::new(name)` | `.count_per_element(name)` |
| `CountGlobally::new(name)` | `.count_globally(name)` |
| `CombineGlobally::new(name, fn)` | `.combine_globally(name, fn)` |
| `Reshuffle::new(name)` | `.reshuffle(name)` |
| `Partition::new(name, n, f)` | `.partition(name, n, f)` |
| `Flatten::new(name)` on a `PCollectionList` | `.flatten(name, &[&other])`, or `list.flatten(name)` |
| `WindowInto::new(name, window_fn)` | `.window_into(name, window_fn)` |
| `BatchElements::new(name, min, max)` | `.batch_elements(name, max)` (min 1) |
| `BatchElements` + a [`BatchedDoFn`](accelerated-workloads.md#batcheddofn) | `.par_do_batch(name, max, do_fn)` / `.par_do_batch_elementwise(name, max, do_fn)` |
| `GroupByKey::new(name)` | `.group_by_key(name)` |
| `CombinePerKey::new(name, fn)` | `.combine_per_key(name, fn)` |
| `CoGroupByKey::new(name)` | `.co_group_by_key(name, &other)` |
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

let words = lines.apply(ParDo::new("SplitWords", SplitWords { min_len: 3 })); // apply
let words = lines.par_do("SplitWords", SplitWords { min_len: 3 });            // fluent
```

A closure that needs the context (timestamps, windows, several outputs per element):

```rust
let words = lines.apply(ParDo::from_fn("Split", |line: String, ctx| {
    line.split_whitespace().try_for_each(|w| ctx.emit(w.to_string()))
}));

let words = lines.par_do_fn("Split", |line: String, ctx| {
    line.split_whitespace().try_for_each(|w| ctx.emit(w.to_string()))
});
```

`map`, `flat_map` and `filter` stay pure functions of the element. If the
closure needs `ctx`, use `ParDo::from_fn` / `.par_do_fn`. If it keeps state across
elements or bundles, use a `DoFn` struct.

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

    fn finish_bundle(&mut self, _ctx: &mut ProcessContext<'_, String>) -> beam::Result {
        write_batch(&self.endpoint, &std::mem::take(&mut self.buffer))
    }
}
```

`finish_bundle` has no current element. To emit from it, record
`ctx.header().clone()` (a `beam::coders::WindowedHeader`) during processing.
Then emit with `ctx.output(v).windowed(&header).emit()`, so that the output keeps its window.

To run code after the runner commits a bundle's output (for example, to
acknowledge messages), call `ctx.register_finalizer(|| { …; Ok(()) })` and
return `true` from `DoFn::requests_finalization`. Otherwise the callbacks are
dropped. `populate_display_data` adds display data for runner UIs.

### Emitting output

| Call | Effect |
|---|---|
| `ctx.emit(v)` | Main output, inheriting timestamp, windows and pane |
| `ctx.emit_all(iter)` | `emit` for each item |
| `ctx.output(v).at(ts).emit()` | Main output with event timestamp `ts` (ms) |
| `ctx.output(v).to(tag).emit()` | Tagged output, same type as `Out`; `tag` is a `&str`, `String` or index |
| `ctx.output(v).to(tag).at(ts).emit()` | Both |
| `ctx.output_to(tag, u).emit()` | Tagged output whose element type `U` differs from `Out` |
| `ctx.emit_failure(input, error)` | Failure output of a `TryParDo` |

The builder also offers `.windowed(&header)`, `.with_pane(..)`, `.with_metadata(..)`
and, for single metadata fields, `.with_drain(..)`, `.with_value_kind(..)` and
`.with_trace(traceparent, tracestate)`.
Per-element facts: `ctx.timestamp()`, `ctx.window()`, `ctx.interval_window()`,
`ctx.pane()`, `ctx.metadata()`, `ctx.is_draining()`, `ctx.current_key::<K>()`.

A multi-output ParDo declares its tags at construction and returns a `PCollectionList`:

```rust
use beam::transforms::ParDoMulti;

let by_size = words.apply(ParDoMulti::new("BySize", ["short", "long"], ClassifyFn)); // apply
let by_size = words.par_do_multi_tags("BySize", ["short", "long"], ClassifyFn);        // fluent
let long = by_size.get(1).unwrap();

// inside ClassifyFn::process_element:
// ctx.output(word).to(if word.len() > 5 { "long" } else { "short" }).emit()
```

## Side inputs

To make a [`PCollectionView`](../beam/core/src/values/view.rs) from a
`PCollection`, call `as_singleton()`, `as_iter()` or `as_multimap()`. To read
the view, call `ctx.side_input`, `ctx.side_input_iter` or `ctx.side_input_map`.

For one side input, the fluent helpers take the side `PCollection` directly and
build the view. They are the only exception to the naming rule: each helper is a
`ParDo` with one side input.

```rust
let factor = p.apply(Create::new("Factor", vec![3_i64]));
let stopwords = p.apply(Create::new("Stopwords", vec!["the".to_string()]));
let roles = p.apply(Create::new("Roles", vec![("lear".to_string(), "King".to_string())]));

let scaled = amounts.with_side_singleton("Scale", &factor, |x: i64, f: i64| x * f);
let flagged = words.with_side_iter("Flag", &stopwords, |w: String, sw: Vec<String>| {
    let stop = sw.contains(&w);
    (w, stop)
});
let with_roles = names.with_side_map("Roles", &roles, |name: String, lookup| {
    let found = lookup(&name)?;
    Ok((name, found))
});
```

For several views, or for a side input on a `DoFn`, attach the views with
`ParDo::new(..).with_side_input(&view)`:

```rust
let min_len = p.apply(Create::new("MinLen", vec![6_i64])).as_singleton();
let stop = p.apply(Create::new("Stop", vec!["should".to_string()])).as_iter();
let (min_ref, stop_ref) = (min_len.clone(), stop.clone());

let kept = words.apply(
    ParDo::from_fn("Keep", move |w: String, ctx| {
        let min = ctx.side_input(&min_ref)? as usize;
        let stop = ctx.side_input_iter(&stop_ref)?;
        if w.len() >= min && !stop.contains(&w) { ctx.emit(w) } else { Ok(()) }
    })
    .with_side_input(&min_len)
    .with_side_input(&stop),
);
```

Full example: [examples/side_inputs](../examples/side_inputs/src/main.rs).

## Grouping, combining and joins

Apply style:

```rust
let grouped = scores.apply(GroupByKey::new("Group"));          // (String, BeamIterable<i64>)
let totals = scores.apply(CombinePerKey::new("SumPerTeam", Sum)); // (String, i64)
let total = amounts.apply(CombineGlobally::new("Total", Sum));    // i64
let counts = words.apply(CountPerElement::new("Count"));          // (String, i64)
```

Fluent style:

```rust
let grouped = scores.group_by_key("Group");
let totals = scores.combine_per_key("SumPerTeam", Sum);
let total = amounts.combine_globally("Total", Sum);
let counts = words.count_per_element("Count");
```

To read a `BeamIterable<V>`, call `values.try_into_iter()`. It yields
`Result<V>`. `Sum`, `Min` and `Max` combine `i64`. For a custom aggregation,
implement [`CombineFn`](../beam/core/src/transforms/combine.rs):

```rust
struct Mean;

impl CombineFn for Mean {
    type Input = i64;
    type Accum = (i64, i64);
    type Output = f64;

    fn create_accumulator(&self) -> (i64, i64) { (0, 0) }
    fn add_input(&self, (s, n): (i64, i64), x: i64) -> (i64, i64) { (s + x, n + 1) }
    fn merge_accumulators(&self, accs: Vec<(i64, i64)>) -> (i64, i64) {
        accs.into_iter().fold((0, 0), |(s, n), (s2, n2)| (s + s2, n + n2))
    }
    fn extract_output(&self, (s, n): (i64, i64)) -> f64 {
        if n == 0 { 0.0 } else { s as f64 / n as f64 }
    }
}
```

`CombinePerKey` expands to partial combine → `GroupByKey` → merge, so only
one accumulator per key per bundle crosses the shuffle. On windowed input, use
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
    .apply(CoGroupByKey::new("CoGroup"));             // (String, CoGbkResult)
// per group: result.get::<String>("emails")? (BeamIterable) or result.get_vec::<String>("emails")?

let joined = emails.apply(InnerJoin::new("Join", &phones));          // (K, (V, V2))
let left = emails.apply(LeftJoin::new("Left", &phones));             // (K, (V, Option<V2>))
let bcast = orders.apply(BroadcastLeftJoin::new("Enrich", &users));  // no shuffle of `orders`
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

## Windowing and triggers

Window functions, `WindowInto` and triggers are in the prelude and in `beam::windowing`.

Apply style, with a trigger, accumulation and allowed lateness:

```rust
use std::time::Duration;

let windowed = events.apply(
    WindowInto::new("Fixed", FixedWindows::of(Duration::from_secs(60)))
        .triggering(
            Trigger::after_end_of_window()
                .with_early_firings(Trigger::after_processing_time(Duration::from_secs(10)))
                .with_late_firings(Trigger::after_count(1)),
        )
        .accumulating_fired_panes()
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
- **Triggers**: `after_end_of_window()` with `.with_early_firings(..)` /
  `.with_late_firings(..)`, `after_count(n)`, `after_processing_time(d)`,
  `repeatedly(t)`, `after_each`, `after_all`, `after_any`, `or_finally`, `always`, `never`.
- **`WindowInto` builders**: `triggering`, `accumulating_fired_panes` /
  `discarding_fired_panes`, `with_allowed_lateness`, `with_output_time`
  (`beam::windowing::OutputTime::{EndOfWindow, EarliestInPane, LatestInPane}`),
  `with_closing_behavior`, `with_on_time_behavior`.
- **Timestamps**: assign event time with `ctx.output(v).at(ts).emit()`.

`ParDo`, `CombinePerKey`, `GroupByKey`, `Flatten` and the file sinks keep
windows and panes from end to end.

## State and timers

Declare state and timer specs as fields of the `DoFn`. Also register them on
the `ParDo` with `.with_state_spec(&spec)` / `.with_timer_family(&spec)`. State is
scoped to a key and a window. Pass the key explicitly.

| Spec | Cell | Operations |
|---|---|---|
| `ValueStateSpec<V>` | `ctx.value_state(&spec, &key)?` | `read`, `write`, `clear` |
| `BagStateSpec<V>` | `ctx.bag_state(&spec, &key)?` | `read`, `append`, `clear` |
| `MapStateSpec<K, V>` | `ctx.map_state(&spec, &key)?` | `get`, `put`, `remove`, `keys`, `entries`, `clear` |
| `SetStateSpec<T>` | `ctx.set_state(&spec, &key)?` | `contains`, `insert`, `remove`, `read`, `clear` |

`ctx.timer(&spec)?` returns a [`Timer`](../beam/core/src/transforms/dofn/timer.rs)
for the current key and window. `.tag(t)` selects a dynamic tag. `.key(&k)?`
selects an explicit key. Then call `.set(ts)`, `.set_with_hold(fire, hold)`,
`.set_relative(d)` or `.clear()`.

```rust
#[derive(Clone)]
struct IdleCounter {
    count: ValueStateSpec<i64>,
    expiry: TimerFamilySpec,
}

impl DoFn for IdleCounter {
    type In = (String, String);
    type Out = (String, i64);

    fn process_element(&mut self, (user, _event): Self::In, ctx: &mut ProcessContext<'_, Self::Out>) -> beam::Result {
        let mut count = ctx.value_state(&self.count, &user)?;
        count.write(count.read()?.unwrap_or(0) + 1)?;
        ctx.timer(&self.expiry)?.tag("idle").set(ctx.timestamp() + 10 * 60 * 1000);
        Ok(())
    }

    fn on_timer(
        &mut self,
        _timer_family: &str,
        _tag: &str,
        _timestamp: i64,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        let user: String = ctx.current_key()?;
        let mut count = ctx.value_state(&self.count, &user)?;
        let n = count.read()?.unwrap_or(0);
        count.clear()?;
        ctx.emit((user, n))
    }
}

let count = ValueStateSpec::<i64>::new("count");
let expiry = TimerFamilySpec::event_time("expiry");
let idle = events.apply(
    ParDo::new("IdleCounter", IdleCounter { count: count.clone(), expiry: expiry.clone() })
        .with_state_spec(&count)
        .with_timer_family(&expiry),
);
```

`TimerFamilySpec::processing_time(name)` declares a processing-time family.
State reads and writes go through the Fn API State service. They are committed
when the bundle completes. Full examples:
[examples/state_conformance](../examples/state_conformance/src/lib.rs) and
[examples/gaming](../examples/gaming/src/lib.rs).

## Errors and dead-letter outputs

Fallible hooks return `beam::Result<T = ()>`. `beam::Error` converts from any
`std::error::Error` and from `String` / `&str`. It keeps the source chain. To add
context, call `.context("…")`. An `Err` fails the bundle, and the runner retries the bundle.

To send bad elements to a dead-letter output, use `TryMap` (fluent `.try_map`)
or `TryParDo`. Both return
[`WithFailures<U, F>`](../beam/core/src/transforms/failure.rs) with `output` and
`failures`. `failures_to(sink)` applies `sink` to the failures and returns the
output, so the chain continues. The default failure type `Failure<T>` holds the
input and the `Display` text of the error.

Apply style:

```rust
let parsed = lines.apply(TryMap::new("Parse", |s: &String| s.trim().parse::<i64>()));
parsed
    .failures // PCollection<Failure<String>>: fields `input` and `error`
    .apply(Map::new("FormatDlq", |f: Failure<String>| format!("{}: {}", f.input, f.error)))
    .apply(textio::Write::new("WriteDlq", "/tmp/dlq").with_suffix(".txt"));
let doubled = parsed.output.apply(Map::new("Double", |n: i64| n * 2));

// custom failure element
let parsed = lines.apply(
    TryMap::new("Parse", |s: &String| s.trim().parse::<i64>())
        .exceptions_via(|e| format!("{}: {}", e.element, e.exception)),
);
```

Fluent style:

```rust
let doubled = lines
    .try_map("Parse", |s: &String| s.trim().parse::<i64>())
    // Any PTransform<PCollection<Failure<String>>> works as the sink.
    .failures_to(Inspect::new("LogDlq", |f: &Failure<String>| {
        eprintln!("bad line {:?}: {}", f.input, f.error)
    }))
    .map("Double", |n: i64| n * 2);
```

`TryParDo`, a `DoFn` that decides per element:

```rust
use beam::transforms::failure::OUTPUT_TAG;

#[derive(Clone)]
struct ParseFn;

impl DoFn for ParseFn {
    type In = String;
    type Out = i64;

    fn process_element(&mut self, line: String, ctx: &mut ProcessContext<'_, i64>) -> beam::Result {
        match line.trim().parse::<i64>() {
            Ok(n) => ctx.output(n).to(OUTPUT_TAG).emit(),
            Err(e) => ctx.emit_failure(line, e.to_string()),
        }
    }
}

let parsed: WithFailures<i64, Failure<String>> = lines.apply(TryParDo::new("Parse", ParseFn));
```

In a `TryParDo`, an `Err` return value also fails the bundle. Use it for errors
that are not related to one element (a lost connection, a model that did not load).

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
    type Position = i64;
    type Tracker = OffsetRangeTracker;

    fn initial_restriction(&self, n: &i64) -> OffsetRange {
        OffsetRange::new(0, *n)
    }

    fn split_restriction(&self, _n: &i64, r: &OffsetRange) -> Vec<OffsetRange> {
        r.sized_splits(1_000)
    }

    fn create_tracker(&self, r: &OffsetRange) -> OffsetRangeTracker {
        OffsetRangeTracker::new(*r)
    }

    fn process_element(
        &self,
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

let out = sizes.apply(SplittableParDo::new("Expand", Expand));
```

- `SplittableDoFn` methods take `&self`. The dynamic splitter of the runner calls
  `restriction_size` from a different thread while `process_element` runs. The
  lifecycle hooks (`setup`, `start_bundle`, `finish_bundle`, `teardown`) have defaults.
- An unbounded SDF returns `false` from `is_bounded`. It yields with
  `ProcessContinuation::resume()` / `resume_after(d)`.
- Watermarks: wrap a tracker as `WatermarkedTracker::new(tracker, estimator)`.
  Use a [`WatermarkEstimator`](../beam/core/src/windowing/watermark.rs) from
  `beam::windowing` (`ManualWatermarkEstimator`,
  `TimestampObservingWatermarkEstimator`, `WallTimeWatermarkEstimator`).
- A `SplittableParDo` expands into the standard portable SDF stages, so runners
  can split dynamically. `GenerateSequence` and `textio::Read` use SDFs.

## File I/O (`beam::io`)

`textio` is in the prelude. The other file I/O items are in `beam::io`
(`beam::io::fileio`, `beam::io::WriteFiles`, …).

### Writes

Each file sink runs on [`WriteFiles`](../beam/io/file/src/write_files/mod.rs).
Elements go to temporary files with unique names. When all files are written,
the temporary files get their final names. A failed or retried bundle can leave
orphaned temporary files. It never leaves a partial or duplicate output file.

```rust
use beam::io::{FormatSink, TextFormat, WriteFiles};
use beam::prelude::*;

// Runner-chosen file count: /out/words-00000-of-0000N.txt …
lines.apply(textio::Write::new("WriteLines", "/out/words").with_suffix(".txt"));

// Exactly 4 files, each rolling over every 100k records.
lines.apply(
    textio::Write::new("WriteLines", "/out/words")
        .with_num_shards(4)
        .with_max_records_per_file(100_000),
);

// A single file named exactly /out/summary.txt.
summary.apply(textio::Write::new("WriteSummary", "/out/summary.txt").without_sharding());

// Any FileSink; the output is the final file names.
let written: PCollection<String> = lines.apply(
    WriteFiles::new("WriteFiles", "gs://bucket/out/part", FormatSink::new(TextFormat))
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
    .apply(
        fileio::Match::new("MatchDocs", "gs://bucket/docs/*.json")
            .with_empty_match_treatment(EmptyMatchTreatment::Allow),
    )
    .apply(fileio::ReadMatches::new("ReadMatches"))
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

let events: PCollection<Event> = p.apply(parquetio::Read::new("ReadEvents", "/data/events/*.parquet"));
events.apply(
    parquetio::Write::new("WriteEvents", "/out/events", parquetio::ParquetSink::new())
        .with_suffix(".parquet")
        .with_num_shards(4),
);

let records: PCollection<Record> = p.apply(avroio::Read::new("ReadRecords", "/data/*.avro"));
records.apply(avroio::Write::new("WriteRecords", "/out/records", avroio::AvroSink::new()).with_suffix(".avro"));

// Schema known only at runtime: read generic Rows.
let schema = parquetio::schema_of("/data/events/part-0.parquet")?;
let rows: PCollection<Row> = p.apply(parquetio::ReadRows::new("ReadRows", "/data/events/*.parquet", schema));
```

`ReadFiles<T>` / `ReadRowFiles` read the files in a `PCollection<String>` of paths.

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

let rows: PCollection<Row> = p.apply(
    ManagedRead::new("Managed Read(ICEBERG)", managed::ICEBERG)
        .with_config_entry("table", "db.events")
        .with_config_entry("catalog_name", "local")
        .with_config_entry("catalog_properties", BTreeMap::from([("type", "hadoop"), ("warehouse", "gs://b/w")])),
);
rows.apply(ManagedWrite::new("Managed Write(POSTGRES)", managed::POSTGRES).with_config_url("gs://b/postgres.yaml"));
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
  (`get` / `expect` / `tags`). An Iceberg write emits `managed::SNAPSHOTS`.
  `with_error_handling("errors")` sends failed records to an `errors` output.
  `KafkaRead`, `KafkaWrite` and `BigQueryWrite` also have this method. Output
  tags are declared at construction, because workers rebuild the pipeline
  without an expansion service. Expansion fails if Java does not produce a declared tag.

```rust
let written = rows.apply(ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG).with_config(cfg).with_outputs());
let snapshots = written.expect(managed::SNAPSHOTS)?;

let read = p.apply(KafkaRead::new("ReadEvents", "broker:9092", "t").with_error_handling("errors").to_managed()?.with_all_outputs());
let (records, errors) = (read.expect(managed::OUTPUT)?, read.expect("errors")?);
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
let predictions = input.apply(
    RunInference::new("Predict", handler).with_resource_hints(
        ResourceHints::new()
            .with_accelerator("type:nvidia-tesla-t4;count:1;install-nvidia-driver")
            .with_min_ram_bytes(16 * 1024 * 1024 * 1024),
    ),
);

// Every transform added while the guard is alive
{
    let _guard = p.enter_resource_hints_scope(ResourceHints::new().with_cpu_count(8));
    let out = input.apply(StepA).apply(StepB);
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
    .apply(
        TestStream::new("Events")
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
- With `accumulating_fired_panes()`, a window emits one more pane with its total
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
| `beam::transforms` | All core transforms, including `ParDoMulti`, `ExplodeBatch`, `BatchedDoFn`, `OutputTag`, `WithResourceHintsExt` |
| `beam::transforms::failure` | `OUTPUT_TAG`, `FAILURES_TAG`, `ExceptionElement` |
| `beam::transforms::sdf` | `SplittableDoFn`, `SplittableParDo`, `RestrictionTracker`, `OffsetRange`, `OffsetRangeTracker`, `WatermarkedTracker`, `ProcessContinuation` |
| `beam::windowing` | Window functions, `WindowInto`, triggers, `OutputTime`, watermark estimators |
| `beam::pipeline` | `ResourceHints`, `URN_RESOURCE_*` |
| `beam::metrics` | Metrics query API: `MetricResults`, `MetricFilter`, `MetricResult` |
| `beam::coders` | Coders, `WindowedHeader`, `PaneInfo` |
| `beam::options` | `parse`, `parse_from`, `try_parse_from`, option groups |
| `beam::io::*` | `beam::io::textio`, `beam::io::fileio`, `beam::io::WriteFiles`, `beam::io::parquet::parquetio`, `beam::io::avro::avroio`, `beam::io::kafka`, `beam::io::managed` |
| `beam::ml` | `RunInference`, `ModelHandler`, ONNX / Candle / remote handlers ([guide](accelerated-workloads.md)) |
| `beam::testing` | `passert`, `TestPipeline`, `TestStream` |
| `beam::internals` | Runner plumbing: `BundleHandler`, `ElementSink`, `HandlerContext`, `DoFnHandler`, `ParDoRegistration`, SDF handlers |
