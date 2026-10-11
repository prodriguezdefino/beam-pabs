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

//! The `PAssert` builder and the sub-graph each check adds to the pipeline.

use std::collections::HashMap;
use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use beam::coders::{
    BeamIterable, DefaultCoder, IntervalWindow, Timing, URN_KV, URN_NULLABLE, WindowedHeader,
};
use beam::internals::PipelineInternals;
use beam::metrics::{Counter, Metrics};
use beam::pipeline::Pipeline;
use beam::transforms::{Create, Flatten, GroupByKey, Map, ParDo};
use beam::values::{PCollection, PDone};
use beam::windowing::{
    AccumulationMode, ClosingBehavior, GlobalWindows, OnTimeBehavior, OutputTime, Trigger,
    WindowInto,
};

use super::mismatch::{Shown, describe_elements, multiset_difference};
use super::verify::{failure_counter_name, success_counter_name};
use super::windowed::interval_windows;
use super::{FAILURE_COUNTER, PASSERT_ANNOTATION, PASSERT_NAMESPACE, SUCCESS_COUNTER};

/// Every element is grouped under this one key.
const SINGLE_KEY: i64 = 0;

/// The coder of `(SINGLE_KEY, Option<element>)`, from the coder of the elements.
fn keyed_coder(pipeline: &Pipeline, element_coder_id: &str) -> String {
    let value = pipeline.register_coder(URN_NULLABLE, vec![element_coder_id.to_string()]);
    pipeline.register_coder(URN_KV, vec![i64::register_coder(pipeline), value])
}

/// A check over the entire, selected contents of a collection.
type Check<T> = Arc<dyn Fn(Vec<T>) -> beam::Result + Send + Sync>;

/// Which firings of a window an assertion considers.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum PaneSelector {
    /// Every element, whatever pane produced it.
    All,
    /// Elements from the firing triggered by the watermark passing the end of the window.
    OnTime,
    /// Elements from the last firing of the window.
    Final,
    /// Elements from firings before the watermark passed the end of the window.
    Early,
    /// Elements from firings after the watermark passed the end of the window.
    Late,
}

impl PaneSelector {
    fn accepts(self, header: &WindowedHeader) -> bool {
        let pane = header.pane();
        match self {
            Self::All => true,
            Self::OnTime => pane.timing() == Timing::OnTime,
            Self::Final => pane.is_last(),
            Self::Early => pane.timing() == Timing::Early,
            Self::Late => pane.timing() == Timing::Late,
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Self::All => "",
            Self::OnTime => " (on-time pane)",
            Self::Final => " (final pane)",
            Self::Early => " (early panes)",
            Self::Late => " (late panes)",
        }
    }
}

/// Starts an assertion about the contents of `pcoll`.
///
/// Nothing is added to the pipeline until a terminal method such as
/// [`contains_in_any_order`](PAssert::contains_in_any_order) is called.
///
/// # Panics
///
/// The returned builder, and every clone of it, panics when the last of them is
/// dropped without a check ever having been added: `passert::that("AssertOutput", &pc);` alone
/// asserts nothing, and would otherwise pass silently.
#[must_use = "an assertion checks nothing until a method such as `contains_in_any_order` is called"]
pub fn that<T: DefaultCoder>(name: impl Into<String>, pcoll: &PCollection<T>) -> PAssert<T> {
    let input = pcoll.clone();
    PAssert::reading(name, pcoll, move || input.clone())
}

/// An assertion about the contents of a [`PCollection`], built by [`that`].
///
/// The builder methods narrow what is checked; each terminal method adds one check to
/// the pipeline and returns the builder, so several checks can be made about the same
/// collection. The first check is a composite named after the assertion. Check `n` of the
/// same builder is named `{name}_{n}`.
pub struct PAssert<T> {
    pipeline: Pipeline,
    /// The id of the asserted PCollection, the input of each check.
    input_id: String,
    /// Returns the collection that a check reads. A check calls it in its own name scope,
    /// so a step that it adds is a child of the check.
    read: Arc<dyn Fn() -> PCollection<T>>,
    name: String,
    window: Option<IntervalWindow>,
    panes: PaneSelector,
    /// Shared by every clone, to detect a builder that never added a check.
    guard: Arc<UsageGuard>,
}

/// Not derived: the elements are not cloned, so `T` (such as a `BeamIterable`) need not be
/// `Clone`.
impl<T> Clone for PAssert<T> {
    fn clone(&self) -> Self {
        Self {
            pipeline: self.pipeline.clone(),
            input_id: self.input_id.clone(),
            read: Arc::clone(&self.read),
            name: self.name.clone(),
            window: self.window,
            panes: self.panes,
            guard: Arc::clone(&self.guard),
        }
    }
}

/// Panics, once the last clone of an assertion builder is dropped, if none of them
/// ever added a check. Counts the checks, to name each one.
struct UsageGuard {
    pcollection: String,
    checks: AtomicUsize,
}

impl Drop for UsageGuard {
    fn drop(&mut self) {
        if *self.checks.get_mut() == 0 && !std::thread::panicking() {
            panic!(
                "passert::that(..) on PCollection '{}' was dropped without a check: call a \
                 method such as `contains_in_any_order`, `has_count` or `empty`",
                self.pcollection
            );
        }
    }
}

impl<T: 'static> Debug for PAssert<T> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PAssert")
            .field("pcollection", &self.input_id)
            .field("name", &self.name)
            .field("window", &self.window)
            .field("panes", &self.panes)
            .finish()
    }
}

impl<T: DefaultCoder> PAssert<T> {
    /// Starts an assertion named `name` about `input`. Each check reads the collection that
    /// `read` returns, and `read` runs in the name scope of the check.
    pub(super) fn reading<In: 'static>(
        name: impl Into<String>,
        input: &PCollection<In>,
        read: impl Fn() -> PCollection<T> + 'static,
    ) -> Self {
        Self {
            pipeline: input.pipeline().clone(),
            input_id: input.id().to_string(),
            read: Arc::new(read),
            name: name.into(),
            window: None,
            panes: PaneSelector::All,
            guard: Arc::new(UsageGuard {
                pcollection: input.id().to_string(),
                checks: AtomicUsize::new(0),
            }),
        }
    }

    /// Considers only the elements assigned to `window`.
    #[must_use]
    pub fn in_window(mut self, window: IntervalWindow) -> Self {
        self.window = Some(window);
        self.panes = PaneSelector::All;
        self
    }

    /// Considers only the elements of `window` emitted by its on-time firing.
    #[must_use]
    pub fn in_on_time_pane(mut self, window: IntervalWindow) -> Self {
        self.window = Some(window);
        self.panes = PaneSelector::OnTime;
        self
    }

    /// Considers only the elements of `window` emitted by its final firing.
    #[must_use]
    pub fn in_final_pane(mut self, window: IntervalWindow) -> Self {
        self.window = Some(window);
        self.panes = PaneSelector::Final;
        self
    }

    /// Considers only the elements of `window` emitted by its early (speculative) firings.
    #[must_use]
    pub fn in_early_panes(mut self, window: IntervalWindow) -> Self {
        self.window = Some(window);
        self.panes = PaneSelector::Early;
        self
    }

    /// Considers only the elements of `window` emitted by its late firings.
    #[must_use]
    pub fn in_late_panes(mut self, window: IntervalWindow) -> Self {
        self.window = Some(window);
        self.panes = PaneSelector::Late;
        self
    }

    /// Asserts that the collection holds exactly `expected`, in any order.
    ///
    /// Duplicates count: `[1, 1]` does not match `[1]`.
    pub fn contains_in_any_order<I>(self, expected: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: PartialEq + Debug,
    {
        let expected: Vec<T> = expected.into_iter().collect();
        self.satisfies(move |actual| {
            let (missing, unexpected) = multiset_difference(actual, &expected);
            if missing.is_empty() && unexpected.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "expected {} in any order, but got {}{}{}",
                    Shown(&expected),
                    Shown(actual),
                    describe_elements("missing", &missing),
                    describe_elements("unexpected", &unexpected),
                )
                .into())
            }
        })
    }

    /// Asserts that the collection holds at least `expected`, in any order.
    ///
    /// Other elements may be present too. Duplicates count: expecting `[1, 1]` requires
    /// two ones.
    pub fn contains<I>(self, expected: I) -> Self
    where
        I: IntoIterator<Item = T>,
        T: PartialEq + Debug,
    {
        let expected: Vec<T> = expected.into_iter().collect();
        self.satisfies(move |actual| {
            let (missing, _) = multiset_difference(actual, &expected);
            if missing.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "expected to contain {}, but got {}{}",
                    Shown(&expected),
                    Shown(actual),
                    describe_elements("missing", &missing),
                )
                .into())
            }
        })
    }

    /// Asserts that the collection holds no elements.
    pub fn empty(self) -> Self
    where
        T: Debug,
    {
        self.satisfies(|actual| {
            if actual.is_empty() {
                Ok(())
            } else {
                Err(format!("expected no elements, but got {}", Shown(actual)).into())
            }
        })
    }

    /// Asserts that the collection holds at least one element.
    pub fn not_empty(self) -> Self {
        self.satisfies(|actual| {
            if actual.is_empty() {
                Err("expected at least one element, but got none".into())
            } else {
                Ok(())
            }
        })
    }

    /// Asserts that the collection holds exactly `count` elements.
    pub fn has_count(self, count: usize) -> Self
    where
        T: Debug,
    {
        self.satisfies(move |actual| {
            if actual.len() == count {
                Ok(())
            } else {
                Err(format!(
                    "expected {count} element(s), but got {}: {}",
                    actual.len(),
                    Shown(actual)
                )
                .into())
            }
        })
    }

    /// Asserts that every element satisfies `predicate`, described by `description`.
    ///
    /// Passes on an empty collection. Pair it with [`has_count`](Self::has_count) or
    /// [`not_empty`](Self::not_empty) when elements are expected.
    pub fn all<F>(self, description: impl Into<String>, predicate: F) -> Self
    where
        F: Fn(&T) -> bool + Send + Sync + 'static,
        T: Debug,
    {
        let description = description.into();
        self.satisfies(move |actual| {
            let failing: Vec<&T> = actual.iter().filter(|e| !predicate(e)).collect();
            if failing.is_empty() {
                Ok(())
            } else {
                Err(format!(
                    "expected every element to be {description}, but {} of {} were not: {}",
                    failing.len(),
                    actual.len(),
                    Shown(&failing)
                )
                .into())
            }
        })
    }

    /// Asserts that `check` accepts the whole collection.
    ///
    /// `check` receives every selected element at once, in no particular order, and
    /// returns an explanation when the collection is not what was expected. It runs,
    /// and so can fail, even when the collection is empty.
    pub fn satisfies<F>(self, check: F) -> Self
    where
        F: Fn(&[T]) -> beam::Result + Send + Sync + 'static,
    {
        self.apply_owned_check(move |elements| check(&elements))
    }

    /// Like [`satisfies`](Self::satisfies), with `check` taking ownership of the elements.
    pub(super) fn apply_owned_check<F>(self, check: F) -> Self
    where
        F: Fn(Vec<T>) -> beam::Result + Send + Sync + 'static,
    {
        self.apply_check(Arc::new(check));
        self
    }

    /// Builds the assertion's sub-graph around `check`, in a composite named after the
    /// assertion. The composite reads the asserted collection and has no output.
    fn apply_check(&self, check: Check<T>) {
        let number = self.guard.checks.fetch_add(1, Ordering::Relaxed) + 1;
        let pipeline = &self.pipeline;
        let name = match number {
            1 => self.name.clone(),
            n => format!("{}_{n}", self.name),
        };
        let inputs = HashMap::from([("in".to_string(), self.input_id.clone())]);
        pipeline.apply_scoped(&name, inputs, || {
            let name = pipeline.scope_name().unwrap_or_default();
            self.build_check(&name, check);
            // The assertion's name, which its per-assertion counters are named after.
            pipeline.set_scope_annotation(PASSERT_ANNOTATION, name.into_bytes());
            PDone::new(pipeline.clone())
        });
    }

    /// Adds the steps of one check. `name` is the full name of the check.
    fn build_check(&self, name: &str, check: Check<T>) {
        let selected = self.select_window_and_panes(name, (self.read)());

        // Every field is set, so the strategy is the default one of the sentinel below, and
        // the grouping fires once, at the end, whatever the upstream trigger was.
        let rewindowed = selected.apply(
            "RewindowGlobally",
            WindowInto::new(GlobalWindows)
                .with_trigger(Trigger::Default)
                .with_accumulation_mode(AccumulationMode::Discarding)
                .with_allowed_lateness(Duration::ZERO)
                .with_output_time(OutputTime::EndOfWindow)
                .with_closing_behavior(ClosingBehavior::EmitIfNonempty)
                .with_on_time_behavior(OnTimeBehavior::FireAlways),
        );
        // The element coder comes from the input: the coder of `T` has no schema for a row.
        let keyed = rewindowed
            .apply("Key", Map::new(|element: T| (SINGLE_KEY, Some(element))))
            .with_coder_derived_from(&rewindowed, keyed_coder);

        // The sentinel makes the grouping emit, so an empty collection is checked.
        let sentinel = self
            .pipeline
            .apply("Sentinel", Create::new([(SINGLE_KEY, None::<T>)]))
            .with_coder_derived_from(&rewindowed, keyed_coder);

        let description = format!("{name}{}", self.describe_selection());
        let success_counters = [
            Metrics::counter(PASSERT_NAMESPACE, SUCCESS_COUNTER),
            Metrics::counter(PASSERT_NAMESPACE, success_counter_name(name)),
        ];
        let failure_counters = [
            Metrics::counter(PASSERT_NAMESPACE, FAILURE_COUNTER),
            Metrics::counter(PASSERT_NAMESPACE, failure_counter_name(name)),
        ];
        Flatten::pcollections("Flatten", &[&keyed, &sentinel])
            .apply("GroupGlobally", GroupByKey::new())
            .apply(
                "Check",
                ParDo::from_fn(
                    move |(_, grouped): (i64, BeamIterable<Option<T>>),
                          _ctx: &mut beam::transforms::ProcessContext<'_, ()>| {
                        let elements: Vec<T> = grouped
                            .into_vec()
                            .map_err(|e| {
                                beam::Error::from(e)
                                    .context(format!("{description}: failed to read elements"))
                            })?
                            .into_iter()
                            .flatten()
                            .collect();
                        match check(elements) {
                            Ok(()) => {
                                success_counters.iter().for_each(Counter::inc);
                                Ok(())
                            }
                            Err(reason) => {
                                failure_counters.iter().for_each(Counter::inc);
                                Err(format!("PAssert '{description}' failed: {reason}").into())
                            }
                        }
                    },
                ),
            );
    }

    /// Filters `input` down to the requested window and panes, if any were requested.
    fn select_window_and_panes(&self, name: &str, input: PCollection<T>) -> PCollection<T> {
        let Some(window) = self.window else {
            return input;
        };
        let name = name.to_string();
        let panes = self.panes;
        input.apply(
            "SelectWindow",
            ParDo::from_fn(move |element: T, ctx| {
                let header = beam::internals::element_header(ctx);
                // Decode windows before reading the pane, so an unreadable or global
                // window fails each element instead of filtering it out.
                let windows = interval_windows(header).map_err(|e| {
                    beam::Error::from(e).context(format!("PAssert '{name}' cannot select a window"))
                })?;
                if panes.accepts(header) && windows.contains(&window) {
                    ctx.emit(element)
                } else {
                    Ok(())
                }
            }),
        )
    }

    fn describe_selection(&self) -> String {
        self.window.map_or_else(String::new, |w| {
            format!(" in window {}{}", w, self.panes.describe())
        })
    }
}
