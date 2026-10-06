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

use std::fmt::{self, Debug};
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use beam::coders::{BeamIterable, DefaultCoder, IntervalWindow, Timing, WindowedHeader};
use beam::metrics::Metrics;
use beam::transforms::{Create, Flatten, GroupByKey, Map, ParDo};
use beam::values::PCollection;
use beam::windowing::{GlobalWindows, WindowInto};

use super::mismatch::{Shown, describe_elements, multiset_difference};
use super::verify::{failure_counter_name, success_counter_name};
use super::windowed::interval_windows;
use super::{FAILURE_COUNTER, PASSERT_ANNOTATION, PASSERT_NAMESPACE, SUCCESS_COUNTER};

/// Every element is grouped under this one key.
const SINGLE_KEY: i64 = 0;

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
            Self::OnTime => pane.timing == Timing::OnTime,
            Self::Final => pane.is_last,
            Self::Early => pane.timing == Timing::Early,
            Self::Late => pane.timing == Timing::Late,
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
    PAssert {
        pcoll: pcoll.clone(),
        name: name.into(),
        window: None,
        panes: PaneSelector::All,
        guard: Arc::new(UsageGuard {
            pcollection: pcoll.id().to_string(),
            checked: AtomicBool::new(false),
        }),
    }
}

/// An assertion about the contents of a [`PCollection`], built by [`that`].
///
/// The builder methods narrow what is checked; each terminal method adds one check to
/// the pipeline and returns the builder, so several checks can be made about the same
/// collection.
#[derive(Clone)]
pub struct PAssert<T> {
    pcoll: PCollection<T>,
    name: String,
    window: Option<IntervalWindow>,
    panes: PaneSelector,
    /// Shared by every clone, to detect a builder that never added a check.
    guard: Arc<UsageGuard>,
}

/// Panics, once the last clone of an assertion builder is dropped, if none of them
/// ever added a check.
struct UsageGuard {
    pcollection: String,
    checked: AtomicBool,
}

impl Drop for UsageGuard {
    fn drop(&mut self) {
        if !*self.checked.get_mut() && !std::thread::panicking() {
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
            .field("pcollection", &self.pcoll.id())
            .field("name", &self.name)
            .field("window", &self.window)
            .field("panes", &self.panes)
            .finish()
    }
}

impl<T: DefaultCoder> PAssert<T> {
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

    /// Builds the assertion's sub-graph around `check`.
    fn apply_check(&self, check: Check<T>) {
        self.guard.checked.store(true, Ordering::Relaxed);
        let pipeline = self.pcoll.pipeline();
        let name = pipeline.unique_transform_name(&self.name);
        let first_new_transform = pipeline.lock().transform_order.len();

        let selected = self.select_window_and_panes(&name);

        let keyed = selected
            .apply(WindowInto::new(
                format!("{name}/RewindowGlobally"),
                GlobalWindows,
            ))
            .apply(Map::new(format!("{name}/Key"), |element: T| {
                (SINGLE_KEY, Some(element))
            }));

        // The sentinel makes the grouping emit, so an empty collection is checked.
        let sentinel = pipeline.apply(Create::new(
            format!("{name}/Sentinel"),
            [(SINGLE_KEY, None::<T>)],
        ));

        let description = format!("{name}{}", self.describe_selection());
        let success_counter = success_counter_name(&name);
        let failure_counter = failure_counter_name(&name);
        Flatten::pcollections(format!("{name}/Flatten"), &[&keyed, &sentinel])
            .apply(GroupByKey::new(format!("{name}/GroupGlobally")))
            .apply(ParDo::from_fn(
                format!("{name}/Check"),
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
                            Metrics::counter(PASSERT_NAMESPACE, SUCCESS_COUNTER).inc();
                            Metrics::counter(PASSERT_NAMESPACE, &success_counter).inc();
                            Ok(())
                        }
                        Err(reason) => {
                            Metrics::counter(PASSERT_NAMESPACE, FAILURE_COUNTER).inc();
                            Metrics::counter(PASSERT_NAMESPACE, &failure_counter).inc();
                            Err(format!("PAssert '{description}' failed: {reason}").into())
                        }
                    }
                },
            ));

        wrap_in_composite(&self.pcoll, &name, first_new_transform);
    }

    /// Filters the input down to the requested window and panes, if any were requested.
    fn select_window_and_panes(&self, name: &str) -> PCollection<T> {
        let Some(window) = self.window else {
            return self.pcoll.clone();
        };
        let name = name.to_string();
        let panes = self.panes;
        let step = format!("{name}/SelectWindow");
        self.pcoll
            .apply(ParDo::from_fn(step, move |element: T, ctx| {
                let header = ctx.header();
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
            }))
    }

    fn describe_selection(&self) -> String {
        self.window.map_or_else(String::new, |w| {
            format!(
                " in window [{}, {}){}",
                w.start_millis,
                w.end_millis,
                self.panes.describe()
            )
        })
    }
}

/// Nests the transforms added since `first_new_transform` under a composite named `name`.
///
/// Transforms already nested under another composite, such as the pieces of a `Create`,
/// stay where they are.
fn wrap_in_composite<T: 'static>(input: &PCollection<T>, name: &str, first_new_transform: usize) {
    let pipeline = input.pipeline();
    let subtransforms: Vec<String> = {
        let inner = pipeline.lock();
        let added = &inner.transform_order[first_new_transform..];
        let nested: std::collections::HashSet<&str> = added
            .iter()
            .filter_map(|id| inner.components.transforms.get(id))
            .flat_map(|t| t.subtransforms.iter().map(String::as_str))
            .collect();
        added
            .iter()
            .filter(|id| !nested.contains(id.as_str()))
            .cloned()
            .collect()
    };
    let inputs = std::collections::HashMap::from([("in".to_string(), input.id().to_string())]);
    let composite_id = pipeline.add_composite_transform(
        name,
        None,
        Vec::new(),
        inputs,
        std::collections::HashMap::new(),
        subtransforms,
    );
    if let Some(composite) = pipeline.lock().components.transforms.get_mut(&composite_id) {
        // The assertion's name, which its per-assertion counters are named after.
        composite
            .annotations
            .insert(PASSERT_ANNOTATION.to_string(), name.as_bytes().to_vec());
    }
}
