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

//! The trigger DSL and its Runner API serialization.

use std::time::Duration;

use model::pipeline as proto;

use super::error::WindowingError;

/// Determines when an aggregation such as GroupByKey or CombinePerKey emits output.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum Trigger {
    /// The default trigger. Fires once when the watermark passes the end of the window.
    #[default]
    Default,
    /// Fires when the watermark passes the end of the window, with optional `early` and `late`
    /// firings before and after that point.
    #[non_exhaustive]
    AfterEndOfWindow {
        early: Option<Box<Trigger>>,
        late: Option<Box<Trigger>>,
    },
    /// Fires when processing time passes the first element of the pane, changed by
    /// `transforms` in order: a delay, an alignment to a period, or both.
    #[non_exhaustive]
    AfterProcessingTime { transforms: Vec<TimestampTransform> },
    /// Fires after upstream processing time has caught up.
    AfterSynchronizedProcessingTime,
    /// Fires when `count` elements have arrived in the current pane.
    #[non_exhaustive]
    AfterCount { count: i32 },
    /// Runs `subtrigger` repeatedly. Its state resets each time it completes.
    #[non_exhaustive]
    Repeatedly { subtrigger: Box<Trigger> },
    /// Runs the subtriggers in sequence. It moves to the next subtrigger after each firing.
    #[non_exhaustive]
    AfterEach { subtriggers: Vec<Trigger> },
    /// Fires when all subtriggers have fired.
    #[non_exhaustive]
    AfterAll { subtriggers: Vec<Trigger> },
    /// Fires when any subtrigger fires.
    #[non_exhaustive]
    AfterAny { subtriggers: Vec<Trigger> },
    /// Fires `main` repeatedly until `finally` fires. Then output stops.
    #[non_exhaustive]
    OrFinally {
        main: Box<Trigger>,
        finally: Box<Trigger>,
    },
    /// Always ready to fire.
    Always,
    /// Never fires.
    Never,
}

impl Trigger {
    /// Fires when the watermark passes the end of the window. Add early and late firings
    /// with [`AfterWatermark::with_early_firings`] and [`AfterWatermark::with_late_firings`].
    pub fn after_end_of_window() -> AfterWatermark {
        AfterWatermark::default()
    }

    /// Fires when processing time passes `delay` after the first element of the pane arrives.
    pub fn after_processing_time(delay: Duration) -> Self {
        Self::after_processing_time_with(vec![TimestampTransform::Delay(delay)])
    }

    /// Fires when processing time passes the first element of the pane, changed by
    /// `transforms` in order.
    pub fn after_processing_time_with(transforms: Vec<TimestampTransform>) -> Self {
        Self::AfterProcessingTime { transforms }
    }

    /// Fires when `count` elements have arrived.
    pub fn after_count(count: i32) -> Self {
        Self::AfterCount { count }
    }

    /// Repeats `subtrigger` indefinitely.
    pub fn repeatedly(subtrigger: impl Into<Trigger>) -> Self {
        Self::Repeatedly {
            subtrigger: Box::new(subtrigger.into()),
        }
    }

    /// Runs each trigger in sequence.
    pub fn after_each(subtriggers: Vec<Trigger>) -> Self {
        Self::AfterEach { subtriggers }
    }

    /// Fires when all subtriggers have fired.
    pub fn after_all(subtriggers: Vec<Trigger>) -> Self {
        Self::AfterAll { subtriggers }
    }

    /// Fires when any subtrigger fires.
    pub fn after_any(subtriggers: Vec<Trigger>) -> Self {
        Self::AfterAny { subtriggers }
    }

    /// Fires `main` until `finally` fires.
    pub fn or_finally(main: impl Into<Trigger>, finally: impl Into<Trigger>) -> Self {
        Self::OrFinally {
            main: Box::new(main.into()),
            finally: Box::new(finally.into()),
        }
    }

    /// Returns a trigger that is always ready to fire.
    pub fn always() -> Self {
        Self::Always
    }

    /// Returns a trigger that never fires.
    pub fn never() -> Self {
        Self::Never
    }

    /// Returns the trigger of the output of an aggregation that this trigger fired. A
    /// downstream aggregation fires once for each upstream pane, not after the same count
    /// or delay again: a count becomes 1 and a processing-time delay becomes synchronized
    /// processing time.
    pub fn continuation(&self) -> Self {
        let all = |subtriggers: &[Trigger]| subtriggers.iter().map(Self::continuation).collect();
        let boxed = |trigger: &Trigger| Box::new(trigger.continuation());
        match self {
            Self::Default => Self::Default,
            Self::AfterEndOfWindow { early, late } => Self::AfterEndOfWindow {
                early: early.as_deref().map(boxed),
                late: late.as_deref().map(boxed),
            },
            Self::AfterProcessingTime { .. } | Self::AfterSynchronizedProcessingTime => {
                Self::AfterSynchronizedProcessingTime
            }
            Self::AfterCount { .. } => Self::AfterCount { count: 1 },
            Self::Repeatedly { subtrigger } => Self::Repeatedly {
                subtrigger: boxed(subtrigger),
            },
            // The upstream panes can come from any subtrigger, so fire on each of them.
            Self::AfterEach { subtriggers } => Self::Repeatedly {
                subtrigger: Box::new(Self::AfterAny {
                    subtriggers: all(subtriggers),
                }),
            },
            Self::AfterAll { subtriggers } => Self::AfterAll {
                subtriggers: all(subtriggers),
            },
            Self::AfterAny { subtriggers } => Self::AfterAny {
                subtriggers: all(subtriggers),
            },
            Self::OrFinally { main, finally } => Self::OrFinally {
                main: boxed(main),
                finally: boxed(finally),
            },
            Self::Always => Self::Always,
            Self::Never => Self::Never,
        }
    }
}

impl From<&Trigger> for proto::Trigger {
    fn from(trigger: &Trigger) -> Self {
        use proto::trigger::Trigger as Inner;

        let inner = match trigger {
            Trigger::Default => Inner::Default(proto::trigger::Default {}),
            Trigger::AfterEndOfWindow { early, late } => {
                Inner::AfterEndOfWindow(Box::new(proto::trigger::AfterEndOfWindow {
                    early_firings: early
                        .as_ref()
                        .map(|t| Box::new(proto::Trigger::from(t.as_ref()))),
                    late_firings: late
                        .as_ref()
                        .map(|t| Box::new(proto::Trigger::from(t.as_ref()))),
                }))
            }
            Trigger::AfterProcessingTime { transforms } => {
                Inner::AfterProcessingTime(proto::trigger::AfterProcessingTime {
                    timestamp_transforms: transforms
                        .iter()
                        .map(proto::TimestampTransform::from)
                        .collect(),
                })
            }
            Trigger::AfterSynchronizedProcessingTime => Inner::AfterSynchronizedProcessingTime(
                proto::trigger::AfterSynchronizedProcessingTime {},
            ),
            Trigger::AfterCount { count } => Inner::ElementCount(proto::trigger::ElementCount {
                element_count: *count,
            }),
            Trigger::Repeatedly { subtrigger } => Inner::Repeat(Box::new(proto::trigger::Repeat {
                subtrigger: Some(Box::new(proto::Trigger::from(subtrigger.as_ref()))),
            })),
            Trigger::AfterEach { subtriggers } => Inner::AfterEach(proto::trigger::AfterEach {
                subtriggers: subtriggers.iter().map(proto::Trigger::from).collect(),
            }),
            Trigger::AfterAll { subtriggers } => Inner::AfterAll(proto::trigger::AfterAll {
                subtriggers: subtriggers.iter().map(proto::Trigger::from).collect(),
            }),
            Trigger::AfterAny { subtriggers } => Inner::AfterAny(proto::trigger::AfterAny {
                subtriggers: subtriggers.iter().map(proto::Trigger::from).collect(),
            }),
            Trigger::OrFinally { main, finally } => {
                Inner::OrFinally(Box::new(proto::trigger::OrFinally {
                    main: Some(Box::new(proto::Trigger::from(main.as_ref()))),
                    finally: Some(Box::new(proto::Trigger::from(finally.as_ref()))),
                }))
            }
            Trigger::Always => Inner::Always(proto::trigger::Always {}),
            Trigger::Never => Inner::Never(proto::trigger::Never {}),
        };

        proto::Trigger {
            trigger: Some(inner),
        }
    }
}

impl From<Trigger> for proto::Trigger {
    fn from(trigger: Trigger) -> Self {
        proto::Trigger::from(&trigger)
    }
}

impl TryFrom<&proto::Trigger> for Trigger {
    type Error = WindowingError;

    fn try_from(proto_trigger: &proto::Trigger) -> Result<Self, Self::Error> {
        use proto::trigger::Trigger as Inner;

        let Some(inner) = &proto_trigger.trigger else {
            return Ok(Self::Default);
        };

        match inner {
            Inner::Default(_) => Ok(Self::Default),
            Inner::AfterEndOfWindow(w) => {
                let early = w
                    .early_firings
                    .as_deref()
                    .map(Self::try_from)
                    .transpose()?
                    .map(Box::new);
                let late = w
                    .late_firings
                    .as_deref()
                    .map(Self::try_from)
                    .transpose()?
                    .map(Box::new);
                Ok(Self::AfterEndOfWindow { early, late })
            }
            Inner::AfterProcessingTime(pt) => Ok(Self::AfterProcessingTime {
                transforms: pt
                    .timestamp_transforms
                    .iter()
                    .filter_map(TimestampTransform::from_proto)
                    .collect(),
            }),
            Inner::AfterSynchronizedProcessingTime(_) => Ok(Self::AfterSynchronizedProcessingTime),
            Inner::ElementCount(c) => {
                if c.element_count < 1 {
                    return Err(WindowingError::InvalidTriggerCount(c.element_count));
                }
                Ok(Self::AfterCount {
                    count: c.element_count,
                })
            }
            Inner::Repeat(r) => {
                let sub = r
                    .subtrigger
                    .as_deref()
                    .map(Self::try_from)
                    .transpose()?
                    .unwrap_or(Self::Default);
                Ok(Self::Repeatedly {
                    subtrigger: Box::new(sub),
                })
            }
            Inner::AfterEach(e) => {
                let subs = e
                    .subtriggers
                    .iter()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::AfterEach { subtriggers: subs })
            }
            Inner::AfterAll(a) => {
                let subs = a
                    .subtriggers
                    .iter()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::AfterAll { subtriggers: subs })
            }
            Inner::AfterAny(a) => {
                let subs = a
                    .subtriggers
                    .iter()
                    .map(Self::try_from)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::AfterAny { subtriggers: subs })
            }
            Inner::OrFinally(of) => {
                let main = of
                    .main
                    .as_deref()
                    .map(Self::try_from)
                    .transpose()?
                    .unwrap_or(Self::Default);
                let finally = of
                    .finally
                    .as_deref()
                    .map(Self::try_from)
                    .transpose()?
                    .unwrap_or(Self::Default);
                Ok(Self::OrFinally {
                    main: Box::new(main),
                    finally: Box::new(finally),
                })
            }
            Inner::Always(_) => Ok(Self::Always),
            Inner::Never(_) => Ok(Self::Never),
        }
    }
}

impl TryFrom<proto::Trigger> for Trigger {
    type Error = WindowingError;

    fn try_from(proto_trigger: proto::Trigger) -> Result<Self, Self::Error> {
        Self::try_from(&proto_trigger)
    }
}

/// The trigger that fires when the watermark passes the end of the window.
/// Only this trigger takes early and late firings.
#[derive(Clone, Debug, Default, PartialEq)]
pub struct AfterWatermark {
    early: Option<Trigger>,
    late: Option<Trigger>,
}

impl AfterWatermark {
    /// Also fires with `early` before the watermark passes the end of the window.
    pub fn with_early_firings(mut self, early: impl Into<Trigger>) -> Self {
        self.early = Some(early.into());
        self
    }

    /// Also fires with `late` for data that arrives after the watermark.
    pub fn with_late_firings(mut self, late: impl Into<Trigger>) -> Self {
        self.late = Some(late.into());
        self
    }
}

impl From<AfterWatermark> for Trigger {
    fn from(t: AfterWatermark) -> Self {
        Trigger::AfterEndOfWindow {
            early: t.early.map(Box::new),
            late: t.late.map(Box::new),
        }
    }
}

/// A change to the time that an [`Trigger::AfterProcessingTime`] trigger waits for.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum TimestampTransform {
    /// Adds a delay.
    Delay(Duration),
    /// Rounds up to the next multiple of `period` after `offset`.
    AlignTo { period: Duration, offset: Duration },
}

impl TimestampTransform {
    /// Decodes a transform. A transform with no kind set decodes as `None`, and a negative
    /// duration as zero, because `Duration` cannot be negative.
    fn from_proto(transform: &proto::TimestampTransform) -> Option<Self> {
        use proto::timestamp_transform::TimestampTransform as Kind;
        let millis = |value: i64| Duration::from_millis(u64::try_from(value).unwrap_or(0));
        match transform.timestamp_transform.as_ref()? {
            Kind::Delay(delay) => Some(Self::Delay(millis(delay.delay_millis))),
            Kind::AlignTo(align) => Some(Self::AlignTo {
                period: millis(align.period),
                offset: millis(align.offset),
            }),
        }
    }
}

impl From<&TimestampTransform> for proto::TimestampTransform {
    fn from(transform: &TimestampTransform) -> Self {
        use proto::timestamp_transform::{AlignTo, Delay, TimestampTransform as Kind};
        let millis = |duration: &Duration| i64::try_from(duration.as_millis()).unwrap_or(i64::MAX);
        let kind = match transform {
            TimestampTransform::Delay(delay) => Kind::Delay(Delay {
                delay_millis: millis(delay),
            }),
            TimestampTransform::AlignTo { period, offset } => Kind::AlignTo(AlignTo {
                period: millis(period),
                offset: millis(offset),
            }),
        };
        Self {
            timestamp_transform: Some(kind),
        }
    }
}
