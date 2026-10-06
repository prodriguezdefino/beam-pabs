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

/// Determines when an aggregation such as GroupByKey or CombinePerKey emits output.
#[derive(Clone, Debug, PartialEq, Default)]
pub enum Trigger {
    /// The default trigger. Fires once when the watermark passes the end of the window.
    #[default]
    Default,
    /// Fires when the watermark passes the end of the window, with optional `early` and `late`
    /// firings before and after that point.
    AfterEndOfWindow {
        early: Option<Box<Trigger>>,
        late: Option<Box<Trigger>>,
    },
    /// Fires when processing time passes `delay` after the first element of the pane arrives.
    AfterProcessingTime { delay: Duration },
    /// Fires after upstream processing time has caught up.
    AfterSynchronizedProcessingTime,
    /// Fires when `count` elements have arrived in the current pane.
    AfterCount { count: i32 },
    /// Runs `subtrigger` repeatedly. Its state resets each time it completes.
    Repeatedly { subtrigger: Box<Trigger> },
    /// Runs the subtriggers in sequence. It moves to the next subtrigger after each firing.
    AfterEach { subtriggers: Vec<Trigger> },
    /// Ready when all subtriggers are ready.
    AfterAll { subtriggers: Vec<Trigger> },
    /// Ready when any subtrigger is ready.
    AfterAny { subtriggers: Vec<Trigger> },
    /// Fires `main` repeatedly until `finally` fires. Then output stops.
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
    /// Fires once when the watermark passes the end of the window.
    pub fn after_end_of_window() -> Self {
        Self::AfterEndOfWindow {
            early: None,
            late: None,
        }
    }

    /// Attaches early firings to an `AfterEndOfWindow` trigger, or replaces another variant.
    pub fn with_early_firings(mut self, early: Trigger) -> Self {
        match &mut self {
            Self::AfterEndOfWindow {
                early: early_slot, ..
            } => {
                *early_slot = Some(Box::new(early));
                self
            }
            _ => Self::AfterEndOfWindow {
                early: Some(Box::new(early)),
                late: None,
            },
        }
    }

    /// Attaches late firings to an `AfterEndOfWindow` trigger, or replaces another variant.
    pub fn with_late_firings(mut self, late: Trigger) -> Self {
        match &mut self {
            Self::AfterEndOfWindow {
                late: late_slot, ..
            } => {
                *late_slot = Some(Box::new(late));
                self
            }
            _ => Self::AfterEndOfWindow {
                early: None,
                late: Some(Box::new(late)),
            },
        }
    }

    /// Fires when processing time passes `delay` after the first element of the pane arrives.
    pub fn after_processing_time(delay: Duration) -> Self {
        Self::AfterProcessingTime { delay }
    }

    /// Fires when `count` elements have arrived.
    pub fn after_count(count: i32) -> Self {
        Self::AfterCount { count }
    }

    /// Repeats `subtrigger` indefinitely.
    pub fn repeatedly(subtrigger: Trigger) -> Self {
        Self::Repeatedly {
            subtrigger: Box::new(subtrigger),
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
    pub fn or_finally(main: Trigger, finally: Trigger) -> Self {
        Self::OrFinally {
            main: Box::new(main),
            finally: Box::new(finally),
        }
    }

    pub fn always() -> Self {
        Self::Always
    }

    pub fn never() -> Self {
        Self::Never
    }

    /// Serializes this trigger into the Runner API [`proto::Trigger`].
    pub fn to_proto(&self) -> proto::Trigger {
        use proto::trigger::Trigger as Inner;

        let inner = match self {
            Self::Default => Inner::Default(proto::trigger::Default {}),
            Self::AfterEndOfWindow { early, late } => {
                Inner::AfterEndOfWindow(Box::new(proto::trigger::AfterEndOfWindow {
                    early_firings: early.as_ref().map(|t| Box::new(t.to_proto())),
                    late_firings: late.as_ref().map(|t| Box::new(t.to_proto())),
                }))
            }
            Self::AfterProcessingTime { delay } => {
                Inner::AfterProcessingTime(proto::trigger::AfterProcessingTime {
                    timestamp_transforms: vec![proto::TimestampTransform {
                        timestamp_transform: Some(
                            proto::timestamp_transform::TimestampTransform::Delay(
                                proto::timestamp_transform::Delay {
                                    delay_millis: delay.as_millis() as i64,
                                },
                            ),
                        ),
                    }],
                })
            }
            Self::AfterSynchronizedProcessingTime => Inner::AfterSynchronizedProcessingTime(
                proto::trigger::AfterSynchronizedProcessingTime {},
            ),
            Self::AfterCount { count } => Inner::ElementCount(proto::trigger::ElementCount {
                element_count: *count,
            }),
            Self::Repeatedly { subtrigger } => Inner::Repeat(Box::new(proto::trigger::Repeat {
                subtrigger: Some(Box::new(subtrigger.to_proto())),
            })),
            Self::AfterEach { subtriggers } => Inner::AfterEach(proto::trigger::AfterEach {
                subtriggers: subtriggers.iter().map(Trigger::to_proto).collect(),
            }),
            Self::AfterAll { subtriggers } => Inner::AfterAll(proto::trigger::AfterAll {
                subtriggers: subtriggers.iter().map(Trigger::to_proto).collect(),
            }),
            Self::AfterAny { subtriggers } => Inner::AfterAny(proto::trigger::AfterAny {
                subtriggers: subtriggers.iter().map(Trigger::to_proto).collect(),
            }),
            Self::OrFinally { main, finally } => {
                Inner::OrFinally(Box::new(proto::trigger::OrFinally {
                    main: Some(Box::new(main.to_proto())),
                    finally: Some(Box::new(finally.to_proto())),
                }))
            }
            Self::Always => Inner::Always(proto::trigger::Always {}),
            Self::Never => Inner::Never(proto::trigger::Never {}),
        };

        proto::Trigger {
            trigger: Some(inner),
        }
    }

    /// Deserializes a [`Trigger`] from the Runner API [`proto::Trigger`]. An unset trigger or
    /// subtrigger decodes to [`Trigger::Default`].
    pub fn from_proto(proto_trigger: &proto::Trigger) -> Result<Self, String> {
        use proto::trigger::Trigger as Inner;

        let Some(inner) = &proto_trigger.trigger else {
            return Ok(Self::Default);
        };

        match inner {
            Inner::Default(_) => Ok(Self::Default),
            Inner::AfterEndOfWindow(w) => {
                let early = w
                    .early_firings
                    .as_ref()
                    .map(|t| Self::from_proto(t))
                    .transpose()?
                    .map(Box::new);
                let late = w
                    .late_firings
                    .as_ref()
                    .map(|t| Self::from_proto(t))
                    .transpose()?
                    .map(Box::new);
                Ok(Self::AfterEndOfWindow { early, late })
            }
            Inner::AfterProcessingTime(pt) => {
                let delay_millis = pt
                    .timestamp_transforms
                    .iter()
                    .find_map(|tt| match &tt.timestamp_transform {
                        Some(proto::timestamp_transform::TimestampTransform::Delay(d)) => {
                            Some(d.delay_millis)
                        }
                        _ => None,
                    })
                    .unwrap_or(0);
                Ok(Self::AfterProcessingTime {
                    delay: Duration::from_millis(delay_millis.max(0) as u64),
                })
            }
            Inner::AfterSynchronizedProcessingTime(_) => Ok(Self::AfterSynchronizedProcessingTime),
            Inner::ElementCount(c) => Ok(Self::AfterCount {
                count: c.element_count,
            }),
            Inner::Repeat(r) => {
                let sub = r
                    .subtrigger
                    .as_ref()
                    .map(|t| Self::from_proto(t))
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
                    .map(Self::from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::AfterEach { subtriggers: subs })
            }
            Inner::AfterAll(a) => {
                let subs = a
                    .subtriggers
                    .iter()
                    .map(Self::from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::AfterAll { subtriggers: subs })
            }
            Inner::AfterAny(a) => {
                let subs = a
                    .subtriggers
                    .iter()
                    .map(Self::from_proto)
                    .collect::<Result<Vec<_>, _>>()?;
                Ok(Self::AfterAny { subtriggers: subs })
            }
            Inner::OrFinally(of) => {
                let main = of
                    .main
                    .as_ref()
                    .map(|t| Self::from_proto(t))
                    .transpose()?
                    .unwrap_or(Self::Default);
                let finally = of
                    .finally
                    .as_ref()
                    .map(|t| Self::from_proto(t))
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
