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

//! Event generation rate units and curve shapes.

use std::str::FromStr;

use serde::{Deserialize, Serialize};

/// Time unit for event generation rates.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RateUnit {
    #[default]
    PerSecond,
    PerMinute,
}

impl RateUnit {
    /// Microseconds in one unit interval.
    pub const fn us_per_unit(self) -> i64 {
        match self {
            Self::PerSecond => 1_000_000,
            Self::PerMinute => 60_000_000,
        }
    }

    /// Microseconds between events at `rate` events per unit, rounded to nearest integer.
    pub const fn rate_to_period_us(self, rate: i64) -> i64 {
        (self.us_per_unit() + rate / 2) / rate
    }
}

impl FromStr for RateUnit {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "PER_SECOND" => Ok(Self::PerSecond),
            "PER_MINUTE" => Ok(Self::PerMinute),
            _ => Err(format!("unknown rate unit: {s}")),
        }
    }
}

/// Shape of the synthetic event rate curve.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, Hash, Serialize, Deserialize)]
#[serde(rename_all = "SCREAMING_SNAKE_CASE")]
pub enum RateShape {
    /// Step wave alternating between `first_event_rate` and `next_event_rate`.
    Square,
    /// Ten-step sine curve around the midpoint rate.
    #[default]
    Sine,
}

impl RateShape {
    /// Maximum step count across all rate shapes.
    pub const MAX_STEPS: usize = 10;

    /// Inter-event delays in microseconds for one generator across one rate cycle.
    pub fn inter_event_delay_us(
        self,
        first_rate: i32,
        next_rate: i32,
        unit: RateUnit,
        num_generators: i32,
    ) -> Vec<i64> {
        let (buf, len) = self.delay_curve_us(first_rate, next_rate, unit, num_generators);
        buf[..len].to_vec()
    }

    pub(crate) fn delay_curve_us(
        self,
        first_rate: i32,
        next_rate: i32,
        unit: RateUnit,
        num_generators: i32,
    ) -> ([i64; Self::MAX_STEPS], usize) {
        let gens = i64::from(num_generators);
        let mut out = [0_i64; Self::MAX_STEPS];
        if first_rate == next_rate {
            out[0] = unit.rate_to_period_us(i64::from(first_rate)) * gens;
            return (out, 1);
        }
        match self {
            Self::Square => {
                out[0] = unit.rate_to_period_us(i64::from(first_rate)) * gens;
                out[1] = unit.rate_to_period_us(i64::from(next_rate)) * gens;
                (out, 2)
            }
            Self::Sine => {
                let mid = f64::from(first_rate + next_rate) / 2.0;
                let amp = f64::from(first_rate - next_rate) / 2.0;
                let sine = std::array::from_fn(|i| {
                    let angle =
                        (2.0 * std::f64::consts::PI * (i as f64)) / (Self::MAX_STEPS as f64);
                    unit.rate_to_period_us(round_half_up(mid + amp * angle.cos())) * gens
                });
                (sine, Self::MAX_STEPS)
            }
        }
    }

    /// Seconds spent at each step of [`Self::inter_event_delay_us`] for `rate_period_sec`.
    pub const fn step_length_sec(self, rate_period_sec: i32) -> i32 {
        let steps = match self {
            Self::Square => 2,
            Self::Sine => Self::MAX_STEPS as i32,
        };
        (rate_period_sec + steps - 1) / steps
    }
}

impl FromStr for RateShape {
    type Err = String;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s.to_ascii_uppercase().as_str() {
            "SQUARE" => Ok(Self::Square),
            "SINE" => Ok(Self::Sine),
            _ => Err(format!("unknown rate shape: {s}")),
        }
    }
}

/// Rounds finite values with `floor(x + 0.5)` and maps `NaN` to zero.
pub(crate) fn round_half_up(x: f64) -> i64 {
    if x.is_nan() {
        0
    } else {
        (x + 0.5).floor() as i64
    }
}
