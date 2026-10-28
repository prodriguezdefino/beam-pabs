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

//! CPU time of the bench process as a criterion measurement.

use std::mem::MaybeUninit;
use std::time::Duration;

use criterion::measurement::{Measurement, ValueFormatter, WallTime};

/// User and system CPU time of all threads of the process, so it includes the bench runner.
pub struct CpuTime;

fn process_cpu_time() -> Duration {
    let mut usage = MaybeUninit::<libc::rusage>::zeroed();
    // SAFETY: `RUSAGE_SELF` is valid and `getrusage` fills the struct. All-zero is also a
    // valid `rusage`.
    let usage = unsafe {
        libc::getrusage(libc::RUSAGE_SELF, usage.as_mut_ptr());
        usage.assume_init()
    };
    let duration = |time: libc::timeval| {
        Duration::new(time.tv_sec as u64, 0) + Duration::from_micros(time.tv_usec as u64)
    };
    duration(usage.ru_utime) + duration(usage.ru_stime)
}

impl Measurement for CpuTime {
    type Intermediate = Duration;
    type Value = Duration;

    fn start(&self) -> Duration {
        process_cpu_time()
    }

    fn end(&self, start: Duration) -> Duration {
        process_cpu_time().saturating_sub(start)
    }

    fn add(&self, v1: &Duration, v2: &Duration) -> Duration {
        *v1 + *v2
    }

    fn zero(&self) -> Duration {
        Duration::ZERO
    }

    fn to_f64(&self, value: &Duration) -> f64 {
        value.as_nanos() as f64
    }

    fn formatter(&self) -> &dyn ValueFormatter {
        // Both measure a duration in nanoseconds.
        static WALL_TIME: WallTime = WallTime;
        WALL_TIME.formatter()
    }
}
