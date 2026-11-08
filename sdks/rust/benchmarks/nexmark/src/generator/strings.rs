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

//! Deterministic string, price, and bounded integer sampling helpers.

use super::JavaRandom;
use crate::config::round_half_up;

const MIN_STRING_LENGTH: i32 = 3;

/// Samples a trimmed lowercase phrase of up to `max_length` characters.
pub(crate) fn next_string(random: &mut JavaRandom, max_length: i32) -> String {
    let len = if max_length <= MIN_STRING_LENGTH {
        MIN_STRING_LENGTH
    } else {
        MIN_STRING_LENGTH + random.next_int_bound(max_length - MIN_STRING_LENGTH)
    };
    let raw: String = (0..len)
        .map(|_| {
            if random.next_int_bound(13) == 0 {
                ' '
            } else {
                (b'a' + (random.next_int_bound(26) as u8)) as char
            }
        })
        .collect();
    match raw.trim() {
        trimmed if trimmed.len() == raw.len() => raw,
        trimmed => trimmed.to_owned(),
    }
}

/// Samples an exact `length`-byte ASCII string with six base-26 digits per 32-bit draw.
pub(crate) fn next_exact_string(random: &mut JavaRandom, length: usize) -> String {
    (0..length)
        .scan((0_i32, 0_u8), |(rnd, rem), _| {
            if *rem == 0 {
                *rnd = random.next_int();
                *rem = 6;
            }
            let ch = ((i32::from(b'a') + (*rnd % 26)) as u8) as char;
            *rnd /= 26;
            *rem -= 1;
            Some(ch)
        })
        .collect()
}

/// Samples `extra` padding so the total record approaches `desired_average_size` bytes.
pub(crate) fn next_extra(
    random: &mut JavaRandom,
    current_size: i32,
    desired_average_size: i32,
) -> String {
    if current_size > desired_average_size {
        return String::new();
    }
    let avg = desired_average_size - current_size;
    let delta = round_half_up(f64::from(avg) * 0.2) as i32;
    let jitter = if delta == 0 {
        0
    } else {
        random.next_int_bound(2 * delta)
    };
    let desired_size = avg - delta + jitter;
    if desired_size <= 0 {
        String::new()
    } else {
        next_exact_string(random, desired_size as usize)
    }
}

/// Samples a log-uniform price in cents (`$1.00` to `$1,000,000.00`).
pub(crate) fn next_price(random: &mut JavaRandom) -> i64 {
    round_half_up(10.0_f64.powf(random.next_double() * 6.0) * 100.0)
}

/// Samples a bounded integer in `0..n` (or `0` when `n <= 0`).
pub(crate) fn next_long(random: &mut JavaRandom, n: i64) -> i64 {
    if n <= 0 {
        0
    } else if n < i64::from(i32::MAX) {
        i64::from(random.next_int_bound(n as i32))
    } else {
        (random.next_long() % n).wrapping_abs()
    }
}
