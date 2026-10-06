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

//! Dataflow job names.

use std::hash::{BuildHasher, RandomState};
use std::time::{Duration, SystemTime, UNIX_EPOCH};

pub(super) fn sanitize_job_name(name: &str) -> String {
    let sanitized: String = name
        .chars()
        .map(|c| {
            if c.is_ascii_alphanumeric() {
                c.to_ascii_lowercase()
            } else {
                '-'
            }
        })
        .collect();

    match sanitized.trim_matches('-') {
        "" => "beam-job".to_string(),
        trimmed => trimmed.to_string(),
    }
}

/// Formats a default job name as `<app>-<user>-<MMddHHmmss UTC>-<random hex>`, where `app`
/// is the executable name.
pub fn generate_job_name() -> String {
    let now = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default();
    let app = std::env::current_exe()
        .ok()
        .and_then(|exe| exe.file_stem().map(|s| s.to_string_lossy().into_owned()))
        .unwrap_or_default();
    let user = ["USER", "USERNAME"]
        .iter()
        .find_map(|key| std::env::var(key).ok())
        .unwrap_or_default();
    let random = RandomState::new().hash_one((now.as_nanos(), std::process::id())) as u32;
    format!("{}-{random:08x}", job_name_prefix(&app, &user, now))
}

/// Formats `<app>-<user>-<MMddHHmmss>`, reducing each part to `[a-z0-9]`.
pub fn job_name_prefix(app: &str, user: &str, since_epoch: Duration) -> String {
    let normalize = |part: &str| -> String {
        part.chars()
            .map(|c| c.to_ascii_lowercase())
            .map(|c| if c.is_ascii_alphanumeric() { c } else { '0' })
            .collect()
    };
    let app = Some(normalize(app))
        .filter(|app| !app.is_empty())
        .map_or_else(
            || "beamapp".to_string(),
            |app| {
                if app.starts_with(|c: char| c.is_ascii_lowercase()) {
                    app
                } else {
                    format!("a{}", app.get(1..).unwrap_or_default())
                }
            },
        );
    let secs = since_epoch.as_secs();
    let (month, day) = civil_month_day(secs / 86_400);
    let (hour, minute, second) = (secs / 3_600 % 24, secs / 60 % 60, secs % 60);
    let date = format!("{month:02}{day:02}{hour:02}{minute:02}{second:02}");
    [app, normalize(user), date]
        .into_iter()
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("-")
}

/// Month and day of the UTC date `days` after 1970-01-01 (Hinnant's `civil_from_days`).
fn civil_month_day(days: u64) -> (u64, u64) {
    let doe = (days + 719_468) % 146_097;
    let yoe = (doe - doe / 1_460 + doe / 36_524 - doe / 146_096) / 365;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let day = doy - (153 * mp + 2) / 5 + 1;
    let month = if mp < 10 { mp + 3 } else { mp - 9 };
    (month, day)
}
