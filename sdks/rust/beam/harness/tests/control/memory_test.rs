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

//! The memory monitor: reading the resident size and the cgroup limit, and the threshold.

use harness::memory::{MemoryReading, mib, parse_cgroup_limit, parse_status_rss};

#[test]
fn the_resident_size_comes_from_the_vmrss_line() {
    let status = "Name:\tworker\nVmPeak:\t  900000 kB\nVmRSS:\t  204800 kB\nThreads:\t12\n";
    assert_eq!(parse_status_rss(status), Some(204_800 * 1024));
    assert_eq!(parse_status_rss("Name:\tworker\n"), None);
    assert_eq!(parse_status_rss("VmRSS:\tlots\n"), None);
}

#[test]
fn a_cgroup_limit_is_bytes_and_max_or_the_v1_sentinel_mean_no_limit() {
    assert_eq!(parse_cgroup_limit("4294967296\n"), Some(4 << 30));
    assert_eq!(parse_cgroup_limit("max\n"), None);
    assert_eq!(parse_cgroup_limit("9223372036854771712\n"), None);
    assert_eq!(parse_cgroup_limit("0"), None);
}

#[test]
fn a_reading_is_near_the_limit_from_ninety_percent() {
    let reading = |rss, limit| MemoryReading {
        rss_bytes: rss,
        peak_rss_bytes: rss,
        limit_bytes: limit,
    };
    assert!(reading(Some(90), Some(100)).near_limit());
    assert!(!reading(Some(89), Some(100)).near_limit());
    assert!(!reading(Some(99), None).near_limit());
    assert!(!reading(None, Some(100)).near_limit());
    assert!(reading(Some(u64::MAX), Some(u64::MAX)).near_limit());
}

#[test]
fn sizes_print_in_mebibytes() {
    assert_eq!(mib(Some(3 << 20)), "3 MiB");
    assert_eq!(mib(None), "unknown");
}
