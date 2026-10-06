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

use file::filesystem::{GlobMatcher, glob_match};

#[test]
fn test_glob_semantics() {
    for (pattern, path, expected) in [
        ("*.txt", "hello.txt", true),
        ("*.txt", "hello.csv", false),
        ("dir/*.txt", "dir/hello.txt", true),
        // `*` must not cross a path separator.
        ("dir/*.txt", "dir/sub/hello.txt", false),
        ("dir/**/*.txt", "dir/sub/hello.txt", true),
        // `**/` also matches zero segments.
        ("dir/**/*.txt", "dir/hello.txt", true),
        ("**/*.txt", "a/b/c/hello.txt", true),
        ("item_?.log", "item_1.log", true),
        ("item_?.log", "item_10.log", false),
        // `?` must not match a path separator.
        ("a?b", "a/b", false),
        ("item_[0-9].log", "item_7.log", true),
        ("item_[0-9].log", "item_x.log", false),
        ("item_[!0-9].log", "item_x.log", true),
        ("report.{csv,txt}", "report.csv", true),
        ("report.{csv,txt}", "report.txt", true),
        ("report.{csv,txt}", "report.json", false),
    ] {
        assert_eq!(
            glob_match(pattern, path).unwrap(),
            expected,
            "{pattern} vs {path}"
        );
    }
}

#[test]
fn test_glob_malformed_pattern_is_an_error() {
    let err = glob_match("item_[0-9.log", "item_7.log").unwrap_err();
    assert!(
        err.to_string().contains("invalid glob pattern"),
        "unexpected error text: {err}"
    );
    // And it converts into an io::Error so `FileSystem` impls can use `?`.
    let io_err: std::io::Error = err.into();
    assert_eq!(io_err.kind(), std::io::ErrorKind::InvalidInput);
}

#[test]
fn test_glob_matcher_is_parseable_and_reusable() {
    let matcher: GlobMatcher = "data/**/*.txt".parse().unwrap();
    assert!(matcher.is_match("data/2024/jan/events.txt"));
    assert!(!matcher.is_match("data/2024/jan/events.csv"));
}

#[test]
fn test_glob_matching_is_linear_not_backtracking() {
    // Pathological input for a backtracking matcher.
    let text = "a".repeat(40);
    let start = std::time::Instant::now();
    assert!(!glob_match("*a*a*a*a*a*a*a*a*b", &text).unwrap());
    let elapsed = start.elapsed();
    assert!(
        elapsed.as_millis() < 500,
        "glob matching took {elapsed:?}; expected linear-time behavior"
    );
}
