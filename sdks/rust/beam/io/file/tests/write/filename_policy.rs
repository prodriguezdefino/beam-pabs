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

//! Output file naming: shard templates and [`FilenamePolicy::file_path`].

use beam::coders::{IntervalWindow, PaneInfo, Timing};
use file::{DefaultFilenamePolicy, FileNamingContext, FilenamePolicy, format_shard_template};

fn ctx<'a>(index: u32, total: u32, template: &'a str, suffix: &'a str) -> FileNamingContext<'a> {
    FileNamingContext {
        windowed: false,
        window: None,
        pane: None,
        shard_index: index,
        num_shards: total,
        shard_template: template,
        suffix,
    }
}

#[test]
fn shard_is_padded_and_placed_before_extension_or_suffix() {
    for (template, index, total, expected) in [
        ("-SSSSS-of-NNNNN", 3, 12, "-00003-of-00012"),
        ("_S_N", 3, 12, "_3_12"),
        ("", 3, 12, ""),
    ] {
        assert_eq!(format_shard_template(template, index, total), expected);
    }

    let p = DefaultFilenamePolicy;
    for (base, naming, expected) in [
        (
            "/out/data.txt",
            ctx(1, 4, "-SSSSS-of-NNNNN", ""),
            "/out/data-00001-of-00004.txt",
        ),
        (
            "/out/data",
            ctx(1, 4, "-SSSSS-of-NNNNN", ""),
            "/out/data-00001-of-00004",
        ),
        ("/out/data.txt", ctx(0, 1, "", ""), "/out/data.txt"),
        // An explicit suffix follows the shard.
        (
            "/out/part",
            ctx(2, 3, "-SS-of-NN", ".tar.gz"),
            "/out/part-02-of-03.tar.gz",
        ),
    ] {
        assert_eq!(p.file_path(base, &naming), expected);
    }
}

#[test]
fn hidden_files_and_dotted_directories_have_no_extension() {
    let p = DefaultFilenamePolicy;
    assert_eq!(
        p.file_path("/out.d/.data", &ctx(0, 2, "-S", "")),
        "/out.d/.data-0"
    );
}

#[test]
fn windowed_names_combine_window_and_shard() {
    let p = DefaultFilenamePolicy;
    let naming = FileNamingContext {
        windowed: true,
        window: Some(IntervalWindow::new(0, 60_000)),
        ..ctx(0, 2, "-S-of-N", "")
    };
    assert_eq!(
        p.file_path("/out/w.txt", &naming),
        "/out/w-0-60000-0-of-2.txt"
    );
}

#[test]
fn windowed_names_include_non_trivial_panes() {
    let late = PaneInfo {
        is_first: false,
        is_last: false,
        timing: Timing::Late,
        index: 2,
        on_time_index: 1,
    };
    let naming = FileNamingContext {
        windowed: true,
        window: Some(IntervalWindow::new(0, 10)),
        pane: Some(late),
        ..ctx(0, 1, "", ".txt")
    };
    assert_eq!(naming.window_string(), "-0-10-pane-2-late");
    assert_eq!(
        DefaultFilenamePolicy.file_path("/o/p", &naming),
        "/o/p-0-10-pane-2-late.txt"
    );
}

#[test]
fn closures_are_policies() {
    let policy = |base: &str, ctx: &FileNamingContext<'_>| {
        format!("{base}-custom{}{}", ctx.shard_string(), ctx.suffix)
    };
    assert_eq!(
        policy.file_path("/out/p", &ctx(1, 2, "-S-of-N", ".csv")),
        "/out/p-custom-1-of-2.csv"
    );
}

#[test]
fn window_string_omits_trivial_panes() {
    let pane = |is_first, is_last, timing, index| PaneInfo {
        is_first,
        is_last,
        timing,
        index,
        on_time_index: 0,
    };
    let window = Some(IntervalWindow::new(0, 10));
    for (windowed, window, pane, expected) in [
        (false, window, Some(pane(false, false, Timing::Late, 2)), ""),
        (true, None, None, "-GlobalWindow"),
        (true, window, None, "-0-10"),
        (
            true,
            window,
            Some(PaneInfo::ON_TIME_AND_ONLY_FIRING),
            "-0-10",
        ),
        (true, window, Some(PaneInfo::NO_FIRING), "-0-10"),
        (
            true,
            window,
            Some(pane(false, false, Timing::Unknown, 3)),
            "-0-10",
        ),
        (
            true,
            window,
            Some(pane(true, false, Timing::OnTime, 0)),
            "-0-10-pane-0-on-time",
        ),
        (
            true,
            window,
            Some(pane(false, true, Timing::OnTime, 1)),
            "-0-10-pane-1-on-time",
        ),
        (
            true,
            window,
            Some(pane(true, true, Timing::Early, 0)),
            "-0-10-pane-0-early",
        ),
    ] {
        let naming = FileNamingContext {
            windowed,
            window,
            pane,
            ..ctx(0, 1, "", "")
        };
        assert_eq!(naming.window_string(), expected, "{pane:?}");
    }
}
