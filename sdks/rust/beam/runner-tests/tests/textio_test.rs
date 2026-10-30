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

use std::fs;
use std::io::Read as _;
use std::time::Duration;

use beam::io::file::textio;
use beam::io::file::{
    FileBasedSource, FileFormat, FileNamingContext, FileRecordReader, FileSystem, FormatSink,
    WriteFiles,
};
use beam::pipeline::Pipeline;
use beam::prelude::*;
use beam::testing::TestStream;
use beam::transforms::Create;
use beam::transforms::sdf::{OffsetRangeTracker, RestrictionTracker};
use beam::values::IsBounded;
use beam::windowing::{GlobalWindows, Trigger};
use fluent::prelude::{PCollectionExt, PCollectionKeyedExt};
use prism::PrismRunner;

/// Building a pipeline must not touch the destination; only running it may.
#[tokio::test]
async fn test_textio_write_leaves_destination_untouched_until_run() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_deferred_{}", std::process::id()));
    let out_file = temp_dir.join("existing.txt");

    fs::create_dir_all(&temp_dir).unwrap();
    fs::write(&out_file, "pre-existing content\n").unwrap();

    let p = Pipeline::new();
    p.apply("Create", Create::new(vec!["replacement".to_string()]))
        .apply(
            "TextIO.Write",
            textio::Write::new(out_file.to_str().unwrap()).without_sharding(),
        );

    assert_eq!(
        fs::read_to_string(&out_file).unwrap(),
        "pre-existing content\n",
        "constructing the pipeline must not modify the destination"
    );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    assert_eq!(fs::read_to_string(&out_file).unwrap(), "replacement\n");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_textio_multi_file_glob_and_write() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_glob_test_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();

    let file_a = temp_dir.join("input_a.txt");
    let file_b = temp_dir.join("input_b.txt");
    let out_file = temp_dir.join("combined_out.txt");

    fs::write(&file_a, "alpha\nbeta\n").unwrap();
    fs::write(&file_b, "gamma\ndelta\n").unwrap();

    let p = Pipeline::new();
    let glob_pattern = format!("{}/input_*.txt", temp_dir.to_str().unwrap());
    let out_str = out_file.to_str().unwrap();

    let lines = p.apply("TextIO.Read", textio::Read::new(&glob_pattern));
    let uppercased = lines.map("Uppercase", |line: String| line.to_uppercase());
    uppercased.apply(
        "TextIO.Write",
        textio::Write::new(out_str).without_sharding(),
    );

    let runner = PrismRunner::new();
    let res = p.run_with_runner(&runner).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    let result_content = fs::read_to_string(&out_file).unwrap();
    let mut result_lines: Vec<&str> = result_content.lines().collect();
    result_lines.sort();
    assert_eq!(result_lines, vec!["ALPHA", "BETA", "DELTA", "GAMMA"]);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_textio_empty_file() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_empty_test_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();

    let in_file = temp_dir.join("empty_input.txt");
    let out_file = temp_dir.join("empty_output.txt");
    fs::write(&in_file, "").unwrap();

    let p = Pipeline::new();
    let in_str = in_file.to_str().unwrap();
    let out_str = out_file.to_str().unwrap();

    let lines = p.apply("TextIO.Read", textio::Read::new(in_str));
    lines.apply(
        "TextIO.Write",
        textio::Write::new(out_str).without_sharding(),
    );

    let runner = PrismRunner::new();
    let res = p.run_with_runner(&runner).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    let result_content = fs::read_to_string(&out_file).unwrap();
    assert_eq!(
        result_content, "",
        "Empty file should result in 0 lines written"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_textio_crlf_newlines() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_crlf_test_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();

    let in_file = temp_dir.join("crlf_input.txt");
    let out_file = temp_dir.join("crlf_output.txt");
    fs::write(&in_file, "line1\r\nline2\r\nline3\r\n").unwrap();

    let p = Pipeline::new();
    let in_str = in_file.to_str().unwrap();
    let out_str = out_file.to_str().unwrap();

    let lines = p.apply("TextIO.Read", textio::Read::new(in_str));
    lines.apply(
        "TextIO.Write",
        textio::Write::new(out_str).without_sharding(),
    );

    let runner = PrismRunner::new();
    let res = p.run_with_runner(&runner).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    let result_content = fs::read_to_string(&out_file).unwrap();
    assert_eq!(result_content, "line1\nline2\nline3\n");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_textio_read_with_filename() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_with_name_test_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();

    let in_file = temp_dir.join("file_named.txt");
    let out_file = temp_dir.join("out_named.txt");
    fs::write(&in_file, "item1\nitem2\n").unwrap();

    let p = Pipeline::new();
    let in_str = in_file.to_str().unwrap();
    let out_str = out_file.to_str().unwrap();

    let kv_lines = p.apply(
        "TextIO.ReadWithFilename",
        textio::ReadWithFilename::new(in_str),
    );
    let formatted = kv_lines.map("FormatKV", |(file, line): (String, String)| {
        format!("{file} -> {line}")
    });
    formatted.apply(
        "TextIO.Write",
        textio::Write::new(out_str).without_sharding(),
    );

    let runner = PrismRunner::new();
    let res = p.run_with_runner(&runner).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    let result_content = fs::read_to_string(&out_file).unwrap();
    let mut lines: Vec<&str> = result_content.lines().collect();
    lines.sort();
    let expected = [format!("{in_str} -> item1"), format!("{in_str} -> item2")];
    let expected_refs: Vec<&str> = expected.iter().map(String::as_str).collect();
    assert_eq!(lines, expected_refs);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_textio_no_matching_files_error() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_err_test_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();

    let non_existent_pattern = format!("{}/does_not_exist_*.txt", temp_dir.to_str().unwrap());
    let out_file = temp_dir.join("out.txt");

    let p = Pipeline::new();
    let lines = p.apply("TextIO.Read", textio::Read::new(&non_existent_pattern));
    lines.apply(
        "TextIO.Write",
        textio::Write::new(out_file.to_str().unwrap()).without_sharding(),
    );

    let runner = PrismRunner::new();
    let err = p
        .run_with_runner(&runner)
        .await
        .expect_err("Expected pipeline execution to fail when no files match pattern");
    let message = format!("{err:?}");
    assert!(message.contains("No files matched pattern"), "{message}");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[derive(Clone, Default)]
struct AssignStringTimestampDoFn;

impl DoFn for AssignStringTimestampDoFn {
    type In = (String, i64);
    type Out = String;

    fn process_element(
        &mut self,
        (line, ts): Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> Result {
        ctx.output(line).at(Instant::from_millis(ts)).emit()
    }
}

#[tokio::test]
async fn test_textio_windowed_writes_fixed_windows() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_windowed_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();
    let out_file = temp_dir.join("output.txt");

    let p = Pipeline::new();
    p.apply(
        "Create",
        Create::new(vec![
            ("window 1 line 1".to_string(), 1_000i64),
            ("window 1 line 2".to_string(), 5_000i64),
            ("window 2 line 1".to_string(), 11_000i64),
            ("window 2 line 2".to_string(), 15_000i64),
        ]),
    )
    .par_do("AssignTimestamps", AssignStringTimestampDoFn)
    .apply(
        "WindowInto",
        WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
    )
    .apply(
        "TextIO.Write",
        textio::Write::new(out_file.to_str().unwrap())
            .without_sharding()
            .with_windowed_writes(),
    );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    let win1_file = temp_dir.join("output-0-10000.txt");
    let win2_file = temp_dir.join("output-10000-20000.txt");

    assert!(win1_file.exists(), "File {win1_file:?} must exist");
    assert!(win2_file.exists(), "File {win2_file:?} must exist");

    assert_eq!(
        fs::read_to_string(&win1_file).unwrap(),
        "window 1 line 1\nwindow 1 line 2\n"
    );
    assert_eq!(
        fs::read_to_string(&win2_file).unwrap(),
        "window 2 line 1\nwindow 2 line 2\n"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[derive(Clone, Default)]
struct CsvFormat;

impl FileFormat<(String, i64)> for CsvFormat {
    fn write_header(&self, writer: &mut dyn std::io::Write) -> Result {
        Ok(writeln!(writer, "name,score")?)
    }

    fn write_element(&self, element: &(String, i64), writer: &mut dyn std::io::Write) -> Result {
        Ok(writeln!(writer, "{},{}", element.0, element.1)?)
    }
}

#[tokio::test]
async fn test_write_files_custom_csv_format() {
    let temp_dir = std::env::temp_dir().join(format!("beam_csv_sink_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();
    let out_file = temp_dir.join("scores.csv");

    let p = Pipeline::new();
    p.apply(
        "Create",
        Create::new(vec![
            ("Alice".to_string(), 100i64),
            ("Bob".to_string(), 85i64),
        ]),
    )
    .apply(
        "WriteFiles",
        WriteFiles::new(out_file.to_str().unwrap(), FormatSink::new(CsvFormat)).without_sharding(),
    );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    assert_eq!(
        fs::read_to_string(&out_file).unwrap(),
        "name,score\nAlice,100\nBob,85\n"
    );

    let _ = fs::remove_dir_all(&temp_dir);
}

#[derive(Clone, Default)]
struct CsvRecordReader;

impl FileRecordReader<(String, i64)> for CsvRecordReader {
    fn read_records(
        &self,
        fs: &dyn FileSystem,
        file: &str,
        tracker: &OffsetRangeTracker,
        ctx: &mut ProcessContext<'_, (String, i64)>,
    ) -> Result {
        let rest = tracker.current_restriction();
        if tracker.try_claim(&rest.start) {
            let mut handle = fs.open_read(file)?;
            let mut content = String::new();
            handle.read_to_string(&mut content)?;

            for (idx, line) in content.lines().enumerate() {
                if idx == 0 && line.starts_with("name,score") {
                    continue;
                }
                if let Some((name, score_str)) = line.split_once(',')
                    && let Ok(score) = score_str.trim().parse::<i64>()
                {
                    ctx.emit((name.to_string(), score))?;
                }
            }
            let _ = tracker.try_claim(&rest.end);
        }
        Ok(())
    }
}

#[tokio::test]
async fn test_filebasedsource_custom_csv_format() {
    let temp_dir = std::env::temp_dir().join(format!("beam_csv_source_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();
    let in_file = temp_dir.join("scores.csv");
    let out_file = temp_dir.join("formatted.txt");

    fs::write(&in_file, "name,score\nAlice,100\nBob,85\n").unwrap();

    let p = Pipeline::new();
    p.apply(
        "FileBasedSource",
        FileBasedSource::new(in_file.to_str().unwrap(), CsvRecordReader),
    )
    .map("Format", |(name, score): (String, i64)| {
        format!("{name}={score}")
    })
    .apply(
        "TextIO.Write",
        textio::Write::new(out_file.to_str().unwrap()).without_sharding(),
    );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    assert!(out_file.exists());
    let mut lines: Vec<String> = fs::read_to_string(&out_file)
        .unwrap()
        .lines()
        .map(|s| s.to_string())
        .collect();
    lines.sort();
    assert_eq!(lines, vec!["Alice=100", "Bob=85"]);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_textio_non_windowed_writes_on_windowed_collection() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_rewindow_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();
    let out_file = temp_dir.join("collapsed_output.txt");

    let p = Pipeline::new();
    p.apply(
        "Create",
        Create::new(vec![
            ("window 1 line".to_string(), 1_000i64),
            ("window 2 line".to_string(), 11_000i64),
        ]),
    )
    .par_do("AssignTimestamps", AssignStringTimestampDoFn)
    .apply(
        "WindowInto",
        WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
    )
    // Without .with_windowed_writes(), WriteFiles collapses non-global windows
    // into GlobalWindows with RewindowIntoGlobal.
    .apply(
        "TextIO.Write",
        textio::Write::new(out_file.to_str().unwrap()).without_sharding(),
    );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    assert!(out_file.exists(), "Collapsed file must exist");
    let content = fs::read_to_string(&out_file).unwrap();
    let mut lines: Vec<&str> = content.lines().collect();
    lines.sort();
    assert_eq!(lines, vec!["window 1 line", "window 2 line"]);

    let _ = fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_unbounded_write_without_windowed_writes_is_a_construction_error() {
    let p = Pipeline::new();
    let coder_id = p.register_coder("beam:coder:string_utf8:v1", vec![]);
    let unbounded =
        p.add_pcollection::<String>("unbounded_stream", &coder_id, IsBounded::Unbounded);

    unbounded.apply(
        "TextIO.Write",
        textio::Write::new("/tmp/any.txt").without_sharding(),
    );
    let err = p.validate().unwrap_err();
    assert!(
        err.to_string()
            .contains("an unbounded PCollection must be written with windowed writes"),
        "{err}"
    );
}

#[test]
fn test_unbounded_write_after_a_map_without_windowed_writes_is_a_construction_error() {
    let p = Pipeline::new();
    p.apply("Events", TestStream::new().add_elements(["x".to_string()]))
        .apply("Upper", Map::new(|line: String| line.to_uppercase()))
        .apply(
            "TextIO.Write",
            textio::Write::new("/tmp/any.txt").without_sharding(),
        );
    let err = p.validate().unwrap_err();
    assert!(
        err.to_string()
            .contains("an unbounded PCollection must be written with windowed writes"),
        "{err}"
    );
}

#[tokio::test]
async fn test_textio_custom_pane_filename_policy() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_pane_policy_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();
    let out_file = temp_dir.join("data.txt");

    let p = Pipeline::new();
    p.apply(
        "Create",
        Create::new(vec![
            ("line 1".to_string(), 1_000i64),
            ("line 2".to_string(), 11_000i64),
        ]),
    )
    .par_do("AssignTimestamps", AssignStringTimestampDoFn)
    .apply(
        "WindowInto",
        WindowInto::new(FixedWindows::of(Duration::from_secs(10))),
    )
    .apply(
        "TextIO.Write",
        textio::Write::new(out_file.to_str().unwrap())
            .without_sharding()
            .with_windowed_writes()
            .with_filename_policy(|base: &str, ctx: &FileNamingContext<'_>| {
                let win_str = ctx
                    .window()
                    .map(|w| format!("{}-{}", w.start.as_millis(), w.end.as_millis()))
                    .unwrap_or_default();
                let pane_idx = ctx.pane().and_then(|p| p.index()).map_or(-1, |i| i as i64);
                format!("{base}.w_{win_str}.p_{pane_idx}{}", ctx.shard_string())
            }),
    );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    let win1 = temp_dir.join("data.txt.w_0-10000.p_0");
    let win2 = temp_dir.join("data.txt.w_10000-20000.p_0");

    assert!(win1.exists(), "File {win1:?} must exist");
    assert!(win2.exists(), "File {win2:?} must exist");
    assert_eq!(fs::read_to_string(&win1).unwrap(), "line 1\n");
    assert_eq!(fs::read_to_string(&win2).unwrap(), "line 2\n");

    let _ = fs::remove_dir_all(&temp_dir);
}

#[tokio::test]
async fn test_textio_triggered_early_pane_writes() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_early_pane_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();
    let out_file = temp_dir.join("output.txt");

    let composite_trigger =
        Trigger::after_end_of_window().with_early_firings(Trigger::after_count(2));

    let p = Pipeline::new();
    p.apply(
        "Create",
        Create::new(vec![
            ("k".to_string(), "elem1".to_string()),
            ("k".to_string(), "elem2".to_string()),
            ("k".to_string(), "elem3".to_string()),
            ("k".to_string(), "elem4".to_string()),
        ]),
    )
    .apply(
        "WindowInto",
        WindowInto::new(GlobalWindows)
            .with_trigger(composite_trigger)
            .with_accumulation_mode(AccumulationMode::Accumulating),
    )
    .group_by_key("GroupEarly")
    .flat_map(
        "FlattenValues",
        |(_k, values): (String, BeamIterable<String>)| {
            values.into_vec().expect("group values decode")
        },
    )
    .apply(
        "TextIO.Write",
        textio::Write::new(out_file.to_str().unwrap())
            .without_sharding()
            .with_windowed_writes(),
    );

    let res = p.run_with_runner(&PrismRunner::new()).await;
    assert!(res.is_ok(), "PrismRunner failed: {res:?}");

    let mut entries = fs::read_dir(&temp_dir)
        .unwrap()
        .filter_map(|e| e.ok().map(|e| e.file_name().to_string_lossy().into_owned()))
        .collect::<Vec<_>>();
    entries.sort();

    // Each pane has its own file. How many early panes fire depends on how the runner
    // bundles the input; each holds at least the two elements that fired it, and the panes
    // accumulate, so the on-time pane holds all four.
    let read_lines = |name: &str| -> Vec<String> {
        let mut lines: Vec<String> = fs::read_to_string(temp_dir.join(name))
            .unwrap()
            .lines()
            .map(String::from)
            .collect();
        lines.sort();
        lines
    };
    let all = ["elem1", "elem2", "elem3", "elem4"].map(String::from);
    let (early, on_time): (Vec<&String>, Vec<&String>) = entries
        .iter()
        .partition(|name| name.ends_with("-early.txt"));
    assert_eq!(on_time.len(), 1, "one on-time file: {entries:?}");
    assert!(on_time[0].ends_with("-on-time.txt"), "{entries:?}");
    assert_eq!(read_lines(on_time[0]), all);
    assert!(
        !early.is_empty(),
        "the early trigger must fire: {entries:?}"
    );
    for name in early {
        let lines = read_lines(name);
        assert!(lines.len() >= 2, "{name}: {lines:?}");
        assert!(
            lines.iter().all(|line| all.contains(line)),
            "{name}: {lines:?}"
        );
    }

    let _ = fs::remove_dir_all(&temp_dir);
}

/// The `build_textio_roundtrip` pipeline on Prism: what was read is asserted in the
/// pipeline, and the written file is read back by the driver.
#[tokio::test]
async fn test_textio_roundtrip_validation() {
    let temp_dir =
        std::env::temp_dir().join(format!("beam_textio_validate_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();
    let p = tests::test_pipeline();
    tests::build_textio_roundtrip(&p, &temp_dir);
    tests::Expectation::Succeeds.check("textio_roundtrip", p.run_with(&PrismRunner::new()).await);
    let read_back = fs::read_to_string(temp_dir.join("output.txt")).unwrap();
    assert_eq!(read_back, tests::TEXTIO_ROUNDTRIP_CONTENTS);
    let _ = fs::remove_dir_all(&temp_dir);
}
