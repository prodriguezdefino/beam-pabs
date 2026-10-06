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

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use std::fs;
use testutils::read_shards;
use wordcount::{Args, CountWords, build_pipeline, extract_words, format_counts};

const WORDS: [&str; 6] = ["hi there", "hi", "hi sue bob", "hi sue", "", "bob hi"];

const COUNTS: [&str; 4] = ["hi: 5", "there: 1", "sue: 2", "bob: 2"];

/// Example test that checks a single step of the pipeline.
///
/// The input comes from an in-memory `Create`, and `passert` checks the
/// output inside the pipeline, so the test runs on any runner.
#[tokio::test]
async fn test_extract_words_step() {
    let p = TestPipeline::new();

    let lines = [" some  input  words ", " ", " cool ", " foo", " bar"].map(String::from);
    let words = p
        .apply(Create::new("Create", lines.to_vec()))
        .par_do_fn("ExtractWords", |line: String, out| {
            extract_words(&line).try_for_each(|word| out.emit(word))
        });

    passert::that("AssertWords", &words)
        .contains_in_any_order(["some", "input", "words", "cool", "foo", "bar"].map(String::from));

    p.run().await.expect("ExtractWords assertions must pass");
}

/// Example test that checks a composite `PTransform` on in-memory input.
#[tokio::test]
async fn test_count_words() {
    let p = TestPipeline::new();

    let output = p
        .apply(Create::new("Create", WORDS.map(String::from).to_vec()))
        .apply(CountWords)
        .map("FormatCounts", |(word, count): (String, i64)| {
            format_counts(&word, count)
        });

    passert::that("AssertOutput", &output).contains_in_any_order(COUNTS.map(String::from));

    p.run().await.expect("CountWords assertions must pass");
}

#[test]
fn test_extract_words() {
    assert_eq!(
        extract_words("To be, or not to be: that is the question!").collect::<Vec<_>>(),
        vec![
            "To", "be", "or", "not", "to", "be", "that", "is", "the", "question"
        ]
    );
    assert_eq!(
        extract_words("'tis a consummation devoutly to be wish'd.").collect::<Vec<_>>(),
        vec![
            "tis",
            "a",
            "consummation",
            "devoutly",
            "to",
            "be",
            "wish",
            "d"
        ]
    );
    assert_eq!(
        extract_words("rev 1024, año_2024 Straße").collect::<Vec<_>>(),
        vec!["rev", "año", "Straße"],
        "digits and underscores separate words; non-ASCII letters are kept"
    );
    assert_eq!(
        extract_words("cafe\u{301} Ⅻ").collect::<Vec<_>>(),
        vec!["cafe"],
        "combining marks and letter numbers are not in Unicode \\p{{L}}"
    );
}

#[test]
fn test_pipeline_construction() {
    let args = Args {
        input: "/tmp/input.txt".to_string(),
        output: "/tmp/output.txt".to_string(),
    };
    let pipeline = build_pipeline(&PipelineOptions::default(), &args);
    pipeline.validate().expect("Pipeline DAG must be valid");

    let proto = pipeline.to_proto();
    let components = proto.components.expect("Components must exist");

    // Composite TextIO.Read (composite parent + Impulse, Match, and SDF Read),
    // ExtractWords, CountWords/PairWithOne,
    // CountWords/Sum (composite + 3 combine subtransform stages),
    // FormatCounts, TextIO.Write (composite + WriteBundles, Impulse and
    // FinalizeUnwindowed).
    assert_eq!(components.transforms.len(), 15);
    assert_eq!(proto.root_transform_ids.len(), 6);

    let combine_composite = components
        .transforms
        .values()
        .find(|t| t.unique_name == "CountWords/Sum" || t.unique_name.starts_with("CountWords/Sum_"))
        .expect("CountWords/Sum composite transform must exist");
    assert_eq!(
        combine_composite.spec.as_ref().map(|s| s.urn.as_str()),
        Some(beam::pipeline::URN_COMBINE_PER_KEY)
    );
    assert_eq!(combine_composite.subtransforms.len(), 3);

    // Counting must aggregate before the shuffle rather than grouping every
    // occurrence and summing afterwards.
    let names: Vec<&str> = components
        .transforms
        .values()
        .map(|t| t.unique_name.as_str())
        .collect();

    for stage in [
        "CountWords/Sum/PartialCombine",
        "CountWords/Sum/GroupAccumulators",
        "CountWords/Sum/MergeAccumulators",
    ] {
        assert!(
            names.iter().any(|n| n.starts_with(stage)),
            "expected a '{stage}' stage, found {names:?}"
        );
    }
}

#[tokio::test]
async fn test_wordcount_on_prism_runner() {
    let temp_dir = std::env::temp_dir().join(format!("beam_wordcount_{}", std::process::id()));
    fs::create_dir_all(&temp_dir).unwrap();

    let in_file = temp_dir.join("input.txt");
    let out_file = temp_dir.join("output.txt");
    fs::write(&in_file, "to be, or not to be:\nthat is the question:\n").unwrap();

    let args = Args {
        input: in_file.to_str().unwrap().to_string(),
        output: out_file.to_str().unwrap().to_string(),
    };
    let pipeline = build_pipeline(&PipelineOptions::default(), &args);

    let result = pipeline
        .run()
        .await
        .expect("WordCount must succeed on the prism runner");
    assert_eq!(result.state, "DONE");

    let output = read_shards(&out_file);
    let lines: Vec<&str> = output.lines().collect();
    assert!(lines.contains(&"to: 2"));
    assert!(lines.contains(&"be: 2"));
    assert!(lines.contains(&"question: 1"));

    let _ = fs::remove_dir_all(&temp_dir);
}
