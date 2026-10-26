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

//! Tests for the TF-IDF example: the word splitting, `ComputeTfIdf` on Prism over two small
//! documents, and the `tf.train.Example`s that `WriteExamples` writes as TFRecords.

use beam::io::tfrecord::example::Example;
use beam::io::tfrecord::tfrecordio;
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use prost::Message;
use tfidf::{
    ComputeTfIdf, DOCUMENT_FEATURE, TFIDF_FEATURE, WORDS_FEATURE, WriteExamples, tf_idf, words,
};

fn s(value: &str) -> String {
    value.to_string()
}

#[test]
fn words_are_lowercase_runs_of_letters_and_digits() {
    assert_eq!(
        words("To be, or NOT to-be: 2b").collect::<Vec<_>>(),
        ["to", "be", "or", "not", "to", "be", "2b"]
    );
    assert_eq!(words("  ,;  ").count(), 0);
}

/// Two documents: `d1` with the lines `a b` and `A`, and `d2` with the line `a, c`.
fn documents() -> Vec<(String, String)> {
    vec![(s("d1"), s("a b")), (s("d1"), s("A")), (s("d2"), s("a, c"))]
}

/// `d1` has the words `a b a` and `d2` the words `a c`. `a` is in both documents, so its
/// TF-IDF is zero; `b` and `c` are each in half of the documents.
#[tokio::test]
async fn compute_tf_idf_scores_each_word_of_each_document() {
    let p = TestPipeline::new();
    let scores = p
        .apply("Documents", Create::new(documents()))
        .apply("ComputeTfIdf", ComputeTfIdf);

    passert::that("TfIdf", &scores).contains_in_any_order([
        (s("a"), (s("d1"), tf_idf(2.0 / 3.0, 1.0))),
        (s("a"), (s("d2"), tf_idf(1.0 / 2.0, 1.0))),
        (s("b"), (s("d1"), tf_idf(1.0 / 3.0, 0.5))),
        (s("c"), (s("d2"), tf_idf(1.0 / 2.0, 0.5))),
    ]);

    p.run().await.expect("pipeline and assertions pass");
}

/// The document, the words and the scores of an encoded `tf.train.Example`.
fn decode(bytes: Vec<u8>) -> (String, Vec<String>, Vec<f64>) {
    let example = Example::decode(bytes.as_slice()).expect("a tf.train.Example");
    let text = |name| -> Vec<String> {
        example
            .bytes(name)
            .expect("a bytes feature")
            .iter()
            .map(|value| String::from_utf8_lossy(value).into_owned())
            .collect()
    };
    let document = text(DOCUMENT_FEATURE).concat();
    let scores = example
        .floats(TFIDF_FEATURE)
        .expect("a float feature")
        .iter()
        .map(|&score| f64::from(score))
        .collect();
    (document, text(WORDS_FEATURE), scores)
}

/// A score as the `float` that a `tf.train.Example` holds.
fn as_float(score: f64) -> f64 {
    f64::from(score as f32)
}

#[tokio::test]
async fn write_examples_writes_one_tf_example_per_document() {
    let dir = tempfile::tempdir().expect("create a temporary directory");
    let output = dir.path().join("examples.tfrecord").display().to_string();

    let write = TestPipeline::new();
    write
        .apply("Documents", Create::new(documents()))
        .apply("ComputeTfIdf", ComputeTfIdf)
        .apply("WriteExamples", WriteExamples::new(output));
    write.run().await.expect("write pipeline");

    let read = TestPipeline::new();
    let examples = read
        .apply(
            "ReadExamples",
            tfrecordio::Read::new(dir.path().join("examples-*.tfrecord").display().to_string()),
        )
        .apply("Decode", Map::new(decode));
    passert::that("Examples", &examples).contains_in_any_order([
        (
            s("d1"),
            vec![s("a"), s("b")],
            vec![
                as_float(tf_idf(2.0 / 3.0, 1.0)),
                as_float(tf_idf(1.0 / 3.0, 0.5)),
            ],
        ),
        (
            s("d2"),
            vec![s("a"), s("c")],
            vec![
                as_float(tf_idf(1.0 / 2.0, 1.0)),
                as_float(tf_idf(1.0 / 2.0, 0.5)),
            ],
        ),
    ]);
    read.run().await.expect("read pipeline");
}
