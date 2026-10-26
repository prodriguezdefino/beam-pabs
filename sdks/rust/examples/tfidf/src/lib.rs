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

//! TF-IDF: how important each word is to each document of a collection.
//!
//! For a word `w` in a document `d`:
//!
//! - the term frequency `tf(w, d)` is the number of times that `w` occurs in `d`, divided by
//!   the number of words in `d`;
//! - the document frequency `df(w)` is the number of documents that contain `w`, divided by
//!   the number of documents;
//! - `tf-idf(w, d) = tf(w, d) * ln(1 / df(w))`.
//!
//! The pipeline shows:
//!
//! - `distinct` for the distinct documents and the distinct `(document, word)` pairs;
//! - `count_per_element` and `count_globally`;
//! - a singleton side input (the number of documents) with `map_with_side_inputs`;
//! - `inner_join` to bring together the counts of a document and of a word;
//! - TFRecord output: one `tf.train.Example` per document, which TensorFlow reads with
//!   `tf.data.TFRecordDataset` and `tf.io.parse_single_example`;
//! - composite transforms ([`ComputeTfIdf`], [`WriteExamples`]) that keep the steps reusable
//!   and testable.

use beam::io::tfrecord::example::{Example, Feature};
use beam::io::tfrecord::tfrecordio;
use beam::prelude::*;
use clap::Args as ClapArgs;
use prost::Message;
use serde::{Deserialize, Serialize};

/// The feature of an [`Example`] with the document name.
pub const DOCUMENT_FEATURE: &str = "document";
/// The feature of an [`Example`] with the words of the document, in alphabetical order.
pub const WORDS_FEATURE: &str = "words";
/// The feature of an [`Example`] with the TF-IDF of each word, in the order of the words.
pub const TFIDF_FEATURE: &str = "tfidf";

/// The options of the example.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(name = "tfidf", about = "Apache Beam TF-IDF example", version)]
pub struct Args {
    /// Path or glob pattern of the documents, one file per document.
    #[arg(long, default_value = "gs://apache-beam-samples/shakespeare/*")]
    pub input: String,

    /// Prefix of the TFRecord files of `tf.train.Example`s, one per document.
    #[arg(long, default_value = "/tmp/tfidf.tfrecord")]
    pub output: String,
}

impl PipelineOptionGroup for Args {}

/// The lowercase words of `line`: the runs of letters and digits.
pub fn words(line: &str) -> impl Iterator<Item = String> + '_ {
    line.split(|c: char| !c.is_alphanumeric())
        .filter(|word| !word.is_empty())
        .map(str::to_lowercase)
}

/// The TF-IDF of a word with term frequency `tf` and document frequency `df`.
pub fn tf_idf(tf: f64, df: f64) -> f64 {
    tf * (1.0 / df).ln()
}

/// Computes `(word, (document, tf-idf))` from `(document, line)` pairs.
pub struct ComputeTfIdf;

impl PTransform<PCollection<(String, String)>> for ComputeTfIdf {
    type Output = PCollection<(String, (String, f64))>;

    fn expand(&self, document_lines: &PCollection<(String, String)>) -> Self::Output {
        let total_documents = document_lines
            .map("GetDocuments", |(document, _): (String, String)| document)
            .distinct("DistinctDocuments")
            .count_globally("CountDocuments")
            .as_singleton();

        // Each pair is its own element, so it owns a copy of the document name.
        let document_words =
            document_lines.flat_map("SplitWords", |(document, line): (String, String)| {
                words(&line)
                    .map(|word| (document.clone(), word))
                    .collect::<Vec<_>>()
            });

        // The number of documents that contain each word.
        let word_documents = document_words
            .distinct("DistinctDocumentWords")
            .map("GetWords", |(_, word): (String, String)| word)
            .count_per_element("CountDocumentsPerWord");

        // The number of words in each document.
        let document_word_totals = document_words
            .map("GetWordDocuments", |(document, _): (String, String)| {
                document
            })
            .count_per_element("CountWordsPerDocument");

        // The number of times that each word occurs in each document.
        let document_word_counts = document_words
            .count_per_element("CountDocumentWordPairs")
            .map(
                "KeyByDocument",
                |((document, word), count): ((String, String), i64)| (document, (word, count)),
            );

        let term_frequencies = document_word_totals
            .inner_join("JoinWordTotals", &document_word_counts)
            .map(
                "ComputeTermFrequencies",
                |(document, (total, (word, count))): (String, (i64, (String, i64)))| {
                    (word, (document, count as f64 / total as f64))
                },
            );

        let document_frequencies = word_documents.map_with_side_inputs(
            "ComputeDocumentFrequencies",
            &total_documents,
            |(word, documents): (String, i64), total: i64| {
                Ok((word, documents as f64 / total as f64))
            },
        );

        term_frequencies
            .inner_join("JoinDocumentFrequencies", &document_frequencies)
            .map(
                "ComputeTfIdf",
                |(word, ((document, tf), df)): (String, ((String, f64), f64))| {
                    (word, (document, tf_idf(tf, df)))
                },
            )
    }
}

/// The `tf.train.Example` of a document: its name, its words in alphabetical order and the
/// TF-IDF of each word.
pub fn document_example(document: &str, mut scores: Vec<(String, f64)>) -> Example {
    scores.sort_by(|left, right| left.0.cmp(&right.0));
    let (words, tfidf): (Vec<String>, Vec<f32>) = scores
        .into_iter()
        .map(|(word, score)| (word, score as f32))
        .unzip();
    Example::new([
        (DOCUMENT_FEATURE, Feature::bytes([document])),
        (WORDS_FEATURE, Feature::bytes(words)),
        (TFIDF_FEATURE, Feature::floats(tfidf)),
    ])
}

/// Groups `(word, (document, tf-idf))` by document and writes one `tf.train.Example` per
/// document (see [`document_example`]) to TFRecord files.
pub struct WriteExamples {
    output: String,
}

impl WriteExamples {
    /// Writes to the files with the prefix `output`.
    pub fn new(output: impl Into<String>) -> Self {
        Self {
            output: output.into(),
        }
    }
}

impl PTransform<PCollection<(String, (String, f64))>> for WriteExamples {
    type Output = PCollection<String>;

    fn expand(&self, scores: &PCollection<(String, (String, f64))>) -> PCollection<String> {
        scores
            .map(
                "KeyScoresByDocument",
                |(word, (document, score)): (String, (String, f64))| (document, (word, score)),
            )
            .group_by_key("GroupScoresByDocument")
            .par_do_fn(
                "BuildExamples",
                |(document, scores): (String, BeamIterable<(String, f64)>), out| {
                    out.emit(document_example(&document, scores.into_vec()?).encode_to_vec())
                },
            )
            .apply("WriteExamples", tfrecordio::Write::new(&self.output))
    }
}

/// Builds the pipeline from [`Args`], run with `options`: reads the documents of
/// `args.input`, computes the TF-IDF of each word and writes one `tf.train.Example` per
/// document to `args.output`.
pub fn build_pipeline(options: &PipelineOptions, args: &Args) -> Pipeline {
    let p = Pipeline::create(options);
    p.apply("ReadDocuments", textio::ReadWithFilename::new(&args.input))
        .apply("ComputeTfIdf", ComputeTfIdf)
        .apply("WriteExamples", WriteExamples::new(&args.output));
    p
}
