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

//! Side inputs: counts the words that are longer than the mean word length, without the
//! stopwords.
//!
//! The pipeline computes the mean length (a singleton side input) from the words
//! themselves. The stopwords come from the options (an iterable side input). A `DoFn`
//! struct keeps both views as fields.

use beam::prelude::*;
use clap::Args as ClapArgs;
use serde::{Deserialize, Serialize};

/// Options for this pipeline, on top of the standard Beam options.
#[derive(ClapArgs, Serialize, Deserialize, Clone, Debug)]
#[command(
    name = "side_inputs",
    about = "Apache Beam side inputs example",
    version
)]
pub struct Args {
    /// Path of the file to read from.
    #[arg(
        long,
        default_value = "gs://apache-beam-samples/shakespeare/kinglear.txt"
    )]
    pub input: String,

    /// Path of the file to write to.
    #[arg(long, default_value = "/tmp/side_inputs.txt")]
    pub output: String,

    /// Words to skip, separated by commas.
    #[arg(
        long,
        value_delimiter = ',',
        default_value = "should,would,though,before,within"
    )]
    pub stopwords: Vec<String>,
}

impl PipelineOptionGroup for Args {}

/// Counts the lowercase words longer than the mean word length that are not `stopwords`.
pub struct CountLongWords {
    pub stopwords: Vec<String>,
}

impl PTransform<PCollection<String>> for CountLongWords {
    type Output = PCollection<(String, i64)>;

    fn expand(&self, lines: &PCollection<String>) -> Self::Output {
        let words = lines.flat_map("ExtractWords", |line: String| {
            wordcount::extract_words(&line)
                .map(|word| word.to_lowercase())
                .collect::<Vec<_>>()
        });
        let mean_len = words
            .map("WordLength", |word: String| word.len() as i64)
            .mean_globally("MeanLength")
            .as_singleton();
        // `Create` owns its elements and `expand` only borrows `self`.
        let stopwords = lines
            .pipeline()
            .apply("Stopwords", Create::new(self.stopwords.clone()))
            .as_iter();

        words
            .par_do(
                "KeepLongWords",
                KeepLongWordsFn {
                    mean_len,
                    stopwords,
                },
            )
            .count_per_element("CountWords")
    }
}

/// Keeps the words longer than `mean_len` that are not in `stopwords`.
#[derive(Clone, Serialize, Deserialize, Register)]
struct KeepLongWordsFn {
    mean_len: SingletonView<f64>,
    stopwords: IterView<String>,
}

impl DoFn for KeepLongWordsFn {
    type In = String;
    type Out = String;

    fn side_inputs(&self) -> Vec<&dyn AnySideInput> {
        vec![&self.mean_len, &self.stopwords]
    }

    fn process_element(&mut self, word: String, ctx: &mut ProcessContext<'_, String>) -> Result {
        let mean_len = ctx.side_input(&self.mean_len)?;
        let is_stopword = ctx
            .side_input(&self.stopwords)?
            .iter()?
            .try_fold(false, |found, stopword| {
                Ok::<_, Error>(found || stopword? == word)
            })?;
        if word.len() as f64 > mean_len && !is_stopword {
            ctx.emit(word)
        } else {
            Ok(())
        }
    }
}
