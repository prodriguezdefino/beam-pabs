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

use std::collections::BTreeMap;

use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use iceberg_wordcount::{Args, CountWordsToRows, IcebergConfig, WordCount};

#[tokio::test]
async fn test_count_words_to_rows() {
    let p = TestPipeline::new();

    let lines = ["King Lear, act 1", "Lear: 'tis"].map(String::from);
    let counts = p
        .apply("Create", Create::new(lines.to_vec()))
        .apply("CountWordsToRows", CountWordsToRows)
        .par_do_fn("FromRow", |row: Row, out| {
            out.emit(WordCount::from_row(&row)?)
        });

    passert::that("AssertCounts", &counts).contains_in_any_order(
        [("King", 1), ("Lear", 2), ("act", 1), ("tis", 1)].map(|(word, count)| WordCount {
            word: word.to_string(),
            count,
        }),
    );

    p.run()
        .await
        .expect("CountWordsToRows assertions must pass");
}

#[test]
fn test_catalog_properties() {
    let (_, args) = beam::options::parse_from::<Args, _, _>([
        "iceberg_wordcount",
        "--warehouse=s3://bucket/warehouse",
        "--catalog_property=uri=https://catalog.example.com/?a=b",
        "--catalog_property=type=rest",
    ]);
    assert_eq!(
        IcebergConfig::from(&args).catalog_properties,
        BTreeMap::from([
            ("type", "rest"),
            ("uri", "https://catalog.example.com/?a=b"),
            ("warehouse", "s3://bucket/warehouse"),
        ]),
        "a --catalog_property replaces the value of --catalog_type"
    );
}
