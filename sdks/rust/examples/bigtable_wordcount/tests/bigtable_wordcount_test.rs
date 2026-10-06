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

use beam::io::gcp::bigtable::{BigtableCell, BigtableColumn};
use beam::prelude::*;
use beam::testing::{TestPipeline, passert};
use bigtable_wordcount::{COUNT_QUALIFIER, CountWordsToMutations, decode_count};

fn column(family: &str, qualifier: &[u8], value: &[u8]) -> BigtableColumn {
    BigtableColumn {
        key: b"lear".to_vec(),
        family_name: family.into(),
        column_qualifier: qualifier.to_vec(),
        cells: vec![BigtableCell {
            value: value.to_vec(),
            timestamp_micros: 1,
        }],
    }
}

#[tokio::test]
async fn test_count_words_to_mutations() {
    let p = TestPipeline::new();

    let lines = ["King Lear, act 1", "Lear: 'tis"].map(String::from);
    let cells = p
        .apply("Create", Create::new(lines.to_vec()))
        .apply(
            "CountWordsToMutations",
            CountWordsToMutations {
                column_family: "counts".to_string(),
            },
        )
        .par_do_fn("KeyAndValue", |row: Row, out| {
            let key = row.get_bytes("key")?.unwrap_or_default();
            let value = row.get_bytes("value")?.unwrap_or_default();
            out.emit((
                String::from_utf8(key.to_vec())?,
                String::from_utf8(value.to_vec())?,
            ))
        });

    passert::that("AssertCells", &cells).contains_in_any_order(
        [("King", "1"), ("Lear", "2"), ("act", "1"), ("tis", "1")]
            .map(|(word, count)| (word.to_string(), count.to_string())),
    );

    p.run()
        .await
        .expect("CountWordsToMutations assertions must pass");
}

#[test]
fn test_decode_count() {
    let count = COUNT_QUALIFIER.as_bytes();
    assert_eq!(
        decode_count("counts", column("counts", count, b"42")),
        Some(("lear".to_string(), 42))
    );
    assert_eq!(decode_count("counts", column("other", count, b"1")), None);
    assert_eq!(
        decode_count("counts", column("counts", b"other", b"1")),
        None
    );
    assert_eq!(decode_count("counts", column("counts", count, b"x")), None);
}
