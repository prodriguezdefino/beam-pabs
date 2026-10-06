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

use beam::io::gcp::bigtable::{BigtableColumn, mutation_schema};
use bigtable_wordcount::{COUNT_QUALIFIER, count_mutation, decode_count, extract_words};

#[test]
fn test_extract_words() {
    let words: Vec<_> = extract_words("  King Lear, act 1: 'tis!  ").collect();
    assert_eq!(words, ["King", "Lear", "act", "tis"]);
}

#[test]
fn test_count_mutation_round_trips_through_decode() {
    let row = count_mutation("counts", "lear", 42).to_row();
    assert_eq!(row.schema(), &mutation_schema());
    assert_eq!(row.get_string("type").unwrap(), Some("SetCell"));
    assert_eq!(row.get_bytes("key").unwrap(), Some(&b"lear"[..]));
    assert_eq!(row.get_bytes("value").unwrap(), Some(&b"42"[..]));

    // Simulated cell returned by the read transform.
    let column = BigtableColumn {
        key: b"lear".to_vec(),
        family_name: "counts".into(),
        column_qualifier: COUNT_QUALIFIER.as_bytes().to_vec(),
        cells: vec![beam::io::gcp::bigtable::BigtableCell {
            value: b"42".to_vec(),
            timestamp_micros: 1,
        }],
    };
    assert_eq!(decode_count("counts", &column), Some(("lear".into(), 42)));
}

#[test]
fn test_decode_count_ignores_other_columns() {
    let column = |family: &str, qualifier: &[u8], value: &[u8]| BigtableColumn {
        key: b"w".to_vec(),
        family_name: family.into(),
        column_qualifier: qualifier.to_vec(),
        cells: vec![beam::io::gcp::bigtable::BigtableCell {
            value: value.to_vec(),
            timestamp_micros: 1,
        }],
    };
    assert_eq!(
        decode_count("counts", &column("other", b"count", b"1")),
        None
    );
    assert_eq!(
        decode_count("counts", &column("counts", b"other", b"1")),
        None
    );
    assert_eq!(
        decode_count("counts", &column("counts", b"count", b"x")),
        None
    );
}
