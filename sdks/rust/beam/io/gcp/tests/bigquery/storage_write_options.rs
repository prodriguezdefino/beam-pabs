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

//! The `keep`, `drop` and `only` options of the native Storage Write connector.

use gcp::bigquery::storage::write::WriteOptions;
use gcp::bigquery::storage::write::projection::FieldSelection;

#[test]
fn at_most_one_of_keep_drop_and_only() {
    let options = || WriteOptions::new("project.dataset.table");
    for valid in [
        options(),
        options().with_keep(["a"]),
        options().with_drop(["a"]),
        options().with_only("a"),
    ] {
        assert!(valid.validate().is_ok(), "{valid:?}");
    }
    for invalid in [
        options().with_keep(["a"]).with_drop(["b"]),
        options().with_keep(["a"]).with_only("b"),
        options().with_drop(["a"]).with_only("b"),
    ] {
        let err = invalid.validate().unwrap_err().to_string();
        assert!(err.contains("at most one of keep, drop and only"), "{err}");
    }
}

#[test]
fn field_selection_follows_the_option_that_is_set() {
    let options = WriteOptions::new("project.dataset.table");
    assert_eq!(options.field_selection().unwrap(), FieldSelection::All);
    assert_eq!(
        options
            .clone()
            .with_keep(["a", "b"])
            .field_selection()
            .unwrap(),
        FieldSelection::Keep(vec!["a".into(), "b".into()])
    );
    assert_eq!(
        options.clone().with_drop(["a"]).field_selection().unwrap(),
        FieldSelection::Drop(vec!["a".into()])
    );
    assert_eq!(
        options.with_only("p").field_selection().unwrap(),
        FieldSelection::Only("p".into())
    );
}
