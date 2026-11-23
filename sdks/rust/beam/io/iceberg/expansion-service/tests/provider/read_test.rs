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

//! Tests of the `filter` parser and the configuration of the Iceberg read provider.

use beam::io::managed::urns;
use beam_expansion::SchemaTransformProvider;
use beam_io_iceberg_expansion::{
    IcebergReadConfig, IcebergReadProvider, IcebergWriteProvider, parse_filter,
};
use iceberg::expr::Reference;
use iceberg::spec::Datum;

use crate::common::{CATALOG_URI, catalog_props};

fn read_config() -> IcebergReadConfig {
    IcebergReadConfig {
        table: "db.events".to_string(),
        catalog_properties: Some(catalog_props(&[("uri", CATALOG_URI)])),
        ..Default::default()
    }
}

fn read_error(config: IcebergReadConfig) -> String {
    config
        .read_options()
        .expect_err("configuration is not valid")
}

#[test]
fn filter_comparisons_and_logic() {
    let id = || Reference::new("id");
    let name = || Reference::new("name");
    let cases = [
        ("id = 5", id().equal_to(Datum::long(5))),
        ("id <> 5", id().not_equal_to(Datum::long(5))),
        ("5 < id", id().greater_than(Datum::long(5))),
        ("id <= -2", id().less_than_or_equal_to(Datum::long(-2))),
        ("2.5 >= id", id().less_than_or_equal_to(Datum::double(2.5))),
        (
            "id > 1 AND (name = 'a' OR NOT name IS NULL)",
            id().greater_than(Datum::long(1))
                .and(name().equal_to(Datum::string("a")).or(!name().is_null())),
        ),
        ("name IS NOT NULL", name().is_not_null()),
        ("id IN (1, 2)", id().is_in([Datum::long(1), Datum::long(2)])),
        ("id NOT IN (3)", id().is_not_in([Datum::long(3)])),
        ("name LIKE 'ab%'", name().starts_with(Datum::string("ab"))),
        (
            "name NOT LIKE 'ab%'",
            name().not_starts_with(Datum::string("ab")),
        ),
        ("name LIKE 'ab'", name().equal_to(Datum::string("ab"))),
        (
            "t.flag = true",
            Reference::new("t.flag").equal_to(Datum::bool(true)),
        ),
    ];
    for (sql, expected) in cases {
        assert_eq!(parse_filter(sql), Ok(expected), "{sql}");
    }
}

#[test]
fn filter_rejects_other_forms() {
    for sql in [
        "id = ",
        "id = 1 extra",
        "id + 1 = 2",
        "id = other",
        "1 = 2",
        "name LIKE '%ab'",
        "name LIKE 'a_b'",
        "name LIKE 'a%b%'",
        "id * 2",
    ] {
        let error = parse_filter(sql).expect_err(sql);
        assert!(
            error.starts_with(&format!("Invalid filter '{sql}'")),
            "{error}"
        );
    }
}

#[test]
fn read_options_accept_a_full_configuration() {
    let config = IcebergReadConfig {
        keep: Some(vec!["id".to_string()]),
        filter: Some("id > 3".to_string()),
        snapshot_id: Some(7),
        batch_size: Some(100),
        ..read_config()
    };
    config.read_options().expect("configuration is valid");
}

#[test]
fn read_options_reject_conflicts_and_bad_values() {
    let cases = [
        (
            IcebergReadConfig {
                table: " ".to_string(),
                ..read_config()
            },
            "Set table",
        ),
        (
            IcebergReadConfig {
                keep: Some(vec!["a".to_string()]),
                drop: Some(vec!["b".to_string()]),
                ..read_config()
            },
            "Set at most one of keep and drop",
        ),
        (
            IcebergReadConfig {
                snapshot_id: Some(1),
                as_of_timestamp_millis: Some(2),
                ..read_config()
            },
            "Set at most one of snapshot_id and as_of_timestamp_millis",
        ),
        (
            IcebergReadConfig {
                batch_size: Some(0),
                ..read_config()
            },
            "batch_size must be positive, got 0",
        ),
        (
            IcebergReadConfig {
                config_properties: Some(catalog_props(&[("a", "b")])),
                ..read_config()
            },
            "config_properties is not supported",
        ),
        (
            IcebergReadConfig {
                filter: Some("id +".to_string()),
                ..read_config()
            },
            "Invalid filter 'id +'",
        ),
    ];
    for (config, expected) in cases {
        let error = read_error(config);
        assert!(error.starts_with(expected), "{error}");
    }
}

#[test]
fn providers_answer_the_managed_iceberg_urns() {
    assert_eq!(
        IcebergReadProvider.managed_identifiers(),
        [urns::ICEBERG_READ]
    );
    assert_eq!(
        IcebergWriteProvider.managed_identifiers(),
        [urns::ICEBERG_WRITE]
    );
    assert!(IcebergReadProvider.input_tags().is_empty());
    assert_eq!(IcebergReadProvider.output_tags(), ["output", "errors"]);
}
