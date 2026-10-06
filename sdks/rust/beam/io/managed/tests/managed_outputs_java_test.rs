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

//! Expands Managed transforms against the real Java expansion services and checks the
//! output tags Java returns match what the Rust side declares: Iceberg's `snapshots`,
//! and the error outputs configured with `with_error_handling`.
//!
//! Expansion does not contact brokers or catalogs, so no Kafka cluster or warehouse is
//! needed, but Java and the expansion service JARs are, so it is ignored by default:
//! `cargo test -p apache-beam-io-managed --test managed_outputs_java_test -- --ignored`.

use std::collections::BTreeMap;
use std::sync::Arc;

use beam::prelude::*;
use beam::schema::{Field, FieldType, FieldValue};
use managed_io::{self as managed, ManagedRead, ManagedWrite};

fn rows(p: &Pipeline, schema: &Arc<Schema>, values: Vec<FieldValue>) -> PCollection<Row> {
    let rows: Vec<Row> = values
        .into_iter()
        .map(|v| Row::new(Arc::clone(schema), vec![Some(v)]).expect("row matches schema"))
        .collect();
    p.apply(Create::new("Create", rows)).with_row_schema(schema)
}

#[test]
#[ignore = "requires Java and the Java expansion service JARs"]
fn kafka_read_and_write_return_the_configured_error_outputs() {
    let p = Pipeline::new();

    let read = ManagedRead::new("Managed Read(KAFKA)", managed::KAFKA)
        .with_config_entry("bootstrap_servers", "localhost:9092")
        .with_config_entry("topic", "events")
        .with_config_entry("format", "RAW")
        .with_error_handling("bad_records")
        .with_all_outputs()
        .try_expand(&p.begin())
        .expect("Kafka read expands");
    assert_eq!(read.tags().collect::<Vec<_>>(), ["bad_records", "output"]);
    assert!(
        !read
            .expect("bad_records")
            .expect("error output")
            .coder_id()
            .is_empty()
    );

    let payloads = Arc::new(Schema::new(vec![Field::new("payload", FieldType::bytes())]));
    let written = rows(&p, &payloads, vec![FieldValue::Bytes(b"hi".to_vec())]);
    let outputs = ManagedWrite::new("Managed Write(KAFKA)", managed::KAFKA)
        .with_config_entry("bootstrap_servers", "localhost:9092")
        .with_config_entry("topic", "out")
        .with_config_entry("format", "RAW")
        .with_error_handling("unwritable")
        .with_outputs()
        .try_expand(&written)
        .expect("Kafka write expands");
    assert_eq!(outputs.tags().collect::<Vec<_>>(), ["unwritable"]);
}

#[test]
#[ignore = "requires Java and the Java expansion service JARs"]
fn iceberg_write_returns_snapshots() {
    let warehouse = std::env::temp_dir().join("rust_managed_outputs_warehouse");
    let p = Pipeline::new();
    let words = Arc::new(Schema::new(vec![Field::new("word", FieldType::string())]));
    let input = rows(&p, &words, vec![FieldValue::String("hello".into())]);

    let outputs = ManagedWrite::new("Managed Write(ICEBERG)", managed::ICEBERG)
        .with_config_entry("table", "db.words")
        .with_config_entry("catalog_name", "local")
        .with_config_entry(
            "catalog_properties",
            BTreeMap::from([
                ("type", "hadoop".to_string()),
                ("warehouse", format!("file://{}", warehouse.display())),
            ]),
        )
        .with_outputs()
        .try_expand(&input)
        .expect("Iceberg write expands");
    assert_eq!(outputs.tags().collect::<Vec<_>>(), [managed::SNAPSHOTS]);
}
