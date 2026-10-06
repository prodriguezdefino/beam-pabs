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

//! Checks the Rust configuration rows against the schemas the Java providers advertise.
//!
//! Needs Java and downloads (or finds a locally built) I/O expansion service JAR, so it
//! is ignored by default: `cargo test -p apache-beam-io-kafka --test kafka_discovery_test -- --ignored`.

use external::ExpansionClient;
use kafka_io::{DEFAULT_EXPANSION_SERVICE, KafkaRead, KafkaWrite, URN_KAFKA_READ, URN_KAFKA_WRITE};
use model::pipeline::FieldType as ProtoFieldType;

fn proto_fields(schema: &model::pipeline::Schema) -> Vec<(String, bool)> {
    schema
        .fields
        .iter()
        .map(|f| {
            let nullable = f
                .r#type
                .as_ref()
                .is_some_and(|t: &ProtoFieldType| t.nullable);
            (f.name.clone(), nullable)
        })
        .collect()
}

fn rust_fields(row: &beam::prelude::Row) -> Vec<(String, bool)> {
    row.schema()
        .fields
        .iter()
        .map(|f| (f.name.clone(), f.field_type.nullable))
        .collect()
}

#[tokio::test]
#[ignore = "requires Java and the I/O expansion service JAR"]
async fn config_rows_match_java_provider_schemas() {
    let mut client = ExpansionClient::connect(DEFAULT_EXPANSION_SERVICE)
        .await
        .expect("start expansion service");
    let resp = client
        .discover_schema_transforms()
        .await
        .expect("discover schema transforms");

    let read_row = KafkaRead::new("KafkaRead", "b:9092", "t")
        .build_config_row()
        .unwrap();
    let write_row = KafkaWrite::new("KafkaWrite", "b:9092", "t")
        .build_config_row()
        .unwrap();

    for (urn, row) in [(URN_KAFKA_READ, read_row), (URN_KAFKA_WRITE, write_row)] {
        let config = resp
            .schema_transform_configs
            .get(urn)
            .unwrap_or_else(|| panic!("{urn} not advertised by the expansion service"));
        let java = proto_fields(config.config_schema.as_ref().expect("config schema"));
        assert_eq!(rust_fields(&row), java, "config schema mismatch for {urn}");
    }
}
