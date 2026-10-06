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

//! Checks the Managed configuration row, and that each mapped URN is advertised by its
//! expansion service, against the real Java expansion services. Needs Java and downloads
//! (or finds locally built) expansion service JARs, so it is ignored by default:
//! `cargo test -p apache-beam-io-managed --test managed_discovery_test -- --ignored`.

use external::ExpansionClient;
use managed_io::{
    GCP_EXPANSION_SERVICE, IO_EXPANSION_SERVICE, URN_MANAGED, default_expansion_service,
    managed_config_schema, urns,
};
use model::pipeline::FieldType as ProtoFieldType;

const ALL_URNS: &[&str] = &[
    urns::ICEBERG_READ,
    urns::ICEBERG_WRITE,
    urns::ICEBERG_CDC_READ,
    urns::KAFKA_READ,
    urns::KAFKA_WRITE,
    urns::BIGQUERY_READ,
    urns::BIGQUERY_WRITE,
    urns::POSTGRES_READ,
    urns::POSTGRES_WRITE,
    urns::MYSQL_READ,
    urns::MYSQL_WRITE,
    urns::SQL_SERVER_READ,
    urns::SQL_SERVER_WRITE,
    urns::DELTA_LAKE_READ,
];

#[tokio::test]
#[ignore = "requires Java and the Java expansion service JARs"]
async fn managed_config_and_urns_match_java_expansion_services() {
    let rust_fields: Vec<(String, bool)> = managed_config_schema()
        .fields
        .iter()
        .map(|f| (f.name.clone(), f.field_type.nullable))
        .collect();

    for service in [IO_EXPANSION_SERVICE, GCP_EXPANSION_SERVICE] {
        let mut client = ExpansionClient::connect(service)
            .await
            .unwrap_or_else(|e| panic!("start {service}: {e}"));
        let resp = client
            .discover_schema_transforms()
            .await
            .expect("discover schema transforms");

        let managed = resp
            .schema_transform_configs
            .get(URN_MANAGED)
            .unwrap_or_else(|| panic!("{URN_MANAGED} not advertised by {service}"));
        let java: Vec<(String, bool)> = managed
            .config_schema
            .as_ref()
            .expect("config schema")
            .fields
            .iter()
            .map(|f| {
                let nullable = f
                    .r#type
                    .as_ref()
                    .is_some_and(|t: &ProtoFieldType| t.nullable);
                (f.name.clone(), nullable)
            })
            .collect();
        assert_eq!(
            rust_fields, java,
            "Managed config schema mismatch on {service}"
        );

        for urn in ALL_URNS
            .iter()
            .filter(|u| default_expansion_service(u) == Some(service))
        {
            assert!(
                resp.schema_transform_configs.contains_key(*urn),
                "{urn} is mapped to {service} but not advertised by it"
            );
        }
    }
}
