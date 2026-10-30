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

//! Configuration, registration, and graph expansion of [`BigtableReadProvider`] and
//! [`BigtableWriteProvider`].

use std::time::Duration;

use beam::io::gcp::bigtable::RowFilterExt as _;
use beam::io::gcp::bigtable::proto::RowFilter;
use beam::io::gcp::bigtable::read::ReadOptions;
use beam::io::gcp::bigtable::target::{ReadTarget, WriteTarget};
use beam::io::gcp::bigtable::write::WriteOptions;
use beam::io::gcp::bigtable::{BigtableMutation, mutation_schema};
use beam::pipeline::Pipeline;
use beam::schema::{BeamRow, Row};
use beam::transforms::Create;
use bytes::Bytes;
use expansion::{
    PCollectionId, SchemaTransformProvider, SchemaTransformRegistration, TransformInputs,
};
use gcp_expansion::{
    BigtableReadConfig, BigtableReadProvider, BigtableWriteConfig, BigtableWriteProvider,
    ErrorHandling, URN_BIGTABLE_READ, URN_BIGTABLE_WRITE,
};

#[test]
fn bigtable_read_and_write_providers_are_registered() {
    let identifiers: Vec<_> = expansion::inventory::iter::<SchemaTransformRegistration>
        .into_iter()
        .map(|reg| (reg.provider)().identifier())
        .collect();
    assert!(identifiers.contains(&URN_BIGTABLE_READ));
    assert!(identifiers.contains(&URN_BIGTABLE_WRITE));
}

#[test]
fn bigtable_read_config_round_trips_and_maps_all_targets_and_options() {
    let row_filter = RowFilter::cells_per_column_limit(1);
    let config = BigtableReadConfig {
        project_id: "proj".to_string(),
        instance_id: "inst".to_string(),
        table_id: None,
        materialized_view_id: Some("mv_1".to_string()),
        authorized_view_id: None,
        app_profile_id: Some("databoost".to_string()),
        flatten: Some(false),
        start_key: Some(Bytes::from_static(b"a")),
        end_key: Some(Bytes::from_static(b"z")),
        row_filter: Some(Bytes::from(row_filter.to_proto_bytes())),
        value_bitmask: Some(Bytes::from_static(&[0x80])),
        value_bitmask_family: Some("cf".to_string()),
        value_bitmask_qualifier: Some(Bytes::from_static(b"flags")),
        max_initial_bundles: Some(4096),
        sub_split_tablets: Some(true),
        max_silent_scan_seconds: Some(30),
        endpoint: Some("http://localhost:8086".to_string()),
        error_handling: Some(ErrorHandling {
            output: "errors".to_string(),
        }),
    };
    let row = config.to_row().unwrap();
    assert_eq!(BigtableReadConfig::from_row(&row).unwrap(), config);

    let options = config.read_options().unwrap();
    assert_eq!(
        options,
        ReadOptions::new(ReadTarget::materialized_view("proj", "inst", "mv_1"))
            .with_app_profile_id("databoost")
            .with_flattened(false)
            .with_key_range(b"a".to_vec(), b"z".to_vec())
            .with_row_filter(&RowFilter::chain([
                row_filter,
                RowFilter::column_value_bitmask("cf", "flags", vec![0x80]),
            ]))
            .with_max_initial_bundles(4096)
            .with_sub_split_tablets(true)
            .with_max_silent_scan_duration(Duration::from_secs(30))
            .with_endpoint("http://localhost:8086")
    );
}

fn table_config() -> BigtableReadConfig {
    BigtableReadConfig {
        project_id: "p".to_string(),
        instance_id: "i".to_string(),
        table_id: Some("t".to_string()),
        ..BigtableReadConfig::default()
    }
}

#[test]
fn bigtable_read_config_with_only_value_bitmask_filters_cells() {
    let config = BigtableReadConfig {
        value_bitmask: Some(Bytes::from_static(&[0x0F])),
        ..table_config()
    };
    assert_eq!(
        config.read_options().unwrap(),
        ReadOptions::new(ReadTarget::table("p", "i", "t"))
            .with_row_filter(&RowFilter::value_bitmask(vec![0x0F]))
    );
}

#[test]
fn bigtable_read_config_rejects_incomplete_value_bitmask_options() {
    let cases = [
        (None, Some("cf"), Some(&b"q"[..]), "need value_bitmask"),
        (None, Some("cf"), None, "need value_bitmask"),
        (None, None, Some(&b"q"[..]), "need value_bitmask"),
        (Some(&[0x01][..]), Some("cf"), None, "Set both"),
        (Some(&[0x01][..]), None, Some(&b"q"[..]), "Set both"),
    ];
    for (mask, family, qualifier, expected) in cases {
        let config = BigtableReadConfig {
            value_bitmask: mask.map(Bytes::copy_from_slice),
            value_bitmask_family: family.map(str::to_string),
            value_bitmask_qualifier: qualifier.map(Bytes::copy_from_slice),
            ..table_config()
        };
        let err = config
            .read_options()
            .expect_err("incomplete bitmask options");
        assert!(err.contains(expected), "{err}");
    }
}

#[test]
fn bigtable_read_and_write_providers_expand_pipelines_with_and_without_errors() {
    let mut pipeline = Pipeline::new();
    let read_cfg = BigtableReadConfig {
        project_id: "p".to_string(),
        instance_id: "i".to_string(),
        table_id: Some("t".to_string()),
        error_handling: Some(ErrorHandling {
            output: "failures".to_string(),
        }),
        ..BigtableReadConfig::default()
    };
    let read_outputs = BigtableReadProvider
        .build_transform(
            read_cfg.to_row().unwrap(),
            TransformInputs::default(),
            &mut pipeline,
        )
        .unwrap();
    assert!(read_outputs.contains_key("output"));
    assert!(read_outputs.contains_key("failures"));

    let seed = pipeline
        .apply(
            "Seed",
            Create::new(vec![Row::from(BigtableMutation::set_cell(
                "k", "cf", "q", "v",
            ))]),
        )
        .with_row_schema(&mutation_schema());
    let write_cfg = BigtableWriteConfig {
        project_id: "p".to_string(),
        instance_id: "i".to_string(),
        table_id: "t".to_string(),
        authorized_view_id: Some("av".to_string()),
        app_profile_id: Some("ap".to_string()),
        max_elements_per_batch: Some(250),
        max_bytes_per_batch: Some(1024 * 1024),
        max_in_flight_requests: Some(4),
        enable_flow_control: Some(true),
        endpoint: Some("http://localhost:8086".to_string()),
        error_handling: Some(ErrorHandling {
            output: "write_errors".to_string(),
        }),
    };
    assert_eq!(
        write_cfg.clone().write_options().unwrap(),
        WriteOptions::new(WriteTarget::authorized_view("p", "i", "t", "av"))
            .with_app_profile_id("ap")
            .with_max_elements_per_batch(250)
            .with_max_bytes_per_batch(1024 * 1024)
            .with_max_in_flight_requests(4)
            .with_flow_control(true)
            .with_endpoint("http://localhost:8086")
    );

    let write_outputs = BigtableWriteProvider
        .build_transform(
            write_cfg.to_row().unwrap(),
            TransformInputs::from_iter([("input".to_string(), PCollectionId::from(seed.id()))]),
            &mut pipeline,
        )
        .unwrap();
    assert!(write_outputs.contains_key("write_errors"));
}
