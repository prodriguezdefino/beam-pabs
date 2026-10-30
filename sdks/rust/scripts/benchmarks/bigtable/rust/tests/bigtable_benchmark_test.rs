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

//! Tests of the Cloud Bigtable benchmark in a Rust pipeline. They need no Cloud Bigtable
//! instance.

use std::sync::Arc;

use beam::io::gcp::bigtable::schema::{
    BigtableReadShape, cell_schema, flattened_row_schema, nested_row_schema,
};
use beam::io::gcp::bigtable::target::ReadTarget;
use beam::options::{ParseError, PipelineOptions, try_parse_from};
use beam::schema::{Field, FieldValue, Row, Schema};
use bigtable_benchmark::{
    Args, Connector, Shape, build_pipeline, format_row, parse_positive_usize, read_options,
};

/// Parses the required flags and then `flags`.
fn try_parse(flags: &[&str]) -> Result<(PipelineOptions, Args), ParseError> {
    let args = [
        "bigtable_benchmark",
        "--bigtable_project=test-project",
        "--bigtable_instance=test-instance",
        "--bigtable_table=test-table",
        "--output=gs://test-bucket/out",
    ]
    .into_iter()
    .chain(flags.iter().copied());
    try_parse_from::<Args, _, _>(args)
}

fn parse(flags: &[&str]) -> (PipelineOptions, Args) {
    try_parse(flags).expect("valid command line")
}

#[test]
fn flags_have_the_defaults_of_the_launcher() {
    let (_, args) = parse(&[]);
    assert_eq!(args.reader, Connector::Rust);
    assert_eq!(args.shape, Shape::BtToGcs);
    assert_eq!(args.bigtable_project, "test-project");
    assert_eq!(args.bigtable_instance, "test-instance");
    assert_eq!(args.bigtable_table.as_deref(), Some("test-table"));
    assert_eq!(args.bigtable_materialized_view, None);
    assert_eq!(args.app_profile, None);
    assert!(!args.flatten);
    assert_eq!(args.output, "gs://test-bucket/out");
    assert_eq!(args.max_initial_bundles, None);
    assert_eq!(args.sub_split_tablets, None);

    let (_, custom) = parse(&[
        "--app_profile=databoost-profile",
        "--flatten=true",
        "--max_initial_bundles=64",
        "--sub_split_tablets=false",
    ]);
    assert_eq!(custom.app_profile.as_deref(), Some("databoost-profile"));
    assert!(custom.flatten);
    assert_eq!(custom.max_initial_bundles, Some(64));
    assert_eq!(custom.sub_split_tablets, Some(false));
}

#[test]
fn flags_reject_unknown_variants_and_zero_bundles() {
    for flags in [
        &["--shape=unknown"][..],
        &["--reader=java-native"],
        &["--max_initial_bundles=0"],
        &["--bigtable_materialized_view=test-view"],
    ] {
        assert!(try_parse(flags).is_err(), "{flags:?}");
    }
    assert!(parse_positive_usize("0").is_err());
    assert!(parse_positive_usize("abc").is_err());
    assert_eq!(parse_positive_usize("16"), Ok(16));
}

#[test]
fn read_options_reflect_parsed_flags() {
    let (_, args) = parse(&[
        "--app_profile=databoost-profile",
        "--flatten=false",
        "--max_initial_bundles=128",
        "--sub_split_tablets=false",
    ]);
    let options = read_options(&args).expect("valid options");
    assert_eq!(options.app_profile_id.as_deref(), Some("databoost-profile"));
    assert_eq!(options.shape, BigtableReadShape::Nested);
    assert_eq!(options.max_initial_bundles, 128);
    assert!(!options.sub_split_tablets);
    assert_eq!(
        options.target,
        ReadTarget::table("test-project", "test-instance", "test-table")
    );
}

#[test]
fn a_materialized_view_flag_reads_the_view_instead_of_a_table() {
    let args = [
        "bigtable_benchmark",
        "--bigtable_project=test-project",
        "--bigtable_instance=test-instance",
        "--bigtable_materialized_view=test-view",
        "--output=gs://test-bucket/out",
    ];
    let (_, args) = try_parse_from::<Args, _, _>(args).expect("valid command line");
    assert_eq!(args.bigtable_table, None);
    let options = read_options(&args).expect("valid options");
    assert_eq!(
        options.target,
        ReadTarget::materialized_view("test-project", "test-instance", "test-view")
    );
}

#[test]
fn format_row_formats_flattened_and_nested_rows() {
    let cell = Row::new(
        cell_schema(),
        vec![
            Some(FieldValue::Bytes(b"1234".to_vec())),
            Some(FieldValue::Int64(100)),
        ],
    )
    .expect("valid cell row");
    let flat_row = Row::new(
        flattened_row_schema(),
        vec![
            Some(FieldValue::Bytes(b"Article#42".to_vec())),
            Some(FieldValue::String("counts".to_string())),
            Some(FieldValue::Bytes(b"num_characters".to_vec())),
            Some(FieldValue::Array(vec![Some(FieldValue::Row(cell.clone()))])),
        ],
    )
    .expect("valid flattened row");
    assert_eq!(
        format_row(&flat_row, true),
        "Article#42\tcounts:num_characters\t1234"
    );

    let nested_row = Row::new(
        nested_row_schema(),
        vec![
            Some(FieldValue::Bytes(b"Article#42".to_vec())),
            Some(FieldValue::Map(vec![(
                FieldValue::String("counts".to_string()),
                Some(FieldValue::Map(vec![(
                    FieldValue::String("num_characters".to_string()),
                    Some(FieldValue::Array(vec![Some(FieldValue::Row(cell))])),
                )])),
            )])),
        ],
    )
    .expect("valid nested row");
    assert_eq!(
        format_row(&nested_row, false),
        "Article#42\tcounts:num_characters=1234"
    );
}

#[test]
fn format_row_reads_nested_fields_by_name_in_another_order() {
    let cell = |value: &[u8]| {
        Some(FieldValue::Array(vec![Some(FieldValue::Row(
            Row::new(
                cell_schema(),
                vec![
                    Some(FieldValue::Bytes(value.to_vec())),
                    Some(FieldValue::Int64(100)),
                ],
            )
            .expect("valid cell row"),
        ))]))
    };
    let families = FieldValue::Map(vec![
        (
            FieldValue::String("counts".to_string()),
            Some(FieldValue::Map(vec![
                (FieldValue::String("a".to_string()), cell(b"1")),
                (FieldValue::String("b".to_string()), None),
            ])),
        ),
        (FieldValue::String("meta".to_string()), None),
        (
            FieldValue::String("text".to_string()),
            Some(FieldValue::Map(vec![(
                FieldValue::String("title".to_string()),
                cell(b"Hi"),
            )])),
        ),
    ]);
    let nested = nested_row_schema();
    let reversed = Arc::new(Schema::new(
        nested.fields.iter().rev().cloned().collect::<Vec<Field>>(),
    ));
    let row = Row::new(
        reversed,
        vec![Some(families), Some(FieldValue::Bytes(b"k1".to_vec()))],
    )
    .expect("valid reversed row");
    assert_eq!(
        format_row(&row, false),
        "k1\tcounts:a=1\tcounts:b=\ttext:title=Hi"
    );
}

#[test]
fn pipeline_builds_for_flattened_and_nested_reads() {
    for flag in ["--flatten=true", "--flatten=false"] {
        let (options, args) = parse(&[flag]);
        let pipeline = build_pipeline(&options, &args).expect("the pipeline builds");
        let names = pipeline
            .to_proto()
            .components
            .expect("components")
            .transforms
            .into_values()
            .map(|transform| transform.unique_name)
            .collect::<Vec<_>>();
        for step in ["Read", "Touch", "Write"] {
            assert!(names.iter().any(|name| name == step), "{step} in {names:?}");
        }
    }
}
