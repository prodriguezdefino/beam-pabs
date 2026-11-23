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

//! Planning and reading the data files of a table of the `iceberg` memory catalog.

mod common;

use std::sync::Arc;

use beam::pipeline::Pipeline;
use beam::schema::BeamRow;
use beam_io_arrow::FieldSelection;
use beam_io_iceberg::catalog::file_io;
use beam_io_iceberg::read::plan::{plan_tasks, row_schema};
use beam_io_iceberg::read::reader::row_groups;
use beam_io_iceberg::{Read, ReadRows, read_failure_schema};
use iceberg::expr::Reference;
use iceberg::spec::Datum;

use common::{Fixture, options, row_count};

#[derive(Clone, Debug, BeamRow)]
struct EventRow {
    id: i64,
    name: String,
    score: f64,
}

#[derive(BeamRow)]
struct NonCloneEvent {
    id: i64,
}

fn names(schema: &beam::schema::Schema) -> Vec<&str> {
    schema.fields.iter().map(|f| f.name.as_str()).collect()
}

#[tokio::test(flavor = "multi_thread")]
async fn a_read_returns_every_row_of_the_current_snapshot() {
    let fixture = Fixture::new().await;
    fixture.append(0..100).await;
    fixture.append(100..150).await;

    let batches = fixture
        .read(&options(), &FieldSelection::All)
        .await
        .unwrap();

    assert_eq!(row_count(&batches), 150);
    let schema = batches[0].schema();
    let columns: Vec<_> = schema.fields().iter().map(|f| f.name().as_str()).collect();
    assert_eq!(columns, ["id", "name", "score"]);
}

#[tokio::test(flavor = "multi_thread")]
async fn keep_and_drop_select_the_columns() {
    let fixture = Fixture::new().await;
    fixture.append(0..10).await;
    let catalog = &fixture.catalog;

    let keep = FieldSelection::Keep(vec!["score".into(), "id".into()]);
    let drop = FieldSelection::Drop(vec!["name".into()]);
    for columns in [keep, drop] {
        let schema = row_schema(catalog, &options(), &columns).await.unwrap();
        assert_eq!(names(&schema), ["id", "score"]);
        let batches = fixture.read(&options(), &columns).await.unwrap();
        assert_eq!(batches[0].num_columns(), 2);
        assert_eq!(row_count(&batches), 10);
    }
}

#[tokio::test(flavor = "multi_thread")]
async fn a_filter_returns_only_the_matching_rows() {
    let fixture = Fixture::new().await;
    fixture.append(0..100).await;
    let filter = Reference::new("id").less_than(Datum::long(25));

    let batches = fixture
        .read(&options().with_filter(filter), &FieldSelection::All)
        .await
        .unwrap();

    assert_eq!(row_count(&batches), 25);
}

#[tokio::test(flavor = "multi_thread")]
async fn a_snapshot_id_reads_an_older_snapshot() {
    let fixture = Fixture::new().await;
    let first = fixture.append(0..40).await;
    fixture.append(40..100).await;

    let older = fixture
        .read(&options().with_snapshot_id(first), &FieldSelection::All)
        .await
        .unwrap();
    let missing = fixture
        .read(&options().with_snapshot_id(7), &FieldSelection::All)
        .await
        .unwrap_err();

    assert_eq!(row_count(&older), 40);
    assert!(
        missing.to_string().contains("no snapshot with id 7"),
        "{missing}"
    );
}

#[tokio::test(flavor = "multi_thread")]
async fn a_table_with_no_snapshot_has_no_tasks() {
    let fixture = Fixture::new().await;
    let tasks = plan_tasks(&fixture.catalog, &options(), &FieldSelection::All)
        .await
        .unwrap();
    assert!(tasks.is_empty());
}

#[tokio::test(flavor = "multi_thread")]
async fn invalid_column_selections_fail() {
    let fixture = Fixture::new().await;
    fixture.append(0..1).await;
    let cases = [
        (
            FieldSelection::Keep(vec!["nope".into()]),
            "keep: the input has no field 'nope'",
        ),
        (
            FieldSelection::Only("name".into()),
            "only applies to a write",
        ),
        (
            FieldSelection::Drop(vec!["id".into(), "name".into(), "score".into()]),
            "leaves no columns to read",
        ),
    ];
    for (columns, expected) in cases {
        let error = row_schema(&fixture.catalog, &options(), &columns)
            .await
            .unwrap_err();
        assert!(error.to_string().contains(expected), "{error}");
    }
    assert!(options().with_batch_size(0).check().is_err());
}

#[tokio::test(flavor = "multi_thread")]
async fn row_groups_lie_inside_the_data_file() {
    let fixture = Fixture::new().await;
    fixture.append(0..100).await;
    let table = iceberg::Catalog::load_table(&fixture.catalog, &options().table_ident().unwrap())
        .await
        .unwrap();
    let manifests = table.scan().build().unwrap().plan_files().await.unwrap();
    let tasks: Vec<_> = futures::TryStreamExt::try_collect(manifests).await.unwrap();
    let task = &tasks[0];

    let groups = row_groups(
        &file_io([]),
        task.data_file_path(),
        task.file_size_in_bytes(),
    )
    .await
    .unwrap();

    assert!(!groups.is_empty());
    assert!(groups.iter().all(|(start, end)| 4 <= *start && start < end));
    assert!(
        groups
            .iter()
            .all(|(_, end)| *end < task.file_size_in_bytes())
    );
}

fn assert_clone<C: Clone>(_: &C) {}

#[tokio::test(flavor = "multi_thread")]
async fn try_apply_with_failures_and_clone_work_for_read_and_read_rows() {
    let non_clone = Read::<NonCloneEvent>::new(options());
    assert_clone(&non_clone);
    assert_clone(&non_clone.with_failures());

    let fixture = Fixture::new().await;
    fixture.append(0..5).await;
    let (_warehouse, client) = fixture.into_client();

    let pipeline = Pipeline::new();
    let read_typed = Read::<EventRow>::new(options()).with_client(Arc::clone(&client));
    let _ = pipeline
        .try_apply("ReadTyped", read_typed.clone())
        .expect("typed read expands");
    let typed_with_failures = pipeline
        .try_apply("ReadTypedWithFailures", read_typed.with_failures())
        .expect("typed read with failures expands");
    assert_eq!(
        typed_with_failures.failures.row_schema().as_deref(),
        Some(read_failure_schema().as_ref())
    );

    let read_rows = ReadRows::new(options()).with_client(Arc::clone(&client));
    let rows = pipeline
        .try_apply("ReadRows", read_rows.clone())
        .expect("row read expands");
    assert_eq!(
        names(rows.row_schema().expect("table schema").as_ref()),
        ["id", "name", "score"]
    );
    let rows_with_failures = pipeline
        .try_apply("ReadRowsWithFailures", read_rows.with_failures())
        .expect("row read with failures expands");
    assert_eq!(
        rows_with_failures.failures.row_schema().as_deref(),
        Some(read_failure_schema().as_ref())
    );

    let bad = options().with_batch_size(0);
    assert!(
        pipeline
            .try_apply("BadTyped", Read::<EventRow>::new(bad.clone()))
            .is_err()
    );
    assert!(
        pipeline
            .try_apply(
                "BadTypedFailures",
                Read::<EventRow>::new(bad.clone()).with_failures()
            )
            .is_err()
    );
    assert!(
        pipeline
            .try_apply(
                "BadRows",
                ReadRows::new(bad.clone()).with_client(Arc::clone(&client))
            )
            .is_err()
    );
    assert!(
        pipeline
            .try_apply(
                "BadRowsFailures",
                ReadRows::new(bad.clone())
                    .with_client(Arc::clone(&client))
                    .with_failures()
            )
            .is_err()
    );
    assert_eq!(pipeline.validate(), Ok(()));

    let invalid_pipeline = Pipeline::new();
    let invalid_out = invalid_pipeline.apply("Invalid", Read::<EventRow>::new(bad).with_failures());
    assert_eq!(
        invalid_out.failures.row_schema().as_deref(),
        Some(read_failure_schema().as_ref())
    );
    assert!(invalid_pipeline.validate().is_err());
}
