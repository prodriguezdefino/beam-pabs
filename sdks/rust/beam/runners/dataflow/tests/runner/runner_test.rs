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

use std::sync::Arc;
use std::time::Duration;

use crate::common::MockDataflowClient;

use beam::options::PipelineOptions;
use beam::pipeline::{Pipeline, ResourceHints, URN_RESOURCE_CPU_COUNT, URN_RESOURCE_MIN_RAM_BYTES};
use beam::runners::{JobState, PipelineRunner};
use beam::transforms::ProcessContext;
use beam_fluent::prelude::*;
use beam_runner_dataflow::{DataflowRunner, JOB_STATE_FAILED, JOB_STATE_RUNNING};
use beam_test_utils::InMemoryFileSystem;
use prost::Message;

#[tokio::test]
async fn test_dataflow_runner_dry_run_job_file() {
    let temp_file = std::env::temp_dir().join(format!("dataflow_job_{}.json", std::process::id()));
    let job_file_arg = format!("--dataflow_job_file={}", temp_file.display());

    let options = PipelineOptions::parse_from([
        "app",
        "--runner=dataflow",
        "--project=dry-run-project",
        "--region=us-central1",
        "--temp_location=gs://dry-run-bucket/temp",
        "--sdk_container_image=gcr.io/test/rust:v1",
        &job_file_arg,
    ]);

    let p = Pipeline::new();
    // Resolved from `--runner=dataflow` through the runner registry.
    let result = beam::runners::run(&p, &options).await.unwrap();
    assert_eq!(result.job_id, "dry-run");
    assert_eq!(result.state, JobState::Done);

    assert!(temp_file.exists(), "Job description file must be written");
    let content = std::fs::read_to_string(&temp_file).unwrap();
    assert!(content.contains("\"projectId\": \"dry-run-project\""));
    assert!(content.contains("\"use_runner_v2\""));
    assert!(content.contains("\"steps\": []"));

    let _ = std::fs::remove_file(&temp_file);
}

#[tokio::test]
async fn test_dataflow_runner_end_to_end_mocked() {
    let options = PipelineOptions::parse_from([
        "app",
        "--runner=DataflowRunner",
        "--project=beam-dataflow-mock",
        "--region=us-central1",
        "--temp_location=gs://mock-bucket/temp",
        "--staging_location=gs://mock-bucket/staging",
        "--job_name=custom-rust-job",
        "--sdk_container_image=gcr.io/mock/runner:v1",
        "--num_workers=2",
        "--worker_machine_type=e2-standard-4",
    ]);

    let p = Pipeline::new();
    let col = p.apply(
        "Create",
        Create::new(vec!["hello".to_string(), "world".to_string()]),
    );
    let _ = col.map("uppercase", |s: String| s.to_uppercase());

    let fs = Arc::new(InMemoryFileSystem::default());
    let client = Arc::new(MockDataflowClient::new());
    client.add_message("Starting Dataflow worker pools");
    client.add_message("Bundle execution completed successfully");

    let runner = DataflowRunner::with_options_and_clients(
        options,
        Arc::clone(&fs) as _,
        Arc::clone(&client) as _,
    );
    let result = runner.run(&p).await.unwrap();

    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, JobState::Done);

    // Dataflow reads the staged proto, not a step list, so the built graph is staged verbatim.
    let staged_model = fs
        .get_file("gs://mock-bucket/staging/custom-rust-job/model")
        .expect("Model must be staged");
    let proto = p.to_proto();
    assert_eq!(staged_model, proto.encode_to_vec());

    assert!(
        proto
            .components
            .as_ref()
            .unwrap()
            .transforms
            .values()
            .any(|t| !t.subtransforms.is_empty()),
        "composites must reach the runner intact"
    );

    // The submitted job carries the requested options.
    let submitted = client.submitted_jobs();
    assert_eq!(submitted.len(), 1);
    let job = &submitted[0];
    assert_eq!(job.project_id, "beam-dataflow-mock");
    assert_eq!(job.name, "custom-rust-job");
    assert_eq!(job.job_type, "JOB_TYPE_BATCH");
    assert!(job.steps.is_empty(), "Runner v2 jobs carry no steps");
    assert!(
        job.client_request_id
            .as_deref()
            .is_some_and(|id| !id.is_empty()),
        "each submission carries a clientRequestId"
    );
    assert_eq!(job.environment.worker_pools.len(), 1);
    let pool = &job.environment.worker_pools[0];
    assert_eq!(pool.num_workers, 2);
    assert_eq!(pool.machine_type.as_deref(), Some("e2-standard-4"));
    let images: Vec<&str> = pool
        .sdk_harness_container_images
        .iter()
        .map(|i| i.container_image.as_str())
        .collect();
    assert_eq!(images, vec!["gcr.io/mock/runner:v1"]);
    let options = &job.environment.sdk_pipeline_options.options;
    assert_eq!(options.region, "us-central1");
    assert_eq!(
        options.pipeline_url,
        "gs://mock-bucket/staging/custom-rust-job/model"
    );
    // Job DONE from creation: exactly one status poll.
    assert_eq!(client.get_job_calls(), 1);
}

#[tokio::test]
async fn test_dataflow_runner_stateful_map_set_and_row_serde() {
    use beam_model::pipeline as proto;

    #[derive(State)]
    struct PlayerInventoryState {
        player_points: MapState<String, i64>,
        unlocked_achievements: SetState<String>,
    }

    #[derive(Clone)]
    struct PlayerInventoryDoFn;

    impl StatefulDoFn for PlayerInventoryDoFn {
        type Key = String;
        type Value = (String, (i64, String));
        type Out = String;
        type State = PlayerInventoryState;
        type Timer = NoTimers;

        fn start_bundle(&mut self) -> Result {
            Ok(())
        }

        fn process_element(
            &mut self,
            team: &Self::Key,
            (player, (points, badge)): Self::Value,
            state: &mut Self::State,
            ctx: &mut StatefulContext<'_, Self>,
        ) -> Result {
            let prev_score = state.player_points.get(&player)?.unwrap_or(0);
            let next_score = prev_score + points;
            state.player_points.put(&player, &next_score)?;

            let already_had = state.unlocked_achievements.contains(&badge)?;
            if !already_had {
                state.unlocked_achievements.insert(&badge)?;
            }

            ctx.emit(format!("team:{team}:{player}:{next_score}:{already_had}"))
        }

        fn finish_bundle(&mut self, out: &mut FinishBundleContext<'_, Self::Out>) -> Result {
            out.emit(
                "finish:flushed".to_string(),
                BoundedWindow::Global(beam::windowing::GlobalWindow),
                Instant::END_OF_GLOBAL_WINDOW,
            )
        }
    }

    let options = PipelineOptions::parse_from([
        "app",
        "--runner=DataflowRunner",
        "--project=beam-dataflow-stateful",
        "--region=us-central1",
        "--temp_location=gs://mock-bucket/temp",
        "--staging_location=gs://mock-bucket/staging",
        "--job_name=stateful-rust-job",
        "--sdk_container_image=gcr.io/mock/runner:v1",
    ]);

    let p = Pipeline::new();
    let events = p.apply(
        "GameEvents",
        Create::new(vec![
            (
                "teamA".to_string(),
                ("alice".to_string(), (10i64, "badge1".to_string())),
            ),
            (
                "teamA".to_string(),
                ("bob".to_string(), (20i64, "badge2".to_string())),
            ),
        ]),
    );

    let _ = events.apply("TrackPlayerInventory", ParDo::new(PlayerInventoryDoFn));

    let fs = Arc::new(InMemoryFileSystem::default());
    let client = Arc::new(MockDataflowClient::new());
    client.add_message("Starting Dataflow worker pools");
    client.add_message("Stateful processing completed");

    let runner = DataflowRunner::with_options_and_clients(
        options,
        Arc::clone(&fs) as _,
        Arc::clone(&client) as _,
    );
    let result = runner.run(&p).await.unwrap();

    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, JobState::Done);

    let staged_bytes = fs
        .get_file("gs://mock-bucket/staging/stateful-rust-job/model")
        .expect("Stateful model must be staged to GCS");
    let staged_pipeline = proto::Pipeline::decode(staged_bytes.as_slice())
        .expect("Staged pipeline must be valid proto");

    let components = staged_pipeline
        .components
        .expect("Components must be present");
    let mut found_map_spec = false;
    let mut found_set_spec = false;

    for transform in components.transforms.values() {
        if let Some(ref spec) = transform.spec
            && spec.urn == beam::pipeline::URN_PAR_DO
            && !spec.payload.is_empty()
            && let Ok(pardo_payload) = proto::ParDoPayload::decode(spec.payload.as_slice())
        {
            for (state_id, state_spec) in &pardo_payload.state_specs {
                if state_id == "player_points"
                    && let Some(proto::state_spec::Spec::MapSpec(_)) = state_spec.spec
                {
                    found_map_spec = true;
                }
                if state_id == "unlocked_achievements"
                    && let Some(proto::state_spec::Spec::SetSpec(_)) = state_spec.spec
                {
                    found_set_spec = true;
                }
            }
        }
    }

    assert!(
        found_map_spec,
        "MapStateSpec must be present in staged ParDoPayload"
    );
    assert!(
        found_set_spec,
        "SetStateSpec must be present in staged ParDoPayload"
    );
}

#[tokio::test]
async fn test_dataflow_runner_fails_on_missing_gcp_fields() {
    let options = PipelineOptions::parse_from([
        "app",
        "--runner=dataflow",
        "--project=my-project",
        // missing region and temp_location
    ]);

    let p = Pipeline::new();
    let runner = DataflowRunner::with_options(options);
    let err = runner.run(&p).await.unwrap_err();
    assert!(
        err.to_string().contains("Google Cloud region"),
        "Error must mention missing region, got: {err}"
    );
}

#[tokio::test]
async fn test_dataflow_runner_worker_shape_resource_hints() {
    #[derive(Clone)]
    struct PassThroughDoFn;
    impl DoFn for PassThroughDoFn {
        type In = String;
        type Out = String;
        fn process_element(
            &mut self,
            element: Self::In,
            ctx: &mut ProcessContext<'_, Self::Out>,
        ) -> Result {
            ctx.emit(element)
        }
    }

    let options = PipelineOptions::parse_from([
        "app",
        "--runner=DataflowRunner",
        "--project=beam-dataflow-resource-hints",
        "--region=us-central1",
        "--temp_location=gs://mock-bucket/temp",
        "--staging_location=gs://mock-bucket/staging",
        "--job_name=resource-hints-job",
        "--sdk_container_image=gcr.io/mock/runner:v1",
        "--resource_hints=min_ram=16GB,cpu_count=4",
    ]);

    let p = Pipeline::create(&options);
    let col = p.apply(
        "Create",
        Create::new(vec!["hello".to_string(), "world".to_string()]),
    );

    // Transform with default pipeline hints
    let default_step = col.map("default_shape", |s: String| s.to_uppercase());

    // Transform with heavier custom resource hints
    let heavy_hints = ResourceHints::new()
        .with_min_ram_bytes(32 * 1000 * 1000 * 1000)
        .with_cpu_count(8);
    let _ = default_step.apply(
        "heavy_shape",
        ParDo::new(PassThroughDoFn).with_resource_hints(heavy_hints),
    );

    let fs = Arc::new(InMemoryFileSystem::default());
    let client = Arc::new(MockDataflowClient::new());
    client.add_message("Starting Dataflow worker pools");

    let runner = DataflowRunner::with_options_and_clients(
        options,
        Arc::clone(&fs) as _,
        Arc::clone(&client) as _,
    );
    let result = runner.run(&p).await.unwrap();

    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, JobState::Done);

    let staged_bytes = fs
        .get_file("gs://mock-bucket/staging/resource-hints-job/model")
        .expect("Model must be staged");
    let staged_proto = beam_model::pipeline::Pipeline::decode(staged_bytes.as_slice())
        .expect("Staged bytes must decode as Pipeline proto");
    let components = staged_proto.components.unwrap();

    // The default environment carries the pipeline-level hints: 16 GB RAM and 4 CPUs.
    let default_env = &components.environments[&p.default_environment_id()];
    assert_eq!(
        default_env
            .resource_hints
            .get(URN_RESOURCE_MIN_RAM_BYTES)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("16000000000")
    );
    assert_eq!(
        default_env
            .resource_hints
            .get(URN_RESOURCE_CPU_COUNT)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("4")
    );

    // The heavy transform environment carries hints for 32 GB RAM and 8 CPUs.
    let heavy_transform = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("heavy_shape"))
        .unwrap();
    assert_ne!(heavy_transform.environment_id, p.default_environment_id());
    let heavy_env = &components.environments[&heavy_transform.environment_id];
    assert_eq!(
        heavy_env
            .resource_hints
            .get(URN_RESOURCE_MIN_RAM_BYTES)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("32000000000")
    );
    assert_eq!(
        heavy_env
            .resource_hints
            .get(URN_RESOURCE_CPU_COUNT)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("8")
    );

    // The submitted Dataflow job carries the resource hints in its display data.
    let submitted = client.submitted_jobs();
    assert_eq!(submitted.len(), 1);
    let job = &submitted[0];
    let display_data = &job.environment.sdk_pipeline_options.display_data;
    assert!(
        display_data
            .iter()
            .any(|d| d.key == "resource_hints" && d.value == "min_ram=16GB,cpu_count=4")
    );

    // Without a machine type, `machine_type` stays `None` and Dataflow resolves the hints.
    assert_eq!(job.environment.worker_pools.len(), 1);
    assert_eq!(job.environment.worker_pools[0].machine_type, None);
}

#[tokio::test]
async fn test_dataflow_runner_surfaces_worker_error_messages_on_job_failure() {
    let options = PipelineOptions::parse_from([
        "app",
        "--runner=DataflowRunner",
        "--project=beam-dataflow-fail",
        "--region=us-central1",
        "--temp_location=gs://mock-bucket/temp",
        "--staging_location=gs://mock-bucket/staging",
        "--job_name=worker-failure-job",
        "--sdk_container_image=gcr.io/mock/runner:v1",
    ]);

    let p = Pipeline::new();
    let fs = Arc::new(InMemoryFileSystem::default());

    // Case 1: job transitions from RUNNING to FAILED across polls.
    let client = Arc::new(MockDataflowClient::with_initial_job_state(
        JOB_STATE_RUNNING,
    ));
    client.add_message("Starting Dataflow worker pools");
    client.add_error_message("Worker panic in DoFn 'explode': index out of bounds");
    client.script_get_job([Ok(JOB_STATE_RUNNING), Ok(JOB_STATE_FAILED)]);

    let runner =
        DataflowRunner::with_options_and_clients(options.clone(), Arc::clone(&fs) as _, client)
            .with_poll_interval(Duration::from_millis(1));
    let err = runner.run(&p).await.unwrap_err().to_string();
    assert!(
        err.contains("Dataflow job 'job-1' failed with terminal state 'JOB_STATE_FAILED'"),
        "{err}"
    );
    assert!(
        err.contains("Worker panic in DoFn 'explode': index out of bounds"),
        "RunnerError must include JOB_MESSAGE_ERROR text: {err}"
    );
    assert!(
        !err.contains("Starting Dataflow worker pools"),
        "RunnerError must exclude non-error job messages: {err}"
    );
    assert_eq!(
        err.matches("Worker panic in DoFn 'explode'").count(),
        1,
        "Error message must not repeat across polls: {err}"
    );

    // Case 2: job is already in JOB_STATE_FAILED on the first poll.
    let immediate_client = Arc::new(MockDataflowClient::with_initial_job_state(JOB_STATE_FAILED));
    immediate_client.add_error_message("Container startup failed: exit code 1");
    let immediate_runner = DataflowRunner::with_options_and_clients(options, fs, immediate_client)
        .with_poll_interval(Duration::from_millis(1));
    let immediate_err = immediate_runner.run(&p).await.unwrap_err().to_string();
    assert!(
        immediate_err.contains("Container startup failed: exit code 1"),
        "RunnerError must include error message from terminal poll: {immediate_err}"
    );
}
