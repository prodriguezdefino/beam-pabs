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

mod common;
use common::MockDataflowClient;

use beam::options::PipelineOptions;
use beam::pipeline::{
    Pipeline, ResourceHints, URN_RESOURCE_CPU_COUNT, URN_RESOURCE_MIN_RAM_BYTES,
    default_sdk_container_image,
};
use beam::runners::PipelineRunner;
use beam::transforms::ProcessContext;
use dataflow::client::JOB_STATE_DONE;
use dataflow::constants::STAGED_WORKER_NAME;
use dataflow::runner::{DataflowRunner, generate_job_name, job_name_prefix};
use dataflow::translate::DataflowJob;
use fluent::prelude::*;
use prost::Message;
use testutils::InMemoryFileSystem;

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
    assert_eq!(result.state, "JOB_STATE_DONE");

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
    let col = p.apply(Create::new(
        "Create",
        vec!["hello".to_string(), "world".to_string()],
    ));
    let _ = col.map("uppercase", |s: String| s.to_uppercase());

    let fs = Arc::new(InMemoryFileSystem::default());
    let client = Arc::new(MockDataflowClient::new());
    client.add_message("Starting Dataflow worker pools");
    client.add_message("Bundle execution completed successfully");

    let runner = DataflowRunner::with_options_and_clients(options, fs.clone(), client.clone());
    let result = runner.run(&p).await.unwrap();

    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, "JOB_STATE_DONE");

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
    use model::pipeline as proto;

    #[derive(Clone)]
    struct PlayerInventoryDoFn {
        player_points: MapStateSpec<String, i64>,
        unlocked_achievements: SetStateSpec<String>,
    }

    impl DoFn for PlayerInventoryDoFn {
        type In = (String, (String, (i64, String)));
        type Out = String;

        fn start_bundle(&mut self) -> Result {
            Ok(())
        }

        fn process_element(
            &mut self,
            (team, (player, (points, badge))): Self::In,
            ctx: &mut ProcessContext<'_, Self::Out>,
        ) -> Result {
            let mut scores_map = ctx.map_state(&self.player_points, &team)?;
            let mut badges_set = ctx.set_state(&self.unlocked_achievements, &team)?;

            let prev_score = scores_map.get(&player)?.unwrap_or(0);
            let next_score = prev_score + points;
            scores_map.put(player.clone(), next_score)?;

            let already_had = badges_set.contains(&badge)?;
            if !already_had {
                badges_set.insert(badge)?;
            }

            ctx.emit(format!("team:{team}:{player}:{next_score}:{already_had}"))
        }

        fn finish_bundle(&mut self, out: &mut ProcessContext<Self::Out>) -> Result {
            out.emit("finish:flushed".to_string())
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
    let events = p.apply(Create::new(
        "GameEvents",
        vec![
            (
                "teamA".to_string(),
                ("alice".to_string(), (10i64, "badge1".to_string())),
            ),
            (
                "teamA".to_string(),
                ("bob".to_string(), (20i64, "badge2".to_string())),
            ),
        ],
    ));

    let scores_spec = MapStateSpec::<String, i64>::new("player_points");
    let badges_spec = SetStateSpec::<String>::new("unlocked_achievements");

    let _ = events.apply(
        ParDo::new(
            "TrackPlayerInventory",
            PlayerInventoryDoFn {
                player_points: scores_spec.clone(),
                unlocked_achievements: badges_spec.clone(),
            },
        )
        .with_state_spec(&scores_spec)
        .with_state_spec(&badges_spec),
    );

    let fs = Arc::new(InMemoryFileSystem::default());
    let client = Arc::new(MockDataflowClient::new());
    client.add_message("Starting Dataflow worker pools");
    client.add_message("Stateful processing completed");

    let runner = DataflowRunner::with_options_and_clients(options, fs.clone(), client.clone());
    let result = runner.run(&p).await.unwrap();

    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, "JOB_STATE_DONE");

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
            && spec.urn == beam::pipeline::constants::URN_PAR_DO
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
        "--resource_hints=min_ram_bytes=16GB,cpu_count=4",
    ]);

    let p = Pipeline::create(&options);
    let col = p.apply(Create::new(
        "Create",
        vec!["hello".to_string(), "world".to_string()],
    ));

    // Transform with default pipeline hints
    let default_step = col.map("default_shape", |s: String| s.to_uppercase());

    // Transform with heavier custom resource hints
    let heavy_hints = ResourceHints::new()
        .with_min_ram_bytes(32 * 1000 * 1000 * 1000)
        .with_cpu_count(8);
    let _ = default_step
        .apply(ParDo::new("heavy_shape", PassThroughDoFn).with_resource_hints(heavy_hints));

    let fs = Arc::new(InMemoryFileSystem::default());
    let client = Arc::new(MockDataflowClient::new());
    client.add_message("Starting Dataflow worker pools");

    let runner = DataflowRunner::with_options_and_clients(options, fs.clone(), client.clone());
    let result = runner.run(&p).await.unwrap();

    assert_eq!(result.job_id, "job-1");
    assert_eq!(result.state, JOB_STATE_DONE);

    let staged_bytes = fs
        .get_file("gs://mock-bucket/staging/resource-hints-job/model")
        .expect("Model must be staged");
    let staged_proto = model::pipeline::Pipeline::decode(staged_bytes.as_slice())
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
            .any(|d| d.key == "resource_hints" && d.value == "min_ram_bytes=16GB,cpu_count=4")
    );

    // Without a machine type, `machine_type` stays `None` and Dataflow resolves the hints.
    assert_eq!(job.environment.worker_pools.len(), 1);
    assert_eq!(job.environment.worker_pools[0].machine_type, None);
}

/// The image every container test names, since Dataflow requires one.
const TEST_IMAGE: &str = "gcr.io/test/rust:v1";

/// Options for a run against the mocks, with `extra` flags appended.
fn container_options(job_name: &str, extra: &[&str]) -> PipelineOptions {
    let base = [
        "app".to_string(),
        "--runner=dataflow".to_string(),
        "--project=container-proj".to_string(),
        "--region=us-central1".to_string(),
        "--temp_location=gs://container-bucket/temp".to_string(),
        format!("--job_name={job_name}"),
    ];
    PipelineOptions::parse_from(
        base.into_iter()
            .chain(extra.iter().map(|flag| flag.to_string())),
    )
}

/// Where the runner stages `artifact` for `job_name`, given [`container_options`].
fn staged(job_name: &str, artifact: &str) -> String {
    format!("gs://container-bucket/temp/{job_name}/{artifact}")
}

fn uppercase_pipeline() -> Pipeline {
    let p = Pipeline::new();
    let _ = p
        .apply(Create::new("Create", vec!["hello".to_string()]))
        .map("uppercase", |s: String| s.to_uppercase());
    p
}

/// A stand-in worker binary, deleted on drop even when the test fails.
struct TempBinary(std::path::PathBuf);

impl TempBinary {
    const CONTENT: &'static [u8] = b"mock-elf-binary";

    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!("{name}_{}", std::process::id()));
        std::fs::write(&path, Self::CONTENT).expect("write the stand-in worker binary");
        Self(path)
    }

    fn flag(&self) -> String {
        format!("--worker_binary={}", self.0.display())
    }
}

impl Drop for TempBinary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn mocks() -> (Arc<InMemoryFileSystem>, Arc<MockDataflowClient>) {
    (
        Arc::new(InMemoryFileSystem::default()),
        Arc::new(MockDataflowClient::new()),
    )
}

#[tokio::test]
async fn test_dataflow_runner_requires_sdk_container_image() {
    let bin = TempBinary::new("no_image_worker");
    let worker_binary = bin.flag();

    // A staged binary does not make the image optional: workers still pull one.
    for extra in [vec![], vec![worker_binary.as_str()]] {
        let (fs, client) = mocks();
        let runner = DataflowRunner::with_options_and_clients(
            container_options("no-image", &extra),
            fs.clone(),
            client.clone(),
        );

        let err = runner
            .run(&uppercase_pipeline())
            .await
            .unwrap_err()
            .to_string();

        assert!(
            err.contains("Missing required option: sdk_container_image"),
            "{err}"
        );
        assert!(
            err.contains(&default_sdk_container_image()),
            "must name the unpublished default image: {err}"
        );
        assert!(fs.all_paths().is_empty(), "nothing may be staged");
        assert!(client.submitted_jobs().is_empty(), "no job may be created");
    }
}

#[tokio::test]
async fn test_dataflow_runner_prebaked_image_stages_no_binary() {
    let image = format!("--sdk_container_image={TEST_IMAGE}");
    let (fs, client) = mocks();
    let runner = DataflowRunner::with_options_and_clients(
        container_options("prebaked", &[&image]),
        fs.clone(),
        client.clone(),
    );

    let p = uppercase_pipeline();
    let result = runner.run(&p).await.unwrap();
    assert_eq!(result.state, JOB_STATE_DONE);

    // Only the graph is staged, and it names the image without a binary to fetch.
    assert_eq!(fs.all_paths(), vec![staged("prebaked", "model")]);
    let model = fs.get_file(&staged("prebaked", "model")).unwrap();
    let proto = model::pipeline::Pipeline::decode(model.as_slice()).unwrap();
    let env = &proto.components.unwrap().environments[&p.default_environment_id()];
    let payload = model::pipeline::DockerPayload::decode(env.payload.as_slice()).unwrap();
    assert_eq!(payload.container_image, TEST_IMAGE);
    assert!(env.dependencies.is_empty(), "{:?}", env.dependencies);

    let jobs = client.submitted_jobs();
    let pool = &jobs[0].environment.worker_pools[0];
    assert!(
        pool.packages
            .iter()
            .all(|pkg| pkg.name != STAGED_WORKER_NAME)
    );
    assert_eq!(
        pool.sdk_harness_container_images[0].container_image,
        TEST_IMAGE
    );
}

#[tokio::test]
async fn test_dataflow_runner_stages_worker_binary_for_sdk_image() {
    let bin = TempBinary::new("staged_worker");
    let (image, worker_binary) = (format!("--sdk_container_image={TEST_IMAGE}"), bin.flag());
    let (fs, client) = mocks();
    let runner = DataflowRunner::with_options_and_clients(
        container_options("staged", &[&image, &worker_binary]),
        fs.clone(),
        client.clone(),
    );

    runner.run(&uppercase_pipeline()).await.unwrap();

    let worker = staged("staged", STAGED_WORKER_NAME);
    assert_eq!(fs.get_file(&worker).as_deref(), Some(TempBinary::CONTENT));
    let jobs = client.submitted_jobs();
    let pool = &jobs[0].environment.worker_pools[0];
    assert!(
        pool.packages
            .iter()
            .any(|pkg| pkg.name == STAGED_WORKER_NAME && pkg.location == worker),
        "{:?}",
        pool.packages
    );
    assert_eq!(
        pool.sdk_harness_container_images[0].container_image,
        TEST_IMAGE
    );
}

#[tokio::test]
async fn test_dataflow_runner_template_location_writes_job_without_submitting() {
    let bin = TempBinary::new("template_worker");
    let location = "gs://container-bucket/templates/uppercase";
    let (image, worker_binary, template) = (
        format!("--sdk_container_image={TEST_IMAGE}"),
        bin.flag(),
        format!("--template_location={location}"),
    );
    let (fs, client) = mocks();
    let runner = DataflowRunner::with_options_and_clients(
        container_options("templated", &[&image, &worker_binary, &template]),
        fs.clone(),
        client.clone(),
    );

    let result = runner.run(&uppercase_pipeline()).await.unwrap();

    assert_eq!(result.job_id, "template");
    assert_eq!(result.state, JOB_STATE_DONE);
    assert!(
        client.submitted_jobs().is_empty(),
        "a template creates no job"
    );

    // The template is the job that would be submitted, and its artifacts are staged.
    let job: DataflowJob = serde_json::from_slice(&fs.get_file(location).unwrap()).unwrap();
    assert_eq!(job.name, "templated");
    assert!(job.steps.is_empty());
    let pipeline_url = &job.environment.sdk_pipeline_options.options.pipeline_url;
    assert_eq!(pipeline_url, &staged("templated", "model"));
    let pool = &job.environment.worker_pools[0];
    assert_eq!(
        pool.sdk_harness_container_images[0].container_image,
        TEST_IMAGE
    );
    assert!(
        pool.packages
            .iter()
            .all(|pkg| fs.contains_file(&pkg.location)),
        "{:?}",
        pool.packages
    );
    assert!(fs.contains_file(pipeline_url));
    assert!(fs.contains_file(&staged("templated", STAGED_WORKER_NAME)));
}

#[tokio::test]
async fn test_dataflow_runner_rejects_job_file_with_template_location() {
    let job_file = std::env::temp_dir().join(format!("both_modes_{}.json", std::process::id()));
    let (image, job_file_flag) = (
        format!("--sdk_container_image={TEST_IMAGE}"),
        format!("--dataflow_job_file={}", job_file.display()),
    );
    let (fs, client) = mocks();
    let runner = DataflowRunner::with_options_and_clients(
        container_options(
            "both-modes",
            &[
                &image,
                &job_file_flag,
                "--template_location=gs://container-bucket/templates/both",
            ],
        ),
        fs.clone(),
        client.clone(),
    );

    let err = runner.run(&Pipeline::new()).await.unwrap_err().to_string();

    assert!(
        err.contains("dataflow_job_file and template_location cannot be combined"),
        "{err}"
    );
    assert!(!job_file.exists(), "no job description may be written");
    assert!(fs.all_paths().is_empty(), "nothing may be staged");
    assert!(client.submitted_jobs().is_empty());
}

#[tokio::test]
async fn test_dataflow_runner_rejects_experiments_that_disable_runner_v2() {
    let image = format!("--sdk_container_image={TEST_IMAGE}");
    for experiment in [
        "disable_runner_v2",
        "disable_runner_v2_until_2023",
        "disable_runner_v2_until_v2.50",
        "disable_prime_runner_v2",
        "disable_runner_v2=true",
    ] {
        let experiments = format!("--experiments=beam_fn_api,{experiment}");
        let (fs, client) = mocks();
        let runner = DataflowRunner::with_options_and_clients(
            container_options("legacy", &[&image, &experiments]),
            fs.clone(),
            client.clone(),
        );

        let err = runner.run(&Pipeline::new()).await.unwrap_err().to_string();

        let name = experiment.split('=').next().unwrap();
        assert!(
            err.contains(&format!("experiment '{name}' disables Dataflow Runner v2")),
            "{experiment}: {err}"
        );
        assert!(
            fs.all_paths().is_empty(),
            "{experiment}: nothing may be staged"
        );
        assert!(client.submitted_jobs().is_empty(), "{experiment}");
    }

    // Experiments that only mention Runner v2 are passed through.
    let (fs, client) = mocks();
    let runner = DataflowRunner::with_options_and_clients(
        container_options("runner-v2", &[&image, "--experiments=use_runner_v2"]),
        fs,
        client.clone(),
    );
    runner.run(&Pipeline::new()).await.unwrap();
    assert_eq!(client.submitted_jobs().len(), 1);
}

#[test]
fn job_name_prefix_matches_java_format() {
    // 2026-10-03T04:44:08Z
    let at = Duration::from_secs(1_791_002_648);
    assert_eq!(
        job_name_prefix("wordcount", "rpablo", at),
        "wordcount-rpablo-1003044408"
    );
    assert_eq!(
        job_name_prefix("Word_Count", "R.Pablo", at),
        "word0count-r0pablo-1003044408"
    );
    assert_eq!(job_name_prefix("1st", "", at), "ast-1003044408");
    assert_eq!(job_name_prefix("", "u", at), "beamapp-u-1003044408");
    // Leap day, 2024-02-29T12:00:00Z.
    let leap = Duration::from_secs(1_709_208_000);
    assert_eq!(job_name_prefix("a", "b", leap), "a-b-0229120000");
}

#[test]
fn generated_job_name_is_valid_for_dataflow() {
    let name = generate_job_name();
    assert!(name.starts_with(|c: char| c.is_ascii_lowercase()), "{name}");
    assert!(
        name.ends_with(|c: char| c.is_ascii_alphanumeric()),
        "{name}"
    );
    assert!(
        name.chars()
            .all(|c| c == '-' || c.is_ascii_lowercase() || c.is_ascii_digit()),
        "{name}"
    );
}
