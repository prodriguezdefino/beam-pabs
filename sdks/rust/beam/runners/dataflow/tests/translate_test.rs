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

use beam::options::{
    DebugOptions, OptionsSnapshot, PipelineOptionGroup, PipelineOptions, SDK_OPTIONS_OPTION,
    WorkerOptions, try_parse_from,
};
use beam::pipeline::{Pipeline, URN_ENV_DOCKER};
use dataflow::constants::{
    EXPERIMENT_USE_RUNNER_V2, FNAPI_STREAMING, JOB_TYPE_BATCH, JOB_TYPE_STREAMING,
};
use dataflow::options::{DataflowJobOptions, DataflowOptions};
use dataflow::translate::{DisplayDataItem, StagedArtifacts, TranslateError, translate_job};
use fluent::prelude::*;
use gcp::GcpOptions;
use prost::Message;
use serde::{Deserialize, Serialize};

/// Stands in for a pipeline's own option group.
#[derive(clap::Args, Serialize, Deserialize, Clone, Debug, PartialEq)]
struct CustomArgs {
    #[arg(long)]
    custom_string: String,
    #[arg(long)]
    custom_int: i64,
    #[arg(long)]
    custom_bool: bool,
}

impl PipelineOptionGroup for CustomArgs {}

/// Parses a pipeline's command line, as `beam::options::parse` does.
fn custom_pipeline(args: &[&str]) -> (PipelineOptions, CustomArgs) {
    try_parse_from::<CustomArgs, _, _>(args.iter().copied()).expect("valid command line")
}

/// Job options for `options` with these groups set, resolved as the runner does.
fn job_options_for(
    options: &PipelineOptions,
    gcp: &GcpOptions,
    worker: &WorkerOptions,
    debug: &DebugOptions,
    dataflow: &DataflowOptions,
) -> DataflowJobOptions {
    options.set(gcp.clone()).expect("valid GCP options");
    options.set(worker.clone()).expect("valid worker options");
    options.set(debug.clone()).expect("valid debug options");
    options
        .set(dataflow.clone())
        .expect("valid Dataflow options");
    DataflowJobOptions::try_from(options).expect("job options resolve")
}

/// Job options with these groups set and every other option at its default.
fn job_options(
    gcp: &GcpOptions,
    worker: &WorkerOptions,
    debug: &DebugOptions,
    dataflow: &DataflowOptions,
) -> DataflowJobOptions {
    job_options_for(
        &PipelineOptions::with_runner("dataflow"),
        gcp,
        worker,
        debug,
        dataflow,
    )
}

/// Returns the only display data item named `key`, failing if it is missing or repeated.
fn single<'a>(dd: &'a [DisplayDataItem], key: &str) -> &'a DisplayDataItem {
    let matches: Vec<&DisplayDataItem> = dd.iter().filter(|d| d.key == key).collect();
    assert_eq!(
        matches.len(),
        1,
        "expected exactly one display data item '{key}', got {matches:?}"
    );
    matches[0]
}

/// Asserts the only item named `key` has this value and type.
fn assert_item(dd: &[DisplayDataItem], key: &str, value: &str, item_type: &str) {
    let item = single(dd, key);
    assert_eq!(
        (item.value.as_str(), item.item_type.as_str()),
        (value, item_type),
        "display data '{key}'"
    );
}

/// Asserts no two display data items share a key.
fn assert_unique_keys(dd: &[DisplayDataItem]) {
    let mut keys: Vec<&str> = dd.iter().map(|d| d.key.as_str()).collect();
    keys.sort_unstable();
    let before = keys.len();
    keys.dedup();
    assert_eq!(keys.len(), before, "duplicate display data keys in {dd:?}");
}

#[test]
fn test_translate_job_minimal() {
    let p = Pipeline::new();
    p.set_docker_environment("gcr.io/my-proj/beam-rust:v1");
    let proto_pipeline = p.to_proto();

    let gcp_opts = GcpOptions {
        project: Some("my-gcp-project".to_string()),
        region: Some("us-central1".to_string()),
        temp_location: Some("gs://my-bucket/temp".to_string()),
        ..Default::default()
    };

    let worker_opts = WorkerOptions::default();
    let debug_opts = DebugOptions::default();
    let df_opts = DataflowOptions::default();
    let artifacts = StagedArtifacts {
        model_url: "gs://my-bucket/staging/my-wordcount-job/model",
        model_hash: "hash-abc-123",
        worker_url: None,
        worker_hash: None,
        packages: &[],
    };

    let job_opts = job_options(&gcp_opts, &worker_opts, &debug_opts, &df_opts);
    let job = translate_job(&proto_pipeline, &job_opts, "my-wordcount-job", &artifacts).unwrap();

    assert_eq!(job.project_id, "my-gcp-project");
    assert_eq!(job.name, "my-wordcount-job");
    assert_eq!(job.job_type, JOB_TYPE_BATCH);
    assert!(
        job.steps.is_empty(),
        "steps array must be empty for Runner v2"
    );

    let env = &job.environment;
    assert_eq!(env.temp_storage_prefix, "gs://my-bucket/temp");
    assert!(
        env.experiments
            .contains(&EXPERIMENT_USE_RUNNER_V2.to_string()),
        "experiments must contain use_runner_v2"
    );

    assert_eq!(
        env.sdk_pipeline_options.options.pipeline_url,
        "gs://my-bucket/staging/my-wordcount-job/model"
    );
    assert_eq!(
        env.sdk_pipeline_options.options.pipeline_proto_hash,
        "hash-abc-123"
    );

    assert_eq!(env.worker_pools.len(), 1);
    let pool = &env.worker_pools[0];
    assert_eq!(pool.kind, "harness");
    assert_eq!(
        pool.worker_harness_container_image.as_deref(),
        Some("gcr.io/my-proj/beam-rust:v1")
    );
    assert_eq!(pool.ip_configuration, "WORKER_IP_UNSPECIFIED");
    assert!(pool.packages.is_empty());

    let dd = &env.sdk_pipeline_options.display_data;
    assert_unique_keys(dd);
    assert_item(dd, "runner", "DataflowRunner", "STRING");
    assert_item(dd, "project", "my-gcp-project", "STRING");
    assert_item(dd, "region", "us-central1", "STRING");
    assert_item(dd, "temp_location", "gs://my-bucket/temp", "STRING");
    assert_item(
        dd,
        "sdk_container_image",
        "gcr.io/my-proj/beam-rust:v1",
        "STRING",
    );
}

#[test]
fn test_translate_job_advanced_options() {
    let p = Pipeline::new();
    let proto_pipeline = p.to_proto();

    let gcp_opts = GcpOptions {
        project: Some("prod-gcp-project".to_string()),
        region: Some("europe-west1".to_string()),
        zone: Some("europe-west1-b".to_string()),
        temp_location: Some("gs://prod-bucket/temp".to_string()),
        staging_location: Some("gs://prod-bucket/staging".to_string()),
        service_account_email: Some("sa@prod-gcp-project.iam.gserviceaccount.com".to_string()),
    };

    let worker_opts = WorkerOptions {
        sdk_container_image: Some("gcr.io/prod/custom-runner:latest".to_string()),
        num_workers: Some(3),
        max_num_workers: Some(12),
        ..Default::default()
    };

    let debug_opts = DebugOptions {
        experiments: vec!["enable_prime".to_string()],
        ..Default::default()
    };

    let df_opts = DataflowOptions {
        worker_machine_type: Some("e2-standard-4".to_string()),
        disk_size_gb: Some(60),
        disk_type: Some("pd-ssd".to_string()),
        network: Some("custom-vpc".to_string()),
        subnetwork: Some("custom-subnet".to_string()),
        no_use_public_ips: true,
        dataflow_service_options: vec!["max_workflow_threads=30".to_string()],
        labels: Some("environment=production,tier=analytics".to_string()),
        ..Default::default()
    };

    let artifacts = StagedArtifacts {
        model_url: "gs://prod-bucket/staging/analytics-pipeline/model",
        model_hash: "model-hash-456",
        worker_url: Some("gs://prod-bucket/staging/analytics-pipeline/worker"),
        worker_hash: Some("worker-hash-789"),
        packages: &[],
    };

    let job_opts = job_options(&gcp_opts, &worker_opts, &debug_opts, &df_opts);
    let job = translate_job(&proto_pipeline, &job_opts, "analytics-pipeline", &artifacts).unwrap();

    assert_eq!(job.project_id, "prod-gcp-project");
    assert_eq!(job.name, "analytics-pipeline");
    assert_eq!(
        job.labels.get("environment"),
        Some(&"production".to_string())
    );
    assert_eq!(job.labels.get("tier"), Some(&"analytics".to_string()));

    let env = &job.environment;
    assert_eq!(
        env.service_account_email.as_deref(),
        Some("sa@prod-gcp-project.iam.gserviceaccount.com")
    );
    assert_eq!(
        env.service_options,
        vec!["max_workflow_threads=30".to_string()]
    );
    assert!(env.experiments.contains(&"use_runner_v2".to_string()));
    assert!(env.experiments.contains(&"enable_prime".to_string()));

    let pool = &env.worker_pools[0];
    assert_eq!(pool.machine_type.as_deref(), Some("e2-standard-4"));
    assert_eq!(pool.num_workers, 3);
    assert_eq!(
        pool.autoscaling_settings.as_ref().unwrap().max_num_workers,
        12
    );
    assert_eq!(pool.disk_size_gb, Some(60));
    assert_eq!(pool.disk_type.as_deref(), Some("pd-ssd"));
    assert_eq!(pool.ip_configuration, "WORKER_IP_PRIVATE");
    assert_eq!(pool.network.as_deref(), Some("custom-vpc"));
    assert_eq!(pool.subnetwork.as_deref(), Some("custom-subnet"));
    assert_eq!(pool.zone.as_deref(), Some("europe-west1-b"));

    assert_eq!(pool.packages.len(), 1);
    assert_eq!(pool.packages[0].name, "worker");
    assert_eq!(
        pool.packages[0].location,
        "gs://prod-bucket/staging/analytics-pipeline/worker"
    );
    assert_eq!(pool.packages[0].sha256.as_deref(), Some("worker-hash-789"));

    let dd = &env.sdk_pipeline_options.display_data;
    assert_unique_keys(dd);
    assert_item(dd, "worker_machine_type", "e2-standard-4", "STRING");
    assert_item(dd, "num_workers", "3", "INTEGER");
    assert_item(dd, "max_num_workers", "12", "INTEGER");
    assert_item(dd, "disk_size_gb", "60", "INTEGER");
    assert_item(dd, "disk_type", "pd-ssd", "STRING");
    assert_item(dd, "network", "custom-vpc", "STRING");
    assert_item(dd, "subnetwork", "custom-subnet", "STRING");
    assert_item(dd, "zone", "europe-west1-b", "STRING");
    assert_item(
        dd,
        "service_account_email",
        "sa@prod-gcp-project.iam.gserviceaccount.com",
        "STRING",
    );
    assert_item(dd, "no_use_public_ips", "true", "BOOLEAN");
}

#[test]
fn test_translate_json_serialization() {
    let p = Pipeline::new();
    let proto_pipeline = p.to_proto();

    let gcp_opts = GcpOptions {
        project: Some("test-project".to_string()),
        region: Some("us-east1".to_string()),
        temp_location: Some("gs://test-bucket/temp".to_string()),
        ..Default::default()
    };

    let worker_opts = WorkerOptions::default();
    let debug_opts = DebugOptions::default();
    let df_opts = DataflowOptions::default();
    let artifacts = StagedArtifacts {
        model_url: "gs://test-bucket/staging/model",
        model_hash: "hash-xyz",
        worker_url: None,
        worker_hash: None,
        packages: &[],
    };

    let job_opts = job_options(&gcp_opts, &worker_opts, &debug_opts, &df_opts);
    let job = translate_job(&proto_pipeline, &job_opts, "json-test-job", &artifacts).unwrap();

    let json_str = serde_json::to_string(&job).unwrap();
    let json: serde_json::Value = serde_json::from_str(&json_str).unwrap();

    // Field names are what the Dataflow v1b3 REST API expects (camelCase, `type`), except
    // for `display_data` and the `version` keys, which the service reads in snake_case.
    assert_eq!(json["projectId"], "test-project");
    assert_eq!(json["name"], "json-test-job");
    assert_eq!(json["type"], "JOB_TYPE_BATCH");
    assert_eq!(json["steps"], serde_json::json!([]));
    assert!(json.get("labels").is_none(), "empty labels are omitted");
    let env = &json["environment"];
    assert_eq!(env["tempStoragePrefix"], "gs://test-bucket/temp");
    // Dataflow rejects a classic template without `version.job_type`.
    assert_eq!(env["version"]["job_type"], "FNAPI_BATCH");
    assert!(env["version"].get("jobType").is_none());
    assert_eq!(env["version"]["major"], "6");
    assert!(env.get("serviceAccountEmail").is_none());
    let sdk_opts = &env["sdkPipelineOptions"];
    assert_eq!(
        sdk_opts["options"]["pipelineUrl"],
        "gs://test-bucket/staging/model"
    );
    assert_eq!(sdk_opts["options"]["pipelineProtoHash"], "hash-xyz");
    assert_eq!(sdk_opts["options"]["region"], "us-east1");
    assert_eq!(sdk_opts["options"]["tempLocation"], "gs://test-bucket/temp");
    assert!(sdk_opts["display_data"].is_array());
    assert!(sdk_opts.get("displayData").is_none());
    let pools = env["workerPools"].as_array().unwrap();
    assert_eq!(pools.len(), 1);
    assert_eq!(pools[0]["kind"], "harness");
    assert_eq!(pools[0]["numWorkers"], 1);
    assert_eq!(pools[0]["ipConfiguration"], "WORKER_IP_UNSPECIFIED");
    assert!(pools[0]["sdkHarnessContainerImages"].is_array());

    // Verify roundtrip deserialization
    let deserialized: dataflow::translate::DataflowJob = serde_json::from_str(&json_str).unwrap();
    assert_eq!(deserialized, job);
}

#[test]
fn test_translate_with_pipeline_options_display_data() {
    let p = Pipeline::new();
    let proto_pipeline = p.to_proto();

    let gcp_opts = GcpOptions {
        project: Some("cli-gcp-project".to_string()),
        region: Some("us-central1".to_string()),
        temp_location: Some("gs://cli-bucket/temp".to_string()),
        staging_location: Some("gs://cli-bucket/staging".to_string()),
        ..Default::default()
    };
    let worker_opts = WorkerOptions {
        num_workers: Some(5),
        ..Default::default()
    };
    let debug_opts = DebugOptions::default();
    let df_opts = DataflowOptions {
        worker_machine_type: Some("n1-standard-4".to_string()),
        ..Default::default()
    };
    let (pipeline_opts, _) = custom_pipeline(&[
        "wordcount",
        "--runner=DataflowRunner",
        "--job_name=custom-wordcount",
        "--custom_string=hello",
        "--custom_int=42",
        "--custom_bool",
    ]);
    let artifacts = StagedArtifacts {
        model_url: "gs://cli-bucket/staging/custom-wordcount/model",
        model_hash: "hash-model",
        worker_url: None,
        worker_hash: None,
        packages: &[],
    };

    let job_opts = job_options_for(
        &pipeline_opts,
        &gcp_opts,
        &worker_opts,
        &debug_opts,
        &df_opts,
    );
    let job = translate_job(&proto_pipeline, &job_opts, "custom-wordcount", &artifacts).unwrap();

    let display_data = &job.environment.sdk_pipeline_options.display_data;
    assert_unique_keys(display_data);
    assert_item(display_data, "custom_string", "hello", "STRING");
    assert_item(display_data, "custom_int", "42", "INTEGER");
    assert_item(display_data, "custom_bool", "true", "BOOLEAN");
    assert_item(display_data, "num_workers", "5", "INTEGER");
    assert_eq!(single(display_data, "project").value, "cli-gcp-project");

    let additional = &job
        .environment
        .sdk_pipeline_options
        .options
        .additional_options;
    assert_eq!(
        additional.get("customString"),
        Some(&serde_json::Value::String("hello".to_string()))
    );
    assert_eq!(additional.get("customInt"), Some(&serde_json::json!(42)));
    assert_eq!(
        additional.get("customBool"),
        Some(&serde_json::Value::Bool(true))
    );
    assert_eq!(
        additional.get("workerMachineType"),
        Some(&serde_json::Value::String("n1-standard-4".to_string()))
    );
}

#[test]
fn streaming_job_type_from_flag_or_unbounded_input() {
    let cases = [
        (false, vec!["--streaming=true"], "streaming-app"),
        (true, vec![], "unbounded-app"),
    ];

    for (unbounded, flags, name) in cases {
        let p = Pipeline::new();
        p.set_docker_environment("gcr.io/my-proj/beam-rust:v1");
        if unbounded {
            let _ = p.apply(GenerateSequence::new("GenerateSequence", 0));
        }
        let proto_pipeline = p.to_proto();

        let gcp_opts = GcpOptions {
            project: Some(format!("{name}-project")),
            region: Some("us-central1".to_string()),
            temp_location: Some(format!("gs://{name}-bucket/temp")),
            ..Default::default()
        };
        let worker_opts = WorkerOptions::default();
        let debug_opts = DebugOptions::default();
        let df_opts = DataflowOptions::default();

        let mut args = vec![name, "--runner=DataflowRunner"];
        args.extend(flags);
        let pipeline_opts = PipelineOptions::parse_from(args);

        let artifacts = StagedArtifacts {
            model_url: "gs://bucket/staging/model",
            model_hash: "hash-123",
            worker_url: None,
            worker_hash: None,
            packages: &[],
        };

        let job_opts = job_options_for(
            &pipeline_opts,
            &gcp_opts,
            &worker_opts,
            &debug_opts,
            &df_opts,
        );
        let job = translate_job(&proto_pipeline, &job_opts, name, &artifacts).unwrap();

        assert_eq!(job.job_type, JOB_TYPE_STREAMING);
        assert_eq!(job.environment.version.job_type, FNAPI_STREAMING);
        assert!(
            job.environment
                .experiments
                .contains(&EXPERIMENT_USE_RUNNER_V2.to_string()),
            "{name}: streaming jobs must use runner v2"
        );
    }
}

// ---------------------------------------------------------------------------
// sdk_harness_container_images, options snapshot, display-data dedup, error paths
// ---------------------------------------------------------------------------

fn base_gcp() -> GcpOptions {
    GcpOptions {
        project: Some("p".to_string()),
        region: Some("us-central1".to_string()),
        temp_location: Some("gs://b/temp".to_string()),
        ..Default::default()
    }
}

const NO_ARTIFACTS: StagedArtifacts<'static> = StagedArtifacts {
    model_url: "gs://b/model",
    model_hash: "h",
    worker_url: None,
    worker_hash: None,
    packages: &[],
};

fn docker_env(image: &str, caps: &[&str]) -> model::pipeline::Environment {
    model::pipeline::Environment {
        urn: URN_ENV_DOCKER.to_string(),
        payload: model::pipeline::DockerPayload {
            container_image: image.to_string(),
        }
        .encode_to_vec(),
        capabilities: caps.iter().map(|c| c.to_string()).collect(),
        ..Default::default()
    }
}

fn pipeline_with_envs(
    envs: Vec<(&str, model::pipeline::Environment)>,
) -> model::pipeline::Pipeline {
    model::pipeline::Pipeline {
        components: Some(model::pipeline::Components {
            environments: envs
                .into_iter()
                .map(|(id, env)| (id.to_string(), env))
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    }
}

#[test]
fn test_sdk_harness_container_images_per_environment_sorted_with_overrides() {
    let pipeline = pipeline_with_envs(vec![
        ("z_rust", docker_env("gcr.io/me/rust:1", &["cap:rust"])),
        ("a_java", docker_env("apache/beam_java17_sdk:2.60.0", &[])),
        (
            "m_process",
            model::pipeline::Environment {
                urn: "beam:env:process:v1".to_string(),
                ..Default::default()
            },
        ),
        ("n_empty_image", docker_env("", &["cap:x"])),
    ]);
    let worker = WorkerOptions {
        sdk_harness_container_image_overrides: vec![".*java.*,my/java:dev".to_string()],
        ..Default::default()
    };
    let (gcp, debug, df) = (
        base_gcp(),
        DebugOptions::default(),
        DataflowOptions::default(),
    );
    let opts = job_options(&gcp, &worker, &debug, &df);
    let job = translate_job(&pipeline, &opts, "imgs", &NO_ARTIFACTS).unwrap();

    let pool = &job.environment.worker_pools[0];
    let images: Vec<(&str, &str, Vec<&str>, bool)> = pool
        .sdk_harness_container_images
        .iter()
        .map(|i| {
            (
                i.environment_id.as_str(),
                i.container_image.as_str(),
                i.capabilities.iter().map(String::as_str).collect(),
                i.use_single_core_per_container,
            )
        })
        .collect();
    // Sorted by environment id; non-Docker and image-less environments are skipped;
    // capabilities are forwarded exactly as declared (the Java env declared none).
    assert_eq!(
        images,
        vec![
            ("a_java", "my/java:dev", vec![], false),
            ("z_rust", "gcr.io/me/rust:1", vec!["cap:rust"], false),
        ]
    );
    // Without --sdk_container_image the first (sorted) environment's image is primary.
    assert_eq!(
        pool.worker_harness_container_image.as_deref(),
        Some("my/java:dev")
    );
    assert_eq!(
        single(
            &job.environment.sdk_pipeline_options.display_data,
            "sdk_container_image"
        )
        .value,
        "my/java:dev"
    );
}

#[test]
fn test_sdk_harness_container_images_default_when_no_docker_environment() {
    let pipeline = pipeline_with_envs(vec![]);
    let worker = WorkerOptions {
        sdk_container_image: Some("gcr.io/me/explicit:3".to_string()),
        ..Default::default()
    };
    let (gcp, debug, df) = (
        base_gcp(),
        DebugOptions::default(),
        DataflowOptions::default(),
    );
    let opts = job_options(&gcp, &worker, &debug, &df);
    let job = translate_job(&pipeline, &opts, "default-img", &NO_ARTIFACTS).unwrap();

    let pool = &job.environment.worker_pools[0];
    assert_eq!(pool.sdk_harness_container_images.len(), 1);
    let image = &pool.sdk_harness_container_images[0];
    assert_eq!(image.container_image, "gcr.io/me/explicit:3");
    assert_eq!(
        image.environment_id,
        dataflow::constants::DEFAULT_RUST_ENVIRONMENT_ID
    );
    assert_eq!(image.capabilities, beam::pipeline::standard_capabilities());
    assert_eq!(
        pool.worker_harness_container_image.as_deref(),
        Some("gcr.io/me/explicit:3")
    );

    // With neither an option nor a Docker environment, the SDK default image is used.
    let worker = WorkerOptions::default();
    let opts = job_options(&gcp, &worker, &debug, &df);
    let job = translate_job(&pipeline, &opts, "default-img", &NO_ARTIFACTS).unwrap();
    assert_eq!(
        job.environment.worker_pools[0].sdk_harness_container_images[0].container_image,
        beam::pipeline::default_sdk_container_image()
    );
}

#[test]
fn test_translate_job_rejects_malformed_docker_payload() {
    let pipeline = pipeline_with_envs(vec![(
        "bad",
        model::pipeline::Environment {
            urn: URN_ENV_DOCKER.to_string(),
            payload: vec![0xff, 0xff, 0xff],
            ..Default::default()
        },
    )]);
    let (gcp, worker, debug, df) = (
        base_gcp(),
        WorkerOptions::default(),
        DebugOptions::default(),
        DataflowOptions::default(),
    );
    let opts = job_options(&gcp, &worker, &debug, &df);
    let err = translate_job(&pipeline, &opts, "bad", &NO_ARTIFACTS).unwrap_err();
    assert!(matches!(err, TranslateError::DecodePayload(_)), "{err:?}");
}

#[test]
fn test_translate_job_requires_project_region_and_temp_location() {
    let (worker, debug, df) = (
        WorkerOptions::default(),
        DebugOptions::default(),
        DataflowOptions::default(),
    );
    let cases = [
        (
            GcpOptions {
                project: None,
                ..base_gcp()
            },
            "Missing GCP project",
        ),
        (
            GcpOptions {
                region: None,
                ..base_gcp()
            },
            "Missing GCP region",
        ),
        (
            GcpOptions {
                temp_location: None,
                ..base_gcp()
            },
            "Missing GCP temp_location",
        ),
    ];
    for (gcp, expected) in cases {
        let opts = job_options(&gcp, &worker, &debug, &df);
        let err =
            translate_job(&Pipeline::new().to_proto(), &opts, "j", &NO_ARTIFACTS).unwrap_err();
        assert!(
            matches!(&err, TranslateError::Options(msg) if msg == expected),
            "expected {expected:?}, got {err:?}"
        );
    }
}

#[test]
fn test_translate_job_forwards_the_typed_options_snapshot() {
    let (pipeline_opts, custom) = custom_pipeline(&[
        "wordcount",
        "--runner=DataflowRunner",
        "--custom_string=hello",
        "--custom_int=42",
        "--custom_bool",
    ]);
    let (gcp, worker, debug, df) = (
        base_gcp(),
        WorkerOptions::default(),
        DebugOptions::default(),
        DataflowOptions::default(),
    );
    let opts = job_options_for(&pipeline_opts, &gcp, &worker, &debug, &df);
    let job = translate_job(&Pipeline::new().to_proto(), &opts, "args", &NO_ARTIFACTS).unwrap();

    let additional = &job
        .environment
        .sdk_pipeline_options
        .options
        .additional_options;
    let encoded = additional
        .get(SDK_OPTIONS_OPTION)
        .and_then(serde_json::Value::as_str)
        .expect("the snapshot travels as a string");

    // A worker restores exactly the typed values the driver parsed.
    let restored = PipelineOptions::from_snapshot(
        OptionsSnapshot::decode(encoded).expect("decodes"),
        Default::default(),
    )
    .expect("restores");
    assert_eq!(restored.view_as::<CustomArgs>().expect("restored"), custom);
    assert_eq!(restored.view_as::<GcpOptions>().expect("restored"), gcp);

    // Fields carried structurally in the payload never leak into additional options.
    for key in [
        "region",
        "tempLocation",
        "temp_location",
        "experiments",
        "pipelineUrl",
        "pipelineProtoHash",
    ] {
        assert!(
            !additional.contains_key(key),
            "{key} leaked: {additional:?}"
        );
    }
}

#[test]
fn test_translate_job_forwards_the_snapshot_without_user_options() {
    let (gcp, worker, debug, df) = (
        base_gcp(),
        WorkerOptions::default(),
        DebugOptions::default(),
        DataflowOptions::default(),
    );
    let opts = job_options(&gcp, &worker, &debug, &df);
    let job = translate_job(&Pipeline::new().to_proto(), &opts, "noargs", &NO_ARTIFACTS).unwrap();
    let additional = &job
        .environment
        .sdk_pipeline_options
        .options
        .additional_options;

    assert!(additional.contains_key(SDK_OPTIONS_OPTION));
    assert!(!additional.contains_key("rust_sdk_args"));
}

#[test]
fn test_translate_job_display_data_dedup_keeps_structural_value() {
    // num_workers arrives both from WorkerOptions set programmatically (5) and from the
    // command line (7); a group that was set overrides the command line.
    let worker = WorkerOptions {
        num_workers: Some(5),
        ..Default::default()
    };
    let pipeline_opts = PipelineOptions::parse_from([
        "app",
        "--runner=DataflowRunner",
        "--num_workers=7",
        "--project=cli-project",
    ]);
    let (gcp, debug, df) = (
        base_gcp(),
        DebugOptions::default(),
        DataflowOptions::default(),
    );
    let opts = job_options_for(&pipeline_opts, &gcp, &worker, &debug, &df);
    let job = translate_job(&Pipeline::new().to_proto(), &opts, "dedup", &NO_ARTIFACTS).unwrap();

    let dd = &job.environment.sdk_pipeline_options.display_data;
    assert_unique_keys(dd);
    assert_item(dd, "num_workers", "5", "INTEGER");
    assert_item(dd, "project", "p", "STRING");
    let additional = &job
        .environment
        .sdk_pipeline_options
        .options
        .additional_options;
    assert_eq!(additional.get("numWorkers"), Some(&serde_json::json!(5)));
    assert_eq!(additional.get("project"), Some(&serde_json::json!("p")));
}
