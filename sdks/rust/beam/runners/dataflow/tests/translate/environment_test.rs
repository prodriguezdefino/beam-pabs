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

//! The environment of the job: SDK container images, the options snapshot, display data,
//! autoscaling and the required options.

use beam::options::{
    DebugOptions, OptionsSnapshot, PipelineOptions, SDK_OPTIONS_OPTION, WorkerOptions,
};
use beam::pipeline::{Pipeline, URN_ENV_DOCKER};
use dataflow::{AutoscalingAlgorithm, DataflowOptions};
use dataflow::{StagedArtifacts, TranslateError, translate_job};
use gcp::options::GcpOptions;
use prost::Message;
use serde_json::Value;

use crate::common::{
    CustomArgs, assert_item, assert_unique_keys, custom_pipeline, job_options, job_options_for,
    single,
};

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
    assert_eq!(image.environment_id, dataflow::DEFAULT_RUST_ENVIRONMENT_ID);
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
        DataflowOptions {
            autoscaling_algorithm: Some(AutoscalingAlgorithm::Fixed),
            ..Default::default()
        },
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
        .and_then(Value::as_str)
        .expect("the snapshot travels as a string");

    // A worker restores exactly the typed values the driver parsed.
    let restored = PipelineOptions::from_snapshot(
        OptionsSnapshot::decode(encoded).expect("decodes"),
        Default::default(),
    )
    .expect("restores");
    assert_eq!(restored.view_as::<CustomArgs>().expect("restored"), custom);
    assert_eq!(restored.view_as::<GcpOptions>().expect("restored"), gcp);
    assert_eq!(restored.view_as::<DataflowOptions>().expect("restored"), df);

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

/// The `autoscalingSettings` of the worker pool of a job with these options, as JSON.
fn autoscaling_json(worker: &WorkerOptions, dataflow: &DataflowOptions) -> Value {
    let opts = job_options(&base_gcp(), worker, &DebugOptions::default(), dataflow);
    let job = translate_job(&Pipeline::new().to_proto(), &opts, "scaling", &NO_ARTIFACTS)
        .expect("the job translates");
    serde_json::to_value(&job).expect("the job is JSON")["environment"]["workerPools"][0]
        ["autoscalingSettings"]
        .clone()
}

#[test]
fn test_autoscaling_settings_without_algorithm_have_only_max_num_workers() {
    // With no --autoscaling_algorithm, the request has only maxNumWorkers, and no settings
    // when max_num_workers is not set.
    assert_eq!(
        autoscaling_json(&WorkerOptions::default(), &DataflowOptions::default()),
        Value::Null
    );
    let worker = WorkerOptions {
        num_workers: Some(4),
        max_num_workers: Some(10),
        ..Default::default()
    };
    assert_eq!(
        autoscaling_json(&worker, &DataflowOptions::default()),
        serde_json::json!({"maxNumWorkers": 10})
    );
}

#[test]
fn test_autoscaling_algorithm_maps_to_the_api_value() {
    let worker = WorkerOptions {
        num_workers: Some(4),
        max_num_workers: Some(4),
        ..Default::default()
    };
    let fixed = DataflowOptions {
        autoscaling_algorithm: Some(AutoscalingAlgorithm::Fixed),
        ..Default::default()
    };
    assert_eq!(
        autoscaling_json(&worker, &fixed),
        serde_json::json!({"algorithm": "AUTOSCALING_ALGORITHM_NONE", "maxNumWorkers": 4})
    );
    let throughput = DataflowOptions {
        autoscaling_algorithm: Some(AutoscalingAlgorithm::ThroughputBased),
        ..Default::default()
    };
    assert_eq!(
        autoscaling_json(&WorkerOptions::default(), &throughput),
        serde_json::json!({"algorithm": "AUTOSCALING_ALGORITHM_BASIC"})
    );
}

#[test]
fn test_autoscaling_algorithm_from_the_command_line() {
    let options = PipelineOptions::parse_from([
        "app",
        "--runner=DataflowRunner",
        "--autoscaling_algorithm=NONE",
    ]);
    let dataflow: DataflowOptions = options.view_as().expect("valid Dataflow options");
    assert_eq!(
        dataflow.autoscaling_algorithm,
        Some(AutoscalingAlgorithm::Fixed)
    );

    let any_case = PipelineOptions::parse_from(["app", "--autoscaling_algorithm=throughput_based"]);
    let dataflow: DataflowOptions = any_case.view_as().expect("valid Dataflow options");
    assert_eq!(
        dataflow.autoscaling_algorithm,
        Some(AutoscalingAlgorithm::ThroughputBased)
    );

    let unknown = PipelineOptions::parse_from(["app", "--autoscaling_algorithm=FASTEST"]);
    assert!(
        unknown.view_as::<DataflowOptions>().is_err(),
        "an unknown algorithm fails when the options are parsed"
    );
}
