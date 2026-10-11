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
    DebugOptions, OptionsError, PipelineOptionGroup, PipelineOptions, PortableOptions,
    WorkerOptions,
};
use clap::Args;
use serde::{Deserialize, Serialize};

#[test]
fn test_default_options() {
    let opts = PipelineOptions::default();
    assert_eq!(opts.runner, "prism");
    assert_eq!(opts.endpoint, None);
    assert_eq!(opts.job_name, None);
    assert!(!opts.streaming);
}

#[test]
fn test_with_runner() {
    let opts = PipelineOptions::with_runner("prism");
    assert_eq!(opts.runner, "prism");
    assert_eq!(opts.endpoint, None);
}

#[test]
fn test_parse_standard_options() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=prism",
        "--endpoint=http://localhost:8073",
        "--job_name=my-test-job",
    ]);
    assert_eq!(opts.runner, "prism");
    assert_eq!(opts.endpoint.as_deref(), Some("http://localhost:8073"));
    assert_eq!(opts.job_name.as_deref(), Some("my-test-job"));

    // Verify alias --jobName.
    let opts_alias = PipelineOptions::parse_from(["app", "--jobName=aliased-job"]);
    assert_eq!(opts_alias.job_name.as_deref(), Some("aliased-job"));
}

#[test]
fn test_downcast_to_worker_options() {
    let current_exe = std::env::current_exe().unwrap();
    let binary_str = current_exe.to_str().unwrap();
    let worker_arg = format!("--worker_binary={binary_str}");

    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=prism",
        "--sdk_container_image=apache/beam_rust_sdk:latest",
        &worker_arg,
        "--num_workers=3",
        "--max_num_workers=10",
        "--sdk_harness_container_image_overrides=.*java.*,apache/beam_java21_sdk:latest",
    ]);

    let worker: WorkerOptions = opts.view_as().expect("WorkerOptions should parse");
    assert_eq!(
        worker.sdk_container_image.as_deref(),
        Some("apache/beam_rust_sdk:latest")
    );
    assert_eq!(worker.worker_binary.as_deref(), Some(binary_str));
    assert_eq!(worker.num_workers, Some(3));
    assert_eq!(worker.max_num_workers, Some(10));
    assert_eq!(
        worker.sdk_harness_container_image_overrides,
        vec![".*java.*,apache/beam_java21_sdk:latest".to_string()]
    );

    // Later reads return the same resolved group.
    let again: WorkerOptions = opts.view_as().expect("WorkerOptions should view");
    assert_eq!(again, worker);
}

#[test]
fn test_worker_options_validation() {
    let opts_invalid_workers =
        PipelineOptions::parse_from(["app", "--num_workers=10", "--max_num_workers=3"]);
    assert!(opts_invalid_workers.view_as::<WorkerOptions>().is_err());

    let opts_nonexistent_binary =
        PipelineOptions::parse_from(["app", "--worker_binary=/nonexistent/path/to/binary"]);
    assert!(opts_nonexistent_binary.view_as::<WorkerOptions>().is_err());
}

#[test]
fn test_resolve_container_image() {
    use beam::options::resolve_container_image;

    let overrides = vec![
        ".*java.*,apache/beam_java21_sdk:custom".to_string(),
        "python=apache/beam_python:v2".to_string(),
    ];

    assert_eq!(
        resolve_container_image("apache/beam_java21_sdk:1.2.3.dev", &overrides),
        "apache/beam_java21_sdk:custom"
    );
    assert_eq!(
        resolve_container_image("apache/beam_python3.11_sdk:latest", &overrides),
        "apache/beam_python:v2"
    );
    assert_eq!(
        resolve_container_image("apache/beam_rust_sdk:latest", &overrides),
        "apache/beam_rust_sdk:latest"
    );
    // Preserved when no override matches.
    assert_eq!(
        resolve_container_image("apache/beam_java21_sdk:1.2.3.dev", &[]),
        "apache/beam_java21_sdk:1.2.3.dev"
    );
}

#[test]
fn test_downcast_to_debug_options() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=prism",
        "--experiments=use_runner_v2,enable_metrics",
        "--async_job",
    ]);

    let debug: DebugOptions = opts.view_as().expect("DebugOptions should parse");
    assert_eq!(debug.experiments, vec!["use_runner_v2", "enable_metrics"]);
    assert!(debug.async_job);

    // Verify aliases --async and --execute_async.
    let opts_async = PipelineOptions::parse_from(["app", "--async"]);
    let debug_async: DebugOptions = opts_async.view_as().unwrap();
    assert!(debug_async.async_job);
}

#[derive(Args, Serialize, Deserialize, Debug, Clone, PartialEq, Eq, Default)]
struct CustomRunnerOptions {
    #[arg(long)]
    parallelism: Option<usize>,

    #[arg(long)]
    checkpoint_interval_ms: Option<u64>,
}

impl PipelineOptionGroup for CustomRunnerOptions {
    fn group_name() -> &'static str {
        "CustomRunnerOptions"
    }

    fn validate(&self) -> Result<(), OptionsError> {
        if let Some(p) = self.parallelism
            && p == 0
        {
            return Err(OptionsError::Validation {
                group: Self::group_name(),
                message: "parallelism must be greater than 0".to_string(),
            });
        }
        Ok(())
    }
}

#[test]
fn test_custom_runner_options_dynamic_downcast() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=custom",
        "--parallelism=4",
        "--checkpoint_interval_ms=5000",
    ]);

    let custom: CustomRunnerOptions = opts.view_as().expect("CustomRunnerOptions should downcast");
    assert_eq!(custom.parallelism, Some(4));
    assert_eq!(custom.checkpoint_interval_ms, Some(5000));
}

#[test]
fn test_custom_runner_options_validation_failure() {
    let opts = PipelineOptions::parse_from(["app", "--runner=custom", "--parallelism=0"]);

    let err = opts
        .view_as::<CustomRunnerOptions>()
        .expect_err("parallelism=0 must fail validation");

    match err {
        OptionsError::Validation { group, message } => {
            assert_eq!(group, "CustomRunnerOptions");
            assert!(message.contains("parallelism must be greater than 0"));
        }
        other => panic!("expected Validation error, got: {other:?}"),
    }
}

#[test]
fn test_programmatic_options_injection() {
    let opts = PipelineOptions::with_runner("custom");
    opts.set(CustomRunnerOptions {
        parallelism: Some(8),
        checkpoint_interval_ms: Some(1000),
    })
    .expect("valid options are accepted");

    assert!(opts.contains::<CustomRunnerOptions>());

    let retrieved: CustomRunnerOptions = opts
        .view_as()
        .expect("programmatically set options should be retrieved");
    assert_eq!(retrieved.parallelism, Some(8));
    assert_eq!(retrieved.checkpoint_interval_ms, Some(1000));
}

#[test]
fn test_portable_options_defaults() {
    let opts = PortableOptions::default();
    assert_eq!(opts.environment_type, None);
    assert_eq!(opts.environment_config, None);
    assert!(!opts.is_docker());
    assert!(!opts.is_loopback());
    assert_eq!(opts.container_image("fallback:latest"), "fallback:latest");
}

#[test]
fn test_portable_options_docker_and_loopback_predicates() {
    let docker_upper = PortableOptions {
        environment_type: Some("DOCKER".to_string()),
        environment_config: Some("my-image:v1".to_string()),
    };
    assert!(docker_upper.is_docker());
    assert!(!docker_upper.is_loopback());
    assert_eq!(
        docker_upper.container_image("fallback:latest"),
        "my-image:v1"
    );

    let docker_lower = PortableOptions {
        environment_type: Some("docker".to_string()),
        environment_config: None,
    };
    assert!(docker_lower.is_docker());
    assert_eq!(
        docker_lower.container_image("fallback:latest"),
        "fallback:latest"
    );

    let loopback = PortableOptions {
        environment_type: Some("LOOPBACK".to_string()),
        environment_config: None,
    };
    assert!(loopback.is_loopback());
    assert!(!loopback.is_docker());
}

#[test]
fn test_downcast_to_portable_options() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=prism",
        "--environment_type=DOCKER",
        "--environment_config=apache/beam_rust_sdk:custom",
    ]);

    let portable: PortableOptions = opts.view_as().expect("PortableOptions should parse");
    assert_eq!(portable.environment_type.as_deref(), Some("DOCKER"));
    assert_eq!(
        portable.environment_config.as_deref(),
        Some("apache/beam_rust_sdk:custom")
    );
    assert!(portable.is_docker());

    // Verify aliases --environmentType and --environmentConfig.
    let opts_alias = PipelineOptions::parse_from([
        "app",
        "--environmentType=LOOPBACK",
        "--environmentConfig=localhost:50000",
    ]);
    let portable_alias: PortableOptions = opts_alias.view_as().unwrap();
    assert_eq!(portable_alias.environment_type.as_deref(), Some("LOOPBACK"));
    assert_eq!(
        portable_alias.environment_config.as_deref(),
        Some("localhost:50000")
    );
    assert!(portable_alias.is_loopback());
}

#[test]
fn test_programmatic_options_are_validated() {
    let opts = PipelineOptions::with_runner("custom");
    let err = opts
        .set(CustomRunnerOptions {
            parallelism: Some(0),
            checkpoint_interval_ms: None,
        })
        .expect_err("invalid options are rejected");

    assert!(matches!(err, OptionsError::Validation { .. }));
    assert!(!opts.contains::<CustomRunnerOptions>());
}

#[test]
fn test_groups_default_without_arguments() {
    let opts = PipelineOptions::default();

    let portable: PortableOptions = opts.view_as().expect("defaults parse");
    assert_eq!(portable, PortableOptions::default());

    let debug: DebugOptions = opts.view_as().expect("defaults parse");
    assert!(debug.experiments.is_empty());
    assert!(!debug.async_job);
}

#[test]
fn test_resolved_groups_are_shared_by_clones() {
    let opts = PipelineOptions::parse_from(["app", "--parallelism=4"]);
    let clone = opts.clone();

    let _: CustomRunnerOptions = opts.view_as().expect("parses");
    assert!(clone.contains::<CustomRunnerOptions>());
}

#[test]
fn test_pipeline_options_equality_and_inequality() {
    let opt1 = PipelineOptions::parse_from(["app", "--runner=prism"]);
    let opt2 = PipelineOptions::parse_from(["app", "--runner=prism"]);
    assert_eq!(opt1, opt2);

    let opt_diff_runner = PipelineOptions::parse_from(["app", "--runner=dataflow"]);
    assert_ne!(opt1, opt_diff_runner);

    let opt_diff_args = PipelineOptions::parse_from(["app", "--runner=prism", "--num_workers=2"]);
    assert_ne!(opt1, opt_diff_args);

    let mut opt_streaming = opt1.clone();
    opt_streaming.streaming = true;
    assert_ne!(opt1, opt_streaming);

    let mut opt_endpoint = opt1.clone();
    opt_endpoint.endpoint = Some("localhost:1234".to_string());
    assert_ne!(opt1, opt_endpoint);

    let mut opt_job_name = opt1.clone();
    opt_job_name.job_name = Some("job-xyz".to_string());
    assert_ne!(opt1, opt_job_name);
}

#[test]
fn test_from_args_parses_the_process_command_line() {
    // No option group declares the flags of the test harness, so the parser ignores them.
    let opts = PipelineOptions::from_args();
    assert_eq!(opts.runner, "prism");
}
