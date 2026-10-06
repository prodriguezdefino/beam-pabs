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

//! Unit tests for the Prism runner's pure pipeline-rewriting and config helpers.
#![expect(clippy::unwrap_used, reason = "integration test helpers")]

use std::collections::HashMap;

use beam::options::PipelineOptions;
use beam::pipeline::{
    URN_ARTIFACT_ROLE_WORKER_BINARY, URN_ARTIFACT_TYPE_FILE, URN_ENV_DOCKER, URN_ENV_EXTERNAL,
    default_sdk_container_image,
};
use model::job_management::job_state;
use model::pipeline::{
    ArtifactFilePayload, Components, DisplayData, DockerPayload, Environment, FunctionSpec,
    PTransform, WindowingStrategy,
};
use prism::PrismRunnerOptions;
use prism::runner::{
    PrismRunnerError, apply_container_image_overrides, bind_pipeline_environment,
    docker_environment_for, merge_identical_environments, terminal_outcome,
};
use prost::Message;

fn docker_env(image: &str) -> Environment {
    Environment {
        urn: URN_ENV_DOCKER.to_string(),
        payload: DockerPayload {
            container_image: image.to_string(),
        }
        .encode_to_vec(),
        ..Default::default()
    }
}

fn image_of(env: &Environment) -> String {
    assert_eq!(env.urn, URN_ENV_DOCKER);
    DockerPayload::decode(env.payload.as_slice())
        .unwrap()
        .container_image
}

fn transform(urn: &str, env: &str) -> PTransform {
    PTransform {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            payload: Vec::new(),
        }),
        environment_id: env.to_string(),
        ..Default::default()
    }
}

fn ws(env: &str) -> WindowingStrategy {
    WindowingStrategy {
        environment_id: env.to_string(),
        ..Default::default()
    }
}

fn options(args: &[&str]) -> PrismRunnerOptions {
    let argv = std::iter::once("app").chain(args.iter().copied());
    PrismRunnerOptions::from(&PipelineOptions::parse_from(argv))
}

fn env_ids(c: &Components) -> Vec<&str> {
    let mut ids: Vec<&str> = c.environments.keys().map(String::as_str).collect();
    ids.sort_unstable();
    ids
}

// ---------------------------------------------------------------------------
// bind_pipeline_environment
// ---------------------------------------------------------------------------

#[test]
fn bind_moves_only_this_sdks_transforms_and_leaves_foreign_ones() {
    let mut c = Components {
        environments: HashMap::from([
            ("env_default".to_string(), Environment::default()),
            ("java_env".to_string(), docker_env("apache/beam_java17_sdk")),
        ]),
        transforms: HashMap::from([
            (
                "impulse".to_string(),
                transform("beam:transform:impulse:v1", "env_default"),
            ),
            (
                "gbk".to_string(),
                transform("beam:transform:group_by_key:v1", ""),
            ),
            (
                "rust_pardo".to_string(),
                transform("beam:transform:pardo:v1", "env_default"),
            ),
            (
                "rust_unbound".to_string(),
                transform("beam:transform:pardo:v1", ""),
            ),
            (
                "java_pardo".to_string(),
                transform("beam:transform:pardo:v1", "java_env"),
            ),
        ]),
        windowing_strategies: HashMap::from([
            ("ws_rust".to_string(), ws("env_default")),
            ("ws_empty".to_string(), ws("")),
            ("ws_java".to_string(), ws("java_env")),
        ]),
        ..Default::default()
    };
    let new_env = docker_env("rust:img");

    bind_pipeline_environment(&mut c, "env_default", "env_new", new_env.clone());

    assert_eq!(env_ids(&c), vec!["env_new", "java_env"]);
    assert_eq!(c.environments["env_new"], new_env);
    assert_eq!(
        image_of(&c.environments["java_env"]),
        "apache/beam_java17_sdk"
    );

    let env_of = |id: &str| c.transforms[id].environment_id.as_str();
    assert_eq!(env_of("impulse"), "", "runner transforms carry no env");
    assert_eq!(env_of("gbk"), "");
    assert_eq!(env_of("rust_pardo"), "env_new");
    assert_eq!(env_of("rust_unbound"), "env_new");
    assert_eq!(
        env_of("java_pardo"),
        "java_env",
        "foreign transform rebound"
    );

    let ws_env = |id: &str| c.windowing_strategies[id].environment_id.as_str();
    assert_eq!(ws_env("ws_rust"), "env_new");
    assert_eq!(ws_env("ws_empty"), "env_new");
    assert_eq!(ws_env("ws_java"), "java_env");
}

// ---------------------------------------------------------------------------
// merge_identical_environments
// ---------------------------------------------------------------------------

#[test]
fn merge_collapses_equivalent_environments_into_lowest_id() {
    let mut a = docker_env("java:img");
    a.capabilities = vec!["cap:b".into(), "cap:a".into()];
    let mut b = a.clone();
    // Order of capabilities and display data do not distinguish environments.
    b.capabilities = vec!["cap:a".into(), "cap:b".into()];
    b.display_data = vec![DisplayData {
        urn: "beam:display_data:labelled:v1".into(),
        payload: vec![1],
    }];
    let distinct = docker_env("other:img");

    let mut c = Components {
        environments: HashMap::from([
            ("z_env".to_string(), b),
            ("a_env".to_string(), a.clone()),
            ("m_env".to_string(), distinct.clone()),
        ]),
        transforms: HashMap::from([
            (
                "t1".to_string(),
                transform("beam:transform:pardo:v1", "z_env"),
            ),
            (
                "t2".to_string(),
                transform("beam:transform:pardo:v1", "a_env"),
            ),
            (
                "t3".to_string(),
                transform("beam:transform:pardo:v1", "m_env"),
            ),
            ("t4".to_string(), transform("beam:transform:impulse:v1", "")),
        ]),
        windowing_strategies: HashMap::from([
            ("w1".to_string(), ws("z_env")),
            ("w2".to_string(), ws("m_env")),
        ]),
        ..Default::default()
    };

    merge_identical_environments(&mut c);

    assert_eq!(env_ids(&c), vec!["a_env", "m_env"]);
    assert_eq!(c.environments["a_env"], a);
    assert_eq!(c.environments["m_env"], distinct);
    let env_of = |id: &str| c.transforms[id].environment_id.as_str();
    assert_eq!(env_of("t1"), "a_env");
    assert_eq!(env_of("t2"), "a_env");
    assert_eq!(env_of("t3"), "m_env");
    assert_eq!(env_of("t4"), "");
    assert_eq!(c.windowing_strategies["w1"].environment_id, "a_env");
    assert_eq!(c.windowing_strategies["w2"].environment_id, "m_env");
}

#[test]
fn merge_keeps_environments_that_differ_in_dependencies() {
    let mut a = docker_env("java:img");
    a.dependencies = vec![model::pipeline::ArtifactInformation {
        type_urn: URN_ARTIFACT_TYPE_FILE.into(),
        type_payload: vec![1],
        ..Default::default()
    }];
    let mut b = a.clone();
    b.dependencies[0].type_payload = vec![2];
    let mut c = Components {
        environments: HashMap::from([("a".to_string(), a), ("b".to_string(), b)]),
        transforms: HashMap::from([("t".to_string(), transform("u", "b"))]),
        ..Default::default()
    };
    let before = c.clone();
    merge_identical_environments(&mut c);
    assert_eq!(c, before);
}

// ---------------------------------------------------------------------------
// apply_container_image_overrides
// ---------------------------------------------------------------------------

#[test]
fn overrides_rewrite_only_matching_docker_images() {
    let external = Environment {
        urn: URN_ENV_EXTERNAL.to_string(),
        payload: b"java".to_vec(),
        ..Default::default()
    };
    let malformed = Environment {
        urn: URN_ENV_DOCKER.to_string(),
        payload: vec![0xff, 0xff],
        ..Default::default()
    };
    let mut c = Components {
        environments: HashMap::from([
            (
                "java".to_string(),
                docker_env("apache/beam_java11_sdk:2.60.0"),
            ),
            (
                "rust".to_string(),
                docker_env("apache/beam_rust_sdk:2.60.0"),
            ),
            ("external".to_string(), external.clone()),
            ("malformed".to_string(), malformed.clone()),
        ]),
        ..Default::default()
    };

    apply_container_image_overrides(&mut c, &[".*java.*,my/java:dev".to_string()]);

    assert_eq!(image_of(&c.environments["java"]), "my/java:dev");
    assert_eq!(
        image_of(&c.environments["rust"]),
        "apache/beam_rust_sdk:2.60.0"
    );
    assert_eq!(c.environments["external"], external);
    assert_eq!(c.environments["malformed"], malformed);

    // An empty override list is a no-op; the `pattern=image` form is accepted too.
    let before = c.clone();
    apply_container_image_overrides(&mut c, &[]);
    assert_eq!(c, before);
    apply_container_image_overrides(&mut c, &["java=replaced:1".to_string()]);
    assert_eq!(image_of(&c.environments["java"]), "replaced:1");
}

// ---------------------------------------------------------------------------
// docker_environment_for
// ---------------------------------------------------------------------------

/// A file standing in for a Linux pipeline binary, since `WorkerOptions` validation requires
/// `--worker_binary` to exist.
struct TempBinary(std::path::PathBuf);

impl TempBinary {
    fn new(name: &str) -> Self {
        let path = std::env::temp_dir().join(format!(
            "beam_prism_runner_config_{name}_{}",
            std::process::id()
        ));
        std::fs::write(&path, b"elf").unwrap();
        Self(path)
    }

    fn path(&self) -> &str {
        self.0.to_str().unwrap()
    }

    fn flag(&self) -> String {
        format!("--worker_binary={}", self.path())
    }
}

impl Drop for TempBinary {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

/// The path of the single staged worker binary `env` declares.
fn staged_binary_of(env: &Environment) -> String {
    assert_eq!(env.dependencies.len(), 1, "{:?}", env.dependencies);
    let dep = &env.dependencies[0];
    assert_eq!(dep.type_urn, URN_ARTIFACT_TYPE_FILE);
    assert_eq!(dep.role_urn, URN_ARTIFACT_ROLE_WORKER_BINARY);
    assert!(dep.role_payload.is_empty());
    let payload = ArtifactFilePayload::decode(dep.type_payload.as_slice()).unwrap();
    assert_eq!(payload.sha256, "");
    payload.path
}

#[test]
fn docker_env_is_none_without_image_flags_or_with_explicit_loopback() {
    assert_eq!(docker_environment_for(&options(&[])).unwrap(), None);
    // Explicit LOOPBACK wins over either image flag.
    for image_flag in ["--environment_config=img:b", "--sdk_container_image=img:b"] {
        assert_eq!(
            docker_environment_for(&options(&["--environment_type=LOOPBACK", image_flag])).unwrap(),
            None
        );
    }
}

#[test]
fn docker_env_is_implied_by_either_image_flag() {
    for image_flag in [
        "--environment_config=my/img:1",
        "--sdk_container_image=my/img:1",
    ] {
        let env = docker_environment_for(&options(&[image_flag]))
            .unwrap()
            .expect("an image flag implies docker mode");
        assert_eq!(image_of(&env), "my/img:1");
        // Nothing staged: the image is assumed to have the binary pre-baked.
        assert!(env.dependencies.is_empty());
    }
}

#[test]
fn docker_env_uses_configured_image_with_docker_capability_first() {
    let env = docker_environment_for(&options(&[
        "--environment_type=docker",
        "--environment_config=my/img:1",
    ]))
    .unwrap()
    .expect("docker mode");
    assert_eq!(image_of(&env), "my/img:1");
    assert_eq!(env.capabilities[0], URN_ENV_DOCKER);
    assert_eq!(
        env.capabilities[1..],
        beam::pipeline::standard_capabilities()[..]
    );
}

#[test]
fn docker_env_stages_worker_binary() {
    let binary = TempBinary::new("stages_worker_binary");
    // (extra flags, expected image): default image when none is configured.
    let cases: [(&[&str], String); 2] = [
        (
            &["--environment_type=DOCKER"],
            default_sdk_container_image(),
        ),
        (
            &["--sdk_container_image=custom/worker:v1"],
            "custom/worker:v1".to_string(),
        ),
    ];
    for (flags, image) in cases {
        let mut args: Vec<&str> = flags.to_vec();
        let flag = binary.flag();
        args.push(&flag);
        let env = docker_environment_for(&options(&args))
            .unwrap()
            .expect("docker mode");
        assert_eq!(image_of(&env), image, "{flags:?}");
        assert_eq!(staged_binary_of(&env), binary.path(), "{flags:?}");
    }
}

#[test]
fn docker_env_rejects_invalid_flags() {
    // (flags, substring the error must mention, must be an Options error).
    let cases: [(&[&str], &str, bool); 3] = [
        (&["--environment_type=DOCKER"], "--worker_binary", true),
        (
            &[
                "--environment_config=a/img:1",
                "--sdk_container_image=b/img:2",
            ],
            "",
            true,
        ),
        (
            &[
                "--environment_type=DOCKER",
                "--worker_binary=/nonexistent/beam/worker/binary",
            ],
            "/nonexistent/beam/worker/binary",
            false,
        ),
    ];
    for (flags, needle, options_error) in cases {
        let err = docker_environment_for(&options(flags)).unwrap_err();
        if options_error {
            assert!(
                matches!(err, PrismRunnerError::Options(_)),
                "{flags:?}: {err}"
            );
        }
        assert!(err.to_string().contains(needle), "{flags:?}: {err}");
    }
}

// ---------------------------------------------------------------------------
// terminal_outcome
// ---------------------------------------------------------------------------

#[test]
fn terminal_outcome_table() {
    // DONE is a success carrying the job id and state name.
    let res = terminal_outcome(job_state::Enum::Done as i32, "job1", "ignored")
        .expect("terminal")
        .expect("success");
    assert_eq!(res.job_id, "job1");
    assert_eq!(res.state, "DONE");

    // FAILED includes the last error when present, bare job id otherwise.
    let err = terminal_outcome(job_state::Enum::Failed as i32, "job1", "boom")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(&err, PrismRunnerError::JobFailed(detail) if detail == "job1: boom"),
        "{err:?}"
    );
    let err = terminal_outcome(job_state::Enum::Failed as i32, "job1", "")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(&err, PrismRunnerError::JobFailed(detail) if detail == "job1"),
        "{err:?}"
    );
    assert_eq!(err.to_string(), "Job 'job1' failed during execution");

    // CANCELLED is its own error.
    let err = terminal_outcome(job_state::Enum::Cancelled as i32, "job9", "msg")
        .unwrap()
        .unwrap_err();
    assert!(
        matches!(&err, PrismRunnerError::JobCancelled(id) if id == "job9"),
        "{err:?}"
    );

    // Non-terminal and unknown states are None.
    for state in [
        job_state::Enum::Unspecified,
        job_state::Enum::Stopped,
        job_state::Enum::Running,
        job_state::Enum::Starting,
        job_state::Enum::Cancelling,
    ] {
        assert!(
            terminal_outcome(state as i32, "j", "").is_none(),
            "{state:?} treated as terminal"
        );
    }
    assert!(terminal_outcome(9999, "j", "").is_none());
}
