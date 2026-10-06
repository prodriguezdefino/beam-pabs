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

use std::fs::File;
use std::io::{Read, Write};

use beam_io_file::{FileSystem, LocalFileSystem};
use beam_model::pipeline as proto;
use beam_runner_dataflow::{
    compute_sha256, stage_and_resolve_environment_artifacts, stage_pipeline_model,
    stage_worker_binary,
};
use prost::Message;

#[test]
fn stage_model_and_worker_binary() {
    // FIPS 180-4 standard test vector verifies compute_sha256.
    assert_eq!(
        compute_sha256(b"abc"),
        "ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad"
    );

    let fs = LocalFileSystem::new();
    let temp_dir =
        std::env::temp_dir().join(format!("dataflow_stage_artifacts_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let staging_loc = temp_dir.to_str().unwrap();

    enum ArtifactInput<'a> {
        Model(&'a [u8]),
        Worker(&'a [u8]),
    }

    let cases = [
        (
            "job-model",
            ArtifactInput::Model(b"sample pipeline protobuf bytes"),
            "model",
        ),
        (
            "job-worker",
            ArtifactInput::Worker(b"\x7fELF\x02\x01\x01mock_executable_bytes"),
            "worker",
        ),
    ];

    for (job_id, input, artifact_name) in cases {
        let (url, hash, bytes) = match input {
            ArtifactInput::Model(bytes) => {
                let (url, hash) = stage_pipeline_model(&fs, staging_loc, job_id, bytes).unwrap();
                (url, hash, bytes)
            }
            ArtifactInput::Worker(bytes) => {
                let bin_path = temp_dir.join(format!("{job_id}_bin"));
                let mut file = File::create(&bin_path).unwrap();
                file.write_all(bytes).unwrap();
                let (url, hash) = stage_worker_binary(&fs, staging_loc, &bin_path).unwrap();
                (url, hash, bytes)
            }
        };

        assert_eq!(hash, compute_sha256(bytes));
        let expected_url = match artifact_name {
            "worker" => format!("{staging_loc}/worker-{hash}"),
            _ => format!("{staging_loc}/{job_id}/{artifact_name}"),
        };
        assert_eq!(url, expected_url);

        let mut staged = Vec::new();
        let mut reader = fs.open_read(&url).unwrap();
        reader.read_to_end(&mut staged).unwrap();
        assert_eq!(staged, bytes);
    }

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn stage_worker_binary_skips_an_object_that_is_already_staged() {
    let fs = LocalFileSystem::new();
    let temp_dir =
        std::env::temp_dir().join(format!("dataflow_stage_worker_once_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let staging_loc = temp_dir.to_str().unwrap();
    let bin_path = temp_dir.join("worker_bin");
    std::fs::write(&bin_path, b"worker bytes").unwrap();

    let (first_url, first_hash) = stage_worker_binary(&fs, staging_loc, &bin_path).unwrap();
    // Same size, other bytes: a second upload would write the binary again.
    std::fs::write(&first_url, b"marker bytes").unwrap();
    let (second_url, second_hash) = stage_worker_binary(&fs, staging_loc, &bin_path).unwrap();

    assert_eq!(
        (second_url.as_str(), second_hash.as_str()),
        (first_url.as_str(), first_hash.as_str())
    );
    assert_eq!(std::fs::read(&second_url).unwrap(), b"marker bytes");

    // Other size: the object is not this binary, so staging writes it again.
    std::fs::write(&first_url, b"short").unwrap();
    stage_worker_binary(&fs, staging_loc, &bin_path).unwrap();
    assert_eq!(std::fs::read(&first_url).unwrap(), b"worker bytes");

    let _ = std::fs::remove_dir_all(&temp_dir);
}

#[test]
fn test_stage_and_resolve_environment_artifacts() {
    let fs = LocalFileSystem::new();
    let temp_dir = std::env::temp_dir().join(format!("dataflow_stage_env_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let staging_loc = temp_dir.to_str().unwrap();

    // Create a dummy local jar file to simulate a file artifact
    let dummy_jar = temp_dir.join("dummy-expansion.jar");
    std::fs::write(&dummy_jar, b"dummy jar content").unwrap();

    let file_payload = proto::ArtifactFilePayload {
        path: dummy_jar.to_string_lossy().to_string(),
        sha256: compute_sha256(b"dummy jar content"),
    };

    let mut pipeline = proto::Pipeline {
        components: Some(proto::Components {
            environments: [(
                "java_env".to_string(),
                proto::Environment {
                    urn: beam::pipeline::URN_ENV_DOCKER.to_string(),
                    payload: b"apache/beam_java21_sdk:2.64.0".to_vec(),
                    dependencies: vec![proto::ArtifactInformation {
                        type_urn: "beam:artifact:type:file:v1".to_string(),
                        type_payload: file_payload.encode_to_vec(),
                        role_urn: "beam:artifact:role:staging_to:v1".to_string(),
                        role_payload: proto::ArtifactStagingToRolePayload {
                            staged_name: "custom-expansion.jar".to_string(),
                        }
                        .encode_to_vec(),
                    }],
                    capabilities: Vec::new(),
                    display_data: Vec::new(),
                    resource_hints: std::collections::HashMap::new(),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let packages =
        stage_and_resolve_environment_artifacts(&fs, staging_loc, "job-789", &mut pipeline, &[])
            .unwrap();

    assert_eq!(packages.len(), 1);
    assert_eq!(packages[0].name, "custom-expansion.jar");

    let env = pipeline
        .components
        .unwrap()
        .environments
        .remove("java_env")
        .unwrap();
    assert_eq!(env.dependencies.len(), 1);
    assert_eq!(env.dependencies[0].type_urn, "beam:artifact:type:url:v1");

    let url_payload =
        proto::ArtifactUrlPayload::decode(env.dependencies[0].type_payload.as_slice()).unwrap();
    assert!(
        url_payload
            .url
            .contains("job-789/xlang/custom-expansion.jar")
    );
    assert_eq!(url_payload.sha256, compute_sha256(b"dummy jar content"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

/// A deferred artifact names no path, so it must be resolved against the expansion JAR the
/// pipeline recorded while expanding — not against an arbitrary file found on disk.
#[test]
fn test_deferred_artifact_resolves_to_recorded_expansion_jar() {
    let fs = LocalFileSystem::new();
    let temp_dir =
        std::env::temp_dir().join(format!("dataflow_stage_deferred_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);
    let staging_loc = temp_dir.to_str().unwrap();

    let recorded_jar = temp_dir.join("beam-sdks-java-expansion-service-2.64.0.jar");
    std::fs::write(&recorded_jar, b"recorded jar").unwrap();

    let mut pipeline = proto::Pipeline {
        components: Some(proto::Components {
            environments: [(
                "java_env".to_string(),
                proto::Environment {
                    urn: beam::pipeline::URN_ENV_DOCKER.to_string(),
                    payload: Vec::new(),
                    dependencies: vec![proto::ArtifactInformation {
                        type_urn: "beam:artifact:type:deferred:v1".to_string(),
                        ..Default::default()
                    }],
                    capabilities: Vec::new(),
                    display_data: Vec::new(),
                    resource_hints: std::collections::HashMap::new(),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let packages = stage_and_resolve_environment_artifacts(
        &fs,
        staging_loc,
        "job-deferred",
        &mut pipeline,
        std::slice::from_ref(&recorded_jar),
    )
    .unwrap();

    assert_eq!(packages.len(), 1);
    assert_eq!(
        packages[0].name,
        "beam-sdks-java-expansion-service-2.64.0.jar"
    );

    let env = pipeline
        .components
        .unwrap()
        .environments
        .remove("java_env")
        .unwrap();
    assert_eq!(env.dependencies.len(), 1);
    assert_eq!(env.dependencies[0].type_urn, "beam:artifact:type:url:v1");

    let url_payload =
        proto::ArtifactUrlPayload::decode(env.dependencies[0].type_payload.as_slice()).unwrap();
    assert_eq!(url_payload.sha256, compute_sha256(b"recorded jar"));

    let _ = std::fs::remove_dir_all(&temp_dir);
}

/// With no expansion artifact recorded there is nothing safe to stage, so the dependency
/// must be left alone: staging an unrelated JAR would fail at run time, far from the cause.
#[test]
fn test_deferred_artifact_without_recorded_jar_is_left_untouched() {
    let fs = LocalFileSystem::new();
    let temp_dir =
        std::env::temp_dir().join(format!("dataflow_stage_nodefer_{}", std::process::id()));
    let _ = std::fs::create_dir_all(&temp_dir);

    let mut pipeline = proto::Pipeline {
        components: Some(proto::Components {
            environments: [(
                "java_env".to_string(),
                proto::Environment {
                    urn: beam::pipeline::URN_ENV_DOCKER.to_string(),
                    payload: Vec::new(),
                    dependencies: vec![proto::ArtifactInformation {
                        type_urn: "beam:artifact:type:deferred:v1".to_string(),
                        ..Default::default()
                    }],
                    capabilities: Vec::new(),
                    display_data: Vec::new(),
                    resource_hints: std::collections::HashMap::new(),
                },
            )]
            .into_iter()
            .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let packages = stage_and_resolve_environment_artifacts(
        &fs,
        temp_dir.to_str().unwrap(),
        "job-none",
        &mut pipeline,
        &[],
    )
    .unwrap();

    assert!(packages.is_empty());
    let env = pipeline
        .components
        .unwrap()
        .environments
        .remove("java_env")
        .unwrap();
    assert_eq!(
        env.dependencies[0].type_urn,
        "beam:artifact:type:deferred:v1"
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}

/// A Docker environment with `dependencies`.
fn environment_with(dependencies: Vec<proto::ArtifactInformation>) -> proto::Environment {
    proto::Environment {
        urn: beam::pipeline::URN_ENV_DOCKER.to_string(),
        dependencies,
        ..Default::default()
    }
}

/// A `file` artifact at `path` with the staged name `staged_name`.
fn file_dependency(path: &std::path::Path, staged_name: &str) -> proto::ArtifactInformation {
    proto::ArtifactInformation {
        type_urn: beam::pipeline::URN_ARTIFACT_TYPE_FILE.to_string(),
        type_payload: proto::ArtifactFilePayload {
            path: path.to_string_lossy().into_owned(),
            sha256: String::new(),
        }
        .encode_to_vec(),
        role_urn: beam::pipeline::URN_ARTIFACT_ROLE_STAGING_TO.to_string(),
        role_payload: proto::ArtifactStagingToRolePayload {
            staged_name: staged_name.to_string(),
        }
        .encode_to_vec(),
    }
}

/// Two files with the same name get two staged names, so neither overwrites the other.
#[test]
fn files_with_the_same_name_are_staged_under_different_names() {
    let fs = LocalFileSystem::new();
    let temp_dir =
        std::env::temp_dir().join(format!("dataflow_stage_collide_{}", std::process::id()));
    let (dir_a, dir_b) = (temp_dir.join("a"), temp_dir.join("b"));
    std::fs::create_dir_all(&dir_a).unwrap();
    std::fs::create_dir_all(&dir_b).unwrap();
    let (jar_a, jar_b) = (dir_a.join("io.jar"), dir_b.join("io.jar"));
    std::fs::write(&jar_a, b"jar a").unwrap();
    std::fs::write(&jar_b, b"jar b").unwrap();
    let staging_loc = temp_dir.join("staging");
    let mut pipeline = proto::Pipeline {
        components: Some(proto::Components {
            environments: [
                (
                    "env_a".to_string(),
                    environment_with(vec![file_dependency(&jar_a, "io.jar")]),
                ),
                (
                    "env_b".to_string(),
                    environment_with(vec![file_dependency(&jar_b, "io.jar")]),
                ),
            ]
            .into_iter()
            .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let packages = stage_and_resolve_environment_artifacts(
        &fs,
        staging_loc.to_str().unwrap(),
        "job-collide",
        &mut pipeline,
        &[],
    )
    .unwrap();

    let mut names: Vec<&str> = packages.iter().map(|p| p.name.as_str()).collect();
    names.sort_unstable();
    assert_eq!(names, ["io-1.jar", "io.jar"]);
    let mut contents: Vec<Vec<u8>> = packages
        .iter()
        .map(|p| std::fs::read(&p.location).unwrap())
        .collect();
    contents.sort();
    assert_eq!(contents, [b"jar a".to_vec(), b"jar b".to_vec()]);

    let _ = std::fs::remove_dir_all(&temp_dir);
}

/// With more than one expansion artifact, staging keeps a deferred artifact as it is.
#[test]
fn deferred_artifact_with_many_recorded_jars_is_left_untouched() {
    let fs = LocalFileSystem::new();
    let temp_dir =
        std::env::temp_dir().join(format!("dataflow_stage_many_jars_{}", std::process::id()));
    std::fs::create_dir_all(&temp_dir).unwrap();
    let jars = [temp_dir.join("one.jar"), temp_dir.join("two.jar")];
    jars.iter()
        .for_each(|jar| std::fs::write(jar, b"jar").unwrap());
    let deferred = proto::ArtifactInformation {
        type_urn: beam::pipeline::URN_ARTIFACT_TYPE_DEFERRED.to_string(),
        ..Default::default()
    };
    let mut pipeline = proto::Pipeline {
        components: Some(proto::Components {
            environments: [("env".to_string(), environment_with(vec![deferred.clone()]))]
                .into_iter()
                .collect(),
            ..Default::default()
        }),
        ..Default::default()
    };

    let packages = stage_and_resolve_environment_artifacts(
        &fs,
        temp_dir.join("staging").to_str().unwrap(),
        "job-many",
        &mut pipeline,
        &jars,
    )
    .unwrap();

    assert!(packages.is_empty());
    assert_eq!(
        pipeline.components.unwrap().environments["env"].dependencies,
        [deferred]
    );

    let _ = std::fs::remove_dir_all(&temp_dir);
}
