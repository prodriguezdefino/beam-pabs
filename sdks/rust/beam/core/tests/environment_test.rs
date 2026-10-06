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

//! Tests for worker container image resolution across runners.

use beam::options::{PortableOptions, WorkerOptions};
use beam::pipeline::{DockerEnvironment, PREBAKED_WORKER_BINARY_PATH, default_sdk_container_image};

fn portable(environment_type: Option<&str>, environment_config: Option<&str>) -> PortableOptions {
    PortableOptions {
        environment_type: environment_type.map(str::to_string),
        environment_config: environment_config.map(str::to_string),
    }
}

fn worker(sdk_container_image: Option<&str>, worker_binary: Option<&str>) -> WorkerOptions {
    WorkerOptions {
        sdk_container_image: sdk_container_image.map(str::to_string),
        worker_binary: worker_binary.map(str::to_string),
        ..WorkerOptions::default()
    }
}

fn docker(image: &str, worker_binary: Option<&str>) -> DockerEnvironment {
    DockerEnvironment {
        image: image.to_string(),
        worker_binary: worker_binary.map(str::to_string),
    }
}

#[test]
fn resolve_stages_the_binary_in_the_given_image() {
    assert_eq!(
        DockerEnvironment::resolve(Some("registry/rust:1"), Some("/out/wordcount")),
        Ok(docker("registry/rust:1", Some("/out/wordcount")))
    );
}

#[test]
fn resolve_stages_the_binary_in_the_default_image_when_none_is_given() {
    assert_eq!(
        DockerEnvironment::resolve(None, Some("/out/wordcount")),
        Ok(docker(
            &default_sdk_container_image(),
            Some("/out/wordcount")
        ))
    );
}

#[test]
fn resolve_treats_an_image_without_a_binary_as_prebaked() {
    assert_eq!(
        DockerEnvironment::resolve(Some("registry/wordcount:1"), None),
        Ok(docker("registry/wordcount:1", None))
    );
}

#[test]
fn resolve_skips_staging_when_binary_matches_prebaked_path_in_custom_image() {
    assert_eq!(
        DockerEnvironment::resolve(
            Some("registry/wordcount:1"),
            Some(PREBAKED_WORKER_BINARY_PATH)
        ),
        Ok(docker("registry/wordcount:1", None))
    );
}

#[test]
fn resolve_stages_binary_when_matching_prebaked_path_in_default_image() {
    assert_eq!(
        DockerEnvironment::resolve(
            Some(&default_sdk_container_image()),
            Some(PREBAKED_WORKER_BINARY_PATH)
        ),
        Ok(docker(
            &default_sdk_container_image(),
            Some(PREBAKED_WORKER_BINARY_PATH)
        ))
    );
}

#[test]
fn resolve_fails_with_both_remedies_when_there_is_nothing_to_run() {
    let message = DockerEnvironment::resolve(None, None)
        .unwrap_err()
        .to_string();
    for remedy in [
        "--worker_binary",
        "--sdk_container_image",
        PREBAKED_WORKER_BINARY_PATH,
    ] {
        assert!(message.contains(remedy), "missing '{remedy}': {message}");
    }
}

#[test]
fn resolve_treats_empty_values_as_unset() {
    assert!(DockerEnvironment::resolve(Some(""), Some("")).is_err());
    assert_eq!(
        DockerEnvironment::resolve(Some(""), Some("/out/wordcount")),
        Ok(docker(
            &default_sdk_container_image(),
            Some("/out/wordcount")
        ))
    );
}

#[test]
fn portable_runs_loopback_without_image_flags() {
    assert_eq!(
        DockerEnvironment::for_portable_runner(&portable(None, None), &worker(None, None)),
        Ok(None)
    );
    // Specifying only a worker binary defaults to loopback mode.
    assert_eq!(
        DockerEnvironment::for_portable_runner(
            &portable(None, None),
            &worker(None, Some("/out/wordcount"))
        ),
        Ok(None)
    );
}

#[test]
fn portable_runs_docker_when_either_image_flag_is_given() {
    for (portable, worker) in [
        (
            portable(None, Some("registry/rust:1")),
            worker(None, Some("/out/wordcount")),
        ),
        (
            portable(None, None),
            worker(Some("registry/rust:1"), Some("/out/wordcount")),
        ),
    ] {
        assert_eq!(
            DockerEnvironment::for_portable_runner(&portable, &worker),
            Ok(Some(docker("registry/rust:1", Some("/out/wordcount"))))
        );
    }
}

#[test]
fn portable_accepts_the_same_image_in_both_flags() {
    assert_eq!(
        DockerEnvironment::for_portable_runner(
            &portable(None, Some("registry/rust:1")),
            &worker(Some("registry/rust:1"), None)
        ),
        Ok(Some(docker("registry/rust:1", None)))
    );
}

#[test]
fn portable_rejects_different_images_in_the_two_flags() {
    let message = DockerEnvironment::for_portable_runner(
        &portable(None, Some("registry/a:1")),
        &worker(Some("registry/b:2"), Some("/out/wordcount")),
    )
    .unwrap_err()
    .to_string();
    assert!(message.contains("registry/a:1"), "{message}");
    assert!(message.contains("registry/b:2"), "{message}");
}

#[test]
fn portable_explicit_docker_applies_the_container_rules() {
    assert_eq!(
        DockerEnvironment::for_portable_runner(
            &portable(Some("DOCKER"), None),
            &worker(None, Some("/out/wordcount"))
        ),
        Ok(Some(docker(
            &default_sdk_container_image(),
            Some("/out/wordcount")
        )))
    );
    assert_eq!(
        DockerEnvironment::for_portable_runner(
            &portable(Some("docker"), Some("registry/wordcount:1")),
            &worker(None, None)
        ),
        Ok(Some(docker("registry/wordcount:1", None)))
    );
    assert!(
        DockerEnvironment::for_portable_runner(
            &portable(Some("DOCKER"), None),
            &worker(None, None)
        )
        .is_err()
    );
}

#[test]
fn portable_explicit_loopback_ignores_image_flags() {
    for (portable, worker) in [
        (
            portable(Some("LOOPBACK"), Some("registry/rust:1")),
            worker(None, None),
        ),
        (
            portable(Some("loopback"), None),
            worker(Some("registry/rust:1"), Some("/out/wordcount")),
        ),
    ] {
        assert_eq!(
            DockerEnvironment::for_portable_runner(&portable, &worker),
            Ok(None)
        );
    }
}

#[test]
fn portable_external_config_is_not_compared_with_the_image() {
    // For `EXTERNAL`, `--environment_config` is the endpoint of the worker pool, not an image.
    // So it does not conflict with `--sdk_container_image`.
    assert_eq!(
        DockerEnvironment::for_portable_runner(
            &portable(Some("EXTERNAL"), Some("localhost:50000")),
            &worker(Some("registry/rust:1"), None)
        ),
        Ok(None)
    );
}
