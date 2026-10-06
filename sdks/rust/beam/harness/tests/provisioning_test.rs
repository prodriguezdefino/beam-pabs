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

//! Tests for the container boot program's provisioning helpers: deciding which pipeline
//! binary to run, recovering the driver's options, writing the binary, and building its flags.

use std::collections::BTreeMap;
use std::path::{Path, PathBuf};

use beam::options::{HarnessOptions, SDK_OPTIONS_OPTION};
use beam::pipeline::{
    PREBAKED_WORKER_BINARY_PATH, URN_ARTIFACT_ROLE_WORKER_BINARY, URN_ARTIFACT_TYPE_URL,
};
use harness::provisioning::{
    WorkerBinary, artifact_endpoint, resolve_worker_binary, sdk_options_from, select_worker_binary,
    worker_flags, worker_options, write_artifact,
};
use model::fn_execution::ProvisionInfo;
use model::job_management::GetArtifactResponse;
use model::pipeline::{ApiServiceDescriptor, ArtifactInformation};
use prost_types::{Struct, Value, value::Kind};

const URN_KEY: &str = "beam:option:rust_options:v1";

fn artifact(role: &str, type_payload: &[u8]) -> ArtifactInformation {
    ArtifactInformation {
        type_urn: URN_ARTIFACT_TYPE_URL.to_string(),
        type_payload: type_payload.to_vec(),
        role_urn: role.to_string(),
        role_payload: Vec::new(),
    }
}

fn string(s: &str) -> Value {
    Value {
        kind: Some(Kind::StringValue(s.to_string())),
    }
}

fn nested(field: &str, value: Value) -> Value {
    Value {
        kind: Some(Kind::StructValue(Struct {
            fields: BTreeMap::from([(field.to_string(), value)]),
        })),
    }
}

fn info_with(fields: Vec<(&str, Value)>) -> ProvisionInfo {
    ProvisionInfo {
        pipeline_options: Some(Struct {
            fields: fields
                .into_iter()
                .map(|(k, v)| (k.to_string(), v))
                .collect(),
        }),
        ..Default::default()
    }
}

#[test]
fn select_picks_the_first_worker_binary_role_or_a_lone_dependency() {
    let staging = "beam:artifact:role:staging_to:v1";
    let role = URN_ARTIFACT_ROLE_WORKER_BINARY;
    let chosen: [(&str, Vec<ArtifactInformation>, usize); 3] = [
        (
            "the role wins over position",
            vec![
                artifact(staging, b"a"),
                artifact(role, b"bin"),
                artifact(staging, b"c"),
            ],
            1,
        ),
        (
            "the first of several roles",
            vec![artifact(role, b"first"), artifact(role, b"second")],
            0,
        ),
        (
            "a lone dependency with any role",
            vec![artifact(staging, b"only")],
            0,
        ),
    ];
    for (case, deps, index) in &chosen {
        let found = select_worker_binary(deps).unwrap_or_else(|e| panic!("{case}: {e}"));
        assert!(std::ptr::eq(found, &deps[*index]), "{case}");
    }

    let rejected: [(Vec<ArtifactInformation>, &str, &str); 2] = [
        (
            vec![artifact("role:a", b"a"), artifact("role:b", b"b")],
            "No pipeline binary among 2 staged dependencies.",
            "found roles: [role:a, role:b]",
        ),
        (
            vec![],
            "No pipeline binary among 0 staged dependencies.",
            "found roles: []",
        ),
    ];
    for (deps, prefix, roles) in rejected {
        let err = select_worker_binary(&deps).unwrap_err().to_string();
        assert!(err.starts_with(prefix), "{err}");
        assert!(err.contains(role), "{err}");
        assert!(err.contains(roles), "{err}");
    }
}

#[test]
fn sdk_options_are_read_from_the_urn_key_then_the_dataflow_nesting() {
    let cases = [
        ("no pipeline options", ProvisionInfo::default(), None),
        ("empty pipeline options", info_with(vec![]), None),
        (
            "the URN-keyed snapshot",
            info_with(vec![(URN_KEY, string("{\"urn\":{}}"))]),
            Some("{\"urn\":{}}"),
        ),
        (
            "the Dataflow nesting",
            info_with(vec![("options", nested(SDK_OPTIONS_OPTION, string("{}")))]),
            Some("{}"),
        ),
        (
            "both, preferring the URN key",
            info_with(vec![
                (URN_KEY, string("urn")),
                ("options", nested(SDK_OPTIONS_OPTION, string("nested"))),
            ]),
            Some("urn"),
        ),
    ];
    for (case, info, expected) in &cases {
        assert_eq!(sdk_options_from(info), *expected, "{case}");
    }
}

#[test]
fn sdk_options_ignore_values_that_are_not_strings() {
    let number = Value {
        kind: Some(Kind::NumberValue(1.0)),
    };
    assert_eq!(sdk_options_from(&info_with(vec![(URN_KEY, number)])), None);
    // A non-struct `options` field is not a Dataflow nesting.
    assert_eq!(
        sdk_options_from(&info_with(vec![("options", string("{}"))])),
        None
    );
    // Neither the bare option name nor an unrelated key is read.
    assert_eq!(
        sdk_options_from(&info_with(vec![(SDK_OPTIONS_OPTION, string("{}"))])),
        None
    );
    assert_eq!(
        sdk_options_from(&info_with(vec![("other", string("{}"))])),
        None
    );
}

#[test]
fn resolve_prefers_the_prebaked_binary_and_flags_only_an_ignored_worker_binary() {
    let prebaked = Path::new(PREBAKED_WORKER_BINARY_PATH);
    // Only the worker-binary role counts as a binary being ignored.
    for (role, ignored_staged) in [
        (URN_ARTIFACT_ROLE_WORKER_BINARY, true),
        ("beam:artifact:role:staging_to:v1", false),
    ] {
        let deps = [artifact(role, b"staged")];
        assert_eq!(
            resolve_worker_binary(Some(prebaked), &deps).unwrap(),
            WorkerBinary::Prebaked {
                path: prebaked,
                ignored_staged,
            },
            "{role}"
        );
    }

    let deps = [
        artifact("beam:artifact:role:staging_to:v1", b"data"),
        artifact(URN_ARTIFACT_ROLE_WORKER_BINARY, b"staged"),
    ];
    assert_eq!(
        resolve_worker_binary(None, &deps).unwrap(),
        WorkerBinary::Staged(&deps[1])
    );
}

#[test]
fn resolve_uses_the_prebaked_binary_when_nothing_was_staged() {
    let prebaked = Path::new(PREBAKED_WORKER_BINARY_PATH);
    assert_eq!(
        resolve_worker_binary(Some(prebaked), &[]).unwrap(),
        WorkerBinary::Prebaked {
            path: prebaked,
            ignored_staged: false,
        }
    );
}

#[test]
fn resolve_explains_both_remedies_when_there_is_nothing_to_run() {
    let err = resolve_worker_binary(None, &[]).unwrap_err().to_string();
    assert!(err.contains(PREBAKED_WORKER_BINARY_PATH), "{err}");
    assert!(err.contains("--worker_binary"), "{err}");
}

#[test]
fn resolve_keeps_the_role_diagnostic_when_staged_dependencies_name_no_binary() {
    let deps = [artifact("role:a", b"a"), artifact("role:b", b"b")];
    let err = resolve_worker_binary(None, &deps).unwrap_err().to_string();
    assert!(err.contains("found roles: [role:a, role:b]"), "{err}");
}

fn endpoint(url: &str) -> Option<ApiServiceDescriptor> {
    Some(ApiServiceDescriptor {
        url: url.to_string(),
        ..Default::default()
    })
}

/// Provision info with its own status and artifact endpoints.
fn info_with_endpoints() -> ProvisionInfo {
    ProvisionInfo {
        status_endpoint: endpoint("provisioned-status:1"),
        artifact_endpoint: endpoint("provisioned-artifact:2"),
        ..Default::default()
    }
}

#[test]
fn worker_options_carry_the_options_file_and_flag_or_provisioned_endpoints() {
    let args = HarnessOptions {
        id: Some("worker-1".to_string()),
        control_endpoint: Some("control:3".to_string()),
        ..Default::default()
    };
    let options_file = PathBuf::from("/tmp/beam-rust-worker/pipeline_options.json");

    let options = worker_options(args, &info_with_endpoints(), Some(options_file.clone()));

    assert_eq!(
        options.status_endpoint.as_deref(),
        Some("provisioned-status:1")
    );
    assert_eq!(
        options.artifact_endpoint.as_deref(),
        Some("provisioned-artifact:2")
    );
    assert_eq!(options.options_file, Some(options_file));
    // Other options pass through.
    assert_eq!(options.id.as_deref(), Some("worker-1"));
    assert_eq!(options.control_endpoint.as_deref(), Some("control:3"));

    let flags = worker_flags(&options);
    for expected in [
        "--worker=true",
        "--id=worker-1",
        "--control_endpoint=control:3",
        "--status_endpoint=provisioned-status:1",
        "--artifact_endpoint=provisioned-artifact:2",
        "--semi_persist_dir=/tmp",
        "--options_file=/tmp/beam-rust-worker/pipeline_options.json",
    ] {
        assert!(
            flags.iter().any(|f| f == expected),
            "{expected} missing from {flags:?}"
        );
    }
    assert!(
        !flags.iter().any(|f| f.starts_with("--logging_endpoint")),
        "unset flags are omitted: {flags:?}"
    );

    // Flag endpoints override provisioned ones.
    let args = HarnessOptions {
        status_endpoint: Some("flag-status:4".to_string()),
        artifact_endpoint: Some("flag-artifact:5".to_string()),
        ..Default::default()
    };
    let info = info_with_endpoints();
    assert_eq!(
        artifact_endpoint(&args, &info).as_deref(),
        Some("flag-artifact:5")
    );
    let options = worker_options(args, &info, Some(PathBuf::from("options.json")));
    assert_eq!(options.status_endpoint.as_deref(), Some("flag-status:4"));
    assert_eq!(
        options.artifact_endpoint.as_deref(),
        Some("flag-artifact:5")
    );
}

#[test]
fn a_job_without_a_rust_snapshot_starts_the_binary_without_an_options_file() {
    // A driver of another SDK sends no Rust options snapshot.
    assert_eq!(sdk_options_from(&info_with_endpoints()), None);
    let args = HarnessOptions {
        id: Some("worker-1".to_string()),
        ..Default::default()
    };
    let options = worker_options(args, &info_with_endpoints(), None);
    assert_eq!(options.options_file, None);
    let flags = worker_flags(&options);
    assert_eq!(flags[0], "--worker=true");
    assert!(
        !flags.iter().any(|f| f.starts_with("--options_file")),
        "no options file: {flags:?}"
    );
}

/// A path unique to this process and `name`, removed on drop.
struct ScratchFile(PathBuf);

impl ScratchFile {
    fn new(name: &str) -> Self {
        Self(std::env::temp_dir().join(format!(
            "beam-provisioning-test-{}-{name}",
            std::process::id()
        )))
    }
}

impl Drop for ScratchFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}

fn chunks(
    parts: &[&[u8]],
) -> impl tokio_stream::Stream<Item = Result<GetArtifactResponse, tonic::Status>> + Unpin {
    let items: Vec<_> = parts
        .iter()
        .map(|data| {
            Ok(GetArtifactResponse {
                data: data.to_vec(),
            })
        })
        .collect();
    tokio_stream::iter(items)
}

#[tokio::test]
async fn write_artifact_writes_every_chunk_and_propagates_a_stream_error() {
    let file = ScratchFile::new("chunks");

    let written = write_artifact(chunks(&[b"ab", b"", b"cde"]), &file.0)
        .await
        .unwrap();

    assert_eq!(written, 5);
    assert_eq!(std::fs::read(&file.0).unwrap(), b"abcde");

    let file = ScratchFile::new("error");
    let stream = tokio_stream::iter(vec![
        Ok(GetArtifactResponse {
            data: b"partial".to_vec(),
        }),
        Err(tonic::Status::unavailable("artifact store went away")),
    ]);
    let err = write_artifact(stream, &file.0)
        .await
        .unwrap_err()
        .to_string();
    assert!(err.contains("artifact store went away"), "{err}");
}

#[tokio::test]
async fn write_artifact_rejects_an_empty_download() {
    let cases: [(&str, &[&[u8]]); 2] = [("no-chunks", &[]), ("empty-chunks", &[b"", b""])];

    for (name, parts) in cases {
        let file = ScratchFile::new(name);
        let err = write_artifact(chunks(parts), &file.0)
            .await
            .expect_err(name)
            .to_string();
        assert!(err.contains("returned no bytes"), "{name}: {err}");
    }
}
