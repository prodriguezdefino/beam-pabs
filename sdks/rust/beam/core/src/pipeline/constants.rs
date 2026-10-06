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

//! Standard URN constants, artifact specifications, and runner protocols.

pub const URN_IMPULSE: &str = "beam:transform:impulse:v1";
pub const URN_PAR_DO: &str = "beam:transform:pardo:v1";
pub const URN_GROUP_BY_KEY: &str = "beam:transform:group_by_key:v1";
pub const URN_COMBINE_PER_KEY: &str = "beam:transform:combine_per_key:v1";
pub const URN_COMBINE_PER_KEY_PRECOMBINE: &str = "beam:transform:combine_per_key_precombine:v1";
pub const URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS: &str =
    "beam:transform:combine_per_key_merge_accumulators:v1";
pub const URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS: &str =
    "beam:transform:combine_per_key_extract_outputs:v1";

/// Maps main input windows to side input windows.
pub const URN_MAP_WINDOWS: &str = "beam:transform:map_windows:v1";

/// Renders elements as strings so that runners can show sampled elements (Dataflow data
/// sampling). Input `KV<nonce, element>`, output `KV<nonce, string>`. A runner routes it only to
/// an environment that advertises this URN as a capability, else it warns once per PCollection.
pub const URN_TO_STRING: &str = "beam:transform:to_string:v1";

pub const URN_REQUIREMENT_STATEFUL: &str = "beam:requirement:pardo:stateful:v1";

pub const URN_REQUIREMENT_SPLITTABLE_DOFN: &str = "beam:requirement:pardo:splittable_dofn:v1";

pub const URN_REQUIREMENT_BUNDLE_FINALIZATION: &str = "beam:requirement:pardo:finalization:v1";

pub const URN_RUST_DOFN: &str = "beam:dofn:rust:v1";

pub const URN_SDF_PAIR_WITH_RESTRICTION: &str = "beam:transform:sdf_pair_with_restriction:v1";

pub const URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS: &str =
    "beam:transform:sdf_split_and_size_restrictions:v1";

pub const URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS: &str =
    "beam:transform:sdf_process_sized_element_and_restrictions:v1";

/// Truncates restrictions during drain.
pub const URN_SDF_TRUNCATE_SIZED_RESTRICTIONS: &str =
    "beam:transform:sdf_truncate_sized_restrictions:v1";

pub const SDF_STAGE_PAIR: &str = "sdf_pair";

pub const SDF_STAGE_SPLIT_AND_SIZE: &str = "sdf_split_and_size";

/// Stands in for the coder and windowing strategy of a cross-language output that has
/// not been expanded yet.
pub const UNEXPANDED_PLACEHOLDER_ID: &str = "__unexpanded_xlang_placeholder__";

pub const SDF_STAGE_PROCESS: &str = "sdf_process";

/// Builds the handler key for an expanded stage of a Splittable DoFn.
pub fn sdf_stage_key(handler_key: &str, stage: &str) -> String {
    format!("{handler_key}/{stage}")
}

pub const URN_USER_STATE_BAG: &str = "beam:user_state:bag:v1";

pub const URN_USER_STATE_MULTIMAP: &str = "beam:user_state:multimap:v1";

/// Marks a `CombinePayload.combine_fn` that carries this SDK's handler key, not a serialized
/// combine function. A runner that lifts a combiner copies the `CombinePayload` unchanged into
/// the precombine, merge_accumulators and extract_outputs stages, so the harness finds the
/// handler without the transform ids or names that the runner synthesizes.
pub const URN_COMBINE_FN_HANDLER_KEY: &str = "beam:combine_fn:rust_handler_key:v1";

/// Stage that combines elements before the shuffle.
pub const COMBINE_STAGE_PRECOMBINE: &str = "precombine";

/// Stage that merges accumulators after the shuffle.
pub const COMBINE_STAGE_MERGE: &str = "merge";

/// Stage that turns a merged accumulator into the output value.
pub const COMBINE_STAGE_EXTRACT: &str = "extract";

/// Builds the handler key for one stage of a lifted combine. Both the registering transform
/// and the harness call it, so the keys agree.
pub fn combine_stage_key(handler_key: &str, stage: &str) -> String {
    format!("{handler_key}/{stage}")
}

pub const URN_ENV_DEFAULT: &str = "beam:env:default:v1";
pub const URN_ENV_DOCKER: &str = "beam:env:docker:v1";
pub const URN_ENV_PROCESS: &str = "beam:env:process:v1";
pub const URN_ENV_EXTERNAL: &str = "beam:env:external:v1";
pub const URN_FLATTEN: &str = "beam:transform:flatten:v1";
pub const URN_WINDOW_INTO: &str = "beam:transform:window_into:v1";
pub const URN_WINDOW_FN_GLOBAL_WINDOWS: &str = "beam:window_fn:global_windows:v1";
pub const URN_WINDOW_FN_FIXED_WINDOWS: &str = "beam:window_fn:fixed_windows:v1";
pub const URN_WINDOW_FN_SLIDING_WINDOWS: &str = "beam:window_fn:sliding_windows:v1";
pub const URN_WINDOW_FN_SESSION_WINDOWS: &str = "beam:window_fn:session_windows:v1";

/// Runner-executed source replaying a scripted sequence of elements, watermark advances
/// and processing-time advances.
pub const URN_TEST_STREAM: &str = "beam:transform:teststream:v1";

/// Standard pipeline option namespaces.
pub const OPTION_NAMESPACE_CORE: &str = "beam:option:core:v1";
pub const OPTION_NAMESPACE_RUNNER: &str = "beam:option:runner:v1";
pub const OPTION_NAMESPACE_PORTABLE: &str = "beam:option:portable:v1";
pub const OPTION_NAMESPACE_WORKER: &str = "beam:option:worker:v1";
pub const OPTION_NAMESPACE_DEBUG: &str = "beam:option:debug:v1";
pub const OPTION_NAMESPACE_GCP: &str = "beam:option:gcp:v1";
pub const OPTION_NAMESPACE_DATAFLOW: &str = "beam:option:dataflow:v1";
pub const OPTION_NAMESPACE_RESOURCE: &str = "beam:option:resource:v1";
pub const OPTION_NAMESPACE_USER: &str = "beam:option:user:v1";

pub const URN_RESOURCE_ACCELERATOR: &str = "beam:resources:accelerator:v1";

/// Payload is the decimal byte count.
pub const URN_RESOURCE_MIN_RAM_BYTES: &str = "beam:resources:min_ram_bytes:v1";

pub const URN_RESOURCE_CPU_COUNT: &str = "beam:resources:cpu_count:v1";

pub const URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER: &str =
    "beam:resources:max_active_bundles_per_worker:v1";

/// Returns whether `urn` is a runner-implemented primitive (Impulse, GroupByKey, TestStream)
/// that must omit environment_id, as `beam_runner_api.proto` requires. Flatten has an
/// environment so that portable runners, such as Dataflow Runner v2, can fuse it.
pub fn is_runner_primitive_urn(urn: &str) -> bool {
    matches!(urn, URN_IMPULSE | URN_GROUP_BY_KEY | URN_TEST_STREAM)
}

/// The Beam SDK version of this build, in Gradle's `sdk_version` form (`<major>.<minor>.<patch>.dev`
/// for development builds). `build.rs` takes it from the `BEAM_SDK_VERSION` variable that Gradle
/// sets, else from the repository's `gradle.properties`; a crate built outside the repository
/// falls back to its own version.
pub const BEAM_SDK_VERSION: &str = match option_env!("BEAM_SDK_VERSION") {
    Some(version) => version,
    None => env!("CARGO_PKG_VERSION"),
};

/// The Beam release this build belongs to, without any pre-release suffix such as `-SNAPSHOT`.
/// Resolved like [`BEAM_SDK_VERSION`]: `BEAM_RELEASE_VERSION` from Gradle, else the repository's
/// `gradle.properties`, else the crate version without its pre-release suffix.
pub const BEAM_RELEASE_VERSION: &str = match option_env!("BEAM_RELEASE_VERSION") {
    Some(version) => version,
    None => concat!(
        env!("CARGO_PKG_VERSION_MAJOR"),
        ".",
        env!("CARGO_PKG_VERSION_MINOR"),
        ".",
        env!("CARGO_PKG_VERSION_PATCH")
    ),
};

pub const SDK_CONTAINER_IMAGE_REPOSITORY: &str = "apache/beam_rust_sdk";

/// The container image of this SDK release, `apache/beam_rust_sdk:<version>`, used by the
/// default environment and in the `beam:version:sdk_base:` capability. A `-SNAPSHOT` version
/// maps to the `.dev` tag that Gradle gives the image, so a development build uses the image
/// built from the same tree.
pub fn default_sdk_container_image() -> String {
    let tag = BEAM_SDK_VERSION.strip_suffix("-SNAPSHOT").map_or_else(
        || BEAM_SDK_VERSION.to_string(),
        |base| format!("{base}.dev"),
    );
    format!("{SDK_CONTAINER_IMAGE_REPOSITORY}:{tag}")
}

/// Path of a pre-baked pipeline binary in a custom image. The container `boot` program runs
/// it, and uses a staged binary only when this path has no binary.
pub const PREBAKED_WORKER_BINARY_PATH: &str = "/opt/apache/beam/worker_binary";

/// `StandardArtifacts.Types.URL`: a dependency the worker fetches from a URL.
pub const URN_ARTIFACT_TYPE_URL: &str = "beam:artifact:type:url:v1";

/// `StandardArtifacts.Types.FILE`: a path on the submitting machine, valid only for the
/// submitting process. Runners that pull bytes through `ReverseArtifactRetrievalService` need it.
pub const URN_ARTIFACT_TYPE_FILE: &str = "beam:artifact:type:file:v1";

/// `StandardArtifacts.Types.DEFERRED`: a token that only the issuing expansion service can
/// redeem. Before submission, each runner must replace it with a form its workers can fetch.
pub const URN_ARTIFACT_TYPE_DEFERRED: &str = "beam:artifact:type:deferred:v1";

/// `StandardArtifacts.Roles.STAGING_TO`: the file name to stage a dependency under.
pub const URN_ARTIFACT_ROLE_STAGING_TO: &str = "beam:artifact:role:staging_to:v1";

/// Marks the staged pipeline binary that a worker container runs. Runners treat it as opaque;
/// only the Rust container boot program reads it.
pub const URN_ARTIFACT_ROLE_WORKER_BINARY: &str = "beam:artifact:role:rust_worker_binary:v1";

/// Prefix of the base-version capability that tells Rust SDK environments apart from
/// environments that cross-language expansion adds.
pub const SDK_BASE_VERSION_CAPABILITY_PREFIX: &str = "beam:version:sdk_base:rust:";

/// Returns `beam:version:sdk_base:rust:<image>`, which runners use to identify the SDK release.
/// The form `beam:version:sdk_base:<sdk>:<image>` is common to all SDKs.
pub fn sdk_base_version_capability() -> String {
    format!(
        "{SDK_BASE_VERSION_CAPABILITY_PREFIX}{}",
        default_sdk_container_image()
    )
}

/// Named data streams over BeamFnData.
pub const PROTOCOL_NAMED_DATA_STREAMS: &str = "beam:protocol:named_data_streams:v1";

/// Reading, writing and propagating element metadata.
pub const PROTOCOL_ELEMENT_METADATA: &str = "beam:protocol:element_metadata:v1";

/// Processing bundles in parallel in one worker. Without it, Dataflow starts one container for
/// each vCPU when a driver of another SDK submits the job.
pub const PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING: &str =
    "beam:protocol:multi_core_bundle_processing:v1";

/// Returns the Fn API protocols, SDK-executed transforms and coder URNs that the harness
/// implements. Each entry is a promise to the runner: add one only after the harness handles
/// it. Coder URNs come from [`crate::coders::SUPPORTED_CODER_URNS`].
pub fn standard_capabilities() -> Vec<String> {
    const PROTOCOLS: &[&str] = &[
        "beam:protocol:progress_reporting:v1",
        "beam:protocol:harness_monitoring_infos:v1",
        PROTOCOL_NAMED_DATA_STREAMS,
        PROTOCOL_ELEMENT_METADATA,
        PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING,
        crate::metrics::PROTOCOL_MONITORING_INFO_SHORT_IDS,
    ];
    const TRANSFORMS: &[&str] = &[URN_TO_STRING];

    PROTOCOLS
        .iter()
        .chain(TRANSFORMS)
        .chain(crate::coders::SUPPORTED_CODER_URNS)
        .map(|urn| (*urn).to_string())
        .chain(std::iter::once(sdk_base_version_capability()))
        .collect()
}
