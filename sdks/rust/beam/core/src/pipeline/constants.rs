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

use std::fmt;
use std::str::FromStr;

// Runner primitives and standard transforms.

/// Runner primitive that emits one empty byte array.
pub const URN_IMPULSE: &str = "beam:transform:impulse:v1";

/// Applies a DoFn to each element.
pub const URN_PAR_DO: &str = "beam:transform:pardo:v1";

/// Runner primitive that groups the values of each key and window.
pub const URN_GROUP_BY_KEY: &str = "beam:transform:group_by_key:v1";

/// Merges PCollections of the same type into one.
pub const URN_FLATTEN: &str = "beam:transform:flatten:v1";

/// Assigns elements to windows.
pub const URN_WINDOW_INTO: &str = "beam:transform:window_into:v1";

/// Maps main input windows to side input windows.
pub const URN_MAP_WINDOWS: &str = "beam:transform:map_windows:v1";

/// Renders sampled elements as strings: `KV<nonce, element>` to `KV<nonce, string>`.
pub const URN_TO_STRING: &str = "beam:transform:to_string:v1";

/// Runner source that replays elements, watermark advances and processing-time advances.
pub const URN_TEST_STREAM: &str = "beam:transform:teststream:v1";

/// Composite that breaks fusion and spreads elements across workers. A runner can replace
/// it with its own shuffle; other runners run its subtransforms.
pub const URN_RESHUFFLE: &str = "beam:transform:reshuffle:v1";

/// Composite that writes elements to files.
pub const URN_WRITE_FILES: &str = "beam:transform:write_files:v1";

/// Returns whether `urn` is a runner primitive without an environment.
///
/// `Flatten` is omitted because graph construction assigns it an environment
/// to carry resource hints.
pub fn is_runner_primitive_urn(urn: &str) -> bool {
    matches!(urn, URN_IMPULSE | URN_GROUP_BY_KEY | URN_TEST_STREAM)
}

// Combine.

/// Combines the values of each key. Its subtransforms are a `GroupByKey` and a
/// `combine_grouped_values`. Runners can lift it into the precombine, merge and extract stages
/// below.
pub const URN_COMBINE_PER_KEY: &str = "beam:transform:combine_per_key:v1";

/// Combines all values of a collection. Its payload is that of the `combine_per_key` inside.
pub const URN_COMBINE_GLOBALLY: &str = "beam:transform:combine_globally:v1";

/// Lifted combine stage that combines values before the shuffle.
pub const URN_COMBINE_PER_KEY_PRECOMBINE: &str = "beam:transform:combine_per_key_precombine:v1";

/// Lifted combine stage that merges accumulators after the shuffle.
pub const URN_COMBINE_PER_KEY_MERGE_ACCUMULATORS: &str =
    "beam:transform:combine_per_key_merge_accumulators:v1";

/// Lifted combine stage that turns merged accumulators into outputs.
pub const URN_COMBINE_PER_KEY_EXTRACT_OUTPUTS: &str =
    "beam:transform:combine_per_key_extract_outputs:v1";

/// Lifted combine stage that turns each value into an accumulator, with no combining.
pub const URN_COMBINE_PER_KEY_CONVERT_TO_ACCUMULATORS: &str =
    "beam:transform:combine_per_key_convert_to_accumulators:v1";

/// Combines the grouped values of each key into the output value: the step after the
/// `GroupByKey` of a `combine_per_key` that a runner does not lift.
pub const URN_COMBINE_GROUPED_VALUES: &str = "beam:transform:combine_grouped_values:v1";

/// A `CombinePayload.combine_fn` that holds a handler key. Lifted stages copy the payload, so
/// the harness finds the handler in each stage.
pub const URN_COMBINE_FN_HANDLER_KEY: &str = "beam:combine_fn:rust_handler_key:v1";

/// One stage of a combine.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum CombineStage {
    /// Combines elements into accumulators before the shuffle.
    Precombine,
    /// Merges the accumulators of a key after the shuffle.
    Merge,
    /// Turns a merged accumulator into the output value.
    Extract,
    /// Turns each value into an accumulator, with no combining.
    ConvertToAccumulators,
    /// Combines the grouped values of a key into the output value.
    CombineGroupedValues,
}

/// Error returned on invalid [`CombineStage`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown combine stage '{0}'")]
pub struct UnknownCombineStage(pub String);

impl CombineStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Precombine => "precombine",
            Self::Merge => "merge",
            Self::Extract => "extract",
            Self::ConvertToAccumulators => "convert_to_accumulators",
            Self::CombineGroupedValues => "combine_grouped_values",
        }
    }
}

impl AsRef<str> for CombineStage {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for CombineStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for CombineStage {
    type Err = UnknownCombineStage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "precombine" => Ok(Self::Precombine),
            "merge" => Ok(Self::Merge),
            "extract" => Ok(Self::Extract),
            "convert_to_accumulators" => Ok(Self::ConvertToAccumulators),
            "combine_grouped_values" => Ok(Self::CombineGroupedValues),
            _ => Err(UnknownCombineStage(s.to_string())),
        }
    }
}

impl TryFrom<&str> for CombineStage {
    type Error = UnknownCombineStage;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

/// Builds the handler key for an expanded stage.
pub fn stage_key(handler_key: &str, stage: impl fmt::Display) -> String {
    format!("{handler_key}/{stage}")
}

// Splittable DoFn.

/// Splittable DoFn stage that pairs each element with its initial restriction.
pub const URN_SDF_PAIR_WITH_RESTRICTION: &str = "beam:transform:sdf_pair_with_restriction:v1";

/// Splittable DoFn stage that splits restrictions and computes their sizes.
pub const URN_SDF_SPLIT_AND_SIZE_RESTRICTIONS: &str =
    "beam:transform:sdf_split_and_size_restrictions:v1";

/// Splittable DoFn stage that processes each element and restriction.
pub const URN_SDF_PROCESS_SIZED_ELEMENT_AND_RESTRICTIONS: &str =
    "beam:transform:sdf_process_sized_element_and_restrictions:v1";

/// Truncates restrictions during drain.
pub const URN_SDF_TRUNCATE_SIZED_RESTRICTIONS: &str =
    "beam:transform:sdf_truncate_sized_restrictions:v1";

/// Stage of an expanded Splittable DoFn.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SdfStage {
    /// Pairs each element with its initial restriction.
    Pair,
    /// Splits restrictions and computes their sizes.
    SplitAndSize,
    /// Processes each element and restriction.
    Process,
}

/// Error returned on invalid [`SdfStage`].
#[derive(Clone, Debug, PartialEq, Eq, thiserror::Error)]
#[error("unknown sdf stage '{0}'")]
pub struct UnknownSdfStage(pub String);

impl SdfStage {
    pub const fn as_str(self) -> &'static str {
        match self {
            Self::Pair => "sdf_pair",
            Self::SplitAndSize => "sdf_split_and_size",
            Self::Process => "sdf_process",
        }
    }
}

impl AsRef<str> for SdfStage {
    fn as_ref(&self) -> &str {
        self.as_str()
    }
}

impl fmt::Display for SdfStage {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.write_str(self.as_str())
    }
}

impl FromStr for SdfStage {
    type Err = UnknownSdfStage;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        match s {
            "sdf_pair" => Ok(Self::Pair),
            "sdf_split_and_size" => Ok(Self::SplitAndSize),
            "sdf_process" => Ok(Self::Process),
            _ => Err(UnknownSdfStage(s.to_string())),
        }
    }
}

impl TryFrom<&str> for SdfStage {
    type Error = UnknownSdfStage;

    fn try_from(s: &str) -> Result<Self, Self::Error> {
        s.parse()
    }
}

// ParDo requirements.

/// The pipeline has a stateful DoFn.
pub const URN_REQUIREMENT_STATEFUL: &str = "beam:requirement:pardo:stateful:v1";

/// The pipeline has a Splittable DoFn.
pub const URN_REQUIREMENT_SPLITTABLE_DOFN: &str = "beam:requirement:pardo:splittable_dofn:v1";

/// The pipeline has a DoFn that asks for bundle finalization.
pub const URN_REQUIREMENT_BUNDLE_FINALIZATION: &str = "beam:requirement:pardo:finalization:v1";

/// The pipeline has a DoFn that must see the same input when the runner retries a bundle.
pub const URN_REQUIREMENT_STABLE_INPUT: &str = "beam:requirement:pardo:stable_input:v1";

/// The pipeline has a stateful DoFn that must see the elements of a key in timestamp order.
pub const URN_REQUIREMENT_TIME_SORTED_INPUT: &str = "beam:requirement:pardo:time_sorted_input:v1";

// User state.

/// Bag user state. Value state uses it too.
pub const URN_USER_STATE_BAG: &str = "beam:user_state:bag:v1";

/// Multimap user state. Map and set state use it.
pub const URN_USER_STATE_MULTIMAP: &str = "beam:user_state:multimap:v1";

/// Ordered list user state: values sorted by a timestamp.
pub const URN_USER_STATE_ORDERED_LIST: &str = "beam:user_state:ordered_list:v1";

// Window functions.

/// One window that holds all elements.
pub const URN_WINDOW_FN_GLOBAL_WINDOWS: &str = "beam:window_fn:global_windows:v1";

/// Windows of one size that do not overlap.
pub const URN_WINDOW_FN_FIXED_WINDOWS: &str = "beam:window_fn:fixed_windows:v1";

/// Windows of one size that start at a fixed period and can overlap.
pub const URN_WINDOW_FN_SLIDING_WINDOWS: &str = "beam:window_fn:sliding_windows:v1";

/// Windows per key that close after a gap without elements.
pub const URN_WINDOW_FN_SESSION_WINDOWS: &str = "beam:window_fn:session_windows:v1";

// Environments and resource hints.

/// The default environment of the runner.
pub const URN_ENV_DEFAULT: &str = "beam:env:default:v1";

/// An environment that runs the worker in a container image.
pub const URN_ENV_DOCKER: &str = "beam:env:docker:v1";

/// An environment that runs the worker as a local process.
pub const URN_ENV_PROCESS: &str = "beam:env:process:v1";

/// An environment whose worker runs outside the runner, at an endpoint.
pub const URN_ENV_EXTERNAL: &str = "beam:env:external:v1";

/// The accelerator that a transform needs, such as a GPU type.
pub const URN_RESOURCE_ACCELERATOR: &str = "beam:resources:accelerator:v1";

/// Payload is the decimal byte count.
pub const URN_RESOURCE_MIN_RAM_BYTES: &str = "beam:resources:min_ram_bytes:v1";

/// The minimum number of CPUs that a transform needs.
pub const URN_RESOURCE_CPU_COUNT: &str = "beam:resources:cpu_count:v1";

/// The maximum number of bundles that one worker runs at the same time.
pub const URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER: &str =
    "beam:resources:max_active_bundles_per_worker:v1";

// Artifacts.

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

// Pipeline option namespaces.
// Keys with the format `beam:option:<group>:v1` namespace structured option groups
// in the pipeline options struct.

/// Options of the pipeline itself.
pub const OPTION_NAMESPACE_CORE: &str = "beam:option:core:v1";

/// Options that select and configure the runner.
pub const OPTION_NAMESPACE_RUNNER: &str = "beam:option:runner:v1";

/// Options of portable runners and job services.
pub const OPTION_NAMESPACE_PORTABLE: &str = "beam:option:portable:v1";

/// Options of the SDK worker.
pub const OPTION_NAMESPACE_WORKER: &str = "beam:option:worker:v1";

/// Debug options, such as experiments.
pub const OPTION_NAMESPACE_DEBUG: &str = "beam:option:debug:v1";

/// Google Cloud options.
pub const OPTION_NAMESPACE_GCP: &str = "beam:option:gcp:v1";

/// Dataflow options.
pub const OPTION_NAMESPACE_DATAFLOW: &str = "beam:option:dataflow:v1";

/// Resource hint options.
pub const OPTION_NAMESPACE_RESOURCE: &str = "beam:option:resource:v1";

/// Options that the pipeline author defines.
pub const OPTION_NAMESPACE_USER: &str = "beam:option:user:v1";

// SDK version and container image.

/// The Beam SDK version of this build, in Gradle's `sdk_version` form
/// (`<major>.<minor>.<patch>.dev` for development builds). `build.rs` takes it from
/// `BEAM_SDK_VERSION`, else from the repository's `gradle.properties`. Outside the
/// repository, it is the crate version.
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

/// The repository of the SDK container image.
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

// Rust SDK markers.

/// The `DoFn` payload of this SDK: a handler key that the worker resolves.
pub const URN_RUST_DOFN: &str = "beam:dofn:rust:v1";

/// The `do_fn` of `Create`. The payload holds the encoded elements, so the worker needs no
/// rebuilt pipeline.
pub const URN_RUST_DOFN_CREATE: &str = "beam:dofn:rust:create:v1";

/// A registered `DoFn`. The payload holds the id of the type and its serialized fields.
pub const URN_RUST_DOFN_REGISTERED: &str = "beam:dofn:rust:registered:v1";

/// A registered `CombineFn`, as the `combine_fn` of a `CombinePerKey`. The payload holds the
/// id of the type, its serialized fields and the key coder. The combine stage that runs it
/// selects the function.
pub const URN_RUST_COMBINE_FN_REGISTERED: &str = "beam:combine_fn:rust:registered:v1";

/// Stands in for the coder and windowing strategy of a cross-language output that has
/// not been expanded yet.
pub const UNEXPANDED_PLACEHOLDER_ID: &str = "__unexpanded_xlang_placeholder__";

// Cross-language and expansion payload URNs.

/// Standard expansion payload URN for schema transforms.
pub const URN_EXPANSION_SCHEMA_TRANSFORM: &str = "beam:expansion:payload:schematransform:v1";

/// Standard expansion payload URN for Java class lookup transforms.
pub const URN_EXPANSION_JAVA_CLASS_LOOKUP: &str = "beam:expansion:payload:java_class_lookup:v1";

// Fn API protocols and capabilities.

/// Named data streams over BeamFnData.
pub const PROTOCOL_NAMED_DATA_STREAMS: &str = "beam:protocol:named_data_streams:v1";

/// The SDK reads, writes, and propagates element metadata.
pub const PROTOCOL_ELEMENT_METADATA: &str = "beam:protocol:element_metadata:v1";

/// The SDK sets `consuming_received_data` in progress responses.
pub const PROTOCOL_SDK_CONSUMING_RECEIVED_DATA: &str =
    "beam:protocol:sdk_consuming_received_data:v1";

/// The SDK processes bundles in parallel in one worker.
/// Without it, Dataflow starts one container for each vCPU.
pub const PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING: &str =
    "beam:protocol:multi_core_bundle_processing:v1";

/// The SDK answers WorkerStatusRequest on the BeamFnWorkerStatus service.
pub const PROTOCOL_WORKER_STATUS: &str = "beam:protocol:worker_status:v1";

/// The SDK reads and writes multimap user state.
pub const PROTOCOL_MULTIMAP_STATE: &str = "beam:protocol:multimap_state:v1";

/// The SDK reads and writes ordered list user state.
pub const PROTOCOL_ORDERED_LIST_STATE: &str = "beam:protocol:ordered_list_state:v1";

/// The SDK keeps user state and side inputs across bundles while the runner sends the same
/// cache token.
pub const PROTOCOL_STATE_CACHING: &str = "beam:protocol:state_caching:v1";

/// The SDK keeps samples of elements and of failed elements, and answers SampleDataRequest.
pub const PROTOCOL_DATA_SAMPLING: &str = "beam:protocol:data_sampling:v1";

/// The SDK reads bundle input from `ProcessBundleRequest.elements`.
pub const PROTOCOL_CONTROL_REQUEST_ELEMENTS_EMBEDDING: &str =
    "beam:protocol:control_request_elements_embedding:v1";

/// A runner capability: the runner reads bundle output from `ProcessBundleResponse.elements`.
/// The SDK puts output there only when the runner declares it.
pub const PROTOCOL_CONTROL_RESPONSE_ELEMENTS_EMBEDDING: &str =
    "beam:protocol:control_response_elements_embedding:v1";

/// The SDK reports bundle execution progress on the data stream.
pub const PROTOCOL_PROGRESS_REPORTING: &str = "beam:protocol:progress_reporting:v1";

/// The SDK reports `MonitoringInfo` entries in harness metrics.
pub const PROTOCOL_HARNESS_MONITORING_INFOS: &str = "beam:protocol:harness_monitoring_infos:v1";

/// Returns the Fn API protocols, SDK-executed transforms and coder URNs that the harness
/// implements. Each entry is a promise to the runner: add one only after the harness handles
/// it. Coder URNs come from [`crate::coders::SUPPORTED_CODER_URNS`].
pub fn standard_capabilities() -> Vec<String> {
    const PROTOCOLS: &[&str] = &[
        PROTOCOL_PROGRESS_REPORTING,
        PROTOCOL_HARNESS_MONITORING_INFOS,
        PROTOCOL_NAMED_DATA_STREAMS,
        PROTOCOL_ELEMENT_METADATA,
        PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING,
        PROTOCOL_SDK_CONSUMING_RECEIVED_DATA,
        PROTOCOL_WORKER_STATUS,
        PROTOCOL_MULTIMAP_STATE,
        PROTOCOL_STATE_CACHING,
        PROTOCOL_ORDERED_LIST_STATE,
        PROTOCOL_DATA_SAMPLING,
        PROTOCOL_CONTROL_REQUEST_ELEMENTS_EMBEDDING,
        crate::metrics::PROTOCOL_MONITORING_INFO_SHORT_IDS,
    ];
    const TRANSFORMS: &[&str] = &[URN_TO_STRING, URN_SDF_TRUNCATE_SIZED_RESTRICTIONS];

    PROTOCOLS
        .iter()
        .chain(TRANSFORMS)
        .chain(crate::coders::SUPPORTED_CODER_URNS)
        .map(|urn| (*urn).to_string())
        .chain(std::iter::once(sdk_base_version_capability()))
        .collect()
}
