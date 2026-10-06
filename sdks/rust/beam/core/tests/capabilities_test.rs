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

//! Tests that advertised runner capabilities match decodable formats in this SDK.

use beam::coders::{
    SUPPORTED_CODER_URNS, URN_PARAM_WINDOWED_VALUE, URN_ROW, URN_STATE_BACKED_ITERABLE,
};
use beam::pipeline::standard_capabilities;

/// Advertise a coder capability only when this SDK can decode that encoding.
///
/// A runner that sees a coder URN in the environment capabilities stops wrapping that
/// encoding in a length prefix and sends it raw. If the harness has no decoder for it,
/// Dataflow aborts the bundle with `malformed VarInt64`.
const CONSEQUENCE: &str = "advertising a coder makes the runner drop its length-prefix \
     fallback and send that encoding raw, so a URN without a dispatch arm in \
     coders::skip_coder_value corrupts the data stream (Dataflow aborts with \
     'malformed VarInt64'). Add the arm and list the URN in SUPPORTED_CODER_URNS, or do \
     not advertise it";

#[test]
fn test_advertised_coders_are_all_decodable() {
    let advertised: Vec<String> = standard_capabilities()
        .into_iter()
        .filter(|urn| urn.starts_with("beam:coder:"))
        .collect();

    assert!(
        !advertised.is_empty(),
        "standard_capabilities() must advertise the coders this SDK implements"
    );

    let unsupported: Vec<&String> = advertised
        .iter()
        .filter(|urn| !SUPPORTED_CODER_URNS.contains(&urn.as_str()))
        .collect();

    assert!(
        unsupported.is_empty(),
        "{unsupported:?} are advertised by standard_capabilities() but absent from \
         SUPPORTED_CODER_URNS: {CONSEQUENCE}."
    );
}

#[test]
fn test_state_backed_iterable_coder_is_advertised() {
    let capabilities = standard_capabilities();

    let urn = URN_STATE_BACKED_ITERABLE;
    assert!(
        capabilities.iter().any(|advertised| advertised == urn),
        "{urn} has a dispatch arm in coders::skip_coder_value and streaming support, and must be advertised"
    );
}

#[test]
fn test_row_coder_is_advertised() {
    let capabilities = standard_capabilities();
    assert!(
        capabilities.iter().any(|advertised| advertised == URN_ROW),
        "{URN_ROW} is supported and must be advertised in standard_capabilities()"
    );
}

#[test]
fn test_param_windowed_value_coder_is_advertised() {
    let capabilities = standard_capabilities();
    assert!(
        capabilities
            .iter()
            .any(|advertised| advertised == URN_PARAM_WINDOWED_VALUE),
        "{URN_PARAM_WINDOWED_VALUE} has a dispatch arm and must be advertised, otherwise the \
         runner keeps length-prefixing an encoding this SDK can read natively"
    );
}

#[test]
fn test_advertised_protocols() {
    use beam::pipeline::{
        PROTOCOL_ELEMENT_METADATA, PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING,
        PROTOCOL_NAMED_DATA_STREAMS,
    };

    let capabilities = standard_capabilities();
    assert!(
        capabilities
            .iter()
            .any(|c| c == PROTOCOL_NAMED_DATA_STREAMS),
        "standard_capabilities() must advertise PROTOCOL_NAMED_DATA_STREAMS"
    );
    assert!(
        capabilities.iter().any(|c| c == PROTOCOL_ELEMENT_METADATA),
        "the harness reads the pane's metadata bit and propagates what it finds onto \
         outputs, so it must tell the runner it is safe to send"
    );
    assert!(
        capabilities
            .iter()
            .any(|c| c == PROTOCOL_MULTI_CORE_BUNDLE_PROCESSING),
        "the worker runs each instruction on its own task, and Dataflow starts one worker for \
         each vCPU when the capability is absent"
    );
}

/// The default image is the one built from the same tree: Gradle tags it with `sdk_version`,
/// which is also what core reports as `BEAM_SDK_VERSION`. A `-SNAPSHOT` crate version, used
/// only outside the repository, maps to the `.dev` tag.
#[test]
fn test_default_image_and_sdk_base_capability_name_this_sdk_version() {
    use beam::pipeline::{
        BEAM_SDK_VERSION, SDK_BASE_VERSION_CAPABILITY_PREFIX, default_sdk_container_image,
        sdk_base_version_capability,
    };

    let expected_tag = BEAM_SDK_VERSION.strip_suffix("-SNAPSHOT").map_or_else(
        || BEAM_SDK_VERSION.to_string(),
        |base| format!("{base}.dev"),
    );
    let image = default_sdk_container_image();
    assert_eq!(image, format!("apache/beam_rust_sdk:{expected_tag}"));

    let capability = sdk_base_version_capability();
    assert_eq!(
        capability,
        format!("{SDK_BASE_VERSION_CAPABILITY_PREFIX}{image}")
    );
    assert!(standard_capabilities().contains(&capability));
}
