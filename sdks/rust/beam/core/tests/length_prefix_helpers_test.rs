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

//! Tests for unwrapping runner-inserted `length_prefix` coders.

use std::collections::HashMap;

use beam::coders::{
    URN_LENGTH_PREFIX, URN_VARINT, URN_WINDOWED_VALUE, coder_urn, length_prefix_component,
    peel_length_prefixes,
};
use model::pipeline::{Coder, FunctionSpec};

fn coder(urn: &str, components: &[&str]) -> Coder {
    Coder {
        spec: Some(FunctionSpec {
            urn: urn.to_string(),
            ..Default::default()
        }),
        component_coder_ids: components.iter().map(|c| (*c).to_string()).collect(),
    }
}

/// Coder table with `lp2 -> lp1 -> wv`, plus a length prefix whose component is missing.
fn coders() -> HashMap<String, Coder> {
    HashMap::from([
        ("wv".to_string(), coder(URN_WINDOWED_VALUE, &["v"])),
        ("v".to_string(), coder(URN_VARINT, &[])),
        ("lp1".to_string(), coder(URN_LENGTH_PREFIX, &["wv"])),
        ("lp2".to_string(), coder(URN_LENGTH_PREFIX, &["lp1"])),
        (
            "dangling".to_string(),
            coder(URN_LENGTH_PREFIX, &["missing"]),
        ),
        ("empty".to_string(), coder(URN_LENGTH_PREFIX, &[])),
    ])
}

#[test]
fn length_prefixes_unwrap_one_level_or_all() {
    let coders = coders();
    // (coder id, URN of its length-prefix component, URN after peeling every prefix)
    let cases = [
        ("lp1", Some(URN_WINDOWED_VALUE), URN_WINDOWED_VALUE),
        ("lp2", Some(URN_LENGTH_PREFIX), URN_WINDOWED_VALUE),
        ("wv", None, URN_WINDOWED_VALUE),
        // An unresolvable prefix has no component, so peeling stops at it.
        ("dangling", None, URN_LENGTH_PREFIX),
        ("empty", None, URN_LENGTH_PREFIX),
    ];
    for (id, component, peeled) in cases {
        let coder = &coders[id];
        assert_eq!(
            length_prefix_component(coder, &coders).map(coder_urn),
            component,
            "{id}"
        );
        assert_eq!(
            coder_urn(peel_length_prefixes(coder, &coders)),
            peeled,
            "{id}"
        );
    }
}
