/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

//! Unit tests for Reshuffle transform graph construction.

use beam::pipeline::{Pipeline, URN_GROUP_BY_KEY, URN_PAR_DO};
use beam::transforms::{Create, Reshuffle};

#[test]
fn test_reshuffle_graph_expansion() {
    let p = Pipeline::new();
    let words = p.apply(Create::new(
        "Create",
        vec![
            "apple".to_string(),
            "banana".to_string(),
            "cherry".to_string(),
        ],
    ));

    let reshuffled = words.apply(Reshuffle::new("TestReshuffle"));
    assert_eq!(reshuffled.id(), "TestReshuffle/Expand_out");

    let proto = p.to_proto();
    let components = proto.components.expect("components present");

    let (_, composite) = components
        .transforms
        .iter()
        .find(|(_, t)| t.unique_name == "TestReshuffle")
        .expect("TestReshuffle composite transform must exist");

    assert_eq!(composite.subtransforms.len(), 3);

    let t1 = &components.transforms[&composite.subtransforms[0]];
    assert!(t1.unique_name.ends_with("/PairWithRandomKey"));
    assert_eq!(t1.spec.as_ref().unwrap().urn, URN_PAR_DO);

    let t2 = &components.transforms[&composite.subtransforms[1]];
    assert!(t2.unique_name.ends_with("/GroupByKey"));
    assert_eq!(t2.spec.as_ref().unwrap().urn, URN_GROUP_BY_KEY);

    let t3 = &components.transforms[&composite.subtransforms[2]];
    assert!(t3.unique_name.ends_with("/Expand"));
    assert_eq!(t3.spec.as_ref().unwrap().urn, URN_PAR_DO);
}
