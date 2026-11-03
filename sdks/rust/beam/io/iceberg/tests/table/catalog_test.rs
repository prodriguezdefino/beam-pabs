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

//! The props of the `iceberg` REST client for a `Catalog`.

use beam_io_iceberg::Catalog;
use std::collections::HashMap;

use beam_io_iceberg::catalog::{ACCESS_DELEGATION, StorageCredential, credential_for, rest_props};

#[test]
fn the_target_job_catalog_maps_to_rest_props() {
    let catalog = Catalog::rest("https://biglake.googleapis.com/iceberg/v1/restcatalog")
        .with_warehouse("bl://projects/p/catalogs/c")
        .with_header("x-goog-user-project", "p")
        .with_property("gcp_location", "us-central1")
        .with_vended_credentials();
    let props = rest_props(&catalog);
    let get = |key: &str| props.get(key).map(String::as_str);
    assert_eq!(
        get("uri"),
        Some("https://biglake.googleapis.com/iceberg/v1/restcatalog")
    );
    assert_eq!(get("warehouse"), Some("bl://projects/p/catalogs/c"));
    assert_eq!(get("header.x-goog-user-project"), Some("p"));
    assert_eq!(
        get(&format!("header.{ACCESS_DELEGATION}")),
        Some("vended-credentials")
    );
    assert_eq!(get("gcp_location"), Some("us-central1"));
    assert_eq!(get("token"), None, "the token is added to each request");
    assert_eq!(props.len(), 5);
}

#[test]
fn a_catalog_without_options_has_only_its_uri() {
    let props = rest_props(&Catalog::rest("http://localhost:8181"));
    assert_eq!(props.len(), 1);
    assert_eq!(
        props.get("uri").map(String::as_str),
        Some("http://localhost:8181")
    );
}

#[test]
fn the_debug_form_of_a_catalog_hides_the_token() {
    let debug = format!("{:?}", Catalog::rest("http://x").with_token("secret-token"));
    assert!(!debug.contains("secret-token"), "{debug}");
}

fn credential(prefix: &str, token: &str) -> StorageCredential {
    StorageCredential {
        prefix: prefix.to_string(),
        config: HashMap::from([("gcs.oauth2.token".to_string(), token.to_string())]),
    }
}

#[test]
fn the_longest_prefix_that_holds_the_location_gives_the_credential() {
    let credentials = || {
        vec![
            credential("gs://bucket/", "bucket"),
            credential("gs://bucket/ns/table/", "table"),
            credential("gs://bucket/ns/table-other/", "other"),
        ]
    };
    let token = |location: &str| {
        credential_for(credentials(), location)
            .and_then(|config| config.get("gcs.oauth2.token").cloned())
    };
    assert_eq!(token("gs://bucket/ns/table").as_deref(), Some("table"));
    assert_eq!(token("gs://bucket/ns/table/").as_deref(), Some("table"));
    assert_eq!(token("gs://bucket/ns/tab").as_deref(), Some("bucket"));
    assert_eq!(token("gs://elsewhere/ns/table"), None);
}
