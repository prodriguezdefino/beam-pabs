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

//! Google authentication: the token format, the client properties and the context. No test
//! gets a Google token.

use std::time::Duration;

use base64::Engine;
use base64::engine::general_purpose::URL_SAFE_NO_PAD;
use kafka_io::native::auth::managed_kafka_token;
use kafka_io::native::client::{Auth, Connection};
use kafka_io::native::context::KafkaContext;
use kafka_io::native::read::ReadOptions;
use kafka_io::native::write::WriteOptions;
use rdkafka::client::ClientContext;

fn decode(part: &str) -> String {
    let bytes = URL_SAFE_NO_PAD
        .decode(part)
        .expect("each part of a token is base64url");
    String::from_utf8(bytes).expect("each part of a test token is UTF-8")
}

#[test]
fn the_token_has_the_header_claims_and_access_token_of_managed_kafka() {
    let token = managed_kafka_token(
        "reader@project.iam.gserviceaccount.com",
        Duration::from_secs(1_000),
        Duration::from_secs(1_180),
        "ya29.access",
    );
    let parts: Vec<&str> = token.split('.').collect();
    assert_eq!(parts.len(), 3);
    let header: serde_json::Value = serde_json::from_str(&decode(parts[0])).unwrap();
    assert_eq!(
        header,
        serde_json::json!({"typ": "JWT", "alg": "GOOG_OAUTH2_TOKEN"})
    );
    let claims: serde_json::Value = serde_json::from_str(&decode(parts[1])).unwrap();
    assert_eq!(
        claims,
        serde_json::json!({
            "exp": 1_180,
            "iat": 1_000,
            "iss": "Google",
            "scope": "kafka",
            "sub": "reader@project.iam.gserviceaccount.com",
        })
    );
    assert_eq!(decode(parts[2]), "ya29.access");
}

#[test]
fn google_auth_sets_sasl_ssl_and_oauthbearer_unless_the_properties_do() {
    let google = Connection {
        auth: Auth::GoogleAdc,
        ..Connection::new("broker:9092")
    };
    let config = google.producer_config();
    assert_eq!(config.get("security.protocol"), Some("SASL_SSL"));
    assert_eq!(config.get("sasl.mechanism"), Some("OAUTHBEARER"));

    let mut overridden = google;
    overridden
        .properties
        .insert("security.protocol".into(), "SASL_PLAINTEXT".into());
    assert_eq!(
        overridden.consumer_config().get("security.protocol"),
        Some("SASL_PLAINTEXT")
    );

    let plain = Connection::new("broker:9092").consumer_config();
    assert_eq!(plain.get("security.protocol"), None);
    assert_eq!(plain.get("sasl.mechanism"), None);
}

#[test]
fn google_auth_rejects_another_sasl_mechanism() {
    let mut connection = Connection {
        auth: Auth::GoogleAdc,
        ..Connection::new("broker:9092")
    };
    assert!(connection.validate().is_ok());
    connection
        .properties
        .insert("sasl.mechanism".into(), "oauthbearer".into());
    assert!(connection.validate().is_ok(), "the case does not matter");
    connection
        .properties
        .insert("sasl.mechanism".into(), "PLAIN".into());
    let error = connection.validate().unwrap_err().to_string();
    assert!(
        error.contains("OAUTHBEARER") && error.contains("PLAIN"),
        "{error}"
    );
}

#[test]
fn the_options_turn_google_auth_on() {
    let write = WriteOptions::new("broker:9092", "events").with_google_auth();
    let read = ReadOptions::new("broker:9092", "events").with_google_auth();
    assert_eq!(write.connection.auth, Auth::GoogleAdc);
    assert_eq!(read.connection.auth, Auth::GoogleAdc);
    assert_eq!(Connection::new("broker:9092").auth, Auth::Properties);
}

#[test]
fn a_context_without_google_auth_gives_no_token() {
    let context = KafkaContext::new(Auth::Properties);
    assert!(!context.is_google());
    assert!(KafkaContext::new(Auth::GoogleAdc).is_google());
    assert!(context.generate_oauth_token(None).is_err());
    assert!(!context.has_issued_token());
}
