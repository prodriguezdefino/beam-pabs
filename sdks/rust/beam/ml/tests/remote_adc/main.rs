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

//! Application Default Credentials tests, in their own binary because the credentials load
//! once per process.

use beam_ml::ModelHandler;
use beam_ml::remote::{
    GeminiAdapter, GeminiEndpoint, RemoteAuth, RemoteConfig, RemoteModelHandler,
};

/// Without credentials, loading the client fails, so no request goes without credentials.
#[test]
fn test_missing_application_default_credentials_fail_setup() {
    // SAFETY: this binary has one test, and no other thread reads the environment yet.
    unsafe {
        std::env::set_var(
            "GOOGLE_APPLICATION_CREDENTIALS",
            "/nonexistent/beam-remote-adc-test.json",
        );
    }
    let handler = RemoteModelHandler::new(
        RemoteConfig::default().with_auth(RemoteAuth::ApplicationDefault),
        GeminiAdapter::new(
            "gemini-2.5-flash",
            &GeminiEndpoint::Url("http://localhost:1/x".into()),
        ),
    );
    let err = handler
        .load_model()
        .err()
        .expect("setup fails without credentials")
        .to_string();
    assert!(err.contains("Authentication error"), "{err}");
}
