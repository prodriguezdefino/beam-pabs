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

use beam::options::{OptionsError, PipelineOptions};
use gcp::GcpOptions;

// Unwraps an `OptionsError::Validation` into `(group, message)`.
fn validation(err: OptionsError) -> (&'static str, String) {
    match err {
        OptionsError::Validation { group, message } => (group, message),
        other => panic!("expected Validation error, got: {other:?}"),
    }
}

const NO_PROJECT: &str = "Google Cloud project (--project) is required for Dataflow";
const NO_REGION: &str = "Google Cloud region (--region) is required for Dataflow";
const NO_LOCATION: &str = "A GCS staging or temp location (--staging_location or --temp_location) is required for Dataflow";

// Without GCP flags, parsing must not invent values from clap defaults or environment
// fallbacks. The group equals `Default`, validates, and fails the Dataflow check on
// `--project`.
#[test]
fn test_gcp_options_without_flags_are_empty() {
    let gcp: GcpOptions = PipelineOptions::parse_from(["app"])
        .view_as()
        .expect("empty GcpOptions validate");
    assert_eq!(gcp, GcpOptions::default());
    assert_eq!(
        validation(gcp.require_dataflow_fields().unwrap_err()),
        ("GcpOptions", NO_PROJECT.to_string())
    );
}

#[test]
fn test_dynamic_downcast_from_pipeline_options() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=dataflow",
        "--project=my-gcp-project",
        "--region=us-central1",
        "--zone=us-central1-a",
        "--temp_location=gs://my-bucket/temp",
        "--staging_location=gs://my-bucket/staging",
        "--service_account_email=worker@my-project.iam.gserviceaccount.com",
    ]);

    let gcp: GcpOptions = opts
        .view_as()
        .expect("GcpOptions should downcast cleanly from generic PipelineOptions");

    assert_eq!(gcp.project.as_deref(), Some("my-gcp-project"));
    assert_eq!(gcp.region.as_deref(), Some("us-central1"));
    assert_eq!(gcp.zone.as_deref(), Some("us-central1-a"));
    assert_eq!(gcp.temp_location.as_deref(), Some("gs://my-bucket/temp"));
    assert_eq!(
        gcp.staging_location.as_deref(),
        Some("gs://my-bucket/staging")
    );
    assert_eq!(
        gcp.service_account_email.as_deref(),
        Some("worker@my-project.iam.gserviceaccount.com")
    );

    let gcp_view: GcpOptions = opts.view_as().unwrap();
    assert_eq!(gcp_view, gcp);
}

#[test]
fn test_parse_aliases() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--tempLocation=gs://alias-bucket/temp",
        "--stagingLocation=gs://alias-bucket/staging",
        "--service_account=sa@alias.com",
    ]);

    let gcp: GcpOptions = opts.view_as().unwrap();
    assert_eq!(gcp.temp_location.as_deref(), Some("gs://alias-bucket/temp"));
    assert_eq!(
        gcp.staging_location.as_deref(),
        Some("gs://alias-bucket/staging")
    );
    assert_eq!(gcp.service_account_email.as_deref(), Some("sa@alias.com"));
}

#[test]
fn test_validation_fails_on_non_gcs_uris() {
    let err = PipelineOptions::parse_from(["app", "--temp_location=s3://wrong-cloud/temp"])
        .view_as::<GcpOptions>()
        .expect_err("non-gs URI for temp_location must fail validation");
    assert_eq!(
        validation(err),
        (
            "GcpOptions",
            "temp_location must be a valid gs:// URI, found: 's3://wrong-cloud/temp'".to_string()
        )
    );

    let err = PipelineOptions::parse_from(["app", "--staging_location=/local/fs/staging"])
        .view_as::<GcpOptions>()
        .expect_err("non-gs URI for staging_location must fail validation");
    assert_eq!(
        validation(err),
        (
            "GcpOptions",
            "staging_location must be a valid gs:// URI, found: '/local/fs/staging'".to_string()
        )
    );
}

#[test]
fn test_dataflow_requirements_validation() {
    let check = |gcp: GcpOptions| gcp.require_dataflow_fields().map_err(validation);
    let project = || Some("my-proj".to_string());
    let region = || Some("us-central1".to_string());

    assert_eq!(
        check(GcpOptions {
            region: region(),
            staging_location: Some("gs://b/stg".to_string()),
            ..Default::default()
        }),
        Err(("GcpOptions", NO_PROJECT.to_string()))
    );
    assert_eq!(
        check(GcpOptions {
            project: project(),
            staging_location: Some("gs://b/stg".to_string()),
            ..Default::default()
        }),
        Err(("GcpOptions", NO_REGION.to_string()))
    );
    assert_eq!(
        check(GcpOptions {
            project: project(),
            region: region(),
            ..Default::default()
        }),
        Err(("GcpOptions", NO_LOCATION.to_string()))
    );

    // Either location satisfies the check.
    assert_eq!(
        check(GcpOptions {
            project: project(),
            region: region(),
            temp_location: Some("gs://b/tmp".to_string()),
            ..Default::default()
        }),
        Ok(())
    );
    assert_eq!(
        check(GcpOptions {
            project: project(),
            region: region(),
            staging_location: Some("gs://b/stg".to_string()),
            ..Default::default()
        }),
        Ok(())
    );
}

#[test]
fn test_generic_method_receiving_generic_options() {
    fn stage_dataflow_job(options: &PipelineOptions) -> Result<String, OptionsError> {
        let gcp: GcpOptions = options.view_as()?;
        gcp.require_dataflow_fields()?;
        let staging = gcp
            .staging_location
            .or(gcp.temp_location)
            .expect("checked by require_dataflow_fields");
        Ok(staging)
    }

    let valid_opts = PipelineOptions::parse_from([
        "app",
        "--runner=dataflow",
        "--project=my-cloud-project",
        "--region=us-west1",
        "--staging_location=gs://my-bucket/staging",
    ]);
    let staging = stage_dataflow_job(&valid_opts).expect("should succeed");
    assert_eq!(staging, "gs://my-bucket/staging");

    // Only --region is missing, so validation reports that field.
    let incomplete_opts = PipelineOptions::parse_from([
        "app",
        "--runner=dataflow",
        "--project=p",
        "--temp_location=gs://b/tmp",
    ]);
    let err = stage_dataflow_job(&incomplete_opts).expect_err("should fail");
    assert_eq!(validation(err), ("GcpOptions", NO_REGION.to_string()));
}

#[test]
fn test_programmatic_gcp_options_override_the_command_line() {
    let options = PipelineOptions::parse_from(["app", "--runner=dataflow", "--project=cli"]);
    options
        .set(GcpOptions {
            project: Some("app-project".to_string()),
            ..Default::default()
        })
        .expect("valid options");

    let gcp: GcpOptions = options.view_as().expect("set programmatically");
    assert_eq!(gcp.project.as_deref(), Some("app-project"));
}
