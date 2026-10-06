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

use beam::options::{PipelineOptions, ResourceHintsOptions};
use beam::pipeline::{
    Pipeline, ResourceHints, URN_RESOURCE_ACCELERATOR, URN_RESOURCE_CPU_COUNT,
    URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER, URN_RESOURCE_MIN_RAM_BYTES, parse_storage_size,
};
use beam::transforms::{Create, DoFn, ParDo, ProcessContext, WithResourceHintsExt};

#[derive(Clone)]
struct IdentityDoFn;

impl DoFn for IdentityDoFn {
    type In = String;
    type Out = String;

    fn process_element(
        &mut self,
        element: Self::In,
        ctx: &mut ProcessContext<'_, Self::Out>,
    ) -> beam::Result {
        ctx.emit(element)
    }
}

#[test]
fn test_parse_storage_size() {
    assert_eq!(parse_storage_size("1024").unwrap(), 1024);
    assert_eq!(parse_storage_size("1024b").unwrap(), 1024);
    assert_eq!(parse_storage_size("1KB").unwrap(), 1_000);
    assert_eq!(parse_storage_size("1KiB").unwrap(), 1_024);
    assert_eq!(parse_storage_size("512MB").unwrap(), 512_000_000);
    assert_eq!(parse_storage_size("512MiB").unwrap(), 512 * 1024 * 1024);
    assert_eq!(parse_storage_size("16GB").unwrap(), 16_000_000_000);
    assert_eq!(
        parse_storage_size("16GiB").unwrap(),
        16 * 1024 * 1024 * 1024
    );
    assert_eq!(
        parse_storage_size("1.5 GiB").unwrap(),
        (1.5 * 1024.0 * 1024.0 * 1024.0) as u64
    );

    assert!(parse_storage_size("").is_err());
    assert!(parse_storage_size("invalid").is_err());
    assert!(parse_storage_size("-5GB").is_err());
}

#[test]
fn test_resource_hints_parsing() {
    let (urn, val) = ResourceHints::parse_hint("accelerator=type:nvidia-tesla-t4;count:1").unwrap();
    assert_eq!(urn, URN_RESOURCE_ACCELERATOR);
    assert_eq!(val, b"type:nvidia-tesla-t4;count:1");

    let (urn, val) = ResourceHints::parse_hint("min_ram=16GB").unwrap();
    assert_eq!(urn, URN_RESOURCE_MIN_RAM_BYTES);
    assert_eq!(val, b"16000000000");

    let (urn, val) = ResourceHints::parse_hint("minRam=4GiB").unwrap();
    assert_eq!(urn, URN_RESOURCE_MIN_RAM_BYTES);
    assert_eq!(val, (4 * 1024 * 1024 * 1024u64).to_string().into_bytes());

    let (urn, val) = ResourceHints::parse_hint("cpu_count=8").unwrap();
    assert_eq!(urn, URN_RESOURCE_CPU_COUNT);
    assert_eq!(val, b"8");

    let (urn, val) = ResourceHints::parse_hint("cpuCount=4").unwrap();
    assert_eq!(urn, URN_RESOURCE_CPU_COUNT);
    assert_eq!(val, b"4");

    let (urn, val) = ResourceHints::parse_hint("max_active_bundles_per_worker=10").unwrap();
    assert_eq!(urn, URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER);
    assert_eq!(val, b"10");

    // Custom URN
    let (urn, val) = ResourceHints::parse_hint("beam:resources:custom:v1=custom_val").unwrap();
    assert_eq!(urn, "beam:resources:custom:v1");
    assert_eq!(val, b"custom_val");

    // Errors
    assert!(ResourceHints::parse_hint("no_equals_sign").is_err());
    assert!(ResourceHints::parse_hint("unknown_hint=val").is_err());
    assert!(ResourceHints::parse_hint("cpu_count=0").is_err());
    assert!(ResourceHints::parse_hint("cpu_count=-1").is_err());
    assert!(ResourceHints::parse_hint("min_ram=invalid").is_err());
}

#[test]
fn test_resource_hints_merge_rules() {
    let outer = ResourceHints::new()
        .with_accelerator("outer_gpu")
        .with_min_ram_bytes(8_000_000_000)
        .with_cpu_count(4)
        .with_max_active_bundles_per_worker(2)
        .with_hint("beam:resources:custom:v1", "outer_custom");

    let inner = ResourceHints::new()
        .with_accelerator("inner_gpu") // Overrides outer.
        .with_min_ram_bytes(16_000_000_000) // Takes maximum.
        .with_cpu_count(2) // Takes maximum (outer 4 > inner 2).
        .with_max_active_bundles_per_worker(3) // Sums (2 + 3 = 5).
        .with_hint("beam:resources:custom:v1", "inner_custom") // Overrides outer.
        .with_hint("beam:resources:extra:v1", "extra_val");

    let merged = inner.merge_with_outer(&outer);

    assert_eq!(merged.accelerator(), Some("inner_gpu"));
    assert_eq!(merged.min_ram_bytes(), Some(16_000_000_000));
    assert_eq!(merged.cpu_count(), Some(4));
    assert_eq!(merged.max_active_bundles_per_worker(), Some(5));
    assert_eq!(
        merged
            .get("beam:resources:custom:v1")
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("inner_custom")
    );
    assert_eq!(
        merged
            .get("beam:resources:extra:v1")
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("extra_val")
    );

    let empty = ResourceHints::new();
    assert_eq!(inner.merge_with_outer(&empty), inner);
    assert_eq!(empty.merge_with_outer(&outer), outer);
}

#[test]
fn test_pipeline_options_resource_hints() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=prism",
        "--resource_hints=accelerator=type:nvidia-tesla-t4;count:1,min_ram=16GB",
        "--resource_hint=cpu_count=8",
    ]);

    let res_opts: ResourceHintsOptions = opts.view_as().expect("ResourceHintsOptions should parse");
    assert_eq!(res_opts.resource_hints.len(), 3);

    let hints = opts.resource_hints().expect("hints should parse");
    assert_eq!(hints.accelerator(), Some("type:nvidia-tesla-t4;count:1"));
    assert_eq!(hints.min_ram_bytes(), Some(16_000_000_000));
    assert_eq!(hints.cpu_count(), Some(8));
}

#[test]
fn test_pipeline_default_environment_resource_hints() {
    let opts = PipelineOptions::parse_from([
        "app",
        "--runner=prism",
        "--resource_hints=min_ram=8GB,cpu_count=4",
    ]);

    let p = Pipeline::create(&opts);
    let proto = p.to_proto();
    let components = proto.components.unwrap();
    let default_env = &components.environments[&p.default_environment_id()];

    assert_eq!(
        default_env
            .resource_hints
            .get(URN_RESOURCE_MIN_RAM_BYTES)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("8000000000")
    );
    assert_eq!(
        default_env
            .resource_hints
            .get(URN_RESOURCE_CPU_COUNT)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("4")
    );
}

#[test]
fn test_transform_resource_hints_environment_scoping() {
    let p = Pipeline::new().with_resource_hints(ResourceHints::new().with_cpu_count(2));

    let col = p.apply(Create::new("Create", vec!["hello".to_string()]));

    // Inherits default environment when no extra hints are set.
    let t1 = col.apply(ParDo::new("DefaultParDo", IdentityDoFn));

    // Custom hints create a scoped environment.
    let t2 = t1.apply(ParDo::new("GpuParDo", IdentityDoFn).with_resource_hints(
        ResourceHints::new().with_accelerator("type:nvidia-tesla-t4;count:1"),
    ));

    // Identical hints share the scoped environment.
    let _t3 = t2.apply(
        ParDo::new("AnotherGpuParDo", IdentityDoFn).with_resource_hints(
            ResourceHints::new().with_accelerator("type:nvidia-tesla-t4;count:1"),
        ),
    );

    let proto = p.to_proto();
    let components = proto.components.unwrap();

    let default_env_id = p.default_environment_id();
    let default_pardo = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("DefaultParDo"))
        .unwrap();
    assert_eq!(default_pardo.environment_id, default_env_id);

    let gpu_pardo_1 = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("GpuParDo"))
        .unwrap();
    let gpu_pardo_2 = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("AnotherGpuParDo"))
        .unwrap();

    // Verify GPU transforms share the same dedicated environment.
    assert_ne!(gpu_pardo_1.environment_id, default_env_id);
    assert_eq!(gpu_pardo_1.environment_id, gpu_pardo_2.environment_id);

    let gpu_env = &components.environments[&gpu_pardo_1.environment_id];
    assert_eq!(
        gpu_env
            .resource_hints
            .get(URN_RESOURCE_ACCELERATOR)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("type:nvidia-tesla-t4;count:1")
    );
    // Inherited outer cpu_count hint is preserved during environment merge.
    assert_eq!(
        gpu_env
            .resource_hints
            .get(URN_RESOURCE_CPU_COUNT)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("2")
    );
}

#[test]
fn test_with_resource_hints_ext_wrapper() {
    let p = Pipeline::new();
    let col = p.apply(Create::new("Create", vec!["test".to_string()]));

    // Use WithResourceHintsExt on a generic transform
    let tform = ParDo::new("WrappedParDo", IdentityDoFn);
    let wrapped = WithResourceHintsExt::with_resource_hints(
        tform,
        ResourceHints::new().with_min_ram_bytes(16_000_000_000),
    );
    let _res = col.apply(wrapped);

    let proto = p.to_proto();
    let components = proto.components.unwrap();
    let wrapped_transform = components
        .transforms
        .values()
        .find(|t| t.unique_name.starts_with("WrappedParDo"))
        .unwrap();
    assert_ne!(wrapped_transform.environment_id, p.default_environment_id());

    let env = &components.environments[&wrapped_transform.environment_id];
    assert_eq!(
        env.resource_hints
            .get(URN_RESOURCE_MIN_RAM_BYTES)
            .map(|b| std::str::from_utf8(b).unwrap()),
        Some("16000000000")
    );
}
