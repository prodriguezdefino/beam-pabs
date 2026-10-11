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

//! Resource hints (accelerator, RAM, CPU count, bundle concurrency) for a pipeline or a
//! transform. Hints are advisory: a runner can ignore hints that it does not support.
//! See <https://beam.apache.org/documentation/runtime/resource-hints/>.

use std::collections::{BTreeMap, HashMap};

use crate::options::groups::ResourceHintsOptions;
use crate::pipeline::constants::{
    URN_RESOURCE_ACCELERATOR, URN_RESOURCE_CPU_COUNT, URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER,
    URN_RESOURCE_MIN_RAM_BYTES,
};

#[derive(thiserror::Error, Debug, Clone, PartialEq, Eq)]
pub enum ResourceHintError {
    /// The string is not formatted as `name=value`.
    #[error("Invalid resource hint format '{0}': expected '<name_or_urn>=<value>'")]
    InvalidFormat(String),

    /// The hint name is neither a standard hint name nor a URN.
    #[error(
        "Unknown resource hint '{0}': must be a standard hint name or start with 'beam:resources:'"
    )]
    UnknownHint(String),

    #[error("Invalid memory size '{0}': {1}")]
    InvalidMemorySize(String, String),

    /// The count is not a positive integer.
    #[error("Invalid integer count for '{0}': {1}")]
    InvalidInteger(String, String),
}

/// Resource hints of a pipeline or transform, keyed by URN.
#[derive(Clone, Debug, Default, PartialEq, Eq)]
pub struct ResourceHints {
    hints: BTreeMap<String, Vec<u8>>,
}

impl ResourceHints {
    pub fn new() -> Self {
        Self {
            hints: BTreeMap::new(),
        }
    }

    /// Sets the accelerator, for example `type:nvidia-tesla-t4;count:1`.
    pub fn with_accelerator(mut self, spec: impl Into<String>) -> Self {
        self.hints.insert(
            URN_RESOURCE_ACCELERATOR.to_string(),
            spec.into().into_bytes(),
        );
        self
    }

    pub fn with_min_ram_bytes(mut self, bytes: u64) -> Self {
        self.hints.insert(
            URN_RESOURCE_MIN_RAM_BYTES.to_string(),
            bytes.to_string().into_bytes(),
        );
        self
    }

    /// Sets the minimum RAM from a size such as `"16GB"`, `"4GiB"` or `"512MB"`.
    pub fn with_min_ram(self, spec: &str) -> Result<Self, ResourceHintError> {
        let bytes = parse_storage_size(spec)?;
        Ok(self.with_min_ram_bytes(bytes))
    }

    pub fn with_cpu_count(mut self, cpus: usize) -> Self {
        self.hints.insert(
            URN_RESOURCE_CPU_COUNT.to_string(),
            cpus.to_string().into_bytes(),
        );
        self
    }

    pub fn with_max_active_bundles_per_worker(mut self, n: usize) -> Self {
        self.hints.insert(
            URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER.to_string(),
            n.to_string().into_bytes(),
        );
        self
    }

    /// Sets any hint by URN and encoded payload.
    pub fn with_hint(mut self, urn: impl Into<String>, payload: impl Into<Vec<u8>>) -> Self {
        self.hints.insert(urn.into(), payload.into());
        self
    }

    /// Merges these inner hints with `outer` (pipeline or parent composite hints).
    /// `min_ram_bytes` and `cpu_count` take the maximum, `max_active_bundles_per_worker` takes
    /// the sum, and for all other hints the inner value wins.
    pub fn merge_with_outer(&self, outer: &ResourceHints) -> ResourceHints {
        if outer.is_empty() {
            return self.clone();
        }
        if self.is_empty() {
            return outer.clone();
        }

        let merge_u64 =
            |outer_bytes: Option<&Vec<u8>>, inner_bytes: &[u8], op: fn(u64, u64) -> u64| {
                let inner_val = parse_u64_payload(inner_bytes).unwrap_or(0);
                let outer_val = outer_bytes.and_then(|p| parse_u64_payload(p)).unwrap_or(0);
                op(inner_val, outer_val).to_string().into_bytes()
            };

        let merged = self
            .hints
            .iter()
            .fold(outer.hints.clone(), |mut acc, (urn, payload)| {
                match urn.as_str() {
                    URN_RESOURCE_MIN_RAM_BYTES | URN_RESOURCE_CPU_COUNT => {
                        let combined = merge_u64(acc.get(urn), payload, u64::max);
                        acc.insert(urn.clone(), combined);
                    }
                    URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER => {
                        let combined = merge_u64(acc.get(urn), payload, u64::saturating_add);
                        acc.insert(urn.clone(), combined);
                    }
                    _ => {
                        acc.insert(urn.clone(), payload.clone());
                    }
                }
                acc
            });

        ResourceHints { hints: merged }
    }

    /// Parses `<name_or_urn>=<value>` into a canonical URN and payload.
    pub fn parse_hint(raw: &str) -> Result<(String, Vec<u8>), ResourceHintError> {
        let (key, value) = raw
            .split_once('=')
            .ok_or_else(|| ResourceHintError::InvalidFormat(raw.to_string()))?;
        Self::from_key_value(key.trim(), value.trim())
    }

    /// Converts a hint name or URN and a value into a canonical URN and payload. Counts must
    /// be greater than 0.
    pub fn from_key_value(key: &str, value: &str) -> Result<(String, Vec<u8>), ResourceHintError> {
        let urn = resolve_urn(key)?;
        let payload = match urn.as_str() {
            URN_RESOURCE_ACCELERATOR => value.as_bytes().to_vec(),
            URN_RESOURCE_MIN_RAM_BYTES => {
                let bytes = parse_storage_size(value)?;
                bytes.to_string().into_bytes()
            }
            URN_RESOURCE_CPU_COUNT => {
                let count: usize = value.parse().map_err(|e| {
                    ResourceHintError::InvalidInteger(key.to_string(), format!("{e}"))
                })?;
                if count == 0 {
                    return Err(ResourceHintError::InvalidInteger(
                        key.to_string(),
                        "must be greater than 0".to_string(),
                    ));
                }
                count.to_string().into_bytes()
            }
            URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER => {
                let count: usize = value.parse().map_err(|e| {
                    ResourceHintError::InvalidInteger(key.to_string(), format!("{e}"))
                })?;
                if count == 0 {
                    return Err(ResourceHintError::InvalidInteger(
                        key.to_string(),
                        "must be greater than 0".to_string(),
                    ));
                }
                count.to_string().into_bytes()
            }
            _ => value.as_bytes().to_vec(),
        };

        Ok((urn, payload))
    }
}

impl TryFrom<&ResourceHintsOptions> for ResourceHints {
    type Error = ResourceHintError;

    fn try_from(opts: &ResourceHintsOptions) -> Result<Self, Self::Error> {
        let mut hints = Self::new();
        for raw in &opts.resource_hints {
            let (urn, payload) = Self::parse_hint(raw)?;
            hints.hints.insert(urn, payload);
        }
        Ok(hints)
    }
}

impl From<&HashMap<String, Vec<u8>>> for ResourceHints {
    fn from(proto_map: &HashMap<String, Vec<u8>>) -> Self {
        Self {
            hints: proto_map
                .iter()
                .map(|(k, v)| (k.clone(), v.clone()))
                .collect(),
        }
    }
}

impl From<HashMap<String, Vec<u8>>> for ResourceHints {
    fn from(proto_map: HashMap<String, Vec<u8>>) -> Self {
        Self {
            hints: proto_map.into_iter().collect(),
        }
    }
}

impl From<&ResourceHints> for HashMap<String, Vec<u8>> {
    fn from(hints: &ResourceHints) -> Self {
        hints
            .hints
            .iter()
            .map(|(k, v)| (k.clone(), v.clone()))
            .collect()
    }
}

impl From<ResourceHints> for HashMap<String, Vec<u8>> {
    fn from(hints: ResourceHints) -> Self {
        hints.hints.into_iter().collect()
    }
}

impl ResourceHints {
    pub fn is_empty(&self) -> bool {
        self.hints.is_empty()
    }

    /// Returns the number of hint URNs.
    pub fn len(&self) -> usize {
        self.hints.len()
    }

    /// Returns the raw payload for `urn`.
    pub fn get(&self, urn: &str) -> Option<&[u8]> {
        self.hints.get(urn).map(Vec::as_slice)
    }

    pub fn accelerator(&self) -> Option<&str> {
        self.get(URN_RESOURCE_ACCELERATOR)
            .and_then(|p| std::str::from_utf8(p).ok())
    }

    pub fn min_ram_bytes(&self) -> Option<u64> {
        self.get(URN_RESOURCE_MIN_RAM_BYTES)
            .and_then(parse_u64_payload)
    }

    pub fn cpu_count(&self) -> Option<usize> {
        self.get(URN_RESOURCE_CPU_COUNT)
            .and_then(parse_u64_payload)
            .map(|n| n as usize)
    }

    pub fn max_active_bundles_per_worker(&self) -> Option<usize> {
        self.get(URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER)
            .and_then(parse_u64_payload)
            .map(|n| n as usize)
    }
}

fn resolve_urn(name_or_urn: &str) -> Result<String, ResourceHintError> {
    match name_or_urn {
        "accelerator" | URN_RESOURCE_ACCELERATOR => Ok(URN_RESOURCE_ACCELERATOR.to_string()),
        // `minRam` and `cpuCount` are the names of these hints in the Java SDK.
        "min_ram" | "minRam" | URN_RESOURCE_MIN_RAM_BYTES => {
            Ok(URN_RESOURCE_MIN_RAM_BYTES.to_string())
        }
        "cpu_count" | "cpuCount" | URN_RESOURCE_CPU_COUNT => Ok(URN_RESOURCE_CPU_COUNT.to_string()),
        "max_active_bundles_per_worker" | URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER => {
            Ok(URN_RESOURCE_MAX_ACTIVE_BUNDLES_PER_WORKER.to_string())
        }
        custom if custom.starts_with("beam:resources:") => Ok(custom.to_string()),
        other => Err(ResourceHintError::UnknownHint(other.to_string())),
    }
}

fn parse_u64_payload(payload: &[u8]) -> Option<u64> {
    std::str::from_utf8(payload)
        .ok()?
        .trim()
        .parse::<u64>()
        .ok()
}

/// Parses a storage size, such as `"16GB"`, `"4GiB"`, `"1024MB"` or `"1048576"`, into bytes.
/// `KB`, `MB`, ... are 1000-based; `KiB`, `MiB`, ... are 1024-based.
pub fn parse_storage_size(s: &str) -> Result<u64, ResourceHintError> {
    let raw = s.trim();
    if raw.is_empty() {
        return Err(ResourceHintError::InvalidMemorySize(
            s.to_string(),
            "empty string".to_string(),
        ));
    }

    const KIB: f64 = 1024.0;
    const MIB: f64 = 1024.0 * KIB;
    const GIB: f64 = 1024.0 * MIB;
    const TIB: f64 = 1024.0 * GIB;
    const PIB: f64 = 1024.0 * TIB;

    const KB: f64 = 1000.0;
    const MB: f64 = 1000.0 * KB;
    const GB: f64 = 1000.0 * MB;
    const TB: f64 = 1000.0 * GB;
    const PB: f64 = 1000.0 * TB;

    let suffixes: &[(&str, f64)] = &[
        ("kib", KIB),
        ("mib", MIB),
        ("gib", GIB),
        ("tib", TIB),
        ("pib", PIB),
        ("kb", KB),
        ("mb", MB),
        ("gb", GB),
        ("tb", TB),
        ("pb", PB),
        ("b", 1.0),
    ];

    let lower = raw.to_ascii_lowercase();

    for &(suffix, multiplier) in suffixes {
        if let Some(num_str) = lower.strip_suffix(suffix) {
            let num_str = num_str.trim();
            let value = num_str
                .parse::<f64>()
                .map_err(|e| ResourceHintError::InvalidMemorySize(s.to_string(), format!("{e}")))?;
            if value < 0.0 {
                return Err(ResourceHintError::InvalidMemorySize(
                    s.to_string(),
                    "size cannot be negative".to_string(),
                ));
            }
            let bytes = (value * multiplier).round();
            // `as u64` saturates, so reject NaN, infinity and sizes above u64::MAX first.
            if !bytes.is_finite() || bytes >= u64::MAX as f64 {
                return Err(ResourceHintError::InvalidMemorySize(
                    s.to_string(),
                    "size is not a finite number of bytes below 2^64".to_string(),
                ));
            }
            return Ok(bytes as u64);
        }
    }

    // A value without a suffix is a plain byte count.
    raw.parse::<u64>().map_err(|e| {
        ResourceHintError::InvalidMemorySize(s.to_string(), format!("unrecognized format: {e}"))
    })
}
