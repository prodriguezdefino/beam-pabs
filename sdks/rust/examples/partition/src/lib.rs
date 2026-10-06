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

//! Partition example: demonstrates splitting a PCollection by a partition function and merging with Flatten.
//!
//! This pipeline takes student test scores,
//! partitions them into 3 performance tiers (Honours, Pass, Support), processes each tier independently,
//! and recombines the results with `Flatten`.

use beam::prelude::*;

/// Number of partitions created for the student performance tiers.
pub const NUM_TIERS: usize = 3;

/// Index of the Honours partition (score >= 80).
pub const TIER_HONOURS: usize = 0;

/// Index of the Pass partition (50 <= score < 80).
pub const TIER_PASS: usize = 1;

/// Index of the Support partition (score < 50).
pub const TIER_SUPPORT: usize = 2;

/// Computes the performance tier partition index for a given score (0..=100).
pub fn partition_by_tier(score: i64) -> usize {
    match score {
        s if s >= 80 => TIER_HONOURS,
        s if s >= 50 => TIER_PASS,
        _ => TIER_SUPPORT,
    }
}

/// Returns a default sample list of students and their test scores.
pub fn default_students() -> Vec<(String, i64)> {
    vec![
        ("Alice".to_string(), 95),
        ("Bob".to_string(), 88),
        ("Charlie".to_string(), 76),
        ("David".to_string(), 65),
        ("Ellen".to_string(), 82),
        ("Frank".to_string(), 45),
        ("Grace".to_string(), 91),
        ("Hannah".to_string(), 58),
        ("Isaac".to_string(), 33),
        ("Jack".to_string(), 70),
        ("Kelly".to_string(), 85),
        ("Liam".to_string(), 40),
    ]
}

/// Constructs the partition and merge pipeline.
///
/// - Partitions students into 3 collections using `Partition`.
/// - Formats each partition with tier-specific labels using `Map`.
/// - Flattens the 3 streams into a unified `PCollection<String>` using `Flatten`.
/// - Logs each record through `inspect`.
pub fn build_partition_pipeline(
    pipeline: &Pipeline,
    students: Vec<(String, i64)>,
) -> PCollection<String> {
    let input = pipeline.apply(Create::new("CreateStudents", students));

    let tiers = input.partition("PartitionByScoreTier", NUM_TIERS, |(_, score)| {
        partition_by_tier(*score)
    });

    let honours = tiers[TIER_HONOURS].map("FormatHonours", |(name, score)| {
        format!("Honours: {name} ({score})")
    });

    let pass = tiers[TIER_PASS].map("FormatPass", |(name, score)| {
        format!("Pass: {name} ({score})")
    });

    let support = tiers[TIER_SUPPORT].map("FormatSupport", |(name, score)| {
        format!("Support: {name} ({score})")
    });

    let merged = honours.flatten("MergeTiers", &[&pass, &support]);

    merged.inspect("LogStudentTier", |msg: &String| {
        tracing::info!("{msg}");
    })
}
